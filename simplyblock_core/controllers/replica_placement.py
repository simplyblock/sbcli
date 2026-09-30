# coding=utf-8
"""Global, constraint-solving planner for secondary/tertiary replica placement.

Pure logic -- no DB access, no SPDK calls -- so it can be unit-tested in
isolation. The orchestrator that consumes the plan lives in
``storage_node_ops`` (node removal, phase 3b).

Why a global planner
--------------------
The historical relocation path (``_pick_replica_relocation_node`` and
friends) repairs **one stranded role at a time**: it looks at a single
primary, asks "where can this one replica go", and takes the first
candidate that is domain-diverse; when no free candidate exists it splices
into an existing pairing, then patches up at most one further hop of
collateral damage. Every decision is local and irrevocable.

That is provably insufficient for the invariant it is trying to hold.
Consider the reported case: 4 failure domains x 3 hosts = 12 nodes, FTT2,
and one host removed from each domain in turn. Each removal frees exactly
one secondary slot and one tertiary slot system-wide, so at every step the
greedy picker has at most one free candidate per role. If that one free
candidate happens to sit in the primary's own domain -- or in the domain of
the role that is *not* being relocated -- the picker cannot fix it by
moving the other role too, because it never considers the other role. It
either splices (perturbing an uninvolved third node) or falls back to a
weaker "at least one cross-domain role" floor and logs a warning. After a
few removals the layout has accumulated several such compromises even
though a fully diverse layout existed the whole time and was reachable by
*swapping* two already-placed replicas -- a move no local repair can ever
express.

This module solves the whole assignment at once instead:

* every node is primary of exactly one LVS, and hosts at most one
  secondary and at most one tertiary (``lvstore_stack_secondary`` /
  ``lvstore_stack_tertiary`` are single-valued), so "which node hosts
  whose secondary" is a **permutation** of the node set, not a bag of
  independent choices;
* full pairwise diversity (``fd(P)``, ``fd(S)``, ``fd(T)`` all distinct)
  is a hard edge constraint on that permutation;
* "don't move replicas that are already fine" is an objective, not a
  constraint.

That is a min-cost perfect bipartite matching, solved exactly by the
Hungarian algorithm (:func:`min_cost_matching`) in O(n^3) -- trivial at
cluster scale. Secondary and tertiary are coupled only through the
*domain* of the secondary, so they are solved in two stages with an exact
feasibility test between them (:func:`tertiary_blocking_pairs`, Hall's
condition specialised to this structure) and a penalty retry that steers
the secondary stage away from a domain pattern that would strand the
tertiary stage.

The result is: full diversity whenever it is mathematically achievable,
with the provably smallest number of physical replica rebuilds, and an
explicit, machine-checkable statement of what is impossible when it is
not -- instead of a warning buried in a log.
"""

import logging
from typing import Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple

logger = logging.getLogger()


ROLE_SECONDARY = "secondary"
ROLE_TERTIARY = "tertiary"

# Cost scale for the matching. FORBIDDEN is not "infinity" on purpose: the
# matcher stays in integer arithmetic, and a solution that had to use a
# forbidden edge is detected afterwards by its total cost, which is both
# simpler and more robust than sentinel-aware relaxation inside the inner
# loop. Any single edge >= FORBIDDEN means at least one hard constraint was
# unsatisfiable, i.e. no valid perfect matching exists.
MOVE_COST = 1000          # rebuilding a replica on a different host
LABEL_PENALTY = 1         # soft physical-label anti-affinity
PAIR_PENALTY = 50         # steer stage 1 away from a tertiary-blocking pattern
FORBIDDEN = 10 ** 9

# Bound on the stage-1 retry loop. Each iteration penalises the domain pairs
# that made the tertiary stage infeasible, so progress is monotone in
# practice; the bound only exists so a pathological topology degrades to the
# constructive fallback instead of spinning.
MAX_PAIR_RETRIES = 8


class Placement(NamedTuple):
    """Where one LVS's non-leader roles live. ``tertiary`` is ``""`` on FTT1."""
    secondary: str
    tertiary: str


class ReplicaMove(NamedTuple):
    """One planned relocation of a single role.

    ``from_node_id`` is ``""`` when the role currently has no host at all
    (its previous host is the node being removed, already torn down).
    ``scratch`` marks a move that only exists to break a rotation cycle: the
    role is parked on a temporarily free host and moved again later -- see
    :func:`order_moves`.
    """
    lvs_primary_node_id: str
    role: str
    from_node_id: str
    to_node_id: str
    scratch: bool = False


class DiversityPlan(NamedTuple):
    """Result of :func:`plan_diverse_layout`.

    ``layout`` maps primary node id -> :class:`Placement`. ``full_diversity``
    is True when every LVS in ``layout`` has pairwise-distinct domains across
    primary/secondary/tertiary. ``violations`` describes what could not be
    satisfied (empty iff ``full_diversity``). ``notes`` records how the plan
    was reached (which stage / which fallback), for the operator-facing log.
    """
    layout: Dict[str, Placement]
    full_diversity: bool
    violations: List[str]
    notes: List[str]


class InfeasiblePlacement(Exception):
    """No valid assignment exists at all -- not even ignoring failure domains.

    Raised rather than returned because it means the *host-disjointness*
    floor is unsatisfiable (e.g. too few hosts for the fault-tolerance
    level), which is a precondition failure the caller must surface, not a
    quality degradation it can log and continue past.
    """


# ---------------------------------------------------------------------------
# Min-cost perfect matching (Hungarian / Jonker-Volgenant, O(n^3))
# ---------------------------------------------------------------------------

def min_cost_matching(cost: Sequence[Sequence[int]]) -> List[int]:
    """Min-cost perfect matching on a rectangular cost matrix (rows <= cols).

    ``cost[i][j]`` is the cost of assigning row ``i`` to column ``j``. Returns
    a list ``a`` with ``a[i] == j``.

    Forbidden pairs are expressed as :data:`FORBIDDEN`; callers check the
    chosen edges against :data:`FORBIDDEN` to detect "no valid assignment
    exists" rather than relying on the matcher to reject them (see the module
    docstring).

    Shortest-augmenting-path formulation with dual potentials: rows are added
    one at a time, each via a Dijkstra pass over the reduced costs, so the
    matching stays optimal after every augmentation.
    """
    n = len(cost)
    if n == 0:
        return []
    m = len(cost[0])
    if m < n:
        raise ValueError(
            f"cost matrix must have at least as many columns as rows ({n}x{m})")

    inf = FORBIDDEN * 8
    u = [0] * (n + 1)
    v = [0] * (m + 1)
    # p[j] = row currently matched to column j (0 = unmatched); way[j] is the
    # predecessor column on the augmenting path being built.
    p = [0] * (m + 1)
    way = [0] * (m + 1)

    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [inf] * (m + 1)
        used = [False] * (m + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = inf
            j1 = 0
            row = cost[i0 - 1]
            for j in range(1, m + 1):
                if used[j]:
                    continue
                cur = row[j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(m + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break
        while j0:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1

    result = [-1] * n
    for j in range(1, m + 1):
        if p[j]:
            result[p[j] - 1] = j - 1
    return result


# ---------------------------------------------------------------------------
# Diversity checking
# ---------------------------------------------------------------------------

def full_diversity_violations(
    layout: Mapping[str, Placement],
    fd_by_node: Mapping[str, int],
    ftt: int,
) -> List[str]:
    """Report every LVS whose roles are NOT pairwise domain-distinct.

    Deliberately stricter than
    ``cluster_expansion.planner.compute_fd_layout_violations``, which only
    asserts the weaker ">=1 cross-domain role" floor: that floor is satisfied
    by a layout whose secondary and tertiary share a domain (one host outage
    then costs two of the three copies), which is exactly the state the
    incremental relocation path kept producing. A primary with an unset
    domain (< 0) is skipped -- the feature is off for it -- but an unset
    domain on a *role holder* counts as a violation, since "unknown" cannot
    be asserted to be disjoint.
    """
    violations: List[str] = []
    for primary_id in sorted(layout):
        placement = layout[primary_id]
        fd_p = fd_by_node.get(primary_id, -1)
        if fd_p < 0:
            continue
        roles = [(ROLE_SECONDARY, placement.secondary)]
        if ftt >= 2:
            roles.append((ROLE_TERTIARY, placement.tertiary))
        seen: Dict[int, str] = {fd_p: f"primary {primary_id}"}
        for role, holder in roles:
            if not holder:
                violations.append(f"LVS@{primary_id} (fd={fd_p}) has no {role}")
                continue
            fd_h = fd_by_node.get(holder, -1)
            if fd_h < 0:
                violations.append(
                    f"LVS@{primary_id} (fd={fd_p}) {role}={holder} has no "
                    f"failure domain set")
                continue
            if fd_h in seen:
                violations.append(
                    f"LVS@{primary_id} (fd={fd_p}) {role}={holder} (fd={fd_h}) "
                    f"shares a domain with {seen[fd_h]}")
                continue
            seen[fd_h] = f"{role} {holder}"
    return violations


# ---------------------------------------------------------------------------
# Feasibility (Hall's condition, specialised)
# ---------------------------------------------------------------------------

def secondary_overloaded_domains(domain_sizes: Mapping[int, int]) -> List[int]:
    """Domains that make a fully diverse SECONDARY permutation impossible.

    Every node hosts exactly one secondary, so the secondary assignment is a
    permutation and the ``n_d`` primaries of domain ``d`` must all be hosted
    outside ``d``. By Hall's condition that needs ``n_d <= N - n_d``, i.e. no
    domain may hold more than half the cluster. Independent of any particular
    assignment -- purely structural.
    """
    total = sum(domain_sizes.values())
    return sorted(d for d, size in domain_sizes.items() if 2 * size > total)


def tertiary_blocking_pairs(
    forbidden_pairs: Mapping[Tuple[int, int], int],
    domain_sizes: Mapping[int, int],
) -> List[Tuple[int, int]]:
    """Domain pairs that make the TERTIARY stage infeasible under a given
    secondary assignment.

    Given the secondary permutation, primary ``p`` may host its tertiary in
    any domain except ``F(p) = {fd(p), fd(sec(p))}``. Hall's condition over
    the domain-block structure reduces to checking only the subsets whose
    complement is contained in some ``F(p)``, and ``|F(p)| <= 2``, so exactly
    two families need checking:

    * singletons ``{d}``: ``#{p : d in F(p)} <= N - n_d``. That count is
      always ``2 * n_d`` (``n_d`` primaries live in ``d``, and exactly ``n_d``
      secondaries land in ``d`` because the assignment is a permutation), so
      this is the assignment-independent condition
      :func:`secondary_overloaded_domains` already covers -- not repeated
      here.
    * pairs ``{d, e}``: ``#{p : F(p) == {d, e}} <= N - n_d - n_e``. THIS one
      depends on the secondary assignment, and is what the stage-1 penalty
      retry steers away from.

    ``forbidden_pairs`` maps the normalised pair ``(min, max)`` to how many
    primaries currently have exactly that ``F(p)``. Returns the violating
    pairs.
    """
    total = sum(domain_sizes.values())
    blocking: List[Tuple[int, int]] = []
    for (d, e), count in sorted(forbidden_pairs.items()):
        capacity = total - domain_sizes.get(d, 0) - domain_sizes.get(e, 0)
        if count > capacity:
            blocking.append((d, e))
    return blocking


# ---------------------------------------------------------------------------
# The planner
# ---------------------------------------------------------------------------

def _assignment_from_matching(
    primaries: Sequence[str],
    hosts: Sequence[str],
    cost: Sequence[Sequence[int]],
) -> Optional[Dict[str, str]]:
    """Run the matcher and reject the result if it had to use a forbidden
    edge. ``None`` means no assignment satisfying the hard constraints
    exists."""
    matching = min_cost_matching(cost)
    assignment: Dict[str, str] = {}
    for i, j in enumerate(matching):
        if j < 0 or cost[i][j] >= FORBIDDEN:
            return None
        assignment[primaries[i]] = hosts[j]
    return assignment


def _pair_key(a: int, b: int) -> Tuple[int, int]:
    return (a, b) if a <= b else (b, a)


def plan_diverse_layout(
    node_ids: Sequence[str],
    fd_by_node: Mapping[str, int],
    current_layout: Mapping[str, Placement],
    ftt: int,
    *,
    host_by_node: Optional[Mapping[str, str]] = None,
    label_by_node: Optional[Mapping[str, int]] = None,
) -> DiversityPlan:
    """Compute the cheapest fully domain-diverse layout over ``node_ids``.

    ``node_ids`` is the set of nodes that will be alive AFTER the topology
    change; every one of them is primary of its own LVS and is available to
    host one secondary and one tertiary. ``current_layout`` is the layout as
    it stands right now, keyed by primary; entries pointing at nodes outside
    ``node_ids`` (e.g. the node being removed) are treated as "no host", so
    the planner naturally re-homes them. Primaries missing from
    ``current_layout`` are treated the same way.

    Hard constraints, in both stages:

    * a role never lands on its own primary;
    * a role never lands on a host already used by another role of the same
      LVS (host-disjointness -- what actually makes a single host loss
      survivable);
    * with failure domains in play, the domains of primary / secondary /
      tertiary are pairwise distinct.

    Soft preferences, expressed as cost:

    * keeping a role where it already is costs 0, moving it costs
      :data:`MOVE_COST` -- so the returned layout is a *minimum-rebuild*
      layout, not just any valid one;
    * sharing a ``physical_label`` with another role of the same LVS costs
      :data:`LABEL_PENALTY`, matching the existing best-effort treatment of
      that dimension.

    When full diversity is unreachable the domain constraint is dropped (the
    host-disjointness floor is kept) and the plan comes back with
    ``full_diversity=False`` and the specific ``violations``, so the caller
    can decide whether to proceed degraded or refuse -- instead of silently
    settling like the incremental path did.

    Raises :class:`InfeasiblePlacement` when even the host-disjoint floor has
    no solution.
    """
    nodes = list(node_ids)
    n = len(nodes)
    notes: List[str] = []
    if n == 0:
        return DiversityPlan({}, True, [], notes)
    if ftt not in (1, 2):
        raise ValueError(f"ftt must be 1 or 2, got {ftt}")
    if n < ftt + 1:
        raise InfeasiblePlacement(
            f"{n} node(s) cannot host {ftt + 1} distinct copies (FTT{ftt})")

    host_of = dict(host_by_node or {})
    label_of = dict(label_by_node or {})
    fd_of = {node: fd_by_node.get(node, -1) for node in nodes}
    fd_enabled = all(fd_of[node] >= 0 for node in nodes) and len(set(fd_of.values())) > 1

    domain_sizes: Dict[int, int] = {}
    for node in nodes:
        domain_sizes[fd_of[node]] = domain_sizes.get(fd_of[node], 0) + 1

    def _host(node: str) -> str:
        return host_of.get(node, node)

    def _label(node: str) -> int:
        return label_of.get(node, 0)

    def _current(primary: str) -> Placement:
        placement = current_layout.get(primary, Placement("", ""))
        secondary = placement.secondary if placement.secondary in fd_of else ""
        tertiary = placement.tertiary if placement.tertiary in fd_of else ""
        return Placement(secondary, tertiary)

    overloaded = secondary_overloaded_domains(domain_sizes) if fd_enabled else []
    if overloaded:
        notes.append(
            f"failure domain(s) {overloaded} hold more than half the cluster; "
            f"a fully diverse layout is structurally impossible")

    # -- stage 1: secondary permutation ------------------------------------
    def _secondary_cost(enforce_fd: bool, pair_penalties: Mapping[Tuple[int, int], int]):
        matrix: List[List[int]] = []
        for primary in nodes:
            row: List[int] = []
            for host in nodes:
                if host == primary or _host(host) == _host(primary):
                    row.append(FORBIDDEN)
                    continue
                if enforce_fd and fd_of[host] == fd_of[primary]:
                    row.append(FORBIDDEN)
                    continue
                cost = 0 if host == _current(primary).secondary else MOVE_COST
                if _label(host) > 0 and _label(host) == _label(primary):
                    cost += LABEL_PENALTY
                cost += pair_penalties.get(_pair_key(fd_of[primary], fd_of[host]), 0)
                row.append(cost)
            matrix.append(row)
        return matrix

    enforce_fd = fd_enabled and not overloaded
    penalties: Dict[Tuple[int, int], int] = {}
    secondary: Optional[Dict[str, str]] = None
    for attempt in range(MAX_PAIR_RETRIES + 1):
        secondary = _assignment_from_matching(
            nodes, nodes, _secondary_cost(enforce_fd, penalties))
        if secondary is None:
            break
        if not enforce_fd or ftt < 2:
            break
        pair_counts: Dict[Tuple[int, int], int] = {}
        for primary in nodes:
            key = _pair_key(fd_of[primary], fd_of[secondary[primary]])
            pair_counts[key] = pair_counts.get(key, 0) + 1
        blocking = tertiary_blocking_pairs(pair_counts, domain_sizes)
        if not blocking:
            break
        if attempt == MAX_PAIR_RETRIES:
            notes.append(
                f"could not steer the secondary layout away from "
                f"tertiary-blocking domain pattern(s) {blocking}")
            break
        for pair in blocking:
            penalties[pair] = penalties.get(pair, 0) + PAIR_PENALTY

    if secondary is None and enforce_fd:
        # Domain-diverse secondaries are unreachable; keep the host-disjoint
        # floor so the cluster still survives a single host loss.
        enforce_fd = False
        notes.append(
            "no domain-diverse secondary permutation exists; falling back to "
            "host-disjoint placement")
        secondary = _assignment_from_matching(nodes, nodes, _secondary_cost(False, {}))
    if secondary is None:
        raise InfeasiblePlacement(
            "no host-disjoint secondary placement exists for this node set")

    if ftt < 2:
        layout = {primary: Placement(secondary[primary], "") for primary in nodes}
        violations = full_diversity_violations(layout, fd_of, ftt) if fd_enabled else []
        return DiversityPlan(layout, not violations, violations, notes)

    # -- stage 2: tertiary permutation, given the secondary domains --------
    def _tertiary_cost(enforce: bool):
        matrix: List[List[int]] = []
        for primary in nodes:
            sec = secondary[primary]
            row: List[int] = []
            for host in nodes:
                if host in (primary, sec):
                    row.append(FORBIDDEN)
                    continue
                if _host(host) in (_host(primary), _host(sec)):
                    row.append(FORBIDDEN)
                    continue
                if enforce and fd_of[host] in (fd_of[primary], fd_of[sec]):
                    row.append(FORBIDDEN)
                    continue
                cost = 0 if host == _current(primary).tertiary else MOVE_COST
                if _label(host) > 0 and _label(host) in (_label(primary), _label(sec)):
                    cost += LABEL_PENALTY
                row.append(cost)
            matrix.append(row)
        return matrix

    tertiary = _assignment_from_matching(nodes, nodes, _tertiary_cost(enforce_fd))
    if tertiary is None and enforce_fd:
        notes.append(
            "no fully domain-diverse tertiary permutation exists for the chosen "
            "secondary layout; falling back to host-disjoint placement")
        tertiary = _assignment_from_matching(nodes, nodes, _tertiary_cost(False))
    if tertiary is None:
        raise InfeasiblePlacement(
            "no host-disjoint tertiary placement exists for this node set")

    layout = {
        primary: Placement(secondary[primary], tertiary[primary])
        for primary in nodes
    }
    violations = full_diversity_violations(layout, fd_of, ftt) if fd_enabled else []
    return DiversityPlan(layout, not violations, violations, notes)


# ---------------------------------------------------------------------------
# Diffing and ordering
# ---------------------------------------------------------------------------

def diff_layout(
    current_layout: Mapping[str, Placement],
    target_layout: Mapping[str, Placement],
    ftt: int,
) -> List[ReplicaMove]:
    """Unordered set of role relocations turning ``current`` into ``target``.

    Only primaries present in ``target_layout`` are considered -- a primary
    that disappeared (the node being removed) has its replicas torn down by
    the caller, not relocated. ``from_node_id`` is ``""`` when the role has
    no current host.
    """
    moves: List[ReplicaMove] = []
    roles = [(ROLE_SECONDARY, 0)] + ([(ROLE_TERTIARY, 1)] if ftt >= 2 else [])
    for primary in sorted(target_layout):
        target = target_layout[primary]
        current = current_layout.get(primary, Placement("", ""))
        for role, index in roles:
            want = target[index]
            have = current[index]
            if not want or want == have:
                continue
            moves.append(ReplicaMove(primary, role, have, want))
    return moves


def order_moves(
    moves: Sequence[ReplicaMove],
    current_layout: Mapping[str, Placement],
    all_node_ids: Sequence[str],
    ftt: int,
) -> List[ReplicaMove]:
    """Order ``moves`` so every one lands on a host slot that is free at the
    time it runs -- and insert scratch hops where that is impossible.

    A node's ``lvstore_stack_secondary`` / ``lvstore_stack_tertiary`` is a
    single string, so a host can record at most one secondary and one
    tertiary. Relocations therefore cannot be applied in arbitrary order: a
    move onto an occupied slot has to wait for its occupant to leave. Within
    one role, the pending moves form a functional graph over host slots, so:

    * **chains** ending on a currently-free slot execute back-to-front and
      always fit (a removal frees exactly one slot per role, which is what
      makes the ordinary post-removal repair a single chain);
    * **cycles** (a pure rotation, e.g. two primaries swapping hosts) have no
      free slot to start from. They are broken by parking one member on a
      free host first -- an extra ``scratch=True`` build -- and moving it to
      its real target once the rotation has come round. Without this the
      recursive vacate in ``_relocate_replica_between`` walks the cycle and
      hits its own cycle backstop, failing the whole relocation; a swap
      between two already-placed replicas is exactly the repair a local
      picker can never express, so it has to be executable here.

    Cycles are broken BEFORE any chain runs, and never on demand once the
    ordering is under way. A removal frees exactly one slot per role, and a
    chain consumes that slot when it terminates -- so a cycle discovered
    after the chains have run would have nowhere left to park. Breaking a
    cycle is slot-neutral (parking on the free host immediately frees the
    parked member's own host, and the final hop out of the scratch host
    gives it back), so doing all of them up front leaves the chains exactly
    the one slot they need.

    Raises :class:`InfeasiblePlacement` if a cycle has to be broken and no
    host slot is free at all. That cannot happen in the removal flow, which
    always frees one; it does happen when repairing a fully-occupied layout
    in place, where a rotation is unexecutable while
    ``lvstore_stack_secondary`` / ``_tertiary`` stay single-valued.
    """
    ordered: List[ReplicaMove] = []
    roles = [(ROLE_SECONDARY, 0)] + ([(ROLE_TERTIARY, 1)] if ftt >= 2 else [])
    for role, index in roles:
        role_moves = [m for m in moves if m.role == role]
        if not role_moves:
            continue
        occupied: Dict[str, str] = {}
        for primary, placement in current_layout.items():
            holder = placement[index]
            if holder:
                occupied[holder] = primary
        pending: Dict[str, ReplicaMove] = {m.lvs_primary_node_id: m for m in role_moves}
        # A slot is free when no surviving primary's role currently sits on it.
        survivors = set(all_node_ids)
        free = sorted(node for node in all_node_ids if node not in occupied)

        def _emit(move: ReplicaMove) -> None:
            ordered.append(move)
            if move.from_node_id:
                occupied.pop(move.from_node_id, None)
                # A role vacated off the node being removed frees nothing
                # usable: that node is on its way out and must never be
                # picked as a scratch host.
                if move.from_node_id in survivors and move.from_node_id not in free:
                    free.append(move.from_node_id)
            occupied[move.to_node_id] = move.lvs_primary_node_id
            if move.to_node_id in free:
                free.remove(move.to_node_id)

        _break_cycles(pending, free, role, _emit)

        while pending:
            for primary in sorted(pending):
                if pending[primary].to_node_id not in occupied:
                    _emit(pending.pop(primary))
                    break
            else:  # pragma: no cover - _break_cycles leaves only chains
                raise InfeasiblePlacement(
                    f"cannot order {role} relocations: no move can run without "
                    f"displacing a replica that is not itself moving")
    return ordered


def _break_cycles(pending, free, role, emit) -> None:
    """Rewrite every rotation cycle in ``pending`` into a chain.

    Each pending move points at the move that vacates its target host -- or
    at nothing, when the target is already free. In a valid target layout
    each host is the target of exactly one primary, so that mapping is a
    partial function and its components are exactly chains (ending on a free
    host) and cycles. For each cycle, one member is parked on a free host via
    a ``scratch`` move; its own host becomes free, turning the cycle into a
    chain that terminates there.
    """
    vacated_by = {
        move.from_node_id: primary
        for primary, move in pending.items() if move.from_node_id
    }
    visited: set = set()
    for start in sorted(pending):
        if start in visited:
            continue
        path: List[str] = []
        position: Dict[str, int] = {}
        cursor: Optional[str] = start
        while cursor is not None and cursor not in visited:
            position[cursor] = len(path)
            path.append(cursor)
            visited.add(cursor)
            cursor = vacated_by.get(pending[cursor].to_node_id)
        if cursor is None or cursor not in position:
            continue  # a chain, or it joined an already-classified component
        if not free:
            raise InfeasiblePlacement(
                f"cannot order {role} relocations: a rotation cycle "
                f"({' -> '.join(path[position[cursor]:])}) has to be broken and "
                f"no host slot is free to park a replica on")
        scratch_host = free[0]
        primary = sorted(path[position[cursor]:])[0]
        move = pending[primary]
        emit(ReplicaMove(primary, role, move.from_node_id, scratch_host, scratch=True))
        pending[primary] = ReplicaMove(primary, role, scratch_host, move.to_node_id)
        vacated_by.pop(move.from_node_id, None)
        vacated_by[scratch_host] = primary


def plan_moves(
    current_layout: Mapping[str, Placement],
    target_layout: Mapping[str, Placement],
    all_node_ids: Sequence[str],
    ftt: int,
) -> List[ReplicaMove]:
    """:func:`diff_layout` followed by :func:`order_moves`."""
    return order_moves(
        diff_layout(current_layout, target_layout, ftt),
        current_layout, all_node_ids, ftt)


def describe_plan(plan: DiversityPlan, moves: Sequence[ReplicaMove]) -> str:
    """One-line operator-facing summary, for the removal log."""
    scratch = sum(1 for m in moves if m.scratch)
    status = "fully domain-diverse" if plan.full_diversity else "DEGRADED"
    detail = f"{len(moves)} replica move(s)"
    if scratch:
        detail += f" ({scratch} scratch hop(s) to break rotation cycles)"
    if plan.notes:
        detail += "; " + "; ".join(plan.notes)
    if plan.violations:
        detail += f"; {len(plan.violations)} unresolved violation(s)"
    return f"{status}: {detail}"
