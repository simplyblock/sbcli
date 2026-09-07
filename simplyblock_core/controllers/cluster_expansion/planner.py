"""Pure planning logic for cluster expansion (FTT1 and FTT2).

When new node(s) are added to a cluster that already has its (primary,
secondary[, tertiary]) rotation set up by ``cluster_activate``, integrating the
newcomers requires re-homing some existing secondary/tertiary roles so that the
newcomers can take their primary slots and pick up their share of replicas.
This module computes that re-home plan as a list of :class:`RoleMove`
operations, ordered so that callers can execute them safely
(create-before-destroy).

The module is intentionally pure — no DB access, no SPDK calls — so it can be
unit-tested in isolation without pulling in the rest of ``storage_node_ops``.
The orchestrator that consumes the returned plan lives elsewhere.

Layout assumption
-----------------
Both ``cluster_activate`` and the production layout produced by repeated
``cluster_expand`` calls follow a **host-rotation**: with ``H`` hosts and a
uniform ``p`` storage nodes per host (so ``k = H * p`` total nodes and ``k``
LVStores), the node at slot ``s`` of host ``i`` is primary of
``LVS_{i*p + s}`` and its secondary/tertiary live on the *same slot* of the
next/next-next host::

    primary(LVS_{i*p+s})   = host_i.slot_s
    secondary(LVS_{i*p+s}) = host_{(i+1) mod H}.slot_s
    tertiary(LVS_{i*p+s})  = host_{(i+2) mod H}.slot_s   # FTT2 only

Same-slot placement keeps primary/sec/tert host-disjoint by construction —
the invariant that makes a single host loss tolerable. The original
single-node-per-host design is the ``p=1`` special case of this (``+1`` /
``+2`` node stride is the same as ``+1`` / ``+2`` host stride when
``p=1``).

Two entry points:

* :func:`compute_role_diff` — flat-list, 1-node-per-host special case. Kept
  for back-compat with the original single-node-expansion entry point.
* :func:`compute_role_diff_topology` — the general host-rotation planner.
  Use this for multi-node-per-host clusters (``p>=2``) or whenever the
  caller needs to express the host topology explicitly.
"""

from typing import NamedTuple
from collections.abc import Sequence


# Role names match the wire protocol used by bdev_lvol_set_lvs_opts.
ROLE_PRIMARY = "primary"
ROLE_SECONDARY = "secondary"
ROLE_TERTIARY = "tertiary"


# Lifecycle phases for ``Cluster.expand_state['phase']``.
EXPAND_PHASE_IN_PROGRESS = "in_progress"
EXPAND_PHASE_COMPLETED = "completed"
EXPAND_PHASE_ABORTED = "aborted"

# Schema version for the persisted ``Cluster.expand_state`` blob. Bump on any
# breaking change to the move serialization format so the orchestrator can
# refuse to resume a state it doesn't understand instead of silently
# misinterpreting older fields.
EXPAND_STATE_SCHEMA_VERSION = 1


class RoleMove(NamedTuple):
    """A single role transition planned for a cluster expand.

    Attributes
    ----------
    lvs_primary_node_id:
        The node that owns (is primary for) the LVS this move concerns. The
        LVS itself is identified by its primary because the DB models hang
        the ``lvstore_stack_secondary`` / ``lvstore_stack_tertiary``
        back-references on the holder and the ``secondary_node_id`` /
        ``tertiary_node_id`` pointers off the primary's node record.
    role:
        ``"primary"``, ``"secondary"``, or ``"tertiary"``. ``"primary"`` is
        used only for newcomer-create moves (the new node creating its own
        primary LVS) — this codebase does not migrate primaries via this
        planner.
    from_node_id:
        Donor node giving up the role. Empty string when the role is being
        newly created (no donor).
    to_node_id:
        Recipient node taking the role.
    """

    lvs_primary_node_id: str
    role: str
    from_node_id: str
    to_node_id: str

    @property
    def is_create(self) -> bool:
        """True when this move creates a new role (no donor to tear down)."""
        return self.from_node_id == ""


def _host_rotation_layout(hosts: list[list[str]], ftt: int):
    """Yield ``(lvs_primary_id, sec_id, tert_id_or_empty)`` for a host
    rotation.

    ``hosts`` is the list of hosts in rotation order, each host being a list
    of node IDs in slot order. With ``H = len(hosts)`` and uniform
    ``p = len(hosts[0])``, the node at ``hosts[i][s]`` is primary of
    ``LVS_{i*p + s}``; its secondary is ``hosts[(i+1) mod H][s]`` and (FTT2)
    tertiary is ``hosts[(i+2) mod H][s]`` — i.e., the same slot on the
    next/next-next host. Same-slot placement preserves host-disjointness
    by construction.

    Caller is responsible for validating uniform ``p`` —
    :func:`compute_role_diff_topology` does this before calling here.
    """
    H = len(hosts)
    if H == 0:
        return
    for i, host in enumerate(hosts):
        sec_host = hosts[(i + 1) % H]
        tert_host = hosts[(i + 2) % H] if ftt >= 2 else None
        for s, primary_id in enumerate(host):
            yield (primary_id,
                   sec_host[s],
                   (tert_host[s] if tert_host is not None else ""))


def _rotation_layout(node_ids: list[str], ftt: int):
    """1-node-per-host rotation. Special case of :func:`_host_rotation_layout`.

    Kept as a thin wrapper so existing callers and tests that imported this
    helper before the host-aware refactor continue to work without change.
    """
    yield from _host_rotation_layout([[n] for n in node_ids], ftt)


def _validate_topology(topology: list[list[str]], label: str) -> None:
    """Reject obviously-malformed host topologies before they reach the
    rotation helper. Enforced invariants:

    * non-empty
    * every host has the same number of slots (uniform ``p``)
    * every node id is non-empty and globally unique within the topology
    """
    if not topology:
        raise ValueError(f"{label} must be non-empty")
    p = len(topology[0])
    if p == 0:
        raise ValueError(f"{label} hosts must have at least one slot")
    seen = set()
    for i, host in enumerate(topology):
        if len(host) != p:
            raise ValueError(
                f"{label} requires uniform nodes_per_host; host index {i} "
                f"has {len(host)} nodes, expected {p}")
        for s, node_id in enumerate(host):
            if not node_id:
                raise ValueError(
                    f"{label} host {i} slot {s} has an empty node id")
            if node_id in seen:
                raise ValueError(
                    f"{label} contains duplicate node id {node_id!r}")
            seen.add(node_id)


def compute_role_diff_topology(
    current_topology: list[list[str]],
    new_topology: list[list[str]],
    ftt: int,
    *,
    current_layout: dict[str, tuple[str, str]] | None = None,
) -> list[RoleMove]:
    """Plan the role moves to integrate one or more new hosts.

    Parameters
    ----------
    current_topology:
        Hosts currently in the cluster, in rotation order. Each host is a
        list of node IDs in slot order. ``p = nodes_per_host`` must be
        uniform across all hosts. Must contain at least ``ftt + 1`` hosts
        (so primary, secondary[, tertiary] can be placed on distinct
        physical hosts).
    new_topology:
        The post-add topology. Must contain ``current_topology`` as a strict
        prefix (existing hosts keep their position and slot order); new
        hosts appear after the existing ones. Total host count must exceed
        ``len(current_topology)`` — there's no work to do otherwise.
    ftt:
        Cluster fault-tolerance level (1 or 2).
    current_layout:
        Optional override of "what the actual current layout is" — when
        omitted, it's computed from ``current_topology`` via the rotation
        formula. When supplied (typically built from each primary's
        ``secondary_node_id`` / ``_2`` fields read from the DB), the planner
        uses it directly as the *before* state. This defends against drift
        between ``cluster_activate``'s helper-driven placement and the
        idealized rotation, and lets the planner detect mismatches early.

    Returns
    -------
    List[RoleMove]
        Moves in Phase A (re-home) → Phase B (newcomer creates) order.

    Raises
    ------
    ValueError
        On invalid input: bad ``ftt``, malformed topology, non-uniform ``p``,
        too few hosts for ``ftt``, ``new_topology`` not a prefix-extension of
        ``current_topology``, or duplicate node IDs.
    """
    if ftt not in (1, 2):
        raise ValueError(f"ftt must be 1 or 2, got {ftt}")

    _validate_topology(current_topology, "current_topology")
    _validate_topology(new_topology, "new_topology")

    p_current = len(current_topology[0])
    p_new = len(new_topology[0])
    if p_current != p_new:
        raise ValueError(
            f"nodes_per_host must match between current ({p_current}) and "
            f"new ({p_new}) topologies")

    H_current = len(current_topology)
    H_new = len(new_topology)
    if H_new <= H_current:
        raise ValueError(
            f"new_topology has {H_new} hosts; must be more than "
            f"current_topology's {H_current}")
    if H_current < ftt + 1:
        raise ValueError(
            f"current_topology has {H_current} hosts; need at least "
            f"{ftt + 1} for FTT{ftt} before expansion")

    # Existing hosts must keep their position and slot composition. Allowing
    # reordering would silently shift everyone's rotation and invalidate the
    # diff — refusing here surfaces it as an operator error instead.
    for i, host in enumerate(current_topology):
        if new_topology[i] != host:
            raise ValueError(
                f"new_topology host {i} differs from current_topology; "
                f"existing hosts must keep their position and slot order "
                f"(current={host!r}, new={new_topology[i]!r})")

    # Cross-topology duplicate check: a node id may not appear in two slots.
    all_new = [n for h in new_topology for n in h]
    if len(set(all_new)) != len(all_new):
        raise ValueError("new_topology contains duplicate node ids")

    current_nodes = {n for h in current_topology for n in h}
    newcomers = set(all_new) - current_nodes

    if current_layout is None:
        current_layout = {
            primary_id: (sec_id, tert_id)
            for primary_id, sec_id, tert_id in _host_rotation_layout(
                current_topology, ftt)
        }
    desired = {
        primary_id: (sec_id, tert_id)
        for primary_id, sec_id, tert_id in _host_rotation_layout(
            new_topology, ftt)
    }

    phase_a: list[RoleMove] = []
    phase_b: list[RoleMove] = []

    for primary_id, (new_sec, new_tert) in desired.items():
        if primary_id in newcomers:
            # Newcomer's own LVS: all create-moves, Phase B.
            phase_b.append(RoleMove(primary_id, ROLE_PRIMARY, "", primary_id))
            phase_b.append(RoleMove(primary_id, ROLE_SECONDARY, "", new_sec))
            if ftt >= 2:
                phase_b.append(
                    RoleMove(primary_id, ROLE_TERTIARY, "", new_tert))
            continue

        old_sec, old_tert = current_layout[primary_id]
        if old_sec != new_sec:
            phase_a.append(
                RoleMove(primary_id, ROLE_SECONDARY, old_sec, new_sec))
        if ftt >= 2 and old_tert != new_tert:
            phase_a.append(
                RoleMove(primary_id, ROLE_TERTIARY, old_tert, new_tert))

    return phase_a + phase_b


def compute_role_diff(
    existing_node_ids: list[str],
    new_node_id: str,
    ftt: int,
) -> list[RoleMove]:
    """Plan the role moves to integrate ``new_node_id`` into the cluster.

    This is the **1-node-per-host** special case of
    :func:`compute_role_diff_topology`, kept as the back-compat entry point
    for callers that still reason about the cluster as a flat node list.
    For multi-node-per-host clusters (``p >= 2``), call
    :func:`compute_role_diff_topology` directly with the explicit host
    topology — calling this function with a flat list would silently treat
    each node as living on its own host and produce a plan that ignores
    physical host boundaries.

    Parameters
    ----------
    existing_node_ids:
        Node IDs currently in the cluster, in rotation order (one per host).
        Must contain ``ftt + 1`` or more entries (FTT1 ≥ 2, FTT2 ≥ 3).
    new_node_id:
        ID of the node being added. Must not be in ``existing_node_ids``.
    ftt:
        Cluster fault-tolerance level (1 or 2).

    Returns
    -------
    List[RoleMove]
        Moves in safe execution order:

        1. **Phase A — re-home existing roles** to the new node and the
           internal shifts they cascade. Executed before any newcomer
           create-moves so existing LVStores never lose redundancy below
           FTT during the transition.
        2. **Phase B — create newcomer's roles** — primary LVS plus its
           secondary (and, for FTT2, tertiary) on the rotation-designated
           holders.

    Raises
    ------
    ValueError
        On invalid input: bad ``ftt``, newcomer already present, cluster
        too small for the requested FTT, or duplicates in
        ``existing_node_ids``.
    """
    # Keep the explicit early-exit messages for back-compat with the
    # original API; the rest is delegated to compute_role_diff_topology
    # via the singleton-host topology that 1-per-host implies.
    if new_node_id == "":
        raise ValueError("new_node_id must be non-empty")
    if new_node_id in existing_node_ids:
        raise ValueError(
            f"new_node_id {new_node_id!r} already present in existing_node_ids")

    current_topology = [[n] for n in existing_node_ids]
    new_topology = current_topology + [[new_node_id]]
    return compute_role_diff_topology(current_topology, new_topology, ftt)


# ---------------------------------------------------------------------------
# Failure-domain policy helpers (pure).
#
# Invariant model: with failure domains enabled, secondary/tertiary *role*
# placement only affects availability (client paths / promotion), never
# durability — data-chunk anti-affinity and the HA-JM per-domain caps are
# enforced independently by the data plane and the JM picker. The role
# invariant we hold is therefore:
#
#   every LVS keeps AT LEAST ONE cross-domain non-leader role
#   (its secondary or its tertiary lives in a different failure domain
#   than its primary)
#
# so that a full-domain outage never leaves an LVS with zero surviving
# paths while its data is still readable in the surviving domain. With two
# domains and three roles the pigeonhole forces one co-located pair per
# LVS; the interleaved rotation below keeps that pair deterministic and
# confines "secondary same-domain as primary" to at most the odd host when
# the domain populations differ by one.
#
# Topology admission (the ``max |n_a - n_b| <= 1`` rule and the >=2 hosts
# per domain floor) is validated by :func:`fd_balance_violation`; callers
# (add/remove/activate) decide which bound applies to their operation.
# ---------------------------------------------------------------------------


def fd_interleaved_host_order(
    host_fd_pairs: Sequence[tuple[str, int]],
) -> list[str]:
    """Order hosts round-robin across failure domains.

    ``host_fd_pairs`` is a list of ``(host_key, failure_domain)`` in a
    deterministic base order (the caller fixes tie-breaking, e.g. by
    create-time or id). Domains are cycled in ascending id order; within a
    domain, hosts keep their base order.

    With equal per-domain populations the result alternates perfectly, so
    the ``(i+1) mod H`` rotation places every secondary in a different
    domain than its primary. With a one-host imbalance the larger domain
    contributes the final host, producing exactly one same-domain
    adjacency (at the cyclic wrap) — the single degraded LVS the +/-1
    policy accepts, whose tertiary the rotation still lands cross-domain.
    """
    by_fd: dict[int, list[str]] = {}
    for host, fd in host_fd_pairs:
        by_fd.setdefault(fd, []).append(host)
    fds = sorted(by_fd)
    order: list[str] = []
    for round_idx in range(max((len(v) for v in by_fd.values()), default=0)):
        for fd in fds:
            if round_idx < len(by_fd[fd]):
                order.append(by_fd[fd][round_idx])
    return order


def rotation_layout(
    topology: list[list[str]],
    ftt: int,
) -> dict[str, tuple[str, str]]:
    """Public wrapper around the host-rotation formula: the desired
    ``primary -> (secondary, tertiary)`` layout for ``topology`` (tertiary
    is ``""`` for FTT1). Validates the topology first (uniform slots per
    host, unique non-empty ids) and raises ``ValueError`` on malformed
    input. Used by cluster activation to assign roles deterministically
    instead of greedily."""
    if ftt not in (1, 2):
        raise ValueError(f"ftt must be 1 or 2, got {ftt}")
    _validate_topology(topology, "topology")
    if len(topology) < ftt + 1:
        raise ValueError(
            f"topology has {len(topology)} hosts; need at least {ftt + 1} "
            f"for FTT{ftt} (roles must land on distinct hosts)")
    return {
        primary_id: (sec_id, tert_id)
        for primary_id, sec_id, tert_id in _host_rotation_layout(
            topology, ftt)
    }


def compute_fd_layout_violations(
    topology: list[list[str]],
    ftt: int,
    fd_by_node: dict[str, int],
    *,
    layout: dict[str, tuple[str, str]] | None = None,
) -> list[str]:
    """Check the >=1-cross-domain-role invariant over a (desired) layout.

    Evaluates the rotation layout implied by ``topology`` (or an explicit
    ``layout`` of ``primary -> (sec, tert)`` when given, e.g. the *actual*
    layout read from DB pointers) against ``fd_by_node``. Nodes with an
    unset domain (< 0 / missing) are skipped — the feature is off for
    them. A role whose holder has an unset domain does NOT count as
    cross-domain (unknown is not disjoint).

    Returns a list of human-readable violation strings; empty means the
    layout satisfies the invariant.
    """
    if layout is None:
        layout = {
            primary_id: (sec_id, tert_id)
            for primary_id, sec_id, tert_id in _host_rotation_layout(
                topology, ftt)
        }
    violations: list[str] = []
    for primary_id, (sec_id, tert_id) in layout.items():
        fd_p = fd_by_node.get(primary_id, -1)
        if fd_p < 0:
            continue
        fd_s = fd_by_node.get(sec_id, -1) if sec_id else -1
        fd_t = fd_by_node.get(tert_id, -1) if tert_id else -1
        sec_cross = fd_s >= 0 and fd_s != fd_p
        tert_cross = ftt >= 2 and fd_t >= 0 and fd_t != fd_p
        if not sec_cross and not tert_cross:
            violations.append(
                f"LVS@{primary_id} (fd={fd_p}) keeps no cross-domain role: "
                f"secondary={sec_id or '-'} (fd={fd_s}), "
                f"tertiary={tert_id or '-'} (fd={fd_t})")
    return violations


def fd_balance_violation(
    fd_host_counts: dict[int, int],
    *,
    max_delta: int = 1,
    min_hosts_per_fd: int | None = None,
) -> str | None:
    """Validate per-domain host counts against the balance policy.

    ``fd_host_counts`` maps failure-domain id (>= 0) to the number of
    distinct hosts it would contain AFTER the operation under admission.
    Returns a human-readable reason on violation, ``None`` when the
    counts are acceptable. Empty counts (feature unused) are acceptable.

    ``max_delta`` is the allowed population spread (the +/-1 rule).
    ``min_hosts_per_fd``, when given, is the per-domain floor (2 for any
    operation on an activated HA cluster: below two hosts per domain the
    2-2 HA-journal split and cross-domain role hosting both collapse).
    """
    counts = {fd: c for fd, c in fd_host_counts.items() if fd >= 0}
    if not counts:
        return None
    lo, hi = min(counts.values()), max(counts.values())
    if hi - lo > max_delta:
        return (
            f"failure domains would be unbalanced by {hi - lo} hosts "
            f"(allowed: {max_delta}); populations: "
            f"{ {fd: counts[fd] for fd in sorted(counts)} }. Restore "
            f"balance before the next topology change.")
    if min_hosts_per_fd is not None:
        for fd in sorted(counts):
            if counts[fd] < min_hosts_per_fd:
                return (
                    f"failure domain {fd} would drop to {counts[fd]} "
                    f"host(s); at least {min_hosts_per_fd} are required "
                    f"per domain.")
    return None


def fd_activation_domain_count_violation(
    npcs: int, distinct_domain_count: int,
) -> str | None:
    """Validate the number of distinct failure domains for fresh activation.

    A 2-FD layout can never absorb a second independent failure once one
    domain is fully down (confirmed with the backend team), so it is not
    supported at any npcs level.

    The bare *correctness* minimum for the rotation layout itself is
    npcs+1 (e.g. 2 domains for npcs=1, 3 for npcs=2 -- below that even the
    initial static placement is wrong: at exactly 2 domains the tertiary
    role mathematically always lands back in the primary's own domain,
    since "2 steps ahead" in a period-2 round-robin wraps to where it
    started; verified directly against rotation_layout()). But a
    minimum-correct STATIC layout has zero spare hosts per domain, and the
    moment a single node is added or removed, the relocation logic
    (_pick_replica_relocation_node) has no spare candidate left to
    reassign the stranded role to -- verified directly: removing one node
    from a bare-minimum npcs=1/2-domain or npcs=2/3-domain layout strands
    another node's secondary/tertiary with no replacement at all, blocking
    the removal outright rather than just degrading placement quality.

    Requiring npcs+2 domains (3 for npcs=1, 4 for npcs=2, which also rules
    out exactly 2 for both) keeps one domain of spare capacity beyond the
    bare correctness floor, so a single add/remove has somewhere to place
    the relocated role instead of failing immediately. Returns a
    human-readable reason on violation, ``None`` when the count is
    acceptable.
    """
    min_domains = npcs + 2
    if distinct_domain_count < min_domains:
        return (
            f"failure domains are enabled with npcs={npcs}, which requires at "
            f"least {min_domains} distinct failure domains (2 domains is not "
            f"supported at any npcs level); currently have "
            f"{distinct_domain_count}. Add hosts in additional domains, or "
            f"disable failure domains, then activate.")
    return None


# ---------------------------------------------------------------------------
# Persistence helpers for ``Cluster.expand_state``.
#
# These are pure: they take and return plain dicts. The orchestrator owns the
# actual ``cluster.expand_state = ...; cluster.write_to_db()`` cycle. Splitting
# it this way keeps the planner unit-testable without DB / FoundationDB mocks
# and makes the on-disk schema explicit.
#
# Persisted schema (``Cluster.expand_state`` JSON):
#
#     {
#         "schema_version": 1,
#         "phase": "in_progress" | "completed" | "aborted",
#         "new_node_id": "<uuid>",
#         "moves": [ {"lvs_primary_node_id", "role",
#                     "from_node_id", "to_node_id"}, ... ],
#         "cursor": <int>,         # index of the next move to execute
#         "abort_reason": "<str>"  # only present when phase == "aborted"
#     }
#
# The empty dict ``{}`` is the in-DB representation of "no expansion in
# flight" and is what newly-created Cluster records carry by default.
# ---------------------------------------------------------------------------


def move_to_dict(move: RoleMove) -> dict:
    """Serialize a :class:`RoleMove` to a JSON-safe dict."""
    return {
        "lvs_primary_node_id": move.lvs_primary_node_id,
        "role": move.role,
        "from_node_id": move.from_node_id,
        "to_node_id": move.to_node_id,
    }


def move_from_dict(data: dict) -> RoleMove:
    """Deserialize a :class:`RoleMove` from a persisted dict."""
    return RoleMove(
        lvs_primary_node_id=data["lvs_primary_node_id"],
        role=data["role"],
        from_node_id=data["from_node_id"],
        to_node_id=data["to_node_id"],
    )


def make_expand_state(new_node_id: str, moves: list[RoleMove]) -> dict:
    """Build the initial ``Cluster.expand_state`` for a fresh expansion.

    Parameters
    ----------
    new_node_id:
        ID of the node being added (the newcomer whose primary LVS will
        come up at the end of Phase B).
    moves:
        The full ordered move list as returned by :func:`compute_role_diff`.

    Returns
    -------
    dict
        A new state dict in phase ``in_progress`` with ``cursor`` at 0.
    """
    if new_node_id == "":
        raise ValueError("new_node_id must be non-empty")
    return {
        "schema_version": EXPAND_STATE_SCHEMA_VERSION,
        "phase": EXPAND_PHASE_IN_PROGRESS,
        "new_node_id": new_node_id,
        "moves": [move_to_dict(m) for m in moves],
        "cursor": 0,
    }


def is_expand_in_progress(state: dict) -> bool:
    """True iff there is an in-flight expansion to resume or continue."""
    return bool(state) and state.get("phase") == EXPAND_PHASE_IN_PROGRESS


def pending_moves(state: dict) -> list[RoleMove]:
    """Return the moves at and after ``state['cursor']`` — the work that
    remains. Returns an empty list when the expansion is finished or the
    state is empty/aborted."""
    if not is_expand_in_progress(state):
        return []
    cursor = state.get("cursor", 0)
    return [move_from_dict(d) for d in state["moves"][cursor:]]


def expand_state_advance(state: dict) -> dict:
    """Return a new state dict with the cursor advanced by one. The caller is
    responsible for writing it back to the DB.

    Raises
    ------
    ValueError
        If the state is not in_progress or the cursor is already past the
        last move.
    """
    if not is_expand_in_progress(state):
        raise ValueError(
            f"cannot advance: state is not in_progress (phase="
            f"{state.get('phase')!r})")
    cursor = state.get("cursor", 0)
    if cursor >= len(state["moves"]):
        raise ValueError("cannot advance: cursor already past last move")
    new_state = dict(state)
    new_state["cursor"] = cursor + 1
    return new_state


def expand_state_complete(state: dict) -> dict:
    """Return a state dict marked as ``completed``. Idempotent if already
    completed; rejects partially-executed states (cursor below the move
    count) so we don't silently mark a half-done expansion done."""
    if not state:
        raise ValueError("cannot complete: state is empty")
    if state.get("phase") == EXPAND_PHASE_COMPLETED:
        return dict(state)
    if state.get("phase") != EXPAND_PHASE_IN_PROGRESS:
        raise ValueError(
            f"cannot complete: phase is {state.get('phase')!r}, "
            f"expected {EXPAND_PHASE_IN_PROGRESS!r}")
    if state.get("cursor", 0) != len(state.get("moves", [])):
        raise ValueError(
            f"cannot complete: cursor at {state.get('cursor')} but "
            f"{len(state.get('moves', []))} moves planned")
    new_state = dict(state)
    new_state["phase"] = EXPAND_PHASE_COMPLETED
    return new_state


def expand_state_abort(state: dict, reason: str) -> dict:
    """Return a state dict marked as ``aborted`` with the given reason.
    The cursor and move list are preserved for post-hoc inspection."""
    if not state:
        raise ValueError("cannot abort: state is empty")
    if not reason:
        raise ValueError("abort reason must be non-empty")
    new_state = dict(state)
    new_state["phase"] = EXPAND_PHASE_ABORTED
    new_state["abort_reason"] = reason
    return new_state


def expand_state_rearm(state: dict) -> dict:
    """Return an ``aborted`` state flipped back to ``in_progress`` so the
    orchestrator resumes it from the preserved cursor on the next attempt.

    Used by the task runner to retry a failed expansion by *resuming* the
    same plan (re-attempting the move the cursor points at) rather than
    recomputing a fresh role diff against a topology that the aborted run
    may have already partially mutated.

    The ``abort_reason`` is cleared (kept under ``last_abort_reason`` for
    history). Idempotent on an already-in-progress state.

    Raises
    ------
    ValueError
        If the state is empty or in a terminal ``completed`` phase (a
        finished expansion has nothing to resume).
    """
    if not state:
        raise ValueError("cannot rearm: state is empty")
    phase = state.get("phase")
    if phase == EXPAND_PHASE_IN_PROGRESS:
        return dict(state)
    if phase != EXPAND_PHASE_ABORTED:
        raise ValueError(
            f"cannot rearm: phase is {phase!r}, expected "
            f"{EXPAND_PHASE_ABORTED!r}")
    new_state = dict(state)
    new_state["phase"] = EXPAND_PHASE_IN_PROGRESS
    if "abort_reason" in new_state:
        new_state["last_abort_reason"] = new_state.pop("abort_reason")
    return new_state


def is_expand_state_compatible(state: dict) -> bool:
    """Cheap version-check before resuming a persisted state. Returns True
    only when the schema_version matches the current code's expected version
    — older or newer states should be flagged by the orchestrator before any
    moves are executed."""
    if not state:
        return True  # empty state is always compatible
    return state.get("schema_version") == EXPAND_STATE_SCHEMA_VERSION
