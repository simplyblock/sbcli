# coding=utf-8
"""Pure planning logic for single-node cluster expansion (FTT1 and FTT2).

When a single node is added to a cluster that already has its (primary,
secondary[, tertiary]) rotation set up by ``cluster_activate``, integrating the
newcomer requires re-homing some existing secondary/tertiary roles so that the
newcomer can take its primary slot and pick up its share of replicas. This
module computes that re-home plan as a list of :class:`RoleMove` operations,
ordered so that callers can execute them safely (create-before-destroy).

The module is intentionally pure — no DB access, no SPDK calls — so it can be
unit-tested in isolation without pulling in the rest of ``storage_node_ops``.
The orchestrator that consumes the returned plan lives elsewhere.

Layout assumption
-----------------
Both ``cluster_activate`` and the production layout produced by repeated
``cluster_expand`` calls follow a rotation: with ``k`` nodes ``n_1..n_k`` and
``k`` LVStores ``LVS_1..LVS_k`` (one primary per node), for each ``LVS_i``::

    primary(LVS_i)   = n_i
    secondary(LVS_i) = n_{(i mod k) + 1}
    tertiary(LVS_i)  = n_{((i+1) mod k) + 1}     # FTT2 only

Adding a single node ``n_{k+1}`` recomputes the same rotation with modulus
``k+1``, which shifts a small, deterministic set of roles.
"""

from typing import List, NamedTuple


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
        ``lvstore_stack_secondary_1`` / ``_2`` and the ``secondary_node_id`` /
        ``secondary_node_id_2`` pointers off the primary's node record.
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


def _rotation_layout(node_ids: List[str], ftt: int):
    """Yield ``(lvs_primary_id, sec_id, tert_id_or_empty)`` for a rotation.

    The rotation matches the layout produced by ``cluster_activate``. With ``k
    = len(node_ids)`` nodes and one LVS per node, ``LVS_i`` is owned by
    ``node_ids[i-1]``, its secondary is ``node_ids[i mod k]`` and its tertiary
    (FTT2 only) is ``node_ids[(i+1) mod k]``.
    """
    k = len(node_ids)
    for i, primary_id in enumerate(node_ids, start=1):
        sec_id = node_ids[i % k]
        tert_id = node_ids[(i + 1) % k] if ftt >= 2 else ""
        yield primary_id, sec_id, tert_id


def compute_role_diff(
    existing_node_ids: List[str],
    new_node_id: str,
    ftt: int,
) -> List[RoleMove]:
    """Plan the role moves to integrate ``new_node_id`` into the cluster.

    Parameters
    ----------
    existing_node_ids:
        Node IDs currently in the cluster, in rotation order. This is the
        order ``cluster_activate`` assigned LVStores in (``LVS_i`` is owned
        by ``existing_node_ids[i-1]``). Must contain ``ftt + 1`` or more
        entries (FTT1 ≥ 2, FTT2 ≥ 3).
    new_node_id:
        ID of the node being added. Must not be in ``existing_node_ids``.
    ftt:
        Cluster fault-tolerance level (1 or 2).

    Returns
    -------
    List[RoleMove]
        Moves in safe execution order:

        1. **Phase A — re-home existing roles** to the new node and the
           internal shifts they cascade. These move sec/tert away from a
           current donor onto a new recipient. Executed before any newcomer
           create-moves so existing LVStores never lose redundancy below FTT
           during the transition.

        2. **Phase B — create newcomer's roles**. The new node's primary LVS
           plus its secondary (and, for FTT2, tertiary) on the
           rotation-designated holders.

    Raises
    ------
    ValueError
        On invalid input: bad ``ftt``, newcomer already present, or cluster
        too small for the requested FTT.
    """
    if ftt not in (1, 2):
        raise ValueError(f"ftt must be 1 or 2, got {ftt}")
    if new_node_id == "":
        raise ValueError("new_node_id must be non-empty")
    if new_node_id in existing_node_ids:
        raise ValueError(
            f"new_node_id {new_node_id!r} already present in existing_node_ids")
    min_nodes = ftt + 1
    if len(existing_node_ids) < min_nodes:
        raise ValueError(
            f"cluster has {len(existing_node_ids)} nodes, "
            f"need at least {min_nodes} for FTT{ftt} before single-node expand")
    if len(set(existing_node_ids)) != len(existing_node_ids):
        raise ValueError("existing_node_ids contains duplicates")

    new_node_ids = list(existing_node_ids) + [new_node_id]

    # Build current and desired layouts keyed by the LVS's primary node id.
    # Newcomer's LVS appears only in the desired layout.
    current = {
        primary_id: (sec_id, tert_id)
        for primary_id, sec_id, tert_id in _rotation_layout(
            list(existing_node_ids), ftt)
    }
    desired = {
        primary_id: (sec_id, tert_id)
        for primary_id, sec_id, tert_id in _rotation_layout(new_node_ids, ftt)
    }

    phase_a: List[RoleMove] = []
    phase_b: List[RoleMove] = []

    for primary_id, (new_sec, new_tert) in desired.items():
        if primary_id == new_node_id:
            # Newcomer's own LVS: all create-moves, Phase B.
            phase_b.append(RoleMove(primary_id, ROLE_PRIMARY, "", primary_id))
            phase_b.append(RoleMove(primary_id, ROLE_SECONDARY, "", new_sec))
            if ftt >= 2:
                phase_b.append(
                    RoleMove(primary_id, ROLE_TERTIARY, "", new_tert))
            continue

        old_sec, old_tert = current[primary_id]
        if old_sec != new_sec:
            phase_a.append(
                RoleMove(primary_id, ROLE_SECONDARY, old_sec, new_sec))
        if ftt >= 2 and old_tert != new_tert:
            phase_a.append(
                RoleMove(primary_id, ROLE_TERTIARY, old_tert, new_tert))

    return phase_a + phase_b


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


def make_expand_state(new_node_id: str, moves: List[RoleMove]) -> dict:
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


def pending_moves(state: dict) -> List[RoleMove]:
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


def is_expand_state_compatible(state: dict) -> bool:
    """Cheap version-check before resuming a persisted state. Returns True
    only when the schema_version matches the current code's expected version
    — older or newer states should be flagged by the orchestrator before any
    moves are executed."""
    if not state:
        return True  # empty state is always compatible
    return state.get("schema_version") == EXPAND_STATE_SCHEMA_VERSION
