# coding=utf-8
"""Orchestrator for single-node cluster expansion.

Consumes a list of :class:`~simplyblock_core.cluster_expand_planner.RoleMove`
objects (produced by ``compute_role_diff``) and drives them to completion,
persisting a resumability cursor on the cluster's
:attr:`~simplyblock_core.models.cluster.Cluster.expand_state` field after every
step.

Design split
------------
* The **orchestrator loop** in this module is side-effect-free except for
  calling ``executor.execute(move)`` and ``cluster.write_to_db()``. It does no
  SPDK or RPC calls itself, so it can be unit-tested by passing in a stub
  executor and a fake cluster object.
* The **executor** is an abstract :class:`MoveExecutor` whose concrete SPDK
  implementation lives elsewhere (added in a later step). Splitting it this
  way keeps the cursor / persistence / abort logic decoupled from the heavy
  parts (``recreate_lvstore_on_sec``, ``teardown_non_leader_lvstore``, JC
  quorum gating) and lets us swap in a no-op executor for dry-run.

Resume semantics
----------------
``execute_expand_plan`` checks ``cluster.expand_state`` first:

* If a state is already in progress, the orchestrator **resumes** from the
  persisted cursor and ignores ``planned_moves`` / ``new_node_id`` (those
  describe a *new* plan). Refusing to overwrite an in-progress plan prevents
  silent loss of partial work after a mgmt-node crash.
* Otherwise a fresh state is built from ``planned_moves`` and ``new_node_id``,
  persisted, and executed from cursor 0.

If the persisted state's schema version does not match the current code's
expected version, the orchestrator refuses to touch it — better to surface
the mismatch to the operator than to misinterpret a future schema.
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from simplyblock_core import utils
from simplyblock_core.cluster_expand_planner import (
    RoleMove,
    expand_state_abort,
    expand_state_advance,
    expand_state_complete,
    is_expand_in_progress,
    is_expand_state_compatible,
    make_expand_state,
    pending_moves,
)


logger = utils.get_logger(__name__)


class MoveExecutor(ABC):
    """Side-effect interface for executing a single :class:`RoleMove`.

    A concrete executor wraps the SPDK / DB glue needed to perform a move:
    creating a new sec/tert stack on a recipient, tearing down the donor,
    updating the primary's ``secondary_node_id`` pointer, and so on.

    Implementations must raise on any unrecoverable failure — the
    orchestrator catches exceptions and records them as the abort reason.
    Returning normally signals success and lets the orchestrator advance the
    cursor.
    """

    @abstractmethod
    def execute(self, move: RoleMove) -> None:
        """Execute a single move. Raise on failure."""
        raise NotImplementedError


class NoopMoveExecutor(MoveExecutor):
    """Test / dry-run executor: records calls without performing them.

    Useful for unit tests (verify the orchestrator's loop and persistence
    behavior) and for the planned ``--dry-run`` CLI flag (print the plan
    without touching SPDK).
    """

    def __init__(self) -> None:
        self.executed: List[RoleMove] = []

    def execute(self, move: RoleMove) -> None:
        self.executed.append(move)


class ExpandPlanError(Exception):
    """Raised by ``execute_expand_plan`` when a fatal precondition is not
    met (e.g., trying to start a fresh plan while one is already in
    progress, or resuming an incompatible-schema state). Distinct from the
    underlying executor exception that triggers an abort, which is
    re-raised as-is after the abort is persisted."""


def execute_expand_plan(
    cluster,
    executor: MoveExecutor,
    *,
    planned_moves: Optional[List[RoleMove]] = None,
    new_node_id: Optional[str] = None,
) -> None:
    """Drive the expansion to completion, persisting cursor progress as we go.

    Parameters
    ----------
    cluster:
        A ``Cluster`` model instance. Mutated: ``cluster.expand_state`` is
        updated and ``cluster.write_to_db()`` is called after every cursor
        movement and on terminal phase changes.
    executor:
        :class:`MoveExecutor` implementation. Called once per pending move.
    planned_moves, new_node_id:
        Required when starting a fresh expansion (``cluster.expand_state``
        is empty). Ignored when resuming (an in-progress state already
        contains the move list).

    Raises
    ------
    ExpandPlanError
        On precondition failures — fresh start without a plan, or attempt
        to start while an in-progress state already exists, or schema
        mismatch on resume.
    Exception
        Any exception raised by the executor is caught, recorded as the
        abort reason on the persisted state, and then re-raised so the
        caller sees the underlying failure.
    """
    state = cluster.expand_state or {}

    if is_expand_in_progress(state):
        if not is_expand_state_compatible(state):
            raise ExpandPlanError(
                f"persisted expand_state schema_version="
                f"{state.get('schema_version')!r} is not understood by this "
                f"version of the orchestrator; manual operator action required")
        if planned_moves is not None or new_node_id is not None:
            # Surface this rather than silently ignore — the operator should
            # know they're about to resume an existing plan, not start a new
            # one. The orchestrator could be permissive here, but a strict
            # contract makes the resume path obvious in logs.
            raise ExpandPlanError(
                "an expansion is already in progress; do not pass "
                "planned_moves/new_node_id when resuming")
        logger.info(
            f"resuming expansion: new_node_id={state['new_node_id']} "
            f"cursor={state['cursor']}/{len(state['moves'])}")
    else:
        if planned_moves is None or new_node_id is None:
            raise ExpandPlanError(
                "no in-progress expansion to resume; planned_moves and "
                "new_node_id are required to start a fresh plan")
        state = make_expand_state(new_node_id, planned_moves)
        cluster.expand_state = state
        cluster.write_to_db()
        logger.info(
            f"starting expansion: new_node_id={new_node_id} "
            f"moves={len(planned_moves)}")

    while True:
        pending = pending_moves(state)
        if not pending:
            break
        move = pending[0]
        logger.info(
            f"expand cursor {state['cursor']}/{len(state['moves'])}: "
            f"executing {move.role} move for LVS@{move.lvs_primary_node_id} "
            f"({move.from_node_id or '-'} -> {move.to_node_id})")
        try:
            executor.execute(move)
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
            logger.error(
                f"expand aborted at cursor {state['cursor']}: {reason}")
            cluster.expand_state = expand_state_abort(state, reason=reason)
            cluster.write_to_db()
            raise

        state = expand_state_advance(state)
        cluster.expand_state = state
        cluster.write_to_db()

    cluster.expand_state = expand_state_complete(state)
    cluster.write_to_db()
    logger.info(
        f"expansion complete: new_node_id={state['new_node_id']} "
        f"({len(state['moves'])} moves)")
