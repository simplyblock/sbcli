# coding=utf-8
"""Unit tests for ``cluster_expand_orchestrator.execute_expand_plan``.

These exercise the cursor / persistence / abort loop without touching SPDK.
A fake Cluster object captures every ``write_to_db`` invocation so we can
assert the orchestrator persists state at each step (essential for
crash-resume correctness).
"""

import unittest
from typing import List

from simplyblock_core.cluster_expand_orchestrator import (
    ExpandPlanError,
    MoveExecutor,
    NoopMoveExecutor,
    execute_expand_plan,
)
from simplyblock_core.cluster_expand_planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    EXPAND_PHASE_IN_PROGRESS,
    EXPAND_STATE_SCHEMA_VERSION,
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RoleMove,
    compute_role_diff,
    make_expand_state,
)


class FakeCluster:
    """Stand-in for ``models.cluster.Cluster``. Captures every write_to_db
    snapshot so tests can assert per-step persistence."""

    def __init__(self, expand_state=None):
        self.uuid = "cluster-fake"
        self.expand_state = expand_state or {}
        self.write_history: List[dict] = []

    def write_to_db(self):
        # Snapshot the current state at write time so we can assert ordering
        # and that the cursor was persisted before the next executor call.
        import copy
        self.write_history.append(copy.deepcopy(self.expand_state))


class FailingExecutor(MoveExecutor):
    """Executes successfully for the first ``fail_at`` moves, then raises."""

    def __init__(self, fail_at: int, exc: Exception):
        self.fail_at = fail_at
        self.exc = exc
        self.executed: List[RoleMove] = []

    def execute(self, move: RoleMove) -> None:
        if len(self.executed) == self.fail_at:
            raise self.exc
        self.executed.append(move)


class CursorObservingExecutor(MoveExecutor):
    """Captures the cluster's cursor at the moment of each execute() call —
    used to assert that the orchestrator advances the cursor *after* the
    executor returns, not before."""

    def __init__(self, cluster: FakeCluster):
        self.cluster = cluster
        self.cursors_seen: List[int] = []

    def execute(self, move: RoleMove) -> None:
        self.cursors_seen.append(self.cluster.expand_state["cursor"])


# ---------------------------------------------------------------------------
# 1. Happy path — fresh start, all moves complete
# ---------------------------------------------------------------------------

class TestFreshStartHappyPath(unittest.TestCase):

    def setUp(self):
        self.moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        self.cluster = FakeCluster()
        self.executor = NoopMoveExecutor()

    def test_executes_every_move_in_order(self):
        execute_expand_plan(
            self.cluster, self.executor,
            planned_moves=self.moves, new_node_id="n5")
        self.assertEqual(self.executor.executed, self.moves)

    def test_terminal_state_is_completed(self):
        execute_expand_plan(
            self.cluster, self.executor,
            planned_moves=self.moves, new_node_id="n5")
        self.assertEqual(
            self.cluster.expand_state["phase"], EXPAND_PHASE_COMPLETED)
        self.assertEqual(
            self.cluster.expand_state["cursor"], len(self.moves))

    def test_writes_initial_state_before_any_execute(self):
        # First write must happen with cursor=0 in the in_progress phase, so
        # that a crash before move 0 leaves a recoverable record.
        execute_expand_plan(
            self.cluster, self.executor,
            planned_moves=self.moves, new_node_id="n5")
        first = self.cluster.write_history[0]
        self.assertEqual(first["phase"], EXPAND_PHASE_IN_PROGRESS)
        self.assertEqual(first["cursor"], 0)

    def test_persists_after_each_move(self):
        # 1 initial write + 1 per move + 1 completion = N + 2 writes
        execute_expand_plan(
            self.cluster, self.executor,
            planned_moves=self.moves, new_node_id="n5")
        self.assertEqual(
            len(self.cluster.write_history), len(self.moves) + 2)
        # Cursor monotonically advances through the writes.
        cursors = [w["cursor"] for w in self.cluster.write_history]
        self.assertEqual(cursors, sorted(cursors))
        # Final write is the completion phase.
        self.assertEqual(
            self.cluster.write_history[-1]["phase"], EXPAND_PHASE_COMPLETED)

    def test_cursor_advanced_after_executor_returns(self):
        # If the orchestrator advanced cursor *before* calling execute, a
        # crash inside the executor would lose track of which move failed.
        # Verify execute() sees the *unincremented* cursor.
        observer = CursorObservingExecutor(self.cluster)
        execute_expand_plan(
            self.cluster, observer,
            planned_moves=self.moves, new_node_id="n5")
        self.assertEqual(observer.cursors_seen,
                         list(range(len(self.moves))))


# ---------------------------------------------------------------------------
# 2. Resume from persisted cursor (mgmt-node crash recovery)
# ---------------------------------------------------------------------------

class TestResume(unittest.TestCase):

    def test_resume_skips_already_executed_moves(self):
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        # Simulate: state was persisted with cursor=3 (3 moves done)
        state = make_expand_state("n5", moves)
        state["cursor"] = 3
        cluster = FakeCluster(expand_state=state)
        executor = NoopMoveExecutor()

        execute_expand_plan(cluster, executor)

        # Only the remaining 3 moves should execute.
        self.assertEqual(executor.executed, moves[3:])
        self.assertEqual(cluster.expand_state["phase"], EXPAND_PHASE_COMPLETED)

    def test_resume_ignores_planned_moves_when_passed(self):
        # Strict contract: passing planned_moves while resuming is a bug;
        # the orchestrator must refuse rather than silently overwrite.
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        state = make_expand_state("n5", moves)
        state["cursor"] = 1
        cluster = FakeCluster(expand_state=state)

        with self.assertRaises(ExpandPlanError):
            execute_expand_plan(
                cluster, NoopMoveExecutor(),
                planned_moves=moves, new_node_id="n5")

    def test_resume_with_completed_state_is_a_fresh_start(self):
        # An old completed state should be treated as "no expansion in
        # progress" — the operator can start a new one in its place.
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        old_state = make_expand_state("n5", moves)
        old_state["cursor"] = len(moves)
        old_state["phase"] = EXPAND_PHASE_COMPLETED
        cluster = FakeCluster(expand_state=old_state)
        new_moves = compute_role_diff(
            ["n1", "n2", "n3", "n4", "n5"], "n6", ftt=2)
        executor = NoopMoveExecutor()

        execute_expand_plan(
            cluster, executor,
            planned_moves=new_moves, new_node_id="n6")

        self.assertEqual(executor.executed, new_moves)
        self.assertEqual(cluster.expand_state["new_node_id"], "n6")

    def test_resume_with_aborted_state_is_a_fresh_start(self):
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        old_state = make_expand_state("n5", moves)
        old_state["phase"] = EXPAND_PHASE_ABORTED
        old_state["abort_reason"] = "earlier failure"
        cluster = FakeCluster(expand_state=old_state)

        execute_expand_plan(
            cluster, NoopMoveExecutor(),
            planned_moves=moves, new_node_id="n5")

        self.assertEqual(
            cluster.expand_state["phase"], EXPAND_PHASE_COMPLETED)


# ---------------------------------------------------------------------------
# 3. Abort on executor failure
# ---------------------------------------------------------------------------

class TestAbort(unittest.TestCase):

    def test_executor_exception_persists_aborted_state(self):
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        cluster = FakeCluster()
        executor = FailingExecutor(fail_at=2, exc=RuntimeError("boom"))

        with self.assertRaises(RuntimeError):
            execute_expand_plan(
                cluster, executor,
                planned_moves=moves, new_node_id="n5")

        self.assertEqual(
            cluster.expand_state["phase"], EXPAND_PHASE_ABORTED)
        # Cursor at the failing move (NOT advanced past it) so a future
        # resume retries the same move.
        self.assertEqual(cluster.expand_state["cursor"], 2)
        self.assertIn("RuntimeError", cluster.expand_state["abort_reason"])
        self.assertIn("boom", cluster.expand_state["abort_reason"])

    def test_aborted_state_is_the_last_persisted_write(self):
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        cluster = FakeCluster()
        executor = FailingExecutor(fail_at=1, exc=ValueError("oops"))

        with self.assertRaises(ValueError):
            execute_expand_plan(
                cluster, executor,
                planned_moves=moves, new_node_id="n5")

        last = cluster.write_history[-1]
        self.assertEqual(last["phase"], EXPAND_PHASE_ABORTED)
        self.assertEqual(last["cursor"], 1)

    def test_resume_after_abort_requires_fresh_plan(self):
        # An aborted state is treated as "no in-progress expansion" — the
        # operator must re-plan and pass new moves to retry.
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        cluster = FakeCluster()
        with self.assertRaises(RuntimeError):
            execute_expand_plan(
                cluster, FailingExecutor(fail_at=0, exc=RuntimeError("x")),
                planned_moves=moves, new_node_id="n5")

        # Resuming without a fresh plan should fail loudly, not silently
        # do nothing.
        with self.assertRaises(ExpandPlanError):
            execute_expand_plan(cluster, NoopMoveExecutor())


# ---------------------------------------------------------------------------
# 4. Precondition errors
# ---------------------------------------------------------------------------

class TestPreconditions(unittest.TestCase):

    def test_fresh_start_requires_plan(self):
        cluster = FakeCluster()
        with self.assertRaises(ExpandPlanError):
            execute_expand_plan(cluster, NoopMoveExecutor())

    def test_fresh_start_requires_new_node_id(self):
        moves = compute_role_diff(["n1", "n2", "n3"], "n4", ftt=2)
        cluster = FakeCluster()
        with self.assertRaises(ExpandPlanError):
            execute_expand_plan(
                cluster, NoopMoveExecutor(), planned_moves=moves)

    def test_resume_refuses_incompatible_schema(self):
        moves = compute_role_diff(["n1", "n2", "n3"], "n4", ftt=2)
        state = make_expand_state("n4", moves)
        state["schema_version"] = EXPAND_STATE_SCHEMA_VERSION + 99
        cluster = FakeCluster(expand_state=state)
        with self.assertRaises(ExpandPlanError):
            execute_expand_plan(cluster, NoopMoveExecutor())


# ---------------------------------------------------------------------------
# 5. NoopMoveExecutor sanity (also stands in for --dry-run)
# ---------------------------------------------------------------------------

class TestNoopMoveExecutor(unittest.TestCase):

    def test_records_calls_in_order(self):
        ex = NoopMoveExecutor()
        m1 = RoleMove("n3", ROLE_TERTIARY, "n1", "n5")
        m2 = RoleMove("n5", ROLE_PRIMARY, "", "n5")
        ex.execute(m1)
        ex.execute(m2)
        self.assertEqual(ex.executed, [m1, m2])


if __name__ == "__main__":
    unittest.main()
