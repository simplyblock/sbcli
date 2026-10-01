"""A removal records which step it is on.

The orchestrator has always been resumable, but it had no saved position — it
re-derived one from ``snode.status`` plus DB state on every entry. That is safe
and stays in place; what it cannot do is answer the question asked of a removal
that has been running for hours: *which step is it stuck on*. It also leaves a
step nowhere to keep work in progress, which volume drain will need in order to
remember the migrations it started.

Control flow is deliberately unchanged. A step that cannot finish still returns
False and is retried in place, never stepped over.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_node_removal as runner


def _task(params=None):
    task = JobSchedule()
    task.uuid = "t1"
    task.cluster_id = "c1"
    task.node_id = "n1"
    task.function_name = JobSchedule.FN_NODE_REMOVAL
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params = params if params is not None else {}
    task.canceled = False
    task.retry = 0
    task.max_retry = 100
    return task


class TestCursorRecordsPosition(unittest.TestCase):

    def test_entering_a_step_writes_it_to_the_task(self):
        task = _task()
        cur = storage_node_ops.RemovalCursor(task)
        cur.enter("decommission_jm", "phase 2 — decommission JM")
        self.assertEqual(cur.step, "decommission_jm")
        self.assertEqual(task.function_params["step"], "decommission_jm")

    def test_position_survives_a_retry(self):
        """A fresh cursor over the same task resumes where the last pass left
        off — that is the whole point of persisting it."""
        task = _task()
        storage_node_ops.RemovalCursor(task).enter("relocate_hosted", "phase 3b")
        resumed = storage_node_ops.RemovalCursor(task)
        self.assertEqual(resumed.step, "relocate_hosted")

    def test_per_step_data_survives_a_retry(self):
        task = _task()
        cur = storage_node_ops.RemovalCursor(task)
        cur.enter("devices", "phase 5")
        cur.data["migrations"] = ["m1", "m2"]
        cur.save()
        self.assertEqual(
            storage_node_ops.RemovalCursor(task).data["migrations"], ["m1", "m2"])

    def test_each_step_keeps_its_own_scratch(self):
        """Scratch is namespaced per step rather than one bag cleared on
        transition. The orchestrator replays earlier steps on every pass, so a
        bag that reset on change was wiped before the step that owned it ever
        read it back -- the drain restarted a migration it had already started
        (2026-09-15)."""
        task = _task()
        cur = storage_node_ops.RemovalCursor(task)
        cur.enter("drain_lvols", "drain")
        cur.data["migrations"] = ["m1"]
        cur.save()
        cur.enter("finalize", "phase 4")
        self.assertEqual(cur.data, {}, "the new step starts with its own empty scratch")
        cur.enter("drain_lvols", "drain")
        self.assertEqual(cur.data, {"migrations": ["m1"]},
                         "returning to a step restores what it saved")

    def test_replaying_the_earlier_steps_does_not_wipe_a_later_one(self):
        """Exactly the live failure: every pass walks recheck -> devices ->
        drain, and the drain must still find its own bookkeeping."""
        task = _task()
        cur = storage_node_ops.RemovalCursor(task)
        for step in ("recheck_conditions", "migrate_devices", "drain_lvols"):
            cur.enter(step, step)
        cur.data["migrations"] = ["m1"]
        cur.save()
        resumed = storage_node_ops.RemovalCursor(task)
        for step in ("recheck_conditions", "migrate_devices", "drain_lvols"):
            resumed.enter(step, step)
        self.assertEqual(resumed.data, {"migrations": ["m1"]})

    def test_re_entering_the_same_step_keeps_its_data(self):
        """A retry re-enters the step it failed on; its scratch must not be
        wiped, or a resumed drain would re-issue every migration."""
        task = _task()
        cur = storage_node_ops.RemovalCursor(task)
        cur.enter("devices", "phase 5")
        cur.data["migrations"] = ["m1"]
        cur.save()
        cur.enter("devices", "phase 5")
        self.assertEqual(cur.data, {"migrations": ["m1"]})

    def test_a_cursor_without_a_task_is_inert(self):
        """Direct calls and tests pass no task; recording must not raise."""
        cur = storage_node_ops._NullCursor()
        cur.enter("shutdown", "phase 1")
        cur.data["x"] = 1
        cur.save()
        self.assertEqual(cur.step, "shutdown")

    def test_every_recorded_step_is_a_declared_one(self):
        src = __import__("inspect").getsource(
            storage_node_ops.node_removal_orchestrate)
        recorded = set()
        for line in src.splitlines():
            if "cursor.enter(" in line:
                recorded.add(line.split('cursor.enter("', 1)[1].split('"', 1)[0])
        self.assertTrue(recorded, "the orchestrator records no steps at all")
        unknown = recorded - set(storage_node_ops.REMOVAL_STEPS)
        self.assertEqual(
            unknown, set(),
            f"steps recorded but not declared in REMOVAL_STEPS: {unknown}")


class TestOrchestratorStillWorksWithoutACursor(unittest.TestCase):

    def test_cursor_is_optional(self):
        """Callers that pass no cursor keep the old signature's behaviour."""
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = KeyError("gone")
        with patch.object(storage_node_ops, "DBController", return_value=db):
            self.assertFalse(storage_node_ops.node_removal_orchestrate("n1"))


class TestGivingUpNamesTheStep(unittest.TestCase):

    def test_the_failure_message_says_where_it_stopped(self):
        task = _task({"step": "relocate_hosted"})
        task.retry = 99
        task.max_retry = 100
        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate",
                          return_value=False), \
             patch.object(runner.storage_node_ops, "set_node_status") as set_status:
            runner.process_task(task)
        self.assertIn("relocate_hosted", task.function_result)
        self.assertEqual(set_status.call_args.args[1],
                         StorageNode.STATUS_REMOVED_FAILED)


if __name__ == "__main__":
    unittest.main()


class TestTheWaitClockMeasuresTheWaitNotTheRemoval(unittest.TestCase):
    """A bounded wait's budget means "this has been unmet continuously for too
    long". The clock is stamped on first entry and the orchestrator replays
    every step on every pass, so without clear_clock() it measures wall time
    since the removal first reached the step -- charging the wait for every
    minute spent legitimately elsewhere.

    That ended a removal on its first good pass (2026-09-15 20:10:05). The
    condition re-check had passed repeatedly and the drain had just started its
    migration; the migration briefly fenced the target, the next re-check saw
    that peer down for ~13s, and because the step clock had run for 46 minutes
    -- nearly all of it with conditions fine, blocked on an unrelated rebalance
    deadlock -- the 30-minute budget was already spent and it gave up at once.
    """

    def _cursor(self):
        task = MagicMock()
        task.function_params = {}
        return storage_node_ops.RemovalCursor(task)

    def test_a_passing_check_resets_the_clock(self):
        c = self._cursor()
        with patch.object(storage_node_ops.time, "time", return_value=1000.0):
            c.enter("recheck_conditions", "msg")
        with patch.object(storage_node_ops.time, "time", return_value=3800.0):
            self.assertGreater(c.elapsed(), 2700)   # 46 min, would give up
            c.clear_clock()
        # Next entry starts a fresh budget.
        with patch.object(storage_node_ops.time, "time", return_value=3801.0):
            c.enter("recheck_conditions", "msg")
        with patch.object(storage_node_ops.time, "time", return_value=3811.0):
            self.assertLess(c.elapsed(), 30)

    def test_a_step_that_keeps_failing_still_ages_out(self):
        """clear_clock is only called on success, so a genuinely stuck
        condition still hits its budget."""
        c = self._cursor()
        with patch.object(storage_node_ops.time, "time", return_value=1000.0):
            c.enter("recheck_conditions", "msg")
        for t in (1100.0, 1500.0, 2000.0):
            with patch.object(storage_node_ops.time, "time", return_value=t):
                c.enter("recheck_conditions", "msg")   # re-entry, no reset
        with patch.object(storage_node_ops.time, "time", return_value=2900.0):
            self.assertGreaterEqual(c.elapsed(), 1800)

    def test_clear_clock_persists(self):
        c = self._cursor()
        with patch.object(storage_node_ops.time, "time", return_value=1000.0):
            c.enter("recheck_conditions", "msg")
        self.assertIn("recheck_conditions",
                      c._task.function_params["step_entered_at"])
        c.clear_clock()
        self.assertNotIn("recheck_conditions",
                         c._task.function_params["step_entered_at"])

    def test_clear_clock_is_harmless_when_already_clear(self):
        c = self._cursor()
        c.clear_clock()
        c.step = "recheck_conditions"
        c.clear_clock()

    def test_it_only_clears_the_current_step(self):
        c = self._cursor()
        with patch.object(storage_node_ops.time, "time", return_value=1000.0):
            c.enter("recheck_conditions", "m")
            c.enter("drain_lvols", "m")
        c.clear_clock()   # current step is drain_lvols
        stamps = c._task.function_params["step_entered_at"]
        self.assertIn("recheck_conditions", stamps)
        self.assertNotIn("drain_lvols", stamps)
