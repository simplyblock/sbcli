"""
``tasks_controller.cancel_pending_node_restart_tasks`` exclusion semantics.

The helper existed to reap obsolete FN_NODE_RESTART rows when a node comes
back ONLINE. ``shutdown_storage_node`` now also calls it on the opposite
transition, so a row queued before a deliberate stop cannot fire afterwards
and undo it (live 2026-09-03: a cluster graceful-shutdown left two nodes
ONLINE because queued rows fired just behind the sweep, and the resulting
ONLINE transition cleared their auto_restart_disabled flag).

That second caller makes the exclusion load-bearing. The restart runner drives
shutdown_storage_node as its own kill step (tasks_runner_restart.py:542,
passing current_restart_task_id), so a blanket cancel there would abort the
very restart performing the shutdown -- turning a fix for a rare race into a
failure on every single node restart.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule


def _task(uuid, node_id="n1", function_name=JobSchedule.FN_NODE_RESTART,
          status=JobSchedule.STATUS_NEW, canceled=False):
    t = MagicMock(spec=JobSchedule)
    t.uuid = uuid
    t.node_id = node_id
    t.function_name = function_name
    t.status = status
    t.canceled = canceled
    t.get_id = MagicMock(return_value=f"c1/{uuid}")
    t.write_to_db = MagicMock(return_value=True)
    return t


class TestCancelPendingNodeRestartTasks(unittest.TestCase):

    def _run(self, tasks, **kwargs):
        db = MagicMock()
        db.get_job_tasks = MagicMock(return_value=tasks)
        with patch.object(tasks_controller, "db", db):
            n = tasks_controller.cancel_pending_node_restart_tasks(
                "c1", "n1", **kwargs)
        return n

    def test_pending_rows_are_canceled(self):
        t = _task("t1")
        self.assertEqual(self._run([t]), 1)
        self.assertTrue(t.canceled)
        self.assertEqual(t.status, JobSchedule.STATUS_DONE)

    def test_the_callers_own_task_is_left_alone(self):
        # The regression this guards: cancelling it aborts the restart that
        # is driving the shutdown.
        mine, other = _task("mine"), _task("other")
        self.assertEqual(self._run([mine, other], exclude_task_id="mine"), 1)
        self.assertFalse(mine.canceled)
        self.assertNotEqual(mine.status, JobSchedule.STATUS_DONE)
        self.assertTrue(other.canceled)

    def test_no_exclusion_still_cancels_everything(self):
        # set_node_status's ONLINE path passes no exclusion; behaviour there
        # must be unchanged.
        a, b = _task("a"), _task("b")
        self.assertEqual(self._run([a, b]), 2)
        self.assertTrue(a.canceled and b.canceled)

    def test_other_nodes_and_other_task_types_are_untouched(self):
        other_node = _task("t1", node_id="n2")
        other_kind = _task("t2", function_name=JobSchedule.FN_DEV_MIG)
        done = _task("t3", status=JobSchedule.STATUS_DONE)
        already = _task("t4", canceled=True)
        self.assertEqual(self._run([other_node, other_kind, done, already]), 0)
        self.assertFalse(other_node.canceled)
        self.assertFalse(other_kind.canceled)

    def test_reason_is_recorded(self):
        t = _task("t1")
        self._run([t], reason="node deliberately shut down")
        self.assertEqual(t.function_result,
                         "canceled: node deliberately shut down")


if __name__ == "__main__":
    unittest.main()
