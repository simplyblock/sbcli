# coding=utf-8
"""Unit tests for ``tasks_runner_cluster_expand.task_runner``.

No FDB / SPDK: ``integrate_new_node_into_cluster`` and the DB handle are
mocked, so these run in milliseconds. This is the "fast tier" that lets
expansion logic be developed without the multi-hour real-FDB simulation.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.controllers.cluster_expansion.planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    EXPAND_PHASE_IN_PROGRESS,
)
import simplyblock_core.services.tasks_runner_cluster_expand as runner


def _task(status=JobSchedule.STATUS_NEW, retry=0, max_retry=3,
          canceled=False, new_node_id="n5"):
    t = JobSchedule()
    t.uuid = "task-1"
    t.cluster_id = "cl-1"
    t.function_name = JobSchedule.FN_CLUSTER_EXPAND
    t.function_params = {"new_node_id": new_node_id} if new_node_id else {}
    t.status = status
    t.retry = retry
    t.max_retry = max_retry
    t.canceled = canceled
    return t


def _cluster(expand_state=None):
    c = MagicMock()
    c.expand_state = expand_state or {}
    return c


def _node_with_devices(*statuses):
    n = MagicMock()
    devs = []
    for i, st in enumerate(statuses):
        d = MagicMock()
        d.status = st
        d.get_id.return_value = f"dev-{i}"
        devs.append(d)
    n.nvme_devices = devs
    return n


class TestProcessTask(unittest.TestCase):

    def setUp(self):
        self.db = MagicMock()
        patcher_db = patch.object(runner, "db", self.db)
        patcher_db.start()
        self.addCleanup(patcher_db.stop)

        self.integrate = patch.object(
            runner, "integrate_new_node_into_cluster").start()
        self.addCleanup(patch.stopall)

        self.tc = patch.object(runner, "tasks_controller").start()

    def test_canceled_marks_done(self):
        task = _task(canceled=True)
        res = runner.task_runner(task)
        self.assertFalse(res)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(task.function_result, "canceled")
        self.integrate.assert_not_called()

    def test_max_retry_marks_done(self):
        task = _task(retry=3, max_retry=3)
        res = runner.task_runner(task)
        self.assertTrue(res)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(task.function_result, "max retry reached")
        self.integrate.assert_not_called()

    def test_missing_new_node_id_marks_done(self):
        task = _task(new_node_id=None)
        res = runner.task_runner(task)
        self.assertTrue(res)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.integrate.assert_not_called()

    def test_happy_path_completes_and_queues_dev_mig(self):
        cluster = _cluster()
        new_node = _node_with_devices(
            NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_ONLINE,
            "unavailable")
        self.db.get_cluster_by_id.return_value = cluster
        self.db.get_storage_node_by_id.return_value = new_node

        def _integrate(c, snode, **kw):
            c.expand_state = {"phase": EXPAND_PHASE_COMPLETED}
        self.integrate.side_effect = _integrate

        task = _task()
        res = runner.task_runner(task)

        self.assertTrue(res)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.integrate.assert_called_once()
        # Only the two ONLINE devices get a migration task.
        self.assertEqual(self.tc.add_new_device_mig_task.call_count, 2)

    def test_failure_suspends_and_increments_retry(self):
        self.db.get_cluster_by_id.return_value = _cluster()
        self.db.get_storage_node_by_id.return_value = _node_with_devices()
        self.integrate.side_effect = RuntimeError("boom")

        task = _task(retry=0)
        res = runner.task_runner(task)

        self.assertFalse(res)
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(task.retry, 1)
        self.assertIn("boom", task.function_result)
        self.tc.add_new_device_mig_task.assert_not_called()

    def test_aborted_state_is_rearmed_before_resume(self):
        # A prior attempt aborted at cursor 2; the runner must flip it back to
        # in_progress (preserving the cursor) before re-invoking integrate.
        cluster = _cluster({
            "schema_version": 1,
            "phase": EXPAND_PHASE_ABORTED,
            "new_node_id": "n5",
            "moves": [{}, {}, {}, {}],
            "cursor": 2,
            "abort_reason": "earlier failure",
        })
        self.db.get_cluster_by_id.return_value = cluster
        self.db.get_storage_node_by_id.return_value = _node_with_devices()

        seen_phase = {}

        def _integrate(c, snode, **kw):
            seen_phase["phase"] = c.expand_state["phase"]
            seen_phase["cursor"] = c.expand_state["cursor"]
            c.expand_state = {"phase": EXPAND_PHASE_COMPLETED}
        self.integrate.side_effect = _integrate

        task = _task()
        runner.task_runner(task)

        # By the time integrate ran, the state was rearmed to in_progress and
        # the cursor preserved.
        self.assertEqual(seen_phase["phase"], EXPAND_PHASE_IN_PROGRESS)
        self.assertEqual(seen_phase["cursor"], 2)


if __name__ == "__main__":
    unittest.main()
