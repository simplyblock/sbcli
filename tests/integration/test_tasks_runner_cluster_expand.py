# coding=utf-8
"""Unit tests for ``tasks_runner_cluster_expand.process_task``.

No FDB / SPDK: ``integrate_new_node_into_cluster`` and the DB handle are
mocked, so these run in milliseconds. This is the "fast tier" that lets
expansion logic be developed without the multi-hour real-FDB simulation.

Scope is the handler only. Cancellation, max-retry, status transitions and
the retry counter belong to the task runner driver and are tested once for
every runner in ``tests/unit/tasks/test_task_runner_base.py``.
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
from simplyblock_core.services.task_runner_base import TaskAbort, TaskRetry
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

    def test_missing_new_node_id_aborts(self):
        task = _task(new_node_id=None)
        with self.assertRaises(TaskAbort):
            runner.process_task(task)
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
        runner.process_task(task)

        self.integrate.assert_called_once()
        self.assertIn("expansion complete", task.function_result)
        # Only the two ONLINE devices get a migration task.
        self.assertEqual(self.tc.add_new_device_mig_task.call_count, 2)

    def test_failure_propagates_for_the_driver_to_retry(self):
        self.db.get_cluster_by_id.return_value = _cluster()
        self.db.get_storage_node_by_id.return_value = _node_with_devices()
        self.integrate.side_effect = RuntimeError("boom")

        task = _task(retry=0)
        # Suspending and counting the retry is the driver's half of the
        # contract; the handler only has to not swallow the failure.
        with self.assertRaises(RuntimeError):
            runner.process_task(task)

        self.tc.add_new_device_mig_task.assert_not_called()

    def test_unexpected_phase_after_run_is_retried(self):
        self.db.get_cluster_by_id.return_value = _cluster()
        self.db.get_storage_node_by_id.return_value = _node_with_devices()
        self.integrate.side_effect = lambda c, snode, **kw: None

        with self.assertRaises(TaskRetry):
            runner.process_task(_task())

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
        runner.process_task(task)

        # By the time integrate ran, the state was rearmed to in_progress and
        # the cursor preserved.
        self.assertEqual(seen_phase["phase"], EXPAND_PHASE_IN_PROGRESS)
        self.assertEqual(seen_phase["cursor"], 2)


if __name__ == "__main__":
    unittest.main()
