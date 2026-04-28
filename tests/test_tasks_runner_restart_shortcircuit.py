# coding=utf-8
"""
test_tasks_runner_restart_shortcircuit.py — regression tests for the
FN_NODE_RESTART task-runner short-circuit in
``simplyblock_core.services.tasks_runner_restart.task_runner_node``.

Background: before this change, the runner treated a queued node_restart
task as "still needs work" unless BOTH ``status == ONLINE`` AND
``unavailable_devices_count == 0`` held. Any residual UNAVAILABLE device
record (routine transient after an outage — peer nodes mark remote device
records as unavailable, and that flag is cleared asynchronously by a
different code path) caused the runner to fire another
``shutdown_storage_node(force=True) + restart_storage_node`` cycle on a
node that was already serving IO. Observed as an endless
online → in_shutdown → offline → in_restart → online loop in the event
log.

The fix: the short-circuit now only requires ``status == ONLINE and
health_check`` — device-level recovery is the responsibility of
FN_DEV_RESTART, not this runner.

Note on import:
  The module has a ``while True:`` at module level (the service main loop),
  so a plain ``import`` hangs. We load it via ``importlib.util`` with
  ``time.sleep`` patched to raise SystemExit after one tick — the exec
  unwinds out of the loop and we get a usable module with all function
  definitions intact. The same pattern is already used by other
  service-module tests in the tree.
"""

import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


_RUNNER_PATH = os.path.join(
    os.path.dirname(__file__), "..",
    "simplyblock_core", "services", "tasks_runner_restart.py",
)


def _load_runner_module():
    """Import tasks_runner_restart.py without entering its service loop.

    We patch ``time.sleep`` to raise SystemExit on first call so the
    module-level ``while True`` unwinds out. The module dict (including
    task_runner_node) is fully populated at that point.
    """
    spec = importlib.util.spec_from_file_location(
        "tasks_runner_restart_under_test",
        os.path.abspath(_RUNNER_PATH),
    )
    mod = importlib.util.module_from_spec(spec)

    def _break(*_a, **_kw):
        raise SystemExit("test-bailout")

    with patch("simplyblock_core.db_controller.DBController") as mock_db_cls, \
         patch("time.sleep", side_effect=_break):
        mock_db = MagicMock()
        mock_db.get_clusters.return_value = []
        mock_db_cls.return_value = mock_db
        try:
            spec.loader.exec_module(mod)
        except SystemExit:
            pass
    return mod


def _mk_task(node_id="node-1", retry=0, max_retry=11,
             status=JobSchedule.STATUS_NEW, canceled=False):
    t = MagicMock(spec=JobSchedule)
    t.node_id = node_id
    t.retry = retry
    t.max_retry = max_retry
    t.status = status
    t.canceled = canceled
    t.function_result = ""
    t.write_to_db = MagicMock()
    return t


def _mk_node(status=StorageNode.STATUS_ONLINE, health_check=True,
             nvme_devices=None):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = "node-1"
    n.status = status
    n.health_check = health_check
    n.nvme_devices = nvme_devices or []
    n.mgmt_ip = "10.0.0.1"
    n.data_nics = []
    return n


class TestShortCircuitSkipsRestartForHealthyNode(unittest.TestCase):
    """If the node is ONLINE and health_check=True, the runner should mark
    the task DONE immediately — regardless of what the devices look like.
    This is the core anti-cycling guarantee."""

    def test_online_and_healthy_with_no_devices_skips_restart(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=True,
                        nvme_devices=[])
        with patch.object(mod, "db") as mock_db:
            mock_db.get_storage_node_by_id.return_value = node
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("online", task.function_result.lower())

    def test_online_and_healthy_with_unavailable_devices_still_skips(self):
        """The critical regression this test pins: devices flagged UNAVAILABLE
        must NOT block task short-circuit. Device recovery is a separate
        task; spinning the node through another shutdown+restart here is
        exactly the bug."""
        mod = _load_runner_module()
        task = _mk_task()
        bad_dev = MagicMock()
        bad_dev.status = NVMeDevice.STATUS_UNAVAILABLE
        bad_dev.get_id.return_value = "dev-1"
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=True,
                        nvme_devices=[bad_dev])
        with patch.object(mod, "db") as mock_db:
            mock_db.get_storage_node_by_id.return_value = node
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)


class TestShortCircuitRejectsUnhealthyNode(unittest.TestCase):
    """If the node is NOT actually healthy, the task must not short-circuit
    — it must continue to the shutdown+restart path."""

    def test_online_but_unhealthy_does_not_short_circuit(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=False,
                        nvme_devices=[])
        with patch.object(mod, "db") as mock_db, \
             patch.object(mod, "health_controller") as mock_health:
            mock_db.get_storage_node_by_id.return_value = node
            # Fail the reachability checks immediately so we don't proceed
            # into the shutdown path — we only need to assert the function
            # didn't return True from the short-circuit.
            mock_health._check_node_ping.return_value = False
            mock_health._check_node_api.return_value = False
            mock_health._check_ping_from_node.return_value = False
            ret = mod.task_runner_node(task)
        self.assertNotEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertFalse(ret)

    def test_offline_does_not_short_circuit(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_OFFLINE, health_check=True)
        with patch.object(mod, "db") as mock_db, \
             patch.object(mod, "health_controller") as mock_health:
            mock_db.get_storage_node_by_id.return_value = node
            mock_health._check_node_ping.return_value = False
            mock_health._check_node_api.return_value = False
            mock_health._check_ping_from_node.return_value = False
            _ = mod.task_runner_node(task)
        self.assertNotEqual(task.status, JobSchedule.STATUS_DONE)


class TestTerminalStatusesStillDoneImmediately(unittest.TestCase):
    """Terminal/owning statuses (REMOVED, SCHEDULABLE, DOWN) already had a
    dedicated early-return above the short-circuit. Pin that path so a
    refactor doesn't accidentally drop it."""

    def test_removed_short_circuits_without_restart(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_REMOVED)
        with patch.object(mod, "db") as mock_db:
            mock_db.get_storage_node_by_id.return_value = node
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)

    def test_down_short_circuits_without_restart(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_DOWN)
        with patch.object(mod, "db") as mock_db:
            mock_db.get_storage_node_by_id.return_value = node
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)


if __name__ == "__main__":
    unittest.main()
