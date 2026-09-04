# coding=utf-8
"""
test_tasks_runner_restart_shortcircuit.py — regression tests for the
FN_NODE_RESTART task-runner short-circuit in
``simplyblock_core.services.tasks_runner_restart.task_runner_node``.

Background: the runner used to treat a queued node_restart task as "still
needs work" unless BOTH ``status == ONLINE`` AND ``health_check == True``
held. The first iteration of this fix relaxed the device-count requirement
but kept ``health_check == True``. That residual requirement still let the
runner fire ``shutdown_storage_node(force=True) + restart_storage_node`` on
a node that was already serving IO whenever an auxiliary check (peer-side
remote-device records, port checks, transient lvstore consistency, etc.)
had flipped ``health_check`` to False. Observed as an endless
online → in_shutdown → offline → in_restart → online loop in the event
log.

The current contract: the short-circuit fires on **any** ``STATUS_ONLINE``
status, regardless of ``health_check``. An ONLINE node is, by definition,
serving IO from the data plane — and a destructive SPDK kill+restart is
never the right remedy for the auxiliary-check failures that flip
``health_check`` to False. Those have dedicated tasks (FN_DEV_RESTART,
FN_PORT_ALLOW, peer-side recreate_lvstore, health-service auto-fix).

Note on import:
  The module is loaded via ``importlib.util`` under a private name so the
  test gets its own copy. Its service loop lives inside ``main()`` behind an
  ``if __name__ == "__main__"`` guard, so executing the module runs only the
  import-time side effects (a module-level ``DBController()``, which is why
  that is patched); nothing needs to be interrupted.
"""

import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


_RUNNER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..",
    "simplyblock_core", "services", "tasks_runner_restart.py",
)


def _load_runner_module():
    """Import tasks_runner_restart.py under a private module name.

    No sleep patching: the service loop is inside ``main()``, called only
    under ``if __name__ == "__main__"``, and ``exec_module`` runs the module
    as ``tasks_runner_restart_under_test``, so the guard never fires.

    This used to patch ``time.sleep`` to raise SystemExit "to unwind the
    module-level ``while True``" -- a loop that does not exist. Because
    ``patch("time.sleep")`` replaces the attribute on the shared stdlib
    module, the SystemExit landed in whatever unrelated thread happened to be
    sleeping (the spdk_http_proxy_server stats thread), killing it and
    breaking later tests. A stack-walking guard was then added to aim the
    exception; deleting the patch removes the need for both.
    """
    spec = importlib.util.spec_from_file_location(
        "tasks_runner_restart_under_test",
        os.path.abspath(_RUNNER_PATH),
    )
    mod = importlib.util.module_from_spec(spec)

    with patch("simplyblock_core.db_controller.DBController") as mock_db_cls:
        mock_db = MagicMock()
        mock_db.get_clusters.return_value = []
        mock_db_cls.return_value = mock_db
        spec.loader.exec_module(mod)
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


def _mk_db(node):
    """A db double whose ``atomic_update`` actually applies the mutator.

    Terminal task writes go through ``db.atomic_update(task, _mutate)`` — a
    plain write of the runner's in-memory copy would erase a concurrent
    cancellation. An unconfigured MagicMock swallows ``_mutate``, so the task
    never reached DONE and the short-circuit looked broken when it was not.
    """
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
    return db


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


class TestShortCircuitSkipsRestartForOnlineNode(unittest.TestCase):
    """ONLINE is the universal short-circuit signal: regardless of
    health_check or device flags, an ONLINE node never gets put through
    a destructive shutdown+restart by this runner. This is the core
    anti-cycling guarantee."""

    def test_online_and_healthy_with_no_devices_skips_restart(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=True,
                        nvme_devices=[])
        with patch.object(mod, "db", _mk_db(node)):
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("online", task.function_result.lower())

    def test_online_and_healthy_with_unavailable_devices_still_skips(self):
        """Devices flagged UNAVAILABLE must NOT block task short-circuit.
        Device recovery is a separate task; spinning the node through
        another shutdown+restart here is exactly the bug."""
        mod = _load_runner_module()
        task = _mk_task()
        bad_dev = MagicMock()
        bad_dev.status = NVMeDevice.STATUS_UNAVAILABLE
        bad_dev.get_id.return_value = "dev-1"
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=True,
                        nvme_devices=[bad_dev])
        with patch.object(mod, "db", _mk_db(node)):
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)

    def test_online_but_unhealthy_still_skips_restart(self):
        """Critical regression: an ONLINE node with health_check=False
        (set by the health service for peer-side / port / lvstore-consistency
        reasons that don't warrant killing SPDK) must short-circuit. Failing
        to short-circuit here was the root cause of observed
        online → in_shutdown → offline cycles in soak iteration 32."""
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_ONLINE, health_check=False,
                        nvme_devices=[])
        with patch.object(mod, "db", _mk_db(node)):
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("online", task.function_result.lower())


class TestShortCircuitDoesNotApplyToNonOnlineStatuses(unittest.TestCase):
    """Statuses other than ONLINE must continue to the shutdown+restart path
    (or be handled by their own dedicated early-returns above). This pins
    the boundary so the ONLINE-only relaxation doesn't accidentally swallow
    OFFLINE/DOWN/UNREACHABLE tasks."""

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
    """REMOVED and SCHEDULABLE have dedicated early-returns at the top of
    task_runner_node. Pin REMOVED so a refactor doesn't accidentally drop
    it.

    Note: DOWN does NOT short-circuit here today — it falls through to the
    shutdown+restart path. Per the rationale of commit 2d69bab3
    ("auto-restart: only OFFLINE warrants a destructive SPDK restart")
    DOWN arguably should short-circuit too (SPDK is alive, recovery is
    port-unblock), but flipping that is a behavior change separate from
    the auto-restart cleanup work and is tracked as a follow-up."""

    def test_removed_short_circuits_without_restart(self):
        mod = _load_runner_module()
        task = _mk_task()
        node = _mk_node(status=StorageNode.STATUS_REMOVED)
        with patch.object(mod, "db", _mk_db(node)):
            ret = mod.task_runner_node(task)
        self.assertTrue(ret)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)


if __name__ == "__main__":
    unittest.main()
