# coding=utf-8
"""
test_activation_gate.py — regression tests for the auto-reactivation races
fixed in ``simplyblock_core/services/storage_node_monitor.py`` after incident
2026-06-25.

Two interlocking bugs caused a ~2h11m hang after a full network outage on an
FTT=1 cluster:

1. **Premature / unsafe auto-activation.** The SUSPENDED -> activate gate only
   blocked on IN_SHUTDOWN / IN_CREATION / RESTARTING (the online-status check
   was commented out). So it fired while nodes were still UNREACHABLE (race #1,
   harmless bounce) and, worse, while nodes were merely OFFLINE/DOWN (race #2),
   starting a cluster_activate against not-yet-online nodes that then wedged.

2. **No watchdog on IN_ACTIVATION.** Once wedged, the monitor early-returned
   every tick and ``add_node_to_auto_restart`` refused to queue restarts while
   the cluster was not SUSPENDED -> deadlock with no path out.

Fix: ``_activation_node_gate`` requires every node ONLINE before activating
(allowing up to ``max_fault_tolerance`` deliberately shut-down nodes), and
``_watchdog_stuck_activation`` reverts a cluster stuck in IN_ACTIVATION back to
SUSPENDED after CLUSTER_ACTIVATION_WATCHDOG_SEC.
"""

import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, status=StorageNode.STATUS_ONLINE, auto_restart_disabled=False,
          online_since=""):
    n = MagicMock(spec=StorageNode)
    n.status = status
    # Default False: a bare MagicMock attribute is truthy, which would make the
    # gate treat every node as a deliberate shutdown.
    n.auto_restart_disabled = auto_restart_disabled
    n.online_since = online_since
    n.get_id = MagicMock(return_value=uuid)
    return n


# ===========================================================================
# _activation_node_gate
# ===========================================================================


class TestActivationNodeGate(unittest.TestCase):

    def _gate(self, nodes, ftt=1, active_restart_task=None):
        from simplyblock_core.services import storage_node_monitor as mod
        with patch.object(mod, "tasks_controller") as mock_tasks:
            mock_tasks.get_active_node_restart_task.return_value = active_restart_task
            return mod._activation_node_gate("cluster-1", nodes, ftt)

    # ---- happy path -----------------------------------------------------

    def test_all_online_can_activate(self):
        nodes = [_node(f"n{i}") for i in range(4)]
        ok, reason = self._gate(nodes)
        self.assertTrue(ok, reason)

    def test_removed_nodes_ignored(self):
        nodes = [_node("n0"), _node("n1"), _node("gone", status=StorageNode.STATUS_REMOVED)]
        ok, _ = self._gate(nodes)
        self.assertTrue(ok)

    # ---- race #1: unreachable / transitional must block -----------------

    def test_unreachable_node_blocks(self):
        nodes = [_node("n0"), _node("n1", status=StorageNode.STATUS_UNREACHABLE)]
        ok, reason = self._gate(nodes)
        self.assertFalse(ok)
        self.assertIn("transitioning", reason)

    def test_in_restart_node_blocks(self):
        nodes = [_node("n0"), _node("n1", status=StorageNode.STATUS_RESTARTING)]
        ok, _ = self._gate(nodes)
        self.assertFalse(ok)

    def test_in_shutdown_node_blocks(self):
        nodes = [_node("n0"), _node("n1", status=StorageNode.STATUS_IN_SHUTDOWN)]
        ok, _ = self._gate(nodes)
        self.assertFalse(ok)

    # ---- race #2: offline/down (not deliberate) must block --------------

    def test_offline_node_not_deliberate_blocks(self):
        nodes = [_node("n0"), _node("n1", status=StorageNode.STATUS_OFFLINE)]
        ok, reason = self._gate(nodes)
        self.assertFalse(ok)
        self.assertIn("not deliberately", reason)

    def test_down_node_not_deliberate_blocks(self):
        nodes = [_node("n0"), _node("n1", status=StorageNode.STATUS_DOWN)]
        ok, _ = self._gate(nodes)
        self.assertFalse(ok)

    # ---- deliberate-shutdown exception, capped at FTT -------------------

    def test_one_deliberate_shutdown_within_ftt1_ok(self):
        nodes = [_node("n0"),
                 _node("n1", status=StorageNode.STATUS_OFFLINE, auto_restart_disabled=True)]
        ok, reason = self._gate(nodes, ftt=1)
        self.assertTrue(ok, reason)

    def test_two_deliberate_shutdowns_exceed_ftt1_blocks(self):
        nodes = [_node("n0"),
                 _node("n1", status=StorageNode.STATUS_OFFLINE, auto_restart_disabled=True),
                 _node("n2", status=StorageNode.STATUS_OFFLINE, auto_restart_disabled=True)]
        ok, reason = self._gate(nodes, ftt=1)
        self.assertFalse(ok)
        self.assertIn("fault tolerance", reason)

    def test_two_deliberate_shutdowns_within_ftt2_ok(self):
        nodes = [_node("n0"),
                 _node("n1", status=StorageNode.STATUS_OFFLINE, auto_restart_disabled=True),
                 _node("n2", status=StorageNode.STATUS_OFFLINE, auto_restart_disabled=True)]
        ok, reason = self._gate(nodes, ftt=2)
        self.assertTrue(ok, reason)

    # ---- prior guards preserved -----------------------------------------

    def test_active_restart_task_blocks(self):
        nodes = [_node("n0"), _node("n1")]
        ok, reason = self._gate(nodes, active_restart_task=MagicMock())
        self.assertFalse(ok)
        self.assertIn("restart task", reason)

    def test_recently_online_blocks(self):
        recent = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        nodes = [_node("n0"), _node("n1", online_since=recent)]
        ok, reason = self._gate(nodes)
        self.assertFalse(ok)
        self.assertIn("30 seconds", reason)

    def test_settled_online_ok(self):
        old = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        nodes = [_node("n0", online_since=old), _node("n1", online_since=old)]
        ok, reason = self._gate(nodes)
        self.assertTrue(ok, reason)


# ===========================================================================
# _watchdog_stuck_activation
# ===========================================================================


class TestActivationWatchdog(unittest.TestCase):

    def _run(self, in_activation_since):
        from simplyblock_core.services import storage_node_monitor as mod
        cluster = MagicMock(spec=Cluster)
        cluster.in_activation_since = in_activation_since
        cluster.get_id = MagicMock(return_value="cluster-1")
        with patch.object(mod, "cluster_ops") as mock_ops:
            mod._watchdog_stuck_activation(cluster)
            return mock_ops

    def test_no_timestamp_does_nothing(self):
        ops = self._run("")
        ops.set_cluster_status.assert_not_called()

    def test_recent_activation_not_reverted(self):
        recent = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        ops = self._run(recent)
        ops.set_cluster_status.assert_not_called()

    def test_stuck_activation_reverted_to_suspended(self):
        stale = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
        ops = self._run(stale)
        ops.set_cluster_status.assert_called_once_with("cluster-1", Cluster.STATUS_SUSPENDED)

    def test_malformed_timestamp_does_nothing(self):
        ops = self._run("not-a-timestamp")
        ops.set_cluster_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
