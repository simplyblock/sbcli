# coding=utf-8
"""
test_status_guards.py — regression tests for the "don't clobber a node
mid-restart" guards added to:

- ``simplyblock_core.services.storage_node_monitor.set_node_offline``
- ``simplyblock_core.services.storage_node_monitor.set_node_unreachable``
- ``simplyblock_core.controllers.health_controller.check_node``

Background: HealthCheck and StorageNodeMonitor both mutate node/device
state in FDB on "looks sick" detection. Their detection (spdk_process_is_up
HTTP probe and distrib cluster_map parsing) is unavoidably racy with the
runner's own status transitions during IN_SHUTDOWN/RESTARTING. Observed
failure: monitor's spdk_process_is_up catches the runner's shutdown→restart
window, fires set_node_offline, which — without this guard — flips status
to OFFLINE mid-restart and marks devices unavailable, failing the runner's
post-restart check and triggering another retry.

These tests pin the guard behaviour: while the node is in a transient
state (IN_SHUTDOWN, RESTARTING, UNREACHABLE) the monitor/health service
must not write to FDB.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice


def _node(uuid="node-1", status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = uuid
    n.status = status
    n.cluster_id = "cluster-1"
    n.nvme_devices = []
    n.mgmt_ip = "10.0.0.1"
    return n


# ---------------------------------------------------------------------------
# storage_node_monitor.set_node_offline
# ---------------------------------------------------------------------------


class TestSetNodeOfflineGuard(unittest.TestCase):

    def _patch(self, node):
        from simplyblock_core.services import storage_node_monitor as mod
        p_db = patch.object(mod, "db")
        p_ops = patch.object(mod, "storage_node_ops")
        p_devctl = patch.object(mod, "device_controller")
        p_tasks = patch.object(mod, "tasks_controller")
        p_upd = patch.object(mod, "update_cluster_status")
        # Module-level ``cluster_id`` is set at service start-up. For unit
        # tests we inject it directly so update_cluster_status(cluster_id)
        # doesn't raise NameError.
        p_cid = patch.object(mod, "cluster_id", "cluster-1", create=True)
        self._patches = [p_db, p_ops, p_devctl, p_tasks, p_upd, p_cid]
        mocks = [p.start() for p in self._patches]
        self.mock_db, self.mock_ops, self.mock_devctl, self.mock_tasks, self.mock_upd, _ = mocks
        self.mock_db.get_storage_node_by_id.return_value = node
        return mod

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()

    def _assert_no_writes(self):
        self.mock_ops.set_node_status.assert_not_called()
        self.mock_devctl.device_set_unavailable.assert_not_called()
        self.mock_tasks.add_node_to_auto_restart.assert_not_called()

    def test_skips_when_node_is_restarting(self):
        mod = self._patch(_node(status=StorageNode.STATUS_RESTARTING))
        mod.set_node_offline(mod.db.get_storage_node_by_id.return_value)
        self._assert_no_writes()

    def test_skips_when_node_is_in_shutdown(self):
        mod = self._patch(_node(status=StorageNode.STATUS_IN_SHUTDOWN))
        mod.set_node_offline(mod.db.get_storage_node_by_id.return_value)
        self._assert_no_writes()

    def test_skips_when_node_already_offline(self):
        mod = self._patch(_node(status=StorageNode.STATUS_OFFLINE))
        mod.set_node_offline(mod.db.get_storage_node_by_id.return_value)
        self._assert_no_writes()

    def test_fires_when_node_unreachable(self):
        # UNREACHABLE → OFFLINE is the legitimate escalation path from
        # _check_data_plane_and_escalate: when peers confirm the data plane
        # is down, the node should be promoted to OFFLINE so auto-restart
        # gets queued. Previously (briefly) UNREACHABLE was skipped here,
        # which left nodes wedged in UNREACHABLE with no auto-restart.
        node = _node(status=StorageNode.STATUS_UNREACHABLE)
        dev = MagicMock()
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.get_id.return_value = "dev-1"
        node.nvme_devices = [dev]
        mod = self._patch(node)
        mod.set_node_offline(node)
        self.mock_ops.set_node_status.assert_called_once_with(
            node.get_id(), StorageNode.STATUS_OFFLINE)
        self.mock_tasks.add_node_to_auto_restart.assert_called_once_with(node)

    def test_fires_when_node_online(self):
        node = _node(status=StorageNode.STATUS_ONLINE)
        dev = MagicMock()
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.get_id.return_value = "dev-1"
        node.nvme_devices = [dev]
        mod = self._patch(node)
        mod.set_node_offline(node)
        self.mock_ops.set_node_status.assert_called_once_with(
            node.get_id(), StorageNode.STATUS_OFFLINE)
        self.mock_devctl.device_set_unavailable.assert_called_once_with("dev-1")
        self.mock_tasks.add_node_to_auto_restart.assert_called_once_with(node)


# ---------------------------------------------------------------------------
# storage_node_monitor.set_node_unreachable
# ---------------------------------------------------------------------------


class TestSetNodeUnreachableGuard(unittest.TestCase):

    def _patch(self, node):
        from simplyblock_core.services import storage_node_monitor as mod
        p_ops = patch.object(mod, "storage_node_ops")
        p_upd = patch.object(mod, "update_cluster_status")
        p_esc = patch.object(mod, "_check_data_plane_and_escalate")
        p_cid = patch.object(mod, "cluster_id", "cluster-1", create=True)
        self._patches = [p_ops, p_upd, p_esc, p_cid]
        mocks = [p.start() for p in self._patches]
        self.mock_ops, self.mock_upd, self.mock_esc, _ = mocks
        return mod

    def tearDown(self):
        for p in getattr(self, "_patches", []):
            p.stop()

    def test_skips_status_write_when_node_is_restarting(self):
        mod = self._patch(None)
        mod.set_node_unreachable(_node(status=StorageNode.STATUS_RESTARTING))
        self.mock_ops.set_node_status.assert_not_called()

    def test_skips_status_write_when_node_is_in_shutdown(self):
        mod = self._patch(None)
        mod.set_node_unreachable(_node(status=StorageNode.STATUS_IN_SHUTDOWN))
        self.mock_ops.set_node_status.assert_not_called()

    def test_skips_status_write_when_already_unreachable(self):
        mod = self._patch(None)
        mod.set_node_unreachable(_node(status=StorageNode.STATUS_UNREACHABLE))
        self.mock_ops.set_node_status.assert_not_called()

    def test_fires_status_write_when_node_online(self):
        mod = self._patch(None)
        node = _node(status=StorageNode.STATUS_ONLINE)
        mod.set_node_unreachable(node)
        self.mock_ops.set_node_status.assert_called_once_with(
            node.get_id(), StorageNode.STATUS_UNREACHABLE)

    def test_escalation_always_runs(self):
        # The data-plane escalation helper should still be invoked regardless
        # of whether the status-write was skipped — it's the only path that
        # can promote an unreachable-but-truly-dead node to OFFLINE.
        mod = self._patch(None)
        mod.set_node_unreachable(_node(status=StorageNode.STATUS_UNREACHABLE))
        self.mock_esc.assert_called_once()


# ---------------------------------------------------------------------------
# health_controller.check_node
# ---------------------------------------------------------------------------


class TestHealthCheckNodeGuard(unittest.TestCase):

    def _run(self, status):
        from simplyblock_core.controllers import health_controller as mod
        node = _node(status=status)
        # For the ONLINE case we need the full checker stack to be harmless
        # mocks: node has no devices / remote_devices / lvstore_stack / JM,
        # so the function walks past the early skip and exercises the top
        # guard branch. Note that for transient states we return at the
        # guard; the downstream mocks are only needed for the ONLINE case.
        node.nvme_devices = []
        node.remote_devices = []
        node.remote_jm_devices = []
        node.data_nics = []
        node.jm_device = None
        node.enable_ha_jm = False
        node.lvstore_stack = []
        node.lvstore_stack_secondary = None
        node.lvstore_stack_tertiary = None
        node.secondary_node_id = None
        node.tertiary_node_id = None
        node.is_secondary_node = False
        node.lvstore = "LVS_NA"
        node.get_lvol_subsys_port.return_value = 4420
        with patch.object(mod, "DBController") as mock_db_cls, \
             patch.object(mod, "_check_node_ping", return_value=True) as mock_ping, \
             patch.object(mod, "_check_node_api", return_value=True) as mock_api, \
             patch.object(mod, "check_node_rpc", return_value=(True, True)) as mock_rpc, \
             patch.object(mod, "_check_ping_from_node", return_value=True), \
             patch.object(mod, "check_port_on_node", return_value=True):
            db = mock_db_cls.return_value
            db.get_storage_node_by_id.return_value = node
            result = mod.check_node(node.get_id())
            return result, mock_ping, mock_api, mock_rpc

    def test_skips_when_restarting(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_RESTARTING)
        self.assertTrue(ret)
        ping.assert_not_called()
        api.assert_not_called()
        rpc.assert_not_called()

    def test_skips_when_in_shutdown(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_IN_SHUTDOWN)
        self.assertTrue(ret)
        ping.assert_not_called()

    def test_skips_when_unreachable(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_UNREACHABLE)
        self.assertTrue(ret)
        ping.assert_not_called()

    def test_skips_when_suspended(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_SUSPENDED)
        self.assertTrue(ret)
        ping.assert_not_called()

    def test_skips_when_in_creation(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_IN_CREATION)
        self.assertTrue(ret)
        ping.assert_not_called()

    def test_skips_when_offline(self):
        # Pre-existing behaviour — kept as a regression anchor.
        ret, ping, api, rpc = self._run(StorageNode.STATUS_OFFLINE)
        self.assertTrue(ret)
        ping.assert_not_called()

    def test_runs_when_online(self):
        ret, ping, api, rpc = self._run(StorageNode.STATUS_ONLINE)
        # Online node — pings/checks should have been invoked.
        ping.assert_called()


if __name__ == "__main__":
    unittest.main()
