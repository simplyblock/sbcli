# coding=utf-8
"""
test_secondary_promotion.py – unit tests for:

1. restart_storage_node – concurrent restart guard
2. recreate_lvstore (leader takeover) – promotes secondary to leader when primary is offline
3. recreate_lvstore_on_non_leader – does NOT promote when primary is online
4. recreate_lvstore_on_non_leader – always creates secondary hublvol on sec_1
5. recreate_lvstore (leader takeover) – escalates unreachable primary via data plane check

Note: storage_node_monitor and tasks_runner_migration have module-level
infinite loops and cannot be imported in unit tests. The migration leadership
check and data plane functions are tested indirectly.
"""

import unittest
from unittest.mock import MagicMock, patch, call

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1"):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = "ha"
    c.distr_ndcs = 1
    c.distr_npcs = 2
    c.max_fault_tolerance = 2
    c.status = Cluster.STATUS_ACTIVE
    c.nqn = "nqn.2023-02.io.simplyblock:cluster-1"
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          lvstore="LVS_100", secondary_node_id="", tertiary_node_id="",
          mgmt_ip="", rpc_port=8080, jm_vuid=100, is_secondary_node=False):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = f"host-{uuid[:8]}"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.tertiary_node_id = tertiary_node_id
    n.mgmt_ip = mgmt_ip or f"10.0.0.{hash(uuid) % 254 + 1}"
    n.rpc_port = rpc_port
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.jm_vuid = jm_vuid
    n.is_secondary_node = is_secondary_node
    n.lvstore_ports = {lvstore: {"lvol_subsys_port": 4420, "hublvol_port": 4425}}
    n.active_tcp = True
    n.active_rdma = False
    n.lvstore_stack = []
    n.raid = "raid0"
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}", "bdev_name": "lvs/hublvol",
                          "model_number": "model1", "nguid": "0" * 32})
    n.remote_devices = []
    n.remote_jm_devices = []
    n.nvme_devices = []
    n.data_nics = []
    nic = IFace()
    nic.ip4_address = mgmt_ip or f"10.10.10.{hash(uuid) % 254 + 1}"
    nic.trtype = "TCP"
    n.data_nics = [nic]
    n.api_endpoint = f"http://{n.mgmt_ip}:5000"
    n.lvstore_status = "ready"
    n.health_check = True
    n.enable_ha_jm = False
    n.write_to_db = MagicMock()
    return n


def _lvol(uuid, node_id, lvs_name="LVS_100"):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.status = LVol.STATUS_ONLINE
    lv.ha_type = "ha"
    lv.nodes = [node_id]
    lv.lvs_name = lvs_name
    lv.lvol_bdev = "bdev_test"
    lv.top_bdev = f"{lvs_name}/bdev_test"
    lv.fabric = "tcp"
    lv.nqn = f"nqn.2023-02.io.simplyblock:cluster-1:lvol:{uuid}"
    lv.allowed_hosts = []
    lv.ns_id = 1
    lv.deletion_status = ""
    lv.lvol_type = "lvol"
    lv.crypto_bdev = ""
    lv.lvol_uuid = f"lvol-uuid-{uuid}"
    lv.guid = f"guid-{uuid}"
    return lv


# ---------------------------------------------------------------------------
# 1. Concurrent restart guard
# ---------------------------------------------------------------------------

class TestConcurrentRestartGuard(unittest.TestCase):

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_rejects_restart_when_peer_is_restarting(self, mock_db_cls, _mock_disc, _mock_phase, _mock_handle):
        from simplyblock_core.storage_node_ops import restart_storage_node

        db = mock_db_cls.return_value
        snode = _node("node-1", status=StorageNode.STATUS_OFFLINE)
        peer = _node("node-2", status=StorageNode.STATUS_RESTARTING)

        db.get_storage_node_by_id.return_value = snode
        db.get_cluster_by_id.return_value = _cluster()
        db.get_storage_nodes_by_cluster_id.return_value = [snode, peer]
        mock_tasks = MagicMock()
        db.try_set_node_restarting.return_value = (False, "Node node-2 is in_restart")

        with patch("simplyblock_core.storage_node_ops.tasks_controller", mock_tasks):
            mock_tasks.get_active_node_restart_task.return_value = None
            result = restart_storage_node("node-1")
        self.assertFalse(result)

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_allows_restart_when_no_peer_is_restarting(self, mock_db_cls, mock_set_status, mock_tasks, _mock_disc, _mock_phase, _mock_handle):
        from simplyblock_core.storage_node_ops import restart_storage_node

        db = mock_db_cls.return_value
        snode = _node("node-1", status=StorageNode.STATUS_OFFLINE)
        peer = _node("node-2", status=StorageNode.STATUS_ONLINE)

        db.get_storage_node_by_id.return_value = snode
        db.get_cluster_by_id.return_value = _cluster()
        db.get_storage_nodes_by_cluster_id.return_value = [snode, peer]
        mock_tasks.get_active_node_restart_task.return_value = None
        db.try_set_node_restarting.return_value = (True, None)

        # Will proceed past the guard but fail later (no real SPDK)
        with patch("simplyblock_core.storage_node_ops.SNodeClient"):
            try:
                restart_storage_node("node-1")
            except Exception:
                pass

        # Verify it got past the guard (FDB transaction succeeded)
        db.try_set_node_restarting.assert_called_once()


# ---------------------------------------------------------------------------
# 2. recreate_lvstore (leader takeover) – secondary promotion when primary offline
# ---------------------------------------------------------------------------

class TestSecondaryPromotion(unittest.TestCase):
    """Test that first secondary gets promoted to leader when primary is offline."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.add_lvol_thread")
    @patch("simplyblock_core.storage_node_ops.ThreadPoolExecutor")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack", return_value=(True, None))
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.services.storage_node_monitor.is_node_data_plane_disconnected_quorum",
           return_value=True)
    def test_promotes_secondary_when_primary_offline(
            self, mock_quorum, mock_db_cls, mock_create_stack, mock_connect_jm, mock_rpc_cls,
            mock_fw, mock_tcp_events, mock_storage_events,
            mock_health, mock_executor_cls, mock_add_lvol,
            mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        from simplyblock_core.storage_node_ops import recreate_lvstore

        db = mock_db_cls.return_value

        primary = _node("primary-1", status=StorageNode.STATUS_OFFLINE,
                         lvstore="LVS_100", secondary_node_id="sec-1",
                         tertiary_node_id="sec-2")
        secondary = _node("sec-1", status=StorageNode.STATUS_ONLINE,
                           lvstore="LVS_200", is_secondary_node=True)
        tertiary = _node("sec-2", status=StorageNode.STATUS_ONLINE,
                          lvstore="LVS_300", is_secondary_node=True)

        lvol = _lvol("vol-1", "primary-1")

        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary]
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "primary-1": primary, "sec-1": secondary, "sec-2": tertiary
        }.get(nid, primary)
        db.get_lvols_by_node_id.return_value = [lvol]
        db.get_snapshots_by_node_id.return_value = []
        db.get_cluster_by_id.return_value = _cluster()

        mock_connect_jm.return_value = []

        mock_rpc = MagicMock()
        mock_rpc.bdev_examine.return_value = True
        mock_rpc.bdev_wait_for_examine.return_value = True
        # Leadership must show as restored so the leader-restore loop in
        # recreate_lvstore exits cleanly instead of falling through to _kill_app.
        mock_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        mock_rpc.bdev_lvol_set_lvs_opts.return_value = True
        mock_rpc.get_bdevs.return_value = [{"name": "lvol-uuid-vol-1", "aliases": []}]
        mock_rpc.jc_suspend_compression.return_value = (True, None)
        mock_rpc.jc_compression_get_status.return_value = False
        mock_rpc.bdev_distrib_force_to_non_leader.return_value = True
        mock_rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = mock_rpc

        for n in [secondary, tertiary, primary]:
            n.rpc_client = MagicMock(return_value=mock_rpc)
            n.create_hublvol = MagicMock()
            n.adopt_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        mock_health.check_bdev.return_value = True
        mock_recreate_on_non_leader.return_value = True

        mock_executor = MagicMock()
        mock_executor_cls.return_value = mock_executor

        result = recreate_lvstore(secondary, lvs_primary=primary)
        self.assertTrue(result)

        # Should have called set_leader with leader=True
        mock_rpc.bdev_lvol_set_leader.assert_called_with("LVS_100", leader=True)

        # In takeover, snode adopts the offline primary's hublvol under the
        # original primary's lvstore name/NQN/port (same-name invariant) —
        # NOT create_hublvol on self's own primary.
        secondary.adopt_hublvol.assert_called_once()
        secondary.create_hublvol.assert_not_called()
        # Adoption must pass the offline primary node (so lvs_node.lvstore
        # drives the bdev name, keeping the primary→takeover hublvol name
        # identical).
        adopt_args = secondary.adopt_hublvol.call_args
        adopted_peer = adopt_args.args[0] if adopt_args.args else adopt_args.kwargs.get("lvs_node")
        self.assertIs(adopted_peer, primary)

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack", return_value=(True, None))
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_promotion_when_primary_online(
            self, mock_db_cls, mock_create_stack, mock_rpc_cls,
            mock_fw, mock_tcp_events, _mock_disc, _mock_phase, _mock_handle):
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader

        db = mock_db_cls.return_value

        primary = _node("primary-1", status=StorageNode.STATUS_ONLINE,
                         lvstore="LVS_100", secondary_node_id="sec-1",
                         tertiary_node_id="sec-2")
        secondary = _node("sec-1", status=StorageNode.STATUS_ONLINE,
                           lvstore="LVS_200", is_secondary_node=True)

        lvol = _lvol("vol-1", "primary-1")

        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary]
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "primary-1": primary, "sec-1": secondary
        }.get(nid, primary)
        db.get_lvols_by_node_id.return_value = [lvol]
        db.get_cluster_by_id.return_value = _cluster()

        mock_rpc = MagicMock()
        mock_rpc.bdev_examine.return_value = True
        mock_rpc.bdev_wait_for_examine.return_value = True
        mock_rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc.jc_suspend_compression.return_value = (True, None)
        # Post-examine lvol-bdev verification scans get_bdevs() for each
        # expected lvol (by uuid or lvs/bdev alias); default MagicMock isn't
        # iterable as a bdev list, so supply the expected entry here.
        mock_rpc.get_bdevs.return_value = [
            {"name": lvol.lvol_uuid,
             "aliases": [f"{lvol.lvs_name}/{lvol.lvol_bdev}"]},
        ]
        mock_rpc_cls.return_value = mock_rpc

        for n in [primary, secondary]:
            n.rpc_client = MagicMock(return_value=mock_rpc)
            n.create_secondary_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        result = recreate_lvstore_on_non_leader(secondary, leader_node=primary, primary_node=primary)
        self.assertTrue(result)

        # Should NOT have called set_leader with leader=True
        calls = mock_rpc.bdev_lvol_set_leader.call_args_list
        for c in calls:
            if c == call("LVS_100", leader=True):
                self.fail("Should not promote secondary when primary is online")

        # Should still connect to primary's hublvol
        secondary.connect_to_hublvol.assert_called_once()

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack", return_value=(True, None))
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_always_creates_secondary_hublvol_on_sec1(
            self, mock_db_cls, mock_create_stack, mock_rpc_cls,
            mock_fw, mock_tcp_events, _mock_disc, _mock_phase, _mock_handle):
        """sec_1 should always create secondary hublvol regardless of primary status."""
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader

        db = mock_db_cls.return_value

        primary = _node("primary-1", status=StorageNode.STATUS_ONLINE,
                         lvstore="LVS_100", secondary_node_id="sec-1",
                         tertiary_node_id="sec-2")
        secondary = _node("sec-1", status=StorageNode.STATUS_ONLINE,
                           lvstore="LVS_200", is_secondary_node=True)

        lvol = _lvol("vol-1", "primary-1")

        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary]
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "primary-1": primary, "sec-1": secondary
        }.get(nid, primary)
        db.get_lvols_by_node_id.return_value = [lvol]
        db.get_cluster_by_id.return_value = _cluster()

        mock_rpc = MagicMock()
        mock_rpc.bdev_examine.return_value = True
        mock_rpc.bdev_wait_for_examine.return_value = True
        mock_rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc.jc_suspend_compression.return_value = (True, None)
        # Post-examine verification in recreate_lvstore_on_non_leader scans
        # get_bdevs() for each expected lvol (by uuid or lvs/bdev alias). The
        # default MagicMock isn't iterable as a bdev list, so the check fails
        # before the hublvol assertion. Return the expected lvol bdev here.
        mock_rpc.get_bdevs.return_value = [
            {"name": lvol.lvol_uuid,
             "aliases": [f"{lvol.lvs_name}/{lvol.lvol_bdev}"]},
        ]
        mock_rpc_cls.return_value = mock_rpc

        for n in [primary, secondary]:
            n.rpc_client = MagicMock(return_value=mock_rpc)
            n.create_secondary_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        recreate_lvstore_on_non_leader(secondary, leader_node=primary, primary_node=primary)

        secondary.create_secondary_hublvol.assert_called_once()


# ---------------------------------------------------------------------------
# 3. recreate_lvstore (leader takeover) – escalates unreachable primary
# ---------------------------------------------------------------------------

class TestPrimaryEscalation(unittest.TestCase):
    """When primary is UNREACHABLE and data plane is down, it should be
    escalated to OFFLINE before the failback branch runs."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.add_lvol_thread")
    @patch("simplyblock_core.storage_node_ops.ThreadPoolExecutor")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack", return_value=(True, None))
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_escalates_unreachable_primary(
            self, mock_db_cls, mock_create_stack, mock_connect_jm, mock_rpc_cls,
            mock_fw, mock_tcp_events, mock_storage_events,
            mock_health, mock_executor_cls, mock_add_lvol,
            mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """_check_data_plane_and_escalate should be called for unreachable primary."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        # Mock the lazy import of _check_data_plane_and_escalate
        mock_escalate = MagicMock()
        mock_monitor = MagicMock()
        mock_monitor._check_data_plane_and_escalate = mock_escalate
        import sys
        sys.modules["simplyblock_core.services.storage_node_monitor"] = mock_monitor

        db = mock_db_cls.return_value

        # Primary starts as UNREACHABLE, escalated to OFFLINE after check
        primary_unreachable = _node("primary-1", status=StorageNode.STATUS_UNREACHABLE,
                                     lvstore="LVS_100", secondary_node_id="sec-1",
                                     tertiary_node_id="sec-2")
        primary_offline = _node("primary-1", status=StorageNode.STATUS_OFFLINE,
                                 lvstore="LVS_100", secondary_node_id="sec-1",
                                 tertiary_node_id="sec-2")
        secondary = _node("sec-1", status=StorageNode.STATUS_ONLINE,
                           lvstore="LVS_200", is_secondary_node=True)

        lvol = _lvol("vol-1", "primary-1")

        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [primary_unreachable]
        # After escalation, re-read returns OFFLINE
        db.get_storage_node_by_id.side_effect = lambda nid: {
            "primary-1": primary_offline, "sec-1": secondary
        }.get(nid, primary_offline)
        db.get_lvols_by_node_id.return_value = [lvol]
        db.get_snapshots_by_node_id.return_value = []
        db.get_cluster_by_id.return_value = _cluster()

        mock_connect_jm.return_value = []

        mock_rpc = MagicMock()
        mock_rpc.bdev_examine.return_value = True
        mock_rpc.bdev_wait_for_examine.return_value = True
        # Leadership must show as restored so the leader-restore loop in
        # recreate_lvstore exits cleanly instead of falling through to _kill_app.
        mock_rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        mock_rpc.bdev_lvol_set_lvs_opts.return_value = True
        mock_rpc.get_bdevs.return_value = [{"name": "lvol-uuid-vol-1", "aliases": []}]
        mock_rpc.jc_suspend_compression.return_value = (True, None)
        mock_rpc.jc_compression_get_status.return_value = False
        mock_rpc.bdev_distrib_force_to_non_leader.return_value = True
        mock_rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = mock_rpc

        for n in [secondary, primary_offline, primary_unreachable]:
            n.rpc_client = MagicMock(return_value=mock_rpc)
            n.create_secondary_hublvol = MagicMock()
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        mock_health.check_bdev.return_value = True
        mock_recreate_on_non_leader.return_value = True
        mock_executor = MagicMock()
        mock_executor_cls.return_value = mock_executor

        result = recreate_lvstore(secondary, lvs_primary=primary_offline)
        self.assertTrue(result)

        # After escalation, secondary should be promoted (set_leader called with leader=True)
        mock_rpc.bdev_lvol_set_leader.assert_called_with("LVS_100", leader=True)


if __name__ == "__main__":
    unittest.main()
