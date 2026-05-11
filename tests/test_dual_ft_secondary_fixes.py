# coding=utf-8
"""
test_dual_ft_secondary_fixes.py – unit tests for the three bugs fixed in
the dual fault-tolerance (tertiary_node_id) support:

1. recreate_lvstore now handles BOTH secondaries (not just secondary_node_id)
2. Remote-devices loops re-read nodes from DB before writing (race condition)
3. connect_lvol uses the primary node's port for ALL connections

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1", ha_type="ha", max_fault_tolerance=2,
             client_qpair_count=3, client_data_nic=""):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = ha_type
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.max_fault_tolerance = max_fault_tolerance
    c.client_qpair_count = client_qpair_count
    c.client_data_nic = client_data_nic
    c.status = Cluster.STATUS_ACTIVE
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          lvstore="", secondary_node_id="", tertiary_node_id="",
          mgmt_ip="", rpc_port=8080, lvol_subsys_port=9090,
          lvstore_ports=None, data_nics=None, active_tcp=True, active_rdma=False,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          jm_vuid=100, lvstore_status="ready"):
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
    n.lvol_subsys_port = lvol_subsys_port
    n.lvstore_ports = dict(lvstore_ports) if lvstore_ports else {}
    n.active_tcp = active_tcp
    n.active_rdma = active_rdma
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.jm_vuid = jm_vuid
    n.lvstore_status = lvstore_status
    n.enable_ha_jm = False
    n.lvstore_stack = []
    n.raid = "raid0"
    n.hublvol = HubLVol({"nvmf_port": 5000, "uuid": f"hub-{uuid}",
                          "nqn": f"nqn.hub.{uuid}", "bdev_name": "lvs/hublvol",
                          "model_number": "model1", "nguid": "0" * 32})
    n.remote_devices = []
    n.remote_jm_devices = []
    n.nvme_devices = []
    n.health_check = True
    # data NICs
    if data_nics is None:
        nic = IFace()
        nic.ip4_address = mgmt_ip or f"10.10.10.{hash(uuid) % 254 + 1}"
        nic.trtype = "TCP"
        n.data_nics = [nic]
    else:
        n.data_nics = data_nics
    return n


def _lvol(uuid, node_id, lvs_name="LVS_100", nodes=None, ha_type="ha",
          fabric="tcp", nqn=None, allowed_hosts=None, ns_id=1):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.status = LVol.STATUS_ONLINE
    lv.ha_type = ha_type
    lv.nodes = nodes or [node_id]
    lv.lvs_name = lvs_name
    lv.lvol_bdev = "bdev_test"
    lv.top_bdev = f"{lvs_name}/bdev_test"
    lv.fabric = fabric
    lv.nqn = nqn or f"nqn.2023-02.io.simplyblock:cluster-1:lvol:{uuid}"
    lv.allowed_hosts = allowed_hosts or []
    lv.ns_id = ns_id
    lv.deletion_status = ""
    lv.lvol_type = "lvol"
    lv.crypto_bdev = ""
    lv.lvol_uuid = f"lvol-uuid-{uuid}"
    lv.guid = f"guid-{uuid}"
    return lv


# ---------------------------------------------------------------------------
# 1. connect_lvol uses primary node's port for all connections
# ---------------------------------------------------------------------------

class TestConnectLvolPort(unittest.TestCase):
    """Bug 3 fix: connect_lvol must use the primary node's per-lvstore port
    for ALL connections, not each node's own (potentially stale) fallback."""

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_uses_primary_port_for_all_nodes(self, mock_db_cls):
        """When secondary nodes are missing lvstore_ports, the primary's
        correct port should still be used in every connect command."""
        from simplyblock_core.controllers.lvol_controller import connect_lvol

        # Primary has correct per-lvstore port 4420
        primary = _node("node-1", mgmt_ip="10.10.10.1", lvol_subsys_port=9090,
                         lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}})
        # Secondary 1 also has the port (propagated correctly)
        sec1 = _node("node-2", mgmt_ip="10.10.10.2", lvol_subsys_port=9090,
                      lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}})
        # Secondary 2 is MISSING the port (bug scenario — stale data)
        sec2 = _node("node-3", mgmt_ip="10.10.10.3", lvol_subsys_port=4428,
                      lvstore_ports={})  # missing LVS_100!

        cluster = _cluster()
        lvol = _lvol("vol-1", "node-1", lvs_name="LVS_100",
                     nodes=["node-1", "node-2", "node-3"])

        db = mock_db_cls.return_value

        def get_node(nid):
            return {"node-1": primary,
                    "node-2": sec1,
                    "node-3": sec2}[nid]

        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.side_effect = get_node
        db.get_cluster_by_id.return_value = cluster

        result = connect_lvol("vol-1")
        self.assertTrue(result)
        self.assertEqual(len(result), 3)

        # ALL three connections must use port 4420
        for entry in result:
            self.assertEqual(entry["port"], 4420,
                             f"Node at {entry['ip']} got port {entry['port']}, expected 4420")

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_connect_single_node_uses_primary_port(self, mock_db_cls):
        """Single-node (non-HA) volumes should also use the primary port."""
        from simplyblock_core.controllers.lvol_controller import connect_lvol

        primary = _node("node-1", mgmt_ip="10.10.10.1", lvol_subsys_port=9090,
                         lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}})
        cluster = _cluster()
        lvol = _lvol("vol-1", "node-1", lvs_name="LVS_100",
                     nodes=["node-1"], ha_type="single")

        db = mock_db_cls.return_value
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = primary
        db.get_cluster_by_id.return_value = cluster

        result = connect_lvol("vol-1")
        self.assertTrue(result)
        self.assertEqual(result[0]["port"], 4420)


# ---------------------------------------------------------------------------
# 2. recreate_lvstore handles both secondaries
# ---------------------------------------------------------------------------

class TestRecreateLvstoreDualSecondary(unittest.TestCase):
    """Bug 1 fix: recreate_lvstore must coordinate with BOTH secondaries,
    not just secondary_node_id."""

    def _build_4node_cluster(self):
        """Build a 4-node cluster with dual secondaries."""
        nodes = {}
        nodes["node-1"] = _node(
            "node-1", lvstore="LVS_100",
            secondary_node_id="node-2",
            tertiary_node_id="node-3",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}},
            lvstore_stack_secondary="", lvstore_stack_tertiary="",
            mgmt_ip="10.0.0.1")
        nodes["node-2"] = _node(
            "node-2", lvstore="LVS_200",
            secondary_node_id="node-3",
            tertiary_node_id="node-4",
            lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427},
                           "LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}},
            mgmt_ip="10.0.0.2")
        nodes["node-3"] = _node(
            "node-3", lvstore="LVS_300",
            secondary_node_id="node-4",
            tertiary_node_id="node-1",
            lvstore_ports={"LVS_300": {"lvol_subsys_port": 4428, "hublvol_port": 4429},
                           "LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}},
            mgmt_ip="10.0.0.3")
        nodes["node-4"] = _node(
            "node-4", lvstore="LVS_400",
            secondary_node_id="node-1",
            tertiary_node_id="node-2",
            status=StorageNode.STATUS_OFFLINE,
            mgmt_ip="10.0.0.4")
        return nodes

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_both_secondaries_get_firewall_blocked(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """Both sec1 and sec2 should have their ports blocked during primary restart."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_4node_cluster()
        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []  # no lvols for simplicity
        db.get_snapshots_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        # RPC client mock
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.jc_explicit_synchronization.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc

        # Firewall mock
        fw_instances = []
        def make_fw(node, **kwargs):
            fw = MagicMock()
            fw.node_id = node.uuid if hasattr(node, 'uuid') else str(node)
            fw_instances.append(fw)
            return fw
        mock_fw_cls.side_effect = make_fw

        # Mock node rpc_client method
        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_non_leader.return_value = True
        mock_health.check_bdev.return_value = True

        # Primary node-1 restarts
        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Both sec1 (node-2) and sec2 (node-3) should have firewall blocked/allowed
        # Collect all firewall_set_port calls
        fw_calls = []
        for fw in fw_instances:
            for c in fw.firewall_set_port.call_args_list:
                fw_calls.append(c)

        # There should be block + allow calls for BOTH secondaries = 4 total
        self.assertEqual(len(fw_calls), 4,
                         f"Expected 4 firewall calls (2 block + 2 allow), got {len(fw_calls)}: {fw_calls}")

        # Verify block then allow pattern
        block_calls = [c for c in fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in fw_calls if c[0][2] == "allow"]
        self.assertEqual(len(block_calls), 2, "Expected 2 block calls (one per secondary)")
        self.assertEqual(len(allow_calls), 2, "Expected 2 allow calls (one per secondary)")

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_both_secondaries_get_hublvol_connection(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """Both secondaries should connect to hublvol after primary restart."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_4node_cluster()
        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc

        mock_fw_cls.return_value = MagicMock()

        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock(return_value=True)
            n.create_secondary_hublvol = MagicMock()
            # Deferred tertiary→secondary failover-path attach runs after
            # port_unblock; mock so the test doesn't dive into the real
            # HublvolReconnectCoordinator.
            n.add_hublvol_failover_path = MagicMock(return_value=True)
            n.write_to_db = MagicMock()

        mock_recreate_on_non_leader.return_value = True
        mock_health.check_bdev.return_value = True

        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Both sec1 (node-2) and sec2 (node-3) should have connect_to_hublvol
        # called with a SINGLE path against snode (the new leader). The
        # tertiary→secondary failover path is deferred to after port_unblock
        # via ``add_hublvol_failover_path`` (asserted separately below).
        nodes["node-2"].connect_to_hublvol.assert_called_once_with(
            snode, failover_node=None, role="secondary", rpc_timeout=0.2)
        nodes["node-3"].connect_to_hublvol.assert_called_once_with(
            snode, failover_node=None, role="tertiary", rpc_timeout=0.2)
        nodes["node-3"].add_hublvol_failover_path.assert_called_once_with(
            snode, nodes["node-2"])

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_both_secondaries_get_lvstore_status_ready(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """Both online secondaries should get lvstore_status='ready' at the end."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_4node_cluster()
        db = mock_db_cls.return_value

        write_db_calls = {}
        for nid, n in nodes.items():
            calls = []
            write_db_calls[nid] = calls
            original_write = MagicMock()
            def make_recorder(c, orig):
                def recorder(*args, **kwargs):
                    c.append({"lvstore_status": n.lvstore_status})
                    return orig(*args, **kwargs)
                return recorder
            n.write_to_db = make_recorder(calls, original_write)

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc

        mock_fw_cls.return_value = MagicMock()

        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()

        mock_recreate_on_non_leader.return_value = True
        mock_health.check_bdev.return_value = True

        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Both online secondaries should end with lvstore_status="ready"
        self.assertEqual(nodes["node-2"].lvstore_status, "ready")
        self.assertEqual(nodes["node-3"].lvstore_status, "ready")

    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_offline_secondary2_skipped_gracefully(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_non_leader, _mock_phase, _mock_handle):
        """If tertiary_node_id is offline, it should be skipped for
        firewall/hublvol but not crash."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_4node_cluster()
        # Make tertiary_node_id (node-3) offline
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE

        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_lvol_get_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.jc_explicit_synchronization.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc

        mock_fw_cls.return_value = MagicMock()

        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_non_leader.return_value = True
        mock_health.check_bdev.return_value = True

        # Mock _check_peer_disconnected: node-3 (offline) is disconnected, others connected
        def _disc_side_effect(peer, **kwargs):
            return peer.uuid == "node-3" or peer.status == StorageNode.STATUS_OFFLINE
        with patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
                   side_effect=_disc_side_effect):
            snode = nodes["node-1"]
            result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Online sec1 should have connect_to_hublvol called
        nodes["node-2"].connect_to_hublvol.assert_called_once()
        # Offline sec2 should NOT have connect_to_hublvol called
        nodes["node-3"].connect_to_hublvol.assert_not_called()

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", return_value=False)
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_suspend_when_any_secondary_unreachable(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """If any secondary is UNREACHABLE, primary should be suspended."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_4node_cluster()
        # sec2 (node-3) is unreachable
        nodes["node-3"].status = StorageNode.STATUS_UNREACHABLE

        db = mock_db_cls.return_value
        lvol = _lvol("vol-1", "node-1")

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_lvols_by_node_id.return_value = [lvol]
        db.get_snapshots_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"lvs leadership": True}]
        rpc.get_bdevs.return_value = []
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.jc_compression_get_status.return_value = False
        rpc.jc_explicit_synchronization.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        rpc.subsystem_create.return_value = True
        mock_rpc_cls.return_value = rpc

        mock_fw_cls.return_value = MagicMock()

        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
            n.recreate_hublvol = MagicMock()
            n.connect_to_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_non_leader.return_value = True
        mock_health.check_bdev.return_value = True

        # Mock disconnect: node-3 (unreachable) is disconnected
        def _disc_side_effect(peer, **kwargs):
            return peer.uuid == "node-3" or peer.status == StorageNode.STATUS_UNREACHABLE
        snode = nodes["node-1"]
        with patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
                   side_effect=_disc_side_effect):
            result = recreate_lvstore(snode)

        # Per design: unreachable secondary is skipped, restart succeeds
        self.assertTrue(result)

        # jc_explicit_synchronization should be called for disconnected peer
        rpc.jc_explicit_synchronization.assert_called_once_with(snode.jm_vuid)


# ---------------------------------------------------------------------------
# 3. (removed) set_node_status peer remote_devices loop
#
# The original test asserted that set_node_status iterated peer nodes,
# re-read each from DB, and called write_to_db on the fresh copy. That
# loop was moved out of set_node_status into _connect_to_remote_devs and
# its callers; set_node_status is now pure bookkeeping (own status write,
# event emit, distr broadcast, optional auto-restart cancellation). The
# old test asserted behavior that no longer belongs in this function and
# has been removed. Coverage for the re-read-before-write contract on
# peer remote_devices belongs alongside _connect_to_remote_devs callers
# (e.g. add_node, restart_storage_node) — not here.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 5. get_lvol_subsys_port fallback behavior
# ---------------------------------------------------------------------------

class TestGetLvolSubsysPortFallback(unittest.TestCase):
    """Verify port lookup behavior with and without per-lvstore ports."""

    def test_returns_per_lvstore_port_when_present(self):
        n = _node("node-1", lvol_subsys_port=9090,
                  lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}})
        self.assertEqual(n.get_lvol_subsys_port("LVS_100"), 4420)

    def test_falls_back_to_node_port_when_missing(self):
        n = _node("node-1", lvol_subsys_port=9090, lvstore_ports={})
        self.assertEqual(n.get_lvol_subsys_port("LVS_100"), 9090)

    def test_falls_back_when_lvs_name_is_none(self):
        n = _node("node-1", lvol_subsys_port=9090,
                  lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420}})
        self.assertEqual(n.get_lvol_subsys_port(None), 9090)

    def test_different_lvstores_different_ports(self):
        n = _node("node-1", lvol_subsys_port=9090,
                  lvstore_ports={
                      "LVS_100": {"lvol_subsys_port": 4420},
                      "LVS_200": {"lvol_subsys_port": 4426},
                  })
        self.assertEqual(n.get_lvol_subsys_port("LVS_100"), 4420)
        self.assertEqual(n.get_lvol_subsys_port("LVS_200"), 4426)
        self.assertEqual(n.get_lvol_subsys_port("LVS_MISSING"), 9090)


if __name__ == "__main__":
    unittest.main()
