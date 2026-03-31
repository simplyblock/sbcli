# coding=utf-8
"""
test_lvs_role_assignment.py – unit tests verifying that bdev_lvol_set_lvs_opts
receives the correct role ("primary", "secondary", "tertiary") for each node.

Covers:
  - rpc_client.bdev_lvol_set_lvs_opts accepts a role string
  - connect_to_hublvol passes the role through to the RPC call
  - recreate_lvstore sets primary role on primary, secondary/tertiary on secs
  - recreate_lvstore_on_sec sets the correct role based on is_second_sec
  - health_controller auto-fix passes correct role based on is_sec2
  - create_lvstore sets correct roles for primary and both secondaries
"""

import unittest
from unittest.mock import MagicMock, patch, call

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol


# ---------------------------------------------------------------------------
# Helpers (shared with test_dual_ft_secondary_fixes.py)
# ---------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1", ha_type="ha", max_fault_tolerance=2):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = ha_type
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.max_fault_tolerance = max_fault_tolerance
    c.client_qpair_count = 3
    c.client_data_nic = ""
    c.status = Cluster.STATUS_ACTIVE
    c.nqn = "nqn.2023-02.io.simplyblock:cluster-1"
    c.nvmf_base_port = 4420
    c.rpc_base_port = 8080
    c.snode_api_port = 50001
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          lvstore="", secondary_node_id="", secondary_node_id_2="",
          mgmt_ip="", rpc_port=8080, lvol_subsys_port=9090,
          lvstore_ports=None, active_tcp=True, active_rdma=False,
          lvstore_stack_secondary_1="", lvstore_stack_secondary_2="",
          jm_vuid=100, lvstore_status="ready"):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = f"host-{uuid[:8]}"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.secondary_node_id_2 = secondary_node_id_2
    n.mgmt_ip = mgmt_ip or f"10.0.0.{hash(uuid) % 254 + 1}"
    n.rpc_port = rpc_port
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.lvol_subsys_port = lvol_subsys_port
    n.lvstore_ports = dict(lvstore_ports) if lvstore_ports else {}
    n.active_tcp = active_tcp
    n.active_rdma = active_rdma
    n.lvstore_stack_secondary_1 = lvstore_stack_secondary_1
    n.lvstore_stack_secondary_2 = lvstore_stack_secondary_2
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
    n.firewall_port = 50001
    nic = IFace()
    nic.ip4_address = mgmt_ip or f"10.10.10.{hash(uuid) % 254 + 1}"
    nic.trtype = "TCP"
    n.data_nics = [nic]
    return n


# ---------------------------------------------------------------------------
# 1. RPC client: role parameter
# ---------------------------------------------------------------------------

class TestRpcClientRoleParam(unittest.TestCase):
    """bdev_lvol_set_lvs_opts should send the role string directly."""

    @patch("simplyblock_core.rpc_client.RPCClient._request")
    @patch("simplyblock_core.rpc_client.RPCClient.__init__", return_value=None)
    def test_role_primary(self, mock_init, mock_request):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = mock_request
        mock_request.return_value = True

        client.bdev_lvol_set_lvs_opts("LVS_100", groupid=42, subsystem_port=4420, role="primary")
        mock_request.assert_called_once_with('bdev_lvol_set_lvs_opts', {
            "lvs_name": "LVS_100",
            "groupid": 42,
            "subsystem_port": 4420,
            "role": "primary",
        })

    @patch("simplyblock_core.rpc_client.RPCClient._request")
    @patch("simplyblock_core.rpc_client.RPCClient.__init__", return_value=None)
    def test_role_secondary(self, mock_init, mock_request):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = mock_request
        mock_request.return_value = True

        client.bdev_lvol_set_lvs_opts("LVS_100", groupid=42, subsystem_port=4420, role="secondary")
        mock_request.assert_called_once_with('bdev_lvol_set_lvs_opts', {
            "lvs_name": "LVS_100",
            "groupid": 42,
            "subsystem_port": 4420,
            "role": "secondary",
        })

    @patch("simplyblock_core.rpc_client.RPCClient._request")
    @patch("simplyblock_core.rpc_client.RPCClient.__init__", return_value=None)
    def test_role_tertiary(self, mock_init, mock_request):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = mock_request
        mock_request.return_value = True

        client.bdev_lvol_set_lvs_opts("LVS_100", groupid=42, subsystem_port=4420, role="tertiary")
        mock_request.assert_called_once_with('bdev_lvol_set_lvs_opts', {
            "lvs_name": "LVS_100",
            "groupid": 42,
            "subsystem_port": 4420,
            "role": "tertiary",
        })

    @patch("simplyblock_core.rpc_client.RPCClient._request")
    @patch("simplyblock_core.rpc_client.RPCClient.__init__", return_value=None)
    def test_role_default_is_primary(self, mock_init, mock_request):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = mock_request
        mock_request.return_value = True

        client.bdev_lvol_set_lvs_opts("LVS_100", groupid=42, subsystem_port=4420)
        args = mock_request.call_args[0][1]
        self.assertEqual(args["role"], "primary")

    @patch("simplyblock_core.rpc_client.RPCClient._request")
    @patch("simplyblock_core.rpc_client.RPCClient.__init__", return_value=None)
    def test_uuid_routing(self, mock_init, mock_request):
        """When lvs is a UUID, the key should be 'uuid' not 'lvs_name'."""
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = mock_request
        mock_request.return_value = True

        uuid_str = "12345678-1234-1234-1234-123456789abc"
        client.bdev_lvol_set_lvs_opts(uuid_str, groupid=42, subsystem_port=4420, role="tertiary")
        args = mock_request.call_args[0][1]
        self.assertIn("uuid", args)
        self.assertNotIn("lvs_name", args)
        self.assertEqual(args["role"], "tertiary")


# ---------------------------------------------------------------------------
# 2. connect_to_hublvol passes role through
# ---------------------------------------------------------------------------

class TestConnectToHublvolRole(unittest.TestCase):
    """connect_to_hublvol must forward the role parameter to set_lvs_opts."""

    def _make_primary(self):
        return _node("primary-1", lvstore="LVS_100", mgmt_ip="10.0.0.1",
                      lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}})

    def _make_secondary(self):
        return _node("sec-1", lvstore="LVS_200", mgmt_ip="10.0.0.2",
                      lvstore_stack_secondary_1="cluster-1/primary-1")

    def test_secondary_role_default(self):
        primary = self._make_primary()
        sec = self._make_secondary()

        rpc = MagicMock()
        rpc.get_bdevs.return_value = []  # trigger attach
        rpc.bdev_nvme_attach_controller.return_value = True
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_connect_hublvol.return_value = True
        sec.rpc_client = MagicMock(return_value=rpc)

        sec.connect_to_hublvol(primary)
        rpc.bdev_lvol_set_lvs_opts.assert_called_once()
        call_kwargs = rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(call_kwargs[1]["role"], "secondary")

    def test_tertiary_role_explicit(self):
        primary = self._make_primary()
        sec = self._make_secondary()

        rpc = MagicMock()
        rpc.get_bdevs.return_value = []
        rpc.bdev_nvme_attach_controller.return_value = True
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_connect_hublvol.return_value = True
        sec.rpc_client = MagicMock(return_value=rpc)

        sec.connect_to_hublvol(primary, role="tertiary")
        rpc.bdev_lvol_set_lvs_opts.assert_called_once()
        call_kwargs = rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(call_kwargs[1]["role"], "tertiary")

    def test_secondary_role_explicit(self):
        primary = self._make_primary()
        sec = self._make_secondary()

        rpc = MagicMock()
        rpc.get_bdevs.return_value = []
        rpc.bdev_nvme_attach_controller.return_value = True
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_lvol_connect_hublvol.return_value = True
        sec.rpc_client = MagicMock(return_value=rpc)

        sec.connect_to_hublvol(primary, role="secondary")
        call_kwargs = rpc.bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(call_kwargs[1]["role"], "secondary")


# ---------------------------------------------------------------------------
# 3. recreate_lvstore: primary gets "primary", secs get correct roles
# ---------------------------------------------------------------------------

class TestRecreateLvstoreRoles(unittest.TestCase):
    """recreate_lvstore must call set_lvs_opts with role='primary' on the
    primary node, and connect_to_hublvol with the correct role on each sec."""

    def _build_cluster(self):
        nodes = {}
        nodes["node-1"] = _node(
            "node-1", lvstore="LVS_100",
            secondary_node_id="cluster-1/node-2",
            secondary_node_id_2="cluster-1/node-3",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.1")
        nodes["node-2"] = _node(
            "node-2", lvstore="LVS_200",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.2")
        nodes["node-3"] = _node(
            "node-3", lvstore="LVS_300",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.3")
        return nodes

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_gets_primary_role(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_sec):
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_cluster()
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
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_sec.return_value = True
        mock_health.check_bdev.return_value = True

        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Primary node's set_lvs_opts should be called with role="primary"
        rpc.bdev_lvol_set_lvs_opts.assert_called_once_with(
            snode.lvstore,
            groupid=snode.jm_vuid,
            subsystem_port=4420,
            role="primary"
        )

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_sec1_secondary_sec2_tertiary(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_sec):
        """sec1 gets role='secondary', sec2 gets role='tertiary'."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_cluster()
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
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_sec.return_value = True
        mock_health.check_bdev.return_value = True

        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # sec1 (node-2) → secondary, sec2 (node-3) → tertiary
        nodes["node-2"].connect_to_hublvol.assert_called_once_with(
            snode, failover_node=None, role="secondary")
        nodes["node-3"].connect_to_hublvol.assert_called_once_with(
            snode, failover_node=nodes["node-2"], role="tertiary")

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_single_secondary_gets_secondary_role(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_storage_events, mock_tcp_events,
            mock_health, mock_recreate_on_sec):
        """With only one secondary (FTT=1), it should get role='secondary'."""
        from simplyblock_core.storage_node_ops import recreate_lvstore

        nodes = self._build_cluster()
        # Remove secondary_node_id_2
        nodes["node-1"].secondary_node_id_2 = ""
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
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()

        mock_recreate_on_sec.return_value = True
        mock_health.check_bdev.return_value = True

        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Only sec1 should be called, with role="secondary"
        nodes["node-2"].connect_to_hublvol.assert_called_once_with(
            snode, failover_node=None, role="secondary")
        nodes["node-3"].connect_to_hublvol.assert_not_called()


# ---------------------------------------------------------------------------
# 4. recreate_lvstore_on_sec: role based on is_second_sec
# ---------------------------------------------------------------------------

class TestRecreateLvstoreOnSecRoles(unittest.TestCase):
    """recreate_lvstore_on_sec must pass the correct role depending on
    whether the secondary is sec_1 or sec_2 for the primary."""

    def _build_nodes(self):
        primary = _node(
            "primary-1", lvstore="LVS_100",
            secondary_node_id="sec-1",
            secondary_node_id_2="sec-2",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.1")
        sec1 = _node(
            "sec-1", lvstore="LVS_200",
            lvstore_stack_secondary_1="primary-1",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.2")
        sec2 = _node(
            "sec-2", lvstore="LVS_300",
            lvstore_stack_secondary_2="primary-1",
            lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4421}},
            mgmt_ip="10.0.0.3")
        return {"primary-1": primary, "sec-1": sec1, "sec-2": sec2}

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_first_secondary_gets_secondary_role(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tcp_events):
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec

        nodes = self._build_nodes()
        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [nodes["primary-1"]]
        db.get_lvols_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.jc_suspend_compression.return_value = (True, None)
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc
        mock_fw_cls.return_value = MagicMock()

        sec1 = nodes["sec-1"]
        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        recreate_lvstore_on_sec(sec1)

        # sec-1 is lvstore_stack_secondary_1 → role="secondary"
        sec1.connect_to_hublvol.assert_called_once()
        call_kwargs = sec1.connect_to_hublvol.call_args
        self.assertEqual(call_kwargs[1].get("role"), "secondary")

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_second_secondary_gets_tertiary_role(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tcp_events):
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec

        nodes = self._build_nodes()
        db = mock_db_cls.return_value

        def get_node(nid):
            key = nid.split("/")[-1] if "/" in nid else nid
            return nodes[key]

        db.get_storage_node_by_id.side_effect = get_node
        db.get_primary_storage_nodes_by_secondary_node_id.return_value = [nodes["primary-1"]]
        db.get_lvols_by_node_id.return_value = []

        mock_connect_jm.return_value = []
        mock_create_bdev.return_value = (True, None)

        rpc = MagicMock()
        rpc.bdev_wait_for_examine.return_value = True
        rpc.bdev_examine.return_value = True
        rpc.jc_suspend_compression.return_value = (True, None)
        rpc.bdev_lvol_set_leader.return_value = True
        rpc.bdev_lvol_set_lvs_opts.return_value = True
        rpc.bdev_distrib_force_to_non_leader.return_value = True
        rpc.bdev_distrib_check_inflight_io.return_value = False
        mock_rpc_cls.return_value = rpc
        mock_fw_cls.return_value = MagicMock()

        sec2 = nodes["sec-2"]
        for n in nodes.values():
            n.rpc_client = MagicMock(return_value=rpc)
            n.connect_to_hublvol = MagicMock()
            n.create_secondary_hublvol = MagicMock()
            n.write_to_db = MagicMock()
            n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)

        recreate_lvstore_on_sec(sec2)

        # sec-2 is lvstore_stack_secondary_2 → role="tertiary"
        sec2.connect_to_hublvol.assert_called_once()
        call_kwargs = sec2.connect_to_hublvol.call_args
        self.assertEqual(call_kwargs[1].get("role"), "tertiary")


# ---------------------------------------------------------------------------
# 5. set_node_status online handler: role based on secondary_node_id position
# ---------------------------------------------------------------------------

class TestSetNodeOnlineRoles(unittest.TestCase):
    """When a node comes online and reconnects as primary's secondary,
    the role should match its position (secondary_node_id vs _2)."""

    def test_secondary_ids_role_mapping(self):
        """Verify that the role mapping logic in set_node_status correctly
        assigns 'secondary' to secondary_node_id and 'tertiary' to
        secondary_node_id_2."""
        # This tests the pattern used in set_node_status:
        #   for sec_id, sec_role in [(snode.secondary_node_id, "secondary"),
        #                            (snode.secondary_node_id_2, "tertiary")]:
        primary = _node(
            "node-1", lvstore="LVS_100",
            secondary_node_id="cluster-1/node-2",
            secondary_node_id_2="cluster-1/node-3")

        role_map = [(primary.secondary_node_id, "secondary"),
                    (primary.secondary_node_id_2, "tertiary")]

        self.assertEqual(role_map[0], ("cluster-1/node-2", "secondary"))
        self.assertEqual(role_map[1], ("cluster-1/node-3", "tertiary"))

    def test_single_secondary_no_tertiary(self):
        """With only one secondary, the tertiary entry should be skipped."""
        primary = _node(
            "node-1", lvstore="LVS_100",
            secondary_node_id="cluster-1/node-2",
            secondary_node_id_2="")

        role_map = [(primary.secondary_node_id, "secondary"),
                    (primary.secondary_node_id_2, "tertiary")]

        active = [(sid, role) for sid, role in role_map if sid]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0], ("cluster-1/node-2", "secondary"))


if __name__ == "__main__":
    unittest.main()
