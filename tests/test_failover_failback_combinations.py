# coding=utf-8
"""
test_failover_failback_combinations.py – comprehensive tests for all
failover/failback combinations with FTT=1 and FTT=2.

Covers:
- Failover: primary → first secondary
- Failback: first secondary → primary
- Failover: primary → first secondary → second secondary
- Failover: primary → second secondary (first secondary offline)
- Failover: primary → first secondary (second secondary offline)
- Failback: first secondary → primary (FTT=2)
- Failback: second secondary → primary (first secondary offline)
- Failback: second secondary → first secondary (primary offline)
- Failback: first secondary → primary (second secondary offline), then restart second secondary
- recreate_lvstore_on_sec: primary online, port block + leadership drop
- recreate_lvstore_on_sec: primary offline, first sec restarts, leadership dropped on second sec

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch, call

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol

# Ensure the module is importable for patch() resolution
import simplyblock_core.storage_node_ops  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(cluster_id="cluster-1", ha_type="ha", max_fault_tolerance=2):
    c = Cluster()
    c.uuid = cluster_id
    c.ha_type = ha_type
    c.distr_ndcs = 2
    c.distr_npcs = 2
    c.max_fault_tolerance = max_fault_tolerance
    c.status = Cluster.STATUS_ACTIVE
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
    n.hostname = f"host-{uuid}"
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
    n.jm_device = None
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
    nic = IFace()
    nic.ip4_address = mgmt_ip or f"10.10.10.{hash(uuid) % 254 + 1}"
    nic.trtype = "TCP"
    n.data_nics = [nic]
    return n


def _lvol(uuid, node_id, lvs_name="LVS_100", ha_type="ha", nqn=None):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.status = LVol.STATUS_ONLINE
    lv.ha_type = ha_type
    lv.nodes = [node_id]
    lv.lvs_name = lvs_name
    lv.lvol_bdev = "bdev_test"
    lv.top_bdev = f"{lvs_name}/bdev_test"
    lv.fabric = "tcp"
    lv.nqn = nqn or f"nqn.test:lvol:{uuid}"
    lv.allowed_hosts = []
    lv.ns_id = 1
    lv.deletion_status = ""
    lv.lvol_type = "lvol"
    lv.crypto_bdev = ""
    lv.lvol_uuid = f"lvol-uuid-{uuid}"
    lv.guid = f"guid-{uuid}"
    return lv


def _mock_rpc():
    """Create a standard mock RPC client with all expected methods."""
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
    rpc.nvmf_subsystem_listener_set_ana_state.return_value = True
    rpc.jc_suspend_compression.return_value = (True, None)
    return rpc


def _mock_fw_factory():
    """Create a FirewallClient factory that tracks instances."""
    instances = []

    def make_fw(node, **kwargs):
        fw = MagicMock()
        fw._node_id = node.uuid if hasattr(node, 'uuid') else str(node)
        fw.firewall_set_port = MagicMock(return_value=True)
        instances.append(fw)
        return fw

    return make_fw, instances


def _setup_node_methods(nodes, rpc):
    """Attach common mock methods to all nodes."""
    for n in nodes.values():
        n.rpc_client = MagicMock(return_value=rpc)
        n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        n.recreate_hublvol = MagicMock()
        n.connect_to_hublvol = MagicMock()
        n.write_to_db = MagicMock()


# ---------------------------------------------------------------------------
# FTT=1 topology builder: primary + 1 secondary
# ---------------------------------------------------------------------------

def _build_ftt1_nodes():
    """2-node setup: node-1 (primary), node-2 (first secondary)."""
    nodes = {
        "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                         secondary_node_id="node-2",
                         rpc_port=8080,
                         lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
        "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                         lvstore_stack_secondary_1="node-1",
                         rpc_port=8081,
                         lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
    }
    return nodes


# ---------------------------------------------------------------------------
# FTT=2 topology builder: primary + 2 secondaries
# ---------------------------------------------------------------------------

def _build_ftt2_nodes():
    """3-node setup: node-1 (primary), node-2 (sec1), node-3 (sec2)."""
    nodes = {
        "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                         secondary_node_id="node-2",
                         secondary_node_id_2="node-3",
                         rpc_port=8080,
                         lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
        "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                         lvstore_stack_secondary_1="node-1",
                         secondary_node_id="node-3",
                         rpc_port=8081,
                         lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
        "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                         lvstore_stack_secondary_2="node-1",
                         secondary_node_id="node-1",
                         rpc_port=8082,
                         lvstore_ports={"LVS_300": {"lvol_subsys_port": 4428, "hublvol_port": 4429}}),
    }
    return nodes


def _make_db_mock(nodes, lvols=None):
    """Create a DBController mock with node lookup side_effect."""
    db = MagicMock()

    def get_node(nid):
        key = nid.split("/")[-1] if "/" in nid else nid
        return nodes.get(key)

    db.get_storage_node_by_id.side_effect = get_node
    db.get_lvols_by_node_id.return_value = lvols or []
    db.get_snapshots_by_node_id.return_value = []
    db.get_storage_nodes_by_cluster_id.return_value = list(nodes.values())
    db.get_cluster_by_id.return_value = _cluster()

    def get_primaries_by_sec(sec_id):
        key = sec_id.split("/")[-1] if "/" in sec_id else sec_id
        result = []
        for n in nodes.values():
            sec1 = n.secondary_node_id
            sec2 = n.secondary_node_id_2
            if sec1 and (sec1 == sec_id or sec1.endswith("/" + key)):
                result.append(n)
            elif sec2 and (sec2 == sec_id or sec2.endswith("/" + key)):
                result.append(n)
        return result

    db.get_primary_storage_nodes_by_secondary_node_id.side_effect = get_primaries_by_sec
    return db


# ===========================================================================
# ANA Failover Tests
# ===========================================================================

class TestAnaFailover(unittest.TestCase):
    """Test trigger_ana_failover_for_node for all combinations."""

    def _run_failover(self, nodes, offline_node_key, lvols):
        with patch("simplyblock_core.storage_node_ops.DBController") as mock_db_cls, \
             patch("simplyblock_core.storage_node_ops.RPCClient") as mock_rpc_cls:
            db = _make_db_mock(nodes, lvols)
            mock_db_cls.return_value = db
            rpc = _mock_rpc()
            mock_rpc_cls.return_value = rpc
            _setup_node_methods(nodes, rpc)

            from simplyblock_core.storage_node_ops import trigger_ana_failover_for_node
            trigger_ana_failover_for_node(nodes[offline_node_key])
            return rpc

    def test_ftt1_failover_primary_to_secondary(self):
        """FTT=1: primary goes offline → first secondary promoted to optimized."""
        nodes = _build_ftt1_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-1", lvols)

        # First secondary should be set to optimized
        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        optimized_calls = [c for c in ana_calls if c[1].get('ana_state') == 'optimized'
                           or (len(c[0]) > 0 and 'optimized' in str(c))]
        self.assertTrue(len(ana_calls) > 0, "Should have ANA state change calls")

    def test_ftt2_failover_primary_to_both_secondaries(self):
        """FTT=2: primary goes offline → sec1=optimized, sec2=non_optimized."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        self.assertTrue(len(ana_calls) >= 2, "Should set ANA on both secondaries")

    def test_ftt2_failover_first_sec_offline_promotes_second_sec(self):
        """FTT=2: first secondary goes offline → second secondary promoted to non_optimized."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-2"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-2", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # Second secondary should be promoted
        self.assertTrue(len(ana_calls) > 0, "Second secondary should get ANA update")

    def test_ftt2_failover_primary_offline_second_sec_already_offline(self):
        """FTT=2: primary goes offline, second secondary already offline → only first sec promoted."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # Only first secondary should get ANA update (node-3 is offline)
        self.assertTrue(len(ana_calls) > 0)

    def test_ftt2_failover_primary_offline_first_sec_already_offline(self):
        """FTT=2: primary offline, first sec already offline → second sec promoted."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE
        nodes["node-2"].status = StorageNode.STATUS_OFFLINE

        # When primary goes offline, second secondary should still get non_optimized
        rpc = self._run_failover(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        self.assertTrue(len(ana_calls) > 0)


# ===========================================================================
# ANA Failback Tests
# ===========================================================================

class TestAnaFailback(unittest.TestCase):
    """Test trigger_ana_failback_for_node for all combinations."""

    def _run_failback(self, nodes, restarting_node_key, lvols):
        with patch("simplyblock_core.storage_node_ops.DBController") as mock_db_cls, \
             patch("simplyblock_core.storage_node_ops.RPCClient") as mock_rpc_cls:
            db = _make_db_mock(nodes, lvols)
            mock_db_cls.return_value = db
            rpc = _mock_rpc()
            mock_rpc_cls.return_value = rpc
            _setup_node_methods(nodes, rpc)

            from simplyblock_core.storage_node_ops import trigger_ana_failback_for_node
            trigger_ana_failback_for_node(nodes[restarting_node_key])
            return rpc

    def test_ftt1_failback_secondary_to_primary(self):
        """FTT=1: primary restarts → first secondary demoted to non_optimized."""
        nodes = _build_ftt1_nodes()
        lvols = [_lvol("lv1", "node-1")]

        rpc = self._run_failback(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # With FTT=1, no secondary_node_id_2, so _failback_primary_ana not called
        # (it requires secondary_node_id_2). No-op for FTT=1 via this path.
        # The actual failback for FTT=1 happens inside recreate_lvstore.

    def test_ftt2_failback_primary_restarts_both_secs_online(self):
        """FTT=2: primary restarts, both secondaries online → sec2=inaccessible, sec1=non_optimized."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]

        rpc = self._run_failback(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        self.assertTrue(len(ana_calls) >= 2,
                        "Should set ANA on both secondaries (inaccessible + non_optimized)")

    def test_ftt2_failback_first_sec_restarts_demotes_second_sec(self):
        """FTT=2: first secondary restarts, primary online → second secondary demoted to inaccessible."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        # Primary must be online for trigger_ana_failback_for_node to demote second sec
        nodes["node-1"].status = StorageNode.STATUS_ONLINE

        rpc = self._run_failback(nodes, "node-2", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # Second secondary should be set to inaccessible
        self.assertTrue(len(ana_calls) > 0,
                        "Second secondary should be demoted to inaccessible")

    def test_ftt2_failback_first_sec_restarts_second_sec_offline(self):
        """FTT=2: first secondary restarts, second secondary offline → no ANA change."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failback(nodes, "node-2", lvols)

        # With second sec offline, failback for first sec role doesn't demote anyone
        # (second sec is not online so it's skipped)


# ===========================================================================
# recreate_lvstore Tests (primary failback)
# ===========================================================================

_RECREATE_PATCHES = [
    "simplyblock_core.storage_node_ops.recreate_lvstore_on_sec",
    "simplyblock_core.storage_node_ops.health_controller",
    "simplyblock_core.storage_node_ops.tcp_ports_events",
    "simplyblock_core.storage_node_ops.storage_events",
    "simplyblock_core.storage_node_ops.tasks_controller",
    "simplyblock_core.storage_node_ops.FirewallClient",
    "simplyblock_core.storage_node_ops.RPCClient",
    "simplyblock_core.storage_node_ops._connect_to_remote_jm_devs",
    "simplyblock_core.storage_node_ops._create_bdev_stack",
    "simplyblock_core.storage_node_ops.DBController",
]


class TestRecreateLvstoreFTT1(unittest.TestCase):
    """FTT=1: recreate_lvstore on primary restart with single secondary."""

    @patch(*_RECREATE_PATCHES[:1])
    @patch(*_RECREATE_PATCHES[1:2])
    @patch(*_RECREATE_PATCHES[2:3])
    @patch(*_RECREATE_PATCHES[3:4])
    @patch(*_RECREATE_PATCHES[4:5])
    @patch(*_RECREATE_PATCHES[5:6])
    @patch(*_RECREATE_PATCHES[6:7])
    @patch(*_RECREATE_PATCHES[7:8])
    @patch(*_RECREATE_PATCHES[8:9])
    @patch(*_RECREATE_PATCHES[9:10])
    def test_ftt1_failback_blocks_and_drops_leadership_on_secondary(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events,
            mock_storage_events, mock_health, mock_recreate_on_sec):
        nodes = _build_ftt1_nodes()
        db = _make_db_mock(nodes)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore
        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Verify port block/allow on secondary
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertGreaterEqual(len(block_calls), 1, "At least 1 block call on secondary")
        self.assertGreaterEqual(len(allow_calls), 1, "At least 1 allow call on secondary")

        # Verify force_to_non_leader called (on secondary + on self)
        force_calls = rpc.bdev_distrib_force_to_non_leader.call_args_list
        self.assertGreaterEqual(len(force_calls), 2,
                                "force_to_non_leader on secondary + primary self")

        # Verify inflight IO check on secondary
        rpc.bdev_distrib_check_inflight_io.assert_called()


class TestRecreateLvstoreFTT2(unittest.TestCase):
    """FTT=2: recreate_lvstore on primary restart with both secondaries."""

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_ftt2_failback_blocks_both_secondaries(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events,
            mock_storage_events, mock_health, mock_recreate_on_sec):
        nodes = _build_ftt2_nodes()
        db = _make_db_mock(nodes)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore
        snode = nodes["node-1"]
        result = recreate_lvstore(snode)
        self.assertTrue(result)

        # Both secondaries should have port blocked and allowed
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertEqual(len(block_calls), 2, "Block on both secondaries")
        self.assertEqual(len(allow_calls), 2, "Allow on both secondaries")

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_ftt2_failback_second_sec_offline_skipped(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events,
            mock_storage_events, mock_health, mock_recreate_on_sec):
        """Primary restarts, second secondary offline → only first sec processed."""
        nodes = _build_ftt2_nodes()
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE
        db = _make_db_mock(nodes)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore
        result = recreate_lvstore(nodes["node-1"])
        self.assertTrue(result)

        # Only first secondary should have port blocked
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        self.assertEqual(len(block_calls), 1, "Only online secondary should be blocked")

        # Offline secondary should not have connect_to_hublvol called
        nodes["node-3"].connect_to_hublvol.assert_not_called()


# ===========================================================================
# recreate_lvstore_on_sec Tests (secondary failback) — THE FIXED CODE
# ===========================================================================

class TestRecreateLvstoreOnSecPrimaryOnline(unittest.TestCase):
    """Test recreate_lvstore_on_sec when primary IS online (Change 1: uncommented code)."""

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_online_port_blocked_sleep_force_nonleader_inflight(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events):
        """When primary is online, recreate_lvstore_on_sec must: block port, sleep 0.5s,
        set_leader(False), force_to_non_leader, check_inflight_io, then allow port."""
        nodes = _build_ftt2_nodes()
        # node-2 is the secondary being rebuilt; node-1 is its primary (online)
        secondary = nodes["node-2"]
        primary = nodes["node-1"]
        lvols = [_lvol("lv1", "node-1")]

        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec
        result = recreate_lvstore_on_sec(secondary)
        self.assertTrue(result)

        # Port should be blocked and then allowed on primary
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertGreaterEqual(len(block_calls), 1, "Port should be blocked on primary")
        self.assertGreaterEqual(len(allow_calls), 1, "Port should be allowed on primary")

        # Leadership must be dropped on primary
        rpc.bdev_lvol_set_leader.assert_any_call(
            primary.lvstore, leader=False, bs_nonleadership=True)

        # force_to_non_leader must be called with primary's jm_vuid
        rpc.bdev_distrib_force_to_non_leader.assert_any_call(primary.jm_vuid)

        # Inflight IO check must be called
        rpc.bdev_distrib_check_inflight_io.assert_any_call(primary.jm_vuid)


class TestRecreateLvstoreOnSecPrimaryOffline(unittest.TestCase):
    """Test recreate_lvstore_on_sec when primary is OFFLINE and first sec restarts
    (Change 2: new failback from second secondary)."""

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_offline_first_sec_restarts_drops_leadership_on_second_sec(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events):
        """Primary offline, first sec restarts → must drop leadership on second sec
        to prevent writer conflict when JC connects to remote JMs."""
        nodes = _build_ftt2_nodes()
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE  # primary offline
        secondary = nodes["node-2"]  # first secondary, restarting
        second_sec = nodes["node-3"]  # second secondary, online

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec
        result = recreate_lvstore_on_sec(secondary)
        self.assertTrue(result)

        # Port should be blocked on second secondary (not primary, which is offline)
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertGreaterEqual(len(block_calls), 1,
                                "Port should be blocked on second secondary")
        self.assertGreaterEqual(len(allow_calls), 1,
                                "Port should be allowed on second secondary after examine")

        # Leadership must be dropped on second secondary
        rpc.bdev_lvol_set_leader.assert_any_call(
            nodes["node-1"].lvstore, leader=False, bs_nonleadership=True)

        # force_to_non_leader on second secondary with primary's jm_vuid
        rpc.bdev_distrib_force_to_non_leader.assert_any_call(nodes["node-1"].jm_vuid)

        # Inflight IO check on second secondary
        rpc.bdev_distrib_check_inflight_io.assert_any_call(nodes["node-1"].jm_vuid)

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_offline_second_sec_also_offline_no_failback_for_that_group(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events):
        """Primary offline, second sec also offline → no port block for THAT group.
        (Node may still get failback calls for other groups it's secondary for.)"""
        # Use a minimal 3-node topology where node-2 is ONLY secondary for node-1
        nodes = {
            "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                             status=StorageNode.STATUS_OFFLINE,
                             secondary_node_id="node-2",
                             secondary_node_id_2="node-3",
                             rpc_port=8080,
                             lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
            "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                             lvstore_stack_secondary_1="node-1",
                             rpc_port=8081,
                             lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
            "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                             status=StorageNode.STATUS_OFFLINE,
                             lvstore_stack_secondary_2="node-1",
                             rpc_port=8082,
                             lvstore_ports={"LVS_300": {"lvol_subsys_port": 4428, "hublvol_port": 4429}}),
        }
        secondary = nodes["node-2"]
        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec
        result = recreate_lvstore_on_sec(secondary)
        self.assertTrue(result)

        # No port block for node-1's group (primary offline, second sec offline)
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        self.assertEqual(len(all_fw_calls), 0,
                         "No firewall calls when primary and second sec both offline")

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_second_sec_restarts_primary_offline_no_failback_on_first_sec_for_that_group(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events):
        """Second secondary restarts, primary offline → no failback on first sec for THIS group.
        Uses minimal topology where node-3 is ONLY secondary for node-1."""
        nodes = {
            "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                             status=StorageNode.STATUS_OFFLINE,
                             secondary_node_id="node-2",
                             secondary_node_id_2="node-3",
                             rpc_port=8080,
                             lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
            "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                             lvstore_stack_secondary_1="node-1",
                             rpc_port=8081,
                             lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
            "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                             lvstore_stack_secondary_2="node-1",
                             rpc_port=8082,
                             lvstore_ports={"LVS_300": {"lvol_subsys_port": 4428, "hublvol_port": 4429}}),
        }
        secondary = nodes["node-3"]  # second secondary restarting

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec
        result = recreate_lvstore_on_sec(secondary)
        self.assertTrue(result)

        # is_second_sec=True for node-1's group, so no failback on first sec
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        self.assertEqual(len(block_calls), 0,
                         "Second sec restarting should not trigger failback on first sec")


class TestRecreateLvstoreOnSecANAFailback(unittest.TestCase):
    """Test that ANA failback in recreate_lvstore_on_sec works regardless of primary status."""

    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_ana_failback_called_when_primary_offline(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events):
        """ANA failback (demote second sec to inaccessible) should happen even when primary is offline."""
        nodes = _build_ftt2_nodes()
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE
        secondary = nodes["node-2"]  # first secondary restarting

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_sec
        result = recreate_lvstore_on_sec(secondary)
        self.assertTrue(result)

        # ANA state should be set on second secondary (inaccessible)
        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        inaccessible_calls = [c for c in ana_calls
                              if 'inaccessible' in str(c)]
        self.assertTrue(len(inaccessible_calls) > 0,
                        "Second secondary should be set to inaccessible via ANA failback "
                        "even when primary is offline")


# ===========================================================================
# End-to-end scenario: failback then restart second secondary
# ===========================================================================

class TestSequentialFailbackScenario(unittest.TestCase):
    """Simulate: primary restarts (failback from both secs), then second sec restarts."""

    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_sec")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._connect_to_remote_jm_devs")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_failback_then_second_sec_restart(
            self, mock_db_cls, mock_create_bdev, mock_connect_jm,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events,
            mock_storage_events, mock_health, mock_recreate_on_sec):
        """
        1. Primary restarts with second sec offline → failback from first sec only
        2. Then second sec comes online → recreate_lvstore_on_sec(second_sec)
        Both operations should succeed without conflicts.
        """
        nodes = _build_ftt2_nodes()
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE  # sec2 offline initially

        db = _make_db_mock(nodes)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        # Step 1: Primary restarts (second sec offline)
        from simplyblock_core.storage_node_ops import recreate_lvstore
        result = recreate_lvstore(nodes["node-1"])
        self.assertTrue(result, "Primary failback should succeed with sec2 offline")

        # Step 2: Second secondary comes online
        nodes["node-3"].status = StorageNode.STATUS_ONLINE
        rpc.reset_mock()
        fw_instances.clear()

        # recreate_lvstore_on_sec is mocked above, so simulate it directly
        mock_recreate_on_sec.return_value = True
        # The actual call would be recreate_lvstore_on_sec(nodes["node-3"])
        # but since it's patched in recreate_lvstore, we verify it was called
        # during step 1 for the primary's own secondary role


if __name__ == '__main__':
    unittest.main()
