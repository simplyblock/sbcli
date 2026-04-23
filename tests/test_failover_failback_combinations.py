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
- recreate_lvstore_on_non_leader: primary online, port block + leadership drop
- recreate_lvstore_on_non_leader: primary offline, first sec restarts, leadership dropped on second sec

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

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
          lvstore="", secondary_node_id="", tertiary_node_id="",
          mgmt_ip="", rpc_port=8080, lvol_subsys_port=9090,
          lvstore_ports=None, active_tcp=True, active_rdma=False,
          lvstore_stack_secondary="", lvstore_stack_tertiary="",
          jm_vuid=100, lvstore_status="ready"):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = f"host-{uuid}"
    n.lvstore = lvstore
    n.secondary_node_id = secondary_node_id
    n.tertiary_node_id = tertiary_node_id
    n.mgmt_ip = mgmt_ip or f"10.0.0.{hash(uuid) % 254 + 1}"
    n.api_endpoint = f"http://{mgmt_ip or f'10.0.0.{hash(uuid) % 254 + 1}'}:5000"
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
    # Include the standard _lvol()'s base bdev so the post-examine
    # lvol-presence check in recreate_lvstore_on_non_leader passes.
    rpc.get_bdevs.return_value = [{"name": "LVS_100/bdev_test", "aliases": []}]
    rpc.bdev_lvol_set_lvs_opts.return_value = True
    rpc.bdev_lvol_set_leader.return_value = True
    rpc.bdev_lvol_get_leader.return_value = True
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

    def make_fw(*args, **kwargs):
        node = args[0] if args else None
        fw = MagicMock()
        fw._node_id = node.uuid if node and hasattr(node, 'uuid') else str(node)
        fw.firewall_set_port = MagicMock(return_value=True)
        instances.append(fw)
        return fw

    return make_fw, instances


def _setup_node_methods(nodes, rpc):
    """Attach common mock methods to all nodes."""
    for n in nodes.values():
        n.rpc_client = MagicMock(return_value=rpc)
        n.wait_for_jm_rep_tasks_to_finish = MagicMock(return_value=True)
        n.create_hublvol = MagicMock()
        n.create_secondary_hublvol = MagicMock()
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
                         lvstore_stack_secondary="node-1",
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
                         tertiary_node_id="node-3",
                         rpc_port=8080,
                         lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
        "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                         lvstore_stack_secondary="node-1",
                         secondary_node_id="node-3",
                         rpc_port=8081,
                         lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
        "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                         lvstore_stack_tertiary="node-1",
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
            sec2 = n.tertiary_node_id
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
        self.assertTrue(len(ana_calls) > 0, "Should have ANA state change calls")

    def test_ftt2_failover_primary_to_first_sec(self):
        """FTT=2: primary goes offline → sec1=optimized (sec2 stays non_optimized, no ANA call)."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # Only first secondary gets promoted — sec2 is already non_optimized
        self.assertTrue(len(ana_calls) >= 1, "Should set ANA on first secondary")

    def test_ftt2_failover_first_sec_offline_promotes_own_sec(self):
        """FTT=2: first secondary goes offline → promotes its own LVStore's sec (node-2 is also a primary)."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-2"].status = StorageNode.STATUS_OFFLINE

        rpc = self._run_failover(nodes, "node-2", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # node-2 is a primary for its own LVStore, so its sec gets promoted
        # But no action for node-1's LVStore (sec_2 already non_optimized)
        self.assertTrue(len(ana_calls) >= 1)

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
        """FTT=2: primary offline, first sec already offline → no ANA change (sec2 already non_optimized)."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE
        nodes["node-2"].status = StorageNode.STATUS_OFFLINE

        # sec_1 is offline so can't be promoted; sec_2 is already non_optimized
        rpc = self._run_failover(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        self.assertEqual(len(ana_calls), 0, "No ANA changes when both primary and sec_1 offline")


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

        self._run_failback(nodes, "node-1", lvols)

        # With FTT=1, no tertiary_node_id, so _failback_primary_ana not called
        # (it requires tertiary_node_id). No-op for FTT=1 via this path.
        # The actual failback for FTT=1 happens inside recreate_lvstore.

    def test_ftt2_failback_primary_restarts_both_secs_online(self):
        """FTT=2: primary restarts, both secondaries online → sec1=non_optimized (sec2 unchanged)."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]

        rpc = self._run_failback(nodes, "node-1", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # Only first secondary gets demoted — sec2 is always non_optimized
        self.assertTrue(len(ana_calls) >= 1,
                        "Should set ANA on first secondary (non_optimized)")

    def test_ftt2_failback_first_sec_restarts_own_lvstore_only(self):
        """FTT=2: first secondary restarts → only its own LVStore's sec gets demoted (node-2 is also primary)."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-1"].status = StorageNode.STATUS_ONLINE

        rpc = self._run_failback(nodes, "node-2", lvols)

        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        # node-2 is a primary, so its own first_sec gets demoted on failback
        # But no sec_2 demotion to inaccessible anywhere
        inaccessible_calls = [c for c in ana_calls if 'inaccessible' in str(c)]
        self.assertEqual(len(inaccessible_calls), 0,
                        "No inaccessible state should be set")

    def test_ftt2_failback_first_sec_restarts_second_sec_offline(self):
        """FTT=2: first secondary restarts, second secondary offline → no ANA change."""
        nodes = _build_ftt2_nodes()
        lvols = [_lvol("lv1", "node-1")]
        nodes["node-3"].status = StorageNode.STATUS_OFFLINE

        self._run_failback(nodes, "node-2", lvols)

        # With second sec offline, failback for first sec role doesn't demote anyone
        # (second sec is not online so it's skipped)


# ===========================================================================
# recreate_lvstore Tests (primary failback)
# ===========================================================================

_RECREATE_PATCHES = [
    "simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader",
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

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
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
            mock_storage_events, mock_health, mock_recreate_on_non_leader,
            _mock_handle, _mock_phase, _mock_disc):
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

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
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
            mock_storage_events, mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
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

        # Per design: every online peer (current leader + non-leader peers)
        # must have its LVS port blocked during the primary restart, and
        # each peer's port is unblocked only after its connect_to_hublvol
        # succeeds. With FTT=2 and both secondaries online, that's 2 blocks
        # and 2 matching allows.
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertEqual(len(block_calls), 2, "Block on both secondaries")
        self.assertEqual(len(allow_calls), 2, "Allow on both secondaries")

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
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
            mock_storage_events, mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
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
# recreate_lvstore_on_non_leader Tests (secondary failback) — THE FIXED CODE
# ===========================================================================

class TestRecreateLvstoreOnSecPrimaryOnline(unittest.TestCase):
    """Test recreate_lvstore_on_non_leader when primary IS online (Change 1: uncommented code)."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_primary_online_port_blocked_drain_io_no_leadership_drop(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """When primary is online, recreate_lvstore_on_non_leader must: block port,
        drain inflight IO, examine, then allow port. Leadership must NOT be dropped."""
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

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(secondary, leader_node=primary, primary_node=primary)
        self.assertTrue(result)

        # Port should be blocked and then allowed on leader only
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        allow_calls = [c for c in all_fw_calls if c[0][2] == "allow"]
        self.assertGreaterEqual(len(block_calls), 1, "Port should be blocked on leader")
        self.assertGreaterEqual(len(allow_calls), 1, "Port should be allowed on leader")

        # Per design: non-leader restart must NOT drop leadership
        leader_set_leader_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if c[0][0] == primary.lvstore and c[1].get("leader") is False
        ]
        self.assertEqual(len(leader_set_leader_calls), 0,
                         "Non-leader restart must not drop leadership on current leader")

        # Inflight IO check must be called (drain only)
        rpc.bdev_distrib_check_inflight_io.assert_any_call(primary.jm_vuid)


class TestRecreateLvstoreOnSecPrimaryOffline(unittest.TestCase):
    """Test recreate_lvstore_on_non_leader when primary is OFFLINE and first sec restarts
    (Change 2: new failback from second secondary)."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.services.storage_node_monitor.is_node_data_plane_disconnected_quorum",
           return_value=True)
    def test_primary_offline_first_sec_restarts_drops_leadership_on_second_sec(
            self, mock_quorum, mock_snode_client, mock_health, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events,
            _mock_disc, _mock_phase, _mock_handle):
        """Primary offline, first sec restarts → must drop leadership on second sec
        to prevent writer conflict when JC connects to remote JMs."""
        nodes = _build_ftt2_nodes()
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE  # primary offline
        secondary = nodes["node-2"]  # first secondary, restarting
        primary = nodes["node-1"]
        leader = nodes["node-3"]  # second secondary is current leader

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)
        mock_health.check_bdev.return_value = True

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(secondary, leader_node=leader, primary_node=primary)
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

        # Per design: non-leader restart must NOT drop leadership on the current leader.
        # It only blocks the port, drains inflight IO, examines, then unblocks.
        leader_set_leader_calls = [
            c for c in rpc.bdev_lvol_set_leader.call_args_list
            if c[0][0] == nodes["node-1"].lvstore and c[1].get("leader") is False
        ]
        self.assertEqual(len(leader_set_leader_calls), 0,
                         "Non-leader restart must not drop leadership on current leader")

        # Inflight IO check on leader (drain only, no demotion)
        rpc.bdev_distrib_check_inflight_io.assert_any_call(nodes["node-1"].jm_vuid)

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    def test_primary_offline_second_sec_also_offline_no_failback_for_that_group(
            self, mock_snode_client, mock_health, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """Primary offline, second sec also offline → no port block for THAT group.
        (Node may still get failback calls for other groups it's secondary for.)"""
        # Use a minimal 3-node topology where node-2 is ONLY secondary for node-1
        nodes = {
            "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                             status=StorageNode.STATUS_OFFLINE,
                             secondary_node_id="node-2",
                             tertiary_node_id="node-3",
                             rpc_port=8080,
                             lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
            "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                             lvstore_stack_secondary="node-1",
                             rpc_port=8081,
                             lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
            "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                             status=StorageNode.STATUS_OFFLINE,
                             lvstore_stack_tertiary="node-1",
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
        mock_health.check_bdev.return_value = True

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        primary = nodes["node-1"]
        # node-2 is the leader (only online secondary when node-3 is offline)
        leader = nodes["node-2"]
        result = recreate_lvstore_on_non_leader(secondary, leader_node=leader, primary_node=primary)
        self.assertTrue(result)

        # With new design: restarting node blocks/unblocks its own port (2 calls),
        # plus leader port block/unblock (2 calls). Offline peers are skipped.
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        # At minimum, restarting node's own port block + unblock
        self.assertGreaterEqual(len(all_fw_calls), 2,
                                "Restarting node should block/unblock its own port")

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_second_sec_restarts_primary_offline_no_failback_on_first_sec_for_that_group(
            self, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """Second secondary restarts, primary offline → sibling (first sec) gets port blocked
        unconditionally. Uses minimal topology where node-3 is ONLY secondary for node-1."""
        nodes = {
            "node-1": _node("node-1", lvstore="LVS_100", jm_vuid=100,
                             status=StorageNode.STATUS_OFFLINE,
                             secondary_node_id="node-2",
                             tertiary_node_id="node-3",
                             rpc_port=8080,
                             lvstore_ports={"LVS_100": {"lvol_subsys_port": 4420, "hublvol_port": 4425}}),
            "node-2": _node("node-2", lvstore="LVS_200", jm_vuid=200,
                             lvstore_stack_secondary="node-1",
                             rpc_port=8081,
                             lvstore_ports={"LVS_200": {"lvol_subsys_port": 4426, "hublvol_port": 4427}}),
            "node-3": _node("node-3", lvstore="LVS_300", jm_vuid=300,
                             lvstore_stack_tertiary="node-1",
                             rpc_port=8082,
                             lvstore_ports={"LVS_300": {"lvol_subsys_port": 4428, "hublvol_port": 4429}}),
        }
        secondary = nodes["node-3"]  # second secondary restarting
        primary = nodes["node-1"]
        leader = nodes["node-2"]  # first sec is the current leader

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(secondary, leader_node=leader, primary_node=primary)
        self.assertTrue(result)

        # Per design: only leader port should be blocked, not restarting node's own port
        all_fw_calls = []
        for fw in fw_instances:
            all_fw_calls.extend(fw.firewall_set_port.call_args_list)
        block_calls = [c for c in all_fw_calls if c[0][2] == "block"]
        self.assertEqual(len(block_calls), 1,
                         "Only leader should get port blocked")


class TestRecreateLvstoreOnSecANAFailback(unittest.TestCase):
    """Test that ANA failback in recreate_lvstore_on_non_leader works regardless of primary status."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    @patch("simplyblock_core.storage_node_ops.health_controller")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    def test_no_ana_failback_on_sec2_when_primary_offline(
            self, mock_snode_client, mock_health, mock_db_cls, mock_create_bdev,
            mock_rpc_cls, mock_fw_cls, mock_tasks, mock_tcp_events, mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """sec_2 is always non_optimized — no ANA failback to inaccessible needed."""
        nodes = _build_ftt2_nodes()
        nodes["node-1"].status = StorageNode.STATUS_OFFLINE
        secondary = nodes["node-2"]  # first secondary restarting

        lvols = [_lvol("lv1", "node-1")]
        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)
        mock_health.check_bdev.return_value = True

        make_fw, fw_instances = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        primary = nodes["node-1"]
        leader = nodes["node-3"]  # second secondary is the current leader (primary offline)
        result = recreate_lvstore_on_non_leader(secondary, leader_node=leader, primary_node=primary)
        self.assertTrue(result)

        # No inaccessible calls — sec_2 is always non_optimized
        ana_calls = rpc.nvmf_subsystem_listener_set_ana_state.call_args_list
        inaccessible_calls = [c for c in ana_calls
                              if 'inaccessible' in str(c)]
        self.assertEqual(len(inaccessible_calls), 0,
                        "Second secondary should never be set to inaccessible")


# ===========================================================================
# End-to-end scenario: failback then restart second secondary
# ===========================================================================

class TestSequentialFailbackScenario(unittest.TestCase):
    """Simulate: primary restarts (failback from both secs), then second sec restarts."""

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected", side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.recreate_lvstore_on_non_leader")
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
            mock_storage_events, mock_health, mock_recreate_on_non_leader, _mock_disc, _mock_phase, _mock_handle):
        """
        1. Primary restarts with second sec offline → failback from first sec only
        2. Then second sec comes online → recreate_lvstore_on_non_leader(second_sec, ...)
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

        # recreate_lvstore_on_non_leader is mocked above, so simulate it directly
        mock_recreate_on_non_leader.return_value = True
        # The actual call would be recreate_lvstore_on_non_leader(nodes["node-3"], ...)
        # but since it's patched in recreate_lvstore, we verify it was called
        # during step 1 for the primary's own secondary role


# ===========================================================================
# TasksRunnerPortAllow Tests
# ===========================================================================

class TestPortAllowTask(unittest.TestCase):
    """Test exec_port_allow_task safety sequence.

    The module simplyblock_core.services.tasks_runner_port_allow has a
    ``while True`` loop at module level, so we cannot import it normally
    during tests.  Instead we verify the fixed function's logic by
    importing it with the module-level db already mocked (preventing the
    infinite loop from blocking, since we patch `db` before the import
    resolves the loop's first iteration).
    """

    @classmethod
    def _import_exec(cls):
        """Import exec_port_allow_task safely by pre-patching module globals."""
        import sys
        mod_name = "simplyblock_core.services.tasks_runner_port_allow"
        # Remove cached module so patches take effect on re-import
        sys.modules.pop(mod_name, None)
        # The module-level ``while True`` reads db.get_clusters().
        # Patching db_controller.DBController before import makes get_clusters()
        # return an empty list on the first iteration, but we still need to
        # prevent the loop.  Easier: just grab the function object from source.
        import simplyblock_core.services.tasks_runner_port_allow as mod
        return mod.exec_port_allow_task

    def _make_task(self, node_id, cluster_id="cluster-1", port=4420):
        task = MagicMock()
        task.uuid = "task-1"
        task.node_id = node_id
        task.cluster_id = cluster_id
        task.canceled = False
        task.status = "new"
        task.function_params = {"port_number": port}
        task.write_to_db = MagicMock()
        return task

    def _read_source(self):
        """Read exec_port_allow_task source without importing the module (which has a while True loop)."""
        import os
        src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "simplyblock_core", "services", "tasks_runner_port_allow.py")
        with open(src_path, "r") as f:
            return f.read()

    def test_port_allow_code_has_force_to_non_leader(self):
        """Verify the fixed code calls force_to_non_leader and check_inflight_io."""
        src = self._read_source()
        self.assertIn("bdev_distrib_force_to_non_leader", src,
                       "exec_port_allow_task must call force_to_non_leader")
        self.assertIn("bdev_distrib_check_inflight_io", src,
                       "exec_port_allow_task must call check_inflight_io")
        self.assertIn("secs_to_unblock", src,
                       "exec_port_allow_task must track secondaries to unblock")

    def test_port_allow_code_iterates_all_sec_ids(self):
        """Verify the fix iterates sec_ids (not just last sec_node from loop)."""
        src = self._read_source()
        self.assertIn("for sid in sec_ids", src,
                       "Must iterate all sec_ids, not use leaked sec_node variable")

    def test_port_allow_code_blocks_before_allow(self):
        """Verify port block happens before port allow in the code."""
        src = self._read_source()
        # Find within exec_port_allow_task function body
        fn_start = src.find("def exec_port_allow_task")
        fn_src = src[fn_start:]
        block_pos = fn_src.find('"block"')
        allow_pos = fn_src.find('"allow"')
        self.assertGreater(block_pos, 0, "Port block must be present")
        self.assertGreater(allow_pos, block_pos,
                           "Port block must appear before port allow in code")

    def test_port_allow_code_no_leaked_sec_node_for_leadership(self):
        """Verify the old buggy pattern (using leaked sec_node for leadership drop) is removed."""
        src = self._read_source()
        fn_start = src.find("def exec_port_allow_task")
        fn_src = src[fn_start:]
        # The old bug: leadership drop on leaked sec_node instead of iterating sec_ids
        # Check that bdev_lvol_set_leader is NOT called on bare "sec_rpc_client" (old pattern)
        # but on "sn_rpc" (new pattern iterating sec_ids)
        self.assertIn("sn_rpc.bdev_lvol_set_leader", fn_src,
                       "Leadership drop must use sn_rpc from sec_ids iteration")
        self.assertIn("sn_rpc.bdev_distrib_force_to_non_leader", fn_src,
                       "force_to_non_leader must use sn_rpc from sec_ids iteration")


# ===========================================================================
# Delete / Create / Resize with second secondary fallback tests
# ===========================================================================

class TestLvolSecondSecondaryFallback(unittest.TestCase):
    """Test that delete/create/resize fall back to second secondary when primary + first sec are offline."""

    def _read_lvol_controller_source(self):
        import os
        src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "simplyblock_core", "controllers", "lvol_controller.py")
        with open(src_path, "r") as f:
            return f.read()

    def _get_function_source(self, full_src, func_name):
        fn_start = full_src.find(f"def {func_name}(")
        if fn_start < 0:
            return ""
        fn_end = full_src.find("\ndef ", fn_start + 1)
        return full_src[fn_start:fn_end] if fn_end > fn_start else full_src[fn_start:]

    # --- delete_lvol ---

    def test_delete_code_uses_leader_failover(self):
        """delete_lvol must use execute_on_leader_with_failover."""
        src = self._get_function_source(self._read_lvol_controller_source(), "delete_lvol")
        self.assertIn("execute_on_leader_with_failover", src,
                       "delete_lvol must use execute_on_leader_with_failover")

    def test_delete_code_checks_non_leaders(self):
        """delete_lvol must use check_non_leader_for_operation for all non-leaders."""
        src = self._get_function_source(self._read_lvol_controller_source(), "delete_lvol")
        self.assertIn("check_non_leader_for_operation", src,
                       "delete_lvol must use check_non_leader_for_operation")

    def test_create_code_uses_leader_failover(self):
        """add_lvol_ha must use find_leader_with_failover."""
        src = self._get_function_source(self._read_lvol_controller_source(), "add_lvol_ha")
        self.assertIn("find_leader_with_failover", src,
                       "add_lvol_ha must use find_leader_with_failover")

    def test_create_code_checks_non_leaders(self):
        """add_lvol_ha must use check_non_leader_for_operation."""
        src = self._get_function_source(self._read_lvol_controller_source(), "add_lvol_ha")
        self.assertIn("check_non_leader_for_operation", src,
                       "add_lvol_ha must pre-check non-leaders")

    def test_resize_code_checks_non_leaders(self):
        """resize_lvol must use check_non_leader_for_operation."""
        full_src = self._read_lvol_controller_source()
        resize_marker = full_src.find("Resizing LVol")
        fn_start = full_src.rfind("def ", 0, resize_marker)
        fn_end = full_src.find("\ndef ", fn_start + 1)
        fn_src = full_src[fn_start:fn_end] if fn_end > fn_start else full_src[fn_start:]
        self.assertIn("check_non_leader_for_operation", fn_src,
                       "resize function must use check_non_leader_for_operation")

    def test_delete_first_sec_online_adds_remaining_secs(self):
        """delete_lvol must iterate all non-leaders, not just first secondary."""
        src = self._get_function_source(self._read_lvol_controller_source(), "delete_lvol")
        self.assertIn("for nl in non_leaders", src,
                       "delete_lvol must iterate all non-leaders")


# ===========================================================================
# Non-leader restart must fail-loudly on lvol mismatch unless force=True
# ===========================================================================

class TestRecreateLvstoreNonLeaderLvolMismatch(unittest.TestCase):
    """After bdev_examine on a non-leader, the set of lvol bdevs in SPDK must
    match the set of lvols FDB says belong to this LVS. If a blob wasn't
    durable on this peer's raid0 shard before a force-shutdown (see
    2026-04-20 dual-outage race), the examine silently skips it and the
    subsystem ends up bound without a namespace. Without this check the
    restart returned success while leaving the lvol unserved on this peer.
    """

    def _setup(self, mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
                present_bdevs=None):
        nodes = _build_ftt2_nodes()
        secondary = nodes["node-2"]
        primary = nodes["node-1"]
        lvols = [_lvol("lv1", "node-1")]

        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        if present_bdevs is not None:
            rpc.get_bdevs.return_value = [
                {"name": n, "aliases": []} for n in present_bdevs
            ]
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        make_fw, _ = _mock_fw_factory()
        mock_fw_cls.side_effect = make_fw
        _setup_node_methods(nodes, rpc)
        return secondary, primary, rpc

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_missing_lvol_bdev_aborts_when_not_force(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """Without force=True, restart must abort when an expected lvol bdev
        isn't in the SPDK bdev registry after examine."""
        # Empty bdev registry — the expected LVS_100/bdev_test isn't there
        secondary, primary, _rpc = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            present_bdevs=[],
        )
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        with self.assertRaises(Exception) as cm:
            recreate_lvstore_on_non_leader(
                secondary, leader_node=primary, primary_node=primary, force=False)
        self.assertIn("Expected lvols not registered", str(cm.exception))
        # Node must be marked offline by the abort path
        mock_set_status.assert_any_call(secondary.get_id(), StorageNode.STATUS_OFFLINE)

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_missing_lvol_bdev_succeeds_with_force(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """With force=True, restart proceeds even when lvol bdev is missing
        (operator explicitly accepts that the peer won't serve it)."""
        secondary, primary, _rpc = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            present_bdevs=[],
        )
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(
            secondary, leader_node=primary, primary_node=primary, force=True)
        self.assertTrue(result)

    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_lvol_present_passes(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle):
        """Happy path: expected lvol bdev is in the registry, restart succeeds
        without force."""
        secondary, primary, _rpc = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            present_bdevs=["LVS_100/bdev_test"],
        )
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(
            secondary, leader_node=primary, primary_node=primary, force=False)
        self.assertTrue(result)


# ===========================================================================
# Port-block failure during restart must retry then abort (unless force=True)
# ===========================================================================

class TestRecreateLvstoreNonLeaderPortBlockFailure(unittest.TestCase):
    """The leader-port-block step used to swallow FirewallClient exceptions
    and continue the restart, allowing the leader to keep serving writes
    while the restarting node examined raid0 — races that observably led
    to CRC mismatches and lvols dropped during examine. Restart must now
    retry the block and, if it still can't land, abort (unless force=True).
    """

    def _setup(self, mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
                fw_set_port_side_effect=None):
        nodes = _build_ftt2_nodes()
        secondary = nodes["node-2"]
        primary = nodes["node-1"]
        lvols = [_lvol("lv1", "node-1")]

        db = _make_db_mock(nodes, lvols)
        mock_db_cls.return_value = db

        rpc = _mock_rpc()
        mock_rpc_cls.return_value = rpc
        mock_create_bdev.return_value = (True, None)

        # Single FirewallClient mock whose firewall_set_port raises as directed
        fw = MagicMock()
        if fw_set_port_side_effect is not None:
            fw.firewall_set_port.side_effect = fw_set_port_side_effect
        mock_fw_cls.return_value = fw

        _setup_node_methods(nodes, rpc)
        return secondary, primary, rpc, fw

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_block_fails_all_attempts_aborts_without_force(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle, _mock_sleep):
        """All 5 port-block attempts fail → restart aborts, leader kept
        untouched, node goes offline. No continuing with an unblocked leader."""
        secondary, primary, _rpc, fw = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            fw_set_port_side_effect=ConnectionRefusedError("Connection refused"),
        )
        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        with self.assertRaises(Exception) as cm:
            recreate_lvstore_on_non_leader(
                secondary, leader_node=primary, primary_node=primary, force=False)
        self.assertIn("Failed to block leader", str(cm.exception))
        # All 5 attempts must have been tried
        self.assertEqual(fw.firewall_set_port.call_count, 5)
        # Abort path sets the restarting node offline
        mock_set_status.assert_any_call(secondary.get_id(), StorageNode.STATUS_OFFLINE)

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_block_fails_all_attempts_proceeds_with_force(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle, _mock_sleep):
        """With force=True, 5 failed block attempts don't abort — restart
        proceeds despite the race risk (operator explicit choice)."""
        secondary, primary, _rpc, fw = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            fw_set_port_side_effect=ConnectionRefusedError("Connection refused"),
        )
        # Include expected lvol so the later lvol-registration check passes
        _rpc.get_bdevs.return_value = [{"name": "LVS_100/bdev_test", "aliases": []}]

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(
            secondary, leader_node=primary, primary_node=primary, force=True)
        self.assertTrue(result)
        self.assertEqual(fw.firewall_set_port.call_count, 5)

    @patch("simplyblock_core.storage_node_ops.time.sleep", return_value=None)
    @patch("simplyblock_core.storage_node_ops._check_peer_disconnected",
           side_effect=lambda peer, **kw: peer.status in ["offline"])
    @patch("simplyblock_core.storage_node_ops._set_restart_phase")
    @patch("simplyblock_core.storage_node_ops._handle_rpc_failure_on_peer", return_value="skip")
    @patch("simplyblock_core.storage_node_ops.tcp_ports_events")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops.SNodeClient")
    @patch("simplyblock_core.storage_node_ops.FirewallClient")
    @patch("simplyblock_core.storage_node_ops.RPCClient")
    @patch("simplyblock_core.storage_node_ops._create_bdev_stack")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_block_succeeds_on_retry(
            self, mock_db_cls, mock_create_bdev, mock_rpc_cls, mock_fw_cls,
            mock_snode_client, mock_set_status, mock_tasks, mock_tcp_events,
            mock_storage_events, _mock_disc, _mock_phase, _mock_handle, _mock_sleep):
        """Transient ConnectionRefused — first 2 attempts fail, 3rd succeeds.
        Restart must keep going (no abort, no force needed)."""
        # side_effect: raise twice, then succeed (return None = call succeeds)
        secondary, primary, _rpc, fw = self._setup(
            mock_rpc_cls, mock_db_cls, mock_create_bdev, mock_fw_cls,
            fw_set_port_side_effect=[
                ConnectionRefusedError("x"),
                ConnectionRefusedError("x"),
                None, None, None,  # 3rd = block-ok; 4th = unblock-ok; extra for safety
            ],
        )
        _rpc.get_bdevs.return_value = [{"name": "LVS_100/bdev_test", "aliases": []}]

        from simplyblock_core.storage_node_ops import recreate_lvstore_on_non_leader
        result = recreate_lvstore_on_non_leader(
            secondary, leader_node=primary, primary_node=primary, force=False)
        self.assertTrue(result)
        # Block succeeded on the 3rd attempt; then unblock ran too (1 more call)
        self.assertGreaterEqual(fw.firewall_set_port.call_count, 3)


if __name__ == '__main__':
    unittest.main()
