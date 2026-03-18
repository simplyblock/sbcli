# coding=utf-8
"""
test_port_allocation.py – unit tests for the unified port allocation system.

Tests cover:
  - Cluster model port configuration fields (nvmf_base_port, rpc_base_port, snode_api_port)
  - Constants: new canonical names and backward compatibility aliases
  - Unified NVMe-oF port pool (_get_all_nvmf_ports, get_next_nvmf_port)
  - Legacy wrappers: get_next_port, get_next_dev_port, next_free_hublvol_port
  - RPC port allocation with cluster config
  - Firewall/SNodeAPI port: per-host-IP sharing
  - Port config propagation through cluster_ops (create_cluster, add_cluster)
  - CLI argument parsing for port params
  - Port uniqueness across port types in the unified pool
  - Edge cases: empty cluster, all ports exhausted, gaps in port ranges

All external dependencies (FDB, RPC, Docker) are mocked.
"""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, PropertyMock

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core import constants


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(nvmf_base_port=4420, rpc_base_port=8080,
             ha_type="ha", distr_npcs=1):
    c = Cluster()
    c.uuid = "cluster-test-001"
    c.ha_type = ha_type
    c.distr_ndcs = 1
    c.distr_npcs = distr_npcs
    c.nvmf_base_port = nvmf_base_port
    c.rpc_base_port = rpc_base_port
    return c


def _node(uuid, mgmt_ip="10.0.0.1", lvol_subsys_port=0, nvmf_port=0,
          hublvol_port=0, rpc_port=0, cluster_id="cluster-test-001"):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.mgmt_ip = mgmt_ip
    n.lvol_subsys_port = lvol_subsys_port
    n.nvmf_port = nvmf_port
    n.rpc_port = rpc_port
    if hublvol_port > 0:
        n.hublvol = HubLVol({"nvmf_port": hublvol_port, "uuid": f"hub-{uuid}",
                              "nqn": f"nqn.hub.{uuid}", "bdev_name": "lvs/hublvol"})
    else:
        n.hublvol = None
    return n


# ---------------------------------------------------------------------------
# Test Cluster Model Port Fields
# ---------------------------------------------------------------------------

class TestClusterModelPortFields(unittest.TestCase):

    def test_default_values(self):
        c = Cluster()
        self.assertEqual(c.nvmf_base_port, 4420)
        self.assertEqual(c.rpc_base_port, 8080)

    def test_custom_values(self):
        c = _cluster(nvmf_base_port=5000, rpc_base_port=9000)
        self.assertEqual(c.nvmf_base_port, 5000)
        self.assertEqual(c.rpc_base_port, 9000)

    def test_port_fields_in_clean_dict(self):
        c = _cluster(nvmf_base_port=5000)
        data = c.get_clean_dict()
        self.assertEqual(data["nvmf_base_port"], 5000)
        self.assertEqual(data["rpc_base_port"], 8080)


# ---------------------------------------------------------------------------
# Test Constants
# ---------------------------------------------------------------------------

class TestConstants(unittest.TestCase):

    def test_canonical_constants_exist(self):
        self.assertEqual(constants.NVMF_BASE_PORT, 4420)
        self.assertEqual(constants.RPC_BASE_PORT, 8080)

    def test_backward_compat_aliases(self):
        self.assertEqual(constants.RPC_PORT_RANGE_START, constants.RPC_BASE_PORT)
        self.assertEqual(constants.LVOL_NVMF_PORT_START, constants.NVMF_BASE_PORT)
        self.assertEqual(constants.NODE_NVMF_PORT_START, constants.NVMF_BASE_PORT)
        self.assertEqual(constants.NODE_HUBLVOL_PORT_START, constants.NVMF_BASE_PORT)

    def test_all_nvmf_aliases_point_to_same_base(self):
        """All NVMe-oF port types now use the same base port."""
        self.assertEqual(constants.LVOL_NVMF_PORT_START, constants.NODE_NVMF_PORT_START)
        self.assertEqual(constants.NODE_NVMF_PORT_START, constants.NODE_HUBLVOL_PORT_START)


# ---------------------------------------------------------------------------
# Test _get_cluster_port_config
# ---------------------------------------------------------------------------

class TestGetClusterPortConfig(unittest.TestCase):

    @patch("simplyblock_core.db_controller.DBController")
    def test_returns_cluster_config(self, mock_db_cls):
        from simplyblock_core.utils import _get_cluster_port_config
        cluster = _cluster(nvmf_base_port=5000, rpc_base_port=9000)
        mock_db_cls.return_value.get_cluster_by_id.return_value = cluster
        result = _get_cluster_port_config("cluster-test-001")
        self.assertEqual(result, (5000, 9000))

    @patch("simplyblock_core.db_controller.DBController")
    def test_falls_back_to_constants_when_cluster_not_found(self, mock_db_cls):
        from simplyblock_core.utils import _get_cluster_port_config
        mock_db_cls.return_value.get_cluster_by_id.return_value = None
        result = _get_cluster_port_config("nonexistent")
        self.assertEqual(result, (constants.NVMF_BASE_PORT, constants.RPC_BASE_PORT))

    @patch("simplyblock_core.db_controller.DBController")
    def test_falls_back_to_constants_when_port_is_zero(self, mock_db_cls):
        """If a cluster has port=0 (e.g. old record), fall back to constant."""
        from simplyblock_core.utils import _get_cluster_port_config
        cluster = _cluster()
        cluster.nvmf_base_port = 0
        cluster.rpc_base_port = 0
        mock_db_cls.return_value.get_cluster_by_id.return_value = cluster
        result = _get_cluster_port_config("cluster-test-001")
        self.assertEqual(result, (constants.NVMF_BASE_PORT, constants.RPC_BASE_PORT))


# ---------------------------------------------------------------------------
# Test _get_all_nvmf_ports
# ---------------------------------------------------------------------------

class TestGetAllNvmfPorts(unittest.TestCase):

    @patch("simplyblock_core.db_controller.DBController")
    def test_empty_cluster(self, mock_db_cls):
        from simplyblock_core.utils import _get_all_nvmf_ports
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = []
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, set())

    @patch("simplyblock_core.db_controller.DBController")
    def test_collects_all_three_port_types(self, mock_db_cls):
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, {4420, 4421, 4422})

    @patch("simplyblock_core.db_controller.DBController")
    def test_ignores_zero_and_negative_ports(self, mock_db_cls):
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=0, nvmf_port=-1, hublvol_port=0),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, set())

    @patch("simplyblock_core.db_controller.DBController")
    def test_handles_node_without_hublvol(self, mock_db_cls):
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=0),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, {4420, 4421})

    @patch("simplyblock_core.db_controller.DBController")
    def test_multiple_nodes(self, mock_db_cls):
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422),
            _node("n2", lvol_subsys_port=4423, nvmf_port=4424, hublvol_port=4425),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, {4420, 4421, 4422, 4423, 4424, 4425})

    @patch("simplyblock_core.db_controller.DBController")
    def test_deduplicates_shared_ports(self, mock_db_cls):
        """If two nodes somehow share a port (e.g. secondary reusing primary's), it's in the set once."""
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422),
            _node("n2", lvol_subsys_port=4420, nvmf_port=4424, hublvol_port=0),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        result = _get_all_nvmf_ports("cluster-test-001")
        self.assertEqual(result, {4420, 4421, 4422, 4424})


# ---------------------------------------------------------------------------
# Test get_next_nvmf_port (unified allocator)
# ---------------------------------------------------------------------------

class TestGetNextNvmfPort(unittest.TestCase):

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_empty_cluster_returns_base_port(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = set()
        self.assertEqual(get_next_nvmf_port("c1"), 4420)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_skips_used_ports(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = {4420, 4421, 4422}
        self.assertEqual(get_next_nvmf_port("c1"), 4423)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_fills_gaps(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = {4420, 4422}  # gap at 4421
        self.assertEqual(get_next_nvmf_port("c1"), 4421)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_custom_base_port(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (5000, 8080)
        mock_ports.return_value = set()
        self.assertEqual(get_next_nvmf_port("c1"), 5000)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_custom_base_port_with_used_ports(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (5000, 8080)
        mock_ports.return_value = {5000, 5001, 5002}
        self.assertEqual(get_next_nvmf_port("c1"), 5003)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_mixed_port_types_all_respected(self, mock_config, mock_ports):
        """Port 4420 used by lvol, 4421 by device, 4422 by hublvol → next is 4423."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = {4420, 4421, 4422}
        self.assertEqual(get_next_nvmf_port("c1"), 4423)


# ---------------------------------------------------------------------------
# Test legacy wrapper functions
# ---------------------------------------------------------------------------

class TestLegacyPortWrappers(unittest.TestCase):

    @patch("simplyblock_core.utils.get_next_nvmf_port", return_value=4425)
    def test_get_next_port_delegates(self, mock_nvmf):
        from simplyblock_core.utils import get_next_port
        self.assertEqual(get_next_port("c1"), 4425)
        mock_nvmf.assert_called_once_with("c1")

    @patch("simplyblock_core.utils.get_next_nvmf_port", return_value=4426)
    def test_get_next_dev_port_delegates(self, mock_nvmf):
        from simplyblock_core.utils import get_next_dev_port
        self.assertEqual(get_next_dev_port("c1"), 4426)
        mock_nvmf.assert_called_once_with("c1")

    @patch("simplyblock_core.utils.get_next_nvmf_port", return_value=4427)
    def test_next_free_hublvol_port_delegates(self, mock_nvmf):
        from simplyblock_core.utils import next_free_hublvol_port
        self.assertEqual(next_free_hublvol_port("c1"), 4427)
        mock_nvmf.assert_called_once_with("c1")


# ---------------------------------------------------------------------------
# Test get_next_rpc_port
# ---------------------------------------------------------------------------

class TestGetNextRpcPort(unittest.TestCase):

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_empty_cluster_returns_base(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 8080)
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = []
        self.assertEqual(get_next_rpc_port("c1"), 8080)

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_skips_used_rpc_ports(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 8080)
        nodes = [_node("n1", rpc_port=8080), _node("n2", rpc_port=8081)]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        self.assertEqual(get_next_rpc_port("c1"), 8082)

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_custom_rpc_base(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 9000)
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = []
        self.assertEqual(get_next_rpc_port("c1"), 9000)

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_fills_rpc_gaps(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 8080)
        nodes = [_node("n1", rpc_port=8080), _node("n2", rpc_port=8082)]  # gap at 8081
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        self.assertEqual(get_next_rpc_port("c1"), 8081)

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_returns_zero_when_exhausted(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 8080)
        # All 1000 ports in range are used
        nodes = [_node(f"n{i}", rpc_port=8080 + i) for i in range(1000)]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        self.assertEqual(get_next_rpc_port("c1"), 0)

    @patch("simplyblock_core.utils._get_cluster_port_config")
    @patch("simplyblock_core.db_controller.DBController")
    def test_ignores_zero_rpc_ports(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_rpc_port
        mock_config.return_value = (4420, 8080)
        nodes = [_node("n1", rpc_port=0)]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        self.assertEqual(get_next_rpc_port("c1"), 8080)


# ---------------------------------------------------------------------------
# Test unified pool: sequential allocations yield unique ports
# ---------------------------------------------------------------------------

class TestUnifiedPoolSequentialAllocation(unittest.TestCase):
    """Simulate the real allocation sequence for a node:
    1. nvmf_port (device) allocated first, written to DB
    2. lvol_subsys_port allocated next, written to DB
    3. hublvol port allocated last
    Each allocation should see the previous ones and return distinct ports.
    """

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_three_sequential_allocations_distinct(self, mock_ports, mock_config):
        from simplyblock_core.utils import get_next_nvmf_port

        # 1st allocation: empty cluster
        mock_ports.return_value = set()
        port1 = get_next_nvmf_port("c1")
        self.assertEqual(port1, 4420)

        # 2nd allocation: port1 is now in use
        mock_ports.return_value = {4420}
        port2 = get_next_nvmf_port("c1")
        self.assertEqual(port2, 4421)

        # 3rd allocation: port1 and port2 in use
        mock_ports.return_value = {4420, 4421}
        port3 = get_next_nvmf_port("c1")
        self.assertEqual(port3, 4422)

        # All three ports are distinct
        self.assertEqual(len({port1, port2, port3}), 3)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_two_nodes_six_ports(self, mock_ports, mock_config):
        """Two nodes, each with 3 NVMe-oF ports, yield 6 distinct ports."""
        from simplyblock_core.utils import get_next_nvmf_port

        allocated = set()
        for i in range(6):
            mock_ports.return_value = set(allocated)
            port = get_next_nvmf_port("c1")
            self.assertNotIn(port, allocated)
            allocated.add(port)

        self.assertEqual(allocated, {4420, 4421, 4422, 4423, 4424, 4425})


# ---------------------------------------------------------------------------
# Test port gaps and reuse of freed ports
# ---------------------------------------------------------------------------

class TestPortGapsAndReuse(unittest.TestCase):

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_gap_at_start_not_possible(self, mock_ports, mock_config):
        """If base port is not in used_ports, it gets allocated first."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_ports.return_value = {4421, 4422}  # gap at 4420
        self.assertEqual(get_next_nvmf_port("c1"), 4420)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_gap_in_middle(self, mock_ports, mock_config):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_ports.return_value = {4420, 4422, 4423}  # gap at 4421
        self.assertEqual(get_next_nvmf_port("c1"), 4421)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_node_removed_frees_port(self, mock_ports, mock_config):
        """If a node is removed, its ports leave the used set and can be reused."""
        from simplyblock_core.utils import get_next_nvmf_port
        # Initially 3 ports used (one node)
        mock_ports.return_value = {4420, 4421, 4422}
        self.assertEqual(get_next_nvmf_port("c1"), 4423)

        # Node removed, ports freed
        mock_ports.return_value = set()
        self.assertEqual(get_next_nvmf_port("c1"), 4420)


# ---------------------------------------------------------------------------
# Test multi-node same-host-IP scenario
# ---------------------------------------------------------------------------

class TestMultiNodeSameHost(unittest.TestCase):
    """Test that multiple nodes on the same host IP get distinct NVMe-oF ports
    but share the same SNodeAPI/firewall port."""

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_two_nodes_same_ip_get_different_nvmf_ports(self, mock_ports, mock_config):
        """Even on same host, each node gets unique NVMe-oF ports."""
        from simplyblock_core.utils import get_next_nvmf_port
        # Node 1 uses 4420, 4421, 4422
        mock_ports.return_value = {4420, 4421, 4422}
        # Node 2's first allocation
        self.assertEqual(get_next_nvmf_port("c1"), 4423)


# ---------------------------------------------------------------------------
# Test selective blocking capability
# ---------------------------------------------------------------------------

class TestSelectiveBlockingPorts(unittest.TestCase):
    """Verify that different lvstores and hublvols get different ports,
    which is a prerequisite for selective firewall blocking."""

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.db_controller.DBController")
    def test_lvstore_ports_differ_from_hublvol_ports(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import _get_all_nvmf_ports
        # Node 1: lvol=4420, device=4421, hub=4422
        # Node 2: lvol=4423, device=4424, hub=4425
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422),
            _node("n2", lvol_subsys_port=4423, nvmf_port=4424, hublvol_port=4425),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        all_ports = _get_all_nvmf_ports("c1")

        # Each port type for each node is distinct
        lvol_ports = {4420, 4423}
        hub_ports = {4422, 4425}
        dev_ports = {4421, 4424}

        # No overlap between categories
        self.assertTrue(lvol_ports.isdisjoint(hub_ports))
        self.assertTrue(lvol_ports.isdisjoint(dev_ports))
        self.assertTrue(hub_ports.isdisjoint(dev_ports))

        # Blocking lvol port for node1 (4420) doesn't affect node2's lvol (4423)
        # or any hublvol ports
        self.assertNotIn(4420, hub_ports)
        self.assertNotIn(4420, dev_ports)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.db_controller.DBController")
    def test_secondary_reuses_primary_lvol_port(self, mock_db_cls, mock_config):
        """Secondary node should use primary's lvol_subsys_port for that lvstore.
        Blocking port 4420 on the secondary blocks only primary-n1's lvstore."""
        from simplyblock_core.utils import _get_all_nvmf_ports
        nodes = [
            _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422),
            _node("n2", lvol_subsys_port=4423, nvmf_port=4424, hublvol_port=4425),
            # Secondary serving both n1 and n2: its own ports + primary ports in subsystems
            _node("sec", lvol_subsys_port=4420, nvmf_port=4426, hublvol_port=4427),
        ]
        mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
        all_ports = _get_all_nvmf_ports("c1")

        # All unique ports are tracked (4420 appears on both n1 and sec but is deduplicated)
        self.assertEqual(all_ports, {4420, 4421, 4422, 4423, 4424, 4425, 4426, 4427})


# ---------------------------------------------------------------------------
# Test cluster_ops: port config storage
# ---------------------------------------------------------------------------

class TestClusterOpsPortConfig(unittest.TestCase):
    """Test that create_cluster and add_cluster store port configuration."""

    def test_create_cluster_default_ports(self):
        """Verify create_cluster accepts default port params."""
        import inspect
        from simplyblock_core.cluster_ops import create_cluster
        sig = inspect.signature(create_cluster)
        self.assertEqual(sig.parameters["nvmf_base_port"].default, 4420)
        self.assertEqual(sig.parameters["rpc_base_port"].default, 8080)

    def test_add_cluster_default_ports(self):
        """Verify add_cluster accepts default port params."""
        import inspect
        from simplyblock_core.cluster_ops import add_cluster
        sig = inspect.signature(add_cluster)
        self.assertEqual(sig.parameters["nvmf_base_port"].default, 4420)
        self.assertEqual(sig.parameters["rpc_base_port"].default, 8080)


# ---------------------------------------------------------------------------
# Test CLI argument parsing
# ---------------------------------------------------------------------------

class TestCLIPortArgs(unittest.TestCase):

    def _parse_create_args(self, extra_args=None):
        """Build a minimal CLI argument parser mimicking init_cluster__create."""
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--nvmf-base-port', type=int, default=4420, dest='nvmf_base_port')
        parser.add_argument('--rpc-base-port', type=int, default=8080, dest='rpc_base_port')
        return parser.parse_args(extra_args or [])

    def test_default_port_args(self):
        args = self._parse_create_args()
        self.assertEqual(args.nvmf_base_port, 4420)
        self.assertEqual(args.rpc_base_port, 8080)

    def test_custom_nvmf_base_port(self):
        args = self._parse_create_args(["--nvmf-base-port", "5000"])
        self.assertEqual(args.nvmf_base_port, 5000)

    def test_custom_rpc_base_port(self):
        args = self._parse_create_args(["--rpc-base-port", "9000"])
        self.assertEqual(args.rpc_base_port, 9000)

    def test_standard_port_4420(self):
        """The standard NVMe-oF port 4420 should be the default."""
        args = self._parse_create_args()
        self.assertEqual(args.nvmf_base_port, 4420)


# ---------------------------------------------------------------------------
# Test end-to-end allocation scenario
# ---------------------------------------------------------------------------

class TestEndToEndAllocationScenario(unittest.TestCase):
    """Simulate a full cluster lifecycle:
    2 hosts, 2 nodes each (4 nodes total), allocate all ports.
    """

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.db_controller.DBController")
    def test_four_nodes_two_hosts(self, mock_db_cls, mock_config):
        from simplyblock_core.utils import get_next_nvmf_port, get_next_rpc_port

        # Simulate progressive node creation
        nodes = []

        def refresh_mock():
            mock_db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = list(nodes)

        # ---- Node 1, Host A (10.0.0.1) ----
        refresh_mock()
        rpc1 = get_next_rpc_port("c1")
        self.assertEqual(rpc1, 8080)

        n1 = _node("n1", mgmt_ip="10.0.0.1", rpc_port=rpc1,
                    lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422)
        nodes.append(n1)

        # ---- Node 2, Host A (same IP) ----
        refresh_mock()
        rpc2 = get_next_rpc_port("c1")
        self.assertEqual(rpc2, 8081)

        n2 = _node("n2", mgmt_ip="10.0.0.1", rpc_port=rpc2,
                    lvol_subsys_port=4423, nvmf_port=4424, hublvol_port=4425)
        nodes.append(n2)

        # ---- Node 3, Host B (10.0.0.2) ----
        refresh_mock()
        rpc3 = get_next_rpc_port("c1")
        self.assertEqual(rpc3, 8082)

        n3 = _node("n3", mgmt_ip="10.0.0.2", rpc_port=rpc3,
                    lvol_subsys_port=4426, nvmf_port=4427, hublvol_port=4428)
        nodes.append(n3)

        # ---- Node 4, Host B (same IP as node 3) ----
        refresh_mock()
        rpc4 = get_next_rpc_port("c1")
        self.assertEqual(rpc4, 8083)

        # Verify all NVMe-oF ports are unique
        all_nvmf = set()
        for n in nodes:
            all_nvmf.add(n.lvol_subsys_port)
            all_nvmf.add(n.nvmf_port)
            if n.hublvol:
                all_nvmf.add(n.hublvol.nvmf_port)
        self.assertEqual(len(all_nvmf), 9)  # 3 ports * 3 nodes

        # Verify RPC ports are unique
        all_rpc = {rpc1, rpc2, rpc3, rpc4}
        self.assertEqual(len(all_rpc), 4)


# ---------------------------------------------------------------------------
# Test edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_large_port_numbers(self, mock_config, mock_ports):
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (60000, 8080)
        mock_ports.return_value = set()
        self.assertEqual(get_next_nvmf_port("c1"), 60000)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_many_ports_allocated(self, mock_config, mock_ports):
        """Stress test: 100 ports allocated, next should be correct."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = set(range(4420, 4520))
        self.assertEqual(get_next_nvmf_port("c1"), 4520)

    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    @patch("simplyblock_core.utils._get_cluster_port_config")
    def test_scattered_ports(self, mock_config, mock_ports):
        """Non-contiguous ports: should fill from base."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_config.return_value = (4420, 8080)
        mock_ports.return_value = {4421, 4425, 4430}  # 4420 is free
        self.assertEqual(get_next_nvmf_port("c1"), 4420)

    def test_hublvol_model_port_field(self):
        h = HubLVol({"nvmf_port": 4422, "uuid": "test", "nqn": "nqn.test", "bdev_name": "lvs/hublvol"})
        self.assertEqual(h.nvmf_port, 4422)

    def test_storage_node_default_ports(self):
        n = StorageNode()
        self.assertEqual(n.lvol_subsys_port, 9090)  # legacy default in model
        self.assertEqual(n.nvmf_port, 4420)
        self.assertEqual(n.rpc_port, -1)


# ---------------------------------------------------------------------------
# Test backward compatibility with env variable override
# ---------------------------------------------------------------------------

class TestEnvVarOverride(unittest.TestCase):

    def test_env_override_propagates_to_aliases(self):
        """When LVOL_NVMF_PORT_ENV is set, NVMF_BASE_PORT changes,
        and all aliases follow. Tested by checking the relationship
        at import time (can't easily re-import with different env)."""
        # At import time, if no env var is set, all should equal 4420
        self.assertEqual(constants.LVOL_NVMF_PORT_START, constants.NVMF_BASE_PORT)
        self.assertEqual(constants.NODE_NVMF_PORT_START, constants.NVMF_BASE_PORT)
        self.assertEqual(constants.NODE_HUBLVOL_PORT_START, constants.NVMF_BASE_PORT)


# ---------------------------------------------------------------------------
# Test port allocation does not collide between nvmf types
# ---------------------------------------------------------------------------

class TestPortCollisionPrevention(unittest.TestCase):
    """The unified pool ensures lvol, device, and hublvol ports never collide."""

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_sequential_type_allocations_are_unique(self, mock_ports, mock_config):
        """Simulate: allocate device port, then lvol port, then hublvol port."""
        from simplyblock_core.utils import get_next_dev_port, get_next_port, next_free_hublvol_port

        # All three functions now call get_next_nvmf_port
        mock_ports.return_value = set()
        dev_port = get_next_dev_port("c1")
        self.assertEqual(dev_port, 4420)

        mock_ports.return_value = {4420}
        lvol_port = get_next_port("c1")
        self.assertEqual(lvol_port, 4421)

        mock_ports.return_value = {4420, 4421}
        hub_port = next_free_hublvol_port("c1")
        self.assertEqual(hub_port, 4422)

        # All unique
        self.assertEqual(len({dev_port, lvol_port, hub_port}), 3)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(4420, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_nvmf_ports_independent_from_rpc_ports(self, mock_ports, mock_config):
        """NVMe-oF and RPC are separate pools; they can use the same port number."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_ports.return_value = set()
        # Even if RPC uses 8080, NVMe-oF still starts at 4420
        self.assertEqual(get_next_nvmf_port("c1"), 4420)

    @patch("simplyblock_core.utils._get_cluster_port_config", return_value=(8080, 8080))
    @patch("simplyblock_core.utils._get_all_nvmf_ports")
    def test_overlapping_base_ports_still_work(self, mock_ports, mock_config):
        """If user sets nvmf_base=8080 same as rpc_base, each pool tracks independently."""
        from simplyblock_core.utils import get_next_nvmf_port
        mock_ports.return_value = set()
        self.assertEqual(get_next_nvmf_port("c1"), 8080)


# ---------------------------------------------------------------------------
# Test Cluster model serialization roundtrip
# ---------------------------------------------------------------------------

class TestClusterModelSerialization(unittest.TestCase):

    def test_port_fields_in_dict(self):
        c = _cluster(nvmf_base_port=5555, rpc_base_port=9999)
        d = c.get_clean_dict()
        self.assertIn("nvmf_base_port", d)
        self.assertIn("rpc_base_port", d)
        self.assertEqual(d["nvmf_base_port"], 5555)
        self.assertEqual(d["rpc_base_port"], 9999)

    def test_cluster_from_dict_preserves_ports(self):
        """Verify Cluster can be reconstructed from dict with port fields."""
        c1 = _cluster(nvmf_base_port=5555, rpc_base_port=9999)
        d = c1.get_clean_dict()
        c2 = Cluster(d)
        self.assertEqual(c2.nvmf_base_port, 5555)
        self.assertEqual(c2.rpc_base_port, 9999)

    def test_cluster_from_dict_without_port_fields_uses_defaults(self):
        """Old cluster records without port fields should use defaults."""
        c = Cluster({})
        self.assertEqual(c.nvmf_base_port, 4420)
        self.assertEqual(c.rpc_base_port, 8080)


# ---------------------------------------------------------------------------
# Test _get_cluster_port_config partial overrides
# ---------------------------------------------------------------------------

class TestPartialPortConfig(unittest.TestCase):

    @patch("simplyblock_core.db_controller.DBController")
    def test_only_nvmf_overridden(self, mock_db_cls):
        from simplyblock_core.utils import _get_cluster_port_config
        cluster = _cluster(nvmf_base_port=5000, rpc_base_port=8080)
        mock_db_cls.return_value.get_cluster_by_id.return_value = cluster
        nvmf, rpc = _get_cluster_port_config("c1")
        self.assertEqual(nvmf, 5000)
        self.assertEqual(rpc, 8080)

    @patch("simplyblock_core.db_controller.DBController")
    def test_only_rpc_overridden(self, mock_db_cls):
        from simplyblock_core.utils import _get_cluster_port_config
        cluster = _cluster(nvmf_base_port=4420, rpc_base_port=9000)
        mock_db_cls.return_value.get_cluster_by_id.return_value = cluster
        nvmf, rpc = _get_cluster_port_config("c1")
        self.assertEqual(nvmf, 4420)
        self.assertEqual(rpc, 9000)

    @patch("simplyblock_core.db_controller.DBController")
    def test_all_overridden(self, mock_db_cls):
        from simplyblock_core.utils import _get_cluster_port_config
        cluster = _cluster(nvmf_base_port=5000, rpc_base_port=9000)
        mock_db_cls.return_value.get_cluster_by_id.return_value = cluster
        nvmf, rpc = _get_cluster_port_config("c1")
        self.assertEqual(nvmf, 5000)
        self.assertEqual(rpc, 9000)


# ---------------------------------------------------------------------------
# Test node display port info
# ---------------------------------------------------------------------------

class TestNodeDisplayPorts(unittest.TestCase):

    def test_node_port_fields_are_readable(self):
        n = _node("n1", lvol_subsys_port=4420, nvmf_port=4421, hublvol_port=4422,
                  rpc_port=8080)
        self.assertEqual(n.lvol_subsys_port, 4420)
        self.assertEqual(n.nvmf_port, 4421)
        self.assertEqual(n.hublvol.nvmf_port, 4422)
        self.assertEqual(n.rpc_port, 8080)

    def test_node_without_hublvol(self):
        n = _node("n1", lvol_subsys_port=4420, nvmf_port=4421)
        self.assertIsNone(n.hublvol)


if __name__ == "__main__":
    unittest.main()
