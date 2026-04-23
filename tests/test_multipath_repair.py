# coding=utf-8
"""
test_multipath_repair.py -- unit tests for multipath path repair logic.

Covers:
  - storage_node_ops.repair_multipath_controller skip / repair / failure
  - Health check service integration with multipath repair
  - Hublvol controller multipath path repair in health_controller
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice, RemoteJMDevice
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.hublvol import HubLVol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(uuid="node-1", mgmt_ip="10.0.0.1", rpc_port=5260,
          active_rdma=False, active_tcp=True, data_nics=None,
          status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          secondary_node_id="", lvstore_stack_secondary="",
          lvstore_stack_tertiary="", lvstore="lvs_test",
          lvstore_status="ready"):
    n = StorageNode()
    n.uuid = uuid
    n.mgmt_ip = mgmt_ip
    n.rpc_port = rpc_port
    n.rpc_username = "user"
    n.rpc_password = "pass"
    n.active_rdma = active_rdma
    n.active_tcp = active_tcp
    n.data_nics = data_nics or []
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = f"host-{uuid}"
    n.secondary_node_id = secondary_node_id
    n.lvstore_stack_secondary = lvstore_stack_secondary
    n.lvstore_stack_tertiary = lvstore_stack_tertiary
    n.lvstore = lvstore
    n.lvstore_status = lvstore_status
    return n


def _device(nvmf_multipath=True, nvmf_ip="10.0.0.10,10.0.0.11",
            nvmf_nqn="nqn.test", nvmf_port=4420, alceml_bdev="dev0",
            node_id="node-target", status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.nvmf_multipath = nvmf_multipath
    d.nvmf_ip = nvmf_ip
    d.nvmf_nqn = nvmf_nqn
    d.nvmf_port = nvmf_port
    d.alceml_bdev = alceml_bdev
    d.node_id = node_id
    d.status = status
    return d


def _iface(ip, trtype="TCP"):
    iface = IFace()
    iface.ip4_address = ip
    iface.trtype = trtype
    return iface


def _hublvol(bdev_name="hublvol0", nqn="nqn.hublvol", nvmf_port=4420):
    h = HubLVol()
    h.bdev_name = bdev_name
    h.nqn = nqn
    h.nvmf_port = nvmf_port
    return h


def _controller_list_response(primary_ip, alternate_ips=None, state="enabled"):
    """Build a mock return value for bdev_nvme_controller_list."""
    alt_trids = [{"traddr": ip} for ip in (alternate_ips or [])]
    return [{
        "ctrlrs": [{
            "state": state,
            "trid": {"traddr": primary_ip},
            "alternate_trids": alt_trids,
        }]
    }]


# ---------------------------------------------------------------------------
# 1. repair_multipath_controller unit tests
# ---------------------------------------------------------------------------

class TestRepairMultipathController(unittest.TestCase):
    """Direct tests for storage_node_ops.repair_multipath_controller."""

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_skips_non_multipath_device(self, mock_db_cls):
        """Device with nvmf_multipath=False should return True immediately."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_multipath=False)
        node = _node()
        node.rpc_client = MagicMock()

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)
        # rpc_client should not have been called at all
        node.rpc_client.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_skips_single_ip_device(self, mock_db_cls):
        """Device with only one nvmf_ip should return True (not real multipath)."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_multipath=True, nvmf_ip="10.0.0.10")
        node = _node()
        node.rpc_client = MagicMock()

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_all_paths_present_no_attach(self, mock_db_cls):
        """When all expected IPs are already attached, no attach call is made."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target")
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=["10.0.0.11"])
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_missing_path_triggers_attach(self, mock_db_cls):
        """When one IP is missing from attached paths, attach is called for it."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target", active_tcp=True, active_rdma=False)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        # Only one IP attached -- 10.0.0.11 is missing
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=[])
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_called_once_with(
            "ctrl0", device.nvmf_nqn, "10.0.0.11", device.nvmf_port,
            "TCP", multipath="multipath")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_controller_gone_returns_true(self, mock_db_cls):
        """When controller_list returns empty/None, return True (reconnect later)."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = None
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_attach_failure_returns_false(self, mock_db_cls):
        """When attach_controller raises an exception, return False."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target", active_tcp=True, active_rdma=False)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=[])
        rpc.bdev_nvme_attach_controller.side_effect = RuntimeError("RPC failed")
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertFalse(result)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_rdma_transport_used_when_active(self, mock_db_cls):
        """When target node has active_rdma, RDMA transport type is used."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target", active_tcp=False, active_rdma=True)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=[])
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_called_once_with(
            "ctrl0", device.nvmf_nqn, "10.0.0.11", device.nvmf_port,
            "RDMA", multipath="multipath")

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_transport_returns_false(self, mock_db_cls):
        """When target node has neither rdma nor tcp active, return False."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target", active_tcp=False, active_rdma=False)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=[])
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        self.assertFalse(result)

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_disabled_controller_state_skipped(self, mock_db_cls):
        """Controller entries with state != 'enabled' should be skipped."""
        from simplyblock_core.storage_node_ops import repair_multipath_controller

        device = _device(nvmf_ip="10.0.0.10,10.0.0.11")
        target_node = _node(uuid="node-target")
        mock_db_cls.return_value.get_storage_node_by_id.return_value = target_node

        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = _controller_list_response(
            "10.0.0.10", alternate_ips=[], state="disabled")
        node = _node()
        node.rpc_client = MagicMock(return_value=rpc)

        result = repair_multipath_controller("ctrl0", device, node)
        # No attach should be called because the only controller entry is disabled
        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_not_called()


# ---------------------------------------------------------------------------
# 2. Health check service integration -- remote device multipath repair
# ---------------------------------------------------------------------------

class TestHealthCheckMultipathIntegration(unittest.TestCase):
    """Test that health_check_service.check_node calls repair when appropriate."""

    def _setup_check_node_mocks(self, snode, devices, remote_devices,
                                 cluster_status=Cluster.STATUS_ACTIVE,
                                 node_bdev_names=None):
        """Return a dict of patch targets -> mocks for check_node."""
        from simplyblock_core.models.cluster import Cluster as ClusterModel
        cluster = ClusterModel()
        cluster.uuid = "cluster-1"
        cluster.status = cluster_status
        cluster.ha_type = "ha"
        cluster.blk_size = 4096
        cluster.page_size_in_blocks = 1024

        db_mock = MagicMock()
        db_mock.get_storage_node_by_id.return_value = snode
        db_mock.get_cluster_by_id.return_value = cluster
        db_mock.get_storage_nodes.return_value = [snode]
        db_mock.get_storage_nodes_by_cluster_id.return_value = [snode]

        snode.nvme_devices = devices
        snode.remote_devices = remote_devices
        snode.remote_jm_devices = []
        snode.jm_device = MagicMock()
        snode.jm_device.get_id.return_value = ""
        snode.enable_ha_jm = False

        return db_mock, cluster

    @patch("simplyblock_core.services.health_check_service.time.sleep")
    @patch("simplyblock_core.services.health_check_service.RPCClient")
    @patch("simplyblock_core.services.health_check_service.storage_node_ops")
    @patch("simplyblock_core.services.health_check_service.health_controller")
    @patch("simplyblock_core.services.health_check_service.db")
    def test_repair_called_when_bdev_exists_and_multipath(
            self, mock_db_mod, mock_hc, mock_sn_ops, mock_rpc_cls, _mock_sleep):
        """When remote bdev exists and device.nvmf_multipath is True, repair is called."""
        from simplyblock_core.services import health_check_service

        snode = _node(uuid="node-1", status=StorageNode.STATUS_ONLINE)

        org_dev = _device(nvmf_multipath=True, alceml_bdev="dev0",
                          node_id="node-target", status=NVMeDevice.STATUS_ONLINE)

        remote_dev = RemoteDevice()
        remote_dev.remote_bdev = "remote_dev0n1"
        remote_dev.node_id = "node-target"
        remote_dev.nvmf_multipath = False  # RemoteDevice field, not checked here

        org_node = _node(uuid="node-target", status=StorageNode.STATUS_ONLINE)

        # Configure db mock — route get_storage_node_by_id by id so both
        # the self-refresh calls and the per-remote-device lookups resolve.
        def _get_node(node_id):
            return org_node if node_id == "node-target" else snode
        mock_db_mod.get_storage_node_by_id.side_effect = _get_node
        mock_db_mod.get_cluster_by_id.return_value = MagicMock(
            status="active", ha_type="ha")
        mock_db_mod.get_storage_device_by_id.return_value = org_dev

        snode.nvme_devices = []
        snode.remote_devices = [remote_dev]
        snode.remote_jm_devices = []
        snode.jm_device = MagicMock()
        snode.jm_device.get_id.return_value = ""
        snode.enable_ha_jm = False

        # health_controller.check_bdev returns True (bdev exists)
        mock_hc.check_bdev.return_value = True
        mock_hc.check_subsystem.return_value = True

        # sync returns False (no changes)
        mock_sn_ops.sync_remote_devices_from_spdk.return_value = False

        # RPCClient.get_bdevs / subsystem_list must not hit the network.
        mock_rpc_cls.return_value.get_bdevs.return_value = []
        mock_rpc_cls.return_value.subsystem_list.return_value = []

        # Now call check_node -- we want to verify repair is called
        # We need to patch at the module level since check_node uses a module-level db
        health_check_service.check_node(snode)

        # repair_multipath_controller should have been called for the multipath device
        mock_sn_ops.repair_multipath_controller.assert_called_once()
        args = mock_sn_ops.repair_multipath_controller.call_args
        self.assertEqual(args[0][0], "remote_dev0")  # ctrl_name
        self.assertEqual(args[0][1], org_dev)
        self.assertEqual(args[0][2], snode)

    @patch("simplyblock_core.services.health_check_service.time.sleep")
    @patch("simplyblock_core.services.health_check_service.RPCClient")
    @patch("simplyblock_core.services.health_check_service.storage_node_ops")
    @patch("simplyblock_core.services.health_check_service.health_controller")
    @patch("simplyblock_core.services.health_check_service.db")
    def test_repair_not_called_when_multipath_disabled(
            self, mock_db_mod, mock_hc, mock_sn_ops, mock_rpc_cls, _mock_sleep):
        """When device.nvmf_multipath is False, repair is NOT called."""
        from simplyblock_core.services import health_check_service

        snode = _node(uuid="node-1", status=StorageNode.STATUS_ONLINE)

        org_dev = _device(nvmf_multipath=False, alceml_bdev="dev0",
                          node_id="node-target", status=NVMeDevice.STATUS_ONLINE)

        remote_dev = RemoteDevice()
        remote_dev.remote_bdev = "remote_dev0n1"
        remote_dev.node_id = "node-target"

        org_node = _node(uuid="node-target", status=StorageNode.STATUS_ONLINE)

        def _get_node(node_id):
            return org_node if node_id == "node-target" else snode
        mock_db_mod.get_storage_node_by_id.side_effect = _get_node
        mock_db_mod.get_cluster_by_id.return_value = MagicMock(
            status="active", ha_type="ha")
        mock_db_mod.get_storage_device_by_id.return_value = org_dev

        snode.nvme_devices = []
        snode.remote_devices = [remote_dev]
        snode.remote_jm_devices = []
        snode.jm_device = MagicMock()
        snode.jm_device.get_id.return_value = ""
        snode.enable_ha_jm = False

        mock_hc.check_bdev.return_value = True
        mock_hc.check_subsystem.return_value = True
        mock_sn_ops.sync_remote_devices_from_spdk.return_value = False

        mock_rpc_cls.return_value.get_bdevs.return_value = []
        mock_rpc_cls.return_value.subsystem_list.return_value = []

        health_check_service.check_node(snode)

        mock_sn_ops.repair_multipath_controller.assert_not_called()

    @patch("simplyblock_core.services.health_check_service.time.sleep")
    @patch("simplyblock_core.services.health_check_service.RPCClient")
    @patch("simplyblock_core.services.health_check_service.storage_node_ops")
    @patch("simplyblock_core.services.health_check_service.health_controller")
    @patch("simplyblock_core.services.health_check_service.db")
    def test_jm_repair_called_when_multipath_enabled(
            self, mock_db_mod, mock_hc, mock_sn_ops, mock_rpc_cls, _mock_sleep):
        """For remote JM devices with nvmf_multipath=True, repair is called."""
        from simplyblock_core.services import health_check_service

        snode = _node(uuid="node-1", status=StorageNode.STATUS_ONLINE)
        snode.enable_ha_jm = True

        remote_jm = RemoteJMDevice()
        remote_jm.remote_bdev = "jm_ctrl0n1"
        remote_jm.node_id = "node-jm"
        remote_jm.nvmf_multipath = True

        snode.nvme_devices = []
        snode.remote_devices = []
        snode.remote_jm_devices = [remote_jm]
        snode.jm_device = MagicMock()
        snode.jm_device.get_id.return_value = "local-jm"
        snode.jm_device.jm_bdev = "local_jm_bdev"
        snode.jm_ids = []

        mock_db_mod.get_storage_node_by_id.return_value = snode
        mock_db_mod.get_cluster_by_id.return_value = MagicMock(
            status="active", ha_type="ha")

        mock_hc.check_bdev.side_effect = [
            True,   # jm_device.jm_bdev check
            True,   # remote_jm.remote_bdev check
        ]
        mock_hc.check_subsystem.return_value = True
        mock_sn_ops.sync_remote_devices_from_spdk.return_value = False

        mock_rpc_cls.return_value.get_bdevs.return_value = []
        mock_rpc_cls.return_value.subsystem_list.return_value = []

        health_check_service.check_node(snode)

        # repair should be called with ctrl_name = remote_bdev with "n1" replaced
        mock_sn_ops.repair_multipath_controller.assert_called_once()
        args = mock_sn_ops.repair_multipath_controller.call_args
        self.assertEqual(args[0][0], "jm_ctrl0")  # "jm_ctrl0n1".replace("n1","")
        self.assertEqual(args[0][2], snode)


# ---------------------------------------------------------------------------
# 3. Hublvol multipath path repair in health_controller
# ---------------------------------------------------------------------------

class TestHublvolMultipathRepair(unittest.TestCase):
    """Test _check_sec_node_hublvol multipath path repair logic."""

    @patch("simplyblock_core.controllers.health_controller.DBController")
    @patch("simplyblock_core.controllers.health_controller.RPCClient")
    @patch("simplyblock_core.controllers.health_controller.check_bdev")
    def test_repairs_missing_primary_nic_path(self, mock_check_bdev, mock_rpc_cls, mock_db_cls):
        """When a primary NIC path is missing, it should be re-attached."""
        from simplyblock_core.controllers.health_controller import _check_sec_node_hublvol

        primary_node = _node(
            uuid="primary-1",
            active_tcp=True,
            active_rdma=False,
            data_nics=[_iface("10.0.0.50", "TCP"), _iface("10.0.0.51", "TCP")],
            lvstore_status="ready",
            status=StorageNode.STATUS_ONLINE,
        )
        primary_node.hublvol = _hublvol(bdev_name="hublvol0", nqn="nqn.hub", nvmf_port=4420)
        primary_node.raid = "raid0"
        primary_node.lvstore = "lvs_primary"
        primary_node.secondary_node_id = ""

        sec_node = _node(
            uuid="sec-1",
            lvstore_stack_secondary="primary-1",
        )

        mock_db_cls.return_value.get_storage_node_by_id.return_value = primary_node
        mock_db_cls.return_value.get_cluster_by_id.return_value = MagicMock(
            blk_size=4096, page_size_in_blocks=1024)

        rpc = MagicMock()
        mock_rpc_cls.return_value = rpc

        # Controller exists with only one path (10.0.0.50); 10.0.0.51 is missing
        controller_resp = _controller_list_response("10.0.0.50", alternate_ips=[])
        rpc.bdev_nvme_controller_list.return_value = controller_resp

        # get_bdevs returns something so node_bdev is populated
        rpc.get_bdevs.return_value = [
            {"name": "hublvol0n1", "aliases": []},
        ]
        rpc.subsystem_list.return_value = []
        rpc.bdev_lvol_get_lvstores.return_value = [{
            "name": "lvs_primary",
            "lvs leadership": False,
            "lvs_secondary": True,
            "lvs_read_only": False,
            "lvs_redirect": True,
            "remote_bdev": "hublvol0n1",
            "connect_state": True,
            "base_bdev": "raid0",
            "block_size": 4096,
            "cluster_size": 1024,
        }]
        rpc.bdev_nvme_attach_controller.return_value = True
        mock_check_bdev.return_value = True

        result = _check_sec_node_hublvol(sec_node, auto_fix=True)

        self.assertTrue(result)
        # Should have called attach for the missing IP 10.0.0.51
        rpc.bdev_nvme_attach_controller.assert_called_once_with(
            "hublvol0", "nqn.hub", "10.0.0.51", 4420, "TCP", multipath="multipath")

    @patch("simplyblock_core.controllers.health_controller.DBController")
    @patch("simplyblock_core.controllers.health_controller.RPCClient")
    @patch("simplyblock_core.controllers.health_controller.check_bdev")
    def test_no_repair_when_all_paths_present(self, mock_check_bdev, mock_rpc_cls, mock_db_cls):
        """When all primary NIC paths are attached, no attach call is made."""
        from simplyblock_core.controllers.health_controller import _check_sec_node_hublvol

        primary_node = _node(
            uuid="primary-1",
            active_tcp=True,
            active_rdma=False,
            data_nics=[_iface("10.0.0.50", "TCP"), _iface("10.0.0.51", "TCP")],
            lvstore_status="ready",
            status=StorageNode.STATUS_ONLINE,
        )
        primary_node.hublvol = _hublvol(bdev_name="hublvol0", nqn="nqn.hub", nvmf_port=4420)
        primary_node.raid = "raid0"
        primary_node.lvstore = "lvs_primary"
        primary_node.secondary_node_id = ""

        sec_node = _node(
            uuid="sec-1",
            lvstore_stack_secondary="primary-1",
        )

        mock_db_cls.return_value.get_storage_node_by_id.return_value = primary_node
        mock_db_cls.return_value.get_cluster_by_id.return_value = MagicMock(
            blk_size=4096, page_size_in_blocks=1024)

        rpc = MagicMock()
        mock_rpc_cls.return_value = rpc

        # Both paths already attached
        controller_resp = _controller_list_response(
            "10.0.0.50", alternate_ips=["10.0.0.51"])
        rpc.bdev_nvme_controller_list.return_value = controller_resp
        rpc.get_bdevs.return_value = [
            {"name": "hublvol0n1", "aliases": []},
        ]
        rpc.subsystem_list.return_value = []
        rpc.bdev_lvol_get_lvstores.return_value = [{
            "name": "lvs_primary",
            "lvs leadership": False,
            "lvs_secondary": True,
            "lvs_read_only": False,
            "lvs_redirect": True,
            "remote_bdev": "hublvol0n1",
            "connect_state": True,
            "base_bdev": "raid0",
            "block_size": 4096,
            "cluster_size": 1024,
        }]
        mock_check_bdev.return_value = True

        result = _check_sec_node_hublvol(sec_node, auto_fix=True)

        self.assertTrue(result)
        rpc.bdev_nvme_attach_controller.assert_not_called()

    @patch("simplyblock_core.controllers.health_controller.DBController")
    @patch("simplyblock_core.controllers.health_controller.RPCClient")
    @patch("simplyblock_core.controllers.health_controller.check_bdev")
    def test_rdma_nic_repair(self, mock_check_bdev, mock_rpc_cls, mock_db_cls):
        """When primary uses RDMA, only RDMA NICs are considered and RDMA transport is used."""
        from simplyblock_core.controllers.health_controller import _check_sec_node_hublvol

        primary_node = _node(
            uuid="primary-1",
            active_tcp=False,
            active_rdma=True,
            data_nics=[
                _iface("10.0.0.50", "RDMA"),
                _iface("10.0.0.51", "RDMA"),
                _iface("10.0.0.99", "TCP"),   # should be ignored
            ],
            lvstore_status="ready",
            status=StorageNode.STATUS_ONLINE,
        )
        primary_node.hublvol = _hublvol(bdev_name="hublvol0", nqn="nqn.hub", nvmf_port=4420)
        primary_node.raid = "raid0"
        primary_node.lvstore = "lvs_primary"
        primary_node.secondary_node_id = ""

        sec_node = _node(
            uuid="sec-1",
            lvstore_stack_secondary="primary-1",
        )

        mock_db_cls.return_value.get_storage_node_by_id.return_value = primary_node
        mock_db_cls.return_value.get_cluster_by_id.return_value = MagicMock(
            blk_size=4096, page_size_in_blocks=1024)

        rpc = MagicMock()
        mock_rpc_cls.return_value = rpc

        # Only first RDMA NIC attached; second RDMA NIC missing
        controller_resp = _controller_list_response("10.0.0.50", alternate_ips=[])
        rpc.bdev_nvme_controller_list.return_value = controller_resp
        rpc.get_bdevs.return_value = [{"name": "hublvol0n1", "aliases": []}]
        rpc.subsystem_list.return_value = []
        rpc.bdev_lvol_get_lvstores.return_value = [{
            "name": "lvs_primary",
            "lvs leadership": False,
            "lvs_secondary": True,
            "lvs_read_only": False,
            "lvs_redirect": True,
            "remote_bdev": "hublvol0n1",
            "connect_state": True,
            "base_bdev": "raid0",
            "block_size": 4096,
            "cluster_size": 1024,
        }]
        rpc.bdev_nvme_attach_controller.return_value = True
        mock_check_bdev.return_value = True

        result = _check_sec_node_hublvol(sec_node, auto_fix=True)

        self.assertTrue(result)
        # Only the missing RDMA IP should be re-attached, TCP NIC ignored
        rpc.bdev_nvme_attach_controller.assert_called_once_with(
            "hublvol0", "nqn.hub", "10.0.0.51", 4420, "RDMA", multipath="multipath")


if __name__ == "__main__":
    unittest.main()
