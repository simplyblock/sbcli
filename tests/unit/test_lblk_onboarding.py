# coding=utf-8
"""Unit tests for lblk-mode device onboarding.

Covered:
  - utils.addAioDevices: fresh-create vs reuse (restart idempotency),
    by-id-preferred filename, examine + qd-sampling wiring, zero-size skip,
    create-failure raise, full NVMeDevice field population.
  - storage_node_ops._classify_existing_endpoint_record: serial-based
    overlap detection for lblk nodes (add-node idempotency).
  - cluster_ops._validated_device_mode.
  - constants sanity (placeholder BDF shape, excluded prefixes are a tuple
    usable with str.startswith).
"""

import re
import unittest
from unittest.mock import MagicMock

from simplyblock_core import cluster_ops, constants, utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import _classify_existing_endpoint_record


def _entry(name="sdb", serial="S1", by_id="/dev/disk/by-id/wwn-1",
           size=100 << 30, numa=0, model="MODEL-X"):
    return {"name": name, "serial": serial, "by_id": by_id, "size": size,
            "numa": numa, "model": model, "current_path": f"/dev/{name}",
            "has_partitions": False}


def _snode(node_id="node-1", cluster_id="cluster-1", physical_label=3):
    n = StorageNode()
    n.uuid = node_id
    n.cluster_id = cluster_id
    n.physical_label = physical_label
    return n


class _FakeRpc:
    """Minimal SPDK RPC fake for addAioDevices: get_bdevs answers from an
    internal registry; bdev_aio_create registers; every call is recorded."""

    def __init__(self, existing=None, create_ok=True, block_size=4096,
                 num_blocks=1000):
        self.host = "test-host"
        self.bdevs = dict(existing or {})
        self.create_ok = create_ok
        self.block_size = block_size
        self.num_blocks = num_blocks
        self.calls = []

    def get_bdevs(self, name):
        self.calls.append(("get_bdevs", name))
        if name in self.bdevs:
            return [self.bdevs[name]]
        return None

    def bdev_aio_create(self, name, filename, block_size=0):
        self.calls.append(("bdev_aio_create", name, filename))
        if not self.create_ok:
            return None
        self.bdevs[name] = {"name": name, "block_size": self.block_size,
                            "num_blocks": self.num_blocks}
        return name

    def bdev_examine(self, name):
        self.calls.append(("bdev_examine", name))
        return True

    def bdev_wait_for_examine(self):
        self.calls.append(("bdev_wait_for_examine",))
        return True

    def bdev_set_qd_sampling_period(self, name, period):
        self.calls.append(("qd_sampling", name, period))
        return True

    def _called(self, method):
        return [c for c in self.calls if c[0] == method]


class TestAddAioDevices(unittest.TestCase):

    def test_fresh_create_full_field_population(self):
        rpc = _FakeRpc()
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(len(devs), 1)
        dev = devs[0]
        self.assertIsInstance(dev, NVMeDevice)
        self.assertEqual(dev.bdev_type, "aio")
        self.assertEqual(dev.nvme_bdev, utils.aio_bdev_name_for_serial("S1"))
        self.assertEqual(dev.serial_number, "S1")
        self.assertEqual(dev.device_name, "sdb")
        self.assertEqual(dev.device_path, "/dev/sdb")
        self.assertEqual(dev.by_id_path, "/dev/disk/by-id/wwn-1")
        self.assertEqual(dev.pcie_address, "")
        self.assertEqual(dev.nvme_controller, "")
        self.assertEqual(dev.model_id, "MODEL-X")
        self.assertEqual(dev.size, 4096 * 1000)
        self.assertEqual(dev.physical_label, 3)
        self.assertEqual(dev.node_id, "node-1")
        self.assertEqual(dev.cluster_id, "cluster-1")
        self.assertEqual(dev.status, NVMeDevice.STATUS_ONLINE)

    def test_filename_prefers_by_id(self):
        rpc = _FakeRpc()
        utils.addAioDevices(rpc, _snode(), [_entry()])
        create = rpc._called("bdev_aio_create")[0]
        self.assertEqual(create[2], "/dev/disk/by-id/wwn-1")

    def test_filename_falls_back_to_current_path(self):
        rpc = _FakeRpc()
        utils.addAioDevices(rpc, _snode(), [_entry(by_id="")])
        create = rpc._called("bdev_aio_create")[0]
        self.assertEqual(create[2], "/dev/sdb")

    def test_reuse_existing_bdev_no_create(self):
        name = utils.aio_bdev_name_for_serial("S1")
        rpc = _FakeRpc(existing={name: {"name": name, "block_size": 4096,
                                        "num_blocks": 10}})
        devs = utils.addAioDevices(rpc, _snode(), [_entry()])
        self.assertEqual(len(devs), 1)
        self.assertEqual(rpc._called("bdev_aio_create"), [])

    def test_examine_and_qd_sampling_wired(self):
        rpc = _FakeRpc()
        utils.addAioDevices(rpc, _snode(), [_entry()])
        self.assertTrue(rpc._called("bdev_examine"))
        self.assertTrue(rpc._called("bdev_wait_for_examine"))
        qd = rpc._called("qd_sampling")[0]
        self.assertEqual(qd[2], constants.AIO_QD_SAMPLING_PERIOD_US)

    def test_zero_size_skipped(self):
        rpc = _FakeRpc(num_blocks=0)
        devs = utils.addAioDevices(rpc, _snode(), [_entry()])
        self.assertEqual(devs, [])

    def test_create_failure_raises(self):
        rpc = _FakeRpc(create_ok=False)
        with self.assertRaises(Exception):
            utils.addAioDevices(rpc, _snode(), [_entry()])

    def test_multiple_devices(self):
        rpc = _FakeRpc()
        devs = utils.addAioDevices(rpc, _snode(), [
            _entry(name="sdb", serial="S1"), _entry(name="sdc", serial="S2")])
        self.assertEqual([d.serial_number for d in devs], ["S1", "S2"])
        self.assertEqual(len({d.nvme_bdev for d in devs}), 2)


class TestClassifyEndpointRecordLblk(unittest.TestCase):

    def _db_with(self, node):
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [node]
        return db

    def _lblk_node(self, status, serials=("S1",)):
        n = StorageNode()
        n.uuid = "existing"
        n.api_endpoint = "1.2.3.4:5000"
        n.status = status
        n.ssd_pcie = []
        n.lblk_devices = [{"name": f"sd{i}", "serial": s}
                          for i, s in enumerate(serials)]
        return n

    def test_serial_overlap_online_is_already_added(self):
        node = self._lblk_node(StorageNode.STATUS_ONLINE)
        action, found = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "1.2.3.4:5000", [], lblk_serials=["S1"])
        self.assertEqual(action, "already_added")
        self.assertIs(found, node)

    def test_serial_overlap_in_creation_is_cleanup(self):
        node = self._lblk_node(StorageNode.STATUS_IN_CREATION)
        action, _ = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "1.2.3.4:5000", [], lblk_serials=["S1"])
        self.assertEqual(action, "cleanup")

    def test_serial_overlap_other_status_is_conflict(self):
        node = self._lblk_node(StorageNode.STATUS_OFFLINE)
        action, _ = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "1.2.3.4:5000", [], lblk_serials=["S1"])
        self.assertEqual(action, "conflict")

    def test_no_serial_overlap_no_match(self):
        node = self._lblk_node(StorageNode.STATUS_ONLINE, serials=("OTHER",))
        action, found = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "1.2.3.4:5000", [], lblk_serials=["S1"])
        self.assertIsNone(action)
        self.assertIsNone(found)

    def test_different_endpoint_ignored(self):
        node = self._lblk_node(StorageNode.STATUS_ONLINE)
        action, _ = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "9.9.9.9:5000", [], lblk_serials=["S1"])
        self.assertIsNone(action)

    def test_nvme_pcie_overlap_still_works(self):
        node = StorageNode()
        node.uuid = "existing"
        node.api_endpoint = "1.2.3.4:5000"
        node.status = StorageNode.STATUS_ONLINE
        node.ssd_pcie = ["0000:00:1e.0"]
        action, _ = _classify_existing_endpoint_record(
            self._db_with(node), "c1", "1.2.3.4:5000", ["0000:00:1e.0"])
        self.assertEqual(action, "already_added")


class TestDeviceModeValidation(unittest.TestCase):

    def test_accepts_both_modes_case_insensitive(self):
        self.assertEqual(cluster_ops._validated_device_mode("nvme"), "nvme")
        self.assertEqual(cluster_ops._validated_device_mode("LBLK"), "lblk")

    def test_none_defaults_to_nvme(self):
        self.assertEqual(cluster_ops._validated_device_mode(None), "nvme")

    def test_rejects_unknown(self):
        with self.assertRaises(ValueError):
            cluster_ops._validated_device_mode("scsi")


class TestLblkConstants(unittest.TestCase):

    def test_placeholder_is_valid_bdf_and_never_a_device(self):
        self.assertTrue(re.fullmatch(
            r"[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]",
            constants.LBLK_PCI_ALLOWED_PLACEHOLDER))
        self.assertEqual(constants.LBLK_PCI_ALLOWED_PLACEHOLDER, "0000:00:00.0")

    def test_excluded_prefixes_usable_with_startswith(self):
        self.assertIsInstance(constants.LBLK_EXCLUDED_NAME_PREFIXES, tuple)
        self.assertTrue("loop7".startswith(constants.LBLK_EXCLUDED_NAME_PREFIXES))
        self.assertFalse("sdb".startswith(constants.LBLK_EXCLUDED_NAME_PREFIXES))

    def test_watchdog_thresholds_positive(self):
        self.assertGreater(constants.AIO_HUNG_IO_STALL_POLLS, 0)
        self.assertGreater(constants.AIO_DEVICE_ABSENT_POLLS, 0)
        self.assertGreater(constants.AIO_QD_SAMPLING_PERIOD_US, 0)


if __name__ == "__main__":
    unittest.main()
