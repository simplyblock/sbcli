"""Unit tests for the block size of lblk AIO base bdevs.

A new device gets a 4096-byte AIO bdev unless 4096 is impossible, then the
device's logical block size. A device that already has a record is always
re-created with the block size it was formatted with, because alceml's
on-disk layout depends on it: 0 (legacy, created before the block size was
recorded) means "no block size", exactly as it was created.

Covered:
  - utils.choose_aio_block_size: 512e, 512n with a non-4K size, 4Kn, logical
    above 4096, an agent that does not send logical_block_size, partition
    start alignment.
  - utils.addAioDevices: new device -> chosen size recorded on the device and
    on the node's lblk_devices entry; legacy record -> no block size; 4096
    record -> 4096 whatever the inventory says; record dropped by an older
    control-plane service -> recovered from the lblk_devices entry; journal
    device record; SPDK refusing 4096 for a new device -> retried without;
    block-size mismatch -> refused.
  - utils.resolve_lblk_entries carries the block-size fields.
  - storage_node_ops._reconcile_discovered_devices restores a dropped size.
  - device_controller.restart_device / new_device_from_failed pass the
    recorded size.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import _reconcile_discovered_devices

GIB = 1 << 30


def _entry(serial="S1", size=100 * GIB, logical=512, physical=4096, **extra):
    e = {"name": "sdb", "serial": serial, "by_id": f"/dev/disk/by-id/wwn-{serial}",
         "size": size, "numa": 0, "model": "M", "current_path": "/dev/sdb",
         "has_partitions": False}
    if logical is not None:
        e["logical_block_size"] = logical
    if physical is not None:
        e["physical_block_size"] = physical
    e.update(extra)
    return e


def _snode(devices=(), lblk_devices=None, jm_device=None):
    n = StorageNode()
    n.uuid = "node-1"
    n.cluster_id = "cluster-1"
    n.nvme_devices = list(devices)
    n.lblk_devices = lblk_devices if lblk_devices is not None else [
        {"name": "sdb", "serial": "S1", "size": 100 * GIB}]
    if jm_device is not None:
        n.jm_device = jm_device
    return n


def _record(serial="S1", aio_block_size=0, status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.uuid = f"dev-{serial}"
    d.serial_number = serial
    d.bdev_type = "aio"
    d.nvme_bdev = utils.aio_bdev_name_for_serial(serial)
    d.aio_block_size = aio_block_size
    d.status = status
    return d


class _FakeRpc:
    """SPDK fake modelling bdev_aio_create's block-size rules: no block size
    -> the logical block size; one below the logical block size -> refused."""

    def __init__(self, logical=512, num_bytes=100 * GIB, existing=None,
                 ignore_block_size=False):
        self.host = "test-host"
        self.logical = logical
        self.num_bytes = num_bytes
        self.bdevs = dict(existing or {})
        self.ignore_block_size = ignore_block_size
        self.creates = []

    def bdev_get(self, name):
        return self.bdevs.get(name)

    def bdev_aio_create(self, name, filename, block_size=0):
        self.creates.append(block_size)
        if block_size and block_size < self.logical:
            return None
        bs = self.logical if (not block_size or self.ignore_block_size) else block_size
        self.bdevs[name] = {"name": name, "block_size": bs,
                            "num_blocks": self.num_bytes // bs}
        return name

    def bdev_examine(self, name):
        return True

    def bdev_wait_for_examine(self):
        return True

    def bdev_set_qd_sampling_period(self, name, period):
        return True


class TestChooseAioBlockSize(unittest.TestCase):

    def test_512e_gets_4096(self):
        self.assertEqual(utils.choose_aio_block_size(_entry(logical=512, physical=4096)), 4096)

    def test_512n_with_non_4k_size_gets_512(self):
        self.assertEqual(utils.choose_aio_block_size(
            _entry(logical=512, physical=512, size=100 * GIB + 512)), 512)

    def test_512n_with_4k_size_gets_4096(self):
        self.assertEqual(utils.choose_aio_block_size(_entry(logical=512, physical=512)), 4096)

    def test_4kn_gets_4096(self):
        self.assertEqual(utils.choose_aio_block_size(_entry(logical=4096, physical=4096)), 4096)

    def test_logical_above_4096_kept(self):
        self.assertEqual(utils.choose_aio_block_size(_entry(logical=8192)), 8192)

    def test_agent_without_field_4k_size_gets_4096(self):
        self.assertEqual(utils.choose_aio_block_size(_entry(logical=None, physical=None)), 4096)

    def test_agent_without_field_odd_size_leaves_it_to_spdk(self):
        self.assertEqual(utils.choose_aio_block_size(
            _entry(logical=None, physical=None, size=100 * GIB + 512)), 0)

    def test_unaligned_partition_start_gets_logical(self):
        self.assertEqual(utils.choose_aio_block_size(
            _entry(type="part", start_bytes=63 * 512)), 512)

    def test_aligned_partition_start_gets_4096(self):
        self.assertEqual(utils.choose_aio_block_size(
            _entry(type="part", start_bytes=2048 * 512)), 4096)


class TestAddAioDevicesBlockSize(unittest.TestCase):

    def test_new_512e_device_created_at_4096_and_recorded(self):
        rpc = _FakeRpc(logical=512)
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [4096])
        self.assertEqual(devs[0].aio_block_size, 4096)
        self.assertEqual(snode.lblk_devices[0]["aio_block_size"], 4096)

    def test_new_512n_odd_size_created_at_512(self):
        rpc = _FakeRpc(logical=512, num_bytes=100 * GIB + 512)
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_entry(physical=512, size=100 * GIB + 512)])
        self.assertEqual(rpc.creates, [512])
        self.assertEqual(devs[0].aio_block_size, 512)
        self.assertEqual(snode.lblk_devices[0]["aio_block_size"], 512)

    def test_new_4kn_device_created_at_4096(self):
        rpc = _FakeRpc(logical=4096)
        devs = utils.addAioDevices(rpc, _snode(), [_entry(logical=4096)])
        self.assertEqual(rpc.creates, [4096])
        self.assertEqual(devs[0].aio_block_size, 4096)

    def test_legacy_record_restarted_without_block_size(self):
        rpc = _FakeRpc(logical=512)
        snode = _snode(devices=[_record(aio_block_size=0)])
        devs = utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [0])
        self.assertEqual(devs[0].aio_block_size, 0)
        self.assertNotIn("aio_block_size", snode.lblk_devices[0])

    def test_4096_record_restarted_at_4096(self):
        rpc = _FakeRpc(logical=512)
        snode = _snode(devices=[_record(aio_block_size=4096)])
        # Whatever the inventory says now, the recorded size wins.
        utils.addAioDevices(rpc, snode, [_entry(size=100 * GIB + 512)])
        self.assertEqual(rpc.creates, [4096])

    def test_512_record_restarted_at_512(self):
        rpc = _FakeRpc(logical=512)
        snode = _snode(devices=[_record(aio_block_size=512)])
        utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [512])

    def test_size_dropped_from_record_recovered_from_lblk_entry(self):
        # An older control-plane service rewrote the node and dropped
        # NVMeDevice.aio_block_size; the lblk_devices dict kept it.
        rpc = _FakeRpc(logical=512)
        snode = _snode(devices=[_record(aio_block_size=0)],
                       lblk_devices=[{"name": "sdb", "serial": "S1", "aio_block_size": 4096}])
        devs = utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [4096])
        self.assertEqual(devs[0].aio_block_size, 4096)

    def test_journal_device_record_used(self):
        jm = JMDevice()
        jm.serial_number = "S1"
        jm.device_data_dict = {"serial_number": "S1", "aio_block_size": 4096}
        rpc = _FakeRpc(logical=512)
        utils.addAioDevices(rpc, _snode(jm_device=jm), [_entry(size=100 * GIB + 512)])
        self.assertEqual(rpc.creates, [4096])

    def test_legacy_journal_device_record_without_block_size(self):
        jm = JMDevice()
        jm.serial_number = "S1"
        jm.device_data_dict = {"serial_number": "S1"}
        rpc = _FakeRpc(logical=512)
        utils.addAioDevices(rpc, _snode(jm_device=jm), [_entry()])
        self.assertEqual(rpc.creates, [0])

    def test_agent_without_field_new_device(self):
        rpc = _FakeRpc(logical=512)
        devs = utils.addAioDevices(rpc, _snode(), [_entry(logical=None, physical=None)])
        self.assertEqual(rpc.creates, [4096])
        self.assertEqual(devs[0].aio_block_size, 4096)

    def test_new_device_refused_4096_retried_without(self):
        rpc = _FakeRpc(logical=8192)
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_entry(logical=None, physical=None)])
        self.assertEqual(rpc.creates, [4096, 0])
        self.assertEqual(devs[0].aio_block_size, 8192)
        self.assertEqual(snode.lblk_devices[0]["aio_block_size"], 8192)

    def test_recorded_device_refused_is_not_retried(self):
        rpc = _FakeRpc(logical=8192)
        snode = _snode(devices=[_record(aio_block_size=4096)])
        with self.assertRaisesRegex(Exception, "bdev_aio_create failed"):
            utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [4096])

    def test_spdk_ignoring_block_size_refused(self):
        rpc = _FakeRpc(logical=512, ignore_block_size=True)
        snode = _snode(devices=[_record(aio_block_size=4096)])
        with self.assertRaisesRegex(Exception, "expected 4096"):
            utils.addAioDevices(rpc, snode, [_entry()])

    def test_existing_bdev_of_other_size_refused(self):
        name = utils.aio_bdev_name_for_serial("S1")
        rpc = _FakeRpc(existing={name: {"name": name, "block_size": 512, "num_blocks": 10}})
        snode = _snode(devices=[_record(aio_block_size=4096)])
        with self.assertRaisesRegex(Exception, "expected 4096"):
            utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates, [])

    def test_existing_bdev_of_new_device_recorded_at_its_size(self):
        name = utils.aio_bdev_name_for_serial("S1")
        rpc = _FakeRpc(existing={name: {"name": name, "block_size": 512, "num_blocks": 10}})
        devs = utils.addAioDevices(rpc, _snode(), [_entry()])
        self.assertEqual(rpc.creates, [])
        self.assertEqual(devs[0].aio_block_size, 512)


class TestResolveCarriesBlockSize(unittest.TestCase):

    def _live(self, serial):
        return {"name": "sdx", "device_path": "/dev/sdx", "serial": serial,
                "by_id_path": "", "size": 100 * GIB, "numa_node": 0,
                "logical_block_size": 512, "physical_block_size": 4096,
                "type": "disk"}

    def test_fields_carried_for_same_device(self):
        configured = [{"name": "sdb", "serial": "S1", "size": 1, "aio_block_size": 4096}]
        resolved, _ = utils.resolve_lblk_entries(configured, [self._live("S1")])
        self.assertEqual(resolved[0]["logical_block_size"], 512)
        self.assertEqual(resolved[0]["physical_block_size"], 4096)
        self.assertEqual(resolved[0]["aio_block_size"], 4096)

    def test_recorded_size_not_carried_to_a_different_device(self):
        configured = [{"name": "sdx", "serial": "S1", "size": 1, "aio_block_size": 512}]
        resolved, _ = utils.resolve_lblk_entries(configured, [self._live("OTHER")])
        self.assertNotIn("aio_block_size", resolved[0])


class TestReconcileRestoresBlockSize(unittest.TestCase):

    def test_dropped_size_restored(self):
        db_dev = _record(aio_block_size=0)
        found = _record(aio_block_size=4096)
        snode = _snode(devices=[db_dev])
        _reconcile_discovered_devices(snode, [found])
        self.assertEqual(db_dev.aio_block_size, 4096)

    def test_recorded_size_never_overwritten(self):
        db_dev = _record(aio_block_size=512)
        found = _record(aio_block_size=4096)
        _reconcile_discovered_devices(_snode(devices=[db_dev]), [found])
        self.assertEqual(db_dev.aio_block_size, 512)


class TestDeviceControllerBlockSize(unittest.TestCase):

    def _restart(self, aio_block_size, lblk_devices=None):
        device = _record(aio_block_size=aio_block_size, status=NVMeDevice.STATUS_REMOVED)
        device.node_id = "node-1"
        device.cluster_id = "cluster-1"
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        device.nvmf_nqn = ""
        device.alceml_bdev = ""
        snode = MagicMock()
        snode.cluster_id = "cluster-1"
        snode.nvme_devices = [device]
        snode.jm_device = None
        snode.lblk_devices = lblk_devices or []

        rpc = MagicMock()
        state = {"present": False}
        rpc.bdev_get.side_effect = (
            lambda name: {"name": name, "block_size": state.get("bs", 512)}
            if state["present"] else None)

        def _aio_create(name, filename, block_size=0):
            state["present"] = True
            state["bs"] = block_size or 512
            return name

        rpc.bdev_aio_create.side_effect = _aio_create
        snode.rpc_client.return_value = rpc
        snode.client.return_value.get_blockdevices.return_value = ([], None)

        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        db.get_storage_node_by_id.return_value = snode
        with patch.object(device_controller, "DBController", return_value=db), \
             patch.object(device_controller.tasks_controller,
                          "get_active_dev_restart_task", return_value=None), \
             patch.object(device_controller, "device_set_retries_exhausted"), \
             patch.object(device_controller, "device_set_unavailable"), \
             patch.object(device_controller, "_def_create_device_stack", return_value=True), \
             patch.object(device_controller, "device_set_io_error"), \
             patch.object(device_controller, "device_set_online"), \
             patch.object(device_controller, "device_events"):
            result = device_controller.restart_device(device.uuid)
        return result, rpc

    def test_restart_legacy_device_without_block_size(self):
        result, rpc = self._restart(0)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with("aio_S1", "/dev/disk/by-id/wwn-1")

    def test_restart_4096_device_at_4096(self):
        result, rpc = self._restart(4096)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)

    def test_restart_uses_lblk_entry_when_record_dropped_it(self):
        result, rpc = self._restart(0, lblk_devices=[{"serial": "S1", "aio_block_size": 4096}])
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)

    def test_new_device_from_failed_keeps_block_size(self):
        device = _record(aio_block_size=4096, status=NVMeDevice.STATUS_FAILED_AND_MIGRATED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        node = MagicMock()
        node.get_id.return_value = "node-1"
        node.nvme_devices = [device]
        node.lblk_devices = []
        rpc = MagicMock()
        state = {"present": False}
        rpc.bdev_get.side_effect = (
            lambda name: {"name": name, "block_size": 4096} if state["present"] else None)

        def _aio_create(name, filename, block_size=0):
            state["present"] = True
            return name

        rpc.bdev_aio_create.side_effect = _aio_create
        node.rpc_client.return_value = rpc
        node.client.return_value.get_blockdevices.return_value = ([], None)
        db = MagicMock()
        db.get_storage_node_by_device_id.return_value = node
        with patch.object(device_controller, "DBController", return_value=db):
            self.assertTrue(device_controller.new_device_from_failed(device.uuid))
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)


if __name__ == "__main__":
    unittest.main()
