"""Unit tests for the I/O engine of lblk base bdevs.

SPDK's bdev_aio submits with RWF_NOWAIT, which device-mapper honours only
when every target does; dm-multipath does not, so a multipath map cannot be
served by bdev_aio at all. Device-mapper nodes are served by bdev_uring
instead, everything else keeps bdev_aio. The bdev name (aio_<serial>) and
NVMeDevice.bdev_type "aio" are the same for both engines.

Covered:
  - utils.lblk_io_engine_for: dm-* kernel name, lsblk TYPE mpath/lvm/crypt/dm,
    a kpartx partition of a multipath map, /dev/mapper and by-id dm-* paths
    (older agent), sd/nvme disks and partitions and md arrays -> aio.
  - utils.resolve_lblk_entries carries kname/type, the derived engine and the
    recorded engine (same device only).
  - utils.addAioDevices: dm device -> bdev_uring_create with the block size,
    recorded on the device and the node's lblk_devices entry; sd/md -> aio;
    recorded uring kept; recorded aio on a dm node -> uring; older agent;
    an existing bdev's engine is kept.
  - delete / rescan route by engine; device growth rescans a uring device
    with bdev_uring_rescan.
  - storage_node_ops._reconcile_discovered_devices takes the engine over.
  - device_controller.restart_device / new_device_from_failed create the
    bdev with the device's engine.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import _reconcile_discovered_devices

GIB = 1 << 30
AIO = constants.LBLK_IO_ENGINE_AIO
URING = constants.LBLK_IO_ENGINE_URING


def _entry(serial="S1", name="sdb", current_path="/dev/sdb", by_id=None, **extra):
    e = {"name": name, "serial": serial,
         "by_id": by_id if by_id is not None else f"/dev/disk/by-id/wwn-{serial}",
         "size": 100 * GIB, "numa": 0, "model": "M", "current_path": current_path,
         "has_partitions": False, "logical_block_size": 512,
         "physical_block_size": 4096}
    e.update(extra)
    return e


def _mpath_entry(serial="S1", **extra):
    return _entry(serial=serial, name="mpatha", current_path="/dev/mpatha",
                  by_id=f"/dev/disk/by-id/dm-uuid-mpath-{serial}",
                  kname="dm-0", dev_type="mpath", **extra)


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


def _record(serial="S1", engine=AIO, aio_block_size=4096,
            status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.uuid = f"dev-{serial}"
    d.serial_number = serial
    d.bdev_type = "aio"
    d.nvme_bdev = utils.aio_bdev_name_for_serial(serial)
    d.aio_block_size = aio_block_size
    d.lblk_io_engine = engine
    d.status = status
    return d


class _FakeRpc:
    """SPDK fake with both base-bdev engines; records every create."""

    def __init__(self, existing=None):
        self.host = "test-host"
        self.bdevs = dict(existing or {})
        self.creates = []  # (engine, name, filename, block_size)

    def bdev_get(self, name):
        return self.bdevs.get(name)

    def _create(self, engine, name, filename, block_size):
        self.creates.append((engine, name, filename, block_size))
        bs = block_size or 512
        self.bdevs[name] = {"name": name, "block_size": bs,
                            "num_blocks": 100 * GIB // bs,
                            "driver_specific": {engine: {"filename": filename}}}
        return name

    def bdev_aio_create(self, name, filename, block_size=0):
        return self._create(AIO, name, filename, block_size)

    def bdev_uring_create(self, name, filename, block_size=0):
        return self._create(URING, name, filename, block_size)

    def bdev_examine(self, name):
        return True

    def bdev_wait_for_examine(self):
        return True

    def bdev_set_qd_sampling_period(self, name, period):
        return True


class TestEngineRule(unittest.TestCase):

    def test_dm_kernel_name_is_uring(self):
        self.assertEqual(utils.lblk_io_engine_for({"name": "mpatha", "kname": "dm-3"}), URING)

    def test_dm_types_are_uring(self):
        for t in ("mpath", "lvm", "crypt", "dm"):
            with self.subTest(t=t):
                self.assertEqual(utils.lblk_io_engine_for({"name": "x", "type": t}), URING)

    def test_kpartx_partition_of_multipath_is_uring(self):
        self.assertEqual(utils.lblk_io_engine_for(
            {"name": "mpatha1", "kname": "dm-4", "type": "part"}), URING)

    def test_mapper_and_dm_paths_are_uring(self):
        for path in ("/dev/mapper/mpatha", "/dev/dm-0",
                     "/dev/disk/by-id/dm-uuid-mpath-3600a", "/dev/disk/by-id/dm-name-mpatha"):
            with self.subTest(path=path):
                self.assertEqual(utils.lblk_io_engine_for(
                    {"name": "x", "device_path": path}), URING)

    def test_plain_devices_are_aio(self):
        for dev in ({"name": "sdb", "kname": "sdb", "type": "disk",
                     "device_path": "/dev/sdb", "by_id_path": "/dev/disk/by-id/wwn-0x5"},
                    {"name": "nvme0n1p2", "kname": "nvme0n1p2", "type": "part"},
                    {"name": "sdc1", "type": "part", "device_path": "/dev/sdc1"},
                    {"name": "md0", "kname": "md0", "type": "raid5"},
                    {"name": "md127", "type": "raid1", "device_path": "/dev/md127"}):
            with self.subTest(dev=dev["name"]):
                self.assertEqual(utils.lblk_io_engine_for(dev), AIO)

    def test_older_agent_without_fields_is_aio(self):
        self.assertEqual(utils.lblk_io_engine_for(
            {"name": "sdb", "device_path": "/dev/sdb", "by_id_path": ""}), AIO)

    def test_choose_prefers_recorded_unless_rule_says_uring(self):
        self.assertEqual(utils.choose_lblk_io_engine(None, AIO), AIO)
        self.assertEqual(utils.choose_lblk_io_engine(URING, AIO), URING)
        self.assertEqual(utils.choose_lblk_io_engine(AIO, URING), URING)
        self.assertEqual(utils.choose_lblk_io_engine(AIO, AIO), AIO)


class TestResolveCarriesEngine(unittest.TestCase):

    def _live(self, serial, **extra):
        d = {"name": "mpatha", "kname": "dm-0", "type": "mpath",
             "device_path": "/dev/mpatha", "serial": serial, "by_id_path": "",
             "size": 100 * GIB, "numa_node": 0}
        d.update(extra)
        return d

    def test_dm_device_resolves_to_uring(self):
        resolved, _ = utils.resolve_lblk_entries(
            [{"name": "mpatha", "serial": "S1", "size": 1}], [self._live("S1")])
        self.assertEqual(resolved[0]["kname"], "dm-0")
        self.assertEqual(resolved[0]["dev_type"], "mpath")
        self.assertEqual(resolved[0]["io_engine"], URING)

    def test_disk_resolves_to_aio(self):
        live = self._live("S1", name="sdb", kname="sdb", type="disk", device_path="/dev/sdb")
        resolved, _ = utils.resolve_lblk_entries(
            [{"name": "sdb", "serial": "S1", "size": 1}], [live])
        self.assertEqual(resolved[0]["io_engine"], AIO)

    def test_older_agent_mapper_path(self):
        live = {"name": "mpatha", "device_path": "/dev/mapper/mpatha", "serial": "S1",
                "by_id_path": "", "size": 100 * GIB, "numa_node": 0}
        resolved, _ = utils.resolve_lblk_entries(
            [{"name": "mpatha", "serial": "S1", "size": 1}], [live])
        self.assertEqual(resolved[0]["io_engine"], URING)

    def test_recorded_engine_carried_for_same_device_only(self):
        live = self._live("S1", name="sdb", kname="sdb", type="disk", device_path="/dev/sdb")
        resolved, _ = utils.resolve_lblk_entries(
            [{"name": "sdb", "serial": "S1", "size": 1, "lblk_io_engine": URING}], [live])
        self.assertEqual(resolved[0]["lblk_io_engine"], URING)
        live = dict(live, serial="OTHER")
        resolved, _ = utils.resolve_lblk_entries(
            [{"name": "sdb", "serial": "S1", "size": 1, "lblk_io_engine": URING}], [live])
        self.assertNotIn("lblk_io_engine", resolved[0])


class TestAddAioDevicesEngine(unittest.TestCase):

    def test_new_multipath_device_created_with_uring(self):
        rpc = _FakeRpc()
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_mpath_entry(io_engine=URING)])
        self.assertEqual(rpc.creates, [(URING, "aio_S1", "/dev/disk/by-id/dm-uuid-mpath-S1", 4096)])
        self.assertEqual(devs[0].lblk_io_engine, URING)
        self.assertEqual(devs[0].nvme_bdev, "aio_S1")
        self.assertEqual(devs[0].bdev_type, "aio")
        self.assertEqual(devs[0].aio_block_size, 4096)
        self.assertEqual(snode.lblk_devices[0]["lblk_io_engine"], URING)

    def test_new_disk_created_with_aio(self):
        rpc = _FakeRpc()
        snode = _snode()
        devs = utils.addAioDevices(rpc, snode, [_entry(io_engine=AIO)])
        self.assertEqual(rpc.creates[0][0], AIO)
        self.assertEqual(devs[0].lblk_io_engine, AIO)
        self.assertEqual(snode.lblk_devices[0]["lblk_io_engine"], AIO)

    def test_md_array_created_with_aio(self):
        rpc = _FakeRpc()
        entry = _entry(name="md0", current_path="/dev/md0", by_id="/dev/disk/by-id/md-uuid-1",
                       kname="md0", dev_type="raid5")
        devs = utils.addAioDevices(rpc, _snode(), [entry])
        self.assertEqual(rpc.creates[0][0], AIO)
        self.assertEqual(devs[0].lblk_io_engine, AIO)

    def test_entry_without_derived_engine_uses_rule(self):
        rpc = _FakeRpc()
        entry = _mpath_entry()
        utils.addAioDevices(rpc, _snode(), [entry])
        self.assertEqual(rpc.creates[0][0], URING)

    def test_older_agent_disk_without_fields_is_aio(self):
        rpc = _FakeRpc()
        entry = _entry()
        entry.pop("logical_block_size")
        utils.addAioDevices(rpc, _snode(), [entry])
        self.assertEqual(rpc.creates[0][0], AIO)

    def test_recorded_uring_kept(self):
        # e.g. an older agent that reports the map under a by-id wwn link only
        rpc = _FakeRpc()
        snode = _snode(devices=[_record(engine=URING)])
        devs = utils.addAioDevices(rpc, snode, [_entry(io_engine=AIO)])
        self.assertEqual(rpc.creates, [(URING, "aio_S1", "/dev/disk/by-id/wwn-S1", 4096)])
        self.assertEqual(devs[0].lblk_io_engine, URING)

    def test_recorded_uring_on_lblk_entry_kept(self):
        rpc = _FakeRpc()
        snode = _snode(devices=[_record(engine=AIO)],
                       lblk_devices=[{"name": "sdb", "serial": "S1", "lblk_io_engine": URING}])
        utils.addAioDevices(rpc, snode, [_entry()])
        self.assertEqual(rpc.creates[0][0], URING)

    def test_recorded_aio_on_dm_node_switches_to_uring(self):
        rpc = _FakeRpc()
        snode = _snode(devices=[_record(engine=AIO, aio_block_size=512)])
        with self.assertLogs(utils.logger, level="WARNING") as logs:
            devs = utils.addAioDevices(rpc, snode, [_mpath_entry(io_engine=URING)])
        self.assertEqual(rpc.creates[0][0], URING)
        self.assertEqual(rpc.creates[0][3], 512)  # the recorded block size is kept
        self.assertEqual(devs[0].lblk_io_engine, URING)
        self.assertTrue(any("using uring" in m for m in logs.output))

    def test_journal_record_engine_used(self):
        jm = JMDevice()
        jm.serial_number = "S1"
        jm.device_data_dict = {"serial_number": "S1", "lblk_io_engine": URING,
                               "aio_block_size": 4096}
        rpc = _FakeRpc()
        utils.addAioDevices(rpc, _snode(jm_device=jm), [_entry()])
        self.assertEqual(rpc.creates[0][0], URING)

    def test_existing_bdev_engine_recorded(self):
        existing = {"aio_S1": {"name": "aio_S1", "block_size": 4096,
                               "num_blocks": 100 * GIB // 4096,
                               "driver_specific": {URING: {"filename": "/dev/dm-0"}}}}
        rpc = _FakeRpc(existing=existing)
        devs = utils.addAioDevices(rpc, _snode(devices=[_record(engine=URING)]), [_entry()])
        self.assertEqual(rpc.creates, [])
        self.assertEqual(devs[0].lblk_io_engine, URING)


class TestDeleteRescanRouting(unittest.TestCase):

    def test_delete_routes_by_engine(self):
        rpc = MagicMock()
        utils.delete_lblk_bdev(rpc, "aio_S1", URING)
        rpc.bdev_uring_delete.assert_called_once_with("aio_S1")
        rpc.bdev_aio_delete.assert_not_called()
        rpc = MagicMock()
        utils.delete_lblk_bdev(rpc, "aio_S1", AIO)
        rpc.bdev_aio_delete.assert_called_once_with("aio_S1")
        rpc.bdev_uring_delete.assert_not_called()

    def test_rescan_routes_by_engine(self):
        rpc = MagicMock()
        utils.rescan_lblk_bdev(rpc, "aio_S1", URING)
        rpc.bdev_uring_rescan.assert_called_once_with("aio_S1")
        rpc.bdev_aio_rescan.assert_not_called()
        rpc = MagicMock()
        utils.rescan_lblk_bdev(rpc, "aio_S1")
        rpc.bdev_aio_rescan.assert_called_once_with("aio_S1")

    def test_create_routes_by_engine_with_block_size(self):
        rpc = _FakeRpc()
        self.assertEqual(utils.create_aio_bdev(rpc, "aio_S1", "/dev/dm-0", 4096,
                                               io_engine=URING), 4096)
        self.assertEqual(rpc.creates, [(URING, "aio_S1", "/dev/dm-0", 4096)])

    def test_reopen_alceml_rescans_uring_device(self):
        dev = _record(engine=URING)
        dev.alceml_bdev = "alceml_1"
        snode = _snode(devices=[dev])
        rpc = MagicMock()
        with patch.object(StorageNode, "rpc_client", return_value=rpc):
            device_controller._reopen_alceml(snode, dev)
        rpc.bdev_uring_rescan.assert_called_once_with("aio_S1")
        rpc.bdev_aio_rescan.assert_not_called()

    def test_reopen_alceml_rescans_aio_device(self):
        dev = _record(engine=AIO)
        dev.alceml_bdev = "alceml_1"
        snode = _snode(devices=[dev])
        rpc = MagicMock()
        with patch.object(StorageNode, "rpc_client", return_value=rpc):
            device_controller._reopen_alceml(snode, dev)
        rpc.bdev_aio_rescan.assert_called_once_with("aio_S1")
        rpc.bdev_uring_rescan.assert_not_called()


class TestReconcileTakesEngine(unittest.TestCase):

    def test_engine_taken_over(self):
        db_dev = _record(engine=AIO)
        found = _record(engine=URING)
        _reconcile_discovered_devices(_snode(devices=[db_dev]), [found])
        self.assertEqual(db_dev.lblk_io_engine, URING)


class TestDeviceControllerEngine(unittest.TestCase):

    def _restart(self, device, inventory=(), lblk_devices=None):
        device.node_id = "node-1"
        device.cluster_id = "cluster-1"
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
            lambda name: {"name": name, "block_size": 4096} if state["present"] else None)

        def _create(name, filename, block_size=0):
            state["present"] = True
            return name

        rpc.bdev_aio_create.side_effect = _create
        rpc.bdev_uring_create.side_effect = _create
        snode.rpc_client.return_value = rpc
        snode.client.return_value.get_blockdevices.return_value = (list(inventory), None)

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

    def test_restart_uring_device(self):
        device = _record(engine=URING, status=NVMeDevice.STATUS_REMOVED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        result, rpc = self._restart(device)
        self.assertTrue(result)
        rpc.bdev_uring_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)
        rpc.bdev_aio_create.assert_not_called()

    def test_restart_aio_device(self):
        device = _record(engine=AIO, status=NVMeDevice.STATUS_REMOVED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        result, rpc = self._restart(device)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)
        rpc.bdev_uring_create.assert_not_called()

    def test_restart_recorded_aio_dm_device_gets_uring(self):
        device = _record(engine=AIO, status=NVMeDevice.STATUS_REMOVED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        inventory = [{"serial": "S1", "name": "mpatha", "kname": "dm-0", "type": "mpath",
                      "device_path": "/dev/mpatha",
                      "by_id_path": "/dev/disk/by-id/dm-uuid-mpath-S1"}]
        result, rpc = self._restart(device, inventory=inventory)
        self.assertTrue(result)
        rpc.bdev_uring_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/dm-uuid-mpath-S1", block_size=4096)
        self.assertEqual(device.lblk_io_engine, URING)

    def test_restart_engine_from_lblk_entry(self):
        device = _record(engine=AIO, status=NVMeDevice.STATUS_REMOVED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        result, rpc = self._restart(
            device, lblk_devices=[{"serial": "S1", "lblk_io_engine": URING}])
        self.assertTrue(result)
        rpc.bdev_uring_create.assert_called_once()

    def test_new_device_from_failed_keeps_engine(self):
        device = _record(engine=URING, status=NVMeDevice.STATUS_FAILED_AND_MIGRATED)
        device.by_id_path = "/dev/disk/by-id/wwn-1"
        node = MagicMock()
        node.get_id.return_value = "node-1"
        node.nvme_devices = [device]
        node.lblk_devices = []
        rpc = MagicMock()
        state = {"present": False}
        rpc.bdev_get.side_effect = (
            lambda name: {"name": name, "block_size": 4096} if state["present"] else None)

        def _create(name, filename, block_size=0):
            state["present"] = True
            return name

        rpc.bdev_uring_create.side_effect = _create
        node.rpc_client.return_value = rpc
        node.client.return_value.get_blockdevices.return_value = ([], None)
        db = MagicMock()
        db.get_storage_node_by_device_id.return_value = node
        with patch.object(device_controller, "DBController", return_value=db):
            self.assertTrue(device_controller.new_device_from_failed(device.uuid))
        rpc.bdev_uring_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1", block_size=4096)
        rpc.bdev_aio_create.assert_not_called()


class TestNoDirectAioRpcCalls(unittest.TestCase):
    """Every bdev_aio_create/delete/rescan goes through the engine-aware
    helpers in utils."""

    def test_no_direct_calls_outside_helpers(self):
        import pathlib
        import re
        root = pathlib.Path(utils.__file__).resolve().parents[2]
        pattern = re.compile(r"\.bdev_aio_(create|delete|rescan)\(")
        offenders = []
        for sub in ("simplyblock_core", "simplyblock_web", "simplyblock_cli"):
            for path in (root / sub).rglob("*.py"):
                if path.name == "rpc_client.py":
                    continue
                for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                    if pattern.search(line):
                        offenders.append(f"{path.relative_to(root)}:{n}")
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
