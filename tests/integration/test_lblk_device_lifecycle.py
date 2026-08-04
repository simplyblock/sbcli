# coding=utf-8
"""Integration tests for the lblk (Linux block device / SPDK AIO) device
mode against a real FoundationDB (testcontainer via tests/integration/
conftest.py).

What runs REAL here: the FDB persistence layer (model round-trips), the
device_controller state machine (device_set_state flap accounting, forced
FAILED, device_remove) and the device_monitor watchdog logic. What is
faked: SPDK RPC (per-call mocks), the node agent (blockdevices inventory)
and the distr/event fan-out (patched at the consuming module).

Scenarios:
  1. Model round-trip — cluster.device_mode, node.lblk_devices and the
     per-device aio identity fields survive FDB serialization.
  2. Restart identity contract — resolve_lblk_entries + addAioDevices
     against a renamed-device inventory produce records whose serials match
     the DB reconcile keys (serial-first restart survival).
  3. Watchdog stall — real device_set_unavailable/io_error transitions in
     FDB after the hung-IO threshold, with a countable flap.
  4. Flap limit — repeated LOCAL_FAILURE transitions force STATUS_FAILED
     and queue failed-device migration.
  5. Disappearance — the presence sweep drives the real device_remove to
     STATUS_REMOVED.
  6. reset_storage_device — aio liveness probe against a real DB record.
"""

import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import device_monitor


CLUSTER_ID = "11111111-1111-1111-1111-111111111111"


def _seed_cluster(db, device_mode="lblk", status=Cluster.STATUS_ACTIVE):
    cluster = Cluster()
    cluster.uuid = CLUSTER_ID
    cluster.status = status
    cluster.device_mode = device_mode
    cluster.ha_type = "ha"
    cluster.write_to_db(db.kv_store)
    return cluster


def _aio_device(serial="S1", name="sdb", status=NVMeDevice.STATUS_ONLINE):
    dev = NVMeDevice()
    dev.uuid = str(uuid_mod.uuid4())
    dev.cluster_id = CLUSTER_ID
    dev.status = status
    dev.bdev_type = "aio"
    dev.serial_number = serial
    dev.device_name = name
    dev.device_path = f"/dev/{name}"
    dev.by_id_path = f"/dev/disk/by-id/wwn-{serial}"
    dev.nvme_bdev = utils.aio_bdev_name_for_serial(serial)
    dev.size = 100 << 30
    dev.cluster_device_order = 0
    return dev


def _seed_node(db, devices, node_id=None, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = node_id or str(uuid_mod.uuid4())
    node.cluster_id = CLUSTER_ID
    node.status = status
    node.mgmt_ip = "10.0.0.1"
    node.api_endpoint = "10.0.0.1:5000"
    node.lblk_devices = [
        {"name": d.device_name, "serial": d.serial_number,
         "by_id": d.by_id_path, "size": d.size, "numa": 0}
        for d in devices
    ]
    for d in devices:
        d.node_id = node.uuid
    node.nvme_devices = devices
    node.write_to_db(db.kv_store)
    return node


@pytest.fixture()
def db():
    return DBController()


# ---------------------------------------------------------------------------
# 1. Model round-trips
# ---------------------------------------------------------------------------

class TestModelRoundTrip:

    def test_cluster_device_mode_persists(self, db):
        _seed_cluster(db, device_mode="lblk")
        read = db.get_cluster_by_id(CLUSTER_ID)
        assert read.device_mode == "lblk"

    def test_cluster_device_mode_defaults_nvme(self, db):
        cluster = Cluster()
        cluster.uuid = CLUSTER_ID
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.write_to_db(db.kv_store)
        assert db.get_cluster_by_id(CLUSTER_ID).device_mode == "nvme"

    def test_node_and_device_fields_persist(self, db):
        _seed_cluster(db)
        dev = _aio_device(serial="S3Z8NX0M600123", name="sdb")
        node = _seed_node(db, [dev])

        read_node = db.get_storage_node_by_id(node.get_id())
        assert read_node.lblk_devices == [{
            "name": "sdb", "serial": "S3Z8NX0M600123",
            "by_id": "/dev/disk/by-id/wwn-S3Z8NX0M600123",
            "size": 100 << 30, "numa": 0,
        }]
        read_dev = read_node.nvme_devices[0]
        assert read_dev.bdev_type == "aio"
        assert read_dev.device_path == "/dev/sdb"
        assert read_dev.by_id_path == "/dev/disk/by-id/wwn-S3Z8NX0M600123"
        assert read_dev.nvme_bdev == utils.aio_bdev_name_for_serial("S3Z8NX0M600123")
        assert read_dev.pcie_address == ""
        assert read_dev.nvme_controller == ""

    def test_nvme_device_records_unaffected(self, db):
        _seed_cluster(db, device_mode="nvme")
        dev = NVMeDevice()
        dev.uuid = str(uuid_mod.uuid4())
        dev.cluster_id = CLUSTER_ID
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.pcie_address = "0000:00:1e.0"
        dev.nvme_controller = "nvme_1e"
        node = _seed_node(db, [dev])
        read_dev = db.get_storage_node_by_id(node.get_id()).nvme_devices[0]
        assert read_dev.bdev_type == "nvme"
        assert read_dev.pcie_address == "0000:00:1e.0"


# ---------------------------------------------------------------------------
# 2. Restart identity contract (serial-first over renamed devices)
# ---------------------------------------------------------------------------

class TestRestartIdentityContract:

    def test_renamed_devices_resolve_to_same_reconcile_keys(self, db):
        _seed_cluster(db)
        d1, d2 = _aio_device("S1", "sdb"), _aio_device("S2", "sdc")
        node = _seed_node(db, [d1, d2])
        node = db.get_storage_node_by_id(node.get_id())

        # Reboot renamed sdb->sdd and sdc->sdb (a swap-adjacent shuffle).
        live_inventory = [
            {"name": "sdd", "device_path": "/dev/sdd", "serial": "S1",
             "by_id_path": "/dev/disk/by-id/wwn-S1", "size": 100 << 30,
             "numa_node": 0, "model": "M"},
            {"name": "sdb", "device_path": "/dev/sdb", "serial": "S2",
             "by_id_path": "/dev/disk/by-id/wwn-S2", "size": 100 << 30,
             "numa_node": 0, "model": "M"},
        ]
        resolved, missing = utils.resolve_lblk_entries(node.lblk_devices, live_inventory)
        assert missing == []

        rpc = MagicMock()
        rpc.host = "t"
        rpc.get_bdevs.return_value = None
        created = {}

        def _create(name, filename, block_size=0):
            created[name] = filename
            rpc.get_bdevs.return_value = [
                {"name": name, "block_size": 4096, "num_blocks": 100}]
            return name

        rpc.bdev_aio_create.side_effect = _create
        discovered = utils.addAioDevices(rpc, node, resolved)

        # The reconcile at restart keys on serial_number: every discovered
        # serial must match a DB record, with the CURRENT (renamed) path.
        db_by_serial = {d.serial_number: d for d in node.nvme_devices}
        for found in discovered:
            assert found.serial_number in db_by_serial
        by_serial = {d.serial_number: d for d in discovered}
        assert by_serial["S1"].device_name == "sdd"
        assert by_serial["S2"].device_name == "sdb"
        # Stable bdev names: identical to what add-node created.
        assert by_serial["S1"].nvme_bdev == db_by_serial["S1"].nvme_bdev
        # AIO filename used the stable by-id path, not the volatile name.
        assert created[by_serial["S1"].nvme_bdev] == "/dev/disk/by-id/wwn-S1"

    def test_missing_device_flagged_for_removal_semantics(self, db):
        _seed_cluster(db)
        node = _seed_node(db, [_aio_device("S1", "sdb"), _aio_device("S2", "sdc")])
        node = db.get_storage_node_by_id(node.get_id())
        live_inventory = [
            {"name": "sdb", "device_path": "/dev/sdb", "serial": "S1",
             "by_id_path": "", "size": 1, "numa_node": 0, "model": "M"},
        ]
        resolved, missing = utils.resolve_lblk_entries(node.lblk_devices, live_inventory)
        assert [e["serial"] for e in resolved] == ["S1"]
        assert [e["serial"] for e in missing] == ["S2"]


# ---------------------------------------------------------------------------
# 3-5. Watchdog + real device_controller state machine
# ---------------------------------------------------------------------------

def _patched_fanout():
    """Patch the SPDK/event fan-out that device_set_state / device_remove
    perform, leaving the FDB state machine real."""
    return [
        patch.object(device_controller, "distr_controller", MagicMock()),
        patch.object(device_controller, "device_events", MagicMock()),
        patch.object(StorageNode, "rpc_client",
                     lambda self, **kw: MagicMock()),
    ]


class TestWatchdogAgainstRealStateMachine:

    def setup_method(self, _method):
        device_monitor._aio_progress.clear()
        device_monitor._aio_absent.clear()

    def test_stall_marks_device_unavailable_with_flap(self, db):
        _seed_cluster(db)
        dev = _aio_device("S1", "sdb")
        node = _seed_node(db, [dev])
        node = db.get_storage_node_by_id(node.get_id())

        stall_rpc = MagicMock()
        stall_rpc.get_lvol_stats.return_value = {"bdevs": [{
            "num_read_ops": 100, "num_write_ops": 0, "num_unmap_ops": 0,
            "queue_depth": 4}]}
        inventory = [{"name": "sdb", "serial": "S1"}]
        agent = MagicMock()
        agent.get_blockdevices.return_value = (inventory, None)

        patches = _patched_fanout() + [
            patch.object(StorageNode, "client", lambda self, **kw: agent),
        ]
        for p in patches:
            p.start()
        try:
            with patch.object(StorageNode, "rpc_client",
                              lambda self, **kw: stall_rpc):
                for _ in range(constants.AIO_HUNG_IO_STALL_POLLS + 1):
                    node = db.get_storage_node_by_id(node.get_id())
                    device_monitor._sweep_aio_devices(node)
        finally:
            for p in patches:
                p.stop()

        read = db.get_storage_device_by_id(dev.get_id())
        assert read.status == NVMeDevice.STATUS_UNAVAILABLE
        assert read.io_error is True
        assert read.flap_count == 1  # ONLINE -> UNAVAILABLE, LOCAL_FAILURE, node ONLINE

    def test_flap_limit_forces_failed_and_queues_migration(self, db):
        _seed_cluster(db)
        dev = _aio_device("S1", "sdb")
        _seed_node(db, [dev])

        patches = _patched_fanout() + [
            patch.object(device_controller, "DEVICE_FLAP_DEBOUNCE_SEC", 0.0),
            # re-online between flaps queues FN_DEV_MIG — irrelevant noise here
            patch.object(device_controller.tasks_controller,
                         "add_device_mig_task_for_node", return_value=None),
            patch.object(device_controller.tasks_controller,
                         "add_device_failed_mig_task"),
        ]
        started = [p.start() for p in patches]
        mig_task = started[-1]
        try:
            for _ in range(device_controller.DEVICE_FLAP_LIMIT + 1):
                device_controller.device_set_unavailable(
                    dev.get_id(), cause=device_controller.CAUSE_LOCAL_FAILURE)
                read = db.get_storage_device_by_id(dev.get_id())
                if read.status == NVMeDevice.STATUS_FAILED:
                    break
                device_controller.device_set_online(dev.get_id())
        finally:
            for p in patches:
                p.stop()

        read = db.get_storage_device_by_id(dev.get_id())
        assert read.status == NVMeDevice.STATUS_FAILED
        mig_task.assert_called_once_with(dev.get_id())

    def test_disappearance_drives_real_device_remove(self, db):
        _seed_cluster(db)
        dev = _aio_device("S1", "sdb")
        node = _seed_node(db, [dev])

        agent = MagicMock()
        agent.get_blockdevices.return_value = (
            [{"name": "other", "serial": "ZZZ"}], None)
        idle_rpc = MagicMock()
        idle_rpc.get_lvol_stats.return_value = {"bdevs": [{
            "num_read_ops": 0, "num_write_ops": 0, "num_unmap_ops": 0,
            "queue_depth": 0}]}

        patches = _patched_fanout() + [
            patch.object(StorageNode, "client", lambda self, **kw: agent),
        ]
        for p in patches:
            p.start()
        try:
            with patch.object(StorageNode, "rpc_client",
                              lambda self, **kw: idle_rpc):
                for _ in range(constants.AIO_DEVICE_ABSENT_POLLS):
                    fresh = db.get_storage_node_by_id(node.get_id())
                    device_monitor._sweep_aio_devices(fresh)
        finally:
            for p in patches:
                p.stop()

        read = db.get_storage_device_by_id(dev.get_id())
        assert read.status == NVMeDevice.STATUS_REMOVED


# ---------------------------------------------------------------------------
# 6. reset_storage_device against a real DB record
# ---------------------------------------------------------------------------

class TestResetAgainstDb:

    def test_reset_aio_liveness_probe_recovers_unavailable_device(self, db):
        _seed_cluster(db)
        dev = _aio_device("S1", "sdb", status=NVMeDevice.STATUS_UNAVAILABLE)
        _seed_node(db, [dev])

        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": dev.nvme_bdev}]

        patches = _patched_fanout()
        for p in patches:
            p.start()
        try:
            with patch.object(StorageNode, "rpc_client", lambda self, **kw: rpc), \
                 patch.object(device_controller.tasks_controller,
                              "get_active_dev_restart_task", return_value=None), \
                 patch.object(device_controller.tasks_controller,
                              "add_device_mig_task_for_node", return_value=None):
                assert device_controller.reset_storage_device(dev.get_id())
        finally:
            for p in patches:
                p.stop()

        read = db.get_storage_device_by_id(dev.get_id())
        assert read.status == NVMeDevice.STATUS_ONLINE
        assert read.io_error is False
        rpc.reset_device.assert_not_called()
