"""Integration tests for the online growth of lblk devices against a real
FoundationDB (testcontainer via tests/integration/conftest.py).

What runs REAL here: the FDB persistence of the grown device (size and the
frozen placement_size), the cluster-map build from the persisted records,
and device_controller.resize_device / grow_cluster_devices end to end. What
is faked: SPDK RPC, the node agent (rescan_block_device), and the event
fan-out.

Scenarios:
  1. A grown device persists its new size and keeps its placement size, and
     the cluster map built from the stored records carries the old weight.
  2. A second growth keeps the original placement size.
  3. grow_cluster_devices grows every data device by the same factor and
     reports the cluster as uniform; the journal device is left alone.
  4. A failed reopen leaves the stored size unchanged and the device
     unavailable.
  5. placement_size survives an FDB round-trip and defaults to 0.
"""

import uuid as uuid_mod
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import distr_controller, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

CLUSTER_ID = "22222222-2222-2222-2222-222222222222"
_GIB = 1 << 30


def _seed_cluster(db):
    cluster = Cluster()
    cluster.uuid = CLUSTER_ID
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.device_mode = "lblk"
    cluster.ha_type = "ha"
    cluster.write_to_db(db.kv_store)
    return cluster


def _aio_device(serial, order, size_gib=100, status=NVMeDevice.STATUS_ONLINE):
    dev = NVMeDevice()
    dev.uuid = str(uuid_mod.uuid4())
    dev.cluster_id = CLUSTER_ID
    dev.status = status
    dev.bdev_type = "aio"
    dev.serial_number = serial
    dev.device_path = f"/dev/{serial.lower()}"
    dev.by_id_path = f"/dev/disk/by-id/wwn-{serial}"
    dev.nvme_bdev = utils.aio_bdev_name_for_serial(serial)
    dev.alceml_bdev = f"alceml_{serial}"
    dev.size = size_gib * _GIB
    dev.cluster_device_order = order
    return dev


def _seed_node(db, devices):
    node = StorageNode()
    node.uuid = str(uuid_mod.uuid4())
    node.cluster_id = CLUSTER_ID
    node.status = StorageNode.STATUS_ONLINE
    node.mgmt_ip = "10.0.0.1"
    node.api_endpoint = "10.0.0.1:5000"
    for d in devices:
        d.node_id = node.uuid
    node.nvme_devices = devices
    node.write_to_db(db.kv_store)
    return node


def _agent(sizes_by_path):
    agent = MagicMock()
    agent.rescan_block_device.side_effect = lambda path: (
        {"device": path.rsplit("-", 1)[-1], "size": sizes_by_path[path], "rescanned": []}, None)
    return agent


def _run(agent, rpc, fn, *args):
    with patch.object(StorageNode, "client", lambda self, **kw: agent), \
         patch.object(StorageNode, "rpc_client", lambda self, **kw: rpc), \
         patch.object(device_controller, "device_events", MagicMock()), \
         patch.object(device_controller, "distr_controller", MagicMock()), \
         patch.object(device_controller.tasks_controller,
                      "get_active_dev_restart_task", return_value=None), \
         patch.object(device_controller.time, "sleep"):
        return fn(*args)


@pytest.fixture()
def db():
    return DBController()


class TestResizePersistence:

    def test_placement_size_round_trip(self, db):
        _seed_cluster(db)
        dev = _aio_device("S1", 0)
        _seed_node(db, [dev])
        read = db.get_storage_device_by_id(dev.get_id())
        assert read.placement_size == 0
        assert read.weight_size() == 100 * _GIB

    def test_grown_device_keeps_cluster_map_weight(self, db):
        _seed_cluster(db)
        a, b = _aio_device("S1", 0), _aio_device("S2", 1)
        node = _seed_node(db, [a, b])
        agent = _agent({a.by_id_path: 300 * _GIB})

        res = _run(agent, MagicMock(), device_controller.resize_device, a.get_id())

        assert res["resized"] is True
        assert res["uniform"] is False
        read = db.get_storage_device_by_id(a.get_id())
        assert read.size == 300 * _GIB
        assert read.placement_size == 100 * _GIB

        fresh = db.get_storage_node_by_id(node.get_id())
        cl_map = distr_controller.get_distr_cluster_map([fresh], fresh, "distr-1")
        weights = [i["weight"] for i in cl_map["map_prob"][0]["items"]]
        assert weights == [utils.convert_size(100 * _GIB, "GiB")] * 2

    def test_second_growth_keeps_original_placement(self, db):
        _seed_cluster(db)
        a = _aio_device("S1", 0)
        _seed_node(db, [a])
        sizes = {a.by_id_path: 200 * _GIB}
        agent = _agent(sizes)
        _run(agent, MagicMock(), device_controller.resize_device, a.get_id())
        sizes[a.by_id_path] = 400 * _GIB
        _run(agent, MagicMock(), device_controller.resize_device, a.get_id())

        read = db.get_storage_device_by_id(a.get_id())
        assert read.size == 400 * _GIB
        assert read.placement_size == 100 * _GIB


class TestGrowCluster:

    def test_uniform_growth_of_all_data_devices(self, db):
        _seed_cluster(db)
        a, b = _aio_device("S1", 0), _aio_device("S2", 1)
        jm = _aio_device("J1", 2, size_gib=10, status=NVMeDevice.STATUS_JM)
        _seed_node(db, [a, b, jm])
        agent = _agent({a.by_id_path: 200 * _GIB, b.by_id_path: 200 * _GIB})

        res = _run(agent, MagicMock(), device_controller.grow_cluster_devices, CLUSTER_ID)

        assert res["errors"] == {}
        assert res["uniform"] is True
        assert sorted(r["device_id"] for r in res["devices"]) == sorted([a.get_id(), b.get_id()])
        assert db.get_storage_device_by_id(jm.get_id()).size == 10 * _GIB
        assert res["rebalance"] is None
        assert db.get_storage_device_by_id(a.get_id()).placement_size == 100 * _GIB

    def test_uneven_growth_reweights_and_rebalances(self, db):
        _seed_cluster(db)
        a, b = _aio_device("S1", 0), _aio_device("S2", 1)
        node = _seed_node(db, [a, b])
        agent = _agent({a.by_id_path: 200 * _GIB, b.by_id_path: 300 * _GIB})

        with patch.object(device_controller.tasks_controller, "add_device_mig_task_for_node",
                          return_value=True) as queue:
            res = _run(agent, MagicMock(), device_controller.grow_cluster_devices, CLUSTER_ID)

        assert res["uniform"] is False
        assert res["rebalance"] == {"weights_updated": True, "started": True, "reason": ""}
        queue.assert_called_once_with(node.get_id())
        assert db.get_storage_device_by_id(a.get_id()).placement_size == 200 * _GIB
        assert db.get_storage_device_by_id(b.get_id()).placement_size == 300 * _GIB
        assert db.get_cluster_by_id(CLUSTER_ID).reweight_pending is False

        fresh = db.get_storage_node_by_id(node.get_id())
        cl_map = distr_controller.get_distr_cluster_map([fresh], fresh, "distr-1")
        weights = [i["weight"] for i in cl_map["map_prob"][0]["items"]]
        assert weights == [200, 300]

    def test_device_added_after_uniform_growth_is_weighted_on_the_same_scale(self, db):
        from simplyblock_core import storage_node_ops
        _seed_cluster(db)
        a, b = _aio_device("S1", 0), _aio_device("S2", 1)
        _seed_node(db, [a, b])
        agent = _agent({a.by_id_path: 200 * _GIB, b.by_id_path: 200 * _GIB})
        _run(agent, MagicMock(), device_controller.grow_cluster_devices, CLUSTER_ID)

        assert storage_node_ops.scaled_placement_size(db, CLUSTER_ID, 200 * _GIB) == 100 * _GIB


class TestResizeFailure:

    def test_failed_reopen_keeps_size_and_sets_unavailable(self, db):
        _seed_cluster(db)
        a = _aio_device("S1", 0)
        _seed_node(db, [a])
        agent = _agent({a.by_id_path: 200 * _GIB})
        rpc = MagicMock()
        rpc.alceml_bdev_backend_resize_end.side_effect = RPCRemoteError("reopen", -7)

        with pytest.raises(ValueError, match="unavailable"):
            _run(agent, rpc, device_controller.resize_device, a.get_id())

        read = db.get_storage_device_by_id(a.get_id())
        assert read.size == 100 * _GIB
        assert read.placement_size == 0
        assert read.status == NVMeDevice.STATUS_UNAVAILABLE
