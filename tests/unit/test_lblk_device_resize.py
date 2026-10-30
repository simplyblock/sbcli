"""Unit tests for the online growth of lblk devices (a LUN grown on its
storage array): device_controller.resize_device / grow_cluster_devices, the
frozen placement weight in distr_controller, and the host-side rescan in
simplyblock_web.node_utils.

Covered:
  - The cluster-map weight of a grown device stays at its placement size,
    on full-map builds and on the add-device path.
  - resize_device runs host rescan -> resize_begin -> bdev_aio_rescan ->
    resize_end, persists size and placement_size, and reports uniformity.
  - Refusals: not lblk, not online, partition, testing bdev, shrink.
  - Recovery: a failed AIO rescan reopens alceml and leaves the device
    unchanged; a failed reopen fails the I/O and sets the device unavailable.
  - rescan_block_device: SCSI rescan, NVMe controller rescan, partitions.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

from simplyblock_core import constants, distr_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

_GIB = 1024 ** 3


def _aio_dev(uid="dev-1", size_gib=100, placement_gib=0, status=NVMeDevice.STATUS_ONLINE, order=0):
    d = NVMeDevice()
    d.uuid = uid
    d.node_id = "node-1"
    d.cluster_id = "cluster-1"
    d.status = status
    d.bdev_type = "aio"
    d.serial_number = f"S-{uid}"
    d.nvme_bdev = f"aio_{uid}"
    d.alceml_bdev = f"alceml_{uid}"
    d.device_path = "/dev/sdb"
    d.by_id_path = f"/dev/disk/by-id/wwn-{uid}"
    d.size = size_gib * _GIB
    d.placement_size = placement_gib * _GIB
    d.cluster_device_order = order
    d.physical_label = 0
    return d


def _node(devices, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = "node-1"
    n.cluster_id = "cluster-1"
    n.status = status
    n.is_secondary_node = False
    n.failure_domain = -1
    n.nvme_devices = devices
    n.remote_devices = []
    return n


class TestPlacementWeight(unittest.TestCase):

    def test_weight_size_defaults_to_size(self):
        d = _aio_dev(size_gib=100)
        self.assertEqual(d.weight_size(), 100 * _GIB)

    def test_weight_size_is_frozen_after_growth(self):
        d = _aio_dev(size_gib=200, placement_gib=100)
        self.assertEqual(d.weight_size(), 100 * _GIB)

    def _build_map(self, node):
        with patch.object(distr_controller, "DBController") as DBCtor:
            cluster = MagicMock()
            cluster.enable_node_affinity = False
            cluster.enable_failure_domain = False
            DBCtor.return_value.get_cluster_by_id.return_value = cluster
            return distr_controller.get_distr_cluster_map([node], node, "distr-1")

    def test_cluster_map_weight_unchanged_by_growth(self):
        before = self._build_map(_node([_aio_dev("a", 100), _aio_dev("b", 100, order=1)]))
        after = self._build_map(_node([_aio_dev("a", 300, 100), _aio_dev("b", 100, order=1)]))
        self.assertEqual(before["map_prob"], after["map_prob"])
        self.assertEqual(after["map_prob"][0]["items"][0]["weight"], utils.convert_size(100 * _GIB, "GiB"))

    def test_add_device_weight_uses_placement_size(self):
        dev = _aio_dev(size_gib=300, placement_gib=100)
        target = _node([dev])
        db = MagicMock()
        db.get_storage_node_by_id.return_value = target
        rpc = MagicMock()
        target.rpc_client.return_value = rpc
        with patch.object(distr_controller, "DBController", return_value=db):
            distr_controller.send_cluster_map_add_device(dev, target)
        sent = rpc.distr_add_devices.call_args[0][0]
        self.assertEqual(sent["devices"][0]["weight"], utils.convert_size(100 * _GIB, "GiB"))


class _ResizeHarness:
    """Wires resize_device against mocks: one lblk cluster, one node."""

    def __init__(self, dev, new_size, others=(), node_status=StorageNode.STATUS_ONLINE,
                 device_mode=constants.DEVICE_MODE_LBLK):
        self.dev = dev
        self.node = _node([dev, *others], status=node_status)
        self.cluster = MagicMock()
        self.cluster.device_mode = device_mode
        self.cluster.status = "active"
        self.cluster.reweight_pending = False
        self.rpc = MagicMock()
        self.node.rpc_client.return_value = self.rpc
        self.client = MagicMock()
        self.client.rescan_block_device.return_value = (
            {"device": "sdb", "size": new_size, "rescanned": ["/sys/block/sdb/device/rescan"]}, None)
        self.node.client.return_value = self.client
        self.db = MagicMock()
        self.db.get_storage_device_by_id.side_effect = lambda _id: next(
            d for d in self.node.nvme_devices if d.get_id() == _id)
        self.db.get_storage_node_by_id.return_value = self.node
        self.db.get_storage_nodes_by_cluster_id.return_value = [self.node]
        self.db.get_cluster_by_id.return_value = self.cluster
        self.db.get_job_tasks.return_value = []
        self.push_ok = True

        def _atomic(node, mut):
            mut(node)
            return node
        self.db.atomic_update.side_effect = _atomic

    def run(self, fn=None, *args):
        with patch.object(device_controller, "DBController", return_value=self.db), \
             patch.object(device_controller.tasks_controller,
                          "get_active_dev_restart_task", return_value=None), \
             patch.object(device_controller, "device_set_unavailable") as self.set_unavail, \
             patch.object(device_controller, "device_events") as self.events, \
             patch.object(device_controller.distr_controller, "send_cluster_map_to_node",
                          side_effect=lambda n: self.push_ok) as self.push, \
             patch.object(device_controller.tasks_controller, "add_device_mig_task_for_node",
                          return_value=True) as self.rebalance_task, \
             patch.object(device_controller.time, "sleep"):
            return (fn or device_controller.resize_device)(*(args or (self.dev.get_id(),)))


class TestResizeDevice(unittest.TestCase):

    def test_grows_device_and_freezes_weight(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB)
        res = h.run()
        self.assertTrue(res["resized"])
        self.assertEqual(res["old_size"], 100 * _GIB)
        self.assertEqual(res["new_size"], 200 * _GIB)
        self.assertEqual(h.dev.size, 200 * _GIB)
        self.assertEqual(h.dev.placement_size, 100 * _GIB)
        self.assertEqual(res["placement_size"], 100 * _GIB)
        self.assertTrue(res["uniform"])
        h.client.rescan_block_device.assert_called_once_with("/dev/disk/by-id/wwn-dev-1")
        self.assertEqual(h.rpc.mock_calls[:3], [
            call.alceml_bdev_backend_resize_begin(
                "alceml_dev-1", request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC),
            call.bdev_aio_rescan("aio_dev-1"),
            call.alceml_bdev_backend_resize_end(
                "alceml_dev-1", b_fail=False, cntr_path="",
                request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC),
        ])
        h.events.device_resized.assert_called_once()

    def test_second_growth_keeps_original_placement(self):
        h = _ResizeHarness(_aio_dev(size_gib=200, placement_gib=100), 300 * _GIB)
        h.run()
        self.assertEqual(h.dev.placement_size, 100 * _GIB)
        self.assertEqual(h.dev.size, 300 * _GIB)

    def test_uneven_growth_warns(self):
        other = _aio_dev("dev-2", size_gib=100, order=1)
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB, others=[other])
        res = h.run()
        self.assertTrue(res["resized"])
        self.assertFalse(res["uniform"])
        self.assertIn("unevenly", res["warning"])

    def test_even_growth_is_uniform(self):
        other = _aio_dev("dev-2", size_gib=200, placement_gib=100, order=1)
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB, others=[other])
        self.assertTrue(h.run()["uniform"])

    def test_journal_device_is_ignored_for_uniformity(self):
        jm = _aio_dev("jm", size_gib=10, status=NVMeDevice.STATUS_JM, order=1)
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB, others=[jm])
        self.assertTrue(h.run()["uniform"])

    def test_unchanged_size_is_noop(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 100 * _GIB)
        res = h.run()
        self.assertFalse(res["resized"])
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()

    def test_shrink_is_refused(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 50 * _GIB)
        with self.assertRaisesRegex(ValueError, "shrank"):
            h.run()
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()
        self.assertEqual(h.dev.size, 100 * _GIB)

    def test_refuses_nvme_cluster(self):
        h = _ResizeHarness(_aio_dev(), 200 * _GIB, device_mode=constants.DEVICE_MODE_NVME)
        with self.assertRaisesRegex(ValueError, "lblk"):
            h.run()

    def test_refuses_offline_device(self):
        h = _ResizeHarness(_aio_dev(status=NVMeDevice.STATUS_UNAVAILABLE), 200 * _GIB)
        with self.assertRaisesRegex(ValueError, "online"):
            h.run()

    def test_refuses_offline_node(self):
        h = _ResizeHarness(_aio_dev(), 200 * _GIB, node_status=StorageNode.STATUS_OFFLINE)
        with self.assertRaisesRegex(ValueError, "Storage node must be online"):
            h.run()

    def test_refuses_partition(self):
        dev = _aio_dev()
        dev.is_partition = True
        h = _ResizeHarness(dev, 200 * _GIB)
        with self.assertRaisesRegex(ValueError, "Partitions"):
            h.run()

    def test_refuses_testing_bdev(self):
        dev = _aio_dev()
        dev.testing_bdev = "test_dev-1"
        h = _ResizeHarness(dev, 200 * _GIB)
        with self.assertRaisesRegex(ValueError, "testing or hang"):
            h.run()

    def test_host_rescan_error(self):
        h = _ResizeHarness(_aio_dev(), 200 * _GIB)
        h.client.rescan_block_device.return_value = (None, "not found")
        with self.assertRaisesRegex(ValueError, "Rescan"):
            h.run()
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()

    def test_aio_rescan_failure_reopens_and_leaves_device_unchanged(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB)
        h.rpc.bdev_aio_rescan.side_effect = RPCRemoteError("rescan failed", -1)
        with self.assertRaisesRegex(ValueError, "unchanged"):
            h.run()
        h.rpc.alceml_bdev_backend_resize_end.assert_called_once_with(
            "alceml_dev-1", b_fail=False, cntr_path="",
            request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC)
        self.assertEqual(h.dev.size, 100 * _GIB)
        self.assertEqual(h.dev.placement_size, 0)
        h.set_unavail.assert_not_called()

    def test_reopen_failure_fails_io_and_sets_unavailable(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB)
        h.rpc.alceml_bdev_backend_resize_end.side_effect = RPCRemoteError("reopen", -7)
        with self.assertRaisesRegex(ValueError, "unavailable"):
            h.run()
        calls = h.rpc.alceml_bdev_backend_resize_end.call_args_list
        self.assertEqual(len(calls), device_controller.RESIZE_END_ATTEMPTS + 1)
        self.assertTrue(calls[-1].kwargs["b_fail"])
        h.set_unavail.assert_called_once()
        self.assertEqual(h.dev.size, 100 * _GIB)

    def test_begin_failure_reopens(self):
        h = _ResizeHarness(_aio_dev(size_gib=100), 200 * _GIB)
        h.rpc.alceml_bdev_backend_resize_begin.side_effect = RPCRemoteError("busy", -6)
        with self.assertRaisesRegex(ValueError, "resize_begin failed.*unchanged"):
            h.run()
        h.rpc.bdev_aio_rescan.assert_not_called()
        h.rpc.alceml_bdev_backend_resize_end.assert_called()


class TestGrowClusterDevices(unittest.TestCase):

    def test_grows_all_aio_devices_and_collects_errors(self):
        good = _aio_dev("dev-1", size_gib=100)
        bad = _aio_dev("dev-2", size_gib=100, order=1)
        bad.testing_bdev = "x"
        jm = _aio_dev("jm", size_gib=10, status=NVMeDevice.STATUS_JM, order=2)
        h = _ResizeHarness(good, 200 * _GIB, others=[bad, jm])
        res = h.run(device_controller.grow_cluster_devices, "cluster-1")
        self.assertEqual([r["device_id"] for r in res["devices"]], ["dev-1"])
        self.assertIn("dev-2", res["errors"])
        self.assertNotIn("jm", res["errors"])


class _GrowHarness(_ResizeHarness):
    """Several devices whose block devices report their own new sizes."""

    def __init__(self, sizes_gib, **kw):
        devs = [_aio_dev(f"dev-{i}", size_gib=100, order=i) for i in range(len(sizes_gib))]
        super().__init__(devs[0], 0, others=devs[1:], **kw)
        self.devs = devs
        by_path = {d.by_id_path: g * _GIB for d, g in zip(devs, sizes_gib)}
        self.client.rescan_block_device.side_effect = lambda path: (
            {"device": "sdx", "size": by_path[path], "rescanned": []}, None)

    def grow(self, **kw):
        return self.run(lambda cid: device_controller.grow_cluster_devices(cid, **kw), "cluster-1")


class TestRebalanceAfterGrowth(unittest.TestCase):

    def test_uniform_growth_keeps_weights_and_does_not_rebalance(self):
        h = _GrowHarness([200, 200, 200])
        res = h.grow()
        self.assertTrue(res["uniform"])
        self.assertIsNone(res["rebalance"])
        self.assertEqual([d.weight_size() for d in h.devs], [100 * _GIB] * 3)
        h.push.assert_not_called()
        h.rebalance_task.assert_not_called()

    def test_uneven_growth_reweights_pushes_and_rebalances(self):
        h = _GrowHarness([200, 300, 100])
        res = h.grow()
        self.assertFalse(res["uniform"])
        self.assertEqual(res["rebalance"], {"weights_updated": True, "started": True, "reason": ""})
        self.assertEqual([d.weight_size() for d in h.devs], [200 * _GIB, 300 * _GIB, 100 * _GIB])
        h.push.assert_called_once_with(h.node)
        h.rebalance_task.assert_called_once_with("node-1")
        self.assertFalse(h.cluster.reweight_pending)
        h.events.devices_reweighted.assert_called_once_with("cluster-1", True)

    def test_no_rebalance_keeps_frozen_weights(self):
        h = _GrowHarness([200, 300])
        res = h.grow(rebalance=False)
        self.assertFalse(res["uniform"])
        self.assertIsNone(res["rebalance"])
        self.assertEqual([d.weight_size() for d in h.devs], [100 * _GIB] * 2)
        h.push.assert_not_called()

    def test_open_migration_blocks_reweight(self):
        h = _GrowHarness([200, 300])
        task = MagicMock(function_name=device_controller.JobSchedule.FN_NEW_DEV_MIG,
                         status="running", canceled=False, uuid="t-1")
        h.db.get_job_tasks.return_value = [task]
        res = h.grow()
        self.assertFalse(res["rebalance"]["started"])
        self.assertIn("t-1", res["rebalance"]["reason"])
        self.assertEqual([d.weight_size() for d in h.devs], [100 * _GIB] * 2)
        self.assertFalse(h.cluster.reweight_pending)
        h.push.assert_not_called()

    def test_inactive_cluster_blocks_reweight(self):
        h = _GrowHarness([200, 300])
        h.cluster.status = "degraded"
        res = h.grow()
        self.assertIn("degraded", res["rebalance"]["reason"])
        self.assertEqual([d.weight_size() for d in h.devs], [100 * _GIB] * 2)

    def test_failed_growth_does_not_reweight(self):
        h = _GrowHarness([200, 300])
        h.devs[1].testing_bdev = "x"
        res = h.grow()
        self.assertIn("dev-1", res["errors"])
        self.assertFalse(res["rebalance"]["started"])
        self.assertIn("failed to grow", res["rebalance"]["reason"])
        h.push.assert_not_called()

    def test_failed_push_keeps_pending_and_retry_finishes(self):
        h = _GrowHarness([200, 300])
        h.push_ok = False
        res = h.grow()
        self.assertFalse(res["rebalance"]["started"])
        self.assertIn("push failed", res["rebalance"]["reason"])
        self.assertTrue(h.cluster.reweight_pending)
        self.assertEqual([d.weight_size() for d in h.devs], [200 * _GIB, 300 * _GIB])
        h.rebalance_task.assert_not_called()

        # Re-run: sizes unchanged, growth now looks uniform, but the pending
        # flag makes it push again and start the rebalance.
        h.push_ok = True
        res = h.grow()
        self.assertTrue(res["rebalance"]["started"])
        self.assertFalse(h.cluster.reweight_pending)
        h.rebalance_task.assert_called_once_with("node-1")

    def test_weights_updated_without_volume_stores_to_rebalance(self):
        h = _GrowHarness([200, 300])

        def _grow_without_queue(cluster_id):
            h.rebalance_task.return_value = False
            return device_controller.grow_cluster_devices(cluster_id)
        res = h.run(_grow_without_queue, "cluster-1")
        self.assertTrue(res["rebalance"]["weights_updated"])
        self.assertFalse(res["rebalance"]["started"])
        self.assertIn("no rebalance was queued", res["rebalance"]["reason"])
        self.assertFalse(h.cluster.reweight_pending)
        self.assertEqual([d.weight_size() for d in h.devs], [200 * _GIB, 300 * _GIB])

    def test_cluster_map_carries_new_weights_after_reweight(self):
        h = _GrowHarness([200, 300])
        h.grow()
        cl_map = TestPlacementWeight._build_map(None, h.node)
        weights = [i["weight"] for i in cl_map["map_prob"][0]["items"]]
        self.assertEqual(weights, [200, 300])


class TestScaledPlacementSize(unittest.TestCase):

    def _db(self, devs):
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = [_node(devs)]
        return db

    def test_no_growth_uses_full_size(self):
        db = self._db([_aio_dev("a", size_gib=100), _aio_dev("b", size_gib=100, order=1)])
        self.assertEqual(storage_node_ops.scaled_placement_size(db, "cluster-1", 100 * _GIB), 0)

    def test_uniform_growth_scales_new_device(self):
        db = self._db([_aio_dev("a", size_gib=200, placement_gib=100),
                       _aio_dev("b", size_gib=200, placement_gib=100, order=1)])
        self.assertEqual(storage_node_ops.scaled_placement_size(db, "cluster-1", 200 * _GIB), 100 * _GIB)

    def test_uneven_growth_uses_full_size(self):
        db = self._db([_aio_dev("a", size_gib=200, placement_gib=100),
                       _aio_dev("b", size_gib=100, order=1)])
        self.assertEqual(storage_node_ops.scaled_placement_size(db, "cluster-1", 200 * _GIB), 0)

    def test_new_and_unordered_devices_are_ignored(self):
        db = self._db([_aio_dev("a", size_gib=200, placement_gib=100),
                       _aio_dev("n", size_gib=50, status=NVMeDevice.STATUS_NEW, order=-1)])
        self.assertEqual(storage_node_ops.scaled_placement_size(db, "cluster-1", 400 * _GIB), 200 * _GIB)


class TestRescanBlockDevice(unittest.TestCase):
    """node_utils.rescan_block_device against a fake sysfs tree."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    def _mk(self, rel, content=""):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        return path

    def _run(self, name):
        from simplyblock_web import node_utils
        real_exists = os.path.exists

        def _map(p):
            return os.path.join(self.root, p.lstrip("/")) if p.startswith("/sys/") else p

        writes = []

        def _write(p, v):
            writes.append((p, v))

        with patch.object(node_utils.os.path, "realpath", return_value=f"/dev/{name}"), \
             patch.object(node_utils.os.path, "exists", side_effect=lambda p: real_exists(_map(p))), \
             patch.object(node_utils, "_read_sysfs",
                          side_effect=lambda p: open(_map(p)).read().strip() if real_exists(_map(p)) else ""), \
             patch("glob.glob", side_effect=lambda pat: sorted(
                 "/" + os.path.relpath(x, self.root).replace(os.sep, "/")
                 for x in __import__("glob").iglob(_map(pat)))), \
             patch.object(node_utils, "_write_sysfs", side_effect=_write):
            return node_utils.rescan_block_device(f"/dev/disk/by-id/x-{name}"), writes

    def test_scsi_disk_is_rescanned(self):
        self._mk("sys/class/block/sdb/size", "419430400")
        self._mk("sys/class/block/sdb/device/rescan")
        res, writes = self._run("sdb")
        self.assertEqual(res["size"], 419430400 * 512)
        self.assertEqual(writes, [("/sys/class/block/sdb/device/rescan", "1")])

    def test_nvme_multipath_head_rescans_every_controller(self):
        self._mk("sys/class/block/nvme1n1/size", "2048")
        self._mk("sys/class/block/nvme1n1/device/nvme1/rescan_controller")
        self._mk("sys/class/block/nvme1n1/device/nvme2/rescan_controller")
        res, writes = self._run("nvme1n1")
        self.assertEqual(res["size"], 2048 * 512)
        self.assertEqual([w[0] for w in writes], [
            "/sys/class/block/nvme1n1/device/nvme1/rescan_controller",
            "/sys/class/block/nvme1n1/device/nvme2/rescan_controller",
        ])

    def test_partition_is_refused(self):
        self._mk("sys/class/block/sdb1/size", "2048")
        self._mk("sys/class/block/sdb1/partition", "1")
        with self.assertRaisesRegex(ValueError, "partition"):
            self._run("sdb1")

    def test_virtio_without_rescan_reads_size(self):
        self._mk("sys/class/block/vdb/size", "4096")
        res, writes = self._run("vdb")
        self.assertEqual(res["size"], 4096 * 512)
        self.assertEqual(writes, [])


if __name__ == "__main__":
    unittest.main()
