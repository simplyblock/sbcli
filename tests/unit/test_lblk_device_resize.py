"""Unit tests for the online growth of lblk devices (LUNs grown on their
storage array): device_controller.grow_cluster_devices, the frozen placement
weight in distr_controller, the capacity cap, and the host-side rescan in
simplyblock_web.node_utils.

Covered:
  - The cluster-map weight of a grown device stays at its placement size,
    on full-map builds and on the add-device path; growth never changes it.
  - Growth is uniform: every data device grows by the smallest growth factor
    of all block devices (1.5/2.0 -> 1.5); a device that did not grow stops
    all growth; repeated runs change nothing.
  - A shrunk block device is refused, logged, and raises device_shrunk.
  - Refusals: not lblk, offline device or node, partition, testing bdev.
  - Recovery: a failed reopen or AIO rescan leaves every size unchanged.
  - The capacity statistics leave out the unused rest of a block device.
  - rescan_block_device: SCSI rescan, NVMe controller rescan, partitions.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, call, patch

from simplyblock_core import constants, distr_controller, storage_node_ops, utils
from simplyblock_core.controllers import alerts_controller, device_controller
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


class _GrowHarness:
    """Wires grow_cluster_devices against mocks: one lblk cluster, one node
    with data devices of 100 GiB whose block devices report the given sizes."""

    def __init__(self, sizes_gib, others=(), node_status=StorageNode.STATUS_ONLINE,
                 device_mode=constants.DEVICE_MODE_LBLK, dev_gib=100):
        self.devs = [_aio_dev(f"dev-{i}", size_gib=dev_gib, order=i) for i in range(len(sizes_gib))]
        self.node = _node([*self.devs, *others], status=node_status)
        self.cluster = MagicMock()
        self.cluster.device_mode = device_mode
        self.rpc = MagicMock()
        self.node.rpc_client.return_value = self.rpc
        self.client = MagicMock()
        self.set_sizes(sizes_gib)
        self.node.client.return_value = self.client
        self.db = MagicMock()
        self.db.get_storage_device_by_id.side_effect = lambda _id: next(
            d for d in self.node.nvme_devices if d.get_id() == _id)
        self.db.get_storage_node_by_id.return_value = self.node
        self.db.get_storage_nodes_by_cluster_id.return_value = [self.node]
        self.db.get_cluster_by_id.return_value = self.cluster

        def _atomic(node, mut):
            mut(node)
            return node
        self.db.atomic_update.side_effect = _atomic

    def set_sizes(self, sizes_gib):
        by_path = {d.by_id_path: int(g * _GIB) for d, g in zip(self.devs, sizes_gib)}
        self.client.rescan_block_device.side_effect = lambda path: (
            {"device": "sdx", "size": by_path[path], "rescanned": []}, None)

    def grow(self):
        self.rpc.reset_mock()
        with patch.object(device_controller, "DBController", return_value=self.db), \
             patch.object(device_controller.tasks_controller,
                          "get_active_dev_restart_task", return_value=None), \
             patch.object(device_controller, "device_set_unavailable") as self.set_unavail, \
             patch.object(device_controller, "device_events") as self.events, \
             patch.object(device_controller.time, "sleep"):
            return device_controller.grow_cluster_devices("cluster-1")

    def sizes(self):
        return [d.size // _GIB for d in self.devs]


class TestGrowClusterDevices(unittest.TestCase):

    def test_uniform_growth_grows_every_device(self):
        h = _GrowHarness([150, 150, 150])
        res = h.grow()
        self.assertTrue(res["grown"])
        self.assertAlmostEqual(res["factor"], 1.5)
        self.assertEqual(h.sizes(), [150, 150, 150])
        for d in h.devs:
            self.assertEqual(d.placement_size, 100 * _GIB)
            self.assertEqual(d.backend_size, 150 * _GIB)
            self.assertEqual(d.unused_size(), 0)
        self.assertEqual([r["unused_size"] for r in res["devices"]], [0, 0, 0])
        self.assertEqual(h.rpc.mock_calls[:3], [
            call.alceml_bdev_backend_resize_begin(
                "alceml_dev-0", request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC),
            call.bdev_aio_rescan("aio_dev-0"),
            call.alceml_bdev_backend_resize_end(
                "alceml_dev-0", b_fail=False, cntr_path="",
                request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC),
        ])
        self.assertEqual(h.rpc.alceml_bdev_backend_resize_begin.call_count, 3)
        self.assertEqual(h.events.device_resized.call_count, 3)

    def test_mixed_factors_grow_all_by_the_smallest(self):
        h = _GrowHarness([150, 150, 150, 200, 200, 200])
        res = h.grow()
        self.assertTrue(res["grown"])
        self.assertAlmostEqual(res["factor"], 1.5)
        self.assertEqual(h.sizes(), [150] * 6)
        self.assertEqual([d.unused_size() // _GIB for d in h.devs], [0, 0, 0, 50, 50, 50])
        self.assertEqual([d.placement_size // _GIB for d in h.devs], [100] * 6)
        # Every alceml reopens: all of them take their whole block device.
        self.assertEqual(h.rpc.alceml_bdev_backend_resize_begin.call_count, 6)

    def test_weights_never_change(self):
        h = _GrowHarness([150, 200])
        before = TestPlacementWeight()._build_map(_node([copy_dev(d) for d in h.devs]))
        h.grow()
        after = TestPlacementWeight()._build_map(h.node)
        self.assertEqual(before["map_prob"], after["map_prob"])

    def test_device_that_did_not_grow_stops_all_growth(self):
        h = _GrowHarness([150, 100, 200])
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertEqual(res["factor"], 1.0)
        self.assertIn("Not every block device has grown", res["reason"])
        self.assertEqual(h.sizes(), [100, 100, 100])
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()
        # The rescanned sizes are recorded all the same.
        self.assertEqual([d.backend_size // _GIB for d in h.devs], [150, 100, 200])

    def test_shrunk_device_is_refused_and_alerted(self):
        h = _GrowHarness([150, 80, 150])
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertIn("dev-1", res["errors"])
        self.assertIn("shrank", res["errors"]["dev-1"])
        self.assertEqual(res["shrunk"], ["dev-1"])
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()
        self.assertEqual(h.sizes(), [100, 100, 100])
        self.assertTrue(h.devs[1].is_shrunk())
        h.events.device_shrink_detected.assert_called_once()
        alerts = alerts_controller._device_alerts("cluster-1", [h.node])
        shrunk = [a for a in alerts if a["kind"] == alerts_controller.DEVICE_SHRUNK]
        self.assertEqual([a["device_id"] for a in shrunk], ["dev-1"])

    def test_restored_lun_resolves_the_shrink(self):
        h = _GrowHarness([100, 80])
        h.grow()
        h.set_sizes([100, 100])
        res = h.grow()
        self.assertFalse(h.devs[1].is_shrunk())
        self.assertEqual(res["errors"], {})
        alerts = alerts_controller._device_alerts("cluster-1", [h.node])
        self.assertFalse([a for a in alerts if a["kind"] == alerts_controller.DEVICE_SHRUNK])

    def test_repeated_runs_are_idempotent(self):
        h = _GrowHarness([150, 200])
        h.grow()
        sizes = h.sizes()
        for _ in range(2):
            res = h.grow()
            self.assertFalse(res["grown"])
            self.assertEqual(h.sizes(), sizes)
            h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()
        # A later, larger growth still applies on top.
        h.set_sizes([300, 300])
        res = h.grow()
        self.assertTrue(res["grown"])
        self.assertAlmostEqual(res["factor"], 2.0)
        self.assertEqual(h.sizes(), [300, 300])
        self.assertEqual([d.placement_size // _GIB for d in h.devs], [100, 100])

    def test_offline_device_stops_all_growth(self):
        h = _GrowHarness([150, 150])
        h.devs[1].status = NVMeDevice.STATUS_UNAVAILABLE
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertIn("dev-1", res["errors"])
        h.client.rescan_block_device.assert_not_called()
        self.assertEqual(h.sizes(), [100, 100])

    def test_journal_and_new_devices_are_ignored(self):
        jm = _aio_dev("jm", size_gib=10, status=NVMeDevice.STATUS_JM, order=5)
        new = _aio_dev("new", size_gib=50, status=NVMeDevice.STATUS_NEW, order=-1)
        h = _GrowHarness([150, 150], others=[jm, new])
        res = h.grow()
        self.assertTrue(res["grown"])
        self.assertEqual(jm.size, 10 * _GIB)
        self.assertEqual(new.size, 50 * _GIB)

    def test_refuses_nvme_cluster(self):
        h = _GrowHarness([150], device_mode="nvme")
        with self.assertRaisesRegex(ValueError, "lblk"):
            h.grow()

    def test_offline_node_stops_all_growth(self):
        h = _GrowHarness([150], node_status=StorageNode.STATUS_OFFLINE)
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertIn("Storage node must be online", res["errors"]["dev-0"])

    def test_partition_or_testing_bdev_stops_all_growth(self):
        h = _GrowHarness([150, 150])
        h.devs[0].is_partition = True
        h.devs[1].testing_bdev = "x"
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertEqual(set(res["errors"]), {"dev-0", "dev-1"})

    def test_rescan_error_stops_all_growth(self):
        h = _GrowHarness([150, 150])
        h.client.rescan_block_device.side_effect = lambda path: (None, "no such device")
        res = h.grow()
        self.assertFalse(res["grown"])
        h.rpc.alceml_bdev_backend_resize_begin.assert_not_called()

    def test_reopen_failure_keeps_all_sizes_and_sets_unavailable(self):
        h = _GrowHarness([150, 150])
        h.rpc.alceml_bdev_backend_resize_end.side_effect = [
            None, RPCRemoteError("reopen", -7), RPCRemoteError("reopen", -7),
            RPCRemoteError("reopen", -7), None]
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertIn("dev-1", res["errors"])
        self.assertEqual(h.sizes(), [100, 100])
        h.set_unavail.assert_called_once()
        h.events.device_resized.assert_not_called()

    def test_aio_rescan_failure_keeps_all_sizes(self):
        h = _GrowHarness([150, 150])
        h.rpc.bdev_aio_rescan.side_effect = RPCRemoteError("rescan", -5)
        res = h.grow()
        self.assertFalse(res["grown"])
        self.assertIn("unchanged", res["errors"]["dev-0"])
        self.assertEqual(h.sizes(), [100, 100])
        h.set_unavail.assert_not_called()

    def test_begin_failure_reopens(self):
        h = _GrowHarness([150])
        h.rpc.alceml_bdev_backend_resize_begin.side_effect = RPCRemoteError("timeout", -1)
        res = h.grow()
        self.assertFalse(res["grown"])
        h.rpc.alceml_bdev_backend_resize_end.assert_called_with(
            "alceml_dev-0", b_fail=False, cntr_path="",
            request_timeout=device_controller.RESIZE_RPC_TIMEOUT_SEC)
        h.set_unavail.assert_not_called()


class TestUnusedCapacity(unittest.TestCase):
    """The capacity statistics leave out the part of a block device beyond
    the device size: alceml spans the whole block device."""

    def test_device_stats_cap_total_at_device_size(self):
        from simplyblock_core.models.stats import DeviceStatObject
        from simplyblock_core.services import capacity_and_stats_collector as collector
        dev = _aio_dev(size_gib=150, placement_gib=100)
        dev.backend_size = 200 * _GIB
        cl = MagicMock()
        cl.get_id.return_value = "cluster-1"
        cl.distr_ndcs, cl.distr_npcs = 1, 1
        page = 2 * 1024 * 1024
        cap = {"res": 1, "npages_nmax": (200 * _GIB) // page, "npages_used": 0, "pba_page_size": page}
        db = MagicMock()
        db.get_device_stats.return_value = []
        with patch.object(collector, "db", db), patch.object(collector, "last_object_record", {}), \
             patch.object(DeviceStatObject, "write_to_db", lambda self, kv: True):
            rec = collector.add_device_stats(cl, dev, cap, None)
        self.assertEqual(rec.size_total_raw, 150 * _GIB)

    def test_alert_free_device_reports_unused(self):
        dev = _aio_dev(size_gib=150, placement_gib=100)
        dev.backend_size = 200 * _GIB
        self.assertEqual(dev.unused_size(), 50 * _GIB)
        self.assertFalse(dev.is_shrunk())


def copy_dev(d):
    return NVMeDevice(d.to_dict())


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

    def test_inconsistent_ratios_use_full_size(self):
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
