# coding=utf-8
"""
test_restart_connect_hardening.py — unit tests for the 2026-07-16
half-cluster restart-storm fixes.

Background (incident 2026-07-16): 16 nodes of one failure domain rebooted;
every restart attempt spawned ~34 device-connect threads, each of which did
a full node-table scan plus a whole-node record write in FDB just to flip
the ``connecting_from_node`` debounce flag. 500+ concurrent threads
saturated FDB (error 1031 storms), connect threads died BEFORE their attach
RPC ran, the node-specific cluster maps degraded healthy online devices to
``unavailable``, and the first stripe read through the distribs (the raid
examine read inside recreate_lvstore) failed with EIO ~5 minutes into every
attempt — for 1.5 hours, until the control plane collapsed and suspended
the cluster.

Invariants covered:

  1. ``NVMeDevice.lock_device_connection`` performs a targeted
     ``get_storage_node_by_id`` + ``atomic_update`` (never a full
     ``get_storage_nodes`` scan, never ``write_to_db``), and any DB failure
     is swallowed — the flag is a best-effort debounce, not a correctness
     gate.
  2. ``_connect_device_thread`` retries a failed connect (the attach RPC is
     cheap; a CP-DB hiccup must not silently cost the connection) and never
     raises out of the thread body.
  3. ``_verify_online_device_coverage`` reports full coverage as [],
     excludes in_restart/offline peers (their links are re-established by
     the health service later), repairs missing connects and refreshes the
     persisted remote_devices, and reports devices that stay missing.
  4. ``recreate_all_lvstores`` is strictly serial across concurrent callers.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _make_node(uuid, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = "c1"
    n.nvme_devices = []
    n.remote_devices = []
    n.write_to_db = MagicMock(return_value=True)
    return n


def _make_dev(uuid, node_id, status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.uuid = uuid
    d.node_id = node_id
    d.status = status
    d.alceml_bdev = f"alceml_{uuid}"
    d.alceml_name = f"alceml_{uuid}"
    d.size = 1024
    d.nvmf_multipath = False
    return d


class TestDeviceConnectionLock(unittest.TestCase):
    """Invariant 1: the debounce flag write is targeted and best-effort."""

    def _patch_db(self):
        patcher = patch("simplyblock_core.db_controller.DBController")
        mock_cls = patcher.start()
        self.addCleanup(patcher.stop)
        return mock_cls.return_value

    def test_lock_uses_targeted_atomic_update_not_full_scan(self):
        db = self._patch_db()
        owner = _make_node("node-b")
        dev = _make_dev("dev-1", "node-b")
        owner.nvme_devices = [dev]
        db.get_storage_node_by_id.return_value = owner

        dev.lock_device_connection("node-a")

        db.get_storage_node_by_id.assert_called_once_with("node-b")
        db.atomic_update.assert_called_once()
        db.get_storage_nodes.assert_not_called()
        owner.write_to_db.assert_not_called()

        # The mutate_fn flips the flag on the fresh copy of the same device
        # and aborts (returns False) when the device is gone from the record.
        mutate_fn = db.atomic_update.call_args.args[1]
        fresh = _make_node("node-b")
        fresh_dev = _make_dev("dev-1", "node-b")
        fresh.nvme_devices = [fresh_dev]
        self.assertIsNone(mutate_fn(fresh))
        self.assertEqual(fresh_dev.connecting_from_node, "node-a")
        empty = _make_node("node-b")
        self.assertIs(mutate_fn(empty), False)

    def test_lock_swallows_db_errors(self):
        db = self._patch_db()
        db.get_storage_node_by_id.side_effect = RuntimeError("fdb 1031")
        dev = _make_dev("dev-1", "node-b")
        dev.lock_device_connection("node-a")  # must not raise

    def test_lock_without_node_id_is_noop(self):
        db = self._patch_db()
        dev = _make_dev("dev-1", node_id="")
        dev.lock_device_connection("node-a")
        db.get_storage_node_by_id.assert_not_called()


class TestConnectDeviceThread(unittest.TestCase):
    """Invariant 2: bounded retry, loud logging, never raises."""

    def setUp(self):
        patcher_connect = patch.object(storage_node_ops, "connect_device")
        self.mock_connect = patcher_connect.start()
        self.addCleanup(patcher_connect.stop)

        patcher_sleep = patch.object(storage_node_ops.time, "sleep")
        patcher_sleep.start()
        self.addCleanup(patcher_sleep.stop)

        self.node = _make_node("node-a")
        self.dev = _make_dev("dev-1", "node-b")

    def test_transient_failure_is_retried_to_success(self):
        self.mock_connect.side_effect = [RuntimeError("fdb 1031"), "bdev-n1"]
        storage_node_ops._connect_device_thread("remote_x", self.dev, self.node)
        self.assertEqual(self.mock_connect.call_count, 2)

    def test_terminal_failure_gives_up_after_three_attempts_without_raising(self):
        self.mock_connect.side_effect = RuntimeError("unreachable")
        storage_node_ops._connect_device_thread("remote_x", self.dev, self.node)
        self.assertEqual(self.mock_connect.call_count, 3)

    def test_success_connects_once(self):
        self.mock_connect.return_value = "bdev-n1"
        storage_node_ops._connect_device_thread("remote_x", self.dev, self.node)
        self.assertEqual(self.mock_connect.call_count, 1)


class TestVerifyOnlineDeviceCoverage(unittest.TestCase):
    """Invariant 3: coverage gate semantics."""

    def setUp(self):
        self.snode = _make_node("node-a")
        self.rpc = MagicMock()
        self.snode.rpc_client = MagicMock(return_value=self.rpc)

        patcher_db = patch.object(storage_node_ops, "DBController")
        self.mock_db_cls = patcher_db.start()
        self.addCleanup(patcher_db.stop)
        self.mock_db = self.mock_db_cls.return_value

        patcher_thread = patch.object(storage_node_ops, "_connect_device_thread")
        self.mock_connect_thread = patcher_thread.start()
        self.addCleanup(patcher_thread.stop)

        patcher_reconcile = patch.object(storage_node_ops, "_connect_to_remote_devs")
        self.mock_reconcile = patcher_reconcile.start()
        self.addCleanup(patcher_reconcile.stop)
        self.mock_reconcile.return_value = ["refreshed-records"]

    def _set_cluster(self, *peers):
        self.mock_db.get_storage_nodes_by_cluster_id.return_value = [
            self.snode, *peers]

    def test_full_coverage_returns_empty_without_repair(self):
        peer = _make_node("node-b")
        peer.nvme_devices = [_make_dev("dev-1", "node-b")]
        self._set_cluster(peer)
        self.rpc.get_bdevs.return_value = [{"name": "remote_alceml_dev-1n1"}]

        missing = storage_node_ops._verify_online_device_coverage(self.snode)

        self.assertEqual(missing, [])
        self.mock_connect_thread.assert_not_called()
        self.mock_reconcile.assert_not_called()

    def test_restarting_and_offline_peers_are_excluded(self):
        restarting = _make_node("node-b", status=StorageNode.STATUS_RESTARTING)
        restarting.nvme_devices = [_make_dev("dev-1", "node-b")]
        offline = _make_node("node-c", status=StorageNode.STATUS_OFFLINE)
        offline.nvme_devices = [_make_dev("dev-2", "node-c")]
        self._set_cluster(restarting, offline)
        self.rpc.get_bdevs.return_value = []  # nothing attached at all

        missing = storage_node_ops._verify_online_device_coverage(self.snode)

        self.assertEqual(missing, [])
        self.mock_connect_thread.assert_not_called()

    def test_missing_device_is_repaired_and_records_refreshed(self):
        peer = _make_node("node-b")
        dev = _make_dev("dev-1", "node-b")
        peer.nvme_devices = [dev]
        self._set_cluster(peer)
        # Probes are batched (one unfiltered dump per pass, 2026-07-21):
        # pass 1 dump lacks the expected bdev; post-repair dump has it. The
        # dumps must be non-empty — an empty dump is treated as a failed
        # fetch and falls back to per-device filtered probes.
        self.rpc.get_bdevs.side_effect = [
            [{"name": "some_other_bdev"}],
            [{"name": "remote_alceml_dev-1n1"}],
        ]

        missing = storage_node_ops._verify_online_device_coverage(self.snode)

        self.assertEqual(missing, [])
        self.mock_connect_thread.assert_called_once()
        self.assertEqual(
            self.mock_connect_thread.call_args.args[0], "remote_alceml_dev-1")
        self.mock_reconcile.assert_called_once()
        self.mock_db.atomic_update.assert_called_once()

    def test_unrepairable_device_is_reported(self):
        peer = _make_node("node-b")
        peer.nvme_devices = [_make_dev("dev-1", "node-b")]
        self._set_cluster(peer)
        self.rpc.get_bdevs.return_value = []  # absent before AND after repair

        missing = storage_node_ops._verify_online_device_coverage(self.snode)

        self.assertEqual(missing, ["remote_alceml_dev-1n1"])
        self.mock_connect_thread.assert_called_once()
        # No refresh of remote_devices when coverage is still incomplete.
        self.mock_reconcile.assert_not_called()

    def test_repair_false_only_probes(self):
        peer = _make_node("node-b")
        peer.nvme_devices = [_make_dev("dev-1", "node-b")]
        self._set_cluster(peer)
        self.rpc.get_bdevs.return_value = []

        missing = storage_node_ops._verify_online_device_coverage(
            self.snode, repair=False)

        self.assertEqual(missing, ["remote_alceml_dev-1n1"])
        self.mock_connect_thread.assert_not_called()


class TestRecreateAllLvstoresSerialization(unittest.TestCase):
    """Invariant 4 (revised 2026-07-20): recreate_all_lvstores is no longer
    globally serialized — the process-wide gate was replaced by per-LVS
    locks (same-LVS recreates serialize; different-LVS run concurrently,
    covered by TestPerLvsRecreateLock in test_restart_cpu_fixes.py). The
    port-block critical span is serialized separately by
    ``_port_block_window_gate``. Here: different-node calls must be ABLE to
    overlap (the whole point of dropping the global gate), and all complete.
    """

    def test_concurrent_calls_can_overlap(self):
        active = []
        overlap_seen = []
        done = []

        def _fake_serial(snode, force=False):
            active.append(snode)
            if len(active) > 1:
                overlap_seen.append(True)
            time.sleep(0.05)
            active.remove(snode)
            done.append(snode)
            return True

        with patch.object(
                storage_node_ops, "_recreate_all_lvstores_serial",
                side_effect=_fake_serial):
            threads = [
                threading.Thread(
                    target=storage_node_ops.recreate_all_lvstores,
                    args=(_make_node(f"node-{i}"),))
                for i in range(4)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(len(done), 4)
        self.assertTrue(overlap_seen,
                        "different-node recreates must run concurrently "
                        "(global gate was removed by design)")


if __name__ == "__main__":
    unittest.main()
