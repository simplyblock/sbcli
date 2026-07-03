# coding=utf-8
"""
test_remote_dev_reconcile.py — unit tests for
``storage_node_ops.reconnect_dropped_remote_devs``.

Background: ``node.remote_devices`` is rebuilt as "whatever was reachable
at that moment" by the restart / port-allow paths, so a peer that is
unreachable while this node restarts (e.g. network outage) is silently
dropped from the list. The health-check repair loop iterates only the
persisted list, so the dropped connection is invisible and never
re-established — even long after the outage ends, because a DOWN peer
recovers via port-unblock (no restart, no reconnect fan-out).

``reconnect_dropped_remote_devs`` closes the gap by diffing the persisted
list against cluster topology and reconnecting what should be there.

Invariants covered:

  1. A device on an ONLINE peer that is missing from remote_devices is
     reconnected via ``connect_device`` and appended to the persisted list
     -> (changed=True, all_ok=True).
  2. DOWN and UNREACHABLE peers are expected targets (matching
     ``health_controller._peer_connections_relevant``): connections to
     them SHOULD exist, so the reconnect is attempted.
  3. OFFLINE / RESTARTING peers are skipped entirely — no connect
     attempt, no health penalty (a restarting peer's own restart fans
     out reconnects when it completes).
  4. A failed connect to an expected device returns all_ok=False and does
     not append a bogus entry.
  5. Devices already present in remote_devices are left alone (no
     duplicate connect / append).
  6. Non-usable device statuses (unavailable, failed, ...) are not
     expected and not connected.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice
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


def _make_remote_entry(dev):
    r = RemoteDevice()
    r.uuid = dev.uuid
    r.alceml_name = dev.alceml_name
    r.node_id = dev.node_id
    r.size = dev.size
    r.status = NVMeDevice.STATUS_ONLINE
    r.nvmf_multipath = dev.nvmf_multipath
    r.remote_bdev = f"remote_{dev.alceml_bdev}n1"
    return r


class TestReconnectDroppedRemoteDevs(unittest.TestCase):

    def setUp(self):
        self.this_node = _make_node("node-a")

        patcher_db = patch.object(storage_node_ops, "DBController")
        self.mock_db_cls = patcher_db.start()
        self.addCleanup(patcher_db.stop)
        self.mock_db = self.mock_db_cls.return_value
        self.mock_db.get_storage_node_by_id.return_value = self.this_node

        patcher_connect = patch.object(storage_node_ops, "connect_device")
        self.mock_connect = patcher_connect.start()
        self.addCleanup(patcher_connect.stop)
        self.mock_connect.side_effect = (
            lambda name, dev, node, bdev_names, reattach: f"{name}n1")

    def _set_cluster(self, *peers):
        self.mock_db.get_storage_nodes_by_cluster_id.return_value = [
            self.this_node, *peers]

    def _run(self):
        return storage_node_ops.reconnect_dropped_remote_devs(
            self.this_node, node_bdev_names=[])

    def test_missing_device_on_online_peer_is_reconnected(self):
        peer = _make_node("node-b")
        dev = _make_dev("dev-1", "node-b")
        peer.nvme_devices = [dev]
        self._set_cluster(peer)

        changed, all_ok = self._run()

        self.assertTrue(changed)
        self.assertTrue(all_ok)
        self.mock_connect.assert_called_once()
        self.assertEqual(self.mock_connect.call_args.args[0], "remote_alceml_dev-1")
        self.assertEqual(len(self.this_node.remote_devices), 1)
        entry = self.this_node.remote_devices[0]
        self.assertEqual(entry.get_id(), "dev-1")
        self.assertEqual(entry.node_id, "node-b")
        self.assertEqual(entry.remote_bdev, "remote_alceml_dev-1n1")
        self.assertEqual(entry.status, NVMeDevice.STATUS_ONLINE)
        self.this_node.write_to_db.assert_called_once()

    def test_down_and_unreachable_peers_are_expected_targets(self):
        for status in (StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE):
            with self.subTest(status=status):
                self.mock_connect.reset_mock()
                self.this_node.remote_devices = []
                self.this_node.write_to_db.reset_mock()
                peer = _make_node("node-b", status=status)
                peer.nvme_devices = [_make_dev("dev-1", "node-b")]
                self._set_cluster(peer)

                changed, all_ok = self._run()

                self.assertTrue(changed)
                self.assertTrue(all_ok)
                self.mock_connect.assert_called_once()

    def test_offline_restarting_peers_are_skipped(self):
        for status in (StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_RESTARTING):
            with self.subTest(status=status):
                self.mock_connect.reset_mock()
                self.this_node.remote_devices = []
                peer = _make_node("node-b", status=status)
                peer.nvme_devices = [_make_dev("dev-1", "node-b")]
                self._set_cluster(peer)

                changed, all_ok = self._run()

                self.assertFalse(changed)
                self.assertTrue(all_ok)
                self.mock_connect.assert_not_called()

    def test_failed_connect_fails_health_without_appending(self):
        peer = _make_node("node-b")
        peer.nvme_devices = [_make_dev("dev-1", "node-b")]
        self._set_cluster(peer)
        self.mock_connect.side_effect = RuntimeError("connect failed")

        changed, all_ok = self._run()

        self.assertFalse(changed)
        self.assertFalse(all_ok)
        self.assertEqual(self.this_node.remote_devices, [])
        self.this_node.write_to_db.assert_not_called()

    def test_known_device_is_left_alone(self):
        peer = _make_node("node-b")
        dev = _make_dev("dev-1", "node-b")
        peer.nvme_devices = [dev]
        self.this_node.remote_devices = [_make_remote_entry(dev)]
        self._set_cluster(peer)

        changed, all_ok = self._run()

        self.assertFalse(changed)
        self.assertTrue(all_ok)
        self.mock_connect.assert_not_called()
        self.assertEqual(len(self.this_node.remote_devices), 1)

    def test_unusable_device_statuses_are_not_expected(self):
        peer = _make_node("node-b")
        peer.nvme_devices = [
            _make_dev("dev-1", "node-b", status=NVMeDevice.STATUS_UNAVAILABLE),
            _make_dev("dev-2", "node-b", status=NVMeDevice.STATUS_FAILED),
        ]
        self._set_cluster(peer)

        changed, all_ok = self._run()

        self.assertFalse(changed)
        self.assertTrue(all_ok)
        self.mock_connect.assert_not_called()

    def test_partial_failure_still_appends_successes(self):
        peer = _make_node("node-b")
        good = _make_dev("dev-good", "node-b")
        bad = _make_dev("dev-bad", "node-b")
        peer.nvme_devices = [good, bad]
        self._set_cluster(peer)

        def connect(name, dev, node, bdev_names, reattach):
            if dev.get_id() == "dev-bad":
                raise RuntimeError("connect failed")
            return f"{name}n1"
        self.mock_connect.side_effect = connect

        changed, all_ok = self._run()

        self.assertTrue(changed)
        self.assertFalse(all_ok)
        self.assertEqual(
            [d.get_id() for d in self.this_node.remote_devices], ["dev-good"])
        self.this_node.write_to_db.assert_called_once()


if __name__ == "__main__":
    unittest.main()
