"""test_classify_existing_endpoint_record.py - unit tests for
_classify_existing_endpoint_record.

Regression coverage for the 2026-08-06 gr5kf incident: a pod restart
mid-onboarding (a transient node NotReady blip restarting the storage-node
DaemonSet pod before the backend's own online-match safeguard applies) can
leave a stale in_creation record behind for the SAME endpoint+SSDs even
after a LATER add attempt for that host has gone fully online. The
function must always classify that stale record for cleanup, regardless
of whether another matching record is already online, and regardless of
which one the underlying iteration happens to encounter first.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import _classify_existing_endpoint_record


def _node(node_id, status, api_endpoint="10.0.0.1", ssd_pcie=None):
    n = MagicMock(spec=StorageNode)
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.api_endpoint = api_endpoint
    n.ssd_pcie = ssd_pcie if ssd_pcie is not None else ["0000:02:00.0"]
    return n


def _db(nodes):
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id = MagicMock(return_value=nodes)
    return db


class TestClassifyExistingEndpointRecord(unittest.TestCase):

    def test_no_match_returns_none(self):
        db = _db([])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertIsNone(action)
        self.assertIsNone(node)

    def test_online_match_is_already_added(self):
        online = _node("n1", StorageNode.STATUS_ONLINE)
        db = _db([online])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertEqual(action, "already_added")
        self.assertEqual(node.get_id(), "n1")

    def test_in_creation_match_is_cleanup(self):
        stale = _node("n1", StorageNode.STATUS_IN_CREATION)
        db = _db([stale])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertEqual(action, "cleanup")
        self.assertEqual(node.get_id(), "n1")

    def test_other_status_is_conflict(self):
        offline = _node("n1", StorageNode.STATUS_UNREACHABLE)
        db = _db([offline])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertEqual(action, "conflict")
        self.assertEqual(node.get_id(), "n1")

    def test_online_and_stale_in_creation_both_present_prioritizes_cleanup(self):
        # order: online first, in_creation second -- must still classify
        # cleanup, not stop at the first (online) match.
        online = _node("n1", StorageNode.STATUS_ONLINE)
        stale = _node("n2", StorageNode.STATUS_IN_CREATION)
        db = _db([online, stale])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertEqual(action, "cleanup")
        self.assertEqual(node.get_id(), "n2")

    def test_stale_in_creation_and_online_both_present_reverse_order_still_prioritizes_cleanup(self):
        # order reversed: in_creation first, online second -- confirms the
        # priority isn't merely an artifact of iteration order.
        stale = _node("n1", StorageNode.STATUS_IN_CREATION)
        online = _node("n2", StorageNode.STATUS_ONLINE)
        db = _db([stale, online])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertEqual(action, "cleanup")
        self.assertEqual(node.get_id(), "n1")

    def test_different_endpoint_is_ignored(self):
        other = _node("n1", StorageNode.STATUS_ONLINE, api_endpoint="10.0.0.9")
        db = _db([other])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertIsNone(action)
        self.assertIsNone(node)

    def test_different_ssds_is_ignored(self):
        other = _node("n1", StorageNode.STATUS_ONLINE, ssd_pcie=["0000:0b:00.0"])
        db = _db([other])
        action, node = _classify_existing_endpoint_record(db, "c1", "10.0.0.1", ["0000:02:00.0"])
        self.assertIsNone(action)
        self.assertIsNone(node)


if __name__ == "__main__":
    unittest.main()