"""A target replica that is leaving must not block the migration for ever.

_get_target_secondary_node / _get_target_tertiary_node classify the target's
replica: ONLINE use it, OFFLINE skip silently, SUSPENDED-and-is-the-source
continue (overlap drain), anything else BLOCK creation on the target primary.

Every removal status fell into "anything else". A node the removal has shut
down cannot register anything and is never coming back, so blocking on it
blocks permanently -- and because this is the TARGET's replica, it blocks
migrations between two entirely healthy nodes for the whole duration of any
node removal.

Live 2026-09-16, cluster 5aaf0a5d, migrating healthy rpksz -> healthy zqhjg:

    Target secondary node b1d65620-...-a973a640e39b is in state
    'migrating_lvols'; cannot create on target primary

suspended indefinitely, while neither endpoint had anything to do with the
node being removed.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_lvol_migration as runner


def _tgt(sec_id="sec", ter_id=None):
    n = MagicMock()
    n.secondary_node_id = sec_id
    n.tertiary_node_id = ter_id
    return n


def _replica(status, node_id="sec"):
    r = MagicMock()
    r.status = status
    r.get_id.return_value = node_id
    return r


def _secondary(status, src_node_id=None):
    db = MagicMock()
    db.get_storage_node_by_id.return_value = _replica(status)
    with patch.object(runner, "db", db):
        return runner._get_target_secondary_node(_tgt(), src_node_id)


def _tertiary(status, src_node_id=None):
    db = MagicMock()
    db.get_storage_node_by_id.return_value = _replica(status, "ter")
    with patch.object(runner, "db", db):
        return runner._get_target_tertiary_node(_tgt(sec_id=None, ter_id="ter"), src_node_id)


class TestALeavingReplicaIsSkipped(unittest.TestCase):

    def test_secondary_every_shut_down_status_skips(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            node, err = _secondary(status)
            self.assertIsNone(node, status)
            self.assertIsNone(err, f"{status} must not block the migration")

    def test_tertiary_every_shut_down_status_skips(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            node, err = _tertiary(status)
            self.assertIsNone(node, status)
            self.assertIsNone(err, f"{status} must not block the migration")

    def test_migrating_lvols_specifically(self):
        """The exact status from the live failure."""
        node, err = _secondary(StorageNode.STATUS_MIGRATING_LVOLS)
        self.assertIsNone(node)
        self.assertIsNone(err)


class TestTheOtherClassificationsAreUnchanged(unittest.TestCase):

    def test_online_replica_is_used(self):
        node, err = _secondary(StorageNode.STATUS_ONLINE)
        self.assertIsNotNone(node)
        self.assertIsNone(err)

    def test_offline_still_skips_silently(self):
        self.assertEqual(_secondary(StorageNode.STATUS_OFFLINE), (None, None))

    def test_overlap_drain_still_continues_through_a_suspended_source(self):
        node, err = _secondary(StorageNode.STATUS_SUSPENDED, src_node_id="sec")
        self.assertIsNotNone(node)
        self.assertIsNone(err)

    def test_a_genuinely_unexpected_state_still_blocks(self):
        """The catch-all must keep catching. RESTARTING is transient and the
        migration should wait for it rather than silently drop the replica."""
        node, err = _secondary(StorageNode.STATUS_RESTARTING)
        self.assertIsNone(node)
        self.assertIsNotNone(err)
        self.assertIn("cannot create on target primary", err)

    def test_no_replica_configured_is_still_fine(self):
        db = MagicMock()
        with patch.object(runner, "db", db):
            self.assertEqual(
                runner._get_target_secondary_node(_tgt(sec_id=None), None),
                (None, None))


if __name__ == "__main__":
    unittest.main()
