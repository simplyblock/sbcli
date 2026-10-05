"""Every "is this node leaving?" question derives its answer from one place.

Six call sites each kept their own hand-written list of removal statuses. When
node-removal grew MIGRATING_LVOLS and REMOVED_FAILED, five of them were not
updated, and each failed in its own way on cluster a6e7569d (2026-09-15):

  * _PEER_DISCONNECTED_STATUSES missed MIGRATING_LVOLS, so the lvol-migration
    runner fell through to the JM quorum -- which abstains on every peer once
    the departing node's remote_jm bdevs are gone, tallies 0/0, and votes
    "connected" -- then spent two retry rounds RPC-ing a SnodeAPI hostname that
    no longer resolves.
  * distr_controller sent the raw string 'migrating_lvols' to the data plane,
    whose JSON decoder rejects the whole event batch on an unknown status and
    drops it silently: 106 of 194 status events discarded over 14 minutes.

These tests pin the derivation, not the current membership, so the next status
added to the removal flow reaches every consumer or fails here.
"""
import unittest

from simplyblock_core import cluster_ops, distr_controller, storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


class TestTheTwoSets(unittest.TestCase):

    def test_shut_down_is_departing_minus_pending_removal(self):
        """PENDING_REMOVAL is the one departing status a node can still be
        serving in -- it is stamped when the removal is requested, before the
        orchestrator's shutdown step runs."""
        self.assertEqual(
            set(StorageNode.REMOVAL_SHUT_DOWN_STATUSES),
            set(StorageNode.DEPARTING_STATUSES) - {StorageNode.STATUS_PENDING_REMOVAL})

    def test_departing_covers_every_removal_state(self):
        for status in (StorageNode.STATUS_PENDING_REMOVAL,
                       StorageNode.STATUS_MIGRATING_LVOLS,
                       StorageNode.STATUS_IN_REMOVAL,
                       StorageNode.STATUS_REMOVED,
                       StorageNode.STATUS_REMOVED_FAILED):
            self.assertIn(status, StorageNode.DEPARTING_STATUSES)

    def test_neither_set_claims_a_node_that_can_come_back(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_RESTARTING):
            self.assertNotIn(status, StorageNode.DEPARTING_STATUSES)
            self.assertNotIn(status, StorageNode.REMOVAL_SHUT_DOWN_STATUSES)


class TestConsumersDerive(unittest.TestCase):

    def test_peer_disconnected_covers_every_shut_down_status(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertIn(status, storage_node_ops._PEER_DISCONNECTED_STATUSES,
                          f"a peer in {status} has no SPDK to answer an RPC")
        # ...and still the two non-removal ways a peer goes unreachable.
        self.assertIn(StorageNode.STATUS_OFFLINE,
                      storage_node_ops._PEER_DISCONNECTED_STATUSES)
        self.assertIn(StorageNode.STATUS_UNREACHABLE,
                      storage_node_ops._PEER_DISCONNECTED_STATUSES)

    def test_peer_disconnected_excludes_pending_removal(self):
        """A node in PENDING_REMOVAL may still be up. Calling it disconnected
        would skip the port-block that stops it writing."""
        self.assertNotIn(StorageNode.STATUS_PENDING_REMOVAL,
                         storage_node_ops._PEER_DISCONNECTED_STATUSES)

    def test_grace_shutdown_skips_shut_down_nodes_only(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertTrue(cluster_ops._grace_shutdown_skipped(_node(status)),
                            f"{status}: removal already owns this node")
        self.assertFalse(
            cluster_ops._grace_shutdown_skipped(
                _node(StorageNode.STATUS_PENDING_REMOVAL)),
            "a node still serving must be shut down like any other member")
        self.assertFalse(
            cluster_ops._grace_shutdown_skipped(_node(StorageNode.STATUS_ONLINE)))


class TestDataPlaneVocabulary(unittest.TestCase):
    """The data plane drops a whole event batch on a status string it cannot
    decode, so nothing outside its vocabulary may reach it."""

    def test_every_departing_status_is_canonicalised(self):
        for status in StorageNode.DEPARTING_STATUSES:
            if status == StorageNode.STATUS_REMOVED:
                continue  # the data plane knows this one
            self.assertEqual(
                distr_controller.data_plane_node_status(status),
                StorageNode.STATUS_UNREACHABLE,
                f"{status} would be sent verbatim and silently dropped")

    def test_migrating_lvols_specifically(self):
        """The first status the decoder rejected, 2026-09-15 17:20:03."""
        self.assertEqual(
            distr_controller.data_plane_node_status(
                StorageNode.STATUS_MIGRATING_LVOLS),
            StorageNode.STATUS_UNREACHABLE)

    def test_removed_failed_specifically(self):
        self.assertEqual(
            distr_controller.data_plane_node_status(
                StorageNode.STATUS_REMOVED_FAILED),
            StorageNode.STATUS_UNREACHABLE)

    def test_removed_is_passed_through(self):
        """Excluded on purpose: the data plane names it and always has."""
        self.assertEqual(
            distr_controller.data_plane_node_status(StorageNode.STATUS_REMOVED),
            StorageNode.STATUS_REMOVED)

    def test_serving_statuses_are_untouched(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE):
            self.assertEqual(distr_controller.data_plane_node_status(status), status)

    def test_health_check_canonicalises_the_same_set_plus_transients(self):
        for status in distr_controller._DATA_PLANE_UNKNOWN_NODE_STATUSES:
            self.assertIn(status, distr_controller._HEALTH_CHECK_NOT_SERVING_STATUSES)
        for status in (StorageNode.STATUS_RESTARTING,
                       StorageNode.STATUS_IN_SHUTDOWN):
            self.assertIn(status, distr_controller._HEALTH_CHECK_NOT_SERVING_STATUSES)


def _node(status):
    node = StorageNode()
    node.status = status
    return node


if __name__ == "__main__":
    unittest.main()
