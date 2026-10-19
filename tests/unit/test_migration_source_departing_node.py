"""Invariants over the node-status sets the removal flow reads.

The behavioural half of this file has moved to
test_migration_source_during_drain.py. It asserted that start_migration decides
by comparing the primary's status to MIGRATION_SOURCE_STATUSES, and that is no
longer how the decision is made: every removal shuts the node down before
moving its volumes, so the primary is never up by then and the source side is
served by an online replica (resolve_source_node). Asking the primary's own
status refused every migration a drain issued.

What survives here is what that change did not touch -- the shape of the sets
themselves, which several call sites depend on agreeing.
"""
import unittest

from simplyblock_core.models.storage_node import StorageNode


class SourceStatusSetTests(unittest.TestCase):

    def test_the_allow_list_and_the_shut_down_set_cannot_overlap(self):
        """Stated as an invariant rather than a list, so adding a drain status
        later (migrating_devices, migrating_lvols) cannot quietly admit a node
        whose SPDK is already stopped."""
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertNotIn(status, StorageNode.MIGRATION_SOURCE_STATUSES)

    def test_a_draining_node_is_never_one_whose_spdk_has_stopped(self):
        """The two sets answer different questions -- "still serving?" and
        "already gone?" -- and a status in both would be read as serving by one
        caller and as gone by the next."""
        self.assertEqual(
            set(StorageNode.DRAINING_STATUSES) & set(StorageNode.REMOVAL_SHUT_DOWN_STATUSES),
            set())

    def test_pending_removal_is_departing_but_still_serving(self):
        """The two facts that together make this bug possible, pinned so the
        next reader sees why one status sits on both sides of the line."""
        self.assertIn(StorageNode.STATUS_PENDING_REMOVAL,
                      StorageNode.DEPARTING_STATUSES)
        self.assertNotIn(StorageNode.STATUS_PENDING_REMOVAL,
                         StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
        self.assertIn(StorageNode.STATUS_PENDING_REMOVAL,
                      StorageNode.MIGRATION_SOURCE_STATUSES)


if __name__ == '__main__':
    unittest.main()
