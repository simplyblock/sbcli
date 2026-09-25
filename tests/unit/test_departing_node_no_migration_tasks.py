"""No migration work may be queued against a node that is leaving.

A device-migration task executes ON the node it is queued for. A node on its
way out is suspended or shut down, so such a task can never run: the runner
parks it with "node is not online, retrying" (tasks_runner_failed_migration.py)
and it stays suspended for ever, while the device it belongs to never reaches
FAILED_AND_MIGRATED and the removal waits on it.

add_device_failed_mig_task already said this in a comment -- naming IN_REMOVAL
explicitly and predicting it "would stall the node-removal completion check
forever" -- but only tested STATUS_REMOVED. That held while device failure
happened at the very end of a removal, after the status flip and after the
node's lvstore had been torn down: it had no distrib left to queue against.

Failing the devices FIRST, as the Kubernetes drain does, makes the gap
reachable again. Observed live on a 7-node cluster (2026-09-25): six tasks
queued on the node being drained, all parked on "node is not online, retrying",
the device phase stuck at "0 of 3 devices rebuilt" indefinitely.

Adapted from the same test on the main line, minus the two statuses this branch
does not have (MIGRATING_LVOLS, REMOVED_FAILED).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.lvstore_stack = [{"type": "bdev_distr", "name": f"distr_{node_id}"}]
    node.secondary_node_id = ""
    node.tertiary_node_id = ""
    return node


class TestDepartingNodesAreSkipped(unittest.TestCase):

    def _queued_for(self, statuses, fn):
        device = MagicMock()
        device.cluster_id = "c1"
        device.node_id = "n0"
        device.get_id.return_value = "dev-1"
        nodes = [_node(f"n{i}", s) for i, s in enumerate(statuses)]
        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        db.get_storage_node_by_id.side_effect = lambda i: next(
            (n for n in nodes if n.get_id() == i), _node(i, StorageNode.STATUS_ONLINE))
        added = []
        with patch.object(tasks_controller, "db", db), \
             patch.object(tasks_controller, "_add_task",
                          side_effect=lambda *a, **k: added.append(a[2])):
            fn("dev-1")
        return added

    def test_failed_migration_skips_every_departing_status(self):
        added = self._queued_for(
            [StorageNode.STATUS_ONLINE] + list(StorageNode.DEPARTING_STATUSES),
            tasks_controller.add_device_failed_mig_task)
        self.assertEqual(
            added, ["n0"],
            "only the ONLINE node may get a migration task; a departing node "
            "cannot run one, so queueing it strands work nothing will do")

    def test_new_device_migration_skips_them_too(self):
        added = self._queued_for(
            [StorageNode.STATUS_ONLINE] + list(StorageNode.DEPARTING_STATUSES),
            tasks_controller.add_new_device_mig_task)
        self.assertEqual(added, ["n0"])

    def test_pending_removal_is_skipped(self):
        """The status the drain stamps before it fails the devices, and the one
        that made the long-documented gap reachable on this branch."""
        added = self._queued_for(
            [StorageNode.STATUS_ONLINE, StorageNode.STATUS_PENDING_REMOVAL],
            tasks_controller.add_device_failed_mig_task)
        self.assertEqual(added, ["n0"])

    def test_in_removal_is_skipped(self):
        """Named in the original comment, never actually tested for."""
        added = self._queued_for(
            [StorageNode.STATUS_ONLINE, StorageNode.STATUS_IN_REMOVAL],
            tasks_controller.add_device_failed_mig_task)
        self.assertEqual(added, ["n0"])

    def test_healthy_nodes_still_get_their_tasks(self):
        added = self._queued_for(
            [StorageNode.STATUS_ONLINE, StorageNode.STATUS_ONLINE],
            tasks_controller.add_device_failed_mig_task)
        self.assertEqual(added, ["n0", "n1"])


class TestDepartingStatusSet(unittest.TestCase):

    def test_it_covers_every_removal_state_this_branch_has(self):
        for status in (StorageNode.STATUS_PENDING_REMOVAL,
                       StorageNode.STATUS_IN_REMOVAL,
                       StorageNode.STATUS_REMOVED):
            self.assertIn(status, StorageNode.DEPARTING_STATUSES)

    def test_it_excludes_states_a_node_can_return_from(self):
        """A node that is merely down or suspended may come back, and should
        still be given its migration work when it does."""
        for status in (StorageNode.STATUS_ONLINE,
                       StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_SUSPENDED,
                       StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_RESTARTING):
            self.assertNotIn(status, StorageNode.DEPARTING_STATUSES)


if __name__ == '__main__':
    unittest.main()
