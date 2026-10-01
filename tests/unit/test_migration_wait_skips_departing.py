"""A suspended migration must not wait for a node that is being removed.

_cluster_unavailable_state feeds _migration_retry_allowed, which holds a
device-migration task suspended until something on the list recovers. A node
under removal never will, so listing it is a wait for an event that cannot
happen -- and it deadlocked the removal it was waiting on.

Cluster a6e7569d, 2026-09-15: recovering a node started balancing_on_restart
with 10 device_migration subtasks; all 10 suspended on
"waiting for unavailable nodes/devices to recover ... ['node:a1b050f1-...]" --
the node being removed. The master never completed, the cluster stayed
ACTIVE-REBALANCING, create_migration refused ("Cluster is rebalancing"), the
removal's drain could never start, and so the node could never finish leaving.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_migration as runner


def _node(node_id, status, dev_status=NVMeDevice.STATUS_ONLINE):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    dev = MagicMock()
    dev.get_id.return_value = f"{node_id}-dev"
    dev.status = dev_status
    node.nvme_devices = [dev]
    return node


def _unavailable(nodes):
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    with patch.object(runner, "db", db):
        return runner._cluster_unavailable_state("c1")


class TestDepartingNodesAreNotWaitedFor(unittest.TestCase):

    def test_every_departing_status_is_skipped(self):
        for status in StorageNode.DEPARTING_STATUSES:
            self.assertEqual(
                _unavailable([_node("leaving", status,
                                    NVMeDevice.STATUS_FAILED_AND_MIGRATED)]),
                [], f"{status}: this node is never coming back")

    def test_migrating_lvols_specifically(self):
        """The status that produced the live deadlock."""
        self.assertEqual(
            _unavailable([_node("a1b050f1", StorageNode.STATUS_MIGRATING_LVOLS,
                                NVMeDevice.STATUS_FAILED_AND_MIGRATED)]),
            [])

    def test_a_departing_nodes_devices_are_skipped_too(self):
        """Even a device left in a non-online, non-migrated state on a
        departing node must not hold a migration open."""
        self.assertEqual(
            _unavailable([_node("leaving", StorageNode.STATUS_IN_REMOVAL,
                                NVMeDevice.STATUS_UNAVAILABLE)]),
            [])


class TestRealOutagesAreStillWaitedFor(unittest.TestCase):

    def test_a_node_that_can_come_back_is_still_listed(self):
        for status in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE,
                       StorageNode.STATUS_RESTARTING):
            self.assertEqual(_unavailable([_node("n1", status)]),
                             ["node:n1"], status)

    def test_a_failed_device_on_a_healthy_node_is_still_listed(self):
        self.assertEqual(
            _unavailable([_node("n1", StorageNode.STATUS_ONLINE,
                                NVMeDevice.STATUS_UNAVAILABLE)]),
            ["dev:n1-dev"])

    def test_in_creation_is_still_skipped(self):
        """Not gone -- not yet arrived."""
        self.assertEqual(
            _unavailable([_node("n1", StorageNode.STATUS_IN_CREATION)]), [])

    def test_a_healthy_cluster_waits_for_nothing(self):
        self.assertEqual(
            _unavailable([_node("n1", StorageNode.STATUS_ONLINE),
                          _node("n2", StorageNode.STATUS_SUSPENDED)]), [])


if __name__ == "__main__":
    unittest.main()
