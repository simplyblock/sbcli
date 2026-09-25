"""A removal must shut the node down before it dismantles it.

remove_storage_node's phase 1 decides whether to call shutdown_storage_node by
asking what state the node is in. It used to ask for ONLINE or SUSPENDED, which
was exhaustive only because a removal could reach that line from exactly two
places: a live node, or a re-entry that had already moved the status past
shutdown.

The Kubernetes drain reaches it from a third. It stamps PENDING_REMOVAL before
failing the devices -- the node is still up and serving -- and then calls
remove_storage_node for the final teardown. PENDING_REMOVAL is neither ONLINE
nor SUSPENDED nor past-shutdown, so phase 1 was skipped and the node was
dismantled with its SPDK running: JM decommissioned, replicas relocated off it,
devices torn down underneath a target that was still answering.

The condition is "is this node still running", so it is asked as ONLINE plus
every draining status.
"""
import unittest

from simplyblock_core.models.storage_node import StorageNode


class ShutdownGateStatusTests(unittest.TestCase):

    #: The statuses phase 1 must act on, kept here rather than imported so the
    #: test states the intent independently of the expression under test.
    STILL_RUNNING = (
        StorageNode.STATUS_ONLINE,
        StorageNode.STATUS_SUSPENDED,
        StorageNode.STATUS_PENDING_REMOVAL,
    )

    def _gate(self, status):
        """The phase-1 condition, evaluated the way the code evaluates it."""
        return status in (StorageNode.STATUS_ONLINE,) + StorageNode.DRAINING_STATUSES

    def test_every_still_running_status_triggers_the_shutdown(self):
        for status in self.STILL_RUNNING:
            with self.subTest(status=status):
                self.assertTrue(
                    self._gate(status),
                    f"a node in {status} is still serving, so a removal that "
                    "skips phase 1 tears it down with its SPDK running")

    def test_pending_removal_specifically(self):
        """The drain's status, and the one the original pair missed."""
        self.assertTrue(self._gate(StorageNode.STATUS_PENDING_REMOVAL))

    def test_a_node_already_shut_down_is_not_shut_down_again(self):
        """Re-entry after the removal has already stopped the SPDK: phase 1 has
        nothing left to do, and calling it again would fail the removal."""
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            with self.subTest(status=status):
                self.assertFalse(self._gate(status))

    def test_the_gate_and_the_shut_down_set_are_complementary(self):
        """Stated as an invariant: a status cannot be both "still running" and
        "already stopped", however many statuses are added later."""
        gate = set((StorageNode.STATUS_ONLINE,) + StorageNode.DRAINING_STATUSES)
        self.assertEqual(gate & set(StorageNode.REMOVAL_SHUT_DOWN_STATUSES), set())

    def test_an_unreachable_node_is_left_alone(self):
        """Not an oversight: shutting down a node that cannot be reached fails,
        and those statuses already mean the SPDK is not answering."""
        for status in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE):
            with self.subTest(status=status):
                self.assertFalse(self._gate(status))


if __name__ == '__main__':
    unittest.main()
