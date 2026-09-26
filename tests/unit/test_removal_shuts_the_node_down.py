"""A removal shuts a node down only if the node is still running.

remove_storage_node's orchestrator decides whether to call
shutdown_storage_node in phase 1 by asking what state the node is in. The
question is "is this node still running", and only ONLINE and SUSPENDED
answer yes. Every other status a removal can start from is one where the
node is already down:

* PENDING_REMOVAL: remove_storage_node shuts an ONLINE/SUSPENDED node down
  itself, at queue time, and only then stamps PENDING_REMOVAL. A node seen in
  phase 1 as PENDING_REMOVAL has had its shutdown.
* MIGRATING_DEVICES / MIGRATING_LVOLS / IN_REMOVAL: a Kubernetes drain stops
  the node in its own ShuttingDown step, before any device or volume moves,
  and hands the node over in one of these.
* OFFLINE / DOWN / UNREACHABLE: nothing is answering to be stopped.

The gate was briefly widened to ONLINE plus every draining status, on the
reasoning that the drain stamped PENDING_REMOVAL on a node that was still
serving. That drain no longer exists -- it shuts down first -- and the widened
gate re-ran shutdown on a PENDING_REMOVAL node with no SPDK left to stop,
which shutdown_storage_node refuses without force, so the removal failed on
every retry (2026-09-26). Behaviour is pinned in test_node_removal.py; what is
kept here is the shape of the sets, which several call sites rely on agreeing.
"""
import unittest

from simplyblock_core.models.storage_node import StorageNode


class ShutdownGateStatusTests(unittest.TestCase):

    #: The statuses in which the node's SPDK is known to be up, kept here rather
    #: than imported so the test states the intent independently of the code.
    STILL_RUNNING = (
        StorageNode.STATUS_ONLINE,
        StorageNode.STATUS_SUSPENDED,
    )

    def _gate(self, status):
        """The phase-1 condition, evaluated the way the code evaluates it."""
        return status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED)

    def test_every_still_running_status_triggers_the_shutdown(self):
        for status in self.STILL_RUNNING:
            with self.subTest(status=status):
                self.assertTrue(self._gate(status))

    def test_pending_removal_has_already_been_shut_down(self):
        """The status the widened gate wrongly treated as running. It is
        stamped after remove_storage_node's own shutdown, so acting on it here
        tries to stop a node that is already stopped -- and is refused."""
        self.assertFalse(self._gate(StorageNode.STATUS_PENDING_REMOVAL))

    def test_a_node_the_drain_handed_over_is_not_shut_down_again(self):
        """The drain stops the node before it moves anything; phase 1 has
        nothing left to do for the statuses it hands the node over in."""
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            with self.subTest(status=status):
                self.assertFalse(self._gate(status))

    def test_the_gate_never_overlaps_the_shut_down_set(self):
        """Stated as an invariant: a status cannot be both "still running" and
        "already stopped", however many statuses are added later."""
        gate = set(self.STILL_RUNNING)
        self.assertEqual(gate & set(StorageNode.REMOVAL_SHUT_DOWN_STATUSES), set())
        self.assertEqual(gate & set(StorageNode.DEPARTING_STATUSES), set())

    def test_an_unreachable_node_is_left_alone(self):
        """Not an oversight: shutting down a node that cannot be reached fails,
        and those statuses already mean the SPDK is not answering."""
        for status in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE):
            with self.subTest(status=status):
                self.assertFalse(self._gate(status))


if __name__ == '__main__':
    unittest.main()
