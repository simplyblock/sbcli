"""ANA failover fires once per offline episode, not once per monitor cycle.

Incident 2026-08-09 (iteration 28): the monitor's OFFLINE branch re-ran
``trigger_ana_failover_for_node`` on EVERY cycle for as long as a node stayed
offline, producing 2789 ``nvmf_subsystem_listener_set_ana_state`` RPCs in 16
minutes (a flat 170/min, one per subsystem per cycle) against a single peer,
then another ~320 against a second peer.

Each of those calls is a real ``spdk_nvmf_subsystem_pause`` of a live
subsystem: ``rpc_nvmf_subsystem_listener_set_ana_state`` pauses before it
reaches the "state already matches" short-circuit, so an ANA set that changes
nothing still quiesces the subsystem. Reading the current state first is not
an option either — ``nvmf_subsystem_get_listeners`` pauses too. The only fix
is not to make the call.

The promotion itself is idempotent in effect, so once per transition is
sufficient; the marker is cleared as soon as the node is seen in any
non-OFFLINE state so a later outage re-arms it.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import storage_node_monitor


class TestAnaFailoverOnce(unittest.TestCase):

    def setUp(self):
        storage_node_monitor._ana_failover_applied.clear()
        self.addCleanup(storage_node_monitor._ana_failover_applied.clear)

        self.node = MagicMock()
        self.node.get_id.return_value = "node-a"

        p = patch.object(storage_node_monitor, "storage_node_ops")
        self.ops = p.start()
        self.addCleanup(p.stop)

    def test_first_call_triggers(self):
        self.assertTrue(storage_node_monitor._ana_failover_once(self.node))
        self.ops.trigger_ana_failover_for_node.assert_called_once_with(self.node)

    def test_repeated_calls_do_not_retrigger(self):
        storage_node_monitor._ana_failover_once(self.node)
        for _ in range(50):
            self.assertFalse(storage_node_monitor._ana_failover_once(self.node))
        self.assertEqual(
            self.ops.trigger_ana_failover_for_node.call_count, 1,
            "a node that stays offline must not be re-promoted every cycle")

    def test_rearm_allows_a_later_offline_episode_to_trigger(self):
        storage_node_monitor._ana_failover_once(self.node)
        storage_node_monitor._ana_failover_rearm("node-a")
        self.assertTrue(storage_node_monitor._ana_failover_once(self.node))
        self.assertEqual(self.ops.trigger_ana_failover_for_node.call_count, 2)

    def test_nodes_are_tracked_independently(self):
        other = MagicMock()
        other.get_id.return_value = "node-b"
        self.assertTrue(storage_node_monitor._ana_failover_once(self.node))
        self.assertTrue(storage_node_monitor._ana_failover_once(other))
        self.assertFalse(storage_node_monitor._ana_failover_once(self.node))
        self.assertEqual(self.ops.trigger_ana_failover_for_node.call_count, 2)

    def test_rearm_of_unknown_node_is_a_noop(self):
        storage_node_monitor._ana_failover_rearm("never-seen")  # must not raise

    def test_a_failover_that_could_not_promote_does_not_consume_the_episode(self):
        """2026-10-14 run 63: the primary was shut down for removal while its
        first secondary was itself restarting after a crash. The trigger
        skipped (nothing online to promote onto), the marker was set anyway,
        and the volumes stayed unserved until a manual promotion. A skipped
        call must leave the episode open so the next cycle retries."""
        self.ops.trigger_ana_failover_for_node.return_value = False
        self.assertFalse(storage_node_monitor._ana_failover_once(self.node))
        self.assertFalse(storage_node_monitor._ana_failover_once(self.node))
        self.assertEqual(self.ops.trigger_ana_failover_for_node.call_count, 2,
                         "a skipped promotion must be retried, not recorded as done")
        self.assertNotIn("node-a", storage_node_monitor._ana_failover_applied)

        self.ops.trigger_ana_failover_for_node.return_value = True
        self.assertTrue(storage_node_monitor._ana_failover_once(self.node))
        self.assertFalse(storage_node_monitor._ana_failover_once(self.node))
        self.assertEqual(self.ops.trigger_ana_failover_for_node.call_count, 3)


class TestTriggerReportsAttemptedNotSucceeded(unittest.TestCase):
    """The trigger's return value drives the one-shot: False only when nothing
    was attempted (not a primary, or the first secondary is not online), True
    for any attempt, including one whose flips failed -- a failed attempt must
    keep consuming the episode, or a peer that cannot take the flips is hammered
    every cycle (2026-08-09 storm)."""

    def setUp(self):
        from simplyblock_core import storage_node_ops
        self.ops = storage_node_ops
        self.node = MagicMock()
        self.node.get_id.return_value = "primary-a"
        self.node.secondary_node_id = "secondary-b"

    def test_not_a_primary_is_not_an_attempt(self):
        self.node.secondary_node_id = ""
        with patch.object(self.ops, "_failover_primary_ana") as fo:
            self.assertFalse(self.ops.trigger_ana_failover_for_node(self.node))
            fo.assert_not_called()

    def test_a_skipped_promotion_is_not_an_attempt(self):
        with patch.object(self.ops, "_failover_primary_ana", return_value=False):
            self.assertFalse(self.ops.trigger_ana_failover_for_node(self.node))

    def test_an_issued_promotion_is_an_attempt(self):
        with patch.object(self.ops, "_failover_primary_ana", return_value=True):
            self.assertTrue(self.ops.trigger_ana_failover_for_node(self.node))

    def test_a_failed_attempt_still_counts_as_the_one_shot(self):
        with patch.object(self.ops, "_failover_primary_ana", side_effect=RuntimeError("peer rpc down")):
            self.assertTrue(self.ops.trigger_ana_failover_for_node(self.node))


class TestRemovalStatesAreCovered(unittest.TestCase):
    """check_node owes the same one-shot failover to a node whose removal has
    already shut its SPDK down: such a node never returns through OFFLINE, the
    drain stamps MIGRATING_DEVICES over it, and without this branch a lost
    shutdown-time promotion is never retried (2026-10-14 run 63)."""

    def test_check_node_routes_shut_down_removal_states_to_the_one_shot(self):
        import inspect

        from simplyblock_core.models.storage_node import StorageNode

        src = inspect.getsource(storage_node_monitor.check_node)
        removal_branch = src.index("REMOVAL_SHUT_DOWN_STATUSES")
        generic_skip = src.index("STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN")
        self.assertLess(removal_branch, generic_skip,
                        "the removal branch must run before the generic skip "
                        "that returns for every non-live status")
        self.assertIn("_ana_failover_once(snode)", src[removal_branch:generic_skip])
        # The set is the one that means "SPDK already stopped": a node that is
        # merely PENDING_REMOVAL may still be serving and must not be failed over.
        self.assertNotIn(StorageNode.STATUS_PENDING_REMOVAL, StorageNode.REMOVAL_SHUT_DOWN_STATUSES)


if __name__ == "__main__":
    unittest.main()
