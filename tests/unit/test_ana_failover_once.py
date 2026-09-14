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


if __name__ == "__main__":
    unittest.main()
