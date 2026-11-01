"""The stale-fence remediation stops reopening a port it has already reopened
FENCE_RELEASE_BUDGET times in one window (prod ams1, 2026-11-01).

_remediate_stale_port_blocks exists to lift a fence SPDK applied and nobody
released -- a one-off. On 2026-11-01 the fence was not a leak: a stripe of
LVS_4033 could not be served while one of its nodes rebooted, SPDK re-fenced
port 9113 on every retried read, and this remediation (plus the port_allow
tasks the health check kept scheduling) reopened it within 12-17 s each time.
The client's retry then moved leadership and the fence to a peer: 60
leadership switches and 144 failed reads in nine minutes, and the churn drove
a peer's SPDK into a self-abort.

These tests cover the monitor's side of the gate. The ledger it consults is
tests/unit/models/test_fence_release_ledger.py; the budget state itself lives
in FDB because three processes share it, so the DB-backed wrapper
(controllers/port_release_budget.py) is patched here, not faked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor
from tests._mocks import route_fences

PORT = 9113


def _node(node_id="d3a07483", status=StorageNode.STATUS_DOWN):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.lvstore = "LVS_4033"
    node.cluster_id = "cl-1"
    return node


class _Harness:
    """One fenced port, aged past the threshold, every other gate open; only
    the release budget varies."""

    def __init__(self, exhausted):
        self.node = _node()
        self.db = MagicMock()
        self.db.get_storage_node_by_id.return_value = self.node
        self.exhausted = exhausted
        self.unblocked = []

    def run(self):
        storage_node_monitor._blocked_port_since.clear()
        hc = storage_node_monitor.health_controller
        fb = storage_node_monitor.port_release_budget
        route_fences([self.node], lambda n, p, **kw: self.unblocked.append(p))
        with patch.object(hc, "_restart_owns_lvs", return_value=False), \
             patch.object(hc, "_check_node_hublvol", return_value=True), \
\
             patch.object(fb, "exhausted", return_value=self.exhausted) as exhausted, \
             patch.object(fb, "refuse") as refuse, \
             patch.object(fb, "record_release") as record, \
             patch.object(storage_node_monitor.time, "monotonic",
                          side_effect=[0.0, 100.0]):
            storage_node_monitor._remediate_stale_port_blocks(
                self.db, self.node, {PORT: False}, {PORT: self.node.get_id()})
        self.exhausted_calls = exhausted.call_args_list
        self.refused = refuse.call_args_list
        self.recorded = record.call_args_list
        return self.unblocked


class TestTheBudgetGate(unittest.TestCase):

    def test_with_budget_the_fence_is_lifted_and_the_release_recorded(self):
        h = _Harness(exhausted=False)
        self.assertEqual(h.run(), [PORT])
        self.assertEqual(len(h.recorded), 1)
        db, node, port = h.recorded[0].args
        self.assertIs(db, h.db)
        self.assertIs(node, h.node)
        self.assertEqual(port, PORT)
        self.assertEqual(h.refused, [])

    def test_without_budget_the_fence_stays_and_the_loop_is_reported(self):
        """The regression. The third reopen in ten minutes is the last one."""
        h = _Harness(exhausted=True)
        self.assertEqual(h.run(), [], "the port must stay fenced")
        self.assertEqual(h.recorded, [], "nothing was released, nothing to record")
        self.assertEqual(len(h.refused), 1)
        db, node, port, lvstore, held = h.refused[0].args
        self.assertIs(node, h.node)
        self.assertEqual((port, lvstore), (PORT, "LVS_4033"))
        self.assertGreaterEqual(held, storage_node_monitor.STALE_PORT_BLOCK_SEC)

    def test_the_budget_is_asked_for_this_node_and_port(self):
        h = _Harness(exhausted=True)
        h.run()
        self.assertEqual(len(h.exhausted_calls), 1)
        db, node, port = h.exhausted_calls[0].args
        self.assertIs(db, h.db)
        self.assertIs(node, h.node)
        self.assertEqual(port, PORT)

    def test_a_refused_port_keeps_its_first_seen_stamp(self):
        """So the next tick does not have to age the fence past the threshold
        again before it can re-evaluate the budget."""
        h = _Harness(exhausted=True)
        h.run()
        self.assertIn((h.node.get_id(), PORT), storage_node_monitor._blocked_port_since)


if __name__ == "__main__":
    unittest.main()
