"""Port checks on multi-attach clusters (1+0): a port SPDK reports blocked is
its own fence, and one bad device fences every LVS of the cluster at once
(2026-11-20 FTT2 bed: a stale error event marked a taken-over LUN
unavailable; three healthy nodes were set DOWN within 4 s, the node floor
broke and the cluster suspended).

  A. the node's own port gets the fence grace peer-owned ports have;
  B. more nodes than the cluster tolerates failing only the port check at
     once are not set DOWN.
Regular clusters keep the 1 s re-read and set DOWN as before."""

import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services import storage_node_monitor as snm
from tests.unit.test_port_check_deliberate_fence import OWN_PORT, _node


def _cluster(multi, ftt=2):
    c = Cluster()
    c.uuid = "c1"
    c.status = Cluster.STATUS_ACTIVE
    c.multi_attach = multi
    c.max_fault_tolerance = ftt
    return c


class TestOwnPortGrace(unittest.TestCase):

    def setUp(self):
        snm._blocked_port_since.clear()
        self.addCleanup(snm._blocked_port_since.clear)

    def _check(self, cluster, reads):
        node = _node("n1")
        db = MagicMock()
        db.get_primary_storage_nodes_by_secondary_node_id.return_value = []
        db.get_cluster_by_id.return_value = cluster
        reads = list(reads)

        def _ports(_n, ports):
            own = reads.pop(0) if reads else True
            return {p: (own if p == OWN_PORT else True) for p in ports}
        sleep = MagicMock()
        with patch.object(snm, "db", db), \
                patch.object(snm.health_controller, "check_ports_on_node", side_effect=_ports), \
                patch.object(snm, "_remediate_stale_port_blocks"), \
                patch.object(snm.time, "sleep", sleep):
            return snm.node_port_check_fun(node), sleep

    def test_fresh_own_fence_is_not_counted(self):
        verdict, sleep = self._check(_cluster(True), [False])
        self.assertTrue(verdict)
        sleep.assert_not_called()

    def test_own_fence_past_the_grace_counts(self):
        snm._blocked_port_since[("n1", OWN_PORT)] = time.monotonic() - snm.PEER_PORT_FENCE_GRACE_SEC - 1
        verdict, _ = self._check(_cluster(True), [False])
        self.assertFalse(verdict)

    def test_regular_cluster_keeps_the_re_read(self):
        verdict, sleep = self._check(_cluster(False), [False, False])
        self.assertFalse(verdict)
        sleep.assert_called_once_with(snm.PORT_BLOCK_CONFIRM_SEC)


class TestCorrelatedPortFailure(unittest.TestCase):

    def setUp(self):
        snm._port_failed_at.clear()
        snm._correlated_event_at.clear()
        self.addCleanup(snm._port_failed_at.clear)

    def _fail(self, *nids):
        for nid in nids:
            snm._note_port_check(_node(nid), False)

    def test_within_tolerance_the_node_goes_down(self):
        self._fail("n1", "n2")
        self.assertEqual(snm._correlated_port_failure(_cluster(True, ftt=2), _node("n2")), "")

    def test_beyond_tolerance_no_node_goes_down(self):
        self._fail("n1", "n2", "n3")
        with patch.object(snm.storage_events, "snode_port_check_correlated") as ev:
            reason = snm._correlated_port_failure(_cluster(True, ftt=2), _node("n3"))
            snm._correlated_port_failure(_cluster(True, ftt=2), _node("n1"))
        self.assertIn("3 nodes fail only their port check", reason)
        ev.assert_called_once()  # rate-limited per cluster

    def test_a_passing_check_clears_the_node(self):
        self._fail("n1", "n2", "n3")
        snm._note_port_check(_node("n1"), True)
        self.assertEqual(snm._correlated_port_failure(_cluster(True, ftt=2), _node("n2")), "")

    def test_old_failures_age_out(self):
        self._fail("n1", "n2", "n3")
        for nid in ("n1", "n2"):
            cid, _t = snm._port_failed_at[nid]
            snm._port_failed_at[nid] = (cid, time.monotonic() - snm.PORT_FAILURE_CORRELATION_SEC - 1)
        self.assertEqual(snm._correlated_port_failure(_cluster(True, ftt=2), _node("n3")), "")

    def test_regular_cluster_never_holds(self):
        self._fail("n1", "n2", "n3", "n4")
        self.assertEqual(snm._correlated_port_failure(_cluster(False, ftt=1), _node("n1")), "")


if __name__ == "__main__":
    unittest.main()
