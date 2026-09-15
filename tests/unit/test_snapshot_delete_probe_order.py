"""Don't ask a torn-down node first when hunting for the lvstore leader.

snapshot_controller._delete_locked detects the leader by RPC rather than by
status -- deliberately, so leadership is never inferred from a stale status
field. But it probed the snapshot's home node first, and during a node drain
that home node is the one being removed: its SPDK is stopped and its DNS name
no longer resolves, so the probe spends the full SnodeAPI retry budget
(3 attempts, ~6s) before the exception is swallowed and the secondary is tried.

Live 2026-09-15, every intermediate-snapshot cleanup during a drain:

    Requesting method: bdev_lvol_get_lvstores ... bd4qf ...
    Retrying (total=2) ... Failed to resolve '...bd4qf...' [Errno -2]
    Retrying (total=1) ... Failed to resolve ...
    Retrying (total=0) ... Failed to resolve ...
    (6s later) ... d8tj5 ... "lvs leadership": true  -> delete succeeds

Ordered, not filtered: every candidate is still probed, so this cannot select
a different leader than before -- only the sequence changes.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    return n


def _probe_order(statuses, leader=None):
    """Return the order candidates are probed for leadership."""
    nodes = [_node(nid, st) for nid, st in statuses]
    probed = []

    def _is_leader(cand, lvs):
        probed.append(cand.get_id())
        if cand.get_id() == leader:
            return True
        if cand.status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            raise RuntimeError("name does not resolve")
        return False

    all_nodes = list(nodes)
    all_nodes.sort(
        key=lambda n: n.status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
    for cand in all_nodes:
        try:
            if _is_leader(cand, "LVS_1"):
                break
        except Exception:
            continue
    return probed


class TestProbeOrder(unittest.TestCase):

    def test_a_departing_home_node_is_probed_last(self):
        order = _probe_order([
            ("home", StorageNode.STATUS_MIGRATING_LVOLS),
            ("secondary", StorageNode.STATUS_ONLINE),
        ], leader="secondary")
        self.assertEqual(order, ["secondary"],
                         "the reachable replica answers first; the torn-down "
                         "home node is never probed at all")

    def test_every_shut_down_status_is_deprioritised(self):
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            order = _probe_order([("home", status),
                                  ("secondary", StorageNode.STATUS_ONLINE)],
                                 leader="secondary")
            self.assertEqual(order, ["secondary"], status)

    def test_a_normal_offline_home_node_is_still_probed_first(self):
        """OFFLINE is not a removal: the node may still answer, and the
        original 'no status checks' contract applies."""
        order = _probe_order([("home", StorageNode.STATUS_OFFLINE),
                              ("secondary", StorageNode.STATUS_ONLINE)],
                             leader="secondary")
        self.assertEqual(order, ["home", "secondary"])

    def test_a_departing_node_is_still_probed_if_nothing_else_answers(self):
        """Ordered, not filtered -- it remains a candidate of last resort."""
        order = _probe_order([("home", StorageNode.STATUS_IN_REMOVAL),
                              ("secondary", StorageNode.STATUS_ONLINE)],
                             leader=None)
        self.assertEqual(order, ["secondary", "home"])

    def test_order_is_stable_when_nothing_is_departing(self):
        order = _probe_order([("home", StorageNode.STATUS_OFFLINE),
                              ("secondary", StorageNode.STATUS_ONLINE),
                              ("tertiary", StorageNode.STATUS_ONLINE)],
                             leader="tertiary")
        self.assertEqual(order, ["home", "secondary", "tertiary"])


class TestTheSortIsInTheCode(unittest.TestCase):

    def test_delete_locked_orders_its_candidates(self):
        import inspect
        src = inspect.getsource(snapshot_controller._delete_locked)
        self.assertIn("REMOVAL_SHUT_DOWN_STATUSES", src)
        self.assertIn("all_nodes.sort(", src)


if __name__ == "__main__":
    unittest.main()
