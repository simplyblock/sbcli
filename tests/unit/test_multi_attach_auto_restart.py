"""Auto-restart admission on multi-attach clusters: they run 1+0 (npcs 0) and
tolerate ``max_fault_tolerance`` node losses, so two nodes down at FTT 2 must
each be restarted (2026-11-20 FTT2 bed: network cut + SPDK kill left both
offline for good, each blocking the other). Regular clusters keep the npcs
rule."""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller as tc
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


def _node(nid, status):
    n = StorageNode()
    n.uuid = nid
    n.cluster_id = "c1"
    n.status = status
    n.mgmt_ip = f"10.0.0.{len(nid)}{nid[-1]}"
    return n


def _cluster(multi, ftt, npcs):
    c = Cluster()
    c.uuid = "c1"
    c.status = Cluster.STATUS_DEGRADED
    c.multi_attach = multi
    c.max_fault_tolerance = ftt
    c.distr_ndcs, c.distr_npcs = (1, npcs)
    return c


class TestAutoRestartTolerance(unittest.TestCase):

    def _queue(self, cluster, offline_others):
        nodes = [_node("n0", StorageNode.STATUS_OFFLINE)]
        nodes += [_node(f"n{i + 1}", StorageNode.STATUS_OFFLINE) for i in range(offline_others)]
        nodes += [_node(f"m{i}", StorageNode.STATUS_ONLINE) for i in range(3)]
        db = MagicMock()
        db.get_storage_node_by_id.return_value = nodes[0]
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        with patch.object(tc, "db", db), \
                patch.object(tc, "is_auto_restart_paused", return_value=False), \
                patch.object(tc, "_add_task", return_value="task") as add:
            tc.add_node_to_auto_restart(nodes[0])
        return add.called

    def test_multi_attach_ftt2_restarts_with_another_node_down(self):
        self.assertTrue(self._queue(_cluster(True, 2, 0), offline_others=1))

    def test_multi_attach_ftt2_holds_beyond_its_tolerance(self):
        # Same rule as npcs on a regular cluster: held once more OTHER nodes
        # are down than the cluster tolerates.
        self.assertTrue(self._queue(_cluster(True, 2, 0), offline_others=2))
        self.assertFalse(self._queue(_cluster(True, 2, 0), offline_others=3))

    def test_multi_attach_ftt1(self):
        self.assertTrue(self._queue(_cluster(True, 1, 0), offline_others=1))
        self.assertFalse(self._queue(_cluster(True, 1, 0), offline_others=2))

    def test_regular_cluster_keeps_the_npcs_rule(self):
        self.assertTrue(self._queue(_cluster(False, 1, 1), offline_others=1))
        self.assertFalse(self._queue(_cluster(False, 2, 1), offline_others=2))


if __name__ == "__main__":
    unittest.main()
