"""Cluster status on a multi-attach (1+0 array LUN) cluster: a lost node is
covered by the LUN takeover, so the node-loss tolerance is the cluster's FTT,
not its (zero) parity. Found on the multi-attach bed: the first node loss
suspended the cluster, which shut the other nodes down, takeover standby
included."""
import unittest
from unittest.mock import MagicMock

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services import storage_node_monitor as mon


def _cluster(multi_attach=True, ftt=1, ndcs=1, npcs=0):
    return MagicMock(multi_attach=multi_attach, max_fault_tolerance=ftt, distr_ndcs=ndcs,
                     distr_npcs=npcs, enable_failure_domain=False,
                     strict_node_anti_affinity=False, migration_ft_impact=None)


def _verdict(cluster, affected, online_nodes=4, online_devices=4):
    return mon._status_verdict_rules(cluster, [], set(range(affected)), online_nodes,
                                     online_devices, [])


class TestMultiAttachNodeTolerance(unittest.TestCase):

    def test_healthy_is_active(self):
        self.assertEqual(_verdict(_cluster(), 0), Cluster.STATUS_ACTIVE)

    def test_one_lost_node_with_ftt1_is_degraded(self):
        self.assertEqual(_verdict(_cluster(), 1, online_nodes=3), Cluster.STATUS_DEGRADED)

    def test_two_lost_nodes_with_ftt1_suspend(self):
        # each LUN is attached to two nodes only
        self.assertEqual(_verdict(_cluster(), 2, online_nodes=2), Cluster.STATUS_SUSPENDED)

    def test_two_lost_nodes_with_ftt2_is_degraded(self):
        self.assertEqual(_verdict(_cluster(ftt=2), 2, online_nodes=2), Cluster.STATUS_DEGRADED)

    def test_regular_no_parity_cluster_unchanged(self):
        # 1+0 without multi-attach: no takeover, a lost node suspends as before
        self.assertEqual(_verdict(_cluster(multi_attach=False), 1, online_nodes=3),
                         Cluster.STATUS_SUSPENDED)

    def test_regular_parity_cluster_unchanged(self):
        self.assertEqual(_verdict(_cluster(multi_attach=False, ndcs=2, npcs=1), 1, online_nodes=3),
                         Cluster.STATUS_DEGRADED)
        self.assertEqual(_verdict(_cluster(multi_attach=False, ndcs=2, npcs=1), 2, online_nodes=2),
                         Cluster.STATUS_SUSPENDED)


if __name__ == "__main__":
    unittest.main()
