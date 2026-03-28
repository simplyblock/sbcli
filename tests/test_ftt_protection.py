# coding=utf-8
"""
test_ftt_protection.py – unit tests for _check_ftt_allows_node_removal.

Tests cover all FTT scenarios:
  - FTT=1 (npcs=1): block if any node not online or journal replicating
  - FTT=2 (npcs=2, ft=2): allow one not-online, block at two
  - npcs=2, ft=1: like FTT=2 plus primary-secondary pair constraint
  - Rebalancing: block based on headroom (active_nodes - ndcs - npcs)
  - Journal replication counts as one additional not-online node
  - REMOVED and IN_CREATION nodes are excluded from counts
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.storage_node_ops import _check_ftt_allows_node_removal


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(ha_type="ha", npcs=1, ndcs=2, ft=1, rebalancing=False):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.ha_type = ha_type
    cl.distr_npcs = npcs
    cl.distr_ndcs = ndcs
    cl.max_fault_tolerance = ft
    cl.is_re_balancing = rebalancing
    cl.status = Cluster.STATUS_ACTIVE
    return cl


def _node(node_id, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          secondary_id="", secondary_id_2="", jm_vuid=8881, lvstore="LVS_1"):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = cluster_id
    n.secondary_node_id = secondary_id
    n.secondary_node_id_2 = secondary_id_2
    n.jm_vuid = jm_vuid
    n.lvstore = lvstore
    n.mgmt_ip = f"10.0.0.{hash(node_id) % 256}"
    # rpc_client mock: journal replication not active by default
    rpc = MagicMock()
    rpc.bdev_lvol_get_lvstores = MagicMock(return_value=[{"name": lvstore}])
    rpc.jc_get_jm_status = MagicMock(return_value={"jm1": True})
    n.rpc_client = MagicMock(return_value=rpc)
    return n


def _db(cluster, nodes):
    db = MagicMock()
    db.get_cluster_by_id = MagicMock(return_value=cluster)
    db.get_storage_nodes_by_cluster_id = MagicMock(return_value=nodes)

    def get_node(nid):
        for n in nodes:
            if n.get_id() == nid:
                return n
        raise KeyError(nid)
    db.get_storage_node_by_id = MagicMock(side_effect=get_node)
    return db


def _set_jm_replicating(node):
    """Make a node report active journal replication."""
    rpc = node.rpc_client()
    rpc.jc_get_jm_status.return_value = {"jm1": False}


# ---------------------------------------------------------------------------
# Non-HA clusters
# ---------------------------------------------------------------------------

class TestNonHA(unittest.TestCase):

    def test_single_mode_always_allowed(self):
        cl = _cluster(ha_type="single")
        n1 = _node("n1")
        db = _db(cl, [n1])
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# FTT=1 (npcs=1)
# ---------------------------------------------------------------------------

class TestFTT1(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_one_offline_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=1", reason)

    def test_one_suspended_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_SUSPENDED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=1", reason)

    def test_one_down_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_DOWN)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)

    def test_journal_replication_blocks_removal(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        _set_jm_replicating(nodes[1])  # n2 is replicating
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("journal replication", reason)

    def test_removed_node_does_not_count_as_offline(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_REMOVED)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_in_creation_node_does_not_count_as_offline(self):
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2"), _node("n3", status=StorageNode.STATUS_IN_CREATION)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_target_node_not_counted_in_offline(self):
        """The node being removed should not count against itself."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        db = _db(cl, nodes)
        # n1 is being removed — n2 is online, so this should be allowed
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# FTT=2 (npcs=2, ft=2)
# ---------------------------------------------------------------------------

class TestFTT2(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node(f"n{i}") for i in range(5)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed)

    def test_one_offline_allows_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE), _node("n5")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_two_offline_blocks_removal(self):
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE),
                 _node("n5", status=StorageNode.STATUS_SUSPENDED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("FTT=2", reason)

    def test_one_offline_plus_jm_replication_blocks(self):
        """One offline + journal replication = effective count of 2 → block."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE), _node("n5")]
        _set_jm_replicating(nodes[1])  # n2 replicating
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("journal replication", reason)

    def test_jm_replication_alone_allows_one_removal(self):
        """Journal replication = effective 1 not-online → still room for one more in FTT=2."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [_node(f"n{i}") for i in range(5)]
        _set_jm_replicating(nodes[1])
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed)


# ---------------------------------------------------------------------------
# npcs=2, ft=1 (primary-secondary pair constraint)
# ---------------------------------------------------------------------------

class TestNpcs2Ft1(unittest.TestCase):

    def test_all_online_allows_removal(self):
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"), _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_unrelated_offline_allows_removal(self):
        """An offline node that is NOT the pair of the target → allowed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_OFFLINE), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_secondary_offline_blocks_primary_removal(self):
        """If n1's secondary (n2) is offline, n1 cannot be removed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2"),
                 _node("n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("secondary", reason)
        self.assertIn("n2", reason)

    def test_primary_offline_blocks_secondary_removal(self):
        """If primary n1 is offline, its secondary n2 cannot be removed."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", status=StorageNode.STATUS_OFFLINE),
                 _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n2", db)
        self.assertFalse(allowed)
        self.assertIn("secondary of not-online primary", reason)
        self.assertIn("n1", reason)

    def test_secondary_id_2_offline_blocks(self):
        """Also works for secondary_node_id_2."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n2", secondary_id_2="n3"),
                 _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("n3", reason)

    def test_two_unrelated_offline_blocks(self):
        """Two offline nodes (even if unrelated to pair) → blocked by capacity rule."""
        cl = _cluster(npcs=2, ft=1)
        nodes = [_node("n1", secondary_id="n5"),
                 _node("n2"), _node("n3", status=StorageNode.STATUS_OFFLINE),
                 _node("n4", status=StorageNode.STATUS_SUSPENDED), _node("n5")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("npcs=2/ft=1", reason)


# ---------------------------------------------------------------------------
# Rebalancing
# ---------------------------------------------------------------------------

class TestRebalancing(unittest.TestCase):

    def test_rebalancing_exact_minimum_nodes_blocks(self):
        """ndcs=2, npcs=1: minimum=3. With exactly 3 nodes and rebalancing → block."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("rebalancing", reason)

    def test_rebalancing_one_above_minimum_allows(self):
        """ndcs=2, npcs=1: minimum=3. With 4 nodes and rebalancing → allowed."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_rebalancing_one_above_min_but_one_offline_blocks(self):
        """ndcs=2, npcs=1: 4 nodes, 1 offline, rebalancing → headroom=1, not_online=1 → block."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("rebalancing", reason)

    def test_rebalancing_two_above_min_one_offline_allows_ftt2(self):
        """ndcs=2, npcs=2, ft=2: 6 nodes, 1 offline, rebalancing → headroom=2, not_online=1 → allowed."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3"), _node("n4"),
                 _node("n5"), _node("n6", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_rebalancing_npcs2_exact_minimum_blocks(self):
        """ndcs=2, npcs=2: minimum=4. With exactly 4 nodes and rebalancing → block."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3"), _node("n4")]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("rebalancing", reason)

    def test_rebalancing_npcs2_one_above_allows_first(self):
        """ndcs=2, npcs=2: 5 nodes, rebalancing → headroom=1, not_online=0 → allowed."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, rebalancing=True)
        nodes = [_node(f"n{i}") for i in range(5)]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n0", db)
        self.assertTrue(allowed)

    def test_rebalancing_npcs2_one_above_blocks_second(self):
        """ndcs=2, npcs=2: 5 nodes, 1 offline, rebalancing → headroom=1, not_online=1 → block."""
        cl = _cluster(npcs=2, ndcs=2, ft=2, rebalancing=True)
        nodes = [_node("n1"), _node("n2"), _node("n3"), _node("n4"),
                 _node("n5", status=StorageNode.STATUS_OFFLINE)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("rebalancing", reason)

    def test_not_rebalancing_exact_minimum_allows(self):
        """Same scenario but NOT rebalancing → rebalancing check doesn't apply."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=False)
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_rebalancing_removed_nodes_excluded(self):
        """Removed nodes don't count as active, reducing headroom."""
        cl = _cluster(npcs=1, ndcs=2, ft=1, rebalancing=True)
        # 4 nodes but one is removed → 3 active → exact minimum → block
        nodes = [_node("n1"), _node("n2"), _node("n3"),
                 _node("n4", status=StorageNode.STATUS_REMOVED)]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("rebalancing", reason)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases(unittest.TestCase):

    def test_node_not_found(self):
        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(side_effect=KeyError("nope"))
        allowed, reason = _check_ftt_allows_node_removal("missing", db)
        self.assertFalse(allowed)
        self.assertIn("not found", reason)

    def test_jm_rpc_failure_treated_as_no_replication(self):
        """If RPC to check journal status fails, assume no replication (don't block)."""
        cl = _cluster(npcs=1, ft=1)
        nodes = [_node("n1"), _node("n2")]
        nodes[1].rpc_client().jc_get_jm_status.side_effect = Exception("RPC failed")
        db = _db(cl, nodes)
        allowed, _ = _check_ftt_allows_node_removal("n1", db)
        self.assertTrue(allowed)

    def test_multiple_offline_statuses(self):
        """Various not-online statuses all count: offline, suspended, down, unreachable."""
        cl = _cluster(npcs=2, ft=2)
        nodes = [
            _node("n1"),
            _node("n2", status=StorageNode.STATUS_OFFLINE),
            _node("n3", status=StorageNode.STATUS_DOWN),
            _node("n4"), _node("n5"),
        ]
        db = _db(cl, nodes)
        allowed, reason = _check_ftt_allows_node_removal("n1", db)
        self.assertFalse(allowed)
        self.assertIn("2 not-online", reason)


if __name__ == "__main__":
    unittest.main()
