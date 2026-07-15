# coding=utf-8
"""Unit tests for the no-leader fail-fast gate (2026-07-15).

Mass create/delete run 20260712-231123 left LVS_1 leaderless for hours after a
cluster-wide abort; every lvol/snapshot create re-ran the full leader
probe/recovery machinery, storming each LVS member with ~61k
bdev_lvol_get_lvstores RPCs. The fix:

  * find_leader_with_failover records a "no leader" verdict in the shared
    no_leader_cache and fails fast (no probing) for NO_LEADER_TTL_SEC.
  * snapshot_controller._find_lvs_leader honors the same negative cache and
    delegates its scan-miss to find_leader_with_failover (single recovery
    pass per window).
  * snapshot create / clone reject the request when there is no leader
    instead of attempting it on the configured primary.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import ttl_cache


def _node(uuid="n1", cluster_id="c1"):
    n = MagicMock(spec=StorageNode)
    n.uuid = uuid
    n.get_id = MagicMock(return_value=uuid)
    n.cluster_id = cluster_id
    n.status = StorageNode.STATUS_ONLINE
    return n


def _read_snapshot_controller_src():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    with open(os.path.join(root, "simplyblock_core/controllers/snapshot_controller.py")) as f:
        return f.read()


class TestFindLeaderNoLeaderGate(unittest.TestCase):
    def setUp(self):
        ttl_cache.no_leader_cache.invalidate()
        ttl_cache.leader_cache.invalidate()
        from simplyblock_core import storage_node_ops
        self.mod = storage_node_ops
        self.nodes = [_node("n1"), _node("n2"), _node("n3")]

    def tearDown(self):
        ttl_cache.no_leader_cache.invalidate()
        ttl_cache.leader_cache.invalidate()

    def test_no_leader_verdict_is_cached_and_fails_fast(self):
        with patch.object(self.mod, "_find_leader_with_failover_impl",
                          return_value=(None, [])) as impl:
            leader, non_leaders = self.mod.find_leader_with_failover(self.nodes, "LVS_1")
            self.assertIsNone(leader)
            self.assertEqual(non_leaders, [])
            self.assertEqual(impl.call_count, 1)

            # Second call inside the TTL window must not re-run the probe pass.
            leader, non_leaders = self.mod.find_leader_with_failover(self.nodes, "LVS_1")
            self.assertIsNone(leader)
            self.assertEqual(non_leaders, [])
            self.assertEqual(impl.call_count, 1)

    def test_negative_cache_is_per_lvs(self):
        with patch.object(self.mod, "_find_leader_with_failover_impl",
                          return_value=(None, [])) as impl:
            self.mod.find_leader_with_failover(self.nodes, "LVS_1")
            self.mod.find_leader_with_failover(self.nodes, "LVS_2")
            self.assertEqual(impl.call_count, 2)

    def test_confirmed_leader_clears_negative_verdict(self):
        key = ("c1", "LVS_1")
        ttl_cache.no_leader_cache.put(key, True)
        # Simulate the TTL expiring so the gate lets one pass through.
        ttl_cache.no_leader_cache.invalidate(key)
        with patch.object(self.mod, "_find_leader_with_failover_impl",
                          return_value=(self.nodes[0], self.nodes[1:])):
            leader, _ = self.mod.find_leader_with_failover(self.nodes, "LVS_1")
        self.assertIs(leader, self.nodes[0])
        self.assertIsNone(
            ttl_cache.no_leader_cache.get(key, ttl_cache.NO_LEADER_TTL_SEC))


class TestSnapshotFindLvsLeaderGate(unittest.TestCase):
    def setUp(self):
        ttl_cache.no_leader_cache.invalidate()
        ttl_cache.leader_cache.invalidate()
        from simplyblock_core.controllers import snapshot_controller
        self.mod = snapshot_controller
        self.nodes = [_node("n1"), _node("n2"), _node("n3")]

    def tearDown(self):
        ttl_cache.no_leader_cache.invalidate()
        ttl_cache.leader_cache.invalidate()

    def test_negative_cache_short_circuits_before_any_probe(self):
        ttl_cache.no_leader_cache.put(("c1", "LVS_1"), True)
        from simplyblock_core.controllers import lvol_controller
        with patch.object(lvol_controller, "is_node_leader") as probe:
            self.assertIsNone(
                self.mod._find_lvs_leader("c1", "LVS_1", self.nodes))
            probe.assert_not_called()

    def test_scan_miss_delegates_to_failover_helper(self):
        from simplyblock_core.controllers import lvol_controller
        from simplyblock_core import storage_node_ops
        with patch.object(lvol_controller, "is_node_leader", return_value=False), \
             patch.object(storage_node_ops, "find_leader_with_failover",
                          return_value=(None, [])) as failover:
            self.assertIsNone(
                self.mod._find_lvs_leader("c1", "LVS_1", self.nodes))
            failover.assert_called_once_with(self.nodes, "LVS_1")

    def test_scan_hit_returns_leader_without_delegation(self):
        from simplyblock_core.controllers import lvol_controller
        from simplyblock_core import storage_node_ops
        with patch.object(lvol_controller, "is_node_leader",
                          side_effect=[False, True]), \
             patch.object(storage_node_ops, "find_leader_with_failover") as failover:
            leader = self.mod._find_lvs_leader("c1", "LVS_1", self.nodes)
            self.assertIs(leader, self.nodes[1])
            failover.assert_not_called()


def _function_source(src, name):
    """Slice out one top-level function body (up to the next top-level def)."""
    start = src.index(f"\ndef {name}(")
    end = src.find("\ndef ", start + 1)
    return src[start:end if end != -1 else len(src)]


class TestCreatePathsRejectOnNoLeader(unittest.TestCase):
    """Source-level invariants: the create paths must never fall back to the
    configured primary when leader detection returns None. (The delete path
    keeps its fallback deliberately — deletes must stay possible under force.)"""

    def test_snapshot_create_does_not_fall_back_to_host_node(self):
        src = _function_source(_read_snapshot_controller_src(), "add")
        self.assertNotIn(
            "primary_node = host_node", src,
            "snapshot create must reject on no-leader, not attempt on host_node")
        self.assertIn("rejecting snapshot create until leadership", src)

    def test_clone_does_not_fall_back_to_host_node(self):
        src = _function_source(_read_snapshot_controller_src(), "clone")
        self.assertNotIn(
            "primary_node = host_node", src,
            "clone must reject on no-leader, not attempt on host_node")
        self.assertIn("rejecting clone until leadership", src)


if __name__ == "__main__":
    unittest.main()
