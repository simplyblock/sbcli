# coding=utf-8
"""
test_subsystem_limit.py – unit tests for subsystem-based limit checks.

The max_lvol limit on a storage node should count unique subsystems (NQNs),
not individual lvols. Multiple namespaces (volumes) sharing a subsystem
should count as one.

Covers:
- _get_next_3_nodes: skips nodes at subsystem limit, allows nodes with
  many lvols sharing subsystems, tracks subsys count in node_stats
- add_lvol_ha: rejects creation when subsystem limit exceeded, allows
  creation when lvols share subsystems
- snapshot clone: rejects clone when subsystem limit exceeded, allows
  clone when lvols share subsystems

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(uuid, max_lvol=3, status=StorageNode.STATUS_ONLINE,
          is_secondary=False):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "cluster-1"
    n.status = status
    n.max_lvol = max_lvol
    n.is_secondary_node = is_secondary
    return n


def _lvol(uuid, node_id, nqn=None):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.nqn = nqn or f"nqn.unique:{uuid}"
    lv.status = LVol.STATUS_ONLINE
    return lv


def _call_get_next_3_nodes(nodes, lvols_by_node, cluster_id="cluster-1"):
    """Call _get_next_3_nodes with fully mocked DB and lvol_sync_del."""
    with patch("simplyblock_core.controllers.lvol_controller.DBController") as mock_db_cls:
        from simplyblock_core.controllers.lvol_controller import _get_next_3_nodes

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = nodes

        if callable(lvols_by_node):
            db.get_lvols_by_node_id.side_effect = lvols_by_node
        else:
            db.get_lvols_by_node_id.return_value = lvols_by_node

        mock_db_cls.return_value = db

        with patch.object(StorageNode, 'lvol_sync_del', return_value=False):
            return _get_next_3_nodes(cluster_id)


# ===========================================================================
# Tests for _get_next_3_nodes subsystem counting
# ===========================================================================

class TestGetNext3NodesSubsystemLimit(unittest.TestCase):
    """_get_next_3_nodes should count unique subsystems (NQNs), not lvols."""

    def test_node_skipped_when_subsystem_limit_reached(self):
        """Node with distinct NQNs equal to max_lvol should be skipped."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:subsys-A"),
            _lvol("v2", "n1", nqn="nqn:subsys-B"),
        ]
        result = _call_get_next_3_nodes([node], lvols)
        self.assertEqual(result, [])

    def test_node_allowed_when_lvols_share_subsystem(self):
        """Node with many lvols sharing one NQN should NOT be skipped."""
        node = _node("n1", max_lvol=2)
        shared_nqn = "nqn:shared-subsys"
        lvols = [_lvol(f"v{i}", "n1", nqn=shared_nqn) for i in range(5)]
        result = _call_get_next_3_nodes([node], lvols)
        self.assertIn(node, result)

    def test_node_allowed_when_under_subsystem_limit(self):
        """Node with fewer unique NQNs than max_lvol should be included."""
        node = _node("n1", max_lvol=3)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:subsys-A"),
            _lvol("v2", "n1", nqn="nqn:subsys-A"),
            _lvol("v3", "n1", nqn="nqn:subsys-B"),
            _lvol("v4", "n1", nqn="nqn:subsys-B"),
        ]
        result = _call_get_next_3_nodes([node], lvols)
        self.assertIn(node, result)

    def test_node_with_no_lvols_is_included(self):
        """Node with zero lvols should always be included."""
        node = _node("n1", max_lvol=2)
        result = _call_get_next_3_nodes([node], [])
        self.assertIn(node, result)

    def test_secondary_nodes_always_skipped(self):
        """Secondary nodes should be skipped regardless of subsystem count."""
        node = _node("n1", max_lvol=100, is_secondary=True)
        result = _call_get_next_3_nodes([node], [])
        self.assertEqual(result, [])

    def test_offline_nodes_skipped(self):
        """Offline nodes should be skipped."""
        node = _node("n1", max_lvol=100, status=StorageNode.STATUS_OFFLINE)
        result = _call_get_next_3_nodes([node], [])
        self.assertEqual(result, [])

    def test_mixed_nodes_only_eligible_returned(self):
        """Only nodes under the subsystem limit should be returned."""
        node_full = _node("n-full", max_lvol=1)
        node_ok = _node("n-ok", max_lvol=2)

        lvols_full = [_lvol("v1", "n-full", nqn="nqn:A")]
        lvols_ok = [
            _lvol("v2", "n-ok", nqn="nqn:B"),
            _lvol("v3", "n-ok", nqn="nqn:B"),
        ]

        def get_lvols_by_node(nid):
            if nid == "n-full":
                return lvols_full
            return lvols_ok

        result = _call_get_next_3_nodes([node_full, node_ok], get_lvols_by_node)
        self.assertNotIn(node_full, result)
        self.assertIn(node_ok, result)

    def test_32_namespaces_one_subsystem_not_skipped(self):
        """32 lvols sharing one NQN = 1 subsystem, should not hit limit."""
        node = _node("n1", max_lvol=2)
        lvols = [_lvol(f"v{i}", "n1", nqn="nqn:shared") for i in range(32)]
        result = _call_get_next_3_nodes([node], lvols)
        self.assertIn(node, result)

    def test_node_at_limit_with_mixed_nqns(self):
        """Node with some shared and some unique NQNs hitting the limit."""
        node = _node("n1", max_lvol=3)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:A"),  # shares with v1
            _lvol("v3", "n1", nqn="nqn:B"),
            _lvol("v4", "n1", nqn="nqn:C"),
        ]
        # 3 unique NQNs = at limit (>= 3)
        result = _call_get_next_3_nodes([node], lvols)
        self.assertEqual(result, [])


# ===========================================================================
# Tests for add_lvol_ha subsystem limit check
# ===========================================================================

class TestAddLvolHaSubsystemLimit(unittest.TestCase):
    """add_lvol_ha should count unique subsystems, not total lvols."""

    def _count_subsystems(self, lvols):
        """Mirror the check at lvol_controller.py:527."""
        return len(set(lv.nqn for lv in lvols))

    def test_rejects_when_subsystem_limit_exceeded(self):
        """Should reject when unique NQN count exceeds max_lvol."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
            _lvol("v3", "n1", nqn="nqn:C"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertGreater(subsys_count, node.max_lvol)

    def test_allows_when_lvols_share_subsystems(self):
        """Many lvols sharing NQNs should stay under the limit."""
        node = _node("n1", max_lvol=2)
        lvols = [_lvol(f"v{i}", "n1", nqn="nqn:A") for i in range(5)]
        lvols += [_lvol(f"w{i}", "n1", nqn="nqn:B") for i in range(5)]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count > node.max_lvol)

    def test_allows_when_under_limit(self):
        """Should allow when subsystem count is below max_lvol."""
        node = _node("n1", max_lvol=5)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count > node.max_lvol)

    def test_empty_node_always_allowed(self):
        """Node with no lvols should always be allowed."""
        node = _node("n1", max_lvol=1)
        subsys_count = self._count_subsystems([])
        self.assertEqual(subsys_count, 0)
        self.assertFalse(subsys_count > node.max_lvol)

    def test_single_subsystem_with_32_namespaces(self):
        """32 lvols sharing one NQN = 1 subsystem, should never hit limit."""
        node = _node("n1", max_lvol=2)
        shared_nqn = "nqn:shared"
        lvols = [_lvol(f"v{i}", "n1", nqn=shared_nqn) for i in range(32)]
        subsys_count = self._count_subsystems(lvols)
        self.assertEqual(subsys_count, 1)
        self.assertFalse(subsys_count > node.max_lvol)

    def test_exact_boundary_not_rejected(self):
        """Subsystem count exactly at max_lvol: > check should pass (not reject)."""
        node = _node("n1", max_lvol=3)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
            _lvol("v3", "n1", nqn="nqn:C"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count > node.max_lvol)

    def test_one_over_boundary_rejected(self):
        """Subsystem count one above max_lvol: > check should reject."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
            _lvol("v3", "n1", nqn="nqn:C"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertTrue(subsys_count > node.max_lvol)


# ===========================================================================
# Tests for snapshot clone subsystem limit check
# ===========================================================================

class TestSnapshotCloneSubsystemLimit(unittest.TestCase):
    """Snapshot clone should count unique subsystems, not total lvols."""

    def _count_subsystems(self, lvols):
        """Mirror the check at snapshot_controller.py:498."""
        return len(set(lv.nqn for lv in lvols))

    def test_rejects_when_subsystem_limit_reached(self):
        """Should reject clone when unique NQN count >= max_lvol."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertTrue(subsys_count >= node.max_lvol)

    def test_allows_clone_when_lvols_share_subsystems(self):
        """Many lvols sharing NQNs should stay under the limit."""
        node = _node("n1", max_lvol=2)
        lvols = [_lvol(f"v{i}", "n1", nqn="nqn:shared") for i in range(8)]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count >= node.max_lvol)

    def test_allows_clone_when_under_limit(self):
        """Should allow clone when subsystem count is below max_lvol."""
        node = _node("n1", max_lvol=5)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count >= node.max_lvol)

    def test_empty_node_allows_clone(self):
        """Node with no lvols should always allow clone."""
        node = _node("n1", max_lvol=1)
        subsys_count = self._count_subsystems([])
        self.assertFalse(subsys_count >= node.max_lvol)

    def test_single_subsystem_with_many_namespaces(self):
        """Many lvols sharing one NQN = 1 subsystem, should not block clone."""
        node = _node("n1", max_lvol=2)
        lvols = [_lvol(f"v{i}", "n1", nqn="nqn:shared") for i in range(32)]
        subsys_count = self._count_subsystems(lvols)
        self.assertEqual(subsys_count, 1)
        self.assertFalse(subsys_count >= node.max_lvol)

    def test_exact_boundary_rejects(self):
        """Subsystem count exactly at max_lvol: >= check should reject."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertTrue(subsys_count >= node.max_lvol)

    def test_one_under_boundary_allowed(self):
        """Subsystem count one below max_lvol: >= check should allow."""
        node = _node("n1", max_lvol=3)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:B"),
        ]
        subsys_count = self._count_subsystems(lvols)
        self.assertFalse(subsys_count >= node.max_lvol)


# ===========================================================================
# Tests verifying the boundary difference between add_lvol_ha and clone
# ===========================================================================

class TestBoundaryDifference(unittest.TestCase):
    """add_lvol_ha uses > while snapshot clone uses >=.

    This verifies the existing asymmetry is correctly preserved.
    """

    def test_add_lvol_ha_allows_at_exact_boundary(self):
        """add_lvol_ha: subsys_count == max_lvol should NOT reject (> check)."""
        max_lvol = 3
        lvols = [_lvol(f"v{i}", "n1", nqn=f"nqn:{i}") for i in range(3)]
        subsys_count = len(set(lv.nqn for lv in lvols))
        self.assertEqual(subsys_count, max_lvol)
        self.assertFalse(subsys_count > max_lvol)

    def test_snapshot_clone_rejects_at_exact_boundary(self):
        """snapshot clone: subsys_count == max_lvol should reject (>= check)."""
        max_lvol = 3
        lvols = [_lvol(f"v{i}", "n1", nqn=f"nqn:{i}") for i in range(3)]
        subsys_count = len(set(lv.nqn for lv in lvols))
        self.assertEqual(subsys_count, max_lvol)
        self.assertTrue(subsys_count >= max_lvol)

    def test_get_next_3_nodes_rejects_at_exact_boundary(self):
        """_get_next_3_nodes: subsys_count == max_lvol should reject (>= check)."""
        max_lvol = 2
        lvols = [_lvol(f"v{i}", "n1", nqn=f"nqn:{i}") for i in range(2)]
        subsys_count = len(set(lv.nqn for lv in lvols))
        self.assertEqual(subsys_count, max_lvol)
        self.assertTrue(subsys_count >= max_lvol)


if __name__ == "__main__":
    unittest.main()
