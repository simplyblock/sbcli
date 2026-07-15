# coding=utf-8
"""
test_subsystem_limit.py – unit tests for subsystem-based limit checks.

``max_lvol`` on a storage node caps the number of PRIMARY lvol subsystems:
distinct NQNs among the lvols whose ``node_id`` is that node. Secondary and
tertiary replica subsystems are intentionally NOT counted — the node's memory
reservation already provisions for the replica subsystems it hosts for other
nodes' lvols. Namespaced volumes share a subsystem (one NQN) and count as one.

Covers:
- count_lvol_subsystems: distinct primary NQNs; excludes other nodes' lvols
  (i.e. replicas hosted for them) and lvols in deletion
- _get_next_3_nodes: skips nodes at the subsystem limit; keeps at-limit nodes
  for namespaced creates when an existing subsystem has a free namespace
  slot; prefers nodes with free namespace slots for namespaced creates
- _resolve_lvol_subsystem: namespaced lvols join an existing subsystem
  without consuming a subsystem slot (cap not enforced); creating a new
  subsystem enforces the cap

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
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


def _lvol(uuid, node_id, nqn=None, max_ns=1, status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.nqn = nqn or f"nqn.unique:{uuid}"
    lv.status = status
    lv.max_namespace_per_subsys = max_ns
    return lv


def _cluster(nqn="nqn.2023-02.io.simplyblock:cluster-1"):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.nqn = nqn
    return cl


def _flatten(nodes, lvols_by_node):
    """``lvols_by_node`` may be a flat list of all cluster lvols, or a
    callable ``node_id -> [lvols]``."""
    if callable(lvols_by_node):
        return [lv for n in nodes for lv in lvols_by_node(n.get_id())]
    return list(lvols_by_node)


def _call_get_next_3_nodes(nodes, lvols_by_node, cluster_id="cluster-1",
                           namespaced=False):
    """Call _get_next_3_nodes with the DB mocked out.

    Subsystem counting works on the pre-fetched ``all_lvols`` list (one
    subsystem per distinct NQN among a node's primary lvols), so only the
    storage-node lookup needs mocking.
    """
    all_lvols = _flatten(nodes, lvols_by_node)

    with patch("simplyblock_core.controllers.lvol_controller.DBController") as mock_db_cls:
        from simplyblock_core.controllers.lvol_controller import _get_next_3_nodes

        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        db.get_storage_node_by_id.side_effect = lambda nid: next(
            n for n in nodes if n.get_id() == nid)
        mock_db_cls.return_value = db

        with patch.object(StorageNode, 'lvol_sync_del', return_value=False):
            return _get_next_3_nodes(cluster_id, all_lvols=all_lvols,
                                     namespaced=namespaced)


# ===========================================================================
# Tests for count_lvol_subsystems
# ===========================================================================

class TestCountLvolSubsystems(unittest.TestCase):
    """Only primary subsystems (distinct NQNs of lvols whose node_id is the
    node) count against max_lvol."""

    def _count(self, node, lvols):
        from simplyblock_core.controllers.lvol_controller import count_lvol_subsystems
        return count_lvol_subsystems(node, lvols)

    def test_counts_distinct_primary_nqns(self):
        node = _node("n1")
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n1", nqn="nqn:A"),  # shares subsystem with v1
            _lvol("v3", "n1", nqn="nqn:B"),
        ]
        self.assertEqual(self._count(node, lvols), 2)

    def test_excludes_other_nodes_lvols(self):
        """LVols whose primary is another node must not count — the node may
        host their secondary/tertiary replica subsystems, but the memory
        reservation already provisions for those."""
        node = _node("n1")
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n2", nqn="nqn:B"),
            _lvol("v3", "n3", nqn="nqn:C"),
        ]
        self.assertEqual(self._count(node, lvols), 1)

    def test_excludes_lvols_in_deletion(self):
        node = _node("n1")
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A", status=LVol.STATUS_IN_DELETION),
            _lvol("v2", "n1", nqn="nqn:B", status=LVol.STATUS_DELETED),
            _lvol("v3", "n1", nqn="nqn:C"),
        ]
        self.assertEqual(self._count(node, lvols), 1)

    def test_counts_lvols_in_creation(self):
        """In-creation lvols hold a subsystem slot — their subsystem is about
        to exist; not counting them would let concurrent creates overshoot."""
        node = _node("n1")
        lvols = [_lvol("v1", "n1", nqn="nqn:A", status=LVol.STATUS_IN_CREATION)]
        self.assertEqual(self._count(node, lvols), 1)

    def test_empty(self):
        self.assertEqual(self._count(_node("n1"), []), 0)


# ===========================================================================
# Tests for _get_next_3_nodes subsystem counting
# ===========================================================================

class TestGetNext3NodesSubsystemLimit(unittest.TestCase):
    """_get_next_3_nodes should count unique primary subsystems (NQNs)."""

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

    def test_replica_subsystems_do_not_count(self):
        """LVols primary on other nodes (whose replicas this node hosts) must
        not push the node over its limit."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            # primaries elsewhere; n1 may hold their replica subsystems
            _lvol("v2", "n2", nqn="nqn:B"),
            _lvol("v3", "n3", nqn="nqn:C"),
            _lvol("v4", "n4", nqn="nqn:D"),
        ]
        result = _call_get_next_3_nodes([node], lvols)
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
# Tests for namespaced placement in _get_next_3_nodes
# ===========================================================================

class TestGetNext3NodesNamespaced(unittest.TestCase):
    """Namespaced creates consume a namespace slot, not a subsystem slot,
    when an existing subsystem can take them."""

    def test_at_limit_node_kept_when_namespace_slot_free(self):
        """A node at max_lvol stays eligible for a namespaced create if one
        of its subsystems has a free namespace slot."""
        node = _node("n1", max_lvol=1)
        lvols = [_lvol("parent", "n1", nqn="nqn:parent", max_ns=300)]
        self.assertEqual(_call_get_next_3_nodes([node], lvols), [])
        result = _call_get_next_3_nodes([node], lvols, namespaced=True)
        self.assertIn(node, result)

    def test_at_limit_node_skipped_when_no_namespace_slot(self):
        """A node at max_lvol whose subsystems are all full stays ineligible
        even for namespaced creates."""
        node = _node("n1", max_lvol=1)
        lvols = [_lvol("v1", "n1", nqn="nqn:full", max_ns=1)]
        result = _call_get_next_3_nodes([node], lvols, namespaced=True)
        self.assertEqual(result, [])

    def test_namespaced_prefers_nodes_with_free_slot(self):
        """When some nodes have joinable subsystems, a namespaced create
        should land there instead of opening a new subsystem elsewhere."""
        node_parent = _node("n-parent", max_lvol=10)
        node_empty = _node("n-empty", max_lvol=10)
        lvols = [_lvol("parent", "n-parent", nqn="nqn:parent", max_ns=300)]

        result = _call_get_next_3_nodes([node_parent, node_empty], lvols,
                                        namespaced=True)
        self.assertEqual(result, [node_parent])

    def test_non_namespaced_ignores_namespace_slots(self):
        """Non-namespaced creates keep the normal weighted selection."""
        node_parent = _node("n-parent", max_lvol=10)
        node_empty = _node("n-empty", max_lvol=10)
        lvols = [_lvol("parent", "n-parent", nqn="nqn:parent", max_ns=300)]

        result = _call_get_next_3_nodes([node_parent, node_empty], lvols)
        self.assertIn(node_parent, result)
        self.assertIn(node_empty, result)

    def test_namespaced_no_slots_anywhere_falls_back_to_normal(self):
        """With no joinable subsystem on any node, namespaced creates fall
        back to the normal selection over nodes below the limit."""
        node_a = _node("n-a", max_lvol=10)
        node_b = _node("n-b", max_lvol=10)
        lvols = [_lvol("v1", "n-a", nqn="nqn:full", max_ns=1)]

        result = _call_get_next_3_nodes([node_a, node_b], lvols,
                                        namespaced=True)
        self.assertIn(node_a, result)
        self.assertIn(node_b, result)


# ===========================================================================
# Tests for _resolve_lvol_subsystem (create path subsystem choice + cap)
# ===========================================================================

class TestResolveLvolSubsystem(unittest.TestCase):
    """The subsystem cap only applies when a new subsystem is created;
    joining an existing subsystem bypasses it."""

    def _resolve(self, lvol, host_node, namespaced, all_lvols):
        from simplyblock_core.controllers.lvol_controller import _resolve_lvol_subsystem
        return _resolve_lvol_subsystem(lvol, host_node, _cluster(), namespaced,
                                       all_lvols)

    def test_namespaced_joins_existing_subsystem_on_full_node(self):
        """Namespaced lvol joins a subsystem with a free slot even when the
        node is at max_lvol — no new subsystem is needed."""
        node = _node("n1", max_lvol=1)
        parent = _lvol("parent", "n1", nqn="nqn:parent", max_ns=300)
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, True, [parent])
        self.assertTrue(ok, error)
        self.assertEqual(new_lvol.nqn, "nqn:parent")
        self.assertEqual(new_lvol.namespace, "parent")
        self.assertEqual(new_lvol.max_namespace_per_subsys, 300)

    def test_namespaced_rejected_on_full_node_without_slot(self):
        node = _node("n1", max_lvol=1)
        existing = _lvol("v1", "n1", nqn="nqn:full", max_ns=1)
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, True, [existing])
        self.assertFalse(ok)
        self.assertIn("Too many subsystems", error)

    def test_namespaced_new_subsystem_when_under_cap(self):
        node = _node("n1", max_lvol=10)
        existing = _lvol("v1", "n1", nqn="nqn:full", max_ns=1)
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, True, [existing])
        self.assertTrue(ok, error)
        self.assertIn(new_lvol.uuid, new_lvol.nqn)

    def test_namespaced_ignores_subsystems_on_other_nodes(self):
        """A joinable subsystem on another node is irrelevant — namespaces
        live on the subsystem's node."""
        node = _node("n1", max_lvol=1)
        own_full = _lvol("v1", "n1", nqn="nqn:own", max_ns=1)  # n1 at cap
        parent_elsewhere = _lvol("parent", "n2", nqn="nqn:parent", max_ns=300)
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, True,
                                  [own_full, parent_elsewhere])
        self.assertFalse(ok)
        self.assertIn("Too many subsystems", error)
        self.assertNotEqual(new_lvol.nqn, "nqn:parent")

    def test_non_namespaced_rejected_at_cap(self):
        node = _node("n1", max_lvol=1)
        existing = _lvol("v1", "n1", nqn="nqn:A")
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, False, [existing])
        self.assertFalse(ok)
        self.assertIn("Too many subsystems", error)

    def test_non_namespaced_allowed_below_cap(self):
        node = _node("n1", max_lvol=2)
        existing = _lvol("v1", "n1", nqn="nqn:A")
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, False, [existing])
        self.assertTrue(ok, error)
        self.assertIn(new_lvol.uuid, new_lvol.nqn)

    def test_non_namespaced_never_joins(self):
        """Non-namespaced lvols always get their own subsystem even when a
        joinable one exists."""
        node = _node("n1", max_lvol=10)
        parent = _lvol("parent", "n1", nqn="nqn:parent", max_ns=300)
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, False, [parent])
        self.assertTrue(ok, error)
        self.assertNotEqual(new_lvol.nqn, "nqn:parent")

    def test_replica_subsystems_do_not_block_creation(self):
        """LVols primary on other nodes must not count toward this node's
        cap (their replica subsystems are covered by the memory reservation)."""
        node = _node("n1", max_lvol=2)
        lvols = [
            _lvol("v1", "n1", nqn="nqn:A"),
            _lvol("v2", "n2", nqn="nqn:B"),
            _lvol("v3", "n3", nqn="nqn:C"),
        ]
        new_lvol = _lvol("new", "n1")

        ok, error = self._resolve(new_lvol, node, False, lvols)
        self.assertTrue(ok, error)


if __name__ == "__main__":
    unittest.main()
