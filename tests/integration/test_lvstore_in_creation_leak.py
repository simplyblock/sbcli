# coding=utf-8
"""Regression tests for the leaked lvstore_status="in_creation" incident
(2026-07-07 13:52): a failed replica-rebuild phase in restart_storage_node
left the PEER primary's lvstore_status as "in_creation"; check_node skips all
monitoring of such a node, so the peer (whose SPDK had segfaulted inside the
window) stayed 'online/health True' for 1.5h+ while every restart that needed
it kept failing.

Two fixes pinned here:
1. storage_node_ops._restore_peer_lvstore_status_ready — restores the marker
   on the Step-2/3 failure paths (call sites pinned at source level; the
   phases themselves need a full restart harness).
2. storage_node_monitor.check_node — the in_creation skip is BOUNDED: while a
   restart task owns the node or the marker is fresh it skips as before; a
   stale marker with no owning task is reclaimed to "ready" and checks resume.
"""

import os
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _node(node_id="peer-1", status=StorageNode.STATUS_ONLINE,
          lvstore_status="in_creation"):
    n = StorageNode()
    n.uuid = node_id
    n.status = status
    n.lvstore_status = lvstore_status
    n.cluster_id = "cluster-1"
    n.nvme_devices = []
    return n


class TestRestorePeerLvstoreStatus(unittest.TestCase):

    def _db(self, node):
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        return db

    def test_restores_leaked_in_creation(self):
        from simplyblock_core import storage_node_ops as ops
        node = _node()
        node.write_to_db = MagicMock()
        db = self._db(node)
        ops._restore_peer_lvstore_status_ready("peer-1", db)
        self.assertEqual(node.lvstore_status, "ready")
        node.write_to_db.assert_called_once()

    def test_leaves_other_status_alone(self):
        from simplyblock_core import storage_node_ops as ops
        node = _node(lvstore_status="ready")
        node.write_to_db = MagicMock()
        db = self._db(node)
        ops._restore_peer_lvstore_status_ready("peer-1", db)
        node.write_to_db.assert_not_called()

    def test_missing_node_is_noop(self):
        from simplyblock_core import storage_node_ops as ops
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = KeyError("gone")
        ops._restore_peer_lvstore_status_ready("peer-1", db)  # must not raise

    def test_failure_paths_call_restore(self):
        """Source pin: every Step-2/3 failure branch (ret=False AND exception)
        restores the peer marker. The success path's restore lives inside
        recreate_lvstore_on_non_leader and is intentionally not duplicated."""
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, "simplyblock_core/storage_node_ops.py")) as f:
            src = f.read()
        for phase, marker in (("secondary", "Failed to recreate secondary LVS"),
                              ("tertiary", "Failed to recreate tertiary LVS")):
            i = src.index(marker)
            window = src[i:i + 900]
            self.assertIn(
                "_restore_peer_lvstore_status_ready", window,
                f"{phase} ret-False branch must restore the peer's "
                f"lvstore_status (leak = permanent monitoring blackout)")
        for exc_marker in ("Secondary LVS recreation failed",
                           "Tertiary LVS recreation failed"):
            i = src.index(exc_marker)
            window = src[i:i + 900]
            self.assertIn(
                "_restore_peer_lvstore_status_ready", window,
                f"exception branch after {exc_marker!r} must restore the "
                f"peer's lvstore_status")


class TestBoundedInCreationSkip(unittest.TestCase):

    def setUp(self):
        from simplyblock_core.services import storage_node_monitor as mod
        self.mod = mod
        mod._lvstore_in_creation_first_seen.clear()
        self.addCleanup(mod._lvstore_in_creation_first_seen.clear)

    def _check(self, node, active_task=None):
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        with patch.object(self.mod, "db", db), \
             patch.object(self.mod.health_controller,
                          "_check_node_ping", return_value=False), \
             patch.object(self.mod.tasks_controller,
                          "get_active_node_restart_task",
                          return_value=active_task):
            try:
                return self.mod.check_node(node)
            except Exception:
                # Downstream liveness checks run against MagicMocks once the
                # reclaim falls through; only the reclaim itself is under test.
                return "fell-through"

    def test_fresh_marker_skips(self):
        node = _node()
        ret = self._check(node)
        self.assertFalse(ret)
        self.assertEqual(node.lvstore_status, "in_creation")

    def test_owned_marker_skips_even_when_stale(self):
        node = _node()
        self.mod._lvstore_in_creation_first_seen["peer-1"] = (
            time.time() - self.mod.LVSTORE_IN_CREATION_STALE_SEC - 5)
        ret = self._check(node, active_task=MagicMock())
        self.assertFalse(ret)
        self.assertEqual(node.lvstore_status, "in_creation")

    def test_stale_unowned_marker_is_reclaimed(self):
        node = _node()
        node.write_to_db = MagicMock()
        self.mod._lvstore_in_creation_first_seen["peer-1"] = (
            time.time() - self.mod.LVSTORE_IN_CREATION_STALE_SEC - 5)
        self._check(node, active_task=None)
        self.assertEqual(node.lvstore_status, "ready")
        node.write_to_db.assert_called()
        # tracking entry cleared so a future window restarts the clock
        self.assertNotIn("peer-1", self.mod._lvstore_in_creation_first_seen)

    def test_marker_clear_resets_clock(self):
        node = _node(lvstore_status="ready")
        self.mod._lvstore_in_creation_first_seen["peer-1"] = time.time() - 10_000
        self._check(node)
        self.assertNotIn("peer-1", self.mod._lvstore_in_creation_first_seen)


if __name__ == "__main__":
    unittest.main()
