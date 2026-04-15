# coding=utf-8
"""
test_restart_lock.py – unit tests for the pre-restart FDB transaction guard.

Covers:
  - db_controller.try_set_node_restarting (FDB transaction)
  - restart_storage_node pre-restart check integration
"""

import unittest
from unittest.mock import MagicMock, patch, call
import json

from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# 1. Pre-restart FDB transaction guard
# ---------------------------------------------------------------------------

class TestPreRestartGuard(unittest.TestCase):
    """Test the FDB transactional pre-restart check."""

    def _make_node(self, uuid, cluster_id, status):
        n = StorageNode()
        n.uuid = uuid
        n.cluster_id = cluster_id
        n.status = status
        return n

    def test_succeeds_when_no_peer_in_restart_or_shutdown(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)

        nodes = [
            self._make_node("node-1", "c1", StorageNode.STATUS_OFFLINE),
            self._make_node("node-2", "c1", StorageNode.STATUS_ONLINE),
            self._make_node("node-3", "c1", StorageNode.STATUS_ONLINE),
        ]

        tr = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=nodes):
            result, reason = DBController._try_set_node_restarting_tx(
                db, tr, "c1", "node-1")

        self.assertTrue(result)
        self.assertIsNone(reason)
        # Should have written the node status update
        tr.__setitem__.assert_called_once()

    def test_blocked_when_peer_is_restarting(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)

        nodes = [
            self._make_node("node-1", "c1", StorageNode.STATUS_OFFLINE),
            self._make_node("node-2", "c1", StorageNode.STATUS_RESTARTING),
        ]

        tr = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=nodes):
            result, reason = DBController._try_set_node_restarting_tx(
                db, tr, "c1", "node-1")

        self.assertFalse(result)
        self.assertIn("node-2", reason)
        self.assertIn("in_restart", reason)

    def test_blocked_when_peer_is_in_shutdown(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)

        nodes = [
            self._make_node("node-1", "c1", StorageNode.STATUS_OFFLINE),
            self._make_node("node-2", "c1", StorageNode.STATUS_IN_SHUTDOWN),
        ]

        tr = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=nodes):
            result, reason = DBController._try_set_node_restarting_tx(
                db, tr, "c1", "node-1")

        self.assertFalse(result)
        self.assertIn("node-2", reason)
        self.assertIn("in_shutdown", reason)

    def test_ignores_nodes_in_other_clusters(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)

        nodes = [
            self._make_node("node-1", "c1", StorageNode.STATUS_OFFLINE),
            self._make_node("node-X", "c2", StorageNode.STATUS_RESTARTING),  # different cluster
        ]

        tr = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=nodes):
            result, reason = DBController._try_set_node_restarting_tx(
                db, tr, "c1", "node-1")

        self.assertTrue(result)

    def test_sets_node_to_in_restart(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)

        node = self._make_node("node-1", "c1", StorageNode.STATUS_OFFLINE)
        nodes = [node]

        tr = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=nodes):
            result, reason = DBController._try_set_node_restarting_tx(
                db, tr, "c1", "node-1")

        self.assertTrue(result)
        # Verify the written data has status=in_restart
        written_key = tr.__setitem__.call_args[0][0]
        written_data = json.loads(tr.__setitem__.call_args[0][1])
        self.assertEqual(written_data["status"], StorageNode.STATUS_RESTARTING)

    def test_no_kv_store_returns_false(self):
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)
        db.kv_store = None

        result, reason = db.try_set_node_restarting("c1", "n1")
        self.assertFalse(result)
        self.assertEqual(reason, "No DB connection")


# ---------------------------------------------------------------------------
# 1a. Post-commit event emission for the restart guard
# ---------------------------------------------------------------------------

class TestRestartGuardEventEmission(unittest.TestCase):
    """Regression tests for the silent-DB-write bug: the restart guard
    tx writes status=in_restart directly via ``tr[...] = ...`` and bypasses
    set_node_status. The wrapper must emit the storage-event + peer
    notification after the commit so the transition is observable.
    """

    def _make_node(self, uuid, status):
        n = StorageNode()
        n.uuid = uuid
        n.cluster_id = "c1"
        n.status = status
        return n

    def _prepare_db(self, pre_status, post_status):
        """Build a DBController with get_storage_node_by_id returning a
        pre-tx node first, then a post-tx node with updated status.
        """
        from simplyblock_core.db_controller import DBController
        db = DBController.__new__(DBController)
        db.kv_store = MagicMock()  # truthy so we don't short-circuit
        pre = self._make_node("n1", pre_status)
        post = self._make_node("n1", post_status)
        db.get_storage_node_by_id = MagicMock(side_effect=[pre, post])
        return db

    @patch("simplyblock_core.distr_controller.send_node_status_event")
    @patch("simplyblock_core.controllers.storage_events.snode_status_change")
    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_emits_events_on_offline_to_restarting(
            self, mock_transactional, mock_status_change, mock_peer_event):
        """Happy path: offline → in_restart. Both events must fire."""
        # Pretend the tx commits successfully.
        mock_transactional.return_value = MagicMock(return_value=(True, None))

        db = self._prepare_db(
            pre_status=StorageNode.STATUS_OFFLINE,
            post_status=StorageNode.STATUS_RESTARTING,
        )

        acquired, reason = db.try_set_node_restarting("c1", "n1")

        self.assertTrue(acquired)
        self.assertIsNone(reason)
        mock_status_change.assert_called_once()
        mock_peer_event.assert_called_once()

        # Old status must be captured (pre-tx snapshot), not None/unknown.
        args, kwargs = mock_status_change.call_args
        # signature: (snode, new_status, old_status, caused_by="...")
        self.assertEqual(args[1], StorageNode.STATUS_RESTARTING)
        self.assertEqual(args[2], StorageNode.STATUS_OFFLINE)
        self.assertEqual(kwargs.get("caused_by"), "restart_guard")

    @patch("simplyblock_core.distr_controller.send_node_status_event")
    @patch("simplyblock_core.controllers.storage_events.snode_status_change")
    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_no_events_when_tx_blocked(
            self, mock_transactional, mock_status_change, mock_peer_event):
        """Guard rejected the claim — no events."""
        mock_transactional.return_value = MagicMock(
            return_value=(False, "Node n2 is in_restart"))

        db = self._prepare_db(
            pre_status=StorageNode.STATUS_OFFLINE,
            post_status=StorageNode.STATUS_OFFLINE,
        )

        acquired, reason = db.try_set_node_restarting("c1", "n1")

        self.assertFalse(acquired)
        self.assertIn("in_restart", reason)
        mock_status_change.assert_not_called()
        mock_peer_event.assert_not_called()

    @patch("simplyblock_core.distr_controller.send_node_status_event")
    @patch("simplyblock_core.controllers.storage_events.snode_status_change")
    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_no_events_when_status_unchanged(
            self, mock_transactional, mock_status_change, mock_peer_event):
        """Force-restart on an already-RESTARTING node: tx succeeds but
        status is the same on both sides. Avoid spurious
        RESTARTING→RESTARTING change events.
        """
        mock_transactional.return_value = MagicMock(return_value=(True, None))

        db = self._prepare_db(
            pre_status=StorageNode.STATUS_RESTARTING,
            post_status=StorageNode.STATUS_RESTARTING,
        )

        acquired, reason = db.try_set_node_restarting("c1", "n1")

        self.assertTrue(acquired)
        mock_status_change.assert_not_called()
        mock_peer_event.assert_not_called()

    @patch("simplyblock_core.distr_controller.send_node_status_event")
    @patch("simplyblock_core.controllers.storage_events.snode_status_change")
    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_emission_failure_does_not_mask_commit(
            self, mock_transactional, mock_status_change, mock_peer_event):
        """If event emission raises, the function must still return the
        acquisition result truthfully — the FDB state has already been
        committed and cannot be rolled back.
        """
        mock_transactional.return_value = MagicMock(return_value=(True, None))
        mock_status_change.side_effect = RuntimeError("broker down")

        db = self._prepare_db(
            pre_status=StorageNode.STATUS_OFFLINE,
            post_status=StorageNode.STATUS_RESTARTING,
        )

        acquired, reason = db.try_set_node_restarting("c1", "n1")

        self.assertTrue(acquired)
        self.assertIsNone(reason)


# ---------------------------------------------------------------------------
# 2. restart_storage_node pre-restart integration
# ---------------------------------------------------------------------------

class TestRestartStorageNodePreCheck(unittest.TestCase):

    def _node(self, uuid="node-1", status=StorageNode.STATUS_OFFLINE,
              cluster_id="cluster-1"):
        n = StorageNode()
        n.uuid = uuid
        n.status = status
        n.cluster_id = cluster_id
        n.mgmt_ip = "10.0.0.1"
        n.rpc_port = 8080
        return n

    @patch("simplyblock_core.storage_node_ops.tasks_controller")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_returns_false_when_pre_restart_check_blocked(self, mock_db_cls, mock_tasks):
        from simplyblock_core.storage_node_ops import restart_storage_node

        node = self._node()
        db = mock_db_cls.return_value
        db.get_storage_node_by_id.return_value = node
        db.get_cluster_by_id.return_value = MagicMock(status="active")
        mock_tasks.get_active_node_restart_task.return_value = False

        db.try_set_node_restarting.return_value = (False, "Node node-2 is in_restart")

        result = restart_storage_node("node-1")
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
