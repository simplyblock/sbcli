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
