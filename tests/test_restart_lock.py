# coding=utf-8
"""
test_restart_lock.py – unit tests for the pre-restart FDB transaction guard.

Covers:
  - db_controller.try_set_node_restarting (FDB transaction)
  - restart_storage_node pre-restart check integration
"""

import unittest
from unittest.mock import MagicMock, patch
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


# ---------------------------------------------------------------------------
# 3. Wrapper cleanup safety: never clobber a concurrent caller's RESTARTING
# ---------------------------------------------------------------------------

class TestRestartWrapperCleanupSafety(unittest.TestCase):
    """The wrapper's RESTARTING→OFFLINE cleanup must not fire when another
    caller owns the lock. Regression test for the soak-test stall on
    2026-04-25, where a parallel CLI retry bailed fast in the impl
    (status != OFFLINE) but the wrapper still wrote OFFLINE on top of the
    auto-restart task's RESTARTING state. That clobber caused
    health_controller on peers to flip the node's local devices to
    UNAVAILABLE, and they stayed stuck after the restart completed."""

    def _node(self, status, uuid="node-1", cluster_id="c1"):
        n = StorageNode()
        n.uuid = uuid
        n.status = status
        n.cluster_id = cluster_id
        n.mgmt_ip = "10.0.0.1"
        n.rpc_port = 8080
        return n

    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_cleanup_when_pre_status_already_restarting(
            self, mock_db_cls, mock_impl, mock_set_status):
        """Concurrent caller owns RESTARTING; impl bails fast; wrapper
        must NOT write OFFLINE. This is the exact bug from the soak run."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        # Pre-call: node is already RESTARTING (auto-restart task in flight).
        # Impl returns False fast (status != OFFLINE). Post-call: still RESTARTING.
        node = self._node(StorageNode.STATUS_RESTARTING)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = node
        mock_impl.return_value = False

        result = restart_storage_node("node-1")
        self.assertFalse(result)
        mock_set_status.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_cleanup_when_pre_status_in_shutdown(
            self, mock_db_cls, mock_impl, mock_set_status):
        """Peer is shutting down; we must not clobber that transition."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        node = self._node(StorageNode.STATUS_IN_SHUTDOWN)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = node
        mock_impl.return_value = False

        result = restart_storage_node("node-1")
        self.assertFalse(result)
        mock_set_status.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.distr_controller")
    @patch("simplyblock_core.storage_node_ops.storage_events")
    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_cleanup_runs_when_pre_offline_and_post_restarting(
            self, mock_db_cls, mock_impl, mock_set_status,
            mock_events, mock_distr):
        """WE acquired RESTARTING (pre=OFFLINE) and a later step failed
        (impl returned False). Cleanup must reset the node to OFFLINE so
        future attempts can proceed.

        The wrapper does this with a direct DB write rather than
        ``set_node_status`` (deliberately, to avoid second-order effects
        from the helper). The contract this test pins is therefore: on
        the failure path, ``post_node.write_to_db`` is invoked and the
        post_node's status is set to OFFLINE before the write."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        pre_node = self._node(StorageNode.STATUS_OFFLINE)
        post_node = self._node(StorageNode.STATUS_RESTARTING)
        post_node.write_to_db = MagicMock()

        mock_db_cls.return_value.get_storage_node_by_id.side_effect = [
            pre_node, post_node,
        ]
        mock_impl.return_value = False

        result = restart_storage_node("node-1")
        self.assertFalse(result)
        # The wrapper does not route the cleanup write through
        # set_node_status.
        mock_set_status.assert_not_called()
        # It writes the post_node directly with status flipped to OFFLINE.
        post_node.write_to_db.assert_called_once()
        self.assertEqual(post_node.status, StorageNode.STATUS_OFFLINE)

    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_cleanup_when_impl_succeeded(
            self, mock_db_cls, mock_impl, mock_set_status):
        """Successful restart leaves the node ONLINE — wrapper must not
        touch status."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        node = self._node(StorageNode.STATUS_OFFLINE)
        mock_db_cls.return_value.get_storage_node_by_id.return_value = node
        mock_impl.return_value = True

        result = restart_storage_node("node-1")
        self.assertTrue(result)
        mock_set_status.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_cleanup_when_pre_status_unreadable(
            self, mock_db_cls, mock_impl, mock_set_status):
        """If we can't read pre-status (DB error, deleted node), be
        conservative and skip cleanup rather than risk a clobber."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        # Pre-read fails; post-read (in finally) would see RESTARTING.
        post_node = self._node(StorageNode.STATUS_RESTARTING)
        mock_db_cls.return_value.get_storage_node_by_id.side_effect = [
            KeyError("node not found"),
            post_node,
        ]
        mock_impl.return_value = False

        result = restart_storage_node("node-1")
        self.assertFalse(result)
        mock_set_status.assert_not_called()

    @patch("simplyblock_core.storage_node_ops.set_node_status")
    @patch("simplyblock_core.storage_node_ops._restart_storage_node_impl")
    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_no_cleanup_when_post_status_not_restarting(
            self, mock_db_cls, mock_impl, mock_set_status):
        """Pre=OFFLINE, post=OFFLINE (impl bailed before acquisition).
        Nothing to clean up."""
        from simplyblock_core.storage_node_ops import restart_storage_node

        offline_node = self._node(StorageNode.STATUS_OFFLINE)
        mock_db_cls.return_value.get_storage_node_by_id.side_effect = [
            offline_node, offline_node,
        ]
        mock_impl.return_value = False

        result = restart_storage_node("node-1")
        self.assertFalse(result)
        mock_set_status.assert_not_called()


if __name__ == "__main__":
    unittest.main()
