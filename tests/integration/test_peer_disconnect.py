# coding=utf-8
"""
test_peer_disconnect.py — regression tests for the FDB-status short-circuit
added to ``simplyblock_core.storage_node_ops._check_peer_disconnected``.

Background: the restart path uses ``_check_peer_disconnected(peer_node)`` to
decide between the takeover path (recreate as leader for an offline peer's
LVS) and the non-leader path (port-block the leader and recreate as
secondary). Before this change, only the data-plane quorum — which reads
stale NVMe-controller state on surviving peers — decided. In practice,
when mgmt had already observed a peer leaving the cluster (status flipped
to OFFLINE in FDB), the quorum still reported "connected" for ~10-30 s
until NVMe-TCP keep-alive propagated on surviving peers. During that
window the code went into the non-leader path, tried to port-block the
dead peer's mgmt (ECONNREFUSED), retried 5×, and aborted the restart with
a misleading ``"LVStore recovery failed"`` event.

This test pins: ``_check_peer_disconnected`` now short-circuits to True
for ``OFFLINE`` / ``REMOVED`` / ``UNREACHABLE`` in FDB, without reaching
the data-plane quorum. Transient states that the runner owns
(``IN_SHUTDOWN`` / ``RESTARTING``) deliberately fall through to the
quorum — preempting another node's leadership during its own restart
would be incorrect.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _node(uuid="peer-1", status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = uuid
    n.status = status
    return n


class TestCheckPeerDisconnected(unittest.TestCase):

    def _run(self, status, quorum_result=False):
        # _check_peer_disconnected refreshes peer_node from FDB at entry, then
        # imports ``is_node_data_plane_disconnected_quorum`` locally from the
        # services module at call time. Both must be patched: DBController so
        # the refresh returns our test peer (preserving the injected status),
        # and the quorum at its source module.
        from simplyblock_core import storage_node_ops as mod
        peer = _node(status=status)
        db_ctrl = MagicMock()
        db_ctrl.get_storage_node_by_id.return_value = peer
        with patch.object(mod, "DBController", return_value=db_ctrl), \
             patch(
                "simplyblock_core.services.storage_node_monitor.is_node_data_plane_disconnected_quorum",
                return_value=quorum_result) as q:
            return mod._check_peer_disconnected(peer), q

    # -----------------------------------------------------------------
    # Short-circuit branches — FDB says the peer is gone
    # -----------------------------------------------------------------

    def test_offline_short_circuits_to_disconnected(self):
        # FDB OFFLINE is the canonical "mgmt confirmed peer left" state.
        # Must return True without calling the data-plane quorum.
        disconnected, mock_quorum = self._run(StorageNode.STATUS_OFFLINE)
        self.assertTrue(disconnected)
        mock_quorum.assert_not_called()

    def test_removed_short_circuits_to_disconnected(self):
        disconnected, mock_quorum = self._run(StorageNode.STATUS_REMOVED)
        self.assertTrue(disconnected)
        mock_quorum.assert_not_called()

    def test_unreachable_short_circuits_to_disconnected(self):
        disconnected, mock_quorum = self._run(StorageNode.STATUS_UNREACHABLE)
        self.assertTrue(disconnected)
        mock_quorum.assert_not_called()

    # -----------------------------------------------------------------
    # Transient states — do NOT short-circuit, fall through to quorum
    # -----------------------------------------------------------------

    def test_in_shutdown_falls_through_to_quorum(self):
        # Another node is actively being shut down. That's brief — quorum
        # decides whether to preempt.
        disconnected, mock_quorum = self._run(StorageNode.STATUS_IN_SHUTDOWN,
                                              quorum_result=False)
        self.assertFalse(disconnected)
        mock_quorum.assert_called_once()

    def test_restarting_falls_through_to_quorum(self):
        disconnected, mock_quorum = self._run(StorageNode.STATUS_RESTARTING,
                                              quorum_result=False)
        self.assertFalse(disconnected)
        mock_quorum.assert_called_once()

    def test_online_falls_through_to_quorum(self):
        disconnected, mock_quorum = self._run(StorageNode.STATUS_ONLINE,
                                              quorum_result=False)
        self.assertFalse(disconnected)
        mock_quorum.assert_called_once()

    def test_online_with_quorum_disconnect_returns_true(self):
        # Mgmt says ONLINE but data-plane quorum confirms the peer is gone
        # (classic fabric partition where mgmt is still reachable). Function
        # must respect the quorum result.
        disconnected, mock_quorum = self._run(StorageNode.STATUS_ONLINE,
                                              quorum_result=True)
        self.assertTrue(disconnected)
        mock_quorum.assert_called_once()


if __name__ == "__main__":
    unittest.main()
