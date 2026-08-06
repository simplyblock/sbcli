# coding=utf-8
"""
test_data_plane_quorum.py — unit tests for
``simplyblock_core.services.storage_node_monitor._count_data_plane_votes``
and the ``is_node_data_plane_disconnected`` /
``is_node_data_plane_disconnected_quorum`` helpers on top of it.

Background: the previous implementation queried ``jc_get_jm_status`` on
each online peer and counted ``remote_jm_{target}n1: false`` as a
"disconnected" vote. That ``bool`` is actually a sync-health flag
updated only by the JC's replication/leveling state machine — it is
not flipped to ``false`` on NVMe-TCP controller loss or bdev removal,
so a quietly-dead peer perpetually voted "connected" and the quorum
stayed wedged. The new implementation uses the actual NVMe controller
state on each peer (``bdev_nvme_get_controllers``) and a pre-check of
the namespace bdev existence (``bdev_get_bdevs``) so a degraded
controller doesn't stall the probe.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _peer(uuid, status=StorageNode.STATUS_ONLINE, jm_vuid=999):
    """Build a mock peer node with an ``rpc_client(timeout, retry)`` factory."""
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = uuid
    n.status = status
    n.jm_vuid = jm_vuid
    n.cluster_id = "cluster-1"

    n._rpc = MagicMock()
    n.rpc_client = MagicMock(return_value=n._rpc)
    return n


def _target(uuid="target-node"):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = uuid
    n.cluster_id = "cluster-1"
    return n


def _set_peer_response(peer, bdev_present, ctrl_states):
    """Wire a peer's RPCClient to return a bdev list and controller list.

    ``bdev_present`` — True to return a bdev dict, False to return [].
    ``ctrl_states``  — list of state strings per path, or None to return [].
    """
    if bdev_present:
        peer._rpc.get_bdevs.return_value = [{"name": "ignored", "aliases": []}]
    else:
        peer._rpc.get_bdevs.return_value = []

    if ctrl_states is None:
        peer._rpc.bdev_nvme_controller_list.return_value = []
    else:
        peer._rpc.bdev_nvme_controller_list.return_value = [
            {"name": "remote_jm_target-node",
             "ctrlrs": [{"state": s} for s in ctrl_states]}
        ]


# ---------------------------------------------------------------------------
# _count_data_plane_votes
# ---------------------------------------------------------------------------


class TestCountDataPlaneVotes(unittest.TestCase):

    def _run(self, peers):
        from simplyblock_core.services import storage_node_monitor as mod
        target = _target("target-node")
        with patch.object(mod, "db") as mock_db:
            mock_db.get_storage_nodes_by_cluster_id.return_value = [target] + peers
            return mod._count_data_plane_votes(target)

    def test_no_online_peers_returns_zero(self):
        # All peers offline / excluded → no votes cast.
        p = _peer("p1", status=StorageNode.STATUS_OFFLINE)
        disc, total = self._run([p])
        self.assertEqual((disc, total), (0, 0))

    def test_all_peers_enabled_not_disconnected(self):
        peers = [_peer(f"p{i}") for i in range(3)]
        for p in peers:
            _set_peer_response(p, bdev_present=True, ctrl_states=["enabled"])
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (0, 3))

    def test_all_peers_failed_all_disconnected(self):
        peers = [_peer(f"p{i}") for i in range(3)]
        for p in peers:
            _set_peer_response(p, bdev_present=True, ctrl_states=["failed"])
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (3, 3))

    def test_bdev_absent_abstains(self):
        # Missing bdev = peer doesn't have a topology link, abstain.
        peers = [_peer(f"p{i}") for i in range(3)]
        _set_peer_response(peers[0], bdev_present=False, ctrl_states=None)
        _set_peer_response(peers[1], bdev_present=True, ctrl_states=["enabled"])
        _set_peer_response(peers[2], bdev_present=True, ctrl_states=["failed"])
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (1, 2))  # p0 abstains; p1 connected, p2 disc

    def test_bdev_present_but_controller_missing_is_disconnected(self):
        # Inconsistent state: namespace bdev exists but controller list empty.
        # Count as disconnected so the restart path doesn't wait for a phantom.
        peers = [_peer("p0")]
        _set_peer_response(peers[0], bdev_present=True, ctrl_states=None)
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (1, 1))

    def test_resetting_state_counts_as_disconnected(self):
        peers = [_peer("p0")]
        _set_peer_response(peers[0], bdev_present=True, ctrl_states=["resetting"])
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (1, 1))

    def test_any_path_enabled_counts_as_connected(self):
        # Multipath: if any path reports enabled, treat controller as up.
        peers = [_peer("p0")]
        _set_peer_response(peers[0], bdev_present=True,
                           ctrl_states=["failed", "enabled"])
        disc, total = self._run(peers)
        self.assertEqual((disc, total), (0, 1))

    def test_get_bdevs_exception_skips_peer(self):
        peers = [_peer("p0"), _peer("p1")]
        peers[0]._rpc.get_bdevs.side_effect = Exception("rpc timeout")
        _set_peer_response(peers[1], bdev_present=True, ctrl_states=["enabled"])
        disc, total = self._run(peers)
        # p0 skipped (no vote), p1 enabled
        self.assertEqual((disc, total), (0, 1))

    def test_controller_list_exception_skips_peer(self):
        peers = [_peer("p0"), _peer("p1")]
        peers[0]._rpc.get_bdevs.return_value = [{"name": "x"}]
        peers[0]._rpc.bdev_nvme_controller_list.side_effect = Exception("stuck")
        _set_peer_response(peers[1], bdev_present=True, ctrl_states=["failed"])
        disc, total = self._run(peers)
        # p0 skipped, p1 disconnected
        self.assertEqual((disc, total), (1, 1))

    def test_peer_without_jm_vuid_excluded(self):
        p_no_jm = _peer("p-nojm")
        p_no_jm.jm_vuid = None
        _set_peer_response(p_no_jm, bdev_present=True, ctrl_states=["failed"])
        p_ok = _peer("p-ok")
        _set_peer_response(p_ok, bdev_present=True, ctrl_states=["enabled"])
        disc, total = self._run([p_no_jm, p_ok])
        self.assertEqual((disc, total), (0, 1))  # nojm excluded; ok connected

    def test_bdev_lookup_uses_namespace_suffix(self):
        # Must query for remote_jm_{node}n1, not remote_jm_{node}.
        peers = [_peer("p0")]
        _set_peer_response(peers[0], bdev_present=True, ctrl_states=["enabled"])
        self._run(peers)
        peers[0]._rpc.get_bdevs.assert_called_once()
        (arg,), _ = peers[0]._rpc.get_bdevs.call_args
        self.assertEqual(arg, "remote_jm_target-noden1")

    def test_controller_lookup_uses_no_namespace_suffix(self):
        peers = [_peer("p0")]
        _set_peer_response(peers[0], bdev_present=True, ctrl_states=["enabled"])
        self._run(peers)
        peers[0]._rpc.bdev_nvme_controller_list.assert_called_once()
        (arg,), _ = peers[0]._rpc.bdev_nvme_controller_list.call_args
        self.assertEqual(arg, "remote_jm_target-node")


# ---------------------------------------------------------------------------
# is_node_data_plane_disconnected[_quorum]
# ---------------------------------------------------------------------------


class TestDataPlaneDisconnectedPredicates(unittest.TestCase):

    def _run_with_votes(self, votes):
        """Patch _count_data_plane_votes to return (disc, total) directly."""
        from simplyblock_core.services import storage_node_monitor as mod
        target = _target()
        with patch.object(mod, "_count_data_plane_votes", return_value=votes):
            return (mod.is_node_data_plane_disconnected(target),
                    mod.is_node_data_plane_disconnected_quorum(target))

    def test_no_peers_both_false(self):
        absolute, quorum = self._run_with_votes((0, 0))
        self.assertFalse(absolute)
        self.assertFalse(quorum)

    def test_all_disconnected_both_true(self):
        absolute, quorum = self._run_with_votes((3, 3))
        self.assertTrue(absolute)
        self.assertTrue(quorum)

    def test_majority_disconnected_quorum_true_absolute_false(self):
        absolute, quorum = self._run_with_votes((2, 3))
        self.assertFalse(absolute)
        self.assertTrue(quorum)

    def test_minority_disconnected_both_false(self):
        absolute, quorum = self._run_with_votes((1, 3))
        self.assertFalse(absolute)
        self.assertFalse(quorum)

    def test_exact_half_quorum_false(self):
        # With total=2 and disc=1, 1 > 2//2 -> 1 > 1 -> False.
        absolute, quorum = self._run_with_votes((1, 2))
        self.assertFalse(absolute)
        self.assertFalse(quorum)


if __name__ == "__main__":
    unittest.main()
