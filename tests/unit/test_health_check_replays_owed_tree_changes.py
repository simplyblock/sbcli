"""The health check replays the snapshot-tree changes a node owes
(replica_chain.replay_pending) once its SPDK answers again.

A member that missed a bdev_lvol_add_clone / bdev_lvol_convert while it could
not be reached, and keeps serving without reloading its lvstores, keeps the
tree it had then (bf23 2026-11-12, LVS_16). The health check is the pass that
notices it answers again; it must tell the node, and never fail on it.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import health_check_service as hcs


def _snode(status=StorageNode.STATUS_UNREACHABLE):
    n = MagicMock()
    n.get_id.return_value = "n-1"
    n.status = status
    n.nvme_devices = []
    n.cluster_id = "c1"
    return n


class TestHealthCheckReplay(unittest.TestCase):

    def _check(self, snode, rpc_ok):
        with patch.object(hcs, "db") as db, \
                patch.object(hcs.health_controller, "_check_node_ping", return_value=True), \
                patch.object(hcs.health_controller, "_check_node_api", return_value=True), \
                patch.object(hcs.health_controller, "check_node_rpc", return_value=rpc_ok), \
                patch.object(hcs, "set_node_health_check"), \
                patch.object(hcs.replica_chain, "replay_pending") as replay:
            db.get_storage_node_by_id.return_value = snode
            hcs.check_node(snode)
        return replay

    def test_a_node_that_answers_again_is_told_what_it_missed(self):
        snode = _snode()
        replay = self._check(snode, rpc_ok=True)
        replay.assert_called_once()
        self.assertIs(replay.call_args.args[0], snode)

    def test_a_node_that_does_not_answer_is_left_alone(self):
        replay = self._check(_snode(), rpc_ok=False)
        replay.assert_not_called()

    def test_a_failed_replay_does_not_fail_the_health_check(self):
        snode = _snode(StorageNode.STATUS_ONLINE)
        with patch.object(hcs, "db") as db, \
                patch.object(hcs.health_controller, "_check_node_ping", return_value=True), \
                patch.object(hcs.health_controller, "_check_node_api", return_value=True), \
                patch.object(hcs.health_controller, "check_node_rpc", return_value=True), \
                patch.object(hcs, "set_node_health_check") as set_health, \
                patch.object(hcs.replica_chain, "replay_pending", side_effect=RuntimeError("x")):
            db.get_storage_node_by_id.return_value = snode
            hcs.check_node(snode)
        set_health.assert_called_once()


if __name__ == "__main__":
    unittest.main()
