# coding=utf-8
"""Create-rollback delete protocol (run 20260725, SNAP_3299).

An async delete must ALWAYS be followed by sync deletes on EVERY non-leader
HA member of the LVS — never on the leader (sync deletes clear the peers'
lvol registrations; the leader's async pass removed its own state). A failed
register RPC never proves the peer holds no registration, so the owed set is
unconditional. Unreachable peers get a durable sync-delete task; -19 ("No
such device") counts as already-clean.
"""
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.storage_node import StorageNode


def _mk_node(node_id, status=StorageNode.STATUS_ONLINE,
             sync_ret=(True, None)):
    rpc = MagicMock()
    rpc.delete_lvol.return_value = sync_ret
    rpc.bdev_lvol_get_lvol_delete_status.return_value = 0
    return types.SimpleNamespace(
        get_id=lambda: node_id, status=status, cluster_id="c1",
        rpc_client=lambda *a, **k: rpc, _rpc=rpc)


class TestRollbackOwesAllNonLeaders(unittest.TestCase):
    def setUp(self):
        patcher_tasks = patch.object(
            snapshot_controller.tasks_controller, "add_lvol_sync_del_task")
        self.mock_task = patcher_tasks.start()
        self.addCleanup(patcher_tasks.stop)
        patcher_sleep = patch.object(snapshot_controller.time, "sleep")
        patcher_sleep.start()
        self.addCleanup(patcher_sleep.stop)

    def _rollback(self, leader, members):
        snapshot_controller._rollback_snapshot_bdev(
            "c1", "LVS_1", leader, "SNAP_X", members, lock=False)

    def test_every_online_non_leader_gets_sync_delete(self):
        leader = _mk_node("leader-1")
        sec = _mk_node("sec-2")
        tert = _mk_node("tert-3")
        self._rollback(leader, [leader, sec, tert])

        # leader: two calls, one sync, one async
        assert leader._rpc.delete_lvol.call_count == 2
        sec._rpc.delete_lvol.assert_called_once_with(
            "LVS_1/SNAP_X", sync=True)
        tert._rpc.delete_lvol.assert_called_once_with(
            "LVS_1/SNAP_X", sync=True)
        self.mock_task.assert_not_called()

    def test_sync_delete_owed_even_when_registration_never_succeeded(self):
        # The 20260725 regression: register answered -19 on the peer, the old
        # owed-set (registered + restart-gated) was empty -> async-only
        # delete. The peer must be owed regardless of registration outcome;
        # its -19 answer to the sync delete is the tolerated "already clean".
        leader = _mk_node("leader-1")
        sec = _mk_node("sec-2", sync_ret=(False, {"code": -19}))
        self._rollback(leader, [leader, sec])

        sec._rpc.delete_lvol.assert_called_once_with(
            "LVS_1/SNAP_X", sync=True)
        self.mock_task.assert_not_called()  # -19 == clean, no durable task

    def test_unreachable_peer_gets_durable_task(self):
        leader = _mk_node("leader-1")
        down = _mk_node("sec-2", status=StorageNode.STATUS_UNREACHABLE)
        self._rollback(leader, [leader, down])

        down._rpc.delete_lvol.assert_not_called()
        self.mock_task.assert_called_once_with(
            "c1", "sec-2", "LVS_1/SNAP_X", "leader-1")

    def test_failed_sync_delete_gets_durable_task(self):
        leader = _mk_node("leader-1")
        sec = _mk_node("sec-2", sync_ret=(False, {"code": -32603}))
        self._rollback(leader, [leader, sec])

        self.mock_task.assert_called_once_with(
            "c1", "sec-2", "LVS_1/SNAP_X", "leader-1")
