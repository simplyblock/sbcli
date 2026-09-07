"""Snapshot-delete phase-2 semantics (run 20260725).

Phase-1 (async delete) is leader-only; phase-2 (per-node sync deletes) needs
NO leader. The monitor previously (a) raised per snapshot per cycle when no
leader was confirmable — 244,072 "Failed to get leader node" in one hour
while 18k completed phase-1s waited for sync deletes that never came — and
(b) re-issued phase-1 on the new leader whenever leadership moved.

Under test on snapshot_monitor.process_snap_delete:
  - leaderless + phase-1 confirmed complete on the recorded node -> finish
    (phase-2) runs;
  - leaderless + phase-1 still in flight -> wait, no error storm;
  - leaderless + phase-1 never issued -> defer quietly (rate-limited warn);
  - leadership moved + recorded node still owns a live/complete async ->
    NO re-issue;
  - leadership moved + recorded node provably lost the async (status 4) ->
    re-issue on the current leader.
"""
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import snapshot_monitor
from simplyblock_core.models.storage_node import StorageNode


def _mk_node(node_id, leadership=False, delete_status=None,
             secondary_node_id="", tertiary_node_id=""):
    rpc = MagicMock()
    rpc.bdev_lvol_get_lvstores.return_value = [
        {"name": "LVS_1", "lvs leadership": leadership}]
    if delete_status is not None:
        rpc.bdev_lvol_get_lvol_delete_status.return_value = delete_status
    rpc.delete_lvol.return_value = (True, None)
    return types.SimpleNamespace(
        get_id=lambda: node_id, status=StorageNode.STATUS_ONLINE,
        lvstore="LVS_1", secondary_node_id=secondary_node_id,
        tertiary_node_id=tertiary_node_id,
        rpc_client=lambda *a, **k: rpc, _rpc=rpc)


def _mk_snap(deletion_status=""):
    return types.SimpleNamespace(
        get_id=lambda: "snap-1",
        snap_bdev="LVS_1/SNAP_1",
        deletion_status=deletion_status,
        instances=[],
        status="in_deletion",
        lvol=types.SimpleNamespace(lvs_name="LVS_1", node_id="host-1",
                                   get_id=lambda: "lvol-1"),
        write_to_db=MagicMock())


class TestPhase2WithoutLeader(unittest.TestCase):
    def setUp(self):
        patcher_db = patch.object(snapshot_monitor, "db")
        self.db = patcher_db.start()
        self.addCleanup(patcher_db.stop)
        patcher_finish = patch.object(snapshot_monitor,
                                      "process_snap_delete_finish")
        self.mock_finish = patcher_finish.start()
        self.addCleanup(patcher_finish.stop)
        patcher_sleep = patch.object(snapshot_monitor.time, "sleep")
        patcher_sleep.start()
        self.addCleanup(patcher_sleep.stop)
        snapshot_monitor._leaderless_warn_memo.clear()

    def test_leaderless_completed_phase1_finishes(self):
        host = _mk_node("host-1", leadership=False)
        recorded = _mk_node("rec-1", leadership=False, delete_status=0)
        self.db.get_storage_node_by_id.return_value = recorded
        snap = _mk_snap(deletion_status="rec-1")

        ok = snapshot_monitor.process_snap_delete(snap, host, all_mini_lvols=[])

        self.assertTrue(ok)
        self.mock_finish.assert_called_once_with(snap, recorded)
        recorded._rpc.delete_lvol.assert_not_called()

    def test_leaderless_inflight_phase1_waits(self):
        host = _mk_node("host-1", leadership=False)
        recorded = _mk_node("rec-1", leadership=False, delete_status=1)
        self.db.get_storage_node_by_id.return_value = recorded
        snap = _mk_snap(deletion_status="rec-1")

        ok = snapshot_monitor.process_snap_delete(snap, host, all_mini_lvols=[])

        self.assertTrue(ok)
        self.mock_finish.assert_not_called()
        recorded._rpc.delete_lvol.assert_not_called()

    def test_leaderless_no_phase1_defers_quietly(self):
        host = _mk_node("host-1", leadership=False)
        snap = _mk_snap(deletion_status="")

        ok = snapshot_monitor.process_snap_delete(snap, host, all_mini_lvols=[])

        self.assertFalse(ok)
        self.mock_finish.assert_not_called()
        host._rpc.delete_lvol.assert_not_called()

    def test_leaderless_warning_is_rate_limited(self):
        host = _mk_node("host-1", leadership=False)
        snap = _mk_snap(deletion_status="")
        with patch.object(snapshot_monitor.logger, "warning") as warn:
            for _ in range(50):
                snapshot_monitor.process_snap_delete(
                    snap, host, all_mini_lvols=[])
        self.assertEqual(warn.call_count, 1)


class TestNoReissueOnLeadershipMove(unittest.TestCase):
    def setUp(self):
        patcher_db = patch.object(snapshot_monitor, "db")
        self.db = patcher_db.start()
        self.addCleanup(patcher_db.stop)
        patcher_finish = patch.object(snapshot_monitor,
                                      "process_snap_delete_finish")
        self.mock_finish = patcher_finish.start()
        self.addCleanup(patcher_finish.stop)
        patcher_sleep = patch.object(snapshot_monitor.time, "sleep")
        patcher_sleep.start()
        self.addCleanup(patcher_sleep.stop)

        # Host IS the current leader; phase-1 was recorded elsewhere.
        self.host = _mk_node("host-1", leadership=True, delete_status=1)
        self.snap = _mk_snap(deletion_status="old-leader")
        self.recorded = _mk_node("old-leader", leadership=False)

        def by_id(node_id):
            return {"old-leader": self.recorded,
                    "host-1": self.host}[node_id]
        self.db.get_storage_node_by_id.side_effect = by_id
        self.db.get_snapshot_by_id.return_value = self.snap

    def test_completed_on_recorded_node_finishes_without_reissue(self):
        self.recorded._rpc.bdev_lvol_get_lvol_delete_status.return_value = 2
        ok = snapshot_monitor.process_snap_delete(
            self.snap, self.host, all_mini_lvols=[])
        self.assertTrue(ok)
        self.mock_finish.assert_called_once_with(self.snap, self.recorded)
        self.host._rpc.delete_lvol.assert_not_called()

    def test_inflight_on_recorded_node_waits_without_reissue(self):
        self.recorded._rpc.bdev_lvol_get_lvol_delete_status.return_value = 1
        ok = snapshot_monitor.process_snap_delete(
            self.snap, self.host, all_mini_lvols=[])
        self.assertTrue(ok)
        self.host._rpc.delete_lvol.assert_not_called()

    def test_lost_async_reissues_on_current_leader(self):
        self.recorded._rpc.bdev_lvol_get_lvol_delete_status.return_value = 4
        snapshot_monitor.process_snap_delete(
            self.snap, self.host, all_mini_lvols=[])
        self.host._rpc.delete_lvol.assert_called_once_with(
            "LVS_1/SNAP_1", sync=False, special_delete=False)

    def test_transient_poll_error_retries_next_cycle(self):
        self.recorded._rpc.bdev_lvol_get_lvol_delete_status.side_effect = (
            RuntimeError("timeout"))
        ok = snapshot_monitor.process_snap_delete(
            self.snap, self.host, all_mini_lvols=[])
        self.assertFalse(ok)
        self.host._rpc.delete_lvol.assert_not_called()
