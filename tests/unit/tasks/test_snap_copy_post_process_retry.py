"""Unit tests for _handle_snap_copy / _handle_group_snap_copy in
tasks_runner_lvol_migration.py: a post-processing failure after a completed
data transfer must not discard the completed transfer.

Root cause this guards against: _post_process_snap(_group) failing used to
reset migration.transfer_context to {}, discarding the just-completed
transfer_done=True marker. The next tick would then treat the snapshot as
never-started and re-run bdev_lvol_transfer from scratch (re-copying data
that was already on the target) instead of just retrying post-processing.

Pure logic tests with the DB and node/RPC objects mocked, so this belongs in
the unit tier rather than integration/migration (which provisions a real FDB).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_migration import LVolMigration

import simplyblock_core.services.tasks_runner_lvol_migration as runner


def _migration(transfers):
    m = LVolMigration()
    m.uuid = "migration-1"
    m.lvol_id = "lvol-1"
    m.snap_migration_plan = [t["snap_uuid"] for t in transfers]
    m.transfer_context = {"stage": "parallel_transfer", "transfers": transfers}
    return m


def _node(node_id="node-1", lvstore="LVS_1"):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvstore = lvstore
    return n


def _snap(snap_bdev="LVS_1/SNAP_1"):
    s = MagicMock()
    s.snap_bdev = snap_bdev
    return s


class TestSnapCopyPostProcessRetry(unittest.TestCase):
    """Solo migration: _handle_snap_copy."""

    def test_post_process_failure_keeps_completed_transfer_marked_done(self):
        transfer = {"snap_uuid": "snap-1", "snap_short": "SNAP_1m",
                    "snap_index": 0, "transfer_done": True, "post_done": False}
        migration = _migration([transfer])
        src_node = _node("src")
        tgt_node = _node("tgt")

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("no lvol")
        mock_db.get_snapshot_by_id.return_value = _snap()

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_get_target_secondary_node", return_value=(None, None)), \
             patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)), \
             patch.object(runner, "_post_process_snap", return_value=(False, "add_clone failed")):
            done, suspend, error = runner._handle_snap_copy(
                migration, src_node, tgt_node, MagicMock(), MagicMock())

        assert (done, suspend) == (False, True)
        assert error == "add_clone failed"
        # The completed transfer must still be marked done -- not wiped.
        assert migration.transfer_context.get("stage") == "parallel_transfer"
        kept = migration.transfer_context["transfers"][0]
        assert kept["transfer_done"] is True
        assert kept["post_done"] is False

    def test_next_tick_retries_post_process_without_re_transferring(self):
        """After the failure above, a second call must go straight back to
        _post_process_snap -- bdev_lvol_transfer_stat must not be consulted
        again for a transfer already marked done."""
        transfer = {"snap_uuid": "snap-1", "snap_short": "SNAP_1m",
                    "snap_index": 0, "transfer_done": True, "post_done": False}
        migration = _migration([transfer])
        src_node = _node("src")
        tgt_node = _node("tgt")
        src_rpc = MagicMock()

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("no lvol")
        mock_db.get_snapshot_by_id.return_value = _snap()

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_get_target_secondary_node", return_value=(None, None)), \
             patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)), \
             patch.object(runner, "_post_process_snap", return_value=(True, None)):
            done, suspend, error = runner._handle_snap_copy(
                migration, src_node, tgt_node, src_rpc, MagicMock())

        assert error is None
        src_rpc.bdev_lvol_transfer_stat.assert_not_called()


class TestTransferDonePersistsBeforePostProcessing(unittest.TestCase):
    """SPDK destroys the source-side transfer task as soon as it reports
    Done (confirmed live: destroy_xfer_task fires within the same tick the
    stat check reports success). If _post_process_snap(_group) then raises
    -- an uncaught RPCException, not just a clean (False, err) return --
    before the old code's persist-after-post-process write ever ran, the
    in-memory transfer_done=True update was lost: the next tick would see
    transfer_done=False, re-poll bdev_lvol_transfer_stat, find the
    already-destroyed task, misread that as Failed/No process, and wipe
    transfer_context -- forcing a full re-transfer of data already on the
    target. Persisting transfer_done=True immediately (before
    _post_process_snap runs, not after) closes that window regardless of how
    post-processing fails.
    """

    def test_solo_persists_transfer_done_even_if_post_process_raises(self):
        transfer = {"snap_uuid": "snap-1", "snap_short": "SNAP_1m",
                    "snap_index": 0, "transfer_done": False, "post_done": False}
        migration = _migration([transfer])
        src_node = _node("src")
        tgt_node = _node("tgt")
        src_rpc = MagicMock()
        src_rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "Done"}

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("no lvol")
        mock_db.get_snapshot_by_id.return_value = _snap()

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_get_target_secondary_node", return_value=(None, None)), \
             patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)), \
             patch.object(runner, "_post_process_snap",
                          side_effect=RuntimeError("connection error")):
            with self.assertRaises(RuntimeError):
                runner._handle_snap_copy(migration, src_node, tgt_node, src_rpc, MagicMock())

        # transfer_done must already be persisted, even though the exception
        # escaped before _handle_snap_copy's own post-process bookkeeping ran.
        assert migration.transfer_context.get("stage") == "parallel_transfer"
        kept = migration.transfer_context["transfers"][0]
        assert kept["transfer_done"] is True
        assert kept["post_done"] is False

    def test_group_persists_transfer_done_even_if_post_process_raises(self):
        transfer = {"snap_uuid": "snap-1", "snap_short": "SNAP_1m",
                    "snap_index": 0, "transfer_done": False, "post_done": False}
        migration = _migration([transfer])
        src_node = _node("src")
        tgt_node = _node("tgt")
        src_rpc = MagicMock()
        src_rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "Done"}

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("no lvol")
        mock_db.get_snapshot_by_id.return_value = _snap()

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_post_process_snap_group",
                          side_effect=RuntimeError("connection error")):
            with self.assertRaises(RuntimeError):
                runner._handle_group_snap_copy(
                    migration, src_node, tgt_node, src_rpc, MagicMock())

        kept = migration.transfer_context["transfers"][0]
        assert kept["transfer_done"] is True
        assert kept.get("post_done") is not True


class TestGroupSnapCopyPostProcessRetry(unittest.TestCase):
    """Batch/group worker migration: _handle_group_snap_copy."""

    def test_post_process_failure_keeps_completed_transfer_marked_done(self):
        transfer = {"snap_uuid": "snap-1", "snap_short": "SNAP_1m",
                    "snap_index": 0, "transfer_done": True, "post_done": False}
        migration = _migration([transfer])
        src_node = _node("src")
        tgt_node = _node("tgt")

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("no lvol")
        mock_db.get_snapshot_by_id.return_value = _snap()

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_post_process_snap_group", return_value=(False, "boom")):
            done, suspend, error = runner._handle_group_snap_copy(
                migration, src_node, tgt_node, MagicMock(), MagicMock())

        assert (done, suspend) == (False, True)
        assert error == "boom"
        kept = migration.transfer_context["transfers"][0]
        assert kept["transfer_done"] is True
        assert kept.get("post_done") is not True


if __name__ == "__main__":
    unittest.main()
