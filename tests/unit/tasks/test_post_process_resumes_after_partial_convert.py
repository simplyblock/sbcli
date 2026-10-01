"""A migration retry after a partially finished post-process must resume where
it stopped, and a replica with no target bdev is skipped, not failed.

Live on 2026-09-28 (run 5, first CRD removal): the migration's source node
was also the target's replica (overlap). The convert on that replica failed
with "No such device", so the runner suspended; the retry then reused the
target bdev -- already converted on the primary -- and fired
bdev_lvol_transfer into it. The immutable snapshot rejected the writes, SPDK
dropped the lvstore's leadership and fenced the node's ports, and the node
read "down" for 20 s with its replica following. Two rules fix that:

* a replica that has no such bdev has nothing to link or convert, and cannot
  be in the split state the convert-on-both-sides rule guards against;
* a target bdev that is already an immutable snapshot is never transferred
  into again; post-processing resumes at whatever is still pending.

Pure logic tests: DB, RPC clients and events are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_lvol_migration as runner
from simplyblock_core.models.lvol_migration import LVolMigration

WRITABLE = [{"driver_specific": {"lvol": {"is_snapshot": False, "blobid": 7}}, "uuid": "u"}]
IMMUTABLE = [{"driver_specific": {"lvol": {"is_snapshot": True, "blobid": 7}}, "uuid": "u"}]


def _snap(uuid="snap-1"):
    s = MagicMock()
    s.uuid = uuid
    s.snap_name = "SNAP_46"
    s.snap_bdev = "LVS_1/SNAP_46"
    s.size = 10 * 1024 ** 3
    s.lvol = MagicMock()
    s.lvol.ha_type = "single"
    s.lvol.uuid = "lvol-1"
    return s


def _node(node_id, lvstore):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvstore = lvstore
    return n


def _migration():
    m = LVolMigration()
    m.uuid = "migration-1"
    m.lvol_id = "lvol-1"
    m.snaps_migrated = []
    m.snaps_preexisting_on_target = []
    m.target_snap_bdevs = []
    return m


def _post_process(tgt_rpc, sec_rpc=None):
    snap = _snap()
    tgt_node = _node("tgt", "LVS_10")
    tgt_sec = _node("sec", "LVS_10") if sec_rpc is not None else None
    transfer = {"snap_uuid": snap.uuid, "snap_short": "SNAP_46m", "snap_index": 0,
                "transfer_done": True, "post_done": False}
    mock_db = MagicMock()
    mock_db.get_snapshot_by_id.side_effect = KeyError("not in db")
    with patch.object(runner, "db", mock_db), \
         patch.object(runner.migration_controller, "get_snapshot_chain", return_value=[snap.uuid]), \
         patch.object(runner.migration_events, "migration_snap_copied"), \
         patch("simplyblock_core.controllers.lvol_controller.is_node_leader", return_value=True):
        return runner._post_process_snap(snap, tgt_node, tgt_rpc, _migration(), transfer,
                                         tgt_sec=tgt_sec, sec_rpc=sec_rpc)


class TestBdevIsImmutableSnapshot(unittest.TestCase):

    def test_reads_the_flag_and_tolerates_anything_else(self):
        self.assertTrue(runner._bdev_is_immutable_snapshot(IMMUTABLE))
        self.assertTrue(runner._bdev_is_immutable_snapshot(
            [{"driver_specific": {"lvol": {"snapshot": True}}}]))
        self.assertFalse(runner._bdev_is_immutable_snapshot(WRITABLE))
        self.assertFalse(runner._bdev_is_immutable_snapshot([]))
        self.assertFalse(runner._bdev_is_immutable_snapshot(None))
        self.assertFalse(runner._bdev_is_immutable_snapshot(runner._BDEV_INFO_UNSET))
        self.assertFalse(runner._bdev_is_immutable_snapshot([{"name": "x"}]))


class TestReplicaWithoutTheBdevIsSkipped(unittest.TestCase):

    def test_convert_is_not_attempted_on_a_replica_that_has_no_such_bdev(self):
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = WRITABLE
        tgt_rpc.bdev_lvol_convert.return_value = True
        sec_rpc = MagicMock()
        sec_rpc.get_bdevs.return_value = []  # overlap node: never registered
        ok, err = _post_process(tgt_rpc, sec_rpc)
        self.assertEqual((ok, err), (True, None))
        tgt_rpc.bdev_lvol_convert.assert_called_once_with("LVS_10/SNAP_46m")
        sec_rpc.bdev_lvol_convert.assert_not_called()

    def test_a_replica_that_has_the_bdev_is_still_converted(self):
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = WRITABLE
        tgt_rpc.bdev_lvol_convert.return_value = True
        sec_rpc = MagicMock()
        sec_rpc.get_bdevs.return_value = WRITABLE
        sec_rpc.bdev_lvol_convert.return_value = True
        ok, err = _post_process(tgt_rpc, sec_rpc)
        self.assertEqual((ok, err), (True, None))
        sec_rpc.bdev_lvol_convert.assert_called_once_with("LVS_10/SNAP_46m")

    def test_a_replica_convert_failure_still_fails(self):
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = WRITABLE
        tgt_rpc.bdev_lvol_convert.return_value = True
        sec_rpc = MagicMock()
        sec_rpc.get_bdevs.return_value = WRITABLE
        sec_rpc.bdev_lvol_convert.return_value = None
        ok, err = _post_process(tgt_rpc, sec_rpc)
        self.assertFalse(ok)
        self.assertIn("secondary", err)


class TestRetryResumesAfterThePrimaryConvert(unittest.TestCase):

    def test_primary_already_converted_is_not_converted_again(self):
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = IMMUTABLE
        sec_rpc = MagicMock()
        sec_rpc.get_bdevs.return_value = WRITABLE
        sec_rpc.bdev_lvol_convert.return_value = True
        ok, err = _post_process(tgt_rpc, sec_rpc)
        self.assertEqual((ok, err), (True, None))
        tgt_rpc.bdev_lvol_convert.assert_not_called()
        tgt_rpc.bdev_lvol_add_clone.assert_not_called()
        # the pending replica convert is what the retry is for
        sec_rpc.bdev_lvol_convert.assert_called_once_with("LVS_10/SNAP_46m")

    def test_intermediate_retry_does_not_transfer_into_a_converted_target(self):
        """The solo intermediate round finds its owned target bdev already
        immutable: no bdev_lvol_transfer, straight to post-processing."""
        migration = _migration()
        migration.intermediate_snap_rounds = 0
        migration.max_intermediate_snap_rounds = 1
        migration.snap_migration_plan = []
        migration.transfer_context = {}
        migration.write_to_db = MagicMock()
        src_node = _node("src", "LVS_1")
        tgt_node = _node("tgt", "LVS_10")
        snap = _snap("snap-i")
        composite = "LVS_10/" + runner._snap_tgt_short_name(snap)
        migration.target_snap_bdevs = [composite]

        src_rpc = MagicMock()
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = IMMUTABLE

        lvol = MagicMock()
        lvol.lvol_bdev = "LVOL_1"
        lvol.size = 10 * 1024 ** 3
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_snapshot_by_id.return_value = snap

        def take_snapshot(m):
            m.snap_migration_plan = [snap.uuid]
            m.intermediate_snap_rounds += 1

        seen = {}

        def post_process(s, tn, tr, m, transfer, **kw):
            seen["transfer"] = transfer
            return True, None

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_get_lvol_delta_bytes", return_value=None), \
             patch.object(runner, "_take_intermediate_snapshot", side_effect=take_snapshot), \
             patch.object(runner, "_setup_snap_transfer") as setup, \
             patch.object(runner, "_post_process_snap", side_effect=post_process):
            done, suspend, err = runner._handle_snap_copy(
                migration, src_node, tgt_node, src_rpc, tgt_rpc)

        self.assertEqual((done, suspend, err), (True, False, None))
        setup.assert_not_called()
        src_rpc.bdev_lvol_transfer.assert_not_called()
        self.assertTrue(seen["transfer"]["transfer_done"])
        self.assertEqual(seen["transfer"]["snap_short"], runner._snap_tgt_short_name(snap))


class TestFailedIntermediateSnapshotTakesNothing(unittest.TestCase):
    """When the intermediate snapshot cannot be taken (snapshot limit, say)
    the plan is unchanged and its last entry is an already-migrated PLANNED
    snapshot. Falling back to plan[-1] re-migrated that snapshot and listed
    it twice (tests/integration/migration/test_scalability.py: 251 of 250).
    The round is skipped instead."""

    def test_no_transfer_and_no_duplicate_when_the_snapshot_was_not_taken(self):
        migration = _migration()
        migration.intermediate_snap_rounds = 0
        migration.max_intermediate_snap_rounds = 3
        migration.snap_migration_plan = ["snap-p"]
        migration.snaps_migrated = ["snap-p"]
        migration.transfer_context = {}
        migration.write_to_db = MagicMock()
        src_node = _node("src", "LVS_1")
        tgt_node = _node("tgt", "LVS_10")
        lvol = MagicMock()
        lvol.lvol_bdev = "LVOL_1"
        lvol.size = 10 * 1024 ** 3
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_snapshot_by_id.return_value = _snap("snap-p")
        tgt_rpc = MagicMock()
        tgt_rpc.bdev_lvol_get_lvols.return_value = []
        tgt_rpc.get_bdevs.return_value = IMMUTABLE

        def failed_take(m):
            m.intermediate_snap_rounds = m.max_intermediate_snap_rounds  # as the real one does

        with patch.object(runner, "db", mock_db), \
             patch.object(runner, "_get_lvol_delta_bytes", return_value=None), \
             patch.object(runner, "_take_intermediate_snapshot", side_effect=failed_take), \
             patch.object(runner, "_setup_snap_transfer") as setup, \
             patch.object(runner, "_post_process_snap") as post:
            done, suspend, err = runner._handle_snap_copy(
                migration, src_node, tgt_node, MagicMock(), tgt_rpc)

        self.assertEqual((done, suspend, err), (True, False, None))
        setup.assert_not_called()
        post.assert_not_called()
        self.assertEqual(migration.snaps_migrated, ["snap-p"])

    def test_listing_a_snapshot_as_migrated_is_idempotent(self):
        tgt_rpc = MagicMock()
        tgt_rpc.get_bdevs.return_value = WRITABLE
        tgt_rpc.bdev_lvol_convert.return_value = True
        snap = _snap()
        tgt_node = _node("tgt", "LVS_10")
        migration = _migration()
        migration.snaps_migrated = [snap.uuid]
        transfer = {"snap_uuid": snap.uuid, "snap_short": "SNAP_46m", "snap_index": 0,
                    "transfer_done": True, "post_done": False}
        mock_db = MagicMock()
        mock_db.get_snapshot_by_id.side_effect = KeyError("not in db")
        with patch.object(runner, "db", mock_db), \
             patch.object(runner.migration_controller, "get_snapshot_chain", return_value=[snap.uuid]), \
             patch.object(runner.migration_events, "migration_snap_copied"), \
             patch("simplyblock_core.controllers.lvol_controller.is_node_leader", return_value=True):
            ok, err = runner._post_process_snap(snap, tgt_node, tgt_rpc, migration, transfer)
        self.assertEqual((ok, err), (True, None))
        self.assertEqual(migration.snaps_migrated, [snap.uuid])


if __name__ == "__main__":
    unittest.main()
