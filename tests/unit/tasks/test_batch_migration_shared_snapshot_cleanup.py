"""A batch group deletes a source snapshot its own members clone from (SFAM-2840).

MIG-T-008 moved LVOL_83 with its snapshot SNAP_84 and the two clones of it,
CLN_85 and CLN_86, as one batch. Only the owner's worker has SNAP_84 in its
plan, and it cleans up first, while the clones' records still place them on
the source -- so it kept the snapshot, no other worker ever reconsidered it,
and LVS_19/SNAP_84 outlived the batch. The next hop back to that node then
collided with it.

Pure logic tests: one in-memory DB stands in for FDB for both the controller
and the runners; RPC clients and bdev deletes are mocked.
"""
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.controllers.migration_controller as ctl
import simplyblock_core.services.tasks_runner_batch_migration as batch_runner
import simplyblock_core.services.tasks_runner_lvol_migration as runner
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

SRC, TGT = "node-src", "node-tgt"
SRC_LVS, TGT_LVS = "LVS_19", "LVS_7"


class _FakeDB:

    kv_store = None  # write_to_db is patched out; only passed through

    def __init__(self):
        self.lvols, self.snaps, self.migrations, self.groups = {}, {}, {}, {}

    def get_lvols_by_node_id(self, node_id):
        return [lv for lv in self.lvols.values() if lv.node_id == node_id]

    def _get(self, table, key):
        if key not in table:
            raise KeyError(key)
        return table[key]

    def get_lvol_by_id(self, uuid):
        return self._get(self.lvols, uuid)

    def get_snapshot_by_id(self, uuid):
        return self._get(self.snaps, uuid)

    def get_migration_by_id(self, uuid):
        return self._get(self.migrations, uuid)

    def get_migration_group_by_id(self, uuid):
        return self._get(self.groups, uuid)

    def atomic_update(self, obj, mutate):
        mutate(obj)
        return obj


def _lvol(uuid, bdev, cloned_from_snap=""):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = SRC
    lv.lvol_bdev = bdev
    lv.cloned_from_snap = cloned_from_snap
    return lv


def _node(node_id, lvstore):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.lvstore = lvstore
    n.secondary_node_id = ""
    n.tertiary_node_id = ""
    return n


def _tree():
    """LVOL_83 -> SNAP_84 -> {CLN_85, CLN_86}, all on the source, one batch group."""
    db = _FakeDB()
    db.lvols = {
        "lvol-83": _lvol("lvol-83", "LVOL_83"),
        "cln-85": _lvol("cln-85", "CLN_85", cloned_from_snap="snap-84"),
        "cln-86": _lvol("cln-86", "CLN_86", cloned_from_snap="snap-84"),
    }
    snap = SnapShot()
    snap.uuid = "snap-84"
    snap.snap_bdev = f"{SRC_LVS}/SNAP_84"
    snap.lvol = db.lvols["lvol-83"]
    db.snaps = {"snap-84": snap}

    group = LVolMigrationGroup()
    group.uuid = "grp"
    group.source_node_id = SRC
    group.target_node_id = TGT
    group.snap_owners = {"snap-84": "mig-83"}
    for ns_id, lvol_id in enumerate(("lvol-83", "cln-85", "cln-86"), start=1):
        m = LVolMigration()
        m.uuid = "mig-" + lvol_id.split("-")[1]
        m.lvol_id = lvol_id
        m.source_node_id = SRC
        m.target_node_id = TGT
        m.migration_group_id = "grp"
        # start_batch_migration: a shared snapshot is in its owner's plan only.
        owned = ["snap-84"] if lvol_id == "lvol-83" else []
        m.snap_migration_plan = list(owned)
        m.snaps_migrated = list(owned)
        m.transfer_context = {"source_lvol_bdev": db.lvols[lvol_id].lvol_bdev}
        db.migrations[m.uuid] = m
        group.members.append({"ns_id": ns_id, "migration_id": m.uuid})
    db.groups = {"grp": group}
    return db


def _apply_moves_record(db):
    """_apply_migration_to_db stand-in: the lvol and its owned snaps go to the target."""
    def _apply(migration, **_):
        db.lvols[migration.lvol_id].node_id = TGT
        for snap_uuid in migration.snaps_migrated:
            snap = db.snaps[snap_uuid]
            snap.snap_bdev = f"{TGT_LVS}/{snap.snap_bdev.split('/', 1)[1]}m"
        return True
    return _apply


class TestSharedSnapshotOfAGroup(unittest.TestCase):

    def setUp(self):
        self.db = _tree()
        self.src_node, self.tgt_node = _node(SRC, SRC_LVS), _node(TGT, TGT_LVS)
        self.src_rpc, self.tgt_rpc = MagicMock(), MagicMock()
        self.tgt_rpc.bdev_lvol_get_lvols.return_value = [{"name": f"{TGT_LVS}/SNAP_84m"}]
        self.worker_deletes = MagicMock()
        patches = [
            patch.object(ctl, "db", self.db),
            patch.object(runner, "db", self.db),
            patch.object(batch_runner, "db", self.db),
            patch.object(runner, "_delete_bdev_blocking", self.worker_deletes),
            patch.object(runner, "_apply_migration_to_db", _apply_moves_record(self.db)),
            patch.object(runner, "_get_source_secondary_node", return_value=None),
            patch.object(runner, "_get_source_tertiary_node", return_value=None),
            patch.object(runner, "_get_target_secondary_node", return_value=(None, None)),
            patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)),
            patch.object(runner, "_rename_migrated_bdevs"),
            patch.object(batch_runner, "_get_source_tertiary_node", return_value=None),
            patch.object(LVolMigration, "write_to_db"),
            patch.object(LVolMigrationGroup, "write_to_db"),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _run_worker_cleanups(self):
        # The order the run logged: owner first (lowest ns_id), then the clones.
        for mig_id in ("mig-83", "mig-85", "mig-86"):
            runner._handle_cleanup_source(
                self.db.migrations[mig_id], self.src_node, self.src_rpc,
                self.tgt_node, self.tgt_rpc)

    def _worker_deleted(self):
        return [c.args[0] for c in self.worker_deletes.call_args_list]

    def test_owner_defers_the_snapshot_instead_of_keeping_it(self):
        self._run_worker_cleanups()
        self.assertNotIn(f"{SRC_LVS}/SNAP_84", self._worker_deleted())  # clones still on it
        self.assertEqual(self.db.groups["grp"].source_snaps_deferred,
                         {"snap-84": f"{SRC_LVS}/SNAP_84"})

    def test_orchestrator_deletes_the_snapshot_after_the_barrier(self):
        self._run_worker_cleanups()
        with patch.object(batch_runner, "delete_bdev_blocking") as deletes:
            notes = batch_runner._delete_deferred_source_snaps(
                self.db.groups["grp"], self.src_node, self.src_rpc)
        deletes.assert_called_once()
        self.assertEqual(deletes.call_args.args[0], f"{SRC_LVS}/SNAP_84")
        self.assertEqual(deletes.call_args.kwargs["lvs_name"], SRC_LVS)
        self.assertEqual(notes, [])

    def test_a_sibling_left_on_the_source_still_protects_it(self):
        # CLN_86's worker failed instead of signalling: it is still on the source.
        for mig_id in ("mig-83", "mig-85"):
            runner._handle_cleanup_source(
                self.db.migrations[mig_id], self.src_node, self.src_rpc,
                self.tgt_node, self.tgt_rpc)
        with patch.object(batch_runner, "delete_bdev_blocking") as deletes:
            notes = batch_runner._delete_deferred_source_snaps(
                self.db.groups["grp"], self.src_node, self.src_rpc)
        deletes.assert_not_called()
        self.assertEqual(len(notes), 1)
        self.assertIn("cln-86", notes[0])

    def test_an_already_deleted_snapshot_is_skipped(self):
        # Orchestrator re-entry after a crash between delete and COMPLETED.
        self._run_worker_cleanups()
        self.src_rpc.bdev_get.return_value = []
        with patch.object(batch_runner, "delete_bdev_blocking") as deletes:
            notes = batch_runner._delete_deferred_source_snaps(
                self.db.groups["grp"], self.src_node, self.src_rpc)
        deletes.assert_not_called()
        self.assertEqual(notes, [])


class TestMovingLvolIds(unittest.TestCase):

    def test_moving_clones_no_longer_protect_their_parent(self):
        db = _tree()
        mig = db.migrations["mig-83"]
        with patch.object(ctl, "db", db):
            self.assertEqual(ctl.get_snaps_safe_to_delete_on_source(mig), set())
            self.assertEqual(
                ctl.get_snaps_safe_to_delete_on_source(
                    mig, moving_lvol_ids={"lvol-83", "cln-85", "cln-86"}),
                {"snap-84"})

    def test_a_clone_outside_the_group_still_protects_it(self):
        db = _tree()
        mig = db.migrations["mig-83"]
        with patch.object(ctl, "db", db):
            self.assertEqual(
                ctl.get_snaps_safe_to_delete_on_source(
                    mig, moving_lvol_ids={"lvol-83", "cln-85"}),
                set())


if __name__ == "__main__":
    unittest.main()
