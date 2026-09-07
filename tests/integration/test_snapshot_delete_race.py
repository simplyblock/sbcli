"""Unit tests for the snapshot/clone delete-race fixes.

Fixes covered, numbered to match the section banners below. Fix 1 was the
``get_random_vuid`` bdev-name dedupe; it no longer exists — vuids are now
allocated monotonically via ``DBController.next_vuid``, which cannot collide,
so the tests for it were removed.

2. ``snapshot_controller.add`` rejects creating a snapshot from an lvol
   in ``STATUS_IN_DELETION``, and ``snapshot_controller.clone`` rejects
   cloning a snapshot that is ``deleted`` or in ``STATUS_IN_DELETION``.
   This closes the window between an async snapshot delete being issued
   and a fresh clone-create slipping through against the same snapshot
   — the sequence that produced the stuck-snapshot
   ``open_ref=2 / clone-entries empty`` metadata inconsistency
   (incident: aws_dual_soak 2026-04-30, 14 stuck snapshots).

3. ``snapshot_controller.delete`` blocks the snapshot's hard-delete
   while any clone's SPDK-side delete is still in flight. Previously
   any IN_DELETION clone was treated as "already gone" and the snap
   delete proceeded to call SPDK, which returned EBUSY because the
   clone's bdev was still open. Now a clone is only treated as gone
   when its ``deletion_status`` field has been set (i.e. the leader's
   ``delete_lvol_from_node`` returned). Otherwise the snapshot is
   soft-deleted and the clone's own delete-completion path will
   re-trigger the hard delete once SPDK has actually released it.

All external dependencies (FDB, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


def _cluster():
    c = Cluster()
    c.uuid = "cluster-1"
    c.nqn = "nqn.test:cluster-1"
    c.status = Cluster.STATUS_ACTIVE
    return c


def _pool():
    p = Pool()
    p.uuid = "pool-1"
    p.cluster_id = "cluster-1"
    p.status = Pool.STATUS_ACTIVE
    p.lvol_max_size = 0
    p.pool_max_size = 0
    return p


def _node():
    n = StorageNode()
    n.uuid = "node-1"
    n.status = StorageNode.STATUS_ONLINE
    n.cluster_id = "cluster-1"
    n.lvstore = "LVS_100"
    n.lvstore_status = "ready"
    n.lvstore_stack = []
    n.hostname = "h1"
    n.lvol_sync_del = MagicMock(return_value=False)
    n.max_lvol = 1000
    return n


def _lvol(uuid, status=LVol.STATUS_ONLINE, lvol_bdev=None,
          deletion_status=""):
    lv = LVol()
    lv.uuid = uuid
    lv.status = status
    lv.node_id = "node-1"
    lv.pool_uuid = "pool-1"
    lv.cluster_id = "cluster-1"
    lv.lvol_name = f"VOL_{uuid}"
    lv.lvol_bdev = lvol_bdev or f"LVOL_{uuid}"
    lv.top_bdev = f"LVS_100/{lv.lvol_bdev}"
    lv.lvs_name = "LVS_100"
    lv.size = 1024 ** 3
    lv.max_size = 0
    lv.base_bdev = "raid0_100"
    lv.ha_type = "ha"
    lv.nodes = ["node-1"]
    lv.allowed_hosts = []
    lv.cloned_from_snap = ""
    lv.namespace = ""
    lv.max_namespace_per_subsys = 1
    lv.subsys_port = 4420
    lv.crypto_bdev = ""
    lv.vuid = 100
    lv.ndcs = 1
    lv.npcs = 1
    lv.deletion_status = deletion_status
    return lv


def _snapshot(uuid, lvol, snap_bdev=None,
              status=SnapShot.STATUS_ONLINE, deleted=False, ref_count=0):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.snap_name = f"SNAP_NAME_{uuid}"
    s.snap_bdev = snap_bdev or f"SNAP_{uuid}"
    s.status = status
    s.deleted = deleted
    s.ref_count = ref_count
    s.snap_ref_id = ""
    s.size = lvol.size
    s.cluster_id = "cluster-1"
    s.vuid = 100
    return s


# ---------------------------------------------------------------------------
# Fix 2a: snapshot_controller.add rejects on lvol in deletion
# ---------------------------------------------------------------------------

class TestRejectSnapshotAddOnDeletingLvol(unittest.TestCase):

    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_add_rejects_when_lvol_is_in_deletion(self, mock_db):
        """Creating a snapshot from an lvol that is mid-delete must be
        rejected. SPDK's blobstore ties the snapshot's parent metadata
        to the source lvol; if the lvol is being deleted concurrently
        the resulting snapshot can end up with a dangling parent_id
        and no recoverable lineage."""
        from simplyblock_core.controllers import snapshot_controller

        deleting = _lvol("lv-deleting", status=LVol.STATUS_IN_DELETION)
        mock_db.get_lvol_by_id.return_value = deleting

        ok, msg = snapshot_controller.add("lv-deleting", "snap-name-1")

        self.assertFalse(ok)
        self.assertIn("in deletion", msg.lower())
        # Must not have proceeded to look up a pool / storage node /
        # issue any RPCs.
        mock_db.get_pool_by_id.assert_not_called()
        mock_db.get_storage_node_by_id.assert_not_called()


# ---------------------------------------------------------------------------
# Fix 2b: snapshot_controller.clone rejects on snapshot in deletion
# ---------------------------------------------------------------------------

class TestRejectCloneOnDeletingSnapshot(unittest.TestCase):

    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_clone_rejects_when_snap_is_soft_deleted(self, mock_db):
        """A snapshot whose ``deleted`` flag is True is being torn down
        (the soft-delete branch waits for clones to drain). Cloning
        from it now would produce the same stuck-snapshot metadata
        inconsistency that motivated this fix."""
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        snap = _snapshot("snap-deleted", src, deleted=True)
        mock_db.get_snapshot_by_id.return_value = snap

        ok, msg = snapshot_controller.clone("snap-deleted", "clone-name-1")

        self.assertFalse(ok)
        self.assertIn("deletion", msg.lower())
        mock_db.get_pool_by_id.assert_not_called()

    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_clone_rejects_when_snap_status_is_in_deletion(self, mock_db):
        """``status == STATUS_IN_DELETION`` is the equivalent state for
        the synchronous delete path. Reject just like ``deleted=True``."""
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        snap = _snapshot("snap-in-del", src,
                         status=SnapShot.STATUS_IN_DELETION)
        mock_db.get_snapshot_by_id.return_value = snap

        ok, msg = snapshot_controller.clone("snap-in-del", "clone-name-1")

        self.assertFalse(ok)
        self.assertIn("deletion", msg.lower())

    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_clone_proceeds_for_healthy_snapshot(self, mock_db):
        """Sanity: the rejection guard only fires for deleting snapshots.
        A healthy snapshot continues into the rest of the function (here
        we let it fall through to the next mocked check, the pool-lookup,
        which proves we're past the new guard)."""
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        snap = _snapshot("snap-healthy", src,
                         status=SnapShot.STATUS_ONLINE, deleted=False)
        mock_db.get_snapshot_by_id.return_value = snap
        # Make pool lookup fail so we exit cleanly after the guard
        # we want to verify is *not* triggered.
        mock_db.get_pool_by_id.side_effect = KeyError("pool gone")

        ok, msg = snapshot_controller.clone("snap-healthy", "clone-name-1")

        self.assertFalse(ok)
        # We exited at the pool lookup, NOT at the in-deletion guard.
        self.assertIn("pool", msg.lower())


# ---------------------------------------------------------------------------
# Fix 3: snapshot.delete blocks while clone's SPDK delete is in flight
# ---------------------------------------------------------------------------

class TestSnapshotDeleteWaitsForCloneInFlight(unittest.TestCase):

    def _setup_db(self, mock_db, snap, clone, node):
        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_pool_by_id.return_value = _pool()
        mock_db.get_storage_node_by_id.return_value = node
        # delete() scans clones via get_mini_lvols() (the lightweight
        # LVolMini projection), not get_lvols(). Only cloned_from_snap,
        # status and deletion_status are read off each entry, all of
        # which the full LVol fixture exposes, so the fixture stands in
        # for the mini projection here.
        mock_db.get_mini_lvols.return_value = [clone]
        mock_db.get_lvols.return_value = [clone]
        mock_db.get_cluster_by_id.return_value = _cluster()
        mock_db.kv_store = MagicMock()
        # No active migrations / backups
        mock_db.get_backups_by_snapshot_id.return_value = []

    @patch("simplyblock_core.controllers.migration_controller.get_active_migration_for_lvol")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_soft_deletes_when_clone_alive(self, mock_db, mock_get_active_mig):
        """Existing behaviour: an ONLINE clone keeps the snapshot in
        soft-delete (deferred). Locked in to make sure the broader
        change doesn't regress this case."""
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        snap = _snapshot("snap-1", src)
        clone = _lvol("cl-alive", status=LVol.STATUS_ONLINE)
        clone.cloned_from_snap = "snap-1"
        node = _node()

        self._setup_db(mock_db, snap, clone, node)
        mock_get_active_mig.return_value = None

        ok = snapshot_controller.delete("snap-1", lock=False)

        self.assertTrue(ok)  # soft delete returns True
        self.assertTrue(snap.deleted)

    @patch("simplyblock_core.controllers.migration_controller.get_active_migration_for_lvol")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_soft_deletes_when_clone_in_deletion_but_spdk_not_done(
            self, mock_db, mock_get_active_mig):
        """**Fix 3**: an IN_DELETION clone whose ``deletion_status`` is
        unset means SPDK has NOT yet removed the clone's bdev. The
        snapshot's hard-delete must NOT proceed — SPDK would return
        EBUSY because the clone keeps the snapshot bdev open. Soft
        delete instead; the clone's delete-completion path will
        re-trigger this once SPDK has released it."""
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        snap = _snapshot("snap-1", src)
        # Mid-flight clone delete: status set to IN_DELETION but SPDK
        # leader op hasn't returned yet (deletion_status still empty).
        clone = _lvol("cl-mid", status=LVol.STATUS_IN_DELETION,
                      deletion_status="")
        clone.cloned_from_snap = "snap-1"
        node = _node()

        self._setup_db(mock_db, snap, clone, node)
        mock_get_active_mig.return_value = None

        ok = snapshot_controller.delete("snap-1", lock=False)

        self.assertTrue(ok)
        self.assertEqual(
            snap.status, SnapShot.STATUS_IN_DELETION,
            "snapshot must be defer deleted while an IN_DELETION clone's ")
        self.assertEqual(
            snap.deletion_status, "",
            "snapshot must be defer deleted while an IN_DELETION clone's, "
            "and deletion_status is unset")

    @patch("simplyblock_core.controllers.migration_controller.get_active_migration_for_lvol")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_proceeds_when_clone_spdk_delete_completed(
            self, mock_db, mock_get_active_mig):
        """**Fix 3 (other half)**: an IN_DELETION clone with
        ``deletion_status`` already set means the leader's
        ``delete_lvol_from_node`` returned successfully — SPDK has
        already removed the clone bdev, only the DB record is awaiting
        cleanup. The snapshot's hard-delete is safe to proceed.

        We assert here that ``snap.deleted`` is NOT flipped to True
        (which would indicate the soft-delete branch fired); the
        function then proceeds further into the hard-delete path
        which we short-circuit by setting the source lvol's ha_type
        to "single" with an OFFLINE host node — that returns False
        cleanly without performing RPCs and without touching
        ``snap.deleted``.
        """
        from simplyblock_core.controllers import snapshot_controller

        src = _lvol("lv-src")
        src.ha_type = "single"  # take the simpler branch in delete()
        snap = _snapshot("snap-1", src)
        clone = _lvol("cl-done", status=LVol.STATUS_IN_DELETION,
                      deletion_status="node-1")  # SPDK delete returned
        clone.cloned_from_snap = "snap-1"
        node = _node()
        node.status = StorageNode.STATUS_OFFLINE  # short-circuit single path

        self._setup_db(mock_db, snap, clone, node)
        mock_get_active_mig.return_value = None

        snapshot_controller.delete("snap-1", lock=False)

        self.assertFalse(
            snap.deleted,
            "snapshot must NOT be soft-deleted when the only IN_DELETION "
            "clone has already had its SPDK delete completed "
            "(deletion_status set)")


if __name__ == "__main__":
    unittest.main()
