"""Hard object limits: volume size, snapshots per volume (100),
clones per snapshot (500).

The limits are pure functions in ``controllers/object_limits.py`` fed by the
mini records the create paths already hold, plus wiring into the four
admission points: ``validate_add_lvol_func`` / ``add_lvol_ha`` (create),
``resize_lvol`` (resize), ``snapshot_controller.add`` (snapshot) and
``snapshot_controller.clone`` (clone, incl. ``--resize``).

Counting semantics follow the existing per-lvstore object cap: deleted objects
never count, objects still in deletion do (they are still in the blob chain).
Internal snapshots (replication / migration) are exempt from the snapshot cap.
"""
import inspect
import re
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, utils
from simplyblock_core.controllers import lvol_controller, object_limits, pool_controller
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol, LVolMini
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini

TIB = 1024 ** 4
#: Derive every size assertion from the constant. The product limit is a
#: business decision that does change (50 -> 70 TiB on 2026-09-12); pinning
#: the number in a dozen places means the next change reds the suite instead
#: of exercising it. TestConstants below is the ONE deliberate pin.
MAX_SIZE = constants.MAX_LVOL_SIZE
LV = "11111111-1111-4111-8111-111111111111"
SN = "22222222-2222-4222-8222-222222222222"


def _mini_snap(lvol_id, deleted=False, status=SnapShot.STATUS_ONLINE, snap_type=SnapShot.TYPE_USER):
    lv = LVolMini()
    lv.uuid = lvol_id
    lv.lvol_uuid = lvol_id
    s = SnapShotMini()
    s.uuid = f"snap-{id(s)}"
    s.lvol = lv
    s.status = status
    s.deleted = deleted
    s.snap_type = snap_type
    return s


def _full_snap(lvol_id, deleted=False):
    lv = LVol()
    lv.uuid = lvol_id
    s = SnapShot()
    s.uuid = f"snap-{id(s)}"
    s.lvol = lv
    s.deleted = deleted
    return s


def _mini_clone(snap_id, status=LVol.STATUS_ONLINE):
    lv = LVolMini()
    lv.uuid = f"lv-{id(lv)}"
    lv.cloned_from_snap = snap_id
    lv.status = status
    return lv


# ------------------------------------------------------------------ constants

class TestConstants(unittest.TestCase):
    def test_values(self):
        # The one deliberate pin: bump this and the comment in constants.py
        # together when the product limit changes. Everything else derives.
        self.assertEqual(constants.MAX_LVOL_SIZE, 70 * TIB)
        self.assertEqual(constants.MAX_SNAPSHOTS_PER_LVOL, 100)
        self.assertEqual(constants.MAX_CLONES_PER_SNAPSHOT, 500)

    def test_legacy_alias_now_points_at_the_enforced_limit(self):
        self.assertEqual(constants.MAX_SNAP_COUNT, constants.MAX_SNAPSHOTS_PER_LVOL)


# ----------------------------------------------------------------------- size

class TestLvolSize(unittest.TestCase):
    def test_at_limit_allowed(self):
        self.assertIsNone(object_limits.check_lvol_size(MAX_SIZE))

    def test_one_byte_over_rejected(self):
        err = object_limits.check_lvol_size(MAX_SIZE + 1)
        self.assertIsNotNone(err)
        self.assertIn("exceeds the maximum", err)
        # humanbytes renders the limit; TB/TiB spelling varies by helper version
        self.assertIn(utils.humanbytes(MAX_SIZE).replace(" TB", " TiB"),
                      err.replace(" TB", " TiB"))

    def test_zero_and_small_allowed(self):
        self.assertIsNone(object_limits.check_lvol_size(0))
        self.assertIsNone(object_limits.check_lvol_size(10 * 1024 ** 3))

    def test_what_names_the_value(self):
        err = object_limits.check_lvol_size(MAX_SIZE + 1, what="New size")
        self.assertTrue(err.startswith("New size "))


# ------------------------------------------------------------------ snapshots

class TestSnapshotCount(unittest.TestCase):
    def test_counts_only_this_lvols_active_snapshots(self):
        snaps = [_mini_snap(LV) for _ in range(3)] + [_mini_snap("other")] + [_mini_snap(LV, deleted=True)]
        self.assertEqual(object_limits.count_active_snapshots(LV, snaps), 3)

    def test_in_deletion_still_counts(self):
        snaps = [_mini_snap(LV, status=SnapShot.STATUS_IN_DELETION)]
        self.assertEqual(object_limits.count_active_snapshots(LV, snaps), 1)

    def test_full_records_work_too(self):
        snaps = [_full_snap(LV), _full_snap(LV, deleted=True)]
        self.assertEqual(object_limits.count_active_snapshots(LV, snaps), 1)

    def test_snapshot_without_lvol_is_ignored(self):
        s = SnapShotMini()
        s.lvol = None
        self.assertEqual(object_limits.count_active_snapshots(LV, [s]), 0)

    def test_limit_boundary(self):
        at_99 = [_mini_snap(LV) for _ in range(99)]
        self.assertIsNone(object_limits.check_snapshot_limit(LV, at_99))
        at_100 = at_99 + [_mini_snap(LV)]
        err = object_limits.check_snapshot_limit(LV, at_100)
        self.assertIn("Snapshot limit reached", err)
        self.assertIn("100 active", err)
        self.assertIn(str(constants.MAX_SNAPSHOTS_PER_LVOL), err)

    def test_deleted_snapshots_free_the_budget(self):
        snaps = [_mini_snap(LV) for _ in range(100)]
        snaps[0].deleted = True
        self.assertIsNone(object_limits.check_snapshot_limit(LV, snaps))

    def test_other_lvols_do_not_consume_the_budget(self):
        snaps = [_mini_snap("other") for _ in range(500)]
        self.assertIsNone(object_limits.check_snapshot_limit(LV, snaps))


# --------------------------------------------------------------------- clones

class TestCloneCount(unittest.TestCase):
    def test_counts_only_active_clones_of_this_snapshot(self):
        lvols = [_mini_clone(SN) for _ in range(4)]
        lvols += [_mini_clone("other-snap"), _mini_clone(SN, status=LVol.STATUS_DELETED)]
        plain = LVolMini()  # a plain volume, not a clone
        plain.status = LVol.STATUS_ONLINE
        lvols.append(plain)
        self.assertEqual(object_limits.count_active_clones(SN, lvols), 4)

    def test_in_deletion_still_counts(self):
        self.assertEqual(object_limits.count_active_clones(
            SN, [_mini_clone(SN, status=LVol.STATUS_IN_DELETION)]), 1)

    def test_limit_boundary(self):
        at_499 = [_mini_clone(SN) for _ in range(499)]
        self.assertIsNone(object_limits.check_clone_limit(SN, at_499))
        err = object_limits.check_clone_limit(SN, at_499 + [_mini_clone(SN)])
        self.assertIn("Clone limit reached", err)
        self.assertIn("500 active", err)


# ------------------------------------------------------------- mini carries it

class TestSnapShotMiniCarriesDeleted(unittest.TestCase):
    def test_from_snapshot_copies_deleted(self):
        full = _full_snap(LV, deleted=True)
        full.snap_type = SnapShot.TYPE_INTERNAL
        mini = SnapShotMini().from_snapshot(full)
        self.assertTrue(mini.deleted)
        self.assertEqual(mini.snap_type, SnapShot.TYPE_INTERNAL)

    def test_default_is_not_deleted(self):
        self.assertFalse(SnapShotMini().deleted)


# --------------------------------------------------------------------- wiring

def _code(fn):
    return re.sub(r'""".*?"""', "", inspect.getsource(fn), flags=re.DOTALL)


class TestWiring(unittest.TestCase):
    def test_create_paths_check_size(self):
        self.assertIn("object_limits.check_lvol_size(size)", _code(lvol_controller.validate_add_lvol_func))
        src = _code(lvol_controller.add_lvol_ha)
        self.assertIn("object_limits.check_lvol_size(size)", src)
        # max_size (thin ceiling) is deliberately NOT capped: the CLI/CSI pass a
        # large default; growth is capped in resize_lvol instead.
        self.assertNotIn('check_lvol_size(max_size', src)

    def test_resize_checks_size_before_any_state_change(self):
        src = _code(lvol_controller.resize_lvol)
        i_gate = src.index("assert_object_ops_allowed")
        i_chk = src.index('object_limits.check_lvol_size(new_size, what="New size")')
        i_first_write = min(x for x in (src.find("write_to_db"), src.find("atomic_update"), src.find("rpc_client")) if x >= 0)
        self.assertLess(i_gate, i_chk)
        self.assertLess(i_chk, i_first_write)

    def test_snapshot_add_checks_limit_only_for_user_snapshots(self):
        src = _code(snapshot_controller.add)
        i = src.index("object_limits.check_snapshot_limit(")
        window = src[max(0, i - 300):i]
        self.assertIn("snap_type == SnapShot.TYPE_USER", window)
        self.assertIn("cached_mini_snapshots(db_controller)", src[i:i + 200])

    def test_clone_checks_limit_and_resize_size(self):
        src = _code(snapshot_controller.clone)
        i = src.index("object_limits.check_clone_limit(")
        self.assertIn("cached_mini_lvols(db_controller)", src[i:i + 200])
        self.assertIn('check_lvol_size(new_size, what="Clone size")', src)


# -------------------------------------------------------- resize end to end

class TestResizeRejectsOversize(unittest.TestCase):
    """resize_lvol raises PreconditionError before touching the pool math."""

    def test_over_50tib_rejected(self):
        lvol = MagicMock(spec=LVol)
        lvol.uuid = LV
        lvol.pool_uuid = "pool-1"
        lvol.node_id = "node-1"
        lvol.size = 1 * TIB
        lvol.max_size = 100 * TIB
        lvol.status = LVol.STATUS_ONLINE
        pool = MagicMock(spec=Pool)
        pool.cluster_id = "cl-1"
        pool.lvol_max_size = 0
        pool.pool_max_size = 0
        cluster = MagicMock(spec=Cluster)
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.MUTABLE_STATUSES = [Cluster.STATUS_ACTIVE]
        snode = MagicMock()
        snode.lvstore_status = "ready"
        with patch.object(lvol_controller, "DBController") as db_cls, \
             patch.object(lvol_controller, "ops_gate", MagicMock(unsafe=True)), \
             patch("simplyblock_core.controllers.migration_controller.get_active_migration_for_lvol",
                   return_value=None), \
             patch.object(pool_controller, "get_pool_total_capacity") as pool_total:
            db = db_cls.return_value
            db.get_lvol_by_id.return_value = lvol
            db.get_pool_by_id.return_value = pool
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_node_by_id.return_value = snode
            with self.assertRaises(PreconditionError) as cm:
                lvol_controller.resize_lvol(LV, MAX_SIZE + 1)
            self.assertIn("exceeds the maximum", str(cm.exception))
            pool_total.assert_not_called()


if __name__ == "__main__":
    unittest.main()
