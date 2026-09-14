"""Hard object limits enforced end to end on the control plane.

Drives the real ``snapshot_controller.add`` / ``snapshot_controller.clone`` /
``lvol_controller.add_lvol_ha`` / ``resize_lvol`` up to (and through) the limit
checks with the DB, RPC and gates mocked out. The cached mini tables are
patched to a synthetic population, which is exactly what the create paths
consume in production (never the full tables).

Where the call must *pass* the check, the next downstream step is replaced by
a sentinel exception so the test proves the check was passed without running
the rest of the create (which needs a live lvstore).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.controllers import lvol_controller, snapshot_controller
from simplyblock_core.controllers import object_limits
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol, LVolMini
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.storage_node import StorageNode

TIB = 1024 ** 4
#: Derived, not pinned -- see tests/unit/test_object_limits.py.
MAX_SIZE = constants.MAX_LVOL_SIZE
LV = "11111111-1111-4111-8111-111111111111"
SN = "22222222-2222-4222-8222-222222222222"
NODE = "33333333-3333-4333-8333-333333333333"
POOL = "44444444-4444-4444-8444-444444444444"
CL = "55555555-5555-4555-8555-555555555555"


class _Passed(Exception):
    """Raised by the patched limit check after it returned None (= allowed)."""


def _mini_snap(lvol_id, deleted=False):
    lv = LVolMini(); lv.uuid = lvol_id; lv.lvol_uuid = lvol_id; lv.node_id = NODE; lv.pool_uuid = POOL
    s = SnapShotMini(); s.uuid = f"s-{id(s)}"; s.lvol = lv; s.status = SnapShot.STATUS_ONLINE; s.deleted = deleted
    return s


def _mini_clone(snap_id, status=LVol.STATUS_ONLINE):
    lv = LVolMini(); lv.uuid = f"c-{id(lv)}"; lv.lvol_uuid = lv.uuid
    lv.cloned_from_snap = snap_id; lv.status = status; lv.node_id = NODE; lv.pool_uuid = POOL
    return lv


def _lvol():
    lv = MagicMock(spec=LVol)
    lv.uuid = LV; lv.get_id.return_value = LV
    lv.status = LVol.STATUS_ONLINE; lv.node_id = NODE; lv.pool_uuid = POOL
    lv.size = 10 * 1024 ** 3; lv.max_size = 100 * TIB; lv.lvs_name = "LVS_1"
    return lv


def _snap():
    s = MagicMock(spec=SnapShot)
    s.uuid = SN; s.get_id.return_value = SN
    s.deleted = False; s.status = SnapShot.STATUS_ONLINE; s.cluster_id = CL
    s.size = 10 * 1024 ** 3; s.lvol = _lvol()
    return s


def _pool():
    p = MagicMock(spec=Pool)
    p.uuid = POOL; p.get_id.return_value = POOL; p.pool_name = "pool"
    p.cluster_id = CL; p.status = Pool.STATUS_ACTIVE
    p.lvol_max_size = 0; p.pool_max_size = 0
    return p


def _cluster():
    c = MagicMock(spec=Cluster)
    c.uuid = CL; c.get_id.return_value = CL
    c.status = Cluster.STATUS_ACTIVE; c.MUTABLE_STATUSES = [Cluster.STATUS_ACTIVE]
    c.fabric_tcp = True; c.fabric_rdma = False
    return c


def _snode():
    n = MagicMock(spec=StorageNode)
    n.uuid = NODE; n.get_id.return_value = NODE
    n.lvstore_status = "ready"; n.status = StorageNode.STATUS_ONLINE
    n.lvol_sync_del.return_value = False
    return n


class _SnapshotEnv:
    """Mock everything snapshot add()/clone() touch before the limit checks."""

    def __init__(self, snaps=(), lvols=()):
        self.snaps, self.lvols = list(snaps), list(lvols)
        self.patches = []

    def __enter__(self):
        db = MagicMock()
        db.get_lvol_by_id.return_value = _lvol()
        db.get_snapshot_by_id.return_value = _snap()
        db.get_pool_by_id.return_value = _pool()
        db.get_cluster_by_id.return_value = _cluster()
        db.get_storage_node_by_id.return_value = _snode()
        db.snap_name_taken.return_value = False
        db.lvol_name_lookup.return_value = None
        self.db = db
        self.patches = [
            # snapshot_controller holds ONE module-level DBController instance;
            # patch the instance, not the class.
            patch.object(snapshot_controller, "db_controller", db),
            patch.object(snapshot_controller, "ops_gate", MagicMock(unsafe=True)),
            patch.object(snapshot_controller, "_wait_for_node_sync_delete", return_value=True),
            patch("simplyblock_core.controllers.migration_controller.is_migration_active_on_node",
                  return_value=False),
            # isolate from the per-lvstore object cap
            patch.object(lvol_controller, "check_lvstore_object_limit", return_value=None),
            patch("simplyblock_core.utils.ttl_cache.cached_mini_snapshots",
                  side_effect=lambda db_, *a, **k: self.snaps),
            patch("simplyblock_core.utils.ttl_cache.cached_mini_lvols",
                  side_effect=lambda db_, *a, **k: self.lvols),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *a):
        for p in reversed(self.patches):
            p.stop()
        return False


def _passing(real):
    """Wrap a limit check: call the real one, assert allowed, then stop the create."""
    def wrapped(*a, **k):
        err = real(*a, **k)
        if err is not None:
            raise AssertionError(f"limit check unexpectedly rejected: {err}")
        raise _Passed()
    return wrapped


# ---------------------------------------------------------------- snapshots

class TestSnapshotLimitEndToEnd(unittest.TestCase):
    def test_101st_user_snapshot_is_rejected(self):
        snaps = [_mini_snap(LV) for _ in range(constants.MAX_SNAPSHOTS_PER_LVOL)]
        with _SnapshotEnv(snaps=snaps):
            ok, err = snapshot_controller.add(LV, "snap-101")
        self.assertFalse(ok)
        self.assertIn("Snapshot limit reached", err)
        self.assertIn(LV, err)

    def test_100th_snapshot_passes_the_check(self):
        snaps = [_mini_snap(LV) for _ in range(constants.MAX_SNAPSHOTS_PER_LVOL - 1)]
        with _SnapshotEnv(snaps=snaps), \
             patch.object(snapshot_controller.object_limits, "check_snapshot_limit",
                          side_effect=_passing(object_limits.check_snapshot_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.add(LV, "snap-100")

    def test_deleted_snapshots_do_not_count(self):
        snaps = [_mini_snap(LV) for _ in range(constants.MAX_SNAPSHOTS_PER_LVOL)]
        snaps[0].deleted = True
        with _SnapshotEnv(snaps=snaps), \
             patch.object(snapshot_controller.object_limits, "check_snapshot_limit",
                          side_effect=_passing(object_limits.check_snapshot_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.add(LV, "snap-x")

    def test_other_volumes_snapshots_do_not_count(self):
        snaps = [_mini_snap("some-other-lvol") for _ in range(3 * constants.MAX_SNAPSHOTS_PER_LVOL)]
        with _SnapshotEnv(snaps=snaps), \
             patch.object(snapshot_controller.object_limits, "check_snapshot_limit",
                          side_effect=_passing(object_limits.check_snapshot_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.add(LV, "snap-x")

    def test_internal_snapshot_is_exempt_at_the_cap(self):
        """Replication / migration snapshots must still be possible at the cap."""
        snaps = [_mini_snap(LV) for _ in range(constants.MAX_SNAPSHOTS_PER_LVOL + 5)]
        calls = []
        with _SnapshotEnv(snaps=snaps), \
             patch.object(snapshot_controller.object_limits, "check_snapshot_limit",
                          side_effect=lambda *a, **k: calls.append(1) or None), \
             patch.object(snapshot_controller, "check_snapshot_capacity", side_effect=_Passed):
            # check_snapshot_capacity is the first call after the cap block
            with self.assertRaises(_Passed):
                snapshot_controller.add(LV, "repl_x", snap_type=SnapShot.TYPE_INTERNAL)
        self.assertEqual(calls, [], "internal snapshot must not be limit-checked")


# ------------------------------------------------------------------- clones

class TestCloneLimitEndToEnd(unittest.TestCase):
    def test_501st_clone_is_rejected(self):
        lvols = [_mini_clone(SN) for _ in range(constants.MAX_CLONES_PER_SNAPSHOT)]
        with _SnapshotEnv(lvols=lvols):
            ok, err = snapshot_controller.clone(SN, "clone-501")
        self.assertFalse(ok)
        self.assertIn("Clone limit reached", err)
        self.assertIn(SN, err)

    def test_500th_clone_passes_the_check(self):
        lvols = [_mini_clone(SN) for _ in range(constants.MAX_CLONES_PER_SNAPSHOT - 1)]
        with _SnapshotEnv(lvols=lvols), \
             patch.object(snapshot_controller.object_limits, "check_clone_limit",
                          side_effect=_passing(object_limits.check_clone_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.clone(SN, "clone-500")

    def test_deleted_clones_do_not_count(self):
        lvols = [_mini_clone(SN) for _ in range(constants.MAX_CLONES_PER_SNAPSHOT)]
        lvols[0].status = LVol.STATUS_DELETED
        with _SnapshotEnv(lvols=lvols), \
             patch.object(snapshot_controller.object_limits, "check_clone_limit",
                          side_effect=_passing(object_limits.check_clone_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.clone(SN, "clone-x")

    def test_clones_of_other_snapshots_do_not_count(self):
        lvols = [_mini_clone("other-snap") for _ in range(2 * constants.MAX_CLONES_PER_SNAPSHOT)]
        with _SnapshotEnv(lvols=lvols), \
             patch.object(snapshot_controller.object_limits, "check_clone_limit",
                          side_effect=_passing(object_limits.check_clone_limit)):
            with self.assertRaises(_Passed):
                snapshot_controller.clone(SN, "clone-x")

    def test_clone_with_resize_over_the_size_cap_is_rejected(self):
        with _SnapshotEnv():
            ok, err = snapshot_controller.clone(SN, "big-clone", new_size=MAX_SIZE + 1)
        self.assertFalse(ok)
        self.assertIn("Clone size", err)
        self.assertIn("exceeds the maximum", err)


# ------------------------------------------------------------ volume size

class TestVolumeSizeEndToEnd(unittest.TestCase):
    def test_validate_add_lvol_rejects_over_the_size_cap(self):
        ok, err = lvol_controller.validate_add_lvol_func("v", MAX_SIZE + 1, NODE, POOL, 0, 0, 0, 0)
        self.assertFalse(ok)
        self.assertIn("exceeds the maximum", err)

    def test_validate_add_lvol_allows_exactly_the_size_cap(self):
        """The size cap itself must not reject the cap value (other checks may)."""
        self.assertIsNone(object_limits.check_lvol_size(MAX_SIZE))

    def _add_lvol_ha(self, size, max_size=0):
        pool = _pool()
        db = MagicMock()
        db.get_pools.return_value = [pool]
        db.get_cluster_by_id.return_value = _cluster()
        db.get_storage_node_by_id.return_value = _snode()
        with patch.object(lvol_controller, "DBController", return_value=db), \
             patch.object(lvol_controller, "ops_gate", MagicMock(unsafe=True)):
            return lvol_controller.add_lvol_ha("vol", size, None, "ha", POOL, max_size=max_size)

    def test_add_lvol_ha_rejects_over_the_size_cap(self):
        ok, err = self._add_lvol_ha(MAX_SIZE + 1)
        self.assertFalse(ok)
        self.assertIn("Volume size", err)
        self.assertIn("exceeds the maximum", err)

    def test_add_lvol_ha_does_not_cap_the_thin_max_size_default(self):
        """The CLI/CSI pass a large default max_size (1000T) when the user gives
        none; capping it rejected every `sbctl volume add`. Growth is capped in
        resize_lvol instead, so a huge ceiling must pass the size check."""
        with patch.object(lvol_controller.object_limits, "check_lvol_size",
                          side_effect=_passing(object_limits.check_lvol_size)):
            with self.assertRaises(_Passed):
                self._add_lvol_ha(1 * TIB, max_size=1000 * 1000 ** 4)

    def test_add_lvol_ha_passes_size_check_at_the_cap(self):
        with patch.object(lvol_controller.object_limits, "check_lvol_size",
                          side_effect=_passing(object_limits.check_lvol_size)):
            with self.assertRaises(_Passed):
                self._add_lvol_ha(MAX_SIZE)

    def test_resize_over_the_size_cap_raises_precondition(self):
        db = MagicMock()
        db.get_lvol_by_id.return_value = _lvol()
        db.get_pool_by_id.return_value = _pool()
        db.get_cluster_by_id.return_value = _cluster()
        db.get_storage_node_by_id.return_value = _snode()
        with patch.object(lvol_controller, "DBController", return_value=db), \
             patch.object(lvol_controller, "ops_gate", MagicMock(unsafe=True)), \
             patch("simplyblock_core.controllers.migration_controller.get_active_migration_for_lvol",
                   return_value=None):
            with self.assertRaises(PreconditionError) as cm:
                lvol_controller.resize_lvol(LV, MAX_SIZE + 1)
        self.assertIn("New size", str(cm.exception))
        self.assertIn("exceeds the maximum", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
