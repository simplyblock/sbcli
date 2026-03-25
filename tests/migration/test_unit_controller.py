# coding=utf-8
"""
test_unit_controller.py – unit tests for migration_controller pure-logic functions.

Every test patches the module-level ``db`` singleton so no FDB connection is needed.
"""

import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

# Module under test (import after patching, but top-level import is fine since
# we patch the db attribute before each individual call).
import simplyblock_core.controllers.migration_controller as ctl


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _migration(lvol_id="lvol-1", source_node="node-src", target_node="node-tgt",
               status=LVolMigration.STATUS_RUNNING, cluster_id="cluster-1",
               snap_plan=None, snaps_migrated=None, snaps_preexisting=None):
    m = LVolMigration()
    m.uuid = "mig-uuid"
    m.cluster_id = cluster_id
    m.lvol_id = lvol_id
    m.source_node_id = source_node
    m.target_node_id = target_node
    m.status = status
    m.snap_migration_plan = snap_plan or []
    m.snaps_migrated = snaps_migrated or []
    m.snaps_preexisting_on_target = snaps_preexisting or []
    m.intermediate_snaps = []
    m.canceled = False
    return m


def _snap(uuid, lvol_uuid, node_id, ref_id="", created_at=None, status=SnapShot.STATUS_ONLINE):
    s = SnapShot()
    s.uuid = uuid
    s.snap_uuid = uuid
    s.snap_ref_id = ref_id
    s.status = status
    s.created_at = created_at or int(time.time())
    lvol = LVol()
    lvol.uuid = lvol_uuid
    lvol.node_id = node_id
    s.lvol = lvol
    s.snap_bdev = f"lvs/{uuid}"
    return s


def _lvol(uuid, node_id, status=LVol.STATUS_ONLINE, cloned_from_snap=""):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.status = status
    lv.cloned_from_snap = cloned_from_snap
    return lv


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          hostname="", lvstore="", secondary_node_id=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = hostname or f"host-{uuid}"
    n.lvstore = lvstore or f"lvs_{uuid}"
    n.secondary_node_id = secondary_node_id
    return n


# ---------------------------------------------------------------------------
# _get_snap_ancestry
# ---------------------------------------------------------------------------

class TestGetSnapAncestry(unittest.TestCase):

    def _snap_db(self, snaps_by_id):
        """Return a mock db whose get_snapshot_by_id looks up from a dict."""
        mock_db = MagicMock()
        def _get(snap_id):
            if snap_id not in snaps_by_id:
                raise KeyError(snap_id)
            return snaps_by_id[snap_id]
        mock_db.get_snapshot_by_id.side_effect = _get
        return mock_db

    def test_single_root_snap(self):
        s1 = _snap("s1", "lvol", "node", ref_id="")
        with patch.object(ctl, 'db', self._snap_db({"s1": s1})):
            result = ctl._get_snap_ancestry("s1")
        assert result == ["s1"]

    def test_chain_returned_root_first(self):
        # s1 → s2 → s3 (s3 is leaf, ref_id points to parent)
        s1 = _snap("s1", "lvol", "node", ref_id="")
        s2 = _snap("s2", "lvol", "node", ref_id="s1")
        s3 = _snap("s3", "lvol", "node", ref_id="s2")
        db = self._snap_db({"s1": s1, "s2": s2, "s3": s3})
        with patch.object(ctl, 'db', db):
            result = ctl._get_snap_ancestry("s3")
        assert result == ["s1", "s2", "s3"]

    def test_missing_snap_breaks_chain(self):
        s2 = _snap("s2", "lvol", "node", ref_id="s1-missing")
        db = self._snap_db({"s2": s2})
        with patch.object(ctl, 'db', db):
            result = ctl._get_snap_ancestry("s2")
        # s1-missing is not in db, so chain stops at s2
        assert result == ["s2"]

    def test_cycle_protection(self):
        # If snap_ref_id somehow points to itself, must not loop forever
        s1 = _snap("s1", "lvol", "node", ref_id="s1")
        db = self._snap_db({"s1": s1})
        with patch.object(ctl, 'db', db):
            result = ctl._get_snap_ancestry("s1")
        assert result == ["s1"]


# ---------------------------------------------------------------------------
# get_snapshot_chain
# ---------------------------------------------------------------------------

class TestGetSnapshotChain(unittest.TestCase):

    def _make_db(self, lvol, node_snaps):
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_snapshots_by_node_id.return_value = node_snaps
        # Default: no snapshots found by id (ancestry walk won't be called)
        mock_db.get_snapshot_by_id.side_effect = KeyError
        return mock_db

    def test_direct_snaps_oldest_first(self):
        lvol = _lvol("lvol-1", "node-src")
        now = int(time.time())
        s1 = _snap("s1", "lvol-1", "node-src", created_at=now - 100)
        s2 = _snap("s2", "lvol-1", "node-src", created_at=now - 50)
        s3 = _snap("s3", "lvol-1", "node-src", created_at=now)
        with patch.object(ctl, 'db', self._make_db(lvol, [s3, s1, s2])):
            result = ctl.get_snapshot_chain("lvol-1", "node-src")
        assert result == ["s1", "s2", "s3"]

    def test_in_deletion_snap_excluded(self):
        lvol = _lvol("lvol-1", "node-src")
        now = int(time.time())
        s1 = _snap("s1", "lvol-1", "node-src", created_at=now - 100)
        s_del = _snap("s_del", "lvol-1", "node-src", created_at=now,
                      status=SnapShot.STATUS_IN_DELETION)
        with patch.object(ctl, 'db', self._make_db(lvol, [s1, s_del])):
            result = ctl.get_snapshot_chain("lvol-1", "node-src")
        assert "s_del" not in result
        assert "s1" in result

    def test_snaps_for_other_volumes_excluded(self):
        lvol = _lvol("lvol-1", "node-src")
        now = int(time.time())
        s1 = _snap("s1", "lvol-1", "node-src", created_at=now)
        s_other = _snap("s_other", "lvol-other", "node-src", created_at=now)
        with patch.object(ctl, 'db', self._make_db(lvol, [s1, s_other])):
            result = ctl.get_snapshot_chain("lvol-1", "node-src")
        assert "s_other" not in result
        assert "s1" in result

    def test_clone_ancestry_prepended(self):
        # lvol cloned from s2; s2's ancestry is s1 → s2
        lvol = _lvol("lvol-clone", "node-src", cloned_from_snap="s2")
        now = int(time.time())
        s3 = _snap("s3", "lvol-clone", "node-src", created_at=now)

        s1 = _snap("s1", "lvol-orig", "node-src", ref_id="")
        s2 = _snap("s2", "lvol-orig", "node-src", ref_id="s1")

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_snapshots_by_node_id.return_value = [s3]

        def _get_snap(sid):
            return {"s1": s1, "s2": s2}[sid]
        mock_db.get_snapshot_by_id.side_effect = _get_snap

        with patch.object(ctl, 'db', mock_db):
            result = ctl.get_snapshot_chain("lvol-clone", "node-src")

        # ancestry first (root → leaf), then direct snap
        assert result == ["s1", "s2", "s3"]

    def test_no_duplicates_between_ancestry_and_direct(self):
        # s2 is in the clone ancestry AND appears as a direct snap
        lvol = _lvol("lvol-clone", "node-src", cloned_from_snap="s2")
        now = int(time.time())
        s1 = _snap("s1", "lvol-orig", "node-src", ref_id="")
        s2 = _snap("s2", "lvol-orig", "node-src", ref_id="s1")
        # s2 also appears in direct snaps (edge case)
        s2_direct = _snap("s2", "lvol-clone", "node-src", created_at=now)

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_snapshots_by_node_id.return_value = [s2_direct]

        def _get_snap(sid):
            return {"s1": s1, "s2": s2}[sid]
        mock_db.get_snapshot_by_id.side_effect = _get_snap

        with patch.object(ctl, 'db', mock_db):
            result = ctl.get_snapshot_chain("lvol-clone", "node-src")

        assert result.count("s2") == 1

    def test_empty_chain_when_no_snaps(self):
        lvol = _lvol("lvol-1", "node-src")
        with patch.object(ctl, 'db', self._make_db(lvol, [])):
            result = ctl.get_snapshot_chain("lvol-1", "node-src")
        assert result == []


# ---------------------------------------------------------------------------
# get_active_migration_for_lvol / get_active_migration_on_node /
# is_migration_active_on_node
# ---------------------------------------------------------------------------

class TestActiveChecks(unittest.TestCase):

    def _db_with_migrations(self, migrations):
        mock_db = MagicMock()
        mock_db.get_migrations.return_value = migrations
        return mock_db

    def test_get_active_migration_for_lvol_found(self):
        m = _migration(lvol_id="lvol-1", status=LVolMigration.STATUS_RUNNING)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            result = ctl.get_active_migration_for_lvol("lvol-1")
        assert result is m

    def test_get_active_migration_for_lvol_terminal_ignored(self):
        m = _migration(lvol_id="lvol-1", status=LVolMigration.STATUS_DONE)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            result = ctl.get_active_migration_for_lvol("lvol-1")
        assert result is None

    def test_get_active_migration_for_lvol_different_lvol_ignored(self):
        m = _migration(lvol_id="lvol-other", status=LVolMigration.STATUS_RUNNING)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            result = ctl.get_active_migration_for_lvol("lvol-1")
        assert result is None

    def test_get_active_migration_on_node_found(self):
        m = _migration(source_node="node-src", status=LVolMigration.STATUS_RUNNING)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            result = ctl.get_active_migration_on_node("cluster-1", "node-src")
        assert result is m

    def test_get_active_migration_on_node_done_ignored(self):
        m = _migration(source_node="node-src", status=LVolMigration.STATUS_DONE)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            result = ctl.get_active_migration_on_node("cluster-1", "node-src")
        assert result is None

    def test_is_migration_active_on_node_true(self):
        m = _migration(source_node="node-src", status=LVolMigration.STATUS_SUSPENDED)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            assert ctl.is_migration_active_on_node("node-src")

    def test_is_migration_active_on_node_false(self):
        m = _migration(source_node="node-src", status=LVolMigration.STATUS_FAILED)
        with patch.object(ctl, 'db', self._db_with_migrations([m])):
            assert not ctl.is_migration_active_on_node("node-src")


# ---------------------------------------------------------------------------
# _protect_snap_and_ancestors / _collect_snap_ancestry
# ---------------------------------------------------------------------------

class TestSnapAncestryHelpers(unittest.TestCase):

    def _snap_db(self, snaps_by_id):
        mock_db = MagicMock()
        def _get(sid):
            if sid not in snaps_by_id:
                raise KeyError(sid)
            return snaps_by_id[sid]
        mock_db.get_snapshot_by_id.side_effect = _get
        return mock_db

    def test_protect_removes_snap_and_parents(self):
        s1 = _snap("s1", "lvol", "node", ref_id="")
        s2 = _snap("s2", "lvol", "node", ref_id="s1")
        candidate_set = {"s1", "s2", "s3"}
        with patch.object(ctl, 'db', self._snap_db({"s1": s1, "s2": s2})):
            ctl._protect_snap_and_ancestors("s2", candidate_set)
        assert "s1" not in candidate_set
        assert "s2" not in candidate_set
        assert "s3" in candidate_set

    def test_protect_handles_missing_snap(self):
        candidate_set = {"s1"}
        with patch.object(ctl, 'db', self._snap_db({})):
            # Should not raise even if snap not found
            ctl._protect_snap_and_ancestors("s1", candidate_set)
        # s1 gets discarded before the KeyError break
        assert "s1" not in candidate_set

    def test_collect_adds_snap_and_parents(self):
        s1 = _snap("s1", "lvol", "node", ref_id="")
        s2 = _snap("s2", "lvol", "node", ref_id="s1")
        out_set = set()
        with patch.object(ctl, 'db', self._snap_db({"s1": s1, "s2": s2})):
            ctl._collect_snap_ancestry("s2", out_set)
        assert out_set == {"s1", "s2"}

    def test_collect_handles_cycle(self):
        s1 = _snap("s1", "lvol", "node", ref_id="s1")
        out_set = set()
        with patch.object(ctl, 'db', self._snap_db({"s1": s1})):
            ctl._collect_snap_ancestry("s1", out_set)
        assert out_set == {"s1"}


# ---------------------------------------------------------------------------
# get_snaps_safe_to_delete_on_source
# ---------------------------------------------------------------------------

class TestGetSnapsSafeToDeleteOnSource(unittest.TestCase):

    def _make_db(self, snaps_by_id, source_lvols):
        mock_db = MagicMock()
        def _get_snap(sid):
            if sid not in snaps_by_id:
                raise KeyError(sid)
            return snaps_by_id[sid]
        mock_db.get_snapshot_by_id.side_effect = _get_snap
        mock_db.get_lvols_by_node_id.return_value = source_lvols
        return mock_db

    def test_owned_snaps_are_candidates(self):
        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         snap_plan=["s1", "s2"])
        s1 = _snap("s1", "lvol-1", "node-src")
        s2 = _snap("s2", "lvol-1", "node-src")
        with patch.object(ctl, 'db', self._make_db({"s1": s1, "s2": s2}, [])):
            result = ctl.get_snaps_safe_to_delete_on_source(mig)
        assert result == {"s1", "s2"}

    def test_snaps_of_other_volumes_excluded(self):
        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         snap_plan=["s1", "s_other"])
        s1 = _snap("s1", "lvol-1", "node-src")
        s_other = _snap("s_other", "lvol-other", "node-src")
        with patch.object(ctl, 'db', self._make_db(
                {"s1": s1, "s_other": s_other}, [])):
            result = ctl.get_snaps_safe_to_delete_on_source(mig)
        assert "s_other" not in result
        assert "s1" in result

    def test_snap_referenced_by_clone_is_protected(self):
        # lvol-clone on source references s1 via cloned_from_snap
        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         snap_plan=["s1", "s2"])
        s1 = _snap("s1", "lvol-1", "node-src", ref_id="")
        s2 = _snap("s2", "lvol-1", "node-src", ref_id="s1")
        clone = _lvol("lvol-clone", "node-src", cloned_from_snap="s1")
        with patch.object(ctl, 'db', self._make_db({"s1": s1, "s2": s2}, [clone])):
            result = ctl.get_snaps_safe_to_delete_on_source(mig)
        # s1 is referenced by clone → protected; s2 is safe
        assert "s1" not in result
        assert "s2" in result

    def test_intermediate_snaps_always_included(self):
        mig = _migration(lvol_id="lvol-1", source_node="node-src", snap_plan=[])
        mig.intermediate_snaps = ["si1", "si2"]
        s1 = _snap("si1", "lvol-1", "node-src")
        s2 = _snap("si2", "lvol-1", "node-src")
        with patch.object(ctl, 'db', self._make_db({"si1": s1, "si2": s2}, [])):
            result = ctl.get_snaps_safe_to_delete_on_source(mig)
        assert "si1" in result
        assert "si2" in result

    def test_missing_snap_in_plan_skipped(self):
        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         snap_plan=["s_gone"])
        with patch.object(ctl, 'db', self._make_db({}, [])):
            result = ctl.get_snaps_safe_to_delete_on_source(mig)
        assert "s_gone" not in result


# ---------------------------------------------------------------------------
# get_snaps_to_delete_on_target
# ---------------------------------------------------------------------------

class TestGetSnapsToDeleteOnTarget(unittest.TestCase):

    def _make_db(self, snaps_by_id, target_lvols):
        mock_db = MagicMock()
        def _get_snap(sid):
            if sid not in snaps_by_id:
                raise KeyError(sid)
            return snaps_by_id[sid]
        mock_db.get_snapshot_by_id.side_effect = _get_snap
        mock_db.get_lvols_by_node_id.return_value = target_lvols
        return mock_db

    def test_migrated_snaps_returned_for_deletion(self):
        mig = _migration(lvol_id="lvol-1", target_node="node-tgt",
                         snaps_migrated=["s1", "s2"])
        with patch.object(ctl, 'db', self._make_db({}, [])):
            result = ctl.get_snaps_to_delete_on_target(mig)
        assert set(result) == {"s1", "s2"}

    def test_preexisting_snaps_are_protected(self):
        mig = _migration(lvol_id="lvol-1", target_node="node-tgt",
                         snaps_migrated=["s1", "s2"],
                         snaps_preexisting=["s1"])
        with patch.object(ctl, 'db', self._make_db({}, [])):
            result = ctl.get_snaps_to_delete_on_target(mig)
        assert "s1" not in result
        assert "s2" in result

    def test_snap_referenced_by_target_lvol_protected(self):
        # lvol already on target references s1 via cloned_from_snap
        mig = _migration(lvol_id="lvol-1", target_node="node-tgt",
                         snaps_migrated=["s1", "s2"])
        s1 = _snap("s1", "lvol-other", "node-tgt", ref_id="")
        other_lvol = _lvol("lvol-other", "node-tgt", cloned_from_snap="s1")
        with patch.object(ctl, 'db', self._make_db({"s1": s1}, [other_lvol])):
            result = ctl.get_snaps_to_delete_on_target(mig)
        assert "s1" not in result
        assert "s2" in result

    def test_migrating_lvol_itself_not_counted_as_reference(self):
        # The migrating lvol's own cloned_from_snap must not protect its snaps
        mig = _migration(lvol_id="lvol-1", target_node="node-tgt",
                         snaps_migrated=["s1"])
        migrating = _lvol("lvol-1", "node-tgt", cloned_from_snap="s1")
        s1 = _snap("s1", "lvol-1", "node-tgt", ref_id="")
        with patch.object(ctl, 'db', self._make_db({"s1": s1}, [migrating])):
            result = ctl.get_snaps_to_delete_on_target(mig)
        # lvol-1 is the migrating volume, excluded from protection check
        assert "s1" in result

    def test_empty_migrated_list_returns_empty(self):
        mig = _migration(snaps_migrated=[])
        with patch.object(ctl, 'db', self._make_db({}, [])):
            result = ctl.get_snaps_to_delete_on_target(mig)
        assert result == []


# ---------------------------------------------------------------------------
# apply_migration_to_db
# ---------------------------------------------------------------------------

class TestApplyMigrationToDb(unittest.TestCase):

    def _tgt_node(self):
        return _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt")

    def _mock_db(self, lvol, snaps=None, tgt_node=None):
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.return_value = tgt_node or self._tgt_node()
        if snaps is not None:
            if isinstance(snaps, dict):
                mock_db.get_snapshot_by_id.side_effect = lambda sid: snaps[sid]
            elif isinstance(snaps, Exception):
                mock_db.get_snapshot_by_id.side_effect = snaps
            else:
                mock_db.get_snapshot_by_id.return_value = snaps
        return mock_db

    def test_lvol_node_id_updated(self):
        lvol = _lvol("lvol-1", "node-src")
        snap = _snap("s1", "lvol-1", "node-src")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=["s1"])

        mock_db = self._mock_db(lvol, snap)
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True
        assert lvol.node_id == "node-tgt"
        assert lvol.hostname == "host-tgt"
        assert lvol.lvs_name == "lvs_tgt"
        assert lvol.nodes == ["node-tgt"]

    def test_lvol_nodes_includes_secondary(self):
        lvol = _lvol("lvol-1", "node-src")
        tgt = _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt",
                     secondary_node_id="node-tgt-sec")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=[])

        mock_db = self._mock_db(lvol, tgt_node=tgt)
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True
        assert lvol.nodes == ["node-tgt", "node-tgt-sec"]

    def test_snapshot_node_id_updated(self):
        lvol = _lvol("lvol-1", "node-src")
        snap = _snap("s1", "lvol-1", "node-src")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=["s1"])

        mock_db = self._mock_db(lvol, snap)
        with patch.object(ctl, 'db', mock_db):
            ctl.apply_migration_to_db(mig)

        assert snap.lvol.node_id == "node-tgt"
        assert snap.snap_bdev == "lvs_tgt/s1"

    def test_missing_lvol_returns_false(self):
        mig = _migration(lvol_id="lvol-gone")

        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("lvol-gone")

        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is False

    def test_missing_snapshot_does_not_raise(self):
        lvol = _lvol("lvol-1", "node-src")
        mig = _migration(lvol_id="lvol-1", snaps_migrated=["s_gone"])

        mock_db = self._mock_db(lvol, KeyError("s_gone"))
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True  # still succeeds; missing snaps are warned


# ---------------------------------------------------------------------------
# cancel_migration
# ---------------------------------------------------------------------------

class TestCancelMigration(unittest.TestCase):

    def test_cancel_sets_canceled_flag(self):
        mig = _migration(status=LVolMigration.STATUS_RUNNING)

        mock_db = MagicMock()
        mock_db.get_migration_by_id.return_value = mig

        with patch.object(ctl, 'db', mock_db), \
             patch('simplyblock_core.controllers.migration_controller.migration_events'):
            ok, err = ctl.cancel_migration("mig-uuid")

        assert ok is True
        assert err is None
        assert mig.canceled is True

    def test_cancel_inactive_migration_fails(self):
        mig = _migration(status=LVolMigration.STATUS_DONE)

        mock_db = MagicMock()
        mock_db.get_migration_by_id.return_value = mig

        with patch.object(ctl, 'db', mock_db):
            ok, err = ctl.cancel_migration("mig-uuid")

        assert ok is False
        assert "not active" in err

    def test_cancel_nonexistent_migration_fails(self):
        mock_db = MagicMock()
        mock_db.get_migration_by_id.side_effect = KeyError("mig-missing")

        with patch.object(ctl, 'db', mock_db):
            ok, err = ctl.cancel_migration("mig-missing")

        assert ok is False


# ---------------------------------------------------------------------------
# start_migration – precondition validation
# ---------------------------------------------------------------------------

class TestStartMigrationPreconditions(unittest.TestCase):
    """
    Test only the precondition guard-clauses in start_migration.
    Each case patches db to exercise one rejection path.
    """

    def _base_db(self, lvol, src_node, tgt_node, migrations=None):
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_storage_node_by_id.side_effect = lambda nid: (
            src_node if nid == src_node.uuid else tgt_node
        )
        mock_db.get_migrations.return_value = migrations or []
        mock_db.get_snapshots_by_node_id.return_value = []
        return mock_db

    def test_reject_lvol_not_found(self):
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.side_effect = KeyError("not found")
        with patch.object(ctl, 'db', mock_db):
            ok, err = ctl.start_migration("bad-lvol", "node-tgt")
        assert ok is False

    def test_reject_lvol_not_online(self):
        lvol = _lvol("lvol-1", "node-src", status=LVol.STATUS_OFFLINE)
        src = _node("node-src")
        tgt = _node("node-tgt")
        with patch.object(ctl, 'db', self._base_db(lvol, src, tgt)):
            ok, err = ctl.start_migration("lvol-1", "node-tgt")
        assert ok is False
        assert "not online" in err

    def test_reject_same_source_and_target(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src")
        with patch.object(ctl, 'db', self._base_db(lvol, src, src)):
            ok, err = ctl.start_migration("lvol-1", "node-src")
        assert ok is False
        assert "different" in err

    def test_reject_source_node_offline(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src", status=StorageNode.STATUS_OFFLINE)
        tgt = _node("node-tgt")
        with patch.object(ctl, 'db', self._base_db(lvol, src, tgt)):
            ok, err = ctl.start_migration("lvol-1", "node-tgt")
        assert ok is False
        assert "Source node is not online" in err

    def test_reject_target_node_offline(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src")
        tgt = _node("node-tgt", status=StorageNode.STATUS_OFFLINE)
        with patch.object(ctl, 'db', self._base_db(lvol, src, tgt)):
            ok, err = ctl.start_migration("lvol-1", "node-tgt")
        assert ok is False
        assert "Target node is not online" in err

    def test_reject_active_migration_on_source(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src")
        tgt = _node("node-tgt")
        existing = _migration(lvol_id="lvol-other", source_node="node-src",
                              status=LVolMigration.STATUS_RUNNING)
        with patch.object(ctl, 'db', self._base_db(lvol, src, tgt, [existing])):
            ok, err = ctl.start_migration("lvol-1", "node-tgt")
        assert ok is False
        assert "active on source node" in err

    def test_reject_volume_already_migrating(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src")
        tgt = _node("node-tgt")
        # A migration for lvol-1 on a different source node
        existing_vol_mig = _migration(lvol_id="lvol-1", source_node="node-other",
                                      status=LVolMigration.STATUS_RUNNING)
        # node-src has no active migration → passes node check
        existing_vol_mig.source_node_id = "node-other"

        mock_db = self._base_db(lvol, src, tgt, [existing_vol_mig])
        with patch.object(ctl, 'db', mock_db):
            ok, err = ctl.start_migration("lvol-1", "node-tgt")
        assert ok is False
        assert "active migration" in err

    def test_success_creates_migration_and_task(self):
        lvol = _lvol("lvol-1", "node-src")
        src = _node("node-src")
        tgt = _node("node-tgt")

        mock_db = self._base_db(lvol, src, tgt)
        mock_db.kv_store = MagicMock()

        with patch.object(ctl, 'db', mock_db), \
             patch('simplyblock_core.controllers.migration_controller.tasks_controller') as tc, \
             patch('simplyblock_core.controllers.migration_controller.migration_events'):
            tc.add_lvol_mig_task.return_value = "task-uuid"
            ok, err = ctl.start_migration("lvol-1", "node-tgt")

        assert ok is not False
        assert err is None
        tc.add_lvol_mig_task.assert_called_once()
