"""A source snapshot whose replicated copy a live volume is cloned from is
held back -- soft-deleted, nothing removed -- until that clone goes, against
real FDB.

A failed-over volume is a clone of a replicated copy on the other cluster, and
the replication back home chains onto that pair. KI-12, the fail-back of run
20261026: the healed origin's stop cleanup deleted the source half of the
pair the fail-over had used (it was not the newest pair, which the cleanup
keeps). The snapshot monitor finished that delete by handing it on to the
source's "instance", the copy, and set the copy in_deletion; it then refused
to delete it (live clone), so the copy stayed in_deletion for good and every
reverse replication stopped on "chain ancestor ... is mid-deletion".

``_delete_locked`` now soft-deletes such a source before anything reaches the
data plane, and ``delete_held_sources`` -- called when a clone of the copy is
deleted, after the fail-back -- deletes it once no live clone is left.
Mocked is only what sits above the database: ``snapshot_controller.delete`` in
the release tests, replaced by a recorder.
"""

import pytest

from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

ORIGIN = "cluster-a"
FAILOVER = "cluster-b"
POOL_ID = "pool-1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _write_lvol(db, uuid, cloned_from_snap="", status=LVol.STATUS_ONLINE):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.pool_uuid = POOL_ID
    lvol.node_id = "N1"
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.lvol_bdev = f"LVOL_{uuid}"
    lvol.lvs_name = "LVS_1"
    lvol.top_bdev = f"LVS_1/LVOL_{uuid}"
    lvol.size = 1024 ** 3
    lvol.status = status
    lvol.cloned_from_snap = cloned_from_snap
    lvol.write_to_db(db.kv_store)
    return lvol


def _write_snapshot(db, uuid, lvol, cluster_id, target="", instances=(), deleted=False):
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = cluster_id
    snap.pool_uuid = POOL_ID
    snap.status = SnapShot.STATUS_ONLINE
    snap.snap_type = SnapShot.TYPE_INTERNAL
    snap.target_replicated_snap_uuid = target
    snap.snap_name = f"SNAP_{uuid}"
    snap.snap_bdev = f"LVS_1/{uuid}"
    snap.snap_uuid = f"uuid-{uuid}"
    snap.size = lvol.size
    snap.lvol = lvol
    snap.instances = list(instances)
    snap.deleted = deleted
    snap.write_to_db(db.kv_store)
    return snap


@pytest.fixture
def copy_lvol(db):
    return _write_lvol(db, "TGT_LV")


@pytest.fixture
def source_lvol(db):
    return _write_lvol(db, "SRC_LV")


def test_a_source_whose_copy_has_a_live_clone_is_only_soft_deleted(db, source_lvol, copy_lvol):
    _write_snapshot(db, "SRC", source_lvol, ORIGIN, target="TGT")
    _write_snapshot(db, "TGT", copy_lvol, FAILOVER)
    _write_lvol(db, "FAILED_OVER", cloned_from_snap="TGT")

    assert snapshot_controller.delete("SRC") is True

    src = db.get_snapshot_by_id("SRC")
    assert src.deleted is True
    assert src.status == SnapShot.STATUS_ONLINE, "nothing may reach the data plane or the monitor"
    assert db.get_snapshot_by_id("TGT").status == SnapShot.STATUS_ONLINE


def test_an_instance_on_the_other_cluster_holds_the_source_too(db, source_lvol, copy_lvol):
    copy = _write_snapshot(db, "TGT", copy_lvol, FAILOVER)
    _write_snapshot(db, "SRC", source_lvol, ORIGIN, instances=[copy.get_clean_dict()])
    _write_lvol(db, "FAILED_OVER", cloned_from_snap="TGT")

    assert snapshot_controller.delete("SRC") is True
    assert db.get_snapshot_by_id("SRC").deleted is True


def test_an_instance_on_the_same_cluster_is_not_a_replicated_copy(db, source_lvol, copy_lvol):
    # A migration copy: the same snapshot on another node of its own cluster.
    copy = _write_snapshot(db, "MIG", copy_lvol, ORIGIN)
    src = _write_snapshot(db, "SRC", source_lvol, ORIGIN, instances=[copy.get_clean_dict()])
    _write_lvol(db, "CLONE", cloned_from_snap="MIG")

    assert snapshot_controller._replicated_copy_with_live_clone(src) == ""


def test_a_copy_whose_clone_is_being_deleted_does_not_hold_the_source(db, source_lvol, copy_lvol):
    src = _write_snapshot(db, "SRC", source_lvol, ORIGIN, target="TGT")
    _write_snapshot(db, "TGT", copy_lvol, FAILOVER)
    _write_lvol(db, "GOING", cloned_from_snap="TGT", status=LVol.STATUS_IN_DELETION)

    assert snapshot_controller._replicated_copy_with_live_clone(src) == ""


@pytest.fixture
def deletes(monkeypatch):
    deleted = []
    monkeypatch.setattr(snapshot_controller, "delete",
                        lambda uuid, *args, **kwargs: deleted.append(uuid) or True)
    return deleted


def test_the_last_clone_going_releases_the_held_source(db, source_lvol, copy_lvol, deletes):
    _write_snapshot(db, "SRC", source_lvol, ORIGIN, target="TGT", deleted=True)
    _write_snapshot(db, "OTHER", source_lvol, ORIGIN, target="TGT2", deleted=True)
    _write_snapshot(db, "TGT", copy_lvol, FAILOVER)
    _write_lvol(db, "FAILED_OVER", cloned_from_snap="TGT", status=LVol.STATUS_IN_DELETION)

    snapshot_controller.delete_held_sources("TGT")

    assert deletes == ["SRC"]


def test_a_remaining_live_clone_keeps_holding_the_source(db, source_lvol, copy_lvol, deletes):
    _write_snapshot(db, "SRC", source_lvol, ORIGIN, target="TGT", deleted=True)
    _write_snapshot(db, "TGT", copy_lvol, FAILOVER)
    _write_lvol(db, "GOING", cloned_from_snap="TGT", status=LVol.STATUS_IN_DELETION)
    _write_lvol(db, "STAYING", cloned_from_snap="TGT")

    snapshot_controller.delete_held_sources("TGT")

    assert deletes == []


def test_a_source_not_held_back_is_left_alone(db, source_lvol, copy_lvol, deletes):
    _write_snapshot(db, "SRC", source_lvol, ORIGIN, target="TGT")
    _write_snapshot(db, "TGT", copy_lvol, FAILOVER)

    snapshot_controller.delete_held_sources("TGT")

    assert deletes == []
