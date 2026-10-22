"""Deleting a source snapshot keeps its replicated copy while a volume is
cloned from that copy, against real FDB.

A failed-over volume is a clone of its last replicated target snapshot, and
the replication back home chains from that copy. Run 20261021-1331-ebs: when
the healed origin stopped its old group's replication, the stop's cleanup
kept the target copy ("a volume is cloned from it") and then deleted the
source half; the source's delete cascades into ``delete_replicated``, which
deleted the copy anyway (a soft delete: it has clones), and every reverse
replication then stopped on "chain ancestor ... is mid-deletion".

``delete_replicated`` decides by reading model state: the source snapshot's
link to its copy (``get_snapshot_by_id``) and the volumes cloned from the copy
(``get_mini_lvols``). Mocked here, per the tier's rule, is only what sits above
the database: ``snapshot_controller.delete``, the data-plane delete of the
copy, replaced by a recorder.
"""

import pytest

from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

CLUSTER_ID = "cluster-1"
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


def _write_snapshot(db, uuid, lvol, target=""):
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER_ID
    snap.pool_uuid = POOL_ID
    snap.status = SnapShot.STATUS_ONLINE
    snap.snap_type = SnapShot.TYPE_INTERNAL
    snap.target_replicated_snap_uuid = target
    snap.snap_name = f"SNAP_{uuid}"
    snap.snap_bdev = f"LVS_1/{uuid}"
    snap.snap_uuid = f"uuid-{uuid}"
    snap.size = lvol.size
    snap.lvol = lvol
    snap.write_to_db(db.kv_store)
    return snap


@pytest.fixture
def pair(db, monkeypatch):
    """A source snapshot SRC and its replicated copy TGT; the deletes of TGT."""
    source_lvol = _write_lvol(db, "SRC_LV")
    target_lvol = _write_lvol(db, "TGT_LV")
    _write_snapshot(db, "SRC", source_lvol, target="TGT")
    _write_snapshot(db, "TGT", target_lvol)
    deleted = []
    monkeypatch.setattr(snapshot_controller, "delete",
                        lambda uuid, *args, **kwargs: deleted.append(uuid) or True)
    return deleted


def test_a_copy_a_live_volume_is_cloned_from_is_kept(db, pair):
    _write_lvol(db, "FAILED_OVER", cloned_from_snap="TGT")

    assert snapshot_controller.delete_replicated("SRC") is True
    assert pair == [], "the copy the failed-over clone depends on must survive"


def test_a_copy_without_a_clone_is_deleted(db, pair):
    assert snapshot_controller.delete_replicated("SRC") is True
    assert pair == ["TGT"]


def test_a_clone_that_is_itself_being_deleted_does_not_keep_the_copy(db, pair):
    _write_lvol(db, "GOING", cloned_from_snap="TGT", status=LVol.STATUS_IN_DELETION)

    assert snapshot_controller.delete_replicated("SRC") is True
    assert pair == ["TGT"]
