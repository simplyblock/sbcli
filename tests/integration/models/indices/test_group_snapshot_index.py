"""The SnapShot (group_id, group_seq) index: a consistency group's snapshots, and
one generation's, without scanning every snapshot of the cluster.

The answers must be the same while the index builds (the filtered-scan fallback)
and once it is ready, and a snapshot outside any group, whose group_id is blank,
has no entry at all.
"""
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot

from .helpers import CLUSTER, index_keys, ready

GROUP_A = f"{CLUSTER}/group-a"
GROUP_B = f"{CLUSTER}/group-b"


def _snap(db, uuid, group_id="", group_seq=0):
    snap = SnapShot()
    snap.uuid = uuid
    snap.cluster_id = CLUSTER
    snap.group_id = group_id
    snap.group_seq = group_seq
    snap.lvol = LVol()
    snap.lvol.uuid = f"lv-{uuid}"
    snap.write_to_db(db.kv_store)
    return snap


def _seed(db):
    _snap(db, "a-3-x", GROUP_A, 3)
    _snap(db, "a-3-y", GROUP_A, 3)
    _snap(db, "a-4-x", GROUP_A, 4)
    _snap(db, "b-3-x", GROUP_B, 3)
    _snap(db, "loose")


def _uuids(snaps):
    return sorted(s.uuid for s in snaps)


def _answers(db):
    return (_uuids(db.get_group_snapshots(GROUP_A)),
            _uuids(db.get_group_snapshots(GROUP_A, 3)),
            _uuids(db.get_group_snapshots(GROUP_B, 4)))


def test_group_snapshots_agree_across_the_flip(db):
    _seed(db)
    building = _answers(db)
    ready(SnapShot)
    assert _answers(db) == building == (
        ["a-3-x", "a-3-y", "a-4-x"], ["a-3-x", "a-3-y"], [])


def test_a_snapshot_outside_any_group_is_not_indexed(db):
    _seed(db)
    ready(SnapShot)
    keys = [k for k in index_keys(db, SnapShot) if b"/group_id+group_seq/" in k]
    assert keys and not any(k.endswith(b"loose") for k in keys)


def test_a_blank_group_has_no_snapshots(db):
    _seed(db)
    ready(SnapShot)
    assert db.get_group_snapshots("") == []
