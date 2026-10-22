"""Resolving a consistency group at a generation to its member snapshots, against
real FoundationDB.

`generation_snapshots(group, seq)` is the one lookup of a generation's member
snapshots. `resolve_complete_generation(group, seq)` also requires the
generation whole: an ONLINE snapshot of every volume that was a member at that
sequence (the membership epochs, `ConsistencyGroup.included_in_seq`). A group
backup is taken through the strict form, so a generation missing a member is
refused rather than backed up as a group that silently lacks a volume
(design-consistency-group-backup.md §5).
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.snapshot import SnapShot

CLUSTER_ID = "cgr-cluster-1"


@pytest.fixture
def db():
    return DBController()


def _lvol(db, uuid):
    lvol = LVol()
    lvol.uuid = uuid
    lvol.cluster_id = CLUSTER_ID
    lvol.lvol_name = f"VOL_{uuid}"
    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db.kv_store)
    return lvol


def _group(db, members, uuid="cgr-group-1"):
    """A group whose members map lvol uuid -> (joined_seq, removed_seq)."""
    group = ConsistencyGroup()
    group.uuid = uuid
    group.cluster_id = CLUSTER_ID
    group.group_name = uuid
    group.members = {lv: {"joined_seq": j, "removed_seq": r} for lv, (j, r) in members.items()}
    group.last_group_seq = 9
    group.write_to_db(db.kv_store)
    return group


def _snap(db, group, lvol, seq, status=SnapShot.STATUS_ONLINE, uuid=None):
    snap = SnapShot()
    snap.uuid = uuid or f"cgr-snap-{group.uuid}-{lvol.uuid}-{seq}"
    snap.cluster_id = CLUSTER_ID
    snap.lvol = lvol
    snap.group_id = group.get_id()
    snap.group_seq = seq
    snap.status = status
    snap.write_to_db(db.kv_store)
    return snap


def _ids(snaps):
    return sorted(s.get_id() for s in snaps)


def test_generation_snapshots_are_this_groups_at_this_sequence(db):
    a, b = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    other = _group(db, {"lv-a": (1, 0)}, uuid="cgr-group-2")
    want = [_snap(db, group, a, 3), _snap(db, group, b, 3)]
    _snap(db, group, a, 4)
    _snap(db, other, a, 3)

    assert _ids(cgc.generation_snapshots(group, 3)) == _ids(want)


def test_a_complete_generation_resolves_to_its_member_snapshots(db):
    a, b = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    want = [_snap(db, group, a, 3), _snap(db, group, b, 3)]

    assert _ids(cgc.resolve_complete_generation(group, 3)) == _ids(want)


def test_a_generation_missing_a_member_is_refused_naming_it(db):
    a, _ = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    _snap(db, group, a, 3)

    with pytest.raises(cgc.IncompleteGenerationError, match="lv-b"):
        cgc.resolve_complete_generation(group, 3)


def test_a_member_snapshot_being_deleted_counts_as_missing(db):
    a, b = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    _snap(db, group, a, 3)
    _snap(db, group, b, 3, status=SnapShot.STATUS_IN_DELETION)

    with pytest.raises(cgc.IncompleteGenerationError, match="lv-b"):
        cgc.resolve_complete_generation(group, 3)


def test_only_the_members_at_that_sequence_are_expected(db):
    """A volume that joined later, or left earlier, is not part of the generation."""
    a = _lvol(db, "lv-a")
    _lvol(db, "lv-late"), _lvol(db, "lv-gone")
    group = _group(db, {"lv-a": (1, 0), "lv-late": (5, 0), "lv-gone": (1, 2)})
    want = [_snap(db, group, a, 3)]

    assert _ids(cgc.resolve_complete_generation(group, 3)) == _ids(want)


def test_a_generation_with_no_snapshots_is_refused(db):
    _lvol(db, "lv-a")
    group = _group(db, {"lv-a": (1, 0)})

    with pytest.raises(cgc.ConsistencyGroupError, match="no member snapshots"):
        cgc.resolve_complete_generation(group, 3)


def test_a_group_backup_of_an_incomplete_generation_is_refused(db):
    a, _ = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    _snap(db, group, a, 3)

    group_backup_id, err = group_backup.create_group_backup(group.get_id(), group_seq=3)

    assert group_backup_id is None
    assert "lv-b" in err


def test_a_replicated_copy_on_the_peer_cluster_is_not_a_member_snapshot(db):
    """A copy replicated to the peer carries the same group id, but belongs to
    the peer: deleting or counting a generation must not reach it."""
    a = _lvol(db, "lv-a")
    group = _group(db, {"lv-a": (1, 0)})
    mine = _snap(db, group, a, 3)
    copy = _snap(db, group, a, 3, uuid="cgr-copy-on-peer")
    copy.cluster_id = "cgr-cluster-peer"
    copy.write_to_db(db.kv_store)

    assert _ids(cgc.generation_snapshots(group, 3)) == _ids([mine])


def test_a_group_backup_backs_up_the_generation_its_take_answered(db, monkeypatch):
    """Without a sequence, the backup takes a generation and backs up the one
    the take answers with, not one read back off a snapshot it created."""
    a, _ = _lvol(db, "lv-a"), _lvol(db, "lv-b")
    group = _group(db, {"lv-a": (1, 0), "lv-b": (1, 0)})
    _snap(db, group, a, 3)
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group", lambda g: (3, None))

    group_backup_id, err = group_backup.create_group_backup(group.get_id())

    assert group_backup_id is None
    assert "lv-b" in err
