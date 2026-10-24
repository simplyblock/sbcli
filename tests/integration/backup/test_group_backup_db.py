"""GroupBackup records round-trip and the group indexes resolve, against real
FoundationDB.

The database is real; nothing here mocks the DB layer. Nodes are not involved.
"""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, GroupBackup


@pytest.fixture
def db():
    return DBController()


def _group_backup(uuid="gb-1", cluster="cl-1", cg="cl-1/cg-1", seq=7):
    gb = GroupBackup()
    gb.uuid = uuid
    gb.cluster_id = cluster
    gb.consistency_group_id = cg
    gb.consistency_group_name = "gitea"
    gb.group_seq = seq
    gb.member_backup_ids = [f"{cluster}/b-db"]
    return gb


def _member_backup(uuid, cluster, group_backup_id, lvol="lv-db"):
    b = Backup()
    b.uuid = uuid
    b.cluster_id = cluster
    b.lvol_id = lvol
    b.group_backup_id = group_backup_id
    b.group_seq = 7
    return b


def test_group_backup_round_trips_and_reads_by_id(db):
    gb = _group_backup()
    gb.write_to_db(db.kv_store)

    got = db.get_group_backup_by_id(gb.get_id())
    assert got.get_id() == gb.get_id()
    assert got.consistency_group_id == "cl-1/cg-1"
    assert got.member_backup_ids == gb.member_backup_ids


def test_get_group_backup_by_id_missing_raises(db):
    with pytest.raises(KeyError):
        db.get_group_backup_by_id("cl-1/nope")


def test_group_backups_by_consistency_group(db):
    a = _group_backup(uuid="gb-a", seq=1)
    b = _group_backup(uuid="gb-b", seq=2)
    other = _group_backup(uuid="gb-c", cg="cl-1/cg-2", seq=1)
    for g in (a, b, other):
        g.write_to_db(db.kv_store)

    got = db.get_group_backups_by_consistency_group("cl-1/cg-1")
    assert {g.uuid for g in got} == {"gb-a", "gb-b"}


def test_backups_by_group_backup_id_finds_members_only(db):
    gid = "cl-1/gb-1"
    m1 = _member_backup("b-db", "cl-1", gid, lvol="lv-db")
    m2 = _member_backup("b-data", "cl-1", gid, lvol="lv-data")
    standalone = _member_backup("b-solo", "cl-1", "", lvol="lv-x")
    for b in (m1, m2, standalone):
        b.write_to_db(db.kv_store)

    got = db.get_backups_by_group_backup_id(gid)
    assert {b.uuid for b in got} == {"b-db", "b-data"}


def test_standalone_backup_is_not_indexed_under_the_empty_group(db):
    # A blank group_backup_id is dropped by the index, so a standalone backup
    # does not pile onto a "" key (models/indices.py).
    _member_backup("b-solo", "cl-1", "", lvol="lv-x").write_to_db(db.kv_store)
    assert db.get_backups_by_group_backup_id("") == []
