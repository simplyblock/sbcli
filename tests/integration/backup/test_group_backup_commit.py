"""The all-or-nothing commit barrier for a consistency-group backup, against
real FoundationDB.

The database is real; nothing here mocks the DB layer. The only boundaries
patched are the ones above it: the S3 manifest write and the bucket-credential
lookup, which the integration tier never reaches a real endpoint for. The
member backups are seeded as real Backup records and driven by setting their
status, standing in for what the FN_BACKUP runner does.
"""
import zlib
from unittest.mock import patch

import pytest

from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, GroupBackup


def _uuid(name: str) -> str:
    # A deterministic, valid UUID string: the commit path converts member and
    # group ids with UUID(...) while building the manifest, so the fixtures must
    # be real UUIDs, not readable labels.
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")


@pytest.fixture
def db():
    return DBController()


def _member(name, group_backup_id, status, size=100):
    b = Backup()
    b.uuid = _uuid(name)
    b.cluster_id = CLUSTER
    b.lvol_id = _uuid("lv-" + name)
    b.lvol_name = name
    b.s3_id = size
    b.size = size
    b.group_backup_id = group_backup_id
    b.group_seq = 7
    b.status = status
    return b


def _group_backup(members, status=GroupBackup.STATUS_IN_PROGRESS):
    gb = GroupBackup()
    gb.uuid = _uuid("gb")
    gb.cluster_id = CLUSTER
    gb.consistency_group_id = f"{CLUSTER}/{_uuid('cg')}"
    gb.consistency_group_name = "gitea"
    gb.group_seq = 7
    gb.created_at = 100
    gb.status = status
    gb.member_backup_ids = [m.get_id() for m in members]
    return gb


def _seed(db, members, status=GroupBackup.STATUS_IN_PROGRESS):
    for m in members:
        m.write_to_db(db.kv_store)
    gb = _group_backup(members, status=status)
    gb.write_to_db(db.kv_store)
    return gb


def test_pending_member_defers_without_touching_s3(db):
    gb = _seed(db, [
        _member("b-db", "", Backup.STATUS_COMPLETED),
        _member("b-data", "", Backup.STATUS_IN_PROGRESS),
    ])
    # group_backup_id on the members is irrelevant here: try_commit reads the
    # members by the ids recorded on the group backup, not by the index.
    with patch.object(group_backup.backup_manifest, "write_group") as write_group:
        state = group_backup.try_commit_group_backup(gb.get_id())

    assert state == group_backup.CommitState.PENDING
    write_group.assert_not_called()
    assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_IN_PROGRESS


def test_failed_member_fails_the_group_without_touching_s3(db):
    gb = _seed(db, [
        _member("b-db", "", Backup.STATUS_COMPLETED),
        _member("b-data", "", Backup.STATUS_FAILED),
    ])
    with patch.object(group_backup.backup_manifest, "write_group") as write_group:
        state = group_backup.try_commit_group_backup(gb.get_id())

    assert state == group_backup.CommitState.FAILED
    write_group.assert_not_called()
    stored = db.get_group_backup_by_id(gb.get_id())
    assert stored.status == GroupBackup.STATUS_FAILED
    assert "failed" in stored.error_message


def test_all_completed_publishes_the_manifest_then_completes(db):
    members = [
        _member("b-db", "", Backup.STATUS_COMPLETED, size=100),
        _member("b-data", "", Backup.STATUS_COMPLETED, size=50),
    ]
    gb = _seed(db, members)

    with patch.object(group_backup.backup_manifest, "write_group") as write_group, \
            patch.object(group_backup, "_config_for_group", return_value="cfg"):
        state = group_backup.try_commit_group_backup(gb.get_id())

    assert state == group_backup.CommitState.READY
    # The manifest is published exactly once, before the record is completed.
    write_group.assert_called_once()
    _, manifest = write_group.call_args[0]
    assert {str(i) for i in manifest.member_backup_ids} == {_uuid("b-db"), _uuid("b-data")}
    assert manifest.size == 150

    stored = db.get_group_backup_by_id(gb.get_id())
    assert stored.status == GroupBackup.STATUS_COMPLETED
    assert stored.size == 150
    assert stored.completed_at > 0


def test_a_missing_member_record_fails_the_group(db):
    members = [_member("b-db", "", Backup.STATUS_COMPLETED)]
    gb = _group_backup(members)
    # Record a second member on the group backup whose Backup record was never
    # written (e.g. hard-deleted): the group cannot be committed.
    gb.member_backup_ids.append(f"{CLUSTER}/b-gone")
    members[0].write_to_db(db.kv_store)
    gb.write_to_db(db.kv_store)

    with patch.object(group_backup.backup_manifest, "write_group") as write_group:
        state = group_backup.try_commit_group_backup(gb.get_id())

    assert state == group_backup.CommitState.FAILED
    write_group.assert_not_called()
    assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_FAILED


def test_commit_is_idempotent_once_completed(db):
    gb = _seed(db, [_member("b-db", "", Backup.STATUS_COMPLETED)],
               status=GroupBackup.STATUS_COMPLETED)
    with patch.object(group_backup.backup_manifest, "write_group") as write_group:
        state = group_backup.try_commit_group_backup(gb.get_id())

    assert state == group_backup.CommitState.READY
    write_group.assert_not_called()


def test_commit_of_an_unknown_group_backup_is_terminal(db):
    assert group_backup.try_commit_group_backup(f"{CLUSTER}/nope") == group_backup.CommitState.FAILED
