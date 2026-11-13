"""Deleting (or merging away) one backup, and deleting every backup of a
volume, against real FoundationDB.

A backup with nothing chaining against it is claimed for deletion: its
status moves to DELETING, its manifest is marked DELETING in the bucket, and
a task is queued to remove its S3 objects. A backup something chains against
is instead merged into that successor -- deleting it and merging it are the
same request once a successor exists (``trigger_merge``). Both forms go
through the same chain-wide guard (``DBController.claim_backup_chain_op``),
so a chain already claimed by another merge, delete, or an active restore
refuses a second one.

The database is real. The bucket is the one boundary faked: the S3 client the
manifest functions use is an in-memory one, so manifest marking runs for
real over it.
"""
import io
import zlib
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest,
    BackupManifestState,
    ManifestDataPlane,
    ManifestSource,
    ManifestVolume,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule


def _uuid(name: str) -> str:
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")
LVOL = _uuid("lvol")
NODE = _uuid("node")
BUCKET = "simplyblock-backup-cluster"


def _config():
    return BackupConfig.model_validate({"bucket_name": BUCKET, "region": "eu-central-1"})


@pytest.fixture
def db():
    db = DBController()
    c = Cluster()
    c.uuid = CLUSTER
    c.backup_config = _config().model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return db


class _Bucket:
    """An in-memory S3 bucket, behind the manifest calls and the data-object
    listing/deletion calls ``backup_manifest.delete_data_objects`` makes."""

    def __init__(self):
        self.objects: dict = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, _name):
        objects = self.objects

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in objects if k.startswith(Prefix)]}
        return _Paginator()


@pytest.fixture
def bucket(monkeypatch):
    """The backup manifests in the bucket. The manifest functions run for
    real over it; only the S3 client is faked."""
    b = _Bucket()
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)
    return b


def _manifest(backup_id: str) -> BackupManifest:
    return BackupManifest(
        backup_id=UUID(backup_id),
        s3_id=zlib.crc32(backup_id.encode()) % 1000,
        created_at=100,
        completed_at=200,
        size=4096,
        source=ManifestSource(cluster_id=UUID(CLUSTER), node_id=UUID(NODE)),
        volume=ManifestVolume(
            lvol_id=UUID(LVOL), lvol_name="vol", snapshot_id=UUID(_uuid("snap")),
            snapshot_name="snap", size=4096),
        dataplane=ManifestDataPlane(),
    )


def _backup(db, bucket, name, prev="", group_backup_id="", status=Backup.STATUS_COMPLETED):
    b = Backup()
    b.uuid = _uuid(name)
    b.cluster_id = CLUSTER
    b.lvol_id = LVOL
    b.node_id = NODE
    b.s3_id = zlib.crc32(name.encode()) % 1000
    b.prev_backup_id = _uuid(prev) if prev else ""
    b.group_backup_id = group_backup_id
    b.status = status
    b.location = _config().location().model_dump(mode="json")
    b.write_to_db(db.kv_store)
    if status == Backup.STATUS_COMPLETED:
        backup_manifest.write(_config(), _manifest(b.uuid))
    return b


def _group_backup(db, status=GroupBackup.STATUS_COMPLETED):
    gb = GroupBackup()
    gb.uuid = _uuid("gb")
    gb.cluster_id = CLUSTER
    gb.status = status
    gb.write_to_db(db.kv_store)
    return gb


def _exists(db, backup):
    try:
        db.get_backup_by_id(backup.uuid)
        return True
    except KeyError:
        return False


def _status(db, backup):
    return db.get_backup_by_id(backup.uuid).status


def _manifest_state(bucket, backup):
    return backup_manifest._parse(
        bucket.objects[backup_manifest.manifest_key(UUID(backup.uuid))],
        "test").state


def _delete_tasks(db, backup_id=None):
    tasks = [t for t in db.get_job_tasks_by_function(CLUSTER, JobSchedule.FN_BACKUP_DELETE)
            if t.status != JobSchedule.STATUS_DONE and not t.canceled]
    if backup_id is None:
        return tasks
    return [t for t in tasks if backup_id in (t.function_params or {}).get("backup_ids", [])]


def _merge_tasks(db, old_backup_id=None):
    tasks = [t for t in db.get_job_tasks_by_function(CLUSTER, JobSchedule.FN_BACKUP_MERGE)
            if t.status != JobSchedule.STATUS_DONE and not t.canceled]
    if old_backup_id is None:
        return tasks
    return [t for t in tasks if (t.function_params or {}).get("old_backup_id") == old_backup_id]


def _restore_task(db, chain_s3_ids):
    """A live FN_BACKUP_RESTORE task reading ``chain_s3_ids``, so the restore
    index picks it up."""
    task, err = db.create_backup_restore_task_if_chain_clear(
        LVOL, CLUSTER, NODE,
        function_params={"backup_id": "", "lvol_name": "x", "lvol_id": "",
                         "chain_ids": chain_s3_ids, "s3_config": None},
        max_retry=1)
    assert err is None, err
    return task


def test_a_leaf_backup_is_claimed_and_queued_for_deletion(db, bucket):
    leaf = _backup(db, bucket, "leaf")

    assert backup_controller.delete_backup(leaf.uuid) is None

    assert _exists(db, leaf)
    assert _status(db, leaf) == Backup.STATUS_DELETING
    assert _manifest_state(bucket, leaf) == BackupManifestState.DELETING
    assert _delete_tasks(db, leaf.uuid)


def test_resuming_an_already_deleting_backup_requeues_without_reclaiming(db, bucket):
    leaf = _backup(db, bucket, "leaf")
    backup_controller.delete_backup(leaf.uuid)
    first_tasks = {t.uuid for t in _delete_tasks(db, leaf.uuid)}

    backup_controller.delete_backup(leaf.uuid)

    assert _status(db, leaf) == Backup.STATUS_DELETING
    assert {t.uuid for t in _delete_tasks(db, leaf.uuid)} == first_tasks


def test_a_backup_with_a_completed_successor_is_merged(db, bucket):
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")

    assert backup_controller.delete_backup(base.uuid) is None

    assert _status(db, base) == Backup.STATUS_MERGING
    assert _status(db, nxt) == Backup.STATUS_COMPLETED
    assert _merge_tasks(db, base.uuid)


def test_a_backup_with_an_incomplete_successor_is_refused(db, bucket):
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "next", prev="base", status=Backup.STATUS_IN_PROGRESS)

    with pytest.raises(PreconditionError, match="completed"):
        backup_controller.delete_backup(base.uuid)

    assert _status(db, base) == Backup.STATUS_COMPLETED


def test_a_member_of_a_group_backup_that_exists_is_refused(db, bucket):
    gb = _group_backup(db)
    member = _backup(db, bucket, "member", group_backup_id=gb.get_id())

    with pytest.raises(PreconditionError, match="group backup"):
        backup_controller.delete_backup(member.uuid)

    assert _status(db, member) == Backup.STATUS_COMPLETED


def test_a_member_whose_group_backup_is_gone_is_queued_for_deletion(db, bucket):
    orphan = _backup(db, bucket, "orphan", group_backup_id=f"{CLUSTER}/{_uuid('gone')}")

    backup_controller.delete_backup(orphan.uuid)

    assert _status(db, orphan) == Backup.STATUS_DELETING


def test_an_unknown_backup_raises(db):
    with pytest.raises(KeyError):
        backup_controller.delete_backup(_uuid("nope"))


def test_a_chain_already_being_merged_refuses_a_delete_of_another_backup(db, bucket):
    """Two leaves of the same volume's chain, one already MERGING -- claiming
    the other must see it and refuse."""
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")
    other_leaf = _backup(db, bucket, "other")
    ok, reason = backup_controller.trigger_merge(nxt, base)
    assert ok, reason

    with pytest.raises(PreconditionError, match="already"):
        backup_controller.delete_backup(other_leaf.uuid)

    assert _status(db, other_leaf) == Backup.STATUS_COMPLETED


def test_a_chain_read_by_an_active_restore_refuses_a_delete(db, bucket):
    leaf = _backup(db, bucket, "leaf")
    _restore_task(db, [leaf.s3_id])

    with pytest.raises(PreconditionError, match="restore"):
        backup_controller.delete_backup(leaf.uuid)

    assert _status(db, leaf) == Backup.STATUS_COMPLETED


def test_a_chain_read_by_an_active_restore_refuses_a_new_restore_task(db, bucket):
    """create_backup_restore_task_if_chain_clear itself refuses once a sibling
    is DELETING -- the other half of the same guard."""
    leaf = _backup(db, bucket, "leaf")
    backup_controller.delete_backup(leaf.uuid)

    task, err = db.create_backup_restore_task_if_chain_clear(
        LVOL, CLUSTER, NODE,
        function_params={"backup_id": leaf.uuid, "lvol_name": "x", "lvol_id": "",
                         "chain_ids": [leaf.s3_id], "s3_config": None},
        max_retry=1)

    assert task is None
    assert "deleting" in err


def test_delete_backups_refuses_while_a_backup_is_still_being_created(db, bucket):
    _backup(db, bucket, "done")
    _backup(db, bucket, "uploading", status=Backup.STATUS_IN_PROGRESS)

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "still being created" in err


def test_delete_backups_refuses_while_a_backup_is_merging(db, bucket):
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")
    ok, reason = backup_controller.trigger_merge(nxt, base)
    assert ok, reason

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "merged" in err


def test_delete_backups_refuses_for_a_live_group_member(db, bucket):
    gb = _group_backup(db)
    _backup(db, bucket, "member", group_backup_id=gb.get_id())

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "group backup" in err


def test_delete_backups_claims_completed_backups_and_removes_failed_ones_directly(db, bucket):
    completed = _backup(db, bucket, "completed")
    failed = _backup(db, bucket, "failed", status=Backup.STATUS_FAILED)

    ok, err = backup_controller.delete_backups(LVOL)

    assert ok, err
    assert _status(db, completed) == Backup.STATUS_DELETING
    assert _manifest_state(bucket, completed) == BackupManifestState.DELETING
    assert _delete_tasks(db, completed.uuid)
    assert not _exists(db, failed)


def test_delete_backups_is_a_noop_resume_once_everything_is_already_deleting(db, bucket):
    _backup(db, bucket, "leaf")
    ok, err = backup_controller.delete_backups(LVOL)
    assert ok, err

    ok, err = backup_controller.delete_backups(LVOL)

    assert ok, err


def _seed_data_objects(bucket, backup):
    """Fake data the data plane would have written under this backup's
    ``s3_id`` -- a metadata object (``mid=1``) and one data cluster
    (``mid=0``), per ``ManifestDataPlane.key_format``."""
    bucket.objects[f"{backup.s3_id}/1/0"] = b"metadata"
    bucket.objects[f"{backup.s3_id}/0/0"] = b"cluster-0"


class TestDeleteTask:
    """``FN_BACKUP_DELETE`` (``tasks_runner_backup._run_delete``): the task
    ``backup_controller.delete_backup``/``delete_backups`` queue after
    claiming a backup, which removes its S3 objects itself -- there is no
    data-plane RPC for this and none is planned. A backup with a successor is
    never reached here: that goes through a merge instead
    (``trigger_merge``/``_run_merge``), which is where the data plane stays
    involved.
    """

    def _task(self, db, backup_id):
        return _delete_tasks(db, backup_id)[0]

    def test_deleting_removes_data_objects_manifest_and_record(self, db, bucket):
        from simplyblock_core.services import tasks_runner_backup as runner
        leaf = _backup(db, bucket, "leaf")
        _seed_data_objects(bucket, leaf)
        backup_controller.delete_backup(leaf.uuid)

        runner._run_delete(self._task(db, leaf.uuid))

        assert not any(k.startswith(f"{leaf.s3_id}/") for k in bucket.objects)
        assert backup_manifest.manifest_key(UUID(leaf.uuid)) not in bucket.objects
        assert not _exists(db, leaf)

    def test_a_backup_no_longer_deleting_is_skipped(self, db, bucket):
        """A prior cycle of this same task (or, for a group member, a
        different caller) already retired it -- resuming must not re-delete
        objects out from under whatever claimed it next."""
        from simplyblock_core.services import tasks_runner_backup as runner
        leaf = _backup(db, bucket, "leaf")
        backup_controller.delete_backup(leaf.uuid)
        task = self._task(db, leaf.uuid)
        db.atomic_update(leaf, lambda b: setattr(b, "status", Backup.STATUS_COMPLETED))

        runner._run_delete(task)

        assert _status(db, leaf) == Backup.STATUS_COMPLETED

    def test_a_bucket_failure_retries_without_touching_the_manifest_or_record(
            self, db, bucket, monkeypatch):
        from simplyblock_core.services import tasks_runner_backup as runner
        from simplyblock_core.services.task_runner_base import TaskRetry
        leaf = _backup(db, bucket, "leaf")
        _seed_data_objects(bucket, leaf)
        backup_controller.delete_backup(leaf.uuid)

        def unreachable(_name):
            raise ClientError({"Error": {"Code": "500"}}, "ListObjectsV2")
        monkeypatch.setattr(bucket, "get_paginator", unreachable)

        with pytest.raises(TaskRetry):
            runner._run_delete(self._task(db, leaf.uuid))

        assert _status(db, leaf) == Backup.STATUS_DELETING
        assert _manifest_state(bucket, leaf) == BackupManifestState.DELETING
        assert any(k.startswith(f"{leaf.s3_id}/") for k in bucket.objects)
