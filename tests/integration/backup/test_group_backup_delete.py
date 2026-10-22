"""Deleting a group backup, against real FoundationDB (design §9).

A group backup is deleted the way it is created, metadata outermost: the group
manifest and the record are first marked as being deleted, then the member
backups are retired (a leaf deleted, a base merged into the next generation's
copy), and only once every member is retired are the manifest and the record
removed. A delete that fails before any member was touched restores the group
backup as it was; one that fails after says so, by marking it broken, in the
record and in the bucket.

The database is real. The bucket is the one boundary faked: the S3 client the
group manifest functions use is an in-memory one.
"""
import io
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import GroupManifestState
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule

CLUSTER = "c3000000-0000-4000-8000-000000000001"
NODE = "d3000000-0000-4000-8000-000000000001"
BUCKET = "simplyblock-backup-primary"
GEN_7 = "9d000000-0000-4000-8000-000000000007"
GEN_8 = "9d000000-0000-4000-8000-000000000008"


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


class _GroupManifests:
    """An in-memory bucket, behind the three object calls the group manifest
    functions make, read and written by group backup uuid."""

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

    @staticmethod
    def _key(uuid):
        return backup_manifest.group_manifest_key(UUID(uuid))

    def __getitem__(self, uuid):
        return backup_manifest._parse_group(self.objects[self._key(uuid)], self._key(uuid))

    def __setitem__(self, uuid, manifest):
        self.objects[self._key(uuid)] = manifest.model_dump_json().encode()

    def __contains__(self, uuid):
        return self._key(uuid) in self.objects

    def __len__(self):
        return len(self.objects)


@pytest.fixture
def bucket(monkeypatch):
    """The group manifests in the bucket. The manifest functions run for real
    over it; only the S3 client is faked."""
    b = _GroupManifests()
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)
    return b


def _backup_uuid(gen, lvol):
    return f"0ae0{gen:04d}-0000-4000-8000-{lvol:012d}"


def _member(db, gen_uuid, gen, lvol, prev_gen=None, status=Backup.STATUS_COMPLETED):
    b = Backup()
    b.uuid = _backup_uuid(gen, lvol)
    b.s3_id = gen * 100 + lvol
    b.cluster_id = CLUSTER
    b.lvol_id = f"lv-{lvol}"
    b.node_id = NODE
    b.snapshot_id = f"5a000000-0000-4000-8000-{gen * 100 + lvol:012d}"
    b.prev_backup_id = _backup_uuid(prev_gen, lvol) if prev_gen is not None else ""
    b.group_backup_id = f"{CLUSTER}/{gen_uuid}"
    b.group_seq = gen
    b.size = 4096
    b.created_at = gen
    b.status = status
    b.location = _config().location().model_dump(mode="json")
    b.write_to_db(db.kv_store)
    return b


def _group_backup(db, bucket, gen_uuid, gen, members, status=GroupBackup.STATUS_COMPLETED):
    gb = GroupBackup()
    gb.uuid = gen_uuid
    gb.cluster_id = CLUSTER
    gb.consistency_group_id = f"{CLUSTER}/cccc0000-0000-4000-8000-000000000001"
    gb.consistency_group_name = "gitea"
    gb.group_seq = gen
    gb.members = [{"lvol_id": m.lvol_id, "backup_id": m.get_id()} for m in members]
    gb.created_at = 100
    gb.status = status
    if status == GroupBackup.STATUS_COMPLETED:
        gb.completed_at = 200
        bucket[gen_uuid] = group_backup.build_group_manifest(
            gb, {m.get_id(): m for m in members})
    gb.write_to_db(db.kv_store)
    return gb


def _two_generations(db, bucket):
    """Generation 7 of two volumes, and generation 8 of one of them.

    lv-1's generation-7 copy is the base its generation-8 copy chains against,
    so retiring it is a merge; lv-2's has no successor, so it is a leaf.
    """
    base = _member(db, GEN_7, 7, 1)
    leaf = _member(db, GEN_7, 7, 2)
    gb = _group_backup(db, bucket, GEN_7, 7, [base, leaf])
    successor = _member(db, GEN_8, 8, 1, prev_gen=7)
    _group_backup(db, bucket, GEN_8, 8, [successor])
    return gb, base, leaf, successor


def _status(db, backup):
    try:
        return db.get_backup_by_id(backup.get_id()).status
    except KeyError:
        return None


def _delete_tasks(db):
    return [t for t in db.get_job_tasks_by_function(CLUSTER, JobSchedule.FN_GROUP_BACKUP_DELETE)
            if t.status != JobSchedule.STATUS_DONE and not t.canceled]


class TestMarkFirst:

    def test_the_record_and_manifest_are_marked_before_any_member_is_touched(self, db, bucket):
        gb, base, leaf, _ = _two_generations(db, bucket)

        group_backup.delete_group_backup(gb.get_id())

        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_DELETING
        assert bucket[GEN_7].state == GroupManifestState.DELETING
        assert _status(db, base) == Backup.STATUS_COMPLETED
        assert _status(db, leaf) == Backup.STATUS_COMPLETED
        assert len(_delete_tasks(db)) == 1

    def test_a_manifest_that_cannot_be_marked_refuses_the_delete_and_changes_nothing(
            self, db, bucket, monkeypatch):
        gb, base, leaf, _ = _two_generations(db, bucket)

        def unreachable(config, manifest):
            raise backup_manifest.ManifestError("bucket unreachable")
        monkeypatch.setattr(backup_manifest, "write_group", unreachable)

        with pytest.raises(backup_manifest.ManifestError):
            group_backup.delete_group_backup(gb.get_id())

        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_COMPLETED
        assert bucket[GEN_7].state == GroupManifestState.COMMITTED
        assert _delete_tasks(db) == []

    def test_a_group_backup_still_committing_is_refused(self, db, bucket):
        member = _member(db, GEN_7, 7, 1, status=Backup.STATUS_IN_PROGRESS)
        gb = _group_backup(db, bucket, GEN_7, 7, [member], status=GroupBackup.STATUS_IN_PROGRESS)

        with pytest.raises(PreconditionError, match="committing"):
            group_backup.delete_group_backup(gb.get_id())

        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_IN_PROGRESS

    def test_deleting_a_group_backup_already_being_deleted_queues_no_second_task(self, db, bucket):
        gb, *_ = _two_generations(db, bucket)

        group_backup.delete_group_backup(gb.get_id())
        group_backup.delete_group_backup(gb.get_id())

        assert len(_delete_tasks(db)) == 1

    def test_an_unknown_group_backup_raises(self, db, bucket):
        with pytest.raises(KeyError):
            group_backup.delete_group_backup(f"{CLUSTER}/{GEN_7}")


class TestRetireMembers:

    def test_a_leaf_is_deleted_and_a_base_merged_while_the_group_stays(self, db, bucket):
        gb, base, leaf, _ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())

        state = group_backup.advance_group_backup_delete(gb.get_id())

        assert state == group_backup.DeleteState.PENDING
        assert _status(db, leaf) is None
        assert _status(db, base) == Backup.STATUS_MERGING
        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_DELETING
        assert bucket[GEN_7].state == GroupManifestState.DELETING

    def test_the_manifest_and_record_go_only_once_every_member_is_retired(self, db, bucket):
        gb, base, leaf, _ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())
        group_backup.advance_group_backup_delete(gb.get_id())
        merged = db.get_backup_by_id(base.get_id())
        merged.status = Backup.STATUS_MERGED      # the merge task finished
        merged.write_to_db(db.kv_store)

        state = group_backup.advance_group_backup_delete(gb.get_id())

        assert state == group_backup.DeleteState.DONE
        assert GEN_7 not in bucket
        with pytest.raises(KeyError):
            db.get_group_backup_by_id(gb.get_id())

    def test_the_next_generation_is_left_whole(self, db, bucket):
        gb, base, leaf, successor = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())

        group_backup.advance_group_backup_delete(gb.get_id())

        assert _status(db, successor) == Backup.STATUS_COMPLETED
        assert db.get_group_backup_by_id(f"{CLUSTER}/{GEN_8}").status == GroupBackup.STATUS_COMPLETED
        assert bucket[GEN_8].state == GroupManifestState.COMMITTED

    def test_a_failed_group_backup_has_no_manifest_to_mark(self, db, bucket):
        member = _member(db, GEN_7, 7, 1, status=Backup.STATUS_FAILED)
        gb = _group_backup(db, bucket, GEN_7, 7, [member], status=GroupBackup.STATUS_FAILED)

        group_backup.delete_group_backup(gb.get_id())
        state = group_backup.advance_group_backup_delete(gb.get_id())

        assert state == group_backup.DeleteState.DONE
        assert len(bucket) == 0
        assert _status(db, member) is None


class TestFailure:

    def test_a_delete_that_touched_no_member_restores_the_group_backup(self, db, bucket):
        gb, base, leaf, _ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())

        group_backup.abandon_group_backup_delete(gb.get_id(), "timeout after 3600s")

        restored = db.get_group_backup_by_id(gb.get_id())
        assert restored.status == GroupBackup.STATUS_COMPLETED
        assert "timeout" in restored.error_message
        assert bucket[GEN_7].state == GroupManifestState.COMMITTED

    def test_a_delete_that_retired_a_member_marks_the_group_backup_broken(self, db, bucket):
        gb, base, leaf, _ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())
        group_backup.advance_group_backup_delete(gb.get_id())   # leaf deleted, base merging

        group_backup.abandon_group_backup_delete(gb.get_id(), "timeout after 3600s")

        broken = db.get_group_backup_by_id(gb.get_id())
        assert broken.status == GroupBackup.STATUS_BROKEN
        assert "timeout" in broken.error_message
        assert bucket[GEN_7].state == GroupManifestState.BROKEN

    def test_a_broken_group_backup_is_not_restorable(self, db, bucket):
        gb, *_ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())
        group_backup.advance_group_backup_delete(gb.get_id())
        group_backup.abandon_group_backup_delete(gb.get_id(), "timeout")

        _, _, err = group_backup.restore_group_backup(gb.get_id(), "restored", "pool")

        assert err is not None

    def test_a_broken_group_backup_can_be_deleted_again(self, db, bucket):
        gb, base, *_ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())
        group_backup.advance_group_backup_delete(gb.get_id())
        group_backup.abandon_group_backup_delete(gb.get_id(), "timeout")
        for task in _delete_tasks(db):
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)

        group_backup.delete_group_backup(gb.get_id())

        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_DELETING
        assert bucket[GEN_7].state == GroupManifestState.DELETING
        assert len(_delete_tasks(db)) == 1


class TestTask:
    """The FN_GROUP_BACKUP_DELETE task drives the retirement across cycles."""

    def _task(self, db, gb):
        return [t for t in _delete_tasks(db)
                if t.function_params.get("group_backup_id") == gb.uuid][0]

    def test_it_defers_while_a_member_is_still_being_retired(self, db, bucket):
        from simplyblock_core.services import tasks_runner_backup as runner
        from simplyblock_core.services.task_runner_base import TaskDefer
        gb, *_ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())

        with pytest.raises(TaskDefer):
            runner._run_group_backup_delete(self._task(db, gb))

    def test_it_owns_the_delete_function(self):
        from simplyblock_core.services import tasks_runner_backup as runner
        assert JobSchedule.FN_GROUP_BACKUP_DELETE in runner.SPEC.function_names
        assert JobSchedule.FN_GROUP_BACKUP_DELETE in runner.BACKUP_FUNCTIONS

    def test_a_task_that_ends_unfinished_settles_the_group_backup(self, db, bucket):
        """A timeout, the retry ceiling, or a cancellation the handler never sees:
        the group backup is restored or marked broken, never left deleting."""
        from simplyblock_core.services import tasks_runner_backup as runner
        gb, *_ = _two_generations(db, bucket)
        group_backup.delete_group_backup(gb.get_id())
        task = self._task(db, gb)
        task.function_result = "timeout after 3600s"

        runner.finalize_resource(task)

        assert db.get_group_backup_by_id(gb.get_id()).status == GroupBackup.STATUS_COMPLETED
        assert bucket[GEN_7].state == GroupManifestState.COMMITTED
