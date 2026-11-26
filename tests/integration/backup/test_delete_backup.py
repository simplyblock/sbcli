"""Deleting (or merging away) one backup, and deleting every backup of a
volume, against real FoundationDB.

Admission (``delete_backup``/``delete_backups``/``trigger_merge``) only
queues a task now -- it claims nothing. The claim (``DBController.
claim_backup_chain_op``, via ``_claim_for_delete``/``_claim_for_merge``) runs
as the first thing the queued task's own execution does
(``tasks_runner_backup._run_backup_delete``/``_run_backup_delete_chain``),
re-validating every precondition fresh (F2/F3 of
docs/reviews/backup-deletion-review-round3.md). Admission's own guard against
a second, conflicting operation on the same chain is a single atomic gate
keyed by ``lvol_id`` (``DBController.ensure_backup_delete_task``): at most one
open delete/merge task per chain, so a second request -- for the same
backup, a sibling, or the whole volume -- is refused outright as "chain
busy" rather than raced or silently superseded.

A backup with nothing chaining against it is claimed for deletion at
execution time: its status moves to DELETING, its manifest is marked
DELETING in the bucket, and its S3 objects are removed directly (no
data-plane RPC for this). A backup something chains against is instead
merged into that successor -- deleting it and merging it are the same
request once a successor exists (``trigger_merge``).

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
from simplyblock_core.controllers.backup import group_backup
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
    """Open FN_BACKUP_DELETE tasks: the single task type that covers both
    the direct-delete and fold-into-successor cases (D5/D8) -- which one a
    given task drives is read off the backup's own status at execution time,
    not from a separate task type."""
    tasks = [t for t in db.get_job_tasks_by_function(CLUSTER, JobSchedule.FN_BACKUP_DELETE)
            if t.status != JobSchedule.STATUS_DONE and not t.canceled]
    if backup_id is None:
        return tasks
    return [t for t in tasks if (t.function_params or {}).get("backup_id") == backup_id]


def _chain_delete_tasks(db, lvol_id=None):
    tasks = [t for t in db.get_job_tasks_by_function(CLUSTER, JobSchedule.FN_BACKUP_DELETE_CHAIN)
            if t.status != JobSchedule.STATUS_DONE and not t.canceled]
    if lvol_id is None:
        return tasks
    return [t for t in tasks if (t.function_params or {}).get("lvol_id") == lvol_id]


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


def test_a_leaf_backup_queues_a_delete_task_but_claims_nothing_yet(db, bucket):
    """Admission (F2/F3) no longer claims: the status/manifest transition to
    DELETING happens as the first thing the queued task's own execution does
    (see TestDeleteTask), not here."""
    leaf = _backup(db, bucket, "leaf")

    assert backup_controller.delete_backup(leaf.uuid) is None

    assert _exists(db, leaf)
    assert _status(db, leaf) == Backup.STATUS_COMPLETED
    assert _manifest_state(bucket, leaf) == BackupManifestState.ACTIVE
    assert _delete_tasks(db, leaf.uuid)


def test_repeating_a_delete_request_before_its_task_runs_refuses_as_busy(db, bucket):
    """Admission is one atomic "chain busy" gate per lvol_id now (F2/F3), not
    a claim with idempotent resume: a second request for the very same
    backup, before its task has even run once, is refused exactly like a
    request for an unrelated member of the same chain would be."""
    leaf = _backup(db, bucket, "leaf")
    backup_controller.delete_backup(leaf.uuid)
    first_tasks = {t.uuid for t in _delete_tasks(db, leaf.uuid)}

    with pytest.raises(PreconditionError, match="busy"):
        backup_controller.delete_backup(leaf.uuid)

    assert _status(db, leaf) == Backup.STATUS_COMPLETED
    assert {t.uuid for t in _delete_tasks(db, leaf.uuid)} == first_tasks


def test_resuming_a_broken_backup_requeues_its_delete_task(db, bucket):
    """Unlike DELETING/MERGING (always backed by an open task -- F2/F3 of
    docs/reviews/backup-deletion-review-round3.md), BROKEN has no such
    guarantee: its S3 data is already confirmed gone, so a task that ends
    before retiring it is deliberately left BROKEN rather than reverted.
    delete_backup re-admits a BROKEN backup through the normal gate (unlike
    DELETING/MERGING, which are a pure no-op) so this stuck case still has a
    way back: a manual re-invocation queues a fresh retire task."""
    leaf = _backup(db, bucket, "leaf")
    leaf.status = Backup.STATUS_BROKEN
    leaf.write_to_db(db.kv_store)

    backup_controller.delete_backup(leaf.uuid)

    assert _status(db, leaf) == Backup.STATUS_BROKEN
    assert _delete_tasks(db, leaf.uuid)


def test_resuming_delete_backups_with_broken_members_requeues_the_chain_task(db, bucket):
    """delete_backups needs the same BROKEN-tolerant resume as delete_backup
    (F2): a crash between claim_backup_chain_op's commit and the chain-delete
    task's own enqueue leaves every member DELETING/BROKEN with no task."""
    leaf = _backup(db, bucket, "leaf")
    leaf.status = Backup.STATUS_BROKEN
    leaf.write_to_db(db.kv_store)

    ok, err = backup_controller.delete_backups(LVOL)

    assert ok, err
    assert _chain_delete_tasks(db, LVOL)


def test_a_backup_with_a_completed_successor_queues_a_merge(db, bucket):
    """The merge-vs-delete decision and its claim (MERGING) both happen at
    the queued task's own execution now (F2/F3), not here -- see
    tests/unit/tasks/test_runner_specs.py for the execution-time merge fold
    (RPC-driven, so exercised with a mocked node/data plane there, not
    against this file's bucket-only fixture)."""
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")

    assert backup_controller.delete_backup(base.uuid) is None

    assert _status(db, base) == Backup.STATUS_COMPLETED
    assert _status(db, nxt) == Backup.STATUS_COMPLETED
    assert _delete_tasks(db, base.uuid)


def test_a_backup_with_an_incomplete_successor_is_refused(db, bucket):
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "next", prev="base", status=Backup.STATUS_IN_PROGRESS)

    with pytest.raises(PreconditionError, match="completed"):
        backup_controller.delete_backup(base.uuid)

    assert _status(db, base) == Backup.STATUS_COMPLETED


def test_a_forked_predecessor_raises_instead_of_merging_into_one_arbitrarily(db, bucket):
    """Two backups naming the same predecessor is a chain fork that should
    never occur (snapshot chains are linear and backup creation locks the
    whole ancestry) -- if one gets into the chain anyway (e.g. a corrupted
    import), deleting the predecessor must fail loudly, not silently merge it
    into whichever fork happens to be completed and ignore the other, even
    one still being uploaded."""
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "completed_fork", prev="base")
    _backup(db, bucket, "uploading_fork", prev="base", status=Backup.STATUS_IN_PROGRESS)

    with pytest.raises(PreconditionError, match="multiple successors"):
        backup_controller.delete_backup(base.uuid)

    assert _status(db, base) == Backup.STATUS_COMPLETED


def test_a_member_of_a_group_backup_that_exists_is_refused(db, bucket):
    gb = _group_backup(db)
    member = _backup(db, bucket, "member", group_backup_id=gb.get_id())

    with pytest.raises(PreconditionError, match="group backup"):
        backup_controller.delete_backup(member.uuid)

    assert _status(db, member) == Backup.STATUS_COMPLETED


def test_a_member_whose_group_backup_is_gone_queues_a_delete_task(db, bucket):
    orphan = _backup(db, bucket, "orphan", group_backup_id=f"{CLUSTER}/{_uuid('gone')}")

    backup_controller.delete_backup(orphan.uuid)

    assert _status(db, orphan) == Backup.STATUS_COMPLETED
    assert _delete_tasks(db, orphan.uuid)


def test_retire_orphaned_member_backups_raises_on_a_forked_chain(db, bucket):
    """retire_orphaned_member_backups indexes a chain by prev_backup_id to
    find each orphan's successor; two backups sharing one predecessor must
    raise instead of silently keeping whichever the index construction saw
    last and losing the other."""
    orphan = _backup(db, bucket, "orphan", group_backup_id=f"{CLUSTER}/{_uuid('gone')}")
    _backup(db, bucket, "next_a", prev="orphan")
    _backup(db, bucket, "next_b", prev="orphan")

    with pytest.raises(PreconditionError, match="multiple successors"):
        group_backup.retire_orphaned_member_backups([LVOL], db=db)

    assert _status(db, orphan) == Backup.STATUS_COMPLETED


def test_an_unknown_backup_raises(db):
    with pytest.raises(KeyError):
        backup_controller.delete_backup(_uuid("nope"))


def test_a_chain_with_an_open_merge_task_refuses_a_delete_of_another_backup(db, bucket):
    """Two leaves of the same volume's chain, one already queued for a merge
    -- admission's "chain busy" gate is keyed by lvol_id (F2/F3), so claiming
    the other is refused by task admission itself, before either backup's
    status ever has to disagree."""
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")
    other_leaf = _backup(db, bucket, "other")
    ok, reason = backup_controller.trigger_merge(nxt, base)
    assert ok, reason

    with pytest.raises(PreconditionError, match="busy"):
        backup_controller.delete_backup(other_leaf.uuid)

    assert _status(db, other_leaf) == Backup.STATUS_COMPLETED


def test_a_backup_claimed_for_delete_refuses_a_concurrent_merge_claim(db, bucket):
    """claim_backup_chain_op's own-targets exemption must only cover a target
    already sitting in new_status (resuming the identical claim) -- not a
    target held in the OTHER status by a genuinely different operation."""
    base = _backup(db, bucket, "base")

    ok, reason = db.claim_backup_chain_op(LVOL, [base.uuid], Backup.STATUS_DELETING)
    assert ok, reason

    ok, reason = db.claim_backup_chain_op(LVOL, [base.uuid], Backup.STATUS_MERGING)

    assert not ok
    assert base.uuid in reason
    assert _status(db, base) == Backup.STATUS_DELETING


def test_claim_for_delete_refuses_when_a_live_dependent_exists(db, bucket):
    """F11 of docs/reviews/backup-deletion-review.md: "safe to delete
    directly instead of merging" means no live backup chains against this
    one. That precondition used to be checked once, in plain Python, by the
    caller, before the atomic claim -- leaving a window where a concurrent
    backup_lvol could pick the target as its new prev_backup_id between that
    read and the claim committing. The check now lives inside
    claim_backup_chain_op's own transaction, so claiming base here -- with
    "next" already chaining against it -- is refused outright, whether or
    not the caller thought to check first."""
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "next", prev="base")

    ok, reason = db.claim_backup_chain_op(LVOL, [base.uuid], Backup.STATUS_DELETING)

    assert not ok
    assert "live dependent" in reason
    assert _status(db, base) == Backup.STATUS_COMPLETED


def test_claim_for_delete_ignores_a_failed_dependent(db, bucket):
    """D7: a FAILED dependent was never restorable (no manifest, nothing to
    protect), so it does not block a direct delete."""
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "next", prev="base", status=Backup.STATUS_FAILED)

    ok, reason = db.claim_backup_chain_op(LVOL, [base.uuid], Backup.STATUS_DELETING)

    assert ok, reason


def test_claim_for_merge_is_unaffected_by_the_dependents_check(db, bucket):
    """A MERGING claim WANTS a live dependent -- it is the backup being
    folded into. The dependents check applies only to STATUS_DELETING."""
    base = _backup(db, bucket, "base")
    _backup(db, bucket, "next", prev="base")

    ok, reason = db.claim_backup_chain_op(LVOL, [base.uuid], Backup.STATUS_MERGING)

    assert ok, reason


def test_claim_failure_at_execution_reverts_every_marked_manifest_and_record(
        db, bucket, monkeypatch):
    """F3: _revert_backup_claim must revert the manifest, not just the FDB
    record. The claim itself now runs at the chain-delete task's own
    execution (F2/F3), not inside delete_backups: claiming [b1, b2], b1's
    manifest is marked DELETING, then marking b2's throws (simulating e.g.
    S3 throttling) -- both records must revert to COMPLETED AND b1's
    manifest, already flipped, must revert to ACTIVE rather than being left
    permanently disagreeing with the record. The task itself stays open
    (deferred, not destroyed) so a later pass retries."""
    from simplyblock_core.services import tasks_runner_backup as runner
    from simplyblock_core.services.task_runner_base import TaskDefer
    b1 = _backup(db, bucket, "b1")
    b2 = _backup(db, bucket, "b2", prev="b1")

    ok, err = backup_controller.delete_backups(LVOL)
    assert ok, err
    task = _chain_delete_tasks(db, LVOL)[0]

    real_mark_deleting = backup_controller.mark_manifest_deleting
    calls = []

    def flaky(backup):
        calls.append(backup.uuid)
        if len(calls) == 2:
            raise backup_manifest.ManifestError("simulated S3 throttling")
        real_mark_deleting(backup)
    monkeypatch.setattr(backup_controller, "mark_manifest_deleting", flaky)

    with pytest.raises(TaskDefer):
        runner._run_backup_delete_chain(task)

    assert _status(db, b1) == Backup.STATUS_COMPLETED
    assert _status(db, b2) == Backup.STATUS_COMPLETED
    assert _manifest_state(bucket, b1) == BackupManifestState.ACTIVE
    assert _manifest_state(bucket, b2) == BackupManifestState.ACTIVE
    assert _chain_delete_tasks(db, LVOL), "the task stays open to retry"


def test_a_chain_read_by_an_active_restore_defers_the_claim_at_execution(db, bucket):
    """Admission no longer checks for an active restore (F2/F3): that
    precondition is re-validated fresh by claim_backup_chain_op, as the first
    thing the queued task's own execution does. So delete_backup itself
    succeeds here (it only queues a task); the task defers once it actually
    tries to claim the chain."""
    from simplyblock_core.services import tasks_runner_backup as runner
    from simplyblock_core.services.task_runner_base import TaskDefer
    leaf = _backup(db, bucket, "leaf")
    _restore_task(db, [leaf.s3_id])

    assert backup_controller.delete_backup(leaf.uuid) is None

    task = _delete_tasks(db, leaf.uuid)[0]
    with pytest.raises(TaskDefer, match="restore"):
        runner._run_backup_delete(task)

    assert _status(db, leaf) == Backup.STATUS_COMPLETED


def test_a_chain_claimed_for_delete_refuses_a_new_restore_task(db, bucket):
    """create_backup_restore_task_if_chain_clear itself refuses once a sibling
    is DELETING -- the other half of the same guard. DELETING is reached at
    task execution time now (F2/F3); claimed directly here via
    _claim_for_delete rather than through delete_backup, which only queues
    the task that would otherwise do it."""
    leaf = _backup(db, bucket, "leaf")
    ok, err = backup_controller._claim_for_delete(LVOL, [leaf])
    assert ok, err

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


def test_delete_backups_refuses_while_a_merge_task_is_open(db, bucket):
    """The explicit MERGING-sibling check delete_backups used to run is gone
    (F2/F3): admission's "chain busy" gate, keyed by lvol_id, already refuses
    a chain-delete while any other delete/merge task is open against the
    same chain -- including one still a queued merge, not yet claimed."""
    base = _backup(db, bucket, "base")
    nxt = _backup(db, bucket, "next", prev="base")
    ok, reason = backup_controller.trigger_merge(nxt, base)
    assert ok, reason

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "busy" in err


def test_delete_backups_refuses_for_a_live_group_member(db, bucket):
    gb = _group_backup(db)
    _backup(db, bucket, "member", group_backup_id=gb.get_id())

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "group backup" in err


def test_delete_backups_queues_a_chain_delete_and_removes_failed_ones_directly(db, bucket):
    """The chain-delete task's claim -- COMPLETED to DELETING, manifest to
    DELETING -- runs at the task's own execution now (F2/F3), not here. A
    FAILED backup has no manifest or claim to wait on either way, so it is
    still removed directly and synchronously."""
    completed = _backup(db, bucket, "completed")
    failed = _backup(db, bucket, "failed", status=Backup.STATUS_FAILED)

    ok, err = backup_controller.delete_backups(LVOL)

    assert ok, err
    assert _status(db, completed) == Backup.STATUS_COMPLETED
    assert _manifest_state(bucket, completed) == BackupManifestState.ACTIVE
    assert _chain_delete_tasks(db, LVOL)
    assert not _exists(db, failed)


def test_delete_backups_called_again_before_its_task_runs_refuses_as_busy(db, bucket):
    """Admission is one atomic "chain busy" gate now (F2/F3), not a claim
    with idempotent resume: a second call before the first task has even run
    once is refused, not silently accepted."""
    _backup(db, bucket, "leaf")
    ok, err = backup_controller.delete_backups(LVOL)
    assert ok, err

    ok, err = backup_controller.delete_backups(LVOL)

    assert not ok
    assert "busy" in err


def _seed_data_objects(bucket, backup):
    """Fake data the data plane would have written under this backup's
    ``s3_id`` -- a metadata object (``mid=1``) and one data cluster
    (``mid=0``), per ``ManifestDataPlane.key_format``."""
    bucket.objects[f"{backup.s3_id}/1/0"] = b"metadata"
    bucket.objects[f"{backup.s3_id}/0/0"] = b"cluster-0"


class TestDeleteTask:
    """``FN_BACKUP_DELETE`` (``tasks_runner_backup._run_backup_delete``): the
    task ``backup_controller.delete_backup`` queues after claiming a backup
    that nothing chains against, which removes its S3 objects itself --
    there is no data-plane RPC for this and none is planned. A backup with a
    successor is folded into it instead, by the same task's MERGING branch
    (``_run_merge_fold``), which is where the data plane stays involved.
    """

    def _task(self, db, backup_id):
        return _delete_tasks(db, backup_id)[0]

    def test_deleting_removes_data_objects_manifest_and_record(self, db, bucket):
        from simplyblock_core.services import tasks_runner_backup as runner
        leaf = _backup(db, bucket, "leaf")
        _seed_data_objects(bucket, leaf)
        backup_controller.delete_backup(leaf.uuid)

        runner._run_backup_delete(self._task(db, leaf.uuid))

        assert not any(k.startswith(f"{leaf.s3_id}/") for k in bucket.objects)
        assert backup_manifest.manifest_key(UUID(leaf.uuid)) not in bucket.objects
        assert not _exists(db, leaf)

    def test_a_backup_no_longer_deleting_is_skipped(self, db, bucket):
        """A prior cycle of this same task (or, for a group member, a
        different caller) already retired or otherwise resolved it --
        resuming must not re-delete objects out from under whatever claimed
        it next. COMPLETED is not this case any more (F2/F3): the task
        itself re-claims a COMPLETED backup fresh on each cycle that reaches
        one, so this uses MERGED -- a status only something else (a
        different merge) could have put it in."""
        from simplyblock_core.services import tasks_runner_backup as runner
        leaf = _backup(db, bucket, "leaf")
        backup_controller.delete_backup(leaf.uuid)
        task = self._task(db, leaf.uuid)
        db.atomic_update(leaf, lambda b: setattr(b, "status", Backup.STATUS_MERGED))

        runner._run_backup_delete(task)

        assert _status(db, leaf) == Backup.STATUS_MERGED

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
            runner._run_backup_delete(self._task(db, leaf.uuid))

        assert _status(db, leaf) == Backup.STATUS_DELETING
        assert _manifest_state(bucket, leaf) == BackupManifestState.DELETING
        assert any(k.startswith(f"{leaf.s3_id}/") for k in bucket.objects)

    def test_a_crash_after_s3_delete_leaves_it_broken_and_self_heals(self, db, bucket, monkeypatch):
        """F1 of docs/reviews/backup-deletion-review.md: the delete loop
        removes S3 objects BEFORE touching manifests/records, so a crash in
        between must never be observable as "safely COMPLETED" -- the next
        pass (here, a second call with the manifest write failing once more)
        has to find BROKEN and retry the manifest/record cleanup, not
        re-attempt the S3 delete (already confirmed empty) nor claim success
        silently."""
        from simplyblock_core.services import tasks_runner_backup as runner
        from simplyblock_core.services.task_runner_base import TaskRetry
        leaf = _backup(db, bucket, "leaf")
        _seed_data_objects(bucket, leaf)
        backup_controller.delete_backup(leaf.uuid)
        task = self._task(db, leaf.uuid)

        # Simulate the crash: S3 data removed, but the manifest/record
        # cleanup that follows never ran (the process died first).
        real_delete_manifest = backup_controller.delete_manifest

        def fail_once(backup):
            monkeypatch.setattr(backup_controller, "delete_manifest", real_delete_manifest)
            raise backup_manifest.ManifestError("simulated crash")
        monkeypatch.setattr(backup_controller, "delete_manifest", fail_once)

        with pytest.raises(TaskRetry):
            runner._run_backup_delete(task)

        assert _status(db, leaf) == Backup.STATUS_BROKEN
        assert _manifest_state(bucket, leaf) == BackupManifestState.BROKEN
        assert not any(k.startswith(f"{leaf.s3_id}/") for k in bucket.objects), \
            "S3 data must already be gone -- BROKEN means confirmed deleted"

        # Re-drive: must resume at the manifest/record cleanup, not re-delete.
        runner._run_backup_delete(self._task(db, leaf.uuid))

        assert not _exists(db, leaf)
