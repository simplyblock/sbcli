"""
tasks_runner_backup.py - background task runner for S3 backup operations.

Handles:
  - FN_BACKUP: perform an S3 backup from a snapshot
  - FN_BACKUP_RESTORE: restore a backup chain into a new lvol
  - FN_BACKUP_DELETE: delete (or fold into its successor) ONE backup that has
    been claimed for removal -- the runner decides which, at execution time,
    from the backup's current status and chain (see ``_run_backup_delete``;
    D5/D8 of docs/reviews/backup-deletion-review.md). Replaces what used to be
    two task types, FN_BACKUP_DELETE (a list of ids) and FN_BACKUP_MERGE.
  - FN_BACKUP_DELETE_CHAIN: delete every backup of one volume, always directly
    (never a merge).

FN_BACKUP, FN_BACKUP_RESTORE and the fold-into-successor path of
FN_BACKUP_DELETE are multi-cycle: a task issues its RPC, defers, and polls the
data plane's transfer state on later cycles until it reaches a terminal
state. The direct-delete paths of FN_BACKUP_DELETE/FN_BACKUP_DELETE_CHAIN are
single-cycle: a plain S3 delete has nothing to poll.
"""
import errno
import time

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import device as backup_device
from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.controllers.backup.chain import SuccessorIndex
from simplyblock_core.controllers.backup.manifest import ManifestError
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException, RPCRemoteError
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskRetry,
    checkpoint,
    serve,
    set_result,
)

logger = utils.get_logger(__name__)

db = db_controller.DBController()

# Time-based backstop for a task that is stuck but not erroring.
_DEFAULT_BACKUP_TIMEOUT_SEC = 14400


def _online_node(node_id):
    """The node the task's RPCs go to, or a signal to stop/wait."""
    try:
        snode = db.get_storage_node_by_id(node_id)
    except KeyError:
        raise TaskAbort(f"Node {node_id} not found")

    if snode.status != StorageNode.STATUS_ONLINE:
        raise TaskRetry(f"Node {snode.status}, retrying")
    return snode


def _defer_if_busy(e: RPCException) -> None:
    """Separate S3-device contention from a genuine transfer RPC failure.

    EBUSY means the target S3 device already has another transfer in flight:
    expected and self-resolving, so it defers rather than spending retry budget
    the way every other RPC error does. Returns for anything else, leaving the
    caller's own error handling to run.
    """
    if isinstance(e, RPCRemoteError) and e.code == -errno.EBUSY:
        raise TaskDefer("S3 device busy with another transfer, retrying")


def _transfer_state(rpc_client, bdev_name):
    try:
        stat = rpc_client.bdev_lvol_transfer_stat(bdev_name)
    except RPCException:
        raise TaskRetry("transfer stat RPC failed, retrying")

    if not stat or not isinstance(stat, dict):
        raise TaskRetry("unexpected transfer stat response, retrying")
    return stat.get("transfer_state", "")


def _run_backup(task):
    backup_id = task.function_params.get("backup_id")
    if not backup_id:
        raise TaskAbort("Missing backup_id")

    try:
        backup = db.get_backup_by_id(backup_id)
    except KeyError:
        raise TaskAbort(f"Backup {backup_id} not found")

    if backup.status not in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS):
        raise TaskAbort(f"Backup is already {backup.status}")

    snode = _online_node(backup.node_id)
    rpc_client = snode.rpc_client(timeout=30)

    try:
        snapshot = db.get_snapshot_by_id(backup.snapshot_id)
    except KeyError:
        raise TaskAbort(f"Snapshot {backup.snapshot_id} not found")

    snap_bdev_name = snapshot.snap_bdev
    if not snap_bdev_name:
        snap_bdev_name = f"{snapshot.lvol.lvs_name}/{snapshot.snap_name}"

    if backup.status == Backup.STATUS_PENDING:
        try:
            ret = rpc_client.bdev_lvol_s3_backup(
                backup.s3_id, [snap_bdev_name],
                backup_device.primary_s3_bdev_name(snode), cluster_batch=16)
        except RPCException as e:
            _defer_if_busy(e)
            raise TaskAbort(f"RPC error: {e}")
        if not ret:
            raise TaskAbort("bdev_lvol_s3_backup RPC failed")

        backup.status = Backup.STATUS_IN_PROGRESS
        backup.write_to_db()
        # Give the data plane time to start the transfer before polling
        raise TaskDefer("Backup in progress")

    state = _transfer_state(rpc_client, snap_bdev_name)
    if state == "Done":
        backup.completed_at = int(time.time())

        # Publish the manifest BEFORE marking the backup completed, so that
        # COMPLETED implies "identifiable from the bucket alone". Data with
        # no manifest is data nobody can attribute to a volume later, so a
        # manifest failure fails the backup rather than leaving that behind.
        try:
            backup_controller.write_manifest(backup)
        except (ManifestError, PreconditionError) as e:
            raise TaskAbort(f"Failed to publish backup manifest: {e}")

        backup.status = Backup.STATUS_COMPLETED
        backup.write_to_db()
        backup_events.backup_completed(backup.cluster_id, backup.node_id, backup)
        set_result(task, "Backup completed")
        return

    if state in ("Failed", "No process") and backup.status == Backup.STATUS_IN_PROGRESS:
        # Both mean no transfer is running for this bdev and it did not finish:
        # "No process" because the backup died outright (e.g. an SPDK crash
        # wiped the in-flight transfer); "Failed" because the data plane gave
        # up after an I/O error (e.g. the S3 endpoint was briefly unreachable --
        # an OOM-killed MinIO pod is back in a couple of seconds). A plain
        # backup upload is additive, so re-issuing it onto the same s3_id is
        # safe: it just re-uploads and overwrites.
        #
        # Re-issue by resetting to PENDING, and COUNT it as a retry so the
        # task's own max_retry ceiling (BACKUP_TASK_MAX_RETRIES) can stop a
        # backup that keeps failing -- the driver already owns that count,
        # shared across every retryable cause for this task; nothing here
        # keeps one of its own.
        #
        # NOTE: this treats both as a failure. It relies on a healthy
        # in-progress backup NOT sitting in either state; if the data plane
        # ever reports one for a running backup, this would fail it
        # prematurely and completion needs another signal.
        backup.status = Backup.STATUS_PENDING
        backup.write_to_db()
        raise TaskRetry(f"{state}, retrying backup start")

    raise TaskDefer("Backup in progress")


def _set_lvol_online(task):
    """Mark restored lvol as online after successful data recovery."""
    lvol_id = task.function_params.get("lvol_id")
    if not lvol_id:
        return
    try:
        lvol = db.get_lvol_by_id(lvol_id)
        if lvol.status == LVol.STATUS_RESTORING:
            lvol.status = LVol.STATUS_ONLINE
            lvol.write_to_db()
            logger.info(f"Restored lvol {lvol_id} is now online")
    except KeyError:
        logger.warning(f"Restored lvol {lvol_id} not found in DB")


def _set_lvol_restore_failed(task, reason):
    """Mark restored lvol as restore_failed after exhausting all retries."""
    lvol_id = task.function_params.get("lvol_id")
    if not lvol_id:
        return
    try:
        lvol = db.get_lvol_by_id(lvol_id)
        if lvol.status == LVol.STATUS_RESTORING:
            lvol.status = LVol.STATUS_RESTORE_FAILED
            lvol.write_to_db()
            logger.error(f"Restore of lvol {lvol_id} failed: {reason}")
    except KeyError:
        logger.warning(f"Restored lvol {lvol_id} not found in DB")


def _restore_s3_bdev(task, snode) -> str:
    """The S3 device this restore reads from.

    A foreign bucket gets its own device, named from the backup id so a retry
    re-derives the same name instead of leaking one per attempt. Otherwise the
    node's own backup device already points at the right bucket.
    """
    if task.function_params.get("s3_config"):
        return backup_device.restore_s3_bdev_name(
            task.function_params["backup_id"])
    return backup_device.primary_s3_bdev_name(snode)


def _ensure_restore_s3_bdev(task, snode) -> None:
    """Create the foreign-bucket device if this restore needs one.

    Idempotent, and re-run on every attempt rather than once: a node restart
    mid-restore takes the device with it, and the runner is the only component
    positioned to put it back.
    """
    config = task.function_params.get("s3_config")
    if not config:
        return

    backup_device.create_restore_s3_bdev(
        snode, BackupConfig.model_validate(config), _restore_s3_bdev(task, snode))


def _release_restore_s3_bdev(task, snode) -> None:
    """Delete the foreign-bucket device and forget its credentials.

    Called from ``finalize_resource``, so it covers every terminal path at once
    — success, abort, timeout, retry ceiling, cancellation. The credentials are
    scrubbed from the task record because a task is retained for weeks after it
    finishes, and there is no reason for another cluster's S3 keys to outlive
    the restore that needed them.
    """
    if not task.function_params.get("s3_config"):
        return

    if snode is not None:
        backup_device.delete_restore_s3_bdev(snode, _restore_s3_bdev(task, snode))

    task.function_params["s3_config"] = None


def _scrub_s3_config(task) -> None:
    task.function_params = dict(task.function_params, s3_config=None)


def _run_restore(task):
    backup_id = task.function_params.get("backup_id")
    lvol_name = task.function_params.get("lvol_name")
    chain_ids = task.function_params.get("chain_ids", [])
    node_id = task.node_id

    snode = _online_node(node_id)
    rpc_client = snode.rpc_client(timeout=30)

    # Check that the target lvol still exists in DB before doing any RPC work
    lvol_id = task.function_params.get("lvol_id")
    if lvol_id:
        try:
            if db.get_lvol_by_id(lvol_id).status == LVol.STATUS_IN_DELETION:
                raise TaskAbort(f"Restore target {lvol_id} has been deleted")
        except KeyError:
            raise TaskAbort(f"Restore target {lvol_id} no longer exists")

    if not task.function_params.get("recovery_started", False):
        # The device is established here, not when the restore was requested:
        # only now is the node known (add_lvol_ha chooses it), and only the
        # runner can put it back after a node restart wipes it mid-restore.
        try:
            _ensure_restore_s3_bdev(task, snode)
        except (RuntimeError, ValueError) as e:
            raise TaskRetry(f"Could not attach the backup's bucket: {e}")

        try:
            ret = rpc_client.bdev_lvol_s3_recovery(
                lvol_name, chain_ids, cluster_batch=16,
                s3_bdev=_restore_s3_bdev(task, snode))
        except RPCException as e:
            _defer_if_busy(e)
            raise TaskRetry(f"RPC error: {e}")
        if not ret:
            raise TaskRetry("bdev_lvol_s3_recovery RPC failed")

        # Don't re-issue the RPC on subsequent polls, and give the data plane
        # time to start the transfer before the first one.
        checkpoint(task, recovery_started=True)
        raise TaskDefer("Restore started")

    state = _transfer_state(rpc_client, lvol_name)
    if state == "Done":
        _set_lvol_online(task)
        try:
            backup = db.get_backup_by_id(backup_id)
            backup_events.backup_restore_completed(
                task.cluster_id, node_id, backup, lvol_name)
        except KeyError:
            logger.warning(
                f"Backup {backup_id} no longer exists, "
                f"skipping restore-completed event for {lvol_name}")
        set_result(task, f"Restore completed: {lvol_name}")
        return

    if state == "Failed":
        fail_count = task.function_params.get("fail_count", 0) + 1
        checkpoint(task, fail_count=fail_count)
        reason = f"S3 transfer failed on data plane (attempt {fail_count})"
        if fail_count < 3:
            raise TaskRetry(reason)

        _set_lvol_restore_failed(task, reason)
        try:
            backup = db.get_backup_by_id(backup_id)
            backup_events.backup_restore_failed(
                task.cluster_id, node_id, backup, lvol_name, reason)
        except KeyError:
            logger.warning(
                "Backup %s not found in DB; restore-failed event skipped for lvol %s",
                backup_id, lvol_name)
        raise TaskAbort(reason)

    if state == "No process":
        checkpoint(task, recovery_started=False)
        raise TaskDefer("No process, restarting recovery")

    raise TaskDefer("Restore in progress")


def _retire_backup(backup: Backup) -> None:
    """The shared final step for a BROKEN backup -- a direct delete whose S3
    data is confirmed gone: (re-)mark its manifest BROKEN (idempotent, so a
    crash between the record write and this self-heals on retry), remove the
    manifest, clean up its KMS keys and record, and emit the deleted event.

    Not used for a successful merge's old backup, which goes to MERGED
    instead and is retired by ``_run_merge_fold`` directly -- its own
    manifest work (survivor rewritten, old one deleted) differs enough that
    sharing this helper would not simplify it.
    """
    try:
        backup_controller.mark_manifest_broken(backup)
    except (ManifestError, PreconditionError, KeyError) as e:
        raise TaskRetry(f"Data retired, manifest mark-broken failed: {e}")

    try:
        backup_controller.delete_manifest(backup)
    except (ManifestError, PreconditionError) as e:
        # The data is already gone and is not reversible, so the task cannot
        # be aborted here -- retry the manifest/record cleanup.
        raise TaskRetry(f"Delete done, manifest cleanup failed: {e}")

    backup_controller.cleanup_backup_kms_keys([backup])
    backup_events.backup_deleted(backup.cluster_id, backup.node_id, backup)
    backup.remove(db.kv_store)


def _run_merge_fold(task, old_backup: Backup) -> None:
    """The fold-into-successor path of ``_run_backup_delete``: merge
    ``old_backup`` into its live successor via the data plane's
    ``bdev_lvol_s3_merge``, mirroring what used to be the standalone
    FN_BACKUP_MERGE task.

    The successor is re-resolved here, fresh, rather than trusted from
    whatever ``trigger_merge`` saw when it queued this task (D5/D8): nothing
    can actually make it disagree (claim_backup_chain_op's mutual exclusion
    keeps a successor from being independently claimed while ``old_backup``
    is MERGING), but re-deriving it is the one choke point the review settled
    on, shared with every other "does something live chain against this
    backup" decision.
    """
    successor = SuccessorIndex.of_lvol(
        db.get_backups_by_lvol_id(old_backup.lvol_id)).live_successor_of(old_backup.uuid)
    if successor is None:
        raise TaskAbort(
            f"backup {old_backup.uuid} is MERGING but nothing live chains "
            "against it any more")
    keep_backup = successor

    snode = _online_node(keep_backup.node_id)
    rpc_client = snode.rpc_client(timeout=30)

    if not task.function_params.get("merge_started", False):
        try:
            ret = rpc_client.bdev_lvol_s3_merge(
                keep_backup.s3_id, old_backup.s3_id, cluster_batch=16,
                s3_bdev=backup_device.primary_s3_bdev_name(snode),
                lvs_name=snode.lvstore)
        except RPCException as e:
            _defer_if_busy(e)
            raise TaskRetry(f"RPC error: {e}")
        if not ret:
            raise TaskRetry("bdev_lvol_s3_merge RPC failed")

        checkpoint(task, merge_started=True)
        # Give the data plane time to complete the merge before finalizing.
        raise TaskDefer("Merge started")

    # The merge RPC only queues the merge on the data plane; the actual work
    # runs asynchronously afterward. Poll bdev_lvol_s3_merge_stat (keyed by
    # the same s3_id/old_s3_id pair, since a merge task has no lvol) instead
    # of assuming success.
    try:
        stat = rpc_client.bdev_lvol_s3_merge_stat(keep_backup.s3_id, old_backup.s3_id)
    except RPCException as e:
        raise TaskRetry(f"merge stat RPC failed: {e}")

    if not stat or not isinstance(stat, dict):
        raise TaskRetry("merge stat returned no usable state")

    state = stat.get("transfer_state", "")
    if state == "Done":
        # Finalize: update the chain links and retire the old backup.
        keep_backup.prev_backup_id = old_backup.prev_backup_id
        keep_backup.status = Backup.STATUS_COMPLETED
        keep_backup.write_to_db()

        old_backup.status = Backup.STATUS_MERGED
        old_backup.write_to_db()

        # Two objects, and only two: the survivor's manifest, whose prev_backup_id
        # just changed, and the merged-away one, which describes keys the data plane
        # has unmapped. Every descendant's manifest stays valid because none of them
        # names anything but its own immediate predecessor -- the chain is walked at
        # read time rather than stored, precisely so a merge does not have to rewrite
        # the whole line of descent and cannot half-succeed at it.
        try:
            backup_controller.write_manifest(keep_backup)
            backup_controller.delete_manifest(old_backup)
        except (ManifestError, PreconditionError) as e:
            # The S3 merge already happened and is not reversible, so the task
            # cannot be aborted here -- retry the manifest work instead.
            raise TaskRetry(f"Merge done, manifest update failed: {e}")

        set_result(task, "Merge completed")
        logger.info(f"Merge completed: {old_backup.uuid} merged into {keep_backup.uuid}")
        return

    if state == "Failed":
        # Terminal, not retried: XFER_STATE_FAILED can be reached after the
        # data plane has already started deleting old_backup's S3 objects
        # (its DELETE state runs near the end of the merge), so retrying the
        # identical merge isn't known to be safe from here, and reverting to
        # COMPLETED would claim data that may already be gone is still there
        # (D4). merge_started is always True by the time this state is seen
        # (it gates every call that could reach the stat poll above), so the
        # data plane may have already removed old_backup's objects -- mark it
        # BROKEN, matching the direct-delete path's own confirmed-gone state,
        # and leave it for a later delete_backup/delete_backups to retire.
        old_backup.status = Backup.STATUS_BROKEN
        old_backup.write_to_db()
        try:
            backup_controller.mark_manifest_broken(old_backup)
        except (ManifestError, PreconditionError) as e:
            logger.error(
                "Backup %s marked BROKEN after a failed merge, but its "
                "manifest could not be marked broken: %s", old_backup.uuid, e)
        raise TaskAbort("Merge failed on data plane")

    if state == "No process":
        # Never started, or its result was already swept — re-issue.
        checkpoint(task, merge_started=False)
        raise TaskRetry("merge not running on the data plane; re-issuing")

    # "In progress" — still running, come back next pass.
    raise TaskDefer("Merge in progress")


def _set_backup_status(backup: Backup, status: str) -> Backup | None:
    """Compare-and-set a claimed backup's status, returning the freshly
    committed record.

    Use this instead of ``backup.status = x; backup.write_to_db()`` anywhere
    a backup already claimed by a delete/delete-chain task has its status
    advanced or reverted: a task superseded and canceled mid-run (see
    ``tasks_controller.add_backup_delete_chain_task``) can still be writing
    the same record when the superseding task's own revert/retire runs --
    ``atomic_update`` is the one primitive that conflicts on that instead of
    silently clobbering it (closes F4 of
    docs/reviews/backup-deletion-review-round2.md; see
    simplyblock_core/AGENTS.md on ``atomic_update`` as the only safe
    compare-and-set for state touched by more than one task/process).
    """
    return db.atomic_update(backup, lambda b: setattr(b, "status", status))


def _delete_one_backup(backup: Backup) -> Backup | None:
    """The shared step that drives one already-claimed backup (DELETING or
    BROKEN) the rest of the way to BROKEN: remove its S3 data if not already
    gone, then CAS the status.

    Shared by ``_run_backup_delete`` (its one target) and
    ``_run_backup_delete_chain`` (once per target), so a future hardening of
    this step cannot land on only one of the two near-duplicate paths (F10 of
    docs/reviews/backup-deletion-review-round3.md) -- the chain-delete task
    stays structurally different from "a single-delete given a longer id
    list" (it re-derives its target set from ``lvol_id`` alone, which is what
    makes "every backup of the volume" true by construction rather than
    something re-verified at execution time), but the per-backup delete
    sequence itself is extracted so it cannot drift between the two.

    Returns the freshly BROKEN record, or ``None`` if a concurrent writer
    already moved it past DELETING (nothing left for this call to do).

    Raises:
        TaskDefer: a live dependent now exists -- can only trip on a bug in
            the claim that marked this backup DELETING, which already refused
            in that case; deferring rather than destroying data a live
            successor needs.
        TaskRetry: the S3 data delete failed.
    """
    if backup.status != Backup.STATUS_DELETING:
        return backup   # already BROKEN: nothing left to delete here

    # Re-assert: a live dependent must still be absent at execution time, not
    # just at claim time (claim_backup_chain_op's own in-transaction check
    # already refuses a claim where one exists, so this can only trip on a
    # bug there).
    successor = SuccessorIndex.of_lvol(
        db.get_backups_by_lvol_id(backup.lvol_id)).live_successor_of(backup.uuid)
    if successor is not None:
        raise TaskDefer(
            f"backup {backup.uuid} now has a live dependent "
            f"{successor.uuid}; deferring")

    try:
        backup_controller._delete_backup_data(backup)
    except (ManifestError, PreconditionError) as e:
        raise TaskRetry(f"S3 data delete failed: {e}")

    return _set_backup_status(backup, Backup.STATUS_BROKEN)


def _run_backup_delete(task):
    """Delete, or fold into its successor, the backup named by
    ``function_params["backup_id"]``.

    A COMPLETED backup has not been claimed yet: this is the first execution
    of this task to reach it, so the merge-vs-delete decision and the claim
    itself (``_claim_for_merge``/``_claim_for_delete``) both happen right
    here, fresh, rather than trusting whatever was true when
    ``backup_controller`` queued this task (F2/F3 of
    docs/reviews/backup-deletion-review-round3.md): a live, completed
    successor means a fold-in (``_run_merge_fold``); otherwise a direct S3
    delete, nothing chains against it. Once claimed, later executions (and a
    resumed BROKEN one) skip straight to driving whichever path the claim
    settled on: MERGING to ``_run_merge_fold``; DELETING/BROKEN to
    ``_delete_one_backup`` and then retirement.

    No data-plane RPC for the direct-delete path: there is no
    ``bdev_lvol_s3_delete`` and none is planned, so the control plane removes
    the objects itself, the same way it removes a manifest.

    A backup already gone, or in neither status this handler acts on, is
    skipped: a prior cycle of this same task (or the group-backup path, or a
    covering FN_BACKUP_DELETE_CHAIN) already retired it.
    """
    backup_id = task.function_params.get("backup_id")
    if not backup_id:
        raise TaskAbort("Missing backup_id")

    try:
        backup = db.get_backup_by_id(backup_id)
    except KeyError:
        set_result(task, "Nothing left to delete")
        return

    if backup.status == Backup.STATUS_COMPLETED:
        successor = SuccessorIndex.of_lvol(
            db.get_backups_by_lvol_id(backup.lvol_id)).live_successor_of(backup.uuid)
        if successor is not None and successor.status != Backup.STATUS_COMPLETED:
            raise TaskDefer(
                f"backup {backup.uuid}'s successor {successor.uuid} is not "
                "completed yet; deferring")

        if successor is not None:
            ok, reason = backup_controller._claim_for_merge(successor, backup)
        else:
            ok, reason = backup_controller._claim_for_delete(backup.lvol_id, [backup])
        if not ok:
            raise TaskDefer(f"cannot claim backup {backup.uuid} yet: {reason}")
        backup = db.get_backup_by_id(backup_id)

    if backup.status == Backup.STATUS_MERGING:
        _run_merge_fold(task, backup)
        return

    if backup.status not in (Backup.STATUS_DELETING, Backup.STATUS_BROKEN):
        set_result(task, "Nothing left to delete")
        return

    fresh = _delete_one_backup(backup)
    if fresh is None:
        set_result(task, "Nothing left to delete")
        return

    _retire_backup(fresh)
    set_result(task, "Backup deleted")
    logger.info(f"Deleted backup: {backup_id}")


def _run_backup_delete_chain(task):
    """Delete every backup of ``function_params["lvol_id"]``: always a direct
    S3 delete, never a merge -- ``backup_controller.delete_backups``' own
    invariant is that nothing outside the call survives to chain against any
    of its targets.

    Nothing has been claimed yet the first time this reaches a COMPLETED
    backup of the volume: claims the whole chain now, fresh -- re-validating
    every precondition in the same transaction, rather than trusting
    whatever was true when ``delete_backups`` queued this task (F2/F3 of
    docs/reviews/backup-deletion-review-round3.md). Already-claimed siblings
    (an earlier cycle of this same task) ride along unchanged:
    ``claim_backup_chain_op`` is idempotent on a target already in the
    claimed status.

    Re-reads the volume's backups at execution time rather than carrying an
    explicit id list, so a retry always acts on the current claimed set.
    """
    lvol_id = task.function_params.get("lvol_id")
    if not lvol_id:
        raise TaskAbort("Missing lvol_id")

    backups = db.get_backups_by_lvol_id(lvol_id)
    claimable = [b for b in backups
                if b.status in (Backup.STATUS_COMPLETED, Backup.STATUS_DELETING,
                                Backup.STATUS_BROKEN)]
    if not claimable:
        set_result(task, "Nothing left to delete")
        return

    if any(b.status == Backup.STATUS_COMPLETED for b in claimable):
        ok, reason = backup_controller._claim_for_delete(lvol_id, claimable)
        if not ok:
            raise TaskDefer(f"cannot claim backups of volume {lvol_id} yet: {reason}")
        backups = db.get_backups_by_lvol_id(lvol_id)

    targets = [b for b in backups
              if b.status in (Backup.STATUS_DELETING, Backup.STATUS_BROKEN)]
    if not targets:
        set_result(task, "Nothing left to delete")
        return

    broken = [b for b in (_delete_one_backup(backup) for backup in targets) if b is not None]
    for backup in broken:
        _retire_backup(backup)

    set_result(task, "Backups deleted")
    logger.info(f"Deleted backups: {', '.join(b.uuid for b in targets)}")


def _run_group_backup_commit(task):
    """Commit a consistency-group backup once its members are done.

    Does no data-plane RPC. It watches the member backups -- which the FN_BACKUP
    runner drives independently -- and publishes the group manifest when every
    one has completed, the all-or-nothing barrier. It defers while
    any member is still in flight, aborts if a member failed, and treats a
    manifest-publish failure as retryable S3 trouble rather than a group failure.
    """
    group_backup_id = task.function_params.get("group_backup_id")
    if not group_backup_id:
        raise TaskAbort("Missing group_backup_id")

    try:
        state = group_backup.try_commit_group_backup(group_backup_id)
    except (ManifestError, PreconditionError) as e:
        raise TaskRetry(f"Could not publish group manifest: {e}")

    if state == group_backup.CommitState.PENDING:
        raise TaskDefer("Group backup members still in flight")
    if state == group_backup.CommitState.FAILED:
        raise TaskAbort("Group backup failed: a member backup did not complete")

    set_result(task, "Group backup completed")


def _run_group_backup_delete(task):
    """Retire a deleting group backup's members, then remove its manifest and
    record.

    Does no data-plane RPC of its own: a member's merge is its own
    FN_BACKUP_DELETE task, which this waits on. Defers while a member is still
    being retired, and treats a manifest or member failure as retryable; a task
    that ends unfinished settles the group backup in :func:`finalize_resource`.
    """
    group_backup_id = task.function_params.get("group_backup_id")
    if not group_backup_id:
        raise TaskAbort("Missing group_backup_id")

    try:
        state = group_backup.advance_group_backup_delete(group_backup_id)
    except (ManifestError, PreconditionError) as e:
        raise TaskRetry(f"Could not finish deleting the group backup: {e}")

    if state == group_backup.DeleteState.PENDING:
        raise TaskDefer("Group backup members still being retired")

    set_result(task, "Group backup deleted")


_HANDLERS = {
    JobSchedule.FN_BACKUP: _run_backup,
    JobSchedule.FN_BACKUP_RESTORE: _run_restore,
    JobSchedule.FN_BACKUP_DELETE: _run_backup_delete,
    JobSchedule.FN_BACKUP_DELETE_CHAIN: _run_backup_delete_chain,
    JobSchedule.FN_GROUP_BACKUP_COMMIT: _run_group_backup_commit,
    JobSchedule.FN_GROUP_BACKUP_DELETE: _run_group_backup_delete,
}


def process_task(task):
    cluster = db.get_cluster_by_id(task.cluster_id)
    backup_timeout_sec = getattr(cluster, 'backup_timeout_seconds', 0) or _DEFAULT_BACKUP_TIMEOUT_SEC
    elapsed = int(time.time()) - task.date if task.date else 0
    if elapsed > backup_timeout_sec:
        raise TaskAbort(f"timeout after {elapsed}s")

    _HANDLERS[task.function_name](task)


def _is_eligible(task, cluster):
    """Left untouched while the cluster is in_activation, or while a site
    activation holds the LVS of the task's node."""
    return (cluster.status != Cluster.STATUS_IN_ACTIVATION
            and not tasks_controller.site_activation_holds(cluster, task.node_id))


def finalize_resource(task):
    """Release the backup/restore/merge the task was driving, once it is over.

    Reached on every terminal path, so it is written to be a no-op when the
    handler completed the resource itself and to only act when the task ended
    with the resource still in flight — a timeout, the retry ceiling, an abort
    or a cancellation, none of which the handler sees.
    """
    reason = task.function_result

    if task.function_name == JobSchedule.FN_BACKUP:
        backup_id = task.function_params.get("backup_id")
        if not backup_id:
            return
        try:
            backup = db.get_backup_by_id(backup_id)
        except KeyError:
            return
        if backup.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS):
            backup.status = Backup.STATUS_FAILED
            backup.error_message = reason
            backup.write_to_db()
            backup_events.backup_failed(backup.cluster_id, backup.node_id, backup)

    elif task.function_name == JobSchedule.FN_BACKUP_RESTORE:
        _set_lvol_restore_failed(task, reason)
        if task.function_params.get("s3_config"):
            try:
                snode = db.get_storage_node_by_id(task.node_id)
            except KeyError:
                snode = None
            _release_restore_s3_bdev(task, snode)
            # Not `drop_params`: this runs after the terminal write, and a task
            # already DONE is exactly what the handler-facing helpers refuse to
            # touch. The scrub still has to land, so it commits directly.
            db.atomic_update(task, _scrub_s3_config)

    elif task.function_name == JobSchedule.FN_GROUP_BACKUP_COMMIT:
        group_backup_id = task.function_params.get("group_backup_id")
        if not group_backup_id:
            return
        try:
            gb = db.get_group_backup_by_id(group_backup_id)
        except KeyError:
            return
        # The commit task ended (timeout, retry ceiling, abort, cancellation)
        # while the group was still unresolved: fail it as a unit. A member
        # backup that is still running is the FN_BACKUP runner's to terminate;
        # this only closes the group record so nothing reads it as in progress
        # forever.
        if gb.status in (GroupBackup.STATUS_PENDING, GroupBackup.STATUS_IN_PROGRESS):
            gb.status = GroupBackup.STATUS_FAILED
            gb.error_message = reason
            gb.write_to_db()

    elif task.function_name == JobSchedule.FN_BACKUP_DELETE:
        # The delete/merge did not finish (timeout, retry ceiling, abort,
        # cancellation). What to do depends on which path it was on and how
        # far it got -- BROKEN is left exactly as _run_backup_delete would
        # (S3 data confirmed gone; do not revert). DELETING had its S3
        # objects left untouched (the direct-delete path only reaches BROKEN
        # once it has confirmed removal), so it reverts to COMPLETED, record
        # and manifest both. MERGING is the D4/F5 case: before
        # ``merge_started`` nothing reached the data plane and reverting to
        # COMPLETED is still safe; once it is set the data plane may already
        # have removed the old backup's objects (its DELETE state runs near
        # the end of a merge), so it is marked BROKEN instead, never reverted.
        backup_id = task.function_params.get("backup_id")
        if not backup_id:
            return
        try:
            backup = db.get_backup_by_id(backup_id)
        except KeyError:
            return

        if backup.status == Backup.STATUS_BROKEN:
            return

        if backup.status == Backup.STATUS_MERGING:
            if task.function_params.get("merge_started", False):
                _set_backup_status(backup, Backup.STATUS_BROKEN)
                try:
                    backup_controller.mark_manifest_broken(backup)
                except (ManifestError, PreconditionError, KeyError) as e:
                    logger.error(
                        f"Backup {backup.uuid} marked BROKEN after an "
                        f"unfinished merge, but its manifest could not be "
                        f"marked broken: {e}")
            else:
                _set_backup_status(backup, Backup.STATUS_COMPLETED)
            return

        if backup.status != Backup.STATUS_DELETING:
            return
        try:
            backup_controller.revert_manifest_deleting(backup)
        except (ManifestError, PreconditionError, KeyError) as e:
            logger.error(
                f"Backup {backup.uuid} reverted to COMPLETED, but its "
                f"manifest could not be marked active and still says "
                f"deleting: {e}")
        _set_backup_status(backup, Backup.STATUS_COMPLETED)

    elif task.function_name == JobSchedule.FN_BACKUP_DELETE_CHAIN:
        # Same DELETING -> revert / BROKEN -> leave shape as FN_BACKUP_DELETE,
        # applied to every backup of the volume this chain-delete claimed.
        lvol_id = task.function_params.get("lvol_id")
        if not lvol_id:
            return
        for backup in db.get_backups_by_lvol_id(lvol_id):
            if backup.status == Backup.STATUS_BROKEN:
                continue
            if backup.status != Backup.STATUS_DELETING:
                continue
            try:
                backup_controller.revert_manifest_deleting(backup)
            except (ManifestError, PreconditionError, KeyError) as e:
                logger.error(
                    f"Backup {backup.uuid} reverted to COMPLETED, but its "
                    f"manifest could not be marked active and still says "
                    f"deleting: {e}")
            _set_backup_status(backup, Backup.STATUS_COMPLETED)

    elif task.function_name == JobSchedule.FN_GROUP_BACKUP_DELETE:
        group_backup_id = task.function_params.get("group_backup_id")
        if not group_backup_id:
            return
        # Restored if no member was touched, marked broken if one was; a no-op
        # once the delete finished and the record is gone.
        group_backup.abandon_group_backup_delete(group_backup_id, reason)


SPEC = RunnerSpec(
    name="tasks-runner-backup",
    function_names=list(_HANDLERS),
    handler=process_task,
    on_finish=finalize_resource,
    is_eligible=_is_eligible,
)


#: The task types this runner owns. The cluster's task list holds every other
#: runner's tasks too, and process_task terminates what it is handed (timeout,
#: max retry): without this filter it closed snapshot-replication tasks with
#: "max retry reached (8/8)" and would end any other task older than the
#: backup timeout (2026-09-29). The one definition lives on JobSchedule
#: itself (JobSchedule.BACKUP_FUNCTIONS) so this and
#: cluster_expansion.preconditions.EXPANSION_BLOCKING_TASK_FNS can't drift
#: apart the way EXPANSION_BLOCKING_TASK_FNS once did (F4 of
#: docs/reviews/backup-deletion-review.md).
BACKUP_FUNCTIONS = JobSchedule.BACKUP_FUNCTIONS


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
