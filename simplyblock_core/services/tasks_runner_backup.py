"""
tasks_runner_backup.py - background task runner for S3 backup operations.

Handles three task types:
  - FN_BACKUP: perform an S3 backup from a snapshot
  - FN_BACKUP_RESTORE: restore a backup chain into a new lvol
  - FN_BACKUP_MERGE: merge two backups to shorten the chain

All three are multi-cycle: a task issues its RPC, defers, and polls the data
plane's transfer state on later cycles until it reaches a terminal state.
"""
import errno
import time

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import backup_events
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import device as backup_device
from simplyblock_core.controllers.backup.manifest import ManifestError
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
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

    if state == "Failed":
        raise TaskAbort("Backup transfer failed on data plane")

    if state == "No process" and backup.status == Backup.STATUS_IN_PROGRESS:
        # "No process" means no transfer is running for this bdev — the backup
        # died (e.g. an SPDK crash wiped the in-flight transfer). Re-issue by
        # resetting to PENDING, but COUNT it as a retry so the max_retry ceiling
        # can stop a backup that keeps failing. Without that, re-issuing an RPC
        # that crashes the data plane just re-crashes it, forever.
        # NOTE: this treats "No process" as a failure. It relies on a healthy
        # in-progress backup NOT sitting in "No process"; if the data plane ever
        # reports "No process" for a running backup, this would fail it
        # prematurely and completion needs another signal.
        backup.status = Backup.STATUS_PENDING
        backup.write_to_db()
        raise TaskRetry("No process, retrying backup start")

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


def _run_merge(task):
    keep_backup_id = task.function_params.get("keep_backup_id")
    old_backup_id = task.function_params.get("old_backup_id")

    try:
        keep_backup = db.get_backup_by_id(keep_backup_id)
        old_backup = db.get_backup_by_id(old_backup_id)
    except KeyError as e:
        raise TaskAbort(str(e))

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
        logger.info(f"Merge completed: {old_backup_id} merged into {keep_backup_id}")
        return

    if state == "Failed":
        # Terminal, not retried: XFER_STATE_FAILED can be reached after the
        # data plane has already started deleting old_backup's S3 objects
        # (its DELETE state runs near the end of the merge), so retrying the
        # identical merge isn't known to be safe from here.
        if old_backup.status == Backup.STATUS_MERGING:
            old_backup.status = Backup.STATUS_COMPLETED
            old_backup.write_to_db()
        raise TaskAbort("Merge failed on data plane")

    if state == "No process":
        # Never started, or its result was already swept — re-issue.
        checkpoint(task, merge_started=False)
        raise TaskRetry("merge not running on the data plane; re-issuing")

    # "In progress" — still running, come back next pass.
    raise TaskDefer("Merge in progress")


_HANDLERS = {
    JobSchedule.FN_BACKUP: _run_backup,
    JobSchedule.FN_BACKUP_RESTORE: _run_restore,
    JobSchedule.FN_BACKUP_MERGE: _run_merge,
}


def process_task(task):
    cluster = db.get_cluster_by_id(task.cluster_id)
    backup_timeout_sec = getattr(cluster, 'backup_timeout_seconds', 0) or _DEFAULT_BACKUP_TIMEOUT_SEC
    elapsed = int(time.time()) - task.date if task.date else 0
    if elapsed > backup_timeout_sec:
        raise TaskAbort(f"timeout after {elapsed}s")

    _HANDLERS[task.function_name](task)


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

    elif task.function_name == JobSchedule.FN_BACKUP_MERGE:
        old_backup_id = task.function_params.get("old_backup_id")
        if not old_backup_id:
            return
        try:
            old_backup = db.get_backup_by_id(old_backup_id)
        except KeyError:
            return
        if old_backup.status == Backup.STATUS_MERGING:
            # Merge did not finish; leave the old backup intact.
            old_backup.status = Backup.STATUS_COMPLETED
            old_backup.write_to_db()


SPEC = RunnerSpec(
    name="tasks-runner-backup",
    function_names=list(_HANDLERS),
    handler=process_task,
    on_finish=finalize_resource,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
)


#: The task types this runner owns. The cluster's task list holds every other
#: runner's tasks too, and process_task terminates what it is handed (timeout,
#: max retry): without this filter it closed snapshot-replication tasks with
#: "max retry reached (8/8)" and would end any other task older than the
#: backup timeout (2026-09-29).
BACKUP_FUNCTIONS = (JobSchedule.FN_BACKUP, JobSchedule.FN_BACKUP_RESTORE,
                    JobSchedule.FN_BACKUP_MERGE)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
