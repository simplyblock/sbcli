# coding=utf-8
"""
tasks_runner_backup.py - background task runner for S3 backup operations.

Handles three task types:
  - FN_BACKUP: perform an S3 backup from a snapshot
  - FN_BACKUP_RESTORE: restore a backup chain into a new lvol
  - FN_BACKUP_MERGE: merge two backups to shorten the chain

All three are multi-cycle: a task issues its RPC, defers, and polls the data
plane's transfer state on later cycles until it reaches a terminal state.
"""
import time

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import backup_events
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskRetry,
    serve,
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
            ret = rpc_client.bdev_lvol_s3_backup(backup.s3_id, [snap_bdev_name], cluster_batch=16)
        except RPCException as e:
            raise TaskAbort(f"RPC error: {e}")
        if not ret:
            raise TaskAbort("bdev_lvol_s3_backup RPC failed")

        backup.status = Backup.STATUS_IN_PROGRESS
        backup.write_to_db()
        # Give the data plane time to start the transfer before polling
        raise TaskDefer("Backup in progress")

    state = _transfer_state(rpc_client, snap_bdev_name)
    if state == "Done":
        backup.status = Backup.STATUS_COMPLETED
        backup.completed_at = int(time.time())
        backup.write_to_db()
        backup_events.backup_completed(backup.cluster_id, backup.node_id, backup)
        task.function_result = "Backup completed"
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
        try:
            ret = rpc_client.bdev_lvol_s3_recovery(lvol_name, chain_ids, cluster_batch=16)
        except RPCException as e:
            raise TaskRetry(f"RPC error: {e}")
        if not ret:
            raise TaskRetry("bdev_lvol_s3_recovery RPC failed")

        # Don't re-issue the RPC on subsequent polls, and give the data plane
        # time to start the transfer before the first one.
        task.function_params["recovery_started"] = True
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
        task.function_result = f"Restore completed: {lvol_name}"
        return

    if state == "Failed":
        fail_count = task.function_params.get("fail_count", 0) + 1
        task.function_params["fail_count"] = fail_count
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
        task.function_params["recovery_started"] = False
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
                keep_backup.s3_id, old_backup.s3_id, cluster_batch=16, lvs_name=snode.lvstore)
        except RPCException as e:
            raise TaskRetry(f"RPC error: {e}")
        if not ret:
            raise TaskRetry("bdev_lvol_s3_merge RPC failed")

        task.function_params["merge_started"] = True
        # Give the data plane time to complete the merge before finalizing
        raise TaskDefer("Merge started")

    # The merge RPC is synchronous on the data plane — once it returned
    # successfully, the S3 data has been merged.  Finalize: update the
    # chain links, remove the old backup, and mark the task done.
    keep_backup.prev_backup_id = old_backup.prev_backup_id
    keep_backup.status = Backup.STATUS_COMPLETED
    keep_backup.write_to_db()

    old_backup.status = Backup.STATUS_MERGED
    old_backup.write_to_db()

    task.function_result = "Merge completed"
    logger.info(f"Merge completed: {old_backup_id} merged into {keep_backup_id}")


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


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
