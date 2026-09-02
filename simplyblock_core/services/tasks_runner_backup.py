# coding=utf-8
"""
tasks_runner_backup.py - background task runner for S3 backup operations.

Handles three task types:
  - FN_BACKUP: perform an S3 backup from a snapshot
  - FN_BACKUP_RESTORE: restore a backup chain into a new lvol
  - FN_BACKUP_MERGE: merge two backups to shorten the chain
"""
import errno
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import backup_events
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import device as backup_device
from simplyblock_core.controllers.backup.manifest import ManifestError
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException, RPCRemoteError

logger = utils.get_logger(__name__)

db = db_controller.DBController()


def _fail_backup(backup, task, message):
    backup.status = Backup.STATUS_FAILED
    backup.error_message = message
    backup.write_to_db()
    backup_events.backup_failed(backup.cluster_id, backup.node_id, backup)
    task.function_result = message
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


def _run_backup(task):
    backup_id = task.function_params.get("backup_id")
    if not backup_id:
        task.function_result = "Missing backup_id"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    try:
        backup = db.get_backup_by_id(backup_id)
    except KeyError:
        task.function_result = f"Backup {backup_id} not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if backup.status not in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS):
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    try:
        snode = db.get_storage_node_by_id(backup.node_id)
    except KeyError:
        _fail_backup(backup, task, f"Node {backup.node_id} not found")
        return

    if snode.status != StorageNode.STATUS_ONLINE:
        task.retry += 1
        task.function_result = f"Node {snode.status}, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    rpc_client = snode.rpc_client(timeout=30)

    # Resolve snapshot bdev name (needed for both kick-off and polling)
    try:
        snapshot = db.get_snapshot_by_id(backup.snapshot_id)
    except KeyError:
        _fail_backup(backup, task, f"Snapshot {backup.snapshot_id} not found")
        return

    snap_bdev_name = snapshot.snap_bdev
    if not snap_bdev_name:
        snap_bdev_name = f"{snapshot.lvol.lvs_name}/{snapshot.snap_name}"

    if backup.status == Backup.STATUS_PENDING:
        try:
            ret = rpc_client.bdev_lvol_s3_backup(
                backup.s3_id, [snap_bdev_name],
                backup_device.primary_s3_bdev_name(snode), cluster_batch=16)
            if not ret:
                _fail_backup(backup, task, "bdev_lvol_s3_backup RPC failed")
                return
        except RPCRemoteError as e:
            if e.code == -errno.EBUSY:
                # Target S3 device already has another transfer in flight.
                # Expected, self-resolving contention -- retry without
                # counting against max_retry or failing the backup.
                task.function_result = "S3 device busy with another transfer, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return
            _fail_backup(backup, task, f"RPC error: {e}")
            return
        except RPCException as e:
            _fail_backup(backup, task, f"RPC error: {e}")
            return

        backup.status = Backup.STATUS_IN_PROGRESS
        backup.write_to_db()
        # Give the data plane time to start the transfer before polling
        task.status = JobSchedule.STATUS_SUSPENDED
        task.function_result = "Backup in progress"
        task.write_to_db(db.kv_store)
        return

    # Poll via bdev_lvol_transfer_stat on the snapshot bdev
    try:
        stat = rpc_client.bdev_lvol_transfer_stat(snap_bdev_name)
    except RPCException:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if stat and isinstance(stat, dict):
        state = stat.get("transfer_state", "")
        if state == "Done":
            backup.completed_at = int(time.time())

            # Publish the manifest BEFORE marking the backup completed, so that
            # COMPLETED implies "identifiable from the bucket alone". Data with
            # no manifest is data nobody can attribute to a volume later, so a
            # manifest failure fails the backup rather than leaving that behind.
            try:
                backup_controller.write_manifest(backup)
            except (ManifestError, PreconditionError) as e:
                _fail_backup(backup, task, f"Failed to publish backup manifest: {e}")
                return

            backup.status = Backup.STATUS_COMPLETED
            backup.write_to_db()
            backup_events.backup_completed(backup.cluster_id, backup.node_id, backup)
            task.function_result = "Backup completed"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
        elif state == "Failed":
            _fail_backup(backup, task, "Backup transfer failed on data plane")
        elif state == "No process" and backup.status == Backup.STATUS_IN_PROGRESS:
            # "No process" means no transfer is running for this bdev — the
            # backup died (e.g. an SPDK crash wiped the in-flight transfer).
            # Re-issue by resetting to PENDING, but COUNT it as a retry so the
            # max_retry ceiling in process_task() can stop a backup that keeps
            # failing. Without the increment this branch loops forever, and
            # re-issuing an RPC that crashes the data plane just re-crashes it.
            # NOTE: this treats "No process" as a failure. It relies on a
            # healthy in-progress backup NOT sitting in "No process"; if the
            # data plane ever reports "No process" for a running backup, this
            # would fail it prematurely and completion needs another signal.
            task.retry += 1
            backup.status = Backup.STATUS_PENDING
            backup.write_to_db()
            task.function_result = "No process, retrying backup start"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
        else:
            # "In progress" — still running, retry later
            task.status = JobSchedule.STATUS_SUSPENDED
            task.function_result = "Backup in progress"
            task.write_to_db(db.kv_store)
    else:
        # Unexpected response — retry
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)


def _set_lvol_online(task):
    """Mark restored lvol as online after successful data recovery."""
    lvol_id = task.function_params.get("lvol_id")
    if not lvol_id:
        return
    try:
        from simplyblock_core.models.lvol_model import LVol
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
        from simplyblock_core.models.lvol_model import LVol
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

    Called on every terminal path. The credentials are scrubbed from the task
    record because a task is retained for weeks after it finishes, and there is
    no reason for another cluster's S3 keys to outlive the restore that needed
    them.
    """
    if not task.function_params.get("s3_config"):
        return

    if snode is not None:
        backup_device.delete_restore_s3_bdev(snode, _restore_s3_bdev(task, snode))

    task.function_params["s3_config"] = None


def _run_restore(task):
    backup_id = task.function_params.get("backup_id")
    lvol_name = task.function_params.get("lvol_name")
    chain_ids = task.function_params.get("chain_ids", [])
    node_id = task.node_id
    recovery_started = task.function_params.get("recovery_started", False)

    try:
        snode = db.get_storage_node_by_id(node_id)
    except KeyError:
        task.function_result = f"Node {node_id} not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if snode.status != StorageNode.STATUS_ONLINE:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    rpc_client = snode.rpc_client(timeout=30)

    # Check that the target lvol still exists in DB before doing any RPC work
    lvol_id = task.function_params.get("lvol_id")
    if lvol_id:
        try:
            from simplyblock_core.models.lvol_model import LVol
            lvol = db.get_lvol_by_id(lvol_id)
            if lvol.status == LVol.STATUS_IN_DELETION:
                _release_restore_s3_bdev(task, snode)
                task.function_result = f"Restore target {lvol_id} has been deleted"
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db(db.kv_store)
                return
        except KeyError:
            _release_restore_s3_bdev(task, snode)
            task.function_result = f"Restore target {lvol_id} no longer exists"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return

    if not recovery_started:
        # The device is established here, not when the restore was requested:
        # only now is the node known (add_lvol_ha chooses it), and only the
        # runner can put it back after a node restart wipes it mid-restore.
        try:
            _ensure_restore_s3_bdev(task, snode)
        except (RuntimeError, ValueError) as e:
            task.function_result = f"Could not attach the backup's bucket: {e}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        try:
            ret = rpc_client.bdev_lvol_s3_recovery(
                lvol_name, chain_ids, cluster_batch=16,
                s3_bdev=_restore_s3_bdev(task, snode))
            if not ret:
                task.function_result = "bdev_lvol_s3_recovery RPC failed"
                task.retry += 1
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return
        except RPCRemoteError as e:
            if e.code == -errno.EBUSY:
                # Target S3 device already has another transfer in flight.
                # Expected, self-resolving contention -- retry without
                # counting against max_retry.
                task.function_result = "S3 device busy with another transfer, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return
            task.function_result = f"RPC error: {e}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        except RPCException as e:
            task.function_result = f"RPC error: {e}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        # Mark recovery as started so we don't re-issue the RPC on subsequent polls
        task.function_params["recovery_started"] = True
        # Give the data plane time to start the transfer before polling
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # Poll via bdev_lvol_transfer_stat on the target lvol
    try:
        stat = rpc_client.bdev_lvol_transfer_stat(lvol_name)
    except RPCException:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if stat and isinstance(stat, dict):
        state = stat.get("transfer_state", "")
        if state == "Done":
            _set_lvol_online(task)
            try:
                backup = db.get_backup_by_id(backup_id)
                backup_events.backup_restore_completed(
                    task.cluster_id, node_id, backup, lvol_name)
            except KeyError:
                pass
            _release_restore_s3_bdev(task, snode)
            task.function_result = f"Restore completed: {lvol_name}"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
        elif state == "Failed":
            fail_count = task.function_params.get("fail_count", 0) + 1
            task.function_params["fail_count"] = fail_count
            reason = f"S3 transfer failed on data plane (attempt {fail_count})"
            task.function_result = reason
            if fail_count >= 3:
                _set_lvol_restore_failed(task, reason)
                try:
                    backup = db.get_backup_by_id(backup_id)
                    backup_events.backup_restore_failed(
                        task.cluster_id, node_id, backup, lvol_name, reason)
                except KeyError:
                    logger.warning(
                        "Backup %s not found in DB; restore-failed event skipped for lvol %s",
                        backup_id, lvol_name)
                _release_restore_s3_bdev(task, snode)
                task.status = JobSchedule.STATUS_DONE
            else:
                task.retry += 1
                task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
        elif state == "No process":
            task.function_params["recovery_started"] = False
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
        else:
            # "In progress" — still running, retry later
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
    else:
        # Unexpected response — retry
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)


def _run_merge(task):
    keep_backup_id = task.function_params.get("keep_backup_id")
    old_backup_id = task.function_params.get("old_backup_id")
    merge_started = task.function_params.get("merge_started", False)

    try:
        keep_backup = db.get_backup_by_id(keep_backup_id)
        old_backup = db.get_backup_by_id(old_backup_id)
    except KeyError as e:
        task.function_result = str(e)
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    try:
        snode = db.get_storage_node_by_id(keep_backup.node_id)
    except KeyError:
        task.function_result = f"Node {keep_backup.node_id} not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if snode.status != StorageNode.STATUS_ONLINE:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    rpc_client = snode.rpc_client(timeout=30)

    if not merge_started:
        try:
            ret = rpc_client.bdev_lvol_s3_merge(
                keep_backup.s3_id, old_backup.s3_id, cluster_batch=16,
                s3_bdev=backup_device.primary_s3_bdev_name(snode),
                lvs_name=snode.lvstore)
            if not ret:
                task.function_result = "bdev_lvol_s3_merge RPC failed"
                task.retry += 1
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return
        except RPCRemoteError as e:
            if e.code == -errno.EBUSY:
                # Target S3 device already has another transfer in flight.
                # Expected, self-resolving contention -- retry without
                # counting against max_retry.
                task.function_result = "S3 device busy with another transfer, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return
            task.function_result = f"RPC error: {e}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        except RPCException as e:
            task.function_result = f"RPC error: {e}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        task.function_params["merge_started"] = True
        task.write_to_db(db.kv_store)
        # Give the data plane time to complete the merge before finalizing
        return

    # The merge RPC is synchronous on the data plane — once it returned
    # successfully, the S3 data has been merged.  Finalize: update the
    # chain links, remove the old backup, and mark the task done.
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
        # cannot be failed here -- retry the manifest work instead.
        task.function_result = f"Merge done, manifest update failed: {e}"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    task.function_result = "Merge completed"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)
    logger.info(f"Merge completed: {old_backup_id} merged into {keep_backup_id}")


def _terminate_task(task, reason):
    """Terminate a backup/restore/merge task and finalize its resource.

    Shared by the time-based timeout and the max_retry ceiling so both stop
    the task the same way instead of leaving it to loop.
    """
    if task.function_name == JobSchedule.FN_BACKUP:
        bid = task.function_params.get("backup_id")
        if bid:
            try:
                b = db.get_backup_by_id(bid)
                if b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS):
                    _fail_backup(b, task, reason)
                    return
            except KeyError:
                pass
    elif task.function_name == JobSchedule.FN_BACKUP_RESTORE:
        _set_lvol_restore_failed(task, reason)
        # This is the timeout / retry-ceiling path, so it is terminal too and
        # owes the same cleanup as a normal failure.
        try:
            _release_restore_s3_bdev(task, db.get_storage_node_by_id(task.node_id))
        except KeyError:
            _release_restore_s3_bdev(task, None)
    elif task.function_name == JobSchedule.FN_BACKUP_MERGE:
        old_bid = task.function_params.get("old_backup_id")
        if old_bid:
            try:
                ob = db.get_backup_by_id(old_bid)
                if ob.status == Backup.STATUS_MERGING:
                    # Merge did not finish; leave the old backup intact.
                    ob.status = Backup.STATUS_COMPLETED
                    ob.write_to_db()
            except KeyError:
                pass

    task.function_result = reason
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


def process_task(task, cl):
    """Advance a single backup task by one step, or terminate it.

    Terminates on cancellation, on the time-based timeout, or once the
    max_retry ceiling is reached — the last is what stops a backup that keeps
    crashing the data plane from re-issuing its RPC forever.
    """
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    # Time-based backstop for a task that is stuck but not erroring (default 4h).
    backup_timeout_sec = getattr(cl, 'backup_timeout_seconds', 0) or 14400
    elapsed = int(time.time()) - task.date if task.date else 0
    if elapsed > backup_timeout_sec:
        _terminate_task(task, f"timeout after {elapsed}s")
        return

    # Retry ceiling: every other task runner enforces this. Without it a task
    # whose step keeps failing (e.g. an RPC that crashes SPDK) loops until the
    # timeout, re-triggering the failure each cycle. max_retry <= 0 means the
    # task is intentionally unbounded and only the timeout applies.
    if task.max_retry > 0 and task.retry >= task.max_retry:
        _terminate_task(task, f"max retry reached ({task.retry}/{task.max_retry})")
        return

    try:
        if task.function_name == JobSchedule.FN_BACKUP:
            _run_backup(task)
        elif task.function_name == JobSchedule.FN_BACKUP_RESTORE:
            _run_restore(task)
        elif task.function_name == JobSchedule.FN_BACKUP_MERGE:
            _run_merge(task)
    except Exception as e:
        logger.error(f"Error running backup task {task.uuid}: {e}")
        # Increment retry so the task eventually reaches max_retry
        # instead of looping forever on non-RPCException errors
        task.retry += 1
        task.function_result = f"Unhandled error: {e}"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)


if __name__ == "__main__":
    logger.info("Starting backup tasks runner...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                if task.status == JobSchedule.STATUS_DONE or task.canceled:
                    continue

                # Re-fetch task for freshness
                task = db.get_task_by_id(task.uuid)
                process_task(task, cl)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
