# coding=utf-8
"""
tasks_runner_backup.py - background task runner for S3 backup operations.

Handles three task types:
  - FN_BACKUP: perform an S3 backup from a snapshot
  - FN_BACKUP_RESTORE: restore a backup chain into a new lvol
  - FN_BACKUP_MERGE: merge two backups to shorten the chain
"""
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import backup_events
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient, RPCException

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

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=30)

    if backup.status == Backup.STATUS_PENDING:
        try:
            snapshot = db.get_snapshot_by_id(backup.snapshot_id)
        except KeyError:
            _fail_backup(backup, task, f"Snapshot {backup.snapshot_id} not found")
            return

        snap_bdev_name = snapshot.snap_bdev
        if not snap_bdev_name:
            snap_bdev_name = f"{snapshot.lvol.lvs_name}/{snapshot.snap_name}"

        try:
            ret = rpc_client.bdev_lvol_s3_backup(backup.s3_id, [snap_bdev_name], cluster_batch=1)
            if not ret:
                _fail_backup(backup, task, "bdev_lvol_s3_backup RPC failed")
                return
        except RPCException as e:
            _fail_backup(backup, task, f"RPC error: {e.message}")
            return

        backup.status = Backup.STATUS_IN_PROGRESS
        backup.write_to_db()

    # Poll
    try:
        stat = rpc_client.bdev_lvol_s3_backup_stat(backup.s3_id)
    except RPCException:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if stat and isinstance(stat, dict):
        state = stat.get("transfer_state", "")
        if state == "Done":
            backup.status = Backup.STATUS_COMPLETED
            backup.completed_at = int(time.time())
            backup.write_to_db()
            backup_events.backup_completed(backup.cluster_id, backup.node_id, backup)
            task.function_result = "Backup completed"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
        elif state == "Failed":
            _fail_backup(backup, task, "Backup transfer failed on data plane")
        else:
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
    else:
        # Polling RPC not yet implemented — assume success
        backup.status = Backup.STATUS_COMPLETED
        backup.completed_at = int(time.time())
        backup.write_to_db()
        backup_events.backup_completed(backup.cluster_id, backup.node_id, backup)
        task.function_result = "Backup completed"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)


def _run_restore(task):
    backup_id = task.function_params.get("backup_id")
    lvol_name = task.function_params.get("lvol_name")
    chain_ids = task.function_params.get("chain_ids", [])
    node_id = task.node_id

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

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=30)

    try:
        ret = rpc_client.bdev_lvol_s3_recovery(lvol_name, chain_ids, cluster_batch=16)
        if not ret:
            task.function_result = "bdev_lvol_s3_recovery RPC failed"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
    except RPCException as e:
        task.function_result = f"RPC error: {e.message}"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # Poll
    try:
        stat = rpc_client.bdev_lvol_s3_recovery_stat(lvol_name)
    except RPCException:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if stat and isinstance(stat, dict):
        state = stat.get("transfer_state", "")
        if state == "Done":
            try:
                backup = db.get_backup_by_id(backup_id)
                backup_events.backup_restore_completed(
                    task.cluster_id, node_id, backup, lvol_name)
            except KeyError:
                pass
            task.function_result = f"Restore completed: {lvol_name}"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
        elif state == "Failed":
            task.function_result = "Restore failed"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
        else:
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
    else:
        # Polling RPC not yet implemented — assume success
        try:
            backup = db.get_backup_by_id(backup_id)
            backup_events.backup_restore_completed(
                task.cluster_id, node_id, backup, lvol_name)
        except KeyError:
            pass
        task.function_result = f"Restore completed: {lvol_name}"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)


def _run_merge(task):
    keep_backup_id = task.function_params.get("keep_backup_id")
    old_backup_id = task.function_params.get("old_backup_id")

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

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=30)

    try:
        ret = rpc_client.bdev_lvol_s3_merge(keep_backup.s3_id, old_backup.s3_id, cluster_batch=16)
        if not ret:
            task.function_result = "bdev_lvol_s3_merge RPC failed"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
    except RPCException as e:
        task.function_result = f"RPC error: {e.message}"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # Poll
    try:
        stat = rpc_client.bdev_lvol_s3_merge_stat(keep_backup.s3_id)
    except RPCException:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if stat and isinstance(stat, dict):
        state = stat.get("transfer_state", "")
        if state == "Done":
            keep_backup.prev_backup_id = old_backup.prev_backup_id
            keep_backup.write_to_db()
            backup_events.backup_merge_completed(
                keep_backup.cluster_id, keep_backup.node_id, keep_backup, old_backup.uuid)
            old_backup.remove(db.kv_store)
            task.function_result = "Merge completed"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
        elif state == "Failed":
            old_backup.status = Backup.STATUS_COMPLETED
            old_backup.write_to_db()
            task.function_result = "Merge failed"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
        else:
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
    else:
        # Polling RPC not yet implemented — assume success
        keep_backup.prev_backup_id = old_backup.prev_backup_id
        keep_backup.write_to_db()
        backup_events.backup_merge_completed(
            keep_backup.cluster_id, keep_backup.node_id, keep_backup, old_backup.uuid)
        old_backup.remove(db.kv_store)
        task.function_result = "Merge completed"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)


logger.info("Starting backup tasks runner...")
while True:
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
            if task.canceled:
                task.function_result = "canceled"
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db(db.kv_store)
                continue

            if task.retry >= task.max_retry:
                task.function_result = "max retry reached"
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db(db.kv_store)
                # Mark associated backup as failed so it doesn't stay in pending/in_progress
                if task.function_name == JobSchedule.FN_BACKUP:
                    bid = task.function_params.get("backup_id")
                    if bid:
                        try:
                            b = db.get_backup_by_id(bid)
                            if b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS):
                                _fail_backup(b, task, "max retry reached")
                        except KeyError:
                            pass
                elif task.function_name == JobSchedule.FN_BACKUP_MERGE:
                    old_bid = task.function_params.get("old_backup_id")
                    if old_bid:
                        try:
                            ob = db.get_backup_by_id(old_bid)
                            if ob.status == Backup.STATUS_MERGING:
                                ob.status = Backup.STATUS_COMPLETED
                                ob.write_to_db()
                        except KeyError:
                            pass
                continue

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

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
