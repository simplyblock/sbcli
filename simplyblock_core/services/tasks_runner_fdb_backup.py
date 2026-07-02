# coding=utf-8
import time


from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import backup_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_fdb_backup_task(task):
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached, stopping task"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    ret = backup_controller.create_backup()
    if ret:
        task.function_result = "Backup created"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)


def process_snap_backup_task(task):

    task = db.get_task_by_id(task.uuid)

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    node = db.get_storage_node_by_id(task.node_id)

    if not node:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
        msg = f"Node is {node.status}, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    cluster = db.get_cluster_by_id(task.cluster_id)
    snapshot_id = task.function_params["snapshot_id"]
    snapshot = db.get_snapshot_by_id(snapshot_id)
    logger.info(f"backing up: {snapshot_id}")
    if cluster.backup_s3_bucket and cluster.backup_s3_cred:
        folder = f"backup-{snapshot_id}"
        folder = folder.replace(" ", "-")
        folder = folder.replace(":", "-")
        folder = folder.split(".")[0]
        backup_path = f"blobstore://{cluster.backup_s3_cred}@s3.{cluster.backup_s3_region}.amazonaws.com/{folder}?bucket={cluster.backup_s3_bucket}&region={cluster.backup_s3_region}&sc=0"
        ret, err = node.rpc_client().bdev_lvol_s3_backup(backup_path, snapshot.snap_bdev)
        if not ret:
            logger.error(
                f"Failed to backup snapshot: {snapshot_id}, error: {err}")

            task.function_result = f"snap backup error"
        else:
            task.function_result = f"snap backup done"

    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


logger.info("Starting Tasks runner fdb backup...")

while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            tasks = db.get_job_tasks(cl.get_id())
            for task in tasks:
                if task.status != JobSchedule.STATUS_DONE:
                    if task.function_name == JobSchedule.FN_FDB_BACKUP:
                        process_fdb_backup_task(task)
                    elif task.function_name == JobSchedule.FN_SNAPSHOT_BACKUP:
                        process_snap_backup_task(task)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
