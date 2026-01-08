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

    snapshot_id = task.function_params["snapshot_id"]

    logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
    ret, err = node.rpc_client().delete_lvol(lvol_bdev_name, del_async=True)
    if not ret:
        if "code" in err and err["code"] == -19:
            logger.error(f"Sync delete completed with error: {err}")
        else:
            logger.error(
                f"Failed to sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")

    task.function_result = f"bdev {lvol_bdev_name} deleted"
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
