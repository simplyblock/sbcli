# coding=utf-8
import time
from typing import Optional

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import snapshot_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

def get_primary_node(task) -> Optional[StorageNode]:
    if "primary_node" in task.function_params:
        return db.get_storage_node_by_id(task.function_params["primary_node"])

    nodes = db.get_primary_storage_nodes_by_secondary_node_id(task.node_id)
    if nodes:
        return nodes[0]
    return None


def _run_sync_op(task):
    # Re-read (it may have been canceled concurrently).
    task = db.get_task_by_id(task.uuid)
    if task.status == JobSchedule.STATUS_DONE:
        return
    try:
        tasks_controller.run_lvol_sync_op_task(task)
    except Exception as e:
        logger.error(f"lvol sync-op task {task.uuid} crashed: {e}")


def _run_sync_del(task):
    # get new task object because it could be changed from cancel task
    task = db.get_task_by_id(task.uuid)

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        primary_node = get_primary_node(task)
        if primary_node:
            primary_node.lvol_del_sync_lock_reset()
        return

    node = db.get_storage_node_by_id(task.node_id)

    if not node:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        primary_node = db.get_storage_node_by_id(task.function_params["primary_node"])
        primary_node.lvol_del_sync_lock_reset()
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

    lvol_bdev_name = task.function_params["lvol_bdev_name"]

    logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
    try:
        # Per-node lvstore lock: the sync delete mutates the replica blob tree
        # and must not interleave with a create/register of another object on
        # this node. The try also keeps a dead node from killing the runner: on
        # 2026-07-16 an unhandled RPCException ('connection error') here took the
        # whole service down and no deferred sync delete ever ran again.
        with snapshot_controller.lvstore_op_lock(
                node.cluster_id,
                lvol_bdev_name.split("/")[0],
                node_id=node.get_id()):
            ret, err = node.rpc_client().delete_lvol(lvol_bdev_name, del_async=True)
    except Exception as e:
        msg = (f"Sync delete of {lvol_bdev_name} on {node.get_id()} "
               f"failed: {e}; will retry")
        logger.error(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return
    if not ret:
        if "code" in err and err["code"] == -19:
            logger.error(f"Sync delete completed with error: {err}")
        else:
            msg = f"Failed to sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

    task.function_result = f"bdev {lvol_bdev_name} deleted"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)
    primary_node = get_primary_node(task)
    if primary_node:
        primary_node.lvol_del_sync_lock_reset()


def task_runner(task):
    if task.function_name == JobSchedule.FN_LVOL_SYNC_OP:
        _run_sync_op(task)
    elif task.function_name == JobSchedule.FN_LVOL_SYNC_DEL:
        _run_sync_del(task)


def main():
    logger.info("Starting Tasks runner...")

    while True:
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                if cl.status == Cluster.STATUS_IN_ACTIVATION:
                    continue

                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    if task.function_name not in (
                        JobSchedule.FN_LVOL_SYNC_OP,
                        JobSchedule.FN_LVOL_SYNC_DEL,
                    ):
                        continue
                    if task.status != JobSchedule.STATUS_DONE:
                        if not tasks_controller.claim_task(task):
                            logger.info(f"LVol sync task {task.uuid} owned by another runner host; skipping")
                            continue
                        task_runner(task)

        time.sleep(3)


if __name__ == "__main__":
    main()
