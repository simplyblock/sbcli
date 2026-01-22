# coding=utf-8
import time
from typing import Optional

from simplyblock_core import db_controller, utils
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

                if task.function_name == JobSchedule.FN_LVOL_SYNC_DEL:
                    if task.status != JobSchedule.STATUS_DONE:

                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)

                        if task.canceled:
                            task.function_result = "canceled"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            primary_node = get_primary_node(task)
                            if primary_node:
                                primary_node.lvol_del_sync_lock_reset()
                            continue

                        node = db.get_storage_node_by_id(task.node_id)

                        if not node:
                            task.function_result = "node not found"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
                            msg = f"Node is {node.status}, retry task"
                            logger.info(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        if task.status != JobSchedule.STATUS_RUNNING:
                            task.status = JobSchedule.STATUS_RUNNING
                            task.write_to_db(db.kv_store)

                        lvol_bdev_name = task.function_params["lvol_bdev_name"]

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
                        primary_node = get_primary_node(task)
                        if primary_node:
                            primary_node.lvol_del_sync_lock_reset()

    time.sleep(3)
