# coding=utf-8
import threading
import time

from simplyblock_core import db_controller, utils, rpc_client
from simplyblock_core.models.lvstore_queue import LVStoreQueueTask
from simplyblock_core.models.storage_node import StorageNode


utils.init_sentry_sdk()
logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()



def task_runner(task):
    task.status = LVStoreQueueTask.STATUS_RUNNING
    task.write_to_db(db.kv_store)

    # check leadership
    primary_node = db.get_storage_node_by_id(task.primary_node_id)
    sec_node = None
    if primary_node.secondary_node_id:
        sec_node = db.get_storage_node_by_id(primary_node.secondary_node_id)
    leader_node = None
    if primary_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
        if not ret:
            raise Exception("Failed to get LVol info")
        lvs_info = ret[0]
        if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
            leader_node = snode

    if not leader_node and sec_node:
        ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
        if not ret:
            raise Exception("Failed to get LVol info")
        lvs_info = ret[0]
        if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
            leader_node = sec_node

    if not leader_node:
        raise Exception("Failed to get leader node")



    if task.function_name == LVStoreQueueTask.FN_LVOL_ADD:
        res = rpc_client.lvol_add(task.lvstore, task.function_params["lvol_name"], task.function_params["size"])
        if res:
            task.function_result = res
            task.status = LVStoreQueueTask.STATUS_DONE
            task.write_to_db(db.kv_store)
            logger.info(f"LVOL {task.function_params['lvol_name']} added to lvstore {task.lvstore} successfully")
            return True
        else:
            task.function_result = "failed to add lvol"
            task.write_to_db(db.kv_store)
            logger.error(f"Failed to add lvol {task.function_params['lvol_name']} to lvstore {task.lvstore}")
            return False


def start_on_node(node_id):
    try:
        node = db.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error(f"Storage node {node_id} not found in DB, skipping...")
        return
    if not node.lvstore:
        logger.info(f"Storage node {node.get_id()} has no lvstore, skipping...")
        return

    logger.info(f"Starting LVStore queue: {node.lvstore}")

    tasks = db.get_lvstore_queue_tasks(node_id)
    for task in tasks:
        while task.status != LVStoreQueueTask.STATUS_DONE:
            res = task_runner(task)
            if res:
                if task.status == LVStoreQueueTask.STATUS_DONE:
                    break
            time.sleep(3)


threads_maps: dict[str, threading.Thread] = {}

while True:
    clusters = db.get_clusters()
    for cluster in clusters:
        nodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())
        for snode in nodes:
            node_id = snode.get_id()
            if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                t = threading.Thread(target=start_on_node, args=(node_id,))
                t.start()
                threads_maps[node_id] = t

    time.sleep(3)
