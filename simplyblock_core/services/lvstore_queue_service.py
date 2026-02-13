# coding=utf-8
import threading
import time

from simplyblock_core import db_controller, utils, rpc_client
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvstore_queue import LVStoreQueueTask
from simplyblock_core.models.storage_node import StorageNode


utils.init_sentry_sdk()
logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


# def complete_lvol_add(task):

def task_runner(task):
    task.status = LVStoreQueueTask.STATUS_RUNNING
    task.write_to_db(db.kv_store)

    # check leadership
    primary_node = db.get_storage_node_by_id(task.primary_node_id)
    secondary_node = None
    if primary_node.secondary_node_id:
        secondary_node = db.get_storage_node_by_id(primary_node.secondary_node_id)
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
        lvol = db.get_lvol_by_id(task.function_params["lvol_id"])
        if leader_node:
            lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, primary_node)
            if error:
                logger.error(error)
                lvol.remove(db.kv_store)
                return False, error

            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']


        if secondary_node:
            secondary_node = db.get_storage_node_by_id(secondary_node.get_id())
            if secondary_node.status == StorageNode.STATUS_ONLINE:
                lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, secondary_node, is_primary=False)
                if error:
                    logger.error(error)
                    # remove lvol from primary
                    ret = lvol_controller.delete_lvol_from_node(lvol.get_id(), primary_node.get_id())
                    if not ret:
                        logger.error("")
                    lvol.remove(db_controller.kv_store)
                    return False, error

        lvol.status = LVol.STATUS_ONLINE
        lvol.write_to_db(db_controller.kv_store)
        lvol_events.lvol_create(lvol)

        if pool.has_qos():
            connect_lvol_to_pool(lvol.uuid)

        # set QOS
        if max_rw_iops >= 0 or max_rw_mbytes >= 0 or max_r_mbytes >= 0 or max_w_mbytes >= 0:
            set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)


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
