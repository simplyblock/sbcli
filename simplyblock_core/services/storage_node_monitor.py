# coding=utf-8
import logging
import os

import time
import sys
from datetime import datetime


from simplyblock_core import constants, kv_store, cluster_ops
from simplyblock_core.controllers import storage_events, health_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)


# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController(kv_store=db_store)


def update_cluster_status(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)

    snodes = db_controller.get_storage_nodes()
    online_nodes = 0
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes += 1
    other_nodes = len(snodes) - online_nodes
    if cluster.ha_type == "ha":
        if online_nodes < 3:
            if cluster.status == Cluster.STATUS_ACTIVE:
                logger.warning(f"Cluster mode is HA=1 but online storage nodes are less than 3, "
                               f"suspending cluster: {cluster_id}")
                cluster_ops.suspend_cluster(cluster_id)
        else:
            if cluster.status == Cluster.STATUS_SUSPENDED and other_nodes < 2:
                logger.info(f"Cluster mode is HA=1 and online storage nodes are 3 or more, "
                            f"resuming cluster: {cluster_id}")
                cluster_ops.unsuspend_cluster(cluster_id)

        if other_nodes == 0:
            if cluster.status == Cluster.STATUS_DEGRADED:
                logger.info(f"Cluster mode is HA=1 and offline storage nodes is 0, "
                            f"setting cluster to active: {cluster_id}")
                cluster_ops.unsuspend_cluster(cluster_id)
        elif other_nodes == 1:
            if cluster.status == Cluster.STATUS_ACTIVE:
                logger.warning(f"Cluster mode is HA=1 and offline storage nodes is 1, "
                               f"degrading cluster: {cluster_id}")
                cluster_ops.degrade_cluster(cluster_id)
        elif other_nodes > 1:
            if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
                logger.warning(f"Cluster mode is HA=1 and offline storage nodes is more than 1, "
                               f"suspending cluster: {cluster_id}")
                cluster_ops.suspend_cluster(cluster_id)


def set_node_online(node):
    if node.status == StorageNode.STATUS_UNREACHABLE:
        snode = db_controller.get_storage_node_by_id(node.get_id())
        # for dev in snode.nvme_devices:
        #     if dev.status == 'unavailable':
        #         dev.status = 'online'
        #         distr_controller.send_dev_status_event(dev.cluster_device_order, "online")

        old_status = snode.status
        snode.status = StorageNode.STATUS_ONLINE
        snode.updated_at = str(datetime.now())
        snode.write_to_db(db_store)
        storage_events.snode_status_change(snode, snode.status, old_status, caused_by="monitor")


def set_node_offline(node):
    if node.status == StorageNode.STATUS_ONLINE:
        snode = db_controller.get_storage_node_by_id(node.get_id())
        # for dev in snode.nvme_devices:
        #     if dev.status == 'online':
        #         dev.status = 'unavailable'
        #         distr_controller.send_dev_status_event(dev.cluster_device_order, "unavailable")

        old_status = snode.status
        snode.status = StorageNode.STATUS_UNREACHABLE
        snode.updated_at = str(datetime.now())
        snode.write_to_db(db_store)
        storage_events.snode_status_change(snode, snode.status, old_status, caused_by="monitor")


logger.info("Starting node monitor")

while True:
    # get storage nodes
    nodes = db_controller.get_storage_nodes()
    for snode in nodes:
        if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE]:
            logger.info(f"Node status is: {snode.status}, skipping")
            continue

        logger.info(f"Checking node {snode.hostname}")

        # 1- check node ping
        ping_check = health_controller._check_node_ping(snode.mgmt_ip)
        logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

        # 2- check node API
        node_api_check = health_controller._check_node_api(snode.mgmt_ip)
        logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

        # 3- check node RPC
        node_rpc_check = health_controller._check_node_rpc(
            snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
        logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

        # 4- docker API
        node_docker_check = health_controller._check_node_docker_api(snode.mgmt_ip)
        logger.info(f"Check: node docker API {snode.mgmt_ip}:2375 ... {node_docker_check}")

        is_node_online = ping_check and node_api_check and node_rpc_check and node_docker_check
        if is_node_online:
            set_node_online(snode)
        else:
            set_node_offline(snode)

        update_cluster_status(snode.cluster_id)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
