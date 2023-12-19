# coding=utf-8
import logging
import os

import time
import sys
from datetime import datetime


SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(SCRIPT_PATH, "../.."))

from management import constants, kv_store, cluster_ops, distr_controller
from management.controllers import storage_events
from management.models.storage_node import StorageNode
from management.rpc_client import RPCClient

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
    cls = db_controller.get_clusters(id=cluster_id)
    cl = cls[0]
    snodes = db_controller.get_storage_nodes()
    online_nodes = 0
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes += 1
    other_nodes = len(snodes) - online_nodes
    if cl.ha_type == "ha":
        if online_nodes < 3:
            if cl.status == cl.STATUS_ACTIVE:
                logger.warning(f"Cluster mode is HA=1 but online storage nodes are less than 3, "
                               f"suspending cluster: {cluster_id}")
                cluster_ops.suspend_cluster(cluster_id)
        else:
            if cl.status == cl.STATUS_SUSPENDED and other_nodes < 2:
                logger.info(f"Cluster mode is HA=1 and online storage nodes are 3 or more, "
                            f"resuming cluster: {cluster_id}")
                cluster_ops.unsuspend_cluster(cluster_id)

        if other_nodes == 0:
            if cl.status == cl.STATUS_DEGRADED:
                logger.info(f"Cluster mode is HA=1 and offline storage nodes is 0, "
                            f"setting cluster to active: {cluster_id}")
                cluster_ops.unsuspend_cluster(cluster_id)
        elif other_nodes == 1:
            if cl.status == cl.STATUS_ACTIVE:
                logger.warning(f"Cluster mode is HA=1 and offline storage nodes is 1, "
                               f"degrading cluster: {cluster_id}")
                cluster_ops.degrade_cluster(cluster_id)
        elif other_nodes > 1:
            if cl.status in [cl.STATUS_ACTIVE, cl.STATUS_DEGRADED]:
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


def ping_host(ip):
    logger.info(f"Pinging ip {ip}")
    response = os.system(f"ping -c 1 {ip}")
    if response == 0:
        logger.info(f"{ip} is UP")
        return True
    else:
        logger.info(f"{ip} is DOWN")
        return False


logger.info("Starting node monitor")

while True:
    # get storage nodes
    nodes = db_controller.get_storage_nodes()
    for node in nodes:
        if node.status not in [node.STATUS_ONLINE, node.STATUS_UNREACHABLE]:
            logger.info(f"Node status is: {node.status}, skipping")
            continue

        logger.info(f"Checking node {node.hostname}")
        if not ping_host(node.mgmt_ip):
            logger.info(f"Node {node.hostname} is offline")
            set_node_offline(node)
            continue
        rpc_client = RPCClient(
            node.mgmt_ip,
            node.rpc_port,
            node.rpc_username,
            node.rpc_password,
            timeout=2, retry=2)
        try:
            logger.info(f"Calling rpc get_version...")
            response = rpc_client.get_version()
            if response:
                logger.info(f"Node {node.hostname} is online")
                set_node_online(node)
            else:
                logger.info(f"Node rpc {node.hostname} is unreachable")
                set_node_offline(node)
        except Exception as e:
            print(e)
            logger.info(f"Error connecting to node: {node.hostname}")
            set_node_offline(node)
        update_cluster_status(node.cluster_id)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
