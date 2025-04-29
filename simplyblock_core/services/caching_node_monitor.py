# coding=utf-8
import os

import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.models.caching_node import CachingNode


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def set_node_online(node):
    if node.status == CachingNode.STATUS_UNREACHABLE:
        snode = db.get_caching_node_by_id(node.get_id())
        old_status = snode.status
        snode.status = CachingNode.STATUS_ONLINE
        snode.updated_at = str(datetime.now())
        snode.write_to_db()
        # mgmt_events.status_change(snode, snode.status, old_status, caused_by="monitor")


def set_node_offline(node):
    if node.status == CachingNode.STATUS_ONLINE:
        snode = db.get_caching_node_by_id(node.get_id())
        old_status = snode.status
        snode.status = CachingNode.STATUS_UNREACHABLE
        snode.updated_at = str(datetime.now())
        snode.write_to_db()
        # mgmt_events.status_change(snode, snode.status, old_status, caused_by="monitor")


def ping_host(ip):
    logger.info(f"Pinging ip {ip}")
    response = os.system(f"ping -c 1 {ip}")
    if response == 0:
        logger.info(f"{ip} is UP")
        return True
    else:
        logger.info(f"{ip} is DOWN")
        return False


logger.info("Starting Caching node monitor")


while True:
    nodes = db.get_caching_nodes()
    for node in nodes:
        if node.status not in [CachingNode.STATUS_ONLINE, CachingNode.STATUS_UNREACHABLE]:
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
            timeout=3, retry=1)
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

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
