# coding=utf-8
import os

import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import mgmt_events, health_controller
from simplyblock_core.models.mgmt_node import MgmtNode


logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()


def set_node_online(node):
    if node.status == MgmtNode.STATUS_UNREACHABLE:
        snode = db.get_mgmt_node_by_id(node.get_id())
        old_status = snode.status
        snode.status = MgmtNode.STATUS_ONLINE
        snode.updated_at = str(datetime.now())
        snode.write_to_db()
        mgmt_events.status_change(snode, snode.status, old_status, caused_by="monitor")


def set_node_offline(node):
    if node.status == MgmtNode.STATUS_ONLINE:
        snode = db.get_mgmt_node_by_id(node.get_id())
        old_status = snode.status
        snode.status = MgmtNode.STATUS_UNREACHABLE
        snode.updated_at = str(datetime.now())
        snode.write_to_db()
        mgmt_events.status_change(snode, snode.status, old_status, caused_by="monitor")


logger.info("Starting Mgmt node monitor")


while True:
    # get storage nodes
    nodes = db.get_mgmt_nodes()
    for node in nodes:
        if node.status not in [MgmtNode.STATUS_ONLINE, MgmtNode.STATUS_UNREACHABLE]:
            logger.info(f"Node status is: {node.status}, skipping")
            continue

        # 1- check node ping
        ping_check = health_controller._check_node_ping(node.mgmt_ip)
        logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
        if not ping_check:
            time.sleep(1)
            ping_check = health_controller._check_node_ping(node.mgmt_ip)
            logger.info(f"Check 2: ping mgmt ip {node.mgmt_ip} ... {ping_check}")

        if not ping_check:
            logger.info(f"Node {node.hostname} is offline")
            set_node_offline(node)
            continue

        c = utils.get_docker_client()
        nl = c.nodes.list(filters={'role': 'manager'})
        docker_node = None
        for n in nl:
            if n.attrs['ManagerStatus']['Addr'].startswith(node.mgmt_ip):
                docker_node = n
                break
        if not docker_node:
            logger.error("Node is not part of the docker swarm!")
            set_node_offline(node)
            continue

        if docker_node.attrs['ManagerStatus']['Reachability'] == 'reachable':
            set_node_online(node)
        else:
            set_node_offline(node)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
