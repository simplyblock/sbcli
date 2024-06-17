# coding=utf-8
import json
import logging
import uuid

import docker

from simplyblock_core import utils
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

logger = logging.getLogger()


def add_mgmt_node(node_ip, cluster_id=None):
    db_controller = DBController()
    hostname = utils.get_hostname()
    node = db_controller.get_mgmt_node_by_hostname(hostname)
    if node:
        logger.error("Node already exists in the cluster")
        return False

    node = MgmtNode()
    node.uuid = str(uuid.uuid4())
    node.hostname = hostname
    node.docker_ip_port = f"{node_ip}:2375"
    node.cluster_id = cluster_id
    node.node_ip = node_ip
    node.status = MgmtNode.STATUS_ONLINE
    node.write_to_db(db_controller.kv_store)

    mgmt_events.mgmt_add(node)
    logger.info("Done")
    return True


def list_mgmt_nodes(is_json):
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    data = []
    output = ""

    for node in nodes:
        logging.debug(node)
        logging.debug("*" * 20)
        data.append({
            "UUID": node.get_id(),
            "Hostname": node.hostname,
            "IP": node.node_ip,
            "Status": node.status,
        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def remove_mgmt_node(uuid):
    db_controller = DBController()
    snode = db_controller.get_mgmt_node_by_id(uuid)
    if not snode:
        logger.error("can not find node")
        return False

    logging.info("Removing mgmt node")
    snode.remove(db_controller.kv_store)

    logger.info("Leaving swarm...")
    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    node_docker.swarm.leave()

    mgmt_events.mgmt_remove(snode)
    logging.info("done")

