# coding=utf-8
import datetime
import json
import logging
import uuid

import docker

from simplyblock_core import utils
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.mgmt_node import MgmtNode

logger = logging.getLogger()


def add_mgmt_node(mgmt_ip, cluster_id=None):
    db_controller = DBController()
    hostname = utils.get_hostname()
    node = db_controller.get_mgmt_node_by_hostname(hostname)
    if node:
        logger.error("Node already exists in the cluster")
        return False

    node = MgmtNode()
    node.system_uuid = str(uuid.uuid4())
    node.hostname = hostname
    node.docker_ip_port = f"{mgmt_ip}:2375"
    node.cluster_id = cluster_id
    node.mgmt_ip = mgmt_ip
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
            "Hostname": node.hostname,
            "IP": node.docker_ip_port.split(":")[0],
            "Status": node.status,
        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def remove_mgmt_node(hostname):
    db_controller = DBController()
    logging.info("removing mgmt node")
    snode = db_controller.get_mgmt_node_by_hostname(hostname)
    if not snode:
        logger.error("can not find node")
        exit(1)

    snode.remove(db_controller.kv_store)

    logger.info("Leaving swarm...")
    node_docker = docker.DockerClient(base_url=f"tcp://{snode.docker_ip_port}", version="auto")
    node_docker.swarm.leave()

    mgmt_events.mgmt_remove(snode)
    logging.info("done")


def show_cluster():
    c = utils.get_docker_client()
    nl = c.nodes.list(filters={'role': 'manager'})
    nodes = []
    for n in nl:
        nodes.append({
            "Hostname": n.attrs['Description']['Hostname'],
            "IP": n.attrs['ManagerStatus']['Addr'].split(":")[0],
            "Status": n.attrs['Status']['State'],
            "UpdatedAt": datetime.datetime.strptime(n.attrs['UpdatedAt'][:26], "%Y-%m-%dT%H:%M:%S.%f").strftime(
                "%H:%M:%S, %d/%m/%Y"),
        })
    return utils.print_table(nodes)


def cluster_status():
    c = utils.get_docker_client()
    ns = c.nodes.list(filters={'role': 'manager'})
    total_nodes = len(ns)
    active_nodes = 0
    lead_node = None
    for n in ns:
        if n.attrs['Status']['State'] == 'ready':
            active_nodes += 1
        if 'Leader' in n.attrs['ManagerStatus'] and n.attrs['ManagerStatus']['Leader']:
            lead_node = n.attrs['Description']['Hostname']

    status = {
        "Status": "Online",
        "Active Nodes": active_nodes,
        "Total nodes": total_nodes,
        "Leader": lead_node
    }

    return utils.print_table([status])
