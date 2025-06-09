# coding=utf-8
import os

import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import mgmt_events
from simplyblock_core.models.mgmt_node import MgmtNode


logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()


# ----- Backend Abstractions -----

class NodeBackend:
    def get_reachable_nodes(self) -> list[str]:
        raise NotImplementedError


class DockerNodeBackend(NodeBackend):
    def get_reachable_nodes(self) -> list[str]:
        client = utils.get_docker_client()
        reachable = []
        for node in client.nodes.list(filters={"role": "manager"}):
            addr = node.attrs["ManagerStatus"]["Addr"].split(":")[0]
            if node.attrs["ManagerStatus"]["Reachability"] == "reachable":
                reachable.append(addr)
        return reachable


class K8sNodeBackend(NodeBackend):
    def __init__(self):
        from kubernetes import client, config
        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.v1 = client.CoreV1Api()

    def get_reachable_nodes(self) -> list[str]:
        reachable = []
        nodes = self.v1.list_node().items
        for node in nodes:
            ready = any(c.status == "True" and c.type == "Ready" for c in node.status.conditions)
            if ready:
                for addr in node.status.addresses:
                    if addr.type == "InternalIP":
                        reachable.append(addr.address)
        return reachable


# ----- Backend Selection -----

backend_type = os.getenv("BACKEND_TYPE", "docker").lower()
if backend_type == "docker":
    backend = DockerNodeBackend()
elif backend_type == "k8s":
    backend = K8sNodeBackend()
else:
    raise ValueError(f"Unsupported BACKEND_TYPE '{backend_type}', use 'docker' or 'k8s'")

logger.info(f"Using backend: {backend_type}")

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


def ping_host(ip):
    logger.info(f"Pinging ip {ip}")
    response = os.system(f"ping -c 1 {ip}")
    if response == 0:
        logger.info(f"{ip} is UP")
        return True
    else:
        logger.info(f"{ip} is DOWN")
        return False


logger.info("Starting Mgmt node monitor")


while True:
    # get storage nodes
    nodes = db.get_mgmt_nodes()
    reachable_ips = set(backend.get_reachable_nodes())

    for node in nodes:
        if node.status not in [MgmtNode.STATUS_ONLINE, MgmtNode.STATUS_UNREACHABLE]:
            logger.info(f"Node status is: {node.status}, skipping")
            continue

        logger.info(f"Checking node {node.hostname}")
        if not ping_host(node.mgmt_ip):
            logger.info(f"Node {node.hostname} is offline")
            set_node_offline(node)
            continue

        if node.mgmt_ip in reachable_ips:
            set_node_online(node)
        else:
            set_node_offline(node)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
