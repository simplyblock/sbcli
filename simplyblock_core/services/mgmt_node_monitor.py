# coding=utf-8

import time
import os
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import mgmt_events, health_controller
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


def _set_mgmt_node_status(node, from_status, to_status):
    # Atomic compare-and-set so a full read-modify-write never clobbers a
    # concurrent change to another field of the mgmt node row (the same
    # lost-update class as incident 2026-06-18). Only flips when the row is
    # still in `from_status` on the freshly-read value.
    if node.status != from_status:
        return
    now = str(datetime.now())
    outcome = {"old": None, "changed": False}

    def _mut(n):
        if n.status != from_status:
            return False
        outcome["old"] = n.status
        outcome["changed"] = True
        n.status = to_status
        n.updated_at = now
        return True

    snode = db.atomic_update(db.get_mgmt_node_by_id(node.get_id()), _mut)
    if snode is not None and outcome["changed"]:
        mgmt_events.status_change(snode, snode.status, outcome["old"], caused_by="monitor")


def set_node_online(node):
    _set_mgmt_node_status(node, MgmtNode.STATUS_UNREACHABLE, MgmtNode.STATUS_ONLINE)


def set_node_offline(node):
    _set_mgmt_node_status(node, MgmtNode.STATUS_ONLINE, MgmtNode.STATUS_UNREACHABLE)


def main():
    logger.info("Starting Mgmt node monitor")

    backend_type = os.getenv("BACKEND_TYPE", "docker").lower()
    backend: NodeBackend

    if backend_type == "docker":
        backend = DockerNodeBackend()
    elif backend_type == "k8s":
        backend = K8sNodeBackend()
    else:
        raise ValueError(f"Unsupported BACKEND_TYPE '{backend_type}', use 'docker' or 'k8s'")

    logger.info(f"Using backend: {backend_type}")

    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue

        try:
            nodes = db.get_mgmt_nodes()
            reachable_ips = set(backend.get_reachable_nodes())
        except Exception as e:
            logger.error(f"Failed to enumerate mgmt nodes / reachable IPs: {e}")
            time.sleep(3)
            continue

        for node in nodes:
            # Per-node isolation: a failure on one mgmt node must not abort the
            # sweep over the remaining nodes for this tick.
            try:
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

                if node.mgmt_ip in reachable_ips:
                    set_node_online(node)
                else:
                    set_node_offline(node)
            except Exception as e:
                logger.error(f"Mgmt node monitor failed for node {node.get_id()}: {e}")
                logger.exception(e)

        logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
        time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
