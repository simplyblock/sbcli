# coding=utf-8
import logging

import json

from simplyblock_core import utils
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.compute_node import ComputeNode

logger = logging.getLogger()


def add_compute_node(kv_store):
    db_controller = DBController(kv_store)
    global_settings = db_controller.get_global_settings()
    logging.info("Add compute node")
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if cnode:
        logger.error("Node already exists")
        exit(1)

    # todo: add services

    logging.info("Setting node status to Active")
    cnode.status = ComputeNode.STATUS_ONLINE
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return cnode


def reset_compute_node(kv_store):
    db_controller = DBController(kv_store)
    logging.info("Reset compute node")
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if not cnode:
        logger.error("This compute node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", cnode.hostname, cnode.status)
    if cnode.status in [ComputeNode.STATUS_ONLINE, ComputeNode.STATUS_IN_CREATION, ComputeNode.STATUS_REMOVED]:
        logging.error("Can not reset node in state: %s", cnode.status)
        exit(1)

    # todo: reset services

    logging.info("Setting node status to Active")
    cnode.status = ComputeNode.STATUS_ONLINE
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return True


def remove_compute_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if not cnode:
        logger.error("This compute node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", cnode.hostname, cnode.status)
    if cnode.status != ComputeNode.STATUS_OFFLINE:
        logging.error("Node is not in offline state")
        exit(1)

    logging.info("Removing node")

    # todo: remove services

    logging.info("Setting node status to removed")
    cnode.status = ComputeNode.STATUS_REMOVED
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return True


def suspend_compute_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if not cnode:
        logger.error("This compute node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", cnode.hostname, cnode.status)
    if cnode.status != ComputeNode.STATUS_ONLINE:
        logging.error("Node is not in online state")
        exit(1)

    logging.info("Suspending node")

    # todo: suspend node

    logging.info("Setting node status to suspended")
    cnode.status = ComputeNode.STATUS_SUSPENDED
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return True


def resume_compute_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if not cnode:
        logger.error("This compute node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", cnode.hostname, cnode.status)
    if cnode.status != ComputeNode.STATUS_SUSPENDED:
        logging.error("Node is not in suspended state")
        exit(1)

    logging.info("Resuming node")

    # todo: resuming node

    logging.info("Setting node status to online")
    cnode.status = ComputeNode.STATUS_ONLINE
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return True


def shutdown_compute_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    cnode = db_controller.get_compute_node_by_id(baseboard_sn)
    if not cnode:
        logger.error("This compute node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", cnode.hostname, cnode.status)
    if cnode.status != ComputeNode.STATUS_ONLINE:
        logging.error("Node is not in online state")
        exit(1)

    logging.info("Shutting down node")

    # todo: shutting down node

    logging.info("Setting node status to in shutdown")
    cnode.status = ComputeNode.STATUS_IN_SHUTDOWN
    cnode.write_to_db(kv_store)
    logger.info("Done")
    return True


def list_compute_nodes(kv_store, is_json):
    db_controller = DBController(kv_store)
    nodes = db_controller.get_compute_nodes()
    data = []

    for node in nodes:
        data.append({
            "baseboard_sn": node.baseboard_sn,
            "create_dt": node.create_dt,
            "host_nqn": node.host_nqn,
            "hostname": node.hostname,
            "mgmt_ip": node.mgmt_ip,
            "status": node.status,
            "subsystem": node.subsystem,
            "system_uuid": node.system_uuid})

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)
