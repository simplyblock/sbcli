# coding=utf-8
import logging
import time

from management import constants
from management import utils
from management.kv_store import DBController
from management.models.compute_node import ComputeNode
from management.models.nvme_device import NVMeDevice
from management.models.storage_node import StorageNode
from management import services
from management import spdk_installer
from management.pci_utils import bind_spdk_driver, get_nvme_devices, bind_nvme_driver
from management.rpc_client import RPCClient
from management.storage_node_ops import _get_nvme_list, _run_nvme_smart_log, _run_nvme_smart_log_add

logger = logging.getLogger()



def shutdown_storage_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_ONLINE:
        logging.error("Node is not in online state")
        exit(1)

    logging.info("Shutting down node")
    snode.status = StorageNode.STATUS_IN_SHUTDOWN
    snode.write_to_db(kv_store)

    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    logger.info("Stopping spdk_nvmf_tgt service")
    nvmf_service = services.spdk_nvmf_tgt
    if nvmf_service.is_service_running():
        nvmf_service.service_stop()

    # make shutdown request
    response = rpc_client.shutdown_node(snode.get_id())
    if 'result' in response and response['result']:
        logging.info("Setting node status to Offline")
        snode.status = StorageNode.STATUS_OFFLINE
        snode.write_to_db(kv_store)
        logger.info("Done")
        return True
    else:
        logger.error("Error shutting down node")
        logger.debug(response)
        exit(1)


def suspend_storage_node(kv_store):
    #  in this case all process must be running
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_ONLINE:
        logging.error("Node is not in online state")
        exit(1)

    logging.info("Suspending node")

    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # make suspend request
    response = rpc_client.suspend_node(snode.get_id())
    if 'result' in response and response['result']:
        logging.info("Setting node status to suspended")
        snode.status = StorageNode.STATUS_SUSPENDED
        snode.write_to_db(kv_store)
        logger.info("Done")
        return True
    else:
        logger.error("Error suspending node")
        logger.debug(response)
        exit(1)


def resume_storage_node(kv_store):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    logging.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_SUSPENDED:
        logging.error("Node is not in suspended state")
        exit(1)

    logging.info("Resuming node")

    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # make suspend request
    response = rpc_client.resume_node(snode.get_id())
    if 'result' in response and response['result']:
        logging.info("Setting node status to online")
        snode.status = StorageNode.STATUS_ONLINE
        snode.write_to_db(kv_store)
        logger.info("Done")
        return True
    else:
        logger.error("Error suspending node")
        logger.debug(response)
        exit(1)
