# coding=utf-8
import logging
import os

import time
import sys


from simplyblock_core import constants, kv_store, storage_node_ops
from simplyblock_core.controllers import health_controller,  device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

# Import the GELF logger
from graypy import GELFUDPHandler


def set_dev_status(device, status):
    node = db_controller.get_storage_node_by_id(device.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online, {node.get_id()}, status: {node.status}, "
                     f"skipping device status change")
        return

    for dev in node.nvme_devices:
        if dev.get_id() == device.get_id():
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                device_controller.device_set_state(device.get_id(), status)
            break
    return


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()


logger.info("Starting Device monitor...")
while True:
    nodes = db_controller.get_storage_nodes()
    for node in nodes:
        if node.status != StorageNode.STATUS_ONLINE:
            logger.warning(f"Node status is not online, id: {node.get_id()}, status: {node.status}")
            continue
        for dev in node.nvme_devices:
            if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                logger.warning(f"Device status is not online or unavailable, id: {dev.get_id()}, status: {dev.status}")
                continue
            if dev.io_error:
                logger.debug(f"Skipping Device check because of io_error {dev.get_id()}")
                continue

            ret = health_controller.check_device(dev.get_id())
            logger.info(f"Device: {dev.get_id()}, is healthy: {ret}")
            # if ret:
            #     set_dev_status(dev, NVMeDevice.STATUS_ONLINE)
            # else:
            #     set_dev_status(dev, NVMeDevice.STATUS_UNAVAILABLE)

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
