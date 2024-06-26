# coding=utf-8
import logging
import time
import sys
import uuid

from simplyblock_core import constants, kv_store
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

# Import the GELF logger
from graypy import GELFUDPHandler


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
        auto_restart_devices = []
        online_devices = []
        if node.status != StorageNode.STATUS_ONLINE:
            logger.warning(f"Node status is not online, id: {node.get_id()}, status: {node.status}")
            continue
        for dev in node.nvme_devices:
            if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                logger.warning(f"Device status is not online or unavailable, id: {dev.get_id()}, status: {dev.status}")
                continue

            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices.append(dev)

            if dev.io_error and dev.status == NVMeDevice.STATUS_UNAVAILABLE and not dev.retries_exhausted:
                logger.info("Adding device to auto restart")
                auto_restart_devices.append(dev)

        if len(auto_restart_devices) >= 2 or len(online_devices) == 0:
            tasks_controller.add_node_to_auto_restart(node)
        elif len(auto_restart_devices) == 1:
            tasks_controller.add_device_to_auto_restart(auto_restart_devices[0])

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
