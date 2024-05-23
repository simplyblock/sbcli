# coding=utf-8
import logging
import os

import time
import sys
import uuid

from simplyblock_core import constants, kv_store
from simplyblock_core.models.job_schedule import JobSchedule
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


def add_to_auto_restart(device):
    tasks = db_controller.get_job_tasks(device.cluster_id)
    for task in tasks:
        if task.device_id == device.get_id():
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return

    ds = JobSchedule()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_id = device.cluster_id
    ds.node_id = device.node_id
    ds.device_id = device.get_id()
    ds.date = int(time.time())
    ds.function_name = "device_restart"
    ds.status = 'new'

    ds.write_to_db(db_store)
    return ds.get_id()


# def set_dev_status(device, status):
#     node = db_controller.get_storage_node_by_id(device.node_id)
#     if node.status != StorageNode.STATUS_ONLINE:
#         logger.error(f"Node is not online, {node.get_id()}, status: {node.status}, "
#                      f"skipping device status change")
#         return
#
#     for dev in node.nvme_devices:
#         if dev.get_id() == device.get_id():
#             if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
#                 device_controller.device_set_state(device.get_id(), status)
#             break
#     return


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

            if dev.io_error and dev.status == NVMeDevice.STATUS_UNAVAILABLE:
                logger.info("Adding device to auto restart")
                add_to_auto_restart(dev)

            # ret = health_controller.check_device(dev.get_id())
            # logger.info(f"Device: {dev.get_id()}, is healthy: {ret}")
            # if ret:
            #     set_dev_status(dev, NVMeDevice.STATUS_ONLINE)
            # else:
            #     set_dev_status(dev, NVMeDevice.STATUS_UNAVAILABLE)

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
