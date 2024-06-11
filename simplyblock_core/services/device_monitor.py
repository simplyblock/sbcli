# coding=utf-8
import logging
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


def add_device_to_auto_restart(device):
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


def add_node_to_auto_restart(node):
    tasks = db_controller.get_job_tasks(node.cluster_id)
    for task in tasks:
        if task.node_id == node.get_id():
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return

    ds = JobSchedule()
    ds.uuid = str(uuid.uuid4())
    ds.cluster_id = node.cluster_id
    ds.node_id = node.get_id()
    ds.date = int(time.time())
    ds.function_name = "node_restart"
    ds.status = 'new'

    ds.write_to_db(db_store)
    return ds.get_id()


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

        if len(auto_restart_devices) == 1:
            add_device_to_auto_restart(auto_restart_devices[0])
        elif len(auto_restart_devices) >= 2 and len(online_devices) == 0:
            add_node_to_auto_restart(node)

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
