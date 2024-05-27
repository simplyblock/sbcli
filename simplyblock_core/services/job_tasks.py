# coding=utf-8
import logging
import time
import sys


from simplyblock_core import constants, kv_store
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice

# Import the GELF logger
from graypy import GELFUDPHandler

from simplyblock_core.models.storage_node import StorageNode


def _get_device(task):
    node = db_controller.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.get_id() == task.device_id:
            return dev


def task_runner(task):
    device = _get_device(task)

    if task.retry <= 0:
        task.function_result = "timeout"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        return

    node = db_controller.get_storage_node_by_id(task.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online: {node.get_id()} , skipping task: {task.get_id()}")
        task.function_result = "Node is offline"
        task.retry -= 1
        task.write_to_db(db_controller.kv_store)
        return

    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}, no restart needed")
        task.function_result = "skipped because dev is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return

    task.status = JobSchedule.STATUS_RUNNING
    task.write_to_db(db_controller.kv_store)

    # resetting device
    logger.info(f"Resetting device {device.get_id()}")
    res = device_controller.reset_storage_device(device.get_id())
    time.sleep(5)
    device = _get_device(task)
    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return

    logger.info(f"Restarting device {device.get_id()}")
    res = device_controller.restart_device(device.get_id(), force=True)
    time.sleep(5)
    device = _get_device(task)
    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return

    task.retry -= 1
    task.write_to_db(db_controller.kv_store)
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
db_controller = kv_store.DBController()

logger.info("Starting Jobs runner...")
while True:

    clusters = db_controller.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db_controller.get_job_tasks(cl.get_id())
            for task in tasks:
                if task.status != JobSchedule.STATUS_DONE:
                    res = task_runner(task)

    time.sleep(5)
