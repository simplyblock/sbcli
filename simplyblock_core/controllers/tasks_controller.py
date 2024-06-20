# coding=utf-8
import logging
import time
import uuid

from simplyblock_core import kv_store
from simplyblock_core.controllers import tasks_events
from simplyblock_core.models.job_schedule import JobSchedule

logger = logging.getLogger()
db_controller = kv_store.DBController()


def _validate_new_task_node_restart(cluster_id, node_id):
    tasks = db_controller.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _validate_new_task_dev_restart(cluster_id, node_id, device_id):
    tasks = db_controller.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_RESTART and task.device_id == device_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
        elif task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _add_task(function_name, cluster_id, node_id, device_id):

    if function_name in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_DEV_MIG]:
        if not _validate_new_task_dev_restart(cluster_id, node_id, device_id):
            return False
    elif function_name == JobSchedule.FN_NODE_RESTART:
        if not _validate_new_task_node_restart(cluster_id, node_id):
            return False

    task_obj = JobSchedule()
    task_obj.uuid = str(uuid.uuid4())
    task_obj.cluster_id = cluster_id
    task_obj.node_id = node_id
    task_obj.device_id = device_id
    task_obj.date = int(time.time())
    task_obj.function_name = function_name
    task_obj.status = JobSchedule.STATUS_NEW
    task_obj.write_to_db(db_controller.kv_store)
    tasks_events.task_create(task_obj)
    return task_obj.uuid


def add_device_mig_task(device_id):
    device = db_controller.get_storage_devices(device_id)
    return _add_task(JobSchedule.FN_DEV_MIG, device.cluster_id, device.node_id, device.get_id())


def add_device_to_auto_restart(device):
    return _add_task(JobSchedule.FN_DEV_RESTART, device.cluster_id, device.node_id, device.get_id())


def add_node_to_auto_restart(node):
    return _add_task(JobSchedule.FN_NODE_RESTART, node.cluster_id, node.get_id(), "")
