# coding=utf-8
import time

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, tasks_events, health_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)

# get DB controller
db_controller = db_controller.DBController()

utils.init_sentry_sdk()


def _get_node_unavailable_devices_count(node_id):
    node = db_controller.get_storage_node_by_id(node_id)
    devices = []
    for dev in node.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            devices.append(dev)
    return len(devices)


def _get_device(task):
    node = db_controller.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.get_id() == task.device_id:
            return dev


def _validate_no_task_node_restart(cluster_id, node_id):
    tasks = db_controller.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def task_runner(task):
    if task.function_name == JobSchedule.FN_DEV_RESTART:
        return task_runner_device(task)
    if task.function_name == JobSchedule.FN_NODE_RESTART:
        return task_runner_node(task)


def task_runner_device(task):
    device = _get_device(task)

    if task.retry >= constants.TASK_EXEC_RETRY_COUNT:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    if not _validate_no_task_node_restart(task.cluster_id, task.node_id):
        task.function_result = "canceled: node restart found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    node = db_controller.get_storage_node_by_id(task.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online: {node.get_id()}, retry")
        task.function_result = "Node is offline"
        task.retry += 1
        task.write_to_db(db_controller.kv_store)
        return False

    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "Device is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Device is not unavailable: {device.get_id()}, {device.status} , stopping task")
        task.function_result = f"stopped because dev is {device.status}"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db_controller.kv_store)

    # set device online for the first 3 retries
    if task.retry < 3:
        logger.info(f"Set device online {device.get_id()}")
        device_controller.device_set_io_error(device.get_id(), False)
        device_controller.device_set_state(device.get_id(), NVMeDevice.STATUS_ONLINE)
    else:
        logger.info(f"Restarting device {device.get_id()}")
        device_controller.restart_device(device.get_id(), force=True)

    # check device status
    time.sleep(5)
    device = _get_device(task)
    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)

        tasks_controller.add_device_mig_task(device.get_id())
        return True

    task.retry += 1
    task.write_to_db(db_controller.kv_store)
    return False


def task_runner_node(task):
    node = db_controller.get_storage_node_by_id(task.node_id)
    if task.retry >= task.max_retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        storage_node_ops.set_node_status(task.node_id, StorageNode.STATUS_OFFLINE)
        return True

    if not node:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if node.status in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN]:
        logger.info(f"Node is {node.status}, stopping task")
        task.function_result = f"Node is {node.status}, stopping"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if _get_node_unavailable_devices_count(node.get_id()) == 0 and node.status == StorageNode.STATUS_ONLINE:
        logger.info(f"Node is online: {node.get_id()}")
        task.function_result = "Node is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        if node.status == StorageNode.STATUS_RESTARTING:
            logger.info(f"Node is restarting, stopping task")
            task.function_result = f"Node is restarting"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db_controller.kv_store)
            return True
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db_controller.kv_store)

    # is node reachable?
    ping_check = health_controller._check_node_ping(node.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
    node_api_check = health_controller._check_node_api(node.mgmt_ip)
    logger.info(f"Check: node API {node.mgmt_ip}:5000 ... {node_api_check}")
    if not ping_check or not node_api_check:
        # node is unreachable, retry
        logger.info(f"Node is not reachable: {task.node_id}, retry")
        task.function_result = f"Node is unreachable, retry"
        task.retry += 1
        task.write_to_db(db_controller.kv_store)
        return False


    # shutting down node
    logger.info(f"Shutdown node {node.get_id()}")
    ret = storage_node_ops.shutdown_storage_node(node.get_id(), force=True)
    if ret:
        logger.info(f"Node shutdown succeeded")

    time.sleep(3)

    # resetting node
    logger.info(f"Restart node {node.get_id()}")
    ret = storage_node_ops.restart_storage_node(node.get_id(), force=True)
    if ret:
        logger.info(f"Node restart succeeded")

    time.sleep(3)
    node = db_controller.get_storage_node_by_id(task.node_id)
    if _get_node_unavailable_devices_count(node.get_id()) == 0 and node.status == StorageNode.STATUS_ONLINE:
        logger.info(f"Node is online: {node.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    task.retry += 1
    task.write_to_db(db_controller.kv_store)
    return False


logger.info("Starting Tasks runner...")
while True:
    clusters = db_controller.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db_controller.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                if task.function_name in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_NODE_RESTART]:
                    while task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db_controller.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            if task.status == JobSchedule.STATUS_DONE:
                                tasks_events.task_updated(task)
                                break
                        else:
                            if task.retry <= 3:
                                delay_seconds *= 1
                            else:
                                delay_seconds *= 2
                        time.sleep(delay_seconds)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)