# coding=utf-8
import time
from datetime import datetime

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_events, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule


logger = utils.get_logger(__name__)

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


def task_runner(task):

    snode = db_controller.get_storage_node_by_id(task.node_id)
    if not snode:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = f"Node not found: {task.node_id}"
        task.write_to_db(db_controller.kv_store)
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db_controller.kv_store)
        tasks_events.task_updated(task)

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db_controller.kv_store)
        return False

    if snode.online_since:
        diff = datetime.now() - datetime.fromisoformat(snode.online_since)
        if diff.total_seconds() < 60:
            task.function_result = "node is online < 1 min, retrying"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.retry += 1
            task.write_to_db(db_controller.kv_store)
            return False

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password,
                           timeout=5, retry=2)
    if "migration" not in task.function_params:
        all_devs_online = True
        for node in db_controller.get_storage_nodes_by_cluster_id(task.cluster_id):
            for dev in node.nvme_devices:
                if dev.status not in [NVMeDevice.STATUS_ONLINE,
                                      NVMeDevice.STATUS_FAILED,
                                      NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                    all_devs_online = False
                    break

        if not all_devs_online:
            task.function_result = "Some devs are offline, retrying"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.retry += 1
            task.write_to_db(db_controller.kv_store)
            return False

        device = db_controller.get_storage_device_by_id(task.device_id)
        distr_name = task.function_params["distr_name"]

        if not device:
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "Device not found"
            task.write_to_db(db_controller.kv_store)
            return True

        rsp = rpc_client.distr_migration_to_primary_start(device.cluster_device_order, distr_name)
        if not rsp:
            logger.error(f"Failed to start device migration task, storage_ID: {device.cluster_device_order}")
            task.function_result = "Failed to start device migration task"
            task.retry += 1
            task.write_to_db(db_controller.kv_store)
            return False
        task.function_params['migration'] = {
            "name": distr_name}
        task.write_to_db(db_controller.kv_store)
        time.sleep(3)

    if "migration" in task.function_params:
        mig_info = task.function_params["migration"]
        res = rpc_client.distr_migration_status(**mig_info)
        for st in res:
            if st['status'] == "completed":
                if st['error'] != 0:
                    task.function_result = "mig completed with errors, retrying"
                    task.retry += 1
                    task.status = JobSchedule.STATUS_SUSPENDED
                    del task.function_params['migration']
                else:
                    task.status = JobSchedule.STATUS_DONE
                    task.function_result = "Done"

                task.write_to_db(db_controller.kv_store)
                return True

            task.function_result = f"progress: {st['progress']}%, errors: {st['error'] }"
            task.write_to_db(db_controller.kv_store)

    task.retry += 1
    task.write_to_db(db_controller.kv_store)
    return False


# get DB controller
db_controller = db_controller.DBController()

logger.info("Starting Tasks runner...")
while True:
    time.sleep(3)
    clusters = db_controller.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db_controller.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                delay_seconds = 3
                if task.function_name == JobSchedule.FN_DEV_MIG:
                    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                        active_task = tasks_controller.get_active_node_mig_task(task.cluster_id, task.node_id)
                        if active_task:
                            logger.info("task found on same node, retry")
                            continue
                    if task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db_controller.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            tasks_events.task_updated(task)

                        time.sleep(delay_seconds)
