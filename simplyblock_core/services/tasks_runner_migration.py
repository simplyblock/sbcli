# coding=utf-8
import time


from simplyblock_core import constants, kv_store, utils
from simplyblock_core.controllers import tasks_events
from simplyblock_core.models.job_schedule import JobSchedule


logger = utils.get_logger(__name__)

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


def task_runner(task):

    snode = db_controller.get_storage_node_by_id(task.node_id)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=5, retry=2)

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db_controller.kv_store)
        return True

    if task.status == JobSchedule.STATUS_NEW:
        active_task_node_ids.append(task.node_id)
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db_controller.kv_store)
        tasks_events.task_updated(task)

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.retry += 1
        task.write_to_db(db_controller.kv_store)
        return False

    if "migration" not in task.function_params:
        if task.retry >= 2:
            all_devs_online = True
            for node in db_controller.get_storage_nodes_by_cluster_id(task.cluster_id):
                for dev in node.nvme_devices:
                    if dev.status != NVMeDevice.STATUS_ONLINE:
                        all_devs_online = False
                        break

            if not all_devs_online:
                task.function_result = "Some devs are offline, retrying"
                task.retry += 1
                task.write_to_db(db_controller.kv_store)
                return False

        device = db_controller.get_storage_devices(task.device_id)
        lvol = db_controller.get_lvol_by_id(task.function_params["lvol_id"])

        rsp = rpc_client.distr_migration_to_primary_start(device.cluster_device_order, lvol.base_bdev)
        if not rsp:
            logger.error(f"Failed to start device migration task, storage_ID: {device.cluster_device_order}")
            task.function_result = "Failed to start device migration task"
            task.retry += 1
            task.write_to_db(db_controller.kv_store)
            return False
        task.function_params['migration'] = {
            "name": lvol.base_bdev}
        task.write_to_db(db_controller.kv_store)
        time.sleep(3)

    if "migration" in task.function_params:
        mig_info = task.function_params["migration"]
        res = rpc_client.distr_migration_status(**mig_info)
        for st in res:
            if st['status'] == "completed":
                task.status = JobSchedule.STATUS_DONE
                task.function_result = "Done"
                if st['error'] == 1:
                    task.function_result = "mig completed with errors"
                task.write_to_db(db_controller.kv_store)
                return True

            task.function_result = f"progress: {st['progress']}%, errors: {st['error'] }"
            task.write_to_db(db_controller.kv_store)

    task.retry += 1
    task.write_to_db(db_controller.kv_store)
    return False


# get DB controller
db_controller = kv_store.DBController()
active_task_node_ids = []
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
                delay_seconds = 5
                if task.function_name == JobSchedule.FN_DEV_MIG:
                    if task.status == JobSchedule.STATUS_NEW and task.node_id in active_task_node_ids:
                        continue
                    if task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db_controller.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            tasks_events.task_updated(task)
                            if task.node_id in active_task_node_ids:
                                active_task_node_ids.remove(task.node_id)
                        else:
                            time.sleep(delay_seconds)
