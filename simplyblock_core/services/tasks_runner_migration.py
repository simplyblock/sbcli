# coding=utf-8
import time
from datetime import datetime, timezone

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_events, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

logger = utils.get_logger(__name__)


def task_runner(task):

    task = db.get_task_by_id(task.uuid)
    snode = db.get_storage_node_by_id(task.node_id)
    if not snode:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = f"Node not found: {task.node_id}"
        task.write_to_db(db.kv_store)
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    cluster = db.get_cluster_by_id(task.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
        task.function_result = "cluster is not active, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        for node in db.get_storage_nodes_by_cluster_id(task.cluster_id):
            if node.is_secondary_node:  # pass
                continue

            if node.status == StorageNode.STATUS_ONLINE and node.online_since:
                try:
                    diff = datetime.now(timezone.utc) - datetime.fromisoformat(node.online_since)
                    if diff.total_seconds() < 60:
                        task.function_result = "node is online < 1 min, retrying"
                        task.status = JobSchedule.STATUS_SUSPENDED
                        task.retry += 1
                        task.write_to_db(db.kv_store)
                        return False
                except Exception as e:
                    logger.error(f"Failed to get online since: {e}")

            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_UNAVAILABLE]:
                    task.function_result = f"Some dev status is {dev.status }, retrying"
                    task.status = JobSchedule.STATUS_SUSPENDED
                    task.retry += 1
                    task.write_to_db(db.kv_store)
                    return False

        task.status = JobSchedule.STATUS_RUNNING
        task.function_result = ""
        task.write_to_db(db.kv_store)


    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password,
                           timeout=5, retry=2)
    if "migration" not in task.function_params:

        device = db.get_storage_device_by_id(task.device_id)
        distr_name = task.function_params["distr_name"]

        if not device:
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "Device not found"
            task.write_to_db(db.kv_store)
            return True

        qos_high_priority = False
        if db.get_cluster_by_id(snode.cluster_id).enable_qos:
            qos_high_priority = True
        rsp = rpc_client.distr_migration_expansion_start(distr_name, qos_high_priority)
        if not rsp:
            logger.error(f"Failed to start device migration task, storage_ID: {device.cluster_device_order}")
            task.function_result = "Failed to start device migration task, retry later"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.retry += 1
            task.write_to_db(db.kv_store)
            return True
        task.function_params['migration'] = {
            "name": distr_name}
        task.write_to_db(db.kv_store)
        # time.sleep(1)

    try:
        if "migration" in task.function_params:
            mig_info = task.function_params["migration"]
            res = rpc_client.distr_migration_status(**mig_info)
            return utils.handle_task_result(task, res)
    except Exception as e:
        logger.error("Failed to get migration task status")
        logger.exception(e)
        task.function_result = "Failed to get migration status"

    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


# get DB controller
db = db_controller.DBController()

logger.info("Starting Tasks runner...")


def update_master_task(task):
    master_task = None
    tasks = {t.uuid: t for t in db.get_job_tasks(cl.get_id(), reverse=False)}
    for t in tasks.values():
        if task.uuid in t.sub_tasks:
            master_task = t
            break

    def _set_master_task_status(master_task, status):
        if master_task.status != status:
            logger.info(f"_set_master_task_status: {status}")
            master_task.status = status
            master_task.function_result = status
            master_task.write_to_db(db.kv_store)
            tasks_events.task_updated(master_task)

    status_map = {
        JobSchedule.STATUS_DONE: 0,
        JobSchedule.STATUS_NEW: 0,
        JobSchedule.STATUS_SUSPENDED: 0,
        JobSchedule.STATUS_RUNNING: 0,
    }
    if master_task:
        for sub_task_id in master_task.sub_tasks:
            sub_task = tasks[sub_task_id]
            status_map[sub_task.status] = status_map.get(sub_task.status, 0) + 1

        logger.info(f"master_task.sub_tasks: {len(master_task.sub_tasks)}")
        logger.info(f"status_map: {status_map}")

        if status_map[JobSchedule.STATUS_DONE] == len(master_task.sub_tasks):  # all tasks done
            _set_master_task_status(master_task, JobSchedule.STATUS_DONE)
        elif status_map[JobSchedule.STATUS_NEW] == len(master_task.sub_tasks):  # all tasks new
            _set_master_task_status(master_task, JobSchedule.STATUS_NEW)
        elif status_map[JobSchedule.STATUS_SUSPENDED] == len(master_task.sub_tasks):  # all tasks suspended
            _set_master_task_status(master_task, JobSchedule.STATUS_SUSPENDED)
        else:  # set running
            _set_master_task_status(master_task, JobSchedule.STATUS_RUNNING)
        return True


while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                if task.function_name == JobSchedule.FN_DEV_MIG and task.status != JobSchedule.STATUS_DONE:
                    task = db.get_task_by_id(task.uuid)
                    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                        active_task = False
                        suspended_task= False
                        for t in db.get_job_tasks(task.cluster_id):
                            if t.function_name in [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_DEV_MIG,
                                                      JobSchedule.FN_NEW_DEV_MIG] and t.node_id == task.node_id:
                                if "distr_name" in t.function_params and t.function_params[
                                    "distr_name"] == task.function_params["distr_name"] and t.canceled is False:
                                    if t.status == JobSchedule.STATUS_RUNNING:
                                        active_task = True
                                    elif t.status == JobSchedule.STATUS_SUSPENDED and t.function_name == JobSchedule.FN_NEW_DEV_MIG:
                                        suspended_task = True
                            if active_task and suspended_task:
                                break
                        if active_task or suspended_task:
                            logger.info("task found on same node, retry")
                            continue
                    elif task.status == JobSchedule.STATUS_RUNNING:
                        pass

                    res = task_runner(task)
                    update_master_task(task)
                    if res:
                        node_task = tasks_controller.get_active_node_tasks(task.cluster_id, task.node_id)
                        if not node_task:
                            logger.info("no task found on same node, resuming compression")
                            node = db.get_storage_node_by_id(task.node_id)
                            rpc_client = RPCClient(
                                node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=5, retry=2)
                            ret = rpc_client.jc_suspend_compression(jm_vuid=node.jm_vuid, suspend=False)
                            if not ret:
                                logger.error("Failed to resume JC compression")

    time.sleep(3)
