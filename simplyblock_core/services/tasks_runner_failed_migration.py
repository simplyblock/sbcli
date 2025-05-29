# coding=utf-8
import logging
import time
import sys
from datetime import datetime

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_events, tasks_controller, device_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule



from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


def task_runner(task):

    snode = db.get_storage_node_by_id(task.node_id)
    if not snode:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = f"Node not found: {task.node_id}"
        task.write_to_db(db.kv_store)
        return True

    cluster = db.get_cluster_by_id(task.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
        task.function_result = "cluster is not active, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status in [JobSchedule.STATUS_NEW ,JobSchedule.STATUS_SUSPENDED]:
        if task.status == JobSchedule.STATUS_NEW:
            for node in db.get_storage_nodes_by_cluster_id(task.cluster_id):
                if node.online_since:
                    try:
                        diff = datetime.now() - datetime.fromisoformat(node.online_since)
                        if diff.total_seconds() < 60:
                            task.function_result = "node is online < 1 min, retrying"
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.retry += 1
                            task.write_to_db(db.kv_store)
                            return False
                    except Exception as e:
                        logger.error(f"Failed to get online since: {e}")

        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    active_f_task = tasks_controller.get_new_device_mig_task_for_device(task.cluster_id)
    if active_f_task:
        msg = "dev expansion task found, retry"
        logger.info(msg)
        task.function_result = msg
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

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
        rsp = rpc_client.distr_migration_failure_start(distr_name, device.cluster_device_order, qos_high_priority)
        if not rsp:
            logger.error(f"Failed to start device migration task, storage_ID: {device.cluster_device_order}")
            task.function_result = "Failed to start device migration task"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return False

        task.function_params['migration'] = {"name": distr_name}
        task.write_to_db(db.kv_store)
        time.sleep(3)

    try:
        if "migration" in task.function_params:
            mig_info = task.function_params["migration"]
            res = rpc_client.distr_migration_status(**mig_info)
            out = utils.handle_task_result(task, res)
            dev_failed_task = tasks_controller.get_failed_device_mig_task(task.cluster_id, task.device_id)
            if not dev_failed_task:
                device_controller.device_set_failed_and_migrated(task.device_id)

            return out
    except Exception as e:
        logger.error(f"Failed to get migration task status")
        logger.exception(e)
        task.function_result = "Failed to get migration status"

    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()
logger.info("Starting Tasks runner...")
while True:
    time.sleep(3)
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                if task.function_name == JobSchedule.FN_FAILED_DEV_MIG:
                    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                        active_task = tasks_controller.get_active_node_mig_task(
                            task.cluster_id, task.node_id, task.function_params["distr_name"])
                        if active_task:
                            logger.info("task found on same node, retry")
                            continue
                    if task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            tasks_events.task_updated(task)
                        else:
                            time.sleep(3)
