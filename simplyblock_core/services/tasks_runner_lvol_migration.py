# coding=utf-8
import time
from datetime import datetime, timezone

from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.controllers.lvol_migration_controller import \
    MigrationController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule



from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


def task_runner(task):
    try:
        snode = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = f"Node not found: {task.node_id}"
        task.write_to_db(db.kv_store)
        return True

    migration_id = task.function_params.get("migration_id")
    if migration_id:
        try:
            migration = db.get_migration_by_id(migration_id)
        except KeyError:
            task.status = JobSchedule.STATUS_DONE
            task.function_result = f"Migration not found: {migration_id}"
            task.write_to_db(db.kv_store)
            return True
    else:
        migration = None

    migration_controller.m = migration

    if task.canceled:
        task.function_result = "canceled"
        if migration:
            # initiate cancelling of migration that was is progress
            migration_controller.cancel_migration()
        else:
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return True
        return False

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    cluster = db.get_cluster_by_id(task.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
        task.function_result = "cluster is not active, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    lvol = db.get_lvol_by_id(task.function_params["lvol_id"])
    target_node = db.get_storage_node_by_id(task.function_params["target_node_id"])

    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        # initiate migration
        result = migration_controller.lvol_migrate(lvol, target_node, migration)
        if result:
            task.function_result = JobSchedule.STATUS_RUNNING
            task.status = JobSchedule.STATUS_RUNNING
            task.write_to_db(db.kv_store)


    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


logger = utils.get_logger(__name__)
migration_controller = MigrationController()
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
                if task.function_name == JobSchedule.FN_LVOL_MIGRATION:
                    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                        active_task = tasks_controller.get_active_lvol_mig_task(
                            task.cluster_id, task.node_id)
                        if active_task:
                            logger.info("task found on same node, retry")
                            continue
                    if task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if not res:
                            time.sleep(2)
