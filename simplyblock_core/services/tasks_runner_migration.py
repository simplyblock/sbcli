# coding=utf-8
import logging
import time
import sys


from simplyblock_core import constants, kv_store
from simplyblock_core.controllers import tasks_events
from simplyblock_core.models.job_schedule import JobSchedule


# Import the GELF logger
from graypy import GELFUDPHandler

from simplyblock_core.rpc_client import RPCClient


def task_runner(task):

    snode = db_controller.get_storage_node_by_id(task.node_id)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    if task.status == JobSchedule.STATUS_NEW:
        device = None
        for dev in snode.nvme_devices:
            if dev.get_id() == task.device_id:
                device = dev
                break

        rsp = rpc_client.distr_migration_to_primary_start(device.cluster_device_order)
        if not rsp:
            logger.error(f"Failed to start device migration task, storage_ID: {device.cluster_device_order}")
            task.function_result = "Failed to start device migration task"
            task.retry += 1
            task.write_to_db(db_controller.kv_store)
            return False

        task.status = JobSchedule.STATUS_RUNNING
        task.function_params = {"migration_ids": rsp}
        task.write_to_db(db_controller.kv_store)
        tasks_events.task_updated(task)
        return False

    elif task.status == JobSchedule.STATUS_RUNNING:
        is_done = True
        for mig_id in task.function_params["migration_ids"]:
            res = rpc_client.distr_migration_status(mig_id)
            for st in res:
                if st["migration_id"] == mig_id:
                    if st['status'] != "completed":
                        is_done = False
        if is_done:
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "Done"
            task.write_to_db(db_controller.kv_store)
            return True

    task.retry += 1
    task.write_to_db(db_controller.kv_store)
    return False


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
                    if task.status != JobSchedule.STATUS_DONE:
                        res = task_runner(task)
                        if res:
                            tasks_events.task_updated(task)
                        else:
                            time.sleep(delay_seconds)
