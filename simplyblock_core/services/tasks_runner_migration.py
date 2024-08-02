# coding=utf-8
import logging
import time
import sys


from simplyblock_core import constants, kv_store
from simplyblock_core.controllers import tasks_events
from simplyblock_core.models.job_schedule import JobSchedule


# Import the GELF logger
from graypy import GELFUDPHandler


def task_runner(task):
    task.status = JobSchedule.STATUS_RUNNING
    task.write_to_db(db_controller.kv_store)
    tasks_events.task_updated(task)

    time.sleep(30)

    task.function_result = "sleep 30"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db_controller.kv_store)
    tasks_events.task_updated(task)

    return True


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
                delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                if task.function_name == JobSchedule.FN_DEV_MIG:
                    while task.status != JobSchedule.STATUS_DONE:
                        res = task_runner(task)
                        if res:
                            tasks_events.task_updated(task)
                        else:
                            time.sleep(delay_seconds)
                            delay_seconds *= 2
