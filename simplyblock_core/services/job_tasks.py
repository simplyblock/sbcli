# coding=utf-8
import logging
import os

import time
import sys

import psutil


from simplyblock_core import constants, kv_store, utils
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.port_stat import PortStat

# Import the GELF logger
from graypy import GELFUDPHandler


def task_runner(task):
    now = int(time.time())
    data = {
        "uuid": nic.get_id(),
        "node_id": snode.get_id(),
        "date": now,
        "bytes_sent": stats.bytes_sent,
        "bytes_received": stats.bytes_recv,
        "packets_sent": stats.packets_sent,
        "packets_received": stats.packets_recv,
        "errin": stats.errin,
        "errout": stats.errout,
        "dropin": stats.dropin,
        "dropout": stats.dropout,
    }
    last_stat = PortStat(data={"uuid": nic.get_id(), "node_id": snode.get_id()}).get_last(db_controller.kv_store)
    if last_stat:
        data.update({
            "out_speed": int((stats.bytes_sent - last_stat.bytes_sent) / (now - last_stat.date)),
            "in_speed": int((stats.bytes_recv - last_stat.bytes_received) / (now - last_stat.date)),
        })
    stat_obj = PortStat(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
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
                if task.status == JobSchedule.STATUS_NEW:
                    res = task_runner(task)

    time.sleep(3)
