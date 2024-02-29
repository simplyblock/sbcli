# coding=utf-8
import logging
import os

import time
import sys


from simplyblock_core import constants, kv_store
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import health_controller, storage_events


def set_lvol_status(lvol, status):
    if lvol.status != status:
        lvol = db_controller.get_lvol_by_id(lvol.get_id())
        old_status = lvol.status
        lvol.status = status
        lvol.write_to_db(db_store)
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        storage_events.lvol_status_change(snode.cluster_id, lvol, lvol.status, old_status, caused_by="monitor")


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()


logger.info("Starting LVol monitor...")
while True:
    lvols = db_controller.get_lvols()
    if not lvols:
        logger.error("LVols list is empty")

    for lvol in lvols:
        ret = health_controller.check_lvol(lvol.get_id())
        logger.info(f"LVol: {lvol.get_id()}, is healthy: {ret}")
        if ret:
            set_lvol_status(lvol, LVol.STATUS_ONLINE)
        else:
            set_lvol_status(lvol, LVol.STATUS_OFFLINE)

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
