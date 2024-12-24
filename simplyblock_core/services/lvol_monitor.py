# coding=utf-8
import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import health_controller, lvol_events


logger = utils.get_logger(__name__)


def set_lvol_status(lvol, status):
    if lvol.status != status:
        lvol = db_controller.get_lvol_by_id(lvol.get_id())
        old_status = lvol.status
        lvol.status = status
        lvol.write_to_db()
        lvol_events.lvol_status_change(lvol, lvol.status, old_status, caused_by="monitor")


def set_lvol_health_check(lvol, health_check_status):
    lvol = db_controller.get_lvol_by_id(lvol.get_id())
    if lvol.health_check == health_check_status:
        return
    old_status = lvol.health_check
    lvol.health_check = health_check_status
    lvol.updated_at = str(datetime.now())
    lvol.write_to_db()
    lvol_events.lvol_health_check_change(lvol, lvol.health_check, old_status, caused_by="monitor")


def set_snapshot_health_check(snap, health_check_status):
    snap = db_controller.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    snap.health_check = health_check_status
    snap.updated_at = str(datetime.now())
    snap.write_to_db()


# get DB controller
db_controller = db_controller.DBController()

logger.info("Starting LVol monitor...")
while True:
    lvols = db_controller.get_lvols()  # pass
    if not lvols:
        logger.error("LVols list is empty")

    for lvol in lvols:
        if lvol.io_error:
            logger.debug(f"Skipping LVol health check because of io_error {lvol.get_id()}")
            continue
        if lvol.status == lvol.STATUS_IN_DELETION:
            logger.warning(f"LVol in deletion, id: {lvol.get_id()}, status: {lvol.status}.. skipping")
            continue
        ret = health_controller.check_lvol(lvol.get_id())
        if not ret:
            time.sleep(5)
            ret = health_controller.check_lvol(lvol.get_id())

        logger.info(f"LVol: {lvol.get_id()}, is healthy: {ret}")
        set_lvol_health_check(lvol, ret)
        if ret:
            set_lvol_status(lvol, LVol.STATUS_ONLINE)
        else:
            set_lvol_status(lvol, LVol.STATUS_OFFLINE)

    for snap in db_controller.get_snapshots():
        logger.debug("Checking Snapshot: %s", snap.get_id())
        ret = health_controller.check_snap(snap.get_id())
        set_snapshot_health_check(snap, ret)

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
