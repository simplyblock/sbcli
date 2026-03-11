# coding=utf-8
"""
tasks_runner_backup_merge.py - periodic service that evaluates backup policies
and triggers merges when retention limits are exceeded.
"""
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import backup_controller
from simplyblock_core.models.cluster import Cluster

logger = utils.get_logger(__name__)

db = db_controller.DBController()

logger.info("Starting backup merge service...")
while True:
    clusters = db.get_clusters()
    for cl in clusters:
        if cl.status == Cluster.STATUS_IN_ACTIVATION:
            continue

        try:
            lvols = db.get_lvols(cl.get_id())
            for lvol in lvols:
                try:
                    backup_controller.evaluate_policy(lvol)
                except Exception as e:
                    logger.error(f"Error evaluating policy for lvol {lvol.get_id()}: {e}")
                try:
                    backup_controller.evaluate_schedule(lvol)
                except Exception as e:
                    logger.error(f"Error evaluating schedule for lvol {lvol.get_id()}: {e}")
        except Exception as e:
            logger.error(f"Error processing cluster {cl.get_id()}: {e}")

    time.sleep(constants.BACKUP_MERGE_SERVICE_INTERVAL_SEC)
