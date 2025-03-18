# coding=utf-8
import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import health_controller, lvol_events, lvol_controller
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)

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

    for cluster in db_controller.get_clusters():

        if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
            logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
            continue

        for snode in db_controller.get_storage_nodes_by_cluster_id(cluster.get_id()):

            if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:

                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=5, retry=1)

                for lvol in db_controller.get_lvols_by_node_id(snode.get_id()):

                    if lvol.io_error:
                        logger.debug(f"Skipping LVol health check because of io_error {lvol.get_id()}")
                        continue

                    if lvol.status == lvol.STATUS_IN_DELETION:
                        ret = rpc_client.bdev_lvol_get_lvol_delete_status(f"{lvol.lvs_name}/{lvol.lvol_bdev}")
                        if ret == 0: # delete complete
                            logger.info(f"LVol deleted successfully, id: {lvol.get_id()}")
                            lvol_events.lvol_delete(lvol)
                            lvol.remove(db_controller.kv_store)

                        elif ret == 1: # deletion is in progress.
                            logger.info(f"LVol deletion in progress, id: {lvol.get_id()}")

                        elif ret == 2: # deletion error
                            logger.info(f"LVol deletion error, id: {lvol.get_id()}")
                            lvol = db_controller.get_lvol_by_id(lvol.get_id())
                            lvol.io_error = True
                            lvol.write_to_db()
                            set_lvol_status(lvol, LVol.STATUS_OFFLINE)

                        continue

                    ret = health_controller.check_lvol(lvol.get_id())
                    if not ret:
                        time.sleep(3)
                        ret = health_controller.check_lvol(lvol.get_id())

                    logger.info(f"LVol: {lvol.get_id()}, is healthy: {ret}")
                    set_lvol_health_check(lvol, ret)
                    lvol = db_controller.get_lvol_by_id(lvol.get_id())
                    if lvol.status != lvol.STATUS_IN_DELETION:
                        if ret:
                            set_lvol_status(lvol, LVol.STATUS_ONLINE)
                        else:
                            set_lvol_status(lvol, LVol.STATUS_OFFLINE)

            for snap in db_controller.get_snapshots_by_node_id(snode.get_id()):
                logger.debug("Checking Snapshot: %s", snap.get_id())
                ret = health_controller.check_snap(snap.get_id())
                set_snapshot_health_check(snap, ret)

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
