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
                    snode.rpc_username, snode.rpc_password, timeout=3, retry=2)
                node_bdevs = rpc_client.get_bdevs()
                if node_bdevs:
                    node_bdev_names = [b['name'] for b in node_bdevs]
                else:
                    node_bdev_names = []

                node_lvols_nqns = {}
                ret = rpc_client.subsystem_list()
                for sub in ret:
                    node_lvols_nqns[sub['nqn']] = sub

                sec_node_bdev_names = {}
                sec_node_lvols_nqns = {}

                if snode.secondary_node_id:
                    sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                    if sec_node and sec_node.status==StorageNode.STATUS_ONLINE:
                        sec_rpc_client = RPCClient(
                            sec_node.mgmt_ip, sec_node.rpc_port,
                            sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)
                        ret = sec_rpc_client.get_bdevs()
                        for bdev in ret:
                            sec_node_bdev_names[bdev['name']] = bdev

                        ret = sec_rpc_client.subsystem_list()
                        for sub in ret:
                            sec_node_lvols_nqns[sub['nqn']] = sub

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

                    passed = True

                    if lvol.ha_type == 'single':
                        ret = health_controller.check_lvol_on_node(
                            lvol.get_id(), lvol.node_id, node_bdev_names, node_lvols_nqns)
                        if not ret:
                            passed = False


                    if lvol.ha_type == "ha":
                        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            ret = health_controller.check_lvol_on_node(
                                lvol.get_id(), snode.secondary_node_id, sec_node_bdev_names, sec_node_lvols_nqns)
                            if not ret:
                                passed = False


                    logger.info(f"LVol: {lvol.get_id()}, is healthy: {passed}")
                    set_lvol_health_check(lvol, passed)
                    if passed:
                        set_lvol_status(lvol, LVol.STATUS_ONLINE)

                for snap in db_controller.get_snapshots_by_node_id(snode.get_id()):
                    if snap.snap_bdev in node_bdev_names:
                        logger.info(f"Checking bdev: {snap.snap_bdev} ... ok")
                        set_snapshot_health_check(snap, True)
                    else:
                        logger.error(f"Checking bdev: {snap.snap_bdev} ... failed")
                        set_snapshot_health_check(snap, False)
                        passed = False

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
