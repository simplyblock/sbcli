# coding=utf-8
import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.controllers import health_controller, snapshot_events, tasks_controller
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)


def set_snapshot_health_check(snap, health_check_status):
    snap = db.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    snap.health_check = health_check_status
    snap.updated_at = str(datetime.now())
    snap.write_to_db()


def process_snap_delete_finish(snap, leader_node):
    logger.info(f"Snapshot deleted successfully, id: {snap.get_id()}")

    # check leadership
    snode = db.get_storage_node_by_id(snap.lvol.node_id)
    sec_node = None
    if snode.secondary_node_id:
        sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
    leader_node = None
    snode = db.get_storage_node_by_id(snode.get_id())
    if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
        if not ret:
            raise Exception("Failed to get LVol info")
        lvs_info = ret[0]
        if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
            leader_node = snode

    if not leader_node and sec_node:
        ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
        if not ret:
            raise Exception("Failed to get LVol info")
        lvs_info = ret[0]
        if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
            leader_node = sec_node

    if not leader_node:
        raise Exception("Failed to get leader node")

    if snap.deletion_status != leader_node.get_id():
        ret, _ = leader_node.rpc_client().delete_lvol(snap.snap_bdev)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")
        snap = db.get_snapshot_by_id(snap.get_id())
        snap.deletion_status = leader_node.get_id()
        snap.write_to_db()
        return

    # 3-1 async delete lvol bdev from primary
    primary_node = db.get_storage_node_by_id(leader_node.get_id())
    if primary_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        ret, _ = primary_node.rpc_client().delete_lvol(snap.snap_bdev, del_async=True)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")

    # 3-2 async delete lvol bdev from secondary
    non_leader_id = snode.secondary_node_id
    if snode.get_id() != leader_node.get_id():
        non_leader_id = snode.get_id()

    non_leader = db.get_storage_node_by_id(non_leader_id)
    if non_leader:
        tasks_controller.add_lvol_sync_del_task(non_leader.cluster_id, non_leader.get_id(), snap.snap_bdev)

    snapshot_events.snapshot_delete(snap)
    snap.remove(db.kv_store)


def process_snap_delete_try_again(snap):
    snap = db.get_snapshot_by_id(snap.get_id())
    snap.deletion_status = ""
    snap.write_to_db()


def set_snap_offline(snap):
    sn = db.get_snapshot_by_id(snap.get_id())
    sn.deletion_status = ""
    sn.status = SnapShot.STATUS_OFFLINE
    sn.write_to_db()


# get DB controller
db = db_controller.DBController()

logger.info("Starting LVol monitor...")
while True:

    for cluster in db.get_clusters():

        if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
            logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
            continue

        for snode in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
            node_bdev_names = []
            node_lvols_nqns = {}
            sec_node_bdev_names = {}
            sec_node = None

            if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:

                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=3, retry=2)
                node_bdevs = rpc_client.get_bdevs()
                if node_bdevs:
                    node_bdev_names = [b['name'] for b in node_bdevs]
                    for bdev in node_bdevs:
                        if "aliases" in bdev and bdev["aliases"]:
                            node_bdev_names.extend(bdev['aliases'])

                ret = rpc_client.subsystem_list()
                if ret:
                    for sub in ret:
                        node_lvols_nqns[sub['nqn']] = sub

            if snode.secondary_node_id:
                sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
                if sec_node and sec_node.status==StorageNode.STATUS_ONLINE:
                    sec_rpc_client = RPCClient(
                        sec_node.mgmt_ip, sec_node.rpc_port,
                        sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)
                    ret = sec_rpc_client.get_bdevs()
                    if ret:
                        for bdev in ret:
                            sec_node_bdev_names[bdev['name']] = bdev

            if snode.lvstore_status == "ready":

                for snap in db.get_snapshots_by_node_id(snode.get_id()):
                    if snap.status == SnapShot.STATUS_ONLINE:

                        present = health_controller.check_bdev(snap.snap_bdev, bdev_names=node_bdev_names)
                        set_snapshot_health_check(snap, present)

                    elif snap.status == SnapShot.STATUS_IN_DELETION:

                        # check leadership
                        leader_node = None
                        if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                                            StorageNode.STATUS_DOWN]:
                            ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                            if not ret:
                                raise Exception("Failed to get LVol store info")
                            lvs_info = ret[0]
                            if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                                leader_node = snode

                        if not leader_node and sec_node:
                            ret = sec_node.rpc_client().bdev_lvol_get_lvstores(sec_node.lvstore)
                            if not ret:
                                raise Exception("Failed to get LVol store info")
                            lvs_info = ret[0]
                            if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                                leader_node = sec_node

                        if not leader_node:
                            raise Exception("Failed to get leader node")

                        if snap.deletion_status == "" or snap.deletion_status != leader_node.get_id():

                            ret, _ = leader_node.rpc_client().delete_lvol(snap.snap_bdev)
                            if not ret:
                                logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                                continue
                            snap = db.get_snapshot_by_id(snap.get_id())
                            snap.deletion_status = leader_node.get_id()
                            snap.write_to_db()

                            time.sleep(3)

                        try:
                            ret = leader_node.rpc_client().bdev_lvol_get_lvol_delete_status(snap.snap_bdev)
                        except Exception as e:
                            logger.error(e)
                            # timeout detected, check other node
                            break

                        if ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
                            process_snap_delete_finish(snap, leader_node)

                        elif ret == 1:  # Async lvol deletion is in progress or queued
                            logger.info(f"Snap deletion in progress, id: {snap.get_id()}")

                        elif ret == 3:  # Async deletion is done, but leadership has changed (sync deletion is now blocked)
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error(
                                "Async deletion is done, but leadership has changed (sync deletion is now blocked)")

                        elif ret == 4:  # No async delete request exists for this Snap
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("No async delete request exists for this snap")
                            set_snap_offline(snap)

                        elif ret == -1:  # Operation not permitted
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Operation not permitted")
                            process_snap_delete_try_again(snap)

                        elif ret == -2:  # No such file or directory
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("No such file or directory")
                            process_snap_delete_finish(snap, leader_node)

                        elif ret == -5:  # I/O error
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("I/O error")
                            process_snap_delete_try_again(snap)

                        elif ret == -11:  # Try again
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Try again")
                            process_snap_delete_try_again(snap)

                        elif ret == -12:  # Out of memory
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Out of memory")
                            process_snap_delete_try_again(snap)

                        elif ret == -16:  # Device or resource busy
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Device or resource busy")
                            process_snap_delete_try_again(snap)

                        elif ret == -19:  # No such device
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("No such device")
                            set_snap_offline(snap)

                        elif ret == -35:  # Leadership changed
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Leadership changed")
                            process_snap_delete_try_again(snap)

                        elif ret == -36:  # Failed to update lvol for deletion
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Failed to update snapshot for deletion")
                            process_snap_delete_try_again(snap)

                        else:  # Failed to update lvol for deletion
                            logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
                            logger.error("Failed to update snapshot for deletion")


    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
