# coding=utf-8
import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import health_controller, lvol_events, tasks_controller, lvol_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)

def set_lvol_status(lvol, status):
    if lvol.status != status:
        lvol = db.get_lvol_by_id(lvol.get_id())
        old_status = lvol.status
        lvol.status = status
        lvol.write_to_db()
        lvol_events.lvol_status_change(lvol, lvol.status, old_status, caused_by="monitor")


def set_lvol_health_check(lvol, health_check_status):
    lvol = db.get_lvol_by_id(lvol.get_id())
    if lvol.health_check == health_check_status:
        return
    old_status = lvol.health_check
    lvol.health_check = health_check_status
    lvol.updated_at = str(datetime.now())
    lvol.write_to_db()
    lvol_events.lvol_health_check_change(lvol, lvol.health_check, old_status, caused_by="monitor")


def set_snapshot_health_check(snap, health_check_status):
    snap = db.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    snap.health_check = health_check_status
    snap.updated_at = str(datetime.now())
    snap.write_to_db()


lvol_del_start_time = 0.0
def pre_lvol_delete_rebalance():
    global lvol_del_start_time
    if lvol_del_start_time == 0:
        lvol_del_start_time = time.time()


def resume_comp(lvol):
    logger.info("resuming compression")
    node = db.get_storage_node_by_id(lvol.node_id)
    for n in db.get_storage_nodes_by_cluster_id(node.cluster_id):
        if n.status != StorageNode.STATUS_ONLINE:
            logger.warning("Not all nodes are online, can not resume JC compression")
            return
    rpc_client = RPCClient(
        node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=5, retry=2)
    ret, err = rpc_client.jc_suspend_compression(jm_vuid=node.jm_vuid, suspend=False)
    if err:
        logger.info("Failed to resume JC compression adding task...")
        tasks_controller.add_jc_comp_resume_task(node.cluster_id, node.get_id(), node.jm_vuid)


def post_lvol_delete_rebalance(lvol):
    global lvol_del_start_time
    diff = time.time() - lvol_del_start_time
    if diff > 0:
        records = db.get_cluster_capacity(cluster, int(diff/5))
        total_size = records[0].size_total
        current_cap = records[0].size_used
        start_cap = records[-1].size_used
        if start_cap - current_cap > int(total_size * 10 / 100):
            resume_comp(lvol)
        lvol_del_start_time = 0
        return True
    lvol_records = db.get_lvol_stats(lvol, 1)
    if lvol_records:
        total_size = db.get_cluster_capacity(cluster, 1)[0].size_total
        if lvol_records[0].size_used > int(total_size * 10 / 100):
            resume_comp(lvol)


def process_lvol_delete_finish(lvol):
    logger.info(f"LVol deleted successfully, id: {lvol.get_id()}")

    # check leadership
    snode = db.get_storage_node_by_id(lvol.node_id)
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

    if lvol.deletion_status != leader_node.get_id():
        lvol_controller.delete_lvol_from_node(lvol.get_id(), leader_node.get_id())
        return

    # 3-1 async delete lvol bdev from primary
    primary_node = db.get_storage_node_by_id(leader_node.get_id())
    if primary_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        ret = lvol_controller.delete_lvol_from_node(lvol.get_id(), primary_node.get_id(), del_async=True)
        if not ret:
            logger.error(f"Failed to delete lvol from primary_node node: {primary_node.get_id()}")

    # 3-2 async delete lvol bdev from secondary
    if snode.get_id() == leader_node.get_id():
        sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
    else:
        sec_node = db.get_storage_node_by_id(snode.get_id())

    if sec_node:
        tasks_controller.add_lvol_sync_del_task(sec_node.cluster_id, sec_node.get_id(), f"{lvol.lvs_name}/{lvol.lvol_bdev}")

    lvol_events.lvol_delete(lvol)
    lvol.remove(db.kv_store)
    # check for full devices
    full_devs_ids = []
    all_devs_ids = []
    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
            continue
        all_devs_ids.append(dev.get_id())
        if dev.status == NVMeDevice.STATUS_CANNOT_ALLOCATE:
            full_devs_ids.append(dev.get_id())

    if 0 < len(full_devs_ids) == len(all_devs_ids):
        logger.info("All devices are full, starting expansion migrations")
        for dev_id in full_devs_ids:
            tasks_controller.add_new_device_mig_task(dev_id)
    post_lvol_delete_rebalance(lvol)


def process_lvol_delete_try_again(lvol):
    lvol = db.get_lvol_by_id(lvol.get_id())
    lvol.deletion_status = ""
    lvol.write_to_db()


def check_node(snode):
    node_bdev_names = []
    node_lvols_nqns = {}
    sec_node_bdev_names = {}
    sec_node_lvols_nqns = {}
    sec_node = None

    if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        node_bdevs = snode.rpc_client().get_bdevs()
        if node_bdevs:
            node_bdev_names = [b['name'] for b in node_bdevs]
            for bdev in node_bdevs:
                if "aliases" in bdev and bdev["aliases"]:
                    node_bdev_names.extend(bdev['aliases'])
        ret = snode.rpc_client().subsystem_list()
        if ret:
            for sub in ret:
                node_lvols_nqns[sub['nqn']] = sub

    if snode.secondary_node_id:
        sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
            sec_rpc_client = RPCClient(
                sec_node.mgmt_ip, sec_node.rpc_port,
                sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)
            ret = sec_rpc_client.get_bdevs()
            if ret:
                for bdev in ret:
                    sec_node_bdev_names[bdev['name']] = bdev

            ret = sec_rpc_client.subsystem_list()
            if ret:
                for sub in ret:
                    sec_node_lvols_nqns[sub['nqn']] = sub

    for lvol in db.get_lvols_by_node_id(snode.get_id()):

        if lvol.status == LVol.STATUS_IN_CREATION:
            continue

        if lvol.status == lvol.STATUS_IN_DELETION:
            # check leadership
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

            if lvol.deletion_status == "" or lvol.deletion_status != leader_node.get_id():
                lvol_controller.delete_lvol_from_node(lvol.get_id(), leader_node.get_id())
                time.sleep(3)

            try:
                ret = leader_node.rpc_client().bdev_lvol_get_lvol_delete_status(
                    f"{lvol.lvs_name}/{lvol.lvol_bdev}")
            except Exception as e:
                logger.error(e)
                # timeout detected, check other node
                break

            if ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
                process_lvol_delete_finish(lvol)

            elif ret == 1:  # Async lvol deletion is in progress or queued
                logger.info(f"LVol deletion in progress, id: {lvol.get_id()}")
                pre_lvol_delete_rebalance()

            elif ret == 3:  # Async deletion is done, but leadership has changed (sync deletion is now blocked)
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Async deletion is done, but leadership has changed (sync deletion is now blocked)")

            elif ret == 4:  # No async delete request exists for this lvol
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("No async delete request exists for this lvol")
                lvol = db.get_lvol_by_id(lvol.get_id())
                lvol.io_error = True
                lvol.write_to_db()
                set_lvol_status(lvol, LVol.STATUS_OFFLINE)

            elif ret == -1:  # Operation not permitted
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Operation not permitted")
                lvol = db.get_lvol_by_id(lvol.get_id())
                lvol.io_error = True
                lvol.write_to_db()
                set_lvol_status(lvol, LVol.STATUS_OFFLINE)

            elif ret == -2:  # No such file or directory
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("No such file or directory")
                process_lvol_delete_finish(lvol)

            elif ret == -5:  # I/O error
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("I/O error")
                process_lvol_delete_try_again(lvol)

            elif ret == -11:  # Try again
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Try again")
                process_lvol_delete_try_again(lvol)

            elif ret == -12:  # Out of memory
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Out of memory")
                process_lvol_delete_try_again(lvol)

            elif ret == -16:  # Device or resource busy
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Device or resource busy")
                process_lvol_delete_try_again(lvol)

            elif ret == -19:  # No such device
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Finishing lvol delete")
                process_lvol_delete_finish(lvol)

            elif ret == -35:  # Leadership changed
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Leadership changed")
                process_lvol_delete_try_again(lvol)

            elif ret == -36:  # Failed to update lvol for deletion
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Failed to update lvol for deletion")
                process_lvol_delete_try_again(lvol)

            else:  # Failed to update lvol for deletion
                logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                logger.error("Failed to update lvol for deletion")

            continue

        passed = True
        try:
            ret = health_controller.check_lvol_on_node(
                lvol.get_id(), lvol.node_id, node_bdev_names, node_lvols_nqns)
            if not ret:
                passed = False
        except Exception as e:
            logger.error(f"Failed to check lvol:{lvol.get_id()} on node: {lvol.node_id}")
            logger.error(e)

        if lvol.ha_type == "ha":
            sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
            if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                try:
                    ret = health_controller.check_lvol_on_node(
                        lvol.get_id(), snode.secondary_node_id, sec_node_bdev_names, sec_node_lvols_nqns)
                    if not ret:
                        passed = False
                    else:
                        passed = True
                except Exception as e:
                    logger.error(f"Failed to check lvol: {lvol.get_id()} on node: {snode.secondary_node_id}")
                    logger.error(e)

        if snode.lvstore_status == "ready":

            logger.info(f"LVol: {lvol.get_id()}, is healthy: {passed}")
            set_lvol_health_check(lvol, passed)
            if passed:
                set_lvol_status(lvol, LVol.STATUS_ONLINE)

    if snode.lvstore_status == "ready":

        for snap in db.get_snapshots_by_node_id(snode.get_id()):
            present = health_controller.check_bdev(snap.snap_bdev, bdev_names=node_bdev_names)
            set_snapshot_health_check(snap, present)



# get DB controller
db = db_controller.DBController()

logger.info("Starting LVol monitor...")
while True:

    for cluster in db.get_clusters():

        if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
            logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
            continue

        for snode in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
            try:
                check_node(snode)
            except Exception as e:
                logger.error(e)

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
