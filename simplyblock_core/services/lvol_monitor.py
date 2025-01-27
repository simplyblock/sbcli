# coding=utf-8
import time
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import health_controller, lvol_events
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




def check_lvol_stack(lvol, node_bdev_names, subsystem_list, node):
    passed = True
    try:
        logger.info(f"Checking LVol {lvol.get_id()} on node {node.get_id()}")
        for bdev_info in lvol.bdev_stack:
            bdev_name = bdev_info['name']
            if bdev_info['type'] == "bdev_lvol":
                bdev_name = bdev_info['params']["lvs_name"] + "/" + bdev_info['params']["name"]
            if bdev_name in node_bdev_names:
                logger.info(f"Checking bdev: {bdev_name} ... ok")
            else:
                logger.error(f"Checking bdev: {bdev_name} ... failed")
                passed = False

        if lvol.nqn in subsystem_list:
            logger.info(f"Checking subsystem ... ok")
        else:
            logger.info(f"Checking subsystem ... not found")
            passed = False

    except Exception as e:
        logger.exception(e)
    return passed




# get DB controller
db_controller = db_controller.DBController()

logger.info("Starting LVol monitor...")
while True:

    for node in db_controller.get_storage_nodes():

        if node.is_secondary_node:
            continue

        lvols = db_controller.get_lvols_by_node_id(node.get_id())  # pass
        if not lvols:
            logger.debug("LVols list is empty")
            continue

        primary_node = node
        secondary_node = None
        if primary_node.secondary_node_id:
            sec_node = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)
            if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                secondary_node = sec_node


        rpc_client = RPCClient(
            primary_node.mgmt_ip, primary_node.rpc_port,
            primary_node.rpc_username, primary_node.rpc_password, timeout=10, retry=1)

        node_bdev_names = []
        for node_bdev in rpc_client.get_bdevs():
            node_bdev_names.append(node_bdev['name'])
            if "aliases" in node_bdev and node_bdev["aliases"]:
                node_bdev_names.extend(node_bdev["aliases"])

        subsystem_list = []
        ret = rpc_client.subsystem_list()
        if ret:
            subsystem_list = [b['nqn'] for b in ret]

        sec_node_bdev_names = []
        sec_subsystem_list = []
        if secondary_node:
            rpc_client = RPCClient(
                secondary_node.mgmt_ip, secondary_node.rpc_port,
                secondary_node.rpc_username, secondary_node.rpc_password, timeout=10, retry=1)
            for node_bdev in rpc_client.get_bdevs():
                sec_node_bdev_names.append(node_bdev['name'])
                if "aliases" in node_bdev and node_bdev["aliases"]:
                    sec_node_bdev_names.extend(node_bdev["aliases"])

            ret = rpc_client.subsystem_list()
            if ret:
                sec_subsystem_list = [b['nqn'] for b in ret]

        # print(node_bdev_names)
        # print(subsystem_list)
        # print(sec_node_bdev_names)
        # print(sec_subsystem_list)

        for lvol in lvols:
            if lvol.io_error:
                logger.debug(f"Skipping LVol health check because of io_error {lvol.get_id()}")
                continue
            if lvol.status == lvol.STATUS_IN_DELETION:
                logger.warning(f"LVol in deletion, id: {lvol.get_id()}, status: {lvol.status}.. skipping")
                continue

            passed_prim = check_lvol_stack(lvol, node_bdev_names, subsystem_list, primary_node)
            passed_sec = True
            if secondary_node:
                passed_sec = check_lvol_stack(lvol, sec_node_bdev_names, sec_subsystem_list, secondary_node)

            ret = passed_prim and passed_sec
            logger.info(f"LVol: {lvol.get_id()}, is healthy: {ret}")
            set_lvol_health_check(lvol, ret)
            if passed_prim == False and passed_sec == False:
                set_lvol_status(lvol, LVol.STATUS_OFFLINE)
            else:
                set_lvol_status(lvol, LVol.STATUS_ONLINE)

        for snap in db_controller.get_snapshots_by_node_id(primary_node.get_id()):
            logger.info("Checking Snapshot: %s, on node: %s", snap.get_id(), primary_node.get_id())
            passed_prim = True
            passed_sec = True
            if snap.snap_bdev not in node_bdev_names:
                passed_prim = False
            if secondary_node and snap.snap_bdev not in sec_node_bdev_names:
                passed_sec = False

            ret = passed_prim and passed_sec
            set_snapshot_health_check(snap, ret)

    time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)
