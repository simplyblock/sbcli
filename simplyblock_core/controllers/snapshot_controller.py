# coding=utf-8
import datetime
import json
import logging as lg
import time
import uuid

from simplyblock_core.controllers import lvol_controller, snapshot_events

from simplyblock_core import utils, distr_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient


logger = lg.getLogger()

db_controller = DBController()


def add(lvol_id, snapshot_name):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.id}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    snap_count = 0
    for sn in db_controller.get_snapshots():
        if sn.lvol.get_id() == lvol_id:
            snap_count += 1

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    snap_name = f"{snapshot_name}_{snap_count}"
    snap_bdev = f"{lvol.lvs_name}/{snap_name}"

    ret = rpc_client.lvol_create_snapshot(lvol.lvol_bdev, snap_name)
    if not ret:
        return False, "Failed to create snapshot lvol bdev"


    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.snap_name = snapshot_name
    snap.snap_bdev = snap_bdev
    snap.created_at = int(time.time())
    snap.lvol = lvol

    snap.write_to_db(db_controller.kv_store)
    logger.info("Done")
    snapshot_events.snapshot_create(snap)
    return snap.uuid


def list(all=False):
    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        if all is False and snap.deleted is True:
            continue
        logger.debug(snap)
        data.append({
            "UUID": snap.uuid,
            "Name": snap.snap_name,
            "Size": utils.humanbytes(snap.lvol.size),
            "BDev": snap.snap_bdev,
            "LVol ID": snap.lvol.get_id(),
            "Created At": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(snap.created_at)),
        })
    return utils.print_table(data)


def delete(snapshot_uuid):
    snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    if not snap:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    if not snode:
        logger.error(f"Storage node not found {snap.lvol.node_id}")
        return False

    for lvol in db_controller.get_lvols():
        if lvol.cloned_from_snap and lvol.cloned_from_snap == snapshot_uuid:
            logger.warning(f"Soft delete snapshot with clones, lvol ID: {lvol.get_id()}")
            snap.deleted = True
            snap.write_to_db(db_controller.kv_store)
            return True

    logger.info(f"Removing snapshot: {snapshot_uuid}")

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    ret = rpc_client.delete_lvol(snap.snap_bdev)
    if not ret:
        logger.error(f"Failed to delete BDev {snap.snap_bdev}")
        return False
    #
    # ret = rpc_client.bdev_distrib_delete(snap.base_bdev)
    # if not ret:
    #     logger.error(f"Failed to delete BDev {snap.base_bdev}")
    #     return False

    snap.remove(db_controller.kv_store)

    base_lvol = db_controller.get_lvol_by_id(snap.lvol.get_id())
    if base_lvol.deleted is True:
        lvol_controller.delete_lvol(base_lvol.get_id())

    logger.info("Done")
    snapshot_events.snapshot_delete(snap)
    return True


def clone(snapshot_id, clone_name, new_size=0):
    snap = db_controller.get_snapshot_by_id(snapshot_id)
    if not snap:
        msg=f"Snapshot not found {snapshot_id}"
        logger.error(msg)
        return False, msg

    # pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
    # if not pool:
    #     msg=f"Pool not found: {snap.lvol.pool_uuid}"
    #     logger.error(msg)
    #     return False, msg
    #
    # if pool.status == Pool.STATUS_INACTIVE:
    #     msg="Pool is disabled"
    #     logger.error(msg)
    #     return False, msg

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    if not snode:
        msg=f"Storage node not found: {snap.lvol.node_id}"
        logger.error(msg)
        return False, msg

    if snode.status != snode.STATUS_ONLINE:
        msg="Storage node in not Online"
        logger.error(msg)
        return False, msg

    for lvol in db_controller.get_lvols():
        if lvol.lvol_name == clone_name:
            msg=f"LVol name must be unique: {clone_name}"
            logger.error(msg)
            return False, msg

    lvol = LVol(data=snap.lvol.to_dict())
    lvol.lvol_name = clone_name
    # lvol.size = snap.lvol.size

    if new_size:
        if snap.lvol.size >= new_size:
            msg=f"New size {new_size} must be higher than the original size {snap.lvol.size}"
            logger.error(msg)
            return False, msg

        if snap.lvol.max_size < new_size:
            msg=f"New size {new_size} must be smaller than the max size {snap.lvol.max_size}"
            logger.error(msg)
            return False, msg
        # lvol.size = new_size


    # jm_names = lvol_controller.get_jm_names(snode)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    logger.info("Creating clone bdev")

    lvol_name = f"clone_{lvol.lvol_name}"
    lvol_bdev = f"{lvol.lvs_name}/{lvol_name}"
    ret = rpc_client.lvol_clone(snap.snap_bdev, lvol_name)
    if not ret:
        return False, "Failed to create clone lvol bdev"

    if new_size:
        ret = rpc_client.bdev_lvol_resize(lvol_bdev, new_size)
        if not ret:
            return False, "Failed to create clone lvol bdev"
        lvol.size = new_size


    lvol_id = str(uuid.uuid4())
    # lvs_name = snap.lvol.lvol_bdev.split("/")[0]
    # bdev_stack.append({"type": "ultra_lvol", "name": lvol_name})
    bdev_stack = [{
        "type": "bdev_lvol",
        "name": lvol_bdev,
        "params": {
            "name": lvol_name
        }
    }]
    # lvol_type = 'lvol'

    subsystem_nqn = snode.subsystem + ":lvol:" + lvol_id
    logger.info("creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', lvol_id)
    logger.debug(ret)

    # add listeners
    logger.info("adding listeners")
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            ret = rpc_client.transport_create(tr_type)
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")

    logger.info(f"add lvol {clone_name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, lvol_bdev)

    lvol.bdev_stack = bdev_stack
    lvol.uuid = lvol_id
    lvol.lvol_bdev = lvol_bdev
    lvol.top_bdev = lvol_bdev
    # lvol.base_bdev = name
    # lvol.hostname = snode.hostname
    # lvol.node_id = snode.get_id()
    # lvol.mode = 'read-write'
    lvol.lvol_type = "clone"
    lvol.cloned_from_snap = snapshot_id
    lvol.nqn = subsystem_nqn
    # lvol.pool_uuid = pool.id
    # lvol.ha_type = snap.lvol.ha_type
    lvol.status = LVol.STATUS_ONLINE

    lvol.write_to_db(db_controller.kv_store)
    snode.lvols.append(lvol_id)
    snode.write_to_db(db_controller.kv_store)
    logger.info("Done")
    snapshot_events.snapshot_clone(snap, lvol)

    return True, lvol_id
