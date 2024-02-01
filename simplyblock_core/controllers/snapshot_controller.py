# coding=utf-8
import datetime
import logging as lg
import time
import uuid

from simplyblock_core.controllers import lvol_controller

from simplyblock_core import utils
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCClient

logger = lg.getLogger()

db_controller = DBController()


def add(lvol_id, snapshot_name):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.id}")

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    ret = rpc_client.lvol_create_snapshot(lvol_id, snapshot_name)
    if not ret:
        logger.error(f"Error creating snapshot")
        return False

    snap = SnapShot()
    snap.uuid = ret
    snap.snap_name = snapshot_name
    snap.snap_bdev = lvol.lvol_bdev.split("/")[0]+"/"+snapshot_name
    snap.created_at = int(time.time())
    snap.lvol = lvol
    snap.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return ret


def list():
    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        if snap.deleted is True:
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
    snap.remove(db_controller.kv_store)

    base_lvol = db_controller.get_lvol_by_id(snap.lvol.get_id())
    if base_lvol.deleted is True:
        lvol_controller.delete_lvol(base_lvol.get_id())

    logger.info("Done")
    return True


def clone(snapshot_id, clone_name):
    snap = db_controller.get_snapshot_by_id(snapshot_id)
    if not snap:
        logger.error(f"Snapshot not found {snapshot_id}")
        return False

    pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
    if not pool:
        logger.error(f"Pool not found: {snap.lvol.pool_uuid}")
        return False
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    if not pool:
        logger.error(f"Storage node not found: {snap.lvol.node_id}")
        return False

    if not snode.nvme_devices:
        logger.error("Storage node has no nvme devices")
        return False

    if snode.status != snode.STATUS_ONLINE:
        logger.error("Storage node in not Online")
        return False

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    if not snode.nvme_devices:
        logger.error("Storage node has no nvme devices")
        return False

    if snode.status != snode.STATUS_ONLINE:
        logger.error("Storage node in not Online")
        return False

    lvol = LVol()
    lvol.lvol_name = clone_name
    lvol.size = snap.lvol.size
    bdev_stack = []

    ret = rpc_client.lvol_clone(snap.snap_bdev, clone_name)
    if not ret:
        logger.error("failed to clone snapshot on the storage node")
        return False

    lvol_id = ret
    lvs_name = snap.lvol.lvol_bdev.split("/")[0]
    lvol_bdev = f"{lvs_name}/{clone_name}"
    bdev_stack.append({"type": "lvol", "name": lvol_bdev})
    lvol_type = 'lvol'
    top_bdev = lvol_bdev

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
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, top_bdev)

    lvol.bdev_stack = bdev_stack
    lvol.uuid = lvol_id
    lvol.lvol_bdev = lvol_bdev
    lvol.hostname = snode.hostname
    lvol.node_id = snode.get_id()
    lvol.mode = 'read-write'
    lvol.lvol_type = lvol_type
    lvol.cloned_from_snap = snapshot_id
    lvol.nqn = subsystem_nqn
    lvol.pool_uuid = pool.id
    pool.lvols.append(lvol_id)
    pool.write_to_db(db_controller.kv_store)
    lvol.write_to_db(db_controller.kv_store)
    snode.lvols.append(lvol_id)
    snode.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return lvol_id
