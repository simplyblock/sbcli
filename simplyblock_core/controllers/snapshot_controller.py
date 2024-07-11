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

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.id}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

##############################################################################
    # Validate adding snap on storage node
    snode_api = SNodeClient(snode.api_endpoint)
    result, _ = snode_api.info()
    memory_free = result["memory_details"]["free"]
    huge_free = result["memory_details"]["huge_free"]
    total_node_capacity = db_controller.get_snode_size(snode.get_id())

    error = utils.validate_add_lvol_or_snap_on_node(memory_free, huge_free, snode.max_snap, lvol.size, total_node_capacity,
                                                    len(db_controller.get_snapshots_by_node_id(snode.get_id())))

    if error:
        logger.error(f"Failed to add snap on node {snode.get_id()}")
        logger.error(error)
        return False

##############################################################################
    snap_count = 0
    for sn in db_controller.get_snapshots():
        if sn.lvol.get_id() == lvol_id:
            snap_count += 1

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    spdk_mem_info_before = rpc_client.ultra21_util_get_malloc_stats()

    num_blocks = int(lvol.size / lvol.distr_bs)
    new_vuid = utils.get_random_vuid()
    base_name = f"distr_{new_vuid}_{snap_count}"
    ret = rpc_client.bdev_distrib_create(
        base_name, new_vuid, lvol.ndcs, lvol.npcs, num_blocks,
        lvol.distr_bs, lvol_controller.get_jm_names(snode), lvol.distr_chunk_bs,
        None, None, lvol.distr_page_size, distrib_cpu_mask=snode.distrib_cpu_mask)
    if not ret:
        logger.error("Failed to create Distr bdev")
        return False, "Failed to create Distr bdev"

    ret = distr_controller.send_cluster_map_to_node(snode)
    if not ret:
        return False, "Failed to send cluster map"

    ret = rpc_client.ultra21_lvol_bmap_init(
        base_name, num_blocks, lvol.distr_bs, int(lvol.distr_page_size / lvol.distr_bs), num_blocks * 10)
    if not ret:
        return False, "Failed to init distr bdev"

    logger.info("Creating Snapshot bdev")
    lvol_name = f"lvol_{lvol.vuid}_{lvol.lvol_name}"
    snap_name = f"{lvol.snapshot_name}_{snap_count}"
    ret = rpc_client.ultra21_lvol_mount_snapshot(snap_name, lvol_name, base_name)
    if not ret:
        return False, "Failed to create snapshot lvol bdev"

##############################################################################
    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.snap_name = snapshot_name
    snap.base_bdev = base_name
    snap.snap_bdev = snap_name
    snap.created_at = int(time.time())
    snap.lvol = lvol


    spdk_mem_info_after = rpc_client.ultra21_util_get_malloc_stats()
    logger.debug("ultra21_util_get_malloc_stats:")
    logger.debug(spdk_mem_info_after)
    diff = {}
    for key in spdk_mem_info_after.keys():
        diff[key] = spdk_mem_info_after[key] - spdk_mem_info_before[key]
    logger.info("spdk mem diff:")
    logger.info(json.dumps(diff, indent=2))
    snap.mem_diff = diff
    snap.write_to_db(db_controller.kv_store)
    logger.info("Done")
    snapshot_events.snapshot_create(snap)
    return snap.uuid


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

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    if not snode:
        logger.error(f"Storage node not found {snap.lvol.node_id}")
        return False

    for lvol in db_controller.get_lvols(snode.cluster_id):
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

    ret = rpc_client.ultra21_lvol_dismount(snap.snap_bdev)
    if not ret:
        logger.error(f"Failed to delete BDev {snap.snap_bdev}")
        return False

    ret = rpc_client.bdev_distrib_delete(snap.base_bdev)
    if not ret:
        logger.error(f"Failed to delete BDev {snap.base_bdev}")
        return False

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

    pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
    if not pool:
        msg=f"Pool not found: {snap.lvol.pool_uuid}"
        logger.error(msg)
        return False, msg

    if pool.status == Pool.STATUS_INACTIVE:
        msg="Pool is disabled"
        logger.error(msg)
        return False, msg

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
        if lvol.pool_uuid == pool.get_id():
            if lvol.lvol_name == clone_name:
                msg=f"LVol name must be unique: {clone_name}"
                logger.error(msg)
                return False, msg

    # Validate cloning snap on storage node
    snode_api = SNodeClient(snode.api_endpoint)
    result, _ = snode_api.info()
    memory_free = result["memory_details"]["free"]
    huge_free = result["memory_details"]["huge_free"]
    total_node_capacity = db_controller.get_snode_size(snode.get_id())
    error = utils.validate_add_lvol_or_snap_on_node(memory_free, huge_free, snode.max_lvol, snap.lvol.size,  total_node_capacity, len(snode.lvols))
    if error:
        logger.error(error)
        return False, f"Failed to add lvol on node {snode.get_id()}"
    lvol = LVol()
    lvol.lvol_name = clone_name
    lvol.size = snap.lvol.size
    lvol.vuid = snap.lvol.vuid
    lvol.distr_bs = snap.lvol.distr_bs
    lvol.ndcs = snap.lvol.ndcs
    lvol.npcs = snap.lvol.npcs
    lvol.distr_chunk_bs = snap.lvol.distr_chunk_bs
    lvol.distr_page_size = snap.lvol.distr_page_size
    lvol.distr_page_size = snap.lvol.distr_page_size
    if new_size:
        if snap.lvol.size >= new_size:
            msg=f"New size {new_size} must be higher than the original size {snap.lvol.size}"
            logger.error(msg)
            return False, msg

        if snap.lvol.max_size < new_size:
            msg=f"New size {new_size} must be smaller than the max size {snap.lvol.max_size}"
            logger.error(msg)
            return False, msg
        lvol.size = new_size


    bdev_stack = []
    jm_names = lvol_controller.get_jm_names(snode)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    spdk_mem_info_before = rpc_client.ultra21_util_get_malloc_stats()

    num_blocks = int(lvol.size / lvol.distr_bs)
    new_vuid = utils.get_random_vuid()
    name = f"distr_{new_vuid}_1"
    ret = rpc_client.bdev_distrib_create(
        name, new_vuid, lvol.ndcs, lvol.npcs, num_blocks,
        lvol.distr_bs, jm_names, lvol.distr_chunk_bs, None, None, lvol.distr_page_size,
        distrib_cpu_mask=snode.distrib_cpu_mask)
    if not ret:
        msg="Failed to create Distr bdev"
        logger.error(msg)
        return False, msg
    if ret == "?":
        logger.error(f"Failed to create Distr bdev, ret={ret}")
        # return False

    logger.info("Sending cluster map to the lvol")
    ret = distr_controller.send_cluster_map_to_node(snode)
    if not ret:
        msg = "Failed to send cluster map"
        logger.error(msg)
        return False, msg

    logger.info("Creating clone bdev")
    block_len = lvol.distr_bs
    page_len = int(lvol.distr_page_size / lvol.distr_bs)
    max_num_blocks = snap.lvol.max_size
    ret = rpc_client.ultra21_lvol_bmap_init(name, num_blocks, block_len, page_len, max_num_blocks)
    if not ret:
        return False, "Failed to init distr bdev"

    lvol_name = f"clone_{lvol.vuid+2}_{lvol.lvol_name}"
    ret = rpc_client.ultra21_lvol_mount_clone(lvol_name, snap.snap_bdev, name, num_blocks)
    if not ret:
        return False, "Failed to create clone lvol bdev"

    ##############################################################################

    lvol_id = str(uuid.uuid4())
    # lvs_name = snap.lvol.lvol_bdev.split("/")[0]
    # lvol_bdev = f"{lvs_name}/{clone_name}"
    bdev_stack.append({"type": "ultra_lvol", "name": lvol_name})
    lvol_type = 'lvol'
    top_bdev = lvol_name

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

    lvol.vuid = new_vuid
    lvol.bdev_stack = bdev_stack
    lvol.uuid = lvol_id
    lvol.lvol_bdev = lvol_name
    lvol.top_bdev = top_bdev
    lvol.base_bdev = name
    lvol.hostname = snode.hostname
    lvol.node_id = snode.get_id()
    lvol.mode = 'read-write'
    lvol.lvol_type = lvol_type
    lvol.cloned_from_snap = snapshot_id
    lvol.nqn = subsystem_nqn
    lvol.pool_uuid = pool.id
    lvol.ha_type = snap.lvol.ha_type
    lvol.status = LVol.STATUS_ONLINE

    spdk_mem_info_after = rpc_client.ultra21_util_get_malloc_stats()
    logger.debug("ultra21_util_get_malloc_stats:")
    logger.debug(spdk_mem_info_after)

    diff = {}
    for key in spdk_mem_info_after.keys():
        diff[key] = spdk_mem_info_after[key] - spdk_mem_info_before[key]

    logger.info("spdk mem diff:")
    logger.info(json.dumps(diff, indent=2))
    lvol.mem_diff = diff

    pool.lvols.append(lvol_id)
    pool.write_to_db(db_controller.kv_store)
    lvol.write_to_db(db_controller.kv_store)
    snode.lvols.append(lvol_id)
    snode.write_to_db(db_controller.kv_store)
    logger.info("Done")
    snapshot_events.snapshot_clone(snap, lvol)

    return True, lvol_id
