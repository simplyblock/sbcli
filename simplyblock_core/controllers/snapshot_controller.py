# coding=utf-8
import datetime
import json
import logging as lg
import time
import uuid

from simplyblock_core.controllers import lvol_controller, snapshot_events, pool_controller

from simplyblock_core import utils, distr_controller, constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient


logger = lg.getLogger()

db_controller = DBController()


def add(lvol_id, snapshot_name):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        msg = f"LVol not found: {lvol_id}"
        logger.error(msg)
        return False, msg

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        msg = f"Pool is disabled"
        logger.error(msg)
        return False, msg

    if lvol.cloned_from_snap:
        snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        ref_count = snap.ref_count
        if snap.snap_ref_id:
            ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
            ref_count = ref_snap.ref_count

        if ref_count >= constants.MAX_SNAP_COUNT:
            msg = f"Can not create more than {constants.MAX_SNAP_COUNT} snaps from this clone"
            logger.error(msg)
            return False, msg

    for sn in db_controller.get_snapshots():
        if sn.cluster_id == pool.cluster_id:
            if sn.snap_name == snapshot_name:
                return False, f"Snapshot name must be unique: {snapshot_name}"

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.get_id()}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    rec = db_controller.get_lvol_stats(lvol, 1)
    if rec:
        size = rec[0].size_used
    else:
        size = lvol.size

    if 0 < pool.lvol_max_size < size:
        msg = f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, LVol size: {utils.humanbytes(size)} must be below this limit"
        logger.error(msg)
        return False, msg

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + size > pool.pool_max_size:
            msg =  f"Invalid LVol size: {utils.humanbytes(size)}. pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + lvol.size > pool.pool_max_size:
            msg = f"Pool max size has reached {utils.humanbytes(total)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in [cluster.STATUS_ACTIVE, cluster.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cluster.status}"

    snap_bdev_name = f"SNAP_{utils.get_random_vuid()}"
    size = lvol.size
    blobid = 0
    snap_uuid = ""
    used_size = 0

    if lvol.ha_type == "single":
        if snode.status == StorageNode.STATUS_ONLINE:
            rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
            logger.info("Creating Snapshot bdev")
            ret = rpc_client.lvol_create_snapshot(f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name)
            if not ret:
                return False, f"Failed to create snapshot on node: {snode.get_id()}"

            snap_bdev = rpc_client.get_bdevs(f"{lvol.lvs_name}/{snap_bdev_name}")
            if snap_bdev:
                snap_uuid = snap_bdev[0]['uuid']
                blobid = snap_bdev[0]['driver_specific']['lvol']['blobid']
                cluster_size = cluster.page_size_in_blocks
                num_allocated_clusters = snap_bdev[0]["driver_specific"]["lvol"]["num_allocated_clusters"]
                used_size = int(num_allocated_clusters*cluster_size)
        else:
            msg = f"Host node is not online {snode.get_id()}"
            logger.error(msg)
            return False, msg

    if lvol.ha_type == "ha":
        primary_node = None
        secondary_node = None
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
        lvol.nodes = [host_node.get_id(), host_node.secondary_node_id]

        if host_node.status == StorageNode.STATUS_ONLINE:
            primary_node = host_node
            if sec_node.status == StorageNode.STATUS_ONLINE:
                secondary_node = sec_node
            elif sec_node.status == StorageNode.STATUS_DOWN:
                msg = f"Secondary node is in down status, can not create snapshot"
                logger.error(msg)
                return False, msg
            else:
                # sec node is not online, set primary as leader
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            return False, msg

        if primary_node:
            rpc_client = RPCClient(
                primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username, primary_node.rpc_password)

            logger.info("Creating Snapshot bdev")
            ret = rpc_client.lvol_create_snapshot(f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name)
            if not ret:
                return False, f"Failed to create snapshot on node: {snode.get_id()}"

            snap_bdev = rpc_client.get_bdevs(f"{lvol.lvs_name}/{snap_bdev_name}")
            if snap_bdev:
                snap_uuid = snap_bdev[0]['uuid']
                blobid = snap_bdev[0]['driver_specific']['lvol']['blobid']
                cluster_size = cluster.page_size_in_blocks
                num_allocated_clusters = snap_bdev[0]["driver_specific"]["lvol"]["num_allocated_clusters"]
                used_size = int(num_allocated_clusters*cluster_size)
            else:
                return False, f"Failed to create snapshot on node: {snode.get_id()}"


        if secondary_node:
            sec_rpc_client = RPCClient(
                secondary_node.mgmt_ip, secondary_node.rpc_port, secondary_node.rpc_username, secondary_node.rpc_password)

            ret = sec_rpc_client.bdev_lvol_snapshot_register(
                f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name, snap_uuid, blobid)
            if not ret:
                msg = f"Failed to register snapshot on node: {sec_node.get_id()}"
                logger.error(msg)
                logger.info(f"Removing snapshot from {primary_node.get_id()}")
                rpc_client = RPCClient(
                    primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username, primary_node.rpc_password)
                ret = rpc_client.delete_lvol(f"{lvol.lvs_name}/{snap_bdev_name}")
                if not ret:
                    logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                return False, msg

    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.snap_uuid = snap_uuid
    snap.size = size
    snap.used_size = used_size
    snap.blobid = blobid
    snap.pool_uuid = pool.get_id()
    snap.cluster_id = pool.cluster_id
    snap.snap_name = snapshot_name
    snap.snap_bdev = f"{lvol.lvs_name}/{snap_bdev_name}"
    snap.created_at = int(time.time())
    snap.lvol = lvol

    snap.write_to_db(db_controller.kv_store)

    if lvol.cloned_from_snap:
        original_snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        if original_snap:
            if original_snap.snap_ref_id:
                original_snap = db_controller.get_snapshot_by_id(original_snap.snap_ref_id)

            original_snap.ref_count += 1
            original_snap.write_to_db(db_controller.kv_store)
            snap.snap_ref_id = original_snap.get_id()
            snap.write_to_db(db_controller.kv_store)

    logger.info("Done")
    snapshot_events.snapshot_create(snap)
    return snap.uuid, False


def list(all=False):
    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        logger.debug(snap)
        if snap.deleted is True and all is False:
            continue
        data.append({
            "UUID": snap.uuid,
            "Name": snap.snap_name,
            "Size": utils.humanbytes(snap.used_size),
            "ProvSize": utils.humanbytes(snap.size),
            "BDev": snap.snap_bdev,
            "LVol ID": snap.lvol.get_id(),
            "Created At": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(snap.created_at)),
            "Health": snap.health_check,
        })
    return utils.print_table(data)


def delete(snapshot_uuid, force_delete=False):
    snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    if not snap:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    if not snode:
        logger.error(f"Storage node not found {snap.lvol.node_id}")
        return False

    clones = []
    for lvol in db_controller.get_lvols(snode.cluster_id):
        if lvol.cloned_from_snap and lvol.cloned_from_snap == snapshot_uuid:
            clones.append(lvol)

    if len(clones) >= 1:
        logger.warning(f"Soft delete snapshot with clones")
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
        snap.deleted = True
        snap.write_to_db(db_controller.kv_store)
        return True

    logger.info(f"Removing snapshot: {snapshot_uuid}")

    if snap.lvol.ha_type == "single":
        if snode.status == StorageNode.STATUS_ONLINE:
            rpc_client = RPCClient(
                snode.mgmt_ip,
                snode.rpc_port,
                snode.rpc_username,
                snode.rpc_password)

            ret = rpc_client.delete_lvol(snap.snap_bdev)
            if not ret:
                logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                if not force_delete:
                    return False

        else:
            msg = f"Host node is not online {snode.get_id()}"
            logger.error(msg)
            return False

    else:

        secondary_node = None
        primary_node = None
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:
            if lvol_controller.is_node_leader(host_node, snap.lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = f"Secondary node is in down status, can not delete snapshot"
                    logger.error(msg)
                    return False

                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_node = sec_node
                else:
                    secondary_node = None

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if lvol_controller.is_node_leader(sec_node, snap.lvol.lvs_name):
                    primary_node = sec_node
                    secondary_node = host_node
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_node = sec_node

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # primary is not online but secondary is, create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            return False

        if primary_node:

            rpc_client = RPCClient(primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username,
                                       primary_node.rpc_password)

            ret = rpc_client.delete_lvol(snap.snap_bdev)
            if not ret:
                logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                if not force_delete:
                    return False

        if secondary_node:
            secondary_node = db_controller.get_storage_node_by_id(secondary_node.get_id())
            if secondary_node.status == StorageNode.STATUS_ONLINE:
                sec_rpc_client = RPCClient(secondary_node.mgmt_ip, secondary_node.rpc_port, secondary_node.rpc_username,
                                           secondary_node.rpc_password)
                ret = sec_rpc_client.delete_lvol(snap.snap_bdev)
                if not ret:
                    logger.error(f"Failed to delete snap from node: {secondary_node.get_id()}")
                    if not force_delete:
                        return False


    snap.remove(db_controller.kv_store)

    base_lvol = db_controller.get_lvol_by_id(snap.lvol.get_id())
    if base_lvol and base_lvol.deleted is True:
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

    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in [cluster.STATUS_ACTIVE, cluster.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cluster.status}"

    ref_count = snap.ref_count
    if snap.snap_ref_id:
        ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
        ref_count = ref_snap.ref_count

    if ref_count >= constants.MAX_SNAP_COUNT:
        msg = f"Can not create more than {constants.MAX_SNAP_COUNT} clones from this snapshot"
        logger.error(msg)
        return False, msg

    for lvol in db_controller.get_lvols():
        if lvol.pool_uuid == pool.get_id():
            if lvol.lvol_name == clone_name:
                msg=f"LVol name must be unique: {clone_name}"
                logger.error(msg)
                return False, msg

    size = snap.size
    if 0 < pool.lvol_max_size < size:
        msg = f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, LVol size: {utils.humanbytes(size)} must be below this limit"
        logger.error(msg)
        return False, msg

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + size > pool.pool_max_size:
            msg =  f"Invalid LVol size: {utils.humanbytes(size)}. Pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    lvol_count = len(db_controller.get_lvols_by_node_id(snode.get_id()))
    if lvol_count >= snode.max_lvol:
        error = f"Too many lvols on node: {snode.get_id()}, max lvols reached: {lvol_count}"
        logger.error(error)
        return False, error

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + snap.lvol.size > pool.pool_max_size:
            msg = f"Pool max size has reached {utils.humanbytes(total)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    lvol = LVol()
    lvol.uuid = str(uuid.uuid4())
    lvol.lvol_name = clone_name
    lvol.size = snap.lvol.size
    lvol.max_size = snap.lvol.max_size
    lvol.base_bdev = snap.lvol.base_bdev
    lvol.lvol_bdev = f"CLN_{utils.get_random_vuid()}"
    lvol.lvs_name = snap.lvol.lvs_name
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.hostname = snode.hostname
    lvol.node_id = snode.get_id()
    lvol.nodes = snap.lvol.nodes
    lvol.mode = 'read-write'
    lvol.cloned_from_snap = snapshot_id
    lvol.nqn = cluster.nqn + ":lvol:" + lvol.uuid
    lvol.pool_uuid = pool.get_id()
    lvol.ha_type = snap.lvol.ha_type
    lvol.lvol_type = 'lvol'
    lvol.guid = utils.generate_hex_string(16)
    lvol.vuid = snap.lvol.vuid
    lvol.snapshot_name = snap.snap_bdev
    lvol.subsys_port = snap.lvol.subsys_port

    lvol.status = LVol.STATUS_ONLINE
    lvol.bdev_stack = [
        {
            "type": "bdev_lvol_clone",
            "name": lvol.top_bdev,
            "params": {
                "snapshot_name": lvol.snapshot_name,
                "clone_name": lvol.lvol_bdev
            }
        }
    ]

    if snap.lvol.crypto_bdev:
        lvol.crypto_bdev = f"crypto_{lvol.lvol_bdev}"
        lvol.bdev_stack.append({
            "type": "crypto",
            "name": lvol.crypto_bdev,
            "params": {
                "name": lvol.crypto_bdev,
                "base_name": lvol.top_bdev,
                "key1": snap.lvol.crypto_key1,
                "key2": snap.lvol.crypto_key2,
            }
        })
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev
        lvol.crypto_key1 = snap.lvol.crypto_key1
        lvol.crypto_key2 = snap.lvol.crypto_key2

    if new_size:
        if snap.lvol.size >= new_size:
            msg = f"New size {new_size} must be higher than the original size {snap.lvol.size}"
            logger.error(msg)
            return False, msg

        if snap.lvol.max_size < new_size:
            msg = f"New size {new_size} must be smaller than the max size {snap.lvol.max_size}"
            logger.error(msg)
            return False, msg
        lvol.size = new_size

    lvol.write_to_db(db_controller.kv_store)

    if lvol.ha_type == "single":
        lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, snode)
        if error:
            return False, error
        lvol.nodes = [snode.get_id()]
        lvol.lvol_uuid = lvol_bdev['uuid']
        lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

    if lvol.ha_type == "ha":
        host_node = snode
        lvol.nodes = [host_node.get_id(), host_node.secondary_node_id]
        primary_node = None
        secondary_node = None
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:

            if lvol_controller.is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = f"Secondary node is in down status, can not clone snapshot"
                    logger.error(msg)
                    lvol.remove(db_controller.kv_store)
                    return False, msg

                if sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_node = sec_node

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if lvol_controller.is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    secondary_node = host_node
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_node = sec_node

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            lvol.remove(db_controller.kv_store)
            return False, msg


        if primary_node:
            lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, primary_node)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

        if secondary_node:
            lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, secondary_node, is_primary=False)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

    lvol.write_to_db(db_controller.kv_store)

    if snap.snap_ref_id:
        ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
        ref_snap.ref_count += 1
        ref_snap.write_to_db(db_controller.kv_store)
    else:
        snap.ref_count += 1
        snap.write_to_db(db_controller.kv_store)

    logger.info("Done")
    snapshot_events.snapshot_clone(snap, lvol)
    if new_size:
        lvol_controller.resize_lvol(lvol.get_id(), new_size)
    return lvol.uuid, False
