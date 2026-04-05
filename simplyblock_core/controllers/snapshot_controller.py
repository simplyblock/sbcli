# coding=utf-8
import json
import logging as lg
import math
import time
import uuid

from simplyblock_core.controllers import lvol_controller, snapshot_events, pool_controller, tasks_controller

from simplyblock_core import utils, constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


logger = lg.getLogger()

db_controller = DBController()


def add(lvol_id, snapshot_name, backup=False):
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        msg = "Pool is disabled"
        logger.error(msg)
        return False, msg

    if lvol.cloned_from_snap:
        try:
            snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            ref_count = snap.ref_count
            if snap.snap_ref_id:
                ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
                ref_count = ref_snap.ref_count

            if ref_count >= constants.MAX_SNAP_COUNT:
                msg = f"Can not create more than {constants.MAX_SNAP_COUNT} snaps from this clone"
                logger.error(msg)
                return False, msg
        except KeyError:
            pass

    for sn in db_controller.get_snapshots():
        if sn.cluster_id == pool.cluster_id:
            if sn.snap_name == snapshot_name:
                return False, f"Snapshot name must be unique: {snapshot_name}"

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    if snode.lvol_sync_del():
        logger.error(f"LVol sync deletion found on node: {snode.get_id()}")
        return False, f"LVol sync deletion found on node: {snode.get_id()}"

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.get_id()}")

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

    snap_vuid = utils.get_random_snapshot_vuid()
    snap_bdev_name = f"SNAP_{snap_vuid}"
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
        secondary_nodes = []
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)

        # Build nodes list with all secondaries
        secondary_ids = [host_node.secondary_node_id]
        if host_node.secondary_node_id_2:
            secondary_ids.append(host_node.secondary_node_id_2)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        if host_node.status == StorageNode.STATUS_ONLINE:
            if lvol_controller.is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = "Secondary node is in down status, can not create snapshot"
                    logger.error(msg)
                    return False, msg
                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_nodes.append(sec_node)

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if lvol_controller.is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    if host_node.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(host_node)
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_nodes.append(sec_node)

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # create on secondary and set leader if needed,
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = "Host nodes are not online"
            logger.error(msg)
            return False, msg

        # Add additional secondaries if online
        for extra_sec_id in secondary_ids[1:]:
            try:
                extra_sec = db_controller.get_storage_node_by_id(extra_sec_id)
                if extra_sec.status == StorageNode.STATUS_ONLINE and extra_sec.get_id() != (primary_node.get_id() if primary_node else None):
                    secondary_nodes.append(extra_sec)
            except KeyError:
                pass

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

        for sec in secondary_nodes:
            sec_rpc_client = RPCClient(
                sec.mgmt_ip, sec.rpc_port, sec.rpc_username, sec.rpc_password)

            ret = sec_rpc_client.bdev_lvol_snapshot_register(
                f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name, snap_uuid, blobid)
            if not ret:
                msg = f"Failed to register snapshot on node: {sec.get_id()}"
                logger.error(msg)
                logger.info(f"Removing snapshot from {primary_node.get_id()}")
                rpc_client = RPCClient(
                    primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username, primary_node.rpc_password)
                ret, _ = rpc_client.delete_lvol(f"{lvol.lvs_name}/{snap_bdev_name}")
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
    snap.fabric = lvol.fabric
    snap.vuid = snap_vuid
    snap.status = SnapShot.STATUS_ONLINE

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

    for sn in db_controller.get_snapshots(cluster.get_id()):
        if sn.get_id() == snap.get_id():
            continue
        if sn.lvol.get_id() == lvol_id:
            if not sn.next_snap_uuid:
                sn.next_snap_uuid = snap.get_id()
                snap.prev_snap_uuid = sn.get_id()
                sn.write_to_db()
                snap.write_to_db()
                break

    snapshot_events.snapshot_create(snap)
    if lvol.do_replicate:
        task = tasks_controller.add_snapshot_replication_task(snap.cluster_id, snap.lvol.node_id, snap.get_id())
        if task:
            snapshot_events.replication_task_created(snap)
    if lvol.cloned_from_snap:
        lvol_snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        if lvol_snap.source_replicated_snap_uuid:
            try:
                org_snap = db_controller.get_snapshot_by_id(lvol_snap.source_replicated_snap_uuid)
                if org_snap and org_snap.status == SnapShot.STATUS_ONLINE:
                    task = tasks_controller.add_snapshot_replication_task(
                        snap.cluster_id, org_snap.lvol.node_id, snap.get_id(), replicate_to_source=True)
                    if task:
                        logger.info("Created snapshot replication task on original node")
            except KeyError:
                pass

    if backup:
        from simplyblock_core.controllers import backup_controller
        backup_id, backup_err = backup_controller.backup_snapshot(snap.uuid)
        if backup_err:
            logger.warning(f"Snapshot created but backup failed: {backup_err}")

    return snap.uuid, False


def list(all=False, cluster_id=None, with_details=False):
    snaps = db_controller.get_snapshots(cluster_id)
    snaps = sorted(snaps, key=lambda snap: snap.created_at)

    # Build set of lvol UUIDs with active migrations (single DB scan)
    migrating_lvols = []
    for m in db_controller.get_migrations():
        if m.is_active():
            migrating_lvols.append(m.lvol_id)
    data = []
    for snap in snaps:
        logger.debug(snap)
        clones = []
        for lvol in db_controller.get_lvols():
            if lvol.cloned_from_snap and lvol.cloned_from_snap == snap.get_id():
                clones.append(lvol.get_id())
        d = {
            "UUID": snap.uuid,
            "BDdev UUID": snap.snap_uuid,
            "BlobID": snap.blobid,
            "Name": snap.snap_name,
            "Size": utils.humanbytes(snap.used_size),
            "BDev": snap.snap_bdev,
            "Node ID": snap.lvol.node_id,
            "LVol ID": snap.lvol.get_id(),
            "M": "M" if snap.lvol and snap.lvol.uuid in migrating_lvols else "",
            "Created At": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(snap.created_at)),
            "Base Snapshot": snap.snap_ref_id,
            "Clones": clones,
            "Status": snap.status,
        }
        if with_details:
            d["Replication target snap"] = snap.target_replicated_snap_uuid
            d["Replication source snap"] = snap.source_replicated_snap_uuid
            d["Rrev snap"] = snap.prev_snap_uuid
            d["Next snap"] = snap.next_snap_uuid
        data.append(d)
    return utils.print_table(data)


def delete(snapshot_uuid, force_delete=False):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    except KeyError:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    if snap.status == SnapShot.STATUS_IN_DELETION:
        logger.error(f"Snapshot is in deletion {snapshot_uuid}")
        if not force_delete:
            return True

    # Block deletion if the snapshot's parent volume is being migrated
    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(
        snap.lvol.uuid, snap.cluster_id)
    if active_mig and not force_delete:
        logger.error(
            f"Cannot delete snapshot {snapshot_uuid}: parent volume "
            f"{snap.lvol.uuid} has active migration {active_mig.uuid}")
        return False

    # Block deletion if a backup referencing this snapshot is still in progress
    if not force_delete:
        from simplyblock_core.models.backup import Backup
        backups = db_controller.get_backups_by_snapshot_id(snapshot_uuid)
        active_backups = [b for b in backups if b.status in (
            Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS)]
        if active_backups:
            logger.error(
                f"Cannot delete snapshot {snapshot_uuid}: "
                f"{len(active_backups)} backup(s) still in progress")
            return False

    if snap.status == SnapShot.STATUS_IN_REPLICATION:
        logger.error("Snapshot is in replication")
        return False

    try:
        snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    except KeyError:
        logger.exception(f"Storage node not found {snap.lvol.node_id}")
        if force_delete:
            snap.remove(db_controller.kv_store)
            return True
        return False

    clones = []
    for lvol in db_controller.get_lvols(snode.cluster_id):
        if lvol.cloned_from_snap and lvol.cloned_from_snap == snapshot_uuid and lvol.status != LVol.STATUS_IN_DELETION:
            clones.append(lvol)

    if len(clones) >= 1:
        logger.warning(f"Soft delete snapshot with clones: {snapshot_uuid}")
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

            ret, _ = rpc_client.delete_lvol(snap.snap_bdev)
            if not ret:
                logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                if not force_delete:
                    return False
            snap = db_controller.get_snapshot_by_id(snapshot_uuid)
            snap.status = SnapShot.STATUS_IN_DELETION
            snap.deletion_status = snode.get_id()
            snap.write_to_db(db_controller.kv_store)
        else:
            msg = f"Host node is not online {snode.get_id()}"
            logger.error(msg)
            return False

    else:

        primary_node = None
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        sec_nodes = []
        if snode.secondary_node_id:
            sec_nodes.append(db_controller.get_storage_node_by_id(snode.secondary_node_id))
        if snode.secondary_node_id_2:
            sec_nodes.append(db_controller.get_storage_node_by_id(snode.secondary_node_id_2))

        if host_node.status == StorageNode.STATUS_ONLINE:
            if lvol_controller.is_node_leader(host_node, snap.lvol.lvs_name):
                primary_node = host_node
                # Check if any secondary is in DOWN status
                for sec_node in sec_nodes:
                    if sec_node.status == StorageNode.STATUS_DOWN:
                        msg = "Secondary node is in down status, can not delete snapshot"
                        logger.error(msg)
                        return False
            else:
                # Check if any secondary is the leader
                for sec_node in sec_nodes:
                    if sec_node.status == StorageNode.STATUS_ONLINE and \
                            lvol_controller.is_node_leader(sec_node, snap.lvol.lvs_name):
                        primary_node = sec_node
                        break
                if not primary_node:
                    # no secondary is leader, use host as leader
                    primary_node = host_node

        else:
            # host is not online, find an online secondary
            for sec_node in sec_nodes:
                if sec_node.status == StorageNode.STATUS_ONLINE:
                    primary_node = sec_node
                    break

        if not primary_node:
            msg = "Host nodes are not online"
            logger.error(msg)
            return False

        if not primary_node:
            msg = "Host nodes are not online"
            logger.error(msg)
            return False

        rpc_client = RPCClient(primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username,
                                   primary_node.rpc_password)

        ret, _ = rpc_client.delete_lvol(snap.snap_bdev)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")
            if not force_delete:
                return False
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
        snap.deletion_status = primary_node.get_id()
        snap.status = SnapShot.STATUS_IN_DELETION
        snap.write_to_db(db_controller.kv_store)

    try:
        base_lvol = db_controller.get_lvol_by_id(snap.lvol.get_id())
        if base_lvol and base_lvol.deleted is True:
            lvol_controller.delete_lvol(base_lvol.get_id())
    except KeyError:
        pass

    if snap.target_replicated_snap_uuid:
        delete_replicated(snap.uuid)

    logger.info("Done")
    return True


def clone(snapshot_id, clone_name, new_size=0, pvc_name=None, pvc_namespace=None, delete_snap_on_lvol_delete=False):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    try:
        pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
    except KeyError:
        msg=f"Pool not found: {snap.lvol.pool_uuid}"
        logger.error(msg)
        return False, msg

    if pool.status == Pool.STATUS_INACTIVE:
        msg="Pool is disabled"
        logger.error(msg)
        return False, msg

    try:
        snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    except KeyError:
        msg = 'Storage node not found'
        logger.exception(msg)
        return False, msg

    if snode.lvol_sync_del():
        logger.error(f"LVol sync deletion found on node: {snode.get_id()}")
        return False, f"LVol sync deletion found on node: {snode.get_id()}"

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

    subsys_count = len(set(lv.nqn for lv in db_controller.get_lvols_by_node_id(snode.get_id())))
    if subsys_count >= snode.max_lvol:
        error = f"Too many subsystems on node: {snode.get_id()}, max subsystems reached: {subsys_count}"
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
    lvol.pool_uuid = pool.get_id()
    lvol.ha_type = snap.lvol.ha_type
    lvol.lvol_type = 'lvol'
    lvol.guid = utils.generate_hex_string(16)
    lvol.vuid = snap.lvol.vuid
    lvol.snapshot_name = snap.snap_bdev
    lvol.subsys_port = snap.lvol.subsys_port
    lvol.fabric = snap.fabric
    lvol.allowed_hosts = snap.lvol.allowed_hosts
    lvol.delete_snap_on_lvol_delete = bool(delete_snap_on_lvol_delete)
    lvol.ndcs = snap.lvol.ndcs
    lvol.npcs = snap.lvol.npcs

    # Inherit namespace sharing from the source lvol.  Find the master
    # lvol that owns the subsystem (either the source itself or its master
    # if the source is already a namespace member).
    source_lvol = snap.lvol
    if source_lvol.namespace:
        try:
            master_lvol = db_controller.get_lvol_by_id(source_lvol.namespace)
        except KeyError:
            master_lvol = source_lvol
    else:
        master_lvol = source_lvol

    if master_lvol.max_namespace_per_subsys > 1:
        # Count how many lvols currently share this master's subsystem
        ns_count = 0
        for lv in db_controller.get_lvols(cluster.get_id()):
            if lv.nqn == master_lvol.nqn and lv.status not in [LVol.STATUS_IN_DELETION]:
                ns_count += 1
        if ns_count < master_lvol.max_namespace_per_subsys:
            lvol.nqn = master_lvol.nqn
            lvol.namespace = master_lvol.get_id()
        else:
            # Subsystem full — create a new one but keep the same capacity
            lvol.nqn = cluster.nqn + ":lvol:" + lvol.uuid
            lvol.max_namespace_per_subsys = master_lvol.max_namespace_per_subsys
    else:
        lvol.nqn = cluster.nqn + ":lvol:" + lvol.uuid

    if pvc_name:
        lvol.pvc_name = pvc_name
    if pvc_namespace and not lvol.namespace:
        lvol.namespace = pvc_namespace

    lvol.status = LVol.STATUS_IN_CREATION
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

    conv_new_size = 0
    if new_size:
        conv_new_size = math.ceil(new_size / (1024 * 1024 * 1024)) * 1024 * 1024 * 1024
        if snap.lvol.size > conv_new_size:
            msg = f"New size {conv_new_size} must be higher than the original size {snap.lvol.size}"
            logger.error(msg)
            return False, msg

        if snap.lvol.max_size < conv_new_size:
            msg = f"New size {conv_new_size} must be smaller than the max size {snap.lvol.max_size}"
            logger.error(msg)
            return False, msg

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
        # Build nodes list with all secondaries
        secondary_ids = [host_node.secondary_node_id]
        if host_node.secondary_node_id_2:
            secondary_ids.append(host_node.secondary_node_id_2)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        primary_node = None
        secondary_nodes = []
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:

            if lvol_controller.is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = "Secondary node is in down status, can not clone snapshot"
                    logger.error(msg)
                    lvol.remove(db_controller.kv_store)
                    return False, msg

                if sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_nodes.append(sec_node)

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if lvol_controller.is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    if host_node.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(host_node)
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_nodes.append(sec_node)

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # create on secondary and set leader if needed,
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = "Host nodes are not online"
            logger.error(msg)
            lvol.remove(db_controller.kv_store)
            return False, msg

        # Add additional secondaries if online
        for extra_sec_id in secondary_ids[1:]:
            try:
                extra_sec = db_controller.get_storage_node_by_id(extra_sec_id)
                if extra_sec.status == StorageNode.STATUS_ONLINE and extra_sec.get_id() != (primary_node.get_id() if primary_node else None):
                    secondary_nodes.append(extra_sec)
            except KeyError:
                pass

        if primary_node:
            lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, primary_node)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

        for sec in secondary_nodes:
            lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, sec, is_primary=False)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

    lvol.status = LVol.STATUS_ONLINE
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
    if new_size and conv_new_size > snap.lvol.size:
        lvol_controller.resize_lvol(lvol.get_id(), new_size)
    return lvol.uuid, False


def list_replication_tasks(cluster_id):
    tasks = db_controller.get_job_tasks(cluster_id)

    data = []
    for task in tasks:
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            logger.debug(task)
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue

            duration = ""
            try:
                if task.status == JobSchedule.STATUS_RUNNING:
                    duration = utils.strfdelta_seconds(int(time.time()) - task.function_params["start_time"])
                elif "end_time" in task.function_params:
                    duration = utils.strfdelta_seconds(
                        task.function_params["end_time"] - task.function_params["start_time"])
            except Exception as e:
                logger.error(e)
            status = task.status
            if task.canceled:
                status = "cancelled"
            replicate_to = "target"
            if "replicate_to_source" in task.function_params:
                if task.function_params["replicate_to_source"] is True:
                    replicate_to = "source"
            offset = 0
            if "offset" in task.function_params:
                offset = task.function_params["offset"]
            data.append({
                "Task ID": task.uuid,
                "Snapshot ID": snap.uuid,
                "Size": utils.humanbytes(snap.used_size),
                "Duration": duration,
                "Offset": offset,
                "Status": status,
                "Replicate to": replicate_to,
                "Result": task.function_result,
                "Cluster ID": task.cluster_id,
            })
    return utils.print_table(data)


def delete_replicated(snapshot_id):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_id)
    except KeyError:
        logger.error(f"Snapshot not found {snapshot_id}")
        return False

    try:
        target_replicated_snap = db_controller.get_snapshot_by_id(snap.target_replicated_snap_uuid)
        logger.info("Deleting replicated snapshot %s", target_replicated_snap.uuid)
        ret = delete(target_replicated_snap.uuid)
        if not ret:
            logger.error("Failed to delete snapshot %s", target_replicated_snap.uuid)
            return False

    except KeyError:
        logger.error(f"Snapshot not found {snap.target_replicated_snap_uuid}")
        return False

    return True


def get(snapshot_uuid):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    except KeyError:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    return json.dumps(snap.get_clean_dict(), indent=2)


def set_value(snapshot_uuid, attr, value) -> bool:
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    except KeyError:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    if attr not in snap.get_attrs_map():
        raise KeyError('Attribute not found')

    value = snap.get_attrs_map()[attr]['type'](value)
    logger.info(f"Setting {attr} to {value}")
    setattr(snap, attr, value)
    snap.write_to_db()
    return True

def list_by_node(node_id=None, is_json=False):
    snaps = db_controller.get_snapshots()
    snaps = sorted(snaps, key=lambda snap: snap.created_at)
    data = []
    for snap in snaps:
        if node_id:
            if snap.lvol.node_id != node_id:
                continue
        logger.debug(snap)
        clones = []
        for lvol in db_controller.get_lvols():
            if lvol.cloned_from_snap and lvol.cloned_from_snap == snap.get_id():
                clones.append(lvol.get_id())
        data.append({
            "UUID": snap.uuid,
            "BDdev UUID": snap.snap_uuid,
            "BlobID": snap.blobid,
            "Name": snap.snap_name,
            "Size": utils.humanbytes(snap.used_size),
            "BDev": snap.snap_bdev.split("/")[1],
            "Node ID": snap.lvol.node_id,
            "LVol ID": snap.lvol.get_id(),
            "Created At": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(snap.created_at)),
            "Base Snapshot": snap.snap_ref_id,
            "Clones": clones,
            "Status": snap.status,
        })
    if is_json:
        return json.dumps(data, indent=2)
    return utils.print_table(data)