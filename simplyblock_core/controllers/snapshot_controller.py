# coding=utf-8
import builtins
import json
import logging as lg
import math
import time
import uuid
from datetime import datetime

from simplyblock_core.controllers import lvol_controller, snapshot_events, pool_controller, tasks_controller, \
    migration_controller

from simplyblock_core import utils
from simplyblock_core.kms import create_kms_connection
from simplyblock_core.kms._exceptions import KMSException
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


logger = lg.getLogger()

db_controller = DBController()


def _acquire_lvol_mutation_lock(node):
    """Block concurrent lvstore mutations while HA registration is in flight."""
    had_lock = node.lvol_sync_del()
    if not had_lock:
        node.lvol_del_sync_lock()
    return had_lock


def _release_lvol_mutation_lock(node, had_lock):
    if not had_lock:
        node.lvol_del_sync_lock_reset()


def _rollback_lvol_creation(lvol, node_ids):
    for node_id in dict.fromkeys(node_ids):
        try:
            lvol_controller.delete_lvol_from_node(lvol.get_id(), node_id)
        except Exception as e:
            logger.error(f"Failed to rollback lvol {lvol.get_id()} from node {node_id}: {e}")


def add(lvol_id, snapshot_name, backup=False, lock=True, all_snaps=None, all_lvols=None):
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError:
        logger.exception("Volume lookup failed for snapshot request: %s", lvol_id)
        return False, "Volume not found"

    # Reject snapshot creation on an lvol that is being deleted. SPDK's
    # blobstore reuses the lvol's metadata for the snapshot's parent
    # pointer; if the lvol is mid-delete (async or sync), creating a
    # snapshot from it can leave the resulting snapshot's parent_id
    # dangling and produce the open_ref/clone-entries inconsistency
    # that makes the snapshot undeletable until node restart.
    if lvol.status == LVol.STATUS_IN_DELETION:
        msg = (f"Cannot create snapshot from lvol {lvol_id}: "
               f"lvol is in deletion")
        logger.error(msg)
        return False, msg

    # Block during restart Phase 5
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        if snode.lvstore_status == "in_creation":
            msg = "Cannot create snapshot: node LVStore restart in progress"
            logger.error(msg)
            return False, msg
    except KeyError:
        pass

    # Block while a live volume migration holds the snapshot-freeze on this
    # source node. The migration runner freezes the source LVS to copy a
    # consistent snapshot chain; a snapshot created mid-migration races that
    # freeze and can corrupt the per-node snapshot plan. This enforces the
    # one-migration-per-source-node invariant the migration controller
    # documents but previously never checked (is_migration_active_on_node had
    # no callers). cluster_id is omitted because LVol has no cluster_id field;
    # the predicate matches on node_id, so an all-clusters scan is correct.
    try:
        if migration_controller.is_migration_active_on_node(lvol.node_id):
            msg = (f"Cannot create snapshot: a live volume migration is active "
                   f"on node {lvol.node_id}")
            logger.error(msg)
            return False, msg
    except Exception as e:
        logger.warning(f"Migration-active check failed for node {lvol.node_id}: {e}")

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        msg = "Pool is disabled"
        logger.error(msg)
        return False, msg

    if not all_snaps:
        all_snaps = db_controller.get_snapshots(pool.cluster_id)
    for sn in all_snaps:
        if sn.snap_name == snapshot_name:
            return False, f"Snapshot name must be unique: {snapshot_name}"

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    if snode.lvol_sync_del() and lock:
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

    if not all_lvols:
        all_lvols = db_controller.get_mini_lvols()
    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id(), all_lvols, all_snaps)
        if total + size > pool.pool_max_size:
            msg = f"Invalid LVol size: {utils.humanbytes(size)}. pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg
        if total + lvol.size > pool.pool_max_size:
            msg = f"Pool max size has reached {utils.humanbytes(total)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in [cluster.STATUS_ACTIVE, cluster.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cluster.status}"

    snap_vuid = utils.get_random_snapshot_vuid(all_lvols, all_snaps)
    snap_bdev_name = f"SNAP_{snap_vuid}"
    size = lvol.size
    blobid = 0
    snap_uuid = ""
    used_size = 0

    if lvol.ha_type == "single":
        if snode.status == StorageNode.STATUS_ONLINE:
            rpc_client = snode.rpc_client()
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
        from simplyblock_core.storage_node_ops import check_non_leader_for_operation

        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        # Build nodes list with all secondaries
        secondary_ids = [host_node.secondary_node_id]
        if host_node.tertiary_node_id:
            secondary_ids.append(host_node.tertiary_node_id)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        # Detect leader via RPC (no status checks)
        all_nodes = [host_node]
        for sid in secondary_ids:
            try:
                all_nodes.append(db_controller.get_storage_node_by_id(sid))
            except KeyError:
                pass

        primary_node = None
        secondary_nodes = []
        for candidate in all_nodes:
            try:
                if lvol_controller.is_node_leader(candidate, lvol.lvs_name):
                    primary_node = candidate
                    break
            except Exception:
                continue
        if not primary_node:
            primary_node = host_node

        # Check non-leader nodes (no status checks)
        for candidate in all_nodes:
            if candidate.get_id() == primary_node.get_id():
                continue
            action = check_non_leader_for_operation(
                candidate.get_id(), lvol.lvs_name, operation_type="create")
            if action == "reject":
                msg = f"Cannot create snapshot: non-leader {candidate.get_id()[:8]} unreachable but fabric healthy"
                logger.error(msg)
                return False, msg
            elif action == "proceed":
                secondary_nodes.append(candidate)
            # "skip", "queue" — handled by the registration gate below

        had_lock = False
        if lock:
            had_lock = _acquire_lvol_mutation_lock(host_node)

        try:
            if primary_node:
                rpc_client = primary_node.rpc_client()

                logger.info("Creating Snapshot bdev")
                ret = False
                for i in range(5):
                    ret, err = rpc_client.lvol_create_snapshot2(f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name)
                    if not ret:
                        if err and err.get("code") == -32602: # {"code": -32602, "message": "Device or resource busy"}}
                            logger.error(f"Failed to create snapshot, retrying: {err}")
                            time.sleep(0.1)
                        else:
                            break
                    else:
                        break
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
                # Per design: gate snapshot registration around restart port block.
                from simplyblock_core.storage_node_ops import wait_or_delay_for_restart_gate, queue_for_restart_drain
                gate = wait_or_delay_for_restart_gate(sec.get_id(), lvol.lvs_name)
                if gate == "delay":
                    queue_for_restart_drain(
                        sec.get_id(), lvol.lvs_name,
                        lambda s=sec: s.rpc_client().bdev_lvol_snapshot_register(
                            f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name, snap_uuid, blobid),
                        f"register snapshot {snap_bdev_name} on {sec.get_id()[:8]}")
                    continue

                sec_rpc_client = sec.rpc_client()

                ret = sec_rpc_client.bdev_lvol_snapshot_register(
                    f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name, snap_uuid, blobid)
                if not ret:
                    msg = f"Failed to register snapshot on node: {sec.get_id()}"
                    logger.error(msg)
                    logger.info(f"Removing snapshot from {primary_node.get_id()}")
                    rpc_client = primary_node.rpc_client()
                    ret, _ = rpc_client.delete_lvol(f"{lvol.lvs_name}/{snap_bdev_name}")
                    if not ret:
                        logger.error(f"Failed to delete snap from node: {snode.get_id()}")
                    return False, msg
        finally:
            if lock:
                _release_lvol_mutation_lock(host_node, had_lock)

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
    snap.create_dt = str(datetime.now())

    snap.write_to_db(db_controller.kv_store)

    _parent_snap = None
    if lvol.cloned_from_snap:
        _parent_snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        original_snap = _parent_snap
        if original_snap:
            if original_snap.snap_ref_id:
                original_snap = db_controller.get_snapshot_by_id(original_snap.snap_ref_id)

            # Atomic increment: a plain read-modify-write loses an increment
            # when two clones of the same snapshot run concurrently, which can
            # under-count ref_count and let a still-referenced snapshot be
            # deleted (data loss).
            if original_snap:
                original_snap = db_controller.atomic_update(
                    original_snap, lambda s: setattr(s, "ref_count", s.ref_count + 1))
            if original_snap:
                snap.snap_ref_id = original_snap.get_id()
                snap.write_to_db(db_controller.kv_store)

    for sn in all_snaps:
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
        lvol_snap = _parent_snap  # reuse fetch from above — same ID, no second DB read
        if lvol_snap and lvol_snap.source_replicated_snap_uuid:
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


def list(all=False, cluster_id=None, with_details=False, pool_id_or_name=None):
    if pool_id_or_name:
        try:
            pool = (
                    db_controller.get_pool_by_id(pool_id_or_name)
                    if utils.UUID_PATTERN.match(pool_id_or_name) is not None
                    else db_controller.get_pool_by_name(pool_id_or_name)
            )
            snaps = db_controller.get_snapshots_by_pool_id(pool.get_id())
        except KeyError:
            logger.error("Can not find pool with provided pool_id_or_name: %s", pool_id_or_name)
            return False
    else:
        snaps = db_controller.get_snapshots(cluster_id)

    snaps = sorted(snaps, key=lambda snap: snap.created_at)

    # Build set of lvol UUIDs with active migrations (single DB scan)
    migrating_lvols = []
    for m in db_controller.get_migrations():
        if m.is_active():
            migrating_lvols.append(m.lvol_id)
    # Build snap_id → clone list in one pass instead of rescanning all lvols
    # for every snapshot (was O(M×N) in-memory).
    clones_by_snap: dict[str, builtins.list[str]] = {}
    for lv in db_controller.get_mini_lvols():
        if lv.cloned_from_snap:
            clones_by_snap.setdefault(lv.cloned_from_snap, []).append(lv.get_id())

    data = []
    for snap in snaps:
        logger.debug(snap)
        clones = clones_by_snap.get(snap.get_id(), [])
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

    # Block during restart Phase 5
    snode = None
    try:
        snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
        if snode.lvstore_status == "in_creation" and not force_delete:
            logger.error(f"Cannot delete snapshot {snapshot_uuid}: node LVStore restart in progress")
            return False
    except KeyError:
        pass

    # Block deletion if the snapshot's parent volume is being migrated
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
        if snode is None:
            snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    except KeyError:
        logger.exception(f"Storage node not found {snap.lvol.node_id}")
        if force_delete:
            snap.remove(db_controller.kv_store)
            return True
        return False

    # A clone counts as "still blocking the snapshot" when either it's
    # alive (status != IN_DELETION) OR its SPDK-side delete hasn't
    # completed yet (deletion_status not set). The previous code only
    # excluded IN_DELETION clones unconditionally — that allowed the
    # snapshot's hard-delete to fire while SPDK still held the clone's
    # bdev open, returning EBUSY (-16) "Cannot remove snapshot because
    # it is open" and ultimately producing the open_ref / no-clone-
    # entries metadata inconsistency that requires a node restart.
    # Now we soft-delete the snapshot in that case; the clone's own
    # delete-completion path will re-trigger snapshot_controller.delete
    # once SPDK has actually removed the bdev (deletion_status set).
    clones = []
    in_deletion_clones = []
    for lvol in db_controller.get_mini_lvols():
        if not lvol.cloned_from_snap or lvol.cloned_from_snap != snapshot_uuid:
            continue

        if lvol.status == LVol.STATUS_IN_DELETION:
            in_deletion_clones.append(lvol)

        if lvol.status != LVol.STATUS_IN_DELETION:
            clones.append(lvol)
            continue
        # # IN_DELETION: only treat as gone if SPDK delete already
        # # completed for this clone (data-plane removed, just awaiting
        # # DB cleanup). Otherwise it's still in flight and blocks us.
        # if not getattr(lvol, "deletion_status", None):
        #     clones.append(lvol)

    if len(clones) >= 1:
        logger.warning(f"Soft delete snapshot with clones: {snapshot_uuid}")
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
        snap.deleted = True
        snap.write_to_db(db_controller.kv_store)
        return True

    # if there are no active clones and clones in status in_deletion found, then we
    # Defer delete the snapshot, meaning we switch snapshot status to in_deletion
    # and rely on the snapshot monitor to initiate the delete process once the clones
    # in deletion are fully deleted.
    elif len(in_deletion_clones) >= 1:
        logger.info(f"Defer deleting snapshot: {snapshot_uuid}")
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
        snap.status = SnapShot.STATUS_IN_DELETION
        snap.deletion_status = ""
        snap.write_to_db(db_controller.kv_store)
        return True

    logger.info(f"Removing snapshot: {snapshot_uuid}")

    if snap.lvol.ha_type == "single":
        if snode.status == StorageNode.STATUS_ONLINE:
            rpc_client = snode.rpc_client()

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

        # Detect leader via RPC (no status checks)
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        all_nodes = [host_node]
        if snode.secondary_node_id:
            try:
                all_nodes.append(db_controller.get_storage_node_by_id(snode.secondary_node_id))
            except KeyError:
                pass
        if snode.tertiary_node_id:
            try:
                all_nodes.append(db_controller.get_storage_node_by_id(snode.tertiary_node_id))
            except KeyError:
                pass

        primary_node = None
        for candidate in all_nodes:
            try:
                if lvol_controller.is_node_leader(candidate, snap.lvol.lvs_name):
                    primary_node = candidate
                    break
            except Exception:
                continue
        if not primary_node:
            primary_node = host_node

        rpc_client = primary_node.rpc_client()

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


def clone(snapshot_id, clone_name, new_size=0, pvc_name=None, pvc_namespace=None, delete_snap_on_lvol_delete=False,
          lock=True, namespaced=True, all_snaps=None, all_lvols=None):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_id)
    except KeyError:
        logger.exception("Snapshot lookup failed for clone request: %s", snapshot_id)
        return False, "Snapshot not found"

    # Reject cloning a snapshot that is in pending deletion. If a prior
    # clone-create failed (e.g. an SPDK duplicate-name collision on the
    # CLN_xxxx bdev) the mgmt layer issues an async snapshot delete; if
    # we let a fresh clone slip through that window, SPDK ends up with
    # the snapshot's parent metadata partially overwritten by the new
    # clone's lineage. The later sync delete then leaves the original
    # snapshot with non-zero open_ref but no clone entries, producing
    # the "Cannot remove snapshot because it is open" / EBUSY (-16)
    # state that requires a node restart to clear.
    if snap.deleted or snap.status == SnapShot.STATUS_IN_DELETION:
        msg = (f"Cannot clone snapshot {snapshot_id}: "
               f"snapshot is in deletion (deleted={snap.deleted}, "
               f"status={snap.status})")
        logger.error(msg)
        return False, msg

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

    # Block during restart Phase 5
    if snode.lvstore_status == "in_creation":
        msg = f"Cannot clone: node LVStore restart in progress on {snode.get_id()}"
        logger.error(msg)
        return False, msg

    if snode.lvol_sync_del() and lock:
        logger.error(f"LVol sync deletion found on node: {snode.get_id()}")
        return False, f"LVol sync deletion found on node: {snode.get_id()}"

    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in [cluster.STATUS_ACTIVE, cluster.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cluster.status}"

    if not all_lvols:
        all_lvols = db_controller.get_mini_lvols()
    for lvol in all_lvols:
        if lvol.pool_uuid != pool.get_id() or lvol.lvol_name != clone_name:
            continue
        if lvol.cloned_from_snap == snapshot_id:
            if lvol.status in [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION]:
                msg = f"Clone status {lvol.status} can not proceed"
                logger.error(msg)
                return False, msg
            logger.info(f"Clone already exists, reusing lvol: {lvol.get_id()}")
            return lvol.get_id(), False
        msg = f"LVol name must be unique: {clone_name}"
        logger.error(msg)
        return False, msg

    if not all_snaps:
        all_snaps = db_controller.get_snapshots()
    size = snap.size
    if 0 < pool.lvol_max_size < size:
        msg = f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, LVol size: {utils.humanbytes(size)} must be below this limit"
        logger.error(msg)
        return False, msg

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id(), all_lvols=all_lvols, all_snaps=all_snaps)
        if total + size > pool.pool_max_size:
            msg = f"Invalid LVol size: {utils.humanbytes(size)}. Pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    records = db_controller.get_cluster_capacity(cluster, 1)
    if records:
        rec = records[0]
        cluster_size_prov_util = int(((rec.size_prov+size) / rec.size_total) * 100)

        if cluster.prov_cap_crit and cluster.prov_cap_crit < cluster_size_prov_util:
            msg = f"Cluster provisioned cap critical would be, util: {cluster_size_prov_util}% of cluster util: {cluster.prov_cap_crit}"
            logger.error(msg)
            return False, msg

        elif cluster.prov_cap_warn and cluster.prov_cap_warn < cluster_size_prov_util:
            logger.warning(f"Cluster provisioned cap warning, util: {cluster_size_prov_util}% of cluster util: {cluster.prov_cap_warn}")


    # Resolve the namespace slot early so we can (a) skip the subsystem limit
    # check when the clone fits into an existing subsystem, and (b) reuse the
    # result below instead of calling get_next_available_subsystem_on_node twice.
    _available_subsys = lvol_controller.get_next_available_subsystem_on_node(snode.get_id(), all_lvols=all_lvols) if namespaced else None

    if not _available_subsys:
        subsys_count = len(set(
            lv.nqn for lv in all_lvols if lv.node_id == snode.get_id() and
            lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED]
        ))
        if subsys_count >= snode.max_lvol:
            error = f"Too many subsystems on node: {snode.get_id()}, max subsystems reached: {snode.max_lvol}"
            logger.error(error)
            return False, error

    clone_vuid = utils.get_random_vuid(all_lvols, all_snaps)
    lvol = LVol()
    lvol.uuid = str(uuid.uuid4())
    lvol.create_dt = str(datetime.now())
    lvol.lvol_name = clone_name
    lvol.size = snap.lvol.size
    lvol.max_size = snap.lvol.max_size
    lvol.base_bdev = snap.lvol.base_bdev
    lvol.lvol_bdev = f"CLN_{clone_vuid}"
    lvol.lvs_name = snap.lvol.lvs_name
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.hostname = snode.hostname
    lvol.node_id = snode.get_id()
    lvol.nodes = snap.lvol.nodes
    lvol.cloned_from_snap = snapshot_id
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    lvol.ha_type = snap.lvol.ha_type
    lvol.lvol_type = 'lvol'
    lvol.guid = utils.generate_hex_string(16)
    lvol.vuid = clone_vuid
    lvol.snapshot_name = snap.snap_bdev
    lvol.subsys_port = snap.lvol.subsys_port
    lvol.fabric = snap.fabric
    lvol.allowed_hosts = snap.lvol.allowed_hosts
    lvol.delete_snap_on_lvol_delete = bool(delete_snap_on_lvol_delete)
    lvol.ndcs = snap.lvol.ndcs
    lvol.npcs = snap.lvol.npcs

    # Create a new subsystem by default unless namespaced is set
    lvol.nqn = cluster.nqn + ":lvol:" + lvol.uuid
    lvol.max_namespace_per_subsys = snap.lvol.max_namespace_per_subsys

    if namespaced:
        # reuse the slot resolved above — avoids a second DB read
        if _available_subsys:
            lvol.nqn = _available_subsys.nqn
            lvol.namespace = _available_subsys.uuid
            lvol.max_namespace_per_subsys = _available_subsys.max_namespace_per_subsys

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

    if snap.lvol.crypto_bdev:
        with create_kms_connection(cluster) as kms:
            try:
                key1, key2 = kms.get_data_encryption_keys(snap.lvol)
                kms.import_data_encryption_keys(lvol, (key1, key2))
            except KMSException:
                msg = f"Failed to copy encryption keys for clone {lvol.crypto_bdev}"
                logger.exception(msg)
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
        from simplyblock_core.storage_node_ops import check_non_leader_for_operation, queue_for_restart_drain

        host_node = snode
        secondary_ids = [host_node.secondary_node_id]
        if host_node.tertiary_node_id:
            secondary_ids.append(host_node.tertiary_node_id)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        # Detect leader via RPC (no status checks)
        all_nodes = [host_node]
        for sid in secondary_ids:
            try:
                all_nodes.append(db_controller.get_storage_node_by_id(sid))
            except KeyError:
                pass

        primary_node = None
        secondary_nodes = []
        for candidate in all_nodes:
            try:
                if lvol_controller.is_node_leader(candidate, lvol.lvs_name):
                    primary_node = candidate
                    break
            except Exception:
                continue
        if not primary_node:
            primary_node = host_node

        # Assign each non-leader a stable index so its subsystem is created
        # with a unique cntlid window (sec0 -> min_cntlid 1000, sec1 -> 2000,
        # ...). CNTLID must be unique per subsystem across all paths on the
        # host; without distinct windows every secondary defaulted to
        # secondary_index=0 -> min_cntlid 1000 and the tertiary path collided
        # with the secondary ("Duplicate cntlid 1000 ... rejecting"). Keyed by
        # node id so the index is stable whether a node proceeds now or is
        # queued for deferred registration.
        secondary_index_map: dict[str,int]= {}
        for candidate in all_nodes:
            if candidate.get_id() == primary_node.get_id():
                continue
            secondary_index_map[candidate.get_id()] = len(secondary_index_map)

        # Check non-leader nodes (no status checks)
        for candidate in all_nodes:
            if candidate.get_id() == primary_node.get_id():
                continue
            action = check_non_leader_for_operation(
                candidate.get_id(), lvol.lvs_name, operation_type="create")
            if action == "reject":
                msg = f"Cannot clone: non-leader {candidate.get_id()[:8]} unreachable but fabric healthy"
                logger.error(msg)
                lvol.remove(db_controller.kv_store)
                return False, msg
            elif action == "proceed":
                secondary_nodes.append(candidate)
            elif action == "queue":
                queue_for_restart_drain(
                    candidate.get_id(), lvol.lvs_name,
                    lambda c=candidate, si=secondary_index_map[candidate.get_id()]:
                        lvol_controller.add_lvol_on_node(lvol, c, is_primary=False, secondary_index=si),
                    f"register clone {lvol.uuid} on {candidate.get_id()[:8]}")
            # "skip" — disconnected or pre_block, skip

        had_lock = False
        if lock:
            had_lock = _acquire_lvol_mutation_lock(host_node)

        try:
            if primary_node:
                lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, primary_node)
                if error:
                    logger.error(error)
                    if lvol.status != LVol.STATUS_IN_DELETION:
                        lvol.remove(db_controller.kv_store)
                    return False, error
                lvol.lvol_uuid = lvol_bdev['uuid']
                lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

            for sec in secondary_nodes:
                lvol_bdev, error = lvol_controller.add_lvol_on_node(
                    lvol, sec, is_primary=False,
                    secondary_index=secondary_index_map[sec.get_id()])
                if error:
                    logger.error(error)
                    if lvol.status != LVol.STATUS_IN_DELETION:
                        lvol.remove(db_controller.kv_store)
                    return False, error
        finally:
            if lock:
                _release_lvol_mutation_lock(host_node, had_lock)

    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db_controller.kv_store)

    # Atomic increment (see add() above): concurrent clones must not lose a
    # ref_count bump.
    if snap.snap_ref_id:
        ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
        if ref_snap:
            db_controller.atomic_update(ref_snap, lambda s: setattr(s, "ref_count", s.ref_count + 1))
    else:
        db_controller.atomic_update(snap, lambda s: setattr(s, "ref_count", s.ref_count + 1))

    logger.info("Done")
    snapshot_events.snapshot_clone(snap, lvol)
    if new_size and conv_new_size > snap.lvol.size:
        try:
            lvol_controller.resize_lvol(lvol.get_id(), new_size)
        except Exception:
            msg = "Resize failed"
            logger.exception(msg)
            return False, msg
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

    # Build snap_id → clone list once instead of a full DB read per snapshot
    # (was O(M×N) DB reads).
    clones_by_snap: dict[str, builtins.list[str]] = {}
    for lv in db_controller.get_mini_lvols():
        if lv.cloned_from_snap:
            clones_by_snap.setdefault(lv.cloned_from_snap, []).append(lv.get_id())

    data = []
    for snap in snaps:
        if node_id:
            if snap.lvol.node_id != node_id:
                continue
        logger.debug(snap)
        clones = clones_by_snap.get(snap.get_id(), [])
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
