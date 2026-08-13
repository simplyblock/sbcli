# coding=utf-8
import time
import uuid
from typing import Optional

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import lvol_controller, snapshot_events, snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)
# get DB controller
db = db_controller.DBController()


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    replicate_to_source = task.function_params["replicate_to_source"]
    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        if replicate_to_source:
            org_snap = db.get_snapshot_by_id(snapshot.source_replicated_snap_uuid)
            try:
                remote_node_uuid = db.get_storage_node_by_id(task.node_id)
            except KeyError:
                msg = f"Unable to find node: {task.node_id}, stopping task"
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            remote_pool_uuid = org_snap.lvol.pool_uuid
        else:  # replicate to target
            remote_node_uuid = db.get_storage_node_by_id(snapshot.lvol.replication_node_id)
            cluster = db.get_cluster_by_id(remote_node_uuid.cluster_id)
            remote_pool_uuid = None
            if cluster.snapshot_replication_target_pool:
                remote_pool_uuid = cluster.snapshot_replication_target_pool
            else:
                for bool in db.get_pools(remote_node_uuid.cluster_id):
                    if bool.status == Pool.STATUS_ACTIVE:
                        remote_pool_uuid = bool.uuid
                        break
            if not remote_pool_uuid:
                logger.error(f"Unable to find pool on remote cluster: {remote_node_uuid.cluster_id}")
                return

        lv_id, err = lvol_controller.add_lvol_ha(
            f"REP_{snapshot.snap_name}", snapshot.size, remote_node_uuid.get_id(), snapshot.lvol.ha_type,
            remote_pool_uuid)
        if lv_id:
            task.function_params["remote_lvol_id"] = lv_id
            task.write_to_db()
        else:
            logger.error(err)
            task.function_result = "Error creating remote lvol"
            task.write_to_db()
            return

    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    remote_lv_node = db.get_storage_node_by_id(remote_lv.node_id)
    if remote_lv_node.status != StorageNode.STATUS_ONLINE:
        task.function_result = "Target node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    # 2 attach the TARGET NODE'S TRANSFER HUBLVOL on the source. Transfers must
    # go over a hublvol: the fork demuxes each write by the map id carried in
    # the top 16 bits of the LBA (lvol_map.lvol[offset >> 48]) and that demux
    # only exists on a hublvol namespace. The receiving volume's own namespace
    # is not a valid transfer gateway. This mirrors the (working) migration
    # runner, which has always sent bulk transfers hub+map_id.
    from simplyblock_core.services.replication_final_step import ensure_hub_attached
    _hub_ctrl, hub_bdev, hub_err = ensure_hub_attached(snode.rpc_client(), remote_lv_node)
    if hub_err:
        logger.error(f"Transfer hub attach failed: {hub_err}")
        task.function_result = "transfer hub attach failed, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    # The receiving volume's map id rides in every write's LBA (see above); the
    # hub uses it to route the data into the receiving volume. Without it the
    # transfer cannot land.
    ret = remote_lv_node.rpc_client().get_bdevs(remote_lv.top_bdev)
    try:
        remote_map_id = ret[0]["driver_specific"]["lvol"]["map_id"]
    except (TypeError, KeyError, IndexError):
        remote_map_id = None
    if not remote_map_id:
        logger.error(f"map_id of receiving lvol {remote_lv.top_bdev} not found on "
                     f"{remote_lv.node_id}; not starting a transfer that cannot land")
        task.function_result = "receiving lvol map_id unavailable, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    # NOTE deliberately NO bdev_lvol_set_migration_flag here: the flag drives the
    # distrib-level special_io machinery of INTRA-cluster migration; it has no
    # place in a cross-cluster receive (the source cluster's map/COW context does
    # not exist on the target cluster).
    # The hub rejects receive IO on a non-leader ("receive io for hublvol in
    # nonleader mode"); do not start a transfer that cannot land.
    if not _require_lvs_leader(remote_lv_node, remote_lv.lvs_name, "transfer receive"):
        task.function_result = "target node not LVS leader, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    offset = 0
    if "offset" in task.function_params and task.function_params["offset"]:
        offset = task.function_params["offset"]
    # 3 start replication
    snode.rpc_client().bdev_lvol_transfer(
        name=snapshot.snap_bdev,
        offset=offset,
        batch_size=16,
        bdev_name=hub_bdev,
        operation="replicate",
        lvol_id=remote_map_id
    )
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params["start_time"] = int(time.time())
    task.write_to_db()

    if snapshot.status != SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_IN_REPLICATION
        snapshot.write_to_db()


def _require_lvs_leader(node, lvs_name, what):
    """True when *node* currently holds LVS leadership for *lvs_name*.

    Transfers into a hub on a non-leader fail loudly, but bdev_lvol_convert on a
    non-leader DEGRADES SILENTLY: the fork's non-leader branch marks the blob
    CLEAN and replies success without persisting anything — the "snapshot"
    looks converted while its metadata never reached the journal. Leadership
    must therefore be verified BEFORE the operation; on False the caller
    suspends and retries rather than proceeding.
    """
    from simplyblock_core.controllers import lvol_controller
    if lvol_controller.is_node_leader(node, lvs_name):
        return True
    logger.error("Node %s is not LVS leader of %s — refusing %s (retry)",
                 node.get_id(), lvs_name, what)
    return False


def _other_active_transfers_to_node(current_task, target_node_id):
    """True when another RUNNING snapshot-replication task is transferring into
    *target_node_id* — its writes ride the same shared hub session, so the hub
    must not be detached under it."""
    for t in db.get_job_tasks(current_task.cluster_id):
        if (t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
                and t.get_id() != current_task.get_id()
                and t.status == JobSchedule.STATUS_RUNNING):
            rid = t.function_params.get("remote_lvol_id")
            if not rid:
                continue
            try:
                if db.get_lvol_by_id(rid).node_id == target_node_id:
                    return True
            except KeyError:
                continue
    return False


def _has_dependent_clone(snapshot_uuid):
    """True when any live volume is cloned from *snapshot_uuid*.

    A failed-over volume is a clone of the last replicated target snapshot, so
    that snapshot must outlive it. Uses the mini index (same source the snapshot
    delete path consults) and ignores volumes that are themselves going away.
    """
    for lvol in db.get_mini_lvols():
        if lvol.cloned_from_snap != snapshot_uuid:
            continue
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        return True
    return False


def _prune_internal_snapshots(source_lvol):
    """Retention for replication-driven internal snapshots.

    Internal snapshots are transient checkpoints taken at a fixed interval
    purely to drive replication. Once a newer internal snapshot has been
    successfully replicated, the older internal snapshots are redundant: they
    are removed on BOTH the target (the explicit requirement — only the last
    replicated internal snapshot persists there) and the source (so the source
    snapshot chain stays bounded). User snapshots are never auto-deleted, on
    either side.

    Only snapshots strictly older than the most-recent replicated internal
    snapshot are pruned, so the newest internal snapshot — which serves as the
    base for the next delta transfer — always remains.
    """
    replicated_internal = [
        s for s in db.get_snapshots_by_node_id(source_lvol.node_id)
        if s.lvol.get_id() == source_lvol.get_id()
        and s.snap_type == SnapShot.TYPE_INTERNAL
        and s.status == SnapShot.STATUS_ONLINE
        and s.target_replicated_snap_uuid
    ]
    if len(replicated_internal) <= 1:
        return

    replicated_internal.sort(key=lambda s: s.created_at)
    # Keep the newest replicated internal snapshot; prune everything older.
    for snap in replicated_internal[:-1]:
        target_uuid = snap.target_replicated_snap_uuid
        try:
            db.get_snapshot_by_id(target_uuid)
        except KeyError:
            target_uuid = ""  # already gone — fall through to source cleanup
        if target_uuid and _has_dependent_clone(target_uuid):
            # Never prune a target snapshot a volume is cloned from. The delete
            # reaches SPDK as bdev_lvol_delete(sync=False) and frees the blocks
            # there and then, so no downstream DB-level guard can save the clone:
            # a failed-over volume built on this snapshot would silently start
            # reading zeros. Keep both copies; the pair is released once the
            # dependent volume is gone.
            logger.info("Keeping replicated internal snapshot %s on source and "
                        "%s on target: a volume is cloned from the target copy",
                        snap.get_id(), target_uuid)
            continue
        if target_uuid:
            logger.info("Pruning replicated internal snapshot on target: %s", target_uuid)
            if not snapshot_controller.delete(target_uuid):
                logger.warning("Failed to delete target internal snapshot %s, will retry", target_uuid)
                continue
        logger.info("Pruning internal snapshot on source: %s", snap.get_id())
        if not snapshot_controller.delete(snap.get_id()):
            logger.warning("Failed to delete source internal snapshot %s, will retry", snap.get_id())


def process_snap_replicate_finish(task, snapshot):

    # Close the transfer session — but ONLY when this was the last active
    # transfer into that target node. The hub is ONE shared session per target
    # node: a naive per-cycle detach rips the qpair out from under the other
    # volumes' in-flight transfers, mass-failing their IO on the hub and
    # churning LVS leadership on the target ("receive io for hublvol in
    # nonleader mode" storms, observed live 2026-08-13). This is the refcount
    # discipline the migration runner's hub_manager exists for.
    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    remote_snode = db.get_storage_node_by_id(remote_lv.node_id)
    _src_node = db.get_storage_node_by_id(snapshot.lvol.node_id)
    if remote_snode.transfer_hublvol and remote_snode.transfer_hublvol.bdev_name:
        if not _other_active_transfers_to_node(task, remote_snode.get_id()):
            _src_node.rpc_client().bdev_nvme_detach_controller(
                remote_snode.transfer_hublvol.bdev_name)
    replicate_to_source = task.function_params["replicate_to_source"]
    if "replicate_as_snap_instance" in task.function_params:
        replicate_as_snap_instance = task.function_params["replicate_as_snap_instance"]
    else:
        replicate_as_snap_instance = False
    target_prev_snap: Optional[dict] = None
    _prev_snap_for_db: Optional[SnapShot] = None
    if replicate_to_source:
        org_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
        try:
            _snap_obj = db.get_snapshot_by_id(org_snap.source_replicated_snap_uuid)
            target_prev_snap = {"snap_bdev": _snap_obj.snap_bdev}
            _prev_snap_for_db = _snap_obj
        except KeyError as e:
            logger.error(e)
    else:
        if snapshot.snap_ref_id:
            try:
                prev_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
                for sn_inst in prev_snap.instances:
                    if sn_inst["lvol"]["node_id"] == remote_snode.get_id():
                        target_prev_snap = sn_inst
                        _prev_snap_for_db = prev_snap
                        break
            except KeyError as e:
                logger.error(e)

    # Leadership gate BEFORE chain/convert on the primary: a convert on a
    # non-leader returns success without persisting (silent conversion error).
    if not _require_lvs_leader(remote_snode, remote_lv.lvs_name, "add_clone/convert"):
        return False

    # chain snaps on primary
    if target_prev_snap:
        logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {target_prev_snap['snap_bdev']}")
        ret = remote_snode.rpc_client().bdev_lvol_add_clone( remote_lv.top_bdev, target_prev_snap['snap_bdev'])
        if not ret:
            logger.error("Failed to chain replicated snapshot on primary node")
            return False

    # convert to snapshot on primary
    ret = remote_snode.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)
    if not ret:
        logger.error("Failed to convert to snapshot on primary node")
        return False

    # chain snaps on secondary
    sec_node = db.get_storage_node_by_id(remote_snode.secondary_node_id)
    if sec_node.status == StorageNode.STATUS_ONLINE:
        if target_prev_snap:
            logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {target_prev_snap['snap_bdev']}")
            ret = sec_node.rpc_client().bdev_lvol_add_clone(remote_lv.top_bdev, target_prev_snap['snap_bdev'])
            if not ret:
                logger.error("Failed to chain replicated snapshot on secondary node")
                return False

        # convert to snapshot on secondary
        ret = sec_node.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)
        if not ret:
            logger.error("Failed to convert to snapshot on secondary node")
            return False

    new_snapshot_uuid = str(uuid.uuid4())

    new_snapshot = SnapShot()
    new_snapshot.uuid = new_snapshot_uuid
    new_snapshot.data_uuid = snapshot.data_uuid
    new_snapshot.cluster_id = remote_snode.cluster_id
    new_snapshot.lvol = remote_lv
    new_snapshot.pool_uuid = remote_lv.pool_uuid
    new_snapshot.snap_bdev = remote_lv.top_bdev
    new_snapshot.snap_uuid = remote_lv.lvol_uuid
    new_snapshot.size = snapshot.size
    new_snapshot.used_size = snapshot.used_size
    new_snapshot.snap_name = snapshot.snap_name
    new_snapshot.blobid = remote_lv.blobid
    new_snapshot.created_at = int(time.time())
    new_snapshot.status = SnapShot.STATUS_ONLINE
    snapshot.instances.append(new_snapshot)
    if not replicate_as_snap_instance:
        if replicate_to_source:
            new_snapshot.target_replicated_snap_uuid = snapshot.uuid
            snapshot.source_replicated_snap_uuid = new_snapshot_uuid
        else:
            snapshot.target_replicated_snap_uuid = new_snapshot_uuid
            new_snapshot.source_replicated_snap_uuid = snapshot.uuid

        try:
            if _prev_snap_for_db:
                new_snapshot.prev_snap_uuid = _prev_snap_for_db.get_id()
                _prev_snap_for_db.next_snap_uuid = new_snapshot_uuid
                _prev_snap_for_db.write_to_db()
        except Exception as e:
            logger.error(e)

    new_snapshot.write_to_db()

    if snapshot.status == SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_ONLINE

    snapshot.write_to_db()

    # delete lvol object
    remote_lv.bdev_stack = []
    remote_lv.write_to_db()
    lvol_controller.delete_lvol(remote_lv, force_delete=True)
    remote_lv.remove(db.kv_store)
    snapshot_events.replication_task_finished(snapshot)
    _prune_internal_snapshots(snapshot.lvol)
    return new_snapshot_uuid


def task_runner(task: JobSchedule):
    snapshot = db.get_snapshot_by_id(task.function_params["snapshot_id"])
    if not snapshot:
        task.function_result = "snapshot not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    try:
        snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if task.retry >= task.max_retry or task.canceled is True:
        task.function_result = "max retry reached"
        if task.canceled is True:
            task.function_result = "task cancelled"

        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        if snapshot.status != SnapShot.STATUS_ONLINE:
            snapshot.status = SnapShot.STATUS_ONLINE
            snapshot.write_to_db()

        remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
        # abort path: close the transfer session here too (last user only)
        try:
            _rl_node = db.get_storage_node_by_id(remote_lv.node_id)
            if (_rl_node.transfer_hublvol and _rl_node.transfer_hublvol.bdev_name
                    and not _other_active_transfers_to_node(task, _rl_node.get_id())):
                snode.rpc_client().bdev_nvme_detach_controller(
                    _rl_node.transfer_hublvol.bdev_name)
        except KeyError:
            pass
        lvol_controller.delete_lvol(remote_lv, force_delete=True)

        return True


    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        process_snap_replicate_start(task, snapshot)

    elif task.status == JobSchedule.STATUS_RUNNING:
        snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
        ret = snode.rpc_client().bdev_lvol_transfer_stat(snapshot.snap_bdev)
        if not ret:
            logger.error("Failed to get transfer stat")
            return False
        status = ret["transfer_state"]
        offset = ret["offset"]
        if status == "No process":
            task.function_result = f"Status: {status}, offset:{offset}, retrying"
            task.status = JobSchedule.STATUS_NEW
            task.retry += 1
            task.write_to_db()
            return False
        if status == "In progress":
            task.function_result = f"Status: {status}, offset:{offset}"
            task.function_params["offset"] = offset
            task.write_to_db()
            return True
        if status == "Failed":
            task.function_result = f"Status: {status}, offset:{offset}, retrying"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.retry += 1
            task.write_to_db()
            return False
        if status == "Done":
            new_snapshot_uuid = process_snap_replicate_finish(task, snapshot)
            if new_snapshot_uuid:
                task.function_result = new_snapshot_uuid
                task.status = JobSchedule.STATUS_DONE
                task.function_params["end_time"] = int(time.time())
                task.write_to_db()
            else:
                task.function_result = "complete repl failed, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.retry += 1
                task.write_to_db()
            return True


def main():
    logger.info("Starting Tasks runner...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
                        if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                            active_task = False
                            for t in db.get_job_tasks(task.cluster_id):
                                if t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and t.function_params["snapshot_id"] ==  task.function_params['snapshot_id']:
                                    if t.status == JobSchedule.STATUS_RUNNING and t.canceled is False:
                                        active_task = True
                                        break
                            if active_task:
                                logger.info("replication task found for same snapshot, retry")
                                continue
                        if task.status != JobSchedule.STATUS_DONE:
                            # get new task object because it could be changed from cancel task
                            task = db.get_task_by_id(task.uuid)
                            res = task_runner(task)
                            if not res:
                                time.sleep(3)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
