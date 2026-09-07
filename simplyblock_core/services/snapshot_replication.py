import time
import uuid

from simplyblock_core import constants, db_controller, snapshot_retention, utils
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


def _policy_target_pool(lvol, destination_cluster_id):
    """The pool the volume's replication target names on *destination_cluster_id*."""
    policy = db.get_replication_policy_for_lvol(lvol)
    if policy is None:
        return None
    try:
        target = db.get_replication_target_by_id(policy.target_id)
    except KeyError:
        return None
    if target.target_cluster_id != destination_cluster_id:
        return None
    return target.target_pool_uuid or None


def _destination_pool_uuid(remote_node, lvol=None, source_cluster_id=None):
    """The pool a replicated copy should be created in on *remote_node*.

    Most specific wins:

      1. the pool named by the replication TARGET the volume's policy points at,
         when that target IS this destination — it was chosen for this pair;
      2. the SOURCE cluster's snapshot_replication_target_pool, but only when
         that cluster's configured destination is this one;
      3. the first ACTIVE pool on the destination cluster.

    Step 2 used to be unconditional and was read off the DESTINATION cluster.
    That field is outgoing config -- "the pool I replicate into on my target" --
    so any cluster that is itself a source handed out a pool belonging to a
    third cluster as soon as data came back the other way. Lab 2026-08-19: a
    fail-back into the src cluster placed its REP_* volumes in the tgt cluster's
    pool, and 13 of them ended up stuck in_deletion.
    """
    if lvol is not None:
        pool_uuid = _policy_target_pool(lvol, remote_node.cluster_id)
        if pool_uuid:
            return pool_uuid
    if source_cluster_id:
        source_cluster = db.get_cluster_by_id(source_cluster_id)
        if (source_cluster.snapshot_replication_target_cluster == remote_node.cluster_id
                and source_cluster.snapshot_replication_target_pool):
            return source_cluster.snapshot_replication_target_pool
    for pool in db.get_pools(remote_node.cluster_id):
        if pool.status == Pool.STATUS_ACTIVE:
            return pool.uuid
    return None


def _unreplicated_local_ancestor(snode, snapshot, replicate_to_source):
    """The deepest chain ancestor of *snapshot* that must replicate FIRST.

    bdev_lvol_transfer sends a blob's OWN cluster map and nothing else
    (prepare_s3_clusters copies blob->active.clusters; inherited clusters are 0
    there and are skipped). The remote image is therefore complete only if
    every blob between the remote chain base and this snapshot is transferred
    too, bottom-up. Two things put such blobs in the chain:

      * a fail-over volume is a CLONE — its whole pre-fail-over history lives
        in base snapshots (lab 2026-08-20 case 4: XFS AG3, written once at
        mkfs, was zeros on the fresh cluster; everything fio rewrote after the
        fail-over arrived fine);
      * a USER snapshot between two internal cadence snapshots absorbs the
        writes made before it — the next internal snapshot's own map no longer
        contains them.

    Returns ``(verdict, record, why)``: ``("ok", None, "")`` when the chain
    base below is already replicated (or the snapshot is a self-contained
    root); ``("pending", rec, "")`` naming the DEEPEST unreplicated ancestor —
    replicate that one first and re-check (bottom-up order falls out of
    retrying); ``("blocked", rec_or_None, why)`` when the chain cannot be made
    complete (an ancestor is mid-deletion, or a blob has no snapshot record).

    Races: the walk is repeated on every attempt, so a concurrent user delete
    of an ancestor is harmless — its segments swap-merge into the successor,
    and the next walk sees the new chain. Deleting an ancestor of a snapshot
    that is TRANSFERRING is refused in snapshot_controller.delete (the merge
    would mutate the map mid-transfer).
    """
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    lvs = snapshot.snap_bdev.split("/")[0]
    by_bdev = {}
    for s in db.get_snapshots_by_node_id(snapshot.lvol.node_id):
        by_bdev[s.snap_bdev] = s
    rpc = snode.rpc_client()
    cur = snapshot.snap_bdev
    deepest = None
    for _ in range(64):
        ret = rpc.get_bdevs(cur)
        if not ret:
            return ("blocked", None,
                    f"chain bdev {cur} not readable on {snode.get_id()}")
        base = ((ret[0].get("driver_specific") or {}).get("lvol") or {}).get("base_snapshot")
        if not base:
            # Chain root: a self-contained blob, transferable in full.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        cur = f"{lvs}/{base}"
        rec = by_bdev.get(cur)
        if rec is None:
            return ("blocked", None,
                    f"chain blob {cur} has no snapshot record; its data cannot "
                    f"be replicated and the copy would have holes")
        if getattr(rec, attr, ""):
            # Everything below this point already exists on the remote side.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        if rec.status == SnapShot.STATUS_IN_DELETION:
            return ("blocked", rec,
                    f"chain ancestor {rec.get_id()} is mid-deletion")
        deepest = rec
    return ("blocked", None, "chain deeper than 64 blobs")


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    # Drive the transfer from whichever member of the SOURCE lvstore leads it
    # now — the snapshot exists on every member, so an outage of the recorded
    # primary must not stop replication (see _source_leader_node).
    snode = _source_leader_node(snapshot) or db.get_storage_node_by_id(snapshot.lvol.node_id)
    replicate_to_source = task.function_params["replicate_to_source"]

    # Once ONLY: snapshots form a TREE through clones, so several descendants
    # share ancestors, and each of their gates enqueues the shared ancestor.
    # _add_task dedupes the ACTIVE task; this guard covers the rest — a task
    # for a snapshot that already has its copy on the remote side (a second
    # enqueue that raced the first one's completion, a stale queue entry) must
    # recognize the copy as existent, not build a second one.
    already = getattr(snapshot,
                      "source_replicated_snap_uuid" if replicate_to_source
                      else "target_replicated_snap_uuid", "")
    if already:
        msg = (f"Snapshot {snapshot.get_id()} is already replicated "
               f"(remote copy {already}); nothing to transfer")
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db()
        return

    # The chain below this snapshot must be on the remote side FIRST, or the
    # copy has holes (see _unreplicated_local_ancestor). Checked on every
    # attempt so chain changes (user deletes swap-merging blobs) re-resolve.
    verdict, ancestor, why = _unreplicated_local_ancestor(
        snode, snapshot, replicate_to_source)
    if verdict == "pending":
        from simplyblock_core.controllers import tasks_controller
        dest_lvol_id = (task.function_params.get("dest_lvol_id")
                        or snapshot.lvol.get_id())
        tasks_controller.add_snapshot_replication_task(
            snapshot.cluster_id, task.node_id, ancestor.get_id(),
            replicate_to_source=replicate_to_source, dest_lvol_id=dest_lvol_id)
        msg = (f"waiting for chain ancestor {ancestor.get_id()} to replicate "
               f"first (a copy without it would have holes)")
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return
    if verdict == "blocked":
        msg = f"replication chain of {snapshot.get_id()} is incomplete: {why}; retrying"
        logger.error(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        if replicate_to_source:
            try:
                remote_node_uuid = db.get_storage_node_by_id(task.node_id)
            except KeyError:
                msg = f"Unable to find node: {task.node_id}, stopping task"
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            # A snapshot only has a counterpart on the destination when it was
            # replicated FROM there. Anything created after the fail-over — and
            # everything at all when failing back to a freshly installed cluster
            # — has source_replicated_snap_uuid empty, and looking that up used
            # to hand an empty id to get_snapshot_by_id, which degenerates into a
            # whole-table scan and dies with "Multiple values present" (348 such
            # failures in labs 2026-08-17/18: every fail-back task died here on
            # its first step, so nothing ever replicated back). Reuse the
            # counterpart's pool when there is one, otherwise resolve the pool on
            # the destination cluster the same way the forward direction does.
            remote_pool_uuid = None
            if snapshot.source_replicated_snap_uuid:
                try:
                    remote_pool_uuid = db.get_snapshot_by_id(
                        snapshot.source_replicated_snap_uuid).lvol.pool_uuid
                except KeyError:
                    logger.warning(
                        "Counterpart snapshot %s of %s is gone; resolving the "
                        "destination pool from the cluster instead",
                        snapshot.source_replicated_snap_uuid, snapshot.get_id())
            if not remote_pool_uuid:
                remote_pool_uuid = _destination_pool_uuid(
                    remote_node_uuid, lvol=snapshot.lvol,
                    source_cluster_id=snode.cluster_id)
            if not remote_pool_uuid:
                logger.error("Unable to find pool on remote cluster: %s",
                             remote_node_uuid.cluster_id)
                return
        else:  # replicate to target
            # A task can outlive the configuration that created it (or be queued
            # for a volume that never had a destination, e.g. a REP_* receiving
            # volume). Retrying it for ever burns the runner's cycles and blocks
            # every delete waiting behind its snapshot, so end it here.
            # A chain-ancestor task replicates a snapshot whose own lvol may be
            # long gone or never configured (a fail-over base chain): the
            # destination then comes from the policy-managed DESCENDANT volume,
            # carried on the task as dest_lvol_id by the ancestor gate.
            dest_lvol = snapshot.lvol
            if not dest_lvol.replication_node_id and task.function_params.get("dest_lvol_id"):
                try:
                    dest_lvol = db.get_lvol_by_id(task.function_params["dest_lvol_id"])
                except KeyError:
                    pass
            if not dest_lvol.replication_node_id:
                msg = (f"LVol {snapshot.lvol.get_id()} has no replication destination; "
                       f"dropping replication task for snapshot {snapshot.get_id()}")
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            remote_node_uuid = db.get_storage_node_by_id(dest_lvol.replication_node_id)
            remote_pool_uuid = _destination_pool_uuid(
                remote_node_uuid, lvol=dest_lvol, source_cluster_id=snode.cluster_id)
            if not remote_pool_uuid:
                logger.error(f"Unable to find pool on remote cluster: {remote_node_uuid.cluster_id}")
                return

        # An earlier attempt of THIS task may have created the landing volume
        # and died before storing its id (a node outage mid-create): add_lvol_ha
        # then fails "LVol name must be unique" on EVERY retry and the task
        # loops forever, stalling the volume's whole chain behind it (case 6,
        # run 20260824_144226: three volumes stuck on their first cadence
        # snapshot, retrying every ~31s for the rest of the run). The name is
        # derived from the snapshot, so a record wearing it IS this transfer's
        # landing volume: adopt it when it is usable, clear it when it is not.
        rep_name = f"REP_{snapshot.snap_name}"
        existing = None
        try:
            existing = db.get_lvol_by_name(rep_name)
        except KeyError:
            pass
        if existing is not None:
            if existing.status == LVol.STATUS_ONLINE:
                logger.info(f"Adopting landing volume {existing.get_id()} "
                            f"({rep_name}) left by an interrupted attempt")
                task.function_params["remote_lvol_id"] = existing.get_id()
                task.write_to_db()
            elif existing.status == LVol.STATUS_IN_DELETION:
                task.function_result = f"stale landing volume {rep_name} still deleting, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.retry += 1
                task.write_to_db()
                return
            else:
                logger.warning(f"Deleting half-created landing volume "
                               f"{existing.get_id()} ({rep_name}, status "
                               f"{existing.status}) from an interrupted attempt")
                try:
                    lvol_controller.delete_lvol(existing, force_delete=True)
                except Exception as e:
                    logger.error(f"Failed to clear stale landing volume {rep_name}: {e}")
                task.function_result = "cleared stale landing volume, retrying"
                task.status = JobSchedule.STATUS_SUSPENDED
                task.retry += 1
                task.write_to_db()
                return

    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        # internal=True: this REP_* volume is the landing copy for a transfer,
        # created by the system and never handed to a client. The per-node
        # subsystem cap is a user-admission limit; enforcing it here only stops
        # replication on a node that is already full, which is precisely when
        # the transfers that would let retention free those slots are needed.
        lv_id, err = lvol_controller.add_lvol_ha(
            f"REP_{snapshot.snap_name}", snapshot.size, remote_node_uuid.get_id(), snapshot.lvol.ha_type,
            remote_pool_uuid, internal=True)
        if lv_id:
            task.function_params["remote_lvol_id"] = lv_id
            task.write_to_db()
        else:
            logger.error(err)
            task.function_result = "Error creating remote lvol"
            task.write_to_db()
            return

    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    # Send to whichever member of the target lvstore currently leads it — the
    # hub only accepts receive IO on the leader, and leadership does not
    # return to the recorded node on its own after an outage.
    remote_lv_node = _receiving_leader_node(remote_lv)
    if remote_lv_node is None:
        task.function_result = "No online LVS leader on the target, retrying"
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

    # Flip to IN_REPLICATION under the CHAIN lock, BEFORE the transfer starts.
    # The delete path refuses to delete a snapshot in this state AND refuses to
    # delete its predecessor (whose swap-merge would mutate the cluster map the
    # transfer is reading) under the same chain-root-keyed lock — setting the
    # status after starting the transfer left a window in which a delete could
    # slip between the check and the merge.
    with snapshot_controller.object_mutation_lock(snapshot.cluster_id, snapshot.get_id()):
        try:
            fresh = db.get_snapshot_by_id(snapshot.get_id())
        except KeyError:
            msg = f"Snapshot {snapshot.get_id()} vanished before transfer start"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        if fresh.status == SnapShot.STATUS_IN_DELETION:
            msg = f"Snapshot {snapshot.get_id()} is being deleted; not starting a transfer"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        if fresh.status != SnapShot.STATUS_IN_REPLICATION:
            fresh.status = SnapShot.STATUS_IN_REPLICATION
            fresh.write_to_db()

    # 3 start replication
    snode.rpc_client().bdev_lvol_transfer(
        name=snapshot.snap_bdev,
        offset=offset,
        # 16 in-flight clusters (32 MiB window). With the dispatch fix keeping
        # the window genuinely full AND reads fragmented like writes (32x64KiB
        # per cluster), 16 clusters already put up to 512 concurrent 64 KiB
        # IOs per phase on the wire -- a 64-window quadruples the DMA buffer
        # (128 MiB per transfer task) for little more overlap.
        batch_size=16,
        bdev_name=hub_bdev,
        operation="replicate",
        lvol_id=remote_map_id
    )
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params["start_time"] = int(time.time())
    task.write_to_db()


def _receiving_leader_node(remote_lv):
    """The node that currently leads *remote_lv*'s lvstore, or None.

    The receiving lvol is HA — it exists on every member of the target
    lvstore — but only the LEADER can accept hub receive IO or persist a
    convert. ``remote_lv.node_id`` records where the lvol was created, which
    is not where leadership sits after the target node has been down: case 5
    (target node offline mid-replication) parked leadership on the peer, the
    pinned node kept failing the leadership gate, and the volume retried
    forever without ever converging — lag grew one snapshot per minute while
    the other four volumes replicated normally. Follow leadership instead of
    the recorded node; nothing moves leadership back on its own.
    """
    return _lvs_leader_among(remote_lv.nodes, remote_lv.node_id, remote_lv.lvs_name)


def _lvs_leader_among(nodes_ids, preferred_id, lvs_name):
    """The online node among *nodes_ids* that currently leads *lvs_name*."""
    from simplyblock_core.controllers import lvol_controller
    candidates = []
    for node_id in (nodes_ids or ([preferred_id] if preferred_id else [])):
        try:
            candidates.append(db.get_storage_node_by_id(node_id))
        except KeyError:
            continue
    # Prefer the recorded node while it still leads: keeps a stable home.
    candidates.sort(key=lambda n: n.get_id() != preferred_id)
    for node in candidates:
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            if lvol_controller.is_node_leader(node, lvs_name):
                return node
        except Exception as e:
            logger.warning("Leadership probe failed on %s: %s", node.get_id(), e)
    return None


def _source_leader_node(snapshot):
    """The node that currently leads the SOURCE lvstore, or None.

    A snapshot is registered on every member of its lvstore, so the transfer
    can be driven from whichever member holds leadership — which is the point
    of HA. Pinning to ``snapshot.lvol.node_id`` means an outage of that one
    node stops replication entirely even though the promoted peer serves the
    volume and holds the same snapshot: case 6 (source primary offline,
    secondary survives) saw zero replications during the whole outage, every
    task parked on "node is not online, retrying".
    """
    lv = snapshot.lvol
    return _lvs_leader_among(getattr(lv, "nodes", None), lv.node_id, lv.lvs_name)


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


def _successor_is_chained_to(successor, predecessor_target_uuid):
    """True when *successor*'s remote copy is chained onto *predecessor_target_uuid*.

    Retention deletes a predecessor expecting SPDK to swap-merge its segments
    into the successor that is CHAINED to it. If the chain link was never
    established, that delete does not merge — it drops the segments, and the
    target is left holding the newest delta over holes (all-zeros DR fail-over,
    labs 2026-08-10..17). Keeping N snapshots only widens the race; it never
    establishes the precondition, so verify it explicitly before pruning.

    The DB link is authoritative when present: ``prev_snap_uuid`` is only
    written after bdev_lvol_add_clone and bdev_lvol_convert both succeeded, on
    the primary and on an online secondary (every failure returns before the
    record is written). The converse does not hold — the link write is
    best-effort — so when the link is absent we ask SPDK on the target node,
    which is the real authority, rather than blocking the prune forever.
    """
    successor_target_uuid = successor.target_replicated_snap_uuid
    if not successor_target_uuid:
        return False
    try:
        successor_copy = db.get_snapshot_by_id(successor_target_uuid)
    except KeyError:
        return False

    if successor_copy.prev_snap_uuid == predecessor_target_uuid:
        return True

    # No DB link. Ask the target node whether the blob is actually chained, so a
    # missing link (best-effort write, or a snapshot replicated before chaining
    # was implemented) cannot make the pair unprunable for ever.
    try:
        predecessor_copy = db.get_snapshot_by_id(predecessor_target_uuid)
        remote_snode = db.get_storage_node_by_id(successor_copy.lvol.node_id)
        if remote_snode.status != StorageNode.STATUS_ONLINE:
            return False
        for bdev in (remote_snode.rpc_client().get_bdevs(successor_copy.snap_bdev) or []):
            driver = (bdev.get("driver_specific") or {}).get("lvol") or {}
            if not driver.get("clone"):
                continue
            if driver.get("base_snapshot") in (predecessor_copy.snap_bdev,
                                               predecessor_copy.snap_uuid):
                logger.info("Snapshot %s is chained onto %s in SPDK but the DB link is "
                            "missing; pruning on the SPDK verdict",
                            successor_copy.get_id(), predecessor_target_uuid)
                return True
    except Exception as e:
        logger.warning("Could not verify the chain of %s onto %s: %s",
                       successor_target_uuid, predecessor_target_uuid, e)
    return False


_KEEP_REPLICATED_INTERNAL = 2



def _keep_replicated_for(source_lvol):
    """How many replicated internal snapshots to retain for *source_lvol*.

    A volume under a replication policy uses the policy's ``keep_replicated``
    (never below its floor, since fewer than a pair leaves an arrival with
    nothing to chain onto); otherwise the module default applies.
    """
    try:
        policy = db.get_replication_policy_for_lvol(source_lvol)
    except Exception:
        policy = None
    if policy is None:
        return _KEEP_REPLICATED_INTERNAL
    from simplyblock_core.models.replication import ReplicationPolicy
    return max(policy.keep_replicated, ReplicationPolicy.MIN_KEEP_REPLICATED)

def _retention_schedule_for(source_lvol):
    """Parsed retention tiers from the volume's policy, or [] when it has none.

    A malformed schedule must not silently disable retention or crash the
    replication runner: it is reported and treated as "no schedule", which
    falls back to the flat keep-count.
    """
    try:
        policy = db.get_replication_policy_for_lvol(source_lvol)
    except KeyError:
        return []
    if (policy is None) or (spec := getattr(policy, "retention_schedule", None)) is None:
        return []
    try:
        return snapshot_retention.parse_schedule(spec)
    except snapshot_retention.RetentionScheduleError as e:
        logger.error("Ignoring invalid retention_schedule %r on policy %s: %s",
                     spec, policy.get_id(), e)
        return []


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
    keep = _keep_replicated_for(source_lvol)
    replicated_internal.sort(key=lambda s: s.created_at)

    # A retention SCHEDULE, when the policy defines one, decides which older
    # snapshots survive; without it retention stays the flat "newest N".
    # Either way the newest `keep` are protected, because deleting a snapshot
    # swap-merges its segments into the successor chained to it.
    schedule = _retention_schedule_for(source_lvol)
    if schedule:
        retained_ts = snapshot_retention.select_retained(
            [s.created_at for s in replicated_internal], schedule,
            now=time.time(), always_keep_newest=keep)
        candidates = [(i, s) for i, s in enumerate(replicated_internal)
                      if s.created_at not in retained_ts]
        if not candidates:
            return
    else:
        if len(replicated_internal) <= keep:
            return
        candidates = list(enumerate(replicated_internal))[:-keep]
    # Keep the newest TWO replicated internal snapshots, not just one.
    #
    # A replicated snapshot holds only its own clusters; the rest of the data
    # lives in the chain below it, and deleting a snapshot swap-merges its
    # segments into the successor that is CHAINED to it. Keeping only the
    # newest meant the predecessor was pruned the instant a replication
    # finished, so the NEXT arrival had nothing to chain onto and kept just
    # its delta — the target then holds the last delta over holes. Whether it
    # broke was pure timing, which is why the same fail-over case passed twice
    # and then failed (labs run 15 vs 19).
    #
    # Two kept widens the window in which the successor gets chained, but a
    # count can never establish the precondition: if chaining lagged or failed
    # for one snapshot while newer ones kept arriving, the predecessor was still
    # pruned and its segments were dropped instead of merged. So the chain is
    # verified per candidate below, and an unchained successor defers the prune.
    for index, snap in candidates:
        target_uuid = snap.target_replicated_snap_uuid
        try:
            db.get_snapshot_by_id(target_uuid)
        except KeyError:
            target_uuid = ""  # already gone — fall through to source cleanup
        if target_uuid and not _successor_is_chained_to(
                replicated_internal[index + 1], target_uuid):
            # The successor does not (yet) sit on top of this snapshot. Deleting
            # it now would drop its segments rather than swap-merge them, which
            # is exactly how a fail-over clone ends up reading zeros. Leave both
            # copies and retry next cycle: replication is still converging, or
            # chaining failed and its own retry has to land first.
            logger.warning("Deferring prune of replicated internal snapshot %s: its "
                           "successor %s is not chained onto target copy %s",
                           snap.get_id(), replicated_internal[index + 1].get_id(),
                           target_uuid)
            continue
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


def _previous_replicated_snapshot(snapshot, replicate_to_source):
    """The newest older snapshot of the same lvol whose copy already exists on
    the remote cluster — the chain target for the snapshot being finalized.

    ``snap_ref_id`` wins when populated, but internal replication snapshots
    are created without it, so fall back to age ordering. Returns None only
    when the snapshot genuinely has no replicated predecessor (first snapshot
    of the volume).

    REPLICATED is the whole precondition, and it applies to the snap_ref_id
    shortcut too. Returning a referenced predecessor that had never been
    replicated handed _resolve_chain_target a BLANK remote-copy id, which it
    read as "there is a remote copy, but I cannot resolve it" and refused to
    finalize on. The transfer then retried for ever and the volume's snapshot
    never got its replicated marker (lab 2026-08-20: 299 "Predecessor snapshot
    ... has remote copy  but it cannot be resolved ('Snapshot lookup with a
    blank id')" in 75 minutes, and case 4 never produced a post-baseline
    replicated point)."""
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    if snapshot.snap_ref_id:
        try:
            referenced = db.get_snapshot_by_id(snapshot.snap_ref_id)
        except KeyError as e:
            logger.error("snap_ref_id %s unresolvable: %s", snapshot.snap_ref_id, e)
        else:
            if getattr(referenced, attr, ""):
                return referenced
            logger.info(
                "Referenced predecessor %s of %s has no copy on the remote side "
                "yet; looking for an older replicated sibling instead",
                referenced.get_id(), snapshot.get_id())
    prev = None
    for s in db.get_snapshots_by_node_id(snapshot.lvol.node_id):
        if (s.lvol.get_id() == snapshot.lvol.get_id()
                and s.get_id() != snapshot.get_id()
                and getattr(s, attr, "")
                and s.status != SnapShot.STATUS_IN_DELETION
                and s.created_at < snapshot.created_at
                and (prev is None or s.created_at > prev.created_at)):
            prev = s
    if prev is not None:
        return prev

    # No older SIBLING — but a fail-over volume is a CLONE, and its first
    # snapshot's chain parent is the snapshot it was cloned from, not a
    # sibling. On fail-back that parent is the replicated copy of the
    # fail-over point (n(1)' on the target), whose counterpart on the original
    # source is n(1) — exactly the snapshot the delta must be chained onto.
    # Without this the first fail-back delta lands as a standalone blob and
    # reads its own clusters plus zeros, the same failure as the unchained
    # forward replication (case 2).
    lvol = snapshot.lvol
    try:
        lvol = db.get_lvol_by_id(snapshot.lvol.get_id())
    except (KeyError, AttributeError):
        pass
    parent_uuid = getattr(lvol, "cloned_from_snap", "")
    if not parent_uuid:
        return None
    try:
        parent = db.get_snapshot_by_id(parent_uuid)
    except KeyError as e:
        logger.error("clone parent %s unresolvable: %s", parent_uuid, e)
        return None
    if getattr(parent, attr, ""):
        logger.info("Chain parent for %s is the clone's origin snapshot %s",
                    snapshot.get_id(), parent.get_id())
        return parent
    return None


def _resolve_chain_target(snapshot, replicate_to_source, remote_snode):
    """Resolve the remote-cluster snapshot the new copy must be chained to.

    Returns ``(target_prev_snap, prev_snap_for_db, ok)``. ``ok`` is False when
    a replicated predecessor exists but its remote copy cannot be used — the
    caller must fail-and-retry rather than finalize an unchained snapshot: an
    unchained copy reads only its own delta (zeros elsewhere) and retention's
    delete cannot swap-merge segments into a successor."""
    prev_snap = _previous_replicated_snapshot(snapshot, replicate_to_source)
    if not prev_snap:
        return None, None, True
    remote_copy_uuid = (prev_snap.source_replicated_snap_uuid
                        if replicate_to_source
                        else prev_snap.target_replicated_snap_uuid)
    if not remote_copy_uuid:
        # A blank id is not an unresolvable copy, it is NO copy: this
        # predecessor was never replicated, so there is nothing on the remote
        # side to chain onto and this snapshot starts the chain. Treating the
        # blank as a broken reference made the task refuse to finalize and
        # retry for ever (see _previous_replicated_snapshot).
        logger.info("Predecessor %s has no copy on the remote side; the new "
                    "snapshot starts the chain", prev_snap.get_id())
        return None, None, True
    try:
        _snap_obj = db.get_snapshot_by_id(remote_copy_uuid)
    except KeyError as e:
        logger.error(
            "Predecessor snapshot %s has remote copy %s but it cannot be "
            "resolved (%s); refusing to finalize an unchained snapshot",
            prev_snap.get_id(), remote_copy_uuid, e)
        return None, None, False
    if _snap_obj.lvol.node_id != remote_snode.get_id():
        logger.error(
            "Predecessor remote copy %s lives on node %s but the new snapshot "
            "is on %s; cannot chain across lvstores",
            remote_copy_uuid, _snap_obj.lvol.node_id, remote_snode.get_id())
        return None, None, False
    return {"snap_bdev": _snap_obj.snap_bdev}, _snap_obj, True


def process_snap_replicate_finish(task, snapshot):

    # Close the transfer session — but ONLY when this was the last active
    # transfer into that target node. The hub is ONE shared session per target
    # node: a naive per-cycle detach rips the qpair out from under the other
    # volumes' in-flight transfers, mass-failing their IO on the hub and
    # churning LVS leadership on the target ("receive io for hublvol in
    # nonleader mode" storms, observed live 2026-08-13). This is the refcount
    # discipline the migration runner's hub_manager exists for.
    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    # add_clone/convert must run on the leader too — a convert on a non-leader
    # reports success and persists nothing. Follow leadership, not the node the
    # receiving lvol was created on (see _receiving_leader_node).
    remote_snode = (_receiving_leader_node(remote_lv)
                    or db.get_storage_node_by_id(remote_lv.node_id))
    _src_node = (_source_leader_node(snapshot)
                 or db.get_storage_node_by_id(snapshot.lvol.node_id))
    if remote_snode.transfer_hublvol and remote_snode.transfer_hublvol.bdev_name:
        if not _other_active_transfers_to_node(task, remote_snode.get_id()):
            _src_node.rpc_client().bdev_nvme_detach_controller(
                remote_snode.transfer_hublvol.bdev_name)
    replicate_to_source = task.function_params["replicate_to_source"]
    if "replicate_as_snap_instance" in task.function_params:
        replicate_as_snap_instance = task.function_params["replicate_as_snap_instance"]
    else:
        replicate_as_snap_instance = False
    # Resolve the predecessor's copy on the REMOTE cluster and chain the new
    # snapshot to it. Without this link every replicated snapshot is a
    # standalone blob: a fail-over clone reads only the last delta and zeros
    # elsewhere, and retention's delete cannot swap-merge segments into a
    # successor (all-zeros DR fail-over, labs 2026-08-10..14; chain_attempts=0
    # in every run because snap_ref_id is never populated on internal
    # snapshots). Resolve the predecessor by lvol + age instead, and if one
    # exists but its remote copy cannot be resolved, fail and retry rather
    # than silently building an unchained snapshot.
    target_prev_snap, _prev_snap_for_db, ok = _resolve_chain_target(
        snapshot, replicate_to_source, remote_snode)
    if not ok:
        return False

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

        if _prev_snap_for_db:
            # The chain link is what lets retention delete this snapshot's
            # predecessor safely: the prune path refuses to drop a predecessor
            # until it can see the successor sitting on top of it. Swallowing a
            # failure here used to leave SPDK chained but the record unlinked,
            # so record it before the snapshot is published, and fail the task
            # (it retries) rather than publishing a snapshot that looks
            # unchained to retention.
            new_snapshot.prev_snap_uuid = _prev_snap_for_db.get_id()
            _prev_snap_for_db.next_snap_uuid = new_snapshot_uuid
            try:
                _prev_snap_for_db.write_to_db()
            except Exception as e:
                logger.error("Failed to record the chain back-link on %s: %s",
                             _prev_snap_for_db.get_id(), e)
                return False

    new_snapshot.write_to_db()

    if snapshot.status == SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_ONLINE

    snapshot.write_to_db()

    # Tear down the landing volume's plumbing (subsystem/namespace); its BLOB
    # deliberately lives on -- it was just converted into the chained snapshot.
    # The record removal must not depend on the teardown succeeding: delete_lvol
    # can raise (SPDK refuses to delete a bdev that is now a cloned snapshot),
    # and a record left in_deletion never converges -- the monitor re-issues
    # its delete forever (297 warnings/10min, run 20260821_205111) and every
    # later cleanup that waits for lvols to drain times out on it.
    remote_lv.bdev_stack = []
    remote_lv.write_to_db()
    # Tear the subsystem/namespace down DIRECTLY, not via delete_lvol:
    # delete_lvol flips the record to in_deletion and hands it to the
    # monitor's async machinery, so an interruption anywhere before the
    # remove() below stranded a record the monitor can never finish (empty
    # stack -> nothing to issue -> status poll 4 forever; runs 20260824 and
    # 20260825_125156). With the stack already emptied there is no blob work
    # to do -- only nvmf plumbing on the volume's nodes.
    for _node_id in remote_lv.nodes:
        try:
            _node = db.get_storage_node_by_id(_node_id)
            if _node.status == StorageNode.STATUS_ONLINE:
                lvol_controller.delete_lvol_from_node(
                    remote_lv.get_id(), _node_id, force=True)
        except Exception as e:
            logger.error(f"Landing volume {remote_lv.get_id()} teardown on "
                         f"{_node_id[:8]} raised: {e}; retiring the record "
                         f"anyway (its bdev lives on as the converted snapshot)")
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
        db.get_storage_node_by_id(snapshot.lvol.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    # Any online member of the source lvstore that holds leadership can drive
    # this; waiting for the recorded primary stalls replication for the whole
    # duration of its outage even though the promoted peer holds the snapshot.
    snode = _source_leader_node(snapshot)
    if snode is None:
        task.function_result = "no online source LVS leader, retrying"
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

        # A task can reach max retry BEFORE it ever created a receiving lvol
        # (e.g. every attempt failed at the leadership gate). Reading the param
        # unconditionally raised KeyError out of main() and killed the whole
        # replication runner — one unlucky task stopped replication for every
        # volume in the cluster (lab run 19: the service crash-looped, so no
        # snapshot was ever chained or pruned).
        remote_lv_id = task.function_params.get("remote_lvol_id")
        if not remote_lv_id:
            return True
        try:
            remote_lv = db.get_lvol_by_id(remote_lv_id)
        except KeyError:
            return True
        # abort path: close the transfer session here too (last user only)
        try:
            _rl_node = db.get_storage_node_by_id(remote_lv.node_id)
            if (_rl_node.transfer_hublvol and _rl_node.transfer_hublvol.bdev_name
                    and not _other_active_transfers_to_node(task, _rl_node.get_id())):
                snode.rpc_client().bdev_nvme_detach_controller(
                    _rl_node.transfer_hublvol.bdev_name)
        except Exception as e:
            logger.warning("Abort-path hub detach failed (non-fatal): %s", e)
        try:
            lvol_controller.delete_lvol(remote_lv, force_delete=True)
        except Exception as e:
            logger.warning("Abort-path cleanup of %s failed (non-fatal): %s",
                           remote_lv_id, e)

        return True


    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        process_snap_replicate_start(task, snapshot)

    elif task.status == JobSchedule.STATUS_RUNNING:
        snode = _source_leader_node(snapshot) or db.get_storage_node_by_id(snapshot.lvol.node_id)
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
                            # One task must never take the runner down with it:
                            # an RPC to a node that just went offline, or a
                            # malformed param, used to propagate out of main()
                            # and stop replication for the whole cluster until
                            # the container restarted (and then again).
                            try:
                                res = task_runner(task)
                            except Exception as e:
                                logger.error("Replication task %s failed: %s",
                                             task.get_id(), e)
                                res = False
                            if not res:
                                time.sleep(3)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
