import contextlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple

from simplyblock_core import (
    constants,
    db_controller,
    migration_turn,
    replication_spare,
    snapshot_retention,
    utils,
    xfer_timing,
)
from simplyblock_core.controllers import (
    control_plane_controller,
    lvol_controller,
    lvol_events,
    qos_controller,
    replication_delta,
    replication_recovery_points,
    snapshot_controller,
    snapshot_events,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.client import (
    LocalPeerClient,
    PeerClient,
    PeerError,
    PeerNotFoundError,
    PeerRefusedError,
    PeerUnreachableError,
    peer_for_target,
)
from simplyblock_core.peer.protocol import (
    ChainCheck,
    ChainRequest,
    DescribeRequest,
    EnsureHubRequest,
    FinishRequest,
    LandingRequest,
    SourceGroup,
    SourceHostAccess,
    SourceSnapshot,
    SourceVolume,
)
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)
# get DB controller
db = db_controller.DBController()

# The worker pool main() runs replication tasks on (see _Workers); None until
# main() starts, in which case _submit runs a task inline.
_WORKERS = None

# One lock per transfer hub session (source member -> target member). The hub
# is one shared NVMe-oF session per target node: a finish detaches it only when
# no other transfer into that node is RUNNING, and a start attaches it before
# its task is RUNNING, so with concurrent workers the detach decision and the
# attach-to-RUNNING window must not interleave.
_HUB_LOCKS: dict = {}
_HUB_LOCKS_GUARD = threading.Lock()



def _hub_lock(source_node_id, hub_node_id):
    with _HUB_LOCKS_GUARD:
        return _HUB_LOCKS.setdefault((source_node_id, hub_node_id), threading.Lock())


def _peer_for(source_lvol, cluster_id) -> PeerClient:
    """The peer client for *cluster_id*, the destination of *source_lvol*'s replication.

    The volume's policy target names a peer control plane when the destination
    is on one. A target in this control plane, or a volume replicated without a
    policy (cluster-level replication), gets the in-process client: same code,
    no HTTP.

    Raises:
        KeyError: the volume's policy names a target that no longer exists.
    """
    policy = db.get_replication_policy_for_lvol(source_lvol)
    if policy is not None:
        target = db.get_replication_target_by_id(policy.target_id)
        if target.target_cluster_id == cluster_id:
            return peer_for_target(target)
    return LocalPeerClient(cluster_id)


class _Landing(NamedTuple):
    """A transfer's landing volume, as this cluster knows it: what the
    destination's prepare_landing answered, kept on the task, so no later step
    reads the destination's records."""

    lvol_id: str
    top_bdev: str
    node_id: str
    pool_uuid: str
    lvs_name: str
    #: the destination cluster
    cluster_id: str

    def get_id(self):
        return self.lvol_id


def _leadership_recovery_due(task, now):
    """Whether this pass asks the target to recover its lvstore's leadership:
    the leaderless wait has lasted REPL_TARGET_LEADERLESS_RECOVER_SEC, and the
    last ask is REPL_TARGET_LEADERLESS_RECOVER_EVERY_SEC ago. Records the ask."""
    params = task.function_params
    since = params.get("target_leaderless_since")
    if not since or now - since < constants.REPL_TARGET_LEADERLESS_RECOVER_SEC:
        return False
    tried = params.get("target_recover_tried_at") or 0
    if now - tried < constants.REPL_TARGET_LEADERLESS_RECOVER_EVERY_SEC:
        return False
    params["target_recover_tried_at"] = now
    return True


def _wait_for_target_leader(task, remote_lv, hub, now):
    """Suspend a transfer whose target lvstore has no settled leader, and say
    for how long and why.

    Replication never makes a leader itself. A target lvstore has no client
    IO, so no member promotes itself when its primary goes down; the target's
    node monitor gives it a leader, during the down member's restart too
    (bf24 2026-11-14, LVS_22: the transfer waited ~110 s for the crashed
    primary). Once the wait lasts REPL_TARGET_LEADERLESS_RECOVER_SEC the next
    ensure_hub asks the target's guarded recovery (_leadership_recovery_due).
    The wait is polled every REPL_RETRY_BACKOFF_BASE_SEC, costs no retry, and
    raises a REPLICATION_STALLED event once it lasts
    REPL_TARGET_LEADERLESS_ALERT_SEC, then at most every
    REPL_MERGE_ALERT_EVERY_SEC."""
    params = task.function_params
    since = params.get("target_leaderless_since") or now
    params["target_leaderless_since"] = since
    waited = int(now - since)
    msg = f"{hub.reason} for {waited}s [{hub.members}], retrying"
    alerted = params.get("target_leaderless_alerted_at") or 0
    if (waited >= constants.REPL_TARGET_LEADERLESS_ALERT_SEC
            and now - alerted >= constants.REPL_MERGE_ALERT_EVERY_SEC):
        params["target_leaderless_alerted_at"] = now
        try:
            src = db.get_snapshot_by_id(params["snapshot_id"]).lvol
            lvol_events.replication_target_leaderless(
                src, remote_lv.lvs_name, waited, f"{hub.reason}; members: {hub.members}")
        except Exception as e:                            # noqa: BLE001
            logger.error("Replication task %s: could not log the leaderless-target "
                         "event: %s", task.uuid, e)
    _suspend_for_retry(task, msg, backoff=True, count=_switches_exhausted(task),
                       backoff_step=0)


def _target_leader_found(task):
    """End a leaderless-target wait: log how long it lasted."""
    since = task.function_params.pop("target_leaderless_since", None)
    task.function_params.pop("target_leaderless_alerted_at", None)
    task.function_params.pop("target_recover_tried_at", None)
    if since:
        logger.info("Replication task %s: target leadership settled after %ds without "
                    "a leader", task.uuid, int(time.time() - since))


def _landing_of(task):
    """The landing volume of *task* (see _Landing).

    A task from before the record reads it off the shared database.

    Raises:
        KeyError: such a task names a landing volume or node unknown here.
    """
    landing_id = task.function_params["remote_lvol_id"]
    known = task.function_params.get("landing") or {}
    if known.get("lvol_id") == landing_id:
        return _Landing(**known)
    lvol = db.get_lvol_by_id(landing_id)
    return _Landing(lvol_id=landing_id, top_bdev=getattr(lvol, "top_bdev", ""),
                    node_id=lvol.node_id, pool_uuid=getattr(lvol, "pool_uuid", ""),
                    lvs_name=getattr(lvol, "lvs_name", ""),
                    cluster_id=db.get_storage_node_by_id(lvol.node_id).cluster_id)


def _landing_peer(snapshot, landing) -> PeerClient:
    """The peer client for the cluster holding *landing*, *snapshot*'s landing
    volume (a _Landing; or an LVol record, whose node names the cluster).

    Raises:
        KeyError: the landing volume's node, or the policy's target, is unknown.
    """
    cluster_id = (getattr(landing, "cluster_id", "")
                  or db.get_storage_node_by_id(landing.node_id).cluster_id)
    return _peer_for(snapshot.lvol, cluster_id)


def _replication_class_on(cluster_id):
    """qos_controller.replication_class_available, but never raises: the
    service class is an optimisation and must not stop a transfer."""
    try:
        return qos_controller.replication_class_available(cluster_id)
    except Exception as e:                                # noqa: BLE001
        logger.warning("QoS lookup for cluster %s failed; replicating in Default: %s",
                       cluster_id, e)
        return False


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


def _pool_hints(lvol, source_cluster_id, destination_cluster_id):
    """The pools to land *lvol*'s copies in on *destination_cluster_id*, most
    specific first, from this cluster's own records. The destination takes the
    first one that is a pool of its own and otherwise its first active pool
    (peer.service.prepare_landing):

      0. the pool the caller of replication start named, unless a policy now
         manages the volume;
      1. the pool named by the replication TARGET the volume's policy points at,
         when that target IS this destination -- it was chosen for this pair;
      2. the SOURCE cluster's snapshot_replication_target_pool, but only when
         that cluster's configured destination is this one.

    Step 2 used to be unconditional and was read off the DESTINATION cluster.
    That field is outgoing config -- "the pool I replicate into on my target" --
    so any cluster that is itself a source handed out a pool belonging to a
    third cluster as soon as data came back the other way. Lab 2026-08-19: a
    fail-back into the src cluster placed its REP_* volumes in the tgt cluster's
    pool, and 13 of them ended up stuck in_deletion.
    """
    hints = []
    if not lvol.replication_policy_id and lvol.replication_pool_uuid:
        hints.append(lvol.replication_pool_uuid)
    policy_pool = _policy_target_pool(lvol, destination_cluster_id)
    if policy_pool:
        hints.append(policy_pool)
    if source_cluster_id:
        source_cluster = db.get_cluster_by_id(source_cluster_id)
        if (source_cluster.snapshot_replication_target_cluster == destination_cluster_id
                and source_cluster.snapshot_replication_target_pool):
            hints.append(source_cluster.snapshot_replication_target_pool)
    return hints


def _destination_cluster_of(lvol):
    """The cluster *lvol* replicates to: its policy target's cluster, else the
    cluster of the replication node it was configured with.

    A target on a peer control plane is only reachable the first way: its
    nodes are not in this database.

    Raises:
        KeyError: no policy target names it and the node is unknown here.
    """
    policy = db.get_replication_policy_for_lvol(lvol)
    if policy is not None:
        with contextlib.suppress(KeyError):
            return db.get_replication_target_by_id(policy.target_id).target_cluster_id
    return db.get_storage_node_by_id(lvol.replication_node_id).cluster_id


#: Bound of the clone-ancestry walk in _descendant_destination_lvol: a cycle
#: guard, as in the other chain walks, not a limit on how often a volume moves.
_ANCESTRY_MAX_HOPS = 256


def _descendant_destination_lvol(snapshot):
    """The live volume with a replication destination whose clone ancestry
    holds *snapshot*, or None.

    A landing copy belongs to its REP_* receiving volume, which has no
    destination of its own. When the volume cloned from such copies replicates
    on (the return trip of a migration), its chain ancestors are sent first,
    and a task for one of them that carries no dest_lvol_id resolved its
    destination from the receiving volume and was dropped -- after which the
    descendant waited on the ancestor until it gave up (bf6 2026-10-29 round
    5: vol-5 stalled ~15 minutes, snapshots piling up meanwhile)."""
    target = snapshot.get_id()
    try:
        lvols = db.get_lvols(snapshot.cluster_id)
    except Exception as e:                              # noqa: BLE001
        logger.warning("Cannot list volumes to find the descendant of %s: %s",
                       target, e)
        return None
    for lvol in lvols:
        if not lvol.replication_node_id or not lvol.cloned_from_snap:
            continue
        snap_id, hops = lvol.cloned_from_snap, 0
        while snap_id and hops < _ANCESTRY_MAX_HOPS:
            if snap_id == target:
                return lvol
            try:
                snap_id = db.get_snapshot_by_id(snap_id).prev_snap_uuid
            except KeyError:
                break
            hops += 1
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
        ret = rpc.bdev_get(cur)
        if not ret:
            return ("blocked", None,
                    f"chain bdev {cur} not readable on {snode.get_id()}")
        base = ((ret.get("driver_specific") or {}).get("lvol") or {}).get("base_snapshot")
        if not base:
            # Chain root: a self-contained blob, transferable in full.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        cur = f"{lvs}/{base}"
        rec = by_bdev.get(cur)
        if rec is None:
            return ("blocked", None,
                    (f"chain blob {cur} has no snapshot record; its data cannot "
                    f"be replicated and the copy would have holes"))
        if getattr(rec, attr, ""):
            # Everything below this point already exists on the remote side.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        if rec.status == SnapShot.STATUS_IN_DELETION:
            return ("blocked", rec,
                    f"chain ancestor {rec.get_id()} is mid-deletion")
        if rec.status == SnapShot.STATUS_IN_CREATION:
            # Not yet a snapshot: its create is in progress, or its process
            # died and reap_snapshot_intents will roll it back.
            return ("blocked", rec,
                    f"chain ancestor {rec.get_id()} is still being created")
        if deepest is None and _transfer_completed(rec, replicate_to_source):
            # The direct base has arrived and only its merge and convert on
            # the remote side remain; the finish chains onto its copy once it
            # exists (_finishing_predecessor, _deferred_chain_gate).
            return ("ok", None, "")
        deepest = rec
    return ("blocked", None, "chain deeper than 64 blobs")


def _live_replication_task(cluster_id, snapshot_id):
    """The replication task of *snapshot_id* that can still make progress, or
    None."""
    for t in db.get_job_tasks(cluster_id):
        if (t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
                and (t.function_params or {}).get("snapshot_id") == snapshot_id
                and t.status != JobSchedule.STATUS_DONE and not t.canceled):
            return t
    return None


def _transfer_completed(snapshot, replicate_to_source):
    """Whether *snapshot*'s transfer has completed and its task is only
    finishing (merging, converting) -- with overlap enabled."""
    if not constants.REPL_OVERLAP_MERGE:
        return False
    t = _live_replication_task(snapshot.cluster_id, snapshot.get_id())
    return (t is not None and bool(t.function_params.get("transfer_done"))
            and bool(t.function_params.get("replicate_to_source")) == bool(replicate_to_source))


def _finishing_predecessor(snapshot, replicate_to_source):
    """The snapshot before *snapshot* when its transfer has completed but its
    copy on the remote side is not made yet: the target is still merging it.

    The next transfer need not wait for that merge (constants.
    REPL_OVERLAP_MERGE). Its delta is computed against this predecessor as
    usual, but the landing volume cannot be chained onto the copy before the
    transfer -- the copy does not exist yet -- so it is chained in the finish,
    once it does (_deferred_chain_gate). Until then the newest REPLICATED
    snapshot is an older one, and chaining onto that would build the copy on
    the wrong base. None when the predecessor is replicated (the normal path)
    or not finishing.
    """
    prev = None
    for s in db.get_snapshots_by_node_id(snapshot.lvol.node_id):
        if (s.lvol.get_id() == snapshot.lvol.get_id()
                and s.get_id() != snapshot.get_id()
                and s.status != SnapShot.STATUS_IN_DELETION
                and s.created_at < snapshot.created_at
                and (prev is None or s.created_at > prev.created_at)):
            prev = s
    if prev is None:
        return None
    copy = (prev.source_replicated_snap_uuid if replicate_to_source
            else prev.target_replicated_snap_uuid)
    if copy:
        return None
    return prev if _transfer_completed(prev, replicate_to_source) else None


def _group_id_for_lvol(lvol):
    """The consistency group *lvol* belongs to, or "".

    A member carries its group (group_id). Only a volume recorded before that
    field existed is resolved through its policy, to the group the policy owns
    if any: several standalone groups can follow one policy (bed run
    20261113-2112-ebs-splitdr: every replication task logged "Multiple values
    present" and the members counted as in no group).
    """
    gid = getattr(lvol, "group_id", "")
    if gid:
        return gid
    group = db.get_consistency_group_for_policy(getattr(lvol, "replication_policy_id", ""))
    return group.get_id() if group else ""


def _lvs_transfer_hold(task, snapshot):
    """Why this transfer must wait, or "" when it may start now.

    Two priorities share one lvstore's bandwidth:

    0. A migration-mode volume waits while another volume of its source node
       is migrating (simplyblock_core/migration_turn.py).

    1. A volume in its FINAL CUTOVER owns the lvstore. Its convergence rounds
       decide how long client IO freezes -- every second another volume steals
       from a round is a second of writes the frozen final step must copy --
       so nothing else on that lvstore transfers meanwhile. Members of the same
       consistency group are exempt: the group cuts over together.

    2. Consistency groups outrank loose volumes. A group's members transfer in
       PARALLEL with each other (their snapshots belong to one generation and
       are only useful together), and everything else on the lvstore waits, so
       groups are effectively serialized against each other rather than
       interleaved.
    """
    own_lvol = getattr(snapshot, "lvol", None)
    lvs_name = getattr(own_lvol, "lvs_name", "") if own_lvol else ""
    if own_lvol is None or not lvs_name:
        return ""
    own_id = own_lvol.get_id()
    own_group = _group_id_for_lvol(own_lvol)

    # --- priority 0: one migrating volume per source node ------------------
    # A volume queued behind another on its node ships nothing yet, not even
    # a backlog of snapshots it had before migration started.
    if own_id in migration_turn.waiting_lvol_ids(db, task.cluster_id):
        return "waiting for its node's migration turn"

    tasks = db.get_job_tasks(task.cluster_id)

    # --- priority 1: a cutover in progress on this lvstore -----------------
    for t in tasks:
        if t.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if t.status == JobSchedule.STATUS_DONE or t.canceled:
            continue
        params = t.function_params or {}
        if params.get("cutover_lvs") != lvs_name:
            continue
        holder = params.get("lvol_id")
        if not holder or holder == own_id:
            return ""                       # our own cutover: keep moving
        holder_group = params.get("cutover_group") or ""
        if holder_group and holder_group == own_group:
            return ""                       # same group: cut over together
        return (f"lvol {holder[:8]} is in final cutover on lvstore {lvs_name}")

    # --- priority 2: a consistency group is transferring on this lvstore ---
    for t in tasks:
        if t.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
            continue
        if t.status != JobSchedule.STATUS_RUNNING or t.get_id() == task.get_id():
            continue
        other_snap_id = (t.function_params or {}).get("snapshot_id")
        if not other_snap_id:
            continue
        try:
            other_lvol = db.get_snapshot_by_id(other_snap_id).lvol
        except KeyError:
            continue
        if getattr(other_lvol, "lvs_name", "") != lvs_name:
            continue
        other_group = _group_id_for_lvol(other_lvol)
        if other_group and other_group != own_group:
            return (f"consistency group {other_group.split('/')[-1][:8]} is "
                    f"transferring on lvstore {lvs_name}")

    return ""


def _finish_completed_transfer(task, snapshot, offset):
    """Chain, convert and mark the snapshot replicated. Returns True.

    This is what sets target_replicated_snap_uuid, which is the signal a
    cutover's convergence loop waits on -- so it must run as soon as the
    transfer is known to be finished, not on some later pass.
    """
    # The data is on the landing volume now. Record it BEFORE finishing: the
    # finish converts the landing volume into a snapshot node by node, and a
    # retry after a partial finish must resume the finish, never re-send into
    # a volume that is already a snapshot on some members (see
    # _resume_finish).
    if not task.function_params.get("transfer_done"):
        task.function_params["transfer_done"] = True
        task.write_to_db()
    # submit -> Done, measured. NOT a throughput: see fix_xfer_latency notes.
    xfer_timing.gap("transfer_complete",
                    task.function_params.get("xfer_submit_t"),
                    snap=snapshot.get_id(), lvol=snapshot.lvol.get_id(),
                    bytes=offset)
    with xfer_timing.phase("replicate_finish", snap=snapshot.get_id(),
                           lvol=snapshot.lvol.get_id()):
        try:
            new_snapshot_uuid = process_snap_replicate_finish(task, snapshot)
        except RPCException as e:
            # RPC failures raise; without this they skipped the counted,
            # backed-off retry below and re-ran the finish on every pass
            # forever (SFAM-2834).
            logger.error("Replication finish for snapshot %s failed: %s", snapshot.get_id(), e)
            new_snapshot_uuid = None
    if new_snapshot_uuid == FINISH_PENDING:
        # still merging on the target, or waiting for the copy it is chained
        # onto: no failure, a later pass finishes it
        task.function_result = (task.function_params.pop("pending_why", None)
                                or "landing volume merging with its base")
        task.write_to_db()
        return True
    if new_snapshot_uuid == FINISH_ABANDONED:
        return True                     # suspended for a new transfer already
    if new_snapshot_uuid:
        task.function_result = new_snapshot_uuid
        task.status = JobSchedule.STATUS_DONE
        task.function_params["end_time"] = int(time.time())
        task.write_to_db()
        _hand_off_to_successor(task, snapshot)
    else:
        _suspend_for_retry(task, "complete repl failed, retrying", backoff=True)
    return True


def _hand_off_to_successor(task, snapshot):
    """The copy of *snapshot* now exists: finish the transfer that ran while
    it was merged (chain_deferred_to) right away, which chains its landing
    volume and starts its merge. Waiting for the next pass left the target's
    merges of a volume idle for ~50 s per cycle (bed run 4, 2026-10-24); the
    merges of a volume run one after another, so that idle time was on the
    critical path."""
    for t in db.get_job_tasks(task.cluster_id):
        p = t.function_params or {}
        if (t.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION
                or t.status == JobSchedule.STATUS_DONE or t.canceled
                or p.get("chain_deferred_to") != snapshot.get_id()
                or not p.get("transfer_done")):
            continue
        try:
            succ = db.get_snapshot_by_id(p["snapshot_id"])
        except KeyError:
            return
        logger.info("Replication task %s: %s has its copy; finishing %s now",
                    task.uuid, snapshot.get_id(), succ.get_id())
        # With a pool: through it, never by calling into the other task from
        # here -- a worker may be running it already (then this is a no-op),
        # and two workers must never mutate one task row. Inline (no pool):
        # finish it right here, as always.
        if _WORKERS is not None and _WORKERS.size:
            _submit(t)
            return
        try:
            _resume_finish(t, succ)
        except Exception as e:                            # noqa: BLE001
            # the next pass finishes it as before
            logger.warning("Hand-off to task %s failed: %s", t.uuid, e)
        return


def _backoff_seconds(retry):
    """Delay before the next attempt after a failure that touched the target."""
    return min(constants.REPL_RETRY_BACKOFF_BASE_SEC * (2 ** min(retry, 6)),
               constants.REPL_RETRY_BACKOFF_MAX_SEC)


def _suspend_for_retry(task, msg, backoff=False, count=True, level=logging.WARNING,
                       backoff_step=None):
    """Suspend *task* for another attempt, and say why.

    Every retry is logged: several of these paths used to count a retry
    silently, so a volume could run all its tasks into max retry without one
    line in the log naming the reason (2026-09-29: LVS_1 on the source kept
    losing its leader; vm-a's tasks gave up unseen, and a fail-over later
    found nothing on the target).

    ``backoff`` delays the next attempt after a failure that involved the
    target (a failed transfer or finish): an immediate retry repeats whatever
    the target did, and on 2026-09-29 each retry into a half-converted landing
    volume fenced the target LVS again, 14 times in 11 minutes.

    ``count`` is False for waiting conditions that are not a fault of the
    transfer (no leader on the source right now): those do not spend retries,
    or a leaderless window of half a minute is enough to kill every task.
    """
    logger.log(level, "Replication task %s (snapshot %s): %s",
               task.uuid, task.function_params.get("snapshot_id"), msg)
    task.function_result = msg
    task.function_params["last_error"] = msg
    task.status = JobSchedule.STATUS_SUSPENDED
    if count:
        task.retry += 1
    if backoff:
        step = task.retry if backoff_step is None else backoff_step
        task.function_params["not_before"] = int(time.time()) + _backoff_seconds(step)
    task.write_to_db()


def _resume_finish(task, snapshot):
    """Finish a transfer that already completed; True if this handled the task.

    A finish that failed half-way (converted on the primary, failed on the
    secondary) leaves the landing volume a snapshot on some members. Starting
    over re-sent the transfer into it: every write failed, and the target LVS
    dropped its leadership and fenced its ports on each attempt (2026-09-29,
    LVS_1 on site A, 14 times). The data is complete, so resume the finish
    instead: the destination chains and converts where it has not happened
    yet, answers done when an earlier attempt already recorded the copy, and
    gone when the landing volume is gone (the transfer then starts over).
    """
    if not task.function_params.get("transfer_done"):
        return False
    logger.info("Replication task %s: transfer of %s already completed; "
                "resuming the finish (converted on: %s)", task.uuid,
                snapshot.get_id(), task.function_params.get("converted_nodes") or "none")
    _finish_completed_transfer(task, snapshot, task.function_params.get("offset"))
    return True


def _cutover_owns(lvol_id, cluster_id):
    """True while a final cutover is in its endgame for this volume.

    Such a volume already holds its lvstore and every other transfer on it is
    held, so waiting inline for its transfer starves nothing -- and this is the
    window the client's IO freeze is paying for. A cutover that is merely
    queued (a migration plan queues every volume's up front, waiting for the
    initial copy) holds nothing yet: counting it made each initial copy block
    the runner for the long inline budget while the other volumes' transfers
    were not even submitted.
    """
    if not lvol_id:
        return False
    try:
        tasks = db.get_job_tasks(cluster_id)
    except Exception:                                     # noqa: BLE001
        return False
    for other in tasks:
        if other.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if other.status == JobSchedule.STATUS_DONE or other.canceled:
            continue
        params = other.function_params or {}
        if params.get("lvol_id") == lvol_id and params.get("cutover_lvs"):
            return True
    return False


def _await_transfer_completion(task, snapshot, snode):
    """Poll the just-submitted transfer at 100ms and finish it in this pass.

    Returns True when the transfer completed and was finished here; False to
    leave it for the pass-based path (still in flight, or the budget ran out).

    Before this, submit and the Done check happened on DIFFERENT passes of the
    runner loop, so a transfer that finished in milliseconds was not acted on
    for a median of 81 SECONDS (run 20260828_115307: every poll found state
    already 'Done', never once 'In progress').
    """
    budget = (constants.REPL_XFER_INLINE_WAIT_CUTOVER_SEC
              if _cutover_owns(snapshot.lvol.get_id(), task.cluster_id)
              else constants.REPL_XFER_INLINE_WAIT_SEC)
    deadline = time.time() + budget
    rpc = snode.rpc_client()
    while time.time() < deadline:
        try:
            ret = rpc.bdev_lvol_transfer_stat(snapshot.snap_bdev)
        except Exception as e:                            # noqa: BLE001
            logger.warning("transfer_stat for %s raised while waiting inline "
                           "(%s); leaving it to the next pass",
                           snapshot.get_id(), e)
            return False
        if not ret:
            return False
        state = ret.get("transfer_state")
        if state == "Done":
            return _finish_completed_transfer(task, snapshot, ret.get("offset"))
        if state == "Failed":
            return False              # the pass-based path records the retry
        if state == "No process":
            return False              # transfer never started; pass-based path retries
        time.sleep(constants.REPL_XFER_POLL_INTERVAL_SEC)
    xfer_timing.stamp("inline_wait_expired", snap=snapshot.get_id(),
                      lvol=snapshot.lvol.get_id(), budget=budget)
    return False


# = tasks_runner_replication_final.DIRECT_ROUND_PREFIX; a copy, so this module
# does not load the cutover runner (a test keeps the two equal).
_DIRECT_ROUND_PREFIX = "repl_round_"


def _endgame_next_snap_name(lvol_id, cluster_id):
    """The name a converging cutover of *lvol_id* gives its NEXT shrink
    snapshot (next_shrink_snap_name), or ""."""
    if not lvol_id:
        return ""
    try:
        tasks = db.get_job_tasks(cluster_id)
    except Exception:                                     # noqa: BLE001
        return ""
    for t in tasks:
        if _converging_cutover(t) and (t.function_params or {}).get("lvol_id") == lvol_id:
            return (t.function_params or {}).get("next_shrink_snap_name") or ""
    return ""


def _prestage_next_landing(snapshot, remote_lv):
    """Create the landing volume of a converging cutover's NEXT round now.

    A landing volume cannot be reused across rounds: the finish converts it
    into the round's snapshot on the target, so every round needs a new one,
    and creating it cost 0.7-2.9 s of every round (bed bf12 2026-11-03). The
    cutover names its next shrink snapshot in advance (next_shrink_snap_name),
    so the landing volume of that round -- REP_<name>, the name the start path
    adopts an existing landing volume by -- is created here, while this
    round's data moves. Best effort: on any failure the next round simply
    creates its own. The cutover discards a spare that ends up unused
    (tasks_runner_replication_final._discard_spare_landing).
    """
    try:
        lvol_id = snapshot.lvol.get_id()
        next_name = _endgame_next_snap_name(lvol_id, snapshot.cluster_id)
        if not next_name:
            return None
        # A direct round's snapshot is never replicated: its pages go straight
        # into the cutover clone, so nothing ever adopts REP_<name>, and the
        # endgame's discard only knows the name of the round after it. Each
        # such pre-stage left a full-size internal volume on the target for
        # good (bed bf29 2026-11-20: REP_repl_round_572cd23c / 1333e5d1 /
        # 41e70a17 on cluster 225e0e10, staged 03:53 / 04:11 / 07:47 by the
        # transfer of a cadence snapshot still in flight when the endgame
        # entered direct mode, never removed).
        if next_name.startswith(_DIRECT_ROUND_PREFIX):
            return None
        rep_name = f"REP_{next_name}"
        peer = _landing_peer(snapshot, remote_lv)
        with xfer_timing.phase("landing_prestage", snap=snapshot.get_id(),
                               lvol=lvol_id):
            # create_only: a volume already wearing the name is left alone
            # (already staged, or another attempt's), never adopted or cleared.
            # No data_uuid: the next round's data does not exist yet.
            landing = peer.prepare_landing(LandingRequest(
                source_cluster_id=snapshot.cluster_id, snapshot_id=snapshot.get_id(),
                landing_name=rep_name, size=snapshot.size, ha_type=snapshot.lvol.ha_type,
                max_namespace_per_subsys=snapshot.lvol.max_namespace_per_subsys or 0,
                node_hint=remote_lv.node_id, pool_hints=[remote_lv.pool_uuid],
                create_only=True, source_lvol_id=lvol_id))
        if landing.state != "ready":
            return None                                   # already staged
        lv_id = landing.lvol_id
        # The cutover may have stopped converging while this was created; it
        # discards spares it knows of, so one it no longer names goes here.
        if _endgame_next_snap_name(lvol_id, snapshot.cluster_id) != next_name:
            logger.info("Pre-staged landing volume %s is no longer needed; deleting it",
                        rep_name)
            peer.discard_landing(lv_id)
            return None
        return lv_id
    except Exception as e:                                # noqa: BLE001
        logger.warning("Pre-staging the next landing volume of %s failed: %s",
                       snapshot.get_id(), e)
        return None


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    _t_landing = None          # set only if we create the landing volume below

    hold = _lvs_transfer_hold(task, snapshot)
    if hold:
        # Not a failure and not a retry: come back when the lvstore frees up.
        task.function_result = f"held: {hold}"
        task.write_to_db()
        logger.info("Holding replication of %s: %s", snapshot.get_id(), hold)
        return False
    # The chain must never grow: a volume whose earlier copies the target has
    # not merged away yet sends no further layer (bed bf21 2026-11-10, see
    # REPL_MAX_UNMERGED_LANDING_LAYERS). Waiting, like the lvstore hold above.
    hold = _merge_behind_hold(task, snapshot)
    if hold:
        task.function_result = f"held: {hold}"
        task.write_to_db()
        return False
    # Drive the transfer from the first online member of the SOURCE lvstore
    # that holds the snapshot -- primary, else secondary, else tertiary. The
    # snapshot exists on every member and leadership does not matter for a
    # read, so an outage of the primary (or of primary and secondary) must not
    # stop replication and let the RPO grow with the outage
    # (see _select_source_node).
    snode, why = _select_source_node(snapshot)
    if snode is None:
        _suspend_for_retry(task, f"no online member of {snapshot.lvol.lvs_name} holds "
                                 f"{snapshot.snap_bdev} ({why}), retrying",
                           backoff=True, count=_switches_exhausted(task))
        return False
    _note_switch(task, "source_node_id", snode.get_id(), "source member")
    task.function_params["source_fallback"] = why
    if snode.get_id() != getattr(snapshot.lvol, "node_id", ""):
        logger.info("Replication task %s: snapshot %s is sent from %s",
                    getattr(task, "uuid", ""), snapshot.get_id(), why)
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
        dest_lvol_id = task.function_params.get("dest_lvol_id")
        if not dest_lvol_id:
            # A snapshot of a receiving volume has no destination of its own:
            # its ancestors travel for the live volume cloned from it.
            if snapshot.lvol.replication_node_id:
                dest_lvol_id = snapshot.lvol.get_id()
            else:
                descendant = _descendant_destination_lvol(snapshot)
                dest_lvol_id = descendant.get_id() if descendant else snapshot.lvol.get_id()
        tasks_controller.add_snapshot_replication_task(
            snapshot.cluster_id, task.node_id, ancestor.get_id(),
            replicate_to_source=replicate_to_source, dest_lvol_id=dest_lvol_id)
        msg = (f"waiting for chain ancestor {ancestor.get_id()} to replicate "
               f"first (a copy without it would have holes)")
        # Waiting is not a failure while the ancestor has a live task that can
        # still deliver it: counting each pass spent the task's retries on its
        # own prerequisite (bf11 2026-11-03: a round-trip volume reached retry
        # 7 of 8 while its 10 GiB base copied, one pass from being dropped).
        # Only a wait for an ancestor nothing is working on counts. A short
        # fixed delay keeps the serial runner from re-walking the chain every
        # pass meanwhile.
        live = _live_replication_task(snapshot.cluster_id, ancestor.get_id())
        _suspend_for_retry(task, msg, count=live is None, level=logging.INFO,
                           backoff=True, backoff_step=0)
        return
    if verdict == "blocked":
        # Back off: a blocked chain (a blob without a snapshot record, an
        # ancestor mid-deletion) does not heal in a minute, and the chain walk
        # costs seconds of the serial runner every pass. Retried at runner
        # speed it took ~19 % of the runner on bf8 (2026-11-02) and held every
        # other volume's migration above the cutover gate.
        msg = f"replication chain of {snapshot.get_id()} is incomplete: {why}; retrying"
        task.function_result = msg
        _suspend_for_retry(task, msg, backoff=True, level=logging.ERROR)
        return

    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        if replicate_to_source:
            # Fail-back: the destination member is the one this task was queued
            # for. A copy replicated FROM there names the pool to land in
            # (counterpart_hint); anything created after the fail-over -- or
            # everything, on a freshly installed cluster -- has none, and the
            # pool is resolved the way the forward direction does (348 fail-back
            # tasks died on an empty counterpart lookup in labs 2026-08-17/18).
            node_hint = task.node_id
            try:
                destination_cluster_id = db.get_storage_node_by_id(task.node_id).cluster_id
            except KeyError:
                msg = f"Unable to find node: {task.node_id}, stopping task"
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            dest_lvol = snapshot.lvol
            counterpart_hint = snapshot.source_replicated_snap_uuid or ""
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
                # Queued without the hint (a task for the same snapshot that
                # already existed keeps its own params): find the descendant.
                descendant = _descendant_destination_lvol(snapshot)
                if descendant is not None:
                    dest_lvol = descendant
                    task.function_params["dest_lvol_id"] = descendant.get_id()
            if not dest_lvol.replication_node_id:
                # A volume replicating to a peer control plane that did not
                # answer when its replication started: place it now, or wait.
                try:
                    placed = lvol_controller.place_on_peer_destination(dest_lvol)
                except PeerUnreachableError as e:
                    _suspend_for_retry(task, f"the peer destination of {dest_lvol.get_id()} "
                                             f"does not answer yet ({e}), retrying",
                                       backoff=True, count=False)
                    return
                except PeerError as e:
                    _suspend_for_retry(task, f"the peer destination cannot place "
                                             f"{dest_lvol.get_id()}: {e}", backoff=True)
                    return
                if placed is not None:
                    dest_lvol = placed
            if not dest_lvol.replication_node_id:
                msg = (f"LVol {snapshot.lvol.get_id()} has no replication destination; "
                       f"dropping replication task for snapshot {snapshot.get_id()}")
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            node_hint = dest_lvol.replication_node_id
            destination_cluster_id = _destination_cluster_of(dest_lvol)
            counterpart_hint = ""

        # One call to the destination (prepare_landing) does what the target
        # side needs:
        #   - a copy of this data already on its lvstore is linked instead of
        #     shipped back (after a relocate the chain came FROM there; the full
        #     copy also collided with the original's name on 2026-09-29);
        #   - a volume an earlier attempt left under the landing name is
        #     adopted when usable, waited for while deleting, cleared otherwise
        #     (case 6, run 20260824_144226: "LVol name must be unique" on every
        #     retry stalled three chains);
        #   - a pre-staged cutover round is adopted the same way;
        #   - else the landing volume is created in the first hinted pool.
        _t_landing = xfer_timing.now()
        # A spare created after the previous cycle's submit carries a name of
        # its own; renamed, it is this transfer's landing volume and the
        # destination adopts it instead of creating one (see replication_spare).
        # Spares need both clusters' records in one database: they are kept
        # when this control plane manages the destination, not across a peer.
        if _target_is_local(snapshot.lvol):
            _adopt_spare(snapshot, f"REP_{snapshot.snap_name}")
        try:
            landing = _peer_for(dest_lvol, destination_cluster_id).prepare_landing(LandingRequest(
                source_cluster_id=snode.cluster_id, snapshot_id=snapshot.get_id(),
                landing_name=f"REP_{snapshot.snap_name}", size=snapshot.size,
                ha_type=snapshot.lvol.ha_type,
                max_namespace_per_subsys=snapshot.lvol.max_namespace_per_subsys or 0,
                data_uuid=snapshot.data_uuid or "", node_hint=node_hint or "",
                pool_hints=_pool_hints(dest_lvol, snode.cluster_id, destination_cluster_id),
                counterpart_hint=counterpart_hint, source_lvol_id=snapshot.lvol.get_id(),
                source_busy=_landing_busy_here(task)))
        except PeerNotFoundError as e:
            msg = f"Replication destination is gone ({e}), stopping task"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        except PeerRefusedError as e:
            # Keep the cause: "Storage node is not online" (the target primary
            # is down and no member leads its lvstore yet) reads very
            # differently from a capacity refusal.
            logger.error(str(e))
            task.function_result = str(e)
            task.write_to_db()
            return

        if landing.state == "counterpart":
            if replicate_to_source:
                snapshot.source_replicated_snap_uuid = landing.counterpart_snapshot_id
            else:
                snapshot.target_replicated_snap_uuid = landing.counterpart_snapshot_id
            snapshot.write_to_db()
            msg = (f"Snapshot {snapshot.get_id()} is already replicated "
                   f"(remote copy {landing.counterpart_snapshot_id} on {landing.lvs_name}, "
                   f"same data); linked, nothing to transfer")
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        if landing.state != "ready" and landing.waiting:
            # In creation, maybe by another worker or runner (bf32): waited
            # out without spending a retry.
            _suspend_for_retry(task, landing.reason, count=False, level=logging.INFO)
            return
        if landing.state != "ready":
            _suspend_for_retry(task, f"{landing.reason}, retrying")
            return
        if landing.adopted:
            logger.info(f"Adopting landing volume {landing.lvol_id} "
                        f"(REP_{snapshot.snap_name}) left by an interrupted attempt or pre-staged")
            xfer_timing.stamp("landing_adopted", snap=snapshot.get_id(),
                              lvol=snapshot.lvol.get_id())
            _t_landing = None          # nothing was created: no create time to report
        task.function_params["remote_lvol_id"] = landing.lvol_id
        task.function_params["landing"] = {
            "lvol_id": landing.lvol_id, "top_bdev": landing.top_bdev, "node_id": landing.node_id,
            "pool_uuid": landing.pool_id, "lvs_name": landing.lvs_name,
            "cluster_id": destination_cluster_id}
        # The destination the later steps reach through _task_peer.
        task.function_params["destination_cluster_id"] = destination_cluster_id
        task.write_to_db()

    remote_lv = _landing_of(task)
    # One call to the destination (ensure_hub) gets it ready to receive: its
    # settled leader (never a tertiary), the receiving hub, the landing
    # volume's map id, and the checks that the volume is not a snapshot yet and
    # the leader still leads. A leaderless target is waited out, not failed:
    # the transfer stays suspended and the RPO grows.
    # A wait that outlasts REPL_TARGET_LEADERLESS_RECOVER_SEC asks the target's
    # guarded recovery (see _leadership_recovery_due); that recovery, not
    # replication, decides whether a grant is safe.
    now = time.time()
    recover = _leadership_recovery_due(task, now)
    waited = int(now - (task.function_params.get("target_leaderless_since") or now))
    hub = _landing_peer(snapshot, remote_lv).ensure_hub(EnsureHubRequest(
        landing_lvol_id=task.function_params["remote_lvol_id"],
        recover_leadership=recover,
        recover_reason=f"replication task {task.uuid} waited {waited}s" if recover else ""))
    if hub.state == "unsettled":
        # Waiting, not failing: the target's leadership is not this transfer's
        # fault, and counting it let a leaderless window kill the task.
        _wait_for_target_leader(task, remote_lv, hub, now)
        return
    _target_leader_found(task)
    _note_switch(task, "target_node_id", hub.leader_node_id, "target leader")
    if hub.state == "failed":
        logger.error(hub.reason)
        _suspend_for_retry(task, f"{hub.reason}, retrying", backoff=hub.backoff)
        return
    if hub.state == "converted":
        # Never transfer into a snapshot. The landing volume is written ONLY
        # before it is converted; after the convert it is part of the target's
        # snapshot chain. One that is already a snapshot here, without this task
        # knowing its transfer completed (that case resumes the finish, see
        # _resume_finish), holds data of unknown completeness: discard it and
        # start over on a fresh one.
        logger.error("Replication task %s: landing volume %s is already a snapshot on %s; "
                     "refusing to transfer into it, discarding it and starting over",
                     task.uuid, remote_lv.top_bdev, ", ".join(hub.converted_on))
        try:
            _landing_peer(snapshot, remote_lv).discard_landing(remote_lv.get_id())
        except Exception as e:                            # noqa: BLE001
            logger.error("Failed to discard landing volume %s: %s", remote_lv.get_id(), e)
        for key in ("remote_lvol_id", "converted_nodes", "offset", "transfer_done"):
            task.function_params.pop(key, None)
        _suspend_for_retry(task, f"landing volume {remote_lv.top_bdev} was already a snapshot "
                                 f"on {', '.join(hub.converted_on)}; discarded, retrying",
                           backoff=True)
        return
    if hub.state == "not_leader":
        _suspend_for_retry(task, f"{hub.reason}, retrying", backoff=True, count=False)
        return

    # 2 attach the target's receiving hub on the source. Transfers must go
    # over a hublvol: the fork demuxes each write by the map id carried in the
    # top 16 bits of the LBA (lvol_map.lvol[offset >> 48]) and that demux only
    # exists on a hublvol namespace. The receiving volume's own namespace is not
    # a valid transfer gateway. This mirrors the (working) migration runner,
    # which has always sent bulk transfers hub+map_id.
    # NOTE deliberately NO bdev_lvol_set_migration_flag: the flag drives the
    # distrib-level special_io machinery of INTRA-cluster migration; it has no
    # place in a cross-cluster receive.
    # The attach and the RUNNING status must not interleave with another
    # worker's detach decision on the same hub session (see _hub_lock).
    with _hub_lock(snode.get_id(), hub.leader_node_id):
        from simplyblock_core.services.replication_final_step import attach_hub_address
        xfer_timing.gap("landing_volume_create", _t_landing,
                        snap=snapshot.get_id(), lvol=snapshot.lvol.get_id())
        with xfer_timing.phase("hub_attach", snap=snapshot.get_id(),
                               lvol=snapshot.lvol.get_id(), tgt=hub.leader_node_id):
            _hub_ctrl, hub_bdev, hub_err = attach_hub_address(snode.rpc_client(), hub.hub)
        if hub_err:
            logger.error(f"Transfer hub attach failed: {hub_err}")
            _suspend_for_retry(task, f"transfer hub attach failed: {hub_err}, retrying",
                               backoff=True)
            return
        task.function_params["hub_ctrl"] = _hub_ctrl
        remote_map_id = hub.map_id

        allow_partial, partial_base = _partial_transfer_decision(
            task, snapshot, replicate_to_source, remote_lv, hub, snode.get_id())
        logger.info("Transfer of %s into %s: %s", snapshot.get_id(),
                    remote_lv.top_bdev,
                    "PARTIAL allowed (landing volume is a clone of the previous "
                    "replicated snapshot on every online member)" if allow_partial
                    else "FULL (no delta basis on every online member; see above "
                         "for which check declined)")

        offset = 0
        if task.function_params.get("offset"):
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

        # Decided before xfer_submit_t is stamped: its presence without
        # xfer_landing_submitted marks an attempt by a control plane older than
        # this decision (see _cow_landing_decision).
        cow_landing = _cow_landing_decision(task, remote_lv, hub, allow_partial,
                                            partial_base, offset)

        # 3 start replication
        xfer_timing.stamp("transfer_submit", snap=snapshot.get_id(),
                          lvol=snapshot.lvol.get_id(), size=snapshot.size)
        task.function_params["xfer_submit_t"] = xfer_timing.now()
        task.write_to_db()
        # The transfer reads the snapshot in the replication class; the
        # snapshot's own class, which client reads through it use, is untouched.
        read_class = (constants.QOS_REPLICATION_WIRE_CLASS
                      if _replication_class_on(snode.cluster_id) else 0)
        # Recorded before the submit: whatever this attempt writes, a later attempt
        # into the same landing volume must not skip the pre-allocation (a cluster
        # it allocated and partly filled would not be copied from the base).
        task.function_params["xfer_landing_submitted"] = remote_lv.get_id()
        task.function_params.pop("cow_landing", None)
        task.write_to_db()
        applied: dict = {}
        snode.rpc_client().bdev_lvol_transfer(
            name=snapshot.snap_bdev,
            offset=offset,
            # SPDK sends every replication request in pieces of at most 64 KiB
            # (spdk #89: no whole-cluster requests), so every transfer, delta,
            # full or resumed, keeps 256 in flight (16 MiB).
            batch_size=constants.REPL_XFER_PARTIAL_BATCH,
            bdev_name=hub_bdev,
            operation="replicate",
            lvol_id=remote_map_id,
            # Safe only because the landing volume was chained onto the previous
            # replicated snapshot above; the fork independently refuses the delta
            # path unless the snapshot's dirty generation is complete.
            allow_partial=allow_partial,
            partial_base=partial_base,
            priority_class=read_class,
            cow_landing=cow_landing,
            applied=applied,
        )
        if applied.get("cow_landing"):
            # The landing volume is filled from its base by copy-on-write during
            # the transfer: the finish skips bdev_lvol_merge_base.
            task.function_params["cow_landing"] = remote_lv.get_id()
        task.status = JobSchedule.STATUS_RUNNING
        task.function_params["start_time"] = int(time.time())
        task.write_to_db()

    # A converging cutover's next round needs a landing volume too: create it
    # now, while this transfer runs, so the next round adopts it instead of
    # paying for the create on its critical path (_prestage_next_landing).
    _prestage_next_landing(snapshot, remote_lv)
    # Outside an endgame the next snapshot's name is unknown: keep one spare
    # landing volume per volume instead, renamed when the next cycle adopts it
    # (only where this control plane manages the destination, see above).
    if not _cutover_owns(snapshot.lvol.get_id(), task.cluster_id) and \
            _target_is_local(snapshot.lvol):
        with xfer_timing.phase("landing_spare_prestage", snap=snapshot.get_id(),
                               lvol=snapshot.lvol.get_id()):
            replication_spare.prestage(snapshot, remote_lv, _create_spare_landing,
                                       task.cluster_id)

    # Do not hand the transfer back to the pass loop and forget about it: wait
    # for it here, polling every 100ms, and finish it in this same pass. Before
    # this, submit and the Done check happened on different passes and a
    # transfer that finished in milliseconds went unnoticed for a median of 81
    # SECONDS (run 20260828_115307). Returns False if it is still running when
    # the budget expires, in which case the pass-based path picks it up as
    # before.
    _await_transfer_completion(task, snapshot, snode)


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


def _source_members(lvol):
    """Members of *lvol*'s lvstore in role order: primary, secondary, tertiary.

    ``lvol.node_id`` is the primary; ``lvol.nodes`` lists the members with the
    primary first. Members of one consistency group share the lvstore, so they
    get the same order -- and, with the same node states, the same choice.
    """
    order = []
    for node_id in [lvol.node_id] + list(getattr(lvol, "nodes", None) or []):
        if node_id and node_id not in order:
            order.append(node_id)
    return order


def _select_source_node(snapshot, exclude=()):
    """The member that drives the transfer of *snapshot*, and why; (None, why).

    The first ONLINE member of the source lvstore, in role order, that holds
    the snapshot. Leadership does not matter: every member keeps the whole
    snapshot stack, and a transfer only READS the snapshot. Waiting for the
    leader instead stalled replication through every leaderless window and
    through outages the leadership probe did not see (an acting secondary does
    not always report "lvs leadership", sbcli #1439), and the RPO grew with
    the outage. Re-selected on every attempt, so a member that went down in
    the idle interval between two runs is skipped.
    """
    lvol = snapshot.lvol
    skipped = []
    for idx, node_id in enumerate(_source_members(lvol)):
        role = ("primary", "secondary", "tertiary")[idx] if idx < 3 else f"member {idx}"
        if node_id in exclude:
            skipped.append(f"{role} {node_id[:8]} excluded")
            continue
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            skipped.append(f"{role} {node_id[:8]} unknown")
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            skipped.append(f"{role} {node_id[:8]} {node.status}")
            continue
        try:
            has_snapshot = node.rpc_client(timeout=10).bdev_get(snapshot.snap_bdev) is not None
        except Exception as e:                            # noqa: BLE001
            skipped.append(f"{role} {node_id[:8]} unreachable ({e})")
            continue
        if not has_snapshot:
            skipped.append(f"{role} {node_id[:8]} lacks {snapshot.snap_bdev}")
            continue
        why = (f"{role} {node_id[:8]}" + (f" (skipped: {'; '.join(skipped)})" if skipped else ""))
        return node, why
    return None, "; ".join(skipped) or "no member"


_SETTLED_LEADER = peer_service._SETTLED_LEADER   # the settle cache lives with the settle



def _stable_target_leader(remote_lv):
    """The member that leads the TARGET lvstore once leadership is settled; or
    (None, why). See peer.service.stable_leader, where the settle runs; the
    finish path calls it here until it moves behind the peer client."""
    return peer_service.stable_leader(db, remote_lv)



def _attached_hub_ctrl(task, hub_node):
    """The hub controller this task attached on the source: recorded at attach
    (a secondary's per-lvstore hub has its own name), else -- tasks from before
    the record -- the node's own transfer hub."""
    ctrl = task.function_params.get("hub_ctrl")
    if ctrl:
        return ctrl
    hub = getattr(hub_node, "transfer_hublvol", None)
    return hub.bdev_name if hub and hub.bdev_name else ""


def _note_switch(task, key, new_id, label):
    """Record which node a task uses; True when it changed from the last attempt.

    A change restarts the snapshot's transfer from offset 0 on the new node:
    the landing volume may hold writes the old path never acknowledged, and a
    full resend is always correct.
    """
    old = task.function_params.get(key)
    task.function_params[key] = new_id
    if old and old != new_id:
        # Counted where the move was forced (_restart_elsewhere); a member
        # that came back in the idle window is not a fault.
        task.function_params.pop("offset", None)
        logger.warning("Replication task %s: %s moved %s -> %s; restarting the "
                       "transfer of snapshot %s from the start there",
                       getattr(task, "uuid", ""), label, old, new_id,
                       task.function_params.get("snapshot_id"))
        return True
    return False


def _transfer_source_node(task, snapshot):
    """The member that runs (or ran) this task's transfer, when online; else
    whichever member would drive it now; else None."""
    src_id = task.function_params.get("source_node_id")
    if src_id:
        try:
            node = db.get_storage_node_by_id(src_id)
            if node.status == StorageNode.STATUS_ONLINE:
                return node
        except KeyError:
            pass
    node, _ = _select_source_node(snapshot)
    return node


def _target_leadership_moved(task, snapshot):
    """Why the target's leadership is no longer where this transfer went, or "".

    Compares the member the transfer was sent to with what the target lvstore
    reports now, asked through the destination's peer client: no leader,
    several, or a different one all mean the receive side moved and the
    transfer has to be redone on the new leader.

    Raises:
        PeerError: the destination cannot answer.
    """
    target_id = task.function_params.get("target_node_id")
    remote_id = task.function_params.get("remote_lvol_id")
    if not target_id or not remote_id:
        return ""
    try:
        remote_lv = _landing_of(task)
    except KeyError:
        return ""
    leaders = _landing_peer(snapshot, remote_lv).describe(
        DescribeRequest(landing_lvol_id=remote_id)).landing_leaders
    if leaders is None or leaders == [target_id]:
        return ""
    return (f"target leadership of {remote_lv.lvs_name} moved from {target_id} to "
            f"{', '.join(leaders) or 'no leader'}")


def _restart_elsewhere(task, msg):
    """Suspend a transfer whose source member or target leader went away, so
    the next attempt starts it over on whichever member drives it then.

    Not counted as a retry (an outage is not the transfer's fault) until the
    task has switched nodes more than REPL_MAX_NODE_SWITCHES times, so a
    flapping node cannot keep a task alive for ever.
    """
    switches = int(task.function_params.get("node_switches") or 0) + 1
    task.function_params["node_switches"] = switches
    task.function_params.pop("offset", None)
    # The delay follows the node switches, not task.retry: retry also counts
    # waits that are no failure (a chain ancestor replicating first), and on
    # bf8 (2026-11-01) four of those turned a target primary crash into a
    # 240 s delay -- longer than the crashed node took to come back, so the
    # transfer was never repeated on the new leader during the outage.
    _suspend_for_retry(task, f"{msg}; restarting the transfer on the next "
                             f"available member", backoff=True,
                       count=_switches_exhausted(task), backoff_step=switches - 1)


def _switches_exhausted(task):
    """True when node switches must start counting as retries (see constants)."""
    return int(task.function_params.get("node_switches") or 0) > constants.REPL_MAX_NODE_SWITCHES


def _other_active_transfers_to_node(current_task, target_node_id):
    """True when another RUNNING snapshot-replication task is transferring into
    *target_node_id* — its writes ride the same shared hub session, so the hub
    must not be detached under it.

    Compares the member each task sent its transfer to (``target_node_id``, set
    before the task runs), which is the member owning the hub session. That is
    this cluster's own task record: no read of the target's volumes, which a
    split control plane does not have. A running task that names no target
    member keeps the session up, since it may be using it.
    """
    for t in db.get_job_tasks(current_task.cluster_id):
        if (t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
                and t.get_id() != current_task.get_id()
                and t.status == JobSchedule.STATUS_RUNNING
                and t.function_params.get("remote_lvol_id")):
            sent_to = t.function_params.get("target_node_id")
            if not sent_to or sent_to == target_node_id:
                return True
    return False


class _CopyState(NamedTuple):
    """A replicated copy on the target, as retention needs to know it."""

    exists: bool
    #: The successor's copy is chained onto this one, so deleting this one
    #: swap-merges its segments instead of dropping them.
    chained: bool
    #: A live volume (e.g. a failed-over one) is cloned from this copy.
    has_dependent_clone: bool


def _destination_cluster(source_lvol, target_uuid):
    """The cluster holding *source_lvol*'s replicated copies.

    The volume's policy target (a peer's cluster is known only there), else the
    replication node it is configured with; failing both, the copy's record.

    Raises:
        KeyError: neither the node nor the copy is known here.
    """
    with contextlib.suppress(KeyError):
        return _destination_cluster_of(source_lvol)
    return db.get_snapshot_by_id(target_uuid).cluster_id


def _copy_peer(source_lvol, target_uuid):
    """The peer client of the cluster holding *source_lvol*'s copies, or None
    when neither its replication node nor the copy is known here."""
    try:
        return _peer_for(source_lvol, _destination_cluster(source_lvol, target_uuid))
    except KeyError:
        return None


def _delete_copy(source_lvol, target_uuid):
    """Delete the copy *target_uuid* on the destination; whether it is gone.

    Raises:
        PeerError: the destination cannot answer.
    """
    peer = _copy_peer(source_lvol, target_uuid)
    if peer is None:
        return True                     # unknown here: nothing to delete
    try:
        answer = peer.delete_snapshot(target_uuid)
    except PeerRefusedError as e:
        logger.warning("Copy %s stays: the destination refused to delete it: %s", target_uuid, e)
        return False
    if not answer.deleted:
        logger.warning("Copy %s stays: %s", target_uuid, answer.reason)
    return answer.deleted


def _copy_state(source_lvol, target_uuid, successor) -> _CopyState:
    """What the target says about the copy *target_uuid* before retention deletes it.

    One peer call answers three questions. Retention deletes a predecessor
    expecting SPDK to swap-merge its segments into the successor CHAINED to it;
    without the link the delete drops them, and the target is left holding the
    newest delta over holes (all-zeros DR fail-over, labs 2026-08-10..17). And
    a copy a live volume is cloned from must outlive that volume.

    Raises:
        PeerError: the destination cannot answer.
    """
    peer = _copy_peer(source_lvol, target_uuid)
    if peer is None:
        return _CopyState(exists=False, chained=False, has_dependent_clone=False)
    successor_target = successor.target_replicated_snap_uuid
    answer = peer.describe(DescribeRequest(
        snapshot_ids=[target_uuid],
        chain_checks=([ChainCheck(successor_id=successor_target, predecessor_id=target_uuid)]
                      if successor_target else [])))
    copy = answer.snapshots[0]
    return _CopyState(exists=copy.exists,
                      chained=bool(answer.chains) and answer.chains[0].chained,
                      has_dependent_clone=copy.has_dependent_clone)


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


def _cutover_holds_retention(source_lvol):
    """True while *source_lvol*'s cutover endgame is in progress.

    The endgame converges by taking snapshot after snapshot (shrink rounds)
    until the remaining delta fits the freeze. Pruning between those rounds
    deletes the predecessor of every new copy, and a delete is a merge that
    physically moves data on both clusters (copy-on-write: ultra #222 / spdk
    #86) -- load competing with the convergence and the freeze themselves. So
    nothing of the volume is pruned from its first shrink snapshot until the
    cutover task ends; prune_after_cutover then keeps one.
    """
    try:
        cluster_id = db.get_storage_node_by_id(source_lvol.node_id).cluster_id
        tasks = db.get_job_tasks(cluster_id)
    except Exception:                                     # noqa: BLE001
        return False
    for task in tasks:
        if task.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if task.status == JobSchedule.STATUS_DONE or task.canceled:
            continue
        params = task.function_params or {}
        if params.get("lvol_id") == source_lvol.get_id() and (
                params.get("shrink_snap_id") or params.get("cutover_lvs")):
            return True
    return False


def prune_after_cutover(source_lvol):
    """After a completed cutover: keep only the newest replicated snapshot pair.

    The endgame's snapshots were held while it ran (_cutover_holds_retention);
    once IO serves from the target none of them is a delta base any more, so
    all but the newest go, on both sides. The newest target copy is what the
    cut-over volume is cloned from, and its source counterpart stays with it.
    """
    _prune_internal_snapshots(source_lvol, after_cutover=True)


_PRUNING: set = set()
_PRUNING_LOCK = threading.Lock()


def prune_async(source_lvol, keep=None):
    """Run retention for *source_lvol* in the background and return at once.

    Pruning runs alongside replication and the cutover endgame but nothing
    waits for it: a delete is a merge that moves data on both clusters and
    takes seconds to minutes under load (bf16, 2026-11-07). At most one prune
    per volume runs at a time; a call while one runs is dropped -- the next
    replication finish starts another.
    """
    vid = source_lvol.get_id()
    with _PRUNING_LOCK:
        if vid in _PRUNING:
            return False
        _PRUNING.add(vid)

    def _run():
        try:
            _prune_internal_snapshots(source_lvol, keep=keep)
        except Exception as e:                            # noqa: BLE001
            logger.error("Retention for lvol %s failed: %s", vid, e)
        finally:
            with _PRUNING_LOCK:
                _PRUNING.discard(vid)

    threading.Thread(target=_run, name=f"prune-{vid[:8]}", daemon=True).start()
    return True


def _retention_plan(source_lvol, replicated_internal, after_cutover=False, keep=None):
    """(keep, schedule, candidates) of retention for *replicated_internal*
    (oldest first): the (index, snapshot) pairs retention would prune."""
    # After a cutover there is no next delta to chain: one pair is enough.
    if keep is None:
        keep = 1 if after_cutover else _keep_replicated_for(source_lvol)
    # A retention SCHEDULE, when the policy defines one, decides which older
    # snapshots survive; without it retention stays the flat "newest N".
    # Either way the newest `keep` are protected, because deleting a snapshot
    # swap-merges its segments into the successor chained to it.
    schedule = [] if (after_cutover or keep == 1) else _retention_schedule_for(source_lvol)
    if schedule:
        retained_ts = snapshot_retention.select_retained(
            [s.created_at for s in replicated_internal], schedule,
            now=time.time(), always_keep_newest=keep)
        candidates = [(i, s) for i, s in enumerate(replicated_internal)
                      if s.created_at not in retained_ts]
    elif len(replicated_internal) <= keep:
        candidates = []
    else:
        candidates = list(enumerate(replicated_internal))[:-keep]
    return keep, schedule, candidates


#: How far merge_backlog follows a copy chain down on the target.
_MERGE_WALK_LIMIT = 64


def merge_backlog(source_lvol, cluster_id, exclude_snapshot_id=""):
    """How many of *source_lvol*'s layers on the target are not merged away
    yet, and what they are: (count, detail, prunable) -- prunable is how many
    of them retention has yet to start on (the rest are merging already).

    Every transfer leaves one layer on the target: its landing copy,
    converted into a snapshot on top of the previous copy. Retention merges
    the oldest away (_prune_internal_snapshots: the delete swap-merges it into
    its successor), so a volume that keeps up sits on its retained pairs plus
    at most the one being merged. Counted here, beyond those:

      * replicated pairs retention would prune but has not started on (a
        group's recovery point and a copy a volume is cloned from are held on
        purpose and not counted -- retention never removes them);
      * copies being deleted, i.e. swap-merging, on the target -- found
        through the source's links and by walking the newest copy's chain,
        which still holds a copy whose source record is gone already;
      * landing copies whose transfer is done and that still merge with their
        base (bdev_lvol_merge_base) before the convert.

    Bed bf21 2026-11-10: none of this held replication back, chains reached
    depth 7 and the clusters 93-99 % (REPL_MAX_UNMERGED_LANDING_LAYERS).
    """
    snaps = db.get_snapshots_by_lvol_id(source_lvol.get_id())
    replicated = sorted(
        (s for s in snaps
         if s.snap_type == SnapShot.TYPE_INTERNAL and s.status == SnapShot.STATUS_ONLINE
         and s.target_replicated_snap_uuid),
        key=lambda s: s.created_at)
    _keep, _schedule, candidates = _retention_plan(source_lvol, replicated)
    group_keep = replication_recovery_points.group_recovery_point_ids(
        {getattr(s, "group_id", "") for s in replicated}, db=db) if candidates else set()
    # The destination's side of the backlog (copies a live clone depends on,
    # copies being swap-merged there) is read from its records, which are in
    # this database only when this control plane manages the destination. A
    # destination on a peer control plane counts the source's side alone.
    local = _target_is_local(source_lvol)
    pending = [s for _, s in candidates
               if s.get_id() not in group_keep
               and s.target_replicated_snap_uuid not in group_keep
               and not (local and _has_dependent_clone(s.target_replicated_snap_uuid))]

    replicated_ids = {s.get_id() for s in replicated}
    merging = set()
    newest_copy = None
    for s in snaps:
        if not local or not s.target_replicated_snap_uuid:
            continue
        try:
            copy = db.get_snapshot_by_id(s.target_replicated_snap_uuid)
        except KeyError:
            continue
        if copy.status == SnapShot.STATUS_IN_DELETION:
            merging.add(copy.get_id())
        elif s.get_id() in replicated_ids and (newest_copy is None or s.created_at > newest_copy[0]):
            newest_copy = (s.created_at, copy)
    if newest_copy is not None:
        node, steps = newest_copy[1], 0
        while node.prev_snap_uuid and steps < _MERGE_WALK_LIMIT:
            steps += 1
            try:
                node = db.get_snapshot_by_id(node.prev_snap_uuid)
            except KeyError:
                break
            if node.status == SnapShot.STATUS_IN_DELETION:
                merging.add(node.get_id())

    snap_ids = {s.get_id() for s in snaps} - {exclude_snapshot_id}
    landing = 0
    for t in db.get_job_tasks_by_function(cluster_id, JobSchedule.FN_SNAPSHOT_REPLICATION):
        p = t.function_params or {}
        if (t.status != JobSchedule.STATUS_DONE and not t.canceled
                and p.get("snapshot_id") in snap_ids and p.get("transfer_done")):
            landing += 1
    count = len(pending) + len(merging) + landing
    return count, (f"{len(pending)} beyond retention, {len(merging)} being merged, "
                   f"{landing} landing copies merging"), len(pending)


def _adopt_spare(snapshot, rep_name):
    """Rename the volume's spare landing volume to *rep_name*, unless a volume
    wears that name already. Best effort: a spare is an optimisation and never
    stops a transfer, which then creates its landing volume as before."""
    try:
        with contextlib.suppress(KeyError):
            db.get_lvol_by_name(rep_name, include_deleted=True)
            return                              # the name is taken: nothing to adopt
        if replication_spare.adopt(snapshot, rep_name) is not None:
            xfer_timing.stamp("landing_spare_adopted", snap=snapshot.get_id(),
                              lvol=snapshot.lvol.get_id())
    except Exception as e:                                # noqa: BLE001
        logger.warning("Adopting a spare landing volume as %s failed: %s", rep_name, e)


def _create_spare_landing(name, snapshot, remote_node, pool_uuid):
    """replication_spare.prestage's landing-volume factory: create *name* on the
    destination, through its prepare_landing (create_only), so the create is
    registered as in flight there like any landing volume's (bf32).
    Returns (lvol id, error)."""
    try:
        answer = LocalPeerClient(remote_node.cluster_id).prepare_landing(LandingRequest(
            source_cluster_id=snapshot.cluster_id, snapshot_id=snapshot.get_id(),
            landing_name=name, size=snapshot.size, ha_type=snapshot.lvol.ha_type,
            max_namespace_per_subsys=snapshot.lvol.max_namespace_per_subsys or 0,
            node_hint=remote_node.get_id(), pool_hints=[pool_uuid] if pool_uuid else [],
            create_only=True, source_lvol_id=snapshot.lvol.get_id()))
    except Exception as e:                                # noqa: BLE001
        return None, str(e)
    if answer.state != "ready":
        return None, answer.reason or answer.state
    return answer.lvol_id, None


def _target_is_local(source_lvol):
    """True when this control plane manages the destination of *source_lvol*'s
    replication, so the destination's records are in this database. False
    when the volume's policy target names a peer control plane (the
    control-plane split), whose records only that peer has."""
    try:
        policy = db.get_replication_policy_for_lvol(source_lvol)
        if policy is None:
            return True
        target = db.get_replication_target_by_id(policy.target_id)
    except KeyError:
        return True
    except Exception as e:                                # noqa: BLE001
        # Unreadable: local. What runs only for a local destination reads its
        # records here and finds none of a peer's, so it does nothing then.
        logger.warning("Replication target of %s unreadable (%s); taken as local",
                       source_lvol.get_id(), e)
        return True
    return target.has_peer() is not True


def _has_dependent_clone(snapshot_uuid):
    """True when a live volume is cloned from the copy *snapshot_uuid* (see
    peer.service._has_dependent_clone; the destination's records)."""
    return peer_service._has_dependent_clone(db, snapshot_uuid)


def _hold_progress(hold, value, now):
    """The hold record after a sample of a backlog that is over its limit:
    (hold, expired). The clock restarts whenever the backlog shrank; a hold
    that saw no progress for REPL_MERGE_HOLD_MAX_SEC has expired (pure)."""
    if not hold or value < hold.get("value", value + 1):
        return {"since": now, "value": value}, False
    return hold, now - float(hold["since"]) >= constants.REPL_MERGE_HOLD_MAX_SEC


def _merge_behind_hold(task, snapshot):
    """Why this volume's next transfer waits for the target to merge, or "".

    Replication phase only: a converging cutover's rounds run under their own
    bound (REPL_CUTOVER_MAX_SHRINK_ROUNDS), and a chain-ancestor snapshot or a
    fail-back transfer has no retention of its own to wait for. While held,
    the volume's retention is (re)started, so the merge it waits for runs --
    a finish whose prune was dropped (prune_async, one per volume) otherwise
    leaves the backlog untouched until the next finish, which the hold stops.
    Never holds on an error: a guard that cannot measure must not stop
    replication."""
    params = task.function_params
    try:
        if (snapshot.snap_type != SnapShot.TYPE_INTERNAL
                or params.get("replicate_to_source") or params.get("transfer_done")):
            return ""
        lvol = db.get_lvol_by_id(snapshot.lvol.get_id())
        layers, detail, prunable = merge_backlog(lvol, task.cluster_id,
                                                 exclude_snapshot_id=snapshot.get_id())
        # (read only when over the limit: it scans the task table)
        if (layers > constants.REPL_MAX_UNMERGED_LANDING_LAYERS
                and _cutover_owns(lvol.get_id(), task.cluster_id)):
            layers = 0
    except Exception as e:                                # noqa: BLE001
        logger.debug("No merge backlog reading for %s: %s", snapshot.get_id(), e)
        return ""
    hold = params.get("merge_hold")
    if layers <= constants.REPL_MAX_UNMERGED_LANDING_LAYERS:
        if hold:
            params.pop("merge_hold", None)
            logger.info("Replication of %s resumes: the target merged its backlog (%s)",
                        snapshot.get_id(), detail)
            xfer_timing.stamp("replication_resumed", snap=snapshot.get_id(),
                              lvol=lvol.get_id(), layers=layers)
        return ""
    new_hold, expired = _hold_progress(hold, layers, time.time())
    if expired:
        params.pop("merge_hold", None)
        logger.error("Replication of %s was held for %ds without the target merging "
                     "any of its backlog (%s); transferring anyway -- investigate the "
                     "retention of lvol %s", snapshot.get_id(),
                     constants.REPL_MERGE_HOLD_MAX_SEC, detail, lvol.get_id())
        xfer_timing.stamp("replication_hold_expired", snap=snapshot.get_id(),
                          lvol=lvol.get_id(), layers=layers)
        return ""
    params["merge_hold"] = new_hold
    why = (f"merge behind: {layers} unmerged landing layers of lvol {lvol.get_id()[:8]} "
           f"on the target (limit {constants.REPL_MAX_UNMERGED_LANDING_LAYERS}: {detail})")
    if not hold:
        logger.warning("Replication held for %s: %s", snapshot.get_id(), why)
        xfer_timing.stamp("replication_held_merge_behind", snap=snapshot.get_id(),
                          lvol=lvol.get_id(), layers=layers)
    if prunable:
        # (not for copies already merging: retention would only rescan)
        prune_async(lvol)
    return why


def _prune_internal_snapshots(source_lvol, after_cutover=False, keep=None):
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
    # No hold during a cutover endgame any more: bf16 (2026-11-07) held
    # retention through three failed attempts of one volume, its chain on the
    # target passed the page-copy limit (chain_max), and every copy-on-write
    # into its landing volumes became a 2 MiB read+write (240-370 ms each).
    # Pruning runs alongside the rounds instead (prune_async), never waited on.
    replicated_internal = [
        s for s in db.get_snapshots_by_node_id(source_lvol.node_id)
        if s.lvol.get_id() == source_lvol.get_id()
        and s.snap_type == SnapShot.TYPE_INTERNAL
        and s.status == SnapShot.STATUS_ONLINE
        and s.target_replicated_snap_uuid
    ]
    replicated_internal.sort(key=lambda s: s.created_at)
    keep, schedule, candidates = _retention_plan(
        source_lvol, replicated_internal, after_cutover=after_cutover, keep=keep)
    # Say which retention is in force, every time it prunes. Soak run
    # 20260827_224741 ended with 2 snapshots at consecutive cadence ticks after
    # 124 minutes under `5m:15m,7m:30m,10m:1h` -- which is exactly what the FLAT
    # keep-N path produces, and nothing in the log said which path ran. The
    # ladder itself is provably correct (test_case11_retention_ladder), so the
    # open question is whether the schedule reaches this function at all.
    logger.info(
        "Retention for lvol %s: %s (replicated internal snapshots: %d, keep=%d)",
        source_lvol.get_id(),
        ("schedule %s" % snapshot_retention.describe(schedule)) if schedule
        else "FLAT keep-newest (no schedule on the policy)",
        len(replicated_internal), keep)
    if not candidates:
        return
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
    # A consistency group restores as one cut: the snapshots of its newest
    # complete generation survive retention on both sides even when a member's
    # own count would prune them (a lagging member keeps the generation open).
    group_keep = replication_recovery_points.group_recovery_point_ids(
        {getattr(s, "group_id", "") for s in replicated_internal}, db=db)
    for index, snap in candidates:
        if snap.get_id() in group_keep or snap.target_replicated_snap_uuid in group_keep:
            logger.info("Keeping replicated internal snapshot %s: it belongs to its "
                        "consistency group's newest replicated generation", snap.get_id())
            continue
        target_uuid = snap.target_replicated_snap_uuid
        copy = _copy_state(source_lvol, target_uuid, replicated_internal[index + 1])
        if not copy.exists:
            target_uuid = ""  # already gone — fall through to source cleanup
        if target_uuid and not copy.chained:
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
        if target_uuid and copy.has_dependent_clone:
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
            if not _delete_copy(source_lvol, target_uuid):
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
    replicated handed the chain step a BLANK remote-copy id, which it
    read as "there is a remote copy, but I cannot resolve it" and refused to
    finalize on. The transfer then retried for ever and the volume's snapshot
    never got its replicated marker (lab 2026-08-20: 299 "Predecessor snapshot
    ... has remote copy  but it cannot be resolved ('Snapshot lookup with a
    blank id')" in 75 minutes, and case 4 never produced a post-baseline
    replicated point)."""
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    # The newest older replicated SIBLING is the predecessor, and it must be
    # looked for FIRST: snapshot_controller.add stamps snap_ref_id on EVERY
    # snapshot of a cloned volume, naming the clone lineage's ORIGIN, not the
    # snapshot before this one. Honouring that reference ahead of the siblings
    # chained every delta of a failed-over volume onto the fail-over point
    # instead of onto the previous copy -- a star, not a chain. A partial
    # transfer carries only the delta against the predecessor on the source,
    # so each copy on the destination held "fail-over point + one 5-minute
    # delta" and nothing in between; the next relocate cloned from such a copy
    # and the guest found a file system with holes (2026-10-01, wp-db on the
    # real test bed: XFS metadata CRC errors, MariaDB would not start).
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
    parent = None
    if parent_uuid:
        try:
            parent = db.get_snapshot_by_id(parent_uuid)
        except KeyError as e:
            logger.error("clone parent %s unresolvable: %s", parent_uuid, e)
            return None
    if parent is not None and getattr(parent, attr, None):
        logger.info("Chain parent for %s is the clone's origin snapshot %s",
                    snapshot.get_id(), parent.get_id())
        return parent
    # Last: a referenced snapshot (snap_ref_id). For a clone it names the
    # lineage's origin, which the clone parent above already covers; it is
    # consulted only when nothing else resolved, and only when replicated.
    if snapshot.snap_ref_id and snapshot.snap_ref_id != parent_uuid:
        try:
            referenced = db.get_snapshot_by_id(snapshot.snap_ref_id)
        except KeyError as e:
            logger.error("snap_ref_id %s unresolvable: %s", snapshot.snap_ref_id, e)
            return None
        if getattr(referenced, attr, ""):
            logger.info("Chain parent for %s is its referenced snapshot %s",
                        snapshot.get_id(), referenced.get_id())
            return referenced
    return None


def _prechain_landing_volume(task, snapshot, replicate_to_source, remote_lv, leader_id):
    """Chain the landing volume onto the destination's copy of the PREVIOUS
    snapshot, before the transfer runs, so only the delta has to be sent.

    Returns True when the landing volume is chained on every member of its
    lvstore, which is the precondition for asking for a PARTIAL
    transfer. Returns False to mean "send a full transfer", which is always
    correct.

    This cluster picks the predecessor -- its own newest replicated snapshot
    before this one -- and the destination chains onto that snapshot's copy
    (peer.service.chain_landing explains why chaining first makes a delta
    correct, and the bar it sets). Members chained are recorded on the task so
    neither a retry nor the finish step adds the same clone entry twice.
    """
    copy_id = _predecessor_copy_id(snapshot, replicate_to_source)
    if not copy_id:
        logger.info("Landing volume %s stays a full-transfer target: %s starts the chain, "
                    "so there is no predecessor's copy to clone from",
                    remote_lv.top_bdev, snapshot.get_id())
        return False

    prechained = list(task.function_params.get("prechained_node_ids") or [])
    answer = _landing_peer(snapshot, remote_lv).chain(ChainRequest(
        landing_lvol_id=remote_lv.get_id(), leader_node_id=leader_id,
        predecessor_id=copy_id, chained_on=prechained))
    # Record whatever actually landed, on the failure path too: the entry
    # exists on that member now.
    if answer.chained_on != prechained:
        task.function_params["prechained_node_ids"] = answer.chained_on
        task.write_to_db()
    if answer.state != "chained":
        # A full transfer into a partially chained volume is still correct: it
        # writes every cluster the snapshot owns, and a chained member reads
        # the remainder from the parent exactly as it should.
        logger.info("Landing volume %s stays a full-transfer target: %s",
                    remote_lv.top_bdev, answer.reason)
        return False
    return True


def _partial_transfer_decision(task, snapshot, replicate_to_source, remote_lv,
                               hub, source_node_id):
    """Whether this transfer may ship only the delta, chaining the landing
    volume first if that has not happened yet. *hub* is the destination's
    ensure_hub answer: its leader and whether that leader's pair is online.

    The verdict is cached on the task so a resumed transfer (offset > 0) keeps
    the mode its first attempt used -- but it is KEYED to the landing volume it
    was made about. An interrupted attempt can delete a half-created landing
    volume and build a fresh, unchained one; carrying "partial is fine" onto
    that volume would ship a delta into something that holds nothing, which is
    precisely the silent data loss this design exists to avoid. A key mismatch
    throws away the old verdict AND the old chain record and re-derives both.
    """
    landing_key = remote_lv.get_id()
    if task.function_params.get("allow_partial_landing") == landing_key:
        return (bool(task.function_params.get("allow_partial")),
                int(task.function_params.get("partial_base") or 0))

    # Chain state recorded against a previous landing volume says nothing about
    # this one.
    task.function_params["prechained_node_ids"] = []
    task.function_params.pop("chain_deferred_to", None)
    pred = _finishing_predecessor(snapshot, replicate_to_source)
    if pred is not None:
        allow_partial, partial_base = _deferred_partial_decision(
            task, snapshot, pred, hub, source_node_id)
        task.function_params["allow_partial"] = allow_partial
        task.function_params["partial_base"] = partial_base
        task.function_params["allow_partial_landing"] = landing_key
        task.write_to_db()
        return allow_partial, partial_base

    allow_partial = _prechain_landing_volume(
        task, snapshot, replicate_to_source, remote_lv, hub.leader_node_id)
    partial_base = 0
    if allow_partial:
        # The landing volume is chained onto the copy of this predecessor; the
        # delta must have been computed against exactly it (the fork checks
        # the base blob id), and no other member may have written past the
        # transferring node's bitmap meanwhile.
        prev_snap = _previous_replicated_snapshot(snapshot, replicate_to_source)
        ok, why = (replication_delta.delta_basis(prev_snap, snapshot, source_node_id)
                   if prev_snap else (False, "no replicated predecessor"))
        if ok and prev_snap.blobid:
            partial_base = int(prev_snap.blobid)
        else:
            logger.info("Transfer of %s stays FULL: %s", snapshot.get_id(),
                        why or "predecessor has no blob id")
            allow_partial = False
    task.function_params["allow_partial"] = allow_partial
    task.function_params["partial_base"] = partial_base
    task.function_params["allow_partial_landing"] = landing_key
    task.write_to_db()
    return allow_partial, partial_base


def _cow_landing_applied(task, landing_id):
    """True when the transfer into *landing_id* landed merge-free: SPDK applied
    the cow_landing this task asked for (see _cow_landing_decision)."""
    return task.function_params.get("cow_landing") == landing_id


def _cow_landing_decision(task, remote_lv, hub, allow_partial, partial_base, offset):
    """Whether this delta transfer may land merge-free (SPDK ``cow_landing``).

    A delta writes only the dirty ranges of a partly covered cluster. The
    destination pre-allocates such clusters and a merge job fills the rest
    from the base afterwards (bf11: 7.25 GB, 66 s per cycle). With page copy
    on the destination the first write into an unallocated cluster of a
    chained landing volume copies it from the base by metadata, so the
    pre-allocation and the merge can go. Only when ALL of these hold:

      * a real delta (allow_partial and a partial_base);
      * the landing volume was chained onto its base BEFORE the transfer on the
        receiving member, *hub*.leader_node_id (a deferred chain --
        chain_deferred_to -- happens after, and a copy-on-write before it
        would copy zeroes);
      * the destination cluster has page copy (*hub*.page_copy; otherwise
        every such write copies 2 MiB inline, and the merge is cheaper);
      * a fresh transfer (offset 0) and the first one into THIS landing
        volume: an earlier attempt may have pre-allocated clusters and filled
        them partly, and those would never be copied from the base.
    """
    if not (allow_partial and partial_base) or offset:
        return False
    landing = remote_lv.get_id()
    if task.function_params.get("xfer_landing_submitted") == landing:
        return False
    if (task.function_params.get("xfer_submit_t")
            and not task.function_params.get("xfer_landing_submitted")):
        # submitted by an older control plane, into a landing volume nobody
        # recorded: it may hold pre-allocated, partly filled clusters
        return False
    if task.function_params.get("chain_deferred_to"):
        return False
    if hub.leader_node_id not in _prechained_nodes_for(task, landing):
        return False
    return bool(hub.page_copy)


def _deferred_partial_decision(task, snapshot, pred, hub, source_node_id):
    """The transfer mode while *pred*'s copy is still being merged on the
    target: the landing volume stays unchained until the finish (see
    _finishing_predecessor), and a delta is sent on the same terms as on a
    pre-chained volume -- a clean basis, and both members of the destination
    pair online (*hub*.pair_online). A full transfer is chained there all the
    same."""
    task.function_params["chain_deferred_to"] = pred.get_id()
    logger.info("Transfer of %s runs while %s's copy is merged; its landing volume is "
                "chained onto that copy after the transfer", snapshot.get_id(), pred.get_id())
    if not hub.pair_online:
        logger.info("Transfer of %s stays FULL: a member of %s's lvstore cannot be reached",
                    snapshot.get_id(), hub.leader_node_id)
        return False, 0
    ok, why = replication_delta.delta_basis(pred, snapshot, source_node_id)
    if ok and pred.blobid:
        return True, int(pred.blobid)
    logger.info("Transfer of %s stays FULL: %s", snapshot.get_id(),
                why or "predecessor has no blob id")
    return False, 0


def _deferred_chain_gate(task, snapshot, replicate_to_source):
    """For a transfer that ran while its predecessor was merged
    (chain_deferred_to): "ok" once the predecessor's copy exists and is the
    one the finish will chain onto, "pending" while it is still being made,
    "restart" when it will not be made (its task ended without it, or it is
    being deleted) -- the copy must then be sent again on whatever base there
    is."""
    pred_id = task.function_params.get("chain_deferred_to")
    if not pred_id:
        return "ok"
    try:
        pred = db.get_snapshot_by_id(pred_id)
    except KeyError:
        return "restart"
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    if getattr(pred, attr, ""):
        chain = _previous_replicated_snapshot(snapshot, replicate_to_source)
        if chain is None or chain.get_id() != pred_id:
            logger.error("Replication task %s: %s would chain onto %s, not onto %s its "
                         "delta was taken against", task.uuid, snapshot.get_id(),
                         chain.get_id() if chain else "nothing", pred_id)
            return "restart"
        return "ok"
    if pred.status == SnapShot.STATUS_IN_DELETION:
        return "restart"
    if _live_replication_task(pred.cluster_id, pred_id) is None:
        return "restart"
    return "pending"


def _discard_landing_for_retry(task, snapshot, remote_lv, msg):
    """Delete the landing volume and send the snapshot again from the start."""
    try:
        _landing_peer(snapshot, remote_lv).discard_landing(remote_lv.get_id())
    except Exception as e:                                # noqa: BLE001
        logger.error("Failed to discard landing volume %s: %s", remote_lv.get_id(), e)
    for key in ("remote_lvol_id", "converted_nodes", "offset", "transfer_done",
                "merged_landing", "chain_deferred_to", "cow_landing"):
        task.function_params.pop(key, None)
    _suspend_for_retry(task, msg, backoff=True)


def _prechained_nodes_for(task, landing_id):
    """Members already carrying the landing volume's clone entry, in the order
    they were chained.

    Empty unless the record was made about THIS landing volume: adding the same
    clone entry twice is not idempotent on the SPDK side, and trusting a stale
    record would skip a chain the volume actually needs.
    """
    if task.function_params.get("allow_partial_landing") != landing_id:
        return []
    return list(task.function_params.get("prechained_node_ids") or [])


def _predecessor_copy_id(snapshot, replicate_to_source):
    """The id of the destination's copy of *snapshot*'s predecessor -- the copy
    the new one is chained onto -- or "" when this snapshot starts the chain.

    The predecessor is this cluster's newest older snapshot of the volume that
    was replicated (_previous_replicated_snapshot). A blank copy id is NO copy,
    not an unresolvable one: the predecessor was never replicated, so there is
    nothing to chain onto (treating it as broken made the task retry for ever).
    A fail-back reads the source-side marker. Whether the destination can use
    the copy it names is the destination's call (chain, finish).
    """
    prev = _previous_replicated_snapshot(snapshot, replicate_to_source)
    if prev is None:
        return ""
    return ((prev.source_replicated_snap_uuid if replicate_to_source
             else prev.target_replicated_snap_uuid) or "")


#: process_snap_replicate_finish could not finish yet: the landing volume is
#: still merging with its base. Not a failure -- the next pass finishes it.
FINISH_PENDING = "finish-pending"
#: process_snap_replicate_finish discarded the landing volume and suspended the
#: task for a new transfer: nothing left for the caller to record.
FINISH_ABANDONED = "finish-abandoned"


def _task_peer(task, snapshot):
    """The peer client of *task*'s destination: the cluster prepare_landing
    recorded, reached through the policy of the volume whose destination it is
    (a chain ancestor's is its descendant's, dest_lvol_id).

    Raises:
        KeyError: a task from before the record names a landing volume or
            node unknown here.
    """
    lvol = snapshot.lvol
    if task.function_params.get("dest_lvol_id"):
        with contextlib.suppress(KeyError):
            lvol = db.get_lvol_by_id(task.function_params["dest_lvol_id"])
    cluster_id = task.function_params.get("destination_cluster_id")
    if not cluster_id:
        cluster_id = _landing_of(task).cluster_id
    return _peer_for(lvol, cluster_id)


def _close_hub(task, snapshot, hub_node_id):
    """Detach the transfer session on the source -- but ONLY when this was the
    last active transfer into that target member. The hub is ONE shared session
    per target member: a naive per-cycle detach rips the qpair out from under
    the other volumes' in-flight transfers, mass-failing their IO on the hub
    and churning LVS leadership on the target ("receive io for hublvol in
    nonleader mode" storms, 2026-08-13). A converging cutover sends its next
    round through the same hub within a second: the session stays up for the
    whole endgame (run_cutover leaves it attached as it always has)."""
    hub_ctrl = task.function_params.get("hub_ctrl")
    if not hub_ctrl:
        # a task from before the record: the member's own transfer hub
        with contextlib.suppress(KeyError):
            hub_ctrl = _attached_hub_ctrl(task, db.get_storage_node_by_id(hub_node_id))
    if not hub_ctrl:
        return
    if _cutover_owns(snapshot.lvol.get_id(), task.cluster_id):
        xfer_timing.stamp("hub_kept", snap=snapshot.get_id(), lvol=snapshot.lvol.get_id())
        return
    src_node = (_transfer_source_node(task, snapshot)
                or db.get_storage_node_by_id(snapshot.lvol.node_id))
    # Decide and detach under the hub's lock: a start on another worker
    # attaches and goes RUNNING under the same lock (see _hub_lock).
    with _hub_lock(src_node.get_id(), hub_node_id):
        if _other_active_transfers_to_node(task, hub_node_id):
            return
        xfer_timing.stamp("hub_detach", snap=snapshot.get_id(), lvol=snapshot.lvol.get_id())
        # Non-fatal: a resumed finish finds the hub already detached by the first attempt.
        try:
            src_node.rpc_client().bdev_nvme_detach_controller(hub_ctrl)
        except Exception as e:                            # noqa: BLE001
            logger.warning("Transfer hub detach on %s failed (non-fatal): %s",
                           src_node.get_id(), e)


def _source_volume(snapshot):
    """The snapshot's embedded volume record, sent with the copy so a fail-over
    on the copy's side can clone it with the source cut off; None when the
    snapshot embeds none. The host keys go as secrets, outside the record."""
    lvol = getattr(snapshot, "lvol", None)
    if lvol is None:
        return None
    record = lvol.to_dict()
    hosts = record.pop("allowed_hosts", None) or []
    return SourceVolume(record=record, allowed_hosts=[SourceHostAccess(**h) for h in hosts])


def _source_group(snapshot):
    """The group of a group snapshot, with the source volumes of its generation;
    None for a snapshot outside a group, or when the group record is gone: the
    copy then carries no member list, which the other side takes as a
    generation it cannot prove complete, never as a complete one."""
    seq = getattr(snapshot, "group_seq", 0) or 0
    group_id = getattr(snapshot, "group_id", "") or ""
    if not seq or not group_id:
        return None
    try:
        group = db.get_consistency_group_by_id(group_id)
    except KeyError:
        logger.warning("Snapshot %s names group %s, which is gone; its copy carries no "
                       "member list", snapshot.get_id(), group_id)
        return None
    return SourceGroup(
        group_name=group.group_name, policy_id=group.policy_id or "",
        members=sorted(lid for lid in (group.members or {}) if group.included_in_seq(lid, seq)))


def _merge_backoff(fails):
    """Seconds to wait before relaunching a merge that failed *fails* times in a row."""
    return min(constants.REPL_MERGE_RETRY_MIN_SEC * (2 ** max(fails - 1, 0)),
               constants.REPL_MERGE_RETRY_MAX_SEC)


def _merge_backing_off(task, now=None):
    """True while the finish waits out the back-off after a failed merge of the
    landing volume with its base (_merge_failed): no relaunch before then."""
    return (time.time() if now is None else now) < (task.function_params.get("merge_retry_at") or 0)


def _merge_done(task, landing_id):
    """Record the landing volume as merged with its base, and end any back-off."""
    task.function_params["merged_landing"] = landing_id
    for key in ("merge_fails", "merge_retry_at", "merge_alerted_at"):
        task.function_params.pop(key, None)


def _merge_failed(task, remote_lv, node_id, st):
    """Record a failed landing merge on the destination member *node_id*: back
    off before the next finish relaunches it, and raise a REPLICATION_STALLED
    event once it keeps failing (rate-limited)."""
    params = task.function_params
    now = time.time()
    fails = int(params.get("merge_fails") or 0) + 1
    wait = _merge_backoff(fails)
    params["merge_fails"] = fails
    params["merge_retry_at"] = now + wait
    logger.error("Replication task %s: merge of %s with its base failed on %s (%d in a row, "
                 "retry in %.0fs): %s", task.uuid, remote_lv.top_bdev, node_id,
                 fails, wait, st)
    xfer_timing.stamp("landing_merge_failed", lvol=remote_lv.get_id(), rc=st.get("rc"),
                      fails=fails, retry_in=round(wait))
    alerted = params.get("merge_alerted_at") or 0
    if fails >= constants.REPL_MERGE_FAIL_ALERT and now - alerted >= constants.REPL_MERGE_ALERT_EVERY_SEC:
        params["merge_alerted_at"] = now
        try:
            src = db.get_snapshot_by_id(params["snapshot_id"]).lvol
        except Exception:                                 # noqa: BLE001
            src = remote_lv
        try:
            lvol_events.replication_merge_failing(src, node_id, st.get("rc"), fails, wait)
        except Exception as e:                            # noqa: BLE001
            logger.error("Replication task %s: could not log the stalled-merge event: %s",
                         task.uuid, e)
    task.write_to_db()


def process_snap_replicate_finish(task, snapshot):
    """After the transfer: the destination makes the landing volume the copy of
    *snapshot* (finish: chain, merge, convert, record, retire); this cluster
    closes the transfer session, links its snapshot to the copy and runs
    retention. Returns the copy's id, FINISH_PENDING, FINISH_ABANDONED, or
    False (retried with backoff).
    """
    replicate_to_source = task.function_params["replicate_to_source"]
    as_instance = task.function_params.get("replicate_as_snap_instance", False)
    landing_id = task.function_params["remote_lvol_id"]
    sent_to = task.function_params.get("target_node_id") or ""

    # A transfer that ran while its predecessor was merged is chained onto the
    # predecessor's copy, which must exist first (_deferred_chain_gate).
    gate = _deferred_chain_gate(task, snapshot, replicate_to_source)
    if gate in ("pending", "restart"):
        _close_hub(task, snapshot, sent_to)
        if gate == "pending":
            task.function_params["pending_why"] = "waiting for the previous snapshot's copy"
            return FINISH_PENDING
        _discard_landing_for_retry(
            task, snapshot, _landing_of(task),
            f"the copy {task.function_params.get('chain_deferred_to')} this delta builds on "
            f"will not be made; discarded, retrying")
        return FINISH_ABANDONED

    # The copy is chained onto the predecessor's copy. This cluster picks the
    # predecessor by volume and age (snap_ref_id is never populated on internal
    # snapshots: chain_attempts=0 in every run of labs 2026-08-10..14); the
    # destination refuses to finish when that copy cannot be used.
    # Backing off after a failed merge of the landing volume with its base
    # (_merge_failed): no relaunch before merge_retry_at.
    if _merge_backing_off(task):
        _close_hub(task, snapshot, sent_to)
        task.function_params["pending_why"] = ("backing off after a failed merge of the "
                                               "landing volume with its base")
        return FINISH_PENDING

    predecessor_id = _predecessor_copy_id(snapshot, replicate_to_source)
    chained = _prechained_nodes_for(task, landing_id)
    answer = _task_peer(task, snapshot).finish(FinishRequest(
        source_cluster_id=snapshot.cluster_id, landing_lvol_id=landing_id,
        snapshot=SourceSnapshot(
            snapshot_id=snapshot.get_id(), snap_name=snapshot.snap_name, size=snapshot.size,
            used_size=snapshot.used_size or 0, data_uuid=snapshot.data_uuid or "",
            group_id=getattr(snapshot, "group_id", "") or "",
            group_seq=getattr(snapshot, "group_seq", 0) or 0,
            volume=_source_volume(snapshot), group=_source_group(snapshot)),
        sent_to=sent_to, predecessor_id=predecessor_id,
        partial_base=int(task.function_params.get("partial_base") or 0),
        chained_on=chained,
        converted_on=list(task.function_params.get("converted_nodes") or []),
        merged=task.function_params.get("merged_landing") == landing_id,
        cow_landing=_cow_landing_applied(task, landing_id),
        link="none" if as_instance else ("target" if replicate_to_source else "source")))

    # What the destination did, so a later pass skips it (add_clone and
    # convert are not idempotent in SPDK).
    task.function_params["converted_nodes"] = answer.converted_on
    task.function_params["prechained_node_ids"] = answer.chained_on
    if answer.merged:
        _merge_done(task, landing_id)
    elif answer.merge_failed:
        _merge_failed(task, _landing_of(task), answer.leader_node_id,
                      {"state": "failed", "rc": answer.merge_rc})
    task.write_to_db()

    if answer.state == "pending" and answer.waiting_for_leader:
        # The target is down, or its leadership moving: not the transfer's
        # fault, so not a failure; every pass asks again.
        logger.warning("Replication task %s: not finishing %s yet -- %s", task.uuid,
                       snapshot.get_id(), answer.reason)
        task.function_params["pending_why"] = answer.reason
        return FINISH_PENDING
    # The hub session belongs to the member the transfer was SENT to.
    _close_hub(task, snapshot, sent_to or answer.leader_node_id)
    if answer.state == "pending":
        task.function_params["pending_why"] = answer.reason
        return FINISH_PENDING
    if answer.state == "restart":
        _discard_landing_for_retry(task, snapshot, _landing_of(task),
                                   f"{answer.reason}; discarded, retrying")
        return FINISH_ABANDONED
    if answer.state == "gone":
        # Nothing to finish: start the transfer over on a fresh landing volume.
        logger.warning("Replication task %s: %s; starting the transfer over",
                       task.uuid, answer.reason)
        for key in ("transfer_done", "converted_nodes", "remote_lvol_id", "offset",
                    "merged_landing", "chain_deferred_to"):
            task.function_params.pop(key, None)
        _suspend_for_retry(task, f"{answer.reason}; starting over", backoff=False)
        return FINISH_ABANDONED
    if answer.state != "done":
        logger.error("Replication task %s: finish of %s failed: %s", task.uuid,
                     snapshot.get_id(), answer.reason)
        return False

    # Link this snapshot to its copy. The copy's record is kept on it as an
    # instance, as before; a finish repeated after a lost answer adds it once.
    if all(_instance_id(i) != answer.copy_id for i in (snapshot.instances or [])):
        snapshot.instances.append(answer.record)
    if not as_instance:
        if replicate_to_source:
            snapshot.source_replicated_snap_uuid = answer.copy_id
        else:
            snapshot.target_replicated_snap_uuid = answer.copy_id
    if snapshot.status == SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_ONLINE
    snapshot.write_to_db()
    snapshot_events.replication_task_finished(snapshot)
    prune_async(snapshot.lvol)
    return answer.copy_id


def _instance_id(instance):
    """The id of an entry of SnapShot.instances: a SnapShot or its dict form."""
    if isinstance(instance, dict):
        return instance.get("uuid", "")
    return getattr(instance, "uuid", "")


def task_runner(task: JobSchedule):
    # get_snapshot_by_id raises for a snapshot that is gone; it never returns
    # None. Without the catch the runner failed on every attempt and the task
    # stayed open for good, where replication_stop tripped over it.
    try:
        snapshot = db.get_snapshot_by_id(task.function_params["snapshot_id"])
    except KeyError:
        snapshot = None
    if not snapshot:
        task.function_result = "snapshot not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if (task.status == JobSchedule.STATUS_SUSPENDED and not task.canceled
            and int(time.time()) < int(task.function_params.get("not_before") or 0)):
        # waiting out a backoff is not a failure: no penalty sleep in main()
        return True

    try:
        db.get_storage_node_by_id(snapshot.lvol.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    # Which member drives the transfer is decided per attempt by
    # _select_source_node (any online member holding the snapshot, primary
    # first); a running transfer is polled where it was started.

    if task.retry >= task.max_retry or task.canceled is True:
        # Carry the last real reason: "max retry reached" alone names none.
        last = task.function_params.get("last_error")
        task.function_result = (f"max retry reached ({task.retry}/{task.max_retry}) after: {last}"
                                if last else "max retry reached")
        if task.canceled is True:
            task.function_result = "task cancelled"
        logger.error("Replication task %s (snapshot %s) gave up: %s",
                     task.uuid, snapshot.get_id(), task.function_result)

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
            remote_lv = _landing_of(task)
        except KeyError:
            return True
        # abort path: close the transfer session here too (last user only),
        # on the member that attached it
        try:
            _rl_node = db.get_storage_node_by_id(
                task.function_params.get("target_node_id") or remote_lv.node_id)
            snode = _transfer_source_node(task, snapshot)
            _ctrl = _attached_hub_ctrl(task, _rl_node)
            if (snode is not None and _ctrl
                    and not _other_active_transfers_to_node(task, _rl_node.get_id())):
                snode.rpc_client().bdev_nvme_detach_controller(_ctrl)
        except Exception as e:
            logger.warning("Abort-path hub detach failed (non-fatal): %s", e)
        try:
            _landing_peer(snapshot, remote_lv).discard_landing(remote_lv.get_id())
        except Exception as e:
            logger.warning("Abort-path cleanup of %s failed (non-fatal): %s",
                           remote_lv_id, e)

        return True


    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        if not _resume_finish(task, snapshot):
            process_snap_replicate_start(task, snapshot)

    elif task.status == JobSchedule.STATUS_RUNNING:
        # Poll the member the transfer runs on. If that member went down or
        # cannot be asked, the transfer is lost with it: start the snapshot
        # over on the next online member (see _select_source_node).
        src_id = task.function_params.get("source_node_id") or snapshot.lvol.node_id
        try:
            snode = db.get_storage_node_by_id(src_id)
        except KeyError:
            snode = None
        if snode is None or snode.status != StorageNode.STATUS_ONLINE:
            _restart_elsewhere(task, f"source member {src_id} is "
                                     f"{snode.status if snode else 'gone'} mid-transfer")
            return False
        try:
            ret = snode.rpc_client().bdev_lvol_transfer_stat(snapshot.snap_bdev)
        except Exception as e:                            # noqa: BLE001
            _restart_elsewhere(task, f"source member {src_id} cannot be asked "
                                     f"about the transfer ({e})")
            return False
        if ret:
            # offset is the bytes moved so far: the ONLY direct read on actual
            # transfer throughput, as distinct from round duration.
            xfer_timing.stamp("transfer_running", snap=snapshot.get_id(),
                              lvol=snapshot.lvol.get_id(),
                              state=str(ret.get("transfer_state")).replace(" ", "_"),
                              offset=ret.get("offset"))
        if not ret:
            logger.error("Failed to get transfer stat")
            return False
        status = ret["transfer_state"]
        offset = ret["offset"]
        if status == "No process":
            _suspend_for_retry(task, f"Status: {status}, offset:{offset}, retrying")
            return False
        if status == "In progress":
            task.function_result = f"Status: {status}, offset:{offset}"
            task.function_params["offset"] = offset
            task.write_to_db()
            return True
        if status == "Failed":
            moved = _target_leadership_moved(task, snapshot)
            if moved:
                # Not the transfer's fault: the target's leadership moved
                # (primary -> secondary, secondary -> tertiary) under it.
                # Restart on the new leader once it is settled.
                _restart_elsewhere(task, f"transfer failed at offset {offset}: {moved}")
                return False
            _suspend_for_retry(task, f"transfer failed at offset {offset}, retrying",
                               backoff=True)
            return False
        if status == "Done":
            return _finish_completed_transfer(task, snapshot, offset)


def _replication_in_flight(tasks):
    """Whether a transfer or a landing merge is running: then a finished one
    must be noticed within REPL_ACTIVE_PASS_SEC, not TASK_EXEC_INTERVAL_SEC."""
    return any(t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
               and t.status == JobSchedule.STATUS_RUNNING and not t.canceled
               for t in tasks)


def _converging_cutover(t):
    """True for a final-cutover task between taking the lvstore and its
    freeze: its shrink snapshots must start transferring the moment they
    exist."""
    if t.function_name != JobSchedule.FN_REPLICATION_FINAL:
        return False
    if t.status == JobSchedule.STATUS_DONE or t.canceled:
        return False
    params = t.function_params or {}
    if not params.get("cutover_lvs"):
        return False
    # The cutover clone is built once the rounds are over -- unless it was
    # built early (REPL_CUTOVER_EARLY_PREPARE), in which case rounds run
    # until the freeze, the preconnect wait included.
    if params.get("tgt_lvol_composite") and not params.get("early_prepared"):
        return False
    return True


class _EndgameWake:
    """Starts a cutover's shrink-snapshot transfer as soon as it is taken.

    A shrink snapshot's replication task used to wait for this runner's next
    pass -- TASK_EXEC_INTERVAL_SEC (10 s) when nothing else was transferring,
    which is every round of a converging cutover (bed bf12 2026-11-03: ~12 s
    of every ~21 s round). The cutover runner runs in another process, so the
    hand-over goes through the database: while a cutover converges, this
    runner re-reads THAT cutover's task row (a point read, one per converging
    cutover -- at most the concurrent migration slots) every
    REPL_ENDGAME_WAKE_POLL_SEC, and a new shrink_snap_id on it submits the
    snapshot's replication task to the worker pool at once (_submit): a
    worker busy with another volume's task no longer delays it. The task
    table itself is still scanned only per pass, or when a wake fires.
    """

    def __init__(self):
        self._watch = {}            # final task id -> (cluster id, shrink_snap_id seen)

    def refresh(self, cluster_id, tasks):
        """Re-arm from a full read of *cluster_id*'s tasks; returns the
        shrink snapshots in flight there (the pass dispatches those first)."""
        for tid in [k for k, (cid, _) in self._watch.items() if cid == cluster_id]:
            del self._watch[tid]
        snaps = set()
        for t in tasks:
            if _converging_cutover(t):
                snap = (t.function_params or {}).get("shrink_snap_id") or ""
                self._watch[t.uuid] = (cluster_id, snap)
                if snap:
                    snaps.add(snap)
        return snaps

    def watching(self):
        return bool(self._watch)

    def poll(self):
        """{cluster id: shrink snapshots taken since they were last seen}."""
        fresh: dict = {}
        for tid, (cid, seen) in list(self._watch.items()):
            try:
                t = db.get_task_by_id(tid)
            except Exception:                             # noqa: BLE001
                self._watch.pop(tid, None)
                continue
            if not _converging_cutover(t):
                self._watch.pop(tid, None)
                continue
            snap = (t.function_params or {}).get("shrink_snap_id") or ""
            if snap and snap != seen:
                self._watch[tid] = (cid, snap)
                fresh.setdefault(cid, set()).add(snap)
        return fresh


def _endgame_first(tasks, snap_ids):
    """The pass in creation order, a converging cutover's shrink snapshot
    first: it is the one whose every second of waiting lands in a freeze."""
    if not snap_ids:
        return list(tasks)
    first = [t for t in tasks
             if t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
             and (t.function_params or {}).get("snapshot_id") in snap_ids]
    return first + [t for t in tasks if t not in first]


def _check_endgame_wake(wake):
    """Submit the replication tasks of shrink snapshots taken since the last
    look."""
    if wake is None or not wake.watching():
        return
    fresh = wake.poll()
    if not fresh:
        return
    for cluster_id, snap_ids in fresh.items():
        try:
            tasks = db.get_job_tasks(cluster_id, reverse=False)
        except Exception as e:                            # noqa: BLE001
            logger.warning("Cannot read tasks to start shrink snapshot(s) %s: %s",
                           ", ".join(sorted(snap_ids)), e)
            continue
        for t in tasks:
            if (t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
                    and t.status != JobSchedule.STATUS_DONE
                    and (t.function_params or {}).get("snapshot_id") in snap_ids):
                xfer_timing.stamp("endgame_wake",
                                  snap=t.function_params.get("snapshot_id"))
                _submit(t)


def _wait(seconds, wake):
    """Sleep *seconds*, serving converging cutovers' new shrink snapshots
    meanwhile (see _EndgameWake). A plain sleep while none converges."""
    end = time.time() + seconds
    while True:
        remaining = end - time.time()
        if remaining <= 0:
            return
        if wake is None or not wake.watching():
            time.sleep(remaining)
            return
        time.sleep(min(remaining, constants.REPL_ENDGAME_WAKE_POLL_SEC))
        _check_endgame_wake(wake)


def _landing_busy_here(task):
    """Why this cluster cannot call an in_creation landing volume of *task*'s
    snapshot abandoned: another live task of the snapshot may be creating it
    (bed bf32). Empty when none is live. Sent with the landing request
    (LandingRequest.source_busy); the destination adds what it knows."""
    try:
        siblings = _live_siblings(task)
    except Exception as e:                                # noqa: BLE001
        return f"the task table cannot be read ({e})"
    return f"task {siblings[0].uuid} of the same snapshot is live" if siblings else ""


def _live_siblings(task):
    """The other live (not done, not canceled) replication tasks of *task*'s
    snapshot, through the repl_snapshot_id index (a point read, not a scan
    of the never-pruned task table)."""
    snapshot_id = (task.function_params or {}).get("snapshot_id")
    if not snapshot_id:
        return []
    return [t for t in db.get_replication_tasks_for_snapshot(snapshot_id)
            if t.uuid != task.uuid
            and (not task.cluster_id or t.cluster_id == task.cluster_id)
            and t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
            and t.status != JobSchedule.STATUS_DONE and t.canceled is False]


def _task_precedence(t):
    """Sort key: which of several live tasks of one snapshot may run. A
    transfer already running first, then one that owns a landing volume,
    then the oldest; the uuid breaks ties, so every worker picks the same."""
    params = t.function_params or {}
    return (t.status != JobSchedule.STATUS_RUNNING,
            not params.get("remote_lvol_id"),
            int(t.date or 0), t.uuid)


def _yields_to_sibling(task):
    """The task of the same snapshot *task* must wait for, or None.

    One snapshot has one replication task (_add_task claims it atomically),
    but a duplicate written before that claim existed -- or by any other
    path -- must not run beside it: two runs of one snapshot each create and
    delete the same REP_* landing volume (bed bf32 2026-11-21). Of the live
    tasks of a snapshot exactly one runs, by _task_precedence; the others
    wait until it is done, then find the snapshot replicated."""
    siblings = _live_siblings(task)
    if not siblings:
        return None
    first = min(siblings + [task], key=_task_precedence)
    return None if first.uuid == task.uuid else first


def _dispatch(task):
    """Run one replication task the way a pass does; task_runner's result
    (False on a failure, which the caller follows with a short pause)."""
    if task.status != JobSchedule.STATUS_DONE and not task.canceled:
        try:
            first = _yields_to_sibling(task)
        except Exception as e:                            # noqa: BLE001
            # Unknown is not "alone": run it on a later pass.
            logger.warning("Cannot read the tasks of snapshot %s: %s",
                           task.function_params.get("snapshot_id"), e)
            return False
        if first is not None:
            logger.info("Replication task %s waits: task %s of the same snapshot %s "
                        "runs first", task.uuid, first.uuid,
                        task.function_params.get("snapshot_id"))
            return True
    if task.status == JobSchedule.STATUS_DONE:
        return True
    # Re-read the task in case cancel changed it. If it has
    # since vanished -- retention, a concurrent cleanup, or a
    # stale index entry a repair has yet to clear -- skip it.
    # One missing task must never take the runner down with
    # it: this KeyError used to propagate out of main() and
    # stop replication for every cluster, then crash the
    # restarted container on the same entry (live 2026-09-28).
    try:
        task = db.get_task_by_id(task.uuid)
    except KeyError:
        logger.warning("Replication task %s vanished before "
                       "dispatch; skipping", task.uuid)
        return True
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
        # Spend a retry: a raise used to leave the task
        # as it was, so the same failure re-ran on every
        # pass and max_retry was never reached.
        try:
            task = db.get_task_by_id(task.uuid)
            if task.status != JobSchedule.STATUS_DONE and not task.canceled:
                _suspend_for_retry(task, f"task raised: {e}", backoff=True)
        except Exception as e2:
            logger.error("Could not record retry for task %s: %s",
                         task.get_id(), e2)
    return res


class _Workers:
    """The pool replication tasks run on.

    One runner used to execute every replication task of every volume of
    every cluster one after the other, and a task holds it for its whole
    transfer (a cutover round waits inline up to
    REPL_XFER_INLINE_WAIT_CUTOVER_SEC) and for every slow metadata RPC on the
    target. On bed bf13 (2026-11-05, 6 volumes under heavy IO) that gave four
    replicating volumes 1.7-way concurrency, stretched their 1-minute cadence
    to 127-157 s with 6-9 GB deltas, and made a converging cutover wait 22-34 s
    for its own round to start while another volume's landing volume was
    created. Here each task runs on one of REPL_RUNNER_WORKERS threads.

    Rules: a task is on at most one worker at a time (a second submit while it
    runs is a no-op); a task whose run returned a failure is not resubmitted
    for REPL_RUNNER_RETRY_PAUSE_SEC; with no workers (size 0) a submit runs
    the task inline and returns its result, the way the pass loop always did.
    The per-lvstore ordering rules (_lvs_transfer_hold, migration_turn) are
    decided from the task table by each task itself, as before; what the
    workers share is guarded: the settled-leader cache, and the transfer hub
    sessions through _hub_lock.
    """

    def __init__(self, size):
        self.size = max(0, int(size))
        self._pool = (ThreadPoolExecutor(max_workers=self.size, thread_name_prefix="repl-worker")
                      if self.size else None)
        self._guard = threading.Lock()
        self._inflight: dict = {}       # task uuid -> Future
        self._inflight_snaps: dict = {}  # snapshot id -> task uuid running it
        self._not_before: dict = {}     # task uuid -> earliest next submit

    def paused(self, task):
        """A task whose last run failed waits REPL_RUNNER_RETRY_PAUSE_SEC."""
        with self._guard:
            return time.time() < self._not_before.get(task.uuid, 0.0)

    def running(self):
        with self._guard:
            return len(self._inflight)

    def submit(self, task):
        """Run *task* on a worker, unless it is running already or paused after
        a failure. Inline when the pool has no workers: returns the task's
        result then, True otherwise."""
        if self._pool is None:
            return _dispatch(task)
        snap_id = (task.function_params or {}).get("snapshot_id") or ""
        with self._guard:
            if task.uuid in self._inflight:
                return True
            # One snapshot on one worker: a second task of the same snapshot
            # never runs beside the first (bed bf32 2026-11-21).
            if snap_id and self._inflight_snaps.get(snap_id, task.uuid) != task.uuid:
                return True
            if time.time() < self._not_before.get(task.uuid, 0.0):
                return True
            if snap_id:
                self._inflight_snaps[snap_id] = task.uuid
            self._inflight[task.uuid] = self._pool.submit(self._run, task)
        return True

    def _run(self, task):
        res = False
        try:
            res = _dispatch(task)
        except Exception as e:                            # noqa: BLE001
            # _dispatch guards the task's own failures; this is a runner bug
            logger.error("Replication worker failed on task %s: %s", task.uuid, e)
        finally:
            with self._guard:
                self._inflight.pop(task.uuid, None)
                snap_id = (task.function_params or {}).get("snapshot_id") or ""
                if snap_id and self._inflight_snaps.get(snap_id) == task.uuid:
                    del self._inflight_snaps[snap_id]
                if not res:
                    self._not_before[task.uuid] = (
                        time.time() + constants.REPL_RUNNER_RETRY_PAUSE_SEC)
                # keep the pause table from growing with retired tasks
                if len(self._not_before) > 1024:
                    now = time.time()
                    for k in [k for k, v in self._not_before.items() if v < now]:
                        del self._not_before[k]
        return res


def _submit(task):
    """Run *task* on the pool (inline before main() built one)."""
    if _WORKERS is None:
        return _dispatch(task)
    return _WORKERS.submit(task)


def main():
    global _WORKERS
    _WORKERS = _Workers(constants.REPL_RUNNER_WORKERS)
    logger.info("Starting Tasks runner (%d workers)...", _WORKERS.size)
    wake = _EndgameWake()
    while True:
        control_plane_controller.wait_until_services_enabled("snapshot-replication")
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        active = False
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                active = active or _replication_in_flight(tasks)
                endgame_snaps = wake.refresh(cl.get_id(), tasks)
                for task in _endgame_first(tasks, endgame_snaps):
                    if task.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
                        continue
                    if task.status == JobSchedule.STATUS_DONE:
                        continue
                    if _WORKERS.paused(task):
                        continue
                    res = _WORKERS.submit(task)
                    if _WORKERS.size == 0 and not res:
                        _wait(constants.REPL_RUNNER_RETRY_PAUSE_SEC, wake)
                    # A round finished in this dispatch lets its cutover take
                    # the next snapshot: start that one now, not after the
                    # rest of the pass.
                    _check_endgame_wake(wake)

        _wait(constants.REPL_ACTIVE_PASS_SEC if active or _WORKERS.running()
              else constants.TASK_EXEC_INTERVAL_SEC, wake)


if __name__ == "__main__":
    main()
