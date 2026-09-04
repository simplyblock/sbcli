import builtins
import contextlib
import logging as lg
import math
import os
import socket
import threading
import time
import uuid
from datetime import datetime

from simplyblock_core.controllers import ops_gate
from simplyblock_core.controllers import lvol_controller, snapshot_events, pool_controller, tasks_controller, \
    migration_controller

from simplyblock_core import constants, utils
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.kms import create_kms_connection, lvol_dek_path, pool_kek_name
from simplyblock_core.kms._exceptions import KMSException
from simplyblock_core.db_controller import DBController, SubsystemCapacityError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


logger = lg.getLogger()

db_controller = DBController()


def _wait_for_node_sync_delete(node, timeout=None, poll=0.5):
    """Block until any in-flight LVol sync-deletion on this node's HA peers
    drains, then let the create proceed — instead of rejecting it.

    The node-level ``lvol_del_sync_lock`` is set by the lvol/snapshot delete
    monitors when an object is deleted on the primary while a secondary/tertiary
    is down: that secondary's delete is deferred to an FN_LVOL_SYNC_DEL task, and
    until it drains a new create/register on the same node could race the pending
    delete in the replica blob tree. Previously the create was rejected (HTTP
    400); now it waits for the deferred delete to finish.

    Returns True once clear, or False if ``timeout`` seconds elapse (caller fails
    the create so the client retries). Bounded by LVOL_SYNC_DELETE_WAIT_SEC; this
    plus the later per-lvstore lock wait must stay under the front-end API timeout
    (HAProxy ``timeout server``) so the lock always times out before the API cuts
    the connection — see the budget note in constants.py. ``node.lvol_sync_del()``
    reads the lock fresh from the DB each call, so re-fetching the node is
    unnecessary."""
    if timeout is None:
        timeout = constants.LVOL_SYNC_DELETE_WAIT_SEC
    deadline = time.time() + timeout
    while node.lvol_sync_del():
        if time.time() >= deadline:
            return False
        time.sleep(poll)
    return True


def _new_lvstore_lock_owner():
    """Unique owner id for one critical section: host + pid + thread + nonce, so
    a stale lock is never mistaken for a live holder's and an owner-scoped
    release only ever frees this section's own lock."""
    return f"{socket.gethostname()}-{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex[:8]}"


def _acquire_lvstore_lock_blocking(db_controller, cluster_id, lvs_name, owner,
                                   timeout=None, poll=0.5):
    """Block until the per-lvstore snapshot-mutation lock is held by ``owner``.

    Returns True once acquired, or False if ``timeout`` seconds elapse without
    acquiring (the caller should fail the create so it is retried). A lock
    abandoned by a crashed holder is reclaimed once its heartbeat goes stale
    (constants.LVSTORE_MUTATION_LOCK_TTL_SEC), so the wait is bounded even if a
    holder died mid-section."""
    if timeout is None:
        timeout = constants.LVSTORE_MUTATION_LOCK_WAIT_SEC
    deadline = time.time() + timeout
    while True:
        won, current_owner = db_controller.acquire_lvstore_lock(cluster_id, lvs_name, owner)
        if won:
            return True
        if time.time() >= deadline:
            logger.error("Timed out waiting for lvstore lock %s (held by %s)",
                         lvs_name, current_owner)
            return False
        # Wait on an FDB watch of the lock key instead of sleeping a blind
        # poll interval: the waiter wakes the moment the holder releases (the
        # watch also fires on heartbeats, which just re-attempts early). The
        # 2s failsafe re-attempts even without a watch event so a missed
        # event or a stale-TTL reclaim window is never waited out in full.
        # ``is_ready()`` is a local check — no FDB round-trip in the spin.
        watch = None
        try:
            watch = db_controller.watch_lvstore_lock(cluster_id, lvs_name)
        except Exception:
            watch = None
        if watch is None:
            time.sleep(poll)
            continue
        wait_until = min(time.time() + 2.0, deadline)
        try:
            while time.time() < wait_until and not watch.is_ready():
                time.sleep(0.02)
        finally:
            try:
                watch.cancel()
            except Exception:
                pass


def _lvstore_lock_heartbeat(db_controller, cluster_id, lvs_name, owner, stop_event):
    """Refresh the lvstore lock until ``stop_event`` is set, so a slow
    create→register section (register RPCs can take many seconds under load) is
    not reclaimed out from under a live holder."""
    while not stop_event.wait(constants.LVSTORE_MUTATION_LOCK_HEARTBEAT_SEC):
        if not db_controller.refresh_lvstore_lock(cluster_id, lvs_name, owner):
            logger.warning("Lost lvstore lock %s heartbeat (reclaimed)", lvs_name)
            return


@contextlib.contextmanager
def lvstore_op_lock(cluster_id, lvs_name, *, node_id=None, enabled=True,
                    best_effort=False, timeout=None):
    """INNER lock — serialize a SINGLE single-node data-plane operation per
    lvstore (one RPC to one node).

    A "single operation" is an operation on one node only: one snapshot/clone
    create on the primary, one replica register on a secondary, one delete on
    the leader, one sync-delete on a non-leader, one resize on a node. This
    lock is acquired and released around each such op individually — it is
    NOT held across the whole multi-node sequence. It guarantees no two object
    operations (create/delete/resize of any lvol/snapshot/clone) mutate the
    lvstore on a node at the same time, so replica blob-tree mutations never
    interleave and corrupt the tree.

    ``node_id`` scopes the lock to the node the RPC targets. The invariant is
    per-node ("mutate the lvstore ON A NODE"), so ops of DIFFERENT objects may
    proceed on the primary and a secondary of the same lvstore concurrently —
    keying on lvs_name alone serialized them cluster-wide and was the dominant
    queueing cost of mass snapshot creation. Callers that cannot name the
    target node fall back to the whole-lvstore key (strictly stronger).

    Chain (blobid) ordering across operations on the *same* object is provided
    by the OUTER per-object lock (``object_mutation_lock``), not by this one.
    """
    if not enabled or not cluster_id:
        yield
        return
    lock_key = f"{lvs_name}@{node_id[:8]}" if node_id else lvs_name
    owner = _new_lvstore_lock_owner()
    if not _acquire_lvstore_lock_blocking(db_controller, cluster_id, lock_key,
                                          owner, timeout=timeout):
        if best_effort:
            # Recovery/cleanup (force delete): the holder may be a process
            # that died mid-section on a node that is now gone, so blocking
            # forever is not an option -- but neither is the old behaviour
            # of skipping the lock entirely, which let a forced delete
            # interleave with any create/delete/resize on the same lvstore.
            # Wait the bounded time, then proceed and say so loudly.
            logger.warning(
                "Proceeding WITHOUT the lvstore lock on %s (force/recovery "
                "path); a concurrent object operation on this node is "
                "possible", lock_key)
            yield
            return
        raise PreconditionError(
            f"Timed out acquiring lvstore lock on {lock_key}")
    stop = threading.Event()
    threading.Thread(
        target=_lvstore_lock_heartbeat,
        args=(db_controller, cluster_id, lock_key, owner, stop),
        daemon=True).start()
    try:
        yield
    finally:
        stop.set()
        db_controller.release_lvstore_lock(cluster_id, lock_key, owner)


def _find_lvs_leader(cluster_id, lvs_name, all_nodes):
    """Return the confirmed leader among ``all_nodes`` for ``lvs_name``, or None.

    Cached fast path (same contract as
    storage_node_ops.find_leader_with_failover): probe only the
    recently-confirmed leader — the probe is itself a fresh confirmation, so a
    moved leadership simply misses and falls back to scanning every candidate.
    Replaces the per-create full scan, which paid one bdev_lvol_get_lvstores
    RPC per candidate node on every snapshot/clone.

    No-leader fail-fast: when the LVS was recently confirmed leaderless,
    return None without probing at all — callers must reject the operation
    until a leader is re-established. When the scan itself comes up empty,
    delegate to find_leader_with_failover, which owns the leaderless-LVS
    recovery (take-leadership on the configured primary) and the shared
    negative cache, so at most one recovery pass runs per NO_LEADER_TTL_SEC
    even under a snapshot/clone-only workload."""
    from simplyblock_core.controllers import lvol_controller
    from simplyblock_core.utils.ttl_cache import (
        leader_cache, LEADER_TTL_SEC, no_leader_cache, NO_LEADER_TTL_SEC)

    key = (cluster_id, lvs_name)
    if no_leader_cache.get(key, NO_LEADER_TTL_SEC):
        logger.warning(
            "LVS %s was confirmed leaderless less than %ss ago — failing fast "
            "without re-probing", lvs_name, NO_LEADER_TTL_SEC)
        return None
    cached_id = leader_cache.get(key, LEADER_TTL_SEC)
    if cached_id:
        cached_node = next((n for n in all_nodes if n.get_id() == cached_id), None)
        if cached_node is not None:
            try:
                if lvol_controller.is_node_leader(cached_node, lvs_name):
                    leader_cache.put(key, cached_id)
                    return cached_node
            except Exception:
                pass
        leader_cache.invalidate(key)
    for candidate in all_nodes:
        try:
            if lvol_controller.is_node_leader(candidate, lvs_name):
                leader_cache.put(key, candidate.get_id())
                return candidate
        except Exception:
            continue
    # Nobody admits leadership — run the full failover/recovery helper once;
    # it records the no-leader verdict in the shared negative cache so every
    # call within NO_LEADER_TTL_SEC (including the fast path above) fails
    # instantly instead of re-probing.
    from simplyblock_core.storage_node_ops import find_leader_with_failover
    leader, _ = find_leader_with_failover(all_nodes, lvs_name)
    return leader


# Namespace prefix so a per-object lock key can never collide with a real
# lvs_name in the shared FDB lock table.
_OBJECT_LOCK_PREFIX = "__obj__"


# A peer that is not running owes nothing: its in-memory registration dies with
# the process and is never rebuilt, because the object's record is already gone
# from the DB by the time phase-2 runs. Only a LIVE peer (serving, or suspended
# but still up) has state that a sync delete must clear.
_PEER_ALIVE_STATUSES = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED)


def _successor_mid_replication(snap):
    """Whether *snap*'s chain successor is being replicated right now.

    Deleting a snapshot swap-merges its segments into the SUCCESSOR, i.e. it
    mutates the successor's cluster map. While the successor is replicating,
    bdev_lvol_transfer is iterating exactly that map, so the merge would tear
    the copy mid-flight — silently: the transfer completes and the remote image
    is simply missing the merged clusters. Callers run under the chain-root
    lock (delete holds object_mutation_lock; the transfer path flips the
    successor to IN_REPLICATION under the same lock), so there is no window
    between this check and the merge.
    """
    if not snap.next_snap_uuid:
        return False
    try:
        successor = db_controller.get_snapshot_by_id(snap.next_snap_uuid)
    except KeyError:
        return False
    return successor.status == SnapShot.STATUS_IN_REPLICATION


def delete_bdev_absent_ok(node, bdev_name, sync=False, special_delete=False):
    """``delete_lvol`` that treats "already gone" as done. Returns True on success.

    A delete is a statement about the desired end state, and ``-19 / No such
    device`` says that state already holds — the bdev is not there. Reading it
    as a failure makes the delete non-idempotent, and every caller that retries
    then retries for ever: the record stays ``in_deletion``, the monitor picks
    it up again next cycle, and nothing ever converges.

    Lab 2026-08-20: ~10,300 "Failed to delete snap from node" in 2.5 hours,
    every one of them a ``-19`` for a bdev the async phase had already removed
    (the poll one line earlier had returned status 0, "deleted successfully").
    ``sync_delete_on_peer`` already tolerated this on the peer path; the
    leader/primary paths did not, so the two halves of the same delete
    disagreed about what had happened.
    """
    try:
        ret, err = node.rpc_client().delete_lvol(
            bdev_name, sync=sync, special_delete=special_delete)
    except Exception as e:
        ret, err = False, {"message": str(e)}
    if ret:
        return True
    if isinstance(err, dict) and err.get("code") == -19:
        logger.info(f"Delete of {bdev_name} on {node.get_id()[:8]}: already absent")
        return True
    return False


def sync_delete_on_peer(peer_node, bdev_name, primary_node_id, special_delete=False):
    """Phase-2 sync delete on one peer. Returns True when nothing is owed.

    Attempt first, classify afterwards. Pre-judging by node status queues a
    durable task for every peer that is merely suspended, and those tasks then
    refuse to run *because* the node is suspended — a pile that also blocks the
    node's own shutdown (lab run 15 case 6: 46 queued sync-deletes, node stuck
    in `suspended`). A failure is only worth a retry task when the peer is
    still alive; when it is gone, the delete is already satisfied.
    """
    try:
        ret, err = peer_node.rpc_client().delete_lvol(
            bdev_name, sync=True, special_delete=special_delete)
    except Exception as e:
        ret, err = False, {"message": str(e)}
    if ret:
        return True
    if isinstance(err, dict) and err.get("code") == -19:
        logger.info(f"Sync delete of {bdev_name} on {peer_node.get_id()[:8]}: "
                    f"already absent")
        return True

    try:
        fresh = db_controller.get_storage_node_by_id(peer_node.get_id())
        status = fresh.status
    except Exception:
        status = peer_node.status
    if status not in _PEER_ALIVE_STATUSES:
        logger.info(
            f"Ignoring sync-delete failure for {bdev_name} on "
            f"{peer_node.get_id()[:8]}: node is {status}, its registration is "
            f"gone with the process and is not rebuilt")
        return True

    logger.error(f"Failed to sync delete bdev: {bdev_name} from node: "
                 f"{peer_node.get_id()} ({err}), adding task...")
    tasks_controller.add_lvol_sync_del_task(
        peer_node.cluster_id, peer_node.get_id(), bdev_name, primary_node_id)
    return False


_CHAIN_WALK_MAX_HOPS = 256


def resolve_chain_root(object_uuid):
    """Return ``(chain_root_uuid, lvs_name)`` for any lvol/snapshot uuid.

    A blob chain is the transitive closure of "is derived from": a volume, all
    snapshots taken of it, every clone made from those snapshots, that clone's
    snapshots, and so on. Deleting inside a chain rewrites its links (a delete
    swap-merges the snapshot's segments into its successor), so operations on
    ANY member mutate the same structure and must not interleave — while
    different chains are independent and may run fully in parallel.

    Walking upwards: a clone points at the snapshot it came from
    (``LVol.cloned_from_snap``), a snapshot points at the volume it was taken
    from (``SnapShot.lvol``). The walk ends at the base volume, whose uuid
    names the chain. Unknown uuids and broken links resolve to the uuid itself
    (its own one-member chain — never a shared key, so a corrupt record can
    only under-share the lock, never wrongly alias two real chains).
    """
    current = object_uuid
    lvs_name = ""
    for _ in range(_CHAIN_WALK_MAX_HOPS):
        obj = None
        try:
            obj = db_controller.get_lvol_by_id(current)
        except Exception:
            obj = None
        if obj is not None:
            lvs_name = getattr(obj, "lvs_name", "") or lvs_name
            parent = getattr(obj, "cloned_from_snap", "")
            if not parent:
                return current, lvs_name
            current = parent
            continue
        try:
            snap = db_controller.get_snapshot_by_id(current)
        except Exception:
            return current, lvs_name
        snap_lvol = getattr(snap, "lvol", None)
        lvs_name = getattr(snap_lvol, "lvs_name", "") or lvs_name
        parent_lvol_id = snap_lvol.get_id() if snap_lvol is not None else ""
        if not parent_lvol_id or parent_lvol_id == current:
            return current, lvs_name
        current = parent_lvol_id
    logger.warning("Chain walk for %s exceeded %d hops; using %s as chain root",
                   object_uuid, _CHAIN_WALK_MAX_HOPS, current)
    return current, lvs_name


@contextlib.contextmanager
def object_mutation_lock(cluster_id, object_uuid, *, enabled=True,
                         best_effort=False, timeout=None):
    """OUTER lock — serialize the WHOLE multi-node sequence of one operation on
    a CHAIN (a volume, its snapshots, their clones, recursively) and exclude
    any other operation (create / delete / resize / clone) on that same chain
    while it runs.

    Held across the entire controller action; the inner per-lvstore lock
    (``lvstore_op_lock``) is taken and released around each single-node RPC
    inside it. The scope is the chain, not the single object: a delete
    swap-merges segments into the neighbouring snapshot and re-links parents,
    so a concurrent create/clone/delete anywhere in the same chain mutates the
    structure the first operation is walking. Distinct chains never share blob
    links, so they proceed concurrently.

    Reuses the lvstore-lock primitive keyed on the CHAIN ROOT (namespaced via
    ``_OBJECT_LOCK_PREFIX`` so it never collides with a real lvs_name). The
    outer key and the inner lvs_name are different keys in the same lock table,
    always acquired outer-then-inner, so the two never deadlock.
    """
    if not enabled or not cluster_id or not object_uuid:
        yield
        return
    chain_root, chain_lvs = resolve_chain_root(object_uuid)
    key = f"{_OBJECT_LOCK_PREFIX}/{chain_lvs}:{chain_root}"
    owner = _new_lvstore_lock_owner()
    if not _acquire_lvstore_lock_blocking(db_controller, cluster_id, key, owner,
                                          timeout=timeout):
        if best_effort:
            logger.warning(
                "Proceeding WITHOUT the chain lock on %s (force/recovery "
                "path); a concurrent operation on this chain is possible",
                chain_root)
            yield
            return
        raise PreconditionError(
            f"Timed out acquiring chain lock on {chain_root} "
            f"(for {object_uuid})")
    stop = threading.Event()
    threading.Thread(
        target=_lvstore_lock_heartbeat,
        args=(db_controller, cluster_id, key, owner, stop),
        daemon=True).start()
    try:
        yield
    finally:
        stop.set()
        db_controller.release_lvstore_lock(cluster_id, key, owner)


def _rollback_lvol_creation(lvol, node_ids):
    for node_id in dict.fromkeys(node_ids):
        try:
            lvol_controller.delete_lvol_from_node(lvol.get_id(), node_id)
        except Exception as e:
            logger.error(f"Failed to rollback lvol {lvol.get_id()} from node {node_id}: {e}")


def _rollback_snapshot_bdev(cluster_id, lvs_name, primary_node, snap_bdev_name,
                            lvs_member_nodes, lock=True):
    """Complete the async→sync delete protocol for a snapshot bdev that was
    created on the leader but must be rolled back after a replica-registration
    failure.

    Invariant (revised 2026-08-13, upgrade run 20260812): an async delete must
    ALWAYS be followed by a sync delete on the LEADER plus sync deletes on
    EVERY non-leader HA member of the LVS. The async delete on the leader only
    clears the data clusters — the blob metadata stays on disk and the bdev
    stays registered until the leader's sync delete removes them (SPDK admits
    it once the async pass reports done). The peers' sync deletes clear their
    lvol REGISTRATIONS, and a failed register RPC never proves a peer holds no
    registration: the peer may have registered before the failure, a timed-out
    register may have landed anyway, and journal replay on a restart-gated
    peer can materialize the blob with no registration at all. The previous
    "registered + restart-gated only" set left SNAP_3299 (register answered
    -19) with an async-only delete — the exact async-without-sync the delete
    protocol forbids. A needless sync delete is tolerated by design: -19
    answers "already clean".

    The primary's lvstore lock is held across the async delete AND its
    completion poll, so no other object create/delete interleaves with the
    open window on the leader; each peer sync delete runs under that peer's
    own lvstore lock. Peers that cannot be reached get a durable sync-delete
    task instead of being forgotten."""
    rpc_client = primary_node.rpc_client()
    bdev_name = f"{lvs_name}/{snap_bdev_name}"

    delete_completed = False
    with lvstore_op_lock(cluster_id, lvs_name,
                         node_id=primary_node.get_id(), enabled=lock):
        ret, _ = rpc_client.delete_lvol(bdev_name)  # async initial delete
        if not ret:
            logger.error(f"Rollback: failed to delete {bdev_name} from node: "
                         f"{primary_node.get_id()}")
        else:
            # Bounded completion poll INSIDE the lock: the delete window on
            # the leader stays exclusive until the async pass has finished.
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    st = rpc_client.bdev_lvol_get_lvol_delete_status(bdev_name)
                except Exception as e:
                    logger.error(f"Rollback: delete-status poll for {bdev_name} "
                                 f"failed: {e}")
                    break
                if st in (0, 2, -2, -19):  # completed / not found
                    delete_completed = True
                    break
                time.sleep(0.5)
            if not delete_completed:
                logger.error(f"Rollback: async delete of {bdev_name} did not "
                             f"complete within 15s on {primary_node.get_id()}; "
                             f"peers still get their sync deletes")
            else:
                # The async pass only cleared data clusters; this sync delete
                # removes the leader's blob metadata and bdev registration
                # (see invariant above). -19 answers "already clean".
                ret2, err2 = rpc_client.delete_lvol(bdev_name, sync=True)
                if not ret2 and not (err2 and err2.get("code") == -19):
                    logger.error(f"Rollback: leader sync delete of {bdev_name} "
                                 f"on {primary_node.get_id()[:8]} failed "
                                 f"({err2})")

    # Every non-leader LVS member owes a sync delete (see invariant above).
    # Everyone reachable gets it now (under their own lvstore lock); everyone
    # else gets a durable task so the delete is never lost.
    for node in lvs_member_nodes:
        if node.get_id() == primary_node.get_id():
            continue
        if node.status == StorageNode.STATUS_ONLINE:
            try:
                with lvstore_op_lock(cluster_id, lvs_name,
                                     node_id=node.get_id(), enabled=lock):
                    ret, err = node.rpc_client().delete_lvol(bdev_name, sync=True)
                if ret or (err and err.get("code") == -19):
                    continue
                logger.error(f"Rollback: sync delete of {bdev_name} on "
                             f"{node.get_id()[:8]} failed ({err}); adding task")
            except Exception as e:
                logger.error(f"Rollback: sync delete of {bdev_name} on "
                             f"{node.get_id()[:8]} raised: {e}; adding task")
        tasks_controller.add_lvol_sync_del_task(
            cluster_id, node.get_id(), bdev_name, primary_node.get_id())


def add(lvol_id, snapshot_name, backup=False, lock=True, all_snaps=None, all_lvols=None,
        bypass_migration_check=False, snap_type=SnapShot.TYPE_USER):
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError:
        logger.exception("Volume lookup failed for snapshot request: %s", lvol_id)
        return False, "Volume not found"

    ops_gate.assert_object_ops_allowed("snapshot create", pool_uuid=lvol.pool_uuid)

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
    if not bypass_migration_check:
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

    # Name uniqueness via the per-cluster name index (O(1) point read) instead of
    # scanning every snapshot in the cluster on each create.
    if db_controller.snap_name_taken(pool.cluster_id, snapshot_name):
        return False, f"Snapshot name must be unique: {snapshot_name}"

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    if lock and not _wait_for_node_sync_delete(snode):
        msg = f"Timed out waiting for in-flight LVol sync deletion to drain on node: {snode.get_id()}"
        logger.error(msg)
        return False, msg

    # Hard per-lvstore object cap (lvols + clones + snapshots).
    from simplyblock_core.controllers import lvol_controller as _lvol_ctrl
    from simplyblock_core.utils.ttl_cache import cached_mini_lvols, cached_mini_snapshots
    limit_error = _lvol_ctrl.check_lvstore_object_limit(
        snode, cached_mini_lvols(db_controller),
        cached_mini_snapshots(db_controller))
    if limit_error:
        logger.error(limit_error)
        return False, limit_error

    logger.info(f"Creating snapshot: {snapshot_name} from LVol: {lvol.get_id()}")

    # The stats read only refines the size used for the pool-limit checks
    # below; on unlimited pools (the common case) its result is never
    # consulted, so skip the FDB stats lookup entirely.
    size = lvol.size
    if pool.lvol_max_size > 0 or pool.pool_max_size > 0:
        rec = db_controller.get_lvol_stats(lvol, 1)
        if rec:
            size = rec[0].size_used

    if 0 < pool.lvol_max_size < size:
        msg = f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, LVol size: {utils.humanbytes(size)} must be below this limit"
        logger.error(msg)
        return False, msg

    if pool.pool_max_size > 0:
        # Only load the full lvol/snapshot sets when a pool size limit is set
        # (the capacity sum). Unlimited pools — the common case — skip both scans.
        if not all_lvols:
            all_lvols = db_controller.get_mini_lvols()
        if not all_snaps:
            all_snaps = db_controller.get_mini_snapshots()
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
    if cluster.status not in cluster.MUTABLE_STATUSES:
        return False, f"Cluster is not active, status: {cluster.status}"

    snap_vuid = utils.get_random_snapshot_vuid()
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

        secondary_nodes = []
        primary_node = _find_lvs_leader(pool.cluster_id, lvol.lvs_name, all_nodes)
        if not primary_node:
            # Never fall back to the configured primary: a create attempted on
            # a non-leader fails on SPDK anyway, and under mass-create retries
            # the per-request leader probing stormed every LVS member for
            # hours (run 20260712-231123). Fail until a leader is
            # re-established (recovery runs inside _find_lvs_leader, at most
            # once per NO_LEADER_TTL_SEC).
            msg = (f"No leader available for LVS {lvol.lvs_name} — "
                   f"rejecting snapshot create until leadership is re-established")
            logger.error(msg)
            return False, msg

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

        # OUTER per-object lock: serialize the whole snapshot-create sequence
        # for this source lvol, so its snapshot chain is created and registered
        # in blobid order and no concurrent delete/resize/clone of the same
        # lvol races it. The INNER lvstore_op_lock wraps each single-node RPC
        # (primary create, each replica register) so no two object operations
        # touch the lvstore on a node at once.
        with object_mutation_lock(pool.cluster_id, lvol.uuid, enabled=lock):
            if primary_node:
                rpc_client = primary_node.rpc_client()

                logger.info("Creating Snapshot bdev")
                ret = False
                with lvstore_op_lock(pool.cluster_id, lvol.lvs_name,
                                     node_id=primary_node.get_id(), enabled=lock):
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

                # Read-only follow-up — deliberately OUTSIDE the lvstore lock:
                # the lock exists to keep lvstore mutations from interleaving,
                # and holding it across this read serialized every other
                # waiter behind a query that mutates nothing.
                snap_bdev = rpc_client.get_bdevs(f"{lvol.lvs_name}/{snap_bdev_name}")
                if snap_bdev:
                    snap_uuid = snap_bdev[0]['uuid']
                    blobid = snap_bdev[0]['driver_specific']['lvol']['blobid']
                    cluster_size = cluster.page_size_in_blocks
                    num_allocated_clusters = snap_bdev[0]["driver_specific"]["lvol"]["num_allocated_clusters"]
                    used_size = int(num_allocated_clusters*cluster_size)
                else:
                    return False, f"Failed to create snapshot on node: {snode.get_id()}"

            # On any register failure, the rollback owes sync deletes to
            # EVERY non-leader LVS member unconditionally (see
            # _rollback_snapshot_bdev) — no per-peer bookkeeping needed.
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

                with lvstore_op_lock(pool.cluster_id, lvol.lvs_name,
                                     node_id=sec.get_id(), enabled=lock):
                    ret = sec_rpc_client.bdev_lvol_snapshot_register(
                        f"{lvol.lvs_name}/{lvol.lvol_bdev}", snap_bdev_name, snap_uuid, blobid)
                if not ret:
                    msg = f"Failed to register snapshot on node: {sec.get_id()}"
                    logger.error(msg)
                    logger.info(f"Removing snapshot from {primary_node.get_id()}")
                    _rollback_snapshot_bdev(
                        pool.cluster_id, lvol.lvs_name, primary_node,
                        snap_bdev_name, all_nodes, lock=lock)
                    return False, msg

    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.data_uuid = str(uuid.uuid4())
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
    snap.snap_type = snap_type
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

    # Link into this lvol's snapshot chain using the by-lvol index (a single
    # reverse read for the current tail) instead of scanning every cluster
    # snapshot. Find the predecessor BEFORE registering the new snap below.
    prev = db_controller.get_lvol_latest_snapshot(lvol_id, exclude_uuid=snap.get_id())
    if prev is not None and not prev.next_snap_uuid:
        prev.next_snap_uuid = snap.get_id()
        snap.prev_snap_uuid = prev.get_id()
        prev.write_to_db()
        snap.write_to_db()

    # Register the new snapshot in the name + by-lvol indexes (O(1)).
    db_controller.index_snapshot(snap)

    snapshot_events.snapshot_create(snap)
    if lvol.do_replicate:
        task = tasks_controller.add_snapshot_replication_task(snap.cluster_id, snap.lvol.node_id, snap.get_id())
        if task:
            snapshot_events.replication_task_created(snap)
    # Keep-the-recovered-source-in-sync: a FAIL-OVER clone's snapshots are
    # shipped back to the original cluster so a later fail-back is a delta.
    # ONLY when nothing else owns the volume's replication: once a forward
    # policy is attached (migration onward, case-4 style), this to-source task
    # runs CONCURRENTLY with the policy's forward transfers on the same
    # snapshots — 2026-08-21: two volumes' shrink snapshots kept landing on
    # the (emptied!) original cluster, the target-side copies the cutover was
    # gated on never appeared, and both cutovers died on max retry.
    if lvol.cloned_from_snap and not getattr(lvol, "replication_policy_id", ""):
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


def list_snapshots(cluster_id=None, node_id=None, lvol_id=None,pool_id_or_name=None, with_details=False):
    all_snaps = db_controller.get_snapshots()
    if lvol_id:
        try:
            lvol = (db_controller.get_lvol_by_id(lvol_id) if utils.UUID_PATTERN.match(lvol_id) is not None
                    else db_controller.get_lvol_by_name(lvol_id))
            snaps = [sn for sn in all_snaps if sn.lvol.get_id() == lvol.get_id()]
        except KeyError:
            logger.error("Can not find lvol with provided lvol_id_or_name: %s", lvol_id)
            return False
    elif pool_id_or_name:
        try:
            pool = db_controller.get_pool_by_id_or_name(pool_id_or_name)
            snaps = db_controller.get_snapshots_by_pool_id(pool.get_id())
        except KeyError:
            logger.error("Can not find pool with provided pool_id_or_name: %s", pool_id_or_name)
            return False
    elif node_id:
        try:
            node = (db_controller.get_storage_node_by_id(node_id)
                    if utils.UUID_PATTERN.match(node_id) is not None
                    else db_controller.get_storage_nodes_by_hostname(node_id)[0])
            snaps = [sn for sn in all_snaps if sn.lvol.node_id == node.get_id()]
        except KeyError:
            logger.error("Can not find node with provided value: %s", node_id)
            return False

    elif cluster_id:
        snaps = [sn for sn in all_snaps if sn.cluster_id == cluster_id]
    else:
        snaps = all_snaps

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
            instances = []
            if snap.instances:
                instances.extend([SnapShot(i).lvol.node_id for i in snap.instances])
            d["Replication target snap"] = snap.target_replicated_snap_uuid
            d["Replication source snap"] = snap.source_replicated_snap_uuid
            d["Prev snap"] = snap.prev_snap_uuid
            d["Next snap"] = snap.next_snap_uuid
            d["Instance on other nodes"] = instances
        data.append(d)

    return data


def delete(snapshot_uuid, force_delete=False, lock=True):
    try:
        snap = db_controller.get_snapshot_by_id(snapshot_uuid)
    except KeyError:
        logger.error(f"Snapshot not found {snapshot_uuid}")
        return False

    ops_gate.assert_object_ops_allowed("snapshot delete",
                                       cluster_id=snap.cluster_id)

    # OUTER per-object lock: make the clone-count check and the data-plane
    # delete atomic against a concurrent clone-create of this same snapshot
    # (which holds the same lock for its whole sequence). Without it a clone
    # can register against the snapshot's blob just as the snapshot is
    # hard-deleted, corrupting the replica blob tree ("Clone entry not
    # found"). force_delete (recovery/cleanup) bypasses the lock so it always
    # pushes through.
    try:
        with object_mutation_lock(snap.cluster_id, snap.uuid, enabled=lock and not force_delete):
            return _delete_locked(snap, snapshot_uuid, force_delete, lock=lock)
    except PreconditionError as e:
        logger.error(str(e))
        return False


def _delete_locked(snap, snapshot_uuid, force_delete=False, lock=True):
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

    # Refuse deletes while the cluster cannot complete them: the controller
    # only issues the leader-side async delete and marks the snapshot
    # in_deletion; snapshot_monitor performs the sync deletes on the
    # non-leaders and removes the record, but it skips clusters in these
    # states — accepting the delete here would strand the snapshot forever
    # (2026-07-12 mass-delete run: ~60k snapshot deletes accepted while the
    # cluster was stuck in_activation, none ever completed). read_only stays
    # allowed: deletes free space.
    if not force_delete:
        cluster = db_controller.get_cluster_by_id(snap.cluster_id)
        if cluster.status in [cluster.STATUS_SUSPENDED, cluster.STATUS_IN_ACTIVATION,
                              cluster.STATUS_UNREADY, cluster.STATUS_INACTIVE]:
            logger.error(f"Cannot delete snapshot {snapshot_uuid}: cluster "
                         f"{cluster.get_id()} status is {cluster.status}")
            return False

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

    if not force_delete and _successor_mid_replication(snap):
        logger.error(
            f"Cannot delete snapshot {snapshot_uuid}: its successor "
            f"{snap.next_snap_uuid} is replicating right now, and the "
            f"delete's swap-merge would change the cluster map that "
            f"transfer is reading. Retry after the transfer finishes.")
        return False

    try:
        if snode is None:
            snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    except KeyError:
        logger.exception(f"Storage node not found {snap.lvol.node_id}")
        if force_delete:
            db_controller.unindex_snapshot(snap)
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
            with lvstore_op_lock(snap.cluster_id, snap.lvol.lvs_name,
                                 node_id=snode.get_id(), enabled=lock and not force_delete):
                ret = delete_bdev_absent_ok(snode, snap.snap_bdev)
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
            # No confirmed leader: never fire phase-1 at a guessed node — the
            # async delete is leader-only on SPDK, so the guess just errors
            # ("Deleting async lvol on non-leader lvs.", 18k of them in run
            # 20260725) or worse lands on a stale acting leader mid-handoff.
            # Park the snapshot IN_DELETION with no recorded phase-1 node; the
            # snapshot monitor drives the delete once a leader is confirmed.
            logger.warning(
                f"No confirmed leader for {snap.lvol.lvs_name} — deferring "
                f"snapshot {snapshot_uuid} delete to the monitor")
            snap = db_controller.get_snapshot_by_id(snapshot_uuid)
            snap.status = SnapShot.STATUS_IN_DELETION
            snap.deletion_status = ""
            snap.write_to_db(db_controller.kv_store)
            return True

        # special_delete (SPDK migration_flag) must be set ONLY when the SAME
        # snapshot exists on more than one node — i.e. lvol migration placed a
        # copy on another node. snap.instances holds exactly those extra
        # node-copies; it is empty for a snapshot on its home node only and is
        # NOT grown by local clones. Previously this was derived from the
        # blobstore blob open_ref>1, which a local clone also bumps and which a
        # clone-entry metadata leak can strand high — both wrongly forced
        # special_delete=True (e2e 20260717: LVS_9/SNAP_34 had only a local
        # clone, no migration, yet went out special_delete=True).
        special_delete = len(snap.instances) > 0

        with lvstore_op_lock(snap.cluster_id, snap.lvol.lvs_name,
                             node_id=primary_node.get_id(), enabled=lock and not force_delete):
            ret = delete_bdev_absent_ok(primary_node, snap.snap_bdev, sync=False,
                                        special_delete=special_delete)
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
            try:
                lvol_controller.delete_lvol(base_lvol)
            except (PreconditionError, RuntimeError):
                logger.warning("Failed to delete volume", exc_info=True)
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

    ops_gate.assert_object_ops_allowed("clone create", cluster_id=snap.cluster_id)

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

    if lock and not _wait_for_node_sync_delete(snode):
        msg = f"Timed out waiting for in-flight LVol sync deletion to drain on node: {snode.get_id()}"
        logger.error(msg)
        return False, msg

    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in cluster.MUTABLE_STATUSES:
        return False, f"Cluster is not active, status: {cluster.status}"

    # Hard per-lvstore object cap (lvols + clones + snapshots).
    from simplyblock_core.controllers import lvol_controller as _lvol_ctrl
    from simplyblock_core.utils.ttl_cache import cached_mini_lvols, cached_mini_snapshots
    limit_error = _lvol_ctrl.check_lvstore_object_limit(
        snode, cached_mini_lvols(db_controller),
        cached_mini_snapshots(db_controller))
    if limit_error:
        logger.error(limit_error)
        return False, limit_error

    # Clone-name uniqueness / reuse via the per-pool lvol name index (O(1) point
    # read) instead of scanning every lvol in the DB.
    existing = db_controller.lvol_name_lookup(pool.get_id(), clone_name)
    if existing is not None:
        if existing.cloned_from_snap == snapshot_id:
            if existing.status in [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION]:
                msg = f"Clone status {existing.status} can not proceed"
                logger.error(msg)
                return False, msg
            logger.info(f"Clone already exists, reusing lvol: {existing.get_id()}")
            return existing.get_id(), False
        msg = f"LVol name must be unique: {clone_name}"
        logger.error(msg)
        return False, msg

    # all_snaps only feeds the pool-capacity sum below (get_random_vuid no
    # longer dedupes); minis suffice and the load is skipped entirely for
    # unlimited pools instead of full-scanning every snapshot per clone.
    if not all_snaps and pool.pool_max_size > 0:
        all_snaps = db_controller.get_mini_snapshots()
    if not all_lvols:
        all_lvols = db_controller.get_mini_lvols()
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
    if records and records[0].size_total > 0:
        # Both operands are EFFECTIVE (client-visible) bytes: size_prov is the
        # sum of provisioned lvol sizes, and size_total is parity-adjusted at
        # collection time (see simplyblock_core.utils.capacity). size_total is
        # zero on a cluster whose collector has not yet reported any device --
        # skip the check rather than dividing by it.
        rec = records[0]
        # rec.size_prov is lvol-only; snapshots hold ACTUAL bytes on top of it
        # (see pool_controller.get_cluster_snapshot_utilization).
        if not all_snaps:
            all_snaps = db_controller.get_mini_snapshots()
        snap_used = pool_controller.get_cluster_snapshot_utilization(
            cluster.get_id(), all_snaps=all_snaps)
        cluster_size_prov_util = int(((rec.size_prov + snap_used + size) / rec.size_total) * 100)

        if cluster.prov_cap_crit and cluster.prov_cap_crit < cluster_size_prov_util:
            msg = f"Cluster provisioned cap critical would be, util: {cluster_size_prov_util}% of cluster util: {cluster.prov_cap_crit}"
            logger.error(msg)
            return False, msg

        elif cluster.prov_cap_warn and cluster.prov_cap_warn < cluster_size_prov_util:
            logger.warning(f"Cluster provisioned cap warning, util: {cluster_size_prov_util}% of cluster util: {cluster.prov_cap_warn}")


    # ADVISORY early capacity check only — the authoritative namespace-slot
    # pick happens transactionally in claim_lvol_ns_slot at record-write time
    # (two concurrent clones/creates otherwise race for the same last slot).
    _available_subsys = lvol_controller.get_next_available_subsystem_on_node(snode.get_id(), all_lvols=all_lvols) if namespaced else None

    if not _available_subsys:
        subsys_count = lvol_controller.count_lvol_subsystems(snode, all_lvols)
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

    if pvc_name:
        lvol.pvc_name = pvc_name

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

    # Process pool allowed hosts (for host restriction and/or DH-HMAC-CHAP authentication)
    if pool.dhchap:
        # Pool-level DHCHAP: inherit allowed hosts from pool (no per-host key generation)
        lvol.allowed_hosts = [{"nqn": h} for h in pool.allowed_hosts]

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
        lvol.crypto_bdev = f"crypto_{lvol.lvol_bdev}"
        # The crypto stack entry must carry name + params: _create_bdev_stack
        # reads bdev['name']/bdev['params'] for every entry. Appending only
        # {"type": "crypto"} raised KeyError: 'name' when cloning an encrypted
        # volume. Mirror the create path (lvol_controller.add_lvol). base_name
        # is the current top_bdev (the clone bdev) — captured before top_bdev is
        # reassigned to the crypto bdev below.
        lvol.bdev_stack.append({
            "type": "crypto",
            "name": lvol.crypto_bdev,
            "params": {
                "name": lvol.crypto_bdev,
                "base_name": lvol.top_bdev,
            },
        })
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev
        with create_kms_connection(cluster) as kms:
            try:
                kms.rekey_data_encryption_keys(
                    lvol_dek_path(cluster.get_id(), snap.lvol.get_id()),
                    pool_kek_name(pool.get_id()),
                    lvol_dek_path(cluster.get_id(), lvol.get_id()),
                    pool_kek_name(pool.get_id()),
                )
            except KMSException:
                msg = f"Failed to copy encryption keys for clone {lvol.crypto_bdev}"
                logger.exception(msg)
                return False, msg

    # ONE FDB transaction: pick the namespace slot and persist the record
    # (STATUS_IN_CREATION) together — the record is the slot claim, so
    # concurrent clones/creates conflict-retry instead of double-booking the
    # subsystem's last free namespace slot.
    try:
        db_controller.claim_lvol_ns_slot(
            lvol, snode, bool(namespaced),
            standalone_nqn=cluster.nqn + ":lvol:" + lvol.uuid,
            standalone_namespace=pvc_namespace or "")
    except SubsystemCapacityError as e:
        logger.error(str(e))
        return False, str(e)

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

        secondary_nodes = []
        primary_node = _find_lvs_leader(pool.cluster_id, lvol.lvs_name, all_nodes)
        if not primary_node:
            # Same contract as snapshot create: never attempt the clone on a
            # non-leader — fail until a leader is re-established. The lvol
            # record was already persisted above, so roll it back.
            msg = (f"No leader available for LVS {lvol.lvs_name} — "
                   f"rejecting clone until leadership is re-established")
            logger.error(msg)
            db_controller.release_lvol_ns_slot(lvol)
            return False, msg

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
                db_controller.release_lvol_ns_slot(lvol)
                return False, msg
            elif action == "proceed":
                secondary_nodes.append(candidate)
            elif action == "queue":
                queue_for_restart_drain(
                    candidate.get_id(), lvol.lvs_name,
                    lambda c=candidate: lvol_controller.add_lvol_on_node(
                        lvol, c, is_primary=False,
                        secondary_index=secondary_index_map[c.get_id()]),
                    f"register clone {lvol.uuid} on {candidate.get_id()[:8]}")
            # "skip" — disconnected or pre_block, skip

        # OUTER per-object lock keyed on the SOURCE snapshot: serialize the
        # whole clone-create sequence so concurrent clones of this snapshot
        # (and a concurrent delete of it) never race the snapshot's clone
        # tree, and the clone's replica blobs register after the snapshot's.
        # The INNER lvstore_op_lock wraps each single-node add (primary, then
        # each replica) so no two object operations touch the lvstore on a
        # node at once.
        try:
            with object_mutation_lock(pool.cluster_id, snap.uuid, enabled=lock):
                if primary_node:
                    with lvstore_op_lock(pool.cluster_id, lvol.lvs_name,
                                         node_id=primary_node.get_id(), enabled=lock):
                        lvol_bdev, error = lvol_controller.add_lvol_on_node(lvol, primary_node)
                    if error:
                        logger.error(error)
                        if lvol.status != LVol.STATUS_IN_DELETION:
                            db_controller.release_lvol_ns_slot(lvol)
                        return False, error
                    lvol.lvol_uuid = lvol_bdev['uuid']
                    lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

                for sec in secondary_nodes:
                    with lvstore_op_lock(pool.cluster_id, lvol.lvs_name,
                                         node_id=sec.get_id(), enabled=lock):
                        lvol_bdev, error = lvol_controller.add_lvol_on_node(
                            lvol, sec, is_primary=False,
                            secondary_index=secondary_index_map[sec.get_id()])
                    if error:
                        logger.error(error)
                        if lvol.status != LVol.STATUS_IN_DELETION:
                            db_controller.release_lvol_ns_slot(lvol)
                        return False, error
        except PreconditionError as e:
            if lvol.status != LVol.STATUS_IN_DELETION:
                db_controller.release_lvol_ns_slot(lvol)
            return False, str(e)

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

    return snap.get_clean_dict()


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
