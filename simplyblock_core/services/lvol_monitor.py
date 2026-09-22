import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.controllers import (health_controller, lvol_events, tasks_controller, lvol_controller,
                                           snapshot_controller, storage_events)
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import jc_compression_upgrade

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)


def try_repair_lvol_on_non_leader(lvol, sec_node, secondary_index):
    """Self-heal a missing/incomplete lvol registration on an ONLINE
    non-leader (secondary/tertiary).

    A create-time registration can be lost: the create path queues the
    non-leader registration when the node's LVS is gated (restart phase),
    and that queue is in-memory per process — a stale/resurrected phase
    turns the deferral into a permanent loss (incident 2026-07-10: lvol
    cef09c39's tertiary subsystem was never created, the volume served
    2/3 paths and a dual outage within FTT killed all IO). The monitor
    already detects the miss via check_lvol_on_node — this converts the
    detection into a repair: create the subsystem if absent (mirrors the
    restart flow's idempotent registration loop) and re-run the
    idempotent ns+listener registration.

    Returns True when the registration was repaired."""
    from simplyblock_core import storage_node_ops

    ok, err = storage_node_ops.repair_lvol_registration_on_non_leader(
        lvol, sec_node, secondary_index)
    if not ok:
        logger.error("Repair of lvol %s registration on %s failed: %s",
                     lvol.get_id(), sec_node.get_id(), err)
        return False
    logger.info("Repaired lvol %s registration on non-leader %s",
                lvol.get_id(), sec_node.get_id())
    return True

def set_lvol_status(lvol, status):
    # Atomic compare-and-set: a full read-modify-write would clobber a concurrent
    # change to another LVol field (e.g. lvol_stat_collector clearing io_error,
    # or a deletion_status update) — the same lost-update class as incident
    # 2026-06-18. Mutate only status on the freshly-read row.
    if lvol.status == status:
        return
    outcome = {"old": None, "changed": False}

    def _mut(x):
        if x.status == status:
            return False
        outcome["old"] = x.status
        outcome["changed"] = True
        x.status = status
        return True

    lvol = db.atomic_update(db.get_lvol_by_id(lvol.get_id()), _mut)
    if lvol is not None and outcome["changed"]:
        lvol_events.lvol_status_change(lvol, lvol.status, outcome["old"], caused_by="monitor")


def set_lvol_health_check(lvol, health_check_status):
    lvol = db.get_lvol_by_id(lvol.get_id())
    if lvol.health_check == health_check_status:
        return
    now = str(datetime.now())
    outcome = {"old": None, "changed": False}

    def _mut(x):
        if x.health_check == health_check_status:
            return False
        outcome["old"] = x.health_check
        outcome["changed"] = True
        x.health_check = health_check_status
        x.updated_at = now
        return True

    lvol = db.atomic_update(lvol, _mut)
    if lvol is not None and outcome["changed"]:
        lvol_events.lvol_health_check_change(lvol, lvol.health_check, outcome["old"], caused_by="monitor")


def set_snapshot_health_check(snap, health_check_status):
    snap = db.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    now = str(datetime.now())
    def _apply_health_check(s, v=health_check_status, t=now):
        s.health_check = v
        s.updated_at = t
    db.atomic_update(snap, _apply_health_check)


lvol_del_start_time = 0.0
def pre_lvol_delete_rebalance():
    global lvol_del_start_time
    if lvol_del_start_time == 0:
        lvol_del_start_time = time.time()


def resume_comp(lvol):
    logger.info("resuming compression")
    node = db.get_storage_node_by_id(lvol.node_id)
    # Release-upgrade guard (remove with the jc_compression_upgrade plugin):
    # resumes are held until `cluster upgrade-complete`.
    if jc_compression_upgrade.resume_is_held(db.get_cluster_by_id(node.cluster_id)):
        logger.info("JC compression resume held: cluster upgrade in progress")
        return
    for n in db.get_storage_nodes_by_cluster_id(node.cluster_id):
        if n.status != StorageNode.STATUS_ONLINE:
            logger.warning("Not all nodes are online, can not resume JC compression")
            return
    rpc_client = node.rpc_client(timeout=5, retry=2)
    ret, err = rpc_client.jc_suspend_compression(jm_vuid=node.jm_vuid, suspend=False)
    if err:
        logger.info("Failed to resume JC compression adding task...")
        tasks_controller.add_jc_comp_resume_task(node.cluster_id, node.get_id(), node.jm_vuid)


def post_lvol_delete_rebalance(cluster, lvol):
    global lvol_del_start_time
    diff = time.time() - lvol_del_start_time
    if diff > 0:
        records = db.get_cluster_capacity(cluster, int(diff/5))
        if not records:
            # No capacity samples over the window (a freshly bootstrapped
            # cluster, or a monitor restart mid-delete leaves
            # lvol_del_start_time at its 0.0 default, making diff -- and the
            # requested window -- huge). Nothing to compare against; skip the
            # rebalance check rather than indexing an empty list.
            lvol_del_start_time = 0
            return False
        total_size = records[0].size_total
        current_cap = records[0].size_used
        start_cap = records[-1].size_used
        if start_cap - current_cap > int(total_size * 10 / 100):
            resume_comp(lvol)
        lvol_del_start_time = 0
        return True
    lvol_records = db.get_lvol_stats(lvol, 1)
    if lvol_records:
        total_size = db.get_cluster_capacity(cluster, 1)[0].size_total
        if lvol_records[0].size_used > int(total_size * 10 / 100):
            resume_comp(lvol)


def _await_delete_completion(node, bdev_name, wait_sec):
    """Poll the async-delete status until it leaves "in progress" (1) or the
    budget runs out. Returns the last status seen."""
    deadline = time.time() + max(0, wait_sec)
    ret = node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
    while ret == 1 and time.time() < deadline:
        time.sleep(0.2)
        ret = node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
    return ret


def process_lvol_delete_finish(cluster, lvol, leader_independent=False):
    logger.info(f"LVol deleted successfully, id: {lvol.get_id()}")

    # Re-read the record: `lvol` comes from the cycle-start snapshot in
    # check_node, and the API delete call may have completed its inline sync
    # legs (and recorded them in sync_deleted_nodes) after that snapshot was
    # taken. Using the stale copy would re-issue a sync delete on a node that
    # already had one.
    try:
        lvol = db.get_lvol_by_id(lvol.get_id())
    except KeyError:
        return  # already finalised by another pass

    if not lvol.bdev_stack:
        # RECORD-ONLY retirement (see the empty-stack branch in check_node): a
        # retired landing volume's stack was emptied on purpose because its
        # blob lives on as the converted, chained snapshot. Nothing is owed on
        # any node — and the teardown gate below could never pass for it, since
        # the bdev is SUPPOSED to still be there.
        logger.info(
            f"LVol {lvol.get_id()}: empty bdev stack, retiring the record only")
        lvol_events.lvol_delete(lvol)
        lvol.remove(db.kv_store)
        return

    snode = db.get_storage_node_by_id(lvol.node_id)
    sec_nodes = []
    for sec_id in lvol.nodes[1:]:
        try:
            sec_nodes.append(db.get_storage_node_by_id(sec_id))
        except KeyError:
            pass

    _ONLINE = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
               StorageNode.STATUS_DOWN]

    if leader_independent:
        # The data-plane async delete is already DONE (get_lvol_delete_status
        # returned 3: "async done, leadership changed, sync now blocked"). The
        # clusters are freed; every remaining sync delete is just per-node
        # record / subsystem / bdev cleanup and does NOT depend on who holds
        # LVS leadership now. So skip the leader probe entirely and drive the
        # sync deletes in the fixed sequence: the node the async delete ran on
        # first (deletion_status), then primary -> secondary -> tertiary,
        # whichever is online and not yet done.
        #
        # Without this, ret == 3 fell through with only a log line: leadership
        # had moved off the async node, delete_lvol_from_node then hit
        # check_non_leader_for_operation and skipped/queued the leader-side sync
        # delete forever, so the blob + bdev survived and the record was never
        # removed. LVolMonitor logged "error code: 3" thousands of times with
        # no state change while retried client DELETEs short-circuited on
        # in_deletion and returned success in ms (prod run
        # n_plus_k_failover_multi_client_ha_all_nodes-20260908-174343: 2250x for
        # one lvol, still going 14 min after the test gave up).
        leader_node = None
        if lvol.deletion_status:
            try:
                _asy = db.get_storage_node_by_id(lvol.deletion_status)
                if _asy.status in _ONLINE:
                    leader_node = _asy
            except KeyError:
                pass
        if leader_node is None:
            for _nid in lvol.nodes:
                try:
                    _n = db.get_storage_node_by_id(_nid)
                except KeyError:
                    continue
                if _n.status in _ONLINE:
                    leader_node = _n
                    break
        if leader_node is None:
            logger.warning(
                "LVol %s: async delete done but no online node to complete the "
                "sync delete on; will retry next pass", lvol.get_id())
            return

    else:
        leader_node = None
    snode = db.get_storage_node_by_id(snode.get_id())
    if not leader_independent and snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
        if not ret:
            raise Exception("Failed to get LVol info")
        lvs_info = ret[0]
        if lvs_info.get('lvs leadership'):
            leader_node = snode

    if not leader_independent:
        if not leader_node:
            for sec_node in sec_nodes:
                if sec_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                    ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                    if ret:
                        lvs_info = ret[0]
                        if lvs_info.get('lvs leadership'):
                            leader_node = sec_node
                            break

        if not leader_node:
            raise Exception("Failed to get leader node")

        # Leader stickiness (same rationale as check_node): the async delete
        # already completed on the deletion_status node — finish THERE while
        # it is reachable instead of restarting the whole delete on a node
        # that grabbed leadership during a flap.
        if lvol.deletion_status and lvol.deletion_status != leader_node.get_id():
            try:
                owner = db.get_storage_node_by_id(lvol.deletion_status)
                if owner.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                                    StorageNode.STATUS_DOWN]:
                    leader_node = owner
            except KeyError:
                pass

        if lvol.deletion_status != leader_node.get_id():
            with snapshot_controller.lvstore_op_lock(
                    cluster.get_id(), lvol.lvs_name, node_id=leader_node.get_id()):
                try:
                    lvol_controller.delete_lvol_from_node(lvol.get_id(), leader_node.get_id())
                except (PreconditionError, RuntimeError) as e:
                    logger.warning(
                        f"LVol {lvol.get_id()}: teardown on {leader_node.get_id()[:8]} "
                        f"not completed this pass: {e}")
            return

    # Both paths above set leader_node or returned/raised.
    assert leader_node is not None

    # Determine non-leader nodes for sync delete
    non_leader_nodes = []
    for node_id in lvol.nodes:
        if node_id != leader_node.get_id():
            try:
                non_leader_nodes.append(db.get_storage_node_by_id(node_id))
            except KeyError:
                pass
    # The leader NEEDS its sync delete. The async delete (sync=False) only
    # clears the data clusters and strips the in-memory clone-list entries
    # (bs_delete_blob_finish_async -> blob_clear_clusters_async); the blob
    # metadata stays on disk and the bdev stays registered. This leader-side
    # sync delete is the only operation that removes them
    # (_vbdev_lvol_destroy is_sync=true), and SPDK admits it only after the
    # async pass reported done — which is exactly when this function runs.
    #
    # Removing this leg (ae4679792, based on run 20260807) leaked the blob
    # and bdev of EVERY deleted lvol on its leader: upgrade run 20260812
    # (test_major_upgrade-20260812-170049) ended with all 4 LVSes retaining
    # their complete object sets, and the follow-up snapshot delete failing
    # EBUSY ("Cannot remove snapshot because it is open") because its
    # children's blobs were still alive on the leader. The "Clone entry not
    # found" storm this leg produces (4361x in run 20260807) is benign noise:
    # blob_get_snapshot_and_clone_entries only logs when the async pass has
    # already removed the in-memory entry, and the delete proceeds. The
    # run-20260807 "0 leftovers without this leg" observation was drawn from
    # create-rollback objects and does not hold for the regular delete path.
    primary_node = db.get_storage_node_by_id(leader_node.get_id())
    lvol_bdev_name = f"{lvol.lvs_name}/{lvol.lvol_bdev}"

    # THE GATE. The leader's sync delete is the only operation that removes the
    # blob metadata and unregisters the bdev, and until this was added the
    # record removal below did not depend on it in any way: the whole block was
    # skipped when the leader was merely unreachable or restarting, a failure
    # was logged and stepped over, and delete_lvol_from_node answered "True"
    # for nodes it had decided not to touch. Each of those dropped the FDB
    # record while the lvol was still live in SPDK, with nothing left to find
    # it by. Anything short of a confirmed teardown now keeps the record
    # in_deletion so the next monitor pass retries it.
    if primary_node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                                   StorageNode.STATUS_DOWN]:
        logger.warning(
            f"LVol {lvol.get_id()}: leader {primary_node.get_id()[:8]} is "
            f"{primary_node.status}; cannot complete the sync delete of "
            f"{lvol_bdev_name}. Keeping the record in_deletion for the next pass")
        return

    # Check if any non-leader node needs sync lock
    for nln in non_leader_nodes:
        if nln.status in [StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE]:
            primary_node.lvol_del_sync_lock()
            break
    with snapshot_controller.lvstore_op_lock(
            cluster.get_id(), lvol.lvs_name, node_id=primary_node.get_id()):
        try:
            lvol_controller.delete_lvol_from_node(
                lvol.get_id(), primary_node.get_id(), sync=True,
                force=leader_independent)
        except PreconditionError as e:
            # Not a failure: a durable task (or delete_lvol_from_node itself,
            # for a disconnected node) already owns finishing this. The record
            # stays in_deletion for the next pass either way.
            logger.warning(
                f"LVol {lvol.get_id()}: teardown on primary_node "
                f"{primary_node.get_id()} deferred: {e}")
            return
        except RuntimeError as e:
            logger.error(
                f"Failed to delete lvol from primary_node node: "
                f"{primary_node.get_id()} ({e}); keeping the record in_deletion")
            # Durable retry in case this monitor process dies before the next
            # pass; the record stays either way.
            tasks_controller.add_lvol_sync_del_task(
                cluster.get_id(), primary_node.get_id(), lvol_bdev_name, lvol.node_id)
            return

    # Post-condition, not just an acknowledged RPC: confirm the bdev is really
    # gone from the leader. A failed probe is not "clean".
    try:
        primary_confirmed = lvol_controller.lvol_bdev_absent_on_node(lvol, primary_node)
    except Exception as e:
        logger.error(
            f"LVol {lvol.get_id()}: could not verify {lvol_bdev_name} is gone "
            f"from {primary_node.get_id()[:8]} ({e}); keeping the record in_deletion")
        return
    if not primary_confirmed:
        logger.error(
            f"LVol {lvol.get_id()}: sync delete of {lvol_bdev_name} reported "
            f"success but the bdev is still present on "
            f"{primary_node.get_id()[:8]}; keeping the record in_deletion")
        # Fall through rather than returning: the peers below are cleared
        # independently of the primary, and remembering an already-cleared
        # peer here saves a retry pass from re-walking its blob tree even
        # though the record has to wait on the primary regardless.

    peers_owing: list[str] = []
    peers_cleared: list[str] = []
    for sec_node in non_leader_nodes:
        if sec_node.get_id() in lvol.sync_deleted_nodes:
            # The API delete call already completed this node's sync leg
            # inline (lvol_controller._delete_lvol_from_all_nodes). Issuing a
            # second one walks the replica blob tree again and errors on every
            # entry the first pass cleaned — the same defect the leader used
            # to suffer above.
            logger.debug(
                f"Sync delete of {lvol_bdev_name} on {sec_node.get_id()[:8]} "
                f"already done inline; skipping")
            continue
        # Attempt first, classify afterwards: a suspended peer is still up and
        # can clear its registration, and a peer that is gone owes nothing —
        # its in-memory state dies with the process and is not rebuilt, because
        # the record is already deleted. Only a failure on a live peer earns a
        # retry task. Pre-judging by status queued a task for every non-online
        # peer, and those tasks then refused to run *because* the node was
        # suspended, which also blocked the node's own shutdown (run 15 case 6).
        logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {sec_node.get_id()}")
        # Same per-node serialization as the primary: the sync delete
        # mutates the replica blob tree and must not interleave with a
        # create/register of another object on this node ("operation
        # sneaked in between async and sync delete").
        with snapshot_controller.lvstore_op_lock(
                cluster.get_id(), lvol.lvs_name, node_id=sec_node.get_id()):
            cleared = snapshot_controller.sync_delete_on_peer(
                sec_node, lvol_bdev_name, primary_node.get_id())
        # sync_delete_on_peer returns True for "nothing is owed here" — which
        # covers both a successful delete and a peer that is gone (its
        # registration dies with the process and is not rebuilt). False means a
        # LIVE peer still carries the registration; it has a durable retry
        # task, but until that lands the bdev is present in SPDK, so the record
        # must not go yet. This return value was previously discarded.
        (peers_cleared if cleared else peers_owing).append(sec_node.get_id())

    if peers_cleared:
        # Remember the peers that are done, so a retry pass does not re-walk a
        # replica blob tree that is already clean.
        db.atomic_update(
            db.get_lvol_by_id(lvol.get_id()),
            lambda x: x.sync_deleted_nodes.extend(
                n for n in peers_cleared if n not in x.sync_deleted_nodes))

    # Release the primary's del-sync gate — see the note in
    # snapshot_monitor.process_snap_delete_finish: it is set whenever a peer
    # looks down, it blocks creation on this node, and only the sync-del task
    # runner clears it. Reset keeps it only while sync-del tasks are pending.
    primary_node.lvol_del_sync_lock_reset()

    if peers_owing:
        logger.warning(
            f"LVol {lvol.get_id()}: {lvol_bdev_name} still registered on live "
            f"peers {[n[:8] for n in peers_owing]}; their sync-delete tasks are "
            f"queued. Keeping the record in_deletion until they drain")
        return

    if not primary_confirmed:
        # Already logged above; the peers were cleared (or found owing) on
        # the way here so a retry does not repeat that work.
        return

    lvol_events.lvol_delete(lvol)
    lvol.remove(db.kv_store)

    # check for full devices
    full_devs_ids = []
    all_devs_ids = []
    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
            continue
        all_devs_ids.append(dev.get_id())
        if dev.status == NVMeDevice.STATUS_CANNOT_ALLOCATE:
            full_devs_ids.append(dev.get_id())

    if 0 < len(full_devs_ids) == len(all_devs_ids):
        logger.info("All devices are full, starting expansion migrations")
        for dev_id in full_devs_ids:
            tasks_controller.add_new_device_mig_task(dev_id)
    post_lvol_delete_rebalance(cluster, lvol)


def process_lvol_delete_try_again(lvol):
    db.atomic_update(db.get_lvol_by_id(lvol.get_id()),
                     lambda x: setattr(x, "deletion_status", ""))


#: Wall-clock of the last per-lvol subsystem verification sweep.
_last_subsys_sweep = 0.0


def _subsys_sweep_due() -> bool:
    """True at most once per LVOL_MONITOR_SUBSYS_CHECK_INTERVAL_SEC.

    Evaluated once per monitor cycle (not per lvol) and stamps the clock as a
    side effect, so a cycle either sweeps every node or none of them.
    """
    global _last_subsys_sweep
    now = time.time()
    if now - _last_subsys_sweep < constants.LVOL_MONITOR_SUBSYS_CHECK_INTERVAL_SEC:
        return False
    _last_subsys_sweep = now
    return True


def check_node(cluster, snode, all_lvols, subsys_check=False):
    # Number of in-deletion lvols acted on this pass — the main loop uses it
    # to shorten the inter-cycle sleep while a mass delete is draining.
    deletions_processed = 0
    # Per-pass leadership cache: the probe below costs 1-3 get_lvstores RPCs
    # and used to run once PER LVOL — at mass-delete scale that alone adds
    # tens of seconds per cycle. Leadership moves mid-pass are already
    # handled by the poll's own error codes (-35/4), which reset
    # deletion_status and re-resolve on the next cycle.
    leader_cache: dict = {}

    for lvol in all_lvols:
        if lvol.node_id != snode.get_id():
            continue

        if lvol.status in (LVol.STATUS_RESTORING, LVol.STATUS_RESTORE_FAILED):
            # tasks_runner_backup.py owns status transitions for these states
            continue

        if lvol.status == LVol.STATUS_IN_CREATION:
            # A create that died (process killed) between writing the
            # IN_CREATION record and the final ONLINE transition leaves a
            # permanent zombie: nothing advances it, yet it keeps counting
            # against pool capacity and max_lvol. Detect a stale IN_CREATION —
            # far older than any real create — and route it through the normal
            # force-delete so partial data-plane state is torn down and the DB
            # record (and its reserved capacity) is freed. An in-progress
            # create is younger than the threshold and is left untouched.
            stale = True
            if lvol.create_dt:
                try:
                    age = (datetime.now() - datetime.fromisoformat(lvol.create_dt)).total_seconds()
                    stale = age > constants.LVOL_IN_CREATION_STALE_SEC
                except (ValueError, TypeError):
                    stale = True
            if stale:
                logger.error(
                    f"LVol {lvol.get_id()} stuck in {LVol.STATUS_IN_CREATION} "
                    f"since {lvol.create_dt}; cleaning up orphaned create")
                try:
                    # `all_lvols` holds mini records; the delete needs the
                    # full lvol. Re-read also re-verifies the status — the
                    # create may have finished since the cycle-start scan.
                    full_lvol = db.get_lvol_by_id(lvol.get_id())
                    if full_lvol.status == LVol.STATUS_IN_CREATION:
                        lvol_controller.delete_lvol(full_lvol, force_delete=True)
                except KeyError:
                    pass
                except Exception as e:
                    logger.error(f"Failed to clean up orphaned in_creation lvol {lvol.get_id()}: {e}")
            continue

        if lvol.status == LVol.STATUS_IN_DELETION:
            # `all_lvols` holds mini records; the deletion state machine
            # needs the full lvol (nodes, deletion_status, lvs_name). The
            # re-read also re-verifies the status against the authoritative
            # record instead of the cycle-start snapshot.
            try:
                lvol = db.get_lvol_by_id(lvol.get_id())
            except KeyError:
                continue
            if lvol.status != LVol.STATUS_IN_DELETION:
                continue

            deletions_processed += 1

            # RECORD-ONLY deletion: a retired landing volume's record carries
            # an EMPTY bdev_stack on purpose -- its blob lives on as the
            # converted, chained snapshot and must never be deleted. When the
            # retirement sequence in snapshot_replication is interrupted
            # between emptying the stack and removing the record, the record
            # is left in_deletion here; the delete flow below then has nothing
            # to issue, the status poll returns 4 ("no async delete request")
            # forever (856x/30min, run 20260825_125156), and every cleanup
            # that waits for lvols to drain times out behind it. Nothing on
            # any node belongs to this record any more: retire it.
            if not lvol.bdev_stack:
                logger.info(
                    f"LVol {lvol.get_id()} ({lvol.lvol_name}) is in_deletion "
                    f"with an empty bdev stack (retired landing volume); "
                    f"removing the record only -- its blob lives on as the "
                    f"converted snapshot")
                process_lvol_delete_finish(cluster, lvol)
                continue

            # The FULL delete of a chain member — the async delete, its
            # completion wait, and the sync deletes that follow — is one
            # atomic sequence per LVS+chain: a delete swap-merges segments
            # into the neighbouring snapshot and re-links parents, so no
            # create/clone/delete anywhere else in the chain may interleave.
            # Distinct chains hold different keys and run in parallel.
            with snapshot_controller.object_mutation_lock(
                    cluster.get_id(), lvol.get_id()):

                # check leadership (cached per pass, see leader_cache above)
                cache_key = (snode.get_id(), tuple(lvol.nodes[1:]))
                leader_node = leader_cache.get(cache_key)
                if leader_node is None:
                    snode = db.get_storage_node_by_id(snode.get_id())
                    if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                        ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                        if not ret:
                            raise Exception("Failed to get LVol info")
                        lvs_info = ret[0]
                        if lvs_info.get('lvs leadership'):
                            leader_node = snode

                    if not leader_node:
                        for sec_id in lvol.nodes[1:]:
                            try:
                                _sec = db.get_storage_node_by_id(sec_id)
                            except KeyError:
                                continue
                            if _sec.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                                ret = _sec.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                                if ret:
                                    lvs_info = ret[0]
                                    if lvs_info.get('lvs leadership'):
                                        leader_node = _sec
                                        break

                    if not leader_node:
                        raise Exception("Failed to get leader node")
                    leader_cache[cache_key] = leader_node

                # Leader stickiness: while the node that owns the in-flight
                # async delete (deletion_status) is still reachable, keep
                # polling IT — even if another node currently claims lvs
                # leadership. During the 2026-07-16 flap a secondary claimed
                # leadership while the real owner was merely marked down, and
                # this branch re-issued 139 full initial deletes against the
                # secondary, mutating shared snapshot metadata from two nodes.
                # Only re-target when the owner is genuinely gone; the poll's
                # own error codes (-35/4) handle real leadership changes.
                if lvol.deletion_status and lvol.deletion_status != leader_node.get_id():
                    try:
                        owner = db.get_storage_node_by_id(lvol.deletion_status)
                        if owner.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                                            StorageNode.STATUS_DOWN]:
                            leader_node = owner
                    except KeyError:
                        pass

                if lvol.deletion_status == "" or lvol.deletion_status != leader_node.get_id():
                    # Serialize against creates/registers on the target node —
                    # an unlocked delete interleaving with another object's
                    # create on the same lvstore corrupts the replica blob tree.
                    with snapshot_controller.lvstore_op_lock(
                            cluster.get_id(), lvol.lvs_name, node_id=leader_node.get_id()):
                        try:
                            lvol_controller.delete_lvol_from_node(lvol.get_id(), leader_node.get_id())
                        except (PreconditionError, RuntimeError) as e:
                            # Not fatal to this cycle's sweep -- a deferred or
                            # failed issue here must not abort the rest of the
                            # in_deletion lvols on this node this pass (that
                            # blast radius is exactly what the outer
                            # try/except in main()'s _sweep would otherwise
                            # give it).
                            logger.warning(
                                f"LVol {lvol.get_id()}: async delete issue on "
                                f"{leader_node.get_id()[:8]} not completed "
                                f"this pass: {e}")
                    # NOTE no inline sleep here: the loop is SERIAL over every
                    # in-deletion lvol, so a per-object pause multiplies into
                    # minutes of added latency for every object in a mass-delete
                    # wave (run 20260730: cycle times of 4-5 min, single deletes
                    # taking 5+ min end-to-end). The status poll below handles a
                    # still-running async delete (ret == 1) by simply retrying on
                    # the next cycle.

                try:
                    # Bounded wait so the async delete and the sync deletes that
                    # follow stay ONE atomic sequence inside the chain lock (see
                    # the chain-lock note where this block is entered).
                    ret = _await_delete_completion(
                        leader_node, f"{lvol.lvs_name}/{lvol.lvol_bdev}",
                        constants.SNAP_DELETE_COMPLETION_WAIT_SEC)
                except Exception as e:
                    logger.error(e)
                    # timeout detected, check other node
                    break

                if ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
                    if ret == 0:
                        # "Not found" is an answer ABOUT THE NODE WE ASKED, not
                        # about the cluster. When leadership has moved to a peer
                        # whose registration was never created (the miss
                        # try_repair_lvol_on_non_leader exists to repair), that
                        # peer truthfully answers 0 while the blob is alive on
                        # the real owner. This used to finish the delete and drop
                        # the record on the strength of it; the teardown gate in
                        # process_lvol_delete_finish now re-resolves the leader
                        # and confirms before anything is removed — log which
                        # node answered so the misdirection is traceable.
                        logger.info(
                            f"LVol {lvol.get_id()}: {leader_node.get_id()[:8]} "
                            f"reports no such lvol; verifying teardown before "
                            f"finalising")
                    process_lvol_delete_finish(cluster, lvol)

                elif ret == 1:  # Async lvol deletion is in progress or queued
                    logger.info(f"LVol deletion in progress, id: {lvol.get_id()}")
                    pre_lvol_delete_rebalance()

                elif ret == 3:  # Async deletion is done, but leadership has changed (sync deletion is now blocked)
                    # Data is already gone (async unmap completed); only the
                    # per-node record/subsystem/bdev cleanup remains, and that
                    # does not depend on current leadership. Complete it
                    # leader-independently instead of logging forever. See the
                    # process_lvol_delete_finish leader_independent branch.
                    logger.info(
                        f"LVol {lvol.get_id()}: async delete done, leadership "
                        f"moved -> completing sync deletes leader-independently")
                    process_lvol_delete_finish(cluster, lvol,
                                               leader_independent=True)

                elif ret == 4:  # No async delete request exists for this lvol
                    # Transient during leadership/RPC churn (e.g. a peer down +
                    # post-unblock drain): the async-delete request was never
                    # registered on the node we polled because leadership flipped or
                    # the re-issue RPC didn't land on a flaky leader. This is NOT a
                    # terminal error — flipping the lvol OFFLINE + io_error here
                    # abandons the deletion and strands it (incident
                    # mass_create_delete_docker-20260629: 14 lvols stuck offline).
                    # Reset deletion_status so the next pass re-issues the async
                    # delete on the then-current leader; the lvol stays in_deletion
                    # and drains once leadership/RPC settles (same handling as the
                    # -35 "leadership changed" case).
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.warning("No async delete request exists for this lvol; re-issuing on next pass")
                    process_lvol_delete_try_again(lvol)

                elif ret == -1:  # Operation not permitted
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Operation not permitted")
                    lvol = db.atomic_update(db.get_lvol_by_id(lvol.get_id()),
                                            lambda x: setattr(x, "io_error", True))
                    set_lvol_status(lvol, LVol.STATUS_OFFLINE)

                elif ret == -2:  # No such file or directory
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("No such file or directory")
                    process_lvol_delete_finish(cluster, lvol)

                elif ret == -5:  # I/O error
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("I/O error")
                    process_lvol_delete_try_again(lvol)

                elif ret == -11:  # Try again
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Try again")
                    process_lvol_delete_try_again(lvol)

                elif ret == -12:  # Out of memory
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Out of memory")
                    process_lvol_delete_try_again(lvol)

                elif ret == -16:  # Device or resource busy
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Device or resource busy")
                    process_lvol_delete_try_again(lvol)

                elif ret == -19:  # No such device
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Finishing lvol delete")
                    process_lvol_delete_finish(cluster, lvol)

                elif ret == -35:  # Leadership changed
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Leadership changed")
                    process_lvol_delete_try_again(lvol)

                elif ret == -36:  # Failed to update lvol for deletion
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Failed to update lvol for deletion")
                    process_lvol_delete_try_again(lvol)

                else:  # Failed to update lvol for deletion
                    logger.info(f"LVol deletion error, id: {lvol.get_id()}, error code: {ret}")
                    logger.error("Failed to update lvol for deletion")

                continue

        # Continuous per-lvol subsystem verification + repair. On by default;
        # the caller decides once per cycle whether the (2 RPCs per lvol)
        # sweep is due — see _subsys_sweep_due().
        if not subsys_check:
            continue

        # `all_lvols` is a cycle-start snapshot and a full pass takes minutes
        # at scale — a mass delete can flip a volume to in_deletion long
        # before the loop reaches it. Acting on the stale status here made
        # the monitor "repair" (re-add) namespaces the delete flow had just
        # removed, re-exposing an async-deleted blob to clients and
        # resurrecting the DB record (incident mass_create_delete_k8s
        # 2026-07-14: 2123 leaked lvols + all-night restart storm). Re-read
        # before checking; states owned by other flows wait for the next
        # cycle's snapshot to route them to their own branch above.
        try:
            lvol = db.get_lvol_by_id(lvol.get_id())
        except KeyError:
            continue
        if lvol.status not in (LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE):
            continue

        if not getattr(lvol, "from_source", True):
            # The retired SOURCE of a fail-over: its namespace was removed
            # deliberately (_retire_source_data_path) and the record only
            # survives as the fail-back delta base. Checking it always fails,
            # and the self-heal below would register the namespace back —
            # resurrecting the stale device the retirement removed.
            continue

        if snode.lvstore_status != "ready":
            continue

        passed = True
        try:
            # Verify against the WIRE identity: after a fail-back the record
            # uuid differs from the namespace's advertised uuid, and checking
            # the record uuid here marked healthy volumes unhealthy and drove
            # the self-heal below into re-registering the wrong identity.
            passed &= health_controller.check_subsystem(
                lvol.nqn, rpc_client=snode.rpc_client(), ns_uuid=lvol.get_ns_uuid())
        except Exception as e:
            logger.error(f"Failed to check lvol:{lvol.get_id()} on node: {lvol.node_id}")
            logger.error(e)

        if lvol.ha_type == "ha":
            for sec_index, sec_id in enumerate(lvol.nodes[1:]):
                try:
                    sec_node = db.get_storage_node_by_id(sec_id)
                except KeyError:
                    continue
                if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                    try:
                        ret = health_controller.check_subsystem(
                            lvol.nqn, rpc_client=sec_node.rpc_client(),
                            ns_uuid=lvol.get_ns_uuid())
                        if not ret:
                            passed = False
                            # Explicit, greppable degraded-path signal. Without
                            # it a replica whose subsystem is missing (or,
                            # worse, present but with no namespace) is visible
                            # nowhere above the client: the host connects
                            # successfully and simply has one path fewer than
                            # it believes. Incident 2026-08-09 ran 36 hours in
                            # that state before an outage exposed it.
                            logger.error(
                                "DEGRADED PATHS: lvol %s (%s) replica on node "
                                "%s is not serving — subsystem missing or has "
                                "no namespace; volume is running below its "
                                "configured redundancy",
                                lvol.get_id(), lvol.lvs_name, sec_id)
                            # Self-heal: a missing registration on an online
                            # non-leader never fixes itself (the create-time
                            # deferral queue is lossy) — re-register now. The
                            # next monitor cycle re-checks and restores
                            # health_check once the repair sticks.
                            try:
                                try_repair_lvol_on_non_leader(lvol, sec_node, sec_index)
                            except Exception as re:
                                logger.error(
                                    f"Repair attempt for lvol {lvol.get_id()} "
                                    f"on node {sec_id} raised: {re}")
                    except Exception as e:
                        logger.error(f"Failed to check lvol: {lvol.get_id()} on node: {sec_id}")
                        logger.error(e)

        logger.info(f"LVol: {lvol.get_id()}, is healthy: {passed}")
        set_lvol_health_check(lvol, passed)
        if passed:
            set_lvol_status(lvol, LVol.STATUS_ONLINE)

    return deletions_processed


#: Wall-clock of the last orphan-reconciliation sweep.
_last_orphan_sweep = 0.0


def _orphan_sweep_due() -> bool:
    """True at most once per LVOL_MONITOR_ORPHAN_CHECK_INTERVAL_SEC."""
    global _last_orphan_sweep
    now = time.time()
    if now - _last_orphan_sweep < constants.LVOL_MONITOR_ORPHAN_CHECK_INTERVAL_SEC:
        return False
    _last_orphan_sweep = now
    return True


def sweep_orphan_objects(cluster, snodes):
    """Report lvstore blobs that no FDB record claims. DETECT ONLY.

    The gap this closes is not that the comparison did not exist — it is
    ``storage_node_ops.auto_repair``'s — but that nothing ever ran it.
    auto_repair is an operator-invoked CLI command that prints to stdout, so
    every path that dropped a record while its blob survived leaked silently
    and permanently: the bdev is re-registered from lvstore metadata on the
    next node restart, and nothing in FDB or the cluster log points at it.
    Four such volumes accumulated unnoticed in R26.3, one with no cluster-log
    trace at all.

    Deliberately does NOT delete. A blob this sweep cannot correlate is the one
    whose ownership it understands least, and a false positive would destroy
    live data. auto_repair remains the (interactive, ref-count-aware) way to
    remove them; raising the alarm while the leak is fresh is this sweep's job.
    """
    from simplyblock_core import storage_node_ops

    for snode in snodes:
        if snode.status != StorageNode.STATUS_ONLINE or not snode.lvstore:
            continue
        try:
            orphans = storage_node_ops.find_orphan_lvstore_blobs(snode.get_id())
        except Exception as e:
            logger.warning(
                f"Orphan sweep: could not reconcile {snode.get_id()[:8]}: {e}")
            continue

        if not orphans:
            continue

        detail = ", ".join(
            f"{o['name']} (blobid={o['blobid']}, ref={o['ref']})" for o in orphans)
        logger.error(
            f"ORPHANED LVSTORE OBJECTS on node {snode.get_id()} "
            f"({snode.lvstore}): {len(orphans)} blob(s) exist in SPDK with no "
            f"record in the database — {detail}. They hold capacity and are "
            f"re-registered on every restart. Not deleted automatically: "
            f"inspect with `sbctl sn auto-repair --validate-only`")
        try:
            storage_events.snode_orphaned_objects(snode, orphans)
        except Exception:
            logger.exception("Orphan sweep: could not log the cluster event")


# get DB controller
db = db_controller.DBController()


def main():
    logger.info("Starting LVol monitor...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        deletions_in_flight = 0
        for cluster in db.get_clusters():

            if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
                logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
                continue
            # Mini records: check_node only filters on node_id/status/
            # create_dt here and re-reads the full lvol for any state it
            # acts on. The full-LVol scan re-read every 72-field record
            # per 30s cycle.
            all_lvols = db.get_mini_lvols()
            # Decided once per cycle so a sweep covers every node consistently.
            subsys_check = constants.LVOL_MONITOR_SUBSYS_CHECK and _subsys_sweep_due()
            # Nodes are swept concurrently: a node's work is independent, and
            # anything that mutates a blob chain takes the chain lock, so two
            # workers can never interleave inside one chain (nor with a
            # create/clone running in another CP process). One serial sweep
            # made every node wait for the slowest one, and a delete backlog
            # on a single node stalled the whole cluster's monitoring.
            snodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())

            def _sweep(snode, cluster=cluster, all_lvols=all_lvols, subsys_check=subsys_check):
                try:
                    return check_node(cluster, snode, all_lvols,
                                      subsys_check=subsys_check) or 0
                except Exception as e:
                    logger.error(e)
                    return 0

            if snodes:
                workers = max(1, min(constants.LVOL_MONITOR_NODE_WORKERS, len(snodes)))
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    deletions_in_flight += sum(ex.map(_sweep, snodes))

            # After the per-node sweep, so an object this cycle just finished
            # deleting is not reported as an orphan.
            if constants.LVOL_MONITOR_ORPHAN_CHECK and _orphan_sweep_due():
                try:
                    sweep_orphan_objects(cluster, snodes)
                except Exception as e:
                    logger.error(f"Orphan sweep failed: {e}")

        # Adaptive cadence: while deletes are draining, every full-interval
        # sleep adds up to 30s of latency PER CHAIN HOP (a clone must fully
        # finish before its snapshot becomes deletable, and that before the
        # parent snapshot) — run 20260730: a 7-object delete chain took ~50
        # minutes almost entirely in monitor waits. Re-scan quickly while
        # in-deletion objects exist; idle clusters keep the low-load 30s.
        if deletions_in_flight > 0:
            time.sleep(constants.LVOL_MONITOR_DELETION_INTERVAL_SEC)
        else:
            time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
