# coding=utf-8
import threading
import time
from datetime import datetime, timezone


from simplyblock_core import constants, db_controller, cluster_ops, storage_node_ops, utils
from simplyblock_core.controllers import health_controller, device_controller, tasks_controller, storage_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()

node_rpc_timeout_threads: dict[str, threading.Thread] = {}

# A cluster_activate() that wedges — started before all nodes were back, or
# blocked/aborted partway through its per-node recreate RPCs — leaves the
# cluster stuck in IN_ACTIVATION. The monitor early-returns on IN_ACTIVATION
# and add_node_to_auto_restart refuses to queue restarts while the cluster is
# not SUSPENDED, so the cluster can never recover on its own (incident
# 2026-06-25: ~2h11m hang). If the cluster has been IN_ACTIVATION longer than
# this, revert it to SUSPENDED so the gated activation + auto-restart re-queue
# paths run again. A healthy activation completes in well under a minute.
CLUSTER_ACTIVATION_WATCHDOG_SEC = 600

# In-flight suspend-recovery force-shutdowns, keyed by node_id. A force shutdown
# (kill + ANA failover) can take many seconds; this keeps the monitor tick from
# re-spawning one for the same node before it has flipped to in_shutdown/offline.
recovery_shutdown_threads: dict[str, threading.Thread] = {}


def is_new_migrated_node(cluster_id, node):
    dev_lst = []
    for dev in node.nvme_devices:
        if dev.status == NVMeDevice.STATUS_ONLINE:
            dev_lst.append(dev.get_id())

    distr_names = []
    for item in node.lvstore_stack:
        if item["type"] == "bdev_distr":
            distr_names.append(item["name"])

    if dev_lst:
        tasks = db.get_job_tasks(cluster_id)
        for task in tasks:
            if task.function_name == JobSchedule.FN_NEW_DEV_MIG and task.node_id == node.get_id():
                if task.device_id not in dev_lst:
                    continue
                if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                    if "distr_name" in task.function_params and task.function_params["distr_name"] in distr_names:
                        return True
    return False


# How long a node may sit in IN_SHUTDOWN before the monitor treats it as a
# stranded shutdown and reconciles it to OFFLINE. A graceful shutdown (device
# events + SPDK kill + ANA failover) completes well within this; anything longer
# means the shutdown crashed or its offline flip was lost (incident 2026-06-18).
IN_SHUTDOWN_RECONCILE_GRACE_SEC = 120


def _in_shutdown_longer_than(node, seconds):
    """True if a node has been IN_SHUTDOWN for at least ``seconds``.

    Keyed off ``node.shutdown_since`` (stamped by set_node_status on the
    ->IN_SHUTDOWN transition). A missing/blank/unparseable timestamp is treated
    as NOT long enough — be conservative and never reconcile a node whose entry
    time we can't establish (e.g. a still-running shutdown on an old row).
    """
    ss = getattr(node, "shutdown_since", "") or ""
    if not ss:
        return False
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(ss)).total_seconds() >= seconds
    except Exception:
        return False


def _down_longer_than(node, seconds):
    """True if a DOWN node has been DOWN for at least ``seconds``.

    Keyed off ``node.down_since`` (stamped by set_node_status on the ->DOWN
    transition). A missing/blank/unparseable timestamp is treated as
    "long enough" so we stay conservative: an old row without the field still
    suspends + auto-restarts rather than being silently ignored forever.
    """
    ds = getattr(node, "down_since", "") or ""
    if not ds:
        return True
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(ds)).total_seconds() >= seconds
    except Exception:
        return True


def _fd_aware_cluster_status(cluster, snodes, affected_ips, n, k, jm_replication_tasks):
    """Failure-domain-aware cluster status.

    Implements the operator-agreed availability contract for clusters created
    with ``--enable-failure-domain`` (where data/parity chunks are spread
    across distinct domains):

      (A) Losing ALL nodes and/or any combination of devices within a SINGLE
          failure domain is tolerated — the cluster stays serving (DEGRADED),
          never SUSPENDED.
      (B) Losing a whole failure domain AND one additional node/device outage
          (>=1 devices on a SINGLE node) on ONE other domain is also tolerated,
          but ONLY when FTT == 2 (``distr_npcs`` == 2) AND the cluster spans at
          least ndcs (``distr_ndcs``) independent failure domains.

    Anything broader than (A)/(B) -> SUSPENDED.

    ``affected_ips`` is the list of distinct physical-host mgmt IPs that have a
    device/node outage (the same set the flat per-node logic counts).

    Failure-domain ids are 32-bit ints: ``-1`` means "unset", ``>= 0`` is a
    real domain (0 is a valid domain — never test truthiness here).

    Returns a ``Cluster.STATUS_*`` string, or ``None`` to signal "this is not a
    usable failure-domain layout — fall back to the flat per-node suspend
    logic": fewer than 2 tagged domains, or an affected host carrying no domain
    tag (mixed/partially-tagged cluster — be safe, don't reason FD-wise).
    """
    all_fds = {nd.failure_domain for nd in snodes if nd.failure_domain >= 0}
    if len(all_fds) < 2:
        return None

    fd_by_ip = {nd.mgmt_ip: nd.failure_domain for nd in snodes}
    # Group the affected physical hosts by their failure domain.
    damaged: dict[int, set] = {}
    for ip in affected_ips:
        fd = fd_by_ip.get(ip, -1)
        if fd < 0:
            return None
        damaged.setdefault(fd, set()).add(ip)

    num_damaged_fds = len(damaged)
    if num_damaged_fds == 0:
        # No FTT-affecting damage; an in-flight JM replication still degrades.
        return Cluster.STATUS_DEGRADED if jm_replication_tasks else Cluster.STATUS_ACTIVE

    # Any surviving damage means reduced redundancy -> at best DEGRADED.
    # (A) Damage confined to one domain is always tolerated.
    if num_damaged_fds == 1:
        return Cluster.STATUS_DEGRADED

    # (B) One whole domain + at most one extra affected node on exactly one
    # other domain, gated on FTT==2 and >=ndcs independent domains.
    if num_damaged_fds == 2 and k == 2 and len(all_fds) >= n:
        nodes_in_smaller_domain = min(len(ips) for ips in damaged.values())
        if nodes_in_smaller_domain <= 1:
            return Cluster.STATUS_DEGRADED

    # Damage spans more domains (or more than one extra node on the second
    # domain) than the contract permits.
    return Cluster.STATUS_SUSPENDED


def get_next_cluster_status(cluster_id):
    logger.info(f"get_next_cluster_status for cluster_id: {cluster_id}")
    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status == cluster.STATUS_UNREADY:
        return Cluster.STATUS_UNREADY
    snodes = db.get_primary_storage_nodes_by_cluster_id(cluster_id)

    online_nodes = 0
    offline_nodes = 0
    affected_nodes = 0
    online_devices = 0
    offline_devices = 0
    jm_replication_tasks = False

    affected_physical_nodes = []

    for node in snodes:

        node_online_devices = 0
        node_offline_devices = 0

        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_SUSPENDED]:
            continue

        if node.status == StorageNode.STATUS_ONLINE:
            if is_new_migrated_node(cluster_id, node):
                continue
            online_nodes += 1
            try:
                # check for jm rep tasks:
                if node.rpc_client(timeout=10).bdev_lvol_get_lvstores(node.lvstore):
                    ret = node.rpc_client(timeout=8).jc_get_jm_status(node.jm_vuid)
                    for jm in ret:
                        if ret[jm] is False: # jm is not ready (has active replication task)
                            jm_replication_tasks = True
                            logger.warning("Replication task found!")
                            break
            except Exception:
                logger.warning("Failed to get replication task!")
        elif node.status == StorageNode.STATUS_REMOVED:
            pass
        else:
            offline_nodes += 1
        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_JM,
                              NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                node_online_devices += 1
            elif dev.status == NVMeDevice.STATUS_FAILED_AND_MIGRATED:
                pass
            else:
                node_offline_devices += 1

        if node_offline_devices > 0 or (node_online_devices == 0 and node.status != StorageNode.STATUS_REMOVED):
            affected_nodes += 1
            if node.mgmt_ip not in affected_physical_nodes:
                affected_physical_nodes.append(node.mgmt_ip)
        elif node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_REMOVED,
                                 StorageNode.STATUS_DOWN]:
            # Non-ONLINE (UNREACHABLE / SCHEDULABLE / IN_SHUTDOWN / RESTARTING)
            # with devices still flagged online in the DB (e.g. UNREACHABLE
            # before _check_data_plane_and_escalate fires). From a client's
            # perspective the node is unavailable, so it contributes to the FTT
            # bucket.
            #
            # DOWN is intentionally excluded and never counts toward suspension:
            # it is a temporary, self-recovering state — SPDK and its devices are
            # alive, only the client-facing LVS port is firewall-blocked
            # (set_node_down flips node status only). Recovery is a port-unblock,
            # not a destructive restart, so a DOWN node must not tip the cluster
            # toward SUSPENDED.
            if node.mgmt_ip not in affected_physical_nodes:
                affected_physical_nodes.append(node.mgmt_ip)

        online_devices += node_online_devices
        offline_devices += node_offline_devices

    affected_nodes = len(affected_physical_nodes)
    logger.debug(f"online_nodes: {online_nodes}")
    logger.debug(f"offline_nodes: {offline_nodes}")
    logger.debug(f"affected_nodes: {affected_nodes}")
    logger.debug(f"online_devices: {online_devices}")
    logger.debug(f"offline_devices: {offline_devices}")
    # ndcs n = 2
    # npcs k = 1
    n = cluster.distr_ndcs
    k = cluster.distr_npcs

    # Failure-domain-aware suspend criteria. When the cluster was created with
    # --enable-failure-domain, chunks are spread across distinct domains, so it
    # can survive losing whole domains that the flat per-node count below would
    # treat as a fatal FTT breach (affected_nodes > k). The helper returns None
    # when the layout isn't a usable FD layout (<2 tagged domains, or an
    # affected host without a domain tag), in which case we fall through to the
    # legacy node-count logic unchanged.
    if cluster.enable_failure_domain:
        fd_status = _fd_aware_cluster_status(
            cluster, snodes, affected_physical_nodes, n, k, jm_replication_tasks)
        if fd_status is not None:
            return fd_status

    # if number of devices in the cluster unavailable on DIFFERENT nodes > k --> I cannot read and in some cases cannot write (suspended)
    if affected_nodes == k and (not cluster.strict_node_anti_affinity or online_nodes >= (n + k)):
        return Cluster.STATUS_DEGRADED
    elif jm_replication_tasks:
        return Cluster.STATUS_DEGRADED
    elif (affected_nodes > k or online_devices < (n + k) or (
            online_nodes < (n + k) and cluster.strict_node_anti_affinity)):
        return Cluster.STATUS_SUSPENDED
    else:
        return Cluster.STATUS_ACTIVE


def _requeue_stuck_auto_restarts(cluster_id):
    """Re-queue auto-restart for any OFFLINE / SCHEDULABLE node that does
    not currently have an active FN_NODE_RESTART task.

    Why this exists: ``set_node_offline`` only queues once (it guards on
    the OFFLINE -> OFFLINE no-op), so if ``add_node_to_auto_restart`` was
    refused at that moment — typically because too many peers were
    non-online while the cluster was still DEGRADED — nothing else
    retries it, and the cluster stays in SUSPENDED / DEGRADED forever
    with no path back. Running this scan every monitor tick closes the
    loop: once the cluster transitions to SUSPENDED (via the non-ONLINE
    counting in ``get_next_cluster_status``), the queue lands on the
    next tick. Also handles SCHEDULABLE recoveries that bypassed the
    queue for the same reason.
    """
    try:
        # While a suspended cluster is still draining to all-offline, auto-restart
        # is paused (add_node_to_auto_restart would refuse anyway); skip the scan
        # so it does not log a misleading "re-queuing" line every tick. Mirrors
        # tasks_controller.is_auto_restart_paused; inlined here to avoid coupling
        # the scan to that import.
        cluster = db.get_cluster_by_id(cluster_id)
        if (cluster.status == Cluster.STATUS_SUSPENDED
                and not cluster.suspend_drain_complete):
            return
        for node in db.get_storage_nodes_by_cluster_id(cluster_id):
            if node.status not in (StorageNode.STATUS_OFFLINE,
                                   StorageNode.STATUS_SCHEDULABLE):
                continue
            # Nodes stopped on purpose (`sn shutdown`) land in OFFLINE just
            # like a failure-detected node, but must NOT be auto-restarted.
            # add_node_to_auto_restart enforces this too, but skip them here
            # so the scan does not log a misleading "re-queuing" line every
            # tick for a node that is intentionally down.
            if node.auto_restart_disabled:
                continue
            if tasks_controller.get_active_node_restart_task(cluster_id, node.get_id()):
                continue
            logger.info(
                "Node %s is %s with no active restart task; re-queuing auto-restart",
                node.get_id(), node.status,
            )
            try:
                tasks_controller.add_node_to_auto_restart(node)
            except Exception as e:
                logger.error("Failed to re-queue auto-restart for %s: %s", node.get_id(), e)
    except Exception as e:
        logger.error("Auto-restart re-queue scan failed for cluster %s: %s", cluster_id, e)


def _activation_node_gate(cluster_id, nodes, max_fault_tolerance):
    """Decide whether the storage nodes are settled enough to (re)activate.

    Returns ``(can_activate: bool, reason: str)``.

    Auto-reactivation must NOT start until the outage has fully recovered —
    all nodes brought back ONLINE one-by-one — otherwise it either fires
    uselessly while nodes are still UNREACHABLE (race #1) or wedges the
    cluster in IN_ACTIVATION when it runs against a not-yet-online node
    (race #2). Both were observed in incident 2026-06-25. Rules:

      * No node may be mid-transition (IN_SHUTDOWN / IN_CREATION / RESTARTING)
        or UNREACHABLE — recovery is still in flight; activating now races the
        still-restarting nodes.
      * Every node must be ONLINE, EXCEPT up to ``max_fault_tolerance`` nodes
        that an operator *deliberately* shut down (``auto_restart_disabled``):
        those are not coming back on their own and the cluster is designed to
        run without up-to-FTT of them.
      * A node that is OFFLINE / DOWN / SCHEDULABLE but was NOT a deliberate
        shutdown is still recovering (auto-restart will bring it back) — wait.
      * Prior guards preserved: a node with an active restart task, or one
        ONLINE for < 30s (not yet settled), block activation.
    """
    ftt = max_fault_tolerance if isinstance(max_fault_tolerance, int) and max_fault_tolerance >= 1 else 1
    deliberate_down = 0
    for node in nodes:
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        if node.status in (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_IN_CREATION,
                            StorageNode.STATUS_RESTARTING, StorageNode.STATUS_UNREACHABLE):
            return False, f"node {node.get_id()} is still transitioning ({node.status})"
        if node.status != StorageNode.STATUS_ONLINE:
            # OFFLINE / DOWN / SCHEDULABLE.
            if getattr(node, "auto_restart_disabled", False):
                deliberate_down += 1
                continue
            return False, (f"node {node.get_id()} is {node.status} and was not deliberately "
                           f"shut down; waiting for it to restart and come back online")
        # ONLINE from here on.
        if tasks_controller.get_active_node_restart_task(cluster_id, node.get_id()):
            return False, f"node {node.get_id()} has an active restart task"
        if node.online_since:
            try:
                diff = datetime.now(timezone.utc) - datetime.fromisoformat(node.online_since)
                if diff.total_seconds() < 30:
                    return False, f"node {node.get_id()} has been online less than 30 seconds"
            except (ValueError, TypeError):
                pass
    if deliberate_down > ftt:
        return False, (f"{deliberate_down} deliberately-shut-down node(s) exceed the cluster "
                       f"fault tolerance ({ftt}); not enough nodes to activate safely")
    return True, ""


def _watchdog_stuck_activation(cluster):
    """Revert a cluster wedged in IN_ACTIVATION back to SUSPENDED.

    ``cluster_activate`` runs in the monitor's own status thread; if a tick's
    activation died or returned without completing the transition, the status
    stays IN_ACTIVATION, every later tick early-returns, and
    ``add_node_to_auto_restart`` refuses to queue restarts while the cluster
    is not SUSPENDED — a deadlock that never clears on its own (incident
    2026-06-25). After ``CLUSTER_ACTIVATION_WATCHDOG_SEC`` with no completion,
    flip back to SUSPENDED so the gated activation and the auto-restart
    re-queue scan run again.
    """
    since = getattr(cluster, "in_activation_since", "")
    if not since:
        return
    try:
        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(since)).total_seconds()
    except (ValueError, TypeError):
        return
    if elapsed < CLUSTER_ACTIVATION_WATCHDOG_SEC:
        return
    logger.error(
        "Cluster %s stuck in IN_ACTIVATION for %.0fs (> %ds); reverting to SUSPENDED so "
        "activation re-gates and auto-restart can re-queue",
        cluster.get_id(), elapsed, CLUSTER_ACTIVATION_WATCHDOG_SEC)
    cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_SUSPENDED)


# Node statuses that mean a node is NOT yet drained for suspend recovery:
# either still up/serving or mid-transition. The drain is complete only once
# every (non operator-stopped) node has left all of these for OFFLINE/REMOVED.
_DRAIN_PENDING_STATUSES = (
    StorageNode.STATUS_ONLINE,
    StorageNode.STATUS_DOWN,
    StorageNode.STATUS_UNREACHABLE,
    StorageNode.STATUS_IN_SHUTDOWN,
    StorageNode.STATUS_RESTARTING,
    StorageNode.STATUS_IN_CREATION,
    StorageNode.STATUS_SCHEDULABLE,
)


def _spawn_recovery_shutdown(node):
    """Force-shutdown a node for suspend recovery in a daemon thread (so the
    kill / ANA-failover work does not block the monitor tick). keep_auto_restart
    leaves auto_restart_disabled unset, so once the whole cluster has drained
    the existing auto-restart brings this node back."""
    node_id = node.get_id()
    existing = recovery_shutdown_threads.get(node_id)
    if existing is not None and existing.is_alive():
        return

    def _run():
        try:
            storage_node_ops.shutdown_storage_node(
                node_id, force=True, keep_auto_restart=True)
        except Exception as e:
            logger.error("Suspend-recovery shutdown of %s failed: %s", node_id, e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    recovery_shutdown_threads[node_id] = t


def _drive_suspend_recovery(cluster):
    """Drive a SUSPENDED cluster to a clean all-offline slate before any
    auto-restart runs (auto-restart stays paused via
    tasks_controller.is_auto_restart_paused until this finishes).

    Per tick while the drain is incomplete:
      * ONLINE / DOWN nodes (reachable and up) -> force-shutdown.
      * UNREACHABLE nodes -> escalate to OFFLINE directly. We cannot reach them
        to shut SPDK down, and once peers have drained there is no online quorum
        left for the normal data-plane escalation, so without this the drain
        would stall forever on a stranded UNREACHABLE node. (Nodes that move
        UNREACHABLE -> OFFLINE on their own simply reach OFFLINE first; this is
        just the convergence backstop.)
      * Operator-stopped nodes (auto_restart_disabled) are left alone and
        excluded from both the drain wait and the later restart.
    Once every node has settled to OFFLINE/REMOVED, flip
    suspend_drain_complete so the existing auto-restart resumes. After that this
    is a no-op, so it never re-kills nodes that are restarting back up."""
    if cluster.suspend_drain_complete:
        return

    nodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())
    for node in nodes:
        if node.auto_restart_disabled:
            continue
        if node.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
            logger.info(
                "Cluster %s suspended: force-shutting-down node %s (status %s) "
                "for clean recovery drain",
                cluster.get_id(), node.get_id(), node.status)
            _spawn_recovery_shutdown(node)
        elif node.status == StorageNode.STATUS_UNREACHABLE:
            logger.info(
                "Cluster %s suspended: escalating unreachable node %s to OFFLINE "
                "to converge recovery drain", cluster.get_id(), node.get_id())
            try:
                set_node_offline(node)
            except Exception as e:
                logger.error("Failed to escalate unreachable node %s to OFFLINE: %s",
                             node.get_id(), e)

    drained = all(
        node.auto_restart_disabled or node.status not in _DRAIN_PENDING_STATUSES
        for node in nodes
    )
    if drained:
        fresh = db.get_cluster_by_id(cluster.get_id())
        if fresh.status == Cluster.STATUS_SUSPENDED and not fresh.suspend_drain_complete:
            fresh.suspend_drain_complete = True
            fresh.write_to_db()
            logger.info(
                "Cluster %s drained to all-offline; suspend-recovery auto-restart "
                "unpaused", cluster.get_id())


def update_cluster_status(cluster_id):
    # Run the re-queue scan FIRST, before any of the transition branches
    # that may early-return. Otherwise OFFLINE/SCHEDULABLE nodes can stay
    # stranded whenever the cluster takes a recovery path (e.g.
    # DEGRADED -> ACTIVE).
    # _requeue_stuck_auto_restarts(cluster_id)

    next_current_status = get_next_cluster_status(cluster_id)
    logger.info("cluster_new_status: %s", next_current_status)

    rebalancing_task_names = {
        JobSchedule.FN_DEV_MIG,
        JobSchedule.FN_NEW_DEV_MIG,
        JobSchedule.FN_FAILED_DEV_MIG,
        JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
        JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
        JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION,
        JobSchedule.FN_LVOL_MIG,
    }
    active_rebalancing_tasks = 0
    for task in db.get_job_tasks(cluster_id):
        if task.canceled:
            continue
        if task.status == JobSchedule.STATUS_DONE:
            continue
        if task.function_name in rebalancing_task_names:
            active_rebalancing_tasks += 1

    cluster = db.get_cluster_by_id(cluster_id)
    # Atomic: a full write here would clobber a concurrent cluster.status change
    # committed by set_cluster_status (same lost-update class as incident
    # 2026-06-18). Mutate only is_re_balancing on the freshly-read row.
    is_re_balancing = active_rebalancing_tasks > 0
    cluster = db.atomic_update(
        cluster, lambda c, v=is_re_balancing: setattr(c, "is_re_balancing", v))

    current_cluster_status = cluster.status
    logger.info("cluster_status: %s", current_cluster_status)

    # Suspend recovery: while the cluster is SUSPENDED, first drain every node
    # to OFFLINE (auto-restart is paused until then), so recovery restarts from
    # a clean slate instead of one-by-one onto stale/half-initialized peers.
    if current_cluster_status == Cluster.STATUS_SUSPENDED:
        try:
            _drive_suspend_recovery(cluster)
        except Exception:
            logger.exception("Suspend-recovery drive failed for cluster %s", cluster_id)

    # One-shot auto-migration to shared (per-chunk) data placement.
    # Armed (shared_placement_migration_pending) by exactly two events:
    #   * cluster creation — for brand-new clusters
    #   * cluster_ops.update_cluster, only AFTER every node's upgrade restart
    #     has completed — never mid rolling-restart
    # We require that explicit flag rather than firing on a bare
    # shared_placement==False, because during a rolling upgrade the cluster
    # passes through transient ACTIVE / not-rebalancing / all-online windows
    # between node restarts; switching then would race the still-restarting
    # nodes. With the flag set only at upgrade completion, switching here is
    # safe once the cluster has settled.
    if (cluster.shared_placement_migration_pending
            and not cluster.shared_placement
            and current_cluster_status == Cluster.STATUS_ACTIVE
            and not cluster.is_re_balancing):
        sp_nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
        if sp_nodes and all(n.status == StorageNode.STATUS_ONLINE for n in sp_nodes):
            logger.info(
                "Auto-enabling shared (per-chunk) placement on cluster %s: "
                "armed, ACTIVE, all nodes online, not rebalancing", cluster_id)
            try:
                if cluster_ops.set_shared_placement(cluster_id, enable=True):
                    # set_shared_placement persisted shared_placement=True;
                    # disarm the request so it runs exactly once. Atomic so it
                    # doesn't clobber a concurrent cluster.status change.
                    db.atomic_update(
                        db.get_cluster_by_id(cluster_id),
                        lambda c: setattr(c, "shared_placement_migration_pending", False))
                    logger.info("shared_placement enabled on cluster %s", cluster_id)
                else:
                    logger.warning(
                        "set_shared_placement returned False for cluster %s; "
                        "will retry next monitor cycle", cluster_id)
            except Exception:
                logger.exception(
                    "Auto shared_placement enable raised for cluster %s", cluster_id)

    if current_cluster_status == Cluster.STATUS_IN_ACTIVATION:
        # Don't drive transitions while an activation is in flight, but check
        # whether it has wedged (incident 2026-06-25) and revert if so.
        _watchdog_stuck_activation(cluster)
        return

    if current_cluster_status in [Cluster.STATUS_UNREADY, Cluster.STATUS_IN_EXPANSION]:
        return

    if current_cluster_status == Cluster.STATUS_DEGRADED and next_current_status == Cluster.STATUS_ACTIVE:
        # if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_UNREADY] and cluster_current_status == Cluster.STATUS_ACTIVE:
        # cluster_ops.cluster_activate(cluster_id, True)
        cluster_ops.set_cluster_status(cluster_id, Cluster.STATUS_ACTIVE)
        return
    elif current_cluster_status == Cluster.STATUS_READONLY and next_current_status in [
        Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        return
    elif current_cluster_status == Cluster.STATUS_SUSPENDED and next_current_status \
            in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        # needs activation — but only once the outage has fully recovered.
        # Auto-reactivation must NOT start while nodes are still UNREACHABLE
        # or mid-restart: it either fires uselessly (race #1) or wedges the
        # cluster in IN_ACTIVATION against a not-yet-online node (race #2).
        # Wait until every node is back ONLINE one-by-one, allowing up to FTT
        # deliberately shut-down nodes to stay down (incident 2026-06-25).
        can_activate, reason = _activation_node_gate(
            cluster_id,
            db.get_storage_nodes_by_cluster_id(cluster_id),
            cluster.max_fault_tolerance,
        )
        if not can_activate:
            logger.warning("can not activate cluster: %s", reason)
        else:
            cluster_ops.cluster_activate(cluster_id, force=True)
    else:
        cluster_ops.set_cluster_status(cluster_id, next_current_status)


# Note: a `set_node_online` helper used to live here. It flipped DOWN /
# UNREACHABLE / SCHEDULABLE -> ONLINE on health-check pass. That violated
# the state-machine rule that ONLINE may only be reached from RESTARTING
# (or IN_CREATION / SUSPENDED via the dedicated paths) and contributed
# to the iteration-77 hang where a half-completed restart left the node
# DB-online while peer reconnects had silently failed.
#
# Recovery for nodes stuck in OFFLINE / UNREACHABLE / SCHEDULABLE flows
# through the auto-restart task (FN_NODE_RESTART) -> the restart impl,
# which is the only authority for the ONLINE flip.
#
# DOWN is NOT routed through auto-restart: SPDK is still alive and
# cluster-internal traffic works -- only the client-facing port is
# blocked. Recovery is port-unblock, not a destructive restart.


def set_node_offline(node):
    node = db.get_storage_node_by_id(node.get_id())
    # Do not flip to OFFLINE while the node is mid-restart / mid-shutdown:
    # the runner owns the status transitions during those phases, and an
    # external flip here would race with it. Observed failure mode:
    # HealthCheck/monitor's spdk_process_is_up probe catches the runner's
    # shutdown→restart window and returns False, so set_node_offline fires
    # with status==IN_RESTART and clobbers the runner's progress — also
    # marking devices unavailable, which then fails the runner's
    # post-restart check and forces a full retry loop.
    #
    # UNREACHABLE is intentionally NOT in this skip list: the legitimate
    # escalation path UNREACHABLE → OFFLINE runs through here (via
    # _check_data_plane_and_escalate). Skipping it leaves the node stuck
    # in UNREACHABLE and auto-restart never gets queued.
    if node.status not in [StorageNode.STATUS_OFFLINE,
                           StorageNode.STATUS_IN_SHUTDOWN,
                           StorageNode.STATUS_RESTARTING]:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_OFFLINE)
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    # Default cause is correct: node going offline cascades
                    # to all its devices, that does not count as a flap.
                    device_controller.device_set_unavailable(dev.get_id())
        except Exception as e:
            logger.debug("Setting node to OFFLINE state failed")
            logger.error(e)
            return

        # Cluster-status refresh and ANA failover are best-effort. They must
        # not gate the auto-restart queue: if either throws (e.g., a peer
        # node's RPC times out inside cluster_activate's reach), we would
        # leave the node OFFLINE with no restart queued and the cluster
        # stuck. Isolate each step.
        #
        # node.cluster_id (not the module-level free variable cluster_id):
        # the original code relied on a name set by the main loop in
        # __main__, which makes the function only correct when called
        # transitively from that loop. Bind to the node's own field so
        # the function is callable from any context (and unit-testable).
        try:
            update_cluster_status(node.cluster_id)
        except Exception as cs_e:
            logger.error("update_cluster_status after %s went offline failed: %s",
                         node.get_id(), cs_e)

        try:
            logger.info(f"Triggering ANA failover for node {node.get_id()}")
            storage_node_ops.trigger_ana_failover_for_node(node)
        except Exception as ana_e:
            logger.error("ANA failover for node %s failed: %s", node.get_id(), ana_e)

        try:
            logger.info(f"Node {node.get_id()} set to OFFLINE, adding to auto-restart")
            tasks_controller.add_node_to_auto_restart(node)
        except Exception as ar_e:
            logger.error("add_node_to_auto_restart for %s failed: %s", node.get_id(), ar_e)


def set_node_unreachable(node):
    # Same rationale as set_node_offline: when the runner owns the node's
    # status transitions (IN_SHUTDOWN → OFFLINE → IN_RESTART → ONLINE), the
    # monitor must not race those writes with its own UNREACHABLE flip.
    if node.status not in [StorageNode.STATUS_UNREACHABLE,
                           StorageNode.STATUS_IN_SHUTDOWN,
                           StorageNode.STATUS_RESTARTING]:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_UNREACHABLE)
            update_cluster_status(cluster_id)
        except Exception as e:
            logger.debug("Setting node to UNREACHABLE state failed")
            logger.error(e)

    # Check data-plane health from surviving nodes.  If all online peers
    # report the unreachable node's remote JM as disconnected, the data
    # plane is truly down and we can escalate to offline.
    try:
        _check_data_plane_and_escalate(node)
    except Exception as e:
        logger.error("Data-plane check for unreachable node %s failed: %s", node.get_id(), e)


def is_node_data_plane_disconnected(node):
    """Return True if all other online nodes report *node*'s remote JM as disconnected.

    Returns False if no peers are available to check (conservative).
    """
    disconnected, total = _count_data_plane_votes(node)
    return total > 0 and disconnected == total


def is_node_data_plane_disconnected_quorum(node, lvs_peer_ids=None):
    """Return True if a majority of online nodes report *node*'s remote JM as disconnected.

    Returns False if no peers are available to check (conservative).
    """
    disconnected, total = _count_data_plane_votes(node)
    return total > 0 and disconnected > total // 2


def _count_data_plane_votes(node):
    """Query all other online storage nodes for *node*'s JM connectivity.

    For each online peer, check the state of its NVMe controller that points
    at *node*'s JM subsystem (controller name = ``remote_jm_{node_id}``).
    A peer reports "connected" only if the controller exists AND is in the
    ``enabled`` SPDK state (``deleting``/``failed``/``resetting``/
    ``reconnect_is_delayed``/``disabled`` all count as disconnected).

    Peers that never attached a controller for *node* (no topology link)
    are ignored — they don't have a meaningful vote.

    Why not ``jc_get_jm_status``: that RPC reads ``mjm_stat``, a sync-health
    map updated only by the JC's replication/leveling state machine. It is
    never flipped to ``false`` when the underlying NVMe controller is lost
    (bdev-remove / keep-alive timeout / peer death), so a quietly-dead peer
    perpetually votes "connected" and the quorum gets stuck.

    Returns (disconnected_count, total_peers_checked).
    """
    node_id = node.get_id()
    cluster_nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)

    online_peers = [
        n for n in cluster_nodes
        if n.get_id() != node_id
        and n.status == StorageNode.STATUS_ONLINE
        and n.jm_vuid
    ]

    if not online_peers:
        logger.debug("No online peers to verify data plane for %s", node_id)
        return 0, 0

    ctrl_name = f"remote_jm_{node_id}"
    bdev_name = f"{ctrl_name}n1"
    disconnected = 0
    total = 0

    for peer in online_peers:
        peer_rpc = peer.rpc_client(timeout=8, retry=1)

        # Fast path: does the namespace bdev still exist on the peer?
        # A missing bdev means the controller has been torn down / is being
        # torn down, which is unambiguously "disconnected". We check this
        # first because bdev_get_bdevs is a pure registry lookup and can't
        # block on a degraded controller's internal state, whereas
        # bdev_nvme_get_controllers on a `resetting` / `reconnect_is_delayed`
        # ctrlr can sit on locks during reset.
        try:
            bdevs = peer_rpc.get_bdevs(bdev_name)
        except Exception as e:
            logger.debug("get_bdevs(%s) on peer %s failed: %s", bdev_name, peer.get_id(), e)
            continue

        if not bdevs:
            # If the peer never had a topology link to this node, it never
            # created this bdev either. We can't distinguish "never had it"
            # from "had it and it's gone" just from a missing bdev, so abstain
            # -- callers that know the topology (lvs_peer_ids) are the right
            # place to assert "this peer SHOULD have seen it".
            logger.debug("Data-plane check: peer %s has no %s bdev; abstaining",
                         peer.get_id(), bdev_name)
            continue

        # Bdev exists -> controller must exist too. Now check its state.
        try:
            ret = peer_rpc.bdev_nvme_controller_list(ctrl_name)
        except Exception as e:
            logger.debug("bdev_nvme_controller_list(%s) on peer %s failed: %s",
                         ctrl_name, peer.get_id(), e)
            continue

        total += 1
        if not ret:
            logger.info("Data-plane check: peer %s has %s bdev but no controller -> disconnected",
                        peer.get_id(), bdev_name)
            disconnected += 1
            continue

        paths = ret[0].get("ctrlrs") or []
        enabled = any((p.get("state") == "enabled") for p in paths)
        if enabled:
            logger.info("Data-plane check: peer %s sees %s controller enabled",
                        peer.get_id(), node_id)
        else:
            states = [p.get("state") for p in paths] or ["no-paths"]
            logger.info("Data-plane check: peer %s reports %s controller state=%s -> disconnected",
                        peer.get_id(), node_id, states)
            disconnected += 1

    logger.info("Data-plane check for %s: %d/%d peers report disconnected", node_id, disconnected, total)
    return disconnected, total


def _check_data_plane_and_escalate(unreachable_node):
    """Escalate to offline if data plane is confirmed down."""
    node = db.get_storage_node_by_id(unreachable_node.get_id())
    if node.status == StorageNode.STATUS_RESTARTING:
        logger.debug("Node %s is restarting, skipping data-plane escalation", node.get_id())
        return

    disconnected, total = _count_data_plane_votes(node)
    if total > 0 and disconnected == total:
        logger.info("Data-plane check: all peers report %s JM disconnected, escalating to offline",
                    node.get_id())
        set_node_offline(node)
        return

    if total > 0:
        # Some peers still see the controller — don't escalate yet.
        return

    # No online peers available to vote. Cluster has lost so many nodes that
    # the peer-quorum check can't decide. Fall back to probing the node's
    # own SnodeAPI for SPDK liveness: if SPDK is confirmed gone, we have
    # enough signal to escalate to OFFLINE and unblock auto-restart. Without
    # this fallback, a node whose SPDK died during a multi-node outage stays
    # UNREACHABLE forever (and the cluster stays SUSPENDED with no recovery
    # path), because every peer is also non-online.
    try:
        snode_api = node.client(timeout=10, retry=1)
        is_up, _ = snode_api.spdk_process_is_up(node.rpc_port, node.cluster_id)
        if not is_up:
            logger.info(
                "Data-plane check: no peers to vote, but SnodeAPI confirms SPDK gone on %s; escalating to offline",
                node.get_id(),
            )
            set_node_offline(node)
    except Exception as e:
        logger.debug("Fallback SnodeAPI SPDK probe for %s failed: %s", node.get_id(), e)


def set_node_schedulable(node):
    node = db.get_storage_node_by_id(node.get_id())
    if node.status not in [StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_IN_SHUTDOWN]:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_SCHEDULABLE)
            # initiate shutdown
            # initiate restart
            tasks_controller.add_node_to_auto_restart(node)
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    device_controller.device_set_unavailable(dev.get_id())
            update_cluster_status(cluster_id)
        except Exception as e:
            logger.debug("Setting node to SCHEDULABLE state failed")
            logger.error(e)


def set_node_down(node):
    node = db.get_storage_node_by_id(node.get_id())
    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_IN_SHUTDOWN]:
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_DOWN)
        update_cluster_status(cluster_id)


def node_rpc_timeout_check_and_report(node):
    start_time = time.time()
    try:
        rpc_client = node.rpc_client(timeout=60, retry=5)
        ret = rpc_client.get_version()
        if ret:
            logger.debug(f"SPDK version: {ret['version']}")
            return True
    except Exception as e:
        logger.debug(e)
    # RPC timeout detected, send to cluster log
    storage_events.snode_rpc_timeout(node, int(time.time() - start_time))
    return False


def node_port_check_fun(snode):
    node_port_check = True
    if snode.lvstore_status == "ready":
        ports = [snode.nvmf_port]
        if snode.lvstore_stack_secondary or snode.lvstore_stack_tertiary:
            for n in db.get_primary_storage_nodes_by_secondary_node_id(snode.get_id()):
                if n.lvstore_status != "ready":
                    continue
                # Skip port check during failback: if the primary or the
                # other secondary (sec_1) for this lvstore is online/restarting,
                # the port on this node may be intentionally blocked.
                skip = False
                if n.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_RESTARTING]:
                    skip = True
                elif n.secondary_node_id and n.secondary_node_id != snode.get_id():
                    sec1 = db.get_storage_node_by_id(n.secondary_node_id)
                    if sec1 and sec1.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_RESTARTING]:
                        skip = True
                if not skip:
                    ports.append(n.get_lvol_subsys_port(n.lvstore))
        if not snode.is_secondary_node:
            ports.append(snode.get_lvol_subsys_port(snode.lvstore))

        for port in ports:
            try:
                ret = health_controller.check_port_on_node(snode, port)
                logger.info(f"Check: node port {snode.mgmt_ip}, {port} ... {ret}")
                node_port_check &= ret
            except Exception as e:
                health_controller._log_port_check_failure(db, snode, port, e)

        # Data-NIC reachability via the node agent. _check_ping_from_node is
        # tri-state: True (up), False (agent ran ping, it failed / carrier down),
        # None (SnodeAPI ping_ip timed out -> inconclusive). A timeout must NOT
        # flip the node DOWN: ignore it and re-evaluate next cycle. Only a
        # definitive False with no NIC confirmed up is a real data-NIC failure.
        data_results = []
        for data_nic in snode.data_nics:
            if data_nic.ip4_address:
                data_ping_check = health_controller._check_ping_from_node(data_nic.ip4_address, ifname=data_nic.if_name, node=snode)
                logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping_check}")
                data_results.append(data_ping_check)

        if any(r is True for r in data_results):
            pass  # at least one data NIC confirmed reachable
        elif any(r is False for r in data_results):
            node_port_check = False  # node reports its data NIC down
        else:
            logger.info(
                f"Data-nic ping for {snode.mgmt_ip} inconclusive "
                f"(SnodeAPI ping_ip timed out); ignoring this cycle")

    return node_port_check


# Bounded wait (s) for the parallel port/data-nic check to finish before we
# read its result. The probe started in parallel with the liveness checks, so
# by the time we collect it it has usually completed; if not, we treat it as
# inconclusive for this cycle rather than blocking node-status evaluation.
PORT_CHECK_JOIN_TIMEOUT_SEC = 5


def _spawn_port_check(snode):
    """Run node_port_check_fun in a background daemon thread so the LVS-port /
    data-nic probes — which matter ONLY for an otherwise-online node and can
    hang on a rebooting host's SnodeAPI — run in PARALLEL with the liveness
    checks instead of serializing after them. Returns (thread, holder); read
    holder['result'] after a bounded join (None = errored/never-finished)."""
    holder = {"result": None}

    def _run():
        try:
            holder["result"] = node_port_check_fun(snode)
        except Exception as e:
            logger.debug(f"Port check thread failed for {snode.get_id()}: {e}")

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t, holder


class State:
    counter = 0
def increment():
    State.counter = 1
def decrement():
    State.counter = 0
def value():
    return State.counter

def _spdk_is_dead(snode):
    """True if the node's SPDK is confirmed NOT running (or the node API can't
    be reached, which for our purposes means it isn't serving)."""
    try:
        snode_api = snode.client(timeout=20, retry=1)
        is_up, _ = snode_api.spdk_process_is_up(snode.rpc_port, snode.cluster_id)
        return not is_up
    except Exception as e:
        logger.debug("spdk_process_is_up probe failed for %s: %s", snode.get_id(), e)
        return True


def check_node(snode):
    snode = db.get_storage_node_by_id(snode.get_id())

    # Self-heal a node stranded in IN_SHUTDOWN. A completed shutdown flips the
    # node to OFFLINE; if it is still IN_SHUTDOWN well past the grace window the
    # shutdown crashed or its offline flip was lost (incident 2026-06-18: a
    # concurrent full-object write reverted offline->in_shutdown, after which the
    # monitor skipped the node forever and both re-shutdown and the peer's
    # concurrent shutdown were rejected — a permanent deadlock). Reconcile to
    # OFFLINE once SPDK is confirmed dead so the node is restartable again.
    # auto_restart_disabled (set at shutdown) stays set, so this does NOT trigger
    # an unwanted auto-restart of an intentionally-stopped node.
    if snode.status == StorageNode.STATUS_IN_SHUTDOWN:
        if _in_shutdown_longer_than(snode, IN_SHUTDOWN_RECONCILE_GRACE_SEC) and _spdk_is_dead(snode):
            logger.warning(
                "Node %s stuck in in_shutdown for >%ss with SPDK down; "
                "reconciling to OFFLINE",
                snode.get_id(), IN_SHUTDOWN_RECONCILE_GRACE_SEC)
            storage_node_ops.set_node_status(
                snode.get_id(), StorageNode.STATUS_OFFLINE, caused_by="monitor_reconcile")
        else:
            logger.info(f"Node status is: {snode.status}, skipping")
        return False

    if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                            StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN,
                            StorageNode.STATUS_OFFLINE]:
        logger.info(f"Node status is: {snode.status}, skipping")
        return False

    if snode.status == StorageNode.STATUS_ONLINE and snode.lvstore_status == "in_creation":
        logger.info(f"Node lvstore is in creation: {snode.get_id()}, skipping")
        return False

    # A restart task already owns this node's full lifecycle (shutdown ->
    # IN_RESTART -> recreate_lvstore -> ONLINE). The monitor must not run its
    # own liveness probes against a node in that window: while the restart
    # impl brings SPDK back up (notably during recreate_lvstore) the JSON-RPC
    # unix socket has not begun accept()ing yet, so spdk_process_is_up /
    # check_node_rpc transiently fail and the monitor would flip the node
    # OFFLINE and queue a SECOND restart that fights the one already in flight
    # (incident 2026-06-24: device-25 JC abort, then ~90s of monitor-vs-restart
    # kill/restart churn). Defer until the task reaches DONE/canceled; the
    # restart runner owns recovery and has its own max_retry + transient-reset,
    # so this cannot defer forever.
    if tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id()):
        logger.info(
            "Node %s has an active restart task; monitor deferring liveness "
            "checks until it completes", snode.get_id())
        return True

    logger.info(f"Checking node {snode.hostname}")

    # If the node is offline, ensure ANA failover was processed.
    # Another service may have set the node offline without triggering it.
    # Note: do NOT add auto-restart here — the node may have been
    # intentionally shut down via sbctl.  Auto-restart is only added by
    # set_node_offline() when the monitor itself detects a failure.
    if snode.status == StorageNode.STATUS_OFFLINE:
        try:
            storage_node_ops.trigger_ana_failover_for_node(snode)
        except Exception as e:
            logger.error("ANA failover for offline node %s failed: %s", snode.get_id(), e)
        return True

    # 1- check node ping
    ping_check = health_controller._check_node_ping(snode.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")
    if not ping_check:
        logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}: FAILED")
        set_node_unreachable(snode)
        return False

    # Kick off the LVS-port / data-nic check NOW so it runs in parallel with
    # the liveness checks below (its result is only consumed at the end, and
    # only matters for an otherwise-online node). If a liveness check fails
    # first we return early and this daemon thread is simply abandoned.
    port_check_thread, port_check_holder = _spawn_port_check(snode)

    # 2- check node API
    try:
        # Liveness probe: short timeout, no connect retries — a host that is
        # rebooting stops accepting connections, and retrying with backoff only
        # delays flipping it not-online (incident 2026-06-25: ~21s of detection
        # latency on a host_reboot came from stacked SnodeAPI retries/timeouts).
        snode_api = snode.client(timeout=5, retry=1, connect_retry=0)
        ret, _ = snode_api.is_live()
        logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {ret}")
        if not ret:
            logger.info("Check: node API failed, setting node unreachable")
            set_node_unreachable(snode)
            return False
    except Exception as e:
        logger.debug(e)
        set_node_unreachable(snode)
        return False

    # 3- check spdk process through node API
    try:
        snode_api = snode.client(timeout=10, retry=1, connect_retry=0)
        is_up, _ = snode_api.spdk_process_is_up(snode.rpc_port, snode.cluster_id)
        logger.info(f"Check: spdk process {snode.mgmt_ip}:5000 ... {bool(is_up)}")
        if not is_up:
            logger.info("Check: node API failed, setting node offline")
            set_node_offline(snode)
            return False
    except Exception as e:
        logger.debug(e)
        return False

    # 4- check node rpc interface
    node_rpc_check, node_rpc_check_1 = health_controller.check_node_rpc(snode, timeout=20, retry=1)
    logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

    #if RPC times out, we dont know if its due to node becoming unavailable or spdk hanging
    #so we try it twice. If all other checks pass again, but only this one fails: it's the spdk process
    if not node_rpc_check:
        logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}:TIMEOUT")
        if value()==0:
           increment()
           return False

    decrement()
    if not node_rpc_check or not node_rpc_check_1:
        logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}:FAILED")
        set_node_schedulable(snode)
        return False

    #if not node_rpc_check and snode.get_id() not in node_rpc_timeout_threads:
    #    t = threading.Thread(target=node_rpc_timeout_check_and_report, args=(snode,))
    #    t.start()
    #    node_rpc_timeout_threads[snode.get_id()] = t

    # Collect the port/data-nic check that ran in parallel with the liveness
    # checks. Bounded wait: if it hasn't finished (e.g. a slow/hung SnodeAPI on
    # a rebooting host) treat it as inconclusive and let the next cycle decide,
    # rather than pinning this node's cycle. Only a definitive False escalates.
    port_check_thread.join(PORT_CHECK_JOIN_TIMEOUT_SEC)
    if port_check_thread.is_alive():
        logger.info(
            f"Check: port/data-nic for {snode.mgmt_ip} inconclusive "
            f"(still running after {PORT_CHECK_JOIN_TIMEOUT_SEC}s); "
            f"deferring port-down decision to next cycle")
        node_port_check = None
    else:
        node_port_check = port_check_holder["result"]

    if node_port_check is False:
        cluster = db.get_cluster_by_id(snode.cluster_id)
        if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
            logger.error("Port check failed")
            set_node_down(snode)
            return True

    # Health checks pass. No auto-restart from this tail: the legitimate
    # auto-restart triggers (OFFLINE, SCHEDULABLE) are paired at the call
    # site that flips the status (set_node_offline, set_node_schedulable).
    #
    # UNREACHABLE / DOWN → ONLINE: a node in either of these states that
    # now passes ping / SnodeAPI / spdk_process_is_up / RPC / port checks
    # has SPDK alive end-to-end and its client-facing port reachable.
    # Peer JM keep-alive / NVMe controller reconnect rebuilds the
    # data-plane links on their own — no destructive restart needed.
    # We flip ONLINE here because nothing else closes the loop for a
    # transient mgmt-plane blip (UNREACHABLE) or a transient port flap
    # (DOWN, e.g. peer-restart cascade). OFFLINE and SCHEDULABLE are
    # intentionally NOT cleared here — those have dedicated recovery
    # via auto-restart.
    if snode.status in (StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_DOWN):
        logger.info(
            "Node %s health checks pass after %s; "
            "clearing to ONLINE (SPDK alive; peer keep-alive reconnects)",
            snode.get_id(), snode.status,
        )
        storage_node_ops.set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE)


def loop_for_node(snode):
    # global logger
    # logger = logging.getLogger()
    # logger_handler = logging.StreamHandler(stream=sys.stdout)
    # logger_handler.setFormatter(logging.Formatter(f'%(asctime)s: node:{snode.mgmt_ip} %(levelname)s: %(message)s'))
    # logger.addHandler(logger_handler)
    while True:
        check_node(snode)
        logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
        time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    logger.info("Starting node monitor")
    threads_maps: dict[str, threading.Thread] = {}

    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cluster in clusters:
            cluster_id = cluster.get_id()
            if cluster.status == Cluster.STATUS_IN_ACTIVATION:
                logger.info(f"Cluster status is: {cluster.status}, skipping monitoring")
                continue
            logger.info(f"Looping for cluster {cluster_id}")
            nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
            for node in nodes:
                node_id = node.get_id()
                if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                    logger.info(f"Creating thread for node {node_id}")
                    t = threading.Thread(target=loop_for_node, args=(node,))
                    t.start()
                    threads_maps[node_id] = t
                    logger.debug(threads_maps[node_id])

            try:
                update_cluster_status(cluster_id)
                logger.debug("Iteration has been finished...")
            except Exception:
                logger.error("Error while updating cluster status")
        time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
