# coding=utf-8
import datetime
import logging
import socket
import time
import uuid

from simplyblock_core import db_controller, constants, utils
from simplyblock_core.controllers import tasks_events, device_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()
db = db_controller.DBController()

# Identity used for task leases. Hostname (not pid) so a runner that crashes
# and restarts on the same host re-claims its own in-flight tasks immediately.
_RUNNER_HOST = socket.gethostname()


def _task_lease_is_stale(task):
    """True if the task's lease (its last write) is older than the TTL, i.e.
    the owning runner host is presumed dead and another host may take over."""
    if not task.updated_at:
        return True
    try:
        last = datetime.datetime.fromisoformat(task.updated_at)
    except (ValueError, TypeError):
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=datetime.timezone.utc)
    age = (datetime.datetime.now(datetime.timezone.utc) - last).total_seconds()
    return age > constants.TASK_LEASE_TTL_SEC


def claim_task(task, owner=None):
    """Atomically claim a task for this runner host before executing it.

    Returns True if this host now holds the lease and may run the task, or
    False if another still-alive host owns it (caller must skip it this cycle).

    The lease is keyed by hostname and refreshed (via updated_at) on every
    claim and on every task write. A second runner replica on a *different*
    host is locked out until the lease goes stale (constants.TASK_LEASE_TTL_SEC),
    which is what prevents two replicas from both executing the same
    side-effecting task during a rolling deploy or a transient dual-manager
    window. A runner on the *same* host always wins immediately, so the common
    single-replica deployment is unaffected (this gate returns True).

    Done/canceled tasks are never claimed.
    """
    owner = owner or _RUNNER_HOST
    decision = {"won": False}
    now = str(datetime.datetime.now(datetime.timezone.utc))

    def _mutate(t):
        if t.status == JobSchedule.STATUS_DONE:
            return False  # not claimable; decision stays False
        if t.owner and t.owner != owner and not _task_lease_is_stale(t):
            return False  # owned by another live host
        t.owner = owner
        t.updated_at = now  # refresh the lease (atomic_update bypasses write_to_db)
        decision["won"] = True
        return True

    if db.atomic_update(task, _mutate) is None:
        return False
    return decision["won"]


def _validate_new_task_dev_restart(cluster_id, node_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_RESTART and task.device_id == device_id and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
        elif task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _validate_new_task_node_restart(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.get_id()
    return False


def _add_task(function_name, cluster_id, node_id, device_id,
              max_retry=constants.TASK_EXEC_RETRY_COUNT, function_params=None, send_to_cluster_log=True):

    # NOTE on expansion: migration-family tasks (device / new-device /
    # failed-device / lvol migration) may be QUEUED while the cluster is in
    # expansion — e.g. an unexpected node outage during the rebalance queues
    # its recovery migrations — but they do not RUN: every migration runner
    # defers while the cluster is not ACTIVE/DEGRADED/READONLY AND while a
    # cluster-expand task is open. Refusing creation here (an earlier
    # revision did) silently lost the outage's recovery work. Ordering after
    # an expansion: expansion completes -> outage device migration drains ->
    # expansion (new-device) migration runs (see tasks_runner_new_dev_migration).

    if function_name in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_FAILED_DEV_MIG]:
        if not _validate_new_task_dev_restart(cluster_id, node_id, device_id):
            return False

    if function_name == JobSchedule.FN_NODE_RESTART:
        task_id = _validate_new_task_node_restart(cluster_id, node_id)
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_NEW_DEV_MIG:
        task_id = get_new_device_mig_task(cluster_id, node_id, function_params['distr_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_DEV_MIG:
        task_id = get_device_mig_task(cluster_id, node_id, device_id, function_params['distr_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_JC_COMP_RESUME:
        task_id = get_jc_comp_task(cluster_id, node_id, function_params['jm_vuid'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_PORT_ALLOW:
        task_id = get_port_allow_tasks(cluster_id, node_id, function_params['port_number'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_LVOL_SYNC_DEL:
        task_id = get_lvol_sync_del_task(cluster_id, node_id, function_params['lvol_bdev_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_LVOL_SYNC_OP:
        task_id = get_lvol_sync_op_task(cluster_id, node_id,
                                        function_params['lvol_id'], function_params['op'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_LVOL_MIG:
        task_id = get_active_lvol_mig_task(cluster_id, function_params.get("lvol_id"))
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    elif function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
        task_id = get_snapshot_replication_task(
            cluster_id, function_params['snapshot_id'], function_params['replicate_to_source'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    elif function_name == JobSchedule.FN_REPLICATION_FINAL:
        # One replication cutover per volume at a time.
        task_id = get_active_replication_final_task(cluster_id, function_params.get("lvol_id"))
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    elif function_name == JobSchedule.FN_CLUSTER_EXPAND:
        # One expansion per cluster at a time: the orchestrator freezes the
        # role rotation while it runs, so a second concurrent expansion would
        # plan against a moving target.
        task_id = get_active_cluster_expand_task(cluster_id)
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    task_obj = JobSchedule()
    task_obj.uuid = str(uuid.uuid4())
    task_obj.cluster_id = cluster_id
    task_obj.node_id = node_id
    task_obj.device_id = device_id
    task_obj.date = int(time.time())
    task_obj.function_name = function_name
    if function_params and type(function_params) is dict:
        task_obj.function_params = function_params
    task_obj.max_retry = max_retry
    task_obj.status = JobSchedule.STATUS_NEW
    task_obj.write_to_db(db.kv_store)
    if send_to_cluster_log:
        tasks_events.task_create(task_obj)
    return task_obj.uuid


def add_device_mig_task_for_node(node_id):
    sub_tasks = []
    node = db.get_storage_node_by_id(node_id)
    cluster_id = node.cluster_id
    master_task = None
    for task in  db.get_job_tasks(cluster_id):
        if task.function_name == JobSchedule.FN_BALANCING_AFTER_NODE_RESTART :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                logger.info("Master task found, skip adding new master task")
                master_task = task
                break

    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue

        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                task_id = _add_task(JobSchedule.FN_DEV_MIG, cluster_id, node.get_id(), bdev['name'],
                          max_retry=-1, function_params={'distr_name': bdev['name']}, send_to_cluster_log=False)
                if task_id:
                    sub_tasks.append(task_id)
    if sub_tasks:
        if master_task:
            master_task.sub_tasks.extend(sub_tasks)
            master_task.write_to_db()
        else:
            task_obj = JobSchedule()
            task_obj.uuid = str(uuid.uuid4())
            task_obj.cluster_id = cluster_id
            task_obj.date = int(time.time())
            task_obj.function_name = JobSchedule.FN_BALANCING_AFTER_NODE_RESTART
            task_obj.sub_tasks = sub_tasks
            task_obj.status = JobSchedule.STATUS_NEW
            task_obj.write_to_db(db.kv_store)
            tasks_events.task_create(task_obj)
        return True


def add_device_to_auto_restart(device):
    return _add_task(JobSchedule.FN_DEV_RESTART, device.cluster_id, device.node_id, device.get_id())


def is_suspension_operator_caused(cluster):
    """True when the cluster's SUSPENDED state is explained by deliberate
    operator node shutdowns (``auto_restart_disabled`` markers, set by
    ``sn shutdown``): without those nodes, the remaining not-online nodes
    alone would still be within the cluster's fault tolerance.

    In that case the automated suspend recovery (drain force-shutdown of the
    surviving nodes + full parallel restart + reactivation) must NOT run — it
    would fight an intentional shutdown by killing and restarting the healthy
    nodes. Recovery is the operator's call: restart the stopped nodes, or run
    ``cluster restart`` (which clears the markers and re-arms recovery).

    A suspension where the non-deliberate outages already exceed FTT is a
    genuine failure regardless of any deliberate shutdowns, and auto recovery
    proceeds."""
    if cluster.status != Cluster.STATUS_SUSPENDED:
        return False
    deliberate_down = 0
    other_not_online = 0
    for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
        if node.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_REMOVED):
            continue
        if node.auto_restart_disabled:
            deliberate_down += 1
        else:
            other_not_online += 1
    ftt = cluster.max_fault_tolerance if isinstance(cluster.max_fault_tolerance, int) \
        and cluster.max_fault_tolerance >= 1 else 1
    return deliberate_down > 0 and other_not_online <= ftt


def is_auto_restart_paused(cluster):
    """Auto-restart is paused while a SUSPENDED cluster is still being drained
    to a clean all-offline slate by the suspend-recovery auto-shutdown
    (storage_node_monitor). Recovering a suspended cluster by restarting nodes
    piecemeal — while others are still up or half-initialized — is what strands
    it (e.g. a node left lvstore_status "in_creation" that then never gets
    health-checked). So we hold every restart until the drain is complete
    (cluster.suspend_drain_complete), then let the existing auto-restart bring
    the nodes back from offline. Used by both the queue chokepoint
    (add_node_to_auto_restart) and the restart task runner.

    Exception: an operator-caused suspension (see
    ``is_suspension_operator_caused``) never drains, so the drain marker would
    pause restarts forever. The surviving nodes are still up in that state —
    restarting a genuinely-failed node one-by-one onto up peers is the normal
    restart path, not the strand-prone post-drain path — so don't pause."""
    return (cluster.status == Cluster.STATUS_SUSPENDED
            and not cluster.suspend_drain_complete
            and not is_suspension_operator_caused(cluster))


def add_node_to_auto_restart(node):
    # Auto-restart kills SPDK and runs the full recreate path. Only states
    # where SPDK itself is the problem warrant that:
    #   - OFFLINE: SnodeAPI confirmed SPDK is gone.
    #   - SCHEDULABLE: SPDK RPC double-timed-out — SPDK is sick.
    #
    # UNREACHABLE does NOT trigger auto-restart: while UNREACHABLE we cannot
    # reach SnodeAPI to perform the restart anyway. Once the node is
    # reachable again, check_node naturally drops it to OFFLINE (if SPDK
    # died) — which then triggers auto-restart — or flips it back to
    # ONLINE (if SPDK was alive throughout).
    #
    # DOWN does NOT trigger auto-restart: SPDK is still up and
    # cluster-internal traffic works; only the client-facing port is
    # blocked. Recovery is port-unblock, not a destructive kill-and-replay.
    _AUTO_RESTART_OK = (
        StorageNode.STATUS_OFFLINE,
        StorageNode.STATUS_SCHEDULABLE,
    )
    # Re-fetch from DB: callers commonly do `set_node_status(...,
    # OFFLINE/SCHEDULABLE)` immediately before this and pass their stale
    # local node object whose .status is still ONLINE — which would trip
    # the guard below and silently drop the restart.
    node = db.get_storage_node_by_id(node.get_id())
    if node.status not in _AUTO_RESTART_OK:
        logger.warning(
            "Refusing to queue auto-restart for node %s in status %s "
            "(only OFFLINE / SCHEDULABLE are valid triggers)",
            node.get_id(), node.status,
        )
        return False

    # A node stopped deliberately via `sn shutdown` (CLI/API, ±--force) must
    # never be auto-restarted — it stays stopped until an operator brings it
    # back. This is the single chokepoint for every auto-restart queue path
    # (set_node_offline, set_node_schedulable, the monitor's re-queue scan,
    # device_monitor, the restart runner's give-up re-queue), so guarding it
    # here closes all of them. The flag is cleared in set_node_status() once
    # the node deliberately returns ONLINE.
    if node.auto_restart_disabled:
        logger.info(
            "Refusing auto-restart for node %s: it was deliberately shut down "
            "(auto_restart_disabled); only an explicit restart can bring it back",
            node.get_id(),
        )
        return False

    cluster = db.get_cluster_by_id(node.cluster_id)
    # Suspended cluster: hold every auto-restart until the suspend-recovery
    # auto-shutdown has drained the whole cluster offline. Restarting nodes
    # one-by-one before the drain completes is exactly what wedged the cluster
    # (stale ONLINE peers / stuck lvstore "in_creation"). Once
    # suspend_drain_complete flips true, the normal path below runs.
    if is_auto_restart_paused(cluster):
        logger.info(
            "Cluster %s is SUSPENDED and not yet drained; pausing auto-restart "
            "for node %s until all nodes are offline",
            node.cluster_id, node.get_id())
        return False
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
                              Cluster.STATUS_READONLY, Cluster.STATUS_UNREADY, Cluster.STATUS_SUSPENDED]:
        logger.warning(f"Cluster is not active, skip node auto restart, status: {cluster.status}")
        return False
    # Past-fault-tolerance guard: don't auto-restart nodes one-by-one when
    # more than the cluster can tolerate is already offline — that churn is
    # what wedged the cluster before (stale ONLINE peers / stuck lvstore
    # in_creation), and the SUSPENDED-drain path (above) owns that case.
    #
    # BUT this raw ``offline_nodes > distr_npcs`` count is failure-domain
    # blind. On a failure-domain cluster a WHOLE domain going offline (e.g.
    # a 1+1 cluster losing all 16 nodes of one domain) is the *tolerated*
    # case: the FD-aware status logic keeps the cluster DEGRADED (never
    # SUSPENDED), so `offline_nodes(15) > npcs(1)` trips and, because it is
    # not SUSPENDED, blocks auto-restart for every node in the domain —
    # permanently, since nothing will ever move it to SUSPENDED. Result:
    # the whole rebooted domain never restarts (incident 2026-07-08,
    # 32-node/2-domain/1+1 whole-domain reboot soak).
    #
    # For a failure-domain cluster the cluster STATUS already encodes
    # tolerance (the FD-aware get_next_cluster_status returns DEGRADED when
    # the loss is within the domain budget, SUSPENDED when it exceeds it).
    # So when enable_failure_domain is set, trust the status: ACTIVE/DEGRADED
    # means "tolerated, go ahead and restart"; the SUSPENDED-not-drained case
    # is already held by is_auto_restart_paused above. Only apply the flat
    # node-count guard to non-FD clusters.
    if not cluster.enable_failure_domain:
        offline_nodes = 0
        for sn in db.get_storage_nodes_by_cluster_id(node.cluster_id):
            if node.get_id() != sn.get_id() and sn.status != StorageNode.STATUS_ONLINE and node.mgmt_ip != sn.mgmt_ip:
                offline_nodes += 1
        if offline_nodes > cluster.distr_npcs and cluster.status != Cluster.STATUS_SUSPENDED:
            logger.info("Node found that is not online, skip node auto restart")
            return False
    return _add_task(JobSchedule.FN_NODE_RESTART, node.cluster_id, node.get_id(), "", max_retry=11)


def cancel_pending_node_restart_tasks(cluster_id, node_id):
    # Called from set_node_status the moment a node transitions to ONLINE.
    # Without this, an obsolete FN_NODE_RESTART row left over from the
    # outage stays in `new`/`running` and blocks every subsequent restart
    # via the dedup guard in `_validate_new_task_node_restart` until the
    # task runner happens to pick it up — observed as a 5-minute window
    # of failing manual restarts after the node was already back online.
    canceled = 0
    for task in db.get_job_tasks(cluster_id):
        if (task.function_name == JobSchedule.FN_NODE_RESTART
                and task.node_id == node_id
                and task.status != JobSchedule.STATUS_DONE
                and not task.canceled):
            task.canceled = True
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "canceled: node back online"
            task.write_to_db(db.kv_store)
            canceled += 1
            logger.info(
                f"Canceled obsolete node_restart task {task.get_id()} (node {node_id} back online)")
    return canceled


def list_tasks(cluster_id, is_json=False, limit=50, **kwargs):
    try:
        db.get_cluster_by_id(cluster_id)
    except KeyError:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    data = []
    tasks = db.get_job_tasks(cluster_id, reverse=True)
    tasks.reverse()
    if is_json is True:
        for t in tasks:
            if t.function_name == JobSchedule.FN_DEV_MIG:
                continue
            data.append(t.get_clean_dict())
            if len(data)+1 > limit > 0:
                return utils.dump_json(data, indent=2, unwrap_secrets=True)
        return utils.dump_json(data, indent=2, unwrap_secrets=True)

    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_MIG:
            continue
        logger.debug(task)
        if task.max_retry > 0:
            retry = f"{task.retry}/{task.max_retry}"
        else:
            retry = f"{task.retry}"
        logger.debug(task)
        upd = task.updated_at
        if upd:
            try:
                parsed = datetime.datetime.fromisoformat(upd)
                upd = parsed.strftime("%H:%M:%S, %d/%m/%Y")
            except Exception as e:
                logger.error(e)

        if task.sub_tasks:
            target_id = f"Master task for {len(task.sub_tasks)} subtasks"

        else:
            target_id = f"NodeID:{task.node_id}"
            if task.device_id:
                target_id += f"\nDeviceID:{task.device_id}"

        data.append({
            "Task ID": task.uuid,
            "Target ID": target_id,
            "Function": task.function_name,
            "Retry": retry,
            "Status": task.status,
            "Result": task.function_result,
            "Updated At": upd or "",
        })
        if len(data)+1 > limit > 0:
            return utils.print_table(data, unwrap_secrets=True)
    return utils.print_table(data, unwrap_secrets=True)


def cancel_task(task_id):
    try:
        task = db.get_task_by_id(task_id)
    except KeyError as e:
        logger.error(e)
        return False

    if task.sub_tasks:
        logger.error("Can not cancel master task")
        return False

    if task.device_id:
        device_controller.device_set_retries_exhausted(task.device_id, True)

    task.canceled = True
    task.write_to_db(db.kv_store)
    tasks_events.task_canceled(task)
    return True


def get_subtasks(master_task_id):
    master_task = db.get_task_by_id(master_task_id)
    data = []
    tasks = {t.uuid: t for t in db.get_job_tasks(master_task.cluster_id)}
    for sub_task_id in master_task.sub_tasks:
        sub_task = tasks[sub_task_id]
        if sub_task.max_retry > 0:
            retry = f"{sub_task.retry}/{sub_task.max_retry}"
        else:
            retry = f"{sub_task.retry}"

        upd = sub_task.updated_at
        if upd:
            try:
                parsed = datetime.datetime.fromisoformat(upd)
                upd = parsed.strftime("%H:%M:%S, %d/%m/%Y")
            except Exception as e:
                logger.error(e)

        logger.debug(sub_task)
        data.append({
            "Task ID": sub_task.uuid,
            "Node ID": sub_task.node_id,
            "Distrib": sub_task.device_id,
            "Function": sub_task.function_name,
            "Retry": retry,
            "Status": sub_task.status,
            "Result": sub_task.function_result,
            "Updated At": upd or "",
        })
    return utils.print_table(data)


def get_active_node_restart_task(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def get_active_dev_restart_task(cluster_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_RESTART and task.device_id == device_id:
            if task.status == JobSchedule.STATUS_RUNNING and task.canceled is False:
                return task.uuid
    return False


def get_active_node_mig_task(cluster_id, node_id, distr_name=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name in [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_DEV_MIG,
                                  JobSchedule.FN_NEW_DEV_MIG] and task.node_id == node_id:
            if task.status == JobSchedule.STATUS_RUNNING and task.canceled is False:
                if distr_name:
                    if "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                        return task.uuid
                else:
                    return task.uuid
    return False




def add_device_failed_mig_task(device_id):
    device = db.get_storage_device_by_id(device_id)
    for node in db.get_storage_nodes_by_cluster_id(device.cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                _add_task(JobSchedule.FN_FAILED_DEV_MIG, device.cluster_id, node.get_id(), device.get_id(),
                          max_retry=-1, function_params={'distr_name': bdev['name']})
    return True


def add_new_device_mig_task(device_id):
    device = db.get_storage_device_by_id(device_id)
    for node in db.get_storage_nodes_by_cluster_id(device.cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                _add_task(JobSchedule.FN_NEW_DEV_MIG, device.cluster_id, node.get_id(), device.get_id(),
                          max_retry=-1, function_params={'distr_name': bdev['name']})
    return True


def add_node_add_task(cluster_id, function_params):
    return _add_task(JobSchedule.FN_NODE_ADD, cluster_id, "", "",
                     function_params=function_params, max_retry=11)


def add_cluster_expand_task(cluster_id, new_node_id):
    """Queue a single-node cluster-expansion task. The runner drives the
    planner/orchestrator/executor to integrate ``new_node_id`` into the
    role rotation, resuming from a persisted cursor across retries.

    max_retry=-1 (never give up): an unexpected node outage mid-expansion
    can hold the plan suspended for many backoff cycles, but a half-moved
    topology MUST complete — abandoning it strands stale sec/tert
    pointers. The task is cancellable by the operator if truly stuck."""
    return _add_task(
        JobSchedule.FN_CLUSTER_EXPAND, cluster_id, new_node_id, "",
        function_params={"new_node_id": new_node_id}, max_retry=-1)


def get_active_cluster_expand_task(cluster_id):
    """Return the UUID of an active (non-done, non-cancelled) cluster
    expansion task for the cluster, or False if none."""
    for task in db.get_job_tasks(cluster_id):
        if task.function_name == JobSchedule.FN_CLUSTER_EXPAND \
                and task.canceled is False \
                and task.status != JobSchedule.STATUS_DONE:
            return task.uuid
    return False


def defer_task_for_expansion(task):
    """Suspend ``task`` if a cluster expansion is in progress. Returns True
    when deferred.

    Migration-family runners call this right after their cluster-status
    gate. The status gate alone is not enough: a node outage mid-expansion
    suspends the cluster-expand task and restores the cluster status to
    ACTIVE between its retries — without this gate the outage's recovery
    migrations would start running in that window and then block the
    expansion resume, inverting the required order (expansion completes
    FIRST, then outage device migration, then expansion migration).
    Deliberately does not consume a retry: this is a deferral, not a
    failure."""
    if not get_active_cluster_expand_task(task.cluster_id):
        return False
    task.function_result = "cluster expansion in progress, deferring"
    task.status = JobSchedule.STATUS_SUSPENDED
    task.write_to_db(db.kv_store)
    return True


def get_active_node_tasks(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    out = []
    for task in tasks:
        if task.function_name in [JobSchedule.FN_PORT_ALLOW, JobSchedule.FN_JC_COMP_RESUME]:
            continue
        if task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                out.append(task)
    return out


def get_new_device_mig_task(cluster_id, node_id, distr_name, dev_id=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NEW_DEV_MIG and task.node_id == node_id:
            if dev_id:
                if task.device_id != dev_id:
                    continue
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False \
                    and "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                return task.uuid
    return False


def get_device_mig_task(cluster_id, node_id, device_id, distr_name):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_MIG and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False \
                    and "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                return task.uuid
    return False


def get_new_device_mig_task_for_device(cluster_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NEW_DEV_MIG:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def get_failed_device_mig_task(cluster_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_FAILED_DEV_MIG and task.device_id == device_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def add_port_allow_task(cluster_id, node_id, port_number):
    return _add_task(JobSchedule.FN_PORT_ALLOW, cluster_id, node_id, "", function_params={"port_number": port_number})


def get_port_allow_tasks(cluster_id, node_id, port_number):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_PORT_ALLOW and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if "port_number" in task.function_params and task.function_params["port_number"] == port_number:
                    return task.uuid
    return False


def add_jc_comp_resume_task(cluster_id, node_id, jm_vuid):
    return _add_task(JobSchedule.FN_JC_COMP_RESUME, cluster_id, node_id, "",
                     function_params={"jm_vuid": jm_vuid}, max_retry=10)


def get_jc_comp_task(cluster_id, node_id, jm_vuid=0):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_JC_COMP_RESUME and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if jm_vuid and "jm_vuid" in task.function_params and task.function_params["jm_vuid"] == jm_vuid:
                    return task.uuid
    return False


def get_active_lvol_mig_task(cluster_id, lvol_id):
    """Return the UUID of an active (non-done, non-cancelled) lvol migration task."""
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_MIG and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                if task.function_params.get("lvol_id") == lvol_id:
                    return task.uuid
    return False


def get_active_lvol_mig_task_on_node(cluster_id, node_id):
    """Return the UUID of an active lvol migration task on a given source node."""
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_MIG and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def add_lvol_mig_task(migration):
    """Create the JobSchedule task that drives a live volume migration."""
    return _add_task(
        JobSchedule.FN_LVOL_MIG,
        migration.cluster_id,
        migration.source_node_id,
        "",
        max_retry=migration.max_retries,
        function_params={
            "migration_id": migration.uuid,
            "lvol_id": migration.lvol_id,
            "target_node_id": migration.target_node_id,
        },
    )


def add_batch_mig_task(group):
    """Create the JobSchedule task that drives a batch (shared-namespace) migration."""
    return _add_task(
        JobSchedule.FN_LVOL_BATCH_MIG,
        group.cluster_id,
        group.source_node_id,
        "",
        function_params={
            "group_id": group.uuid,
            "target_node_id": group.target_node_id,
        },
    )


def add_lvol_sync_del_task(cluster_id, node_id, lvol_bdev_name, primary_node):
    return _add_task(JobSchedule.FN_LVOL_SYNC_DEL, cluster_id, node_id, "",
                     function_params={"lvol_bdev_name": lvol_bdev_name, "primary_node": primary_node}, max_retry=10)


def get_lvol_sync_op_task(cluster_id, node_id, lvol_id, op):
    for task in db.get_job_tasks(cluster_id):
        if task.function_name == JobSchedule.FN_LVOL_SYNC_OP and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if (task.function_params.get("lvol_id") == lvol_id
                        and task.function_params.get("op") == op):
                    return task.uuid
    return False


def add_lvol_sync_op_task(cluster_id, node_id, lvol_id, op, secondary_index=0):
    """DB-backed deferred per-node lvol operation (``op`` is ``"register"``
    or ``"resize"``).

    Replaces the in-memory ``_restart_op_queues`` deferral for lvol
    create/resize registrations on non-leaders: that queue is a
    module-level dict — per process (webappapi queues, the restart runner
    drains only its OWN copy on phase transitions) and gone on process
    restart. Incident 2026-07-10 lost a tertiary create-registration that
    way and a dual outage within FTT killed all IO paths. A JobSchedule
    task survives process boundaries and restarts; the sync-op runner
    (tasks_runner_sync_lvol_del service) applies it once the node is
    ONLINE, the lvol settled, and no restart owns the LVS. The op is
    idempotent, and obsolescence (lvol deleted) completes the task."""
    return _add_task(JobSchedule.FN_LVOL_SYNC_OP, cluster_id, node_id, "",
                     function_params={"lvol_id": lvol_id, "op": op,
                                      "secondary_index": secondary_index},
                     max_retry=-1)


def run_lvol_sync_op_task(task):
    """Execute one FN_LVOL_SYNC_OP task (called by the sync runner's loop;
    lives here so it is importable without triggering the runner's
    module-level loop). Idempotent; never raises."""
    from simplyblock_core import storage_node_ops
    from simplyblock_core.models.lvol_model import LVol

    def _finish(result):
        task.function_result = result
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

    def _defer(result):
        task.function_result = result
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)

    try:
        if task.canceled:
            _finish("canceled")
            return

        lvol_id = task.function_params.get("lvol_id")
        op = task.function_params.get("op")
        try:
            lvol = db.get_lvol_by_id(lvol_id)
        except KeyError:
            _finish("lvol no longer exists")
            return
        if lvol.status == LVol.STATUS_IN_DELETION:
            _finish("lvol is being deleted")
            return
        if lvol.status != LVol.STATUS_ONLINE:
            _defer(f"lvol status is {lvol.status}, retrying")
            return

        try:
            node = db.get_storage_node_by_id(task.node_id)
        except KeyError:
            _finish("node no longer exists")
            return
        if node.get_id() not in lvol.nodes:
            _finish("node no longer hosts this lvol (topology moved)")
            return
        if node.status != StorageNode.STATUS_ONLINE:
            _defer(f"node is {node.status}, retrying")
            return
        if storage_node_ops.get_restart_phase(task.node_id, lvol.lvs_name):
            # The owning flow (restart/activation/expansion) re-registers
            # lvols itself; re-check once it has released the LVS.
            _defer("LVS owned by a restart/activation/expansion, retrying")
            return

        if task.status != JobSchedule.STATUS_RUNNING:
            task.status = JobSchedule.STATUS_RUNNING
            task.write_to_db(db.kv_store)

        if op == "register":
            ok, err = storage_node_ops.repair_lvol_registration_on_non_leader(
                lvol, node, task.function_params.get("secondary_index", 0))
            if ok:
                _finish(f"registered lvol {lvol_id} on {task.node_id}")
            else:
                _defer(f"registration failed: {err}")
        elif op == "resize":
            # Converge to the CURRENT DB size — resize_lvol persists the new
            # size after the fan-out, so this always applies the latest
            # target even if the lvol was resized again meanwhile.
            size_in_mib = utils.convert_size(lvol.size, 'MiB')
            ret = node.rpc_client(timeout=10, retry=2).bdev_lvol_resize(
                f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            if ret:
                _finish(f"resized lvol {lvol_id} on {task.node_id} to {size_in_mib} MiB")
            else:
                _defer("resize RPC failed, retrying")
        else:
            _finish(f"unknown op {op!r}")
    except Exception as e:
        logger.error(f"lvol sync-op task {task.uuid} failed: {e}")
        try:
            _defer(f"error: {e}")
        except Exception:
            pass

def get_lvol_sync_del_task(cluster_id, node_id, lvol_bdev_name=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_SYNC_DEL and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if lvol_bdev_name:
                    if "lvol_bdev_name" in task.function_params and task.function_params["lvol_bdev_name"] == lvol_bdev_name:
                        return task.uuid
                else:
                    return task.uuid
    return False

def get_snapshot_replication_task(cluster_id, snapshot_id, replicate_to_source):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and task.function_params["snapshot_id"] == snapshot_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if task.function_params["replicate_to_source"] == replicate_to_source:
                    return task.uuid
    return False


def add_backup_task(backup):
    """Create the task that performs an S3 backup."""
    return _add_task(
        JobSchedule.FN_BACKUP,
        backup.cluster_id,
        backup.node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "backup_id": backup.uuid,
        },
    )


def add_backup_restore_task(cluster_id, node_id, backup_id, lvol_name, chain_ids, lvol_id=""):
    """Create the task that restores an S3 backup chain into a new lvol."""
    return _add_task(
        JobSchedule.FN_BACKUP_RESTORE,
        cluster_id,
        node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "backup_id": backup_id,
            "lvol_name": lvol_name,
            "lvol_id": lvol_id,
            "chain_ids": chain_ids,
        },
    )


def add_backup_merge_task(cluster_id, node_id, keep_backup_id, old_backup_id):
    """Create the task that merges two S3 backups."""
    return _add_task(
        JobSchedule.FN_BACKUP_MERGE,
        cluster_id,
        node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "keep_backup_id": keep_backup_id,
            "old_backup_id": old_backup_id,
        },
    )


def _check_snap_instance_on_node(snapshot_id: str , node_id: str):
    snapshot = db.get_snapshot_by_id(snapshot_id)
    for sn_inst in snapshot.instances:
        if sn_inst["lvol"]["node_id"] == node_id:
            logger.info("Snapshot instance found on node, skip adding replication task")
            return

    if snapshot.snap_ref_id:
        prev_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
        _check_snap_instance_on_node(prev_snap.get_id(), node_id)

    _add_task(JobSchedule.FN_SNAPSHOT_REPLICATION, snapshot.cluster_id, node_id, "",
              function_params={"snapshot_id": snapshot.get_id(), "replicate_to_source": False,
                               "replicate_as_snap_instance": True},
              send_to_cluster_log=False)


def add_snapshot_replication_task(cluster_id, node_id, snapshot_id, replicate_to_source=False):
    if not replicate_to_source:
        snapshot = db.get_snapshot_by_id(snapshot_id)
        if snapshot.snap_ref_id:
            prev_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
            _check_snap_instance_on_node(prev_snap.get_id(), node_id)

    return _add_task(JobSchedule.FN_SNAPSHOT_REPLICATION, cluster_id, node_id, "",
                     function_params={"snapshot_id": snapshot_id, "replicate_to_source": replicate_to_source},
                     send_to_cluster_log=False)


def get_active_replication_final_task(cluster_id, lvol_id):
    """Return the UUID of an active (non-done, non-cancelled) replication
    cutover task for *lvol_id*, or False."""
    for task in db.get_job_tasks(cluster_id):
        if task.function_name == JobSchedule.FN_REPLICATION_FINAL and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE and task.function_params.get("lvol_id") == lvol_id:
                return task.uuid
    return False


def add_replication_final_task(cluster_id, src_node_id, function_params):
    """Create the JobSchedule task that drives a cross-cluster replication
    cutover (freeze + final delta + ANA flip).

    function_params must carry: lvol_id, src_node_id, tgt_node_id,
    tgt_lvol_composite, tgt_map_id, tgt_snap_composite, operation, replication_id,
    final_state.
    """
    return _add_task(JobSchedule.FN_REPLICATION_FINAL, cluster_id, src_node_id, "",
                     function_params=function_params, send_to_cluster_log=False)
