# coding=utf-8
import time
from concurrent.futures import ThreadPoolExecutor

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, health_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClientException


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()


def _get_node_unavailable_devices_count(node_id):
    node = db.get_storage_node_by_id(node_id)
    devices = []
    for dev in node.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            devices.append(dev)
    return len(devices)


def _get_device(task):
    node = db.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.get_id() == task.device_id:
            return dev


def _validate_no_task_node_restart(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _ensure_spdk_killed(node):
    """Best-effort kill of the SPDK process on the node before we mark it
    OFFLINE. Without this, flipping the status to OFFLINE while SPDK is still
    running produces a DB-vs-data-plane split: the DB says the node is not
    serving, but SPDK is actually still serving IO — and a subsequent
    restart_storage_node would spin up a second SPDK on top.

    Returns True if we are confident the data plane is not serving (SPDK
    killed successfully, or the node API is unreachable which implies the
    process is also unreachable). Returns False only when the node API is
    reachable but spdk_process_kill raised — in that narrow case we don't
    know for sure whether SPDK is gone, so the caller should leave the DB
    state as-is and let a later attempt retry.
    """
    if not health_controller._check_node_api(node):
        # Node API is down; the SPDK process on the same host is not reachable
        # to serve IO either. Safe to proceed.
        logger.info(
            f"Node {node.get_id()} API unreachable at {node.mgmt_ip}:5000; "
            f"assuming SPDK is not serving"
        )
        return True

    # Short-circuit when the SPDK container is already gone (common after a
    # `docker kill spdk_*`: by the time this task body runs, SNodeAPI reports
    # the container in `exited` state).  Skipping the kill RPC avoids a ~30 s
    # retry-then-timeout cycle on an already-dead container.
    try:
        client = node.client(timeout=5, retry=2)
        is_up, _ = client.spdk_process_is_up(node.rpc_port, node.cluster_id)
        if not is_up:
            logger.info(
                f"SPDK on {node.get_id()} already not running; skipping kill"
            )
            return True
    except Exception as exc:
        # If the probe itself fails, fall through and try the kill — it's
        # the conservative path (better to over-kill than leave SPDK serving).
        logger.warning(
            f"spdk_process_is_up probe failed on {node.get_id()}: {exc}; "
            f"proceeding with kill"
        )

    try:
        logger.info(f"Killing SPDK on node {node.get_id()} (rpc_port={node.rpc_port})")
        node.client(timeout=10, retry=5).spdk_process_kill(node.rpc_port, node.cluster_id)
    except SNodeClientException as exc:
        logger.error(
            f"Failed to kill SPDK on {node.get_id()}: {exc}; "
            f"leaving DB state unchanged to avoid split-brain"
        )
        return False
    except Exception as exc:
        # Other transport errors — treat as unreachable (process also unreachable).
        logger.warning(
            f"spdk_process_kill transport error on {node.get_id()}: {exc}; "
            f"assuming SPDK is not serving"
        )
        return True

    # Confirm the process is actually gone before reporting success. The kill
    # RPC returns as soon as SIGKILL is *delivered* — the SNodeAPI handler does
    # not wait for the kernel reap or dockerd record cleanup — so trusting its
    # bare return races a subsequent spdk_process_start that would launch a
    # fresh SPDK while the old instance (or its teardown) is still settling.
    # That is the kill/start race behind the 2026-06-03 LVS_8720 zero-leader
    # outage. Poll spdk_process_is_up (a Unix-socket liveness probe) until it
    # reports down, bounded; refuse to declare "killed" if it never does.
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            is_up, _ = node.client(timeout=5, retry=2).spdk_process_is_up(
                node.rpc_port, node.cluster_id)
        except Exception as exc:
            logger.warning(
                f"spdk_process_is_up confirm-probe failed on {node.get_id()}: "
                f"{exc}; assuming SPDK is down"
            )
            return True
        if not is_up:
            logger.info(f"Confirmed SPDK down on {node.get_id()} after kill")
            return True
        time.sleep(2)

    logger.error(
        f"SPDK on {node.get_id()} still up 30s after kill; refusing to report it "
        f"killed (would race a fresh spdk_process_start)"
    )
    return False


def _reset_if_transient(node_id):
    """Roll the node back to STATUS_OFFLINE if a partial shutdown/restart
    left it stuck in an intermediate CP state. Without this, a failed
    attempt leaves the node pinned in STATUS_IN_SHUTDOWN or STATUS_RESTARTING,
    which (a) blocks future restart attempts via the mutual-exclusion guard,
    and (b) causes peers' cluster_map health checks to fail cluster-wide.

    Before flipping to OFFLINE we confirm the SPDK process is not running
    on the node's host — otherwise we'd risk a split-brain where the DB
    says OFFLINE but SPDK is still serving IO.
    """
    try:
        node = db.get_storage_node_by_id(node_id)
    except KeyError:
        return
    if node.status not in (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_RESTARTING):
        return
    logger.warning(
        f"Node {node_id} left in {node.status} after failed restart attempt; "
        f"verifying SPDK is not serving before resetting to OFFLINE"
    )
    if not _ensure_spdk_killed(node):
        logger.error(
            f"Could not confirm SPDK is down on {node_id}; refusing to flip to "
            f"OFFLINE to avoid split-brain. Next retry will attempt again."
        )
        return
    try:
        # Tag as restart_cleanup so the RESTARTING-lock guard in
        # set_node_status admits this transition (we've just verified
        # SPDK is dead, so the lock is no longer protecting anything).
        storage_node_ops.set_node_status(
            node_id, StorageNode.STATUS_OFFLINE, caused_by="restart_cleanup")
        logger.info(f"Node {node_id} reset to OFFLINE (SPDK confirmed down)")
    except Exception as exc:
        logger.error(f"Failed to reset node {node_id} to OFFLINE: {exc}")


def task_runner(task):
    if task.function_name == JobSchedule.FN_DEV_RESTART:
        return task_runner_device(task)
    if task.function_name == JobSchedule.FN_NODE_RESTART:
        return task_runner_node(task)


def task_runner_device(task):
    device = _get_device(task)

    if task.retry >= constants.TASK_EXEC_RETRY_COUNT:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    if not _validate_no_task_node_restart(task.cluster_id, task.node_id):
        task.function_result = "canceled: node restart found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    node = db.get_storage_node_by_id(task.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online: {node.get_id()}, retry")
        task.function_result = "Node is offline"
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "Device is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Device is not unavailable: {device.get_id()}, {device.status} , stopping task")
        task.function_result = f"stopped because dev is {device.status}"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    # set device online for the first 3 retries
    if task.retry < 3:
        logger.info(f"Set device online {device.get_id()}")
        device_controller.device_set_io_error(device.get_id(), False)
        device_controller.device_set_state(device.get_id(), NVMeDevice.STATUS_ONLINE)
    else:
        logger.info(f"Restarting device {device.get_id()}")
        device_controller.restart_device(device.get_id(), force=True)

    # check device status
    time.sleep(5)
    device = _get_device(task)
    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        tasks_controller.add_device_mig_task_for_node(task.node_id)

        return True

    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


def task_runner_node(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        # restart_cleanup: this task ran try_set_node_restarting earlier
        # and is the lock owner; tagging unblocks the RESTARTING-lock
        # guard so the giving-up flip lands.
        storage_node_ops.set_node_status(
            task.node_id, StorageNode.STATUS_OFFLINE, caused_by="restart_cleanup")
        # Re-queue a fresh auto-restart task so the node does not get
        # stranded in OFFLINE forever. Without this, the legitimate
        # auto-restart trigger (set_node_offline) won't fire either —
        # it skips when status is already OFFLINE — so the only path
        # back is operator intervention. Hours-of-backoff exhaustion
        # almost always means a long peer-side recovery is in flight;
        # once it clears, the new task can succeed.
        try:
            node_obj = db.get_storage_node_by_id(task.node_id)
            tasks_controller.add_node_to_auto_restart(node_obj)
        except KeyError:
            pass
        except Exception as exc:
            logger.error(f"Failed to re-queue auto-restart for {task.node_id}: {exc}")
        return True

    if node.status in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_SCHEDULABLE]:
        logger.info(f"Node is {node.status}, stopping task")
        task.function_result = f"Node is {node.status}, stopping"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True
    # DOWN used to short-circuit here too. After removing the monitor's
    # set_node_online (which previously did DOWN -> ONLINE on health-check
    # pass), DOWN must be handled by this runner: shutdown + restart drives
    # the node through IN_RESTART -> ONLINE, which is the only legal path.

    # The node-restart task is meant to fix the NODE, not individual devices.
    # Previously this short-circuit also required `unavailable_devices_count
    # == 0`, which meant a node that was ONLINE but still had any residual
    # UNAVAILABLE device (a routine transient right after an outage — peer
    # nodes call device_set_unavailable on the target's remote-device records
    # and clearing those is decoupled from the target node's own restart
    # completion) would be treated as "still broken", and the runner would
    # slam through another shutdown + restart cycle even though the node was
    # serving IO just fine. That produced visible online → in_shutdown →
    # offline → in_restart cycles.
    #
    # Device-level recovery has its own task type (add_device_to_auto_restart
    # / FN_DEV_RESTART); this one only needs the NODE to be healthy.
    #
    # CRITICAL: short-circuit on ANY ONLINE status, regardless of health_check.
    # health_check=False can be set by the health service for many non-fatal
    # reasons (peer-side device records, port checks, transient lvstore
    # consistency blips). A destructive SPDK kill+restart on a serving node is
    # never the right remedy for those — they have dedicated tasks
    # (FN_DEV_RESTART, FN_PORT_ALLOW, peer-side recreate_lvstore). Requiring
    # health_check==True here caused observable online → in_shutdown → offline
    # cycles when an FN_NODE_RESTART task queued during a legitimate OFFLINE
    # window was consumed later, after the node had come back ONLINE but with
    # a still-False health_check from auxiliary checks.
    if node.status == StorageNode.STATUS_ONLINE:
        logger.info(f"Node is online: {node.get_id()}")
        task.function_result = "Node is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        if node.status == StorageNode.STATUS_RESTARTING:
            logger.info("Node is restarting, stopping task")
            task.function_result = "Node is restarting"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return True
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    # Peer-restart mutual-exclusion pre-check: if any peer is RESTARTING
    # or IN_SHUTDOWN we cannot proceed (try_set_node_restarting in the
    # restart impl uses an FDB-tx with the same predicate and would fail
    # acquisition). This is purely transient — burning a retry on a lock
    # we know we can't acquire just collapses the backoff budget. Return
    # False without incrementing task.retry; the runner's outer loop
    # will sleep with exponential backoff and re-call us. Once the peer
    # finishes its transition, this check passes and we proceed with a
    # fresh budget.
    #
    # Skipped only for a SUSPENDED **and drained** cluster: recovery restarts
    # run in parallel then (see the dispatch loop below) so peers in
    # RESTARTING / IN_SHUTDOWN are expected, not a conflict. The FDB guard in
    # restart_storage_node is relaxed the same way (allow_concurrent_peers).
    # An operator-caused suspension never drains — its survivors still serve
    # IO — so it keeps the full pre-check.
    cluster_obj = db.get_cluster_by_id(node.cluster_id)
    if not (cluster_obj.status == Cluster.STATUS_SUSPENDED
            and cluster_obj.suspend_drain_complete) \
            and not storage_node_ops.fd_dead_recovery_allowed(db, node):
        # Strict one-restart-at-a-time outside the two sanctioned cases:
        # drained suspension, and a fully-dead failure domain
        # (fd_dead_recovery_allowed — no domain member ONLINE, so parallel
        # recovery cannot touch served IO; see the predicate's docstring).
        # The former relaxation that fanned out same-domain restarts while
        # the domain was still SERVING (2026-07-16 violation: parallel
        # in_restart while DEGRADED) remains removed.
        for peer in db.get_storage_nodes_by_cluster_id(node.cluster_id):
            if peer.get_id() == node.get_id():
                continue
            if peer.status in (StorageNode.STATUS_RESTARTING,
                               StorageNode.STATUS_IN_SHUTDOWN):
                msg = (f"Peer {peer.get_id()[:8]} is {peer.status}; "
                       f"deferring (no retry consumed)")
                logger.info(msg)
                task.function_result = msg
                task.write_to_db(db.kv_store)
                return False

    # is node reachable?
    ping_check = health_controller._check_node_ping(node.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
    node_api_check = health_controller._check_node_api(node)
    logger.info(f"Check: node API {node.mgmt_ip}:5000 ... {node_api_check}")
    node_data_nic_ping_check = False
    for data_nic in node.data_nics:
        if data_nic.ip4_address:
            data_ping_check = health_controller._check_ping_from_node(data_nic.ip4_address, ifname=data_nic.if_name, node=node)
            logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping_check}")
            node_data_nic_ping_check |= data_ping_check
    if not ping_check or not node_api_check or not node_data_nic_ping_check:
        # node is unreachable, retry
        logger.info(f"Node is not reachable: {task.node_id}, retry")
        task.function_result = "Node is unreachable, retry"
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False


    # Cleanup shutdown before the restart — but only when there is something
    # to clean: a node that is already OFFLINE had SPDK confirmed gone (that
    # is what put it in OFFLINE), so force-shutting it down again only walks
    # it through a pointless offline -> in_shutdown -> offline cycle. And run
    # it at most ONCE per task: re-running the full shutdown on every retry
    # multiplied the state churn during whole-cluster recovery (2026-07-13:
    # every FDB-contention retry replayed in_shutdown -> offline -> in_restart
    # on all 32 nodes). The once-flag is persisted on the task so it survives
    # runner restarts. A node stuck in a non-OFFLINE state from a dead
    # attempt (e.g. RESTARTING) still gets exactly one cleanup shutdown.
    shutdown_needed = (node.status != StorageNode.STATUS_OFFLINE
                       and not task.function_params.get("cleanup_shutdown_done"))
    shutdown_succeeded = not shutdown_needed
    try:
        if shutdown_needed:
            try:
                # shutting down node
                logger.info(f"Shutdown node {node.get_id()}")
                ret = storage_node_ops.shutdown_storage_node(node.get_id(), force=True)
                if ret:
                    logger.info("Node shutdown succeeded")
                    shutdown_succeeded = True
                    task.function_params = dict(task.function_params)
                    task.function_params["cleanup_shutdown_done"] = True
                    task.write_to_db(db.kv_store)
                else:
                    logger.error("Node shutdown returned False; will retry after reset")
                time.sleep(3)
            except Exception as e:
                logger.error(e)
                return False
        else:
            logger.info(
                f"Skipping cleanup shutdown for {node.get_id()}: "
                f"status={node.status}, "
                f"already_done={bool(task.function_params.get('cleanup_shutdown_done'))}")

        # Skip the restart step if shutdown did not succeed — restarting on top
        # of a half-shutdown node produced the in_restart hang we're guarding
        # against. Let the outer retry reattempt the whole cycle.
        if not shutdown_succeeded:
            task.retry += 1
            task.write_to_db(db.kv_store)
            return False

        try:
            # resetting node
            logger.info(f"Restart node {node.get_id()}")
            # task.uuid, NOT task.get_id(): get_active_node_restart_task (the
            # guard restart_storage_node compares this against) returns the
            # bare uuid, while JobSchedule.get_id() is the composite
            # "cluster/date/uuid" FDB key. Passing the composite here meant
            # the comparison could never match, so this call's own task was
            # never recognized as "ours" — masked only because this call
            # uses force=True, which proceeds past the guard regardless and
            # just logged a spurious "Restart task found" error every time.
            ret = storage_node_ops.restart_storage_node(node.get_id(), force=True, current_restart_task_id=task.uuid)
            if ret:
                logger.info("Node restart succeeded")
        except Exception as e:
            logger.error(e)
            return False

        time.sleep(3)
        node = db.get_storage_node_by_id(task.node_id)
        if ret and node.status == StorageNode.STATUS_RESTARTING:
            # Self-heal for the silent stale-write race (2026-07-21,
            # d3fc2c16): the restart impl SUCCEEDED and committed the
            # in_restart->online CAS, but within ~2.5s a stale full-object
            # node write resurrected status=in_restart — no event, no log
            # (the [NODE-WRITE] tripwire in BaseModel.write_to_db names the
            # writer on the next occurrence). Without this branch the
            # re-read below declares the successful restart failed and the
            # finally-guard kills SPDK on a healthy, serving node — a
            # 2-minute self-inflicted outage per hit. Re-assert ONLINE
            # (atomic CAS; the FSM allows RESTARTING->ONLINE) and continue.
            # A genuinely new concurrent restart would have logged its own
            # guard acquisition + event; none existed in the incident.
            logger.warning(
                "Node %s reads in_restart although its restart just "
                "succeeded — stale-write resurrection suspected; "
                "re-asserting ONLINE (see [NODE-WRITE] tripwire)",
                task.node_id)
            storage_node_ops.set_node_status(
                task.node_id, StorageNode.STATUS_ONLINE, caused_by="restart")
            node = db.get_storage_node_by_id(task.node_id)
        # Mirrors the task-entry short-circuit: success of THIS task is
        # "node is ONLINE". health_check / residual device UNAVAILABLE flags
        # are the responsibility of other recovery paths (FN_DEV_RESTART,
        # health service auto-fix, peer-side recreate_lvstore). Requiring
        # health_check==True here would cause repeat shutdown+restart cycles
        # of an already-serving node when an auxiliary check happens to be
        # False at the moment we re-read the DB.
        if node.status == StorageNode.STATUS_ONLINE:
            logger.info(f"Node is online: {node.get_id()}")
            task.function_result = "done"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return True

        task.retry += 1
        task.write_to_db(db.kv_store)
        return False
    finally:
        # On any non-success exit from the shutdown/restart sequence, make sure
        # we don't leave the node pinned in STATUS_IN_SHUTDOWN or
        # STATUS_RESTARTING — both are terminal traps if the task doesn't
        # reach STATUS_ONLINE.
        try:
            post_node = db.get_storage_node_by_id(task.node_id)
            if post_node.status != StorageNode.STATUS_ONLINE:
                _reset_if_transient(task.node_id)
        except KeyError:
            pass
        except Exception as exc:
            logger.error(f"Post-task status reset check failed: {exc}")


# Per-task restart scheduling (in-memory; this runner is a single long-lived
# process). Maps task uuid -> epoch time when the task is next eligible to run.
# This lets the runner round-robin all restart tasks instead of pinning the
# thread on one task's blocking retry loop: a task that is waiting — deferred on
# a concurrent peer restart, or in failure backoff — no longer blocks the other
# pending restart tasks behind it (incident 2026-06-25: a single task deferring
# on a peer that was briefly in_restart sat in a growing backoff and starved a
# second node's brand-new restart task indefinitely).
_restart_next_attempt: dict = {}

# A genuine restart FAILURE first retries at a steady 1-minute cadence for a few
# attempts (so a node that just needs a moment to come back recovers quickly),
# then falls back to the existing exponential backoff capped at
# RESTART_TASK_EXEC_INTERVAL_MAX_SEC. A DEFER (peer-restart mutual exclusion) is
# NOT a failure and does not back off at all — see the loop below.
RESTART_LEAD_IN_RETRIES = 3
RESTART_LEAD_IN_INTERVAL_SEC = 60


def _restart_backoff_seconds(retry):
    """Delay before the next attempt of a FAILED restart (one that consumed a
    retry). First RESTART_LEAD_IN_RETRIES attempts use a constant 1-minute
    cadence; after that the existing exponential backoff applies, continuing
    upward from the lead-in interval and capped at the configured maximum."""
    if retry <= RESTART_LEAD_IN_RETRIES:
        return RESTART_LEAD_IN_INTERVAL_SEC
    exp = RESTART_LEAD_IN_INTERVAL_SEC * (2 ** (retry - RESTART_LEAD_IN_RETRIES))
    return min(exp, constants.RESTART_TASK_EXEC_INTERVAL_MAX_SEC)


# Watchdog for orphaned transitional states. A node whose restart/shutdown
# flow is interrupted (this runner's pod evicted mid-restart during a node
# drain, node crash, ...) is left in STATUS_RESTARTING / STATUS_IN_SHUTDOWN
# with no pending task and no live process owning the transition. Those
# states are locked against outside writers (set_node_status) and the only
# sanctioned cleanup, _reset_if_transient, runs solely while a task for that
# node is being processed — so an ownerless node is wedged forever. The k8s
# operator's nodedrain controller then holds its drain slot waiting for the
# node to come online, deadlocking MachineConfig rollouts cluster-wide
# (incident 2026-07-04: every MCO reboot wedged the rollout until the node
# was manually reset).
#
# First-seen tracking is in-memory: a runner restart resets the clock, which
# only delays recovery by one grace period. Two grace tiers: when the node's
# SPDK pod is absent, nothing can be mid-flight on the data plane and we
# recover fast; when a pod exists, an unseen foreground CLI restart (which
# holds no task and looks ownerless to this check) may be driving it, and
# resetting under it would kill the SPDK it just started — so wait long
# enough for any legitimate restart to finish.
_transitional_first_seen: dict = {}
ORPHANED_STATE_GRACE_SEC = 20 * 60
ORPHANED_STATE_FAST_GRACE_SEC = 5 * 60


def _spdk_pod_exists(node):
    """Whether the node's SPDK pod exists (kubernetes mode). Used only to
    pick the watchdog grace tier — on any doubt return True so the
    conservative (long) tier applies."""
    try:
        cluster = db.get_cluster_by_id(node.cluster_id)
        if cluster.mode != "kubernetes":
            return True
        utils.load_kube_config_with_fallback()
        from kubernetes import client as k8s_client
        namespace = getattr(node, "cr_namespace", "") or constants.K8S_NAMESPACE
        prefix = f"snode-spdk-pod-{node.rpc_port}-"
        for pod in k8s_client.CoreV1Api().list_namespaced_pod(namespace=namespace).items:
            if pod.metadata.name.startswith(prefix):
                return True
        return False
    except Exception as e:
        logger.debug(f"SPDK pod lookup failed for {node.get_id()}: {e}")
        return True


def _watchdog_orphaned_transitional_nodes(cluster_id):
    """Detect nodes stuck in a transitional CP state with no restart task
    owning them, and route them through the sanctioned recovery: verify the
    data plane is down, reset to OFFLINE (_reset_if_transient), then queue a
    normal auto-restart task."""
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        node_id = node.get_id()
        if node.status not in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN):
            _transitional_first_seen.pop(node_id, None)
            continue
        # An unfinished restart task owns this state; its own flow calls
        # _reset_if_transient when appropriate.
        if not _validate_no_task_node_restart(cluster_id, node_id):
            _transitional_first_seen.pop(node_id, None)
            continue
        first_seen = _transitional_first_seen.setdefault(node_id, time.time())
        elapsed = time.time() - first_seen
        grace = ORPHANED_STATE_GRACE_SEC if _spdk_pod_exists(node) else ORPHANED_STATE_FAST_GRACE_SEC
        if elapsed < grace:
            continue
        logger.warning(
            f"Node {node_id} stuck in {node.status} for {int(elapsed)}s with no "
            f"restart task owning it; attempting reset to OFFLINE")
        _reset_if_transient(node_id)
        node = db.get_storage_node_by_id(node_id)
        if node.status == StorageNode.STATUS_OFFLINE:
            _transitional_first_seen.pop(node_id, None)
            if tasks_controller.add_node_to_auto_restart(node):
                logger.info(f"Queued auto-restart for recovered node {node_id}")


# Parallel restart execution for SUSPENDED clusters: during full-cluster
# recovery every node is offline and no client IO flows, so node restarts
# cannot violate FTT and are fanned out on this pool (~70 s each; strictly
# sequential recovery of a 32-node cluster took ~38 min, 2026-07-08). The
# per-primary consistency of the cross-node connect section is preserved by
# storage_node_ops._remote_connect_gate, and the peer-exclusion guards
# (task_runner_node pre-check + try_set_node_restarting) are relaxed only
# while the cluster is SUSPENDED. Online clusters never dispatch here.
_restart_pool = ThreadPoolExecutor(
    max_workers=constants.NODE_RESTART_MAX_PARALLEL_SUSPENDED,
    thread_name_prefix="node-restart")
_restart_inflight: dict = {}  # task uuid -> Future
# node_id -> Future. Parallel dispatch MUST also be exclusive per NODE, not
# only per task: multiple node_restart tasks can be queued for the same node
# (escalation + requeue paths), and keying inflight by task uuid alone let
# them run concurrently — each kill-and-restarting the same SPDK out from
# under the other, flipping the node offline/in_restart in a loop (observed
# 2026-07-10 mass-reboot recovery: 79 concurrent same-node dispatches, nodes
# stuck bouncing for 10+ minutes).
_node_inflight: dict = {}


def _process_restart_task(task_uuid):
    """Claim and drive one restart task, including the per-task backoff
    bookkeeping. Runs inline (serialized) normally, or on the
    suspended-cluster parallel pool. Never raises: a crash in one task must
    not kill recovery of every other node."""
    try:
        # Re-read (it may have been canceled / changed concurrently).
        task = db.get_task_by_id(task_uuid)
        if task.status == JobSchedule.STATUS_DONE:
            _restart_next_attempt.pop(task_uuid, None)
            return
        # Lease gate: do not drive a task another live runner host
        # already owns (prevents a second replica issuing a
        # concurrent shutdown/restart).
        if not tasks_controller.claim_task(task):
            logger.info(f"Restart task {task_uuid} owned by another runner host; skipping")
            return
        retry_before = task.retry
        # Device restarts (and parts of node restarts outside the
        # restart_storage_node wrapper) block without task writes; heartbeat
        # the lease so it never goes stale mid-execution and gets stolen by
        # another runner host.
        with tasks_controller.task_lease_heartbeat(task):
            res = task_runner(task)
        task = db.get_task_by_id(task_uuid)
        if res or task.status == JobSchedule.STATUS_DONE:
            _restart_next_attempt.pop(task_uuid, None)
        elif task.retry > retry_before:
            # Genuine failure (retry consumed): 1-min lead-in, then
            # exponential backoff.
            _restart_next_attempt[task_uuid] = (
                time.time() + _restart_backoff_seconds(task.retry))
        else:
            # Defer (peer-restart mutual exclusion; retry NOT
            # consumed): not a failure — do not back off. Re-poll on
            # the next short pass so this picks up immediately once
            # the blocking restart finishes.
            _restart_next_attempt[task_uuid] = (
                time.time() + constants.RESTART_TASK_EXEC_INTERVAL_SEC)
    except Exception as e:
        logger.error(f"Restart task {task_uuid} processing crashed: {e}")
        logger.exception(e)
        try:
            retry = db.get_task_by_id(task_uuid).retry
        except Exception:
            retry = 0
        _restart_next_attempt[task_uuid] = (
            time.time() + _restart_backoff_seconds(retry))



# Watchdog for orphaned transitional states. A node whose restart/shutdown
# flow is interrupted (this runner's pod evicted mid-restart during a node
# drain, node crash, ...) is left in STATUS_RESTARTING / STATUS_IN_SHUTDOWN
# with no pending task and no live process owning the transition. Those
# states are locked against outside writers (set_node_status) and the only
# sanctioned cleanup, _reset_if_transient, runs solely while a task for that
# node is being processed — so an ownerless node is wedged forever. The k8s
# operator's nodedrain controller then holds its drain slot waiting for the
# node to come online, deadlocking MachineConfig rollouts cluster-wide
# (incident 2026-07-04: every MCO reboot wedged the rollout until the node
# was manually reset).
#
# First-seen tracking is in-memory: a runner restart resets the clock, which
# only delays recovery by one grace period. Two grace tiers: when the node's
# SPDK pod is absent, nothing can be mid-flight on the data plane and we
# recover fast; when a pod exists, an unseen foreground CLI restart (which
# holds no task and looks ownerless to this check) may be driving it, and
# resetting under it would kill the SPDK it just started — so wait long
# enough for any legitimate restart to finish.
_transitional_first_seen: dict = {}
ORPHANED_STATE_GRACE_SEC = 20 * 60
ORPHANED_STATE_FAST_GRACE_SEC = 5 * 60


def _spdk_pod_exists(node):
    """Whether the node's SPDK pod exists (kubernetes mode). Used only to
    pick the watchdog grace tier — on any doubt return True so the
    conservative (long) tier applies."""
    try:
        cluster = db.get_cluster_by_id(node.cluster_id)
        if cluster.mode != "kubernetes":
            return True
        utils.load_kube_config_with_fallback()
        from kubernetes import client as k8s_client
        namespace = getattr(node, "cr_namespace", "") or constants.K8S_NAMESPACE
        prefix = f"snode-spdk-pod-{node.rpc_port}-"
        for pod in k8s_client.CoreV1Api().list_namespaced_pod(namespace=namespace).items:
            if pod.metadata.name.startswith(prefix):
                return True
        return False
    except Exception as e:
        logger.debug(f"SPDK pod lookup failed for {node.get_id()}: {e}")
        return True


def _watchdog_orphaned_transitional_nodes(cluster_id):
    """Detect nodes stuck in a transitional CP state with no restart task
    owning them, and route them through the sanctioned recovery: verify the
    data plane is down, reset to OFFLINE (_reset_if_transient), then queue a
    normal auto-restart task."""
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        node_id = node.get_id()
        if node.status not in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN):
            _transitional_first_seen.pop(node_id, None)
            continue
        # An unfinished restart task owns this state; its own flow calls
        # _reset_if_transient when appropriate.
        if not _validate_no_task_node_restart(cluster_id, node_id):
            _transitional_first_seen.pop(node_id, None)
            continue
        first_seen = _transitional_first_seen.setdefault(node_id, time.time())
        elapsed = time.time() - first_seen
        grace = ORPHANED_STATE_GRACE_SEC if _spdk_pod_exists(node) else ORPHANED_STATE_FAST_GRACE_SEC
        if elapsed < grace:
            continue
        logger.warning(
            f"Node {node_id} stuck in {node.status} for {int(elapsed)}s with no "
            f"restart task owning it; attempting reset to OFFLINE")
        _reset_if_transient(node_id)
        node = db.get_storage_node_by_id(node_id)
        if node.status == StorageNode.STATUS_OFFLINE:
            _transitional_first_seen.pop(node_id, None)
            if tasks_controller.add_node_to_auto_restart(node):
                logger.info(f"Queued auto-restart for recovered node {node_id}")


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
                    if task.function_name not in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_NODE_RESTART]:
                        continue
                    if task.status == JobSchedule.STATUS_DONE:
                        _restart_next_attempt.pop(task.uuid, None)
                        _restart_inflight.pop(task.uuid, None)
                        continue
                    # Round-robin: skip a task that is not yet due so a waiting task
                    # (deferred on a concurrent peer restart, or in failure backoff)
                    # does NOT block the other pending restart tasks behind it. The
                    # outer loop revisits every task each pass (TASK_EXEC_INTERVAL_SEC).
                    if time.time() < _restart_next_attempt.get(task.uuid, 0):
                        continue
                    # Suspend recovery: while a SUSPENDED cluster is still being
                    # drained to all-offline, pause node restarts. Executing one now
                    # would fight the auto-shutdown and re-create the wedged
                    # half-restarted state we are fixing. Re-poll soon without
                    # consuming a retry; the task runs once the drain completes
                    # (suspend_drain_complete).
                    dispatch_parallel = False
                    if task.function_name == JobSchedule.FN_NODE_RESTART:
                        cl_fresh = db.get_cluster_by_id(cl.get_id())
                        if tasks_controller.is_auto_restart_paused(cl_fresh):
                            logger.info(
                                "Cluster %s suspended and draining; deferring "
                                "node-restart task %s", cl.get_id(), task.uuid)
                            _restart_next_attempt[task.uuid] = (
                                time.time() + constants.RESTART_TASK_EXEC_INTERVAL_SEC)
                            continue
                        # SUSPENDED and drained: full-cluster recovery â€” fan node
                        # restarts out on the pool (see _restart_pool). Online
                        # clusters stay strictly sequential. suspend_drain_complete
                        # is required: it certifies every (non operator-stopped)
                        # node went OFFLINE, i.e. no client IO â€” an operator-caused
                        # suspension never drains, its survivors are still serving,
                        # and its restarts must stay sequential with full guards.
                        # Parallel dispatch in the two sanctioned cases only:
                        # drained suspension (full-cluster recovery), and a
                        # fully-dead failure domain (fd_dead_recovery_allowed:
                        # no domain member ONLINE, no cross-domain restart in
                        # flight — recovery of a rebooted domain fans out
                        # instead of 16 x single-restart serially). The former
                        # relaxation that fanned out while the domain was
                        # still SERVING (2026-07-16 violation) stays removed.
                        dispatch_parallel = (cl_fresh.status == Cluster.STATUS_SUSPENDED
                                             and cl_fresh.suspend_drain_complete)
                        if not dispatch_parallel and task.node_id:
                            try:
                                dispatch_parallel = storage_node_ops.fd_dead_recovery_allowed(
                                    db, db.get_storage_node_by_id(task.node_id))
                            except KeyError:
                                pass

                    if dispatch_parallel:
                        inflight = _restart_inflight.get(task.uuid)
                        if inflight is not None and not inflight.done():
                            continue
                        # Per-node exclusion: never run two restart tasks for the
                        # same node concurrently (see _node_inflight above). The
                        # duplicate task re-polls next pass; by then the winner
                        # has usually completed and marked it obsolete.
                        node_inflight = _node_inflight.get(task.node_id)
                        if node_inflight is not None and not node_inflight.done():
                            _restart_next_attempt[task.uuid] = (
                                time.time() + constants.RESTART_TASK_EXEC_INTERVAL_SEC)
                            continue
                        fut = _restart_pool.submit(_process_restart_task, task.uuid)
                        _restart_inflight[task.uuid] = fut
                        if task.node_id:
                            _node_inflight[task.node_id] = fut
                        continue

                    # Inline (serialized) execution; _process_restart_task never
                    # raises, so a crash in one task cannot escape to the outer
                    # `while True` and kill recovery of every other node.
                    _process_restart_task(task.uuid)

            try:
                _watchdog_orphaned_transitional_nodes(cl.get_id())
            except Exception as e:
                logger.error(f"Orphaned-node watchdog failed for cluster {cl.get_id()}: {e}")

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
