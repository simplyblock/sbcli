# coding=utf-8
import time

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, health_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskRetry,
    checkpoint,
    serve,
)
from simplyblock_core.snode_client import SNodeClientException


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()


def _parallel_restart_allowed(node):
    """The two sanctioned cases for restarting nodes in parallel: a drained
    suspension (full-cluster recovery — every node offline, no client IO), and
    a fully-dead failure domain (no domain member ONLINE, so parallel recovery
    cannot touch served IO).

    Decides both the dispatch mode and whether the peer-exclusion pre-check in
    task_runner_node applies. The two must agree: a task fanned out in parallel
    would otherwise immediately defer on the very peers it was dispatched
    alongside.
    """
    cluster = db.get_cluster_by_id(node.cluster_id)
    if cluster.status == Cluster.STATUS_SUSPENDED and cluster.suspend_drain_complete:
        return True
    return storage_node_ops.fd_dead_recovery_allowed(db, node)


def _is_eligible(task, cluster):
    """Suspend recovery: while a SUSPENDED cluster is still being drained to
    all-offline, pause node restarts. Executing one now would fight the
    auto-shutdown and re-create the wedged half-restarted state we are fixing.
    The task re-polls without consuming a retry and runs once the drain
    completes."""
    if task.function_name != JobSchedule.FN_NODE_RESTART:
        return True
    return not tasks_controller.is_auto_restart_paused(db.get_cluster_by_id(cluster.get_id()))


def _serialize(task, cluster):
    if task.function_name != JobSchedule.FN_NODE_RESTART or not task.node_id:
        return True
    try:
        return not _parallel_restart_allowed(db.get_storage_node_by_id(task.node_id))
    except KeyError:
        return True


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

    if not _validate_no_task_node_restart(task.cluster_id, task.node_id):
        # The node-level restart supersedes this device-level one.
        device_controller.device_set_unavailable(device.get_id())
        raise TaskAbort("canceled: node restart found")

    node = db.get_storage_node_by_id(task.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online: {node.get_id()}, retry")
        raise TaskRetry("Node is offline")

    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "Device is online"
        return

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Device is not unavailable: {device.get_id()}, {device.status} , stopping task")
        raise TaskAbort(f"stopped because dev is {device.status}")

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
    if device.status != NVMeDevice.STATUS_ONLINE or device.io_error is not False:
        raise TaskRetry(f"Device is {device.status}, retry")

    logger.info(f"Device is online: {device.get_id()}")
    task.function_result = "done"
    tasks_controller.add_device_mig_task_for_node(task.node_id)


def _give_up_on_device(task):
    """The retry ceiling has terminated a device-restart task."""
    device = _get_device(task)
    if device is None:
        return
    device_controller.device_set_unavailable(device.get_id())
    device_controller.device_set_retries_exhausted(device.get_id(), True)


def _abandon_task(task):
    """Driver on_finish: the terminal paths a handler never reaches.

    Only two need anything. The retry ceiling gives up on the target, which for
    a node means parking it OFFLINE and re-queueing, and for a device means
    marking it unavailable and out of retries. A cancellation stops a device
    task retrying forever. Everything else — success, the aborts the handler
    raises itself — leaves the target alone.
    """
    ceiling_reached = 0 <= task.max_retry <= task.retry

    if task.function_name == JobSchedule.FN_NODE_RESTART:
        if ceiling_reached and not task.canceled:
            _give_up_on_node(task)
        return

    if task.canceled:
        device = _get_device(task)
        if device is not None:
            device_controller.device_set_retries_exhausted(device.get_id(), True)
    elif ceiling_reached:
        _give_up_on_device(task)


def _give_up_on_node(task):
    """The retry ceiling has terminated a node-restart task. Reached through
    the driver's on_finish, since the handler never sees that path."""
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
        logger.debug(
            f"Node {task.node_id} no longer exists, skipping auto-restart re-queue")
    except Exception as exc:
        logger.error(f"Failed to re-queue auto-restart for {task.node_id}: {exc}")


def task_runner_node(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node not found")

    if node.status in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_SCHEDULABLE]:
        logger.info(f"Node is {node.status}, stopping task")
        raise TaskAbort(f"Node is {node.status}, stopping")
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
        return

    # A restart already in flight makes this task redundant — unless it is our
    # own from an earlier attempt. This used to be inferred from the task still
    # being NEW/SUSPENDED, which no longer distinguishes anything now that the
    # driver moves the task to RUNNING before calling; the marker below records
    # the fact directly, and also covers an attempt that issued the restart and
    # then died, whose RESTARTING is ours to finish rather than defer to.
    if (node.status == StorageNode.STATUS_RESTARTING
            and not task.function_params.get("restart_issued")):
        logger.info("Node is restarting, stopping task")
        raise TaskAbort("Node is restarting")

    # Peer-restart mutual-exclusion pre-check: if any peer is RESTARTING
    # or IN_SHUTDOWN we cannot proceed (try_set_node_restarting in the
    # restart impl uses an FDB-tx with the same predicate and would fail
    # acquisition). This is purely transient — burning a retry on a lock
    # we know we can't acquire just collapses the backoff budget, so it
    # defers instead: no retry consumed, re-polled on the next short pass.
    # Once the peer finishes its transition, this check passes and we
    # proceed with a fresh budget.
    #
    # Skipped only for a SUSPENDED **and drained** cluster: recovery restarts
    # run in parallel then (see the dispatch loop below) so peers in
    # RESTARTING / IN_SHUTDOWN are expected, not a conflict. The FDB guard in
    # restart_storage_node is relaxed the same way (allow_concurrent_peers).
    # An operator-caused suspension never drains — its survivors still serve
    # IO — so it keeps the full pre-check.
    if not _parallel_restart_allowed(node):
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
                raise TaskDefer(f"Peer {peer.get_id()[:8]} is {peer.status}; "
                                f"deferring (no retry consumed)")

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
            # data_ping_check is tri-state (True/False/None): None means the
            # SnodeAPI call itself errored/timed out (inconclusive), not that
            # the ping failed. `|=` against None raises TypeError and crashes
            # task processing every retry, wedging the task in a non-DONE
            # state forever. Only an explicit True should flip this to True.
            if data_ping_check is True:
                node_data_nic_ping_check = True
    if not ping_check or not node_api_check or not node_data_nic_ping_check:
        logger.info(f"Node is not reachable: {task.node_id}, retry")
        raise TaskRetry("Node is unreachable, retry")

    # Last-line defense before the destructive shutdown/restart sequence:
    # everything above ran against reads taken seconds ago (the reachability
    # checks alone take a while). A cancellation committed meanwhile
    # (set_node_status(ONLINE) -> cancel_pending_node_restart_tasks) must stop
    # this entry HERE — in the 2026-07-29 double restart the second entry never
    # re-checked and force-shut a node that was back up and serving.
    fresh = db.get_task_by_id(task.uuid)
    if fresh is None or fresh.canceled or fresh.status == JobSchedule.STATUS_DONE:
        logger.info(
            f"Task {task.uuid} was canceled/finished concurrently; "
            f"stopping before shutdown")
        raise TaskAbort("canceled")
    task = fresh

    # Cross-actor claim check on a fresh node read: a live driver (e.g. a
    # manual `sn restart`) mid-transition on this node holds the per-node
    # restart claim. Proceeding would shutdown+restart over its in-flight
    # work (2026-08-06 iter-50: this exact path destroyed a CLI restart's
    # SPDK container at finalization). The shutdown/restart guards below
    # would refuse anyway — but only after burning a retry; defer like the
    # peer-exclusion pre-check instead, without consuming the budget. When
    # the driver finishes (node ONLINE cancels this task) or dies (claim
    # expires within RESTART_CLAIM_TTL_SEC), the next cycle proceeds.
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node not found")
    if node.status in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN):
        claim_holder = db_controller.restart_claim_active(node)
        if claim_holder:
            raise TaskDefer(f"Node restart claim held by {claim_holder}; "
                            f"deferring (no retry consumed)")

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
                # task.uuid so check_node_shutdown_preconditions recognizes
                # this task as the shutdown's own driver instead of reporting
                # it as a competing restart task.
                ret = storage_node_ops.shutdown_storage_node(
                    node.get_id(), force=True, current_restart_task_id=task.uuid)
                if ret:
                    logger.info("Node shutdown succeeded")
                    shutdown_succeeded = True
                    updated = checkpoint(task, cleanup_shutdown_done=True)
                    if updated is None:
                        # Canceled under us right after the shutdown; do not
                        # drive the restart of a canceled task. The monitor's
                        # offline re-queue scan picks the node up again.
                        raise TaskAbort("canceled during cleanup shutdown")
                    task = updated
                else:
                    logger.error("Node shutdown returned False; will retry after reset")
                time.sleep(3)
            except (TaskAbort, TaskDefer, TaskRetry):
                raise
            except Exception as e:
                logger.error(e)
                # Preserved as a defer, not a failure: this branch never
                # consumed a retry, and restart's give-up has side effects
                # (OFFLINE flip + re-queue) that a changed verdict would start
                # triggering where it previously could not.
                raise TaskDefer(f"cleanup shutdown raised: {e}")
        else:
            logger.info(
                f"Skipping cleanup shutdown for {node.get_id()}: "
                f"status={node.status}, "
                f"already_done={bool(task.function_params.get('cleanup_shutdown_done'))}")

        # Skip the restart step if shutdown did not succeed — restarting on top
        # of a half-shutdown node produced the in_restart hang we're guarding
        # against. Let the outer retry reattempt the whole cycle.
        if not shutdown_succeeded:
            raise TaskRetry("Node shutdown did not succeed")

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
            # Recorded before the call: a restart that starts and then loses
            # this process still owns the node's RESTARTING state, and the next
            # attempt must recognise it as ours rather than stopping for it.
            updated = checkpoint(task, restart_issued=True)
            if updated is None:
                raise TaskAbort("canceled before restart")
            task = updated
            ret = storage_node_ops.restart_storage_node(node.get_id(), force=True, current_restart_task_id=task.uuid)
            if ret:
                logger.info("Node restart succeeded")
        except (TaskAbort, TaskDefer, TaskRetry):
            raise
        except Exception as e:
            logger.error(e)
            raise TaskDefer(f"restart raised: {e}")

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
            return

        raise TaskRetry("Node did not come back online")
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


# A genuine restart FAILURE first retries at a steady 1-minute cadence for a few
# attempts (so a node that just needs a moment to come back recovers quickly),
# then falls back to exponential backoff capped at
# RESTART_TASK_EXEC_INTERVAL_MAX_SEC. A DEFER (peer-restart mutual exclusion) is
# NOT a failure and does not back off at all — the driver re-polls it next pass.
RESTART_LEAD_IN_RETRIES = 3
RESTART_LEAD_IN_INTERVAL_SEC = 60


def _restart_backoff_seconds(retry):
    """Delay before the next attempt of a FAILED restart (one that consumed a
    retry). First RESTART_LEAD_IN_RETRIES attempts use a constant 1-minute
    cadence; after that exponential backoff applies, continuing upward from the
    lead-in interval and capped at the configured maximum."""
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


SPEC = RunnerSpec(
    name="tasks-runner-restart",
    function_names=[JobSchedule.FN_DEV_RESTART, JobSchedule.FN_NODE_RESTART],
    handler=task_runner,
    on_finish=_abandon_task,
    on_cycle=lambda cluster: _watchdog_orphaned_transitional_nodes(cluster.get_id()),
    is_eligible=_is_eligible,
    interval=constants.TASK_EXEC_INTERVAL_SEC,
    # Parallel restart execution for SUSPENDED clusters: during full-cluster
    # recovery every node is offline and no client IO flows, so node restarts
    # cannot violate FTT and are fanned out (~70 s each; strictly sequential
    # recovery of a 32-node cluster took ~38 min, 2026-07-08). The per-primary
    # consistency of the cross-node connect section is preserved by
    # storage_node_ops._remote_connect_gate, and the peer-exclusion guards
    # (the pre-check in task_runner_node + try_set_node_restarting) are relaxed
    # under exactly the same condition. Online clusters stay sequential.
    concurrency=constants.NODE_RESTART_MAX_PARALLEL_SUSPENDED,
    serialize=_serialize,
    # Never two restart tasks for the same node at once: multiple node_restart
    # tasks can be queued for one node (escalation + requeue paths), and
    # excluding by task alone let them run concurrently — each kill-and-
    # restarting the same SPDK out from under the other, flipping the node
    # offline/in_restart in a loop (2026-07-10 mass-reboot recovery: 79
    # concurrent same-node dispatches, nodes bouncing for 10+ minutes).
    exclusion_key=lambda task: task.node_id or None,
    backoff=_restart_backoff_seconds,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
