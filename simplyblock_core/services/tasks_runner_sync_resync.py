"""Sync-replication resync runner (``FN_SYNC_RESYNC``, one task per LVS).

A zone desync of an LVS (a ``*_zone_unavailable`` event, recorded by the distr
event collector) leaves one zone behind; the data plane does not catch it up
on its own. This runner does, once the lagging zone is back: it starts the
catch-up (``distr_migration_expansion_start``, which on a sync distrib also
runs the zone sync) for every distrib of the LVS on the leader of its active
triplet, polls ``distr_migration_status`` and, when every distrib completed
without error and the leader reports ``synced``, resolves the LVS's zone
events and finishes.

Any other outcome - an error bit (11: started while the zone was lost, 8
SHUTDOWN: the zone got lost during it, IO / destination errors), a lost
migration, a leadership change, a status other than ``synced`` - makes the run
a non-converging one, a retry of the shared task driver (task_runner_base):
the task is SUSPENDED with ``retry`` + 1 and re-run after ``retry_delay`` of
that count, once the lagging zone is up again. Never an in-place sleep. The
driver holds that backoff in memory: a restarted runner re-runs at once, still
gated by the lagging-zone check. After ``SYNC_RESYNC_ALERT_RUNS`` such runs
one alert is raised; the task itself is unbounded (``max_retry=-1``). A wait
(a lost site, a site activation, a lagging zone down, no leader, ...) is a
TaskDefer: no run, no backoff.

A started catch-up is recorded in ``resync`` and polled while the task stays
RUNNING. A converged run records ``converged`` (the event sequence its
``synced`` answers cover, the duration) and returns; the spec's terminal
commit (finish_task) then closes it in one transaction with the LVS state key:
DONE, or SUSPENDED again for a new run when a newer zone desync arrived.

The runner never makes a leader: its lookup runs without recovery
(``find_leader_with_failover(recover=False)``). An LVS nobody leads - the
normal state between a promote and the first client IO - is waited for, and
the wait is not a run.
"""
import time
from typing import NoReturn

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import (
    activation_gate,
    events_controller,
    sync_replication_controller,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException, RPCRemoteError
from simplyblock_core.services.task_runner_base import (
    Ending,
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskProgress,
    TaskRetry,
    checkpoint,
    drop_params,
    finish_done,
    serve,
)

logger = utils.get_logger(__name__)

db = db_controller.DBController()

#: distr_migration_status states of a catch-up that has not ended yet.
MIGRATION_ACTIVE_STATES = ("pending", "running", "stopping")

OUTCOME_RUNNING = "running"
OUTCOME_OK = "ok"
OUTCOME_FAILED = "failed"


def migration_outcome(element) -> str:
    """Classify one ``distr_migration_status`` element of a catch-up: still
    running, completed without error, or anything else (an error bitmask, a
    migration the node no longer knows - ``none`` after a restart -, no
    answer), which ends the run as non-converging."""
    if not isinstance(element, dict):
        return OUTCOME_FAILED
    status = element.get("status")
    if status in MIGRATION_ACTIVE_STATES:
        return OUTCOME_RUNNING
    if status == "completed" and element.get("error", -1) == 0:
        return OUTCOME_OK
    return OUTCOME_FAILED


def retry_delay(runs: int) -> int:
    """Seconds before re-running after the ``runs``-th non-converging run."""
    return min(constants.SYNC_RESYNC_RETRY_BASE_SEC * 2 ** (max(runs, 1) - 1),
               constants.SYNC_RESYNC_RETRY_MAX_SEC)


def _run_failed(task, owner, reason) -> NoReturn:
    """End a non-converging run: the driver records it (``retry`` + 1) and
    backs off ``retry_delay`` of the new count. Always raises."""
    runs = task.retry + 1
    task = drop_params(task, "resync")
    if task is None:
        raise TaskAbort("canceled")
    message = (f"catch-up run {runs} did not converge ({reason}); "
               f"re-run in {retry_delay(runs)}s once the lagging zone is up")
    logger.warning("LVS %s: %s", owner.lvstore, message)
    if runs >= constants.SYNC_RESYNC_ALERT_RUNS and not task.function_params.get("alerted"):
        # The alert first, the mark after: an alert that failed to be written
        # is raised again on the next non-converging run.
        events_controller.log_event_cluster(
            cluster_id=task.cluster_id,
            domain=events_controller.DOMAIN_CLUSTER,
            event="SYNC_RESYNC_NOT_CONVERGING",
            db_object=owner,
            caused_by=events_controller.CAUSED_BY_MONITOR,
            message=(f"LVS {owner.lvstore}: the zone catch-up has not converged after "
                     f"{runs} runs (last: {reason}); the replicas stay out of sync"),
            node_id=owner.get_id(),
            event_level="Error")
        if checkpoint(task, alerted=True) is None:
            raise TaskAbort("canceled")
    raise TaskRetry(message)


def _other_site(owner, nodes):
    return next((n.site for n in nodes if n.site and n.site != owner.site), "")


def _migration_status(rpc, name):
    res = rpc.distr_migration_status(name)
    if not isinstance(res, list):
        return None
    return next((e for e in res if isinstance(e, dict) and e.get("name", name) == name), None)


def _start(task, cluster, owner, leader, rpc, distribs) -> NoReturn:
    qos_high_priority = cluster.is_qos_set()
    for name in distribs:
        task = checkpoint(task)
        if task is None:
            raise TaskAbort("canceled")
        try:
            try:
                started = bool(rpc.distr_migration_expansion_start(
                    name, qos_high_priority, job_size=constants.MIG_JOB_SIZE,
                    jobs=constants.MIG_PARALLEL_JOBS))
            except RPCRemoteError as e:
                logger.info("LVS %s: %s start answered %s; probing for a run in progress",
                            owner.lvstore, name, e)
                started = False
            # An error answer includes "Migration is already in progress" (a
            # run started before a lost status answer or a runner restart):
            # adopt that one instead of failing it.
            if not started:
                started = migration_outcome(_migration_status(rpc, name)) == OUTCOME_RUNNING
        except RPCException as e:
            _run_failed(task, owner, f"{name}: start failed on {leader.get_id()}: {e}")
        if not started:
            _run_failed(task, owner, f"{name}: start refused on {leader.get_id()}")
    resync = {"leader_id": leader.get_id(), "started_at": time.time(), "distribs": distribs}
    if checkpoint(task, resync=resync) is None:
        raise TaskAbort("canceled")
    result = f"catch-up started on {leader.get_id()}: {distribs}"
    logger.info("LVS %s: %s", owner.lvstore, result)
    raise TaskProgress(result)


def _poll(task, owner, rpc, resync):
    lvs_name = owner.lvstore
    running = []
    for name in resync["distribs"]:
        try:
            element = _migration_status(rpc, name)
        except RPCException as e:
            _run_failed(task, owner, f"{name}: status query failed: {e}")
        outcome = migration_outcome(element)
        if outcome == OUTCOME_FAILED:
            _run_failed(task, owner, f"{name}: catch-up ended with {element}")
        if outcome == OUTCOME_RUNNING:
            running.append(f"{name} {element.get('progress', '?')}%")
    if running:
        raise TaskProgress(f"catch-up running on {resync['leader_id']}: {', '.join(running)}")

    # Every distrib completed without error. Only the status of the node that
    # ran the catch-up confirms the zones are equal; an event received after
    # this point may be a newer desync that answer does not cover.
    seq_limit = db.get_sync_state(task.cluster_id, lvs_name)["seq"]
    for name in resync["distribs"]:
        try:
            answer = rpc.distr_sync_replication_status(name)
        except RPCException as e:
            _run_failed(task, owner, f"{name}: sync status query failed: {e}")
        status = answer[0].get("status") if isinstance(answer, list) and answer else None
        if status != "synced":
            _run_failed(task, owner, f"{name}: {status} after the catch-up")
    duration = round(time.time() - resync["started_at"], 1)
    # Closed by the spec's terminal commit (finish_task), with the LVS state.
    if checkpoint(task, converged={"seq": seq_limit, "duration_sec": duration}) is None:
        raise TaskAbort("canceled")


def process_task(task):
    """The driver's handler: one pass over ``task``. Returns once the
    catch-up converged; every other outcome is a signal of the driver."""
    lvs_name = task.function_params.get("lvs_name", "")
    try:
        owner = db.get_storage_node_by_id(task.node_id)
    except KeyError as e:
        raise TaskAbort(f"LVS owner {task.node_id} not found") from e
    if owner.status == StorageNode.STATUS_REMOVED or owner.lvstore != lvs_name:
        raise TaskAbort(f"LVS {lvs_name} no longer owned by {task.node_id}")
    cluster = db.get_cluster_by_id(task.cluster_id)
    if not cluster.sync_replication:
        raise TaskAbort("not a sync-replication cluster")

    # A lost site is being (or has been) failed over; the catch-up waits for
    # its return, whatever else is going on.
    if cluster.lost_site:
        raise TaskDefer(f"site {cluster.lost_site} is lost, waiting for its return")
    site_activation = activation_gate.site_activation_blocks(cluster)
    if site_activation:
        raise TaskDefer(site_activation)
    if cluster.status not in Cluster.OPERABLE_STATUSES:
        raise TaskDefer(f"cluster is {cluster.status}, waiting")
    if owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX):
        raise TaskDefer(f"leadership of {lvs_name} is moving ({owner.lvs_active_site}), waiting")

    resync = task.function_params.get("resync")
    if not resync:
        nodes = db.get_storage_nodes_by_cluster_id(task.cluster_id)
        zones = sync_replication_controller.lagging_sites(
            owner.site, _other_site(owner, nodes),
            db.get_unresolved_sync_replication_events(task.cluster_id, lvs_name))
        down = [item for site in zones for item in sync_replication_controller.zone_not_up(nodes, site)]
        if down:
            raise TaskDefer(f"waiting for the lagging zone(s) {zones} to be up: {down}")

    leader, _ = storage_node_ops.find_leader_with_failover([owner], lvs_name, recover=False)
    if leader is None:
        raise TaskDefer(f"no leader of {lvs_name} in its active triplet, waiting")
    rpc = leader.rpc_client(timeout=5, retry=2)
    if not resync:
        distribs = [bdev["name"] for bdev in owner.lvstore_stack or []
                    if bdev.get("type") == "bdev_distr"]
        _start(task, cluster, owner, leader, rpc, distribs)
    if resync["leader_id"] != leader.get_id():
        _run_failed(task, owner, f"leadership moved from {resync['leader_id']} "
                                 f"to {leader.get_id()} during the catch-up")
    _poll(task, owner, rpc, resync)


def finish_task(task, ending, result):
    """The spec's terminal commit. A converged run closes the catch-up in one
    transaction with the LVS state key (DBController.finish_sync_resync): DONE,
    or SUSPENDED again - a re-arm the driver runs on its next cycle - when a
    zone desync newer than the run's ``synced`` answer arrived. Every other
    ending is the driver's own DONE commit."""
    converged = task.function_params.get("converged")
    if ending is not Ending.SUCCEEDED or not converged:
        return finish_done(task, result, context=SPEC.name)
    lvs_name = task.function_params.get("lvs_name", "")
    duration = converged["duration_sec"]
    if db.finish_sync_resync(task, lvs_name, converged["seq"],
                             f"synced; catch-up took {duration}s", duration):
        logger.info("LVS %s: zones synced, catch-up took %ss", lvs_name, duration)
    else:
        logger.info("LVS %s: a newer zone desync arrived during the catch-up, re-running", lvs_name)
    return db.get_task_by_id(task.uuid)


SPEC = RunnerSpec(
    name="tasks-runner-sync-resync",
    function_names=[JobSchedule.FN_SYNC_RESYNC],
    handler=process_task,
    finish=finish_task,
    backoff=retry_delay,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
