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
a non-converging one: the task is SUSPENDED with ``retry`` + 1 and re-run on a
later pass after a backoff, once the lagging zone is up again. Never an
in-place sleep. After ``SYNC_RESYNC_ALERT_RUNS`` such runs one alert is
raised; the task itself is unbounded (``max_retry=-1``).
"""
import time
from datetime import UTC, datetime

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import events_controller, sync_replication_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

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


def _save(task, mutate):
    """Apply ``mutate`` to the task as stored NOW and stamp ``updated_at``
    (as a full JobSchedule write does), in one transaction. The runner's copy
    was read before blocking RPCs; writing that whole object back would erase
    what others changed meanwhile - a cancel above all. Returns the stored
    task."""
    def _apply(fresh):
        mutate(fresh)
        fresh.updated_at = str(datetime.now(UTC))
    return db.atomic_update(task, _apply) or task


def _finish(task, result):
    def done(t):
        t.status = JobSchedule.STATUS_DONE
        t.function_result = result
    _save(task, done)
    return True


def _wait(task, reason):
    """Not a run: nothing started, ``retry`` untouched."""
    def suspend(t):
        t.status = JobSchedule.STATUS_SUSPENDED
        t.function_result = reason
    _save(task, suspend)
    return False


def _run_failed(task, owner, reason):
    def fail(t):
        t.retry += 1
        delay = retry_delay(t.retry)
        t.function_params.pop("resync", None)
        t.function_params["next_run_at"] = time.time() + delay
        t.status = JobSchedule.STATUS_SUSPENDED
        t.function_result = (f"catch-up run {t.retry} did not converge ({reason}); "
                             f"re-run in {delay}s once the lagging zone is up")
    task = _save(task, fail)
    logger.warning("LVS %s: %s", owner.lvstore, task.function_result)
    if task.retry >= constants.SYNC_RESYNC_ALERT_RUNS and not task.function_params.get("alerted"):
        # The alert first, the mark after: an alert that failed to be written
        # is raised again on the next non-converging run.
        events_controller.log_event_cluster(
            cluster_id=task.cluster_id,
            domain=events_controller.DOMAIN_CLUSTER,
            event="SYNC_RESYNC_NOT_CONVERGING",
            db_object=owner,
            caused_by=events_controller.CAUSED_BY_MONITOR,
            message=(f"LVS {owner.lvstore}: the zone catch-up has not converged after "
                     f"{task.retry} runs (last: {reason}); the replicas stay out of sync"),
            node_id=owner.get_id(),
            event_level="Error")
        _save(task, lambda t: t.function_params.update(alerted=True))
    return False


def _other_site(owner, nodes):
    return next((n.site for n in nodes if n.site and n.site != owner.site), "")


def _zone_not_up(nodes, site):
    """What keeps ``site``'s zone from being fully up: its nodes that are not
    online and their devices that are not online (removed / migrated-away ones
    do not count, as for the other migration runners)."""
    down = []
    for node in nodes:
        if node.site != site or node.status in (StorageNode.STATUS_IN_CREATION,
                                                StorageNode.STATUS_REMOVED):
            continue
        if node.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
            down.append(f"node:{node.get_id()}")
        for dev in node.nvme_devices:
            if dev.status in (NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED_AND_MIGRATED):
                continue
            if dev.status != NVMeDevice.STATUS_ONLINE:
                down.append(f"dev:{dev.get_id()}")
    return down


def _migration_status(rpc, name):
    res = rpc.distr_migration_status(name)
    if not isinstance(res, list):
        return None
    return next((e for e in res if isinstance(e, dict) and e.get("name", name) == name), None)


def _start(task, cluster, owner, leader, rpc, distribs):
    qos_high_priority = cluster.is_qos_set()
    for name in distribs:
        if db.get_task_by_id(task.uuid).canceled:
            return _finish(task, "canceled")
        try:
            started = bool(rpc.distr_migration_expansion_start(
                name, qos_high_priority, job_size=constants.MIG_JOB_SIZE,
                jobs=constants.MIG_PARALLEL_JOBS))
            # An error answer includes "Migration is already in progress" (a
            # run started before a lost status answer or a runner restart):
            # adopt that one instead of failing it.
            if not started:
                started = migration_outcome(_migration_status(rpc, name)) == OUTCOME_RUNNING
        except RPCException as e:
            return _run_failed(task, owner, f"{name}: start failed on {leader.get_id()}: {e}")
        if not started:
            return _run_failed(task, owner, f"{name}: start refused on {leader.get_id()}")
    result = f"catch-up started on {leader.get_id()}: {distribs}"

    def running(t):
        t.function_params["resync"] = {"leader_id": leader.get_id(), "started_at": time.time(),
                                       "distribs": distribs}
        t.function_params.pop("next_run_at", None)
        t.status = JobSchedule.STATUS_RUNNING
        t.function_result = result
    _save(task, running)
    logger.info("LVS %s: %s", owner.lvstore, result)
    return False


def _poll(task, owner, rpc, resync):
    lvs_name = owner.lvstore
    running = []
    for name in resync["distribs"]:
        try:
            element = _migration_status(rpc, name)
        except RPCException as e:
            return _run_failed(task, owner, f"{name}: status query failed: {e}")
        outcome = migration_outcome(element)
        if outcome == OUTCOME_FAILED:
            return _run_failed(task, owner, f"{name}: catch-up ended with {element}")
        if outcome == OUTCOME_RUNNING:
            running.append(f"{name} {element.get('progress', '?')}%")
    if running:
        result = f"catch-up running on {resync['leader_id']}: {', '.join(running)}"
        _save(task, lambda t: setattr(t, "function_result", result))
        return False

    # Every distrib completed without error. Only the status of the node that
    # ran the catch-up confirms the zones are equal; an event received after
    # this point may be a newer desync that answer does not cover.
    seq_limit = db.get_sync_state(task.cluster_id, lvs_name)["seq"]
    for name in resync["distribs"]:
        try:
            answer = rpc.distr_sync_replication_status(name)
        except RPCException as e:
            return _run_failed(task, owner, f"{name}: sync status query failed: {e}")
        status = answer[0].get("status") if isinstance(answer, list) and answer else None
        if status != "synced":
            return _run_failed(task, owner, f"{name}: {status} after the catch-up")
    duration = round(time.time() - resync["started_at"], 1)
    if db.finish_sync_resync(task, lvs_name, seq_limit, f"synced; catch-up took {duration}s", duration):
        logger.info("LVS %s: zones synced, catch-up took %ss", lvs_name, duration)
        return True
    logger.info("LVS %s: a newer zone desync arrived during the catch-up, re-running", lvs_name)
    return False


def task_runner(task):
    """One pass over ``task``. True when it finished."""
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        return _finish(task, "canceled")
    lvs_name = task.function_params.get("lvs_name", "")
    try:
        owner = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        return _finish(task, f"LVS owner {task.node_id} not found")
    if owner.status == StorageNode.STATUS_REMOVED or owner.lvstore != lvs_name:
        return _finish(task, f"LVS {lvs_name} no longer owned by {task.node_id}")
    cluster = db.get_cluster_by_id(task.cluster_id)
    if not cluster.sync_replication:
        return _finish(task, "not a sync-replication cluster")

    # A lost site is being (or has been) failed over; the catch-up waits for
    # its return, whatever else is going on.
    if cluster.lost_site:
        return _wait(task, f"site {cluster.lost_site} is lost, waiting for its return")
    if cluster.status not in Cluster.OPERABLE_STATUSES:
        return _wait(task, f"cluster is {cluster.status}, waiting")
    if owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX):
        return _wait(task, f"leadership of {lvs_name} is moving ({owner.lvs_active_site}), waiting")

    resync = task.function_params.get("resync")
    if not resync:
        next_run_at = task.function_params.get("next_run_at", 0)
        if time.time() < next_run_at:
            return _wait(task, f"run {task.retry + 1} backs off for "
                               f"{int(next_run_at - time.time())}s more")
        nodes = db.get_storage_nodes_by_cluster_id(task.cluster_id)
        zones = sync_replication_controller.lagging_sites(
            owner.site, _other_site(owner, nodes),
            db.get_unresolved_sync_replication_events(task.cluster_id, lvs_name))
        down = [item for site in zones for item in _zone_not_up(nodes, site)]
        if down:
            return _wait(task, f"waiting for the lagging zone(s) {zones} to be up: {down}")

    leader, _ = storage_node_ops.find_leader_with_failover([owner], lvs_name)
    if leader is None:
        return _wait(task, f"no leader of {lvs_name} in its active triplet, waiting")
    rpc = leader.rpc_client(timeout=5, retry=2)
    if not resync:
        distribs = [bdev["name"] for bdev in owner.lvstore_stack or []
                    if bdev.get("type") == "bdev_distr"]
        return _start(task, cluster, owner, leader, rpc, distribs)
    if resync["leader_id"] != leader.get_id():
        return _run_failed(task, owner, f"leadership moved from {resync['leader_id']} "
                                        f"to {leader.get_id()} during the catch-up")
    return _poll(task, owner, rpc, resync)


def main():
    logger.info("Starting sync-replication resync runner...")
    while True:
        try:
            clusters = db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        for cluster in clusters:
            if not cluster.sync_replication:
                continue
            # At most one active task per LVS (add_sync_resync_task), plus a
            # canceled one on its way out.
            for task in db.get_active_sync_resync_tasks(cluster.get_id()):
                lvs_name = task.function_params.get("lvs_name", "")
                if not tasks_controller.claim_task(task):
                    logger.info(f"Resync task {task.uuid} owned by another runner host; skipping")
                    continue
                try:
                    with tasks_controller.task_lease_heartbeat(task):
                        task_runner(task)
                except Exception as e:
                    logger.exception(f"Resync task {task.uuid} of LVS {lvs_name} failed: {e}")
        time.sleep(3)


if __name__ == "__main__":
    main()
