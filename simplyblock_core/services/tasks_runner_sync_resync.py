"""Sync-replication resync runner (``FN_SYNC_RESYNC``, one task per LVS).

A zone desync of an LVS (a ``*_zone_unavailable`` event, recorded by the distr
event collector) leaves one zone behind; the data plane does not catch it up
on its own. This runner does, once the lagging zone is back: it starts the
catch-up (``distr_migration_replica_sync_start``, the only RPC that runs it)
for every distrib of the LVS on the node that leads the LVS's active
triplet, whatever that node's role in the triplet is (primary, or a
secondary / tertiary acting for it). Then it polls ``distr_migration_status``.
When every started catch-up has ended and the leader reports ``synced`` for
every distrib of the LVS, it resolves the LVS's zone events and the task
finishes.

Start answers, per distrib: ``true`` starts it. EALREADY (the zones are
already synced) starts nothing and is left to the final ``synced`` check.
EBUSY means the distrib's single migration slot is taken: the runner probes
``distr_migration_status`` and adopts a catch-up (type 3) it finds there,
running or completed, and waits for any other migration type to free the
slot. EAGAIN (the sync status is not loaded yet) is a wait. Every other
answer fails the run.

The catch-up shares that slot with the expansion and failure migrations of
the same distrib. A started device migration on the leader's node whose
result its runner has not read yet, or any open failed-device migration
there, holds the catch-up back (the reverse guard): starting it would
replace that result as the bdev's latest migration status, and a
failed-device migration goes first. While held, the task records the holder
in ``held_by``; a hold that lasts ``SYNC_RESYNC_HELD_ALERT_SEC`` raises one
alert. During the poll an element of another migration type means the
catch-up has ended and the slot was reused: the final ``synced`` check
decides the run.

Any other outcome - an error bit (11: the zones are still apart, 8
SHUTDOWN: the catch-up was stopped, IO / destination errors), a lost
migration, a status other than ``synced`` - makes the run a non-converging
one, a retry of the shared task driver (task_runner_base): the task is
SUSPENDED with ``retry`` + 1 and re-run after ``retry_delay`` of that count,
once the lagging zone is up again. Never an in-place sleep. The driver holds
that backoff in memory: a restarted runner re-runs at once, still gated by
the lagging-zone check. After ``SYNC_RESYNC_ALERT_RUNS`` such runs one alert
is raised; the task itself is unbounded (``max_retry=-1``). A wait (a lost
site, a site activation, a lagging zone down, no leader, a held or busy
slot, ...) is a TaskDefer: no run, no backoff. So is a new leader of the
active triplet during a catch-up: the recorded catch-up is dropped and the
next pass starts it on the new leader.

A started catch-up is recorded in ``resync`` and polled while the task stays
RUNNING. A converged run records ``converged`` (the event sequence its
``synced`` answers cover, the duration, the leader) and returns; the spec's
terminal commit (finish_task) then closes it in one transaction with the LVS
state key: DONE with a result naming the leader, or SUSPENDED again for a
new run when a newer zone desync arrived.

The runner never makes a leader: its lookup runs without recovery
(``find_leader_with_failover(recover=False)``). An LVS nobody leads - the
normal state between a promote and the first client IO - is waited for, and
the wait is not a run.
"""
import errno
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
from simplyblock_core.services import migration_task_common
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

#: distr_migration_status states of a migration that has not ended yet.
MIGRATION_ACTIVE_STATES = ("pending", "running", "stopping")

#: distr_migration_status ``type`` of the replica-sync catch-up.
MIGRATION_TYPE_RESYNC = 3

OUTCOME_RUNNING = "running"
OUTCOME_OK = "ok"
OUTCOME_FAILED = "failed"
OUTCOME_FOREIGN = "foreign"


def migration_outcome(element) -> str:
    """Classify one ``distr_migration_status`` element of a catch-up: still
    running, completed without error, foreign, or anything else (an error
    bitmask, a migration the node no longer knows - ``none`` after a restart
    -, no answer), which ends the run as non-converging.

    Foreign is an element of another migration type: the bdev keeps only its
    latest migration, so the catch-up has ended and an expansion or failure
    migration has taken the slot since. Its outcome is not the catch-up's;
    the final ``synced`` check decides the run. An element without a type
    (``none``) is not foreign."""
    if not isinstance(element, dict):
        return OUTCOME_FAILED
    if "type" in element and element["type"] != MIGRATION_TYPE_RESYNC:
        return OUTCOME_FOREIGN
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


def _alert_once(task, owner, flag, message):
    """Raise the ``SYNC_RESYNC_NOT_CONVERGING`` alert of ``owner``'s LVS with
    ``message``, unless the task already carries ``flag``; then set ``flag``.
    The alert first, the mark after: an alert that failed to be written is
    raised again on the next call. Returns the task."""
    if task.function_params.get(flag):
        return task
    events_controller.log_event_cluster(
        cluster_id=task.cluster_id,
        domain=events_controller.DOMAIN_CLUSTER,
        event="SYNC_RESYNC_NOT_CONVERGING",
        db_object=owner,
        caused_by=events_controller.CAUSED_BY_MONITOR,
        message=message,
        node_id=owner.get_id(),
        event_level="Error")
    task = checkpoint(task, **{flag: True})
    if task is None:
        raise TaskAbort("canceled")
    return task


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
    if runs >= constants.SYNC_RESYNC_ALERT_RUNS:
        _alert_once(task, owner, "alerted",
                    f"LVS {owner.lvstore}: the zone catch-up has not converged after "
                    f"{runs} runs (last: {reason}); the replicas stay out of sync")
    raise TaskRetry(message)


def _migration_status(rpc, name):
    res = rpc.distr_migration_status(name)
    if not isinstance(res, list):
        return None
    return next((e for e in res if isinstance(e, dict) and e.get("name", name) == name), None)


def _slot_holder(task, lvs_name, distribs, leader_id) -> JobSchedule | None:
    """The first open device-migration task on the leader's node that holds
    the catch-up of ``lvs_name`` back: a started one (its runner has not read
    its result yet), or a failed-device one, started or not. The migration
    slot and its status belong to one node's data plane, so a task on another
    node does not hold the catch-up on the leader."""
    for mig in db.get_active_device_migration_tasks(task.cluster_id):
        params = mig.function_params
        if (mig.node_id == leader_id and params.get("lvs_name") == lvs_name
                and params.get("distr_name") in distribs
                and (migration_task_common.migration_started(mig)
                     or mig.function_name == JobSchedule.FN_FAILED_DEV_MIG)):
            return mig
    return None


def _guard(task, owner, leader_id, distribs):
    """The reverse guard before a start. While a device-migration task holds
    the catch-up back, record it in ``held_by`` (``since`` stays the time of
    the first hold while the hold lasts, whoever holds), raise the alert once
    the hold has lasted ``SYNC_RESYNC_HELD_ALERT_SEC``, and wait. Otherwise
    drop the held state and return the task to start with."""
    holder = _slot_holder(task, owner.lvstore, distribs, leader_id)
    if holder is None:
        if {"held_by", "held_alerted"} & set(task.function_params):
            task = drop_params(task, "held_by", "held_alerted")
            if task is None:
                raise TaskAbort("canceled")
        return task
    now = time.time()
    since = (task.function_params.get("held_by") or {}).get("since", now)
    task = checkpoint(task, held_by={"task": holder.uuid, "function": holder.function_name,
                                     "node": leader_id, "since": since})
    if task is None:
        raise TaskAbort("canceled")
    held_for = now - since
    if held_for >= constants.SYNC_RESYNC_HELD_ALERT_SEC:
        _alert_once(task, owner, "held_alerted",
                    f"LVS {owner.lvstore}: the zone catch-up has been held for {int(held_for)}s "
                    f"by migration task {holder.uuid} ({holder.function_name}) on {leader_id}")
    raise TaskDefer(f"waiting for open migration task {holder.uuid} ({holder.function_name}) "
                    f"of {holder.function_params.get('distr_name')} on {leader_id} to finish")


def _probe_busy(task, owner, leader_id, rpc, name) -> None:
    """The start of ``name`` answered EBUSY: return to adopt the catch-up that
    holds the slot (type 3, running or completed - the poll reads its
    outcome), wait while another migration type holds it, or wait one pass
    when the holder has just ended."""
    try:
        element = _migration_status(rpc, name)
    except RPCException as e:
        _run_failed(task, owner, f"{name}: status probe failed on {leader_id}: {e}")
    if not isinstance(element, dict):
        _run_failed(task, owner, f"{name}: status probe on {leader_id} answered no migration")
    if element.get("type") == MIGRATION_TYPE_RESYNC:
        return
    if element.get("status") in MIGRATION_ACTIVE_STATES:
        raise TaskDefer(f"{name}: migration slot held by a type {element.get('type')} "
                        f"migration on {leader_id}")
    raise TaskDefer(f"{name}: migration slot just freed on {leader_id}")


def _start(task, cluster, owner, leader, rpc, distribs) -> NoReturn:
    qos_high_priority = cluster.is_qos_set()
    leader_id = leader.get_id()
    started, in_sync = [], []
    for name in distribs:
        task = checkpoint(task)
        if task is None:
            raise TaskAbort("canceled")
        try:
            answer = rpc.distr_migration_replica_sync_start(
                name, qos_high_priority, job_size=constants.MIG_JOB_SIZE,
                jobs=constants.MIG_PARALLEL_JOBS)
        except RPCRemoteError as e:
            if e.code == errno.EALREADY:
                in_sync.append(name)
                continue
            if e.code == errno.EAGAIN:
                raise TaskDefer(f"{name}: sync status not loaded yet on {leader_id}") from e
            if e.code != errno.EBUSY:
                _run_failed(task, owner, f"{name}: start refused on {leader_id}: {e} (code {e.code})")
            _probe_busy(task, owner, leader_id, rpc, name)
            answer = True
        except RPCException as e:
            _run_failed(task, owner, f"{name}: start failed on {leader_id}: {e}")
        if answer is not True:
            _run_failed(task, owner, f"{name}: start refused on {leader_id}: answered {answer!r}")
        started.append(name)
    resync = {"leader_id": leader_id, "started_at": time.time(), "distribs": started}
    if checkpoint(task, resync=resync) is None:
        raise TaskAbort("canceled")
    result = f"catch-up started on {leader_id}: {started}; already synced: {in_sync}"
    logger.info("LVS %s: %s", owner.lvstore, result)
    raise TaskProgress(result)


def _poll(task, owner, rpc, resync, distribs):
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

    # Every started catch-up has ended. Only the status of the node that ran
    # it confirms the zones are equal, for every distrib of the LVS (one that
    # answered EALREADY at the start included); an event received after this
    # point may be a newer desync that answer does not cover.
    seq_limit = db.get_sync_state(task.cluster_id, lvs_name)["seq"]
    for name in distribs:
        try:
            answer = rpc.distr_sync_replication_status(name)
        except RPCException as e:
            _run_failed(task, owner, f"{name}: sync status query failed: {e}")
        status = answer[0].get("status") if isinstance(answer, list) and answer else None
        if status != "synced":
            _run_failed(task, owner, f"{name}: {status} after the catch-up")
    duration = round(time.time() - resync["started_at"], 1)
    # Closed by the spec's terminal commit (finish_task), with the LVS state.
    if checkpoint(task, converged={"seq": seq_limit, "duration_sec": duration,
                                   "leader_id": resync["leader_id"]}) is None:
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

    # A lost site comes first, whatever else is going on: the catch-up waits
    # for its return. A catch-up not started yet also waits for its lagging
    # zone(s) to be up.
    nodes = db.get_storage_nodes_by_cluster_id(task.cluster_id)
    wait_reason = sync_replication_controller.resync_wait_reason(cluster, owner, nodes, task)
    if wait_reason:
        raise TaskDefer(wait_reason)
    site_activation = activation_gate.site_activation_blocks(cluster)
    if site_activation:
        raise TaskDefer(site_activation)
    if cluster.status not in Cluster.OPERABLE_STATUSES:
        raise TaskDefer(f"cluster is {cluster.status}, waiting")
    if owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX):
        raise TaskDefer(f"leadership of {lvs_name} is moving ({owner.lvs_active_site}), waiting")

    leader, _ = storage_node_ops.find_leader_with_failover([owner], lvs_name, recover=False)
    if leader is None:
        raise TaskDefer(f"no leader of {lvs_name} in its active triplet, waiting")
    leader_id = leader.get_id()
    rpc = leader.rpc_client(timeout=5, retry=2)
    distribs = [bdev["name"] for bdev in owner.lvstore_stack or []
                if bdev.get("type") == "bdev_distr"]
    resync = task.function_params.get("resync")
    if not resync:
        task = _guard(task, owner, leader_id, distribs)
        _start(task, cluster, owner, leader, rpc, distribs)
    if resync["leader_id"] != leader_id:
        # The new leader runs the catch-up from the start; not a failed run.
        if drop_params(task, "resync") is None:
            raise TaskAbort("canceled")
        raise TaskDefer(f"leadership moved from {resync['leader_id']} to {leader_id} "
                        f"during the catch-up; restarting on {leader_id}")
    _poll(task, owner, rpc, resync, distribs)


def finish_task(task, ending, result):
    """The spec's terminal commit. A converged run closes the catch-up in one
    transaction with the LVS state key (DBController.finish_sync_resync): DONE,
    with a result naming the leader that ran it, or SUSPENDED again - a re-arm
    the driver runs on its next cycle - when a zone desync newer than the
    run's ``synced`` answer arrived. Every other ending is the driver's own
    DONE commit."""
    converged = task.function_params.get("converged")
    if ending is not Ending.SUCCEEDED or not converged:
        return finish_done(task, result, context=SPEC.name)
    lvs_name = task.function_params.get("lvs_name", "")
    duration = converged["duration_sec"]
    if db.finish_sync_resync(task, lvs_name, converged["seq"],
                             f"synced on {converged['leader_id']}; catch-up took {duration}s", duration):
        logger.info("LVS %s: zones synced on %s, catch-up took %ss",
                    lvs_name, converged["leader_id"], duration)
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
