"""Shared pieces of the three device-migration runners.

``tasks_runner_migration`` (FN_DEV_MIG), ``tasks_runner_new_dev_migration``
(FN_NEW_DEV_MIG) and ``tasks_runner_failed_migration`` (FN_FAILED_DEV_MIG) all
drive the same data-plane operation — start a distr migration, then poll
``distr_migration_status`` until it settles — and differ only in what they
migrate and when they are allowed to start.
"""
from datetime import UTC, datetime

from simplyblock_core import db_controller, storage_node_ops, utils
from simplyblock_core.controllers import activation_gate, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services.task_runner_base import (
    TaskAbort,
    TaskDefer,
    TaskProgress,
    TaskRetry,
    checkpoint,
    drop_params,
    set_result,
)

logger = utils.get_logger(__name__)

db = db_controller.DBController()

MIGRATION_WAIT_UNAVAILABLE_KEY = "wait_unavailable_before_retry"

# A migration may only start once the cluster has been serving for a moment:
# a node that just came back is still settling its lvstore.
NODE_SETTLE_SEC = 60


def migration_started(task):
    """Whether this task has already issued its data-plane migration.

    The runners used to read this off ``task.status`` being RUNNING. That no
    longer distinguishes anything — the driver moves a task to RUNNING before
    calling the handler — so the marker the start step writes is used directly,
    which is what the status was standing in for anyway.
    """
    return "migration" in task.function_params


def defer_unless_operable(cluster):
    """Raise :class:`TaskDefer` while ``cluster`` does not allow operations.

    A deferral, not a failure: the cluster being suspended, in activation or
    in expansion is external state the task waits on, and the driver contract
    reserves ``TaskRetry`` for a failed attempt. Raising ``TaskRetry`` here
    consumed a retry per poll for as long as an outage lasted, and the
    driver's exponential backoff (interval * 2**(retry-1), capped at
    RESTART_TASK_EXEC_INTERVAL_MAX_SEC) then kept the migrations silent long
    after the cluster was back: 2026-10-14 run 63, sixteen failed-device
    migrations at retry 12 did not resume for the hour after activation while
    the node removal that waited on them sat in MigratingDevices. On a regular
    cluster nothing else wakes them, since ``backoff_reset_key`` only acts on
    single-node and two-node clusters.

    IN_SHRINK is operable: draining data off the departing node is the node
    removal's own work, and removal blocks until every data device reaches
    FAILED_AND_MIGRATED — which only this family sets. Refusing here would
    deadlock the removal against the status it set itself.
    """
    # A running sync-replication site activation holds every migration. A hold,
    # not a failure: no retry consumed and no backoff, so the migration resumes
    # on the first pass after the activation ends.
    site_activation = activation_gate.site_activation_blocks(cluster)
    if site_activation:
        raise TaskDefer(site_activation)
    if not cluster.allows_operation():
        raise TaskDefer(f"cluster is {cluster.status}, deferring until it allows operations")


def require_active_cluster(task):
    cluster = db.get_cluster_by_id(task.cluster_id)
    defer_unless_operable(cluster)

    # Expansion-first ordering: no data migration runs while a cluster
    # expansion is open — even between the expand task's retries, when the
    # cluster status is momentarily ACTIVE. Without this, an outage's recovery
    # migrations would start in that window and then block the expansion
    # resume, inverting the required order (expansion completes FIRST, then
    # outage device migration, then expansion migration). A deferral, not a
    # failure: no retry consumed.
    if tasks_controller.get_active_cluster_expand_task(task.cluster_id):
        raise TaskDefer("cluster expansion in progress, deferring")


def require_node(task):
    try:
        return db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort(f"Node not found: {task.node_id}")


def place_on_role_node(task, snode):
    """Make sure that a sync-replication migration task that has not started
    is on the node that must run it.

    That node is the primary of the LVS's active triplet
    (tasks_controller.migration_role_node). A promote can change it after
    the task was queued. When this function returns, ``task`` is on the
    right node, and the runner continues on ``snode`` (the task's node) in
    the same way as on a cluster without sync replication.

    - A task on another node is moved to the right node and deferred. The
      next runner pass checks it against the new node's start conditions.
    - While the LVS is being moved, the task is deferred and stays where it is.
    - When the primary is departing, a re-balance or new-device task is
      skipped (it ends DONE). A failed-device task waits while every member of
      the active triplet is departing.
    - A started task stays on its node, and the runner polls it there.

    A sync-replication task holds the LVS name and its owner (``lvs_name``,
    ``lvs_owner``). A task on a sync-replication node without them is
    aborted. All other tasks are not changed.
    """
    params = task.function_params
    distr_name = params.get("distr_name")
    if "lvs_owner" not in params:
        if storage_node_ops._sync_site(snode):
            raise TaskAbort(f"{task.function_name} of {distr_name} on {task.node_id} has no lvs_owner: "
                            f"a sync-replication migration task must name its LVS owner")
        return
    if migration_started(task):
        return

    lvs_name = params["lvs_name"]
    try:
        owner = db.get_storage_node_by_id(params["lvs_owner"])
    except KeyError:
        raise TaskAbort(f"Owner {params['lvs_owner']} of {lvs_name} not found")
    try:
        placement = tasks_controller.migration_role_node(owner, task.function_name)
    except KeyError as e:
        raise TaskAbort(str(e))

    if placement.reason == tasks_controller.PLACEMENT_MOVING:
        raise TaskDefer(f"{lvs_name} (owner {owner.get_id()}) is being moved ({owner.lvs_active_site}); "
                        f"placing {task.function_name} of {distr_name} once the move settles")
    if placement.node is None:
        members = storage_node_ops.lvs_active_triplet(owner).node_ids
        if task.function_name == JobSchedule.FN_FAILED_DEV_MIG:
            raise TaskDefer(f"every member of the active triplet of {lvs_name} ({', '.join(members)}) "
                            f"is departing; waiting")
        raise TaskAbort(f"skipped: primary {members[0]} of {lvs_name} is departing")

    target_id = placement.node.get_id()
    if target_id == task.node_id:
        return
    if move_unstarted_task(task, target_id):
        raise TaskDefer(f"moved {task.function_name} of {distr_name} ({lvs_name}) from {task.node_id} "
                        f"to {target_id}, the primary of its active triplet")
    raise TaskDefer(f"{task.function_name} of {distr_name} ({lvs_name}) changed on {task.node_id} "
                    f"while being moved to {target_id}")


def move_unstarted_task(task, node_id):
    """Move ``task`` to ``node_id`` in one atomic update. The move happens only
    if the stored task is still open, not started, and on ``task.node_id``.
    Return True if the task moved."""
    old_node_id = task.node_id
    won = {"ok": False}

    def _mutate(fresh):
        won["ok"] = False
        if (fresh.status == JobSchedule.STATUS_DONE or fresh.canceled
                or migration_started(fresh) or fresh.node_id != old_node_id):
            return False
        fresh.node_id = node_id
        won["ok"] = True
        return True

    return db.atomic_update(task, _mutate) is not None and won["ok"]


def nodes_settled(cluster_id, primaries_only=False):
    """False while any node has been back for less than NODE_SETTLE_SEC.

    The three runners disagree on whether waiting for this costs a retry, so
    the verdict is returned rather than raised.

    ``online_since`` is stamped timezone-aware (storage_node_ops.py), so the
    comparison must be too. A naive ``datetime.now()`` raises TypeError on
    every call, the except below swallows it, and the whole guard silently
    never fires — failed-device migrations then start immediately against a
    node that has just come back, which is precisely what this wait exists to
    prevent (246 occurrences across two node removals on 2026-09-11, from the
    one call site of four that had been missed). Having a single owner for the
    check is what keeps the four from drifting apart again.
    """
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if primaries_only and node.is_secondary_node:
            continue
        if not node.online_since:
            continue
        try:
            settled_for = datetime.now(UTC) - datetime.fromisoformat(node.online_since)
        except Exception as e:
            logger.error(f"Failed to get online since: {e}")
            continue
        if settled_for.total_seconds() < NODE_SETTLE_SEC:
            return False
    return True


def cluster_unavailable_state(cluster_id):
    """Nodes and devices that are neither serving nor written off, as stable
    ids — the set a migration waits on before (re)starting.

    A node that is LEAVING must never appear here. This list is a wait
    condition -- require_recovery_progress holds the task until something in
    it recovers -- so listing a node that is being removed is waiting for an
    event that cannot happen.

    It deadlocked the removal it was waiting on (2026-09-15, cluster
    a6e7569d). Recovering a node kicked off balancing_on_restart with 10
    device_migration subtasks; every one of them suspended on
    "waiting for unavailable nodes/devices to recover: ['node:a1b050f1']" --
    the node under removal. The master task never finished, so the cluster
    stayed ACTIVE-REBALANCING, so create_migration refused ("Cluster is
    rebalancing; wait for it to finish before migrating"), so the removal's
    drain could never run, so the node could never finish leaving and
    "recover". Each side waited for the other.

    IN_CREATION is excluded for the opposite reason: not gone, not yet
    arrived. Devices are covered by their own REMOVED / FAILED_AND_MIGRATED
    skip below.
    """
    unavailable = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]:
            continue
        if node.status in StorageNode.DEPARTING_STATUSES:
            continue
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            unavailable.append(f"node:{node.get_id()}")
        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                continue
            if dev.status != NVMeDevice.STATUS_ONLINE:
                unavailable.append(f"dev:{dev.get_id()}")
    return sorted(unavailable)


def require_recovery_progress(task, unavailable):
    """Hold a migration back while the cluster is degraded, releasing it only
    when something actually recovers.

    Retrying against an unchanged set of unavailable nodes/devices just burns
    the budget, so the set is recorded on the task and compared: an unchanged
    (or grown) set defers, while any member coming back is the recovery event
    that lets the migration proceed.
    """
    previous = sorted(task.function_params.get(MIGRATION_WAIT_UNAVAILABLE_KEY, []))

    if not unavailable:
        if previous:
            drop_params(task, MIGRATION_WAIT_UNAVAILABLE_KEY)
        return

    recovered = set(previous) - set(unavailable)
    checkpoint(task, **{MIGRATION_WAIT_UNAVAILABLE_KEY: unavailable})
    if previous and recovered:
        logger.info("Migration retry allowed after recovery event for task %s: %s",
                    task.uuid, sorted(recovered))
        return

    raise TaskDefer("waiting for unavailable nodes/devices to recover before "
                    f"restarting migration: {unavailable}")


def start_migration(task, start, **params):
    """Issue the data-plane migration and record that it was issued.

    ``start`` is the runner's own RPC call, returning falsy on failure. The
    marker is checkpointed immediately: a crash between the RPC and the end of
    the handler would otherwise lose it and start a second migration. Any
    ``params`` are recorded in the same commit, so a marker can never land
    without the context written alongside it.
    """
    try:
        started = start()
    except Exception as e:
        logger.error(e)
        started = False
    if not started:
        raise TaskRetry("Failed to start device migration task, retry later")

    return checkpoint(task, migration={"name": task.function_params["distr_name"]},
                      migration_started=datetime.now(UTC).isoformat(), **params)


def _ft_impact_key(task):
    return f"{task.node_id}/{task.function_params.get('distr_name', '')}"


def record_migration_ft_impact(task, entry):
    """Set (``entry``) or clear (None) the distrib's ``Cluster.migration_ft_impact``."""
    key = _ft_impact_key(task)
    cluster = db.get_cluster_by_id(task.cluster_id)
    if entry is None and key not in (cluster.migration_ft_impact or {}):
        return

    def _mutate(c):
        impact = dict(c.migration_ft_impact or {})
        if entry is None:
            if key not in impact:
                return False
            impact.pop(key)
        else:
            impact[key] = entry
        c.migration_ft_impact = impact

    db.atomic_update(cluster, _mutate)


def _node_back_since(cluster_id, started):
    """Whether a node of the cluster came (back) online after ``started``
    (the migration's start, ``migration_started``): an outage that ended during
    the migration, whose errors (source or target not available) and fault
    tolerance verdict (pages on the then offline devices) are not final.
    Unknown (no start recorded, e.g. a migration started before an upgrade, or
    an unparsable stamp) counts as yes: the caller then retries, as before."""
    if not started:
        return True
    try:
        t0 = datetime.fromisoformat(started)
        for node in db.get_storage_nodes_by_cluster_id(cluster_id):
            if node.online_since and datetime.fromisoformat(node.online_since) >= t0:
                return True
    except Exception as e:
        logger.warning("Migration start / node online stamps not comparable (%s); retrying", e)
        return True
    return False


def report_migration_status(task, res, allow_all_errors=False, allowed_error_codes=None):
    """Translate a ``distr_migration_status`` poll into the task's outcome."""
    if not res:
        raise TaskRetry("Failed to get mig status")

    allowed_error_codes = allowed_error_codes or [0]
    res_data = res[0]
    migration_status = res_data.get("status")
    error_code = res_data.get("error", -1)
    progress = res_data.get("progress", -1)

    if migration_status == "completed":
        if error_code == 0:
            record_migration_ft_impact(task, None)
            set_result(task, "Done")
            return
        if error_code in allowed_error_codes or allow_all_errors:
            set_result(task, f"mig completed with status: {error_code}")
            return
        # Drop the marker so the next attempt starts a fresh migration rather
        # than polling the one that just errored.
        drop_params(task, 'migration')
        if cluster_unavailable_state(task.cluster_id):
            # Something is down: the errors may be its doing, retry once it is back.
            raise TaskRetry(f"mig error: {error_code}, retrying")
        if _node_back_since(task.cluster_id, task.function_params.get("migration_started")):
            # Everything is online now, but a node came back during this migration (2026-10-29 soak: a
            # network outage ended 3 s before the migration did; its errors and the pages it counted
            # without full fault tolerance were the outage's). The retry runs with everything online.
            raise TaskRetry(f"mig error: {error_code}, a node came back during the migration, retrying")
        # Every node and device is online, so a retry would hit the same errors.
        # The migration is done; whether the cluster may be ACTIVE depends on
        # whether the pages it could not place are fault tolerant as they are.
        ft_pages = res_data.get("ft_impacted_pages")
        if ft_pages == 0:
            record_migration_ft_impact(task, None)
            logger.warning("Migration %s ended with error %s, all nodes online, "
                           "not retried: the pages it could not place are fault tolerant",
                           task.uuid, error_code)
        else:
            record_migration_ft_impact(task, {
                "error": error_code, "ft_impacted_pages": ft_pages,
                "time": datetime.now(UTC).isoformat()})
            logger.error("Migration %s ended with error %s, all nodes online, not retried: "
                         "%s pages without full fault tolerance, cluster stays degraded",
                         task.uuid, error_code,
                         "unknown number of" if ft_pages is None else ft_pages)
        set_result(task, f"mig completed with error {error_code}, all nodes online, not retried; "
                         f"ft_impacted_pages: {ft_pages}")
        return

    if migration_status == "failed":
        raise TaskAbort(migration_status)

    if migration_status == "none":
        drop_params(task, 'migration')
        raise TaskRetry("mig retry after restart")

    raise TaskProgress(f"Status: {migration_status}, progress:{progress}")


def no_sibling_migration(task):
    """Eligibility: only one migration of a distr runs at a time on a node.
    For a sync-replication task, only one runs at a time on all nodes
    together (tasks_controller.mig_scope_node).

    Only gates a task that has not started yet — once its own migration is
    running it is itself the sibling everything else waits on.
    """
    if migration_started(task):
        return True
    return not tasks_controller.get_active_node_mig_task(
        task.cluster_id, tasks_controller.mig_scope_node(task.node_id, task.function_params),
        task.function_params.get("distr_name"))


def allow_all_migration_errors(cluster_id, statuses):
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            if dev.status in statuses:
                return True
    return False


def poll_migration(task, rpc_client, allow_all_errors=False):
    try:
        res = rpc_client.distr_migration_status(**task.function_params["migration"])
    except (TaskAbort, TaskDefer, TaskProgress, TaskRetry):
        raise
    except Exception as e:
        logger.error("Failed to get migration task status")
        logger.exception(e)
        raise TaskRetry("Failed to get migration status")

    report_migration_status(task, res, allow_all_errors=allow_all_errors)


def qos_high_priority(cluster_id):
    return db.get_cluster_by_id(cluster_id).is_qos_set()


def needs_relaxed_placement(member_nodes, ndcs, npcs):
    """Whether a migration on a cluster of ``member_nodes`` must run relaxed.

    Strict placement puts every chunk of a stripe on a different node. A
    1- or 2-node cluster, or one with fewer nodes than chunks per stripe,
    has no such placement, so a strict migration has nowhere to move data
    and never completes; relocation then needs a third node. ultra already
    relaxes writes in that case on its own, but migrations only relax when
    asked (``relaxed_mode``, default false).
    """
    return member_nodes <= 2 or member_nodes < ndcs + npcs


def migration_relaxed(cluster_id):
    cluster = db.get_cluster_by_id(cluster_id)
    members = [n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
               if n.status != StorageNode.STATUS_REMOVED]
    return needs_relaxed_placement(len(members), cluster.distr_ndcs, cluster.distr_npcs)


def sibling_eligibility(task, cluster):
    """Spec-shaped wrapper of :func:`no_sibling_migration`."""
    return no_sibling_migration(task)


def backoff_reset_key(cluster):
    """What a deferred migration waits on: node statuses and LVS leadership.

    Used as the migration runners' ``RunnerSpec.backoff_reset_key``: a change
    drops the in-memory retry backoff, so a rebuild that failed on "not the
    leader" or on a node being down resumes as soon as that changes. On a
    two-node arbitrated cluster the arbitration record's epoch (every verdict)
    and ``leadership_restored_at`` (leadership reconciliation) stand for
    leadership changes that come without a node status change.

    Small clusters only (single-node, or two-node with arbitration): on every
    other cluster this returns None without reading anything, so the runners
    keep their previous behaviour and FDB load (no reset, no node-list read).
    """
    if not (getattr(cluster, "two_node_arbitration", False) is True
            or getattr(cluster, "is_single_node", False) is True):
        return None
    nodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())
    statuses = tuple(sorted((n.get_id(), n.status) for n in nodes))
    rec = db.get_cluster_arbitration(cluster.get_id()) if getattr(
        cluster, "two_node_arbitration", False) else None
    leadership = (rec.epoch, rec.leadership_restored_at) if rec is not None else ()
    return statuses, leadership
