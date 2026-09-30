"""Sync-replication promote runner (``FN_SYNC_PROMOTE``).

A promote (sync_replication_controller.sync_promote_lvol / _group) queues one
task per request; this runner does it in ONE pass and ends the task DONE,
successful or not - no in-task retry: the caller's next promote call judges
the promote table again.

Per LVS the task may move (``owners``), led from the other site T:

1. the planned gate, live, outside any transaction;
2. under the LVS's site-rule lock, one FDB transaction re-checks the DB-only
   part (no lost site, ``lvs_active_site`` unchanged, every volume of the LVS
   demoted on T, no leadership grant in progress) and marks the LVS
   ``moving:<S>``, recording in the task the site it had (``moves``);
3. the live gate again, right before this LVS's hand-off (also for a move a
   restarted pass resumes) - failing now writes the previous site back;
4. the fenced leadership hand-off to the primary of the S triplet
   (storage_node_ops.move_lvs_leadership), recorded in ``transferring``
   before it starts; the marker then settles on S.

Then, after one more live gate, every volume of the request is opened on S
under the site-rule lock: the strict ANA open first, ``sync_active_site = S``
after; a volume being deleted is refused (and closed again when its delete
started during the open).

Any failure or a cancel ends the task DONE FIRST (it then no longer owns the
move), then settles every marker it set: the previous site written back for
an LVS whose hand-off never started, storage_node_ops.reconcile_lvs_move for
one whose hand-off did (the leader decides). A pass that restarts after a
crash finds its markers by ``moves`` and goes on from the DB state.
"""
import time
from datetime import UTC, datetime

from simplyblock_core import db_controller, storage_node_ops, utils
from simplyblock_core.controllers import sync_replication_controller as sync_ctl
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.exceptions import PreconditionError, SyncAnaError, SyncGateError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol

logger = utils.get_logger(__name__)

db = db_controller.DBController()


class _PromoteFailed(Exception):
    """Ends the pass: the task is recorded DONE with this reason."""


class _PromoteCanceled(_PromoteFailed):
    pass


def _save(task, mutate):
    """Apply ``mutate`` to the task as stored NOW (never a whole stale copy:
    that would erase a cancel or the journal) and stamp ``updated_at``."""
    def _apply(fresh):
        mutate(fresh)
        fresh.updated_at = str(datetime.now(UTC))
    return db.atomic_update(task, _apply) or task


def _checkpoint(task):
    """The task as stored now; a cancel ends the pass here."""
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        raise _PromoteCanceled("canceled")
    return task


def _settle_markers(task) -> None:
    """Settle every ``moving:`` marker the (DONE) task still holds."""
    params = task.function_params
    moving = storage_node_ops.lvs_moving_value(params["site"])
    transferring = set(params.get("transferring", []))
    for lvs_name, previous in sorted((params.get("moves") or {}).items()):
        owner_id = params["owners"][lvs_name]
        try:
            owner = db.get_storage_node_by_id(owner_id)
        except KeyError:
            continue
        if owner.lvs_active_site != moving:
            continue
        if lvs_name in transferring:
            settled = storage_node_ops.reconcile_lvs_move(owner_id)
            logger.warning("LVS %s: move of an ended promote reconciled to %r", lvs_name, settled)
        elif storage_node_ops.set_lvs_active_site(owner_id, previous, expect=moving):
            logger.warning("LVS %s: move of an ended promote undone, lvs_active_site=%r",
                           lvs_name, previous)


def _end(task, result) -> bool:
    def done(t):
        t.status = JobSchedule.STATUS_DONE
        t.function_result = result
    task = _save(task, done)
    logger.info("Sync promote task %s: %s", task.uuid, result)
    _settle_markers(task)
    return True


def _gate(cluster_id, when):
    try:
        sync_ctl.check_gate(cluster_id)
    except SyncGateError as e:
        raise _PromoteFailed(f"gate failed {when}: {e}") from e


def _classify(task, site):
    """``(to_move, resumed)``: the LVS still led from the other site
    (``{lvs: (owner id, lvs_active_site)}``) and the LVS this task already
    marked moving to ``site``."""
    params = task.function_params
    moving = storage_node_ops.lvs_moving_value(site)
    to_move, resumed = {}, []
    for lvs_name, owner_id in sorted(params["owners"].items()):
        try:
            owner = db.get_storage_node_by_id(owner_id)
        except KeyError as e:
            raise _PromoteFailed(f"LVS {lvs_name}: owner {owner_id} not found") from e
        active = owner.lvs_active_site
        if active == moving and lvs_name in (params.get("moves") or {}):
            resumed.append(lvs_name)
        elif active.startswith(storage_node_ops.LVS_MOVING_PREFIX):
            raise _PromoteFailed(f"LVS {lvs_name}: a leadership move is in flight ({active})")
        elif storage_node_ops.lvs_active_site_of(owner) != site:
            to_move[lvs_name] = (owner_id, active)
    return to_move, resumed


def _mark_moving(task, cluster_id, to_move, site):
    def check(cluster, owner, volumes, expect):
        return sync_ctl.promote_move_problems(cluster, owner, volumes, expect, site)

    with storage_node_ops.sync_site_rule_locks(cluster_id, to_move):
        problems = db.begin_sync_promote_moves(task, to_move, site, check)
    if problems:
        raise _PromoteFailed("; ".join(problems))
    logger.info("Sync promote task %s: %s marked moving to %s", task.uuid, sorted(to_move), site)


def _add_transferring(task, lvs_name):
    transferring = task.function_params.setdefault("transferring", [])
    if lvs_name not in transferring:
        transferring.append(lvs_name)


def _transfer(task, lvs_name, site):
    owner_id = task.function_params["owners"][lvs_name]
    moving = storage_node_ops.lvs_moving_value(site)
    owner = db.get_storage_node_by_id(owner_id)
    if owner.lvs_active_site != moving:
        raise _PromoteFailed(f"LVS {lvs_name}: lvs_active_site changed to {owner.lvs_active_site!r}")
    taker_id = sync_ctl.lvs_site_triplet(owner, site)[0]
    leader, _ = storage_node_ops.find_leader_with_failover([owner], lvs_name)
    _save(task, lambda t: _add_transferring(t, lvs_name))
    if leader is not None and leader.get_id() == taker_id:
        # A hand-off that granted before the pass ended: only the marker.
        if not storage_node_ops.set_lvs_active_site(owner_id, site, expect=moving):
            raise _PromoteFailed(f"LVS {lvs_name}: marker changed")
        return
    try:
        storage_node_ops.move_lvs_leadership(
            owner_id, moving, site, current_leader_id=leader.get_id() if leader is not None else None,
            taker_id=taker_id)
    except (storage_node_ops.LVSMoveChangedError, storage_node_ops.LeadershipTransferError,
            PreconditionError) as e:
        raise _PromoteFailed(f"LVS {lvs_name}: leadership transfer to {taker_id} failed: {e}") from e
    logger.info("Sync promote task %s: LVS %s led from %s", task.uuid, lvs_name, site)


#: A volume in these states is never opened: it is not fully created, or its
#: namespace is being (or has been) torn down. A served volume enters
#: deletion only through lvol_controller.delete_lvol, which takes the
#: site-rule lock for that transition (_mark_in_deletion).
_NOT_SERVABLE = (LVol.STATUS_IN_CREATION, LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)


def _open(cluster_id, lvol_id, site):
    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError as e:
        raise _PromoteFailed(f"volume {lvol_id} not found") from e
    with storage_node_ops.sync_site_rule_locks(cluster_id, [lvol.lvs_name]):
        try:
            lvol = db.get_lvol_by_id(lvol_id)
        except KeyError as e:
            raise _PromoteFailed(f"volume {lvol_id} not found") from e
        if lvol.status in _NOT_SERVABLE:
            raise _PromoteFailed(f"volume {lvol_id} is {lvol.status}")
        owner = db.get_storage_node_by_id(lvol.node_id)
        if (owner.lvs_active_site.startswith(storage_node_ops.LVS_MOVING_PREFIX)
                or storage_node_ops.lvs_active_site_of(owner) != site):
            raise _PromoteFailed(f"volume {lvol_id}: LVS {lvol.lvs_name} is not led from {site}")
        if sync_ctl.volume_open_on(lvol, owner, site):
            return
        nodes = sync_ctl.site_triplet_nodes(db, owner, site)
        try:
            sync_ctl.set_site_ana_strict(lvol, nodes, open_site=True)
        except SyncAnaError as e:
            raise _PromoteFailed(f"volume {lvol_id}: {e}") from e
        recorded = {"ok": False}

        def _opened(v):
            recorded["ok"] = False    # atomic_update may replay this
            if v.status in _NOT_SERVABLE:
                return False
            v.sync_active_site = site
            v.sync_demoted_sites = [s for s in v.sync_demoted_sites if s != site]
            recorded["ok"] = True
            return True
        db.atomic_update(lvol, _opened)
        if not recorded["ok"]:
            # Defense in depth - delete_lvol cannot get here (it takes this
            # lock for the transition): a status change without the lock
            # after the read above. Close what was just opened, record nothing.
            try:
                sync_ctl.set_site_ana_strict(lvol, nodes, open_site=False)
            except SyncAnaError as e:
                raise _PromoteFailed(f"volume {lvol_id} left servable: its status changed during "
                                     f"the open and closing its paths again failed: {e}") from e
            raise _PromoteFailed(f"volume {lvol_id}: its status changed during the open")
    logger.info("Volume %s promoted on site %s", lvol_id, site)


def _run(task):
    params = task.function_params
    site = params["site"]
    cluster = db.get_cluster_by_id(task.cluster_id)
    if not cluster.sync_replication:
        raise _PromoteFailed("not a sync-replication cluster")
    to_move, resumed = _classify(task, site)
    if to_move:
        _gate(task.cluster_id, "before the move")
        _mark_moving(task, task.cluster_id, to_move, site)
    for lvs_name in sorted([*to_move, *resumed]):
        task = _checkpoint(task)
        # Live, right before each hand-off - a resumed move and the second LVS
        # of a group too: the state may have changed since the marker.
        _gate(task.cluster_id, f"before the transfer of {lvs_name}")
        _transfer(task, lvs_name, site)
    # Live again before the paths open - also for a task that moves nothing
    # (the LVS is led from S already); one check for all volumes of the
    # request, as a group demote has.
    _gate(task.cluster_id, "before opening the volumes")
    for lvol_id in params["lvol_ids"]:
        task = _checkpoint(task)
        _open(task.cluster_id, lvol_id, site)
    return f"promoted {len(params['lvol_ids'])} volume(s) to site {site}"


def task_runner(task):
    """One pass over ``task``; it always ends DONE. True when it finished."""
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        return _end(task, "canceled")
    if task.status == JobSchedule.STATUS_NEW:
        task = _save(task, lambda t: setattr(t, "status", JobSchedule.STATUS_RUNNING))
    try:
        result = _run(task)
    except _PromoteCanceled:
        return _end(task, "canceled")
    except _PromoteFailed as e:
        return _end(task, f"failed: {e}")
    return _end(task, result)


def _record_unexpected_error(task, error) -> None:
    """An error the pass does not expect (a DB or RPC error outside the
    promote's own failure paths) leaves the task as it was, to be resumed by
    the next loop; after ``max_retry`` of them it ends like a failure (its
    markers settled), so a persistent error never keeps an LVS moving."""
    def bump(t):
        t.retry += 1
        t.function_result = f"unexpected error (attempt {t.retry}/{t.max_retry}): {error}"
    task = _save(task, bump)
    if task.status != JobSchedule.STATUS_DONE and task.retry >= task.max_retry:
        _end(task, f"failed: max retry reached after unexpected errors, last: {error}")


def main():
    logger.info("Starting sync-replication promote runner...")
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
            for task in db.get_active_sync_promote_tasks(cluster.get_id()):
                if not tasks_controller.claim_task(task):
                    logger.info(f"Promote task {task.uuid} owned by another runner host; skipping")
                    continue
                try:
                    # The live lease is what makes the task own its moves
                    # (storage_node_ops.sync_promote_task_owns_move).
                    with tasks_controller.task_lease_heartbeat(task):
                        task_runner(task)
                except Exception as e:
                    logger.exception(f"Promote task {task.uuid} failed: {e}")
                    _record_unexpected_error(task, e)
        time.sleep(3)


if __name__ == "__main__":
    main()
