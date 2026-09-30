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

A disaster fail-over (``lost_site`` in the params: the forced promote of a site
T that is lost) judges every gate by the disaster gate of T
(sync_replication_controller.check_disaster_gate) and, before any marker,
runs the site steps (_site_steps) unless the cluster records them as done:
T proven down (_site_loss_problems), ``lost_site = T`` / ``fencing``, every T
device ``unavailable`` in every distrib of the surviving site, the T nodes
offline, every other volume of every LVS led from T fenced there, T proven
down again, ``done``. A retry that finds ``fencing`` redoes them. Each LVS's
hand-off is preceded by a check that none of its T members leads it.
"""
import time
from datetime import UTC, datetime

from simplyblock_core import db_controller, distr_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, tasks_controller
from simplyblock_core.controllers import sync_replication_controller as sync_ctl
from simplyblock_core.exceptions import PreconditionError, SyncAnaError, SyncGateError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.snode_client import SNodeClientException

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


def _gate(cluster_id, when, lost_site="", lvs_names=()):
    """The planned gate, or with ``lost_site`` the disaster gate of that site
    over ``lvs_names`` (the LVS the promote moves: another LVS of the lost
    site blocks only its own promote)."""
    try:
        if lost_site:
            sync_ctl.check_disaster_gate(cluster_id, lost_site, lvs_names)
        else:
            sync_ctl.check_gate(cluster_id)
    except SyncGateError as e:
        raise _PromoteFailed(f"gate failed {when}: {e}") from e


# ---------------------------------------------------------------------------
# disaster fail-over: the site steps
# ---------------------------------------------------------------------------

def _spdk_answers(node) -> bool:
    """Whether the management plane sees ``node``'s SPDK running: the node
    API says the process is up, or its RPC answers at all."""
    try:
        is_up, _ = node.client(timeout=5, retry=1).spdk_process_is_up(node.rpc_port, node.cluster_id)
        if is_up:
            return True
    except SNodeClientException as e:
        logger.info("Node %s: spdk_process_is_up probe failed: %s", node.get_id(), e)
    try:
        node.rpc_client(timeout=3, retry=0).get_version()
        return True
    except RPCException:
        return False


def _data_plane_gone(node) -> bool:
    from simplyblock_core.services import storage_node_monitor
    return storage_node_monitor.is_node_data_plane_disconnected(node, fresh=True)


def _site_loss_problems(cluster_id, site) -> list[str]:
    """Why ``site`` cannot be taken as lost now: a node of it (not removed)
    whose status says SPDK may run, whose SPDK the management plane sees
    running, or that the surviving site's data plane still reaches (a fresh
    vote; no voter at all proves nothing). Status alone is no evidence: an
    UNREACHABLE node may still serve IO."""
    problems = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.site != site or node.status == StorageNode.STATUS_REMOVED:
            continue
        nid = node.get_id()
        if node.status in sync_ctl.SITE_RUNNING_STATES:
            problems.append(f"node {nid} is {node.status}")
        elif _spdk_answers(node):
            problems.append(f"node {nid}: SPDK answers the management plane")
        elif not _data_plane_gone(node):
            problems.append(f"node {nid}: the surviving site's data plane does not report it gone")
    return problems


def _require_site_lost(cluster_id, site, when):
    problems = _site_loss_problems(cluster_id, site)
    if problems:
        raise _PromoteFailed(f"site {site} is not proven lost {when}: " + "; ".join(problems))


#: Surviving-site node states that may turn ONLINE again without a restart
#: that rebuilds their cluster map from the DB (storage_node_ops
#: _ALLOWED_PRE_STATUSES_FOR_ONLINE minus RESTARTING / IN_CREATION, which do):
#: each must acknowledge the lost devices.
_ACK_REQUIRED = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                 StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SUSPENDED)

#: Device states a site fence does not send: never in a cluster map / gone.
_FENCE_SKIPPED = (NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_REMOVED)
#: Terminal device states a site fence keeps (device_controller refuses to
#: leave them) and delivers as they are.
_FENCE_KEPT = (NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED)


def _fence_devices(cluster_id, site, acked: set):
    """Every device of ``site`` lost to the surviving site: ``unavailable`` in
    the DB first (a node that rebuilds its map on the way to ONLINE reads it
    there) - a failed one keeps its terminal state - then delivered with its
    status to every surviving-site node in an _ACK_REQUIRED state not yet in
    ``acked``, one strict RPC per node posted to every distrib it hosts (an
    earlier best-effort event a distrib missed is covered too). A node that
    does not acknowledge fails the pass."""
    statuses = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.site != site or node.status == StorageNode.STATUS_REMOVED:
            continue
        for dev in node.nvme_devices:
            if dev.status in _FENCE_SKIPPED:
                continue
            if dev.status in _FENCE_KEPT:
                statuses.append((dev, dev.status))
                continue
            if dev.status != NVMeDevice.STATUS_UNAVAILABLE:
                device_controller.device_set_unavailable(dev.get_id())
            statuses.append((dev, NVMeDevice.STATUS_UNAVAILABLE))
    failed = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if (node.site == site or node.get_id() in acked
                or node.status not in _ACK_REQUIRED):
            continue
        if distr_controller.send_dev_statuses_to_node(node, statuses):
            acked.add(node.get_id())
        else:
            failed.append(f"{node.get_id()} ({node.status})")
    if failed:
        raise _PromoteFailed(f"the devices of site {site} could not be marked unavailable on "
                             f"{failed}")


def _offline_site_nodes(cluster_id, site):
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if (node.site != site
                or node.status in (StorageNode.STATUS_REMOVED, StorageNode.STATUS_OFFLINE)):
            continue
        if not storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_OFFLINE,
                                                caused_by="sync_site_lost"):
            raise _PromoteFailed(f"node {node.get_id()} of site {site} could not be set offline "
                                 f"(it is {node.status})")


def _set_lost_site_state(cluster_id, site, state, expect_states) -> bool:
    """Compare-and-set of the cluster's ``lost_site`` / ``lost_site_state``:
    ``site`` / ``state`` when the stored pair is ``site`` (or unset) with a
    state in ``expect_states``."""
    written = {"ok": False}

    def _mutate(c):
        written["ok"] = False    # atomic_update may replay this
        if c.lost_site not in ("", site) or c.lost_site_state not in expect_states:
            return False
        if c.lost_site == site and c.lost_site_state == state:
            written["ok"] = True
            return False
        c.lost_site, c.lost_site_state = site, state
        written["ok"] = True
        return True
    db.atomic_update(db.get_cluster_by_id(cluster_id), _mutate)
    return written["ok"]


def _site_steps(task, site):
    """The site steps of a disaster fail-over of ``site``, all before any
    marker or hand-off; skipped once the cluster records them ``done``, redone
    (idempotent) from ``fencing``."""
    cluster_id = task.cluster_id
    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.lost_site == site and cluster.lost_site_state == sync_ctl.LOST_SITE_DONE:
        return
    if cluster.lost_site not in ("", site):
        raise _PromoteFailed(f"site {cluster.lost_site} is already lost")
    _require_site_lost(cluster_id, site, "before the fence")
    if not _set_lost_site_state(cluster_id, site, sync_ctl.LOST_SITE_FENCING,
                                ("", sync_ctl.LOST_SITE_FENCING)):
        raise _PromoteFailed(f"the lost-site record changed before the fence of {site}")
    logger.warning("Sync promote task %s: site %s lost, fencing", task.uuid, site)
    acked: set = set()
    _fence_devices(cluster_id, site, acked)
    _offline_site_nodes(cluster_id, site)
    sync_ctl.demote_site_volumes(db, cluster_id, site, task.function_params["lvol_ids"])
    _require_site_lost(cluster_id, site, "before the fence is recorded done")
    # A node that turned ack-required since the first pass (e.g. a restart
    # that built its map before the DB write) gets the devices now.
    _fence_devices(cluster_id, site, acked)
    if not _set_lost_site_state(cluster_id, site, sync_ctl.LOST_SITE_DONE,
                                (sync_ctl.LOST_SITE_FENCING,)):
        raise _PromoteFailed(f"the lost-site record changed during the fence of {site}")
    logger.warning("Sync promote task %s: fence of site %s done", task.uuid, site)


def _lost_members_not_leading(lvs_name, owner_id, site):
    """Right before a disaster hand-off of ``lvs_name``: no member of it on the
    lost ``site`` may lead it. A member counts when it answers and does not
    report the leadership (rebuilt non-leader under the lost-site rule), or
    when it is proven down (its SPDK not seen by the management plane and gone
    from the surviving site's data plane)."""
    owner = db.get_storage_node_by_id(owner_id)
    problems = []
    for node_id in storage_node_ops._lvs_member_ids(owner):
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.site != site or node.status == StorageNode.STATUS_REMOVED:
            continue
        try:
            ret = node.rpc_client(timeout=5, retry=1).bdev_lvol_get_lvstores(lvs_name)
        except RPCException:
            ret = None
        else:
            if not (ret and ret[0].get("lvs leadership")):
                continue
            problems.append(f"{node_id} reports the leadership")
            continue
        if _spdk_answers(node) or not _data_plane_gone(node):
            problems.append(f"{node_id} is neither answering as a non-leader nor proven down")
    if problems:
        raise _PromoteFailed(f"LVS {lvs_name}: members on the lost site {site}: "
                             + "; ".join(problems))


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
    lost = task.function_params.get("lost_site", "")
    request_ids = list(task.function_params["lvol_ids"])

    def check(cluster, owner, volumes, expect):
        if lost:
            return sync_ctl.disaster_move_problems(cluster, owner, volumes, expect, site, lost,
                                                   request_ids)
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
    lost = params.get("lost_site", "")
    requested = sorted(params["owners"])
    if lost:
        # Before any change: the disaster gate, then the site steps.
        _gate(task.cluster_id, "before the site steps", lost, requested)
        _site_steps(task, lost)
    to_move, resumed = _classify(task, site)
    if to_move:
        _gate(task.cluster_id, "before the move", lost, requested)
        _mark_moving(task, task.cluster_id, to_move, site)
    for lvs_name in sorted([*to_move, *resumed]):
        task = _checkpoint(task)
        # Live, right before each hand-off - a resumed move and the second LVS
        # of a group too: the state may have changed since the marker.
        _gate(task.cluster_id, f"before the transfer of {lvs_name}", lost, [lvs_name])
        if lost:
            _lost_members_not_leading(lvs_name, params["owners"][lvs_name], lost)
        _transfer(task, lvs_name, site)
    # Live again before the paths open - also for a task that moves nothing
    # (the LVS is led from S already); one check for all volumes of the
    # request, as a group demote has.
    _gate(task.cluster_id, "before opening the volumes", lost, requested)
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
