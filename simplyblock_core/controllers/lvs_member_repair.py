"""Repair of an lvstore member whose copy keeps failing to register volumes.

bf36 R6 (2026-11-25): a tertiary that had been shut down came back, and its copy
of LVS_19 failed every volume registration for 10 hours ("Metadata page is all
zero", "Invalid blobid according to used_blobids"). The control plane alerted
(LVS_MEMBER_REGISTER_FAILING, #1715) but did nothing about it.

The repair here is the control plane's one non-disruptive reload of a member's
copy: ``bdev_lvol_update_lvstore`` (spdk_lvs_update_live with no blob), which
re-reads the blobstore's super block and its masks from disk on a non-leader.
It is the reload the guarded leaderless recovery runs before a grant
(storage_node_ops._recover_leaderless_lvs). It repairs a member whose in-memory
blobstore state went stale (a used-blobid mask it missed: "Invalid blobid
according to used_blobids"). It does NOT repair a member whose data layer
serves stale data: there the reload reads the same wrong pages. That was bf36's
case: the member's distrib placement map stopped because its journal copy was
excluded (spdk #113, ultra #250). Only a restart of the member's storage node
rebuilds that view, so a repair that does not stop the failures says so.

Safety: the member is repaired only when
  * it is ONLINE and no restart/activation/expansion owns the lvstore on it;
  * its own SPDK answers that it does NOT lead the lvstore (the reload is a
    non-leader operation; SPDK refuses it on a leader);
  * another member answers that it DOES lead the lvstore: the broken copy is
    never the only one left serving it.
Rate limits are the caller's (tasks_runner_sync_lvol_del).
"""
from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import replica_chain, storage_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

REPAIRED = "repaired"
SKIPPED = "skipped"
FAILED = "failed"


def lvstore_members(lvs_name, cluster_id, db=None):
    """The members of *lvs_name*: its primary first, then its secondary and
    tertiary (replica_chain.lvstore_replicas). [] when no node owns it."""
    db = db or db_controller.DBController()
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.lvstore == lvs_name and node.status != StorageNode.STATUS_REMOVED:
            return [node] + [n for _role, n in replica_chain.lvstore_replicas(node, db=db)]
    return []


def _preconditions(node, lvs_name, db):
    """``""`` when *node*'s copy of *lvs_name* may be reloaded now, else why not."""
    if node.status != StorageNode.STATUS_ONLINE:
        return f"node is {node.status}"
    try:
        cluster = db.get_cluster_by_id(node.cluster_id)
        if cluster.status == Cluster.STATUS_IN_ACTIVATION:
            return "cluster is in activation"
    except KeyError:
        return "cluster not found"
    if storage_node_ops.get_restart_phase(node.get_id(), lvs_name):
        return "a restart/activation/expansion owns the lvstore on this node"
    try:
        if storage_node_ops._lvs_leadership_reported(node, lvs_name):
            return "the node leads the lvstore (the reload is a non-leader operation)"
    except Exception as e:                                  # noqa: BLE001
        return f"the node does not answer whether it leads the lvstore ({e})"
    others = [m for m in lvstore_members(lvs_name, node.cluster_id, db=db)
              if m.get_id() != node.get_id() and m.status == StorageNode.STATUS_ONLINE]
    leaders, _unknown = storage_node_ops._lvs_leader_probe(others, lvs_name)
    if not leaders:
        return "no other member answers as leader of the lvstore"
    return ""


def repair_member_copy(node, lvs_name, db=None, previous_repair_at=None):
    """Reload *node*'s copy of *lvs_name* (see the module doc).

    Returns ``(outcome, detail)``: REPAIRED, SKIPPED (a safety rule held; no RPC
    sent) or FAILED. Every outcome but SKIPPED raises an event; a skip is only
    logged, since it repeats while the condition holds."""
    db = db or db_controller.DBController()
    why = _preconditions(node, lvs_name, db)
    if why:
        logger.warning("Not reloading %s on %s: %s", lvs_name, node.get_id(), why)
        return SKIPPED, why
    _event(node, lvs_name, "started", "reloading this node's copy of the lvstore "
           "(bdev_lvol_update_lvstore)", previous_repair_at)
    try:
        ok = node.rpc_client(timeout=constants.LVS_MEMBER_REPAIR_RPC_TIMEOUT_SEC,
                             retry=1).bdev_lvol_update_lvstore(lvs_name)
    except Exception as e:                                  # noqa: BLE001
        ok, detail = False, f"bdev_lvol_update_lvstore failed: {e}"
    else:
        detail = "reloaded" if ok else "bdev_lvol_update_lvstore returned False"
    _event(node, lvs_name, "done" if ok else "failed", detail, previous_repair_at)
    return (REPAIRED if ok else FAILED), detail


def _event(node, lvs_name, stage, detail, previous_repair_at):
    try:
        storage_events.snode_lvs_member_repair(node, lvs_name, stage, detail,
                                               previous_repair_at=previous_repair_at)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("Could not log the member repair event: %s", e)


# ---------------- automatic restart when a reload did not help ----------------
#
# A reload re-reads the blobstore; it cannot rebuild a data layer that serves
# stale data (bf36: the member's distrib placement map stopped). A restart of
# the member's node does: its distrib re-reads the journal from the most
# up-to-date copy. When the registrations fail again after a successful
# reload, the control plane restarts the node through the existing unattended
# path for a serving node, the one the suspend recovery uses: a graceful
# shutdown_storage_node(keep_auto_restart=True), then
# tasks_controller.add_node_to_auto_restart, whose FN_NODE_RESTART task the
# restart runner executes. (That runner treats an ONLINE node as healthy and
# does nothing, and add_node_to_auto_restart refuses one: hence the shutdown.)

RESTART_STARTED = "restart_started"
RESTART_SKIPPED = "restart_skipped"
RESTART_MANUAL = "restart_manual"


def _blocking_task_names():
    """Tasks during which no node is restarted for this: a migration or
    replication cutover, an expansion (activation is a cluster status)."""
    from simplyblock_core.models.job_schedule import JobSchedule
    return {JobSchedule.FN_REPLICATION_FINAL, JobSchedule.FN_LVOL_MIG,
            JobSchedule.FN_LVOL_BATCH_MIG, JobSchedule.FN_CLUSTER_EXPAND}


def _member_lvstores(node, db):
    """``[(lvs_name, [other members])]`` for every lvstore *node* belongs to:
    the one it is primary of, and those it serves as secondary or tertiary."""
    primaries = [node] if node.lvstore else []
    for attr in ("lvstore_stack_secondary", "lvstore_stack_tertiary"):
        pid = getattr(node, attr, "")
        if pid:
            try:
                primaries.append(db.get_storage_node_by_id(pid))
            except KeyError:
                continue
    out = []
    for primary in primaries:
        members = [primary] + [n for _r, n in replica_chain.lvstore_replicas(primary, db=db)]
        out.append((primary.lvstore, [m for m in members if m.get_id() != node.get_id()]))
    return out


def restart_gate(node, cluster, db, now):
    """``(stage, why)``: ``("", "")`` when *node* may be restarted now, else
    RESTART_MANUAL (no automation for it) or RESTART_SKIPPED (a gate holds now;
    tried again at the next alert)."""
    from simplyblock_core import floors
    from simplyblock_core.controllers import activation_gate, tasks_controller
    from simplyblock_core.models.job_schedule import JobSchedule

    nodes = [n for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())
             if n.status != StorageNode.STATUS_REMOVED]
    # Never automated for these cluster types: event only.
    if cluster.sync_replication is True:
        return RESTART_MANUAL, "sync-replication clusters are never restarted automatically for this"
    if floors.is_two_node_cluster(cluster, len(nodes)):
        return RESTART_MANUAL, "two-node clusters are never restarted automatically for this"
    if node.auto_restart_disabled:
        return RESTART_MANUAL, "the node was shut down deliberately (auto-restart disabled)"
    last = float(getattr(node, "lvs_member_auto_restart_at", 0.0) or 0.0)
    if last and now - last < constants.LVS_MEMBER_AUTO_RESTART_INTERVAL_SEC:
        return RESTART_MANUAL, (
            f"it was restarted automatically for this {int(now - last)}s ago and the failures "
            f"persist (at most one automatic restart per "
            f"{constants.LVS_MEMBER_AUTO_RESTART_INTERVAL_SEC // 3600} h)")
    if node.status != StorageNode.STATUS_ONLINE:
        return RESTART_SKIPPED, f"the node is {node.status}"
    if cluster.status != Cluster.STATUS_ACTIVE:
        return RESTART_SKIPPED, f"the cluster is {cluster.status}, not active"
    blocks = activation_gate.site_activation_blocks(cluster)
    if blocks:
        return RESTART_SKIPPED, f"a site activation runs ({blocks})"
    not_online = [f"{n.get_id()[:8]} {n.status}" for n in nodes
                  if n.get_id() != node.get_id() and n.status != StorageNode.STATUS_ONLINE]
    if not_online:
        return RESTART_SKIPPED, f"other nodes are not online ({', '.join(not_online)})"
    if tasks_controller.get_active_node_restart_task(cluster.get_id(), node.get_id()):
        return RESTART_SKIPPED, "a restart task is already queued for the node"
    blocking = _blocking_task_names()
    for t in db.get_job_tasks(cluster.get_id()):
        if t.function_name in blocking and t.status != JobSchedule.STATUS_DONE and not t.canceled:
            return RESTART_SKIPPED, f"a {t.function_name} task is in progress"
    if node.lvstore:
        sec = None
        if node.secondary_node_id:
            try:
                sec = db.get_storage_node_by_id(node.secondary_node_id)
            except KeyError:
                sec = None
        if sec is None or sec.status != StorageNode.STATUS_ONLINE:
            return RESTART_SKIPPED, f"its lvstore {node.lvstore} has no online secondary to take over"
    for lvs, others in _member_lvstores(node, db):
        if not any(o.status == StorageNode.STATUS_ONLINE for o in others):
            return RESTART_SKIPPED, f"it is the only online member of {lvs}"
    return "", ""


def auto_restart_member(node, lvs_name, db=None, now=None, run=None):
    """Restart *node* because its copy of *lvs_name* keeps failing after a
    reload (see above), when every gate passes. ``(outcome, detail)``:
    RESTART_STARTED, RESTART_SKIPPED or RESTART_MANUAL. Every decision raises an
    LVS_MEMBER_AUTO_RESTART event; the shutdown and the queueing run in ``run``
    (a background thread by default) and report their result.

    The restart time is persisted on the node BEFORE anything is stopped, in a
    transaction that re-checks the limit: no restart of the monitor or the task
    runner, and no concurrent decision, can grant a second restart early."""
    import time as _time
    db = db or db_controller.DBController()
    now = _time.time() if now is None else now
    try:
        node = db.get_storage_node_by_id(node.get_id())
        cluster = db.get_cluster_by_id(node.cluster_id)
    except KeyError as e:
        return RESTART_SKIPPED, f"not found: {e}"
    stage, why = restart_gate(node, cluster, db, now)
    if stage:
        _restart_event(node, lvs_name, "manual" if stage == RESTART_MANUAL else "skipped", why)
        logger.warning("No automatic restart of %s for %s: %s", node.get_id(), lvs_name, why)
        return stage, why

    def _claim(fresh):
        prev = float(getattr(fresh, "lvs_member_auto_restart_at", 0.0) or 0.0)
        if prev and now - prev < constants.LVS_MEMBER_AUTO_RESTART_INTERVAL_SEC:
            return False
        fresh.lvs_member_auto_restart_at = now
        return None

    claimed = db.atomic_update(node, _claim)
    if claimed is None or float(getattr(claimed, "lvs_member_auto_restart_at", 0.0) or 0.0) != now:
        why = "another decision restarted it for this meanwhile"
        _restart_event(node, lvs_name, "manual", why)
        return RESTART_MANUAL, why

    _restart_event(node, lvs_name, "restarting", "registrations failed again after a reload")
    (run or _run_in_thread)(lambda: _shutdown_and_queue(node.get_id(), lvs_name, db))
    return RESTART_STARTED, "graceful shutdown, then the automatic restart"


def _shutdown_and_queue(node_id, lvs_name, db):
    """The graceful shutdown that keeps auto-restart enabled, then the queued
    restart task. Reports the result as an event; True once the node is down."""
    from simplyblock_core.controllers import tasks_controller
    node = db.get_storage_node_by_id(node_id)
    err = "shutdown_storage_node refused or failed (see its log)"
    try:
        ok = storage_node_ops.shutdown_storage_node(node_id, keep_auto_restart=True)
    except Exception as e:                                  # noqa: BLE001
        ok, err = False, str(e)
    if not ok:
        _restart_event(node, lvs_name, "failed", err)
        return False
    try:
        task = tasks_controller.add_node_to_auto_restart(db.get_storage_node_by_id(node_id))
    except Exception as e:                                  # noqa: BLE001
        logger.error("add_node_to_auto_restart for %s failed: %s", node_id, e)
        task = False
    if task:
        _restart_event(node, lvs_name, "queued", f"restart task {task}")
    else:
        # OFFLINE with auto-restart enabled: the monitor's re-queue scan
        # queues it on its next pass.
        _restart_event(node, lvs_name, "queued",
                       "the restart task could not be queued now; the monitor re-queues it")
    return True


def _run_in_thread(fn):
    import threading
    threading.Thread(target=fn, daemon=True, name="lvs-member-auto-restart").start()


def _restart_event(node, lvs_name, stage, detail):
    try:
        storage_events.snode_lvs_member_auto_restart(node, lvs_name, stage, detail)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("Could not log the member auto-restart event: %s", e)
