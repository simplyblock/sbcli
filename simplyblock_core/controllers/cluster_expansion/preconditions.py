# coding=utf-8
"""Pre-flight checks for single-node cluster expansion.

The role rebalance re-wires the secondary LVS at the newcomer's direct
successor and the tertiary LVS at the second-in-line successor — which
requires tearing down the existing sec/tert stacks on those two donor
nodes. That teardown is only safe on a fully quiescent, fully redundant
cluster, so an expansion may not start unless:

* the cluster is ACTIVE and every storage node is ONLINE;
* no data migration (device / new-device / failed-device / balancing),
  lvol migration, node restart, or backup/restore task is open anywhere
  in the cluster (open = NEW, RUNNING or SUSPENDED — anything not DONE);
* no delete is in flight on the impacted donor nodes (lvol sync-delete
  tasks targeting them, or lvols in deletion on the LVS whose sec/tert
  they hold).

The checks run twice: cluster-wide at ``add_node --expansion`` time
(before the new node is even added), and in full — with the impacted
donors derived from the planned moves — right before the rebalance
executes (``integrate_new_node_into_cluster``), since tasks may have
been queued between the add and the rebalance task pickup.

While the expansion runs, the locks live elsewhere: every migration
runner defers its tasks while a cluster-expand task is open (they may be
QUEUED — e.g. by an unexpected node outage mid-expansion — but never run
before the expansion completes; see
``tasks_controller.defer_task_for_expansion``), ``shutdown_storage_node``
refuses shutdowns during IN_EXPANSION, the executor holds a restart-phase
gate on each donor (queueing create/delete/resize for the affected LVS),
and the donors' outbound hublvol connections are dropped up-front (see
``executor``). After completion the order is: outage device migration
drains first, then the expansion (new-device) migration runs
(tasks_runner_new_dev_migration's recovery-before-expansion gate).
"""

from simplyblock_core import utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


#: Cluster-wide task families that must be fully drained before an
#: expansion may start. An open instance of any of these means data or
#: roles are moving somewhere in the cluster — re-wiring sec/tert on top
#: of that races the mover.
EXPANSION_BLOCKING_TASK_FNS = frozenset({
    JobSchedule.FN_DEV_MIG,
    JobSchedule.FN_NEW_DEV_MIG,
    JobSchedule.FN_FAILED_DEV_MIG,
    JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
    JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
    JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION,
    JobSchedule.FN_LVOL_MIG,
    JobSchedule.FN_NODE_RESTART,
    JobSchedule.FN_BACKUP,
    JobSchedule.FN_BACKUP_RESTORE,
    JobSchedule.FN_BACKUP_MERGE,
})

#: Task families that block only when they target an impacted donor node.
EXPANSION_IMPACTED_NODE_TASK_FNS = frozenset({
    JobSchedule.FN_LVOL_SYNC_DEL,
})

#: Subset of the blocking families that DEFER on an open cluster-expand task
#: (their runners suspend while the expansion is in progress — see
#: ``tasks_controller.defer_task_for_expansion``). A RESUME of an in-progress
#: plan tolerates open tasks from these families: they are typically the
#: recovery migrations queued by an unexpected node outage mid-expansion,
#: and they wait for us, not the other way around (required order: expansion
#: completes -> outage device migration -> expansion migration). A FRESH
#: expansion still refuses to start over them.
EXPANSION_DEFERRING_TASK_FNS = frozenset({
    JobSchedule.FN_DEV_MIG,
    JobSchedule.FN_NEW_DEV_MIG,
    JobSchedule.FN_FAILED_DEV_MIG,
    JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
    JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
    JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION,
    JobSchedule.FN_LVOL_MIG,
})


def _task_is_open(task) -> bool:
    return task.status != JobSchedule.STATUS_DONE and not task.canceled


def impacted_donor_node_ids(moves) -> set:
    """Donor node ids of the re-home moves — the nodes whose secondary /
    tertiary role is being exchanged (the newcomer's direct and
    second-in-line successors)."""
    return {m.from_node_id for m in moves
            if not m.is_create and m.from_node_id}


def affected_primary_node_ids(moves) -> set:
    """Primaries of the LVS whose sec/tert roles are re-homed."""
    return {m.lvs_primary_node_id for m in moves
            if not m.is_create and m.lvs_primary_node_id}


def check_expansion_preconditions(cluster, db_controller,
                                  impacted_node_ids=None,
                                  affected_primary_ids=None,
                                  resume=False):
    """Verify the cluster is quiescent enough to start an expansion.

    ``impacted_node_ids`` / ``affected_primary_ids`` are None for the
    early (pre-add) cluster-wide check and populated from the planned
    moves for the full pre-rebalance check.

    ``resume=True`` (retrying an in-progress plan, e.g. after a node
    outage interrupted it) tolerates open tasks from the families that
    defer on the expansion (``EXPANSION_DEFERRING_TASK_FNS``) — refusing
    on those would deadlock: they suspend until the expansion completes.
    Node-restart and backup/restore families stay blocking even on
    resume.

    Returns ``(ok: bool, reason: str)`` — ``reason`` is empty on success.
    """
    # Cluster status must be a steady ACTIVE. DEGRADED/SUSPENDED mean an
    # outage is in progress; IN_EXPANSION means another expansion runs.
    if cluster.status != Cluster.STATUS_ACTIVE:
        return False, f"cluster status is {cluster.status}, expansion requires active"

    # Every node must be ONLINE — a sec/tert teardown while any peer is
    # out reduces redundancy below the FTT contract.
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster.get_id()):
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            return False, (f"node {node.get_id()} is {node.status}; "
                           f"all nodes must be online")

    for task in db_controller.get_job_tasks(cluster.get_id()):
        if not _task_is_open(task):
            continue
        if task.function_name in EXPANSION_BLOCKING_TASK_FNS:
            if resume and task.function_name in EXPANSION_DEFERRING_TASK_FNS:
                continue  # waits for the expansion, not the other way around
            return False, (f"open {task.function_name} task {task.uuid} "
                           f"(status: {task.status}); wait for it to finish "
                           f"or cancel it before expanding")
        if (impacted_node_ids
                and task.function_name in EXPANSION_IMPACTED_NODE_TASK_FNS
                and task.node_id in impacted_node_ids):
            return False, (f"open {task.function_name} task {task.uuid} on "
                           f"impacted node {task.node_id}; deletes must "
                           f"finish before its sec/tert role can move")

    # In-flight lvol deletions on the LVS whose sec/tert roles move: the
    # delete fan-out targets the donor's per-lvol subsystems, which the
    # rebalance is about to tear down.
    if affected_primary_ids:
        for lvol in db_controller.get_lvols(cluster.get_id()):
            if lvol.status != LVol.STATUS_IN_DELETION:
                continue
            if lvol.node_id in affected_primary_ids:
                return False, (f"lvol {lvol.get_id()} on affected primary "
                               f"{lvol.node_id} is in deletion; wait for it "
                               f"to finish before expanding")

    return True, ""
