"""One migrating volume per source node at a time (docs/online-migration.md).

A cluster migration moves every volume of a node, but the node replicates
only ONE of them at a time: that volume's initial copy, catch-up and cutover
run to the end before the next volume of the node takes its first snapshot.
Distinct nodes progress in parallel.

The turn is derived, not stored: of the migration-mode volumes a node
replicates, the one queued first holds it (``LVol.migration_queued_at``, then
uuid). A cutover that completes stops the source's replication
(``do_replicate`` cleared), so the next volume holds the turn on the next
pass; nothing has to hand it over, and a crash cannot lose a hand-over. A
cutover that gave up or was canceled releases the turn too, or one bad volume
would park its node's whole queue behind it.

A consistency group moves as one unit: its members share their holder's turn,
because the group is snapshotted as one crash-consistent generation.

Volumes that wait take no cadence snapshots, so nothing of theirs is shipped
and their cutover task (queued by the plan with ``require_baseline``) defers
until their turn comes.
"""
from simplyblock_core.models.job_schedule import JobSchedule


def _unit(db, lvol):
    """A volume's consistency group (stamped, or owned by its policy), else
    the volume itself."""
    if lvol.group_id:
        return lvol.group_id
    policy_id = getattr(lvol, "replication_policy_id", "")
    if policy_id:
        try:
            group = db.get_consistency_group_for_policy(policy_id)
        except Exception:                                   # noqa: BLE001
            group = None
        if group is not None:
            return group.get_id()
    return lvol.get_id()


def _released_lvol_ids(db, cluster_id):
    """Volumes whose cutover ended without moving them and that have no
    cutover still running."""
    ended, active = set(), set()
    for task in db.get_job_tasks(cluster_id):
        if task.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        lvol_id = (task.function_params or {}).get("lvol_id")
        if not lvol_id:
            continue
        if task.canceled or (task.status == JobSchedule.STATUS_DONE
                             and task.function_result != "cutover done"):
            ended.add(lvol_id)
        elif task.status != JobSchedule.STATUS_DONE:
            active.add(lvol_id)
    return ended - active


def waiting_lvol_ids(db, cluster_id, lvols=None):
    """Ids of the migration-mode volumes that must wait for their node's turn."""
    if lvols is None:
        lvols = db.get_lvols(cluster_id)
    migrating = [lv for lv in lvols
                 if lv.do_replicate and lv.replication_mode == "migration"]
    if len(migrating) < 2:
        return set()
    released = _released_lvol_ids(db, cluster_id)
    contenders = [lv for lv in migrating if lv.get_id() not in released]

    unit = {lv.get_id(): _unit(db, lv) for lv in contenders}
    holder_unit = {}
    for lv in sorted(contenders,
                     key=lambda v: (getattr(v, "migration_queued_at", 0) or 0, v.get_id())):
        holder_unit.setdefault(lv.node_id, unit[lv.get_id()])
    return {lv.get_id() for lv in contenders if holder_unit[lv.node_id] != unit[lv.get_id()]}
