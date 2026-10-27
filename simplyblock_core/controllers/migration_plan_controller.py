"""Bulk online migration of volumes to another cluster (migration plans).

A plan selects a cluster's volumes -- all of them, one pool's, or an explicit
list --, starts migration-mode replication for those that do not replicate yet
(placing them with a shared load map so the batch spreads over the target's
nodes), and queues every volume's cutover at once. The cutover runner waits for
each volume to catch up and serialises the endgame per source lvstore, so
queueing them all up front costs nothing. Progress and a forecast are read
back from the volumes' tasks and snapshots. See docs/online-migration.md.
"""
import time
import uuid as uuid_lib

from simplyblock_core import constants, migration_forecast, migration_turn, utils
from simplyblock_core.controllers import lvol_controller, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.migration_plan import MigrationPlan

logger = utils.get_logger(__name__)


class MigrationPlanError(ValueError):
    """The request cannot be planned (bad cluster, nothing selected, ...)."""


def _target_cluster_of(db, lvol):
    if not lvol.replication_node_id:
        return ""
    try:
        return db.get_storage_node_by_id(lvol.replication_node_id).cluster_id
    except KeyError:
        return ""


def _used_bytes(db, lvol):
    """Data the initial copy has to move: the latest capacity record's used
    bytes, else the provisioned size (an upper bound)."""
    try:
        stats = db.get_lvol_stats(lvol, limit=1)
        if stats and stats[-1].size_used:
            return int(stats[-1].size_used)
    except Exception as e:                                # noqa: BLE001
        logger.debug("No capacity record for %s: %s", lvol.get_id(), e)
    return int(lvol.size)


def _skip_reason(db, lvol, target_cluster_id, cutover_done_ids, listed=False):
    """Why *lvol* cannot join a plan to *target_cluster_id*, or ''.

    A volume that already replicates to the target in failover (DR) mode is
    skipped by a broad selection -- a whole-cluster move must not rewire DR
    by accident --, but taken when it is named explicitly: that is how a
    policy-driven caller (the operator's ReplicationOps) asks for it.
    """
    if lvol.status in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED):
        return "volume is being deleted"
    if lvol.lvol_name.startswith("REP_"):
        return "replication landing volume"
    if lvol.get_id() in cutover_done_ids:
        return "already cut over"
    if lvol.do_replicate:
        current = _target_cluster_of(db, lvol)
        if current and current != target_cluster_id:
            return f"replicates to cluster {current}; stop that replication first"
        if lvol.replication_mode != "migration" and not listed:
            return ("replicates in failover (DR) mode; a migration plan does not "
                    "rewire a DR relationship")
    elif lvol.replication_policy_id:
        return (f"follows replication policy {lvol.replication_policy_id}; attach a "
                f"migration-mode policy to it instead")
    return ""


def select_volumes(db, cluster_id, target_cluster_id, pool_id=None, lvol_ids=None):
    """(selected, skipped): the volumes a plan moves, largest first, and the
    ones it leaves out as ``{"lvol_id", "name", "size", "error"}``."""
    lvols = db.get_lvols(cluster_id)
    if lvol_ids:
        wanted = set(lvol_ids)
        missing = wanted - {lv.get_id() for lv in lvols}
        if missing:
            raise MigrationPlanError(
                f"volumes not on cluster {cluster_id}: {', '.join(sorted(missing))}")
        lvols = [lv for lv in lvols if lv.get_id() in wanted]
    elif pool_id:
        lvols = [lv for lv in lvols if lv.pool_uuid == pool_id]

    cutover_done_ids = {
        rep.source_lvol.get_id() for rep in db.get_lvol_replication_objects()
        if rep.source_lvol and rep.state == LVolReplication.STATE_CUTOVER_DONE
        and rep.source_cluster_id == cluster_id}

    selected, skipped = [], []
    for lv in lvols:
        reason = _skip_reason(db, lv, target_cluster_id, cutover_done_ids,
                              listed=bool(lvol_ids))
        if reason:
            skipped.append({"lvol_id": lv.get_id(), "name": lv.lvol_name,
                            "size": lv.size, "error": reason})
        else:
            selected.append(lv)

    # A consistency group cuts over as one unit; moving part of it splits the
    # group across clusters.
    chosen = {lv.get_id() for lv in selected}
    all_live = db.get_lvols(cluster_id)
    partial_groups = set()
    for lv in selected:
        if lv.group_id and any(o.group_id == lv.group_id and o.get_id() not in chosen
                               and o.status not in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)
                               for o in all_live):
            partial_groups.add(lv.group_id)
    if partial_groups:
        kept = []
        for lv in selected:
            if lv.group_id in partial_groups:
                skipped.append({"lvol_id": lv.get_id(), "name": lv.lvol_name, "size": lv.size,
                                "error": f"consistency group {lv.group_id} is not "
                                         f"selected as a whole"})
            else:
                kept.append(lv)
        selected = kept

    selected.sort(key=lambda lv: (-lv.size, lv.get_id()))
    return selected, skipped


def _predict_destination(db, lvol, cluster, target_cluster_id, load, batch_nqn_node):
    """The node replication_start would pick for *lvol*, without writing it:
    the same pinning order (subsystem sibling, group, chain holder), then the
    least planned load. Siblings earlier in the same batch count."""
    if lvol.replication_node_id:
        return lvol.replication_node_id
    if lvol.nqn in batch_nqn_node:
        return batch_nqn_node[lvol.nqn]
    pinned = (lvol_controller._sibling_replication_node(lvol, cluster)
              or lvol_controller._group_replication_node_for(lvol, db)
              or lvol_controller._chain_counterpart_node(db, lvol, target_cluster_id))
    if pinned:
        return pinned
    node = lvol_controller._pick_replication_node(db, target_cluster_id, lvol.size, load=load)
    return node.get_id() if node else ""


def _source_capacity(db, cluster_id):
    """(utilization %, warning threshold %, total bytes, used bytes) of
    *cluster_id* from its latest capacity record, or None without one."""
    try:
        cluster = db.get_cluster_by_id(cluster_id)
        records = db.get_cluster_capacity(cluster, 1)
    except Exception:                                     # noqa: BLE001
        return None
    if not records:
        return None
    rec = records[0]
    return (getattr(rec, "size_util", 0), getattr(cluster, "cap_warn", 0) or 0,
            getattr(rec, "size_total", 0), getattr(rec, "size_used", 0))


def _write_rate(db, lvol_id, samples=6):
    """Bytes/sec the clients write to *lvol_id*: the mean of its latest IO
    stats records, or 0 without any. This is what re-dirties a volume while it
    is being copied, and it is readable from the start -- unlike a rate derived
    from cadence snapshots, of which a volume holds only one while its initial
    copy is outstanding."""
    try:
        lvol = db.get_lvol_by_id(lvol_id)
        stats = db.get_lvol_stats(lvol, limit=samples)
    except Exception:                                     # noqa: BLE001
        return 0
    rates = [getattr(s, "write_bytes_ps", 0) or 0 for s in stats or []]
    return sum(rates) / len(rates) if rates else 0


def _validate_clusters(db, cluster_id, target_cluster_id):
    if not target_cluster_id:
        raise MigrationPlanError("a target cluster is required")
    if target_cluster_id == cluster_id:
        raise MigrationPlanError("a volume cannot migrate to its own cluster")
    for cid in (cluster_id, target_cluster_id):
        try:
            db.get_cluster_by_id(cid)
        except KeyError:
            raise MigrationPlanError(f"cluster not found: {cid}")


def _infer_target_cluster(db, cluster_id, lvol_ids):
    """The one cluster the listed volumes already replicate to, for a caller
    that knows the volumes but not the destination's id."""
    if not lvol_ids:
        raise MigrationPlanError("a target cluster is required unless volumes are listed")
    targets = set()
    for lv in db.get_lvols(cluster_id):
        if lv.get_id() in lvol_ids:
            targets.add(_target_cluster_of(db, lv) if lv.do_replicate else "")
    targets.discard("")
    if len(targets) != 1:
        raise MigrationPlanError(
            "cannot infer the target cluster: the listed volumes replicate to "
            + (", ".join(sorted(targets)) if targets else "no cluster"))
    return targets.pop()


def build_plan(cluster_id, target_cluster_id, pool_id=None, lvol_ids=None,
               delete_source=False, interval_min=None, db=None, name=""):
    """The plan a start would execute, with placement and forecast; nothing is
    written (a dry run returns exactly this). Without *target_cluster_id* the
    destination is the one the listed volumes already replicate to."""
    db = db or DBController()
    if not target_cluster_id:
        target_cluster_id = _infer_target_cluster(db, cluster_id, lvol_ids)
    _validate_clusters(db, cluster_id, target_cluster_id)
    selected, skipped = select_volumes(db, cluster_id, target_cluster_id, pool_id, lvol_ids)

    plan = MigrationPlan()
    plan.uuid = str(uuid_lib.uuid4())
    plan.plan_name = name or ""
    plan.cluster_id = cluster_id
    plan.target_cluster_id = target_cluster_id
    plan.pool_id = pool_id or ""
    plan.delete_source = bool(delete_source)
    plan.interval_min = int(interval_min if interval_min is not None
                            else constants.REPL_MIGRATION_DEFAULT_INTERVAL_MIN)
    plan.created_at = int(time.time())

    cluster = db.get_cluster_by_id(cluster_id)
    # Every volume's initial-copy snapshot pins the blocks overwritten while it
    # is outstanding -- up to the volume's whole used size. A source already
    # past its warning threshold has no room for that (the EKS bed of
    # 2026-10-22 filled, went read_only, and its clients got EIO).
    capacity = _source_capacity(db, cluster_id)
    if capacity:
        util, warn, total, used = capacity
        if warn and util >= warn:
            raise MigrationPlanError(
                f"source cluster {cluster_id} is at {util}% capacity, at or above "
                f"its warning threshold of {warn}%: snapshots taken for the "
                f"migration would fill it")
        pin = sum(_used_bytes(db, lv) for lv in selected)
        if total and total - used < pin:
            plan.warnings.append(
                f"source cluster has {utils.humanbytes(total - used)} free, less than "
                f"the {utils.humanbytes(pin)} the volumes' initial-copy snapshots can "
                f"pin under sustained overwrites; keep client writes well below the "
                f"replication throughput")
    load = lvol_controller._replication_node_load(db, target_cluster_id)
    batch_nqn_node: dict = {}
    for lv in selected:
        node_id = _predict_destination(db, lv, cluster, target_cluster_id, load, batch_nqn_node)
        if node_id and lv.nqn:
            batch_nqn_node.setdefault(lv.nqn, node_id)
        plan.volumes.append({
            "lvol_id": lv.get_id(), "name": lv.lvol_name, "size": lv.size,
            "used_bytes": _used_bytes(db, lv), "lvs_name": lv.lvs_name,
            "target_node_id": node_id, "task_id": "",
            "state": MigrationPlan.VOL_REPLICATING if node_id else MigrationPlan.VOL_FAILED,
            "error": "" if node_id else "no destination node available",
        })
    for sk in skipped:
        plan.volumes.append({**sk, "used_bytes": 0, "lvs_name": "", "target_node_id": "",
                             "task_id": "", "state": MigrationPlan.VOL_SKIPPED})
    if not selected:
        raise MigrationPlanError("no volume can be migrated: " + "; ".join(
            f"{s['name'] or s['lvol_id']}: {s['error']}" for s in skipped) if skipped
            else "no volume selected")
    return plan


def start_plan(cluster_id, target_cluster_id, pool_id=None, lvol_ids=None,
               delete_source=False, interval_min=None, name=""):
    """Build, persist and start a plan. Returns the plan.

    *name* makes the create idempotent: an unfinished plan of the same name
    on this cluster is returned instead of starting a second one -- a caller
    that died after its request reached us (the operator) finds its plan.
    """
    db = DBController()
    if name:
        for existing in MigrationPlan().read_from_db(db.kv_store, id=cluster_id):
            if existing.plan_name == name and not existing.is_finished():
                return existing
    plan = build_plan(cluster_id, target_cluster_id, pool_id, lvol_ids,
                      delete_source, interval_min, db=db, name=name)
    plan.write_to_db(db.kv_store)

    load = lvol_controller._replication_node_load(db, target_cluster_id)
    for vol in plan.volumes:
        if vol["state"] != MigrationPlan.VOL_REPLICATING:
            continue
        _start_volume(db, plan, vol, load)
        # Written per volume: a crash half-way leaves a plan that names the
        # tasks it did queue, and a status read picks them up.
        plan.write_to_db(db.kv_store)
    _refresh(db, plan)
    return plan


def _start_volume(db, plan, vol, load):
    lvol_id = vol["lvol_id"]
    lvol = db.get_lvol_by_id(lvol_id)
    if not lvol.do_replicate:
        interval = plan.interval_min or lvol.replication_interval_min
        ok = lvol_controller.replication_start(
            lvol_id, replication_cluster_id=plan.target_cluster_id, mode="migration",
            interval_min=interval, placement_load=load)
        if not ok:
            vol.update(state=MigrationPlan.VOL_FAILED, error="replication could not be started")
            return
        lvol = db.get_lvol_by_id(lvol_id)
    elif not lvol.replication_interval_min:
        # Replicating without a cadence ships user snapshots only; the
        # catch-up the cutover waits for would never come.
        lvol.replication_interval_min = plan.interval_min
        lvol.write_to_db(db.kv_store)
    vol["target_node_id"] = lvol.replication_node_id

    result = lvol_controller.replication_commit(
        lvol_id, delete_source=plan.delete_source, plan_id=plan.uuid, require_baseline=True)
    if isinstance(result, dict):
        vol["task_id"] = result["task_id"]
        return
    # A cutover already queued for this volume is adopted rather than doubled.
    existing = tasks_controller.get_active_replication_final_task(plan.cluster_id, lvol_id)
    if existing:
        vol["task_id"] = existing
        return
    error = result[1] if isinstance(result, tuple) else "the cutover could not be queued"
    vol.update(state=MigrationPlan.VOL_FAILED, error=str(error))


def _volume_state(task):
    """(state, error) of a plan volume from its cutover task."""
    params = task.function_params or {}
    if task.canceled:
        return MigrationPlan.VOL_CANCELED, ""
    if task.status == JobSchedule.STATUS_DONE:
        if task.function_result == "cutover done":
            return MigrationPlan.VOL_DONE, ""
        return MigrationPlan.VOL_FAILED, (params.get("last_error") or task.function_result
                                          or "cutover gave up")
    if "tgt_lvol_composite" in params:
        return MigrationPlan.VOL_CUTTING_OVER, ""
    if params.get("cutover_lvs") or params.get("shrink_snap_id"):
        return MigrationPlan.VOL_CONVERGING, ""
    if str(task.function_result).startswith("queued for lvstore"):
        return MigrationPlan.VOL_QUEUED, ""
    return MigrationPlan.VOL_REPLICATING, ""


def _transfer_progress(db, lvol_id, used_bytes):
    """(bytes_remaining, samples): what replication still has to ship, and
    ``(bytes, seconds)`` of the transfers it completed."""
    try:
        snaps = db.get_snapshots_by_lvol_id(lvol_id)
    except Exception as e:                                # noqa: BLE001
        logger.debug("Cannot read snapshots of %s: %s", lvol_id, e)
        return used_bytes, []
    snaps = [s for s in snaps if not getattr(s, "deleted", False)]
    replicated = [s for s in snaps if s.target_replicated_snap_uuid]
    outstanding = [s for s in snaps if not s.target_replicated_snap_uuid]
    samples = []
    for snap in replicated:
        for task in db.get_replication_tasks_for_snapshot(snap.get_id()):
            p = task.function_params or {}
            if "start_time" in p and "end_time" in p:
                samples.append((snap.used_size, float(p["end_time"]) - float(p["start_time"])))
    if not replicated:
        # Nothing has reached the target: the whole initial copy is ahead,
        # whether or not its snapshot exists yet.
        return max(used_bytes, sum(s.used_size for s in outstanding)), samples
    return sum(s.used_size for s in outstanding), samples


def _refresh(db, plan, persist=True):
    """Re-derive volume states and the forecast; persist a status change
    (never for a dry run, whose plan does not exist)."""
    samples_by_lvs: dict = {}
    dirty_by_lvs: dict = {}
    cutover_durations = []
    waiting = migration_turn.waiting_lvol_ids(db, plan.cluster_id)
    for vol in plan.volumes:
        if vol["state"] in MigrationPlan.VOL_TERMINAL:
            vol["bytes_remaining"] = 0
            continue
        if vol.get("task_id"):
            try:
                task = db.get_task_by_id(vol["task_id"])
                vol["state"], vol["error"] = _volume_state(task)
                p = task.function_params or {}
                if vol["state"] == MigrationPlan.VOL_DONE and p.get("endgame_started_at"):
                    cutover_durations.append(float(p["end_time"]) - float(p["endgame_started_at"]))
            except KeyError:
                vol.update(state=MigrationPlan.VOL_FAILED, error="cutover task not found")
        if vol["state"] in (MigrationPlan.VOL_REPLICATING, MigrationPlan.VOL_WAITING):
            vol["state"] = (MigrationPlan.VOL_WAITING if vol["lvol_id"] in waiting
                            else MigrationPlan.VOL_REPLICATING)
        if vol["state"] in (MigrationPlan.VOL_WAITING, MigrationPlan.VOL_REPLICATING,
                            MigrationPlan.VOL_QUEUED):
            remaining, samples = _transfer_progress(db, vol["lvol_id"], vol.get("used_bytes", 0))
            vol["bytes_remaining"] = remaining
            samples_by_lvs.setdefault(vol.get("lvs_name", ""), []).extend(samples)
        if vol["state"] not in MigrationPlan.VOL_TERMINAL:
            lvs = vol.get("lvs_name", "")
            dirty_by_lvs[lvs] = dirty_by_lvs.get(lvs, 0) + _write_rate(db, vol["lvol_id"])
        else:
            vol["bytes_remaining"] = 0

    throughput = {lvs: migration_forecast.measured_throughput(s)
                  for lvs, s in samples_by_lvs.items()}
    per_volume, overall = migration_forecast.forecast(
        plan.volumes, throughput,
        migration_forecast.typical_cutover_seconds(cutover_durations), time.time(),
        dirty_by_lvs=dirty_by_lvs)
    for vol in plan.volumes:
        vol["eta_seconds"] = per_volume.get(vol["lvol_id"], 0)

    if persist and plan.status not in MigrationPlan.TERMINAL_STATUSES and all(
            v["state"] in MigrationPlan.VOL_TERMINAL for v in plan.volumes):
        done = [v for v in plan.volumes if v["state"] == MigrationPlan.VOL_DONE]
        plan.status = (MigrationPlan.STATUS_DONE if len(done) == len(plan.volumes)
                       else MigrationPlan.STATUS_PARTIAL if done
                       else MigrationPlan.STATUS_FAILED)
        plan.finished_at = int(time.time())
        plan.write_to_db(db.kv_store)
    return overall


def forecast_only(plan, db=None):
    """The forecast of a plan that build_plan returned (a dry run)."""
    return _refresh(db or DBController(), plan, persist=False)


def get_plan(plan_id, db=None):
    db = db or DBController()
    # " ", not "": an empty id makes read_from_db fall back to the blank
    # object's own id ("/"), a prefix no plan is stored under. A blank id
    # strips to the class prefix, which is every cluster's plans.
    for plan in MigrationPlan().read_from_db(db.kv_store, id=" "):
        if plan.uuid == plan_id.split("/")[-1]:
            return plan
    raise KeyError(f"migration plan {plan_id} not found")


def plan_status(plan_id):
    """(plan, forecast) with live per-volume state."""
    db = DBController()
    plan = get_plan(plan_id, db)
    forecast = _refresh(db, plan)
    return plan, forecast


def list_plans(cluster_id):
    db = DBController()
    return sorted(MigrationPlan().read_from_db(db.kv_store, id=cluster_id),
                  key=lambda p: p.created_at)


def cancel_plan(plan_id):
    """Cancel every volume whose cutover has not prepared its target clone.

    A cutover past that point is already freezing or about to; stopping it
    there leaves a half-built target, so it is left to finish. Volumes the plan
    started keep replicating; stop them or commit them again separately.
    """
    db = DBController()
    plan = get_plan(plan_id, db)
    for vol in plan.volumes:
        if vol["state"] in MigrationPlan.VOL_TERMINAL or not vol.get("task_id"):
            continue
        try:
            task = db.get_task_by_id(vol["task_id"])
        except KeyError:
            continue
        if "tgt_lvol_composite" in (task.function_params or {}):
            continue
        task.canceled = True
        task.write_to_db(db.kv_store)
        vol["state"] = MigrationPlan.VOL_CANCELED
    if all(v["state"] in MigrationPlan.VOL_TERMINAL for v in plan.volumes):
        plan.status = MigrationPlan.STATUS_CANCELED
        plan.finished_at = int(time.time())
    plan.write_to_db(db.kv_store)
    return plan
