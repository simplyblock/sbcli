# coding=utf-8
"""
tasks_runner_batch_migration.py – main orchestrator for batch (shared-namespace)
lvol migration.

Drives FN_LVOL_BATCH_MIG tasks, which coordinate N worker FN_LVOL_MIG tasks
(one per member of a shared NVMe-oF subsystem) through the following phases:

Phase state-machine
-------------------
PHASE_SNAP_COPY (orchestrator: wait)
    Wait for all workers to signal snap_copy_done.
    Then reconstruct the full ancestry tree on the target: for each worker's
    transferred snaps (in ns_id order, oldest→newest within each member), call
    bdev_lvol_add_clone and bdev_lvol_convert to freeze them as immutable
    snapshots in the correct parent–child order.
    Advance group to PHASE_INTERMEDIATE.

PHASE_INTERMEDIATE (orchestrator: wait + batch-final)
    Wait for all workers to signal intermediates_done.
    Build the batch-final-step argument lists (one entry per member, ordered
    by ns_id), acquire a shared hub connection via hub_manager, and call
    bdev_lvol_batch_final_step on the source node.
    Set group.batch_result = True/False.
    If True: flip ANA to optimized on target, advance to PHASE_CLEANUP_SOURCE.
    If False: advance to PHASE_CLEANUP_TARGET.

PHASE_CLEANUP_SOURCE (orchestrator: wait + source teardown)
    Wait for all workers to signal cleanup_source_done.
    Delete the source NVMe-oF subsystem (workers delete individual source bdevs).
    Advance to PHASE_COMPLETED → mark task DONE.

PHASE_CLEANUP_TARGET (orchestrator: wait + target teardown)
    Workers handle their own target snap cleanup.
    After all workers are DONE/FAILED/CANCELLED, orchestrator deletes the target
    NVMe-oF subsystem and marks group FAILED/CANCELLED.
"""

import time
from typing import List, Optional, Tuple

from simplyblock_core import db_controller as db_mod, utils, constants
from simplyblock_core.utils import convert_size
from simplyblock_core.controllers import migration_controller, tasks_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient, RPCException
from simplyblock_core.services.hub_controller_manager import hub_manager
from simplyblock_core.services.tasks_runner_lvol_migration import (
    _make_rpc,
    _snap_tgt_short_name,
    _snap_short_name,
    _get_target_secondary_node,
    _get_target_tertiary_node,
    _lvol_tgt_bdev_name,
    _build_paths,
    _swap_namespace,
    _cleanup_subsystem_or_ns,
)

logger = utils.get_logger(__name__)
db = db_mod.DBController()


# ---------------------------------------------------------------------------
# Tree reconstruction helpers
# ---------------------------------------------------------------------------

def _get_migration_nic(node):
    trtype = "RDMA" if node.active_rdma else "TCP"
    for nic in node.data_nics:
        if nic.ip4_address:
            return trtype, nic.ip4_address
    return trtype, node.mgmt_ip


def _reconstruct_snap_tree(group, member_migrations, tgt_node, tgt_rpc) -> Optional[str]:
    """
    After all workers have transferred their owned snaps (without add_clone/convert),
    reconstruct the full ancestry tree on the target in correct order.

    For each member (sorted by ns_id), iterate their snaps_transferred_group in
    plan order (oldest→newest) and call:
      1. bdev_lvol_add_clone — link to predecessor (if any predecessor exists in
         the migration chain: snaps_migrated + snaps_preexisting_on_target)
      2. bdev_lvol_convert  — freeze as immutable snapshot

    Returns None on success, or an error string on failure.
    """
    tgt_sec, _ = _get_target_secondary_node(tgt_node, "")
    sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, "")
    ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None

    # Global set of snaps that have been committed as immutable on the target,
    # either pre-existing or reconstructed in this call.
    committed: set = set()
    for m in member_migrations:
        committed.update(m.snaps_preexisting_on_target)

    # Process members in ns_id order so shared ancestor snaps are committed
    # before any later member references them.
    for m in sorted(member_migrations, key=lambda x: x.migration_group_id or ""):
        # Determine ns_id for sorting: look up in group.members
        ns_id = next(
            (rec['ns_id'] for rec in group.members if rec['migration_id'] == m.uuid),
            999,
        )
        m._sort_ns_id = ns_id  # type: ignore[attr-defined]

    for m in sorted(member_migrations, key=lambda x: getattr(x, '_sort_ns_id', 999)):
        chain = migration_controller.get_snapshot_chain(m.lvol_id, m.source_node_id)

        for snap_uuid in m.snaps_transferred_group:
            if snap_uuid in committed:
                continue

            try:
                snap = db.get_snapshot_by_id(snap_uuid)
            except KeyError:
                return f"Snap {snap_uuid} not found during tree reconstruction"

            snap_short = _snap_tgt_short_name(snap)
            tgt_composite = f"{tgt_node.lvstore}/{snap_short}"

            # Find predecessor: the snap immediately before snap_uuid in this
            # lvol's chain that is already committed.
            pred_uuid = None
            for sid in chain:
                if sid == snap_uuid:
                    break
                if sid in committed:
                    pred_uuid = sid

            if pred_uuid:
                try:
                    pred_snap = db.get_snapshot_by_id(pred_uuid)
                    pred_short = _snap_tgt_short_name(pred_snap)
                    pred_composite = f"{tgt_node.lvstore}/{pred_short}"
                    if not tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                        return f"bdev_lvol_add_clone failed: {snap_uuid} → {pred_uuid}"
                    if sec_rpc:
                        sec_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
                    if ter_rpc:
                        ter_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
                except KeyError:
                    logger.warning(f"Predecessor {pred_uuid} not found; skipping add_clone")

            if not tgt_rpc.bdev_lvol_convert(tgt_composite):
                return f"bdev_lvol_convert failed for {snap_uuid}"
            if sec_rpc:
                sec_rpc.bdev_lvol_convert(tgt_composite)
            if ter_rpc:
                ter_rpc.bdev_lvol_convert(tgt_composite)

            committed.add(snap_uuid)
            # Update migration record so snaps_migrated reflects committed state.
            if snap_uuid not in m.snaps_migrated:
                m.snaps_migrated.append(snap_uuid)

        m.write_to_db(db.kv_store)

    return None  # success


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

def _handle_snap_copy_barrier(group, member_migrations, tgt_node, tgt_rpc):
    """
    Wait for all workers to reach snap_copy_done, then reconstruct the
    ancestry tree.  Returns (done, error) where error is None on success.
    """
    expected = {rec['migration_id'] for rec in group.members}
    done_set = set(group.snap_copy_done)
    if not expected.issubset(done_set):
        waiting = expected - done_set
        logger.debug(f"snap_copy barrier: waiting for {len(waiting)} workers")
        return False, None

    logger.info(
        f"Group {group.uuid[:8]}: all {len(expected)} workers reached snap_copy_done; "
        f"reconstructing ancestry tree")
    err = _reconstruct_snap_tree(group, member_migrations, tgt_node, tgt_rpc)
    if err:
        return False, err
    return True, None


def _build_batch_final_args(group, member_migrations, src_node, tgt_node, tgt_rpc):
    """
    Build the argument lists for bdev_lvol_batch_final_step, ordered by ns_id.

    Returns (lvol_names, lvol_ids, snapshot_names) or raises ValueError.
    """
    mid_to_migration = {m.uuid: m for m in member_migrations}
    ordered_ids = group.ordered_migration_ids()

    lvol_names = []
    lvol_ids = []
    snapshot_names = []

    lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
    name_to_entry = {}
    for entry in lvols_list:
        short = (entry.get('name', '') or entry.get('lvol_name', '')).split('/')[-1]
        if short:
            name_to_entry[short] = entry

    for migration_id in ordered_ids:
        m = mid_to_migration.get(migration_id)
        if m is None:
            raise ValueError(f"migration {migration_id} not found in member_migrations")

        lvol = db.get_lvol_by_id(m.lvol_id)
        src_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"
        lvol_names.append(src_composite)

        tgt_bdev_short = _lvol_tgt_bdev_name(lvol.lvol_bdev)
        entry = name_to_entry.get(tgt_bdev_short) or name_to_entry.get(
            tgt_bdev_short.split('/')[-1])
        if entry is None:
            raise ValueError(f"target bdev {tgt_bdev_short} not found for migration {migration_id}")
        map_id = entry.get('map_id')
        if map_id is None:
            raise ValueError(f"map_id missing for {tgt_bdev_short}")
        lvol_ids.append(map_id)

        # Last transferred snap = last entry in snaps_migrated (the intermediate).
        tgt_snap_composite = ""
        if m.snaps_migrated:
            last_uuid = m.snaps_migrated[-1]
            try:
                last_snap = db.get_snapshot_by_id(last_uuid)
                tgt_snap_composite = f"{tgt_node.lvstore}/{_snap_tgt_short_name(last_snap)}"
            except KeyError:
                pass
        elif m.snaps_preexisting_on_target:
            last_uuid = m.snaps_preexisting_on_target[-1]
            try:
                last_snap = db.get_snapshot_by_id(last_uuid)
                if last_snap.lvol and last_snap.lvol.node_id == tgt_node.get_id():
                    tgt_snap_composite = last_snap.snap_bdev
            except KeyError:
                pass
        snapshot_names.append(tgt_snap_composite)

    return lvol_names, lvol_ids, snapshot_names


def _flip_ana_to_optimized(group, member_migrations, src_node, src_rpc, tgt_node, tgt_rpc):
    """
    After a successful bdev_lvol_batch_final_step, drive clients to the new target:

      1. TGT primary listeners  → optimized   (with retry, same as single-lvol)
      2. TGT secondary/tertiary → non_optimized
      3. Overlap nodes: swap each member's SRC namespace to the migrated TGT bdev
      4. SRC listeners (non-overlap) → inaccessible

    Best-effort: logs warnings but does not raise.
    """
    nqn = group.target_nqn
    src_paths, tgt_paths, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)

    def _flip(rpc, ip, port, trtype, state, label):
        try:
            rpc.nvmf_subsystem_listener_set_ana_state(nqn, ip, port, trtype=trtype, ana=state)
            logger.info(f"Group {group.uuid[:8]}: ANA {label} {ip}:{port} → {state}")
            return True
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: ANA {label} {ip}:{port} (non-fatal): {e}")
            return False

    def _flip_required(rpc, ip, port, trtype, state, label, attempts=3):
        for i in range(attempts):
            if _flip(rpc, ip, port, trtype, state, label):
                return True
            if i < attempts - 1:
                time.sleep(1.0)
        return False

    # Step 1: TGT primary → optimized (required; abort flip sequence if it fails)
    tp = tgt_paths[0]
    if not _flip_required(tp['rpc'], tp['ip'], tp['port'], tp['trtype'], "optimized", "TGT-prim"):
        logger.error(f"Group {group.uuid[:8]}: failed to flip TGT primary to optimized after retries")

    # Step 2: TGT secondary / tertiary → non_optimized
    for i, tp in enumerate(tgt_paths[1:], 1):
        _flip(tp['rpc'], tp['ip'], tp['port'], tp['trtype'], "non_optimized", f"TGT-rep{i}")

    # Step 3: overlap nodes — swap each member's SRC namespace to the migrated TGT bdev
    if overlap_ids:
        for sp in src_paths:
            if sp['node_id'] not in overlap_ids:
                continue
            for m in member_migrations:
                try:
                    lvol = db.get_lvol_by_id(m.lvol_id)
                    ns_id = next(
                        (rec['ns_id'] for rec in group.members if rec['migration_id'] == m.uuid),
                        None)
                    tgt_bdev_composite = f"{tgt_node.lvstore}/{_lvol_tgt_bdev_name(lvol.lvol_bdev)}"
                    if ns_id:
                        try:
                            sp['rpc'].nvmf_subsystem_remove_ns(nqn, ns_id)
                            logger.info(
                                f"Group {group.uuid[:8]}: removed ns_id={ns_id} "
                                f"from overlap {sp['node_id'][:8]}")
                        except Exception as e:
                            logger.warning(
                                f"Group {group.uuid[:8]}: remove ns overlap "
                                f"{sp['node_id'][:8]} (non-fatal): {e}")
                    try:
                        sp['rpc'].nvmf_subsystem_add_ns(nqn, tgt_bdev_composite, lvol.uuid, lvol.guid)
                        logger.info(
                            f"Group {group.uuid[:8]}: added TGT ns {tgt_bdev_composite} "
                            f"on overlap {sp['node_id'][:8]}")
                    except Exception as e:
                        logger.warning(
                            f"Group {group.uuid[:8]}: add ns overlap "
                            f"{sp['node_id'][:8]} (non-fatal): {e}")
                except Exception as e:
                    logger.warning(
                        f"Group {group.uuid[:8]}: overlap member {m.uuid[:8]}: {e}")

    # Step 4: SRC listeners (non-overlap) → inaccessible
    for sp in src_paths:
        if sp['node_id'] in overlap_ids:
            continue
        _flip(sp['rpc'], sp['ip'], sp['port'], sp['trtype'], "inaccessible", f"SRC-{sp['node_id'][:8]}")


def _handle_intermediate_barrier(group, member_migrations, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Wait for all workers to reach intermediates_done, then call
    bdev_lvol_batch_final_step.  Returns (batch_ok, error).
    """
    expected = {rec['migration_id'] for rec in group.members}
    done_set = set(group.intermediates_done)
    if not expected.issubset(done_set):
        waiting = expected - done_set
        logger.debug(f"intermediates barrier: waiting for {len(waiting)} workers")
        return None, None  # None = still waiting

    logger.info(
        f"Group {group.uuid[:8]}: all workers reached intermediates_done; "
        f"calling bdev_lvol_batch_final_step")

    trtype, _ = _get_migration_nic(tgt_node)
    ctrl_name, hub_bdev, hub_err = hub_manager.acquire(
        src_node.get_id(), src_rpc, tgt_node, trtype)
    if hub_err:
        return None, hub_err

    try:
        lvol_names, lvol_ids, snapshot_names = _build_batch_final_args(
            group, member_migrations, src_node, tgt_node, tgt_rpc)
    except (ValueError, KeyError) as e:
        try:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
        except Exception:
            pass
        return None, str(e)

    logger.info(
        f"Group {group.uuid[:8]}: batch_final_step "
        f"lvols={len(lvol_names)} hub={hub_bdev}")
    batch_ok = False
    batch_err = None
    try:
        ret = src_rpc.bdev_lvol_batch_final_step(
            lvol_names, lvol_ids, snapshot_names, 2, hub_bdev, "migrate")
        logger.info(f"Group {group.uuid[:8]}: bdev_lvol_batch_final_step returned {ret!r}")
        batch_ok = True
    except Exception as e:
        logger.error(f"Group {group.uuid[:8]}: bdev_lvol_batch_final_step failed: {e}")
        batch_err = str(e)

    if batch_ok:
        # bdev_lvol_batch_final_step handles add_clone on the primary internally.
        # Secondary and tertiary nodes need an explicit add_clone call for each member's
        # final migrated bdev, linking it to the last intermediate snapshot.
        sec_node, _ = _get_target_secondary_node(tgt_node)
        ter_node, _ = _get_target_tertiary_node(tgt_node)
        if sec_node or ter_node:
            sec_rpc_extra = _make_rpc(sec_node) if sec_node else None
            ter_rpc_extra = _make_rpc(ter_node) if ter_node else None
            for m, snap_composite in zip(member_migrations, snapshot_names):
                if not snap_composite:
                    continue
                try:
                    lvol = db.get_lvol_by_id(m.lvol_id)
                    tgt_bdev_composite = f"{tgt_node.lvstore}/{_lvol_tgt_bdev_name(lvol.lvol_bdev)}"
                    for extra_rpc, extra_label in [
                        (sec_rpc_extra, "secondary"),
                        (ter_rpc_extra, "tertiary"),
                    ]:
                        if not extra_rpc:
                            continue
                        ret = extra_rpc.bdev_lvol_add_clone(tgt_bdev_composite, snap_composite)
                        if not ret:
                            logger.warning(
                                f"Group {group.uuid[:8]}: add_clone on {extra_label} "
                                f"failed for {tgt_bdev_composite} (non-fatal)")
                        else:
                            logger.info(
                                f"Group {group.uuid[:8]}: add_clone on {extra_label} "
                                f"OK: {tgt_bdev_composite} → {snap_composite}")
                except Exception as e:
                    logger.warning(
                        f"Group {group.uuid[:8]}: add_clone for member {m.uuid[:8]} (non-fatal): {e}")

        _flip_ana_to_optimized(group, member_migrations, src_node, src_rpc, tgt_node, tgt_rpc)

    try:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
    except Exception as e:
        logger.warning(f"Group {group.uuid[:8]}: hub detach (non-fatal): {e}")

    return batch_ok, batch_err


def _all_workers_terminal(group):
    """Return True if every worker migration is in a terminal state."""
    expected = {rec['migration_id'] for rec in group.members}
    for mid in expected:
        try:
            m = db.get_migration_by_id(mid)
            if m.is_active():
                return False
        except KeyError:
            pass
    return True


def _handle_cleanup_source_barrier(group):
    """Return True once all workers have signalled cleanup_source_done."""
    expected = {rec['migration_id'] for rec in group.members}
    return expected.issubset(set(group.cleanup_source_done))


def _delete_source_subsystem(group, src_node, src_rpc, tgt_node, tgt_rpc):
    """
    Delete the source NVMe-oF subsystem on all SRC replicas (primary, secondary,
    tertiary).  Overlap nodes (which also host TGT replicas) are skipped because
    the subsystem is still in use on those nodes.  Best-effort.
    """
    nqn = group.target_nqn
    _, _, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)

    def _try_delete(rpc, node_id, label):
        if node_id in overlap_ids:
            logger.info(f"Group {group.uuid[:8]}: skipping {label} subsystem delete (overlap)")
            return
        try:
            rpc.subsystem_delete(nqn)
            logger.info(f"Group {group.uuid[:8]}: deleted {label} source subsystem {nqn}")
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: {label} source subsystem delete (non-fatal): {e}")

    _try_delete(src_rpc, src_node.get_id(), "primary")

    if src_node.secondary_node_id:
        try:
            sec_node = db.get_storage_node_by_id(src_node.secondary_node_id)
            sec_rpc = _make_rpc(sec_node)
            _try_delete(sec_rpc, sec_node.get_id(), "secondary")
        except Exception as e:
            logger.warning(
                f"Group {group.uuid[:8]}: secondary src node lookup (non-fatal): {e}")

    tert_node, _ = _get_target_tertiary_node(src_node)
    if tert_node:
        tert_rpc = _make_rpc(tert_node)
        _try_delete(tert_rpc, tert_node.get_id(), "tertiary")


def _delete_target_subsystem(group, tgt_node, tgt_rpc):
    """Delete the target NVMe-oF subsystem on all TGT replicas.  Best-effort."""
    nqn = group.target_nqn

    def _try_delete(rpc, label):
        try:
            rpc.subsystem_delete(nqn)
            logger.info(f"Group {group.uuid[:8]}: deleted {label} target subsystem {nqn}")
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: {label} target subsystem delete (non-fatal): {e}")

    _try_delete(tgt_rpc, "primary")

    if tgt_node.secondary_node_id:
        try:
            sec_node = db.get_storage_node_by_id(tgt_node.secondary_node_id)
            sec_rpc = _make_rpc(sec_node)
            _try_delete(sec_rpc, "secondary")
        except Exception as e:
            logger.warning(
                f"Group {group.uuid[:8]}: secondary tgt node lookup (non-fatal): {e}")

    tert_node, _ = _get_target_tertiary_node(tgt_node)
    if tert_node:
        tert_rpc = _make_rpc(tert_node)
        _try_delete(tert_rpc, "tertiary")


# ---------------------------------------------------------------------------
# Main task runner
# ---------------------------------------------------------------------------

def task_runner(task):
    """
    Process one iteration of a FN_LVOL_BATCH_MIG task.

    Returns True if the task reached a terminal state (done/failed/cancelled),
    False if it should be retried on the next runner loop iteration.
    """
    task = db.get_task_by_id(task.uuid)
    group_id = task.function_params.get("group_id")
    if not group_id:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = "task missing group_id in function_params"
        task.write_to_db(db.kv_store)
        return True

    try:
        group = db.get_migration_group_by_id(group_id)
    except KeyError:
        task.status = JobSchedule.STATUS_DONE
        task.function_result = f"LVolMigrationGroup {group_id} not found"
        task.write_to_db(db.kv_store)
        return True

    if group.status in (
        LVolMigrationGroup.STATUS_DONE,
        LVolMigrationGroup.STATUS_FAILED,
        LVolMigrationGroup.STATUS_CANCELLED,
    ):
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    try:
        src_node = db.get_storage_node_by_id(group.source_node_id)
    except KeyError:
        task.function_result = f"source node {group.source_node_id} not found"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    try:
        tgt_node = db.get_storage_node_by_id(group.target_node_id)
    except KeyError:
        task.function_result = f"target node {group.target_node_id} not found"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    cluster = db.get_cluster_by_id(group.cluster_id)
    if cluster.status not in (Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED):
        task.function_result = f"cluster not active (status={cluster.status})"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    if task.status in (JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED):
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    src_rpc = _make_rpc(src_node)
    tgt_rpc = _make_rpc(tgt_node)

    member_migrations = []
    for rec in group.members:
        try:
            member_migrations.append(db.get_migration_by_id(rec['migration_id']))
        except KeyError:
            task.function_result = f"worker migration {rec['migration_id']} not found"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return False

    phase = group.phase

    # ── PHASE_SNAP_COPY: wait for all workers, then reconstruct tree ───────────
    if phase == LVolMigrationGroup.PHASE_SNAP_COPY:
        done, err = _handle_snap_copy_barrier(group, member_migrations, tgt_node, tgt_rpc)
        if err:
            task.function_result = err
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            logger.error(f"Group {group_id[:8]}: snap_copy barrier error: {err}")
            return False
        if not done:
            task.write_to_db(db.kv_store)
            return False

        group.phase = LVolMigrationGroup.PHASE_INTERMEDIATE
        group.write_to_db(db.kv_store)
        logger.info(f"Group {group_id[:8]}: advanced to INTERMEDIATE")
        task.write_to_db(db.kv_store)
        return False

    # ── PHASE_INTERMEDIATE: wait for intermediates, then batch_final_step ──────
    if phase == LVolMigrationGroup.PHASE_INTERMEDIATE:
        batch_ok, err = _handle_intermediate_barrier(
            group, member_migrations, src_node, tgt_node, src_rpc, tgt_rpc)

        if err:
            task.function_result = err
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            logger.error(f"Group {group_id[:8]}: intermediate barrier error: {err}")
            return False

        if batch_ok is None:
            # Still waiting for workers.
            task.write_to_db(db.kv_store)
            return False

        group.batch_result = batch_ok
        if batch_ok:
            group.phase = LVolMigrationGroup.PHASE_CLEANUP_SOURCE
            logger.info(
                f"Group {group_id[:8]}: batch_final_step succeeded → CLEANUP_SOURCE")
        else:
            group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
            logger.error(
                f"Group {group_id[:8]}: batch_final_step failed → CLEANUP_TARGET")
        group.write_to_db(db.kv_store)
        task.write_to_db(db.kv_store)
        return False

    # ── PHASE_CLEANUP_SOURCE: wait for workers, then delete source subsystem ───
    if phase == LVolMigrationGroup.PHASE_CLEANUP_SOURCE:
        group = db.get_migration_group_by_id(group_id)
        if not _handle_cleanup_source_barrier(group):
            logger.debug(
                f"Group {group_id[:8]}: waiting for cleanup_source_done "
                f"({len(group.cleanup_source_done)}/{group.member_count()})")
            task.write_to_db(db.kv_store)
            return False

        _delete_source_subsystem(group, src_node, src_rpc, tgt_node, tgt_rpc)

        group.phase = LVolMigrationGroup.PHASE_COMPLETED
        group.status = LVolMigrationGroup.STATUS_DONE
        group.write_to_db(db.kv_store)
        task.status = JobSchedule.STATUS_DONE
        task.function_result = "Batch migration completed successfully"
        task.write_to_db(db.kv_store)
        tasks_events.task_updated(task)
        logger.info(f"Group {group_id[:8]}: batch migration COMPLETED")
        return True

    # ── PHASE_CLEANUP_TARGET: wait for workers, then delete target subsystem ───
    if phase == LVolMigrationGroup.PHASE_CLEANUP_TARGET:
        group = db.get_migration_group_by_id(group_id)
        if not _all_workers_terminal(group):
            logger.debug(f"Group {group_id[:8]}: CLEANUP_TARGET waiting for workers")
            task.write_to_db(db.kv_store)
            return False

        _delete_target_subsystem(group, tgt_node, tgt_rpc)

        group.status = LVolMigrationGroup.STATUS_FAILED
        group.write_to_db(db.kv_store)
        task.status = JobSchedule.STATUS_DONE
        task.function_result = group.error_message or "Batch migration failed; target cleaned up"
        task.write_to_db(db.kv_store)
        tasks_events.task_updated(task)
        logger.error(f"Group {group_id[:8]}: batch migration FAILED")
        return True

    # Unknown phase
    task.function_result = f"Unknown group phase: {phase}"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)
    return True


# ---------------------------------------------------------------------------
# Runner main loop
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting Batch Migration orchestrator task runner...")

    while True:
        try:
            clusters = db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue

        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                for task in db.get_active_batch_migration_tasks(cl.get_id()):
                    task_runner(task)

        time.sleep(3)
