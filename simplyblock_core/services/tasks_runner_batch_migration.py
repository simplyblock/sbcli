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
    Wait for all workers to signal intermediates_done for the current round.
    If any worker's dirty delta is still above the threshold, start another
    synchronized round (every member retakes a snapshot together) up to
    LVOL_MIG_MAX_INTERMEDIATE_SNAPS rounds. Once no more rounds are needed,
    build the batch-final-step argument lists (one entry per member, ordered
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
from typing import Optional

from simplyblock_core import constants, db_controller as db_mod, utils
from simplyblock_core.controllers import migration_controller, migration_events, tasks_controller, tasks_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError, RPCException
from simplyblock_core.services.hub_controller_manager import HubControllerManager
from simplyblock_core.services.tasks_runner_lvol_migration import (
    _make_rpc,
    _snap_tgt_short_name,
    _get_target_secondary_node,
    _get_target_tertiary_node,
    _get_source_tertiary_node,
    _lvol_tgt_bdev_name,
    _build_paths,
    _ensure_and_prune_target_paths,
)

logger = utils.get_logger(__name__)
db = db_mod.DBController()
# Constructed explicitly here, once, rather than as a module-level singleton
# inside hub_controller_manager.py — see that module's docstring. This
# process's own manager; tasks_runner_lvol_migration.py constructs its own
# separate instance, and the two coordinate the detach cooldown via the
# DB-backed HubDetachCooldown record, not shared memory.
hub_manager = HubControllerManager(db)


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

    # A member's snaps_preexisting_on_target (set at create_batch_migration_continue
    # time) conflates two very different things: snaps truly already on the
    # target from OUTSIDE this group (a prior, unrelated migration), and
    # "non_owned_preexisting" snaps -- ancestor snaps in this member's own
    # chain that a DIFFERENT member of THIS SAME group owns and hasn't
    # transferred/committed yet. Only the former may seed `committed` up
    # front; seeding from the latter marks every ancestor snap "already
    # committed" before its true owner ever gets a turn in the loop below,
    # so add_clone/convert never runs for ANY snapshot in the tree.
    owned_or_pending_uuids: set = set()
    for m in member_migrations:
        owned_or_pending_uuids.update(m.snap_migration_plan or [])
        owned_or_pending_uuids.update(m.snaps_transferred_group or [])

    # Global set of snaps that have been committed as immutable on the target,
    # either pre-existing or reconstructed in this call.
    committed: set = set()
    all_preexisting: set = set()
    for m in member_migrations:
        truly_external_preexisting = [
            s for s in m.snaps_preexisting_on_target if s not in owned_or_pending_uuids]
        committed.update(truly_external_preexisting)
        all_preexisting.update(truly_external_preexisting)
        # Snaps already committed in a prior (crashed) run are in snaps_migrated.
        # Seeding committed from them prevents re-convert on re-entry (SPDK rejects
        # converting an already-immutable bdev, which would stall the group forever).
        committed.update(m.snaps_migrated)

    _lvstore_prefix = tgt_node.lvstore + "/"

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
                    if pred_uuid in all_preexisting:
                        # Preexisting snap already lives on target with its canonical
                        # (no-suffix) bdev name.  Check snap_bdev directly first
                        # (home-node case: bdev already has the target lvstore prefix),
                        # then fall back to instances for the non-home-node case.
                        pred_short = None
                        if pred_snap.snap_bdev and pred_snap.snap_bdev.startswith(_lvstore_prefix):
                            pred_short = pred_snap.snap_bdev.split('/', 1)[1]
                        else:
                            for _inst in pred_snap.instances or []:
                                _inst_bdev = _inst.get('snap_bdev', '')
                                if _inst_bdev.startswith(_lvstore_prefix):
                                    pred_short = _inst_bdev.split('/', 1)[1]
                                    break
                        if not pred_short:
                            pred_short = _snap_tgt_short_name(pred_snap)
                            logger.warning(
                                f"Preexisting predecessor {pred_uuid}: no TGT bdev in "
                                f"snap_bdev or instances; falling back to {pred_short!r}")
                    else:
                        pred_short = _snap_tgt_short_name(pred_snap)
                    pred_composite = f"{tgt_node.lvstore}/{pred_short}"
                    if not tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                        return f"bdev_lvol_add_clone failed: {snap_uuid} → {pred_uuid}"
                    if sec_rpc:
                        if not sec_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                            return f"bdev_lvol_add_clone on secondary failed: {snap_uuid} → {pred_uuid}"
                    if ter_rpc:
                        if not ter_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                            return f"bdev_lvol_add_clone on tertiary failed: {snap_uuid} → {pred_uuid}"
                except KeyError:
                    logger.warning(f"Predecessor {pred_uuid} not found; skipping add_clone")

            # Leadership gate: convert on a non-leader silently persists nothing.
            from simplyblock_core.controllers import lvol_controller as _lc
            if not _lc.is_node_leader(tgt_node, tgt_composite.split("/")[0]):
                return f"target node not LVS leader for convert of {snap_uuid}, retrying"
            if not tgt_rpc.bdev_lvol_convert(tgt_composite):
                return f"bdev_lvol_convert failed for {snap_uuid}"
            if sec_rpc:
                if not sec_rpc.bdev_lvol_convert(tgt_composite):
                    return f"bdev_lvol_convert on secondary failed for {snap_uuid}"
            if ter_rpc:
                if not ter_rpc.bdev_lvol_convert(tgt_composite):
                    return f"bdev_lvol_convert on tertiary failed for {snap_uuid}"

            # Early partial DB update: route health-check/delete to the target
            # node right away rather than waiting for apply_migration_to_db()
            # at this worker's CLEANUP_SOURCE -- which, on the batch path, is
            # still several phases (INTERMEDIATE, final-step transfer, ANA
            # flip) and potentially a long wall-clock gap away. Mirrors the
            # single-lvol path's _post_process_snap.
            try:
                snap_rec = db.get_snapshot_by_id(snap_uuid)
                if snap_rec.lvol.uuid == m.lvol_id:
                    snap_rec.lvol.node_id = tgt_node.get_id()
                    snap_rec.write_to_db(db.kv_store)
            except KeyError:
                logger.warning(f"Snapshot {snap_uuid} not found in DB for early node update")

            committed.add(snap_uuid)
            # Update migration record so snaps_migrated reflects committed state.
            if snap_uuid not in m.snaps_migrated:
                m.snaps_migrated.append(snap_uuid)
            # Track this snap's target bdev so cleanup_migration_target() knows
            # to delete it on rollback -- mirrors _post_process_snap; without
            # this the batch path never recorded it, so cleanup silently left
            # every migrated snapshot bdev orphaned on the target.
            if snap_uuid not in m.snaps_preexisting_on_target and tgt_composite not in m.target_snap_bdevs:
                m.target_snap_bdevs.append(tgt_composite)

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
                logger.debug(
                    "Migrated snapshot %s not found while building batch final args for migration %s; "
                    "continuing with empty snapshot path",
                    last_uuid,
                    migration_id,
                )
        elif m.snaps_preexisting_on_target:
            last_uuid = m.snaps_preexisting_on_target[-1]
            try:
                last_snap = db.get_snapshot_by_id(last_uuid)
                _lvstore_prefix = tgt_node.lvstore + "/"
                if last_snap.snap_bdev and last_snap.snap_bdev.startswith(_lvstore_prefix):
                    # Home node: snap_bdev is already the composite path on this lvstore.
                    tgt_snap_composite = last_snap.snap_bdev
                else:
                    # Non-home node: find the instance that lives on the target lvstore.
                    for _inst in last_snap.instances or []:
                        _inst_bdev = _inst.get('snap_bdev', '')
                        if _inst_bdev.startswith(_lvstore_prefix):
                            tgt_snap_composite = _inst_bdev
                            break
            except KeyError:
                logger.debug(
                    "Preexisting snapshot %s not found while building batch final args for migration %s; "
                    "continuing with empty snapshot path",
                    last_uuid,
                    migration_id,
                )
        snapshot_names.append(tgt_snap_composite)

    return lvol_names, lvol_ids, snapshot_names


def _commit_intermediate_snapshot_chain(group, member_migrations, tgt_node, tgt_rpc):
    """
    Link each member's intermediate ("shrink") snapshots into the target
    ancestry chain and freeze them immutable, before bdev_lvol_batch_final_step
    runs.

    _handle_group_intermediate transfers each round's data to the target (and
    registers it on secondary/tertiary via _setup_snap_transfer) but
    deliberately skips add_clone/convert -- same as the snap_copy phase --
    deferring tree-building to the orchestrator. _reconstruct_snap_tree is the
    orchestrator step that normally does that linking, but it only covers the
    snap_copy chain (via snaps_transferred_group) and only runs once, at the
    SNAP_COPY -> INTERMEDIATE transition, before any intermediate round
    exists -- it can never reach forward to link them. _build_batch_final_args
    then picks only the LAST intermediate snapshot (snaps_migrated[-1]) as the
    one boundary snapshot, and the post-final_step code below links only that
    single snapshot to the live bdev. Nothing anywhere ever linked the
    intermediate snapshots to each other, or round 0 to the snap_copy chain's
    last snapshot.

    With exactly one intermediate round this was invisible: the only link
    needed (live bdev -> round 0) was the one link that existed. With two or
    more rounds, every earlier round's target blob was left parentless --
    reads falling outside what that specific round itself captured returned
    zeros instead of falling through to the real predecessor data (observed
    as fio's "bad magic header 0" checksum failures).

    Called once per group, before batch_final_step, so the whole chain is
    committed immutable ahead of the live cutover -- mirrors
    _reconstruct_snap_tree's own add_clone-on-all-replicas-then-convert
    ordering. Returns None on success, or an error string.
    """
    tgt_sec_node, _ = _get_target_secondary_node(tgt_node, "")
    sec_rpc = _make_rpc(tgt_sec_node) if tgt_sec_node else None
    tgt_ter_node, _ = _get_target_tertiary_node(tgt_node, "")
    ter_rpc = _make_rpc(tgt_ter_node) if tgt_ter_node else None
    from simplyblock_core.controllers import lvol_controller as _lc

    for m in member_migrations:
        intermediate_snaps = m.intermediate_snaps or []
        if not intermediate_snaps:
            continue

        # Predecessor for round 0: the last snap_copy-chain snapshot already
        # committed on target -- mirrors _reconstruct_snap_tree's own
        # preexisting-vs-freshly-transferred predecessor lookup.
        pred_composite = None
        pred_uuid = (m.snaps_transferred_group or m.snaps_preexisting_on_target or [None])[-1]
        if pred_uuid:
            try:
                pred_snap = db.get_snapshot_by_id(pred_uuid)
                if pred_uuid in (m.snaps_preexisting_on_target or []):
                    _lvstore_prefix = tgt_node.lvstore + '/'
                    pred_short = None
                    if pred_snap.snap_bdev and pred_snap.snap_bdev.startswith(_lvstore_prefix):
                        pred_short = pred_snap.snap_bdev.split('/', 1)[1]
                    else:
                        for _inst in pred_snap.instances or []:
                            _inst_bdev = _inst.get('snap_bdev', '')
                            if _inst_bdev.startswith(_lvstore_prefix):
                                pred_short = _inst_bdev.split('/', 1)[1]
                                break
                    pred_short = pred_short or _snap_tgt_short_name(pred_snap)
                else:
                    pred_short = _snap_tgt_short_name(pred_snap)
                pred_composite = f"{tgt_node.lvstore}/{pred_short}"
            except KeyError:
                logger.warning(
                    f"Group {group.uuid[:8]}: intermediate-chain predecessor "
                    f"{pred_uuid} not found for member {m.uuid[:8]}; linking round 0 "
                    f"without a parent")

        for snap_uuid in intermediate_snaps:
            try:
                snap = db.get_snapshot_by_id(snap_uuid)
            except KeyError:
                return f"Intermediate snapshot {snap_uuid} not found while committing chain"
            tgt_composite = f"{tgt_node.lvstore}/{_snap_tgt_short_name(snap)}"

            # Same known SPDK behavior _reconstruct_snap_tree already guards
            # against: converting an already-immutable bdev is rejected, so a
            # retry that reaches an earlier round already committed here would
            # otherwise fail every time. bdev_lvol_get_bdevs reports
            # is_snapshot on the primary; treat that as "already done" and
            # just advance the predecessor pointer.
            _existing = tgt_rpc.get_bdevs(tgt_composite)
            if _existing and _existing[0].get('driver_specific', {}).get('lvol', {}).get('is_snapshot'):
                pred_composite = tgt_composite
                continue

            if pred_composite:
                if not tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                    return f"bdev_lvol_add_clone failed for intermediate snap {snap_uuid}"
                if sec_rpc and not sec_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                    return f"bdev_lvol_add_clone on secondary failed for intermediate snap {snap_uuid}"
                if ter_rpc and not ter_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite):
                    return f"bdev_lvol_add_clone on tertiary failed for intermediate snap {snap_uuid}"

            if not _lc.is_node_leader(tgt_node, tgt_composite.split("/")[0]):
                return f"target node not LVS leader for convert of intermediate snap {snap_uuid}, retrying"
            if not tgt_rpc.bdev_lvol_convert(tgt_composite):
                return f"bdev_lvol_convert failed for intermediate snap {snap_uuid}"
            if sec_rpc and not sec_rpc.bdev_lvol_convert(tgt_composite):
                return f"bdev_lvol_convert on secondary failed for intermediate snap {snap_uuid}"
            if ter_rpc and not ter_rpc.bdev_lvol_convert(tgt_composite):
                return f"bdev_lvol_convert on tertiary failed for intermediate snap {snap_uuid}"

            logger.info(
                f"Group {group.uuid[:8]}: committed intermediate snap {snap_uuid[:8]} "
                f"({tgt_composite}) parent={pred_composite}")
            pred_composite = tgt_composite

    return None


def _flip_ana_to_optimized(group, member_migrations, src_node, src_rpc, tgt_node, tgt_rpc):
    """
    After a successful bdev_lvol_batch_final_step, drive clients to the new target.

    Mirrors the single-lvol Done-handler ANA sequence, minus the SRC-inaccessible
    step: _handle_intermediate_barrier's pre-final-step freeze already drives
    every SRC path (primary + secondary/tertiary) inaccessible before
    batch_final_step even runs, so re-flipping them here would just repeat
    the same RPC calls with no effect.

    No-overlap:
      1. TGT primary → optimized  (required; logs error on failure but continues
         since bdev_lvol_batch_final_step cannot be undone)
      2. TGT secondary/tertiary → non_optimized

    Overlap:
      1. First non-overlap TGT → optimized  (live path before touching overlap)
      2. Namespace swap on overlap TGT paths: SRC bdev → migrated TGT bdev
         (uses _swap_namespace which dynamically re-queries nsid; respects crypto_bdev)
      3. All TGT paths → correct ANA state at TGT port
      4. Remove old SRC-port listener from overlap TGT nodes if port changed
    """
    nqn = group.target_nqn
    src_paths, tgt_paths, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
    src_port_by_id = {p['node_id']: p['port'] for p in src_paths}

    # Detect and repair a target-side node restart that wiped the migration's
    # NVMe-oF subsystem/listener/namespace, right before this ANA-flip
    # sequence -- that state is only ever consumed here, at cutover, so there
    # is no need to poll for it during PHASE_SNAP_COPY/PHASE_INTERMEDIATE.
    # All workers share the same NQN/subsystem, so the first member is a
    # representative stand-in. bdev_lvol_batch_final_step has already run and
    # cannot be undone, so unlike solo migration this is always best-effort:
    # secondary/tertiary get pruned from tgt_paths on failure (same as solo),
    # and even a primary failure is only logged -- there is no "abort" option
    # left at this point, matching this function's existing tolerance for any
    # ANA-flip step failing (see docstring above).
    if member_migrations:
        try:
            first_lvol = db.get_lvol_by_id(member_migrations[0].lvol_id)
            tgt_paths, _ensure_err = _ensure_and_prune_target_paths(
                member_migrations[0], first_lvol, tgt_node, tgt_paths)
            if _ensure_err:
                logger.error(
                    f"Group {group.uuid[:8]}: target primary NVMe-oF state check "
                    f"failed (non-fatal, batch_final_step already committed): {_ensure_err}")
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: target NVMe-oF state check error (non-fatal): {e}")

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

    def _flip_all(rpc, ips, port, trtype, state, label):
        for _ip in ips:
            _flip(rpc, _ip, port, trtype, state, label)

    def _flip_all_required(rpc, ips, port, trtype, state, label, attempts=3):
        ok = True
        for _ip in ips:
            if not _flip_required(rpc, _ip, port, trtype, state, label, attempts):
                ok = False
        return ok

    if not overlap_ids:
        # Step 1 (no-overlap): TGT primary → optimized
        primary_tgt = tgt_paths[0]
        if not _flip_all_required(primary_tgt['rpc'], primary_tgt['ips'], primary_tgt['port'],
                                   primary_tgt['trtype'], "optimized",
                                   f"TGT-{primary_tgt['node_id'][:8]}"):
            logger.error(
                f"Group {group.uuid[:8]}: ANA flip TGT primary→optimized failed; "
                f"clients may be on degraded path")

        # Step 2: TGT secondary/tertiary → non_optimized
        for i, tp in enumerate(tgt_paths[1:], 1):
            _flip_all(tp['rpc'], tp['ips'], tp['port'], tp['trtype'], "non_optimized", f"TGT-rep{i}")
    else:
        # Step 1: first non-overlap TGT → optimized. SRC paths (overlap and
        # non-overlap alike) are already inaccessible from the pre-final-step
        # freeze -- no need to re-flip them here.
        non_overlap_tgt = next(
            (t for t in tgt_paths if t['node_id'] not in overlap_ids), None)
        if non_overlap_tgt:
            if not _flip_all_required(non_overlap_tgt['rpc'], non_overlap_tgt['ips'],
                                       non_overlap_tgt['port'], non_overlap_tgt['trtype'],
                                       "optimized",
                                       f"TGT-{non_overlap_tgt['node_id'][:8]}(pre)"):
                logger.error(
                    f"Group {group.uuid[:8]}: ANA flip non-overlap TGT→optimized failed; "
                    f"proceeding anyway")

        # Step 2: namespace swap on overlap TGT paths.
        # Each member has its own namespace in the shared NQN. We look up each
        # member's nsid by matching ns['uuid'] == lvol.uuid so we never remove
        # the wrong namespace (positional removal would corrupt I/O for other members).
        # Query subsystem once per overlap node, then match by UUID for each member.
        for tgt in tgt_paths:
            if tgt['node_id'] not in overlap_ids:
                continue
            try:
                s = tgt['rpc'].subsystem_get(nqn)
                ns_by_uuid = {
                    ns.get('uuid'): ns['nsid']
                    for ns in (s.get('namespaces', []) if s else [])
                }
            except Exception as e:
                logger.warning(
                    f"Group {group.uuid[:8]}: subsystem_get on {tgt['node_id'][:8]} "
                    f"(non-fatal): {e}")
                ns_by_uuid = {}

            # Two-pass swap: remove ALL old namespaces first, then add ALL new ones.
            # A per-member remove+add loop would cause N sequential "namespace gone"
            # events visible to initiators — each one triggering a reconnect. Batching
            # the removes into one pass and the adds into a second pass collapses this
            # into a single collective disruption, which initiators handle cleanly.
            ns_adds = []  # (tgt_ns_bdev, uuid, guid, nsid) — collected during remove pass
            for m in member_migrations:
                try:
                    lvol = db.get_lvol_by_id(m.lvol_id)
                    tgt_bdev_short = _lvol_tgt_bdev_name(lvol.lvol_bdev)
                    tgt_ns_bdev = (
                        f"crypto_{tgt_bdev_short}" if lvol.crypto_bdev
                        else f"{tgt_node.lvstore}/{tgt_bdev_short}"
                    )
                    nsid = ns_by_uuid.get(lvol.uuid)
                    if nsid:
                        try:
                            tgt['rpc'].nvmf_subsystem_remove_ns(nqn, nsid)
                            logger.info(
                                f"Group {group.uuid[:8]}: swap NS {tgt['node_id'][:8]}: "
                                f"removed nsid={nsid} for lvol {lvol.uuid[:8]}")
                        except Exception as e:
                            logger.warning(
                                f"Group {group.uuid[:8]}: remove ns {tgt['node_id'][:8]} "
                                f"nsid={nsid} (non-fatal): {e}")
                    else:
                        logger.warning(
                            f"Group {group.uuid[:8]}: no namespace for uuid={lvol.uuid[:8]} "
                            f"on {tgt['node_id'][:8]}; skipping remove")
                    # Re-add under the SAME nsid it just had here, instead of letting
                    # SPDK auto-assign the next free one. Client-side identity across
                    # the swap is carried by uuid/nguid regardless (Linux NVMe
                    # multipath groups paths by NGUID, not nsid), so this isn't a
                    # correctness fix -- it just keeps the namespace's nsid stable on
                    # this node across the swap instead of drifting to a new value.
                    ns_adds.append((tgt_ns_bdev, lvol.uuid, lvol.guid, nsid))
                except Exception as e:
                    logger.warning(
                        f"Group {group.uuid[:8]}: namespace swap member {m.uuid[:8]} "
                        f"on {tgt['node_id'][:8]} (non-fatal): {e}")

            for tgt_ns_bdev, uuid, guid, nsid in ns_adds:
                try:
                    ret = tgt['rpc'].nvmf_subsystem_add_ns(nqn, tgt_ns_bdev, uuid, guid, nsid=nsid)
                    if not ret:
                        logger.error(
                            f"Group {group.uuid[:8]}: add ns {tgt_ns_bdev} failed "
                            f"on {tgt['node_id'][:8]}")
                    else:
                        logger.info(
                            f"Group {group.uuid[:8]}: swap NS {tgt['node_id'][:8]}: "
                            f"added {tgt_ns_bdev}")
                except Exception as e:
                    logger.warning(
                        f"Group {group.uuid[:8]}: add ns {tgt_ns_bdev} "
                        f"on {tgt['node_id'][:8]} (non-fatal): {e}")

        # Step 3: all TGT paths → correct ANA state at TGT port
        primary_tgt = tgt_paths[0]
        if not _flip_all_required(primary_tgt['rpc'], primary_tgt['ips'], primary_tgt['port'],
                                   primary_tgt['trtype'], "optimized",
                                   f"TGT-{primary_tgt['node_id'][:8]}"):
            logger.error(
                f"Group {group.uuid[:8]}: ANA flip TGT primary→optimized (step 3) failed")
        for tgt in tgt_paths[1:]:
            _flip_all(tgt['rpc'], tgt['ips'], tgt['port'], tgt['trtype'],
                      "non_optimized", f"TGT-{tgt['node_id'][:8]}")

        # Step 4: remove old SRC-port listener from overlap TGT nodes if port changed
        for tgt in tgt_paths:
            if tgt['node_id'] in overlap_ids:
                old_port = src_port_by_id.get(tgt['node_id'])
                if old_port and old_port != tgt['port']:
                    for _ip in tgt['ips']:
                        try:
                            tgt['rpc'].listeners_del(nqn, tgt['trtype'], _ip, old_port)
                            logger.info(
                                f"Group {group.uuid[:8]}: removed old SRC listener "
                                f"{_ip}:{old_port} from overlap {tgt['node_id'][:8]}")
                        except Exception as e:
                            logger.warning(
                                f"Group {group.uuid[:8]}: remove old SRC listener "
                                f"{tgt['node_id'][:8]} (non-fatal): {e}")


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

    # Every member finished this round. If any of them still has too much
    # dirty delta to freeze quickly at cutover, start another synchronized
    # round -- every member retakes a snapshot together, even ones whose own
    # delta was already low -- up to the round cap.
    if (group.intermediate_more_needed
            and group.intermediate_round + 1 < constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS):
        group.intermediate_round += 1
        group.intermediates_done = []
        group.intermediate_more_needed = []
        group.write_to_db(db.kv_store)
        logger.info(
            f"Group {group.uuid[:8]}: dirty delta still high after round "
            f"{group.intermediate_round}/{constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS}; "
            f"starting another synchronized intermediate round")
        return None, None  # None = still waiting -- workers will redo this round

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
        # Hub controller left attached — hub_manager owns its lifecycle
        # entirely via its own idle timeout.
        return None, str(e)

    chain_err = _commit_intermediate_snapshot_chain(group, member_migrations, tgt_node, tgt_rpc)
    if chain_err:
        # Hub controller left attached — see comment above.
        return None, chain_err

    # Pre-freeze: take SRC/TGT paths out of the read/write path before the
    # synchronous final-step transfer below (see the diagnostic block further
    # down for the current, temporarily-widened version of this).
    nqn = group.target_nqn
    src_paths, tgt_paths, _ = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
    src_replica_paths = src_paths[1:]  # secondary/tertiary only; used for the failure-path revert below

    def _flip(rpc, ip, port, trtype, state, label):
        try:
            rpc.nvmf_subsystem_listener_set_ana_state(nqn, ip, port, trtype=trtype, ana=state)
            logger.info(f"Group {group.uuid[:8]}: ANA {label} {ip}:{port} → {state}")
            return True
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: ANA {label} {ip}:{port} (non-fatal): {e}")
            return False

    def _flip_all(rpc, ips, port, trtype, state, label):
        for _ip in ips:
            _flip(rpc, _ip, port, trtype, state, label)

    def _revert_src_replicas(reason):
        # Final step didn't complete — put every SRC path back into the
        # read/write path (their pre-freeze state) so clients keep access to
        # the still-live source instead of being stuck with nothing reachable.
        # Primary -> optimized (it was driven inaccessible pre-final-step by
        # the diagnostic widened freeze above); secondary/tertiary -> non_optimized.
        logger.warning(f"Group {group.uuid[:8]}: {reason}; reverting SRC paths "
                       f"(primary optimized, replicas non_optimized)")
        primary_src = src_paths[0]
        _flip_all(primary_src['rpc'], primary_src['ips'], primary_src['port'],
                  primary_src['trtype'], "optimized", f"SRC-{primary_src['node_id'][:8]}(revert)")
        for p in src_replica_paths:
            _flip_all(p['rpc'], p['ips'], p['port'], p['trtype'],
                      "non_optimized", f"SRC-{p['node_id'][:8]}(revert)")

    # TEMPORARILY CHANGED for a diagnostic test: instead of only freezing SRC
    # secondary/tertiary pre-final-step (primary relied on the RPC's own
    # internal freeze), make EVERY path -- all SRC (primary included) and all
    # TGT -- inaccessible up front, wait 2s so any in-flight client I/O has
    # time to fully settle/drain before the data actually moves, THEN call
    # final_step. Re-enable the narrower pre-freeze once this test is done.
    logger.info(f"Group {group.uuid[:8]}: setting ALL SRC and TGT paths inaccessible "
               f"pre-final-step (diagnostic)")
    for p in src_paths:
        _flip_all(p['rpc'], p['ips'], p['port'], p['trtype'],
                  "inaccessible", f"SRC-{p['node_id'][:8]}(pre-freeze)")
    for p in tgt_paths:
        _flip_all(p['rpc'], p['ips'], p['port'], p['trtype'],
                  "inaccessible", f"TGT-{p['node_id'][:8]}(pre-freeze)")
    logger.info(f"Group {group.uuid[:8]}: sleeping 2s after all-paths-inaccessible "
               f"before batch_final_step (diagnostic)")
    time.sleep(2)

    logger.info(
        f"Group {group.uuid[:8]}: batch_final_step "
        f"lvols={len(lvol_names)} hub={hub_bdev}")
    batch_ok = False
    batch_err = None
    try:
        # This call moves real data and can legitimately run longer than the
        # 5s blanket timeout _make_rpc()/src_rpc uses for every other RPC in
        # this file -- use a dedicated, longer-timeout client just for it.
        final_step_rpc = src_node.rpc_client(timeout=15, retry=2)
        ret = final_step_rpc.bdev_lvol_batch_transfer_final_step(
            lvol_names, lvol_ids, snapshot_names,
            constants.LVOL_MIG_TRANSFER_BATCH_SIZE, hub_bdev, "migrate")
        logger.info(f"Group {group.uuid[:8]}: bdev_lvol_batch_transfer_final_step returned {ret!r}")
        # The RPC can return normally (no exception) while still reporting the
        # transfer itself failed -- transfer_state is one of "No process" |
        # "In progress" | "Failed" | "Done" (see bdev_lvol_transfer_stat).
        # Treating any non-exception response as success let a "Failed"
        # transfer proceed straight to the ANA flip and source cleanup as if
        # the data had actually moved (observed run 2026-08-22, group
        # 911aa7af/mig-108: 'Failed' logged then treated as success, target
        # listeners never came up, checksum corruption followed).
        transfer_state = ret.get("transfer_state") if isinstance(ret, dict) else None
        if transfer_state == "Done":
            batch_ok = True
        else:
            batch_err = f"transfer_state={transfer_state!r} (expected 'Done'): {ret!r}"
            logger.error(f"Group {group.uuid[:8]}: bdev_lvol_batch_transfer_final_step "
                        f"did not report success: {batch_err}")
    except RPCRemoteError as e:
        logger.error(f"Group {group.uuid[:8]}: bdev_lvol_batch_transfer_final_step RPC error code={e.code}: {e}")
        batch_err = str(e)
        if e.code == RPCErrorCode.method_not_found:
            _revert_src_replicas("batch_final_step failed (method_not_found)")
            return False, batch_err  # Retrying will never help; surface as fatal so the group fails immediately.
    except Exception as e:
        logger.error(f"Group {group.uuid[:8]}: bdev_lvol_batch_transfer_final_step failed: {e}")
        batch_err = str(e)

    if not batch_ok:
        _revert_src_replicas("batch_final_step failed")
        # The revert above reopened SRC to live client I/O. Retrying with the
        # snapshots taken before this reopen would silently miss whatever the
        # client writes in the meantime -- force every member through one
        # more synchronized intermediate round first, same mechanism as the
        # dirty-delta trigger above, so the retry's snapshots actually cover
        # the reopen window. Falls through to the normal suspend/retry-budget
        # path once the round cap is hit, so a persistently failing group
        # still eventually resolves instead of looping forever.
        if group.intermediate_round + 1 < constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS:
            group.intermediate_round += 1
            group.intermediates_done = []
            group.intermediate_more_needed = []
            group.write_to_db(db.kv_store)

            # bdev_lvol_set_migration_flag drives the distrib-level special_io
            # machinery for the target bdev (see snapshot_replication.py's
            # comment on the same flag); it's only ever set once, at initial
            # target-bdev creation (migration_controller.create_migration).
            # A failed/aborted final_step attempt may clear it on the target,
            # so re-assert it on every member's target bdev before retrying —
            # otherwise the retry's cutover could run without the target
            # being treated as migration-aware.
            tgt_sec_node, _ = _get_target_secondary_node(tgt_node, src_node.get_id())
            tgt_ter_node, _ = _get_target_tertiary_node(tgt_node, src_node.get_id())
            tgt_sec_rpc_reflag = _make_rpc(tgt_sec_node) if tgt_sec_node else None
            tgt_ter_rpc_reflag = _make_rpc(tgt_ter_node) if tgt_ter_node else None
            for m in member_migrations:
                try:
                    m_lvol = db.get_lvol_by_id(m.lvol_id)
                    m_tgt_composite = f"{tgt_node.lvstore}/{_lvol_tgt_bdev_name(m_lvol.lvol_bdev)}"
                except KeyError:
                    continue
                if not tgt_rpc.bdev_lvol_set_migration_flag(m_tgt_composite):
                    logger.warning(
                        f"Group {group.uuid[:8]}: re-assert migration flag on primary "
                        f"failed for {m_tgt_composite} (may already be flagged)")
                for _extra_rpc in (tgt_sec_rpc_reflag, tgt_ter_rpc_reflag):
                    if _extra_rpc:
                        try:
                            _extra_rpc.bdev_lvol_set_migration_flag(m_tgt_composite)
                        except Exception as e:
                            logger.warning(
                                f"Group {group.uuid[:8]}: re-assert migration flag on "
                                f"replica failed for {m_tgt_composite} (non-fatal): {e}")

            logger.warning(
                f"Group {group.uuid[:8]}: batch_final_step failed; forcing another "
                f"synchronized intermediate round {group.intermediate_round}/"
                f"{constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS} before retrying")
            return None, None  # None = still waiting -- workers will redo this round
    # else: left as-is — all SRC/TGT paths were already driven inaccessible
    # before final_step (diagnostic, see above); only TGT primary needs to
    # come back optimized on success, handled below.

    if batch_ok:
        # bdev_lvol_batch_final_step handles add_clone on the primary internally.
        # Secondary and tertiary nodes need an explicit add_clone call for each member's
        # final migrated bdev, linking it to the last intermediate snapshot.
        sec_node, _ = _get_target_secondary_node(tgt_node, src_node.get_id())
        ter_node, _ = _get_target_tertiary_node(tgt_node, src_node.get_id())
        if sec_node or ter_node:
            sec_rpc_extra = _make_rpc(sec_node) if sec_node else None
            ter_rpc_extra = _make_rpc(ter_node) if ter_node else None
            _reordered_ids = group.ordered_migration_ids()
            snap_by_migration_id = dict(zip(_reordered_ids, snapshot_names))
            for m in member_migrations:
                snap_composite = snap_by_migration_id.get(m.uuid, "")
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

    # Hub controller left attached on both success and failure — hub_manager
    # owns its lifecycle entirely via its own idle timeout. Detaching it here
    # unconditionally, on every group's final step, defeated the whole point
    # of keeping it warm for the next group to reuse.
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
            logger.debug(
                "_all_workers_terminal: worker migration %s not found; treating as terminal", mid,
            )
    return True


def _handle_cleanup_source_barrier(group):
    """Return True once all workers have signalled cleanup_source_done.

    Workers that are already terminal (DONE/FAILED/CANCELLED) without having
    signalled are counted as complete — they will never signal, so waiting for
    them would block the orchestrator indefinitely.
    """
    expected = {rec['migration_id'] for rec in group.members}
    done_set = set(group.cleanup_source_done)
    remaining = expected - done_set
    for mid in list(remaining):
        try:
            m = db.get_migration_by_id(mid)
            if not m.is_active():
                done_set.add(mid)
        except KeyError:
            done_set.add(mid)
    return expected.issubset(done_set)


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

    tert_node = _get_source_tertiary_node(src_node)
    if tert_node:
        tert_rpc = _make_rpc(tert_node)
        _try_delete(tert_rpc, tert_node.get_id(), "tertiary")


def _delete_target_subsystem(group, src_node, src_rpc, tgt_node, tgt_rpc):
    """Delete the target NVMe-oF subsystem on all TGT replicas.  Best-effort.

    Nodes that appear in both SRC and TGT replica sets (overlap nodes) share the
    subsystem; deleting the target-side subsystem on them would remove the source
    listener too, so those nodes are skipped here.
    """
    nqn = group.target_nqn

    try:
        _, _, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
    except Exception as e:
        logger.warning(
            f"Group {group.uuid[:8]}: _build_paths in _delete_target_subsystem (non-fatal): {e}")
        overlap_ids = set()

    def _try_delete(rpc, node_id, label):
        if node_id in overlap_ids:
            logger.debug(
                f"Group {group.uuid[:8]}: skip {label} tgt subsystem delete — overlap node")
            return
        try:
            rpc.subsystem_delete(nqn)
            logger.info(f"Group {group.uuid[:8]}: deleted {label} target subsystem {nqn}")
        except Exception as e:
            logger.warning(f"Group {group.uuid[:8]}: {label} target subsystem delete (non-fatal): {e}")

    _try_delete(tgt_rpc, tgt_node.get_id(), "primary")

    if tgt_node.secondary_node_id:
        try:
            sec_node = db.get_storage_node_by_id(tgt_node.secondary_node_id)
            sec_rpc = _make_rpc(sec_node)
            _try_delete(sec_rpc, sec_node.get_id(), "secondary")
        except Exception as e:
            logger.warning(
                f"Group {group.uuid[:8]}: secondary tgt node lookup (non-fatal): {e}")

    tert_node, _ = _get_target_tertiary_node(tgt_node, "")
    if tert_node:
        tert_rpc = _make_rpc(tert_node)
        _try_delete(tert_rpc, tert_node.get_id(), "tertiary")


# ---------------------------------------------------------------------------
# Retry-budget helper
# ---------------------------------------------------------------------------

def _batch_budget_suspend(task, group, group_id, error_msg):
    """Charge retry budget and suspend; redirect to cleanup_target when exhausted.

    Uses constants.LVOL_MIG_MAX_RETRIES as the internal ceiling, independent of
    task.max_retry (which is set to -1 to disable the backup runner's kill switch).
    """
    task.retry += 1
    task.function_result = error_msg
    if task.retry >= constants.LVOL_MIG_MAX_RETRIES:
        ceiling_msg = (
            f"Group {group_id[:8]}: max retry ({constants.LVOL_MIG_MAX_RETRIES}) "
            f"reached; entering cleanup_target: {error_msg}"
        )
        logger.error(ceiling_msg)
        group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
        group.error_message = ceiling_msg
        task.function_result = ceiling_msg
        group.write_to_db(db.kv_store)
        for rec in group.members:
            try:
                mig = db.get_migration_by_id(rec['migration_id'])
                migration_events.migration_phase_changed(mig)
            except Exception:
                pass
    task.status = JobSchedule.STATUS_SUSPENDED
    task.write_to_db(db.kv_store)
    return False


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
        return _batch_budget_suspend(
            task, group, group_id, f"source node {group.source_node_id} not found")

    try:
        tgt_node = db.get_storage_node_by_id(group.target_node_id)
    except KeyError:
        return _batch_budget_suspend(
            task, group, group_id, f"target node {group.target_node_id} not found")

    phase = group.phase
    _is_cleanup_phase = phase in (
        LVolMigrationGroup.PHASE_CLEANUP_TARGET,
        LVolMigrationGroup.PHASE_CLEANUP_SOURCE,
    )

    cluster = db.get_cluster_by_id(group.cluster_id)
    if cluster.status not in Cluster.MUTABLE_STATUSES:
        if not _is_cleanup_phase:
            task.function_result = f"cluster not active (status={cluster.status})"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return False

    if tasks_controller.get_active_cluster_expand_task(task.cluster_id):
        if not _is_cleanup_phase:
            task.function_result = "cluster expansion in progress, deferring"
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

    # If source or target went offline during data-transfer phases, enter
    # CLEANUP_TARGET immediately — same fast-path as the single-lvol runner.
    _data_transfer_phases = (LVolMigrationGroup.PHASE_SNAP_COPY,
                             LVolMigrationGroup.PHASE_INTERMEDIATE)
    if phase in _data_transfer_phases:
        fresh_tgt = db.get_storage_node_by_id(group.target_node_id)
        if fresh_tgt.status != StorageNode.STATUS_ONLINE:
            logger.warning(
                f"Group {group_id[:8]}: target node offline "
                f"(status={fresh_tgt.status}) during {phase}; entering cleanup_target")
            group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
            group.error_message = (
                f"target node offline (status={fresh_tgt.status}); batch migration failed")
            group.write_to_db(db.kv_store)
            task.function_result = group.error_message
            task.write_to_db(db.kv_store)
            return False

        fresh_src = db.get_storage_node_by_id(group.source_node_id)
        if fresh_src.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
            logger.warning(
                f"Group {group_id[:8]}: source node unavailable "
                f"(status={fresh_src.status}) during {phase}; suspending")
            return _batch_budget_suspend(
                task, group, group_id,
                f"source node unavailable (status={fresh_src.status})")

    # --- Deadline check (GAP F2) ---
    if not _is_cleanup_phase and member_migrations:
        first_mig = member_migrations[0]
        if first_mig.has_deadline_passed():
            logger.warning(f"Group {group_id[:8]}: migration deadline exceeded; entering cleanup_target")
            group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
            group.error_message = "migration deadline exceeded"
            group.write_to_db(db.kv_store)
            task.function_result = "migration deadline exceeded"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return False

    try:
        # ── PHASE_SNAP_COPY: wait for all workers, then reconstruct tree ─────────
        if phase == LVolMigrationGroup.PHASE_SNAP_COPY:
            done, err = _handle_snap_copy_barrier(group, member_migrations, tgt_node, tgt_rpc)
            if err:
                logger.error(f"Group {group_id[:8]}: snap_copy barrier error: {err}")
                return _batch_budget_suspend(task, group, group_id, err)
            if not done:
                task.write_to_db(db.kv_store)
                return False

            group.phase = LVolMigrationGroup.PHASE_INTERMEDIATE
            group.write_to_db(db.kv_store)
            logger.info(f"Group {group_id[:8]}: advanced to INTERMEDIATE")
            task.write_to_db(db.kv_store)
            return False

        # ── PHASE_INTERMEDIATE: wait for intermediates, then batch_final_step ────
        if phase == LVolMigrationGroup.PHASE_INTERMEDIATE:
            batch_ok, err = _handle_intermediate_barrier(
                group, member_migrations, src_node, tgt_node, src_rpc, tgt_rpc)

            if err:
                logger.error(f"Group {group_id[:8]}: intermediate barrier error: {err}")
                return _batch_budget_suspend(task, group, group_id, err)

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

    except RPCException as exc:
        logger.warning(f"Group {group_id[:8]}: RPC error in phase {phase}: {exc}")
        fresh_tgt = db.get_storage_node_by_id(group.target_node_id)
        if fresh_tgt.status != StorageNode.STATUS_ONLINE:
            logger.warning(
                f"Group {group_id[:8]}: target offline during {phase}; entering cleanup_target")
            group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
            group.error_message = f"target node offline during {phase}: {exc}"
            group.write_to_db(db.kv_store)
            task.function_result = str(exc)
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return False
        return _batch_budget_suspend(task, group, group_id, f"RPC error in phase {phase}: {exc}")

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

        _delete_target_subsystem(group, src_node, src_rpc, tgt_node, tgt_rpc)

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

def main():
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
                    # Lease gate: skip a task another live runner host owns, so
                    # two replicas can't both drive the same batch migration's
                    # multi-phase data-plane state-machine concurrently.
                    if not tasks_controller.claim_task(task):
                        logger.info(f"Batch-migration task {task.uuid} owned by another runner host; skipping")
                        continue
                    with tasks_controller.task_lease_heartbeat(task):
                        task_runner(task)

        time.sleep(3)


if __name__ == "__main__":
    main()
