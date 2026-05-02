# coding=utf-8
"""
tasks_runner_lvol_migration.py – background task runner for live volume migration.

This runner is the data-plane orchestrator.  It is driven by JobSchedule tasks
of type FN_LVOL_MIG and advances the associated LVolMigration through its
phase state-machine until completion or permanent failure.

Phase state-machine
-------------------
  NEW / SUSPENDED
      ↓  (preconditions met)
  RUNNING
      ↓
  [PHASE_SNAP_COPY]
      For each snapshot in snap_migration_plan (index: next_snap_index):
        1. Check target secondary node state (block if not online/offline)
        2. Create a writable lvol on target  (bdev_lvol_create with same UUID)
        3. bdev_lvol_set_migration_flag on target
        4. Expose target lvol via NVMe-oF (temp subsystem + listener + namespace)
        5. bdev_nvme_attach_controller on source  →  remote bdev name = ctrl+"n1"
        6. bdev_lvol_transfer on source (async)
        7. Poll bdev_lvol_transfer_stat until Done/Failed
        8. bdev_lvol_add_clone on target linking to predecessor (if any)
        9. bdev_lvol_convert on target to freeze as snapshot
       10. Register snapshot on target secondary (if online)
       11. Detach temp controller on source; delete temp subsystem on target
      After all planned snaps: take ≤ max_intermediate_snap_rounds intermediate
        "shrink" snapshots and transfer each the same way to minimise the delta.
      When all snapshots copied → advance to PHASE_LVOL_MIGRATE.

  [PHASE_LVOL_MIGRATE]
      1. Check target secondary node state
      2. Create target lvol with the SAME NQN as the source lvol's subsystem
      3. Get target blobid via bdev_lvol_get_lvols
      4. Connect source to target's hub lvol (bdev_nvme_attach_controller)
      5. bdev_lvol_final_migration on source (async)
      6. Poll bdev_lvol_transfer_stat on source lvol until Done/Failed
      7. Register lvol on target secondary (if online)
      8. Create subsystem + listeners + namespace on target secondary (if online)
      → advance to PHASE_CLEANUP_SOURCE

  [PHASE_CLEANUP_SOURCE]
      Delete snapshots on the source that are exclusively owned by this volume
      (verified via migration_controller.get_snaps_safe_to_delete_on_source()).
      Uses storage_node_ops.safe_delete_bdev() for multi-step async deletion
      (async start → poll → sync finalize on primary and secondary).
      Calls apply_migration_to_db() after source cleanup is complete.
      → advance to PHASE_COMPLETED → mark task + migration DONE

  [PHASE_CLEANUP_TARGET]   ← entered on failure or cancellation
      Delete snapshots on the target that are safe to remove, using
      storage_node_ops.safe_delete_bdev() which implements the full
      async-poll-sync-secondary delete pattern.
      Also cleans up any partially-created target lvol/subsystem.
      → mark task + migration FAILED / CANCELLED

Transfer context
----------------
``migration.transfer_context`` is a dict persisted to FDB that tracks the
fine-grained state of a single in-progress async operation so that the runner
can resume after a process restart:

  stage     : "transfer"
  snap_uuid : snapshot UUID being transferred  (SNAP_COPY phase only)
  temp_nqn  : temporary NVMe-oF subsystem NQN  (SNAP_COPY phase only)
  ctrl_name : NVMe-oF controller name on source node
  nqn       : volume subsystem NQN             (LVOL_MIGRATE phase only)
  tgt_lvol_created : bool                      (LVOL_MIGRATE phase only)

Idempotency
-----------
To survive a crash between issuing an async RPC and persisting its context to
FDB, the runner writes ``transfer_context`` to FDB *before* calling
``bdev_lvol_transfer`` / ``bdev_lvol_final_migration``.  On restart, the
phase handler checks ``bdev_lvol_transfer_stat`` to detect an already-running
transfer and reconstructs the context without issuing a second RPC.

Performance
-----------
``_handle_snap_copy`` runs a ``while True`` loop so that consecutive snapshots
are started back-to-back within one invocation; it only returns to the caller
when it must wait for an async data-plane transfer.  Phase transitions also
happen immediately via a tail-recursive call to ``task_runner``, eliminating
the 3-second service-loop gap between phases.
"""

import time

from simplyblock_core import db_controller as db_mod, utils, constants
from simplyblock_core.controllers import (
    migration_controller, migration_events, snapshot_controller, tasks_events
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)
db = db_mod.DBController()

# Sentinel used as the ``error`` return value when a phase handler wants to
# suspend the task WITHOUT incrementing the retry counter.  This is distinct
# from a real operation failure: it signals a *transient external condition*
# (e.g. secondary node in unexpected state) that the runner should wait for,
# not charge against the retry budget.
_WAIT = object()

# Busy-poll settings for intermediate ("shrink") snapshot transfers.
# Intermediate snapshots represent a small dirty delta so they should complete
# quickly; we spin at _INTERMEDIATE_POLL_INTERVAL_S rather than waiting for
# the next 3-second service-loop iteration.
_INTERMEDIATE_POLL_INTERVAL_S = 0.1   # seconds between stat checks
_INTERMEDIATE_POLL_MAX = 3000          # max iterations ≈ 5 min


# ---------------------------------------------------------------------------
# NIC / transport helpers
# ---------------------------------------------------------------------------

def _get_migration_nic(node):
    """Return (trtype, ip_address) for the preferred migration interface."""
    trtype = "RDMA" if node.active_rdma else "TCP"
    for nic in node.data_nics:
        if nic.ip4_address:
            return trtype, nic.ip4_address
    return trtype, node.mgmt_ip


def _snap_short_name(snap):
    """Return the bare bdev name for a snapshot, stripping any lvstore prefix."""
    path = snap.snap_bdev
    return path.split('/', 1)[1] if '/' in path else path


def _snap_composite(lvstore, snap):
    """SPDK composite bdev name for a snapshot on a given node: ``<lvstore>/<bdev>``."""
    return f"{lvstore}/{_snap_short_name(snap)}"


def _bytes_to_mib(nbytes):
    """Convert bytes to MiB, rounding up.  Returns at least 1."""
    if nbytes <= 0:
        return 1
    return max(1, (int(nbytes) + 1024 * 1024 - 1) // (1024 * 1024))


def _delete_bdev_blocking(bdev_name, primary_rpc, secondary_rpc=None, max_polls=120):
    """
    Full 3-step async-delete sequence for use in synchronous error-recovery paths.
    Mirrors the control-plane pattern in storage_node_ops.safe_delete_bdev():

      1. delete_lvol(sync=False) on primary  – start async background deletion
      2. poll bdev_lvol_get_lvol_delete_status until complete (0) or not-found (2)
      3. delete_lvol(sync=True)  on primary  – sync finalize / confirm removal
         delete_lvol(sync=True)  on secondary – sync finalize (best-effort)

    Blocks for up to max_polls × 0.25 s.  Use only in error-recovery paths where
    a bdev was just created and must be cleaned up before returning.
    """
    # Step 1: start async deletion
    ret, _ = primary_rpc.delete_lvol(bdev_name)
    if not ret:
        logger.warning(f"delete bdev {bdev_name}: async start failed (continuing)")

    # Step 2: poll
    for _ in range(max_polls):
        status = primary_rpc.bdev_lvol_get_lvol_delete_status(bdev_name)
        if status in (0, 2):
            break
        if status == 1:
            time.sleep(0.25)
        else:
            logger.warning(f"delete bdev {bdev_name}: unexpected status {status}")
            break

    # Step 3: sync finalize
    primary_rpc.delete_lvol(bdev_name, del_async=True)
    if secondary_rpc:
        secondary_rpc.delete_lvol(bdev_name, del_async=True)


# ---------------------------------------------------------------------------
# Secondary-node helpers
# ---------------------------------------------------------------------------

def _get_target_secondary_node(tgt_node):
    """
    Return ``(sec_node, error_string)`` describing how to handle the target's
    secondary node when creating a new object on the target primary.

    Rules (consistent with migration policy):
      - No secondary configured   → (None, None)   skip silently
      - Secondary STATUS_ONLINE   → (sec_node, None) register on secondary
      - Secondary STATUS_OFFLINE  → (None, None)   administratively down, skip
      - Any other status          → (None, err)    block creation on primary
    """
    if not tgt_node.secondary_node_id:
        return None, None
    try:
        sec = db.get_storage_node_by_id(tgt_node.secondary_node_id)
    except KeyError:
        return None, None

    if sec.status == StorageNode.STATUS_ONLINE:
        return sec, None
    if sec.status == StorageNode.STATUS_OFFLINE:
        return None, None
    return None, (
        f"Target secondary node {tgt_node.secondary_node_id} is in state "
        f"'{sec.status}'; cannot create on target primary"
    )


def _get_target_secondary_nodes(tgt_node):
    """
    Return ``(sec_nodes_list, error_string)`` for all secondaries on the target.
    Checks both secondary_node_id and tertiary_node_id.
    """
    sec_nodes = []
    for peer_id in [tgt_node.secondary_node_id, tgt_node.tertiary_node_id]:
        if not peer_id:
            continue
        try:
            sec = db.get_storage_node_by_id(peer_id)
        except KeyError:
            continue

        if sec.status == StorageNode.STATUS_ONLINE:
            sec_nodes.append(sec)
        elif sec.status == StorageNode.STATUS_OFFLINE:
            continue
        else:
            return [], (
                f"Target secondary node {peer_id} is in state "
                f"'{sec.status}'; cannot create on target primary"
            )
    return sec_nodes, None



def _expose_lvol_on_secondary(lvol, tgt_node, tgt_sec, sec_rpc, tgt_blobid, tgt_lvol_uuid):
    """
    Register the migrated lvol on the target secondary and expose it via
    the same NVMe-oF NQN (with non-optimized ANA state).

    ``tgt_blobid`` and ``tgt_lvol_uuid`` come from querying the target primary
    after final migration (not from the DB record, which still reflects source).

    This mirrors what add_lvol_on_node() does for is_primary=False.
    """
    # The lvol blob lives in the target primary's lvstore; the secondary mirrors
    # that same lvstore, so bdev_lvol_register must reference tgt_node.lvstore,
    # not the secondary's own lvstore name (which is a different string).
    ret = sec_rpc.bdev_lvol_register(
        lvol.lvol_bdev, tgt_node.lvstore, tgt_lvol_uuid, tgt_blobid,
        lvol.lvol_priority_class)
    if not ret:
        return False, f"bdev_lvol_register failed on secondary {tgt_sec.get_id()}"

    # Create subsystem with same NQN only if it doesn't already exist on secondary.
    # Multiple volumes may share the same subsystem (namespace sharing group); a
    # prior migration of a sibling volume may have already created it.
    existing_sec_sub = sec_rpc.subsystem_list(lvol.nqn)
    if not existing_sec_sub:
        ret = sec_rpc.subsystem_create(
            lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid=1000,
            max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS)
        if not ret:
            logger.warning(f"subsystem_create on secondary may already exist: {lvol.nqn}")

        # Add listeners on each data NIC (non-optimized ANA since secondary is not primary)
        for iface in tgt_sec.data_nics:
            if iface.ip4_address and lvol.fabric == iface.trtype.lower():
                ret, err = sec_rpc.nvmf_subsystem_add_listener(
                    lvol.nqn, iface.trtype, iface.ip4_address,
                    tgt_sec.get_lvol_subsys_port(tgt_node.lvstore), "non_optimized")
                if not ret:
                    if err and isinstance(err, dict) and err.get("code") == -32602:
                        logger.warning("Listener already exists on secondary")
                    else:
                        logger.warning(
                            f"Failed to add listener on secondary {tgt_sec.get_id()}: {err}")
    else:
        logger.info(
            f"Subsystem {lvol.nqn} already exists on secondary {tgt_sec.get_id()}; "
            "attaching namespace only")

    # Add namespace using the target primary's lvstore for the composite bdev name
    top_bdev = f"{tgt_node.lvstore}/{lvol.lvol_bdev}"
    ret = sec_rpc.nvmf_subsystem_add_ns(lvol.nqn, top_bdev, lvol.uuid, lvol.guid)
    if not ret:
        return False, f"nvmf_subsystem_add_ns failed on secondary {tgt_sec.get_id()}"

    return True, None


# ---------------------------------------------------------------------------
# Transfer-context cleanup helpers
# ---------------------------------------------------------------------------

def _cleanup_snap_transfer(src_rpc, tgt_rpc, ctx):
    """Tear down the temporary NVMe-oF plumbing from a snapshot transfer."""
    ctrl_name = ctx.get('ctrl_name')
    temp_nqn = ctx.get('temp_nqn')
    if ctrl_name:
        try:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
        except Exception as e:
            logger.warning(f"detach migration ctrl {ctrl_name}: {e}")
    if temp_nqn:
        try:
            tgt_rpc.subsystem_delete(temp_nqn)
        except Exception as e:
            logger.warning(f"delete migration subsystem {temp_nqn}: {e}")


def _cleanup_final_migration(src_rpc, ctx, tgt_rpc=None, rollback_target=False):
    """Detach the hub controller attached for the final lvol migration.

    When *rollback_target* is True the target lvol and its subsystem/namespace
    are also torn down so that a retry can re-create them from scratch.
    """
    ctrl_name = ctx.get('ctrl_name')
    if ctrl_name:
        try:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
        except Exception as e:
            logger.warning(f"detach hub ctrl {ctrl_name}: {e}")

    if rollback_target and tgt_rpc:
        tgt_composite = ctx.get('tgt_lvol_composite')
        nqn = ctx.get('nqn')
        tgt_ns_id = ctx.get('tgt_ns_id')
        sub_created = ctx.get('subsystem_created_on_target', False)
        if nqn and tgt_ns_id is not None:
            try:
                _cleanup_subsystem_or_ns(nqn, tgt_ns_id, sub_created, tgt_rpc)
            except Exception as e:
                logger.warning(f"cleanup target subsystem {nqn}: {e}")
        if tgt_composite:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite}: {e}")


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------

def _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers):
    """
    Best-effort cleanup of NVMe-oF plumbing for in-flight parallel transfers.

    Only tears down the temporary controller (source) and subsystem (target).
    The target bdevs themselves are left for _handle_cleanup_target to remove
    via the full async-delete sequence.
    """
    for t in transfers:
        if not t.get('post_done'):
            _cleanup_snap_transfer(src_rpc, tgt_rpc, t)


def _setup_snap_transfer(snap, snap_index, migration, src_node, tgt_node,
                         src_rpc, tgt_rpc, trtype, target_ip,
                         tgt_sec=None, sec_rpc=None):
    """
    Prepare a single snapshot for async transfer:
      1. Create writable lvol on target primary
      2. Register on target secondary immediately (keeps secondary consistent)
      3. Set migration flag on primary
      4. Create temp NVMe-oF subsystem + listener + namespace on target
      5. Attach NVMe-oF controller on source
      6. Fire bdev_lvol_transfer (async)

    Returns a transfer-dict on success or (None, error_string) on failure.
    Callers are responsible for rolling back any previously launched transfers.
    """
    snap_uuid = snap.uuid
    snap_short = _snap_short_name(snap)
    src_composite = _snap_composite(src_node.lvstore, snap)
    tgt_composite = f"{tgt_node.lvstore}/{snap_short}"
    temp_nqn = f"nqn.2023-02.io.simplyblock:mig:{migration.uuid[:8]}:{snap_index}"
    ctrl_name = f"mig_{migration.uuid[:8]}_{snap_index}"

    # Step 1: create target lvol on primary
    # Note: SPDK's bdev_lvol_create 'uuid' param is for the lvol *store*, not
    # the new lvol.  Do not pass the snapshot UUID here.
    size_in_mib = _bytes_to_mib(snap.used_size)
    ret = tgt_rpc.create_lvol(snap_short, size_in_mib, tgt_node.lvstore)
    if not ret:
        return None, f"Failed to create target lvol for snap {snap_uuid}"

    # Step 2: register on secondary immediately so secondary stays consistent
    if tgt_sec and sec_rpc:
        bdev_info = tgt_rpc.get_bdevs(tgt_composite)
        if bdev_info:
            snap_blobid = bdev_info[0]['driver_specific']['lvol']['blobid']
            snap_uuid_on_tgt = bdev_info[0]['uuid']
            ret_sec = sec_rpc.bdev_lvol_register(
                snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
                snap.lvol.lvol_priority_class if hasattr(snap, 'lvol') else 0)
            if not ret_sec:
                logger.warning(
                    f"bdev_lvol_register on secondary failed for snap {snap_uuid}; continuing")
        else:
            logger.warning(f"Could not get bdev info for {tgt_composite} to register on secondary")

    # Step 3: migration flag
    ret = tgt_rpc.bdev_lvol_set_migration_flag(tgt_composite)
    if not ret:
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"bdev_lvol_set_migration_flag failed for snap {snap_uuid}"

    # Step 3: expose via temp NVMe-oF subsystem
    serial = f"SBMIG{snap_uuid[:10].upper().replace('-', '')}"
    ret = tgt_rpc.subsystem_create(temp_nqn, serial, "SimplyBlock Migration")
    if not ret:
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"Failed to create migration subsystem for snap {snap_uuid}"

    tgt_lvs_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)
    ret = tgt_rpc.listeners_create(temp_nqn, trtype, target_ip, tgt_lvs_port)
    if not ret:
        tgt_rpc.subsystem_delete(temp_nqn)
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"Failed to create migration listener for snap {snap_uuid}"

    ret = tgt_rpc.nvmf_subsystem_add_ns(temp_nqn, tgt_composite)
    if not ret:
        tgt_rpc.subsystem_delete(temp_nqn)
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"Failed to add ns to migration subsystem for snap {snap_uuid}"

    # Step 4: connect source to target
    ret = src_rpc.bdev_nvme_attach_controller(
        ctrl_name, temp_nqn, target_ip, tgt_lvs_port, trtype)
    if not ret:
        tgt_rpc.subsystem_delete(temp_nqn)
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"Failed to attach migration controller for snap {snap_uuid}"

    # Step 5: fire async transfer
    remote_bdev = f"{ctrl_name}n1"
    ret = src_rpc.bdev_lvol_transfer(src_composite, 0, 16, remote_bdev, "migrate")
    if ret is None:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
        tgt_rpc.subsystem_delete(temp_nqn)
        _delete_bdev_blocking(tgt_composite, tgt_rpc)
        return None, f"bdev_lvol_transfer failed for snap {snap_uuid}"

    return {
        'snap_uuid': snap_uuid,
        'snap_short': snap_short,
        'snap_index': snap_index,
        'temp_nqn': temp_nqn,
        'ctrl_name': ctrl_name,
        'transfer_done': False,
        'post_done': False,
    }, None


def _post_process_snap(snap, tgt_node, tgt_rpc, src_rpc, migration, t,
                       tgt_sec=None, sec_rpc=None):
    """
    Post-transfer steps for a single snapshot whose data has been fully copied:
      add_clone → convert (on primary, then mirrored on secondary) → cleanup.

    Mutates ``migration.snaps_migrated`` and fires migration events on success.
    Returns (ok: bool, error: str|None).
    """
    snap_uuid = snap.uuid
    snap_short = t['snap_short']
    tgt_composite = f"{tgt_node.lvstore}/{snap_short}"

    # Link to predecessor snapshot in target's ancestry chain (primary)
    if migration.snaps_migrated:
        pred_uuid = migration.snaps_migrated[-1]
        try:
            pred_snap = db.get_snapshot_by_id(pred_uuid)
            pred_composite = _snap_composite(tgt_node.lvstore, pred_snap)
            ret = tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
            if not ret:
                return False, f"bdev_lvol_add_clone failed for {snap_uuid}"
            # Mirror add_clone on secondary
            if tgt_sec and sec_rpc:
                sec_pred_composite = _snap_composite(tgt_node.lvstore, pred_snap)
                ret_sec = sec_rpc.bdev_lvol_add_clone(tgt_composite, sec_pred_composite)
                if not ret_sec:
                    logger.warning(f"bdev_lvol_add_clone on secondary failed for {snap_uuid}")
        except KeyError:
            logger.warning(f"Predecessor snap {pred_uuid} not found; skipping add_clone")

    # Convert writable lvol → immutable snapshot (primary)
    ret = tgt_rpc.bdev_lvol_convert(tgt_composite)
    if not ret:
        return False, f"bdev_lvol_convert failed for {snap_uuid}"

    # Mirror convert on secondary
    if tgt_sec and sec_rpc:
        ret_sec = sec_rpc.bdev_lvol_convert(tgt_composite)
        if not ret_sec:
            logger.warning(f"bdev_lvol_convert on secondary failed for {snap_uuid}")

    # Cleanup temp NVMe-oF plumbing for this snapshot
    _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
    migration.snaps_migrated.append(snap_uuid)
    migration_events.migration_snap_copied(migration, snap_uuid)
    logger.info(f"Snapshot {snap_uuid} migrated successfully")
    return True, None


def _handle_snap_copy(migration, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Drive the SNAP_COPY phase.

    Planned snapshots (snap_migration_plan)
    ---------------------------------------
    All planned snapshots whose transfers are not yet in progress are set up
    and launched in a tight back-to-back loop within a single invocation.
    The function then returns ``(False, False, None)`` and the caller comes
    back on the next service-loop tick to poll for completion.

    On each subsequent call the function polls all in-flight transfers and
    performs post-processing (add_clone → convert → register on secondary)
    for each that has completed, in snapshot-index order (required by the
    add_clone ancestry chain constraint).  As long as at least one transfer
    is still in-flight the function returns ``(False, False, None)`` again.

    Intermediate ("shrink") snapshots
    ----------------------------------
    After all planned snapshots have been processed, up to
    ``max_intermediate_snap_rounds`` additional snapshots are taken from the
    live lvol and transferred one at a time with a tight busy-poll
    (``_INTERMEDIATE_POLL_INTERVAL_S`` between stat checks).  This avoids any
    service-loop latency between the last shrink snapshot completing and the
    start of PHASE_LVOL_MIGRATE.

    Idempotency / crash recovery
    ----------------------------
    The full transfer-context list is written to FDB ONCE after all RPCs have
    been fired successfully.  On restart:
      - Transfers that are "In progress" are detected via bdev_lvol_transfer_stat
        and re-joined without issuing a second RPC.
      - Transfers whose bdev exists on the target but whose stat shows no process
        (runner crashed mid-setup before the RPC) are pre-cleaned and restarted.
      - Transfers already in snaps_migrated are skipped.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    plan = migration.snap_migration_plan
    trtype, target_ip = _get_migration_nic(tgt_node)
    ctx = migration.transfer_context or {}

    # ── A. Launch / resume planned snapshots one at a time ───────────────────
    # SPDK only supports one bdev_lvol_transfer per poller group at a time;
    # launching multiple causes "poller already exists" and stuck transfers.
    _PARALLEL_BATCH = 1
    if ctx.get('stage') != 'parallel_transfer':
        all_unprocessed = [u for u in plan if u not in migration.snaps_migrated]
        unprocessed = all_unprocessed[:_PARALLEL_BATCH]

        if unprocessed:
            # HA secondary gate – check once; all snaps belong to the same volume
            tgt_sec = None
            sec_rpc = None
            for snap_uuid in unprocessed:
                try:
                    snap = db.get_snapshot_by_id(snap_uuid)
                except KeyError:
                    return False, True, f"Snapshot {snap_uuid} not found in DB"
                if snap.lvol.ha_type == "ha":
                    tgt_sec, sec_err = _get_target_secondary_node(tgt_node)
                    if sec_err:
                        migration.error_message = sec_err
                        migration.write_to_db(db.kv_store)
                        return False, True, _WAIT
                    if tgt_sec:
                        sec_rpc = _make_rpc(tgt_sec)
                    break  # one check is enough

            transfers: list[dict] = []
            for snap_uuid in unprocessed:
                snap_index = plan.index(snap_uuid)
                try:
                    snap = db.get_snapshot_by_id(snap_uuid)
                except KeyError:
                    _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                    return False, True, f"Snapshot {snap_uuid} not found in DB"

                snap_short = _snap_short_name(snap)
                src_composite = _snap_composite(src_node.lvstore, snap)
                tgt_composite = f"{tgt_node.lvstore}/{snap_short}"
                temp_nqn = (f"nqn.2023-02.io.simplyblock:mig:"
                            f"{migration.uuid[:8]}:{snap_index}")
                ctrl_name = f"mig_{migration.uuid[:8]}_{snap_index}"

                # Idempotency: transfer already running from a previous crashed run
                existing_stat = src_rpc.bdev_lvol_transfer_stat(src_composite)
                if (existing_stat is not None
                        and existing_stat.get('transfer_state') == 'In progress'):
                    logger.info(
                        f"Resuming in-progress transfer for snap {snap_uuid}")
                    transfers.append({
                        'snap_uuid': snap_uuid,
                        'snap_short': snap_short,
                        'snap_index': snap_index,
                        'temp_nqn': temp_nqn,
                        'ctrl_name': ctrl_name,
                        'transfer_done': False,
                        'post_done': False,
                    })
                    continue

                # Pre-existence: already on target from a sibling migration
                if tgt_rpc.get_bdevs(tgt_composite):
                    logger.info(
                        f"Snapshot {snap_uuid} already on target; skipping transfer")
                    migration.snaps_preexisting_on_target.append(snap_uuid)
                    migration.snaps_migrated.append(snap_uuid)
                    continue

                # Pre-cleanup: remove leftover from a previous failed attempt
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc)
                except Exception:
                    pass

                t, err = _setup_snap_transfer(
                    snap, snap_index, migration, src_node, tgt_node,
                    src_rpc, tgt_rpc, trtype, target_ip,
                    tgt_sec=tgt_sec, sec_rpc=sec_rpc)
                if t is None:
                    _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                    return False, True, err

                transfers.append(t)
                logger.info(
                    f"Started transfer: snap {snap_uuid} "
                    f"({src_composite} → {tgt_composite})")

            if transfers:
                migration.next_snap_index = len(plan)
                migration.transfer_context = {
                    'stage': 'parallel_transfer',
                    'transfers': transfers,
                }
                migration.write_to_db(db.kv_store)
                ctx = migration.transfer_context
                # Return now; poll for completion on next service-loop tick.
                return False, False, None

            # All unprocessed snaps were pre-existing → fall through to
            # intermediate snaps below.
            migration.next_snap_index = len(plan)
            migration.write_to_db(db.kv_store)

    # ── B. Poll all in-flight transfers; post-process completed ones ──────────
    if ctx.get('stage') == 'parallel_transfer':
        transfers = ctx['transfers']
        # Resolve secondary once for the whole poll pass
        tgt_sec, _sec_err = _get_target_secondary_node(tgt_node)
        sec_rpc = _make_rpc(tgt_sec) if tgt_sec and not _sec_err else None
        # Process in snap_index order: add_clone requires predecessor to be
        # converted first.  prev_post_done tracks whether the predecessor has
        # been post-processed; if not, we must not post-process the current snap
        # either (even if its transfer is done).
        prev_post_done = True
        all_done = True

        for t in sorted(transfers, key=lambda x: x['snap_index']):
            if t['post_done']:
                continue

            snap_uuid = t['snap_uuid']
            try:
                snap = db.get_snapshot_by_id(snap_uuid)
            except KeyError:
                _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                migration.transfer_context = {}
                migration.write_to_db(db.kv_store)
                return False, True, f"Snapshot {snap_uuid} disappeared during transfer"

            src_composite = _snap_composite(src_node.lvstore, snap)

            # Update transfer-done status for this entry
            if not t['transfer_done']:
                result = src_rpc.bdev_lvol_transfer_stat(src_composite)
                if result is None:
                    _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, (
                        f"bdev_lvol_transfer_stat returned None for {snap_uuid}")

                state = result.get('transfer_state', 'No process')
                if state == 'In progress':
                    # Still running; can't post-process this or any subsequent snap.
                    all_done = False
                    prev_post_done = False
                    continue
                if state in ('Failed', 'No process'):
                    _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, f"Snapshot transfer {state} for {snap_uuid}"

                t['transfer_done'] = True

            # Transfer done.  Post-process only if predecessor is also done.
            if not prev_post_done:
                all_done = False
                continue

            ok, err = _post_process_snap(
                snap, tgt_node, tgt_rpc, src_rpc, migration, t,
                tgt_sec=tgt_sec, sec_rpc=sec_rpc)
            if not ok:
                _rollback_parallel_transfers(src_rpc, tgt_rpc, transfers)
                migration.transfer_context = {}
                if err is _WAIT:
                    migration.error_message = (
                        f"Secondary node not ready during post-process of {snap_uuid}")
                    migration.write_to_db(db.kv_store)
                    return False, True, _WAIT
                migration.write_to_db(db.kv_store)
                return False, True, err

            t['post_done'] = True
            prev_post_done = True
            # Persist incremental progress so a crash here doesn't re-do work.
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)

        if not all_done:
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
            return False, False, None

        # All parallel transfers in this batch complete
        migration.transfer_context = {}
        migration.write_to_db(db.kv_store)
        ctx = {}

        # If there are more unprocessed snaps, return now so the next tick
        # launches the next batch.
        remaining = [u for u in plan if u not in migration.snaps_migrated]
        if remaining:
            return False, False, None

    # ── C. Intermediate ("shrink") snapshots – busy-poll within this call ────
    # These snapshots capture only the delta written since the last planned snap.
    # They should be small and complete quickly; we spin rather than returning to
    # the service loop so that LVOL_MIGRATE starts with minimal latency.
    while migration.intermediate_snap_rounds < migration.max_intermediate_snap_rounds:
        _take_intermediate_snapshot(migration)
        plan = migration.snap_migration_plan
        snap_uuid = plan[-1]
        snap_index = len(plan) - 1

        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            return False, True, f"Intermediate snapshot {snap_uuid} not found in DB"

        if snap.lvol.ha_type == "ha":
            _, sec_err = _get_target_secondary_node(tgt_node)
            if sec_err:
                migration.error_message = sec_err
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT

        snap_short = _snap_short_name(snap)
        src_composite = _snap_composite(src_node.lvstore, snap)
        tgt_composite = f"{tgt_node.lvstore}/{snap_short}"

        # Pre-cleanup
        try:
            _delete_bdev_blocking(tgt_composite, tgt_rpc)
        except Exception:
            pass

        t, err = _setup_snap_transfer(
            snap, snap_index, migration, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype, target_ip)
        if t is None:
            return False, True, err

        logger.info(
            f"Started intermediate snap transfer: {snap_uuid} "
            f"({src_composite} → {tgt_composite})")

        # Busy-poll: spin at _INTERMEDIATE_POLL_INTERVAL_S until done or timeout
        for _poll_i in range(_INTERMEDIATE_POLL_MAX):
            result = src_rpc.bdev_lvol_transfer_stat(src_composite)
            if result is None:
                _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
                _delete_bdev_blocking(tgt_composite, tgt_rpc)
                return False, True, (
                    f"Transfer stat failed for intermediate snap {snap_uuid}")
            state = result.get('transfer_state', 'No process')
            if state == 'Done':
                break
            if state in ('Failed', 'No process'):
                _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
                _delete_bdev_blocking(tgt_composite, tgt_rpc)
                return False, True, (
                    f"Intermediate snap transfer {state} for {snap_uuid}")
            time.sleep(_INTERMEDIATE_POLL_INTERVAL_S)
        else:
            _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
            _delete_bdev_blocking(tgt_composite, tgt_rpc)
            return False, True, (
                f"Intermediate snap transfer timed out for {snap_uuid}")

        ok, err = _post_process_snap(
            snap, tgt_node, tgt_rpc, src_rpc, migration, t)
        if not ok:
            if err is _WAIT:
                migration.error_message = (
                    f"Secondary node not ready after intermediate snap {snap_uuid}")
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT
            return False, True, err

        migration.next_snap_index = len(plan)
        migration.write_to_db(db.kv_store)
        logger.info(f"Intermediate snapshot {snap_uuid} migrated successfully")

    return True, False, None  # SNAP_COPY phase complete


def _take_intermediate_snapshot(migration):
    """
    Take an additional "shrink" snapshot from the live lvol on the source node
    to reduce the delta that must be frozen during PHASE_LVOL_MIGRATE.
    """
    snap_name = f"_mig_{migration.uuid[:8]}_r{migration.intermediate_snap_rounds}"
    snap_uuid, err = snapshot_controller.add(migration.lvol_id, snap_name)
    if err:
        logger.warning(f"Intermediate snapshot failed (proceeding without): {err}")
        migration.intermediate_snap_rounds = migration.max_intermediate_snap_rounds
        migration.write_to_db(db.kv_store)
        return

    migration.intermediate_snaps.append(snap_uuid)
    migration.snap_migration_plan.append(snap_uuid)
    migration.intermediate_snap_rounds += 1
    migration.write_to_db(db.kv_store)
    logger.info(
        f"Intermediate snapshot taken: {snap_name} "
        f"(round {migration.intermediate_snap_rounds}/{migration.max_intermediate_snap_rounds})"
    )


def _handle_lvol_migrate(migration, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Drive the LVOL_MIGRATE phase.

    Creates the target lvol with the same NQN as the source subsystem, connects
    the source to the target's hub lvol, and issues bdev_lvol_final_migration
    (async).  Polls until Done, then registers the lvol on the target secondary
    (if applicable) and updates ANA states.

    Note: apply_migration_to_db() is NOT called here; it is deferred to the end
    of PHASE_CLEANUP_SOURCE after source snap deletion is complete.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
    except KeyError as e:
        return False, True, str(e)

    trtype, target_ip = _get_migration_nic(tgt_node)
    src_lvol_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"
    tgt_lvol_composite = f"{tgt_node.lvstore}/{lvol.lvol_bdev}"
    ctx = migration.transfer_context or {}

    # --- Poll in-progress final migration ---
    if ctx.get('stage') == 'transfer':
        result = src_rpc.bdev_lvol_transfer_stat(src_lvol_composite)
        if result is None:
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True)
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, "bdev_lvol_transfer_stat returned None for final migration"

        state = result.get('transfer_state', 'No process')

        if state == 'In progress':
            return False, False, None

        if state in ('Failed', 'No process'):
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True)
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, f"Final migration {state}"

        # state == 'Done' ─ register on secondary and update ANA states
        _cleanup_final_migration(src_rpc, ctx)
        migration.current_job_id = ""
        migration.write_to_db(db.kv_store)

        # Register lvol on target secondary (if ha type and secondary is online)
        if lvol.ha_type == "ha":
            tgt_sec, sec_err = _get_target_secondary_node(tgt_node)
            if sec_err:
                migration.error_message = sec_err
                migration.transfer_context = {}
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT
            if tgt_sec is not None:
                # Re-query target to get the final blobid and SPDK uuid
                lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore)
                tgt_blobid = None
                tgt_lvol_uuid = lvol.lvol_uuid
                if lvols_list:
                    for entry in lvols_list:
                        entry_name = entry.get('name', '') or entry.get('lvol_name', '')
                        if entry_name in (lvol.lvol_bdev, tgt_lvol_composite):
                            tgt_blobid = entry.get('blobid')
                            tgt_lvol_uuid = entry.get('uuid', lvol.lvol_uuid)
                            break
                if tgt_blobid is None:
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, (
                        "Cannot get blobid from target for secondary registration")
                sec_rpc = _make_rpc(tgt_sec)
                ok, err = _expose_lvol_on_secondary(
                    lvol, tgt_node, tgt_sec, sec_rpc, tgt_blobid, tgt_lvol_uuid)
                if not ok:
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, err
                logger.info(
                    f"Lvol {lvol.uuid} registered and exposed on secondary {tgt_sec.get_id()}")

        migration.transfer_context = {}

        _update_ana_states(migration, src_node, tgt_node, src_rpc, tgt_rpc)

        # apply_migration_to_db is intentionally deferred to CLEANUP_SOURCE
        return True, False, None

    # --- Gate: check target secondary state before creating on target primary ---
    if lvol.ha_type == "ha":
        _, sec_err = _get_target_secondary_node(tgt_node)
        if sec_err:
            migration.error_message = sec_err
            migration.write_to_db(db.kv_store)
            return False, True, _WAIT

    # --- Start the final migration ---

    # Step 1: create writable target lvol (size in MiB)
    # Note: SPDK's bdev_lvol_create 'uuid' param is for the lvol *store*, not
    # the new lvol.  Do not pass the lvol UUID here.
    ret = tgt_rpc.create_lvol(lvol.lvol_bdev, _bytes_to_mib(lvol.size), tgt_node.lvstore)
    if not ret:
        return False, True, f"Failed to create target lvol {tgt_lvol_composite}"
    logger.info(f"[MIGRATION SIZE CHECK] lvol={lvol.lvol_bdev} source_size_bytes={lvol.size} target_size_mib={_bytes_to_mib(lvol.size)}")

    ret = tgt_rpc.bdev_lvol_set_migration_flag(tgt_lvol_composite)
    if not ret:
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, f"bdev_lvol_set_migration_flag failed for target lvol {tgt_lvol_composite}"

    # Step 1b: expose via NVMe-oF using the SOURCE's NQN (clients follow same NQN)
    # Multiple volumes may share the same subsystem (namespace sharing group):
    # if the subsystem already exists on target (placed there by a prior migration
    # of a sibling volume), just add a new namespace to it.
    nqn = lvol.nqn
    existing_sub = tgt_rpc.subsystem_list(nqn)
    subsystem_created_on_target = False

    if not existing_sub:
        # First volume from this namespace group to arrive on target
        serial = lvol.uuid[:20].upper().replace('-', '')
        ret = tgt_rpc.subsystem_create(nqn, serial, "SimplyBlock")
        if not ret:
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
            return False, True, f"Failed to create subsystem {nqn} on target"
        subsystem_created_on_target = True

        ret = tgt_rpc.listeners_create(nqn, trtype, target_ip, tgt_node.get_lvol_subsys_port(tgt_node.lvstore))
        if not ret:
            tgt_rpc.subsystem_delete(nqn)
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
            return False, True, "Failed to create listener on target"
    else:
        logger.info(
            f"Subsystem {nqn} already exists on target; attaching namespace only")

    ns_ret = tgt_rpc.nvmf_subsystem_add_ns(nqn, tgt_lvol_composite, uuid=lvol.uuid)
    if not ns_ret:
        if subsystem_created_on_target:
            tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, "Failed to add namespace to target subsystem"
    tgt_ns_id = int(ns_ret) if ns_ret else lvol.ns_id

    # Step 2: get blobid of the newly created target lvol (passed as lvol_id to bdev_lvol_final_migration)
    lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore)
    if not lvols_list:
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, "bdev_lvol_get_lvols returned empty result from target"

    tgt_map_id = None
    tgt_lvol_uuid = lvol.lvol_uuid  # fallback to source UUID
    for entry in lvols_list:
        entry_name = entry.get('name', '') or entry.get('lvol_name', '')
        if entry_name in (lvol.lvol_bdev, tgt_lvol_composite):
            tgt_map_id = entry.get('map_id')
            tgt_lvol_uuid = entry.get('uuid', lvol.lvol_uuid)
            break

    if tgt_map_id is None:
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, f"Could not find map_id for {lvol.lvol_bdev} on target"

    # Step 3: connect source to target hub lvol
    ctrl_name = f"mighub_{migration.uuid[:8]}"
    hub_nqn = tgt_node.hublvol.nqn
    hub_port = tgt_node.hublvol.nvmf_port
    ret = src_rpc.bdev_nvme_attach_controller(ctrl_name, hub_nqn, target_ip, hub_port, trtype)
    if not ret:
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, "Failed to connect source to target hub"

    hub_bdev = f"{ctrl_name}n1"

    # Step 4: locate the last migrated snapshot's composite name on the source
    if not migration.snaps_migrated:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, "No snapshots migrated; cannot perform final migration"

    last_snap_uuid = migration.snaps_migrated[-1]
    try:
        last_snap = db.get_snapshot_by_id(last_snap_uuid)
    except KeyError:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, f"Last snapshot {last_snap_uuid} not found"

    tgt_snap_composite = _snap_composite(tgt_node.lvstore, last_snap)

    # Step 5: start final migration (async) – I/O is frozen for the small delta
    ret = src_rpc.bdev_lvol_final_migration(
        src_lvol_composite, tgt_map_id, tgt_snap_composite, 2, hub_bdev)
    if ret is None:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
        tgt_rpc.subsystem_delete(nqn)
        _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
        return False, True, "bdev_lvol_final_migration failed to start"

    migration.transfer_context = {
        'stage': 'transfer',
        'ctrl_name': ctrl_name,
        'nqn': nqn,
        'tgt_lvol_composite': tgt_lvol_composite,
        'tgt_ns_id': tgt_ns_id,
        'subsystem_created_on_target': subsystem_created_on_target,
    }
    migration.write_to_db(db.kv_store)
    logger.info(f"Started final migration: lvol={lvol.uuid} map_id={tgt_map_id}")
    return False, False, None


def _update_ana_states(migration, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    After a successful migration set ANA states so clients follow the volume:
      target listener → optimized   (new primary)
      source listener → inaccessible (stale; will be cleaned up)
    """
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
        nqn = lvol.nqn
        tgt_trtype, tgt_ip = _get_migration_nic(tgt_node)
        src_trtype, src_ip = _get_migration_nic(src_node)

        tgt_rpc.nvmf_subsystem_listener_set_ana_state(
            nqn, tgt_ip, tgt_node.get_lvol_subsys_port(tgt_node.lvstore), trtype=tgt_trtype, ana="optimized")
        logger.info(f"ANA: {nqn} on target {tgt_ip} → optimized")

        src_rpc.nvmf_subsystem_listener_set_ana_state(
            nqn, src_ip, src_node.get_lvol_subsys_port(src_node.lvstore), trtype=src_trtype, ana="inaccessible")
        logger.info(f"ANA: {nqn} on source {src_ip} → inaccessible")
    except Exception as e:
        logger.error(f"ANA state update error (non-fatal): {e}")


def _cleanup_subsystem_or_ns(nqn, ns_id, subsystem_was_created_by_migration, rpc):
    """
    Remove a volume's namespace from an NVMe-oF subsystem, deleting the
    subsystem entirely only when no other namespaces remain AND we originally
    created the subsystem (i.e. it wasn't pre-existing from a sibling volume).

    If ``subsystem_was_created_by_migration`` is False the subsystem was already
    present before we attached our namespace, so we never delete it—we only
    remove our namespace entry.
    """
    sub_list = rpc.subsystem_list(nqn)
    if not sub_list:
        return  # already gone

    sub = sub_list[0] if isinstance(sub_list, list) else sub_list
    ns_count = len(sub.get('namespaces', []))

    if ns_count > 1 or not subsystem_was_created_by_migration:
        # Other namespaces still alive or we didn't create the subsystem:
        # remove only our namespace entry.
        if ns_id:
            rpc.nvmf_subsystem_remove_ns(nqn, ns_id)
        else:
            logger.warning(
                f"Cannot remove namespace from {nqn}: ns_id unknown; skipping")
    else:
        # We're the sole namespace and we created the subsystem – delete it.
        rpc.subsystem_delete(nqn)


def _get_secondary_rpc(node):
    """Return RPC clients for node's online secondaries."""
    if not node.secondary_node_id:
        return None
    try:
        sec = db.get_storage_node_by_id(node.secondary_node_id)
        if sec.status == StorageNode.STATUS_ONLINE:
            return _make_rpc(sec)
    except KeyError:
        pass
    return None


def _get_all_secondary_rpcs(node):
    """Return list of RPC clients for all online secondaries of node."""
    rpcs = []
    for peer_id in [node.secondary_node_id, node.tertiary_node_id]:
        if not peer_id:
            continue
        try:
            sec = db.get_storage_node_by_id(peer_id)
            if sec.status == StorageNode.STATUS_ONLINE:
                rpcs.append(_make_rpc(sec))
        except KeyError:
            pass
    return rpcs


def _handle_cleanup_source(migration, src_node, src_rpc):
    """
    Delete snapshots from the source node that are exclusively owned by the
    migrated volume, using a non-blocking async state machine so the task
    runner is never blocked.

    State machine (tracked in migration.transfer_context):

      stage='cleanup_src'  pending=[...] current_bdev=None
        → pop first pending bdev, call delete_lvol(sync=False), set current_bdev
        → return (False, False, None)  ← come back next iteration to poll

      stage='cleanup_src'  current_bdev=<name>
        → poll bdev_lvol_get_lvol_delete_status
        → if in-progress: return (False, False, None)
        → if done/not-found: finalize on primary + secondary, clear current_bdev
        → continue with next pending item in the same invocation

      When pending is empty and no current_bdev:
        → cleanup source subsystem/ns, apply_migration_to_db, return (True,…)

    This avoids snapshot_controller.delete() clone-check soft-delete behaviour.
    apply_migration_to_db() is called AFTER all deletes complete.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    ctx = migration.transfer_context or {}

    # --- First entry: initialize cleanup state ---
    if ctx.get('stage') != 'cleanup_src':
        to_delete = migration_controller.get_snaps_safe_to_delete_on_source(migration)
        ctx = {'stage': 'cleanup_src', 'pending': list(to_delete), 'current_bdev': None}
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    src_sec_rpc = _get_secondary_rpc(src_node)

    # --- Poll a currently running async delete ---
    if ctx.get('current_bdev'):
        bdev_name = ctx['current_bdev']
        status = src_rpc.bdev_lvol_get_lvol_delete_status(bdev_name)
        if status == 1:
            return False, False, None  # still in progress
        if status in (0, 2):
            src_rpc.delete_lvol(bdev_name, del_async=True)
            if src_sec_rpc:
                src_sec_rpc.delete_lvol(bdev_name, del_async=True)
            logger.info(f"Deleted source bdev {bdev_name}")
            ctx['current_bdev'] = None
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
        else:
            return False, False, f"delete_status returned {status} for {bdev_name}"

    # --- Start the next pending delete ---
    while ctx['pending']:
        snap_uuid = ctx['pending'].pop(0)
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            snap_short = _snap_short_name(snap)
            bdev_name = f"{src_node.lvstore}/{snap_short}"
            ret, _ = src_rpc.delete_lvol(bdev_name)
            if not ret:
                return False, False, f"delete_lvol async start failed for {bdev_name}"
            ctx['current_bdev'] = bdev_name
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
            return False, False, None  # poll on next call
        except KeyError:
            logger.warning(f"Source snapshot {snap_uuid} not found in DB; skipping")

    # --- All deletes finished: cleanup source NVMe-oF exposure ---
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
        _cleanup_subsystem_or_ns(lvol.nqn, lvol.ns_id, True, src_rpc)
        if src_sec_rpc:
            _cleanup_subsystem_or_ns(lvol.nqn, lvol.ns_id, True, src_sec_rpc)
    except Exception as e:
        logger.warning(f"Source subsystem cleanup failed (non-fatal): {e}")

    migration.transfer_context = {}
    if not migration_controller.apply_migration_to_db(migration):
        return False, False, "Failed to update DB records after source cleanup"

    return True, False, None


def _handle_cleanup_target(migration, tgt_node, tgt_rpc):
    """
    Roll back a failed or cancelled migration: remove snapshots copied to the
    target and any partially-created target lvol/subsystem, using a non-blocking
    async state machine identical in structure to _handle_cleanup_source.

    State machine stages in migration.transfer_context:

      stage='cleanup_tgt_lvol'  (optional first step: delete the target lvol
         created by LVOL_MIGRATE if the failure happened after it was set up)

      stage='cleanup_tgt'  pending=[...] current_bdev=None / <name>
         (same polling pattern as cleanup_src)

    Returns (done: bool, suspend: bool, error: str|None).
    """
    ctx = migration.transfer_context or {}
    tgt_sec_rpc = _get_secondary_rpc(tgt_node)

    # --- Step 0: if LVOL_MIGRATE created the target lvol before failing, delete it first ---
    if ctx.get('stage') not in ('cleanup_tgt', 'cleanup_tgt_lvol'):
        # Detect a dangling target lvol from transfer_context written by LVOL_MIGRATE
        tgt_lvol_composite = ctx.get('tgt_lvol_composite')
        nqn = ctx.get('nqn')
        tgt_ns_id = ctx.get('tgt_ns_id')
        subsystem_created_on_target = ctx.get('subsystem_created_on_target', True)
        if tgt_lvol_composite:
            # Clean up subsystem first (sync), then start async lvol delete
            if nqn:
                try:
                    _cleanup_subsystem_or_ns(nqn, tgt_ns_id, subsystem_created_on_target, tgt_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target subsystem {nqn}: {e}")
                if tgt_sec_rpc:
                    try:
                        _cleanup_subsystem_or_ns(nqn, tgt_ns_id, subsystem_created_on_target,
                                                 tgt_sec_rpc)
                    except Exception as e:
                        logger.warning(f"cleanup target secondary subsystem {nqn}: {e}")
            ret, _ = tgt_rpc.delete_lvol(tgt_lvol_composite)
            if not ret:
                logger.warning(f"delete_lvol async start failed for {tgt_lvol_composite}")
            ctx = {
                'stage': 'cleanup_tgt_lvol',
                'current_bdev': tgt_lvol_composite,
                'sec_rpc_needed': tgt_sec_rpc is not None,
            }
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
            return False, False, None  # poll on next call
        else:
            # No dangling lvol; go straight to snapshot cleanup
            to_delete = migration_controller.get_snaps_to_delete_on_target(migration)
            ctx = {'stage': 'cleanup_tgt', 'pending': list(to_delete), 'current_bdev': None}
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)

    # --- Poll the target lvol delete (cleanup_tgt_lvol stage) ---
    if ctx.get('stage') == 'cleanup_tgt_lvol':
        bdev_name = ctx['current_bdev']
        status = tgt_rpc.bdev_lvol_get_lvol_delete_status(bdev_name)
        if status == 1:
            return False, False, None
        if status in (0, 2):
            tgt_rpc.delete_lvol(bdev_name, del_async=True)
            if tgt_sec_rpc:
                tgt_sec_rpc.delete_lvol(bdev_name, del_async=True)
            logger.info(f"Deleted target lvol {bdev_name}")
        else:
            logger.warning(f"delete_status {status} for {bdev_name}; proceeding anyway")
        # Transition to snapshot cleanup
        to_delete = migration_controller.get_snaps_to_delete_on_target(migration)
        ctx = {'stage': 'cleanup_tgt', 'pending': list(to_delete), 'current_bdev': None}
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    # --- Poll a currently running async snapshot delete ---
    if ctx.get('current_bdev'):
        bdev_name = ctx['current_bdev']
        status = tgt_rpc.bdev_lvol_get_lvol_delete_status(bdev_name)
        if status == 1:
            return False, False, None
        if status in (0, 2):
            tgt_rpc.delete_lvol(bdev_name, del_async=True)
            if tgt_sec_rpc:
                tgt_sec_rpc.delete_lvol(bdev_name, del_async=True)
            logger.info(f"Deleted target snapshot bdev {bdev_name}")
            ctx['current_bdev'] = None
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
        else:
            return False, False, f"delete_status returned {status} for {bdev_name}"

    # --- Start the next pending snapshot delete ---
    while ctx['pending']:
        snap_uuid = ctx['pending'].pop(0)
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            snap_short = _snap_short_name(snap)
            bdev_name = f"{tgt_node.lvstore}/{snap_short}"
            ret, _ = tgt_rpc.delete_lvol(bdev_name)
            if not ret:
                return False, False, f"delete_lvol async start failed for {bdev_name}"
            ctx['current_bdev'] = bdev_name
            migration.transfer_context = ctx
            migration.write_to_db(db.kv_store)
            return False, False, None
        except KeyError:
            logger.warning(f"Target snapshot {snap_uuid} not found in DB; skipping")

    # --- All done ---
    migration.transfer_context = {}
    migration.write_to_db(db.kv_store)
    return True, False, None


# ---------------------------------------------------------------------------
# Main task runner entry point
# ---------------------------------------------------------------------------

def task_runner(task):
    """
    Process one iteration of a FN_LVOL_MIG task.

    Returns True if the task reached a terminal state (done/failed/cancelled),
    False if it should be retried on the next runner loop iteration.
    """
    task = db.get_task_by_id(task.uuid)
    migration_id = task.function_params.get("migration_id")
    if not migration_id:
        _fail_task(task, "task is missing migration_id in function_params")
        return True

    try:
        migration = db.get_migration_by_id(migration_id)
    except KeyError:
        _fail_task(task, f"LVolMigration not found: {migration_id}")
        return True

    # --- Already terminal ---
    if migration.status in (LVolMigration.STATUS_DONE,
                             LVolMigration.STATUS_FAILED,
                             LVolMigration.STATUS_CANCELLED):
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    # --- Cancellation ---
    if migration.canceled or task.canceled:
        if migration.phase not in (LVolMigration.PHASE_CLEANUP_TARGET,
                                   LVolMigration.PHASE_COMPLETED):
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.status = LVolMigration.STATUS_RUNNING
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)

    # --- Deadline ---
    if migration.has_deadline_passed() and migration.is_active():
        if migration.phase not in (LVolMigration.PHASE_CLEANUP_TARGET,):
            logger.warning(f"Migration {migration_id} deadline exceeded; aborting")
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.error_message = "Migration deadline exceeded"
            migration.status = LVolMigration.STATUS_RUNNING
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)

    # --- Load nodes ---
    try:
        src_node = db.get_storage_node_by_id(migration.source_node_id)
    except KeyError:
        return _suspend_task(task, migration, "source node not found")

    try:
        tgt_node = db.get_storage_node_by_id(migration.target_node_id)
    except KeyError:
        return _suspend_task(task, migration, "target node not found")

    # For cleanup_target we only need the target node to be reachable.
    if migration.phase != LVolMigration.PHASE_CLEANUP_TARGET:
        if src_node.status != StorageNode.STATUS_ONLINE:
            return _suspend_task(
                task, migration, f"source node not online (status={src_node.status})")

    if tgt_node.status != StorageNode.STATUS_ONLINE:
        return _suspend_task(
            task, migration, f"target node not online (status={tgt_node.status})")

    # --- Cluster health ---
    cluster = db.get_cluster_by_id(migration.cluster_id)
    if cluster.status not in (Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED):
        return _suspend_task(
            task, migration, f"cluster not active (status={cluster.status})")

    # --- Transition NEW/SUSPENDED → RUNNING ---
    if task.status in (JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED):
        task.status = JobSchedule.STATUS_RUNNING
        migration.status = LVolMigration.STATUS_RUNNING
        task.write_to_db(db.kv_store)
        migration.write_to_db(db.kv_store)

    src_rpc = _make_rpc(src_node)
    tgt_rpc = _make_rpc(tgt_node)

    # --- Phase dispatch ---
    phase = migration.phase
    done = suspend = False
    error = None

    try:
        if phase == LVolMigration.PHASE_SNAP_COPY:
            done, suspend, error = _handle_snap_copy(
                migration, src_node, tgt_node, src_rpc, tgt_rpc)
            next_phase = LVolMigration.PHASE_LVOL_MIGRATE

        elif phase == LVolMigration.PHASE_LVOL_MIGRATE:
            done, suspend, error = _handle_lvol_migrate(
                migration, src_node, tgt_node, src_rpc, tgt_rpc)
            next_phase = LVolMigration.PHASE_CLEANUP_SOURCE

        elif phase == LVolMigration.PHASE_CLEANUP_SOURCE:
            done, suspend, error = _handle_cleanup_source(migration, src_node, src_rpc)
            next_phase = LVolMigration.PHASE_COMPLETED

        elif phase == LVolMigration.PHASE_CLEANUP_TARGET:
            done, suspend, error = _handle_cleanup_target(migration, tgt_node, tgt_rpc)
            next_phase = ""  # terminal failure path

        else:
            _fail_task(task, migration, f"unknown phase: {phase}")
            return True
    except RPCException as exc:
        logger.warning(f"Migration {migration_id} RPC error in phase {phase}: {exc}")
        return _suspend_task(task, migration, str(exc))

    # --- Handle error / suspend ---
    if error is _WAIT:
        # Transient external condition (e.g. secondary node not ready).
        # Suspend without charging against the retry budget.
        return _suspend_task(task, migration, migration.error_message or "waiting")

    if error:
        # Real operation failure – increment retry counter.
        migration.retry_count += 1
        migration.error_message = error
        task.retry += 1
        task.function_result = error

        if migration.retry_count >= migration.max_retries:
            logger.error(
                f"Migration {migration_id} exceeded max retries "
                f"({migration.max_retries}); entering cleanup_target"
            )
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return False  # will re-enter runner for cleanup

        return _suspend_task(task, migration, error)

    if suspend:
        return _suspend_task(task, migration, migration.error_message or "suspended")

    # --- Phase complete: advance ---
    if done:
        if phase == LVolMigration.PHASE_CLEANUP_SOURCE:
            # Happy path terminal state
            migration.phase = LVolMigration.PHASE_COMPLETED
            migration.status = LVolMigration.STATUS_DONE
            migration.completed_at = int(time.time())
            migration.write_to_db(db.kv_store)
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "Migration completed successfully"
            task.write_to_db(db.kv_store)
            tasks_events.task_updated(task)
            migration_events.migration_completed(migration)
            logger.info(f"Migration {migration_id} completed successfully")
            return True

        elif phase == LVolMigration.PHASE_CLEANUP_TARGET:
            # Failure-path terminal state
            migration.status = LVolMigration.STATUS_FAILED if not migration.canceled \
                else LVolMigration.STATUS_CANCELLED
            migration.completed_at = int(time.time())
            migration.write_to_db(db.kv_store)
            task.status = JobSchedule.STATUS_DONE
            task.function_result = migration.error_message or "Migration failed; target cleaned up"
            task.write_to_db(db.kv_store)
            tasks_events.task_updated(task)
            migration_events.migration_failed(migration, migration.error_message)
            logger.info(f"Migration {migration_id} failed; target rolled back")
            return True

        else:
            # Advance to next phase and continue immediately in the same invocation.
            # This avoids the 3-second sleep between phase transitions (e.g. the gap
            # between the last snapshot completing and LVOL_MIGRATE starting).
            assert next_phase is not None
            migration.phase = next_phase
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            logger.info(f"Migration {migration_id} advanced to phase '{next_phase}'")
            return task_runner(task)  # recurse; depth bounded by number of phases

    # Phase still in progress – write any state changes and come back.
    migration.write_to_db(db.kv_store)
    task.write_to_db(db.kv_store)
    return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rpc(node):
    return node.rpc_client(timeout=5, retry=2)


def _suspend_task(task, migration, reason):
    task.status = JobSchedule.STATUS_SUSPENDED
    task.function_result = reason
    task.retry += 1
    task.write_to_db(db.kv_store)
    migration.status = LVolMigration.STATUS_SUSPENDED
    migration.error_message = reason
    migration.write_to_db(db.kv_store)
    logger.warning(f"Migration task suspended: {reason}")
    return False


def _fail_task(task, migration_or_msg, reason=None):
    if reason is None:
        # Called as _fail_task(task, reason_string)
        reason = migration_or_msg
        task.status = JobSchedule.STATUS_DONE
        task.function_result = reason
        task.write_to_db(db.kv_store)
        logger.error(f"Migration task failed: {reason}")
        return True

    migration = migration_or_msg
    migration.status = LVolMigration.STATUS_FAILED
    migration.error_message = reason
    migration.completed_at = int(time.time())
    migration.write_to_db(db.kv_store)
    task.status = JobSchedule.STATUS_DONE
    task.function_result = reason
    task.write_to_db(db.kv_store)
    migration_events.migration_failed(migration, reason)
    logger.error(f"Migration failed permanently: {reason}")
    return True


# ---------------------------------------------------------------------------
# Runner main loop
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting LVol Migration task runner...")

    while True:
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                for task in db.get_active_migration_tasks(cl.get_id()):
                    task_runner(task)

        time.sleep(3)
