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
      5. bdev_lvol_final_migration on source (synchronous — blocks until done)
      6. Rebuild NVMe-oF subsystem on TGT (delete old → create fresh, min_cntlid=2000)
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

import datetime
import time


def _now_ms():
    """Return current wall-clock time as an ISO-8601 string with milliseconds."""
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

from simplyblock_core import db_controller as db_mod, utils, constants
from simplyblock_core.controllers import (
    migration_controller, migration_events, snapshot_controller, tasks_controller, tasks_events
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


# Suffix appended to every bdev created on the target node during migration so
# that we never accidentally operate on a real pre-existing target bdev during
# retry or initial attempts.  Must match _MIGRATION_BDEV_SUFFIX in
# migration_controller.py.
_MIGRATION_BDEV_SUFFIX = 'm'


def _snap_short_name(snap):
    """Return the bare bdev name for a snapshot, stripping any lvstore prefix."""
    path = snap.snap_bdev
    return path.split('/', 1)[1] if '/' in path else path


def _snap_tgt_short_name(snap):
    """Return the migration-target bdev short name for a snapshot.

    Normally the target bdev is named <src_short> + _MIGRATION_BDEV_SUFFIX.
    However, when apply_migration_to_db() is called early (at cutover time),
    snap.snap_bdev is already updated to the target name which already carries the
    suffix.  In that case return it as-is to avoid producing a double suffix (e.g.
    'SNAP_16745mm' instead of 'SNAP_16745m').
    """
    short = _snap_short_name(snap)
    if short.endswith(_MIGRATION_BDEV_SUFFIX):
        return short   # apply_migration_to_db already updated this snap
    return short + _MIGRATION_BDEV_SUFFIX


def _snap_composite(lvstore, snap):
    """SPDK composite bdev name for a snapshot on a given node: ``<lvstore>/<bdev>``."""
    return f"{lvstore}/{_snap_short_name(snap)}"


def _bytes_to_mib(nbytes):
    """Convert bytes to MiB, rounding down (floor).  Returns at least 1.

    Must use floor to match the lvol creation code which also uses floor when
    converting user-specified bytes to size_in_mib.  SPDK's bdev_lvol_create
    then applies its own ceiling at the cluster boundary — if we pass ceil here
    the cluster count on the target ends up one higher than the source, causing
    a 2 MiB capacity change on the client after migration.
    """
    if nbytes <= 0:
        return 1
    return max(1, utils.convert_size(nbytes, 'MiB', round_up=False))


def _log_spdk_bdev_size(rpc, composite_name, label):
    """Query SPDK for *composite_name* and emit a [BDEV SIZE] log line.

    Reports num_blocks × block_size → actual_mib and sectors@512 (the sector
    count the client sees via the NVMe namespace).  Never raises.
    """
    _MIB = 1048576
    try:
        info = rpc.get_bdevs(composite_name)
        if not info:
            logger.warning(
                f"[BDEV SIZE] {label}: {composite_name} — bdev not found in SPDK")
            return None
        b = info[0]
        num_blocks   = b.get('num_blocks', 0)
        block_size   = b.get('block_size', 512)
        actual_bytes = num_blocks * block_size
        actual_mib   = actual_bytes // _MIB
        sectors_512  = num_blocks if block_size == 512 else actual_bytes // 512
        blobid       = b.get('driver_specific', {}).get('lvol', {}).get('blobid', '?')
        logger.info(
            f"[BDEV SIZE] {label}: {composite_name} "
            f"num_blocks={num_blocks} block_size={block_size} "
            f"actual_mib={actual_mib} sectors@512={sectors_512} blobid={blobid}"
        )
        return actual_bytes
    except Exception as exc:
        logger.warning(
            f"[BDEV SIZE] {label}: {composite_name} — query error: {exc}")
        return None


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




def _expose_lvol_on_secondary(lvol, tgt_node, tgt_sec, sec_rpc, tgt_blobid, tgt_lvol_uuid,
                              already_registered=False, tgt_bdev_name=None, min_cntlid=None):
    """
    Expose the migrated lvol on the target secondary via NVMe-oF.

    When ``already_registered=True`` the bdev_lvol_register step is skipped
    (the lvol was registered in the setup section before final migration started,
    so it is already known to SPDK on the secondary).

    ``tgt_bdev_name`` is the actual SPDK bdev short name on the target (may carry
    the migration suffix, e.g. ``LVOL_2882m``).  When omitted, ``lvol.lvol_bdev``
    is used (i.e. the post-DB-update canonical name).

    This mirrors what add_lvol_on_node() does for is_primary=False.
    """
    bdev_name = tgt_bdev_name or lvol.lvol_bdev
    if not already_registered:
        # The lvol blob lives in the target primary's lvstore; the secondary mirrors
        # that same lvstore, so bdev_lvol_register must reference tgt_node.lvstore,
        # not the secondary's own lvstore name (which is a different string).
        ret = sec_rpc.bdev_lvol_register(
            bdev_name, tgt_node.lvstore, tgt_lvol_uuid, tgt_blobid,
            lvol.lvol_priority_class)
        if not ret:
            return False, f"bdev_lvol_register failed on secondary {tgt_sec.get_id()}"

    # Create subsystem with same NQN only if it doesn't already exist on secondary.
    # Multiple volumes may share the same subsystem (namespace sharing group); a
    # prior migration of a sibling volume may have already created it.
    existing_sec_sub = sec_rpc.subsystem_list(lvol.nqn)
    subsystem_created_on_sec = False
    if not existing_sec_sub:
        _sec_min_cntlid = min_cntlid if min_cntlid is not None else 1000
        ret = sec_rpc.subsystem_create(
            lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid=_sec_min_cntlid,
            max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS)
        if not ret:
            sec_rpc.delete_lvol(bdev_name, del_async=True)
            return False, f"subsystem_create on secondary failed: {lvol.nqn}"
        subsystem_created_on_sec = True

        if lvol.allowed_hosts:
            from simplyblock_core.storage_node_ops import _reapply_allowed_hosts
            _reapply_allowed_hosts(lvol, tgt_sec, sec_rpc)

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
                        sec_rpc.subsystem_delete(lvol.nqn)
                        sec_rpc.delete_lvol(bdev_name, del_async=True)
                        return False, (
                            f"Failed to add listener on secondary {tgt_sec.get_id()}: {err}")
    else:
        logger.info(
            f"Subsystem {lvol.nqn} already exists on secondary {tgt_sec.get_id()}; "
            "attaching namespace only")
        # Remove any stale namespace for this lvol from the pre-existing subsystem
        # (same LVS mismatch issue as on the primary: old mirror bdev on a different
        # lvstore would produce zeros for clients still on that nsid).
        try:
            sub_list = sec_rpc.subsystem_list(lvol.nqn)
            if sub_list:
                sub = sub_list[0] if isinstance(sub_list, list) else sub_list
                top_bdev_new = f"{tgt_node.lvstore}/{bdev_name}"
                for ns in sub.get('namespaces', []):
                    ns_bdev = ns.get('bdev_name') or ns.get('name', '')
                    if (ns_bdev.endswith(f"/{bdev_name}")
                            and ns_bdev != top_bdev_new):
                        old_nsid = ns.get('nsid')
                        sec_rpc.nvmf_subsystem_remove_ns(lvol.nqn, old_nsid)
                        logger.info(
                            f"Removed stale secondary namespace nsid={old_nsid} "
                            f"bdev={ns_bdev} from subsystem {lvol.nqn} on "
                            f"secondary {tgt_sec.get_id()}")
        except Exception as e:
            logger.warning(
                f"Could not clean up stale namespace in {lvol.nqn} on secondary "
                f"{tgt_sec.get_id()}: {e}")

    # Add namespace using the target primary's lvstore for the composite bdev name
    top_bdev = f"{tgt_node.lvstore}/{bdev_name}"
    ret = sec_rpc.nvmf_subsystem_add_ns(lvol.nqn, top_bdev, lvol.uuid, lvol.guid)
    if not ret:
        if subsystem_created_on_sec:
            sec_rpc.subsystem_delete(lvol.nqn)
        sec_rpc.delete_lvol(bdev_name, del_async=True)
        return False, f"nvmf_subsystem_add_ns failed on secondary {tgt_sec.get_id()}"

    return True, None


def _update_ana_states(lvol, src_node, tgt_node, src_rpc, tgt_rpc,
                       tgt_sec=None, sec_rpc=None):
    """Flip ANA states at cutover: TGT → optimized/non_optimized, SRC → inaccessible."""
    nqn = lvol.nqn
    tgt_trtype, tgt_ip = _get_migration_nic(tgt_node)
    src_trtype, src_ip = _get_migration_nic(src_node)
    tgt_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)
    src_port = src_node.get_lvol_subsys_port(src_node.lvstore)
    try:
        tgt_rpc.nvmf_subsystem_listener_set_ana_state(
            nqn, tgt_ip, tgt_port, trtype=tgt_trtype, ana="optimized")
        logger.info(f"ANA: {nqn} TGT {tgt_ip}:{tgt_port} → optimized")
    except Exception as e:
        logger.error(f"ANA update TGT failed (non-fatal): {e}")
    try:
        src_rpc.nvmf_subsystem_listener_set_ana_state(
            nqn, src_ip, src_port, trtype=src_trtype, ana="inaccessible")
        logger.info(f"ANA: {nqn} SRC {src_ip}:{src_port} → inaccessible")
    except Exception as e:
        logger.error(f"ANA update SRC failed (non-fatal): {e}")
    if tgt_sec is not None and sec_rpc is not None:
        try:
            tgt_sec_trtype, tgt_sec_ip = _get_migration_nic(tgt_sec)
            tgt_sec_port = tgt_sec.get_lvol_subsys_port(tgt_node.lvstore)
            sec_rpc.nvmf_subsystem_listener_set_ana_state(
                nqn, tgt_sec_ip, tgt_sec_port, trtype=tgt_sec_trtype, ana="non_optimized")
            logger.info(f"ANA: {nqn} TGT-sec {tgt_sec_ip}:{tgt_sec_port} → non_optimized")
        except Exception as e:
            logger.error(f"ANA update TGT-sec failed (non-fatal): {e}")


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
    """Clean up after a final lvol migration attempt.

    On the success path (rollback_target=False) the hub controller is kept
    attached on source — detaching it would drop the migration path before
    clients have switched to the new target path.

    On the rollback path (rollback_target=True) the hub controller IS detached
    and the target lvol/subsystem are torn down so a retry starts clean.
    """
    ctrl_name = ctx.get('ctrl_name')
    if ctrl_name and rollback_target:
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
        if tgt_composite and tgt_rpc.get_bdevs(tgt_composite):
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
    snap_short = _snap_short_name(snap) + _MIGRATION_BDEV_SUFFIX
    src_composite = _snap_composite(src_node.lvstore, snap)
    tgt_composite = f"{tgt_node.lvstore}/{snap_short}"
    temp_nqn = f"nqn.2023-02.io.simplyblock:mig:{migration.uuid[:8]}:{snap_index}"
    ctrl_name = f"mig_{migration.uuid[:8]}_{snap_index}"

    # Step 1: create target lvol on primary
    # Note: SPDK's bdev_lvol_create 'uuid' param is for the lvol *store*, not
    # the new lvol.  Do not pass the snapshot UUID here.
    size_in_mib = _bytes_to_mib(snap.size)
    logger.info(
        f"[SNAP SIZE] snap={snap_uuid[:8]} db snap.size={snap.size} size_in_mib={size_in_mib}"
    )
    _log_spdk_bdev_size(src_rpc, src_composite, f"SRC snap[{snap_uuid[:8]}] pre-create")
    ret = tgt_rpc.create_lvol(snap_short, size_in_mib, tgt_node.lvstore)
    if not ret:
        return None, f"Failed to create target lvol for snap {snap_uuid}"
    _log_spdk_bdev_size(tgt_rpc, tgt_composite, f"TGT snap[{snap_uuid[:8]}] post-create")

    # Step 2: register on secondary immediately so secondary stays consistent.
    # If registration fails we clean up the primary bdev and abort — continuing
    # with an unregistered secondary would leave the cluster in split state.
    sec_registered = False
    if tgt_sec and sec_rpc:
        bdev_info = tgt_rpc.get_bdevs(tgt_composite)
        if not bdev_info:
            _delete_bdev_blocking(tgt_composite, tgt_rpc)
            return None, f"Could not get bdev info for {tgt_composite} after creation"
        snap_blobid = bdev_info[0]['driver_specific']['lvol']['blobid']
        snap_uuid_on_tgt = bdev_info[0]['uuid']
        ret_sec = sec_rpc.bdev_lvol_register(
            snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
            snap.lvol.lvol_priority_class if hasattr(snap, 'lvol') else 0)
        if not ret_sec:
            _delete_bdev_blocking(tgt_composite, tgt_rpc)
            return None, f"bdev_lvol_register on secondary failed for snap {snap_uuid}"
        sec_registered = True

    # Helper: clean both primary and secondary (if registered) atomically
    def _cleanup(subsystem=None):
        if subsystem:
            tgt_rpc.subsystem_delete(subsystem)
        _delete_bdev_blocking(tgt_composite, tgt_rpc,
                              secondary_rpc=sec_rpc if sec_registered else None)

    # Step 3: migration flag on primary
    ret = tgt_rpc.bdev_lvol_set_migration_flag(tgt_composite)
    if not ret:
        _cleanup()
        return None, f"bdev_lvol_set_migration_flag failed for snap {snap_uuid}"

    # Step 4: expose via temp NVMe-oF subsystem
    serial = f"SBMIG{snap_uuid[:10].upper().replace('-', '')}"
    ret = tgt_rpc.subsystem_create(temp_nqn, serial, "SimplyBlock Migration")
    if not ret:
        _cleanup()
        return None, f"Failed to create migration subsystem for snap {snap_uuid}"

    tgt_lvs_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)
    ret = tgt_rpc.listeners_create(temp_nqn, trtype, target_ip, tgt_lvs_port)
    if not ret:
        _cleanup(subsystem=temp_nqn)
        return None, f"Failed to create migration listener for snap {snap_uuid}"

    ret = tgt_rpc.nvmf_subsystem_add_ns(temp_nqn, tgt_composite)
    if not ret:
        _cleanup(subsystem=temp_nqn)
        return None, f"Failed to add ns to migration subsystem for snap {snap_uuid}"

    # Step 5: connect source to target
    ret = src_rpc.bdev_nvme_attach_controller(
        ctrl_name, temp_nqn, target_ip, tgt_lvs_port, trtype)
    if not ret:
        _cleanup(subsystem=temp_nqn)
        return None, f"Failed to attach migration controller for snap {snap_uuid}"

    # Step 6: fire async transfer
    remote_bdev = f"{ctrl_name}n1"
    ret = src_rpc.bdev_lvol_transfer(src_composite, 0, 16, remote_bdev, "migrate")
    if ret is None:
        src_rpc.bdev_nvme_detach_controller(ctrl_name)
        _cleanup(subsystem=temp_nqn)
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

    # Link to predecessor snapshot in target's ancestry chain.
    # add_clone must succeed on BOTH primary and secondary before we convert
    # either — once convert runs the lvol is immutable and cannot be re-linked.
    if migration.snaps_migrated:
        pred_uuid = migration.snaps_migrated[-1]
        try:
            pred_snap = db.get_snapshot_by_id(pred_uuid)
            # Predecessor was created on target with the migration suffix — build
            # composite from the source short name + suffix, not from snap_bdev
            # (which still holds the source path until apply_migration_to_db runs).
            pred_composite = f"{tgt_node.lvstore}/{_snap_short_name(pred_snap) + _MIGRATION_BDEV_SUFFIX}"
            ret = tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
            if not ret:
                return False, f"bdev_lvol_add_clone failed for {snap_uuid}"
            if tgt_sec and sec_rpc:
                ret_sec = sec_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
                if not ret_sec:
                    return False, f"bdev_lvol_add_clone on secondary failed for {snap_uuid}"
        except KeyError:
            logger.warning(f"Predecessor snap {pred_uuid} not found; skipping add_clone")

    # Convert writable lvol → immutable snapshot.
    # Must succeed on both sides — a primary-only convert leaves secondary with
    # a writable bdev where primary has a read-only snapshot (split state).
    ret = tgt_rpc.bdev_lvol_convert(tgt_composite)
    if not ret:
        return False, f"bdev_lvol_convert failed for {snap_uuid}"

    if tgt_sec and sec_rpc:
        ret_sec = sec_rpc.bdev_lvol_convert(tgt_composite)
        if not ret_sec:
            return False, f"bdev_lvol_convert on secondary failed for {snap_uuid}"

    # Early partial DB update: route health-check and delete to the target node
    # immediately after convert.  snap_bdev keeps its source path here; the full
    # update (with migration suffix and all other fields) happens in
    # apply_migration_to_db() at the end of CLEANUP_SOURCE.
    try:
        snap_rec = db.get_snapshot_by_id(snap_uuid)
        if snap_rec.lvol.uuid == migration.lvol_id:
            snap_rec.lvol.node_id = tgt_node.get_id()
            snap_rec.write_to_db(db.kv_store)
    except KeyError:
        logger.warning(f"Snapshot {snap_uuid} not found in DB for early node update")

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

    # Pre-create TGT subsystems with inaccessible listeners on the very first call
    # (before any snapshot is transferred). The client can pre-connect to these
    # endpoints during the long snapshot-transfer window so no reconnect is needed
    # at cutover — only ANA state flips are required.
    if not migration.snaps_migrated and not ctx:
        try:
            lvol_pre = db.get_lvol_by_id(migration.lvol_id)
            nqn_pre = lvol_pre.nqn
            tgt_trtype_pre, tgt_ip_pre = _get_migration_nic(tgt_node)
            tgt_port_pre = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)
            if tgt_rpc.subsystem_create(
                    nqn_pre, lvol_pre.ha_type, lvol_pre.uuid, min_cntlid=2000,
                    max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS):
                tgt_rpc.listeners_create(
                    nqn_pre, tgt_trtype_pre, tgt_ip_pre, tgt_port_pre,
                    ana_state="inaccessible")
                logger.info(
                    f"Pre-created TGT subsystem {nqn_pre} "
                    f"listener={tgt_ip_pre}:{tgt_port_pre} inaccessible "
                    f"(client may pre-connect now)")
            else:
                logger.warning(
                    f"TGT subsystem pre-create returned False for {nqn_pre} "
                    f"(may already exist — continuing)")
            if lvol_pre.ha_type == "ha":
                tgt_sec_pre, sec_pre_err = _get_target_secondary_node(tgt_node)
                if sec_pre_err:
                    logger.warning(
                        f"TGT-sec not available for subsystem pre-create: {sec_pre_err}")
                elif tgt_sec_pre is not None:
                    sec_pre_rpc = _make_rpc(tgt_sec_pre)
                    tgt_sec_trtype_pre, tgt_sec_ip_pre = _get_migration_nic(tgt_sec_pre)
                    tgt_sec_port_pre = tgt_sec_pre.get_lvol_subsys_port(tgt_node.lvstore)
                    if sec_pre_rpc.subsystem_create(
                            nqn_pre, lvol_pre.ha_type, lvol_pre.uuid, min_cntlid=3000,
                            max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS):
                        sec_pre_rpc.listeners_create(
                            nqn_pre, tgt_sec_trtype_pre, tgt_sec_ip_pre,
                            tgt_sec_port_pre, ana_state="inaccessible")
                        logger.info(
                            f"Pre-created TGT-sec subsystem {nqn_pre} "
                            f"listener={tgt_sec_ip_pre}:{tgt_sec_port_pre} inaccessible")
                    else:
                        logger.warning(
                            f"TGT-sec subsystem pre-create returned False for {nqn_pre}")
        except Exception as e:
            logger.warning(f"Subsystem pre-create failed (non-fatal, continuing): {e}")

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

                snap_short_src = _snap_short_name(snap)
                snap_short_tgt = snap_short_src + _MIGRATION_BDEV_SUFFIX
                src_composite = _snap_composite(src_node.lvstore, snap)
                tgt_composite = f"{tgt_node.lvstore}/{snap_short_tgt}"
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
                        'snap_short': snap_short_tgt,
                        'snap_index': snap_index,
                        'temp_nqn': temp_nqn,
                        'ctrl_name': ctrl_name,
                        'transfer_done': False,
                        'post_done': False,
                    })
                    continue

                # Check for a bdev already present on the target.
                # Two distinct cases must be handled:
                #   1. Genuinely pre-existing (placed by a sibling migration that
                #      already ran) — snap is already in snaps_migrated → skip.
                #   2. Leftover from a previous failed attempt of THIS migration —
                #      snap is NOT yet in snaps_migrated → delete and retry, so we
                #      do not resume from a partially-written bdev.
                if tgt_rpc.get_bdevs(tgt_composite):
                    if snap_uuid in migration.snaps_migrated:
                        logger.info(
                            f"Snapshot {snap_uuid} already on target; skipping transfer")
                        migration.snaps_preexisting_on_target.append(snap_uuid)
                        continue
                    logger.info(
                        f"Removing leftover target bdev {tgt_composite} from failed attempt")
                    try:
                        _delete_bdev_blocking(tgt_composite, tgt_rpc)
                    except Exception as e:
                        logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")

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

        tgt_sec = None
        sec_rpc = None
        if snap.lvol.ha_type == "ha":
            tgt_sec, sec_err = _get_target_secondary_node(tgt_node)
            if sec_err:
                migration.error_message = sec_err
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT
            if tgt_sec:
                sec_rpc = _make_rpc(tgt_sec)

        snap_short_src = _snap_short_name(snap)
        snap_short_tgt = snap_short_src + _MIGRATION_BDEV_SUFFIX
        src_composite  = _snap_composite(src_node.lvstore, snap)
        tgt_composite  = f"{tgt_node.lvstore}/{snap_short_tgt}"

        # Pre-cleanup: only delete if the bdev actually exists on the target
        # (stale from a previous crashed run). Deleting blindly masks real errors.
        if tgt_rpc.get_bdevs(tgt_composite):
            logger.info(f"Pre-cleanup: removing stale intermediate bdev {tgt_composite}")
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc)
            except Exception as e:
                logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")

        t, err = _setup_snap_transfer(
            snap, snap_index, migration, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype, target_ip,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc)
        if t is None:
            return False, True, err

        logger.info(
            f"Started intermediate snap transfer: {snap_uuid} "
            f"({src_composite} -> {tgt_composite})")

        # Busy-poll: spin at _INTERMEDIATE_POLL_INTERVAL_S until done or timeout
        for _ in range(_INTERMEDIATE_POLL_MAX):
            result = src_rpc.bdev_lvol_transfer_stat(src_composite)
            if result is None:
                _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
                _delete_bdev_blocking(tgt_composite, tgt_rpc, secondary_rpc=sec_rpc)
                return False, True, (
                    f"Transfer stat failed for intermediate snap {snap_uuid}")
            state = result.get('transfer_state', 'No process')
            if state == 'Done':
                break
            if state in ('Failed', 'No process'):
                _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
                _delete_bdev_blocking(tgt_composite, tgt_rpc, secondary_rpc=sec_rpc)
                return False, True, (
                    f"Intermediate snap transfer {state} for {snap_uuid}")
            time.sleep(_INTERMEDIATE_POLL_INTERVAL_S)
        else:
            _cleanup_snap_transfer(src_rpc, tgt_rpc, t)
            _delete_bdev_blocking(tgt_composite, tgt_rpc, secondary_rpc=sec_rpc)
            return False, True, (
                f"Intermediate snap transfer timed out for {snap_uuid}")

        ok, err = _post_process_snap(
            snap, tgt_node, tgt_rpc, src_rpc, migration, t,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc)
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
    logger.info(
        f"[IO-FREEZE] {_now_ms()} intermediate snapshot starting: "
        f"lvol={migration.lvol_id} round={migration.intermediate_snap_rounds} name={snap_name}")
    snap_uuid, err = snapshot_controller.add(migration.lvol_id, snap_name)
    if err:
        logger.warning(f"Intermediate snapshot failed (proceeding without): {err}")
        migration.intermediate_snap_rounds = migration.max_intermediate_snap_rounds
        migration.write_to_db(db.kv_store)
        return

    logger.info(
        f"[IO-RESUME] {_now_ms()} intermediate snapshot done: "
        f"lvol={migration.lvol_id} snap={snap_uuid}")
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
    (synchronous — blocks until SPDK completes the delta copy).  On success,
    immediately rebuilds the TGT NVMe-oF subsystem and sets STATUS_CUTOVER.

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
    tgt_lvol_bdev = lvol.lvol_bdev + _MIGRATION_BDEV_SUFFIX
    tgt_lvol_composite = f"{tgt_node.lvstore}/{tgt_lvol_bdev}"
    ctx = migration.transfer_context or {}

    # --- Crash recovery: Done handler was interrupted mid-run ---
    # bdev_lvol_final_migration is synchronous — it blocks until SPDK completes.
    # If we re-enter with stage='transfer' the migration already finished; check
    # stat once to detect the rare SPDK-side failure, then re-run Done handler.
    if ctx.get('stage') == 'transfer':
        result = src_rpc.bdev_lvol_transfer_stat(src_lvol_composite)
        if result is None:
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True)
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, "bdev_lvol_transfer_stat returned None (crash recovery)"
        state = result.get('transfer_state', 'No process')
        if state == 'Failed':
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True)
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, "Final migration Failed (crash recovery)"
        # 'Done' or 'No process': migration completed — SPDK cleans up the
        # transfer poller after a sync call so 'No process' means finished.
        logger.info(
            f"[IO-RESUME] {_now_ms()} final migration Done (crash recovery, state={state}): "
            f"lvol={migration.lvol_id} io now live on target")
        tgt_uuid_carry = {
            'tgt_lvol_uuid': ctx.get('tgt_lvol_uuid'),
            'tgt_lvol_bdev': tgt_lvol_bdev,
            'hub_ctrl_name': ctx.get('ctrl_name'),
        }

    else:
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
        lvol_size_in_mib = _bytes_to_mib(lvol.size)
        logger.info(
            f"[MIGRATION SIZE CHECK] lvol={lvol.lvol_bdev} "
            f"source_size_bytes={lvol.size} target_size_mib={lvol_size_in_mib}"
        )
        _log_spdk_bdev_size(src_rpc, src_lvol_composite, f"SRC lvol[{lvol.lvol_bdev}] pre-create")
        ret = tgt_rpc.create_lvol(tgt_lvol_bdev, lvol_size_in_mib, tgt_node.lvstore)
        if not ret:
            return False, True, f"Failed to create target lvol {tgt_lvol_composite}"
        _log_spdk_bdev_size(tgt_rpc, tgt_lvol_composite, f"TGT lvol[{lvol.lvol_bdev}] post-create")

        ret = tgt_rpc.bdev_lvol_set_migration_flag(tgt_lvol_composite)
        if not ret:
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
            return False, True, f"bdev_lvol_set_migration_flag failed for target lvol {tgt_lvol_composite}"

        # Step 1b: query map_id / blobid / uuid — needed for secondary registration
        # and for bdev_lvol_final_migration.  Do this once here rather than again
        # after NVMe-oF setup to keep secondary state consistent from the start.
        lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore)
        if not lvols_list:
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
            return False, True, "bdev_lvol_get_lvols returned empty result from target"

        tgt_map_id = None
        tgt_blobid = None
        tgt_uuid = None
        for entry in lvols_list:
            entry_name = entry.get('name', '') or entry.get('lvol_name', '')
            if entry_name in (tgt_lvol_bdev, tgt_lvol_composite):
                tgt_map_id = entry.get('map_id')
                tgt_blobid = entry.get('blobid')
                tgt_uuid = entry.get('uuid')
                break

        if tgt_map_id is None:
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
            return False, True, f"Could not find map_id for {lvol.lvol_bdev} on target"

        # Step 1c: register lvol on secondary and set migration flag there.
        # The secondary's hublvol_write() checks migration_flag before deciding
        # whether to treat the completion signal as a chain-parent operation.
        # If the flag is not set the signal is treated as normal I/O and the
        # secondary's lvol is never chained to its parent snapshot.
        sec_setup_rpc = None
        if lvol.ha_type == "ha":
            tgt_sec_setup, sec_setup_err = _get_target_secondary_node(tgt_node)
            if sec_setup_err:
                _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
                return False, True, sec_setup_err
            if tgt_sec_setup is not None:
                sec_setup_rpc = _make_rpc(tgt_sec_setup)
                ret = sec_setup_rpc.bdev_lvol_register(
                    tgt_lvol_bdev, tgt_node.lvstore, tgt_uuid, tgt_blobid,
                    lvol.lvol_priority_class)
                if not ret:
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
                    return False, True, (
                        f"bdev_lvol_register on secondary failed for {tgt_lvol_composite}")
                ret = sec_setup_rpc.bdev_lvol_set_migration_flag(tgt_lvol_composite)
                if not ret:
                    sec_setup_rpc.delete_lvol(tgt_lvol_bdev, del_async=True)
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc)
                    return False, True, (
                        f"bdev_lvol_set_migration_flag on secondary failed for {tgt_lvol_composite}")

        # NVMe-oF subsystem setup is deferred to the Done handler — the subsystem
        # is deleted and recreated fresh after transfer completes so all paths get
        # a clean primary-port subsystem (min_cntlid=2000).

        # Step 3: connect source to target hub lvol
        ctrl_name = f"mighub_{migration.uuid[:8]}"
        hub_nqn = tgt_node.hublvol.nqn
        hub_port = tgt_node.hublvol.nvmf_port
        ret = src_rpc.bdev_nvme_attach_controller(ctrl_name, hub_nqn, target_ip, hub_port, trtype)
        if not ret:
            # Attachment can fail with EEXIST (-17) if the task runner crashed
            # after creating the hub controller in a previous attempt but before
            # persisting transfer_context to the DB.  The zombie controller is
            # still alive on the source SPDK.  Check whether the controller's
            # namespace bdev already exists; if so, reuse it (idempotent retry).
            if src_rpc.get_bdevs(f"{ctrl_name}n1"):
                logger.info(
                    f"Hub controller {ctrl_name} already exists on source "
                    f"(zombie from previous attempt); reusing"
                )
            else:
                _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc, secondary_rpc=sec_setup_rpc)
                return False, True, "Failed to connect source to target hub"

        hub_bdev = f"{ctrl_name}n1"

        # Step 4: locate the last migrated snapshot's composite name on the target
        if not migration.snaps_migrated:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc, secondary_rpc=sec_setup_rpc)
            return False, True, "No snapshots migrated; cannot perform final migration"

        last_snap_uuid = migration.snaps_migrated[-1]
        try:
            last_snap = db.get_snapshot_by_id(last_snap_uuid)
        except KeyError:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc, secondary_rpc=sec_setup_rpc)
            return False, True, f"Last snapshot {last_snap_uuid} not found"

        tgt_snap_composite = f"{tgt_node.lvstore}/{_snap_short_name(last_snap) + _MIGRATION_BDEV_SUFFIX}"

        # Step 5: start final migration — synchronous: blocks until SPDK completes
        # the IO drain and delta copy.  Returns success/failure directly; no polling needed.
        logger.info(
            f"[IO-FREEZE] {_now_ms()} bdev_lvol_final_migration starting: "
            f"lvol={lvol.uuid} src={src_lvol_composite} tgt_snap={tgt_snap_composite}")
        ret = src_rpc.bdev_lvol_final_migration(
            src_lvol_composite, tgt_map_id, tgt_snap_composite, 2, hub_bdev)
        if ret is None:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
            _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc, secondary_rpc=sec_setup_rpc)
            return False, True, "bdev_lvol_final_migration failed"

        logger.info(
            f"[IO-RESUME] {_now_ms()} final migration Done: "
            f"lvol={migration.lvol_id} io now live on target")
        migration.current_job_id = ""
        # Save crash recovery anchor before Done handler so a mid-handler crash
        # re-enters here with stage='transfer' and skips re-doing setup.
        migration.transfer_context = {
            'stage': 'transfer',
            'ctrl_name': ctrl_name,
            'tgt_lvol_composite': tgt_lvol_composite,
            'tgt_lvol_uuid': tgt_uuid,
            'tgt_lvol_bdev': tgt_lvol_bdev,
        }
        migration.write_to_db(db.kv_store)
        tgt_uuid_carry = {
            'tgt_lvol_uuid': tgt_uuid,
            'tgt_lvol_bdev': tgt_lvol_bdev,
            'hub_ctrl_name': ctrl_name,
        }

    # --- Done handler (shared by first-call and crash-recovery paths) ---
    migration.transfer_context = tgt_uuid_carry

    # Done handler: add LVOL_XXXm as namespace and flip ANA states so the client
    # follows the volume without disconnect/reconnect.
    # REVERT: git checkout a11a0974 -- simplyblock_core/services/tasks_runner_lvol_migration.py
    nqn = lvol.nqn
    # Adjacent migration: TGT is the source's secondary node — its subsystem already
    # exists with nsid=1 pointing to the old source-secondary bdev.
    tgt_is_src_secondary = (tgt_node.get_id() == src_node.secondary_node_id)

    if tgt_is_src_secondary:
        logger.info(
            f"Adjacent migration: TGT {tgt_node.get_id()} == SRC secondary — "
            f"swapping existing namespace to {tgt_lvol_composite}")
        # Try atomic bdev swap first (SPDK ≥ 24.01): no namespace-change AER to client,
        # nsid/uuid/nguid stay identical.
        swap_ret = tgt_rpc.nvmf_subsystem_ns_update(nqn, 1, tgt_lvol_composite)
        if swap_ret is not None:
            logger.info(f"nvmf_subsystem_ns_update: nsid=1 → {tgt_lvol_composite}")
        else:
            # Fallback: remove old nsid=1 (TGT is non-optimized so no active IO through
            # this path), then re-add — SPDK auto-assigns nsid=1 to the now-empty subsystem.
            logger.info("nvmf_subsystem_ns_update unavailable — using remove + add_ns")
            try:
                tgt_rpc.nvmf_subsystem_remove_ns(nqn, 1)
                logger.info(f"Removed old nsid=1 from TGT subsystem {nqn}")
            except Exception as e:
                logger.warning(f"Could not remove old nsid=1 from TGT (non-fatal): {e}")
            try:
                ns_ret = tgt_rpc.nvmf_subsystem_add_ns(
                    nqn, tgt_lvol_composite, lvol.uuid, lvol.guid)
                if ns_ret:
                    logger.info(
                        f"Added {tgt_lvol_composite} nsid={ns_ret} to TGT subsystem {nqn}")
                else:
                    logger.error(
                        f"nvmf_subsystem_add_ns failed on TGT for adjacent case")
            except Exception as e:
                logger.error(f"TGT namespace add failed (adjacent): {e}")
        # The TGT listener lives at the SOURCE's lvol port (e.g. 4430), not TGT's own
        # lvstore port (4432).  Flip it to optimized NOW — before _update_ana_states
        # makes SRC inaccessible — so the client always has an optimized path.
        tgt_adj_trtype, tgt_adj_ip = _get_migration_nic(tgt_node)
        tgt_adj_port = src_node.get_lvol_subsys_port(src_node.lvstore)
        try:
            tgt_rpc.nvmf_subsystem_listener_set_ana_state(
                nqn, tgt_adj_ip, tgt_adj_port, trtype=tgt_adj_trtype, ana="optimized")
            logger.info(
                f"ANA: {nqn} TGT-adjacent {tgt_adj_ip}:{tgt_adj_port} → optimized")
        except Exception as e:
            logger.error(f"ANA update TGT-adjacent failed: {e}")
    else:
        # Independent case: TGT subsystem was pre-created empty during PHASE_SNAP_COPY.
        # Add namespace now — SPDK auto-assigns nsid=1.
        try:
            ns_ret = tgt_rpc.nvmf_subsystem_add_ns(
                nqn, tgt_lvol_composite, lvol.uuid, lvol.guid)
            if ns_ret:
                logger.info(
                    f"Added namespace {tgt_lvol_composite} nsid={ns_ret} "
                    f"to TGT subsystem {nqn}")
            else:
                logger.error(
                    f"nvmf_subsystem_add_ns {tgt_lvol_composite} failed on TGT — "
                    f"ANA flip may leave client with no usable namespace")
        except Exception as e:
            logger.error(f"TGT namespace add failed: {e}")

    # TGT secondary: chain ancestry via add_clone, then attach namespace to
    # pre-created secondary subsystem (reuses inaccessible subsystem from SNAP_COPY).
    tgt_sec = None
    sec_rpc = None
    if lvol.ha_type == "ha":
        tgt_sec, sec_err = _get_target_secondary_node(tgt_node)
        if sec_err:
            logger.warning(
                f"Cannot find TGT secondary for namespace setup ({sec_err}); continuing without it")
        elif tgt_sec is not None:
            sec_rpc = _make_rpc(tgt_sec)
            if migration.snaps_migrated:
                try:
                    last_snap = db.get_snapshot_by_id(migration.snaps_migrated[-1])
                    tgt_snap_composite = (
                        f"{tgt_node.lvstore}/"
                        f"{_snap_short_name(last_snap) + _MIGRATION_BDEV_SUFFIX}")
                    ret = sec_rpc.bdev_lvol_add_clone(tgt_lvol_composite, tgt_snap_composite)
                    if not ret:
                        logger.warning(
                            f"bdev_lvol_add_clone on secondary failed for "
                            f"{tgt_lvol_composite} → {tgt_snap_composite}; "
                            f"skipping secondary namespace setup")
                        tgt_sec = None
                except KeyError as e:
                    logger.warning(
                        f"Last snapshot not found for secondary chaining: {e}; "
                        f"skipping secondary setup")
                    tgt_sec = None
            if tgt_sec is not None:
                ok, err = _expose_lvol_on_secondary(
                    lvol, tgt_node, tgt_sec, sec_rpc, None, None,
                    already_registered=True, tgt_bdev_name=tgt_lvol_bdev,
                    min_cntlid=3000)
                if ok:
                    logger.info(f"Namespace added on TGT secondary {tgt_sec.get_id()}")
                else:
                    logger.warning(f"TGT secondary namespace setup failed (non-fatal): {err}")
                    tgt_sec = None

    # Flip ANA states atomically: TGT → optimized/non_optimized, SRC → inaccessible.
    # Client's kernel multipath re-routes IO instantly without disconnect/reconnect.
    _update_ana_states(lvol, src_node, tgt_node, src_rpc, tgt_rpc,
                       tgt_sec=tgt_sec, sec_rpc=sec_rpc)

    # Save source snap bdev names before apply_migration_to_db updates
    # them — PHASE_CLEANUP_SOURCE uses this map to derive the correct
    # source bdev names regardless of which path ran.
    source_snap_bdevs = {}
    for _snap_uuid in migration.snaps_migrated:
        try:
            _snap = db.get_snapshot_by_id(_snap_uuid)
            source_snap_bdevs[_snap_uuid] = _snap.snap_bdev
        except KeyError:
            pass
    tgt_uuid_carry['source_snap_bdevs'] = source_snap_bdevs
    # Save original source lvol bdev name before apply_migration_to_db
    # renames lvol.lvol_bdev to the target 'm'-suffix name.
    tgt_uuid_carry['source_lvol_bdev'] = lvol.lvol_bdev
    # Persist before apply_migration_to_db updates snap.snap_bdev / lvol.lvol_bdev
    # so a crash between apply and the runner's DB write still has correct source
    # paths on re-entry.
    migration.write_to_db(db.kv_store)

    # Apply DB records now so sbctl volume connect returns TGT endpoints
    # for clients polling migration status at cutover time.
    migration_controller.apply_migration_to_db(
        migration,
        tgt_lvol_uuid=tgt_uuid_carry.get('tgt_lvol_uuid'),
        tgt_lvol_bdev=tgt_uuid_carry.get('tgt_lvol_bdev'))

    tgt_uuid_carry['cutover_notified_at'] = time.time()
    migration.status = LVolMigration.STATUS_CUTOVER
    logger.info(
        f"Migration {migration.uuid}: STATUS_CUTOVER set — "
        f"TGT subsystem live, grace period starts, "
        f"client should reconnect now")
    return True, False, None



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


def _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc=None):
    """
    Delete migration-created intermediate ('shrink') snapshots from the target
    after a successful migration.

    Must be called AFTER apply_migration_to_db() — at that point snap.snap_bdev
    already holds the target composite path (e.g. LVS_TGT/SNAP_xxxm).

    No migration flag is set before deletion so SPDK actually coalesces the
    clusters into the child bdev and frees them.  Deletion proceeds oldest-first
    so each snapshot has at most one child when it is removed, satisfying SPDK's
    snapshot-deletion constraint.
    """
    for snap_uuid in migration.intermediate_snaps:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            logger.info(f"Intermediate snap {snap_uuid} already removed from DB; skipping")
            continue

        tgt_composite = snap.snap_bdev  # updated to target path by apply_migration_to_db

        if tgt_rpc.get_bdevs(tgt_composite):
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc, secondary_rpc=tgt_sec_rpc)
                logger.info(f"Deleted intermediate snap bdev {tgt_composite} from target")
            except Exception as e:
                logger.warning(
                    f"Could not delete intermediate snap {tgt_composite} from target: {e}")
        else:
            logger.info(
                f"Intermediate snap bdev {tgt_composite} absent from target; skipping SPDK delete")

        try:
            snap.remove(db.kv_store)
            logger.info(f"Removed intermediate snap {snap_uuid} from DB")
        except Exception as e:
            logger.warning(f"Could not remove intermediate snap {snap_uuid} from DB: {e}")


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




_SKIP_CLEANUP_SOURCE = False  # DEBUG: set True to skip source cleanup


def _handle_cleanup_source(migration, src_node, src_rpc, tgt_node, tgt_rpc):
    """
    Delete snapshots from the source node that are exclusively owned by the
    migrated volume, then tear down the source NVMe-oF subsystem, delete the
    source lvol bdev, and update DB records.

    Each snapshot deletion uses _delete_bdev_blocking (async-start → poll →
    sync-finalize on primary and secondary) which blocks until the bdev is
    gone.  This avoids snapshot_controller.delete() clone-check soft-delete
    behaviour.  apply_migration_to_db() is called AFTER all deletes complete.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    if _SKIP_CLEANUP_SOURCE:
        logger.info("SKIP_CLEANUP_SOURCE flag is set — skipping source cleanup, applying DB only")
        _ctx = migration.transfer_context or {}
        migration_controller.apply_migration_to_db(
            migration,
            tgt_lvol_uuid=_ctx.get('tgt_lvol_uuid'),
            tgt_lvol_bdev=_ctx.get('tgt_lvol_bdev'))
        if migration.intermediate_snaps:
            tgt_sec_rpc = _get_secondary_rpc(tgt_node)
            _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc)
        return True, False, None

    ctx = migration.transfer_context or {}

    # --- Cutover grace period ---
    # Wait here until the client has had time to reconnect to the new TGT
    # subsystem before we tear down the source subsystem.  The grace period
    # begins when PHASE_LVOL_MIGRATE sets cutover_notified_at and changes
    # migration.status to STATUS_CUTOVER — the test script polls for that
    # status and triggers client reconnect immediately upon seeing it.
    cutover_notified_at = ctx.get('cutover_notified_at', 0)
    if cutover_notified_at and ctx.get('stage') != 'cleanup_src':
        elapsed = time.time() - cutover_notified_at
        grace = 30.0
        if elapsed < grace:
            logger.info(
                f"PHASE_CLEANUP_SOURCE: cutover grace — "
                f"{grace - elapsed:.1f}s remaining for client reconnect")
            return False, False, None
        logger.info(
            f"PHASE_CLEANUP_SOURCE: cutover grace elapsed "
            f"({elapsed:.1f}s >= {grace}s), proceeding with source cleanup")

    # --- First entry: initialize cleanup state ---
    if ctx.get('stage') != 'cleanup_src':
        # Preserve the target lvol UUID and bdev name written by PHASE_LVOL_MIGRATE
        # so we can update lvol.lvol_uuid / lvol.lvol_bdev in the DB after cleanup.
        tgt_lvol_uuid = ctx.get('tgt_lvol_uuid')
        tgt_lvol_bdev = ctx.get('tgt_lvol_bdev')
        # Carry the pre-apply source bdev names so cleanup deletes hit the correct
        # bdevs even after apply_migration_to_db renamed them to target names in DB.
        source_snap_bdevs_saved = ctx.get('source_snap_bdevs', {})
        source_lvol_bdev_saved  = ctx.get('source_lvol_bdev', '')

        to_delete = migration_controller.get_snaps_safe_to_delete_on_source(migration)

        # Verify each snapshot to be deleted physically exists on the target
        # before we remove anything from the source.  Target bdevs carry the
        # migration suffix (e.g. SNAP_xxxm) — derive from source short name.
        tgt_lvols = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
        tgt_names = {e.get('name', '').split('/')[-1] for e in tgt_lvols}
        for snap_uuid in to_delete:
            try:
                snap = db.get_snapshot_by_id(snap_uuid)
                snap_short_tgt = _snap_tgt_short_name(snap)
                if snap_short_tgt not in tgt_names:
                    return False, False, (
                        f"Target missing snapshot {snap_short_tgt} ({snap_uuid}) "
                        "— aborting source cleanup to prevent data loss"
                    )
            except KeyError:
                pass  # already gone from DB; safe to skip

        ctx = {
            'stage': 'cleanup_src',
            'tgt_lvol_uuid': tgt_lvol_uuid,
            'tgt_lvol_bdev': tgt_lvol_bdev,
            # Carry hub_ctrl_name through the ctx rebuild so the deferred
            # hub detach at the end of cleanup can still find it.
            'hub_ctrl_name': (migration.transfer_context or {}).get('hub_ctrl_name'),
            'source_snap_bdevs': source_snap_bdevs_saved,
            'source_lvol_bdev':  source_lvol_bdev_saved,
        }
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    src_sec_rpc = _get_secondary_rpc(src_node)

    # --- Delete source snapshots (blocking, idempotent) ---
    # Recompute to_delete here so re-entries after crash work without stored pending list.
    # _delete_bdev_blocking handles "not found" (status 2) gracefully, so double-deletes
    # from a crash-recovery re-run are safe.
    to_delete = migration_controller.get_snaps_safe_to_delete_on_source(migration)
    source_snap_bdevs = ctx.get('source_snap_bdevs', {})
    for snap_uuid in to_delete:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            if snap_uuid in source_snap_bdevs:
                bdev_name = source_snap_bdevs[snap_uuid]
            else:
                bdev_name = f"{src_node.lvstore}/{_snap_short_name(snap)}"
            # Mark as migration-source so SPDK drops only the blob reference
            # without freeing the physical clusters (data lives on the target now).
            src_rpc.bdev_lvol_set_migration_flag(bdev_name)
            if src_sec_rpc:
                src_sec_rpc.bdev_lvol_set_migration_flag(bdev_name)
            _delete_bdev_blocking(bdev_name, src_rpc, secondary_rpc=src_sec_rpc)
            logger.info(f"Deleted source bdev {bdev_name}")
        except KeyError:
            logger.warning(f"Source snapshot {snap_uuid} not found in DB; skipping")

    # --- All deletes finished: hub detach then NVMe-oF teardown ---
    #
    # Teardown order:
    #   Step 7: detach hub controller on SRC — severs the SRC→TGT mirror link.
    #           Must happen BEFORE deleting the source subsystem so that
    #           bdev_nvme_detach_controller can still reach the hub bdev.
    #   Step 8: delete source primary NVMe-oF subsystem.
    #   Then:   delete source lvol bdev.
    hub_ctrl_name = ctx.get('hub_ctrl_name')
    if hub_ctrl_name:
        try:
            src_rpc.bdev_nvme_detach_controller(hub_ctrl_name)
            logger.info(f"Step 7: deferred hub controller detach: {hub_ctrl_name}")
        except Exception as e:
            logger.warning(f"Deferred hub detach {hub_ctrl_name} (non-fatal): {e}")

    lvol = None
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
        logger.info(f"Step 8: removing source NVMe-oF subsystem {lvol.nqn}")
        _cleanup_subsystem_or_ns(lvol.nqn, lvol.ns_id, True, src_rpc)
        if src_sec_rpc and src_node.secondary_node_id != tgt_node.get_id():
            # When TGT is SRC's secondary, src_sec_rpc points to TGT whose
            # subsystem was rebuilt as the new primary — do not touch it here.
            _cleanup_subsystem_or_ns(lvol.nqn, lvol.ns_id, True, src_sec_rpc)
    except Exception as e:
        logger.warning(f"Source subsystem cleanup failed (non-fatal): {e}")

    # Explicitly delete the source lvol bdev.  bdev_lvol_final_migration may
    # have already freed it on the SPDK side — _delete_bdev_blocking handles
    # that gracefully (status 2 = not-found is treated as complete).
    if lvol is not None:
        try:
            # Use the saved pre-apply bdev name; apply_migration_to_db already
            # renamed lvol.lvol_bdev to the target 'm'-suffix name in DB.
            src_bdev_short = ctx.get('source_lvol_bdev') or lvol.lvol_bdev
            src_lvol_composite = f"{src_node.lvstore}/{src_bdev_short}"
            # Set migration flag so SPDK drops only the blob reference without
            # freeing the physical clusters — data now lives on the target.
            try:
                src_rpc.bdev_lvol_set_migration_flag(src_lvol_composite)
            except Exception as _mf_err:
                logger.warning(
                    f"bdev_lvol_set_migration_flag for source lvol failed "
                    f"(non-fatal): {_mf_err}")
            _delete_bdev_blocking(
                src_lvol_composite, src_rpc,
                secondary_rpc=src_sec_rpc)
            logger.info(f"Deleted source lvol bdev {src_lvol_composite}")
        except Exception as e:
            logger.warning(f"Source lvol delete failed (non-fatal): {e}")

    tgt_lvol_uuid = ctx.get('tgt_lvol_uuid')
    tgt_lvol_bdev = ctx.get('tgt_lvol_bdev')
    migration.transfer_context = {}
    if not migration_controller.apply_migration_to_db(
            migration, tgt_lvol_uuid=tgt_lvol_uuid, tgt_lvol_bdev=tgt_lvol_bdev):
        return False, False, "Failed to update DB records after source cleanup"

    # Delete intermediate (shrink) snapshots from the target — they are migration
    # artifacts and do not need to be preserved. No migration flag so SPDK
    # coalesces and frees their clusters into the child bdev.
    if migration.intermediate_snaps:
        tgt_sec_rpc = _get_secondary_rpc(tgt_node)
        _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc)

    return True, False, None


def _handle_cleanup_target(migration, tgt_node, tgt_rpc):
    """
    Roll back a failed or cancelled migration: remove any partially-created
    target lvol/subsystem, then delete all snapshots copied to the target.

    Each deletion uses _delete_bdev_blocking (async-start → poll → sync-finalize
    on primary and secondary).  Idempotent: "not found" (status 2) is treated as
    already done, so a crash-recovery re-run is safe.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    ctx = migration.transfer_context or {}
    tgt_sec_rpc = _get_secondary_rpc(tgt_node)

    # --- Step 0: delete dangling target lvol from a failed LVOL_MIGRATE ---
    if ctx.get('stage') != 'cleanup_tgt':
        tgt_lvol_composite = ctx.get('tgt_lvol_composite')
        nqn = ctx.get('nqn')
        tgt_ns_id = ctx.get('tgt_ns_id')
        subsystem_created_on_target = ctx.get('subsystem_created_on_target', True)
        if tgt_lvol_composite:
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
            try:
                _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc, secondary_rpc=tgt_sec_rpc)
                logger.info(f"Deleted target lvol {tgt_lvol_composite}")
            except Exception as e:
                logger.warning(f"delete target lvol {tgt_lvol_composite} (non-fatal): {e}")

        ctx = {'stage': 'cleanup_tgt'}
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    # --- Delete target snapshots (blocking, idempotent) ---
    # Reverse order: children/leaves before parents/roots (SPDK open-ref constraint).
    # _delete_bdev_blocking handles "not found" (status 2) gracefully, so a
    # crash-recovery re-run that re-deletes already-removed bdevs is safe.
    for snap_uuid in reversed(migration.snaps_migrated):
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            snap_short = _snap_tgt_short_name(snap)
            bdev_name = f"{tgt_node.lvstore}/{snap_short}"
            if not tgt_rpc.get_bdevs(bdev_name):
                logger.info(f"Target bdev {bdev_name} not found; skipping (already cleaned up)")
                continue
            _delete_bdev_blocking(bdev_name, tgt_rpc, secondary_rpc=tgt_sec_rpc)
            logger.info(f"Deleted target snapshot bdev {bdev_name}")
        except KeyError:
            logger.warning(f"Target snapshot {snap_uuid} not found in DB; skipping")

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
            done, suspend, error = _handle_cleanup_source(migration, src_node, src_rpc, tgt_node, tgt_rpc)
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
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                for task in db.get_active_migration_tasks(cl.get_id()):
                    # Lease gate: skip a task another live runner host owns, so
                    # two replicas can't both drive the same migration's
                    # multi-phase data-plane state-machine concurrently.
                    if not tasks_controller.claim_task(task):
                        logger.info(f"LVol-migration task {task.uuid} owned by another runner host; skipping")
                        continue
                    task_runner(task)

        time.sleep(3)
