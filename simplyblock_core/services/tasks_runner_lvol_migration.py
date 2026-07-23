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
import random
import time
from typing import Optional

from simplyblock_core import db_controller as db_mod, utils, constants
from simplyblock_core.utils import convert_size
from simplyblock_core.controllers import (
    migration_controller, migration_events, snapshot_controller, tasks_controller, tasks_events
)
from simplyblock_core.controllers.host_auth import _reapply_allowed_hosts
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.rpc_client import RPCException, RPCClient
from simplyblock_core.services.hub_controller_manager import hub_manager

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
_INTERMEDIATE_POLL_INTERVAL_S = 1      # seconds between stat checks
_INTERMEDIATE_POLL_MAX = 300           # max iterations ≈ 5 min


def _now_ms():
    """Return current wall-clock time as an ISO-8601 string with milliseconds."""
    return datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]


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



_MIGRATION_BDEV_SUFFIX = constants.LVOL_MIG_BDEV_SUFFIX
# Suffix applied when the canonical (no-suffix) name is already taken on the
# target after migration.  "am" = "after migration" — stays distinct from the
# in-flight 'm' suffix so pre-existing migrated snapshots remain identifiable.
_MIGRATION_BDEV_SUFFIX_DONE = 'am'


def _apply_migration_to_db(migration, tgt_lvol_uuid=None, tgt_lvol_bdev=None):
    """
    Update control-plane DB records after a successful lvol migration.

    Updates every field that is node- or lvstore-specific on the canonical
    LVol record, its bdev_stack, and on every migrated SnapShot's own fields
    plus the embedded snap.lvol copy — so that delete, clone, and health-check
    paths all use correct target values with nothing stale.

    ``tgt_lvol_bdev`` is the actual SPDK bdev short name on the target (carries
    the migration suffix, e.g. ``LVOL_2882m``).  When provided, ``lvol.lvol_bdev``
    and ``bdev_stack['params']['name']`` are updated to match.

    ANA state changes (optimized/non-optimized/inaccessible) on the NVMe-oF
    subsystems are handled by the task runner after this call.
    """
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
    except KeyError as e:
        logger.error(f"_apply_migration_to_db: lvol not found: {e}")
        return False

    try:
        tgt_node = db.get_storage_node_by_id(migration.target_node_id)
    except KeyError as e:
        logger.error(f"_apply_migration_to_db: target node not found: {e}")
        return False

    # Query SPDK once for all bdevs on the target lvstore.
    # Used to update snap_uuid, blobid on snapshots and lvol.blobid.
    # Degrades gracefully: if unreachable, location fields still get updated.
    spdk_info = {}
    try:
        tgt_rpc = tgt_node.rpc_client()
        raw = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
        for entry in raw:
            short = entry.get('name', '').split('/')[-1]
            if short:
                spdk_info[short] = {
                    'uuid': entry.get('uuid', ''),
                    'blobid': entry.get('blobid', 0),
                }
        logger.info(
            f"_apply_migration_to_db: queried {len(spdk_info)} bdevs "
            f"from target lvstore {tgt_node.lvstore}")
        subsys = tgt_rpc.subsystem_get(lvol.nqn)
        if subsys:
            for ns in subsys.get('namespaces') or []:
                if ns['uuid'] == lvol.uuid:
                    lvol.ns_id = ns['nsid']
                    break
    except Exception as e:
        logger.warning(
            f"_apply_migration_to_db: could not query target SPDK — "
            f"snap_uuid/blobid will not be updated: {e}")

    # Update canonical LVol record
    lvol.node_id = tgt_node.get_id()
    lvol.hostname = tgt_node.hostname
    lvol.lvs_name = tgt_node.lvstore
    lvol.subsys_port = tgt_node.lvol_subsys_port
    if tgt_lvol_bdev:
        lvol.lvol_bdev = tgt_lvol_bdev
    lvol.top_bdev = f"{tgt_node.lvstore}/{lvol.lvol_bdev}"
    if tgt_lvol_uuid:
        lvol.lvol_uuid = tgt_lvol_uuid
    elif lvol.lvol_bdev in spdk_info and spdk_info[lvol.lvol_bdev].get('uuid'):
        lvol.lvol_uuid = spdk_info[lvol.lvol_bdev]['uuid']

    # bdev_stack: the 'bdev_lvol' entry bakes in lvs_name (and name) at creation
    # time; _remove_bdev_stack() uses them to build the delete bdev composite, so
    # both must reflect target values or the delete will hit the wrong bdev.
    for entry in lvol.bdev_stack:
        if entry.get('type') == 'bdev_lvol' and 'params' in entry:
            entry['params']['lvs_name'] = tgt_node.lvstore
            if tgt_lvol_bdev:
                entry['params']['name'] = tgt_lvol_bdev
        elif entry.get('type') == 'bdev_lvol_clone':
            entry['params']['clone_name'] = lvol.lvol_bdev
            entry['name'] = lvol.top_bdev

    if lvol.lvol_bdev in spdk_info:
        lvol.blobid = spdk_info[lvol.lvol_bdev]['blobid']

    lvol.nodes = [tgt_node.get_id()]
    if tgt_node.secondary_node_id:
        lvol.nodes.append(tgt_node.secondary_node_id)
    if tgt_node.tertiary_node_id:
        lvol.nodes.append(tgt_node.tertiary_node_id)

    lvol.write_to_db(db.kv_store)
    logger.info(
        f"_apply_migration_to_db: updated lvol {migration.lvol_id} "
        f"node_id={tgt_node.get_id()}, lvs_name={tgt_node.lvstore}, nodes={lvol.nodes}"
    )

    tgt_subsys_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)

    for snap_uuid in migration.snaps_migrated:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            logger.warning(f"_apply_migration_to_db: snapshot not found: {snap_uuid}")
            continue

        # snap_bdev: update lvstore prefix and normalise the migration suffix.
        # Use the lvstore to detect a retry (snap already updated) rather than
        # endswith(suffix), which gives a false-positive on back-to-back migrations
        # where the source bdev legitimately ends with the suffix from the prior run.
        tgt_short = None
        if snap.snap_bdev and '/' in snap.snap_bdev:
            src_lvstore, src_short = snap.snap_bdev.split('/', 1)
            if src_lvstore == tgt_node.lvstore:
                tgt_short = src_short  # already updated by a previous call — idempotent
            else:
                tgt_short = _snap_tgt_short_name(snap)
            snap.snap_bdev = f"{tgt_node.lvstore}/{tgt_short}"

        if tgt_short and tgt_short in spdk_info:
            snap.snap_uuid = spdk_info[tgt_short]['uuid']
            snap.blobid = spdk_info[tgt_short]['blobid']

        snap.lvol.node_id = tgt_node.get_id()
        snap.lvol.hostname = tgt_node.hostname
        snap.lvol.lvs_name = tgt_node.lvstore
        if tgt_lvol_bdev:
            snap.lvol.lvol_bdev = tgt_lvol_bdev
        snap.lvol.top_bdev = f"{tgt_node.lvstore}/{snap.lvol.lvol_bdev}"
        snap.lvol.nodes = list(lvol.nodes)
        snap.lvol.subsys_port = tgt_subsys_port
        if tgt_lvol_uuid:
            snap.lvol.lvol_uuid = tgt_lvol_uuid

        if snap.lvol.uuid != migration.lvol_id:
            logger.debug(
                f"_apply_migration_to_db: snapshot {snap_uuid} "
                f"belongs to another lvol {snap.lvol.uuid}")
            original_snap = db.get_snapshot_by_id(snap_uuid)
            if not any(s.get('lvol', {}).get('node_id') == snap.lvol.node_id
                       for s in original_snap.instances):
                original_snap.instances.append({
                    "lvol": {
                        "node_id": snap.lvol.node_id,
                        "hostname": snap.lvol.hostname,
                        "lvol_bdev": snap.lvol.lvol_bdev,
                        "uuid": snap.lvol.uuid,
                    },
                    "snap_bdev": snap.snap_bdev,
                    "uuid": snap.uuid,
                })
                original_snap.write_to_db(db.kv_store)
        else:
            referenced = False
            for mini in db.get_mini_lvols():
                if mini.uuid == migration.lvol_id:
                    continue
                if mini.cloned_from_snap and mini.cloned_from_snap == snap_uuid:
                    logger.debug(
                        f"_apply_migration_to_db: snapshot {snap_uuid} "
                        f"is still referenced by lvol {mini.uuid}")
                    original_snap = db.get_snapshot_by_id(snap_uuid)
                    if not any(s.get('lvol', {}).get('node_id') == snap.lvol.node_id
                               for s in original_snap.instances):
                        original_snap.instances.append({
                            "lvol": {
                                "node_id": snap.lvol.node_id,
                                "hostname": snap.lvol.hostname,
                                "lvol_bdev": snap.lvol.lvol_bdev,
                                "uuid": snap.lvol.uuid,
                            },
                            "snap_bdev": snap.snap_bdev,
                            "uuid": snap.uuid,
                        })
                        original_snap.write_to_db(db.kv_store)
                    referenced = True
                    break
            if not referenced:
                snap.write_to_db(db.kv_store)

        logger.debug(
            f"_apply_migration_to_db: updated snapshot {snap_uuid} "
            f"snap_bdev={snap.snap_bdev}")

    # Update DB location for snaps already on TGT from a prior migration.
    # Their bdev is already canonical (renamed when the prior migration cleaned up),
    # so only the location fields need updating — no bdev suffix manipulation.
    for snap_uuid in (migration.snaps_preexisting_on_target or []):
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            logger.warning(
                f"_apply_migration_to_db: preexisting snap not found: {snap_uuid}")
            continue
        # Only update the primary record for snaps owned by the migrating lvol.
        # Ancestor snaps owned by a different lvol already had their primary record
        # updated when that other lvol was migrated; we must not overwrite it here.
        if snap.lvol.uuid != migration.lvol_id:
            continue
        if snap.snap_bdev and '/' in snap.snap_bdev:
            src_lvstore, src_short = snap.snap_bdev.split('/', 1)
            if src_lvstore != tgt_node.lvstore:
                # Strip any leftover migration suffix (defensive)
                base = (src_short[:-len(_MIGRATION_BDEV_SUFFIX)]
                        if src_short.endswith(_MIGRATION_BDEV_SUFFIX) else src_short)
                snap.snap_bdev = f"{tgt_node.lvstore}/{base}"
        tgt_short = snap.snap_bdev.split('/', 1)[1] if '/' in snap.snap_bdev else None
        if tgt_short and tgt_short in spdk_info:
            snap.snap_uuid = spdk_info[tgt_short]['uuid']
            snap.blobid = spdk_info[tgt_short]['blobid']
        snap.lvol.node_id = tgt_node.get_id()
        snap.lvol.hostname = tgt_node.hostname
        snap.lvol.lvs_name = tgt_node.lvstore
        if tgt_lvol_bdev:
            snap.lvol.lvol_bdev = tgt_lvol_bdev
        snap.lvol.top_bdev = f"{tgt_node.lvstore}/{snap.lvol.lvol_bdev}"
        snap.lvol.nodes = list(lvol.nodes)
        snap.lvol.subsys_port = tgt_subsys_port
        if tgt_lvol_uuid:
            snap.lvol.lvol_uuid = tgt_lvol_uuid
        snap.write_to_db(db.kv_store)
        logger.debug(
            f"_apply_migration_to_db: updated preexisting snap {snap_uuid} "
            f"snap_bdev={snap.snap_bdev}")

    return True


def _snap_short_name(snap):
    """Return the bare bdev name for a snapshot, stripping any lvstore prefix."""
    path = snap.snap_bdev
    return path.split('/', 1)[1] if '/' in path else path


def _snap_tgt_short_name(snap):
    """Return the migration-target bdev short name for a snapshot.

    Always strips any existing migration suffix before adding one so that
    back-to-back migrations (where the source bdev already carries the suffix
    from the previous migration) do not produce a double suffix like 'SNAP_16745mm'.
    """
    short = _snap_short_name(snap)
    if short.endswith(_MIGRATION_BDEV_SUFFIX):
        short = short[:-len(_MIGRATION_BDEV_SUFFIX)]
    return short + _MIGRATION_BDEV_SUFFIX


def _lvol_tgt_bdev_name(lvol_bdev: str) -> str:
    """Return the migration-target bdev short name for a writable lvol.

    Thin wrapper around the shared utils.lvol_tgt_bdev_name so existing
    call-sites in this module don't need to change.
    """
    return utils.lvol_tgt_bdev_name(lvol_bdev)


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


def _delete_bdev_blocking(bdev_name, primary_rpc, secondary_rpc=None, tertiary_rpc=None,
                          timeout_s=120, coalescing=False):
    """
    Two-phase blocking bdev delete.

    Phase 1 — primary only, sync=False (del_async=False): initiates the async
      delete.  By default (coalescing=False) special_delete=True tells SPDK to
      free the bdev's own clusters without merging them into any child —
      correct for source cleanup, rollback, and any path where no child needs
      to inherit data.  Pass coalescing=True when the bdev's child must
      inherit its clusters instead (e.g. deleting a migration intermediate
      snapshot, where the surviving lvol/snap needs the merged data).
    Wait   — poll bdev_lvol_get_lvol_delete_status on primary until done.
    Phase 2 — all nodes (primary + secondary + tertiary), sync=True
      (del_async=True, special_delete=False): finalises the deletion on every replica.
    """
    ret, _ = primary_rpc.delete_lvol(bdev_name, del_async=False, special_delete=not coalescing)
    if not ret:
        raise RuntimeError(f"delete bdev {bdev_name}: initiation failed")

    deadline = time.monotonic() + timeout_s
    while primary_rpc.bdev_lvol_get_lvol_delete_status(bdev_name) == 1:
        if time.monotonic() > deadline:
            if not primary_rpc.get_bdevs(bdev_name):
                logger.warning(
                    f"delete bdev {bdev_name}: poll timed out after {timeout_s}s "
                    f"but bdev is gone — treating as success")
                break
            raise RuntimeError(
                f"delete bdev {bdev_name}: timed out after {timeout_s}s, bdev still present")
        time.sleep(0.2)

    for rpc in filter(None, [primary_rpc, secondary_rpc, tertiary_rpc]):
        try:
            rpc.delete_lvol(bdev_name, del_async=True, special_delete=False)
        except Exception as e:
            logger.warning(f"delete bdev {bdev_name} finalize on replica (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Secondary-node helpers
# ---------------------------------------------------------------------------

def _get_target_secondary_node(tgt_node, src_node_id):
    """
    Return ``(sec_node, error_string)`` describing how to handle the target's
    secondary node when creating a new object on the target primary.

    Rules (consistent with migration policy):
      - No secondary configured   → (None, None)   skip silently
      - Secondary STATUS_ONLINE   → (sec_node, None) register on secondary
      - Secondary STATUS_OFFLINE  → (None, None)   administratively down, skip
      - Secondary STATUS_SUSPENDED and node == src_node → (sec_node, None)
        overlap drain: source is being drained but is still the target's
        secondary; migration must continue through it
      - Any other status          → (None, err)    block creation on primary

    ``src_node_id`` must always be supplied so the overlap-drain case (SUSPENDED
    secondary that is also the migration source) is handled correctly.
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
    if sec.status == StorageNode.STATUS_SUSPENDED and src_node_id and sec.get_id() == src_node_id:
        return sec, None
    return None, (
        f"Target secondary node {tgt_node.secondary_node_id} is in state "
        f"'{sec.status}'; cannot create on target primary"
    )


def _get_target_tertiary_node(tgt_node, src_node_id):
    """
    Return ``(ter_node, error_string)`` describing how to handle the target's
    tertiary node when creating a new object on the target primary.

    Rules mirror _get_target_secondary_node:
      - No tertiary configured    → (None, None)   skip silently
      - Tertiary STATUS_ONLINE    → (ter_node, None) register on tertiary
      - Tertiary STATUS_OFFLINE   → (None, None)   administratively down, skip
      - Tertiary STATUS_SUSPENDED and node == src_node → (ter_node, None)
        overlap drain: source is being drained but is still the target's
        tertiary; migration must continue through it
      - Any other status          → (None, err)    block creation on primary

    ``src_node_id`` must always be supplied so the overlap-drain case is
    handled correctly.
    """
    if not tgt_node.tertiary_node_id:
        return None, None
    try:
        ter = db.get_storage_node_by_id(tgt_node.tertiary_node_id)
    except KeyError:
        return None, None
    if ter.status == StorageNode.STATUS_ONLINE:
        return ter, None
    if ter.status == StorageNode.STATUS_OFFLINE:
        return None, None
    if ter.status == StorageNode.STATUS_SUSPENDED and src_node_id and ter.get_id() == src_node_id:
        return ter, None
    return None, (
        f"Target tertiary node {tgt_node.tertiary_node_id} is in state "
        f"'{ter.status}'; cannot create on target primary"
    )


def _get_source_secondary_node(src_node):
    """
    Return the secondary node of src_node for source cleanup operations, or None.

    During source cleanup the secondary still holds replica data that must be
    deleted.  SUSPENDED is accepted because a draining node is still reachable
    via RPC; OFFLINE is skipped as unreachable.
    """
    if not src_node.secondary_node_id:
        return None
    try:
        sec = db.get_storage_node_by_id(src_node.secondary_node_id)
    except KeyError:
        return None
    if sec.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
        return sec
    return None


def _get_source_tertiary_node(src_node):
    """
    Return the tertiary node of src_node for source cleanup operations, or None.

    Mirrors _get_source_secondary_node: ONLINE or SUSPENDED are both accepted;
    OFFLINE is skipped.
    """
    if not src_node.tertiary_node_id:
        return None
    try:
        ter = db.get_storage_node_by_id(src_node.tertiary_node_id)
    except KeyError:
        return None
    if ter.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
        return ter
    return None



def _build_paths(src_node, tgt_node, src_rpc, tgt_rpc):
    """Build ordered path lists for source and target nodes and compute overlap.

    Returns (src_paths, tgt_paths, overlap_ids) where each path entry is:
        {'node', 'rpc', 'ip', 'trtype', 'port', 'node_id'}

    Port is role-specific: SRC entries use src_node.lvstore; TGT entries use
    tgt_node.lvstore.  Adding tertiary support = append one more entry to each
    list; all callers automatically handle it via loop/set operations.
    """
    def _entry(node, rpc, lvstore):
        trtype, ip = _get_migration_nic(node)
        return {
            'node': node, 'rpc': rpc, 'ip': ip, 'trtype': trtype,
            'port': node.get_lvol_subsys_port(lvstore),
            'node_id': node.get_id(),
        }

    src_paths = [_entry(src_node, src_rpc, src_node.lvstore)]
    if src_node.secondary_node_id:
        try:
            ss = db.get_storage_node_by_id(src_node.secondary_node_id)
            if ss.status == StorageNode.STATUS_ONLINE:
                src_paths.append(_entry(ss, _make_rpc(ss), src_node.lvstore))
        except KeyError:
            pass
    if src_node.tertiary_node_id:
        try:
            ts = db.get_storage_node_by_id(src_node.tertiary_node_id)
            if ts.status == StorageNode.STATUS_ONLINE:
                src_paths.append(_entry(ts, _make_rpc(ts), src_node.lvstore))
        except KeyError:
            pass

    tgt_paths = [_entry(tgt_node, tgt_rpc, tgt_node.lvstore)]
    tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
    if not sec_err and tgt_sec is not None:
        tgt_paths.append(_entry(tgt_sec, _make_rpc(tgt_sec), tgt_node.lvstore))
    tgt_ter, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
    if not ter_err and tgt_ter is not None:
        tgt_paths.append(_entry(tgt_ter, _make_rpc(tgt_ter), tgt_node.lvstore))

    overlap_ids = {p['node_id'] for p in src_paths} & {p['node_id'] for p in tgt_paths}
    return src_paths, tgt_paths, overlap_ids


def _swap_namespace(rpc, nqn, new_bdev, uuid, guid, label):
    """Remove the existing namespace from a subsystem and add a new one.

    Discovers the current nsid dynamically rather than assuming nsid=1.
    """
    s = rpc.subsystem_get(nqn)
    ns_list = s.get('namespaces', []) if s else []
    nsid = ns_list[0]['nsid'] if ns_list else 1
    try:
        rpc.nvmf_subsystem_remove_ns(nqn, nsid)
        logger.info(f"Swap NS {label}: removed nsid={nsid}")
    except Exception as e:
        logger.warning(f"Swap NS remove (non-fatal) on {label}: {e}")
    ret = rpc.nvmf_subsystem_add_ns(nqn, new_bdev, uuid, guid)
    if not ret:
        logger.error(f"Swap NS add failed on {label}")


# ---------------------------------------------------------------------------
# Target-restart reconciliation
# ---------------------------------------------------------------------------

_CNTLID_RANGES = {'primary': (3, 500), 'secondary': (1003, 1500), 'tertiary': (2003, 2500)}


def _target_role_label(node_id, tgt_node):
    if node_id == tgt_node.get_id():
        return 'primary'
    if node_id == tgt_node.secondary_node_id:
        return 'secondary'
    if node_id == tgt_node.tertiary_node_id:
        return 'tertiary'
    return 'unknown'


def _ensure_target_nvmf_state(migration, lvol, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Detect and repair a target-side node restart that wiped the migration's
    NVMe-oF subsystem / listener / namespace before cutover.

    A restart is inferred rather than tracked explicitly: every call re-checks
    that the subsystem, our TGT-port listener, and (on nodes whose subsystem
    lifecycle we own) the namespace are still present on the target primary
    and its online secondary/tertiary, recreating whatever SPDK lost.

    A recreated listener is always set to ana_state="inaccessible" — pre-cutover
    that already is the desired state, and post-final-migration
    _handle_lvol_migrate's Done handler unconditionally re-runs its ANA-flip
    sequence on every call, correcting it within the same tick.

    Overlap nodes (subsystem shared with the SRC role) never had their
    subsystem created by this migration — if that subsystem is entirely gone,
    something outside migration's control needs to fix it, so this only waits.

    Returns None if everything is fine (or was successfully repaired), or the
    _WAIT sentinel if the caller should suspend without charging the
    migration's retry budget.
    """
    nqn = lvol.nqn
    try:
        _, tgt_paths, _overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
    except Exception as e:
        logger.warning(f"_ensure_target_nvmf_state: could not build target paths: {e}")
        migration.error_message = f"target topology unavailable: {e}"
        return _WAIT

    owned_node_ids = set(migration.target_subsystem_node_ids or [])
    short_bdev = _lvol_tgt_bdev_name(lvol.lvol_bdev)
    ns_bdev_short = f"crypto_{short_bdev}" if lvol.crypto_bdev else short_bdev
    ns_composite = f"{tgt_node.lvstore}/{ns_bdev_short}"

    for path in tgt_paths:
        node_id = path['node_id']
        rpc = path['rpc']
        label = _target_role_label(node_id, tgt_node)
        owns_subsystem = node_id in owned_node_ids

        try:
            sub = rpc.subsystem_get(nqn)
        except Exception as e:
            logger.warning(
                f"_ensure_target_nvmf_state: subsystem_get failed on "
                f"{label} {node_id[:8]}: {e}")
            migration.error_message = f"could not query subsystem on {label} target node: {e}"
            return _WAIT

        if not sub:
            if not owns_subsystem:
                msg = (f"target {label} node {node_id[:8]} is missing subsystem "
                       f"{nqn} that this migration does not own; waiting for "
                       f"node recovery")
                logger.warning(f"_ensure_target_nvmf_state: {msg}")
                migration.error_message = msg
                return _WAIT

            logger.warning(
                f"_ensure_target_nvmf_state: subsystem {nqn} missing on "
                f"{label} target node {node_id[:8]} (likely node restart) "
                f"— recreating")
            try:
                lo, hi = _CNTLID_RANGES.get(label, (3, 500))
                rpc.subsystem_create(
                    nqn, lvol.ha_type, lvol.uuid, min_cntlid=random.randint(lo, hi),
                    max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS)
                if lvol.allowed_hosts:
                    _reapply_allowed_hosts(lvol, path['node'], rpc)
                rpc.listeners_create(nqn, path['trtype'], path['ip'], path['port'],
                                     ana_state="inaccessible")
                ns = rpc.nvmf_subsystem_add_ns(nqn, ns_composite, lvol.uuid, lvol.guid)
                if not ns:
                    logger.warning(
                        f"_ensure_target_nvmf_state: namespace add failed on "
                        f"{label} {node_id[:8]} after subsystem recreate")
                if node_id not in migration.target_subsystem_node_ids:
                    migration.target_subsystem_node_ids.append(node_id)
                logger.info(
                    f"_ensure_target_nvmf_state: recreated subsystem+listener+ns "
                    f"for migration {migration.uuid} on {label} {node_id[:8]}")
            except Exception as e:
                logger.warning(
                    f"_ensure_target_nvmf_state: recreate failed on {label} "
                    f"{node_id[:8]}: {e}")
                migration.error_message = f"failed to recreate target subsystem on {label} node: {e}"
                return _WAIT
            continue

        # Subsystem present — verify our listener survived.
        try:
            listeners = rpc.listeners_list(nqn) or []
        except Exception as e:
            logger.warning(
                f"_ensure_target_nvmf_state: listeners_list failed on "
                f"{label} {node_id[:8]}: {e}")
            migration.error_message = f"could not query listeners on {label} target node: {e}"
            return _WAIT

        listener_present = any(
            (ls.get('address', {}).get('traddr') == path['ip']
             and str(ls.get('address', {}).get('trsvcid')) == str(path['port']))
            for ls in listeners)

        if not listener_present:
            logger.warning(
                f"_ensure_target_nvmf_state: listener {path['ip']}:{path['port']} "
                f"missing on {label} target node {node_id[:8]} (likely node "
                f"restart) — recreating as inaccessible")
            try:
                rpc.listeners_create(nqn, path['trtype'], path['ip'], path['port'],
                                     ana_state="inaccessible")
            except Exception as e:
                logger.warning(
                    f"_ensure_target_nvmf_state: listener recreate failed on "
                    f"{label} {node_id[:8]}: {e}")
                migration.error_message = f"failed to recreate target listener on {label} node: {e}"
                return _WAIT

        # Namespace check — only on nodes whose namespace lifecycle we own;
        # overlap nodes legitimately still point at the SRC bdev pre-cutover
        # (the namespace swap is _handle_lvol_migrate's job, not ours).
        if owns_subsystem:
            ns_list = sub.get('namespaces', []) if isinstance(sub, dict) else []
            has_ns = any(ns.get('uuid') == lvol.uuid for ns in ns_list)
            if not has_ns:
                logger.warning(
                    f"_ensure_target_nvmf_state: namespace for {lvol.uuid} "
                    f"missing on {label} target node {node_id[:8]} — re-adding")
                try:
                    ns = rpc.nvmf_subsystem_add_ns(nqn, ns_composite, lvol.uuid, lvol.guid)
                    if not ns:
                        logger.warning(
                            f"_ensure_target_nvmf_state: namespace re-add failed "
                            f"on {label} {node_id[:8]}")
                except Exception as e:
                    logger.warning(
                        f"_ensure_target_nvmf_state: namespace re-add errored on "
                        f"{label} {node_id[:8]}: {e}")
                    migration.error_message = f"failed to re-add namespace on {label} node: {e}"
                    return _WAIT

    return None


# ---------------------------------------------------------------------------
# Transfer-context cleanup helpers
# ---------------------------------------------------------------------------


def _cleanup_final_migration(src_rpc, ctx, tgt_rpc=None, rollback_target=False,
                             tgt_sec_rpc=None, tgt_ter_rpc=None,
                             nqn=None, lvol_uuid=None, subsystem_created_on_target=False):
    """Clean up after a final lvol migration attempt.

    On the success path (rollback_target=False) the hub controller is kept
    attached on source — detaching it would drop the migration path before
    clients have switched to the new target path.

    On the rollback path (rollback_target=True) the hub controller IS detached
    and the target lvol/subsystem are torn down so a retry starts clean.

    ``nqn``/``lvol_uuid``/``subsystem_created_on_target`` must come from the
    caller (the lvol record and migration.target_subsystem_node_ids) —
    transfer_context never carries an nqn/ns_id/ownership entry for this
    stage, so reading them from ``ctx`` here silently no-ops the subsystem
    cleanup entirely.
    """
    ctrl_name = ctx.get('ctrl_name')
    if ctrl_name and rollback_target:
        try:
            src_rpc.bdev_nvme_detach_controller(ctrl_name)
        except Exception as e:
            logger.warning(f"detach hub ctrl {ctrl_name}: {e}")

    if rollback_target and tgt_rpc:
        tgt_composite = ctx.get('tgt_lvol_composite')
        _nqn = ctx.get('nqn') or nqn
        if _nqn and lvol_uuid:
            try:
                migration_controller.cleanup_subsystem_or_ns(_nqn, lvol_uuid, subsystem_created_on_target, tgt_rpc)
            except Exception as e:
                logger.warning(f"cleanup target subsystem {_nqn}: {e}")
        if tgt_composite and tgt_rpc.get_bdevs(tgt_composite):
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite}: {e}")


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------


def _setup_snap_transfer(snap, snap_index, src_node, tgt_node,
                         src_rpc, tgt_rpc, trtype,
                         tgt_sec=None, sec_rpc=None, tgt_ter=None, ter_rpc=None,
                         lvol_size_mib=None):
    """
    Prepare a single snapshot for async transfer:
      1. Create writable lvol on target primary
      2. Register on target secondary/tertiary immediately (keeps HA nodes consistent)
      3. Set migration flag on primary
      4. Get map_id of target bdev for hub-based transfer
      5. Ensure hub NVMe-oF controller is attached on source
      6. Fire bdev_lvol_transfer via hub (async)

    Returns a transfer-dict on success or (None, error_string) on failure.
    Callers are responsible for rolling back any previously launched transfers.
    """
    snap_uuid = snap.uuid
    snap_short = _snap_tgt_short_name(snap)
    src_composite = _snap_composite(src_node.lvstore, snap)
    tgt_composite = f"{tgt_node.lvstore}/{snap_short}"

    # Step 1: create target lvol on primary.
    # The target bdev must cover the FULL logical address range of the source snap
    # (= parent lvol total size).  snap.size is only the blob's own allocated
    # clusters; using it causes LBA-out-of-range when the transfer reads CoW data
    # from the parent chain.  Callers pass lvol_size_mib from _bytes_to_mib(lvol.size).
    size_in_mib = lvol_size_mib if lvol_size_mib else _bytes_to_mib(snap.size)
    logger.info(
        f"[SNAP SIZE] snap={snap_uuid[:8]} snap.size={snap.size} "
        f"size_in_mib={size_in_mib} (lvol_size_mib={lvol_size_mib})"
    )
    _log_spdk_bdev_size(src_rpc, src_composite, f"SRC snap[{snap_uuid[:8]}] pre-create")
    # snap.lvol is the embedded copy of the owning lvol captured at snapshot
    # creation time; it should always be populated but we degrade gracefully
    # for legacy/corrupt records by falling back to pool defaults (0 = use
    # the pool's default replication scheme / priority class) rather than
    # failing the migration outright.
    snap_lvol = snap.lvol
    _ndcs = snap_lvol.ndcs if snap_lvol else 0
    _npcs = snap_lvol.npcs if snap_lvol else 0
    _priority_class = snap_lvol.lvol_priority_class if snap_lvol else 0
    ret = tgt_rpc.create_lvol(snap_short, size_in_mib, tgt_node.lvstore, ndcs=_ndcs, npcs=_npcs)
    if not ret:
        return None, f"Failed to create target lvol for snap {snap_uuid}"
    _log_spdk_bdev_size(tgt_rpc, tgt_composite, f"TGT snap[{snap_uuid[:8]}] post-create")

    # Step 2: register on secondary immediately so secondary stays consistent.
    # If registration fails we clean up the primary bdev and abort — continuing
    # with an unregistered secondary would leave the cluster in split state.
    sec_registered = False
    ter_registered = False
    if tgt_sec and sec_rpc:
        bdev_info = tgt_rpc.get_bdevs(tgt_composite)
        if not bdev_info:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
            return None, f"Could not get bdev info for {tgt_composite} after creation"
        snap_blobid = bdev_info[0]['driver_specific']['lvol']['blobid']
        snap_uuid_on_tgt = bdev_info[0]['uuid']
        ret_sec = sec_rpc.bdev_lvol_register(
            snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
            _priority_class)
        if not ret_sec:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
            return None, f"bdev_lvol_register on secondary failed for snap {snap_uuid}"
        sec_registered = True
        if tgt_ter and ter_rpc:
            ret_ter = ter_rpc.bdev_lvol_register(
                snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
                _priority_class)
            if not ret_ter:
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc, ter_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
                return None, f"bdev_lvol_register on tertiary failed for snap {snap_uuid}"
            ter_registered = True

    # Helper: clean primary, secondary, and tertiary (if registered) on error
    def _cleanup():
        try:
            _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                  secondary_rpc=sec_rpc if sec_registered else None,
                                  tertiary_rpc=ter_rpc if ter_registered else None)
        except Exception as e:
            logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")

    # Step 3: migration flag on primary
    ret = tgt_rpc.bdev_lvol_set_migration_flag(tgt_composite)
    if not ret:
        _cleanup()
        return None, f"bdev_lvol_set_migration_flag failed for snap {snap_uuid}"

    # Step 4: get map_id of target bdev — used by bdev_lvol_transfer to route
    # data through the hub instead of a per-snap temp NVMe-oF subsystem.
    lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore)
    tgt_map_id = None
    for entry in (lvols_list or []):
        entry_name = entry.get('name', '') or entry.get('lvol_name', '')
        if entry_name in (snap_short, tgt_composite):
            tgt_map_id = entry.get('map_id')
            break
    if tgt_map_id is None:
        _cleanup()
        return None, f"Could not get map_id for snap {snap_uuid} on target"

    # Step 5: ensure hub controller is attached on source (shared across all
    # snapshot transfers; created once, reused by PHASE_LVOL_MIGRATE, released
    # in CLEANUP_SOURCE and detached lazily by HubControllerManager).
    _, hub_bdev, hub_err = hub_manager.acquire(
        src_node.get_id(), src_rpc, tgt_node, trtype)
    if hub_err:
        _cleanup()
        return None, hub_err

    # Step 6: fire async transfer via hub
    ret = src_rpc.bdev_lvol_transfer(src_composite, 0, 16, hub_bdev, "migrate", lvol_id=tgt_map_id)
    if ret is None:
        _cleanup()
        return None, f"bdev_lvol_transfer failed for snap {snap_uuid}"

    return {
        'snap_uuid': snap_uuid,
        'snap_short': snap_short,
        'snap_index': snap_index,
        'transfer_done': False,
        'post_done': False,
    }, None


def _post_process_snap(snap: SnapShot, tgt_node: StorageNode, tgt_rpc: RPCClient, migration: LVolMigration,
                       transfer: dict, tgt_sec:Optional[StorageNode]=None, sec_rpc: Optional[RPCClient]=None,
                       tgt_ter:Optional[StorageNode]=None, ter_rpc: Optional[RPCClient]=None):
    """
    Post-transfer steps for a single snapshot whose data has been fully copied:
      add_clone → convert (on primary, then mirrored on secondary) → cleanup.

    Mutates ``migration.snaps_migrated`` and fires migration events on success.
    Returns (ok: bool, error: str|None).
    """
    snap_uuid = snap.uuid
    snap_short = transfer['snap_short']
    tgt_composite = f"{tgt_node.lvstore}/{snap_short}"

    # Link to predecessor snapshot in target's ancestry chain.
    # add_clone must succeed on BOTH primary and secondary before we convert
    # either — once the convert runs, the lvol is immutable and cannot be re-linked.
    pred_uuid = None
    for snap_rec in migration_controller.get_snapshot_chain(migration.lvol_id):
        if snap_rec == snap_uuid:
            break
        pred_uuid = snap_rec

    if pred_uuid:
        if pred_uuid not in migration.snaps_migrated+migration.snaps_preexisting_on_target:
            return False, f"Predecessor {pred_uuid} not in migration chain"

        try:
            pred_snap = db.get_snapshot_by_id(pred_uuid)
            # For predecessors migrated as part of THIS migration, the bdev was
            # created with the migration suffix (_m) and not yet renamed — build
            # the composite from source short name + suffix.
            # For predecessors already on TGT from a PRIOR migration (preexisting),
            # the bdev carries its canonical name (no suffix).  Two sub-cases:
            #   a) TGT is the snap's home node → snap.snap_bdev already has the
            #      correct lvstore prefix and canonical name (e.g. round-trip back).
            #   b) TGT is a non-home node → canonical name is in snap.instances.
            if pred_uuid in (migration.snaps_preexisting_on_target or []):
                pred_short = None
                _lvstore_prefix = tgt_node.lvstore + '/'
                # (a) home-node case: snap_bdev already has TGT lvstore
                _snap_bdev = pred_snap.snap_bdev or ''
                if _snap_bdev.startswith(_lvstore_prefix):
                    pred_short = _snap_bdev.split('/', 1)[1]
                else:
                    # (b) non-home: look for a TGT instance entry
                    for _inst in pred_snap.instances:
                        _inst_bdev = _inst.get('snap_bdev', '')
                        if _inst_bdev.startswith(_lvstore_prefix):
                            pred_short = _inst_bdev.split('/', 1)[1]
                            break
                if not pred_short:
                    pred_short = _snap_tgt_short_name(pred_snap)
                    logger.warning(
                        f"bdev_lvol_add_clone: no TGT bdev found for preexisting "
                        f"predecessor {pred_uuid}; using computed name {pred_short!r}")
            else:
                pred_short = _snap_tgt_short_name(pred_snap)
            pred_composite = f"{tgt_node.lvstore}/{pred_short}"
            ret = tgt_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
            if not ret:
                return False, f"bdev_lvol_add_clone failed for {snap_uuid}"
            if tgt_sec and sec_rpc:
                ret_sec = sec_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
                if not ret_sec:
                    return False, f"bdev_lvol_add_clone on secondary failed for {snap_uuid}"
            if tgt_ter and ter_rpc:
                ret_ter = ter_rpc.bdev_lvol_add_clone(tgt_composite, pred_composite)
                if not ret_ter:
                    return False, f"bdev_lvol_add_clone on tertiary failed for {snap_uuid}"
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
    if tgt_ter and ter_rpc:
        ret_ter = ter_rpc.bdev_lvol_convert(tgt_composite)
        if not ret_ter:
            return False, f"bdev_lvol_convert on tertiary failed for {snap_uuid}"

    # Early partial DB update: route health-check and delete to the target node
    # immediately after convert.  snap_bdev keeps its source path here; the full
    # update (with migration suffix and all other fields) happens in
    # apply_migration_to_db() at the end of CLEANUP_SOURCE.
    try:
        if snap_uuid in migration.snaps_migrated:
            snap_rec = db.get_snapshot_by_id(snap_uuid)
            if snap_rec.lvol.uuid == migration.lvol_id:
                snap_rec.lvol.node_id = tgt_node.get_id()
                snap_rec.write_to_db(db.kv_store)
    except KeyError:
        logger.warning(f"Snapshot {snap_uuid} not found in DB for early node update")

    migration.snaps_migrated.append(snap_uuid)
    if snap_uuid not in migration.snaps_preexisting_on_target:
        tgt_bdev_path = f"{tgt_node.lvstore}/{_snap_tgt_short_name(snap)}"
        if tgt_bdev_path not in migration.target_snap_bdevs:
            migration.target_snap_bdevs.append(tgt_bdev_path)
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
    trtype, _ = _get_migration_nic(tgt_node)
    ctx = migration.transfer_context or {}

    # Snap bdevs on TGT must cover the full logical address range of the lvol,
    # not just each snap's own allocated clusters.
    try:
        _lvol_for_size = db.get_lvol_by_id(migration.lvol_id)
        _snap_lvol_size_mib = _bytes_to_mib(_lvol_for_size.size)
    except KeyError:
        _snap_lvol_size_mib = None

    tgt_sec = None
    sec_rpc = None
    tgt_ter = None
    ter_rpc = None

    # ── PRE-SCAN: mark snapshots already on target as pre-existing ────────────
    # Query the target lvstore once. Any planned snap whose target bdev already
    # exists AND is immutable (is_snapshot=true in SPDK) was fully converted by
    # a prior migration — mark pre-existing so we skip the transfer entirely.
    # We use the SPDK is_snapshot flag rather than the DB snap_bdev field because
    # snap_bdev is not updated until apply_migration_to_db() runs at the very end
    # of CLEANUP_SOURCE; a migration that succeeded at SNAP_COPY but failed later
    # would leave immutable snapshots on the target with stale DB records.
    # Writable bdevs (is_snapshot=false) are leftovers from a crashed transfer —
    # they fall through to the per-snap pre-cleanup to be deleted and retried.
    if ctx.get('stage') != 'parallel_transfer' and plan:
        try:
            _tgt_lvols = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
            _tgt_immutable = {
                e.get('name', '').split('/')[-1]
                for e in _tgt_lvols
                if e.get('is_snapshot', False)
            }
        except Exception as _pre_e:
            logger.warning(f"Pre-scan: bdev_lvol_get_lvols failed ({_pre_e}); skipping")
            _tgt_immutable = set()

        _pre_scan_updated = False
        for _snap_uuid in plan:
            if (_snap_uuid in migration.snaps_migrated
                    or _snap_uuid in migration.snaps_preexisting_on_target):
                continue
            try:
                _s = db.get_snapshot_by_id(_snap_uuid)
            except KeyError:
                continue
            _short_tgt = _snap_tgt_short_name(_s)          # SNAP_Xm  (in-flight)
            _short_canonical = _snap_short_name(_s)         # SNAP_X   (post-rename)
            _short_am = _short_canonical + _MIGRATION_BDEV_SUFFIX_DONE  # SNAP_Xam (fallback)
            _found_as = next(
                (n for n in (_short_tgt, _short_canonical, _short_am)
                 if n in _tgt_immutable),
                None)
            if _found_as:
                logger.info(
                    f"Pre-scan: {_snap_uuid} ({_found_as}) is already an immutable "
                    f"snapshot on target; marking pre-existing")
                migration.snaps_preexisting_on_target.append(_snap_uuid)
                _pre_scan_updated = True
        if _pre_scan_updated:
            migration.write_to_db(db.kv_store)

    # ── A. Launch / resume planned snapshots one at a time ───────────────────
    # SPDK only supports one bdev_lvol_transfer per poller group at a time;
    # launching multiple causes "poller already exists" and stuck transfers.
    _PARALLEL_BATCH = 1
    if ctx.get('stage') != 'parallel_transfer':
        all_unprocessed = [u for u in plan
                           if u not in migration.snaps_migrated
                           and u not in migration.snaps_preexisting_on_target]
        unprocessed = all_unprocessed[:_PARALLEL_BATCH]

        if unprocessed:
            # HA secondary/tertiary gate – check once; all snaps belong to the same volume
            for snap_uuid in unprocessed:
                try:
                    snap = db.get_snapshot_by_id(snap_uuid)
                except KeyError:
                    return False, True, f"Snapshot {snap_uuid} not found in DB"
                if snap.lvol.ha_type == "ha":
                    tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
                    if sec_err:
                        migration.error_message = sec_err
                        migration.write_to_db(db.kv_store)
                        return False, True, _WAIT
                    if tgt_sec:
                        sec_rpc = _make_rpc(tgt_sec)
                elif snap.lvol.ha_type == "ha3":
                    tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
                    if sec_err:
                        migration.error_message = sec_err
                        migration.write_to_db(db.kv_store)
                        return False, True, _WAIT
                    if tgt_sec:
                        sec_rpc = _make_rpc(tgt_sec)
                    tgt_ter, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
                    if ter_err:
                        migration.error_message = ter_err
                        migration.write_to_db(db.kv_store)
                        return False, True, _WAIT
                    if tgt_ter:
                        ter_rpc = _make_rpc(tgt_ter)
                break  # one check is enough

            transfers: list[dict] = []
            for snap_uuid in unprocessed:
                snap_index = plan.index(snap_uuid)
                try:
                    snap = db.get_snapshot_by_id(snap_uuid)
                except KeyError:
                    return False, True, f"Snapshot {snap_uuid} not found in DB"

                snap_short_tgt = _snap_tgt_short_name(snap)
                src_composite = _snap_composite(src_node.lvstore, snap)
                tgt_composite = f"{tgt_node.lvstore}/{snap_short_tgt}"

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
                        'transfer_done': False,
                        'post_done': False,
                    })
                    continue

                # Pre-existing (immutable) bdevs were caught by the pre-scan above and
                # excluded from unprocessed. Anything still found here is a writable
                # leftover from a previous failed attempt — delete and retry.
                if tgt_rpc.get_bdevs(tgt_composite):
                    logger.info(
                        f"Removing writable leftover target bdev {tgt_composite}")
                    try:
                        _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc, ter_rpc)
                        for _ in range(10):
                            if not tgt_rpc.get_bdevs(tgt_composite):
                                break
                            time.sleep(0.2)
                    except Exception as e:
                        logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")

                t, err = _setup_snap_transfer(
                    snap, snap_index, src_node, tgt_node,
                    src_rpc, tgt_rpc, trtype,
                    tgt_sec=tgt_sec, sec_rpc=sec_rpc,
                    tgt_ter=tgt_ter, ter_rpc=ter_rpc,
                    lvol_size_mib=_snap_lvol_size_mib)
                if t is None:
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
        tgt_sec, _sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
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
                migration.transfer_context = {}
                migration.write_to_db(db.kv_store)
                return False, True, f"Snapshot {snap_uuid} disappeared during transfer"

            src_composite = _snap_composite(src_node.lvstore, snap)

            # Update transfer-done status for this entry
            if not t['transfer_done']:
                result = src_rpc.bdev_lvol_transfer_stat(src_composite)
                if result is None:
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
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, f"Snapshot transfer {state} for {snap_uuid}"

                t['transfer_done'] = True

            # Transfer done.  Post-process only if predecessor is also done.
            if not prev_post_done:
                all_done = False
                continue

            ok, err = _post_process_snap(
                snap, tgt_node, tgt_rpc, migration, t,
                tgt_sec=tgt_sec, sec_rpc=sec_rpc,
                tgt_ter=tgt_ter, ter_rpc=ter_rpc)
            if not ok:
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
        remaining = [u for u in plan
                     if u not in migration.snaps_migrated
                     and u not in migration.snaps_preexisting_on_target]
        if remaining:
            return False, False, None

    # ── C. Intermediate ("shrink") snapshots – busy-poll within this call ────
    # These snapshots capture only the delta written since the last planned snap.
    # They should be small and complete quickly; we spin rather than returning to
    # the service loop so that LVOL_MIGRATE starts with minimal latency.
    #
    # Before each round check the current dirty delta.  If it is already below
    # the threshold the remaining freeze window will be short enough that no
    # additional shrink pass is worth the overhead.
    while migration.intermediate_snap_rounds < migration.max_intermediate_snap_rounds:
        _lvol = db.get_lvol_by_id(migration.lvol_id)
        _src_composite = f"{src_node.lvstore}/{_lvol.lvol_bdev}"
        _delta = _get_lvol_delta_bytes(src_rpc, _src_composite)
        _threshold = constants.LVOL_MIG_INTERMEDIATE_SNAP_THRESHOLD_BYTES
        if migration.intermediate_snap_rounds > 0 and _delta is not None and _delta <= _threshold:
            logger.info(
                f"Intermediate snapshot skipped: delta {convert_size(_delta, 'MiB')} MiB "
                f"<= {convert_size(_threshold, 'MiB')} MiB threshold "
                f"(round {migration.intermediate_snap_rounds}/{migration.max_intermediate_snap_rounds})")
            break
        _reason = (
            "forced (round 0)" if migration.intermediate_snap_rounds == 0
            else f"delta {'unknown' if _delta is None else str(convert_size(_delta, 'MiB')) + ' MiB'} "
                 f"exceeds {convert_size(_threshold, 'MiB')} MiB threshold"
        )
        logger.info(f"Intermediate snapshot triggered: {_reason}")
        _take_intermediate_snapshot(migration)
        plan = migration.snap_migration_plan
        if not plan:
            return False, True, "Intermediate snapshot failed"
        snap_uuid = plan[-1]
        snap_index = len(plan) - 1

        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            return False, True, f"Intermediate snapshot {snap_uuid} not found in DB"

        tgt_sec = None
        sec_rpc = None
        tgt_ter = None
        ter_rpc = None
        if snap.lvol.ha_type in ("ha", "ha3"):
            tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
            if sec_err:
                migration.error_message = sec_err
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT
            if tgt_sec:
                sec_rpc = _make_rpc(tgt_sec)
        if snap.lvol.ha_type == "ha3":
            tgt_ter, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
            if ter_err:
                migration.error_message = ter_err
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT
            if tgt_ter:
                ter_rpc = _make_rpc(tgt_ter)

        snap_short_tgt = _snap_tgt_short_name(snap)
        src_composite  = _snap_composite(src_node.lvstore, snap)
        tgt_composite  = f"{tgt_node.lvstore}/{snap_short_tgt}"

        # Pre-cleanup: if a bdev exists on the target it is a writable leftover
        # from a previous crashed run — intermediate snaps are always freshly
        # created by this migration so they can never be pre-existing.
        if tgt_rpc.get_bdevs(tgt_composite):
            logger.info(f"Pre-cleanup: removing stale intermediate bdev {tgt_composite}")
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc)
                for _ in range(10):
                    if not tgt_rpc.get_bdevs(tgt_composite):
                        break
                    time.sleep(0.2)
            except Exception as e:
                logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")

        t, err = _setup_snap_transfer(
            snap, snap_index, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc,
            tgt_ter=tgt_ter, ter_rpc=ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib)
        if t is None:
            return False, True, err

        logger.info(
            f"Started intermediate snap transfer: {snap_uuid} "
            f"({src_composite} -> {tgt_composite})")

        # Busy-poll: spin at _INTERMEDIATE_POLL_INTERVAL_S until done or timeout
        for _ in range(_INTERMEDIATE_POLL_MAX):
            result = src_rpc.bdev_lvol_transfer_stat(src_composite)
            if result is None:
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                          secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target snap {tgt_composite} (non-fatal): {e}")
                return False, True, (
                    f"Transfer stat failed for intermediate snap {snap_uuid}")
            state = result.get('transfer_state', 'No process')
            if state == 'Done':
                break
            if state in ('Failed', 'No process'):
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                          secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target snap {tgt_composite} (non-fatal): {e}")
                return False, True, (
                    f"Intermediate snap transfer {state} for {snap_uuid}")
            time.sleep(_INTERMEDIATE_POLL_INTERVAL_S)
        else:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc)
            except Exception as e:
                logger.warning(f"cleanup target snap {tgt_composite} (non-fatal): {e}")
            return False, True, (
                f"Intermediate snap transfer timed out for {snap_uuid}")

        ok, err = _post_process_snap(
            snap, tgt_node, tgt_rpc, migration, t,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc,
            tgt_ter=tgt_ter, ter_rpc=ter_rpc)
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


def _get_lvol_delta_bytes(src_rpc, composite_name):
    """
    Return the number of bytes currently allocated on the live lvol since its
    last snapshot (the dirty delta that would be frozen by bdev_lvol_final_migration).

    Uses num_allocated_clusters from bdev_get_bdevs multiplied by the lvstore
    cluster_size returned by bdev_lvol_get_lvstores.  Returns None on any RPC
    failure so callers can treat an unknown delta conservatively.
    """
    try:
        info = src_rpc.get_bdevs(composite_name)
        if not info:
            return None
        lvol_data = info[0].get('driver_specific', {}).get('lvol', {})
        num_alloc = lvol_data.get('num_allocated_clusters')
        if num_alloc is None:
            return None
        lvs_name = lvol_data.get('lvs_name') or composite_name.split('/')[0]
        lvs_info = src_rpc.bdev_lvol_get_lvstores(lvs_name)
        if not lvs_info:
            return None
        cluster_size = lvs_info[0].get('cluster_size', 0)
        if not cluster_size:
            return None
        return num_alloc * cluster_size
    except Exception:
        return None


def _take_intermediate_snapshot(migration):
    """
    Take an additional "shrink" snapshot from the live lvol on the source node
    to reduce the delta that must be frozen during PHASE_LVOL_MIGRATE.
    """
    snap_name = f"_mig_{migration.uuid[:8]}_r{migration.intermediate_snap_rounds}"
    logger.info(
        f"[IO-FREEZE] {_now_ms()} intermediate snapshot starting: "
        f"lvol={migration.lvol_id} round={migration.intermediate_snap_rounds} name={snap_name}")
    snap_uuid, err = snapshot_controller.add(
        migration.lvol_id, snap_name, bypass_migration_check=True)
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
    immediately rebuilds the TGT NVMe-oF subsystem and applies the DB records
    so clients polling migration status see the TGT endpoints at cutover time.

    Note: apply_migration_to_db() is NOT called here; it is deferred to the end
    of PHASE_CLEANUP_SOURCE after source snap deletion is complete.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
    except KeyError as e:
        return False, True, str(e)

    trtype, _ = _get_migration_nic(tgt_node)
    src_lvol_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"
    tgt_lvol_bdev = _lvol_tgt_bdev_name(lvol.lvol_bdev)
    tgt_lvol_composite = f"{tgt_node.lvstore}/{tgt_lvol_bdev}"
    ctx = migration.transfer_context or {}
    tgt_sec, _ = _get_target_secondary_node(tgt_node, src_node.get_id())
    tgt_sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, src_node.get_id())
    tgt_ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None
    nqn = lvol.nqn

    # Build topology-aware path lists once up front — used both to pull SRC
    # secondary/tertiary out of the read path before the freeze below, and by
    # the Done handler's ANA + namespace-swap sequence after it.
    # overlap_ids: nodes that appear in BOTH source and target paths — they
    # already have a subsystem (from SRC role); their namespace is swapped in
    # the Done handler's step 4.
    src_paths, tgt_paths, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
    src_replica_paths = src_paths[1:]  # secondary/tertiary only; primary stays live until cutover

    def _flip(rpc, ip, port, trtype, state, label):
        try:
            rpc.nvmf_subsystem_listener_set_ana_state(
                nqn, ip, port, trtype=trtype, ana=state)
            logger.info(f"ANA {nqn} {label} {ip}:{port} → {state}")
            return True
        except Exception as e:
            logger.error(f"ANA {label} failed: {e}")
            return False

    def _flip_required(rpc, ip, port, trtype, state, label, attempts=3):
        for i in range(attempts):
            if _flip(rpc, ip, port, trtype, state, label):
                return True
            if i < attempts - 1:
                time.sleep(1.0)
        return False

    def _revert_src_replicas(reason):
        # Final migration didn't complete — put SRC secondary/tertiary back
        # into the read path (their pre-freeze state) so clients keep
        # multipath access to the still-live source instead of being stuck
        # on primary only.
        if not src_replica_paths:
            return
        logger.warning(
            f"{reason}; reverting SRC secondary/tertiary to non_optimized: "
            f"lvol={lvol.uuid}")
        for p in src_replica_paths:
            _flip(p['rpc'], p['ip'], p['port'], p['trtype'],
                  "non_optimized", f"SRC-{p['node_id'][:8]}(revert)")

    # --- Crash recovery: Done handler was interrupted mid-run ---
    # bdev_lvol_final_migration is synchronous — it blocks until SPDK completes.
    # If we re-enter with stage='transfer' the migration already finished; check
    # stat once to detect the rare SPDK-side failure, then re-run Done handler.
    if ctx.get('stage') == 'transfer':
        try:
            result = src_rpc.bdev_lvol_transfer_stat(src_lvol_composite)
        except Exception:
            # Same reasoning as the fresh-attempt path below: the pre-freeze
            # flip already happened in a prior call before the crash/restart
            # this is recovering from, so a raise here must still revert it.
            _revert_src_replicas("final migration status check failed (crash recovery)")
            raise
        if not result:
            # Falsy covers both a hard None (RPC/connection error) and the
            # malformed-but-200 empty body a target restart can produce mid-RPC
            # (rpc_client._request2 falls back to returning raw response bytes,
            # e.g. b'', when json decoding fails) — neither is a valid stat dict.
            _revert_src_replicas("final migration status unavailable (crash recovery)")
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True,
                                     tgt_sec_rpc=tgt_sec_rpc, tgt_ter_rpc=tgt_ter_rpc,
                                     nqn=lvol.nqn, lvol_uuid=lvol.uuid,
                                     subsystem_created_on_target=(
                                         tgt_node.get_id() in (migration.target_subsystem_node_ids or [])))
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, "bdev_lvol_transfer_stat returned None (crash recovery)"
        state = result.get('transfer_state', 'No process')
        if state == 'Failed':
            _revert_src_replicas("final migration failed (crash recovery)")
            _cleanup_final_migration(src_rpc, ctx, tgt_rpc, rollback_target=True,
                                     tgt_sec_rpc=tgt_sec_rpc, tgt_ter_rpc=tgt_ter_rpc,
                                     nqn=lvol.nqn, lvol_uuid=lvol.uuid,
                                     subsystem_created_on_target=(
                                         tgt_node.get_id() in (migration.target_subsystem_node_ids or [])))
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
            _, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
            if sec_err:
                migration.error_message = sec_err
                migration.write_to_db(db.kv_store)
                return False, True, _WAIT

        # --- Start the final migration ---

        # Step 1: create writable target lvol (size in MiB).
        # Idempotent: create_migration() may have already created the bdev.
        # Note: SPDK's bdev_lvol_create 'uuid' param is for the lvol *store*, not
        # the new lvol.  Do not pass the lvol UUID here.
        lvol_size_in_mib = _bytes_to_mib(lvol.size)
        logger.info(
            f"[MIGRATION SIZE CHECK] lvol={lvol.lvol_bdev} "
            f"source_size_bytes={lvol.size} target_size_mib={lvol_size_in_mib}"
        )
        _log_spdk_bdev_size(src_rpc, src_lvol_composite, f"SRC lvol[{lvol.lvol_bdev}] pre-create")
        _log_spdk_bdev_size(tgt_rpc, tgt_lvol_composite, f"TGT lvol[{lvol.lvol_bdev}] post-create")

        # Step 1b: query map_id / blobid / uuid — needed for secondary registration
        # and for bdev_lvol_final_migration.  Do this once here rather than again
        # after NVMe-oF setup to keep secondary state consistent from the start.
        lvols_list = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore)
        if not lvols_list:
            return False, True, "bdev_lvol_get_lvols returned empty result from target"

        tgt_map_id = None
        tgt_uuid = None
        for entry in lvols_list:
            entry_name = entry.get('name', '') or entry.get('lvol_name', '')
            if entry_name in (tgt_lvol_bdev, tgt_lvol_composite):
                tgt_map_id = entry.get('map_id')
                tgt_uuid = entry.get('uuid')
                break

        if tgt_map_id is None:
            return False, True, f"Could not find map_id for {lvol.lvol_bdev} on target"

        logger.info(f"[MAP_ID] {tgt_lvol_bdev} map_id={tgt_map_id} uuid={tgt_uuid} on {tgt_node.get_id()[:8]}")

        # NVMe-oF subsystem setup is deferred to the Done handler — the subsystem
        # is deleted and recreated fresh after transfer completes so all paths get
        # a clean primary-port subsystem (min_cntlid=2000).

        # Step 3: connect source to target migration hub lvol
        ctrl_name, hub_bdev, hub_err = hub_manager.acquire(
            src_node.get_id(), src_rpc, tgt_node, trtype)
        if hub_err:
            # Do NOT delete the target bdev on hub error — it is unrelated to
            # the hub connection and deleting it forces a recreate on retry,
            # which changes its map_id and breaks concurrent migration tracking.
            return False, True, hub_err

        # Step 4: locate the last migrated snapshot's composite name on the target.
        # At least one intermediate snapshot is always taken (round 0 is unconditional),
        # so snaps_migrated is non-empty in the normal path.  snaps_preexisting_on_target
        # covers the case where a prior migration already placed immutable snapshots.
        tgt_snap_composite = ""
        if migration.snaps_migrated:
            last_snap_uuid = migration.snaps_migrated[-1]
            try:
                last_snap = db.get_snapshot_by_id(last_snap_uuid)
            except KeyError:
                src_rpc.bdev_nvme_detach_controller(ctrl_name)
                try:
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc,
                                          secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target lvol {tgt_lvol_composite} (non-fatal): {e}")
                return False, True, f"Last snapshot {last_snap_uuid} not found"
            tgt_snap_composite = f"{tgt_node.lvstore}/{_snap_tgt_short_name(last_snap)}"
        elif migration.snaps_preexisting_on_target:
            last_snap_uuid = migration.snaps_preexisting_on_target[-1]
            last_snap = db.get_snapshot_by_id(last_snap_uuid)
            if last_snap.lvol.node_id == tgt_node.get_id():
                tgt_snap_composite = last_snap.snap_bdev
            else:
                for instance in (last_snap.instances or []):
                    if not instance:
                        continue
                    lvol_info = instance.get("lvol") or {}
                    if lvol_info.get("node_id") == tgt_node.get_id():
                        snap_bdev = instance.get("snap_bdev")
                        if isinstance(snap_bdev, str):
                            tgt_snap_composite = snap_bdev
                        break
            if not tgt_snap_composite:
                src_rpc.bdev_nvme_detach_controller(ctrl_name)
                try:
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc,
                                          secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc)
                except Exception as e:
                    logger.warning(f"cleanup target lvol {tgt_lvol_composite} (non-fatal): {e}")
                return False, True, f"snapshot {last_snap_uuid} not found on target"
        # else: no snapshots at all — tgt_snap_composite stays "" and SPDK
        # performs a full-lvol transfer without a snapshot base.

        # Pre-freeze: take SRC secondary/tertiary out of the read path before
        # the synchronous final-migration freeze below, so no client can serve
        # stale data off a replica while primary is mid-freeze.  Left as-is if
        # the freeze succeeds — the Done handler's ANA sequence below already
        # drives every SRC path (including primary) to inaccessible on success.
        if src_replica_paths:
            logger.info(
                f"[IO-FREEZE] {_now_ms()} setting SRC secondary/tertiary "
                f"inaccessible pre-final-migration: lvol={lvol.uuid}")
            for p in src_replica_paths:
                _flip(p['rpc'], p['ip'], p['port'], p['trtype'],
                      "inaccessible", f"SRC-{p['node_id'][:8]}(pre-freeze)")

        # Step 5: start final migration — synchronous: blocks until SPDK completes
        # the IO drain and delta copy.  Returns success/failure directly; no polling needed.
        logger.info(
            f"[IO-FREEZE] {_now_ms()} bdev_lvol_final_migration starting: "
            f"lvol={lvol.uuid} src={src_lvol_composite} tgt_snap={tgt_snap_composite}")
        try:
            ret = src_rpc.bdev_lvol_transfer_final_step(
                src_lvol_composite, tgt_map_id, tgt_snap_composite, 2, hub_bdev, "migrate")
            if not ret:
                # Falsy, not just None: a target restart mid-RPC can come back as a
                # 200 with an empty/non-JSON body, which rpc_client._request2 then
                # returns as raw bytes (e.g. b'') rather than None — that must be
                # treated the same as a hard failure, not silently as success.
                # Connection timeout or SPDK error (e.g. "File exists" = already in
                # progress). SPDK may have completed the migration while the RPC
                # connection dropped. Check transfer_stat before treating this as
                # a hard failure. This call can itself raise (source still
                # unreachable) — kept inside this same try so that case is
                # reverted below like any other failure of this attempt.
                stat = src_rpc.bdev_lvol_transfer_stat(src_lvol_composite)
                state = (stat or {}).get('transfer_state') if stat is not None else None
            else:
                state = None
        except Exception:
            # SRC secondary/tertiary were just flipped inaccessible above; if
            # either RPC above raises (e.g. source unreachable — RPCException
            # ("connection error")) rather than returning a falsy result, that
            # revert must still happen here — otherwise a source outage leaves
            # replicas inaccessible for as long as the outage lasts, with no
            # working path at all, since this exception propagates past this
            # function to task_runner's generic RPCException handler which
            # only suspends the task.
            _revert_src_replicas("final migration RPC call failed")
            raise
        if not ret:
            if state not in ('Done', 'No process'):
                _revert_src_replicas("final migration failed")
                src_rpc.bdev_nvme_detach_controller(ctrl_name)
                # Do NOT delete the target bdev on transfer failure — the bdev is
                # still valid and retaining it keeps the map_id stable across retries.
                # Deleting it would force a recreate at a higher map_id (due to
                # concurrent migrations creating bdevs in the interim).
                return False, True, "bdev_lvol_final_migration failed"
            logger.info(
                f"[IO-RESUME] {_now_ms()} final migration complete (recovered from RPC error, "
                f"transfer_state={state}): lvol={migration.lvol_id} io now live on target")
        else:
            logger.info(
                f"[IO-RESUME] {_now_ms()} final migration Done: "
                f"lvol={migration.lvol_id} io now live on target")

        # add_clone on secondary and tertiary to link the final migrated lvol to
        # its predecessor snapshot in their ancestry chain.
        # bdev_lvol_final_migration handles this on the primary internally;
        # non-primary nodes need an explicit call.
        _clone_tgt_composite = f"{tgt_node.lvstore}/{tgt_lvol_bdev}"
        for _extra_node in filter(None, [
            _get_target_secondary_node(tgt_node, src_node.get_id())[0],
            _get_target_tertiary_node(tgt_node, src_node.get_id())[0],
        ]):
            _ret = _make_rpc(_extra_node).bdev_lvol_add_clone(
                _clone_tgt_composite, tgt_snap_composite)
            if not _ret:
                logger.warning(
                    f"add_clone on {_extra_node.get_id()[:8]} failed for final lvol (non-fatal)")

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

    # Done handler: add namespace and flip ANA states so the client follows the
    # volume without disconnect/reconnect.  nqn / src_paths / tgt_paths /
    # overlap_ids were computed once up front, before the pre-freeze step.

    # For crypto lvols the namespace already points to the crypto bdev (set up
    # during create_migration). tgt_ns_bdev is used by overlap step 4 to
    # swap the SRC namespace to the correct bdev at cutover.
    tgt_ns_bdev = tgt_lvol_composite  # plain default
    if lvol.crypto_bdev:
        tgt_ns_bdev = f"crypto_{tgt_lvol_bdev}"

    # Generalized ANA + namespace-swap sequence.
    # Works for any topology (non-overlap, Case A, Case B, tertiary).
    #
    # No-overlap fast path:
    #   Step 1 — all TGT paths: final ANA state (prim=optimized, rest=non_optimized)
    #   Step 3 — all SRC paths → inaccessible
    #   (steps 2, 4, 5, 6 skipped — no overlap nodes)
    #
    # Overlap path:
    #   Step 1 — first non-overlap TGT → optimized  (live path before touching overlap)
    #   Step 2 — overlap SRC paths    → inaccessible (at SRC port)
    #   Step 3 — non-overlap SRC paths → inaccessible
    #   Step 4 — overlap TGT paths: swap namespace (SRC bdev → tgt_ns_bdev)
    #   Step 5 — all TGT paths: correct ANA state at TGT port
    #   Step 6 — overlap TGT paths: remove old SRC listener if port changed
    src_port_by_id = {p['node_id']: p['port'] for p in src_paths}

    if not overlap_ids:
        # Step 1 (no-overlap): TGT primary must be confirmed optimized before
        # making SRC inaccessible — otherwise clients lose all I/O paths.
        primary_tgt = tgt_paths[0]
        if not _flip_required(primary_tgt['rpc'], primary_tgt['ip'], primary_tgt['port'],
                               primary_tgt['trtype'], "optimized",
                               f"TGT-{primary_tgt['node_id'][:8]}"):
            return False, False, (
                "ANA flip: TGT primary→optimized failed after retries "
                "— aborting to keep SRC paths accessible")

        # Secondary TGT paths best-effort — clients survive without them
        for tgt in tgt_paths[1:]:
            _flip(tgt['rpc'], tgt['ip'], tgt['port'], tgt['trtype'],
                  "non_optimized", f"TGT-{tgt['node_id'][:8]}")

        # Step 3 (no-overlap): all SRC paths → inaccessible
        for src in src_paths:
            _flip(src['rpc'], src['ip'], src['port'], src['trtype'],
                  "inaccessible", f"SRC-{src['node_id'][:8]}")
    else:
        # Step 1: first non-overlap TGT → optimized. Must succeed before
        # making any SRC path inaccessible.
        non_overlap_tgt = next(
            (t for t in tgt_paths if t['node_id'] not in overlap_ids), None)
        if non_overlap_tgt:
            if not _flip_required(non_overlap_tgt['rpc'], non_overlap_tgt['ip'],
                                   non_overlap_tgt['port'], non_overlap_tgt['trtype'],
                                   "optimized", f"TGT-{non_overlap_tgt['node_id'][:8]}(pre)"):
                return False, False, (
                    "ANA flip: non-overlap TGT primary→optimized failed after retries "
                    "— aborting to keep SRC paths accessible")

        # Step 2: overlap SRC paths → inaccessible at SRC port
        for src in src_paths:
            if src['node_id'] in overlap_ids:
                _flip(src['rpc'], src['ip'], src['port'], src['trtype'],
                      "inaccessible", f"SRC-{src['node_id'][:8]}(overlap)")

        # Step 3: non-overlap SRC paths → inaccessible
        for src in src_paths:
            if src['node_id'] not in overlap_ids:
                _flip(src['rpc'], src['ip'], src['port'], src['trtype'],
                      "inaccessible", f"SRC-{src['node_id'][:8]}")

        # Step 4: namespace swap on overlap TGT paths (SRC bdev → tgt_ns_bdev).
        # For crypto, tgt_ns_bdev is crypto_LVOL_xxxxm which was created during
        # create_migration; for plain it is the raw migration lvol.
        for tgt in tgt_paths:
            if tgt['node_id'] in overlap_ids:
                _swap_namespace(tgt['rpc'], nqn, tgt_ns_bdev,
                                lvol.uuid, lvol.guid, tgt['node_id'][:8])

        # Step 5: all TGT paths → correct ANA state at TGT port.
        # Primary required; secondaries best-effort.
        primary_tgt = tgt_paths[0]
        if not _flip_required(primary_tgt['rpc'], primary_tgt['ip'], primary_tgt['port'],
                               primary_tgt['trtype'], "optimized",
                               f"TGT-{primary_tgt['node_id'][:8]}"):
            return False, False, (
                "ANA flip: TGT primary→optimized (step 5) failed after retries")
        for tgt in tgt_paths[1:]:
            _flip(tgt['rpc'], tgt['ip'], tgt['port'], tgt['trtype'],
                  "non_optimized", f"TGT-{tgt['node_id'][:8]}")

    # Step 6: overlap TGT paths → remove old SRC listener if port changed
    for tgt in tgt_paths:
        if tgt['node_id'] in overlap_ids:
            old_port = src_port_by_id.get(tgt['node_id'])
            if old_port and old_port != tgt['port']:
                try:
                    tgt['rpc'].listeners_del(nqn, tgt['trtype'], tgt['ip'], old_port)
                    logger.info(
                        f"Removed old SRC listener {tgt['ip']}:{old_port} "
                        f"from overlap node {tgt['node_id'][:8]}")
                except Exception as e:
                    logger.warning(f"Remove old SRC listener (non-fatal): {e}")

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
    _apply_migration_to_db(
        migration,
        tgt_lvol_uuid=tgt_uuid_carry.get('tgt_lvol_uuid'),
        tgt_lvol_bdev=tgt_uuid_carry.get('tgt_lvol_bdev'))

    logger.info(f"Migration {migration.uuid}: PHASE_LVOL_MIGRATE done — TGT subsystem live")
    return True, False, None



def _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc=None, tgt_ter_rpc=None):
    """
    Delete migration-created intermediate ('shrink') snapshots from the target
    after a successful migration.

    Must be called AFTER apply_migration_to_db() — at that point snap.snap_bdev
    already holds the target composite path (e.g. LVS_TGT/SNAP_xxxm).

    Delegates to _delete_bdev_blocking(coalescing=True): the intermediate
    snapshot's clusters must be merged into its child bdev before being freed
    (special_delete=False), or the child's parent chain breaks and
    pre-migration data is lost.
    """
    for snap_uuid in migration.intermediate_snaps:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            logger.info(f"Intermediate snap {snap_uuid} already removed from DB; skipping")
            continue

        tgt_composite = snap.snap_bdev  # updated to target path by apply_migration_to_db

        if not tgt_rpc.get_bdevs(tgt_composite):
            logger.info(
                f"Intermediate snap bdev {tgt_composite} absent from target; skipping SPDK delete")
        else:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                      coalescing=True)
                logger.info(f"Deleted intermediate snap bdev {tgt_composite} from target")
            except Exception as e:
                logger.warning(
                    f"Could not delete intermediate snap {tgt_composite} from target: {e}")

        try:
            snap.remove(db.kv_store)
            logger.info(f"Removed intermediate snap {snap_uuid} from DB")
        except Exception as e:
            logger.warning(f"Could not remove intermediate snap {snap_uuid} from DB: {e}")


def _rename_migrated_bdevs(migration, tgt_node, tgt_rpc, tgt_sec_rpc=None, tgt_ter_rpc=None):
    """
    After migration completes, rename 'm'-suffixed bdevs on the target back to
    their canonical names (without the suffix).  This prevents suffix accumulation
    on repeated migrations of the same volume.

    bdev_lvol_rename returns JSON-RPC error -32602 "File exists" (None to caller)
    when the target name is already taken; we use that to try the fallback
    (_MIGRATION_BDEV_SUFFIX_DONE) instead of doing a pre-query of all bdevs.

    Must be called AFTER _apply_migration_to_db() — snap.snap_bdev and
    lvol.lvol_bdev already point to the 'm'-suffixed target paths at that point.
    """
    lvstore = tgt_node.lvstore
    preexisting = set(migration.snaps_preexisting_on_target or [])

    _EXISTS = "EXISTS"

    def _do_rename(old_composite, new_short, label):
        """Rename on prim + sec + ter.  Returns 'EXISTS' if the target name is
        already taken on the PRIMARY (SPDK returns JSON-RPC error -32602 'File
        exists' -> None), True on success.  new_short must be the short name only
        (no lvstore prefix).

        Secondary/tertiary conflicts are non-fatal: an overlap node may already
        carry the bdev at the canonical name, so a None return there must not
        mask a successful primary rename.
        """
        ret = tgt_rpc.bdev_lvol_rename(old_composite, new_short)
        logger.debug(f"_do_rename prim: {old_composite!r} -> {new_short!r}: ret={ret!r}")
        # SPDK returns None on name collision (-32602 "File exists"); only the
        # primary result determines whether we should try the fallback name.
        prim_exists = (not ret) or (ret == _EXISTS)
        for role, rpc in [("sec", tgt_sec_rpc), ("ter", tgt_ter_rpc)]:
            if rpc:
                try:
                    r = rpc.bdev_lvol_rename(old_composite, new_short)
                    logger.debug(f"_do_rename {role}: {old_composite!r} -> {new_short!r}: ret={r!r}")
                    if (not r) or r == _EXISTS:
                        logger.warning(
                            f"_rename_migrated_bdevs: {role} rename {label} "
                            f"{old_composite!r} -> {new_short!r}: non-fatal "
                            f"({role} may already have bdev at target name)")
                except Exception as exc:
                    logger.warning(
                        f"_rename_migrated_bdevs: {role} rename {label} (non-fatal): {exc}")
        if prim_exists:
            return _EXISTS
        return True

    def _rename_with_fallback(current_short, label):
        """Try canonical name; on EXISTS try the _done fallback.
        Returns the target short name on success, None if skipped."""
        canonical = current_short[:-len(_MIGRATION_BDEV_SUFFIX)]
        old = f"{lvstore}/{current_short}"

        ret = _do_rename(old, canonical, label)
        if ret == _EXISTS:
            fallback = canonical + _MIGRATION_BDEV_SUFFIX_DONE
            logger.info(
                f"_rename_migrated_bdevs: {canonical} exists - trying fallback {fallback}")
            ret2 = _do_rename(old, fallback, label)
            if ret2 == _EXISTS:
                logger.warning(
                    f"_rename_migrated_bdevs: both {canonical} and {fallback} "
                    f"exist - leaving {current_short} as-is")
                return None
            target = fallback
        else:
            target = canonical

        logger.info(f"_rename_migrated_bdevs: {current_short} -> {target}")
        return target

    # --- Snapshots (owned) ---
    for snap_uuid in migration.snaps_migrated:
        if snap_uuid in preexisting:
            continue
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            continue

        snap_bdev = snap.snap_bdev
        if '/' not in snap_bdev:
            continue
        current_short = snap_bdev.split('/', 1)[1]
        if not current_short.endswith(_MIGRATION_BDEV_SUFFIX):
            continue

        if snap.lvol.uuid != migration.lvol_id:
            continue  # ancestor snap — handled below

        target = _rename_with_fallback(current_short, current_short)
        if target:
            snap.snap_bdev = f"{lvstore}/{target}"
            snap.write_to_db(db.kv_store)

    # --- Ancestor chain blobs (non-owned snaps) ---
    # _apply_migration_to_db added an instances entry with the _m bdev name.
    # Rename it and update the entry in place.
    for snap_uuid in migration.snaps_migrated:
        if snap_uuid in preexisting:
            continue
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            continue
        if snap.lvol.uuid == migration.lvol_id:
            continue  # owned snap — handled above

        updated = False
        for inst in snap.instances:
            inst_bdev = inst.get('snap_bdev', '')
            if '/' not in inst_bdev:
                continue
            inst_lvstore, inst_short = inst_bdev.split('/', 1)
            if inst_lvstore != lvstore:
                continue
            if not inst_short.endswith(_MIGRATION_BDEV_SUFFIX):
                continue

            try:
                target = _rename_with_fallback(inst_short, inst_short)
            except Exception as exc:
                logger.warning(
                    f"_rename_migrated_bdevs: ancestor {inst_short} failed: {exc}")
                continue
            if target:
                inst['snap_bdev'] = f"{lvstore}/{target}"
                updated = True

        if updated:
            snap.write_to_db(db.kv_store)

    # --- Lvol ---
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
    except KeyError:
        logger.warning(f"_rename_migrated_bdevs: lvol {migration.lvol_id} not found")
        return

    current_lvol_short = lvol.lvol_bdev
    if not current_lvol_short.endswith(_MIGRATION_BDEV_SUFFIX):
        return

    target = _rename_with_fallback(current_lvol_short, current_lvol_short)
    if target:
        old_composite = f"{lvstore}/{current_lvol_short}"
        lvol.lvol_bdev = target
        lvol.top_bdev = f"{lvstore}/{target}"
        for entry in lvol.bdev_stack:
            if (entry.get('type') == 'bdev_lvol'
                    and entry.get('params', {}).get('name') == current_lvol_short):
                entry['params']['name'] = target
            elif entry.get('name') == old_composite:
                # bdev_lvol_clone (and any other type) stores the composite bdev
                # path in 'name'; keep it in sync with the renamed bdev so that
                # _remove_bdev_stack sends the delete to the correct bdev name.
                entry['name'] = lvol.top_bdev
                if entry.get('params', {}).get('clone_name') == current_lvol_short:
                    entry['params']['clone_name'] = target
        lvol.write_to_db(db.kv_store)


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
    ctx = migration.transfer_context or {}

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
        if not source_lvol_bdev_saved:
            return False, False, (
                "source_lvol_bdev missing from transfer_context — cannot safely "
                "identify source bdev; apply_migration_to_db may have already "
                "renamed lvol.lvol_bdev to the target name")

        to_delete = migration_controller.get_snaps_safe_to_delete_on_source(migration)

        # Verify each snapshot to be deleted physically exists on the target
        # before we remove anything from the source.  Bdevs may carry the
        # migration suffix (SNAP_Xm), canonical name (SNAP_X), or the
        # post-rename fallback (SNAP_Xam) — check all three so that a
        # crash-recovery re-run after a partial rename still passes.
        tgt_lvols = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
        tgt_names = {e.get('name', '').split('/')[-1] for e in tgt_lvols}
        for snap_uuid in to_delete:
            try:
                snap = db.get_snapshot_by_id(snap_uuid)
                # snap.snap_bdev was updated to the target path by apply_migration_to_db;
                # use it as the primary lookup, then fall back to derived names.
                _snap_bdev = snap.snap_bdev or ''
                _primary = _snap_bdev.split('/', 1)[1] if '/' in _snap_bdev else _snap_bdev
                _m_name = _snap_tgt_short_name(snap)
                _canonical = _snap_short_name(snap)
                _am_name = _canonical + _MIGRATION_BDEV_SUFFIX_DONE
                if not any(n in tgt_names for n in (_primary, _m_name, _canonical, _am_name)):
                    return False, False, (
                        f"Target missing snapshot {_m_name} ({snap_uuid}) "
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

    src_sec = _get_source_secondary_node(src_node)
    src_sec_rpc = _make_rpc(src_sec) if src_sec else None
    src_ter = _get_source_tertiary_node(src_node)
    src_ter_rpc = _make_rpc(src_ter) if src_ter else None

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
            try:
                _delete_bdev_blocking(bdev_name, src_rpc,
                                      secondary_rpc=src_sec_rpc, tertiary_rpc=src_ter_rpc)
                logger.info(f"Deleted source bdev {bdev_name}")
            except Exception as e:
                logger.warning(f"delete source bdev {bdev_name} (non-fatal): {e}")
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
    lvol = None
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
        logger.info(f"Step 8: removing source NVMe-oF subsystem {lvol.nqn}")
        # Overlap nodes had their subsystem taken over by the target — skip delete.
        # Non-overlap source nodes own their subsystem exclusively; delete it.
        _src_paths_cu, _, _overlap_ids_cu = _build_paths(
            src_node, tgt_node, src_rpc, tgt_rpc)
        for _sp in _src_paths_cu:
            if _sp['node_id'] in _overlap_ids_cu:
                logger.info(
                    f"Step 8: skip subsystem delete on overlap node "
                    f"{_sp['node_id'][:8]} (now serving TGT)")
            else:
                migration_controller.cleanup_subsystem_or_ns(lvol.nqn, lvol.uuid, True, _sp['rpc'])
    except Exception as e:
        logger.warning(f"Source subsystem cleanup failed (non-fatal): {e}")

    # Explicitly delete the source lvol bdev.  bdev_lvol_final_migration may
    # have already freed it on the SPDK side — _delete_bdev_blocking handles
    # that gracefully (status 2 = not-found is treated as complete).
    if lvol is not None:
        try:
            # Use the saved pre-apply bdev name; apply_migration_to_db already
            # renamed lvol.lvol_bdev to the target 'm'-suffix name in DB.
            src_bdev_short = ctx.get('source_lvol_bdev')
            if not src_bdev_short:
                raise RuntimeError(
                    "source_lvol_bdev missing from ctx at delete site — "
                    "refusing to fall back to lvol.lvol_bdev which may point to target")
            src_lvol_composite = f"{src_node.lvstore}/{src_bdev_short}"
            # Set migration flag so SPDK drops only the blob reference without
            # freeing the physical clusters — data now lives on the target.
            try:
                src_rpc.bdev_lvol_set_migration_flag(src_lvol_composite)
                if src_sec_rpc:
                    src_sec_rpc.bdev_lvol_set_migration_flag(src_lvol_composite)
                if src_ter_rpc:
                    src_ter_rpc.bdev_lvol_set_migration_flag(src_lvol_composite)
            except Exception as _mf_err:
                logger.warning(
                    f"bdev_lvol_set_migration_flag for source lvol failed "
                    f"(non-fatal): {_mf_err}")
            _delete_bdev_blocking(
                src_lvol_composite, src_rpc,
                secondary_rpc=src_sec_rpc, tertiary_rpc=src_ter_rpc)
            logger.info(f"Deleted source lvol bdev {src_lvol_composite}")
        except Exception as e:
            logger.warning(f"Source lvol delete failed (non-fatal): {e}")

    tgt_lvol_uuid = ctx.get('tgt_lvol_uuid')
    tgt_lvol_bdev = ctx.get('tgt_lvol_bdev')
    migration.transfer_context = {}
    if not _apply_migration_to_db(
            migration, tgt_lvol_uuid=tgt_lvol_uuid, tgt_lvol_bdev=tgt_lvol_bdev):
        return False, False, "Failed to update DB records after source cleanup"

    tgt_sec, _ = _get_target_secondary_node(tgt_node, src_node.get_id())
    tgt_sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, src_node.get_id())
    tgt_ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None

    # Delete intermediate (shrink) snapshots from the target — they are migration
    # artifacts and do not need to be preserved. No migration flag so SPDK
    # coalesces and frees their clusters into the child bdev.
    try:
        if migration.intermediate_snaps:
            _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc, tgt_ter_rpc)
        _rename_migrated_bdevs(migration, tgt_node, tgt_rpc, tgt_sec_rpc, tgt_ter_rpc)
    except Exception as e:
        return False, False, str(e)

    return True, False, None


def _handle_cleanup_target(migration, tgt_node, tgt_rpc, src_rpc=None):
    """
    Roll back a failed or cancelled migration: remove any partially-created
    target lvol/subsystem, then delete all snapshots copied to the target.

    Each deletion uses _delete_bdev_blocking (async-start → poll → sync-finalize
    on primary and secondary).  Idempotent: "not found" (status 2) is treated as
    already done, so a crash-recovery re-run is safe.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    # Immediately detach the hub controller on failure/cancel — don't leave it
    # connected to a target whose snapshots we're about to roll back.
    hub_manager.detach_now(migration.source_node_id, tgt_node.get_id(), src_rpc=src_rpc)

    ctx = migration.transfer_context or {}
    tgt_sec, _ = _get_target_secondary_node(tgt_node, migration.source_node_id)
    tgt_sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, migration.source_node_id)
    tgt_ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None

    # --- Step 0: delete dangling target lvol/subsystems from a failed LVOL_MIGRATE ---
    # Also handles the pre-create case where bdev/subsystems were set up by
    # create_migration() but migration was cancelled before LVOL_MIGRATE completed.
    if ctx.get('stage') != 'cleanup_tgt':
        tgt_lvol_composite = ctx.get('tgt_lvol_composite')
        nqn = ctx.get('nqn')

        # Per-node ownership: migration.target_subsystem_node_ids is the
        # authoritative record of which nodes had their subsystem *created*
        # by this migration (see _ensure_target_nvmf_state). An overlap node
        # reuses a preexisting subsystem it doesn't own, so it's never added
        # to this list — cleanup must not delete a subsystem this migration
        # never created just because the transfer failed/was cancelled.
        owned_node_ids = set(migration.target_subsystem_node_ids or [])

        # Derive the migration bdev name in case it was pre-created but not yet
        # recorded in transfer_context (i.e. failure before LVOL_MIGRATE saved ctx).
        _pre_nqn: Optional[str] = None
        try:
            _lvol = db.get_lvol_by_id(migration.lvol_id)
            _pre_bdev = f"{tgt_node.lvstore}/{_lvol_tgt_bdev_name(_lvol.lvol_bdev)}"
            _pre_nqn  = _lvol.nqn
        except Exception:
            _pre_bdev = None
            _pre_nqn  = str(nqn) if nqn else None

        # Clean up NVMe-oF subsystem — from ctx (LVOL_MIGRATE failure) or from pre-create.
        _nqn_to_clean = nqn or _pre_nqn
        if _nqn_to_clean:
            try:
                migration_controller.cleanup_subsystem_or_ns(
                    _nqn_to_clean, migration.lvol_id,
                    tgt_node.get_id() in owned_node_ids, tgt_rpc)
            except Exception as e:
                logger.warning(f"cleanup target subsystem {_nqn_to_clean}: {e}")
            for _label, _extra_node, _extra_rpc in [
                ("secondary", tgt_sec, tgt_sec_rpc),
                ("tertiary", tgt_ter, tgt_ter_rpc),
            ]:
                if _extra_rpc and _extra_node:
                    try:
                        migration_controller.cleanup_subsystem_or_ns(
                            _nqn_to_clean, migration.lvol_id,
                            _extra_node.get_id() in owned_node_ids, _extra_rpc)
                    except Exception as e:
                        logger.warning(f"cleanup target {_label} subsystem {_nqn_to_clean}: {e}")

        # Delete target migration bdev — prefer ctx composite, fall back to derived name.
        _bdev_to_delete = tgt_lvol_composite or _pre_bdev
        if _bdev_to_delete and tgt_rpc.get_bdevs(_bdev_to_delete):
            try:
                _delete_bdev_blocking(_bdev_to_delete, tgt_rpc,
                                      secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc)
                logger.info(f"Deleted target lvol {_bdev_to_delete}")
            except Exception as e:
                logger.warning(f"delete target lvol {_bdev_to_delete} (non-fatal): {e}")

        ctx = {'stage': 'cleanup_tgt'}
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    # --- Delete target snapshots (blocking, idempotent) ---
    # Reverse order: children/leaves before parents/roots (SPDK open-ref constraint).
    # _delete_bdev_blocking handles "not found" (status 2) gracefully, so a
    # crash-recovery re-run that re-deletes already-removed bdevs is safe.
    # Uses get_snaps_to_delete_on_target to skip pre-existing snaps and snaps
    # still referenced by other lvols already on the target node.
    for snap_uuid in reversed(migration_controller.get_snaps_to_delete_on_target(migration)):
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            # Try all possible bdev name variants: in-flight (m), canonical, am-fallback.
            # After a partial rename (crash mid-cleanup) the bdev may have been
            # renamed before the rollback was triggered.
            _lvstore = tgt_node.lvstore
            _m_name  = _snap_tgt_short_name(snap)
            _canonical = _snap_short_name(snap)
            _am_name = _canonical + _MIGRATION_BDEV_SUFFIX_DONE
            bdev_name = next(
                (f"{_lvstore}/{n}" for n in (_m_name, _canonical, _am_name)
                 if tgt_rpc.get_bdevs(f"{_lvstore}/{n}")),
                None)
            if not bdev_name:
                logger.info(
                    f"Target bdev {_lvstore}/{_m_name} not found in any variant; "
                    f"skipping (already cleaned up)")
                continue
            try:
                _delete_bdev_blocking(bdev_name, tgt_rpc,
                                      secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc)
                logger.info(f"Deleted target snapshot bdev {bdev_name}")
            except Exception as e:
                logger.warning(f"delete target snapshot bdev {bdev_name} (non-fatal): {e}")
        except KeyError:
            logger.warning(f"Target snapshot {snap_uuid} not found in DB; skipping")

    migration.transfer_context = {}
    migration.target_lvol_bdev = ""
    migration.target_subsystem_nqn = ""
    migration.target_subsystem_node_ids = []
    migration.target_snap_bdevs = []
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
        if src_node.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
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

    # Expansion-first ordering: defer while a cluster expansion is open —
    # even between the expand task's retries, when the cluster status is
    # momentarily ACTIVE (see tasks_controller.defer_task_for_expansion).
    if tasks_controller.get_active_cluster_expand_task(task.cluster_id):
        return _suspend_task(
            task, migration, "cluster expansion in progress, deferring")

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

    # --- Target-restart reconciliation ---
    # Before cutover, nothing else re-verifies that the target subsystem/
    # listener/namespace created by create_migration are still there; a
    # target (or its secondary/tertiary) restarting mid-migration silently
    # drops them, which would otherwise only surface as a hard ANA-flip
    # failure once PHASE_LVOL_MIGRATE's Done handler runs.  Not applicable to
    # group workers (batch migration's shared subsystem is reconciled by its
    # own orchestrator).
    if not migration.migration_group_id and phase in (
            LVolMigration.PHASE_SNAP_COPY, LVolMigration.PHASE_LVOL_MIGRATE):
        try:
            lvol = db.get_lvol_by_id(migration.lvol_id)
        except KeyError:
            return _suspend_task(task, migration, f"LVol {migration.lvol_id} not found")

        nvmf_wait = _ensure_target_nvmf_state(migration, lvol, src_node, tgt_node, src_rpc, tgt_rpc)
        if nvmf_wait is _WAIT:
            return _suspend_task(task, migration, migration.error_message or "waiting for target node to recover")

    try:
        if migration.migration_group_id:
            return _group_worker_phase_dispatch(
                task, migration, phase, src_node, tgt_node, src_rpc, tgt_rpc)

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
            done, suspend, error = _handle_cleanup_target(migration, tgt_node, tgt_rpc, src_rpc=src_rpc)
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
# Group worker (batch migration) helpers
# ---------------------------------------------------------------------------

def _post_process_snap_group(snap, migration):
    """
    Lightweight post-processing for a group worker's snap transfer.

    Unlike ``_post_process_snap`` for standalone migrations, this skips
    ``bdev_lvol_add_clone`` and ``bdev_lvol_convert`` — the main orchestrator
    reconstructs the full ancestry tree for all workers together after the
    snap_copy barrier.  The transferred bdev stays writable on the target
    until the orchestrator calls add_clone + convert.

    Tracks the snap in ``migration.snaps_transferred_group`` (raw data on
    target, pending tree reconstruction) rather than ``snaps_migrated``
    (which implies the snapshot is fully committed as immutable).
    """
    snap_uuid = snap.uuid
    if snap_uuid not in migration.snaps_transferred_group:
        migration.snaps_transferred_group.append(snap_uuid)
    migration_events.migration_snap_copied(migration, snap_uuid)
    logger.info(f"Group worker: snap {snap_uuid} raw-transferred (pending tree reconstruction)")
    return True, None


def _handle_group_snap_copy(migration, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    SNAP_COPY phase for a group worker.

    Transfers all owned snapshots (``migration.snap_migration_plan``) to the
    target, skipping the add_clone/convert tree-building steps.  The main
    orchestrator will reconstruct the full ancestry tree after all workers
    reach the snap_copy_done barrier.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    plan = migration.snap_migration_plan
    trtype, _ = _get_migration_nic(tgt_node)
    ctx = migration.transfer_context or {}

    try:
        _lvol_for_size = db.get_lvol_by_id(migration.lvol_id)
        _snap_lvol_size_mib = _bytes_to_mib(_lvol_for_size.size)
    except KeyError:
        _snap_lvol_size_mib = None

    # Determine which snaps still need transferring (owned, not yet transferred).
    already_done = set(migration.snaps_transferred_group) | set(migration.snaps_preexisting_on_target)
    remaining = [u for u in plan if u not in already_done]

    if not remaining and ctx.get('stage') != 'parallel_transfer':
        return True, False, None  # all owned snaps transferred

    # Launch / resume one snap at a time (SPDK only supports one per poller group).
    if ctx.get('stage') != 'parallel_transfer' and remaining:
        snap_uuid = remaining[0]
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            return False, True, f"Snapshot {snap_uuid} not found in DB"

        snap_short_tgt = _snap_tgt_short_name(snap)
        src_composite = _snap_composite(src_node.lvstore, snap)
        tgt_composite = f"{tgt_node.lvstore}/{snap_short_tgt}"

        existing_stat = src_rpc.bdev_lvol_transfer_stat(src_composite)
        if (existing_stat is not None
                and existing_stat.get('transfer_state') == 'In progress'):
            migration.transfer_context = {
                'stage': 'parallel_transfer',
                'transfers': [{'snap_uuid': snap_uuid, 'snap_short': snap_short_tgt,
                               'snap_index': plan.index(snap_uuid),
                               'transfer_done': False, 'post_done': False}],
            }
            migration.write_to_db(db.kv_store)
            return False, False, None

        _g_tgt_sec, _g_sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
        _g_tgt_ter, _g_ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
        if _g_sec_err:
            migration.error_message = _g_sec_err
            migration.write_to_db(db.kv_store)
            return False, True, _WAIT
        if _g_ter_err:
            migration.error_message = _g_ter_err
            migration.write_to_db(db.kv_store)
            return False, True, _WAIT
        _g_sec_rpc = _make_rpc(_g_tgt_sec) if _g_tgt_sec else None
        _g_ter_rpc = _make_rpc(_g_tgt_ter) if _g_tgt_ter else None

        if tgt_rpc.get_bdevs(tgt_composite):
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc, _g_sec_rpc, _g_ter_rpc)
            except Exception as e:
                logger.warning(f"Group worker: pre-cleanup of {tgt_composite} failed: {e}")

        t, err = _setup_snap_transfer(
            snap, plan.index(snap_uuid), src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=_g_tgt_sec, sec_rpc=_g_sec_rpc,
            tgt_ter=_g_tgt_ter, ter_rpc=_g_ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib)
        if t is None:
            return False, True, err

        migration.transfer_context = {
            'stage': 'parallel_transfer',
            'transfers': [t],
        }
        migration.write_to_db(db.kv_store)
        return False, False, None

    # Poll the in-flight transfer.
    if ctx.get('stage') == 'parallel_transfer':
        transfers = ctx['transfers']
        for t in transfers:
            snap_uuid = t['snap_uuid']
            if t.get('post_done'):
                continue
            try:
                snap = db.get_snapshot_by_id(snap_uuid)
            except KeyError:
                migration.transfer_context = {}
                migration.write_to_db(db.kv_store)
                return False, True, f"Snapshot {snap_uuid} disappeared during transfer"

            src_composite = _snap_composite(src_node.lvstore, snap)
            if not t['transfer_done']:
                result = src_rpc.bdev_lvol_transfer_stat(src_composite)
                if result is None:
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, f"bdev_lvol_transfer_stat returned None for {snap_uuid}"
                state = result.get('transfer_state', 'No process')
                if state == 'In progress':
                    migration.transfer_context = ctx
                    migration.write_to_db(db.kv_store)
                    return False, False, None
                if state in ('Failed', 'No process'):
                    migration.transfer_context = {}
                    migration.write_to_db(db.kv_store)
                    return False, True, f"Snapshot transfer {state} for {snap_uuid}"
                t['transfer_done'] = True

            # Transfer done — record without add_clone/convert.
            ok, err = _post_process_snap_group(snap, migration)
            if not ok:
                migration.transfer_context = {}
                migration.write_to_db(db.kv_store)
                return False, True, err
            t['post_done'] = True
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)

        # Check if more snaps remain after this one.
        already_done = set(migration.snaps_transferred_group) | set(migration.snaps_preexisting_on_target)
        remaining = [u for u in plan if u not in already_done]
        if remaining:
            return False, False, None  # come back for the next snap
        return True, False, None  # all done

    return True, False, None


def _handle_group_intermediate(migration, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    INTERMEDIATE phase for a group worker.

    Takes exactly one intermediate ("shrink") snapshot and transfers it to the
    target, skipping add_clone/convert (same as snap_copy).  After this the
    worker signals intermediates_done to the group and waits for batch_result.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    trtype, _ = _get_migration_nic(tgt_node)
    ctx = migration.transfer_context or {}

    # If we already took and transferred the intermediate snap, we're done.
    if ctx.get('stage') == 'intermediate_done':
        return True, False, None

    # Take the intermediate snapshot if not already in flight.
    if ctx.get('stage') != 'intermediate_transfer':
        _take_intermediate_snapshot(migration)
        plan = migration.snap_migration_plan
        if not plan:
            return False, True, "Group intermediate: _take_intermediate_snapshot failed"
        snap_uuid = plan[-1]
        snap_index = len(plan) - 1

        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            return False, True, f"Intermediate snapshot {snap_uuid} not found"

        snap_short_tgt = _snap_tgt_short_name(snap)
        tgt_composite = f"{tgt_node.lvstore}/{snap_short_tgt}"

        try:
            _lvol_for_size = db.get_lvol_by_id(migration.lvol_id)
            _snap_lvol_size_mib = _bytes_to_mib(_lvol_for_size.size)
        except KeyError:
            _snap_lvol_size_mib = None

        _g_tgt_sec, _g_sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
        _g_tgt_ter, _g_ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
        if _g_sec_err:
            migration.error_message = _g_sec_err
            migration.write_to_db(db.kv_store)
            return False, True, _WAIT
        if _g_ter_err:
            migration.error_message = _g_ter_err
            migration.write_to_db(db.kv_store)
            return False, True, _WAIT
        _g_sec_rpc = _make_rpc(_g_tgt_sec) if _g_tgt_sec else None
        _g_ter_rpc = _make_rpc(_g_tgt_ter) if _g_tgt_ter else None

        if tgt_rpc.get_bdevs(tgt_composite):
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc, _g_sec_rpc, _g_ter_rpc)
            except Exception as e:
                logger.warning(f"Group intermediate: pre-cleanup of {tgt_composite} failed: {e}")

        t, err = _setup_snap_transfer(
            snap, snap_index, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=_g_tgt_sec, sec_rpc=_g_sec_rpc,
            tgt_ter=_g_tgt_ter, ter_rpc=_g_ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib)
        if t is None:
            return False, True, err

        migration.transfer_context = {
            'stage': 'intermediate_transfer',
            'transfer': t,
        }
        migration.write_to_db(db.kv_store)
        return False, False, None

    # Poll the intermediate transfer.
    t = ctx['transfer']
    snap_uuid = t['snap_uuid']
    try:
        snap = db.get_snapshot_by_id(snap_uuid)
    except KeyError:
        migration.transfer_context = {}
        migration.write_to_db(db.kv_store)
        return False, True, f"Intermediate snap {snap_uuid} disappeared"

    src_composite = _snap_composite(src_node.lvstore, snap)
    if not t.get('transfer_done'):
        result = src_rpc.bdev_lvol_transfer_stat(src_composite)
        if result is None:
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, f"bdev_lvol_transfer_stat returned None for {snap_uuid}"
        state = result.get('transfer_state', 'No process')
        if state == 'In progress':
            return False, False, None
        if state in ('Failed', 'No process'):
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            return False, True, f"Intermediate transfer {state} for {snap_uuid}"
        t['transfer_done'] = True

    # Record the intermediate snap — the orchestrator will call bdev_lvol_convert
    # for all members at once under the hub lock, immediately before
    # bdev_lvol_batch_final_step. Converting here would drop the hub NVMe
    # connection before the orchestrator can use it.
    if snap_uuid not in migration.snaps_migrated:
        migration.snaps_migrated.append(snap_uuid)
    migration.transfer_context = {'stage': 'intermediate_done'}
    migration.write_to_db(db.kv_store)
    return True, False, None


def _group_worker_phase_dispatch(task, migration, phase, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Complete phase dispatcher for FN_LVOL_MIG tasks that belong to a batch
    migration group (``migration.migration_group_id`` is set).

    Manages the group worker state machine:
      SNAP_COPY   → transfer owned snaps (no add_clone/convert)
                  → signal snap_copy_done to group → wait for INTERMEDIATE
      LVOL_MIGRATE (repurposed as the single-intermediate phase for workers)
                  → take + transfer 1 intermediate snap
                  → signal intermediates_done → wait for batch_result
      CLEANUP_SOURCE → normal source cleanup + signal cleanup_source_done
      CLEANUP_TARGET → normal target rollback

    Returns True if the task reached a terminal state, False otherwise.
    """
    group_id = migration.migration_group_id
    migration_id = migration.uuid

    try:
        group = db.get_migration_group_by_id(group_id)
    except KeyError:
        return _fail_task(task, migration, f"LVolMigrationGroup {group_id} not found")

    # --- SNAP_COPY ---
    if phase == LVolMigration.PHASE_SNAP_COPY:
        if migration_id not in group.snap_copy_done:
            # Still transferring owned snaps.
            done, suspend, error = _handle_group_snap_copy(
                migration, src_node, tgt_node, src_rpc, tgt_rpc)
            if error and error is not _WAIT:
                return _suspend_task(task, migration, error)
            if error is _WAIT or suspend:
                return _suspend_task(task, migration, migration.error_message or "waiting")
            if done:
                # Signal snap_copy_done to group.
                group = db.get_migration_group_by_id(group_id)
                if migration_id not in group.snap_copy_done:
                    group.snap_copy_done.append(migration_id)
                    group.write_to_db(db.kv_store)
                    logger.info(
                        f"Group worker {migration_id[:8]}: signalled snap_copy_done "
                        f"({len(group.snap_copy_done)}/{group.member_count()})")
            # Wait for orchestrator to advance group to INTERMEDIATE.
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            return False

        # snap_copy_done already signalled — check if group has advanced.
        group = db.get_migration_group_by_id(group_id)
        if group.phase == LVolMigrationGroup.PHASE_INTERMEDIATE:
            migration.phase = LVolMigration.PHASE_LVOL_MIGRATE
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return _group_worker_phase_dispatch(
                task, migration, LVolMigration.PHASE_LVOL_MIGRATE,
                src_node, tgt_node, src_rpc, tgt_rpc)
        if group.phase == LVolMigrationGroup.PHASE_CLEANUP_TARGET:
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.write_to_db(db.kv_store)
            return _group_worker_phase_dispatch(
                task, migration, LVolMigration.PHASE_CLEANUP_TARGET,
                src_node, tgt_node, src_rpc, tgt_rpc)
        # Still waiting for other workers.
        task.write_to_db(db.kv_store)
        return False

    # --- LVOL_MIGRATE (group worker: take 1 intermediate + wait for batch_result) ---
    if phase == LVolMigration.PHASE_LVOL_MIGRATE:
        if migration_id not in group.intermediates_done:
            done, suspend, error = _handle_group_intermediate(
                migration, src_node, tgt_node, src_rpc, tgt_rpc)
            if error and error is not _WAIT:
                return _suspend_task(task, migration, error)
            if error is _WAIT or suspend:
                return _suspend_task(task, migration, migration.error_message or "waiting")
            if done:
                group = db.get_migration_group_by_id(group_id)
                if migration_id not in group.intermediates_done:
                    group.intermediates_done.append(migration_id)
                    group.write_to_db(db.kv_store)
                    logger.info(
                        f"Group worker {migration_id[:8]}: signalled intermediates_done "
                        f"({len(group.intermediates_done)}/{group.member_count()})")
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            return False

        # intermediates_done signalled — wait for batch_result.
        group = db.get_migration_group_by_id(group_id)
        if group.batch_result is True:
            lvol = db.get_lvol_by_id(migration.lvol_id)
            migration.phase = LVolMigration.PHASE_CLEANUP_SOURCE
            migration.transfer_context = {
                'source_lvol_bdev': lvol.lvol_bdev,
                'tgt_lvol_bdev': _lvol_tgt_bdev_name(lvol.lvol_bdev),
            }
            migration.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return _group_worker_phase_dispatch(
                task, migration, LVolMigration.PHASE_CLEANUP_SOURCE,
                src_node, tgt_node, src_rpc, tgt_rpc)
        if group.batch_result is False:
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.transfer_context = {}
            migration.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return _group_worker_phase_dispatch(
                task, migration, LVolMigration.PHASE_CLEANUP_TARGET,
                src_node, tgt_node, src_rpc, tgt_rpc)
        task.write_to_db(db.kv_store)
        return False

    # --- CLEANUP_SOURCE ---
    if phase == LVolMigration.PHASE_CLEANUP_SOURCE:
        try:
            done, suspend, error = _handle_cleanup_source(
                migration, src_node, src_rpc, tgt_node, tgt_rpc)
        except RPCException as exc:
            return _suspend_task(task, migration, str(exc))

        if error and error is not _WAIT:
            return _suspend_task(task, migration, error)
        if error is _WAIT or suspend:
            return _suspend_task(task, migration, migration.error_message or "waiting")
        if done:
            # Signal cleanup_source_done to group.
            group = db.get_migration_group_by_id(group_id)
            if migration_id not in group.cleanup_source_done:
                group.cleanup_source_done.append(migration_id)
                group.write_to_db(db.kv_store)
            migration.phase = LVolMigration.PHASE_COMPLETED
            migration.status = LVolMigration.STATUS_DONE
            migration.completed_at = int(time.time())
            migration.write_to_db(db.kv_store)
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "Group worker migration completed successfully"
            task.write_to_db(db.kv_store)
            tasks_events.task_updated(task)
            migration_events.migration_completed(migration)
            logger.info(f"Group worker {migration_id[:8]}: CLEANUP_SOURCE done → COMPLETED")
            return True
        migration.write_to_db(db.kv_store)
        task.write_to_db(db.kv_store)
        return False

    # --- CLEANUP_TARGET ---
    if phase == LVolMigration.PHASE_CLEANUP_TARGET:
        try:
            done, suspend, error = _handle_cleanup_target(
                migration, tgt_node, tgt_rpc, src_rpc=src_rpc)
        except RPCException as exc:
            return _suspend_task(task, migration, str(exc))

        if error and error is not _WAIT:
            return _suspend_task(task, migration, error)
        if error is _WAIT or suspend:
            return _suspend_task(task, migration, migration.error_message or "waiting")
        if done:
            migration.status = (LVolMigration.STATUS_CANCELLED if migration.canceled
                                else LVolMigration.STATUS_FAILED)
            migration.completed_at = int(time.time())
            migration.write_to_db(db.kv_store)
            task.status = JobSchedule.STATUS_DONE
            task.function_result = migration.error_message or "Group worker rolled back"
            task.write_to_db(db.kv_store)
            tasks_events.task_updated(task)
            migration_events.migration_failed(migration, migration.error_message)
            logger.info(f"Group worker {migration_id[:8]}: CLEANUP_TARGET done → FAILED/CANCELLED")
            return True
        migration.write_to_db(db.kv_store)
        task.write_to_db(db.kv_store)
        return False

    return _fail_task(task, migration, f"Group worker: unknown phase {phase}")


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
