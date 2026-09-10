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
from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError, RPCException, RPCClient
from simplyblock_core.services.hub_controller_manager import HubControllerManager
from simplyblock_core.controllers.migration_bdev_ops import delete_bdev_blocking as _delete_bdev_blocking

logger = utils.get_logger(__name__)
db = db_mod.DBController()
# Constructed explicitly here, once, rather than as a module-level singleton
# inside hub_controller_manager.py — see that module's docstring. This
# process's own manager; tasks_runner_batch_migration.py constructs its own
# separate instance, and the two coordinate the detach cooldown via the
# DB-backed HubDetachCooldown record, not shared memory.
hub_manager = HubControllerManager(db)

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


# Sentinel distinguishing "caller has no answer, query fresh" from a
# caller-supplied get_bdevs() result -- including an explicit [] (confirmed
# absent). Used by _log_spdk_bdev_size and _setup_snap_transfer to avoid
# repeating an RPC round-trip the caller already paid for.
_BDEV_INFO_UNSET = object()


def _log_spdk_bdev_size(rpc, composite_name, label, bdev_info=_BDEV_INFO_UNSET):
    """Query SPDK for *composite_name* and emit a [BDEV SIZE] log line.

    Reports num_blocks × block_size → actual_mib and sectors@512 (the sector
    count the client sees via the NVMe namespace).  Never raises.

    ``bdev_info``: pass an already-fetched get_bdevs() result to log against
    it instead of paying for a second identical RPC round-trip -- callers
    that are about to query (or just queried) the same composite for their
    own purposes should pass that result through here.
    """
    _MIB = 1048576
    try:
        info = rpc.get_bdevs(composite_name) if bdev_info is _BDEV_INFO_UNSET else bdev_info
        if not info:
            logger.warning(
                f"[BDEV SIZE] {label}: {composite_name} — bdev not found in SPDK")
            return None
        b = info[0]  # type: ignore[index]
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


# _delete_bdev_blocking lives in controllers/migration_bdev_ops.py (imported
# above as delete_bdev_blocking) -- see that module's docstring for why.


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
        {'node', 'rpc', 'ip', 'ips', 'trtype', 'port', 'node_id'}

    'ip' is the primary NIC (from _get_migration_nic); 'ips' is the full list
    of fabric-matching NIC IPs on that node (one listener per NIC).

    Port is role-specific: SRC entries use src_node.lvstore; TGT entries use
    tgt_node.lvstore.  Adding tertiary support = append one more entry to each
    list; all callers automatically handle it via loop/set operations.
    """
    def _entry(node, rpc, lvstore):
        trtype, ip = _get_migration_nic(node)
        fabric = trtype.lower()
        ips = [
            nic.ip4_address
            for nic in (node.data_nics or [])
            if nic.ip4_address and nic.trtype and nic.trtype.lower() == fabric
        ]
        if not ips:
            ips = [ip] if ip else []
        return {
            'node': node, 'rpc': rpc, 'ip': ip, 'ips': ips, 'trtype': trtype,
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
    ret = rpc.nvmf_subsystem_add_ns(nqn, new_bdev, uuid, guid, nsid=nsid)
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


def _ensure_nvmf_state_on_node(migration, lvol, nqn, path, label, owns_subsystem, ns_composite):
    """Detect and repair a target-side node restart that wiped the migration's
    NVMe-oF subsystem / listener / namespace on a single path. Raises on any
    failure to reach or repair the node; the caller decides what that means
    (mandatory for the primary, best-effort for secondary/tertiary)."""
    node_id = path['node_id']
    rpc = path['rpc']

    sub = rpc.subsystem_get(nqn)

    if not sub:
        if not owns_subsystem:
            raise RuntimeError(
                f"target {label} node {node_id[:8]} is missing subsystem {nqn} "
                f"that this migration does not own; waiting for node recovery")

        logger.warning(
            f"_ensure_nvmf_state_on_node: subsystem {nqn} missing on "
            f"{label} target node {node_id[:8]} (likely node restart) — recreating")
        lo, hi = _CNTLID_RANGES.get(label, (3, 500))
        rpc.subsystem_create(
            nqn, lvol.ha_type, lvol.uuid, min_cntlid=random.randint(lo, hi),
            max_namespaces=lvol.max_namespace_per_subsys)
        if lvol.allowed_hosts:
            _reapply_allowed_hosts(lvol, path['node'], rpc)
        for _ip in path['ips']:
            rpc.listeners_create(nqn, path['trtype'], _ip, path['port'],
                                 ana_state="inaccessible")
        ns = rpc.nvmf_subsystem_add_ns(nqn, ns_composite, lvol.uuid, lvol.guid, nsid=lvol.ns_id)
        if not ns:
            logger.warning(
                f"_ensure_nvmf_state_on_node: namespace add failed on "
                f"{label} {node_id[:8]} after subsystem recreate")
        if node_id not in migration.target_subsystem_node_ids:
            migration.target_subsystem_node_ids.append(node_id)
        logger.info(
            f"_ensure_nvmf_state_on_node: recreated subsystem+listener+ns "
            f"for migration {migration.uuid} on {label} {node_id[:8]}")
        return

    # Subsystem present — verify our listener survived.
    listeners = rpc.listeners_list(nqn) or []
    listener_addrs = {
        (ls.get('address', {}).get('traddr'), str(ls.get('address', {}).get('trsvcid')))
        for ls in listeners
    }
    for _ip in path['ips']:
        if (_ip, str(path['port'])) not in listener_addrs:
            logger.warning(
                f"_ensure_nvmf_state_on_node: listener {_ip}:{path['port']} "
                f"missing on {label} target node {node_id[:8]} (likely node "
                f"restart) — recreating as inaccessible")
            rpc.listeners_create(nqn, path['trtype'], _ip, path['port'],
                                 ana_state="inaccessible")

    # Namespace check — only on nodes whose namespace lifecycle we own;
    # overlap nodes legitimately still point at the SRC bdev pre-cutover
    # (the namespace swap is _handle_lvol_migrate's job, not ours).
    if owns_subsystem:
        ns_list = sub.get('namespaces', []) if isinstance(sub, dict) else []
        has_ns = any(ns.get('uuid') == lvol.uuid for ns in ns_list)
        if not has_ns:
            logger.warning(
                f"_ensure_nvmf_state_on_node: namespace for {lvol.uuid} "
                f"missing on {label} target node {node_id[:8]} — re-adding")
            ns = rpc.nvmf_subsystem_add_ns(nqn, ns_composite, lvol.uuid, lvol.guid, nsid=lvol.ns_id)
            if not ns:
                logger.warning(
                    f"_ensure_nvmf_state_on_node: namespace re-add failed "
                    f"on {label} {node_id[:8]}")


def _ensure_and_prune_target_paths(migration, lvol, tgt_node, tgt_paths):
    """
    Detect and repair a target-side node restart that wiped the migration's
    NVMe-oF subsystem / listener / namespace. Called ONCE, right before
    cutover (from _handle_lvol_migrate, before the freeze/transfer/ANA-flip
    sequence) — subsystem/listener/namespace state on the target only matters
    at cutover, so there is no need to poll for it during PHASE_SNAP_COPY.

    Every path is attempted regardless of an earlier failure (the caller may
    be running this after an already-irreversible step — e.g. batch
    migration's cutover, which runs this after bdev_lvol_batch_final_step —
    so bailing out early is not always safe). The target primary is always
    kept in the returned list, whether or not its own check succeeded, so
    callers can rely on tgt_paths[0] still being the primary; its failure is
    reported separately via the returned error string, and it is up to the
    caller whether that means suspend-without-charging-retry-budget (solo
    migration, before its irreversible step) or just a logged, non-fatal
    warning (batch migration, after it).

    Secondary/tertiary are always best-effort: a failure there is logged and
    that path is dropped from the returned list instead of failing anything
    — the rest of the ANA-flip sequence only operates on whatever's in the
    returned list, so a dropped replica's cutover state is simply skipped for
    this attempt. If that node is still down when it eventually restarts, the
    ordinary per-lvol restart reconciliation in storage_node_ops.py (keyed
    off lvol.node_id, which points at the new primary once cutover
    completes) recreates its subsystem the normal way — no special handling
    needed here.

    Returns (pruned_tgt_paths, primary_error). primary_error is None unless
    the target primary itself could not be verified/repaired.
    """
    nqn = lvol.nqn
    owned_node_ids = set(migration.target_subsystem_node_ids or [])
    short_bdev = _lvol_tgt_bdev_name(lvol.lvol_bdev)
    ns_bdev_short = f"crypto_{short_bdev}" if lvol.crypto_bdev else short_bdev
    ns_composite = f"{tgt_node.lvstore}/{ns_bdev_short}"

    pruned_paths = []
    primary_error = None
    for path in tgt_paths:
        node_id = path['node_id']
        label = _target_role_label(node_id, tgt_node)
        is_primary = (node_id == tgt_node.get_id())
        owns_subsystem = node_id in owned_node_ids

        try:
            _ensure_nvmf_state_on_node(migration, lvol, nqn, path, label,
                                       owns_subsystem, ns_composite)
            pruned_paths.append(path)
        except Exception as e:
            if is_primary:
                primary_error = str(e)
                pruned_paths.append(path)  # keep it -- caller decides what a primary failure means
            else:
                logger.warning(
                    f"_ensure_and_prune_target_paths: {label} {node_id[:8]} "
                    f"unreachable/repair failed — skipping its cutover state "
                    f"for this attempt: {e}")
    return pruned_paths, primary_error


# ---------------------------------------------------------------------------
# Transfer-context cleanup helpers
# ---------------------------------------------------------------------------


def _cleanup_final_migration(src_rpc, ctx, tgt_rpc=None, rollback_target=False,
                             tgt_sec_rpc=None, tgt_ter_rpc=None,
                             nqn=None, lvol_uuid=None, subsystem_created_on_target=False,
                             tgt_all_nodes=None, tgt_lvs_name=None):
    """Clean up after a final lvol migration attempt.

    The hub controller is never touched here on either path — it is owned
    and lifecycle-managed entirely by hub_manager's own activity-based idle
    timeout, not by this function.

    On the rollback path (rollback_target=True) the target lvol/subsystem
    are torn down so a retry starts clean.

    ``nqn``/``lvol_uuid``/``subsystem_created_on_target`` must come from the
    caller (the lvol record and migration.target_subsystem_node_ids) —
    transfer_context never carries an nqn/ns_id/ownership entry for this
    stage, so reading them from ``ctx`` here silently no-ops the subsystem
    cleanup entirely.
    """
    # The hub controller is intentionally left attached here, even on
    # rollback: it's managed entirely by hub_manager's own activity-based
    # idle timeout (IDLE_TIMEOUT with no acquire()s). A retry of this same
    # migration will just reuse it via acquire() instead of paying the
    # reattach + DETACH_COOLDOWN cost, and a sibling migration to the same
    # target isn't disrupted.
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
                                      secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                      all_nodes=tgt_all_nodes, lvs_name=tgt_lvs_name)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite}: {e}")


# ---------------------------------------------------------------------------
# Phase handlers
# ---------------------------------------------------------------------------


# Sentinel distinguishing "caller has no answer, query fresh" from a caller-
# supplied get_bdevs() result (including an explicit [], i.e. "confirmed
# absent") for _setup_snap_transfer's existing_bdev_info param below.
_BDEV_INFO_UNSET = object()


def _setup_snap_transfer(snap, snap_index, src_node, tgt_node,
                         src_rpc, tgt_rpc, trtype,
                         tgt_sec=None, sec_rpc=None, tgt_ter=None, ter_rpc=None,
                         lvol_size_mib=None, migration=None,
                         existing_bdev_info=_BDEV_INFO_UNSET):
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

    ``existing_bdev_info``: every caller already runs its own get_bdevs(tgt_composite)
    pre-check (to decide whether to reuse an owned bdev or clean up a stale one)
    immediately before calling this function, which then repeated the identical
    query for its own reuse-vs-create decision -- two RPC round-trips for the
    same fact. Callers that already have a trustworthy answer (the bdev was
    confirmed absent, or confirmed present and owned) can pass that result
    straight through here instead of paying for a second lookup.
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
    # Step 1: create target lvol on primary, or reuse if already owned by this migration.
    # Pre-cleanup skips deletion of owned bdevs so we can reuse them here on retry
    # rather than paying the create cost again.
    if existing_bdev_info is _BDEV_INFO_UNSET:
        _bdev_info = tgt_rpc.get_bdevs(tgt_composite)
    else:
        _bdev_info = existing_bdev_info
    if _bdev_info:
        logger.info(
            f"[REUSE] snap={snap_uuid[:8]} reusing owned writable bdev {tgt_composite}")
        _log_spdk_bdev_size(tgt_rpc, tgt_composite, f"TGT snap[{snap_uuid[:8]}] reuse",
                           bdev_info=_bdev_info)
        if migration is not None and tgt_composite not in migration.target_snap_bdevs:
            migration.target_snap_bdevs.append(tgt_composite)
            migration.write_to_db(db.kv_store)
    else:
        ok, err = migration_controller._ensure_lvstore_primary_leader(
            tgt_rpc, tgt_node.lvstore, tgt_node.get_id())
        if not ok:
            return None, err
        ret = tgt_rpc.create_lvol(snap_short, size_in_mib, tgt_node.lvstore, ndcs=_ndcs, npcs=_npcs)
        if not ret:
            return None, f"Failed to create target lvol for snap {snap_uuid}"
        _bdev_info = tgt_rpc.get_bdevs(tgt_composite)
        _log_spdk_bdev_size(tgt_rpc, tgt_composite, f"TGT snap[{snap_uuid[:8]}] post-create",
                           bdev_info=_bdev_info)
        if migration is not None and tgt_composite not in migration.target_snap_bdevs:
            migration.target_snap_bdevs.append(tgt_composite)
            migration.write_to_db(db.kv_store)

    # Step 2: register on secondary/tertiary if not already there.
    # On a normal first pass the secondary has no knowledge of this bdev yet;
    # on a retry the secondary may already be registered — skip in that case.
    # If registration fails we clean up the primary bdev and abort.
    sec_registered = False
    ter_registered = False
    if tgt_sec and sec_rpc:
        if not _bdev_info:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                      lvs_name=tgt_node.lvstore)
            except Exception as e:
                logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
            return None, f"Could not get bdev info for {tgt_composite} after creation"
        snap_blobid = _bdev_info[0]['driver_specific']['lvol']['blobid']  # type: ignore[index]
        snap_uuid_on_tgt = _bdev_info[0]['uuid']  # type: ignore[index]
        if sec_rpc.get_bdevs(tgt_composite):
            sec_registered = True
            logger.info(f"Secondary already has {tgt_composite}; skipping registration")
        else:
            ret_sec = sec_rpc.bdev_lvol_register(
                snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
                _priority_class)
            if not ret_sec:
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                except Exception as e:
                    logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
                return None, f"bdev_lvol_register on secondary failed for snap {snap_uuid}"
            sec_registered = True
        if tgt_ter and ter_rpc:
            if ter_rpc.get_bdevs(tgt_composite):
                ter_registered = True
            else:
                ret_ter = ter_rpc.bdev_lvol_register(
                    snap_short, tgt_node.lvstore, snap_uuid_on_tgt, snap_blobid,
                    _priority_class)
                if not ret_ter:
                    try:
                        _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc, ter_rpc,
                                              all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                              lvs_name=tgt_node.lvstore)
                    except Exception as e:
                        logger.warning(f"cleanup target lvol {tgt_composite} (non-fatal): {e}")
                    return None, f"bdev_lvol_register on tertiary failed for snap {snap_uuid}"
                ter_registered = True

    # Helper: clean primary, secondary, and tertiary (if registered) on error
    def _cleanup():
        try:
            _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                  secondary_rpc=sec_rpc if sec_registered else None,
                                  tertiary_rpc=ter_rpc if ter_registered else None,
                                  all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                  lvs_name=tgt_node.lvstore)
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
    ret = src_rpc.bdev_lvol_transfer(
        src_composite, 0, constants.LVOL_MIG_TRANSFER_BATCH_SIZE, hub_bdev,
        "migrate", lvol_id=tgt_map_id)
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
    # Leadership gate first: a convert on a non-leader returns success WITHOUT
    # persisting anything (the fork's non-leader branch marks the blob CLEAN
    # only) — a silent conversion error that must fail-and-retry instead.
    from simplyblock_core.controllers import lvol_controller as _lc
    if not _lc.is_node_leader(tgt_node, tgt_composite.split("/")[0]):
        return False, f"target node not LVS leader for convert of {snap_uuid}, retrying"
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
                if snap.lvol.ha_type != "single":
                    tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
                    if sec_err:
                        migration.error_message = sec_err
                        migration.write_to_db(db.kv_store)
                        # transient replica state: suspend (via error_message),
                        # don't charge the retry budget toward cleanup_target
                        return False, True, None
                    if tgt_sec:
                        sec_rpc = _make_rpc(tgt_sec)

                    # Tertiary eligibility is a property of the target node's own
                    # LVS topology (tgt_node.tertiary_node_id), not of this lvol's
                    # ha_type -- match every other subsystem's detection
                    # (snapshot_controller.delete, lvol_controller, health_controller,
                    # snapshot_monitor, storage_node_monitor, cluster_expansion,
                    # replication_final_step all check tertiary_node_id directly).
                    tgt_ter, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
                    if ter_err:
                        migration.error_message = ter_err
                        migration.write_to_db(db.kv_store)
                        # transient replica state: suspend, don't charge retries
                        return False, True, None
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
                _existing_bdev = tgt_rpc.get_bdevs(tgt_composite)
                if _existing_bdev:
                    if tgt_composite in (migration.target_snap_bdevs or []):
                        logger.info(
                            f"Owned writable bdev {tgt_composite} found — reusing for retry")
                    else:
                        logger.info(
                            f"Removing writable leftover target bdev {tgt_composite}")
                        try:
                            _delete_bdev_blocking(tgt_composite, tgt_rpc, sec_rpc, ter_rpc,
                                                  all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                                  lvs_name=tgt_node.lvstore)
                            for _ in range(10):
                                if not tgt_rpc.get_bdevs(tgt_composite):
                                    _existing_bdev = []
                                    break
                                time.sleep(0.2)
                            else:
                                # Deletion never confirmed within the polling window —
                                # state is uncertain, let _setup_snap_transfer re-query.
                                _existing_bdev = _BDEV_INFO_UNSET
                        except Exception as e:
                            logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")
                            _existing_bdev = _BDEV_INFO_UNSET

                t, err = _setup_snap_transfer(
                    snap, snap_index, src_node, tgt_node,
                    src_rpc, tgt_rpc, trtype,
                    tgt_sec=tgt_sec, sec_rpc=sec_rpc,
                    tgt_ter=tgt_ter, ter_rpc=ter_rpc,
                    lvol_size_mib=_snap_lvol_size_mib,
                    migration=migration,
                    existing_bdev_info=_existing_bdev)
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
        # Resolve secondary and tertiary once for the whole poll pass. This
        # branch runs on a fresh function invocation (tgt_sec/tgt_ter default
        # to None at the top of this function) whenever a transfer launched
        # on a prior tick is still being polled -- the common case, since
        # transfers essentially never finish within the same tick they start.
        # Tertiary was previously left unresolved here, silently skipping its
        # add_clone/convert in _post_process_snap below (the `if tgt_ter and
        # ter_rpc:` guard just evaluated false, with no error logged).
        tgt_sec, _sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
        sec_rpc = _make_rpc(tgt_sec) if tgt_sec and not _sec_err else None
        tgt_ter, _ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
        ter_rpc = _make_rpc(tgt_ter) if tgt_ter and not _ter_err else None
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
        if snap.lvol.ha_type != "single":
            tgt_sec, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
            if sec_err:
                migration.error_message = sec_err
                migration.write_to_db(db.kv_store)
                # transient replica state: suspend (via error_message),
                # don't charge the retry budget toward cleanup_target
                return False, True, None
            if tgt_sec:
                sec_rpc = _make_rpc(tgt_sec)

            # Tertiary eligibility is a property of the target node's own LVS
            # topology (tgt_node.tertiary_node_id), not of this lvol's ha_type
            # -- match every other subsystem's detection (see the identical
            # comment at the snap-copy call site above).
            tgt_ter, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
            if ter_err:
                migration.error_message = ter_err
                migration.write_to_db(db.kv_store)
                # transient replica state: suspend, don't charge retries
                return False, True, None
            if tgt_ter:
                ter_rpc = _make_rpc(tgt_ter)

        snap_short_tgt = _snap_tgt_short_name(snap)
        src_composite  = _snap_composite(src_node.lvstore, snap)
        tgt_composite  = f"{tgt_node.lvstore}/{snap_short_tgt}"

        # Pre-cleanup: if a bdev exists on the target it is a writable leftover
        # from a previous crashed run — intermediate snaps are always freshly
        # created by this migration so they can never be pre-existing.
        _existing_bdev = tgt_rpc.get_bdevs(tgt_composite)
        if _existing_bdev:
            if tgt_composite in (migration.target_snap_bdevs or []):
                logger.info(
                    f"Owned writable intermediate bdev {tgt_composite} found — reusing for retry")
            else:
                logger.info(f"Pre-cleanup: removing stale intermediate bdev {tgt_composite}")
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                          secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                    for _ in range(10):
                        if not tgt_rpc.get_bdevs(tgt_composite):
                            _existing_bdev = []
                            break
                        time.sleep(0.2)
                    else:
                        _existing_bdev = _BDEV_INFO_UNSET
                except Exception as e:
                    logger.warning(f"Pre-cleanup of {tgt_composite} failed (continuing): {e}")
                    _existing_bdev = _BDEV_INFO_UNSET

        t, err = _setup_snap_transfer(
            snap, snap_index, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc,
            tgt_ter=tgt_ter, ter_rpc=ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib,
            migration=migration,
            existing_bdev_info=_existing_bdev)
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
                                          secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
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
                                          secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                except Exception as e:
                    logger.warning(f"cleanup target snap {tgt_composite} (non-fatal): {e}")
                return False, True, (
                    f"Intermediate snap transfer {state} for {snap_uuid}")
            time.sleep(_INTERMEDIATE_POLL_INTERVAL_S)
        else:
            try:
                _delete_bdev_blocking(tgt_composite, tgt_rpc,
                                      secondary_rpc=sec_rpc, tertiary_rpc=ter_rpc,
                                      all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                      lvs_name=tgt_node.lvstore)
            except Exception as e:
                logger.warning(f"cleanup target snap {tgt_composite} (non-fatal): {e}")
            return False, True, (
                f"Intermediate snap transfer timed out for {snap_uuid}")

        ok, err = _post_process_snap(
            snap, tgt_node, tgt_rpc, migration, t,
            tgt_sec=tgt_sec, sec_rpc=sec_rpc,
            tgt_ter=tgt_ter, ter_rpc=ter_rpc)
        if not ok:
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

    # Detect and repair a target-side node restart that wiped the migration's
    # NVMe-oF subsystem/listener/namespace before cutover -- run once here,
    # right before the freeze/transfer/ANA-flip sequence below, since that
    # state is only ever consumed at cutover (not during PHASE_SNAP_COPY).
    # Target primary failures suspend without charging the retry budget
    # (mirroring PHASE_CLEANUP_TARGET/SOURCE's identical transient-connectivity
    # carve-out); secondary/tertiary failures just drop that path so the rest
    # of this function's best-effort ANA-flip steps skip it for this attempt.
    tgt_paths, _ensure_err = _ensure_and_prune_target_paths(migration, lvol, tgt_node, tgt_paths)
    if _ensure_err:
        migration.error_message = _ensure_err
        migration.write_to_db(db.kv_store)
        # transient replica state: suspend, don't charge the retry budget
        return False, True, None

    def _flip(rpc, ip, port, trtype, state, label):
        try:
            # anagrpid == the volume's namespace id: a subsystem may carry other
            # namespaces whose volumes are NOT migrating, and a subsystem-wide
            # flip would move their IO too (they share the client's controller).
            rpc.nvmf_subsystem_listener_set_ana_state(
                nqn, ip, port, trtype=trtype, ana=state, anagrpid=lvol.ns_id)
            logger.info(f"ANA {nqn} ns {lvol.ns_id} {label} {ip}:{port} → {state}")
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

    def _flip_all(rpc, ips, port, trtype, state, label):
        for _ip in ips:
            _flip(rpc, _ip, port, trtype, state, label)

    def _flip_all_required(rpc, ips, port, trtype, state, label, attempts=3):
        ok = True
        for _ip in ips:
            if not _flip_required(rpc, _ip, port, trtype, state, label, attempts):
                ok = False
        return ok

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
            _flip_all(p['rpc'], p['ips'], p['port'], p['trtype'],
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
                                         tgt_node.get_id() in (migration.target_subsystem_node_ids or [])),
                                     tgt_all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                     tgt_lvs_name=tgt_node.lvstore)
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
                                         tgt_node.get_id() in (migration.target_subsystem_node_ids or [])),
                                     tgt_all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                     tgt_lvs_name=tgt_node.lvstore)
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
        # --- Gate: check target secondary/tertiary state before creating on
        # target primary. tgt_sec/tgt_ter were already resolved unconditionally
        # above (node topology, not lvol.ha_type) -- match that here instead of
        # re-deriving readiness from ha_type. ---
        _, sec_err = _get_target_secondary_node(tgt_node, src_node.get_id())
        if sec_err:
            migration.error_message = sec_err
            migration.write_to_db(db.kv_store)
            # transient replica state: suspend (via error_message),
            # don't charge the retry budget toward cleanup_target
            return False, True, None
        _, ter_err = _get_target_tertiary_node(tgt_node, src_node.get_id())
        if ter_err:
            migration.error_message = ter_err
            migration.write_to_db(db.kv_store)
            # transient replica state: suspend, don't charge retries
            return False, True, None

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
                # Hub controller left attached — hub_manager owns its
                # lifecycle entirely via its own idle timeout.
                try:
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc,
                                          secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
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
                # Hub controller left attached — see comment above.
                try:
                    _delete_bdev_blocking(tgt_lvol_composite, tgt_rpc,
                                          secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
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
                _flip_all(p['rpc'], p['ips'], p['port'], p['trtype'],
                          "inaccessible", f"SRC-{p['node_id'][:8]}(pre-freeze)")

        # Step 5: start final migration — synchronous: blocks until SPDK completes
        # the IO drain and delta copy.  Returns success/failure directly; no polling needed.
        logger.info(
            f"[IO-FREEZE] {_now_ms()} bdev_lvol_final_migration starting: "
            f"lvol={lvol.uuid} src={src_lvol_composite} tgt_snap={tgt_snap_composite}")
        try:
            ret = src_rpc.bdev_lvol_transfer_final_step(
                src_lvol_composite, tgt_map_id, tgt_snap_composite,
                constants.LVOL_MIG_TRANSFER_BATCH_SIZE, hub_bdev, "migrate")
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
                # Hub controller left attached — see comment above; this is a
                # retryable suspend, not an abandoned migration.
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
        if not _flip_all_required(primary_tgt['rpc'], primary_tgt['ips'], primary_tgt['port'],
                                   primary_tgt['trtype'], "optimized",
                                   f"TGT-{primary_tgt['node_id'][:8]}"):
            return False, False, (
                "ANA flip: TGT primary→optimized failed after retries "
                "— aborting to keep SRC paths accessible")

        # Secondary TGT paths best-effort — clients survive without them
        for tgt in tgt_paths[1:]:
            _flip_all(tgt['rpc'], tgt['ips'], tgt['port'], tgt['trtype'],
                      "non_optimized", f"TGT-{tgt['node_id'][:8]}")

        # Step 3 (no-overlap): all SRC paths → inaccessible
        for src in src_paths:
            _flip_all(src['rpc'], src['ips'], src['port'], src['trtype'],
                      "inaccessible", f"SRC-{src['node_id'][:8]}")
    else:
        # Step 1: first non-overlap TGT → optimized. Must succeed before
        # making any SRC path inaccessible.
        non_overlap_tgt = next(
            (t for t in tgt_paths if t['node_id'] not in overlap_ids), None)
        if non_overlap_tgt:
            if not _flip_all_required(non_overlap_tgt['rpc'], non_overlap_tgt['ips'],
                                       non_overlap_tgt['port'], non_overlap_tgt['trtype'],
                                       "optimized", f"TGT-{non_overlap_tgt['node_id'][:8]}(pre)"):
                return False, False, (
                    "ANA flip: non-overlap TGT primary→optimized failed after retries "
                    "— aborting to keep SRC paths accessible")

        # Step 2: overlap SRC paths → inaccessible at SRC port
        for src in src_paths:
            if src['node_id'] in overlap_ids:
                _flip_all(src['rpc'], src['ips'], src['port'], src['trtype'],
                          "inaccessible", f"SRC-{src['node_id'][:8]}(overlap)")

        # Step 3: non-overlap SRC paths → inaccessible
        for src in src_paths:
            if src['node_id'] not in overlap_ids:
                _flip_all(src['rpc'], src['ips'], src['port'], src['trtype'],
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
        if not _flip_all_required(primary_tgt['rpc'], primary_tgt['ips'], primary_tgt['port'],
                                   primary_tgt['trtype'], "optimized",
                                   f"TGT-{primary_tgt['node_id'][:8]}"):
            return False, False, (
                "ANA flip: TGT primary→optimized (step 5) failed after retries")
        for tgt in tgt_paths[1:]:
            _flip_all(tgt['rpc'], tgt['ips'], tgt['port'], tgt['trtype'],
                      "non_optimized", f"TGT-{tgt['node_id'][:8]}")

    # Step 6: overlap TGT paths → remove old SRC listener if port changed
    for tgt in tgt_paths:
        if tgt['node_id'] in overlap_ids:
            old_port = src_port_by_id.get(tgt['node_id'])
            if old_port and old_port != tgt['port']:
                for _ip in tgt['ips']:
                    try:
                        tgt['rpc'].listeners_del(nqn, tgt['trtype'], _ip, old_port)
                        logger.info(
                            f"Removed old SRC listener {_ip}:{old_port} "
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



def _delete_intermediate_snaps_on_target(migration, tgt_rpc, tgt_sec_rpc=None, tgt_ter_rpc=None,
                                         tgt_all_nodes=None, tgt_lvs_name=None,
                                         src_rpc=None, src_sec_rpc=None, src_ter_rpc=None,
                                         src_all_nodes=None, src_lvs_name=None):
    """
    Delete migration-created intermediate ('shrink') snapshots from wherever
    each one actually lives after a successful migration.

    Most rounds' snap.snap_bdev holds the target composite path (e.g.
    LVS_TGT/SNAP_xxxm), updated by apply_migration_to_db(). But a round whose
    OWN transfer never completed (superseded by a later round, e.g. a
    multi-round intermediate sequence) never got that update -- snap.snap_bdev
    is still the ORIGINAL source composite. Routing that delete through the
    current target's lvstore name/node-list makes the leader-routed async
    delete's lvstore mismatch what it's operating on, which SPDK rejects --
    _delete_bdev_blocking then raises before ever reaching its poll/sync
    phases, silently caught below, leaking the blob's metadata on every
    replica (observed run 2026-08-22, group B LVOL_30's round-0 snap
    SNAP_611: async delete fired and was rejected, zero follow-up poll or
    sync calls ever appeared in the SPDK logs). Resolve the lvstore actually
    present in snap.snap_bdev and route to the matching node-list/rpc set
    (target's or source's) instead of assuming it's always the target's.

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

        composite = snap.snap_bdev  # target path if its round's transfer completed, else still source
        actual_lvs = composite.split('/', 1)[0] if composite and '/' in composite else None

        if actual_lvs == tgt_lvs_name:
            _rpc, _sec_rpc, _ter_rpc, _all_nodes, _lvs_name = (
                tgt_rpc, tgt_sec_rpc, tgt_ter_rpc, tgt_all_nodes, tgt_lvs_name)
        elif src_lvs_name and actual_lvs == src_lvs_name:
            logger.info(
                f"Intermediate snap {composite}: this round's transfer never completed "
                f"(still on source lvstore {actual_lvs}); routing delete to source")
            _rpc, _sec_rpc, _ter_rpc, _all_nodes, _lvs_name = (
                src_rpc, src_sec_rpc, src_ter_rpc, src_all_nodes, src_lvs_name)
        else:
            logger.warning(
                f"Intermediate snap {composite}: lvstore {actual_lvs!r} matches neither "
                f"target ({tgt_lvs_name!r}) nor source ({src_lvs_name!r}); skipping delete "
                f"to avoid routing it against the wrong lvstore")
            continue

        if _rpc is None:
            logger.warning(
                f"Intermediate snap {composite}: no RPC client available for lvstore "
                f"{actual_lvs!r} (caller did not supply source routing info); skipping delete")
            continue

        if not _rpc.get_bdevs(composite):
            logger.info(
                f"Intermediate snap bdev {composite} absent; skipping SPDK delete")
        else:
            try:
                _delete_bdev_blocking(composite, _rpc,
                                      secondary_rpc=_sec_rpc, tertiary_rpc=_ter_rpc,
                                      coalescing=True,
                                      all_nodes=_all_nodes, lvs_name=_lvs_name)
                logger.info(f"Deleted intermediate snap bdev {composite}")
            except Exception as e:
                logger.warning(
                    f"Could not delete intermediate snap {composite}: {e}")

        try:
            snap.remove(db.kv_store)
            logger.info(f"Removed intermediate snap {snap_uuid} from DB")
        except Exception as e:
            logger.warning(f"Could not remove intermediate snap {snap_uuid} from DB: {e}")


def _rename_migrated_bdevs(migration, tgt_node, tgt_rpc, tgt_sec_rpc=None, tgt_ter_rpc=None,
                            warnings=None):
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
        exists'), True on success.  new_short must be the short name only
        (no lvstore prefix).

        Secondary/tertiary conflicts are non-fatal: an overlap node may already
        carry the bdev at the canonical name, so a collision there must not
        mask a successful primary rename.
        """
        try:
            ret = tgt_rpc.bdev_lvol_rename(old_composite, new_short)
            prim_exists = False
        except RPCRemoteError as exc:
            if exc.code == RPCErrorCode.invalid_params:
                logger.warning(
                    f"_do_rename prim: {old_composite!r} -> {new_short!r}: "
                    f"'File exists' (-32602) — will try fallback name"
                )
                prim_exists = True
                ret = None
            else:
                raise
        logger.debug(f"_do_rename prim: {old_composite!r} -> {new_short!r}: ret={ret!r}")
        for role, rpc in [("sec", tgt_sec_rpc), ("ter", tgt_ter_rpc)]:
            if rpc:
                try:
                    rpc.bdev_lvol_rename(old_composite, new_short)
                    logger.debug(f"_do_rename {role}: {old_composite!r} -> {new_short!r}: ok")
                except RPCRemoteError as exc:
                    logger.warning(
                        f"_rename_migrated_bdevs: {role} rename {label} "
                        f"{old_composite!r} -> {new_short!r}: non-fatal "
                        f"(code={exc.code}: {exc})")
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
            msg = (
                f"bdev rename {current_short!r} -> {canonical!r} failed (File exists); "
                f"retried as fallback {fallback!r}"
            )
            logger.warning(f"_rename_migrated_bdevs: {msg}")
            if warnings is not None:
                warnings.append(msg)
            ret2 = _do_rename(old, fallback, label)
            if ret2 == _EXISTS:
                skip_msg = (
                    f"bdev rename {current_short!r}: both {canonical!r} and {fallback!r} "
                    f"exist — left as-is"
                )
                logger.warning(f"_rename_migrated_bdevs: {skip_msg}")
                if warnings is not None:
                    warnings.append(skip_msg)
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
    Best-effort source cleanup after a successful migration.  The lvol is
    already live on the target — this phase only removes source-side artifacts
    and updates DB records.

    All failures are logged and collected; the migration is always marked done
    regardless of cleanup outcome.  Never suspends, never transitions to
    CLEANUP_TARGET.

    Returns (True, False, None) always.
    """
    ctx = migration.transfer_context or {}
    _warnings: list = []

    # --- First entry: initialize cleanup state ---
    if ctx.get('stage') != 'cleanup_src':
        tgt_lvol_uuid = ctx.get('tgt_lvol_uuid')
        tgt_lvol_bdev = ctx.get('tgt_lvol_bdev')
        source_snap_bdevs_saved = ctx.get('source_snap_bdevs', {})
        source_lvol_bdev_saved  = ctx.get('source_lvol_bdev', '')
        if not source_lvol_bdev_saved:
            logger.error(
                "source_lvol_bdev missing from transfer_context; "
                "source bdev deletion will be skipped")
            _warnings.append("source_lvol_bdev not in ctx; source bdev not deleted")

        # Build the safe-to-delete list, cross-checking each snap exists on the
        # target before we touch the source.  If the target is unreachable, skip
        # all source snap deletions to avoid accidental data loss.
        _snaps_to_delete_src: list = []
        try:
            to_delete_all = migration_controller.get_snaps_safe_to_delete_on_source(migration)
            tgt_lvols = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
            tgt_names = {e.get('name', '').split('/')[-1] for e in tgt_lvols}
            for snap_uuid in to_delete_all:
                try:
                    snap = db.get_snapshot_by_id(snap_uuid)
                    _snap_bdev = snap.snap_bdev or ''
                    _primary  = _snap_bdev.split('/', 1)[1] if '/' in _snap_bdev else _snap_bdev
                    _m_name   = _snap_tgt_short_name(snap)
                    _canonical = _snap_short_name(snap)
                    _am_name  = _canonical + _MIGRATION_BDEV_SUFFIX_DONE
                    if any(n in tgt_names for n in (_primary, _m_name, _canonical, _am_name)):
                        _snaps_to_delete_src.append(snap_uuid)
                    else:
                        logger.warning(
                            f"Target missing snapshot {_m_name} ({snap_uuid}); "
                            "skipping source delete to protect data")
                        _warnings.append(f"target missing snap {_m_name}; source copy kept")
                except KeyError:
                    pass  # already gone from DB; safe to skip
        except Exception as _ve:
            logger.warning(
                f"Could not verify snapshots on target ({_ve}); "
                "skipping all source snap deletions")
            _warnings.append(f"snap target-verification failed: {_ve}")
            _snaps_to_delete_src = []

        ctx = {
            'stage': 'cleanup_src',
            'tgt_lvol_uuid': tgt_lvol_uuid,
            'tgt_lvol_bdev': tgt_lvol_bdev,
            'hub_ctrl_name': (migration.transfer_context or {}).get('hub_ctrl_name'),
            'source_snap_bdevs': source_snap_bdevs_saved,
            'source_lvol_bdev':  source_lvol_bdev_saved,
            'snaps_to_delete':   _snaps_to_delete_src,
            'cleanup_warnings':  _warnings,
        }
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    src_sec = _get_source_secondary_node(src_node)
    src_sec_rpc = _make_rpc(src_sec) if src_sec else None
    src_ter = _get_source_tertiary_node(src_node)
    src_ter_rpc = _make_rpc(src_ter) if src_ter else None

    # --- Delete source snapshots (best-effort, leader-routed) ---
    # Use the verified list from first-entry; on crash-recovery re-run (ctx already
    # at 'cleanup_src') snaps_to_delete was saved, so re-deletes are safe (idempotent).
    source_snap_bdevs = ctx.get('source_snap_bdevs', {})
    for snap_uuid in ctx.get('snaps_to_delete', []):
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            bdev_name = (source_snap_bdevs.get(snap_uuid)
                        or f"{src_node.lvstore}/{_snap_short_name(snap)}")
            try:
                _delete_bdev_blocking(bdev_name, src_rpc,
                                      secondary_rpc=src_sec_rpc, tertiary_rpc=src_ter_rpc,
                                      all_nodes=[n for n in [src_node, src_sec, src_ter] if n],
                                      lvs_name=src_node.lvstore)
                logger.info(f"Deleted source bdev {bdev_name}")
            except Exception as e:
                logger.warning(f"delete source bdev {bdev_name}: {e}")
        except KeyError:
            logger.warning(f"Source snapshot {snap_uuid} not found in DB; skipping")

    # --- Source NVMe-oF subsystem teardown (best-effort) ---
    # Batch group workers share ONE subsystem across every member -- deleting
    # it here, per worker, would mean up to N-1 redundant attempts before the
    # group orchestrator's own _delete_source_subsystem() runs once, after
    # the barrier, on the master thread. Skip it here for group workers;
    # single-lvol migrations (no group) still own their own subsystem and
    # must still delete it themselves.
    lvol = None
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
        if migration.migration_group_id:
            logger.info(f"Step 8: source subsystem delete deferred to group "
                       f"orchestrator (worker of group {migration.migration_group_id[:8]})")
        else:
            logger.info(f"Step 8: removing source NVMe-oF subsystem {lvol.nqn}")
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
        logger.warning(f"Source subsystem cleanup failed: {e}")

    # --- Source lvol bdev deletion (best-effort, leader-routed) ---
    # Use the saved pre-apply name; apply_migration_to_db already renamed
    # lvol.lvol_bdev in the DB to the target name, so we must not use lvol.lvol_bdev.
    src_bdev_short = ctx.get('source_lvol_bdev')
    if lvol is not None and src_bdev_short:
        src_lvol_composite = f"{src_node.lvstore}/{src_bdev_short}"
        try:
            _delete_bdev_blocking(
                src_lvol_composite, src_rpc,
                secondary_rpc=src_sec_rpc, tertiary_rpc=src_ter_rpc,
                all_nodes=[n for n in [src_node, src_sec, src_ter] if n],
                lvs_name=src_node.lvstore)
            logger.info(f"Deleted source lvol bdev {src_lvol_composite}")
        except Exception as e:
            logger.warning(f"Source lvol delete failed: {e}")

    # --- DB update ---
    tgt_lvol_uuid = ctx.get('tgt_lvol_uuid')
    tgt_lvol_bdev = ctx.get('tgt_lvol_bdev')
    migration.transfer_context = {}
    _warnings = list(ctx.get('cleanup_warnings', []))
    if not _apply_migration_to_db(
            migration, tgt_lvol_uuid=tgt_lvol_uuid, tgt_lvol_bdev=tgt_lvol_bdev):
        logger.error("apply_migration_to_db failed; lvol DB record may still point to source")
        _warnings.append("DB update failed; lvol record may still reference source node")

    # --- Target-side artifact cleanup: intermediate snaps + bdev renames ---
    tgt_sec, _ = _get_target_secondary_node(tgt_node, src_node.get_id())
    tgt_sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, src_node.get_id())
    tgt_ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None
    try:
        if migration.intermediate_snaps:
            _delete_intermediate_snaps_on_target(
                migration, tgt_rpc, tgt_sec_rpc, tgt_ter_rpc,
                tgt_all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                tgt_lvs_name=tgt_node.lvstore,
                # A round whose own transfer never completed (superseded by a
                # later round) leaves snap.snap_bdev on the SOURCE composite --
                # pass source routing too so that case gets deleted from the
                # right place instead of being mis-routed against the target's
                # lvstore and silently rejected. See the function's docstring.
                src_rpc=src_rpc, src_sec_rpc=src_sec_rpc, src_ter_rpc=src_ter_rpc,
                src_all_nodes=[n for n in [src_node, src_sec, src_ter] if n],
                src_lvs_name=src_node.lvstore)
        _rename_migrated_bdevs(migration, tgt_node, tgt_rpc, tgt_sec_rpc, tgt_ter_rpc,
                               warnings=_warnings)
    except Exception as e:
        logger.warning(f"Target artifact cleanup (rename/intermediate snaps) failed: {e}")
        _warnings.append(f"target rename/intermediate-snap cleanup failed: {e}")

    # Record any partial-cleanup notes and always mark done.
    if _warnings:
        migration.error_message = "source cleanup partial: " + "; ".join(str(w) for w in _warnings)
        migration.write_to_db(db.kv_store)

    return True, False, None


def _handle_cleanup_target(migration, tgt_node, tgt_rpc, src_rpc=None, src_node=None):
    """
    Roll back a failed or cancelled migration: remove any partially-created
    target lvol/subsystem, then delete all snapshots copied to the target.

    Each deletion uses _delete_bdev_blocking (async-start → poll → sync-finalize
    on primary and secondary).  Idempotent: "not found" (status 2) is treated as
    already done, so a crash-recovery re-run is safe.

    Overlap safety: a target node that is also one of this lvol's SOURCE
    replica paths (shared/overlap topology) still has its namespace pointing
    at the SRC bdev pre-cutover -- it is the live path a client is currently
    using, not a spare "target" namespace. Rollback must never touch the
    subsystem/namespace on such a node; only non-overlap target-only nodes
    are safe to tear down.

    Returns (done: bool, suspend: bool, error: str|None).
    """

    # Hub controller left attached here too — hub_manager owns its lifecycle
    # entirely via its own idle timeout now; nothing in the migration runners
    # calls detach_now() any more.

    ctx = migration.transfer_context or {}
    tgt_sec, _ = _get_target_secondary_node(tgt_node, migration.source_node_id)
    tgt_sec_rpc = _make_rpc(tgt_sec) if tgt_sec else None
    tgt_ter, _ = _get_target_tertiary_node(tgt_node, migration.source_node_id)
    tgt_ter_rpc = _make_rpc(tgt_ter) if tgt_ter else None

    overlap_ids = set()
    if src_node is not None:
        try:
            _, _, overlap_ids = _build_paths(src_node, tgt_node, src_rpc, tgt_rpc)
        except Exception as e:
            logger.warning(
                f"cleanup_target: could not compute overlap nodes, treating "
                f"none as overlap (safer would be all -- proceeding with caution): {e}")

    # --- Step 0: delete dangling target lvol/subsystems from a failed LVOL_MIGRATE ---
    # Also handles the pre-create case where bdev/subsystems were set up by
    # create_migration() but migration was cancelled before LVOL_MIGRATE completed.
    if ctx.get('stage') != 'cleanup_tgt':
        tgt_lvol_composite = ctx.get('tgt_lvol_composite')
        nqn = ctx.get('nqn')

        # Per-node ownership: migration.target_subsystem_node_ids is the
        # authoritative record of which nodes had their subsystem *created*
        # by this migration (see _ensure_and_prune_target_paths). An overlap node
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
            if tgt_node.get_id() in overlap_ids:
                logger.info(
                    f"cleanup_target: skip subsystem/ns teardown on overlap "
                    f"node {tgt_node.get_id()[:8]} (still serving live SRC path)")
            else:
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
                    if _extra_node.get_id() in overlap_ids:
                        logger.info(
                            f"cleanup_target: skip {_label} subsystem/ns teardown on "
                            f"overlap node {_extra_node.get_id()[:8]} (still serving live SRC path)")
                        continue
                    try:
                        migration_controller.cleanup_subsystem_or_ns(
                            _nqn_to_clean, migration.lvol_id,
                            _extra_node.get_id() in owned_node_ids, _extra_rpc)
                    except Exception as e:
                        logger.warning(f"cleanup target {_label} subsystem {_nqn_to_clean}: {e}")

        # Delete target migration bdev — prefer ctx composite, fall back to derived name.
        # The existence check uses the primary; on connection failure (node offline or
        # restarting) we assume the bdev may still exist and attempt the delete anyway.
        # _delete_bdev_blocking uses execute_on_leader_with_failover so the request is
        # routed to the current LVS leader even when the primary is down.
        _bdev_to_delete = tgt_lvol_composite or _pre_bdev
        if _bdev_to_delete:
            try:
                _bdev_present = bool(tgt_rpc.get_bdevs(_bdev_to_delete))
            except Exception:
                _bdev_present = True  # primary unreachable; attempt delete via leader failover
            if _bdev_present:
                try:
                    _delete_bdev_blocking(_bdev_to_delete, tgt_rpc,
                                          secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                          all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                    logger.info(f"Deleted target lvol {_bdev_to_delete}")
                except Exception as e:
                    _emsg = str(e).lower()
                    if "not found" in _emsg or "no such" in _emsg or "enoent" in _emsg:
                        logger.info(f"Target lvol {_bdev_to_delete} already gone")
                    else:
                        # Transient failure (e.g. LVS leaderless while node restarts).
                        # Suspend and retry; do NOT mark cleanup done with orphaned bdevs.
                        migration.error_message = f"delete {_bdev_to_delete}: {e}"
                        logger.warning(
                            f"delete target lvol {_bdev_to_delete} (transient, will retry): {e}")
                        return False, True, None

        ctx = {'stage': 'cleanup_tgt'}
        migration.transfer_context = ctx
        migration.write_to_db(db.kv_store)

    # --- Delete target snapshots (blocking, idempotent) ---
    # Iterate target_snap_bdevs (recorded at bdev creation time) in reverse so
    # children/leaves are deleted before parents/roots (SPDK open-ref constraint).
    # This catches both completed transfers (previously in snaps_migrated only) and
    # in-flight bdevs whose transfer was cancelled before completion.
    #
    # Protection: skip snaps referenced by other lvols already on the target.
    _allowed_uuids = set(migration_controller.get_snaps_to_delete_on_target(migration))
    _protected_bases: set = set()
    for _uuid in migration.snaps_migrated:
        if _uuid in migration.snaps_preexisting_on_target or _uuid in _allowed_uuids:
            continue
        try:
            _s = db.get_snapshot_by_id(_uuid)
            _sbase = _s.snap_bdev.split('/', 1)[-1]
            if _sbase.endswith(_MIGRATION_BDEV_SUFFIX):
                _sbase = _sbase[:-len(_MIGRATION_BDEV_SUFFIX)]
            _protected_bases.add(_sbase)
        except KeyError:
            continue

    for _stored_path in reversed(migration.target_snap_bdevs):
        if '/' not in _stored_path:
            continue
        _lvstore, _short_m = _stored_path.rsplit('/', 1)
        _short_base = (
            _short_m[:-len(_MIGRATION_BDEV_SUFFIX)]
            if _short_m.endswith(_MIGRATION_BDEV_SUFFIX) else _short_m
        )
        if _short_base in _protected_bases:
            logger.info(
                f"Target bdev {_stored_path} protected (referenced by sibling); skipping")
            continue
        _am_name = _short_base + _MIGRATION_BDEV_SUFFIX_DONE
        # Find which name variant the bdev lives under.  If the primary is
        # unreachable (node offline or restarting), assume it may still exist
        # under the original name and route the delete via the LVS leader.
        bdev_name = None
        _primary_unreachable = False
        for _n in (_short_m, _short_base, _am_name):
            _cand = f"{_lvstore}/{_n}"
            try:
                if tgt_rpc.get_bdevs(_cand):
                    bdev_name = _cand
                    break
            except Exception:
                _primary_unreachable = True
                break
        if _primary_unreachable and bdev_name is None:
            bdev_name = f"{_lvstore}/{_short_m}"  # default; delete is idempotent on not-found
        if not bdev_name:
            logger.info(
                f"Target bdev {_stored_path} not found in any variant; "
                f"skipping (already cleaned up)")
            continue
        try:
            _delete_bdev_blocking(bdev_name, tgt_rpc,
                                  secondary_rpc=tgt_sec_rpc, tertiary_rpc=tgt_ter_rpc,
                                  all_nodes=[n for n in [tgt_node, tgt_sec, tgt_ter] if n],
                                  lvs_name=tgt_node.lvstore)
            logger.info(f"Deleted target snapshot bdev {bdev_name}")
        except Exception as e:
            _emsg = str(e).lower()
            if "not found" in _emsg or "no such" in _emsg or "enoent" in _emsg:
                logger.info(f"Target snapshot bdev {bdev_name} already gone")
            else:
                # Transient failure (e.g. LVS leaderless while node restarts).
                migration.error_message = f"delete {bdev_name}: {e}"
                logger.warning(
                    f"delete target snapshot bdev {bdev_name} (transient, will retry): {e}")
                return False, True, None

    migration.transfer_context = {}
    migration.target_lvol_bdev = ""
    migration.target_subsystem_nqn = ""
    # For a failover restart, preserve target_subsystem_node_ids so that
    migration.target_subsystem_node_ids = []
    migration.target_snap_bdevs = []
    migration.write_to_db(db.kv_store)
    return True, False, None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _budget_suspend(task, migration, migration_id, error_msg):
    """Charge retry budget and suspend; redirect to cleanup_target when exhausted."""
    migration.retry_count += 1
    migration.error_message = error_msg
    task.function_result = error_msg
    if migration.retry_count >= migration.max_retries:
        logger.error(
            f"Migration {migration_id} exceeded max retries "
            f"({migration.max_retries}); entering cleanup_target"
        )
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
        migration.current_job_id = ""
        # Write task first so the runner can always re-enter and attempt cleanup,
        # even if the migration write below fails.
        task.write_to_db(db.kv_store)
        migration.write_to_db(db.kv_store)
        migration_events.migration_phase_changed(migration)
        return False
    return _suspend_task(task, migration, error_msg)


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
        return _budget_suspend(task, migration, migration_id, "source node not found")

    try:
        tgt_node = db.get_storage_node_by_id(migration.target_node_id)
    except KeyError:
        return _budget_suspend(task, migration, migration_id, "target node not found")

    # Cleanup phases proceed regardless of node status: deletes go through LVS
    # leadership, so a downed node doesn't block the cleanup path.
    _is_cleanup_phase = migration.phase in (
        LVolMigration.PHASE_CLEANUP_TARGET, LVolMigration.PHASE_CLEANUP_SOURCE)
    if not _is_cleanup_phase:
        if src_node.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
            return _budget_suspend(
                task, migration, migration_id,
                f"source node not online (status={src_node.status})")

    if tgt_node.status != StorageNode.STATUS_ONLINE:
        if (migration.phase in (LVolMigration.PHASE_SNAP_COPY,
                                LVolMigration.PHASE_LVOL_MIGRATE)
                and not migration.canceled):
            # Target went offline mid-migration — clean up and fail.
            # The user may start a fresh migration once the cluster recovers.
            logger.warning(
                f"Migration {migration_id}: target node offline "
                f"(status={tgt_node.status}) during {migration.phase}; "
                f"entering cleanup"
            )
            migration.error_message = (
                f"target node offline (status={tgt_node.status}); migration failed"
            )
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return False
        if not _is_cleanup_phase:
            # cleanup phases are exempt: deletes go through LVS leadership;
            # subsystems are lost on node restart anyway.
            return _suspend_task(
                task, migration, f"target node not online (status={tgt_node.status})")

    # --- Cluster health ---
    # Cleanup phases are exempt: they do all deletions through LVS leadership
    # (which routes to the secondary when the primary is down) and the DB update
    # needs no node RPC at all.  Suspending cleanup on cluster health means a
    # completed migration stays stuck if the source node goes through a brief
    # STATUS_UNREADY / STATUS_IN_ACTIVATION window during recovery.
    cluster = db.get_cluster_by_id(migration.cluster_id)
    if cluster.status not in Cluster.MUTABLE_STATUSES:
        if not _is_cleanup_phase:
            return _suspend_task(
                task, migration, f"cluster not active (status={cluster.status})",
                charge_retry=False)

    # Expansion-first ordering: defer while a cluster expansion is open —
    # even between the expand task's retries, when the cluster status is
    # momentarily ACTIVE (see tasks_controller.defer_task_for_expansion).
    if tasks_controller.get_active_cluster_expand_task(task.cluster_id):
        return _suspend_task(
            task, migration, "cluster expansion in progress, deferring",
            charge_retry=False)

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
    next_phase = ""

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
            done, suspend, error = _handle_cleanup_target(migration, tgt_node, tgt_rpc, src_rpc=src_rpc, src_node=src_node)
            next_phase = ""  # terminal — done-handler always sets STATUS_FAILED/CANCELLED

        else:
            _fail_task(task, migration, f"unknown phase: {phase}")
            return True
    except RPCException as exc:
        logger.warning(f"Migration {migration_id} RPC error in phase {phase}: {exc}")
        # Re-read the target node to detect if it went offline mid-operation.
        # If so, trigger failover cleanup (same logic as the pre-tick status gate).
        try:
            _fresh_tgt = db.get_storage_node_by_id(migration.target_node_id)
        except KeyError:
            _fresh_tgt = tgt_node
        if (_fresh_tgt.status != StorageNode.STATUS_ONLINE
                and phase in (LVolMigration.PHASE_SNAP_COPY,
                              LVolMigration.PHASE_LVOL_MIGRATE)
                and not migration.canceled):
            logger.warning(
                f"Migration {migration_id}: target offline after RPC error "
                f"(status={_fresh_tgt.status}); entering cleanup"
            )
            migration.error_message = f"target node offline during {phase}: {exc}"
            migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
            migration.current_job_id = ""
            migration.write_to_db(db.kv_store)
            task.write_to_db(db.kv_store)
            migration_events.migration_phase_changed(migration)
            return False
        # During CLEANUP_TARGET, RPC failures are transient connectivity waits
        # (node restarting or secondary taking over leadership).  Don't charge
        # the retry budget — suspend and let the runner retry when the node recovers.
        # Note: CLEANUP_SOURCE never raises here (all its exceptions are handled
        # internally) — this guard is a safety net only.
        if phase in (LVolMigration.PHASE_CLEANUP_TARGET,
                     LVolMigration.PHASE_CLEANUP_SOURCE):
            return _suspend_task(task, migration, str(exc))
        # Not a node-offline event — treat as a retryable operation failure
        # and let the retry budget decide whether to continue or clean up.
        error = str(exc)

    # --- Handle error / suspend ---
    if error:
        # Operation failure – increment retry counter.
        migration.retry_count += 1
        migration.error_message = error
        task.function_result = error

        if migration.retry_count >= migration.max_retries:
            if phase not in (LVolMigration.PHASE_SNAP_COPY,
                             LVolMigration.PHASE_LVOL_MIGRATE):
                # Already past the migration — never redirect to CLEANUP_TARGET.
                # CLEANUP_SOURCE failures must not roll back a completed migration.
                logger.error(
                    f"Migration {migration_id} exceeded max retries in phase "
                    f"{phase}; suspending for operator review (not entering cleanup_target)")
                return _suspend_task(task, migration, error)
            logger.error(
                f"Migration {migration_id} exceeded max retries "
                f"({migration.max_retries}); entering cleanup_target"
            )
            task.retry += 1
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
    logger.info(
        f"Group worker {migration.uuid[:8]}: DIAG snap {snap_uuid[:8]} raw-transferred "
        f"(pending tree reconstruction), lvol={migration.lvol_id[:8] if migration.lvol_id else None}, "
        f"snaps_transferred_group now={list(migration.snaps_transferred_group)}")
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
            return False, True, _g_sec_err
        if _g_ter_err:
            migration.error_message = _g_ter_err
            migration.write_to_db(db.kv_store)
            return False, True, _g_ter_err
        _g_sec_rpc = _make_rpc(_g_tgt_sec) if _g_tgt_sec else None
        _g_ter_rpc = _make_rpc(_g_tgt_ter) if _g_tgt_ter else None

        _existing_bdev = tgt_rpc.get_bdevs(tgt_composite)
        if _existing_bdev:
            if tgt_composite in (migration.target_snap_bdevs or []):
                logger.info(
                    f"Owned writable bdev {tgt_composite} found — reusing for retry")
            else:
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc, _g_sec_rpc, _g_ter_rpc,
                                          all_nodes=[n for n in [tgt_node, _g_tgt_sec, _g_tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                    # No post-delete confirmation poll here (unlike the solo-path
                    # callers) — state after this point is uncertain, so let
                    # _setup_snap_transfer re-query rather than assuming deleted.
                    _existing_bdev = _BDEV_INFO_UNSET
                except Exception as e:
                    logger.warning(f"Group worker: pre-cleanup of {tgt_composite} failed: {e}")
                    _existing_bdev = _BDEV_INFO_UNSET

        t, err = _setup_snap_transfer(
            snap, plan.index(snap_uuid), src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=_g_tgt_sec, sec_rpc=_g_sec_rpc,
            tgt_ter=_g_tgt_ter, ter_rpc=_g_ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib,
            migration=migration,
            existing_bdev_info=_existing_bdev)
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


def _handle_group_intermediate(migration, src_node, tgt_node, src_rpc, tgt_rpc, target_round=0):
    """
    INTERMEDIATE phase for a group worker.

    Takes one intermediate ("shrink") snapshot per round and transfers it to
    the target, skipping add_clone/convert (same as snap_copy). After this
    the worker signals intermediates_done to the group and waits for either
    another round or batch_result.

    ``target_round`` is the group's current intermediate_round. If this
    worker has already completed that round (migration.intermediate_snap_rounds
    > target_round), it's done for now. Otherwise -- including when it was
    previously done for an earlier round but the group has since asked for
    another synchronized round -- it resets and takes a fresh snapshot.

    Returns (done: bool, suspend: bool, error: str|None).
    """
    trtype, _ = _get_migration_nic(tgt_node)
    ctx = migration.transfer_context or {}

    # If we already took and transferred the intermediate snap for the round
    # the group is currently on, we're done. Otherwise the group has asked
    # for another round since we last finished -- fall through and redo.
    if ctx.get('stage') == 'intermediate_done':
        if migration.intermediate_snap_rounds > target_round:
            return True, False, None
        ctx = {}
        migration.transfer_context = {}

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
            return False, True, _g_sec_err
        if _g_ter_err:
            migration.error_message = _g_ter_err
            migration.write_to_db(db.kv_store)
            return False, True, _g_ter_err
        _g_sec_rpc = _make_rpc(_g_tgt_sec) if _g_tgt_sec else None
        _g_ter_rpc = _make_rpc(_g_tgt_ter) if _g_tgt_ter else None

        _existing_bdev = tgt_rpc.get_bdevs(tgt_composite)
        if _existing_bdev:
            if tgt_composite in (migration.target_snap_bdevs or []):
                logger.info(
                    f"Owned writable intermediate bdev {tgt_composite} found — reusing for retry")
            else:
                try:
                    _delete_bdev_blocking(tgt_composite, tgt_rpc, _g_sec_rpc, _g_ter_rpc,
                                          all_nodes=[n for n in [tgt_node, _g_tgt_sec, _g_tgt_ter] if n],
                                          lvs_name=tgt_node.lvstore)
                    # No post-delete confirmation poll here — state after this
                    # point is uncertain, so let _setup_snap_transfer re-query.
                    _existing_bdev = _BDEV_INFO_UNSET
                except Exception as e:
                    logger.warning(f"Group intermediate: pre-cleanup of {tgt_composite} failed: {e}")
                    _existing_bdev = _BDEV_INFO_UNSET

        t, err = _setup_snap_transfer(
            snap, snap_index, src_node, tgt_node,
            src_rpc, tgt_rpc, trtype,
            tgt_sec=_g_tgt_sec, sec_rpc=_g_sec_rpc,
            tgt_ter=_g_tgt_ter, ter_rpc=_g_ter_rpc,
            lvol_size_mib=_snap_lvol_size_mib,
            migration=migration,
            existing_bdev_info=_existing_bdev)
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


def _group_worker_budget_suspend(task, migration, group_id, error_msg):
    """Charge retry budget for a group worker; fail the WHOLE GROUP when this
    worker's own budget is exhausted.

    The top-level task_runner's retry-ceiling check (see the `if error:` block
    after phase dispatch) never runs for group workers -- they're routed to
    _group_worker_phase_dispatch before that point. Without this, a worker
    hitting a persistent error just suspended forever via plain _suspend_task,
    never reaching a terminal state; the barrier it was blocking
    (snap_copy_done / intermediates_done / cleanup_source_done) never noticed
    it was stuck, so the whole group hung instead of failing.
    """
    migration.retry_count += 1
    migration.error_message = error_msg
    task.function_result = error_msg
    if migration.retry_count >= migration.max_retries:
        logger.error(
            f"Group worker {migration.uuid[:8]}: exceeded max retries "
            f"({migration.max_retries}); entering cleanup_target: {error_msg}")
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
        migration.current_job_id = ""
        task.write_to_db(db.kv_store)
        migration.write_to_db(db.kv_store)
        migration_events.migration_phase_changed(migration)

        # This worker will never signal done to its barrier now -- fail the
        # whole group rather than let siblings (and the orchestrator) wait
        # on it forever.
        try:
            group = db.get_migration_group_by_id(group_id)
            if group.phase not in (LVolMigrationGroup.PHASE_CLEANUP_TARGET,
                                   LVolMigrationGroup.PHASE_CLEANUP_SOURCE,
                                   LVolMigrationGroup.PHASE_COMPLETED):
                group.phase = LVolMigrationGroup.PHASE_CLEANUP_TARGET
                group.error_message = (
                    f"worker {migration.uuid[:8]} (lvol={migration.lvol_id}) "
                    f"exceeded max retries: {error_msg}")
                group.write_to_db(db.kv_store)
                logger.error(
                    f"Group {group_id[:8]}: failing whole group — worker "
                    f"{migration.uuid[:8]} exhausted its retry budget")
        except KeyError:
            # Group may already be removed/cleaned up by another workflow.
            # We keep worker cleanup flow idempotent by not re-raising.
            logger.warning(
                f"Group {group_id[:8]} not found while propagating worker "
                f"{migration.uuid[:8]} retry-budget exhaustion; continuing.")
        return False
    return _suspend_task(task, migration, error_msg)


def _group_worker_phase_dispatch(task, migration, phase, src_node, tgt_node, src_rpc, tgt_rpc):
    """
    Complete phase dispatcher for FN_LVOL_MIG tasks that belong to a batch
    migration group (``migration.migration_group_id`` is set).

    Manages the group worker state machine:
      SNAP_COPY   → transfer owned snaps (no add_clone/convert)
                  → signal snap_copy_done to group → wait for INTERMEDIATE
      LVOL_MIGRATE (repurposed as the intermediate phase for workers)
                  → take + transfer 1 intermediate snap for the current round
                  → signal intermediates_done → wait for either another
                    synchronized round or batch_result
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
            try:
                done, suspend, error = _handle_group_snap_copy(
                    migration, src_node, tgt_node, src_rpc, tgt_rpc)
            except RPCException as exc:
                # Charge this worker's own retry budget and report failure to
                # the group -- never decide/roll back unilaterally (see
                # _group_worker_budget_suspend's docstring).
                return _group_worker_budget_suspend(task, migration, group_id, str(exc))
            if error:
                return _group_worker_budget_suspend(task, migration, group_id, error)
            if suspend:
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

    # --- LVOL_MIGRATE (group worker: take intermediate round(s) + wait for batch_result) ---
    if phase == LVolMigration.PHASE_LVOL_MIGRATE:
        if migration_id not in group.intermediates_done:
            # A sibling may have already failed and told the group to roll
            # back while we were mid-retry ourselves -- notice it immediately
            # instead of continuing to loop until our own budget runs out.
            group = db.get_migration_group_by_id(group_id)
            if group.phase == LVolMigrationGroup.PHASE_CLEANUP_TARGET:
                migration.phase = LVolMigration.PHASE_CLEANUP_TARGET
                migration.write_to_db(db.kv_store)
                return _group_worker_phase_dispatch(
                    task, migration, LVolMigration.PHASE_CLEANUP_TARGET,
                    src_node, tgt_node, src_rpc, tgt_rpc)

            try:
                done, suspend, error = _handle_group_intermediate(
                    migration, src_node, tgt_node, src_rpc, tgt_rpc,
                    target_round=group.intermediate_round)
            except RPCException as exc:
                # Charge this worker's own retry budget and report failure to
                # the group -- never decide/roll back unilaterally (see
                # _group_worker_budget_suspend's docstring).
                return _group_worker_budget_suspend(task, migration, group_id, str(exc))
            if error:
                return _group_worker_budget_suspend(task, migration, group_id, error)
            if suspend:
                return _suspend_task(task, migration, migration.error_message or "waiting")
            if done:
                group = db.get_migration_group_by_id(group_id)
                if migration_id not in group.intermediates_done:
                    # Below the round cap, check whether this worker's dirty
                    # delta is still too large to freeze quickly at cutover --
                    # if so, flag it so the orchestrator starts another
                    # synchronized round for every member (see
                    # LVolMigrationGroup's INTERMEDIATE docstring).
                    needs_more = False
                    if group.intermediate_round + 1 < constants.LVOL_MIG_MAX_INTERMEDIATE_SNAPS:
                        try:
                            lvol = db.get_lvol_by_id(migration.lvol_id)
                            src_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"
                            delta = _get_lvol_delta_bytes(src_rpc, src_composite)
                            needs_more = (
                                delta is None
                                or delta > constants.LVOL_MIG_INTERMEDIATE_SNAP_THRESHOLD_BYTES)
                        except Exception as e:
                            logger.warning(
                                f"Group worker {migration_id[:8]}: delta check failed "
                                f"(assuming another round is needed): {e}")
                            needs_more = True
                    if needs_more and migration_id not in group.intermediate_more_needed:
                        group.intermediate_more_needed.append(migration_id)
                    group.intermediates_done.append(migration_id)
                    group.write_to_db(db.kv_store)
                    logger.info(
                        f"Group worker {migration_id[:8]}: signalled intermediates_done "
                        f"({len(group.intermediates_done)}/{group.member_count()})"
                        + (" [delta still high, requesting another round]" if needs_more else ""))
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

        if error:
            return _group_worker_budget_suspend(task, migration, group_id, error)
        if suspend:
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
                migration, tgt_node, tgt_rpc, src_rpc=src_rpc, src_node=src_node)
        except RPCException as exc:
            return _suspend_task(task, migration, str(exc))

        if error:
            return _suspend_task(task, migration, error)
        if suspend:
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


def _suspend_task(task, migration, reason, charge_retry=True):
    task.status = JobSchedule.STATUS_SUSPENDED
    task.function_result = reason
    if charge_retry:
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
_STATUS_NEW_TIMEOUT_SECONDS = 300  # 5 minutes


def _cancel_stale_new_migrations(cluster_id):
    """Auto-cancel migrations stuck in STATUS_NEW for longer than the timeout.

    A migration in STATUS_NEW is waiting for the operator to call
    migrate-continue (start_migration).  If it hasn't been continued within
    5 minutes, cancel it so resources on the target node are released and the
    operator gets a clear signal to retry from scratch.
    """
    now = datetime.datetime.now()
    for migration in db.get_migrations(cluster_id):
        if migration.status != LVolMigration.STATUS_NEW:
            continue
        if not migration.create_dt:
            continue
        try:
            created = datetime.datetime.fromisoformat(migration.create_dt)
        except ValueError:
            continue
        age_seconds = (now - created).total_seconds()
        if age_seconds > _STATUS_NEW_TIMEOUT_SECONDS:
            logger.warning(
                f"Migration {migration.uuid} (lvol={migration.lvol_id}) has been "
                f"in STATUS_NEW for {age_seconds:.0f}s (>{_STATUS_NEW_TIMEOUT_SECONDS}s); "
                "auto-cancelling"
            )
            try:
                migration_controller.cancel_migration(migration.uuid)
            except Exception as e:
                logger.error(
                    f"Failed to auto-cancel stale migration {migration.uuid}: {e}"
                )


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
                _cancel_stale_new_migrations(cl.get_id())
                for task in db.get_active_migration_tasks(cl.get_id()):
                    # Lease gate: skip a task another live runner host owns, so
                    # two replicas can't both drive the same migration's
                    # multi-phase data-plane state-machine concurrently.
                    if not tasks_controller.claim_task(task):
                        logger.info(f"LVol-migration task {task.uuid} owned by another runner host; skipping")
                        continue
                    with tasks_controller.task_lease_heartbeat(task):
                        task_runner(task)

        time.sleep(3)
