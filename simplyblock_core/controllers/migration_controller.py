# coding=utf-8
"""
migration_controller.py – control-plane logic for live volume migration.

A live migration moves an lvol (and its complete snapshot chain) from one
storage node to another without any sustained I/O interruption.  The
data-plane RPCs that actually transfer blob data are currently stubs marked
with # TODO(migration-rpc): replace with real RPC call.

Workflow
--------
1. Caller invokes ``start_migration(lvol_id, target_node_id)``.
2. Controller validates preconditions and builds the ordered snapshot chain.
3. A ``JobSchedule`` task (FN_LVOL_MIG) is created; the task runner drives
   the actual data-plane operations asynchronously.
4. The caller can poll ``get_migration(lvol_id)`` or ``list_migrations(cluster_id)``
   to track progress, and ``cancel_migration(migration_id)`` to abort.

Snapshot-chain ordering
-----------------------
We order snapshots for a volume by ``created_at`` (ascending = oldest first).
This matches the underlying blobstore chain: the oldest snapshot is the
deepest ancestor and must arrive on the target first.

If the volume was cloned from a snapshot (``lvol.cloned_from_snap``), the
ancestor chain is prepended (root-to-leaf order) before the volume's own
direct snapshots.  This ensures that the target node can reconstruct the
full parent chain before receiving child blobs.

Cleanup safety
--------------
A snapshot may be shared between multiple volumes (e.g. a common base
snapshot for several clones).  Before deleting a snapshot from the source
or rolling back from the target we verify that no other volume still on
that node references it through its ``cloned_from_snap`` lineage.
"""

import json
import logging
import time
import uuid

from simplyblock_core import constants, utils
from simplyblock_core.controllers import migration_events, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

# Note: JobSchedule is not imported directly here; task creation is delegated to
# tasks_controller.add_lvol_mig_task() which handles event logging consistently.

logger = logging.getLogger()
db = DBController()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_migration(lvol_id, target_node_id,
                    max_retries=constants.LVOL_MIG_MAX_RETRIES,
                    deadline_seconds=constants.LVOL_MIG_DEADLINE_SEC):
    """
    Initiate a live migration of *lvol_id* to *target_node_id*.

    Returns (migration_uuid, None) on success or (False, error_message) on
    failure.

    Preconditions checked:
    - volume exists and is online
    - target node exists, is online, and is not the current node
    - no active migration already running for this volume on its source node
    - cluster is in an active state
    """
    # --- Validate volume ---
    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError as e:
        return False, str(e)

    if lvol.status != LVol.STATUS_ONLINE:
        return False, f"Volume is not online (status={lvol.status})"

    source_node_id = lvol.node_id

    # --- Validate nodes ---
    try:
        source_node = db.get_storage_node_by_id(source_node_id)
    except KeyError as e:
        return False, str(e)

    try:
        target_node = db.get_storage_node_by_id(target_node_id)
    except KeyError as e:
        return False, str(e)

    if source_node_id == target_node_id:
        return False, "Source and target nodes must be different"

    if source_node.status != StorageNode.STATUS_ONLINE:
        return False, f"Source node is not online (status={source_node.status})"

    if target_node.status != StorageNode.STATUS_ONLINE:
        return False, f"Target node is not online (status={target_node.status})"

    cluster_id = source_node.cluster_id

    # --- Check for conflicting active migration on the same source node ---
    existing = get_active_migration_on_node(cluster_id, source_node_id)
    if existing:
        return False, (
            f"Another migration is already active on source node {source_node_id} "
            f"(migration_id={existing.uuid})"
        )

    # --- Check volume is not already being migrated ---
    active = get_active_migration_for_lvol(lvol_id, cluster_id)
    if active:
        return False, f"Volume already has an active migration (migration_id={active.uuid})"

    # --- Build snapshot migration plan ---
    snap_plan = get_snapshot_chain(lvol_id, source_node_id)

    # --- Create LVolMigration record ---
    migration = LVolMigration()
    migration.uuid = str(uuid.uuid4())
    migration.cluster_id = cluster_id
    migration.lvol_id = lvol_id
    migration.source_node_id = source_node_id
    migration.target_node_id = target_node_id
    migration.phase = LVolMigration.PHASE_SNAP_COPY
    migration.snap_migration_plan = snap_plan
    migration.snaps_migrated = []
    migration.intermediate_snaps = []
    migration.next_snap_index = 0
    migration.intermediate_snap_rounds = 0
    migration.started_at = int(time.time())
    migration.deadline = int(time.time()) + deadline_seconds if deadline_seconds else 0
    migration.max_retries = max_retries
    migration.status = LVolMigration.STATUS_NEW
    migration.write_to_db(db.kv_store)

    # --- Create backing JobSchedule task (reuses _add_task for event logging) ---
    task_uuid = tasks_controller.add_lvol_mig_task(migration)
    if not task_uuid:
        migration.status = LVolMigration.STATUS_FAILED
        migration.error_message = "Failed to create backing task"
        migration.write_to_db(db.kv_store)
        return False, migration.error_message

    migration_events.migration_created(migration)
    logger.info(
        f"Migration created: id={migration.uuid} lvol={lvol_id} "
        f"src={source_node_id} dst={target_node_id} "
        f"snaps_to_copy={len(snap_plan)}"
    )
    return migration.uuid, None


def cancel_migration(migration_id):
    """
    Cancel an active migration.  The task runner will detect the cancellation,
    stop data-plane operations, and transition to the CLEANUP_TARGET phase to
    remove any partially-copied snapshots from the target.

    Returns True on success, or (False, error_message) on failure.
    """
    try:
        migration = db.get_migration_by_id(migration_id)
    except KeyError as e:
        return False, str(e)

    if not migration.is_active():
        return False, f"Migration is not active (status={migration.status})"

    migration.canceled = True
    migration.write_to_db(db.kv_store)
    migration_events.migration_cancelled(migration)
    logger.info(f"Migration cancelled: id={migration_id} lvol={migration.lvol_id}")
    return True, None


def get_active_migration_for_lvol(lvol_id, cluster_id=None):
    """Return the active LVolMigration for *lvol_id*, or None."""
    for m in db.get_migrations(cluster_id):
        if m.lvol_id == lvol_id and m.is_active():
            return m
    return None


def get_active_migration_on_node(cluster_id, node_id):
    """
    Return any active migration whose source node is *node_id*, or None.

    Only one migration is permitted per source node at a time so that the
    snapshot-freeze constraint can be enforced cleanly.
    """
    for m in db.get_migrations(cluster_id):
        if m.source_node_id == node_id and m.is_active():
            return m
    return None


def is_migration_active_on_node(node_id, cluster_id=None):
    """Convenience predicate used by snapshot_controller to block new snapshots."""
    for m in db.get_migrations(cluster_id):
        if m.source_node_id == node_id and m.is_active():
            return True
    return False


def list_migrations(cluster_id=None, is_json=False):
    """Return a formatted list (table or JSON) of all migrations."""
    migrations = db.get_migrations(cluster_id)
    data = []
    for m in reversed(migrations):  # newest first
        data.append({
            "Migration ID": m.uuid,
            "Volume ID": m.lvol_id,
            "Source Node": m.source_node_id,
            "Target Node": m.target_node_id,
            "Phase": m.phase,
            "Status": m.status,
            "Snaps": f"{len(m.snaps_migrated)}/{len(m.snap_migration_plan)}",
            "Retries": f"{m.retry_count}/{m.max_retries}",
            "Error": m.error_message or "",
        })
    if is_json:
        return json.dumps(data, indent=2)
    return utils.print_table(data)


def get_migration(migration_id, is_json=False):
    """Return details for a single migration."""
    try:
        m = db.get_migration_by_id(migration_id)
    except KeyError as e:
        logger.error(e)
        return False
    if is_json:
        return json.dumps(m.get_clean_dict(), indent=2)
    data = [m.get_clean_dict()]
    return utils.print_table(data)


# ---------------------------------------------------------------------------
# Snapshot chain helpers
# ---------------------------------------------------------------------------

def get_snapshot_chain(lvol_id, source_node_id=None):
    """
    Return an ordered list of snapshot UUIDs that must be present on the
    target node before the volume can be migrated there.

    Order: oldest ancestor first (root of the blobstore chain), finishing
    with the most-recently-taken snapshot (direct parent of the live lvol).

    The list is built from two parts:
    a) The ancestry chain of ``lvol.cloned_from_snap`` (if the volume is a
       clone), walked upward via ``snap_ref_id`` and reversed so that root
       comes first.  This ensures the target can reconstruct the parent chain.
    b) All snapshots taken *directly* from this volume, sorted by
       ``created_at`` ascending.

    Note: snapshot UUIDs that appear in both parts are deduplicated.
    *source_node_id* narrows the snapshot scan to the source node's snapshots,
    avoiding a full cluster-wide scan.
    """
    lvol = db.get_lvol_by_id(lvol_id)
    node_id = source_node_id or lvol.node_id
    result = []
    seen = set()

    def _add(uid):
        if uid and uid not in seen:
            seen.add(uid)
            result.append(uid)

    # Part (a): clone ancestry – root → leaf
    if lvol.cloned_from_snap:
        for uid in _get_snap_ancestry(lvol.cloned_from_snap):
            _add(uid)

    # Part (b): direct snapshots of this volume on the source node, oldest first
    node_snaps = db.get_snapshots_by_node_id(node_id)
    direct = [
        s for s in node_snaps
        if s.lvol.uuid == lvol_id
        and s.status not in (SnapShot.STATUS_IN_DELETION,)
    ]
    direct.sort(key=lambda s: s.created_at)
    for snap in direct:
        _add(snap.uuid)

    return result


def _get_snap_ancestry(snap_uuid):
    """
    Walk the ``snap_ref_id`` chain from *snap_uuid* upward to the root and
    return the UUIDs in root-first order (oldest ancestor first).

    ``snap_ref_id`` points from a child snapshot to its parent snapshot.
    """
    chain = []
    current = snap_uuid
    visited = set()
    while current and current not in visited:
        visited.add(current)
        try:
            snap = db.get_snapshot_by_id(current)
        except KeyError:
            break
        chain.append(current)
        current = snap.snap_ref_id
    chain.reverse()  # oldest → newest
    return chain


# ---------------------------------------------------------------------------
# Cleanup safety helpers
# ---------------------------------------------------------------------------

def get_snaps_safe_to_delete_on_source(migration):
    """
    Return the set of snapshot UUIDs (from the migration plan) that are safe
    to delete from the *source* node after a successful migration.

    Two rules protect a snapshot from deletion:

    1. **Ownership**: only snapshots whose parent lvol IS the migrating volume
       (``snap.lvol.uuid == migration.lvol_id``) are candidates.  Snapshots
       that belong to another volume's chain (e.g. ancestor snaps inherited by
       a clone) must stay on the source until that other volume is migrated or
       deleted.

    2. **Clone reference**: even among owned candidates, remove any snapshot
       that is still referenced (directly or through ancestry) by another lvol
       on the source node via its ``cloned_from_snap`` field.

    Intermediate snapshots created during migration always belong to the
    migrating volume, so they are always included as initial candidates.
    """
    candidates = set(migration.intermediate_snaps)  # always owned by migrating lvol

    # Only include plan entries that are actually owned by the migrating volume
    for snap_uuid in migration.snap_migration_plan:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
            if snap.lvol.uuid == migration.lvol_id:
                candidates.add(snap_uuid)
            # else: belongs to another volume's chain – leave it on source
        except KeyError:
            pass  # already gone

    # Rule 2: protect snapshots still referenced by other source lvols
    source_lvols = db.get_lvols_by_node_id(migration.source_node_id)
    for lvol in source_lvols:
        if lvol.uuid == migration.lvol_id:
            continue
        if lvol.cloned_from_snap and lvol.cloned_from_snap in candidates:
            _protect_snap_and_ancestors(lvol.cloned_from_snap, candidates)

    return candidates


def get_snaps_to_delete_on_target(migration):
    """
    Return the list of snapshot UUIDs to remove from the *target* node when
    rolling back a failed or cancelled migration.

    Two rules protect a snapshot from deletion:

    1. **Pre-existing**: snapshots in ``migration.snaps_preexisting_on_target``
       were already on the target before this migration started (placed there by
       an earlier migration of a related volume, e.g. a clone).  They must not
       be touched under any circumstances.

    2. **Active reference**: snapshots still referenced by another lvol that is
       already on the target node (via ``cloned_from_snap`` ancestry).
    """
    # Rule 1: never touch pre-existing snaps
    preexisting = set(migration.snaps_preexisting_on_target)

    # Rule 2: protect snaps referenced by other target lvols
    protected: set[str] = set()
    target_lvols = db.get_lvols_by_node_id(migration.target_node_id)
    for lvol in target_lvols:
        if lvol.uuid == migration.lvol_id:
            continue
        if lvol.cloned_from_snap:
            _collect_snap_ancestry(lvol.cloned_from_snap, protected)

    return [
        uid for uid in migration.snaps_migrated
        if uid not in preexisting and uid not in protected
    ]


def _protect_snap_and_ancestors(snap_uuid, candidate_set):
    """Remove *snap_uuid* and all its ancestors from *candidate_set*."""
    current = snap_uuid
    visited = set()
    while current and current not in visited:
        visited.add(current)
        candidate_set.discard(current)
        try:
            snap = db.get_snapshot_by_id(current)
            current = snap.snap_ref_id
        except KeyError:
            break


def _collect_snap_ancestry(snap_uuid, out_set):
    """Add *snap_uuid* and all its ancestors to *out_set*."""
    current = snap_uuid
    visited = set()
    while current and current not in visited:
        visited.add(current)
        out_set.add(current)
        try:
            snap = db.get_snapshot_by_id(current)
            current = snap.snap_ref_id
        except KeyError:
            break


# ---------------------------------------------------------------------------
# Post-migration DB updates
# ---------------------------------------------------------------------------

# Suffix appended to every bdev the task runner creates on the target node
# during migration. Prevents accidental collision with real pre-existing target
# bdevs on retry / initial attempts. Must match _MIGRATION_BDEV_SUFFIX in the
# task runner.
_MIGRATION_BDEV_SUFFIX = 'm'


def apply_migration_to_db(migration, tgt_lvol_uuid=None, tgt_lvol_bdev=None):
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
        logger.error(f"apply_migration_to_db: lvol not found: {e}")
        return False

    try:
        tgt_node = db.get_storage_node_by_id(migration.target_node_id)
    except KeyError as e:
        logger.error(f"apply_migration_to_db: target node not found: {e}")
        return False

    # --- Query SPDK once for all bdevs on the target lvstore ---
    # Used to update snap_uuid, blobid on snapshots and lvol.blobid.
    # Degraded gracefully: if unreachable, location fields still get updated.
    spdk_info = {}
    try:
        tgt_rpc = tgt_node.rpc_client()
        raw = tgt_rpc.bdev_lvol_get_lvols(tgt_node.lvstore) or []
        for entry in raw:
            short = entry.get('name', '').split('/')[-1]
            if short:
                spdk_info[short] = {
                    'uuid':   entry.get('uuid', ''),
                    'blobid': entry.get('driver_specific', {}).get('lvol', {}).get('blobid', 0),
                }
        logger.info(f"apply_migration_to_db: queried {len(spdk_info)} bdevs from target lvstore {tgt_node.lvstore}")
    except Exception as e:
        logger.warning(f"apply_migration_to_db: could not query target SPDK — snap_uuid/blobid will not be updated: {e}")

    # --- Update canonical LVol record ---
    lvol.node_id  = tgt_node.get_id()
    lvol.hostname = tgt_node.hostname
    lvol.lvs_name = tgt_node.lvstore
    if tgt_lvol_bdev:
        lvol.lvol_bdev = tgt_lvol_bdev
    lvol.top_bdev = f"{tgt_node.lvstore}/{lvol.lvol_bdev}"
    if tgt_lvol_uuid:
        lvol.lvol_uuid = tgt_lvol_uuid

    # bdev_stack: the 'bdev_lvol' entry bakes in lvs_name (and name) at creation
    # time; _remove_bdev_stack() uses them to build the delete bdev composite, so
    # both must reflect target values or the delete will hit the wrong bdev.
    for entry in lvol.bdev_stack:
        if entry.get('type') == 'bdev_lvol' and 'params' in entry:
            entry['params']['lvs_name'] = tgt_node.lvstore
            if tgt_lvol_bdev:
                entry['params']['name'] = tgt_lvol_bdev

    if lvol.lvol_bdev in spdk_info:
        lvol.blobid = spdk_info[lvol.lvol_bdev]['blobid']

    # Update the nodes list (primary + all secondaries)
    lvol.nodes = [tgt_node.get_id()]
    if tgt_node.secondary_node_id:
        lvol.nodes.append(tgt_node.secondary_node_id)
    if tgt_node.tertiary_node_id:
        lvol.nodes.append(tgt_node.tertiary_node_id)

    lvol.write_to_db(db.kv_store)
    logger.info(
        f"apply_migration_to_db: updated lvol {migration.lvol_id} "
        f"node_id={tgt_node.get_id()}, lvs_name={tgt_node.lvstore}, nodes={lvol.nodes}"
    )

    # --- Update every migrated snapshot ---
    tgt_subsys_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)

    for snap_uuid in migration.snaps_migrated:
        try:
            snap = db.get_snapshot_by_id(snap_uuid)
        except KeyError:
            logger.warning(f"apply_migration_to_db: snapshot not found: {snap_uuid}")
            continue

        # Only fully update snapshots owned by the migrating volume.
        # Shared ancestry snaps (from a clone chain) belong to a different
        # volume and keep their current location until that volume migrates.
        if snap.lvol.uuid != migration.lvol_id:
            continue

        # snap_bdev: update lvstore prefix and add migration suffix.
        # On the target the bdev was created as <src_short> + _MIGRATION_BDEV_SUFFIX
        # (e.g. SNAP_xxxm) to avoid collisions with real pre-existing target bdevs.
        tgt_short = None
        if snap.snap_bdev and '/' in snap.snap_bdev:
            src_short = snap.snap_bdev.split('/', 1)[1]
            tgt_short = src_short + _MIGRATION_BDEV_SUFFIX
            snap.snap_bdev = f"{tgt_node.lvstore}/{tgt_short}"

        # SPDK-specific fields: snap_uuid (SPDK bdev UUID) and blobid
        if tgt_short and tgt_short in spdk_info:
            snap.snap_uuid = spdk_info[tgt_short]['uuid']
            snap.blobid    = spdk_info[tgt_short]['blobid']

        # snap.lvol: embedded copy of the parent lvol — update all node/location fields
        snap.lvol.node_id     = tgt_node.get_id()
        snap.lvol.hostname    = tgt_node.hostname
        snap.lvol.lvs_name    = tgt_node.lvstore
        if tgt_lvol_bdev:
            snap.lvol.lvol_bdev = tgt_lvol_bdev
        snap.lvol.top_bdev    = f"{tgt_node.lvstore}/{snap.lvol.lvol_bdev}"
        snap.lvol.nodes       = list(lvol.nodes)
        snap.lvol.subsys_port = tgt_subsys_port
        if tgt_lvol_uuid:
            snap.lvol.lvol_uuid = tgt_lvol_uuid

        snap.write_to_db(db.kv_store)
        logger.debug(f"apply_migration_to_db: updated snapshot {snap_uuid} snap_bdev={snap.snap_bdev}")

    return True


