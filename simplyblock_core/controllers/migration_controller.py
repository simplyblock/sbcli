# coding=utf-8
"""
migration_controller.py – control-plane logic for live volume migration.

A live migration moves an lvol (and its complete snapshot chain) from one
storage node to another without any sustained I/O interruption.  The
data-plane RPCs that actually transfer blob data are currently stubs marked
with # TODO(migration-rpc): replace with real RPC call.

Workflow
--------
1. Caller invokes ``create_migration(lvol_id, target_node_id)`` to set up
   target infrastructure and receive the migration_id and connect strings.
2. Caller invokes ``start_migration(migration_id)`` to promote the record to
   PHASE_SNAP_COPY and launch the task runner.
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

import logging
import random
import time
import uuid
from datetime import datetime

from simplyblock_core import constants
from simplyblock_core.controllers import migration_events, tasks_controller, snapshot_controller
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_core.controllers.host_auth import _reapply_allowed_hosts
from simplyblock_core.kms import create_kms_connection, lvol_dek_path, pool_kek_name
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_connect import NvmeConnectEntry
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import convert_size

# Note: JobSchedule is not imported directly here; task creation is delegated to
# tasks_controller.add_lvol_mig_task() which handles event logging consistently.

logger = logging.getLogger()
db = DBController()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_migration(migration_id,
                    max_retries=constants.LVOL_MIG_MAX_RETRIES,
                    deadline_seconds=constants.LVOL_MIG_DEADLINE_SEC):
    """
    Promote a PHASE_PRE_CREATED migration record to PHASE_SNAP_COPY and launch
    the task runner.  Always call create_migration first to set up target
    infrastructure and obtain the migration_id and connect strings.

    Returns migration_uuid on success; raises ValueError on failure.
    """
    try:
        migration = db.get_migration_by_id(migration_id)
    except KeyError:
        raise ValueError(f"Migration {migration_id} not found")

    if migration.phase != LVolMigration.PHASE_PRE_CREATED:
        raise ValueError(
            f"Migration {migration_id} is not in PHASE_PRE_CREATED "
            f"(phase={migration.phase})"
        )

    lvol_id = migration.lvol_id
    target_node_id = migration.target_node_id

    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError as e:
        raise ValueError(str(e))

    if lvol.status != LVol.STATUS_ONLINE:
        raise ValueError(f"Volume is not online (status={lvol.status})")

    source_node_id = lvol.node_id

    try:
        source_node = db.get_storage_node_by_id(source_node_id)
    except KeyError as e:
        raise ValueError(str(e))

    try:
        target_node = db.get_storage_node_by_id(target_node_id)
    except KeyError as e:
        raise ValueError(str(e))

    if source_node_id == target_node_id:
        raise ValueError("Source and target nodes must be different")

    if source_node.status != StorageNode.STATUS_ONLINE:
        raise ValueError(f"Source node is not online (status={source_node.status})")

    if target_node.status != StorageNode.STATUS_ONLINE:
        raise ValueError(f"Target node is not online (status={target_node.status})")

    snap_plan = get_snapshot_chain(lvol_id, source_node_id)

    snaps_found_on_target = [s for s in snap_plan if _is_snap_on_node(s, target_node_id)]
    snap_migration_plan = [s for s in snap_plan if s not in snaps_found_on_target]

    migration.source_node_id = source_node_id
    migration.phase = LVolMigration.PHASE_SNAP_COPY
    migration.snap_migration_plan = snap_migration_plan
    migration.snaps_migrated = []
    migration.snaps_preexisting_on_target = snaps_found_on_target
    migration.intermediate_snaps = []
    migration.next_snap_index = 0
    migration.intermediate_snap_rounds = 0
    migration.started_at = int(time.time())
    migration.deadline = int(time.time()) + deadline_seconds if deadline_seconds else 0
    migration.max_retries = max_retries
    migration.status = LVolMigration.STATUS_NEW
    migration.write_to_db(db.kv_store)
    logger.info(
        f"Promoting pre-created migration {migration.uuid} → PHASE_SNAP_COPY "
        f"lvol={lvol_id} src={source_node_id} dst={target_node_id}"
    )

    # --- Create backing JobSchedule task ---
    task_uuid = tasks_controller.add_lvol_mig_task(migration)
    if not task_uuid:
        migration.status = LVolMigration.STATUS_FAILED
        migration.error_message = "Failed to create backing task"
        migration.write_to_db(db.kv_store)
        raise ValueError(migration.error_message)

    migration_events.migration_created(migration)
    logger.info(
        f"Migration created: id={migration.uuid} lvol={lvol_id} "
        f"src={source_node_id} dst={target_node_id} "
        f"snaps_to_copy={len(snap_plan)}"
    )
    return migration.uuid


def cancel_migration(migration_id):
    """
    Cancel an active migration.

    For PHASE_PRE_CREATED migrations (no task runner involved yet), cleanup is
    performed inline: subsystems and the migration bdev are deleted on the
    target primary and secondary before marking the record cancelled.

    For all other active phases, sets migration.canceled = True so the task
    runner picks it up and transitions to CLEANUP_TARGET.

    Raises ValueError on failure.
    """
    try:
        migration = db.get_migration_by_id(migration_id)
    except KeyError as e:
        raise ValueError(str(e))

    if not migration.is_active():
        raise ValueError(f"Migration is not active (status={migration.status})")

    if migration.phase == LVolMigration.PHASE_PRE_CREATED:
        _cleanup_created(migration)
        migration.status = LVolMigration.STATUS_CANCELLED
        migration.write_to_db(db.kv_store)
        migration_events.migration_cancelled(migration)
        logger.info(f"Pre-created migration cleaned up and cancelled: id={migration_id} lvol={migration.lvol_id}")
        return

    migration.canceled = True
    migration.write_to_db(db.kv_store)
    migration_events.migration_cancelled(migration)
    logger.info(f"Migration cancelled: id={migration_id} lvol={migration.lvol_id}")


def _cleanup_created(migration):
    """
    Delete the subsystem(s) and migration bdev that were created during
    create_migration.  Called inline by cancel_migration when
    phase == PHASE_PRE_CREATED (no task runner is involved yet).

    Best-effort: logs errors but does not raise.
    """
    try:
        lvol = db.get_lvol_by_id(migration.lvol_id)
    except KeyError:
        logger.warning(f"_cleanup_created: lvol {migration.lvol_id} not found — skipping cleanup")
        return

    try:
        tgt_node = db.get_storage_node_by_id(migration.target_node_id)
    except KeyError:
        logger.warning(f"_cleanup_created: target node {migration.target_node_id} not found — skipping cleanup")
        return

    nqn = lvol.nqn
    _lvol_base = lvol.lvol_bdev[:-len(_MIGRATION_BDEV_SUFFIX)] if lvol.lvol_bdev.endswith(_MIGRATION_BDEV_SUFFIX) else lvol.lvol_bdev
    bdev_short = _lvol_base + _MIGRATION_BDEV_SUFFIX
    composite = f"{tgt_node.lvstore}/{bdev_short}"

    # Compute overlap: nodes shared between SRC and TGT paths.
    # Overlap nodes own their subsystem from the SRC role — we only added a
    # listener at the TGT port; remove that listener rather than the subsystem.
    src_node_ids = {migration.source_node_id}
    try:
        _src_node = db.get_storage_node_by_id(migration.source_node_id)
        if _src_node.secondary_node_id:
            src_node_ids.add(_src_node.secondary_node_id)
    except KeyError:
        pass

    tgt_node_ids = {migration.target_node_id}
    if tgt_node.secondary_node_id:
        tgt_node_ids.add(tgt_node.secondary_node_id)
    overlap_ids = src_node_ids & tgt_node_ids

    tgt_rpc = tgt_node.rpc_client()
    tgt_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)

    # Secondary cleanup
    if tgt_node.secondary_node_id:
        try:
            sec_node = db.get_storage_node_by_id(tgt_node.secondary_node_id)
            sec_rpc  = sec_node.rpc_client()
            sec_port = sec_node.get_lvol_subsys_port(tgt_node.lvstore)
            if sec_node.get_id() in overlap_ids:
                for nic in sec_node.data_nics:
                    if nic.ip4_address and nic.trtype.lower() == lvol.fabric:
                        try:
                            sec_rpc.listeners_del(nqn, nic.trtype.lower(),
                                                  nic.ip4_address, sec_port)
                        except Exception:
                            pass
                logger.info(
                    f"_cleanup_created: removed TGT-port listeners from "
                    f"overlap TGT-sec {sec_node.get_id()[:8]}")
            else:
                sec_rpc.subsystem_delete(nqn)
                logger.info(
                    f"_cleanup_created: deleted TGT-sec subsystem {nqn} "
                    f"on {sec_node.get_id()}")
        except Exception as e:
            logger.warning(f"_cleanup_created: could not clean TGT-sec: {e}")

    # Primary cleanup
    if migration.target_node_id in overlap_ids:
        for nic in tgt_node.data_nics:
            if nic.ip4_address and nic.trtype.lower() == lvol.fabric:
                try:
                    tgt_rpc.listeners_del(nqn, nic.trtype.lower(),
                                          nic.ip4_address, tgt_port)
                except Exception:
                    pass
        logger.info(
            f"_cleanup_created: removed TGT-port listeners from "
            f"overlap TGT-prim {tgt_node.get_id()[:8]}")
    else:
        try:
            tgt_rpc.subsystem_delete(nqn)
            logger.info(
                f"_cleanup_created: deleted TGT-prim subsystem {nqn} "
                f"on {tgt_node.get_id()}")
        except Exception as e:
            logger.warning(f"_cleanup_created: could not clean TGT-prim subsystem: {e}")

    # Migration bdev (always delete — we always created it)
    try:
        tgt_rpc.delete_lvol(composite)
        logger.info(f"_cleanup_created: deleted migration bdev {composite} on {tgt_node.get_id()}")
    except Exception as e:
        logger.warning(f"_cleanup_created: could not clean migration bdev: {e}")


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


def list_migrations(cluster_id=None):
    """Return a formatted list (table or JSON) of all migrations."""
    migrations = db.get_migrations(cluster_id)
    migrations = sorted(migrations, key=lambda x: x.create_dt)

    data = []
    for m in reversed(migrations):  # newest first
        logger.debug(m)
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
    return data


def get_migration(migration_id):
    """Return details for a single migration."""
    try:
        return db.get_migration_by_id(migration_id)
    except KeyError as e:
        logger.error(e)
        return False


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


def _is_snap_on_node(snap_id, node_id):
    """Return True if *snap_id* already has an instance on *node_id*."""
    try:
        snap = db.get_snapshot_by_id(snap_id)
    except KeyError:
        return False
    return any(
        inst.get('lvol', {}).get('node_id') == node_id
        for inst in (snap.instances or [])
    )


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

_MIGRATION_BDEV_SUFFIX = constants.LVOL_MIG_BDEV_SUFFIX


def _build_connect_entries(node, port, lvol, nqn, ctrl_loss_tmo, cluster, host_entry, host_nqn):
    """Build NVMe connect-string entries for each data NIC on *node* at *port*."""
    entries = []
    for nic in node.data_nics:
        ip = nic.ip4_address
        if not ip or nic.trtype.lower() != lvol.fabric:
            continue
        trtype = nic.trtype.lower()
        keep_alive_tmo = (constants.LVOL_NVME_KEEP_ALIVE_TO_TCP
                          if trtype == "tcp" else constants.LVOL_NVME_KEEP_ALIVE_TO)
        client_data_nic_str = f"--host-iface={cluster.client_data_nic}" if cluster.client_data_nic else ""
        tls_str = host_auth_str = ""
        if host_entry:
            host_auth_str = f" --hostnqn={host_nqn}"
            if host_entry.get("psk"):
                tls_str = " --tls"
            if host_entry.get("dhchap_key"):
                host_auth_str += f" --dhchap-secret={host_entry['dhchap_key']}"
            if host_entry.get("dhchap_ctrlr_key"):
                host_auth_str += f" --dhchap-ctrl-secret={host_entry['dhchap_ctrlr_key']}"
        elif host_nqn:
            host_auth_str = f" --hostnqn={host_nqn}"
        connect_cmd = (
            f"sudo nvme connect"
            f" --reconnect-delay={constants.LVOL_NVME_CONNECT_RECONNECT_DELAY}"
            f" --ctrl-loss-tmo={ctrl_loss_tmo}"
            f" --fast_io_fail_tmo={constants.LVOL_NVME_CONNECT_FAST_IO_FAIL_TO}"
            f" --nr-io-queues={cluster.client_qpair_count}"
            f" --keep-alive-tmo={keep_alive_tmo}"
            f" --transport={trtype} --traddr={ip} --trsvcid={port} --nqn={nqn}"
            f" {client_data_nic_str}{tls_str}{host_auth_str}"
        )
        entries.append(NvmeConnectEntry(
            transport=trtype,
            ip=ip,
            port=port,
            nqn=nqn,
            reconnect_delay=constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
            ctrl_loss_tmo=ctrl_loss_tmo,
            fast_io_fail_tmo=constants.LVOL_NVME_CONNECT_FAST_IO_FAIL_TO,
            nr_io_queues=cluster.client_qpair_count,
            keep_alive_tmo=keep_alive_tmo,
            host_iface=cluster.client_data_nic or "",
            connect=connect_cmd,
            tls=bool(host_entry and host_entry.get("psk")),
        ))
    return entries


def create_migration(lvol_id, target_node_id,
                         ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
                         host_nqn=None):
    """
    Pre-create the target NVMe-oF infrastructure for a future migration of
    *lvol_id* to *target_node_id*.

    Steps performed on the target primary (and secondary when HA):
      1. Create migration bdev  <lvol_bdev>m  in the target lvstore.
         Idempotent: skipped if the bdev already exists.
      2. Create NVMe-oF subsystem with the same NQN as the source lvol.
      3. Add inaccessible listeners on every data NIC.
         No namespace is added — the task runner wires it up when migration
         actually begins (PHASE_LVOL_MIGRATE).
      4. Create an LVolMigration record in PHASE_PRE_CREATED so that
         cancel_migration can tear everything down on request.

    Returns (migration_id, connect_strings) on success.
    Raises ValueError on any validation or setup failure.
    """
    db = DBController()

    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        raise ValueError(f"LVol {lvol_id} not found")

    try:
        tgt_node = db.get_storage_node_by_id(target_node_id)
    except KeyError:
        raise ValueError(f"Target node {target_node_id} not found")

    if not tgt_node.lvstore:
        raise ValueError(f"Target node {target_node_id} has no lvstore")

    existing_migration = get_active_migration_for_lvol(lvol_id, tgt_node.cluster_id)
    if existing_migration:
        if existing_migration.target_node_id != target_node_id:
            raise MigrationConflictError(
                f"An active migration for {lvol_id} already exists targeting a different node "
                f"({existing_migration.target_node_id}). Cancel it first.")
        if existing_migration.phase != LVolMigration.PHASE_PRE_CREATED:
            raise PreconditionError(
                f"Migration {existing_migration.uuid} for {lvol_id} is already past pre-create "
                f"(phase={existing_migration.phase}). Use /continue or cancel it.")

    src_node_id = lvol.node_id
    if src_node_id == target_node_id:
        raise ValueError(f"LVol {lvol_id} is already on node {target_node_id}; cannot migrate to the same node")

    try:
        src_node = db.get_storage_node_by_id(src_node_id)
    except KeyError:
        raise ValueError(f"Source node {src_node_id} not found")

    cluster = db.get_cluster_by_id(tgt_node.cluster_id)
    tgt_rpc = tgt_node.rpc_client()
    nqn = lvol.nqn
    _lvol_base = lvol.lvol_bdev[:-len(_MIGRATION_BDEV_SUFFIX)] if lvol.lvol_bdev.endswith(_MIGRATION_BDEV_SUFFIX) else lvol.lvol_bdev
    bdev_short = _lvol_base + _MIGRATION_BDEV_SUFFIX
    composite = f"{tgt_node.lvstore}/{bdev_short}"
    size_in_mib = convert_size(lvol.size, 'MiB')
    tgt_port = tgt_node.get_lvol_subsys_port(tgt_node.lvstore)

    # ── 1. Bdev ──────────────────────────────────────────────────────────────
    if not tgt_rpc.get_bdevs(composite):
        ret = tgt_rpc.create_lvol(
            bdev_short, size_in_mib, tgt_node.lvstore,
            lvol_priority_class=lvol.lvol_priority_class,
            ndcs=lvol.ndcs, npcs=lvol.npcs)
        if not ret:
            raise ValueError(f"bdev_lvol_create failed for {composite} on {target_node_id}")
        logger.info(f"create_migration: created bdev {composite}")
    else:
        logger.info(f"create_migration: bdev {composite} already exists — skipping create")

    # ── 1b. Get bdev info for secondary registration ──────────────────────────
    _bdev_info = tgt_rpc.get_bdevs(composite)
    _tgt_blobid = None
    _tgt_uuid   = None
    if _bdev_info and isinstance(_bdev_info[0], dict):
        _tgt_blobid = (_bdev_info[0].get('driver_specific', {})
                       .get('lvol', {}).get('blobid'))
        _tgt_uuid   = _bdev_info[0].get('uuid')

    # ── 1c. Set migration flag on TGT-prim ────────────────────────────────────
    if not tgt_rpc.bdev_lvol_set_migration_flag(composite):
        logger.warning(f"create_migration: bdev_lvol_set_migration_flag on primary "
                       f"failed for {composite} (may already be flagged)")

    # ── 1d. Register migration bdev on TGT-sec and TGT-ter ───────────────────
    # All HA peers need bdev_lvol_register so they can mirror writes during migration.
    _pre_sec_node = None
    if lvol.ha_type in ("ha", "ha3") and tgt_node.secondary_node_id:
        try:
            _pre_sec_node = db.get_storage_node_by_id(tgt_node.secondary_node_id)
            _sec_rpc_reg  = _pre_sec_node.rpc_client()
            if _sec_rpc_reg.get_bdevs(composite):
                logger.info(
                    f"create_migration: {composite} already on secondary "
                    f"{_pre_sec_node.get_id()} — skipping bdev_lvol_register")
            elif _tgt_blobid is not None and _tgt_uuid is not None:
                ret_sec = _sec_rpc_reg.bdev_lvol_register(
                    bdev_short, tgt_node.lvstore, _tgt_uuid, _tgt_blobid,
                    lvol.lvol_priority_class)
                if ret_sec:
                    _sec_rpc_reg.bdev_lvol_set_migration_flag(composite)
                    logger.info(
                        f"create_migration: registered {composite} on "
                        f"secondary {_pre_sec_node.get_id()}")
                else:
                    logger.warning(
                        f"create_migration: bdev_lvol_register on secondary "
                        f"{_pre_sec_node.get_id()} failed (continuing)")
            else:
                logger.warning(
                    f"create_migration: no bdev info for secondary registration of {composite}")
        except Exception as _e:
            logger.warning(
                f"create_migration: secondary registration error (continuing): {_e}")

    _pre_ter_node = None
    if tgt_node.tertiary_node_id:
        try:
            _pre_ter_node = db.get_storage_node_by_id(tgt_node.tertiary_node_id)
            _ter_rpc_reg  = _pre_ter_node.rpc_client()
            if _ter_rpc_reg.get_bdevs(composite):
                logger.info(
                    f"create_migration: {composite} already on tertiary "
                    f"{_pre_ter_node.get_id()} — skipping bdev_lvol_register")
            elif _tgt_blobid is not None and _tgt_uuid is not None:
                ret_ter = _ter_rpc_reg.bdev_lvol_register(
                    bdev_short, tgt_node.lvstore, _tgt_uuid, _tgt_blobid,
                    lvol.lvol_priority_class)
                if ret_ter:
                    _ter_rpc_reg.bdev_lvol_set_migration_flag(composite)
                    logger.info(
                        f"create_migration: registered {composite} on "
                        f"tertiary {_pre_ter_node.get_id()}")
                else:
                    logger.warning(
                        f"create_migration: bdev_lvol_register on tertiary "
                        f"{_pre_ter_node.get_id()} failed (continuing)")
            else:
                logger.warning(
                    f"create_migration: no bdev info for tertiary registration of {composite}")
        except Exception as _e:
            logger.warning(
                f"create_migration: tertiary registration error (continuing): {_e}")

    # ── 2. Subsystem + listeners on all TGT nodes ────────────────────────────
    # Compute overlap: node IDs present in both SRC path and TGT path.
    # Overlap nodes already own a live subsystem from their SRC role — we add
    # only an inaccessible listener at the TGT port.  Non-overlap nodes get a
    # full subsystem, listeners, allowed_hosts, and a namespace pointing to the
    # migration bdev so clients can pre-connect before cutover.
    src_node_ids = {src_node_id}
    if src_node.secondary_node_id:
        src_node_ids.add(src_node.secondary_node_id)
    if src_node.tertiary_node_id:
        src_node_ids.add(src_node.tertiary_node_id)

    tgt_sec_node = None
    if lvol.ha_type in ("ha", "ha3") and tgt_node.secondary_node_id:
        tgt_sec_node = (_pre_sec_node if _pre_sec_node is not None else None)
        if tgt_sec_node is None:
            try:
                tgt_sec_node = db.get_storage_node_by_id(tgt_node.secondary_node_id)
            except KeyError:
                pass

    tgt_ter_node = None
    if tgt_node.tertiary_node_id:
        tgt_ter_node = (_pre_ter_node if _pre_ter_node is not None else None)
        if tgt_ter_node is None:
            try:
                tgt_ter_node = db.get_storage_node_by_id(tgt_node.tertiary_node_id)
            except KeyError:
                pass

    tgt_node_ids = {target_node_id}
    if tgt_sec_node is not None:
        tgt_node_ids.add(tgt_sec_node.get_id())
    if tgt_ter_node is not None:
        tgt_node_ids.add(tgt_ter_node.get_id())
    overlap_ids = src_node_ids & tgt_node_ids

    # Ordered TGT entries: (node, rpc, port, min_cntlid)
    # TGT uses random cntlid values within non-overlapping ranges to avoid kernel-side
    # duplicate-cntlid rejection across consecutive migrations.  SRC occupies 1/1000/2000;
    # TGT uses 3-500 / 1003-1500 / 2003-2500 so ranges never collide.
    tgt_entries = [(tgt_node, tgt_rpc, tgt_port, random.randint(3, 500))]
    if tgt_sec_node is not None:
        _sec_rpc2  = tgt_sec_node.rpc_client()
        _sec_port2 = tgt_sec_node.get_lvol_subsys_port(tgt_node.lvstore)
        tgt_entries.append((tgt_sec_node, _sec_rpc2, _sec_port2, random.randint(1003, 1500)))
    if tgt_ter_node is not None:
        _ter_rpc2  = tgt_ter_node.rpc_client()
        _ter_port2 = tgt_ter_node.get_lvol_subsys_port(tgt_node.lvstore)
        tgt_entries.append((tgt_ter_node, _ter_rpc2, _ter_port2, random.randint(2003, 2500)))

    _TGT_LABELS = ['TGT-prim', 'TGT-sec', 'TGT-ter']

    for _i, (_node, _rpc, _port, _min_cntlid) in enumerate(tgt_entries):
        _node_id = _node.get_id()
        _tgt_label = _TGT_LABELS[_i] if _i < len(_TGT_LABELS) else f'TGT-{_i}'

        # Crypto: create key + bdev on this TGT node before touching the subsystem.
        # The NVMe-oF Target holds an exclusive_write claim on any bdev used as a
        # namespace, so the crypto bdev must be stacked before the namespace is added.
        _ns_bdev = composite
        if lvol.crypto_bdev:
            _crypto_short = f"crypto_{bdev_short}"
            if _rpc.get_bdevs(_crypto_short):
                logger.info(f"create_migration: crypto bdev {_crypto_short} "
                            f"already exists on {_node_id[:8]}")
                _ns_bdev = _crypto_short
            else:
                try:
                    with create_kms_connection(cluster) as kms:
                        _key1, _key2 = kms.get_data_encryption_keys(
                            lvol_dek_path(cluster.get_id(), lvol.get_id()),
                            pool_kek_name(lvol.pool_uuid),
                        )
                    _key_name = f"key_{_crypto_short}"
                    _ret = _rpc.lvol_crypto_key_create(_key_name, _key1, _key2)
                    if not _ret:
                        logger.warning(f"create_migration: crypto key create for "
                                       f"{_key_name} on {_node_id[:8]} failed (key may exist)")
                    _ret = _rpc.lvol_crypto_create(_crypto_short, composite, _key_name)
                    if _ret:
                        logger.info(f"create_migration: created crypto bdev "
                                    f"{_crypto_short} on {_node_id[:8]}")
                        _ns_bdev = _crypto_short
                    else:
                        logger.error(f"create_migration: bdev_crypto_create failed "
                                     f"for {_crypto_short} on {_node_id[:8]}")
                except Exception as _e:
                    logger.error(f"create_migration: crypto bdev setup on "
                                 f"{_node_id[:8]} failed: {_e}")

        subsys_min_cntlid_used = set()
        if _node_id in overlap_ids:
            # Subsystem already exists from SRC role — add inaccessible listener
            # at TGT port so clients can pre-connect to the future TGT endpoint.
            subsys = _rpc.subsystem_list(nqn)[0]
            subsys_min_cntlid_used.add(subsys.get('min_cntlid', 0))

        if _node_id in overlap_ids:
            # Subsystem already exists from SRC role — add inaccessible listener
            # at TGT port so clients can pre-connect to the future TGT endpoint.
            for nic in _node.data_nics:
                if not nic.ip4_address or nic.trtype.lower() != lvol.fabric:
                    continue
                try:
                    _rpc.listeners_create(nqn, nic.trtype.lower(), nic.ip4_address,
                                          _port, ana_state="inaccessible")
                    logger.info(
                        f"create_migration: inaccessible listener on overlap node "
                        f"{_node_id[:8]} {nic.ip4_address}:{_port}")
                except Exception as _e:
                    logger.warning(
                        f"create_migration: listener on overlap {_node_id[:8]} "
                        f"(non-fatal): {_e}")
        else:
            if not _rpc.subsystem_list(nqn):
                if _min_cntlid in subsys_min_cntlid_used:
                    _min_cntlid = _min_cntlid + 10000
                _rpc.subsystem_create(
                    nqn, lvol.ha_type, lvol.uuid, min_cntlid=_min_cntlid,
                    max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS)

            if lvol.allowed_hosts:
                try:
                    _reapply_allowed_hosts(lvol, _node, _rpc)
                except Exception as _e:
                    logger.warning(
                        f"create_migration: allowed_hosts reapply on "
                        f"{_node_id[:8]} (non-fatal): {_e}")

            for nic in _node.data_nics:
                if not nic.ip4_address or nic.trtype.lower() != lvol.fabric:
                    continue
                try:
                    _rpc.listeners_create(nqn, nic.trtype.lower(), nic.ip4_address,
                                          _port, ana_state="inaccessible")
                except Exception as _e:
                    logger.warning(
                        f"create_migration: listener on {_node_id[:8]} "
                        f"(non-fatal): {_e}")

            _ns = _rpc.nvmf_subsystem_add_ns(nqn, _ns_bdev, lvol.uuid, lvol.guid)
            if _ns:
                logger.info(
                    f"create_migration: namespace {_ns_bdev} added on "
                    f"{_tgt_label} {_node_id[:8]} nsid={_ns}")
            else:
                logger.warning(
                    f"create_migration: nvmf_subsystem_add_ns failed on "
                    f"{_tgt_label} {_node_id[:8]}")

            logger.info(
                f"create_migration: subsystem {nqn} ready on {_node_id[:8]}")

    # ── 4. Build connect strings for the target node ──────────────────────────
    host_entry = None
    if lvol.allowed_hosts and host_nqn:
        for h in lvol.allowed_hosts:
            if h["nqn"] == host_nqn:
                host_entry = h
                break

    if lvol.allowed_hosts and not host_nqn:
        raise ValueError(f"Volume {lvol_id} has allowed hosts configured; --host-nqn is required")

    out = []
    for _n, _, _p, _ in tgt_entries:
        out.extend(_build_connect_entries(_n, _p, lvol, nqn, ctrl_loss_tmo, cluster, host_entry, host_nqn))

    # ── 4. Create migration record so cancel can clean up ─────────────────────
    if existing_migration:
        logger.info(
            f"create_migration: idempotent re-call for lvol={lvol_id} target={target_node_id} "
            f"reusing migration_id={existing_migration.uuid} connect_strings={len(out)}")
        return existing_migration.uuid, out

    migration = LVolMigration()
    migration.uuid = str(uuid.uuid4())
    migration.cluster_id = tgt_node.cluster_id
    migration.lvol_id = lvol_id
    migration.source_node_id = lvol.node_id
    migration.target_node_id = target_node_id
    migration.phase = LVolMigration.PHASE_PRE_CREATED
    migration.status = LVolMigration.STATUS_NEW
    migration.snap_migration_plan = []
    migration.snaps_migrated = []
    migration.intermediate_snaps = []
    migration.started_at = int(time.time())
    migration.create_dt = str(datetime.now())
    migration.write_to_db(db.kv_store)

    logger.info(
        f"create_migration: done for lvol={lvol_id} target={target_node_id} "
        f"migration_id={migration.uuid} connect_strings={len(out)}")
    return migration.uuid, out


