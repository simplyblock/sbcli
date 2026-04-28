# coding=utf-8
import logging
import re
import time
import uuid

import boto3
from botocore.exceptions import ClientError

from simplyblock_core.controllers import backup_events, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

logger = logging.getLogger()

db_controller = DBController()


def _generate_backup_id():
    return str(uuid.uuid4())


def _next_s3_id(cluster_id):
    """Return the next cluster-wide unique s3_id (uint32) for data-plane RPCs."""
    max_id = 0
    for b in db_controller.get_backups(cluster_id):
        if b.s3_id > max_id:
            max_id = b.s3_id
    return max_id + 1


def _parse_age_string(age_str):
    """Parse age strings like '2d', '12h', '1w', '30m' into seconds."""
    match = re.match(r'^(\d+)([mhdw])$', age_str.strip())
    if not match:
        raise ValueError(f"Invalid age format: {age_str}. Use <number><m|h|d|w> e.g. 2d, 12h, 1w")
    value = int(match.group(1))
    unit = match.group(2)
    multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800}
    return value * multipliers[unit]


def _parse_schedule(schedule_str):
    """Parse schedule string like '15m,4 60m,11 24h,7' into list of (interval_seconds, keep_count) tuples.
    Returns sorted list by interval ascending. Raises ValueError on invalid input."""
    if not schedule_str or not schedule_str.strip():
        return []
    tiers = []
    for part in schedule_str.strip().split():
        parts = part.split(',')
        if len(parts) != 2:
            raise ValueError(f"Invalid schedule tier: {part}. Expected format: <interval>,<count> e.g. 15m,4")
        interval_seconds = _parse_age_string(parts[0])
        try:
            keep_count = int(parts[1])
        except ValueError:
            raise ValueError(f"Invalid keep count in tier: {part}. Must be an integer.")
        if keep_count < 1:
            raise ValueError(f"Keep count must be >= 1 in tier: {part}")
        tiers.append((interval_seconds, keep_count))
    tiers.sort(key=lambda t: t[0])
    # Validate intervals are strictly increasing
    for i in range(1, len(tiers)):
        if tiers[i][0] <= tiers[i - 1][0]:
            raise ValueError("Schedule tier intervals must be strictly increasing")
    return tiers


def _write_s3_metadata(rpc_client, backup):
    """Write backup metadata to the S3 metadata bucket.
    This metadata is needed for cross-cluster recovery."""
    metadata = {
        "backup_id": backup.uuid,
        "lvol_id": backup.lvol_id,
        "lvol_name": backup.lvol_name,
        "snapshot_id": backup.snapshot_id,
        "snapshot_name": backup.snapshot_name,
        "node_id": backup.node_id,
        "cluster_id": backup.cluster_id,
        "prev_backup_id": backup.prev_backup_id,
        "created_at": backup.created_at,
        "size": backup.size,
        "allowed_hosts": backup.allowed_hosts,
    }
    backup.s3_metadata = metadata
    # The actual S3 metadata write is done via the data plane's S3 bdev.
    # For now we store it in the backup object itself.
    # In production, this would write to the metadata bucket via S3 API.
    return metadata


def _get_latest_backup_for_lvol(lvol_id):
    """Get the most recent non-failed backup for a given lvol.

    Includes pending/in-progress backups so that chain links are set
    even when multiple backups are created in quick succession before
    the earlier ones complete.
    """
    backups = db_controller.get_backups_by_lvol_id(lvol_id)
    valid = [b for b in backups if b.status in (
        Backup.STATUS_COMPLETED, Backup.STATUS_IN_PROGRESS, Backup.STATUS_PENDING)]
    if not valid:
        return None
    valid.sort(key=lambda b: b.created_at, reverse=True)
    return valid[0]


def _compute_s3_cpu_masks(node):
    """Compute CPU masks for the S3 bdev.
    Returns (bdb_lcpu_mask, s3_lcpu_mask):
        bdb_lcpu_mask: app_thread core (SPDK lightweight thread, low overhead)
        s3_lcpu_mask: all system vCPUs (no pinning — let Linux scheduler handle
                      the AWS SDK thread pool; the data plane default would
                      wrongly pin onto SPDK reactor cores)
    """
    # SPDK thread for the bdev poller — reuse the app thread core
    bdb_lcpu_mask = 0
    if node.app_thread_mask:
        bdb_lcpu_mask = int(node.app_thread_mask, 16)

    # AWS SDK thread pool — set all system vCPU bits so threads are unconstrained
    s3_lcpu_mask = (1 << node.cpu) - 1 if node.cpu > 0 else 0

    return bdb_lcpu_mask, s3_lcpu_mask


def create_s3_bdev(node, backup_config):
    """Create the S3 bdev and attach it to a node's lvstore.
    Called during cluster activate / node restart.
    Args:
        node: StorageNode with lvstore set
        backup_config: dict from cluster.backup_config with S3/MinIO connection params
    """
    if not node.lvstore:
        return False
    rpc_client = RPCClient(
        node.mgmt_ip, node.rpc_port,
        node.rpc_username, node.rpc_password)
    s3_bdev_name = f"s3_{node.lvstore}"

    bdb_lcpu_mask, s3_lcpu_mask = _compute_s3_cpu_masks(node)

    # Step 0: Create helper poll group threads for S3 transfers
    cpu_mask = node.app_thread_mask if node.app_thread_mask else "0x1"
    try:
        ret = rpc_client.bdev_lvol_create_poller_group(cpu_mask)
        if not ret:
            logger.warning(f"Failed to create poller group on node {node.get_id()}")
        else:
            logger.info(f"S3 poller group created with mask {cpu_mask} on node {node.get_id()}")
    except Exception as e:
        # May fail if already created — not fatal
        logger.warning(f"Poller group creation returned error (may already exist): {e}")

    # Step 1: Create the S3 bdev
    try:
        ret = rpc_client.bdev_s3_create(
            name=s3_bdev_name,
            secondary_target=backup_config.get("secondary_target", 0),
            with_compression=backup_config.get("with_compression", False),
            snapshot_backups=backup_config.get("snapshot_backups", True),
            local_testing=backup_config.get("local_testing", False),
            local_endpoint=backup_config.get("local_endpoint", ""),
            access_key_id=backup_config.get("access_key_id", ""),
            secret_access_key=backup_config.get("secret_access_key", ""),
            bdb_lcpu_mask=bdb_lcpu_mask,
            s3_lcpu_mask=s3_lcpu_mask,
            s3_thread_pool_size=backup_config.get("s3_thread_pool_size", 0),
        )
        if not ret:
            logger.warning(f"Failed to create S3 bdev on node {node.get_id()}")
            return False
    except Exception as e:
        logger.error(f"Error creating S3 bdev on node {node.get_id()}: {e}")
        return False

    # Step 2: Ensure the S3 bucket exists, then register it with the S3 bdev
    bucket_name = backup_config.get("bucket_name", f"simplyblock-backup-{node.cluster_id}")
    try:
        s3_kwargs = {
            "aws_access_key_id": backup_config.get("access_key_id", ""),
            "aws_secret_access_key": backup_config.get("secret_access_key", ""),
        }
        endpoint_url = backup_config.get("local_endpoint", "")
        if endpoint_url:
            s3_kwargs["endpoint_url"] = endpoint_url
        s3_client = boto3.client("s3", **s3_kwargs)
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"S3 bucket already exists: {bucket_name}")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"S3 bucket created: {bucket_name}")
            else:
                raise
    except Exception as e:
        logger.error(f"Error ensuring S3 bucket {bucket_name} exists: {e}")
        return False

    try:
        ret = rpc_client.bdev_s3_add_bucket_name(s3_bdev_name, bucket_name)
        if not ret:
            logger.warning(f"Failed to set bucket name on S3 bdev {s3_bdev_name}")
            return False
        logger.info(f"S3 bdev bucket set: {bucket_name} on {s3_bdev_name}")
    except Exception as e:
        logger.error(f"Error setting bucket name on S3 bdev {s3_bdev_name}: {e}")
        return False

    # Step 3: Attach the S3 bdev to the lvstore
    try:
        ret = rpc_client.bdev_lvol_s3_bdev(node.lvstore, s3_bdev_name)
        if ret:
            logger.info(f"S3 bdev created and attached: {s3_bdev_name} on node {node.get_id()}")
            return True
        else:
            logger.warning(f"Failed to attach S3 bdev to lvstore on node {node.get_id()}")
            return False
    except Exception as e:
        logger.error(f"Error attaching S3 bdev on node {node.get_id()}: {e}")
        return False


def _get_snapshot_chain(snapshot):
    """Build the snapshot chain ending at this snapshot, oldest first.

    For cloned volumes, walks snap_ref_id upward.  For regular volumes
    (no snap_ref_id), collects all snapshots of the same lvol that were
    created at or before this snapshot, ordered by created_at.
    """
    if snapshot.snap_ref_id:
        # Clone-based chain: walk snap_ref_id
        chain = [snapshot]
        current = snapshot
        while current.snap_ref_id:
            try:
                parent = db_controller.get_snapshot_by_id(current.snap_ref_id)
                chain.append(parent)
                current = parent
            except KeyError:
                break
        chain.reverse()  # oldest first
        return chain

    # Regular volume: all snapshots of the same lvol up to this one
    lvol_id = snapshot.lvol.get_id() if snapshot.lvol else None
    if not lvol_id:
        return [snapshot]

    all_snaps = db_controller.get_snapshots_by_lvol_id(lvol_id)
    # Filter to snapshots created at or before this one, sort oldest first
    chain = [s for s in all_snaps if s.created_at <= snapshot.created_at]
    chain.sort(key=lambda s: s.created_at)
    return chain


def _snapshot_has_backup(snapshot_id):
    """Check if a snapshot already has a non-failed backup."""
    backups = db_controller.get_backups_by_snapshot_id(snapshot_id)
    return any(b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                            Backup.STATUS_COMPLETED) for b in backups)


def _create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup):
    """Create a single backup record and task for one snapshot.
    Returns the created Backup object."""
    backup_id = _generate_backup_id()

    backup = Backup()
    backup.uuid = backup_id
    backup.s3_id = _next_s3_id(cluster_id)
    backup.cluster_id = cluster_id
    backup.source_cluster_id = cluster_id  # local backup
    backup.lvol_id = lvol.get_id()
    backup.lvol_name = lvol.lvol_name
    backup.snapshot_id = snapshot.get_id()
    backup.snapshot_name = snapshot.snap_name
    backup.node_id = node_id
    backup.pool_uuid = lvol.pool_uuid
    backup.prev_backup_id = prev_backup.uuid if prev_backup else ""
    backup.size = snapshot.size
    backup.allowed_hosts = lvol.allowed_hosts
    backup.created_at = int(time.time())
    backup.status = Backup.STATUS_PENDING
    backup.write_to_db()

    _write_s3_metadata(None, backup)
    backup.write_to_db()

    backup_events.backup_created(cluster_id, node_id, backup)
    tasks_controller.add_backup_task(backup)

    return backup


def backup_snapshot(snapshot_id, cluster_id=None):
    """Create a backup from an existing snapshot.

    Walks the snapshot chain to ensure all ancestor snapshots are also
    backed up, since a single snapshot backup is only a delta and cannot
    be restored without its ancestors.

    Returns (backup_id, error_message) where backup_id is the ID of the
    backup for the requested snapshot.
    """
    try:
        snapshot = db_controller.get_snapshot_by_id(snapshot_id)
    except KeyError as e:
        return None, str(e)

    # Block new backups when S3 source is switched to an external cluster
    node_id = snapshot.lvol.node_id if snapshot.lvol else None
    if node_id:
        try:
            snode = db_controller.get_storage_node_by_id(node_id)
            if not is_local_backup_source(snode.cluster_id):
                return None, ("Cannot create backups while backup source is "
                              "switched to an external cluster. Switch back "
                              "to local first.")
        except KeyError:
            pass

    if not snapshot.lvol:
        return None, "Snapshot has no associated lvol"

    lvol = snapshot.lvol
    node_id = lvol.node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError as e:
        return None, str(e)

    if snode.status != StorageNode.STATUS_ONLINE:
        return None, f"Node {node_id} is not online (status: {snode.status})"

    if not cluster_id:
        cluster_id = snode.cluster_id

    snap_chain = _get_snapshot_chain(snapshot)
    chain_snapshot_ids = [snap.get_id() for snap in snap_chain]
    acquired, existing_lock = db_controller.acquire_backup_chain_locks(
        chain_snapshot_ids, snapshot_id, lvol.get_id())
    if not acquired:
        lock_snapshot = getattr(existing_lock, "requested_snapshot_id", "") or getattr(existing_lock, "snapshot_id", "")
        return None, (
            "A backup request is already preparing this snapshot chain"
            + (f" (requested snapshot {lock_snapshot})" if lock_snapshot else "")
        )

    prev_backup = _get_latest_backup_for_lvol(lvol.get_id())
    final_backup_id = None
    try:
        # Walk the snapshot chain and back up all unbacked ancestors first
        for snap in snap_chain:
            if _snapshot_has_backup(snap.get_id()):
                # Already backed up — update prev_backup pointer for chain linking
                backups = db_controller.get_backups_by_snapshot_id(snap.get_id())
                existing = next(
                    (b for b in backups if b.status in (
                        Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS,
                        Backup.STATUS_COMPLETED)),
                    None)
                if existing:
                    prev_backup = existing
                continue

            backup = _create_single_backup(snap, lvol, node_id, cluster_id, prev_backup)
            prev_backup = backup
            if snap.get_id() == snapshot_id:
                final_backup_id = backup.uuid
    finally:
        db_controller.release_backup_chain_locks(chain_snapshot_ids)

    if not final_backup_id:
        # The target snapshot was already backed up
        return None, f"Snapshot {snapshot_id} already has a backup"

    return final_backup_id, None


def restore_backup(backup_id, lvol_name, pool_id_or_name, cluster_id=None,
                   target_node_id=None):
    """Restore a backup chain into a new fully-accessible lvol.

    Creates the volume (with subsystem, listeners, namespace) via
    lvol_controller.add_lvol_ha, then schedules an async task to
    fill in the data from S3.  The volume is in STATUS_RESTORING
    until the data transfer completes.

    Args:
        target_node_id: Optional node to restore onto. If not provided,
            restores to the original backup node. Any node in the cluster
            can restore any backup because S3 keys are node-agnostic
            ({s3_id}/{mid_flag}/{extent}) and all nodes share the same
            S3 bucket and credentials.

    Returns (lvol_uuid, error_message).
    """
    from simplyblock_core.controllers import lvol_controller
    from simplyblock_core.models.lvol_model import LVol

    try:
        backup = db_controller.get_backup_by_id(backup_id)
    except KeyError as e:
        return None, str(e)

    # Verify the backup's source matches the active S3 source.
    # If the backup came from an external cluster, the S3 bdev must be
    # switched to that cluster's bucket before restoring.
    if cluster_id:
        backup_src = backup.source_cluster_id or backup.cluster_id
        try:
            cl = db_controller.get_cluster_by_id(cluster_id)
            active_src = cl.backup_source or cluster_id
            if backup_src != active_src:
                return None, (
                    f"Backup source is {backup_src[:8]} but active S3 source "
                    f"is {active_src[:8]}. Use 'sbctl backup source-switch "
                    f"{backup_src}' first.")
        except KeyError:
            pass

    # Build the backup chain
    chain = db_controller.get_backup_chain(backup_id)
    if not chain:
        return None, f"Could not build backup chain for {backup_id}"

    size = backup.size
    if size <= 0:
        return None, "Backup has no size information"

    # Determine target node: use explicit target, or fall back to backup node
    restore_node_id = target_node_id or backup.node_id

    # Validate target node is online and has an S3 bdev
    try:
        target_node = db_controller.get_storage_node_by_id(restore_node_id)
    except KeyError:
        return None, f"Target node {restore_node_id} not found"

    if target_node.status != StorageNode.STATUS_ONLINE:
        return None, (f"Target node {restore_node_id} is not online "
                      f"(status: {target_node.status})")

    if not target_node.lvstore:
        return None, f"Target node {restore_node_id} has no lvstore (S3 bdev requires lvstore)"

    original_lvol = db_controller.get_lvol_by_id(backup.lvol_id)
    logger.info(f"Backup allowed hosts: {backup.allowed_hosts}")
    lvol_id, error = lvol_controller.add_lvol_ha(
        name=lvol_name,
        size=size,
        pool_id_or_name=pool_id_or_name,
        use_crypto=bool(original_lvol.crypto_bdev),
        max_size=0,
        max_rw_iops=0,
        max_rw_mbytes=0,
        max_r_mbytes=0,
        max_w_mbytes=0,
        host_id_or_name=restore_node_id,
        ha_type="default",
        crypto_key1=original_lvol.crypto_key1,
        crypto_key2=original_lvol.crypto_key2,
        use_comp=False,
        distr_vuid=0,
        lvol_priority_class=0,
        allowed_hosts=[h["nqn"] if isinstance(h, dict) else h
                       for h in (backup.allowed_hosts or [])] or None,
        fabric="tcp",
    )
    if error or not lvol_id:
        return None, f"Failed to create restore volume: {error}"

    # Mark volume as restoring
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError:
        return None, f"Volume created but not found in DB: {lvol_id}"

    lvol.status = LVol.STATUS_RESTORING
    lvol.write_to_db()

    if not cluster_id:
        cluster_id = lvol.node_id
        try:
            snode = db_controller.get_storage_node_by_id(lvol.node_id)
            cluster_id = snode.cluster_id
        except KeyError:
            pass

    # The bdev name the data plane expects (e.g. LVS_7744/LVOL_12345)
    bdev_name = f"{lvol.lvs_name}/{lvol.lvol_bdev}"

    # Only include completed backups — incomplete ones have no metadata in S3
    # Data plane processes s3_ids in array order: the first entry's clusters
    # take priority (skip-if-populated).  Newest-first means the latest
    # incremental data wins, with older backups filling any remaining gaps.
    completed_chain = [b for b in reversed(chain)
                       if b.status == Backup.STATUS_COMPLETED]
    if not completed_chain:
        return None, "No completed backups in chain"

    result = tasks_controller.add_backup_restore_task(
        cluster_id, lvol.node_id, backup_id, bdev_name,
        [b.s3_id for b in completed_chain], lvol_id=lvol_id)

    if result:
        return lvol_id, None
    return None, "Failed to create restore task"


def delete_backups(lvol_id):
    """Delete all backups for a given lvol.
    Returns (success, error_message)."""
    backups = db_controller.get_backups_by_lvol_id(lvol_id)
    if not backups:
        return False, f"No backups found for lvol {lvol_id}"

    # Find node to run delete RPC on
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if not completed:
        # Just remove from DB
        for b in backups:
            b.remove(db_controller.kv_store)
        return True, None

    node_id = completed[0].node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        # Node gone, just clean up DB
        for b in backups:
            b.remove(db_controller.kv_store)
        return True, None

    # Call S3 delete RPC (dummy for now)
    if snode.status == StorageNode.STATUS_ONLINE:
        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password)
        s3_ids = [b.s3_id for b in completed]
        try:
            rpc_client.bdev_lvol_s3_delete(s3_ids)
        except Exception as e:
            logger.error(f"Error deleting S3 backups: {e}")

    cluster_id = completed[0].cluster_id
    for b in backups:
        backup_events.backup_deleted(cluster_id, node_id, b)
        b.remove(db_controller.kv_store)

    return True, None


def list_backups(cluster_id=None):
    """List all backups, optionally filtered by cluster."""
    backups = db_controller.get_backups(cluster_id)
    backups = sorted(backups, key=lambda b: (b.created_at, b.uuid), reverse=True)
    data = []
    for b in backups:
        source = b.source_cluster_id or b.cluster_id
        is_external = source != b.cluster_id
        entry = {
            "ID": b.uuid,
            "S3 ID": b.s3_id,
            "LVol": b.lvol_name,
            "Snapshot": b.snapshot_name,
            "Node": b.node_id[:8] if b.node_id else "",
            "Status": b.status,
            "Prev": b.prev_backup_id[:8] if b.prev_backup_id else "-",
            "Created": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(b.created_at)) if b.created_at else "",
            "Source": source[:8] if is_external else "local",
        }
        data.append(entry)
    return data


def export_backups(cluster_id=None, lvol_name=None):
    """Export completed backup metadata as a list of dicts suitable for import
    into another cluster via import_backups().

    Returns a list of metadata dicts including s3_id, chain links, and size.
    """
    backups = db_controller.get_backups(cluster_id)
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if lvol_name:
        completed = [b for b in completed if b.lvol_name == lvol_name]

    result = []
    for b in completed:
        result.append({
            "backup_id": b.uuid,
            "s3_id": b.s3_id,
            "cluster_id": b.cluster_id,
            "lvol_id": b.lvol_id,
            "lvol_name": b.lvol_name,
            "snapshot_id": b.snapshot_id,
            "snapshot_name": b.snapshot_name,
            "node_id": b.node_id,
            "prev_backup_id": b.prev_backup_id,
            "size": b.size,
            "allowed_hosts": b.allowed_hosts,
            "created_at": b.created_at,
        })
    return result


def import_backups(s3_metadata_list, cluster_id=None):
    """Import backup metadata from another cluster's S3 metadata.

    Backups are stored in the local cluster's DB namespace but keep their
    original s3_ids (scoped to source_cluster_id).  The source_cluster_id
    field tracks which cluster originally created the backup.

    Args:
        s3_metadata_list: list of dicts with backup metadata.
        cluster_id: Target cluster to import into.  Required for cross-cluster
            restore so the backups are visible in the local cluster's DB.
    """
    imported = 0
    for meta in s3_metadata_list:
        backup_id = meta.get("backup_id")
        if not backup_id:
            continue

        source_cluster = meta.get("cluster_id", "")
        target_cluster = cluster_id or source_cluster

        # Skip only if already registered for the target cluster
        try:
            existing = db_controller.get_backup_by_id(backup_id)
            if existing.cluster_id == target_cluster:
                continue  # already imported for this cluster
        except KeyError:
            pass

        backup = Backup()
        backup.uuid = backup_id
        backup.s3_id = meta.get("s3_id", 0)
        backup.cluster_id = target_cluster
        backup.source_cluster_id = source_cluster
        backup.lvol_id = meta.get("lvol_id", "")
        backup.lvol_name = meta.get("lvol_name", "")
        backup.snapshot_id = meta.get("snapshot_id", "")
        backup.snapshot_name = meta.get("snapshot_name", "")
        backup.node_id = meta.get("node_id", "")
        backup.prev_backup_id = meta.get("prev_backup_id", "")
        backup.size = meta.get("size", 0)
        backup.allowed_hosts = meta.get("allowed_hosts", [])
        backup.created_at = meta.get("created_at", 0)
        backup.status = Backup.STATUS_COMPLETED
        backup.s3_metadata = meta
        backup.write_to_db()
        imported += 1

    return imported


def get_backup_sources(cluster_id):
    """List all distinct backup sources (local + imported clusters).

    Returns a list of dicts with source_cluster_id, count, and whether
    it is the currently active source.
    """
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        return []

    backups = db_controller.get_backups(cluster_id)
    sources = {}
    for b in backups:
        src = b.source_cluster_id or cluster_id
        if src not in sources:
            sources[src] = {"source_cluster_id": src, "count": 0, "is_local": src == cluster_id}
        sources[src]["count"] += 1

    active_source = cluster.backup_source or cluster_id
    result = []
    for src_id, info in sources.items():
        info["active"] = (src_id == active_source)
        result.append(info)

    # Always include local even if no backups
    if cluster_id not in sources:
        result.append({
            "source_cluster_id": cluster_id,
            "count": 0,
            "is_local": True,
            "active": active_source == cluster_id,
        })

    return result


def switch_backup_source(cluster_id, source_cluster_id):
    """Switch the active backup source for all nodes in the cluster.

    Reconfigures the S3 bdev on every node to read from the bucket
    belonging to source_cluster_id.  While switched to an external
    source, new backups cannot be created.

    Args:
        cluster_id: The local cluster ID.
        source_cluster_id: The cluster ID whose S3 bucket to activate.
            Use the local cluster_id (or "local") to switch back.

    Returns (success, error_message).
    """
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        return False, f"Cluster {cluster_id} not found"

    if source_cluster_id == "local":
        source_cluster_id = cluster_id

    # Determine the bucket name for the source cluster
    backup_config = cluster.backup_config or {}
    if source_cluster_id == cluster_id:
        bucket_name = backup_config.get("bucket_name",
                                        f"simplyblock-backup-{cluster_id}")
    else:
        bucket_name = f"simplyblock-backup-{source_cluster_id}"

    # Verify the bucket exists
    try:
        s3_kwargs = {
            "aws_access_key_id": backup_config.get("access_key_id", ""),
            "aws_secret_access_key": backup_config.get("secret_access_key", ""),
        }
        endpoint_url = backup_config.get("local_endpoint", "")
        if endpoint_url:
            s3_kwargs["endpoint_url"] = endpoint_url
        s3_client = boto3.client("s3", **s3_kwargs)
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception as e:
        return False, f"S3 bucket {bucket_name} not accessible: {e}"

    # Reconfigure S3 bdev bucket on all online nodes
    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    errors = []
    for node in nodes:
        if node.status != StorageNode.STATUS_ONLINE or not node.lvstore:
            continue
        try:
            rpc_client = RPCClient(
                node.mgmt_ip, node.rpc_port,
                node.rpc_username, node.rpc_password)
            s3_bdev_name = f"s3_{node.lvstore}"
            ret = rpc_client.bdev_s3_add_bucket_name(s3_bdev_name, bucket_name)
            if not ret:
                errors.append(f"Node {node.get_id()}: failed to set bucket")
            else:
                logger.info(f"Switched S3 bucket to {bucket_name} on node {node.get_id()}")
        except Exception as e:
            errors.append(f"Node {node.get_id()}: {e}")

    if errors:
        return False, "; ".join(errors)

    # Persist the active source in the cluster record
    cluster.backup_source = source_cluster_id
    cluster.write_to_db()

    return True, None


def is_local_backup_source(cluster_id):
    """Check if the cluster is currently using its own local backup source."""
    try:
        cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        return True
    return not cluster.backup_source or cluster.backup_source == cluster_id


# ---- Backup Policy Management ----

def add_policy(cluster_id, name, max_versions=0, max_age="", schedule=""):
    """Create a new backup policy.
    Returns (policy_id, error_message)."""
    max_age_seconds = 0
    if max_age:
        try:
            max_age_seconds = _parse_age_string(max_age)
        except ValueError as e:
            return None, str(e)

    if schedule:
        try:
            _parse_schedule(schedule)
        except ValueError as e:
            return None, str(e)

    if max_versions <= 0 and max_age_seconds <= 0 and not schedule:
        return None, "At least one of --versions, --age, or --schedule must be specified"

    # Check name uniqueness
    for p in db_controller.get_backup_policies(cluster_id):
        if p.policy_name == name:
            return None, f"Policy name already exists: {name}"

    policy = BackupPolicy()
    policy.uuid = str(uuid.uuid4())
    policy.cluster_id = cluster_id
    policy.policy_name = name
    policy.max_versions = max_versions
    policy.max_age_seconds = max_age_seconds
    policy.max_age_display = max_age
    policy.backup_schedule = schedule
    policy.status = BackupPolicy.STATUS_ACTIVE
    policy.write_to_db()

    return policy.uuid, None


def remove_policy(policy_id):
    """Remove a backup policy and all its attachments.
    Returns (success, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return False, str(e)

    # Remove attachments
    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id:
            att.remove(db_controller.kv_store)

    policy.remove(db_controller.kv_store)
    return True, None


def attach_policy(policy_id, target_type, target_id):
    """Attach a backup policy to a pool or lvol.
    Returns (attachment_id, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return None, str(e)

    if target_type not in ("pool", "lvol"):
        return None, f"Invalid target_type: {target_type}. Use 'pool' or 'lvol'"

    # Validate target exists
    try:
        if target_type == "pool":
            db_controller.get_pool_by_id(target_id)
        else:
            db_controller.get_lvol_by_id(target_id)
    except KeyError as e:
        return None, str(e)

    # Check if already attached
    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id and att.target_type == target_type and att.target_id == target_id:
            return att.uuid, None  # already attached

    att = BackupPolicyAttachment()
    att.uuid = str(uuid.uuid4())
    att.cluster_id = policy.cluster_id
    att.policy_id = policy_id
    att.target_type = target_type
    att.target_id = target_id
    att.write_to_db()

    return att.uuid, None


def detach_policy(policy_id, target_type, target_id):
    """Detach a backup policy from a pool or lvol.
    Returns (success, error_message)."""
    try:
        policy = db_controller.get_backup_policy_by_id(policy_id)
    except KeyError as e:
        return False, str(e)

    for att in db_controller.get_backup_policy_attachments(policy.cluster_id):
        if att.policy_id == policy_id and att.target_type == target_type and att.target_id == target_id:
            att.remove(db_controller.kv_store)
            return True, None

    return False, "Attachment not found"


def list_policies(cluster_id=None):
    """List all backup policies."""
    policies = db_controller.get_backup_policies(cluster_id)
    data = []
    for p in policies:
        data.append({
            "ID": p.uuid,
            "Name": p.policy_name,
            "Versions": p.max_versions if p.max_versions > 0 else "-",
            "Max Age": p.max_age_display if p.max_age_display else "-",
            "Schedule": p.backup_schedule if p.backup_schedule else "-",
            "Status": p.status,
        })
    return data


def evaluate_policy(lvol):
    """Evaluate backup policy for an lvol and trigger merges if needed.
    Called by the backup merge service."""
    policy = db_controller.get_policy_for_lvol(lvol)
    if not policy:
        return

    backups = db_controller.get_backups_by_lvol_id(lvol.get_id())
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if len(completed) < 2:
        return

    completed.sort(key=lambda b: b.created_at)
    now = int(time.time())

    versions_exceeded = policy.max_versions > 0 and len(completed) > policy.max_versions
    age_exceeded = False
    if policy.max_age_seconds > 0 and completed:
        oldest_age = now - completed[0].created_at
        age_exceeded = oldest_age > policy.max_age_seconds

    # Either condition triggers a merge
    if versions_exceeded or age_exceeded:
        oldest = completed[0]
        second = completed[1]
        _trigger_merge(second, oldest)


def evaluate_schedule(lvol):
    """Evaluate the backup schedule for an lvol and trigger auto-backups + tiered merges.
    Called by the backup merge service."""
    policy = db_controller.get_policy_for_lvol(lvol)
    if not policy or not policy.backup_schedule:
        return

    try:
        tiers = _parse_schedule(policy.backup_schedule)
    except ValueError:
        return

    if not tiers:
        return

    now = int(time.time())

    # Check if we need to create a new auto-backup based on the smallest tier interval
    smallest_interval = tiers[0][0]
    backups = db_controller.get_backups_by_lvol_id(lvol.get_id())
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    pending_or_running = [b for b in backups if b.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS)]

    # Don't create a new backup if one is already in progress
    if not pending_or_running:
        needs_backup = True
        if completed:
            completed.sort(key=lambda b: b.created_at, reverse=True)
            latest = completed[0]
            elapsed = now - latest.created_at
            if elapsed < smallest_interval:
                needs_backup = False

        if needs_backup:
            _auto_backup_lvol(lvol)
            return  # Skip merge evaluation this cycle — let the backup complete first

    # Tiered merge: enforce keep_count per tier.
    # Each tier covers an age range.  Backups age from tier 0 (newest)
    # into higher tiers.  When a tier exceeds its keep_count, the oldest
    # backup in that tier is merged into its successor.
    # All tiers are evaluated each cycle so limits are maintained in parallel.
    if len(completed) < 2:
        return

    completed.sort(key=lambda b: b.created_at)

    # Don't merge while another merge is already in progress
    merging = [b for b in backups if b.status == Backup.STATUS_MERGING]
    if merging:
        return

    for tier_idx, (interval, keep_count) in enumerate(tiers):
        # Age boundaries for this tier
        if tier_idx == 0:
            lower_age = 0
        else:
            lower_age = tiers[tier_idx - 1][0]

        if tier_idx + 1 < len(tiers):
            upper_age = tiers[tier_idx + 1][0]
        else:
            upper_age = float('inf')

        tier_backups = [b for b in completed
                        if lower_age <= (now - b.created_at) < upper_age]

        if len(tier_backups) > keep_count:
            tier_backups.sort(key=lambda b: b.created_at)
            oldest = tier_backups[0]
            second = tier_backups[1]
            _trigger_merge(second, oldest)
            return  # One merge per cycle to avoid conflicts


def _auto_backup_lvol(lvol):
    """Create an automatic snapshot + backup for scheduled backups.

    Unlike manual backup_snapshot() which walks the full ancestor chain,
    auto-backups create a single snapshot and a single backup for it.
    The prev_backup_id is set to the latest existing backup so the
    incremental chain is maintained without re-backing all ancestors.
    """
    from simplyblock_core.controllers import snapshot_controller
    snap_name = f"auto_{lvol.lvol_name}_{int(time.time())}"
    snap_id, error = snapshot_controller.add(lvol.get_id(), snap_name)
    if error:
        logger.warning(f"Auto-backup snapshot failed for lvol {lvol.get_id()}: {error}")
        return

    try:
        snapshot = db_controller.get_snapshot_by_id(snap_id)
    except KeyError:
        logger.warning(f"Auto-backup: snapshot {snap_id} not found after creation")
        return

    node_id = lvol.node_id
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.warning(f"Auto-backup: node {node_id} not found")
        return

    cluster_id = snode.cluster_id
    prev_backup = _get_latest_backup_for_lvol(lvol.get_id())
    _create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup)


def _trigger_merge(keep_backup, old_backup):
    """Trigger a merge of old_backup into keep_backup."""
    if old_backup.status != Backup.STATUS_COMPLETED:
        return
    if keep_backup.status != Backup.STATUS_COMPLETED:
        return

    old_backup.status = Backup.STATUS_MERGING
    old_backup.write_to_db()

    tasks_controller.add_backup_merge_task(
        keep_backup.cluster_id,
        keep_backup.node_id,
        keep_backup.uuid,
        old_backup.uuid)
