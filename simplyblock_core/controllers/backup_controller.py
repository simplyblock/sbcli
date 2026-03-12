# coding=utf-8
import json
import logging
import re
import time
import uuid

from simplyblock_core import constants, utils
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
    """Get the most recent completed backup for a given lvol."""
    backups = db_controller.get_backups_by_lvol_id(lvol_id)
    completed = [b for b in backups if b.status == Backup.STATUS_COMPLETED]
    if not completed:
        return None
    completed.sort(key=lambda b: b.created_at, reverse=True)
    return completed[0]


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
        )
        if not ret:
            logger.warning(f"Failed to create S3 bdev on node {node.get_id()}")
            return False
    except Exception as e:
        logger.error(f"Error creating S3 bdev on node {node.get_id()}: {e}")
        return False

    # Step 2: Attach the S3 bdev to the lvstore
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
    """Walk snap_ref_id to build the full ancestor chain, oldest first."""
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

    # Walk the snapshot chain and back up all unbacked ancestors first
    snap_chain = _get_snapshot_chain(snapshot)
    prev_backup = _get_latest_backup_for_lvol(lvol.get_id())
    final_backup_id = None

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

    if not final_backup_id:
        # The target snapshot was already backed up
        return None, f"Snapshot {snapshot_id} already has a backup"

    return final_backup_id, None


def restore_backup(backup_id, node_id, lvol_name, cluster_id=None):
    """Restore a backup chain into a new lvol.
    Returns (task_id_or_status, error_message)."""
    try:
        backup = db_controller.get_backup_by_id(backup_id)
    except KeyError as e:
        return None, str(e)

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError as e:
        return None, str(e)

    if snode.status != StorageNode.STATUS_ONLINE:
        return None, f"Node {node_id} is not online"

    if not cluster_id:
        cluster_id = snode.cluster_id

    # Build the backup chain
    chain = db_controller.get_backup_chain(backup_id)
    if not chain:
        return None, f"Could not build backup chain for {backup_id}"

    # Create the restore task — pass integer s3_ids for data-plane RPCs
    result = tasks_controller.add_backup_restore_task(
        cluster_id, node_id, backup_id, lvol_name,
        [b.s3_id for b in chain])

    if result:
        return backup_id, None
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
    data = []
    for b in backups:
        data.append({
            "ID": b.uuid,
            "S3 ID": b.s3_id,
            "LVol": b.lvol_name,
            "Snapshot": b.snapshot_name,
            "Node": b.node_id[:8] if b.node_id else "",
            "Status": b.status,
            "Prev": b.prev_backup_id[:8] if b.prev_backup_id else "-",
            "Created": time.strftime("%Y-%m-%d %H:%M", time.localtime(b.created_at)) if b.created_at else "",
        })
    return data


def import_backups(s3_metadata_list):
    """Import backup metadata from another cluster's S3 metadata.
    s3_metadata_list is a list of dicts with backup metadata."""
    imported = 0
    for meta in s3_metadata_list:
        backup_id = meta.get("backup_id")
        if not backup_id:
            continue

        # Check if already exists
        try:
            db_controller.get_backup_by_id(backup_id)
            continue  # already imported
        except KeyError:
            pass

        backup = Backup()
        backup.uuid = backup_id
        backup.s3_id = meta.get("s3_id", _next_s3_id(meta.get("cluster_id", "")))
        backup.cluster_id = meta.get("cluster_id", "")
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

    # Both conditions must be met before merging
    if versions_exceeded and age_exceeded:
        # Merge oldest into second oldest
        oldest = completed[0]
        second = completed[1]
        _trigger_merge(second, oldest)
    elif policy.max_versions > 0 and not policy.max_age_seconds:
        # Only version limit, no age limit
        if versions_exceeded:
            oldest = completed[0]
            second = completed[1]
            _trigger_merge(second, oldest)
    elif policy.max_age_seconds > 0 and not policy.max_versions:
        # Only age limit, no version limit
        if age_exceeded:
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

    # Tiered merge: enforce keep_count per tier
    if len(completed) < 2:
        return

    completed.sort(key=lambda b: b.created_at)

    # Assign backups to tiers (newest first matching).
    # Tier boundaries: tier[i] covers backups with age < tier[i+1].interval (or unlimited for last tier).
    # Within each tier, if count > keep_count, merge the oldest two.
    for tier_idx, (interval, keep_count) in enumerate(tiers):
        # Upper age boundary for this tier
        if tier_idx + 1 < len(tiers):
            upper_age = tiers[tier_idx + 1][0]
        else:
            upper_age = float('inf')

        tier_backups = [b for b in completed if interval <= (now - b.created_at) < upper_age]
        # For the first (smallest) tier, also include backups younger than the first interval
        if tier_idx == 0:
            tier_backups = [b for b in completed if (now - b.created_at) < upper_age]

        if len(tier_backups) > keep_count:
            # Merge oldest two in this tier
            tier_backups.sort(key=lambda b: b.created_at)
            oldest = tier_backups[0]
            second = tier_backups[1]
            _trigger_merge(second, oldest)
            return  # One merge at a time


def _auto_backup_lvol(lvol):
    """Create an automatic snapshot + backup for scheduled backups."""
    from simplyblock_core.controllers import snapshot_controller
    snap_name = f"auto_{lvol.lvol_name}_{int(time.time())}"
    snap_id, error = snapshot_controller.add(lvol.get_id(), snap_name, backup=True)
    if error:
        logger.warning(f"Auto-backup failed for lvol {lvol.get_id()}: {error}")


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
