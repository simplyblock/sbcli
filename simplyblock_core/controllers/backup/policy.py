# coding=utf-8
"""Backup policies: retention limits, tiered schedules, and the merges they cause.

A policy decides *when* a backup is taken and when two are folded together. What
a backup is and how it is written is :mod:`controller`'s business, which this
module calls into rather than reimplements.
"""
import logging
import re
import time
import uuid

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.controllers.backup.chain import BackupChain
from simplyblock_core.controllers.backup.controller import (
    create_single_backup, get_latest_backup_for_lvol)
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment

logger = logging.getLogger()

db_controller = DBController()


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

    # Resolve everything the backup needs BEFORE taking the snapshot. This used
    # to create the snapshot first and discover afterwards that the node or
    # cluster was unusable, leaving an orphaned auto_* snapshot behind on every
    # scheduler tick.
    node_id = lvol.node_id
    prev_backup = get_latest_backup_for_lvol(lvol.get_id())
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
        cluster_id = snode.cluster_id
        location = db_controller.get_cluster_by_id(cluster_id).get_backup_config().location()

        # The chain this backup would join has to stay restorable, exactly as it
        # does for a backup taken by hand. A schedule is the one thing that adds
        # to a chain indefinitely, so it is where a chain would otherwise grow
        # past what the data plane accepts, or quietly split across two buckets
        # after the cluster's backup configuration was repointed.
        #
        # The location and encryption declared here are the ones this backup
        # WOULD be written with; holding the existing chain to them is what
        # catches the disagreement, rather than inheriting whatever the chain
        # already said.
        existing = (
            BackupChain.of_backups(
                uuid.UUID(prev_backup.uuid), db_controller.get_backups(cluster_id)).records()
            if prev_backup is not None else [])
        BackupChain.assemble(
            location, bool(lvol.crypto_bdev), existing,
        ).require_restorable(
            length=len(existing) + 1,
            what=f"The backup chain of volume {lvol.lvol_name}")
    except (KeyError, ValueError, PreconditionError) as e:
        logger.warning(f"Auto-backup skipped for lvol {lvol.get_id()}: {e}")
        return

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

    create_single_backup(snapshot, lvol, node_id, cluster_id, prev_backup, location)


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
