"""Consistency-group backup: back up the member snapshots of one group
generation to S3 as one all-or-nothing unit (design-consistency-group-backup.md).

This module holds the group-level decisions that sit on top of the per-member
single-volume backup path (``controllers/backup/controller.py``): the
homogeneity gate that the first version requires (§3.4) and the all-or-nothing
commit decision that makes a group backup visible only when every member
completed (§3.3). The orchestration around them -- :func:`create_group_backup`
resolving or taking a generation, creating the ``GroupBackup`` record, and
fanning out one single-volume backup per member, and
:func:`try_commit_group_backup` publishing the group manifest last once every
member finished -- lives here too and calls into the single-volume create flow
rather than reimplementing it.
"""
import time
from uuid import UUID, uuid4

from simplyblock_core import utils
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup

logger = utils.get_logger(__name__)

#: The commit decision for a group backup, from its members' statuses.
COMMIT_PENDING = "pending"   # at least one member is not yet terminal
COMMIT_READY = "ready"       # every member completed; publish the group manifest
COMMIT_FAILED = "failed"     # a member failed terminally; fail the group as a unit


def check_members_homogeneous(members: list) -> list:
    """The members that break the first version's homogeneity requirement (§3.4).

    ``members`` is a list of ``{"lvol_id", "location", "encrypted"}``. Every
    member must share one S3 location and one encryption disposition, which is
    the existing per-chain coherence gate (a chain cannot span buckets or
    encodings) applied across the group. Returns ``[(lvol_id, reason), ...]`` for
    the members that do not match the first, named so a rejection points at the
    offenders rather than failing anonymously. Empty means homogeneous.
    """
    if not members:
        return []

    reference = members[0]
    problems = []
    for member in members[1:]:
        if member["location"] != reference["location"]:
            problems.append((member["lvol_id"], "different backup location"))
        elif member["encrypted"] != reference["encrypted"]:
            problems.append((member["lvol_id"], "different encryption disposition"))
    return problems


def group_commit_state(member_statuses: list) -> str:
    """The commit decision for a group backup, from its members' backup statuses.

    All-or-nothing (§3.3): a group backup commits (its group manifest is
    published) only when every member backup has completed, and one terminal
    member failure fails the whole group even while others are still uploading.

    * ``COMMIT_FAILED`` -- any member is terminally failed.
    * ``COMMIT_READY``  -- there is at least one member and every member completed.
    * ``COMMIT_PENDING`` -- otherwise (a member is still pending or in progress,
      or there are no members yet).
    """
    if any(status == Backup.STATUS_FAILED for status in member_statuses):
        return COMMIT_FAILED
    if member_statuses and all(
            status == Backup.STATUS_COMPLETED for status in member_statuses):
        return COMMIT_READY
    return COMMIT_PENDING


def build_group_manifest(group_backup, member_backups: dict):
    """Assemble the group manifest from the ``GroupBackup`` record and its member
    ``Backup`` records (design §4.2).

    Pure: the caller supplies the records. ``member_backups`` maps
    ``Backup.get_id()`` to the member ``Backup`` for every member in
    ``group_backup.members``. Called at the commit barrier, once every member is
    completed, so a missing member is a programming error, not a partial group.

    The group manifest is written LAST, after every member manifest exists, so
    its presence is the all-or-nothing commit barrier (§3.3). It names the member
    backups by id; a reader then reads each ``manifests/<backup_id>.json`` to
    assemble that member's chain. The encryption keys are per-member and live in
    those member manifests; the group manifest records only the shared
    ``encrypted`` disposition (§3.4).

    Raises:
        ValueError: a member backup is missing, or an id is not a UUID -- the
            manifest types ids as UUID, and this is the seam where a malformed
            one is caught while committing rather than during a recovery.
    """
    members = []
    total_size = 0
    encrypted = False
    for entry in group_backup.members:
        backup = member_backups.get(entry["backup_id"])
        if backup is None:
            raise ValueError(
                f"member backup {entry['backup_id']} missing while committing "
                f"group backup {group_backup.uuid}")
        members.append(backup_manifest.GroupManifestMember(
            lvol_id=UUID(backup.lvol_id),
            lvol_name=backup.lvol_name,
            backup_id=UUID(backup.uuid),
            s3_id=backup.s3_id,
        ))
        total_size += backup.size
        encrypted = encrypted or backup.encrypted

    return backup_manifest.GroupBackupManifest(
        group_backup_id=UUID(group_backup.uuid),
        created_at=group_backup.created_at,
        completed_at=group_backup.completed_at,
        size=total_size,
        source=backup_manifest.GroupManifestSource(
            cluster_id=UUID(group_backup.cluster_id),
            consistency_group_id=group_backup.consistency_group_id,
            consistency_group_name=group_backup.consistency_group_name,
            generation=group_backup.group_seq,
        ),
        members=members,
        membership=backup_manifest.GroupManifestMembership(
            expected_lvol_ids=[UUID(entry["lvol_id"]) for entry in group_backup.members],
        ),
        encrypted=encrypted,
    )


def _config_for_group(group_backup):
    """Credentials for a group backup's bucket (design §3.4).

    Like the single-volume ``_config_for``: the location comes from the group
    backup record, and only the credentials come from the cluster, because a
    manifest must never carry them.

    Raises:
        PreconditionError: the cluster's configured bucket is not the one this
            group backup lives in, so its credentials cannot be assumed to reach
            it -- the cluster's backup configuration was repointed while the
            group backup was in flight.
    """
    from simplyblock_core import db_controller

    db = db_controller.DBController()
    config = db.get_cluster_by_id(group_backup.cluster_id).get_backup_config()
    location = group_backup.get_location()
    if config.location() != location:
        raise PreconditionError(
            f"Group backup {group_backup.uuid} lives in bucket "
            f"{location.bucket_name}, but cluster {group_backup.cluster_id} is "
            f"configured for {config.bucket_name}; supply credentials for the "
            f"group backup's bucket")
    return config


def create_group_backup(consistency_group_id, group_seq=None):
    """Back up one generation of a consistency group as an all-or-nothing unit.

    Resolves the generation -- taking a fresh crash-consistent group snapshot
    when ``group_seq`` is None -- gates its members on homogeneity (§3.4),
    records a :class:`GroupBackup`, and fans out one single-volume backup per
    member stamped with the group backup id and generation. The group manifest
    that makes the backup visible is published later, by the commit task this
    enqueues, once every member backup has completed (§3.3). Each member backup
    is an ordinary single-volume backup, so the existing backup runner drives it
    to a terminal state and the commit task only watches.

    Returns ``(group_backup_id, None)`` or ``(None, error_message)``.
    """
    from simplyblock_core import db_controller
    from simplyblock_core.controllers import tasks_controller
    from simplyblock_core.controllers.backup import controller as backup_controller
    from simplyblock_core.controllers.consistency_group_controller import (
        create_group_snapshot_for_group,
    )

    db = db_controller.DBController()
    try:
        group = db.get_consistency_group_by_id(consistency_group_id)
    except KeyError as e:
        return None, str(e)

    cluster_id = group.cluster_id
    if group_seq is None:
        snapshot_ids, err = create_group_snapshot_for_group(group)
        if err is not None:
            return None, err
        try:
            group_seq = db.get_snapshot_by_id(snapshot_ids[0]).group_seq
        except (KeyError, IndexError):
            return None, "group snapshot was taken but its generation is unreadable"

    member_snaps = [
        s for s in db.get_snapshots(cluster_id)
        if s.group_id == group.get_id() and s.group_seq == group_seq
    ]
    if not member_snaps:
        return None, (f"consistency group {consistency_group_id} has no member "
                      f"snapshots at generation {group_seq}")

    try:
        location = db.get_cluster_by_id(cluster_id).get_backup_config().location()
    except (KeyError, ValueError) as e:
        return None, str(e)

    # Homogeneity gate (§3.4): every member must share one location and one
    # encryption disposition, the per-chain coherence rule applied across the
    # group. The location is the cluster's own and shared by construction; the
    # encryption disposition is per volume, so this is where a group mixing an
    # encrypted and a plaintext member is refused before anything is written.
    location_dict = location.model_dump(mode="json")
    member_facts = [
        {"lvol_id": s.lvol.get_id(), "location": location_dict,
         "encrypted": bool(s.lvol.crypto_bdev)}
        for s in member_snaps
    ]
    problems = check_members_homogeneous(member_facts)
    if problems:
        detail = ", ".join(f"{lvol_id} ({reason})" for lvol_id, reason in problems)
        return None, (f"consistency group {consistency_group_id} is not "
                      f"homogeneous and cannot be backed up as one unit: {detail}")

    group_backup = GroupBackup()
    group_backup.uuid = str(uuid4())
    group_backup.cluster_id = cluster_id
    group_backup.consistency_group_id = group.get_id()
    # group_name, not name: BaseModel.name is the class name (replication.py).
    group_backup.consistency_group_name = group.group_name
    group_backup.group_seq = group_seq
    group_backup.location = location_dict
    group_backup.encrypted = any(f["encrypted"] for f in member_facts)
    group_backup.created_at = int(time.time())
    group_backup.status = GroupBackup.STATUS_IN_PROGRESS
    group_backup.write_to_db()

    members = []
    total_size = 0
    for snap in member_snaps:
        backup_id, err = backup_controller.backup_snapshot(snap.get_id(), cluster_id)
        if err is not None:
            group_backup.status = GroupBackup.STATUS_FAILED
            group_backup.error_message = (
                f"member {snap.lvol.get_id()} could not be backed up: {err}")
            group_backup.write_to_db()
            return None, group_backup.error_message

        backup = db.get_backup_by_id(backup_id)
        # Store the group backup's get_id() ("<cluster>/<uuid>"), like the
        # sibling lvol_id/snapshot_id fields, so get_backups_by_group_backup_id
        # resolves the members of a group backup from its id.
        backup.group_backup_id = group_backup.get_id()
        backup.group_seq = group_seq
        backup.write_to_db()
        members.append({"lvol_id": snap.lvol.get_id(), "backup_id": backup.get_id()})
        total_size += backup.size

    group_backup.members = members
    group_backup.size = total_size
    group_backup.write_to_db()

    tasks_controller.add_group_backup_commit_task(group_backup)
    return group_backup.get_id(), None


def try_commit_group_backup(group_backup_id):
    """The all-or-nothing commit barrier for a group backup (design §3.3).

    Reads the member backups, decides with :func:`group_commit_state`, and on
    the first version's three outcomes:

    * ``COMMIT_PENDING`` -- a member is still uploading; the caller defers.
    * ``COMMIT_FAILED`` -- a member failed terminally (or its record is gone);
      the group is marked failed as a unit.
    * ``COMMIT_READY`` -- every member completed, so the group manifest is
      published and only then is the record marked completed, which keeps the
      manifest-before-``COMPLETED`` invariant the single-volume path keeps: a
      ``COMPLETED`` group backup is always identifiable from the bucket alone.

    Idempotent: a group backup already in a terminal state returns that outcome
    without touching S3, so the commit task is safe to re-run. Returns one of the
    ``COMMIT_*`` constants.

    Raises:
        ManifestError, PreconditionError: the group manifest could not be
            published; the record is left in progress so the caller can retry.
    """
    from simplyblock_core import db_controller

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError:
        return COMMIT_FAILED

    if group_backup.status == GroupBackup.STATUS_COMPLETED:
        return COMMIT_READY
    if group_backup.status == GroupBackup.STATUS_FAILED:
        return COMMIT_FAILED

    member_backups = {}
    for entry in group_backup.members:
        try:
            backup = db.get_backup_by_id(entry["backup_id"])
        except KeyError:
            continue
        member_backups[backup.get_id()] = backup

    if len(member_backups) != len(group_backup.members):
        return _fail_group(group_backup, "a member backup record is missing")

    state = group_commit_state([b.status for b in member_backups.values()])
    if state == COMMIT_PENDING:
        return COMMIT_PENDING
    if state == COMMIT_FAILED:
        return _fail_group(group_backup, "a member backup failed")

    # COMMIT_READY: publish the group manifest LAST, before marking completed,
    # so the record is never COMPLETED without its manifest in the bucket.
    group_backup.completed_at = int(time.time())
    group_backup.size = sum(b.size for b in member_backups.values())
    manifest = build_group_manifest(group_backup, member_backups)
    backup_manifest.write_group(_config_for_group(group_backup), manifest)

    group_backup.status = GroupBackup.STATUS_COMPLETED
    group_backup.write_to_db()
    return COMMIT_READY


def _fail_group(group_backup, reason: str) -> str:
    group_backup.status = GroupBackup.STATUS_FAILED
    group_backup.error_message = reason
    group_backup.write_to_db()
    return COMMIT_FAILED


def _unwind_restored(lvol_ids: list) -> None:
    """Tear down the volumes a failed group restore already created (design §6.5).

    Best-effort: every volume is attempted even if one teardown fails, so a
    failed group restore leaves no half-group behind. The volumes are still
    STATUS_RESTORING, so the teardown is forced.
    """
    from simplyblock_core import db_controller
    from simplyblock_core.controllers import lvol_controller

    db = db_controller.DBController()
    for lvol_id in lvol_ids:
        try:
            lvol_controller.delete_lvol(db.get_lvol_by_id(lvol_id), force_delete=True)
        except (KeyError, ValueError, PreconditionError) as e:
            logger.warning("Group restore unwind: could not delete volume %s: %s",
                           lvol_id, e)


def restore_group_backup(group_backup_id, into_group_name, pool_id_or_name,
                         target_node_id=None, s3_credentials=None):
    """Restore a group backup into a NEW consistency group (design §6).

    Recreates the N member volumes of one group backup from S3 and re-forms them
    into a new standalone consistency group named ``into_group_name``, the
    ``clone_generation(into_name=...)`` shape read from a backup rather than live
    snapshots. Each member is an ordinary single-volume restore: it comes up
    STATUS_RESTORING and its data recovery runs asynchronously, exactly as a
    standalone restore does, so "all members online" is each member's restore
    task reaching ONLINE, watched by the caller.

    All-or-nothing (design §6.5): the member chains are all validated restorable
    before any volume is created, every restored volume is pinned to one store so
    the group can form (§3.5), and if any member fails to restore or join, the
    volumes already created are torn down so a failed group restore leaves no
    half-group.

    Returns ``(new_group_id, lvol_ids, None)`` or ``(None, None, error_message)``.
    """
    from simplyblock_core import db_controller
    from simplyblock_core.controllers import consistency_group_controller as cg
    from simplyblock_core.controllers.backup import controller as backup_controller
    from simplyblock_core.controllers.backup.chain import BackupChain

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError as e:
        return None, None, str(e)

    # Only a committed group backup is restorable: an in-progress or failed one
    # is not a crash-consistent generation (design §3.3).
    if group_backup.status != GroupBackup.STATUS_COMPLETED:
        return None, None, (
            f"group backup {group_backup_id} is {group_backup.status}, not "
            f"completed; only a committed group backup can be restored")

    if not into_group_name:
        return None, None, "a restored consistency group needs a name"

    cluster_id = group_backup.cluster_id
    if db.get_consistency_group_by_name(cluster_id, into_group_name) is not None:
        return None, None, (
            f"consistency group {into_group_name} already exists in cluster "
            f"{cluster_id}; a group restore always produces a new group")

    # Resolve the member backups and validate every chain BEFORE creating any
    # volume, so a doomed restore leaves nothing behind (design §6.2). The
    # membership is complete by construction -- a group backup is COMPLETED only
    # when every member committed -- but a member whose Backup record was since
    # deleted is caught here.
    try:
        member_backups = [db.get_backup_by_id(entry["backup_id"])
                          for entry in group_backup.members]
    except KeyError as e:
        return None, None, f"a member backup of the group is missing: {e}"

    all_backups = db.get_backups()
    for backup in member_backups:
        try:
            chain = BackupChain.of_backups(UUID(backup.uuid), all_backups)
            chain.require_restorable(
                completed=True,
                what=f"member {backup.lvol_name} of group backup {group_backup.uuid}")
        except (PreconditionError, ValueError) as e:
            return None, None, str(e)

    # Create the member volumes, pinned to one store so they can form a group.
    # The first restore may auto-select a node; the rest are pinned to it, which
    # add_member_to_group then enforces (design §3.5).
    pin_node = target_node_id
    created: list = []
    for backup in member_backups:
        lvol_name = f"{into_group_name}_{backup.lvol_name}"
        try:
            lvol_id = backup_controller.restore_backup(
                backup.uuid, lvol_name, pool_id_or_name,
                target_node_id=pin_node, s3_credentials=s3_credentials)
        except (PreconditionError, RuntimeError, ValueError) as e:
            _unwind_restored(created)
            return None, None, (
                f"member {backup.lvol_name} could not be restored: {e}")
        created.append(lvol_id)
        if pin_node is None:
            pin_node = db.get_lvol_by_id(lvol_id).node_id

    # Re-form the consistency group from the restored volumes. add_member_to_group
    # enforces single-store placement and fails loudly on an off-store member,
    # which the unwind then cleans up (design §6.4). The group we just created is
    # removed on failure too, so the half-formed name does not block a retry.
    group = cg.ensure_group(cluster_id, into_group_name)
    for lvol_id in created:
        try:
            group = cg.add_member_to_group(group, db.get_lvol_by_id(lvol_id))
        except (cg.ConsistencyGroupError, KeyError) as e:
            _unwind_restored(created)
            db.get_consistency_group_by_id(group.get_id()).remove(db.kv_store)
            return None, None, (
                f"restored volumes could not form group {into_group_name}: {e}")

    logger.info("Restored group backup %s into consistency group %s (%d members)",
                group_backup.uuid, into_group_name, len(created))
    return group.get_id(), created, None


def delete_group_backup(group_backup_id):
    """Delete a group backup: its group manifest and its record (design §9).

    The member backups are left to per-volume retention, as the design says: a
    group backup is the unit of the group manifest, not of the member objects,
    which a later generation may still chain against. Best-effort on the manifest
    -- a bucket that cannot be reached does not block removing the record, which
    would otherwise strand the group backup as permanently un-deletable.

    Returns ``(True, None)`` or ``(False, error_message)``.
    """
    from simplyblock_core import db_controller

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError as e:
        return False, str(e)

    group_backup.status = GroupBackup.STATUS_DELETING
    group_backup.write_to_db()
    try:
        backup_manifest.delete_group(
            _config_for_group(group_backup), UUID(group_backup.uuid))
    except (backup_manifest.ManifestError, PreconditionError) as e:
        logger.warning("Group backup %s: could not delete group manifest: %s",
                       group_backup.uuid, e)
    group_backup.remove(db.kv_store)
    return True, None
