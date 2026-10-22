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
from datetime import UTC, datetime
from enum import Enum
from uuid import UUID, uuid4

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import consistency_group_controller as cg
from simplyblock_core.controllers import lvol_controller, tasks_controller
from simplyblock_core.controllers.backup import chain as backup_chain
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, GroupBackup

logger = utils.get_logger(__name__)

class CommitState(Enum):
    """The commit decision for a group backup, from its members' statuses."""

    #: At least one member is not yet terminal.
    PENDING = "pending"
    #: Every member completed: publish the group manifest.
    READY = "ready"
    #: A member failed terminally: fail the group as a unit.
    FAILED = "failed"


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


def group_commit_state(member_statuses: list) -> CommitState:
    """The commit decision for a group backup, from its members' backup statuses.

    All-or-nothing (§3.3): a group backup commits (its group manifest is
    published) only when every member backup has completed, and one terminal
    member failure fails the whole group even while others are still uploading.

    * ``CommitState.FAILED`` -- any member is terminally failed.
    * ``CommitState.READY``  -- there is at least one member and every member completed.
    * ``CommitState.PENDING`` -- otherwise (a member is still pending or in progress,
      or there are no members yet).
    """
    if any(status == Backup.STATUS_FAILED for status in member_statuses):
        return CommitState.FAILED
    if member_statuses and all(
            status == Backup.STATUS_COMPLETED for status in member_statuses):
        return CommitState.READY
    return CommitState.PENDING


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
    member_backup_ids = []
    total_size = 0
    encrypted = False
    for entry in group_backup.members:
        backup = member_backups.get(entry["backup_id"])
        if backup is None:
            raise ValueError(
                f"member backup {entry['backup_id']} missing while committing "
                f"group backup {group_backup.uuid}")
        member_backup_ids.append(UUID(backup.uuid))
        total_size += backup.size
        encrypted = encrypted or backup.encrypted

    return backup_manifest.GroupBackupManifest(
        group_backup_id=UUID(group_backup.uuid),
        created_at=datetime.fromtimestamp(group_backup.created_at, tz=UTC),
        completed_at=datetime.fromtimestamp(group_backup.completed_at, tz=UTC),
        size=total_size,
        source=backup_manifest.GroupManifestSource(
            cluster_id=UUID(group_backup.cluster_id),
            consistency_group_id=group_backup.consistency_group_id,
            consistency_group_name=group_backup.consistency_group_name,
            generation=group_backup.group_seq,
        ),
        member_backup_ids=member_backup_ids,
        encrypted=encrypted,
    )


def _config_for_group(group_backup):
    """Credentials for the bucket a group backup's members live in (design §3.4).

    The group record keeps no location of its own: each member backup records
    one, and they share it, so the group manifest's bucket is theirs. Each member
    goes through the single-volume ``_config_for``, so one outside the cluster's
    configured bucket is refused exactly as it would be on its own.

    Raises:
        PreconditionError: no member backup record is left to locate the group
            backup by, or a member lives in a bucket other than the cluster's
            configured one -- the cluster's backup configuration was repointed
            while the group backup was in flight.
    """

    db = db_controller.DBController()
    config = None
    for backup_id in group_backup.member_backup_ids():
        try:
            member = db.get_backup_by_id(backup_id)
        except KeyError:
            continue
        config = backup_controller._config_for(member)
    if config is None:
        raise PreconditionError(
            f"Group backup {group_backup.uuid} has no member backup left to "
            f"locate its bucket by")
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

    db = db_controller.DBController()
    try:
        group = db.get_consistency_group_by_id(consistency_group_id)
    except KeyError as e:
        return None, str(e)

    cluster_id = group.cluster_id
    if group_seq is None:
        group_seq, err = cg.create_group_snapshot_for_group(group)
        if err is not None:
            return None, err

    # A torn generation is refused rather than backed up as a group that
    # silently lacks a volume (§5).
    try:
        member_snaps = cg.resolve_complete_generation(group, group_seq)
    except cg.ConsistencyGroupError as e:
        return None, str(e)

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


def try_commit_group_backup(group_backup_id) -> CommitState:
    """The all-or-nothing commit barrier for a group backup (design §3.3).

    Reads the member backups, decides with :func:`group_commit_state`, and on
    the first version's three outcomes:

    * ``CommitState.PENDING`` -- a member is still uploading; the caller defers.
    * ``CommitState.FAILED`` -- a member failed terminally (or its record is gone);
      the group is marked failed as a unit.
    * ``CommitState.READY`` -- every member completed, so the group manifest is
      published and only then is the record marked completed, which keeps the
      manifest-before-``COMPLETED`` invariant the single-volume path keeps: a
      ``COMPLETED`` group backup is always identifiable from the bucket alone.

    Idempotent: a group backup already in a terminal state returns that outcome
    without touching S3, so the commit task is safe to re-run. Returns one of the
    ``CommitState`` values.

    Raises:
        ManifestError, PreconditionError: the group manifest could not be
            published; the record is left in progress so the caller can retry.
    """

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError:
        return CommitState.FAILED

    if group_backup.status == GroupBackup.STATUS_COMPLETED:
        return CommitState.READY
    if group_backup.status == GroupBackup.STATUS_FAILED:
        return CommitState.FAILED

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
    if state == CommitState.PENDING:
        return CommitState.PENDING
    if state == CommitState.FAILED:
        return _fail_group(group_backup, "a member backup failed")

    # CommitState.READY: publish the group manifest LAST, before marking completed,
    # so the record is never COMPLETED without its manifest in the bucket.
    group_backup.completed_at = int(time.time())
    group_backup.size = sum(b.size for b in member_backups.values())
    manifest = build_group_manifest(group_backup, member_backups)
    backup_manifest.write_group(_config_for_group(group_backup), manifest)

    group_backup.status = GroupBackup.STATUS_COMPLETED
    group_backup.write_to_db()
    return CommitState.READY


def _fail_group(group_backup, reason: str) -> CommitState:
    group_backup.status = GroupBackup.STATUS_FAILED
    group_backup.error_message = reason
    group_backup.write_to_db()
    return CommitState.FAILED


def _unwind_restored(lvol_ids: list) -> None:
    """Tear down the volumes a failed group restore already created (design §6.5).

    Best-effort: every volume is attempted even if one teardown fails, so a
    failed group restore leaves no half-group behind. The volumes are still
    STATUS_RESTORING, so the teardown is forced.
    """

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

    Returns ``(new_group_id, members, None)`` or ``(None, None, error_message)``,
    where ``members`` is ``[{"lvol_id", "member_name"}, ...]``, one per restored
    volume, in member order.
    """

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
    existing = db.get_consistency_group_by_name(cluster_id, into_group_name)
    if existing is not None:
        # A group record outlives its members: the last member out resets the
        # generation, it does not remove the group. So the name of an earlier
        # restore whose claims were since deleted is still taken by an EMPTY
        # record, and refusing it blocked every repeat restore into the same
        # name (live 2026-10-20, restored-cg). An empty leftover is reclaimed --
        # deleted, so the restore mints a fresh, correctly pinned group rather
        # than inheriting a stale node pin -- while a group with live members
        # is refused as before: a group restore always produces a new group.
        try:
            cg.delete_group(existing)
        except cg.ConsistencyGroupError as e:
            return None, None, (
                f"consistency group {into_group_name} already exists in cluster "
                f"{cluster_id} and is in use ({e}); a group restore always "
                "produces a new group")
        logger.info("Reclaimed the empty leftover consistency group %s (%s) for "
                    "the restore of group backup %s", into_group_name,
                    existing.uuid[:8], group_backup.uuid)

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
            chain = backup_chain.BackupChain.of_backups(UUID(backup.uuid), all_backups)
            chain.require_restorable(
                completed=True,
                what=f"member {backup.lvol_name} of group backup {group_backup.uuid}")
        except (PreconditionError, ValueError) as e:
            return None, None, str(e)

    # Create the member volumes, pinned to one store so they can form a group.
    # The first restore may auto-select a node; the rest are pinned to it, which
    # add_member_to_group then enforces (design §3.5). Each member is created
    # INTO the new group (add_lvol_ha's consistency_group): that is what lets a
    # member whose source shared a subsystem take the group's own subsystem
    # (cg_colocation), so the restored group is laid out as its source was --
    # one shared subsystem, members at NSIDs 1..N -- rather than one subsystem
    # per member (observed 2026-10-20).
    pin_node = target_node_id
    created_ids: list = []
    members_out: list = []
    for backup in member_backups:
        lvol_name = f"{into_group_name}_{backup.lvol_name}"
        try:
            lvol_id = backup_controller.restore_backup(
                backup.uuid, lvol_name, pool_id_or_name,
                target_node_id=pin_node, s3_credentials=s3_credentials,
                consistency_group=into_group_name)
        except (PreconditionError, RuntimeError, ValueError) as e:
            _unwind_restored(created_ids)
            return None, None, (
                f"member {backup.lvol_name} could not be restored: {e}")
        created_ids.append(lvol_id)
        # member_name is the source member's volume name, which the caller forms
        # the restored claim's name from (the operator names it
        # <claimName>-<member_name>) and uses to tell the members apart.
        members_out.append({"lvol_id": lvol_id, "member_name": backup.lvol_name})
        if pin_node is None:
            pin_node = db.get_lvol_by_id(lvol_id).node_id

    # Re-form the consistency group from the restored volumes. add_member_to_group
    # enforces single-store placement and fails loudly on an off-store member,
    # which the unwind then cleans up (design §6.4). The group we just created is
    # removed on failure too, so the half-formed name does not block a retry.
    group = cg.ensure_group(cluster_id, into_group_name)
    for lvol_id in created_ids:
        try:
            lvol = db.get_lvol_by_id(lvol_id)
            if getattr(lvol, "group_id", "") == group.get_id():
                continue    # joined at creation; re-adding would open a fresh epoch
            group = cg.add_member_to_group(group, lvol)
            # Stamp the volume too: add_member_to_group writes only the group,
            # and a member without group_id cannot be detached when it is
            # deleted, so its entry outlives it and holds the group "in use".
            lvol.group_id = group.get_id()
            lvol.write_to_db(db.kv_store)
        except (cg.ConsistencyGroupError, KeyError) as e:
            _unwind_restored(created_ids)
            db.get_consistency_group_by_id(group.get_id()).remove(db.kv_store)
            return None, None, (
                f"restored volumes could not form group {into_group_name}: {e}")

    logger.info("Restored group backup %s into consistency group %s (%d members)",
                group_backup.uuid, into_group_name, len(created_ids))
    return group.get_id(), members_out, None


class DeleteState(Enum):
    """Where one pass of a group backup's delete left it."""
    #: A member is still being retired: a merge in flight, or a successor or
    #: the member itself not yet completed. The task comes back.
    PENDING = "pending"
    #: Every member is retired and the manifest and record are gone, or the
    #: group backup is no longer being deleted.
    DONE = "done"


def delete_group_backup(group_backup_id):
    """Start deleting a group backup (design §9), metadata first.

    A group backup is created data first and committed last; it is deleted the
    other way round. This marks the group manifest and then the record as being
    deleted, before any member is touched, and queues the task that retires the
    members (:func:`advance_group_backup_delete`). The manifest and the record
    are removed only once every member is retired. A member backup that the next
    generation chains against is MERGED into it, so that generation stays
    restorable; one nothing chains against is deleted.

    Deleting a group backup already being deleted re-queues its task if none is
    open, and deleting a broken one resumes the retirement where it stopped.

    Raises:
        KeyError: no group backup has this id.
        PreconditionError: the group backup is still committing, or the bucket
            its members live in is not the cluster's configured one.
        ManifestError: the group manifest could not be marked; nothing changed.
    """

    db = db_controller.DBController()
    group_backup = db.get_group_backup_by_id(group_backup_id)
    if group_backup.status in (GroupBackup.STATUS_PENDING, GroupBackup.STATUS_IN_PROGRESS):
        raise PreconditionError(
            f"Group backup {group_backup.uuid} is still committing; delete it once "
            f"it has completed or failed")

    if group_backup.status != GroupBackup.STATUS_DELETING:
        # The bucket first: a crash after it leaves a manifest that says
        # deleting over a record that does not, which the next delete repairs,
        # rather than a record nothing will ever finish deleting.
        if group_backup.manifest_published():
            _mark_manifest(group_backup, _config_for_deleting(db, group_backup),
                           backup_manifest.GroupManifestState.DELETING)
        group_backup.status = GroupBackup.STATUS_DELETING
        group_backup.error_message = ""
        group_backup.write_to_db()
    tasks_controller.add_group_backup_delete_task(group_backup)


def advance_group_backup_delete(group_backup_id) -> DeleteState:
    """One pass of a group backup's delete: retire what members can be retired
    now, and once all are, remove the group manifest and then the record.

    Idempotent, so the delete task re-runs it until it answers ``DONE``.

    Raises:
        ManifestError, PreconditionError: the group manifest could not be
            removed, or a member could not be retired; retryable.
    """

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError:
        return DeleteState.DONE
    if group_backup.status != GroupBackup.STATUS_DELETING:
        return DeleteState.DONE

    # Resolved before this pass retires anything: a leaf deleted below takes the
    # record the bucket is found by with it.
    config = _config_for_deleting(db, group_backup)
    if not _retire_members(db, group_backup):
        return DeleteState.PENDING

    if group_backup.manifest_published():
        backup_manifest.delete_group(config, UUID(group_backup.uuid))
    group_backup.remove(db.kv_store)
    logger.info("Group backup %s deleted: every member retired", group_backup.uuid)
    return DeleteState.DONE


def abandon_group_backup_delete(group_backup_id, reason: str) -> None:
    """Settle a group backup whose delete task ended unfinished: a timeout, the
    retry ceiling, or a cancellation, none of which the task's handler sees.

    If no member was touched, the group backup is restored as it was, completed
    with its manifest committed again. If one was, what is left cannot restore as
    a group: it is marked broken, in the record and in the bucket, and deleting
    it again resumes the retirement. Best effort on the manifest: the record is
    written regardless, and a manifest left saying ``deleting`` is never read as
    committed.
    """

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError:
        return
    if group_backup.status != GroupBackup.STATUS_DELETING:
        return

    published = group_backup.manifest_published()
    if not published:
        group_backup.status = GroupBackup.STATUS_FAILED
    elif _members_touched(db, group_backup):
        group_backup.status = GroupBackup.STATUS_BROKEN
    else:
        group_backup.status = GroupBackup.STATUS_COMPLETED
    group_backup.error_message = f"delete did not finish: {reason}"

    if published:
        state = (backup_manifest.GroupManifestState.BROKEN
                 if group_backup.status == GroupBackup.STATUS_BROKEN
                 else backup_manifest.GroupManifestState.COMMITTED)
        try:
            _mark_manifest(group_backup, _config_for_deleting(db, group_backup), state)
        except (backup_manifest.ManifestError, PreconditionError) as e:
            logger.error("Group backup %s is %s, but its manifest could not be marked "
                         "%s and still says deleting: %s",
                         group_backup.uuid, group_backup.status, state, e)
    group_backup.write_to_db()
    logger.warning("Group backup %s delete abandoned (%s): now %s",
                   group_backup.uuid, reason, group_backup.status)


def _mark_manifest(group_backup, config, state) -> None:
    """Rewrite the group manifest in the bucket with ``state``, from the copy
    the bucket holds, so a broken group backup's manifest keeps naming the
    members it was committed with."""
    manifest = backup_manifest.read_group(config, UUID(group_backup.uuid))
    if manifest.state != state:
        backup_manifest.write_group(config, manifest.model_copy(update={"state": state}))


def _config_for_deleting(db, group_backup):
    """Credentials for a group backup's bucket while it is deleted.

    Its members' bucket, as :func:`_config_for_group` resolves it, while any
    member record is left. Retiring the last leaf removes the last record, and
    then it is the cluster's configured bucket: the one every member was checked
    against when the delete was marked.
    """
    for backup_id in group_backup.member_backup_ids():
        try:
            db.get_backup_by_id(backup_id)
        except KeyError:
            continue
        return _config_for_group(group_backup)
    return db.get_cluster_by_id(group_backup.cluster_id).get_backup_config()


def _members_touched(db, group_backup) -> bool:
    """Whether retiring a member has begun: a member deleted, merging, or merged."""
    for backup_id in group_backup.member_backup_ids():
        try:
            member = db.get_backup_by_id(backup_id)
        except KeyError:
            return True
        if member.status in (Backup.STATUS_MERGING, Backup.STATUS_MERGED):
            return True
    return False


def _retire_members(db, group_backup) -> bool:
    """Retire what members of a deleting group backup can be retired now.

    A member nothing chains against is deleted. One the next generation's copy
    chains against is merged into it once both are completed, one merge per
    volume chain at a time, as the single-volume retention keeps it. Returns
    whether every member is retired (deleted or merged).
    """

    retired = True
    for backup_id in group_backup.member_backup_ids():
        try:
            member = db.get_backup_by_id(backup_id)
        except KeyError:
            continue                    # deleted
        if member.status == Backup.STATUS_MERGED:
            continue
        chain = db.get_backups_by_lvol_id(member.lvol_id)
        successor = next((b for b in chain if b.prev_backup_id == member.uuid), None)
        busy = (member.status in (Backup.STATUS_PENDING, Backup.STATUS_IN_PROGRESS)
                or any(b.status == Backup.STATUS_MERGING for b in chain))
        if successor is None and not busy:
            try:
                backup_controller.delete_backup(member.uuid)
            except KeyError:
                pass                    # deleted concurrently
            logger.info("Group backup %s: member %s deleted, nothing chains against it",
                        group_backup.uuid, member.uuid[:8])
            continue
        retired = False
        if (successor is not None and not busy
                and member.status == Backup.STATUS_COMPLETED
                and successor.status == Backup.STATUS_COMPLETED):
            backup_controller.trigger_merge(successor, member)
            logger.info("Group backup %s: member %s merging into its successor %s",
                        group_backup.uuid, member.uuid[:8], successor.uuid[:8])
    return retired


def retire_orphaned_member_backups(lvol_ids, db=None):
    """Retire the member backups of group backups that no longer exist (design
    §9): for each of *lvol_ids*, every completed backup whose ``group_backup_id``
    names a GroupBackup that is gone (pruned, or deleted by hand).

    * A backup the next generation chains against (a successor names it as
      ``prev_backup_id``) is MERGED into that successor, the same
      ``backup_controller.trigger_merge`` the single-volume schedule runs: the chain
      stays restorable and the base's objects fold into the survivor. One
      merge per volume chain, and none while one is already in flight there,
      as the single-volume schedule keeps it; the next cycle takes the rest.
    * A backup nothing chains against is deleted outright.

    Called by the group schedule every cycle, for member copies orphaned before
    a group backup's delete retired its own members (:func:`delete_group_backup`).
    A member of a group backup being deleted is not an orphan: its group backup
    still exists, and its delete task retires it.

    Returns ``(merges_started, deleted)``.
    """

    if db is None:
        db = db_controller.DBController()
    merges = deleted = 0
    for lvol_id in lvol_ids:
        chain = db.get_backups_by_lvol_id(lvol_id)
        if any(b.status == Backup.STATUS_MERGING for b in chain):
            continue
        orphans = []
        for b in chain:
            if b.status != Backup.STATUS_COMPLETED or not b.group_backup_id:
                continue
            try:
                db.get_group_backup_by_id(b.group_backup_id)
            except KeyError:
                orphans.append(b)
        by_prev = {b.prev_backup_id: b for b in chain if b.prev_backup_id}
        # Oldest first: the base of the chain is the one whose merge unblocks
        # the next, and a leaf is only a leaf once nothing is below it.
        for old in sorted(orphans, key=lambda b: (b.created_at, b.uuid)):
            successor = by_prev.get(old.uuid)
            if successor is None:
                try:
                    backup_controller.delete_backup(old.uuid)
                except (KeyError, PreconditionError) as e:
                    logger.warning("Orphaned member backup %s of volume %s not deleted: %s",
                                   old.uuid[:8], lvol_id[:8], e)
                    continue
                deleted += 1
                logger.info("Retired orphaned member backup %s of volume %s: "
                            "nothing chains against it, deleted", old.uuid[:8], lvol_id[:8])
                continue
            if successor.status != Backup.STATUS_COMPLETED:
                break           # let it finish; the next cycle merges
            backup_controller.trigger_merge(successor, old)
            merges += 1
            logger.info("Retiring orphaned member backup %s of volume %s: merging into "
                        "its successor %s", old.uuid[:8], lvol_id[:8], successor.uuid[:8])
            break               # one merge per chain per cycle
    return merges, deleted


def export_group_backup(group_backup_id, cluster_id=None):
    """Export a group backup's manifests as a unit, for import into another
    cluster (design §6.5).

    Emits the group manifest together with the member backup chains it names, as
    one :class:`backup_manifest.GroupBackupExport`. The member chains are grouped
    by the location they live in exactly as the single-volume
    :func:`controller.export_backups` groups its manifests, so the same import
    machinery lands the member ``Backup`` records; the group manifest is what ties
    them back into a ``GroupBackup`` on the way in.

    The member manifests are each member's whole restorable chain, not just the
    member backup itself: an import has to land every ancestor for the group to be
    restorable, the unit ``export_backups(backup_id=...)`` produces for one volume.

    Args:
        cluster_id: scopes the backup population the chains are walked over, as in
            :func:`controller.export_backups`. The group backup's own cluster by
            construction; passed through so a caller can state it.

    Returns a :class:`backup_manifest.GroupBackupExport`.

    Raises:
        PreconditionError: the group backup is unknown, is not a committed unit, a
            member backup record is missing, or a member's chain is broken.
        ValueError: one of the selected backups records no usable location, so it
            cannot be described.
    """

    db = db_controller.DBController()
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError as e:
        raise PreconditionError(str(e)) from e

    # Only a committed group backup is a unit worth exporting: an in-progress or
    # failed one has no published group manifest and is not crash-consistent
    # (design §3.3), the same rule that gates restore.
    if group_backup.status != GroupBackup.STATUS_COMPLETED:
        raise PreconditionError(
            f"group backup {group_backup_id} is {group_backup.status}, not "
            f"completed; only a committed group backup can be exported")

    member_by_id = {}
    for entry in group_backup.members:
        try:
            backup = db.get_backup_by_id(entry["backup_id"])
        except KeyError as e:
            raise PreconditionError(
                f"a member backup of group backup {group_backup_id} is "
                f"missing: {e}") from e
        member_by_id[backup.get_id()] = backup

    group_manifest = build_group_manifest(group_backup, member_by_id)

    # Each member's whole chain, collected and deduplicated across members: an
    # import lands every ancestor so the group is restorable, the unit
    # export_backups(backup_id=...) produces for one volume.
    all_backups = db.get_backups(cluster_id)
    chain_records = {}
    for backup in member_by_id.values():
        try:
            chain = backup_chain.BackupChain.of_backups(UUID(backup.uuid), all_backups)
        except ValueError as e:
            raise PreconditionError(str(e)) from e
        for record in chain.records():
            chain_records[record.uuid] = record

    return backup_manifest.GroupBackupExport(
        group_manifest=group_manifest,
        members=backup_controller._group_by_location(chain_records.values()),
    )


def discover_group_backups(config):
    """Every group backup in a bucket, read from its group manifests alone
    (design §6.5).

    The disaster-recovery entry point for group backups, parallel to the
    single-volume :func:`controller.discover_backups`: given a bucket and
    credentials for it, this answers "what group backups are in here" with no
    reference to any cluster, live or dead. Reads the ``groups/`` prefix through
    :func:`backup_manifest.list_all_groups`.

    Raises:
        ManifestError: the bucket could not be listed, or one of its group
            manifests could not be parsed.
    """
    return backup_manifest.list_all_groups(config)


def import_group_backup(export, cluster_id=None):
    """Register a group backup described by a ``GroupBackupExport`` into this
    cluster, preserving ids and all-or-nothing across the group (design §6.5).

    Recreates both the member ``Backup`` records AND the ``GroupBackup`` record
    that ties them together, keeping every id: the member uuids and s3_ids (which
    name the objects in the bucket and so cannot be reassigned), the group
    backup's own uuid, and the generation. The whole group imports or none of it
    does -- a group id collision, a member id collision, or a broken member chain
    refuses the batch before the group record is written.

    The member backups go in through the single-volume
    :func:`controller.import_backups`, which is where the chain validation and the
    member-id-collision rule live; this adds the group record on top and stamps
    each imported member with the group backup id and generation that the member
    manifests do not themselves carry.

    Args:
        export: a validated :class:`backup_manifest.GroupBackupExport`, from
            :func:`export_group_backup`, a bucket read, or a file parsed into one.
        cluster_id: the cluster to import into, so the records are visible in its
            namespace. The group manifest's own source cluster when omitted.

    Returns the imported group backup's composite ``cluster_id/uuid`` id.

    Raises:
        PreconditionError: the group backup id is already known, or the member
            import's own preconditions fail (a member id collision, a broken or
            over-long chain).
        ValueError: the group names a member whose manifest the export does not
            carry, so the group would import incomplete.
    """

    db = db_controller.DBController()
    group_manifest = export.group_manifest
    target_cluster = cluster_id or str(group_manifest.source.cluster_id)
    uuid = str(group_manifest.group_backup_id)
    composite_id = f"{target_cluster}/{uuid}"

    # Only a committed group backup is whole. One being deleted on its source,
    # or broken by a delete that did not finish, has members that are retired
    # or about to be.
    if group_manifest.state != backup_manifest.GroupManifestState.COMMITTED:
        raise PreconditionError(
            f"Group backup {uuid} is {group_manifest.state} in its bucket, not "
            f"committed; it cannot be imported")

    # Reject a group id collision before writing anything -- the group analog of
    # import_backups' per-backup collision check. Group-backup lookups resolve by
    # uuid, so a reused uuid would make either record unaddressable.
    try:
        existing = db.get_group_backup_by_id(composite_id)
    except KeyError:
        pass
    else:
        raise PreconditionError(
            f"Group backup {uuid} already exists in cluster {existing.cluster_id}")

    # Every member the group names has to be present in the export: a group that
    # imported without one of its members would be a torn generation that looks
    # complete in `group-backup list` and fails only at restore.
    exported = {
        manifest.backup_id
        for group in export.members.groups for manifest in group.manifests
    }
    missing = [backup_id for backup_id in group_manifest.member_backup_ids
               if backup_id not in exported]
    if missing:
        raise ValueError(
            "group export is missing member manifests for "
            + ", ".join(str(b) for b in missing))

    # Land the member backups all-or-nothing (chains, id collisions) on the
    # single-volume path. Nothing above this wrote a record, so a refusal here
    # leaves the import empty.
    backup_controller.import_backups(export.members, cluster_id=target_cluster)

    # Stamp each imported member with the group it belongs to -- the member
    # manifests do not carry it, so import_backups left it blank -- and the
    # generation, both preserved from the group manifest.
    members = []
    for backup_id in group_manifest.member_backup_ids:
        backup = db.get_backup_by_id(str(backup_id))
        backup.group_backup_id = composite_id
        backup.group_seq = group_manifest.source.generation
        backup.write_to_db()
        # The member's volume comes from its own manifest, which import_backups
        # recorded on the backup.
        members.append({"lvol_id": backup.lvol_id, "backup_id": backup.get_id()})

    # Recreate the GroupBackup record, preserving its uuid and generation. It is
    # imported COMPLETED: only a committed group backup is exportable, so what
    # arrives is always a committed one.
    group_backup = GroupBackup()
    group_backup.uuid = uuid
    group_backup.cluster_id = target_cluster
    group_backup.consistency_group_id = group_manifest.source.consistency_group_id
    group_backup.consistency_group_name = group_manifest.source.consistency_group_name
    group_backup.group_seq = group_manifest.source.generation
    group_backup.members = members
    group_backup.size = group_manifest.size
    group_backup.created_at = int(group_manifest.created_at.timestamp())
    group_backup.completed_at = int(group_manifest.completed_at.timestamp())
    group_backup.status = GroupBackup.STATUS_COMPLETED
    group_backup.write_to_db()

    return group_backup.get_id()


def import_group_backup_from_bucket(config, cluster_id=None):
    """Import every group backup a bucket holds (design §6.5).

    The disaster-recovery path: given a bucket and credentials for it, read its
    group manifests and the member manifests they name, and import each group
    backup. Reuses the single-volume bucket read for the member manifests, since a
    group's members live in the same bucket (§3.4), and walks each member's chain
    over them so a group imports exactly its own members' chains rather than every
    backup the bucket happens to hold.

    Each group backup is imported atomically (:func:`import_group_backup`); a
    collision or broken chain refuses that group backup, and the ones imported
    before it remain.

    Returns the composite ids of the group backups imported.

    Raises:
        ManifestError: the bucket could not be read.
        PreconditionError: a group backup it holds cannot be imported.
        ValueError: a group manifest names a member the bucket does not hold.
    """

    group_manifests = backup_manifest.list_all_groups(config)
    member_manifests = backup_manifest.list_all(config)
    location = config.location()
    by_id = {manifest.backup_id: manifest for manifest in member_manifests}

    imported = []
    for group_manifest in group_manifests:
        # A bucket holds group backups being deleted on their source too; they
        # are not part of what it can restore, so they are left, not refused.
        if group_manifest.state != backup_manifest.GroupManifestState.COMMITTED:
            logger.info("Group backup %s is %s in the bucket; not importing it",
                        group_manifest.group_backup_id, group_manifest.state)
            continue
        # Each named member's whole chain, gathered from the bucket's member
        # manifests. Walked rather than taking every manifest in the bucket, which
        # would pull in other group backups' and standalone backups' chains.
        chain_manifests = {}
        for backup_id in group_manifest.member_backup_ids:
            head = by_id.get(backup_id)
            if head is None:
                raise ValueError(
                    f"group backup {group_manifest.group_backup_id} names member "
                    f"backup {backup_id}, which the bucket does not hold")
            # of_manifests builds its links from manifests; the isinstance narrows
            # the Backup|BackupManifest link type back to the manifest it is.
            for link in backup_chain.BackupChain.of_manifests(head, member_manifests, location).links:
                if isinstance(link, backup_manifest.BackupManifest):
                    chain_manifests[link.backup_id] = link

        export = backup_manifest.GroupBackupExport(
            group_manifest=group_manifest,
            members=backup_manifest.BackupExport(groups=[
                backup_manifest.LocatedManifests(
                    location=location, manifests=list(chain_manifests.values()))]),
        )
        imported.append(import_group_backup(export, cluster_id=cluster_id))

    return imported
