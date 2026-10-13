"""Consistency-group backup: back up the member snapshots of one group
generation to S3 as one all-or-nothing unit (design-consistency-group-backup.md).

This module holds the group-level decisions that sit on top of the per-member
single-volume backup path (``controllers/backup/controller.py``): the
homogeneity gate that the first version requires (§3.4) and the all-or-nothing
commit decision that makes a group backup visible only when every member
completed (§3.3). The orchestration around them -- resolving or taking a
generation, creating the ``GroupBackup`` record, enqueuing the member backups,
and publishing the group manifest last -- drives the database and the data plane
and lives beside the single-volume create flow.
"""
from simplyblock_core.models.backup import Backup

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
