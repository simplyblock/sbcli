"""The decision core of consistency-group backup: the homogeneity gate and the
all-or-nothing commit decision (design-consistency-group-backup.md §3.3, §3.4).

Pure logic -- the orchestration around it (resolving a generation, creating the
record, enqueuing member backups, publishing the group manifest) touches the
database and the data plane and is the integration tier's.
"""
from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.models.backup import Backup

LOC_A = {"bucket_name": "backups", "region": "us-east-1"}
LOC_B = {"bucket_name": "other", "region": "us-east-1"}


def _m(lvol_id, location=None, encrypted=False):
    return {"lvol_id": lvol_id, "location": location or LOC_A, "encrypted": encrypted}


class TestHomogeneity:
    def test_matching_members_are_homogeneous(self):
        members = [_m("db"), _m("data"), _m("web")]
        assert group_backup.check_members_homogeneous(members) == []

    def test_a_different_bucket_is_an_offender(self):
        members = [_m("db"), _m("data", location=LOC_B)]
        problems = group_backup.check_members_homogeneous(members)
        assert [p[0] for p in problems] == ["data"]
        assert "location" in problems[0][1]

    def test_a_different_encryption_disposition_is_an_offender(self):
        members = [_m("db"), _m("data", encrypted=True)]
        problems = group_backup.check_members_homogeneous(members)
        assert [p[0] for p in problems] == ["data"]
        assert "encryption" in problems[0][1]

    def test_a_single_member_is_trivially_homogeneous(self):
        assert group_backup.check_members_homogeneous([_m("db")]) == []

    def test_no_members_is_homogeneous(self):
        assert group_backup.check_members_homogeneous([]) == []


class TestCommitDecision:
    def test_all_completed_is_ready_to_commit(self):
        statuses = [Backup.STATUS_COMPLETED, Backup.STATUS_COMPLETED]
        assert group_backup.group_commit_state(statuses) == group_backup.COMMIT_READY

    def test_any_failed_fails_the_group(self):
        statuses = [Backup.STATUS_COMPLETED, Backup.STATUS_FAILED]
        assert group_backup.group_commit_state(statuses) == group_backup.COMMIT_FAILED

    def test_an_in_flight_member_holds_the_group(self):
        statuses = [Backup.STATUS_COMPLETED, Backup.STATUS_IN_PROGRESS]
        assert group_backup.group_commit_state(statuses) == group_backup.COMMIT_PENDING

    def test_a_pending_member_holds_the_group(self):
        statuses = [Backup.STATUS_PENDING, Backup.STATUS_PENDING]
        assert group_backup.group_commit_state(statuses) == group_backup.COMMIT_PENDING

    def test_failure_wins_over_still_in_flight(self):
        # All-or-nothing: one terminal failure fails the group even while others
        # are still uploading (design §3.3).
        statuses = [Backup.STATUS_FAILED, Backup.STATUS_IN_PROGRESS]
        assert group_backup.group_commit_state(statuses) == group_backup.COMMIT_FAILED

    def test_no_members_is_not_ready(self):
        assert group_backup.group_commit_state([]) == group_backup.COMMIT_PENDING
