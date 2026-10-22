"""The decision core of consistency-group backup: the homogeneity gate and the
all-or-nothing commit decision (design-consistency-group-backup.md §3.3, §3.4).

Pure logic -- the orchestration around it (resolving a generation, creating the
record, enqueuing member backups, publishing the group manifest) touches the
database and the data plane and is the integration tier's.
"""
import zlib
from datetime import UTC, datetime
from uuid import UUID

import pytest

from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.models.backup import Backup, GroupBackup


def _uuid(name: str) -> str:
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"

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
        assert group_backup.group_commit_state(statuses) == group_backup.CommitState.READY

    def test_any_failed_fails_the_group(self):
        statuses = [Backup.STATUS_COMPLETED, Backup.STATUS_FAILED]
        assert group_backup.group_commit_state(statuses) == group_backup.CommitState.FAILED

    def test_an_in_flight_member_holds_the_group(self):
        statuses = [Backup.STATUS_COMPLETED, Backup.STATUS_IN_PROGRESS]
        assert group_backup.group_commit_state(statuses) == group_backup.CommitState.PENDING

    def test_a_pending_member_holds_the_group(self):
        statuses = [Backup.STATUS_PENDING, Backup.STATUS_PENDING]
        assert group_backup.group_commit_state(statuses) == group_backup.CommitState.PENDING

    def test_failure_wins_over_still_in_flight(self):
        # All-or-nothing: one terminal failure fails the group even while others
        # are still uploading (design §3.3).
        statuses = [Backup.STATUS_FAILED, Backup.STATUS_IN_PROGRESS]
        assert group_backup.group_commit_state(statuses) == group_backup.CommitState.FAILED

    def test_no_members_is_not_ready(self):
        assert group_backup.group_commit_state([]) == group_backup.CommitState.PENDING

    def test_the_decision_is_a_commit_state_not_a_bare_string(self):
        """A closed set: a caller comparing against a string literal is a bug
        the type makes visible rather than one that silently never matches."""
        state = group_backup.group_commit_state([Backup.STATUS_COMPLETED])
        assert isinstance(state, group_backup.CommitState)
        assert state != "ready"
        assert set(group_backup.CommitState) == {
            group_backup.CommitState.PENDING, group_backup.CommitState.READY, group_backup.CommitState.FAILED}


class TestBuildGroupManifest:
    """build_group_manifest assembles the group manifest from the GroupBackup
    record and its member Backup records (design §4.2)."""

    def _group_and_members(self, encrypted=False):
        cluster = _uuid("cluster")
        cg = f"{cluster}/{_uuid('cg')}"
        gb = GroupBackup()
        gb.uuid = _uuid("gb")
        gb.cluster_id = cluster
        gb.consistency_group_id = cg
        gb.consistency_group_name = "gitea"
        gb.group_seq = 7
        gb.created_at = 100
        gb.completed_at = 200

        members, by_id = [], {}
        for name, size in (("db", 100), ("data", 50)):
            b = Backup()
            b.uuid = _uuid("b-" + name)
            b.cluster_id = cluster
            b.lvol_id = _uuid("lv-" + name)
            b.lvol_name = name
            b.s3_id = size
            b.size = size
            b.encrypted = encrypted
            members.append({"lvol_id": b.lvol_id, "backup_id": b.get_id()})
            by_id[b.get_id()] = b
        gb.members = members
        return gb, by_id

    def test_builds_from_records(self):
        gb, by_id = self._group_and_members()
        manifest = group_backup.build_group_manifest(gb, by_id)

        assert manifest.group_backup_id == UUID(gb.uuid)
        assert manifest.source.consistency_group_id == gb.consistency_group_id
        assert manifest.source.generation == 7
        assert manifest.size == 150
        assert manifest.member_backup_ids == [UUID(by_id[e["backup_id"]].uuid) for e in gb.members]
        assert manifest.created_at == datetime.fromtimestamp(gb.created_at, tz=UTC)
        assert manifest.completed_at == datetime.fromtimestamp(gb.completed_at, tz=UTC)
        assert manifest.encrypted is False

    def test_carries_the_encrypted_disposition(self):
        gb, by_id = self._group_and_members(encrypted=True)
        assert group_backup.build_group_manifest(gb, by_id).encrypted is True

    def test_a_missing_member_backup_is_an_error(self):
        gb, by_id = self._group_and_members()
        by_id.pop(gb.members[0]["backup_id"])   # drop one member's record
        with pytest.raises(ValueError, match="missing"):
            group_backup.build_group_manifest(gb, by_id)
