"""The decision core of consistency-group backup: the homogeneity gate and the
all-or-nothing commit decision (design-consistency-group-backup.md §3.3, §3.4).

Pure logic -- the orchestration around it (resolving a generation, creating the
record, enqueuing member backups, publishing the group manifest) touches the
database and the data plane and is the integration tier's.
"""
import zlib
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
        assert [m.lvol_name for m in manifest.members] == ["db", "data"]
        assert manifest.encrypted is False
        assert manifest.is_complete()

    def test_carries_the_encrypted_disposition(self):
        gb, by_id = self._group_and_members(encrypted=True)
        assert group_backup.build_group_manifest(gb, by_id).encrypted is True

    def test_a_missing_member_backup_is_an_error(self):
        gb, by_id = self._group_and_members()
        by_id.pop(gb.members[0]["backup_id"])   # drop one member's record
        with pytest.raises(ValueError, match="missing"):
            group_backup.build_group_manifest(gb, by_id)


class TestRestoreIntoALeftoverName:
    """A group record outlives its members (the last member out resets the
    generation, nothing removes the record), so the name of an earlier restore
    whose claims were deleted is still taken by an EMPTY group. The restore
    reclaims it; a group with live members is still refused."""

    def _run(self, monkeypatch, leftover_members):
        from simplyblock_core import db_controller
        from simplyblock_core.controllers import consistency_group_controller as cg
        from simplyblock_core.controllers.backup import group_backup
        from simplyblock_core.models.replication import ConsistencyGroup

        gb = GroupBackup()
        gb.uuid = _uuid("gb")
        gb.cluster_id = _uuid("cluster")
        gb.status = GroupBackup.STATUS_COMPLETED
        gb.members = [{"lvol_id": _uuid("m1"), "backup_id": _uuid("b1")}]

        leftover = ConsistencyGroup()
        leftover.uuid = _uuid("old-group")
        leftover.cluster_id = gb.cluster_id
        leftover.group_name = "restored-cg"
        leftover.members = leftover_members
        deleted = []

        class _DB:
            def get_group_backup_by_id(self, _id):
                return gb

            def get_consistency_group_by_name(self, cluster_id, name):
                return leftover if name == "restored-cg" else None

            def get_backup_by_id(self, _id):
                # Past the name check: stop the restore here, before any volume.
                raise KeyError("stop: member lookup reached")

        monkeypatch.setattr(db_controller, "DBController", lambda: _DB())
        def _delete(group):
            # The real rule: only a group with no open-epoch member may go.
            open_members = [k for k, m in (group.members or {}).items()
                            if m.get("removed_seq", 0) == 0]
            if open_members:
                raise cg.ConsistencyGroupError(f"still has {len(open_members)} member(s)")
            deleted.append(group.uuid)

        monkeypatch.setattr(cg, "delete_group", _delete)
        result = group_backup.restore_group_backup(gb.get_id(), "restored-cg", "pool-1")
        return result, deleted

    def test_an_empty_leftover_is_reclaimed(self, monkeypatch):
        (group_id, members, err), deleted = self._run(monkeypatch, {})
        assert deleted == [_uuid("old-group")], "the empty record must be deleted, not reused"
        assert group_id is None and "member lookup reached" in err, \
            "the restore must proceed past the name check"

    def test_a_closed_epoch_member_does_not_hold_the_name(self, monkeypatch):
        (_, _, err), deleted = self._run(
            monkeypatch, {_uuid("gone"): {"joined_seq": 1, "removed_seq": 3}})
        assert deleted == [_uuid("old-group")]
        assert "member lookup reached" in err

    def test_a_group_with_a_live_member_is_still_refused(self, monkeypatch):
        (group_id, members, err), deleted = self._run(
            monkeypatch, {_uuid("live"): {"joined_seq": 1, "removed_seq": 0}})
        assert deleted == []
        assert group_id is None and "already exists" in err and "in use" in err
