"""The GroupBackup model and the group fields on Backup.

A GroupBackup ties the N per-member Backup records of one consistency-group
generation together as one all-or-nothing unit. This test pins its field shape,
its serialization round-trip, the non-shared mutable default, and the group fields
added to Backup.
"""
from simplyblock_core.models.backup import Backup, GroupBackup


def _group_backup():
    gb = GroupBackup()
    gb.uuid = "gb-1"
    gb.cluster_id = "cl-1"
    gb.consistency_group_id = "cl-1/cg-1"
    gb.consistency_group_name = "gitea"
    gb.group_seq = 7
    gb.member_backup_ids = ["cl-1/b-db", "cl-1/b-data"]
    gb.size = 3 * 1024 ** 3
    return gb


def test_group_backup_round_trips():
    gb = _group_backup()
    restored = GroupBackup(gb.to_dict())
    assert restored.uuid == "gb-1"
    assert restored.get_id() == "cl-1/gb-1"
    assert restored.consistency_group_id == "cl-1/cg-1"
    assert restored.group_seq == 7
    assert restored.member_backup_ids == ["cl-1/b-db", "cl-1/b-data"]


def test_the_group_record_names_its_members_by_backup_id_alone():
    """Like the DTO and the group manifest: each member Backup carries its own
    volume, so the group record does not repeat it beside the backup id."""
    attrs = GroupBackup().get_attrs_map()
    assert "member_backup_ids" in attrs
    assert "members" not in attrs


def test_the_group_record_does_not_repeat_its_members_encryption():
    """Each member Backup records whether it is encrypted; the group record does
    not keep a copy that would have to agree with them."""
    assert "encrypted" not in GroupBackup().get_attrs_map()


def test_group_backup_members_default_is_not_shared():
    a, b = GroupBackup(), GroupBackup()
    assert a.member_backup_ids is not b.member_backup_ids
    a.member_backup_ids.append("cl-1/b-x")
    assert b.member_backup_ids == []


def test_group_backup_has_its_own_statuses_and_does_not_merge():
    assert GroupBackup.STATUS_PENDING in GroupBackup._STATUS_CODE_MAP
    assert GroupBackup.STATUS_COMPLETED in GroupBackup._STATUS_CODE_MAP
    # A group backup does not merge: merging is a per-member single-volume
    # concern on Backup, not a group one.
    assert not hasattr(GroupBackup, "STATUS_MERGING")


def test_backup_gains_group_fields_defaulting_to_standalone():
    b = Backup()
    assert b.group_backup_id == ""     # standalone single-volume backup by default
    assert b.group_seq == 0
    b.group_backup_id = "cl-1/gb-1"
    b.group_seq = 7
    restored = Backup(b.to_dict())
    assert restored.group_backup_id == "cl-1/gb-1"
    assert restored.group_seq == 7


def test_backup_indexes_group_backup_id():
    # The blank group_backup_id of a standalone backup is auto-dropped by the
    # index, so only group members are indexed (models/indices.py Index).
    assert "group_backup_id" in [ix.name for ix in Backup._INDEXES]
