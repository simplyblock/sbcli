"""Group restore re-forms a consistency group from a group backup, all-or-nothing,
against real FoundationDB (design-consistency-group-backup.md §6).

The database is real; nothing here mocks the DB layer. The boundaries patched are
the ones above it: the single-volume restore (which creates the volume and drives
the data-plane recovery), the chain validation (its own unit covers it), and the
volume teardown used to unwind. The group forming (ensure_group /
add_member_to_group) runs for real against FDB, which is what the all-or-nothing
and single-store rules turn on.
"""
import zlib
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import consistency_group_controller as cg
from simplyblock_core.controllers.backup import group_backup
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup, GroupBackup
from simplyblock_core.models.lvol_model import LVol


def _uuid(name: str) -> str:
    # Restore validates each member's chain with UUID(backup.uuid), so member
    # backup ids must be real UUIDs, not readable labels.
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")
RESTORE_BACKUP = "simplyblock_core.controllers.backup.controller.restore_backup"
BACKUP_CHAIN = "simplyblock_core.controllers.backup.chain.BackupChain"
DELETE_LVOL = "simplyblock_core.controllers.lvol_controller.delete_lvol"


@pytest.fixture
def db():
    return DBController()


def _member(name, lvol_name, status=Backup.STATUS_COMPLETED):
    b = Backup()
    b.uuid = _uuid(name)
    b.cluster_id = CLUSTER
    b.lvol_id = _uuid("lv-" + name)
    b.lvol_name = lvol_name
    b.size = 100
    b.status = status
    return b


def _seed_group(db, members, status=GroupBackup.STATUS_COMPLETED):
    for m in members:
        m.write_to_db(db.kv_store)
    gb = GroupBackup()
    gb.uuid = "gb-1"
    gb.cluster_id = CLUSTER
    gb.consistency_group_id = f"{CLUSTER}/cg-1"
    gb.consistency_group_name = "gitea"
    gb.group_seq = 7
    gb.status = status
    gb.members = [{"lvol_id": m.lvol_id, "backup_id": m.get_id()} for m in members]
    gb.write_to_db(db.kv_store)
    return gb


def _restored_lvol(db, uuid, node="node-1", lvs="LVS_1"):
    """A volume standing in for what the (mocked) single-volume restore creates."""
    lv = LVol()
    lv.uuid = uuid
    lv.cluster_id = CLUSTER
    lv.pool_uuid = "pool-1"
    lv.node_id = node
    lv.lvs_name = lvs
    lv.lvol_name = uuid
    lv.status = LVol.STATUS_RESTORING
    lv.write_to_db(db.kv_store)
    return lv


def test_restore_forms_a_new_group_from_all_members(db):
    gb = _seed_group(db, [_member("b-db", "db"), _member("b-data", "data")])
    _restored_lvol(db, "rv-db")
    _restored_lvol(db, "rv-data")

    with patch(RESTORE_BACKUP, side_effect=["rv-db", "rv-data"]) as restore, \
            patch(BACKUP_CHAIN):
        group_id, members, err = group_backup.restore_group_backup(
            gb.get_id(), "restored-cg", "pool-1")

    assert err is None
    assert members == [
        {"lvol_id": "rv-db", "member_name": "db"},
        {"lvol_id": "rv-data", "member_name": "data"}]
    # The second member is pinned to the first member's node, not auto-placed.
    assert restore.call_args_list[1].kwargs["target_node_id"] == "node-1"

    group = db.get_consistency_group_by_name(CLUSTER, "restored-cg")
    assert group is not None
    assert group.get_id() == group_id
    assert set(group.members) == {"rv-db", "rv-data"}


def test_a_member_restore_failure_unwinds_and_forms_no_group(db):
    gb = _seed_group(db, [_member("b-db", "db"), _member("b-data", "data")])
    _restored_lvol(db, "rv-db")

    with patch(RESTORE_BACKUP, side_effect=["rv-db", RuntimeError("recovery RPC failed")]), \
            patch(BACKUP_CHAIN), \
            patch(DELETE_LVOL) as delete_lvol:
        group_id, lvol_ids, err = group_backup.restore_group_backup(
            gb.get_id(), "restored-cg", "pool-1")

    assert group_id is None and lvol_ids is None
    assert "could not be restored" in err
    # The one volume already created is torn down, and no group was formed.
    assert delete_lvol.call_count == 1
    assert db.get_consistency_group_by_name(CLUSTER, "restored-cg") is None


def test_an_off_store_member_unwinds_including_the_half_formed_group(db):
    gb = _seed_group(db, [_member("b-db", "db"), _member("b-data", "data")])
    _restored_lvol(db, "rv-db", node="node-1", lvs="LVS_1")
    _restored_lvol(db, "rv-data", node="node-2", lvs="LVS_2")   # a different store

    with patch(RESTORE_BACKUP, side_effect=["rv-db", "rv-data"]), \
            patch(BACKUP_CHAIN), \
            patch(DELETE_LVOL) as delete_lvol:
        group_id, lvol_ids, err = group_backup.restore_group_backup(
            gb.get_id(), "restored-cg", "pool-1")

    assert group_id is None
    assert "could not form group" in err
    # Both restored volumes are torn down and the half-formed group is removed,
    # so the name does not block a retry.
    assert delete_lvol.call_count == 2
    assert db.get_consistency_group_by_name(CLUSTER, "restored-cg") is None


def test_refuses_an_uncommitted_group_backup(db):
    gb = _seed_group(db, [_member("b-db", "db")],
                     status=GroupBackup.STATUS_IN_PROGRESS)
    group_id, lvol_ids, err = group_backup.restore_group_backup(
        gb.get_id(), "restored-cg", "pool-1")
    assert group_id is None and lvol_ids is None
    assert "not" in err and "completed" in err


def test_refuses_a_name_held_by_an_empty_group_and_keeps_it(db):
    """A group restore always produces a new group. An empty group holding the
    name is not known to be meant for replacing, so it is refused, never
    deleted."""
    gb = _seed_group(db, [_member("b-db", "db")])
    existing = cg.ensure_group(CLUSTER, "taken")
    group_id, lvol_ids, err = group_backup.restore_group_backup(
        gb.get_id(), "taken", "pool-1")
    assert group_id is None and lvol_ids is None
    assert "already exists" in err
    assert db.get_consistency_group_by_name(CLUSTER, "taken").uuid == existing.uuid


def test_refuses_when_the_target_group_name_is_in_use(db):
    """A group with a live member is not a leftover: a group restore always
    produces a NEW group, never one that is serving volumes."""
    gb = _seed_group(db, [_member("b-db", "db")])
    taken = cg.ensure_group(CLUSTER, "taken")
    # A LIVE member: its volume record exists. An open entry whose volume is
    # gone is dangling and would not hold the name (see delete_group).
    live = LVol()
    live.uuid = _uuid("live-member")
    live.lvol_name = "live"
    live.group_id = taken.get_id()
    live.write_to_db(db.kv_store)
    taken.members = {live.uuid: {"joined_seq": 1, "removed_seq": 0}}
    taken.write_to_db(db.kv_store)
    group_id, lvol_ids, err = group_backup.restore_group_backup(
        gb.get_id(), "taken", "pool-1")
    assert group_id is None
    assert "already exists" in err
    assert db.get_consistency_group_by_name(CLUSTER, "taken") is not None


def test_refuses_when_a_member_backup_record_is_missing(db):
    members = [_member("b-db", "db")]
    gb = _seed_group(db, members)
    # Record a second member whose Backup record was never written.
    gb.members.append({"lvol_id": f"{CLUSTER}/lv-gone", "backup_id": f"{CLUSTER}/b-gone"})
    gb.write_to_db(db.kv_store)

    group_id, lvol_ids, err = group_backup.restore_group_backup(
        gb.get_id(), "restored-cg", "pool-1")
    assert group_id is None
    assert "missing" in err
