"""A snapshot of a volume waits until the volume's newest snapshot has finished
deleting on every lvstore member.

bf28 (2026-11-19, LVS_1): replication async-deleted SNAP_615 of LVOL_33 at
14:12:57, took SNAP_619 of LVOL_33 at 14:13:41, and the peers' sync deletes of
SNAP_615 ran at 14:14:43. SNAP_619 registered on the peers while they still
listed LVOL_33 under SNAP_615, so the peers' later delete re-parented LVOL_33 a
second time (spdk #96). The node-level sync-delete gate was not set: no peer was
down.
"""
import inspect
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import snapshot_controller as sc
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot


def _snap(uuid, created_at, status=SnapShot.STATUS_ONLINE, bdev=None):
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.status = status
    s.snap_bdev = bdev or f"LVS_1/{uuid}"
    return s


@pytest.fixture
def db():
    with patch.object(sc, "db_controller") as m:
        yield m


def test_newest_snapshot_in_deletion_is_in_flight(db):
    db.get_snapshots_by_lvol_id.return_value = [
        _snap("SNAP_477", 100), _snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    assert [s.uuid for s in sc.parent_snapshot_delete_in_flight("lvol-33")] == ["SNAP_615"]


def test_an_older_snapshot_in_deletion_does_not_hold_a_new_one(db):
    # retention / chain collapse deletes old replication snapshots all the time
    db.get_snapshots_by_lvol_id.return_value = [
        _snap("SNAP_477", 100, SnapShot.STATUS_IN_DELETION), _snap("SNAP_615", 200)]
    assert sc.parent_snapshot_delete_in_flight("lvol-33") == []


def test_a_volume_without_snapshots_has_nothing_in_flight(db):
    db.get_snapshots_by_lvol_id.return_value = []
    assert sc.parent_snapshot_delete_in_flight("lvol-33") == []


def test_the_wait_returns_once_the_delete_has_finished(db):
    deleting = [_snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    db.get_snapshots_by_lvol_id.side_effect = [deleting, deleting, [_snap("SNAP_477", 100)]]
    with patch.object(sc.time, "sleep"):
        assert sc._wait_for_parent_snapshot_delete("lvol-33", timeout=60, poll=0) == []
    assert db.get_snapshots_by_lvol_id.call_count == 3


def test_a_zero_wait_only_looks(db):
    db.get_snapshots_by_lvol_id.return_value = [_snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    with patch.object(sc.time, "sleep", side_effect=AssertionError("must not wait")):
        pending = sc._wait_for_parent_snapshot_delete("lvol-33", timeout=0)
    assert [s.uuid for s in pending] == ["SNAP_615"]


def _add_env(db):
    lvol = LVol()
    lvol.uuid = "lvol-33"
    lvol.status = LVol.STATUS_ONLINE
    lvol.node_id = "node-1"
    lvol.pool_uuid = "pool-1"
    lvol.lvs_name = "LVS_1"
    pool = Pool()
    pool.uuid = "pool-1"
    pool.cluster_id = "cluster-1"
    pool.status = Pool.STATUS_ACTIVE
    snode = MagicMock()
    snode.lvstore_status = "ready"
    snode.lvol_sync_del.return_value = False
    db.get_lvol_by_id.return_value = lvol
    db.get_pool_by_id.return_value = pool
    db.snap_name_taken.return_value = False
    db.get_storage_node_by_id.return_value = snode
    return lvol


class _PastTheGate(Exception):
    pass


@pytest.fixture
def add_env(db):
    _add_env(db)
    with patch.object(sc.ops_gate, "assert_object_ops_allowed"), \
            patch.object(sc.migration_controller, "is_migration_active_on_node", return_value=False), \
            patch("simplyblock_core.utils.ttl_cache.cached_mini_lvols", side_effect=_PastTheGate):
        yield db


def test_add_refuses_while_the_newest_snapshot_is_being_deleted(add_env):
    add_env.get_snapshots_by_lvol_id.return_value = [
        _snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    with patch.object(sc.time, "sleep", side_effect=AssertionError("must not wait")):
        ok, err = sc.add("lvol-33", "SNAP_619", delete_wait=0)
    assert ok is False
    assert sc.SNAPSHOT_DELETE_PENDING in err and "SNAP_615" in err


def test_add_passes_the_gate_once_the_delete_has_finished(add_env):
    add_env.get_snapshots_by_lvol_id.return_value = [_snap("SNAP_615", 200)]
    with pytest.raises(_PastTheGate):
        sc.add("lvol-33", "SNAP_619", delete_wait=0)


def test_add_gates_without_the_lock_too(add_env):
    # clone_lvol snapshots its volume with lock=False
    add_env.get_snapshots_by_lvol_id.return_value = [
        _snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    ok, err = sc.add("lvol-33", "CLONE_1", lock=False, delete_wait=0)
    assert ok is False and sc.SNAPSHOT_DELETE_PENDING in err


def test_add_shares_one_budget_between_both_waits(add_env):
    add_env.get_snapshots_by_lvol_id.return_value = [_snap("SNAP_615", 200)]
    seen = {}

    def node_wait(snode, timeout=None, poll=0.5):
        seen["timeout"] = timeout
        return True

    with patch.object(sc, "_wait_for_node_sync_delete", side_effect=node_wait), \
            pytest.raises(_PastTheGate):
        sc.add("lvol-33", "SNAP_619", delete_wait=7)
    assert 0 <= seen["timeout"] <= 7


def test_a_group_snapshot_refuses_before_any_rpc():
    from simplyblock_core.controllers import consistency_group_controller as cg
    member = MagicMock()
    member.get_id.return_value = "lvol-33"
    group = MagicMock()
    pending = [_snap("SNAP_615", 200, SnapShot.STATUS_IN_DELETION)]
    with patch.object(cg, "generation_for_request", return_value=None), \
            patch.object(cg, "_precheck_members", return_value=([member], None)), \
            patch.object(cg, "_wait_for_parent_snapshot_delete", return_value=pending) as wait, \
            patch.object(cg, "db") as cgdb:
        seq, err = cg.create_group_snapshot_for_group(group, delete_wait=0)
    assert seq is None and cg.SNAPSHOT_DELETE_PENDING in err
    wait.assert_called_once()
    cgdb.get_storage_node_by_id.assert_not_called()


def test_migration_retries_an_intermediate_snapshot_held_by_a_delete():
    from simplyblock_core.services import tasks_runner_lvol_migration as mig
    migration = MagicMock()
    migration.uuid = "mig-12345678"
    migration.intermediate_snap_rounds = 1
    migration.max_intermediate_snap_rounds = 4
    with patch.object(mig.snapshot_controller, "add",
                      return_value=(False, f"{sc.SNAPSHOT_DELETE_PENDING}: SNAP_615 of lvol-33")):
        assert mig._take_intermediate_snapshot(migration) == mig._SNAP_BUSY
    # the rounds stay open
    assert migration.intermediate_snap_rounds == 1
    migration.write_to_db.assert_not_called()


def test_the_snapshot_monitor_never_waits_on_its_own_deletes():
    from simplyblock_core.services import snapshot_monitor
    src = inspect.getsource(snapshot_monitor.take_due_internal_snapshots)
    assert "snap_type=SnapShot.TYPE_INTERNAL, delete_wait=0)" in src
    assert "create_group_snapshot_for_group(\n                group, delete_wait=0)" in src.replace("\r\n", "\n")
