"""Consistency-group subsystem moves (design-cg-live-subsystem-flip.md §9, in
simplyblock-manager), against real FDB.

The move resource is a sequence of DB-recorded steps, each acting on every
node of the group's HA set: create reserves the target against namespace-slot
claims in the transaction that records the move; prepare creates a fresh target
and assigns nsids; move switches the namespaces while the hosts' suspend
deadline holds; commit switches the records; rollback restores the sources.
The storage nodes are faked (a namespace table per node), the database is real.
"""
import time
from unittest import mock

import pytest

from simplyblock_core.controllers import cg_colocation as cgl
from simplyblock_core.controllers import cg_subsystem_move as mv
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.controllers import migration_controller as mc
from simplyblock_core.db_controller import (
    DBController,
    SubsystemCapacityError,
    SubsystemMoveConflictError,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol, LVolMini
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

CLUSTER = "cgmv-cluster"
POOL = "cgmv-pool"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


class _FakeNode:
    """One storage node's subsystems, as nqn -> {nsid: namespace uuid}."""

    def __init__(self, subsystems):
        self.subsystems = subsystems
        self.fail_add_to = ""
        self.rpc = mock.MagicMock()
        self.rpc.subsystem_get.side_effect = self._get
        self.rpc.nvmf_subsystem_remove_ns.side_effect = self._remove
        self.rpc.nvmf_subsystem_add_ns2.side_effect = self._add
        self.rpc.subsystem_create.side_effect = self._create
        self.rpc.subsystem_delete.side_effect = self._delete

    def _get(self, nqn):
        if nqn not in self.subsystems:
            return None
        return {"nqn": nqn, "namespaces": [{"nsid": n, "uuid": u}
                                           for n, u in sorted(self.subsystems[nqn].items())]}

    def _remove(self, nqn, nsid):
        self.subsystems[nqn].pop(nsid, None)
        return True

    def _add(self, nqn, bdev, uuid, nguid, nsid=None, **kw):
        if nqn == self.fail_add_to:
            raise RPCRemoteError("add failed", -1)
        self.subsystems.setdefault(nqn, {})[nsid] = uuid
        return nsid

    def _create(self, nqn, *a, **k):
        self.subsystems.setdefault(nqn, {})
        return True

    def _delete(self, nqn):
        self.subsystems.pop(nqn, None)
        return True


def _write_node(db, nid, max_lvol=10):
    node = StorageNode()
    node.uuid = nid
    node.cluster_id = CLUSTER
    node.max_lvol = max_lvol
    node.write_to_db(db.kv_store)
    return node


def _write_lvol(db, uid, nqn, nsid=1, maxns=2, node="N1", group=""):
    lv = LVol()
    lv.uuid = uid
    lv.lvol_name = f"VOL_{uid}"
    lv.pool_uuid = POOL
    lv.node_id = node
    lv.nodes = [node, "S1"]
    lv.lvs_name = "LVS_1"
    lv.nqn = nqn
    lv.ns_id = nsid
    lv.max_namespace_per_subsys = maxns
    lv.top_bdev = f"LVS_1/LVOL_{uid}"
    lv.guid = f"GUID{uid}"
    lv.group_id = group
    lv.status = LVol.STATUS_ONLINE
    lv.write_to_db(db.kv_store)
    return lv


def _write_group(db, members, node="N1"):
    g = ConsistencyGroup()
    g.uuid = "cgmv-g"
    g.cluster_id = CLUSTER
    g.group_name = "cgmv-g"
    g.node_id = node
    g.lvs_name = "LVS_1"
    g.members = {m: {"joined_seq": 1, "removed_seq": 0} for m in members}
    g.write_to_db(db.kv_store)
    return db.get_consistency_group_by_id(g.get_id())


@pytest.fixture
def world(db):
    """Group {a1, a2} in a full 2-slot subsystem A on N1 (secondary S1); the
    joiner j, already a member, alone in its standalone subsystem J. No
    candidate fits the group, so a move goes to a fresh subsystem."""
    cluster = Cluster()
    cluster.uuid = CLUSTER
    cluster.nqn = "nqn.cgmv"
    cluster.write_to_db(db.kv_store)
    pool = Pool()
    pool.uuid = POOL
    pool.cluster_id = CLUSTER
    pool.pool_name = "cgmv-pool"
    pool.write_to_db(db.kv_store)
    _write_node(db, "N1")
    _write_node(db, "S1")
    gid = f"{CLUSTER}/cgmv-g"
    lvols = [_write_lvol(db, "a1", "A", 1, group=gid), _write_lvol(db, "a2", "A", 2, group=gid),
             _write_lvol(db, "j", "J", 1, maxns=1, group=gid)]
    group = _write_group(db, ["a1", "a2", "j"])
    fakes = {nid: _FakeNode({"A": {1: "a1", 2: "a2"}, "J": {1: "j"}}) for nid in ("N1", "S1")}
    with mock.patch.object(StorageNode, "rpc_client",
                           lambda self, *a, **k: fakes[self.get_id()].rpc), \
            mock.patch.object(mv, "_publish_listeners", return_value=None), \
            mock.patch.object(mv, "_connect_entries", return_value=[{"nqn": "T"}]):
        yield group, lvols, fakes


def _nqns(db, ids=("a1", "a2", "j")):
    return [(db.get_lvol_by_id(i).nqn, db.get_lvol_by_id(i).ns_id) for i in ids]


# --------------------------------------------------------------------------- #
# Create and the reservation
# --------------------------------------------------------------------------- #

def test_create_selects_and_reserves_a_fresh_target(db, world):
    group, _, _ = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    assert rec["target_is_new"] and rec["target_nqn"].startswith("nqn.cgmv:lvol:")
    assert [v["lvol_id"] for v in rec["volumes"]] == ["a1", "a2", "j"]
    assert rec["namespaces_at"] == "source"
    assert db.subsystem_move_reservations("N1") == ({"A", "J", rec["target_nqn"]}, 1)
    assert db.get_consistency_group_by_id(group.get_id()).subsystem_move["id"] == rec["id"]


def test_create_is_idempotent_on_request_id(db, world):
    group, _, _ = world
    first = mv.create_subsystem_move(group, "j", "req-1")
    assert mv.create_subsystem_move(group, "j", "req-1") == first
    with pytest.raises(mv.MoveConflictError):
        mv.create_subsystem_move(group, "j", "req-2")


def test_create_refuses_while_a_group_migration_is_active(db, world):
    group, _, _ = world
    group.migration = {"target_node_id": "N2", "items": []}
    group.write_to_db(db.kv_store)
    with pytest.raises(mv.MoveConflictError, match="migration"):
        mv.create_subsystem_move(group, "j", "req-1")


def test_create_refuses_a_volume_that_is_not_a_member(db, world):
    group, _, _ = world
    group.members.pop("j")
    group.write_to_db(db.kv_store)
    with pytest.raises(cgl.ColocationError, match="not a member"):
        mv.create_subsystem_move(group, "j", "req-1")


def test_create_refuses_on_a_stretched_cluster(db, world):
    group, _, _ = world
    with mock.patch.object(mv, "_is_stretched", return_value=True):
        with pytest.raises(cgl.ColocationError, match="stretched"):
            mv.create_subsystem_move(group, "j", "req-1")
    assert db.subsystem_move_reservations("N1") == (set(), 0)


def test_create_at_the_node_limit_is_refused_and_reserves_nothing(db, world):
    group, _, _ = world
    _write_node(db, "N1", max_lvol=2)
    with pytest.raises(cgl.SubsystemLimitReached):
        mv.create_subsystem_move(group, "j", "req-1")
    assert db.subsystem_move_reservations("N1") == (set(), 0)


def test_create_with_nothing_to_move_reserves_nothing(db, world):
    group, _, _ = world
    gid = group.get_id()
    _write_lvol(db, "a2", "B", 1, maxns=1)              # a2 leaves A: A has room
    group.members.pop("a2")
    group.write_to_db(db.kv_store)
    _write_lvol(db, "j", "A", 2, group=gid)             # j already in A
    rec = mv.create_subsystem_move(group, "j", "req-1")
    assert rec["volumes"] == [] and rec["id"] == ""
    assert db.subsystem_move_reservations("N1") == (set(), 0)


def _new_lvol(uid, maxns=4):
    new = LVol()
    new.uuid = uid
    new.lvol_name = f"VOL_{uid}"
    new.pool_uuid = POOL
    new.node_id = "N1"
    new.status = LVol.STATUS_IN_CREATION
    new.max_namespace_per_subsys = maxns
    return new


def test_a_claim_never_joins_a_subsystem_held_by_a_move(db, world):
    group, _, _ = world
    _write_lvol(db, "x", "X", 1, maxns=8)               # room for the whole group
    rec = mv.create_subsystem_move(group, "j", "req-1")
    assert rec["target_nqn"] == "X" and not rec["target_is_new"]
    node = db.get_storage_node_by_id("N1")
    held = _new_lvol("n1")
    assert db.claim_lvol_ns_slot(held, node, True, "nqn.cgmv:lvol:n1") is False
    assert held.nqn == "nqn.cgmv:lvol:n1"
    db.release_subsystem_move(group, "N1", rec["id"], "rolled_back")
    free = _new_lvol("n2")
    assert db.claim_lvol_ns_slot(free, node, True, "nqn.cgmv:lvol:n2") is True
    assert free.nqn == "X"


def test_a_reserved_fresh_subsystem_counts_against_the_cap(db, world):
    group, _, _ = world
    _write_node(db, "N1", max_lvol=3)                    # A and J held: one spare
    mv.create_subsystem_move(group, "j", "req-1")        # the move reserves it
    new = LVol()
    new.uuid = "n1"
    new.lvol_name = "VOL_n1"
    new.pool_uuid = POOL
    new.node_id = "N1"
    new.status = LVol.STATUS_IN_CREATION
    new.max_namespace_per_subsys = 1
    with pytest.raises(SubsystemCapacityError):
        db.claim_lvol_ns_slot(new, db.get_storage_node_by_id("N1"), False, "nqn.cgmv:lvol:n1")


def test_a_second_reservation_on_the_group_is_refused(db, world):
    group, _, _ = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    with pytest.raises(SubsystemMoveConflictError):
        db.reserve_subsystem_move(group, "N1", dict(rec, id="m2", request_id="r2"), {"Q"}, 0)


# --------------------------------------------------------------------------- #
# Prepare, move, commit, rollback
# --------------------------------------------------------------------------- #

def test_prepare_creates_the_fresh_target_on_every_ha_node_and_assigns_one_nsid_each(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    out, entries = mv.prepare_subsystem_move(group, rec["id"], host_nqn="host-1")
    for f in fakes.values():
        assert rec["target_nqn"] in f.subsystems
    assert [v["target_nsid"] for v in out["volumes"]] == [1, 2, 3]
    assert entries == [{"nqn": "T"}]
    again, _ = mv.prepare_subsystem_move(group, rec["id"])
    assert again["volumes"] == out["volumes"]
    assert fakes["N1"].rpc.subsystem_create.call_count == 1


def test_move_refuses_after_the_deadline_without_touching_a_node(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    mv.prepare_subsystem_move(group, rec["id"])
    with pytest.raises(mv.DeadlinePassedError):
        mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) - 1)
    for f in fakes.values():
        f.rpc.nvmf_subsystem_remove_ns.assert_not_called()


def test_move_moves_every_namespace_and_a_repeat_touches_nothing(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    mv.prepare_subsystem_move(group, rec["id"])
    out = mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) + 30)
    assert out["namespaces_at"] == "target"
    for f in fakes.values():
        assert f.subsystems[rec["target_nqn"]] == {1: "a1", 2: "a2", 3: "j"}
        assert f.subsystems["A"] == {} and f.subsystems["J"] == {}
    calls = fakes["N1"].rpc.nvmf_subsystem_remove_ns.call_count
    again = mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) - 5)
    assert again["namespaces_at"] == "target"
    assert fakes["N1"].rpc.nvmf_subsystem_remove_ns.call_count == calls


def test_a_partial_move_restores_the_sources_and_reports_source(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    mv.prepare_subsystem_move(group, rec["id"])
    fakes["S1"].fail_add_to = rec["target_nqn"]
    with pytest.raises(mv.MoveFailedError) as e:
        mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) + 30)
    assert e.value.namespaces_at == "source"
    for f in fakes.values():
        assert f.subsystems["A"] == {1: "a1", 2: "a2"} and f.subsystems["J"] == {1: "j"}
        assert f.subsystems.get(rec["target_nqn"], {}) == {}
    assert db.get_consistency_group_by_id(group.get_id()).subsystem_move["namespaces_at"] == "source"


def test_commit_switches_every_record_and_deletes_emptied_sources(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    mv.prepare_subsystem_move(group, rec["id"])
    mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) + 30)
    mv.commit_subsystem_move(group, rec["id"])
    t = rec["target_nqn"]
    assert _nqns(db) == [(t, 1), (t, 2), (t, 3)]
    assert {db.get_lvol_by_id(i).max_namespace_per_subsys for i in ("a1", "a2", "j")} == {50}
    assert LVolMini().read_from_db(db.kv_store, id="j")[0].nqn == t
    for f in fakes.values():
        assert "A" not in f.subsystems and "J" not in f.subsystems
    fresh = db.get_consistency_group_by_id(group.get_id())
    assert fresh.subsystem_move == {}
    assert fresh.last_subsystem_move == {"id": rec["id"], "outcome": "committed"}
    assert db.subsystem_move_reservations("N1") == (set(), 0)
    mv.commit_subsystem_move(group, rec["id"])          # retried: a no-op
    assert mv.get_subsystem_move(group, rec["id"])["status"] == "committed"


def test_commit_before_the_namespaces_moved_is_refused(db, world):
    group, _, _ = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    with pytest.raises(mv.MoveConflictError, match="not on the target"):
        mv.commit_subsystem_move(group, rec["id"])


def test_rollback_puts_namespaces_back_and_deletes_an_empty_fresh_target(db, world):
    group, _, fakes = world
    rec = mv.create_subsystem_move(group, "j", "req-1")
    mv.prepare_subsystem_move(group, rec["id"])
    mv.move_subsystem_move(group, rec["id"], deadline=int(time.time()) + 30)
    mv.rollback_subsystem_move(group, rec["id"])
    for f in fakes.values():
        assert f.subsystems["A"] == {1: "a1", 2: "a2"} and f.subsystems["J"] == {1: "j"}
        assert rec["target_nqn"] not in f.subsystems
    assert _nqns(db) == [("A", 1), ("A", 2), ("J", 1)]
    assert db.subsystem_move_reservations("N1") == (set(), 0)
    mv.rollback_subsystem_move(group, rec["id"])        # retried: a no-op
    assert mv.get_subsystem_move(group, rec["id"])["status"] == "rolled_back"


def test_an_unknown_move_id_is_not_found(db, world):
    group, _, _ = world
    with pytest.raises(KeyError):
        mv.get_subsystem_move(group, "nope")


# --------------------------------------------------------------------------- #
# The join plan's preflight, and the guards while a move is active
# --------------------------------------------------------------------------- #

def test_the_join_plan_carries_the_colocation_refusal(db, world):
    group, _, _ = world
    _write_node(db, "N1", max_lvol=2)
    group.members.pop("j")
    group.members.pop("a2")
    group.write_to_db(db.kv_store)
    _write_lvol(db, "j", "J", 1, maxns=1)
    plan = cgl.late_join_plan(db.get_consistency_group_by_id(group.get_id()),
                              db.get_lvol_by_id("j"))
    assert plan.colocation == {"target_nqn": "", "target_is_new": False,
                               "moving_lvol_ids": [],
                               "refusal": "SubsystemLimitReached: node N1 holds its "
                                          "maximum of 2 subsystems"}


def test_the_join_plan_names_the_fresh_target_it_would_need(db, world):
    group, _, _ = world
    group.members.pop("j")
    group.write_to_db(db.kv_store)
    _write_lvol(db, "j", "J", 1, maxns=1)
    plan = cgl.late_join_plan(db.get_consistency_group_by_id(group.get_id()),
                              db.get_lvol_by_id("j"))
    assert plan.colocation == {"target_nqn": "", "target_is_new": True,
                               "moving_lvol_ids": ["a1", "a2", "j"], "refusal": ""}
    assert db.subsystem_move_reservations("N1") == (set(), 0)


def test_a_group_migration_is_refused_during_a_move(db, world):
    group, _, _ = world
    _write_node(db, "N2")
    mv.create_subsystem_move(group, "j", "req-1")
    with pytest.raises(mc.MigrationConflictError, match="subsystem move"):
        mc.create_group_migration("a1", "N2")


def test_adding_a_namespace_to_a_subsystem_in_a_move_is_refused(db, world):
    group, _, _ = world
    mv.create_subsystem_move(group, "j", "req-1")
    lv = _write_lvol(db, "n1", "A", 3)
    lv.namespace = "a1"
    with mock.patch.object(lvol_controller, "_create_bdev_stack") as bdev:
        ok, msg = lvol_controller.add_lvol_on_node(lv, db.get_storage_node_by_id("N1"))
    assert ok is False and "subsystem move" in msg
    bdev.assert_not_called()
