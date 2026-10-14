"""Consistency-group co-location (docs/consistency-group-colocation.md):
migration scope, the whole-group migration guard, the group migration's
all-or-nothing pre-create, the re-pin after a group moved, the group's
subsystem at create time, the flip-out victim, and the late-join plan."""
from unittest import mock

import pytest

from simplyblock_core.controllers import cg_colocation as cgl
from simplyblock_core.controllers import lvol_controller, migration_controller
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.rpc_client import RPCRemoteError


def _lv(uid, nqn, node="N1", lvs="LVS_1", pool="P1", maxns=4, nsid=1, size=10,
        group="", status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uid
    lv.nqn = nqn
    lv.node_id = node
    lv.lvs_name = lvs
    lv.pool_uuid = pool
    lv.max_namespace_per_subsys = maxns
    lv.ns_id = nsid
    lv.size = size
    lv.group_id = group
    lv.status = status
    return lv


def _group(gid, members, node="N1", lvs="LVS_1", removed=()):
    g = ConsistencyGroup()
    g.uuid = gid
    g.cluster_id = "CL"
    g.group_name = gid
    g.node_id = node
    g.lvs_name = lvs
    g.members = {m: {"joined_seq": 1, "removed_seq": 3 if m in removed else 0} for m in members}
    return g


# --------------------------------------------------------------------------- #
# Migration scope
# --------------------------------------------------------------------------- #

def test_scope_of_a_group_spanning_two_subsystems_is_both_subsystems():
    # Group g has a1 (subsystem A) and b1 (subsystem B); A also holds x (no
    # group). Moving a1 must move b1 (same group) and x (same subsystem).
    lvols = [_lv("a1", "A", group="g"), _lv("x", "A", nsid=2),
             _lv("b1", "B", group="g"), _lv("other", "C")]
    groups = [_group("g", ["a1", "b1"])]
    assert cgl.migration_scope(["a1"], lvols, groups) == ["a1", "b1", "x"]
    assert cgl.scope_by_subsystem(["a1", "b1", "x"], lvols) == {"A": ["a1", "x"], "B": ["b1"]}


def test_scope_is_transitive_across_groups_sharing_a_subsystem():
    # g1 = {a1, b1}; subsystem B also holds c1 of group g2 = {c1, d1} in D.
    lvols = [_lv("a1", "A", group="g1"), _lv("b1", "B", group="g1"),
             _lv("c1", "B", nsid=2, group="g2"), _lv("d1", "D", group="g2")]
    groups = [_group("g1", ["a1", "b1"]), _group("g2", ["c1", "d1"])]
    assert cgl.migration_scope(["a1"], lvols, groups) == ["a1", "b1", "c1", "d1"]


def test_scope_ignores_departed_members_and_deleted_volumes():
    lvols = [_lv("a1", "A", group="g"), _lv("gone", "G", group="g"),
             _lv("dead", "A", nsid=2, status=LVol.STATUS_DELETED)]
    groups = [_group("g", ["a1", "gone"], removed=("gone",))]
    assert cgl.migration_scope(["a1"], lvols, groups) == ["a1"]


def test_standalone_subsystems_with_the_same_nqn_root_are_not_siblings():
    # max_namespace_per_subsys == 1: a subsystem that cannot share is never a
    # batch, even when two records carry the same NQN (mirrors the batch rule).
    lvols = [_lv("a", "A", maxns=1), _lv("b", "A", maxns=1, nsid=2)]
    assert cgl.migration_scope(["a"], lvols, []) == ["a"]


def test_uncovered_members_name_what_a_partial_migration_leaves_behind():
    lvols = [_lv("a1", "A", group="g"), _lv("b1", "B", group="g")]
    groups = [_group("g", ["a1", "b1"])]
    assert cgl.uncovered_group_members(["a1"], lvols, groups) == ["b1"]
    assert cgl.uncovered_group_members(["a1", "b1"], lvols, groups) == []


def test_single_migration_of_a_group_member_is_refused():
    lvols = [_lv("a1", "A", group="g"), _lv("b1", "B", group="g")]
    with mock.patch.object(cgl, "_cluster_records", return_value=(lvols, [_group("g", ["a1", "b1"])])):
        with pytest.raises(ValueError, match="split a consistency group.*b1"):
            cgl.require_whole_groups(["a1"], "CL")
        cgl.require_whole_groups(["a1", "b1"], "CL")


# --------------------------------------------------------------------------- #
# Group migration: all or nothing
# --------------------------------------------------------------------------- #

def _wire_group_migration(lvols, scope_by_nqn):
    db = mock.MagicMock()
    by_id = {lv.get_id(): lv for lv in lvols}
    db.get_lvol_by_id.side_effect = lambda i: by_id[i]
    node = mock.MagicMock()
    node.cluster_id = "CL"
    db.get_storage_node_by_id.return_value = node
    scope = sorted(i for ids in scope_by_nqn.values() for i in ids)
    return db, scope


def test_group_migration_creates_one_migration_per_subsystem():
    lvols = [_lv("a1", "A", group="g"), _lv("x", "A", nsid=2), _lv("b1", "B", group="g", maxns=1)]
    by_nqn = {"A": ["a1", "x"], "B": ["b1"]}
    db, scope = _wire_group_migration(lvols, by_nqn)
    group = _group("g", ["a1", "b1"])
    group.write_to_db = mock.MagicMock()
    with mock.patch.object(migration_controller, "db", db), \
            mock.patch.object(cgl, "scope_for", return_value=(scope, by_nqn)), \
            mock.patch.object(migration_controller, "consistency_group_for",
                              side_effect=lambda i: group if i in ("a1", "b1") else None), \
            mock.patch.object(migration_controller, "_get_shared_subsystem_members",
                              side_effect=lambda lv, c: [x for x in lvols if x.nqn == lv.nqn and x.max_namespace_per_subsys > 1]), \
            mock.patch.object(migration_controller, "create_batch_migration",
                              return_value=("BATCH-A", [{"nqn": "A"}])) as batch, \
            mock.patch.object(migration_controller, "create_migration",
                              return_value=("MIG-B", [{"nqn": "B"}])) as single:
        out = migration_controller.create_group_migration("a1", "N2")
    assert [(it["nqn"], it["kind"], it["id"]) for it in out["items"]] == [
        ("A", "batch", "BATCH-A"), ("B", "single", "MIG-B")]
    assert batch.call_args.kwargs["group_scope"] is True
    assert single.call_args.kwargs["group_scope"] is True
    assert group.migration["target_node_id"] == "N2"
    assert "connect_strings" not in group.migration["items"][0]


def test_group_migration_cancels_what_it_created_when_one_subsystem_fails():
    lvols = [_lv("a1", "A", group="g", maxns=1), _lv("b1", "B", group="g", maxns=1)]
    by_nqn = {"A": ["a1"], "B": ["b1"]}
    db, scope = _wire_group_migration(lvols, by_nqn)
    group = _group("g", ["a1", "b1"])
    with mock.patch.object(migration_controller, "db", db), \
            mock.patch.object(cgl, "scope_for", return_value=(scope, by_nqn)), \
            mock.patch.object(migration_controller, "consistency_group_for", return_value=group), \
            mock.patch.object(migration_controller, "_get_shared_subsystem_members", return_value=[]), \
            mock.patch.object(migration_controller, "create_migration",
                              side_effect=[("MIG-A", []), ValueError("no lvstore")]), \
            mock.patch.object(migration_controller, "cancel_migration") as cancel:
        with pytest.raises(ValueError, match="no lvstore"):
            migration_controller.create_group_migration("a1", "N2")
    cancel.assert_called_once_with("MIG-A")
    assert not group.migration


def test_a_second_group_migration_is_refused_while_one_is_active():
    lvols = [_lv("a1", "A", group="g", maxns=1)]
    db, scope = _wire_group_migration(lvols, {"A": ["a1"]})
    group = _group("g", ["a1"])
    group.migration = {"target_node_id": "N9", "items": [{"id": "M"}]}
    with mock.patch.object(migration_controller, "db", db), \
            mock.patch.object(cgl, "scope_for", return_value=(scope, {"A": ["a1"]})), \
            mock.patch.object(migration_controller, "consistency_group_for", return_value=group):
        with pytest.raises(migration_controller.MigrationConflictError):
            migration_controller.create_group_migration("a1", "N2")


# --------------------------------------------------------------------------- #
# Re-pin
# --------------------------------------------------------------------------- #

def test_pin_follows_once_every_member_arrived():
    g = _group("g", ["a1", "b1"], node="N1", lvs="LVS_1")
    half = [_lv("a1", "A", node="N2", lvs="LVS_2"), _lv("b1", "B", node="N1", lvs="LVS_1")]
    assert cgl.repin_target(g, half) is None
    both = [_lv("a1", "A", node="N2", lvs="LVS_2"), _lv("b1", "B", node="N2", lvs="LVS_2")]
    assert cgl.repin_target(g, both) == ("N2", "LVS_2")
    g.node_id, g.lvs_name = "N2", "LVS_2"
    assert cgl.repin_target(g, both) is None


# --------------------------------------------------------------------------- #
# Create time: the group's subsystem, and the flip-out victim
# --------------------------------------------------------------------------- #

def test_group_subsystem_is_the_one_holding_most_members_on_the_pin():
    g = _group("g", ["a1", "a2", "b1"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("b1", "B")]
    assert cgl.group_subsystem_nqn(g, lvols) == "A"
    assert cgl.group_subsystem_nqn(_group("h", ["s"]), [_lv("s", "S", maxns=1)]) == ""


def test_the_claim_prefers_the_group_subsystem_over_the_fullest_one():
    # Fill order would pick F (3 of 4 used); the group's subsystem G (1 of 4)
    # wins when named.
    lvols = [_lv("f1", "F"), _lv("f2", "F", nsid=2), _lv("f3", "F", nsid=3), _lv("g1", "G")]
    pick = lvol_controller.get_next_available_subsystem_on_node("N1", lvols, pool_id="P1")
    assert pick is not None and pick.nqn == "F"
    pick = lvol_controller.get_next_available_subsystem_on_node("N1", lvols, pool_id="P1", prefer_nqn="G")
    assert pick is not None and pick.nqn == "G"


def test_a_full_group_subsystem_falls_back_to_the_ordinary_pick():
    lvols = [_lv(f"g{i}", "G", nsid=i) for i in range(1, 5)] + [_lv("f1", "F")]
    pick = lvol_controller.get_next_available_subsystem_on_node("N1", lvols, pool_id="P1", prefer_nqn="G")
    assert pick is not None and pick.nqn == "F"


def test_flip_victim_is_never_a_group_member_and_prefers_unattached_then_small():
    sub = [_lv("m", "G", group="g"), _lv("big", "G", nsid=2, size=100),
           _lv("small", "G", nsid=3, size=5), _lv("other", "G", nsid=4, group="h")]
    v = cgl.choose_flip_victim(sub)
    assert (v.lvol_id, v.attached) == ("small", False)
    v = cgl.choose_flip_victim(sub, attached_ids=["small"])
    assert v.lvol_id == "big"
    assert cgl.choose_flip_victim(sub, migrating_ids=["big", "small"]) is None
    assert cgl.choose_flip_victim([_lv("m", "G", group="g")]) is None


# --------------------------------------------------------------------------- #
# Late join
# --------------------------------------------------------------------------- #

def test_late_join_on_the_pin_joins_and_colocates():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("v", "V")]
    plan = cgl.plan_late_join(g, lvols[2], lvols, [g])
    assert plan.steps == [cgl.STEP_JOIN, cgl.STEP_COLOCATE]
    assert plan.target_nqn == "A"


def test_late_join_off_the_pin_migrates_first_with_its_subsystem_siblings():
    g = _group("g", ["a1"], node="N1")
    lvols = [_lv("a1", "A", maxns=1), _lv("v", "V", node="N2", lvs="LVS_2"),
             _lv("sib", "V", node="N2", lvs="LVS_2", nsid=2)]
    plan = cgl.plan_late_join(g, lvols[1], lvols, [g])
    assert plan.steps == [cgl.STEP_MIGRATE, cgl.STEP_JOIN]
    assert plan.target_node_id == "N1"
    assert plan.migrate_ids == ["sib", "v"]


def test_late_join_refuses_when_a_sibling_belongs_to_another_group():
    g = _group("g", ["a1"], node="N1")
    h = _group("h", ["sib"], node="N2", lvs="LVS_2")
    lvols = [_lv("a1", "A"), _lv("v", "V", node="N2", lvs="LVS_2"),
             _lv("sib", "V", node="N2", lvs="LVS_2", nsid=2, group="h")]
    with pytest.raises(cgl.ColocationError, match="another consistency group"):
        cgl.plan_late_join(g, lvols[1], lvols, [g, h])


def test_late_join_refuses_another_pool_and_another_group():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A"), _lv("v", "V", pool="P2"), _lv("w", "W", group="h")]
    with pytest.raises(cgl.ColocationError, match="pool"):
        cgl.plan_late_join(g, lvols[1], lvols, [g])
    with pytest.raises(cgl.ColocationError, match="member of consistency group h"):
        cgl.plan_late_join(g, lvols[2], lvols, [g])


def test_late_join_into_an_empty_group_just_joins():
    g = _group("g", [], node="", lvs="")
    v = _lv("v", "V", node="N7")
    assert cgl.plan_late_join(g, v, [v], [g]).steps == [cgl.STEP_JOIN]


# --------------------------------------------------------------------------- #
# Namespace moves are off until the client swap exists
# --------------------------------------------------------------------------- #

def test_namespace_moves_are_refused_while_disabled():
    assert cgl.NAMESPACE_MOVES_ENABLED is False
    with pytest.raises(cgl.ColocationError, match="disabled"):
        cgl.move_namespace(_lv("v", "V"), "G")
    assert cgl.colocate_new_member(_group("g", ["a"]), _lv("v", "V"), "G").startswith("deferred")


def test_an_attached_volume_is_not_moved_without_the_client_swap():
    with mock.patch.object(cgl, "NAMESPACE_MOVES_ENABLED", True), \
            mock.patch.object(cgl, "subsystem_has_hosts", return_value=True):
        with pytest.raises(cgl.ColocationError, match="device-mapper swap"):
            cgl.move_namespace(_lv("v", "V"), "G")


def _move_fixture(add_err_on_second=False):
    lv = _lv("v", "V", nsid=3)
    lv.nodes = ["N1", "N2"]
    lv.top_bdev = "LVS_1/LVOL_v"
    lv.guid = "GUID"
    nodes, rpcs = [], []
    for i, nid in enumerate(("N1", "N2")):
        node = mock.MagicMock()
        node.get_id.return_value = nid
        node.cluster_id = "CL"
        rpc = mock.MagicMock()
        rpc.subsystem_get.return_value = {"namespaces": [{"nsid": 1}, {"nsid": 2}]}
        err = "boom" if (add_err_on_second and i == 1) else None
        if err:
            rpc.nvmf_subsystem_add_ns2.side_effect = RPCRemoteError(err, -1)
        else:
            rpc.nvmf_subsystem_add_ns2.return_value = 3
        node.rpc_client.return_value = rpc
        nodes.append(node)
        rpcs.append(rpc)
    db = mock.MagicMock()
    db.get_storage_node_by_id.side_effect = lambda n: nodes[["N1", "N2"].index(n)]
    root = _lv("g1", "G")
    root.namespace = "ROOT"
    db.get_lvols.return_value = [root, lv]
    return lv, rpcs, db


def test_namespace_move_uses_one_nsid_on_every_node_and_switches_the_record():
    lv, rpcs, db = _move_fixture()
    lv.write_to_db = mock.MagicMock()
    with mock.patch.object(cgl, "NAMESPACE_MOVES_ENABLED", True), \
            mock.patch.object(cgl, "subsystem_has_hosts", return_value=False), \
            mock.patch.object(cgl, "_db", return_value=db), \
            mock.patch.object(lvol_controller, "_remove_lvol_subsys_from_node", return_value=True) as rm:
        cgl.move_namespace(lv, "G")
    assert rm.call_count == 2
    for rpc in rpcs:
        args, kwargs = rpc.nvmf_subsystem_add_ns2.call_args
        assert args[0] == "G" and kwargs["nsid"] == 3   # first free on every node
    assert (lv.nqn, lv.ns_id, lv.namespace) == ("G", 3, "ROOT")


def test_namespace_move_puts_the_namespace_back_when_an_add_fails():
    lv, rpcs, db = _move_fixture(add_err_on_second=True)
    lv.write_to_db = mock.MagicMock()
    with mock.patch.object(cgl, "NAMESPACE_MOVES_ENABLED", True), \
            mock.patch.object(cgl, "subsystem_has_hosts", return_value=False), \
            mock.patch.object(cgl, "_db", return_value=db), \
            mock.patch.object(lvol_controller, "_remove_lvol_subsys_from_node", return_value=True):
        with pytest.raises(cgl.ColocationError, match="adding v to G"):
            cgl.move_namespace(lv, "G")
    rpcs[0].nvmf_subsystem_remove_ns.assert_called_once_with("G", 3)
    for rpc in rpcs:
        restored = [c for c in rpc.nvmf_subsystem_add_ns2.call_args_list if c.args[0] == "V"]
        assert restored and restored[-1].kwargs["nsid"] == 3
    assert lv.nqn == "V"
    lv.write_to_db.assert_not_called()


def test_create_migration_refuses_a_lone_group_member_before_touching_the_target():
    lv = _lv("a1", "A", group="g", maxns=1)
    tgt = mock.MagicMock()
    tgt.lvstore = "LVS_2"
    tgt.cluster_id = "CL"
    db = mock.MagicMock()
    db.get_lvol_by_id.return_value = lv
    db.get_storage_node_by_id.return_value = tgt
    # main's _refuse_restarting_target_replicas reads the replicas through
    # the module-level db: serve it from the same mock (no replicas).
    tgt.secondary_node_id = ""
    tgt.tertiary_node_id = ""
    with mock.patch.object(migration_controller, "DBController", return_value=db), \
            mock.patch.object(migration_controller, "db", db), \
            mock.patch.object(migration_controller, "_get_shared_subsystem_members", return_value=[]), \
            mock.patch.object(cgl, "require_whole_groups",
                              side_effect=ValueError("would split a consistency group")):
        with pytest.raises(ValueError, match="split a consistency group"):
            migration_controller.create_migration("a1", "N2")
    tgt.rpc_client.assert_not_called()
