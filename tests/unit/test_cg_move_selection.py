"""The subsystem a consistency group ends up in when a volume joins it
(design-cg-live-subsystem-flip.md §5, in simplyblock-manager): the group's
subsystem while it has room, otherwise the whole group into the candidate with
the fewest moves, otherwise a fresh subsystem, refused only at the node's
subsystem limit. Pure: records in, decision out, no database."""
import pytest

from simplyblock_core.controllers import cg_colocation as cgl
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import ConsistencyGroup


def _lv(uid, nqn, node="N1", lvs="LVS_1", pool="P1", maxns=4, nsid=1,
        group="", status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uid
    lv.nqn = nqn
    lv.node_id = node
    lv.lvs_name = lvs
    lv.pool_uuid = pool
    lv.max_namespace_per_subsys = maxns
    lv.ns_id = nsid
    lv.group_id = group
    lv.status = status
    return lv


def _group(gid, members, node="N1", lvs="LVS_1"):
    g = ConsistencyGroup()
    g.uuid = gid
    g.cluster_id = "CL"
    g.group_name = gid
    g.node_id = node
    g.lvs_name = lvs
    g.members = {m: {"joined_seq": 1, "removed_seq": 0} for m in members}
    return g


def _select(group, joiner, lvols, limit=10, **kw):
    return cgl.select_move_target(group, joiner, lvols, node_max_subsystems=limit, **kw)


def test_a_snapshot_precheck_refuses_while_a_subsystem_move_is_active():
    from simplyblock_core.controllers import consistency_group_controller as cgc
    g = _group("g", ["a1"])
    g.subsystem_move = {"id": "M", "target_nqn": "T"}
    members, err = cgc._precheck_members(g)
    assert members is None and "subsystem move" in err


# --------------------------------------------------------------------------- #
# Selection (design §5)
# --------------------------------------------------------------------------- #

def test_a_group_subsystem_with_a_free_slot_takes_only_the_joiner():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("j", "J", maxns=1)]
    t = _select(g, lvols[2], lvols)
    assert (t.target_nqn, t.target_is_new, t.moving_ids) == ("A", False, ["j"])


def test_exactly_one_free_slot_still_takes_only_the_joiner():
    g = _group("g", ["a1", "a2", "a3"])
    lvols = [_lv(f"a{i}", "A", nsid=i) for i in (1, 2, 3)] + [_lv("j", "J", maxns=1)]
    t = _select(g, lvols[3], lvols)
    assert (t.target_nqn, t.moving_ids) == ("A", ["j"])


def test_a_full_group_subsystem_moves_the_whole_group_to_the_candidate_with_fewest_moves():
    # A is full (4/4: three members and one non-member). B has room for all
    # four; C holds member a3... no: C holds nothing of the group but has room.
    g = _group("g", ["a1", "a2", "a3"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("a3", "A", nsid=3),
             _lv("x", "A", nsid=4),
             _lv("b1", "B", maxns=8),
             _lv("j", "J", maxns=1)]
    t = _select(g, lvols[-1], lvols)
    assert t.target_nqn == "B" and not t.target_is_new
    assert t.moving_ids == ["a1", "a2", "a3", "j"]


def test_an_exact_fit_candidate_is_taken():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A", maxns=2), _lv("a2", "A", maxns=2, nsid=2),
             _lv("b1", "B", maxns=4), _lv("j", "J", maxns=1)]
    # B: 4 slots, 1 used, moving set {a1, a2, j} = 3 -> exactly fits.
    t = _select(g, lvols[3], lvols)
    assert (t.target_nqn, t.moving_ids) == ("B", ["a1", "a2", "j"])


def test_a_candidate_one_slot_short_is_skipped():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A", maxns=2), _lv("a2", "A", maxns=2, nsid=2),
             _lv("b1", "B", maxns=4), _lv("b2", "B", maxns=4, nsid=2),
             _lv("j", "J", maxns=1)]
    t = _select(g, lvols[4], lvols)
    assert t.target_is_new and t.target_nqn == ""
    assert t.moving_ids == ["a1", "a2", "j"]


def test_ties_go_to_the_fuller_candidate_then_the_lower_nqn():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=1),
             _lv("c1", "C", maxns=8), _lv("c2", "C", maxns=8, nsid=2),
             _lv("b1", "B", maxns=8),
             _lv("d1", "D", maxns=8), _lv("d2", "D", maxns=8, nsid=2),
             _lv("j", "J", maxns=1)]
    # A is standalone (no shared group subsystem) -> nothing to colocate into;
    # make the group share: a1 in a full 2-slot subsystem instead.
    lvols[0] = _lv("a1", "A", maxns=2)
    lvols.append(_lv("x", "A", maxns=2, nsid=2))
    t = _select(g, lvols[6], lvols)
    # B, C, D all need {a1, j}; C and D are fuller than B; C < D.
    assert t.target_nqn == "C"


def test_busy_foreign_pool_and_standalone_subsystems_are_not_candidates():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2),
             _lv("busy", "B", maxns=8),
             _lv("other", "P", maxns=8, pool="P2"),
             _lv("solo", "S", maxns=1),
             _lv("far", "F", maxns=8, node="N2"),
             _lv("j", "J", maxns=1)]
    t = _select(g, lvols[-1], lvols, busy_nqns={"B"})
    assert t.target_is_new


def test_the_group_subsystem_held_by_another_move_is_a_conflict():
    """Another move already reserved its namespaces into the group's
    subsystem: the free slots the records show are spoken for, so the joiner
    waits instead of being added on top of them."""
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("j", "J", maxns=1)]
    with pytest.raises(cgl.SubsystemBusy, match="A"):
        _select(g, lvols[2], lvols, busy_nqns={"A"})


def test_a_joiner_whose_subsystem_is_held_is_a_conflict():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("j", "J", maxns=1)]
    with pytest.raises(cgl.SubsystemBusy, match="J"):
        _select(g, lvols[2], lvols, busy_nqns={"J"})


def test_a_moving_member_whose_subsystem_is_held_is_a_conflict():
    """The whole group would move to a fresh subsystem, but one member sits
    in a subsystem a migration or another move holds."""
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A", maxns=2), _lv("a2", "A", maxns=2, nsid=2), _lv("j", "J", maxns=1)]
    assert _select(g, lvols[2], lvols).target_is_new
    with pytest.raises(cgl.SubsystemBusy, match="A"):
        _select(g, lvols[2], lvols, busy_nqns={"A"})


def test_a_joiner_on_another_node_is_refused():
    """Before the join's migration lands the joiner is on its old node, whose
    subsystems the group's node does not have; the move cannot run there."""
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2), _lv("j", "J", maxns=1, node="N2")]
    with pytest.raises(cgl.ColocationError, match="N2"):
        _select(g, lvols[2], lvols)


def test_a_member_in_deletion_is_not_moved():
    g = _group("g", ["a1", "a2", "d"])
    lvols = [_lv("a1", "A", maxns=2), _lv("a2", "A", maxns=2, nsid=2),
             _lv("d", "D", maxns=1, status=LVol.STATUS_IN_DELETION), _lv("j", "J", maxns=1)]
    t = _select(g, lvols[3], lvols)
    assert (t.target_is_new, t.moving_ids) == (True, ["a1", "a2", "j"])


def test_a_member_without_a_record_is_not_moved():
    g = _group("g", ["a1", "a2", "ghost"])
    lvols = [_lv("a1", "A", maxns=2), _lv("a2", "A", maxns=2, nsid=2), _lv("j", "J", maxns=1)]
    t = _select(g, lvols[2], lvols)
    assert (t.target_is_new, t.moving_ids) == (True, ["a1", "a2", "j"])


def test_a_joiner_that_is_not_alive_is_refused():
    g = _group("g", ["a1", "a2"])
    lvols = [_lv("a1", "A"), _lv("a2", "A", nsid=2),
             _lv("j", "J", maxns=1, status=LVol.STATUS_IN_DELETION)]
    with pytest.raises(cgl.ColocationError, match="j"):
        _select(g, lvols[2], lvols)


def test_no_candidate_means_a_fresh_subsystem_below_the_limit():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2), _lv("j", "J", maxns=1)]
    # Node holds A and J: 2 subsystems, limit 3 -> a fresh one fits.
    t = _select(g, lvols[2], lvols, limit=3)
    assert t.target_is_new and t.moving_ids == ["a1", "j"]


def test_at_the_subsystem_limit_the_move_is_refused():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2), _lv("j", "J", maxns=1)]
    with pytest.raises(cgl.SubsystemLimitReached) as e:
        _select(g, lvols[2], lvols, limit=2)
    assert (e.value.node_id, e.value.limit) == ("N1", 2)


def test_a_reserved_fresh_subsystem_counts_against_the_limit():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2), _lv("j", "J", maxns=1)]
    with pytest.raises(cgl.SubsystemLimitReached):
        _select(g, lvols[2], lvols, limit=3, reserved_fresh=1)


def test_a_joiner_already_in_the_group_subsystem_moves_nothing():
    g = _group("g", ["a1", "j"])
    lvols = [_lv("a1", "A"), _lv("j", "A", nsid=2)]
    t = _select(g, lvols[1], lvols)
    assert (t.target_nqn, t.moving_ids) == ("A", [])


def test_a_group_with_no_shared_subsystem_moves_nothing():
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=1), _lv("j", "J", maxns=4)]
    t = _select(g, lvols[1], lvols)
    assert t.moving_ids == [] and not t.target_is_new


def test_the_joiner_is_never_counted_as_the_group_subsystem():
    # The joiner, already a member, sits in a shared subsystem J; the group's
    # only other member is in A. A tie would otherwise elect J by NQN order.
    g = _group("g", ["a1", "j"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2), _lv("j", "J", maxns=4)]
    t = _select(g, lvols[2], lvols, limit=3)
    assert t.target_nqn == "J" and t.moving_ids == ["a1"]


def test_the_preflight_counts_the_joiners_subsystem_on_the_pin():
    # The joiner lives on N2. After its migration its subsystem J is on N1 too,
    # so a node with 2 of 3 subsystems before the migration has none to spare.
    g = _group("g", ["a1"])
    lvols = [_lv("a1", "A", maxns=2), _lv("x", "A", maxns=2, nsid=2),
             _lv("o", "O", maxns=1),
             _lv("j", "J", maxns=1, node="N2", lvs="LVS_2")]
    with pytest.raises(cgl.SubsystemLimitReached):
        cgl.predict_move_target(g, lvols[3], lvols, ["j"], node_max_subsystems=3)
    t = cgl.predict_move_target(g, lvols[3], lvols, ["j"], node_max_subsystems=4)
    assert t.target_is_new and t.moving_ids == ["a1", "j"]


# --------------------------------------------------------------------------- #
# Leaving the group's subsystem (a relabel to another group)
# Regression/feature: 2026-11-03-cg-relabel-move
# --------------------------------------------------------------------------- #

def _leave(leaver, lvols, limit=10, **kw):
    return cgl.select_leave_target(leaver, lvols, node_max_subsystems=limit, **kw)


def test_a_member_sharing_its_subsystem_leaves_into_a_fresh_one_alone():
    lvols = [_lv("a1", "A"), _lv("v", "A", nsid=2), _lv("x", "A", nsid=3)]
    t = _leave(lvols[1], lvols)
    assert (t.target_nqn, t.target_is_new, t.moving_ids) == ("", True, ["v"])


def test_a_member_alone_in_its_subsystem_moves_nothing():
    lvols = [_lv("a1", "A"), _lv("v", "V", maxns=4)]
    t = _leave(lvols[1], lvols)
    assert (t.target_nqn, t.target_is_new, t.moving_ids) == ("V", False, [])


def test_a_deleted_sibling_does_not_keep_the_leaver_in_its_subsystem():
    lvols = [_lv("v", "A"), _lv("gone", "A", nsid=2, status=LVol.STATUS_IN_DELETION)]
    t = _leave(lvols[0], lvols)
    assert t.moving_ids == [] and t.target_nqn == "A"


def test_leaving_at_the_node_subsystem_limit_is_refused():
    lvols = [_lv("a1", "A"), _lv("v", "A", nsid=2), _lv("o", "O")]
    with pytest.raises(cgl.SubsystemLimitReached):
        _leave(lvols[1], lvols, limit=2)


def test_fresh_subsystems_other_moves_reserved_count_against_the_limit():
    lvols = [_lv("a1", "A"), _lv("v", "A", nsid=2)]
    with pytest.raises(cgl.SubsystemLimitReached):
        _leave(lvols[1], lvols, limit=2, reserved_fresh=1)
    assert _leave(lvols[1], lvols, limit=3, reserved_fresh=1).target_is_new


def test_leaving_a_subsystem_another_move_or_migration_holds_is_a_conflict():
    lvols = [_lv("a1", "A"), _lv("v", "A", nsid=2)]
    with pytest.raises(cgl.SubsystemBusy):
        _leave(lvols[1], lvols, busy_nqns={"A"})
