"""``aggregate_group_replication_info``: a consistency group's replication status
is the roll-up of its members -- oldest recovery point, worst lag, worst health,
summed backlog -- because a group is only as protected as its slowest, sickest
member (design-csi-addons-replication.md §14.4/§14.6). Pure logic, no DB.
"""
from simplyblock_core.controllers import consistency_group_controller as cgc


def _info(role="source", state="in_sync", last=100.0, lag=5, oc=0, ob=0, resync=False):
    return {"role": role, "state": state, "last_replicated_at": last,
            "lag_seconds": lag, "outstanding_count": oc, "outstanding_bytes": ob,
            "resyncing": resync}


def test_empty_group_is_not_replicating():
    agg = cgc.aggregate_group_replication_info([])
    assert agg["member_count"] == 0
    assert agg["role"] == "none"
    assert agg["state"] == "not_replicating"
    assert agg["last_replicated_at"] is None
    assert agg["lag_seconds"] is None


def test_all_healthy_rolls_up_oldest_and_worst():
    infos = [_info(last=100.0, lag=3, oc=1, ob=10),
             _info(last=80.0, lag=9, oc=2, ob=20)]
    agg = cgc.aggregate_group_replication_info(infos)
    assert agg["member_count"] == 2
    assert agg["role"] == "source"
    assert agg["state"] == "in_sync"
    assert agg["last_replicated_at"] == 80.0     # oldest member
    assert agg["lag_seconds"] == 9               # worst member
    assert agg["outstanding_count"] == 3         # summed
    assert agg["outstanding_bytes"] == 30
    assert agg["resyncing"] is False


def test_one_degraded_member_degrades_the_group():
    agg = cgc.aggregate_group_replication_info(
        [_info(state="in_sync"), _info(state="degraded")])
    assert agg["state"] == "degraded"


def test_one_erroring_member_is_the_worst():
    agg = cgc.aggregate_group_replication_info(
        [_info(state="degraded"), _info(state="error")])
    assert agg["state"] == "error"


def test_a_member_without_a_recovery_point_leaves_the_group_without_one():
    agg = cgc.aggregate_group_replication_info(
        [_info(last=100.0, lag=5), _info(last=None, lag=None)])
    assert agg["last_replicated_at"] is None
    assert agg["lag_seconds"] is None


def test_mixed_member_roles_report_none():
    agg = cgc.aggregate_group_replication_info(
        [_info(role="source"), _info(role="failed_over")])
    assert agg["role"] == "none"


def test_any_resyncing_member_marks_the_group_resyncing():
    agg = cgc.aggregate_group_replication_info(
        [_info(resync=False), _info(resync=True)])
    assert agg["resyncing"] is True


class TestAggregateGroupDemote:
    """A group is demoted only when EVERY member is; a hard error on any member
    makes the whole group's demote an error (design §14.4)."""

    def test_all_members_demoted_is_a_demoted_group(self):
        agg = cgc.aggregate_group_demote(
            [("v1", {"demoted": True}), ("v2", {"demoted": True})])
        assert agg["demoted"] is True
        assert agg["error"] is None

    def test_one_member_still_converging_is_not_demoted(self):
        agg = cgc.aggregate_group_demote(
            [("v1", {"demoted": True}), ("v2", {"demoted": False})])
        assert agg["demoted"] is False
        assert agg["error"] is None
        assert {"lvol_id": "v2", "demoted": False} in agg["members"]

    def test_a_member_hard_error_makes_the_group_demote_an_error(self):
        agg = cgc.aggregate_group_demote(
            [("v1", {"demoted": True}), ("v2", (False, "peer unreachable"))])
        assert agg["demoted"] is False
        assert agg["error"] == "v2: peer unreachable"
