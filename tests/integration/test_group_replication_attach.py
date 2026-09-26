"""Group replication attach/detach against real FDB (design-csi-addons-
replication.md §14.4).

``attach_group_policy`` links a standalone consistency group to a group
replication policy and starts each open member replicating WITHOUT re-adding
members -- re-adding would reset their generation epochs
(``add_member_to_group`` stamps a fresh ``joined_seq``) and tear the group's
snapshot history. ``detach_group_policy`` stops replication and unlinks the
policy WITHOUT dissolving the group: the members stay grouped by their label.

The group-state changes are ``DBController`` reads and writes, so this is the
FDB-backed tier. The per-member replication start/stop is node-touching and is
mocked -- the node layer is always mocked in this tier.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import replication_policy_controller as rpc
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.replication import (
    ConsistencyGroup,
    ReplicationPolicy,
    ReplicationTarget,
)

CLUSTER_ID = "grp-attach-cluster-1"
NODE = "grp-attach-node-1"
LVS = "LVS_1"


@pytest.fixture
def db():
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return db


def _seed_policy(db, consistency_group=False):
    target = ReplicationTarget()
    target.uuid = "grp-attach-tgt-1"
    target.cluster_id = CLUSTER_ID
    target.target_cluster_id = "grp-attach-peer-1"
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)

    policy = ReplicationPolicy()
    policy.uuid = "grp-attach-pol-1"
    policy.cluster_id = CLUSTER_ID
    policy.policy_name = "grp-5m"
    policy.target_id = target.get_id()
    policy.interval_min = 5
    policy.mode = ReplicationPolicy.MODE_FAILOVER
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.consistency_group = consistency_group
    policy.write_to_db(db.kv_store)
    return policy


def _seed_group(db, members):
    group = ConsistencyGroup()
    group.uuid = "grp-attach-grp-1"
    group.cluster_id = CLUSTER_ID
    group.group_name = "app-cg"
    group.node_id = NODE
    group.lvs_name = LVS
    group.last_group_seq = 2
    group.members = {m: {"joined_seq": 1, "removed_seq": 0} for m in members}
    group.write_to_db(db.kv_store)
    return group


def test_attach_links_a_plain_policy_and_starts_each_member(db, monkeypatch):
    # A plain policy (no consistency_group flag): membership, not a flag, is what
    # makes the group crash-consistent, so any policy attaches.
    policy = _seed_policy(db)
    group = _seed_group(db, ["v1", "v2"])
    started = []
    monkeypatch.setattr(rpc, "start_member_replication",
                        lambda lvol_id, pol, target: started.append(lvol_id))

    cgc.attach_group_policy(group, policy.get_id())

    reloaded = db.get_consistency_group_by_id(group.get_id())
    assert reloaded.policy_id == policy.get_id()
    assert sorted(started) == ["v1", "v2"]
    # Members are never re-added, so their generation epochs are untouched.
    assert reloaded.members["v1"]["joined_seq"] == 1
    assert reloaded.members["v2"]["joined_seq"] == 1


def test_attach_refuses_a_missing_policy(db, monkeypatch):
    group = _seed_group(db, ["v1"])
    monkeypatch.setattr(rpc, "start_member_replication",
                        lambda *a, **k: pytest.fail("must not start replication"))

    with pytest.raises(cgc.ConsistencyGroupError):
        cgc.attach_group_policy(group, "no-such-policy")

    # A refused attach leaves the group unlinked.
    assert db.get_consistency_group_by_id(group.get_id()).policy_id == ""


def test_detach_unlinks_and_stops_without_dissolving_the_group(db, monkeypatch):
    policy = _seed_policy(db)
    group = _seed_group(db, ["v1", "v2"])
    group.policy_id = policy.get_id()
    group.write_to_db(db.kv_store)
    stopped = []
    monkeypatch.setattr(rpc, "stop_member_replication",
                        lambda lvol_id: stopped.append(lvol_id))

    cgc.detach_group_policy(group)

    reloaded = db.get_consistency_group_by_id(group.get_id())
    assert reloaded.policy_id == ""
    assert sorted(stopped) == ["v1", "v2"]
    # The group is not dissolved -- its members stay grouped by their label.
    assert set(reloaded.members) == {"v1", "v2"}


def test_demote_group_is_demoted_only_when_every_member_is(db, monkeypatch):
    from simplyblock_core.controllers import lvol_controller as lc
    group = _seed_group(db, ["v1", "v2"])

    monkeypatch.setattr(lc, "demote_lvol", lambda lvol_id: {"demoted": True})
    result = cgc.demote_group(group)
    assert result["demoted"] is True
    assert {m["lvol_id"] for m in result["members"]} == {"v1", "v2"}

    # One member still converging keeps the whole group un-demoted.
    monkeypatch.setattr(lc, "demote_lvol", lambda lvol_id: {"demoted": lvol_id == "v1"})
    result = cgc.demote_group(group)
    assert result["demoted"] is False


def test_failback_group_configures_every_member(db, monkeypatch):
    from simplyblock_core.controllers import lvol_controller as lc
    group = _seed_group(db, ["v1", "v2"])
    calls = []

    def _failback(lvol_id, source_cluster_id=None):
        calls.append((lvol_id, source_cluster_id))
        return True

    monkeypatch.setattr(lc, "replication_failback", _failback)
    result = cgc.failback_group(group, source_cluster_id="grp-attach-peer-1")

    assert result["configured"] is True
    assert sorted(c[0] for c in calls) == ["v1", "v2"]
    assert all(c[1] == "grp-attach-peer-1" for c in calls)
