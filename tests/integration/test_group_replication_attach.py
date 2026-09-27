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
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.replication import (
    ConsistencyGroup,
    ReplicationPolicy,
    ReplicationTarget,
)

PEER_CLUSTER = "grp-attach-peer-cluster-1"

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


def _seed_lvol(db, lvol_id, node, lvs, group_id=""):
    lv = LVol()
    lv.uuid = lvol_id
    lv.node_id = node
    lv.lvs_name = lvs
    lv.group_id = group_id
    lv.write_to_db(db.kv_store)
    return lv


def test_reconstitute_forms_the_group_on_the_target_after_failover(db):
    # A fail-over clones each member individually onto the target, leaving them
    # ungrouped; reconstitution re-forms the group there so the target stays
    # crash-consistent. Keyed by name, on the peer cluster, a NEW record.
    src_group = _seed_group(db, ["src1"])
    src_lvol = _seed_lvol(db, "src1", NODE, LVS, group_id=src_group.get_id())

    clone = _seed_lvol(db, "tgt1", "peer-node-1", "LVS_9")
    grp = cgc.reconstitute_group_after_handoff(src_lvol, clone, PEER_CLUSTER)

    assert grp is not None
    assert grp.cluster_id == PEER_CLUSTER
    assert grp.group_name == src_group.group_name
    assert grp.get_id() != src_group.get_id()   # distinct record on the peer cluster
    reloaded = db.get_consistency_group_by_id(grp.get_id())
    assert "tgt1" in reloaded.members and reloaded.members["tgt1"]["removed_seq"] == 0
    assert db.get_lvol_by_id("tgt1").group_id == grp.get_id()


def test_reconstitute_failback_returns_to_the_original_group(db):
    # The original group on the source cluster, its member's epoch CLOSED because
    # the fail-over deleted the source. A fail-back must rejoin THIS group (same
    # record, keyed by name), re-opening the epoch -- not mint a new one.
    orig = _seed_group(db, [])
    orig.members = {"vol1": {"joined_seq": 1, "removed_seq": 2}}
    orig.write_to_db(db.kv_store)

    # The peer-cluster group (same NAME) that held the failed-over volume.
    peer = ConsistencyGroup()
    peer.uuid = "grp-attach-peer-cg-1"
    peer.cluster_id = PEER_CLUSTER
    peer.group_name = orig.group_name
    peer.node_id = "peer-node-1"
    peer.lvs_name = "LVS_9"
    peer.members = {"tvol": {"joined_seq": 1, "removed_seq": 0}}
    peer.write_to_db(db.kv_store)
    peer_lvol = _seed_lvol(db, "tvol", "peer-node-1", "LVS_9", group_id=peer.get_id())

    # The failed-back clone lands back on the ORIGINAL node/LVS with the original
    # UUID (the cutover's UUID swap restored it).
    failed_back = _seed_lvol(db, "vol1", NODE, LVS)
    grp = cgc.reconstitute_group_after_handoff(peer_lvol, failed_back, CLUSTER_ID)

    assert grp.get_id() == orig.get_id(), "fail-back must return to the SAME group"
    reloaded = db.get_consistency_group_by_id(orig.get_id())
    assert reloaded.members["vol1"]["removed_seq"] == 0, "epoch re-opened"
    assert db.get_lvol_by_id("vol1").group_id == orig.get_id()


def test_reconstitute_is_a_noop_for_a_non_group_volume(db):
    src_lvol = _seed_lvol(db, "plain1", NODE, LVS)   # no group_id
    clone = _seed_lvol(db, "plain-clone", "peer-node-1", "LVS_9")
    assert cgc.reconstitute_group_after_handoff(src_lvol, clone, PEER_CLUSTER) is None
    assert db.get_lvol_by_id("plain-clone").group_id == ""


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
