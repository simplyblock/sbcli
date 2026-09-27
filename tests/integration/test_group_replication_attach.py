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
from simplyblock_core.models.snapshot import SnapShot

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


def test_demote_group_ship_home_takes_ONE_group_snapshot(db, monkeypatch):
    # The demote generation must be a SINGLE bdev_lvol_snapshot_group across all
    # members (one group_seq the peer can promote from), NOT a per-member snapshot
    # each. Regression: 2026-09-27 — per-member demote snapshots share no common
    # generation, so the peer's group promote finds no cut to clone.
    import simplyblock_core.services.replication_final_step as rfs
    from simplyblock_core.controllers import lvol_controller as lc
    group = _seed_group(db, ["v1", "v2"])
    _seed_lvol(db, "v1", NODE, LVS, group_id=group.get_id())
    _seed_lvol(db, "v2", NODE, LVS, group_id=group.get_id())
    monkeypatch.setattr(cgc, "_members_are_superseded_source", lambda members: False)
    monkeypatch.setattr(rfs, "fence_source_paths", lambda *a, **k: None)
    monkeypatch.setattr(lc, "_replication_for_lvol", lambda db_, lid: None)

    grp_calls = []

    def _grp_snap(g, **k):
        grp_calls.append(g.get_id())
        ids = []
        for mid in ("v1", "v2"):
            s = SnapShot()
            s.uuid = f"snap-{mid}"
            s.lvol = db.get_lvol_by_id(mid)
            s.write_to_db(db.kv_store)
            ids.append(s.uuid)
        return ids, None
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group", _grp_snap)

    result = cgc.demote_group(group)
    assert len(grp_calls) == 1, "exactly ONE group snapshot for the whole demote, not per member"
    assert result["demoted"] is False, "not done until the generation replicates"
    v1 = db.get_lvol_by_id("v1")
    assert v1.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING
    assert v1.replication_demote_snapshot_id == "snap-v1", "member tracks its group-cut slice"


def test_demote_group_superseded_source_does_not_take_a_group_snapshot(db, monkeypatch):
    # The recovered old primary (source side of a failed-over rep) is superseded:
    # fence + done per member, nothing to ship, no group snapshot.
    from simplyblock_core.controllers import lvol_controller as lc
    group = _seed_group(db, ["v1", "v2"])
    _seed_lvol(db, "v1", NODE, LVS, group_id=group.get_id())
    _seed_lvol(db, "v2", NODE, LVS, group_id=group.get_id())
    monkeypatch.setattr(cgc, "_members_are_superseded_source", lambda members: True)
    grp_calls = []
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group",
                        lambda g, **k: grp_calls.append(g) or ([], None))
    monkeypatch.setattr(lc, "demote_lvol", lambda lvol_id: {"demoted": True})

    result = cgc.demote_group(group)
    assert result["demoted"] is True
    assert grp_calls == [], "a superseded source ships nothing -- no group snapshot"


def test_demote_group_completes_only_when_every_member_generation_replicated(db):
    group = _seed_group(db, ["v1", "v2"])
    for mid, replicated in (("v1", "tgt-v1"), ("v2", "")):
        lv = _seed_lvol(db, mid, NODE, LVS, group_id=group.get_id())
        lv.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
        lv.replication_demote_snapshot_id = f"snap-{mid}"
        lv.write_to_db(db.kv_store)
        s = SnapShot()
        s.uuid = f"snap-{mid}"
        s.target_replicated_snap_uuid = replicated
        s.write_to_db(db.kv_store)

    # v2's slice has not replicated yet -> group not demoted.
    assert cgc.demote_group(group)["demoted"] is False

    # once v2's slice lands, the whole group is demoted.
    s2 = db.get_snapshot_by_id("snap-v2")
    s2.target_replicated_snap_uuid = "tgt-v2"
    s2.write_to_db(db.kv_store)
    assert cgc.demote_group(group)["demoted"] is True


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
