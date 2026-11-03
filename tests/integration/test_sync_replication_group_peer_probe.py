"""The consistency-group demote's peer serving-check against a sync-replication peer.

A re-protect after an unplanned fail-over and a relocate fail-back leave the
same replication state; ``_peer_group_is_being_served`` tells them apart by
asking whether a host is connected to the peer's volumes. On a sync-replication
peer a volume's ``node_id`` is the home owner of its LVS, while a host is only
connected to the triplet of the site the LVS is led from, which may be the
remote one: every instance of the LVS is asked.
"""
import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.replication import ConsistencyGroup
from simplyblock_core.rpc_client import RPCConnectionError
from simplyblock_core.services import replication_final_step
from tests.integration import test_group_replication_attach as attach
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs
_update = stack._update

CONTROLLER = [{"cntlid": 1, "hostnqn": "nqn.host"}]


@pytest.fixture()
def peer(db, rpcs):
    """a0 owns LVS_1, led from site B; one volume on it. No node has a host
    connected until a test says so."""
    cluster, a, b, owner = publish._layout(db, active_site=SITE_B)
    vol = publish._volume(db, cluster, owner, publish._pool(db, cluster), ns_id=1,
                          active_site=SITE_B, demoted=[SITE_A])
    for node in (*a, *b):
        rpcs[node.get_id()].nvmf_subsystem_get_controllers.return_value = []
    return owner, [*a, *b], vol


@pytest.mark.parametrize("member", range(6))
def test_a_host_on_any_instance_means_served(db, rpcs, peer, member):
    _, nodes, vol = peer
    rpcs[nodes[member].get_id()].nvmf_subsystem_get_controllers.return_value = CONTROLLER
    assert cgc._peer_group_is_being_served([vol]) is True


def test_no_host_anywhere_is_not_served_and_every_instance_was_asked(db, rpcs, peer):
    _, nodes, vol = peer
    assert cgc._peer_group_is_being_served([vol]) is False
    for node in nodes:
        rpcs[node.get_id()].nvmf_subsystem_get_controllers.assert_called_once_with(vol.nqn)


def test_an_instance_that_cannot_be_asked_counts_as_served(db, rpcs, peer):
    _, nodes, vol = peer
    rpcs[nodes[5].get_id()].nvmf_subsystem_get_controllers.side_effect = RPCConnectionError("down")
    assert cgc._peer_group_is_being_served([vol]) is True


def test_a_member_that_no_longer_exists_counts_as_served(db, rpcs, peer):
    owner, _, vol = peer
    _update(db, owner, remote_tertiary_node_id="gone-node")
    assert cgc._peer_group_is_being_served([vol]) is True


def test_an_owner_without_a_site_is_asked_alone(db, rpcs, peer):
    owner, nodes, vol = peer
    _update(db, owner, site="")
    rpcs[nodes[3].get_id()].nvmf_subsystem_get_controllers.return_value = CONTROLLER
    assert cgc._peer_group_is_being_served([vol]) is False
    for node in nodes[1:]:
        rpcs[node.get_id()].nvmf_subsystem_get_controllers.assert_not_called()
    rpcs[owner.get_id()].nvmf_subsystem_get_controllers.return_value = CONTROLLER
    assert cgc._peer_group_is_being_served([vol]) is True


def test_a_group_demote_leaves_a_served_sync_peer_untouched(db, rpcs, peer, monkeypatch):
    """A re-protect toward a sync peer: the origin group's members are
    superseded sources, the peer group's volume is served from site B only.
    The demote fences the origin's own members and leaves the peer alone - no
    fence, no reverse pipe, no group cut."""
    _, nodes, vol = peer
    rpcs[nodes[3].get_id()].nvmf_subsystem_get_controllers.return_value = CONTROLLER
    policy = attach._seed_policy(db)
    origin = attach._seed_group(db, ["orig-1"])
    origin.policy_id = policy.get_id()
    origin.write_to_db(db.kv_store)
    attach._seed_lvol(db, "orig-1", attach.NODE, attach.LVS, group_id=origin.get_id())
    peer_group = ConsistencyGroup()
    peer_group.uuid = "sync-peer-grp-1"
    peer_group.cluster_id = db.get_replication_target_by_id(policy.target_id).target_cluster_id
    peer_group.group_name = origin.group_name
    peer_group.members = {vol.get_id(): {"joined_seq": 1, "removed_seq": 0}}
    peer_group.write_to_db(db.kv_store)

    monkeypatch.setattr(cgc, "_members_are_superseded_source",
                        lambda members: all(m.get_id() == "orig-1" for m in members))
    fenced, failed_back, cut, demoted = [], [], [], []
    monkeypatch.setattr(replication_final_step, "fence_source_paths",
                        lambda node, lvs, nqn, ns_id: fenced.append(nqn))
    monkeypatch.setattr(lvol_controller, "replication_failback", failed_back.append)
    monkeypatch.setattr(cgc, "create_group_snapshot_for_group",
                        lambda group, **kwargs: cut.append(group.get_id()) or ([], None))
    monkeypatch.setattr(lvol_controller, "demote_lvol",
                        lambda lvol_id: demoted.append(lvol_id) or {"demoted": True})

    result = cgc.demote_group(origin)

    assert (fenced, failed_back, cut) == ([], [], [])
    assert demoted == ["orig-1"]
    assert result["demoted"] is True
