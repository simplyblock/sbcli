"""Two-node arbitration stays off on a sync-replication cluster.

A sync-replication cluster runs six instances per LVS, a triplet on each site;
two-node arbitration is a two-member mode whose port guards read an HA state
the sync data plane does not have. The v2 PUT refuses the flag there, so a sync
cluster created the normal way keeps it False, and every consumer of the flag
then behaves as on any regular cluster: a fence release unblocks without asking
the HA state, the defensive unblock after a failed recreate reaches every
member, clients keep the regular keep-alive, the arbiter does not take the
cluster, and the journal gets the plain dual-node flag.
"""
from unittest.mock import call, patch

import pytest

from simplyblock_core import constants
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.arbitration.arbiter import Arbiter
from simplyblock_core.utils.nvme import build_nvme_connect_entry
from tests.integration import test_sync_replication_lvs_stack as stack

db = stack.db
rpcs = stack.rpcs
_fresh = stack._fresh

PORT = 4402                     # LVS_1's client port (stack._make_owner)


@pytest.fixture()
def layout(db):
    cluster = stack._seed_cluster(db)
    a, b = stack._sites(db, cluster)
    owner = stack._make_owner(db, a[0], a, b, lvs_id=1)
    return cluster, a, b, owner


def test_the_flag_is_off(db, layout):
    cluster, *_ = layout
    assert db.get_cluster_by_id(cluster.get_id()).two_node_arbitration is False
    assert ops._cluster_two_node_arbitrated(cluster.get_id(), db=db) is False


def test_a_fence_release_unblocks_without_asking_the_ha_state(db, rpcs, layout):
    _, a, _, owner = layout
    peers = [_fresh(db, a[1]), _fresh(db, a[2])]
    fence = ops._LvsFence(_fresh(db, owner), _fresh(db, owner), db)
    fence.blocked_peers.extend(peers)
    for peer in peers:
        fence.unblock_peer_port(peer, demoted=True)
    for peer in peers:
        rpcs[peer.get_id()].nvmf_port_unblock.assert_called_once_with(PORT)
        rpcs[peer.get_id()].jc_ha_status.assert_not_called()
    assert fence.blocked_peers == []


def test_the_defensive_unblock_reaches_every_member(db, rpcs, layout):
    _, a, b, owner = layout
    with patch.object(ops, "tcp_ports_events"):
        ops._release_lvs_subsys_port_on_peers(_fresh(db, owner), a[0].get_id(), db)
    for node in (a[1], a[2], *b):
        rpcs[node.get_id()].nvmf_port_unblock.assert_called_once_with(PORT)
        rpcs[node.get_id()].jc_ha_status.assert_not_called()
    rpcs[a[0].get_id()].nvmf_port_unblock.assert_not_called()


def test_clients_keep_the_regular_keep_alive(db, layout):
    cluster, *_ = layout
    entry = build_nvme_connect_entry(
        transport="tcp", ip="10.3.0.1", port=PORT, nqn="nqn.vol", ctrl_loss_tmo=3600,
        cluster=db.get_cluster_by_id(cluster.get_id()), host_entry=None, host_nqn=None)
    assert entry.keep_alive_tmo == constants.LVOL_NVME_KEEP_ALIVE_TO_TCP
    assert f"--keep-alive-tmo={constants.LVOL_NVME_KEEP_ALIVE_TO_TCP} " in entry.connect


def test_the_arbiter_does_not_take_it(db, layout):
    assert Arbiter(db).arbitrated_clusters() == []


def test_the_journal_gets_the_plain_dual_node_flag(db, rpcs, layout):
    cluster, a, b, _ = layout
    ops.apply_jc_dual_node(cluster.get_id())
    for node in (*a, *b):
        assert rpcs[node.get_id()].jc_set_dual_node.call_args_list == [call(False)]
