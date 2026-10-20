"""Two-node arbitration stays off on a sync-replication cluster.

A sync-replication cluster runs six instances per LVS, a triplet on each site;
two-node arbitration is a two-member mode whose port guards read an HA state
the sync data plane does not have. The flag's writer,
``cluster_ops.set_two_node_arbitration`` (behind the v2 PUT and the private
``cluster set``), refuses it there, so a sync cluster keeps it False, and every
consumer of the flag then behaves as on any regular cluster: a fence release
unblocks without asking the HA state, the defensive unblock after a failed
recreate reaches every member, clients keep the regular keep-alive, the arbiter
does not take the cluster, and the journal gets the plain dual-node flag.
"""
from unittest.mock import call, patch

import pytest

from simplyblock_core import cluster_ops, constants
from simplyblock_core import storage_node_ops as ops
from simplyblock_core.arbitration.arbiter import Arbiter
from simplyblock_core.exceptions import SyncReplicationUnsupportedError
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


@pytest.fixture()
def core_db(db):
    with patch.object(cluster_ops, "db_controller", db):
        yield db


def _flag(db, cluster):
    return db.get_cluster_by_id(cluster.get_id()).two_node_arbitration


def test_enabling_the_flag_is_refused(core_db, layout):
    cluster, *_ = layout
    with pytest.raises(SyncReplicationUnsupportedError):
        cluster_ops.set_two_node_arbitration(cluster.get_id(), True)
    assert _flag(core_db, cluster) is False


def test_the_private_cluster_set_is_refused(core_db, layout):
    cluster, *_ = layout
    with pytest.raises(SyncReplicationUnsupportedError):
        cluster_ops.set_(cluster.get_id(), "two_node_arbitration", "True")
    assert _flag(core_db, cluster) is False


def test_disabling_the_flag_is_allowed(core_db, layout):
    cluster, *_ = layout
    cluster.two_node_arbitration = True
    cluster.write_to_db(core_db.kv_store)
    cluster_ops.set_two_node_arbitration(cluster.get_id(), False)
    assert _flag(core_db, cluster) is False


def test_a_regular_cluster_takes_the_flag(core_db):
    cluster = stack._seed_cluster(core_db)
    cluster.sync_replication = False
    cluster.write_to_db(core_db.kv_store)
    cluster_ops.set_two_node_arbitration(cluster.get_id(), True)
    assert _flag(core_db, cluster) is True


def test_the_refusal_reads_the_record_it_writes(core_db):
    # The record turns sync between the setter's own read and its
    # transaction: the check on the record the transaction writes refuses.
    cluster = stack._seed_cluster(core_db)
    cluster.sync_replication = False
    cluster.write_to_db(core_db.kv_store)
    check = cluster_ops.check_two_node_arbitration

    def check_then_turn_sync(record, enabled):
        check(record, enabled)
        if not record.sync_replication:
            core_db.atomic_update(record, lambda fresh: setattr(fresh, "sync_replication", True))

    with patch.object(cluster_ops, "check_two_node_arbitration", side_effect=check_then_turn_sync), \
            pytest.raises(SyncReplicationUnsupportedError):
        cluster_ops.set_two_node_arbitration(cluster.get_id(), True)
    assert _flag(core_db, cluster) is False


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
