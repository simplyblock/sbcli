"""Paths that act on every instance of an LVS, on a sync-replication cluster,
against the real FoundationDB: the consistency-group snapshot, the snapshot
delete (also of a group generation), auto_repair's safe_delete_bdev and the
del-sync lock reset.

An LVS of a sync-replication cluster has six instances: the owner's home
triplet and its remote triplet on the other site. The triplet of the site in
``lvs_active_site`` is the active one and holds the leader. These paths
register, delete or wait on every instance, and look for the leader in the
active triplet only:

- the group snapshot finds its leader with _find_lvs_leader;
- the snapshot delete and safe_delete_bdev never recover or grant a leader.
  Their candidate lookup may first settle an abandoned move with
  reconcile_lvs_move, which can repeat its fenced hand-off but grants
  nothing; after that the active triplet is only asked who leads. If nobody
  leads, nothing is deleted.

On a cluster without sync replication the home triplet holds every instance,
so the member sets are the home triplet.

Storage nodes (RPC) and the port-block / hublvol side effects are mocked; the
database, its locks and the task queue never are.
"""
import logging
import uuid
from unittest.mock import ANY, call, patch

import pytest

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models import storage_node as storage_node_module
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import (
    RPCConnectionError,
    RPCHTTPError,
    RPCProtocolError,
    RPCRemoteError,
)
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_active_triplet as triplet
from tests.integration import test_sync_replication_cadence_snapshots as cadence
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
_update, _fresh = stack._update, stack._fresh

db = stack.db
rpcs = stack.rpcs
hub = stack.hub
env = stack.env
adopt = stack.adopt
no_sleep = triplet.no_sleep
short_lock_wait = triplet.short_lock_wait

#: What makes a leader: the reload and the grant of the leaderless recovery, the forced hand-over.
_GRANT_RPCS = cadence._GRANT_RPCS


@pytest.fixture(autouse=True)
def _clean_leader_caches():
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()
    yield
    ttl_cache.leader_cache.invalidate()
    ttl_cache.no_leader_cache.invalidate()


@pytest.fixture()
def no_recovery():
    """The leader lookups that may recover or grant a leader, as spies."""
    with patch.object(ops, "find_leader_with_failover", wraps=ops.find_leader_with_failover) as lookup, \
            patch.object(ops, "_recover_leaderless_lvs", wraps=ops._recover_leaderless_lvs) as recover:
        yield lookup, recover


def _never_recovered(no_recovery, rpcs, nodes):
    lookup, recover = no_recovery
    lookup.assert_not_called()
    recover.assert_not_called()
    cadence._not_sent(rpcs, nodes, _GRANT_RPCS)


# ---------------------------------------------------------------------------
# seeding
# ---------------------------------------------------------------------------

def _plain(db):
    """A cluster WITHOUT sync replication: n0 owns LVS_1, n1 / n2 are its
    secondary and tertiary, no node has a site."""
    cluster, owner, pool = cadence._plain_layout(db)
    nodes = [owner] + [db.get_storage_node_by_id(nid)
                       for nid in (owner.secondary_node_id, owner.tertiary_node_id)]
    return cluster, nodes, pool


def _volume(db, cluster, owner, pool):
    return publish._volume(db, cluster, owner, pool,
                           active_site=owner.lvs_active_site or owner.site)


def _snapshot(db, cluster, owner, pool):
    """An online snapshot of a new volume on ``owner``'s LVS."""
    vol = _volume(db, cluster, owner, pool)
    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.cluster_id = cluster.get_id()
    snap.pool_uuid = pool.get_id()
    snap.lvol = vol
    snap.snap_name = f"snap-{snap.uuid[:8]}"
    snap.status = SnapShot.STATUS_ONLINE
    snap.size = vol.size
    snap.snap_bdev = f"{owner.lvstore}/SNAP_{snap.uuid[:8]}"
    snap.fabric = "tcp"
    snap.write_to_db(db.kv_store)
    return snap


def _leads_group(rpcs, node):
    """``node`` leads and takes the group snapshot it is asked for."""
    rpcs.lead(node)
    rpc = rpcs[node.get_id()]
    rpc.bdev_lvol_snapshot_group.return_value = True
    rpc.bdev_get.side_effect = cadence._read_back
    return rpc


def _group(db, cluster, owner, pool, members=2):
    vols = [_volume(db, cluster, owner, pool) for _ in range(members)]
    return cadence._group(db, cluster, owner, vols), vols


def _registered(rpcs, nodes):
    """Node ids among ``nodes`` that got bdev_lvol_snapshot_register."""
    return {n.get_id() for n in nodes if rpcs[n.get_id()].bdev_lvol_snapshot_register.called}


def _ids(nodes):
    return {n.get_id() for n in nodes}


class _Deletes(list):
    """Every delete RPC of ``nodes``, across nodes in the order sent:
    ``(node_id, sync)`` for delete_lvol, ``(node_id, "status")`` for the
    completion poll."""

    def __init__(self, rpcs, nodes, *, status=0):
        super().__init__()
        for node in nodes:
            self._record(rpcs[node.get_id()], node.get_id(), status)

    def _record(self, rpc, node_id, status):
        def _delete(name, sync=False, special_delete=False):
            self.append((node_id, sync))
            return True

        def _status(name):
            self.append((node_id, "status"))
            return status

        rpc.delete_lvol.side_effect = _delete
        rpc.bdev_lvol_get_lvol_delete_status.side_effect = _status

    def fail_sync(self, rpcs, node, error):
        """``node``'s sync delete is recorded, then raises ``error``."""
        node_id = node.get_id()

        def _delete(name, sync=False, special_delete=False):
            self.append((node_id, sync))
            if sync:
                raise error
            return True

        rpcs[node_id].delete_lvol.side_effect = _delete


def _lvs_probes(rpcs, nodes, lvs_name="LVS_1"):
    """Node ids among ``nodes`` asked whether they lead ``lvs_name``."""
    return {n.get_id() for n in nodes if call(lvs_name) in rpcs[n.get_id()].get_lvstore.call_args_list}


# ---------------------------------------------------------------------------
# the consistency-group snapshot
# ---------------------------------------------------------------------------

class TestGroupSnapshotMembers:

    @pytest.mark.parametrize("active_site, site, slot", [
        pytest.param(SITE_B, "b", 0, id="led-from-b-by-its-primary"),
        pytest.param(SITE_B, "b", 1, id="led-from-b-by-its-secondary"),
        pytest.param("", "a", 0, id="led-from-home"),
    ])
    def test_registers_on_every_other_instance(self, db, rpcs, hub, env, no_sleep,
                                               active_site, site, slot):
        cluster, a, b, owner, pool = cadence._layout(db, active_site=active_site)
        leader = {"a": a, "b": b}[site][slot]
        rpc = _leads_group(rpcs, leader)
        group, vols = _group(db, cluster, owner, pool)

        seq, err = cgc.create_group_snapshot_for_group(group)

        assert err is None
        # the generation is taken on the leader, in one RPC
        assert cadence._takers(rpcs, [*a, *b]) == [leader.get_id()]
        rpc.bdev_lvol_snapshot_group.assert_called_once_with("LVS_1", ANY)
        # and each member snapshot is registered on the five other instances
        others = [n for n in (*a, *b) if n.get_id() != leader.get_id()]
        assert _registered(rpcs, [*a, *b]) == _ids(others)
        for node in others:
            assert rpcs[node.get_id()].bdev_lvol_snapshot_register.call_count == len(vols)
        snaps = cgc.generation_snapshots(group, seq)
        assert sorted(s.lvol.get_id() for s in snaps) == sorted(v.get_id() for v in vols)
        cadence._not_sent(rpcs, [*a, *b], _GRANT_RPCS)

    def test_member_of_recorded_site_being_rebuilt_refuses(self, db, rpcs, hub, env, no_sleep):
        """Site A is recorded lost, its nodes are back ONLINE and the site
        activation of A runs; a0 is still in a restart phase of LVS_1 (its
        instance is being rebuilt)."""
        cluster, a, b, owner = triplet._layout(db, active_site=SITE_B, lost_site=SITE_A,
                                               lost_site_state=Cluster.LOST_SITE_DOWN)
        assert src.begin_site_activation(cluster.get_id(), SITE_A)
        pool = publish._pool(db, cluster)
        leader = _leads_group(rpcs, b[0])
        _update(db, a[0], restart_phases={"LVS_1": StorageNode.RESTART_PHASE_PRE_BLOCK})
        group, _ = _group(db, cluster, _fresh(db, owner), pool)

        seq, err = cgc.create_group_snapshot_for_group(group)

        assert seq is None and "is being rebuilt; retry" in err and a[0].get_id()[:8] in err
        leader.bdev_lvol_snapshot_group.assert_not_called()
        assert _registered(rpcs, [*a, *b]) == set()
        assert db.get_snapshots(cluster.get_id()) == []

    def test_non_sync_registers_on_home_secondary_and_tertiary(self, db, rpcs, hub, env, no_sleep):
        cluster, nodes, pool = _plain(db)
        _leads_group(rpcs, nodes[0])
        group, vols = _group(db, cluster, nodes[0], pool)

        seq, err = cgc.create_group_snapshot_for_group(group)

        assert err is None
        assert cadence._takers(rpcs, nodes) == [nodes[0].get_id()]
        assert _registered(rpcs, nodes) == _ids(nodes[1:])
        assert set(rpcs) == _ids(nodes)                  # no other node was asked anything
        assert len(cgc.generation_snapshots(group, seq)) == len(vols)

    def test_failed_register_rolls_back_on_every_instance(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner, pool = cadence._layout(db, active_site=SITE_B)
        _leads_group(rpcs, b[0])
        rpcs[b[2].get_id()].bdev_lvol_snapshot_register.return_value = False
        group, _ = _group(db, cluster, owner, pool, members=1)
        deletes = _Deletes(rpcs, [*a, *b])

        seq, err = cgc.create_group_snapshot_for_group(group)

        assert seq is None and b[2].get_id() in err
        # the leader: async delete, its completion poll, then its sync delete ...
        leader_legs = [d for d in deletes if d[0] == b[0].get_id()]
        assert leader_legs == [(b[0].get_id(), False), (b[0].get_id(), "status"), (b[0].get_id(), True)]
        assert deletes[:3] == leader_legs
        # ... then a sync delete on each of the five other instances
        assert sorted(deletes[3:]) == sorted((n.get_id(), True) for n in (*a, b[1], b[2]))
        assert db.get_snapshots(cluster.get_id()) == []
        assert db.get_consistency_group_by_id(group.get_id()).last_group_seq == 0


# ---------------------------------------------------------------------------
# the snapshot delete and the group generation delete
# ---------------------------------------------------------------------------

def _parked(db, snap):
    """The snapshot waits for the snapshot monitor: it is IN_DELETION and no
    node is recorded as having started its async delete (deletion_status)."""
    snap = db.get_snapshot_by_id(snap.get_id())
    assert (snap.status, snap.deletion_status) == (SnapShot.STATUS_IN_DELETION, "")


def _deleted_on(db, snap, node):
    snap = db.get_snapshot_by_id(snap.get_id())
    assert (snap.status, snap.deletion_status) == (SnapShot.STATUS_IN_DELETION, node.get_id())


class TestSnapshotDelete:

    @pytest.mark.parametrize("slot", [0, 1])
    def test_remote_led_lvs_deletes_on_its_leader_at_once(self, db, rpcs, env, no_recovery, slot):
        cluster, a, b, owner, pool = cadence._layout(db, active_site=SITE_B)
        rpcs.lead(b[slot])
        snap = _snapshot(db, cluster, owner, pool)
        deletes = _Deletes(rpcs, [*a, *b])

        assert snapshot_controller.delete(snap.get_id()) is True

        assert deletes == [(b[slot].get_id(), False)]    # phase 1, async, on the leader only
        _deleted_on(db, snap, b[slot])
        assert _lvs_probes(rpcs, a) == set()             # the passive triplet is not asked
        _never_recovered(no_recovery, rpcs, [*a, *b])

    def test_group_generation_delete_goes_to_remote_leader(self, db, rpcs, hub, env, no_sleep):
        cluster, a, b, owner, pool = cadence._layout(db, active_site=SITE_B)
        _leads_group(rpcs, b[0])
        group, vols = _group(db, cluster, owner, pool)
        seq, err = cgc.create_group_snapshot_for_group(group)
        assert err is None
        deletes = _Deletes(rpcs, [*a, *b])

        with patch.object(ops, "find_leader_with_failover", wraps=ops.find_leader_with_failover) as lookup:
            deleted, err = cgc.delete_generation(group, seq)

        assert err is None and len(deleted) == len(vols)
        assert deletes == [(b[0].get_id(), False)] * len(vols)
        for snap in cgc.generation_snapshots(group, seq):
            _deleted_on(db, snap, b[0])
        lookup.assert_not_called()

    def test_nobody_leading_parks_without_recovery(self, db, rpcs, env, no_recovery):
        """Every member answers, none leads (after a promote, before the first IO)."""
        cluster, a, b, owner, pool = cadence._layout(db, active_site=SITE_B)
        triplet._journal_ready(rpcs, b[0])
        stack._grant_takes(rpcs, b[0])                   # a recovery WOULD make b0 the leader
        snap = _snapshot(db, cluster, owner, pool)
        deletes = _Deletes(rpcs, [*a, *b])

        assert snapshot_controller.delete(snap.get_id()) is True

        assert deletes == []
        _parked(db, snap)
        assert _lvs_probes(rpcs, [*a, *b]) == _ids(b)
        _never_recovered(no_recovery, rpcs, [*a, *b])

    @pytest.mark.parametrize("active_leader", [None, 1])
    def test_passive_member_reporting_leadership_is_never_chosen(
            self, db, rpcs, env, no_recovery, active_leader):
        cluster, a, b, owner, pool = cadence._layout(db, active_site=SITE_B)
        rpcs.lead(a[0])                                  # stale: the home triplet is passive
        if active_leader is not None:
            rpcs.lead(b[active_leader])
        snap = _snapshot(db, cluster, owner, pool)
        deletes = _Deletes(rpcs, [*a, *b])

        assert snapshot_controller.delete(snap.get_id()) is True

        if active_leader is None:
            assert deletes == []
            _parked(db, snap)
        else:
            assert deletes == [(b[active_leader].get_id(), False)]
            _deleted_on(db, snap, b[active_leader])
        rpcs[a[0].get_id()].delete_lvol.assert_not_called()
        _never_recovered(no_recovery, rpcs, [*a, *b])

    def test_abandoned_move_is_settled_and_nobody_leading_parks(
            self, db, rpcs, hub, env, adopt, no_sleep, short_lock_wait, no_recovery):
        """A promote ended and left the LVS at moving:B, and nobody leads it.
        The lookup's reconcile hands the LVS off to b0 without a grant and sets
        the marker to B. b0 takes the leadership only on its first IO, so the
        delete still finds no leader and the snapshot waits for the monitor."""
        cluster, a, b, owner, pool = cadence._layout(db, active_site="moving:" + SITE_B)
        triplet._promote_task(db, cluster, owner, status=JobSchedule.STATUS_DONE)
        for node in (*a, *b):
            triplet._journal_ready(rpcs, node)
        snap = _snapshot(db, cluster, owner, pool)
        deletes = _Deletes(rpcs, [*a, *b])

        assert snapshot_controller.delete(snap.get_id()) is True

        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert deletes == []
        _parked(db, snap)
        triplet._nothing_granted(rpcs, [*a, *b])
        _never_recovered(no_recovery, rpcs, [*a, *b])

    @pytest.mark.parametrize("leader", [None, 1])
    def test_non_sync_probes_home_triplet(self, db, rpcs, no_recovery, leader):
        cluster, nodes, pool = _plain(db)
        if leader is not None:
            rpcs.lead(nodes[leader])
        snap = _snapshot(db, cluster, nodes[0], pool)
        deletes = _Deletes(rpcs, nodes)

        assert snapshot_controller.delete(snap.get_id()) is True

        if leader is None:
            assert deletes == []
            _parked(db, snap)
            assert _lvs_probes(rpcs, nodes) == _ids(nodes)
        else:
            assert deletes == [(nodes[leader].get_id(), False)]
            _deleted_on(db, snap, nodes[leader])
        assert set(rpcs) == _ids(nodes)
        _never_recovered(no_recovery, rpcs, nodes)


# ---------------------------------------------------------------------------
# auto_repair's safe_delete_bdev
# ---------------------------------------------------------------------------

def _not_deleted(caplog):
    return [r.getMessage() for r in caplog.records
            if r.name == ops.logger.name and r.levelno == logging.ERROR
            and "LVS_1/SNAP_14 not deleted" in r.getMessage()]


class TestSafeDeleteBdev:

    @pytest.mark.parametrize("slot", [0, 1])
    def test_remote_led_lvs_deletes_leader_first_then_every_instance(
            self, db, rpcs, env, no_sleep, no_recovery, slot):
        _, a, b, owner, _ = cadence._layout(db, active_site=SITE_B)
        rpcs.lead(b[slot])
        # a stale leadership report on a passive member: it gets only a peer sync delete
        rpcs.lead(a[0])
        deletes = _Deletes(rpcs, [*a, *b])

        assert ops.safe_delete_bdev("SNAP_14", owner.get_id()) is True

        leader = b[slot].get_id()
        assert deletes[:3] == [(leader, False), (leader, "status"), (leader, True)]
        peers = [n.get_id() for n in (*a, *b) if n.get_id() != leader]
        assert deletes[3:] == [(peer, True) for peer in peers]   # role order
        for node in (*a, *b):
            for c in rpcs[node.get_id()].delete_lvol.call_args_list:
                assert c.args[0] == "LVS_1/SNAP_14"    # the owner's lvstore, never the leader's own
        _never_recovered(no_recovery, rpcs, [*a, *b])

    @pytest.mark.parametrize("error", [
        RPCConnectionError("unreachable"),
        RPCHTTPError("bad gateway", 502),
        RPCProtocolError("not JSON-RPC"),
        RPCRemoteError("busy", -16),
    ], ids=["connection", "http", "protocol", "remote"])
    @pytest.mark.parametrize("failing", ["a1", "a2", "b1"])
    def test_failing_instance_does_not_skip_later_instances(
            self, db, rpcs, env, no_sleep, no_recovery, caplog, error, failing):
        _, a, b, owner, _ = cadence._layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        deletes = _Deletes(rpcs, [*a, *b])
        failed = {"a1": a[1], "a2": a[2], "b1": b[1]}[failing]
        deletes.fail_sync(rpcs, failed, error)
        caplog.set_level(logging.INFO)

        assert ops.safe_delete_bdev("SNAP_14", owner.get_id()) is False

        leader = b[0].get_id()
        assert deletes[:3] == [(leader, False), (leader, "status"), (leader, True)]
        peers = [n.get_id() for n in (*a, *b) if n.get_id() != leader]
        assert deletes[3:] == [(peer, True) for peer in peers]   # the failed one, then every later one
        errors = [r.getMessage() for r in caplog.records
                  if r.name == ops.logger.name and r.levelno == logging.ERROR
                  and r.getMessage().startswith("Failed to delete bdev")]
        assert errors == [f"Failed to delete bdev: LVS_1/SNAP_14 from node: {failed.get_id()}: {error}"]
        _never_recovered(no_recovery, rpcs, [*a, *b])

    @pytest.mark.parametrize("passive_leads", [False, True])
    def test_nobody_leading_in_active_triplet_deletes_nothing(
            self, db, rpcs, env, no_sleep, no_recovery, caplog, passive_leads):
        _, a, b, owner, _ = cadence._layout(db, active_site=SITE_B)
        triplet._journal_ready(rpcs, b[0])
        stack._grant_takes(rpcs, b[0])
        if passive_leads:
            rpcs.lead(a[0])
        deletes = _Deletes(rpcs, [*a, *b])
        caplog.set_level(logging.INFO)

        assert ops.safe_delete_bdev("SNAP_14", owner.get_id()) is False

        assert deletes == []
        assert len(_not_deleted(caplog)) == 1
        _never_recovered(no_recovery, rpcs, [*a, *b])

    def test_abandoned_move_is_settled_and_nothing_deleted(
            self, db, rpcs, hub, env, adopt, no_sleep, short_lock_wait, no_recovery, caplog):
        cluster, a, b, owner, _ = cadence._layout(db, active_site="moving:" + SITE_B)
        triplet._promote_task(db, cluster, owner, status=JobSchedule.STATUS_DONE)
        for node in (*a, *b):
            triplet._journal_ready(rpcs, node)
        deletes = _Deletes(rpcs, [*a, *b])
        caplog.set_level(logging.INFO)

        assert ops.safe_delete_bdev("SNAP_14", owner.get_id()) is False

        assert _fresh(db, owner).lvs_active_site == SITE_B
        assert deletes == []
        assert len(_not_deleted(caplog)) == 1
        triplet._nothing_granted(rpcs, [*a, *b])
        _never_recovered(no_recovery, rpcs, [*a, *b])

    def test_non_sync_deletes_owner_first_without_probe(self, db, rpcs, no_sleep, no_recovery):
        _, nodes, _ = _plain(db)
        rpcs.lead(nodes[1])                              # never asked: without sync the delete starts on the owner
        deletes = _Deletes(rpcs, nodes)

        assert ops.safe_delete_bdev("SNAP_14", nodes[0].get_id()) is True

        owner = nodes[0].get_id()
        assert deletes == [(owner, False), (owner, "status"), (owner, True),
                           (nodes[1].get_id(), True), (nodes[2].get_id(), True)]
        assert _lvs_probes(rpcs, nodes) == set()
        _never_recovered(no_recovery, rpcs, nodes)

    def test_non_sync_peer_leg_handles_remote_errors_only(self, db, rpcs, no_sleep):
        """Without sync replication, an RPCRemoteError from a peer's delete
        marks that delete as failed and the next peer is still deleted; any
        other RPC error stops the delete."""
        _, nodes, _ = _plain(db)
        owner, secondary, tertiary = (n.get_id() for n in nodes)
        deletes = _Deletes(rpcs, nodes)
        deletes.fail_sync(rpcs, nodes[1], RPCRemoteError("busy", -16))

        assert ops.safe_delete_bdev("SNAP_14", owner) is False
        assert deletes[3:] == [(secondary, True), (tertiary, True)]

        deletes.clear()
        deletes.fail_sync(rpcs, nodes[1], RPCConnectionError("unreachable"))
        with pytest.raises(RPCConnectionError):
            ops.safe_delete_bdev("SNAP_14", owner)
        assert deletes == [(owner, False), (owner, "status"), (owner, True), (secondary, True)]


# ---------------------------------------------------------------------------
# the del-sync lock reset
# ---------------------------------------------------------------------------

@pytest.fixture()
def lock_no_sleep():
    with patch.object(storage_node_module.time, "sleep"):
        yield


def _sync_del_task(db, cluster, node, owner, *, lvs="LVS_1", canceled=False):
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = node.get_id()
    task.function_name = JobSchedule.FN_LVOL_SYNC_DEL
    task.function_params = {"lvol_bdev_name": f"{lvs}/LVOL_1", "primary_node": owner.get_id()}
    task.status = JobSchedule.STATUS_NEW
    task.canceled = canceled
    task.write_to_db(db.kv_store)
    return task


def _four_per_site(db):
    """Four nodes per site, three LVS whose instance sets differ:
    LVS_1 owned by a0 (home a0 a1 a2, remote b1 b2 b3) and led from site B,
    LVS_2 owned by b1 (home b1 b2 b0, remote a1 a2 a3),
    LVS_3 owned by a3 (home a3 a0 a1, remote b0 b2 b3).
    b1, the remote primary of LVS_1, leads it and holds the del-sync lock;
    a0 is an instance of LVS_1 but not of b1's own LVS_2, and b1 is not an
    instance of LVS_3."""
    cluster = stack._seed_cluster(db)
    a, b = stack._sites(db, cluster, per_site=4)
    owner = stack._make_owner(db, a[0], a[:3], b[1:], lvs_id=1)
    _update(db, owner, lvs_active_site=SITE_B)
    stack._make_owner(db, b[1], [b[1], b[2], b[0]], a[1:], lvs_id=2)
    stack._make_owner(db, a[3], [a[3], a[0], a[1]], [b[0], b[2], b[3]], lvs_id=3)
    return cluster, a, b


class TestDelSyncLockReset:

    def _reset_keeps_lock(self, db, owner):
        owner = _fresh(db, owner)
        owner.lvol_del_sync_lock()
        assert db.get_lvol_del_lock(owner.get_id()) is not None
        owner.lvol_del_sync_lock_reset()
        return db.get_lvol_del_lock(owner.get_id()) is not None

    @pytest.mark.parametrize("site, slot", [("b", 1), ("a", 2)])
    def test_pending_sync_delete_on_any_instance_keeps_lock(self, db, lock_no_sleep, site, slot):
        cluster, a, b, owner = triplet._layout(db)
        task = _sync_del_task(db, cluster, {"a": a, "b": b}[site][slot], owner)

        assert self._reset_keeps_lock(db, owner) is True

        db.atomic_update(task, lambda t: setattr(t, "status", JobSchedule.STATUS_DONE))
        assert self._reset_keeps_lock(db, owner) is False

    def test_nothing_pending_drops_lock(self, db, lock_no_sleep):
        _, _, _, owner = triplet._layout(db)
        assert self._reset_keeps_lock(db, owner) is False

    def test_non_sync_pending_sync_delete_on_secondary_keeps_lock(self, db, lock_no_sleep):
        cluster, nodes, _ = _plain(db)
        _sync_del_task(db, cluster, nodes[1], nodes[0])
        assert self._reset_keeps_lock(db, nodes[0]) is True

    def test_non_sync_pending_sync_delete_elsewhere_drops_lock(self, db, lock_no_sleep):
        cluster, nodes, _ = _plain(db)
        other = stack._seed_node(db, cluster, "n3", "")
        _sync_del_task(db, cluster, other, nodes[0])
        assert self._reset_keeps_lock(db, nodes[0]) is False

    def test_remote_leader_keeps_lock_for_led_lvs_outside_its_own(self, db, lock_no_sleep):
        """The task targets a0, which holds an instance of LVS_1 (the LVS b1
        leads) but not of b1's own LVS_2. The task's primary_node is the owner
        a0, not the lock holder b1."""
        cluster, a, b = _four_per_site(db)
        task = _sync_del_task(db, cluster, a[0], a[0], lvs="LVS_1")

        assert self._reset_keeps_lock(db, b[1]) is True

        db.atomic_update(task, lambda t: setattr(t, "status", JobSchedule.STATUS_DONE))
        assert self._reset_keeps_lock(db, b[1]) is False

    def test_canceled_sync_delete_drops_lock(self, db, lock_no_sleep):
        cluster, a, b = _four_per_site(db)
        _sync_del_task(db, cluster, a[0], a[0], lvs="LVS_1", canceled=True)
        assert self._reset_keeps_lock(db, b[1]) is False

    def test_sync_delete_of_lvs_the_holder_is_no_instance_of_drops_lock(self, db, lock_no_sleep):
        """The task's target b2 is a member of b1's own LVS, but the task
        deletes from LVS_3, and b1 holds no instance of LVS_3. So the task does
        not keep b1's lock."""
        cluster, a, b = _four_per_site(db)
        _sync_del_task(db, cluster, b[2], a[3], lvs="LVS_3")
        assert self._reset_keeps_lock(db, b[1]) is False

    @pytest.mark.parametrize("site, slot, keeps", [
        pytest.param("b", 2, True, id="own-peer"),
        pytest.param("a", 0, False, id="not-own-peer"),
    ])
    def test_sync_delete_of_unknown_lvs_matches_own_peers(self, db, lock_no_sleep, site, slot, keeps):
        cluster, a, b = _four_per_site(db)
        _sync_del_task(db, cluster, {"a": a, "b": b}[site][slot], b[1], lvs="LVS_9")
        assert self._reset_keeps_lock(db, b[1]) is keeps
