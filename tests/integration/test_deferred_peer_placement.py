"""A volume whose replication destination is a peer control plane that cannot be
reached keeps its replication: only the placement on the peer waits, and the
volume's first replication task makes it once the peer answers.

Bed run 20261113-1148-ebs-splitdr (control-plane split, M3): the fail-over on
site A attached Gitea's group to site A's policy toward site B while site B was
still cut off. replication_start asked site B to place each member, timed out,
and the attach was rolled back. Nothing retried it after the heal, so Gitea was
never protected again, and the fail-over call blocked for 124 s on the peer.

Bed run 20261113-2112-ebs-splitdr: each member was placed twice after the heal.
A task reads its volume from the snapshot record, a copy taken when the
snapshot was made, so a second task of the same volume did not see the
placement the first one had stored.

Real FDB: the volume, policy, target, group, snapshot and task records are
seeded and read back. The peer control plane (a remote API) and the source
node's chain walk are mocked.
"""
import time
import uuid

import pytest

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.replication import (
    ConsistencyGroup,
    ReplicationPolicy,
    ReplicationTarget,
)
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.peer.client import PeerRefusedError, PeerUnreachableError
from simplyblock_core.peer.protocol import PlaceResponse
from simplyblock_core.services import snapshot_replication as sr
from tests.integration import test_sync_replication_lvol_publish as publish
from tests.integration import test_sync_replication_lvs_stack as stack

PEER_CLUSTER = "deferred-peer-cluster-b"

db = stack.db


class _Peer:
    """Site B's control plane, as the peer client reaches it. ``place`` answers
    from *nodes* in turn, or raises *error*; every call is recorded."""

    def __init__(self, nodes=(), error=None):
        self.nodes = list(nodes)
        self.error = error
        self.placed = []
        self.landings = []

    def place(self, request):
        self.placed.append(request)
        if self.error is not None:
            raise self.error
        return PlaceResponse(node_id=self.nodes.pop(0), rule="capacity")

    def prepare_landing(self, request):
        self.landings.append(request)
        raise PeerRefusedError("stop")   # past the destination is all these tests need


@pytest.fixture()
def peer(monkeypatch):
    p = _Peer()
    monkeypatch.setattr(lvol_controller, "peer_for_target", lambda target: p)
    monkeypatch.setattr(sr, "peer_for_target", lambda target: p)
    return p


def _site_a(db, members=1):
    """Site A: a cluster, a node, a policy toward site B through its peer
    control plane, and *members* volumes on it."""
    cluster = stack._seed_cluster(db)
    cluster.sync_replication = False
    cluster.write_to_db(db.kv_store)
    node = stack._seed_node(db, cluster, "a1", "")
    pool = publish._pool(db, cluster)

    target = ReplicationTarget()
    target.uuid = str(uuid.uuid4())
    target.cluster_id = cluster.get_id()
    target.target_name = "site-b"
    target.target_cluster_id = PEER_CLUSTER
    target.peer_endpoint = "http://10.70.3.21:15000"
    target.status = ReplicationTarget.STATUS_ACTIVE
    target.write_to_db(db.kv_store)

    policy = ReplicationPolicy()
    policy.uuid = str(uuid.uuid4())
    policy.cluster_id = cluster.get_id()
    policy.policy_name = "async-5m-to-site-b"
    policy.target_id = target.get_id()
    policy.interval_min = 5
    policy.mode = ReplicationPolicy.MODE_FAILOVER
    policy.status = ReplicationPolicy.STATUS_ACTIVE
    policy.write_to_db(db.kv_store)

    vols = []
    for _ in range(members):
        vol = publish._volume(db, cluster, node, pool, active_site="", nodes=[node.get_id()])
        vols.append(vol)
    return cluster, node, policy, vols


def _with(db, vol, **fields):
    fresh = db.get_lvol_by_id(vol.get_id())
    for key, value in fields.items():
        setattr(fresh, key, value)
    fresh.write_to_db(db.kv_store)
    return fresh


def _unreachable():
    return PeerUnreachableError(
        f"POST http://10.70.3.21:15000/api/v2/_peer/clusters/{PEER_CLUSTER}/place: timed out")


# ------------------------------------------------------------------ replication_start

class TestReplicationStart:

    def test_an_unreachable_peer_defers_the_placement(self, db, peer):
        peer.error = _unreachable()
        _, _, policy, (vol,) = _site_a(db)
        _with(db, vol, replication_policy_id=policy.get_id())

        assert lvol_controller.replication_start(vol.get_id(), replication_cluster_id=PEER_CLUSTER,
                                                 from_policy=True) is True
        stored = db.get_lvol_by_id(vol.get_id())
        assert stored.do_replicate is True
        assert stored.replication_node_id == "", "no node until the peer places the volume"

    def test_a_peer_that_refuses_the_placement_still_fails_the_start(self, db, peer):
        peer.error = PeerRefusedError(f"no member of {PEER_CLUSTER} can take 1 GiB")
        _, _, policy, (vol,) = _site_a(db)
        _with(db, vol, replication_policy_id=policy.get_id())

        assert lvol_controller.replication_start(vol.get_id(), replication_cluster_id=PEER_CLUSTER,
                                                 from_policy=True) is False

    def test_a_deferred_start_does_not_call_the_peer(self, db, peer):
        peer.nodes = ["B1"]
        _, _, policy, (vol,) = _site_a(db)
        _with(db, vol, replication_policy_id=policy.get_id())

        assert lvol_controller.replication_start(vol.get_id(), replication_cluster_id=PEER_CLUSTER,
                                                 from_policy=True, defer_peer_placement=True) is True
        stored = db.get_lvol_by_id(vol.get_id())
        assert peer.placed == []
        assert (stored.do_replicate, stored.replication_node_id) == (True, "")


# ------------------------------------------------------------------ the reverse attach

def test_the_reverse_attach_on_a_fail_over_defers_the_placement(db, peer):
    """The promote does not wait on a peer that is likely still cut off."""
    peer.error = _unreachable()
    cluster, node, policy, vols = _site_a(db, members=2)
    group = ConsistencyGroup()
    group.uuid = str(uuid.uuid4())
    group.cluster_id = cluster.get_id()
    group.group_name = "gitea"
    group.node_id = node.get_id()
    group.lvs_name = node.lvstore
    group.last_group_seq = 1
    group.members = {v.get_id(): {"joined_seq": 1, "removed_seq": 0} for v in vols}
    group.write_to_db(db.kv_store)
    for v in vols:
        _with(db, v, group_id=group.get_id())

    assert cgc.attach_reverse_replication(group, PEER_CLUSTER).get_id() == policy.get_id()

    assert peer.placed == []
    assert db.get_consistency_group_by_id(group.get_id()).policy_id == policy.get_id()
    for v in vols:
        stored = db.get_lvol_by_id(v.get_id())
        assert (stored.do_replicate, stored.replication_node_id) == (True, "")


# ------------------------------------------------------------------ the replication task

@pytest.fixture()
def source_side(monkeypatch):
    """The source node's part of a task: which member sends, and the chain
    walk (both node RPCs)."""
    monkeypatch.setattr(sr, "_select_source_node",
                        lambda snap, exclude=(): (sr.db.get_storage_node_by_id(snap.lvol.node_id),
                                                  "primary"))
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor",
                        lambda snode, snapshot, to_source: ("ok", None, ""))


def _unplaced(db, members=1):
    """Volumes replicating under the policy with no node on the peer yet: a
    deferred start."""
    cluster, node, policy, vols = _site_a(db, members)
    return cluster, [_with(db, v, replication_policy_id=policy.get_id(), do_replicate=True,
                           replication_mode="failover")
                     for v in vols]


def _task(db, cluster, vol):
    """A snapshot of *vol* (its record embeds *vol* as it is now) and the
    replication task of that snapshot."""
    snap = SnapShot()
    snap.uuid = str(uuid.uuid4())
    snap.snap_name = f"snap-{snap.uuid[:8]}"
    snap.snap_bdev = f"{vol.lvs_name}/SNAP_{snap.uuid[:8]}"
    snap.lvol = vol
    snap.size = vol.size
    snap.cluster_id = cluster.get_id()
    snap.pool_uuid = vol.pool_uuid
    snap.status = SnapShot.STATUS_ONLINE
    snap.snap_type = SnapShot.TYPE_INTERNAL
    snap.created_at = int(time.time())
    snap.write_to_db(db.kv_store)

    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = vol.node_id
    task.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    task.function_params = {"replicate_to_source": False, "snapshot_id": snap.get_id()}
    task.status = JobSchedule.STATUS_NEW
    task.date = int(time.time())
    task.write_to_db(db.kv_store)
    return task, snap


def test_the_task_of_an_unplaced_volume_waits_for_an_unreachable_peer(db, peer, source_side):
    peer.error = _unreachable()
    cluster, (vol,) = _unplaced(db)
    task, snap = _task(db, cluster, vol)

    sr.process_snap_replicate_start(task, snap)

    stored = db.get_task_by_id(task.uuid)
    assert stored.status == JobSchedule.STATUS_SUSPENDED, "the task waits for the peer, it is not dropped"
    assert stored.retry == 0, "a cut-off peer is a wait, not a fault of the transfer"
    assert stored.function_params.get("not_before"), "the next attempt backs off"
    assert db.get_lvol_by_id(vol.get_id()).replication_node_id == ""


def test_the_task_of_an_unplaced_volume_places_it_and_replicates(db, peer, source_side):
    peer.nodes = ["B1"]
    cluster, (vol,) = _unplaced(db)
    task, snap = _task(db, cluster, vol)

    sr.process_snap_replicate_start(task, snap)

    assert db.get_lvol_by_id(vol.get_id()).replication_node_id == "B1"
    assert [r.node_hint for r in peer.landings] == ["B1"]


def test_a_second_task_of_the_volume_keeps_the_placement_the_first_stored(db, peer, source_side):
    """Both snapshots were taken before the first placement, so both records
    embed the volume with no node. The peer would pick another node the second
    time (B2), and the volume would move mid-chain."""
    peer.nodes = ["B1", "B2"]
    cluster, (vol,) = _unplaced(db)
    first, first_snap = _task(db, cluster, vol)
    second, second_snap = _task(db, cluster, vol)

    sr.process_snap_replicate_start(first, first_snap)
    sr.process_snap_replicate_start(second, second_snap)

    assert len(peer.placed) == 1, "the volume is placed once"
    assert db.get_lvol_by_id(vol.get_id()).replication_node_id == "B1"
    assert [r.node_hint for r in peer.landings] == ["B1", "B1"]


def test_a_placement_that_lost_a_race_keeps_the_stored_node(db, peer, monkeypatch):
    """Two tasks asked the peer at the same time: the node stored first wins,
    and the second caller gets it back instead of overwriting it."""
    peer.nodes = ["B2"]
    cluster, (vol,) = _unplaced(db)
    stale = db.get_lvol_by_id(vol.get_id())

    real_place = lvol_controller._place_on_peer_preferring

    def _place_while_another_task_stores_b1(dbc, lvol, p):
        _with(db, vol, replication_node_id="B1")
        return real_place(dbc, lvol, p)

    monkeypatch.setattr(lvol_controller, "_place_on_peer_preferring",
                        _place_while_another_task_stores_b1)

    placed = lvol_controller.place_on_peer_destination(stale)

    assert placed.replication_node_id == "B1"
    assert db.get_lvol_by_id(vol.get_id()).replication_node_id == "B1"
