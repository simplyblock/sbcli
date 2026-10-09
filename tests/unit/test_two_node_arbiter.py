"""The two-node arbiter against an in-memory DB and fake nodes."""

import errno

import pytest

from simplyblock_core import constants
from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.models.arbitration import ArbitrationEvent, ClusterArbitration
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

A, B, CL = "node-a", "node-b", "cl-1"


class Clock:
    def __init__(self):
        self.t = 1_000_000

    def __call__(self):
        return self.t


class FakeRPC:
    def __init__(self, node):
        self.node = node

    def __getattr__(self, name):
        def call(*args, **kwargs):
            self.node.calls.append((name, args, kwargs))
            if self.node.down:
                raise ConnectionError("down")
            err = self.node.errors.get(name)
            if err:
                raise err
            if name == "supports_two_node_arbitration":
                return True
            return {}
        return call


class Node:
    def __init__(self, nid, jm_vuid):
        self.uuid = nid
        self.jm_vuid = jm_vuid
        self.cluster_id = CL
        self.status = StorageNode.STATUS_ONLINE
        self.remediation_fenced = False
        self.calls, self.errors, self.down = [], {}, False

    def get_id(self):
        return self.uuid

    def rpc_client(self, **_):
        return FakeRPC(self)


class Cluster:
    two_node_arbitration = True

    def get_id(self):
        return CL


class FakeDB:
    kv_store = None

    def __init__(self, nodes):
        self.nodes = nodes
        self.rec = None
        self.events = []

    def get_clusters(self):
        return [Cluster()]

    def get_storage_nodes_by_cluster_id(self, _cid):
        return list(self.nodes.values())

    def get_storage_node_by_id(self, nid):
        return self.nodes[nid]

    def get_cluster_arbitration(self, _cid):
        return self.rec

    def get_arbitration_events(self, _cid, limit=0):
        out, self.events = self.events, []
        return out

    def atomic_update(self, obj, fn):
        fresh = ClusterArbitration(self.rec.to_dict())
        if fn(fresh) is False:
            return self.rec
        self.rec = fresh
        return fresh


@pytest.fixture
def env(monkeypatch):
    nodes = {A: Node(A, 3), B: Node(B, 4)}
    db = FakeDB(nodes)
    monkeypatch.setattr(ClusterArbitration, "write_to_db", lambda self, kv=None: setattr(db, "rec", self))
    monkeypatch.setattr(ArbitrationEvent, "remove", lambda self, kv: None)
    clock = Clock()
    readmitted = []
    a = arb.Arbiter(db, clock=clock, readmit=readmitted.append)
    return a, db, nodes, clock, readmitted


def event(node, seq, status, vuid, **kw):
    e = ArbitrationEvent()
    e.cluster_id, e.node_id, e.instance, e.seq, e.received_at = CL, node, "i-" + node, seq, seq
    e.payload = dict(status=status, jm_vuid=vuid, seq=seq, instance="i-" + node, **kw)
    return e


def calls(node, name):
    return [c for c in node.calls if c[0] == name]


def test_partition_fences_then_grants_and_persists_before_rpcs(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding"),
                 event(A, 2, "remote_jm_unhealthy", 4, ha_state="holding"),
                 event(B, 1, "remote_jm_unhealthy", 3, ha_state="holding"),
                 event(B, 2, "remote_jm_unhealthy", 4, ha_state="holding")]
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "partition"
    assert db.rec.state == "partitioned" and db.rec.epoch == 1
    assert db.rec.verdicts[-1]["epoch"] == 1
    # each node leads its own LVS: A keeps 3, B keeps 4; each fenced for the other
    assert calls(nodes[A], "jc_fence")[0][1] == (1, [4])
    assert calls(nodes[A], "jc_grant_solo")[0][1][:2] == (1, [3])
    assert calls(nodes[B], "jc_fence")[0][1] == (1, [3])


def test_degraded_waits_for_the_lease_then_grants_the_survivor(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].down = True
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    a.renew_all()
    assert a.tick_cluster(CL, [nodes[A], nodes[B]]).kind == "wait"
    clock.t += constants.TWO_NODE_LEASE_TTL_MS + constants.TWO_NODE_LEASE_MARGIN_MS + 1
    a.renew_all()
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "degraded" and v.winner == A
    assert calls(nodes[A], "jc_grant_solo")


def test_failed_fence_withholds_the_grant_and_retries(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].errors["jc_fence"] = RuntimeError("boom")
    db.events = [event(A, 1, "remote_jm_unhealthy", 3), event(B, 1, "remote_jm_unhealthy", 3)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert not calls(nodes[A], "jc_grant_solo")
    nodes[B].errors.clear()
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert calls(nodes[A], "jc_grant_solo")


def test_stale_epoch_on_renew_raises_the_epoch(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_healthy", 3, epoch=41)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    nodes[A].errors["jc_lease_renew"] = RPCRemoteError("stale", -errno.ESTALE)
    a.renew_all()
    assert db.rec.epoch >= 42


def test_restart_resends_the_last_verdict_with_a_higher_epoch(env):
    a, db, nodes, clock, readmitted = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3), event(B, 1, "remote_jm_unhealthy", 3)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    epoch = db.rec.epoch
    fresh = arb.Arbiter(db, clock=clock, readmit=readmitted.append)
    fresh.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.rec.epoch == epoch + 1
    assert calls(nodes[B], "jc_fence")[-1][1][0] == epoch + 1


def test_healing_unfences_after_resync_and_readmits_once(env):
    a, db, nodes, clock, readmitted = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3), event(B, 1, "remote_jm_unhealthy", 3)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    db.events = [event(B, 2, "ha_fenced", 3), event(A, 2, "remote_jm_healthy", 3),
                 event(B, 3, "remote_jm_healthy", 3)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    clock.t += constants.TWO_NODE_STABLE_FOR_S * 1000
    nodes[B].errors["jc_unfence"] = RPCRemoteError("resync", -errno.EAGAIN)
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.rec.state == "healing" and not readmitted
    nodes[B].errors.clear()
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert [n.get_id() for n in readmitted] == [B]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.rec.state == "steady" and db.rec.taint_requests == []
    assert len(readmitted) == 1


def test_not_arbitrated_without_the_flag_or_with_three_nodes(env):
    a, db, nodes, clock, _ = env
    Cluster.two_node_arbitration = False
    try:
        assert a.eligible(Cluster()) is None
    finally:
        Cluster.two_node_arbitration = True
    nodes["node-c"] = Node("node-c", 5)
    assert a.eligible(Cluster()) is None
