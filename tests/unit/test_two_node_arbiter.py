"""The two-node arbiter against an in-memory DB and fake nodes."""

import errno

import pytest

from simplyblock_core import constants
from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.models.arbitration import ArbitrationEvent, ClusterArbitration
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCHTTPError, RPCRemoteError

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
            if self.node.http500 and name != "supports_two_node_arbitration":
                # the node's proxy runs, SPDK behind it does not
                raise RPCHTTPError("HTTP 500 from remote", status_code=500)
            err = self.node.errors.get(name)
            if err:
                raise err
            if name == "supports_two_node_arbitration":
                return True
            if name == "jc_ha_status":
                if self.node.ha is None:
                    raise RuntimeError("no status in this test")
                return self.node.ha_status()
            if name == "jc_set_dual_node":
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
        self.http500 = False
        #: jc_ha_status as the node reports it: None = not answered (the
        #: event-driven view stays), else {jm_vuid: (state, remote_jm_health)}
        self.ha = None
        self.instance, self.seq, self.arbitration = "i-" + nid, 0, True

    def ha_status(self):
        return {"instance": self.instance, "seq": self.seq, "arbitration": self.arbitration,
                "epoch": 0,
                "lvs": [{"jm_vuid": v, "state": st, "remote_jm": {"health": h}}
                        for v, (st, h) in sorted(self.ha.items())]}

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
        self.logged = []

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
    db.leadership_calls = []

    def reconcile_leadership(cluster_id, nodes_, skip_jm_vuids=(), confirmed=()):
        db.leadership_calls.append((cluster_id, set(skip_jm_vuids), set(confirmed)))
        return db.leadership_result

    db.leadership_result = ([], [])
    a = arb.Arbiter(db, clock=clock, readmit=readmitted.append,
                    log_event=lambda item, event: db.logged.append((item.node_id, event["status"])),
                    reconcile_leadership=reconcile_leadership)
    return a, db, nodes, clock, readmitted


def event(node, seq, status, vuid, **kw):
    e = ArbitrationEvent()
    e.cluster_id, e.node_id, e.instance, e.seq, e.received_at = CL, node, "i-" + node, seq, seq
    e.payload = dict(status=status, jm_vuid=vuid, seq=seq, instance="i-" + node, **kw)
    return e


def calls(node, name):
    return [c for c in node.calls if c[0] == name]


def test_partition_fences_both_and_persists_before_rpcs(env):
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
    assert db.rec.verdicts[-1]["fence_all"] == {"3": [A, B], "4": [A, B]}
    # fence_both: IO stops on a partition; both nodes fenced for both LVS, nobody granted
    assert calls(nodes[A], "jc_fence")[0][1] == (1, [3, 4])
    assert calls(nodes[B], "jc_fence")[0][1] == (1, [3, 4])
    assert not calls(nodes[A], "jc_grant_solo") and not calls(nodes[B], "jc_grant_solo")
    assert db.rec.taint_requests == []


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


def test_failed_fence_is_resent_until_it_lands(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].errors["jc_fence"] = RuntimeError("boom")
    db.events = [event(A, 1, "remote_jm_unhealthy", 3), event(B, 1, "remote_jm_unhealthy", 3)]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert CL in a.undelivered
    nodes[B].errors.clear()
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert CL not in a.undelivered
    assert calls(nodes[B], "jc_fence")[-1][1] == (1, [3, 4])
    assert not calls(nodes[A], "jc_grant_solo")


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


def test_healing_after_a_partition_unfences_without_the_restart_flow(env):
    """Campaign c2, F6: fence_both granted nobody, so nobody wrote alone and
    no node is handed to the restart flow; unfence (after resync) is the heal."""
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
    assert calls(nodes[B], "jc_unfence")
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.rec.state == "steady" and db.rec.taint_requests == []
    assert readmitted == []


def test_not_arbitrated_without_the_flag_or_with_three_nodes(env):
    a, db, nodes, clock, _ = env
    Cluster.two_node_arbitration = False
    try:
        assert a.eligible(Cluster()) is None
    finally:
        Cluster.two_node_arbitration = True
    nodes["node-c"] = Node("node-c", 5)
    assert a.eligible(Cluster()) is None


def test_override_grants_the_named_winner_even_if_the_fence_fails(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.atomic_update(db.rec, lambda f: setattr(f, "override", {"winner": A, "reason": "BMC confirmed B off"}))
    nodes[B].down = True
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "degraded" and v.winner == A
    assert calls(nodes[A], "jc_grant_solo")
    assert db.rec.override == {}
    assert "operator override" in db.rec.verdicts[-1].get("reason", "")


def test_override_naming_a_non_member_is_dropped(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.atomic_update(db.rec, lambda f: setattr(f, "override", {"winner": "node-x", "reason": "typo in the request"}))
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "none" and db.rec.override == {}
    assert not calls(nodes[A], "jc_grant_solo") and not calls(nodes[B], "jc_grant_solo")


def test_consumed_events_are_logged_once(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3), event(A, 1, "remote_jm_unhealthy", 3),
                 event(B, 1, "remote_jm_healthy", 4)]
    a.consume_events(CL)
    assert db.logged == [(A, "remote_jm_unhealthy"), (B, "remote_jm_healthy")]


def legacy(node, status, vuid):
    e = event(node, 0, status, vuid)
    e.instance, e.seq = "legacy", 1_700_000_000_000_000_000
    e.payload = dict(status=status, jm_vuid=vuid)
    return e


def test_legacy_copies_are_dropped_for_capable_nodes_and_never_reset_the_stream(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 5, "remote_jm_healthy", 3)]
    a.consume_events(CL)
    a.capable[A] = True
    db.events = [legacy(A, "remote_jm_unhealthy", 3)]
    a.consume_events(CL)
    assert a.signals[A].instance == "i-" + A and a.signals[A].last_seq == 5
    assert not a.signals[A].reports_peer_unhealthy()
    # a node without the protocol is arbitrated from its legacy events
    a.capable[B] = False
    db.events = [legacy(B, "remote_jm_unhealthy", 4)]
    a.consume_events(CL)
    assert a.signals[B].reports_peer_unhealthy() and a.signals[B].instance == ""
    assert (B, "remote_jm_unhealthy") not in db.logged


# ---- campaign c1 fixes -------------------------------------------------------

def test_no_preferred_node_anywhere(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert not db.rec.preferred_node
    for n in nodes.values():
        for _, args, kwargs in calls(n, "jc_set_dual_node"):
            assert args == (True,)
            assert kwargs == {"arbitration": True, "hold_ms": constants.TWO_NODE_HOLD_MS,
                              "lease_ttl_ms": constants.TWO_NODE_LEASE_TTL_MS,
                          "remote_jm_timeout_ms": constants.TWO_NODE_REMOTE_JM_TIMEOUT_MS}


def test_silent_peer_is_granted_after_its_lease_whichever_node_it_is(env):
    """The old preferred-node rule withheld the grant when the silent peer was
    the preferred node (01b/02b). Now any peer whose lease expired counts."""
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].down = True
    db.events = [event(B, 1, "remote_jm_unhealthy", 4, ha_state="holding")]
    a.renew_all()
    clock.t += constants.TWO_NODE_LEASE_TTL_MS + constants.TWO_NODE_LEASE_MARGIN_MS + 1
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "degraded" and v.winner == B


def test_http500_twice_marks_the_node_lost_and_the_peer_is_granted_at_once(env):
    a, db, nodes, clock, _ = env
    a.renew_all()                       # B holds a valid lease from here
    nodes[B].http500 = True             # SPDK died behind a running proxy
    a.renew_all()
    a.renew_all()
    assert a.spdk_lost.get(B)
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    # no clock advance: B's lease is still valid, the 500 streak is the evidence
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "degraded" and v.winner == A
    assert calls(nodes[A], "jc_grant_solo")


def test_a_single_transient_500_grants_nothing(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()                       # one 500
    nodes[B].http500 = False
    a.renew_all()                       # answers again: the streak resets
    nodes[B].http500 = True
    a.renew_all()                       # one 500 again, not two in a row
    nodes[B].http500 = False            # and the next poll answers
    assert not a.spdk_lost.get(B)
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    v = a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert v.kind == "wait"
    assert not calls(nodes[A], "jc_grant_solo")


def test_a_node_lost_by_http500_is_fenced_when_it_comes_back(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    epoch = db.rec.epoch
    nodes[A].ha = {3: ("solo", "unhealthy")}      # A leads LVS 3 alone now
    nodes[B].http500 = False
    nodes[B].ha = {}                              # answers again
    nodes[B].calls.clear()
    a.renew_all()
    names = [c[0] for c in nodes[B].calls]
    # fenced for A's solo LVS, at a new epoch, BEFORE it gets a lease again
    assert names.index("jc_fence") < names.index("jc_lease_renew")
    assert calls(nodes[B], "jc_fence")[0][1] == (epoch + 1, [3])
    assert not a.spdk_lost.get(B)
    assert a.signals[B].lvs_state[3] == "fenced"


def test_no_epoch_is_raised_while_a_lost_node_stays_gone(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()
    a.renew_all()
    nodes[A].ha = {3: ("solo", "unhealthy")}
    epoch = db.rec.epoch
    for _ in range(5):
        a.renew_all()
    assert db.rec.epoch == epoch and a.spdk_lost.get(B)
    assert not calls(nodes[B], "jc_fence")
    # once only
    nodes[B].calls.clear()
    a.renew_all()
    assert not calls(nodes[B], "jc_fence")


def test_a_lost_node_back_without_any_solo_peer_is_not_fenced(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()
    a.renew_all()
    nodes[A].ha = {3: ("normal", "healthy")}
    nodes[B].http500 = False
    nodes[B].ha = {}
    a.renew_all()
    assert not calls(nodes[B], "jc_fence") and not a.spdk_lost.get(B)


def test_view_is_reconciled_from_ha_status_never_append_only(env):
    """08 run 1: an append-only view kept a stale "solo" and the arbiter fenced
    the legitimate survivor. The node's own jc_ha_status replaces the view."""
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 1, "ha_solo", 3, ha_state="solo")]
    a.consume_events(CL)
    assert a.signals[A].lvs_state == {3: "solo"}
    nodes[A].ha = {4: ("normal", "healthy")}
    nodes[A].seq = 1
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert a.signals[A].lvs_state == {4: "normal"}
    # every poll, not only once (EAGAIN: not resynced, so F4 leaves it fenced)
    nodes[A].errors["jc_unfence"] = RPCRemoteError("resync", -errno.EAGAIN)
    nodes[A].ha = {4: ("fenced", "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert a.signals[A].lvs_state == {4: "fenced"}


def test_instance_change_or_sequence_gap_forces_an_immediate_resync(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].ha = {3: ("normal", "healthy")}
    nodes[A].seq = 1
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    nodes[A].calls.clear()
    # a gap: seq 5 after 1 (events 2..4 lost)
    db.events = [event(A, 5, "ha_solo", 3, ha_state="solo")]
    nodes[A].seq = 5
    a.tick_cluster(CL, [nodes[A], nodes[B]])      # inside the reconcile interval
    assert calls(nodes[A], "jc_ha_status")
    assert a.signals[A].lvs_state == {3: "normal"}
    # a new instance (node restarted): resync and configure again
    nodes[A].calls.clear()
    nodes[A].instance, nodes[A].seq = "i-new", 1
    e = event(A, 1, "remote_jm_healthy", 3)
    e.instance = "i-new"
    e.payload["instance"] = "i-new"
    db.events = [e]
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert calls(nodes[A], "jc_ha_status") and calls(nodes[A], "jc_set_dual_node")


def test_reconnect_forces_an_immediate_resync(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].ha = {3: ("normal", "healthy")}
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    nodes[A].down = True
    a.renew_all()
    nodes[A].down = False
    a.renew_all()
    assert A in a.needs_resync
    nodes[A].calls.clear()
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert calls(nodes[A], "jc_ha_status") and A not in a.needs_resync


def test_arbitration_switched_off_on_the_node_is_pushed_again(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].ha = {3: ("normal", "healthy")}
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    pushed = len(calls(nodes[A], "jc_set_dual_node"))
    nodes[A].arbitration = False                  # e.g. an old restart flow
    clock.t += constants.TWO_NODE_RECONCILE_MS
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert len(calls(nodes[A], "jc_set_dual_node")) == pushed + 1


def test_leadership_reconciliation_runs_only_when_steady(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].ha = {3: ("normal", "healthy"), 4: ("solo", "healthy")}
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.leadership_calls and db.leadership_calls[-1][1] == {4}
    # a second pass only after the interval; leaderless names carried over
    db.leadership_result = ([], ["LVS_3"])
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    db.leadership_result = (["LVS_3"], ["LVS_3"])
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert db.leadership_calls[-1][2] == {"LVS_3"}
    assert db.rec.leadership_restored_at == clock.t
    # not while a verdict is in force
    count = len(db.leadership_calls)
    db.atomic_update(db.rec, lambda f: setattr(f, "state", "degraded"))
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert len(db.leadership_calls) == count


def test_ha_normal_demoted_event_resets_solo(env):
    a, db, nodes, clock, _ = env
    db.events = [event(A, 1, "ha_solo", 3, ha_state="solo"),
                 event(A, 2, "ha_normal", 3, reason="demoted", ha_state="normal")]
    a.consume_events(CL)
    assert a.signals[A].lvs_state == {3: "normal"}


def test_ha_status_without_a_preferred_field_is_reconciled(env):
    a, db, nodes, clock, _ = env
    a.renew_all()
    nodes[A].ha = {3: ("solo", "unhealthy")}
    status = nodes[A].ha_status()
    assert "preferred" not in status
    a.tick_cluster(CL, [nodes[A], nodes[B]])
    assert a.signals[A].lvs_state == {3: "solo"}
