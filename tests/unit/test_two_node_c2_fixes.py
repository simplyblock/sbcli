"""Campaign c2 fixes of the two-node arbiter (docs/design/two-node-arbitration.md).

F1 restart precheck vs journal re-level, F2 re-grant while degraded, F3 planned
loss, F4 self-fenced nodes re-admitted, F5 grant without the survivor's report,
F6 partition recovery. Decision rules are table tests; the arbiter runs against
the in-memory DB and fake nodes of test_two_node_arbiter.
"""

import errno

import pytest

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.arbitration import decision as d
from simplyblock_core.arbitration import planned, set_dual_node
from simplyblock_core.models.arbitration import (
    ARB_DEGRADED,
    ARB_PARTITIONED,
    ARB_STEADY,
    LVS_FENCED,
    LVS_HOLDING,
    LVS_NORMAL,
    LVS_SOLO,
)
from simplyblock_core.rpc_client import RPCRemoteError
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    calls,
    env,
    event,
)
from tests.unit.test_two_node_decision import MARGIN, NOW, TTL, A, B

EXPIRED = TTL + MARGIN + 1


def nv(node_id, states=None, *, unhealthy=False, healthy=False, reachable=True,
       lease_age=0, spdk_lost=False, planned_loss=False, fenced=False):
    """A NodeView with LVS 3 (A's primary) and 4 (B's primary)."""
    return d.NodeView(node_id, reports_peer_unhealthy=unhealthy, reports_peer_healthy=healthy,
                      lvs_state=dict(states or {3: LVS_NORMAL, 4: LVS_NORMAL}),
                      reachable=reachable, lease_expires_at=NOW - lease_age + TTL,
                      positively_fenced=fenced, spdk_lost=spdk_lost, planned_loss=planned_loss)


def snap(a, b, state=ARB_STEADY, winner=""):
    return d.Snapshot(now_ms=NOW, state=state, epoch=7, a=a, b=b, leaders={3: A, 4: B},
                      lease_margin_ms=MARGIN, stable_for_ms=30_000, winner=winner)


SOLO = {3: LVS_SOLO, 4: LVS_SOLO}
HOLD = {3: LVS_HOLDING, 4: LVS_HOLDING}


@pytest.mark.parametrize("name,s,kind,winner", [
    # F5: a frozen peer: silent, lease run out; the survivor reports nothing yet
    ("F5 grant without report", snap(nv(A), nv(B, reachable=False, lease_age=EXPIRED)),
     d.V_DEGRADED, A),
    ("F5 lease inside the margin: nothing yet",
     snap(nv(A), nv(B, reachable=False, lease_age=TTL + MARGIN - 1)), d.V_NONE, ""),
    ("F5 never leased: nothing",
     snap(nv(A), d.NodeView(B, lvs_state={3: LVS_NORMAL}, reachable=False)), d.V_NONE, ""),
    ("F5 uplink loss, CP reaches neither: nothing",
     snap(nv(A, reachable=False, lease_age=EXPIRED), nv(B, reachable=False, lease_age=EXPIRED)),
     d.V_NONE, ""),
    ("F5 survivor already solo everywhere: no new grant",
     snap(nv(A, SOLO), nv(B, reachable=False, lease_age=EXPIRED)), d.V_NONE, ""),
    ("F5 survivor holding, peer silent+expired: grant",
     snap(nv(A, HOLD, unhealthy=True), nv(B, reachable=False, lease_age=EXPIRED)),
     d.V_DEGRADED, A),
    ("HTTP 500 streak, no report yet: grant at once",
     snap(nv(A), nv(B, reachable=False, lease_age=100, spdk_lost=True)), d.V_DEGRADED, A),
    # F3: the CP restarts B: B still answers, its proxy never says 500
    ("F3 planned loss, reachable", snap(nv(A), nv(B, planned_loss=True)), d.V_DEGRADED, A),
    ("F3 planned loss, survivor holding",
     snap(nv(A, HOLD, unhealthy=True), nv(B, planned_loss=True)), d.V_DEGRADED, A),
    ("F3 both planned: nobody", snap(nv(A, planned_loss=True), nv(B, planned_loss=True)),
     d.V_NONE, ""),
    ("partition still fences both",
     snap(nv(A, HOLD, unhealthy=True), nv(B, HOLD, unhealthy=True)), d.V_PARTITION, ""),
    # F2: degraded with A the winner
    ("F2 survivor holds again, peer lost again: fresh grant",
     snap(nv(A, {3: LVS_HOLDING, 4: LVS_SOLO}, unhealthy=True),
          nv(B, reachable=False, lease_age=100, spdk_lost=True), ARB_DEGRADED, A),
     d.V_DEGRADED, A),
    ("F2 survivor re-levelled to normal, peer lost again: fresh grant",
     snap(nv(A, {3: LVS_NORMAL, 4: LVS_SOLO}),
          nv(B, reachable=False, lease_age=100, spdk_lost=True), ARB_DEGRADED, A),
     d.V_DEGRADED, A),
    ("F2 survivor solo, peer still gone: verdict in force",
     snap(nv(A, SOLO, unhealthy=True), nv(B, reachable=False, lease_age=EXPIRED),
          ARB_DEGRADED, A), d.V_NONE, ""),
    ("F2 never crowns the returning loser",
     snap(nv(A, SOLO, unhealthy=True), nv(B, HOLD, unhealthy=True), ARB_DEGRADED, A),
     d.V_NONE, ""),
    ("partitioned stays put without healthy JMs",
     snap(nv(A, {3: LVS_FENCED, 4: LVS_FENCED}, unhealthy=True),
          nv(B, {3: LVS_FENCED, 4: LVS_FENCED}, unhealthy=True), ARB_PARTITIONED),
     d.V_NONE, ""),
])
def test_c2_matrix(name, s, kind, winner):
    v = d.decide(s)
    assert v.kind == kind, (name, v.reason)
    assert v.winner == winner, name
    if kind == d.V_DEGRADED:
        assert v.raise_epoch and set(v.grant) == {3, 4} and set(v.grant.values()) == {winner}


def test_planned_loss_fences_the_leaving_node_best_effort():
    v = d.decide(snap(nv(A), nv(B, planned_loss=True)))
    assert v.fence == {3: B, 4: B} and v.fence_best_effort
    # a gone (unreachable) node is not fenced at all
    v = d.decide(snap(nv(A), nv(B, reachable=False, lease_age=100, planned_loss=True)))
    assert v.fence == {} and v.kind == d.V_DEGRADED


# ---- arbiter --------------------------------------------------------------------

@pytest.fixture
def c2(env):  # noqa: F811 - the fixture imported above
    a, db, nodes, clock, readmitted = env
    a.link_ok = {(A, B): True, (B, A): True}
    a.link_probe = lambda n, p: a.link_ok[(n.get_id(), p.get_id())]
    a.reconnected = []
    a.reconnect_jms = lambda n, p: a.reconnected.append((n.get_id(), p.get_id()))
    a.run_async = lambda fn: fn()
    return a, db, nodes, clock, readmitted


def tick(a, nodes):
    return a.tick_cluster(CL, [nodes[A], nodes[B]])


def test_f5_frozen_peer_granted_on_lease_expiry_alone(c2):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    nodes[A].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    nodes[B].down = True                       # frozen: renewals time out, no 500
    clock.t += 100
    a.renew_all()
    assert tick(a, nodes).kind != d.V_DEGRADED
    clock.t += EXPIRED
    a.renew_all()
    v = tick(a, nodes)
    assert v.kind == d.V_DEGRADED and v.winner == A
    assert calls(nodes[A], "jc_grant_solo")[-1][1][1] == [3, 4]


def test_f3_planned_loss_grants_the_peer_at_once_and_ends_on_a_new_instance(c2, monkeypatch):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)

    class Cl:
        two_node_arbitration = True
    db.get_cluster_by_id = lambda _cid: Cl()
    monkeypatch.setattr(planned.time, "time", lambda: clock.t / 1000)
    assert planned.note_planned_loss(nodes[B], "restart", db=db)
    assert db.rec.planned_loss[B]["instance"] == "i-" + B
    v = tick(a, nodes)
    assert v.kind == d.V_DEGRADED and v.winner == A
    assert calls(nodes[B], "jc_fence")[-1][1][1] == [3, 4]      # fenced before the kill
    assert calls(nodes[A], "jc_grant_solo")
    # the restart flow brings B back with a new SPDK instance
    nodes[B].instance = "i-new"
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert B not in db.rec.planned_loss


def test_f3_marker_ends_when_the_stop_never_happened(c2, monkeypatch):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    nodes[B].ha = {4: (LVS_NORMAL, "healthy")}

    class Cl:
        two_node_arbitration = True
    db.get_cluster_by_id = lambda _cid: Cl()
    tick(a, nodes)
    monkeypatch.setattr(planned.time, "time", lambda: clock.t / 1000)
    planned.note_planned_loss(nodes[B], "shutdown", db=db)
    clock.t += 61_000
    a.renew_all()
    tick(a, nodes)
    assert B not in db.rec.planned_loss


def test_planned_loss_is_a_no_op_without_arbitration(env):  # noqa: F811
    a, db, nodes, clock, _ = env

    class Cl:
        two_node_arbitration = False
    db.get_cluster_by_id = lambda _cid: Cl()
    assert not planned.note_planned_loss(nodes[B], "restart", db=db)


def test_planned_loss_never_raises():
    class Broken:
        def get_cluster_by_id(self, _):
            raise RuntimeError("fdb down")
    assert planned.note_planned_loss(type("N", (), {"cluster_id": CL, "uuid": B})(),
                                     "restart", db=Broken()) is False


def test_f2_second_loss_while_degraded_gets_a_fresh_grant(c2):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()
    a.renew_all()                               # 2x HTTP 500: B lost
    nodes[A].ha = {3: (LVS_HOLDING, "unhealthy"), 4: (LVS_HOLDING, "unhealthy")}
    v = tick(a, nodes)
    assert v.kind == d.V_DEGRADED and db.rec.state == ARB_DEGRADED
    epoch = db.rec.epoch
    # B comes back, A re-levels LVS 3 to normal, then B dies again
    nodes[A].ha = {3: (LVS_HOLDING, "unhealthy"), 4: (LVS_SOLO, "unhealthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    v = tick(a, nodes)
    assert v.kind == d.V_DEGRADED and db.rec.epoch == epoch + 1
    assert calls(nodes[A], "jc_grant_solo")[-1][1][0] == epoch + 1


def test_f4_self_fenced_node_is_unfenced_once_its_remote_jm_is_healthy(c2):
    a, db, nodes, clock, readmitted = c2
    a.renew_all()
    nodes[A].ha = {3: (LVS_FENCED, "unhealthy"), 4: (LVS_FENCED, "unhealthy")}
    nodes[B].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    assert not calls(nodes[A], "jc_unfence")    # remote JM still unhealthy
    nodes[A].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    nodes[A].errors["jc_unfence"] = RPCRemoteError("resync", -errno.EAGAIN)
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert len(calls(nodes[A], "jc_unfence")) == 1
    tick(a, nodes)                              # rate limited
    assert len(calls(nodes[A], "jc_unfence")) == 1
    nodes[A].errors.clear()
    clock.t += constants.TWO_NODE_SELF_FENCE_READMIT_MS
    tick(a, nodes)
    assert calls(nodes[A], "jc_unfence")[-1][1] == (db.rec.epoch, [3, 4])
    assert a.signals[A].lvs_state == {3: LVS_NORMAL, 4: LVS_NORMAL}
    assert readmitted == [] and db.rec.state == ARB_STEADY


def test_f4_leaves_an_lvs_the_arbiter_fenced_alone(c2):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    tick(a, nodes)

    def put(f):
        f.state = ARB_DEGRADED
        f.lvs_entry(3).update(leader=A, state="solo", fenced_node=B)
    db.atomic_update(db.rec, put)
    nodes[B].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    nodes[A].ha = {3: (LVS_SOLO, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert calls(nodes[B], "jc_unfence")[-1][1][1] == [4]


def _partition(a, db, nodes):
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding"),
                 event(B, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    assert tick(a, nodes).kind == d.V_PARTITION
    for n in (A, B):
        nodes[n].ha = {3: (LVS_FENCED, "unhealthy"), 4: (LVS_FENCED, "unhealthy")}


def test_f6_partition_reconnects_then_restarts_once_when_the_link_is_back(c2):
    a, db, nodes, clock, readmitted = c2
    a.link_ok = {(A, B): False, (B, A): False}
    _partition(a, db, nodes)
    clock.t += constants.TWO_NODE_LINK_PROBE_S * 1000
    tick(a, nodes)
    assert a.reconnected == [] and db.rec.state == ARB_PARTITIONED
    # the link answers again; the remote JMs do not come back by themselves
    a.link_ok = {(A, B): True, (B, A): True}
    clock.t += constants.TWO_NODE_LINK_PROBE_S * 1000
    tick(a, nodes)                              # probe: link up
    tick(a, nodes)                              # reconnect both directions
    assert sorted(a.reconnected) == [(A, B), (B, A)]
    for _ in range(constants.TWO_NODE_PARTITION_RESTART_AFTER_S // 10 + 2):
        clock.t += 10_000
        tick(a, nodes)
    assert len(readmitted) == 1                 # the manual c2 remedy, once
    assert len(a.reconnected) > 2


def test_f6_no_restart_while_the_link_is_still_cut(c2):
    a, db, nodes, clock, readmitted = c2
    a.link_ok = {(A, B): True, (B, A): False}
    _partition(a, db, nodes)
    for _ in range(40):
        clock.t += 10_000
        tick(a, nodes)
    assert readmitted == [] and a.reconnected == []


def test_f6_healthy_jms_after_a_partition_heal_without_the_restart_flow(c2):
    a, db, nodes, clock, readmitted = c2
    _partition(a, db, nodes)
    for n in (A, B):
        nodes[n].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    clock.t += constants.TWO_NODE_STABLE_FOR_S * 1000
    tick(a, nodes)                              # HEAL
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    tick(a, nodes)
    assert calls(nodes[A], "jc_unfence") and calls(nodes[B], "jc_unfence")
    assert db.rec.state == ARB_STEADY and readmitted == []


# ---- jc_set_dual_node with remote_jm_timeout_ms ---------------------------------

class _RPC:
    def __init__(self, reject):
        self.reject, self.sent = reject, []

    def jc_set_dual_node(self, enable, **kw):
        self.sent.append(kw)
        if any(k in kw for k in self.reject):
            raise RPCRemoteError(code=-32602, message="Invalid parameters")
        return True


@pytest.mark.parametrize("reject,level,n", [
    ((), "full", 1),
    (("remote_jm_timeout_ms",), "no_remote_jm_timeout", 2),
    (("arbitration",), "plain", 3),
])
def test_set_dual_node_falls_back_for_older_nodes(reject, level, n):
    rpc = _RPC(reject)
    err, got = set_dual_node(rpc, True, True)
    assert err is None and got == level and len(rpc.sent) == n
    assert rpc.sent[0]["remote_jm_timeout_ms"] == constants.TWO_NODE_REMOTE_JM_TIMEOUT_MS


def test_set_dual_node_unarbitrated_sends_only_enable():
    rpc = _RPC(())
    assert set_dual_node(rpc, True, False) == (None, "plain") and rpc.sent == [{}]


# ---- F1: restart precheck while the leader re-levels -----------------------------

class _Leader:
    def __init__(self, answers):
        self.answers = list(answers)

    def get_id(self):
        return "leader"

    def rpc_client(self):
        return self

    def jc_compression_get_status(self, _vuid):
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return a


def test_f1_compression_precheck_retries_while_the_journal_relevels():
    t = [0.0]
    leader = _Leader([RuntimeError("Compression request failed")] * 3 + [False])
    got = storage_node_ops._jc_compression_status_with_retry(
        leader, 1, timeout_s=60, interval_s=2, sleep=lambda s: t.__setitem__(0, t[0] + s),
        clock=lambda: t[0])
    assert got is False and t[0] == 6


def test_f1_compression_precheck_gives_up_after_the_window():
    t = [0.0]
    leader = _Leader([RuntimeError("Compression request failed")] * 100)
    with pytest.raises(RuntimeError):
        storage_node_ops._jc_compression_status_with_retry(
            leader, 1, timeout_s=10, interval_s=2,
            sleep=lambda s: t.__setitem__(0, t[0] + s), clock=lambda: t[0])
    assert t[0] <= 10


# ---- ultra c2 interface: "stalled" self-fence, remote_jm_timeout_ms in status ----

def test_stalled_self_fence_event_marks_every_lvs_fenced():
    from simplyblock_core.arbitration import events as ev
    sig = ev.NodeSignals()
    for seq, vuid in ((1, 3), (2, 4)):
        assert ev.apply(sig, {"instance": "i-a", "seq": seq, "status": "ha_self_fenced",
                              "jm_vuid": vuid, "reason": "stalled", "ha_state": "fenced"})
    assert sig.lvs_state == {3: LVS_FENCED, 4: LVS_FENCED}


def test_frozen_node_that_fenced_itself_is_readmitted_while_its_peer_runs_solo(c2):
    """02a with ultra c2: the frozen node wakes, fences every LVS ("stalled")
    and does not abort; its peer was granted (DEGRADED). Once the woken node
    has resynced, it is unfenced at the current epoch like a deadline self-fence."""
    a, db, nodes, clock, readmitted = c2
    a.renew_all()
    nodes[A].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    nodes[B].down = True
    clock.t += EXPIRED
    a.renew_all()
    assert tick(a, nodes).kind == d.V_DEGRADED and db.rec.state == ARB_DEGRADED
    nodes[B].down = False                       # wakes
    db.events = [event(B, 1, "ha_self_fenced", 3, reason="stalled", ha_state="fenced"),
                 event(B, 2, "ha_self_fenced", 4, reason="stalled", ha_state="fenced")]
    nodes[B].ha = {3: (LVS_FENCED, "unhealthy"), 4: (LVS_FENCED, "unhealthy")}
    nodes[A].ha = {3: (LVS_SOLO, "unhealthy"), 4: (LVS_SOLO, "unhealthy")}
    a.renew_all()
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert not calls(nodes[B], "jc_unfence")    # not reconnected yet
    nodes[B].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert calls(nodes[B], "jc_unfence")[-1][1] == (db.rec.epoch, [3, 4])
    assert readmitted == []


def test_node_with_another_remote_jm_timeout_is_configured_again(c2):
    a, db, nodes, clock, _ = c2
    a.renew_all()
    nodes[A].ha = {3: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    n_before = len(calls(nodes[A], "jc_set_dual_node"))
    orig = nodes[A].ha_status
    nodes[A].ha_status = lambda: dict(orig(), remote_jm_timeout_ms=5000)
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    assert len(calls(nodes[A], "jc_set_dual_node")) > n_before
