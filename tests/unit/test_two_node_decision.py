"""Decision matrix of the two-node arbiter (docs/design/two-node-arbitration.md)."""

import pytest

from simplyblock_core.arbitration import decision as d
from simplyblock_core.models.arbitration import (
    ARB_DECIDING,
    ARB_DEGRADED,
    ARB_HEALING,
    ARB_PARTITIONED,
    ARB_STEADY,
    LVS_FENCED,
    LVS_HOLDING,
    LVS_NORMAL,
    LVS_SOLO,
)

NOW = 1_000_000
TTL = 1500
MARGIN = 500
A, B = "node-a", "node-b"


def nv(node_id, *, unhealthy=False, healthy=False, state=LVS_NORMAL, reachable=True,
       lease_age=None, fenced=False, preferred=False):
    """A NodeView with one LVS (jm_vuid 3). lease_age: ms since the lease was granted."""
    exp = 0 if lease_age is None else NOW - lease_age + TTL
    return d.NodeView(node_id, reports_peer_unhealthy=unhealthy, reports_peer_healthy=healthy,
                      lvs_state={3: state}, reachable=reachable, lease_expires_at=exp,
                      positively_fenced=fenced, preferred=preferred)


def snap(a, b, state=ARB_STEADY, leaders=None, healthy_since=0, healing_done=False):
    return d.Snapshot(now_ms=NOW, state=state, epoch=7, a=a, b=b,
                      leaders=leaders if leaders is not None else {3: A},
                      lease_margin_ms=MARGIN, stable_for_ms=30_000,
                      healthy_since=healthy_since, healing_done=healing_done)


@pytest.mark.parametrize("a,b,kind,state,winner", [
    # healthy pair, uplink fine: nothing
    (nv(A), nv(B), d.V_NONE, ARB_STEADY, ""),
    # uplink lost (CP reaches neither), JMs healthy: nothing (invariant 4)
    (nv(A, reachable=False, lease_age=10_000), nv(B, reachable=False, lease_age=10_000),
     d.V_NONE, ARB_STEADY, ""),
    # partition: both report, both reachable -> leader of LVS 3 (A) wins
    (nv(A, unhealthy=True, state=LVS_HOLDING), nv(B, unhealthy=True, state=LVS_HOLDING),
     d.V_PARTITION, ARB_PARTITIONED, A),
    # B down, lease expired beyond the margin -> grant A
    (nv(A, unhealthy=True, state=LVS_HOLDING),
     nv(B, reachable=False, lease_age=TTL + MARGIN + 1),
     d.V_DEGRADED, ARB_DEGRADED, A),
    # B silent but lease still inside ttl+margin -> wait
    (nv(A, unhealthy=True, state=LVS_HOLDING), nv(B, reachable=False, lease_age=TTL + MARGIN - 1),
     d.V_WAIT, ARB_DECIDING, ""),
    # B reachable but its report not in yet -> wait, never half a picture
    (nv(A, unhealthy=True, state=LVS_HOLDING), nv(B), d.V_WAIT, ARB_DECIDING, ""),
    # preferred B silent with expired lease -> wait for positive fencing (invariant 3)
    (nv(A, unhealthy=True, state=LVS_HOLDING),
     nv(B, reachable=False, lease_age=60_000, preferred=True), d.V_WAIT, ARB_DECIDING, ""),
    # preferred B positively fenced -> grant A
    (nv(A, unhealthy=True, state=LVS_HOLDING),
     nv(B, reachable=False, lease_age=60_000, preferred=True, fenced=True),
     d.V_DEGRADED, ARB_DEGRADED, A),
    # both links down: nobody reachable -> the nodes' deadline rule applies
    (nv(A, unhealthy=True, state=LVS_HOLDING, reachable=False),
     nv(B, unhealthy=True, state=LVS_HOLDING, reachable=False),
     d.V_WAIT, ARB_DECIDING, ""),
])
def test_matrix(a, b, kind, state, winner):
    v = d.decide(snap(a, b))
    assert (v.kind, v.new_state) == (kind, state), v.reason
    assert v.winner == winner
    assert v.raise_epoch == (kind in (d.V_PARTITION, d.V_DEGRADED))


def test_partition_fences_before_it_grants_and_splits_by_leader():
    a = d.NodeView(A, reports_peer_unhealthy=True, lvs_state={3: LVS_HOLDING, 4: LVS_HOLDING})
    b = d.NodeView(B, reports_peer_unhealthy=True, lvs_state={3: LVS_HOLDING, 4: LVS_HOLDING})
    v = d.decide(snap(a, b, leaders={3: A, 4: B}))
    assert v.grant == {3: A, 4: B}
    assert v.fence == {3: B, 4: A}
    # two winners: no single cluster-level loser to taint
    assert v.winner == "" and v.loser == ""


def test_degraded_never_grants_the_silent_node():
    v = d.decide(snap(nv(A, unhealthy=True, state=LVS_HOLDING),
                      nv(B, reachable=False, lease_age=TTL + MARGIN + 1)))
    assert set(v.grant.values()) == {A} and v.fence == {}


def test_verdict_in_force_until_healthy_and_stable():
    a, b = nv(A, healthy=True), nv(B, healthy=True)
    assert d.decide(snap(a, b, state=ARB_PARTITIONED)).kind == d.V_WAIT
    v = d.decide(snap(a, b, state=ARB_PARTITIONED, healthy_since=NOW - 29_999))
    assert v.kind == d.V_WAIT
    v = d.decide(snap(a, b, state=ARB_PARTITIONED, healthy_since=NOW - 30_000))
    assert (v.kind, v.new_state, v.raise_epoch) == (d.V_HEAL, ARB_HEALING, True)


def test_flapping_during_healing_starts_a_new_decision():
    v = d.decide(snap(nv(A, unhealthy=True, state=LVS_HOLDING),
                      nv(B, unhealthy=True, state=LVS_HOLDING), state=ARB_HEALING))
    assert v.kind == d.V_PARTITION


def test_healing_finishes():
    v = d.decide(snap(nv(A, healthy=True), nv(B, healthy=True), state=ARB_HEALING,
                      healing_done=True))
    assert (v.kind, v.new_state) == (d.V_DONE, ARB_STEADY)


def test_decide_is_pure():
    s = snap(nv(A, unhealthy=True, state=LVS_HOLDING), nv(B, unhealthy=True, state=LVS_HOLDING))
    assert d.decide(s) == d.decide(s)


def test_unfence_and_solo_helpers():
    a = d.NodeView(A, lvs_state={3: LVS_SOLO, 4: LVS_NORMAL})
    b = d.NodeView(B, lvs_state={3: LVS_FENCED, 4: LVS_FENCED})
    assert d.lvs_needing_unfence(a, b) == {B: [3, 4]}
    assert d.solo_lvs(a) == [3]
