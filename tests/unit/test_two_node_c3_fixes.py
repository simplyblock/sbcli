"""Campaign c3 fixes of the two-node arbiter (docs/design/two-node-arbitration.md).

F10: a node re-admitted after a fence gets the data rebuild a restart gets, is
never granted solo until it has finished, and healing waits for it. 02a: a
frozen node's RPC timeouts never delay a renewal of, or a decision about, its
peer. Decision rules are table tests; the arbiter runs against the in-memory DB
and fake nodes of test_two_node_arbiter.
"""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from simplyblock_core import constants
from simplyblock_core.arbitration import decision as d
from simplyblock_core.models.arbitration import (
    ARB_DEGRADED,
    ARB_HEALING,
    ARB_STEADY,
    LVS_FENCED,
    LVS_HOLDING,
    LVS_NORMAL,
    LVS_SOLO,
)
from simplyblock_core.models.storage_node import StorageNode
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    calls,
    env,
)
from tests.unit.test_two_node_c2_fixes import EXPIRED, nv
from tests.unit.test_two_node_decision import MARGIN, NOW, TTL, A, B

HOLD = {3: LVS_HOLDING, 4: LVS_HOLDING}
SOLO = {3: LVS_SOLO, 4: LVS_SOLO}


def snap(a, b, state=ARB_STEADY, winner="", stale=()):
    return d.Snapshot(now_ms=NOW, state=state, epoch=7, a=a, b=b, leaders={3: A, 4: B},
                      lease_margin_ms=MARGIN, stable_for_ms=30_000, winner=winner,
                      stale_nodes=tuple(stale))


@pytest.mark.parametrize("name,s,kind,winner", [
    ("stale survivor reports, peer SPDK gone: nobody granted",
     snap(nv(A, HOLD, unhealthy=True), nv(B, reachable=False, lease_age=100, spdk_lost=True),
          stale=(A,)), d.V_UNAVAILABLE, ""),
    ("stale survivor, frozen peer (no report): nobody granted",
     snap(nv(A), nv(B, reachable=False, lease_age=EXPIRED), stale=(A,)), d.V_UNAVAILABLE, ""),
    ("stale survivor, planned loss of the peer: nobody granted",
     snap(nv(A), nv(B, planned_loss=True), stale=(A,)), d.V_UNAVAILABLE, ""),
    ("the up-to-date node survives the stale one: granted as before",
     snap(nv(A, HOLD, unhealthy=True), nv(B, reachable=False, lease_age=100, spdk_lost=True),
          stale=(B,)), d.V_DEGRADED, A),
    ("degraded, winner B lost again, A still stale: unavailable, not 'in force'",
     snap(nv(A, HOLD, unhealthy=True), nv(B, reachable=False, lease_age=100, spdk_lost=True),
          ARB_DEGRADED, B, stale=(A,)), d.V_UNAVAILABLE, ""),
    ("healing (rebuild running), up-to-date node lost: unavailable",
     snap(nv(A, HOLD, unhealthy=True), nv(B, reachable=False, lease_age=100, spdk_lost=True),
          ARB_HEALING, stale=(A,)), d.V_UNAVAILABLE, ""),
    ("partition is unaffected by a pending rebuild",
     snap(nv(A, HOLD, unhealthy=True), nv(B, HOLD, unhealthy=True), stale=(A,)),
     d.V_PARTITION, ""),
])
def test_c3_matrix(name, s, kind, winner):
    v = d.decide(s)
    assert v.kind == kind, (name, v.reason)
    assert v.winner == winner, name
    if kind == d.V_UNAVAILABLE:
        assert v.grant == {} and v.fence == {} and not v.raise_epoch, name


# ---- arbiter: F10 ------------------------------------------------------------------

def tick(a, nodes):
    return a.tick_cluster(CL, [nodes[A], nodes[B]])


def _both_normal(nodes):
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}


def _b_frozen_and_granted(a, db, nodes, clock):
    """02b: B freezes, A is granted on B's expired lease."""
    a.renew_all()
    _both_normal(nodes)
    tick(a, nodes)
    nodes[B].down = True
    clock.t += EXPIRED
    a.renew_all()
    v = tick(a, nodes)
    assert v.kind == d.V_DEGRADED and v.winner == A


def _b_back_self_fenced_and_readmitted(a, db, nodes, clock):
    """B wakes, fences itself ("stalled"), its remote JM recovers; A solo."""
    nodes[B].down = False
    nodes[B].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_FENCED, "healthy")}
    nodes[A].ha = {3: (LVS_SOLO, "healthy"), 4: (LVS_SOLO, "healthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    a.renew_all()
    tick(a, nodes)


def test_f10_loser_marked_on_the_grant_and_rebuilt_only_once_back(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _b_frozen_and_granted(a, db, nodes, clock)
    assert B in db.rec.rebuild and db.rec.rebuild[B]["queued"] is False
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert db.rebuild_queued == []               # B is still gone: nothing to copy to
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert calls(nodes[B], "jc_unfence")         # F4 re-admission (unfence)
    assert db.rebuild_queued == [B]              # ... now with the rebuild
    assert db.rec.rebuild[B]["queued"] is True


def test_f10_mark_clears_only_after_the_queued_tasks_finished(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    db.open_tasks = 3
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert B in db.rec.rebuild
    db.open_tasks = 0
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert db.rec.rebuild == {}


def test_f10_nothing_queued_is_not_complete_and_is_retried(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    queued = []
    a.schedule_rebuild = lambda n: queued.append(n.get_id()) and False   # nothing queued
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    assert queued == [B]
    for _ in range(3):
        clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
        tick(a, nodes)
    assert B in db.rec.rebuild and queued == [B]  # "no open tasks" is not "rebuilt"
    clock.t += constants.TWO_NODE_REBUILD_SCHEDULE_RETRY_S * 1000
    tick(a, nodes)
    assert queued == [B, B]


def test_f10_offline_node_is_not_queued_until_online(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _b_frozen_and_granted(a, db, nodes, clock)
    nodes[B].status = StorageNode.STATUS_OFFLINE     # in the restart flow
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    nodes[B].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert db.rebuild_queued == []
    nodes[B].status = StorageNode.STATUS_ONLINE
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    tick(a, nodes)
    assert db.rebuild_queued == [B]


def test_f10_up_to_date_node_lost_during_rebuild_grants_nobody(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    db.open_tasks = 2                             # rebuild running
    grants_before = len(calls(nodes[B], "jc_grant_solo"))
    # 03: the up-to-date node A goes away (reboot): HTTP 500 streak
    nodes[A].http500 = True
    a.renew_all()
    a.renew_all()
    nodes[B].ha = {3: (LVS_HOLDING, "unhealthy"), 4: (LVS_HOLDING, "unhealthy")}
    clock.t += constants.TWO_NODE_RECONCILE_MS
    v = tick(a, nodes)
    assert v.kind == d.V_UNAVAILABLE
    assert len(calls(nodes[B], "jc_grant_solo")) == grants_before
    assert db.rec.data_unavailable["node"] == B
    # reported once per episode
    unavailable = dict(db.rec.data_unavailable)
    tick(a, nodes)
    assert db.rec.data_unavailable == unavailable


def test_f10_healing_waits_for_the_rebuild(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    _b_frozen_and_granted(a, db, nodes, clock)
    _b_back_self_fenced_and_readmitted(a, db, nodes, clock)
    db.open_tasks = 1
    _both_normal(nodes)
    clock.t += constants.TWO_NODE_RECONCILE_MS
    tick(a, nodes)
    clock.t += constants.TWO_NODE_STABLE_FOR_S * 1000
    a.renew_all()
    tick(a, nodes)                                # HEAL
    clock.t += constants.TWO_NODE_RECONCILE_MS
    a.renew_all()
    tick(a, nodes)
    assert db.rec.state == ARB_HEALING            # rebuild still running
    db.open_tasks = 0
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    a.renew_all()
    tick(a, nodes)
    tick(a, nodes)
    assert db.rec.rebuild == {} and db.rec.state == ARB_STEADY


def test_f10_partition_needs_no_rebuild(env):  # noqa: F811
    from tests.unit.test_two_node_arbiter import event
    a, db, nodes, clock, _ = env
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding"),
                 event(B, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    assert tick(a, nodes).kind == d.V_PARTITION
    assert db.rec.rebuild == {}                   # fence_both granted nobody


# ---- arbiter: 02a, a frozen node does not delay its peer --------------------------

def test_02a_frozen_status_poll_does_not_delay_the_tick(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    pool = ThreadPoolExecutor(max_workers=4)
    try:
        a.rpc_pool = pool
        a.renew_all()
        for f in list(a.inflight.values()):
            f.result(timeout=5)
        _both_normal(nodes)
        tick(a, nodes)
        for f in list(a.inflight.values()):
            f.result(timeout=5)
        nodes[B].slow = 1.0                       # every call blocks its full 1 s timeout
        clock.t += constants.TWO_NODE_RECONCILE_MS
        started = time.monotonic()
        tick(a, nodes)
        took = time.monotonic() - started
        assert took < 0.6, took                   # waited at most TWO_NODE_POLL_WAIT_MS
        assert ("status", B) in a.inflight        # still running, applied later
        before = len(calls(nodes[B], "jc_ha_status"))
        clock.t += constants.TWO_NODE_RECONCILE_MS
        tick(a, nodes)
        assert len(calls(nodes[B], "jc_ha_status")) == before   # never started twice
    finally:
        nodes[B].slow = 0.0
        pool.shutdown(wait=True)


def test_02a_frozen_renewal_does_not_delay_the_peer_renewal(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    pool = ThreadPoolExecutor(max_workers=4)
    try:
        a.rpc_pool = pool
        a.capable = {A: True, B: True}            # probed before B froze
        nodes[B].slow = 1.0
        a.renew_all()
        started = time.monotonic()
        a.inflight[("renew", A)].result(timeout=5)
        assert time.monotonic() - started < 0.5
        assert a.lease_granted_at.get(A) == clock.t
        assert not a.inflight[("renew", B)].done()
        a.renew_all()                             # B's renewal still in flight: skipped
        a.inflight[("renew", B)].result(timeout=5)
        assert len(calls(nodes[B], "jc_lease_renew")) == 1
    finally:
        nodes[B].slow = 0.0
        pool.shutdown(wait=True)


def test_02a_grant_comes_at_lease_expiry_plus_margin(env):  # noqa: F811
    """With concurrent polls the grant lands on the first tick after
    last renewal + TTL + margin, whatever the frozen node's timeouts."""
    a, db, nodes, clock, _ = env
    pool = ThreadPoolExecutor(max_workers=4)
    try:
        a.rpc_pool = pool
        a.renew_all()
        for f in list(a.inflight.values()):
            f.result(timeout=5)
        _both_normal(nodes)
        tick(a, nodes)
        last_renew = a.lease_granted_at[B]
        nodes[B].slow = 0.3
        clock.t = last_renew + TTL + MARGIN       # not beyond the margin yet
        assert tick(a, nodes).kind != d.V_DEGRADED
        clock.t = last_renew + TTL + MARGIN + constants.TWO_NODE_DECISION_TICK_MS
        a.reachable[B] = False                    # its renewals time out meanwhile
        v = tick(a, nodes)
        assert v.kind == d.V_DEGRADED and v.winner == A
    finally:
        nodes[B].slow = 0.0
        pool.shutdown(wait=True)


def test_degraded_record_lists_the_stale_node_in_the_snapshot(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    _b_frozen_and_granted(a, db, nodes, clock)
    s = a.snapshot(db.rec, [nodes[A], nodes[B]])
    assert s.stale_nodes == (B,) and db.rec.state == ARB_DEGRADED
