"""Campaign c7, F16: the arbiter's decision thread never blocks on a node.

c7/02b: the leadership reconciliation (B6) probed a node with ``get_lvstore``
on the decision thread just as that node froze (its proxy up, SPDK not
answering). The call waited its 5 s timeout, the grant to the healthy peer on
the frozen node's expired lease never came, the peer fenced itself at its
hold deadline and the client got EIO. Every node RPC the tick needs now runs
on a pool and is applied on a later tick; a node whose every call blocks may
not delay the grant to its peer on any path.

The harness: real thread pools, a fake node whose every RPC blocks
``SLOW`` seconds (frozen SPDK), and a per-tick wall-clock budget well below it.
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
    LVS_NORMAL,
)
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    calls,
    env,
)
from tests.unit.test_two_node_decision import MARGIN, TTL, A, B

#: a frozen node: every RPC blocks this long, then times out
SLOW = 2.0
#: one tick may take at most this long while a node is frozen
BUDGET = 0.5


def tick(a, nodes):
    return a.tick_cluster(CL, [nodes[A], nodes[B]])


def _timed_tick(a, nodes):
    started = time.monotonic()
    v = tick(a, nodes)
    took = time.monotonic() - started
    assert took < BUDGET, "tick blocked %.2fs on the frozen node" % took
    return v


@pytest.fixture
def pools(env):  # noqa: F811
    a = env[0]
    rpc, work = ThreadPoolExecutor(max_workers=4), ThreadPoolExecutor(max_workers=4)
    a.rpc_pool, a.work_pool = rpc, work
    yield env
    for n in env[2].values():
        n.slow = 0.0
    rpc.shutdown(wait=True)
    work.shutdown(wait=True)


def _settle(a):
    for f in list(a.inflight.values()):
        f.result(timeout=10)


def _steady(a, nodes):
    a.renew_all()
    _settle(a)
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    _settle(a)
    tick(a, nodes)


def _grant_on_lease_expiry(a, db, nodes, clock):
    """B is frozen: its renewals time out; the grant to A must land on the
    first tick after B's last renewal + TTL + margin, within the budget."""
    last_renew = a.lease_granted_at[B]
    a.reachable[B] = False                         # its renewals time out meanwhile
    clock.t = last_renew + TTL + MARGIN + constants.TWO_NODE_DECISION_TICK_MS
    v = _timed_tick(a, nodes)
    assert v.kind == d.V_DEGRADED and v.winner == A
    assert calls(nodes[A], "jc_grant_solo")
    return v


# ---- the c7/02b path: a B6 probe in flight on the frozen node ----------------------

def test_b6_probe_of_a_frozen_node_does_not_delay_the_grant(pools):
    a, db, nodes, clock, _ = pools

    def probe_everyone(cluster_id, nodes_, skip_jm_vuids=(), confirmed=()):
        for n in nodes_:
            try:
                n.rpc_client(timeout=5, retry=1).get_lvstore("LVS")
            except Exception:                      # noqa: BLE001 - "unknown"
                pass
        return [], []
    a.reconcile_leadership = probe_everyone
    _steady(a, nodes)
    nodes[B].slow = SLOW                           # B freezes ...
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    _timed_tick(a, nodes)                          # ... just as the B6 pass starts
    assert ("leadership", CL) in a.inflight        # running on the work pool
    _grant_on_lease_expiry(a, db, nodes, clock)
    assert db.rec.state == ARB_DEGRADED


def test_b6_pass_skipped_while_a_node_is_unreachable_or_lost(pools):
    a, db, nodes, clock, _ = pools
    _steady(a, nodes)
    db.leadership_calls.clear()
    a.reachable[B] = False
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    tick(a, nodes)
    assert db.leadership_calls == []
    a.reachable[B] = True
    a.spdk_lost[B] = True
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    tick(a, nodes)
    assert db.leadership_calls == []


def test_b6_result_applied_on_a_later_tick(pools):
    a, db, nodes, clock, _ = pools
    _steady(a, nodes)
    db.leadership_result = ([], ["LVS_1"])
    clock.t += constants.TWO_NODE_LEADERSHIP_RECONCILE_S * 1000
    tick(a, nodes)
    _settle(a)
    tick(a, nodes)
    assert a.leaderless_seen[CL] == {"LVS_1"}


def test_default_reconcile_probes_with_the_short_timeout(monkeypatch):
    from simplyblock_core import storage_node_ops
    from simplyblock_core.arbitration import arbiter as arb
    seen = {}

    def fake(cluster_id, nodes, skip_jm_vuids=(), confirmed=(), probe_timeout=5, probe_retry=1):
        seen.update(timeout=probe_timeout, retry=probe_retry)
        return [], []
    monkeypatch.setattr(storage_node_ops, "reconcile_lvs_leadership", fake)
    arb.default_reconcile_leadership(CL, [])
    assert seen == {"timeout": constants.TWO_NODE_LEADER_PROBE_TIMEOUT_S, "retry": 0}


def test_leader_probe_defaults_unchanged_for_other_callers():
    import inspect

    from simplyblock_core import storage_node_ops
    sig = inspect.signature(storage_node_ops._lvs_leader_probe)
    assert sig.parameters["timeout"].default == 5 and sig.parameters["retry"].default == 1
    sig = inspect.signature(storage_node_ops.reconcile_lvs_leadership)
    assert sig.parameters["probe_timeout"].default == 5
    assert sig.parameters["probe_retry"].default == 1


# ---- configure ----------------------------------------------------------------

def test_configure_of_a_frozen_node_runs_off_the_loop(pools):
    a, db, nodes, clock, _ = pools
    _steady(a, nodes)
    a.configured.pop(B, None)                      # e.g. a new SPDK instance
    nodes[B].slow = SLOW
    _timed_tick(a, nodes)
    assert ("configure", B) in a.inflight
    _grant_on_lease_expiry(a, db, nodes, clock)


def test_configure_applied_with_the_instance_it_was_started_for(env):  # noqa: F811
    a, db, nodes, clock, _ = env                   # InlinePool: applied at once
    a.renew_all()
    tick(a, nodes)
    assert a.configured.get(A) == a.signals.get(A).instance if A in a.signals else True
    assert calls(nodes[A], "jc_set_dual_node") and calls(nodes[B], "jc_set_dual_node")


# ---- heal ---------------------------------------------------------------------

def test_heal_unfence_of_a_frozen_node_does_not_delay_the_grant(pools):
    a, db, nodes, clock, _ = pools
    _steady(a, nodes)
    db.rec.state = ARB_HEALING
    a.signals[B].lvs_state[3] = LVS_FENCED
    nodes[B].slow = SLOW
    _timed_tick(a, nodes)
    assert ("unfence", B) in a.inflight
    db.rec.state = ARB_STEADY
    _grant_on_lease_expiry(a, db, nodes, clock)


def test_heal_unfence_applied_on_a_later_tick(pools):
    a, db, nodes, clock, readmitted = pools
    _steady(a, nodes)
    db.rec.state = ARB_HEALING
    a.signals[B].lvs_state[3] = LVS_FENCED
    nodes[B].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    _settle(a)
    tick(a, nodes)
    assert calls(nodes[B], "jc_unfence")
    assert ("unfence", B) not in a.inflight


# ---- self-fence re-admission and its device check ------------------------------

def test_device_check_of_a_frozen_node_does_not_delay_the_grant(pools):
    a, db, nodes, clock, _ = pools

    def devices_ok(_db, node):
        node.rpc_client(timeout=3, retry=0).bdev_get("alceml_x")
        return True
    a.devices_ok = devices_ok
    _steady(a, nodes)
    a.signals[B].lvs_state[3] = LVS_FENCED
    a.signals[B].peer_unhealthy[3] = False
    nodes[B].slow = SLOW
    _timed_tick(a, nodes)
    assert ("unfence", B) in a.inflight
    _grant_on_lease_expiry(a, db, nodes, clock)


def test_device_check_failure_hands_the_node_to_the_restart_flow(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env          # InlinePool: applied at once
    a.devices_ok = lambda _db, _node: False
    a.renew_all()
    for n in (A, B):
        nodes[n].ha = {3: (LVS_NORMAL, "healthy"), 4: (LVS_NORMAL, "healthy")}
    tick(a, nodes)
    a.signals[B].lvs_state[3] = LVS_FENCED
    a.signals[B].peer_unhealthy[3] = False
    nodes[B].ha = {3: (LVS_FENCED, "healthy"), 4: (LVS_NORMAL, "healthy")}
    clock.t += constants.TWO_NODE_SELF_FENCE_READMIT_MS
    tick(a, nodes)
    assert not calls(nodes[B], "jc_unfence")
    assert nodes[B] in readmitted


# ---- rebuild progress -----------------------------------------------------------

def test_rebuild_progress_with_a_frozen_node_does_not_delay_the_grant(pools):
    a, db, nodes, clock, _ = pools
    _steady(a, nodes)
    db.rec.rebuild = {B: {"reason": "test", "since": clock.t, "scheduled_at": 0,
                          "queued": True}}
    nodes[B].slow = SLOW
    clock.t += constants.TWO_NODE_REBUILD_CHECK_S * 1000
    _timed_tick(a, nodes)
    db.rec.rebuild = {}
    _grant_on_lease_expiry(a, db, nodes, clock)


# ---- verdict sends -------------------------------------------------------------

def test_best_effort_fence_uses_the_short_timeout(env):  # noqa: F811
    a, db, nodes, clock, _ = env
    seen = []
    orig = nodes[B].rpc_client

    def rpc_client(**kw):
        seen.append(kw.get("timeout"))
        return orig(**kw)
    nodes[B].rpc_client = rpc_client
    verdict = d.Verdict(d.V_DEGRADED, ARB_DEGRADED, raise_epoch=True, winner=A, loser=B,
                        grant={3: A, 4: A}, fence={3: B, 4: B}, fence_best_effort=True)
    a.renew_all()
    tick(a, nodes)
    seen.clear()
    nodes[B].down = True
    assert a.send(db.rec, verdict, [nodes[A], nodes[B]]) is True   # granted anyway
    assert seen == [constants.TWO_NODE_BEST_EFFORT_FENCE_TIMEOUT_S]
    assert calls(nodes[A], "jc_grant_solo")
