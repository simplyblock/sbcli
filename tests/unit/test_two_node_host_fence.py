"""Host fencing and the fencing authority (docs/design/two-node-kubernetes-fencing.md).

Mode ``witness`` (CR A): after a degraded verdict whose loser's host state is
unknown, the loser's client host NQN is revoked on every volume subsystem of
the winner; the operator then sets StorageFenced and Kubernetes remediates the
node. Never for an SPDK-only or planned loss, never for a precautionary grant,
never for a cluster without ``two_node_host_fencing``.

Mode ``pacemaker`` (CR B): the arbiter does not act on the cluster at all.
"""

import pytest

from simplyblock_core import constants
from simplyblock_core.arbitration import arbiter as arb
from simplyblock_core.arbitration import host_fence as hf
from simplyblock_core.models.arbitration import (
    HF_RELEASABLE,
    HF_RESTORING,
    HF_REVOKED,
    HF_REVOKING,
    HF_UNAVAILABLE,
)
from tests.unit.test_two_node_arbiter import (  # noqa: F401 - fixture
    CL,
    A,
    B,
    calls,
    env,
    event,
)

HOST_A = "nqn.2014-08.org.nvmexpress:uuid:host-a"
HOST_B = "nqn.2014-08.org.nvmexpress:uuid:host-b"
VOL1 = "nqn.2023-02.io.simplyblock:cl:lvol:v1"
VOL2 = "nqn.2023-02.io.simplyblock:cl:lvol:v2"
HUB = "nqn.2023-02.io.simplyblock:cl:hublvol:LVS_3"


# ---- the pure plan -----------------------------------------------------------

def _sub(nqn, any_host, hosts=()):
    return {"nqn": nqn, "allow_any_host": any_host, "hosts": [{"nqn": h} for h in hosts]}


def test_plan_converts_allow_any_and_keeps_every_other_connected_host():
    other = "nqn.2014-08.org.nvmexpress:uuid:external"
    steps = hf.plan_revoke([_sub(VOL1, True)], {VOL1}, HOST_B, {HOST_A},
                           {VOL1: {HOST_B, other}})
    assert [(s.nqn, s.mode, s.add) for s in steps] == [(VOL1, hf.MODE_ANY, sorted([HOST_A, other]))]


def test_plan_removes_the_loser_from_an_allow_list_only_if_listed():
    steps = hf.plan_revoke([_sub(VOL1, False, [HOST_A, HOST_B]), _sub(VOL2, False, [HOST_A])],
                           {VOL1, VOL2}, HOST_B, {HOST_A}, {})
    assert [(s.nqn, s.mode) for s in steps] == [(VOL1, hf.MODE_LIST)]


def test_plan_never_touches_hublvol_or_foreign_subsystems():
    """Campaign c1, B2: the returning peer resyncs through its hublvol."""
    steps = hf.plan_revoke([_sub(HUB, True), _sub("nqn.2014-08.org.nvmexpress.discovery", True)],
                           {HUB, VOL1}, HOST_B, {HOST_A}, {})
    assert steps == []


def test_plan_covers_volume_subsystems_the_db_did_not_list():
    """A DB read that misses a volume must not leave the loser admitted while
    the fence reports revoked (sim K1: volumes without a pool)."""
    prefix = "nqn.2023-02.io.simplyblock:cl:lvol:"
    steps = hf.plan_revoke([_sub(VOL1, True), _sub(HUB, True)], set(), HOST_B, {HOST_A}, {},
                           volume_prefix=prefix)
    assert [s.nqn for s in steps] == [VOL1]


def test_plan_is_idempotent_after_a_revocation():
    steps = hf.plan_revoke([_sub(VOL1, False, [HOST_A])], {VOL1}, HOST_B, {HOST_A}, {})
    assert steps == []


# ---- revoke / release against one node --------------------------------------

class SubsysRPC:
    """nvmf subsystems of one node, with SPDK's remove_host semantics: an
    unlisted host is an error and nothing is disconnected."""

    def __init__(self, subsystems, connected=None):
        self.subs = {s["nqn"]: {"allow_any_host": s["allow_any_host"],
                                "hosts": [h["nqn"] for h in s["hosts"]]} for s in subsystems}
        self.connected = connected or {}
        self.log = []

    def subsystem_list(self):
        return [{"nqn": n, "allow_any_host": s["allow_any_host"],
                 "hosts": [{"nqn": h} for h in s["hosts"]]} for n, s in self.subs.items()]

    def nvmf_subsystem_get_controllers(self, nqn):
        return [{"hostnqn": h} for h in sorted(self.connected.get(nqn, ()))]

    def subsystem_add_host(self, nqn, host, **_):
        self.log.append(("add", nqn, host))
        self.subs[nqn]["hosts"].append(host)

    def subsystem_remove_host(self, nqn, host):
        self.log.append(("remove", nqn, host))
        if host not in self.subs[nqn]["hosts"]:
            raise RuntimeError("Internal error")
        self.subs[nqn]["hosts"].remove(host)
        self.connected.get(nqn, set()).discard(host)

    def subsystem_allow_any_host(self, nqn, allow):
        self.log.append(("any", nqn, allow))
        self.subs[nqn]["allow_any_host"] = allow


def test_revoke_cuts_the_loser_off_an_allow_any_subsystem():
    rpc = SubsysRPC([_sub(VOL1, True), _sub(HUB, True)], connected={VOL1: {HOST_A, HOST_B}})
    changed = hf.revoke(rpc, {VOL1, HUB}, HOST_B, {HOST_A})
    assert changed == {VOL1: {"mode": hf.MODE_ANY, "added": [HOST_A]}}
    # allow-list first, then list + remove the loser: the remove disconnects it
    assert rpc.log == [("add", VOL1, HOST_A), ("any", VOL1, False),
                       ("add", VOL1, HOST_B), ("remove", VOL1, HOST_B)]
    assert rpc.subs[VOL1] == {"allow_any_host": False, "hosts": [HOST_A]}
    assert HOST_B not in rpc.connected[VOL1]
    assert rpc.subs[HUB]["allow_any_host"] is True
    # a second pass changes nothing
    rpc.log.clear()
    assert hf.revoke(rpc, {VOL1, HUB}, HOST_B, {HOST_A}) == {} and rpc.log == []


def test_release_restores_exactly_what_was_changed():
    rpc = SubsysRPC([_sub(VOL1, False, [HOST_A]), _sub(VOL2, False, [HOST_A])])
    readded = []

    def readd(nqn):
        readded.append(nqn)
        rpc.subsystem_add_host(nqn, HOST_B)
    changes = {VOL1: {"mode": hf.MODE_ANY, "added": [HOST_A]},
               VOL2: {"mode": hf.MODE_LIST, "added": []},
               "nqn.gone": {"mode": hf.MODE_ANY, "added": []}}
    done = hf.release(rpc, changes, HOST_B, readd)
    assert sorted(done) == sorted(changes)
    assert rpc.subs[VOL1]["allow_any_host"] is True
    assert rpc.subs[VOL1]["hosts"] == [HOST_A]           # added hosts stay listed
    assert readded == [VOL2] and HOST_B in rpc.subs[VOL2]["hosts"]


# ---- the arbiter lifecycle ----------------------------------------------------

class HFCluster:
    two_node_arbitration = True
    two_node_fencing_authority = "witness"
    two_node_host_fencing = True

    def get_id(self):
        return CL


@pytest.fixture
def hfenv(env):  # noqa: F811
    a, db, nodes, clock, readmitted = env
    nodes[A].client_host_nqn, nodes[B].client_host_nqn = HOST_A, HOST_B
    db.revokes, db.releases, db.notes = [], [], []
    db.revoke_result = {VOL1: {"mode": hf.MODE_ANY, "added": [HOST_A]}}

    def revoke(_db, winner, host_nqn):
        db.revokes.append((winner.get_id(), host_nqn))
        if isinstance(db.revoke_result, Exception):
            raise db.revoke_result
        return dict(db.revoke_result)

    def release(_db, winner, subsystems, host_nqn):
        db.releases.append((winner.get_id(), dict(subsystems), host_nqn))
    a.host_fence_revoke, a.host_fence_release = revoke, release
    a.notify = lambda cid, node, code, msg: db.notes.append(code)
    return a, db, nodes, clock, HFCluster()


def _lose_b_by_lease(a, db, nodes, clock, cluster, seq=1):
    a.renew_all()
    nodes[B].down = True
    db.events = [event(A, seq, "remote_jm_unhealthy", 3, ha_state="holding"),
                 event(A, seq + 1, "remote_jm_unhealthy", 4, ha_state="holding")]
    a.renew_all()
    clock.t += constants.TWO_NODE_LEASE_TTL_MS + constants.TWO_NODE_LEASE_MARGIN_MS + 1
    return a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)


def _fence(db):
    return (db.rec.host_fences or {}).get(B, {})


def test_silent_loser_is_host_fenced_on_the_winner_after_the_grant(hfenv):
    a, db, nodes, clock, cluster = hfenv
    v = _lose_b_by_lease(a, db, nodes, clock, cluster)
    assert v.kind == "degraded" and v.winner == A and v.host_fence
    assert db.rec.verdicts[-1]["host_fence"] is True
    assert calls(nodes[A], "jc_grant_solo")             # the grant is not held up
    assert db.revokes == [(A, HOST_B)]
    fence = _fence(db)
    assert fence["state"] == HF_REVOKED and fence["on"] == A and fence["host_nqn"] == HOST_B
    assert fence["subsystems"] == {VOL1: {"mode": hf.MODE_ANY, "added": [HOST_A]}}
    assert fence["revoked_at"] == clock.t and "TWO_NODE_HOST_FENCED" in db.notes


def test_revocation_is_reasserted_and_merged(hfenv):
    a, db, nodes, clock, cluster = hfenv
    _lose_b_by_lease(a, db, nodes, clock, cluster)
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert len(db.revokes) == 1                         # not due yet
    clock.t += constants.TWO_NODE_HOST_FENCE_REASSERT_S * 1000
    db.revoke_result = {VOL2: {"mode": hf.MODE_LIST, "added": []}}
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert len(db.revokes) == 2
    assert set(_fence(db)["subsystems"]) == {VOL1, VOL2}


def test_failed_revocation_stays_revoking_and_is_retried(hfenv):
    a, db, nodes, clock, cluster = hfenv
    db.revoke_result = RuntimeError("winner busy")
    _lose_b_by_lease(a, db, nodes, clock, cluster)
    assert _fence(db)["state"] == HF_REVOKING and "winner busy" in _fence(db)["error"]
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert len(db.revokes) == 1                         # 1 s retry spacing
    db.revoke_result = {}
    clock.t += 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert _fence(db)["state"] == HF_REVOKED


def test_spdk_only_loss_never_fences_the_host(hfenv):
    """HTTP 500: the proxy answered, so the host runs and its VMs keep using
    the winner's path. Kubernetes must not restart them."""
    a, db, nodes, clock, cluster = hfenv
    a.renew_all()
    nodes[B].http500 = True
    a.renew_all()
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    v = a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert v.kind == "degraded" and not v.host_fence
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.revokes == [] and not db.rec.host_fences


def test_planned_loss_never_fences_the_host(hfenv):
    a, db, nodes, clock, cluster = hfenv
    a.renew_all()
    db.rec.planned_loss = {B: {"at": clock.t, "instance": "i-" + B, "reason": "restart"}}
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    v = a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert v.kind == "degraded" and not v.host_fence
    assert db.revokes == [] and not db.rec.host_fences


def test_precautionary_grant_waits_until_the_winner_loses_the_peer(hfenv):
    """B unreachable from the hub only (its uplink): A still sees B's JM
    healthy, the arbiter grants A without a report, but B's host keeps
    running its VMs; no host fence until A itself reports B's JM unhealthy."""
    a, db, nodes, clock, cluster = hfenv
    a.renew_all()
    db.events = [event(A, 1, "remote_jm_healthy", 3, ha_state="normal")]
    nodes[B].down = True
    a.renew_all()
    clock.t += constants.TWO_NODE_LEASE_TTL_MS + constants.TWO_NODE_LEASE_MARGIN_MS + 1
    v = a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert v.kind == "degraded" and v.host_fence
    assert db.revokes == [] and not db.rec.host_fences
    db.events = [event(A, 2, "remote_jm_unhealthy", 3, ha_state="solo")]
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.revokes == [(A, HOST_B)] and _fence(db)["state"] == HF_REVOKED


def test_unknown_client_host_is_unavailable_until_reported(hfenv):
    a, db, nodes, clock, cluster = hfenv
    nodes[B].client_host_nqn = ""
    _lose_b_by_lease(a, db, nodes, clock, cluster)
    assert _fence(db)["state"] == HF_UNAVAILABLE and db.revokes == []
    assert db.notes.count("TWO_NODE_HOST_FENCE_UNAVAILABLE") == 1
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.notes.count("TWO_NODE_HOST_FENCE_UNAVAILABLE") == 1   # once per episode
    nodes[B].client_host_nqn = HOST_B
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert _fence(db)["state"] == HF_REVOKED and db.revokes == [(A, HOST_B)]


def _heal(a, db, nodes, clock, cluster):
    """B back: reachable, its LVS normal, both JMs healthy, past the stable window."""
    nodes[B].down = False
    a.renew_all()
    db.events = [event(A, 3, "remote_jm_healthy", 3, ha_state="normal"),
                 event(A, 4, "remote_jm_healthy", 4, ha_state="normal"),
                 event(B, 1, "remote_jm_healthy", 3, ha_state="normal"),
                 event(B, 2, "remote_jm_healthy", 4, ha_state="normal")]
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    clock.t += constants.TWO_NODE_STABLE_FOR_S * 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)


def test_release_only_after_heal_and_the_operator_request(hfenv):
    a, db, nodes, clock, cluster = hfenv
    _lose_b_by_lease(a, db, nodes, clock, cluster)
    _heal(a, db, nodes, clock, cluster)
    assert db.rec.state != "degraded"
    assert _fence(db)["state"] == HF_RELEASABLE and db.releases == []
    # the operator verified the node is clean (API POST .../host-fence/release)
    fences = dict(db.rec.host_fences)
    fences[B] = dict(fences[B], state=HF_RESTORING, release_requested=True)
    db.rec.host_fences = fences
    clock.t += 1000
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.releases == [(A, {VOL1: {"mode": hf.MODE_ANY, "added": [HOST_A]}}, HOST_B)]
    assert B not in db.rec.host_fences
    assert db.rec.verdicts[-1]["kind"] == "host_fence_released"


def test_lost_again_before_release_keeps_the_fence(hfenv):
    a, db, nodes, clock, cluster = hfenv
    _lose_b_by_lease(a, db, nodes, clock, cluster)
    _heal(a, db, nodes, clock, cluster)
    assert _fence(db)["state"] == HF_RELEASABLE
    clock.t += 60_000
    v = _lose_b_by_lease(a, db, nodes, clock, cluster, seq=10)
    assert v.kind == "degraded"
    assert _fence(db)["state"] == HF_REVOKED and db.releases == []


def test_cluster_without_host_fencing_is_unchanged(hfenv):
    a, db, nodes, clock, cluster = hfenv
    cluster.two_node_host_fencing = False
    v = _lose_b_by_lease(a, db, nodes, clock, cluster)
    assert v.kind == "degraded"
    assert db.revokes == [] and not db.rec.host_fences


def test_override_fences_the_named_losers_host(hfenv):
    a, db, nodes, clock, cluster = hfenv
    a.renew_all()
    db.rec.override = {"winner": A, "reason": "operator decided", "by": "api", "at": clock.t}
    db.events = [event(A, 1, "remote_jm_unhealthy", 3, ha_state="holding")]
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.rec.verdicts[-1]["host_fence"] is True
    db.events = [event(A, 2, "ha_solo", 3)]             # the winner runs alone now
    a.tick_cluster(CL, [nodes[A], nodes[B]], cluster)
    assert db.rec.state == "degraded" and db.revokes == [(A, HOST_B)]


# ---- mode pacemaker ---------------------------------------------------------------

def test_pacemaker_authority_keeps_the_arbiter_out(env):  # noqa: F811
    a, db, nodes, clock, _ = env

    class PacemakerCluster(HFCluster):
        two_node_fencing_authority = "pacemaker"
    assert a.eligible(PacemakerCluster()) is None
    db.get_clusters = lambda: [PacemakerCluster()]
    assert a.arbitrated_clusters() == []
    a.renew_all()
    a.tick()
    assert not any(n.calls for n in nodes.values())    # no lease, no verdict, nothing
    assert arb.witness_authority(HFCluster()) and not arb.witness_authority(PacemakerCluster())
    assert not arb.host_fencing_on(PacemakerCluster())


def test_pacemaker_authority_writes_no_planned_loss_and_queues_no_events():
    from simplyblock_core.arbitration import events as ev
    from simplyblock_core.arbitration import planned

    class PacemakerCluster(HFCluster):
        two_node_fencing_authority = "pacemaker"

    class DB:
        def get_cluster_by_id(self, _cid):
            return PacemakerCluster()

    class N:
        cluster_id = CL
        uuid = B

    assert planned.note_planned_loss(N(), "restart", db=DB()) is False
    seen = []
    orig = ev.queue_event
    ev.queue_event = lambda *a, **k: seen.append(a)
    try:
        assert ev.forward_legacy_jm_event(None, PacemakerCluster(), B,
                                          {"event_type": "device_status",
                                           "status": "remote_jm_unhealthy", "jm_vuid": 3}) is True
        assert seen == []
        assert ev.forward_legacy_jm_event(None, HFCluster(), B,
                                          {"event_type": "device_status",
                                           "status": "remote_jm_unhealthy", "jm_vuid": 3}) is True
        assert len(seen) == 1
    finally:
        ev.queue_event = orig
