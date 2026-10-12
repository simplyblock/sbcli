"""The arbiter's decision engine: pure functions over a snapshot of signals.

docs/design/two-node-arbitration.md, sections 2 (invariants), 7.1 (state
machine) and the decision matrix in the design PDF. Nothing here does I/O:
``decide`` takes what the arbiter knows and returns what it should do, so the
whole matrix is covered by table tests with an injected clock.
"""

from dataclasses import dataclass, field

from simplyblock_core.models.arbitration import (
    ARB_DECIDING,
    ARB_DEGRADED,
    ARB_HEALING,
    ARB_PARTITIONED,
    ARB_STEADY,
    LVS_FENCED,
    LVS_HOLDING,
    LVS_SOLO,
)

#: Verdict kinds.
V_NONE = "none"            # nothing to do
V_WAIT = "wait"            # keep deciding; re-evaluate on the next signal or tick
V_PARTITION = "partition"  # fence the loser per LVS, then grant the winner
V_DEGRADED = "degraded"    # grant the survivor (the peer is down and fenced)
V_HEAL = "heal"            # both healthy and stable: run the healing flow
V_DONE = "done"            # healing finished: back to steady


@dataclass
class NodeView:
    """What the arbiter knows about one storage node right now."""
    node_id: str
    #: Reports the peer's JM unhealthy (any LVS).
    reports_peer_unhealthy: bool = False
    #: Reports the peer's JM healthy again (every LVS).
    reports_peer_healthy: bool = False
    #: Per jm_vuid node-side state: normal | holding | solo | fenced.
    lvs_state: dict = field(default_factory=dict)
    #: The CP reached it on its last call (lease renewal or long-poll).
    reachable: bool = True
    #: ms since epoch when its lease expires; 0 = never leased.
    lease_expires_at: int = 0
    #: Positive fencing evidence from the operator (BMC fence, out-of-service taint).
    positively_fenced: bool = False
    #: Positive evidence that the node's SPDK is gone: its proxy answered HTTP
    #: 500 on ``TWO_NODE_SPDK_DOWN_HTTP500_STREAK`` consecutive calls (the proxy
    #: runs, the SPDK socket behind it does not). Counts as loss of the node.
    spdk_lost: bool = False

    def holding(self) -> bool:
        return any(s == LVS_HOLDING for s in self.lvs_state.values())


@dataclass
class Snapshot:
    now_ms: int
    state: str
    epoch: int
    a: NodeView
    b: NodeView
    #: jm_vuid -> current leader node id (from the cluster's LVS placement).
    leaders: dict
    lease_margin_ms: int
    stable_for_ms: int
    #: ms since epoch both nodes were first seen healthy again; 0 = not healthy.
    healthy_since: int = 0
    #: Healing steps finished (resync gate, restart flow, unfence).
    healing_done: bool = False


@dataclass
class Verdict:
    kind: str
    new_state: str
    #: True when the verdict needs a new epoch (fence/grant RPCs follow).
    raise_epoch: bool = False
    winner: str = ""
    loser: str = ""
    #: jm_vuid -> node to grant solo (partition: per-LVS winner).
    grant: dict = field(default_factory=dict)
    #: jm_vuid -> node to fence.
    fence: dict = field(default_factory=dict)
    #: jm_vuid -> [node ids] to fence; a partition (fence_both) fences both.
    fence_all: dict = field(default_factory=dict)
    reason: str = ""


def _lease_expired(node: NodeView, now_ms: int, margin_ms: int) -> bool:
    """Invariant 2: the peer counts as fenced by lease only once its lease has
    run out by more than ``margin_ms`` -- and only matters while it was holding
    (invariant 4), which the caller checks."""
    return node.lease_expires_at > 0 and now_ms > node.lease_expires_at + margin_ms


def _peer_is_fenced(peer: NodeView, now_ms: int, margin_ms: int) -> bool:
    """May the OTHER node be granted solo operation?

    There is no preferred node: every node fences itself at its hold deadline
    without a verdict, and on lease expiry while holding. So the peer counts as
    fenced once its lease has run out (plus the margin), at once when its SPDK
    is positively known to be gone (``spdk_lost``), or on positive fencing
    evidence from the operator.
    """
    if peer.positively_fenced or peer.spdk_lost:
        return True
    return (not peer.reachable) and _lease_expired(peer, now_ms, margin_ms)


def _all_lvs(snap: Snapshot) -> list:
    vuids = set(snap.leaders) | set(snap.a.lvs_state) | set(snap.b.lvs_state)
    return sorted(int(v) for v in vuids)


def _partition_verdict(snap: Snapshot) -> Verdict:
    """Both nodes report each other and both are reachable: a cut link between
    them, both alive and both still renewing.

    Policy ``fence_both`` (user decision, campaign c1): IO stops on a
    partition. Every LVS is fenced on BOTH nodes and nobody is granted; there
    is no preferred node and no leader-wins rule. Healing (section 8) brings
    the pair back once the link is healthy again."""
    vuids = _all_lvs(snap)
    fence_all = {v: [snap.a.node_id, snap.b.node_id] for v in vuids}
    return Verdict(V_PARTITION, ARB_PARTITIONED, raise_epoch=True,
                   fence_all=fence_all,
                   reason="both nodes report each other; both reachable: fence_both")


def _degraded_verdict(snap: Snapshot, survivor: NodeView, dead: NodeView, reason: str) -> Verdict:
    vuids = _all_lvs(snap)
    return Verdict(V_DEGRADED, ARB_DEGRADED, raise_epoch=True, winner=survivor.node_id,
                   loser=dead.node_id, grant={v: survivor.node_id for v in vuids},
                   reason=reason)


def _deciding(snap: Snapshot) -> Verdict:
    """The matrix for a fresh unhealthy / hold / lease-expiry signal."""
    a, b, now, margin = snap.a, snap.b, snap.now_ms, snap.lease_margin_ms

    # Invariant 4: no node holding means no decision is needed. An uplink loss
    # (both unreachable) or WAN jitter with both JMs healthy lands here.
    if not (a.holding() or b.holding() or a.reports_peer_unhealthy or b.reports_peer_unhealthy):
        return Verdict(V_NONE, ARB_STEADY, reason="no node reports its peer and none is holding")

    # Partition: each reports the other and the CP reaches both.
    if a.reports_peer_unhealthy and b.reports_peer_unhealthy and a.reachable and b.reachable:
        return _partition_verdict(snap)

    # Only one side speaks: the silent one is down or isolated.
    for survivor, dead in ((a, b), (b, a)):
        # A node whose SPDK is known gone reports nothing any more; what it
        # reported before it died is stale.
        dead_reports = dead.reports_peer_unhealthy and not dead.spdk_lost
        if survivor.reports_peer_unhealthy and survivor.reachable and not dead_reports:
            if _peer_is_fenced(dead, now, margin):
                why = ("positive fencing" if dead.positively_fenced
                       else "peer SPDK down (proxy HTTP 500)" if dead.spdk_lost
                       else "peer silent and its lease expired")
                return _degraded_verdict(snap, survivor, dead, why)
            if dead.reachable:
                # Reachable but not (yet) reporting: its event may still be on
                # the way. Wait for it, never decide on half the picture.
                return Verdict(V_WAIT, ARB_DECIDING, reason="peer reachable; its report is pending")
            return Verdict(V_WAIT, ARB_DECIDING, reason="waiting for the peer's lease to expire")

    # Nobody reachable that reports: both links (or the CP's uplink) are down.
    # The nodes fence themselves at the hold deadline: a full isolation stops
    # IO on both nodes.
    return Verdict(V_WAIT, ARB_DECIDING,
                   reason="no reachable node reports; the nodes apply the hold-deadline rule")


def decide(snap: Snapshot) -> Verdict:
    """What the arbiter should do now. Pure: same snapshot, same verdict."""
    a, b = snap.a, snap.b
    both_healthy = a.reports_peer_healthy and b.reports_peer_healthy and not (
        a.reports_peer_unhealthy or b.reports_peer_unhealthy)

    if snap.state in (ARB_STEADY, ARB_DECIDING):
        return _deciding(snap)

    if snap.state in (ARB_PARTITIONED, ARB_DEGRADED):
        if both_healthy and a.reachable and b.reachable:
            if snap.healthy_since and snap.now_ms - snap.healthy_since >= snap.stable_for_ms:
                return Verdict(V_HEAL, ARB_HEALING, raise_epoch=True,
                               reason="both healthy for the stable window")
            return Verdict(V_WAIT, snap.state, reason="healthy again; inside the stable window")
        return Verdict(V_NONE, snap.state, reason="verdict in force")

    if snap.state == ARB_HEALING:
        if a.reports_peer_unhealthy or b.reports_peer_unhealthy:
            # Flapped during healing: start over with a new decision.
            return _deciding(snap)
        if snap.healing_done:
            return Verdict(V_DONE, ARB_STEADY, reason="healing finished")
        return Verdict(V_WAIT, ARB_HEALING, reason="healing in progress")

    return Verdict(V_NONE, snap.state, reason="unknown state")


def lvs_needing_unfence(a: NodeView, b: NodeView) -> dict:
    """node id -> jm_vuids it holds fenced (healing, section 8)."""
    out: dict = {}
    for node in (a, b):
        fenced = sorted(int(v) for v, s in node.lvs_state.items() if s == LVS_FENCED)
        if fenced:
            out[node.node_id] = fenced
    return out


def solo_lvs(node: NodeView) -> list:
    return sorted(int(v) for v, s in node.lvs_state.items() if s == LVS_SOLO)
