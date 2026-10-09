"""The two-node arbiter: drives ``decision`` against FDB and the nodes.

Contract: docs/design/two-node-arbitration.md, section 7. One process
(``services/two_node_arbiter.py``) runs ``Arbiter.run``: a lease loop renews
every capable node's lease, and the decision loop consumes the events the Go
collector queued (``ArbitrationEvent``), decides, and applies verdicts.

Ordering rule (section 7.1): the new epoch and the verdict are written to FDB
in one transaction BEFORE any verdict RPC is sent; after a restart the arbiter
reloads the record, raises the epoch and re-sends the last verdict.
"""

import errno
import logging
import time
from collections.abc import Callable

from simplyblock_core import constants
from simplyblock_core.arbitration import decision as dec
from simplyblock_core.arbitration import events as ev
from simplyblock_core.models.arbitration import (
    ARB_HEALING, ARB_STEADY, LVS_FENCED, LVS_NORMAL, ClusterArbitration,
)
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


class Arbiter:
    """State the arbiter keeps in memory between ticks.

    ``signals`` and lease ages are rebuilt after a restart from the nodes
    (``jc_ha_status`` resync by the collector) and from the FDB record, so
    losing them is harmless.
    """

    def __init__(self, db, clock: Callable[[], int] = _now_ms,
                 readmit: Callable[[StorageNode], None] | None = None):
        self.db = db
        self.clock = clock
        self.readmit = readmit or default_readmit
        #: node_id -> NodeSignals
        self.signals: dict = {}
        #: node_id -> ms of the last successful lease renewal
        self.lease_granted_at: dict = {}
        #: node_id -> bool, last RPC to the node succeeded
        self.reachable: dict = {}
        #: node_id -> instance id for which jc_set_dual_node was pushed
        self.configured: dict = {}
        #: node_id -> bool, node exposes the protocol RPCs
        self.capable: dict = {}
        #: cluster_id -> node ids re-admitted in the current healing
        self.readmitted: dict = {}
        #: cluster_id -> verdict whose RPCs did not all land yet
        self.undelivered: dict = {}
        #: clusters whose record was checked after this process started
        self.resumed: set = set()

    # ---- membership ---------------------------------------------------------

    def members(self, cluster_id: str) -> list:
        nodes = [n for n in self.db.get_storage_nodes_by_cluster_id(cluster_id)
                 if n.status != StorageNode.STATUS_REMOVED]
        return sorted(nodes, key=lambda n: n.get_id())

    def eligible(self, cluster) -> list | None:
        """The two member nodes if the cluster is arbitrated, else None."""
        if not getattr(cluster, "two_node_arbitration", False):
            return None
        nodes = self.members(cluster.get_id())
        return nodes if len(nodes) == 2 else None

    def record(self, cluster_id: str, nodes: list) -> ClusterArbitration:
        rec = self.db.get_cluster_arbitration(cluster_id)
        if rec is None:
            rec = ClusterArbitration()
            rec.cluster_id = cluster_id
            rec.preferred_node = nodes[0].get_id()
            rec.state_since = self.clock()
            rec.write_to_db(self.db.kv_store)
        return rec

    # ---- events -------------------------------------------------------------

    def consume_events(self, cluster_id: str) -> int:
        """Fold queued events into ``signals`` (in order per node) and remove them."""
        queued = self.db.get_arbitration_events(cluster_id)
        queued.sort(key=lambda e: (e.node_id, e.received_at, e.seq))
        for item in queued:
            sig = self.signals.setdefault(item.node_id, ev.NodeSignals())
            event = dict(item.payload)
            event.setdefault("instance", item.instance)
            event.setdefault("seq", item.seq)
            ev.apply(sig, event)
            item.remove(self.db.kv_store)
        return len(queued)

    # ---- snapshot -----------------------------------------------------------

    def leaders(self, rec: ClusterArbitration, nodes: list) -> dict:
        """jm_vuid -> leader. The record's view wins; otherwise each node leads
        its own primary LVS (its ``jm_vuid``)."""
        out = {int(n.jm_vuid): n.get_id() for n in nodes if n.jm_vuid}
        for entry in rec.lvs:
            if entry.get("leader"):
                out[int(entry["jm_vuid"])] = entry["leader"]
        return out

    def node_view(self, node: StorageNode, rec: ClusterArbitration) -> dec.NodeView:
        nid = node.get_id()
        sig = self.signals.get(nid) or ev.NodeSignals()
        granted = self.lease_granted_at.get(nid)
        expires = 0
        if granted:
            expires = granted + int(constants.TWO_NODE_LEASE_TTL_MS)
        elif rec.lease_expires_at(nid):
            expires = rec.lease_expires_at(nid)
        return dec.NodeView(
            node_id=nid,
            reports_peer_unhealthy=sig.reports_peer_unhealthy(),
            reports_peer_healthy=sig.reports_peer_healthy(),
            lvs_state=dict(sig.lvs_state),
            reachable=self.reachable.get(nid, True),
            lease_expires_at=expires,
            positively_fenced=bool(node.remediation_fenced),
            preferred=(nid == rec.preferred_node),
        )

    def snapshot(self, rec: ClusterArbitration, nodes: list) -> dec.Snapshot:
        a, b = (self.node_view(n, rec) for n in nodes)
        cid = rec.cluster_id
        return dec.Snapshot(
            now_ms=self.clock(), state=rec.state, epoch=rec.epoch, a=a, b=b,
            leaders=self.leaders(rec, nodes),
            lease_margin_ms=int(constants.TWO_NODE_LEASE_MARGIN_MS),
            stable_for_ms=int(constants.TWO_NODE_STABLE_FOR_S) * 1000,
            healthy_since=rec.healthy_since,
            healing_done=self._healing_done(rec, nodes, cid),
        )

    # ---- node configuration and leases ---------------------------------------

    def probe(self, node: StorageNode) -> bool:
        nid = node.get_id()
        if nid not in self.capable:
            self.capable[nid] = node.rpc_client(timeout=3, retry=0).supports_two_node_arbitration()
            if not self.capable[nid]:
                logger.warning("Node %s lacks the two-node arbitration RPCs; not arbitrated", nid)
        return self.capable[nid]

    def configure(self, rec: ClusterArbitration, node: StorageNode) -> None:
        """Push ``jc_set_dual_node`` with the arbitration fields once per node
        instance (a restarted node comes back with defaults)."""
        nid = node.get_id()
        instance = (self.signals.get(nid) or ev.NodeSignals()).instance
        if self.configured.get(nid) == instance and nid in self.configured:
            return
        node.rpc_client(timeout=3, retry=0).jc_set_dual_node(
            True, preferred=(nid == rec.preferred_node),
            hold_ms=constants.TWO_NODE_HOLD_MS, lease_ttl_ms=constants.TWO_NODE_LEASE_TTL_MS,
            arbitration=True)
        self.configured[nid] = instance

    def renew(self, rec: ClusterArbitration, node: StorageNode) -> None:
        """One lease renewal. A failure only marks the node unreachable: it is
        not a verdict (invariant 4)."""
        nid = node.get_id()
        try:
            node.rpc_client(timeout=1, retry=0).jc_lease_renew(
                rec.epoch, constants.TWO_NODE_LEASE_TTL_MS)
            self.lease_granted_at[nid] = self.clock()
            self.reachable[nid] = True
        except RPCRemoteError as e:
            self.reachable[nid] = True
            if e.code == -errno.ESTALE:
                # The node saw a higher epoch than our record (arbiter failover
                # mid-verdict): move past it before sending anything else.
                self.raise_epoch(rec.cluster_id, at_least=self._seen_epoch(nid) + 1)
            else:
                logger.warning("Lease renewal on %s failed: %s", nid, e)
        except Exception as e:                      # noqa: BLE001 - transport
            self.reachable[nid] = False
            logger.debug("Lease renewal on %s: node unreachable (%s)", nid, e)

    def _seen_epoch(self, node_id: str) -> int:
        return (self.signals.get(node_id) or ev.NodeSignals()).epoch_seen

    def raise_epoch(self, cluster_id: str, at_least: int = 0) -> ClusterArbitration:
        def bump(fresh):
            fresh.epoch = max(fresh.epoch + 1, int(at_least))
        rec = self.db.get_cluster_arbitration(cluster_id)
        return self.db.atomic_update(rec, bump)

    def persist_leases(self, rec: ClusterArbitration) -> None:
        """Store lease ages in the record (support view and arbiter failover).
        Not on every renewal: ``tick`` calls this a few times a second at most."""
        leases = {nid: {"granted_at": ts, "ttl_ms": constants.TWO_NODE_LEASE_TTL_MS}
                  for nid, ts in self.lease_granted_at.items()}

        def put(fresh):
            if fresh.leases == leases:
                return False
            fresh.leases = leases
        self.db.atomic_update(rec, put)

    # ---- verdicts -------------------------------------------------------------

    def commit(self, rec: ClusterArbitration, verdict: dec.Verdict, snap: dec.Snapshot):
        """Write state, epoch and verdict in ONE transaction, before any RPC."""
        now = self.clock()

        def write(fresh):
            if verdict.raise_epoch:
                fresh.epoch = max(fresh.epoch, rec.epoch) + 1
            if fresh.state != verdict.new_state:
                fresh.state, fresh.state_since = verdict.new_state, now
            for vuid, winner in verdict.grant.items():
                entry = fresh.lvs_entry(vuid)
                entry.update(leader=winner, state="solo",
                             fenced_node=verdict.fence.get(vuid, ""), since=now)
            if verdict.loser and verdict.kind == dec.V_PARTITION:
                if verdict.loser not in fresh.taint_requests:
                    fresh.taint_requests.append(verdict.loser)
            if verdict.raise_epoch:
                fresh.record_verdict(
                    {"epoch": fresh.epoch, "at": now, "kind": verdict.kind,
                     "winner": verdict.winner, "loser": verdict.loser,
                     "grant": {str(k): v for k, v in verdict.grant.items()},
                     "fence": {str(k): v for k, v in verdict.fence.items()},
                     "reason": verdict.reason,
                     "signals": {"a": vars(snap.a), "b": vars(snap.b)}},
                    constants.TWO_NODE_VERDICT_HISTORY)
        return self.db.atomic_update(rec, write)

    def send(self, rec: ClusterArbitration, verdict: dec.Verdict, nodes: list) -> bool:
        """Fence first, then grant (invariant 2). Returns True when every RPC
        landed; a failed fence means the grant is NOT sent this round."""
        by_id = {n.get_id(): n for n in nodes}
        fences: dict = {}
        for vuid, nid in verdict.fence.items():
            fences.setdefault(nid, []).append(int(vuid))
        for nid, vuids in fences.items():
            try:
                by_id[nid].rpc_client(timeout=5, retry=0).jc_fence(rec.epoch, sorted(vuids))
            except Exception as e:                  # noqa: BLE001
                logger.warning("jc_fence epoch %s on %s failed: %s; grant withheld",
                               rec.epoch, nid, e)
                return False
        grants: dict = {}
        for vuid, nid in verdict.grant.items():
            grants.setdefault(nid, []).append(int(vuid))
        ok = True
        for nid, vuids in grants.items():
            try:
                by_id[nid].rpc_client(timeout=5, retry=0).jc_grant_solo(
                    rec.epoch, sorted(vuids), constants.TWO_NODE_LEASE_TTL_MS)
            except Exception as e:                  # noqa: BLE001
                logger.warning("jc_grant_solo epoch %s on %s failed: %s", rec.epoch, nid, e)
                ok = False
        return ok

    # ---- healing (section 8) -------------------------------------------------

    def heal(self, rec: ClusterArbitration, nodes: list) -> None:
        """Unfence each fenced LVS (EAGAIN until resync completes), then hand the
        node to the restart flow once."""
        done = self.readmitted.setdefault(rec.cluster_id, set())
        for node in nodes:
            nid = node.get_id()
            sig = self.signals.get(nid) or ev.NodeSignals()
            fenced = sorted(v for v, s in sig.lvs_state.items() if s == LVS_FENCED)
            if fenced:
                try:
                    node.rpc_client(timeout=5, retry=0).jc_unfence(rec.epoch, fenced)
                except RPCRemoteError as e:
                    if e.code == -errno.EAGAIN:
                        logger.info("Node %s resyncing; unfence later", nid)
                    else:
                        logger.warning("jc_unfence on %s failed: %s", nid, e)
                    continue
                except Exception as e:              # noqa: BLE001
                    logger.warning("jc_unfence on %s unreachable: %s", nid, e)
                    continue
                for v in fenced:
                    sig.lvs_state[v] = LVS_NORMAL
                if nid not in done:
                    self.readmit(node)
                    done.add(nid)

    def _healing_done(self, rec: ClusterArbitration, nodes: list, cluster_id: str) -> bool:
        if rec.state != ARB_HEALING:
            return False
        for node in nodes:
            sig = self.signals.get(node.get_id()) or ev.NodeSignals()
            if any(s == LVS_FENCED for s in sig.lvs_state.values()):
                return False
            fresh = self.db.get_storage_node_by_id(node.get_id())
            if fresh.status != StorageNode.STATUS_ONLINE:
                return False
        return True


    # ---- the loop -----------------------------------------------------------

    def renew_all(self) -> None:
        """Lease loop body: every ``TWO_NODE_LEASE_RENEW_MS``."""
        for cluster in self.db.get_clusters():
            nodes = self.eligible(cluster)
            if not nodes:
                continue
            rec = self.record(cluster.get_id(), nodes)
            for node in nodes:
                if self.probe(node):
                    self.renew(rec, node)

    def tick(self) -> None:
        for cluster in self.db.get_clusters():
            nodes = self.eligible(cluster)
            if nodes:
                try:
                    self.tick_cluster(cluster.get_id(), nodes)
                except Exception:                   # noqa: BLE001 - next tick retries
                    logger.exception("Arbitration tick failed for cluster %s", cluster.get_id())

    def tick_cluster(self, cluster_id: str, nodes: list) -> dec.Verdict | None:
        rec = self.record(cluster_id, nodes)
        self.consume_events(cluster_id)
        for node in nodes:
            if self.capable.get(node.get_id()):
                try:
                    self.configure(rec, node)
                except Exception as e:              # noqa: BLE001
                    logger.debug("jc_set_dual_node on %s: %s", node.get_id(), e)

        if cluster_id not in self.resumed:
            self.resumed.add(cluster_id)
            self.resume(rec, nodes)
            rec = self.db.get_cluster_arbitration(cluster_id)

        # A verdict whose RPCs did not all land is re-sent every tick.
        pending = self.undelivered.get(cluster_id)
        if pending is not None:
            if self.send(rec, pending, nodes):
                self.undelivered.pop(cluster_id, None)
            return pending

        snap = self.snapshot(rec, nodes)
        self._track_health(rec, snap)
        rec = self.db.get_cluster_arbitration(cluster_id)
        snap = self.snapshot(rec, nodes)
        verdict = dec.decide(snap)

        if verdict.kind in (dec.V_PARTITION, dec.V_DEGRADED):
            rec = self.commit(rec, verdict, snap)
            logger.warning("Two-node verdict %s epoch %s for cluster %s: %s",
                           verdict.kind, rec.epoch, cluster_id, verdict.reason)
            if not self.send(rec, verdict, nodes):
                self.undelivered[cluster_id] = verdict
        elif verdict.kind == dec.V_HEAL:
            rec = self.commit(rec, verdict, snap)
            self.readmitted[cluster_id] = set()
        elif verdict.kind == dec.V_DONE:
            self._finish_healing(rec)
        elif verdict.new_state != rec.state:
            self.commit(rec, verdict, snap)

        if rec.state == ARB_HEALING:
            self.heal(rec, nodes)
        self.persist_leases(rec)
        return verdict

    def resume(self, rec: ClusterArbitration, nodes: list) -> None:
        """After a restart (section 7.1): raise the epoch and re-send the last
        verdict if one is in force."""
        if rec.state not in ("partitioned", "degraded") or not rec.verdicts:
            return
        last = rec.verdicts[-1]
        verdict = dec.Verdict(
            kind=last.get("kind", dec.V_DEGRADED), new_state=rec.state,
            winner=last.get("winner", ""), loser=last.get("loser", ""),
            grant={int(k): v for k, v in (last.get("grant") or {}).items()},
            fence={int(k): v for k, v in (last.get("fence") or {}).items()},
            reason="re-sent after arbiter restart")
        fresh = self.raise_epoch(rec.cluster_id)
        logger.warning("Arbiter resumed cluster %s in %s; re-sending verdict at epoch %s",
                       rec.cluster_id, rec.state, fresh.epoch)
        if not self.send(fresh, verdict, nodes):
            self.undelivered[rec.cluster_id] = verdict

    def _track_health(self, rec: ClusterArbitration, snap: dec.Snapshot) -> None:
        both = (snap.a.reports_peer_healthy and snap.b.reports_peer_healthy
                and not snap.a.reports_peer_unhealthy and not snap.b.reports_peer_unhealthy)
        now = self.clock()

        def put(fresh):
            if both and not fresh.healthy_since:
                fresh.healthy_since = now
            elif not both and fresh.healthy_since:
                fresh.healthy_since = 0
            else:
                return False
        self.db.atomic_update(rec, put)

    def _finish_healing(self, rec: ClusterArbitration) -> None:
        now = self.clock()

        def put(fresh):
            fresh.state, fresh.state_since = ARB_STEADY, now
            fresh.taint_requests = []
            fresh.healthy_since = 0
            for entry in fresh.lvs:
                entry.update(state="normal", fenced_node="", since=now)
        self.db.atomic_update(rec, put)
        self.readmitted.pop(rec.cluster_id, None)
        logger.info("Cluster %s healed; arbitration back to steady", rec.cluster_id)


def default_readmit(node: StorageNode) -> None:
    """Re-admission through the existing restart flow (section 8, step 3).

    The restart task only acts on a node that is not ONLINE; marking the fenced
    node OFFLINE hands it to the auto-restart path, which shuts SPDK down and
    recreates its stores as secondary of the peer that now leads them.
    """
    from simplyblock_core import storage_node_ops
    from simplyblock_core.controllers import tasks_controller
    storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_OFFLINE,
                                     caused_by="two-node-arbiter")
    tasks_controller.add_node_to_auto_restart(node)
