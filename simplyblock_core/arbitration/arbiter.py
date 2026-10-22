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
import threading
import time
from collections.abc import Callable
from concurrent.futures import ALL_COMPLETED, Future, ThreadPoolExecutor, wait

from simplyblock_core import constants
from simplyblock_core.arbitration import decision as dec
from simplyblock_core.arbitration import events as ev
from simplyblock_core.arbitration import set_dual_node
from simplyblock_core.models.arbitration import (
    ARB_DEGRADED,
    ARB_HEALING,
    ARB_PARTITIONED,
    ARB_STEADY,
    HF_RELEASABLE,
    HF_RESTORING,
    HF_REVOKED,
    HF_REVOKING,
    HF_UNAVAILABLE,
    LVS_FENCED,
    LVS_HOLDING,
    LVS_NORMAL,
    LVS_SOLO,
    ArbitrationEvent,
    ClusterArbitration,
)
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCHTTPError, RPCRemoteError

logger = logging.getLogger(__name__)

#: ultra's migration error code is a bit field, ``1 << EMigrationError``
#: (DISTR_v2 parts/alg_migration.hpp ``set_error``): "mig error: 520" is
#: RESHUFFLE (9) | DST_NOT_AVAILABLE (3).
MIG_ERROR_NAMES = {1: "NO_FREE_PLACEMENT", 2: "SRC_NOT_AVAILABLE", 3: "DST_NOT_AVAILABLE",
                   4: "IO_READ", 5: "IO_WRITE", 6: "IO_UNMAP", 7: "JOURNAL_UPDATE",
                   8: "SHUTDOWN", 9: "RESHUFFLE", 10: "MISC"}
MIG_UNAVAILABLE_BITS = (1 << 2) | (1 << 3)


def describe_mig_error(code: int) -> str:
    """``"RESHUFFLE|DST_NOT_AVAILABLE"`` for 520."""
    names = [name for bit, name in sorted(MIG_ERROR_NAMES.items(), reverse=True)
             if int(code) & (1 << bit)]
    return "|".join(names) or "none"


class InlinePool:
    """Runs a submitted call at once, in the caller's thread.

    The default for ``Arbiter(rpc_pool=...)``: deterministic, so unit tests see
    every renewal and status poll complete before ``tick`` decides. The service
    passes ``rpc_pool()`` (a bounded thread pool) instead."""

    def submit(self, fn, *args, **kwargs) -> Future:
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except Exception as e:                      # noqa: BLE001 - delivered via the future
            fut.set_exception(e)
        return fut


def rpc_pool() -> ThreadPoolExecutor:
    """The bounded pool the service runs node RPCs on (campaign c3, 02a)."""
    return ThreadPoolExecutor(max_workers=int(constants.TWO_NODE_RPC_WORKERS),
                              thread_name_prefix="arbiter-rpc")


def work_pool() -> ThreadPoolExecutor:
    """The bounded pool for node work that is not a renewal or status poll
    (campaign c7, F16): jc_set_dual_node, device check + jc_unfence and the
    leadership reconciliation run there, never on the decision thread, and
    never on ``rpc_pool`` where calls blocked on a frozen node could starve
    the renewals of its healthy peer."""
    return ThreadPoolExecutor(max_workers=int(constants.TWO_NODE_WORK_WORKERS),
                              thread_name_prefix="arbiter-work")


def _now_ms() -> int:
    return int(time.time() * 1000)


class Arbiter:
    """State the arbiter keeps in memory between ticks.

    ``signals`` and lease ages are rebuilt after a restart from the nodes
    (``jc_ha_status`` resync by the collector) and from the FDB record, so
    losing them is harmless.
    """

    def __init__(self, db, clock: Callable[[], int] = _now_ms,
                 readmit: Callable[[StorageNode], None] | None = None,
                 log_event: Callable[[ArbitrationEvent, dict], None] | None = None,
                 reconcile_leadership: Callable | None = None,
                 link_probe: Callable | None = None,
                 reconnect_jms: Callable | None = None,
                 run_async: Callable | None = None,
                 rpc_pool=None,
                 work_pool=None,
                 schedule_rebuild: Callable | None = None,
                 open_rebuild_tasks: Callable | None = None,
                 devices_ok: Callable | None = None,
                 rebuild_task_errors: Callable | None = None,
                 restore_leadership: Callable | None = None,
                 notify: Callable | None = None,
                 host_fence_revoke: Callable | None = None,
                 host_fence_release: Callable | None = None):
        self.db = db
        self.clock = clock
        self.readmit = readmit or default_readmit
        self.log_event = log_event or default_log_event
        self.reconcile_leadership = reconcile_leadership or default_reconcile_leadership
        #: (node, peer) -> True/False/None: can ``node`` reach ``peer`` on the
        #: data network (None: the probe itself failed, inconclusive).
        self.link_probe = link_probe or default_link_probe
        #: (node, peer) -> None: reconnect node's remote JM devices of peer.
        self.reconnect_jms = reconnect_jms or default_reconnect_jms
        #: Runs slow recovery work (pings, reconnects) off the decision loop.
        self.run_async = run_async or _run_in_thread
        #: Runs lease renewals and status polls; one call per node in flight
        #: (campaign c3, 02a). InlinePool by default (deterministic tests).
        self.rpc_pool = rpc_pool or InlinePool()
        #: Runs every other node RPC the tick needs (campaign c7, F16): the
        #: decision thread never blocks on a node. InlinePool by default.
        self.work_pool = work_pool or InlinePool()
        #: (kind, id) -> context of a work-pool call still to be applied
        self.work_ctx: dict = {}
        #: (kind, node_id) -> Future of a renewal or status poll still running
        self.inflight: dict = {}
        #: No arbitrated cluster at the last scan: clock (ms) of the next scan.
        self._idle_until_ms = 0
        #: node -> bool: queue the data rebuild after a re-admission (F10)
        self.schedule_rebuild = schedule_rebuild or default_schedule_rebuild
        #: cluster_id -> number of open data-rebuild (migration) tasks
        self.open_rebuild_tasks = open_rebuild_tasks or default_open_rebuild_tasks
        #: cluster_id -> ms of the last rebuild progress check
        self.rebuild_checked_at: dict = {}
        #: (db, node) -> True: its data devices exist and are online; False:
        #: one is missing or not online; None: could not tell (campaign c4, F12)
        self.devices_ok = devices_ok or default_devices_ok
        #: (db, cluster_id) -> {node_id: mig error bitmask} of open rebuild tasks
        #: that last failed with a migration error (campaign c4, F12)
        self.rebuild_task_errors = rebuild_task_errors or default_rebuild_task_errors
        #: (cluster_id, survivor, primary, lvs_name) -> bool: make the survivor
        #: (cluster_id, primary, peer, lvs_name) -> bool: the LVS is led by a
        #: node again, with ANA optimized on its leader (campaign c5, 07/08)
        self.restore_leadership = restore_leadership or default_restore_leadership
        #: cluster_id -> {lvs_name: {"primary": id, "vuid": int, "why": str}}:
        #: LVS whose leadership is put back once both nodes serve it again
        self.lead_pending: dict = {}
        #: (cluster_id, lvs_name) restorations running / ms of the last attempt
        self.lead_running: set = set()
        self.lead_tried_at: dict = {}
        #: (cluster_id, node, code, message) -> None: cluster event log
        self.notify = notify or default_notify
        #: (db, winner, loser host nqn) -> {nqn: change}: revoke the loser's
        #: client host on the winner (kubernetes-fencing contract, 3.2)
        self.host_fence_revoke = host_fence_revoke or default_host_fence_revoke
        #: (db, winner, subsystems, loser host nqn) -> None: undo a revocation
        self.host_fence_release = host_fence_release or default_host_fence_release
        #: fenced node id -> ms of the last revocation or release attempt
        self.host_fence_tried_at: dict = {}
        #: node_id -> ms when its rebuild was first seen failing on unavailable devices
        self.rebuild_stuck_since: dict = {}
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
        #: node_id -> consecutive HTTP 500 answers from its SPDK proxy
        self.http500_streak: dict = {}
        #: node_id -> True once its SPDK is positively known gone (HTTP 500
        #: streak); cleared only after the returning node has been fenced
        self.spdk_lost: dict = {}
        #: node_id -> ms of the last jc_ha_status reconciliation
        self.reconciled_at: dict = {}
        #: node ids whose view must be resynced from jc_ha_status at once
        #: (reconnect, instance change, sequence gap)
        self.needs_resync: set = set()
        #: cluster_id -> ms of the last leadership reconciliation pass
        self.leadership_checked_at: dict = {}
        #: cluster_id -> LVS names seen leaderless on the previous pass
        self.leaderless_seen: dict = {}
        #: node_id -> SPDK instance the node had when its planned loss was
        #: first seen (the marker ends once it answers with another one)
        self.planned_instance: dict = {}
        #: node_id -> ms of the last unfence attempt of a self-fenced LVS (F4)
        self.self_fence_tried_at: dict = {}
        #: cluster_id -> partition recovery progress (F6): link_ok_since,
        #: probed_at, reconnected_at, restarted, busy
        self.partition_recovery: dict = {}
        self._lock = threading.Lock()

    # ---- membership ---------------------------------------------------------

    def members(self, cluster_id: str) -> list:
        nodes = [n for n in self.db.get_storage_nodes_by_cluster_id(cluster_id)
                 if n.status != StorageNode.STATUS_REMOVED]
        return sorted(nodes, key=lambda n: n.get_id())

    def eligible(self, cluster) -> list | None:
        """The two member nodes if the cluster is arbitrated, else None."""
        if not getattr(cluster, "two_node_arbitration", False):
            return None
        if not witness_authority(cluster):
            return None
        nodes = self.members(cluster.get_id())
        return nodes if len(nodes) == 2 else None

    def record(self, cluster_id: str, nodes: list) -> ClusterArbitration:
        rec = self.db.get_cluster_arbitration(cluster_id)
        if rec is None:
            rec = ClusterArbitration()
            rec.cluster_id = cluster_id
            rec.state_since = self.clock()
            rec.write_to_db(self.db.kv_store)
        return rec

    # ---- events -------------------------------------------------------------

    def consume_events(self, cluster_id: str) -> int:
        """Fold queued events into ``signals`` (in order per node) and remove them."""
        queued = self.db.get_arbitration_events(cluster_id)
        queued.sort(key=lambda e: (e.node_id, e.received_at, e.seq))
        for item in queued:
            legacy = item.instance == ev.LEGACY_INSTANCE
            if legacy and self.capable.get(item.node_id):
                # JC pushes a remote-JM event into both queues; on a node with
                # jc_wait_events the Go collector already delivered it.
                item.remove(self.db.kv_store)
                continue
            sig = self.signals.setdefault(item.node_id, ev.NodeSignals())
            event = dict(item.payload)
            event["instance"] = item.instance
            event.setdefault("seq", item.seq)
            if not legacy and event.get("status") != ev.ST_RESYNC:
                # The view is a mirror of the node, never an append-only log:
                # a new instance or a gap in the sequence means events were
                # lost (or the node restarted), so resync from jc_ha_status.
                seq = int(event.get("seq") or 0)
                if (sig.instance and item.instance != sig.instance) or \
                        (sig.last_seq and seq > sig.last_seq + 1):
                    self.needs_resync.add(item.node_id)
            # Legacy events were logged by the Python distr collector already.
            if ev.apply(sig, event) and not legacy:
                self.log_event(item, event)
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
            spdk_lost=bool(self.spdk_lost.get(nid)),
            planned_loss=nid in (rec.planned_loss or {}),
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
            winner=(rec.verdicts[-1].get("winner", "")
                    if rec.state == ARB_DEGRADED and rec.verdicts else ""),
            stale_nodes=tuple(sorted(rec.rebuild or {})),
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
        instance (a restarted node comes back with defaults), and again when
        the node's ``jc_ha_status`` shows arbitration off (``reconcile``)."""
        nid = node.get_id()
        instance = (self.signals.get(nid) or ev.NodeSignals()).instance
        if self.configured.get(nid) == instance and nid in self.configured:
            return
        err, level = set_dual_node(node.rpc_client(timeout=3, retry=0), True, True)
        if err or level == "plain":
            raise RuntimeError("jc_set_dual_node rejected the arbitration fields: %s" % (err,))
        if level != "full":
            logger.warning("Node %s does not take remote_jm_timeout_ms (older build); "
                           "frozen peers are detected late (campaign c2, F5)", nid)
        self.configured[nid] = instance

    def configure_due(self, rec: ClusterArbitration, nodes: list) -> None:
        """``configure`` for every node that needs it, on the work pool.

        Campaign c7 (F16): a jc_set_dual_node to a frozen node (its proxy up,
        SPDK not answering) blocked the decision thread for its whole timeout.
        At most one call per node in flight; a node is configured for the SPDK
        instance it had when the call was started."""
        for node in nodes:
            nid = node.get_id()
            if not self.capable.get(nid):
                continue
            instance = (self.signals.get(nid) or ev.NodeSignals()).instance
            if self.configured.get(nid) == instance and nid in self.configured:
                continue
            self._work(("configure", nid), {"node": node, "instance": instance},
                       self._configure_rpc, node)
        self._drain_work(rec, nodes)

    @staticmethod
    def _configure_rpc(node: StorageNode) -> str:
        err, level = set_dual_node(node.rpc_client(timeout=3, retry=0), True, True)
        if err or level == "plain":
            raise RuntimeError("jc_set_dual_node rejected the arbitration fields: %s" % (err,))
        return level

    def _work(self, key: tuple, ctx: dict, fn, *args) -> bool:
        """Start ``fn(*args)`` on the work pool unless ``key`` is in flight."""
        fut = self.inflight.get(key)
        if fut is not None and not fut.done():
            return False
        if fut is not None:
            return False                            # done, not applied yet: drained first
        self.work_ctx[key] = ctx
        self.inflight[key] = self.work_pool.submit(fn, *args)
        return True

    def _drain_work(self, rec: ClusterArbitration, nodes: list) -> None:
        """Apply, on the decision thread, every finished work-pool call.
        Never waits: a call still running is applied on a later tick."""
        by_id = {n.get_id(): n for n in nodes}
        for key in sorted(k for k in self.inflight if k[0] in _WORK_KINDS):
            fut = self.inflight[key]
            if not fut.done():
                continue
            del self.inflight[key]
            ctx = self.work_ctx.pop(key, {})
            try:
                result = ("ok", fut.result())
            except Exception as e:                  # noqa: BLE001 - per kind below
                result = ("err", e)
            kind = key[0]
            if kind == "configure":
                self._apply_configure(key[1], ctx, result)
            elif kind == "unfence":
                node = by_id.get(key[1])
                if node is not None:
                    self._apply_unfence(rec, nodes, node, ctx, result)
            elif kind == "leadership":
                self._apply_leadership(key[1], ctx, result)
            elif kind in ("hostfence", "hostrelease"):
                self._apply_host_fence(rec, kind, key[1], ctx, result)

    def _apply_configure(self, nid: str, ctx: dict, result: tuple) -> None:
        kind, value = result
        if kind == "err":
            logger.debug("jc_set_dual_node on %s: %s", nid, value)
            return
        if value != "full":
            logger.warning("Node %s does not take remote_jm_timeout_ms (older build); "
                           "frozen peers are detected late (campaign c2, F5)", nid)
        self.configured[nid] = ctx.get("instance")

    def renew(self, rec: ClusterArbitration, node: StorageNode,
              peer: StorageNode | None = None) -> None:
        """One lease renewal. A transport failure only marks the node
        unreachable: it is not a verdict (invariant 4). HTTP 500 from the
        node's proxy is different: repeated, it is positive evidence that SPDK
        is gone (``note_answer``).

        A node marked lost that answers again is fenced for every LVS its peer
        leads alone BEFORE it gets a lease again (``fence_returning_node``)."""
        nid = node.get_id()
        if self.spdk_lost.get(nid) and not self.fence_returning_node(rec, node, peer):
            return
        try:
            node.rpc_client(timeout=1, retry=0).jc_lease_renew(
                rec.epoch, constants.TWO_NODE_LEASE_TTL_MS)
            self.lease_granted_at[nid] = self.clock()
            self._mark_reachable(nid)
            self.note_answer(nid, http500=False)
        except RPCRemoteError as e:
            self._mark_reachable(nid)
            self.note_answer(nid, http500=False)
            if e.code == -errno.ESTALE:
                # The node saw a higher epoch than our record (arbiter failover
                # mid-verdict): move past it before sending anything else.
                self.raise_epoch(rec.cluster_id, at_least=self._seen_epoch(nid) + 1)
            else:
                logger.warning("Lease renewal on %s failed: %s", nid, e)
        except RPCHTTPError as e:
            self.reachable[nid] = False
            self.note_answer(nid, http500=(e.status_code == 500))
            logger.debug("Lease renewal on %s: HTTP %s", nid, e.status_code)
        except Exception as e:                      # noqa: BLE001 - transport
            self.reachable[nid] = False
            self.note_answer(nid, http500=False)
            logger.debug("Lease renewal on %s: node unreachable (%s)", nid, e)

    def _mark_reachable(self, nid: str) -> None:
        if self.reachable.get(nid) is False:
            # Reconnected: whatever happened meanwhile, the view is resynced.
            self.needs_resync.add(nid)
        self.reachable[nid] = True

    def note_answer(self, nid: str, http500: bool) -> None:
        """Track HTTP 500 answers of a node's SPDK proxy.

        The proxy answers 500 when it cannot reach SPDK on its socket, i.e. a
        dead SPDK behind a running proxy (campaign c1: 01b, 02b). That is loss
        of the node, like positive fencing: the peer may be granted at once.
        One 500 alone can be a transient SPDK timeout (the proxy answers 500 on
        those too), so it takes ``TWO_NODE_SPDK_DOWN_HTTP500_STREAK``
        consecutive 500 answers (lease renewals and status polls alike); any
        other outcome resets the streak.
        """
        if not http500:
            self.http500_streak[nid] = 0
            return
        streak = self.http500_streak.get(nid, 0) + 1
        self.http500_streak[nid] = streak
        if streak >= int(constants.TWO_NODE_SPDK_DOWN_HTTP500_STREAK) and not self.spdk_lost.get(nid):
            self.spdk_lost[nid] = True
            logger.warning("Node %s: SPDK proxy answered HTTP 500 %d times in a row; "
                           "SPDK counts as down (node lost)", nid, streak)

    def fence_returning_node(self, rec: ClusterArbitration, node: StorageNode,
                             peer: StorageNode | None) -> bool:
        """A node marked lost (``spdk_lost``) answers again: before anything
        else, fence it for every LVS its peer leads alone.

        The LVS set comes from the peer's own, fresh ``jc_ha_status`` (never
        from an accumulated view); only if the peer cannot be read, from the
        solo grants in force in the record. Returns True when the node may be
        treated as an ordinary member again: fenced; nothing to fence (a
        transient 500 that never led to a grant); or a fresh SPDK that does not
        know the LVS (ENOENT/EINVAL: restarted, so it cannot be a stale writer;
        the restart flow re-admits it).
        """
        nid = node.get_id()
        try:
            # Cheap probe first: while the node is still gone, nothing else is
            # done (no epoch is raised every renewal period).
            node.rpc_client(timeout=1, retry=0).jc_ha_status()
        except RPCRemoteError:
            pass                                    # SPDK answers (e.g. EINVAL on a fresh one)
        except Exception as e:                      # noqa: BLE001 - still gone
            logger.debug("Lost node %s still not answering: %s", nid, e)
            return False
        vuids = self._peer_solo_lvs(rec, nid, peer)
        if vuids:
            fresh = self.raise_epoch(rec.cluster_id)
            try:
                node.rpc_client(timeout=5, retry=0).jc_fence(fresh.epoch, vuids)
            except RPCRemoteError as e:
                if e.code == -errno.ESTALE:
                    self.raise_epoch(rec.cluster_id, at_least=self._seen_epoch(nid) + 1)
                    return False
                if e.code not in (-errno.ENOENT, -errno.EINVAL):
                    logger.warning("Fencing returning node %s failed: %s", nid, e)
                    return False
                logger.info("Returning node %s does not know LVS %s (%s): restarted, "
                            "nothing to fence", nid, vuids, e)
            except Exception as e:                  # noqa: BLE001 - still gone
                logger.debug("Returning node %s not reachable for the fence: %s", nid, e)
                return False
            else:
                logger.warning("Returning node %s fenced for LVS %s at epoch %s (its peer "
                               "leads them alone)", nid, vuids, fresh.epoch)

                def put(f):
                    for v in vuids:
                        f.lvs_entry(v)["fenced_node"] = nid
                self.db.atomic_update(fresh, put)
                sig = self.signals.setdefault(nid, ev.NodeSignals())
                for v in vuids:
                    sig.lvs_state[v] = LVS_FENCED
        self.spdk_lost.pop(nid, None)
        self.http500_streak[nid] = 0
        self.needs_resync.add(nid)
        return True

    def _peer_solo_lvs(self, rec: ClusterArbitration, nid: str, peer: StorageNode | None) -> list:
        if peer is not None:
            try:
                status = peer.rpc_client(timeout=1, retry=0).jc_ha_status() or {}
                return sorted(int(x["jm_vuid"]) for x in status.get("lvs", [])
                              if x.get("state") == LVS_SOLO)
            except Exception as e:                  # noqa: BLE001 - fall back to the record
                logger.debug("jc_ha_status on %s failed (%s); using the record", peer.get_id(), e)
        peer_id = peer.get_id() if peer is not None else None
        return sorted(int(e["jm_vuid"]) for e in rec.lvs
                      if e.get("state") == LVS_SOLO and e.get("leader")
                      and e.get("leader") != nid and (peer_id is None or e.get("leader") == peer_id))

    def reconcile(self, node: StorageNode) -> bool:
        """Replace this node's per-LVS view (holding/solo/fenced and remote-JM
        health) with its ``jc_ha_status``.

        Bookkeeping requirement (campaign c1, 08 run 1): the view is a mirror of
        the node, never an append-only log of past events; an append-only view
        fenced a legitimate survivor. Called every ``TWO_NODE_RECONCILE_MS``,
        and at once after a reconnect, an instance change or a sequence gap.
        Returns True when the view was replaced."""
        return self.apply_status(node, self.fetch_status(node))

    @staticmethod
    def fetch_status(node: StorageNode) -> tuple:
        """The RPC half of ``reconcile``: no arbiter state is touched, so it
        may run on the RPC pool. ("ok", status) | ("http", code) | ("err", e)."""
        try:
            return "ok", node.rpc_client(timeout=1, retry=0).jc_ha_status() or {}
        except RPCHTTPError as e:
            return "http", e.status_code
        except Exception as e:                      # noqa: BLE001 - next poll retries
            return "err", e

    def apply_status(self, node: StorageNode, result: tuple) -> bool:
        """The bookkeeping half of ``reconcile``; decision-loop thread only."""
        nid = node.get_id()
        kind, value = result
        if kind == "http":
            self.note_answer(nid, http500=(value == 500))
            return False
        if kind == "err":
            self.note_answer(nid, http500=False)
            logger.debug("jc_ha_status on %s failed: %s", nid, value)
            return False
        status = value
        self.note_answer(nid, http500=False)
        sig = self.signals.setdefault(nid, ev.NodeSignals())
        before = (sig.instance, dict(sig.lvs_state))
        ev.apply(sig, {"status": ev.ST_RESYNC, "instance": str(status.get("instance", "")),
                       "seq": int(status.get("seq", 0) or 0), "ha_status": status})
        self.reconciled_at[nid] = self.clock()
        self.needs_resync.discard(nid)
        if before[1] != sig.lvs_state:
            logger.info("Two-node view of %s reconciled from jc_ha_status: %s -> %s",
                        nid, before[1], sig.lvs_state)
        if before[0] and before[0] != sig.instance:
            # New SPDK instance: it comes back with defaults, configure again.
            self.configured.pop(nid, None)
        if status.get("arbitration") is False:
            # Something (an old restart flow) switched arbitration off.
            self.configured.pop(nid, None)
        timeout = status.get("remote_jm_timeout_ms")
        if timeout is not None and int(timeout) != int(constants.TWO_NODE_REMOTE_JM_TIMEOUT_MS):
            # A node that reports the field runs another acknowledgement
            # timeout than ours (campaign c2, F5): configure it again.
            self.configured.pop(nid, None)
        return True

    def reconcile_due(self, nodes: list) -> None:
        """Poll ``jc_ha_status`` of every node that is due, concurrently.

        Campaign c3 (02a): each call to a frozen node blocks for its whole
        timeout, and polled in turn it delayed the decision about its peer by
        that much (grant at +2.66 s instead of about +1.9 s). Polls now run on
        the RPC pool, at most one per node; the tick waits up to
        ``TWO_NODE_POLL_WAIT_MS`` and applies, in node order, what has arrived.
        A poll still running is applied on a later tick, never started twice."""
        now = self.clock()
        by_id = {n.get_id(): n for n in nodes}
        for node in nodes:
            nid = node.get_id()
            if not self.capable.get(nid) or self.spdk_lost.get(nid):
                continue
            due = now - self.reconciled_at.get(nid, 0) >= int(constants.TWO_NODE_RECONCILE_MS)
            if (due or nid in self.needs_resync) and ("status", nid) not in self.inflight:
                self.inflight[("status", nid)] = self.rpc_pool.submit(self.fetch_status, node)
        pending = {k: f for k, f in self.inflight.items() if k[0] == "status" and k[1] in by_id}
        if not pending:
            return
        if not all(f.done() for f in pending.values()):
            wait(list(pending.values()), timeout=int(constants.TWO_NODE_POLL_WAIT_MS) / 1000.0,
                 return_when=ALL_COMPLETED)
        for key in sorted(pending):
            fut = pending[key]
            if not fut.done():
                continue
            del self.inflight[key]
            try:
                result = fut.result()
            except Exception as e:                  # noqa: BLE001 - next poll retries
                result = ("err", e)
            self.apply_status(by_id[key[1]], result)

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
            for vuid, fenced in verdict.fence_all.items():
                entry = fresh.lvs_entry(vuid)
                entry.update(leader="", state="fenced", fenced_node=",".join(fenced), since=now)
            if verdict.loser and verdict.kind == dec.V_PARTITION:
                if verdict.loser not in fresh.taint_requests:
                    fresh.taint_requests.append(verdict.loser)
            if verdict.raise_epoch:
                fresh.record_verdict(
                    {"epoch": fresh.epoch, "at": now, "kind": verdict.kind,
                     "winner": verdict.winner, "loser": verdict.loser,
                     "grant": {str(k): v for k, v in verdict.grant.items()},
                     "fence": {str(k): v for k, v in verdict.fence.items()},
                     "fence_all": {str(k): list(v) for k, v in verdict.fence_all.items()},
                     "reason": verdict.reason,
                     "host_fence": bool(verdict.host_fence),
                     "from_state": snap.state,
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
        for vuid, nids in verdict.fence_all.items():
            for nid in nids:
                if int(vuid) not in fences.setdefault(nid, []):
                    fences[nid].append(int(vuid))
        for nid, vuids in fences.items():
            # A best-effort fence (planned loss) never holds the grant up for
            # long (campaign c7, F16); a required one may take its full timeout.
            timeout = (int(constants.TWO_NODE_BEST_EFFORT_FENCE_TIMEOUT_S)
                       if verdict.fence_best_effort else 5)
            try:
                by_id[nid].rpc_client(timeout=timeout, retry=0).jc_fence(rec.epoch, sorted(vuids))
            except Exception as e:                  # noqa: BLE001
                stale = isinstance(e, RPCRemoteError) and e.code == -errno.ESTALE
                if verdict.fence_best_effort and not stale:
                    # A node being stopped by the control plane (planned
                    # loss): it is going away; the grant does not wait.
                    logger.info("jc_fence epoch %s on %s (planned loss) failed: %s; "
                                "granting its peer anyway", rec.epoch, nid, e)
                    continue
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
        node to the restart flow once.

        Not after a partition (campaign c2, F6): ``fence_both`` granted nobody,
        so no node wrote alone and none needs to be rebuilt; unfence (the
        leader re-levels its journal) and the leadership reconciliation that
        follows in steady are the whole heal. The restart flow re-admits only
        a node that lost while its peer ran solo (DEGRADED)."""
        done = self.readmitted.setdefault(rec.cluster_id, set())
        from_partition = self._healing_from(rec) == ARB_PARTITIONED
        if from_partition:
            done.update(n.get_id() for n in nodes)
        for node in nodes:
            nid = node.get_id()
            sig = self.signals.get(nid) or ev.NodeSignals()
            fenced = sorted(v for v, s in sig.lvs_state.items() if s == LVS_FENCED)
            if fenced:
                # Device check + jc_unfence on the work pool (campaign c7,
                # F16): a node freezing mid-heal never blocks the decisions.
                self._work(("unfence", nid),
                           {"why": "heal", "epoch": rec.epoch, "vuids": fenced,
                            "from_partition": from_partition},
                           self._unfence_rpc, node, rec.epoch, fenced)
        self._drain_work(rec, nodes)

    def _unfence_rpc(self, node: StorageNode, epoch: int, vuids: list) -> tuple:
        """The node half of an unfence (work pool): the device check
        (campaign c4, F12), then ``jc_unfence``. No arbiter state is touched.
        ("devices", True|False|None|exc) when the check did not pass, else
        ("unfence", None) or ("unfence", RPC error)."""
        try:
            ok = self.devices_ok(self.db, node)
        except Exception as e:                      # noqa: BLE001 - applied as "could not tell"
            return "devices", e
        if not ok:
            return "devices", ok
        try:
            node.rpc_client(timeout=5, retry=0).jc_unfence(epoch, vuids)
        except Exception as e:                      # noqa: BLE001 - applied per code
            return "unfence", e
        return "unfence", None

    def _apply_unfence(self, rec: ClusterArbitration, nodes: list, node: StorageNode,
                       ctx: dict, result: tuple) -> None:
        """The bookkeeping half of a heal or self-fence unfence (decision
        thread): the same steps the synchronous version took."""
        nid, why, vuids = node.get_id(), ctx.get("why", ""), ctx.get("vuids", [])
        heal = why == "heal"
        done = self.readmitted.setdefault(rec.cluster_id, set())
        kind, value = result
        # ("ok", (step, value)) from _unfence_rpc; a raised call counts as a
        # failed jc_unfence.
        kind, value = value if kind == "ok" else ("unfence", value)
        what = "heal" if heal else "self-fence re-admission"
        if kind == "devices":
            if isinstance(value, Exception):
                logger.warning("Device check of %s before %s failed: %s", nid, what, value)
            elif value is None:
                logger.info("Device check of %s before %s inconclusive; re-admission later",
                            nid, what)
            else:
                self.recover_devices(rec, node, "data devices missing or not online at %s"
                                     % what)
                if heal:
                    done.add(nid)                   # handed to the restart flow (F12)
            return
        if value is not None:
            e = value
            if isinstance(e, RPCRemoteError) and e.code == -errno.EAGAIN:
                logger.info("Node %s resyncing (%s); unfence later", nid, what)
            elif isinstance(e, RPCRemoteError) and e.code == -errno.ESTALE and not heal:
                self.raise_epoch(rec.cluster_id, at_least=self._seen_epoch(nid) + 1)
            elif isinstance(e, RPCRemoteError):
                logger.warning("jc_unfence (%s) of %s on %s failed: %s", what, vuids, nid, e)
            else:
                logger.warning("jc_unfence (%s) on %s unreachable: %s", what, nid, e)
            return
        sig = self.signals.setdefault(nid, ev.NodeSignals())
        for v in vuids:
            sig.lvs_state[v] = LVS_NORMAL
        if heal:
            # Campaign c5, 07/08: the fence demoted the LVS and only
            # bdev_lvol_set_leader brings leadership (and the primary's
            # ANA) back; nothing else does that promptly after an unfence.
            self.request_leadership(rec, nodes, "heal unfence of %s" % nid)
            if not ctx.get("from_partition"):
                # Its peer ran alone meanwhile (F10): rebuild before the
                # pair counts as redundant again.
                self.mark_rebuild(rec, node, "re-admitted after a degraded verdict")
            if nid not in done:
                self.readmit(node)
                done.add(nid)
            return
        self.request_leadership(rec, nodes, "self-fence re-admission of %s" % nid)
        self.needs_resync.add(nid)
        logger.warning("Self-fenced node %s re-admitted for LVS %s at epoch %s "
                       "(remote JM healthy again)", nid, vuids, ctx.get("epoch"))
        # Campaign c3, F10: the unfence alone left the writes its peer took
        # meanwhile on the peer only (02b -> 03: EIO). Rebuild first.
        self.mark_rebuild(rec, node, "re-admitted after a self-fence (%s)" % ",".join(
            str(v) for v in vuids))

    @staticmethod
    def _healing_from(rec: ClusterArbitration) -> str:
        for verdict in reversed(rec.verdicts or []):
            if verdict.get("kind") == dec.V_HEAL:
                return verdict.get("from_state", "")
        return ""

    # ---- planned loss (campaign c2, F3) ---------------------------------------

    def track_planned_loss(self, rec: ClusterArbitration, nodes: list) -> ClusterArbitration:
        """End planned-loss markers: the node answers with a new SPDK instance
        (it was restarted), it still answers with the same instance well after
        the marker (the stop never happened), or the marker is too old."""
        markers = rec.planned_loss or {}
        if not markers:
            return rec
        now = self.clock()
        ended = []
        for node in nodes:
            nid = node.get_id()
            marker = markers.get(nid)
            if marker is None:
                self.planned_instance.pop(nid, None)
                continue
            sig = self.signals.get(nid) or ev.NodeSignals()
            before = marker.get("instance") or self.planned_instance.setdefault(nid, sig.instance)
            age = now - int(marker.get("at", 0))
            answered = self.reconciled_at.get(nid, 0) > int(marker.get("at", 0))
            if answered and sig.instance and before and sig.instance != before:
                ended.append((nid, "back with a new SPDK instance"))
            elif answered and sig.instance == before and age > 60_000 and self.reachable.get(nid):
                ended.append((nid, "still serving the same SPDK instance; the stop did not happen"))
            elif age > int(constants.TWO_NODE_PLANNED_LOSS_MAX_S) * 1000:
                ended.append((nid, "marker expired"))
        if not ended:
            return rec

        def put(fresh):
            fresh.planned_loss = {k: v for k, v in (fresh.planned_loss or {}).items()
                                  if k not in {nid for nid, _ in ended}}
        for nid, why in ended:
            self.planned_instance.pop(nid, None)
            logger.info("Planned loss of %s ended: %s", nid, why)
        return self.db.atomic_update(rec, put)

    # ---- self-fenced nodes (campaign c2, F4) -----------------------------------

    def readmit_self_fenced(self, rec: ClusterArbitration, nodes: list) -> None:
        """Unfence an LVS a node fenced by itself (hold deadline, lease expiry)
        once its remote JM is healthy again (a stalled node aborts instead).

        Without a verdict in force nothing else ever unfences it (campaign c2:
        30 min fenced in STEADY, LVS leaderless, rebuild stalled). Only in
        STEADY and DEGRADED, only for LVS the arbiter did not fence itself
        (the record's ``fenced_node``), and only while the node's own view says
        the remote JM is healthy: ``jc_unfence`` answers EAGAIN until it is.
        Leadership then comes back through ``maybe_reconcile_leadership``."""
        if rec.state not in (ARB_STEADY, ARB_DEGRADED):
            self._drain_work(rec, nodes)
            return
        now = self.clock()
        arbiter_fenced: dict = {}
        for entry in rec.lvs:
            names = [x for x in str(entry.get("fenced_node") or "").split(",") if x]
            arbiter_fenced[int(entry.get("jm_vuid", -1))] = set(names)
        for node in nodes:
            nid = node.get_id()
            if not self.reachable.get(nid, True) or self.spdk_lost.get(nid) \
                    or nid in (rec.planned_loss or {}):
                continue
            sig = self.signals.get(nid) or ev.NodeSignals()
            vuids = sorted(int(v) for v, st in sig.lvs_state.items()
                           if st == LVS_FENCED and sig.peer_unhealthy.get(v) is False
                           and nid not in arbiter_fenced.get(int(v), set()))
            if not vuids:
                continue
            if now - self.self_fence_tried_at.get(nid, 0) < int(constants.TWO_NODE_SELF_FENCE_READMIT_MS):
                continue
            if self._work(("unfence", nid),
                          {"why": "self-fence", "epoch": rec.epoch, "vuids": vuids},
                          self._unfence_rpc, node, rec.epoch, vuids):
                self.self_fence_tried_at[nid] = now
        self._drain_work(rec, nodes)

    # ---- device check before re-admission (campaign c4, F12) -------------------

    def devices_ready(self, rec: ClusterArbitration, node: StorageNode, why: str) -> bool:
        """True when ``node``'s data devices exist and are online, so an
        unfence re-admits a full replica.

        Campaign c4, 02a: the node woken from a 4 s freeze had its alceml
        stuck-IO watchdog shut both data devices down (bdev gone, -19) while
        the control plane, the cluster maps and the NVMe controllers all still
        showed them online. The unfence re-admitted a node without devices,
        and the rebuild queued for it failed DST_NOT_AVAILABLE for 25 min.
        Such a node is not unfenced; it goes through the restart flow, which
        recreates its devices and queues the rebuild itself. ``None`` (could
        not tell, e.g. the RPC timed out) defers to the next attempt."""
        try:
            ok = self.devices_ok(self.db, node)
        except Exception as e:                      # noqa: BLE001 - next attempt retries
            logger.warning("Device check of %s before %s failed: %s", node.get_id(), why, e)
            return False
        if ok is None:
            logger.info("Device check of %s before %s inconclusive; re-admission later",
                        node.get_id(), why)
            return False
        if ok:
            return True
        self.recover_devices(rec, node, "data devices missing or not online at %s" % why)
        return False

    def recover_devices(self, rec: ClusterArbitration, node: StorageNode, reason: str) -> None:
        """Hand ``node`` to the restart flow (``readmit``), at most once per
        ``TWO_NODE_DEVICE_RECOVERY_RETRY_S``; recorded in ``device_recovery``
        and in the cluster event log."""
        nid, now = node.get_id(), self.clock()
        fresh_rec = self.db.get_cluster_arbitration(rec.cluster_id) or rec
        last = int(((fresh_rec.device_recovery or {}).get(nid) or {}).get("at") or 0)
        if last and now - last < int(constants.TWO_NODE_DEVICE_RECOVERY_RETRY_S) * 1000:
            return

        def put(fresh):
            fresh.device_recovery = dict(fresh.device_recovery or {})
            fresh.device_recovery[nid] = {"reason": reason, "at": now}
        self.db.atomic_update(fresh_rec, put)
        logger.error("Node %s: %s; handing it to the restart flow", nid, reason)
        self.notify(rec.cluster_id, node, "TWO_NODE_DEVICE_RECOVERY", reason)
        self.readmit(node)

    # ---- leadership after an unfence (campaign c5, 07/08) -----------------------

    def request_leadership(self, rec: ClusterArbitration, nodes: list, why: str) -> None:
        """Queue every member's primary LVS for leadership restoration.

        Campaign c5, 07 and 08: after a partition both nodes are fenced; the
        fence demotes the LVS, latches the IO-driven takeover off and sets the
        listeners ANA inaccessible (simplyblock/spdk#76). From then on only
        ``bdev_lvol_set_leader`` restores leadership and the primary's ANA. The
        heal only unfenced, so both LVS stayed leaderless (vol1 inaccessible,
        rebuild deferred on "not the leader") until the general leaderless
        recovery, which runs only once the cluster is steady again and needs
        two leaderless passes (> 60 s). Restored by
        ``restore_pending_leadership`` once both nodes serve the LVS."""
        pend = self.lead_pending.setdefault(rec.cluster_id, {})
        for node in nodes:
            fresh = self.db.get_storage_node_by_id(node.get_id())
            lvs_name = getattr(fresh, "lvstore", "")
            vuid = int(getattr(fresh, "jm_vuid", 0) or 0)
            if not lvs_name or not vuid or getattr(fresh, "is_secondary_node", False):
                continue
            pend.setdefault(lvs_name, {"primary": fresh.get_id(), "vuid": vuid, "why": why})

    def _serves(self, rec: ClusterArbitration, node: StorageNode, vuid: int) -> bool:
        """``node`` is reachable, not lost, and serves ``vuid``: the LVS is
        normal there and its remote JM is healthy (the resync finished)."""
        nid = node.get_id()
        if not self.reachable.get(nid, True) or self.spdk_lost.get(nid) \
                or nid in (rec.planned_loss or {}):
            return False
        sig = self.signals.get(nid) or ev.NodeSignals()
        state = sig.lvs_state.get(vuid, sig.lvs_state.get(str(vuid), LVS_NORMAL))
        healthy = sig.peer_unhealthy.get(vuid, sig.peer_unhealthy.get(str(vuid)))
        return state == LVS_NORMAL and healthy is False

    def restore_pending_leadership(self, rec: ClusterArbitration, nodes: list) -> None:
        """Put each queued LVS's leadership back on its primary.

        Order: the heal or re-admission unfenced (``request_leadership``),
        then both nodes serve the LVS again (normal, remote JM healthy: the
        unfence gate), then ``restore_leadership`` reloads the lvstore
        metadata on the primary, grants leadership, verifies it and sets ANA
        (optimized on the primary, non_optimized on the secondary). Off the
        decision loop, idempotent (an LVS already led is only re-checked for
        ANA), retried every ``TWO_NODE_LEADERSHIP_RETRY_MS`` on failure;
        ``restore_leadership`` leaves the LVS alone while a restart or
        port-allow task moves leadership. Dropped on a new fence verdict."""
        pend = self.lead_pending.get(rec.cluster_id)
        if not pend or rec.state == ARB_PARTITIONED:
            return
        by_id = {n.get_id(): n for n in nodes}
        now = self.clock()
        cid, epoch = rec.cluster_id, rec.epoch
        for lvs_name, entry in list(pend.items()):
            primary = by_id.get(entry["primary"])
            peer = next((n for n in nodes if n.get_id() != entry["primary"]), None)
            if primary is None or peer is None:
                pend.pop(lvs_name, None)
                continue
            vuid = int(entry["vuid"])
            if not (self._serves(rec, primary, vuid) and self._serves(rec, peer, vuid)):
                continue
            key = (cid, lvs_name)
            if key in self.lead_running or \
                    now - self.lead_tried_at.get(key, 0) < int(constants.TWO_NODE_LEADERSHIP_RETRY_MS):
                continue
            self.lead_tried_at[key] = now
            self.lead_running.add(key)
            self.run_async(self._restore_one(cid, epoch, key, lvs_name, primary, peer,
                                             entry.get("why", "")))

    def _restore_one(self, cid, epoch, key, lvs_name, primary, peer, why):
        def run():
            ok = False
            try:
                ok = bool(self.restore_leadership(cid, primary, peer, lvs_name))
            except Exception as e:                  # noqa: BLE001 - retried
                logger.error("Restoring leadership of %s on %s failed: %s",
                             lvs_name, primary.get_id(), e)
            at = self.clock()

            def put(fresh):
                fresh.leadership_restored = dict(fresh.leadership_restored or {})
                fresh.leadership_restored[lvs_name] = {
                    "node": primary.get_id(), "epoch": epoch, "at": at, "ok": ok, "why": why}
            cur = self.db.get_cluster_arbitration(cid)
            if cur is not None:
                self.db.atomic_update(cur, put)
            self.lead_running.discard(key)
            if ok:
                (self.lead_pending.get(cid) or {}).pop(lvs_name, None)
                self.lead_tried_at.pop(key, None)
                logger.warning("LVS %s: leadership restored on %s after %s",
                               lvs_name, primary.get_id(), why)
                self.notify(cid, primary, "TWO_NODE_LEADERSHIP_RESTORED",
                            "LVS %s: leadership restored after %s" % (lvs_name, why))
            else:
                logger.warning("LVS %s: leadership not restored on %s yet; retrying",
                               lvs_name, primary.get_id())
        return run

    # ---- partition recovery (campaign c2, F6) ----------------------------------

    def recover_partition(self, rec: ClusterArbitration, nodes: list) -> None:
        """Bring a partitioned pair back when the link between them answers.

        After a partition, a both-links cut or a lossy link healed, the remote
        JMs stayed unavailable and both nodes fenced: the pair never became
        healthy, so the stable-window heal never started (campaign c2: 07, 08,
        10b). While PARTITIONED with both nodes reachable by the CP, the link
        between them is probed every ``TWO_NODE_LINK_PROBE_S``; while it
        answers but the remote JMs stay unhealthy, the remote JMs are
        reconnected every ``TWO_NODE_JM_RECONNECT_S``, and after
        ``TWO_NODE_PARTITION_RESTART_AFTER_S`` one node is handed to the
        restart flow once (the manual c1/c2 remedy). Healthy remote JMs then
        lead to the ordinary stable-window heal. Slow work runs off the
        decision loop."""
        cid = rec.cluster_id
        if rec.state != ARB_PARTITIONED:
            self.partition_recovery.pop(cid, None)
            return
        st = self.partition_recovery.setdefault(cid, {"link_ok_since": 0, "probed_at": 0,
                                                       "reconnected_at": 0, "restarted": False,
                                                       "busy": False})
        a, b = nodes
        views = [self.signals.get(n.get_id()) or ev.NodeSignals() for n in nodes]
        if not all(self.reachable.get(n.get_id(), True) and not self.spdk_lost.get(n.get_id())
                   for n in nodes):
            st["link_ok_since"] = 0
            return
        if not any(v.reports_peer_unhealthy() for v in views):
            return                                  # healthy again: the heal takes over
        now = self.clock()
        with self._lock:
            if st["busy"]:
                return
        if now - st["probed_at"] >= int(constants.TWO_NODE_LINK_PROBE_S) * 1000:
            st["probed_at"] = now
            self._spawn(st, lambda: self._probe_link(st, a, b))
            with self._lock:
                if st["busy"]:
                    return                          # the probe runs; act on its result next tick
        if not st["link_ok_since"]:
            return
        if now - st["reconnected_at"] >= int(constants.TWO_NODE_JM_RECONNECT_S) * 1000:
            st["reconnected_at"] = now
            logger.warning("Cluster %s partitioned, the link answers again but the remote "
                           "JMs are unavailable: reconnecting them", cid)
            self._spawn(st, lambda: (self.reconnect_jms(a, b), self.reconnect_jms(b, a)))
            return
        if not st["restarted"] and \
                now - st["link_ok_since"] >= int(constants.TWO_NODE_PARTITION_RESTART_AFTER_S) * 1000:
            st["restarted"] = True
            target = self._restart_target(rec, nodes)
            logger.warning("Cluster %s partitioned, link up for %ss and the remote JMs still "
                           "unavailable: handing %s to the restart flow", cid,
                           (now - st["link_ok_since"]) // 1000, target.get_id())
            self.readmit(target)

    def _spawn(self, st: dict, fn) -> None:
        with self._lock:
            st["busy"] = True

        def run():
            try:
                fn()
            except Exception as e:                  # noqa: BLE001 - next round retries
                logger.warning("Partition recovery step failed: %s", e)
            finally:
                with self._lock:
                    st["busy"] = False
        self.run_async(run)

    def _probe_link(self, st: dict, a: StorageNode, b: StorageNode) -> None:
        ok_ab, ok_ba = self.link_probe(a, b), self.link_probe(b, a)
        if ok_ab and ok_ba:
            if not st["link_ok_since"]:
                st["link_ok_since"] = self.clock()
                logger.info("Link between %s and %s answers again", a.get_id(), b.get_id())
        elif ok_ab is False or ok_ba is False:
            st["link_ok_since"] = 0

    def _restart_target(self, rec: ClusterArbitration, nodes: list) -> StorageNode:
        """The node whose primary LVS has no leader in the record, else the
        second member (the first keeps serving the metadata reads)."""
        leaders = {int(e.get("jm_vuid", -1)): e.get("leader", "") for e in rec.lvs}
        for node in nodes:
            if node.jm_vuid and not leaders.get(int(node.jm_vuid)):
                return node
        return nodes[1]

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
        # Campaign c3, F10: healed means redundant again, i.e. rebuilt.
        return not (rec.rebuild or {})


    # ---- data rebuild after re-admission (campaign c3, F10) -------------------

    def mark_rebuild(self, rec: ClusterArbitration, node: StorageNode, reason: str) -> None:
        """Record that ``node`` lacks the writes its peer took while it was
        fenced. ``progress_rebuild`` queues the rebuild and clears the mark once
        it has finished; until then the node is never granted solo
        (``decision._degraded_verdict``) and healing does not finish."""
        nid, now = node.get_id(), self.clock()

        def put(fresh):
            if nid in (fresh.rebuild or {}):
                return False
            fresh.rebuild = dict(fresh.rebuild or {})
            fresh.rebuild[nid] = {"reason": reason, "since": now, "scheduled_at": 0,
                                  "queued": False}
        self.db.atomic_update(self.db.get_cluster_arbitration(rec.cluster_id) or rec, put)
        self.rebuild_checked_at.pop(rec.cluster_id, None)
        logger.warning("Node %s %s: data rebuild required before the pair is redundant again",
                       nid, reason)

    def progress_rebuild(self, rec: ClusterArbitration, nodes: list) -> ClusterArbitration:
        """Queue the rebuild of every marked node once it is ONLINE, and clear
        the mark when the rebuild tasks it queued have all finished.

        The rebuild is the same one a node restart queues
        (``tasks_controller.add_device_mig_task_for_node``). It is cleared only
        after tasks were actually queued (or found open) and none is open any
        more: "no open tasks" alone is what made c3's 02b heal report "rebuild
        complete" without ever starting one."""
        marks = dict(rec.rebuild or {})
        if not marks:
            return rec
        cid, now = rec.cluster_id, self.clock()
        if now - self.rebuild_checked_at.get(cid, 0) < int(constants.TWO_NODE_REBUILD_CHECK_S) * 1000:
            return rec
        self.rebuild_checked_at[cid] = now
        by_id = {n.get_id(): n for n in nodes}
        try:
            open_tasks = int(self.open_rebuild_tasks(cid))
        except Exception as e:                      # noqa: BLE001 - next check retries
            logger.warning("Cannot read the rebuild tasks of %s: %s", cid, e)
            return rec
        changed: dict = {}
        finished = []
        try:
            errors = int(self.rebuild_task_errors(self.db, cid) or 0) if open_tasks else 0
        except Exception as e:                      # noqa: BLE001 - stuck check skipped this round
            logger.debug("Cannot read the rebuild task errors of %s: %s", cid, e)
            errors = 0
        for nid, mark in marks.items():
            mark = dict(mark)
            node = by_id.get(nid)
            if node is None:
                finished.append(nid)                # removed from the cluster
                continue
            retry = int(constants.TWO_NODE_REBUILD_SCHEDULE_RETRY_S) * 1000
            if not mark.get("queued") and now - int(mark.get("scheduled_at") or 0) >= retry:
                # Only once the node is back and serving: a rebuild queued while
                # it is still fenced, holding or down would run (and finish)
                # before the writes it is meant to copy have stopped coming.
                fresh = self.db.get_storage_node_by_id(nid)
                sig = self.signals.get(nid) or ev.NodeSignals()
                back = (fresh.status == StorageNode.STATUS_ONLINE
                        and self.reachable.get(nid, True) and not self.spdk_lost.get(nid)
                        and not any(st in (LVS_FENCED, LVS_HOLDING)
                                    for st in sig.lvs_state.values()))
                if back:
                    try:
                        queued = bool(self.schedule_rebuild(fresh))
                    except Exception as e:          # noqa: BLE001 - retried
                        logger.warning("Queueing the rebuild of %s failed: %s", nid, e)
                        queued = False
                    # Tasks already open (the restart flow queued them, so the
                    # call above found duplicates) cover the node as well.
                    queued = queued or bool(open_tasks)
                    mark["scheduled_at"] = now
                    if queued:
                        mark["queued"] = True
                        logger.info("Data rebuild queued for re-admitted node %s", nid)
                    else:
                        logger.warning("Data rebuild for %s not queued (nothing to queue now); "
                                       "retrying in %ss", nid,
                                       constants.TWO_NODE_REBUILD_SCHEDULE_RETRY_S)
                    changed[nid] = mark
                    continue
            if mark.get("queued") and not open_tasks:
                finished.append(nid)
                self.rebuild_stuck_since.pop(nid, None)
                continue
            if mark.get("queued"):
                self._check_rebuild_stuck(rec, node, mark, errors, now)
            if mark != marks[nid]:
                changed[nid] = mark
        if not changed and not finished:
            return rec

        def put(fresh):
            cur = dict(fresh.rebuild or {})
            for nid, mark in changed.items():
                if nid in cur:
                    cur[nid] = mark
            for nid in finished:
                cur.pop(nid, None)
            fresh.rebuild = cur
            if finished and fresh.data_unavailable.get("node") in finished:
                fresh.data_unavailable = {}
        rec = self.db.atomic_update(rec, put)
        for nid in finished:
            logger.info("Data rebuild of %s finished; the pair is redundant again", nid)
        return rec

    def _check_rebuild_stuck(self, rec: ClusterArbitration, node: StorageNode, mark: dict,
                             errors: int, now: int) -> None:
        """Campaign c4, F12: a rebuild whose migrations keep failing because a
        source or destination device is not available (``mig error`` bits
        SRC/DST_NOT_AVAILABLE) never finishes by retrying. After
        ``TWO_NODE_REBUILD_STUCK_S`` it is flagged (``stuck`` in the mark, an
        event, ``rebuild_stuck`` in the API) and the re-admitted node is handed
        to the restart flow, which recreates its devices."""
        nid = node.get_id()
        if not errors & MIG_UNAVAILABLE_BITS:
            self.rebuild_stuck_since.pop(nid, None)
            return
        since = self.rebuild_stuck_since.setdefault(nid, now)
        if mark.get("stuck") or now - since < int(constants.TWO_NODE_REBUILD_STUCK_S) * 1000:
            return
        mark.update(stuck=True, stuck_at=now, stuck_error=int(errors))
        reason = ("data rebuild stuck for %ss: migrations fail with mig error %s (%s)"
                  % ((now - since) // 1000, errors, describe_mig_error(errors)))
        self.notify(rec.cluster_id, node, "TWO_NODE_REBUILD_STUCK", reason)
        self.recover_devices(rec, node, reason)

    def report_unavailable(self, rec: ClusterArbitration, verdict: dec.Verdict,
                           nodes: list) -> None:
        """The only up-to-date node is lost while its peer is not rebuilt: no
        grant (``V_UNAVAILABLE``). Recorded once per episode and logged as an
        error; the stale survivor fences itself at its hold deadline."""
        stale = next((n.get_id() for n in nodes if n.get_id() != verdict.loser), "")
        if (rec.data_unavailable or {}).get("node") == stale:
            return
        now = self.clock()

        def put(fresh):
            fresh.data_unavailable = {"node": stale, "since": now, "reason": verdict.reason}
        self.db.atomic_update(rec, put)
        logger.error("Cluster %s: %s", rec.cluster_id, verdict.reason)

    # ---- host fencing (kubernetes-fencing contract, section 3.2) -------------

    def progress_host_fence(self, rec: ClusterArbitration, nodes: list) -> None:
        """Drive every host fence one step: arm it on an eligible verdict,
        revoke and re-assert on the winner, mark it releasable once the storage
        no longer needs it, and release it once the operator asked.

        Called only for clusters with ``two_node_host_fencing``. Node RPCs run
        on the work pool; this method only reads and writes the record."""
        now = self.clock()
        by_id = {n.get_id(): n for n in nodes}
        fences = {k: dict(v) for k, v in (rec.host_fences or {}).items()}
        last = rec.verdicts[-1] if rec.verdicts else {}
        loser, winner = last.get("loser", ""), last.get("winner", "")
        in_force = (rec.state == ARB_DEGRADED and bool(last.get("host_fence"))
                    and bool(loser) and bool(winner))
        changed = False

        if in_force and loser in by_id:
            entry = fences.get(loser)
            # Only once the winner itself has lost the peer: a precautionary
            # grant (the peer unreachable from the hub only, its JM still
            # healthy on the winner) never fences a host whose VMs still run.
            lost = (self.signals.get(winner) or ev.NodeSignals()).reports_peer_unhealthy()
            if lost and (entry is None or entry.get("on") != winner
                         or (entry.get("state") == HF_UNAVAILABLE and by_id[loser].client_host_nqn)):
                fences[loser] = self._arm_host_fence(rec, by_id[loser], winner, entry, now)
                changed = True

        for nid, entry in list(fences.items()):
            state = entry.get("state")
            needed = in_force and nid == loser and entry.get("on") == winner
            if state == HF_UNAVAILABLE and not needed:
                del fences[nid]
                changed = True
            elif state == HF_REVOKED and not needed and self._host_back(nid):
                entry.update(state=HF_RELEASABLE, releasable_at=now)
                if nid in by_id:
                    self.notify(rec.cluster_id, by_id[nid], "TWO_NODE_HOST_FENCE_RELEASABLE",
                                "Host fence of %s may be released (storage healed)" % nid)
                changed = True
            elif state in (HF_RELEASABLE, HF_RESTORING) and needed:
                # Lost again before the release ran: the fence stays.
                entry.update(state=HF_REVOKED, release_requested=False)
                changed = True
        if changed:
            rec = self._put_host_fences(rec, fences)
            fences = {k: dict(v) for k, v in (rec.host_fences or {}).items()}

        for nid, entry in fences.items():
            winner_node = by_id.get(entry.get("on", ""))
            if winner_node is None:
                continue
            state = entry.get("state")
            retry_ok = now - self.host_fence_tried_at.get(nid, 0) >= 1000
            reassert = now - int(entry.get("reasserted_at") or 0) >= \
                int(constants.TWO_NODE_HOST_FENCE_REASSERT_S) * 1000
            if retry_ok and (state == HF_REVOKING or (state == HF_REVOKED and reassert)):
                if self._work(("hostfence", nid), {"host_nqn": entry["host_nqn"], "node": by_id.get(nid)},
                              self.host_fence_revoke, self.db, winner_node, entry["host_nqn"]):
                    self.host_fence_tried_at[nid] = now
            elif retry_ok and state == HF_RESTORING:
                if self._work(("hostrelease", nid), {"host_nqn": entry["host_nqn"],
                                                     "node": by_id.get(nid)},
                              self.host_fence_release, self.db, winner_node,
                              dict(entry.get("subsystems") or {}), entry["host_nqn"]):
                    self.host_fence_tried_at[nid] = now
        self._drain_work(rec, nodes)

    def _arm_host_fence(self, rec: ClusterArbitration, loser: StorageNode, winner: str,
                        old: dict | None, now: int) -> dict:
        host_nqn = (loser.client_host_nqn or "").strip()
        if not host_nqn:
            if not old or old.get("state") != HF_UNAVAILABLE:
                self.notify(rec.cluster_id, loser, "TWO_NODE_HOST_FENCE_UNAVAILABLE",
                            "No client host NQN known for %s: its host is not fenced and "
                            "Kubernetes is not asked to remediate it" % loser.get_id())
            return {"state": HF_UNAVAILABLE, "on": winner, "host_nqn": "", "epoch": rec.epoch,
                    "since": now, "error": "no client_host_nqn reported for the node"}
        # A fence already in place on the same winner keeps what it changed,
        # so its release still undoes every pass.
        same = (old or {}).get("on") == winner
        logger.warning("Host fence of %s (%s) on %s at epoch %s", loser.get_id(), host_nqn,
                       winner, rec.epoch)
        return {"state": HF_REVOKING, "on": winner, "host_nqn": host_nqn, "epoch": rec.epoch,
                "since": now, "revoked_at": 0, "reasserted_at": 0, "release_requested": False,
                "subsystems": dict((old or {}).get("subsystems") or {}) if same else {},
                "error": ""}

    def _host_back(self, nid: str) -> bool:
        """The fenced node serves again: reachable, not lost, and no LVS fenced
        or holding (it went through the heal or the restart flow)."""
        if not self.reachable.get(nid, False) or self.spdk_lost.get(nid):
            return False
        sig = self.signals.get(nid)
        if sig is None or not sig.lvs_state:
            return False
        return not any(s in (LVS_FENCED, LVS_HOLDING) for s in sig.lvs_state.values())

    def _put_host_fences(self, rec: ClusterArbitration, fences: dict) -> ClusterArbitration:
        def put(fresh):
            for nid, entry in fences.items():
                # A release the API requested meanwhile is kept.
                cur = (fresh.host_fences or {}).get(nid) or {}
                if cur.get("release_requested") and entry.get("state") == HF_RELEASABLE:
                    entry.update(release_requested=True, state=HF_RESTORING)
            fresh.host_fences = fences
        return self.db.atomic_update(rec, put)

    def _apply_host_fence(self, rec: ClusterArbitration, kind: str, nid: str, ctx: dict,
                          result: tuple) -> None:
        """Bookkeeping of a revocation or release (decision thread)."""
        status, value = result
        now = self.clock()
        fresh = self.db.get_cluster_arbitration(rec.cluster_id)
        entry = dict((fresh.host_fences or {}).get(nid) or {})
        if not entry or entry.get("host_nqn") != ctx.get("host_nqn"):
            return                                  # superseded meanwhile
        release = kind == "hostrelease"
        if status == "err":
            logger.warning("Host fence %s of %s failed: %s",
                           "release" if release else "revocation", nid, value)
            entry["error"] = str(value)[:512]
            self._put_one_host_fence(fresh, nid, entry)
            return
        if release:
            if entry.get("state") != HF_RESTORING:
                return

            def drop(f):
                f.host_fences = {k: v for k, v in (f.host_fences or {}).items() if k != nid}
                f.record_verdict({"epoch": f.epoch, "at": now, "kind": "host_fence_released",
                                  "winner": entry.get("on", ""), "loser": nid,
                                  "reason": "the operator released the host fence"},
                                 constants.TWO_NODE_VERDICT_HISTORY)
            self.db.atomic_update(fresh, drop)
            logger.warning("Host fence of %s released on %s", nid, entry.get("on", ""))
            if ctx.get("node") is not None:
                self.notify(rec.cluster_id, ctx["node"], "TWO_NODE_HOST_FENCE_RELEASED",
                            "Host fence of %s released on %s" % (nid, entry.get("on", "")))
            return
        if entry.get("state") not in (HF_REVOKING, HF_REVOKED):
            return
        subs = dict(entry.get("subsystems") or {})
        subs.update(value or {})
        first = entry.get("state") == HF_REVOKING
        entry.update(state=HF_REVOKED, subsystems=subs, reasserted_at=now, error="")
        if first:
            entry["revoked_at"] = now
        self._put_one_host_fence(fresh, nid, entry)
        if first and ctx.get("node") is not None:
            self.notify(rec.cluster_id, ctx["node"], "TWO_NODE_HOST_FENCED",
                        "Host %s of node %s revoked on %s (%d subsystems); Kubernetes may "
                        "remediate the node" % (entry.get("host_nqn"), nid, entry.get("on"),
                                                len(subs)))

    def _put_one_host_fence(self, rec: ClusterArbitration, nid: str, entry: dict) -> None:
        def put(fresh):
            fences = dict(fresh.host_fences or {})
            if nid not in fences:
                return False
            fences[nid] = entry
            fresh.host_fences = fences
        self.db.atomic_update(rec, put)

    # ---- the loop -----------------------------------------------------------

    def arbitrated_clusters(self) -> list:
        """The clusters with ``two_node_arbitration`` on.

        With none, the cluster list is read again only after
        ``TWO_NODE_IDLE_RESCAN_S`` instead of on every renewal and tick, so a
        deployment without two-node clusters is left almost untouched. Once one
        appears, every call reads the list again (the fast loops)."""
        now = self.clock()
        if now < self._idle_until_ms:
            return []
        # Mode pacemaker (kubernetes-fencing contract, 4.2): the nodes follow
        # Pacemaker's fence; the arbiter does not act on the cluster at all.
        clusters = [c for c in self.db.get_clusters()
                    if getattr(c, "two_node_arbitration", False) is True and witness_authority(c)]
        self._idle_until_ms = 0 if clusters else now + int(constants.TWO_NODE_IDLE_RESCAN_S * 1000)
        return clusters

    def renew_all(self) -> None:
        """Lease loop body: every ``TWO_NODE_LEASE_RENEW_MS``."""
        for cluster in self.arbitrated_clusters():
            nodes = self.eligible(cluster)
            if not nodes:
                continue
            rec = self.record(cluster.get_id(), nodes)
            for node in nodes:
                if self.capable.get(node.get_id()) is False:
                    continue
                # One renewal per node in flight, on the RPC pool: a frozen
                # node's 1 s timeout never delays its peer's renewal (c3, 02a).
                # The capability probe runs there too, for the same reason.
                key = ("renew", node.get_id())
                fut = self.inflight.get(key)
                if fut is not None and not fut.done():
                    continue
                peer = nodes[1] if node is nodes[0] else nodes[0]
                self.inflight[key] = self.rpc_pool.submit(self._renew_safely, rec, node, peer)

    def _renew_safely(self, rec: ClusterArbitration, node: StorageNode, peer: StorageNode) -> None:
        try:
            if self.probe(node):
                self.renew(rec, node, peer)
        except Exception:                           # noqa: BLE001 - next round renews
            logger.exception("Lease renewal of %s failed", node.get_id())

    def tick(self) -> None:
        for cluster in self.arbitrated_clusters():
            nodes = self.eligible(cluster)
            if nodes:
                try:
                    self.tick_cluster(cluster.get_id(), nodes, cluster)
                except Exception:                   # noqa: BLE001 - next tick retries
                    logger.exception("Arbitration tick failed for cluster %s", cluster.get_id())

    def tick_cluster(self, cluster_id: str, nodes: list, cluster=None) -> dec.Verdict | None:
        rec = self.record(cluster_id, nodes)
        self.consume_events(cluster_id)
        # The per-node view is reconciled from jc_ha_status before anything
        # decides on it (bookkeeping requirement, campaign c1).
        self.reconcile_due(nodes)
        # jc_set_dual_node on the work pool (campaign c7, F16).
        self.configure_due(rec, nodes)

        if cluster_id not in self.resumed:
            self.resumed.add(cluster_id)
            self.resume(rec, nodes)
            rec = self.db.get_cluster_arbitration(cluster_id)

        rec = self.track_planned_loss(rec, nodes)

        if rec.override:
            return self.apply_override(rec, nodes)

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
            # A new fence: leadership restoration starts over after its heal.
            self.lead_pending.pop(cluster_id, None)
            rec = self.commit(rec, verdict, snap)
            logger.warning("Two-node verdict %s epoch %s for cluster %s: %s",
                           verdict.kind, rec.epoch, cluster_id, verdict.reason)
            if verdict.kind == dec.V_DEGRADED and verdict.loser:
                # Campaign c3, F10: from now on the winner writes alone, so the
                # loser lacks those writes whichever way it comes back (unfence,
                # self-fence re-admit, restart). Marked here, queued once it is
                # back (progress_rebuild).
                loser = next((n for n in nodes if n.get_id() == verdict.loser), None)
                if loser is not None:
                    self.mark_rebuild(rec, loser, "lost while %s ran alone (epoch %s)"
                                      % (verdict.winner, rec.epoch))
            if not self.send(rec, verdict, nodes):
                self.undelivered[cluster_id] = verdict
        elif verdict.kind == dec.V_UNAVAILABLE:
            self.report_unavailable(rec, verdict, nodes)
            if verdict.new_state != rec.state:
                self.commit(rec, verdict, snap)
        elif verdict.kind == dec.V_HEAL:
            rec = self.commit(rec, verdict, snap)
            self.readmitted[cluster_id] = set()
            if rec.data_unavailable:
                self.db.atomic_update(rec, lambda fresh: setattr(fresh, "data_unavailable", {}))
        elif verdict.kind == dec.V_DONE:
            self._finish_healing(rec)
        elif verdict.new_state != rec.state:
            self.commit(rec, verdict, snap)

        rec = self.db.get_cluster_arbitration(cluster_id)
        if rec.state == ARB_HEALING:
            self.heal(rec, nodes)
        self.readmit_self_fenced(rec, nodes)
        self.restore_pending_leadership(self.db.get_cluster_arbitration(cluster_id), nodes)
        self.progress_rebuild(self.db.get_cluster_arbitration(cluster_id), nodes)
        self.recover_partition(rec, nodes)
        self.persist_leases(rec)
        self.maybe_reconcile_leadership(cluster_id, nodes)
        if host_fencing_on(cluster):
            self.progress_host_fence(self.db.get_cluster_arbitration(cluster_id), nodes)
        return verdict

    def maybe_reconcile_leadership(self, cluster_id: str, nodes: list) -> None:
        """B6: restore a lost LVS leadership once the pair is steady again.

        After fence -> resync -> unfence an LVS can be left without a leader on
        either node; the restart/creation/activation flows are the only other
        places that restore it. Every ``TWO_NODE_LEADERSHIP_RECONCILE_S``,
        while the arbitration state is steady (no verdict in force), or healing
        and only waiting for the data rebuild (campaign c3, F10: the rebuild
        runners defer on "not the leader", so the rebuild could never finish
        behind a leaderless LVS), each node's primary LVS is checked; one found
        leaderless on two consecutive passes is handed to
        ``reconcile_leadership`` (the guarded leaderless-recovery path in
        storage_node_ops, which also requires every member ONLINE), never to an
        ad-hoc leadership RPC. A primary whose own HA state for the LVS is
        fenced or holding, or whose peer leads it solo, is left alone."""
        now = self.clock()
        rec = self.db.get_cluster_arbitration(cluster_id)
        if rec is not None:
            self._drain_work(rec, nodes)            # apply a finished pass first
        last = self.leadership_checked_at.get(cluster_id, 0)
        if now - last < int(constants.TWO_NODE_LEADERSHIP_RECONCILE_S) * 1000:
            return
        self.leadership_checked_at[cluster_id] = now
        rebuild_only = rec is not None and rec.state == ARB_HEALING and bool(rec.rebuild)
        if rec is None or (rec.state != ARB_STEADY and not rebuild_only):
            self.leaderless_seen.pop(cluster_id, None)
            return
        # Campaign c7 (F16): never probe a node that is unreachable or lost,
        # stopped by the control plane, or while one holds (its peer is gone);
        # fenced LVS are only skipped below. And never on the
        # decision thread (the probe of a frozen node blocked it ~5 s, the
        # grant to its peer never came, 02b: EIO).
        for node in nodes:
            nid = node.get_id()
            sig = self.signals.get(nid) or ev.NodeSignals()
            if not self.reachable.get(nid, True) or self.spdk_lost.get(nid) \
                    or nid in (rec.planned_loss or {}) \
                    or any(st == LVS_HOLDING for st in sig.lvs_state.values()):
                return
        blocked = set()
        for node in nodes:
            sig = self.signals.get(node.get_id()) or ev.NodeSignals()
            for vuid, state in sig.lvs_state.items():
                if state != LVS_NORMAL:
                    blocked.add(int(vuid))
        confirmed = set(self.leaderless_seen.get(cluster_id, set()))
        self._work(("leadership", cluster_id), {"rec": rec, "at": now},
                   lambda: self.reconcile_leadership(cluster_id, nodes, skip_jm_vuids=blocked,
                                                     confirmed=confirmed))
        self._drain_work(rec, nodes)

    def _apply_leadership(self, cluster_id: str, ctx: dict, result: tuple) -> None:
        kind, value = result
        if kind == "err":
            logger.warning("Leadership reconciliation for %s failed: %s", cluster_id, value)
            return
        restored, leaderless = value
        self.leaderless_seen[cluster_id] = set(leaderless) - set(restored)
        if restored:
            at = ctx.get("at", self.clock())

            def put(fresh):
                fresh.leadership_restored_at = at
            rec = self.db.get_cluster_arbitration(cluster_id) or ctx.get("rec")
            self.db.atomic_update(rec, put)

    def apply_override(self, rec: ClusterArbitration, nodes: list) -> dec.Verdict:
        """The operator decided (section 7.4): the named node leads every LVS.

        The operator's decision counts as positive fencing of the other node
        (invariant 3), so the grant is sent even if the fence cannot reach it.
        """
        winner = rec.override.get("winner", "")
        ids = [n.get_id() for n in nodes]
        if winner not in ids:
            logger.error("Override names %s, not a member of %s; dropped", winner, rec.cluster_id)
            self.db.atomic_update(rec, lambda f: setattr(f, "override", {}))
            return dec.Verdict(dec.V_NONE, rec.state, reason="invalid override")
        loser = ids[1] if winner == ids[0] else ids[0]
        snap = self.snapshot(rec, nodes)
        vuids = sorted(set(self.leaders(rec, nodes)))
        verdict = dec.Verdict(dec.V_DEGRADED, "degraded", raise_epoch=True, winner=winner,
                              loser=loser, grant={v: winner for v in vuids},
                              fence={v: loser for v in vuids}, host_fence=True,
                              reason="operator override: " + rec.override.get("reason", ""))

        def clear(fresh):
            fresh.override = {}
        rec = self.commit(rec, verdict, snap)
        rec = self.db.atomic_update(rec, clear)
        by_id = {n.get_id(): n for n in nodes}
        try:
            # Operator-asserted, so best effort (campaign c7, F16: short).
            by_id[loser].rpc_client(timeout=int(constants.TWO_NODE_BEST_EFFORT_FENCE_TIMEOUT_S),
                                    retry=0).jc_fence(rec.epoch, vuids)
        except Exception as e:                      # noqa: BLE001 - operator-asserted
            logger.warning("Override: fence of %s failed (%s); granting anyway", loser, e)
        grant_only = dec.Verdict(verdict.kind, verdict.new_state, grant=verdict.grant)
        if not self.send(rec, grant_only, nodes):
            self.undelivered[rec.cluster_id] = grant_only
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
            fence_all={int(k): list(v) for k, v in (last.get("fence_all") or {}).items()},
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


def witness_authority(cluster) -> bool:
    """The arbiter decides for this cluster (mode ``witness``, the default).
    Mode ``pacemaker``: the nodes follow Pacemaker's fence and the arbiter
    stays out (docs/design/two-node-kubernetes-fencing.md, section 4.2)."""
    authority = getattr(cluster, "two_node_fencing_authority", "") or constants.TWO_NODE_AUTHORITY_WITNESS
    return authority != constants.TWO_NODE_AUTHORITY_PACEMAKER


def host_fencing_on(cluster) -> bool:
    """Host fencing and the Kubernetes integration (contract section 3)."""
    return (cluster is not None and getattr(cluster, "two_node_host_fencing", False) is True
            and witness_authority(cluster))


def _volume_nqns(db, cluster_id: str) -> set:
    return {lv.nqn for lv in db.get_lvols(cluster_id) if getattr(lv, "nqn", "")}


def default_host_fence_revoke(db, winner: StorageNode, host_nqn: str) -> dict:
    """Revoke ``host_nqn`` on every volume subsystem of ``winner``."""
    from simplyblock_core.arbitration import host_fence
    keep = {winner.client_host_nqn} if winner.client_host_nqn else set()
    rpc = winner.rpc_client(timeout=int(constants.TWO_NODE_HOST_FENCE_RPC_TIMEOUT_S), retry=0)
    return host_fence.revoke(rpc, _volume_nqns(db, winner.cluster_id), host_nqn, keep)


def default_host_fence_release(db, winner: StorageNode, subsystems: dict, host_nqn: str) -> None:
    """Undo a revocation on ``winner``. An allow-list subsystem gets the host
    back with the keys its volume gives it (``host_auth.add_allowed_host``)."""
    from simplyblock_core.arbitration import host_fence
    from simplyblock_core.controllers import host_auth
    rpc = winner.rpc_client(timeout=int(constants.TWO_NODE_HOST_FENCE_RPC_TIMEOUT_S), retry=0)
    by_nqn: dict = {}
    for lv in db.get_lvols(winner.cluster_id):
        if lv.nqn in subsystems and any(h.get("nqn") == host_nqn for h in lv.allowed_hosts):
            by_nqn.setdefault(lv.nqn, lv)

    def readd(nqn):
        lv = by_nqn.get(nqn)
        if lv is not None:
            host_auth.add_allowed_host(lv, winner, rpc, host_nqn)
        else:
            rpc.subsystem_add_host(nqn, host_nqn)
    host_fence.release(rpc, subsystems, host_nqn, readd)


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


def default_schedule_rebuild(node: StorageNode) -> bool:
    """Queue the data rebuild a node restart queues (campaign c3, F10)."""
    from simplyblock_core.controllers import tasks_controller
    return bool(tasks_controller.add_device_mig_task_for_node(node.get_id()))


def default_open_rebuild_tasks(cluster_id: str) -> int:
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.job_schedule import JobSchedule
    kinds = {JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG,
             JobSchedule.FN_BALANCING_AFTER_NODE_RESTART}
    return sum(1 for t in DBController().get_job_tasks(cluster_id)
               if t.function_name in kinds and t.status != JobSchedule.STATUS_DONE
               and not t.canceled)


def default_devices_ok(db, node: StorageNode):
    """Campaign c4, F12: every data device the control plane counts as
    serving is ONLINE and its alceml bdev exists on the node.

    False: a device is not ONLINE (unavailable, read-only, cannot allocate),
    or its alceml bdev is gone (the node's stuck-IO watchdog shut it down).
    None: the node could not be asked. Removed, failed, migrated, new and JM
    devices are not part of the data path and are skipped."""
    from simplyblock_core.models.nvme_device import NVMeDevice

    skip = {NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED,
            NVMeDevice.STATUS_FAILED_AND_MIGRATED, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_JM}
    fresh = db.get_storage_node_by_id(node.get_id())
    devices = [d for d in (getattr(fresh, "nvme_devices", None) or []) if d.status not in skip]
    if not devices:
        return True
    if any(d.status != NVMeDevice.STATUS_ONLINE for d in devices):
        return False
    rpc = fresh.rpc_client(timeout=3, retry=0)
    for dev in devices:
        name = dev.alceml_bdev or dev.alceml_name
        if not name:
            continue
        try:
            if rpc.bdev_get(name) is None:
                logger.error("Node %s: alceml bdev %s of device %s is gone",
                             fresh.get_id(), name, dev.get_id())
                return False
        except Exception as e:                      # noqa: BLE001 - unknown, not "gone"
            logger.info("Node %s: cannot look up %s: %s", fresh.get_id(), name, e)
            return None
    return True


def default_rebuild_task_errors(db, cluster_id: str) -> int:
    """OR of the ``mig error`` bit fields of the open data-rebuild tasks of
    ``cluster_id`` that last failed with one (``function_result`` "mig error:
    N, retrying", migration_task_common)."""
    from simplyblock_core.models.job_schedule import JobSchedule
    kinds = {JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG}
    bits = 0
    for task in db.get_job_tasks(cluster_id):
        if task.function_name not in kinds or task.status == JobSchedule.STATUS_DONE \
                or task.canceled:
            continue
        result = str(task.function_result or "")
        if not result.startswith("mig error:"):
            continue
        try:
            bits |= int(result.split(":", 1)[1].split(",", 1)[0].strip())
        except ValueError:
            continue
    return bits


def _leadership_task_blocks(db, cluster_id: str, node_ids: list, lvs_name: str) -> bool:
    """A restart or port-allow task on either node owns leadership movement
    (``storage_node_ops._leadership_moving_tasks_active``), so the restoration
    waits for it. A restart always blocks. A port-allow task older than
    ``TWO_NODE_LEADERSHIP_TASK_STALE_S`` does not: in campaign c5 every
    port-allow task of the 2-node bed deferred forever on "unable to connect
    to remote JMs" (a two-node cluster has one remote JM), and the LVS stayed
    leaderless behind it. Both target the LVS's primary, so they do not fight.
    True (wait) when the task state cannot be read."""
    from simplyblock_core.models.job_schedule import JobSchedule
    now = int(time.time())
    try:
        tasks = db.get_job_tasks(cluster_id)
    except Exception as e:                          # noqa: BLE001 - wait, as the shared check
        logger.warning("Cannot read the task state for %s (%s); not restoring leadership",
                       lvs_name, e)
        return True
    for task in tasks:
        if task.function_name not in (JobSchedule.FN_PORT_ALLOW, JobSchedule.FN_NODE_RESTART):
            continue
        if task.node_id not in node_ids or task.status == JobSchedule.STATUS_DONE or task.canceled:
            continue
        age = now - int(task.date or now)
        if task.function_name == JobSchedule.FN_PORT_ALLOW and                 age > int(constants.TWO_NODE_LEADERSHIP_TASK_STALE_S):
            logger.warning("LVS %s: port-allow task %s on %s open for %ss; restoring "
                           "leadership anyway", lvs_name, task.uuid, task.node_id, age)
            continue
        logger.info("LVS %s: %s task %s on %s moves leadership; later",
                    lvs_name, task.function_name, task.uuid, task.node_id)
        return True
    return False


def default_restore_leadership(cluster_id: str, primary: StorageNode, peer: StorageNode,
                               lvs_name: str) -> bool:
    """Campaign c5, 07/08: put the LVS's leadership back on its primary after
    an unfence, and the primary's volumes' ANA with it.

    Same reload-then-grant order as the leaderless recovery:
    ``bdev_lvol_update_lvstore`` before ``bdev_lvol_set_leader``, the grant
    verified. True when the LVS is led and
    ANA is set: optimized on the primary, non_optimized on its secondary
    (``_failback_primary_ana``). Left alone (True) when the peer already leads
    it: the survivor took it over after its peer was lost, and the failback
    belongs to the restart flow. False (retried) while a restart or port-allow task moves
    leadership on either node, or when the grant does not take."""
    from simplyblock_core import storage_node_ops as sno
    from simplyblock_core.controllers.lvol_controller import is_node_leader
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.lvol_model import LVol

    db = DBController()
    owner = db.get_storage_node_by_id(primary.get_id())
    other = db.get_storage_node_by_id(peer.get_id())
    if _leadership_task_blocks(db, cluster_id, [owner.get_id(), other.get_id()], lvs_name):
        return False
    owner_leads = is_node_leader(owner, lvs_name)
    if is_node_leader(other, lvs_name):
        if owner_leads:
            logger.error("LVS %s reports a leader on both %s and %s; not touching it",
                         lvs_name, owner.get_id(), other.get_id())
            return False
        logger.info("LVS %s is led by %s (survivor); failback is the restart flow's",
                    lvs_name, other.get_id())
        return True
    if not owner_leads:
        rpc = owner.rpc_client(timeout=10, retry=0)
        if not rpc.bdev_lvol_update_lvstore(lvs_name):
            logger.error("bdev_lvol_update_lvstore %s on %s returned False; not granting "
                         "leadership on top of un-reloaded metadata", lvs_name, owner.get_id())
            return False
        rpc.bdev_lvol_set_leader(lvs_name, leader=True)
        deadline = time.monotonic() + float(constants.TWO_NODE_PROMOTE_VERIFY_S)
        while not is_node_leader(owner, lvs_name):
            if time.monotonic() >= deadline:
                logger.error("bdev_lvol_set_leader %s on %s did not take effect",
                             lvs_name, owner.get_id())
                return False
            time.sleep(0.1)
    seen = set()
    for lvol in db.get_lvols_by_node_id(owner.get_id()):
        if lvol.status not in (LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE) or lvol.lvs_name != lvs_name:
            continue
        if (lvol.nqn, lvol.ns_id) in seen:
            continue
        seen.add((lvol.nqn, lvol.ns_id))
        sno._set_lvol_ana_on_node(lvol, owner, "optimized")
    sno._failback_primary_ana(owner)
    return True


def default_notify(cluster_id: str, node: StorageNode, code: str, message: str) -> None:
    """Cluster event log entry for an arbiter action. Best effort."""
    from simplyblock_core.controllers import events_controller as ec
    from simplyblock_core.models.events import EventObj
    try:
        ec.log_event_cluster(cluster_id, ec.DOMAIN_JM, code, node, ec.CAUSED_BY_MONITOR,
                             message, node_id=node.get_id(), event_level=EventObj.LEVEL_WARN)
    except Exception as e:                          # noqa: BLE001 - logging only
        logger.warning("Could not log %s for %s: %s", code, node.get_id(), e)


def _run_in_thread(fn) -> None:
    threading.Thread(target=fn, daemon=True, name="arbiter-recovery").start()


def default_link_probe(node: StorageNode, peer: StorageNode):
    """Ping the peer's data IP from the node (its own data NIC). Tri-state:
    None when the probe itself could not be made (inconclusive)."""
    if not node.data_nics or not peer.data_nics:
        return None
    try:
        ret, _ = node.client(timeout=8, retry=1, connect_retry=0).ping_ip(
            peer.data_nics[0].ip4_address, node.data_nics[0].if_name)
        return bool(ret)
    except Exception as e:                          # noqa: BLE001 - inconclusive
        logger.debug("Link probe %s -> %s failed: %s", node.get_id(), peer.get_id(), e)
        return None


def default_reconnect_jms(node: StorageNode, peer: StorageNode) -> None:
    """Reconnect ``node``'s remote JM devices owned by ``peer`` (JM mesh repair)."""
    from simplyblock_core import storage_node_ops
    from simplyblock_core.db_controller import DBController
    db = DBController()
    fresh = db.get_storage_node_by_id(node.get_id())
    remote = storage_node_ops._connect_to_remote_jm_devs(fresh, only_node_id=peer.get_id())
    db.atomic_update(db.get_storage_node_by_id(node.get_id()),
                     lambda n: setattr(n, "remote_jm_devices", remote))


def default_reconcile_leadership(cluster_id, nodes, skip_jm_vuids=(), confirmed=()):
    """See ``storage_node_ops.reconcile_lvs_leadership``."""
    from simplyblock_core import storage_node_ops
    return storage_node_ops.reconcile_lvs_leadership(
        cluster_id, nodes, skip_jm_vuids=skip_jm_vuids, confirmed=confirmed,
        probe_timeout=int(constants.TWO_NODE_LEADER_PROBE_TIMEOUT_S), probe_retry=0)


#: ``inflight`` kinds that run on the work pool (campaign c7, F16).
_WORK_KINDS = ("configure", "unfence", "leadership", "hostfence", "hostrelease")

#: Events that mean the node lost its peer or stopped serving; logged as warnings.
_WARN_STATUSES = {ev.ST_UNHEALTHY, "ha_hold_started", "ha_self_fenced", "ha_lease_expired", "ha_fenced"}


def default_log_event(item: ArbitrationEvent, event: dict) -> None:
    """Contract section 4: HA events also go to the cluster event log, so they
    appear in ``sbctl cluster get-logs``. Best effort: the log never blocks a decision."""
    from simplyblock_core.controllers import events_controller as ec
    from simplyblock_core.models.events import EventObj

    status = str(event.get("status", ""))
    parts = [status]
    for field in ("jm_vuid", "reason", "ha_state", "epoch"):
        if event.get(field) not in (None, ""):
            parts.append("%s=%s" % (field, event[field]))
    level = EventObj.LEVEL_WARN if status in _WARN_STATUSES else EventObj.LEVEL_INFO
    try:
        ec.log_event_cluster(item.cluster_id, ec.DOMAIN_JM, "TWO_NODE_" + status.upper(), item,
                             ec.CAUSED_BY_MONITOR, " ".join(parts), node_id=item.node_id,
                             event_level=level)
    except Exception as e:                          # noqa: BLE001 - logging only
        logger.warning("Could not log HA event %s of %s: %s", status, item.node_id, e)
