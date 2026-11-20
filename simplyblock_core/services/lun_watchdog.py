"""Multi-attach clusters: fast loss detection of the nodes serving LUNs.

With 1+0 erasure coding every volume has data on every device, so all of
them wait on a lost node's LUN until a standby has taken it over
(controllers/lun_takeover). The regular monitor declares a node lost only
after its ping/API checks have failed: 1 s for a killed SPDK, 10-20 s for a
host that is gone, and the OFFLINE verdict takes far longer. That is the
volumes' I/O stall.

This watchdog probes every node that serves a LUN every
``MULTI_ATTACH_PROBE_INTERVAL_SEC``: a ``spdk_get_version`` RPC with
``MULTI_ATTACH_PROBE_TIMEOUT_SEC``. While that fails it also asks the node
agent whether SPDK runs. The node is lost

* at once when the agent reports SPDK not running (killed or crashed);
* when neither SPDK nor the agent has answered for
  ``MULTI_ATTACH_LOST_AFTER_SEC`` (host down or cut off);

and never while the agent reports SPDK running: a slow RPC is the regular
monitor's case. A node that answers again within the window is not taken
over at all.

The takeover of a lost node's LUNs starts straight away, each LUN in its own
thread (the monitor's ``_start_lun_takeover``, idempotent). It fences the
node at the array before anything else, so a node that is alive behind a
management network fault loses nothing; its LUNs pause for the takeover.

Not when this process is the one cut off: no takeover while more nodes of a
cluster fail their probes than it tolerates (``max_fault_tolerance``), or
while no other node of it has answered recently.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from simplyblock_core import array, constants, multi_attach, utils
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

#: Node states the probe watches. A node in a restart or shutdown is owned
#: by that flow; an OFFLINE one was handled when it went offline.
WATCH_STATUSES = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                  StorageNode.STATUS_DOWN, StorageNode.STATUS_SCHEDULABLE)

#: Device states whose LUN a lost node takes down with it.
SERVING_STATES = (NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                  NVMeDevice.STATUS_CANNOT_ALLOCATE)

#: Probe answers.
OK = "ok"                  # SPDK answered
SPDK_DOWN = "spdk_down"    # SPDK did not; the agent says it is not running
SPDK_UP = "spdk_up"        # SPDK did not; the agent says it runs (slow RPC)
NO_ANSWER = "no_answer"    # neither answered

#: Re-trigger of a node still lost (its takeover threads are idempotent).
REFIRE_SEC = 5.0
#: How often the set of watched nodes is re-read.
REFRESH_SEC = 1.0


def _rpc_answers(node, timeout) -> bool:
    try:
        node.rpc_client(timeout=timeout, retry=0).get_version()
        return True
    except Exception:                                       # noqa: BLE001
        return False


def _agent_says(node, timeout) -> str:
    try:
        is_up, _ = node.client(timeout=timeout, retry=0, connect_retry=0).spdk_process_is_up(
            node.rpc_port, node.cluster_id)
    except Exception:                                       # noqa: BLE001
        return NO_ANSWER
    if is_up is True:
        return SPDK_UP
    if is_up is False:
        return SPDK_DOWN
    return NO_ANSWER


def probe(node, failing=False, timeout=None) -> str:
    """One probe of ``node``. While it is already ``failing`` the RPC and the
    agent are asked at the same time, else the agent only after the RPC
    failed."""
    timeout = timeout or constants.MULTI_ATTACH_PROBE_TIMEOUT_SEC
    if not failing:
        if _rpc_answers(node, timeout):
            return OK
        return _agent_says(node, timeout)
    with ThreadPoolExecutor(max_workers=2) as pool:
        rpc = pool.submit(_rpc_answers, node, timeout)
        agent = pool.submit(_agent_says, node, timeout)
        if rpc.result():
            return OK
        return agent.result()


class _Track:
    """Probe history of one node."""

    __slots__ = ("answer", "fired_at", "first_fail", "last_ok")

    def __init__(self, now):
        self.first_fail = None
        self.last_ok = now
        self.fired_at = None
        self.answer = OK


class Watchdog:
    """The decisions, apart from the threads that probe (testable)."""

    def __init__(self, start_takeover, db=None):
        self.start_takeover = start_takeover
        self.db = db
        self.lock = threading.Lock()
        self.tracks: dict[str, _Track] = {}
        self.cluster_of: dict[str, str] = {}
        self.warmed: dict[str, float] = {}

    def _db(self):
        if self.db is None:
            self.db = DBController()
        return self.db

    # ----------------------------------------------------------- watched

    def watched(self) -> dict:
        """``{node_id: node}`` of the nodes to probe now: in a multi-attach
        cluster that is not in activation, in a WATCH_STATUSES state, serving
        at least one LUN in a SERVING_STATES state."""
        out = {}
        db = self._db()
        for cluster in db.get_clusters():
            if not multi_attach.is_multi_attach(cluster):
                continue
            if cluster.status == Cluster.STATUS_IN_ACTIVATION:
                continue
            self._warm(cluster)
            nodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())
            for node in nodes:
                if node.status not in WATCH_STATUSES:
                    continue
                if any(dev.status in SERVING_STATES
                       for _o, dev in multi_attach.devices_served_by(nodes, node.get_id())):
                    out[node.get_id()] = node
        return out

    def _warm(self, cluster):
        """Keep the cluster's array connection ready for a fence (connecting
        costs about a second), refreshed at half its cache time."""
        cid = cluster.get_id()
        now = time.monotonic()
        half = constants.ARRAY_PLUGIN_CACHE_SEC / 2
        if not getattr(cluster, "array_plugin", "") or now - self.warmed.get(cid, -1e9) < half:
            return
        self.warmed[cid] = now

        def _connect():
            try:
                array.cached_plugin_for_cluster(cluster, max_age=half)
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN watchdog: array connection of %s not made: %s", cid, e)
                self.warmed[cid] = time.monotonic() - half + 30  # again in 30 s
        threading.Thread(target=_connect, name=f"array-warm-{cid[:8]}", daemon=True).start()

    # ---------------------------------------------------------- decisions

    def record(self, node, answer, now) -> str | None:
        """Note a probe answer of ``node`` at ``now``; the reason the node is
        lost, or None."""
        nid = node.get_id()
        with self.lock:
            self.cluster_of[nid] = node.cluster_id
            t = self.tracks.setdefault(nid, _Track(now))
            t.answer = answer
            if answer == OK:
                if t.first_fail is not None:
                    logger.info("LUN watchdog: %s answers again after %.1fs",
                                nid, now - t.first_fail)
                t.first_fail, t.fired_at, t.last_ok = None, None, now
                return None
            if t.first_fail is None:
                t.first_fail = now
            if answer == SPDK_UP:
                return None
            if answer == SPDK_DOWN:
                return "SPDK not running"
            silent = now - t.first_fail
            if silent >= constants.MULTI_ATTACH_LOST_AFTER_SEC:
                return f"no answer for {silent:.1f}s"
            return None

    def _observer_ok(self, cluster, nid, now) -> str:
        """Why the takeover of ``nid`` must wait because this process may be
        the one cut off; empty when it may go ahead."""
        cid = cluster.get_id()
        with self.lock:
            peers = {n: t for n, t in self.tracks.items()
                     if self.cluster_of.get(n) == cid}
        failing = [n for n, t in peers.items() if t.first_fail is not None]
        ftt = max(1, int(getattr(cluster, "max_fault_tolerance", 1) or 1))
        if len(failing) > ftt:
            return (f"{len(failing)} nodes of the cluster fail their probes, more than it "
                    f"tolerates ({ftt}): this monitor may be the one cut off")
        recent = constants.MULTI_ATTACH_LOST_AFTER_SEC + 2 * constants.MULTI_ATTACH_PROBE_TIMEOUT_SEC
        if not any(n != nid and t.first_fail is None and now - t.last_ok <= recent
                   for n, t in peers.items()):
            return "no other node of the cluster answered recently"
        return ""

    def fire(self, node_id, reason, now) -> list:
        """Start the takeover of every LUN ``node_id`` serves; the device ids
        started. At most every REFIRE_SEC per node."""
        with self.lock:
            t = self.tracks.get(node_id)
            if t is None or (t.fired_at is not None and now - t.fired_at < REFIRE_SEC):
                return []
        db = self._db()
        node = db.get_storage_node_by_id(node_id)
        if node.status not in WATCH_STATUSES:
            return []
        cluster = db.get_cluster_by_id(node.cluster_id)
        if not multi_attach.is_multi_attach(cluster):
            return []
        why_not = self._observer_ok(cluster, node_id, now)
        if why_not:
            logger.warning("LUN watchdog: %s is lost (%s) but no takeover: %s",
                           node_id, reason, why_not)
            return []
        devs = [dev for _o, dev in multi_attach.devices_served_by(
                    db.get_storage_nodes_by_cluster_id(node.cluster_id), node_id)
                if dev.status in SERVING_STATES]
        with self.lock:
            t.fired_at = now
        if not devs:
            return []
        logger.warning("LUN watchdog: node %s lost (%s); taking over %d LUN(s): %s",
                       node_id, reason, len(devs), [d.get_id() for d in devs])
        started = []
        for dev in devs:
            try:
                self.start_takeover(dev.get_id(), node_id, f"node lost: {reason}")
                started.append(dev.get_id())
            except Exception as e:                          # noqa: BLE001
                logger.error("LUN watchdog: takeover of %s not started: %s", dev.get_id(), e)
        return started

    def forget(self, node_id):
        with self.lock:
            self.tracks.pop(node_id, None)
            self.cluster_of.pop(node_id, None)

    # ------------------------------------------------------------ threads

    def _watch_node(self, node_id, nodes):
        """Probe one node until it is no longer watched."""
        failing = False
        while True:
            node = nodes.get(node_id)
            if node is None:
                self.forget(node_id)
                return
            started = time.monotonic()
            try:
                answer = probe(node, failing)
                now = time.monotonic()
                failing = answer != OK
                reason = self.record(node, answer, now)
                if reason:
                    self.fire(node_id, reason, now)
            except Exception as e:                          # noqa: BLE001
                logger.error("LUN watchdog: probe of %s failed: %s", node_id, e)
            wait = started + constants.MULTI_ATTACH_PROBE_INTERVAL_SEC - time.monotonic()
            if wait > 0:
                time.sleep(wait)

    def run(self):
        nodes: dict = {}
        threads: dict[str, threading.Thread] = {}
        while True:
            try:
                fresh = self.watched()
            except Exception as e:                          # noqa: BLE001
                logger.error("LUN watchdog: watched nodes not read: %s", e)
                fresh = None
            if fresh is not None:
                for nid in list(nodes):
                    if nid not in fresh:
                        nodes.pop(nid, None)
                nodes.update(fresh)
                for nid in fresh:
                    t = threads.get(nid)
                    if t is None or not t.is_alive():
                        t = threading.Thread(target=self._watch_node, args=(nid, nodes),
                                             name=f"lun-watch-{nid[:8]}", daemon=True)
                        threads[nid] = t
                        t.start()
                for nid in [n for n, t in threads.items() if not t.is_alive()]:
                    threads.pop(nid, None)
            time.sleep(REFRESH_SEC)


_started: Watchdog | None = None


def start(start_takeover) -> Watchdog:
    """Start the watchdog thread of this process (once)."""
    global _started
    if _started is None:
        _started = Watchdog(start_takeover)
        threading.Thread(target=_started.run, name="lun-watchdog", daemon=True).start()
        logger.info("LUN watchdog started (probe %.1fs, lost after %.1fs)",
                    constants.MULTI_ATTACH_PROBE_INTERVAL_SEC,
                    constants.MULTI_ATTACH_LOST_AFTER_SEC)
    return _started
