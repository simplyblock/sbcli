"""
port_release_budget.py — the gate every fence release goes through.

See ``models/fence_release.py`` for why this exists. Call sites:

* ``storage_node_monitor._remediate_stale_port_blocks`` -- asks ``exhausted``
  before lifting a stale fence, ``record_release`` after, ``refuse`` instead
  of lifting once the budget is gone;
* ``health_check_service`` -- does not schedule a port_allow task for a port
  whose budget is gone;
* ``tasks_runner_port_allow`` -- skips such a port when it reopens a node's
  LVS ports and still runs the rest of the recovery;
* ``storage_node_ops`` restart -- ``forget_node`` right before the node is
  put back ONLINE, because a restart reopens the ports by design.

The ledger is one small FDB record per (cluster, lvstore); updates are
compare-and-set through ``DBController.atomic_update`` so the monitor and the
task runner cannot overwrite each other's stamps.

The gate is never applied to the lvstore's preferred path (the primary while
it is online, otherwise the first online follower): that path is reopened
every time, so volumes on the lvstore always keep one route in and the budget
only stops the fence from hopping to the other paths.
"""

import logging
import time

from simplyblock_core import constants
from simplyblock_core.controllers import storage_events
from simplyblock_core.models.fence_release import FenceReleaseLedger
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()


# ---- pure helpers (tested without a DB) ----

def lvstore_of_port(node, port) -> str | None:
    """The lvstore whose client subsystem ``node`` serves on ``port``: its own
    primary lvstore, or one it follows as secondary/tertiary."""
    if node.lvstore and node.get_lvol_subsys_port(node.lvstore) == port:
        return node.lvstore
    for lvs_name, entry in (node.lvstore_ports or {}).items():
        if isinstance(entry, dict) and entry.get("lvol_subsys_port") == port:
            return lvs_name
    return None


def preferred_path(lvstore, nodes):
    """The path of ``lvstore`` that is never left fenced: the primary while
    it is ONLINE, otherwise the first ONLINE follower in chain order
    (secondary, then tertiary). None when the lvstore has no primary among
    ``nodes`` or no path is online."""
    by_id = {n.get_id(): n for n in nodes}
    primary = next((n for n in nodes if n.lvstore == lvstore), None)
    if primary is None:
        return None
    for node_id in (primary.get_id(), primary.secondary_node_id, primary.tertiary_node_id):
        node = by_id.get(node_id)
        if node is not None and node.status == StorageNode.STATUS_ONLINE:
            return node
    return None


# ---- ledger access ----

def _key(cluster_id, lvstore) -> str:
    return f"{cluster_id}/{lvstore}"


def load(db, cluster_id, lvstore) -> FenceReleaseLedger:
    key = _key(cluster_id, lvstore)
    # Prefix read: "<cluster>/LVS_1" also matches "<cluster>/LVS_10", so pick
    # the exact key out of the (at most a handful of) records that share it.
    for ledger in FenceReleaseLedger().read_from_db(db.kv_store, id=key):
        if ledger.get_id() == key:
            return ledger
    ledger = FenceReleaseLedger()
    ledger.cluster_id = cluster_id
    ledger.lvstore = lvstore
    return ledger


def _store(db, ledger: FenceReleaseLedger, mutate) -> FenceReleaseLedger:
    """Apply ``mutate(ledger)`` under compare-and-set; create the record if
    this is its first write."""
    fresh = db.atomic_update(ledger, mutate)
    if fresh is not None:
        return fresh
    mutate(ledger)
    ledger.write_to_db(db.kv_store)
    return ledger


def _is_preferred_path(db, node, lvstore) -> bool:
    preferred = preferred_path(lvstore, db.get_storage_nodes_by_cluster_id(node.cluster_id))
    return preferred is not None and preferred.get_id() == node.get_id()


def exhausted(db, node, port, now: float | None = None) -> bool:
    """Whether releasing ``port`` on ``node`` again would feed a fence loop.

    False for a port that serves no known lvstore, and always False for the
    lvstore's preferred path -- that one is reopened whatever the count."""
    now = time.time() if now is None else now
    lvstore = lvstore_of_port(node, port)
    if lvstore is None or _is_preferred_path(db, node, lvstore):
        return False
    return load(db, node.cluster_id, lvstore).exhausted(now)


def record_release(db, node, port, now: float | None = None) -> None:
    """Remember that ``port`` on ``node`` was just unblocked."""
    now = time.time() if now is None else now
    lvstore = lvstore_of_port(node, port)
    if lvstore is None:
        return
    node_id = node.get_id()
    _store(db, load(db, node.cluster_id, lvstore), lambda ledger: ledger.record(node_id, now))


def forget_node(db, node) -> None:
    """Drop ``node``'s stamps from every lvstore it serves: its SPDK was just
    restarted and the reopen that follows is part of the restart, not of a
    loop (review of #1580)."""
    node_id = node.get_id()
    lvstores = {node.lvstore} if node.lvstore else set()
    lvstores.update(name for name in (node.lvstore_ports or {}))
    for lvstore in sorted(lvstores):
        ledger = load(db, node.cluster_id, lvstore)
        if node_id not in ledger.releases:
            continue
        _store(db, ledger, lambda fresh: fresh.forget(node_id))


def refuse(db, node, port, lvstore, held_sec: float, now: float | None = None) -> None:
    """Leave the fence in place and say so -- in the log on every tick, in the
    cluster event log once per window."""
    now = time.time() if now is None else now
    ledger = load(db, node.cluster_id, lvstore)
    count = ledger.count(now)
    reopens_in = ledger.reopens_in(now)
    logger.error(
        "Port %s on %s blocked %.0fs for %s, but paths of %s were already "
        "released %d times in the last %ds and SPDK fenced them again each "
        "time -- leaving it blocked (budget re-opens in %.0fs; the preferred "
        "path stays open). An IO on %s keeps failing on every node; look for "
        "'distr error' events before unblocking.",
        port, node.get_id(), held_sec, lvstore, lvstore, count,
        constants.FENCE_RELEASE_WINDOW_SEC, reopens_in, lvstore)
    if not ledger.should_report(now):
        return

    def _mark(fresh):
        if not fresh.should_report(now):
            return False
        fresh.loop_reported_at = now
        return None

    stored = _store(db, ledger, _mark)
    if stored.loop_reported_at != now:
        return  # another process reported this window
    storage_events.snode_fence_loop_detected(
        node, port, lvstore, count, constants.FENCE_RELEASE_WINDOW_SEC)
