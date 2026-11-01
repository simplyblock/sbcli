"""
port_release_budget.py — the gate every fence release goes through.

See ``models/fence_release.py`` for why this exists. Three call sites:

* ``storage_node_monitor._remediate_stale_port_blocks`` -- asks ``exhausted``
  before lifting a stale fence, ``record_release`` after, ``refuse`` instead
  of lifting once the budget is gone;
* ``health_check_service`` -- does not schedule a port_allow task for a port
  whose budget is gone;
* ``tasks_runner_port_allow`` -- skips such a port when it reopens a node's
  LVS ports, and aborts the task if that was the port it was created for.

The ledger is one small FDB record per (cluster, node, port); updates are
compare-and-set through ``DBController.atomic_update`` so the monitor and the
task runner cannot overwrite each other's stamps.
"""

from __future__ import annotations

import logging
import time

from simplyblock_core import constants
from simplyblock_core.controllers import storage_events
from simplyblock_core.models.fence_release import FenceReleaseLedger

logger = logging.getLogger()


def _key(node, port) -> str:
    return f"{node.cluster_id}/{node.get_id()}/{port}"


def load(db, node, port) -> FenceReleaseLedger:
    key = _key(node, port)
    # Prefix read: "<cluster>/<node>/443" also matches ".../4430", so pick the
    # exact key out of the (at most a handful of) records that share it.
    for ledger in FenceReleaseLedger().read_from_db(db.kv_store, id=key):
        if ledger.get_id() == key:
            return ledger
    ledger = FenceReleaseLedger()
    ledger.cluster_id = node.cluster_id
    ledger.node_id = node.get_id()
    ledger.port = port
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


def exhausted(db, node, port, now: float | None = None) -> bool:
    """Whether releasing ``port`` on ``node`` again would feed a fence loop."""
    now = time.time() if now is None else now
    return load(db, node, port).exhausted(now)


def record_release(db, node, port, now: float | None = None) -> None:
    """Remember that ``port`` on ``node`` was just unblocked."""
    now = time.time() if now is None else now
    _store(db, load(db, node, port), lambda ledger: ledger.record(now))


def refuse(db, node, port, lvstore, held_sec: float, now: float | None = None) -> None:
    """Leave the fence in place and say so -- in the log on every tick, in the
    cluster event log once per window."""
    now = time.time() if now is None else now
    ledger = load(db, node, port)
    reopens_in = ledger.reopens_in(now)
    logger.error(
        "Port %s on %s blocked %.0fs for %s, but it was already released %d "
        "times in the last %ds and SPDK fenced it again each time -- leaving "
        "it blocked (budget re-opens in %.0fs). An IO on %s keeps failing on "
        "every node; look for 'distr error' events before unblocking.",
        port, node.get_id(), held_sec, lvstore, len(ledger.releases),
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
        node, port, lvstore, len(ledger.releases), constants.FENCE_RELEASE_WINDOW_SEC)
