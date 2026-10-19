"""The log surface (design §9).

The baseline is logging only: the control-plane services expose no Prometheus
registry, and a primitive used equally from a short-lived CLI invocation and a
long-lived service designs its log records first. Two of these exist for the
design's own sake rather than to mirror an existing lock — a watch that
silently stops working degrades the primitive to polling with no other symptom,
and a heartbeat read from the future is the one piece of evidence that two
hosts' clocks disagree, which is what the expiry margin is silently spending
itself on.

``utils.get_logger`` hands back the root logger, so ``caplog`` sees these
records without any propagation setup.
"""

import logging
import threading
import time

import fdb

from simplyblock_core.models import lock as lock_mod
from simplyblock_core.models.lock import (
    DbLock,
    _heartbeat,
    _Hold,
    expiry_deadline,
    new_owner,
    store,
)
from simplyblock_core.models.lock.record import EMPTY_FENCE, DbLockRecord, key
from tests.integration.db_lock.conftest import reclaim, take


def _budget():
    """The lease a holder keeps after its last successful refresh."""
    return lock_mod.LEASE_SEC - lock_mod.EXPIRY_MARGIN_SEC


def _matching(caplog, level, *needles):
    return [r for r in caplog.records
            if r.levelno == level and all(n in r.getMessage() for n in needles)]


def _granted_hold(db, name):
    owner = new_owner()
    fence = take(db, name, owner)
    acquired_at = time.monotonic()
    return _Hold(name=name, owner=owner, fence=fence, acquired_at=acquired_at,
                 deadline=expiry_deadline(acquired_at))


def test_reclaiming_a_stale_record_warns(db, name, caplog):
    """Another process died holding the lock, which is never routine.

    Emitted after the commit rather than inside the transaction body, which
    FoundationDB may retry — a log line in there would be written once per
    attempt.
    """
    dead = new_owner()
    take(db, name, dead)
    with caplog.at_level(logging.WARNING):
        reclaim(db, name, new_owner())
    assert _matching(caplog, logging.WARNING, name, dead, "stale")


def test_watch_fallback_warns(db, name, caplog, monkeypatch):
    """A silent fallback to polling looks identical to a working watch."""

    def broken_watch(*args, **kwargs):
        raise fdb.FDBError(1510)

    other = new_owner()
    take(db, name, other)
    monkeypatch.setattr(store, "watch", broken_watch)
    threading.Timer(0.5, store.release, args=(db, name, other)).start()

    with caplog.at_level(logging.WARNING):
        lock = DbLock(name, timeout=20)
        lock.acquire()
        lock.release()
    assert _matching(caplog, logging.WARNING, name, "watch unavailable")


def test_a_heartbeat_from_the_future_warns(db, name, caplog):
    """Two hosts disagreeing about the time is what eats the expiry margin.

    A heartbeat ahead of the reclaiming host's clock cannot have happened, so
    it is evidence of skew rather than of anything the lock did. Nothing else
    reports it: the margin is absolute and every lease is measured against it,
    so a cluster whose clocks have drifted looks exactly like one whose clocks
    have not — right up until two owners hold one name.
    """
    ahead = new_owner()
    now = time.time()
    record = DbLockRecord(name=name, owner=ahead, acquired_at=now,
                          heartbeat_at=now + 30, expires_at=now - 1)
    db[key(name)] = record.to_value(EMPTY_FENCE)

    with caplog.at_level(logging.WARNING):
        take(db, name, new_owner())
    assert _matching(caplog, logging.WARNING, name, "ahead of this host's clock")


def test_no_record_carries_a_credential(db, name, caplog):
    """The owner string is host, pid, thread and a nonce — nothing else.

    Nothing in this primitive handles a secret, so the assertion is that the one
    identifier it *does* mint and log stays that way.
    """
    with caplog.at_level(logging.DEBUG):
        lock = DbLock(name, timeout=10)
        lock.acquire()
        owner = lock._hold.owner
        lock.release()

    assert "@" not in owner and ":" not in owner
    for record in caplog.records:
        message = record.getMessage().lower()
        assert "password" not in message
        assert "://" not in message


def test_a_reclaim_under_a_live_holder_logs_an_error(db, name, caplog):
    """An ERROR, not a WARNING. A critical section is now running
    unprotected, which is never routine."""
    lock = DbLock(name, timeout=10)
    lock.acquire()
    with caplog.at_level(logging.WARNING):
        reclaim(db, name, new_owner())
        while lock.is_held():
            time.sleep(0.2)
    lock.release()

    assert _matching(caplog, logging.ERROR, name, "reclaimed by another owner")
    assert not _matching(caplog, logging.WARNING, name, "reclaimed by another owner")


def test_a_retried_refresh_warns_and_a_self_expiry_errors(db, broken_db, name, caplog, fault_lease):
    """A silent retry hides a database fault the operator wants,
    and the stand-down names the last successful refresh so the outage window
    is readable."""
    hold = _granted_hold(db, name)
    thread = threading.Thread(target=_heartbeat, args=(broken_db, hold), daemon=True)
    with caplog.at_level(logging.WARNING):
        thread.start()
        assert hold.lost.wait(_budget() + 15) is True
        thread.join(timeout=10)

    assert _matching(caplog, logging.WARNING, name, "reached no verdict")
    expiry = _matching(caplog, logging.ERROR, name, "expired its own lease")
    assert expiry
    assert "without a successful refresh" in expiry[0].getMessage()
