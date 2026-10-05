"""Lease expiry and reclaim.

Expiry is the only release path for a holder that stops running, so these tests
are what bound how long a dead holder blocks a name.

**Where the wall clock is spent, and where it is not.** The rule under test is
``now > expires_at``, against the deadline the *holder* wrote — so a record
persisted with a past ``expires_at`` exercises it exactly, and dating the
acquirer's own ``now`` forward exercises it from the other side. Neither spends
wall clock, and both hit the boundary more precisely than sleeping could.

What has to be paid for in real time is the opposite claim: that a heartbeating
holder is *not* preempted. Nothing but a running thread can demonstrate that,
so those tests shrink the lease through the ``short_lease`` fixture rather than
shortening what they wait for. The lease is no longer a constructor argument —
it is the mechanism's, not the caller's — so a test that wants a narrow one
patches the constants, keeping the relations between them intact.
"""

import threading
import time

import pytest

from simplyblock_core.models import lock as lock_mod
from simplyblock_core.models.lock import DbLock, DbLockBusyError, new_owner, store
from simplyblock_core.models.lock.record import EMPTY_FENCE, DbLockRecord, key
from tests.integration.db_lock.conftest import take


def _persist(db, name, owner, expires_in):
    """A real record whose lease ends ``expires_in`` seconds from now.

    Negative for a lease that has already run out. It is ``expires_at`` that
    decides a reclaim, not the heartbeat: a holder writes its own deadline, so
    backdating the heartbeat alone would leave a record that still reads as
    perfectly live.
    """
    now = time.time()
    record = DbLockRecord(name=name, owner=owner, acquired_at=now - 1,
                          heartbeat_at=now - 1, expires_at=now + expires_in)
    db[key(name)] = record.to_value(EMPTY_FENCE)
    return record


def test_stale_record_is_reclaimable(db, name):
    """A holder that stopped heartbeating no longer blocks the name."""
    dead = new_owner()
    _persist(db, name, dead, expires_in=-1)

    lock = DbLock(name, timeout=5)
    lock.acquire()
    try:
        record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
        assert record.owner != dead
    finally:
        lock.release()


def test_fresh_record_is_not_reclaimable(db, name):
    """A live-but-slow holder is never preempted."""
    alive = new_owner()
    _persist(db, name, alive, expires_in=60)

    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=0)
    record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    assert record.owner == alive


def test_reclaim_boundary_brackets_the_expiry(db, name):
    """A lease just inside its deadline is fresh; just outside, free.

    A reclaim even slightly early is a second live owner. The boundary is
    bracketed rather than hit exactly: the acquire takes its own ``time.time()``
    reading, always a shade later than the one that wrote the record, so "at
    exactly the deadline" is not a state a wall-clock comparison can be put in.
    """
    owner = new_owner()
    _persist(db, name, owner, expires_in=1)
    with pytest.raises(DbLockBusyError):
        take(db, name, new_owner())

    _persist(db, name, owner, expires_in=-1)
    assert take(db, name, new_owner())


def test_a_holders_own_deadline_is_what_is_judged(db, name):
    """Not one the acquirer re-derives from its own lease width.

    Two control-plane versions with different LEASE_SEC values meet during a
    rolling upgrade. The one with the shorter lease must still honour a record
    written by the longer-lived one: judging by its own width would preempt a
    holder sitting well inside the lease it promised itself, which is two live
    owners of one name.
    """
    patient = new_owner()
    _persist(db, name, patient, expires_in=lock_mod.LEASE_SEC * 10)

    with pytest.raises(DbLockBusyError):
        take(db, name, new_owner(), lease=1.0)

    record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    assert record.owner == patient


def test_live_holder_survives_more_than_one_lease(db, name, short_lease):
    """A heartbeating holder is never preempted, however long it runs.

    Real wall clock by necessity — only a running thread can show that the
    heartbeat refreshes on its own schedule — but a sub-second lease through
    ``short_lease``, so "longer than one full lease" costs a couple of seconds
    rather than a minute.
    """
    _period, lease, _deadline = short_lease
    lock = DbLock(name, timeout=10)
    lock.acquire()
    try:
        first, _ = DbLockRecord.from_value(bytes(db[key(name)]))
        time.sleep(lease * 1.5)
        later, _ = DbLockRecord.from_value(bytes(db[key(name)]))

        assert later.owner == first.owner
        assert later.acquired_at == first.acquired_at
        assert later.heartbeat_at > first.heartbeat_at
        # The heartbeat buys time, it does not merely report: a refresh that
        # advanced heartbeat_at alone would leave the record expiring on the
        # original deadline while its holder believed otherwise.
        assert later.expires_at > first.expires_at
        assert lock.is_held() is True
        # Another owner must still be refused, which is the point of staying alive.
        with pytest.raises(DbLockBusyError):
            take(db, name, new_owner())
    finally:
        lock.release()


def test_racing_reclaimers_resolve_to_one_winner(db, name):
    """The read and the write share one transaction, so a stale record
    cannot be reclaimed twice."""
    _persist(db, name, new_owner(), expires_in=-1)

    start = threading.Barrier(8)
    outcomes: list = []
    guard = threading.Lock()

    def contend():
        owner = new_owner()
        start.wait()
        try:
            take(db, name, owner)
        except DbLockBusyError as busy:
            with guard:
                outcomes.append(("lost", busy.owner))
        else:
            with guard:
                outcomes.append(("won", owner))

    threads = [threading.Thread(target=contend) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    winners = [owner for verdict, owner in outcomes if verdict == "won"]
    assert len(winners) == 1, f"{len(winners)} winners: {winners}"
    record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    assert record.owner == winners[0]
    # Every loser was told who beat it, not just that it lost.
    assert all(owner for verdict, owner in outcomes if verdict == "lost")


def test_refresh_by_a_displaced_owner_returns_false(db, name):
    """An answer the database gave, not an error."""
    displaced = new_owner()
    _persist(db, name, displaced, expires_in=-1)
    take(db, name, new_owner())
    assert store.refresh(db, name, displaced, lock_mod.LEASE_SEC) is False


def test_refresh_of_a_missing_record_returns_false(db, name):
    assert store.refresh(db, name, new_owner(), lock_mod.LEASE_SEC) is False


def test_heartbeat_stops_on_release(db, name, short_lease):
    """No refresh after the release, so nothing resurrects the record.

    A refresh landing after the delete would recreate the key with a fresh
    heartbeat and no owner intending to hold it — a lock held by nobody for a
    whole lease, which is exactly what expiry exists to bound.
    """
    period, _lease, _deadline = short_lease
    lock = DbLock(name, timeout=10)
    lock.acquire()
    lock.release()
    assert store.get(db, name) is None
    # More than one refresh period: a thread still running would have written by now.
    time.sleep(period * 2)
    assert store.get(db, name) is None
