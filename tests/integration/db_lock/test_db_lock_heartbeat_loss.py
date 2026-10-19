"""What the heartbeat does when a refresh does not succeed (design §5.3).

This is the group design §10 names as reachable only from tests that make
FoundationDB fail underneath a live holder, and every test here distinguishes
*reclaimed* from *could not tell* — the distinction the three existing locks do
not make, and the one that decides whether a holder stops or retries.

**The two cases are produced differently, and that is the point.**

*Could not tell* runs the real :func:`_heartbeat` against the ``broken_db``
handle: a second, real ``fdb.Database`` opened on a cluster nothing answers on,
so ``store.refresh`` fails with an ``FDBError`` out of a real transaction.
Nothing is mocked, which is only possible because the store takes its handle as
an argument — patching ``DBController`` is what ``tests/AGENTS.md`` forbids. The
lock is granted over the live handle first, because what is under test is a
holder that *has* a lease and then loses contact with the database.

*Reclaimed* needs no injection at all: a second owner takes the record over the
live handle through the ``reclaim`` helper, which dates its own ``now`` past
the holder's ``expires_at`` — the ordinary reclaim predicate, reached without
waiting for it — and the holder's next refresh then truthfully returns
``False``. What that helper cannot do is take a *live* lease, since the
predicate is the predicate: it is a way to skip the waiting, not the rule.

The one place a spy stands in for either is where a refresh has to fail and then
*recover*: no handle can be made to break and heal inside a test,
so the failure is injected at the store boundary while the recovery runs
against real FoundationDB.
"""

import threading
import time

import pytest

from simplyblock_core.models import lock as lock_mod
from simplyblock_core.models.lock import (
    DbLock,
    DbLockLostError,
    DbLockUnavailableError,
    _heartbeat,
    _Hold,
    expiry_deadline,
    new_owner,
    store,
)
from simplyblock_core.models.lock.record import DbLockRecord, key
from tests.integration.db_lock.conftest import reclaim, take


def _period():
    """The live refresh period, read through the module so ``short_lease`` shows."""
    return lock_mod.HEARTBEAT_PERIOD_SEC


def _budget():
    """The lease a holder keeps after its last successful refresh."""
    return lock_mod.LEASE_SEC - lock_mod.EXPIRY_MARGIN_SEC


def _granted_hold(db, name):
    """A real grant over the live handle, with no heartbeat thread attached."""
    owner = new_owner()
    acquired_at = time.monotonic()
    fence = take(db, name, owner)
    return _Hold(name=name, owner=owner, fence=fence, acquired_at=acquired_at,
                 deadline=expiry_deadline(acquired_at))


def _reclaim(db, name):
    """Take ``name`` from its current holder through a real transaction."""
    other = new_owner()
    reclaim(db, name, other)
    return other


# --- Could not tell: the unreachable database ------------------------------

def test_unavailable_refresh_does_not_stop_the_heartbeat(db, broken_db, name, fault_lease):
    """Retried inside the remaining lease, then self-expiry.

    The retry gap is REFRESH_RETRY_SEC rather than a whole heartbeat
    period, and the holder stands down at last_success + ttl - margin without
    ever hearing from the database. Both halves matter: the first is the budget
    the divisor buys, and the second is what bounds the overlap with a
    reclaiming owner — a partitioned holder is precisely the one that will never
    receive a reclaim notice.
    """
    hold = _granted_hold(db, name)
    attempts: list[float] = []
    real = store.refresh

    def counting_refresh(handle, lock_name, owner, lease):
        attempts.append(time.monotonic())
        return real(handle, lock_name, owner, lease)

    started = time.monotonic()
    thread = threading.Thread(
        target=_heartbeat, args=(broken_db, hold), daemon=True)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store, "refresh", counting_refresh)
        thread.start()
        assert hold.lost.wait(_budget() + 15) is True
    thread.join(timeout=10)

    elapsed = time.monotonic() - started
    assert _budget() - 2 <= elapsed <= _budget() + 8, (
        f"stood down after {elapsed:.1f}s, not at the {_budget():.0f}s deadline")
    # At one attempt per heartbeat period there would be ~3 in that window.
    assert len(attempts) >= 4, (
        f"{len(attempts)} attempts in {elapsed:.1f}s — retrying at the period, "
        "not at REFRESH_RETRY_SEC")
    # No attempt may be scheduled past the deadline: a success there would
    # extend a lease the holder has already written off.
    assert max(attempts) <= started + _budget() + 1


def test_self_expiry_precedes_another_hosts_reclaim(db, broken_db, name, fault_lease):
    """The margin is the gap between standing down and being reclaimed.

    The holder's deadline arrives lock_mod.EXPIRY_MARGIN_SEC before the record
    itself expires, which is what stops a partitioned holder from still being
    inside its critical section when another host's clock says the lease is
    free.
    """
    hold = _granted_hold(db, name)
    thread = threading.Thread(target=_heartbeat, args=(broken_db, hold), daemon=True)
    thread.start()
    assert hold.lost.wait(_budget() + 15) is True
    thread.join(timeout=10)

    record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    age = time.time() - record.heartbeat_at
    assert age < lock_mod.LEASE_SEC, (
        f"the record was already reclaimable ({age:.1f}s old) when its holder "
        "stood down; the margin bought nothing")
    assert lock_mod.LEASE_SEC - age >= lock_mod.EXPIRY_MARGIN_SEC - 3


def test_self_expiry_is_terminal(db, broken_db, name, fault_lease):
    """A holder that gave the lease up does not resume on reconnect.

    The record may have changed hands and the fence may have moved, so a
    refresh succeeding on a record this owner no longer legitimately holds
    would resurrect a lease the rest of the cluster has already written off.
    """
    hold = _granted_hold(db, name)
    thread = threading.Thread(target=_heartbeat, args=(broken_db, hold), daemon=True)
    thread.start()
    assert hold.lost.wait(_budget() + 15) is True
    thread.join(timeout=10)
    assert not thread.is_alive(), "the heartbeat thread outlived the lease it gave up"

    before, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    time.sleep(_period() + 1)
    after, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    assert after.heartbeat_at == before.heartbeat_at


def test_transient_failure_is_survived(db, name, fault_lease):
    """One blip leaves the lock held and heartbeat_at advancing.

    The regression this group exists for. The shape these heartbeats are
    modelled on returns on the first falsy refresh, and the accessor it calls
    returns that same falsy value for a missing connection — so a one-second
    blip there does not risk expiry, it guarantees it one lease later with the
    holder still running. An implementation that reintroduces the conflation
    passes every other test in this file.
    """
    hold = _granted_hold(db, name)
    before, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    real = store.refresh
    failures = []

    def flaky_refresh(handle, lock_name, owner, lease):
        if len(failures) < 2:
            failures.append(time.monotonic())
            raise DbLockUnavailableError("injected outage")
        return real(handle, lock_name, owner, lease)

    thread = threading.Thread(target=_heartbeat, args=(db, hold), daemon=True)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store, "refresh", flaky_refresh)
        thread.start()
        deadline = time.monotonic() + _budget()
        while time.monotonic() < deadline:
            record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
            if record.heartbeat_at > before.heartbeat_at:
                break
            time.sleep(0.2)
        else:
            pytest.fail("the heartbeat never recovered from two transient failures")
        assert len(failures) == 2
        assert not hold.lost.is_set()
        hold.stop.set()
    thread.join(timeout=10)


# --- Told: the reclaim -----------------------------------------------------

def test_reclaim_stops_the_heartbeat_at_once(db, name):
    """A False refresh is an answer, so there is no budget to spend.

    A holder that has been told stops immediately, where a holder that cannot
    tell retries — which is the whole distinction store.refresh introduces.
    """
    lock = DbLock(name, timeout=10)
    lock.acquire()
    assert lock.is_held() is True
    _reclaim(db, name)

    stood_down = time.monotonic()
    while lock.is_held() and time.monotonic() - stood_down < _budget():
        time.sleep(0.2)
    elapsed = time.monotonic() - stood_down
    assert lock.is_held() is False
    assert elapsed < _period() + 3, (
        f"took {elapsed:.1f}s to notice a reclaim; it spent the retry budget on "
        "an answer it had already been given")
    lock.release()


def test_release_after_a_lost_lease(db, name):
    """Raises nothing, frees the local tier, leaves the new owner alone.

    A holder that declined to give up the local tier because it lost the
    distributed one would wedge this process for that name.
    """
    lock = DbLock(name, timeout=10)
    lock.acquire()
    successor = _reclaim(db, name)
    while lock.is_held():
        time.sleep(0.2)

    lock.release()
    record, _ = DbLockRecord.from_value(bytes(db[key(name)]))
    assert record.owner == successor, "an owner-scoped release deleted a foreign record"

    store.release(db, name, successor)
    later = DbLock(name, timeout=10)
    later.acquire()  # the local tier must have been freed
    later.release()


def test_exit_raises_lost_for_a_block_that_returned_normally(db, name):
    """A section that completed under a lease it no longer held did
    not do what its caller asked, and returning normally would report a success
    that did not happen."""
    lock = DbLock(name, timeout=10)
    with pytest.raises(DbLockLostError) as caught:
        with lock:
            _reclaim(db, name)
            while lock.is_held():
                time.sleep(0.2)
    assert caught.value.name == name
    assert caught.value.owner
    assert caught.value.lost_for >= 0
    assert name in str(caught.value)


def test_a_raising_block_keeps_its_own_exception(db, name):
    """The original failure is the more useful one, and is quite often
    the same outage seen from closer up."""
    marker = RuntimeError("what the block was actually failing with")
    lock = DbLock(name, timeout=10)
    with pytest.raises(RuntimeError) as caught:
        with lock:
            _reclaim(db, name)
            while lock.is_held():
                time.sleep(0.2)
            raise marker
    assert caught.value is marker


def test_unexpected_heartbeat_error_is_treated_as_a_lost_lease(db, name):
    """Nothing propagates out of the thread.

    An unhandled exception kills a daemon thread silently, which stops the
    heartbeat with no record anywhere and guarantees the expiry the thread
    exists to prevent.
    """
    hold = _granted_hold(db, name)

    def exploding_refresh(handle, lock_name, owner, lease):
        raise ValueError("not an FDB failure at all")

    thread = threading.Thread(target=_heartbeat, args=(db, hold), daemon=True)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store, "refresh", exploding_refresh)
        thread.start()
        assert hold.lost.wait(_period() + 10) is True
    thread.join(timeout=10)
    assert not thread.is_alive()


@pytest.mark.timeout(90)
def test_a_dead_holds_verdict_does_not_reach_its_successor(db, name):
    """The hold, not the lock, is the unit of grant state.

    A released hold's heartbeat thread does not stop instantly — it can be
    inside a store.refresh that will not return for up to the handle's
    transaction timeout. With grant state on the instance, that thread's verdict
    would land on whatever hold is current when it finally returns: a stale
    refresh returning False would set ``lost`` under a *successor* hold, and the
    next __exit__ would raise for a lease that never ended.
    """
    lock = DbLock(name, timeout=10)
    lock.acquire()
    first = lock._hold
    _reclaim(db, name)
    while lock.is_held():
        time.sleep(0.2)
    assert first.lost.is_set()
    lock.release()
    store.release(db, name, store.get(db, name)[0].owner)

    with lock:
        assert lock.is_held() is True
        assert lock._hold is not first
        assert not lock._hold.lost.is_set()


def test_release_waits_for_the_heartbeat_thread(db, name):
    """No refresh may rewrite the key after the delete.

    Without the join, store.release can delete the record a still-running
    refresh is about to rewrite, resurrecting a released lease as a record with
    a fresh heartbeat and no owner intending to hold it.
    """
    lock = DbLock(name, timeout=10)
    lock.acquire()
    thread = lock._hold.thread
    lock.release()
    assert not thread.is_alive(), "release() returned with its heartbeat still running"
    assert store.get(db, name) is None
    time.sleep(_period() + 1)
    assert store.get(db, name) is None, "a late refresh resurrected the released record"


def test_successive_holds_mint_a_new_owner_and_fence(db, name):
    """One instance reused, as a module-level LOCK normally is."""
    lock = DbLock(name, timeout=10)
    with lock:
        first_owner, first_fence = lock._hold.owner, lock.fence
    with lock:
        assert lock._hold.owner != first_owner
        assert lock.fence > first_fence


def test_expiry_deadline_matches_what_the_thread_enforces():
    """Stated once against the constants, beside the tests that observe it."""
    assert expiry_deadline(0.0) == _budget()
