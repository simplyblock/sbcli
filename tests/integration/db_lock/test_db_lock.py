"""Acquire, release, the wait, ``try_acquire``, the context manager and the fence.

Real FoundationDB, storage nodes not involved.

**A second owner is a ``store`` call, never a second ``DbLock``.** The local
tier is a module-level map keyed by name, so two ``DbLock`` objects for one name
in one process are the same lock: the loser never issues an FDB call, and a test
written that way would assert ``threading.Lock`` behaviour while passing against
a broken or absent distributed tier. Wherever a test needs a *different owner*
to hold, refresh or release a name, that owner is a direct ``store`` call,
which takes no local lock. Tests that deliberately exercise the local tier live in
``test_db_lock_local_tier.py``.
"""

import threading
import time

import fdb
import pytest

from simplyblock_core.models import lock as lock_mod
from simplyblock_core.models.lock import (
    POLL_SEC,
    DbLock,
    DbLockBusyError,
    DbLockUnavailableError,
    new_owner,
    store,
)
from simplyblock_core.models.lock.record import FENCE_LEN, DbLockRecord, key
from tests.integration.db_lock.conftest import reclaim, take


def _record(db, name):
    got = store.get(db, name)
    return None if got is None else got[0]


# --- Acquire and release (design §4.2, §5.1, §5.4) -------------------------

def test_acquire_writes_a_record(db, name):
    lock = DbLock(name)
    lock.acquire(timeout=5)
    try:
        raw = db[key(name)]
        assert raw is not None
        record, fence = DbLockRecord.from_value(bytes(raw))
        assert record.name == name
        assert record.owner
        assert record.acquired_at > 0
        assert record.heartbeat_at >= record.acquired_at
        assert len(fence) == FENCE_LEN
    finally:
        lock.release()


def test_zero_timeout_against_a_live_owner(db, name):
    """Raises naming that owner, without waiting."""
    other = new_owner()
    take(db, name, other)

    lock = DbLock(name)
    started = time.monotonic()
    with pytest.raises(DbLockBusyError) as caught:
        lock.acquire(timeout=0)
    assert time.monotonic() - started < 2
    assert caught.value.owner == other
    assert _record(db, name).owner == other


def test_zero_timeout_against_a_free_name(db, name):
    lock = DbLock(name)
    started = time.monotonic()
    lock.acquire(timeout=0)
    assert time.monotonic() - started < 2
    lock.release()


def test_release_deletes_the_record(db, name):
    lock = DbLock(name)
    lock.acquire(timeout=5)
    lock.release()
    assert _record(db, name) is None


def test_foreign_release_leaves_the_record(db, name):
    """Owner scoped, so a late release never deletes a lock another owner
    has since reclaimed."""
    owner = new_owner()
    take(db, name, owner)
    store.release(db, name, new_owner())
    assert _record(db, name).owner == owner


def test_retake_by_the_same_owner_preserves_acquired_at(db, name):
    owner = new_owner()
    take(db, name, owner)
    first = _record(db, name).acquired_at
    time.sleep(0.05)
    take(db, name, owner)
    assert _record(db, name).acquired_at == first


def test_prefix_sharing_names_do_not_cross_lock(db, name):
    """Point reads are exact, so "a/1" and "a/10" are separate locks."""
    short, long = f"{name}/1", f"{name}/10"
    take(db, short, new_owner())
    lock = DbLock(long)
    lock.acquire(timeout=0)
    lock.release()


def test_unreachable_handle(broken_db, name):
    """Both layers raise, and no local lock is left held."""
    with pytest.raises(DbLockUnavailableError):
        take(broken_db, name, new_owner())

    with pytest.raises(DbLockUnavailableError):
        DbLock(name, timeout=1, kv_store=broken_db).acquire()
    # The local tier must be free for the next caller, or every later acquire
    # of this name in this process deadlocks.
    survivor = DbLock(name)
    survivor.acquire(timeout=0)
    survivor.release()


# --- try_acquire: contention as a value ------------------------------------

def test_try_acquire_reports_a_live_owner_as_false(db, name):
    """The whole point of the form — contention is an answer, not a raise."""
    other = new_owner()
    take(db, name, other)

    started = time.monotonic()
    assert DbLock(name).try_acquire(timeout=0) is False
    assert time.monotonic() - started < 2
    assert _record(db, name).owner == other


def test_try_acquire_takes_a_free_name(db, name):
    """A True is a lock and not merely a verdict: the name is held on return."""
    lock = DbLock(name)
    assert lock.try_acquire(timeout=0) is True
    assert lock.is_held() is True
    assert _record(db, name).owner == lock._hold.owner
    lock.release()


def test_try_acquire_waits_like_acquire(db, name):
    """Only the reporting differs — the timeout means the same thing.

    A form that quietly probed instead of waiting would satisfy the two cases
    above and still be a different primitive from the one it documents.
    """
    other = new_owner()
    take(db, name, other)
    threading.Timer(0.5, store.release, args=(db, name, other)).start()

    lock = DbLock(name)
    assert lock.try_acquire(timeout=15) is True
    lock.release()


def test_try_acquire_does_not_flatten_an_outage_into_contention(broken_db, name):
    """The case with teeth: DbLockUnavailableError must still raise.

    A database that gave no verdict is not a name someone else holds, and a
    caller that reads "busy" off an outage takes its contention path for a lock
    that may well be free.
    """
    with pytest.raises(DbLockUnavailableError):
        DbLock(name, timeout=1, kv_store=broken_db).try_acquire()


def test_try_acquire_frees_the_local_tier_on_a_false(db, name):
    """The local-tier leak check for this form: a leak wedges the name
    process-wide, and no other test here would see it."""
    other = new_owner()
    take(db, name, other)
    assert DbLock(name).try_acquire(timeout=0) is False
    store.release(db, name, other)

    later = DbLock(name, timeout=10)
    later.acquire()
    later.release()


# --- Blocking wait and watch (design §5.2) ---------------------------------

def test_blocking_acquire_observes_a_release(db, name):
    other = new_owner()
    take(db, name, other)
    threading.Timer(0.5, store.release, args=(db, name, other)).start()

    lock = DbLock(name)
    lock.acquire(timeout=15)
    lock.release()


def test_acquire_raises_at_the_deadline(db, name):
    """DbLockBusyError at the deadline, and no lock held.

    The timeout spans more than one heartbeat period at this TTL, so the watch
    fires on the holder's own heartbeat writes as well. What this asserts is
    that those spurious wakes change nothing.
    """
    other = new_owner()
    take(db, name, other)

    lock = DbLock(name)
    started = time.monotonic()
    with pytest.raises(DbLockBusyError):
        lock.acquire(timeout=7)
    assert 6.5 <= time.monotonic() - started < 12
    assert lock.is_held() is False
    assert _record(db, name).owner == other


def test_waiter_wakes_within_one_round_trip(db, name):
    """The watch short-circuits the wait.

    Without this an implementation that silently falls back to poll-only
    passes every other test in this group.
    """
    other = new_owner()
    take(db, name, other)
    release_at = time.monotonic() + 1.0
    threading.Timer(1.0, store.release, args=(db, name, other)).start()

    lock = DbLock(name)
    lock.acquire(timeout=20)
    latency = time.monotonic() - release_at
    lock.release()
    assert latency < POLL_SEC / 2, (
        f"woke {latency:.2f}s after the release; the wait is polling, not watching")


def test_timeout_zero_makes_exactly_one_attempt(db, name):
    """A probe, not a no-op — the attempt runs before the deadline test."""
    lock = DbLock(name)
    started = time.monotonic()
    lock.acquire(timeout=0)
    assert time.monotonic() - started < 2
    lock.release()

    take(db, name, new_owner())
    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=0)


def test_watch_failure_degrades_the_pass_not_the_acquire(db, name, monkeypatch):
    """Logged, and the wait falls back to the poll ceiling, still
    acquiring. Letting it escape would turn a transient database error into an
    exception out of acquire(), which the loop promises never to do."""

    def broken_watch(*args, **kwargs):
        raise fdb.FDBError(1510)

    other = new_owner()
    take(db, name, other)
    monkeypatch.setattr(store, "watch", broken_watch)
    threading.Timer(0.5, store.release, args=(db, name, other)).start()

    lock = DbLock(name)
    lock.acquire(timeout=20)
    lock.release()


def test_watch_cancel_fires_the_callback(db, name):
    """A cancelled pass can never leave the event unset.

    This is what makes "blocked on an event nothing will set" structurally
    absent rather than handled.
    """
    fired = threading.Event()
    watch = store.watch(db, name)
    watch.on_ready(lambda _f: fired.set())
    watch.cancel()
    assert fired.wait(5) is True


def test_timed_out_acquire_leaves_no_watch_and_spawns_no_thread(db, name):
    """The final pass's watch is cancelled by its own finally, and waiting
    costs no thread — the callback runs on the fdb network thread."""
    take(db, name, new_owner())
    before = threading.active_count()
    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=6)
    assert threading.active_count() <= before


def test_bare_acquire_is_bounded_by_the_lock_ceiling(db, name):
    """A no-argument acquire() is bounded, unlike threading.Lock.

    The ceiling is the constructor timeout — the constant cannot be patched
    down, because it is bound into the signature's default at import.
    """
    take(db, name, new_owner())
    lock = DbLock(name, timeout=4)
    started = time.monotonic()
    with pytest.raises(DbLockBusyError):
        lock.acquire()
    assert 3.5 <= time.monotonic() - started < 9


def test_timeout_is_honoured_at_the_deadline_not_the_poll_boundary(db, name):
    """min(POLL_SEC, left) is the whole of the timeout guarantee.

    3 s against a 5 s poll: stop_after_delay would return at 5.0 s and
    stop_before_delay at 0.0 s. Deliberately not a whole number of poll
    intervals, or this asserts nothing.
    """
    assert 3 % POLL_SEC != 0
    take(db, name, new_owner())
    started = time.monotonic()
    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=3)
    elapsed = time.monotonic() - started
    assert 2.5 <= elapsed < 4.5, f"returned at {elapsed:.1f}s, not at the 3s deadline"


# --- Context manager protocol (design §5.5) --------------------------------

def test_with_enters_and_releases(db, name):
    lock = DbLock(name, timeout=10)
    with lock:
        assert lock.is_held() is True
        assert _record(db, name) is not None
    assert lock.is_held() is False
    assert _record(db, name) is None


def test_with_raises_busy_carrying_the_holder(db, name):
    """A context manager has no other way to report that it did not
    enter, and returning without the lock would run the block unprotected."""
    other = new_owner()
    take(db, name, other)
    with pytest.raises(DbLockBusyError) as caught:
        with DbLock(name, timeout=3):
            pytest.fail("entered a held lock")
    assert caught.value.owner == other


def test_block_exception_still_releases(db, name):
    marker = RuntimeError("from the block")
    with pytest.raises(RuntimeError) as caught:
        with DbLock(name, timeout=10):
            raise marker
    assert caught.value is marker
    assert _record(db, name) is None


def test_failed_enter_leaves_no_record_and_no_local_lock(db, name):
    """A leaked local lock wedges every later caller for this name
    in the process, and is invisible to every other test."""
    other = new_owner()
    take(db, name, other)
    with pytest.raises(DbLockBusyError):
        with DbLock(name, timeout=2):
            pass
    assert _record(db, name).owner == other

    store.release(db, name, other)
    later = DbLock(name, timeout=5)
    later.acquire()
    later.release()


# --- Fencing token (design §4.1) -------------------------------------------

def test_fence_increases_across_grants(db, name):
    first = take(db, name, new_owner())
    owner = new_owner()
    store.release(db, name, _record(db, name).owner)
    second = take(db, name, owner)
    assert len(first) == len(second) == FENCE_LEN
    assert first < second


def test_fence_increases_across_a_delete(db, name):
    """The case with teeth, and the reason the token is a commit
    versionstamp rather than a counter.

    A counter read from the record is monotonic only while the record exists.
    release() deletes it, so the next acquire would restart from nothing and two
    unrelated grants of one name would present the same number — at which point
    a validator comparing ``>`` rejects the rightful holder and one comparing
    ``>=`` accepts a straggler.
    """
    lock = DbLock(name, timeout=5)
    lock.acquire()
    first = lock.fence
    lock.release()
    assert _record(db, name) is None

    lock.acquire()
    second = lock.fence
    lock.release()
    assert first < second


def test_fence_matches_the_record_and_is_empty_when_unheld(db, name):
    lock = DbLock(name, timeout=5)
    assert lock.fence == b""
    lock.acquire()
    _, stored = DbLockRecord.from_value(bytes(db[key(name)]))
    assert lock.fence == stored
    lock.release()
    assert lock.fence == b""


def test_refresh_preserves_the_fence(db, name):
    """A versionstamped write mints a new token every time it runs, so a
    refresh that re-stamped would move the fence on every heartbeat and fence
    the live holder out of its own section."""
    owner = new_owner()
    fence = take(db, name, owner)
    before = _record(db, name).heartbeat_at
    time.sleep(0.05)
    assert store.refresh(db, name, owner, lock_mod.LEASE_SEC) is True
    record, after_fence = DbLockRecord.from_value(bytes(db[key(name)]))
    assert after_fence == fence
    assert record.heartbeat_at > before


def test_fences_of_different_names_are_ordered(db, name):
    """The stamp is database-wide, not per name."""
    first = take(db, f"{name}/a", new_owner())
    second = take(db, f"{name}/b", new_owner())
    assert first < second


def test_reclaim_mints_a_greater_fence(db, name):
    """A reclaimed lease cannot present the token it displaced."""
    dead = new_owner()
    stale = take(db, name, dead)
    reclaimed = reclaim(db, name, new_owner())
    assert stale < reclaimed


def test_reading_a_stamped_key_poisons_the_transaction(db, name):
    """The read-before-stamp order in _acquire_tx is load bearing.

    A key mutated with ``set_versionstamped_value`` is unreadable for the rest
    of that transaction. Measured against 7.3.63: the read raises ``FDBError
    1036`` (``accessed_unreadable``) *and* the transaction is poisoned — the
    commit raises 1036 as well. So catching the read error does not rescue the
    transaction, and a "verify what we wrote" or debug read added inside the
    body would break every acquire at once however carefully it was wrapped.
    """
    import fdb

    record = DbLockRecord(name=name, owner=new_owner(), acquired_at=1.0, heartbeat_at=1.0)
    tr = db.create_transaction()
    tr.set_versionstamped_value(key(name), record.to_stamped_value())

    with pytest.raises(fdb.FDBError) as read_error:
        tr.get(key(name)).wait()
    assert read_error.value.code == 1036

    # Swallowing the read error is not a workaround: the commit fails too.
    with pytest.raises(fdb.FDBError) as commit_error:
        tr.commit().wait()
    assert commit_error.value.code == 1036
