"""The process-local tier (design §5.1).

The local tier is a prerequisite with the *same* semantics as the distributed
one, not a fast path with its own: same waiting behaviour, one shared budget,
and the same ``DbLockBusyError`` on failure, so a caller never has to know
which tier it lost to. These are the only tests in the suite that deliberately
put two ``DbLock`` objects for one name in one process.

A leaked local lock wedges every later caller for that name for the life of the
process, and is invisible to every other test — which is what the two
survives-a-failed-acquire cases below exist for.
"""

import threading
import time

import pytest

from simplyblock_core.models.lock import DbLock, DbLockBusyError, new_owner, store
from tests.integration.db_lock.conftest import take


@pytest.mark.timeout(90)  # 4 threads x 3 acquires, each a real FDB round trip.
def test_two_threads_hold_it_one_at_a_time(name):
    """Exactly one holder at a time, under genuine parallelism.

    The counter is incremented on entry and decremented on exit; a second
    simultaneous holder shows up as a peak above one. Run under the ``py314t-``
    twin this is a GIL-off assertion rather than a formality.
    """
    live = 0
    peak = 0
    guard = threading.Lock()
    entered = []
    start = threading.Barrier(4)

    def worker():
        nonlocal live, peak
        lock = DbLock(name, timeout=25)
        start.wait()
        for _ in range(3):
            with lock:
                with guard:
                    live += 1
                    peak = max(peak, live)
                time.sleep(0.01)
                with guard:
                    live -= 1
                entered.append(threading.get_ident())

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not any(thread.is_alive() for thread in threads)

    assert peak == 1, f"{peak} simultaneous holders"
    assert len(entered) == 12
    assert len(set(entered)) == 4, "a thread was starved"


def test_local_loser_issues_no_fdb_call(name, monkeypatch):
    """Collapsing in-process contention keeps the FDB round trips
    proportional to contending *processes*, not threads.

    A spy that delegates, not a mock: the real transaction still runs.
    """
    calls = []
    real = store.acquire

    def spy(db, lock_name, owner, lease, now):
        calls.append(owner)
        return real(db, lock_name, owner, lease, now)

    monkeypatch.setattr(store, "acquire", spy)

    holder = DbLock(name, timeout=10)
    holder.acquire()
    try:
        assert len(calls) == 1
        with pytest.raises(DbLockBusyError):
            DbLock(name).acquire(timeout=0)
        assert len(calls) == 1, "the local loser still went to the database"
        with pytest.raises(DbLockBusyError):
            DbLock(name).acquire(timeout=1)
        assert len(calls) == 1
    finally:
        holder.release()


def test_local_lock_survives_a_timed_out_acquire(db, name):
    """A later acquire for that name in this process still succeeds."""
    other = new_owner()
    take(db, name, other)
    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=2)
    store.release(db, name, other)

    later = DbLock(name, timeout=10)
    later.acquire()
    later.release()


def test_local_lock_survives_a_failed_zero_timeout_acquire(db, name):
    """The single-attempt path releases the local lock too."""
    other = new_owner()
    take(db, name, other)
    with pytest.raises(DbLockBusyError):
        DbLock(name).acquire(timeout=0)
    store.release(db, name, other)

    later = DbLock(name, timeout=10)
    later.acquire(timeout=0)
    later.release()


def test_local_loser_waits_and_acquires_on_release(name):
    """Waiting is waiting in both tiers.

    Fails an implementation that treats a lost local lock as an immediate
    :class:`DbLockBusyError` — a plausible reading of "the winner holds the
    lock on the whole process's behalf", and one that would make two threads of
    one process behave unlike two processes for no reason a caller can see.
    """
    holder = DbLock(name, timeout=10)
    holder.acquire()
    threading.Timer(0.5, holder.release).start()

    waiter = DbLock(name, timeout=20)
    started = time.monotonic()
    waiter.acquire()
    elapsed = time.monotonic() - started
    waiter.release()
    assert 0.3 <= elapsed < 10, f"acquired after {elapsed:.2f}s"


def test_timeout_is_one_budget_across_both_tiers(name):
    """Stops the local tier's own waiting from turning one budget into two.

    A lock given ``timeout=4`` must not wait four seconds locally and then four
    more on FoundationDB.
    """
    holder = DbLock(name, timeout=30)
    holder.acquire()
    try:
        started = time.monotonic()
        with pytest.raises(DbLockBusyError):
            DbLock(name).acquire(timeout=4)
        elapsed = time.monotonic() - started
        assert 3.5 <= elapsed < 7, f"spent {elapsed:.1f}s on a 4s budget"
    finally:
        holder.release()


def test_enter_losing_the_local_tier_names_the_in_process_holder(name):
    """The in-process holder is the one thing the local tier knows that
    the record does not."""
    holder = DbLock(name, timeout=10)
    holder.acquire()
    try:
        with pytest.raises(DbLockBusyError) as caught:
            with DbLock(name, timeout=1):
                pytest.fail("entered a lock held by this process")
        assert caught.value.owner is not None
        assert str(threading.get_ident()) in caught.value.owner.split("-")
    finally:
        holder.release()
