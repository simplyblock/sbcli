"""A ``threading.Lock``-like mutex synchronized through FoundationDB.

A caller names the resource it protects, and every other caller using that name
waits — whether it runs on another thread of the same process, in the CLI, in
the web API, or in a background service::

    with DbLock("cluster_add/<cluster-id>"):
        ...

Not this::

    lock = DbLock("cluster_add/<cluster-id>")
    try:
        lock.acquire()
        ...
    finally:
        lock.release()

``release()`` raises ``RuntimeError`` if the lock is not held, so when
``acquire()`` itself is the call that raises — the ordinary case, a busy
lock — the ``finally`` block above raises a second, unrelated error over the
first. ``__exit__`` also does more than call ``release()``: it raises
:class:`DbLockLostError` if the lease died mid-block, which a hand-written
``finally`` does not check. Use ``with``; reach for :meth:`acquire` /
:meth:`release` directly only when the hold must outlive one lexical block.

Two tiers carry the exclusion. A process-local ``threading.Lock``, one per name,
resolves contention between threads of one process without a FoundationDB round
trip; the thread that wins it holds the distributed lock on behalf of the whole
process, as a single record guarded by a transaction (``lock/store.py``). That
record is a lease rather than a permanent claim: every ``HEARTBEAT_PERIOD_SEC``
its holder pushes the record's ``expires_at`` further out, and a record whose
own ``expires_at`` has passed counts as free — so a holder that dies without
releasing blocks the name for at most ``LEASE_SEC``, and one that is merely
slow keeps it indefinitely.

Neither number is a constructor argument, which is the one thing to know before
reaching for this. **The lease does not describe the critical section.** How
long a section runs is bounded by nothing here; what the lease bounds is how
long a *dead* holder's name stays taken, and that trade — reclaim a crashed
holder sooner, against preempting a live but slow one — comes out the same for
every lock, because every holder heartbeats identically. A caller has no
information the mechanism lacks, so there is nothing to pass.

What a caller *does* decide is ``timeout``: how long to wait for someone else
before giving up.

The lease is policed from both ends. A holder that cannot confirm its own
heartbeat gives the lock up ``EXPIRY_MARGIN_SEC`` before the deadline it wrote,
rather than waiting to be told it was reclaimed by a database it may no longer
reach.

**This is not the tool for a short section.** Mutual exclusion over a read and
a write that could sit in one FoundationDB transaction belongs in that
transaction, which is serializable, needs no lease, no heartbeat and no clock,
and cannot leave a lock behind when its holder dies. What is left for this lock
is the section a transaction cannot express — minutes of configuration pushed
out to storage nodes over JSON-RPC, say — where the work is not FDB writes and
so cannot be made atomic with them.

Waiting is event driven: a waiter opens an FDB watch on the key and blocks on a
``threading.Event`` the watch's ``on_ready`` callback sets, so a release wakes it
within roughly one round trip instead of at the end of a polling interval. That
callback runs on the ``fdb`` client's own network thread, so waiting costs no
thread of its own.

The lease constants live here rather than in :mod:`simplyblock_core.constants`:
nothing outside this package reads them, and the relations between them (the
lease as a multiple of the refresh period, the margin below that period, the
join budget above the store's transaction timeout) are only checkable beside the
code that enforces them. They carry no ``DB_LOCK_`` prefix, because inside a
module already named ``lock`` it would only stutter.
"""

import os
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Annotated, ClassVar, NamedTuple, Self

import fdb
from pydantic import ConfigDict, Field, validate_call

from simplyblock_core import utils
from simplyblock_core.models.lock import store
from simplyblock_core.models.lock.errors import (
    DbLockBusyError,
    DbLockLostError,
    DbLockUnavailableError,
)
from simplyblock_core.models.lock.record import DbLockRecord

__all__ = [
    'DbLock',
    'DbLockBusyError',
    'DbLockLostError',
    'DbLockRecord',
    'DbLockUnavailableError',
]

logger = utils.get_logger(__name__)

# How often a holder checks in. Fixed rather than derived from the lease,
# because the two answer different questions — how often a live holder talks to
# the database, versus how long a dead one blocks its name — and nothing is
# served by forcing one number to answer both.
HEARTBEAT_PERIOD_SEC = 10
# Lease width, as the number of consecutive heartbeats a holder must miss before
# another host may reclaim its name. Not settable: the trade it expresses
# (reclaim a crashed holder sooner, versus preempt a live but slow one) is the
# same for every lock, because every holder heartbeats the same way. A caller
# has no information the mechanism lacks, which is why the parameter that used
# to be here had no correct value for anyone to pass.
LEASE_PERIODS = 3
LEASE_SEC = HEARTBEAT_PERIOD_SEC * LEASE_PERIODS
# Ceiling on an acquire given no explicit timeout. Well under
# constants.CLUSTER_ADD_LOCK_WAIT_TIMEOUT_SEC: that path waits out queued mesh
# sections, which is a property of add_node rather than of a lock, and a caller
# needing it passes timeout= explicitly.
DEFAULT_WAIT_TIMEOUT_SEC = 300
# Failsafe ceiling on one event wait, so a watch that never fires costs a
# re-attempt rather than the caller's whole timeout.
POLL_SEC = 5
# Re-attempt period after a refresh that reached no verdict, spent inside the
# remaining lease rather than at the next heartbeat tick. A floor on the gap
# between attempts, not a promise about the rate: an attempt that fails by not
# answering has already spent KVD_DB_TIMEOUT_MS before this gap begins.
REFRESH_RETRY_SEC = 2
# Subtracted from the lease when a holder expires itself, covering clock skew
# against the reclaiming host plus that host's own round trip. The one constant
# here with a correctness meaning rather than a tuning one, and an absolute
# rather than a fraction of the lease: skew between two hosts does not scale
# with how long either of them intends to hold a lock.
EXPIRY_MARGIN_SEC = 5
# Added to LOCK_TX_TIMEOUT_MS for release()'s bounded heartbeat-thread join, so a
# refresh that times out at the store's own limit still has room to return
# before the join gives up. The join is what stops an in-flight refresh from
# rewriting the key release() is about to delete.
JOIN_GRACE_SEC = 2

_JOIN_TIMEOUT_SEC = store.LOCK_TX_TIMEOUT_MS / 1000 + JOIN_GRACE_SEC


def new_owner() -> str:
    """Owner id for one grant: host, pid, thread, nonce.

    The nonce is what keeps an owner-scoped release from freeing a later section
    that happens to share a host, a process and a thread identity.
    """
    return f"{socket.gethostname()}-{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex[:8]}"


def expiry_deadline(last_success: float) -> float:
    """When a holder gives the lease up having heard nothing from the database.

    ``last_success`` is a :func:`time.monotonic` reading of the last *successful*
    refresh, not of the last attempt — an attempt that failed proves nothing
    about how much lease is left. Monotonic rather than wall clock because it is
    compared only against this process's own later readings, where an NTP step
    would silently lengthen or end the lease; the record's ``heartbeat_at`` is
    wall clock precisely because the reclaiming host compares it against *its*
    clock, and ``EXPIRY_MARGIN_SEC`` is what covers the skew between the
    two.
    """
    return last_success + LEASE_SEC - EXPIRY_MARGIN_SEC


@dataclass
class _LocalLock:
    """One name's process-local tier: the mutex, and who in this process holds it.

    The registry is on the class because the guard has two duties that only make
    sense together — the map's get-or-create, and each instance's ``owner``.

    Deliberately not a context manager. The success path *keeps* the mutex: it
    is taken in :meth:`DbLock.acquire` and released in :meth:`DbLock.release`,
    a span that outlives any block here and may end on another thread. Only the
    failure paths give it back, which is what the caller's ``except`` covers.
    """

    _guard: ClassVar[threading.Lock] = threading.Lock()
    #: One entry per name for the life of the process, never evicted: evicting
    #: means deleting a ``threading.Lock`` a thread may be about to wait on, and
    #: the guard that would make that safe is the entry itself. The cost is
    #: bounded by the number of *distinct names* the process has ever locked,
    #: which is a constraint on naming (``"<domain>/<resource-id>"``) rather
    #: than a case for eviction.
    _locks: ClassVar[dict[str, '_LocalLock']] = {}

    _mutex: threading.Lock = field(default_factory=threading.Lock)
    #: The in-process holder, for reporting only. Written under the guard, read
    #: without it: the read feeds a ``DbLockBusyError`` message and nothing
    #: else, so observing the window between a winner taking the mutex and
    #: naming itself costs a less informative message, never a wrong decision.
    owner: str | None = None

    @classmethod
    def for_name(cls, name: str) -> '_LocalLock':
        """This process's lock for ``name``, created once and never evicted.

        Get-or-create is under the guard because two threads racing a name's
        first use would otherwise build one instance each, and two mutexes for
        one name admit two holders.
        """
        with cls._guard:
            local = cls._locks.get(name)
            if local is None:
                local = cls._locks[name] = cls()
            return local

    def take(self, *, timeout: float) -> bool:
        """Win the name in this process, or report that a sibling thread holds it.

        Waiting is waiting here as well: a caller that loses waits on the mutex
        exactly as it would wait on a remote holder, bounded by what is left of
        the one shared deadline. A budget already spent is one try, which is
        what makes a zero timeout mean "do not wait" in this tier too.
        """
        return self._mutex.acquire(timeout=max(0.0, timeout))

    def claim(self, owner: str) -> None:
        """Record which grant holds this name, once the mutex is won."""
        with self._guard:
            self.owner = owner

    def free(self) -> None:
        """Give the name back: clear the owner, then release.

        In that order, so the next winner cannot observe its predecessor's
        owner string.
        """
        with self._guard:
            self.owner = None
        self._mutex.release()


class _Grant(NamedTuple):
    """What a successful acquire returns: the token, and when it was taken.

    ``acquired_at`` is monotonic and read *before* the transaction that granted
    the lease, pairing with the wall clock the record was stamped with. Carrying
    it out rather than re-reading the clock at the call site is the whole point:
    two readings taken either side of a commit disagree by its latency, and the
    holder is the party that would benefit from the disagreement.
    """

    fence: bytes
    acquired_at: float


@dataclass
class _Hold:
    """Everything that describes one grant, minted by the successful acquire.

    The hold rather than the ``DbLock`` is the unit of state, and both reasons
    are silent corruption if it is skipped. A ``DbLock`` is reusable and
    shareable — the shape it imitates is a module-level ``LOCK = DbLock(...)``
    entered by every thread that needs it — so per-instance grant state turns
    the second hold into a partial overwrite of the first. And a released hold's
    heartbeat thread does not stop instantly: it can be inside a ``store.refresh``
    that will not return for up to the handle's transaction timeout, and with
    grant state on the instance that thread's verdict would land on whatever
    hold is current when it finally returns.
    """

    name: str
    owner: str
    fence: bytes
    acquired_at: float
    #: Monotonic instant this hold stops believing it holds the lease, advanced
    #: by every successful refresh. Written by the heartbeat thread and read by
    #: the caller's, without a lock: a float store is atomic in both the GIL and
    #: the free-threaded build, and a stale read can only be the *earlier*
    #: deadline, which makes is_lost() pessimistic rather than optimistic.
    deadline: float = 0.0
    stop: threading.Event = field(default_factory=threading.Event)
    lost: threading.Event = field(default_factory=threading.Event)
    lost_at: float = 0.0
    thread: threading.Thread | None = None

    def is_lost(self) -> bool:
        """Whether the lease has ended, by verdict or by the clock.

        Two ways to lose it and only one of them is an event: a reclaim the
        database reported, or a deadline that simply passed. The second is why
        this is computed rather than read off ``lost`` — a holder whose
        heartbeat thread is parked inside a refresh that will not return has
        passed its deadline with nothing left running to notice.
        """
        return self.lost.is_set() or time.monotonic() >= self.deadline

    def give_up(self) -> None:
        self.lost_at = time.monotonic()
        self.lost.set()

    def lost_since(self) -> float:
        """Monotonic instant the lease ended, for reporting.

        Falls back to the deadline: a hold lost by the clock never ran
        ``give_up``, so ``lost_at`` is still zero and would otherwise report a
        loss duration measured from the epoch.
        """
        return self.lost_at or self.deadline


def _heartbeat(db, hold: _Hold) -> None:
    """Refresh ``hold``'s lease until it is released, reclaimed, or given up.

    The whole of this function is what it does when a refresh does *not*
    succeed, because an intermittent database failure is the likeliest way this
    primitive ends up with two live owners. The shape it replaces returns on the
    first falsy refresh, and the accessor it calls returns that same falsy value
    for a missing connection — so one transient blip there does not risk expiry,
    it guarantees it one TTL later with the holder still running.

    No exception leaves this thread. An unhandled one kills a daemon thread
    silently, which stops the heartbeat with no record anywhere and guarantees
    the expiry the thread exists to prevent.
    """
    # Seeded from the hold rather than from a fresh reading: the thread starts
    # after the acquire returns, so re-reading the clock here would hand the
    # lease back the commit latency the hold's deadline was anchored to exclude.
    last_success = hold.acquired_at
    gap = float(HEARTBEAT_PERIOD_SEC)
    while True:
        deadline = hold.deadline
        now = time.monotonic()
        if now >= deadline:
            break
        # Bounded by the deadline as well as the period, so a refresh is never
        # attempted past it: a success there would extend a lease this holder
        # has already written off.
        if hold.stop.wait(min(gap, deadline - now)):
            return
        if time.monotonic() >= deadline:
            break

        try:
            still_held = store.refresh(db, hold.name, hold.owner, LEASE_SEC)
        except DbLockUnavailableError:
            logger.warning(
                "DbLock %r: refresh by %r reached no verdict; retrying inside the "
                "remaining lease (expires in %.1fs)",
                hold.name, hold.owner, deadline - time.monotonic(), exc_info=True)
            gap = REFRESH_RETRY_SEC
            continue
        except Exception:
            logger.exception(
                "DbLock %r: unexpected error refreshing %r; treating the lease as lost",
                hold.name, hold.owner)
            hold.give_up()
            return

        if not still_held:
            logger.error(
                "DbLock %r: lease held by %r was reclaimed by another owner; the "
                "critical section is now running unprotected", hold.name, hold.owner)
            hold.give_up()
            return

        last_success = time.monotonic()
        hold.deadline = expiry_deadline(last_success)
        gap = float(HEARTBEAT_PERIOD_SEC)

    # Self-expiry is terminal: the record may have changed hands and the fence
    # may have moved, so a refresh succeeding later on a record this owner no
    # longer legitimately holds would resurrect a lease the rest of the cluster
    # has already written off. Recovering means acquiring again, which is the
    # caller's decision rather than this thread's.
    logger.error(
        "DbLock %r: holder %r expired its own lease after %.1fs without a successful "
        "refresh; standing down before the record expires",
        hold.name, hold.owner, time.monotonic() - last_success)
    hold.give_up()


class DbLock:
    """A ``threading.Lock``-like mutex synchronized through FoundationDB.

    Two tiers: a process-local ``threading.Lock`` per name gates the FDB round
    trip, so threads of one process contend in process first and the winner
    holds the distributed lock on the whole process's behalf. The local tier is
    a prerequisite with the same semantics as the distributed one — same
    waiting behaviour, same single deadline, same :class:`DbLockBusyError` on
    failure — so a caller never has to know which tier it lost to.

    Not a drop-in for ``threading.Lock``. :meth:`acquire` is bounded (by
    ``DEFAULT_WAIT_TIMEOUT_SEC`` when the lock names no timeout) and reports
    failure by raising rather than by a boolean a caller can discard, so it
    and ``__enter__`` differ only in that the second one also releases;
    :meth:`try_acquire` is the boolean form, for a caller that answers
    contention by doing something else rather than by failing. Neither takes
    a ``blocking`` argument; a zero timeout is how they spell it. And
    exclusion holds only while the holder's heartbeat is live — the lease can
    end under a holder that loses its database, which is what :meth:`is_held`
    reports and what ``__exit__`` raises :class:`DbLockLostError` for.
    Non-reentrant.

    One instance may be shared by many threads and reused for many successive
    holds, which is how a module-level ``LOCK = DbLock("domain/id")`` is
    normally used. Per-grant state (owner, fence, heartbeat, lost flag) lives on
    a private per-acquire object rather than on the instance, so a hold cannot
    be corrupted by the next one or by the previous one's heartbeat thread.
    """

    @validate_call(config=ConfigDict(strict=True))
    def __init__(
        self,
        name: Annotated[str, Field(min_length=1)],
        *,
        timeout: Annotated[float | None, Field(ge=0, allow_inf_nan=False)] = None,
        kv_store=None,
    ):
        """Raises ``ValueError`` for a timeout that is not a duration.

        Neither the lease width nor the refresh period is an argument. Both are
        module constants, because neither asks the caller a question the caller
        can answer: the lease governs how long a *dead* holder blocks this name,
        which is a property of the mechanism, and a live holder's section may
        run as long as it likes under a heartbeat that keeps extending it.

        ``timeout`` is the opposite, and is why it stays: how long *this* caller
        is willing to wait for someone else is exactly the caller's own
        business, and the two call sites answer it differently.

        ``timeout=0`` is a probe: one attempt in each tier, against a
        deadline already spent. It is how this lock spells "do not wait", and
        because ``__enter__`` has no timeout of its own it is also the only
        way to get a single-shot acquire through the context manager, and with
        it ``__exit__``'s check that the lease survived the block.

        ``kv_store`` defaults to ``DBController().kv_store``. It is an argument
        at all so a test can point one lock at a second, deliberately broken
        FoundationDB handle — something a method on the ``DBController``
        singleton cannot express.
        """
        self._name = name
        self._timeout = (float(DEFAULT_WAIT_TIMEOUT_SEC)
                         if timeout is None else float(timeout))
        self._kv_store = kv_store
        self._hold: _Hold | None = None

    @property
    def name(self) -> str:
        return self._name

    def _db(self):
        if self._kv_store is not None:
            return self._kv_store
        from simplyblock_core.db_controller import DBController
        kv_store = DBController().kv_store
        if kv_store is None:
            raise DbLockUnavailableError(f"DbLock {self._name!r}: no database connection")
        return kv_store

    @validate_call(config=ConfigDict(strict=True))
    def acquire(
        self,
        timeout: Annotated[float | None, Field(ge=0, allow_inf_nan=False)] = None,
    ) -> None:
        """Take the lock, or raise.

        :class:`DbLockBusyError` naming the holder when the name stays taken
        for the whole budget, :class:`DbLockUnavailableError` when the database
        never gave a verdict. There is no boolean to forget to check: unlike
        ``threading.Lock.acquire()`` this call is bounded, so a return value
        would make silently entering an unprotected critical section the
        easiest thing to write. A caller that wants contention as a value
        rather than an exception calls :meth:`try_acquire`.

        There is no ``blocking`` flag either — ``timeout=0`` is what "do not
        wait" means here, and it still makes one attempt in each tier.

        ``timeout`` defaults to the lock's own, so a bare ``acquire()`` and an
        ``__enter__`` on the same lock wait exactly as long as each other. It
        is a single deadline across both tiers: the local acquire is bounded by
        what remains of it and the distributed retry loop by what remains after
        that. Two tiers are not two budgets — a caller that asked to wait ten
        seconds waits ten seconds in total, whatever mixture of threads and
        processes it lost to.
        """
        db = self._db()
        deadline = time.monotonic() + (self._timeout if timeout is None else timeout)
        local = _LocalLock.for_name(self._name)
        owner = new_owner()

        if not local.take(timeout=deadline - time.monotonic()):
            # Losing the local tier is losing: the caller's situation is
            # identical to a live remote holder, so it reports identically.
            raise DbLockBusyError(self._name, local.owner)

        try:
            local.claim(owner)
            grant = self._wait_for_grant(db, owner, deadline)
            try:
                hold = _Hold(name=self._name, owner=owner, fence=grant.fence,
                             acquired_at=grant.acquired_at,
                             deadline=expiry_deadline(grant.acquired_at))
                hold.thread = threading.Thread(
                    target=_heartbeat, args=(db, hold), daemon=True,
                    name=f"db-lock-heartbeat-{self._name}")
                hold.thread.start()
            except BaseException:
                # A grant whose heartbeat never started is a lease nobody will
                # renew — worse than a failed acquire, because it blocks the
                # name for a full TTL while its holder believes nothing.
                try:
                    store.release(db, self._name, owner)
                except DbLockUnavailableError:
                    logger.warning(
                        "DbLock %r: could not hand back a grant whose heartbeat "
                        "failed to start; it expires at its TTL", self._name,
                        exc_info=True)
                raise
        except BaseException:
            local.free()
            raise

        self._hold = hold

    @validate_call(config=ConfigDict(strict=True))
    def try_acquire(
        self,
        timeout: Annotated[float | None, Field(ge=0, allow_inf_nan=False)] = None,
    ) -> bool:
        """:meth:`acquire`, with contention as ``False`` instead of an exception.

        For a caller whose answer to a live holder is to do something else —
        skip this cycle, take the best-effort path — rather than to fail. The
        waiting is identical, ``timeout`` and all; only the reporting differs,
        so ``try_acquire(timeout=0)`` is a probe and a bare ``try_acquire()``
        waits out the lock's ceiling before answering.

        :class:`DbLockUnavailableError` still propagates. It is the one thing
        this must not flatten: a database that gave no verdict is not a name
        someone else holds, and a caller that reads "someone else has it" off
        an outage takes its contention path for a lock that may well be free.
        """
        try:
            self.acquire(timeout=timeout)
        except DbLockBusyError:
            return False
        return True

    def _wait_for_grant(self, db, owner: str, deadline: float) -> '_Grant':
        """Attempt, then wait on a watch-fed event, until the deadline.

        The attempt precedes the deadline test, so one always happens — which
        is what makes an already-spent budget (``timeout=0``, or one the local
        tier consumed) a probe rather than a no-op.

        ``min(POLL_SEC, left)`` is the whole of the timeout guarantee:
        capping the pause at what remains is what makes the loop return *at* the
        deadline rather than at the next poll boundary after it, and testing
        ``left`` before opening a watch is what stops the last pass from opening
        one it will never wait on.
        """
        event = threading.Event()
        holder: str | None
        reason: Exception
        while True:
            # One pair of readings per attempt, taken *before* the transaction
            # and shared with it. The record's expiry and this holder's deadline
            # then measure from the same instant, so the commit latency between
            # them falls inside the lease rather than being pocketed as lease
            # the record never granted.
            now, acquired_at = time.time(), time.monotonic()
            try:
                fence = store.acquire(db, self._name, owner, LEASE_SEC, now)
                return _Grant(fence, acquired_at)
            except DbLockBusyError as busy:
                holder, reason = busy.owner, busy
            except DbLockUnavailableError as unavailable:
                # No verdict is a failed attempt, not a failed call: a database
                # unreachable now may answer before the caller's budget runs out.
                holder, reason = None, unavailable

            left = deadline - time.monotonic()
            if left <= 0:
                logger.info("DbLock %r: gave up waiting at its deadline; held by %r",
                            self._name, holder)
                raise reason

            event.clear()
            watch = None
            try:
                watch = store.watch(db, self._name)
                # Runs on the FDB network thread: must only set the event.
                watch.on_ready(lambda _f: event.set())
            except fdb.FDBError:  # type: ignore[attr-defined]  # injected by fdb.api_version()
                logger.warning(
                    "DbLock %r: watch unavailable, this pass falls back to polling",
                    self._name, exc_info=True)
            try:
                logger.info("DbLock %r held by %r; waiting", self._name, holder)
                event.wait(min(POLL_SEC, left))
            finally:
                if watch is not None:
                    # Cancelling also fires the callback (FDBError 1101), so the
                    # event is always set eventually and no pass can leave a
                    # watch outstanding against the per-database limit.
                    watch.cancel()

    def release(self) -> None:
        """Release the lock. Raises ``RuntimeError`` if it is not held.

        Stops the heartbeat and waits for its thread before deleting the record,
        so an in-flight refresh cannot rewrite the key this is about to delete
        and resurrect a released lease. That wait is bounded by the handle's
        transaction timeout plus ``JOIN_GRACE_SEC``, which is what makes
        the abandonment below safe as well as bounded: a refresh still running
        then has already blown its own transaction timeout and cannot commit.

        Releasing a lease that was already lost is not an error: the
        owner-scoped release leaves the new owner's record alone, and the local
        tier is freed regardless — a holder that declines to give up the local
        tier because it lost the distributed one wedges the process for that
        name.
        """
        hold = self._hold
        if hold is None:
            raise RuntimeError("release unlocked lock")
        self._hold = None

        hold.stop.set()
        if hold.thread is not None:
            hold.thread.join(timeout=_JOIN_TIMEOUT_SEC)
            if hold.thread.is_alive():
                logger.warning(
                    "DbLock %r: heartbeat thread for %r did not exit within %.1fs; "
                    "abandoning it — a release that hangs inside a finally is worse "
                    "than a leaked daemon thread", self._name, hold.owner, _JOIN_TIMEOUT_SEC)

        try:
            store.release(self._db(), self._name, hold.owner)
        except DbLockUnavailableError:
            logger.warning(
                "DbLock %r: release by %r reached no verdict; the record expires "
                "at its lease instead", self._name, hold.owner, exc_info=True)
        finally:
            _LocalLock.for_name(self._name).free()

    def is_held(self) -> bool:
        """Whether the lease is still believed held.

        ``False`` once the heartbeat has seen a reclaim, or once the lease has
        simply run out. A critical section longer than a few seconds checks this
        between its own steps, because nothing here can preempt it. Answerable
        without a lock: one attribute load and a clock read, so a hold that has
        been replaced cannot be observed half-swapped.
        """
        hold = self._hold
        return hold is not None and not hold.is_lost()

    @property
    def fence(self) -> bytes:
        """Fencing token of the current lease, or ``b""`` when not held.

        FoundationDB's commit versionstamp for the transaction that granted this
        lease: 10 bytes, unique and increasing across the whole database, so two
        grants of one name never present the same token even across a release
        that deleted the record. Compare with ``<``; the encoding is fixed-width
        and big-endian, so byte order is value order. Nothing here validates it
        — that happens wherever the protected write happens.
        """
        hold = self._hold
        return hold.fence if hold is not None else b''

    def __enter__(self) -> Self:
        """Acquire within the constructor's ``timeout``, or raise.

        Exactly :meth:`acquire` with no argument — the context manager adds the
        release and ``__exit__``'s check that the lease survived the block, and
        nothing else.
        """
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Release, including when the wrapped block raises.

        Raises :class:`DbLockLostError` if the lease ended during a block that
        returned normally — a section that ran to completion under a lease it no
        longer held did not do what its caller asked, and returning normally
        would report a success that did not happen. If the block is already
        raising, the loss is logged and that exception propagates untouched: the
        original failure is the more useful one, and is quite often the same
        outage seen from closer up.
        """
        hold = self._hold
        lost = hold is not None and hold.is_lost()
        lost_for = time.monotonic() - hold.lost_since() if lost and hold is not None else 0.0
        self.release()
        if not lost or hold is None:
            return
        if exc_type is not None:
            logger.error(
                "DbLock %r: lease held by %r was lost %.1fs before the block failed; "
                "reporting the block's own exception", self._name, hold.owner, lost_for)
            return
        raise DbLockLostError(self._name, hold.owner, lost_for)
