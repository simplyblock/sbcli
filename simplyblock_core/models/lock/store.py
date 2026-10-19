"""FoundationDB transactions behind :class:`~simplyblock_core.models.lock.DbLock`.

Five module-level functions over the ``name`` and ``ttl`` that the hand-rolled
locks in ``DBController`` hard-code to one caller. They are functions over a
handle rather than methods on ``DBController`` for two reasons, one of them
structural: ``DBController`` is a ``Singleton``, so a method on it can never be
pointed at a second database handle — which is exactly what a test needs in
order to fail a live holder's refresh against a real FoundationDB rather than a
patched accessor. And the controller already carries three locks' worth of
hand-rolled transactions; a fourth set would deepen the thing this package
exists to replace.

Being module-level also removes the ``fdb.transactional(DBController._x_tx)``
workaround those need: ``fdb.transactional`` injects the transaction as the
first positional argument, which collides with ``self``, so the unbound function
has to be wrapped and ``self`` passed back in by hand. A plain function has no
such collision and takes the decorator directly.
"""

import time
from typing import NamedTuple

import fdb

from simplyblock_core import constants, utils
from simplyblock_core.models.lock.errors import DbLockBusyError, DbLockUnavailableError
from simplyblock_core.models.lock.record import DbLockRecord, key

# ``fdb.transactional`` refuses to be applied before an API version has been
# selected, and the decorators below run at import. ``DBController.__init__``
# makes the same call, so whichever runs first wins: selecting a version twice
# is a no-op, and only a *different* version raises. Both name
# ``KVD_DB_VERSION``, so they cannot disagree.
fdb.api_version(constants.KVD_DB_VERSION)

logger = utils.get_logger(__name__)

# Per-transaction ceiling for this package, overriding the handle's own
# KVD_DB_TIMEOUT_MS (10 s). A lock transaction is a point read and a point
# write, so it has no business taking longer -- and at a heartbeat period of 10 s
# the inherited limit means a single hung refresh consumes an entire period and
# the retry inside the remaining lease never gets to run.
#
# Safe under fdb.transactional's retries: from API version 610 the timeout is
# not reset by on_error(), and the bindings document setting it at the start of
# each transaction as the supported way to express a per-transaction limit. Set
# on the transaction rather than the database so nothing outside this package
# inherits it.
LOCK_TX_TIMEOUT_MS = 2000

#: Grace on a heartbeat_at read from the future before it is called skew rather
#: than scheduling jitter. Well under EXPIRY_MARGIN_SEC, which is what a real
#: disagreement of this size is eating into.
SKEW_REPORT_SEC = 1.0


def _bound(tr):
    """Cap this transaction at the package's own timeout.

    Called first in every transactional body here, including the ones that only
    read: a lock transaction that cannot finish quickly has already lost its
    race, and waiting out the handle's ten seconds only delays the retry.
    """
    tr.options.set_timeout(LOCK_TX_TIMEOUT_MS)


class _Acquired(NamedTuple):
    """What ``_acquire_tx`` reports back to its wrapper.

    ``stamp`` resolves only once the wrapper has committed. The other two are
    observations the transaction made and must *not* log itself: the body is
    retried on conflict, and a log line inside it would be emitted once per
    attempt.
    """

    stamp: object
    #: Owner whose expired lease this acquire took over, if any.
    reclaimed: str | None
    #: Seconds by which the previous holder's heartbeat was ahead of this host's
    #: clock, if it was. Nonzero means the two disagree about the time, which is
    #: what EXPIRY_MARGIN_SEC is spending itself on.
    skew: float


@fdb.transactional
def _acquire_tx(tr, name, owner, lease, now):
    """Take ``name`` for ``owner``, or raise naming the live holder.

    The record is free when it is absent, when it carries this owner, or when
    its own ``expires_at`` has passed. That deadline is the one the *holder*
    wrote, not one re-derived here from this caller's ``lease`` — two control
    plane versions with different lease widths would otherwise disagree about
    when a record goes stale, and the shorter-lived one would reclaim a name
    from a holder still well inside the lease it promised itself. ``lease`` is
    only ever used to stamp the *new* record.

    Because the read and the write share one transaction, two processes racing
    to reclaim the same stale lock resolve to one winner.

    **The key is read before it is stamped, and nothing reads it after.** A key
    mutated by ``set_versionstamped_value`` is unreadable for the rest of the
    transaction, and the error does not surface at the read — it escapes at
    *commit*, as ``FDBError 1036`` (``accessed_unreadable``), so a ``try``/
    ``except`` around a later read would not contain it. The order is load
    bearing rather than stylistic: a "verify what we wrote" or debug read added
    inside this body breaks every acquire at once.

    See :class:`_Acquired` for what comes back and why two of the three are
    observations rather than log lines.
    """
    _bound(tr)
    lock_key = key(name)
    raw = tr.get(lock_key).wait()
    reclaimed = None
    skew = 0.0
    if raw.present():
        existing, _fence = DbLockRecord.from_value(bytes(raw))
        if existing.owner and existing.owner != owner and now <= existing.expires_at:
            raise DbLockBusyError(name, existing.owner)
        # A heartbeat ahead of this host's clock cannot have happened; the two
        # hosts disagree, and that disagreement is subtracted from the margin
        # protecting every lease in the cluster.
        skew = max(0.0, existing.heartbeat_at - now)
        if existing.owner == owner:
            acquired_at = existing.acquired_at
        else:
            acquired_at = now
            reclaimed = existing.owner or None
    else:
        acquired_at = now

    record = DbLockRecord(name=name, owner=owner, acquired_at=acquired_at,
                          heartbeat_at=now, expires_at=now + lease)
    tr.set_versionstamped_value(lock_key, record.to_stamped_value())
    return _Acquired(tr.get_versionstamp(), reclaimed, skew)


def acquire(db, name: str, owner: str, lease: float, now: float) -> bytes:
    """Take ``name`` for ``owner``. Returns the grant's 10-byte fencing token.

    Raises :class:`DbLockBusyError` naming the current holder, or
    :class:`DbLockUnavailableError` when the database gave no verdict. There is
    no ``(bool, ...)`` tuple whose second member changes type with the first and
    no string standing in for an error in the slot that otherwise carries an
    owner, which is what makes the success value un-ignorable.

    ``now`` is supplied by the caller rather than read here, and is the wall
    clock half of a pair whose monotonic half the caller keeps. Stamping the
    record from one reading and dating the holder's deadline from another taken
    after the commit would hand the holder that commit's latency as lease the
    record never granted. Taking it once also means a retried attempt measures
    staleness against the same instant as the first.
    """
    try:
        acquired = _acquire_tx(db, name, owner, lease, now)
        fence = bytes(acquired.stamp.wait())  # type: ignore[attr-defined]  # pending versionstamp future
    except DbLockBusyError:
        raise
    except fdb.FDBError as e:  # type: ignore[attr-defined]  # injected by fdb.api_version()
        raise DbLockUnavailableError(f"acquire {name!r} reached no verdict: {e}") from e
    if acquired.reclaimed is not None:
        logger.warning(
            "DbLock %r: reclaimed a stale lease from %r (its own expiry had passed); "
            "that holder is presumed dead", name, acquired.reclaimed)
    if acquired.skew > SKEW_REPORT_SEC:
        logger.warning(
            "DbLock %r: the previous holder's heartbeat is %.1fs ahead of this host's "
            "clock; the two disagree about the time, which is subtracted from the "
            "margin every lease in this cluster relies on", name, acquired.skew)
    return fence


@fdb.transactional
def _refresh_tx(tr, name, owner, lease, now):
    """Push the lease forward while preserving the already-minted token.

    Both timestamps move: ``expires_at`` is what a reclaimer judges, so a
    heartbeat that advanced only ``heartbeat_at`` would be a holder telling the
    cluster it is alive without buying itself any more time.

    The head of the value is rewritten byte for byte, so only a genuine change
    of hands moves the fence. A versionstamped write here would mint a new token
    on every heartbeat.
    """
    _bound(tr)
    lock_key = key(name)
    raw = tr.get(lock_key).wait()
    if not raw.present():
        return False
    existing, fence = DbLockRecord.from_value(bytes(raw))
    if existing.owner != owner:
        return False
    existing.heartbeat_at = now
    existing.expires_at = now + lease
    tr[lock_key] = existing.to_value(fence)
    return True


def refresh(db, name: str, owner: str, lease: float) -> bool:
    """Heartbeat ``name``'s lease. ``False`` iff it demonstrably changed hands.

    ``False`` means the record names another owner or no longer exists — an
    answer the database gave. A database that could not answer raises
    :class:`DbLockUnavailableError` instead, because a holder that cannot tell
    must retry inside its remaining lease where a holder that has been told must
    stop at once.
    """
    now = time.time()
    try:
        return _refresh_tx(db, name, owner, lease, now)
    except fdb.FDBError as e:  # type: ignore[attr-defined]  # injected by fdb.api_version()
        raise DbLockUnavailableError(f"refresh {name!r} reached no verdict: {e}") from e


@fdb.transactional
def _release_tx(tr, name, owner):
    _bound(tr)
    lock_key = key(name)
    raw = tr.get(lock_key).wait()
    if not raw.present():
        return
    existing, _fence = DbLockRecord.from_value(bytes(raw))
    if existing.owner == owner:
        del tr[lock_key]


def release(db, name: str, owner: str) -> None:
    """Delete ``name``'s record, only while ``owner`` still holds it.

    Owner scoped, so a late release never deletes a lock another owner has since
    reclaimed.
    """
    try:
        _release_tx(db, name, owner)
    except fdb.FDBError as e:  # type: ignore[attr-defined]  # injected by fdb.api_version()
        raise DbLockUnavailableError(f"release {name!r} reached no verdict: {e}") from e


def watch(db, name: str):
    """An ``fdb`` watch future that fires on any write to ``name``'s key.

    Deliberately not an ``@fdb.transactional``: that decorator retries the body
    on a conflict, and each discarded attempt would leave its watch armed
    against FoundationDB's per-database watch limit. A watch is created once,
    committed once, and cancelled by its waiter.

    The future is valid only after the creating transaction commits, and it
    outlives that transaction's own timeout. Raises ``fdb.FDBError``, which the
    wait loop degrades to polling rather than failing the acquire.
    """
    tr = db.create_transaction()
    watch_future = tr.watch(key(name))
    tr.commit().wait()
    return watch_future


@fdb.transactional
def _get_tx(tr, name):
    _bound(tr)
    raw = tr.get(key(name)).wait()
    return DbLockRecord.from_value(bytes(raw)) if raw.present() else None


def get(db, name: str):
    """``(record, fence)`` for ``name``, or ``None``. Read only, for reporting.

    Returns the fence alongside the record because the stored value carries
    both, and an operator listing wants both.
    """
    try:
        return _get_tx(db, name)
    except fdb.FDBError as e:  # type: ignore[attr-defined]  # injected by fdb.api_version()
        raise DbLockUnavailableError(f"get {name!r} reached no verdict: {e}") from e
