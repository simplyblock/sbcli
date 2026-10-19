"""The lock package's error surface.

Its own module so that :mod:`~simplyblock_core.models.lock.store` and the
package's ``__init__`` can both raise these without importing each other.

**Outcomes return, errors raise.** Every ``store`` function draws the line in
the same place: a verdict the database actually gave is a return value, and a
verdict it failed to give is :class:`DbLockUnavailableError`. The existing
hand-rolled locks do not draw it at all — ``refresh_cluster_add_lock`` maps a
missing connection onto the same ``False`` as an owner mismatch — and a holder
cannot act correctly on a signal that conflates the two, because one means stop
and the other means try again.
"""


class DbLockUnavailableError(Exception):
    """The database reached no verdict.

    Raised by every ``store`` function that could not complete its transaction.
    Distinct from ``store.refresh``'s ``False`` return and from
    :class:`DbLockBusyError`, both of which are answers: a holder that cannot
    tell retries inside its remaining lease and expires itself if that runs out,
    where a holder that has been told stops at once. Out of ``store.acquire`` it
    is a failed *attempt* — the wait loop re-attempts until the caller's
    deadline, and only then does it reach the caller.
    """


class DbLockLostError(Exception):
    """Raised by ``DbLock.__exit__`` when the lease ended during the block.

    The critical section ran on without the exclusion it asked for, either
    because another owner reclaimed the record or because this holder could not
    reach FoundationDB for long enough to expire its own lease. Not raised when
    the block is already propagating an exception: that one is more useful, and
    is often the same outage seen from closer up.
    """

    def __init__(self, name: str, owner: str, lost_for: float):
        self.name = name
        self.owner = owner
        self.lost_for = lost_for
        super().__init__(
            f"DbLock lost: {name!r} held by {owner!r}, lease gone for {lost_for:.1f}s")


class DbLockBusyError(Exception):
    """A live owner holds the name.

    Raised by ``store.acquire`` for every failed attempt, and propagated out of
    ``DbLock.acquire`` — and so out of ``__enter__`` — when the wait loop gives
    up at its deadline. ``DbLock.try_acquire`` is the one caller that catches
    it, which is what that form is for; ``acquire`` does not, because a boolean
    would make entering the critical section unprotected the easiest thing to
    write, and a context manager has no other way to report that it did not
    enter. It carries the current holder so a caller can report who it lost to;
    that holder is a remote owner string, or this process's own thread when the
    local tier is what refused.
    """

    def __init__(self, name: str, owner: str | None):
        self.name = name
        self.owner = owner
        super().__init__(f"DbLock busy: {name!r} held by {owner!r}")
