"""Unit tier for ``simplyblock_core.models.lock``.

What holds with no database in play at all: the owner string, the relations
between the lease constants, the self-expiry deadline arithmetic, constructor
validation and the error surface. Everything that asserts acquire, reclaim or
watch semantics
is an integration test — ``tests/AGENTS.md`` admits only two positions, no
database or the real one, and a fake transaction object is the third it rules
out.
"""

import re
import threading

import pytest

from simplyblock_core.models.lock import (
    DEFAULT_WAIT_TIMEOUT_SEC,
    EXPIRY_MARGIN_SEC,
    HEARTBEAT_PERIOD_SEC,
    LEASE_PERIODS,
    LEASE_SEC,
    REFRESH_RETRY_SEC,
    DbLock,
    DbLockBusyError,
    DbLockLostError,
    DbLockUnavailableError,
    expiry_deadline,
    new_owner,
)
from simplyblock_core.models.lock.record import (
    EMPTY_FENCE,
    FENCE_LEN,
    DbLockRecord,
    key,
)

# A kv_store that is not None keeps the constructor off the DBController
# singleton. No test here reaches it: every case fails before any store call.
_NO_DB = object()


def _lock(name="domain/id", **kwargs):
    kwargs.setdefault("kv_store", _NO_DB)
    return DbLock(name, **kwargs)


# --- Owner identity and construction (design §5.1, §7) ---------------------

def test_owner_string_shape():
    """Hostname, PID, thread ident and an 8-hex nonce."""
    owner = new_owner()
    assert re.fullmatch(r".+-\d+-\d+-[0-9a-f]{8}", owner), owner
    assert str(threading.get_ident()) in owner.split("-")


def test_owner_is_unique_per_grant():
    """Two locks for one name in one thread mint different owners.

    The nonce is what keeps an owner-scoped release from freeing a later
    section that shares a host, a process and a thread identity.
    """
    assert new_owner() != new_owner()


def test_lease_is_a_multiple_of_the_refresh_period():
    """The lease says how many heartbeats a holder may miss, and nothing else.

    Asserted as the relation rather than against the literal 30, so moving
    either constant moves the lease instead of leaving this asserting the old
    arithmetic.
    """
    assert LEASE_SEC == HEARTBEAT_PERIOD_SEC * LEASE_PERIODS
    assert LEASE_PERIODS >= 2, "a lease that tolerates no missed heartbeat is a timeout"


def test_a_refresh_is_reachable_inside_the_stand_down_deadline():
    """The margin must leave room for at least one scheduled attempt.

    A holder stands down EXPIRY_MARGIN_SEC before the deadline it wrote; if
    that margin swallowed a whole period, the first refresh would be scheduled
    past the point the holder has already given up.
    """
    assert EXPIRY_MARGIN_SEC < HEARTBEAT_PERIOD_SEC
    assert expiry_deadline(0.0) > HEARTBEAT_PERIOD_SEC


def test_a_failed_refresh_retries_faster_than_it_is_scheduled():
    """The retry gap is a floor between attempts, not a second schedule.

    A retry period at or above the heartbeat period would make a failed
    refresh cost more than simply waiting for the next scheduled one.
    """
    assert REFRESH_RETRY_SEC < HEARTBEAT_PERIOD_SEC


def test_lease_width_is_not_a_constructor_argument():
    """The lease is the mechanism's, not the caller's.

    How long a *dead* holder blocks a name is the same trade for every lock;
    a live holder's section runs as long as it runs. There is nothing here a
    caller knows better, so there is nothing to pass — and a caller that tries
    is told, rather than silently getting the module default.
    """
    with pytest.raises((TypeError, ValueError)):
        _lock(ttl=60)


def test_heartbeat_interval_argument_is_refused():
    """No caller can reintroduce a period wider than the lease.

    Asserts the rejection rather than the exception type: a plain signature
    raises TypeError and the ``@validate_call`` form raises ValidationError,
    and this should survive either choice.
    """
    with pytest.raises((TypeError, ValueError)):
        _lock(heartbeat_interval=5)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_timeout_is_rejected(value):
    """``nan`` compares false against every deadline, which would turn a
    bounded wait into an unbounded one."""
    with pytest.raises(ValueError):
        _lock(timeout=value)


def test_empty_name_is_rejected():
    """Every lock with an empty name is the same lock."""
    with pytest.raises(ValueError):
        _lock("")


@pytest.mark.parametrize("timeout", [-1, -60])
def test_negative_constructor_timeout_is_rejected(timeout):
    with pytest.raises(ValueError):
        _lock(timeout=timeout)


def test_zero_constructor_timeout_is_admitted_as_a_probe():
    """The only spelling of a single-shot ``with``.

    ``__enter__`` takes no timeout of its own, so a zero budget is what makes
    one attempt reachable through the context manager — and with it
    ``__exit__``'s lease-loss check.
    """
    assert _lock(timeout=0)._timeout == 0.0


@pytest.mark.parametrize("method", ["acquire", "try_acquire"])
def test_acquire_takes_no_blocking_argument(method):
    """Neither form takes a ``blocking`` argument — a zero timeout is it.

    Asserts the rejection rather than the exception type, as the
    ``heartbeat_interval`` case does: a plain signature raises TypeError and
    the ``@validate_call`` form raises ValidationError, and this should
    survive either choice.
    """
    with pytest.raises((TypeError, ValueError)):
        getattr(_lock(), method)(blocking=False)


@pytest.mark.parametrize("method", ["acquire", "try_acquire"])
def test_both_acquire_forms_validate_the_timeout_alike(method):
    """try_acquire differs from acquire in its reporting, nothing else.

    A bound enforced on one and not the other would be a second timeout
    contract to keep in step with the first.
    """
    with pytest.raises(ValueError):
        getattr(_lock(), method)(timeout=-1)
    with pytest.raises(ValueError):
        getattr(_lock(), method)(timeout=True)
    with pytest.raises(ValueError):
        getattr(_lock(), method)(timeout=float("nan"))


@pytest.mark.parametrize("value", [30, 30.0])
def test_int_and_float_are_both_accepted_for_a_duration(value):
    """A caller writes ``timeout=30``, not ``timeout=30.0``.

    ``int`` -> ``float`` is the one widening the validator still permits under
    ``strict=True``; it is lossless, and the bounds apply to it unchanged.
    """
    assert _lock(timeout=value)._timeout == 30.0


@pytest.mark.parametrize("value", [True, False])
def test_a_bool_duration_is_rejected(value):
    """``bool`` is an ``int`` subclass, so a lax validator would take ``True``
    as one second — a ceiling arrived at from what is almost certainly a
    mistyped flag. ``strict=True`` is the only thing that refuses it."""
    with pytest.raises(ValueError):
        _lock(timeout=value)
    with pytest.raises(ValueError):
        _lock().acquire(timeout=value)


@pytest.mark.parametrize("value", ["30", "abc"])
def test_a_string_duration_is_rejected(value):
    """No coercion from strings: a duration read out of config or argv is
    converted by its caller, where the failure names the source."""
    with pytest.raises(ValueError):
        _lock(timeout=value)


def test_bounds_apply_to_int_input_too():
    """The widening does not smuggle a value past its constraint."""
    with pytest.raises(ValueError):
        _lock(timeout=-1)
    with pytest.raises(ValueError):
        _lock().acquire(timeout=-1)


def test_defaulted_timeout_is_the_constant():
    """The ceiling a bare acquire() and an __enter__ on one lock both honour."""
    assert _lock()._timeout == float(DEFAULT_WAIT_TIMEOUT_SEC)
    assert _lock(timeout=12)._timeout == 12.0


# --- Lease deadline arithmetic (design §5.3, §7) ---------------------------

def test_expiry_deadline_formula():
    """last_success + LEASE_SEC - EXPIRY_MARGIN_SEC."""
    assert expiry_deadline(1000.0) == 1000.0 + LEASE_SEC - EXPIRY_MARGIN_SEC


def test_expiry_deadline_moves_only_on_success():
    """Measured from the last *successful* refresh, not the last attempt.

    A failed attempt proves nothing about how much lease is left, so a deadline
    that advanced on attempts would extend a lease nothing has renewed.
    """
    first = expiry_deadline(1000.0)
    assert expiry_deadline(1000.0) == first
    assert expiry_deadline(1015.0) == first + 15.0


def test_the_holder_stands_down_before_the_record_expires():
    """The self-expiry deadline sits inside the lease the record was stamped
    with, by exactly the margin covering skew against the reclaiming host."""
    assert expiry_deadline(0.0) == LEASE_SEC - EXPIRY_MARGIN_SEC
    assert expiry_deadline(0.0) < LEASE_SEC


def test_refresh_wait_is_capped_by_the_deadline():
    """The heartbeat sleeps min(period, deadline - now).

    Modelled on the loop rather than run through it: what it asserts is that
    no attempt is ever scheduled past the deadline, at any point in a lease.
    """
    deadline = expiry_deadline(0.0)
    now = 0.0
    attempts = []
    while now < deadline:
        now += min(HEARTBEAT_PERIOD_SEC, deadline - now)
        attempts.append(now)
    assert all(t <= deadline for t in attempts)
    assert attempts[0] == HEARTBEAT_PERIOD_SEC
    # Every attempt but the last lands on a period boundary; the last is the
    # deadline itself, which the loop clamps to rather than overshooting.
    assert attempts[-1] == deadline


def test_a_whole_period_of_scheduled_attempts_is_reachable():
    """At least LEASE_PERIODS - 1 refreshes are scheduled inside the deadline.

    One refresh short of the lease width: the margin eats into the last one.
    This is what "a holder must miss this many heartbeats" buys, and it is the
    reason LEASE_PERIODS is not 1.
    """
    scheduled = int(expiry_deadline(0.0) // HEARTBEAT_PERIOD_SEC)
    assert scheduled >= LEASE_PERIODS - 1


# --- Error surface (design §5.3, §5.4, §5.5) -------------------------------

def test_release_unheld_lock_raises_runtime_error():
    """Matches threading.Lock, and before any database call."""
    with pytest.raises(RuntimeError):
        _lock().release()


def test_busy_error_carries_name_and_owner():
    err = DbLockBusyError("domain/id", "host-1-2-abcd1234")
    assert (err.name, err.owner) == ("domain/id", "host-1-2-abcd1234")
    assert "domain/id" in str(err)
    assert "host-1-2-abcd1234" in str(err)


def test_busy_error_formats_an_unknown_holder():
    """The local tier can refuse before anyone recorded an owner."""
    assert "None" in str(DbLockBusyError("domain/id", None))


def test_lost_error_carries_its_fields():
    err = DbLockLostError("domain/id", "owner-1", 3.25)
    assert (err.name, err.owner, err.lost_for) == ("domain/id", "owner-1", 3.25)
    assert "domain/id" in str(err)
    assert "owner-1" in str(err)
    assert "3.2" in str(err)


def test_unavailable_is_not_a_busy_error():
    """No handler catches both by accident.

    They mean opposite things to a holder — one says stop, the other says try
    again — so a single ``except`` covering both is the bug this prevents.
    """
    assert not issubclass(DbLockUnavailableError, DbLockBusyError)
    assert not issubclass(DbLockBusyError, DbLockUnavailableError)


def test_is_held_and_fence_on_an_unheld_lock():
    """No grant means no lease and no token, without touching the database."""
    lock = _lock()
    assert lock.is_held() is False
    assert lock.fence == b""


# --- Record codec (design §4.1) --------------------------------------------

def test_key_is_the_lock_prefix_plus_the_whole_name():
    """The name is the tail, so a domain is a prefix range and point reads
    stay exact: "a/1" and "a/10" cannot collide."""
    assert key("cluster_add/abc") == b"lock/cluster_add/abc"
    assert not key("a/10").startswith(key("a/1") + b"/")


def test_stamped_value_layout():
    """Placeholder, JSON body, then the little-endian offset FDB strips."""
    record = DbLockRecord(name="a/b", owner="o", acquired_at=1.0, heartbeat_at=2.0)
    raw = record.to_stamped_value()
    assert raw[:FENCE_LEN] == EMPTY_FENCE
    assert raw[-4:] == b"\x00\x00\x00\x00"
    assert b'"owner": "o"' in raw


def test_round_trip_preserves_the_fence():
    """A refresh rewrites the body and leaves the head alone, so only a genuine
    change of hands moves the token."""
    fence = bytes(range(FENCE_LEN))
    record = DbLockRecord(name="a/b", owner="o", acquired_at=1.0, heartbeat_at=2.0,
                          expires_at=32.0)
    back, read_fence = DbLockRecord.from_value(record.to_value(fence))
    assert back == record
    assert back.expires_at == 32.0
    assert read_fence == fence


def test_from_value_tolerates_unknown_and_missing_fields():
    """A record written by another version of the control plane still loads.

    That tolerance is the one property of BaseModel worth carrying over;
    ``cls(**json.loads(...))`` would raise on both.
    """
    raw = EMPTY_FENCE + b'{"name": "a/b", "future_field": 7}'
    record, fence = DbLockRecord.from_value(raw)
    assert record == DbLockRecord(name="a/b", owner="", acquired_at=0.0, heartbeat_at=0.0,
                                  expires_at=0.0)
    assert fence == EMPTY_FENCE


def test_a_record_without_an_expiry_is_never_fresh():
    """The default must read as expired, not as a lease of unknown width.

    ``expires_at`` is what every reclaim decision is made against, so a record
    that somehow lacks it has to fall on the side that frees the name. A
    default that defaulted *forward* — heartbeat_at plus some assumed lease —
    would wedge a name on a malformed record instead.
    """
    record, _fence = DbLockRecord.from_value(EMPTY_FENCE + b'{"name": "a/b"}')
    assert record.expires_at == 0.0
