"""Fixtures for the ``DbLock`` suite, on top of the tier's shared FoundationDB.

The one fixture that is not boilerplate is :func:`broken_db`. Making a live
holder's refresh fail is the only way to reach the heartbeat's loss handling,
and ``tests/AGENTS.md`` § integration forbids the obvious route — patching
``DBController`` or assigning a mock ``kv_store``. Because every ``store``
function takes its handle as an argument, a test can hand one lock a *second,
real* ``fdb.Database`` opened on a cluster nothing answers on: the refresh then
fails the way an outage makes it fail, an ``FDBError`` out of a real
transaction, with nothing mocked.
"""

import time

import fdb
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models import lock
from simplyblock_core.models.lock import store

#: Lease width for the tests that shrink it, via the ``short_lease`` fixture.
#: Not a constructor argument any more — the lease is the mechanism's, so a
#: test that wants a narrow one patches the constant rather than passing one,
#: which also keeps the production relation (lease = periods x period) intact
#: at whatever scale the test runs.
FAST_PERIOD_SEC = 0.5

#: A wider shrink for the fault-injection tests. Those measure the *retry*
#: cadence of a holder whose refreshes fail, and every such attempt costs a
#: whole (rescaled) LOCK_TX_TIMEOUT_MS before it even reports — so at
#: FAST_PERIOD_SEC a lease would fit one or two attempts and could not tell
#: retrying at REFRESH_RETRY_SEC from retrying at the period, which is the
#: whole assertion.
FAULT_PERIOD_SEC = 2.0


def take(db, name, owner, lease=None, now=None):
    """``store.acquire`` with the two readings a live caller supplies itself.

    The lease width and the wall clock are arguments of the store rather than
    of the lock, so every test that drives the store directly would otherwise
    repeat both. Defaults are what production passes.
    """
    return store.acquire(db, name, owner,
                         lock.LEASE_SEC if lease is None else lease,
                         time.time() if now is None else now)


def reclaim(db, name, owner):
    """Take ``name`` from whoever holds it, as a host would once its lease ran out.

    Spends no wall clock: the acquirer supplies ``now``, so an attempt dated
    past the holder's ``expires_at`` exercises exactly the predicate a patient
    test would wait for. What it cannot do is skip a *live* lease — that is the
    predicate, not a flag — so this still fails against a holder that is
    heartbeating, which is what makes it safe to use as a reclaim.
    """
    return take(db, name, owner, now=time.time() + lock.LEASE_SEC * 100)


@pytest.fixture
def db():
    return DBController().kv_store


def _rescale(monkeypatch, period):
    """Run the lock at ``period``, holding every ratio production has.

    The lease is not a constructor argument any more — it is the mechanism's,
    not the caller's — so a test that needs a narrow one moves the constants.
    Moving them *together* is what keeps the test honest: scaling the period
    alone would leave a margin or a retry gap wider than the lease it sits in,
    which is a configuration production cannot be in.

    The margin scales here for a reason it does not in production. There it is
    absolute because it covers clock skew between two hosts, which does not
    care how long anyone holds a lock; here both hosts are this process and it
    is standing in for a round trip to a local FoundationDB.
    """
    scale = period / lock.HEARTBEAT_PERIOD_SEC
    # The store's own transaction timeout scales too, and it is not optional.
    # It is what bounds one *failed* refresh against the unreachable handle, so
    # a test that shrank the lease but left it at the production two seconds
    # would fit fewer attempts into the smaller budget, not more -- which is the
    # opposite of what shrinking is for. It has to be moved here rather than on
    # the handle: a timeout set on the transaction supersedes the database
    # option, so `broken_db` cannot bound these from its side at all.
    monkeypatch.setattr(store, "LOCK_TX_TIMEOUT_MS",
                        max(1, int(store.LOCK_TX_TIMEOUT_MS * scale)))
    lease = period * lock.LEASE_PERIODS
    margin = lock.EXPIRY_MARGIN_SEC * scale
    monkeypatch.setattr(lock, "HEARTBEAT_PERIOD_SEC", period)
    monkeypatch.setattr(lock, "LEASE_SEC", lease)
    monkeypatch.setattr(lock, "EXPIRY_MARGIN_SEC", margin)
    monkeypatch.setattr(lock, "REFRESH_RETRY_SEC", lock.REFRESH_RETRY_SEC * scale)
    return period, lease, lease - margin


@pytest.fixture
def short_lease(monkeypatch):
    """A sub-second lease, for tests that watch one elapse.

    Returns the patched (period, lease, stand-down deadline).
    """
    return _rescale(monkeypatch, FAST_PERIOD_SEC)


@pytest.fixture
def fault_lease(monkeypatch):
    """A few-second lease, for tests that watch refreshes *fail*.

    Wider than :func:`short_lease` by the margin the transaction timeout
    forces: see FAULT_PERIOD_SEC.
    """
    return _rescale(monkeypatch, FAULT_PERIOD_SEC)


@pytest.fixture
def name(request):
    """A name no other test has used.

    Local-tier entries are never evicted, so a name reused across tests would
    carry one test's leaked ``threading.Lock`` into the next.
    """
    return f"test/{request.node.name}"


@pytest.fixture(scope="session")
def broken_db(tmp_path_factory):
    """A real ``fdb.Database`` pointed at a cluster nothing answers on.

    Carries no transaction timeout of its own. Every ``store`` body sets one on
    its transaction, and a per-transaction timeout supersedes the database
    option — so a handle-level setting here would be read by nothing. How long
    a doomed refresh takes to report is ``store.LOCK_TX_TIMEOUT_MS``, which
    :func:`_rescale` is what moves.
    """
    cluster_file = tmp_path_factory.mktemp("broken-fdb") / "fdb.cluster"
    # description:id@host:port — syntactically valid, deliberately unreachable.
    cluster_file.write_text("broken:broken@127.0.0.1:65431\n")
    return fdb.open(str(cluster_file))
