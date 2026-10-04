"""
conftest.py – shared fixtures for all tests in tests/.

The unit-tier fdb stub lives in ``tests/unit/conftest.py`` (and
``simplyblock_core/test/conftest.py``) rather than here, so the integration
tier can connect to a real testcontainer-provided FoundationDB without a
stale stub shadowing it.
"""

import pathlib

import pytest


def pytest_configure(config):
    """Fail fast if a test module is dropped directly under ``tests/``.

    Every test must live in a tier — ``tests/unit/`` (pure logic, fdb stubbed)
    or ``tests/integration/`` (real FoundationDB). A ``test_*.py`` sitting at the
    top level belongs to neither, so it silently escapes the tier split (it is
    not selected by either tox env and gets no tier-specific conftest). This
    guard runs for both ``tox -e unit`` and ``tox -e integration`` because this
    conftest is a parent of both, and turns the mistake into a hard collection
    error instead of a quietly-skipped test.
    """
    tests_dir = pathlib.Path(__file__).parent
    strays = sorted(p.name for p in tests_dir.glob("test_*.py"))
    if strays:
        raise pytest.UsageError(
            "Test modules must live in a tier, not directly under tests/. "
            "Move these into tests/unit/ (pure logic, fdb stubbed) or "
            "tests/integration/ (real FDB) — see tests/AGENTS.md § Tiers:\n  "
            + "\n  ".join(f"tests/{name}" for name in strays)
        )


@pytest.fixture(autouse=True)
def _clear_ttl_caches():
    """Clear every TTL cache around each test.

    They are module-level and keyed by ids tests reuse across cases ('node-1',
    'LVS_1', ...), so a verdict cached in one test leaks into the next (e.g. a
    cached quorum verdict makes _check_peer_disconnected skip the probe a later
    test asserts on).

    ``invalidate_all()`` rather than a list of caches: the list drifted twice
    and left ``no_leader_cache`` and ``storage_node_monitor._status_probe_cache``
    uncleared, which failed 11 of 12 tests in a class that all passed
    individually. Importing the caches' own modules is not required — the
    registry finds any cache that has been constructed."""
    try:
        from simplyblock_core.utils import ttl_cache
    except Exception:
        yield
        return
    ttl_cache.invalidate_all()
    yield
    ttl_cache.invalidate_all()


@pytest.fixture(autouse=True)
def _clear_singleton_cache():
    """Clear DBController Singleton cache before and after each test."""
    from simplyblock_core.db_controller import Singleton
    Singleton._instances.clear()
    yield
    Singleton._instances.clear()


@pytest.fixture(autouse=True)
def _clear_rpc_session_pool():
    """Clear RPCClient's pooled-Session cache before and after each test.

    Fixtures across the suite build clients against the same identity
    (host/port/user) with the default retry, so without this a test can
    silently reuse a mocked Session built by an *earlier* test instead of
    the one it just requested.
    """
    from simplyblock_core.rpc_client import _session_pool
    _session_pool.clear()
    yield
    _session_pool.clear()


@pytest.fixture(autouse=True)
def _no_leaked_port_block_window():
    """Fail (and un-wedge) a test that leaves the port-block window gate held.

    ``storage_node_ops._port_block_window_gate`` is a module-level mutex over
    the client-port-block span, acquired with no timeout. A test that leaves it
    held poisons the rest of the session: every later test entering a window
    blocks on the acquire until pytest-timeout kills it, so one leak surfaces as
    a scattering of unrelated 30s timeouts (observed: 9 of them, in files that
    pass in under a second on their own).

    Release it so the next test starts clean, and fail loudly rather than
    paper over it: a leak means a code path between ``_open_port_block_window``
    and ``_close_port_block_window`` raised without releasing, which is a
    PRODUCTION defect, not a test one. The gate releases are plain statements
    rather than a ``finally``, so any raise inside the window escapes holding
    it — and in a long-lived service process that wedges every later restart
    or recreate forever, since the acquire has no timeout.
    """
    yield
    try:
        from simplyblock_core import storage_node_ops
    except Exception:
        return
    gate = storage_node_ops._port_block_window_gate
    if gate.locked():
        gate.release()
        storage_node_ops._window_clear.set()
        pytest.fail(
            "storage_node_ops._port_block_window_gate was still held after this "
            "test — the code path it exercised opened a port-block window and "
            "raised without closing it. This is a production lock leak: the "
            "gate is process-global and acquired with no timeout, so in a live "
            "service every subsequent restart/recreate would block forever. "
            "The gate has been released so later tests are unaffected.")


