# coding=utf-8
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
    """Clear the create-path TTL caches (leader / quorum-verdict /
    capacity-scan) around every test. They are module-level and keyed by ids
    tests reuse across cases ('node-1', 'LVS_1', ...), so a verdict cached in
    one test would leak into the next (e.g. a cached quorum verdict makes
    _check_peer_disconnected skip the probe a later test asserts on)."""
    try:
        from simplyblock_core.utils import ttl_cache
    except Exception:
        yield
        return
    caches = (ttl_cache.capacity_scan_cache, ttl_cache.leader_cache,
              ttl_cache.quorum_verdict_cache)
    for c in caches:
        c.invalidate()
    yield
    for c in caches:
        c.invalidate()


@pytest.fixture(autouse=True)
def _clear_singleton_cache():
    """Clear DBController Singleton cache before and after each test."""
    from simplyblock_core.db_controller import Singleton
    Singleton._instances.clear()
    yield
    Singleton._instances.clear()


@pytest.fixture(autouse=True)
def _clear_rpc_cache():
    """Clear RPC client cache before each test."""
    try:
        from simplyblock_core.rpc_client import _rpc_cache, _rpc_cache_lock
        with _rpc_cache_lock:
            _rpc_cache.clear()
    except ImportError:
        pass
    yield
    try:
        from simplyblock_core.rpc_client import _rpc_cache, _rpc_cache_lock
        with _rpc_cache_lock:
            _rpc_cache.clear()
    except ImportError:
        pass
