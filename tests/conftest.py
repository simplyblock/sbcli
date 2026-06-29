# coding=utf-8
"""
conftest.py – shared fixtures for all tests in tests/.

The unit-tier fdb stub lives in ``tests/unit/conftest.py`` (and
``simplyblock_core/test/conftest.py``) rather than here, so the integration
tier can connect to a real testcontainer-provided FoundationDB without a
stale stub shadowing it.
"""

import pytest


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
