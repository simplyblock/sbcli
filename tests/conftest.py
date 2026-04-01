# coding=utf-8
"""
conftest.py – shared fixtures for all tests in tests/.
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


@pytest.fixture(autouse=True)
def _debug_dbcontroller_identity(request):
    """Debug: log the identity of DBController to diagnose mock patch issues."""
    import simplyblock_core.controllers.lvol_controller as lc
    import sys
    mod_in_sys = sys.modules.get("simplyblock_core.controllers.lvol_controller")
    if "AddHostToLvol" in request.node.nodeid or "RemoveHost" in request.node.nodeid:
        print(f"\n[DEBUG] test={request.node.nodeid}")
        print(f"[DEBUG] lvol_controller module id: {id(lc)}")
        print(f"[DEBUG] sys.modules module id: {id(mod_in_sys)}")
        print(f"[DEBUG] same? {lc is mod_in_sys}")
        print(f"[DEBUG] DBController: {lc.DBController}")
        print(f"[DEBUG] DBController id: {id(lc.DBController)}")
    yield
