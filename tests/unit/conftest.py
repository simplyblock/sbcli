# coding=utf-8
"""Unit-tier fixtures.

Stub the native ``fdb`` module so unit tests can run without ``libfdb_c`` or
a live FoundationDB cluster. The stub returns ``None`` from ``fdb.open(...)``
which propagates to ``DBController.kv_store`` and lets controller code that
checks for a missing kv_store short-circuit cleanly.
"""

import sys
import types


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if 'fdb' not in sys.modules:
    class _FDBError(Exception):
        pass
    _stub('fdb', open=lambda *a, **kw: None, FDBError=_FDBError)
    _stub('fdb.tuple')


import pytest  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_ttl_caches():
    """The create-path TTL caches (leader / quorum-verdict / capacity-scan) are
    module-level and keyed by ids that unit tests reuse across cases
    ('node-1', 'LVS_1', ...), so a verdict cached in one test would silently
    leak into the next. Clear them around every test."""
    try:
        from simplyblock_core.utils import ttl_cache
    except Exception:
        yield
        return
    for cache in (ttl_cache.capacity_scan_cache, ttl_cache.leader_cache,
                  ttl_cache.quorum_verdict_cache):
        cache.invalidate()
    yield
    for cache in (ttl_cache.capacity_scan_cache, ttl_cache.leader_cache,
                  ttl_cache.quorum_verdict_cache):
        cache.invalidate()
