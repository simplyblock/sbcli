# coding=utf-8
"""Unit-tier conftest: stub out native/unavailable dependencies before any
simplyblock import so unit tests can run without a FoundationDB client.

This stub stays scoped to ``tests/unit/`` so it does not leak into the
integration tier, which needs the real ``foundationdb`` Python client to
talk to the testcontainers-managed FDB.
"""

import sys
import types


def _stub(name, **attrs):
    """Create a minimal stub module and register it in sys.modules."""
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
