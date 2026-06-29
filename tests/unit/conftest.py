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
