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
    # ``transactional`` mirrors the real decorator's calling convention: the
    # wrapped function's first argument is the database/transaction. Unit
    # tests pass a MagicMock kv_store, so running the body directly against
    # it keeps existing ``kv_store.set(...)`` assertions working while
    # watched-model writes land their counter ``add(...)`` on the same mock.
    _stub('fdb', open=lambda *a, **kw: None, FDBError=_FDBError,
          transactional=lambda f: f)
    _stub('fdb.tuple')
