# coding=utf-8
"""Stub the native ``fdb`` module so the unit-tier tests under this directory
can run without ``libfdb_c`` or a live FoundationDB cluster.

Mirrors ``tests/unit/conftest.py`` because pytest discovers conftests per
directory and these tests don't share a parent directory with ``tests/unit/``.
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
