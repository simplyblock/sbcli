"""Stub the native ``fdb`` module so the unit-tier tests under this directory
can run without ``libfdb_c`` or a live FoundationDB cluster.

Mirrors ``tests/unit/conftest.py`` because pytest discovers conftests per
directory and these tests don't share a parent directory with ``tests/unit/``.
"""

import sys
import types

import pytest


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if 'fdb' not in sys.modules:
    class _FDBError(Exception):
        pass
    # ``transactional`` mirrors the real decorator's calling convention, as in
    # tests/unit/conftest.py: whichever of the two stubs installs first serves
    # the whole session, so they must stay equivalent or a combined run breaks
    # on watched-model writes.
    _stub('fdb', open=lambda *a, **kw: None, FDBError=_FDBError,
          transactional=lambda f: f)
    _stub('fdb.tuple')


@pytest.fixture(autouse=True)
def write_through_cas(monkeypatch):
    """Back the handler-facing CAS helpers with the task row a test holds.

    ``checkpoint`` / ``set_result`` / ``drop_params`` commit through
    ``atomic_update`` and hand back a fresh frozen view, so a handler's writes
    never land on the object it was given. With no store behind them they go
    nowhere at all and every assertion on the task passes vacuously.

    A test drives a handler the way the driver does — by passing
    ``task.frozen_view()`` — and reads the result off its own row; taking the
    view is what registers that row here. Mirrors the fixture in
    ``tests/unit/tasks/test_runner_specs.py``; pytest discovers conftests per
    directory and these tests don't share a parent with ``tests/unit/``.
    """
    from simplyblock_core.models.job_schedule import JobSchedule
    import simplyblock_core.services.task_runner_base as trb

    rows: dict = {}
    take_view = JobSchedule.frozen_view

    def frozen_view(self):
        rows.setdefault(self.get_id(), self)
        return take_view(self)

    def atomic_update(obj, mutate):
        row = rows.setdefault(obj.get_id(), obj)
        frozen = row.__dict__.get('_frozen', False)
        row.__dict__['_frozen'] = False
        if frozen:
            row.function_params = dict(row.function_params)
        try:
            if mutate(row) is False:
                return None
        finally:
            row.__dict__['_frozen'] = frozen
        return row

    monkeypatch.setattr(JobSchedule, "frozen_view", frozen_view)
    monkeypatch.setattr(trb.db, "atomic_update", atomic_update)
