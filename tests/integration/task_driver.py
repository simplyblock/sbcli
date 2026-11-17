"""Driving a task runner's ``RunnerSpec`` the way ``serve()`` does, against the
real FoundationDB.

``drive`` is one pass of the driver over one task: eligibility, the lease claim,
the cancel / retry-ceiling checks, the move to RUNNING, the handler and the
outcome commit (``TaskRunner._process``). It does not go through ``_dispatch``,
so the in-flight, exclusion, concurrency and backoff gates are not exercised, and
a new driver per call means repeated calls model no backoff timing.

``one_cycle`` is one cycle of the driver's own loop (``TaskRunner.run``): every
cluster's tasks the spec selects, then its sleep, which ends the cycle.
"""
import dataclasses
import time
import types
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import task_runner_base as trb

db = trb.db


class _StopLoop(BaseException):
    """Raised by the driver's sleep: the cycle is over."""


def drive(spec, task, cluster=None, handler=None) -> JobSchedule:
    """One driver pass over ``task``; the task as stored afterwards.

    ``task`` and ``cluster`` are re-read from the database, the cluster from the
    task's ``cluster_id`` unless given. ``handler`` replaces the spec's handler
    and nothing else, to see whether the driver reaches it.
    """
    fresh = db.get_task_by_id(task.uuid)
    assert fresh.function_name in spec.function_names, (fresh.function_name, spec.name)
    if cluster is None:
        cluster = db.get_cluster_by_id(fresh.cluster_id)
    if handler is not None:
        spec = dataclasses.replace(spec, handler=handler)
    runner = trb.TaskRunner(spec)
    try:
        runner._process(fresh, cluster, [])
    finally:
        runner._executor.shutdown(wait=True)
    return db.get_task_by_id(task.uuid)


def handler_reached(spec, task, cluster=None) -> bool:
    """Whether one driver pass over ``task`` reaches the spec's handler.

    The handler is replaced by a recorder, so a task that reaches it ends DONE.
    A task that does not is asserted untouched: not claimed, nothing written.
    """
    before = db.get_task_by_id(task.uuid)
    handler = MagicMock()
    after = drive(spec, task, cluster, handler=handler)
    if not handler.called:
        assert _row(after) == _row(before)
    return handler.called


def _row(task):
    return (task.status, task.function_result, task.retry, task.canceled, task.owner, task.updated_at)


def one_cycle(spec):
    """Run one cycle of ``serve(spec)``."""
    runner = trb.TaskRunner(spec)
    clock = types.SimpleNamespace(sleep=MagicMock(side_effect=_StopLoop), time=time.time)
    with patch.object(trb, "time", clock):
        try:
            with pytest.raises(_StopLoop):
                runner.run()
        finally:
            runner._executor.shutdown(wait=True)
