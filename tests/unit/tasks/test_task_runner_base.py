# coding=utf-8
"""Unit tests for the shared task-runner driver (``task_runner_base``).

These exercise the per-task lifecycle (``_process``) and the dispatch loop
(``run``) directly, with ``db`` and ``claim_task`` mocked. They pin the handler
contract: return → DONE, TaskDefer → suspend without a retry, TaskRetry /
unexpected Exception → suspend + retry + backoff, TaskAbort → DONE; the pre-run
skip-gates (eligibility, lease); and that a handler exception never escapes the
loop while a DB error does.
"""
import time
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.job_schedule import JobSchedule
import simplyblock_core.services.task_runner_base as trb


@pytest.fixture(autouse=True)
def _no_task_writes(monkeypatch):
    """Task writes would dereference a None kv_store; the driver's state
    transitions are asserted on the in-memory task object instead."""
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())


def _task(status=JobSchedule.STATUS_NEW, retry=0, max_retry=8, canceled=False):
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = "fn"
    task.cluster_id = "cl-1"
    task.node_id = "node-1"
    task.status = status
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = canceled
    task.function_params = {}
    return task


def _wire(monkeypatch, task, claim=True):
    """Mock the module ``db`` (re-fetch returns the same task), the lease, and
    the lease heartbeat (a no-op context manager so no real thread is spawned)."""
    db = MagicMock()
    db.get_task_by_id.return_value = task
    monkeypatch.setattr(trb, "db", db)
    monkeypatch.setattr(trb.tasks_controller, "claim_task", lambda *a, **k: claim)
    monkeypatch.setattr(trb.tasks_controller, "task_lease_heartbeat", MagicMock())
    return db


def _runner(handler, **spec_kw):
    spec = trb.RunnerSpec(function_names=("fn",), handler=handler, **spec_kw)
    return trb.TaskRunner(spec)


# -- handler outcome vocabulary --------------------------------------------

def test_void_return_marks_done(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)
    _runner(handler)._process(task, MagicMock())
    handler.assert_called_once_with(task)
    assert task.status == JobSchedule.STATUS_DONE


def test_handler_runs_under_lease_heartbeat(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    hb = MagicMock()
    monkeypatch.setattr(trb.tasks_controller, "task_lease_heartbeat", hb)
    _runner(MagicMock(return_value=None))._process(task, MagicMock())
    hb.assert_called_once_with(task)
    hb.return_value.__enter__.assert_called_once()
    hb.return_value.__exit__.assert_called_once()


def test_defer_suspends_without_consuming_retry(monkeypatch):
    task = _task(retry=2)
    _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskDefer("node not online")

    runner = _runner(handler)
    runner._process(task, MagicMock())
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 2
    assert task.function_result == "node not online"
    assert "task-1" not in runner._next_attempt  # no backoff for a defer


def test_retry_suspends_consumes_retry_and_backs_off(monkeypatch):
    task = _task(retry=1)
    _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskRetry("rpc failed")

    runner = _runner(handler)
    runner._process(task, MagicMock())
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 2
    assert runner._next_attempt["task-1"] > time.time()


def test_unexpected_exception_is_treated_as_retry(monkeypatch):
    task = _task(retry=0)
    _wire(monkeypatch, task)

    def handler(_task):
        raise RuntimeError("boom")

    runner = _runner(handler)
    runner._process(task, MagicMock())  # must not raise
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 1


def test_success_message_comes_from_the_handler(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)

    def handler(t):
        t.function_result = "Backup created"

    _runner(handler)._process(task, MagicMock())
    assert task.status == JobSchedule.STATUS_DONE
    assert task.function_result == "Backup created"


def test_previous_failure_result_does_not_survive_a_later_success(monkeypatch):
    task = _task(status=JobSchedule.STATUS_SUSPENDED, retry=1)
    task.function_result = "rpc failed"
    _wire(monkeypatch, task)

    _runner(MagicMock(return_value=None))._process(task, MagicMock())
    assert task.status == JobSchedule.STATUS_DONE
    assert task.function_result == "completed"


def test_abort_marks_done_with_reason(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskAbort("missing param")

    _runner(handler)._process(task, MagicMock())
    assert task.status == JobSchedule.STATUS_DONE
    assert task.function_result == "missing param"


# -- pre-run skip-gates -----------------------------------------------------

def test_ineligible_skips_without_claim_or_write(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    claim = MagicMock(return_value=True)
    monkeypatch.setattr(trb.tasks_controller, "claim_task", claim)
    handler = MagicMock()

    _runner(handler, is_eligible=lambda t, c: False)._process(task, MagicMock())

    handler.assert_not_called()
    claim.assert_not_called()
    JobSchedule.write_to_db.assert_not_called()
    assert task.status == JobSchedule.STATUS_NEW


def test_default_eligible_runs(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)
    _runner(handler)._process(task, MagicMock())
    handler.assert_called_once()


def test_lease_denied_skips(monkeypatch):
    task = _task()
    _wire(monkeypatch, task, claim=False)
    handler = MagicMock()
    _runner(handler)._process(task, MagicMock())
    handler.assert_not_called()
    assert task.status == JobSchedule.STATUS_NEW


# -- terminal pre-handler checks -------------------------------------------

def test_canceled_marks_done_without_handler(monkeypatch):
    task = _task(canceled=True)
    _wire(monkeypatch, task)
    handler = MagicMock()
    _runner(handler)._process(task, MagicMock())
    handler.assert_not_called()
    assert task.status == JobSchedule.STATUS_DONE
    assert task.function_result == "canceled"


def test_max_retry_marks_done_without_handler(monkeypatch):
    task = _task(retry=8, max_retry=8)
    _wire(monkeypatch, task)
    handler = MagicMock()
    _runner(handler)._process(task, MagicMock())
    handler.assert_not_called()
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result


def test_negative_max_retry_is_unbounded(monkeypatch):
    task = _task(retry=100, max_retry=-1)
    _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)
    _runner(handler)._process(task, MagicMock())
    handler.assert_called_once()  # ceiling never binds for max_retry < 0
    assert task.status == JobSchedule.STATUS_DONE


# -- dispatch loop ----------------------------------------------------------

class _StopLoop(Exception):
    pass


def test_run_dispatches_only_matching_non_done(monkeypatch):
    match = _task()
    other = _task()
    other.uuid = "other"
    other.function_name = "nope"
    done = _task()
    done.uuid = "done"
    done.status = JobSchedule.STATUS_DONE

    cluster = MagicMock()
    cluster.get_id.return_value = "cl-1"
    db = MagicMock()
    db.get_clusters.return_value = [cluster]
    db.get_job_tasks.return_value = [match, other, done]
    monkeypatch.setattr(trb, "db", db)
    monkeypatch.setattr(trb.time, "sleep", MagicMock(side_effect=_StopLoop))

    runner = _runner(MagicMock())
    dispatched = []
    monkeypatch.setattr(runner, "_dispatch", lambda t, c: dispatched.append(t.uuid))

    with pytest.raises(_StopLoop):
        runner.run()
    assert dispatched == ["task-1"]


def test_run_propagates_db_error(monkeypatch):
    db = MagicMock()
    db.get_clusters.side_effect = RuntimeError("fdb down")
    monkeypatch.setattr(trb, "db", db)
    with pytest.raises(RuntimeError, match="fdb down"):
        _runner(MagicMock()).run()


# -- spec validation --------------------------------------------------------

def test_spec_rejects_zero_concurrency():
    with pytest.raises(ValueError):
        trb.RunnerSpec(function_names=("fn",), handler=MagicMock(), concurrency=0)
