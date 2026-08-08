# coding=utf-8
"""Unit tests for the shared task-runner driver (``task_runner_base``).

These exercise the per-task lifecycle (``_process``) and the dispatch loop
(``run``) directly, against a fake store that models FoundationDB's two write
paths faithfully. They pin the handler contract: return → DONE, TaskDefer →
suspend without a retry, TaskRetry / unexpected Exception → suspend + retry +
backoff, TaskAbort → DONE; the pre-run skip-gates (eligibility, lease); that a
handler exception never escapes the loop while a DB error does; and — the part
that needs a real store to observe — that no transition can revert what another
actor committed while the handler was running.

Assertions read the committed row (``store.row()``), not the caller's copy: the
driver's writes are compare-and-set against the current row and deliberately do
not mutate the stale object it was holding.
"""
import copy
import threading
import time
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.job_schedule import JobSchedule
import simplyblock_core.services.task_runner_base as trb


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


class _Store:
    """The task row plus the two ways it gets written, modelled faithfully.

    ``atomic_update`` follows DBController's contract: the mutator runs against
    the row as it exists *in the store*, not against the caller's copy; a
    mutator returning False aborts the write; the return is the fresh object,
    or None when the row is gone. ``full_write`` is the ``write_to_db`` path —
    it replaces the row wholesale from the caller's (possibly stale) copy.
    """

    def __init__(self, task):
        self.kv_store = "KV"
        self._rows = {task.uuid: copy.deepcopy(task)}

    # DBController surface used by the driver
    def get_task_by_id(self, uuid):
        row = self._rows.get(uuid)
        return copy.deepcopy(row) if row is not None else None

    def atomic_update(self, obj, mutate):
        row = self._rows.get(obj.uuid)
        if row is None:
            return None
        fresh = copy.deepcopy(row)
        if mutate(fresh) is False:
            return copy.deepcopy(row)
        self._rows[obj.uuid] = fresh
        return copy.deepcopy(fresh)

    # test surface
    def full_write(self, obj):
        self._rows[obj.uuid] = copy.deepcopy(obj)

    def row(self, uuid="task-1"):
        return self._rows[uuid]

    def concurrently(self, uuid="task-1", **fields):
        """Another actor commits to the row while the handler is running."""
        for name, value in fields.items():
            setattr(self._rows[uuid], name, value)


def _wire(monkeypatch, task, claim=True):
    """Point the driver at a fake store, grant the lease, and neutralise the
    heartbeat (a no-op context manager, so no real thread is spawned)."""
    store = _Store(task)
    monkeypatch.setattr(trb, "db", store)
    monkeypatch.setattr(JobSchedule, "write_to_db",
                        lambda self, kv=None: store.full_write(self))
    monkeypatch.setattr(trb.tasks_controller, "claim_task", lambda *a, **k: claim)
    monkeypatch.setattr(trb.tasks_controller, "task_lease_heartbeat", MagicMock())
    return store


def _runner(handler, **spec_kw):
    spec = trb.RunnerSpec(function_names=("fn",), handler=handler, **spec_kw)
    return trb.TaskRunner(spec)


# -- handler outcome vocabulary --------------------------------------------

def test_void_return_marks_done(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)

    _runner(handler)._process(task, MagicMock())

    assert handler.call_args[0][0].uuid == "task-1"
    assert store.row().status == JobSchedule.STATUS_DONE


def test_handler_runs_under_lease_heartbeat(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    hb = MagicMock()
    monkeypatch.setattr(trb.tasks_controller, "task_lease_heartbeat", hb)

    _runner(MagicMock(return_value=None))._process(task, MagicMock())

    assert hb.call_args[0][0].uuid == "task-1"
    hb.return_value.__enter__.assert_called_once()
    hb.return_value.__exit__.assert_called_once()


def test_defer_suspends_without_consuming_retry(monkeypatch):
    task = _task(retry=2)
    store = _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskDefer("node not online")

    runner = _runner(handler)
    runner._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_SUSPENDED
    assert store.row().retry == 2
    assert store.row().function_result == "node not online"
    assert "task-1" not in runner._next_attempt  # no backoff for a defer


def test_retry_suspends_consumes_retry_and_backs_off(monkeypatch):
    task = _task(retry=1)
    store = _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskRetry("rpc failed")

    runner = _runner(handler)
    runner._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_SUSPENDED
    assert store.row().retry == 2
    assert runner._next_attempt["task-1"] > time.time()


def test_unexpected_exception_is_treated_as_retry(monkeypatch):
    task = _task(retry=0)
    store = _wire(monkeypatch, task)

    def handler(_task):
        raise RuntimeError("boom")

    _runner(handler)._process(task, MagicMock())  # must not raise

    assert store.row().status == JobSchedule.STATUS_SUSPENDED
    assert store.row().retry == 1


def test_success_message_comes_from_the_handler(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(t):
        t.function_result = "Backup created"

    _runner(handler)._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_DONE
    assert store.row().function_result == "Backup created"


def test_previous_failure_result_does_not_survive_a_later_success(monkeypatch):
    task = _task(status=JobSchedule.STATUS_SUSPENDED, retry=1)
    task.function_result = "rpc failed"
    store = _wire(monkeypatch, task)

    _runner(MagicMock(return_value=None))._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_DONE
    assert store.row().function_result == "completed"


def test_abort_marks_done_with_reason(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskAbort("missing param")

    _runner(handler)._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_DONE
    assert store.row().function_result == "missing param"


# -- terminal cleanup hook --------------------------------------------------

@pytest.mark.parametrize("handler,expected", [
    (MagicMock(return_value=None), "completed"),
    (MagicMock(side_effect=trb.TaskAbort("gone")), "gone"),
])
def test_on_finish_runs_for_every_terminal_outcome(monkeypatch, handler, expected):
    task = _task()
    store = _wire(monkeypatch, task)
    on_finish = MagicMock()

    _runner(handler, on_finish=on_finish)._process(task, MagicMock())

    assert on_finish.call_args[0][0].uuid == "task-1"
    assert store.row().status == JobSchedule.STATUS_DONE
    assert store.row().function_result == expected


def test_on_finish_runs_when_the_handler_is_never_reached(monkeypatch):
    task = _task(canceled=True)
    _wire(monkeypatch, task)
    on_finish = MagicMock()

    _runner(MagicMock(), on_finish=on_finish)._process(task, MagicMock())

    on_finish.assert_called_once()


def test_on_finish_does_not_run_for_a_suspended_task(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    on_finish = MagicMock()

    _runner(MagicMock(side_effect=trb.TaskDefer("later")),
            on_finish=on_finish)._process(task, MagicMock())

    on_finish.assert_not_called()


def test_failing_on_finish_does_not_break_the_task(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    runner = _runner(MagicMock(return_value=None),
                     on_finish=MagicMock(side_effect=RuntimeError("cleanup boom")))
    runner._process(task, MagicMock())  # must not raise

    assert store.row().status == JobSchedule.STATUS_DONE


# -- pre-run skip-gates -----------------------------------------------------

def test_ineligible_skips_without_claim_or_write(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)
    claim = MagicMock(return_value=True)
    monkeypatch.setattr(trb.tasks_controller, "claim_task", claim)
    handler = MagicMock()

    _runner(handler, is_eligible=lambda t, c: False)._process(task, MagicMock())

    handler.assert_not_called()
    claim.assert_not_called()
    assert store.row().status == JobSchedule.STATUS_NEW


def test_default_eligible_runs(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)

    _runner(handler)._process(task, MagicMock())

    handler.assert_called_once()


def test_lease_denied_skips(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task, claim=False)
    handler = MagicMock()

    _runner(handler)._process(task, MagicMock())

    handler.assert_not_called()
    assert store.row().status == JobSchedule.STATUS_NEW


# -- terminal pre-handler checks -------------------------------------------

def test_canceled_marks_done_without_handler(monkeypatch):
    task = _task(canceled=True)
    store = _wire(monkeypatch, task)
    handler = MagicMock()

    _runner(handler)._process(task, MagicMock())

    handler.assert_not_called()
    assert store.row().status == JobSchedule.STATUS_DONE
    assert store.row().function_result == "canceled"


def test_max_retry_marks_done_without_handler(monkeypatch):
    task = _task(retry=8, max_retry=8)
    store = _wire(monkeypatch, task)
    handler = MagicMock()

    _runner(handler)._process(task, MagicMock())

    handler.assert_not_called()
    assert store.row().status == JobSchedule.STATUS_DONE
    assert "max retry" in store.row().function_result


def test_negative_max_retry_is_unbounded(monkeypatch):
    task = _task(retry=100, max_retry=-1)
    store = _wire(monkeypatch, task)
    handler = MagicMock(return_value=None)

    _runner(handler)._process(task, MagicMock())

    handler.assert_called_once()  # ceiling never binds for max_retry < 0
    assert store.row().status == JobSchedule.STATUS_DONE


# -- concurrent-writer safety ----------------------------------------------
#
# A handler runs for minutes (node add, restart, migration) while the driver
# holds the task copy it fetched beforehand. Other actors write that row in the
# meantime — set_node_status(ONLINE) cancels restart tasks
# (tasks_controller.cancel_pending_node_restart_tasks), an operator cancels a
# task, another host's lease heartbeat stamps it. A full-object write of the
# stale copy silently reverts all of that: it is what un-canceled a task and
# wiped its owner lease in the 2026-07-29 double-restart incident, and it is
# why upstream converted the restart runner's task writes to atomic CAS.

def test_defer_does_not_resurrect_a_concurrently_canceled_task(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(_task):
        store.concurrently(canceled=True, status=JobSchedule.STATUS_DONE,
                           function_result="canceled: node back online")
        raise trb.TaskDefer("peer is restarting")

    _runner(handler)._process(task, MagicMock())

    row = store.row()
    assert row.canceled is True
    assert row.status == JobSchedule.STATUS_DONE
    assert row.function_result == "canceled: node back online"


def test_failure_does_not_resurrect_a_concurrently_canceled_task(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(_task):
        store.concurrently(canceled=True, status=JobSchedule.STATUS_DONE)
        raise trb.TaskRetry("rpc failed")

    _runner(handler)._process(task, MagicMock())

    assert store.row().canceled is True
    assert store.row().status == JobSchedule.STATUS_DONE


def test_write_does_not_steal_a_lease_taken_during_the_handler(monkeypatch):
    task = _task()
    task.owner = "this-host"
    store = _wire(monkeypatch, task)

    def handler(_task):
        store.concurrently(owner="other-host")

    _runner(handler)._process(task, MagicMock())

    assert store.row().owner == "other-host"


def test_retry_is_counted_on_the_fresh_row(monkeypatch):
    task = _task(retry=1)
    store = _wire(monkeypatch, task)

    def handler(_task):
        store.concurrently(retry=5)
        raise trb.TaskRetry("rpc failed")

    _runner(handler)._process(task, MagicMock())

    assert store.row().retry == 6


def test_handler_progress_is_carried_onto_the_fresh_row(monkeypatch):
    """Handlers record progress in function_params (recovery_started,
    merge_started, fail_count) — a CAS that only wrote the lifecycle fields
    would drop it and the next attempt would re-issue the RPC."""
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(t):
        t.function_params["recovery_started"] = True
        raise trb.TaskDefer("Restore started")

    _runner(handler)._process(task, MagicMock())

    assert store.row().function_params["recovery_started"] is True


def test_on_finish_is_skipped_when_another_actor_finished_the_task(monkeypatch):
    """Cleanup is a side effect of winning the terminal transition. Running it
    off a lost CAS means two hosts both release the resource."""
    task = _task()
    store = _wire(monkeypatch, task)
    on_finish = MagicMock()

    def handler(_task):
        store.concurrently(status=JobSchedule.STATUS_DONE,
                           function_result="canceled: node back online")

    _runner(handler, on_finish=on_finish)._process(task, MagicMock())

    on_finish.assert_not_called()
    assert store.row().function_result == "canceled: node back online"


# -- single dispatch path ---------------------------------------------------
#
# The 2026-07-29 incident's other half: restart had a parallel branch that
# consulted the inflight map and an inline branch that consulted neither, so a
# dispatch-mode flip mid-restart re-entered a task still running on the pool.
# Serial execution must therefore register in-flight exactly like parallel
# execution does.

def test_a_running_task_is_not_re_entered_by_a_second_dispatch(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)
    calls, started, release = [], threading.Event(), threading.Event()

    def handler(_task):
        calls.append(1)
        started.set()
        release.wait(5)

    runner = _runner(handler)
    cluster = MagicMock()

    worker = threading.Thread(target=runner._dispatch, args=(task, cluster))
    worker.start()
    try:
        assert started.wait(5)
        # The dispatch loop comes round again while the task is still running.
        runner._dispatch(store.get_task_by_id("task-1"), cluster)
    finally:
        release.set()
        worker.join(5)

    assert calls == [1]


def test_serialized_dispatch_waits_for_the_task(monkeypatch):
    """Serialized mode must submit and wait, not run inline — that is what
    makes a mode flip harmless in both directions."""
    task = _task()
    store = _wire(monkeypatch, task)

    runner = _runner(MagicMock(return_value=None), concurrency=4,
                     serialize=lambda t, c: True)
    runner._dispatch(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_DONE


def test_parallel_dispatch_does_not_wait(monkeypatch):
    task = _task()
    _wire(monkeypatch, task)
    started, release = threading.Event(), threading.Event()

    def handler(_task):
        started.set()
        release.wait(5)

    runner = _runner(handler, concurrency=4)
    try:
        runner._dispatch(task, MagicMock())  # returns while the handler runs
        assert started.wait(5)
    finally:
        release.set()


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


# -- handler progress checkpoints -------------------------------------------

def test_checkpoint_persists_progress_immediately(monkeypatch):
    """A destructive step records itself the moment it succeeds, so a crash
    before the handler returns does not repeat it."""
    task = _task()
    store = _wire(monkeypatch, task)

    fresh = trb.checkpoint(store.get_task_by_id("task-1"), cleanup_shutdown_done=True)

    assert fresh.function_params["cleanup_shutdown_done"] is True
    assert store.row().function_params["cleanup_shutdown_done"] is True


def test_checkpoint_keeps_existing_params(monkeypatch):
    task = _task()
    task.function_params = {"node_addr": "1.2.3.4:5000"}
    store = _wire(monkeypatch, task)

    trb.checkpoint(store.get_task_by_id("task-1"), cleanup_shutdown_done=True)

    assert store.row().function_params == {
        "node_addr": "1.2.3.4:5000", "cleanup_shutdown_done": True}


def test_checkpoint_reports_a_cancellation_under_the_handler(monkeypatch):
    """The handler's cancellation probe: a task canceled mid-handler must not
    go on to the next destructive step."""
    task = _task()
    store = _wire(monkeypatch, task)
    store.concurrently(canceled=True)

    assert trb.checkpoint(store.get_task_by_id("task-1"), step_done=True) is None
    assert "step_done" not in store.row().function_params


def test_checkpoint_reports_a_task_finished_under_the_handler(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)
    store.concurrently(status=JobSchedule.STATUS_DONE)

    assert trb.checkpoint(store.get_task_by_id("task-1"), step_done=True) is None


# -- per-cluster cycle hook -------------------------------------------------

def test_cycle_hook_runs_once_per_cluster(monkeypatch):
    cluster = MagicMock()
    cluster.get_id.return_value = "cl-1"
    db = MagicMock()
    db.get_clusters.return_value = [cluster]
    db.get_job_tasks.return_value = []
    monkeypatch.setattr(trb, "db", db)
    monkeypatch.setattr(trb.time, "sleep", MagicMock(side_effect=_StopLoop))
    on_cycle = MagicMock()

    with pytest.raises(_StopLoop):
        _runner(MagicMock(), on_cycle=on_cycle).run()

    on_cycle.assert_called_once_with(cluster)


def test_failing_cycle_hook_does_not_stop_the_loop(monkeypatch):
    cluster = MagicMock()
    cluster.get_id.return_value = "cl-1"
    db = MagicMock()
    db.get_clusters.return_value = [cluster]
    db.get_job_tasks.return_value = []
    monkeypatch.setattr(trb, "db", db)
    monkeypatch.setattr(trb.time, "sleep", MagicMock(side_effect=_StopLoop))

    runner = _runner(MagicMock(), on_cycle=MagicMock(side_effect=RuntimeError("watchdog boom")))
    with pytest.raises(_StopLoop):   # reached the sleep, i.e. the cycle completed
        runner.run()


# -- in-progress polling ----------------------------------------------------

def test_progress_keeps_the_task_running(monkeypatch):
    """A polled long-running operation must not be suspended between polls:
    the migration family gates mutual exclusion on a sibling being RUNNING."""
    task = _task(retry=2)
    store = _wire(monkeypatch, task)

    def handler(_task):
        raise trb.TaskProgress("Status: in_progress, progress:42")

    runner = _runner(handler)
    runner._process(task, MagicMock())

    assert store.row().status == JobSchedule.STATUS_RUNNING
    assert store.row().retry == 2
    assert store.row().function_result == "Status: in_progress, progress:42"
    assert "task-1" not in runner._next_attempt


def test_progress_does_not_resurrect_a_concurrently_canceled_task(monkeypatch):
    task = _task()
    store = _wire(monkeypatch, task)

    def handler(_task):
        store.concurrently(canceled=True, status=JobSchedule.STATUS_DONE)
        raise trb.TaskProgress("still going")

    _runner(handler)._process(task, MagicMock())

    assert store.row().canceled is True
    assert store.row().status == JobSchedule.STATUS_DONE
