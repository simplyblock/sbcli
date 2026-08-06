# coding=utf-8
"""Unit tests for simplyblock_lib.tasks.runner.TaskRunner.

Everything is duck-typed fakes: the runner must work without the JobSchedule
model, DBController, or FDB.
"""
import contextlib

import pytest

from simplyblock_lib.tasks.runner import TaskResult, TaskRunner


class FakeTask:
    def __init__(self, uuid="task-1", function_name="test_fn", status='new',
                 canceled=False, retry=0, max_retry=-1):
        self.uuid = uuid
        self.function_name = function_name
        self.status = status
        self.canceled = canceled
        self.retry = retry
        self.max_retry = max_retry
        self.function_result = ""
        self.writes = 0

    def write_to_db(self, kv_store=None):
        self.writes += 1


class FakeCluster:
    def __init__(self, uuid="cluster-1", status='active'):
        self.uuid = uuid
        self.status = status

    def get_id(self):
        return self.uuid


class FakeDB:
    kv_store = object()

    def __init__(self, clusters=None, tasks=None):
        self.clusters = clusters if clusters is not None else [FakeCluster()]
        self.tasks = tasks or []

    def get_clusters(self):
        return self.clusters

    def get_job_tasks(self, cluster_id, **kwargs):
        return list(self.tasks)

    def get_task_by_id(self, uuid):
        for task in self.tasks:
            if task.uuid == uuid:
                return task
        raise KeyError(uuid)


class RecordingRunner(TaskRunner):
    function_names = ("test_fn",)

    def __init__(self, db, result=None, **kwargs):
        kwargs.setdefault("sleep", lambda _s: None)
        super().__init__(db, **kwargs)
        self.result = result
        self.executed = []
        self.canceled_hook = []

    def execute(self, task):
        self.executed.append(task.uuid)
        return self.result

    def on_canceled(self, task):
        self.canceled_hook.append(task.uuid)


def test_function_names_required():
    with pytest.raises(ValueError):
        TaskRunner(FakeDB())


def test_done_and_foreign_tasks_skipped():
    db = FakeDB(tasks=[FakeTask(uuid="t-done", status='done'),
                       FakeTask(uuid="t-other", function_name="other_fn")])
    runner = RecordingRunner(db)
    runner.run_cycle()
    assert runner.executed == []


def test_execute_done_finalizes_task():
    task = FakeTask()
    runner = RecordingRunner(FakeDB(tasks=[task]), result=TaskResult.done("all good"))
    runner.run_cycle()
    assert runner.executed == ["task-1"]
    assert task.status == 'done'
    assert task.function_result == "all good"


def test_task_marked_running_before_execute():
    task = FakeTask(status='new')
    seen = []

    class Runner(RecordingRunner):
        def execute(self, t):
            seen.append(t.status)

    Runner(FakeDB(tasks=[task])).run_cycle()
    assert seen == ['running']


def test_execute_none_leaves_task_for_next_cycle():
    task = FakeTask()
    runner = RecordingRunner(FakeDB(tasks=[task]), result=None)
    runner.run_cycle()
    runner.run_cycle()
    assert runner.executed == ["task-1", "task-1"]
    assert task.status == 'running'


def test_canceled_task_finalized_with_hook():
    task = FakeTask(canceled=True)
    runner = RecordingRunner(FakeDB(tasks=[task]))
    runner.run_cycle()
    assert runner.executed == []
    assert runner.canceled_hook == ["task-1"]
    assert task.status == 'done'
    assert task.function_result == "canceled"


def test_retry_ceiling_finalizes_task():
    task = FakeTask(retry=3, max_retry=3)
    runner = RecordingRunner(FakeDB(tasks=[task]))
    runner.run_cycle()
    assert runner.executed == []
    assert task.status == 'done'
    assert task.function_result == "max retry reached, stopping task"


def test_negative_max_retry_means_unlimited():
    task = FakeTask(retry=1000, max_retry=-1)
    runner = RecordingRunner(FakeDB(tasks=[task]), result=TaskResult.done())
    runner.run_cycle()
    assert runner.executed == ["task-1"]


def test_retry_result_increments_and_backs_off():
    task = FakeTask()
    clock = {"now": 100.0}
    runner = RecordingRunner(
        FakeDB(tasks=[task]), result=TaskResult.retry("attempt failed"),
        retry_backoff_base_sec=10, retry_backoff_max_sec=3600,
        monotonic=lambda: clock["now"])
    runner.run_cycle()
    assert task.retry == 1
    assert task.function_result == "attempt failed"

    # Within the backoff window the task is skipped …
    runner.run_cycle()
    assert runner.executed == ["task-1"]

    # … and re-attempted once the window has passed.
    clock["now"] += 11
    runner.run_cycle()
    assert runner.executed == ["task-1", "task-1"]
    assert task.retry == 2


def test_backoff_doubles_and_caps():
    clock = {"now": 0.0}
    runner = RecordingRunner(
        FakeDB(), retry_backoff_base_sec=10, retry_backoff_max_sec=25,
        monotonic=lambda: clock["now"])
    task = FakeTask(retry=1)
    runner._schedule_backoff(task)
    assert runner._next_attempt_at[task.uuid] == 10.0
    task.retry = 2
    runner._schedule_backoff(task)
    assert runner._next_attempt_at[task.uuid] == 20.0
    task.retry = 3
    runner._schedule_backoff(task)
    assert runner._next_attempt_at[task.uuid] == 25.0  # capped


def test_suspend_result_does_not_consume_retry():
    task = FakeTask()
    runner = RecordingRunner(FakeDB(tasks=[task]), result=TaskResult.suspend("waiting"))
    runner.run_cycle()
    assert task.status == 'suspended'
    assert task.retry == 0
    assert task.function_result == "waiting"


def test_execute_exception_is_isolated():
    task1 = FakeTask(uuid="t-1")
    task2 = FakeTask(uuid="t-2")

    class ExplodingRunner(RecordingRunner):
        def execute(self, t):
            super().execute(t)
            if t.uuid == "t-1":
                raise RuntimeError("boom")
            return TaskResult.done()

    runner = ExplodingRunner(FakeDB(tasks=[task1, task2]))
    runner.run_cycle()
    # t-1 crashed but t-2 was still processed.
    assert runner.executed == ["t-1", "t-2"]
    assert task1.status == 'running'  # untouched by the crash
    assert task2.status == 'done'


def test_cluster_filter_skips_cluster():
    task = FakeTask()
    db = FakeDB(clusters=[FakeCluster(status='in_activation')], tasks=[task])
    runner = RecordingRunner(db, result=TaskResult.done(),
                             cluster_filter=lambda c: c.status != 'in_activation')
    runner.run_cycle()
    assert runner.executed == []


class FakeLease:
    def __init__(self, grant=True):
        self.grant = grant
        self.claims = []
        self.heartbeats = 0

    def claim(self, task, owner=None):
        self.claims.append(task.uuid)
        return self.grant

    @contextlib.contextmanager
    def heartbeat(self, task, owner=None):
        self.heartbeats += 1
        yield


def test_lease_denied_skips_execute():
    task = FakeTask()
    lease = FakeLease(grant=False)
    runner = RecordingRunner(FakeDB(tasks=[task]), lease=lease, result=TaskResult.done())
    runner.run_cycle()
    assert lease.claims == ["task-1"]
    assert runner.executed == []
    assert task.status == 'new'


def test_lease_granted_executes_under_heartbeat():
    task = FakeTask()
    lease = FakeLease(grant=True)
    runner = RecordingRunner(FakeDB(tasks=[task]), lease=lease, result=TaskResult.done())
    runner.run_cycle()
    assert runner.executed == ["task-1"]
    assert lease.heartbeats == 1
    assert task.status == 'done'


def test_db_failure_threshold_exits():
    class BrokenDB(FakeDB):
        def get_clusters(self):
            raise RuntimeError("fdb 1031")

    sleeps = []
    runner = RecordingRunner(BrokenDB(), db_failure_threshold=3,
                             sleep=sleeps.append)
    runner.run_cycle()
    runner.run_cycle()
    with pytest.raises(SystemExit):
        runner.run_cycle()
    # error cadence used on failures (not the full interval)
    assert sleeps == [runner.error_interval_sec] * 2


def test_empty_cluster_list_counts_as_db_failure():
    runner = RecordingRunner(FakeDB(clusters=[]), db_failure_threshold=2)
    runner.run_cycle()
    with pytest.raises(SystemExit):
        runner.run_cycle()


def test_successful_sweep_resets_failure_counter():
    db = FakeDB(tasks=[])
    flaky = {"fail": False}
    original = db.get_clusters

    def maybe_fail():
        if flaky["fail"]:
            raise RuntimeError("transient")
        return original()

    db.get_clusters = maybe_fail
    runner = RecordingRunner(db, db_failure_threshold=2)
    flaky["fail"] = True
    runner.run_cycle()  # failure 1
    flaky["fail"] = False
    runner.run_cycle()  # success resets
    flaky["fail"] = True
    runner.run_cycle()  # failure 1 again — must NOT exit
    with pytest.raises(SystemExit):
        runner.run_cycle()  # failure 2 — exits


def test_concurrent_finish_between_read_and_process():
    """A task listed as pending but already done on re-read is skipped."""
    stale = FakeTask(uuid="t-1", status='running')
    fresh = FakeTask(uuid="t-1", status='done')

    class DB(FakeDB):
        def get_job_tasks(self, cluster_id, **kwargs):
            return [stale]

        def get_task_by_id(self, uuid):
            return fresh

    runner = RecordingRunner(DB(), result=TaskResult.done())
    runner.run_cycle()
    assert runner.executed == []
