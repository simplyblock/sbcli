# coding=utf-8
"""End-to-end integration test for simplyblock_lib.tasks.runner.TaskRunner
against the real FoundationDB provisioned by tests/integration/conftest.py.

A real Cluster and real JobSchedule records are persisted; a small runner
subclass sweeps them exactly like a production tasks_runner_* service
(cluster scan → task-table range read → re-read → lease claim → execute →
outcome write), and the assertions read the task records back from FDB.
"""
import time
import uuid as uuid_lib

import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.tasks import TaskLease, TaskResult, TaskRunner

FN_TEST = "lib_runner_test"


@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return controller


@pytest.fixture(autouse=True)
def _clean_keyspace(db):
    db.kv_store.clear_range(b"\x00", b"\xff")
    yield


@pytest.fixture()
def cluster(db):
    c = Cluster()
    c.uuid = "runner-it-cluster"
    c.cluster_name = "runner-it"
    c.status = Cluster.STATUS_ACTIVE
    c.write_to_db(db.kv_store)
    return c


def _seed_task(db, cluster, function_name=FN_TEST, canceled=False, max_retry=-1):
    task = JobSchedule()
    task.uuid = str(uuid_lib.uuid4())
    task.cluster_id = cluster.get_id()
    task.date = int(time.time())
    task.function_name = function_name
    task.status = JobSchedule.STATUS_NEW
    task.canceled = canceled
    task.max_retry = max_retry
    task.write_to_db(db.kv_store)
    return task


class Runner(TaskRunner):
    function_names = (FN_TEST,)

    def __init__(self, db, outcome, **kwargs):
        kwargs.setdefault("sleep", lambda _s: None)
        super().__init__(db, **kwargs)
        self.outcome = outcome
        self.executed = []

    def execute(self, task):
        self.executed.append(task.uuid)
        return self.outcome


def test_run_cycle_completes_task(db, cluster):
    task = _seed_task(db, cluster)
    runner = Runner(db, TaskResult.done("completed by lib runner"),
                    lease=TaskLease(db, ttl_sec=180, heartbeat_sec=30, owner="it-host"))
    runner.run_cycle()

    assert runner.executed == [task.uuid]
    persisted = db.get_task_by_id(task.uuid)
    assert persisted.status == JobSchedule.STATUS_DONE
    assert persisted.function_result == "completed by lib runner"
    assert persisted.owner == "it-host"

    # A second sweep must not re-execute a done task.
    runner.run_cycle()
    assert runner.executed == [task.uuid]


def test_run_cycle_ignores_foreign_tasks(db, cluster):
    _seed_task(db, cluster, function_name=JobSchedule.FN_FDB_BACKUP)
    runner = Runner(db, TaskResult.done())
    runner.run_cycle()
    assert runner.executed == []


def test_canceled_task_finalized_without_execute(db, cluster):
    task = _seed_task(db, cluster, canceled=True)
    runner = Runner(db, TaskResult.done())
    runner.run_cycle()

    assert runner.executed == []
    persisted = db.get_task_by_id(task.uuid)
    assert persisted.status == JobSchedule.STATUS_DONE
    assert persisted.function_result == "canceled"


def test_retry_persists_and_hits_ceiling(db, cluster):
    task = _seed_task(db, cluster, max_retry=2)
    runner = Runner(db, TaskResult.retry("attempt failed"))

    runner.run_cycle()
    assert db.get_task_by_id(task.uuid).retry == 1
    runner.run_cycle()
    assert db.get_task_by_id(task.uuid).retry == 2

    # Third sweep trips the ceiling without executing.
    runner.run_cycle()
    persisted = db.get_task_by_id(task.uuid)
    assert persisted.status == JobSchedule.STATUS_DONE
    assert persisted.function_result == "max retry reached, stopping task"
    assert runner.executed == [task.uuid, task.uuid]


def test_two_runner_hosts_do_not_double_execute(db, cluster):
    """The second host's sweep is locked out by the first host's live lease."""
    task = _seed_task(db, cluster)
    lease_a = TaskLease(db, ttl_sec=180, heartbeat_sec=30, owner="hostA")
    lease_b = TaskLease(db, ttl_sec=180, heartbeat_sec=30, owner="hostB")

    # hostA executes but its task body defers (returns None → stays RUNNING).
    runner_a = Runner(db, None, lease=lease_a)
    runner_a.run_cycle()
    assert runner_a.executed == [task.uuid]

    runner_b = Runner(db, TaskResult.done(), lease=lease_b)
    runner_b.run_cycle()
    assert runner_b.executed == []
    assert db.get_task_by_id(task.uuid).owner == "hostA"
    assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_RUNNING
