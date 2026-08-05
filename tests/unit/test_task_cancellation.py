# coding=utf-8
"""Unit tests for task cancellation (tasks_controller.cancel_task and
cancel_pending_node_restart_tasks).

A canceller reads the task, decides, and writes — and in between, the runner
driving that task is writing the same row: claiming its lease, moving it to
running, advancing retry, recording handler progress. A full-object
``write_to_db`` of the canceller's copy puts all of that back. The worst of it
is the owner lease: wiping it hands the task to the next runner host that polls,
which then executes it a second time. That is the same class of lost update as
the 2026-07-29 double restart, in the opposite direction.

These use a store that models both write paths, so the assertions are about what
ends up committed rather than about the caller's copy.
"""
import copy
from unittest.mock import MagicMock

import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule


def _task(uuid="task-1", function_name=JobSchedule.FN_NODE_RESTART,
          status=JobSchedule.STATUS_NEW, node_id="node-1"):
    task = JobSchedule()
    task.uuid = uuid
    task.cluster_id = "cl-1"
    task.node_id = node_id
    task.function_name = function_name
    task.status = status
    task.canceled = False
    task.retry = 0
    task.function_params = {}
    return task


class _Store:
    """Task rows plus the two ways they get written.

    ``atomic_update`` follows DBController's contract: the mutator runs against
    the row as it exists in the store, a mutator returning False aborts the
    write, and the return is the fresh object (or None when the row is gone).
    ``full_write`` is the ``write_to_db`` path — a wholesale replace from the
    caller's copy.
    """

    def __init__(self, *tasks):
        self.kv_store = "KV"
        self._rows = {t.uuid: copy.deepcopy(t) for t in tasks}
        self._stale = None

    def get_task_by_id(self, task_id):
        if self._stale is not None:
            return copy.deepcopy(self._stale[task_id])
        if task_id not in self._rows:
            raise KeyError(task_id)
        return copy.deepcopy(self._rows[task_id])

    def get_job_tasks(self, cluster_id, reverse=True, limit=0):
        rows = self._stale if self._stale is not None else self._rows
        return [copy.deepcopy(row) for row in rows.values()]

    def atomic_update(self, obj, mutate):
        row = self._rows.get(obj.uuid)
        if row is None:
            return None
        fresh = copy.deepcopy(row)
        if mutate(fresh) is False:
            return copy.deepcopy(row)
        self._rows[obj.uuid] = fresh
        return copy.deepcopy(fresh)

    def full_write(self, obj):
        self._rows[obj.uuid] = copy.deepcopy(obj)

    def row(self, uuid="task-1"):
        return self._rows[uuid]

    def runner_claims(self, uuid="task-1", **fields):
        """The runner drives the task after the canceller has read it: pin what
        the canceller saw, then advance the row underneath it."""
        if self._stale is None:
            self._stale = copy.deepcopy(self._rows)
        row = self._rows[uuid]
        row.owner = fields.pop("owner", "host-A")
        row.status = fields.pop("status", JobSchedule.STATUS_RUNNING)
        for name, value in fields.items():
            setattr(row, name, value)


@pytest.fixture
def store(monkeypatch):
    def _install(*tasks):
        s = _Store(*tasks)
        monkeypatch.setattr(tasks_controller, "db", s)
        monkeypatch.setattr(JobSchedule, "write_to_db",
                            lambda self, kv=None: s.full_write(self))
        return s
    monkeypatch.setattr(tasks_controller, "tasks_events", MagicMock())
    monkeypatch.setattr(tasks_controller, "device_controller", MagicMock())
    return _install


# -- cancel_task ------------------------------------------------------------

def test_cancel_task_does_not_wipe_the_lease_of_a_running_task(store):
    """The lease is what stops a second host from running the task. A cancel
    that clears it hands the task straight to the next poller."""
    s = store(_task())
    s.runner_claims(owner="host-A", retry=1)

    assert tasks_controller.cancel_task("task-1") is True

    row = s.row()
    assert row.canceled is True
    assert row.owner == "host-A"
    assert row.status == JobSchedule.STATUS_RUNNING
    assert row.retry == 1


def test_cancel_task_does_not_revert_a_finished_task(store):
    s = store(_task())
    s.runner_claims(owner="host-A", status=JobSchedule.STATUS_DONE,
                    function_result="Node is online")

    tasks_controller.cancel_task("task-1")

    row = s.row()
    assert row.status == JobSchedule.STATUS_DONE
    assert row.function_result == "Node is online"


def test_cancel_task_does_not_lose_handler_progress(store):
    """function_params carries multi-cycle progress (recovery_started,
    merge_started); reverting it makes the next attempt re-issue the RPC."""
    s = store(_task(function_name=JobSchedule.FN_BACKUP_RESTORE))
    s.runner_claims(owner="host-A", function_params={"recovery_started": True})

    tasks_controller.cancel_task("task-1")

    assert s.row().function_params == {"recovery_started": True}
    assert s.row().canceled is True


def test_cancel_task_flags_and_emits_the_event(store):
    s = store(_task())

    assert tasks_controller.cancel_task("task-1") is True

    assert s.row().canceled is True
    tasks_controller.tasks_events.task_canceled.assert_called_once()


def test_cancel_task_is_idempotent(store):
    s = store(_task())
    tasks_controller.cancel_task("task-1")
    tasks_controller.tasks_events.task_canceled.reset_mock()

    assert tasks_controller.cancel_task("task-1") is True

    assert s.row().canceled is True
    tasks_controller.tasks_events.task_canceled.assert_not_called()


def test_cancel_task_refuses_a_master_task(store):
    task = _task()
    task.sub_tasks = ["sub-1"]
    s = store(task)

    assert tasks_controller.cancel_task("task-1") is False
    assert s.row().canceled is False


# -- cancel_pending_node_restart_tasks --------------------------------------

def test_cancel_pending_restart_does_not_revert_concurrent_progress(store):
    """Called from set_node_status the moment a node goes ONLINE, off a bulk
    read — so its copies are stale by construction."""
    s = store(_task())
    s.runner_claims(owner="host-A", retry=3)

    assert tasks_controller.cancel_pending_node_restart_tasks("cl-1", "node-1") == 1

    row = s.row()
    assert row.canceled is True
    assert row.status == JobSchedule.STATUS_DONE
    assert row.function_result == "canceled: node back online"
    assert row.owner == "host-A"
    assert row.retry == 3


def test_cancel_pending_restart_leaves_a_finished_task_alone(store):
    """A task that reached its own outcome between the bulk read and the write
    keeps that outcome — the cancellation is moot."""
    s = store(_task())
    s.runner_claims(status=JobSchedule.STATUS_DONE,
                    function_result="Node is online")

    assert tasks_controller.cancel_pending_node_restart_tasks("cl-1", "node-1") == 0

    row = s.row()
    assert row.function_result == "Node is online"
    assert row.canceled is False


def test_cancel_pending_restart_skips_other_nodes_and_functions(store):
    s = store(
        _task(uuid="task-1", node_id="node-1"),
        _task(uuid="task-2", node_id="node-2"),
        _task(uuid="task-3", node_id="node-1", function_name=JobSchedule.FN_NODE_ADD),
    )

    assert tasks_controller.cancel_pending_node_restart_tasks("cl-1", "node-1") == 1

    assert s.row("task-1").canceled is True
    assert s.row("task-2").canceled is False
    assert s.row("task-3").canceled is False


# -- cancel_node_tasks ------------------------------------------------------

def test_cancel_node_tasks_does_not_wipe_the_lease(store):
    """Node shutdown cancels the migration tasks queued against the node, off
    the same kind of bulk read."""
    s = store(_task(function_name=JobSchedule.FN_DEV_MIG))
    s.runner_claims(owner="host-A", retry=2)

    assert tasks_controller.cancel_node_tasks(
        "cl-1", "node-1", [JobSchedule.FN_DEV_MIG]) == 1

    row = s.row()
    assert row.canceled is True
    assert row.owner == "host-A"
    assert row.status == JobSchedule.STATUS_RUNNING
    assert row.retry == 2


def test_cancel_node_tasks_leaves_a_finished_task_alone(store):
    s = store(_task(function_name=JobSchedule.FN_DEV_MIG))
    s.runner_claims(status=JobSchedule.STATUS_DONE, function_result="migrated")

    assert tasks_controller.cancel_node_tasks(
        "cl-1", "node-1", [JobSchedule.FN_DEV_MIG]) == 0

    assert s.row().canceled is False
    assert s.row().function_result == "migrated"


def test_cancel_node_tasks_skips_other_nodes_and_functions(store):
    s = store(
        _task(uuid="task-1", node_id="node-1", function_name=JobSchedule.FN_DEV_MIG),
        _task(uuid="task-2", node_id="node-1", function_name=JobSchedule.FN_NEW_DEV_MIG),
        _task(uuid="task-3", node_id="node-2", function_name=JobSchedule.FN_DEV_MIG),
        _task(uuid="task-4", node_id="node-1", function_name=JobSchedule.FN_NODE_RESTART),
    )

    assert tasks_controller.cancel_node_tasks(
        "cl-1", "node-1",
        [JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG,
         JobSchedule.FN_NEW_DEV_MIG]) == 2

    assert s.row("task-1").canceled is True
    assert s.row("task-2").canceled is True
    assert s.row("task-3").canceled is False
    assert s.row("task-4").canceled is False
