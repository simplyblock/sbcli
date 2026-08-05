# coding=utf-8
"""Behavioural retry-ceiling tests for the task runners.

These exercise the per-task entry points directly — now possible because each
runner's ``while True`` service loop lives behind an ``if __name__ ==
'__main__'`` guard, so importing the module only defines functions and a
(kv_store=None) ``DBController`` singleton (safe under the unit tier's stubbed
``fdb``).

The invariant under test is the basic task-scheduler semantic: a task whose
``retry`` has reached its ceiling must *terminate* (``STATUS_DONE``) instead of
looping forever, and it must not perform its side-effecting work on that final
poll. Below the ceiling the task still advances. See :mod:`test_retry_ceiling`
for the cross-runner discovery check, and
:mod:`tests.unit.tasks.test_task_runner_base` for the ceiling as enforced by the
shared driver, which the migrated runners delegate it to.
"""
from unittest.mock import MagicMock

import pytest

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

import simplyblock_core.services.tasks_runner_node_add as node_add_runner
from simplyblock_core.services.task_runner_base import TaskDefer, TaskRetry
import simplyblock_core.services.tasks_runner_restart as restart_runner


@pytest.fixture(autouse=True)
def _no_db_writes(monkeypatch):
    """Model writes would dereference the None kv_store and exit(1)."""
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())


def _task(function_name, retry, max_retry, **params):
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = function_name
    task.node_id = "node-1"
    task.device_id = "dev-1"
    task.cluster_id = "cl-1"
    task.status = JobSchedule.STATUS_NEW
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = False
    task.function_params = params
    return task


# --------------------------------------------------------------------------
# tasks_runner_node_add.process_task
#
# The ceiling itself now lives in the shared driver (see
# tests/unit/tasks/test_task_runner_base.py); what this runner still decides is
# whether a failed add counts against it at all.
# --------------------------------------------------------------------------

def test_node_add_success_completes(monkeypatch):
    sops = MagicMock()
    sops.add_node.return_value = True
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)

    task = _task(JobSchedule.FN_NODE_ADD, retry=0, max_retry=3, node_id="node-1")
    assert node_add_runner.process_task(task) is None
    sops.add_node.assert_called_once_with(node_id="node-1")


def test_node_add_failure_counts_a_retry(monkeypatch):
    """A failed add against a responsive node advances retry, so the driver's
    ceiling can eventually bind."""
    sops = MagicMock()
    sops.add_node.return_value = False
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    monkeypatch.setattr(node_add_runner, "_wait_node_reachable", lambda task: False)

    task = _task(JobSchedule.FN_NODE_ADD, retry=1, max_retry=3, node_id="node-1")
    with pytest.raises(TaskRetry):
        node_add_runner.process_task(task)


def test_node_add_failure_during_a_reboot_does_not_count(monkeypatch):
    """The CPU-topology reboot is expected, not a failure: waiting it out must
    not burn one of the task's retries."""
    sops = MagicMock()
    sops.add_node.return_value = False
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    monkeypatch.setattr(node_add_runner, "_wait_node_reachable", lambda task: True)

    task = _task(JobSchedule.FN_NODE_ADD, retry=1, max_retry=3, node_id="node-1")
    with pytest.raises(TaskDefer):
        node_add_runner.process_task(task)


def test_node_add_exception_is_handled_like_a_failed_add(monkeypatch):
    sops = MagicMock()
    sops.add_node.side_effect = RuntimeError("boom")
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    monkeypatch.setattr(node_add_runner, "_wait_node_reachable", lambda task: False)

    task = _task(JobSchedule.FN_NODE_ADD, retry=0, max_retry=3, node_id="node-1")
    with pytest.raises(TaskRetry, match="boom"):
        node_add_runner.process_task(task)


# --------------------------------------------------------------------------
# tasks_runner_restart give-up side effects
#
# The ceiling itself is the driver's (test_task_runner_base); what the runner
# still owns is what to do about the target when the ceiling terminates a task
# — a path its handler never sees, so it hangs off the driver's on_finish.
# --------------------------------------------------------------------------

def _restart_task(function_name, retry, max_retry, canceled=False):
    task = _task(function_name, retry, max_retry, node_id="node-1")
    task.canceled = canceled
    return task


def test_restart_node_give_up_marks_offline_and_requeues(monkeypatch):
    """Exhausting the retries parks the node OFFLINE and queues a fresh
    auto-restart, so it is not stranded until an operator intervenes."""
    fake_db = MagicMock()
    monkeypatch.setattr(restart_runner, "db", fake_db)
    sops = MagicMock()
    tasks_ctrl = MagicMock()
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    monkeypatch.setattr(restart_runner, "tasks_controller", tasks_ctrl)

    restart_runner.SPEC.on_finish(
        _restart_task(JobSchedule.FN_NODE_RESTART, retry=5, max_retry=5))

    sops.set_node_status.assert_called_once()
    assert sops.set_node_status.call_args.args[1] == StorageNode.STATUS_OFFLINE
    tasks_ctrl.add_node_to_auto_restart.assert_called_once()


def test_restart_node_success_leaves_the_node_alone(monkeypatch):
    """A task that finished below the ceiling succeeded — the node is online
    and must not be flipped OFFLINE by the cleanup path."""
    monkeypatch.setattr(restart_runner, "db", MagicMock())
    sops = MagicMock()
    tasks_ctrl = MagicMock()
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    monkeypatch.setattr(restart_runner, "tasks_controller", tasks_ctrl)

    restart_runner.SPEC.on_finish(
        _restart_task(JobSchedule.FN_NODE_RESTART, retry=2, max_retry=5))

    sops.set_node_status.assert_not_called()
    tasks_ctrl.add_node_to_auto_restart.assert_not_called()


def test_restart_node_unbounded_never_gives_up(monkeypatch):
    monkeypatch.setattr(restart_runner, "db", MagicMock())
    sops = MagicMock()
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    monkeypatch.setattr(restart_runner, "tasks_controller", MagicMock())

    restart_runner.SPEC.on_finish(
        _restart_task(JobSchedule.FN_NODE_RESTART, retry=100, max_retry=-1))

    sops.set_node_status.assert_not_called()


def test_restart_device_give_up_marks_unavailable_and_exhausted(monkeypatch):
    device = MagicMock()
    device.get_id.return_value = "dev-1"
    monkeypatch.setattr(restart_runner, "_get_device", lambda task: device)
    monkeypatch.setattr(restart_runner, "db", MagicMock())
    dc = MagicMock()
    monkeypatch.setattr(restart_runner, "device_controller", dc)

    restart_runner.SPEC.on_finish(_restart_task(
        JobSchedule.FN_DEV_RESTART,
        retry=constants.TASK_EXEC_RETRY_COUNT,
        max_retry=constants.TASK_EXEC_RETRY_COUNT))

    dc.device_set_unavailable.assert_called_once_with("dev-1")
    dc.device_set_retries_exhausted.assert_called_once_with("dev-1", True)
    dc.restart_device.assert_not_called()


def test_restart_device_cancel_exhausts_retries(monkeypatch):
    """A canceled device task must not be picked up and retried forever."""
    device = MagicMock()
    device.get_id.return_value = "dev-1"
    monkeypatch.setattr(restart_runner, "_get_device", lambda task: device)
    monkeypatch.setattr(restart_runner, "db", MagicMock())
    dc = MagicMock()
    monkeypatch.setattr(restart_runner, "device_controller", dc)

    restart_runner.SPEC.on_finish(_restart_task(
        JobSchedule.FN_DEV_RESTART, retry=0, max_retry=5, canceled=True))

    dc.device_set_retries_exhausted.assert_called_once_with("dev-1", True)
    dc.device_set_unavailable.assert_not_called()
