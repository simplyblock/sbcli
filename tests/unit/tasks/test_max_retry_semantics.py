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
poll. Below the ceiling the task still advances. See
:mod:`test_retry_ceiling` for the backup runner's version of the same guard and
the static cross-runner check.
"""
from unittest.mock import MagicMock

import pytest

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

import simplyblock_core.services.tasks_runner_node_add as node_add_runner
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
# tasks_runner_node_add.task_runner
# --------------------------------------------------------------------------

def test_node_add_max_retry_finishes_without_adding(monkeypatch):
    """retry >= max_retry must finish the task and never call add_node."""
    sops = MagicMock()
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    monkeypatch.setattr(node_add_runner, "db", MagicMock())

    task = _task(JobSchedule.FN_NODE_ADD, retry=3, max_retry=3, node_id="node-1")
    res = node_add_runner.task_runner(task, MagicMock())

    assert res is True
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result
    sops.add_node.assert_not_called()


def test_node_add_below_ceiling_dispatches(monkeypatch):
    """Below the ceiling the task still runs its step (add_node)."""
    sops = MagicMock()
    sops.add_node.return_value = True
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    # get_cluster_by_id().status is a Mock != STATUS_IN_ACTIVATION, so the
    # in-activation gate is not taken and the task proceeds to add_node.
    monkeypatch.setattr(node_add_runner, "db", MagicMock())

    task = _task(JobSchedule.FN_NODE_ADD, retry=0, max_retry=3, node_id="node-1")
    res = node_add_runner.task_runner(task, MagicMock())

    assert res is True
    sops.add_node.assert_called_once_with(node_id="node-1")
    assert task.status == JobSchedule.STATUS_DONE


def test_node_add_failure_below_ceiling_suspends_and_counts_retry(monkeypatch):
    """A failed add below the ceiling suspends and advances retry (so the
    ceiling can eventually bind), rather than finishing the task."""
    sops = MagicMock()
    sops.add_node.return_value = False
    monkeypatch.setattr(node_add_runner, "storage_node_ops", sops)
    monkeypatch.setattr(node_add_runner, "db", MagicMock())

    task = _task(JobSchedule.FN_NODE_ADD, retry=1, max_retry=3, node_id="node-1")
    res = node_add_runner.task_runner(task, MagicMock())

    assert res is True  # processed; the loop keeps polling
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 2


# --------------------------------------------------------------------------
# tasks_runner_restart.task_runner_node
# --------------------------------------------------------------------------

def test_restart_node_max_retry_finishes_and_marks_offline(monkeypatch):
    """retry >= max_retry finishes the task, flips the node OFFLINE via the
    restart_cleanup path, and re-queues a fresh auto-restart."""
    node = MagicMock()
    node.status = StorageNode.STATUS_ONLINE
    fake_db = MagicMock()
    fake_db.get_storage_node_by_id.return_value = node
    monkeypatch.setattr(restart_runner, "db", fake_db)

    sops = MagicMock()
    tasks_ctrl = MagicMock()
    monkeypatch.setattr(restart_runner, "storage_node_ops", sops)
    monkeypatch.setattr(restart_runner, "tasks_controller", tasks_ctrl)

    task = _task(JobSchedule.FN_NODE_RESTART, retry=5, max_retry=5, node_id="node-1")
    res = restart_runner.task_runner_node(task)

    assert res is True
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result
    sops.set_node_status.assert_called_once()
    assert sops.set_node_status.call_args.args[1] == StorageNode.STATUS_OFFLINE
    tasks_ctrl.add_node_to_auto_restart.assert_called_once()


# --------------------------------------------------------------------------
# tasks_runner_restart.task_runner_device
# --------------------------------------------------------------------------

def test_restart_device_max_retry_finishes_and_exhausts(monkeypatch):
    """retry >= TASK_EXEC_RETRY_COUNT finishes the task and marks the device
    unavailable / retries-exhausted instead of restarting it again."""
    device = MagicMock()
    device.get_id.return_value = "dev-1"
    monkeypatch.setattr(restart_runner, "_get_device", lambda task: device)
    monkeypatch.setattr(restart_runner, "db", MagicMock())

    dc = MagicMock()
    monkeypatch.setattr(restart_runner, "device_controller", dc)

    task = _task(
        JobSchedule.FN_DEV_RESTART,
        retry=constants.TASK_EXEC_RETRY_COUNT,
        max_retry=constants.TASK_EXEC_RETRY_COUNT,
        node_id="node-1",
    )
    res = restart_runner.task_runner_device(task)

    assert res is True
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result
    dc.device_set_unavailable.assert_called_once_with("dev-1")
    dc.device_set_retries_exhausted.assert_called_once_with("dev-1", True)
    dc.restart_device.assert_not_called()
