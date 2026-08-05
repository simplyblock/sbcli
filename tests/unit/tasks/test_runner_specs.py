# coding=utf-8
"""Per-runner tests for the runners migrated onto the shared driver.

Each migrated runner is reduced to a :class:`RunnerSpec`: a void handler that
signals through ``TaskDefer`` / ``TaskRetry`` / ``TaskAbort``, plus an
eligibility predicate. These tests pin that translation — the loop, lease and
retry mechanics themselves are covered once in ``test_task_runner_base.py``.
"""
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
import simplyblock_core.services.task_runner_base as trb


def _task(**params):
    task = JobSchedule()
    task.uuid = "task-1"
    task.cluster_id = "cl-1"
    task.node_id = "node-1"
    task.function_params = params
    return task


def _cluster(status=Cluster.STATUS_ACTIVE):
    cluster = MagicMock()
    cluster.status = status
    return cluster


# -- fdb_backup -------------------------------------------------------------

@pytest.fixture
def fdb_backup(monkeypatch):
    import simplyblock_core.services.tasks_runner_fdb_backup as runner
    monkeypatch.setattr(runner, "fdb_backup_controller", MagicMock())
    return runner


def test_fdb_backup_success_reports_result(fdb_backup):
    fdb_backup.fdb_backup_controller.create_backup.return_value = True
    task = _task()

    assert fdb_backup.SPEC.handler(task) is None
    fdb_backup.fdb_backup_controller.create_backup.assert_called_once_with("cl-1")
    assert task.function_result == "Backup created"


def test_fdb_backup_failure_is_retryable(fdb_backup):
    fdb_backup.fdb_backup_controller.create_backup.return_value = False

    with pytest.raises(trb.TaskRetry):
        fdb_backup.SPEC.handler(_task())


def test_fdb_backup_skips_clusters_in_activation(fdb_backup):
    task = _task()
    assert fdb_backup.SPEC.is_eligible(task, _cluster()) is True
    assert fdb_backup.SPEC.is_eligible(task, _cluster(Cluster.STATUS_IN_ACTIVATION)) is False
