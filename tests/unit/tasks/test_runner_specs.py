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
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.release_upgrades import jc_compression_upgrade
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


# -- jc_comp ----------------------------------------------------------------

def _node(status=StorageNode.STATUS_ONLINE, jm_vuid=7):
    node = MagicMock()
    node.status = status
    node.jm_vuid = jm_vuid
    node.cluster_id = "cl-1"
    return node


@pytest.fixture
def jc_comp(monkeypatch):
    import simplyblock_core.services.tasks_runner_jc_comp as runner
    db = MagicMock()
    monkeypatch.setattr(runner, "db", db)
    monkeypatch.setattr(runner, "tasks_controller", MagicMock())
    runner.tasks_controller.get_active_node_tasks.return_value = []
    db.get_storage_nodes_by_cluster_id.return_value = [_node()]
    # An unstubbed MagicMock attribute auto-creates, so `release_upgrade_state`
    # would answer .get() with a truthy mock and hold every resume.
    db.get_cluster_by_id.return_value.release_upgrade_state = {}
    return runner


def test_jc_comp_resumes_compression(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    node.rpc_client.return_value.jc_suspend_compression.return_value = (True, None)
    task = _task()

    assert jc_comp.SPEC.handler(task) is None
    node.rpc_client.return_value.jc_suspend_compression.assert_called_once_with(
        jm_vuid=7, suspend=False)
    assert task.function_result == "JC 7 compression resumed on node"


def test_jc_comp_prefers_the_task_jm_vuid(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    node.rpc_client.return_value.jc_suspend_compression.return_value = (True, None)

    jc_comp.SPEC.handler(_task(jm_vuid=42))
    node.rpc_client.return_value.jc_suspend_compression.assert_called_once_with(
        jm_vuid=42, suspend=False)


def test_jc_comp_aborts_on_missing_node(jc_comp):
    jc_comp.db.get_storage_node_by_id.side_effect = KeyError("node-1")
    with pytest.raises(trb.TaskAbort):
        jc_comp.SPEC.handler(_task())


def test_jc_comp_defers_while_the_release_upgrade_holds_resumes(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    jc_comp.db.get_cluster_by_id.return_value.release_upgrade_state = {
        jc_compression_upgrade.STATE_KEY: True,
    }

    with pytest.raises(trb.TaskDefer):
        jc_comp.SPEC.handler(_task())
    node.rpc_client.assert_not_called()


def test_jc_comp_defers_while_node_is_offline(jc_comp):
    jc_comp.db.get_storage_node_by_id.return_value = _node(StorageNode.STATUS_OFFLINE)
    with pytest.raises(trb.TaskDefer):
        jc_comp.SPEC.handler(_task())


def test_jc_comp_defers_while_another_task_runs_on_the_node(jc_comp):
    jc_comp.db.get_storage_node_by_id.return_value = _node()
    jc_comp.tasks_controller.get_active_node_tasks.return_value = [_task()]
    with pytest.raises(trb.TaskDefer):
        jc_comp.SPEC.handler(_task())


def test_jc_comp_defers_unless_every_cluster_node_is_online(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    jc_comp.db.get_storage_nodes_by_cluster_id.return_value = [
        _node(StorageNode.STATUS_OFFLINE), node,
    ]

    with pytest.raises(trb.TaskDefer):
        jc_comp.SPEC.handler(_task())
    node.rpc_client.assert_not_called()


def test_jc_comp_aborts_when_compression_is_not_needed(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    node.rpc_client.return_value.jc_suspend_compression.return_value = (False, "not needed")

    with pytest.raises(trb.TaskAbort):
        jc_comp.SPEC.handler(_task())


def test_jc_comp_retries_when_resume_fails(jc_comp):
    node = _node()
    jc_comp.db.get_storage_node_by_id.return_value = node
    node.rpc_client.return_value.jc_suspend_compression.return_value = (False, None)

    with pytest.raises(trb.TaskRetry):
        jc_comp.SPEC.handler(_task())
