# coding=utf-8
"""Per-runner tests for the runners migrated onto the shared driver.

Each migrated runner is reduced to a :class:`RunnerSpec`: a void handler that
signals through ``TaskDefer`` / ``TaskRetry`` / ``TaskAbort``, plus an
eligibility predicate. These tests pin that translation — the loop, lease and
retry mechanics themselves are covered once in ``test_task_runner_base.py``.
"""
import time
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.backup import Backup
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
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


# -- backup -----------------------------------------------------------------

@pytest.fixture
def backup_runner(monkeypatch):
    """The backup runner with its DB, events and model writes neutralised.

    Its ceiling regression (an S3 backup whose bdev_lvol_s3_backup crashed SPDK
    was re-issued every poll forever, and the backup never transitioned to
    failed) now rests on two things this exercises: the "No process" poll
    raising TaskRetry so the ceiling can bind, and finalize_resource failing the
    backup on whichever terminal path the task ends up taking.
    """
    import simplyblock_core.services.tasks_runner_backup as runner
    monkeypatch.setattr(runner, "db", MagicMock())
    monkeypatch.setattr(runner, "backup_events", MagicMock())
    monkeypatch.setattr(Backup, "write_to_db", MagicMock())
    runner.db.get_cluster_by_id.return_value.backup_timeout_seconds = 14400
    return runner


def _backup_task(function_name=JobSchedule.FN_BACKUP, **params):
    task = _task(**params)
    task.function_name = function_name
    task.date = int(time.time())
    return task


def _backup(status=Backup.STATUS_IN_PROGRESS):
    backup = Backup()
    backup.uuid = "bk-1"
    backup.status = status
    backup.snapshot_id = "snap-1"
    return backup


def test_backup_no_process_poll_is_a_retry(backup_runner):
    """The re-issue branch must consume a retry — without it the ceiling can
    never bind to this path and the backup re-issues forever."""
    backup = _backup()
    backup_runner.db.get_backup_by_id.return_value = backup

    snode = _node()
    rpc = snode.rpc_client.return_value
    rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "No process"}
    backup_runner.db.get_storage_node_by_id.return_value = snode

    with pytest.raises(trb.TaskRetry):
        backup_runner.SPEC.handler(_backup_task(backup_id="bk-1"))

    assert backup.status == Backup.STATUS_PENDING
    rpc.bdev_lvol_s3_backup.assert_not_called()  # this poll only resets state


def test_backup_completes_on_done(backup_runner):
    backup = _backup()
    backup_runner.db.get_backup_by_id.return_value = backup
    snode = _node()
    snode.rpc_client.return_value.bdev_lvol_transfer_stat.return_value = {
        "transfer_state": "Done"}
    backup_runner.db.get_storage_node_by_id.return_value = snode

    task = _backup_task(backup_id="bk-1")
    assert backup_runner.SPEC.handler(task) is None
    assert backup.status == Backup.STATUS_COMPLETED
    assert task.function_result == "Backup completed"


def test_backup_times_out(backup_runner):
    backup_runner.db.get_cluster_by_id.return_value = _cluster()
    backup_runner.db.get_cluster_by_id.return_value.backup_timeout_seconds = 10

    task = _backup_task(backup_id="bk-1")
    task.date = int(time.time()) - 3600

    with pytest.raises(trb.TaskAbort, match="timeout"):
        backup_runner.SPEC.handler(task)


def test_unfinished_backup_is_failed_when_the_task_ends(backup_runner):
    """Whichever way the task ended — ceiling, timeout, abort, cancellation —
    the backup must not be left sitting in a pending state forever."""
    backup = _backup()
    backup_runner.db.get_backup_by_id.return_value = backup

    task = _backup_task(backup_id="bk-1")
    task.function_result = "max retry reached (10/10)"
    backup_runner.SPEC.on_finish(task)

    assert backup.status == Backup.STATUS_FAILED
    assert backup.error_message == "max retry reached (10/10)"
    backup_runner.backup_events.backup_failed.assert_called_once()


def test_completed_backup_survives_the_task_ending(backup_runner):
    backup = _backup(Backup.STATUS_COMPLETED)
    backup_runner.db.get_backup_by_id.return_value = backup

    backup_runner.SPEC.on_finish(_backup_task(backup_id="bk-1"))

    assert backup.status == Backup.STATUS_COMPLETED
    backup_runner.backup_events.backup_failed.assert_not_called()


def test_unfinished_merge_leaves_the_old_backup_intact(backup_runner):
    old = _backup(Backup.STATUS_MERGING)
    backup_runner.db.get_backup_by_id.return_value = old

    backup_runner.SPEC.on_finish(
        _backup_task(JobSchedule.FN_BACKUP_MERGE, old_backup_id="bk-0"))

    assert old.status == Backup.STATUS_COMPLETED


# -- cluster_expand ---------------------------------------------------------

@pytest.fixture
def cluster_expand(monkeypatch):
    import simplyblock_core.services.tasks_runner_cluster_expand as runner
    monkeypatch.setattr(runner, "db", MagicMock())
    monkeypatch.setattr(runner, "tasks_controller", MagicMock())
    monkeypatch.setattr(runner, "integrate_new_node_into_cluster", MagicMock())
    return runner


def _expanded_cluster(runner, phase):
    cluster = MagicMock()
    cluster.expand_state = {"phase": phase}
    runner.db.get_cluster_by_id.return_value = cluster
    return cluster


def test_cluster_expand_completes_and_queues_device_migration(cluster_expand):
    from simplyblock_core.controllers.cluster_expansion.planner import EXPAND_PHASE_COMPLETED
    from simplyblock_core.models.nvme_device import NVMeDevice

    _expanded_cluster(cluster_expand, EXPAND_PHASE_COMPLETED)
    device = MagicMock()
    device.status = NVMeDevice.STATUS_ONLINE
    device.get_id.return_value = "dev-1"
    cluster_expand.db.get_storage_node_by_id.return_value.nvme_devices = [device]

    task = _task(new_node_id="new-1")
    assert cluster_expand.SPEC.handler(task) is None
    cluster_expand.tasks_controller.add_new_device_mig_task.assert_called_once_with("dev-1")
    assert task.function_result == "expansion complete: new-1"


def test_cluster_expand_rearms_an_aborted_plan(cluster_expand):
    from simplyblock_core.controllers.cluster_expansion.planner import (
        EXPAND_PHASE_ABORTED,
        EXPAND_PHASE_COMPLETED,
    )

    cluster = MagicMock()
    cluster.expand_state = {"phase": EXPAND_PHASE_ABORTED}
    completed = MagicMock()
    completed.expand_state = {"phase": EXPAND_PHASE_COMPLETED}
    cluster_expand.db.get_cluster_by_id.side_effect = [cluster, completed]
    cluster_expand.db.get_storage_node_by_id.return_value.nvme_devices = []

    cluster_expand.SPEC.handler(_task(new_node_id="new-1"))
    assert cluster.expand_state["phase"] != EXPAND_PHASE_ABORTED
    cluster.write_to_db.assert_called_once()


def test_cluster_expand_retries_an_incomplete_phase(cluster_expand):
    _expanded_cluster(cluster_expand, "in_progress")
    with pytest.raises(trb.TaskRetry, match="unexpected phase"):
        cluster_expand.SPEC.handler(_task(new_node_id="new-1"))


def test_cluster_expand_aborts_without_a_node(cluster_expand):
    with pytest.raises(trb.TaskAbort):
        cluster_expand.SPEC.handler(_task())
