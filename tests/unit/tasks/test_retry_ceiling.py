# coding=utf-8
"""Regression tests for the backup task runner's retry ceiling, plus a
cross-runner guard that keeps the ceiling from being dropped in any runner.

Background: an S3 backup whose ``bdev_lvol_s3_backup`` RPC crashed SPDK was
re-issued every poll cycle forever. ``tasks_runner_backup`` incremented
``task.retry`` (to 21 in the incident) but never compared it against
``task.max_retry`` (10), so the task stayed ``suspended`` and the backup never
transitioned to ``failed`` — only a 4h time-based timeout could end it. Every
other task runner enforces ``task.retry >= <max>``; the backup runner did not.
"""
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.backup import Backup
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode


# --------------------------------------------------------------------------
# Behavioural tests: the backup runner terminates instead of looping forever.
# --------------------------------------------------------------------------

@pytest.fixture
def runner(monkeypatch):
    """Import the backup runner with its FDB writes and events neutralised.

    The module's ``while True`` loop is guarded by ``if __name__ ==
    '__main__'``, so importing it only defines functions and a (kv_store=None)
    ``DBController`` singleton — safe under the unit tier's stubbed ``fdb``.
    """
    import simplyblock_core.services.tasks_runner_backup as runner
    monkeypatch.setattr(runner, "backup_events", MagicMock())
    # Model writes would otherwise dereference the None kv_store and exit(1).
    monkeypatch.setattr(Backup, "write_to_db", MagicMock())
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())
    return runner


def _backup_task(retry, max_retry):
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = JobSchedule.FN_BACKUP
    task.function_params = {"backup_id": "bk-1"}
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = False
    # Recent enough that the 4h time-based timeout does not fire first.
    task.date = int(__import__("time").time())
    return task


def _cluster():
    cl = MagicMock()
    cl.backup_timeout_seconds = 14400
    return cl


def test_backup_fails_when_max_retry_reached(runner, monkeypatch):
    """retry >= max_retry must fail the backup and finish the task, not poll."""
    backup = Backup()
    backup.uuid = "bk-1"
    backup.status = Backup.STATUS_IN_PROGRESS

    fake_db = MagicMock()
    fake_db.get_backup_by_id.return_value = backup
    monkeypatch.setattr(runner, "db", fake_db)
    # If the ceiling is missing this would run and (in the incident) re-issue
    # the crash-inducing RPC; assert it is never reached.
    run_backup = MagicMock()
    monkeypatch.setattr(runner, "_run_backup", run_backup)

    task = _backup_task(retry=10, max_retry=10)
    runner.process_task(task, _cluster())

    assert backup.status == Backup.STATUS_FAILED
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result
    run_backup.assert_not_called()


def test_backup_runs_below_max_retry(runner, monkeypatch):
    """Below the ceiling the task still advances (dispatches its step)."""
    monkeypatch.setattr(runner, "db", MagicMock())
    run_backup = MagicMock()
    monkeypatch.setattr(runner, "_run_backup", run_backup)

    task = _backup_task(retry=9, max_retry=10)
    runner.process_task(task, _cluster())

    run_backup.assert_called_once_with(task)


def test_no_process_poll_counts_as_a_retry(runner, monkeypatch):
    """The 'No process' re-issue branch must advance task.retry, otherwise the
    ceiling can never bind to that path and the backup re-issues forever."""
    backup = Backup()
    backup.uuid = "bk-1"
    backup.status = Backup.STATUS_IN_PROGRESS
    backup.snapshot_id = "snap-1"

    snode = MagicMock()
    snode.status = StorageNode.STATUS_ONLINE
    rpc = MagicMock()
    rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "No process"}
    snode.rpc_client.return_value = rpc

    snapshot = MagicMock()
    snapshot.snap_bdev = "lvs/snap0"

    fake_db = MagicMock()
    fake_db.get_backup_by_id.return_value = backup
    fake_db.get_storage_node_by_id.return_value = snode
    fake_db.get_snapshot_by_id.return_value = snapshot
    monkeypatch.setattr(runner, "db", fake_db)

    task = _backup_task(retry=3, max_retry=10)
    runner._run_backup(task)

    assert task.retry == 4, "No-process re-issue must count toward the ceiling"
    assert backup.status == Backup.STATUS_PENDING
    rpc.bdev_lvol_s3_backup.assert_not_called()  # this poll only resets state


# --------------------------------------------------------------------------
# Cross-runner guard: every retry-driven runner must enforce a ceiling.
# --------------------------------------------------------------------------

# Runners that increment task.retry but are intentionally UNBOUNDED: the
# migration family is created with max_retry=-1 and gates retries on resource
# recovery (see _migration_retry_allowed) rather than a fixed count. If you add
# a runner here, say why — an unbounded task-retry loop is what caused the
# backup outage this test guards against.
INTENTIONALLY_UNBOUNDED = {
    "tasks_runner_migration.py",
    "tasks_runner_failed_migration.py",
    "tasks_runner_new_dev_migration.py",
    "tasks_runner_lvol_migration.py",
}

_INCREMENTS_RETRY = re.compile(r"\.retry\s*\+=\s*1")
_ENFORCES_CEILING = re.compile(r"\.retry\s*>=")


def _runner_files():
    import simplyblock_core.services as services_pkg
    services_dir = Path(services_pkg.__file__).parent
    files = sorted(services_dir.glob("tasks_runner_*.py"))
    assert files, f"no task runners found under {services_dir}"
    return files


def test_all_task_runners_enforce_a_retry_ceiling():
    offenders = []
    for path in _runner_files():
        src = path.read_text()
        if not _INCREMENTS_RETRY.search(src):
            continue  # not a retry-driven runner (e.g. policy/periodic service)
        if path.name in INTENTIONALLY_UNBOUNDED:
            continue
        if not _ENFORCES_CEILING.search(src):
            offenders.append(path.name)

    assert not offenders, (
        "Task runner(s) increment task.retry without enforcing a retry ceiling "
        f"({offenders}). Add a `task.retry >= task.max_retry` guard like the "
        "other runners so a repeatedly-failing task fails instead of looping "
        "forever. If the task is intentionally unbounded (max_retry=-1), add it "
        "to INTENTIONALLY_UNBOUNDED with a reason."
    )


def test_unbounded_allowlist_is_not_stale():
    """Renaming/removing an allowlisted runner must force revisiting the list."""
    names = {p.name for p in _runner_files()}
    missing = INTENTIONALLY_UNBOUNDED - names
    assert not missing, f"allowlisted runners no longer exist: {missing}"
