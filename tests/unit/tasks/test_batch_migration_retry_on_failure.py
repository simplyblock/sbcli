"""Unit tests for _attempt_batch_migration_retry in tasks_runner_batch_migration.py.

Mirrors tests/integration/migration/test_unit_runner_helpers.py's
TestAttemptMigrationRetry for the solo path -- these are pure logic tests
with the DB and migration_controller mocked, so they belong in the unit
tier rather than integration/migration (which provisions a real FDB).
"""
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup

import simplyblock_core.services.tasks_runner_batch_migration as runner


def _task(status=JobSchedule.STATUS_RUNNING):
    t = JobSchedule()
    t.uuid = "task-1"
    t.status = status
    t.function_result = ""
    t.retry = 0
    t.write_to_db = MagicMock()
    return t


def _failed_group(completed_seconds_ago):
    g = LVolMigrationGroup()
    g.uuid = "group-1"
    g.status = LVolMigrationGroup.STATUS_FAILED
    g.error_message = "target node offline"
    g.target_node_id = "node-tgt"
    g.ctrl_loss_tmo = 1234
    g.host_nqn = "nqn.host"
    g.deadline_seconds = 999
    g.completed_at = int(time.time()) - completed_seconds_ago
    g.members = [{"ns_id": 1, "migration_id": "worker-1"}]
    g.write_to_db = MagicMock()
    return g


def _leader_migration():
    m = LVolMigration()
    m.uuid = "worker-1"
    m.lvol_id = "lvol-1"
    m.max_retries = 7
    return m


class TestAttemptBatchMigrationRetry(unittest.TestCase):

    def test_waits_out_the_full_window_before_first_attempt(self):
        task = _task()
        group = _failed_group(completed_seconds_ago=10)

        mock_db = MagicMock()
        with patch.object(runner, 'db', mock_db), \
             patch.object(runner, 'migration_controller') as mock_mc:
            result = runner._attempt_batch_migration_retry(task, group)

        assert result is False
        assert task.status == JobSchedule.STATUS_SUSPENDED
        assert 'retry_on_failure_next_attempt_at' in task.function_params
        mock_mc.create_batch_migration.assert_not_called()
        mock_mc.start_batch_migration.assert_not_called()

    def test_preconditions_not_met_reschedules_without_crashing(self):
        task = _task()
        group = _failed_group(completed_seconds_ago=9999)

        mock_db = MagicMock()
        mock_db.get_migration_by_id.return_value = _leader_migration()
        with patch.object(runner, 'db', mock_db), \
             patch.object(runner, 'migration_controller') as mock_mc:
            mock_mc.create_batch_migration.side_effect = PreconditionError("cluster is rebalancing")
            result = runner._attempt_batch_migration_retry(task, group)

        assert result is False
        assert task.status == JobSchedule.STATUS_SUSPENDED
        assert "preconditions not met yet" in task.function_result
        assert task.function_params['retry_on_failure_next_attempt_at'] > time.time()

    def test_successful_restart_repoints_task_at_new_group(self):
        task = _task()
        task.retry = 3
        group = _failed_group(completed_seconds_ago=9999)

        mock_db = MagicMock()
        mock_db.get_migration_by_id.return_value = _leader_migration()
        with patch.object(runner, 'db', mock_db), \
             patch.object(runner, 'migration_controller') as mock_mc:
            mock_mc.create_batch_migration.return_value = ("group-2", [])
            mock_mc.start_batch_migration.return_value = "group-2"
            result = runner._attempt_batch_migration_retry(task, group)

        assert result is False
        mock_mc.create_batch_migration.assert_called_once_with(
            "lvol-1", "node-tgt", ctrl_loss_tmo=1234, host_nqn="nqn.host")
        mock_mc.start_batch_migration.assert_called_once_with(
            "group-2", max_retries=7, deadline_seconds=999, retry_on_failure=True)
        assert task.function_params['group_id'] == "group-2"
        assert 'retry_on_failure_next_attempt_at' not in task.function_params
        assert task.status == JobSchedule.STATUS_NEW
        assert task.retry == 0


if __name__ == "__main__":
    unittest.main()
