"""A snapshot-replication task whose snapshot no longer exists.

Regression: 2026-09-29, simplyblock-dr real-storage test bed (k3s, two
storage clusters under one control plane). A relocate back (the second hop of
a round trip) never completed: every PromoteVolume on the target returned 500.
The fail-over endpoint runs replicate_lvol_on_target_cluster, which runs
replication_stop first, which looked up the snapshot of every open
snapshot-replication task of the cluster -- and one task named a snapshot
that had been deleted: KeyError, 500, on every retry.

The task stayed open because task_runner never closed it: it checked
``if not snapshot`` for a missing snapshot, but get_snapshot_by_id raises
instead of returning None, so the runner failed on every attempt.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication


def _task(uuid, snapshot_id, status="new"):
    return SimpleNamespace(uuid=uuid, function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                           status=status, function_params={"snapshot_id": snapshot_id},
                           function_result="", write_to_db=MagicMock())


class TestReplicationStopSkipsOrphanTasks(unittest.TestCase):

    def test_orphan_task_does_not_fail_the_stop(self):
        lvol = MagicMock(uuid="lv-1", replication_policy_id="", node_id="n-1")
        own = SimpleNamespace(lvol=SimpleNamespace(uuid="lv-1"))
        other = SimpleNamespace(lvol=SimpleNamespace(uuid="lv-2"))
        snaps = {"s-own": own, "s-other": other}

        def get_snapshot(sid):
            if sid not in snaps:
                raise KeyError(f"Snapshot {sid} not found")
            return snaps[sid]

        db = MagicMock()
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = SimpleNamespace(cluster_id="c-1")
        db.get_job_tasks.return_value = [
            _task("t-orphan", "s-gone"), _task("t-own", "s-own"), _task("t-other", "s-other")]
        db.get_snapshot_by_id.side_effect = get_snapshot

        with patch.object(lvol_controller, "DBController", return_value=db), \
                patch.object(lvol_controller.tasks_controller, "cancel_task") as cancel:
            self.assertTrue(lvol_controller.replication_stop("lv-1", from_policy=True))
        cancel.assert_called_once_with("t-own")


class TestTaskRunnerClosesOrphanTasks(unittest.TestCase):

    def test_missing_snapshot_closes_the_task(self):
        task = _task("t-orphan", "s-gone")
        db = MagicMock()
        db.get_snapshot_by_id.side_effect = KeyError("Snapshot s-gone not found")
        with patch.object(snapshot_replication, "db", db):
            self.assertTrue(snapshot_replication.task_runner(task))
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(task.function_result, "snapshot not found")
        task.write_to_db.assert_called_once()


if __name__ == "__main__":
    unittest.main()
