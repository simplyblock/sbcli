"""Snapshot replication after the 2026-09-29 LVS_1 incident.

simplyblock-dr real-storage test bed, two storage clusters under one control
plane. A B->A transfer's finish converted its landing volume on the primary
and failed on the secondary; every retry re-sent the transfer into the now
half-converted volume, and each attempt made the target LVS drop leadership
and fence its ports (14 times in 11 minutes). Meanwhile the A->B tasks of
another volume on that LVS found no source leader, spent every retry
silently and gave up; the backup runner closed them; the status still
reported a fresh lag, so the volume looked protected, and an unplanned
fail-over found nothing on the target.
"""
import time
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_replication, tasks_runner_backup


def _task(**params):
    base = {"snapshot_id": "s-1", "replicate_to_source": False}
    base.update(params)
    return SimpleNamespace(uuid="t-1", cluster_id="c-1", node_id="n-1",
                           function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                           status=JobSchedule.STATUS_SUSPENDED, canceled=False,
                           retry=0, max_retry=8, function_params=base,
                           function_result="", write_to_db=MagicMock())


class TestSuspendForRetry(unittest.TestCase):

    def test_logs_the_reason_and_counts(self):
        task = _task()
        with self.assertLogs(snapshot_replication.logger, "WARNING") as logs:
            snapshot_replication._suspend_for_retry(task, "why")
        self.assertIn("why", logs.output[0])
        self.assertEqual(task.retry, 1)
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(task.function_params["last_error"], "why")
        self.assertNotIn("not_before", task.function_params)

    def test_waiting_does_not_spend_a_retry(self):
        task = _task()
        with self.assertLogs(snapshot_replication.logger, "WARNING"):
            snapshot_replication._suspend_for_retry(task, "no leader", backoff=True, count=False)
        self.assertEqual(task.retry, 0)
        self.assertGreater(task.function_params["not_before"], int(time.time()))


class TestTaskRunnerLeaderAndBackoff(unittest.TestCase):

    def _snapshot(self):
        return SimpleNamespace(lvol=SimpleNamespace(node_id="n-1", lvs_name="LVS_1"),
                               get_id=lambda: "s-1")

    def test_backoff_is_honoured(self):
        task = _task(not_before=int(time.time()) + 60)
        db = MagicMock()
        db.get_snapshot_by_id.return_value = self._snapshot()
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_source_leader_node") as leader:
            self.assertFalse(snapshot_replication.task_runner(task))
        leader.assert_not_called()

    def test_no_source_leader_waits_without_spending_retries(self):
        task = _task(retry=7)
        task.retry = 7
        db = MagicMock()
        db.get_snapshot_by_id.return_value = self._snapshot()
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_source_leader_node", return_value=None), \
                self.assertLogs(snapshot_replication.logger, "WARNING") as logs:
            self.assertFalse(snapshot_replication.task_runner(task))
        self.assertEqual(task.retry, 7)
        self.assertIn("no online source LVS leader for LVS_1", logs.output[0])

    def test_max_retry_keeps_the_last_reason(self):
        task = _task(last_error="transfer failed at offset 0, retrying")
        task.retry = 8
        snap = self._snapshot()
        snap.status = "online"
        db = MagicMock()
        db.get_snapshot_by_id.return_value = snap
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_source_leader_node", return_value=MagicMock()), \
                self.assertLogs(snapshot_replication.logger, "ERROR"):
            self.assertTrue(snapshot_replication.task_runner(task))
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(task.function_result,
                         "max retry reached (8/8) after: transfer failed at offset 0, retrying")


class TestResumeFinish(unittest.TestCase):

    def test_not_resumed_before_the_transfer_completed(self):
        self.assertFalse(snapshot_replication._resume_finish(_task(remote_lvol_id="r-1"), MagicMock()))

    def test_completed_transfer_is_finished_not_resent(self):
        task = _task(remote_lvol_id="r-1", transfer_done=True, converted_nodes=["n-p"])
        db = MagicMock()
        db.get_lvol_by_id.return_value = SimpleNamespace(status=LVol.STATUS_ONLINE)
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_finish_completed_transfer") as finish, \
                patch.object(snapshot_replication, "process_snap_replicate_start") as start:
            self.assertTrue(snapshot_replication._resume_finish(task, MagicMock()))
        finish.assert_called_once()
        start.assert_not_called()

    def test_gone_landing_volume_starts_over(self):
        task = _task(remote_lvol_id="r-1", transfer_done=True, converted_nodes=["n-p"], offset=5)
        db = MagicMock()
        db.get_lvol_by_id.side_effect = KeyError("r-1")
        with patch.object(snapshot_replication, "db", db), \
                self.assertLogs(snapshot_replication.logger, "WARNING"):
            self.assertFalse(snapshot_replication._resume_finish(task, MagicMock()))
        for key in ("transfer_done", "converted_nodes", "remote_lvol_id", "offset"):
            self.assertNotIn(key, task.function_params)


class TestFinishSkipsConvertedNodes(unittest.TestCase):

    def _run(self, task, sec_convert_ok):
        primary = MagicMock()
        primary.get_id.return_value = "n-p"
        primary.secondary_node_id = "n-s"
        primary.transfer_hublvol = None
        secondary = MagicMock()
        secondary.get_id.return_value = "n-s"
        secondary.status = "online"
        secondary.rpc_client.return_value.bdev_lvol_convert.return_value = sec_convert_ok
        primary.rpc_client.return_value.bdev_lvol_convert.return_value = True
        remote_lv = SimpleNamespace(top_bdev="LVS_1/LVOL_92", lvs_name="LVS_1", node_id="n-p")
        db = MagicMock()
        db.get_lvol_by_id.return_value = remote_lv
        db.get_storage_node_by_id.return_value = secondary
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_receiving_leader_node", return_value=primary), \
                patch.object(snapshot_replication, "_source_leader_node", return_value=MagicMock()), \
                patch.object(snapshot_replication, "_resolve_chain_target", return_value=(None, None, True)), \
                patch.object(snapshot_replication, "_require_lvs_leader", return_value=True), \
                patch.object(snapshot_replication, "_prechained_nodes_for", return_value=set()):
            ret = snapshot_replication.process_snap_replicate_finish(task, MagicMock())
        return ret, primary, secondary

    def test_failed_secondary_convert_records_the_primary(self):
        task = _task(remote_lvol_id="r-1")
        ret, primary, secondary = self._run(task, sec_convert_ok=False)
        self.assertFalse(ret)
        primary.rpc_client.return_value.bdev_lvol_convert.assert_called_once()
        self.assertEqual(task.function_params["converted_nodes"], ["n-p"])

    def test_retry_converts_only_what_is_left(self):
        task = _task(remote_lvol_id="r-1", converted_nodes=["n-p"])
        ret, primary, secondary = self._run(task, sec_convert_ok=False)
        self.assertFalse(ret)
        primary.rpc_client.return_value.bdev_lvol_convert.assert_not_called()
        secondary.rpc_client.return_value.bdev_lvol_convert.assert_called_once()


class TestReplicationStatusCountsOnlyShippedSnapshots(unittest.TestCase):

    def _info(self, snaps, tasks):
        lvol = SimpleNamespace(node_id="n-1", replication_interval_min=5, get_id=lambda: "lv-1")
        by_id = {s.uuid: s for s in snaps}
        db = MagicMock()
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = SimpleNamespace(cluster_id="c-1")
        db.get_job_tasks.return_value = tasks
        db.get_snapshot_by_id.side_effect = lambda sid: by_id[sid]
        for s in snaps:
            s.lvol = lvol
        with patch.object(lvol_controller, "DBController", return_value=db):
            return lvol_controller.get_replication_info("lv-1")

    @staticmethod
    def _snap(sid, age, target=""):
        return SimpleNamespace(uuid=sid, created_at=int(time.time()) - age, used_size=1,
                               target_replicated_snap_uuid=target,
                               source_replicated_snap_uuid="", get_id=lambda: sid,
                               to_dict=lambda: {"uuid": sid})

    @staticmethod
    def _t(sid, result, date):
        return SimpleNamespace(function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                               function_params={"snapshot_id": sid},
                               status=JobSchedule.STATUS_DONE, canceled=False,
                               function_result=result, date=date, updated_at="",
                               to_dict=lambda: {})

    def test_given_up_tasks_are_not_replicated(self):
        snaps = [self._snap("s-1", 600), self._snap("s-2", 300)]
        tasks = [self._t("s-1", "max retry reached (8/8)", 1),
                 self._t("s-2", "max retry reached (8/8)", 2)]
        info = self._info(snaps, tasks)
        self.assertEqual(info["replicated_count"], 0)
        self.assertIsNone(info["lag_seconds"])
        self.assertEqual(info["state"], "error")
        self.assertFalse(info["healthy"])

    def test_give_up_superseded_by_a_later_success_is_history(self):
        snaps = [self._snap("s-1", 600), self._snap("s-2", 60, target="t-2")]
        tasks = [self._t("s-1", "max retry reached (8/8)", 1),
                 self._t("s-2", str(uuid.uuid4()), 2)]
        info = self._info(snaps, tasks)
        self.assertEqual(info["replicated_count"], 1)
        self.assertEqual(info["max_retry_reached"], 0)
        self.assertNotEqual(info["state"], "error")

    def test_task_shipped(self):
        done = SimpleNamespace(status=JobSchedule.STATUS_DONE, canceled=False)
        self.assertTrue(lvol_controller._task_shipped(
            SimpleNamespace(**vars(done), function_result=str(uuid.uuid4()))))
        self.assertTrue(lvol_controller._task_shipped(SimpleNamespace(
            **vars(done), function_result="Snapshot s-1 is already replicated (remote copy t-1); nothing to transfer")))
        self.assertFalse(lvol_controller._task_shipped(
            SimpleNamespace(**vars(done), function_result="max retry reached")))


class TestBackupRunnerOwnsOnlyBackupTasks(unittest.TestCase):

    def test_other_tasks_are_left_alone(self):
        repl = SimpleNamespace(uuid="t-r", function_name=JobSchedule.FN_SNAPSHOT_REPLICATION,
                               status=JobSchedule.STATUS_SUSPENDED, canceled=False)
        backup = SimpleNamespace(uuid="t-b", function_name=JobSchedule.FN_BACKUP,
                                 status=JobSchedule.STATUS_NEW, canceled=False)
        cl = MagicMock(status="active")
        db = MagicMock()
        db.get_clusters.return_value = [cl]
        db.get_job_tasks.return_value = [repl, backup]
        db.get_task_by_id.side_effect = lambda tid: {"t-r": repl, "t-b": backup}[tid]

        class _Stop(Exception):
            pass

        with patch.object(tasks_runner_backup, "db", db), \
                patch.object(tasks_runner_backup, "process_task") as process, \
                patch.object(tasks_runner_backup.time, "sleep", side_effect=_Stop):
            with self.assertRaises(_Stop):
                tasks_runner_backup.main()
        process.assert_called_once_with(backup, cl)


if __name__ == "__main__":
    unittest.main()
