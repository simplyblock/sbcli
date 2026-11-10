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
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import FinishRequest, FinishResponse, SourceSnapshot
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
            # not a failure: True keeps main() from adding its penalty sleep
            self.assertTrue(snapshot_replication.task_runner(task))
        leader.assert_not_called()

    def test_no_source_member_waits_without_spending_retries(self):
        """No online member holds the snapshot: wait, do not spend retries
        (leadership no longer matters on the source, see _select_source_node)."""
        task = _task(retry=7)
        task.retry = 7
        snap = self._snapshot()
        snap.snap_bdev = "LVS_1/SNAP_1"
        db = MagicMock()
        db.get_snapshot_by_id.return_value = snap
        with patch.object(snapshot_replication, "db", db), \
                patch.object(snapshot_replication, "_lvs_transfer_hold", return_value=None), \
                patch.object(snapshot_replication, "_select_source_node",
                             return_value=(None, "primary n-1 offline")), \
                self.assertLogs(snapshot_replication.logger, "WARNING") as logs:
            self.assertFalse(snapshot_replication.task_runner(task))
        self.assertEqual(task.retry, 7)
        self.assertIn("no online member of LVS_1 holds LVS_1/SNAP_1", logs.output[0])

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
        """The destination answers gone: no landing volume and no copy. The
        transfer starts over on a fresh one."""
        task = _task(remote_lvol_id="r-1", transfer_done=True, converted_nodes=["n-p"], offset=5)
        peer = MagicMock()
        peer.finish.return_value = FinishResponse(state="gone", reason="landing volume r-1 is gone")
        snap = SimpleNamespace(get_id=lambda: "s-1", cluster_id="c-1", snap_name="s-1", size=1,
                               used_size=0, data_uuid="", lvol=SimpleNamespace(get_id=lambda: "lv"))
        with patch.object(snapshot_replication, "_deferred_chain_gate", return_value="ok"), \
                patch.object(snapshot_replication, "_previous_replicated_snapshot", return_value=None), \
                patch.object(snapshot_replication, "_task_peer", return_value=peer), \
                patch.object(snapshot_replication, "_suspend_for_retry") as suspend, \
                self.assertLogs(snapshot_replication.logger, "WARNING"):
            self.assertEqual(snapshot_replication.process_snap_replicate_finish(task, snap),
                             snapshot_replication.FINISH_ABANDONED)
        for key in ("transfer_done", "converted_nodes", "remote_lvol_id", "offset"):
            self.assertNotIn(key, task.function_params)
        suspend.assert_called_once()


class TestFinishSkipsConvertedNodes(unittest.TestCase):
    """The destination's finish (peer.service.finish_landing) skips what an
    earlier pass converted or chained: neither is idempotent in SPDK
    (2026-09-29, and bed run 5)."""

    def _finish(self, sec_convert_ok, converted_on=(), chained_on=(), predecessor=None,
                secondary_base=None):
        primary = MagicMock()
        primary.get_id.return_value = "n-p"
        primary.secondary_node_id = "n-s"
        primary.rpc_client.return_value.bdev_lvol_convert.return_value = True
        secondary = MagicMock()
        secondary.get_id.return_value = "n-s"
        secondary.status = "online"
        srpc = secondary.rpc_client.return_value
        srpc.bdev_lvol_convert.return_value = sec_convert_ok
        srpc.bdev_get.return_value = {"driver_specific": {"lvol": (
            {"base_snapshot": secondary_base} if secondary_base else {})}}
        landing = SimpleNamespace(top_bdev="LVS_1/LVOL_92", lvs_name="LVS_1", node_id="n-p",
                                  nodes=["n-p", "n-s"], status=LVol.STATUS_ONLINE,
                                  get_id=lambda: "r-1")

        def snapshot(snapshot_id):
            if predecessor is not None and snapshot_id == "pred":
                return predecessor
            raise KeyError(snapshot_id)

        db = MagicMock()
        db.get_snapshot_by_id.side_effect = snapshot
        db.get_lvol_by_id.return_value = landing
        db.get_storage_node_by_id.return_value = secondary
        request = FinishRequest(
            source_cluster_id="c-a", landing_lvol_id="r-1",
            snapshot=SourceSnapshot(snapshot_id="s-1", snap_name="s-1", size=1),
            predecessor_id="pred" if predecessor is not None else "",
            converted_on=list(converted_on), chained_on=list(chained_on), link="source")
        with patch.object(peer_service, "stable_leader", return_value=(primary, "")), \
                patch.object(peer_service, "require_leader", return_value=True):
            answer = peer_service.finish_landing(db, request)
        return answer, primary, secondary

    def test_failed_secondary_convert_records_the_primary(self):
        answer, primary, secondary = self._finish(sec_convert_ok=False)
        self.assertEqual(answer.state, "retry")
        primary.rpc_client.return_value.bdev_lvol_convert.assert_called_once()
        self.assertEqual(answer.converted_on, ["n-p"])

    def test_secondary_already_chained_is_not_chained_again(self):
        """Bed run 5: a landing volume chained on the primary after its
        transfer reached the secondary through the lvstore's metadata sync
        while it merged; add_clone there failed with "File exists" on every
        retry. A secondary that already reads through the base is skipped."""
        predecessor = SimpleNamespace(snap_bdev="LVS_1/LVOL_83",
                                      lvol=SimpleNamespace(nodes=["n-p", "n-s"], node_id="n-p"))
        answer, primary, secondary = self._finish(
            sec_convert_ok=False, chained_on=["n-p"], predecessor=predecessor,
            secondary_base="LVOL_83")
        secondary.rpc_client.return_value.bdev_lvol_add_clone.assert_not_called()
        secondary.rpc_client.return_value.bdev_lvol_convert.assert_called_once()
        self.assertEqual(answer.chained_on, ["n-p", "n-s"])

    def test_retry_converts_only_what_is_left(self):
        answer, primary, secondary = self._finish(sec_convert_ok=False, converted_on=["n-p"])
        self.assertEqual(answer.state, "retry")
        primary.rpc_client.return_value.bdev_lvol_convert.assert_not_called()
        secondary.rpc_client.return_value.bdev_lvol_convert.assert_called_once()


class TestReplicationStatusCountsOnlyShippedSnapshots(unittest.TestCase):

    def _info(self, snaps, tasks):
        lvol = SimpleNamespace(node_id="n-1", replication_interval_min=5, get_id=lambda: "lv-1",
                               replication_policy_id="", do_replicate=True)
        tasks_by_snap: dict = {}
        for task in tasks:
            tasks_by_snap.setdefault(task.function_params["snapshot_id"], []).append(task)
        db = MagicMock()
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = SimpleNamespace(cluster_id="c-1")
        # get_replication_info resolves the lvol's own snapshots (lvol_uuid index)
        # and each snapshot's shipping task (repl_snapshot_id index) -- no
        # whole-table task scan, so the mock serves those two reads.
        db.get_snapshots_by_lvol_id.return_value = snaps
        db.get_replication_tasks_for_snapshot.side_effect = lambda sid: tasks_by_snap.get(sid, [])
        db.get_job_tasks_by_function.return_value = []
        db.get_lvol_replication_objects.return_value = []
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
                               to_dict=dict)

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
    """Ownership is now declared once, in SPEC.function_names, and enforced
    generically by task_runner_base's dispatch loop (test_task_runner_base.py
    covers the enforcement itself)."""

    def test_snapshot_replication_is_not_owned(self):
        self.assertNotIn(JobSchedule.FN_SNAPSHOT_REPLICATION,
                          tasks_runner_backup.SPEC.function_names)

    def test_backup_family_is_owned(self):
        self.assertEqual(
            set(tasks_runner_backup.SPEC.function_names),
            {JobSchedule.FN_BACKUP, JobSchedule.FN_BACKUP_RESTORE,
             JobSchedule.FN_BACKUP_MERGE, JobSchedule.FN_GROUP_BACKUP_COMMIT,
             JobSchedule.FN_GROUP_BACKUP_DELETE},
        )


class TestNeverTransferIntoASnapshot(unittest.TestCase):

    def test_members_that_hold_the_landing_volume_as_snapshot(self):
        def node(nid, status, is_snap):
            n = MagicMock(status=status)
            n.get_id.return_value = nid
            n.rpc_client.return_value.bdev_get.return_value = \
                {"driver_specific": {"lvol": {"snapshot": is_snap}}}
            return n
        nodes = {"n-p": node("n-p", "online", True), "n-s": node("n-s", "online", False),
                 "n-t": node("n-t", "offline", True)}
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
        remote_lv = SimpleNamespace(nodes=["n-p", "n-s", "n-t"], node_id="n-p", top_bdev="LVS_1/LVOL_92")
        # read on the destination, in ensure_hub
        self.assertEqual(peer_service.converted_members(db, remote_lv), ["n-p"])


class TestRelocatedChainIsLinkedNotReshipped(unittest.TestCase):

    @staticmethod
    def _snap(sid, data, lvs, status="online", created=0, node="n-a1", cluster="c-a"):
        return SimpleNamespace(uuid=sid, data_uuid=data, status=status, created_at=created,
                               cluster_id=cluster, get_id=lambda: sid,
                               lvol=SimpleNamespace(lvs_name=lvs, node_id=node))

    # The counterpart search itself runs on the destination now (prepare_landing):
    # tests/integration/test_landing_preparation.py::TestPrepareLanding::
    # test_links_a_copy_of_the_same_data_on_the_lvstore.

    def test_replication_node_follows_the_chain(self):
        base = self._snap("s-b1", "d-1", "LVS_10", cluster="c-b")
        old_copy = self._snap("s-a0", "d-0", "LVS_7", created=1, node="n-a2")
        new_copy = self._snap("s-a1", "d-1", "LVS_1", created=5, node="n-a1")
        db = MagicMock()
        db.get_snapshots.return_value = [base, old_copy, new_copy]
        db.get_storage_node_by_id.side_effect = lambda nid: SimpleNamespace(
            status="online", get_id=lambda: nid)
        with patch.object(lvol_controller, "replication_backlog",
                          return_value=[base, SimpleNamespace(data_uuid="d-0")]):
            self.assertEqual(lvol_controller._chain_counterpart_node(db, MagicMock(), "c-a"), "n-a1")


if __name__ == "__main__":
    unittest.main()
