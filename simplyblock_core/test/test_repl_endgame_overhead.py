"""Per-round orchestration overhead of the cutover endgame.

Bed bf12 (2026-11-03, heavy fio on 8 volumes): every convergence round took
~21 s and never shrank. Of that, ~12 s passed between taking the shrink
snapshot and its transfer starting (the snapshot-replication runner only
picked the task up on its next pass, TASK_EXEC_INTERVAL_SEC later), 0.7-2.9 s
went to creating the round's landing volume, and the hub was detached and
re-attached every round. With rounds costing T = O + rho*T_prev, O ~ 14 s and
rho ~ 0.31, the fixed point is ~20 s whatever the number of rounds. Then the
operator preconnect (21-101 s) ran AFTER the last round, so the frozen final
step had to copy all of it.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_replication_final as final
from simplyblock_core.services.task_runner_base import TaskDefer, TaskRetry


def _add(lst, item, ret):
    """Append and return *ret*: a side_effect that records its calls."""
    lst.append(item)
    return ret


def _final(uuid="F1", status=JobSchedule.STATUS_RUNNING, canceled=False, **params):
    t = JobSchedule()
    t.uuid = uuid
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = status
    t.canceled = canceled
    t.retry = 0
    t.max_retry = 8
    t.function_params = {"lvol_id": "LV1"}
    t.function_params.update(params)
    return t


def _repl(uuid, snap_id, status=JobSchedule.STATUS_NEW):
    t = JobSchedule()
    t.uuid = uuid
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.status = status
    t.canceled = False
    t.function_params = {"snapshot_id": snap_id}
    return t


class _TaskDB:
    """Just enough of the task table for the runner loop."""

    def __init__(self, tasks):
        self.tasks = {t.uuid: t for t in tasks}

    def get_clusters(self):
        return [type("C", (), {"get_id": lambda s=None: "CL"})()]

    def get_job_tasks(self, cluster_id, reverse=False):
        return list(self.tasks.values())

    def get_task_by_id(self, uuid):
        if uuid not in self.tasks:
            raise KeyError(uuid)
        return self.tasks[uuid]


# --------------------------------------------------------------------------- #
# Fix 1: a shrink snapshot's transfer starts at once, not on the next pass
# --------------------------------------------------------------------------- #

class TestWhichCutoversAreWatched(unittest.TestCase):

    def test_a_claimed_cutover_before_its_clone_is_watched(self):
        self.assertTrue(sr._converging_cutover(_final(cutover_lvs="LVS_1")))

    def test_a_queued_cutover_holds_nothing_and_is_not_watched(self):
        self.assertFalse(sr._converging_cutover(_final()))

    def test_a_cutover_past_its_rounds_is_not_watched(self):
        self.assertFalse(sr._converging_cutover(
            _final(cutover_lvs="LVS_1", tgt_lvol_composite="lvs/LVOL")))

    def test_an_early_prepared_cutover_converges_until_the_freeze(self):
        self.assertTrue(sr._converging_cutover(
            _final(cutover_lvs="LVS_1", tgt_lvol_composite="lvs/LVOL",
                   early_prepared=True)))

    def test_finished_cancelled_and_foreign_tasks_are_not_watched(self):
        self.assertFalse(sr._converging_cutover(
            _final(status=JobSchedule.STATUS_DONE, cutover_lvs="LVS_1")))
        self.assertFalse(sr._converging_cutover(
            _final(canceled=True, cutover_lvs="LVS_1")))
        self.assertFalse(sr._converging_cutover(_repl("R1", "S1")))


class TestEndgameWake(unittest.TestCase):

    def setUp(self):
        self.cut = _final(cutover_lvs="LVS_1", shrink_snap_id="S1")
        self.db = _TaskDB([self.cut, _repl("R1", "S1")])
        patch.object(sr, "db", self.db).start()
        self.addCleanup(patch.stopall)

    def test_refresh_reports_the_rounds_in_flight(self):
        wake = sr._EndgameWake()
        self.assertEqual(wake.refresh("CL", self.db.get_job_tasks("CL")), {"S1"})
        self.assertTrue(wake.watching())

    def test_a_new_shrink_snapshot_wakes_once(self):
        wake = sr._EndgameWake()
        wake.refresh("CL", self.db.get_job_tasks("CL"))
        self.assertEqual(wake.poll(), {}, "the round the pass dispatches is not new")
        self.cut.function_params["shrink_snap_id"] = "S2"
        self.assertEqual(wake.poll(), {"CL": {"S2"}})
        self.assertEqual(wake.poll(), {}, "a snapshot is handed over once")

    def test_a_cutover_that_reached_its_freeze_is_dropped(self):
        wake = sr._EndgameWake()
        wake.refresh("CL", self.db.get_job_tasks("CL"))
        self.cut.function_params["tgt_lvol_composite"] = "lvs/LVOL"
        self.cut.function_params["shrink_snap_id"] = "S2"
        self.assertEqual(wake.poll(), {})
        self.assertFalse(wake.watching())

    def test_the_wake_reads_task_rows_not_the_task_table(self):
        wake = sr._EndgameWake()
        wake.refresh("CL", self.db.get_job_tasks("CL"))
        with patch.object(self.db, "get_job_tasks",
                          side_effect=AssertionError("table scan")):
            wake.poll()

    def test_the_pass_starts_with_the_endgame_snapshot(self):
        tasks = [_repl("R0", "S0"), _repl("R9", "S9"), _repl("R1", "S1")]
        self.assertEqual([t.uuid for t in sr._endgame_first(tasks, {"S1"})],
                         ["R1", "R0", "R9"])
        self.assertEqual([t.uuid for t in sr._endgame_first(tasks, set())],
                         ["R0", "R9", "R1"])


class TestPickupLatency(unittest.TestCase):
    """The idle wait between passes serves a new shrink snapshot within one
    REPL_ENDGAME_WAKE_POLL_SEC instead of TASK_EXEC_INTERVAL_SEC."""

    def setUp(self):
        self.now = [1000.0]
        patch.object(sr.time, "time", lambda: self.now[0]).start()
        self.slept: list = []

        def _sleep(s):
            self.slept.append(s)
            self.now[0] += s
            if self.now[0] - 1000.0 >= 0.5:
                # the cutover takes its next snapshot half a second in
                self.cut.function_params["shrink_snap_id"] = "S2"
        patch.object(sr.time, "sleep", _sleep).start()
        self.cut = _final(cutover_lvs="LVS_1", shrink_snap_id="S1")
        self.db = _TaskDB([self.cut, _repl("R1", "S1", JobSchedule.STATUS_DONE),
                           _repl("R2", "S2")])
        patch.object(sr, "db", self.db).start()
        self.dispatched: list = []
        patch.object(sr, "_dispatch",
                     side_effect=lambda t, *a: _add(
                         self.dispatched, (t.uuid, self.now[0] - 1000.0), True)).start()
        self.addCleanup(patch.stopall)

    def test_the_next_round_is_dispatched_within_a_poll_interval(self):
        wake = sr._EndgameWake()
        wake.refresh("CL", self.db.get_job_tasks("CL"))
        sr._wait(constants.TASK_EXEC_INTERVAL_SEC, wake)
        self.assertEqual(len(self.dispatched), 1)
        uuid, at = self.dispatched[0]
        self.assertEqual(uuid, "R2")
        self.assertLessEqual(at - 0.5, constants.REPL_ENDGAME_WAKE_POLL_SEC + 1e-9)
        self.assertLess(constants.REPL_ENDGAME_WAKE_POLL_SEC, 1.0)

    def test_without_a_converging_cutover_the_wait_is_one_plain_sleep(self):
        wake = sr._EndgameWake()
        sr._wait(constants.TASK_EXEC_INTERVAL_SEC, wake)
        self.assertEqual(self.slept, [constants.TASK_EXEC_INTERVAL_SEC])
        self.assertEqual(self.dispatched, [])


class TestRunnerPass(unittest.TestCase):

    def test_a_pass_dispatches_the_endgame_round_first(self):
        class _Stop(Exception):
            pass
        cut = _final(cutover_lvs="LVS_1", shrink_snap_id="S1")
        db = _TaskDB([_repl("R0", "S0"), cut, _repl("R1", "S1")])
        order: list = []
        with patch.object(sr, "db", db), \
                patch.object(sr.constants, "REPL_RUNNER_WORKERS", 0), \
                patch.object(sr, "task_runner",
                             side_effect=lambda t: _add(order, t.uuid, True)), \
                patch.object(sr.time, "sleep", side_effect=_Stop()):
            with self.assertRaises(_Stop):
                sr.main()
        self.assertEqual(order, ["R1", "R0"])


# --------------------------------------------------------------------------- #
# Fix 2: hub kept for the endgame; next landing volume created ahead
# --------------------------------------------------------------------------- #

class TestHubKeptDuringEndgame(unittest.TestCase):

    def _finish(self, owns):
        snap = MagicMock()
        snap.get_id.return_value = "S1"
        snap.lvol.get_id.return_value = "LV1"
        task = MagicMock()
        task.cluster_id = "CL"
        task.function_params = {"remote_lvol_id": "RL1", "target_node_id": "T1",
                                "replicate_to_source": False}
        src = MagicMock()
        tgt = MagicMock()
        tgt.get_id.return_value = "T1"
        with patch.object(sr, "db") as db, \
                patch.object(sr, "_stable_target_leader", return_value=(tgt, "")), \
                patch.object(sr, "_transfer_source_node", return_value=src), \
                patch.object(sr, "_attached_hub_ctrl", return_value="lvs/transferhub"), \
                patch.object(sr, "_cutover_owns", return_value=owns), \
                patch.object(sr, "_other_active_transfers_to_node", return_value=False), \
                patch.object(sr, "_deferred_chain_gate", return_value="pending"):
            db.get_lvol_by_id.return_value = MagicMock()
            self.assertEqual(sr.process_snap_replicate_finish(task, snap),
                             sr.FINISH_PENDING)
        return src.rpc_client.return_value.bdev_nvme_detach_controller

    def test_a_converging_cutover_keeps_its_hub_attached(self):
        self._finish(owns=True).assert_not_called()

    def test_ordinary_replication_still_detaches_as_the_last_user(self):
        self._finish(owns=False).assert_called_once_with("lvs/transferhub")


class TestLandingPrestage(unittest.TestCase):

    def setUp(self):
        self.snap = MagicMock()
        self.snap.get_id.return_value = "S1"
        self.snap.cluster_id = "CL"
        self.snap.lvol.get_id.return_value = "LV1"
        self.remote_lv = MagicMock()
        self.remote_lv.node_id = "T1"
        self.remote_lv.pool_uuid = "P1"
        self.db = patch.object(sr, "db").start()
        self.db.get_lvol_by_name.side_effect = KeyError("none")
        self.create = patch.object(sr, "_create_landing_volume",
                                   return_value=("LAND2", None)).start()
        self.delete = patch.object(sr.lvol_controller, "delete_lvol").start()
        self.addCleanup(patch.stopall)

    def _names(self, *names):
        patch.object(sr, "_endgame_next_snap_name", side_effect=list(names)).start()

    def test_the_next_rounds_landing_volume_is_created_under_its_name(self):
        self._names("repl_commit_next", "repl_commit_next")
        self.assertEqual(sr._prestage_next_landing(self.snap, self.remote_lv), "LAND2")
        name, snap, _node, pool = self.create.call_args[0]
        self.assertEqual(name, "REP_repl_commit_next")
        self.assertIs(snap, self.snap)
        self.assertEqual(pool, "P1")
        self.delete.assert_not_called()

    def test_nothing_is_staged_outside_a_converging_cutover(self):
        self._names("")
        self.assertIsNone(sr._prestage_next_landing(self.snap, self.remote_lv))
        self.create.assert_not_called()

    def test_an_already_staged_volume_is_not_created_twice(self):
        self._names("repl_commit_next")
        self.db.get_lvol_by_name.side_effect = None
        self.db.get_lvol_by_name.return_value = MagicMock()
        self.assertIsNone(sr._prestage_next_landing(self.snap, self.remote_lv))
        self.create.assert_not_called()

    def test_a_volume_staged_after_the_rounds_ended_is_deleted(self):
        self._names("repl_commit_next", "")
        self.assertIsNone(sr._prestage_next_landing(self.snap, self.remote_lv))
        self.delete.assert_called_once()

    def test_a_failure_never_breaks_the_transfer(self):
        self._names("repl_commit_next")
        self.create.side_effect = RuntimeError("boom")
        self.assertIsNone(sr._prestage_next_landing(self.snap, self.remote_lv))


class TestShrinkSnapshotNames(unittest.TestCase):

    def setUp(self):
        self.added: list = []

        def _add(lvol, name, **kw):
            self.added.append(name)
            return "S%d" % len(self.added), None
        patch("simplyblock_core.controllers.replication_delta.add_snapshot",
              side_effect=_add).start()
        self.addCleanup(patch.stopall)

    def test_each_round_names_the_next_one(self):
        task = _final(shrink_round=0).frozen_view()
        lvol = MagicMock()
        lvol.get_id.return_value = "LV1"
        _, err, task = final._take_shrink_snapshot(task, lvol)
        self.assertIsNone(err)
        nxt = task.function_params["next_shrink_snap_name"]
        self.assertTrue(nxt.startswith("repl_commit_"))
        _, err, task = final._take_shrink_snapshot(task, lvol)
        self.assertEqual(self.added[1], nxt,
                         "the round must use the name its landing volume was staged under")
        self.assertNotEqual(task.function_params["next_shrink_snap_name"], nxt)


class TestSpareLandingDiscard(unittest.TestCase):

    def setUp(self):
        self.db = patch.object(final, "db").start()
        self.delete = patch("simplyblock_core.controllers.lvol_controller.delete_lvol").start()
        self.addCleanup(patch.stopall)

    def test_the_unused_spare_is_deleted_and_forgotten(self):
        task = _final(next_shrink_snap_name="repl_commit_x")
        self.db.get_task_by_id.return_value = task
        spare = MagicMock()
        spare.status = "online"
        self.db.get_lvol_by_name.return_value = spare
        task = final._discard_spare_landing(task.frozen_view())
        self.db.get_lvol_by_name.assert_called_once_with("REP_repl_commit_x")
        self.delete.assert_called_once_with(spare, force_delete=True)
        self.assertNotIn("next_shrink_snap_name", task.function_params)

    def test_no_name_no_work(self):
        task = _final()
        self.db.get_task_by_id.return_value = task
        final._discard_spare_landing(task.frozen_view())
        self.db.get_lvol_by_name.assert_not_called()
        self.delete.assert_not_called()

    def test_a_spare_never_created_is_fine(self):
        task = _final(next_shrink_snap_name="repl_commit_x")
        self.db.get_task_by_id.return_value = task
        self.db.get_lvol_by_name.side_effect = KeyError("none")
        task = final._discard_spare_landing(task.frozen_view())
        self.delete.assert_not_called()
        self.assertNotIn("next_shrink_snap_name", task.function_params)


# --------------------------------------------------------------------------- #
# Fix 3: preconnect in parallel with the rounds (REPL_CUTOVER_EARLY_PREPARE)
# --------------------------------------------------------------------------- #

class _Snap:
    def __init__(self, uuid, bdev="", copy=""):
        self.uuid = uuid
        self.snap_bdev = bdev
        self.target_replicated_snap_uuid = copy

    def get_id(self):
        return self.uuid


class TestEarlyPrepare(unittest.TestCase):

    def setUp(self):
        self.snaps = {"S5": _Snap("S5", copy="C5"),
                      "C5": _Snap("C5", bdev="lvs_t/REP_5"),
                      "S6": _Snap("S6")}
        self.db = patch.object(final, "db").start()

        def _get(sid):
            if sid not in self.snaps:
                raise KeyError(sid)
            return self.snaps[sid]
        self.db.get_snapshot_by_id.side_effect = _get
        self.addCleanup(patch.stopall)

    def test_off_by_default_until_verified_on_a_bed(self):
        self.assertFalse(constants.REPL_CUTOVER_EARLY_PREPARE)

    def test_the_final_step_sits_on_the_last_rounds_copy(self):
        task = _final(early_prepared=True, shrink_snap_id="S5", shrink_round=5,
                      tgt_snap_composite="lvs_t/REP_1").frozen_view()
        task = final._rebase_on_last_round(task)
        self.assertEqual(task.function_params["tgt_snap_composite"], "lvs_t/REP_5")
        self.assertEqual(task.function_params["tgt_snap_id"], "C5")

    def test_never_freezes_onto_an_older_copy_than_the_last_round(self):
        task = _final(early_prepared=True, shrink_snap_id="S6", shrink_round=6,
                      tgt_snap_composite="lvs_t/REP_1").frozen_view()
        with self.assertRaises(TaskDefer):
            final._rebase_on_last_round(task)

    def test_no_replicated_copy_yet_means_the_clone_comes_after_the_rounds(self):
        task = _final(cutover_lvs="LVS_1").frozen_view()
        with patch.object(final, "_prepare_cutover",
                          side_effect=TaskRetry("cutover clone failed: none")):
            out = final._prepare_early(task, MagicMock(), MagicMock(), MagicMock())
        self.assertNotIn("early_prepared", out.function_params)

    def test_an_early_clone_is_marked(self):
        task = _final(cutover_lvs="LVS_1")
        prepared = final._record(task.frozen_view(), tgt_lvol_composite="lvs_t/LVOL")
        with patch.object(final, "_prepare_cutover", return_value=prepared):
            out = final._prepare_early(task.frozen_view(), MagicMock(), MagicMock(),
                                       MagicMock())
        self.assertTrue(out.function_params["early_prepared"])

    def test_it_keeps_converging_while_the_preconnect_is_outstanding(self):
        task = _final(early_prepared=True, shrink_converged=True, shrink_snap_id="S5",
                      shrink_round=5).frozen_view()
        taken = []

        def _take(t, lvol):
            taken.append(1)
            return "S6", None, final._record(t, shrink_snap_id="S6", shrink_round=6)
        with patch.object(final, "_await_cutover_proceed",
                          side_effect=TaskDefer("cutover_pending: waiting")), \
                patch.object(final, "_take_shrink_snapshot", side_effect=_take), \
                patch.object(final, "drop_params") as drop:
            with self.assertRaises(TaskDefer):
                final._await_proceed_converging(task, MagicMock(), task.function_params)
        self.assertEqual(taken, [1])
        drop.assert_called_once()
        self.assertEqual(drop.call_args[0][1], "shrink_converged")

    def test_a_round_in_flight_is_not_doubled_while_waiting(self):
        task = _final(early_prepared=True, shrink_snap_id="S6",
                      shrink_round=6).frozen_view()
        with patch.object(final, "_await_cutover_proceed",
                          side_effect=TaskDefer("cutover_pending: waiting")), \
                patch.object(final, "_take_shrink_snapshot") as take:
            with self.assertRaises(TaskDefer):
                final._await_proceed_converging(task, MagicMock(), task.function_params)
        take.assert_not_called()

    def test_the_runner_freezes_onto_the_last_round_and_records_the_base(self):
        from simplyblock_core.models.storage_node import StorageNode
        node = MagicMock()
        node.status = StorageNode.STATUS_ONLINE
        self.db.get_storage_node_by_id.return_value = node
        self.db.get_lvol_replication_by_id.return_value = MagicMock(cutover_proceed=True)
        self.db.get_task_by_id.side_effect = KeyError("not needed")
        task = _final(src_node_id="S", tgt_node_id="T", cutover_lvs="LVS_1",
                      early_prepared=True, shrink_converged=True,
                      shrink_snap_id="S5", shrink_round=5, start_time=1,
                      tgt_lvol_composite="lvs_t/LVOL", tgt_lvol_id="TL1",
                      tgt_map_id=7, tgt_snap_composite="lvs_t/REP_1",
                      replication_id="REP1")
        with patch.object(final, "_cutover_source", return_value=(node, "")), \
                patch.object(final, "_cutover_target", return_value=(node, "")), \
                patch.object(final, "_record_cutover_done") as done, \
                patch.object(final, "_shrink_step") as shrink, \
                patch.object(final.replication_final_step, "run_cutover",
                             return_value=(True, None)) as run:
            final.task_runner(task.frozen_view(), [])
        shrink.assert_not_called()
        self.assertEqual(run.call_args[0][5], "lvs_t/REP_5")
        done.assert_called_once()
        self.db.atomic_update.assert_called_once()

    def test_the_signal_lets_the_freeze_start_at_once(self):
        task = _final(early_prepared=True, shrink_snap_id="S5").frozen_view()
        with patch.object(final, "_await_cutover_proceed", return_value=None), \
                patch.object(final, "_take_shrink_snapshot") as take:
            final._await_proceed_converging(task, MagicMock(), task.function_params)
        take.assert_not_called()


if __name__ == "__main__":
    unittest.main()
