"""The cutover must converge the delta before it freezes client IO.

Soak run 20260827_110415, case 10 (online migration under heavy IO) measured
the client-observed freeze at avg 40.4s, max 71.9s, with fio logging 8 errors.
The freeze is bdev_lvol_transfer_final_step, which copies everything written
since the cutover clone's base snapshot -- so the freeze lasts as long as the
write window that precedes it.

The intended sequence is: snapshot -> transfer -> IMMEDIATELY (milliseconds)
the next snapshot -> repeat until a round transfers in low seconds ->
IMMEDIATELY the final lvol transfer. Three things broke it:

  * SHRINK_ROUNDS was a fixed 2 -- a count, not a convergence criterion, so
    under load it simply stopped while the delta was still large;
  * each round returned to the task scheduler, costing TASK_EXEC_INTERVAL_SEC
    (10s) of fresh writes per round -- a floor no number of rounds can beat;
  * the operator preconnect gate sat BETWEEN the base snapshot and the freeze,
    and with no operator its 120s fallback fired 34 times in that run.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.services import tasks_runner_replication_final as runner


class _Task:
    def __init__(self, **params):
        self.function_params = dict(params)
        self.function_result = ""
        self.status = ""
        self.retry = 0
        self.max_retry = 0
        self.canceled = False
        self.cluster_id = "CL"

    def write_to_db(self, *a, **kw):
        pass


def _lvol(uuid="LV1", lvs="LVS_1"):
    lv = MagicMock()
    lv.get_id.return_value = uuid
    lv.uuid = uuid
    lv.lvs_name = lvs
    return lv


class _Clock:
    """A clock that only moves when the code under test waits.

    The convergence loop measures a round as (now - shrink_started_at), so a
    fake that returns the same instant for both makes every round look
    instantaneous and the test proves nothing.
    """

    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class TestConvergence(unittest.TestCase):
    """_shrink_step loops until a round is fast, without leaving the pass."""

    def _run(self, round_times, max_rounds=None, exclusive=True):
        """Drive _shrink_step with round i taking round_times[i] seconds.

        exclusive=True means the task already holds its lvstore, which is the
        endgame. Unexclusive rounds converge in the open and then ASK for the
        lvstore instead of handing straight over to the freeze.
        """
        clock = _Clock()
        task = _Task(shrink_snap_id="S0", shrink_round=1,
                     shrink_deadline=10 ** 9, lvol_id="LV1")
        if exclusive:
            task.function_params["cutover_lvs"] = "LVS_1"
        task.function_params["shrink_started_at"] = clock.now
        state = {"i": 0}
        taken = []

        def _done(snap_id):
            # replicated once this round's transfer time has actually passed
            started = task.function_params["shrink_started_at"]
            idx = min(state["i"], len(round_times) - 1)
            return (clock.now - started) >= round_times[idx]

        def _take(task_, lvol_):
            state["i"] += 1
            taken.append(state["i"])
            task_.function_params["shrink_round"] += 1
            task_.function_params["shrink_snap_id"] = "S%d" % state["i"]
            task_.function_params["shrink_started_at"] = clock.now
            return "S%d" % state["i"], None

        patches: list = [
            patch.object(runner.time, "time", clock),
            patch.object(runner.time, "sleep", clock.sleep),
            patch.object(runner, "_shrink_round_done", side_effect=_done),
            patch.object(runner, "_take_shrink_snapshot", side_effect=_take),
        ]
        if max_rounds is not None:
            patches.append(
                patch.object(constants, "REPL_CUTOVER_MAX_SHRINK_ROUNDS", max_rounds))
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        done, err = runner._shrink_step(task, _lvol())
        return done, err, task, taken, clock

    def test_a_fast_round_while_holding_the_lvstore_hands_over(self):
        """Converged AND exclusive -> freeze immediately."""
        done, err, task, taken, _ = self._run([0.5])
        self.assertTrue(done)
        self.assertIsNone(err)
        self.assertIn("converged", task.function_result)
        self.assertEqual(taken, [], "a fast first round needs no further rounds")

    def test_no_round_ever_runs_outside_the_lvstore_claim(self):
        """Rounds are the endgame, and the endgame is exclusive by definition.

        There used to be "open" rounds converging before the claim, so that the
        bulk catch-up was not serialised. That was the wrong instrument: the
        volume is caught up by its ORDINARY replication cadence, which costs the
        cutover nothing, and only then does it ask for the lvstore. The claim is
        therefore taken before round 1, and the loop below need not consider a
        round without it.
        """
        import inspect
        src = inspect.getsource(runner.task_runner)
        entry = src.index("_acquire_lvs_claim")
        self.assertLess(entry, src.index("_take_shrink_snapshot"),
                        "the lvstore is taken before the first round, not after")
        self.assertNotIn("ready_for_exclusive", inspect.getsource(runner),
                         "open rounds are gone")

    def test_a_slow_round_takes_another_snapshot_without_leaving_the_pass(self):
        """The whole point: rounds follow each other in milliseconds."""
        done, err, task, taken, clock = self._run([3.0, 3.0, 0.4])
        self.assertTrue(done)
        self.assertIsNone(err)
        self.assertEqual(taken, [1, 2],
                         "each slow round must be followed immediately by the next")
        self.assertIn("converged", task.function_result)
        # ~6.4s of transfers, not 6.4 + 3 x TASK_EXEC_INTERVAL_SEC of
        # rescheduling: the delta a round carries is the previous round's
        # transfer, nothing more.
        self.assertLess(clock.now - 1000.0, 7.0)

    def test_it_gives_up_after_the_round_cap_and_freezes_anyway(self):
        """Written faster than it replicates: freeze rather than loop forever."""
        done, err, task, taken, _ = self._run([3.0] * 20, max_rounds=3)
        self.assertTrue(done, "the cap must hand over, not fail the cutover")
        self.assertIsNone(err)
        self.assertIn("not converged", task.function_result)

    def test_the_cap_freezes_under_the_claim_it_already_holds(self):
        """Giving up converging goes straight to the freeze.

        The claim was taken on entry to the endgame, so reaching the round cap
        needs no further acquisition -- it just stops converging and freezes.
        """
        done, err, task, taken, _ = self._run([3.0] * 20, max_rounds=3)
        self.assertTrue(done)
        self.assertIsNone(err)
        self.assertIn("not converged", task.function_result)

    def test_a_vanished_snapshot_is_an_error(self):
        # This one runs on the real clock, so the deadline has to be a real
        # future epoch -- 10**9 is 2001 and would trip the timeout instead.
        task = _Task(shrink_snap_id="S0", shrink_round=1,
                     shrink_deadline=10 ** 12, lvol_id="LV1")
        with patch.object(runner, "_shrink_round_done", return_value=None):
            done, err = runner._shrink_step(task, _lvol())
        self.assertFalse(done)
        self.assertIn("disappeared", err)

    def test_the_deadline_still_bounds_the_phase(self):
        """The deadline bounds the phase by handing over to the freeze, not by
        failing: proceeding with a slightly larger residual always beats
        burning a retry on another 900-second shrink window."""
        task = _Task(shrink_snap_id="S0", shrink_round=1,
                     shrink_deadline=0, lvol_id="LV1")
        with patch.object(runner, "_shrink_round_done", return_value=False):
            done, err = runner._shrink_step(task, _lvol())
        self.assertTrue(done)
        self.assertIsNone(err)

    def test_it_yields_the_pass_when_the_budget_runs_out(self):
        """A very slow transfer must not hog the runner forever."""
        clock = _Clock()
        task = _Task(shrink_snap_id="S0", shrink_round=1,
                     shrink_deadline=10 ** 9, lvol_id="LV1")
        task.function_params["shrink_started_at"] = clock.now
        with patch.object(runner.time, "time", clock), \
             patch.object(runner.time, "sleep", clock.sleep), \
             patch.object(runner, "_shrink_round_done", return_value=False), \
             patch.object(constants, "REPL_CUTOVER_CONVERGE_BUDGET_SEC", 5):
            done, err = runner._shrink_step(task, _lvol())
        self.assertFalse(done)
        self.assertIsNone(err, "yielding the pass is not a failure")
        self.assertIn("waiting", task.function_result)


class TestProceedGate(unittest.TestCase):
    """The preconnect wait must be opt-in: it costs freeze time."""

    def test_enabled_now_that_the_operator_signals(self):
        """Flipped 2026-09-02: the operator's reconcileCutoverPending posts
        cutover-proceed for migration AND failback (annotFailbackTarget routes
        the call to the target cluster). Without the gate the ANA flip races
        the client's preconnect: the 2026-09-02 failback run flipped listeners
        no client had connected to and deleted the DR-side subsystem 150ms
        later, orphaning every connected client for ctrl_loss_tmo."""
        self.assertTrue(
            constants.REPL_CUTOVER_PROCEED_REQUIRED,
            "cutover must wait for the operator's preconnect signal; "
            "flipping ANA on listeners no client is connected to and then "
            "deleting the source subsystem strands every live client")

    def test_the_wait_is_guarded_by_the_flag(self):
        import inspect
        src = inspect.getsource(runner.task_runner)
        self.assertIn("constants.REPL_CUTOVER_PROCEED_REQUIRED", src)
        # Compare against the actual freeze CALL SITE, not the first "run_cutover"
        # occurrence — the string also appears earlier in a comment about the
        # retry path, which is not the freeze.
        self.assertLess(
            src.index("REPL_CUTOVER_PROCEED_REQUIRED"),
            src.index("replication_final_step.run_cutover"),
            "the gate must be evaluated before the freeze")


if __name__ == "__main__":
    unittest.main()


class TestLvsAdmission(unittest.TestCase):
    """One lvstore's bandwidth, two priorities.

    Case 10 ran 9 volumes over shared lvstores and every one kept replicating
    while another was trying to converge, stretching each round -- and every
    second a round is stretched is a second of writes that lands in the frozen
    final step.
    """

    def setUp(self):
        from simplyblock_core.services import snapshot_replication as sr
        self.sr = sr
        patcher = patch.object(sr, "db")
        self.db = patcher.start()
        self.addCleanup(patcher.stop)
        self.groups: dict = {}                     # lvol id -> group id
        gp = patch.object(sr, "_group_id_for_lvol",
                          side_effect=lambda lv: self.groups.get(lv.get_id(), ""))
        gp.start()
        self.addCleanup(gp.stop)

    # -- fixtures ---------------------------------------------------------
    def _cutover_task(self, lvol_id, lvs, group="", status="running",
                      canceled=False):
        from simplyblock_core.models.job_schedule import JobSchedule
        t = MagicMock()
        t.function_name = JobSchedule.FN_REPLICATION_FINAL
        t.status = status
        t.canceled = canceled
        t.function_params = {"lvol_id": lvol_id, "cutover_lvs": lvs,
                             "cutover_group": group}
        return t

    def _transfer_task(self, snap_id, task_id="T_other", status=None):
        from simplyblock_core.models.job_schedule import JobSchedule
        t = MagicMock()
        t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
        t.status = status or JobSchedule.STATUS_RUNNING
        t.canceled = False
        t.get_id.return_value = task_id
        t.function_params = {"snapshot_id": snap_id}
        return t

    def _lv(self, lvol_id, lvs="LVS_1"):
        lv = MagicMock()
        lv.get_id.return_value = lvol_id
        lv.lvs_name = lvs
        return lv

    def _snapshot(self, lvol_id="LV_other", lvs="LVS_1"):
        snap = MagicMock()
        snap.lvol = self._lv(lvol_id, lvs)
        return snap

    def _task(self, task_id="T_me"):
        t = MagicMock()
        t.cluster_id = "CL"
        t.get_id.return_value = task_id
        return t

    # -- priority 1: a cutover owns its lvstore ---------------------------
    def test_another_volumes_cutover_holds_this_lvstore(self):
        self.db.get_job_tasks.return_value = [
            self._cutover_task("LV_cutting", "LVS_1")]
        self.assertIn("final cutover",
                      self.sr._lvs_transfer_hold(self._task(), self._snapshot()))

    def test_the_volume_in_cutover_may_still_replicate(self):
        """Its convergence snapshots are exactly what must keep moving."""
        self.db.get_job_tasks.return_value = [
            self._cutover_task("LV_cutting", "LVS_1")]
        self.assertEqual(
            self.sr._lvs_transfer_hold(
                self._task(), self._snapshot(lvol_id="LV_cutting")), "")

    def test_a_group_member_is_not_held_by_its_groups_cutover(self):
        """A consistency group cuts over as a group, not one member at a time."""
        self.groups = {"LV_cutting": "CL/G1", "LV_sibling": "CL/G1"}
        self.db.get_job_tasks.return_value = [
            self._cutover_task("LV_cutting", "LVS_1", group="CL/G1")]
        self.assertEqual(
            self.sr._lvs_transfer_hold(
                self._task(), self._snapshot(lvol_id="LV_sibling")), "")

    def test_a_volume_outside_the_group_is_still_held_by_its_cutover(self):
        self.groups = {"LV_cutting": "CL/G1", "LV_loose": ""}
        self.db.get_job_tasks.return_value = [
            self._cutover_task("LV_cutting", "LVS_1", group="CL/G1")]
        self.assertIn("final cutover", self.sr._lvs_transfer_hold(
            self._task(), self._snapshot(lvol_id="LV_loose")))

    def test_a_cutover_on_a_different_lvstore_does_not_hold_us(self):
        self.db.get_job_tasks.return_value = [
            self._cutover_task("LV_cutting", "LVS_9")]
        self.assertEqual(
            self.sr._lvs_transfer_hold(self._task(), self._snapshot()), "")

    def test_a_finished_or_cancelled_cutover_holds_nothing(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        for task in (self._cutover_task("LV_x", "LVS_1",
                                        status=JobSchedule.STATUS_DONE),
                     self._cutover_task("LV_x", "LVS_1", canceled=True)):
            self.db.get_job_tasks.return_value = [task]
            self.assertEqual(
                self.sr._lvs_transfer_hold(self._task(), self._snapshot()), "")

    def test_a_task_that_has_not_claimed_an_lvs_holds_nothing(self):
        """Before the cutover starts its rounds there is nothing to protect."""
        t = self._cutover_task("LV_cutting", "LVS_1")
        del t.function_params["cutover_lvs"]
        self.db.get_job_tasks.return_value = [t]
        self.assertEqual(
            self.sr._lvs_transfer_hold(self._task(), self._snapshot()), "")

    # -- priority 2: groups outrank loose volumes, and serialize -----------
    def test_a_transferring_group_holds_a_volume_from_another_group(self):
        self.groups = {"LV_a": "CL/G1", "LV_b": "CL/G2"}
        self.db.get_job_tasks.return_value = [self._transfer_task("S_a")]
        self.db.get_snapshot_by_id.return_value = MagicMock(lvol=self._lv("LV_a"))
        self.assertIn("consistency group", self.sr._lvs_transfer_hold(
            self._task(), self._snapshot(lvol_id="LV_b")))

    def test_members_of_the_same_group_transfer_in_parallel(self):
        self.groups = {"LV_a": "CL/G1", "LV_b": "CL/G1"}
        self.db.get_job_tasks.return_value = [self._transfer_task("S_a")]
        self.db.get_snapshot_by_id.return_value = MagicMock(lvol=self._lv("LV_a"))
        self.assertEqual(
            self.sr._lvs_transfer_hold(self._task(), self._snapshot(lvol_id="LV_b")),
            "")

    def test_a_transferring_group_outranks_a_loose_volume(self):
        self.groups = {"LV_a": "CL/G1", "LV_loose": ""}
        self.db.get_job_tasks.return_value = [self._transfer_task("S_a")]
        self.db.get_snapshot_by_id.return_value = MagicMock(lvol=self._lv("LV_a"))
        self.assertIn("consistency group", self.sr._lvs_transfer_hold(
            self._task(), self._snapshot(lvol_id="LV_loose")))

    def test_loose_volumes_still_transfer_in_parallel_with_each_other(self):
        """No group involved: unchanged behaviour, no new serialization."""
        self.groups = {"LV_a": "", "LV_b": ""}
        self.db.get_job_tasks.return_value = [self._transfer_task("S_a")]
        self.db.get_snapshot_by_id.return_value = MagicMock(lvol=self._lv("LV_a"))
        self.assertEqual(
            self.sr._lvs_transfer_hold(self._task(), self._snapshot(lvol_id="LV_b")),
            "")

    def test_a_group_transferring_on_another_lvstore_does_not_hold_us(self):
        self.groups = {"LV_a": "CL/G1", "LV_b": "CL/G2"}
        self.db.get_job_tasks.return_value = [self._transfer_task("S_a")]
        self.db.get_snapshot_by_id.return_value = MagicMock(
            lvol=self._lv("LV_a", lvs="LVS_9"))
        self.assertEqual(
            self.sr._lvs_transfer_hold(self._task(), self._snapshot(lvol_id="LV_b")),
            "")


class TestRoundOneIsMeasured(unittest.TestCase):
    """The regression that let the freeze survive the convergence loop.

    Round 1 used to be created by replicate/commit without the stamp the loop
    measures against, so it measured as 0.00s, counted as converged, and the
    freeze copied the whole delta (run 20260827_172734, 9-55s server-side).

    Every round is now born in one place -- _take_shrink_snapshot, in the
    endgame -- so there is a single stamp to get right.
    """

    def test_every_round_is_stamped_where_it_is_taken(self):
        import inspect
        src = inspect.getsource(runner._take_shrink_snapshot)
        self.assertIn('params["shrink_started_at"] = time.time()', src)
        self.assertIn('params["shrink_round"] = params.get("shrink_round", 0) + 1',
                      src, "the round number and its stamp must move together")

    def test_the_controller_enqueues_no_round_of_its_own(self):
        """Commit takes no snapshot, so it must not claim a round in flight."""
        import inspect
        from simplyblock_core.controllers import lvol_controller as lc
        src = inspect.getsource(lc.replication_commit)
        self.assertIn('"shrink_round": 0', src)
        self.assertNotIn('"shrink_snap_id"', src)

    def test_an_unmeasured_round_is_not_treated_as_converged(self):
        """Belt and braces for tasks enqueued without the stamp."""
        clock = _Clock()
        task = _Task(shrink_snap_id="S0", shrink_round=1,
                     shrink_deadline=10 ** 9, lvol_id="LV1")
        # deliberately NO shrink_started_at
        taken = []

        def _take(task_, lvol_):
            taken.append(task_.function_params["shrink_round"])
            task_.function_params["shrink_round"] += 1
            task_.function_params["shrink_snap_id"] = "S1"
            task_.function_params["shrink_started_at"] = clock.now
            return "S1", None

        with patch.object(runner.time, "time", clock), \
             patch.object(runner.time, "sleep", clock.sleep), \
             patch.object(runner, "_shrink_round_done", return_value=True), \
             patch.object(runner, "_take_shrink_snapshot", side_effect=_take), \
             patch.object(constants, "REPL_CUTOVER_MIN_INLINE_SEC", 0), \
             patch.object(constants, "REPL_CUTOVER_CONVERGE_BUDGET_SEC", 0):
            done, err = runner._shrink_step(task, _lvol())

        self.assertFalse(done, "an unmeasured round must not end the shrink phase")
        self.assertIsNone(err)
        self.assertEqual(taken, [1],
                         "it must take another round instead of freezing")


class TestCutoverQueue(unittest.TestCase):
    """One cutover per lvstore, and the losers queue instead of starving.

    Run 20260827_185009: all 20 volumes entered their cutover together and each
    wrote cutover_lvs, so the claim was a marker with no exclusion. The
    replication side then held everyone but an arbitrary winner -- including
    the other cutovers' own shrink snapshots -- and 9 of 10 volumes per
    lvstore sat at "round 1: waiting to replicate" until their deadline killed
    them (17 x "max retry reached").
    """

    def setUp(self):
        patcher = patch.object(runner, "db")
        self.db = patcher.start()
        self.addCleanup(patcher.stop)
        self.db.kv_store = "KV"
        gp = patch.object(runner, "_group_id_for_lvol", return_value="")
        self.group_of = gp.start()
        self.addCleanup(gp.stop)

    def _task(self, task_id, lvs=None, group="", status="running",
              canceled=False, created="2026-01-01"):
        from simplyblock_core.models.job_schedule import JobSchedule
        t = MagicMock()
        t.function_name = JobSchedule.FN_REPLICATION_FINAL
        t.get_id.return_value = task_id
        t.status = status
        t.canceled = canceled
        t.create_dt = created
        t.function_params = {"lvol_id": "LV_" + task_id}
        if lvs:
            t.function_params["cutover_lvs"] = lvs
            t.function_params["cutover_group"] = group
        return t

    def test_no_owner_means_the_lvstore_is_free(self):
        me = self._task("T1")
        self.db.get_job_tasks.return_value = [me]
        self.assertIsNone(runner._lvs_cutover_owner(me, "LVS_1"))

    def test_an_active_claim_owns_the_lvstore(self):
        me, other = self._task("T1"), self._task("T2", lvs="LVS_1")
        self.db.get_job_tasks.return_value = [me, other]
        owner = runner._lvs_cutover_owner(me, "LVS_1")
        self.assertIsNotNone(owner)
        self.assertEqual(owner.get_id(), "T2")

    def test_a_finished_or_cancelled_cutover_owns_nothing(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        me = self._task("T1")
        for dead in (self._task("T2", lvs="LVS_1",
                                status=JobSchedule.STATUS_DONE),
                     self._task("T3", lvs="LVS_1", canceled=True)):
            self.db.get_job_tasks.return_value = [me, dead]
            self.assertIsNone(runner._lvs_cutover_owner(me, "LVS_1"),
                              "a dead task must not hold the lvstore forever")

    def test_the_earliest_claim_wins_deterministically(self):
        """Two tasks racing must agree on the winner, not each see the other."""
        me = self._task("T1")
        early = self._task("T2", lvs="LVS_1", created="2026-01-01")
        late = self._task("T3", lvs="LVS_1", created="2026-06-01")
        self.db.get_job_tasks.return_value = [me, late, early]
        self.assertEqual(runner._lvs_cutover_owner(me, "LVS_1").get_id(), "T2")

    def test_a_claim_on_another_lvstore_is_irrelevant(self):
        me, other = self._task("T1"), self._task("T2", lvs="LVS_9")
        self.db.get_job_tasks.return_value = [me, other]
        self.assertIsNone(runner._lvs_cutover_owner(me, "LVS_1"))

    def test_circular_stall_is_broken_when_both_tasks_hold_the_claim(self):
        """Both tasks race and both write cutover_lvs — only one wins.

        Regression for production stall:
          36418f5d queued_for_lvstore_LVS_1_behind_309d3aeb
          309d3aeb queued_for_lvstore_LVS_1_behind_36418f5d

        Root cause: _lvs_cutover_owner excluded the calling task from the scan.
        When two threads both raced through the check before either wrote its
        claim, both stored cutover_lvs.  On every subsequent pass each task's
        candidate set was exactly {the other}, so each always deferred to the
        other — a permanent cycle.  The fix: include self in the sort, return
        None iff the caller is the winner.
        """
        # Simulate the post-race DB state: both have cutover_lvs set.
        early = self._task("T1", lvs="LVS_1", created="2026-01-01")
        late = self._task("T2", lvs="LVS_1", created="2026-06-01")
        self.db.get_job_tasks.return_value = [early, late]

        # From T1's perspective: T1 is the earliest claimant → it is the owner.
        self.assertIsNone(
            runner._lvs_cutover_owner(early, "LVS_1"),
            "the earliest claimant must see itself as the winner (None), "
            "not defer to the only other claimant")

        # From T2's perspective: T1 is the earliest claimant → T2 must yield.
        owner_seen_by_late = runner._lvs_cutover_owner(late, "LVS_1")
        self.assertIsNotNone(owner_seen_by_late,
                             "the later claimant must see an owner")
        self.assertEqual(owner_seen_by_late.get_id(), "T1",
                         "the later claimant must defer to the earlier one")


class TestQueuedCutoverDoesNotStarve(unittest.TestCase):
    """A cutover that cannot have the lvstore waits without cost."""

    def setUp(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        from simplyblock_core.models.storage_node import StorageNode
        self.JobSchedule = JobSchedule
        patcher = patch.object(runner, "db")
        self.db = patcher.start()
        self.addCleanup(patcher.stop)
        self.db.kv_store = "KV"

        lvol = MagicMock()
        lvol.get_id.return_value = "LV_me"
        lvol.lvs_name = "LVS_1"
        self.db.get_lvol_by_id.return_value = lvol

        node = MagicMock()
        node.status = StorageNode.STATUS_ONLINE
        node.get_id.return_value = "N1"
        node.cluster_id = "CL"
        self.db.get_storage_node_by_id.return_value = node

        gp = patch.object(runner, "_group_id_for_lvol", return_value="")
        gp.start()
        self.addCleanup(gp.stop)
        # A task waiting for the lvstore must not proceed into the endgame.
        sp = patch.object(runner, "_shrink_step",
                          side_effect=AssertionError(
                              "a queued cutover must not run its endgame rounds"))
        sp.start()
        self.addCleanup(sp.stop)
        # Caught up: the endgame is asked for at this point, and the answer is
        # either the lvstore or a queue slot.
        lp = patch.object(runner, "_replication_lag_sec", return_value=1.0)
        lp.start()
        self.addCleanup(lp.stop)
        tp = patch.object(runner, "_take_shrink_snapshot",
                          side_effect=AssertionError(
                              "a queued cutover must not take a snapshot"))
        tp.start()
        self.addCleanup(tp.stop)

    def _me(self):
        t = MagicMock()
        t.function_name = self.JobSchedule.FN_REPLICATION_FINAL
        t.get_id.return_value = "T_me"
        t.cluster_id = "CL"
        t.status = self.JobSchedule.STATUS_NEW
        t.canceled = False
        t.retry = 0
        t.max_retry = 8
        t.create_dt = "2026-06-01"
        t.function_params = {
            "lvol_id": "LV_me", "src_node_id": "N1", "tgt_node_id": "N2",
            "shrink_round": 1, "shrink_snap_id": "S1",
            "shrink_deadline": 1,          # already expired
        }
        return t

    def _owner(self):
        t = MagicMock()
        t.function_name = self.JobSchedule.FN_REPLICATION_FINAL
        t.get_id.return_value = "T_owner"
        t.status = self.JobSchedule.STATUS_RUNNING
        t.canceled = False
        t.create_dt = "2026-01-01"
        t.function_params = {"lvol_id": "LV_owner", "cutover_lvs": "LVS_1",
                             "cutover_group": ""}
        return t

    def test_it_queues_without_burning_a_retry_or_its_deadline(self):
        me, owner = self._me(), self._owner()
        self.db.get_job_tasks.return_value = [me, owner]

        result = runner.task_runner(me)

        self.assertFalse(result)
        self.assertEqual(me.status, self.JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(me.retry, 0, "queueing is not a failure")
        self.assertIn("queued for lvstore", me.function_result)
        self.assertGreater(
            me.function_params["shrink_deadline"], 10 ** 9,
            "the deadline must be pushed out while queued, or the task dies of "
            "max retries waiting for a lock it cannot win")
        self.assertNotIn("cutover_lvs", me.function_params,
                         "a queued task must not also claim the lvstore")

    def test_a_group_sibling_joins_the_owner_instead_of_queueing(self):
        me, owner = self._me(), self._owner()
        owner.function_params["cutover_group"] = "CL/G1"
        with patch.object(runner, "_group_id_for_lvol", return_value="CL/G1"):
            self.db.get_job_tasks.return_value = [me, owner]
            # It proceeds into the shrink phase, which this fixture makes raise.
            with self.assertRaises(AssertionError):
                runner.task_runner(me)


class TestCutoverFailuresAreVisible(unittest.TestCase):
    """160 failed attempts must not produce zero log lines.

    Run 20260827_194551: every fail-back cutover ended as "max retry reached
    (8/8)" with nothing logged and the cause overwritten, so three separate
    investigations could not name the failing branch.
    """

    def setUp(self):
        patcher = patch.object(runner, "db")
        self.db = patcher.start()
        self.addCleanup(patcher.stop)
        self.db.kv_store = "KV"

    def _task(self, retry=0):
        from simplyblock_core.models.job_schedule import JobSchedule
        t = _Task(lvol_id="LV1")
        t.status = JobSchedule.STATUS_RUNNING
        t.retry = retry
        t.max_retry = 8
        t.canceled = False
        return t

    def test_a_failed_attempt_is_logged_and_remembered(self):
        task = self._task()
        with self.assertLogs(runner.logger, level="WARNING") as logs:
            runner._finalize(task, False, "target subsystem is full")
        self.assertIn("target subsystem is full", "\n".join(logs.output))
        self.assertEqual(task.function_params["last_error"],
                         "target subsystem is full")

    def test_giving_up_reports_the_cause_not_just_the_symptom(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        task = self._task(retry=8)
        task.function_params["last_error"] = "target subsystem is full"
        task.function_params.update({"src_node_id": "N1", "tgt_node_id": "N2"})
        with self.assertLogs(runner.logger, level="ERROR") as logs:
            runner.task_runner(task)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("target subsystem is full", task.function_result,
                      "'max retry reached' alone names a symptom and hides the "
                      "cause")
        self.assertIn("target subsystem is full", "\n".join(logs.output))
