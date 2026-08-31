"""The cutover owner's own transfer must not queue behind the volumes it holds.

Run 20260829_010159, case 7 fail-back. One volume passed the catch-up gate and
entered its endgame cleanly:

    phase=await_catchup     lvol=5348e286 ms=314856
    phase=endgame_entered   lvol=5348e286 ms=21047
    phase=take_shrink_snapshot lvol=5348e286 round=1 ms=300 ok=1

and then round 1 never completed. Its snapshot was never even submitted, while
unheld volumes on another lvstore were transferring in 147ms:

    Holding replication of X: lvol 5348e286 is in final cutover  (x42 in 5min)
    replication tasks for 7cb5a697: [('new', 0, ''), ('new', 0, '')]

Two things put it last:

  * main() sleeps 3s after any task returns False, and a HELD task returned
    False -- so the 17 volumes this cutover had itself put on hold cost ~51s of
    backoff per pass;
  * tasks are walked oldest-first, and a cutover enters its endgame LATE by
    construction, so its round snapshots are the newest tasks in the pass.

The volumes waiting on the owner therefore delayed the owner, and the endgame
could not converge. Nothing here changes the exclusivity rule itself.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr


def _repl_task(snap_id, status=JobSchedule.STATUS_NEW):
    t = MagicMock()
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.status = status
    t.canceled = False
    t.function_params = {"snapshot_id": snap_id}
    return t


def _cutover_task(lvol_id, lvs="LVS_10", status=JobSchedule.STATUS_SUSPENDED):
    t = MagicMock()
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = status
    t.canceled = False
    t.function_params = {"lvol_id": lvol_id, "cutover_lvs": lvs}
    return t


class TestHoldIsNotAFailure(unittest.TestCase):

    def test_a_held_transfer_returns_the_hold_sentinel(self):
        task = MagicMock()
        task.function_params = {}
        snapshot = MagicMock()
        snapshot.get_id.return_value = "S1"
        with patch.object(sr, "_lvs_transfer_hold", return_value="owner busy"), \
                patch.object(sr, "db"):
            res = sr.process_snap_replicate_start(task, snapshot)
        self.assertIs(res, sr.HELD)

    def test_the_sentinel_is_still_falsy(self):
        """Everything that treats "not done" as False must keep working."""
        self.assertFalse(sr.HELD)
        self.assertFalse(bool(sr.HELD))

    def test_main_backs_off_on_a_failure_but_not_on_a_hold(self):
        import inspect
        src = inspect.getsource(sr.main)
        self.assertIn("if not res and res is not HELD:", src)


class TestOwnerGoesFirst(unittest.TestCase):

    def setUp(self):
        self.db = patch.object(sr, "db").start()
        self.addCleanup(patch.stopall)
        self._lvols = {}

        def _snap(sid):
            lvol = MagicMock()
            lvol.get_id.return_value = self._lvols[sid]
            s = MagicMock()
            s.lvol = lvol
            return s
        self.db.get_snapshot_by_id.side_effect = _snap

    def _order(self, tasks):
        return [t.function_params.get("snapshot_id") for t in
                sr._replication_order(tasks)
                if t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION]

    def test_the_owners_round_is_served_before_the_volumes_it_holds(self):
        self._lvols = {"held_1": "LV_a", "held_2": "LV_b", "owner_round": "LV_own"}
        tasks = [_repl_task("held_1"), _repl_task("held_2"),
                 _cutover_task("LV_own"),
                 _repl_task("owner_round")]     # newest, as in the real run
        self.assertEqual(self._order(tasks)[0], "owner_round")

    def test_the_rest_keep_their_original_order(self):
        self._lvols = {"held_1": "LV_a", "held_2": "LV_b", "owner_round": "LV_own"}
        tasks = [_repl_task("held_1"), _repl_task("held_2"),
                 _cutover_task("LV_own"), _repl_task("owner_round")]
        self.assertEqual(self._order(tasks), ["owner_round", "held_1", "held_2"])

    def test_with_no_cutover_the_list_is_untouched(self):
        """No reordering, and no snapshot lookups, on the ordinary path."""
        tasks = [_repl_task("a"), _repl_task("b")]
        self.assertIs(sr._replication_order(tasks), tasks)
        self.db.get_snapshot_by_id.assert_not_called()

    def test_a_finished_cutover_claims_no_priority(self):
        self._lvols = {"a": "LV_a", "b": "LV_own"}
        done = _cutover_task("LV_own", status=JobSchedule.STATUS_DONE)
        tasks = [_repl_task("a"), done, _repl_task("b")]
        self.assertEqual(self._order(tasks), ["a", "b"])

    def test_an_unresolvable_snapshot_does_not_break_the_pass(self):
        self._lvols = {"a": "LV_a"}
        self.db.get_snapshot_by_id.side_effect = KeyError("gone")
        tasks = [_repl_task("a"), _cutover_task("LV_own")]
        self.assertEqual(self._order(tasks), ["a"])


class TestPassIntervalDuringCutover(unittest.TestCase):
    """A 10s pass interval IS the convergence time for a 150ms transfer."""

    def test_a_cutover_in_flight_is_recognised(self):
        self.assertTrue(sr._cutover_in_flight([_cutover_task("LV_own")]))
        done = _cutover_task("LV_own", status=JobSchedule.STATUS_DONE)
        self.assertFalse(sr._cutover_in_flight([done]))
        loose = _cutover_task("LV_own")
        loose.function_params.pop("cutover_lvs")
        self.assertFalse(sr._cutover_in_flight([loose]),
                         "a cutover that has not claimed a lvstore holds nobody")

    def test_the_loop_tightens_only_while_one_is_in_flight(self):
        import inspect
        from simplyblock_core import constants
        src = inspect.getsource(sr.main)
        self.assertIn("REPL_CUTOVER_ACTIVE_POLL_SEC if cutover_in_flight", src)
        self.assertIn("else constants.TASK_EXEC_INTERVAL_SEC", src)
        self.assertGreaterEqual(constants.REPL_CUTOVER_ACTIVE_POLL_SEC, 1.0,
                                "still not a sub-second database poll")


if __name__ == "__main__":
    unittest.main()
