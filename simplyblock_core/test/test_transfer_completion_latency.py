"""No time may be lost between a transfer completing and the next snapshot.

Measured before this fix (run 20260828_115307, case 7 fail-back):

    polls per in-flight transfer : p50 = 1
    gap between polls            : p50 = 81.2s
    states ever seen             : {'Done': 86}  -- never once "In progress"

Every poll found the transfer already finished. Submit happened on one pass of
the runner loop and the Done check on a later one, so a transfer that completed
in milliseconds went unnoticed for ~81 seconds. A convergence round cannot take
its next snapshot until the previous one is marked replicated, so that latency
landed directly in the client's IO freeze.

Requirement: completion must be picked up and the next snapshot initiated in
under one second, in the WORST case, not just typically.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_replication_final as final


class TestInlineCompletion(unittest.TestCase):
    """The submitting pass waits for the transfer and finishes it itself."""

    def setUp(self):
        self.db = patch.object(sr, "db").start()
        self.addCleanup(patch.stopall)
        self.finish = patch.object(sr, "_finish_completed_transfer",
                                   return_value=True).start()
        patch.object(sr, "_cutover_owns", return_value=False).start()
        self.slept: list = []
        patch.object(sr.time, "sleep", side_effect=self.slept.append).start()

        self.snap = MagicMock()
        self.snap.get_id.return_value = "S1"
        self.snap.snap_bdev = "LVS_1/SNAP_1"
        self.snap.lvol.get_id.return_value = "LV1"

        self.task = MagicMock()
        self.task.cluster_id = "CL"
        self.task.function_params = {}

        self.node = MagicMock()
        self.rpc = MagicMock()
        self.node.rpc_client.return_value = self.rpc

    def test_a_transfer_already_done_is_finished_without_sleeping(self):
        self.rpc.bdev_lvol_transfer_stat.return_value = {
            "transfer_state": "Done", "offset": 2 * 1024 * 1024}
        self.assertTrue(
            sr._await_transfer_completion(self.task, self.snap, self.node))
        self.finish.assert_called_once()
        self.assertEqual(self.slept, [],
                         "a finished transfer must be acted on immediately")

    def test_completion_is_noticed_within_one_poll_interval(self):
        states = [{"transfer_state": "In progress", "offset": 1},
                  {"transfer_state": "Done", "offset": 2}]
        self.rpc.bdev_lvol_transfer_stat.side_effect = states
        self.assertTrue(
            sr._await_transfer_completion(self.task, self.snap, self.node))
        self.finish.assert_called_once()
        self.assertEqual(self.slept, [constants.REPL_XFER_POLL_INTERVAL_SEC])
        self.assertLess(constants.REPL_XFER_POLL_INTERVAL_SEC, 1.0,
                        "the poll interval IS the worst-case detection delay")

    def test_a_failed_transfer_is_left_to_the_retry_path(self):
        self.rpc.bdev_lvol_transfer_stat.return_value = {
            "transfer_state": "Failed", "offset": 0}
        self.assertFalse(
            sr._await_transfer_completion(self.task, self.snap, self.node))
        self.finish.assert_not_called()

    def test_a_volume_in_cutover_gets_the_generous_budget(self):
        """Its lvstore is already claimed, so waiting starves nothing."""
        with patch.object(sr, "_cutover_owns", return_value=True):
            self.rpc.bdev_lvol_transfer_stat.return_value = {
                "transfer_state": "Done", "offset": 1}
            self.assertTrue(
                sr._await_transfer_completion(self.task, self.snap, self.node))
        self.assertGreater(constants.REPL_XFER_INLINE_WAIT_CUTOVER_SEC,
                           constants.REPL_XFER_INLINE_WAIT_SEC)

    def test_the_submit_path_calls_the_wait(self):
        import inspect
        src = inspect.getsource(sr.process_snap_replicate_start)
        self.assertIn("_await_transfer_completion", src,
                      "submitting and forgetting is what cost 81 seconds")


class TestPassLatency(unittest.TestCase):
    """A round that yields must be picked up again in well under a second."""

    def test_sub_second_reaction_does_not_come_from_polling_the_database(self):
        """The DB pass interval is deliberately NOT sub-second.

        This loop reads the task table (and each task) per pass, so polling it
        at 5Hz burns transactions proportional to clusters x tasks to learn
        nothing almost every time -- the wrong shape for detecting an event.
        Sub-second reaction comes from the RPC-based inline wait instead.
        """
        self.assertGreaterEqual(constants.REPL_CUTOVER_ACTIVE_POLL_SEC, 1.0,
                                "do not poll a database sub-second")
        self.assertLess(constants.REPL_CUTOVER_ACTIVE_POLL_SEC,
                        constants.TASK_EXEC_INTERVAL_SEC,
                        "but a mid-round cutover still deserves a tighter pass")
        # This is the interval the guarantee actually rests on, and it polls
        # SPDK over RPC, not the DB.
        self.assertLess(constants.REPL_XFER_POLL_INTERVAL_SEC, 1.0)

    def test_a_claimed_or_mid_round_task_counts_as_in_flight(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        def t(**params):
            x = MagicMock()
            x.function_name = JobSchedule.FN_REPLICATION_FINAL
            x.status = JobSchedule.STATUS_RUNNING
            x.canceled = False
            x.function_params = params
            return x
        self.assertTrue(final._any_cutover_in_flight([t(cutover_lvs="LVS_1")]))
        self.assertTrue(final._any_cutover_in_flight([t(shrink_snap_id="S1")]))
        self.assertFalse(final._any_cutover_in_flight([t()]))

    def test_a_finished_cutover_does_not_hold_the_fast_interval(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        done = MagicMock()
        done.function_name = JobSchedule.FN_REPLICATION_FINAL
        done.status = JobSchedule.STATUS_DONE
        done.canceled = False
        done.function_params = {"cutover_lvs": "LVS_1"}
        self.assertFalse(final._any_cutover_in_flight([done]),
                         "an idle cluster must not be spun on")

    def test_the_per_task_backoff_is_gone(self):
        """It punished the COMMON case: 20 queued tasks cost ~70s per pass.

        The DB-failure backoff in the except branch is a different thing and
        stays -- this asserts only that a yielding TASK no longer sleeps.
        """
        import inspect
        src = inspect.getsource(final.main)
        self.assertNotIn("if not res:", src,
                         "a queued or mid-round task must not cost a sleep")
        self.assertIn("REPL_CUTOVER_ACTIVE_POLL_SEC", src)

    def test_the_owner_lookup_reuses_the_prefetched_task_list(self):
        """Re-reading per task is O(N^2) DB reads, unaffordable at 200ms."""
        import inspect
        self.assertIn("tasks=None",
                      inspect.signature(final._lvs_cutover_owner).__str__()
                      .replace(" ", "").replace("'", ""))
        self.assertIn("cluster_tasks", inspect.getsource(final.main))


if __name__ == "__main__":
    unittest.main()
