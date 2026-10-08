"""The intermediate-snapshot transfer poll is a tenacity Retrying, not a
hand-rolled for/sleep loop, and each outcome the old loop distinguished is
still reported: Done, Failed, No process, a stat call that answers nothing,
and a timeout after _INTERMEDIATE_POLL_MAX polls."""
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_lvol_migration as runner


def _rpc(*states):
    rpc = MagicMock()
    rpc.bdev_lvol_transfer_stat.side_effect = [
        None if s is None else {"transfer_state": s} for s in states]
    return rpc


class TestPollIntermediateTransfer(unittest.TestCase):

    def setUp(self):
        self._p1 = patch.object(runner, "_INTERMEDIATE_POLL_INTERVAL_S", 0)
        self._p2 = patch.object(runner, "_INTERMEDIATE_POLL_MAX", 4)
        self._p1.start()
        self._p2.start()

    def tearDown(self):
        self._p1.stop()
        self._p2.stop()

    def test_polls_until_done(self):
        rpc = _rpc("In progress", "In progress", "Done")
        self.assertEqual(runner._poll_intermediate_transfer(rpc, "LVS_1/SNAP_1"), "Done")
        self.assertEqual(rpc.bdev_lvol_transfer_stat.call_count, 3)

    def test_failed_and_no_process_settle_at_once(self):
        for state in ("Failed", "No process"):
            rpc = _rpc(state, "Done")
            self.assertEqual(runner._poll_intermediate_transfer(rpc, "LVS_1/SNAP_1"), state)
            self.assertEqual(rpc.bdev_lvol_transfer_stat.call_count, 1)

    def test_a_stat_that_answers_nothing_is_its_own_outcome(self):
        self.assertEqual(runner._poll_intermediate_transfer(_rpc(None), "LVS_1/SNAP_1"),
                         runner._TRANSFER_STAT_FAILED)

    def test_times_out_after_the_poll_budget(self):
        rpc = _rpc(*(["In progress"] * 10))
        self.assertEqual(runner._poll_intermediate_transfer(rpc, "LVS_1/SNAP_1"),
                         runner._TRANSFER_TIMED_OUT)
        self.assertEqual(rpc.bdev_lvol_transfer_stat.call_count, 4)


if __name__ == "__main__":
    unittest.main()
