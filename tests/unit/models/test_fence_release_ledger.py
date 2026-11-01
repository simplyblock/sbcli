"""The fence-release ledger (models/fence_release.py): the memory the
monitor, the health check and the port_allow runner share about how often one
client port has been reopened.

prod ams1, 2026-11-01 08:52-09:01. One stripe of LVS_4033 had only three
chunks and could not be served while one of its nodes rebooted. Every retried
read made the leader fence port 9113, the next retry moved leadership and the
fence to a peer, and within 12-17 s the control plane reopened each port
again: nine port_allow tasks, 60 leadership switches, 144 failed reads. The
budget here is what makes the third reopen in ten minutes the last one.
"""

import unittest

from simplyblock_core import constants
from simplyblock_core.models.fence_release import FenceReleaseLedger

BUDGET = constants.FENCE_RELEASE_BUDGET
WINDOW = constants.FENCE_RELEASE_WINDOW_SEC


def _ledger():
    ledger = FenceReleaseLedger()
    ledger.cluster_id = "cl-1"
    ledger.node_id = "d3a07483"
    ledger.port = 9113
    return ledger


class TestBudget(unittest.TestCase):

    def test_a_fresh_ledger_has_budget(self):
        self.assertFalse(_ledger().exhausted(now=1000.0))

    def test_one_release_short_of_the_budget_still_has_budget(self):
        ledger = _ledger()
        for i in range(BUDGET - 1):
            ledger.record(now=1000.0 + i)
        self.assertFalse(ledger.exhausted(now=1010.0))

    def test_the_budget_is_exhausted_at_the_limit(self):
        """The incident: three releases in under a minute."""
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(now=1000.0 + 15 * i)
        self.assertTrue(ledger.exhausted(now=1000.0 + 15 * BUDGET))

    def test_releases_outside_the_window_do_not_count(self):
        """A leaked fence lifted once an hour is not a loop."""
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(now=1000.0 + i * (WINDOW + 1))
        last = 1000.0 + (BUDGET - 1) * (WINDOW + 1)
        self.assertFalse(ledger.exhausted(now=last + 1))
        self.assertEqual(len(ledger.releases), 1, "prune keeps only the window")

    def test_the_budget_reopens_when_the_oldest_release_ages_out(self):
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(now=1000.0 + i)
        self.assertTrue(ledger.exhausted(now=1000.0 + BUDGET))
        reopen = ledger.reopens_in(now=1000.0 + BUDGET)
        self.assertAlmostEqual(reopen, WINDOW - BUDGET)
        self.assertFalse(ledger.exhausted(now=1000.0 + WINDOW + 0.5))

    def test_reopens_in_is_zero_with_nothing_recorded(self):
        self.assertEqual(_ledger().reopens_in(now=5.0), 0.0)


class TestLoopReport(unittest.TestCase):
    """One event per window, not one per refused monitor tick (~6 s)."""

    def test_never_reported_is_due(self):
        self.assertTrue(_ledger().should_report(now=1000.0))

    def test_reported_this_window_is_not_due(self):
        ledger = _ledger()
        ledger.loop_reported_at = 1000.0
        self.assertFalse(ledger.should_report(now=1000.0 + WINDOW / 2))

    def test_reported_a_window_ago_is_due_again(self):
        ledger = _ledger()
        ledger.loop_reported_at = 1000.0
        self.assertTrue(ledger.should_report(now=1000.0 + WINDOW))


class TestRecord(unittest.TestCase):

    def test_the_key_is_cluster_node_port(self):
        self.assertEqual(_ledger().get_id(), "cl-1/d3a07483/9113")

    def test_round_trip_keeps_the_stamps(self):
        ledger = _ledger()
        ledger.record(now=1.5)
        ledger.loop_reported_at = 2.5
        copy = FenceReleaseLedger().from_dict(ledger.to_dict())
        self.assertEqual(copy.releases, [1.5])
        self.assertEqual(copy.loop_reported_at, 2.5)
        self.assertEqual(copy.get_id(), ledger.get_id())


if __name__ == "__main__":
    unittest.main()
