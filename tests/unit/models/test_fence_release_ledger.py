"""The fence-release ledger (models/fence_release.py): the memory the
monitor, the health check and the port_allow runner share about how often an
lvstore's client ports have been reopened.

prod ams1, 2026-11-01 08:52-09:01. One stripe of LVS_4033 had only three
chunks and could not be served while one of its nodes rebooted. Every retried
read made the leader fence port 9113, the next retry moved leadership and the
fence to a peer, and within 12-17 s the control plane reopened each port
again: nine port_allow tasks, 60 leadership switches, 144 failed reads. The
budget here is what makes the third reopen of any of the lvstore's paths in
ten minutes the last one for the non-preferred paths.

Keyed per lvstore, not per (node, port): the loop hops between the three
nodes of the lvstore, so a per-node count would hand every path its own
budget (review of #1580).
"""

import unittest

from simplyblock_core import constants
from simplyblock_core.models.fence_release import FenceReleaseLedger

BUDGET = constants.FENCE_RELEASE_BUDGET
WINDOW = constants.FENCE_RELEASE_WINDOW_SEC
LEADER, SEC, TER = "d3a07483", "428ae3c4", "438a0878"


def _ledger():
    ledger = FenceReleaseLedger()
    ledger.cluster_id = "cl-1"
    ledger.lvstore = "LVS_4033"
    return ledger


class TestBudget(unittest.TestCase):

    def test_a_fresh_ledger_has_budget(self):
        self.assertFalse(_ledger().exhausted(now=1000.0))

    def test_one_release_short_of_the_budget_still_has_budget(self):
        ledger = _ledger()
        for i in range(BUDGET - 1):
            ledger.record(LEADER, now=1000.0 + i)
        self.assertFalse(ledger.exhausted(now=1010.0))

    def test_the_budget_is_exhausted_at_the_limit(self):
        """The incident: three releases in under a minute on one node."""
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(LEADER, now=1000.0 + 15 * i)
        self.assertTrue(ledger.exhausted(now=1000.0 + 15 * BUDGET))

    def test_releases_on_different_paths_add_up(self):
        """The loop hops: leader, secondary, tertiary each released once is
        already the budget, not one third of it."""
        ledger = _ledger()
        for i, node in enumerate((LEADER, SEC, TER)[:BUDGET]):
            ledger.record(node, now=1000.0 + 15 * i)
        self.assertEqual(ledger.count(now=1100.0), BUDGET)
        self.assertTrue(ledger.exhausted(now=1100.0))

    def test_releases_outside_the_window_do_not_count(self):
        """A leaked fence lifted once an hour is not a loop."""
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(LEADER, now=1000.0 + i * (WINDOW + 1))
        last = 1000.0 + (BUDGET - 1) * (WINDOW + 1)
        self.assertFalse(ledger.exhausted(now=last + 1))
        self.assertEqual(ledger.count(now=last + 1), 1, "prune keeps only the window")

    def test_the_budget_reopens_when_the_oldest_release_ages_out(self):
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(LEADER, now=1000.0 + i)
        self.assertTrue(ledger.exhausted(now=1000.0 + BUDGET))
        reopen = ledger.reopens_in(now=1000.0 + BUDGET)
        self.assertAlmostEqual(reopen, WINDOW - BUDGET)
        self.assertFalse(ledger.exhausted(now=1000.0 + WINDOW + 0.5))

    def test_reopens_in_is_zero_with_nothing_recorded(self):
        self.assertEqual(_ledger().reopens_in(now=5.0), 0.0)

    def test_a_pruned_node_disappears_from_the_record(self):
        ledger = _ledger()
        ledger.record(LEADER, now=1000.0)
        ledger.record(SEC, now=1000.0 + WINDOW)
        ledger.prune(now=1000.0 + WINDOW + 1)
        self.assertEqual(set(ledger.releases), {SEC})


class TestRestartForgets(unittest.TestCase):
    """A restart reopens the node's ports by design; three restarts in ten
    minutes must not leave the fourth one fenced (review of #1580)."""

    def test_forgetting_a_node_drops_only_its_stamps(self):
        ledger = _ledger()
        for i in range(BUDGET):
            ledger.record(LEADER, now=1000.0 + i)
        ledger.record(SEC, now=1010.0)
        self.assertTrue(ledger.exhausted(now=1020.0))
        ledger.forget(LEADER)
        self.assertFalse(ledger.exhausted(now=1020.0))
        self.assertEqual(ledger.count(now=1020.0), 1)

    def test_forgetting_an_unknown_node_is_a_no_op(self):
        ledger = _ledger()
        ledger.record(SEC, now=1010.0)
        ledger.forget(TER)
        self.assertEqual(ledger.count(now=1020.0), 1)


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

    def test_the_key_is_cluster_and_lvstore(self):
        self.assertEqual(_ledger().get_id(), "cl-1/LVS_4033")

    def test_round_trip_keeps_the_stamps_per_node(self):
        ledger = _ledger()
        ledger.record(LEADER, now=1.5)
        ledger.record(SEC, now=2.0)
        ledger.loop_reported_at = 2.5
        copy = FenceReleaseLedger().from_dict(ledger.to_dict())
        self.assertEqual(copy.releases, {LEADER: [1.5], SEC: [2.0]})
        self.assertEqual(copy.loop_reported_at, 2.5)
        self.assertEqual(copy.get_id(), ledger.get_id())


if __name__ == "__main__":
    unittest.main()
