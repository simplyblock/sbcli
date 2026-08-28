"""Case 11's schedule must retain a ladder, not collapse to the keep-floor.

Soak run 20260827_224741: two volumes on a 1-minute cadence for 124 minutes
with `--retention-schedule 5m:15m,7m:30m,10m:1h` ended with 2 and 3 internal
snapshots, all at CONSECUTIVE cadence ticks (63-64s apart). A working ladder
cannot produce consecutive minutes -- its finest tier keeps one per 5 minutes
-- so either the cadence never ran or retention collapsed to the flat
keep-newest floor.

This pins the retention half of that question with no cluster: feed the real
schedule the snapshot history the run should have produced and assert the
ladder survives.
"""
import unittest

from simplyblock_core.snapshot_retention import (
    horizon_sec, parse_schedule, select_retained,
)

CASE11_SCHEDULE = "5m:15m,7m:30m,10m:1h"
MINUTE = 60


class TestCase11Ladder(unittest.TestCase):

    def setUp(self):
        self.tiers = parse_schedule(CASE11_SCHEDULE)
        self.now = 1_787_870_000.0
        # 124 minutes of a 1-minute cadence, as the run actually took them
        self.history = [self.now - m * MINUTE for m in range(1, 125)]

    def test_schedule_parses_to_the_expected_ladder(self):
        self.assertEqual([(t.every_sec, t.span_sec) for t in self.tiers],
                         [(300, 900), (420, 1800), (600, 3600)])
        # 15m + 30m + 1h of coverage
        self.assertEqual(horizon_sec(self.tiers), 900 + 1800 + 3600)

    def test_the_ladder_retains_a_thinning_history_not_two_snapshots(self):
        keep = select_retained(self.history, self.tiers, self.now,
                               always_keep_newest=2)
        # 15m/5m = 3, then 30m/7m ~= 4-5, then 60m/10m = 6
        self.assertGreaterEqual(
            len(keep), 12,
            "the ladder should retain roughly a dozen points across 105 "
            "minutes of horizon; collapsing to the keep-floor is the bug this "
            "test exists for (got %d)" % len(keep))
        self.assertLessEqual(len(keep), 16, "and it must not keep everything")

    def test_retained_points_are_spread_not_consecutive(self):
        """The observed failure looked like consecutive cadence ticks."""
        keep = sorted(select_retained(self.history, self.tiers, self.now,
                                      always_keep_newest=2), reverse=True)
        # Beyond the protected newest pair, retained points must be spread by
        # roughly a tier interval. Gaps at a TIER BOUNDARY are legitimately
        # smaller (the last bucket of one tier can sit close to the first of
        # the next), so assert on the typical gap, not the minimum.
        gaps = sorted(keep[i] - keep[i + 1] for i in range(2, len(keep) - 1))
        self.assertTrue(gaps, "expected several retained points")
        median = gaps[len(gaps) // 2]
        self.assertGreaterEqual(
            median, 300,
            "retained snapshots a cadence tick apart mean the ladder is not "
            "being applied; the flat keep-N path produces exactly that "
            "(gaps: %s)" % [int(g) for g in gaps])

    def test_snapshots_older_than_the_horizon_are_dropped(self):
        older = [self.now - 200 * MINUTE, self.now - 300 * MINUTE]
        keep = select_retained(self.history + older, self.tiers, self.now,
                               always_keep_newest=2)
        for t in older:
            self.assertNotIn(t, keep,
                             "past the 105-minute horizon nothing is retained")

    def test_an_absent_schedule_falls_back_to_the_keep_floor(self):
        """The suspected production path: no tiers -> only the newest N.

        This is what the soak looked like, and it is CORRECT behaviour for an
        empty schedule -- which is why an empty schedule reaching retention
        silently destroys the history an operator asked for.
        """
        keep = select_retained(self.history, [], self.now, always_keep_newest=2)
        self.assertEqual(len(keep), 2)
        self.assertEqual(sorted(keep, reverse=True), self.history[:2],
                         "the two newest, i.e. consecutive cadence ticks -- "
                         "exactly the shape the soak produced")


if __name__ == "__main__":
    unittest.main()
