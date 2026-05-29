# coding=utf-8
"""
test_soak_outage_gap.py — unit tests for the mixed-churn soak's
inter-outage gap policy.

Targets:
- ``SoakRunner._expected_min_unavail_seconds`` — lower bound on how long
  a node is observably not-online after each outage method. Used to cap
  the gap.
- ``SoakRunner._pick_outage_gap`` — random gap in
  [--outage-gap-min, --outage-gap-max], capped per method 1 so the
  caller-requested --min-outage-overlap is guaranteed (both nodes are
  simultaneously not-online for at least that many seconds).

These tests exist because the gap policy is the only mechanism guarding
the "two nodes are not-online for >= 10s" invariant that the soak is
contracted to exercise. If the cap math drifts, the soak silently stops
covering the dual-outage path it was built for.
"""

import importlib.util
import os
import random
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock


def _load_soak_module():
    """Load the soak script as a module without invoking its main().

    The script lives under scripts/ and is intended to be run with
    `python scripts/aws_dual_node_outage_soak_mixed_churn.py`. It does
    not run anything at import time (only argparse + class defs), so a
    plain importlib load gives us the SoakRunner class.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(repo_root, "scripts",
                        "aws_dual_node_outage_soak_mixed_churn.py")
    spec = importlib.util.spec_from_file_location("_soak_mixed_churn", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_soak_mixed_churn"] = mod
    spec.loader.exec_module(mod)
    return mod


def _runner_stub(**arg_overrides):
    """Bare SoakRunner instance with .args populated and .logger stubbed.

    Bypasses SoakRunner.__init__ (which connects to remote hosts) via
    object.__new__. Only the fields touched by the methods under test
    need to be set.
    """
    mod = _load_soak_module()
    runner = object.__new__(mod.SoakRunner)
    default_args = dict(
        shutdown_gap=0,
        outage_gap_min=15,
        outage_gap_max=180,
        min_outage_overlap=10,
    )
    default_args.update(arg_overrides)
    runner.args = SimpleNamespace(**default_args)
    runner.logger = MagicMock()
    return mod, runner


class TestExpectedMinUnavail(unittest.TestCase):

    def setUp(self):
        self.mod, self.runner = _runner_stub()

    def test_network_outage_parses_duration(self):
        # The method name encodes the NIC-down duration; that is the
        # conservative floor on node-not-online time.
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("network_outage_20"), 20)
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("network_outage_50"), 50)
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("network_outage_120"), 120)

    def test_network_outage_malformed_falls_back(self):
        # Defensive: if the trailing token isn't a number, fall back to
        # the generic 30s floor rather than raising.
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("network_outage_xxx"), 30)

    def test_container_kill_and_host_reboot_floors(self):
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("container_kill"), 30)
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("host_reboot"), 90)

    def test_graceful_and_forced_are_effectively_unbounded(self):
        # graceful/forced leave the node OFFLINE until run_outage_pair
        # later issues `sn restart`, so from the gap's perspective the
        # unavailability window is effectively unbounded. The sentinel
        # has to be large enough that `cap = unavail - min_overlap` will
        # never clip the user-configured max gap (default 180s).
        for m in ("graceful", "forced"):
            with self.subTest(method=m):
                v = self.runner._expected_min_unavail_seconds(m)
                self.assertGreaterEqual(v, 1000,
                    "graceful/forced must not be clipped by the gap cap")

    def test_unknown_method_defaults(self):
        self.assertEqual(
            self.runner._expected_min_unavail_seconds("future_chaos"), 30)


class TestPickOutageGap(unittest.TestCase):

    def setUp(self):
        self.mod, self.runner = _runner_stub()
        # Determinism: lock random.randint for repeatable assertions on
        # bounds. random.uniform is not used by _pick_outage_gap.
        self._orig_randint = random.randint
        self.calls = []
        random.randint = lambda a, b: (self.calls.append((a, b)) or a)

    def tearDown(self):
        random.randint = self._orig_randint

    def _last_bounds(self):
        self.assertTrue(self.calls, "random.randint was not called")
        return self.calls[-1]

    # --- graceful / forced: no clipping --------------------------------

    def test_graceful_uses_full_configured_range(self):
        self.runner._pick_outage_gap("graceful")
        lo, hi = self._last_bounds()
        self.assertEqual(lo, 15)
        self.assertEqual(hi, 180)

    def test_forced_uses_full_configured_range(self):
        self.runner._pick_outage_gap("forced")
        lo, hi = self._last_bounds()
        self.assertEqual(lo, 15)
        self.assertEqual(hi, 180)

    # --- network_outage_N: cap = N - min_overlap -----------------------

    def test_network_outage_50_caps_at_40_with_default_overlap(self):
        # N=50, overlap=10 -> cap=40 < max=180 -> hi clipped to 40,
        # lo stays at min=15.
        self.runner._pick_outage_gap("network_outage_50")
        lo, hi = self._last_bounds()
        self.assertEqual(hi, 40)
        self.assertEqual(lo, 15)

    def test_network_outage_20_clamps_both_lo_and_hi(self):
        # N=20, overlap=10 -> cap=10. That's below min=15 too, so lo is
        # clamped to hi (10). Result: deterministic 10s gap on this
        # method. This is correct: a 20s NIC drop cannot also satisfy
        # a 15s gap + 10s overlap; the overlap invariant wins.
        self.runner._pick_outage_gap("network_outage_20")
        lo, hi = self._last_bounds()
        self.assertEqual(hi, 10)
        self.assertEqual(lo, 10)

    def test_container_kill_caps_at_20(self):
        # unavail=30, overlap=10 -> cap=20.
        self.runner._pick_outage_gap("container_kill")
        lo, hi = self._last_bounds()
        self.assertEqual(hi, 20)
        self.assertEqual(lo, 15)

    def test_host_reboot_caps_at_80(self):
        # unavail=90, overlap=10 -> cap=80.
        self.runner._pick_outage_gap("host_reboot")
        lo, hi = self._last_bounds()
        self.assertEqual(hi, 80)
        self.assertEqual(lo, 15)

    # --- min-outage-overlap edge cases ---------------------------------

    def test_zero_overlap_disables_cap_for_short_methods(self):
        # With overlap=0 the cap equals unavail itself. A network_outage_20
        # then permits up to a 20s gap.
        self.mod, self.runner = _runner_stub(min_outage_overlap=0)
        self.runner._pick_outage_gap("network_outage_20")
        lo, hi = self._last_bounds()
        self.assertEqual(hi, 20)
        self.assertEqual(lo, 15)

    def test_overlap_larger_than_unavail_clamps_to_one(self):
        # Pathological: ask for 60s overlap on a network_outage_20.
        # cap = max(1, 20 - 60) = 1. The gap collapses to 1s — the
        # overlap invariant cannot be met by any positive gap, so the
        # implementation chooses the smallest legal gap (1s) and lets
        # the caller observe the violation via the warning emitted at
        # a higher layer (or via the runtime overlap check).
        self.mod, self.runner = _runner_stub(min_outage_overlap=60)
        self.runner._pick_outage_gap("network_outage_20")
        lo, hi = self._last_bounds()
        self.assertEqual((lo, hi), (1, 1))

    # --- legacy --shutdown-gap takes precedence ------------------------

    def test_shutdown_gap_legacy_overrides_random(self):
        self.mod, self.runner = _runner_stub(shutdown_gap=45)
        gap = self.runner._pick_outage_gap("network_outage_50")
        self.assertEqual(gap, 45)
        # random.randint must NOT have been consulted.
        self.assertEqual(self.calls, [])

    def test_shutdown_gap_warns_when_exceeding_safe_cap(self):
        # --shutdown-gap=200 on network_outage_20 is unsafe (cap=10).
        # The method must still return the legacy value but emit a
        # warning so operators notice.
        self.mod, self.runner = _runner_stub(shutdown_gap=200)
        gap = self.runner._pick_outage_gap("network_outage_20")
        self.assertEqual(gap, 200)
        warned = any(
            "exceeds" in str(call) and "safe cap" in str(call)
            for call in self.runner.logger.log.call_args_list
        )
        self.assertTrue(warned,
            f"expected a 'safe cap' warning log; got {self.runner.logger.log.call_args_list}")


class TestPickOutageGapDistribution(unittest.TestCase):
    """Sanity check that the random gap stays within the documented
    bounds across many draws, for a representative selection of
    method 1 values. No mocking of random — real distribution test.
    """

    def setUp(self):
        self.mod, self.runner = _runner_stub()

    def _bounds_for(self, method):
        """Replicate the cap math the implementation uses."""
        unavail = self.runner._expected_min_unavail_seconds(method)
        overlap = self.runner.args.min_outage_overlap
        cap = max(1, unavail - overlap)
        lo = max(1, self.runner.args.outage_gap_min)
        hi = max(lo, self.runner.args.outage_gap_max)
        hi = min(hi, cap)
        lo = min(lo, hi)
        return lo, hi

    def test_all_draws_inside_bounds(self):
        for method in ("graceful", "forced", "container_kill", "host_reboot",
                       "network_outage_20", "network_outage_50"):
            lo, hi = self._bounds_for(method)
            for _ in range(200):
                g = self.runner._pick_outage_gap(method)
                self.assertGreaterEqual(g, lo,
                    f"{method}: gap {g} below lo {lo}")
                self.assertLessEqual(g, hi,
                    f"{method}: gap {g} above hi {hi}")

    def test_overlap_invariant_holds(self):
        # The whole point: gap + min_overlap <= expected_min_unavail.
        # For graceful/forced this is trivially true (huge sentinel).
        # For network_outage_N this is the tight constraint.
        for method in ("container_kill", "host_reboot",
                       "network_outage_20", "network_outage_50"):
            unavail = self.runner._expected_min_unavail_seconds(method)
            overlap = self.runner.args.min_outage_overlap
            for _ in range(200):
                g = self.runner._pick_outage_gap(method)
                self.assertLessEqual(
                    g + overlap, unavail,
                    f"{method}: gap {g} + overlap {overlap} > unavail {unavail}"
                    " — the overlap invariant the cap exists to preserve has"
                    " been violated")


if __name__ == "__main__":
    unittest.main()
