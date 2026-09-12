# coding=utf-8
"""Hitting a hard object limit raises a WARNING in the cluster event log.

A refusal that only ever reaches the caller's return value is invisible to an
operator: the volume simply never appears, and the reason lives in whatever
CSI logged. Every object-limit refusal now also writes a LIMIT_REACHED event.

The rate is the whole design problem. A limit refusal is driven by the
CALLER's retry loop, not by anything happening in the cluster: CSI re-issues
a rejected create every few seconds, so an event per attempt would bury the
log exactly the way the jm_compression started/finished events did. Hence the
per-(cluster, limit, object) cooldown asserted here.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.models.events import EventObj


def _obj(name="Pool"):
    o = MagicMock()
    o.name = name
    o.get_clean_dict.return_value = {"uuid": "obj-1"}
    return o


class TestLimitEventEmission(unittest.TestCase):

    def setUp(self):
        ec._limit_event_last.clear()
        self.addCleanup(ec._limit_event_last.clear)
        p = patch.object(ec, "log_event_cluster", return_value={"uuid": "ev-1"})
        self.log = p.start()
        self.addCleanup(p.stop)

    def test_emits_a_warning_level_cluster_event(self):
        ret = ec.log_object_limit_reached("c1", _obj(), "Snapshot limit reached",
                                          limit_key="snapshots:lv1")
        self.assertIsNotNone(ret)
        self.log.assert_called_once()
        kwargs = self.log.call_args.kwargs
        self.assertEqual(kwargs["cluster_id"], "c1")
        self.assertEqual(kwargs["event"], ec.EVENT_LIMIT_REACHED)
        self.assertEqual(kwargs["event_level"], EventObj.LEVEL_WARN)
        self.assertEqual(kwargs["message"], "Snapshot limit reached")

    def test_repeat_within_the_cooldown_is_swallowed(self):
        for _ in range(60):
            ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="snapshots:lv1")
        self.assertEqual(self.log.call_count, 1)

    def test_a_different_object_is_a_different_event(self):
        ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="snapshots:lv1")
        ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="snapshots:lv2")
        self.assertEqual(self.log.call_count, 2)

    def test_a_different_cluster_is_a_different_event(self):
        ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="snapshots:lv1")
        ec.log_object_limit_reached("c2", _obj(), "msg", limit_key="snapshots:lv1")
        self.assertEqual(self.log.call_count, 2)

    def test_it_re_arms_after_the_cooldown(self):
        base = 1000.0
        with patch.object(ec.time, "monotonic", side_effect=[base, base + ec.LIMIT_EVENT_COOLDOWN_SEC + 1]):
            ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="k")
            ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="k")
        self.assertEqual(self.log.call_count, 2)

    def test_a_logging_failure_never_propagates(self):
        """The refusal is orderly; failing to record it must not turn the
        caller's clean error into an internal error."""
        self.log.side_effect = RuntimeError("fdb down")
        self.assertIsNone(
            ec.log_object_limit_reached("c1", _obj(), "msg", limit_key="k"))

    def test_cooldown_is_long_enough_to_outlast_a_retry_storm(self):
        self.assertGreaterEqual(ec.LIMIT_EVENT_COOLDOWN_SEC, 60)


class TestEveryLimitRefusalIsWired(unittest.TestCase):
    """Source-level: each admission point that returns a limit error also
    raises the event. Checked on source because the surrounding call paths
    need a live DB/RPC to reach."""

    @staticmethod
    def _src(rel):
        import os
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        with open(os.path.join(root, rel), encoding="utf-8") as f:
            return f.read()

    def test_lvol_controller_sites(self):
        src = self._src("simplyblock_core/controllers/lvol_controller.py")
        self.assertEqual(src.count("events_controller.log_object_limit_reached("), 3)
        self.assertIn('limit_key="lvol_size"', src)
        self.assertIn('limit_key=f"lvstore_objects:', src)

    def test_snapshot_controller_sites(self):
        src = self._src("simplyblock_core/controllers/snapshot_controller.py")
        self.assertEqual(src.count("events_controller.log_object_limit_reached("), 5)
        self.assertIn('limit_key=f"snapshots:', src)
        self.assertIn('limit_key=f"clones:', src)

    def test_no_limit_refusal_is_left_silent(self):
        """Every `return False, <limit error>` / raise in the limit blocks is
        preceded by an event call. Counted rather than parsed: the two
        controllers hold 8 limit refusals between them -- volume size on
        create and on resize, the per-lvstore object cap on create / snapshot
        / clone, the per-volume snapshot cap, the per-snapshot clone cap, and
        the size cap on clone --resize."""
        total = sum(self._src(p).count("events_controller.log_object_limit_reached(")
                    for p in ("simplyblock_core/controllers/lvol_controller.py",
                              "simplyblock_core/controllers/snapshot_controller.py"))
        self.assertEqual(total, 8)


if __name__ == "__main__":
    unittest.main()
