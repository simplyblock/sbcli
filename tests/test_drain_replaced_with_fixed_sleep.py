# coding=utf-8
"""Pin the post-2026-05-02 ordering of the drain vs. leadership-drop in
``recreate_lvstore``.

Background — there are two restart paths that block a peer's LVS port:

  1. ``recreate_lvstore_on_non_leader`` (secondary/tertiary's own
     restart): blocks the *configured primary* (= the actual leader,
     which runs data migration). Migration IO doesn't pause on port
     block, so polling ``bdev_distrib_check_inflight_io`` here was the
     original 10 s drain regression that breached client max_latency.
     This path keeps a fixed ``time.sleep(0.5)`` quiesce.

  2. ``recreate_lvstore`` line 5404 (primary's restart, failback path):
     blocks the *acting leader*, which is a secondary or tertiary that
     took over while the configured primary was out. **Migration never
     runs on a secondary or tertiary**, so the inflight counter genuinely
     drains here. This path *must* drain before the leadership drop —
     in-flight IO present at the moment of demote either gets redirected
     via the hub bdev (which may not be open yet on the new follower) or
     is aborted, both producing client-visible IO errors and qpair
     tear-downs (incident 2026-05-02, k8s_native_failover_ha-20260502-
     101452, worker1).

Pins:
  - ``recreate_lvstore`` (failback) calls
    ``bdev_distrib_check_inflight_io`` *before* the leader drop, with a
    bounded poll, and aborts the restart (via
    ``_abort_restart_and_unblock``) if the drain doesn't complete.
  - ``recreate_lvstore_on_non_leader`` keeps the fixed-duration quiesce
    and does not poll ``bdev_distrib_check_inflight_io``.
  - ``tasks_runner_port_allow`` does not poll
    ``bdev_distrib_check_inflight_io``.

Source-level pins (the alternative — mocking every RPC on a full
restart path — is brittle, and the broader restart tests cover the
runtime behavior).
"""
import os
import re
import unittest


def _read(rel_path):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel_path), "r") as f:
        return f.read()


def _slice(src: str, start_marker: str, end_marker: str) -> str:
    """Return the substring between two anchor lines, both included."""
    s = src.index(start_marker)
    e = src.index(end_marker, s + 1)
    return src[s:e]


class FailbackPath_DrainBeforeDrop(unittest.TestCase):
    """The failback-path block at recreate_lvstore line ~5404 must:
      (a) call bdev_distrib_check_inflight_io BEFORE
          bdev_lvol_set_leader(leader=False)
      (b) bound the poll
      (c) abort+unblock if drain times out
      (d) not have a fixed-duration quiesce surrounding the drop
    """

    @classmethod
    def setUpClass(cls):
        cls.src = _read("simplyblock_core/storage_node_ops.py")
        # Slice out the peer-leader takeover branch in recreate_lvstore.
        # The branch begins at "if current_leader and current_leader in blocked_peers:"
        # and runs until the next "###" section header.
        cls.takeover_block = _slice(
            cls.src,
            "if current_leader and current_leader in blocked_peers:",
            "if disconnected_peers:",
        )

    def test_drain_poll_present_in_failback_branch(self):
        self.assertIn(
            "bdev_distrib_check_inflight_io",
            self.takeover_block,
            "failback path must poll bdev_distrib_check_inflight_io to drain "
            "the acting leader before dropping its leadership",
        )

    def test_drain_runs_BEFORE_set_leader(self):
        idx_drain = self.takeover_block.index("bdev_distrib_check_inflight_io")
        idx_demote = self.takeover_block.index(
            "bdev_lvol_set_leader(lvs_name, leader=False"
        )
        self.assertLess(
            idx_drain, idx_demote,
            "drain poll must happen BEFORE bdev_lvol_set_leader(leader=False); "
            "otherwise in-flight IO at the moment of leadership transition "
            "lands on a non-leader lvstore (incident 2026-05-02)",
        )

    def test_drain_is_bounded(self):
        # A bound is required so a slow JM/distrib can't hold the leader's
        # port blocked beyond client max_latency.
        self.assertRegex(
            self.takeover_block,
            r"_DRAIN_BOUND_SEC\s*=\s*\d+",
            "drain must declare a bounded deadline (e.g. _DRAIN_BOUND_SEC = 2.0)",
        )

    def test_drain_timeout_aborts_and_unblocks(self):
        # On timeout we MUST NOT continue with the demote: the port stays
        # blocked, in-flight IO would still hit a non-leader lvstore.
        # _abort_restart_and_unblock kills the recovering node's SPDK,
        # unblocks every blocked peer, and raises so the restart task
        # runner re-queues.
        # The condition `if not drained` must lead to abort, not warn-and-proceed.
        m = re.search(r"if not drained:\s*\n([\s\S]+?)(?=\n\s{0,12}\#\#\#|\nexcept Exception)", self.takeover_block)
        self.assertIsNotNone(m, "couldn't find `if not drained:` branch in failback takeover")
        not_drained_body = m.group(1)
        self.assertIn(
            "_abort_restart_and_unblock", not_drained_body,
            "drain-timeout MUST call _abort_restart_and_unblock — port unblocked, "
            "restart aborted, task re-queued. Continuing past a non-empty distrib "
            "pipeline reproduces the very failure this drain is meant to prevent.",
        )

    def test_no_fixed_sleep_after_drop_in_failback_branch(self):
        # After the drop, the old code had `time.sleep(0.5)` (the
        # post-drop quiesce). It must be gone — the pre-drop drain
        # accomplishes the equivalent guarantee.
        idx_demote = self.takeover_block.index(
            "bdev_lvol_set_leader(lvs_name, leader=False"
        )
        post_demote = self.takeover_block[idx_demote:]
        self.assertNotIn(
            "time.sleep(0.5)",
            post_demote,
            "no post-drop fixed-sleep in the failback branch — the drain "
            "before the drop already guarantees an empty pipeline",
        )


class NonLeaderPath_KeepsFixedSleep(unittest.TestCase):
    """recreate_lvstore_on_non_leader keeps its fixed-duration quiesce.

    Different scenario: this path blocks the *configured primary* (the
    actual leader, which runs data migration). bdev_distrib_check_inflight_io
    counts migration IO too, so polling for zero does not settle —
    that's the original 10 s regression. A fixed 0.5 s wait is the
    pragmatic compromise because the secondary/tertiary's
    bdev_examine only needs lvstore-metadata coherence (migration IO
    doesn't touch metadata).
    """

    @classmethod
    def setUpClass(cls):
        src = _read("simplyblock_core/storage_node_ops.py")
        # Slice from the function definition to the next top-level def.
        start = src.index("def recreate_lvstore_on_non_leader(")
        end = src.index("\ndef ", start + 1)
        cls.fn = src[start:end]

    def test_does_not_poll_distrib_inflight(self):
        self.assertNotIn(
            ".bdev_distrib_check_inflight_io(",
            self.fn,
            "recreate_lvstore_on_non_leader must NOT poll inflight IO — "
            "the blocked node is the configured primary which runs data "
            "migration; the poll never settles and breaches client "
            "max_latency (the original 10 s regression).",
        )

    def test_has_fixed_quiesce(self):
        self.assertIn(
            "time.sleep(0.5)", self.fn,
            "recreate_lvstore_on_non_leader keeps a fixed-duration quiesce "
            "after blocking the leader's port (sufficient for the "
            "secondary's bdev_examine to see metadata-coherent superblock).",
        )


class PortAllowPath_NoPoll(unittest.TestCase):
    """tasks_runner_port_allow must not poll bdev_distrib_check_inflight_io.

    The leadership-manipulation block in port_allow was removed entirely
    in commit ec34de64 (the runner now only allows the port on the
    recovering node; leadership belongs to the JM heartbeat).
    """

    def test_no_inflight_poll(self):
        src = _read("simplyblock_core/services/tasks_runner_port_allow.py")
        self.assertNotIn(
            ".bdev_distrib_check_inflight_io(",
            src,
            "tasks_runner_port_allow must not poll inflight IO — the entire "
            "leadership-manipulation block was removed; port_allow now only "
            "allows the recovering node's port.",
        )

    def test_no_set_leader(self):
        src = _read("simplyblock_core/services/tasks_runner_port_allow.py")
        self.assertNotIn(
            "bdev_lvol_set_leader",
            src,
            "tasks_runner_port_allow must not manipulate leadership state",
        )


if __name__ == "__main__":
    unittest.main()
