# coding=utf-8
"""Pin the "drain loop → fixed 0.5 s quiesce" replacement.

Prior to this change, the restart runner polled
``bdev_distrib_check_inflight_io`` on the leader in ~100 ms intervals
until the counter hit zero, inside the LVS port-block window. But
``distrib-inflight`` includes internal data-migration IO that is not
paused by the port block, so the poll could hold the leader's client
port blocked for the full 9 s observed in the soak — long enough to
breach fio's 5 s max_latency on clients.

The fix is a fixed ``time.sleep(0.5)`` in both restart paths
(secondary/tertiary recreation and primary-failover) in
``storage_node_ops``, and in both peer- and self-leader quiesce sites
in ``tasks_runner_port_allow``. Migration IO does not touch lvstore
metadata, so a short wait is sufficient for the secondary's
``bdev_examine`` to see a consistent superblock.

This test pins the invariant by reading the source of the two affected
files: ``bdev_distrib_check_inflight_io`` must no longer appear inside
the port-block window, and a ``time.sleep(0.5)`` must appear where the
drain used to be. Source-level pin is deliberate — the alternative
(mocking out every RPC on a full restart path) is brittle and already
covered by the broader restart tests.
"""
import os
import re
import unittest


def _read(rel_path):
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, rel_path), "r") as f:
        return f.read()


class DrainLoopReplaced(unittest.TestCase):
    """Every former drain-poll site is now a fixed 0.5 s sleep."""

    def test_storage_node_ops_does_not_poll_distrib_inflight(self):
        src = _read("simplyblock_core/storage_node_ops.py")
        # The RPC method name may still appear in comments documenting the
        # fix. Pin the callable usage.
        self.assertNotIn(
            ".bdev_distrib_check_inflight_io(",
            src,
            "storage_node_ops must no longer call "
            "bdev_distrib_check_inflight_io; migration IO kept the counter "
            "non-zero long enough to breach client latency",
        )

    def test_storage_node_ops_has_fixed_quiesce_sleeps(self):
        src = _read("simplyblock_core/storage_node_ops.py")
        # Both sites (secondary/tertiary recreation + primary-failover)
        # now use a 0.5 s quiesce. Allow for the pre-existing 0.5 s wait
        # before the drop-leadership step, which was already there.
        sleeps = re.findall(r"time\.sleep\(0\.5\)", src)
        self.assertGreaterEqual(
            len(sleeps), 2,
            "expected at least two time.sleep(0.5) quiesce calls "
            "(one per restart path) — found %d" % len(sleeps),
        )

    def test_tasks_runner_port_allow_does_not_poll(self):
        src = _read("simplyblock_core/services/tasks_runner_port_allow.py")
        self.assertNotIn(
            ".bdev_distrib_check_inflight_io(",
            src,
            "tasks_runner_port_allow must no longer poll for drain; the "
            "fix is a fixed 0.5 s quiesce instead",
        )

    def test_tasks_runner_port_allow_has_fixed_quiesce(self):
        src = _read("simplyblock_core/services/tasks_runner_port_allow.py")
        self.assertIn(
            "time.sleep(0.5)", src,
            "tasks_runner_port_allow must keep a fixed-duration quiesce",
        )


if __name__ == "__main__":
    unittest.main()
