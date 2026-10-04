"""test_ucs_debounce_bound.py — the cluster-status debounce must not capture
its computing thread.

Run mass_create_delete_docker-20260821: the main loop marks the cluster dirty
every 3 seconds, and at 12k entities one recompute pass took ~4.3 seconds —
so `pending` was always set again before a pass ended, and the per-node
monitor thread that happened to be computing looped cluster passes for 10.6
hours. Its node's SPDK was killed 30 times in that window with nothing
watching: the node stayed "online" in the DB with no SPDK process and every
lvol delete returned "No leader available".

The debounce's own comment always promised "at most two sequential passes";
these tests enforce it, along with the housekeeping cadence that made the
passes slow in the first place.
"""

import threading
import unittest
from unittest.mock import patch

from simplyblock_core.services import storage_node_monitor as monitor


class TestDebounceIsBounded(unittest.TestCase):

    def setUp(self):
        monitor._ucs_running.clear()
        monitor._ucs_pending.clear()

    def test_at_most_two_passes_even_if_always_dirty(self):
        """A caller re-arming pending during every pass must not loop forever."""
        passes = []

        def impl(cluster_id):
            passes.append(cluster_id)
            # Simulate the 3s main-loop tick landing during every pass.
            with monitor._ucs_state_lock:
                monitor._ucs_pending[cluster_id] = True

        with patch.object(monitor, "_update_cluster_status_impl", impl):
            monitor.update_cluster_status("cl-1")

        self.assertEqual(len(passes), 2,
                         "the computing thread was captured beyond two passes")
        self.assertFalse(monitor._ucs_running.get("cl-1"))

    def test_single_pass_when_nothing_re_arms(self):
        with patch.object(monitor, "_update_cluster_status_impl") as impl:
            monitor.update_cluster_status("cl-1")
        self.assertEqual(impl.call_count, 1)

    def test_concurrent_caller_still_just_marks_pending(self):
        entered = threading.Event()
        release = threading.Event()
        calls = []

        def slow_impl(cluster_id):
            calls.append(cluster_id)
            entered.set()
            release.wait(5)

        with patch.object(monitor, "_update_cluster_status_impl", slow_impl):
            worker = threading.Thread(target=monitor.update_cluster_status,
                                      args=("cl-1",))
            worker.start()
            entered.wait(5)
            monitor.update_cluster_status("cl-1")   # must return immediately
            self.assertTrue(monitor._ucs_pending.get("cl-1"))
            release.set()
            worker.join(5)
        self.assertEqual(len(calls), 2, "the pending mark should buy one re-pass")

    def test_state_is_clean_after_exit(self):
        def impl(cluster_id):
            with monitor._ucs_state_lock:
                monitor._ucs_pending[cluster_id] = True

        with patch.object(monitor, "_update_cluster_status_impl", impl):
            monitor.update_cluster_status("cl-1")
        self.assertFalse(monitor._ucs_running.get("cl-1"))
        self.assertNotIn("cl-1", monitor._ucs_pending,
                         "a stale pending flag would leak into the next episode")


class TestHousekeepingCadence(unittest.TestCase):

    def setUp(self):
        monitor._housekeeping_last_run.clear()

    def test_recompute_pass_does_no_housekeeping(self):
        """The full-table scans must be gone from the per-pass path."""
        import inspect
        src = inspect.getsource(monitor._update_cluster_status_impl)
        self.assertNotIn("_delete_old_tasks(", src)
        self.assertNotIn("_delete_old_logs(", src)

    def test_housekeeping_runs_then_holds_for_the_interval(self):
        with patch.object(monitor, "_delete_old_tasks") as tasks, \
                patch.object(monitor, "_delete_old_logs") as logs, \
                patch.object(monitor, "db") as db:
            db.get_job_tasks.return_value = []
            db.get_events.return_value = []
            monitor._run_periodic_housekeeping("cl-1")
            monitor._run_periodic_housekeeping("cl-1")
        tasks.assert_called_once()
        logs.assert_called_once()

    def test_housekeeping_failure_is_contained(self):
        with patch.object(monitor, "db") as db, \
                patch.object(monitor, "logger"):
            db.get_job_tasks.side_effect = RuntimeError("fdb gone")
            monitor._run_periodic_housekeeping("cl-1")   # must not raise


if __name__ == "__main__":
    unittest.main()
