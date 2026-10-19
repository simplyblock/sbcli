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

import inspect
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.events import EventObj
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
        src = inspect.getsource(monitor._update_cluster_status_impl)
        self.assertNotIn("_delete_old_tasks(", src)
        self.assertNotIn("_delete_old_logs(", src)

    def test_housekeeping_runs_then_holds_for_the_interval(self):
        with patch.object(monitor, "_delete_old_tasks") as tasks, \
                patch.object(monitor, "_delete_old_logs") as logs, \
                patch.object(monitor, "db") as db:
            db.get_job_tasks.return_value = []
            monitor._run_periodic_housekeeping("cl-1")
            monitor._run_periodic_housekeeping("cl-1")
        tasks.assert_called_once()
        logs.assert_called_once()

    def test_housekeeping_failure_is_contained(self):
        with patch.object(monitor, "db") as db, \
                patch.object(monitor, "logger"):
            db.get_job_tasks.side_effect = RuntimeError("fdb gone")
            monitor._run_periodic_housekeeping("cl-1")   # must not raise

    def test_main_loop_does_no_housekeeping(self):
        """2026-10-19: a sweep in the main loop held the cluster status at
        ACTIVE for 7 minutes after two nodes went offline. Housekeeping runs
        on its own thread; the main loop only refreshes the status."""
        main = inspect.getsource(monitor.main)
        self.assertNotIn("_run_periodic_housekeeping(", main)
        self.assertIn("_start_housekeeping()", main)

    def test_housekeeping_thread_is_a_daemon(self):
        with patch.object(monitor.threading, "Thread") as thread:
            monitor._start_housekeeping()
        thread.assert_called_once_with(
            target=monitor._housekeeping_loop, name="housekeeping", daemon=True)
        thread.return_value.start.assert_called_once_with()

    def test_housekeeping_loop_sweeps_every_cluster(self):
        clusters = [MagicMock(), MagicMock()]
        clusters[0].get_id.return_value = "cl-1"
        clusters[1].get_id.return_value = "cl-2"

        class _Stop(Exception):
            pass

        with patch.object(monitor, "db") as db, \
                patch.object(monitor, "_run_periodic_housekeeping") as sweep, \
                patch.object(monitor.time, "sleep", side_effect=_Stop):
            db.get_clusters.return_value = clusters
            with self.assertRaises(_Stop):
                monitor._housekeeping_loop()
        self.assertEqual([c.args[0] for c in sweep.call_args_list], ["cl-1", "cl-2"])


class _RangeStore:
    """Sorted byte-key store with FDB's clear_range semantics."""

    def __init__(self):
        self.data = {}

    def clear_range(self, begin, end):
        for k in [k for k in self.data if begin <= k < end]:
            del self.data[k]


class TestDeleteOldLogs(unittest.TestCase):

    def _event(self, store, cluster_id, age_sec):
        ev = EventObj()
        ev.uuid = f"{cluster_id}-{age_sec}"
        ev.cluster_uuid = cluster_id
        ev.date = round((time.time() - age_sec) * 1000)
        key = ev.get_db_id().encode()
        store.data[key] = b"{}"
        return key

    def test_clears_only_this_clusters_expired_events(self):
        store = _RangeStore()
        old = constants.TASKS_RETENTION_PERIOD_SEC + 3600
        expired = self._event(store, "cl-1", old)
        recent = self._event(store, "cl-1", 60)
        other_cluster = self._event(store, "cl-2", old)
        with patch.object(monitor, "db") as db:
            db.kv_store = store
            monitor._delete_old_logs("cl-1")
            db.get_events.assert_not_called()   # no full-table read
        self.assertNotIn(expired, store.data)
        self.assertIn(recent, store.data)
        self.assertIn(other_cluster, store.data)

    def test_cluster_id_prefix_does_not_leak(self):
        """'cl-1' must not match the events of 'cl-10'."""
        store = _RangeStore()
        old = constants.TASKS_RETENTION_PERIOD_SEC + 3600
        neighbour = self._event(store, "cl-10", old)
        with patch.object(monitor, "db") as db:
            db.kv_store = store
            monitor._delete_old_logs("cl-1")
        self.assertIn(neighbour, store.data)


if __name__ == "__main__":
    unittest.main()
