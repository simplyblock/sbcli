"""test_repair_fanout.py — repairs of independent objects run together.

One health cycle used to dial every degraded controller in series, so a node
with a dozen degraded remote devices spent that many round-trips before its
hublvol was even inspected. Repairs are now collected during the read-only
inspection and executed as a group, and one failing repair must not take the
rest of the group with it.
"""

import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import health_check_service as svc


class TestRepairFanout(unittest.TestCase):

    def _jobs(self, count):
        return [(f"ctrl{i}", MagicMock(), MagicMock()) for i in range(count)]

    def test_jobs_run_on_several_threads_and_overlap(self):
        # A barrier, not a wall-clock bound: each repair passes only while a
        # second one is running, so overlap is proven however slow the runner
        # is. Run serially, the first wait times out and breaks the barrier.
        barrier = threading.Barrier(2, timeout=10)
        met, lock = [], threading.Lock()

        def meet(ctrl, dev, node):
            barrier.wait()
            with lock:
                met.append(ctrl)

        with patch.object(svc.storage_node_ops, "repair_multipath_controller", meet):
            svc._run_repairs_in_parallel(self._jobs(4), "device")

        self.assertEqual(sorted(met), ["ctrl0", "ctrl1", "ctrl2", "ctrl3"],
                         "repairs did not overlap")

    def test_one_failure_does_not_abort_the_group(self):
        done = []

        def flaky(ctrl, dev, node):
            if ctrl == "ctrl1":
                raise RuntimeError("connect refused")
            done.append(ctrl)

        with patch.object(svc.storage_node_ops, "repair_multipath_controller", flaky):
            svc._run_repairs_in_parallel(self._jobs(4), "device")

        self.assertEqual(sorted(done), ["ctrl0", "ctrl2", "ctrl3"])

    def test_empty_job_list_costs_nothing(self):
        with patch.object(svc.storage_node_ops, "repair_multipath_controller") as rep:
            svc._run_repairs_in_parallel([], "device")
        rep.assert_not_called()

    def test_fanout_is_bounded(self):
        """More jobs than the cap must not spawn one thread per job."""
        threads, lock = set(), threading.Lock()

        def slow(ctrl, dev, node):
            with lock:
                threads.add(threading.current_thread().name)
            time.sleep(0.05)

        with patch.object(svc.storage_node_ops, "repair_multipath_controller", slow):
            svc._run_repairs_in_parallel(self._jobs(svc.REPAIR_FANOUT * 3), "device")

        self.assertLessEqual(len(threads), svc.REPAIR_FANOUT)


if __name__ == "__main__":
    unittest.main()
