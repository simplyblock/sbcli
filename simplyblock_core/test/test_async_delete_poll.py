"""
test_async_delete_poll.py – unit tests for the inline async->sync delete gate.

The API delete path issues the async delete on the leader and then polls
``bdev_lvol_get_lvol_delete_status`` before releasing the sync legs to the
non-leaders. A sync delete issued while the leader is still walking the
snapshot/clone metadata is the interleaving the delete protocol exists to
prevent, so the gate must fail CLOSED: anything other than a real "completed"
status leaves the object in_deletion for lvol_monitor.

No live FoundationDB or SPDK is required.
"""

import unittest
from unittest import mock

from simplyblock_core.controllers import lvol_controller


class _FakeRPC:
    """Returns a scripted sequence of delete-status values."""

    def __init__(self, statuses):
        self._statuses = list(statuses)
        self.calls = 0

    def bdev_lvol_get_lvol_delete_status(self, name):
        self.calls += 1
        if not self._statuses:
            raise AssertionError("polled more times than scripted")
        st = self._statuses.pop(0)
        if isinstance(st, Exception):
            raise st
        return st


class TestWaitAsyncDelete(unittest.TestCase):

    def setUp(self):
        # Keep the tests fast: the cadence itself is not under test here.
        patcher = mock.patch.object(lvol_controller.time, "sleep", lambda _: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_completed_immediately(self):
        rpc = _FakeRPC([0])
        self.assertTrue(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))
        self.assertEqual(rpc.calls, 1)

    def test_all_done_statuses_accepted(self):
        for st in lvol_controller.ASYNC_DELETE_DONE_STATUSES:
            with self.subTest(status=st):
                rpc = _FakeRPC([st])
                self.assertTrue(
                    lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))

    def test_running_then_completed(self):
        rpc = _FakeRPC([1, 1, 2])
        self.assertTrue(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))
        self.assertEqual(rpc.calls, 3)

    def test_leadership_changed_does_not_release_sync_legs(self):
        # -35 means this node is no longer the right target; the monitor owns
        # re-resolving it. Must not be retried and must not return True.
        rpc = _FakeRPC([-35])
        self.assertFalse(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))
        self.assertEqual(rpc.calls, 1)

    def test_no_async_request_does_not_release_sync_legs(self):
        rpc = _FakeRPC([4])
        self.assertFalse(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))

    def test_rpc_exception_fails_closed(self):
        rpc = _FakeRPC([RuntimeError("connection error")])
        self.assertFalse(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))

    def test_none_result_fails_closed(self):
        # The RPC layer returns None when the call itself failed.
        rpc = _FakeRPC([None])
        self.assertFalse(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))

    def test_false_result_is_not_mistaken_for_status_zero(self):
        # Regression: `False == 0` in Python, so a bare membership test against
        # the done-set reads a failed poll as "completed" and releases the sync
        # legs while the leader may still be walking metadata.
        rpc = _FakeRPC([False])
        self.assertFalse(lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))

    def test_times_out_while_still_running(self):
        # Never returns a terminal status: the gate must give up (bounded) and
        # hand the sync legs to lvol_monitor rather than spinning.
        rpc = _FakeRPC([1] * 500)
        with mock.patch.object(lvol_controller.time, "time",
                               side_effect=[0.0] + [0.0, 99.0] * 250):
            self.assertFalse(
                lvol_controller._wait_async_delete(rpc, "LVS_1/LVOL_1"))


if __name__ == "__main__":
    unittest.main()
