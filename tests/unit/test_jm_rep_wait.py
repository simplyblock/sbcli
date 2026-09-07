"""
test_jm_rep_wait.py — pins the bounds on wait_for_jm_rep_tasks_to_finish.

Incident 2026-08-17 22:03 (multipath soak, iteration 1). A container_kill on
node A started JM history replication on peer B. A's restart entered this wait,
B answered once ("replication task found"), then B was host-rebooted ~1 s later.
Every subsequent jc_get_jm_status raised, and the old ``except`` branch neither
decremented ``retry`` nor slept — so the loop spun for 50 minutes, paced only by
urllib3's connect-retries.

The consequence was a three-way deadlock: A stayed RESTARTING forever, which
deferred B's own restart (tasks_runner_restart's strict one-restart-at-a-time),
and StorageNodeMonitor stood down because B "has an active restart task", so
B's SPDK was never restarted — and A was waiting on exactly that SPDK.

Every exit from this loop must therefore be bounded, and a peer the control
plane already considers dead must not be waited on at all.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode


def _node(status=StorageNode.STATUS_ONLINE, rpc=None):
    node = StorageNode({"uuid": "aaaaaaaa-0000-0000-0000-000000000000"})
    node.lvstore = "LVS_10"
    node.status = status
    node.rpc_client = MagicMock(return_value=rpc or MagicMock())
    return node


class TestJmRepWaitBounds(unittest.TestCase):

    def _patch_db(self, fresh_status):
        """Patch the freshness read used to decide if the peer is still usable."""
        fresh = MagicMock()
        fresh.status = fresh_status
        db = MagicMock()
        db.get_storage_node_by_id.return_value = fresh
        return patch("simplyblock_core.db_controller.DBController",
                     return_value=db)

    def test_dead_peer_abandons_the_wait_immediately(self):
        """The incident case: RPCs raise and the CP says the peer is offline."""
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = RuntimeError("connection refused")
        node = _node(rpc=rpc)
        with self._patch_db(StorageNode.STATUS_OFFLINE), \
                patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertFalse(node.wait_for_jm_rep_tasks_to_finish(10))
        sleep.assert_not_called()
        self.assertEqual(rpc.bdev_lvol_get_lvstores.call_count, 1)

    def test_unreachable_rpc_on_a_live_peer_is_bounded(self):
        """A peer the CP still believes in gets the full budget — but only it.

        The old code looped forever here; the count is what makes this a
        regression test rather than a smoke test.
        """
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = RuntimeError("timeout")
        node = _node(rpc=rpc)
        with self._patch_db(StorageNode.STATUS_ONLINE), \
                patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertFalse(
                node.wait_for_jm_rep_tasks_to_finish(10, retry=4, delay=5))
        self.assertEqual(rpc.bdev_lvol_get_lvstores.call_count, 4)
        self.assertEqual(sleep.call_count, 3)  # no sleep after the last attempt

    def test_pre_check_failure_no_longer_escapes(self):
        """The lvstore pre-check used to sit outside the try and raise straight
        out of the method when the peer was already gone."""
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = RuntimeError("connection refused")
        node = _node(rpc=rpc)
        with self._patch_db(StorageNode.STATUS_ONLINE), \
                patch("simplyblock_core.models.storage_node.time.sleep"):
            result = node.wait_for_jm_rep_tasks_to_finish(10, retry=2, delay=1)
        self.assertFalse(result)  # returned, did not raise

    def test_no_lvstore_returns_immediately(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = []
        node = _node(rpc=rpc)
        with patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertTrue(node.wait_for_jm_rep_tasks_to_finish(10))
        sleep.assert_not_called()
        rpc.jc_get_jm_status.assert_not_called()

    def test_busy_then_free_returns_true(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"name": "LVS_10"}]
        rpc.jc_get_jm_status.side_effect = [
            {"jm_a": False, "jm_b": True},   # busy
            {"jm_a": True, "jm_b": True},    # free
        ]
        node = _node(rpc=rpc)
        with patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertTrue(node.wait_for_jm_rep_tasks_to_finish(10, delay=7))
        self.assertEqual(sleep.call_count, 1)
        sleep.assert_called_once_with(7)

    def test_persistently_busy_peer_exhausts_the_budget(self):
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = [{"name": "LVS_10"}]
        rpc.jc_get_jm_status.return_value = {"jm_a": False}
        node = _node(rpc=rpc)
        with patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertFalse(
                node.wait_for_jm_rep_tasks_to_finish(10, retry=3, delay=2))
        self.assertEqual(rpc.jc_get_jm_status.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_db_lookup_failure_keeps_the_bounded_retries(self):
        """If we cannot tell whether the peer is dead, don't abandon early —
        just stay inside the budget."""
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.side_effect = RuntimeError("timeout")
        node = _node(rpc=rpc)
        with patch("simplyblock_core.db_controller.DBController",
                   side_effect=RuntimeError("fdb down")), \
                patch("simplyblock_core.models.storage_node.time.sleep") as sleep:
            self.assertFalse(
                node.wait_for_jm_rep_tasks_to_finish(10, retry=3, delay=1))
        self.assertEqual(rpc.bdev_lvol_get_lvstores.call_count, 3)
        self.assertEqual(sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
