"""Leaderless-LVS recovery: reload-then-grant, never a blind grant.

Design (2026-08-04): when an object operation needs a leader and none
exists, recovery repairs the redirect topology and waits for IO-driven
self-promotion; if the LVS is still leaderless it may grant — but only
after an explicit ``bdev_lvol_update_lvstore`` (blob-md reload from disk,
the same update the IO-driven promotion performs). A bare
``set_leader(True)`` can serve stale blob metadata (incident 2026-07-06
LVS_13); an unguarded grant flapped against the port-allow handoff (run
20260725) and raced the restart flow's fenced handoff into a writer
conflict (2026-07-30 LVS_9).

Contract under test on storage_node_ops._recover_leaderless_lvs:
  - single-flight: no recovery without winning the FDB takeleader lock;
  - self-promotion preferred: hublvol repair + wait, no RPCs beyond probing
    when a node promotes itself;
  - grant suppressed while a port-allow / restart task is active on any LVS
    member (those flows own leadership movement);
  - grant suppressed when the taker's JM quorum is not intact;
  - the metadata reload runs STRICTLY BEFORE the grant, and a failed or
    refused reload means no grant at all;
  - the grant is verified before the taker is routed to.
"""
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, lvstore="LVS_1", jm_vuid=7):
    rpc = MagicMock()
    rpc.jc_get_jm_status.return_value = {"jm1": True, "remote_jm2": True}
    rpc.bdev_lvol_update_lvstore.return_value = True
    rpc.bdev_lvol_set_leader.return_value = True
    return types.SimpleNamespace(
        get_id=lambda: node_id, status=StorageNode.STATUS_ONLINE,
        lvstore=lvstore, jm_vuid=jm_vuid, cluster_id="c1",
        rpc_client=lambda **kwargs: rpc, _rpc=rpc)


class TestRecoverLeaderlessLvs(unittest.TestCase):
    def setUp(self):
        self.taker = _node("primary-1")
        self.peer = _node("peer-2")
        self.nodes = [self.taker, self.peer]

        patcher_db = patch.object(storage_node_ops, "DBController")
        self.mock_db_cls = patcher_db.start()
        self.addCleanup(patcher_db.stop)
        self.db = self.mock_db_cls.return_value
        self.db.acquire_lvstore_lock.return_value = (True, None)
        self.db.get_job_tasks.return_value = []

        patcher_hub = patch.object(
            storage_node_ops.health_controller, "_check_sec_node_hublvol")
        self.mock_hub = patcher_hub.start()
        self.addCleanup(patcher_hub.stop)

        patcher_leader = patch(
            "simplyblock_core.controllers.lvol_controller.is_node_leader")
        self.mock_is_leader = patcher_leader.start()
        self.addCleanup(patcher_leader.stop)
        self.mock_is_leader.return_value = False

        patcher_sleep = patch.object(storage_node_ops.time, "sleep")
        patcher_sleep.start()
        self.addCleanup(patcher_sleep.stop)

    def _run(self):
        return storage_node_ops._recover_leaderless_lvs(
            "c1", self.nodes, "LVS_1", self.taker)

    def test_no_recovery_without_the_lock(self):
        self.db.acquire_lvstore_lock.return_value = (False, "other-holder")
        self.assertIsNone(self._run())
        self.mock_hub.assert_not_called()
        self.taker._rpc.bdev_lvol_update_lvstore.assert_not_called()
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_self_promotion_wins_without_any_grant(self):
        self.mock_is_leader.side_effect = (
            lambda node, lvs: node.get_id() == "primary-1")
        result = self._run()
        self.assertIs(result, self.taker)
        self.taker._rpc.bdev_lvol_update_lvstore.assert_not_called()
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()
        # The redirect path was repaired for the follower first.
        self.mock_hub.assert_called_once()

    def test_no_grant_while_port_allow_task_active(self):
        self.db.get_job_tasks.return_value = [types.SimpleNamespace(
            function_name=JobSchedule.FN_PORT_ALLOW, node_id="peer-2",
            status="running", canceled=False)]
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_update_lvstore.assert_not_called()
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_no_grant_while_restart_task_active(self):
        self.db.get_job_tasks.return_value = [types.SimpleNamespace(
            function_name=JobSchedule.FN_NODE_RESTART, node_id="primary-1",
            status="running", canceled=False)]
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_no_grant_when_task_state_unreadable(self):
        self.db.get_job_tasks.side_effect = RuntimeError("fdb 1031")
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_no_grant_without_jm_quorum(self):
        self.taker._rpc.jc_get_jm_status.return_value = {
            "jm1": True, "remote_jm2": False, "remote_jm3": False}
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_no_grant_when_jm_status_unreadable(self):
        self.taker._rpc.jc_get_jm_status.side_effect = RuntimeError("timeout")
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_reload_runs_strictly_before_grant(self):
        order = []
        self.taker._rpc.bdev_lvol_update_lvstore.side_effect = (
            lambda lvs: order.append("update") or True)

        def grant(lvs, leader=False, bs_nonleadership=False):
            order.append("grant")
            return True

        self.taker._rpc.bdev_lvol_set_leader.side_effect = grant
        self.mock_is_leader.side_effect = (
            lambda node, lvs: "grant" in order and node.get_id() == "primary-1")
        result = self._run()
        self.assertIs(result, self.taker)
        self.assertEqual(order, ["update", "grant"])
        self.taker._rpc.bdev_lvol_set_leader.assert_called_once_with(
            "LVS_1", leader=True)

    def test_reload_failure_means_no_grant(self):
        self.taker._rpc.bdev_lvol_update_lvstore.side_effect = (
            RuntimeError("update failed"))
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_reload_refusal_means_no_grant(self):
        self.taker._rpc.bdev_lvol_update_lvstore.return_value = False
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_unverified_grant_returns_none(self):
        # Reload + grant RPC accepted but leadership never confirmed ->
        # refuse to route.
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_update_lvstore.assert_called_once()
        self.taker._rpc.bdev_lvol_set_leader.assert_called_once()


if __name__ == "__main__":
    unittest.main()
