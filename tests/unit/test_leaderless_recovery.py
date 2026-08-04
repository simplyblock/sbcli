# coding=utf-8
"""Leaderless-LVS recovery must NEVER force leadership from the control plane.

Design rule (2026-08-04, incident sb_logs_20260730_195000_30m LVS_9):
leadership is granted by RPC only where it is structurally required and
race-free — lvstore creation, activation, and the restart flow's fenced
demote->grant handoff. Recovery paths repair the redirect topology and wait
for IO-driven self-promotion; if the LVS stays leaderless they fail fast.
The former guarded "last-resort grant" is gone: on 2026-07-30 it raced the
restart flow's own handoff into a writer conflict (LVS_9), and earlier runs
showed CP-forced grants risking stale blob metadata (2026-07-06 LVS_13) and
grant/demote flapping (run 20260725).

Contract under test on storage_node_ops._recover_leaderless_lvs:
  - single-flight: no recovery without winning the FDB takeleader lock;
  - hublvol repair + bounded self-promotion wait, returning whichever node
    promoted itself;
  - bdev_lvol_set_leader is NEVER called, no matter the outcome;
  - still-leaderless after the wait -> None (callers fail fast).
"""
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, lvstore="LVS_1", jm_vuid=7):
    rpc = MagicMock()
    rpc.jc_get_jm_status.return_value = {"jm1": True, "remote_jm2": True}
    rpc.bdev_lvol_set_leader.return_value = True
    return types.SimpleNamespace(
        get_id=lambda: node_id, status=StorageNode.STATUS_ONLINE,
        lvstore=lvstore, jm_vuid=jm_vuid, cluster_id="c1",
        rpc_client=lambda timeout=None, retry=None: rpc, _rpc=rpc)


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
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_self_promotion_wins_without_any_grant(self):
        self.mock_is_leader.side_effect = (
            lambda node, lvs: node.get_id() == "primary-1")
        result = self._run()
        self.assertIs(result, self.taker)
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()
        # The redirect path was repaired for the follower first.
        self.mock_hub.assert_called_once()

    def test_peer_self_promotion_is_accepted(self):
        # Whichever node promoted itself is returned — the CP does not
        # second-guess the data plane's choice.
        self.mock_is_leader.side_effect = (
            lambda node, lvs: node.get_id() == "peer-2")
        result = self._run()
        self.assertIs(result, self.peer)
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()
        self.peer._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_still_leaderless_returns_none_without_grant(self):
        # No node ever self-promotes: recovery must fail fast and must NOT
        # fall back to a control-plane grant (the removed last resort).
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()
        self.peer._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_hublvol_repair_failure_does_not_trigger_grant(self):
        self.mock_hub.side_effect = RuntimeError("repair failed")
        self.assertIsNone(self._run())
        self.taker._rpc.bdev_lvol_set_leader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
