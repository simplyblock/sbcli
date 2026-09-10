# coding=utf-8
"""Namespace removal must be CONFIRMED before the bdev is deleted.

Online-expand incident 2026-06-12 (CI 27398880537): ``nvmf_subsystem_remove_ns``
is asynchronous inside SPDK — the RPC returned success, the deferred
``nvmf_rpc_remove_ns_paused`` step failed 1-6s later with no error propagated
back, the webappapi deleted the bdev anyway, and every connection on the
shared subsystem (including an unrelated live volume mid-expansion) was torn
down.

Contract pinned here:
  - after a successful remove_ns RPC, ``_remove_lvol_subsys_from_node`` polls
    the subsystem until the nsid is actually gone;
  - if the namespace never disappears (or the RPC fails outright), it returns
    False and ``delete_lvol_from_node`` aborts BEFORE touching the bdev stack;
  - ``force=True`` still proceeds (force-delete must be able to clean up a
    wedged node).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol


NQN = "nqn.2023-02.io.simplyblock:cl:lvol:shared"


def _make_lvol():
    lvol = LVol()
    lvol.uuid = "lvol-1"
    lvol.nqn = NQN
    lvol.lvs_name = "LVS_1"
    lvol.lvol_bdev = "LVOL_9"
    lvol.node_id = "node-1"
    lvol.pool_uuid = "pool-1"
    lvol.bdev_stack = []
    return lvol


class TestRemoveSubsysConfirmation(unittest.TestCase):

    def setUp(self):
        self.lvol = _make_lvol()
        self.rpc = MagicMock(name="rpc")
        self.rpc.nvmf_subsystem_remove_ns.return_value = True
        self.rpc.subsystem_delete.return_value = True
        # Keep the confirmation loop from actually sleeping.
        p = patch.object(lvol_controller.time, "sleep")
        p.start()
        self.addCleanup(p.stop)

    def test_confirmed_removal_then_empty_subsystem_is_deleted(self):
        self.rpc.subsystem_get.side_effect = [
            {"namespaces": [{"nsid": 2, "uuid": "lvol-1"}]},
            {"namespaces": []},  # confirmation poll: ns gone
        ]
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertTrue(ok)
        self.rpc.nvmf_subsystem_remove_ns.assert_called_once_with(NQN, 2)
        self.rpc.subsystem_delete.assert_called_once_with(NQN)

    def test_confirmed_removal_with_surviving_namespaces_keeps_subsystem(self):
        self.rpc.subsystem_get.side_effect = [
            {"namespaces": [{"nsid": 2, "uuid": "lvol-1"},
                            {"nsid": 3, "uuid": "other-live-lvol"}]},
            {"namespaces": [{"nsid": 3, "uuid": "other-live-lvol"}]},
        ]
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertTrue(ok)
        self.rpc.subsystem_delete.assert_not_called()

    def test_unconfirmed_removal_returns_false(self):
        """RPC says success but the nsid never disappears (the deferred
        removal failed inside SPDK) — must return False, and must NOT
        delete the subsystem out from under the stuck namespace."""
        self.rpc.subsystem_get.return_value = {
            "namespaces": [{"nsid": 2, "uuid": "lvol-1"}]}
        with patch.object(lvol_controller, "NS_REMOVAL_CONFIRM_TIMEOUT", 0):
            ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertFalse(ok)
        self.rpc.subsystem_delete.assert_not_called()

    def test_failed_remove_rpc_returns_false(self):
        self.rpc.subsystem_get.return_value = {
            "namespaces": [{"nsid": 2, "uuid": "lvol-1"}]}
        self.rpc.nvmf_subsystem_remove_ns.return_value = None
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertFalse(ok)
        self.rpc.subsystem_delete.assert_not_called()

    def test_subsystem_already_gone_is_noop(self):
        self.rpc.subsystem_get.return_value = None
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertTrue(ok)
        self.rpc.nvmf_subsystem_remove_ns.assert_not_called()


class TestDeleteFromNodeAbortsOnUnconfirmedRemoval(unittest.TestCase):

    def _run_delete(self, subsys_ok, force=False):
        lvol = _make_lvol()
        lvol.write_to_db = MagicMock()

        snode = MagicMock(name="snode")
        snode.get_id.return_value = "node-1"
        snode.cluster_id = "cl-1"

        db_mock = MagicMock()
        db_mock.get_lvol_by_id.return_value = lvol
        db_mock.get_storage_node_by_id.return_value = snode
        db_mock.get_pool_by_id.return_value.has_qos.return_value = False

        remove_stack = MagicMock(return_value=True)
        with patch.object(lvol_controller, "DBController", return_value=db_mock), \
                patch.object(lvol_controller, "_remove_lvol_subsys_from_node",
                             return_value=subsys_ok), \
                patch.object(lvol_controller, "_remove_bdev_stack", remove_stack), \
                patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                      return_value="proceed"):
            ret = lvol_controller.delete_lvol_from_node(
                "lvol-1", "node-1", force=force)
        return ret, remove_stack

    def test_unconfirmed_removal_aborts_bdev_delete(self):
        ret, remove_stack = self._run_delete(subsys_ok=False)
        self.assertFalse(ret)
        remove_stack.assert_not_called()

    def test_force_delete_proceeds_despite_unconfirmed_removal(self):
        ret, remove_stack = self._run_delete(subsys_ok=False, force=True)
        self.assertTrue(ret)
        remove_stack.assert_called_once()

    def test_confirmed_removal_proceeds(self):
        ret, remove_stack = self._run_delete(subsys_ok=True)
        self.assertTrue(ret)
        remove_stack.assert_called_once()


if __name__ == "__main__":
    unittest.main()
