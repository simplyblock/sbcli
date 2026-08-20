# coding=utf-8
"""test_unload_lvstore_leader_gate.py — 'sn unload' must only touch a leader.

bdev_lvol_apply_lvstore acts on the lvstore the node holds. Run against a node
that does not lead that lvstore it would act on follower state, so the command
verifies leadership against the node itself before sending anything.

Leadership comes from bdev_lvol_get_lvstores, where SPDK writes it as
"lvs leadership" -- with a space, from lvs->leader in
rpc_dump_lvol_store_info(). The key is spelled identically on R26.2-PRE and
R26.3. Should a build ever stop emitting it, the gate must fail closed rather
than assume leadership; hence the ``is not True`` test and the missing-key case.
"""

import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.storage_node_ops as ops


class TestUnloadLvstoreLeaderGate(unittest.TestCase):

    def _run(self, stores, lvs_name="LVS_16"):
        """Return (command result, whether the apply RPC was actually sent)."""
        snode = MagicMock()
        snode.lvstore = "LVS_16"
        rpc = MagicMock()
        rpc.bdev_lvol_get_lvstores.return_value = stores
        rpc.bdev_lvol_apply_lvstore.return_value = "done"
        snode.rpc_client.return_value = rpc
        with patch.object(ops, "DBController") as db:
            db.return_value.get_storage_node_by_id.return_value = snode
            result = ops.unload_lvstore("node1", lvs_name)
        return result, rpc.bdev_lvol_apply_lvstore.called

    def test_leader_is_applied(self):
        result, sent = self._run(
            [{"name": "LVS_16", "lvs leadership": True, "lvs_primary": True}])
        self.assertTrue(result)
        self.assertTrue(sent)

    def test_non_leader_aborts_without_sending_the_rpc(self):
        result, sent = self._run(
            [{"name": "LVS_16", "lvs leadership": False, "lvs_secondary": True}])
        self.assertFalse(result)
        self.assertFalse(sent, "the apply RPC must not reach a non-leader")

    def test_missing_leadership_key_fails_closed(self):
        result, sent = self._run([{"name": "LVS_16", "lvs_primary": True}])
        self.assertFalse(result)
        self.assertFalse(sent)

    def test_leadership_is_the_gate_not_the_role(self):
        """A tertiary holding leadership after failover is a legitimate target."""
        result, sent = self._run(
            [{"name": "LVS_16", "lvs leadership": True, "lvs_tertiary": True}])
        self.assertTrue(result)
        self.assertTrue(sent)

    def test_unknown_lvstore_aborts(self):
        for stores in ([], None, [{"name": "LVS_99", "lvs leadership": True}]):
            with self.subTest(stores=stores):
                result, sent = self._run(stores)
                self.assertFalse(result)
                self.assertFalse(sent)

    def test_node_not_found_aborts(self):
        with patch.object(ops, "DBController") as db:
            db.return_value.get_storage_node_by_id.side_effect = KeyError("x")
            self.assertFalse(ops.unload_lvstore("nope", "LVS_16"))


if __name__ == "__main__":
    unittest.main()
