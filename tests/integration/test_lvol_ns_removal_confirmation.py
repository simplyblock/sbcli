# coding=utf-8
"""Empty-subsystem deletion in ``_remove_lvol_subsys_from_node``.

Moved from tests/unit/test_lvol_ns_removal_confirmation.py: since 37751bfe4
the empty-subsystem branch calls DBController().get_lvols_by_node_id to check
for other live volumes still claiming the NQN, so it needs a real DB.
"""

import unittest
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol


NQN = "nqn.2023-02.io.simplyblock:cl:lvol:shared"


def _make_lvol(node_id="node-1"):
    lvol = LVol()
    lvol.uuid = str(uuid_mod.uuid4())
    lvol.nqn = NQN
    lvol.lvs_name = "LVS_1"
    lvol.lvol_bdev = "LVOL_9"
    lvol.node_id = node_id
    lvol.pool_uuid = "pool-1"
    lvol.bdev_stack = []
    return lvol


class TestRemoveSubsysConfirmationEmptySubsystem(unittest.TestCase):

    def setUp(self):
        self.db = DBController()
        if self.db.kv_store is None:
            self.skipTest("FoundationDB is not available")
        self.db.kv_store.clear_range(b"\x00", b"\xff")

        self.lvol = _make_lvol()
        self.lvol.write_to_db(self.db.kv_store)

        self.rpc = MagicMock(name="rpc")
        self.rpc.nvmf_subsystem_remove_ns.return_value = True
        self.rpc.subsystem_delete.return_value = True
        p = patch.object(lvol_controller.time, "sleep")
        p.start()
        self.addCleanup(p.stop)

    def test_confirmed_removal_then_empty_subsystem_is_deleted(self):
        self.rpc.subsystem_get.side_effect = [
            {"namespaces": [{"nsid": 2, "uuid": self.lvol.uuid}]},
            {"namespaces": []},  # confirmation poll: ns gone
        ]
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertTrue(ok)
        self.rpc.nvmf_subsystem_remove_ns.assert_called_once_with(NQN, 2)
        self.rpc.subsystem_delete.assert_called_once_with(NQN)

    def test_other_live_volume_on_same_nqn_keeps_subsystem(self):
        """A second lvol on the same node still sharing the NQN must block
        the delete, even though the subsystem is transiently empty."""
        other = _make_lvol(node_id=self.lvol.node_id)
        other.status = LVol.STATUS_ONLINE
        other.write_to_db(self.db.kv_store)

        self.rpc.subsystem_get.side_effect = [
            {"namespaces": [{"nsid": 2, "uuid": self.lvol.uuid}]},
            {"namespaces": []},
        ]
        ok = lvol_controller._remove_lvol_subsys_from_node(self.lvol, self.rpc)
        self.assertTrue(ok)
        self.rpc.subsystem_delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
