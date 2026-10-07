"""The periodic hublvol repair must not re-stamp a replica's role mid-relocation,
and the removal has the last word on the moved replicas' roles anyway.

Run 51, 2026-10-02 10:01 (image with the cascade fix). Removing gtpqv
relocated LVS_11 (leader b6q2d) as a cascade: secondary gtpqv -> hbdzq, which
held LVS_11 as tertiary, then tertiary hbdzq -> wnn6n. The relocation built
hbdzq as secondary at 10:01:01. At 10:01:11 the hublvol repair in
tasks-runner-port-allow stamped ``role=tertiary`` over it: it derives the role
from ``node.lvstore_stack_tertiary == primary``, and mid-cascade hbdzq carried
BOTH back-references for b6q2d (the secondary one set by move 1, the tertiary
one not cleared until move 2 finished). Its ``_restart_owns_lvs`` gate was
called without a db, so it saw only the primary's record and never the
relocating node's restart phase. Phase 3c reported REPLICA ROLE MISMATCH; the
DB said secondary, SPDK said tertiary.
"""

import inspect
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops as sno
from simplyblock_core.controllers import health_controller as hc
from simplyblock_core.models.storage_node import StorageNode
from tests.unit.test_node_removal import FakeDB, _cluster, _node


class TestTheRepairGates(unittest.TestCase):

    def test_both_repair_gates_ask_the_whole_cluster(self):
        src = inspect.getsource(hc._check_sec_node_hublvol)
        self.assertEqual(src.count("_restart_owns_lvs(primary_node, db_controller)"), 2)
        self.assertNotIn("_restart_owns_lvs(primary_node)", src)

    def test_a_node_carrying_both_roles_of_one_primary_is_not_repaired(self):
        """hbdzq mid-cascade: both back-references name b6q2d. The repair has
        no single role to stamp, so it stamps none."""
        b6q2d = _node("b6q2d", lvstore="LVS_11", secondary_id="hbdzq", tertiary_id="hbdzq")
        b6q2d.hublvol = MagicMock(bdev_name="LVS_11/hublvol")
        b6q2d.lvstore_status = "ready"
        hbdzq = _node("hbdzq", stack_secondary="b6q2d", stack_tertiary="b6q2d")
        rpc = MagicMock()
        rpc.bdev_nvme_controller_list.return_value = [{"ctrlrs": [{"trid": {"traddr": "10.50.0.16"}}]}]
        hbdzq.rpc_client = MagicMock(return_value=rpc)
        db = FakeDB(_cluster(), [b6q2d, hbdzq])
        with patch.object(hc, "DBController", return_value=db), \
             patch.object(hc, "_restart_owns_lvs", return_value=False), \
             patch.object(hc, "repairs_allowed", return_value=True), \
             patch.object(hc.storage_node_ops, "_collect_attached_ips", return_value={"10.50.0.16"}):
            hc._check_sec_node_hublvol(hbdzq, auto_fix=True, primary_node_id="b6q2d", repair_paths=True)
        hbdzq.connect_to_hublvol.assert_not_called()
        hbdzq.add_hublvol_failover_path.assert_not_called()


class TestTheRemovalReassertsTheRoles(unittest.TestCase):

    def _setup(self, held_on_hbdzq="tertiary", hbdzq_status=StorageNode.STATUS_ONLINE, leader=False):
        b6q2d = _node("b6q2d", lvstore="LVS_11", secondary_id="hbdzq", tertiary_id="wnn6n", jm_vuid=11)
        b6q2d.get_lvol_subsys_port = MagicMock(return_value=4442)
        b6q2d.get_hublvol_port = MagicMock(return_value=4443)
        hbdzq = _node("hbdzq", stack_secondary="b6q2d", status=hbdzq_status)
        wnn6n = _node("wnn6n", stack_tertiary="b6q2d")
        answers = {"hbdzq": {"name": "LVS_11", "lvs_tertiary": held_on_hbdzq == "tertiary",
                             "lvs leadership": leader},
                   "wnn6n": {"name": "LVS_11", "lvs_tertiary": True, "lvs leadership": False}}
        rpcs = {}
        for n in (hbdzq, wnn6n):
            rpc = MagicMock()
            rpc.get_lvstore.return_value = answers[n.get_id()]
            rpc.bdev_lvol_set_lvs_opts.return_value = True
            n.rpc_client = MagicMock(return_value=rpc)
            rpcs[n.get_id()] = rpc
        return FakeDB(_cluster(npcs=2, ndcs=2, ft=2), [b6q2d, hbdzq, wnn6n]), rpcs

    def test_the_run_51_mismatch_is_corrected(self):
        db, rpcs = self._setup()
        sno._reassert_moved_replica_roles(["b6q2d"], db)
        rpcs["hbdzq"].bdev_lvol_set_lvs_opts.assert_called_once()
        call = rpcs["hbdzq"].bdev_lvol_set_lvs_opts.call_args
        self.assertEqual(call.args[0], "LVS_11")
        self.assertEqual(call.kwargs["role"], "secondary")
        self.assertEqual((call.kwargs["groupid"], call.kwargs["subsystem_port"], call.kwargs["hublvol_port"]),
                         (11, 4442, 4443))
        rpcs["wnn6n"].bdev_lvol_set_lvs_opts.assert_not_called()

    def test_a_replica_that_already_agrees_is_left_alone(self):
        db, rpcs = self._setup(held_on_hbdzq="secondary")
        sno._reassert_moved_replica_roles(["b6q2d"], db)
        rpcs["hbdzq"].bdev_lvol_set_lvs_opts.assert_not_called()

    def test_a_leader_is_never_demoted(self):
        db, rpcs = self._setup(leader=True)
        sno._reassert_moved_replica_roles(["b6q2d"], db)
        rpcs["hbdzq"].bdev_lvol_set_lvs_opts.assert_not_called()

    def test_an_offline_replica_is_skipped(self):
        db, rpcs = self._setup(hbdzq_status=StorageNode.STATUS_OFFLINE)
        sno._reassert_moved_replica_roles(["b6q2d"], db)
        rpcs["hbdzq"].bdev_lvol_set_lvs_opts.assert_not_called()

    def test_it_runs_after_the_moves_for_the_primaries_that_moved(self):
        removed = _node("gtpqv", status=StorageNode.STATUS_IN_REMOVAL, stack_secondary="b6q2d")
        b6q2d = _node("b6q2d", lvstore="LVS_11", secondary_id="gtpqv", tertiary_id="hbdzq")
        other = _node("7pgdp", lvstore="LVS_4", secondary_id="8jcgb", tertiary_id="b6q2d")
        db = FakeDB(_cluster(npcs=2, ndcs=2, ft=2), [removed, b6q2d, other])
        order = []

        def _plan(_removed, _db):
            b6q2d.secondary_node_id = "hbdzq"
            return True

        with patch.object(sno, "DBController", return_value=db), \
             patch.object(sno, "_plan_driven_relocation", side_effect=_plan), \
             patch.object(sno, "_rewindow_moved_replica_subsystems",
                          side_effect=lambda ids, _db: order.append(("rewindow", list(ids)))), \
             patch.object(sno, "_reassert_moved_replica_roles",
                          side_effect=lambda ids, _db: order.append(("reassert", list(ids)))):
            self.assertTrue(sno._relocate_replicas_hosted_on(removed))
        self.assertEqual(order, [("rewindow", ["b6q2d"]), ("reassert", ["b6q2d"])])


if __name__ == "__main__":
    unittest.main()
