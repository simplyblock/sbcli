# coding=utf-8
"""Namespace IDs must be identical on every path of a shared subsystem.

Mass-create incident 2026-07-06: ``nvmf_subsystem_add_ns`` was issued
without an nsid on every replica, so each node's target auto-assigned
nsids in its own arrival order. Under a concurrent mass create the
nsid→uuid maps diverged on all 10 shared subsystems (first mismatch at
nsid=2), and the client kernel rejected the namespaces ("duplicate IDs
in subsystem for nsid N" / "IDs don't match for shared namespace M"),
leaving lvols without block devices.

Contract pinned here:
  - the PRIMARY add auto-assigns (nsid omitted) and persists the result
    in ``lvol.ns_id``;
  - every REPLICA add passes exactly that nsid;
  - a replica running without a primary-assigned nsid fails loudly and
    never auto-assigns;
  - ``recreate_lvol_on_node`` re-adds with the persisted nsid.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol


class _Base(unittest.TestCase):

    def setUp(self):
        self.rpc = MagicMock(name="rpc")
        self.rpc.nvmf_subsystem_add_ns2.return_value = ("7", None)
        self.rpc.nvmf_subsystem_add_ns.return_value = "7"
        self.rpc.get_bdevs.return_value = [
            {"uuid": "lvol-bdev-uuid",
             "driver_specific": {"lvol": {"blobid": 33}}}]

        self.snode = MagicMock(name="snode")
        self.snode.rpc_client.return_value = self.rpc
        self.snode.data_nics = []
        self.snode.get_id.return_value = "node-1"
        self.snode.get_lvol_subsys_port.return_value = 4420

        self.lvol = LVol()
        self.lvol.uuid = "lvol-1"
        self.lvol.nqn = "nqn.2023-02.io.simplyblock:cl:lvol:shared"
        self.lvol.top_bdev = "LVS_1/LVOL_9"
        self.lvol.lvs_name = "LVS_1"
        self.lvol.lvol_bdev = "LVOL_9"
        self.lvol.guid = "aabbccdd"
        self.lvol.lvol_uuid = "lvol-bdev-uuid"
        self.lvol.blobid = 33
        self.lvol.allowed_hosts = []
        self.lvol.namespace = "shared-ns-group"
        self.lvol.node_id = "node-1"
        self.lvol.ns_id = 0  # create flow resets the model default (1)

        self._patches = [
            patch.object(lvol_controller, "_create_bdev_stack",
                         return_value=(True, None)),
            patch.object(lvol_controller, "_resolve_namespaced_subsystem",
                         return_value=False),
            patch.object(lvol_controller, "DBController"),
            patch.object(
                lvol_controller, "_fail_after_bdev",
                side_effect=lambda lvol, rpc, msg: (False, msg)),
        ]
        for p in self._patches:
            started = p.start()
            self.addCleanup(p.stop)
            if getattr(p, "attribute", "") == "DBController":
                started.return_value.get_pool_by_id.return_value.has_qos.return_value = False


class TestCreatePathNsid(_Base):

    def test_primary_autoassigns_and_persists(self):
        ret, err = lvol_controller.add_lvol_on_node(self.lvol, self.snode)
        self.assertIsNone(err)
        kwargs = self.rpc.nvmf_subsystem_add_ns2.call_args.kwargs
        self.assertIsNone(kwargs.get("nsid"),
                          "the primary add must let the target assign the nsid")
        self.assertEqual(self.lvol.ns_id, 7,
                         "the assigned nsid must be persisted for the replicas")

    def test_replica_reuses_primary_assigned_nsid(self):
        self.lvol.ns_id = 7
        ret, err = lvol_controller.add_lvol_on_node(
            self.lvol, self.snode, is_primary=False, secondary_index=0)
        self.assertIsNone(err)
        kwargs = self.rpc.nvmf_subsystem_add_ns2.call_args.kwargs
        self.assertEqual(kwargs.get("nsid"), 7,
                         "replica adds must carry the primary-assigned nsid")
        self.assertEqual(self.lvol.ns_id, 7,
                         "a replica must never overwrite the persisted nsid")

    def test_replica_without_assignment_fails_loudly(self):
        self.lvol.ns_id = 0
        ret, err = lvol_controller.add_lvol_on_node(
            self.lvol, self.snode, is_primary=False, secondary_index=0)
        self.assertTrue(err, "a replica without a primary-assigned nsid must fail")
        self.assertIn("ns_id", str(err))
        self.rpc.nvmf_subsystem_add_ns2.assert_not_called()


class TestRecreatePathNsid(_Base):

    def test_recreate_passes_persisted_nsid(self):
        self.lvol.ns_id = 42
        ok, err = lvol_controller.recreate_lvol_on_node(self.lvol, self.snode)
        self.assertTrue(ok)
        kwargs = self.rpc.nvmf_subsystem_add_ns.call_args.kwargs
        self.assertEqual(kwargs.get("nsid"), 42,
                         "recreate must present the same nsid as every other "
                         "path of the shared subsystem")

    def test_recreate_legacy_record_falls_back_to_auto(self):
        self.lvol.ns_id = 0
        ok, err = lvol_controller.recreate_lvol_on_node(self.lvol, self.snode)
        self.assertTrue(ok)
        kwargs = self.rpc.nvmf_subsystem_add_ns.call_args.kwargs
        self.assertIsNone(kwargs.get("nsid"))


if __name__ == "__main__":
    unittest.main()
