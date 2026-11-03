"""A JM raid leg that missed writes is never mounted as current (SFAM-2843).

The JM raid is created with superblock=False, so neither SPDK nor a fresh
bdev_raid_create tells a stale leg from a current one. A leg taken out of the
raid while the JM kept writing was put back as current on the next node
restart, the JM alceml read stale page headers and every restart failed with
"Bad address". The leg is now recorded in JMDevice.stale_jm_legs, kept out of
the raid while the JM alceml loads, re-added to rebuild, and dropped from the
record only once the rebuild is seen complete.
"""
import unittest
from unittest.mock import MagicMock, call, patch

from simplyblock_core import jm_raid, storage_node_ops
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCRemoteError


def _jm(**kw):
    jm = JMDevice()
    jm.uuid = "jm1"
    jm.status = JMDevice.STATUS_ONLINE
    for k, v in kw.items():
        setattr(jm, k, v)
    return jm


def _raid(name, members, target=None):
    info = {"name": name, "base_bdevs_list": [{"name": m, "is_configured": True} for m in members]}
    if target:
        info["process"] = {"type": "rebuild", "target": target}
    return info


class TestLegOf(unittest.TestCase):
    def test_legacy_nway_member_is_its_own_leg(self):
        self.assertEqual(jm_raid.leg_of("a", [], []), "a")

    def test_two_drive_leg_is_the_bare_partition(self):
        self.assertEqual(jm_raid.leg_of("b", ["a", "b"], [["a"], ["b"]]), "b")

    def test_member_of_a_raid0_leg_maps_to_the_leg(self):
        self.assertEqual(
            jm_raid.leg_of("b", ["raid_jm_n1_l0", "c"], [["a", "b"], ["c"]]),
            "raid_jm_n1_l0")


class TestLegInSync(unittest.TestCase):
    def test_configured_leg_without_rebuild_is_in_sync(self):
        self.assertTrue(jm_raid.leg_in_sync(_raid("r", ["a", "b"]), "b"))

    def test_rebuild_target_is_not_in_sync(self):
        self.assertFalse(jm_raid.leg_in_sync(_raid("r", ["a", "b"], target="b"), "b"))

    def test_absent_leg_is_not_in_sync(self):
        self.assertFalse(jm_raid.leg_in_sync(_raid("r", ["a"]), "b"))


class TestRestartKeepsStaleLegOut(unittest.TestCase):
    def _run(self, members, stale, after_restart=True, remove_fails=False):
        order = MagicMock()
        rpc = order.rpc
        rpc.bdev_raid_create.return_value = True
        rpc.bdev_jm_create.return_value = True
        rpc.bdev_get.return_value = {"block_size": 512, "num_blocks": 1000}
        if remove_fails:
            rpc.bdev_raid_remove_base_bdev.side_effect = RPCRemoteError("boom", code=-1)

        snode = MagicMock()
        snode.get_id.return_value = "n1"
        snode.enable_ha_jm = False
        snode.data_nics = []
        snode.jm_device = _jm(stale_jm_legs=list(stale), raid_bdev="raid_jm_n1",
                              jm_leg_bdevs=list(members), jm_leg_members=[[m] for m in members])
        snode.create_alceml = order.create_alceml
        snode.create_alceml.return_value = True

        cluster = MagicMock()
        cluster.jm_raid_layout = jm_raid.LAYOUT_RAID01
        with patch.object(storage_node_ops, "DBController") as db_cls:
            db_cls.return_value.get_cluster_by_id.return_value = cluster
            jm = storage_node_ops._create_jm_stack_on_raid(
                rpc, list(members), snode, after_restart=after_restart)
        return jm, order

    def test_stale_leg_is_removed_before_the_alceml_loads_then_re_added(self):
        jm, order = self._run(["a", "b"], stale=["b"])
        names = [c[0] for c in order.mock_calls]
        self.assertLess(names.index("rpc.bdev_raid_remove_base_bdev"), names.index("create_alceml"))
        self.assertLess(names.index("create_alceml"), names.index("rpc.bdev_raid_add_base_bdev"))
        order.rpc.bdev_raid_remove_base_bdev.assert_called_once_with("b")
        order.rpc.bdev_raid_add_base_bdev.assert_called_once_with("raid_jm_n1", "b")
        # Still stale until the health check sees the rebuild finish.
        self.assertEqual(jm.stale_jm_legs, ["b"])

    def test_no_stale_leg_touches_nothing(self):
        jm, order = self._run(["a", "b"], stale=[])
        order.rpc.bdev_raid_remove_base_bdev.assert_not_called()
        order.rpc.bdev_raid_add_base_bdev.assert_not_called()
        self.assertEqual(jm.stale_jm_legs, [])

    def test_fresh_create_drops_the_stale_record(self):
        jm, order = self._run(["a", "b"], stale=["b"], after_restart=False)
        order.rpc.bdev_raid_remove_base_bdev.assert_not_called()
        self.assertEqual(jm.stale_jm_legs, [])

    def test_never_removes_every_leg(self):
        jm, order = self._run(["a", "b"], stale=["a", "b"])
        order.rpc.bdev_raid_remove_base_bdev.assert_not_called()
        self.assertEqual(jm.stale_jm_legs, [])

    def test_failed_removal_fails_the_restart(self):
        jm, order = self._run(["a", "b"], stale=["b"], remove_fails=True)
        self.assertFalse(jm)
        order.create_alceml.assert_not_called()


class _Db:
    """atomic_update applies the mutation to the node it is given."""

    def __init__(self, node):
        self.node = node
        self.get_storage_node_by_id = MagicMock(return_value=node)

    def atomic_update(self, node, mut):
        mut(node)
        return node


class TestLegBookkeeping(unittest.TestCase):
    def _node(self, jm):
        node = StorageNode()
        node.uuid = "n1"
        node.status = StorageNode.STATUS_ONLINE
        node.jm_device = jm
        return node

    def test_raid0_member_removal_drops_the_leg_and_records_it_stale_first(self):
        jm = _jm(raid_bdev="raid_jm_n1", jm_leg_bdevs=["raid_jm_n1_l0", "c"],
                 jm_leg_members=[["a", "b"], ["c"]])
        node = self._node(jm)
        db = _Db(node)
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [_raid("raid_jm_n1", ["raid_jm_n1_l0", "c"])]
        stale_at_removal = []
        rpc.bdev_raid_remove_base_bdev.side_effect = (
            lambda leg: stale_at_removal.append(list(node.jm_device.stale_jm_legs)))
        with patch.object(device_controller, "DBController", return_value=db), \
                patch.object(device_controller, "get_storage_node_by_jm_device", return_value=node), \
                patch.object(StorageNode, "rpc_client", return_value=rpc):
            self.assertTrue(device_controller.remove_from_jm_device("jm1", "b"))
        rpc.bdev_raid_remove_base_bdev.assert_called_once_with("raid_jm_n1_l0")
        self.assertEqual(stale_at_removal, [["raid_jm_n1_l0"]])

    def test_readd_marks_the_leg_stale_and_adds_it(self):
        node = self._node(_jm(raid_bdev="raid_jm_n1", jm_leg_bdevs=["a", "b"],
                              jm_leg_members=[["a"], ["b"]]))
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [_raid("raid_jm_n1", ["a"])]
        with patch.object(device_controller, "DBController", return_value=_Db(node)):
            device_controller._readd_jm_raid_leg(node, "b", rpc)
        rpc.bdev_raid_add_base_bdev.assert_called_once_with("raid_jm_n1", "b")
        self.assertEqual(node.jm_device.stale_jm_legs, ["b"])

    def test_readd_leaves_a_member_leg_alone(self):
        node = self._node(_jm(raid_bdev="raid_jm_n1", jm_leg_bdevs=["a", "b"],
                              jm_leg_members=[["a"], ["b"]]))
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [_raid("raid_jm_n1", ["a", "b"])]
        with patch.object(device_controller, "DBController", return_value=_Db(node)):
            device_controller._readd_jm_raid_leg(node, "b", rpc)
        rpc.bdev_raid_add_base_bdev.assert_not_called()
        self.assertEqual(node.jm_device.stale_jm_legs, [])

    def test_only_rebuilt_legs_are_cleared(self):
        node = self._node(_jm(raid_bdev="raid_jm_n1", stale_jm_legs=["b", "c"]))
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [_raid("raid_jm_n1", ["a", "b", "c"], target="c")]
        with patch.object(device_controller, "DBController", return_value=_Db(node)):
            device_controller.clear_rebuilt_jm_legs(node, rpc)
        self.assertEqual(node.jm_device.stale_jm_legs, ["c"])


class TestOnController(unittest.TestCase):
    def test_own_bdevs_match(self):
        self.assertTrue(device_controller._on_controller("nvme_61n1p1", "nvme_61"))

    def test_longer_controller_name_does_not_match(self):
        # 0000:06:01.0 -> nvme_61, 0000:06:11.0 -> nvme_611
        self.assertFalse(device_controller._on_controller("nvme_611n1p1", "nvme_61"))


class TestDeviceRemoveLeavesDedicatedJmAlone(unittest.TestCase):
    """A JM on its own device is torn down only when that device is gone."""

    def _run(self, removed_ctrl, jm_present):
        dev = NVMeDevice()
        dev.uuid = "d1"
        dev.node_id = "n1"
        dev.status = NVMeDevice.STATUS_ONLINE
        dev.nvme_controller = removed_ctrl
        node = StorageNode()
        node.uuid = "n1"
        node.nvme_devices = [dev]
        node.jm_device = _jm(nvme_bdev="nvme_7n1")
        db = MagicMock()
        db.get_storage_device_by_id.return_value = dev
        db.get_storage_node_by_id.return_value = node
        rpc = MagicMock()
        rpc.bdev_get.side_effect = lambda name: None if (name == "nvme_7n1" and not jm_present) else {"name": name}
        with patch.object(device_controller, "DBController", return_value=db), \
                patch.object(device_controller, "tasks_controller"), \
                patch.object(device_controller, "distr_controller"), \
                patch.object(device_controller, "device_set_unavailable"), \
                patch.object(device_controller, "device_set_state"), \
                patch.object(StorageNode, "rpc_client", return_value=rpc), \
                patch.object(device_controller, "remove_jm_device") as remove_jm:
            device_controller.device_remove("d1", force=True)
        return remove_jm

    def test_removing_another_device_keeps_the_jm(self):
        self._run("nvme_8", jm_present=False).assert_not_called()

    def test_logical_removal_of_the_jm_device_keeps_the_jm(self):
        self._run("nvme_7", jm_present=True).assert_not_called()

    def test_vanished_jm_device_tears_the_jm_down(self):
        self._run("nvme_7", jm_present=False).assert_has_calls([call("jm1")])


if __name__ == "__main__":
    unittest.main()
