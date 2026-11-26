"""The JM raid follows the devices of its node.

A node's journal raid is built over the journal partitions of its devices. A
device that fails and is migrated out takes its partition with it for good,
and a device that is added brings one. The raid cannot take a member in or
give one up in place (raid0 legs, no superblock), so it is built anew, empty,
over the members the node's devices call for, and the journal coordinator of
the LVS leader resynchronises it from the node's other journal copies.

  - jm_raid: the member list after a change, the layout a record was built
    under, and the legs: at most four devices each, and for a JM that holds a
    journal always the ones it was built with.
  - device_controller: which members the node's devices call for, whether the
    raid can be rebuilt now, and the teardown that leaves no raid behind.

The rebuild itself, the task that runs it and the journal resync read and
write node and task records: tests/integration/test_jm_raid_reshape.py.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import jm_raid, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

ONLINE = NVMeDevice.STATUS_ONLINE
MIGRATED = NVMeDevice.STATUS_FAILED_AND_MIGRATED


def _lblk_node(statuses, members=None, node_id="n1"):
    """An lblk node of len(statuses) devices, each a journal partition and a
    data partition. ``members`` is the JM raid's recorded member list; all of
    the node's journal partitions by default."""
    snode = StorageNode()
    snode.uuid = node_id
    snode.cluster_id = "c1"
    snode.status = StorageNode.STATUS_ONLINE
    snode.lblk_devices = []
    snode.nvme_devices = []
    names = []
    for index, status in enumerate(statuses):
        snode.lblk_devices.append({"name": f"sd{index}1", "serial": f"J{index}", "journal": True,
                                   "journal_raid": True, "data_serial": f"D{index}"})
        snode.lblk_devices.append({"name": f"sd{index}2", "serial": f"D{index}"})
        dev = NVMeDevice()
        dev.uuid = f"dev{index}"
        dev.serial_number = f"D{index}"
        dev.bdev_type = "aio"
        dev.nvme_bdev = utils.aio_bdev_name_for_serial(f"D{index}")
        dev.status = status
        snode.nvme_devices.append(dev)
        names.append(utils.aio_bdev_name_for_serial(f"J{index}"))
    jm = JMDevice()
    jm.uuid = "jm1"
    jm.status = JMDevice.STATUS_ONLINE
    jm.jm_nvme_bdev_list = list(names if members is None else members)
    jm.jm_bdev = f"jm_{node_id}"
    jm.alceml_bdev = f"alceml_jm_{node_id}"
    jm.raid_bdev = f"raid_jm_{node_id}" if len(jm.jm_nvme_bdev_list) > 1 else jm.jm_nvme_bdev_list[0]
    legs = jm_raid.split_groups(jm.jm_nvme_bdev_list) if len(jm.jm_nvme_bdev_list) > 1 else []
    jm.jm_leg_members = legs
    jm.jm_leg_bdevs = [leg[0] if len(leg) == 1 else f"raid_jm_{node_id}_l{i}"
                       for i, leg in enumerate(legs)]
    snode.jm_device = jm
    return snode, names


def _nvme_node(statuses, members):
    """An NVMe node whose drives are cut into a journal partition (p1) and one
    data partition (p2)."""
    snode = StorageNode()
    snode.uuid = "n1"
    snode.num_partitions_per_dev = 1
    snode.jm_percent = 3
    snode.nvme_devices = []
    for index, status in enumerate(statuses):
        dev = NVMeDevice()
        dev.uuid = f"dev{index}"
        dev.nvme_controller = f"nvme_{index}"
        dev.nvme_bdev = f"nvme_{index}n1p2"
        dev.is_partition = True
        dev.status = status
        snode.nvme_devices.append(dev)
    jm = JMDevice()
    jm.uuid = "jm1"
    jm.jm_nvme_bdev_list = list(members)
    snode.jm_device = jm
    return snode


class TestReshapedMembers(unittest.TestCase):

    def test_gone_members_leave_and_the_rest_keep_their_order(self):
        self.assertEqual(jm_raid.reshaped_members(list("abcd"), ["b"], []), list("acd"))

    def test_added_members_follow_the_ones_that_stay(self):
        self.assertEqual(jm_raid.reshaped_members(list("ab"), [], ["c", "d"]), list("abcd"))

    def test_a_member_already_there_is_not_added_twice(self):
        self.assertEqual(jm_raid.reshaped_members(list("ab"), [], ["b", "c", "c"]), list("abc"))

    def test_a_gone_member_is_not_added_back(self):
        self.assertEqual(jm_raid.reshaped_members(list("ab"), ["b"], ["b", "c"]), list("ac"))

    def test_nothing_changed(self):
        self.assertEqual(jm_raid.reshaped_members(list("abc"), [], list("abc")), list("abc"))


class TestLayoutOfRecord(unittest.TestCase):

    def test_recorded_legs_are_raid01(self):
        self.assertEqual(jm_raid.layout_of_record(list("abcd"), "raid_jm_n", ["l0", "l1"]),
                         jm_raid.LAYOUT_RAID01)

    def test_a_raid_without_legs_is_the_legacy_mirror(self):
        self.assertEqual(jm_raid.layout_of_record(list("abcd"), "raid_jm_n", []),
                         jm_raid.LAYOUT_LEGACY)

    def test_a_single_device_grows_into_raid01_not_into_the_legacy_mirror(self):
        """A single-device JM records its partition as raid_bdev and no legs,
        the shape of a legacy record. Read as legacy, a node growing from one
        device to three would get a three-way mirror."""
        self.assertEqual(jm_raid.layout_of_record(["a"], "a", []), jm_raid.LAYOUT_RAID01)


class TestRecordedLegs(unittest.TestCase):

    def test_new_jm_of_twelve_devices_gets_three_legs(self):
        members = [f"m{i}" for i in range(12)]
        legs = jm_raid.plan_topology(members)["legs"]
        self.assertEqual([len(leg) for leg in legs], [4, 4, 4])

    def test_a_jm_built_in_two_halves_keeps_them(self):
        """Twelve devices built before the groups were capped are two legs of
        six. Re-planned into three legs of four, every stripe would be read
        from the wrong drives."""
        members = [f"m{i}" for i in range(12)]
        built = [members[:6], members[6:]]
        self.assertEqual(jm_raid.plan_topology(members, legs=built)["legs"], built)

    def test_a_record_of_other_members_is_ignored(self):
        members = list("abcdef")
        legs = jm_raid.plan_topology(members, legs=[list("ab"), list("cd")])["legs"]
        self.assertEqual(legs, [list("abc"), list("def")])

    def test_no_record_plans_anew(self):
        self.assertEqual(jm_raid.plan_topology(list("abcd"), legs=None)["legs"],
                         [list("ab"), list("cd")])
        self.assertEqual(jm_raid.plan_topology(list("abcd"), legs=[])["legs"],
                         [list("ab"), list("cd")])

    def test_is_jm_raid_bdev(self):
        self.assertTrue(jm_raid.is_jm_raid_bdev("raid_jm_n1", "n1"))
        self.assertTrue(jm_raid.is_jm_raid_bdev("raid_jm_n1_l2", "n1"))
        self.assertFalse(jm_raid.is_jm_raid_bdev("raid_jm_n10", "n1"))
        self.assertFalse(jm_raid.is_jm_raid_bdev("raid_jm_n2_l0", "n1"))


class TestTargetMembers(unittest.TestCase):

    def test_all_devices_online_changes_nothing(self):
        snode, names = _lblk_node([ONLINE] * 4)
        self.assertEqual(device_controller.jm_raid_target_members(snode), names)
        self.assertFalse(device_controller.jm_raid_reshape_needed(snode))

    def test_a_migrated_out_device_takes_its_member_with_it(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE, ONLINE])
        self.assertEqual(device_controller.jm_raid_target_members(snode),
                         [names[0], names[2], names[3]])
        self.assertTrue(device_controller.jm_raid_reshape_needed(snode))

    def test_a_device_that_is_only_missing_keeps_its_member(self):
        for status in (NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_UNAVAILABLE,
                       NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_READONLY):
            snode, names = _lblk_node([ONLINE, status, ONLINE])
            self.assertEqual(device_controller.jm_raid_target_members(snode), names, status)
            self.assertFalse(device_controller.jm_raid_reshape_needed(snode), status)

    def test_an_online_device_whose_partition_is_not_in_the_raid_joins(self):
        snode, names = _lblk_node([ONLINE] * 3)
        snode.jm_device.jm_nvme_bdev_list = names[:2]
        self.assertEqual(device_controller.jm_raid_target_members(snode), names)
        self.assertTrue(device_controller.jm_raid_reshape_needed(snode))

    def test_a_new_device_joins_only_once_it_serves_data(self):
        snode, names = _lblk_node([ONLINE, ONLINE, NVMeDevice.STATUS_NEW], members=None)
        snode.jm_device.jm_nvme_bdev_list = names[:2]
        self.assertEqual(device_controller.jm_raid_target_members(snode), names[:2])

    def test_shrink_from_three_to_two_and_to_one(self):
        snode, names = _lblk_node([ONLINE, ONLINE, MIGRATED])
        self.assertEqual(device_controller.jm_raid_target_members(snode), names[:2])
        snode, names = _lblk_node([ONLINE, MIGRATED])
        self.assertEqual(device_controller.jm_raid_target_members(snode), names[:1])

    def test_no_device_left_is_nothing_to_build(self):
        snode, _ = _lblk_node([MIGRATED, MIGRATED])
        self.assertEqual(device_controller.jm_raid_target_members(snode), [])
        self.assertFalse(device_controller.jm_raid_reshape_needed(snode))

    def test_a_journal_on_a_dedicated_device_is_not_a_raid_over_the_devices(self):
        snode, _ = _lblk_node([ONLINE, ONLINE])
        snode.jm_device.jm_nvme_bdev_list = []
        self.assertEqual(device_controller.jm_raid_target_members(snode), [])
        snode.jm_device = None
        self.assertEqual(device_controller.jm_raid_target_members(snode), [])
        self.assertFalse(device_controller.jm_raid_reshape_needed(snode))

    def test_nvme_member_leaves_when_every_partition_of_its_drive_is_migrated(self):
        snode = _nvme_node([ONLINE, MIGRATED, ONLINE], ["nvme_0n1p1", "nvme_1n1p1", "nvme_2n1p1"])
        self.assertEqual(device_controller.jm_raid_target_members(snode),
                         ["nvme_0n1p1", "nvme_2n1p1"])

    def test_nvme_member_stays_while_one_partition_of_its_drive_is_in_use(self):
        snode = _nvme_node([ONLINE, MIGRATED], ["nvme_0n1p1", "nvme_1n1p1"])
        other = NVMeDevice()
        other.nvme_controller = "nvme_1"
        other.nvme_bdev = "nvme_1n1p3"
        other.is_partition = True
        other.status = ONLINE
        snode.nvme_devices.append(other)
        self.assertEqual(device_controller.jm_raid_target_members(snode),
                         ["nvme_0n1p1", "nvme_1n1p1"])

    def test_nvme_controller_names_are_not_matched_by_prefix(self):
        # nvme_1 and nvme_11 are two drives.
        snode = _nvme_node([MIGRATED], ["nvme_11n1p1"])
        snode.nvme_devices[0].nvme_controller = "nvme_1"
        self.assertEqual(device_controller.jm_raid_target_members(snode), ["nvme_11n1p1"])

    def test_nvme_drive_with_a_journal_partition_the_raid_does_not_use_joins(self):
        snode = _nvme_node([ONLINE, ONLINE, ONLINE], ["nvme_0n1p1", "nvme_1n1p1"])
        self.assertEqual(device_controller.jm_raid_target_members(snode),
                         ["nvme_0n1p1", "nvme_1n1p1", "nvme_2n1p1"])

    def test_an_unpartitioned_nvme_drive_brings_no_journal_partition(self):
        snode = _nvme_node([ONLINE, ONLINE], ["nvme_0n1p1"])
        snode.nvme_devices[1].nvme_bdev = "nvme_1n1"
        snode.nvme_devices[1].is_partition = False
        self.assertEqual(device_controller.jm_raid_target_members(snode), ["nvme_0n1p1"])


class TestDeleteJmStack(unittest.TestCase):

    def _rpc(self, raids, present=None):
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [{"name": n} for n in raids]
        rpc.bdev_get.side_effect = lambda name: {"name": name} if present is None or name in present else None
        rpc.subsystem_get.return_value = {"nqn": "x"}
        return rpc

    def test_every_layer_and_every_raid_of_the_node_is_deleted(self):
        snode, _ = _lblk_node([ONLINE] * 12)
        snode.enable_ha_jm = True
        snode.jm_device.nvmf_nqn = "nqn:jm"
        snode.jm_device.pt_bdev = "jm_n1_PT"
        rpc = self._rpc(["raid_jm_n1_l0", "raid_jm_n1_l1", "raid_jm_n1_l2", "raid_jm_n1",
                         "raid_jm_n2", "raid_jm_n10_l0", "other"])
        device_controller._delete_jm_stack(snode, rpc)
        rpc.subsystem_delete.assert_called_once_with("nqn:jm")
        rpc.bdev_PT_NoExcl_delete.assert_called_once_with("jm_n1_PT")
        rpc.bdev_jm_delete.assert_called_once_with("jm_n1", safe_removal=True)
        rpc.bdev_alceml_delete.assert_called_once_with("alceml_jm_n1")
        # The top raid first, then its legs; no raid of another node.
        self.assertEqual([c.args[0] for c in rpc.bdev_raid_delete.call_args_list],
                         ["raid_jm_n1", "raid_jm_n1_l0", "raid_jm_n1_l1", "raid_jm_n1_l2"])
        # A raid that is not online is still found.
        rpc.bdev_raid_get_bdevs.assert_called_once_with(category="all")

    def test_a_second_call_after_a_failure_half_way_skips_what_is_gone(self):
        snode, _ = _lblk_node([ONLINE] * 4)
        snode.enable_ha_jm = True
        snode.jm_device.nvmf_nqn = "nqn:jm"
        snode.jm_device.pt_bdev = "jm_n1_PT"
        rpc = self._rpc(["raid_jm_n1_l1"], present=set())
        rpc.subsystem_get.return_value = None
        device_controller._delete_jm_stack(snode, rpc)
        rpc.subsystem_delete.assert_not_called()
        rpc.bdev_PT_NoExcl_delete.assert_not_called()
        rpc.bdev_jm_delete.assert_not_called()
        rpc.bdev_alceml_delete.assert_not_called()
        rpc.bdev_raid_delete.assert_called_once_with("raid_jm_n1_l1")

    def test_a_single_device_jm_has_no_raid_to_delete(self):
        snode, names = _lblk_node([ONLINE])
        rpc = self._rpc([])
        device_controller._delete_jm_stack(snode, rpc)
        rpc.bdev_raid_delete.assert_not_called()


class TestNotReady(unittest.TestCase):

    def _reason(self, snode, absent=()):
        rpc = MagicMock()
        rpc.bdev_get.side_effect = lambda name: None if name in absent else {"name": name}
        with patch.object(StorageNode, "rpc_client", return_value=rpc):
            return device_controller.jm_raid_reshape_not_ready(snode)

    def test_ready_when_every_member_of_the_new_raid_is_there(self):
        snode, _ = _lblk_node([ONLINE, MIGRATED, ONLINE, ONLINE])
        self.assertEqual(self._reason(snode), "")

    def test_the_member_of_the_device_that_left_is_not_needed(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE])
        self.assertEqual(self._reason(snode, absent={names[1]}), "")

    def test_a_member_the_new_raid_needs_is_absent(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE])
        self.assertIn(names[2], self._reason(snode, absent={names[2]}))

    def test_a_node_that_is_not_online(self):
        snode, _ = _lblk_node([ONLINE, MIGRATED, ONLINE])
        snode.status = StorageNode.STATUS_RESTARTING
        self.assertIn(StorageNode.STATUS_RESTARTING, self._reason(snode))

    def test_a_node_without_a_raid_over_its_devices(self):
        snode, _ = _lblk_node([ONLINE, ONLINE])
        snode.jm_device.jm_nvme_bdev_list = []
        self.assertIn("no JM raid", self._reason(snode))
