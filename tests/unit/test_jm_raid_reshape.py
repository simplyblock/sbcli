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
  - device_controller: which members the node's devices call for, the
    teardown that leaves no raid behind, the rebuild, and whether the resync
    is done.
  - tasks_runner_restart: the task that does it, one node at a time and only
    on a quiet cluster.
"""
import unittest
from unittest.mock import MagicMock, patch

import pytest

import simplyblock_core.services.task_runner_base as trb
from simplyblock_core import jm_raid, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services.task_runner_base import (
    TaskAbort,
    TaskDefer,
    TaskProgress,
    TaskRetry,
)

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


class TestStackKeepsRecordedLegs(unittest.TestCase):
    """storage_node_ops._create_jm_stack_on_raid, RPCs mocked."""

    def _run(self, members, recorded_legs=None, after_restart=False, layout="", pinned=""):
        rpc = MagicMock()
        rpc.bdev_raid_create.return_value = True
        rpc.bdev_jm_create.return_value = True
        rpc.bdev_get.return_value = {"block_size": 512, "num_blocks": 1000}
        snode = MagicMock()
        snode.get_id.return_value = "n1"
        snode.enable_ha_jm = False
        snode.data_nics = []
        snode.create_alceml.return_value = True
        snode.jm_device = None
        if recorded_legs is not None:
            jm = JMDevice()
            jm.raid_bdev = "raid_jm_n1"
            jm.jm_leg_members = recorded_legs
            jm.jm_leg_bdevs = [f"raid_jm_n1_l{i}" for i in range(len(recorded_legs))]
            snode.jm_device = jm
        cluster = MagicMock()
        cluster.jm_raid_layout = pinned
        with patch.object(storage_node_ops, "DBController") as db_cls:
            db_cls.return_value.get_cluster_by_id.return_value = cluster
            jm = storage_node_ops._create_jm_stack_on_raid(
                rpc, list(members), snode, after_restart=after_restart, layout=layout)
        calls = [(c.args[0], list(c.args[1]), c.args[2]) for c in rpc.bdev_raid_create.call_args_list]
        return jm, calls, snode

    def test_restart_of_a_twelve_device_jm_built_in_two_halves(self):
        members = [f"m{i}" for i in range(12)]
        jm, calls, _ = self._run(members, recorded_legs=[members[:6], members[6:]],
                                 after_restart=True)
        self.assertEqual(calls, [
            ("raid_jm_n1_l0", members[:6], "0"),
            ("raid_jm_n1_l1", members[6:], "0"),
            ("raid_jm_n1", ["raid_jm_n1_l0", "raid_jm_n1_l1"], "1"),
        ])
        self.assertEqual(jm.jm_leg_members, [members[:6], members[6:]])

    def test_new_twelve_device_jm_is_three_legs_under_one_raid1(self):
        members = [f"m{i}" for i in range(12)]
        jm, calls, _ = self._run(members)
        self.assertEqual(calls, [
            ("raid_jm_n1_l0", members[:4], "0"),
            ("raid_jm_n1_l1", members[4:8], "0"),
            ("raid_jm_n1_l2", members[8:], "0"),
            ("raid_jm_n1", ["raid_jm_n1_l0", "raid_jm_n1_l1", "raid_jm_n1_l2"], "1"),
        ])

    def test_a_formatted_jm_is_planned_anew_whatever_the_record_says(self):
        members = [f"m{i}" for i in range(12)]
        jm, calls, snode = self._run(members, recorded_legs=[members[:6], members[6:]],
                                     after_restart=False)
        self.assertEqual([len(leg) for leg in jm.jm_leg_members], [4, 4, 4])
        self.assertEqual(snode.create_alceml.call_args.kwargs["pba_init_mode"], 3)

    def test_the_layout_given_decides_when_the_record_cannot(self):
        # The record of a single-device JM looks legacy (a raid_bdev, no legs).
        rpc_jm = JMDevice()
        rpc_jm.raid_bdev = "a"
        with patch.object(storage_node_ops, "DBController") as db_cls:
            cluster = MagicMock()
            cluster.jm_raid_layout = ""
            db_cls.return_value.get_cluster_by_id.return_value = cluster
            rpc = MagicMock()
            rpc.bdev_get.return_value = {"block_size": 512, "num_blocks": 1000}
            snode = MagicMock()
            snode.get_id.return_value = "n1"
            snode.enable_ha_jm = False
            snode.data_nics = []
            snode.jm_device = rpc_jm
            jm = storage_node_ops._create_jm_stack_on_raid(
                rpc, list("abc"), snode, after_restart=False, layout=jm_raid.LAYOUT_RAID01)
        self.assertEqual(jm.jm_leg_members, [["a", "b"], ["c"]])

    def test_a_layout_pinned_on_the_cluster_wins_over_the_one_given(self):
        jm, calls, _ = self._run(list("abc"), layout=jm_raid.LAYOUT_RAID01,
                                 pinned=jm_raid.LAYOUT_LEGACY)
        self.assertEqual(calls, [("raid_jm_n1", list("abc"), "1")])
        self.assertEqual(jm.jm_leg_bdevs, [])


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


class TestQueueReshape(unittest.TestCase):

    def _queue(self, snode):
        with patch.object(device_controller, "DBController") as db_cls, \
                patch.object(device_controller.tasks_controller, "add_jm_raid_reshape_task") as add:
            db_cls.return_value.get_storage_node_by_id.return_value = snode
            device_controller.queue_jm_raid_reshape(snode.get_id())
        return add

    def test_queued_when_the_raid_no_longer_matches(self):
        snode, _ = _lblk_node([ONLINE, MIGRATED, ONLINE])
        self._queue(snode).assert_called_once_with("c1", "n1")

    def test_not_queued_when_it_matches(self):
        snode, _ = _lblk_node([ONLINE, ONLINE, ONLINE])
        self._queue(snode).assert_not_called()

    def test_a_node_that_is_gone_is_ignored(self):
        with patch.object(device_controller, "DBController") as db_cls, \
                patch.object(device_controller.tasks_controller, "add_jm_raid_reshape_task") as add:
            db_cls.return_value.get_storage_node_by_id.side_effect = KeyError("n1")
            device_controller.queue_jm_raid_reshape("n1")
        add.assert_not_called()

    def test_a_device_migrated_out_queues_the_rebuild_of_its_node(self):
        dev = MagicMock()
        dev.node_id = "n1"
        with patch.object(device_controller, "DBController") as db_cls, \
                patch.object(device_controller, "device_set_state"), \
                patch.object(device_controller, "queue_jm_raid_reshape") as queue:
            db_cls.return_value.get_storage_device_by_id.return_value = dev
            db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = []
            device_controller.device_set_failed_and_migrated("dev1")
        queue.assert_called_once_with("n1")


class TestOneOpenTaskPerNode(unittest.TestCase):

    def _open(self, node_id, status=JobSchedule.STATUS_RUNNING, canceled=False):
        task = JobSchedule()
        task.uuid = f"t-{node_id}"
        task.node_id = node_id
        task.function_name = JobSchedule.FN_JM_RAID_RESHAPE
        task.status = status
        task.canceled = canceled
        return task

    def _add(self, existing):
        with patch.object(tasks_controller, "db") as db, \
                patch.object(tasks_controller, "tasks_events"):
            db.get_job_tasks.return_value = existing
            return tasks_controller.add_jm_raid_reshape_task("c1", "n1"), db

    def test_added_when_the_node_has_none(self):
        task_id, _ = self._add([self._open("n2")])
        self.assertTrue(task_id)

    def test_not_added_twice_for_one_node(self):
        task_id, _ = self._add([self._open("n1")])
        self.assertFalse(task_id)

    def test_a_done_or_canceled_task_does_not_count(self):
        task_id, _ = self._add([self._open("n1", status=JobSchedule.STATUS_DONE),
                                self._open("n1", canceled=True)])
        self.assertTrue(task_id)

    def test_blocking_tasks_are_the_ones_that_move_data_or_restart(self):
        mig = self._open("n2")
        mig.function_name = JobSchedule.FN_NEW_DEV_MIG
        comp = self._open("n2")
        comp.function_name = JobSchedule.FN_JC_COMP_RESUME
        done = self._open("n2", status=JobSchedule.STATUS_DONE)
        done.function_name = JobSchedule.FN_NODE_RESTART
        with patch.object(tasks_controller, "db") as db:
            db.get_job_tasks.return_value = [mig, comp, done, self._open("n3")]
            self.assertEqual(tasks_controller.get_jm_raid_reshape_blocking_tasks("c1"), [mig])


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


class TestReshapeJmRaid(unittest.TestCase):

    def _reshape(self, snode, absent=(), new_jm=None, create_error=None):
        rpc = MagicMock()
        rpc.bdev_get.side_effect = lambda name: None if name in absent else {"name": name}
        rpc.bdev_raid_get_bdevs.return_value = []
        self.rpc = rpc
        self.order = []
        if new_jm is None:
            new_jm = JMDevice()
            new_jm.uuid = "jm1"

        def create(*args, **kwargs):
            self.order.append("create")
            if create_error:
                raise create_error
            return new_jm

        with patch.object(device_controller, "DBController") as db_cls, \
                patch.object(StorageNode, "rpc_client", return_value=rpc), \
                patch.object(device_controller, "set_jm_device_state",
                             side_effect=lambda _id, state: self.order.append(state)) as set_state, \
                patch.object(device_controller, "_delete_jm_stack",
                             side_effect=lambda *a: self.order.append("delete")) as delete, \
                patch.object(storage_node_ops, "_create_jm_stack_on_raid", side_effect=create) as build:
            db = db_cls.return_value
            db.get_storage_node_by_id.return_value = snode
            db.atomic_update.side_effect = lambda obj, fn: (fn(obj), self.order.append("record"), obj)[2]
            self.db, self.set_state, self.delete, self.build = db, set_state, delete, build
            return device_controller.reshape_jm_raid(snode.get_id())

    def test_rebuilds_over_the_remaining_devices_empty(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE, ONLINE])
        target = [names[0], names[2], names[3]]
        self.assertEqual(self._reshape(snode), target)
        args, kwargs = self.build.call_args
        self.assertEqual(args[1], target)
        self.assertIs(kwargs["after_restart"], False)        # formatted, not read back
        self.assertEqual(kwargs["layout"], jm_raid.LAYOUT_RAID01)
        # Down, torn down, built, recorded, and only then online for the peers.
        self.assertEqual(self.order, [JMDevice.STATUS_UNAVAILABLE, "delete", "create", "record",
                                      JMDevice.STATUS_ONLINE])

    def test_a_legacy_mirror_stays_a_legacy_mirror(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE])
        snode.jm_device.jm_leg_bdevs = []
        snode.jm_device.jm_leg_members = []
        self._reshape(snode)
        self.assertEqual(self.build.call_args.kwargs["layout"], jm_raid.LAYOUT_LEGACY)

    def test_a_single_device_jm_grows_into_raid01(self):
        snode, names = _lblk_node([ONLINE, ONLINE], members=None)
        snode.jm_device.jm_nvme_bdev_list = names[:1]
        snode.jm_device.raid_bdev = names[0]
        snode.jm_device.jm_leg_bdevs = []
        self.assertEqual(self._reshape(snode), names)
        self.assertEqual(self.build.call_args.kwargs["layout"], jm_raid.LAYOUT_RAID01)

    def test_nothing_is_touched_while_a_member_is_absent(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE])
        with self.assertRaises(device_controller.JMReshapeNotReady) as ctx:
            self._reshape(snode, absent={names[2]})
        self.assertIn(names[2], str(ctx.exception))
        self.assertEqual(self.order, [])

    def test_nothing_is_touched_on_a_node_that_is_not_online(self):
        snode, _ = _lblk_node([ONLINE, MIGRATED, ONLINE])
        snode.status = StorageNode.STATUS_RESTARTING
        with self.assertRaises(device_controller.JMReshapeNotReady):
            self._reshape(snode)
        self.assertEqual(self.order, [])

    def test_a_node_without_a_raid_over_its_devices_is_refused(self):
        snode, _ = _lblk_node([ONLINE, ONLINE])
        snode.jm_device.jm_nvme_bdev_list = []
        with self.assertRaises(device_controller.JMReshapeNotReady):
            self._reshape(snode)
        self.assertEqual(self.order, [])

    def test_a_failed_build_leaves_the_old_record_for_the_retry(self):
        snode, names = _lblk_node([ONLINE, MIGRATED, ONLINE])
        with self.assertRaises(device_controller.JMReshapeError):
            self._reshape(snode, new_jm=False)
        self.assertEqual(self.order, [JMDevice.STATUS_UNAVAILABLE, "delete", "create"])
        # The record still names the old members, so the retry still sees
        # that the raid does not match the devices.
        self.assertEqual(snode.jm_device.jm_nvme_bdev_list, names)
        self.assertTrue(device_controller.jm_raid_reshape_needed(snode))

    def test_an_rpc_failure_during_the_build_is_passed_on(self):
        snode, _ = _lblk_node([ONLINE, MIGRATED, ONLINE])
        with self.assertRaises(RPCException):
            self._reshape(snode, create_error=RPCException("boom"))
        self.assertNotIn("record", self.order)
        self.assertNotIn(JMDevice.STATUS_ONLINE, self.order)


class TestResyncPending(unittest.TestCase):

    def _node(self, node_id, lvstore, jm_vuid, jm_ids=(), sec="", status=StorageNode.STATUS_ONLINE):
        node = StorageNode()
        node.uuid = node_id
        node.cluster_id = "c1"
        node.status = status
        node.lvstore = lvstore
        node.jm_vuid = jm_vuid
        node.jm_ids = list(jm_ids)
        node.secondary_node_id = sec
        jm = JMDevice()
        jm.uuid = f"jm-{node_id}"
        node.jm_device = jm
        return node

    def _pending(self, nodes, leaders, statuses):
        """``leaders``: lvstore -> id of the node that leads it. ``statuses``:
        (node id, jm_vuid) -> jc_get_jm_status answer, or an exception."""
        asked = []

        def rpc_for(node):
            rpc = MagicMock()

            def status(jm_vuid):
                asked.append((node.get_id(), jm_vuid))
                answer = statuses[(node.get_id(), jm_vuid)]
                if isinstance(answer, Exception):
                    raise answer
                return answer
            rpc.jc_get_jm_status.side_effect = status
            return rpc

        from simplyblock_core.controllers import lvol_controller
        with patch.object(device_controller, "DBController") as db_cls, \
                patch.object(StorageNode, "rpc_client", autospec=True,
                             side_effect=lambda self, **kw: rpc_for(self)), \
                patch.object(lvol_controller, "is_node_leader",
                             side_effect=lambda node, lvs: leaders.get(lvs) == node.get_id()):
            db_cls.return_value.get_storage_nodes_by_cluster_id.return_value = nodes
            pending = device_controller.jm_resync_pending(nodes[0])
        return pending, asked

    def test_in_sync_when_every_leader_reports_every_jm_ready(self):
        n1 = self._node("n1", "LVS_1", 1, jm_ids=["jm-n2"], sec="n2")
        n2 = self._node("n2", "LVS_2", 2, jm_ids=["jm-n1"], sec="n1")
        pending, asked = self._pending(
            [n1, n2], {"LVS_1": "n1", "LVS_2": "n2"},
            {("n1", 1): {"jm_n1": True, "remote_jm_n2n1": True},
             ("n2", 2): {"jm_n2": True, "remote_jm_n1n1": True}})
        self.assertEqual(pending, [])
        # Its own journal, and the one it is a remote copy of.
        self.assertEqual(sorted(asked), [("n1", 1), ("n2", 2)])

    def test_a_journal_it_is_no_copy_of_is_not_asked_about(self):
        n1 = self._node("n1", "LVS_1", 1)
        n3 = self._node("n3", "LVS_3", 3, jm_ids=["jm-n2"])
        pending, asked = self._pending([n1, n3], {"LVS_1": "n1", "LVS_3": "n3"},
                                       {("n1", 1): {"jm_n1": True}})
        self.assertEqual((pending, asked), ([], [("n1", 1)]))

    def test_replication_running_is_pending(self):
        n1 = self._node("n1", "LVS_1", 1)
        pending, _ = self._pending([n1], {"LVS_1": "n1"},
                                   {("n1", 1): {"jm_n1": False, "remote_jm_n2n1": True}})
        self.assertEqual(len(pending), 1)
        self.assertIn("LVS_1", pending[0])
        self.assertIn("jm_n1", pending[0])

    def test_the_leader_is_asked_not_the_owner(self):
        n1 = self._node("n1", "LVS_1", 1, sec="n2")
        n2 = self._node("n2", "", 0)
        pending, asked = self._pending([n1, n2], {"LVS_1": "n2"}, {("n2", 1): {"jm_n1": True}})
        self.assertEqual((pending, asked), ([], [("n2", 1)]))

    def test_no_leader_is_pending(self):
        n1 = self._node("n1", "LVS_1", 1)
        pending, asked = self._pending([n1], {}, {})
        self.assertEqual(asked, [])
        self.assertIn("no leader", pending[0])

    def test_a_leader_that_cannot_be_asked_is_pending(self):
        n1 = self._node("n1", "LVS_1", 1)
        pending, _ = self._pending([n1], {"LVS_1": "n1"}, {("n1", 1): RPCException("down")})
        self.assertIn("down", pending[0])

    def test_an_empty_answer_is_pending(self):
        n1 = self._node("n1", "LVS_1", 1)
        pending, _ = self._pending([n1], {"LVS_1": "n1"}, {("n1", 1): {}})
        self.assertEqual(len(pending), 1)


# -- the task ---------------------------------------------------------------

@pytest.fixture
def runner(monkeypatch):
    import simplyblock_core.services.tasks_runner_restart as module

    # checkpoint / set_result commit onto the stored row, not onto the frozen
    # view the handler carries on with: the rows the tests hold are the store.
    _ROWS.clear()

    def atomic_update(obj, mutate):
        row = _ROWS[obj.uuid]
        mutate(row)
        return row

    monkeypatch.setattr(trb.db, "atomic_update", atomic_update)
    monkeypatch.setattr(module.time, "sleep", lambda s: None)

    node = StorageNode()
    node.uuid = "n1"
    node.cluster_id = "c1"
    node.status = StorageNode.STATUS_ONLINE
    peer = StorageNode()
    peer.uuid = "n2"
    peer.status = StorageNode.STATUS_ONLINE
    cluster = MagicMock()
    cluster.status = Cluster.STATUS_ACTIVE

    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    db.get_storage_nodes_by_cluster_id.return_value = [node, peer]
    db.get_cluster_by_id.return_value = cluster
    monkeypatch.setattr(module, "db", db)

    dc = MagicMock()
    dc.JMReshapeNotReady = device_controller.JMReshapeNotReady
    dc.JMReshapeError = device_controller.JMReshapeError
    dc.jm_raid_reshape_needed.return_value = True
    dc.jm_resync_pending.return_value = []
    dc.reshape_jm_raid.return_value = ["a", "b"]
    monkeypatch.setattr(module, "device_controller", dc)

    tc = MagicMock()
    tc.get_jm_raid_reshape_blocking_tasks.return_value = []
    tc.get_active_jm_raid_reshape_tasks.return_value = []
    monkeypatch.setattr(module, "tasks_controller", tc)

    module.node, module.peer, module.cluster = node, peer, cluster
    module.dc, module.tc = dc, tc
    yield module
    for name in ("node", "peer", "cluster", "dc", "tc"):
        delattr(module, name)


_ROWS: dict = {}


def _task(uuid="t1", node_id="n1", **params):
    task = JobSchedule()
    task.uuid = uuid
    task.cluster_id = "c1"
    task.node_id = node_id
    task.function_name = JobSchedule.FN_JM_RAID_RESHAPE
    task.function_params = params
    _ROWS[uuid] = task
    return task


def test_the_restart_runner_serves_the_task(runner):
    assert JobSchedule.FN_JM_RAID_RESHAPE in runner.SPEC.function_names
    task = _task()
    runner.dc.jm_raid_reshape_needed.return_value = False
    runner.SPEC.handler(task)
    assert "already matches" in task.function_result


def test_nothing_to_do_when_the_raid_matches(runner):
    runner.dc.jm_raid_reshape_needed.return_value = False
    task = _task()
    runner.task_runner_jm_reshape(task)
    runner.dc.reshape_jm_raid.assert_not_called()
    assert "phase" not in task.function_params


@pytest.mark.parametrize("block, reason", [
    (lambda r: setattr(r.cluster, "status", Cluster.STATUS_DEGRADED), "cluster is"),
    (lambda r: setattr(r.peer, "status", StorageNode.STATUS_OFFLINE), "node n2 is"),
    (lambda r: setattr(r.tc.get_jm_raid_reshape_blocking_tasks, "return_value",
                       [MagicMock(function_name="new_device_migration", uuid="m1")]), "is open"),
    (lambda r: setattr(r.dc.jm_resync_pending, "return_value", ["LVS_1: ['jm_n2']"]),
     "journal not in sync"),
])
def test_it_waits_and_touches_nothing_while_the_cluster_is_not_quiet(runner, block, reason):
    block(runner)
    task = _task()
    with pytest.raises(TaskDefer) as exc:
        runner.task_runner_jm_reshape(task)
    assert reason in str(exc.value)
    runner.dc.reshape_jm_raid.assert_not_called()
    assert "phase" not in task.function_params


def test_one_node_at_a_time(runner):
    other = _task("t2", "n2", phase="resync")
    queued = _task("t3", "n3")
    runner.tc.get_active_jm_raid_reshape_tasks.return_value = [queued, other]
    task = _task()
    with pytest.raises(TaskDefer) as exc:
        runner.task_runner_jm_reshape(task)
    assert "node n2 is being rebuilt" in str(exc.value)
    runner.dc.reshape_jm_raid.assert_not_called()


def test_a_task_that_only_waits_does_not_hold_up_another(runner):
    queued = _task("t3", "n3")
    runner.tc.get_active_jm_raid_reshape_tasks.return_value = [queued]
    task = _task()
    with pytest.raises(TaskProgress):
        runner.task_runner_jm_reshape(task)
    runner.dc.reshape_jm_raid.assert_called_once_with("n1")


def test_a_removed_node_between_the_others_does_not_block(runner):
    runner.peer.status = StorageNode.STATUS_REMOVED
    with pytest.raises(TaskProgress):
        runner.task_runner_jm_reshape(_task())
    runner.dc.reshape_jm_raid.assert_called_once()


def test_the_phase_is_recorded_before_the_stack_is_taken_down(runner):
    task = _task()
    seen = {}
    runner.dc.reshape_jm_raid.side_effect = lambda node_id: (
        seen.update(phase=task.function_params.get("phase")), ["a", "b"])[1]
    with pytest.raises(TaskProgress):
        runner.task_runner_jm_reshape(task)
    assert seen["phase"] == "rebuild"
    assert task.function_params["phase"] == "resync"
    assert task.function_params["members"] == ["a", "b"]


def test_resync_is_polled_until_the_journals_stay_in_sync(runner):
    task = _task()
    runner.dc.jm_resync_pending.side_effect = [[], ["LVS_1: ['jm_n1']"]]   # precondition, then poll
    with pytest.raises(TaskProgress) as exc:
        runner.task_runner_jm_reshape(task)
    assert "resync running" in str(exc.value)

    runner.dc.jm_resync_pending.side_effect = None
    runner.dc.jm_resync_pending.return_value = []
    for polls in range(1, runner.JM_RESHAPE_READY_POLLS):
        with pytest.raises(TaskProgress):
            runner.task_runner_jm_reshape(task)
        assert task.function_params["ready_polls"] == polls
    runner.task_runner_jm_reshape(task)
    assert "journals in sync" in task.function_result
    # Built once; every later cycle only polled.
    runner.dc.reshape_jm_raid.assert_called_once()


def test_a_poll_that_finds_replication_again_starts_the_count_over(runner):
    task = _task(phase="resync", members=["a", "b"], ready_polls=2)
    runner.dc.jm_resync_pending.return_value = ["LVS_1: ['jm_n1']"]
    with pytest.raises(TaskProgress):
        runner.task_runner_jm_reshape(task)
    assert task.function_params["ready_polls"] == 0
    runner.dc.reshape_jm_raid.assert_not_called()


@pytest.mark.parametrize("error", [
    device_controller.JMReshapeNotReady("member gone"),
    device_controller.JMReshapeError("build failed"),
    RPCException("boom"),
])
def test_a_failed_rebuild_is_retried_without_asking_the_preconditions_again(runner, error):
    task = _task()
    runner.dc.reshape_jm_raid.side_effect = error
    with pytest.raises(TaskRetry):
        runner.task_runner_jm_reshape(task)
    assert task.function_params["phase"] == "rebuild"

    # The stack is down now: the next attempt builds whatever the cluster
    # looks like, and even when the record already matches the devices.
    runner.cluster.status = Cluster.STATUS_DEGRADED
    runner.dc.jm_raid_reshape_needed.return_value = False
    runner.dc.reshape_jm_raid.side_effect = None
    with pytest.raises(TaskProgress):
        runner.task_runner_jm_reshape(task)
    assert runner.dc.reshape_jm_raid.call_count == 2
    assert task.function_params["phase"] == "resync"


def test_node_states(runner):
    runner.node.status = StorageNode.STATUS_OFFLINE
    with pytest.raises(TaskDefer):
        runner.task_runner_jm_reshape(_task())
    runner.node.status = StorageNode.STATUS_REMOVED
    with pytest.raises(TaskAbort):
        runner.task_runner_jm_reshape(_task())
    runner.db.get_storage_node_by_id.side_effect = KeyError("n1")
    with pytest.raises(TaskAbort):
        runner.task_runner_jm_reshape(_task())
    runner.dc.reshape_jm_raid.assert_not_called()


def test_giving_up_does_not_touch_a_device_or_the_node(runner):
    task = _task()
    task.max_retry = 3
    task.retry = 3
    runner._abandon_task(task)
    runner.dc.device_set_unavailable.assert_not_called()
    runner.dc.device_set_retries_exhausted.assert_not_called()
