"""Unit tests for the lblk journal raid.

An lblk storage node of two or more devices carries a journal partition of
the same size on every one of them, and its journal is the RAID 0+1 over
those members:

  - jm_raid.member_size_for_usable / utils.lblk_journal_raid_member_size:
    jm_percent is the journal the node can use, so the member size follows
    from the raid geometry.
  - utils.split_lblk_journal_raid: every device split, entry flags, order.
  - node_utils: the ``sb_jmr`` partition name that tells a member from a
    node's single journal partition, written and recognised.
  - storage_node_ops: the members found among the node's devices, in raid
    order, and never taken for new data devices at a restart.
  - device_controller: the member that shares a device with a lost data
    device, and whether it is really gone.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, jm_raid, storage_node_ops, utils
from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import JMDevice, NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClientException
from simplyblock_web import node_utils

GIB = 1024 ** 3
MIB = 1024 ** 2


def _entry(name, size, **extra):
    entry = {"name": name, "serial": f"SER-{name}", "by_id": f"/dev/disk/by-id/{name}",
             "size": size, "numa": 0}
    entry.update(extra)
    return entry


def _inv(name, size, dtype="part"):
    return {"name": name, "serial": f"SER-{name}", "by_id_path": f"/dev/disk/by-id/{name}",
            "device_path": f"/dev/{name}", "size": size, "numa_node": 0, "type": dtype,
            "partuuid": f"PU-{name}", "parent_serial": "PARENT"}


class TestMemberSize(unittest.TestCase):

    def test_member_size_follows_the_raid_geometry(self):
        self.assertEqual(jm_raid.member_size_for_usable(9 * GIB, 1), 9 * GIB)
        self.assertEqual(jm_raid.member_size_for_usable(9 * GIB, 2), 9 * GIB)
        self.assertEqual(jm_raid.member_size_for_usable(9 * GIB, 3), 9 * GIB)
        self.assertEqual(jm_raid.member_size_for_usable(9 * GIB, 4), 9 * GIB // 2)
        self.assertEqual(jm_raid.member_size_for_usable(9 * GIB, 5), 9 * GIB // 2)
        self.assertEqual(jm_raid.member_size_for_usable(10, 6), 4)  # rounded up

    def test_no_members_refused(self):
        with self.assertRaises(ValueError):
            jm_raid.member_size_for_usable(GIB, 0)

    def test_planned_raid_is_at_least_as_large_as_asked(self):
        # The raid1 is as large as its smaller raid0 leg.
        usable = 7 * GIB + 13
        for count in range(2, 12):
            member = jm_raid.member_size_for_usable(usable, count)
            legs = jm_raid.plan_topology([f"m{i}" for i in range(count)])["legs"]
            smaller_leg = min(len(leg) for leg in legs) * member
            self.assertGreaterEqual(smaller_leg, usable, count)
            self.assertLess(smaller_leg - usable, min(len(leg) for leg in legs), count)

    def test_legs_left_when_a_member_is_missing(self):
        self.assertEqual(jm_raid.mountable_top_legs(["l0", "l1"], [], ["l1"]), ["l0", "l1"])
        self.assertEqual(jm_raid.mountable_top_legs(["l0", "l1"], ["l1"], []), ["l0"])
        self.assertEqual(jm_raid.mountable_top_legs(["l0", "l1"], ["l1"], ["l0"]), [])
        self.assertEqual(jm_raid.mountable_top_legs(["l0", "l1"], ["l0", "l1"], []), [])
        self.assertEqual(jm_raid.mountable_top_legs(list("abc"), ["b"], ["a"]), ["a", "c"])

    def test_jm_on_a_single_leg(self):
        self.assertTrue(jm_raid.runs_on_single_leg("l0", list("abcd"), ["l0", "l1"]))
        self.assertTrue(jm_raid.runs_on_single_leg("a", ["a", "b"], ["a", "b"]))
        self.assertTrue(jm_raid.runs_on_single_leg("c", list("abc"), []))  # legacy mirror
        self.assertFalse(jm_raid.runs_on_single_leg("raid_jm_n", ["a", "b"], ["a", "b"]))
        self.assertFalse(jm_raid.runs_on_single_leg("a", ["a"], []))       # single-drive JM

    def test_three_devices_of_150_200_100_at_two_percent(self):
        # 2% of 450G is a 9G journal; the mirror's smaller leg is one member.
        sizes = [150 * GIB, 200 * GIB, 100 * GIB]
        self.assertEqual(utils.lblk_journal_raid_member_size(sizes, jm_percent=2), 9 * GIB)

    def test_four_devices_halve_the_member(self):
        self.assertEqual(
            utils.lblk_journal_raid_member_size([100 * GIB] * 4, jm_percent=3), 6 * GIB)

    def test_journal_floor(self):
        self.assertEqual(utils.lblk_journal_raid_member_size([10 * GIB, 10 * GIB]),
                         constants.LBLK_JM_MIN_SIZE)

    def test_member_is_a_whole_number_of_mib(self):
        member = utils.lblk_journal_raid_member_size([100 * GIB + 7, 100 * GIB + 5, 300 * GIB])
        self.assertEqual(member % MIB, 0)
        self.assertGreaterEqual(member, (500 * GIB + 12) * 3 // 100)

    def test_member_larger_than_half_the_smallest_device_refused(self):
        with self.assertRaises(ValueError) as ctx:
            utils.lblk_journal_raid_member_size([10 * GIB, 1000 * GIB, 1000 * GIB])
        self.assertIn("smallest", str(ctx.exception))


class TestSplitJournalRaid(unittest.TestCase):

    def _split(self, entries, member_bytes, names=None, fail_on=None):
        calls = []

        def fake(kind):
            def _fake(name, jm_bytes, journal_name=None):
                if name == fail_on:
                    raise ValueError(f"sgdisk failed on {name}")
                calls.append((kind, name, jm_bytes, journal_name))
                return (_inv(f"{name}j", jm_bytes), _inv(f"{name}d", entries[name]["size"] - jm_bytes))
            return _fake

        with patch.object(utils.node_utils, "split_partition_for_journal", side_effect=fake("part")), \
                patch.object(utils.node_utils, "partition_disk_for_journal", side_effect=fake("disk")):
            return utils.split_lblk_journal_raid(entries, member_bytes, names=names), calls

    def test_every_device_gets_a_member_of_the_same_size(self):
        entries = {"sdb": _entry("sdb", 150 * GIB), "sdc1": _entry("sdc1", 200 * GIB, type="part"),
                   "sdd": _entry("sdd", 100 * GIB)}
        result, calls = self._split(entries, 9 * GIB)
        self.assertEqual(calls, [("disk", "sdb", 9 * GIB, "sb_jmr"),
                                 ("part", "sdc1", 9 * GIB, "sb_jmr"),
                                 ("disk", "sdd", 9 * GIB, "sb_jmr")])
        # Journal and data of a device next to each other, in device order.
        self.assertEqual(list(result), ["sdbj", "sdbd", "sdc1j", "sdc1d", "sddj", "sddd"])
        for name in ("sdb", "sdc1", "sdd"):
            jm, data = result[f"{name}j"], result[f"{name}d"]
            self.assertTrue(jm["journal"] and jm["journal_raid"])
            self.assertEqual(jm["size"], 9 * GIB)
            self.assertEqual(jm["data_serial"], data["serial"])
            self.assertNotIn("journal", data)

    def test_only_the_named_devices_are_split(self):
        entries = {"sdbj": _entry("sdbj", 9 * GIB, journal=True, journal_raid=True),
                   "sdbd": _entry("sdbd", 91 * GIB), "sdc": _entry("sdc", 100 * GIB)}
        result, calls = self._split(entries, 9 * GIB, names=["sdc"])
        self.assertEqual([c[1] for c in calls], ["sdc"])
        self.assertEqual(list(result), ["sdbj", "sdbd", "sdcj", "sdcd"])

    def test_a_failed_split_raises(self):
        entries = {"sdb": _entry("sdb", 100 * GIB), "sdc": _entry("sdc", 100 * GIB)}
        with self.assertRaises(ValueError):
            self._split(entries, 9 * GIB, fail_on="sdc")

    def test_resolution_keeps_the_member_flag(self):
        configured = [_entry("sdbj", 9 * GIB, journal=True, journal_raid=True)]
        live = [dict(_inv("sdbj", 9 * GIB), mounted_in_subtree=False, holders=[],
                     is_root_disk=False)]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(missing, [])
        self.assertTrue(resolved[0]["journal"] and resolved[0]["journal_raid"])

    def test_member_bdev_of_a_data_device(self):
        entries = [_entry("sdbj", GIB, journal=True, journal_raid=True, data_serial="SER-sdbd"),
                   _entry("sdbd", GIB),
                   _entry("sdcj", GIB, journal=True, data_serial="SER-sdcd")]
        self.assertEqual(utils.lblk_journal_raid_member_bdev(entries, "SER-sdbd"),
                         utils.aio_bdev_name_for_serial("SER-sdbj"))
        # A single journal partition is not a raid member.
        self.assertEqual(utils.lblk_journal_raid_member_bdev(entries, "SER-sdcd"), "")
        self.assertEqual(utils.lblk_journal_raid_member_bdev(entries, ""), "")
        self.assertEqual(utils.lblk_journal_raid_member_bdev(None, "SER-sdbd"), "")


class TestJournalPartitionName(unittest.TestCase):

    def _fields(self, names):
        return lambda part: {"name": names.get(part, ""),
                             "type": node_utils.SB_GPT_PARTITION_TYPECODE}

    def test_unknown_name_refused_before_anything_is_touched(self):
        with patch.object(node_utils, "get_block_devices_info") as inventory:
            for split in (node_utils.partition_disk_for_journal,
                          node_utils.split_partition_for_journal):
                with self.assertRaises(ValueError):
                    split("sdb", GIB, journal_name="sb_other")
        inventory.assert_not_called()

    def test_member_name_tells_a_member_from_a_single_journal(self):
        names = {"sdb1": "sb_jmr", "sdc1": "sb_jm", "sdc2": "sb_data"}
        with patch.object(node_utils, "_partition_entry_fields", side_effect=self._fields(names)):
            self.assertTrue(node_utils.is_journal_raid_member("sdb1"))
            self.assertFalse(node_utils.is_journal_raid_member("sdc1"))
            self.assertFalse(node_utils.is_journal_raid_member("sdc2"))

    def test_disk_split_into_a_member_is_recognised(self):
        devices = [{"name": "sdb1", "type": "part", "parent_name": "sdb"},
                   {"name": "sdb2", "type": "part", "parent_name": "sdb"}]
        names = {"sdb1": "sb_jmr", "sdb2": "sb_data"}
        with patch.object(node_utils, "_partition_entry_fields", side_effect=self._fields(names)):
            pair = node_utils.journal_split_partitions("sdb", devices)
        self.assertEqual([p["name"] for p in pair], ["sdb1", "sdb2"])

    def test_partition_split_into_a_member_is_recognised_from_either_half(self):
        devices = [{"name": "sda3", "type": "part", "parent_name": "sda"},
                   {"name": "sda5", "type": "part", "parent_name": "sda"}]
        names = {"sda3": "sb_jmr", "sda5": "sb_data"}
        sysfs = {"/sys/block/sda/sda3/start": "2048", "/sys/block/sda/sda3/size": "1000",
                 "/sys/block/sda/sda5/start": "3048", "/sys/block/sda/sda5/size": "5000"}
        with patch.object(node_utils, "_partition_entry_fields", side_effect=self._fields(names)), \
                patch.object(node_utils, "_read_sysfs", side_effect=lambda p: sysfs.get(p, "")):
            for part in devices:
                pair = node_utils.split_partition_partner(part, devices)
                self.assertEqual([p["name"] for p in pair], ["sda3", "sda5"])

    def test_disk_member_partition_is_named_sb_jmr(self):
        disk = {"name": "sdb", "type": "disk", "size": 100 * GIB, "mounted_in_subtree": False,
                "holders": [], "is_root_disk": False, "has_partitions": False}
        parts = [{"name": "sdb1"}, {"name": "sdb2"}]
        inventories = [[disk], parts]
        commands = []

        def fake_run(cmd):
            commands.append(cmd)
            return "", "", 0

        with patch.object(node_utils, "get_block_devices_info",
                          side_effect=lambda: inventories.pop(0) if len(inventories) > 1 else inventories[0]), \
                patch.object(node_utils.shell_utils, "run_command", side_effect=fake_run), \
                patch.object(node_utils, "simplyblock_data_signature", return_value=False), \
                patch.object(node_utils, "_read_sysfs",
                             side_effect=lambda p: p.split("/")[-2][-1] if p.endswith("/partition") else ""), \
                patch("os.listdir", return_value=["sdb1", "sdb2"]):
            node_utils.partition_disk_for_journal(
                "sdb", 9 * GIB, journal_name=node_utils.SB_JOURNAL_RAID_PARTITION_NAME)
        sg = [c for c in commands if c.startswith("sgdisk -n")]
        self.assertIn(f"-n 1:0:+{9 * 1024}M", sg[0])
        self.assertIn("-c 1:sb_jmr ", sg[0])
        self.assertIn("-c 2:sb_data ", sg[1])


def _aio(serial):
    return NVMeDevice({"uuid": f"id-{serial}", "serial_number": serial, "bdev_type": "aio",
                       "nvme_bdev": utils.aio_bdev_name_for_serial(serial),
                       "status": NVMeDevice.STATUS_ONLINE})


def _raid_node(devices=3):
    """A node of ``devices`` block devices, each a journal raid member (Jn)
    and a data partition (Dn)."""
    node = StorageNode()
    node.uuid = "node-1"
    node.lblk_devices = []
    for i in range(devices):
        node.lblk_devices.append({"name": f"sd{i}1", "serial": f"J{i}", "size": GIB,
                                  "journal": True, "journal_raid": True, "data_serial": f"D{i}"})
        node.lblk_devices.append({"name": f"sd{i}2", "serial": f"D{i}", "size": 9 * GIB})
    members = [utils.aio_bdev_name_for_serial(f"J{i}") for i in range(devices)]
    legs = jm_raid.plan_topology(members)["legs"] if devices > 1 else []
    node.jm_device = JMDevice({
        "uuid": "node-1", "status": JMDevice.STATUS_ONLINE, "raid_bdev": "raid_jm_node-1",
        "jm_nvme_bdev_list": members,
        "jm_leg_members": legs,
        "jm_leg_bdevs": [leg[0] if len(leg) == 1 else f"raid_jm_node-1_l{i}"
                         for i, leg in enumerate(legs)],
    })
    node.nvme_devices = [_aio(f"D{i}") for i in range(devices)]
    return node


class TestMembersAmongDevices(unittest.TestCase):

    def test_members_come_back_in_the_order_of_the_node_config(self):
        node = _raid_node(3)
        # Discovery order is not the config order.
        devices = [_aio(s) for s in ("D2", "J2", "D0", "J1", "D1", "J0")]
        members, missing = storage_node_ops._lblk_journal_raid_members(node.lblk_devices, devices)
        self.assertEqual([d.serial_number for d in members], ["J0", "J1", "J2"])
        self.assertEqual(missing, [])

    def test_member_without_a_device_is_reported(self):
        node = _raid_node(2)
        devices = [_aio(s) for s in ("D0", "J0", "D1")]
        members, missing = storage_node_ops._lblk_journal_raid_members(node.lblk_devices, devices)
        self.assertEqual([d.serial_number for d in members], ["J0"])
        self.assertEqual([e["serial"] for e in missing], ["J1"])

    def test_node_without_a_journal_raid(self):
        entries = [{"name": "sdb1", "serial": "J", "journal": True}, {"name": "sdb2", "serial": "D"}]
        self.assertEqual(
            storage_node_ops._lblk_journal_raid_members(entries, [_aio("J"), _aio("D")]), ([], []))
        self.assertEqual(storage_node_ops._lblk_journal_raid_members(None, []), ([], []))

    def test_restart_does_not_take_members_for_new_data_devices(self):
        node = _raid_node(2)
        discovered = [_aio(s) for s in ("J0", "D0", "J1", "D1", "NEW")]
        new_devices = storage_node_ops._reconcile_discovered_devices(node, discovered)
        self.assertEqual([d.serial_number for d in new_devices], ["NEW"])
        self.assertEqual(sorted(d.serial_number for d in node.nvme_devices), ["D0", "D1", "NEW"])


class TestRestartWithMissingMember(unittest.TestCase):
    """_prepare_cluster_devices_on_restart hands the builder the JM's full
    member list and names the missing members, instead of a list of the
    members found."""

    def _restart(self, node, absent, build):
        node.nvme_devices = []
        rpc = MagicMock()
        rpc.bdev_get.side_effect = lambda name: None if name in absent else {"name": name}
        with patch.object(StorageNode, "rpc_client", return_value=rpc), \
                patch.object(StorageNode, "write_to_db"), \
                patch.object(storage_node_ops, "apply_jc_dual_node"), \
                patch.object(storage_node_ops, "_create_jm_stack_on_raid", side_effect=build) as create:
            result = storage_node_ops._prepare_cluster_devices_on_restart(node)
        return result, create

    def test_full_member_list_with_the_missing_ones_named(self):
        node = _raid_node(4)
        members = list(node.jm_device.jm_nvme_bdev_list)
        rebuilt = JMDevice({"uuid": "node-1", "raid_bdev": "raid_jm_node-1_l0"})
        result, create = self._restart(node, absent={members[2]}, build=lambda *a, **k: rebuilt)
        self.assertTrue(result)
        self.assertEqual(create.call_args.args[1], members)
        self.assertEqual(create.call_args.kwargs["missing"], [members[2]])
        self.assertIs(node.jm_device, rebuilt)

    def test_no_current_leg_leaves_the_node_without_a_journal(self):
        node = _raid_node(2)

        def build(*args, **kwargs):
            raise storage_node_ops.JMRaidNoCurrentLeg("no leg")

        result, _ = self._restart(node, absent=set(node.jm_device.jm_nvme_bdev_list), build=build)
        self.assertTrue(result)
        self.assertEqual(node.jm_device.status, JMDevice.STATUS_REMOVED)


class TestMemberOfLostDevice(unittest.TestCase):

    def _present(self, node, device, inventory=None, error=None):
        client = MagicMock()
        if error:
            client.get_blockdevices.side_effect = error
        else:
            client.get_blockdevices.return_value = (inventory, None)
        member = device_controller._jm_raid_member_of_device(node, device)
        with patch.object(StorageNode, "client", return_value=client):
            return device_controller._jm_raid_member_present(node, device, member, MagicMock())

    def test_member_shares_the_device_with_the_data_partition(self):
        node = _raid_node(3)
        self.assertEqual(device_controller._jm_raid_member_of_device(node, node.nvme_devices[1]),
                         utils.aio_bdev_name_for_serial("J1"))

    def test_device_without_a_member(self):
        node = _raid_node(2)
        self.assertEqual(device_controller._jm_raid_member_of_device(node, _aio("OTHER")), "")

    def test_nvme_member_is_the_partition_on_the_same_controller(self):
        node = StorageNode()
        node.jm_device = JMDevice({"jm_nvme_bdev_list": ["nvme_61n1p1", "nvme_611n1p1"]})
        device = NVMeDevice({"nvme_controller": "nvme_611", "nvme_bdev": "nvme_611n1p2"})
        self.assertEqual(device_controller._jm_raid_member_of_device(node, device), "nvme_611n1p1")

    def test_member_on_the_host_is_present(self):
        node = _raid_node(2)
        self.assertTrue(self._present(node, node.nvme_devices[0], [{"serial": "J0"}, {"serial": "J1"}]))

    def test_member_missing_from_the_host_is_gone(self):
        node = _raid_node(2)
        self.assertFalse(self._present(node, node.nvme_devices[0], [{"serial": "J1"}, {"serial": "D1"}]))

    def test_unknown_presence_keeps_the_member(self):
        # Taking a present member out of the raid is the hazard (SFAM-2843).
        node = _raid_node(2)
        device = node.nvme_devices[0]
        self.assertTrue(self._present(node, device, error=SNodeClientException("unreachable")))
        self.assertTrue(self._present(node, device, inventory=None))

    def _remove(self, node, device, present):
        rpc = MagicMock()
        rpc.bdev_raid_get_bdevs.return_value = [{
            "name": node.jm_device.raid_bdev,
            "base_bdevs_list": [{"name": leg} for leg in node.jm_device.jm_leg_bdevs]}]
        with patch.object(device_controller, "_jm_raid_member_present", return_value=present), \
                patch.object(device_controller, "remove_from_jm_device") as remove, \
                patch.object(device_controller, "set_jm_device_state") as set_state:
            device_controller._remove_jm_raid_leg_of_device(node, device, rpc)
        return rpc, remove, set_state

    def test_gone_member_inside_a_raid0_leg_takes_its_leg_out(self):
        # Three devices: J0 and J1 are striped into leg 0, J2 is leg 1.
        node = _raid_node(3)
        member = utils.aio_bdev_name_for_serial("J0")
        rpc, remove, set_state = self._remove(node, node.nvme_devices[0], present=False)
        remove.assert_called_once_with("node-1", member)
        rpc.bdev_aio_delete.assert_called_once_with(member)
        set_state.assert_not_called()

    def test_present_member_stays_in_the_raid(self):
        node = _raid_node(3)
        rpc, remove, _ = self._remove(node, node.nvme_devices[2], present=True)
        remove.assert_not_called()
        rpc.bdev_aio_delete.assert_not_called()

    def test_jm_on_a_single_leg_is_lost_only_with_that_leg(self):
        # A restart found J2 missing: legs are (J0, J1) and (J2), and the JM
        # runs on the raid0 leg alone, with no mirror above it.
        node = _raid_node(3)
        node.jm_device.raid_bdev = node.jm_device.jm_leg_bdevs[0]
        rpc, remove, set_state = self._remove(node, node.nvme_devices[2], present=False)
        remove.assert_not_called()
        set_state.assert_not_called()
        rpc, remove, set_state = self._remove(node, node.nvme_devices[0], present=False)
        remove.assert_not_called()
        set_state.assert_called_once_with("node-1", JMDevice.STATUS_UNAVAILABLE)

    def test_returning_leg_is_not_added_to_a_jm_on_a_single_leg(self):
        node = _raid_node(3)
        node.jm_device.raid_bdev = node.jm_device.jm_leg_bdevs[0]
        rpc = MagicMock()
        device_controller._readd_jm_raid_leg(node, utils.aio_bdev_name_for_serial("J2"), rpc)
        rpc.bdev_raid_add_base_bdev.assert_not_called()

    def test_returned_member_is_opened_again(self):
        node = _raid_node(2)
        member = utils.aio_bdev_name_for_serial("J0")
        rpc = MagicMock()
        rpc.bdev_get.side_effect = [None, {"name": member}]
        client = MagicMock()
        client.get_blockdevices.return_value = (
            [{"serial": "J0", "by_id_path": "/dev/disk/by-id/j0", "device_path": "/dev/sdx1"}], None)
        with patch.object(StorageNode, "client", return_value=client):
            self.assertTrue(device_controller._ensure_aio_jm_member(
                node, node.nvme_devices[0], member, rpc))
        rpc.bdev_aio_create.assert_called_once_with(member, "/dev/disk/by-id/j0")

    def test_member_still_absent_is_not_opened(self):
        node = _raid_node(2)
        rpc = MagicMock()
        rpc.bdev_get.return_value = None
        client = MagicMock()
        client.get_blockdevices.return_value = ([{"serial": "J1"}], None)
        with patch.object(StorageNode, "client", return_value=client):
            self.assertFalse(device_controller._ensure_aio_jm_member(
                node, node.nvme_devices[0], utils.aio_bdev_name_for_serial("J0"), rpc))
        rpc.bdev_aio_create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
