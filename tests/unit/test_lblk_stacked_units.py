"""Unit tests for lblk nodes built on a software-RAID array or a multipath map.

An administrator who built an md array out of a worker's disks, or attached a
LUN of a storage array that multipathd joined into one device-mapper map, means
that device to be the storage. The members and the paths are held by it and
stay refused; the array or the map is a unit like a whole disk.

Covered:
  - get_block_devices_info: an array and a map are inventoried as units under
    their kernel names, with an identity read from the md superblock UUID or
    the map's WWID, their members listed, and a map's own partition maps not
    counted as holders.
  - filter_eligible_block_devices: an array and a map are eligible; a logical
    volume or an encrypted volume is not; auto-selection takes plain disks
    ahead of arrays and maps and takes the arrays and maps when there is no
    plain disk.
  - read_persistent_links: the md-uuid and dm-uuid-mpath links outrank the
    name links beside them.
  - partition_disk_for_journal: an array is laid out like a disk; a multipath
    map is refused with a reason, because the kernel does not read a partition
    table off a device-mapper node.
"""

import json
import os
import shutil
import tempfile
import unittest
from typing import ClassVar
from unittest.mock import patch

from simplyblock_core import utils
from simplyblock_web import node_utils

GIB = 1024 ** 3
MD_UUID = "8c2b6f4a:1d3e5f70:9a8b7c6d:5e4f3a2b"
WWID = "3600a098038303f6e5d5a4a4d31774a63"


def _unit(name, dtype="disk", serial="", size=100 * GIB, holders=None, by_id=""):
    return {
        "name": name, "device_path": f"/dev/{name}", "type": dtype, "size": size,
        "serial": serial or f"SER-{name}", "serial_synthetic": False, "wwn": "",
        "model": "", "vendor": "", "rota": False, "ro": False, "has_partitions": False,
        "mounted_in_subtree": False, "holders": holders or [], "is_root_disk": False,
        "by_id_path": by_id or f"/dev/disk/by-id/acme-{name}", "numa_node": 0,
    }


class TestInventoryOfArraysAndMaps(unittest.TestCase):
    LSBLK: ClassVar[dict] = {
        "blockdevices": [
            {"name": "sda", "kname": "sda", "type": "disk", "size": 4 * GIB, "serial": "",
             "wwn": "0x600a098038303f6e", "mountpoint": None, "model": "LUN C-Mode",
             "rota": False, "ro": False, "vendor": "NETAPP",
             "children": [
                 {"name": "mpatha", "kname": "dm-0", "type": "mpath", "size": 4 * GIB,
                  "mountpoint": None, "ro": False,
                  "children": [
                      {"name": "mpatha1", "kname": "dm-2", "type": "part", "size": GIB,
                       "partuuid": "AAAA-01", "mountpoint": None, "ro": False},
                  ]},
             ]},
            {"name": "sdb", "kname": "sdb", "type": "disk", "size": 4 * GIB, "serial": "",
             "wwn": "0x600a098038303f6e", "mountpoint": None, "model": "LUN C-Mode",
             "rota": False, "ro": False, "vendor": "NETAPP",
             "children": [
                 {"name": "mpatha", "kname": "dm-0", "type": "mpath", "size": 4 * GIB,
                  "mountpoint": None, "ro": False},
             ]},
            {"name": "sdc", "kname": "sdc", "type": "disk", "size": 2 * GIB, "serial": "S-C",
             "wwn": "", "mountpoint": None, "model": "ST2000", "rota": True, "ro": False,
             "vendor": "ATA",
             "children": [
                 {"name": "md0", "kname": "md0", "type": "raid1", "size": 2 * GIB,
                  "mountpoint": None, "ro": False,
                  "children": [
                      {"name": "md0p1", "kname": "md0p1", "type": "part", "size": GIB,
                       "partuuid": "BBBB-01", "mountpoint": None, "ro": False},
                  ]},
             ]},
            {"name": "sdd", "kname": "sdd", "type": "disk", "size": 2 * GIB, "serial": "S-D",
             "wwn": "", "mountpoint": None, "model": "ST2000", "rota": True, "ro": False,
             "vendor": "ATA",
             "children": [
                 {"name": "md0", "kname": "md0", "type": "raid1", "size": 2 * GIB,
                  "mountpoint": None, "ro": False},
             ]},
            {"name": "dm-1", "kname": "dm-1", "type": "lvm", "size": GIB, "serial": "",
             "wwn": "", "mountpoint": "/var", "model": "", "rota": False, "ro": False,
             "vendor": ""},
        ]
    }

    SYSFS: ClassVar[dict] = {
        "/sys/block/md0/md/uuid": MD_UUID,
        "/sys/block/dm-0/dm/uuid": "mpath-" + WWID,
        "/sys/block/dm-0/dm/name": "mpatha",
        "/sys/block/dm-1/dm/uuid": "LVM-abc",
    }
    MEMBERS: ClassVar[dict] = {"md0": ["sdc", "sdd"], "dm-0": ["sda", "sdb"], "dm-1": ["sde"]}
    HOLDERS: ClassVar[dict] = {"sda": ["dm-0"], "sdb": ["dm-0"], "sdc": ["md0"], "sdd": ["md0"],
                               "dm-0": ["dm-2"]}

    def _inventory(self):
        with patch.object(node_utils.shell_utils, "run_command",
                          return_value=(json.dumps(self.LSBLK), "", 0)), \
                patch.object(node_utils, "_root_disk_names", return_value=[]), \
                patch.object(node_utils, "_disk_holders",
                             side_effect=lambda name: self.HOLDERS.get(name, [])), \
                patch.object(node_utils, "_partition_holders", return_value=[]), \
                patch.object(node_utils, "_members",
                             side_effect=lambda name: self.MEMBERS.get(name, [])), \
                patch.object(node_utils, "read_persistent_links", return_value={}), \
                patch.object(node_utils, "_read_sysfs",
                             side_effect=lambda path: self.SYSFS.get(path, "")):
            return {d["name"]: d for d in node_utils.get_block_devices_info()}

    def test_an_array_is_a_unit_named_by_its_superblock(self):
        inv = self._inventory()
        self.assertIn("md0", inv)
        array = inv["md0"]
        self.assertEqual(array["type"], "raid1")
        self.assertEqual(array["device_path"], "/dev/md0")
        self.assertEqual(array["serial"], MD_UUID)
        self.assertFalse(array["serial_synthetic"])
        self.assertEqual(array["members"], ["sdc", "sdd"])
        self.assertTrue(array["has_partitions"])
        self.assertEqual(inv["md0p1"]["parent_name"], "md0")
        self.assertEqual(inv["md0p1"]["parent_serial"], MD_UUID)

    def test_a_multipath_map_is_a_unit_under_its_kernel_name(self):
        inv = self._inventory()
        self.assertIn("dm-0", inv, "the map is inventoried under its kernel name, not mpatha")
        lun = inv["dm-0"]
        self.assertEqual(lun["type"], "mpath")
        self.assertEqual(lun["dm_name"], "mpatha")
        self.assertEqual(lun["device_path"], "/dev/dm-0")
        self.assertEqual(lun["serial"], WWID)
        self.assertFalse(lun["serial_synthetic"])
        self.assertEqual(lun["members"], ["sda", "sdb"])
        # The map appears under both paths and is inventoried once.
        self.assertEqual(sum(1 for d in inv.values() if d.get("type") == "mpath"), 1)

    def test_a_maps_own_partitions_are_not_its_holders(self):
        inv = self._inventory()
        self.assertEqual(inv["dm-0"]["holders"], [],
                         "a partition kpartx mapped out of the map is the map's own, not a holder")
        self.assertTrue(inv["dm-0"]["has_partitions"])
        part = inv["dm-2"]
        self.assertEqual(part["type"], "part")
        self.assertEqual(part["parent_name"], "dm-0")
        self.assertEqual(part["partuuid"], "AAAA-01")

    def test_paths_and_members_stay_held(self):
        inv = self._inventory()
        for name, holder in (("sda", "dm-0"), ("sdb", "dm-0"), ("sdc", "md0"), ("sdd", "md0")):
            self.assertEqual(inv[name]["holders"], [holder], name)

    def test_a_logical_volume_is_not_a_unit(self):
        inv = self._inventory()
        self.assertNotIn("dm-1", inv)


class TestEligibilityOfArraysAndMaps(unittest.TestCase):

    def _reasons(self, devs, **kwargs):
        _, rejected = utils.filter_eligible_block_devices(devs, **kwargs)
        return {d["name"]: r for d, r in rejected}

    def test_an_idle_array_and_map_are_eligible(self):
        devs = [_unit("md0", "raid1", serial=MD_UUID), _unit("dm-0", "mpath", serial=WWID)]
        selected, rejected = utils.filter_eligible_block_devices(devs)
        self.assertEqual(sorted(d["name"] for d in selected), ["dm-0", "md0"])
        self.assertEqual(rejected, [])

    def test_a_logical_or_encrypted_volume_is_not(self):
        reasons = self._reasons([_unit("dm-1", "lvm"), _unit("dm-3", "crypt")])
        for name in ("dm-1", "dm-3"):
            self.assertIn("not a disk, partition, RAID array, or multipath map", reasons[name])

    def test_members_and_paths_stay_held(self):
        reasons = self._reasons([_unit("sdc", holders=["md0"]), _unit("sda", holders=["dm-0"])])
        self.assertIn("held", reasons["sdc"])
        self.assertIn("held", reasons["sda"])

    def test_auto_selection_takes_plain_disks_ahead_of_arrays_and_maps(self):
        devs = [_unit("vdb"), _unit("md0", "raid1", serial=MD_UUID), _unit("dm-0", "mpath", serial=WWID)]
        selected, _ = utils.filter_eligible_block_devices(devs)
        self.assertEqual([d["name"] for d in selected], ["vdb"])

    def test_auto_selection_takes_arrays_and_maps_when_there_is_no_plain_disk(self):
        devs = [_unit("md0", "raid1", serial=MD_UUID), _unit("dm-0", "mpath", serial=WWID),
                _unit("sdc", holders=["md0"])]
        selected, _ = utils.filter_eligible_block_devices(devs)
        self.assertEqual(sorted(d["name"] for d in selected), ["dm-0", "md0"])

    def test_a_named_array_is_taken_beside_a_plain_disk(self):
        devs = [_unit("vdb"), _unit("md0", "raid1", serial=MD_UUID)]
        selected, _ = utils.filter_eligible_block_devices(
            devs, include_names=["/dev/disk/by-id/acme-md0", "/dev/disk/by-id/acme-vdb"])
        self.assertEqual(sorted(d["name"] for d in selected), ["md0", "vdb"])


class TestPersistentNamesOfArraysAndMaps(unittest.TestCase):

    def _dev(self, links):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root, True)
        for link, target in links.items():
            path = os.path.join(root, link)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            os.symlink(f"../../{target}", path)
        return root

    def test_the_superblock_uuid_outranks_the_arrays_name(self):
        dev = self._dev({
            "disk/by-id/md-name-worker-01:0": "md0",
            f"disk/by-id/md-uuid-{MD_UUID}": "md0",
        })
        links = node_utils.read_persistent_links(dev)
        self.assertEqual(links["md0"][0], os.path.join(dev, f"disk/by-id/md-uuid-{MD_UUID}"))

    def test_the_wwid_outranks_the_maps_name(self):
        dev = self._dev({
            "disk/by-id/dm-name-mpatha": "dm-0",
            f"disk/by-id/dm-uuid-mpath-{WWID}": "dm-0",
        })
        links = node_utils.read_persistent_links(dev)
        self.assertEqual(links["dm-0"][0], os.path.join(dev, f"disk/by-id/dm-uuid-mpath-{WWID}"))


class TestJournalLayoutOnArraysAndMaps(unittest.TestCase):

    def _partition(self, unit):
        commands = []

        def fake_run(cmd):
            commands.append(cmd)
            return "", "", 0

        with patch.object(node_utils, "get_block_devices_info", return_value=[unit]), \
                patch.object(node_utils.shell_utils, "run_command", side_effect=fake_run), \
                patch.object(node_utils, "_read_sysfs", return_value=""), \
                patch.object(node_utils, "simplyblock_data_signature", return_value=False), \
                patch("os.listdir", return_value=["md", "queue"]):
            try:
                node_utils.partition_disk_for_journal(unit["name"], GIB)
            except ValueError as e:
                return commands, str(e)
        return commands, ""

    def test_an_array_is_laid_out_like_a_disk(self):
        commands, error = self._partition(_unit("md0", "raid1", serial=MD_UUID))
        self.assertTrue(any(c.startswith("sgdisk -o /dev/md0") for c in commands),
                        f"sgdisk was not run on the array: {commands} {error}")

    def test_a_multipath_map_is_refused_with_a_reason(self):
        commands, error = self._partition(_unit("dm-0", "mpath", serial=WWID))
        self.assertEqual([c for c in commands if c.startswith("sgdisk")], [])
        self.assertIn("multipath map", error)
        self.assertIn("second", error)


if __name__ == "__main__":
    unittest.main()
