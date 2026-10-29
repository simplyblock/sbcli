"""Unit tests for the journal split of a whole lblk disk.

A node on a single whole disk (typically a RAID-protected LUN of a storage
array), or on whole disks with --lblk-journal-split, gets its journal from a
partition of the smallest disk instead of a dedicated journal device:

  - lblk_needs_journal_split: when the split applies.
  - split_lblk_journal_disk: target choice, journal sizing, entry flags.
  - node_utils.partition_disk_for_journal: preconditions and sgdisk layout.
  - node_utils.journal_split_partitions: recognising a previous split.
  - detect_lblk_devices: a re-configure reuses the split disk's partitions
    instead of refusing the partitioned disk.
  - generate_configs: the one-node-per-host guard.
"""

import unittest
from unittest.mock import patch

from simplyblock_core import constants, utils
from simplyblock_web import node_utils

GIB = 1024 ** 3


def _by_id(name):
    return f"/dev/disk/by-id/acme-{name}"


def _blk(name, serial="", size=100 * GIB, dtype="disk", parts=False, parent="",
         partuuid="", mounted=False, holders=None, root=False):
    dev = {
        "name": name,
        "device_path": f"/dev/{name}",
        "type": dtype,
        "size": size,
        "serial": serial or f"SER-{name}",
        "serial_synthetic": False,
        "wwn": "",
        "model": "LUN",
        "vendor": "ARRAY",
        "rota": False,
        "ro": False,
        "has_partitions": parts,
        "mounted_in_subtree": mounted,
        "holders": holders or [],
        "is_root_disk": root,
        "by_id_path": _by_id(name),
        "by_id_paths": [_by_id(name)],
        "numa_node": 0,
    }
    if dtype == "part":
        dev["parent_name"] = parent
        dev["partuuid"] = partuuid
        dev["parent_serial"] = f"SER-{parent}"
        dev["serial"] = f"SER-{parent}-part-{partuuid}"
    return dev


def _entry(name, size, **extra):
    entry = {"name": name, "serial": f"SER-{name}", "by_id": _by_id(name),
             "size": size, "numa": 0}
    entry.update(extra)
    return entry


class TestNeedsJournalSplit(unittest.TestCase):

    def test_single_whole_disk_is_split(self):
        self.assertTrue(utils.lblk_needs_journal_split({"sdb": _entry("sdb", GIB)}))

    def test_several_disks_only_when_asked(self):
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", GIB)}
        self.assertFalse(utils.lblk_needs_journal_split(entries))
        self.assertTrue(utils.lblk_needs_journal_split(entries, journal_split=True))

    def test_existing_journal_is_kept(self):
        entries = {"sdb1": _entry("sdb1", GIB, type="part", journal=True),
                   "sdb2": _entry("sdb2", GIB, type="part")}
        self.assertFalse(utils.lblk_needs_journal_split(entries, journal_split=True))

    def test_partition_selection_uses_partition_split(self):
        entries = {"sdc1": _entry("sdc1", GIB, type="part")}
        self.assertFalse(utils.lblk_needs_journal_split(entries, journal_split=True))

    def test_empty(self):
        self.assertFalse(utils.lblk_needs_journal_split({}, journal_split=True))


class TestSplitJournalDisk(unittest.TestCase):

    def _split(self, entries, jm_percent=3):
        calls = []

        def fake_partition(disk_name, jm_bytes):
            calls.append((disk_name, jm_bytes))
            jm = _blk(f"{disk_name}1", dtype="part", parent=disk_name, partuuid="JM",
                      size=jm_bytes)
            data = _blk(f"{disk_name}2", dtype="part", parent=disk_name, partuuid="DATA",
                        size=entries[disk_name]["size"] - jm_bytes)
            return jm, data

        with patch.object(utils.node_utils, "partition_disk_for_journal",
                          side_effect=fake_partition):
            result = utils.split_lblk_journal_disk(entries, jm_percent=jm_percent)
        return result, calls

    def test_single_disk_becomes_journal_and_data(self):
        result, calls = self._split({"sdb": _entry("sdb", 1000 * GIB)})
        self.assertEqual(calls, [("sdb", 30 * GIB)])
        self.assertEqual(sorted(result), ["sdb1", "sdb2"])
        self.assertTrue(result["sdb1"]["journal"])
        self.assertEqual(result["sdb1"]["type"], "part")
        self.assertEqual(result["sdb1"]["partuuid"], "JM")
        self.assertNotIn("journal", result["sdb2"])

    def test_smallest_disk_is_split_and_others_kept(self):
        result, calls = self._split({"sdb": _entry("sdb", 500 * GIB),
                                     "sdc": _entry("sdc", 400 * GIB)})
        self.assertEqual(calls[0][0], "sdc")
        self.assertEqual(calls[0][1], 27 * GIB)
        self.assertEqual(sorted(result), ["sdb", "sdc1", "sdc2"])

    def test_journal_floor(self):
        _, calls = self._split({"sdb": _entry("sdb", 10 * GIB)})
        self.assertEqual(calls[0][1], constants.LBLK_JM_MIN_SIZE)

    def test_journal_too_big_for_disk_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._split({"sdb": _entry("sdb", 3 * GIB)})
        self.assertIn("larger disk", str(ctx.exception))


class TestPartitionDiskForJournal(unittest.TestCase):
    DISK = "nvme1n1"

    def _run(self, disk_kwargs=None, sgdisk_rc=0, jm_bytes=2 * GIB, size=100 * GIB,
             listing=None):
        disk = _blk(self.DISK, size=size, **(disk_kwargs or {}))
        after = [disk,
                 _blk(f"{self.DISK}p1", dtype="part", parent=self.DISK, partuuid="J"),
                 _blk(f"{self.DISK}p2", dtype="part", parent=self.DISK, partuuid="D")]
        inventories = iter([[disk], after])
        commands = []

        def fake_run(cmd):
            commands.append(cmd)
            if cmd.startswith("sgdisk"):
                return "", "", sgdisk_rc
            return "", "", 0

        def fake_sysfs(path):
            if path.endswith(f"/{self.DISK}p1/partition"):
                return "1"
            if path.endswith(f"/{self.DISK}p2/partition"):
                return "2"
            return ""

        with patch.object(node_utils, "get_block_devices_info",
                          side_effect=lambda: next(inventories)), \
                patch.object(node_utils.shell_utils, "run_command", side_effect=fake_run), \
                patch.object(node_utils, "_read_sysfs", side_effect=fake_sysfs), \
                patch("os.listdir", return_value=listing if listing is not None
                      else ["queue", f"{self.DISK}p1", f"{self.DISK}p2"]):
            result = node_utils.partition_disk_for_journal(self.DISK, jm_bytes)
        return result, commands

    def test_layout_commands_and_result(self):
        (jm, data), commands = self._run()
        self.assertEqual(jm["name"], f"{self.DISK}p1")
        self.assertEqual(data["name"], f"{self.DISK}p2")
        sg = [c for c in commands if c.startswith("sgdisk")]
        self.assertEqual(sg[0], f"sgdisk -o /dev/{self.DISK}")
        self.assertIn("-n 1:0:+2048M", sg[1])
        self.assertIn("-c 1:sb_jm", sg[1])
        self.assertIn(node_utils.SB_GPT_PARTITION_TYPECODE, sg[1])
        self.assertIn("-n 2:0:0", sg[2])
        self.assertIn("-c 2:sb_data", sg[2])

    def test_journal_size_rounds_up_to_mib(self):
        _, commands = self._run(jm_bytes=2 * GIB + 1)
        self.assertTrue(any("-n 1:0:+2049M" in c for c in commands))

    def test_partitioned_disk_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(disk_kwargs={"parts": True})
        self.assertIn("already carries partitions", str(ctx.exception))

    def test_busy_disk_refused(self):
        for kwargs, text in (({"mounted": True}, "mounted"),
                             ({"holders": ["dm-0"]}, "held"),
                             ({"root": True}, "root")):
            with self.assertRaises(ValueError) as ctx:
                self._run(disk_kwargs=kwargs)
            self.assertIn(text, str(ctx.exception))

    def test_disk_too_small_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(jm_bytes=2 * GIB, size=2 * GIB)
        self.assertIn("too small", str(ctx.exception))

    def test_sgdisk_failure_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(sgdisk_rc=1)
        self.assertIn("sgdisk", str(ctx.exception))

    def test_partitions_not_found_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(listing=["queue"])
        self.assertIn("not found", str(ctx.exception))

    def test_missing_disk_refused(self):
        with patch.object(node_utils, "get_block_devices_info", return_value=[]):
            with self.assertRaises(ValueError):
                node_utils.partition_disk_for_journal("sdz", GIB)


def _blkid(entries):
    """A run_command fake answering blkid -p for the given partitions."""
    def fake_run(cmd):
        for name, (label, ptype) in entries.items():
            if cmd == f"blkid -p -o export /dev/{name}":
                return f"DEVNAME=/dev/{name}\nPART_ENTRY_NAME={label}\nPART_ENTRY_TYPE={ptype}\n", "", 0
        return "", "", 2
    return fake_run


SB = node_utils.SB_GPT_PARTITION_TYPECODE


class TestJournalSplitPartitions(unittest.TestCase):

    def _devices(self, extra=()):
        return [_blk("sdb", parts=True),
                _blk("sdb1", dtype="part", parent="sdb", partuuid="J"),
                _blk("sdb2", dtype="part", parent="sdb", partuuid="D"), *extra]

    def test_recognised(self):
        with patch.object(node_utils.shell_utils, "run_command",
                          side_effect=_blkid({"sdb1": ("sb_jm", SB), "sdb2": ("sb_data", SB)})):
            pair = node_utils.journal_split_partitions("sdb", self._devices())
        self.assertEqual((pair[0]["name"], pair[1]["name"]), ("sdb1", "sdb2"))

    def test_foreign_type_not_recognised(self):
        with patch.object(node_utils.shell_utils, "run_command",
                          side_effect=_blkid({"sdb1": ("sb_jm", "0fc63daf-8483-4772-8e79-3d69d8477de4"),
                                              "sdb2": ("sb_data", SB)})):
            self.assertIsNone(node_utils.journal_split_partitions("sdb", self._devices()))

    def test_third_partition_not_recognised(self):
        devices = self._devices([_blk("sdb3", dtype="part", parent="sdb", partuuid="X")])
        with patch.object(node_utils.shell_utils, "run_command",
                          side_effect=_blkid({"sdb1": ("sb_jm", SB), "sdb2": ("sb_data", SB)})):
            self.assertIsNone(node_utils.journal_split_partitions("sdb", devices))

    def test_wrong_labels_not_recognised(self):
        with patch.object(node_utils.shell_utils, "run_command",
                          side_effect=_blkid({"sdb1": ("sb_data", SB), "sdb2": ("sb_data", SB)})):
            self.assertIsNone(node_utils.journal_split_partitions("sdb", self._devices()))


class TestDetectReusesSplitDisk(unittest.TestCase):
    """A configure runs again on every storage-node pod start; the disk it
    split the first time must be found again, not refused as partitioned."""

    def _devices(self):
        return [_blk("sdb", parts=True),
                _blk("sdb1", dtype="part", parent="sdb", partuuid="J", size=3 * GIB),
                _blk("sdb2", dtype="part", parent="sdb", partuuid="D", size=97 * GIB),
                _blk("sdc")]

    def _detect(self, **kwargs):
        def fake_split(disk_name, devices):
            if disk_name != "sdb":
                return None
            by_name = {d["name"]: d for d in devices}
            return by_name["sdb1"], by_name["sdb2"]

        with patch.object(utils.node_utils, "get_block_devices_info",
                          return_value=self._devices()), \
                patch.object(utils.node_utils, "journal_split_partitions",
                             side_effect=fake_split):
            return utils.detect_lblk_devices(**kwargs)

    def test_by_name(self):
        result = self._detect(include_names=[_by_id("sdb")], min_units=1)
        self.assertEqual(sorted(result), ["sdb1", "sdb2"])
        self.assertTrue(result["sdb1"]["journal"])
        self.assertNotIn("journal", result["sdb2"])
        self.assertEqual(result["sdb1"]["partuuid"], "J")

    def test_by_name_with_another_disk(self):
        result = self._detect(include_names=[_by_id("sdb"), _by_id("sdc")])
        self.assertEqual(sorted(result), ["sdb1", "sdb2", "sdc"])

    def test_by_serial(self):
        result = self._detect(include_serials=["SER-sdb"], min_units=1)
        self.assertEqual(sorted(result), ["sdb1", "sdb2"])

    def test_auto_selection(self):
        result = self._detect()
        self.assertEqual(sorted(result), ["sdb1", "sdb2", "sdc"])

    def test_auto_selection_with_force_does_not_take_the_disk_again(self):
        result = self._detect(force_format=True)
        self.assertNotIn("sdb", result)
        self.assertEqual(sorted(result), ["sdb1", "sdb2", "sdc"])

    def test_excluded(self):
        result = self._detect(exclude_names=[_by_id("sdb")], min_units=1)
        self.assertEqual(sorted(result), ["sdc"])

    def test_not_selected(self):
        result = self._detect(include_names=[_by_id("sdc")], min_units=1)
        self.assertEqual(sorted(result), ["sdc"])


class TestGenerateConfigsGuard(unittest.TestCase):

    def _run(self, nodes_per_socket, sockets=(0,)):
        selection = {"names": [_by_id("sdb")], "names_exclude": None,
                     "serials": None, "journal_split": False}
        split_calls = []

        def fake_split(entries, jm_percent=3):
            split_calls.append(sorted(entries))
            raise ValueError("stop here")

        with patch.object(utils, "get_numa_cores", return_value={0: list(range(16))}), \
                patch.object(utils, "validate_sockets"), \
                patch.object(utils, "detect_nics", return_value={}), \
                patch.object(utils, "detect_lblk_devices",
                             return_value={"sdb": _entry("sdb", 100 * GIB)}), \
                patch.object(utils, "split_lblk_journal_disk", side_effect=fake_split):
            result = utils.generate_configs(0, 0, list(sockets), nodes_per_socket, [], [],
                                            lblk_selection=selection)
        return result, split_calls

    def test_two_nodes_per_host_refused(self):
        result, calls = self._run(nodes_per_socket=2)
        self.assertEqual(result, (False, False))
        self.assertEqual(calls, [])

    def test_one_node_per_host_splits(self):
        result, calls = self._run(nodes_per_socket=1)
        self.assertEqual(result, (False, False))
        self.assertEqual(calls, [["sdb"]])


class TestSplitDiskThroughRestartAndAddNode(unittest.TestCase):
    """The entries a split writes are what add-node and every restart work
    from: they resolve against the live partitions and the journal partition
    is the node's journal device, not the smallest unit."""

    def test_chain(self):
        from unittest.mock import MagicMock

        from simplyblock_core import storage_node_ops
        from simplyblock_core.models.storage_node import StorageNode

        jm = _blk("sdb1", dtype="part", parent="sdb", partuuid="J", size=30 * GIB)
        data = _blk("sdb2", dtype="part", parent="sdb", partuuid="D", size=970 * GIB)
        with patch.object(utils.node_utils, "partition_disk_for_journal",
                          return_value=(jm, data)):
            entries = utils.split_lblk_journal_disk({"sdb": _entry("sdb", 1000 * GIB)})

        # After a reboot the kernel names moved; serial and PARTUUID did not.
        live = [dict(jm, name="sdc1", device_path="/dev/sdc1"),
                dict(data, name="sdc2", device_path="/dev/sdc2")]
        resolved, missing = utils.resolve_lblk_entries(list(entries.values()), live)
        self.assertEqual(missing, [])
        self.assertEqual(sorted(e["name"] for e in resolved), ["sdc1", "sdc2"])

        snode = StorageNode()
        snode.uuid = "node-1"
        snode.lblk_devices = list(entries.values())
        devs = []
        for e in resolved:
            d = MagicMock()
            d.serial_number = e["serial"]
            d.size = e["size"]
            devs.append(d)
        found = storage_node_ops._find_flagged_journal_device(snode, devs)
        self.assertEqual(found.serial_number, jm["serial"])

        node = {"socket": 0, "lblk_devices": list(entries.values())}
        self.assertEqual(len(node["lblk_devices"]), constants.LBLK_MIN_DEVICES_PER_NODE)


if __name__ == "__main__":
    unittest.main()
