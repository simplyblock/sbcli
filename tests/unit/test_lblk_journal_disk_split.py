"""Unit tests for the lblk journal split.

By default every lblk storage node gets its journal from a partition of its
own devices (its smallest whole disk is partitioned, or its smallest
partition is split) instead of a dedicated journal device;
--lblk-journal-device keeps the dedicated device for whole disks:

  - lblk_node_journal_layout: the default, the opt-out, and the safety
    rules that never partition a disk of an existing node.
  - previous_lblk_dedicated_journal_serials: the existing layout read from
    the node config file before a configure discards it.
  - split_lblk_journal_disk: target choice, journal sizing, entry flags.
  - node_utils.partition_disk_for_journal: preconditions and sgdisk layout.
  - node_utils.journal_split_partitions: recognising a previous split.
  - detect_lblk_devices: a re-configure reuses the split disk's partitions
    instead of refusing the partitioned disk, in the disk's position.
  - generate_configs: the split per node after the devices are dealt.
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


class TestNodeJournalLayout(unittest.TestCase):
    """lblk_node_journal_layout: the default split and every case that keeps
    a dedicated journal device or refuses."""

    def _layout(self, entries, signatures=None, **kwargs):
        signatures = signatures or {}
        disk_calls, part_calls = [], []

        def fake_disk(entries_, jm_percent=3):
            disk_calls.append(sorted(entries_))
            return {"split": True}

        def fake_part(entries_, jm_percent=3):
            part_calls.append(sorted(entries_))
            return {"split-part": True}

        with patch.object(utils, "split_lblk_journal_disk", side_effect=fake_disk), \
                patch.object(utils, "split_lblk_journal_partition", side_effect=fake_part), \
                patch.object(utils.node_utils, "simplyblock_data_signature",
                             side_effect=lambda n: signatures.get(n, False)):
            result = utils.lblk_node_journal_layout(entries, **kwargs)
        return result, disk_calls, part_calls

    def test_single_whole_disk_is_split(self):
        _, disk_calls, _ = self._layout({"sdb": _entry("sdb", GIB)})
        self.assertEqual(disk_calls, [["sdb"]])

    def test_several_whole_disks_are_split_by_default(self):
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", GIB)}
        _, disk_calls, _ = self._layout(entries)
        self.assertEqual(disk_calls, [["sdb", "sdc"]])

    def test_journal_device_opt_out(self):
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", GIB)}
        result, disk_calls, _ = self._layout(entries, journal_device=True)
        self.assertEqual(result, entries)
        self.assertEqual(disk_calls, [])

    def test_existing_journal_is_kept(self):
        entries = {"sdb1": _entry("sdb1", GIB, type="part", journal=True),
                   "sdb2": _entry("sdb2", GIB, type="part")}
        result, disk_calls, part_calls = self._layout(entries)
        self.assertEqual(result, entries)
        self.assertEqual((disk_calls, part_calls), ([], []))

    def test_existing_journal_with_new_disk_is_kept(self):
        entries = {"sdb1": _entry("sdb1", GIB, type="part", journal=True),
                   "sdb2": _entry("sdb2", GIB, type="part"),
                   "sdc": _entry("sdc", GIB)}
        result, disk_calls, _ = self._layout(entries)
        self.assertEqual(result, entries)
        self.assertEqual(disk_calls, [])

    def test_partitions_are_split_even_with_opt_out(self):
        entries = {"sdb1": _entry("sdb1", GIB, type="part"),
                   "sdb2": _entry("sdb2", GIB, type="part")}
        _, disk_calls, part_calls = self._layout(entries, journal_device=True)
        self.assertEqual(part_calls, [["sdb1", "sdb2"]])
        self.assertEqual(disk_calls, [])

    def test_empty(self):
        self.assertEqual(self._layout({})[0], {})

    # Safety: the upgrade of an existing node and every pod restart.

    def test_disk_with_simplyblock_data_is_never_partitioned(self):
        # An existing node with a dedicated journal device: after add-node
        # every disk starts with the alceml header, invisible to blkid.
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", 2 * GIB)}
        result, disk_calls, _ = self._layout(entries, signatures={"sdc": True})
        self.assertEqual(result, entries)
        self.assertEqual(disk_calls, [])

    def test_unreadable_disk_is_never_partitioned(self):
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", GIB)}
        result, disk_calls, _ = self._layout(entries, signatures={"sdb": None})
        self.assertEqual(result, entries)
        self.assertEqual(disk_calls, [])

    def test_disk_of_an_existing_dedicated_layout_is_kept(self):
        # Configured before the default changed, never written yet (or the
        # signature was not found): the previous config still decides.
        entries = {"sdb": _entry("sdb", GIB), "sdc": _entry("sdc", GIB)}
        result, disk_calls, _ = self._layout(entries, keep_serials={"SER-sdc"})
        self.assertEqual(result, entries)
        self.assertEqual(disk_calls, [])

    def test_partition_with_data_and_no_journal_refused(self):
        entries = {"sdb1": _entry("sdb1", GIB, type="part"),
                   "sdb2": _entry("sdb2", GIB, type="part")}
        with self.assertRaises(ValueError) as ctx:
            self._layout(entries, signatures={"sdb2": True})
        self.assertIn("not split", str(ctx.exception))


class TestPreviousDedicatedJournalSerials(unittest.TestCase):

    def _write(self, tmp, name, config):
        import json
        import os
        path = os.path.join(tmp, name)
        with open(path, "w") as handle:
            json.dump(config, handle)
        return path

    def test_dedicated_layout_nodes_only(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp, "sn_config_file", {"nodes": [
                {"lblk_devices": [_entry("sdb", GIB), _entry("sdc", GIB)]},
                {"lblk_devices": [_entry("sdd1", GIB, type="part", journal=True),
                                  _entry("sdd2", GIB, type="part"), _entry("sde", GIB)]},
                {"ssd_pcis": ["0000:01:00.0"]},
            ]})
            serials = utils.previous_lblk_dedicated_journal_serials([path, path + "_missing"])
        self.assertEqual(serials, {"SER-sdb", "SER-sdc"})

    def test_missing_or_broken_files(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            import os
            broken = os.path.join(tmp, "broken")
            with open(broken, "w") as handle:
                handle.write("{not json")
            self.assertEqual(
                utils.previous_lblk_dedicated_journal_serials([broken, os.path.join(tmp, "none")]),
                set())


class TestGenerateConfigsPerNode(unittest.TestCase):
    """The journal is given per node after the devices are dealt, so two
    nodes on one host each get their own journal on their own devices."""

    def _run(self, entries, nodes_per_socket=1, selection=None, signatures=None):
        selection = dict({"names": None, "names_exclude": None, "serials": None},
                         **(selection or {}))
        signatures = signatures or {}
        split_calls = []

        def fake_partition(disk_name, jm_bytes):
            split_calls.append(disk_name)
            jm = _blk(f"{disk_name}1", dtype="part", parent=disk_name,
                      partuuid=f"J-{disk_name}", size=jm_bytes)
            data = _blk(f"{disk_name}2", dtype="part", parent=disk_name,
                        partuuid=f"D-{disk_name}", size=entries[disk_name]["size"] - jm_bytes)
            return jm, data

        cores = {"cpu_mask": "0xff", "isolated": [1, 2, 3, 4, 5, 6], "core_to_index": {},
                 "distribution": [[]] * 9}
        with patch.object(utils, "get_numa_cores", return_value={0: list(range(32))}), \
                patch.object(utils, "validate_sockets"), \
                patch.object(utils, "detect_nics", return_value={}), \
                patch.object(utils, "detect_lblk_devices", return_value=entries), \
                patch.object(utils, "generate_core_allocation",
                             return_value={0: [dict(cores) for _ in range(nodes_per_socket)]}), \
                patch.object(utils, "get_core_indexes", return_value=[]), \
                patch.object(utils, "generate_l_cores", return_value=""), \
                patch.object(utils, "calculate_pool_count", return_value=(1, 1)), \
                patch.object(utils, "calculate_minimum_hp_memory", return_value=0), \
                patch.object(utils, "node_config_min_sys_memory", return_value=0), \
                patch.object(utils.node_utils, "get_memory_details",
                             return_value={"free": 1 << 40, "huge_total": 0}), \
                patch.object(utils, "regenerate_config", side_effect=lambda a, b, c: a), \
                patch.object(utils, "previous_lblk_dedicated_journal_serials",
                             return_value=set()), \
                patch.object(utils.node_utils, "simplyblock_data_signature",
                             side_effect=lambda n: signatures.get(n, False)), \
                patch.object(utils.node_utils, "partition_disk_for_journal",
                             side_effect=fake_partition):
            config, _ = utils.generate_configs(0, 0, [0], nodes_per_socket, [], [],
                                               lblk_selection=selection)
        return config, split_calls

    @staticmethod
    def _names(config):
        return [[e["name"] for e in n["lblk_devices"]] for n in config["nodes"]]

    def test_single_disk_one_node(self):
        config, calls = self._run({"sdb": _entry("sdb", 100 * GIB)})
        self.assertEqual(calls, ["sdb"])
        self.assertEqual(self._names(config), [["sdb1", "sdb2"]])
        self.assertTrue(config["nodes"][0]["lblk_devices"][0]["journal"])

    def test_several_disks_one_node_split_by_default(self):
        config, calls = self._run({"sdb": _entry("sdb", 100 * GIB),
                                   "sdc": _entry("sdc", 50 * GIB)})
        self.assertEqual(calls, ["sdc"])
        self.assertEqual(self._names(config), [["sdb", "sdc1", "sdc2"]])

    def test_two_nodes_per_host_each_split_their_own_disk(self):
        entries = {n: _entry(n, s * GIB) for n, s in
                   (("sdb", 100), ("sdc", 200), ("sdd", 300), ("sde", 400))}
        config, calls = self._run(entries, nodes_per_socket=2)
        self.assertEqual(sorted(calls), ["sdb", "sdc"])
        self.assertEqual(self._names(config), [["sdb1", "sdb2", "sdd"], ["sdc1", "sdc2", "sde"]])
        for node in config["nodes"]:
            self.assertEqual(sum(1 for e in node["lblk_devices"] if e.get("journal")), 1)

    def test_journal_device_opt_out(self):
        config, calls = self._run({"sdb": _entry("sdb", 100 * GIB),
                                   "sdc": _entry("sdc", 50 * GIB)},
                                  selection={"journal_device": True})
        self.assertEqual(calls, [])
        self.assertEqual(self._names(config), [["sdb", "sdc"]])

    def test_opt_out_with_a_single_disk_refused(self):
        config, calls = self._run({"sdb": _entry("sdb", 100 * GIB)},
                                  selection={"journal_device": True})
        self.assertEqual(config, False)
        self.assertEqual(calls, [])

    def test_reconfigure_keeps_split_pair_on_one_node(self):
        # Second configure of a two-node host: each split disk comes back as
        # its journal and data partition and must stay with its node.
        entries = {
            "sdb1": _entry("sdb1", 3 * GIB, type="part", journal=True, _pair="sdb2"),
            "sdb2": _entry("sdb2", 97 * GIB, type="part"),
            "sdc1": _entry("sdc1", 6 * GIB, type="part", journal=True, _pair="sdc2"),
            "sdc2": _entry("sdc2", 194 * GIB, type="part"),
            "sdd": _entry("sdd", 300 * GIB),
            "sde": _entry("sde", 400 * GIB),
        }
        config, calls = self._run(entries, nodes_per_socket=2,
                                  signatures={"sdd": True, "sde": True})
        self.assertEqual(calls, [])
        self.assertEqual(self._names(config), [["sdb1", "sdb2", "sdd"], ["sdc1", "sdc2", "sde"]])
        for node in config["nodes"]:
            self.assertFalse(any("_pair" in e for e in node["lblk_devices"]))

    def test_upgrade_of_existing_dedicated_layout_is_untouched(self):
        # A node deployed before the default changed: whole disks, journal on
        # the smallest one, data written. The configure after the upgrade
        # (pod restart) must write the same layout and partition nothing.
        entries = {"sdb": _entry("sdb", 100 * GIB), "sdc": _entry("sdc", 50 * GIB)}
        config, calls = self._run(entries, signatures={"sdb": True, "sdc": True})
        self.assertEqual(calls, [])
        self.assertEqual(self._names(config), [["sdb", "sdc"]])
        self.assertFalse(any(e.get("journal") for e in config["nodes"][0]["lblk_devices"]))

    def test_upgrade_uses_previous_config_when_selection_passes_it(self):
        entries = {"sdb": _entry("sdb", 100 * GIB), "sdc": _entry("sdc", 50 * GIB)}
        config, calls = self._run(entries, selection={"keep_serials": ["SER-sdb"]})
        self.assertEqual(calls, [])
        self.assertEqual(self._names(config), [["sdb", "sdc"]])

    def test_single_disk_with_data_refused(self):
        config, calls = self._run({"sdb": _entry("sdb", 100 * GIB)}, signatures={"sdb": True})
        self.assertEqual(config, False)
        self.assertEqual(calls, [])


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
             listing=None, signature=False):
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
                patch.object(node_utils, "simplyblock_data_signature", return_value=signature), \
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

    def test_disk_with_simplyblock_data_refused(self):
        for signature in (True, None):
            with self.assertRaises(ValueError) as ctx:
                self._run(signature=signature)
            self.assertIn("simplyblock data", str(ctx.exception))

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

    def test_pair_in_the_position_of_the_disk_and_marked(self):
        # The order decides which node a device is dealt to; the split disk
        # must come back where the whole disk was, its partitions together.
        result = self._detect()
        self.assertEqual(list(result), ["sdb1", "sdb2", "sdc"])
        self.assertEqual(result["sdb1"]["_pair"], "sdb2")
        self.assertEqual(utils.lblk_device_units(result), {"sdb1": ["sdb1", "sdb2"], "sdc": ["sdc"]})


class TestDetectReusesSplitPartition(unittest.TestCase):
    """A partition split by an earlier configure is now the journal partition
    (same number and PARTUUID); its data partition joins it."""

    def _devices(self):
        return [_blk("sda", parts=True),
                _blk("sda3", dtype="part", parent="sda", partuuid="ORIG", size=3 * GIB),
                _blk("sda5", dtype="part", parent="sda", partuuid="DATA", size=97 * GIB),
                _blk("sda4", dtype="part", parent="sda", partuuid="OTHER", size=50 * GIB)]

    def _detect(self, names, partner=None):
        by_name = {d["name"]: d for d in self._devices()}

        def fake_partner(part, devices):
            if partner is not None:
                return partner(part, devices)
            if part["name"] in ("sda3", "sda5"):
                return by_name["sda3"], by_name["sda5"]
            return None

        with patch.object(utils.node_utils, "get_block_devices_info",
                          return_value=self._devices()), \
                patch.object(utils.node_utils, "journal_split_partitions", return_value=None), \
                patch.object(utils.node_utils, "split_partition_partner",
                             side_effect=fake_partner):
            return utils.detect_lblk_devices(include_names=names, min_units=1)

    def test_original_partition_selected(self):
        result = self._detect([_by_id("sda3"), _by_id("sda4")])
        self.assertEqual(list(result), ["sda3", "sda5", "sda4"])
        self.assertTrue(result["sda3"]["journal"])
        self.assertEqual(result["sda3"]["_pair"], "sda5")
        self.assertNotIn("journal", result["sda5"])

    def test_both_partitions_selected_once(self):
        result = self._detect([_by_id("sda3"), _by_id("sda5")])
        self.assertEqual(list(result), ["sda3", "sda5"])

    def test_orphaned_simplyblock_partition_refused(self):
        def orphan(part, devices):
            raise ValueError("partner was not found")
        with self.assertRaises(ValueError):
            self._detect([_by_id("sda3")], partner=orphan)


_EXTENTS = {"sda3": (2048, 4096), "sda4": (100000, 1000), "sda5": (6144, 8192)}


class TestSplitPartitionPartner(unittest.TestCase):

    def _devices(self):
        return [_blk("sda", parts=True),
                _blk("sda3", dtype="part", parent="sda", partuuid="ORIG"),
                _blk("sda4", dtype="part", parent="sda", partuuid="OTHER"),
                _blk("sda5", dtype="part", parent="sda", partuuid="DATA")]

    def _partner(self, name, labels, extents):
        devices = self._devices()

        def fake_sysfs(path):
            for part, (start, size) in extents.items():
                if path == f"/sys/block/sda/{part}/start":
                    return str(start)
                if path == f"/sys/block/sda/{part}/size":
                    return str(size)
            return ""

        with patch.object(node_utils.shell_utils, "run_command", side_effect=_blkid(labels)), \
                patch.object(node_utils, "_read_sysfs", side_effect=fake_sysfs):
            part = next(d for d in devices if d["name"] == name)
            return node_utils.split_partition_partner(part, devices)

    def test_from_journal(self):
        jm, data = self._partner("sda3", {"sda3": ("sb_jm", SB), "sda5": ("sb_data", SB),
                                          "sda4": ("x", "y")}, _EXTENTS)
        self.assertEqual((jm["name"], data["name"]), ("sda3", "sda5"))

    def test_from_data(self):
        jm, data = self._partner("sda5", {"sda3": ("sb_jm", SB), "sda5": ("sb_data", SB)},
                                 _EXTENTS)
        self.assertEqual((jm["name"], data["name"]), ("sda3", "sda5"))

    def test_ordinary_partition(self):
        self.assertIsNone(self._partner("sda4", {"sda4": ("data", "0fc63daf")}, _EXTENTS))

    def test_journal_without_adjacent_data_refused(self):
        extents = dict(_EXTENTS, sda5=(20000, 8192))
        with self.assertRaises(ValueError) as ctx:
            self._partner("sda3", {"sda3": ("sb_jm", SB), "sda5": ("sb_data", SB)}, extents)
        self.assertIn("not split again", str(ctx.exception))


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


class TestNodeConfigureKeepsExistingLayout(unittest.TestCase):
    """The k8s configure discards the node config file before it generates a
    new one; the dedicated-journal disks of the existing node are read before
    the discard and handed to the generation."""

    def test_keep_serials_read_before_discard(self):
        import argparse
        import json
        import os
        import tempfile

        from simplyblock_web import node_configure

        args = argparse.Namespace(
            upgrade=False, max_lvol="30", max_prov="0", nodes_per_socket="1",
            sockets_to_use="0", pci_allowed="", pci_blocked="", nvme_names="",
            device_model="", size_range="", force=False, lblk=True,
            blk_names="", blk_names_exclude="", blk_serials="",
            lblk_journal_device=False, jm_percent="3", force_format=False)
        with tempfile.TemporaryDirectory() as tmp:
            config = os.path.join(tmp, "sn_config_file")
            with open(config, "w") as handle:
                json.dump({"nodes": [{"lblk_devices": [_entry("sdb", GIB), _entry("sdc", GIB)]}]},
                          handle)
            with patch.object(node_configure.constants, "NODES_CONFIG_FILE", config),                     patch.object(node_configure, "parse_arguments", return_value=args),                     patch.object(node_configure, "validate_arguments"),                     patch.object(node_configure, "_is_pod_present_for_node", return_value=False),                     patch.object(node_configure, "generate_automated_deployment_config",
                                 return_value=True) as generated:
                node_configure.main()
            self.assertFalse(os.path.exists(config))
        selection = generated.call_args.kwargs["lblk_selection"]
        self.assertEqual(selection["keep_serials"], ["SER-sdb", "SER-sdc"])
        self.assertFalse(selection["journal_device"])


if __name__ == "__main__":
    unittest.main()
