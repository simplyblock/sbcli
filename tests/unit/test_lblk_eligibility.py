# coding=utf-8
"""Unit tests for lblk-mode device eligibility, detection, identity and
node-config schema (pure helpers in simplyblock_core.utils).

Covered:
  - filter_eligible_block_devices: every rejection reason, all three
    selection methods (names / names-exclude / serials), hard errors on
    requested-but-ineligible devices and duplicate serials, force_format.
  - detect_lblk_devices: config-entry mapping + synthetic-serial warning.
  - aio_bdev_name_for_serial: stability, sanitization, collision-freedom.
  - resolve_lblk_entries: serial-first resolution (rename survival), stored
    name fallback, missing devices, field refresh.
  - node_config_device_count / node_config_min_sys_memory.
  - validate_node_config: exactly-one-device-source rule + lblk entry shape.
"""

import unittest
from unittest.mock import patch

from simplyblock_core import utils


def _blk(name, serial="", size=100 << 30, mounted=False, holders=None,
         root=False, ro=False, parts=False, dtype="disk", by_id="",
         numa=0, synthetic=False, model="MODEL-X", wwn=""):
    return {
        "name": name,
        "device_path": f"/dev/{name}",
        "type": dtype,
        "size": size,
        "serial": serial or f"SER-{name}",
        "serial_synthetic": synthetic,
        "wwn": wwn,
        "model": model,
        "vendor": "ACME",
        "rota": False,
        "ro": ro,
        "has_partitions": parts,
        "mounted_in_subtree": mounted,
        "holders": holders or [],
        "is_root_disk": root,
        "by_id_path": by_id,
        "numa_node": numa,
    }


class TestEligibility(unittest.TestCase):

    def _reasons(self, devs, **kwargs):
        _, rejected = utils.filter_eligible_block_devices(devs, **kwargs)
        return {d["name"]: r for d, r in rejected}

    def test_clean_disk_is_eligible(self):
        sel, rej = utils.filter_eligible_block_devices([_blk("sdb")])
        self.assertEqual([d["name"] for d in sel], ["sdb"])
        self.assertEqual(rej, [])

    def test_non_disk_non_part_type_rejected(self):
        reasons = self._reasons([_blk("dax0.0", dtype="lvm")])
        self.assertIn("not a disk or partition", reasons["dax0.0"])

    def test_idle_partition_is_eligible_when_requested(self):
        devs = [_blk("sdb1", dtype="part")]
        sel, _ = utils.filter_eligible_block_devices(devs, include_names=["sdb1"])
        self.assertEqual([d["name"] for d in sel], ["sdb1"])

    def test_mounted_partition_rejected(self):
        reasons = self._reasons([_blk("sdb1", dtype="part", mounted=True)])
        self.assertIn("busy", reasons["sdb1"])

    def test_partition_never_auto_selected(self):
        devs = [_blk("sdb"), _blk("sdc1", dtype="part")]
        sel, _ = utils.filter_eligible_block_devices(devs)
        self.assertEqual([d["name"] for d in sel], ["sdb"])

    def test_disk_and_own_partition_conflict(self):
        part = _blk("sdb1", dtype="part")
        part["parent_name"] = "sdb"
        devs = [_blk("sdb", parts=True), part]
        with self.assertRaises(ValueError) as ctx:
            utils.filter_eligible_block_devices(
                devs, include_names=["sdb", "sdb1"], force_format=True)
        self.assertIn("itself selected", str(ctx.exception))

    def test_special_prefixes_rejected(self):
        for name in ("ram0", "loop3", "sr0", "zram1", "nbd0", "md127", "dm-0", "drbd0", "fd0"):
            reasons = self._reasons([_blk(name, serial=f"S-{name}")])
            self.assertIn("special", reasons[name], name)

    def test_mounted_subtree_rejected(self):
        reasons = self._reasons([_blk("sdb", mounted=True)])
        self.assertIn("busy", reasons["sdb"])

    def test_holders_rejected(self):
        reasons = self._reasons([_blk("sdb", holders=["dm-0"])])
        self.assertIn("held by", reasons["sdb"])

    def test_root_disk_rejected(self):
        reasons = self._reasons([_blk("sda", root=True)])
        self.assertIn("root", reasons["sda"])

    def test_read_only_rejected(self):
        reasons = self._reasons([_blk("sdb", ro=True)])
        self.assertIn("read-only", reasons["sdb"])

    def test_zero_size_rejected(self):
        reasons = self._reasons([_blk("sdb", size=0)])
        self.assertIn("zero size", reasons["sdb"])

    def test_partitioned_rejected_without_force(self):
        reasons = self._reasons([_blk("sdb", parts=True)])
        self.assertIn("partitioned", reasons["sdb"])

    def test_partitioned_eligible_with_force(self):
        sel, _ = utils.filter_eligible_block_devices(
            [_blk("sdb", parts=True)], force_format=True)
        self.assertEqual([d["name"] for d in sel], ["sdb"])

    def test_nvme_kernel_devices_remain_eligible(self):
        # "arbitrary Linux block devices" includes kernel-driver NVMe disks
        sel, _ = utils.filter_eligible_block_devices([_blk("nvme0n1")])
        self.assertEqual([d["name"] for d in sel], ["nvme0n1"])

    # --- selection methods ------------------------------------------------

    def test_include_names_selects_only_requested(self):
        devs = [_blk("sdb"), _blk("sdc"), _blk("sdd")]
        sel, _ = utils.filter_eligible_block_devices(devs, include_names=["sdb", "sdd"])
        self.assertEqual(sorted(d["name"] for d in sel), ["sdb", "sdd"])

    def test_include_names_busy_device_is_hard_error(self):
        devs = [_blk("sdb", mounted=True)]
        with self.assertRaises(ValueError) as ctx:
            utils.filter_eligible_block_devices(devs, include_names=["sdb"])
        self.assertIn("busy", str(ctx.exception))

    def test_include_names_absent_device_is_hard_error(self):
        with self.assertRaises(ValueError) as ctx:
            utils.filter_eligible_block_devices([_blk("sdb")], include_names=["sdz"])
        self.assertIn("not present", str(ctx.exception))

    def test_exclude_names(self):
        devs = [_blk("sdb"), _blk("sdc")]
        sel, _ = utils.filter_eligible_block_devices(devs, exclude_names=["sdb"])
        self.assertEqual([d["name"] for d in sel], ["sdc"])

    def test_include_serials(self):
        devs = [_blk("sdb", serial="S1"), _blk("sdc", serial="S2")]
        sel, _ = utils.filter_eligible_block_devices(devs, include_serials=["S2"])
        self.assertEqual([d["name"] for d in sel], ["sdc"])

    def test_include_serials_missing_is_hard_error(self):
        with self.assertRaises(ValueError) as ctx:
            utils.filter_eligible_block_devices(
                [_blk("sdb", serial="S1")], include_serials=["S9"])
        self.assertIn("S9", str(ctx.exception))

    def test_duplicate_serials_hard_error(self):
        devs = [_blk("sdb", serial="DUP"), _blk("sdc", serial="DUP")]
        with self.assertRaises(ValueError) as ctx:
            utils.filter_eligible_block_devices(devs)
        self.assertIn("DUP", str(ctx.exception))

    def test_no_selection_takes_all_eligible(self):
        devs = [_blk("sdb"), _blk("sda", root=True, mounted=True), _blk("sdc")]
        sel, _ = utils.filter_eligible_block_devices(devs)
        self.assertEqual(sorted(d["name"] for d in sel), ["sdb", "sdc"])


class TestDetectLblkDevices(unittest.TestCase):

    def test_maps_config_entry_shape(self):
        devs = [_blk("sdb", serial="S1", by_id="/dev/disk/by-id/wwn-0x1",
                     size=42, numa=1),
                _blk("sdc", serial="S2", size=42, numa=0)]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devs):
            result = utils.detect_lblk_devices()
        self.assertEqual(result["sdb"], {
            "name": "sdb", "serial": "S1",
            "by_id": "/dev/disk/by-id/wwn-0x1", "size": 42, "numa": 1})

    def test_partition_config_entry_carries_identity(self):
        part = _blk("sdc1", dtype="part", serial="S2-part-uuid1", size=42, numa=0)
        part["partuuid"] = "uuid1"
        part["parent_serial"] = "S2"
        devs = [_blk("sdb", serial="S1", size=42), part]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devs):
            result = utils.detect_lblk_devices(include_names=["sdb", "sdc1"])
        self.assertEqual(result["sdc1"]["type"], "part")
        self.assertEqual(result["sdc1"]["partuuid"], "uuid1")
        self.assertEqual(result["sdc1"]["parent_serial"], "S2")

    def test_fewer_than_minimum_units_raises(self):
        devs = [_blk("sdb", serial="S1")]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devs):
            with self.assertRaises(ValueError) as ctx:
                utils.detect_lblk_devices()
        self.assertIn("at least 2", str(ctx.exception))

    def test_synthetic_serial_warns_but_passes(self):
        devs = [_blk("sdb", serial="SYN-abc123", synthetic=True),
                _blk("sdc", serial="S2")]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devs), \
                patch.object(utils, "logger") as mock_logger:
            result = utils.detect_lblk_devices()
        self.assertIn("sdb", result)
        self.assertTrue(mock_logger.warning.called)


class TestAioBdevName(unittest.TestCase):

    def test_plain_serial(self):
        self.assertEqual(utils.aio_bdev_name_for_serial("S3Z8NX0M600123"),
                         "aio_S3Z8NX0M600123")

    def test_stable(self):
        self.assertEqual(utils.aio_bdev_name_for_serial("ABC_1"),
                         utils.aio_bdev_name_for_serial("ABC_1"))

    def test_special_chars_never_collide(self):
        a = utils.aio_bdev_name_for_serial("S1:A")
        b = utils.aio_bdev_name_for_serial("S1;A")
        self.assertNotEqual(a, b)
        for name in (a, b):
            self.assertRegex(name, r"^aio_[A-Za-z0-9_]+$")

    def test_long_serial_truncated_with_hash(self):
        serial = "X" * 100
        name = utils.aio_bdev_name_for_serial(serial)
        self.assertLessEqual(len(name), len("aio_") + 40 + 7)
        self.assertNotEqual(name, utils.aio_bdev_name_for_serial("X" * 99))


class TestResolveLblkEntries(unittest.TestCase):

    CONFIGURED = [
        {"name": "sdb", "serial": "S1", "by_id": "/dev/disk/by-id/wwn-1",
         "size": 100, "numa": 0},
        {"name": "sdc", "serial": "S2", "by_id": "", "size": 200, "numa": 1},
    ]

    def test_serial_first_survives_rename(self):
        # After reboot S1 moved sdb->sdx; stored name must NOT win.
        host = [_blk("sdx", serial="S1", by_id="/dev/disk/by-id/wwn-1"),
                _blk("sdc", serial="S2")]
        resolved, missing = utils.resolve_lblk_entries(self.CONFIGURED, host)
        self.assertEqual(missing, [])
        by_serial = {e["serial"]: e for e in resolved}
        self.assertEqual(by_serial["S1"]["name"], "sdx")
        self.assertEqual(by_serial["S1"]["current_path"], "/dev/sdx")

    def test_name_fallback_when_serial_unknown(self):
        # Host reports a different serial for sdb (e.g. synthetic drift);
        # the stored name is the last-resort match.
        host = [_blk("sdb", serial="OTHER"), _blk("sdc", serial="S2")]
        resolved, missing = utils.resolve_lblk_entries(self.CONFIGURED, host)
        self.assertEqual(missing, [])
        names = {e["name"] for e in resolved}
        self.assertEqual(names, {"sdb", "sdc"})

    def test_missing_device_reported(self):
        host = [_blk("sdc", serial="S2")]
        resolved, missing = utils.resolve_lblk_entries(self.CONFIGURED, host)
        self.assertEqual(len(resolved), 1)
        self.assertEqual(missing[0]["serial"], "S1")

    def test_live_fields_refresh(self):
        host = [_blk("sdb", serial="S1", by_id="/dev/disk/by-id/wwn-NEW",
                     size=999, numa=1, parts=True),
                _blk("sdc", serial="S2")]
        resolved, _ = utils.resolve_lblk_entries(self.CONFIGURED, host)
        entry = next(e for e in resolved if e["serial"] == "S1")
        self.assertEqual(entry["by_id"], "/dev/disk/by-id/wwn-NEW")
        self.assertEqual(entry["size"], 999)
        self.assertEqual(entry["numa"], 1)
        self.assertTrue(entry["has_partitions"])


class TestNodeConfigHelpers(unittest.TestCase):

    def test_device_count_lblk(self):
        node = {"ssd_pcis": [], "lblk_devices": [{"name": "sdb"}, {"name": "sdc"}]}
        self.assertEqual(utils.node_config_device_count(node), 2)

    def test_device_count_nvme(self):
        node = {"ssd_pcis": ["0000:00:1e.0"], "lblk_devices": []}
        self.assertEqual(utils.node_config_device_count(node), 1)

    def test_device_count_missing_keys(self):
        self.assertEqual(utils.node_config_device_count({}), 0)

    def test_min_sys_memory_lblk_uses_capacity_factor(self):
        node = {"lblk_devices": [{"size": 50 << 30}, {"size": 50 << 30}]}
        expected = 2147483648 + int((100 << 30) * utils.SYS_MEMORY_STORAGE_FACTOR)
        self.assertEqual(utils.node_config_min_sys_memory(node), expected)
        # ~2.2 GiB total — NOT 2 GiB + full capacity (the bug the first AWS
        # lblk deploy hit: 102 GiB demanded on 32 GiB hosts).
        self.assertLess(utils.node_config_min_sys_memory(node), 3 << 30)

    def test_min_sys_memory_nvme_delegates(self):
        node = {"ssd_pcis": ["0000:00:1e.0"], "lblk_devices": []}
        with patch.object(utils, "calculate_minimum_sys_memory", return_value=7) as m:
            self.assertEqual(utils.node_config_min_sys_memory(node), 7)
        m.assert_called_once_with(["0000:00:1e.0"])


class TestValidateNodeConfig(unittest.TestCase):

    def _node(self, ssd_pcis=None, lblk_devices=None):
        return {
            "socket": 0,
            "cpu_mask": "0x3",
            "isolated": [0, 1],
            "l-cores": "0@0,1@1",
            "number_of_alcemls": 1,
            "distribution": {
                "app_thread_core": [0], "jm_cpu_core": [0],
                "poller_cpu_cores": [1], "alceml_cpu_cores": [1],
                "distrib_cpu_cores": [1], "jc_singleton_core": [0],
            },
            "ssd_pcis": ssd_pcis if ssd_pcis is not None else [],
            "lblk_devices": lblk_devices if lblk_devices is not None else [],
            "nic_ports": ["eth0"],
            "number_of_distribs": 2,
            "small_pool_count": 1,
            "large_pool_count": 1,
            "max_lvol": 10,
            "max_size": 1 << 30,
            "huge_page_memory": 1 << 30,
            "sys_memory": 1 << 31,
        }

    def test_valid_nvme_config(self):
        self.assertTrue(utils.validate_node_config(self._node(ssd_pcis=["0000:00:1e.0"])))

    def test_valid_lblk_config(self):
        node = self._node(lblk_devices=[{"name": "sdb", "serial": "S1", "size": 100},
                                        {"name": "sdc", "serial": "S2", "size": 100}])
        self.assertTrue(utils.validate_node_config(node))

    def test_single_lblk_entry_rejected(self):
        node = self._node(lblk_devices=[{"name": "sdb", "serial": "S1", "size": 100}])
        self.assertFalse(utils.validate_node_config(node))

    def test_two_journal_flags_rejected(self):
        node = self._node(lblk_devices=[
            {"name": "sdb1", "serial": "S1", "size": 100, "journal": True},
            {"name": "sdb2", "serial": "S2", "size": 100, "journal": True}])
        self.assertFalse(utils.validate_node_config(node))

    def test_one_journal_flag_valid(self):
        node = self._node(lblk_devices=[
            {"name": "sdb1", "serial": "S1", "size": 100, "journal": True},
            {"name": "sdb2", "serial": "S2", "size": 100}])
        self.assertTrue(utils.validate_node_config(node))

    def test_nvme_config_without_lblk_key_still_valid(self):
        node = self._node(ssd_pcis=["0000:00:1e.0"])
        del node["lblk_devices"]
        self.assertTrue(utils.validate_node_config(node))

    def test_both_sources_rejected(self):
        node = self._node(ssd_pcis=["0000:00:1e.0"],
                          lblk_devices=[{"name": "sdb", "serial": "S1", "size": 1}])
        self.assertFalse(utils.validate_node_config(node))

    def test_neither_source_rejected(self):
        self.assertFalse(utils.validate_node_config(self._node()))

    def test_lblk_entry_missing_serial_rejected(self):
        node = self._node(lblk_devices=[{"name": "sdb", "size": 100}])
        self.assertFalse(utils.validate_node_config(node))

    def test_lblk_entry_missing_name_rejected(self):
        node = self._node(lblk_devices=[{"serial": "S1", "size": 100}])
        self.assertFalse(utils.validate_node_config(node))

    def test_lblk_entry_bad_size_rejected(self):
        for size in (0, -5, "100", None):
            node = self._node(lblk_devices=[{"name": "sdb", "serial": "S1", "size": size}])
            self.assertFalse(utils.validate_node_config(node), f"size={size!r}")

    def test_lblk_entry_not_a_dict_rejected(self):
        node = self._node(lblk_devices=["sdb"])
        self.assertFalse(utils.validate_node_config(node))

    def test_invalid_pci_still_rejected(self):
        self.assertFalse(utils.validate_node_config(self._node(ssd_pcis=["/dev/sdb"])))


if __name__ == "__main__":
    unittest.main()
