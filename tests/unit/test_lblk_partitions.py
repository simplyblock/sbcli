"""Unit tests for lblk partition support.

Covered:
  - node_utils.get_block_devices_info: partitions emitted with derived
    identity (parent serial + PARTUUID), busy/root/holder detection,
    synthetic fallback without a PARTUUID.
  - utils.split_lblk_journal_partition: whole-disk selections unchanged,
    idempotency on a journal-flagged selection, smallest-partition choice,
    jm sizing (percent of total, floor, max-fraction cap), resulting entry
    shapes (journal flag, replacement of the split entry).
  - node_utils.split_partition_for_journal: preconditions (missing, mounted,
    held, non-GPT), sgdisk command sequence, sector math and alignment, new
    partition discovery.
  - utils.resolve_lblk_entries: PARTUUID fallback resolution, journal flag
    carry-over, and refusing a match (by any of serial/partuuid/name) that
    lands on a mounted, held, or root-disk live device.
  - storage_node_ops._find_flagged_journal_device.
  - storage_node_ops._reconcile_discovered_devices: restart-time device
    bookkeeping, including a live device's serial colliding with a
    previously operator-removed (admin_removed) record.
"""

import json
import unittest
from typing import ClassVar
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops, utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_web import node_utils

GIB = 1024 * 1024 * 1024


def _entry(name, serial=None, size=100 * GIB, dtype="disk", partuuid="",
           parent_serial="", journal=False):
    e = {"name": name, "serial": serial or f"SER-{name}", "by_id": "",
         "size": size, "numa": 0}
    if dtype == "part":
        e["type"] = "part"
        e["partuuid"] = partuuid
        e["parent_serial"] = parent_serial
    if journal:
        e["journal"] = True
    return e


def _inv(name, serial=None, size=100 * GIB, dtype="disk", partuuid="",
         parent_name="", parent_serial="", mounted=False, holders=None,
         root=False):
    return {
        "name": name, "device_path": f"/dev/{name}", "type": dtype,
        "size": size, "serial": serial or f"SER-{name}",
        "serial_synthetic": False, "partuuid": partuuid,
        "parent_name": parent_name, "parent_serial": parent_serial,
        "wwn": "", "model": "MODEL-X", "vendor": "ACME", "rota": False,
        "ro": False, "has_partitions": False, "mounted_in_subtree": mounted,
        "holders": holders or [], "is_root_disk": root, "by_id_path": "",
        "numa_node": 0,
    }


class TestInventoryPartitions(unittest.TestCase):
    LSBLK: ClassVar[dict] = {
        "blockdevices": [
            {"name": "nvme1n1", "type": "disk", "size": 200 * GIB,
             "serial": "VOL-A", "wwn": "", "mountpoint": None,
             "model": "EBS", "rota": False, "ro": False, "vendor": "AWS",
             "children": [
                 {"name": "nvme1n1p1", "type": "part", "size": 50 * GIB,
                  "partuuid": "AAAA-01", "mountpoint": None, "ro": False},
                 {"name": "nvme1n1p2", "type": "part", "size": 150 * GIB,
                  "partuuid": "AAAA-02", "mountpoint": "/data", "ro": False},
             ]},
            {"name": "nvme2n1", "type": "disk", "size": 100 * GIB,
             "serial": "", "wwn": "", "mountpoint": None,
             "model": "EBS", "rota": False, "ro": False, "vendor": "AWS",
             "children": [
                 {"name": "nvme2n1p1", "type": "part", "size": 100 * GIB,
                  "partuuid": "", "mountpoint": None, "ro": False},
             ]},
        ]
    }

    def _inventory(self):
        with patch.object(node_utils.shell_utils, "run_command",
                          return_value=(json.dumps(self.LSBLK), "", 0)), \
                patch.object(node_utils, "_root_disk_names", return_value=[]), \
                patch.object(node_utils, "_disk_holders", return_value=[]), \
                patch.object(node_utils, "_partition_holders", return_value=[]), \
                patch.object(node_utils, "read_persistent_links", return_value={}), \
                patch.object(node_utils, "_read_sysfs", return_value="0"):
            return {d["name"]: d for d in node_utils.get_block_devices_info()}

    def test_partitions_emitted_with_parent_identity(self):
        inv = self._inventory()
        self.assertIn("nvme1n1p1", inv)
        p1 = inv["nvme1n1p1"]
        self.assertEqual(p1["type"], "part")
        self.assertEqual(p1["serial"], "VOL-A-part-aaaa-01")
        self.assertEqual(p1["parent_name"], "nvme1n1")
        self.assertEqual(p1["parent_serial"], "VOL-A")
        self.assertEqual(p1["partuuid"], "AAAA-01")
        self.assertFalse(p1["serial_synthetic"])

    def test_mounted_partition_marked_busy_but_parent_subtree_too(self):
        inv = self._inventory()
        self.assertTrue(inv["nvme1n1p2"]["mounted_in_subtree"])
        self.assertFalse(inv["nvme1n1p1"]["mounted_in_subtree"])
        # the parent disk carries the subtree mount and its partitions
        self.assertTrue(inv["nvme1n1"]["mounted_in_subtree"])
        self.assertTrue(inv["nvme1n1"]["has_partitions"])

    def test_partition_without_partuuid_gets_synthetic_serial(self):
        inv = self._inventory()
        p = inv["nvme2n1p1"]
        self.assertTrue(p["serial"].startswith("SYN-"))
        self.assertTrue(p["serial_synthetic"])

    def test_disk_without_serial_still_parents_partition_identity(self):
        inv = self._inventory()
        # parent got a synthetic serial; the partition inherits it as parent_serial
        self.assertTrue(inv["nvme2n1"]["serial"].startswith("SYN-"))
        self.assertEqual(inv["nvme2n1p1"]["parent_serial"], inv["nvme2n1"]["serial"])


class TestSplitLblkJournalPartition(unittest.TestCase):

    def test_whole_disk_selection_unchanged(self):
        entries = {"sdb": _entry("sdb"), "sdc": _entry("sdc")}
        with patch.object(utils.node_utils, "split_partition_for_journal") as split:
            out = utils.split_lblk_journal_partition(entries)
        self.assertEqual(out, entries)
        split.assert_not_called()

    def test_idempotent_when_journal_already_flagged(self):
        entries = {
            "sdb1": _entry("sdb1", dtype="part", journal=True),
            "sdb2": _entry("sdb2", dtype="part"),
        }
        with patch.object(utils.node_utils, "split_partition_for_journal") as split:
            out = utils.split_lblk_journal_partition(entries)
        self.assertEqual(out, entries)
        split.assert_not_called()

    def test_smallest_partition_is_split(self):
        entries = {
            "p_big": _entry("p_big", dtype="part", size=100 * GIB, partuuid="B"),
            "p_small": _entry("p_small", dtype="part", size=50 * GIB, partuuid="S"),
        }
        jm_inv = _inv("p_small_jm", serial="S-jm", dtype="part", size=4 * GIB,
                      partuuid="NEW-JM", parent_serial="PAR")
        data_inv = _inv("p_small_data", serial="S-data", dtype="part",
                        size=46 * GIB, partuuid="NEW-DATA", parent_serial="PAR")
        with patch.object(utils.node_utils, "split_partition_for_journal",
                          return_value=(jm_inv, data_inv)) as split:
            out = utils.split_lblk_journal_partition(entries, jm_percent=3)
        split.assert_called_once()
        self.assertEqual(split.call_args[0][0], "p_small")
        # 3% of 150 GiB = 4.5 GiB > 2 GiB floor
        self.assertEqual(split.call_args[0][1], int(150 * GIB * 3 // 100))
        self.assertNotIn("p_small", out)
        self.assertIn("p_big", out)
        self.assertTrue(out["p_small_jm"]["journal"])
        self.assertEqual(out["p_small_jm"]["type"], "part")
        self.assertEqual(out["p_small_jm"]["partuuid"], "NEW-JM")
        self.assertNotIn("journal", out["p_small_data"])

    def test_jm_floor_applies_for_small_capacity(self):
        entries = {
            "p1": _entry("p1", dtype="part", size=10 * GIB),
            "p2": _entry("p2", dtype="part", size=10 * GIB),
        }
        jm_inv = _inv("p1_jm", dtype="part", size=2 * GIB)
        data_inv = _inv("p1_data", dtype="part", size=8 * GIB)
        with patch.object(utils.node_utils, "split_partition_for_journal",
                          return_value=(jm_inv, data_inv)) as split:
            utils.split_lblk_journal_partition(entries, jm_percent=3)
        # 3% of 20 GiB = 0.6 GiB < LBLK_JM_MIN_SIZE floor
        self.assertEqual(split.call_args[0][1], constants.LBLK_JM_MIN_SIZE)

    def test_journal_too_big_for_smallest_partition_raises(self):
        entries = {
            "p1": _entry("p1", dtype="part", size=3 * GIB),
            "p2": _entry("p2", dtype="part", size=500 * GIB),
        }
        with patch.object(utils.node_utils, "split_partition_for_journal"):
            with self.assertRaises(ValueError) as ctx:
                utils.split_lblk_journal_partition(entries, jm_percent=3)
        self.assertIn("provide a larger partition", str(ctx.exception))

    def test_mixed_selection_splits_partition_not_disk(self):
        entries = {
            "sdb": _entry("sdb", size=10 * GIB),  # smaller than the partition
            "p1": _entry("p1", dtype="part", size=100 * GIB),
        }
        jm_inv = _inv("p1_jm", dtype="part", size=3 * GIB)
        data_inv = _inv("p1_data", dtype="part", size=97 * GIB)
        with patch.object(utils.node_utils, "split_partition_for_journal",
                          return_value=(jm_inv, data_inv)) as split:
            out = utils.split_lblk_journal_partition(entries)
        # only partitions are split candidates, never the (smaller) whole disk
        self.assertEqual(split.call_args[0][0], "p1")
        self.assertIn("sdb", out)


class TestSplitPartitionForJournal(unittest.TestCase):
    """node_utils.split_partition_for_journal with mocked host state."""

    PARENT = "nvme1n1"
    PART = "nvme1n1p2"

    def _run(self, jm_bytes=2 * GIB, part_kwargs=None, pttype="gpt",
             start=2048, size_sectors=8 * GIB // 512, sgdisk_rc=0):
        part = _inv(self.PART, dtype="part", parent_name=self.PARENT,
                    **(part_kwargs or {}))
        # post-split inventory contains the two new partitions
        jm_sectors = -(-jm_bytes // 512 // 2048) * 2048
        data_start = start + jm_sectors
        new_jm = _inv(self.PART, dtype="part", partuuid="NEW-JM",
                      parent_name=self.PARENT, serial="PJM")
        new_data = _inv("nvme1n1p9", dtype="part", partuuid="NEW-DATA",
                        parent_name=self.PARENT, serial="PDATA")

        inventories = [[part], [new_jm, new_data]]

        def fake_inventory():
            return inventories.pop(0) if len(inventories) > 1 else inventories[0]

        sysfs = {
            f"/sys/block/{self.PARENT}/{self.PART}/partition": "2",
            f"/sys/block/{self.PARENT}/{self.PART}/start": str(start),
            f"/sys/block/{self.PARENT}/{self.PART}/size": str(size_sectors),
            f"/sys/block/{self.PARENT}/{self.PART}/start_after": "",
        }
        # after the split, sysfs lists the two new partitions
        post_split_children = {
            self.PART: str(start),
            "nvme1n1p9": str(data_start),
        }

        commands = []

        def fake_run(cmd):
            commands.append(cmd)
            # Regression: the storage-node init container has no /run/udev, so
            # lsblk, which reads PTTYPE from the udev database, prints nothing
            # for a GPT disk. Only a probe of the device itself reports it.
            if cmd.startswith("lsblk") and "PTTYPE" in cmd:
                return "", "", 0
            if cmd.startswith("blkid -p"):
                return pttype, "", 0
            if cmd.startswith("sgdisk"):
                return "", "", sgdisk_rc
            return "", "", 0

        def fake_sysfs(path):
            for child, st in post_split_children.items():
                if path.endswith(f"/{child}/start"):
                    return st
            return sysfs.get(path, "")

        with patch.object(node_utils, "get_block_devices_info",
                          side_effect=fake_inventory), \
                patch.object(node_utils.shell_utils, "run_command",
                             side_effect=fake_run), \
                patch.object(node_utils, "_read_sysfs", side_effect=fake_sysfs), \
                patch("os.listdir", return_value=[self.PART, "nvme1n1p9"]):
            result = node_utils.split_partition_for_journal(self.PART, jm_bytes)
        return result, commands

    def test_happy_path_commands_and_result(self):
        (jm, data), commands = self._run()
        self.assertEqual(jm["partuuid"], "NEW-JM")
        self.assertEqual(data["partuuid"], "NEW-DATA")
        sg = [c for c in commands if c.startswith("sgdisk")]
        self.assertEqual(len(sg), 3)
        self.assertIn(f"-d 2 /dev/{self.PARENT}", sg[0])
        # journal recreated at the original start with the original number
        self.assertIn("-n 2:2048:", sg[1])
        self.assertIn(node_utils.SB_GPT_PARTITION_TYPECODE, sg[1])
        # data partition takes the first free number, starts 1MiB-aligned
        jm_sectors = -(-2 * GIB // 512 // 2048) * 2048
        self.assertIn(f"-n 0:{2048 + jm_sectors}:", sg[2])

    def test_non_gpt_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(pttype="dos")
        self.assertIn("GPT", str(ctx.exception))

    def test_mounted_partition_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(part_kwargs={"mounted": True})
        self.assertIn("busy", str(ctx.exception))

    def test_held_partition_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(part_kwargs={"holders": ["dm-0"]})
        self.assertIn("held", str(ctx.exception))

    def test_partition_too_small_refused(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(jm_bytes=16 * GIB, size_sectors=8 * GIB // 512)
        self.assertIn("too small", str(ctx.exception))

    def test_sgdisk_failure_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._run(sgdisk_rc=1)
        self.assertIn("sgdisk", str(ctx.exception))

    def test_missing_partition_refused(self):
        with patch.object(node_utils, "get_block_devices_info", return_value=[]):
            with self.assertRaises(ValueError) as ctx:
                node_utils.split_partition_for_journal("nope1", GIB)
        self.assertIn("not found", str(ctx.exception))


class TestResolvePartitionEntries(unittest.TestCase):

    def test_partuuid_fallback_when_parent_serial_changed(self):
        # hypervisor re-exposed the volume: parent serial (and thus the
        # derived partition serial) changed, PARTUUID survived.
        configured = [_entry("sdb1", serial="OLD-part-aaaa", dtype="part",
                             partuuid="AAAA", parent_serial="OLD")]
        live = [_inv("sdz1", serial="NEW-part-aaaa", dtype="part",
                     partuuid="AAAA", parent_serial="NEW")]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["name"], "sdz1")
        self.assertEqual(resolved[0]["type"], "part")
        # The match survived via PARTUUID, but the reported identity must be
        # the device actually sitting there now, not the stale configured one.
        self.assertEqual(resolved[0]["serial"], "NEW-part-aaaa")

    def test_name_fallback_reports_live_serial_not_stale_one(self):
        # Node restarted on a brand-new host: same kernel device name
        # (nvme0n1), genuinely different physical hardware with a different
        # serial. Matching falls back to name, but the resolved identity
        # must reflect the NEW hardware's real serial -- not echo back the
        # old host's serial -- so the restart path correctly recognizes this
        # as unknown/new storage instead of treating it as already-initialized.
        configured = [_entry("nvme0n1", serial="OLD-921")]
        live = [_inv("nvme0n1", serial="NEW-871")]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["serial"], "NEW-871")

    def test_journal_flag_carried_through_resolution(self):
        configured = [_entry("sdb1", serial="S1", dtype="part",
                             partuuid="AAAA", journal=True)]
        live = [_inv("sdb1", serial="S1", dtype="part", partuuid="AAAA")]
        resolved, _ = utils.resolve_lblk_entries(configured, live)
        self.assertTrue(resolved[0]["journal"])

    def test_disk_entries_unaffected(self):
        configured = [_entry("sdb", serial="S1")]
        live = [_inv("sdb", serial="S1")]
        resolved, _ = utils.resolve_lblk_entries(configured, live)
        self.assertNotIn("type", resolved[0])
        self.assertNotIn("journal", resolved[0])

    def test_name_fallback_onto_root_disk_is_refused(self):
        # Node restarted on genuinely different hardware: the configured
        # slot's name ("sdb") happens to be the NEW host's root/boot disk.
        # Serial matching correctly fails (different hardware), and the name
        # fallback must not hand the live root disk to bdev_aio_create just
        # because something is attached at that kernel name now.
        configured = [_entry("sdb", serial="OLD-SER")]
        live = [_inv("sdb", serial="NEW-SER", mounted=True)]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(resolved, [])
        self.assertEqual(missing, configured)

    def test_name_fallback_onto_held_disk_is_refused(self):
        configured = [_entry("sdc", serial="OLD-SER")]
        live = [_inv("sdc", serial="NEW-SER", holders=["dm-0"])]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(resolved, [])
        self.assertEqual(missing, configured)

    def test_name_fallback_onto_root_disk_flag_is_refused(self):
        configured = [_entry("sdd", serial="OLD-SER")]
        live = [_inv("sdd", serial="NEW-SER", root=True)]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(resolved, [])
        self.assertEqual(missing, configured)

    def test_serial_match_onto_busy_disk_is_also_refused(self):
        # Safety applies regardless of how the match was made -- a serial
        # match is not exempt just because the identity lined up.
        configured = [_entry("sdb", serial="SAME-SER")]
        live = [_inv("sdb", serial="SAME-SER", mounted=True)]
        resolved, missing = utils.resolve_lblk_entries(configured, live)
        self.assertEqual(resolved, [])
        self.assertEqual(missing, configured)


class TestFindFlaggedJournalDevice(unittest.TestCase):

    def _snode(self, lblk_devices):
        n = StorageNode()
        n.uuid = "node-1"
        n.lblk_devices = lblk_devices
        return n

    def _dev(self, serial):
        d = MagicMock()
        d.serial_number = serial
        return d

    def test_flagged_entry_matched_by_serial(self):
        snode = self._snode([
            {"name": "p1", "serial": "S-JM", "journal": True},
            {"name": "p2", "serial": "S-DATA"},
        ])
        devs = [self._dev("S-DATA"), self._dev("S-JM")]
        found = storage_node_ops._find_flagged_journal_device(snode, devs)
        self.assertIs(found, devs[1])

    def test_no_flag_returns_none(self):
        snode = self._snode([{"name": "sdb", "serial": "S1"},
                             {"name": "sdc", "serial": "S2"}])
        devs = [self._dev("S1"), self._dev("S2")]
        self.assertIsNone(
            storage_node_ops._find_flagged_journal_device(snode, devs))

    def test_flag_without_matching_device_returns_none(self):
        snode = self._snode([{"name": "p1", "serial": "S-GONE", "journal": True}])
        devs = [self._dev("S-OTHER")]
        self.assertIsNone(
            storage_node_ops._find_flagged_journal_device(snode, devs))


class TestReconcileDiscoveredDevices(unittest.TestCase):
    """storage_node_ops._reconcile_discovered_devices: the restart-time
    bookkeeping that matches freshly discovered bdevs (from
    addNvmeDevices/addAioDevices) against this node's known NVMeDevice
    records."""

    def _snode(self, nvme_devices):
        n = StorageNode()
        n.uuid = "node-1"
        n.nvme_devices = nvme_devices
        n.jm_device = None
        return n

    def _db_dev(self, serial, status=NVMeDevice.STATUS_ONLINE,
               admin_removed=False, uid=None):
        d = NVMeDevice()
        d.uuid = uid or f"db-{serial}"
        d.serial_number = serial
        d.status = status
        d.admin_removed = admin_removed
        d.is_partition = False
        d.bdev_type = "aio"
        return d

    def _live_dev(self, serial, uid=None):
        d = NVMeDevice()
        d.uuid = uid or f"live-{serial}"
        d.serial_number = serial
        d.is_partition = False
        d.bdev_type = "aio"
        d.device_path = f"/dev/{serial}"
        d.by_id_path = ""
        return d

    def test_serial_colliding_with_admin_removed_device_becomes_new(self):
        # The regression this guards: an operator permanently removed a
        # device, leaving its serial in this node's history. A *different*
        # live device (e.g. a synthetic-serial collision after the node
        # restarted onto storage that looks topologically identical -- see
        # resolve_lblk_entries) that happens to share that exact serial
        # must not be silently dropped: neither merged into the removed
        # record, nor flagged new, nor logged -- which is what happened
        # before this fix.
        removed = self._db_dev("SYN-1", status=NVMeDevice.STATUS_REMOVED,
                               admin_removed=True, uid="removed-dev")
        snode = self._snode([removed])
        live = self._live_dev("SYN-1", uid="live-dev")

        new_devices = storage_node_ops._reconcile_discovered_devices(snode, [live])

        self.assertEqual([d.get_id() for d in new_devices], ["live-dev"])
        self.assertIn(live, snode.nvme_devices)
        self.assertEqual(live.status, NVMeDevice.STATUS_NEW)
        # The old, operator-removed record is untouched: not resurrected,
        # just no longer blocking the new device's identity.
        self.assertEqual(removed.status, NVMeDevice.STATUS_REMOVED)

    def test_serial_colliding_with_transiently_removed_device_is_not_new(self):
        # A device that is merely STATUS_REMOVED (not admin_removed -- e.g.
        # simply not found on an earlier restart) is still "known": its
        # serial must keep suppressing a spurious duplicate "new device"
        # entry if that identity resurfaces.
        removed = self._db_dev("SYN-2", status=NVMeDevice.STATUS_REMOVED,
                               admin_removed=False, uid="removed-dev-2")
        snode = self._snode([removed])
        live = self._live_dev("SYN-2", uid="live-dev-2")

        new_devices = storage_node_ops._reconcile_discovered_devices(snode, [live])

        self.assertEqual(new_devices, [])
        self.assertNotIn(live, snode.nvme_devices)

    def test_matching_active_device_is_updated_not_duplicated(self):
        active = self._db_dev("SYN-3", uid="active-dev")
        snode = self._snode([active])
        live = self._live_dev("SYN-3", uid="live-dev-3")
        live.nvme_bdev = "aio_SYN_3"

        new_devices = storage_node_ops._reconcile_discovered_devices(snode, [live])

        self.assertEqual(new_devices, [])
        self.assertEqual(active.nvme_bdev, "aio_SYN_3")
        self.assertEqual(len(snode.nvme_devices), 1)
