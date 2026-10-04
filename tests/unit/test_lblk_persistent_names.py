"""Unit tests for selecting an lblk device by a persistent /dev/disk name.

A kernel name is a position in one boot's enumeration order, not an identity.
On the lab workers this was written against, the disk the kernel calls sdb is
the one the hypervisor calls drive-scsi0, and sda is drive-scsi2; a host that
probes its controllers in another order hands each kernel name to another disk.
So the operator sends the persistent name udev published, and the selection has
to resolve it.

A kernel name is therefore refused rather than resolved. sn_config_file is
discarded and written whole on every configure, so nothing carries a resolution
forward: the selector is the device's only durable identity and it is read again
at every restart. Accepting a kernel name would mean the identity of a disk in a
deployment is whichever disk the kernel enumerated into that position this boot.

Covered:
  - filter_eligible_block_devices: a selector is matched against every
    persistent /dev/disk link the device answers to, and a kernel name or
    kernel path is a hard error that says what to use instead.
  - Both selection channels that take names: include and exclude.
  - The hard error a requested-but-absent persistent name produces, spelled the
    way the caller spelled it.
  - detect_lblk_devices: the persistent name is what the node config records.
"""

import os
import tempfile
import unittest
from unittest.mock import patch

from simplyblock_core import utils
from simplyblock_web import node_utils

# The two spellings of one disk on a lab worker: the kernel found it second and
# called it sdb, and the hypervisor calls it drive-scsi0.
SDB_BY_ID = "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi0"
SDB_BY_PATH = "/dev/disk/by-path/pci-0000:09:01.0-scsi-0:0:0:0"

# The disk the kernel called first, which the hypervisor calls drive-scsi2.
SDA_BY_ID = "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi2"

# A partition, whose persistent name is the identifier in the partition table.
SDB4_BY_PARTUUID = "/dev/disk/by-partuuid/28427de0-1916-4c05-895f-0829cd8790ba"
SDB4_BY_ID = "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi0-part4"


def _blk(name, paths=(), preferred="", size=100 << 30, dtype="disk", **kwargs):
    """One device of the inventory get_block_devices_info produces."""
    device = {
        "name": name,
        "device_path": f"/dev/{name}",
        "type": dtype,
        "size": size,
        "serial": f"SER-{name}",
        "serial_synthetic": False,
        "wwn": "",
        "model": "QEMU HARDDISK",
        "vendor": "QEMU",
        "rota": False,
        "ro": False,
        "has_partitions": False,
        "mounted_in_subtree": False,
        "holders": [],
        "is_root_disk": False,
        "by_id_path": preferred or (paths[0] if paths else ""),
        "by_id_paths": list(paths),
        "numa_node": 0,
    }
    device.update(kwargs)
    return device


class TestSelectionByPersistentName(unittest.TestCase):

    def _fleet(self):
        return [
            _blk("sda", paths=[SDA_BY_ID]),
            _blk("sdb", paths=[SDB_BY_ID]),
        ]

    def test_a_persistent_name_selects_its_device(self):
        selected, _ = utils.filter_eligible_block_devices(
            self._fleet(), include_names=[SDB_BY_ID])
        self.assertEqual([d["name"] for d in selected], ["sdb"])

    def test_a_persistent_name_is_not_read_as_a_kernel_name(self):
        # The failure this replaces: the last element of the link was sent as
        # though it were a kernel name, matched nothing, and failed the add
        # with the device reported absent.
        selected, _ = utils.filter_eligible_block_devices(
            self._fleet(), include_names=[SDA_BY_ID])
        self.assertEqual([d["name"] for d in selected], ["sda"])

    def test_a_partition_is_selected_by_its_partition_table_identifier(self):
        devices = [
            _blk("sdb", paths=[SDB_BY_ID], has_partitions=True),
            _blk("sdb4", paths=[SDB4_BY_PARTUUID, SDB4_BY_ID], dtype="part",
                 parent_name="sdb", partuuid="28427de0-1916-4c05-895f-0829cd8790ba"),
        ]
        selected, _ = utils.filter_eligible_block_devices(
            devices, include_names=[SDB4_BY_PARTUUID])
        self.assertEqual([d["name"] for d in selected], ["sdb4"])

    def test_any_link_the_device_answers_to_matches(self):
        # The operator and the node each pick their own preferred link, and
        # they need not pick the same one: matching against the whole set is
        # what keeps a selection from depending on two rankings agreeing.
        devices = [_blk("sdb", paths=[SDB_BY_ID, SDB_BY_PATH], preferred=SDB_BY_ID)]
        selected, _ = utils.filter_eligible_block_devices(
            devices, include_names=[SDB_BY_PATH])
        self.assertEqual([d["name"] for d in selected], ["sdb"])

    def test_a_bare_kernel_name_is_refused(self):
        # sn_config_file is discarded and regenerated on every configure, so the
        # selector is the only durable identity a device has in a deployment and
        # it is resolved again at every node restart. A kernel name there is
        # resolved against whatever order the controllers came up in this time.
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(self._fleet(), include_names=["sdb"])
        self.assertIn("sdb", str(caught.exception))

    def test_a_kernel_path_is_refused(self):
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(self._fleet(), include_names=["/dev/sdb"])
        self.assertIn("/dev/sdb", str(caught.exception))

    def test_the_refusal_names_what_the_device_does_answer_to(self):
        # A refusal nobody can act on is a refusal that costs a trip to
        # `ls -l /dev/disk/by-id`, so it carries the names to use instead.
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(self._fleet(), include_names=["sdb"])
        self.assertIn(SDB_BY_ID, str(caught.exception))

    def test_a_kernel_name_in_the_exclude_list_is_refused(self):
        # The exclude list is the worse half. A name that fails to match does
        # not fail loudly: it stops excluding, and the disk somebody named to
        # protect is taken and formatted.
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(self._fleet(), exclude_names=["sda"])
        self.assertIn("sda", str(caught.exception))

    def test_a_device_with_no_persistent_name_cannot_be_selected_by_one(self):
        # Nothing to fall back on: a device udev published no link for has no
        # identity to record, and the refusal says which channel does carry one.
        devices = [_blk("sdb", paths=[])]
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(devices, include_names=["sdb"])
        self.assertIn("--blk-serials", str(caught.exception))

    def test_an_absent_persistent_name_is_a_hard_error_spelled_as_given(self):
        missing = "/dev/disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi9"
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(
                self._fleet(), include_names=[missing])
        # The whole name, so the message names what the caller asked for rather
        # than a fragment of it that identifies nothing.
        self.assertIn(missing, str(caught.exception))

    def test_a_persistent_name_excludes_its_device(self):
        selected, _ = utils.filter_eligible_block_devices(
            self._fleet(), exclude_names=[SDB_BY_ID])
        self.assertEqual([d["name"] for d in selected], ["sda"])

    def test_a_busy_device_requested_by_persistent_name_is_a_hard_error(self):
        devices = [
            _blk("sda", paths=[SDA_BY_ID]),
            _blk("sdb", paths=[SDB_BY_ID], mounted_in_subtree=True),
        ]
        with self.assertRaises(ValueError) as caught:
            utils.filter_eligible_block_devices(devices, include_names=[SDB_BY_ID])
        self.assertIn("mounted", str(caught.exception))


class TestTheNodeConfigRecordsThePersistentName(unittest.TestCase):

    def test_the_entry_carries_the_persistent_name(self):
        devices = [
            _blk("sda", paths=[SDA_BY_ID]),
            _blk("sdb", paths=[SDB_BY_ID]),
        ]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices(include_names=[SDA_BY_ID, SDB_BY_ID])

        self.assertEqual(entries["sda"]["by_id"], SDA_BY_ID)
        self.assertEqual(entries["sdb"]["by_id"], SDB_BY_ID)


class TestReadingThePersistentLinks(unittest.TestCase):
    """The /dev/disk tree the inventory takes a device's persistent names from.

    The trees below are the ones the lab workers actually publish, with every
    alternative udev created beside the preferred link: neither host has a
    single wwn- link, and every device carries a by-path link naming the slot
    it is plugged into.
    """

    def _dev(self, links):
        """A /dev with the udev link directories, link path -> kernel name."""
        root = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, root, True)
        for link, target in links.items():
            path = os.path.join(root, link)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            os.symlink(f"../../{target}", path)
        return root

    def test_a_partition_prefers_its_partition_table_identifier(self):
        dev = self._dev({
            "disk/by-partuuid/28427de0-1916-4c05-895f-0829cd8790ba": "sdb4",
            "disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi0-part4": "sdb4",
            "disk/by-path/pci-0000:09:01.0-scsi-0:0:0:0-part4": "sdb4",
        })
        links = node_utils.read_persistent_links(dev)
        self.assertEqual(
            links["sdb4"][0],
            os.path.join(dev, "disk/by-partuuid/28427de0-1916-4c05-895f-0829cd8790ba"))

    def test_a_namespace_prefers_the_identifier_it_reports(self):
        # The two _ha links are the controller's account of the namespaces and
        # differ only by an index counting them in the order they were found.
        dev = self._dev({
            "disk/by-id/nvme-uuid.8a695f15-8227-4b90-a0d5-66b9e72b496f": "nvme8n1",
            "disk/by-id/nvme-8a695f15-8227-4b90-a0d5-66b9e72b496f_ha": "nvme8n1",
            "disk/by-id/nvme-8a695f15-8227-4b90-a0d5-66b9e72b496f_ha_1": "nvme8n1",
        })
        links = node_utils.read_persistent_links(dev)
        self.assertEqual(
            links["nvme8n1"][0],
            os.path.join(dev, "disk/by-id/nvme-uuid.8a695f15-8227-4b90-a0d5-66b9e72b496f"))

    def test_the_slot_is_never_a_persistent_name(self):
        # by-path survives a reboot and moves to the replacement when a disk is
        # swapped, which is the one failure a persistent name exists to
        # prevent, arriving under a name that looks like it prevents it.
        dev = self._dev({"disk/by-path/pci-0000:09:03.0-scsi-0:0:0:2": "sda"})
        self.assertEqual(node_utils.read_persistent_links(dev), {})

    def test_the_filesystems_own_names_are_never_persistent_names(self):
        # Both are written by mkfs and both move to whatever disk an image is
        # restored onto, so they name the content rather than the device.
        dev = self._dev({
            "disk/by-uuid/0a1b2c3d-4e5f-6071-8293-a4b5c6d7e8f9": "sda",
            "disk/by-label/data": "sda",
        })
        self.assertEqual(node_utils.read_persistent_links(dev), {})

    def test_every_link_a_device_answers_to_is_reported(self):
        dev = self._dev({
            "disk/by-id/scsi-0QEMU_QEMU_HARDDISK_drive-scsi2": "sda",
            "disk/by-id/wwn-0x5000c500a1b2c3d4": "sda",
        })
        self.assertEqual(len(node_utils.read_persistent_links(dev)["sda"]), 2)

    def test_a_host_with_no_link_directory_is_not_a_failure(self):
        self.assertEqual(node_utils.read_persistent_links(tempfile.mkdtemp()), {})


if __name__ == "__main__":
    unittest.main()
