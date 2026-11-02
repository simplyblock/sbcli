"""Unit tests for an lblk device selected by a stable path, taken as given.

The operator identifies a device itself and sends sn configure the stable path
it settled on. That path is the device's identity in the deployment: it is what
the node config records, what add-node and every restart look the device up by,
and what the AIO bdev is opened on. Nothing re-derives an identity of its own
from the device behind it.

Re-deriving one is what this replaces. Matching the stored serial first and the
stored kernel name last finds a device the caller never named whenever the two
disagree with the path: a synthetic serial drifts with the kernel name it was
seeded from, and a kernel name is a position in one boot's enumeration order.
A path that no longer answers is a missing device, not an invitation to guess.

Covered:
  - detect_lblk_devices records the path as the caller spelled it, including
    when it is not the link the inventory ranks best.
  - resolve_lblk_entries matches a path-carrying entry by that path alone: not
    by serial, not by kernel name, and an absent path is a missing device.
  - addAioDevices opens the AIO bdev on the path.
  - An entry without a path (auto-selection, --blk-serials) is unaffected.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import utils

DATA_BY_ID = "/dev/disk/by-id/wwn-0x5000c500a1b2c3d4"
DATA_BY_ID_ALT = "/dev/disk/by-id/scsi-35000c500a1b2c3d4"
JOURNAL_BY_ID = "/dev/disk/by-id/wwn-0x5000c500deadbeef"


def _blk(name, serial, paths=(), size=100 << 30, synthetic=False):
    """One device of the inventory get_block_devices_info produces."""
    return {
        "name": name,
        "device_path": f"/dev/{name}",
        "type": "disk",
        "size": size,
        "serial": serial,
        "serial_synthetic": synthetic,
        "wwn": "",
        "model": "MODEL-X",
        "vendor": "ACME",
        "rota": False,
        "ro": False,
        "has_partitions": False,
        "mounted_in_subtree": False,
        "holders": [],
        "is_root_disk": False,
        "by_id_path": paths[0] if paths else "",
        "by_id_paths": list(paths),
        "numa_node": 0,
    }


def _entry(name, serial, path=None, by_id=""):
    """One lblk_devices entry as the node config stores it."""
    entry = {"name": name, "serial": serial, "by_id": by_id, "size": 100 << 30, "numa": 0}
    if path is not None:
        entry["path"] = path
    return entry


class TestConfigureRecordsTheGivenPath(unittest.TestCase):

    def test_the_entry_carries_the_path_as_given(self):
        devices = [
            _blk("sdb", "S-DATA", paths=[DATA_BY_ID]),
            _blk("sdc", "S-JM", paths=[JOURNAL_BY_ID]),
        ]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices(include_names=[DATA_BY_ID, JOURNAL_BY_ID])

        self.assertEqual(entries["sdb"]["path"], DATA_BY_ID)
        self.assertEqual(entries["sdc"]["path"], JOURNAL_BY_ID)

    def test_a_path_that_is_not_the_preferred_link_is_recorded_as_given(self):
        # The inventory ranks the wwn- link first; the caller chose the scsi- one.
        devices = [
            _blk("sdb", "S-DATA", paths=[DATA_BY_ID, DATA_BY_ID_ALT]),
            _blk("sdc", "S-JM", paths=[JOURNAL_BY_ID]),
        ]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices(include_names=[DATA_BY_ID_ALT, JOURNAL_BY_ID])

        self.assertEqual(entries["sdb"]["path"], DATA_BY_ID_ALT)

    def test_an_auto_selected_device_carries_no_path(self):
        devices = [
            _blk("sdb", "S-DATA", paths=[DATA_BY_ID]),
            _blk("sdc", "S-JM", paths=[JOURNAL_BY_ID]),
        ]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices()

        self.assertNotIn("path", entries["sdb"])

    def test_a_device_selected_by_serial_carries_no_path_and_resolves_by_serial(self):
        devices = [
            _blk("sdb", "S-DATA", paths=[DATA_BY_ID]),
            _blk("sdc", "S-JM", paths=[JOURNAL_BY_ID]),
        ]
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices(include_serials=["S-DATA", "S-JM"])
        self.assertNotIn("path", entries["sdb"])

        # After a reboot the kernel calls it sdx and udev published no link.
        host = [_blk("sdx", "S-DATA"), _blk("sdc", "S-JM", paths=[JOURNAL_BY_ID])]
        resolved, missing = utils.resolve_lblk_entries(list(entries.values()), host)

        self.assertEqual(missing, [])
        self.assertEqual({e["name"] for e in resolved}, {"sdx", "sdc"})


class TestAPathIsNotReResolved(unittest.TestCase):

    def test_the_device_answering_to_the_path_is_taken_whatever_its_serial(self):
        # A synthetic serial is seeded from the kernel name and drifts with it;
        # the path still names the device.
        configured = [_entry("sdb", "SYN-old", path=DATA_BY_ID)]
        host = [_blk("sdx", "SYN-new", paths=[DATA_BY_ID], synthetic=True)]

        resolved, missing = utils.resolve_lblk_entries(configured, host)

        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["name"], "sdx")
        self.assertEqual(resolved[0]["path"], DATA_BY_ID)

    def test_a_device_matching_only_the_stored_serial_is_not_taken(self):
        configured = [_entry("sdb", "S-DATA", path=DATA_BY_ID)]
        host = [
            _blk("sdb", "S-OTHER", paths=[DATA_BY_ID]),
            _blk("sdc", "S-DATA", paths=["/dev/disk/by-id/wwn-0x1"]),
        ]

        resolved, missing = utils.resolve_lblk_entries(configured, host)

        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["name"], "sdb")

    def test_an_absent_path_is_a_missing_device_even_when_serial_and_name_match(self):
        configured = [_entry("sdb", "S-DATA", path=DATA_BY_ID)]
        host = [_blk("sdb", "S-DATA", paths=["/dev/disk/by-id/wwn-0x1"])]

        resolved, missing = utils.resolve_lblk_entries(configured, host)

        self.assertEqual(resolved, [])
        self.assertEqual(missing, configured)

    def test_an_entry_without_a_path_still_resolves_serial_first(self):
        configured = [_entry("sdb", "S-DATA", by_id=DATA_BY_ID)]
        host = [_blk("sdx", "S-DATA", paths=[DATA_BY_ID])]

        resolved, missing = utils.resolve_lblk_entries(configured, host)

        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["name"], "sdx")
        self.assertNotIn("path", resolved[0])


class TestTheAioBdevIsOpenedOnThePath(unittest.TestCase):

    def _rpc(self):
        rpc = MagicMock()
        rpc.bdev_get.side_effect = [None, {"block_size": 4096, "num_blocks": 1 << 20}]
        rpc.bdev_aio_create.return_value = True
        return rpc

    def _snode(self):
        snode = MagicMock()
        snode.physical_label = 0
        snode.get_id.return_value = "node-1"
        snode.cluster_id = "cluster-1"
        return snode

    def test_the_path_is_the_filename_and_the_recorded_by_id_path(self):
        configured = [_entry("sdb", "S-DATA", path=DATA_BY_ID_ALT, by_id=DATA_BY_ID)]
        host = [_blk("sdb", "S-DATA", paths=[DATA_BY_ID, DATA_BY_ID_ALT])]
        resolved, _ = utils.resolve_lblk_entries(configured, host)
        rpc = self._rpc()

        devices = utils.addAioDevices(rpc, self._snode(), resolved)

        rpc.bdev_aio_create.assert_called_once_with(
            utils.aio_bdev_name_for_serial("S-DATA"), DATA_BY_ID_ALT)
        self.assertEqual(devices[0].by_id_path, DATA_BY_ID_ALT)


if __name__ == "__main__":
    unittest.main()
