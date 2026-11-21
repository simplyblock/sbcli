"""Node agent, SAN host side: the initiator report (how an array knows the
host) and the scan for newly presented LUNs."""
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from simplyblock_web import node_utils


class _Root(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        real_read, real_isdir, real_listdir = node_utils._read_sysfs, os.path.isdir, os.listdir

        def _map(p):
            return os.path.join(self.root, p.lstrip("/")) if p.startswith("/") else p
        for target, fn in ((node_utils, "_read_sysfs"),):
            p = patch.object(target, fn, side_effect=lambda path: real_read(_map(path)))
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(node_utils.os.path, "isdir", side_effect=lambda path: real_isdir(_map(path)))
        p.start()
        self.addCleanup(p.stop)
        p = patch.object(node_utils.os, "listdir", side_effect=lambda path: real_listdir(_map(path)))
        p.start()
        self.addCleanup(p.stop)
        self.map = _map

    def mk(self, rel, content=""):
        path = os.path.join(self.root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            fh.write(content)


class TestInitiators(_Root):

    def test_reports_iqn_wwpns_and_hostnqn(self):
        self.mk("etc/iscsi/initiatorname.iscsi",
                "## generated\n#InitiatorName=iqn.ignored\nInitiatorName=iqn.1994-05.com.redhat:abc\n")
        self.mk("sys/class/fc_host/host3/port_name", "0x21000024FF4C1A2B\n")
        self.mk("sys/class/fc_host/host4/port_name", "0x0000000000000000\n")   # unconfigured port
        self.mk("etc/nvme/hostnqn", "nqn.2014-08.org.nvmexpress:uuid:1234\n")
        with patch.object(node_utils.os.path, "exists", return_value=False):
            out = node_utils.get_san_initiators()
        self.assertEqual(out, {
            "iscsi_iqn": "iqn.1994-05.com.redhat:abc",
            "fc_wwpns": ["21000024ff4c1a2b"],
            "nvme_hostnqn": "nqn.2014-08.org.nvmexpress:uuid:1234",
        })

    def test_kubernetes_agent_reads_the_hosts_etc(self):
        self.mk("host/etc/nvme/hostnqn", "nqn.host")
        self.mk("etc/nvme/hostnqn", "nqn.container")
        with patch.object(node_utils.os.path, "exists", return_value=False):
            self.assertEqual(node_utils.get_san_initiators()["nvme_hostnqn"], "nqn.host")

    def test_powerflex_sdc_guid(self):
        with patch.object(node_utils.os.path, "exists",
                          side_effect=lambda p: p == "/opt/emc/scaleio/sdc/bin/drv_cfg"), \
                patch.object(node_utils.shell_utils, "run_command",
                             return_value=("INFO: DRV_CFG: Query GUID: 0E9B4C8A-1234-4ABC-9DEF-00112233AABB",
                                           "", 0)):
            self.assertEqual(node_utils.get_san_initiators()["sdc_guid"],
                             "0E9B4C8A-1234-4ABC-9DEF-00112233AABB")

    def test_nothing_configured(self):
        with patch.object(node_utils.os.path, "exists", return_value=False):
            self.assertEqual(node_utils.get_san_initiators(), {})


class TestScan(_Root):

    def test_scans_scsi_hosts_and_nvme_controllers(self):
        written = {}
        paths = ["/sys/class/scsi_host/host0/scan", "/sys/class/scsi_host/host7/scan",
                 "/sys/class/nvme/nvme2/rescan_controller"]
        with patch("glob.glob", side_effect=lambda pat: [p for p in paths if p.startswith(pat.split("*")[0])]), \
                patch.object(node_utils, "_write_sysfs", side_effect=lambda p, v: written.__setitem__(p, v)):
            out = node_utils.scan_for_new_luns()
        self.assertEqual(out["scanned"], paths)
        self.assertEqual(written["/sys/class/scsi_host/host7/scan"], "- - -")
        self.assertEqual(written["/sys/class/nvme/nvme2/rescan_controller"], "1")

    def test_a_failing_host_does_not_stop_the_scan(self):
        paths = ["/sys/class/scsi_host/host0/scan", "/sys/class/scsi_host/host1/scan"]

        def write(p, v):
            if p.endswith("host0/scan"):
                raise OSError("busy")
        with patch("glob.glob", side_effect=lambda pat: [p for p in paths if "scsi_host" in pat]), \
                patch.object(node_utils, "_write_sysfs", side_effect=write):
            self.assertEqual(node_utils.scan_for_new_luns()["scanned"], paths[1:])


# A SCSI LUN reached over several paths (FC or iSCSI with dm-multipath) is
# one multipath map: the inventory reports the map, with the identity of its
# paths, and a LUN lookup returns the map instead of the paths below it.
_MPATH_LSBLK = {"blockdevices": [
    {"name": "sda", "kname": "sda", "type": "disk", "size": 100 << 30, "serial": "LOCAL1", "wwn": None},
    {"name": "sdb", "kname": "sdb", "type": "disk", "size": 4 << 40, "serial": "624a9370aa", "wwn": "0x624a9370aa",
     "children": [{"name": "mpatha", "kname": "dm-3", "type": "mpath", "size": 4 << 40}]},
    {"name": "sdc", "kname": "sdc", "type": "disk", "size": 4 << 40, "serial": "624a9370aa", "wwn": "0x624a9370aa",
     "children": [{"name": "mpatha", "kname": "dm-3", "type": "mpath", "size": 4 << 40}]},
    {"name": "sdd", "kname": "sdd", "type": "disk", "size": 2 << 40, "serial": "624a9370bb", "wwn": "0x624a9370bb"},
]}


class TestMultipathInventory(unittest.TestCase):

    def setUp(self):
        import json as _json
        patches = [
            patch.object(node_utils.shell_utils, "run_command",
                         return_value=(_json.dumps(_MPATH_LSBLK), "", 0)),
            patch.object(node_utils, "_root_disk_names", return_value={"sda"}),
            patch.object(node_utils.host_network, "node_hostname", return_value="h1"),
            patch.object(node_utils, "read_persistent_links", return_value={
                "dm-3": ["/dev/disk/by-id/dm-uuid-mpath-3624a9370aa", "/dev/disk/by-id/dm-name-mpatha"],
                "sdd": ["/dev/disk/by-id/wwn-0x624a9370bb"]}),
            patch.object(node_utils, "_read_sysfs", return_value=""),
            patch.object(node_utils, "_logical_block_size", return_value=512),
            patch.object(node_utils, "_physical_block_size", return_value=4096),
            patch.object(node_utils, "_disk_holders",
                         side_effect=lambda n: ["dm-3"] if n in ("sdb", "sdc") else []),
            patch.object(node_utils, "_subtree_mounted", return_value=False),
            patch.object(node_utils, "simplyblock_data_signature", return_value=False),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def test_the_map_is_reported_with_its_paths_identity(self):
        devices = {d["name"]: d for d in node_utils.get_block_devices_info()}
        m = devices["mpatha"]
        self.assertEqual((m["type"], m["kname"], m["device_path"]), ("mpath", "dm-3", "/dev/mapper/mpatha"))
        self.assertEqual((m["serial"], m["wwn"], m["size"]), ("624a9370aa", "0x624a9370aa", 4 << 40))
        self.assertEqual(m["paths"], ["sdb", "sdc"])
        self.assertEqual(m["by_id_path"], "/dev/disk/by-id/dm-uuid-mpath-3624a9370aa")
        self.assertEqual(m["holders"], [])
        # The paths stay listed, held by the map, so nothing takes them alone.
        self.assertEqual(devices["sdb"]["multipath_map"], "mpatha")
        self.assertEqual(devices["sdb"]["holders"], ["dm-3"])

    def test_a_lun_lookup_returns_the_map_not_its_paths(self):
        found = node_utils.find_lun_devices("naa.624a9370aa")
        self.assertEqual([d["name"] for d in found], ["mpatha"])
        self.assertEqual(found[0]["device_path"], "/dev/mapper/mpatha")
        self.assertEqual(found[0]["by_id_path"], "/dev/disk/by-id/dm-uuid-mpath-3624a9370aa")

    def test_a_single_path_lun_is_still_its_disk(self):
        found = node_utils.find_lun_devices("624a9370bb")
        self.assertEqual([d["name"] for d in found], ["sdd"])


class TestMultipathResolution(unittest.TestCase):

    def test_a_configured_lun_resolves_to_its_map_not_a_path(self):
        from simplyblock_core import utils
        path = {"type": "disk", "serial": "624a9370aa", "size": 1, "holders": ["dm-3"],
                "multipath_map": "mpatha"}
        live = [dict(path, name="sdb", device_path="/dev/sdb"),
                {"type": "mpath", "name": "mpatha", "kname": "dm-3", "serial": "624a9370aa", "size": 1,
                 "device_path": "/dev/mapper/mpatha", "holders": []},
                dict(path, name="sdc", device_path="/dev/sdc")]
        resolved, missing = utils.resolve_lblk_entries([{"name": "mpatha", "serial": "624a9370aa"}], live)
        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["current_path"], "/dev/mapper/mpatha")
        self.assertEqual(resolved[0]["io_engine"], "uring")
