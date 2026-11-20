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
