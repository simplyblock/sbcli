"""Device health logs, whatever the device is attached through, and the tick
rate the exporter publishes beside each device's latency counters.

The shape is the contract: an NVMe controller read through SPDK, a kernel NVMe
namespace read through nvme-cli, and a SATA/SAS disk read through smartctl all
come back under SPDK's field names, and a device with no health log at all is
``supported: False`` rather than an error.
"""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import device_controller as dc
from simplyblock_web import node_utils


def _device(bdev_type="nvme", device_path="", by_id_path=""):
    return SimpleNamespace(
        bdev_type=bdev_type, device_path=device_path, by_id_path=by_id_path,
        nvme_controller="nvme_0", node_id="node-1", model_id="M1", serial_number="SN1")


NVME_CLI_LOG = {
    "critical_warning": 4, "temperature": 313, "avail_spare": 97, "spare_thresh": 10,
    "percent_used": 12, "media_errors": 2, "power_on_hours": 5000,
    "unsafe_shutdowns": {"value": 7}, "data_units_read": "1,234",
}

SMARTCTL_ATA = {
    "model_name": "WDC WD40", "serial_number": "WD-1", "firmware_version": "82.00",
    "smart_status": {"passed": False},
    "temperature": {"current": 44}, "power_on_time": {"hours": 30000}, "power_cycle_count": 88,
    "ata_smart_attributes": {"table": [
        {"id": 5, "raw": {"value": 16}}, {"id": 197, "raw": {"value": 2}}, {"id": 9, "raw": {"value": 1}},
    ]},
}


class TestMapHostSmart(unittest.TestCase):
    def test_an_nvme_cli_log_uses_the_spdk_names(self):
        info = dc.map_host_smart(_device("aio"), {"supported": True, "source": "nvme-cli", "data": NVME_CLI_LOG})
        self.assertTrue(info["supported"])
        self.assertEqual(info["source"], "nvme-cli")
        self.assertEqual(info["critical_warning"], 4)
        self.assertEqual(info["temperature_celsius"], 40)  # Kelvin in the log
        self.assertEqual(info["available_spare_percentage"], 97)
        self.assertEqual(info["available_spare_threshold_percentage"], 10)
        self.assertEqual(info["percentage_used"], 12)
        self.assertEqual(info["unsafe_shutdowns"], 7)  # {"value": n} wrapper
        self.assertEqual(info["data_units_read"], 1234)  # 128-bit counters print as strings
        self.assertIsNone(info["smart_passed"])

    def test_a_smartctl_ata_log_carries_the_overall_result_and_sectors(self):
        info = dc.map_host_smart(_device("aio"), {"supported": True, "source": "smartctl", "data": SMARTCTL_ATA})
        self.assertIs(info["smart_passed"], False)
        self.assertEqual(info["reallocated_sectors"], 16)
        self.assertEqual(info["pending_sectors"], 2)
        self.assertEqual(info["temperature_celsius"], 44)
        self.assertEqual(info["power_on_hours"], 30000)
        self.assertEqual(info["model_number"], "WDC WD40")
        self.assertIsNone(info["critical_warning"])

    def test_a_smartctl_nvme_log_is_judged_by_critical_warning(self):
        data = {"nvme_smart_health_information_log": {
            "critical_warning": 1, "temperature": 300, "available_spare": 5,
            "available_spare_threshold": 10, "percentage_used": 50}}
        info = dc.map_host_smart(_device("aio"), {"supported": True, "source": "smartctl", "data": data})
        self.assertEqual(info["critical_warning"], 1)
        self.assertEqual(info["available_spare_percentage"], 5)
        self.assertIsNone(info["smart_passed"])

    def test_an_unsupported_device_has_no_metrics(self):
        info = dc.map_host_smart(_device("aio"), {"supported": False, "source": "", "data": None})
        self.assertFalse(info["supported"])
        self.assertIsNone(info["critical_warning"])
        self.assertEqual(info["serial_number"], "SN1")  # identity is still known


class TestGetDeviceHealthInfo(unittest.TestCase):
    def _db(self, device):
        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        node = MagicMock()
        db.get_storage_node_by_id.return_value = node
        return db, node

    def test_an_spdk_device_returns_a_dict(self):
        device = _device("nvme")
        db, node = self._db(device)
        node.rpc_client.return_value.bdev_nvme_get_controller_health_info.return_value = {
            "critical_warning": 0, "temperature_celsius": 35}
        with patch.object(dc, "DBController", return_value=db):
            info = dc.get_device_health_info("dev-1")
        self.assertIsInstance(info, dict)
        self.assertTrue(info["supported"])
        self.assertEqual(info["source"], "spdk")
        self.assertEqual(info["temperature_celsius"], 35)

    def test_an_aio_device_is_read_by_the_node_agent(self):
        device = _device("aio", device_path="/dev/nvme1n1")
        db, node = self._db(device)
        node.client.return_value.device_smart.return_value = (
            {"supported": True, "source": "nvme-cli", "data": NVME_CLI_LOG}, None)
        with patch.object(dc, "DBController", return_value=db):
            info = dc.get_device_health_info("dev-1")
        node.client.return_value.device_smart.assert_called_once_with("/dev/nvme1n1")
        self.assertEqual(info["critical_warning"], 4)

    def test_an_aio_device_without_a_path_is_unsupported(self):
        db, _ = self._db(_device("aio"))
        with patch.object(dc, "DBController", return_value=db):
            info = dc.get_device_health_info("dev-1")
        self.assertFalse(info["supported"])

    def test_an_agent_error_raises(self):
        device = _device("aio", device_path="/dev/sdb")
        db, node = self._db(device)
        node.client.return_value.device_smart.return_value = (None, "boom")
        with patch.object(dc, "DBController", return_value=db), self.assertRaises(ValueError):
            dc.get_device_health_info("dev-1")


class TestReadDeviceSmart(unittest.TestCase):
    def test_a_kernel_nvme_namespace_is_read_with_nvme_cli(self):
        with patch.object(node_utils.os.path, "realpath", side_effect=lambda p: p), \
                patch.object(node_utils.shell_utils, "run_command",
                             return_value=(json.dumps(NVME_CLI_LOG), "", 0)) as run:
            out = node_utils.read_device_smart("/dev/nvme1n1p2")
        run.assert_called_once_with("nvme smart-log -o json /dev/nvme1n1")  # the disk, not the partition
        self.assertEqual(out["source"], "nvme-cli")
        self.assertTrue(out["supported"])

    def test_without_smartctl_a_scsi_disk_is_unsupported(self):
        with patch.object(node_utils.os.path, "realpath", side_effect=lambda p: p), \
                patch.object(node_utils.shutil, "which", return_value=None):
            out = node_utils.read_device_smart("/dev/sdb1")
        self.assertFalse(out["supported"])
        self.assertIn("smartctl", out["reason"])

    def test_smartctl_findings_about_the_disk_are_still_a_log(self):
        # Exit bit 3 ("disk failing") is a finding, not a failure to read.
        with patch.object(node_utils.os.path, "realpath", side_effect=lambda p: p), \
                patch.object(node_utils.shutil, "which", return_value="/usr/sbin/smartctl"), \
                patch.object(node_utils.shell_utils, "run_command",
                             return_value=(json.dumps(SMARTCTL_ATA), "", 0x08)):
            out = node_utils.read_device_smart("/dev/sdb")
        self.assertTrue(out["supported"])
        self.assertEqual(out["source"], "smartctl")

    def test_a_virtio_disk_smartctl_cannot_open_is_unsupported(self):
        doc = {"smartctl": {"messages": [{"string": "Unable to detect device type"}]}}
        with patch.object(node_utils.os.path, "realpath", side_effect=lambda p: p), \
                patch.object(node_utils.shutil, "which", return_value="/usr/sbin/smartctl"), \
                patch.object(node_utils.shell_utils, "run_command", return_value=(json.dumps(doc), "", 0x02)):
            out = node_utils.read_device_smart("/dev/vdb")
        self.assertFalse(out["supported"])
        self.assertIn("Unable to detect device type", out["reason"])

    def test_a_path_outside_dev_is_refused(self):
        for bad in ("/etc/shadow", "/dev/sda;reboot", "/dev/../etc/passwd", "sda"):
            with self.subTest(path=bad), self.assertRaises(ValueError):
                node_utils.read_device_smart(bad)


class TestTickRateExport(unittest.TestCase):
    def test_the_tick_rate_is_exported_and_a_zero_is_omitted(self):
        from simplyblock_web.api.v2 import metrics

        family = metrics._tick_rate_family([
            (["c", "cn", "n1", "d1"], {"tick_rate": 2_100_000_000}),
            (["c", "cn", "n1", "d2"], {"tick_rate": 0}),
            (["c", "cn", "n1", "d3"], {}),
        ])
        self.assertEqual(family.name, "simplyblock_device_tick_rate_hertz")
        self.assertEqual([(s.labels["device"], s.value) for s in family.samples], [("d1", 2_100_000_000)])
