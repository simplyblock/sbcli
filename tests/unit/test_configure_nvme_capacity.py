"""The NVMe capacity `sn configure` records, measured before it unbinds the
devices from the kernel driver.

nvme-cli sees a device only while the kernel nvme driver holds it, and
generate_configs unbinds every selected device. A capacity read after that
point is 0, sys_memory falls to the 2 GiB base, and add_node then refuses the
node because neither the config nor the agent's device list carries a
capacity.
"""

import json
import unittest
from unittest.mock import patch

from simplyblock_core import constants, utils

PCI = "0000:00:04.0"
CAPACITY = 2 * 375 * 1024 ** 3


class _Host:
    """A host whose one NVMe controller is listed by nvme-cli until its
    driver is unbound."""

    def __init__(self, scans_until_namespace=0):
        self.bound = True
        # A controller just bound to nvme (back from vfio/uio) has no
        # namespace until the kernel's asynchronous scan finds it.
        self.scans_until_namespace = scans_until_namespace

    def nvme_list(self):
        if not self.bound:
            return json.dumps({"Devices": []})
        if self.scans_until_namespace > 0:
            self.scans_until_namespace -= 1
            return json.dumps({"Devices": [{"Subsystems": [{
                "Controllers": [{"Address": PCI, "Namespaces": []}],
            }]}]})
        return json.dumps({"Devices": [{"Subsystems": [{
            "Controllers": [{"Address": PCI, "Namespaces": [{"PhysicalSize": CAPACITY}]}],
        }]}]})

    def unbind(self, address):
        assert address == PCI
        self.bound = False


def _configure(host):
    cores = {"cpu_mask": "0xff", "isolated": [1, 2, 3, 4, 5, 6], "core_to_index": {},
             "distribution": [[]] * 9}
    with patch.object(utils, "get_numa_cores", return_value={0: list(range(32))}), \
            patch.object(utils, "validate_sockets"), \
            patch.object(utils, "detect_nics", return_value={}), \
            patch.object(utils, "detect_nvmes",
                         return_value={"nvme0": {"pci_address": PCI, "numa_node": 0}}), \
            patch.object(utils, "get_nvme_list_verbose", side_effect=host.nvme_list), \
            patch.object(utils.pci_utils, "unbind_driver", side_effect=host.unbind), \
            patch.object(utils.time, "sleep"), \
            patch.object(utils, "generate_core_allocation", return_value={0: [dict(cores)]}), \
            patch.object(utils, "get_core_indexes", return_value=[]), \
            patch.object(utils, "generate_l_cores", return_value=""), \
            patch.object(utils, "calculate_core_allocations", return_value=[[]] * 9), \
            patch.object(utils, "calculate_pool_count", return_value=(1, 1)), \
            patch.object(utils, "calculate_minimum_hp_memory", return_value=0), \
            patch.object(utils.node_utils, "get_memory_details",
                         return_value={"free": 1 << 40, "huge_total": 0}):
        config, _ = utils.generate_configs(0, 0, [0], 1, [], [])
    return config


class TestConfigureNvmeCapacity(unittest.TestCase):

    def test_sys_memory_covers_the_devices_unbound_during_configure(self):
        host = _Host()
        config = _configure(host)
        self.assertFalse(host.bound)
        self.assertEqual(config["nodes"][0]["sys_memory"], utils.sys_memory_for_capacity(CAPACITY))
        self.assertGreater(config["nodes"][0]["sys_memory"], constants.SYS_MEMORY_BASE_BYTES)

    def test_a_device_rebound_from_spdk_is_measured_once_its_namespace_appears(self):
        host = _Host(scans_until_namespace=3)
        config = _configure(host)
        self.assertEqual(config["nodes"][0]["sys_memory"], utils.sys_memory_for_capacity(CAPACITY))

    def test_a_recomputation_after_configure_keeps_the_capacity(self):
        # configure-upgrade and add_node recompute sys_memory from the
        # persisted entry, when the devices are long unbound.
        node = _configure(_Host())["nodes"][0]
        self.assertEqual(utils.node_config_min_sys_memory(node), utils.sys_memory_for_capacity(CAPACITY))

    def test_an_edited_device_set_drops_the_recorded_capacity(self):
        old = _configure(_Host())
        new = json.loads(json.dumps(old))
        new["nodes"][0]["ssd_pcis"] = ["0000:00:05.0"]
        with patch.object(utils, "detect_nics", return_value={}), \
                patch.object(utils, "calculate_pool_count", return_value=(1, 1)), \
                patch.object(utils, "calculate_minimum_hp_memory", return_value=0), \
                patch.object(utils, "get_nvme_list_verbose", return_value=json.dumps({"Devices": []})), \
                patch.object(utils.node_utils, "get_memory_details",
                             return_value={"free": 1 << 40, "huge_total": 0}):
            config = utils.regenerate_config(new, old)
        self.assertNotIn("ssd_capacity", config["nodes"][0])


if __name__ == "__main__":
    unittest.main()
