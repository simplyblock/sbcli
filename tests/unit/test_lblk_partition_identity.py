"""A partition used as an lblk device is known by one serial for as long as it
exists.

lsblk prints no PARTUUID in the storage-node container, so the agent gave a
partition there an id made up from its kernel name (``SYN-<hash>``). NVMe
kernel names follow the probe order of the controllers: after a reboot that
ordered them differently, the id changed, the restart saw a new device and the
recorded one was gone.

Covered:
  - the agent reads the PARTUUID from the device and builds the serial from it;
    the id it used before is reported next to it;
  - a control plane that predates this still gets the former id;
  - a node that recorded a partition under the former id finds it under that
    id, also after the kernel renamed it, and records its PARTUUID;
  - a partition with the same PARTUUID on another disk is not that partition;
  - the node-config slot of such a node still answers to the former id.
"""

import json
import unittest
from unittest.mock import patch

from flask import Flask

from simplyblock_core import utils
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClient
from simplyblock_core.utils import lblk_identity
from simplyblock_web import node_utils
from simplyblock_web.api.internal.storage_node import docker as agent_api

GIB = 1024 ** 3
PARTUUID = "58c6f91c-0000-4000-8000-000000000001"


def _lsblk(disk="nvme0n1", lsblk_partuuid=""):
    """One NVMe drive with one partition, as lsblk -J prints it."""
    return {"blockdevices": [
        {"name": disk, "type": "disk", "size": 270 * GIB, "serial": "DRIVE-1421", "wwn": "",
         "mountpoint": None, "model": "QEMU NVMe Ctrl", "rota": False, "ro": False, "vendor": "",
         "children": [
             {"name": f"{disk}p1", "type": "part", "size": 200 * GIB,
              "partuuid": lsblk_partuuid, "mountpoint": None, "ro": False},
         ]},
    ]}


def _inventory(disk="nvme0n1", lsblk_partuuid="", probed=PARTUUID):
    """get_block_devices_info() of a host whose lsblk prints ``lsblk_partuuid``
    and whose partition has the PARTUUID ``probed`` on the device."""
    with patch.object(node_utils.shell_utils, "run_command",
                      return_value=(json.dumps(_lsblk(disk, lsblk_partuuid)), "", 0)), \
            patch.object(node_utils, "_probe_partuuid", return_value=probed) as probe, \
            patch.object(node_utils, "_root_disk_names", return_value=[]), \
            patch.object(node_utils, "_disk_holders", return_value=[]), \
            patch.object(node_utils, "_partition_holders", return_value=[]), \
            patch.object(node_utils, "read_persistent_links", return_value={}), \
            patch.object(node_utils, "_read_sysfs", return_value="0"), \
            patch.object(node_utils.host_network, "node_hostname", return_value="worker-1"):
        devices = node_utils.get_block_devices_info()
    return devices, probe


def _partition(devices):
    return next(d for d in devices if d["type"] == "part")


def _recorded(live, **overrides):
    """The lblk_devices entry a node holds for a partition it added while the
    agent could not read the PARTUUID: the made-up id, no PARTUUID."""
    entry = {"name": live["name"], "serial": live["legacy_serial"], "by_id": "",
             "size": live["size"], "numa": 0, "type": "part", "partuuid": "",
             "parent_serial": live["parent_serial"]}
    entry.update(overrides)
    return entry


class TestAgentReadsThePartuuidFromTheDevice(unittest.TestCase):

    def test_serial_is_built_from_the_probed_partuuid(self):
        devices, _ = _inventory()
        part = _partition(devices)
        self.assertEqual(part["serial"], f"DRIVE-1421-part-{PARTUUID}")
        self.assertEqual(part["partuuid"], PARTUUID)
        self.assertFalse(part["serial_synthetic"])
        self.assertTrue(part["legacy_serial"].startswith("SYN-"))

    def test_serial_survives_a_kernel_rename_and_the_former_id_does_not(self):
        before = _partition(_inventory(disk="nvme0n1")[0])
        after = _partition(_inventory(disk="nvme3n1")[0])
        self.assertEqual(before["serial"], after["serial"])
        self.assertNotEqual(before["legacy_serial"], after["legacy_serial"])

    def test_partuuid_printed_by_lsblk_is_not_probed_and_has_no_former_id(self):
        devices, probe = _inventory(lsblk_partuuid=PARTUUID)
        part = _partition(devices)
        probe.assert_not_called()
        self.assertEqual(part["serial"], f"DRIVE-1421-part-{PARTUUID}")
        self.assertNotIn("legacy_serial", part)

    def test_partition_without_any_partuuid_keeps_the_made_up_id(self):
        part = _partition(_inventory(probed="")[0])
        self.assertTrue(part["serial"].startswith("SYN-"))
        self.assertTrue(part["serial_synthetic"])
        self.assertNotIn("legacy_serial", part)


class TestOlderControlPlaneGetsTheFormerId(unittest.TestCase):

    def _get(self, query_string):
        devices, _ = _inventory()
        app = Flask(__name__)
        with app.test_request_context(), \
                patch.object(agent_api.node_utils, "get_block_devices_info", return_value=devices):
            query = agent_api._BlockDevicesQuery(**query_string)
            response = agent_api.get_blockdevices(query)
        return _partition(response.get_json()["results"]), _partition(devices)

    def test_a_request_without_the_parameter_gets_the_former_id(self):
        sent, part = self._get({})
        self.assertEqual(sent["serial"], part["legacy_serial"])
        self.assertTrue(sent["serial_synthetic"])

    def test_a_request_that_asks_for_it_gets_the_partuuid_serial(self):
        sent, part = self._get({"partition_serials": "partuuid"})
        self.assertEqual(sent["serial"], part["serial"])
        self.assertEqual(sent["legacy_serial"], part["legacy_serial"])

    def test_the_client_asks_for_it(self):
        client = SNodeClient("10.0.0.1:5000")
        with patch.object(client, "_request", return_value=([], None)) as request:
            client.get_blockdevices()
        request.assert_called_once_with("GET", "blockdevices", {"partition_serials": "partuuid"})


class TestNodeFindsThePartitionItRecorded(unittest.TestCase):

    def _node_inventory(self, recorded, live):
        """What StorageNode.client().get_blockdevices() returns for a node
        holding ``recorded`` on a host whose agent reports ``live``."""
        node = StorageNode({"uuid": "node-1", "api_endpoint": "10.0.0.1:5000",
                            "lblk_devices": recorded})
        client = node.client()
        with patch.object(client, "_request", return_value=(live, None)):
            inventory, _ = client.get_blockdevices()
        return inventory

    def test_recorded_under_the_former_id_is_reported_under_it(self):
        live, _ = _inventory()
        entry = _recorded(_partition(live))
        part = _partition(self._node_inventory([entry], live))
        self.assertEqual(part["serial"], entry["serial"])
        self.assertEqual(part["reported_serial"], f"DRIVE-1421-part-{PARTUUID}")

    def test_restart_resolves_it_as_the_device_it_knows(self):
        live, _ = _inventory()
        entry = _recorded(_partition(live))
        resolved, missing = utils.resolve_lblk_entries([entry], self._node_inventory([entry], live))
        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["serial"], entry["serial"])
        self.assertEqual(resolved[0]["partuuid"], PARTUUID)

    def test_found_after_a_kernel_rename_once_the_partuuid_is_recorded(self):
        # First restart on the new agent: found by the former id, PARTUUID recorded.
        live, _ = _inventory(disk="nvme0n1")
        entry = _recorded(_partition(live))
        lblk_identity.remember_partuuids([entry], self._node_inventory([entry], live))
        self.assertEqual(entry["partuuid"], PARTUUID)
        # Reboot: the drive is nvme3n1 now and its former id is another one.
        renamed, _ = _inventory(disk="nvme3n1")
        self.assertNotEqual(_partition(renamed)["legacy_serial"], entry["serial"])
        resolved, missing = utils.resolve_lblk_entries([entry], self._node_inventory([entry], renamed))
        self.assertEqual(missing, [])
        self.assertEqual(resolved[0]["name"], "nvme3n1p1")
        self.assertEqual(resolved[0]["serial"], entry["serial"])

    def test_found_after_a_rename_by_the_by_partuuid_path_it_was_selected_by(self):
        live, _ = _inventory(disk="nvme0n1")
        entry = _recorded(_partition(live), path=f"/dev/disk/by-partuuid/{PARTUUID}")
        renamed, _ = _inventory(disk="nvme3n1")
        part = _partition(self._node_inventory([entry], renamed))
        self.assertEqual(part["serial"], entry["serial"])

    def test_same_partuuid_on_another_disk_is_another_partition(self):
        # A disk cloned from the same template: same PARTUUID, other drive.
        live, _ = _inventory()
        entry = _recorded(_partition(live), partuuid=PARTUUID, parent_serial="DRIVE-OTHER")
        part = _partition(self._node_inventory([entry], live))
        self.assertEqual(part["serial"], f"DRIVE-1421-part-{PARTUUID}")
        self.assertNotIn("reported_serial", part)

    def test_a_partition_recorded_under_its_partuuid_serial_is_left_alone(self):
        live, _ = _inventory()
        entry = _recorded(_partition(live), serial=f"DRIVE-1421-part-{PARTUUID}", partuuid=PARTUUID)
        self.assertEqual(self._node_inventory([entry], live), live)

    def test_two_partitions_answering_to_one_recorded_id_are_left_alone(self):
        live, _ = _inventory()
        entry = _recorded(_partition(live), partuuid=PARTUUID, serial="SYN-recorded")
        twin = dict(_partition(live), name="nvme9n1p1", legacy_serial="SYN-twin")
        inventory = self._node_inventory([entry], [*live, twin])
        self.assertEqual([d.get("reported_serial") for d in inventory], [None, None, None])

    def test_a_host_that_is_no_node_yet_gets_the_inventory_as_reported(self):
        live, _ = _inventory()
        client = SNodeClient("10.0.0.1:5000")
        with patch.object(client, "_request", return_value=(live, None)):
            inventory, _ = client.get_blockdevices()
        self.assertEqual(inventory, live)


class TestNodeConfigSlotAnswersToTheFormerId(unittest.TestCase):

    def test_configure_records_the_former_id_next_to_the_serial(self):
        devices, _ = _inventory()
        with patch.object(utils.node_utils, "get_block_devices_info", return_value=devices):
            entries = utils.detect_lblk_devices(
                include_serials=[f"DRIVE-1421-part-{PARTUUID}"], min_units=1)
        entry = entries["nvme0n1p1"]
        self.assertEqual(entry["serial"], f"DRIVE-1421-part-{PARTUUID}")
        self.assertEqual(entry["legacy_serial"], _partition(devices)["legacy_serial"])

    def test_slot_serials_include_the_former_id(self):
        # persist_node_config: the node sends the id it recorded, the slot of
        # the re-written node config carries the PARTUUID-based serial.
        devices, _ = _inventory()
        part = _partition(devices)
        slot = [{"name": "nvme0n1p1", "serial": part["serial"], "legacy_serial": part["legacy_serial"]}]
        recorded = [_recorded(part)]
        self.assertTrue(utils.lblk_device_serials(slot) & utils.lblk_device_serials(recorded))

    def test_slot_is_found_after_a_reboot_renamed_the_partition(self):
        # The node recorded the made-up id and, on its first restart with the
        # new agent, the PARTUUID. A reboot renames the drive: the agent's
        # node config now carries another made-up id next to the serial.
        before = _partition(_inventory(disk="nvme0n1")[0])
        recorded = [_recorded(before, partuuid=PARTUUID)]
        after = _partition(_inventory(disk="nvme3n1")[0])
        slot = [{"name": "nvme3n1p1", "serial": after["serial"], "legacy_serial": after["legacy_serial"]}]
        self.assertNotIn(recorded[0]["serial"], utils.lblk_device_serials(slot))
        self.assertEqual(utils.lblk_device_serials(slot) & utils.lblk_device_serials(recorded),
                         {f"DRIVE-1421-part-{PARTUUID}"})


if __name__ == "__main__":
    unittest.main()
