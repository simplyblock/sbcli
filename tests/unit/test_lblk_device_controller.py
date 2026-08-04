# coding=utf-8
"""Unit tests for the lblk (aio) branches in controllers/device_controller.py
and the mode-aware late-event gate in services/main_distr_event_collector.py.

Covered:
  - reset_storage_device: aio liveness-probe semantics — bdev present clears
    the error state (no nvme controller reset issued); bdev gone returns
    False so the tasks framework escalates to restart_device.
  - get_device_health_info: aio SMART stub (never calls the nvme RPC).
  - new_device_from_failed: aio path recreates the AIO bdev serial-first
    from the live inventory instead of bind_device_to_spdk + controller
    attach.
  - restart_device: aio path recreates the missing AIO bdev (with qd
    sampling re-armed) instead of the PCIe attach sequence.
  - late-event gate: for aio devices the "controller gone?" probe is
    get_bdevs_2 on the base bdev; a present bdev skips the late event.
"""

import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.services import main_distr_event_collector as collector


def _aio_dev(uid="dev-1", status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.uuid = uid
    d.node_id = "node-1"
    d.cluster_id = "cluster-1"
    d.status = status
    d.bdev_type = "aio"
    d.serial_number = "S1"
    d.nvme_bdev = "aio_S1"
    d.device_path = "/dev/sdb"
    d.by_id_path = "/dev/disk/by-id/wwn-1"
    return d


class TestResetStorageDeviceAio(unittest.TestCase):

    def _run(self, bdev_present):
        device = _aio_dev(status=NVMeDevice.STATUS_UNAVAILABLE)
        snode = MagicMock()
        snode.cluster_id = "cluster-1"
        rpc = MagicMock()
        rpc.get_bdevs.return_value = [{"name": "aio_S1"}] if bdev_present else None
        snode.rpc_client.return_value = rpc

        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        db.get_storage_node_by_id.return_value = snode

        with patch.object(device_controller, "DBController", return_value=db), \
             patch.object(device_controller.tasks_controller,
                          "get_active_dev_restart_task", return_value=None), \
             patch.object(device_controller, "device_set_unavailable") as set_unavail, \
             patch.object(device_controller, "device_set_io_error") as set_io_err, \
             patch.object(device_controller, "device_set_retries_exhausted") as set_retries, \
             patch.object(device_controller, "device_set_online") as set_online, \
             patch.object(device_controller, "device_events"):
            result = device_controller.reset_storage_device("dev-1")
        return result, rpc, set_unavail, set_io_err, set_retries, set_online

    def test_bdev_present_clears_error_state(self):
        result, rpc, _, set_io_err, set_retries, set_online = self._run(True)
        self.assertTrue(result)
        set_io_err.assert_called_once_with("dev-1", False)
        set_retries.assert_called_once_with("dev-1", False)
        set_online.assert_called_once()
        rpc.reset_device.assert_not_called()

    def test_bdev_gone_fails_for_escalation(self):
        result, rpc, _, set_io_err, _, set_online = self._run(False)
        self.assertFalse(result)
        set_io_err.assert_not_called()
        set_online.assert_not_called()
        rpc.reset_device.assert_not_called()


class TestHealthInfoAio(unittest.TestCase):

    def test_aio_returns_stub_without_nvme_rpc(self):
        device = _aio_dev()
        snode = MagicMock()
        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        db.get_storage_node_by_id.return_value = snode
        with patch.object(device_controller, "DBController", return_value=db):
            ret = device_controller.get_device_health_info("dev-1")
        data = json.loads(ret)
        self.assertEqual(data["bdev_type"], "aio")
        self.assertIsNone(data["smart"])
        snode.rpc_client.assert_not_called()


class TestNewDeviceFromFailedAio(unittest.TestCase):

    def _run(self, bdev_present_initially, inventory=None, create_ok=True):
        device = _aio_dev(status=NVMeDevice.STATUS_FAILED_AND_MIGRATED)
        node = MagicMock()
        node.get_id.return_value = "node-1"
        node.nvme_devices = [device]

        rpc = MagicMock()
        state = {"present": bdev_present_initially}

        def _get_bdevs(name):
            return [{"name": name}] if state["present"] else None

        def _aio_create(name, filename, block_size=0):
            if create_ok:
                state["present"] = True
                return name
            return None

        rpc.get_bdevs.side_effect = _get_bdevs
        rpc.bdev_aio_create.side_effect = _aio_create
        node.rpc_client.return_value = rpc

        client = MagicMock()
        client.get_blockdevices.return_value = (inventory or [], None)
        node.client.return_value = client

        db = MagicMock()
        db.get_storage_nodes.return_value = [node]
        with patch.object(device_controller, "DBController", return_value=db):
            result = device_controller.new_device_from_failed("dev-1")
        return result, rpc, db

    def test_bdev_already_present_no_create(self):
        result, rpc, db = self._run(True)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_not_called()
        db.atomic_update.assert_called_once()

    def test_recreates_bdev_serial_first_from_inventory(self):
        inventory = [{"name": "sdx", "serial": "S1",
                      "device_path": "/dev/sdx",
                      "by_id_path": "/dev/disk/by-id/wwn-NEW"}]
        result, rpc, _ = self._run(False, inventory=inventory)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-NEW")
        rpc.bdev_set_qd_sampling_period.assert_called_once()

    def test_falls_back_to_stored_path_when_inventory_empty(self):
        result, rpc, _ = self._run(False, inventory=[])
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1")

    def test_create_failure_returns_false(self):
        result, _, db = self._run(False, inventory=[], create_ok=False)
        self.assertFalse(result)
        db.atomic_update.assert_not_called()


class TestRestartDeviceAio(unittest.TestCase):

    def _run(self, bdev_present):
        device = _aio_dev(status=NVMeDevice.STATUS_REMOVED)
        device.nvmf_nqn = ""
        device.alceml_bdev = ""
        snode = MagicMock()
        snode.cluster_id = "cluster-1"
        snode.nvme_devices = [device]
        snode.jm_device = None

        rpc = MagicMock()
        state = {"present": bdev_present}
        rpc.get_bdevs.side_effect = (
            lambda name: [{"name": name}] if state["present"] else None)

        def _aio_create(name, filename, block_size=0):
            state["present"] = True
            return name

        rpc.bdev_aio_create.side_effect = _aio_create
        snode.rpc_client.return_value = rpc

        client = MagicMock()
        client.get_blockdevices.return_value = ([], None)
        snode.client.return_value = client

        db = MagicMock()
        db.get_storage_device_by_id.return_value = device
        db.get_storage_node_by_id.return_value = snode

        with patch.object(device_controller, "DBController", return_value=db), \
             patch.object(device_controller.tasks_controller,
                          "get_active_dev_restart_task", return_value=None), \
             patch.object(device_controller, "device_set_retries_exhausted"), \
             patch.object(device_controller, "device_set_unavailable"), \
             patch.object(device_controller, "_def_create_device_stack",
                          return_value=True) as create_stack, \
             patch.object(device_controller, "device_set_io_error") as set_io_err, \
             patch.object(device_controller, "device_set_online") as set_online, \
             patch.object(device_controller, "device_events"):
            result = device_controller.restart_device("dev-1")
        return result, rpc, create_stack, set_io_err, set_online

    def test_missing_aio_bdev_recreated_before_stack(self):
        result, rpc, create_stack, set_io_err, set_online = self._run(False)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_called_once_with(
            "aio_S1", "/dev/disk/by-id/wwn-1")
        rpc.bdev_set_qd_sampling_period.assert_called_once()
        create_stack.assert_called_once()
        set_io_err.assert_called_once_with("dev-1", False)
        set_online.assert_called_once()
        # never the nvme path
        rpc.bdev_nvme_controller_attach.assert_not_called()

    def test_present_aio_bdev_not_recreated(self):
        result, rpc, create_stack, _, _ = self._run(True)
        self.assertTrue(result)
        rpc.bdev_aio_create.assert_not_called()
        create_stack.assert_called_once()


class TestLateEventGateAio(unittest.TestCase):

    def test_present_aio_bdev_skips_late_event(self):
        device = _aio_dev()
        device.cluster_device_order = 7

        home_node = MagicMock()
        home_node.get_id.return_value = "node-1"
        home_node.nvme_devices = [device]

        event_node = MagicMock()
        event_node.get_id.return_value = "node-2"
        rpc = MagicMock()
        rpc.get_bdevs_2.return_value = ([{"name": "aio_S1"}], None)
        event_node.rpc_client.return_value = rpc

        event = MagicMock()
        event.message = "error_read"
        event.node_id = "node-2"
        event.storage_id = 7
        stale = datetime.now() - timedelta(seconds=30)
        event.object_dict = {"timestamp": stale.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}

        db = MagicMock()
        db.get_storage_node_by_id.return_value = event_node
        db.get_storage_nodes.return_value = [home_node]

        with patch.object(collector, "db", db), \
             patch.object(collector, "_is_target_remote_controller_healthy",
                          return_value=False):
            collector.process_device_event(event, collector.logger)

        rpc.get_bdevs_2.assert_called_once_with("aio_S1")
        rpc.bdev_nvme_controller_list_2.assert_not_called()
        self.assertIn("skipping", event.status)


if __name__ == "__main__":
    unittest.main()
