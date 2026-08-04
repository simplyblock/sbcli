# coding=utf-8
"""Unit tests for the lblk hung-IO watchdog and device-disappearance sweep
in services/device_monitor.py.

The watchdog is the control-plane replacement for bdev_nvme's
timeout_us/action_on_timeout (which AIO bdevs lack): queue-depth-sampled
iostat with no completion progress across N polls => the device is fed into
the SAME machinery an erroring nvme device hits (io_error + UNAVAILABLE,
countable LOCAL_FAILURE cause). Disappearance from the host inventory =>
device_remove, the SPDK_BDEV_EVENT_REMOVE treatment.

Covered:
  - stall accumulation requires inflight IO on EVERY poll AND zero progress
  - any completion progress resets the window
  - RPC failure / missing bdevs / missing queue_depth freeze (never count)
  - missing queue_depth re-arms qd-sampling
  - threshold trip returns the device
  - non-ONLINE devices are ignored and their tracking state cleared
  - nvme (bdev_type != aio) devices are never touched
  - presence sweep: absent-debounce, recovery clears the counter, inventory
    failure freezes, serial OR name match counts as present
  - action dispatch: 1 stalled -> io_error+UNAVAILABLE(LOCAL_FAILURE);
    >=2 stalled -> node-level auto-restart; gone -> device_remove
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.services import device_monitor
from simplyblock_core.services.device_monitor import (
    _check_aio_device_presence,
    _check_aio_hung_io,
    _sweep_aio_devices,
)


def _aio_dev(uid="dev-1", status=NVMeDevice.STATUS_ONLINE, serial="S1",
             name="sdb", bdev_type="aio"):
    d = NVMeDevice()
    d.uuid = uid
    d.status = status
    d.serial_number = serial
    d.device_name = name
    d.bdev_type = bdev_type
    d.nvme_bdev = f"aio_{serial}"
    return d


def _node(devs, node_id="node-1"):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.nvme_devices = devs
    return n


def _rpc_with_stats(stats_by_bdev):
    rpc = MagicMock()

    def _stats(name):
        entry = stats_by_bdev.get(name)
        if entry is None:
            return {"bdevs": []}
        if isinstance(entry, Exception):
            raise entry
        return {"bdevs": [entry]}

    rpc.get_lvol_stats.side_effect = _stats
    return rpc


def _stat(total_ops, queue_depth):
    return {"num_read_ops": total_ops, "num_write_ops": 0,
            "num_unmap_ops": 0, "queue_depth": queue_depth}


class WatchdogBase(unittest.TestCase):
    def setUp(self):
        device_monitor._aio_progress.clear()
        device_monitor._aio_absent.clear()


class TestHungIoDetection(WatchdogBase):

    def test_first_poll_never_stalls(self):
        dev = _aio_dev()
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        self.assertEqual(_check_aio_hung_io(_node([dev]), rpc), [])

    def test_stall_requires_threshold_consecutive_polls(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        # poll 1 primes; polls 2..N-1 accumulate below threshold
        for _ in range(constants.AIO_HUNG_IO_STALL_POLLS):
            self.assertEqual(_check_aio_hung_io(node, rpc), [])
        # poll that reaches the threshold trips
        self.assertEqual(_check_aio_hung_io(node, rpc), [dev])

    def test_progress_resets_window(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        for _ in range(constants.AIO_HUNG_IO_STALL_POLLS):
            _check_aio_hung_io(node, rpc)
        # completions advanced -> reset
        rpc2 = _rpc_with_stats({dev.nvme_bdev: _stat(101, 5)})
        self.assertEqual(_check_aio_hung_io(node, rpc2), [])
        # stalling again needs the full window again
        rpc3 = _rpc_with_stats({dev.nvme_bdev: _stat(101, 5)})
        self.assertEqual(_check_aio_hung_io(node, rpc3), [])

    def test_zero_queue_depth_is_idle_not_stall(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 0)})
        for _ in range(constants.AIO_HUNG_IO_STALL_POLLS + 2):
            self.assertEqual(_check_aio_hung_io(node, rpc), [])

    def test_rpc_exception_freezes_counter(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        for _ in range(constants.AIO_HUNG_IO_STALL_POLLS):
            _check_aio_hung_io(node, rpc)
        # one failing poll must neither trip nor reset
        bad = _rpc_with_stats({dev.nvme_bdev: RuntimeError("rpc down")})
        self.assertEqual(_check_aio_hung_io(node, bad), [])
        # next good stalled poll trips (counter was frozen, not reset)
        self.assertEqual(_check_aio_hung_io(node, rpc), [dev])

    def test_empty_bdevs_freezes_counter(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({})  # no entry -> {"bdevs": []}
        self.assertEqual(_check_aio_hung_io(node, rpc), [])
        self.assertNotIn(dev.get_id(), device_monitor._aio_progress)

    def test_missing_queue_depth_rearms_sampling_and_freezes(self):
        dev = _aio_dev()
        node = _node([dev])
        stat = {"num_read_ops": 1, "num_write_ops": 0, "num_unmap_ops": 0}
        rpc = _rpc_with_stats({dev.nvme_bdev: stat})
        self.assertEqual(_check_aio_hung_io(node, rpc), [])
        rpc.bdev_set_qd_sampling_period.assert_called_once_with(
            dev.nvme_bdev, constants.AIO_QD_SAMPLING_PERIOD_US)

    def test_non_online_device_ignored_and_state_cleared(self):
        dev = _aio_dev()
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        _check_aio_hung_io(node, rpc)
        self.assertIn(dev.get_id(), device_monitor._aio_progress)
        dev.status = NVMeDevice.STATUS_UNAVAILABLE
        self.assertEqual(_check_aio_hung_io(node, rpc), [])
        self.assertNotIn(dev.get_id(), device_monitor._aio_progress)

    def test_nvme_devices_never_touched(self):
        dev = _aio_dev(bdev_type="nvme")
        node = _node([dev])
        rpc = _rpc_with_stats({dev.nvme_bdev: _stat(100, 5)})
        for _ in range(constants.AIO_HUNG_IO_STALL_POLLS + 2):
            self.assertEqual(_check_aio_hung_io(node, rpc), [])
        rpc.get_lvol_stats.assert_not_called()


class TestDevicePresence(WatchdogBase):

    def _node_with_inventory(self, devs, inventory):
        node = _node(devs)
        client = MagicMock()
        client.get_blockdevices.return_value = (inventory, None)
        node.client.return_value = client
        return node

    def test_present_by_serial(self):
        dev = _aio_dev(serial="S1", name="sdb")
        # renamed on host: serial still matches
        node = self._node_with_inventory([dev], [{"name": "sdx", "serial": "S1"}])
        self.assertEqual(_check_aio_device_presence(node), [])
        self.assertNotIn(dev.get_id(), device_monitor._aio_absent)

    def test_present_by_name_fallback(self):
        dev = _aio_dev(serial="S1", name="sdb")
        node = self._node_with_inventory([dev], [{"name": "sdb", "serial": "OTHER"}])
        self.assertEqual(_check_aio_device_presence(node), [])

    def test_absent_debounced_then_reported(self):
        dev = _aio_dev()
        node = self._node_with_inventory([dev], [{"name": "sdz", "serial": "ZZ"}])
        for _ in range(constants.AIO_DEVICE_ABSENT_POLLS - 1):
            self.assertEqual(_check_aio_device_presence(node), [])
        self.assertEqual(_check_aio_device_presence(node), [dev])

    def test_reappearance_clears_counter(self):
        dev = _aio_dev(serial="S1")
        gone = self._node_with_inventory([dev], [])
        # inventory [] is falsy -> unknown, so use a non-matching entry
        gone = self._node_with_inventory([dev], [{"name": "x", "serial": "y"}])
        _check_aio_device_presence(gone)
        back = self._node_with_inventory([dev], [{"name": "sdb", "serial": "S1"}])
        self.assertEqual(_check_aio_device_presence(back), [])
        self.assertNotIn(dev.get_id(), device_monitor._aio_absent)

    def test_inventory_failure_freezes(self):
        dev = _aio_dev()
        node = _node([dev])
        node.client.side_effect = RuntimeError("agent down")
        for _ in range(constants.AIO_DEVICE_ABSENT_POLLS + 2):
            self.assertEqual(_check_aio_device_presence(node), [])
        self.assertNotIn(dev.get_id(), device_monitor._aio_absent)

    def test_no_aio_devices_no_inventory_call(self):
        dev = _aio_dev(bdev_type="nvme")
        node = _node([dev])
        self.assertEqual(_check_aio_device_presence(node), [])
        node.client.assert_not_called()


class TestSweepActions(WatchdogBase):

    def _sweep(self, node, stalled=None, gone=None):
        with patch.object(device_monitor, "_check_aio_device_presence",
                          return_value=gone or []), \
             patch.object(device_monitor, "_check_aio_hung_io",
                          return_value=stalled or []), \
             patch.object(device_monitor, "device_controller") as dc, \
             patch.object(device_monitor, "tasks_controller") as tc:
            _sweep_aio_devices(node)
        return dc, tc

    def test_single_stalled_marks_unavailable_with_countable_cause(self):
        dev = _aio_dev()
        node = _node([dev])
        dc, tc = self._sweep(node, stalled=[dev])
        dc.device_set_io_error.assert_called_once_with(dev.get_id(), True)
        dc.device_set_unavailable.assert_called_once_with(
            dev.get_id(), cause=device_monitor.CAUSE_LOCAL_FAILURE)
        tc.add_node_to_auto_restart.assert_not_called()

    def test_two_stalled_escalates_to_node_restart(self):
        d1, d2 = _aio_dev("dev-1", serial="S1"), _aio_dev("dev-2", serial="S2")
        node = _node([d1, d2])
        dc, tc = self._sweep(node, stalled=[d1, d2])
        tc.add_node_to_auto_restart.assert_called_once_with(node)
        dc.device_set_unavailable.assert_not_called()

    def test_gone_device_removed_with_countable_cause(self):
        dev = _aio_dev()
        node = _node([dev])
        dc, _ = self._sweep(node, gone=[dev])
        dc.device_remove.assert_called_once_with(
            dev.get_id(), cause=device_monitor.CAUSE_LOCAL_FAILURE)

    def test_stall_tracking_cleared_after_action(self):
        dev = _aio_dev()
        device_monitor._aio_progress[dev.get_id()] = (100, 3)
        node = _node([dev])
        self._sweep(node, stalled=[dev])
        self.assertNotIn(dev.get_id(), device_monitor._aio_progress)


if __name__ == "__main__":
    unittest.main()
