"""The detach and reattach that follow partitioning, one device at a time.

add_node partitions a node's NVMe devices in parallel threads. Each thread
writes the GPT through nbd, then detaches the device's controller and attaches
it again so that SPDK examines the new partition table. Detaching a PCIe
controller is a hot remove in DPDK, and two of them at once on one target do
not both complete: on dev-ocp (2026-11-19) worker-3's two devices were
detached 5 ms apart, SPDK logged

    pci.c: detach_rte: Timeout waiting for DPDK to remove PCI device 0000:03:00.0

and the reattach of that device failed with -19 "No such device". The node add
failed on every attempt, losing whichever device finished its removal second.
"""

import contextlib
import threading
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.rpc_client import RPCRemoteError


class _Target:
    """An SPDK target whose PCIe hot remove loses a device when it overlaps
    the removal of another one."""

    def __init__(self, devices):
        self._guard = threading.Lock()
        # Two threads that reach the detach together meet here, so that the
        # overlap happens on every run rather than by chance. A thread that
        # waits alone gives up after the timeout and goes on.
        self._meet = threading.Barrier(len(devices), timeout=0.5)
        self.removing: set = set()
        self.lost: set = set()

    def client(self):
        rpc = MagicMock()
        rpc.nbd_start_disk.side_effect = lambda bdev, dev: dev
        rpc.nbd_get_disk.return_value = None
        rpc.bdev_nvme_controller_list.return_value = []
        rpc.bdev_nvme_detach_controller.side_effect = self.detach
        rpc.bdev_nvme_controller_attach.side_effect = self.attach
        return rpc

    def detach(self, controller):
        with self._guard:
            self.removing.add(controller)
        # A barrier broken by the timeout means no other removal overlapped.
        with contextlib.suppress(threading.BrokenBarrierError):
            self._meet.wait()
        with self._guard:
            if len(self.removing) > 1:
                self.lost.add(controller)
        return True

    def attach(self, controller, pcie_address):
        with self._guard:
            self.removing.discard(controller)
            if controller in self.lost:
                raise RPCRemoteError("No such device", -19)
        return [f"{controller}n1"]


class _Node:
    """The node both threads partition. A plain object, not a MagicMock: on
    the free-threaded interpreter two threads that touch a mock attribute
    first at the same moment can each create their own child mock."""

    def get_id(self):
        return "node-1"

    def client(self):
        api = MagicMock()
        api.make_gpt_partitions.return_value = (True, None)
        return api


def _device(index):
    nvme = MagicMock()
    nvme.nvme_bdev = f"nvme_{index}n1"
    nvme.nvme_controller = f"nvme_{index}"
    nvme.pcie_address = f"0000:0{index}:00.0"
    nvme.size = 1024 ** 4
    return nvme


class TestPartitionReattach(unittest.TestCase):

    def test_parallel_partitioning_reattaches_every_device(self):
        devices = [_device(2), _device(3)]
        target = _Target(devices)
        snode = _Node()

        results: dict = {}

        def partition(index, nvme):
            results[nvme.nvme_controller] = storage_node_ops._create_device_partitions(
                target.client(), nvme, snode, 1, 3, 0, index + 1)

        with patch.object(storage_node_ops.time, "sleep"):
            threads = [threading.Thread(target=partition, args=(i, d)) for i, d in enumerate(devices)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(target.lost, set(), "a device was lost to an overlapping hot remove")
        self.assertEqual(results, {"nvme_2": True, "nvme_3": True})


if __name__ == "__main__":
    unittest.main()
