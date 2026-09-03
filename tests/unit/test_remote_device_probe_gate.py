# coding=utf-8
"""
``health_controller.check_remote_device`` must not probe for a device whose
owning node has departed.

Found live 2026-09-03. The function gated only the health *verdict*, never the
probe. The caller at health_controller.py:761 discards the result when the
owning node is gone, but it calls this function first, so both RPCs still went
out on every cycle for every surviving node.

For a REMOVED node's devices that never stops: each miss makes SPDK log
``*ERROR*: ctrlr 'remote_alceml_<uuid>' does not exist``, measured at 3-15
errors/min and still climbing 35 minutes after the removal that made those
devices failed_and_migrated (devices 04fce724 / b0ada39d / ddf660f5 of the
removed node 2vk79, probed by 9 surviving nodes). Real faults then drown in a
permanent error stream.

The remote-JM loop in the same file already skips the RPC for an irrelevant
owner; this is the same rule applied to the device path.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import health_controller
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = "c1"
    return n


class TestRemoteDeviceProbeSkipsDepartedOwners(unittest.TestCase):

    def _run(self, owner_status):
        owner = _node("owner", owner_status)
        prober = _node("prober", StorageNode.STATUS_ONLINE)
        rpc = MagicMock()
        rpc.get_bdevs = MagicMock(return_value=[{"name": "x"}])
        rpc.bdev_nvme_controller_list = MagicMock(return_value=[])
        prober.rpc_client = MagicMock(return_value=rpc)

        device = MagicMock()
        device.node_id = "owner"
        device.alceml_bdev = "alceml_d1"
        device.nvmf_multipath = False

        db = MagicMock()
        db.get_storage_device_by_id = MagicMock(return_value=device)
        db.get_storage_node_by_id = MagicMock(return_value=owner)
        db.get_storage_nodes_by_cluster_id = MagicMock(
            return_value=[owner, prober])
        with patch.object(health_controller, "DBController", return_value=db):
            result = health_controller.check_remote_device("d1")
        return result, rpc

    def test_no_rpc_is_issued_for_a_removed_owner(self):
        # Asserting on the RPCs, not the return value: the caller already
        # discards the verdict for a departed owner, so a verdict-only
        # assertion passes with the bug still in place.
        result, rpc = self._run(StorageNode.STATUS_REMOVED)
        rpc.get_bdevs.assert_not_called()
        rpc.bdev_nvme_controller_list.assert_not_called()
        self.assertTrue(result, "a departed owner must not fail health")

    def test_no_rpc_for_other_departed_states(self):
        for status in (StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_IN_REMOVAL,
                       StorageNode.STATUS_RESTARTING):
            with self.subTest(status=status):
                _result, rpc = self._run(status)
                rpc.get_bdevs.assert_not_called()
                rpc.bdev_nvme_controller_list.assert_not_called()

    def test_a_live_owner_is_still_probed(self):
        # _peer_connections_relevant: ONLINE / DOWN / UNREACHABLE are the
        # states where the connection is genuinely expected to exist.
        for status in (StorageNode.STATUS_ONLINE,
                       StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE):
            with self.subTest(status=status):
                _result, rpc = self._run(status)
                rpc.get_bdevs.assert_called_once_with("remote_alceml_d1n1")
                rpc.bdev_nvme_controller_list.assert_called_once_with(
                    "remote_alceml_d1")


if __name__ == "__main__":
    unittest.main()
