# coding=utf-8
"""
test_distr_cluster_map_weight.py — regression test for the parent (node)
weight invariant in ``distr_controller.get_distr_cluster_map``.

When a device fails and is migrated, the data plane is told the device's
slot is dead by flipping its ``id`` to ``-1`` in ``map_prob`` — but the
slot's weight (and therefore the *parent* node weight, which is the sum of
its devices' weights) must stay constant. The node's parent weight is its
share of the weighted placement tree relative to every other node; if a
full-map regen shrinks it when a device goes failed/migrated, the data
plane re-hashes and rebalances the ENTIRE cluster on the next
``distr_send_cluster_map`` instead of migrating only the failed device's
data.

This mirrors the surgical ``distr_replace_id_in_map_prob(order, -1)`` path,
which only swaps the id and leaves all weights untouched.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import distr_controller, utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


_GIB = 1024 ** 3


def _device(order, size_gib, status=NVMeDevice.STATUS_ONLINE):
    d = NVMeDevice()
    d.uuid = f"dev-{order}"
    d.cluster_device_order = order
    d.size = size_gib * _GIB
    d.status = status
    d.alceml_bdev = f"alceml_{order}"
    d.physical_label = 0
    return d


def _node(devices):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = "node-1"
    n.cluster_id = "cluster-1"
    n.status = StorageNode.STATUS_ONLINE
    n.is_secondary_node = False
    # -1 = no failure domain assigned (feature inactive for this node).
    n.failure_domain = -1
    n.nvme_devices = devices
    # target_node is the node itself, so no remote devices are consulted.
    n.remote_devices = []
    return n


def _item_by_weight_index(items, idx):
    """map_prob items preserve device order; index matches cluster_device_order."""
    return items[idx]


class TestClusterMapParentWeightInvariant(unittest.TestCase):

    def _build_map(self, node):
        with patch.object(distr_controller, "DBController") as DBCtor:
            cluster = MagicMock()
            cluster.enable_node_affinity = False
            cluster.enable_failure_domain = False
            DBCtor.return_value.get_cluster_by_id.return_value = cluster
            return distr_controller.get_distr_cluster_map([node], node, "distr-1")

    def test_failed_and_migrated_preserves_parent_weight(self):
        dev_w = utils.convert_size(8 * _GIB, "GiB") or 1  # weight per device
        self.assertGreater(dev_w, 0)

        # --- baseline: two healthy devices ---
        node = _node([_device(0, 8), _device(1, 8)])
        cl_map = self._build_map(node)
        prob = cl_map["map_prob"][0]

        self.assertEqual(prob["weight"], 2 * dev_w)
        self.assertEqual(_item_by_weight_index(prob["items"], 0)["id"], 0)
        self.assertEqual(_item_by_weight_index(prob["items"], 1)["id"], 1)

        # --- device 0 fails and is migrated ---
        node = _node([
            _device(0, 8, status=NVMeDevice.STATUS_FAILED_AND_MIGRATED),
            _device(1, 8),
        ])
        cl_map = self._build_map(node)
        prob = cl_map["map_prob"][0]

        # Parent weight is INVARIANT across the failure transition.
        self.assertEqual(
            prob["weight"], 2 * dev_w,
            "node parent weight must not shrink when a device is "
            "failed_and_migrated — otherwise the whole cluster rebalances")

        # The failed device's slot is marked dead (id=-1) but keeps its weight.
        failed_item = _item_by_weight_index(prob["items"], 0)
        self.assertEqual(failed_item["id"], -1)
        self.assertEqual(failed_item["weight"], dev_w)

        # The surviving device is untouched.
        ok_item = _item_by_weight_index(prob["items"], 1)
        self.assertEqual(ok_item["id"], 1)
        self.assertEqual(ok_item["weight"], dev_w)

    def test_plain_failed_also_preserves_parent_weight(self):
        """STATUS_FAILED (pre-migration) takes the same id=-1 branch and must
        likewise keep contributing its weight to the parent total."""
        dev_w = utils.convert_size(8 * _GIB, "GiB") or 1

        node = _node([
            _device(0, 8, status=NVMeDevice.STATUS_FAILED),
            _device(1, 8),
        ])
        prob = self._build_map(node)["map_prob"][0]

        self.assertEqual(prob["weight"], 2 * dev_w)
        self.assertEqual(_item_by_weight_index(prob["items"], 0)["id"], -1)
        self.assertEqual(_item_by_weight_index(prob["items"], 1)["id"], 1)


if __name__ == "__main__":
    unittest.main()
