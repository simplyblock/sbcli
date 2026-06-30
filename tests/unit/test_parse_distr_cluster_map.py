# coding=utf-8
"""
test_parse_distr_cluster_map.py — regression test for node-status parsing in
``distr_controller.parse_distr_cluster_map``.

The data-plane ``distr_dump_cluster_map`` output appends trailing fields to the
node line, e.g.::

    [0]  uuid_node=<uuid>  status=online  failure_domain=-1

A greedy ``status=(.*)$`` capture swallowed the trailing ``failure_domain=-1``
into the status value, so the parsed status (``"online  failure_domain=-1"``)
never matched the control-plane DB status (``"online"``). Every node then
reported ``failed`` and ``parse_distr_cluster_map`` returned ``passed=False``,
flipping Health=False cluster-wide right after activation.
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core import distr_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.get_id.return_value = node_id
    n.status = status
    return n


def _device(dev_id, status=NVMeDevice.STATUS_ONLINE):
    d = MagicMock(spec=NVMeDevice)
    d.get_id.return_value = dev_id
    d.status = status
    return d


# Mirrors the live SPDK dump format, including the trailing failure_domain field
# on node lines and the physical_label field on device lines.
_MAP_WITH_FAILURE_DOMAIN = """\
    [0]  uuid_node=node-a  status=online  failure_domain=-1
        [0]  storage_ID=0  physical_label=1  status=online  uuid_device=dev-0  storage_bdev_name=alceml_dev-0
    [1]  uuid_node=node-b  status=online  failure_domain=-1
        [0]  storage_ID=1  physical_label=2  status=online  uuid_device=dev-1  storage_bdev_name=remote_alceml_dev-1n1
"""


class TestParseDistrClusterMap(unittest.TestCase):

    def _ctx(self):
        nodes = {"node-a": _node("node-a"), "node-b": _node("node-b")}
        devices = {"dev-0": _device("dev-0"), "dev-1": _device("dev-1")}
        return nodes, devices

    def test_trailing_failure_domain_does_not_break_node_status(self):
        nodes, devices = self._ctx()
        results, passed = distr_controller.parse_distr_cluster_map(
            _MAP_WITH_FAILURE_DOMAIN, nodes, devices)

        node_results = [r for r in results if r["Kind"] == "Node"]
        self.assertEqual(len(node_results), 2)
        for r in node_results:
            self.assertEqual(
                r["Found Status"], "online",
                "trailing failure_domain field must not leak into the status")
            self.assertEqual(r["Results"], "ok")

        self.assertTrue(passed, "all-online map must parse as passed")

    def test_genuine_node_status_mismatch_still_fails(self):
        nodes, devices = self._ctx()
        nodes["node-b"].status = StorageNode.STATUS_UNREACHABLE

        results, passed = distr_controller.parse_distr_cluster_map(
            _MAP_WITH_FAILURE_DOMAIN, nodes, devices)

        self.assertFalse(passed)
        node_b = next(r for r in results if r["UUID"] == "node-b")
        self.assertEqual(node_b["Results"], "failed")
        self.assertEqual(node_b["Found Status"], "online")


if __name__ == "__main__":
    unittest.main()
