# coding=utf-8
"""Unit tests for the two 2026-08-05 incident fixes:

1. verify_jm_mesh_coverage — the activation JM-mesh gate: every ONLINE
   node must hold live remote bdevs for the remote JMs it references,
   with owner-offline tolerance (re-activation with unhealthy nodes must
   never be blocked) and a one-shot reconnect repair.

2. _kill_spdk_until_dead — failure-path SPDK teardown must prefer the
   container-level spdk_process_cleanup (verified-gone semantics) over
   the RPC-socket liveness probe that false-negatives a booted-but-
   RPC-dead SPDK (the hugepage-squatting zombie that starved add-node
   retries).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.nvme_device import JMDevice, RemoteJMDevice
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, status=StorageNode.STATUS_ONLINE, enable_ha_jm=True,
          jm_dev_id=None, jm_ids=(), remote_jms=()):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.enable_ha_jm = enable_ha_jm
    n.cluster_id = "cluster-1"
    if jm_dev_id:
        jm = JMDevice()
        jm.uuid = jm_dev_id
        n.jm_device = jm
    n.jm_ids = list(jm_ids)
    n.remote_jm_devices = list(remote_jms)
    return n


def _rjm(owner_id, remote_bdev):
    r = RemoteJMDevice()
    r.node_id = owner_id
    r.remote_bdev = remote_bdev
    r.jm_bdev = f"jm_{owner_id}"
    return r


class TestJmMeshCoverage(unittest.TestCase):

    def _run(self, nodes, bdevs_by_node, repair=False, reconnect_result=None):
        """bdevs_by_node: {node_uuid: set(bdev names present on that node)}"""
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = nodes
        db.get_storage_node_by_id.side_effect = lambda nid: next(
            n for n in nodes if n.get_id() == nid)

        def _rpc_for(node_self, **kw):
            rpc = MagicMock()
            present = bdevs_by_node.get(node_self.get_id(), set())
            rpc.get_bdevs.side_effect = lambda name: (
                [{"name": name}] if name in present else None)
            return rpc

        patches = [
            patch.object(storage_node_ops, "DBController", return_value=db),
            patch.object(StorageNode, "rpc_client", _rpc_for),
        ]
        if reconnect_result is not None:
            patches.append(patch.object(
                storage_node_ops, "_connect_to_remote_jm_devs",
                return_value=reconnect_result))
        for p in patches:
            p.start()
        try:
            return storage_node_ops.verify_jm_mesh_coverage("cluster-1", repair=repair)
        finally:
            for p in patches:
                p.stop()

    def _two_nodes(self):
        a = _node("node-a", jm_dev_id="jm-a", jm_ids=["jm-a", "jm-b"],
                  remote_jms=[_rjm("node-b", "remote_jm_node-bn1")])
        b = _node("node-b", jm_dev_id="jm-b", jm_ids=["jm-b", "jm-a"],
                  remote_jms=[_rjm("node-a", "remote_jm_node-an1")])
        return a, b

    def test_healthy_mesh(self):
        a, b = self._two_nodes()
        problems = self._run([a, b], {
            "node-a": {"remote_jm_node-bn1"},
            "node-b": {"remote_jm_node-an1"},
        })
        self.assertEqual(problems, [])

    def test_missing_remote_bdev_reported(self):
        a, b = self._two_nodes()
        problems = self._run([a, b], {
            "node-a": set(),  # a cannot see b's JM
            "node-b": {"remote_jm_node-an1"},
        })
        self.assertEqual(len(problems), 1)
        self.assertIn("node-a", problems[0])
        self.assertIn("node-b", problems[0])

    def test_missing_record_reported(self):
        # node-a references jm-b in jm_ids but has NO remote record at all —
        # the exact 2026-08-05 hole.
        a = _node("node-a", jm_dev_id="jm-a", jm_ids=["jm-a", "jm-b"],
                  remote_jms=[])
        b = _node("node-b", jm_dev_id="jm-b", jm_ids=["jm-b"])
        problems = self._run([a, b], {"node-a": set(), "node-b": set()})
        self.assertTrue(any("node-a" in p and "node-b" in p for p in problems))

    def test_offline_owner_tolerated(self):
        # Re-activation rule: a JM whose owner is not ONLINE is skipped.
        a, b = self._two_nodes()
        b.status = StorageNode.STATUS_OFFLINE
        problems = self._run([a, b], {"node-a": set()})
        self.assertEqual(problems, [])

    def test_offline_referencing_node_skipped(self):
        a, b = self._two_nodes()
        a.status = StorageNode.STATUS_OFFLINE
        problems = self._run([a, b], {
            "node-b": {"remote_jm_node-an1"},
        })
        self.assertEqual(problems, [])

    def test_repair_fixes_coverage(self):
        a, b = self._two_nodes()
        # Initially missing on node-a; after reconnect the returned record's
        # bdev IS present.
        problems = self._run(
            [a, b],
            {"node-a": {"remote_jm_node-bn1_new"},
             "node-b": {"remote_jm_node-an1"}},
            repair=True,
            reconnect_result=[_rjm("node-b", "remote_jm_node-bn1_new")],
        )
        self.assertEqual(problems, [])

    def test_repair_failure_still_reported(self):
        a, b = self._two_nodes()
        problems = self._run(
            [a, b],
            {"node-a": set(), "node-b": {"remote_jm_node-an1"}},
            repair=True,
            reconnect_result=[_rjm("node-b", "remote_jm_node-bn1")],
        )
        self.assertEqual(len(problems), 1)


class TestKillSpdkUntilDead(unittest.TestCase):

    def _snode(self):
        snode = MagicMock()
        snode.get_id.return_value = "node-1"
        snode.rpc_port = 4423
        snode.cluster_id = "cluster-1"
        snode.mgmt_ip = "10.0.0.1"
        return snode

    def test_cleanup_success_short_circuits(self):
        snode = self._snode()
        api = snode.client.return_value
        api.spdk_process_cleanup.return_value = (True, None)
        self.assertTrue(storage_node_ops._kill_spdk_until_dead(snode))
        api.spdk_process_cleanup.assert_called_once_with(4423, "cluster-1")
        api.spdk_process_kill.assert_not_called()

    def test_cleanup_unavailable_falls_back_to_kill(self):
        snode = self._snode()
        api = snode.client.return_value
        api.spdk_process_cleanup.side_effect = Exception("404 not found")
        api.spdk_process_is_up.return_value = (False, None)
        self.assertTrue(storage_node_ops._kill_spdk_until_dead(
            snode, max_attempts=1, poll_per_attempt_sec=1))
        api.spdk_process_kill.assert_called_once()

    def test_cleanup_incomplete_falls_back(self):
        snode = self._snode()
        api = snode.client.return_value
        api.spdk_process_cleanup.return_value = (None, "cleanup incomplete")
        api.spdk_process_is_up.return_value = (False, None)
        self.assertTrue(storage_node_ops._kill_spdk_until_dead(
            snode, max_attempts=1, poll_per_attempt_sec=1))
        api.spdk_process_kill.assert_called_once()

    def test_all_paths_fail_returns_false(self):
        snode = self._snode()
        api = snode.client.return_value
        api.spdk_process_cleanup.return_value = (None, "nope")
        api.spdk_process_is_up.return_value = (True, None)
        self.assertFalse(storage_node_ops._kill_spdk_until_dead(
            snode, max_attempts=1, poll_per_attempt_sec=0))


if __name__ == "__main__":
    unittest.main()
