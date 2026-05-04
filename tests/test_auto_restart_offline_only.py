# coding=utf-8
"""
test_auto_restart_offline_only.py – guard tests for
``simplyblock_core.controllers.tasks_controller.add_node_to_auto_restart``.

Background: the storage_node_monitor used to queue an auto-restart task
on any node whose status fell into {DOWN, UNREACHABLE, SCHEDULABLE} when
the next health-check pass observed the node again. Auto-restart kills
SPDK and runs the full recreate path; doing that for transient blips
(DOWN = port-blocked, UNREACHABLE = mgmt-IP unreachable, SCHEDULABLE =
SPDK hung) escalates a recoverable hiccup into a destructive
kill-and-replay, and on a stressed cluster that kill-and-replay can hit
placement errors during lvstore-failover replay (incident 2026-05-02:
worker5 crash-looped 16 times in 52 minutes after a writer-conflict
DOWN got queued for restart and the restart hit unreachable peers).

The fix tightens the controller-level guard: an auto-restart may only
be enqueued when ``node.status == OFFLINE`` (i.e. the SPDK process is
actually gone). Any other state is rejected with a warning log; this
prevents future code paths from reintroducing the bug.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster


def _make_cluster(status=Cluster.STATUS_ACTIVE, distr_npcs=2):
    c = MagicMock(spec=Cluster)
    c.status = status
    c.distr_npcs = distr_npcs
    return c


def _make_node(status, uuid="node-under-test", cluster_id="cluster-1",
               mgmt_ip="10.0.0.1"):
    n = MagicMock(spec=StorageNode)
    n.status = status
    n.cluster_id = cluster_id
    n.mgmt_ip = mgmt_ip
    n.get_id = MagicMock(return_value=uuid)
    return n


class TestAddNodeToAutoRestartGuard(unittest.TestCase):
    """add_node_to_auto_restart must only enqueue when status == OFFLINE."""

    def _call(self, node, peers=None, cluster=None):
        """Invoke add_node_to_auto_restart with patched DB + _add_task.

        Returns (return_value, add_task_mock) so the test can assert both
        the result and whether the underlying task was queued.
        """
        peers = peers or []
        cluster = cluster or _make_cluster()
        from simplyblock_core.controllers import tasks_controller

        with patch.object(tasks_controller, "db") as mock_db, \
             patch.object(tasks_controller, "_add_task") as mock_add_task:
            mock_db.get_cluster_by_id.return_value = cluster
            mock_db.get_storage_nodes_by_cluster_id.return_value = peers
            mock_add_task.return_value = "task-uuid"
            result = tasks_controller.add_node_to_auto_restart(node)
            return result, mock_add_task

    # --- rejection paths ----------------------------------------------------

    def test_rejects_DOWN(self):
        node = _make_node(StorageNode.STATUS_DOWN)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_UNREACHABLE(self):
        node = _make_node(StorageNode.STATUS_UNREACHABLE)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_SCHEDULABLE(self):
        node = _make_node(StorageNode.STATUS_SCHEDULABLE)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_ONLINE(self):
        node = _make_node(StorageNode.STATUS_ONLINE)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_RESTARTING(self):
        node = _make_node(StorageNode.STATUS_RESTARTING)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_IN_SHUTDOWN(self):
        node = _make_node(StorageNode.STATUS_IN_SHUTDOWN)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_rejects_REMOVED(self):
        node = _make_node(StorageNode.STATUS_REMOVED)
        result, add_task = self._call(node)
        self.assertFalse(result)
        add_task.assert_not_called()

    # --- accept path --------------------------------------------------------

    def test_accepts_OFFLINE_and_enqueues(self):
        node = _make_node(StorageNode.STATUS_OFFLINE)
        result, add_task = self._call(node)
        self.assertEqual(result, "task-uuid")
        add_task.assert_called_once()
        # Verify the call was for FN_NODE_RESTART on the right node
        from simplyblock_core.models.job_schedule import JobSchedule
        args, kwargs = add_task.call_args
        self.assertEqual(args[0], JobSchedule.FN_NODE_RESTART)
        self.assertEqual(args[1], node.cluster_id)
        self.assertEqual(args[2], node.get_id())

    # --- the OFFLINE path still honors pre-existing guards ------------------

    def test_OFFLINE_still_rejected_when_cluster_inactive(self):
        # Even an OFFLINE node should not be queued if the cluster is in a
        # state where restart is not meaningful (e.g. UNAVAILABLE / removing).
        node = _make_node(StorageNode.STATUS_OFFLINE)
        cluster = _make_cluster(status="unavailable")
        result, add_task = self._call(node, cluster=cluster)
        self.assertFalse(result)
        add_task.assert_not_called()

    def test_OFFLINE_rejected_when_too_many_peers_offline(self):
        # The pre-existing offline-peer-count guard still applies. With
        # distr_npcs=2 and 3 other peers not online (and not the same
        # mgmt_ip), the cluster cannot afford the additional outage.
        node = _make_node(StorageNode.STATUS_OFFLINE,
                          uuid="self", mgmt_ip="10.0.0.1")
        peers = [
            _make_node(StorageNode.STATUS_OFFLINE,
                       uuid=f"peer-{i}", mgmt_ip=f"10.0.0.{i+2}")
            for i in range(3)
        ]
        # include self in the list to exercise the "skip self" branch
        peers.append(node)
        cluster = _make_cluster(status=Cluster.STATUS_ACTIVE, distr_npcs=2)
        result, add_task = self._call(node, peers=peers, cluster=cluster)
        self.assertFalse(result)
        add_task.assert_not_called()


class TestSetNodeOfflinePairing(unittest.TestCase):
    """Sanity check: set_node_offline is the canonical caller that pairs
    the OFFLINE flip with the auto-restart queue, so its call site is the
    one that must continue to work post-fix.
    """

    def test_set_node_offline_calls_add_node_to_auto_restart(self):
        # Verify the source still has the call: a regression guard that
        # nobody accidentally removes the only legitimate auto-restart
        # trigger while tightening the controller guard.
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "services", "storage_node_monitor.py",
        )
        with open(path, "r") as f:
            src = f.read()
        # The set_node_offline body must still include the auto-restart call.
        self.assertIn("def set_node_offline", src)
        # Locate the function span and verify add_node_to_auto_restart appears
        # within it (use a simple slice between this def and the next def).
        start = src.index("def set_node_offline")
        nxt = src.index("\ndef ", start + 1)
        body = src[start:nxt]
        self.assertIn("add_node_to_auto_restart", body,
                      "set_node_offline must still queue auto-restart")


class TestCheckNodeTailNoLongerEnqueues(unittest.TestCase):
    """Source-level guard: the tail of check_node() in storage_node_monitor
    must not queue auto-restart for DOWN/UNREACHABLE/SCHEDULABLE on
    health-check pass. We assert by source inspection rather than runtime
    because the service module has a ``while True`` and pulling it in
    requires the full module-load dance of the existing service tests.
    """

    def test_check_node_tail_does_not_call_auto_restart_for_DOWN(self):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "services", "storage_node_monitor.py",
        )
        with open(path, "r") as f:
            src = f.read()

        # Locate check_node body
        start = src.index("def check_node(")
        # find the start of the next top-level def
        nxt = src.index("\ndef ", start + 1)
        body = src[start:nxt]

        # The tail block that previously queued auto-restart for the
        # transient states must no longer mention any of the three:
        # the only legitimate trigger is set_node_offline (which is in
        # a different function and called from the spdk_process_is_up=False
        # branch — we check that branch is still intact below).
        self.assertNotIn(
            "tasks_controller.add_node_to_auto_restart",
            body.split("# 1- check node ping")[0]
            if "# 1- check node ping" in body else "",
            "no auto-restart enqueue should appear before the health "
            "checks (would mean we re-introduced the buggy tail block)",
        )

        # Also: the tail (after the port check) must not contain the
        # auto-restart call. Find the port-check block and slice from there.
        if "node_port_check_fun" in body:
            tail = body[body.index("node_port_check_fun"):]
            self.assertNotIn(
                "tasks_controller.add_node_to_auto_restart",
                tail,
                "tail of check_node() must not enqueue auto-restart for "
                "DOWN/UNREACHABLE/SCHEDULABLE on health-check pass",
            )

        # Sanity: the spdk_process_is_up=False branch still calls
        # set_node_offline (which is the legitimate trigger).
        self.assertIn("set_node_offline(snode)", body)


if __name__ == "__main__":
    unittest.main()
