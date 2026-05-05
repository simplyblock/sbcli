# coding=utf-8
"""
test_auto_restart_offline_only.py – guard tests for
``simplyblock_core.controllers.tasks_controller.add_node_to_auto_restart``.

Background: the storage_node_monitor used to queue an auto-restart task
on any node whose status fell into {DOWN, UNREACHABLE, SCHEDULABLE}.
The DOWN branch turned a port-block (SPDK still alive, recovery is
port-unblock) into a destructive kill-and-replay, and on a stressed
cluster that kill-and-replay hit placement errors during lvstore-
failover replay (incident 2026-05-02: worker5 crash-looped 16 times
in 52 minutes after a writer-conflict DOWN got queued for restart and
the restart hit unreachable peers).

The accepted-state set is therefore tightened to {OFFLINE, UNREACHABLE,
SCHEDULABLE} — DOWN no longer triggers auto-restart. UNREACHABLE
remains because by the time UNREACHABLE is set, peer JM connections
and remote-device records on other nodes have already been torn down
on their side; clearing the flag passively would leave the data-plane
view inconsistent with this node, so the full restart impl is the
correct re-integration path.
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
    """add_node_to_auto_restart enqueues only for OFFLINE / UNREACHABLE
    / SCHEDULABLE; everything else is rejected."""

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

    # --- accept paths -------------------------------------------------------

    def _assert_enqueued(self, status):
        node = _make_node(status)
        result, add_task = self._call(node)
        self.assertEqual(result, "task-uuid")
        add_task.assert_called_once()
        from simplyblock_core.models.job_schedule import JobSchedule
        args, kwargs = add_task.call_args
        self.assertEqual(args[0], JobSchedule.FN_NODE_RESTART)
        self.assertEqual(args[1], node.cluster_id)
        self.assertEqual(args[2], node.get_id())

    def test_accepts_OFFLINE_and_enqueues(self):
        self._assert_enqueued(StorageNode.STATUS_OFFLINE)

    def test_accepts_UNREACHABLE_and_enqueues(self):
        self._assert_enqueued(StorageNode.STATUS_UNREACHABLE)

    def test_accepts_SCHEDULABLE_and_enqueues(self):
        self._assert_enqueued(StorageNode.STATUS_SCHEDULABLE)

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


class TestCheckNodeTailQueuesOnlyForUnreachable(unittest.TestCase):
    """Source-level guard: the tail of check_node() in storage_node_monitor
    queues auto-restart only for UNREACHABLE — not for DOWN, not for
    SCHEDULABLE. (The DOWN branch was the iteration-77 / 2026-05-02
    regression: a port-block was being escalated to a kill-and-replay.)

    UNREACHABLE remains because peer JM connections / remote-device
    records on other nodes were torn down on their side; passively
    clearing the flag would leave the data plane inconsistent.

    Assertion is source-level rather than runtime because the service
    module has a ``while True`` and pulling it in requires the full
    module-load dance the other service tests already use.
    """

    def _check_node_body(self):
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..",
            "simplyblock_core", "services", "storage_node_monitor.py",
        )
        with open(path, "r") as f:
            src = f.read()
        start = src.index("def check_node(")
        nxt = src.index("\ndef ", start + 1)
        return src[start:nxt]

    def test_tail_queues_auto_restart_for_UNREACHABLE(self):
        body = self._check_node_body()
        # The tail must contain a path that gates on UNREACHABLE and
        # calls add_node_to_auto_restart. Use the LAST occurrence of
        # STATUS_UNREACHABLE — earlier occurrences are in the
        # early-skip / state-classification blocks at the head of
        # check_node and aren't the auto-restart trigger.
        self.assertIn("STATUS_UNREACHABLE", body)
        idx = body.rindex("STATUS_UNREACHABLE")
        window = body[idx:idx + 2000]
        self.assertIn("add_node_to_auto_restart", window)

    def test_tail_does_not_queue_for_DOWN_or_SCHEDULABLE(self):
        body = self._check_node_body()
        # Find the tail (after the port-check block) and confirm there's
        # no DOWN/SCHEDULABLE-gated auto-restart enqueue in it. We do this
        # by grepping for a tuple/in-list pattern that mentions DOWN
        # together with add_node_to_auto_restart.
        tail = body[body.index("node_port_check_fun"):] if "node_port_check_fun" in body else body
        # The previous buggy form clustered all three states into one
        # tuple. Ensure that pattern is gone.
        self.assertNotIn("STATUS_DOWN", tail.split("add_node_to_auto_restart")[0]
                         + tail.split("add_node_to_auto_restart")[-1]
                         if tail.count("add_node_to_auto_restart") == 1 else "STATUS_DOWN_DUMMY",
                         "auto-restart tail must not gate on DOWN")
        self.assertNotIn(
            "STATUS_SCHEDULABLE",
            tail.split("add_node_to_auto_restart")[0] if tail.count("add_node_to_auto_restart") == 1 else "",
            "auto-restart tail must not gate on SCHEDULABLE inside check_node",
        )

    def test_set_node_offline_branch_still_intact(self):
        body = self._check_node_body()
        # Sanity: the spdk_process_is_up=False branch still calls
        # set_node_offline (which is the legitimate OFFLINE trigger).
        self.assertIn("set_node_offline(snode)", body)


if __name__ == "__main__":
    unittest.main()
