"""A removal that gave up must be re-drivable (2026-09-15, node a1b050f1).

STATUS_REMOVED_FAILED exists to offer exactly one thing: the operator fixes
whatever blocked the removal and starts it again. remove_storage_node admits
the status for that reason. But its phase-1 shutdown was guarded by an
exclusion list -- "shut down unless the node is already PENDING_REMOVAL /
IN_REMOVAL / OFFLINE / REMOVED" -- while the orchestrator's own shutdown step
used an inclusion list, "shut down if the node is ONLINE or SUSPENDED".

The two agreed until REMOVED_FAILED existed. It is in neither list, so the
orchestrator skipped the step (correct: the node is already down) while
admission ran it, and shutdown_storage_node refuses that status outright:

    Node is in removed_failed state; only online/suspended/down can be
    gracefully shut down. Use --force.

The re-drive died there, before queueing anything -- the one status that could
not use the recovery built for it.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


class _Attempt:
    """remove_storage_node far enough to see whether it shuts down and queues."""

    def __init__(self, status):
        self.node = MagicMock()
        self.node.get_id.return_value = "a1b050f1"
        self.node.status = status
        self.node.cluster_id = "c1"
        self.node.hostname = "bd4qf_4420"
        self.shutdown_calls = []
        self.queued = []

    def run(self):
        db = MagicMock()
        db.get_storage_node_by_id.return_value = self.node
        db.get_lvols_by_node_id.return_value = []
        db.get_snapshots.return_value = []
        db.get_cluster_by_id.return_value = MagicMock()
        ops = storage_node_ops
        with patch.object(ops, "DBController", return_value=db), \
             patch.object(ops.tasks_controller, "get_active_node_removal_task",
                          return_value=None), \
             patch.object(ops.tasks_controller, "get_active_node_tasks",
                          return_value=[]), \
             patch.object(ops.tasks_controller, "add_node_removal_task",
                          side_effect=lambda *a, **k: self.queued.append(a) or "task-1"), \
             patch.object(ops, "_check_ftt_allows_node_removal", return_value=(True, "")), \
             patch.object(ops, "_check_replica_relocation_feasible", return_value=(True, "")), \
             patch.object(ops, "set_node_status"), \
             patch.object(ops, "shutdown_storage_node",
                          side_effect=lambda nid, **kw: self.shutdown_calls.append(nid) or True), \
             patch("simplyblock_core.controllers.cluster_expansion.preconditions."
                   "check_fd_admission_for_remove", return_value=(True, "")):
            return ops.remove_storage_node("a1b050f1")


class TestRedriveFromRemovedFailed(unittest.TestCase):

    def test_a_removed_failed_node_is_not_shut_down_again(self):
        """The regression: it is already down, and shutdown_storage_node
        refuses the status, so attempting it aborted the whole re-drive."""
        a = _Attempt(StorageNode.STATUS_REMOVED_FAILED)
        self.assertTrue(a.run())
        self.assertEqual(a.shutdown_calls, [])
        self.assertEqual(len(a.queued), 1, "the re-drive must queue its task")

    def test_an_online_node_is_still_shut_down_first(self):
        a = _Attempt(StorageNode.STATUS_ONLINE)
        self.assertTrue(a.run())
        self.assertEqual(a.shutdown_calls, ["a1b050f1"])
        self.assertEqual(len(a.queued), 1)

    def test_a_suspended_node_is_still_shut_down_first(self):
        a = _Attempt(StorageNode.STATUS_SUSPENDED)
        self.assertTrue(a.run())
        self.assertEqual(a.shutdown_calls, ["a1b050f1"])

    def test_already_stopped_statuses_skip_the_shutdown(self):
        for status in (StorageNode.STATUS_PENDING_REMOVAL,
                       StorageNode.STATUS_IN_REMOVAL,
                       StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE):
            a = _Attempt(status)
            self.assertTrue(a.run(), status)
            self.assertEqual(a.shutdown_calls, [], status)


class TestTheTwoGuardsAgree(unittest.TestCase):

    def test_admission_uses_the_orchestrators_condition(self):
        """One rule, stated the same way in both places. Written as an
        exclusion list on one side and an inclusion list on the other, they
        silently diverged the moment a status was added."""
        import inspect
        src = inspect.getsource(storage_node_ops.remove_storage_node)
        assert ("if snode.status in [StorageNode.STATUS_ONLINE, "
                "StorageNode.STATUS_SUSPENDED]:") in src, src[-2000:]


if __name__ == "__main__":
    unittest.main()
