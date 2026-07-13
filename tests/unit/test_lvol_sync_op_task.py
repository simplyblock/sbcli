# coding=utf-8
"""
test_lvol_sync_op_task.py — unit tests for the DB-backed deferred per-node
lvol operations (FN_LVOL_SYNC_OP) that replace the in-memory
``_restart_op_queues`` deferral (incident 2026-07-10: a create-registration
queued in the webappapi's in-memory queue was never drained; the volume's
tertiary subsystem was never created).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


def _task(op="register", lvol_id="lv-1", node_id="node-2", canceled=False,
          status=JobSchedule.STATUS_NEW, secondary_index=1):
    t = MagicMock(spec=JobSchedule)
    t.uuid = "task-1"
    t.function_name = JobSchedule.FN_LVOL_SYNC_OP
    t.function_params = {"lvol_id": lvol_id, "op": op,
                         "secondary_index": secondary_index}
    t.node_id = node_id
    t.cluster_id = "cl-1"
    t.canceled = canceled
    t.status = status
    return t


def _lvol(status=LVol.STATUS_ONLINE, nodes=("node-1", "node-2", "node-3")):
    lv = MagicMock(spec=LVol)
    lv.status = status
    lv.nodes = list(nodes)
    lv.lvs_name = "LVS_6"
    lv.lvol_bdev = "LVOL_1"
    lv.size = 4 * 1024 * 1024 * 1024
    lv.get_id = MagicMock(return_value="lv-1")
    return lv


def _node(status=StorageNode.STATUS_ONLINE, node_id="node-2"):
    n = MagicMock(spec=StorageNode)
    n.status = status
    n.get_id = MagicMock(return_value=node_id)
    return n


class TestRunLvolSyncOpTask(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(tasks_controller, "db")
        self.db = patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, task, lvol=None, node=None, phase=""):
        self.db.get_lvol_by_id.return_value = lvol or _lvol()
        self.db.get_storage_node_by_id.return_value = node or _node()
        with patch("simplyblock_core.storage_node_ops.get_restart_phase",
                   return_value=phase), \
             patch("simplyblock_core.storage_node_ops."
                   "repair_lvol_registration_on_non_leader",
                   return_value=(True, None)) as repair:
            tasks_controller.run_lvol_sync_op_task(task)
        return repair

    def test_register_repairs_and_completes(self):
        task = _task()
        repair = self._run(task)
        repair.assert_called_once()
        self.assertEqual(repair.call_args[0][2], 1)  # secondary_index
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)

    def test_deleted_lvol_completes_without_action(self):
        task = _task()
        self.db.get_lvol_by_id.side_effect = KeyError()
        tasks_controller.run_lvol_sync_op_task(task)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("no longer exists", task.function_result)

    def test_offline_node_defers(self):
        task = _task()
        self._run(task, node=_node(StorageNode.STATUS_OFFLINE))
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)

    def test_owned_lvs_defers(self):
        task = _task()
        self._run(task, phase=StorageNode.RESTART_PHASE_BLOCKED)
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)

    def test_node_dropped_from_topology_completes(self):
        task = _task(node_id="node-9")
        self._run(task, node=_node(node_id="node-9"))
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertIn("no longer hosts", task.function_result)

    def test_resize_converges_to_current_db_size(self):
        task = _task(op="resize")
        lvol = _lvol()
        node = _node()
        rpc = node.rpc_client.return_value
        rpc.bdev_lvol_resize.return_value = True
        self.db.get_lvol_by_id.return_value = lvol
        self.db.get_storage_node_by_id.return_value = node
        with patch("simplyblock_core.storage_node_ops.get_restart_phase",
                   return_value=""):
            tasks_controller.run_lvol_sync_op_task(task)
        rpc.bdev_lvol_resize.assert_called_once()
        args = rpc.bdev_lvol_resize.call_args[0]
        self.assertEqual(args[0], "LVS_6/LVOL_1")
        self.assertEqual(args[1], 4096)  # 4 GiB in MiB
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)


class TestAddTaskDedup(unittest.TestCase):
    def test_duplicate_sync_op_not_added(self):
        existing = _task()
        existing.status = JobSchedule.STATUS_SUSPENDED
        with patch.object(tasks_controller, "db") as db:
            db.get_job_tasks.return_value = [existing]
            ret = tasks_controller.add_lvol_sync_op_task(
                "cl-1", "node-2", "lv-1", "register", secondary_index=1)
        self.assertFalse(ret)

    def test_dedup_is_per_op_and_per_node(self):
        existing = _task(op="register")
        existing.status = JobSchedule.STATUS_SUSPENDED
        with patch.object(tasks_controller, "db") as db:
            db.get_job_tasks.return_value = [existing]
            # Same node+lvol+op → duplicate.
            self.assertEqual(
                tasks_controller.get_lvol_sync_op_task(
                    "cl-1", "node-2", "lv-1", "register"), existing.uuid)
            # Different op or node → not a duplicate.
            self.assertFalse(tasks_controller.get_lvol_sync_op_task(
                "cl-1", "node-2", "lv-1", "resize"))
            self.assertFalse(tasks_controller.get_lvol_sync_op_task(
                "cl-1", "node-3", "lv-1", "register"))


if __name__ == "__main__":
    unittest.main()
