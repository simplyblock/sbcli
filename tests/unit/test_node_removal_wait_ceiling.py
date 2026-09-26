"""A node removal must wait for its device migration, and must eventually stop.

Two halves of one behaviour:

* ``_decommission_node_devices`` reports "not done" while any data device is
  short of FAILED_AND_MIGRATED. Its docstring always promised this and the task
  runner was always written for it -- "returns False to mean 'incomplete, retry
  later' (most commonly: device failure-migration still in progress)" -- but the
  gate shipped commented out in the original feature commit (a1fd2638b), so
  phase 5 declared the removal complete as soon as it had QUEUED the migrations.
  Live effect on 2026-09-11: removals "finished" in ~20s with failure-migration
  tasks still running, and the next removal was refused with "Task found: 4"
  while the cluster reported ACTIVE.

* Because that wait is now real, it needs an exit. The removal task was created
  with max_retry=-1 (uncapped) on the reasoning that migration waits are long;
  one task was observed retrying 68 times with nothing surfacing why. A removal
  that cannot finish now ends at STATUS_REMOVED_FAILED, which an operator can
  see and re-drive.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_node_removal as runner


def _device(dev_id, status):
    dev = NVMeDevice()
    dev.uuid = dev_id
    dev.status = status
    return dev


class TestDeviceMigrationGate(unittest.TestCase):
    """The completion gate itself."""

    def _run(self, statuses):
        node = StorageNode()
        node.uuid = "n1"
        node.cluster_id = "c1"
        node.nvme_devices = [_device(f"d{i}", s) for i, s in enumerate(statuses)]
        node.jm_device = None
        node.jm_ids = []

        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        db.get_storage_device_by_id.side_effect = lambda i: next(
            d for d in node.nvme_devices if d.get_id() == i)
        db.get_storage_nodes_by_cluster_id.return_value = [node]

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", MagicMock()), \
             patch.object(storage_node_ops, "_decommission_node_jm", MagicMock()):
            return storage_node_ops._decommission_node_devices(node)

    def test_waits_while_a_device_is_still_migrating(self):
        """The regression. FAILED means the migration is queued, not finished."""
        ret = self._run([NVMeDevice.STATUS_FAILED_AND_MIGRATED,
                         NVMeDevice.STATUS_FAILED])
        self.assertFalse(
            ret,
            "a device short of FAILED_AND_MIGRATED must keep the removal open; "
            "returning True here declares the node removed while its data is "
            "still being rebuilt elsewhere")

    def test_done_when_every_device_is_migrated(self):
        ret = self._run([NVMeDevice.STATUS_FAILED_AND_MIGRATED,
                         NVMeDevice.STATUS_FAILED_AND_MIGRATED])
        self.assertTrue(ret)

    def test_jm_devices_do_not_hold_the_removal_open(self):
        """The JM is decommissioned separately and never reaches
        FAILED_AND_MIGRATED, so it must not be counted as pending."""
        ret = self._run([NVMeDevice.STATUS_FAILED_AND_MIGRATED,
                         NVMeDevice.STATUS_JM])
        self.assertTrue(ret)

    def test_a_node_with_no_devices_is_done(self):
        self.assertTrue(self._run([]))


class TestRemovalRetryCeiling(unittest.TestCase):
    """The exit, once the wait is real."""

    def _task(self, retry, max_retry):
        task = JobSchedule()
        task.uuid = "t1"
        task.cluster_id = "c1"
        task.node_id = "n1"
        task.function_name = JobSchedule.FN_NODE_REMOVAL
        task.status = JobSchedule.STATUS_RUNNING
        task.retry = retry
        task.max_retry = max_retry
        task.function_params = {}
        task.canceled = False
        return task

    def _process(self, task, orchestrate_result=False):
        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        set_status = MagicMock()
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate",
                          return_value=orchestrate_result), \
             patch.object(runner.storage_node_ops, "set_node_status", set_status):
            handled = runner.process_task(task)
        return handled, set_status

    def test_keeps_retrying_below_the_ceiling(self):
        task = self._task(retry=3, max_retry=100)
        handled, set_status = self._process(task)
        self.assertFalse(handled)
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)
        self.assertEqual(task.retry, 4)
        set_status.assert_not_called()

    def test_gives_up_at_the_ceiling_and_marks_removed_failed(self):
        task = self._task(retry=99, max_retry=100)
        handled, set_status = self._process(task)
        self.assertTrue(handled, "the task must end, not stay suspended")
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        set_status.assert_called_once()
        self.assertEqual(set_status.call_args.args[0], "n1")
        self.assertEqual(set_status.call_args.args[1],
                         StorageNode.STATUS_REMOVED_FAILED)
        self.assertIn("gave up", task.function_result)

    def test_an_uncapped_task_still_retries(self):
        """max_retry=-1 predates the ceiling; such a task must not be
        mistaken for one that has exhausted its budget."""
        task = self._task(retry=500, max_retry=-1)
        handled, set_status = self._process(task)
        self.assertFalse(handled)
        self.assertEqual(task.status, JobSchedule.STATUS_SUSPENDED)
        set_status.assert_not_called()

    def test_success_never_trips_the_ceiling(self):
        task = self._task(retry=99, max_retry=100)
        handled, set_status = self._process(task, orchestrate_result=True)
        self.assertTrue(handled)
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(task.function_result, "Node removed")
        set_status.assert_not_called()


class TestRemovalTaskIsBounded(unittest.TestCase):

    def test_the_ceiling_is_hours_not_passes(self):
        """A ceiling shorter than a real migration would fail removals that
        were merely slow."""
        self.assertGreaterEqual(constants.NODE_REMOVAL_MAX_WAIT_SEC, 3600)
        self.assertEqual(
            constants.NODE_REMOVAL_MAX_RETRY,
            constants.NODE_REMOVAL_MAX_WAIT_SEC // constants.TASK_EXEC_INTERVAL_SEC)


class TestRemovedFailedIsRedrivable(unittest.TestCase):

    def test_status_is_accepted_for_a_fresh_removal(self):
        src = __import__("inspect").getsource(storage_node_ops.remove_storage_node)
        self.assertIn(
            "STATUS_REMOVED_FAILED", src,
            "a removal that gave up must be re-drivable; admission has to "
            "accept the status it left the node in")

    def test_status_has_a_distinct_code(self):
        codes = StorageNode._STATUS_CODE_MAP
        self.assertIn(StorageNode.STATUS_REMOVED_FAILED, codes)
        self.assertNotEqual(codes[StorageNode.STATUS_REMOVED_FAILED],
                            codes[StorageNode.STATUS_REMOVED])


if __name__ == "__main__":
    unittest.main()
