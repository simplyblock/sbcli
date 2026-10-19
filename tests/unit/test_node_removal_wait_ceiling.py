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


class TestRemovalWaitCeiling(unittest.TestCase):
    """The exit, once the wait is real.

    On the shared task driver an unfinished pass is progress, not a failed
    attempt (TaskProgress, no retry spent), so the ceiling is the time a
    removal has kept coming back unfinished, not a retry count."""

    def _task(self, params=None):
        task = JobSchedule()
        task.uuid = "t1"
        task.cluster_id = "c1"
        task.node_id = "n1"
        task.function_name = JobSchedule.FN_NODE_REMOVAL
        task.status = JobSchedule.STATUS_RUNNING
        task.retry = 0
        task.max_retry = constants.NODE_REMOVAL_MAX_RETRY
        task.function_params = dict(params or {})
        task.canceled = False
        return task.frozen_view()

    def _process(self, task, orchestrate_result=False):
        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        set_status = MagicMock()
        checkpoints = []
        results = []
        raised = None
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate",
                          return_value=orchestrate_result), \
             patch.object(runner.storage_node_ops, "set_node_status", set_status), \
             patch.object(runner, "checkpoint",
                          side_effect=lambda t, **p: checkpoints.append(p) or t), \
             patch.object(runner, "set_result",
                          side_effect=lambda t, m: results.append(m) or t):
            try:
                runner.process_task(task)
            except Exception as e:  # noqa: BLE001 - the signal is the outcome
                raised = e
        return raised, set_status, checkpoints, results

    def test_an_unfinished_pass_is_progress_and_starts_the_clock(self):
        raised, set_status, checkpoints, _ = self._process(self._task())
        self.assertIsInstance(raised, runner.TaskProgress)
        self.assertIn(runner.INCOMPLETE_SINCE_KEY, checkpoints[-1])
        set_status.assert_not_called()

    def test_keeps_waiting_below_the_ceiling(self):
        since = runner.time.time() - constants.NODE_REMOVAL_MAX_WAIT_SEC / 2
        raised, set_status, _, _ = self._process(
            self._task({runner.INCOMPLETE_SINCE_KEY: since}))
        self.assertIsInstance(raised, runner.TaskProgress)
        set_status.assert_not_called()

    def test_gives_up_at_the_ceiling_and_marks_removed_failed(self):
        since = runner.time.time() - constants.NODE_REMOVAL_MAX_WAIT_SEC - 1
        raised, set_status, _, _ = self._process(
            self._task({runner.INCOMPLETE_SINCE_KEY: since}))
        self.assertIsInstance(raised, runner.TaskAbort, "the task must end, not keep waiting")
        self.assertIn("gave up", str(raised))
        set_status.assert_called_once()
        self.assertEqual(set_status.call_args.args[0], "n1")
        self.assertEqual(set_status.call_args.args[1], StorageNode.STATUS_REMOVED_FAILED)

    def test_success_never_trips_the_ceiling(self):
        since = runner.time.time() - constants.NODE_REMOVAL_MAX_WAIT_SEC - 1
        raised, set_status, _, results = self._process(
            self._task({runner.INCOMPLETE_SINCE_KEY: since}), orchestrate_result=True)
        self.assertIsNone(raised)
        self.assertEqual(results, ["Node removed"])
        set_status.assert_not_called()

    def test_a_step_that_gives_up_ends_the_task_as_removed_failed(self):
        task = self._task()
        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate",
                          side_effect=storage_node_ops.RemovalGaveUp("no target")), \
             patch.object(runner.storage_node_ops, "set_node_status") as set_status, \
             patch.object(runner, "checkpoint", side_effect=lambda t, **p: t):
            with self.assertRaises(runner.TaskAbort) as ctx:
                runner.process_task(task)
        self.assertIn("no target", str(ctx.exception))
        self.assertEqual(set_status.call_args.args[1], StorageNode.STATUS_REMOVED_FAILED)

    def test_the_cursor_goes_through_checkpoint_not_the_frozen_task(self):
        task = self._task()
        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        checkpoints = []

        def orchestrate(node_id, force_remove=False, cursor=None):
            cursor.enter("drain_lvols", "drain")
            cursor.enter("drain_lvols", "drain again")
            return True

        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate", side_effect=orchestrate), \
             patch.object(runner, "checkpoint", side_effect=lambda t, **p: checkpoints.append(p) or t), \
             patch.object(runner, "set_result"):
            runner.process_task(task)
        steps = [c["step"] for c in checkpoints if "step" in c]
        self.assertEqual(steps, ["drain_lvols"], "an unchanged position is not re-written")


class TestOnFinish(unittest.TestCase):
    """A task that ended without removing its node -- retry ceiling, cancel --
    must not strand the node mid-removal."""

    def _finish(self, node_status):
        node = MagicMock()
        node.status = node_status
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        task = MagicMock()
        task.node_id = "n1"
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "set_node_status") as set_status:
            runner._on_finish(task)
        return set_status

    def test_a_node_still_mid_removal_is_marked_removed_failed(self):
        for status in StorageNode.REMOVAL_IN_PROGRESS_STATUSES:
            with self.subTest(status=status):
                set_status = self._finish(status)
                set_status.assert_called_once_with(
                    "n1", StorageNode.STATUS_REMOVED_FAILED, caused_by="remove")

    def test_a_finished_removal_is_left_alone(self):
        for status in (StorageNode.STATUS_REMOVED, StorageNode.STATUS_REMOVED_FAILED):
            with self.subTest(status=status):
                self._finish(status).assert_not_called()

    def test_it_is_wired(self):
        self.assertIs(runner.SPEC.on_finish, runner._on_finish)


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



class TestCeilingIsWired(unittest.TestCase):
    """The runner enforces `0 < max_retry <= retry`, so a task queued with
    max_retry=-1 has no ceiling at all. That is how it was queued while the
    ceiling above was written and tested: the test came across the backport,
    the wiring did not (2026-09-28 review)."""

    def test_the_removal_task_is_queued_with_the_ceiling(self):
        from simplyblock_core.controllers import tasks_controller
        with patch.object(tasks_controller, "_add_task", return_value="t1") as add:
            tasks_controller.add_node_removal_task("c1", "n1", {"force_remove": False})
        self.assertEqual(add.call_args.kwargs.get("max_retry"), constants.NODE_REMOVAL_MAX_RETRY)
        self.assertGreater(constants.NODE_REMOVAL_MAX_RETRY, 0)


class TestInActivationStampIsForwardOnly(unittest.TestCase):
    """While the cluster activates, the runner parks the task and marks the
    node as pending removal -- but only a node that has not started leaving.
    Stamping unconditionally rewound a node past pending_removal, up to and
    including one already REMOVED, into the first step of a removal the
    orchestrator then re-ran on a finalized node (2026-09-28 review)."""

    def _park(self, node_status):
        task = JobSchedule()
        task.uuid, task.cluster_id, task.node_id = "t1", "c1", "n1"
        task.function_name = JobSchedule.FN_NODE_REMOVAL
        task.status = JobSchedule.STATUS_RUNNING
        task.retry, task.max_retry, task.function_params, task.canceled = 0, 100, {}, False
        cluster = MagicMock()
        cluster.status = "in_activation"
        node = MagicMock()
        node.status = node_status
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_node_by_id.return_value = node
        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "set_node_status") as stamp, \
             self.assertRaises(runner.TaskDefer):
            runner.process_task(task.frozen_view())
        return stamp

    def test_a_node_not_yet_departing_is_marked_pending(self):
        stamp = self._park(StorageNode.STATUS_ONLINE)
        stamp.assert_called_once_with("n1", StorageNode.STATUS_PENDING_REMOVAL, caused_by="remove")

    def test_a_node_already_on_its_way_out_keeps_its_place(self):
        for status in (StorageNode.STATUS_MIGRATING_DEVICES, StorageNode.STATUS_MIGRATING_LVOLS,
                       StorageNode.STATUS_IN_REMOVAL, StorageNode.STATUS_REMOVED):
            with self.subTest(status=status):
                self._park(status).assert_not_called()

if __name__ == "__main__":
    unittest.main()
