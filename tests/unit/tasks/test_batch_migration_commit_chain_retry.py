"""A retried batch cutover must not re-link intermediate snapshots the failed
attempt already committed, and a permanent RPC error must not burn the retry
budget.

Live on 2026-10-29 (61 cluster, SFAM-2840 round trip, return hop): the first
bdev_lvol_batch_transfer_final_step failed inside SPDK. Every retry then
re-ran _commit_intermediate_snapshot_chain, whose "already committed" guard
never fired. Two reasons, found one after the other on the cluster:

* it read driver_specific.lvol.is_snapshot; bdev_get_bdevs calls the flag
  ``snapshot``;
* even ``snapshot`` stays false on a converted lvol until something is cloned
  from it -- and for an intermediate snapshot that is the cutover that just
  failed. Probed directly: create, convert, bdev_get_bdevs answers
  snapshot=false, clone=false, supported_io_types.write=false.

bdev_lvol_add_clone on the already-linked LVS_1/SNAP_20m answered "File
exists" on all five retries, and the group reached cleanup_target only after
the whole budget, with the SPDK failure that started it buried under the
ceiling message.

The bdev fixtures below copy the shapes a live SPDK returned, not the shape
the code expected -- the previous mocks used is_snapshot=True for a converted
bdev and so agreed with the bug.

Pure logic tests: DB, RPC clients and events are mocked.
"""
import errno
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_batch_migration as runner
from simplyblock_core import constants
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError

TGT_LVS = "LVS_1"


def _bdev(*, converted, base_snapshot=None):
    """bdev_get_bdevs entry as a live SPDK returns it (only the keys read here).

    A converted lvol nothing is cloned from yet still reports snapshot=false;
    only its write support goes away."""
    lvol = {"lvol_store_uuid": "c522c36d", "snapshot": False,
            "clone": base_snapshot is not None,
            "lvol_leadership": True, "lvs_leadership": True}
    if base_snapshot:
        lvol["base_snapshot"] = base_snapshot
    return {"name": "u", "supported_io_types": {"read": True, "write": not converted},
            "driver_specific": {"lvol": lvol}}


def _snap(uuid, short):
    s = MagicMock()
    s.uuid = uuid
    s.short = short
    return s


class TestCommitChainSkipsCommittedSnapshots(unittest.TestCase):

    def setUp(self):
        self.snaps = {
            "pred": _snap("pred", "SNAP_14m"),
            "i0": _snap("i0", "SNAP_20m"),
            "i1": _snap("i1", "SNAP_23m"),
        }
        self.db = MagicMock()
        self.db.get_snapshot_by_id.side_effect = lambda u: self.snaps[u]
        self.tgt_node = MagicMock()
        self.tgt_node.lvstore = TGT_LVS
        self.tgt_rpc = MagicMock()
        self.member = MagicMock()
        self.member.uuid = "mig-1"
        self.member.intermediate_snaps = ["i0", "i1"]
        self.member.snaps_transferred_group = ["pred"]
        self.member.snaps_preexisting_on_target = []
        self.group = MagicMock()
        self.group.uuid = "group-1"
        for p in (
            patch.object(runner, "db", self.db),
            patch.object(runner, "_snap_tgt_short_name", side_effect=lambda s: s.short),
            patch.object(runner, "_get_target_secondary_node", return_value=(None, None)),
            patch.object(runner, "_get_target_tertiary_node", return_value=(None, None)),
            patch("simplyblock_core.controllers.lvol_controller.is_node_leader", return_value=True),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _commit(self):
        return runner._commit_intermediate_snapshot_chain(
            self.group, [self.member], self.tgt_node, self.tgt_rpc)

    def test_retry_after_full_commit_touches_nothing(self):
        self.tgt_rpc.bdev_get.return_value = _bdev(converted=True, base_snapshot="SNAP_14m")
        self.assertIsNone(self._commit())
        self.tgt_rpc.bdev_lvol_add_clone.assert_not_called()
        self.tgt_rpc.bdev_lvol_convert.assert_not_called()

    def test_retry_resumes_after_the_last_committed_snapshot(self):
        # A new intermediate round appended SNAP_23m after SNAP_20m was committed.
        self.tgt_rpc.bdev_get.side_effect = lambda name: (
            _bdev(converted=True, base_snapshot="SNAP_14m") if name.endswith("SNAP_20m")
            else _bdev(converted=False))
        self.assertIsNone(self._commit())
        self.tgt_rpc.bdev_lvol_add_clone.assert_called_once_with(
            f"{TGT_LVS}/SNAP_23m", f"{TGT_LVS}/SNAP_20m")
        self.tgt_rpc.bdev_lvol_convert.assert_called_once_with(f"{TGT_LVS}/SNAP_23m")

    def test_retry_between_link_and_convert_only_converts(self):
        self.member.intermediate_snaps = ["i0"]
        self.tgt_rpc.bdev_get.return_value = _bdev(converted=False, base_snapshot="SNAP_14m")
        self.assertIsNone(self._commit())
        self.tgt_rpc.bdev_lvol_add_clone.assert_not_called()
        self.tgt_rpc.bdev_lvol_convert.assert_called_once_with(f"{TGT_LVS}/SNAP_20m")

    def test_converted_bdev_with_no_clones_reads_as_immutable(self):
        self.assertTrue(runner._bdev_is_immutable_snapshot(_bdev(converted=True)))
        self.assertFalse(runner._bdev_is_immutable_snapshot(_bdev(converted=False)))

    def test_first_attempt_links_the_whole_chain(self):
        self.tgt_rpc.bdev_get.return_value = _bdev(converted=False)
        self.assertIsNone(self._commit())
        self.assertEqual(
            [c.args for c in self.tgt_rpc.bdev_lvol_add_clone.call_args_list],
            [(f"{TGT_LVS}/SNAP_20m", f"{TGT_LVS}/SNAP_14m"),
             (f"{TGT_LVS}/SNAP_23m", f"{TGT_LVS}/SNAP_20m")])


class TestPermanentRpcErrorSkipsRetryBudget(unittest.TestCase):

    def setUp(self):
        self.task = MagicMock()
        self.task.uuid = "task-1"
        self.task.cluster_id = "cl-1"
        self.task.retry = 0
        self.task.status = JobSchedule.STATUS_RUNNING
        self.task.function_params = {"group_id": "grp-1"}

        self.group = LVolMigrationGroup()
        self.group.uuid = "grp-1"
        self.group.cluster_id = "cl-1"
        self.group.source_node_id = "src-1"
        self.group.target_node_id = "tgt-1"
        self.group.phase = LVolMigrationGroup.PHASE_INTERMEDIATE
        self.group.members = [{"ns_id": 1, "migration_id": "mig-1"}]
        self.group.write_to_db = MagicMock()

        node = MagicMock()
        node.status = StorageNode.STATUS_ONLINE
        node.get_id.return_value = "src-1"
        cluster = MagicMock()
        cluster.status = Cluster.STATUS_ACTIVE
        worker = MagicMock()
        worker.has_deadline_passed.return_value = False

        self.db = MagicMock()
        self.db.get_task_by_id.return_value = self.task
        self.db.get_migration_group_by_id.return_value = self.group
        self.db.get_storage_node_by_id.return_value = node
        self.db.get_cluster_by_id.return_value = cluster
        self.db.get_migration_by_id.return_value = worker
        for p in (
            patch.object(runner, "db", self.db),
            patch.object(runner, "_group_source_nodes", return_value=(node, node)),
            patch.object(runner, "_make_rpc", return_value=MagicMock()),
            patch.object(runner, "tasks_controller"),
            patch.object(runner, "migration_events"),
        ):
            p.start()
            self.addCleanup(p.stop)
        runner.tasks_controller.get_active_cluster_expand_task.return_value = None

    def _run_with(self, exc):
        with patch.object(runner, "_handle_intermediate_barrier", side_effect=exc):
            return runner.task_runner(self.task)

    def test_eexist_goes_straight_to_cleanup_target(self):
        # The answer a repeated bdev_lvol_add_clone gave on the cluster: the
        # errno only in the message, under JSON-RPC's generic invalid-params code.
        self._run_with(RPCRemoteError("File exists", RPCErrorCode.invalid_params))
        self.assertEqual(self.group.phase, LVolMigrationGroup.PHASE_CLEANUP_TARGET)
        self.assertEqual(self.task.retry, 0)
        self.assertIn("permanent RPC error", self.group.error_message)
        self.assertIn("File exists", self.group.error_message)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)

    def test_transient_error_still_charges_the_budget(self):
        self._run_with(RPCRemoteError("Device or resource busy", -errno.EBUSY))
        self.assertEqual(self.group.phase, LVolMigrationGroup.PHASE_INTERMEDIATE)
        self.assertEqual(self.task.retry, 1)
        self.assertEqual(self.task.status, JobSchedule.STATUS_SUSPENDED)

    def test_eexist_as_a_negative_errno_code_is_permanent_too(self):
        self._run_with(RPCRemoteError("File exists", -errno.EEXIST))
        self.assertEqual(self.group.phase, LVolMigrationGroup.PHASE_CLEANUP_TARGET)
        self.assertEqual(self.task.retry, 0)

    def test_transient_error_reaches_cleanup_at_the_ceiling(self):
        self.task.retry = constants.LVOL_MIG_MAX_RETRIES - 1
        self._run_with(RPCRemoteError("Device or resource busy", -errno.EBUSY))
        self.assertEqual(self.group.phase, LVolMigrationGroup.PHASE_CLEANUP_TARGET)
        self.assertIn("max retry", self.group.error_message)


class TestRemoteErrorErrno(unittest.TestCase):

    def test_errno_from_a_negative_errno_code(self):
        self.assertEqual(RPCRemoteError("No such device", -errno.ENODEV).posix_errno, errno.ENODEV)

    def test_errno_from_the_message_under_a_generic_code(self):
        self.assertEqual(
            RPCRemoteError("File exists", RPCErrorCode.invalid_params).posix_errno, errno.EEXIST)

    def test_no_errno_for_free_text(self):
        self.assertIsNone(
            RPCRemoteError("lvol is busy migrating", RPCErrorCode.internal_error).posix_errno)

    def test_invalid_state_code_is_not_read_as_eperm(self):
        self.assertIsNone(RPCRemoteError("bad state", RPCErrorCode.invalid_state).posix_errno)


if __name__ == "__main__":
    unittest.main()
