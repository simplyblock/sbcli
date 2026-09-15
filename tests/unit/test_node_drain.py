"""Volume drain: migrate a node's volumes off it as part of removing it.

Volumes used to be a precondition -- removal refused while any were present and
the operator migrated them separately. That cannot work for an OFFLINE node,
whose volumes nobody can read from their primary, so the removal drains them
itself. Standalone `volume migrate` is unchanged; removal is simply another
caller of the same entry points.

Escalation, per unit: retry the same target up to
NODE_DRAIN_MAX_RESTARTS_PER_TARGET times, then move to the next candidate, and
only when no candidate is left does the removal fail (RemovalGaveUp ->
REMOVED_FAILED).
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode


def _lvol(lvol_id, nqn="", size=1024, ns_per_subsys=1):
    lv = MagicMock()
    lv.get_id.return_value = lvol_id
    lv.uuid = lvol_id
    lv.nqn = nqn
    lv.size = size
    lv.status = "online"
    lv.max_namespace_per_subsys = ns_per_subsys
    return lv


def _node(node_id="n1"):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = "c1"
    return node


class _Cursor(storage_node_ops.RemovalCursor):
    def __init__(self):
        super().__init__(None)


class TestGrouping(unittest.TestCase):
    """Volumes sharing a subsystem migrate together; batch cannot be split."""

    def test_volumes_sharing_an_nqn_form_one_unit(self):
        db = MagicMock()
        db.get_lvols_by_node_id.return_value = [
            _lvol("a", nqn="nqn:shared"), _lvol("b", nqn="nqn:shared")]
        units = storage_node_ops._node_drain_units(_node(), db)
        self.assertEqual(len(units), 1)
        self.assertEqual(len(next(iter(units.values()))), 2)

    def test_volumes_with_distinct_nqns_are_separate_units(self):
        db = MagicMock()
        db.get_lvols_by_node_id.return_value = [
            _lvol("a", nqn="nqn:one"), _lvol("b", nqn="nqn:two")]
        self.assertEqual(len(storage_node_ops._node_drain_units(_node(), db)), 2)

    def test_a_volume_being_deleted_is_not_drained(self):
        lv = _lvol("a", nqn="nqn:one")
        lv.status = "in_deletion"
        db = MagicMock()
        db.get_lvols_by_node_id.return_value = [lv]
        self.assertEqual(storage_node_ops._node_drain_units(_node(), db), {})


class TestTargetSelection(unittest.TestCase):

    def test_excludes_the_departing_node_and_everything_already_tried(self):
        picked = {}

        def fake_pick(cluster_id, size, namespaced=False, exclude_ids=None):
            picked["exclude"] = set(exclude_ids or [])
            return ["n5"]

        with patch.object(storage_node_ops.lvol_controller,
                          "_get_next_3_nodes", side_effect=fake_pick):
            target = storage_node_ops._pick_drain_target(
                _node("n1"), _lvol("a"), ["n2", "n3"], MagicMock())

        self.assertEqual(target, "n5")
        self.assertEqual(picked["exclude"], {"n1", "n2", "n3"})

    def test_returns_none_when_no_candidate_is_left(self):
        with patch.object(storage_node_ops.lvol_controller,
                          "_get_next_3_nodes", return_value=[]):
            self.assertIsNone(storage_node_ops._pick_drain_target(
                _node(), _lvol("a"), [], MagicMock()))


class TestDrainLoop(unittest.TestCase):

    def setUp(self):
        self.node = _node("n1")
        self.db = MagicMock()
        self.cursor = _Cursor()

    def _drain(self, lvols, migration_status=None, target="n2"):
        self.db.get_lvols_by_node_id.return_value = lvols
        mig = MagicMock()
        mig.status = migration_status
        self.db.get_migration_by_id.return_value = mig
        self.db.get_migration_group_by_id.return_value = mig
        with patch.object(storage_node_ops, "_pick_drain_target", return_value=target), \
             patch.object(storage_node_ops, "migration_controller") as mc:
            mc.create_migration.return_value = ("mig-1", None)
            mc.create_batch_migration.return_value = ("grp-1", None)
            ret = storage_node_ops._drain_lvols_from_node(
                self.node, self.cursor, self.db)
        return ret, mc

    def test_a_node_with_no_volumes_is_already_drained(self):
        ret, mc = self._drain([])
        self.assertTrue(ret)
        mc.create_migration.assert_not_called()

    def test_starts_a_single_migration_for_a_lone_volume(self):
        ret, mc = self._drain([_lvol("a", nqn="nqn:one")])
        self.assertFalse(ret, "drain is not finished the moment it is started")
        mc.create_migration.assert_called_once()
        mc.start_migration.assert_called_once()
        mc.create_batch_migration.assert_not_called()

    def test_starts_a_batch_migration_for_a_shared_subsystem(self):
        ret, mc = self._drain([_lvol("a", nqn="nqn:s"), _lvol("b", nqn="nqn:s")])
        self.assertFalse(ret)
        mc.create_batch_migration.assert_called_once()
        mc.start_batch_migration.assert_called_once()
        mc.create_migration.assert_not_called()

    def test_a_running_migration_is_not_reissued(self):
        """The whole point of keeping state on the cursor: a retry polls what
        is already in flight instead of starting it again."""
        self._drain([_lvol("a", nqn="nqn:one")])
        _, mc = self._drain([_lvol("a", nqn="nqn:one")],
                            migration_status=LVolMigration.STATUS_RUNNING)
        mc.create_migration.assert_not_called()

    def test_a_failed_migration_is_retried_on_the_same_target(self):
        self._drain([_lvol("a", nqn="nqn:one")])
        self._drain([_lvol("a", nqn="nqn:one")],
                    migration_status=LVolMigration.STATUS_FAILED)
        state = self.cursor.data["drain"]["nqn:one"]
        self.assertEqual(state["restarts"], 1)
        self.assertEqual(state["tried"], [], "the target is not abandoned yet")

    def test_the_target_is_abandoned_after_the_configured_restarts(self):
        self._drain([_lvol("a", nqn="nqn:one")])
        state = self.cursor.data["drain"]["nqn:one"]
        state["restarts"] = constants.NODE_DRAIN_MAX_RESTARTS_PER_TARGET - 1
        self._drain([_lvol("a", nqn="nqn:one")],
                    migration_status=LVolMigration.STATUS_FAILED)
        self.assertIn("n2", state["tried"])
        self.assertEqual(state["restarts"], 0, "the budget resets per target")

    def test_gives_up_when_every_target_is_exhausted(self):
        with patch.object(storage_node_ops, "_pick_drain_target", return_value=None), \
             patch.object(storage_node_ops, "migration_controller"):
            self.db.get_lvols_by_node_id.return_value = [_lvol("a", nqn="nqn:one")]
            with self.assertRaises(storage_node_ops.RemovalGaveUp):
                storage_node_ops._drain_lvols_from_node(
                    self.node, self.cursor, self.db)

    def test_a_vanished_migration_record_counts_as_failed(self):
        """A migration that left no trace did not demonstrably move anything;
        treating it as success would drop a volume on the floor."""
        self._drain([_lvol("a", nqn="nqn:one")])
        self.db.get_migration_by_id.side_effect = KeyError("gone")
        with patch.object(storage_node_ops, "_pick_drain_target", return_value="n2"), \
             patch.object(storage_node_ops, "migration_controller"):
            self.db.get_lvols_by_node_id.return_value = [_lvol("a", nqn="nqn:one")]
            storage_node_ops._drain_lvols_from_node(self.node, self.cursor, self.db)
        self.assertEqual(self.cursor.data["drain"]["nqn:one"]["restarts"], 1)


class TestGiveUpIsTerminal(unittest.TestCase):

    def test_the_runner_turns_it_into_removed_failed(self):
        from simplyblock_core.models.job_schedule import JobSchedule
        from simplyblock_core.services import tasks_runner_node_removal as runner

        task = JobSchedule()
        task.uuid = "t1"
        task.cluster_id = "c1"
        task.node_id = "n1"
        task.function_name = JobSchedule.FN_NODE_REMOVAL
        task.status = JobSchedule.STATUS_RUNNING
        task.function_params = {}
        task.canceled = False
        task.retry = 0
        task.max_retry = 100

        cluster = MagicMock()
        cluster.status = "active"
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster

        with patch.object(runner, "db", db), \
             patch.object(runner.storage_node_ops, "node_removal_orchestrate",
                          side_effect=storage_node_ops.RemovalGaveUp("no target left")), \
             patch.object(runner.storage_node_ops, "set_node_status") as set_status:
            handled = runner.process_task(task)

        self.assertTrue(handled, "the task ends rather than retrying for ever")
        self.assertEqual(task.status, JobSchedule.STATUS_DONE)
        self.assertEqual(set_status.call_args.args[1],
                         StorageNode.STATUS_REMOVED_FAILED)
        self.assertIn("no target left", task.function_result)


if __name__ == "__main__":
    unittest.main()


class TestDeviceWorkSplitFromJmWork(unittest.TestCase):
    """Devices migrate early; the JM stays at phase 2.

    Both used to live in one function, so moving the device work ahead of the
    volume drain would have dragged the JM decommission with it -- ahead of
    phase 3a, which tears down this node's own hosted replicas. A peer holding
    such a replica runs a JC instance naming the dying JM, and jc_replace_jm's
    -17 check rejects the whole batch while it is live (2026-08-25, 2026-09-02).
    """

    def test_the_early_device_step_leaves_the_jm_alone(self):
        node = _node()
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        node.nvme_devices = []
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", MagicMock()), \
             patch.object(storage_node_ops, "_decommission_node_jm") as jm:
            storage_node_ops._fail_and_migrate_node_devices(node)
        jm.assert_not_called()

    def test_the_phase_5_wrapper_still_re_runs_the_jm_defensively(self):
        node = _node()
        node.nvme_devices = []
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", MagicMock()), \
             patch.object(storage_node_ops, "_decommission_node_jm") as jm:
            storage_node_ops._decommission_node_devices(node)
        jm.assert_called_once()

    def test_devices_are_migrated_before_volumes_are_drained(self):
        """The order the data path wants, and the order asked for."""
        src = __import__("inspect").getsource(
            storage_node_ops.node_removal_orchestrate)
        dev = src.index('cursor.enter("migrate_devices"')
        drain = src.index('cursor.enter("drain_lvols"')
        teardown = src.index('cursor.enter("teardown_own_replicas"')
        jm = src.index('cursor.enter("decommission_jm"')
        self.assertLess(dev, drain, "devices must migrate before the volume drain")
        self.assertLess(drain, teardown, "nothing may be torn down before the drain")
        self.assertLess(teardown, jm,
                        "phase 3a must still precede the JM decommission")


class TestClusterStaysActiveUntilTeardown(unittest.TestCase):
    """The drain cannot run against a cluster the removal has already marked
    as shrinking.

    migration_controller refuses to create or start a migration unless the
    cluster is ACTIVE. The removal used to set IN_SHRINK at entry, covering the
    whole orchestration -- so the drain asked for a migration the removal's own
    bookkeeping had just made impossible: "Cluster ... is not active
    (status=in_shrink)", 52 retries before it was caught (2026-09-15).

    Nothing before the teardown is destructive, so IN_SHRINK belongs to the
    phases that actually dismantle the node.
    """

    def _source(self):
        return __import__("inspect").getsource(
            storage_node_ops.node_removal_orchestrate)

    def test_shrink_is_marked_after_the_drain_not_before(self):
        src = self._source()
        shrink = src.index("Cluster.STATUS_IN_SHRINK")
        drain = src.index('cursor.enter("drain_lvols"')
        devices = src.index('cursor.enter("migrate_devices"')
        self.assertGreater(
            shrink, drain,
            "IN_SHRINK must be set after the volume drain -- migration is "
            "refused while the cluster is not ACTIVE")
        self.assertGreater(shrink, devices)

    def test_the_status_is_only_restored_if_it_was_set(self):
        """Early returns happen before the marker now, so the finally must not
        restore a status it never changed."""
        src = self._source()
        self.assertIn("if shrink_marked:", src)


class TestDrainTargetExcludesTheActingSource(unittest.TestCase):
    """The drain runs after the node is down, so the migration reads from a
    replica instead. That replica looks like an ideal target -- ONLINE, has
    capacity -- but picking it makes source and destination the same node and
    create_migration refuses:

        Cannot migrate to node d5785628: source primary a1b050f1 is offline
        and d5785628 is currently serving as the fallback source for this
        volume

    Observed on cluster a6e7569d (2026-09-15): the drain proposed the acting
    source on every pass and could never start. Excluded by asking
    migration_controller.resolve_source_node, not by re-deriving which replica
    it would choose.
    """

    def _pick(self, candidates, source_node_id, tried=()):
        snode = MagicMock()
        snode.get_id.return_value = "departing"
        snode.cluster_id = "c1"
        lvol = MagicMock()
        lvol.size = 1024
        lvol.max_namespace_per_subsys = 1
        acting = MagicMock()
        acting.get_id.return_value = source_node_id
        with patch.object(storage_node_ops.lvol_controller, "_get_next_3_nodes",
                          return_value=list(candidates)), \
             patch.object(storage_node_ops.migration_controller,
                          "resolve_source_node", return_value=acting):
            return storage_node_ops._pick_drain_target(
                snode, lvol, list(tried), MagicMock())

    def test_the_acting_source_is_not_offered(self):
        self.assertEqual(self._pick(["d5785628", "b30f8f0c"], "d5785628"),
                         "b30f8f0c")

    def test_a_normal_candidate_is_still_offered(self):
        self.assertEqual(self._pick(["b30f8f0c", "c968ea0b"], "d5785628"),
                         "b30f8f0c")

    def test_already_tried_and_the_source_are_both_excluded(self):
        self.assertEqual(
            self._pick(["d5785628", "b30f8f0c", "c968ea0b"], "d5785628",
                       tried=["b30f8f0c"]),
            "c968ea0b")

    def test_exhausting_the_candidates_returns_none(self):
        self.assertIsNone(self._pick(["d5785628"], "d5785628"))

    def test_the_departing_node_is_still_excluded(self):
        self.assertIsNone(self._pick(["departing"], "d5785628"))

    def test_no_online_replica_does_not_break_the_pick(self):
        """resolve_source_node raises when nothing can serve as source;
        create_migration will report that properly, so just don't exclude."""
        snode = MagicMock()
        snode.get_id.return_value = "departing"
        snode.cluster_id = "c1"
        lvol = MagicMock()
        lvol.size = 1024
        lvol.max_namespace_per_subsys = 1
        with patch.object(storage_node_ops.lvol_controller, "_get_next_3_nodes",
                          return_value=["b30f8f0c"]), \
             patch.object(storage_node_ops.migration_controller,
                          "resolve_source_node", side_effect=ValueError("none")):
            self.assertEqual(
                storage_node_ops._pick_drain_target(snode, lvol, [], MagicMock()),
                "b30f8f0c")
