"""Three findings from one batch removal (2026-09-28, run 8):

1. The removal gate counted a snapshot whose embedded volume copy still named
   the node although the live volume had migrated away, and refused the
   removal of a node holding nothing -- for a migration-internal intermediate
   record at that.
2. That record was left behind because the source cleanup routed the delete
   by the fallback source's own lvstore, which matched nothing, and skipped it.
3. Two concurrent creates for one subsystem (the operator re-submitting after
   its client timeout, to the other API replica) built two groups sharing the
   same member migrations; the cleanup of the one that failed tore the other's
   target bdevs down. The create now reserves the NQN first.
"""
import inspect
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import migration_controller as ctl
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
import simplyblock_core.services.tasks_runner_lvol_migration as runner


def _snap(name, lvol_uuid="lvol-1", embedded_node="n1"):
    s = MagicMock()
    s.snap_name = name
    s.deleted = False
    s.lvol = MagicMock()
    s.lvol.uuid = lvol_uuid
    s.lvol.node_id = embedded_node
    return s


class TestRemovalGateFollowsTheLiveVolume(unittest.TestCase):

    def _db(self, live_node):
        db = MagicMock()
        lvol = MagicMock()
        lvol.node_id = live_node
        db.get_lvol_by_id.return_value = lvol
        return db

    def test_a_snapshot_of_a_volume_that_left_does_not_hold_the_node(self):
        self.assertFalse(storage_node_ops._snapshot_lives_on_node(
            _snap("snap-user", embedded_node="n1"), "n1", self._db(live_node="n2")))

    def test_a_snapshot_of_a_volume_still_here_does(self):
        self.assertTrue(storage_node_ops._snapshot_lives_on_node(
            _snap("snap-user", embedded_node="n1"), "n1", self._db(live_node="n1")))

    def test_a_migration_intermediate_record_never_does(self):
        self.assertFalse(storage_node_ops._snapshot_lives_on_node(
            _snap("_mig_5f523044_r2", embedded_node="n1"), "n1", self._db(live_node="n1")))

    def test_without_a_live_record_the_embedded_copy_decides(self):
        db = MagicMock()
        db.get_lvol_by_id.side_effect = KeyError("gone")
        self.assertTrue(storage_node_ops._snapshot_lives_on_node(
            _snap("snap-user", embedded_node="n1"), "n1", db))


class TestSourceCleanupRoutesByThePrimaryLvstore(unittest.TestCase):

    def test_intermediate_snapshot_deletes_use_the_primary_lvstore(self):
        body = inspect.getsource(runner._handle_cleanup_source)
        self.assertIn("src_lvs_name=(primary_src_node or src_node).lvstore", body)
        self.assertNotIn("src_lvs_name=src_node.lvstore", body)


def _group(members, age_s=0, phase=LVolMigrationGroup.PHASE_PRE_CREATED):
    g = LVolMigrationGroup()
    g.uuid = "group-1"
    g.members = members
    g.phase = phase
    g.create_dt = str(datetime.now() - timedelta(seconds=age_s))
    return g


class TestBatchCreateReservesTheSubsystem(unittest.TestCase):

    def test_a_fresh_memberless_group_is_a_create_in_flight(self):
        self.assertTrue(ctl._batch_reservation_in_flight(_group([], age_s=5)))
        self.assertFalse(ctl._batch_reservation_is_stale(_group([], age_s=5)))

    def test_an_old_memberless_group_is_a_stale_reservation(self):
        old = _group([], age_s=ctl._BATCH_RESERVATION_MAX_AGE_S + 1)
        self.assertFalse(ctl._batch_reservation_in_flight(old))
        self.assertTrue(ctl._batch_reservation_is_stale(old))

    def test_a_group_with_members_is_neither(self):
        g = _group([{"ns_id": 1, "migration_id": "m1"}], age_s=5)
        self.assertFalse(ctl._batch_reservation_in_flight(g))
        self.assertFalse(ctl._batch_reservation_is_stale(g))

    def test_an_unparsable_date_counts_as_stale_not_in_flight(self):
        g = _group([], age_s=0)
        g.create_dt = "not a date"
        self.assertFalse(ctl._batch_reservation_in_flight(g))
        self.assertTrue(ctl._batch_reservation_is_stale(g))

    def test_the_reservation_is_written_before_any_member_is_created(self):
        body = inspect.getsource(ctl.create_batch_migration)
        reserve = body.index("reservation.write_to_db(db_inst.kv_store)")
        first_member = body.index("migration_id, connect_strings = create_migration(")
        self.assertLess(reserve, first_member)
        self.assertIn("if _batch_reservation_in_flight(g):", body)
        self.assertIn("data migration in progress", body)  # the operator's retry wording
        self.assertIn("group = reservation", body)


if __name__ == "__main__":
    unittest.main()
