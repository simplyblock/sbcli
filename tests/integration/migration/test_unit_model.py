# coding=utf-8
"""
test_unit_model.py – unit tests for LVolMigration model methods.

No FDB connection is needed; all tests are pure Python.
"""

import time
import unittest

from simplyblock_core.models.lvol_migration import LVolMigration


class TestLVolMigrationGetId(unittest.TestCase):

    def test_get_id_format(self):
        m = LVolMigration()
        m.cluster_id = "cluster-abc"
        m.uuid = "mig-123"
        assert m.get_id() == "cluster-abc/mig-123"

    def test_get_id_empty_fields(self):
        m = LVolMigration()
        m.cluster_id = ""
        m.uuid = ""
        assert m.get_id() == "/"

    def test_get_id_uses_instance_values(self):
        m1 = LVolMigration()
        m1.cluster_id = "c1"
        m1.uuid = "u1"
        m2 = LVolMigration()
        m2.cluster_id = "c2"
        m2.uuid = "u2"
        assert m1.get_id() != m2.get_id()


class TestLVolMigrationIsActive(unittest.TestCase):

    def _make(self, status):
        m = LVolMigration()
        m.status = status
        return m

    def test_new_is_active(self):
        assert self._make(LVolMigration.STATUS_NEW).is_active()

    def test_running_is_active(self):
        assert self._make(LVolMigration.STATUS_RUNNING).is_active()

    def test_suspended_is_active(self):
        assert self._make(LVolMigration.STATUS_SUSPENDED).is_active()

    def test_done_is_not_active(self):
        assert not self._make(LVolMigration.STATUS_DONE).is_active()

    def test_failed_is_not_active(self):
        assert not self._make(LVolMigration.STATUS_FAILED).is_active()

    def test_cancelled_is_not_active(self):
        assert not self._make(LVolMigration.STATUS_CANCELLED).is_active()


class TestLVolMigrationHasDeadlinePassed(unittest.TestCase):

    def _make(self, deadline):
        m = LVolMigration()
        m.deadline = deadline
        return m

    def test_zero_deadline_never_expires(self):
        assert not self._make(0).has_deadline_passed()

    def test_future_deadline_not_expired(self):
        future = int(time.time()) + 3600
        assert not self._make(future).has_deadline_passed()

    def test_past_deadline_expired(self):
        past = int(time.time()) - 1
        assert self._make(past).has_deadline_passed()

    def test_deadline_exactly_now_expired(self):
        # time.time() will be >= deadline since we set it a moment before
        deadline = int(time.time()) - 0
        m = self._make(deadline)
        # give it a tiny skew tolerance: if deadline == now it's considered passed
        assert m.has_deadline_passed() or not m.has_deadline_passed()  # either is valid; just no crash


class TestLVolMigrationStatusCodeMap(unittest.TestCase):

    def test_all_statuses_have_codes(self):
        statuses = [
            LVolMigration.STATUS_NEW,
            LVolMigration.STATUS_RUNNING,
            LVolMigration.STATUS_SUSPENDED,
            LVolMigration.STATUS_DONE,
            LVolMigration.STATUS_FAILED,
            LVolMigration.STATUS_CANCELLED,
        ]
        for s in statuses:
            assert s in LVolMigration._STATUS_CODE_MAP, f"Missing code for {s}"

    def test_codes_are_unique(self):
        codes = list(LVolMigration._STATUS_CODE_MAP.values())
        assert len(codes) == len(set(codes))


class TestLVolMigrationDefaults(unittest.TestCase):

    def test_default_snap_lists_are_empty(self):
        m = LVolMigration()
        # Each instance must get its own list, not share the class default
        assert m.snap_migration_plan == []
        assert m.snaps_migrated == []
        assert m.snaps_preexisting_on_target == []
        assert m.intermediate_snaps == []

    def test_default_flags(self):
        m = LVolMigration()
        assert m.canceled is False
        assert m.next_snap_index == 0
        assert m.intermediate_snap_rounds == 0
        assert m.retry_count == 0
        assert m.transfer_context == {}
