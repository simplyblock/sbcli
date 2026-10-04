"""A repair skipped because its owner is mid-transition must be deferred.

2026-09-01, node 22f365ef: "Multipath repair skipped ... owner is in_restart"
was logged 15 times while the two peers the soak had taken down restarted, and
the repair was then never retried once they came back -- re-detection depends
on the remote bdev still looking present and the device still reading ONLINE,
which it did not once the controller was half torn down. The controller sat
single-pathed, its sibling stuck in "deleting", until the node was restarted
20 minutes later.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.services import health_check_service as hcs


@pytest.fixture(autouse=True)
def _clear_registry():
    hcs._deferred_repairs.clear()
    yield
    hcs._deferred_repairs.clear()


def _owner(status):
    n = MagicMock()
    n.status = status
    return n


class TestDefer:
    def test_skipped_repair_is_recorded(self):
        hcs._defer_repair("owner-1", "JM", "remote_jm_x", "dev-1", "target-1")
        assert hcs._deferred_repairs["owner-1"] == {
            ("JM", "remote_jm_x", "dev-1", "target-1")}

    def test_recording_is_idempotent(self):
        for _ in range(5):
            hcs._defer_repair("owner-1", "JM", "remote_jm_x", "dev-1", "target-1")
        assert len(hcs._deferred_repairs["owner-1"]) == 1

    def test_incomplete_entries_are_ignored(self):
        hcs._defer_repair(None, "JM", "c", "d", "t")
        hcs._defer_repair("o", "JM", None, "d", "t")
        hcs._defer_repair("o", "JM", "c", "d", None)
        assert hcs._deferred_repairs == {}

    def test_distinct_controllers_are_kept_separately(self):
        hcs._defer_repair("owner-1", "JM", "ctrl_a", "dev-a", "target-1")
        hcs._defer_repair("owner-1", "device", "ctrl_b", "dev-b", "target-1")
        assert len(hcs._deferred_repairs["owner-1"]) == 2


class TestDrain:
    def test_not_drained_while_owner_still_in_restart(self):
        hcs._defer_repair("owner-1", "JM", "remote_jm_x", "dev-1", "target-1")
        with patch.object(hcs.db, "get_storage_node_by_id",
                          return_value=_owner("in_restart")), \
             patch.object(hcs.health_controller, "repairs_allowed", return_value=False):
            assert hcs._drain_deferred_repairs("target-1") == []
        assert hcs._deferred_repairs["owner-1"]        # still owed

    def test_drained_once_owner_is_online(self):
        hcs._defer_repair("owner-1", "JM", "remote_jm_x", "dev-1", "target-1")
        with patch.object(hcs.db, "get_storage_node_by_id",
                          return_value=_owner("online")), \
             patch.object(hcs.health_controller, "repairs_allowed", return_value=True):
            assert hcs._drain_deferred_repairs("target-1") == [
                ("JM", "remote_jm_x", "dev-1")]
        assert hcs._deferred_repairs == {}             # consumed

    def test_only_entries_for_this_target_are_drained(self):
        hcs._defer_repair("owner-1", "JM", "ctrl_a", "dev-a", "target-1")
        hcs._defer_repair("owner-1", "JM", "ctrl_b", "dev-b", "target-2")
        with patch.object(hcs.db, "get_storage_node_by_id",
                          return_value=_owner("online")), \
             patch.object(hcs.health_controller, "repairs_allowed", return_value=True):
            got = hcs._drain_deferred_repairs("target-1")
        assert got == [("JM", "ctrl_a", "dev-a")]
        assert hcs._deferred_repairs["owner-1"] == {
            ("JM", "ctrl_b", "dev-b", "target-2")}

    def test_vanished_owner_drops_what_was_owed(self):
        hcs._defer_repair("owner-gone", "JM", "ctrl", "dev", "target-1")
        with patch.object(hcs.db, "get_storage_node_by_id", side_effect=KeyError("gone")):
            assert hcs._drain_deferred_repairs("target-1") == []
        assert hcs._deferred_repairs == {}

    def test_owner_returning_none_drops_what_was_owed(self):
        hcs._defer_repair("owner-1", "JM", "ctrl", "dev", "target-1")
        with patch.object(hcs.db, "get_storage_node_by_id", return_value=None):
            assert hcs._drain_deferred_repairs("target-1") == []
        assert hcs._deferred_repairs == {}

    def test_the_incident_sequence(self):
        """Skipped while in_restart, re-driven on the cycle after the owner returns."""
        for _ in range(15):                       # the 15 logged skips
            hcs._defer_repair("peer", "JM", "remote_jm_28eadbb7", "dev-1", "node-98-6")

        with patch.object(hcs.db, "get_storage_node_by_id",
                          return_value=_owner("in_restart")), \
             patch.object(hcs.health_controller, "repairs_allowed", return_value=False):
            assert hcs._drain_deferred_repairs("node-98-6") == []

        with patch.object(hcs.db, "get_storage_node_by_id",
                          return_value=_owner("online")), \
             patch.object(hcs.health_controller, "repairs_allowed", return_value=True):
            assert hcs._drain_deferred_repairs("node-98-6") == [
                ("JM", "remote_jm_28eadbb7", "dev-1")]
