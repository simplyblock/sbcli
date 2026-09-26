"""A drain's own volumes must be migratable off the node it is draining.

Every removal shuts the node down before it moves anything, so by the time the
volumes migrate the primary is never up: it is MIGRATING_DEVICES, then
MIGRATING_LVOLS, both of which mean its SPDK is stopped. Source-side RPCs are
served by an online replica instead -- that is what resolve_source_node is for.

The guards did not ask that question. They asked whether the primary itself was
up, which for a drain is always "no", so every migration a drain issued was
refused on a precondition before the replica was ever considered. The CR failed
before the target was engaged, so nothing could be blamed on the target and
nothing escalated: the same migration was recreated against the same node
indefinitely (2026-09-26).

The two guards are separate call sites -- the API's start_migration and the
runner's per-phase re-check -- and have drifted apart before, so both are
covered here.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import migration_controller as ctl
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status, secondary="", tertiary=""):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    node.secondary_node_id = secondary
    node.tertiary_node_id = tertiary
    return node


class ResolveSourceNodeTests(unittest.TestCase):
    """resolve_source_node is the one owner of "who serves the source side"."""

    def _resolve(self, primary, peers):
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda i: peers[i]
        with patch.object(ctl, 'db', db):
            return ctl.resolve_source_node(primary)

    def test_a_node_stopped_by_its_own_removal_resolves_to_its_replica(self):
        """The case a drain always presents, in both of its statuses."""
        for status in (StorageNode.STATUS_MIGRATING_DEVICES,
                       StorageNode.STATUS_MIGRATING_LVOLS):
            with self.subTest(status=status):
                primary = _node("n1", status, secondary="n2")
                peers = {"n2": _node("n2", StorageNode.STATUS_ONLINE)}
                self.assertEqual(self._resolve(primary, peers).get_id(), "n2")

    def test_a_live_primary_serves_its_own_migration(self):
        primary = _node("n1", StorageNode.STATUS_ONLINE, secondary="n2")
        self.assertEqual(self._resolve(primary, {}).get_id(), "n1")

    def test_the_tertiary_is_used_when_the_secondary_is_down(self):
        primary = _node("n1", StorageNode.STATUS_MIGRATING_LVOLS,
                        secondary="n2", tertiary="n3")
        peers = {"n2": _node("n2", StorageNode.STATUS_DOWN),
                 "n3": _node("n3", StorageNode.STATUS_ONLINE)}
        self.assertEqual(self._resolve(primary, peers).get_id(), "n3")

    def test_no_replica_left_is_the_real_failure(self):
        """The condition the guard is actually for: nothing can serve the
        source side, so the migration genuinely cannot run."""
        primary = _node("n1", StorageNode.STATUS_MIGRATING_LVOLS, secondary="n2")
        peers = {"n2": _node("n2", StorageNode.STATUS_DOWN)}
        with self.assertRaises(ValueError):
            self._resolve(primary, peers)


class ResolvedOnceAtCreateTests(unittest.TestCase):
    """The answer is computed once, by create, and only read afterwards.

    The bug this file was first written for looked like a missing guard, and
    was fixed as one -- by resolving the source again inside start_migration.
    That is the wrong shape. Resolution depends on replica health, so a second
    resolution is free to pick a different node than the one whose target
    subsystem create_migration already built, and neither caller would know.

    Worse, it hides the defect that actually broke the cluster: create
    resolved nothing at all, so active_source_node_id stayed "" and every
    reader fell through to the stopped primary. A guard that re-resolves
    returns the right node and makes the unset field invisible.

    So the invariant is about *where* resolution happens, which no behavioural
    test can observe -- both shapes pass those. It is asserted on the source
    of each function instead.
    """

    def _body(self, fn):
        import inspect
        return inspect.getsource(fn)

    def test_create_pins_the_resolved_source_on_the_record(self):
        """Without this write every reader below silently means "the primary"."""
        body = self._body(ctl.create_migration)
        self.assertIn("_resolve_active_source_node(src_node, target_node_id)", body)
        self.assertIn("migration.active_source_node_id = active_src_node.get_id()", body)

    def test_create_batch_pins_it_on_the_group(self):
        body = self._body(ctl.create_batch_migration)
        self.assertIn("group.active_source_node_id = active_source_node_id", body)

    def test_start_reads_the_pinned_value_and_does_not_re_resolve(self):
        body = self._body(ctl.start_migration)
        self.assertIn("migration.active_source_node_id or source_node_id", body)
        self.assertNotIn(
            "resolve_source_node(", body,
            "start_migration resolves the source a second time; it must read "
            "the value create_migration pinned, or the two can disagree")

    def test_start_takes_the_source_from_the_record_not_the_volume(self):
        """lvol.node_id is the primary, which is exactly the node that is down;
        re-deriving from it discards the fallback create chose."""
        body = self._body(ctl.start_migration)
        self.assertIn("source_node_id = migration.source_node_id", body)
        self.assertNotIn("source_node_id = lvol.node_id", body)

    def test_start_batch_reads_the_pinned_value_too(self):
        body = self._body(ctl.start_batch_migration)
        self.assertIn("group.active_source_node_id or group.source_node_id", body)
        self.assertNotIn("resolve_source_node(", body)

    def test_the_runner_addresses_the_pinned_source(self):
        """The failure this caused on the cluster: RPCs went to the stopped
        primary and came back as a bare `connection error` in cleanup_target."""
        from simplyblock_core.services import tasks_runner_lvol_migration as runner
        import inspect
        body = inspect.getsource(runner.task_runner)
        self.assertIn("migration.active_source_node_id or migration.source_node_id", body)
        self.assertNotIn("resolve_source_node(", body)


class StatusSetInvariantTests(unittest.TestCase):

    def test_the_drain_statuses_are_not_migration_sources(self):
        """Pins why the guards cannot be status comparisons: the statuses a
        drain stamps mean the SPDK is stopped, so they are correctly absent
        from MIGRATION_SOURCE_STATUSES. The source has to come from elsewhere.
        """
        for status in (StorageNode.STATUS_MIGRATING_DEVICES,
                       StorageNode.STATUS_MIGRATING_LVOLS):
            self.assertIn(status, StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
            self.assertNotIn(status, StorageNode.MIGRATION_SOURCE_STATUSES)


if __name__ == '__main__':
    unittest.main()
