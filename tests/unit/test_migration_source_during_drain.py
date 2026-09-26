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


class GuardsAskResolveSourceNodeTests(unittest.TestCase):
    """Both guards must route through it rather than re-deriving the rule.

    Asserted on the guard lines themselves: the failure mode is a status
    comparison reappearing beside resolve_source_node, not a wrong value, and
    the behavioural test above cannot see that.
    """

    def _guard_lines(self, module, pattern):
        import inspect
        import re
        return [line.strip() for line in inspect.getsource(module).splitlines()
                if re.search(pattern, line)]

    def test_the_api_guard_resolves_rather_than_comparing_status(self):
        lines = self._guard_lines(ctl, r'source_node\.status\s+not\s+in')
        self.assertEqual(
            lines, [],
            "start_migration compares the primary's status again; a drained "
            f"primary is never up, so this refuses every drain: {lines}")
        self.assertIn("resolve_source_node(source_node)",
                      "".join(self._guard_lines(ctl, r'resolve_source_node\(source_node\)')))

    def test_the_runner_guard_resolves_too(self):
        from simplyblock_core.services import tasks_runner_lvol_migration as runner

        lines = self._guard_lines(runner, r'src_node\.status\s+not\s+in')
        self.assertEqual(
            lines, [],
            f"the runner compares the primary's status again: {lines}")
        self.assertTrue(
            self._guard_lines(runner, r'resolve_source_node\(src_node\)'),
            "the runner does not resolve the source at all")


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
