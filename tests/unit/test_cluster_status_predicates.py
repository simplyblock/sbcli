"""
Membership pins for Cluster's status predicate sets.

These sets replaced ~17 inline status literals spread across the controllers,
services and task runners. That centralisation is what makes adding a new
status three decisions instead of twenty-five — but it also means a one-line
edit here silently changes every one of those call sites at once, and most of
them cannot be caught by the suite: a gate that wrongly refuses only bites
during a live node removal or a real read-only cluster.

So pin the memberships exactly. A deliberate change updates these lists and
says why; an accidental one fails here.
"""

import unittest

from simplyblock_core.models.cluster import Cluster


class TestStatusSetMembership(unittest.TestCase):

    def test_mutable_statuses(self):
        """Client writes: ACTIVE/DEGRADED, plus IN_SHRINK so a node removal —
        which can run for hours — does not freeze creates cluster-wide.
        READONLY is excluded by definition."""
        self.assertEqual(
            Cluster.MUTABLE_STATUSES,
            {Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
             Cluster.STATUS_IN_SHRINK})
        self.assertNotIn(Cluster.STATUS_READONLY, Cluster.MUTABLE_STATUSES)

    def test_operable_statuses(self):
        """Background work: everything mutations allow, plus READONLY — a
        read-only cluster still needs health checks, monitors and migration,
        which are often what gets it back to ACTIVE."""
        self.assertEqual(
            Cluster.OPERABLE_STATUSES,
            {Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
             Cluster.STATUS_READONLY, Cluster.STATUS_IN_SHRINK})

    def test_operable_is_a_superset_of_mutable(self):
        """If a cluster accepts client writes it must accept the upkeep that
        supports them; the reverse does not hold."""
        self.assertTrue(Cluster.MUTABLE_STATUSES < Cluster.OPERABLE_STATUSES)

    def test_topology_owned_statuses(self):
        """The three lifecycle flows that own the layout and restore the
        status themselves. This set decides whether a restart phase has an
        owner (storage_node_ops.get_restart_phase), so an omission here
        reintroduces the bug where a live phase is cleared mid-rebuild."""
        self.assertEqual(
            Cluster.TOPOLOGY_OWNED_STATUSES,
            {Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_IN_EXPANSION,
             Cluster.STATUS_IN_SHRINK})

    def test_in_shrink_is_both_operable_and_topology_owned(self):
        """Node removal is the one status that is both: the removal depends on
        migration/health work continuing (refusing it deadlocks the removal
        against its own status), while also owning the layout."""
        self.assertIn(Cluster.STATUS_IN_SHRINK, Cluster.OPERABLE_STATUSES)
        self.assertIn(Cluster.STATUS_IN_SHRINK, Cluster.TOPOLOGY_OWNED_STATUSES)

    def test_no_serving_status_is_topology_owned(self):
        """A cluster cannot simultaneously be owned by a lifecycle flow and be
        one of the steady serving states — except IN_SHRINK, which is
        deliberately both (see above)."""
        overlap = Cluster.OPERABLE_STATUSES & Cluster.TOPOLOGY_OWNED_STATUSES
        self.assertEqual(overlap, {Cluster.STATUS_IN_SHRINK})

    def test_every_set_member_is_a_known_status(self):
        """Guards against a typo'd literal silently creating a status that
        nothing ever sets."""
        known = set(Cluster.STATUS_CODE_MAP)
        for name in ("MUTABLE_STATUSES", "OPERABLE_STATUSES",
                     "TOPOLOGY_OWNED_STATUSES"):
            self.assertLessEqual(getattr(Cluster, name), known, name)


class TestStatusPredicates(unittest.TestCase):

    def _cluster(self, status):
        c = Cluster()
        c.status = status
        return c

    def test_predicates_follow_the_sets(self):
        for status in Cluster.STATUS_CODE_MAP:
            cluster = self._cluster(status)
            self.assertEqual(cluster.allows_mutation(),
                             status in Cluster.MUTABLE_STATUSES, status)
            self.assertEqual(cluster.allows_operation(),
                             status in Cluster.OPERABLE_STATUSES, status)
            self.assertEqual(cluster.is_topology_owned(),
                             status in Cluster.TOPOLOGY_OWNED_STATUSES, status)


if __name__ == "__main__":
    unittest.main()
