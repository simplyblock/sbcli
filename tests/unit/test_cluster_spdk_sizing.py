# coding=utf-8
"""test_cluster_spdk_sizing.py — SPDK sizing is a cluster setting, in vCPUs.

max-subsys, the huge-page floor and the SPDK core budget used to be set per
storage node, which let one cluster hold nodes with different subsystem
ceilings and different core budgets. They are cluster fields now: a node adopts
them when it is added and on every restart.

The core budget also changed shape, from a percentage to an absolute count. A
percentage silently meant different things on different hardware -- 40% of 8
cores and 40% of 96 are not comparable budgets -- while a count is what an
operator can reason about. A node that cannot satisfy the count is refused
rather than quietly running SPDK on fewer cores than asked for.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops, constants, utils
from simplyblock_core.models.cluster import Cluster


class TestVcpuRequirement(unittest.TestCase):
    """One core beyond the SPDK budget must remain for the system."""

    def test_exactly_one_spare_core_is_enough(self):
        self.assertTrue(utils.vcpu_requirement_met(7, 6))

    def test_no_spare_core_is_refused(self):
        self.assertFalse(utils.vcpu_requirement_met(6, 6),
                         "a node with no core left for the system must be refused")

    def test_fewer_cores_than_asked_is_refused(self):
        self.assertFalse(utils.vcpu_requirement_met(4, 8))

    def test_plenty_is_fine(self):
        self.assertTrue(utils.vcpu_requirement_met(96, 6))

    def test_zero_means_no_requirement(self):
        for total in (1, 2, 96):
            with self.subTest(total=total):
                self.assertTrue(utils.vcpu_requirement_met(total, 0))

    def test_unknown_core_count_is_refused_when_a_count_is_set(self):
        self.assertFalse(utils.vcpu_requirement_met(None, 4))
        self.assertFalse(utils.vcpu_requirement_met(0, 4))


class TestAbsoluteCoreBudget(unittest.TestCase):
    """generate_core_allocation reads hyperthread siblings from sysfs, which
    only exists on Linux; the pairing is irrelevant to how the budget is split,
    so it is stubbed to "no siblings"."""

    def setUp(self):
        siblings = patch.object(utils, "parse_thread_siblings", return_value={})
        siblings.start()
        self.addCleanup(siblings.stop)

    def test_spdk_gets_exactly_the_requested_cores(self):
        cores = list(range(16))
        self.assertEqual(utils.calculate_unisolated_cores(cores, 6), 10,
                         "16 cores minus a 6-core SPDK budget leaves 10")

    def test_one_core_always_stays_with_the_system(self):
        cores = list(range(8))
        self.assertEqual(utils.calculate_unisolated_cores(cores, 8), 1,
                         "asking for every core still leaves one for the system")

    def test_an_impossible_ask_is_clamped_not_negative(self):
        """add/restart refuse such a node; the layout must stay sane regardless."""
        self.assertEqual(utils.calculate_unisolated_cores(list(range(4)), 99), 1)

    def test_zero_falls_back_to_the_heuristic(self):
        for total, expected in ((8, 2), (16, 3), (24, 4)):
            with self.subTest(total=total):
                self.assertEqual(
                    utils.calculate_unisolated_cores(list(range(total))), expected)

    def test_budget_is_split_across_the_sockets_in_use(self):
        cores_by_numa = {0: list(range(8)), 1: list(range(8, 16))}
        allocation = utils.generate_core_allocation(cores_by_numa, [0, 1], 1,
                                                    vcpu_count=6)
        isolated = {numa: len(entries[0]["isolated"])
                    for numa, entries in allocation.items()}
        self.assertEqual(sum(isolated.values()), 6,
                         f"the whole host budget must be 6 cores, got {isolated}")
        self.assertEqual(sorted(isolated.values()), [3, 3],
                         "an even budget splits evenly across two sockets")

    def test_an_odd_budget_puts_the_remainder_on_the_first_socket(self):
        cores_by_numa = {0: list(range(8)), 1: list(range(8, 16))}
        allocation = utils.generate_core_allocation(cores_by_numa, [0, 1], 1,
                                                    vcpu_count=7)
        isolated = {numa: len(entries[0]["isolated"])
                    for numa, entries in allocation.items()}
        self.assertEqual(sum(isolated.values()), 7)
        self.assertEqual(isolated[0], 4)


class TestClusterFields(unittest.TestCase):

    def test_defaults_keep_the_previous_behaviour(self):
        cluster = Cluster()
        self.assertEqual(cluster.max_subsys, 0)
        self.assertEqual(cluster.hugepages_mem, 0)
        self.assertEqual(cluster.spdk_vcpu_count, 0)

    def test_max_subsys_is_capped_at_the_product_limit(self):
        with self.assertRaises(ValueError):
            cluster_ops.validate_spdk_sizing(
                max_subsys=constants.MAX_SUBSYSTEMS_PER_NODE + 1)

    def test_sizing_can_be_changed_after_creation(self):
        cluster = MagicMock()
        cluster.max_subsys = 0
        cluster.hugepages_mem = 0
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "logger"):
            db.get_cluster_by_id.return_value = cluster
            self.assertTrue(cluster_ops.set_spdk_sizing(
                "cl-1", max_subsys=40, hugepages_mem=8 * 1024 ** 3))
            mutator = db.atomic_update.call_args[0][1]
        target = MagicMock()
        mutator(target)
        self.assertEqual(target.max_subsys, 40)
        self.assertEqual(target.hugepages_mem, 8 * 1024 ** 3)

    def test_nothing_to_change_writes_nothing(self):
        with patch.object(cluster_ops, "db_controller") as db:
            db.get_cluster_by_id.return_value = MagicMock()
            self.assertTrue(cluster_ops.set_spdk_sizing("cl-1"))
            db.atomic_update.assert_not_called()

    def test_the_vcpu_count_is_not_changeable_after_creation(self):
        """Changing a running cluster's core budget rewrites every node's core
        mask; that belongs to a deliberate re-deploy, not a settings write."""
        import inspect
        self.assertNotIn("spdk_vcpu_count",
                         inspect.signature(cluster_ops.set_spdk_sizing).parameters)


if __name__ == "__main__":
    unittest.main()
