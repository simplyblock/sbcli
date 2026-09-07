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

from simplyblock_core import cluster_ops, constants, storage_node_ops, utils
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


class TestApplyClusterVcpuCount(unittest.TestCase):
    """add_node's one-time resize of a host's core layout to the cluster's
    vcpu_count -- the piece that actually makes spdk_vcpu_count take effect,
    as opposed to only gating admission via vcpu_requirement_met."""

    def setUp(self):
        siblings = patch.object(utils, "parse_thread_siblings", return_value={})
        siblings.start()
        self.addCleanup(siblings.stop)

    @staticmethod
    def _node_config(socket, isolated_len, ssd="0000:00:01.0"):
        return {
            "socket": socket,
            "isolated": list(range(isolated_len)),
            "cpu_mask": "0x0",
            "l-cores": "",
            "distribution": {},
            "core_to_index": {},
            "ssd_pcis": [ssd],
        }

    @staticmethod
    def _node_info(cores_by_numa):
        return {"cpu_topology": {str(k): v for k, v in cores_by_numa.items()}}

    def test_resizes_to_the_cluster_budget_and_persists_once(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_info = self._node_info({0: list(range(32))})
        nodes = [self._node_config(0, isolated_len=28)]  # old default heuristic

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 8)

        self.assertTrue(ok)
        self.assertEqual(len(nodes[0]["isolated"]), 8)
        snode_api.persist_node_config.assert_called_once()

        # distribution must be the resolved {"app_thread_core": [...], ...}
        # dict every consumer (add_node, persist_node_config's schema) reads
        # -- utils.calculate_core_allocations itself returns a positional
        # tuple, not this dict; storing that raw tuple 422s persist_node_config
        # (caught 2026-08-21 testing this against a live cluster).
        distribution = nodes[0]["distribution"]
        self.assertIsInstance(distribution, dict)
        for key in ("app_thread_core", "jm_cpu_core", "poller_cpu_cores",
                    "alceml_cpu_cores", "distrib_cpu_cores", "jc_singleton_core",
                    "lvol_poller_core"):
            self.assertIn(key, distribution)
        persisted_kwargs = snode_api.persist_node_config.call_args.kwargs
        self.assertIsInstance(persisted_kwargs["distribution"], dict)

    def test_number_of_distribs_is_rederived_from_the_resized_layout(self):
        """number_of_distribs is sized off distrib_cpu_cores at configure time,
        against the host's full core count -- before the host belongs to any
        cluster. Resizing the layout down to the cluster's vcpu_count must
        rederive it too, or add_node persists a distrib count sized for the
        pre-resize (much larger) layout instead of the one actually running.

        Configured against isolated_len=18 (no cluster yet) gives
        distrib_cpu_cores=6 (table: V=17-18 -> 6 distribs). Resized to the
        cluster's vcpu_count=8, distrib_cpu_cores drops to 2 (table: V=8-9 ->
        2 distribs); number_of_distribs must follow it down to 2, not stay 6.
        """
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_info = self._node_info({0: list(range(32))})
        node = self._node_config(0, isolated_len=18)
        node["number_of_distribs"] = 6  # stale, from configure-time sizing
        nodes = [node]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 8)

        self.assertTrue(ok)
        self.assertEqual(len(nodes[0]["distribution"]["distrib_cpu_cores"]), 2)
        self.assertEqual(nodes[0]["number_of_distribs"], 2,
                         "must be rederived from the resized layout, not left at the stale 6")
        persisted_kwargs = snode_api.persist_node_config.call_args.kwargs
        self.assertEqual(persisted_kwargs["number_of_distribs"], 2,
                         "the rederived count must also be persisted to the node's config file")

    def test_already_correct_is_a_no_op(self):
        """A retried add_node re-fetches the file its own earlier attempt
        already resized; it must not refetch topology or rewrite it again."""
        snode_api = MagicMock()
        node_info = self._node_info({0: list(range(32))})
        nodes = [self._node_config(0, isolated_len=8)]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 8)

        self.assertTrue(ok)
        snode_api.persist_node_config.assert_not_called()

    def test_budget_is_split_across_sockets(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_info = self._node_info({0: list(range(16)), 1: list(range(16, 32))})
        nodes = [self._node_config(0, isolated_len=14, ssd="0000:00:01.0"),
                self._node_config(1, isolated_len=14, ssd="0000:01:01.0")]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 10)

        self.assertTrue(ok)
        self.assertEqual(sorted(len(n["isolated"]) for n in nodes), [5, 5])

    def test_nodes_per_socket_two_shares_the_socket_budget(self):
        """Two SPDK instances on one socket split that socket's share between
        them; the aggregate across both still equals the budget."""
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_info = self._node_info({0: list(range(32))})
        nodes = [self._node_config(0, isolated_len=14, ssd="0000:00:01.0"),
                self._node_config(0, isolated_len=14, ssd="0000:00:02.0")]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 10)

        self.assertTrue(ok)
        self.assertEqual(sum(len(n["isolated"]) for n in nodes), 10)

    def test_missing_topology_refuses_cleanly(self):
        snode_api = MagicMock()
        nodes = [self._node_config(0, isolated_len=28)]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, {}, nodes, 8)

        self.assertFalse(ok)
        snode_api.persist_node_config.assert_not_called()

    def test_failed_persist_fails_the_whole_call(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (False, "disk full")
        node_info = self._node_info({0: list(range(32))})
        nodes = [self._node_config(0, isolated_len=28)]

        ok = storage_node_ops.apply_cluster_vcpu_count(snode_api, node_info, nodes, 8)

        self.assertFalse(ok)


class TestApplyClusterHugepages(unittest.TestCase):
    """add_node's recalculation of huge_page_memory (and the pool counts it
    is derived from) against the cluster's real max_subsys/vcpu_count -- sn
    configure priced it for the worst case since it ran before the node
    belonged to any cluster, and add_node must not just carry that forward."""

    @staticmethod
    def _node_config(max_lvol, isolated_len, number_of_alcemls=4, number_of_distribs=2,
                     poller_cores=None):
        poller_cores = list(range(isolated_len)) if poller_cores is None else poller_cores
        small, large = utils.calculate_pool_count(
            number_of_alcemls, 2 * number_of_distribs, isolated_len,
            len(poller_cores) or isolated_len, max_lvol)
        return {
            "max_lvol": max_lvol,
            "number_of_alcemls": number_of_alcemls,
            "number_of_distribs": number_of_distribs,
            "distribution": {"poller_cpu_cores": poller_cores},
            "socket": 0,
            "ssd_pcis": ["0000:00:01.0"],
            "small_pool_count": small,
            "large_pool_count": large,
            "huge_page_memory": utils.calculate_minimum_hp_memory(
                small, large, max_lvol, 0, isolated_len),
        }

    def test_shrinking_max_lvol_and_cores_lowers_the_figure_and_persists(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        # sn configure priced this for the product ceiling and its own
        # default (larger) core count.
        node_config = self._node_config(max_lvol=constants.MAX_SUBSYSTEMS_PER_NODE,
                                        isolated_len=28)
        configured_hp_memory = node_config["huge_page_memory"]
        node_config["max_lvol"] = 10  # the cluster's real max_subsys, already applied above

        result = storage_node_ops.apply_cluster_hugepages(
            snode_api, node_config, req_cpu_count=8, max_prov=0)

        self.assertLess(result, configured_hp_memory)
        self.assertEqual(node_config["huge_page_memory"], result)
        snode_api.persist_node_config.assert_called_once()

    def test_matching_figures_are_a_noop(self):
        """Neither max_subsys nor vcpu_count set on the cluster -- nothing
        about this entry's sizing has actually changed since configure time."""
        snode_api = MagicMock()
        node_config = self._node_config(max_lvol=10, isolated_len=8,
                                        poller_cores=list(range(8)))

        result = storage_node_ops.apply_cluster_hugepages(
            snode_api, node_config, req_cpu_count=8, max_prov=0)

        self.assertEqual(result, node_config["huge_page_memory"])
        snode_api.persist_node_config.assert_not_called()

    def test_cluster_hugepages_floor_wins_over_the_computed_figure(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (True, None)
        node_config = self._node_config(max_lvol=10, isolated_len=8,
                                        poller_cores=list(range(8)))
        floor = node_config["huge_page_memory"] + 10 ** 9

        result = storage_node_ops.apply_cluster_hugepages(
            snode_api, node_config, req_cpu_count=8, max_prov=floor)

        self.assertEqual(result, floor)
        snode_api.persist_node_config.assert_called_once()

    def test_failed_persist_returns_none(self):
        snode_api = MagicMock()
        snode_api.persist_node_config.return_value = (False, "disk full")
        node_config = self._node_config(max_lvol=constants.MAX_SUBSYSTEMS_PER_NODE,
                                        isolated_len=28)
        node_config["max_lvol"] = 10

        result = storage_node_ops.apply_cluster_hugepages(
            snode_api, node_config, req_cpu_count=8, max_prov=0)

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
