"""A storage node's huge pages are its computed huge-page memory, rounded up to
whole 2 MiB pages, and nothing more.

The computed figure (``calculate_minimum_hp_memory``, floored to max_prov and
to 6.5 GiB) already carries its own overhead factor and reserve. On top of it
the host used to reserve a flat 1 GB more per storage node, counted in pages
of 2,000,000 bytes, and the SPDK pod requested 512 MiB more. The kubelet caps a
container's huge pages at its request, so at the 6.5 GiB floor the host held
3990 pages (7980 MiB) for a pod that could use 7170 MiB: about 810 MiB per
storage node reserved, never used, and still taken from the node's ordinary
memory. The reservation and the pod request now come from one function,
``core_utils.hugepages_2mi_pages``, so they cannot drift apart again.
"""
import math
import unittest
from unittest.mock import patch

from simplyblock_core import utils as core_utils
from simplyblock_web.api.internal.storage_node import kubernetes as k8s_node
from tests.unit.test_talos_hugepages import _TalosHandlerCase

MIB = 1024 * 1024
GIB = 1024 * MIB
PAGE = 2 * MIB

#: The 6.5 GiB floor of a storage node's huge-page memory (H).
FLOOR = 6_980_607_384


class TestPagesPerStorageNode(unittest.TestCase):

    def test_the_floor_reserves_3329_pages(self):
        self.assertEqual(k8s_node._hugepages_per_numa(
            [{"huge_page_memory": FLOOR, "max_size": 0, "socket": 0}]), {0: 3329})

    def test_each_storage_node_is_rounded_up_on_its_own(self):
        """Two storage nodes on NUMA 0 hold two pods' worth of whole pages,
        and a storage node one byte over a page boundary gets the next page."""
        nodes = [
            {"huge_page_memory": FLOOR, "max_size": 0, "socket": 0},
            {"huge_page_memory": 12 * GIB + 1, "max_size": 0, "socket": 0},
            {"huge_page_memory": 4 * GIB, "max_size": 0, "socket": 1},
        ]
        self.assertEqual(k8s_node._hugepages_per_numa(nodes), {
            0: 3329 + (12 * GIB // PAGE + 1),
            1: 4 * GIB // PAGE,
        })

    def test_max_size_still_raises_the_figure_where_larger(self):
        self.assertEqual(k8s_node._hugepages_per_numa(
            [{"huge_page_memory": 4 * GIB, "max_size": 6 * GIB, "socket": 0}]), {0: 6 * GIB // PAGE})


class TestCalculateHpOnly(unittest.TestCase):
    """``sn configure --calculate-hp-only`` tells the operator how many pages to
    reserve: the same figure apply_config reserves, not 1 GB more."""

    def test_advice_is_the_pages_the_node_reserves(self):
        with patch.object(core_utils, 'get_numa_cores', return_value={0: list(range(8))}), \
                patch.object(core_utils, 'generate_core_allocation',
                             return_value={0: [{"isolated": list(range(6))}, {"isolated": list(range(6))}]}), \
                patch.object(core_utils, 'calculate_core_allocations', return_value=[[0]] * 9), \
                patch.object(core_utils, 'calculate_pool_count', return_value=(1, 1)), \
                patch.object(core_utils, 'calculate_minimum_hp_memory', return_value=FLOOR):
            pages = core_utils.calculate_hp_only(32, 4, [0], 2)
        self.assertEqual(pages, 2 * 3329)


class TestSpdkPodRequest(_TalosHandlerCase):
    """The SPDK pod's hugepages-2Mi request is the storage node's pages times
    2 MiB: what the host reserves for it, no more."""

    def _pod_hugepages_mib(self, spdk_mem):
        self.core.create_namespaced_pod.reset_mock()
        data, error = self._start(host_pool=[16 * GIB], kubelet_capacity=['16Gi'], spdk_mem=spdk_mem)
        self.assertIsNone(error, error)
        pod = self.core.create_namespaced_pod.call_args.kwargs['body']
        [spdk] = [c for c in pod['spec']['containers']
                  if 'hugepages-2Mi' in (c.get('resources') or {}).get('limits', {})]
        return k8s_node._quantity_to_mib(spdk['resources']['limits']['hugepages-2Mi'])

    def test_the_floor_requests_6658_mib(self):
        self.assertEqual(self._pod_hugepages_mib(FLOOR), 6658)

    def test_the_request_is_the_reservation(self):
        for spdk_mem in (FLOOR, 12 * GIB, 12 * GIB + 1, 7 * GIB - 1):
            with self.subTest(spdk_mem=spdk_mem):
                pages = k8s_node._hugepages_per_numa([{"huge_page_memory": spdk_mem, "max_size": 0, "socket": 0}])[0]
                self.assertEqual(pages, math.ceil(spdk_mem / PAGE))
                self.assertEqual(self._pod_hugepages_mib(spdk_mem), pages * 2)


class TestHostWithTheOldPool(_TalosHandlerCase):
    """A Talos worker the managed flow provisioned under the old sizing (3990
    pages for one storage node at the floor) keeps its pool: the new target is
    below it, so nothing is applied and nothing is shrunk."""

    def setUp(self):
        super().setUp()
        self.env = dict(self.env, TALOS_MACHINE_CONFIG='managed')
        self._provisioned(user_pages=0, total_pages=3990)
        p = patch.object(k8s_node.core_utils, 'load_config', return_value={"nodes": [
            {"huge_page_memory": FLOOR, "max_size": 0, "socket": 0}]})
        p.start()
        self.addCleanup(p.stop)

    def test_the_old_pool_is_kept_and_the_pod_starts(self):
        data, error = self._start(host_pool=[3990 * PAGE], kubelet_capacity=['7980Mi'], spdk_mem=FLOOR)
        self.assertIsNone(error, error)
        self.assertIn('Pod created', data)
        self.assertEqual(self._talos_jobs(), [])
        self.assertEqual(self._state(), (0, 3990))
