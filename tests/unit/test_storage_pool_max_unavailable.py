"""How many workers the storage MachineConfigPool may take at once.

The pool's maxUnavailable is what OpenShift reads when it rolls a MachineConfig
over the storage workers. It used to be the fault tolerance counted in workers,
which made a rollout take one worker at a time on a cluster that can lose a
whole failure domain. It is now the largest set of workers the storage can
afford to lose together, narrowed by the cap the cluster's owner states.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


def _cluster(ndcs=1, npcs=1, tolerance=1, domains=True):
    cluster = Cluster()
    cluster.distr_ndcs = ndcs
    cluster.distr_npcs = npcs
    cluster.max_fault_tolerance = tolerance
    cluster.enable_failure_domain = domains
    return cluster


def _node(host, domain, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.mgmt_ip = host
    node.failure_domain = domain
    node.status = status
    return node


def _racks(*sizes):
    """One worker per node, sizes[i] of them in failure domain i."""
    return [_node(f"10.0.{rack}.{n}", rack)
            for rack, size in enumerate(sizes) for n in range(size)]


class TestStoragePoolMaxUnavailable(unittest.TestCase):

    def test_a_whole_failure_domain_may_go_at_once(self):
        value = cluster_ops.storage_pool_max_unavailable(_cluster(), _racks(3, 3, 3))
        self.assertEqual(value, 3)

    def test_the_largest_failure_domain_sets_the_size(self):
        value = cluster_ops.storage_pool_max_unavailable(_cluster(), _racks(4, 3, 2))
        self.assertEqual(value, 4)

    def test_a_fault_tolerance_of_two_affords_the_two_largest_domains(self):
        cluster = _cluster(ndcs=2, npcs=2, tolerance=2)
        value = cluster_ops.storage_pool_max_unavailable(cluster, _racks(3, 2, 4, 2))
        self.assertEqual(value, 7)

    def test_a_cluster_outside_failure_domain_mode_keeps_its_fault_tolerance(self):
        value = cluster_ops.storage_pool_max_unavailable(
            _cluster(domains=False), _racks(3, 3, 3))
        self.assertEqual(value, 1)

    def test_too_few_domains_for_a_stripe_keep_the_fault_tolerance(self):
        """Two domains hold the three chunks of a 2+1 stripe, so one of them
        holds two, and two of its workers down are two chunks lost."""
        cluster = _cluster(ndcs=2, npcs=1)
        value = cluster_ops.storage_pool_max_unavailable(cluster, _racks(3, 3))
        self.assertEqual(value, 1)

    def test_a_node_in_no_domain_keeps_the_fault_tolerance(self):
        nodes = _racks(3, 3, 3) + [_node("10.0.9.9", -1)]
        value = cluster_ops.storage_pool_max_unavailable(_cluster(), nodes)
        self.assertEqual(value, 1)

    def test_a_cluster_without_parity_keeps_its_fault_tolerance(self):
        cluster = _cluster(ndcs=1, npcs=0)
        value = cluster_ops.storage_pool_max_unavailable(cluster, _racks(3, 3, 3))
        self.assertEqual(value, 1)

    def test_a_removed_node_is_not_a_worker_of_its_domain(self):
        nodes = _racks(3, 3, 3) + [_node("10.0.0.9", 0, StorageNode.STATUS_REMOVED)]
        value = cluster_ops.storage_pool_max_unavailable(_cluster(), nodes)
        self.assertEqual(value, 3)

    def test_two_nodes_on_one_host_are_one_worker(self):
        """The pool counts machines, and a multi-socket host is one machine."""
        nodes = _racks(2, 2, 2) + [_node("10.0.0.0", 0), _node("10.0.0.1", 0)]
        value = cluster_ops.storage_pool_max_unavailable(_cluster(), nodes)
        self.assertEqual(value, 2)

    def test_the_owners_cap_narrows_it(self):
        value = cluster_ops.storage_pool_max_unavailable(
            _cluster(), _racks(10, 10, 10), worker_cap=3)
        self.assertEqual(value, 3)

    def test_the_owners_cap_does_not_widen_it(self):
        value = cluster_ops.storage_pool_max_unavailable(
            _cluster(), _racks(3, 3, 3), worker_cap=8)
        self.assertEqual(value, 3)

    def test_it_is_never_below_one(self):
        """A pool at zero never rolls a node."""
        cluster = _cluster(tolerance=0, domains=False)
        value = cluster_ops.storage_pool_max_unavailable(cluster, _racks(3))
        self.assertEqual(value, 1)


class TestMaxConcurrentWorkerRestartsFromCR(unittest.TestCase):

    def _read(self, cr):
        api = MagicMock()
        api.get_namespaced_custom_object.return_value = cr
        with patch.object(utils, "load_kube_config_with_fallback"), \
                patch.object(utils.client, "CustomObjectsApi", return_value=api):
            return utils.get_max_concurrent_worker_restarts_from_cr("a-cluster", "simplyblock")

    def test_it_is_read_from_the_spec(self):
        self.assertEqual(self._read({"spec": {"maxConcurrentWorkerRestarts": 3}}), 3)

    def test_an_unstated_cap_is_no_cap(self):
        self.assertIsNone(self._read({"spec": {}}))

    def test_a_cluster_with_no_custom_resource_has_no_cap(self):
        self.assertIsNone(utils.get_max_concurrent_worker_restarts_from_cr("", ""))


if __name__ == "__main__":
    unittest.main()
