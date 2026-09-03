"""Cluster-level admission must count snapshot utilisation on top of the
provisioned lvol sizes.

A 100T cluster with 80T provisioned and 15T of ACTUAL snapshot utilisation
has 5T of admissible headroom, not 20T — otherwise a cluster with no
overprovisioning at all can still run out of physical space. The pool-level
check (get_pool_total_capacity) has always followed this model; these tests
pin the cluster-level helper that feeds add_lvol's and clone's
prov_cap_warn/prov_cap_crit admission.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import pool_controller

T = 1000 ** 4
CLUSTER = "cl-1"
OTHER_CLUSTER = "cl-2"


def _pool(pool_id, cluster_id):
    p = MagicMock()
    p.get_id.return_value = pool_id
    p.cluster_id = cluster_id
    return p


def _snap(pool_id, used_size):
    s = MagicMock()
    s.lvol.pool_uuid = pool_id
    s.used_size = used_size
    return s


class TestClusterSnapshotUtilization(unittest.TestCase):

    def _run(self, pools, snaps, all_snaps_passed=True):
        with patch.object(pool_controller, "DBController") as db_cls:
            db = db_cls.return_value
            db.get_pools.return_value = pools
            db.get_mini_snapshots.return_value = snaps
            return pool_controller.get_cluster_snapshot_utilization(
                CLUSTER, all_snaps=snaps if all_snaps_passed else None)

    def test_sums_only_this_clusters_snapshots(self):
        pools = [_pool("p1", CLUSTER), _pool("p2", CLUSTER),
                 _pool("px", OTHER_CLUSTER)]
        snaps = [_snap("p1", 10 * T), _snap("p2", 5 * T),
                 _snap("px", 7 * T)]  # other cluster — must not count
        self.assertEqual(self._run(pools, snaps), 15 * T)

    def test_no_snapshots_is_zero(self):
        self.assertEqual(self._run([_pool("p1", CLUSTER)], []), 0)

    def test_loads_snapshots_when_not_provided(self):
        pools = [_pool("p1", CLUSTER)]
        snaps = [_snap("p1", 3 * T)]
        self.assertEqual(
            self._run(pools, snaps, all_snaps_passed=False), 3 * T)


if __name__ == "__main__":
    unittest.main()
