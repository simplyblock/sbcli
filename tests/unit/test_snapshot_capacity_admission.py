"""Snapshot-create capacity admission.

A snapshot immediately inherits the source volume's UTILIZED bytes, so it
must be blocked when

    provisioned-capacity limit
      - sum of provisioned volume sizes
      - actual utilization by existing snapshots
    <  utilized size of the source volume

on the pool (pool_max_size) or the cluster (prov_cap_crit percent of
effective capacity). It is charged at the source's utilized size — the old
extra pool check at the source's full PROVISIONED size (which made a 6G
volume in a 10G pool un-snapshottable regardless of utilization) is
deliberately gone; these tests pin that too.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import snapshot_controller

G = 1000 ** 3


def _check(pool_max, source_prov, source_used, pool_total,
           prov_cap_crit=0, cl_size_prov=0, cl_size_total=0, cl_snap_used=0,
           lvol_max=0, stats_available=True):
    pool = MagicMock()
    pool.pool_max_size = pool_max
    pool.lvol_max_size = lvol_max
    pool.get_id.return_value = "pool-1"

    cluster = MagicMock()
    cluster.prov_cap_crit = prov_cap_crit
    cluster.get_id.return_value = "cl-1"

    lvol = MagicMock()
    lvol.size = source_prov

    stats = MagicMock()
    stats.size_used = source_used
    cap_rec = MagicMock()
    cap_rec.size_prov = cl_size_prov
    cap_rec.size_total = cl_size_total

    with patch.object(snapshot_controller, "db_controller") as db, \
         patch.object(snapshot_controller, "pool_controller") as pc:
        db.get_lvol_stats.return_value = [stats] if stats_available else []
        db.get_mini_lvols.return_value = []
        db.get_mini_snapshots.return_value = []
        db.get_cluster_capacity.return_value = [cap_rec]
        pc.get_pool_total_capacity.return_value = pool_total
        pc.get_cluster_snapshot_utilization.return_value = cl_snap_used
        return snapshot_controller.check_snapshot_capacity(pool, cluster, lvol)


class TestPoolLevel(unittest.TestCase):

    def test_charged_at_utilized_not_provisioned_size(self):
        """6G-provisioned volume with 1G utilized in a 10G pool holding 8G
        total: 8+1 <= 10 admits — the old provisioned-size check refused."""
        self.assertIsNone(_check(pool_max=10 * G, source_prov=6 * G,
                                 source_used=1 * G, pool_total=8 * G))

    def test_rejected_when_utilized_exceeds_headroom(self):
        """Formula: limit - volumes - snapshots < source_used => block.
        pool_total already contains both subtrahends."""
        err = _check(pool_max=10 * G, source_prov=6 * G,
                     source_used=3 * G, pool_total=8 * G)
        self.assertIn("Cannot take snapshot", err)

    def test_exact_fit_admits(self):
        self.assertIsNone(_check(pool_max=10 * G, source_prov=6 * G,
                                 source_used=2 * G, pool_total=8 * G))

    def test_missing_stats_falls_back_to_provisioned(self):
        """No stats record: conservative — charge full provisioned size."""
        err = _check(pool_max=10 * G, source_prov=6 * G, source_used=0,
                     pool_total=8 * G, stats_available=False)
        self.assertIn("Cannot take snapshot", err)

    def test_unlimited_pool_and_cluster_admit_without_scans(self):
        self.assertIsNone(_check(pool_max=0, source_prov=6 * G,
                                 source_used=3 * G, pool_total=0))


class TestClusterLevel(unittest.TestCase):
    """Cluster limit = prov_cap_crit% of effective capacity; the collector's
    size_prov is lvol-only, snapshot utilization is added on top."""

    def test_rejected_when_snapshot_would_cross_the_cap(self):
        # 100T cluster, cap 100%: 80T volumes + 15T snapshots + 6T source
        # utilization = 101% -> block.
        err = _check(pool_max=0, source_prov=20 * G, source_used=6 * G,
                     pool_total=0, prov_cap_crit=100, cl_size_prov=80 * G,
                     cl_size_total=100 * G, cl_snap_used=15 * G)
        self.assertIn("cluster provisioned cap", err)

    def test_admitted_within_the_cap(self):
        # 80T + 15T + 5T = exactly 100% -> admit (limit is exceeded-only).
        self.assertIsNone(_check(pool_max=0, source_prov=20 * G,
                                 source_used=5 * G, pool_total=0,
                                 prov_cap_crit=100, cl_size_prov=80 * G,
                                 cl_size_total=100 * G, cl_snap_used=15 * G))

    def test_snapshot_utilization_term_matters(self):
        # Identical numbers but no existing snapshots -> admits.
        self.assertIsNone(_check(pool_max=0, source_prov=20 * G,
                                 source_used=6 * G, pool_total=0,
                                 prov_cap_crit=100, cl_size_prov=80 * G,
                                 cl_size_total=100 * G, cl_snap_used=0))

    def test_no_capacity_record_admits(self):
        with patch.object(snapshot_controller, "db_controller") as db, \
             patch.object(snapshot_controller, "pool_controller"):
            pool = MagicMock(pool_max_size=0, lvol_max_size=0)
            cluster = MagicMock(prov_cap_crit=190)
            lvol = MagicMock(size=6 * G)
            db.get_lvol_stats.return_value = []
            db.get_cluster_capacity.return_value = []
            self.assertIsNone(snapshot_controller.check_snapshot_capacity(
                pool, cluster, lvol))


class TestOldProvisionedCheckRemoved(unittest.TestCase):

    def test_add_charges_utilized_size_only(self):
        import inspect
        src = inspect.getsource(snapshot_controller.add)
        self.assertNotIn("total + lvol.size", src)
        self.assertIn("check_snapshot_capacity", src)


if __name__ == "__main__":
    unittest.main()
