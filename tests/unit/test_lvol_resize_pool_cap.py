"""resize_lvol's pool-capacity admission: the pool total already contains
the volume being resized.

``get_pool_total_capacity`` sums the PROVISIONED size of every lvol in the
pool — including the one being resized. The old check
``total + new_size > pool_max_size`` therefore counted that volume twice
(old size + new size): a single 4G volume in a 10G pool could not be
resized past 6G, and any volume larger than half the pool's remaining
headroom was stuck. The correct admission is
``total - lvol.size + new_size > pool_max_size``.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller, pool_controller
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.pool import Pool

G = 1000 ** 3


class _PassedCapacityCheck(Exception):
    """Raised by the storage-node lookup that directly FOLLOWS the capacity
    check, proving the resize was admitted without running the node-side
    RPC sequence (which a unit test cannot back)."""


def _resize(lvol_size, pool_total, pool_max, new_size):
    lvol = MagicMock()
    lvol.size = lvol_size
    lvol.max_size = 100 * G
    lvol.uuid = "lv-1"
    lvol.pool_uuid = "pool-1"
    lvol.node_id = "node-1"

    pool = MagicMock()
    pool.status = Pool.STATUS_ACTIVE
    pool.lvol_max_size = 0
    pool.pool_max_size = pool_max
    pool.get_id.return_value = "pool-1"

    cluster = MagicMock()
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.get_id.return_value = "cl-1"

    restart_guard_node = MagicMock()
    restart_guard_node.lvstore_status = "ready"

    # unsafe=True: the gate's method is named assert_object_ops_allowed,
    # and a safe MagicMock rejects any attribute starting with "assert".
    with patch.object(lvol_controller, "DBController") as db_cls, \
         patch.object(lvol_controller, "ops_gate",
                      MagicMock(unsafe=True), create=True), \
         patch("simplyblock_core.controllers.migration_controller."
               "get_active_migration_for_lvol", return_value=None), \
         patch.object(pool_controller, "get_pool_total_capacity",
                      return_value=pool_total):
        db = db_cls.return_value
        db.get_lvol_by_id.return_value = lvol
        db.get_pool_by_id.return_value = pool
        db.get_cluster_by_id.return_value = cluster
        # First node lookup feeds the restart-phase guard; the second sits
        # just past the capacity check and stops the test there.
        db.get_storage_node_by_id.side_effect = [
            restart_guard_node, _PassedCapacityCheck()]
        lvol_controller.resize_lvol("lv-1", new_size)


class TestResizeDoesNotDoubleCountItself(unittest.TestCase):

    def test_single_volume_can_grow_within_cap(self):
        """4G volume alone in a 10G pool grows to 8G. The double-count
        computed 4+8=12 > 10 and rejected this."""
        with self.assertRaises(_PassedCapacityCheck):
            _resize(lvol_size=4 * G, pool_total=4 * G,
                    pool_max=10 * G, new_size=8 * G)

    def test_exact_fit_is_admitted(self):
        """4G volume + 4G neighbour in a 10G pool: growing to 6G lands the
        pool exactly at its cap, which must pass (check is >, not >=)."""
        with self.assertRaises(_PassedCapacityCheck):
            _resize(lvol_size=4 * G, pool_total=8 * G,
                    pool_max=10 * G, new_size=6 * G)

    def test_growth_past_cap_is_rejected(self):
        """Same layout, one byte past the cap must still be refused."""
        with self.assertRaises(PreconditionError):
            _resize(lvol_size=4 * G, pool_total=8 * G,
                    pool_max=10 * G, new_size=6 * G + 1)


if __name__ == "__main__":
    unittest.main()
