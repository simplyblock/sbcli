# coding=utf-8
"""Regression test for the clone() cntlid-window bug.

Bug (observed at scale, 2026-05-27): when ``snapshot_controller.clone()``
registered an HA clone's NVMe-oF subsystem on its non-leader nodes, it called
``add_lvol_on_node`` WITHOUT a ``secondary_index``. Every secondary therefore
defaulted to ``secondary_index=0`` and got ``min_cntlid = 1000*(0+1) = 1000``.

CNTLID must be unique per subsystem across all paths on the host. With the
primary at min_cntlid=1, the secondary at 1000 and the tertiary ALSO at 1000,
the Linux host attached primary+secondary fine but rejected the tertiary path:

    nvme nvmeN: Duplicate cntlid 1000 with nvmeM, subsys ..., rejecting

The normal lvol-create path (lvol_controller, ``enumerate(secondary_nodes)``)
already threaded a distinct index per secondary; clone() did not. This test
pins the fix: each non-leader node passed to ``add_lvol_on_node`` from clone()
must receive a DISTINCT ``secondary_index`` (0, 1, ...), so the per-node
cntlid windows (1000, 2000, ...) never collide.

All external dependencies (DB, RPC, locks, events) are mocked.
"""

import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol


def _make_node(node_id, secondary_id=None, tertiary_id=None):
    node = MagicMock(name=node_id)
    node.get_id.return_value = node_id
    node.hostname = node_id
    node.max_lvol = 100
    node.lvstore_status = "ready"
    node.lvol_sync_del.return_value = False
    node.secondary_node_id = secondary_id
    node.tertiary_node_id = tertiary_id
    return node


def _make_snap(node_id):
    """A snapshot whose source lvol is HA, with all fields clone() reads."""
    src = LVol()
    src.pool_uuid = "pool-1"
    src.node_id = node_id
    src.size = 1024 ** 3
    src.max_size = 10 * 1024 ** 3
    src.base_bdev = "base0"
    src.lvs_name = "LVS_100"
    src.nodes = [node_id]
    src.ha_type = "ha"
    src.subsys_port = 4420
    src.allowed_hosts = []
    src.ndcs = 0
    src.npcs = 0
    src.crypto_bdev = ""
    src.max_namespace_per_subsys = 50

    snap = types.SimpleNamespace(
        lvol=src,
        deleted=False,
        status="online",          # not STATUS_IN_DELETION
        size=src.size,
        snap_bdev="LVS_100/SNAP_parent",
        fabric="tcp",
        snap_ref_id=None,
        ref_count=0,
        write_to_db=MagicMock(),
    )
    return snap


class TestCloneSecondaryCntlidIndex(unittest.TestCase):

    def _run_clone(self, secondary_id, tertiary_id):
        """Drive clone() through the HA registration block and return the list
        of (node_id, is_primary, secondary_index) tuples passed to
        add_lvol_on_node."""
        from simplyblock_core.controllers import snapshot_controller

        primary_id = "node-primary"
        host = _make_node(primary_id, secondary_id, tertiary_id)
        nodes_by_id = {primary_id: host}
        if secondary_id:
            nodes_by_id[secondary_id] = _make_node(secondary_id)
        if tertiary_id:
            nodes_by_id[tertiary_id] = _make_node(tertiary_id)

        snap = _make_snap(primary_id)

        pool = MagicMock()
        pool.status = "active"
        pool.get_id.return_value = "pool-1"
        pool.cluster_id = "cluster-1"
        pool.lvol_max_size = 0
        pool.pool_max_size = 0

        cluster = MagicMock()
        cluster.status = "active"
        cluster.STATUS_ACTIVE = "active"
        cluster.STATUS_DEGRADED = "degraded"
        cluster.nqn = "nqn.test:cluster-1"

        db = MagicMock()
        db.get_snapshot_by_id.return_value = snap
        db.get_pool_by_id.return_value = pool
        db.get_storage_node_by_id.side_effect = lambda i: nodes_by_id[i]
        db.get_cluster_by_id.return_value = cluster
        db.get_lvols.return_value = []
        db.get_mini_lvols.return_value = []
        db.get_snapshots.return_value = []
        # No capacity records -> skip the prov-cap-crit/warn gate (which would
        # otherwise compare MagicMock attrs against ints).
        db.get_cluster_capacity.return_value = []
        db.kv_store = MagicMock()

        calls = []

        def _record_add(lvol, node, is_primary=True, secondary_index=0):
            calls.append((node.get_id(), is_primary, secondary_index))
            return ({"uuid": "bdev-uuid",
                     "driver_specific": {"lvol": {"blobid": 1}}}, None)

        lvol_ctrl = MagicMock()
        lvol_ctrl.add_lvol_on_node.side_effect = _record_add
        lvol_ctrl.is_node_leader.side_effect = lambda c, lvs: c is host
        lvol_ctrl.get_next_available_subsystem_on_node.return_value = None

        with patch.object(snapshot_controller, "db_controller", db), \
             patch.object(snapshot_controller, "lvol_controller", lvol_ctrl), \
             patch.object(snapshot_controller, "snapshot_events", MagicMock()), \
             patch.object(snapshot_controller.utils, "get_random_vuid",
                          return_value=12345), \
             patch.object(LVol, "write_to_db", MagicMock()), \
             patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                   return_value="proceed"), \
             patch("simplyblock_core.storage_node_ops.queue_for_restart_drain",
                   MagicMock()):
            # namespaced=False -> each clone creates its own subsystem (the
            # path that calls subsystem_create with the per-node min_cntlid);
            # lock=False -> skip the mutation-lock helpers.
            result, err = snapshot_controller.clone(
                "snap-1", "CLN_test", namespaced=False, lock=False)

        self.assertEqual(err, False, f"clone() returned error: {err}")
        return calls

    def test_secondary_and_tertiary_get_distinct_index(self):
        """primary + secondary + tertiary: the two non-leaders must get
        distinct secondary_index values (0 and 1), so their cntlid windows
        (1000 and 2000) don't collide on the host."""
        calls = self._run_clone("node-sec", "node-ter")

        primary_calls = [c for c in calls if c[1] is True]
        secondary_calls = [c for c in calls if c[1] is False]

        self.assertEqual(len(primary_calls), 1, f"calls={calls}")
        self.assertEqual(len(secondary_calls), 2, f"calls={calls}")

        indices = sorted(c[2] for c in secondary_calls)
        # The regression: both secondaries previously got index 0.
        self.assertEqual(indices, [0, 1],
                         f"secondaries must get distinct indices; got {indices} "
                         f"(calls={calls})")
        self.assertNotEqual(indices[0], indices[1],
                            "secondary and tertiary share a cntlid window")

        # Map index -> node and assert the implied min_cntlid windows differ.
        by_node = {c[0]: c[2] for c in secondary_calls}
        min_cntlids = {n: 1000 * (i + 1) for n, i in by_node.items()}
        self.assertEqual(len(set(min_cntlids.values())), 2,
                         f"min_cntlid windows collide: {min_cntlids}")
        self.assertEqual(min_cntlids["node-sec"], 1000)
        self.assertEqual(min_cntlids["node-ter"], 2000)

    def test_single_secondary_unchanged(self):
        """FT=2 (primary + one secondary): the single secondary keeps index 0
        (min_cntlid 1000), disjoint from the primary's 1."""
        calls = self._run_clone("node-sec", None)
        secondary_calls = [c for c in calls if c[1] is False]
        self.assertEqual(len(secondary_calls), 1, f"calls={calls}")
        self.assertEqual(secondary_calls[0][2], 0, f"calls={calls}")


if __name__ == "__main__":
    unittest.main()
