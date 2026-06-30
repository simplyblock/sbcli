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

This is an integration test: the cluster / pool / storage nodes / source
snapshot are real records in FoundationDB (provisioned by the tier conftest),
and ``clone()`` drives the real ``db_controller`` reads/writes. Only the layer
*above* the database is mocked — the per-node SPDK registration
(``lvol_controller.add_lvol_on_node`` / ``is_node_leader``), the non-leader
readiness probe, and event emission — because the integration tier never talks
to a real storage node.
"""

import unittest
import uuid as uuid_mod
from unittest.mock import MagicMock, patch

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


def _make_node(db, cluster_id, node_id, secondary_id="", tertiary_id=""):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = cluster_id
    node.hostname = node_id
    node.status = StorageNode.STATUS_ONLINE
    node.max_lvol = 100
    node.lvstore_status = "ready"
    node.secondary_node_id = secondary_id or ""
    node.tertiary_node_id = tertiary_id or ""
    node.write_to_db(db.kv_store)
    return node


def _make_source_lvol(cluster_id, pool_id, node_id):
    """The snapshot's source lvol — HA, with every field clone() reads."""
    src = LVol()
    src.uuid = str(uuid_mod.uuid4())
    src.cluster_id = cluster_id
    src.pool_uuid = pool_id
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
    return src


def _make_snapshot(db, cluster_id, pool_id, src):
    snap = SnapShot()
    snap.uuid = "snap-1"
    snap.cluster_id = cluster_id
    snap.pool_uuid = pool_id
    snap.lvol = src
    snap.deleted = False
    snap.status = SnapShot.STATUS_ONLINE
    snap.size = src.size
    snap.snap_bdev = "LVS_100/SNAP_parent"
    snap.fabric = "tcp"
    snap.snap_ref_id = ""
    snap.ref_count = 0
    snap.write_to_db(db.kv_store)
    return snap


class TestCloneSecondaryCntlidIndex(unittest.TestCase):

    def setUp(self):
        self.db = DBController()
        if self.db.kv_store is None:
            self.skipTest("FoundationDB is not available")
        # Per-test isolation: both tests reuse the same clone name / snapshot id,
        # so wipe the user keyspace before seeding fresh state.
        self.db.kv_store.clear_range(b"\x00", b"\xff")

    def _seed(self, secondary_id, tertiary_id):
        cluster = Cluster()
        cluster.uuid = "cluster-1"
        cluster.status = Cluster.STATUS_ACTIVE
        cluster.nqn = "nqn.test:cluster-1"
        cluster.write_to_db(self.db.kv_store)

        pool = Pool()
        pool.uuid = "pool-1"
        pool.pool_name = "pool-1"
        pool.cluster_id = cluster.get_id()
        pool.status = Pool.STATUS_ACTIVE
        pool.lvol_max_size = 0
        pool.pool_max_size = 0
        pool.write_to_db(self.db.kv_store)

        primary_id = "node-primary"
        _make_node(self.db, cluster.get_id(), primary_id,
                   secondary_id=secondary_id, tertiary_id=tertiary_id)
        if secondary_id:
            _make_node(self.db, cluster.get_id(), secondary_id)
        if tertiary_id:
            _make_node(self.db, cluster.get_id(), tertiary_id)

        src = _make_source_lvol(cluster.get_id(), pool.get_id(), primary_id)
        _make_snapshot(self.db, cluster.get_id(), pool.get_id(), src)

    def _run_clone(self, secondary_id, tertiary_id):
        """Drive clone() through the HA registration block against real FDB and
        return the list of (node_id, is_primary, secondary_index) tuples passed
        to add_lvol_on_node."""
        from simplyblock_core.controllers import snapshot_controller

        self._seed(secondary_id, tertiary_id)

        calls = []

        def _record_add(lvol, node, is_primary=True, secondary_index=0):
            calls.append((node.get_id(), is_primary, secondary_index))
            return ({"uuid": "bdev-uuid",
                     "driver_specific": {"lvol": {"blobid": 1}}}, None)

        lvol_ctrl = MagicMock()
        lvol_ctrl.add_lvol_on_node.side_effect = _record_add
        # Leader detection iterates candidates and breaks on the first truthy
        # result; the host (primary) is first, so a blanket True elects it.
        lvol_ctrl.is_node_leader.return_value = True
        lvol_ctrl.get_next_available_subsystem_on_node.return_value = None
        # clone() now counts the node's lvol subsystems directly via the data
        # plane (count_lvol_subsystems) instead of scanning all_lvols; 0 => the
        # host is well under its max_lvol so the limit check passes.
        lvol_ctrl.count_lvol_subsystems.return_value = 0

        with patch.object(snapshot_controller, "lvol_controller", lvol_ctrl), \
             patch.object(snapshot_controller, "snapshot_events", MagicMock()), \
             patch.object(snapshot_controller.utils, "get_random_vuid",
                          return_value=12345), \
             patch("simplyblock_core.storage_node_ops.check_non_leader_for_operation",
                   return_value="proceed"), \
             patch("simplyblock_core.storage_node_ops.queue_for_restart_drain",
                   MagicMock()):
            # namespaced=False -> each clone creates its own subsystem (the
            # path that calls subsystem_create with the per-node min_cntlid);
            # lock=False -> skip the lvstore-lock helpers.
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
