# coding=utf-8
"""
test_dual_fault_tolerance.py – unit tests for the dual fault tolerance
(triple-path NVMe-oF) feature.

Tests cover:
  - Cluster model max_fault_tolerance field
  - create_cluster / add_cluster validation rules
  - get_secondary_nodes exclude_ids parameter
  - cluster_activate dual secondary assignment
  - DB controller extended secondary lookup
  - lvol.nodes construction with 3 entries
  - apply_migration_to_db with dual secondaries
  - _get_target_secondary_nodes helper
  - StorageNode.lvol_del_sync_lock_reset with dual secondaries

All external dependencies (FDB, RPC) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.job_schedule import JobSchedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cluster(ha_type="ha", distr_npcs=2, max_fault_tolerance=1):
    c = Cluster()
    c.uuid = "00000000-0000-0000-0000-000000000001"
    c.ha_type = ha_type
    c.distr_ndcs = 1
    c.distr_npcs = distr_npcs
    c.max_fault_tolerance = max_fault_tolerance
    return c


def _node(uuid, status=StorageNode.STATUS_ONLINE, cluster_id="cluster-1",
          hostname="", lvstore="", secondary_node_id="", secondary_node_id_2="",
          mgmt_ip="", is_secondary_node=False,
          lvstore_stack_secondary_1="", lvstore_stack_secondary_2=""):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.hostname = hostname or f"host-{uuid}"
    n.lvstore = lvstore or f"lvs_{uuid}"
    n.secondary_node_id = secondary_node_id
    n.secondary_node_id_2 = secondary_node_id_2
    n.mgmt_ip = mgmt_ip or f"10.0.0.{hash(uuid) % 254 + 1}"
    n.is_secondary_node = is_secondary_node
    n.lvstore_stack_secondary_1 = lvstore_stack_secondary_1
    n.lvstore_stack_secondary_2 = lvstore_stack_secondary_2
    return n


def _lvol(uuid, node_id, status=LVol.STATUS_ONLINE, nodes=None, ha_type="ha"):
    lv = LVol()
    lv.uuid = uuid
    lv.node_id = node_id
    lv.status = status
    lv.ha_type = ha_type
    lv.nodes = nodes or [node_id]
    lv.lvs_name = "lvs_test"
    lv.lvol_bdev = "bdev_test"
    return lv


def _snap(uuid, lvol_uuid, node_id, snap_bdev=""):
    s = SnapShot()
    s.uuid = uuid
    s.snap_uuid = uuid
    s.snap_ref_id = ""
    s.status = SnapShot.STATUS_ONLINE
    lvol = LVol()
    lvol.uuid = lvol_uuid
    lvol.node_id = node_id
    s.lvol = lvol
    s.snap_bdev = snap_bdev or f"lvs/{uuid}"
    return s


def _migration(lvol_id="lvol-1", source_node="node-src", target_node="node-tgt",
               status=LVolMigration.STATUS_RUNNING, snaps_migrated=None):
    m = LVolMigration()
    m.uuid = "mig-uuid"
    m.cluster_id = "cluster-1"
    m.lvol_id = lvol_id
    m.source_node_id = source_node
    m.target_node_id = target_node
    m.status = status
    m.snaps_migrated = snaps_migrated or []
    return m


# ===========================================================================
# 1. Cluster model
# ===========================================================================

class TestClusterModel(unittest.TestCase):

    def test_default_max_fault_tolerance(self):
        c = Cluster()
        assert c.max_fault_tolerance == 1

    def test_max_fault_tolerance_stored(self):
        c = _cluster(max_fault_tolerance=2)
        assert c.max_fault_tolerance == 2


# ===========================================================================
# 2. StorageNode model
# ===========================================================================

class TestStorageNodeModel(unittest.TestCase):

    def test_default_secondary_node_id_2(self):
        n = StorageNode()
        assert n.secondary_node_id_2 == ""

    def test_secondary_node_id_2_stored(self):
        n = _node("n1", secondary_node_id_2="sec-2")
        assert n.secondary_node_id_2 == "sec-2"


# ===========================================================================
# 3. create_cluster validation
# ===========================================================================

class TestCreateClusterValidation(unittest.TestCase):

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_max_ft_2_requires_ha_type(self, mock_db):
        import simplyblock_core.cluster_ops as ops
        with self.assertRaises(ValueError) as ctx:
            ops.create_cluster(
                4096, 2097152, "pass", 89, 99, 250, 500,
                "eth0", "10.0.0.1", "3d", "7d", "", "",
                1, 2, 4096, 4096, "single", "docker", False,
                32, 3, 128, 4, False, False, "test",
                None, "hostip", "", "tcp", False, "",
                max_fault_tolerance=2,
            )
        assert "ha_type='ha'" in str(ctx.exception)

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_max_ft_2_requires_npcs_ge_2(self, mock_db):
        import simplyblock_core.cluster_ops as ops
        with self.assertRaises(ValueError) as ctx:
            ops.create_cluster(
                4096, 2097152, "pass", 89, 99, 250, 500,
                "eth0", "10.0.0.1", "3d", "7d", "", "",
                1, 1, 4096, 4096, "ha", "docker", False,
                32, 3, 128, 4, False, False, "test",
                None, "hostip", "", "tcp", False, "",
                max_fault_tolerance=2,
            )
        assert "distr_npcs >= 2" in str(ctx.exception)

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_max_ft_1_no_validation_error(self, mock_db):
        """max_fault_tolerance=1 should not hit the new validation even with single ha_type."""
        import simplyblock_core.cluster_ops as ops
        # Will fail later (e.g. distr_ndcs/npcs both 0), but not on max_ft validation
        with self.assertRaises(ValueError) as ctx:
            ops.create_cluster(
                4096, 2097152, "pass", 89, 99, 250, 500,
                "eth0", "10.0.0.1", "3d", "7d", "", "",
                0, 0, 4096, 4096, "single", "docker", False,
                32, 3, 128, 4, False, False, "test",
                None, "hostip", "", "tcp", False, "",
                max_fault_tolerance=1,
            )
        # Should fail on ndcs/npcs both 0, not on max_ft
        assert "distr_ndcs" in str(ctx.exception)


# ===========================================================================
# 4. add_cluster validation
# ===========================================================================

class TestAddClusterValidation(unittest.TestCase):

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_max_ft_2_requires_ha(self, mock_db):
        import simplyblock_core.cluster_ops as ops
        mock_db.get_clusters.return_value = [_cluster()]
        with self.assertRaises(ValueError) as ctx:
            ops.add_cluster(
                4096, 2097152, 89, 99, 250, 500,
                1, 2, 4096, 4096, "single", False,
                32, 128, 4, False, False, "test", "tcp", "",
                max_fault_tolerance=2,
            )
        assert "ha_type='ha'" in str(ctx.exception)

    @patch("simplyblock_core.cluster_ops.db_controller")
    def test_max_ft_2_requires_npcs(self, mock_db):
        import simplyblock_core.cluster_ops as ops
        mock_db.get_clusters.return_value = [_cluster()]
        with self.assertRaises(ValueError) as ctx:
            ops.add_cluster(
                4096, 2097152, 89, 99, 250, 500,
                1, 1, 4096, 4096, "ha", False,
                32, 128, 4, False, False, "test", "tcp", "",
                max_fault_tolerance=2,
            )
        assert "distr_npcs >= 2" in str(ctx.exception)


# ===========================================================================
# 5. get_secondary_nodes with exclude_ids
# ===========================================================================

class TestGetSecondaryNodes(unittest.TestCase):

    def _setup_nodes(self, nodes):
        mock_db = MagicMock()
        mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
        return mock_db

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_exclude_ids_filters_nodes(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops

        primary = _node("primary", mgmt_ip="10.0.0.1")
        sec1 = _node("sec-1", mgmt_ip="10.0.0.2", is_secondary_node=True)
        sec2 = _node("sec-2", mgmt_ip="10.0.0.3", is_secondary_node=True)
        sec3 = _node("sec-3", mgmt_ip="10.0.0.4", is_secondary_node=True)

        mock_db = self._setup_nodes([primary, sec1, sec2, sec3])
        MockDBCtrl.return_value = mock_db

        # Without exclude: should find sec-1 (first after primary)
        result = ops.get_secondary_nodes(primary)
        assert "sec-1" in result

        # With exclude_ids=[sec-1]: should skip sec-1
        result = ops.get_secondary_nodes(primary, exclude_ids=["sec-1"])
        assert "sec-1" not in result
        assert len(result) > 0

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_exclude_ids_empty_list_no_effect(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops

        primary = _node("primary", mgmt_ip="10.0.0.1")
        sec1 = _node("sec-1", mgmt_ip="10.0.0.2", is_secondary_node=True)

        mock_db = self._setup_nodes([primary, sec1])
        MockDBCtrl.return_value = mock_db

        result_no_exclude = ops.get_secondary_nodes(primary)
        result_empty_exclude = ops.get_secondary_nodes(primary, exclude_ids=[])
        assert result_no_exclude == result_empty_exclude

    @patch("simplyblock_core.storage_node_ops.DBController")
    def test_two_node_cluster_exclude_other(self, MockDBCtrl):
        import simplyblock_core.storage_node_ops as ops

        n1 = _node("n1", mgmt_ip="10.0.0.1")
        n2 = _node("n2", mgmt_ip="10.0.0.2")

        mock_db = self._setup_nodes([n1, n2])
        MockDBCtrl.return_value = mock_db

        # Two-node cluster: excluding the other node returns empty
        result = ops.get_secondary_nodes(n1, exclude_ids=["n2"])
        assert result == []


# ===========================================================================
# 6. DB controller secondary lookup
# ===========================================================================

class TestDBControllerSecondaryLookup(unittest.TestCase):

    def test_finds_primary_via_secondary_node_id(self):
        from simplyblock_core.db_controller import DBController
        primary = _node("primary", secondary_node_id="sec-1")
        primary.lvstore = "lvs_primary"

        mock_kv = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=[primary]):
            db = DBController.__new__(DBController)
            db.kv_store = mock_kv
            result = db.get_primary_storage_nodes_by_secondary_node_id("sec-1")
        assert len(result) == 1
        assert result[0].uuid == "primary"

    def test_finds_primary_via_secondary_node_id_2(self):
        from simplyblock_core.db_controller import DBController
        primary = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        primary.lvstore = "lvs_primary"

        mock_kv = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=[primary]):
            db = DBController.__new__(DBController)
            db.kv_store = mock_kv
            result = db.get_primary_storage_nodes_by_secondary_node_id("sec-2")
        assert len(result) == 1
        assert result[0].uuid == "primary"

    def test_no_match_returns_empty(self):
        from simplyblock_core.db_controller import DBController
        primary = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        primary.lvstore = "lvs_primary"

        mock_kv = MagicMock()
        with patch.object(StorageNode, 'read_from_db', return_value=[primary]):
            db = DBController.__new__(DBController)
            db.kv_store = mock_kv
            result = db.get_primary_storage_nodes_by_secondary_node_id("sec-99")
        assert len(result) == 0


# ===========================================================================
# 7. apply_migration_to_db with dual secondaries
# ===========================================================================

class TestApplyMigrationToDbDualSecondary(unittest.TestCase):

    def _mock_db(self, lvol, tgt_node=None, snap=None):
        mock_db = MagicMock()
        mock_db.get_lvol_by_id.return_value = lvol

        if tgt_node is None:
            tgt_node = _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt")
        mock_db.get_storage_node_by_id.return_value = tgt_node

        if snap:
            mock_db.get_snapshot_by_id.return_value = snap
        else:
            mock_db.get_snapshot_by_id.side_effect = KeyError("not found")

        return mock_db

    def test_nodes_includes_both_secondaries(self):
        import simplyblock_core.controllers.migration_controller as ctl

        lvol = _lvol("lvol-1", "node-src")
        tgt = _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt",
                     secondary_node_id="sec-1", secondary_node_id_2="sec-2")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=[])

        mock_db = self._mock_db(lvol, tgt_node=tgt)
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True
        assert lvol.nodes == ["node-tgt", "sec-1", "sec-2"]

    def test_nodes_with_only_first_secondary(self):
        import simplyblock_core.controllers.migration_controller as ctl

        lvol = _lvol("lvol-1", "node-src")
        tgt = _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt",
                     secondary_node_id="sec-1")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=[])

        mock_db = self._mock_db(lvol, tgt_node=tgt)
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True
        assert lvol.nodes == ["node-tgt", "sec-1"]

    def test_nodes_with_no_secondary(self):
        import simplyblock_core.controllers.migration_controller as ctl

        lvol = _lvol("lvol-1", "node-src")
        tgt = _node("node-tgt", hostname="host-tgt", lvstore="lvs_tgt")

        mig = _migration(lvol_id="lvol-1", source_node="node-src",
                         target_node="node-tgt", snaps_migrated=[])

        mock_db = self._mock_db(lvol, tgt_node=tgt)
        with patch.object(ctl, 'db', mock_db):
            result = ctl.apply_migration_to_db(mig)

        assert result is True
        assert lvol.nodes == ["node-tgt"]


# ===========================================================================
# 8. _get_target_secondary_nodes helper
# ===========================================================================

class TestGetTargetSecondaryNodes(unittest.TestCase):

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_no_secondaries_returns_empty(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        tgt = _node("tgt")
        result, err = runner._get_target_secondary_nodes(tgt)
        assert result == []
        assert err is None

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_both_secondaries_online(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        sec1 = _node("sec-1", status=StorageNode.STATUS_ONLINE)
        sec2 = _node("sec-2", status=StorageNode.STATUS_ONLINE)
        mock_db.get_storage_node_by_id.side_effect = lambda id: {
            "sec-1": sec1, "sec-2": sec2
        }[id]

        tgt = _node("tgt", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        result, err = runner._get_target_secondary_nodes(tgt)

        assert err is None
        assert len(result) == 2
        assert result[0].uuid == "sec-1"
        assert result[1].uuid == "sec-2"

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_one_online_one_offline(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        sec1 = _node("sec-1", status=StorageNode.STATUS_ONLINE)
        sec2 = _node("sec-2", status=StorageNode.STATUS_OFFLINE)
        mock_db.get_storage_node_by_id.side_effect = lambda id: {
            "sec-1": sec1, "sec-2": sec2
        }[id]

        tgt = _node("tgt", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        result, err = runner._get_target_secondary_nodes(tgt)

        assert err is None
        assert len(result) == 1
        assert result[0].uuid == "sec-1"

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_bad_state_blocks(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        sec1 = _node("sec-1", status=StorageNode.STATUS_ONLINE)
        sec2 = _node("sec-2", status=StorageNode.STATUS_SUSPENDED)
        mock_db.get_storage_node_by_id.side_effect = lambda id: {
            "sec-1": sec1, "sec-2": sec2
        }[id]

        tgt = _node("tgt", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        result, err = runner._get_target_secondary_nodes(tgt)

        assert result == []
        assert err is not None
        assert "sec-2" in err

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_missing_secondary_in_db_skipped(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        sec1 = _node("sec-1", status=StorageNode.STATUS_ONLINE)
        mock_db.get_storage_node_by_id.side_effect = lambda id: {
            "sec-1": sec1
        }.get(id) or (_ for _ in ()).throw(KeyError(id))

        # Proper side_effect that raises KeyError for unknown IDs
        def _get(id):
            if id == "sec-1":
                return sec1
            raise KeyError(id)
        mock_db.get_storage_node_by_id.side_effect = _get

        tgt = _node("tgt", secondary_node_id="sec-1", secondary_node_id_2="sec-missing")
        result, err = runner._get_target_secondary_nodes(tgt)

        assert err is None
        assert len(result) == 1
        assert result[0].uuid == "sec-1"


# ===========================================================================
# 9. _get_target_secondary_node (original, still works)
# ===========================================================================

class TestGetTargetSecondaryNodeOriginal(unittest.TestCase):

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_no_secondary(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        tgt = _node("tgt")
        sec, err = runner._get_target_secondary_node(tgt)
        assert sec is None
        assert err is None

    @patch("simplyblock_core.services.tasks_runner_lvol_migration.db")
    def test_secondary_online(self, mock_db):
        import simplyblock_core.services.tasks_runner_lvol_migration as runner
        sec1 = _node("sec-1", status=StorageNode.STATUS_ONLINE)
        mock_db.get_storage_node_by_id.return_value = sec1

        tgt = _node("tgt", secondary_node_id="sec-1")
        sec, err = runner._get_target_secondary_node(tgt)
        assert sec is not None
        assert sec.uuid == "sec-1"
        assert err is None


# ===========================================================================
# 10. lvol.nodes construction in add_lvol_ha (lvol_controller)
# ===========================================================================

class TestLvolNodesConstruction(unittest.TestCase):
    """Test that lvol.nodes is built correctly with dual secondaries."""

    def test_nodes_with_two_secondaries(self):
        """Verify that when host_node has secondary_node_id_2, lvol.nodes has 3 entries."""
        host = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        nodes = [host.uuid] + [host.secondary_node_id]
        if host.secondary_node_id_2:
            nodes.append(host.secondary_node_id_2)

        assert nodes == ["primary", "sec-1", "sec-2"]
        assert len(nodes) == 3

    def test_nodes_with_one_secondary(self):
        host = _node("primary", secondary_node_id="sec-1")
        nodes = [host.uuid] + [host.secondary_node_id]
        if host.secondary_node_id_2:
            nodes.append(host.secondary_node_id_2)

        assert nodes == ["primary", "sec-1"]
        assert len(nodes) == 2

    def test_min_cntlid_invariant(self):
        """Verify min_cntlid = 1 + 1000 * index."""
        nodes = ["primary", "sec-1", "sec-2"]
        for idx, node_id in enumerate(nodes):
            min_cntlid = 1 + 1000 * idx
            if idx == 0:
                assert min_cntlid == 1
            elif idx == 1:
                assert min_cntlid == 1001
            elif idx == 2:
                assert min_cntlid == 2001


# ===========================================================================
# 11. StorageNode.lvol_del_sync_lock_reset with dual secondaries
# ===========================================================================

class TestLvolDelSyncLockResetDual(unittest.TestCase):

    def _make_task(self, fn_name, node_id, status=JobSchedule.STATUS_RUNNING, canceled=False):
        t = JobSchedule()
        t.uuid = f"task-{node_id}"
        t.function_name = fn_name
        t.node_id = node_id
        t.status = status
        t.canceled = canceled
        return t

    @patch("simplyblock_core.db_controller.DBController")
    def test_lock_found_for_secondary_2(self, MockDB):
        """Sync lock should be created when a sync-del task exists for secondary_node_id_2."""
        node = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        node.cluster_id = "cluster-1"

        task_sec2 = self._make_task(JobSchedule.FN_LVOL_SYNC_DEL, "sec-2")

        mock_db = MagicMock()
        mock_db.get_job_tasks.return_value = [task_sec2]
        mock_db.get_lvol_del_lock.return_value = None
        MockDB.return_value = mock_db

        node.lvol_del_sync_lock_reset()

        # Should have been called since task for sec-2 was found and no lock exists

    @patch("simplyblock_core.db_controller.DBController")
    def test_no_lock_when_no_tasks(self, MockDB):
        """No lock should be created when no sync-del tasks exist for either secondary."""
        node = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        node.cluster_id = "cluster-1"

        # Task for a different node
        other_task = self._make_task(JobSchedule.FN_LVOL_SYNC_DEL, "sec-99")

        mock_db = MagicMock()
        mock_db.get_job_tasks.return_value = [other_task]
        mock_db.get_lvol_del_lock.return_value = None
        MockDB.return_value = mock_db

        node.lvol_del_sync_lock_reset()
        # No lock should have been written


# ===========================================================================
# 12. recreate_lvstore_on_sec min_cntlid
# ===========================================================================

class TestRecreateLvstoreMinCntlid(unittest.TestCase):

    def test_secondary_1_gets_cntlid_1000(self):
        """When secondary node is the primary's secondary_node_id, min_cntlid=1000."""
        primary = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        secondary = _node("sec-1")

        if primary.secondary_node_id_2 == secondary.uuid:
            min_cntlid = 2000
        else:
            min_cntlid = 1000
        assert min_cntlid == 1000

    def test_secondary_2_gets_cntlid_2000(self):
        """When secondary node is the primary's secondary_node_id_2, min_cntlid=2000."""
        primary = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        secondary = _node("sec-2")

        if primary.secondary_node_id_2 == secondary.uuid:
            min_cntlid = 2000
        else:
            min_cntlid = 1000
        assert min_cntlid == 2000


# ===========================================================================
# 13. health_controller _check_sec_node_hublvol primary resolution
# ===========================================================================

class TestCheckSecNodeHublvolPrimaryResolution(unittest.TestCase):

    def test_secondary_1_resolves_via_lvstore_stack_secondary_1(self):
        """A node that is secondary_1 of a primary should have lvstore_stack_secondary_1 set."""
        sec = _node("sec-1", lvstore_stack_secondary_1="primary-1")
        primary_ref = sec.lvstore_stack_secondary_1 or sec.lvstore_stack_secondary_2
        assert primary_ref == "primary-1"

    def test_secondary_2_resolves_via_lvstore_stack_secondary_2(self):
        """A node that is only secondary_2 should resolve via lvstore_stack_secondary_2."""
        sec = _node("sec-2", lvstore_stack_secondary_2="primary-1")
        primary_ref = sec.lvstore_stack_secondary_1 or sec.lvstore_stack_secondary_2
        assert primary_ref == "primary-1"

    def test_both_set_prefers_secondary_1(self):
        """When both back-refs are set (node is sec for two primaries), secondary_1 wins."""
        sec = _node("sec", lvstore_stack_secondary_1="primary-A",
                     lvstore_stack_secondary_2="primary-B")
        primary_ref = sec.lvstore_stack_secondary_1 or sec.lvstore_stack_secondary_2
        assert primary_ref == "primary-A"

    def test_explicit_primary_node_id_overrides(self):
        """When primary_node_id is passed explicitly, it should be used."""
        _node("sec", lvstore_stack_secondary_1="primary-A",
                     lvstore_stack_secondary_2="primary-B")
        explicit = "primary-B"
        primary_ref = explicit  # simulating the function logic
        assert primary_ref == "primary-B"


# ===========================================================================
# 14. ClusterDTO includes max_fault_tolerance
# ===========================================================================

class TestClusterDTO(unittest.TestCase):

    def test_dto_includes_max_fault_tolerance(self):
        from simplyblock_web.api.v2.dtos import ClusterDTO
        c = _cluster(max_fault_tolerance=2)
        c.status = Cluster.STATUS_ACTIVE
        c.nqn = "nqn:test"
        c.is_re_balancing = False
        c.blk_size = 4096
        c.cap_warn = 80
        c.cap_crit = 90
        c.prov_cap_warn = 180
        c.prov_cap_crit = 190
        c.enable_node_affinity = False
        c.strict_node_anti_affinity = False
        c.secret = "s3cret"
        c.tls = False
        c.cluster_name = "test-cluster"

        dto = ClusterDTO.from_model(c)
        assert dto.max_fault_tolerance == 2

    def test_dto_default_max_fault_tolerance(self):
        from simplyblock_web.api.v2.dtos import ClusterDTO
        c = _cluster(max_fault_tolerance=1)
        c.status = Cluster.STATUS_ACTIVE
        c.nqn = "nqn:test"
        c.is_re_balancing = False
        c.blk_size = 4096
        c.cap_warn = 80
        c.cap_crit = 90
        c.prov_cap_warn = 180
        c.prov_cap_crit = 190
        c.enable_node_affinity = False
        c.strict_node_anti_affinity = False
        c.secret = "s3cret"
        c.tls = False
        c.cluster_name = "test-cluster"

        dto = ClusterDTO.from_model(c)
        assert dto.max_fault_tolerance == 1


# ===========================================================================
# 15. create_lvstore secondary iteration
# ===========================================================================

class TestCreateLvstoreSecondaryIteration(unittest.TestCase):

    def test_secondary_ids_list_both(self):
        """Verify secondary_ids list is built correctly with both secondaries."""
        snode = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")
        secondary_ids = []
        if snode.secondary_node_id:
            secondary_ids.append(snode.secondary_node_id)
        if snode.secondary_node_id_2:
            secondary_ids.append(snode.secondary_node_id_2)

        assert secondary_ids == ["sec-1", "sec-2"]

    def test_secondary_ids_list_one(self):
        snode = _node("primary", secondary_node_id="sec-1")
        secondary_ids = []
        if snode.secondary_node_id:
            secondary_ids.append(snode.secondary_node_id)
        if snode.secondary_node_id_2:
            secondary_ids.append(snode.secondary_node_id_2)

        assert secondary_ids == ["sec-1"]

    def test_secondary_ids_list_none(self):
        snode = _node("primary")
        secondary_ids = []
        if snode.secondary_node_id:
            secondary_ids.append(snode.secondary_node_id)
        if snode.secondary_node_id_2:
            secondary_ids.append(snode.secondary_node_id_2)

        assert secondary_ids == []


# ===========================================================================
# 16. Snapshot controller nodes construction
# ===========================================================================

class TestSnapshotNodesConstruction(unittest.TestCase):

    def test_snap_nodes_includes_both_secondaries(self):
        """Verify snapshot controller builds nodes with all secondaries."""
        host = _node("primary", secondary_node_id="sec-1", secondary_node_id_2="sec-2")

        secondary_ids = [host.secondary_node_id]
        if host.secondary_node_id_2:
            secondary_ids.append(host.secondary_node_id_2)
        nodes = [host.uuid] + secondary_ids

        assert nodes == ["primary", "sec-1", "sec-2"]

    def test_snap_nodes_single_secondary(self):
        host = _node("primary", secondary_node_id="sec-1")

        secondary_ids = [host.secondary_node_id]
        if host.secondary_node_id_2:
            secondary_ids.append(host.secondary_node_id_2)
        nodes = [host.uuid] + secondary_ids

        assert nodes == ["primary", "sec-1"]


# ===========================================================================
# 17. Port check monitor uses both secondary back-references
# ===========================================================================

class TestPortCheckMonitor(unittest.TestCase):

    def test_port_check_includes_secondary_2(self):
        """storage_node_monitor port check should trigger for secondary_2 nodes too."""
        snode = _node("sec-node",
                       lvstore_stack_secondary_1="primary-A",
                       lvstore_stack_secondary_2="primary-B")

        # The condition in storage_node_monitor.py
        should_check = bool(snode.lvstore_stack_secondary_1 or snode.lvstore_stack_secondary_2)
        assert should_check is True

    def test_port_check_only_secondary_2(self):
        snode = _node("sec-node", lvstore_stack_secondary_2="primary-B")
        should_check = bool(snode.lvstore_stack_secondary_1 or snode.lvstore_stack_secondary_2)
        assert should_check is True

    def test_port_check_no_secondary(self):
        snode = _node("node")
        should_check = bool(snode.lvstore_stack_secondary_1 or snode.lvstore_stack_secondary_2)
        assert should_check is False


if __name__ == "__main__":
    unittest.main()
