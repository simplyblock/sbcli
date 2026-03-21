import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


class _FakeLVol:
    instances = []
    STATUS_IN_CREATION = "in_creation"
    STATUS_IN_DELETION = "in_deletion"

    def __init__(self):
        self.removed = False
        self.write_count = 0
        type(self).instances.append(self)

    def write_to_db(self, *args, **kwargs):
        self.write_count += 1

    def remove(self, *args, **kwargs):
        self.removed = True

    def get_id(self):
        return self.uuid


class TestSnapshotControllerRegressions(unittest.TestCase):

    def setUp(self):
        _FakeLVol.instances = []

    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_clone_reuses_existing_clone_for_same_snapshot(self, mock_db):
        from simplyblock_core.controllers import snapshot_controller

        snap_lvol = SimpleNamespace(pool_uuid="pool-1", node_id="node-1")
        snap = SimpleNamespace(lvol=snap_lvol, ref_count=0, snap_ref_id="", size=1024)
        pool = SimpleNamespace(status="active", cluster_id="cluster-1", lvol_max_size=0, pool_max_size=0, get_id=lambda: "pool-1")
        cluster = SimpleNamespace(status="active", STATUS_ACTIVE="active", STATUS_DEGRADED="degraded")
        snode = MagicMock()
        snode.lvol_sync_del.return_value = False
        snode.get_id.return_value = "node-1"
        snode.max_lvol = 100
        existing = SimpleNamespace(
            pool_uuid="pool-1",
            lvol_name="clone-a",
            cloned_from_snap="snap-1",
            status="online",
            get_id=lambda: "existing-clone-id",
        )

        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_pool_by_id.return_value = pool
        mock_db.get_storage_node_by_id.return_value = snode
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_lvols.return_value = [existing]

        clone_id, error = snapshot_controller.clone("snap-1", "clone-a")

        self.assertEqual(clone_id, "existing-clone-id")
        self.assertFalse(error)

    @patch("simplyblock_core.controllers.snapshot_controller.LVol", _FakeLVol)
    @patch("simplyblock_core.controllers.snapshot_controller.lvol_controller.delete_lvol_from_node")
    @patch("simplyblock_core.controllers.snapshot_controller.lvol_controller.add_lvol_on_node")
    @patch("simplyblock_core.controllers.snapshot_controller.lvol_controller.is_node_leader")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_clone_rolls_back_primary_when_secondary_registration_fails(
            self, mock_db, mock_is_leader, mock_add_lvol_on_node, mock_delete_lvol_from_node):
        from simplyblock_core.controllers import snapshot_controller

        host_node = MagicMock()
        host_node.status = "online"
        host_node.secondary_node_id = "sec-1"
        host_node.secondary_node_id_2 = ""
        host_node.hostname = "host-1"
        host_node.get_id.return_value = "node-1"
        host_node.lvol_sync_del.return_value = False
        host_node.max_lvol = 100

        sec_node = MagicMock()
        sec_node.status = "online"
        sec_node.get_id.return_value = "sec-1"

        snap_lvol = SimpleNamespace(
            pool_uuid="pool-1",
            node_id="node-1",
            size=1024,
            max_size=2048,
            base_bdev="base",
            lvs_name="LVS_1942",
            nodes=["node-1", "sec-1"],
            vuid=77,
            subsys_port=9100,
            allowed_hosts=[],
            ha_type="ha",
            crypto_bdev="",
            crypto_key1="",
            crypto_key2="",
        )
        snap = SimpleNamespace(
            lvol=snap_lvol,
            ref_count=0,
            snap_ref_id="",
            size=1024,
            fabric="tcp",
            snap_bdev="LVS_1942/SNAP_4",
        )
        pool = SimpleNamespace(status="active", cluster_id="cluster-1", lvol_max_size=0, pool_max_size=0, get_id=lambda: "pool-1")
        cluster = SimpleNamespace(status="active", STATUS_ACTIVE="active", STATUS_DEGRADED="degraded", nqn="nqn.test")

        def get_node(node_id):
            if node_id == "node-1":
                return host_node
            if node_id == "sec-1":
                return sec_node
            raise KeyError(node_id)

        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_pool_by_id.return_value = pool
        mock_db.get_storage_node_by_id.side_effect = get_node
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_lvols.return_value = []
        mock_db.get_lvols_by_node_id.return_value = []
        mock_db.kv_store = object()
        mock_is_leader.return_value = True
        mock_add_lvol_on_node.side_effect = [
            ({"uuid": "clone-uuid", "driver_specific": {"lvol": {"blobid": 4242}}}, None),
            (False, "Failed to create BDev: LVS_1942/CLN_4153"),
        ]

        clone_id, error = snapshot_controller.clone("snap-1", "clone-a")

        self.assertFalse(clone_id)
        self.assertIn("Failed to create BDev", error)
        created_lvol = _FakeLVol.instances[0]
        self.assertTrue(created_lvol.removed)
        mock_delete_lvol_from_node.assert_called_once_with(created_lvol.get_id(), "node-1")
        host_node.lvol_del_sync_lock.assert_called_once()
        host_node.lvol_del_sync_lock_reset.assert_called_once()

    @patch("simplyblock_core.controllers.snapshot_controller.utils.get_random_snapshot_vuid", return_value=777)
    @patch("simplyblock_core.controllers.snapshot_controller.snapshot_events")
    @patch("simplyblock_core.controllers.snapshot_controller.RPCClient")
    @patch("simplyblock_core.controllers.snapshot_controller.lvol_controller.is_node_leader")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_snapshot_add_rolls_back_registered_secondaries_on_partial_failure(
            self, mock_db, mock_is_leader, mock_rpc_client, _mock_events, _mock_vuid):
        from simplyblock_core.controllers import snapshot_controller

        host_node = MagicMock()
        host_node.status = "online"
        host_node.secondary_node_id = "sec-1"
        host_node.secondary_node_id_2 = "sec-2"
        host_node.get_id.return_value = "node-1"
        host_node.mgmt_ip = "10.0.0.1"
        host_node.rpc_port = 1
        host_node.rpc_username = "u"
        host_node.rpc_password = "p"
        host_node.lvol_sync_del.return_value = False

        sec1 = MagicMock()
        sec1.status = "online"
        sec1.get_id.return_value = "sec-1"
        sec1.mgmt_ip = "10.0.0.2"
        sec1.rpc_port = 2
        sec1.rpc_username = "u"
        sec1.rpc_password = "p"
        sec1.rpc_client.return_value.delete_lvol.return_value = (True, None)

        sec2 = MagicMock()
        sec2.status = "online"
        sec2.get_id.return_value = "sec-2"
        sec2.mgmt_ip = "10.0.0.3"
        sec2.rpc_port = 3
        sec2.rpc_username = "u"
        sec2.rpc_password = "p"

        lvol = SimpleNamespace(
            pool_uuid="pool-1",
            node_id="node-1",
            cloned_from_snap="",
            size=1024,
            ha_type="ha",
            lvs_name="LVS_1942",
            lvol_bdev="LVOL_1",
            get_id=lambda: "lvol-1",
            remove=lambda *args, **kwargs: None,
        )
        pool = SimpleNamespace(status="active", cluster_id="cluster-1", lvol_max_size=0, pool_max_size=0, get_id=lambda: "pool-1")
        cluster = SimpleNamespace(status="active", STATUS_ACTIVE="active", STATUS_DEGRADED="degraded", page_size_in_blocks=4096)

        primary_rpc = MagicMock()
        primary_rpc.lvol_create_snapshot.return_value = True
        primary_rpc.get_bdevs.return_value = [{
            "uuid": "snap-uuid",
            "driver_specific": {"lvol": {"blobid": 555, "num_allocated_clusters": 2}},
        }]
        primary_rpc.delete_lvol.return_value = (True, None)

        sec1_rpc = MagicMock()
        sec1_rpc.bdev_lvol_snapshot_register.return_value = True

        sec2_rpc = MagicMock()
        sec2_rpc.bdev_lvol_snapshot_register.return_value = False

        def get_node(node_id):
            if node_id == "node-1":
                return host_node
            if node_id == "sec-1":
                return sec1
            if node_id == "sec-2":
                return sec2
            raise KeyError(node_id)

        def make_rpc(ip, *args, **kwargs):
            if ip == "10.0.0.1":
                return primary_rpc
            if ip == "10.0.0.2":
                return sec1_rpc
            if ip == "10.0.0.3":
                return sec2_rpc
            raise AssertionError(ip)

        mock_db.get_lvol_by_id.return_value = lvol
        mock_db.get_pool_by_id.return_value = pool
        mock_db.get_cluster_by_id.return_value = cluster
        mock_db.get_storage_node_by_id.side_effect = get_node
        mock_db.get_snapshots.return_value = []
        mock_db.kv_store = object()
        mock_is_leader.return_value = True
        mock_rpc_client.side_effect = make_rpc

        snap_id, error = snapshot_controller.add("lvol-1", "snap-a")

        self.assertFalse(snap_id)
        self.assertIn("Failed to register snapshot", error)
        sec1.rpc_client.return_value.delete_lvol.assert_called_once_with("LVS_1942/SNAP_777")
        primary_rpc.delete_lvol.assert_called_once_with("LVS_1942/SNAP_777")
        host_node.lvol_del_sync_lock.assert_called_once()
        host_node.lvol_del_sync_lock_reset.assert_called_once()
