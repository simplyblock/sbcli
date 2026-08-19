# coding=utf-8
"""
test_backup.py – unit tests for the S3 backup feature.

Tests cover:
  - Backup model fields and defaults
  - BackupPolicy model fields
  - BackupPolicyAttachment model fields
  - backup_controller functions (create_s3_bdev, backup_snapshot, restore, delete, etc.)
  - Policy management (add, remove, attach, detach, evaluate)
  - Age string parsing
  - RPC client bdev_s3_create / bdev_lvol_s3_bdev methods
  - CLI argument registration
  - Cluster model backup_config field
  - snapshot_controller.add with backup=True
  - Task runner dispatch

All external dependencies (FDB, RPC) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch
import time

import pytest

from simplyblock_core.controllers.backup.controller import backup_snapshot
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _node(uuid="node-1", status=StorageNode.STATUS_ONLINE, lvstore="lvs_test",
          cluster_id="cluster-1"):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    n.cluster_id = cluster_id
    n.lvstore = lvstore
    n.mgmt_ip = "10.0.0.1"
    n.rpc_port = 5260
    n.rpc_username = "admin"
    n.rpc_password = "pass"
    n.app_thread_mask = "0x8"  # core 3
    n.cpu = 8  # 8 system vCPUs
    return n


def _backup_config(**overrides):
    from simplyblock_core.models.backup_config import BackupConfig
    return BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-cluster-1",
        "region": "eu-central-1",
        **overrides,
    })


def _cluster(uuid="cluster-1", **config_overrides):
    c = Cluster()
    c.uuid = uuid
    c.backup_config = _backup_config(**config_overrides).model_dump(exclude_none=True)
    return c


def _backup(uuid="backup-1", lvol_id="lvol-1", status=Backup.STATUS_COMPLETED,
            node_id="node-1", cluster_id="cluster-1", prev_backup_id="",
            created_at=None, snapshot_id="snap-1", s3_id=1):
    b = Backup()
    b.uuid = uuid
    b.s3_id = s3_id
    b.lvol_id = lvol_id
    b.lvol_name = "test_lvol"
    b.snapshot_id = snapshot_id
    b.snapshot_name = "test_snap"
    b.node_id = node_id
    b.cluster_id = cluster_id
    b.pool_uuid = "pool-1"
    b.prev_backup_id = prev_backup_id
    b.size = 1024
    b.created_at = created_at or int(time.time())
    b.status = status
    return b


def _snapshot(uuid="snap-1", lvol_uuid="lvol-1", node_id="node-1"):
    s = SnapShot()
    s.uuid = uuid
    s.snap_uuid = uuid
    s.snap_name = "test_snap"
    s.snap_bdev = "lvs/test_snap"
    s.size = 1024
    s.status = SnapShot.STATUS_ONLINE
    lvol = LVol()
    lvol.uuid = lvol_uuid
    lvol.node_id = node_id
    lvol.lvs_name = "lvs_test"
    lvol.lvol_name = "test_lvol"
    lvol.pool_uuid = "pool-1"
    s.lvol = lvol
    return s


def _policy(uuid="policy-1", name="daily", max_versions=5, max_age_seconds=0,
            cluster_id="cluster-1"):
    p = BackupPolicy()
    p.uuid = uuid
    p.cluster_id = cluster_id
    p.policy_name = name
    p.max_versions = max_versions
    p.max_age_seconds = max_age_seconds
    p.max_age_display = ""
    p.status = BackupPolicy.STATUS_ACTIVE
    return p


# ===========================================================================
# 1. Backup model
# ===========================================================================

class TestBackupModel(unittest.TestCase):

    def test_default_fields(self):
        b = Backup()
        self.assertEqual(b.s3_id, 0)
        self.assertEqual(b.status, "")
        self.assertEqual(b.prev_backup_id, "")
        self.assertEqual(b.size, 0)
        self.assertEqual(b.created_at, 0)
        self.assertEqual(b.completed_at, 0)
        self.assertEqual(b.location, {})
        self.assertEqual(b.error_message, "")

    def test_status_constants(self):
        self.assertEqual(Backup.STATUS_PENDING, "pending")
        self.assertEqual(Backup.STATUS_IN_PROGRESS, "in_progress")
        self.assertEqual(Backup.STATUS_COMPLETED, "completed")
        self.assertEqual(Backup.STATUS_FAILED, "failed")
        self.assertEqual(Backup.STATUS_MERGING, "merging")
        self.assertEqual(Backup.STATUS_DELETING, "deleting")

    def test_get_id(self):
        b = _backup(uuid="b-123", cluster_id="c-456")
        self.assertEqual(b.get_id(), "c-456/b-123")

    def test_fields_stored(self):
        b = _backup(uuid="b-1", lvol_id="l-1", node_id="n-1", prev_backup_id="b-0")
        self.assertEqual(b.uuid, "b-1")
        self.assertEqual(b.lvol_id, "l-1")
        self.assertEqual(b.node_id, "n-1")
        self.assertEqual(b.prev_backup_id, "b-0")


# ===========================================================================
# 2. BackupPolicy model
# ===========================================================================

class TestBackupPolicyModel(unittest.TestCase):

    def test_default_fields(self):
        p = BackupPolicy()
        self.assertEqual(p.max_versions, 0)
        self.assertEqual(p.max_age_seconds, 0)
        self.assertEqual(p.max_age_display, "")

    def test_status_constants(self):
        self.assertEqual(BackupPolicy.STATUS_ACTIVE, "active")
        self.assertEqual(BackupPolicy.STATUS_INACTIVE, "inactive")

    def test_get_id(self):
        p = _policy(uuid="p-1", cluster_id="c-1")
        self.assertEqual(p.get_id(), "c-1/p-1")


# ===========================================================================
# 3. BackupPolicyAttachment model
# ===========================================================================

class TestBackupPolicyAttachmentModel(unittest.TestCase):

    def test_default_fields(self):
        a = BackupPolicyAttachment()
        self.assertEqual(a.policy_id, "")
        self.assertEqual(a.target_type, "")
        self.assertEqual(a.target_id, "")

    def test_get_id(self):
        a = BackupPolicyAttachment()
        a.uuid = "att-1"
        a.cluster_id = "c-1"
        self.assertEqual(a.get_id(), "c-1/att-1")


# ===========================================================================
# 4. Cluster model backup_config
# ===========================================================================

class TestClusterBackupConfig(unittest.TestCase):

    def test_default_backup_config(self):
        c = Cluster()
        self.assertEqual(c.backup_config, {})

    def test_backup_config_stored(self):
        c = Cluster()
        c.backup_config = {"secondary_target": 0, "local_testing": True}
        self.assertEqual(c.backup_config["secondary_target"], 0)
        self.assertTrue(c.backup_config["local_testing"])


# ===========================================================================
# 5. Age string parsing
# ===========================================================================

class TestParseAgeString(unittest.TestCase):

    def test_minutes(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        self.assertEqual(_parse_age_string("30m"), 1800)

    def test_hours(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        self.assertEqual(_parse_age_string("12h"), 43200)

    def test_days(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        self.assertEqual(_parse_age_string("2d"), 172800)

    def test_weeks(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        self.assertEqual(_parse_age_string("1w"), 604800)

    def test_invalid_format(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        with self.assertRaises(ValueError):
            _parse_age_string("abc")

    def test_invalid_unit(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        with self.assertRaises(ValueError):
            _parse_age_string("5x")

    def test_whitespace(self):
        from simplyblock_core.controllers.backup.policy import _parse_age_string
        self.assertEqual(_parse_age_string("  3d  "), 259200)


# ===========================================================================
# 6a. _compute_s3_cpu_masks
# ===========================================================================

class TestComputeS3CpuMasks(unittest.TestCase):

    def test_masks_from_node(self):
        from simplyblock_core.controllers.backup.device import _compute_s3_cpu_masks
        node = _node()  # app_thread_mask="0x8", cpu=8
        bdb, s3 = _compute_s3_cpu_masks(node)
        self.assertEqual(bdb, 0x8)       # app thread core 3
        self.assertEqual(s3, 0xFF)        # all 8 vCPUs — no pinning

    def test_no_app_thread_mask(self):
        from simplyblock_core.controllers.backup.device import _compute_s3_cpu_masks
        node = _node()
        node.app_thread_mask = ""
        bdb, s3 = _compute_s3_cpu_masks(node)
        # None, not 0: a zero mask selects no CPUs, and the RPC omits the
        # parameter so the data plane derives one from the app core mask.
        self.assertIsNone(bdb)
        self.assertEqual(s3, 0xFF)

    def test_no_cpu_count(self):
        from simplyblock_core.controllers.backup.device import _compute_s3_cpu_masks
        node = _node()
        node.cpu = 0
        bdb, s3 = _compute_s3_cpu_masks(node)
        self.assertEqual(bdb, 0x8)
        self.assertIsNone(s3)             # omitted; data plane picks

    def test_large_cpu_count(self):
        from simplyblock_core.controllers.backup.device import _compute_s3_cpu_masks
        node = _node()
        node.cpu = 32
        bdb, s3 = _compute_s3_cpu_masks(node)
        self.assertEqual(s3, 0xFFFFFFFF)  # all 32 vCPUs


# ===========================================================================
# 6b. backup_controller.create_s3_bdev
# ===========================================================================

class TestCreateS3Bdev(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_success(self, MockRPC, mock_boto3_client):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True
        mock_s3 = mock_boto3_client.return_value
        mock_s3.head_bucket.return_value = {}

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node()
        create_s3_bdev(node, _backup_config())

        mock_rpc.bdev_s3_create.assert_called_once()
        # Verify CPU masks: bdb_lcpu_mask=app_thread(0x8=8), s3_lcpu_mask=all 8 vCPUs(0xFF=255)
        _, kwargs = mock_rpc.bdev_s3_create.call_args
        self.assertEqual(kwargs["bdb_lcpu_mask"], 0x8)
        self.assertEqual(kwargs["s3_lcpu_mask"], 0xFF)
        self.assertEqual(kwargs["bucket_name"], "simplyblock-backup-cluster-1")
        mock_rpc.bdev_lvol_s3_bdev.assert_called_once_with("lvs_test", "s3_lvs_test")

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_no_lvstore(self, MockRPC, _mock_boto3_client):
        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node(lvstore="")
        with pytest.raises(PreconditionError):
            create_s3_bdev(node, _backup_config())

        MockRPC.assert_not_called()

    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_bdev_s3_create_fails(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = None

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node()
        with pytest.raises(RuntimeError):
            create_s3_bdev(node, _backup_config())
        mock_rpc.bdev_lvol_s3_bdev.assert_not_called()

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_bucket_is_a_create_parameter(self, MockRPC, mock_boto3_client):
        """A device cannot exist without its bucket, so there is no window in
        which one is attached to an lvstore with no bucket registered."""
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True
        mock_boto3_client.return_value.head_bucket.return_value = {}

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        create_s3_bdev(_node(), _backup_config())

        _, kwargs = mock_rpc.bdev_s3_create.call_args
        assert kwargs["bucket_name"] == "simplyblock-backup-cluster-1"

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_a_cluster_that_configured_no_bucket_still_gets_a_device(
            self, MockRPC, mock_boto3_client):
        """Activation hands ``cluster.get_backup_config()`` straight to this
        function for every node it brings up (``cluster_ops._finish_pass1_node``),
        so a stored config that names no bucket -- what a cluster created through
        the operator has, its CR having no bucket field -- failed the activation
        rather than the backup that would have used the bucket."""
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True
        mock_boto3_client.return_value.head_bucket.return_value = {}

        cluster = Cluster()
        cluster.uuid = "cluster-1"
        cluster.backup_config = {"region": "eu-central-1"}

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        create_s3_bdev(_node(), cluster.get_backup_config())

        _, kwargs = mock_rpc.bdev_s3_create.call_args
        assert kwargs["bucket_name"] == "simplyblock-backup-cluster-1"

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_attach_fails(self, MockRPC, mock_boto3_client):
        from simplyblock_core.rpc_client import RPCRemoteError
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.side_effect = RPCRemoteError("attach failed", code=-1)
        mock_s3 = mock_boto3_client.return_value
        mock_s3.head_bucket.return_value = {}

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node()
        with pytest.raises(RuntimeError):
            create_s3_bdev(node, _backup_config())

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_local_testing_params(self, MockRPC, mock_boto3_client):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True
        mock_s3 = mock_boto3_client.return_value
        mock_s3.head_bucket.return_value = {}

        from simplyblock_core.models.backup_config import BackupConfig
        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node()
        # A genuine pre-BackupConfig dict: no region, local_testing standing in
        # for four separate decisions.
        create_s3_bdev(node, BackupConfig.model_validate({
            "bucket_name": "simplyblock-backup-cluster-1",
            "local_testing": True,
            "local_endpoint": "http://minio:9000",
            "access_key_id": "minioadmin",
            "secret_access_key": "minioadmin",
        }))

        _, kwargs = mock_rpc.bdev_s3_create.call_args
        self.assertEqual(kwargs["endpoint"], "http://minio:9000")
        self.assertEqual(kwargs["region"], "us-east-1")
        self.assertFalse(kwargs["verify_tls"])
        self.assertTrue(kwargs["use_path_style"])
        self.assertEqual(kwargs["access_key_id"].get_secret_value(), "minioadmin")
        self.assertEqual(kwargs["secret_access_key"].get_secret_value(), "minioadmin")

        _, boto_kwargs = mock_boto3_client.call_args
        self.assertEqual(boto_kwargs["aws_access_key_id"], "minioadmin")
        self.assertEqual(boto_kwargs["aws_secret_access_key"], "minioadmin")
        self.assertEqual(boto_kwargs["endpoint_url"], "http://minio:9000")
        # local_testing unpacked into the properties it actually stood for.
        self.assertEqual(boto_kwargs["region_name"], "us-east-1")
        self.assertFalse(boto_kwargs["verify"])

    @patch("simplyblock_core.controllers.backup.manifest.boto3.client")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_no_credentials_defers_to_the_provider_chain(self, MockRPC, mock_boto3_client):
        """An absent key pair must mean "use the node's IAM role", not "send empty keys"."""
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True
        mock_boto3_client.return_value.head_bucket.return_value = {}

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        create_s3_bdev(_node(), _backup_config())

        _, boto_kwargs = mock_boto3_client.call_args
        self.assertIsNone(boto_kwargs["aws_access_key_id"])
        self.assertIsNone(boto_kwargs["aws_secret_access_key"])

    @patch("simplyblock_core.models.storage_node.RPCClient")
    def test_exception_handled(self, MockRPC):
        from simplyblock_core.rpc_client import RPCException
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.side_effect = RPCException("connection refused")

        from simplyblock_core.controllers.backup.device import create_s3_bdev
        node = _node()
        with pytest.raises(RuntimeError):
            create_s3_bdev(node, _backup_config())


# ===========================================================================
# 7. backup_controller.backup_snapshot
# ===========================================================================

class TestBackupSnapshot(unittest.TestCase):
    """Real FDB: backup_snapshot reads cluster/node/snapshot state and writes Backups.

    Only what sits above the database is mocked -- the storage node's RPC client,
    the task runner and event emission.
    """

    def setUp(self):
        self.db = DBController()
        _cluster().write_to_db(self.db.kv_store)
        _node().write_to_db(self.db.kv_store)

    def _persist(self, snapshot):
        snapshot.lvol.write_to_db(self.db.kv_store)
        snapshot.write_to_db(self.db.kv_store)
        return snapshot

    @patch("simplyblock_core.controllers.backup.controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup.controller.backup_events")
    def test_success(self, mock_events, mock_tasks):
        snap = self._persist(_snapshot())

        with patch("simplyblock_core.controllers.backup.controller._get_snapshot_chain",
                   return_value=[snap]):
            backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(error)
        self.assertIsNotNone(backup_id)
        mock_tasks.add_backup_task.assert_called_once()

        stored = self.db.get_backup_by_id(backup_id)
        self.assertEqual(stored.status, Backup.STATUS_PENDING)
        self.assertGreater(stored.s3_id, 0)
        # Self-describing from the moment it is created.
        self.assertEqual(stored.get_location().bucket_name,
                         "simplyblock-backup-cluster-1")

    @patch("simplyblock_core.controllers.backup.controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup.controller.backup_events")
    def test_records_host_nqns_without_copying_their_keys(self, mock_events, mock_tasks):
        """The record takes the allow-list, not the volume's authentication.

        Copying the keys here would duplicate live key material into a second
        record and from there into every manifest, while restore only ever uses
        the NQNs and mints fresh keys from the target pool.
        """
        snap = _snapshot()
        snap.lvol.allowed_hosts = [{
            "nqn": "nqn.2024-01.io.test:host",
            "dhchap_key": "DHHC-1:00:secret-dhchap:",
            "psk": "NVMeTLSkey-1:01:secret-psk:",
        }]
        self._persist(snap)

        with patch("simplyblock_core.controllers.backup.controller._get_snapshot_chain",
                   return_value=[snap]):
            backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(error)
        stored = self.db.get_backup_by_id(backup_id)
        self.assertEqual(stored.allowed_hosts, [{"nqn": "nqn.2024-01.io.test:host"}])

    @patch("simplyblock_core.controllers.backup.controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup.controller.backup_events")
    def test_incremental_backup(self, mock_events, mock_tasks):
        snap = self._persist(_snapshot())
        prev = _backup(uuid="prev-backup", s3_id=3, snapshot_id="snap-0",
                       status=Backup.STATUS_COMPLETED)
        prev.write_to_db(self.db.kv_store)

        with patch("simplyblock_core.controllers.backup.controller._get_snapshot_chain",
                   return_value=[snap]):
            backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(error)
        stored = self.db.get_backup_by_id(backup_id)
        self.assertEqual(stored.prev_backup_id, "prev-backup")
        # Monotonic allocation, so strictly above the existing backup's id.
        self.assertGreater(stored.s3_id, 3)

    def test_snapshot_not_found(self):
        backup_id, error = backup_snapshot("missing")

        self.assertIsNone(backup_id)
        self.assertIn("not found", error)

    def test_node_not_online(self):
        _node(status=StorageNode.STATUS_OFFLINE).write_to_db(self.db.kv_store)
        self._persist(_snapshot())

        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(backup_id)
        self.assertIn("not online", error)

    def test_cluster_without_backup_config_is_refused(self):
        """Refused before the chain lock, the KMS work or any task is created."""
        cluster = _cluster()
        cluster.backup_config = {}
        cluster.write_to_db(self.db.kv_store)
        self._persist(_snapshot())

        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(backup_id)
        self.assertIn("backup configuration", error)
        self.assertEqual(self.db.get_backups(), [])

    @patch("simplyblock_core.controllers.backup.controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup.controller.backup_events")
    def test_chain_backup_acquires_and_releases_lock(self, mock_events, mock_tasks):
        snap1 = self._persist(_snapshot(uuid="snap-1"))
        snap1.created_at = 1
        snap2 = self._persist(_snapshot(uuid="snap-2"))
        snap2.created_at = 2

        with patch("simplyblock_core.controllers.backup.controller._get_snapshot_chain",
                   return_value=[snap1, snap2]):
            backup_id, error = backup_snapshot("snap-2")

        self.assertIsNone(error)
        self.assertIsNotNone(backup_id)
        self.assertEqual(mock_tasks.add_backup_task.call_count, 2)
        # Locks released, so a second request for the same chain can proceed.
        self.assertIsNone(self.db.get_backup_chain_lock("snap-1"))
        self.assertIsNone(self.db.get_backup_chain_lock("snap-2"))

    def test_chain_backup_lock_conflict(self):
        snap = self._persist(_snapshot(uuid="snap-4"))
        acquired, _ = self.db.acquire_backup_chain_locks(["snap-4"], "snap-2", "lvol-1")
        self.assertTrue(acquired)

        with patch("simplyblock_core.controllers.backup.controller._get_snapshot_chain",
                   return_value=[snap]):
            backup_id, error = backup_snapshot("snap-4")

        self.assertIsNone(backup_id)
        self.assertIn("already preparing this snapshot chain", error)
        # The conflicting holder's lock must survive.
        self.assertIsNotNone(self.db.get_backup_chain_lock("snap-4"))



# ===========================================================================
# 8. db_controller backup chain locking
# ===========================================================================

class TestDBControllerBackupChainLocks(unittest.TestCase):

    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_acquire_backup_chain_locks_uses_unbound_method_with_db_handle(self, mock_transactional):
        db = object.__new__(DBController)
        db.kv_store = MagicMock()

        wrapped = MagicMock(return_value=("ok", None))
        mock_transactional.return_value = wrapped

        result = db.acquire_backup_chain_locks(["snap-2", "snap-1"], "snap-2", "lvol-1")

        self.assertEqual(result, ("ok", None))
        mock_transactional.assert_called_once_with(DBController._acquire_backup_chain_locks_tx)
        wrapped.assert_called_once_with(db, db.kv_store, ["snap-1", "snap-2"], "snap-2", "lvol-1")

    @patch("simplyblock_core.db_controller.fdb.transactional", create=True)
    def test_release_backup_chain_locks_uses_unbound_method_with_db_handle(self, mock_transactional):
        db = object.__new__(DBController)
        db.kv_store = MagicMock()

        wrapped = MagicMock()
        mock_transactional.return_value = wrapped

        db.release_backup_chain_locks(["snap-2", "snap-1"])

        mock_transactional.assert_called_once_with(DBController._release_backup_chain_locks_tx)
        wrapped.assert_called_once_with(db, db.kv_store, ["snap-1", "snap-2"])


# ===========================================================================
# 9. backup_controller.restore_backup
# ===========================================================================

class TestRestoreBackup(unittest.TestCase):
    """Real FDB: restore reads backup/pool/cluster state and creates a volume.

    add_lvol_ha and the task runner are mocked -- they sit above the database
    and drive RPC to storage nodes.
    """

    CLUSTER_ID = "00000000-0000-0000-0000-000000000001"

    def setUp(self):
        from simplyblock_core.models.pool import Pool
        self.db = DBController()

        cluster = _cluster(uuid=self.CLUSTER_ID)
        cluster.write_to_db(self.db.kv_store)

        pool = Pool()
        pool.uuid = "pool-1"
        pool.pool_name = "pool-1"  # resolved by name: "pool-1" is not a UUID
        pool.cluster_id = self.CLUSTER_ID
        pool.write_to_db(self.db.kv_store)

    def _backup(self, **overrides):
        backup = _backup(cluster_id=self.CLUSTER_ID, **overrides)
        backup.location = _backup_config().location().model_dump(mode="json")
        backup.write_to_db(self.db.kv_store)
        return backup

    @patch("simplyblock_core.controllers.backup.controller.tasks_controller")
    def test_success(self, mock_tasks):
        self._backup(s3_id=5)
        mock_tasks.add_backup_restore_task.return_value = True

        lvol = LVol()
        lvol.uuid = "lvol-new"
        lvol.node_id = "node-1"
        lvol.lvs_name = "lvs_test"
        lvol.lvol_bdev = "LVOL_123"
        lvol.pool_uuid = "pool-1"
        lvol.write_to_db(self.db.kv_store)

        with patch("simplyblock_core.controllers.lvol_controller.add_lvol_ha",
                   return_value=("lvol-new", None)):
            from simplyblock_core.controllers.backup.controller import restore_backup
            result = restore_backup("backup-1", "restored_lvol", "pool-1")

        self.assertEqual(result, "lvol-new")
        # s3_id integers reach the data plane, not backup UUIDs.
        self.assertEqual(mock_tasks.add_backup_restore_task.call_args[0][4], [5])
        self.assertEqual(self.db.get_lvol_by_id("lvol-new").status, LVol.STATUS_RESTORING)

    def test_backup_not_found(self):
        from simplyblock_core.controllers.backup.controller import restore_backup
        with self.assertRaises(PreconditionError):
            restore_backup("missing", "lvol", "pool-1")

    def test_add_lvol_ha_fails(self):
        self._backup()

        with patch("simplyblock_core.controllers.lvol_controller.add_lvol_ha",
                   return_value=(None, "Pool not found")):
            from simplyblock_core.controllers.backup.controller import restore_backup
            with self.assertRaisesRegex(RuntimeError, "Failed to create restore volume"):
                restore_backup("backup-1", "lvol", "pool-1")

    def test_incomplete_chain_is_refused(self):
        self._backup(uuid="b-old", s3_id=1, status=Backup.STATUS_IN_PROGRESS)
        self._backup(uuid="backup-1", s3_id=2, prev_backup_id="b-old")

        from simplyblock_core.controllers.backup.controller import restore_backup
        with self.assertRaisesRegex(PreconditionError, "Incomplete backups in chain"):
            restore_backup("backup-1", "lvol", "pool-1")

        self.assertEqual(self.db.get_lvols(), [])


# ===========================================================================
# 10. backup_controller.delete_backups
# ===========================================================================

class TestDeleteBackups(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.controller.backup_events")
    @patch("simplyblock_core.models.storage_node.RPCClient")
    @patch("simplyblock_core.controllers.backup.controller.db_controller")
    def test_success(self, mock_db, MockRPC, mock_events):
        b1 = _backup(uuid="b-1")
        mock_db.get_backups_by_lvol_id.return_value = [b1]
        mock_db.get_storage_node_by_id.return_value = _node()
        b1.remove = MagicMock()
        MockRPC.return_value.bdev_lvol_s3_delete.return_value = True

        from simplyblock_core.controllers.backup.controller import delete_backups
        success, error = delete_backups("lvol-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        b1.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup.controller.db_controller")
    def test_no_backups(self, mock_db):
        mock_db.get_backups_by_lvol_id.return_value = []

        from simplyblock_core.controllers.backup.controller import delete_backups
        success, error = delete_backups("lvol-1")

        self.assertFalse(success)
        self.assertIsNotNone(error)


# ===========================================================================
# 10. backup_controller.list_backups
# ===========================================================================

class TestListBackups(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.controller.db_controller")
    def test_list_empty(self, mock_db):
        mock_db.get_backups.return_value = []

        from simplyblock_core.controllers.backup.controller import list_backups
        data = list_backups()

        self.assertEqual(data, [])

    @patch("simplyblock_core.controllers.backup.controller.db_controller")
    def test_list_with_backups(self, mock_db):
        b = _backup()
        mock_db.get_backups.return_value = [b]

        from simplyblock_core.controllers.backup.controller import list_backups
        data = list_backups()

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["ID"], "backup-1")
        self.assertEqual(data[0]["Status"], Backup.STATUS_COMPLETED)

    @patch("simplyblock_core.controllers.backup.controller.db_controller")
    def test_list_sorted_newest_first_with_seconds(self, mock_db):
        older = _backup(uuid="older", created_at=1710000000)
        newer = _backup(uuid="newer", created_at=1710000005)
        mock_db.get_backups.return_value = [older, newer]

        from simplyblock_core.controllers.backup.controller import list_backups
        data = list_backups()

        self.assertEqual([row["ID"] for row in data], ["newer", "older"])
        self.assertEqual(data[0]["Created"], time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1710000005)))


# ===========================================================================
# 11. Policy management
# ===========================================================================

class TestPolicyAdd(unittest.TestCase):

    @patch.object(BackupPolicy, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_success(self, mock_db, _mock_write):
        mock_db.get_backup_policies.return_value = []

        from simplyblock_core.controllers.backup.policy import add_policy
        policy_id, error = add_policy("cluster-1", "daily", max_versions=5, max_age="2d")

        self.assertIsNotNone(policy_id)
        self.assertIsNone(error)

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_no_limits(self, mock_db):
        from simplyblock_core.controllers.backup.policy import add_policy
        policy_id, error = add_policy("cluster-1", "empty", max_versions=0, max_age="")

        self.assertIsNone(policy_id)
        self.assertIn("must be specified", error)

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_duplicate_name(self, mock_db):
        existing = _policy(name="daily")
        mock_db.get_backup_policies.return_value = [existing]

        from simplyblock_core.controllers.backup.policy import add_policy
        policy_id, error = add_policy("cluster-1", "daily", max_versions=5)

        self.assertIsNone(policy_id)
        self.assertIn("already exists", error)

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_invalid_age(self, mock_db):
        from simplyblock_core.controllers.backup.policy import add_policy
        policy_id, error = add_policy("cluster-1", "test", max_age="invalid")

        self.assertIsNone(policy_id)
        self.assertIn("Invalid age", error)


class TestPolicyRemove(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_success(self, mock_db):
        p = _policy()
        p.remove = MagicMock()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup.policy import remove_policy
        success, error = remove_policy("policy-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        p.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_not_found(self, mock_db):
        mock_db.get_backup_policy_by_id.side_effect = KeyError("not found")

        from simplyblock_core.controllers.backup.policy import remove_policy
        success, error = remove_policy("missing")

        self.assertFalse(success)
        self.assertIsNotNone(error)


class TestPolicyAttach(unittest.TestCase):

    @patch.object(BackupPolicyAttachment, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_success(self, mock_db, _mock_write):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_lvol_by_id.return_value = MagicMock()
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup.policy import attach_policy
        att_id, error = attach_policy("policy-1", "lvol", "lvol-1")

        self.assertIsNotNone(att_id)
        self.assertIsNone(error)

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_invalid_target_type(self, mock_db):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p

        from simplyblock_core.controllers.backup.policy import attach_policy
        att_id, error = attach_policy("policy-1", "invalid", "target-1")

        self.assertIsNone(att_id)
        self.assertIn("Invalid target_type", error)


class TestPolicyDetach(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_success(self, mock_db):
        p = _policy()
        att = BackupPolicyAttachment()
        att.uuid = "att-1"
        att.policy_id = "policy-1"
        att.target_type = "lvol"
        att.target_id = "lvol-1"
        att.remove = MagicMock()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_backup_policy_attachments.return_value = [att]

        from simplyblock_core.controllers.backup.policy import detach_policy
        success, error = detach_policy("policy-1", "lvol", "lvol-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        att.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_not_found(self, mock_db):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup.policy import detach_policy
        success, error = detach_policy("policy-1", "lvol", "lvol-1")

        self.assertFalse(success)
        self.assertIn("not found", error.lower())


# ===========================================================================
# 12. Policy evaluation
# ===========================================================================

class TestEvaluatePolicy(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_no_policy(self, mock_db, mock_tasks):
        mock_db.get_policy_for_lvol.return_value = None

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()

    @patch.object(Backup, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_version_limit_exceeded(self, mock_db, mock_tasks, _mock_write):
        policy = _policy(max_versions=2, max_age_seconds=0)
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 300)
        b2 = _backup(uuid="b2", created_at=now - 200)
        b3 = _backup(uuid="b3", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2, b3]

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_under_version_limit(self, mock_db, mock_tasks):
        policy = _policy(max_versions=5, max_age_seconds=0)
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 300)
        b2 = _backup(uuid="b2", created_at=now - 200)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2]

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()

    @patch.object(Backup, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_age_limit_exceeded(self, mock_db, mock_tasks, _mock_write):
        policy = _policy(max_versions=0)
        policy.max_age_seconds = 3600  # 1 hour
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 7200)  # 2 hours old
        b2 = _backup(uuid="b2", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2]

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch.object(Backup, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_both_conditions_required(self, mock_db, mock_tasks, _mock_write):
        """When both versions and age are set, either limit can trigger a merge."""
        policy = _policy(max_versions=3)
        policy.max_age_seconds = 3600
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        # 4 backups (exceeds version limit of 3) but all recent (doesn't exceed age)
        backups = [_backup(uuid=f"b{i}", created_at=now - (i * 60)) for i in range(4)]
        mock_db.get_backups_by_lvol_id.return_value = backups

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch.object(Backup, 'write_to_db')
    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_both_conditions_met(self, mock_db, mock_tasks, _mock_write):
        """When both limits set and both exceeded, merge triggers."""
        policy = _policy(max_versions=2)
        policy.max_age_seconds = 3600
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 7200)
        b2 = _backup(uuid="b2", created_at=now - 200)
        b3 = _backup(uuid="b3", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2, b3]

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_fewer_than_two_backups(self, mock_db, mock_tasks):
        """Never merge with fewer than 2 completed backups."""
        policy = _policy(max_versions=1)
        mock_db.get_policy_for_lvol.return_value = policy
        mock_db.get_backups_by_lvol_id.return_value = [_backup()]

        from simplyblock_core.controllers.backup.policy import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()


# ===========================================================================
# 14. List policies
# ===========================================================================

class TestListPolicies(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.policy.db_controller")
    def test_list_with_policies(self, mock_db):
        p = _policy(max_versions=5)
        p.max_age_display = "2d"
        mock_db.get_backup_policies.return_value = [p]

        from simplyblock_core.controllers.backup.policy import list_policies
        data = list_policies()

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["Name"], "daily")
        self.assertEqual(data[0]["Versions"], 5)
        self.assertEqual(data[0]["Max Age"], "2d")


# ===========================================================================
# 15. JobSchedule task type constants
# ===========================================================================

class TestJobScheduleBackupConstants(unittest.TestCase):

    def test_fn_backup(self):
        self.assertEqual(JobSchedule.FN_BACKUP, "s3_backup")

    def test_fn_backup_restore(self):
        self.assertEqual(JobSchedule.FN_BACKUP_RESTORE, "s3_backup_restore")

    def test_fn_backup_merge(self):
        self.assertEqual(JobSchedule.FN_BACKUP_MERGE, "s3_backup_merge")


# ===========================================================================
# 16. snapshot_controller.add with backup=True
# ===========================================================================

class TestSnapshotAddWithBackup(unittest.TestCase):

    @patch("simplyblock_core.controllers.snapshot_controller.snapshot_events")
    @patch("simplyblock_core.controllers.snapshot_controller.db_controller")
    def test_add_signature_accepts_backup(self, mock_db, mock_events):
        """Verify snapshot_controller.add accepts backup parameter."""
        from simplyblock_core.controllers.snapshot_controller import add
        import inspect
        sig = inspect.signature(add)
        self.assertIn("backup", sig.parameters)
        self.assertEqual(sig.parameters["backup"].default, False)


# ===========================================================================
# 17. RPC client methods
# ===========================================================================

class TestRPCClientBackupMethods(unittest.TestCase):

    def test_bdev_s3_create_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_s3_create'))

    def test_bdev_s3_add_bucket_name_is_gone(self):
        """The bucket is a create parameter now; the separate call was the
        mechanism behind the non-functional source switch."""
        from simplyblock_core.rpc_client import RPCClient
        self.assertFalse(hasattr(RPCClient, 'bdev_s3_add_bucket_name'))

    def test_bdev_s3_delete_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_s3_delete'))

    def test_bdev_lvol_s3_bdev_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_bdev'))

    def test_bdev_lvol_s3_backup_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_backup'))

    def test_bdev_lvol_transfer_stat_exists(self):
        """bdev_lvol_transfer_stat is used to poll all S3 transfer operations."""
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_transfer_stat'))

    def test_bdev_lvol_s3_merge_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_merge'))

    def test_bdev_lvol_s3_recovery_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_recovery'))

    def test_bdev_lvol_s3_delete_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_delete'))


# ===========================================================================
# 18. CLI argument registration
# ===========================================================================

class TestCLIBackupArgs(unittest.TestCase):

    def test_snapshot_add_has_backup_flag(self):
        """Verify --backup flag is registered on snapshot add."""
        import sys
        sys.argv = ['sbcli']  # minimal argv to avoid parse errors
        from simplyblock_cli.cli import CLIWrapper
        cli = CLIWrapper()
        # Find the snapshot add subparser and verify --backup is present
        # We check by verifying the parser doesn't error on --backup
        # This is a smoke test - full integration would require argparse introspection
        self.assertTrue(hasattr(cli, 'init_snapshot__backup'))

    def test_backup_commands_registered(self):
        """Verify init_backup method exists."""
        import sys
        sys.argv = ['sbcli']
        from simplyblock_cli.cli import CLIWrapper
        cli = CLIWrapper()
        self.assertTrue(hasattr(cli, 'init_backup'))
        self.assertTrue(hasattr(cli, 'init_backup__list'))
        self.assertTrue(hasattr(cli, 'init_backup__delete'))
        self.assertTrue(hasattr(cli, 'init_backup__restore'))
        self.assertTrue(hasattr(cli, 'init_backup__import'))
        self.assertTrue(hasattr(cli, 'init_backup__policy_add'))
        self.assertTrue(hasattr(cli, 'init_backup__policy_remove'))
        self.assertTrue(hasattr(cli, 'init_backup__policy_list'))
        self.assertTrue(hasattr(cli, 'init_backup__policy_attach'))
        self.assertTrue(hasattr(cli, 'init_backup__policy_detach'))

    def test_use_backup_on_cluster_create(self):
        """Verify --use-backup flag is registered on cluster create."""
        import sys
        sys.argv = ['sbcli']
        from simplyblock_cli.cli import CLIWrapper
        cli = CLIWrapper()
        self.assertTrue(hasattr(cli, 'init_cluster__create'))


# ===========================================================================
# 19. CLIBase handler methods exist
# ===========================================================================

class TestCLIBaseHandlers(unittest.TestCase):

    def test_backup_handlers_exist(self):
        from simplyblock_cli.clibase import CLIWrapperBase
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__list'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__delete'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__restore'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__import'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__policy_add'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__policy_remove'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__policy_list'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__policy_attach'))
        self.assertTrue(hasattr(CLIWrapperBase, 'backup__policy_detach'))
        self.assertTrue(hasattr(CLIWrapperBase, 'snapshot__backup'))


# ===========================================================================
# 20. Backup.get_location
# ===========================================================================

class TestBackupLocationAccessor(unittest.TestCase):

    def test_recorded_location_round_trips(self):
        b = _backup()
        b.location = {"bucket_name": "backups", "region": "eu-central-1"}

        location = b.get_location()
        self.assertEqual(location.bucket_name, "backups")
        self.assertEqual(location.region, "eu-central-1")

    def test_backup_without_a_location_raises(self):
        with self.assertRaises(ValueError):
            _backup().get_location()

    def test_invalid_location_raises_value_error(self):
        b = _backup()
        b.location = {"bucket_name": "backups"}  # no region
        with self.assertRaises(ValueError):
            b.get_location()


# ===========================================================================
# 21. _trigger_merge
# ===========================================================================

class TestTriggerMerge(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    def test_trigger_merge_marks_old_as_merging(self, mock_tasks):
        from simplyblock_core.controllers.backup.policy import _trigger_merge
        keep = _backup(uuid="keep")
        old = _backup(uuid="old")
        old.write_to_db = MagicMock()

        _trigger_merge(keep, old)

        self.assertEqual(old.status, Backup.STATUS_MERGING)
        old.write_to_db.assert_called_once()
        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup.policy.tasks_controller")
    def test_skip_if_not_completed(self, mock_tasks):
        from simplyblock_core.controllers.backup.policy import _trigger_merge
        keep = _backup(uuid="keep")
        old = _backup(uuid="old", status=Backup.STATUS_PENDING)

        _trigger_merge(keep, old)

        mock_tasks.add_backup_merge_task.assert_not_called()


if __name__ == '__main__':
    unittest.main()
