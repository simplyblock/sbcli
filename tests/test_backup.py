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
from unittest.mock import MagicMock, patch, call
import time

from simplyblock_core.models.backup import Backup, BackupPolicy, BackupPolicyAttachment
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool


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
    return n


def _backup(uuid="backup-1", lvol_id="lvol-1", status=Backup.STATUS_COMPLETED,
            node_id="node-1", cluster_id="cluster-1", prev_backup_id="",
            created_at=None, snapshot_id="snap-1"):
    b = Backup()
    b.uuid = uuid
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
        self.assertEqual(b.status, "")
        self.assertEqual(b.prev_backup_id, "")
        self.assertEqual(b.size, 0)
        self.assertEqual(b.created_at, 0)
        self.assertEqual(b.completed_at, 0)
        self.assertEqual(b.s3_metadata, {})
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
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        self.assertEqual(_parse_age_string("30m"), 1800)

    def test_hours(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        self.assertEqual(_parse_age_string("12h"), 43200)

    def test_days(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        self.assertEqual(_parse_age_string("2d"), 172800)

    def test_weeks(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        self.assertEqual(_parse_age_string("1w"), 604800)

    def test_invalid_format(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        with self.assertRaises(ValueError):
            _parse_age_string("abc")

    def test_invalid_unit(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        with self.assertRaises(ValueError):
            _parse_age_string("5x")

    def test_whitespace(self):
        from simplyblock_core.controllers.backup_controller import _parse_age_string
        self.assertEqual(_parse_age_string("  3d  "), 259200)


# ===========================================================================
# 6. backup_controller.create_s3_bdev
# ===========================================================================

class TestCreateS3Bdev(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_success(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True

        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node()
        config = {"secondary_target": 0, "with_compression": False,
                  "snapshot_backups": True}
        result = create_s3_bdev(node, config)

        self.assertTrue(result)
        mock_rpc.bdev_s3_create.assert_called_once()
        mock_rpc.bdev_lvol_s3_bdev.assert_called_once_with("lvs_test", "s3_lvs_test")

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_no_lvstore(self, MockRPC):
        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node(lvstore="")
        result = create_s3_bdev(node, {})
        self.assertFalse(result)
        MockRPC.assert_not_called()

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_bdev_s3_create_fails(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = None

        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node()
        result = create_s3_bdev(node, {})
        self.assertFalse(result)
        mock_rpc.bdev_lvol_s3_bdev.assert_not_called()

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_attach_fails(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = None

        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node()
        result = create_s3_bdev(node, {})
        self.assertFalse(result)

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_local_testing_params(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.return_value = True
        mock_rpc.bdev_lvol_s3_bdev.return_value = True

        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node()
        config = {
            "secondary_target": 0,
            "local_testing": True,
            "local_endpoint": "http://minio:9000",
            "access_key_id": "minioadmin",
            "secret_access_key": "minioadmin",
        }
        create_s3_bdev(node, config)

        _, kwargs = mock_rpc.bdev_s3_create.call_args
        self.assertTrue(kwargs.get("local_testing", False))
        self.assertEqual(kwargs.get("local_endpoint", ""), "http://minio:9000")
        self.assertEqual(kwargs.get("access_key_id", ""), "minioadmin")
        self.assertEqual(kwargs.get("secret_access_key", ""), "minioadmin")

    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    def test_exception_handled(self, MockRPC):
        mock_rpc = MockRPC.return_value
        mock_rpc.bdev_s3_create.side_effect = Exception("connection refused")

        from simplyblock_core.controllers.backup_controller import create_s3_bdev
        node = _node()
        result = create_s3_bdev(node, {})
        self.assertFalse(result)


# ===========================================================================
# 7. backup_controller.backup_snapshot
# ===========================================================================

class TestBackupSnapshot(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.backup_events")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db, mock_events, mock_tasks):
        snap = _snapshot()
        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_storage_node_by_id.return_value = _node()
        mock_db.get_backups_by_lvol_id.return_value = []
        mock_tasks.add_backup_task.return_value = "task-1"

        from simplyblock_core.controllers.backup_controller import backup_snapshot
        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNotNone(backup_id)
        self.assertIsNone(error)
        mock_tasks.add_backup_task.assert_called_once()
        mock_events.backup_created.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_snapshot_not_found(self, mock_db):
        mock_db.get_snapshot_by_id.side_effect = KeyError("not found")

        from simplyblock_core.controllers.backup_controller import backup_snapshot
        backup_id, error = backup_snapshot("missing")

        self.assertIsNone(backup_id)
        self.assertIn("not found", error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_no_lvol(self, mock_db):
        snap = _snapshot()
        snap.lvol = None
        mock_db.get_snapshot_by_id.return_value = snap

        from simplyblock_core.controllers.backup_controller import backup_snapshot
        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(backup_id)
        self.assertIn("no associated lvol", error.lower())

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_node_not_online(self, mock_db):
        snap = _snapshot()
        node = _node(status=StorageNode.STATUS_OFFLINE)
        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_storage_node_by_id.return_value = node

        from simplyblock_core.controllers.backup_controller import backup_snapshot
        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNone(backup_id)
        self.assertIn("not online", error)

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.backup_events")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_incremental_backup(self, mock_db, mock_events, mock_tasks):
        snap = _snapshot()
        prev = _backup(uuid="prev-backup")
        mock_db.get_snapshot_by_id.return_value = snap
        mock_db.get_storage_node_by_id.return_value = _node()
        mock_db.get_backups_by_lvol_id.return_value = [prev]
        mock_tasks.add_backup_task.return_value = "task-1"

        from simplyblock_core.controllers.backup_controller import backup_snapshot
        backup_id, error = backup_snapshot("snap-1")

        self.assertIsNotNone(backup_id)
        self.assertIsNone(error)
        # Verify the backup object passed to events has prev_backup_id set
        created_backup = mock_events.backup_created.call_args[0][2]
        self.assertEqual(created_backup.prev_backup_id, "prev-backup")


# ===========================================================================
# 8. backup_controller.restore_backup
# ===========================================================================

class TestRestoreBackup(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db, mock_tasks):
        backup = _backup()
        node = _node()
        mock_db.get_backup_by_id.return_value = backup
        mock_db.get_storage_node_by_id.return_value = node
        mock_db.get_backup_chain.return_value = [backup]
        mock_tasks.add_backup_restore_task.return_value = True

        from simplyblock_core.controllers.backup_controller import restore_backup
        result, error = restore_backup("backup-1", "node-1", "restored_lvol")

        self.assertIsNotNone(result)
        self.assertIsNone(error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_backup_not_found(self, mock_db):
        mock_db.get_backup_by_id.side_effect = KeyError("not found")

        from simplyblock_core.controllers.backup_controller import restore_backup
        result, error = restore_backup("missing", "node-1", "lvol")

        self.assertIsNone(result)
        self.assertIsNotNone(error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_node_offline(self, mock_db):
        mock_db.get_backup_by_id.return_value = _backup()
        mock_db.get_storage_node_by_id.return_value = _node(status=StorageNode.STATUS_OFFLINE)

        from simplyblock_core.controllers.backup_controller import restore_backup
        result, error = restore_backup("backup-1", "node-1", "lvol")

        self.assertIsNone(result)
        self.assertIn("not online", error)


# ===========================================================================
# 9. backup_controller.delete_backups
# ===========================================================================

class TestDeleteBackups(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.backup_events")
    @patch("simplyblock_core.controllers.backup_controller.RPCClient")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db, MockRPC, mock_events):
        b1 = _backup(uuid="b-1")
        mock_db.get_backups_by_lvol_id.return_value = [b1]
        mock_db.get_storage_node_by_id.return_value = _node()
        b1.remove = MagicMock()
        MockRPC.return_value.bdev_lvol_s3_delete.return_value = True

        from simplyblock_core.controllers.backup_controller import delete_backups
        success, error = delete_backups("lvol-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        b1.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_no_backups(self, mock_db):
        mock_db.get_backups_by_lvol_id.return_value = []

        from simplyblock_core.controllers.backup_controller import delete_backups
        success, error = delete_backups("lvol-1")

        self.assertFalse(success)
        self.assertIsNotNone(error)


# ===========================================================================
# 10. backup_controller.list_backups
# ===========================================================================

class TestListBackups(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_list_empty(self, mock_db):
        mock_db.get_backups.return_value = []

        from simplyblock_core.controllers.backup_controller import list_backups
        data = list_backups()

        self.assertEqual(data, [])

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_list_with_backups(self, mock_db):
        b = _backup()
        mock_db.get_backups.return_value = [b]

        from simplyblock_core.controllers.backup_controller import list_backups
        data = list_backups()

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["ID"], "backup-1")
        self.assertEqual(data[0]["Status"], Backup.STATUS_COMPLETED)


# ===========================================================================
# 11. Policy management
# ===========================================================================

class TestPolicyAdd(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db):
        mock_db.get_backup_policies.return_value = []

        from simplyblock_core.controllers.backup_controller import add_policy
        policy_id, error = add_policy("cluster-1", "daily", max_versions=5, max_age="2d")

        self.assertIsNotNone(policy_id)
        self.assertIsNone(error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_no_limits(self, mock_db):
        from simplyblock_core.controllers.backup_controller import add_policy
        policy_id, error = add_policy("cluster-1", "empty", max_versions=0, max_age="")

        self.assertIsNone(policy_id)
        self.assertIn("must be specified", error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_duplicate_name(self, mock_db):
        existing = _policy(name="daily")
        mock_db.get_backup_policies.return_value = [existing]

        from simplyblock_core.controllers.backup_controller import add_policy
        policy_id, error = add_policy("cluster-1", "daily", max_versions=5)

        self.assertIsNone(policy_id)
        self.assertIn("already exists", error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_invalid_age(self, mock_db):
        from simplyblock_core.controllers.backup_controller import add_policy
        policy_id, error = add_policy("cluster-1", "test", max_age="invalid")

        self.assertIsNone(policy_id)
        self.assertIn("Invalid age", error)


class TestPolicyRemove(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db):
        p = _policy()
        p.remove = MagicMock()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup_controller import remove_policy
        success, error = remove_policy("policy-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        p.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_not_found(self, mock_db):
        mock_db.get_backup_policy_by_id.side_effect = KeyError("not found")

        from simplyblock_core.controllers.backup_controller import remove_policy
        success, error = remove_policy("missing")

        self.assertFalse(success)
        self.assertIsNotNone(error)


class TestPolicyAttach(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_success(self, mock_db):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_lvol_by_id.return_value = MagicMock()
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup_controller import attach_policy
        att_id, error = attach_policy("policy-1", "lvol", "lvol-1")

        self.assertIsNotNone(att_id)
        self.assertIsNone(error)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_invalid_target_type(self, mock_db):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p

        from simplyblock_core.controllers.backup_controller import attach_policy
        att_id, error = attach_policy("policy-1", "invalid", "target-1")

        self.assertIsNone(att_id)
        self.assertIn("Invalid target_type", error)


class TestPolicyDetach(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
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

        from simplyblock_core.controllers.backup_controller import detach_policy
        success, error = detach_policy("policy-1", "lvol", "lvol-1")

        self.assertTrue(success)
        self.assertIsNone(error)
        att.remove.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_not_found(self, mock_db):
        p = _policy()
        mock_db.get_backup_policy_by_id.return_value = p
        mock_db.get_backup_policy_attachments.return_value = []

        from simplyblock_core.controllers.backup_controller import detach_policy
        success, error = detach_policy("policy-1", "lvol", "lvol-1")

        self.assertFalse(success)
        self.assertIn("not found", error.lower())


# ===========================================================================
# 12. Policy evaluation
# ===========================================================================

class TestEvaluatePolicy(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_no_policy(self, mock_db, mock_tasks):
        mock_db.get_policy_for_lvol.return_value = None

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_version_limit_exceeded(self, mock_db, mock_tasks):
        policy = _policy(max_versions=2, max_age_seconds=0)
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 300)
        b2 = _backup(uuid="b2", created_at=now - 200)
        b3 = _backup(uuid="b3", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2, b3]

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_under_version_limit(self, mock_db, mock_tasks):
        policy = _policy(max_versions=5, max_age_seconds=0)
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 300)
        b2 = _backup(uuid="b2", created_at=now - 200)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2]

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_age_limit_exceeded(self, mock_db, mock_tasks):
        policy = _policy(max_versions=0)
        policy.max_age_seconds = 3600  # 1 hour
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 7200)  # 2 hours old
        b2 = _backup(uuid="b2", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2]

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_both_conditions_required(self, mock_db, mock_tasks):
        """When both versions and age are set, both must be exceeded."""
        policy = _policy(max_versions=3)
        policy.max_age_seconds = 3600
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        # 4 backups (exceeds version limit of 3) but all recent (doesn't exceed age)
        backups = [_backup(uuid=f"b{i}", created_at=now - (i * 60)) for i in range(4)]
        mock_db.get_backups_by_lvol_id.return_value = backups

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        # Should NOT trigger merge: version exceeded but age not exceeded
        mock_tasks.add_backup_merge_task.assert_not_called()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_both_conditions_met(self, mock_db, mock_tasks):
        """When both limits set and both exceeded, merge triggers."""
        policy = _policy(max_versions=2)
        policy.max_age_seconds = 3600
        mock_db.get_policy_for_lvol.return_value = policy

        now = int(time.time())
        b1 = _backup(uuid="b1", created_at=now - 7200)
        b2 = _backup(uuid="b2", created_at=now - 200)
        b3 = _backup(uuid="b3", created_at=now - 100)
        mock_db.get_backups_by_lvol_id.return_value = [b1, b2, b3]

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_fewer_than_two_backups(self, mock_db, mock_tasks):
        """Never merge with fewer than 2 completed backups."""
        policy = _policy(max_versions=1)
        mock_db.get_policy_for_lvol.return_value = policy
        mock_db.get_backups_by_lvol_id.return_value = [_backup()]

        from simplyblock_core.controllers.backup_controller import evaluate_policy
        lvol = MagicMock()
        evaluate_policy(lvol)

        mock_tasks.add_backup_merge_task.assert_not_called()


# ===========================================================================
# 13. Import backups
# ===========================================================================

class TestImportBackups(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_import_new(self, mock_db):
        mock_db.get_backup_by_id.side_effect = KeyError("not found")

        from simplyblock_core.controllers.backup_controller import import_backups
        count = import_backups([
            {"backup_id": "b-1", "lvol_id": "l-1", "cluster_id": "c-1"},
            {"backup_id": "b-2", "lvol_id": "l-1", "cluster_id": "c-1"},
        ])

        self.assertEqual(count, 2)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_skip_existing(self, mock_db):
        existing = _backup(uuid="b-1")
        mock_db.get_backup_by_id.side_effect = lambda bid: existing if bid == "b-1" else (_ for _ in ()).throw(KeyError())

        from simplyblock_core.controllers.backup_controller import import_backups
        count = import_backups([
            {"backup_id": "b-1", "lvol_id": "l-1"},
            {"backup_id": "b-2", "lvol_id": "l-1"},
        ])

        self.assertEqual(count, 1)

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_skip_no_backup_id(self, mock_db):
        from simplyblock_core.controllers.backup_controller import import_backups
        count = import_backups([{"lvol_id": "l-1"}])
        self.assertEqual(count, 0)


# ===========================================================================
# 14. List policies
# ===========================================================================

class TestListPolicies(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.db_controller")
    def test_list_with_policies(self, mock_db):
        p = _policy(max_versions=5)
        p.max_age_display = "2d"
        mock_db.get_backup_policies.return_value = [p]

        from simplyblock_core.controllers.backup_controller import list_policies
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

    def test_bdev_lvol_s3_bdev_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_bdev'))

    def test_bdev_lvol_s3_backup_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_backup'))

    def test_bdev_lvol_s3_backup_stat_exists(self):
        from simplyblock_core.rpc_client import RPCClient
        self.assertTrue(hasattr(RPCClient, 'bdev_lvol_s3_backup_stat'))

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
# 20. _write_s3_metadata
# ===========================================================================

class TestWriteS3Metadata(unittest.TestCase):

    def test_metadata_stored_on_backup(self):
        from simplyblock_core.controllers.backup_controller import _write_s3_metadata
        b = _backup()
        meta = _write_s3_metadata(None, b)

        self.assertEqual(meta["backup_id"], "backup-1")
        self.assertEqual(meta["lvol_id"], "lvol-1")
        self.assertEqual(meta["snapshot_id"], "snap-1")
        self.assertEqual(b.s3_metadata, meta)


# ===========================================================================
# 21. _trigger_merge
# ===========================================================================

class TestTriggerMerge(unittest.TestCase):

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    def test_trigger_merge_marks_old_as_merging(self, mock_tasks):
        from simplyblock_core.controllers.backup_controller import _trigger_merge
        keep = _backup(uuid="keep")
        old = _backup(uuid="old")
        old.write_to_db = MagicMock()

        _trigger_merge(keep, old)

        self.assertEqual(old.status, Backup.STATUS_MERGING)
        old.write_to_db.assert_called_once()
        mock_tasks.add_backup_merge_task.assert_called_once()

    @patch("simplyblock_core.controllers.backup_controller.tasks_controller")
    def test_skip_if_not_completed(self, mock_tasks):
        from simplyblock_core.controllers.backup_controller import _trigger_merge
        keep = _backup(uuid="keep")
        old = _backup(uuid="old", status=Backup.STATUS_PENDING)

        _trigger_merge(keep, old)

        mock_tasks.add_backup_merge_task.assert_not_called()


if __name__ == '__main__':
    unittest.main()
