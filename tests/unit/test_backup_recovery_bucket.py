# coding=utf-8
"""
test_backup_recovery_bucket.py — unit tests for cross-cluster restore bucket
resolution (SFAM-2797: a backup exported from C1 and restored on C2 was read
from C2's own bucket, so every S3 GET returned 404 and the restore task
crash-looped until it exhausted its retries).
"""

import unittest
from unittest.mock import MagicMock, call, patch

from simplyblock_core.controllers.backup_controller import (
    backup_bucket_name, register_recovery_buckets,
)
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient

C1 = "3359a4e8-e17b-4ef8-a5fb-1924e0745287"
C2 = "27d33e2f-ba6e-44ad-bde4-06bdcc2197d0"


def _cluster(cluster_id=C2, backup_config=None):
    c = MagicMock(spec=Cluster)
    c.uuid = cluster_id
    c.backup_config = backup_config
    c.get_id = MagicMock(return_value=cluster_id)
    return c


def _node(node_id="node-1", lvstore="LVS_21"):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.status = StorageNode.STATUS_ONLINE
    n.lvstore = lvstore
    n.get_id = MagicMock(return_value=node_id)
    return n


def _backup(s3_id=1, source_cluster_id="", cluster_id=C2):
    b = MagicMock(spec=Backup)
    b.uuid = f"backup-{s3_id}"
    b.s3_id = s3_id
    b.cluster_id = cluster_id
    b.source_cluster_id = source_cluster_id
    b.status = Backup.STATUS_COMPLETED
    return b


class TestBackupBucketName(unittest.TestCase):

    def test_own_bucket_defaults_to_cluster_id(self):
        self.assertEqual(backup_bucket_name({}, C2), f"simplyblock-backup-{C2}")

    def test_own_bucket_honours_config_override(self):
        self.assertEqual(backup_bucket_name({"bucket_name": "custom"}, C2), "custom")

    def test_own_bucket_by_explicit_source(self):
        self.assertEqual(backup_bucket_name({"bucket_name": "custom"}, C2, C2), "custom")

    def test_foreign_bucket_ignores_local_override(self):
        self.assertEqual(
            backup_bucket_name({"bucket_name": "custom"}, C2, C1),
            f"simplyblock-backup-{C1}")

    def test_missing_config_is_tolerated(self):
        self.assertEqual(backup_bucket_name(None, C2), f"simplyblock-backup-{C2}")


class TestRegisterRecoveryBuckets(unittest.TestCase):

    def test_imported_backups_are_registered(self):
        rpc_client = MagicMock(spec=RPCClient)
        register_recovery_buckets(
            rpc_client, _node(), _cluster(),
            [_backup(s3_id=1, source_cluster_id=C1),
             _backup(s3_id=2, source_cluster_id=C1)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once_with(
            "s3_LVS_21", f"simplyblock-backup-{C1}", [1, 2])

    def test_local_backups_are_not_registered(self):
        rpc_client = MagicMock(spec=RPCClient)
        register_recovery_buckets(
            rpc_client, _node(), _cluster(),
            [_backup(s3_id=1), _backup(s3_id=2, source_cluster_id=C2)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_not_called()

    def test_local_backups_under_a_renamed_bucket_are_not_registered(self):
        rpc_client = MagicMock(spec=RPCClient)
        register_recovery_buckets(
            rpc_client, _node(), _cluster(backup_config={"bucket_name": "custom"}),
            [_backup(s3_id=1, source_cluster_id=C2)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_not_called()

    def test_mixed_chain_registers_only_foreign_ids(self):
        rpc_client = MagicMock(spec=RPCClient)
        register_recovery_buckets(
            rpc_client, _node(), _cluster(),
            [_backup(s3_id=1, source_cluster_id=C1), _backup(s3_id=2)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once_with(
            "s3_LVS_21", f"simplyblock-backup-{C1}", [1])

    def test_one_call_per_source_cluster(self):
        other = "11111111-2222-3333-4444-555555555555"
        rpc_client = MagicMock(spec=RPCClient)
        register_recovery_buckets(
            rpc_client, _node(), _cluster(),
            [_backup(s3_id=1, source_cluster_id=C1),
             _backup(s3_id=2, source_cluster_id=other),
             _backup(s3_id=3, source_cluster_id=C1)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_has_calls([
            call("s3_LVS_21", f"simplyblock-backup-{C1}", [1, 3]),
            call("s3_LVS_21", f"simplyblock-backup-{other}", [2]),
        ], any_order=True)
        self.assertEqual(rpc_client.bdev_s3_register_recovery_bucket.call_count, 2)


class TestRestoreTaskRegistersBeforeRecovery(unittest.TestCase):
    """The registration is what makes the data plane read from the source
    cluster's bucket, so it has to reach the node before recovery starts and
    again on every re-issue — the mapping lives only in the S3 bdev's memory.
    """

    def _task(self, recovery_started=False):
        t = MagicMock(spec=JobSchedule)
        t.uuid = "task-1"
        t.cluster_id = C2
        t.node_id = "node-1"
        t.retry = 0
        t.status = JobSchedule.STATUS_NEW
        t.function_params = {
            "backup_id": "backup-1",
            "lvol_name": "LVS_21/LVOL_43",
            "lvol_id": "",
            "chain_ids": [1],
            "recovery_started": recovery_started,
        }
        return t

    def _run(self, task, chain):
        from simplyblock_core.services import tasks_runner_backup

        snode = _node()
        rpc_client = MagicMock(spec=RPCClient)
        snode.rpc_client = MagicMock(return_value=rpc_client)

        db = MagicMock()
        db.get_storage_node_by_id.return_value = snode
        db.get_cluster_by_id.return_value = _cluster()
        db.get_backup_chain.return_value = chain

        with patch.object(tasks_runner_backup, "db", db):
            tasks_runner_backup._run_restore(task)

        return rpc_client

    def test_foreign_bucket_registered_before_recovery(self):
        rpc_client = self._run(self._task(), [_backup(s3_id=1, source_cluster_id=C1)])

        self.assertEqual(
            [c[0] for c in rpc_client.method_calls],
            ["bdev_s3_register_recovery_bucket", "bdev_lvol_s3_recovery"])
        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once_with(
            "s3_LVS_21", f"simplyblock-backup-{C1}", [1])

    def test_recovery_reissue_re_registers(self):
        """"No process" resets recovery_started, so the node may have restarted
        and lost the mapping."""
        rpc_client = self._run(self._task(), [_backup(s3_id=1, source_cluster_id=C1)])
        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once()

        rpc_client = self._run(self._task(), [_backup(s3_id=1, source_cluster_id=C1)])
        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once()

    def test_local_restore_issues_no_registration(self):
        rpc_client = self._run(self._task(), [_backup(s3_id=1)])

        rpc_client.bdev_s3_register_recovery_bucket.assert_not_called()
        rpc_client.bdev_lvol_s3_recovery.assert_called_once_with(
            "LVS_21/LVOL_43", [1], cluster_batch=16)

    def test_backups_outside_the_restored_chain_are_ignored(self):
        rpc_client = self._run(
            self._task(),
            [_backup(s3_id=1, source_cluster_id=C1),
             _backup(s3_id=9, source_cluster_id="other-cluster")])

        rpc_client.bdev_s3_register_recovery_bucket.assert_called_once_with(
            "s3_LVS_21", f"simplyblock-backup-{C1}", [1])
