"""Restoring from a bucket that is not the cluster's own, against real FoundationDB.

This is the disaster-recovery path: the backup's own recorded location says
where its objects are, and the node gets a second S3 device pointed at that
bucket for the duration of the restore.

The device's whole lifecycle belongs to the task runner, not to the restore
request -- the runner is the only component that knows which node the volume
landed on, and the only one that can put the device back after a node restart
mid-restore.
"""
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import device as backup_device
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig, S3Credentials
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_backup


CLUSTER_ID = "cluster-1"
OWN_BUCKET = "simplyblock-backup-cluster-1"
FOREIGN_BUCKET = "someone-elses-bucket"
OWN_ENDPOINT = "http://minio.example.com:9000"
OTHER_ENDPOINT = "https://s3.eu-central-1.amazonaws.com"


def _config(bucket=OWN_BUCKET, **overrides):
    return BackupConfig.model_validate({
        "bucket_name": bucket, "region": "eu-central-1",
        "endpoint": OWN_ENDPOINT, **overrides})


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def cluster(db):
    c = Cluster()
    c.uuid = CLUSTER_ID
    c.backup_config = _config(
        credentials={"access_key_id": "own", "secret_access_key": "own"}
    ).model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


@pytest.fixture
def node(db):
    n = StorageNode()
    n.uuid = "node-1"
    n.cluster_id = CLUSTER_ID
    n.status = StorageNode.STATUS_ONLINE
    n.lvstore = "lvs_test"
    n.mgmt_ip = "10.0.0.1"
    n.rpc_port = 5260
    n.app_thread_mask = "0x8"
    n.cpu = 8
    n.write_to_db(db.kv_store)
    return n


def _backup(db, bucket=OWN_BUCKET, uuid="b-1", **location_overrides):
    b = Backup()
    b.uuid = uuid
    b.s3_id = 1
    b.cluster_id = CLUSTER_ID
    b.lvol_id = "lvol-1"
    b.size = 4096
    b.status = Backup.STATUS_COMPLETED
    b.location = _config(bucket, **location_overrides).location().model_dump(mode="json")
    b.write_to_db(db.kv_store)
    return b


class TestSourceSelection:

    def test_own_bucket_needs_no_new_device(self, db, cluster, node):
        assert backup_controller.foreign_bucket_config(
            _backup(db), cluster, None) is None

    def test_foreign_bucket_yields_its_own_config(self, db, cluster, node):
        config = backup_controller.foreign_bucket_config(
            _backup(db, FOREIGN_BUCKET), cluster,
            S3Credentials(access_key_id="theirs", secret_access_key="theirs"))

        assert config is not None
        assert config.bucket_name == FOREIGN_BUCKET
        assert config.credentials.access_key_id.get_secret_value() == "theirs"

    def test_foreign_bucket_at_the_same_endpoint_inherits_own_credentials(self, db, cluster, node):
        """The ordinary cross-cluster restore: two clusters, one store.

        The keys that open this cluster's bucket open the other one's, so the
        configuration it already has is the answer -- being asked to repeat those
        keys on the command line is what a restore has no business demanding.
        """
        config = backup_controller.foreign_bucket_config(
            _backup(db, FOREIGN_BUCKET), cluster, None)

        assert config.bucket_name == FOREIGN_BUCKET
        assert config.credentials.access_key_id.get_secret_value() == "own"

    def test_bucket_at_another_endpoint_without_credentials_is_refused(self, db, cluster, node):
        """Keys authenticate against a store, so another store's bucket needs its own."""
        with pytest.raises(PreconditionError, match="do not authenticate against"):
            backup_controller.foreign_bucket_config(
                _backup(db, FOREIGN_BUCKET, endpoint=OTHER_ENDPOINT), cluster, None)

    def test_instance_role_cluster_may_reach_a_foreign_bucket(self, db, node):
        """With no static credentials anywhere, the node's role is the only answer."""
        c = Cluster()
        c.uuid = CLUSTER_ID
        c.backup_config = _config().model_dump(exclude_none=True)  # no credentials
        c.write_to_db(db.kv_store)

        config = backup_controller.foreign_bucket_config(
            _backup(db, FOREIGN_BUCKET), c, None)

        assert config.bucket_name == FOREIGN_BUCKET
        assert config.credentials is None

    def test_instance_role_cluster_may_reach_another_endpoint(self, db, node):
        """Not refused: a role can be granted access across stores, and nothing
        here can tell whether this one was."""
        c = Cluster()
        c.uuid = CLUSTER_ID
        c.backup_config = _config().model_dump(exclude_none=True)  # no credentials
        c.write_to_db(db.kv_store)

        config = backup_controller.foreign_bucket_config(
            _backup(db, FOREIGN_BUCKET, endpoint=OTHER_ENDPOINT), c, None)

        assert config.credentials is None

    def test_explicit_credentials_override_the_own_bucket_shortcut(self, db, cluster, node):
        """Restoring the cluster's own bucket with other credentials is legitimate."""
        config = backup_controller.foreign_bucket_config(
            _backup(db), cluster,
            S3Credentials(access_key_id="other", secret_access_key="other"))

        assert config is not None
        assert config.bucket_name == OWN_BUCKET

    def test_device_name_is_derived_from_the_backup(self, db):
        """Stable across retries, so an attempt cannot leak a device per try."""
        assert (backup_device.restore_s3_bdev_name("b-1234567890")
                == backup_device.restore_s3_bdev_name("b-1234567890"))
        assert backup_device.restore_s3_bdev_name(
            "b-1234567890") != backup_device.restore_s3_bdev_name("c-1234567890")


def _restore_task(db, s3_config=None, **params):
    task = JobSchedule()
    task.uuid = "task-1"
    task.cluster_id = CLUSTER_ID
    task.node_id = "node-1"
    task.function_name = JobSchedule.FN_BACKUP_RESTORE
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params = {
        "backup_id": "b-1",
        "lvol_name": "lvs_test/LVOL_1",
        "lvol_id": "",
        "chain_ids": [1],
        "s3_config": s3_config,
        **params,
    }
    task.write_to_db(db.kv_store)
    return task


class TestRunnerOwnsTheDevice:

    def test_own_bucket_creates_nothing(self, db, cluster, node):
        task = _restore_task(db)

        with patch.object(backup_device, "create_restore_s3_bdev") as create:
            tasks_runner_backup._ensure_restore_s3_bdev(task, node)

        create.assert_not_called()

    def test_foreign_bucket_device_is_created_by_the_runner(self, db, cluster, node):
        task = _restore_task(db, s3_config=_config(FOREIGN_BUCKET).model_dump(exclude_none=True))

        with patch.object(backup_device, "create_restore_s3_bdev") as create:
            tasks_runner_backup._ensure_restore_s3_bdev(task, node)

        _, kwargs = create.call_args
        args = create.call_args[0]
        assert args[0] is node
        assert args[1].bucket_name == FOREIGN_BUCKET
        assert args[2] == backup_device.restore_s3_bdev_name("b-1")

    def test_creation_is_idempotent_across_retries(self, db, cluster, node):
        """A node restart mid-restore takes the device with it; the runner rebuilds it."""
        task = _restore_task(db, s3_config=_config(FOREIGN_BUCKET).model_dump(exclude_none=True))

        with patch.object(backup_device, "create_restore_s3_bdev") as create:
            tasks_runner_backup._ensure_restore_s3_bdev(task, node)
            tasks_runner_backup._ensure_restore_s3_bdev(task, node)

        assert create.call_count == 2
        assert {c[0][2] for c in create.call_args_list} == {
            backup_device.restore_s3_bdev_name("b-1")}

    def test_release_deletes_the_device(self, db, cluster, node):
        task = _restore_task(db, s3_config=_config(FOREIGN_BUCKET).model_dump(exclude_none=True))

        with patch.object(backup_device, "delete_restore_s3_bdev") as delete:
            tasks_runner_backup._release_restore_s3_bdev(task, node)

        delete.assert_called_once_with(
            node, backup_device.restore_s3_bdev_name("b-1"))

    def test_release_scrubs_the_credentials(self, db, cluster, node):
        """A task record outlives the restore by weeks; foreign keys must not."""
        task = _restore_task(db, s3_config=_config(
            FOREIGN_BUCKET,
            credentials={"access_key_id": "theirs", "secret_access_key": "theirs"},
        ).model_dump(exclude_none=True))

        with patch.object(backup_device, "delete_restore_s3_bdev"):
            tasks_runner_backup._release_restore_s3_bdev(task, node)

        assert task.function_params["s3_config"] is None

    def test_release_is_a_noop_for_the_own_bucket(self, db, cluster, node):
        """The node's own device is shared; a restore must never delete it."""
        task = _restore_task(db)

        with patch.object(backup_device, "delete_restore_s3_bdev") as delete:
            tasks_runner_backup._release_restore_s3_bdev(task, node)

        delete.assert_not_called()

    def test_release_survives_a_missing_node(self, db, cluster):
        """Cleanup runs on terminal paths, including ones reached because the node is gone."""
        task = _restore_task(db, s3_config=_config(FOREIGN_BUCKET).model_dump(exclude_none=True))

        tasks_runner_backup._release_restore_s3_bdev(task, None)

        assert task.function_params["s3_config"] is None

    def test_delete_failure_does_not_raise(self, db, cluster, node):
        """A cleanup failure must not turn a completed restore into a failed one."""
        node.rpc_client = MagicMock()
        node.rpc_client.return_value.bdev_s3_delete.side_effect = RuntimeError("gone")

        backup_device.delete_restore_s3_bdev(node, "s3_restore_b-1")

    def test_recovery_names_the_device_it_reads_from(self, db, cluster, node):
        task = _restore_task(db, s3_config=_config(FOREIGN_BUCKET).model_dump(exclude_none=True))

        assert tasks_runner_backup._restore_s3_bdev(task, node) == \
            backup_device.restore_s3_bdev_name("b-1")

    def test_recovery_falls_back_to_the_nodes_own_device(self, db, cluster, node):
        task = _restore_task(db)

        assert tasks_runner_backup._restore_s3_bdev(task, node) == \
            backup_device.primary_s3_bdev_name(node)
