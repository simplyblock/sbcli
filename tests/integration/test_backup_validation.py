"""Preconditions on backup creation, restore and import, against real FoundationDB.

Every rule here has the same shape: it must fire *before* any side effect. A
refused backup must leave no Backup record, no KMS key and no task; a refused
restore must leave no volume. The point is that a backup either is restorable or
was never created -- discovering the problem during a recovery is too late.
"""
from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import validation
from simplyblock_core.controllers.backup.manifest import BackupManifest
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


CLUSTER_ID = "cluster-1"


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-cluster-1",
        "region": "eu-central-1",
        **overrides,
    })


@pytest.fixture
def db():
    return DBController()


@pytest.fixture
def cluster(db):
    c = Cluster()
    c.uuid = CLUSTER_ID
    c.backup_config = _config().model_dump(exclude_none=True)
    c.write_to_db(db.kv_store)
    return c


@pytest.fixture
def pool(db):
    p = Pool()
    p.uuid = "pool-1"
    p.pool_name = "pool-1"  # resolved by name: "pool-1" is not a UUID
    p.cluster_id = CLUSTER_ID
    p.write_to_db(db.kv_store)
    return p


@pytest.fixture
def node(db):
    n = StorageNode()
    n.uuid = "node-1"
    n.cluster_id = CLUSTER_ID
    n.status = StorageNode.STATUS_ONLINE
    n.lvstore = "lvs_test"
    n.mgmt_ip = "10.0.0.1"
    n.rpc_port = 5260
    n.write_to_db(db.kv_store)
    return n


def _snapshot(db, uuid="snap-1", crypto=False):
    volume = LVol()
    volume.uuid = "lvol-1"
    volume.lvol_name = "vol"
    volume.node_id = "node-1"
    volume.lvs_name = "lvs_test"
    volume.pool_uuid = "pool-1"
    volume.size = 4096
    if crypto:
        volume.crypto_bdev = "crypto_lvol-1"
    volume.write_to_db(db.kv_store)

    s = SnapShot()
    s.uuid = uuid
    s.snap_uuid = uuid
    s.snap_name = uuid
    s.snap_bdev = f"lvs_test/{uuid}"
    s.size = 4096
    s.status = SnapShot.STATUS_ONLINE
    s.lvol = volume
    s.write_to_db(db.kv_store)
    return s


def _backup(db, uuid, s3_id, snapshot_id, prev="", location=None, encrypted=False):
    b = Backup()
    b.uuid = uuid
    b.s3_id = s3_id
    b.cluster_id = CLUSTER_ID
    b.lvol_id = "lvol-1"
    b.lvol_name = "vol"
    b.snapshot_id = snapshot_id
    b.prev_backup_id = prev
    b.size = 4096
    b.status = Backup.STATUS_COMPLETED
    b.location = (location or _config().location()).model_dump(exclude_none=True)
    b.encrypted = encrypted
    b.write_to_db(db.kv_store)
    return b


def _assert_no_side_effects(db):
    assert db.get_backups() == [], "a refused backup must leave no record"
    assert db.get_job_tasks(CLUSTER_ID) == [], "a refused backup must enqueue no task"


class TestBackupCreationPreconditions:

    def test_missing_backup_config_is_refused(self, db, cluster, node):
        cluster.backup_config = {}
        cluster.write_to_db(db.kv_store)
        _snapshot(db)

        backup_id, error = backup_controller.backup_snapshot("snap-1")

        assert backup_id is None
        assert "backup configuration" in error
        _assert_no_side_effects(db)

    def test_tiering_layout_bucket_is_refused(self, db, cluster, node):
        """snapshot_backups=False selects {tiering_id}/{lpgi}, which restore cannot read."""
        cluster.backup_config = _config(snapshot_backups=False).model_dump(exclude_none=True)
        cluster.write_to_db(db.kv_store)
        _snapshot(db)

        backup_id, error = backup_controller.backup_snapshot("snap-1")

        assert backup_id is None
        assert "snapshot_backups disabled" in error
        _assert_no_side_effects(db)

    def test_overlong_chain_is_refused(self, db, cluster, node):
        """Longer than the data plane's fixed arrays, where it smashes the stack."""
        snap = _snapshot(db)
        too_long = [snap] * (constants.BACKUP_MAX_CHAIN_LENGTH + 1)

        with patch.object(backup_controller, "_get_snapshot_chain", return_value=too_long):
            backup_id, error = backup_controller.backup_snapshot("snap-1")

        assert backup_id is None
        assert "data plane accepts at most" in error
        _assert_no_side_effects(db)

    def test_chain_in_another_bucket_is_refused(self, db, cluster, node):
        """The cluster's bucket changed since the ancestors were written."""
        snap = _snapshot(db)
        _backup(db, "b-old", 1, snapshot_id="snap-0",
                location=_config(bucket_name="the-old-bucket").location())

        with patch.object(backup_controller, "_get_snapshot_chain",
                          return_value=[_named(snap, "snap-0"), snap]):
            backup_id, error = backup_controller.backup_snapshot("snap-1")

        assert backup_id is None
        assert "cannot span buckets" in error
        assert db.get_backups() == [db.get_backup_by_id("b-old")]

    def test_encrypted_volume_over_a_plain_chain_is_refused(self, db, cluster, node):
        snap = _snapshot(db, crypto=True)
        _backup(db, "b-plain", 1, snapshot_id="snap-0", encrypted=False)

        with patch.object(backup_controller, "_get_snapshot_chain",
                          return_value=[_named(snap, "snap-0"), snap]):
            backup_id, error = backup_controller.backup_snapshot("snap-1")

        assert backup_id is None
        assert "cannot mix encrypted and unencrypted" in error

    def test_refusal_happens_before_the_chain_lock(self, db, cluster, node):
        """Otherwise a refused request would block the next one."""
        cluster.backup_config = {}
        cluster.write_to_db(db.kv_store)
        _snapshot(db)

        backup_controller.backup_snapshot("snap-1")

        assert db.get_backup_chain_lock("snap-1") is None


def _named(snapshot, uuid):
    """A shallow copy of a snapshot under a different id, for chain fixtures."""
    clone = SnapShot()
    clone.from_dict(snapshot.to_dict())
    clone.uuid = uuid
    clone.snap_uuid = uuid
    return clone


class TestRestorePreconditions:

    def test_chain_spanning_buckets_is_refused(self, db, cluster, node, pool):
        _backup(db, "b-1", 1, snapshot_id="snap-1",
                location=_config(bucket_name="elsewhere").location())
        _backup(db, "b-2", 2, snapshot_id="snap-2", prev="b-1")

        with pytest.raises(PreconditionError, match="cannot span buckets"):
            backup_controller.restore_backup("b-2", "restored", "pool-1")

    def test_chain_mixing_encryption_is_refused(self, db, cluster, node, pool):
        _backup(db, "b-1", 1, snapshot_id="snap-1", encrypted=True)
        _backup(db, "b-2", 2, snapshot_id="snap-2", prev="b-1", encrypted=False)

        with pytest.raises(PreconditionError, match="mix encrypted and unencrypted"):
            backup_controller.restore_backup("b-2", "restored", "pool-1")

    def test_overlong_chain_is_refused(self, db, cluster, node, pool):
        previous = ""
        for index in range(constants.BACKUP_MAX_CHAIN_LENGTH + 1):
            previous = _backup(db, f"b-{index}", index + 1,
                               snapshot_id=f"snap-{index}", prev=previous).uuid

        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.restore_backup(previous, "restored", "pool-1")

    def test_no_volume_is_created_when_a_precondition_fails(self, db, cluster, node, pool):
        _backup(db, "b-1", 1, snapshot_id="snap-1", encrypted=True)
        _backup(db, "b-2", 2, snapshot_id="snap-2", prev="b-1", encrypted=False)

        with pytest.raises(PreconditionError):
            backup_controller.restore_backup("b-2", "restored", "pool-1")

        assert db.get_lvols() == []


class TestImportPreconditions:

    def _manifest(self, backup_id, prev=None, s3_id=1,
                  bucket="simplyblock-backup-cluster-1"):
        return BackupManifest.model_validate({
            "schema_version": 1,
            "backup_id": backup_id,
            "s3_id": s3_id,
            "created_at": 100,
            "completed_at": 200,
            "size": 4096,
            "prev_backup_id": prev,
            "location": _config(bucket_name=bucket).location().model_dump(mode="json"),
            "source": {"cluster_id": CLUSTER_ID, "node_id": "node-1"},
            "volume": {"lvol_id": "lvol-1", "lvol_name": "vol",
                       "snapshot_id": f"snap-{backup_id}", "snapshot_name": "s",
                       "size": 4096},
            "dataplane": {},
        })

    def _line(self, length, **overrides):
        """A chain of `length` manifests, oldest first."""
        line, prev = [], None
        for index in range(length):
            line.append(self._manifest(f"b-{index}", prev=prev, s3_id=index + 1,
                                       **overrides))
            prev = line[-1].backup_id
        return line

    def test_incomplete_chain_is_refused(self, db, cluster):
        """A delta whose ancestors are missing looks restorable until it is tried."""
        with pytest.raises(PreconditionError, match="neither in this import nor already known"):
            backup_controller.import_backups(
                [self._manifest("b-2", prev="b-1")], cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_chain_satisfied_within_the_batch_is_accepted(self, db, cluster):
        count = backup_controller.import_backups(self._line(2), cluster_id=CLUSTER_ID)

        assert count == 2

    def test_chain_satisfied_by_existing_records_is_accepted(self, db, cluster):
        _backup(db, "b-1", 1, snapshot_id="snap-1")

        count = backup_controller.import_backups(
            [self._manifest("b-2", prev="b-1")], cluster_id=CLUSTER_ID)

        assert count == 1

    def test_chain_spanning_buckets_is_refused(self, db, cluster):
        with pytest.raises(PreconditionError, match="different bucket or encoding"):
            backup_controller.import_backups(
                [self._manifest("b-1", bucket="elsewhere"),
                 self._manifest("b-2", prev="b-1")],
                cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_overlong_chain_is_refused(self, db, cluster):
        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.import_backups(
                self._line(constants.BACKUP_MAX_CHAIN_LENGTH + 1),
                cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_a_chain_lengthened_past_the_limit_by_existing_records_is_refused(
            self, db, cluster):
        """The ancestry already in the database counts towards the limit."""
        previous = ""
        for index in range(constants.BACKUP_MAX_CHAIN_LENGTH):
            previous = _backup(db, f"old-{index}", index + 1,
                               snapshot_id=f"snap-old-{index}", prev=previous).uuid

        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.import_backups(
                [self._manifest("b-new", prev=previous, s3_id=999)],
                cluster_id=CLUSTER_ID)

    def test_a_cyclic_chain_is_refused_rather_than_looping(self, db, cluster):
        with pytest.raises(PreconditionError, match="cyclic"):
            backup_controller.import_backups(
                [self._manifest("b-1", prev="b-2"), self._manifest("b-2", prev="b-1")],
                cluster_id=CLUSTER_ID)


class TestPredicates:
    """The rules answer a yes/no question as well as blocking an operation."""

    def test_chain_fits_at_the_limit(self):
        assert validation.chain_fits(constants.BACKUP_MAX_CHAIN_LENGTH)
        assert not validation.chain_fits(constants.BACKUP_MAX_CHAIN_LENGTH + 1)

    def test_a_tiering_bucket_holds_no_backups(self):
        assert validation.location_holds_backups(_config().location())
        assert not validation.location_holds_backups(
            _config(snapshot_backups=False).location())

    def test_an_empty_chain_is_coherent(self):
        assert validation.chain_is_coherent([], _config().location())

    def test_coherence_covers_a_backup_that_does_not_exist_yet(self, db, cluster):
        chain = [_backup(db, "b-1", 1, snapshot_id="snap-1", encrypted=False)]

        assert validation.chain_is_coherent(chain, _config().location())
        assert validation.chain_is_coherent(
            chain, _config().location(), encrypted=False)
        assert not validation.chain_is_coherent(
            chain, _config().location(), encrypted=True)
