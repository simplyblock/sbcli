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
from simplyblock_core.controllers.backup import policy as backup_policy
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


# Every id a manifest carries is a UUID, so the objects these tests build are
# given real ones rather than readable stand-ins.
CLUSTER_ID = "c1000000-0000-4000-8000-000000000001"
POOL_ID = "b0010000-0000-4000-8000-000000000001"
NODE_ID = "d0de0000-0000-4000-8000-000000000001"
LVOL_ID = "10101000-0000-4000-8000-000000000001"


def _backup_id(index: int) -> str:
    return f"0ac00000-0000-4000-8000-{index:012d}"


def _snapshot_id(index: int) -> str:
    return f"50a50000-0000-4000-8000-{index:012d}"


def _config(**overrides):
    return BackupConfig.model_validate({
        "bucket_name": "simplyblock-backup-primary",
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
    p.uuid = POOL_ID
    p.pool_name = "pool-1"  # resolved by name: "pool-1" is not a UUID
    p.cluster_id = CLUSTER_ID
    p.write_to_db(db.kv_store)
    return p


@pytest.fixture
def node(db):
    n = StorageNode()
    n.uuid = NODE_ID
    n.cluster_id = CLUSTER_ID
    n.status = StorageNode.STATUS_ONLINE
    n.lvstore = "lvs_test"
    n.mgmt_ip = "10.0.0.1"
    n.rpc_port = 5260
    n.write_to_db(db.kv_store)
    return n


def _snapshot(db, index=1, crypto=False):
    volume = LVol()
    volume.uuid = LVOL_ID
    volume.lvol_name = "vol"
    volume.node_id = NODE_ID
    volume.lvs_name = "lvs_test"
    volume.pool_uuid = POOL_ID
    volume.size = 4096
    if crypto:
        volume.crypto_bdev = f"crypto_{LVOL_ID}"
    volume.write_to_db(db.kv_store)

    s = SnapShot()
    s.uuid = _snapshot_id(index)
    s.snap_uuid = _snapshot_id(index)
    s.snap_name = f"snap-{index}"
    s.snap_bdev = f"lvs_test/snap-{index}"
    s.size = 4096
    s.status = SnapShot.STATUS_ONLINE
    s.lvol = volume
    s.write_to_db(db.kv_store)
    return s


def _backup(db, index, snapshot_index=None, prev=None, location=None, encrypted=False):
    """A completed backup, addressed by index rather than by uuid.

    The index feeds the uuid, the s3_id and the snapshot id together, so a chain
    reads as `_backup(db, 2, prev=1)` instead of three parallel UUID literals.
    """
    b = Backup()
    b.uuid = _backup_id(index)
    b.s3_id = index
    b.cluster_id = CLUSTER_ID
    b.lvol_id = LVOL_ID
    b.lvol_name = "vol"
    b.snapshot_id = _snapshot_id(index if snapshot_index is None else snapshot_index)
    b.prev_backup_id = _backup_id(prev) if prev is not None else ""
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

        backup_id, error = backup_controller.backup_snapshot(_snapshot_id(1))

        assert backup_id is None
        assert "backup configuration" in error
        _assert_no_side_effects(db)

    def test_tiering_layout_bucket_is_refused(self, db, cluster, node):
        """snapshot_backups=False selects {tiering_id}/{lpgi}, which restore cannot read."""
        cluster.backup_config = _config(snapshot_backups=False).model_dump(exclude_none=True)
        cluster.write_to_db(db.kv_store)
        _snapshot(db)

        backup_id, error = backup_controller.backup_snapshot(_snapshot_id(1))

        assert backup_id is None
        assert "snapshot_backups disabled" in error
        _assert_no_side_effects(db)

    def test_overlong_chain_is_refused(self, db, cluster, node):
        """Longer than the data plane's fixed arrays, where it smashes the stack."""
        snap = _snapshot(db)
        too_long = [snap] * (constants.BACKUP_MAX_CHAIN_LENGTH + 1)

        with patch.object(backup_controller, "_get_snapshot_chain", return_value=too_long):
            backup_id, error = backup_controller.backup_snapshot(_snapshot_id(1))

        assert backup_id is None
        assert "data plane accepts at most" in error
        _assert_no_side_effects(db)

    def test_chain_in_another_bucket_is_refused(self, db, cluster, node):
        """The cluster's bucket changed since the ancestors were written."""
        snap = _snapshot(db)
        _backup(db, 1, snapshot_index=0,
                location=_config(bucket_name="the-old-bucket").location())

        with patch.object(backup_controller, "_get_snapshot_chain",
                          return_value=[_named(snap, _snapshot_id(0)), snap]):
            backup_id, error = backup_controller.backup_snapshot(_snapshot_id(1))

        assert backup_id is None
        assert "cannot span buckets" in error
        assert db.get_backups() == [db.get_backup_by_id(_backup_id(1))]

    def test_encrypted_volume_over_a_plain_chain_is_refused(self, db, cluster, node):
        snap = _snapshot(db, crypto=True)
        _backup(db, 1, snapshot_index=0, encrypted=False)

        with patch.object(backup_controller, "_get_snapshot_chain",
                          return_value=[_named(snap, _snapshot_id(0)), snap]):
            backup_id, error = backup_controller.backup_snapshot(_snapshot_id(1))

        assert backup_id is None
        assert "cannot mix encrypted and unencrypted" in error

    def test_refusal_happens_before_the_chain_lock(self, db, cluster, node):
        """Otherwise a refused request would block the next one."""
        cluster.backup_config = {}
        cluster.write_to_db(db.kv_store)
        _snapshot(db)

        backup_controller.backup_snapshot(_snapshot_id(1))

        assert db.get_backup_chain_lock(_snapshot_id(1)) is None


def _named(snapshot, uuid):
    """A shallow copy of a snapshot under a different id, for chain fixtures."""
    clone = SnapShot()
    clone.from_dict(snapshot.to_dict())
    clone.uuid = uuid
    clone.snap_uuid = uuid
    return clone


class TestRestorePreconditions:

    def test_chain_spanning_buckets_is_refused(self, db, cluster, node, pool):
        _backup(db, 1, location=_config(bucket_name="elsewhere").location())
        _backup(db, 2, prev=1)

        with pytest.raises(PreconditionError, match="cannot span buckets"):
            backup_controller.restore_backup(_backup_id(2), "restored", "pool-1")

    def test_chain_mixing_encryption_is_refused(self, db, cluster, node, pool):
        _backup(db, 1, encrypted=True)
        _backup(db, 2, prev=1, encrypted=False)

        with pytest.raises(PreconditionError, match="mix encrypted and unencrypted"):
            backup_controller.restore_backup(_backup_id(2), "restored", "pool-1")

    def test_overlong_chain_is_refused(self, db, cluster, node, pool):
        previous = None
        for index in range(1, constants.BACKUP_MAX_CHAIN_LENGTH + 2):
            _backup(db, index, prev=previous)
            previous = index

        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.restore_backup(_backup_id(previous), "restored", "pool-1")

    def test_no_volume_is_created_when_a_precondition_fails(self, db, cluster, node, pool):
        _backup(db, 1, encrypted=True)
        _backup(db, 2, prev=1, encrypted=False)

        with pytest.raises(PreconditionError):
            backup_controller.restore_backup(_backup_id(2), "restored", "pool-1")

        assert db.get_lvols() == []


class TestImportPreconditions:

    def _manifest(self, index, prev=None, s3_id=None, with_compression=False):
        return BackupManifest.model_validate({
            "schema_version": 1,
            "backup_id": _backup_id(index),
            "s3_id": index if s3_id is None else s3_id,
            "created_at": 100,
            "completed_at": 200,
            "size": 4096,
            "prev_backup_id": _backup_id(prev) if prev is not None else None,
            "source": {"cluster_id": CLUSTER_ID, "node_id": NODE_ID},
            "volume": {"lvol_id": LVOL_ID, "lvol_name": "vol",
                       "snapshot_id": _snapshot_id(index), "snapshot_name": "s",
                       "size": 4096},
            "dataplane": {"with_compression": with_compression},
        })

    def _line(self, length, **overrides):
        """A chain of `length` manifests, oldest first."""
        return [
            self._manifest(index, prev=index - 1 if index else None,
                           s3_id=index + 1, **overrides)
            for index in range(length)
        ]

    def test_incomplete_chain_is_refused(self, db, cluster):
        """A delta whose ancestors are missing looks restorable until it is tried."""
        with pytest.raises(PreconditionError, match="neither in this import nor already known"):
            backup_controller.import_backups(
                [self._manifest(2, prev=1)], _config().location(),
                cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_chain_satisfied_within_the_batch_is_accepted(self, db, cluster):
        count = backup_controller.import_backups(
            self._line(2), _config().location(), cluster_id=CLUSTER_ID)

        assert count == 2

    def test_chain_satisfied_by_existing_records_is_accepted(self, db, cluster):
        _backup(db, 1)

        count = backup_controller.import_backups(
            [self._manifest(2, prev=1)], _config().location(),
            cluster_id=CLUSTER_ID)

        assert count == 1

    def test_chain_mixing_encodings_is_refused(self, db, cluster):
        """A batch comes from one bucket, so the divergence it can still carry is
        in the encoding -- which each manifest states for itself."""
        with pytest.raises(PreconditionError, match="cannot span buckets or encodings"):
            backup_controller.import_backups(
                [self._manifest(1, with_compression=True),
                 self._manifest(2, prev=1)],
                _config().location(), cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_chain_reaching_into_another_bucket_is_refused(self, db, cluster):
        """The ancestor is already stored, and stored records do name a bucket."""
        _backup(db, 1, location=_config(bucket_name="elsewhere").location())

        with pytest.raises(PreconditionError, match="cannot span buckets or encodings"):
            backup_controller.import_backups(
                [self._manifest(2, prev=1)], _config().location(),
                cluster_id=CLUSTER_ID)

        assert [b.uuid for b in db.get_backups()] == [_backup_id(1)]

    def test_overlong_chain_is_refused(self, db, cluster):
        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.import_backups(
                self._line(constants.BACKUP_MAX_CHAIN_LENGTH + 1),
                _config().location(), cluster_id=CLUSTER_ID)

        assert db.get_backups() == []

    def test_a_chain_lengthened_past_the_limit_by_existing_records_is_refused(
            self, db, cluster):
        """The ancestry already in the database counts towards the limit."""
        previous = None
        for index in range(1, constants.BACKUP_MAX_CHAIN_LENGTH + 1):
            _backup(db, index, prev=previous)
            previous = index

        with pytest.raises(PreconditionError, match="data plane accepts at most"):
            backup_controller.import_backups(
                [self._manifest(999, prev=previous, s3_id=999)],
                _config().location(), cluster_id=CLUSTER_ID)

    def test_a_cyclic_chain_is_refused_rather_than_looping(self, db, cluster):
        with pytest.raises(PreconditionError, match="cyclic"):
            backup_controller.import_backups(
                [self._manifest(1, prev=2), self._manifest(2, prev=1)],
                _config().location(), cluster_id=CLUSTER_ID)

    def test_chain_mixing_encryption_is_refused(self, db, cluster):
        """Import used to check buckets, encodings and length but not encryption,
        so such a batch landed cleanly and failed at restore -- during the
        recovery it was meant to serve."""
        _backup(db, 1, encrypted=True)

        with pytest.raises(PreconditionError, match="cannot mix encrypted and unencrypted"):
            backup_controller.import_backups(
                [self._manifest(2, prev=1)], _config().location(),
                cluster_id=CLUSTER_ID)

        assert [b.uuid for b in db.get_backups()] == [_backup_id(1)]


class TestScheduledBackupPreconditions:
    """A schedule is the one thing that adds to a chain indefinitely.

    It used to call `create_single_backup` directly, checking nothing -- so the
    rules every hand-taken backup is held to were absent from the path that
    creates most of them. It is also the path that must refuse BEFORE taking its
    snapshot, or every scheduler tick leaves another orphan `auto_*` behind.
    """

    @pytest.fixture
    def volume(self, db):
        return _snapshot(db).lvol

    @pytest.fixture
    def snapshot_add(self):
        with patch("simplyblock_core.controllers.snapshot_controller.add") as add:
            yield add

    def _chain(self, db, length, **overrides):
        for index in range(1, length + 1):
            backup = _backup(db, index, prev=index - 1 if index > 1 else None,
                             **overrides)
            backup.node_id = NODE_ID
            backup.created_at = index
            backup.write_to_db(db.kv_store)

    def test_a_chain_at_the_limit_is_not_extended(
            self, db, cluster, node, volume, snapshot_add):
        self._chain(db, constants.BACKUP_MAX_CHAIN_LENGTH)

        backup_policy._auto_backup_lvol(volume)

        snapshot_add.assert_not_called()
        assert len(db.get_backups()) == constants.BACKUP_MAX_CHAIN_LENGTH
        assert db.get_job_tasks(CLUSTER_ID) == []

    def test_a_chain_in_another_bucket_is_not_extended(
            self, db, cluster, node, volume, snapshot_add):
        """The cluster's backup configuration was repointed since the last one."""
        self._chain(db, 1, location=_config(bucket_name="the-old-bucket").location())

        backup_policy._auto_backup_lvol(volume)

        snapshot_add.assert_not_called()
        assert len(db.get_backups()) == 1

    def test_an_encrypted_volume_over_a_plain_chain_is_not_extended(
            self, db, cluster, node, snapshot_add):
        volume = _snapshot(db, crypto=True).lvol
        self._chain(db, 1, encrypted=False)

        backup_policy._auto_backup_lvol(volume)

        snapshot_add.assert_not_called()
        assert len(db.get_backups()) == 1

    def test_a_healthy_chain_is_extended(
            self, db, cluster, node, volume, snapshot_add):
        """The refusals above must not be the only outcome this path has."""
        self._chain(db, 1)
        snapshot_add.return_value = (None, "stopped before the RPC")

        backup_policy._auto_backup_lvol(volume)

        snapshot_add.assert_called_once()


class TestExportSelection:

    def test_export_by_backup_id_yields_exactly_that_chain(self, db, cluster, node):
        """A volume can hold more than one chain -- after a bucket repoint, or an
        import. Exporting "everything with the same volume name" therefore hands
        an import backups that belong to a different restorable unit."""
        for index, prev in ((1, None), (2, 1), (3, None), (4, 3)):
            backup = _backup(db, index, prev=prev)
            backup.node_id = NODE_ID
            backup.write_to_db(db.kv_store)

        manifests = backup_controller.export_backups(
            cluster_id=CLUSTER_ID, backup_id=_backup_id(2))

        assert [str(m.backup_id) for m in manifests] == [_backup_id(1), _backup_id(2)]

    def test_export_by_volume_name_still_yields_everything(self, db, cluster, node):
        for index, prev in ((1, None), (2, 1), (3, None)):
            backup = _backup(db, index, prev=prev)
            backup.node_id = NODE_ID
            backup.write_to_db(db.kv_store)

        manifests = backup_controller.export_backups(
            cluster_id=CLUSTER_ID, lvol_name="vol")

        assert len(manifests) == 3
