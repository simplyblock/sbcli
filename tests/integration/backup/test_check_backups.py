"""Checking a backup's FDB record against its S3 manifest, against real
FoundationDB.

The backup analog of the secondary-index checker (``check_indices``):
read-only unless ``--repair`` is given, and reports in both directions --
a record whose manifest is missing or disagrees, and a manifest naming no
known record. Only the first direction is ever auto-repaired; an orphaned
manifest may be a disaster-recovery backup nothing has imported yet, which
looks identical to genuine drift from here.

The database is real. The bucket is the one boundary faked, as an in-memory
one, so the manifest functions under test run for real over it.
"""
import io
import zlib
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from simplyblock_core.controllers.backup import controller as backup_controller
from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest,
    BackupManifestState,
    ManifestDataPlane,
    ManifestSource,
    ManifestVolume,
)
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup
from simplyblock_core.models.backup_config import BackupConfig
from simplyblock_core.models.cluster import Cluster


def _uuid(name: str) -> str:
    return f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000"


CLUSTER = _uuid("cluster")
OTHER_CLUSTER = _uuid("other-cluster")
LVOL = _uuid("lvol")
NODE = _uuid("node")
BUCKET = "simplyblock-backup-cluster"


def _config():
    return BackupConfig.model_validate({"bucket_name": BUCKET, "region": "eu-central-1"})


@pytest.fixture
def db():
    db = DBController()
    for cluster_id in (CLUSTER, OTHER_CLUSTER):
        c = Cluster()
        c.uuid = cluster_id
        c.backup_config = _config().model_dump(exclude_none=True)
        c.write_to_db(db.kv_store)
    return db


class _Bucket:
    def __init__(self):
        self.objects: dict = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)

    def get_paginator(self, _name):
        objects = self.objects

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in objects if k.startswith(Prefix)]}
        return _Paginator()


@pytest.fixture
def bucket(monkeypatch):
    b = _Bucket()
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)
    return b


def _manifest(backup_id: str, prev: str = "", state=BackupManifestState.ACTIVE,
              s3_id: int | None = None) -> BackupManifest:
    # s3_id defaults to a value derived from backup_id itself -- fine for an
    # orphan manifest naming no real backup, but a manifest meant to describe
    # a real one MUST pass that backup's own s3_id: a Backup's s3_id is
    # whatever was assigned at creation, not derivable from its uuid.
    return BackupManifest(
        backup_id=UUID(backup_id),
        s3_id=s3_id if s3_id is not None else zlib.crc32(backup_id.encode()) % 1000,
        created_at=100,
        completed_at=200,
        size=4096,
        prev_backup_id=UUID(prev) if prev else None,
        source=ManifestSource(cluster_id=UUID(CLUSTER), node_id=UUID(NODE)),
        volume=ManifestVolume(
            lvol_id=UUID(LVOL), lvol_name="vol", snapshot_id=UUID(_uuid("snap")),
            snapshot_name="snap", size=4096),
        dataplane=ManifestDataPlane(),
        state=state,
    )


def _backup(db, name, prev="", status=Backup.STATUS_COMPLETED, cluster_id=CLUSTER):
    b = Backup()
    b.uuid = _uuid(name)
    b.cluster_id = cluster_id
    b.lvol_id = LVOL
    b.lvol_name = "vol"
    # build_manifest (the --repair path) needs a valid snapshot_id: a real
    # COMPLETED backup always has one.
    b.snapshot_id = _uuid(f"snap-{name}")
    b.snapshot_name = "snap"
    b.node_id = NODE
    b.size = 4096
    b.s3_id = zlib.crc32(name.encode()) % 1000
    b.prev_backup_id = _uuid(prev) if prev else ""
    b.status = status
    b.location = _config().location().model_dump(mode="json")
    b.write_to_db(db.kv_store)
    return b


def test_a_consistent_completed_backup_passes(db, bucket):
    b = _backup(db, "leaf")
    backup_manifest.write(_config(), _manifest(b.uuid, s3_id=b.s3_id))

    assert backup_controller.check_backups(cluster_id=CLUSTER) is True


def test_a_completed_backup_with_no_manifest_fails_and_repairs(db, bucket):
    b = _backup(db, "leaf")

    assert backup_controller.check_backups(cluster_id=CLUSTER) is False

    assert backup_controller.check_backups(cluster_id=CLUSTER, repair=True) is True
    assert backup_manifest.read(_config(), UUID(b.uuid)).state == BackupManifestState.ACTIVE
    # A second run finds it already fixed.
    assert backup_controller.check_backups(cluster_id=CLUSTER) is True


def test_a_disagreeing_manifest_fails_and_repairs(db, bucket):
    b = _backup(db, "leaf", prev="base")
    # Wrong prev_backup_id in the manifest -- as if a merge updated the
    # record but not (yet) the bucket.
    backup_manifest.write(_config(), _manifest(b.uuid, prev="", s3_id=b.s3_id))

    assert backup_controller.check_backups(cluster_id=CLUSTER) is False

    assert backup_controller.check_backups(cluster_id=CLUSTER, repair=True) is True
    assert backup_manifest.read(_config(), UUID(b.uuid)).prev_backup_id == UUID(_uuid("base"))


def test_a_deleting_backup_with_an_active_manifest_fails_and_repairs(db, bucket):
    b = _backup(db, "leaf", status=Backup.STATUS_DELETING)
    backup_manifest.write(_config(), _manifest(b.uuid, state=BackupManifestState.ACTIVE, s3_id=b.s3_id))

    assert backup_controller.check_backups(cluster_id=CLUSTER) is False

    assert backup_controller.check_backups(cluster_id=CLUSTER, repair=True) is True
    assert backup_manifest.read(_config(), UUID(b.uuid)).state == BackupManifestState.DELETING


def test_an_orphaned_manifest_fails_and_is_never_repaired(db, bucket):
    # A manifest with no backing record at all -- s3_id matches nothing.
    backup_manifest.write(_config(), _manifest(_uuid("ghost")))

    assert backup_controller.check_backups(cluster_id=CLUSTER) is False
    assert backup_controller.check_backups(cluster_id=CLUSTER, repair=True) is False


def test_a_merged_backups_leftover_manifest_is_not_flagged(db, bucket):
    """MERGED/MERGING are excluded: the merge path does not mark manifest
    state, so a leftover manifest here is a known gap, not this check's to
    report."""
    b = _backup(db, "old", status=Backup.STATUS_MERGED)
    backup_manifest.write(_config(), _manifest(b.uuid, s3_id=b.s3_id))

    assert backup_controller.check_backups(cluster_id=CLUSTER) is True


def test_scoping_to_one_cluster_ignores_another_clusters_problems(db, bucket):
    b = _backup(db, "leaf", cluster_id=CLUSTER)
    backup_manifest.write(_config(), _manifest(b.uuid, s3_id=b.s3_id))
    # No manifest for this one -- a real problem, but in a different cluster.
    _backup(db, "other-leaf", cluster_id=OTHER_CLUSTER)

    assert backup_controller.check_backups(cluster_id=CLUSTER) is True
    assert backup_controller.check_backups(cluster_id=OTHER_CLUSTER) is False
