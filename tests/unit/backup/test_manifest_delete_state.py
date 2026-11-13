"""Marking a single-backup manifest DELETING/ACTIVE, ahead of actually
removing anything.

A delete marks a backup's manifest DELETING before any of its S3 objects are
touched, and reverts it to ACTIVE if the delete does not finish -- the
single-backup half of the metadata-before-data ordering the group manifest's
``GroupManifestState`` already keeps. Both functions are read-modify-write
over the bucket alone; no database is involved.

The S3 client is the one boundary faked, as an in-memory bucket, following
the same pattern as ``test_group_manifest_delete.py``.
"""
import io
import zlib
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    BackupManifest,
    BackupManifestState,
    ManifestDataPlane,
    ManifestError,
    ManifestSource,
    ManifestVolume,
)
from simplyblock_core.models.backup_config import BackupConfig


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


BACKUP = _id("b-1")
CONFIG = BackupConfig.model_validate({"bucket_name": "backups", "region": "eu-central-1"})
KEY = backup_manifest.manifest_key(BACKUP)


class _Bucket:
    """The three object calls mark_deleting/mark_active make."""

    def __init__(self, fail=False):
        self.objects: dict = {}
        self.fail = fail

    def _check(self, op):
        if self.fail:
            raise ClientError({"Error": {"Code": "InternalError"}}, op)

    def put_object(self, Bucket, Key, Body, **_):
        self._check("PutObject")
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        self._check("GetObject")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self._check("DeleteObject")
        self.objects.pop(Key, None)


@pytest.fixture
def bucket(monkeypatch):
    b = _Bucket()
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)
    return b


def _store(state=BackupManifestState.ACTIVE):
    backup_manifest.write(CONFIG, BackupManifest(
        backup_id=BACKUP,
        s3_id=1,
        created_at=100,
        completed_at=200,
        size=4096,
        source=ManifestSource(cluster_id=_id("cluster"), node_id=_id("node")),
        volume=ManifestVolume(
            lvol_id=_id("lvol"), lvol_name="vol", snapshot_id=_id("snap"),
            snapshot_name="snap", size=4096),
        dataplane=ManifestDataPlane(),
        state=state,
    ))


def test_mark_deleting_rewrites_an_active_manifest(bucket):
    _store(BackupManifestState.ACTIVE)

    backup_manifest.mark_deleting(CONFIG, BACKUP)

    assert backup_manifest.read(CONFIG, BACKUP).state == BackupManifestState.DELETING


def test_mark_deleting_is_a_noop_if_already_deleting(bucket):
    _store(BackupManifestState.DELETING)

    backup_manifest.mark_deleting(CONFIG, BACKUP)

    assert backup_manifest.read(CONFIG, BACKUP).state == BackupManifestState.DELETING


def test_mark_deleting_is_a_noop_if_the_manifest_is_already_gone(bucket):
    backup_manifest.mark_deleting(CONFIG, BACKUP)

    assert bucket.objects == {}


def test_mark_active_reverts_a_deleting_manifest(bucket):
    _store(BackupManifestState.DELETING)

    backup_manifest.mark_active(CONFIG, BACKUP)

    assert backup_manifest.read(CONFIG, BACKUP).state == BackupManifestState.ACTIVE


def test_mark_active_is_a_noop_if_already_active(bucket):
    _store(BackupManifestState.ACTIVE)

    backup_manifest.mark_active(CONFIG, BACKUP)

    assert backup_manifest.read(CONFIG, BACKUP).state == BackupManifestState.ACTIVE


def test_mark_active_is_a_noop_if_the_manifest_is_already_gone(bucket):
    backup_manifest.mark_active(CONFIG, BACKUP)

    assert bucket.objects == {}


def test_a_bucket_that_cannot_be_read_is_a_manifest_error(monkeypatch):
    b = _Bucket(fail=True)
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)

    with pytest.raises(ManifestError):
        backup_manifest.mark_deleting(CONFIG, BACKUP)
