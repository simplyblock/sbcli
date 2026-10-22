"""Removing a group manifest from its bucket (design §9).

Removing the group manifest un-commits its group backup: the bucket stops naming
its members as one restorable generation. The manifest module therefore offers
no unguarded delete. ``delete_retired_group`` removes only a manifest that a
group backup delete has marked ``deleting``, and refuses a committed or broken
one, which leaves it in place.

The S3 client is the one boundary faked, as an in-memory bucket: the manifest
functions under test run for real over it, and no database is involved.
"""
import io
import zlib
from datetime import UTC, datetime
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from simplyblock_core.controllers.backup import manifest as backup_manifest
from simplyblock_core.controllers.backup.manifest import (
    GroupBackupManifest,
    GroupManifestSource,
    GroupManifestState,
    ManifestError,
)
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup_config import BackupConfig


def _id(name: str) -> UUID:
    return UUID(f"{zlib.crc32(name.encode()):08x}-0000-4000-8000-000000000000")


GROUP_BACKUP = _id("gb-1")
CONFIG = BackupConfig.model_validate({"bucket_name": "backups", "region": "eu-central-1"})


class _Bucket:
    """The three object calls the group manifest functions make."""

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


def _store(state):
    backup_manifest.write_group(CONFIG, GroupBackupManifest(
        group_backup_id=GROUP_BACKUP,
        created_at=datetime.fromtimestamp(100, tz=UTC),
        completed_at=datetime.fromtimestamp(200, tz=UTC),
        size=4096,
        source=GroupManifestSource(
            cluster_id=_id("cluster"), consistency_group_id=f"{_id('cluster')}/{_id('cg')}",
            consistency_group_name="gitea", generation=7),
        member_backup_ids=[_id("b-db")],
        state=state,
    ))


KEY = backup_manifest.group_manifest_key(GROUP_BACKUP)


def test_the_module_offers_no_unguarded_group_manifest_delete():
    assert not hasattr(backup_manifest, "delete_group")


def test_a_manifest_marked_deleting_is_removed(bucket):
    _store(GroupManifestState.DELETING)

    backup_manifest.delete_retired_group(CONFIG, GROUP_BACKUP)

    assert KEY not in bucket.objects


@pytest.mark.parametrize("state", [GroupManifestState.COMMITTED, GroupManifestState.BROKEN])
def test_a_manifest_not_marked_deleting_is_refused_and_kept(bucket, state):
    _store(state)

    with pytest.raises(PreconditionError, match=str(state)):
        backup_manifest.delete_retired_group(CONFIG, GROUP_BACKUP)

    assert KEY in bucket.objects


def test_an_absent_manifest_is_already_removed(bucket):
    """A retried delete pass that removed the manifest but not yet the record."""
    backup_manifest.delete_retired_group(CONFIG, GROUP_BACKUP)

    assert bucket.objects == {}


def test_a_bucket_that_cannot_be_read_is_a_manifest_error(monkeypatch):
    b = _Bucket(fail=True)
    monkeypatch.setattr(backup_manifest, "s3_client", lambda config: b)

    with pytest.raises(ManifestError):
        backup_manifest.delete_retired_group(CONFIG, GROUP_BACKUP)
