# coding=utf-8
"""The self-describing part of a backup: a JSON manifest stored alongside its data.

The data plane writes only opaque objects keyed ``{s3_id}/{mid}/{extent}``, and
nothing in them records which volume they came from, how they are encoded, or
which other backups they depend on. All of that used to live exclusively in the
originating cluster's FoundationDB, which is precisely what a disaster recovery
no longer has.

A manifest closes that gap. It is written to the same bucket as the data it
describes, under a ``manifests/`` prefix -- a leading non-numeric segment, so it
cannot collide with the data plane's decimal keyspace. Given a bucket and
credentials for it, every backup in it can be enumerated, understood and
restored with no other input.

Credentials are deliberately absent: a manifest says *where* the objects are and
*how* to read them, never how to authenticate. The reader supplies that.
"""
import json
import logging
from typing import List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict

from simplyblock_core.models.backup_config import BackupConfig, BackupLocation
from simplyblock_core.utils.secrets import unwrap_secret


logger = logging.getLogger()

MANIFEST_PREFIX = "manifests/"

#: Bumped when the manifest's meaning changes in a way an older reader would
#: misinterpret. A reader must refuse a version it does not know rather than
#: guess -- restoring from a misread manifest corrupts the volume silently.
MANIFEST_SCHEMA_VERSION = 1


def manifest_key(backup_id: str) -> str:
    return f"{MANIFEST_PREFIX}{backup_id}.json"


class ChainEntry(BaseModel):
    """One link of the backup chain, oldest first."""
    model_config = ConfigDict(extra="forbid")

    backup_id: str
    s3_id: int


class Source(BaseModel):
    """Where this backup came from. Provenance for an operator reading a bucket.

    Nothing may resolve configuration or keys through these -- that dependency
    on the originating cluster is the whole problem being removed.
    """
    model_config = ConfigDict(extra="forbid")

    cluster_id: str = ""
    cluster_name: str = ""
    node_id: str = ""


class Volume(BaseModel):
    """The shape of the volume this backup was taken from.

    Restore recreates a volume from these rather than from hardcoded defaults,
    so a restored volume resembles the one that was backed up.
    """
    model_config = ConfigDict(extra="forbid")

    lvol_id: str = ""
    lvol_name: str = ""
    pool_name: str = ""
    snapshot_id: str = ""
    snapshot_name: str = ""
    size: int = 0
    allowed_hosts: List[dict] = []
    ha_type: str = "default"
    fabric: str = "tcp"
    lvol_priority_class: int = 0
    max_size: int = 0
    rw_ios_per_sec: int = 0
    rw_mbytes_per_sec: int = 0
    r_mbytes_per_sec: int = 0
    w_mbytes_per_sec: int = 0


class DataPlane(BaseModel):
    """How the objects are laid out, so a later format change is detectable."""
    model_config = ConfigDict(extra="forbid")

    #: Object key template. ``mid=1`` is metadata (``/1/0`` is the data plane's
    #: own root record, ``/1/n`` the extent map); ``mid=0`` is a data cluster.
    key_format: str = "{s3_id}/{mid}/{extent}"

    #: Object body size for both data and metadata objects.
    cluster_size: int = 0


class BackupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = MANIFEST_SCHEMA_VERSION
    backup_id: str
    s3_id: int
    created_at: int = 0
    completed_at: int = 0
    size: int = 0
    encrypted: bool = False
    prev_backup_id: str = ""

    location: BackupLocation

    #: The complete chain ending at this backup, oldest first and including
    #: itself, so a restore needs no object other than this one to know what to
    #: fetch. prev_backup_id alone would require walking manifest by manifest.
    chain: List[ChainEntry] = []

    source: Source = Source()
    volume: Volume = Volume()
    dataplane: DataPlane = DataPlane()


class ManifestError(Exception):
    """A manifest could not be read, written, or understood."""


def s3_client(config: BackupConfig):
    """A boto3 client for a backup location.

    Credentials are passed only when configured; omitting them lets boto3 fall
    back to its default provider chain (instance IAM role, environment,
    profile), which is what an absent ``credentials`` means.
    """
    return boto3.client("s3",
        region_name=config.region,
        endpoint_url=config.endpoint_url,
        verify=config.verify_tls,
        config=BotoConfig(s3={"addressing_style": "path" if config.use_path_style else "auto"}),
        aws_access_key_id=(
            unwrap_secret(config.credentials.access_key_id)
            if config.credentials is not None else None),
        aws_secret_access_key=(
            unwrap_secret(config.credentials.secret_access_key)
            if config.credentials is not None else None),
    )


def write(config: BackupConfig, manifest: BackupManifest) -> None:
    """Store a manifest next to the data it describes.

    Raises:
        ManifestError: The manifest could not be stored. Callers must treat this
            as a failed backup: data in the bucket with no manifest is data
            nobody can identify later.
    """
    try:
        s3_client(config).put_object(
            Bucket=config.bucket_name,
            Key=manifest_key(manifest.backup_id),
            Body=manifest.model_dump_json().encode(),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            f"Failed to write manifest for backup {manifest.backup_id}") from e

    logger.info("Wrote manifest for backup %s to %s",
                manifest.backup_id, config.bucket_name)


def read(config: BackupConfig, backup_id: str) -> BackupManifest:
    """Load one manifest by backup id.

    Raises:
        ManifestError: It is absent, unreadable, or a schema version this build
            does not understand.
    """
    try:
        body = s3_client(config).get_object(
            Bucket=config.bucket_name, Key=manifest_key(backup_id))["Body"].read()
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            f"Failed to read manifest for backup {backup_id} "
            f"from {config.bucket_name}") from e

    return _parse(body, manifest_key(backup_id))


def list_all(config: BackupConfig) -> List[BackupManifest]:
    """Every manifest in the bucket, newest first.

    This is the disaster-recovery entry point: with a bucket and credentials it
    answers "what is in here" without reference to any cluster.

    Raises:
        ManifestError: The bucket could not be listed, or one of its manifests
            could not be parsed. Deliberately not best-effort -- silently
            omitting an unreadable backup from a recovery listing is how an
            operator concludes their data is gone.
    """
    client = s3_client(config)
    manifests = []

    try:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=config.bucket_name, Prefix=MANIFEST_PREFIX)
        for page in pages:
            for entry in page.get("Contents", []):
                key = entry["Key"]
                if not key.endswith(".json"):
                    continue
                body = client.get_object(Bucket=config.bucket_name, Key=key)["Body"].read()
                manifests.append(_parse(body, key))
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(f"Failed to list manifests in {config.bucket_name}") from e

    manifests.sort(key=lambda m: (m.created_at, m.backup_id), reverse=True)
    return manifests


def delete(config: BackupConfig, backup_id: str) -> None:
    """Remove a manifest.

    Only for a backup whose data is genuinely gone -- a merge folding it into
    its successor. Deleting the manifest of a backup whose objects still exist
    turns them into weight nobody can identify or reclaim.
    """
    try:
        s3_client(config).delete_object(
            Bucket=config.bucket_name, Key=manifest_key(backup_id))
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(f"Failed to delete manifest for backup {backup_id}") from e


def _parse(body: bytes, key: str) -> BackupManifest:
    try:
        data = json.loads(body)
    except ValueError as e:
        raise ManifestError(f"Manifest {key} is not valid JSON") from e

    version = data.get("schema_version")
    if version != MANIFEST_SCHEMA_VERSION:
        # Refuse rather than guess: a manifest written by a newer control plane
        # may mean something different by the same field names, and restoring
        # from a misread manifest corrupts the volume without an error.
        raise ManifestError(
            f"Manifest {key} has schema version {version}, "
            f"this build understands {MANIFEST_SCHEMA_VERSION}")

    try:
        return BackupManifest.model_validate(data)
    except ValueError as e:
        raise ManifestError(f"Manifest {key} is malformed: {e}") from e


def find_location(manifests: List[BackupManifest]) -> Optional[BackupLocation]:
    """The single location shared by a set of manifests, or None if they differ.

    Every backup in one chain must live in one bucket, encoded the same way;
    otherwise a restore would have to read half its clusters from somewhere
    else, which no part of the stack can express.
    """
    locations = {m.location for m in manifests}
    return locations.pop() if len(locations) == 1 else None
