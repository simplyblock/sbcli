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

Each manifest describes exactly one backup and names only its immediate
predecessor. Chains are walked at read time by :func:`chain_of`, not stored: a
stored chain would have to be rewritten in every descendant's manifest each time
a merge folded a backup away, so a single merge would cost a write per descendant
and could half-fail, leaving the bucket advertising keys the data plane had
already unmapped.

This document overlaps the ``Backup`` record in FoundationDB by design, and
substantially -- see the note at the top of ``controllers/backup_controller.py``
for where the two genuinely differ and where they should be collapsed.
"""
import json
import logging
from typing import Iterable, List, Literal, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, model_validator

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


class Source(BaseModel):
    """Where this backup came from. Provenance for an operator reading a bucket.

    Nothing may resolve configuration or keys through these -- that dependency
    on the originating cluster is the whole problem being removed.
    """
    model_config = ConfigDict(extra="forbid")

    cluster_id: str
    node_id: str

    #: Absent when the cluster's own record of its name was no longer readable
    #: at the time the manifest was written.
    cluster_name: Optional[str] = None


class Volume(BaseModel):
    """The shape of the volume this backup was taken from.

    Split in two by what is knowable. The identity and size come off the backup
    record and are always present. The settings below them come off the live
    volume, so they are absent together once that volume is deleted -- and
    absent is not the same answer as ``0``, which for a QoS cap means
    "unlimited" and for a priority class is a real class.

    Nothing reads the settings yet; restore still creates its volume with
    hardcoded defaults. They are recorded anyway because a manifest is read
    years after it is written, and a backup taken today cannot be given a shape
    retroactively once its volume is gone.
    """
    model_config = ConfigDict(extra="forbid")

    lvol_id: str
    lvol_name: str
    snapshot_id: str
    snapshot_name: str
    size: int
    allowed_hosts: List[dict] = []

    pool_name: Optional[str] = None
    ha_type: Optional[str] = None
    fabric: Optional[str] = None
    lvol_priority_class: Optional[int] = None
    max_size: Optional[int] = None
    rw_ios_per_sec: Optional[int] = None
    rw_mbytes_per_sec: Optional[int] = None
    r_mbytes_per_sec: Optional[int] = None
    w_mbytes_per_sec: Optional[int] = None


class KeyDescriptor(BaseModel):
    """Where this backup's data encryption key lives. Never the key itself.

    Restoring an encrypted backup means reaching the KMS named here. The working
    assumption is that a KMS is recoverable independently of the cluster that
    used it -- a Vault deployment outlives one cluster, and the FoundationDB
    behind LocalKMS is itself backed up -- so recording the dependency is enough,
    and no key material has to travel with the ciphertext.

    Recording it as a document rather than as loose fields is also what leaves
    room to change that assumption: a scheme that wraps the keys under an
    operator-held secret adds a sibling field here and a branch in
    _resolve_crypto_key, and touches nothing else.
    """
    model_config = ConfigDict(extra="forbid")

    #: Which backend holds the key. Named rather than free text, because a reader
    #: years later has to know which of the fields below mean anything.
    kms: Literal["hashicorp_vault", "local"]

    dek_path: str
    kek_name: str

    #: Vault only; absent for the local backend.
    vault_base_url: Optional[str] = None
    transit_mount: Optional[str] = None
    kv_mount: Optional[str] = None


class Encryption(BaseModel):
    """Whether this backup is ciphertext, and if so how to reach its key."""
    model_config = ConfigDict(extra="forbid")

    encrypted: bool

    #: Where the key lives. Required for an encrypted backup and meaningless
    #: otherwise, so the two cannot disagree.
    descriptor: Optional[KeyDescriptor] = None

    @model_validator(mode="after")
    def _descriptor_matches_encrypted(self) -> "Encryption":
        if self.encrypted and self.descriptor is None:
            raise ValueError(
                "An encrypted backup must record where its key lives; without "
                "that, nothing can decrypt it and nothing can say why")
        if not self.encrypted and self.descriptor is not None:
            raise ValueError(
                "An unencrypted backup has no key to describe")
        return self


class DataPlane(BaseModel):
    """How the objects are laid out, so a later format change is detectable."""
    model_config = ConfigDict(extra="forbid")

    #: Object key template. ``mid=1`` is metadata (``/1/0`` is the data plane's
    #: own root record, ``/1/n`` the extent map); ``mid=0`` is a data cluster.
    key_format: str = "{s3_id}/{mid}/{extent}"

    #: Object body size for both data and metadata objects. Absent when the
    #: writing cluster's record was unreadable -- a reader then has to fall back
    #: on the data plane's own default, which is why it is not silently 0.
    cluster_size: Optional[int] = None


class BackupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = MANIFEST_SCHEMA_VERSION
    backup_id: str
    s3_id: int
    created_at: int
    completed_at: int
    size: int

    #: The backup this one is a delta against, or absent when it is a full
    #: backup and therefore the root of its chain.
    #:
    #: The chain itself is deliberately NOT stored. It is derivable by following
    #: these links across the manifests in the bucket, and storing it would make
    #: every merge invalidate the manifest of every descendant of the merged-away
    #: backup -- so the write amplification of a merge would be the length of the
    #: chain, and a partial failure would leave the bucket advertising object keys
    #: the data plane had already unmapped.
    prev_backup_id: Optional[str] = None

    location: BackupLocation
    encryption: Encryption
    source: Source
    volume: Volume
    dataplane: DataPlane


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


def chain_of(manifest: BackupManifest,
             manifests: Iterable[BackupManifest]) -> List[BackupManifest]:
    """The chain ending at ``manifest``, oldest first and including itself.

    Derived by following ``prev_backup_id`` through the manifests supplied,
    rather than read from a list stored in each one. That keeps a merge a
    two-object write -- republish the survivor, delete the merged-away one --
    instead of a rewrite of every descendant's manifest.

    Raises:
        ManifestError: A link points at a backup that is not among the manifests
            supplied, so the chain cannot be completed from them. Reported rather
            than truncated: a short chain restores a volume with holes in it.
    """
    by_id = {m.backup_id: m for m in manifests}

    chain = [manifest]
    while (previous := chain[-1].prev_backup_id) is not None:
        if previous not in by_id:
            raise ManifestError(
                f"Backup {chain[-1].backup_id} is a delta against {previous}, "
                "which is not among the manifests given")
        if previous in {m.backup_id for m in chain}:
            raise ManifestError(
                f"Backup {manifest.backup_id} has a cyclic chain at {previous}")
        chain.append(by_id[previous])

    chain.reverse()
    return chain
