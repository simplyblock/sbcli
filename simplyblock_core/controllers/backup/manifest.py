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
substantially -- see the note on ``controller.build_manifest``
for where the two genuinely differ and where they should be collapsed.
"""
import json
import logging
from typing import (
    Annotated, Any, Iterable, List, Literal, Optional, Tuple, Union)

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import (
    BaseModel, ConfigDict, Field, HttpUrl, TypeAdapter, model_validator)

from simplyblock_core.kms import KMS
from simplyblock_core.models.backup_config import BackupConfig, BackupLocation
from simplyblock_core.utils import NQN
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

    #: The NQNs allowed to attach, so a restore recreates the volume's
    #: allow-list rather than an open subsystem. NQNs alone: the control plane's
    #: own host entries also carry that host's DHCHAP keys and PSK, which no
    #: reader of a manifest needs -- restore passes the NQNs to add_lvol_ha,
    #: which mints fresh keys from the target pool -- and which a manifest must
    #: not carry any more than it carries bucket credentials.
    allowed_hosts: List[NQN] = []

    pool_name: Optional[str] = None

    #: "default" is not among these: it is the request-time way of saying "the
    #: cluster's", which the volume no longer has once it exists. A volume whose
    #: record does not say is recorded as absent rather than as a guess.
    ha_type: Optional[Literal["single", "ha"]] = None

    fabric: Optional[Literal["tcp", "rdma", "tcp,rdma"]] = None
    lvol_priority_class: Optional[int] = None
    max_size: Optional[int] = None
    rw_ios_per_sec: Optional[int] = None
    rw_mbytes_per_sec: Optional[int] = None
    r_mbytes_per_sec: Optional[int] = None
    w_mbytes_per_sec: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def _hosts_to_nqns(cls, data: Any) -> Any:
        """Take the NQN out of a host entry that carries more than one.

        Two inputs arrive this way: the control plane's own ``allowed_hosts``
        dicts, which is where the key material would otherwise come from, and a
        manifest written before this field was narrowed. Both are read for the
        NQNs they hold rather than refused, and the rest is dropped here -- so
        the next write of that manifest no longer republishes it.
        """
        if isinstance(data, dict) and isinstance(data.get("allowed_hosts"), list):
            data = dict(data)
            data["allowed_hosts"] = [
                host["nqn"] if isinstance(host, dict) else host
                for host in data["allowed_hosts"]
            ]

        return data


class _KeyDescriptor(BaseModel):
    """Where this backup's data encryption key lives. Never the key itself.

    Restoring an encrypted backup means reaching the KMS described here. The
    working assumption is that a KMS is recoverable independently of the cluster
    that used it -- a Vault deployment outlives one cluster, and the FoundationDB
    behind LocalKMS is itself backed up -- so recording the dependency is enough,
    and no key material has to travel with the ciphertext.

    One subclass per backend, discriminated on ``type``, because which fields
    mean anything depends entirely on which backend holds the key: a Vault
    descriptor needs the transit key that unwraps its DEK, and the local one has
    no such key to name. A reader years later gets the fields its backend
    defines and no others, rather than a flat record whose optional halves it has
    to know how to interpret.

    A future scheme -- keys wrapped under an operator-held secret, say -- is a
    third subclass and a third ``read_keys``, and touches nothing else.
    """
    model_config = ConfigDict(extra="forbid")

    #: Where the keys themselves are: the FoundationDB key under the local
    #: backend, the KV path under Vault. Every backend addresses its keys
    #: somehow, so this is the one field they share.
    dek_path: str

    def read_keys(self, kms: KMS) -> Tuple[str, str]:
        """Read this backup's data encryption keys out of an open KMS.

        Here rather than at the call site because how a backend is addressed is
        exactly what distinguishes these types.

        Raises:
            KMSException: The KMS could not be reached, or holds no such key.
        """
        raise NotImplementedError


class FDBKeyDescriptor(_KeyDescriptor):
    """Keys held in the cluster's own FoundationDB, by ``LocalKMS``."""

    type: Literal["fdb"] = "fdb"

    def read_keys(self, kms: KMS) -> Tuple[str, str]:
        # LocalKMS has no key-encryption key at all: it stores DEKs as they are,
        # its KEK operations are no-ops, and it ignores the name it is passed.
        return kms.get_data_encryption_keys(self.dek_path, "")


class HCPKeyDescriptor(_KeyDescriptor):
    """Keys held in HashiCorp Vault, wrapped under a named transit key."""

    type: Literal["hcp"] = "hcp"

    #: The transit key the DEKs are wrapped under. Required, because unwrapping
    #: them is not possible without naming it.
    kek_name: str

    #: Absent where the originating cluster's Vault settings did not record
    #: them; a reader then falls back on its own configuration.
    vault_base_url: Optional[HttpUrl] = None
    transit_mount: Optional[str] = None
    kv_mount: Optional[str] = None

    def read_keys(self, kms: KMS) -> Tuple[str, str]:
        return kms.get_data_encryption_keys(self.dek_path, self.kek_name)


#: Tagged on ``type``, so a stored descriptor is read back as the backend that
#: wrote it rather than matched against each shape in turn.
KeyDescriptor = Annotated[
    Union[FDBKeyDescriptor, HCPKeyDescriptor], Field(discriminator="type")]

_key_descriptor_adapter: TypeAdapter = TypeAdapter(KeyDescriptor)


def parse_key_descriptor(record: dict) -> KeyDescriptor:
    """Read a stored key descriptor, whichever backend wrote it.

    Raises:
        ValidationError: The record names no known backend, or is missing what
            that backend needs to find its keys.
    """
    return _key_descriptor_adapter.validate_python(record)


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

    #: Where this backup's key lives, or absent for a backup that is not
    #: encrypted at all. One optional document rather than a flag beside it: two
    #: fields for one fact can contradict each other, and a manifest is read
    #: exactly when nobody is left who could say which one was right.
    encryption: Optional[KeyDescriptor] = None

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
