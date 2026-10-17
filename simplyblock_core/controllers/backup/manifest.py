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

How to reach the bucket is deliberately absent -- not just the credentials, but
the bucket name, region and endpoint too. A reader holds all of that already, or
it could not have fetched the manifest, and a stored copy can only disagree with
where the manifest was actually found: replicate a bucket and every manifest in
the copy still names the original. So a manifest describes the objects, and the
reader supplies the connection.

What survives is what reading the bucket cannot tell you: the key layout and
whether the bodies are compressed, both on ``dataplane``.

An *export* is the one document that does record a location, because it is not
stored in the bucket it describes: ``BackupExport`` carries manifests grouped by
where they were read from, so a file handed to another cluster says where its
backups live and the reader names nothing. The self-reference above does not
arise -- the file is not a copy of the thing it points at.

Each manifest describes exactly one backup and names only its immediate
predecessor. Chains are walked at read time, by ``chain.BackupChain``, not
stored: a stored chain would have to be rewritten in every descendant's manifest
each time a merge folded a backup away, so a single merge would cost a write per
descendant and could half-fail, leaving the bucket advertising keys the data
plane had already unmapped.

This document overlaps the ``Backup`` record in FoundationDB by design, and
substantially -- see the note on ``controller.build_manifest``
for where the two genuinely differ and where they should be collapsed.
"""
import json
import logging
from typing import Annotated, Literal, Union
from uuid import UUID

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, TypeAdapter

from simplyblock_core.kms import KMS
from simplyblock_core.models.backup_config import BackupConfig, BackupLocation
from simplyblock_core.utils.secrets import unwrap_secret

logger = logging.getLogger()

MANIFEST_PREFIX = "manifests/"

#: Bumped when the manifest's meaning changes in a way an older reader would
#: misinterpret. A reader must refuse a version it does not know rather than
#: guess -- restoring from a misread manifest corrupts the volume silently.
MANIFEST_SCHEMA_VERSION = 1


def manifest_key(backup_id: UUID) -> str:
    return f"{MANIFEST_PREFIX}{backup_id}.json"


class ManifestSource(BaseModel):
    """Where this backup came from. Provenance for an operator reading a bucket.

    Nothing may resolve configuration or keys through these -- that dependency
    on the originating cluster is the whole problem being removed.
    """
    model_config = ConfigDict(extra="forbid")

    cluster_id: UUID
    node_id: UUID

    #: Absent when the cluster's own record of its name was no longer readable
    #: at the time the manifest was written.
    cluster_name: str | None = None


class ManifestVolume(BaseModel):
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

    The volume's allow-list is deliberately not among them. Who may attach is a
    property of the pool a volume lives in, not of the bytes a backup holds, and
    a restore lands in whichever pool it is given -- possibly in another cluster,
    where the source volume's NQNs mean nothing. So a restored volume takes the
    target pool's host configuration, and a stale allow-list from the source
    never overrides it.
    """
    model_config = ConfigDict(extra="forbid")

    lvol_id: UUID
    lvol_name: str
    snapshot_id: UUID
    snapshot_name: str
    size: int

    pool_name: str | None = None

    #: "default" is not among these: it is the request-time way of saying "the
    #: cluster's", which the volume no longer has once it exists. A volume whose
    #: record does not say is recorded as absent rather than as a guess.
    ha_type: Literal["single", "ha"] | None = None

    fabric: Literal["tcp", "rdma", "tcp,rdma"] | None = None
    lvol_priority_class: int | None = None
    max_size: int | None = None
    rw_ios_per_sec: int | None = None
    rw_mbytes_per_sec: int | None = None
    r_mbytes_per_sec: int | None = None
    w_mbytes_per_sec: int | None = None


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

    def read_keys(self, kms: KMS) -> tuple[SecretStr, SecretStr]:
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

    def read_keys(self, kms: KMS) -> tuple[SecretStr, SecretStr]:
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
    vault_base_url: HttpUrl | None = None
    transit_mount: str | None = None
    kv_mount: str | None = None

    def read_keys(self, kms: KMS) -> tuple[SecretStr, SecretStr]:
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


class ManifestDataPlane(BaseModel):
    """How the objects are encoded, so a later format change is detectable.

    Everything here has to be recorded because reading the bucket cannot
    recover it -- unlike the bucket's name, region and endpoint, which the
    reader necessarily supplied to get this far.
    """
    model_config = ConfigDict(extra="forbid")

    #: Object key template. ``mid=1`` is metadata (``/1/0`` is the data plane's
    #: own root record, ``/1/n`` the extent map); ``mid=0`` is a data cluster.
    key_format: str = "{s3_id}/{mid}/{extent}"

    #: Object body size for both data and metadata objects. Absent when the
    #: writing cluster's record was unreadable -- a reader then has to fall back
    #: on the data plane's own default, which is why it is not silently 0.
    cluster_size: int | None = None

    #: Whether the object bodies are ISA-L compressed. Not detectable by
    #: inspecting them, and reading them under the wrong answer yields garbage
    #: rather than an error, so it travels with the backup.
    with_compression: bool = False


class BackupManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = MANIFEST_SCHEMA_VERSION
    backup_id: UUID
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
    prev_backup_id: UUID | None = None

    #: Where this backup's key lives, or absent for a backup that is not
    #: encrypted at all. One optional document rather than a flag beside it: two
    #: fields for one fact can contradict each other, and a manifest is read
    #: exactly when nobody is left who could say which one was right.
    encryption: KeyDescriptor | None = None

    source: ManifestSource
    volume: ManifestVolume
    dataplane: ManifestDataPlane


EXPORT_SCHEMA_VERSION = 1


class LocatedManifests(BaseModel):
    """Manifests that were read from one location, and that location."""
    model_config = ConfigDict(extra="forbid")

    location: BackupLocation
    manifests: list[BackupManifest]


class BackupExport(BaseModel):
    """Backups carried out of a cluster in a file, grouped by where they live.

    Grouped rather than one location for the whole document because a cluster
    can hold backups in several buckets at once -- its own, plus any it has
    imported -- and stamping all of them with a single bucket leaves the ones it
    does not describe unrestorable, which is discovered during the recovery they
    were meant to serve.

    A group's manifests are all in one bucket by construction: a chain cannot
    span buckets, so the only way to collect backups from several is to walk
    more than one chain.
    """
    model_config = ConfigDict(extra="forbid")

    schema_version: int = EXPORT_SCHEMA_VERSION
    groups: list[LocatedManifests]


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


def read(config: BackupConfig, backup_id: UUID) -> BackupManifest:
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


def list_all(config: BackupConfig) -> list[BackupManifest]:
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


def delete(config: BackupConfig, backup_id: UUID) -> None:
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


def parse_export(body: bytes, source: str) -> BackupExport:
    """Read an export document, naming *source* in anything it rejects.

    Raises:
        ManifestError: It is not valid JSON, states a schema version this build
            does not understand, or is not shaped like an export.
    """
    try:
        data = json.loads(body)
    except ValueError as e:
        raise ManifestError(f"{source} is not valid JSON") from e

    version = data.get("schema_version") if isinstance(data, dict) else None
    if version != EXPORT_SCHEMA_VERSION:
        # Refused rather than guessed at, for the reason `_parse` refuses a
        # manifest: the field names may not mean the same thing.
        raise ManifestError(
            f"{source} has export schema version {version}, "
            f"this build understands {EXPORT_SCHEMA_VERSION}")

    try:
        return BackupExport.model_validate(data)
    except ValueError as e:
        raise ManifestError(f"{source} is malformed: {e}") from e


# ---------------------------------------------------------------------------
# Group backup manifest
#
# The self-describing part of a CONSISTENCY-GROUP backup: one JSON document per
# group backup, under a "groups/" prefix, naming the N member backups of one
# generation as a unit. It is written LAST, after every member manifest exists,
# so its presence is the group backup's all-or-nothing commit barrier
# (design-consistency-group-backup.md §3.3, §4.2). A reader resolves a group
# backup from this document, then reads each member's manifests/<backup_id>.json
# to assemble that member's chain. Like the per-backup manifest, it records
# identity and encoding, never the bucket, region, endpoint, or credentials.
# ---------------------------------------------------------------------------

GROUP_MANIFEST_PREFIX = "groups/"
GROUP_MANIFEST_SCHEMA_VERSION = 1


def group_manifest_key(group_backup_id: UUID) -> str:
    return f"{GROUP_MANIFEST_PREFIX}{group_backup_id}.json"


class GroupManifestSource(BaseModel):
    """Where a group backup's generation came from. Provenance for a reader."""
    model_config = ConfigDict(extra="forbid")

    cluster_id: UUID
    #: ``ConsistencyGroup.get_id()``: the composite "<cluster_id>/<uuid>".
    consistency_group_id: str
    consistency_group_name: str
    #: The generation (``group_seq``) this backup was taken from.
    generation: int
    #: Absent when the cluster's own record of its name was unreadable.
    cluster_name: str | None = None


class GroupManifestMember(BaseModel):
    """One member of a group backup: the volume and the backup that copied it."""
    model_config = ConfigDict(extra="forbid")

    lvol_id: UUID
    lvol_name: str
    #: The per-member backup uuid; its own manifest is ``manifests/<backup_id>.json``.
    backup_id: UUID
    s3_id: int


class GroupManifestMembership(BaseModel):
    """The generation's membership, so a reader can verify completeness.

    A group backup is restorable only when every expected member is present
    (design-consistency-group-backup.md §6). Recording the expected ids here is
    the expected-versus-present check (design-consistency-groups.md §8.3) encoded
    in the bucket, so a reader with only the bucket can tell a complete group
    backup from a torn one.
    """
    model_config = ConfigDict(extra="forbid")

    expected_lvol_ids: list[UUID]


class GroupBackupManifest(BaseModel):
    """The self-describing document for one consistency-group backup."""
    model_config = ConfigDict(extra="forbid")

    schema_version: int = GROUP_MANIFEST_SCHEMA_VERSION
    group_backup_id: UUID
    created_at: int
    completed_at: int
    size: int

    source: GroupManifestSource
    members: list[GroupManifestMember]
    membership: GroupManifestMembership

    #: Whether the members are encrypted. Homogeneous (design §3.4): every member
    #: shares this disposition. The actual per-member keys are NOT here -- each
    #: member has its own DEK, recorded in its own manifest
    #: (``manifests/<backup_id>.json``) -- so this is only the group-level
    #: statement that a reader must resolve per-member keys before a restore.
    encrypted: bool = False

    def is_complete(self) -> bool:
        """Whether every expected member is present in this manifest."""
        present = {member.lvol_id for member in self.members}
        return set(self.membership.expected_lvol_ids) <= present


def write_group(config: BackupConfig, manifest: GroupBackupManifest) -> None:
    """Store a group manifest, next to the member backups it names.

    Written LAST, after every member manifest exists: its presence is the group
    backup's commit barrier (design §3.3).

    Raises:
        ManifestError: it could not be stored. A group backup with member
            manifests but no group manifest is not a committed group backup.
    """
    try:
        s3_client(config).put_object(
            Bucket=config.bucket_name,
            Key=group_manifest_key(manifest.group_backup_id),
            Body=manifest.model_dump_json().encode(),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            "Failed to write group manifest for group backup "
            f"{manifest.group_backup_id}") from e

    logger.info("Wrote group manifest for group backup %s to %s",
                manifest.group_backup_id, config.bucket_name)


def read_group(config: BackupConfig, group_backup_id: UUID) -> GroupBackupManifest:
    """Load one group manifest by group backup id.

    Raises:
        ManifestError: it is absent, unreadable, or a schema version this build
            does not understand.
    """
    try:
        body = s3_client(config).get_object(
            Bucket=config.bucket_name,
            Key=group_manifest_key(group_backup_id))["Body"].read()
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            f"Failed to read group manifest for group backup {group_backup_id} "
            f"from {config.bucket_name}") from e

    return _parse_group(body, group_manifest_key(group_backup_id))


def list_all_groups(config: BackupConfig) -> list[GroupBackupManifest]:
    """Every group manifest in the bucket, newest first.

    The disaster-recovery entry point for group backups, parallel to
    :func:`list_all`.

    Raises:
        ManifestError: the bucket could not be listed, or one group manifest
            could not be parsed. Not best-effort, for :func:`list_all`'s reason.
    """
    client = s3_client(config)
    manifests = []

    try:
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=config.bucket_name, Prefix=GROUP_MANIFEST_PREFIX)
        for page in pages:
            for entry in page.get("Contents", []):
                key = entry["Key"]
                if not key.endswith(".json"):
                    continue
                body = client.get_object(Bucket=config.bucket_name, Key=key)["Body"].read()
                manifests.append(_parse_group(body, key))
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            f"Failed to list group manifests in {config.bucket_name}") from e

    manifests.sort(key=lambda m: (m.created_at, m.group_backup_id), reverse=True)
    return manifests


def delete_group(config: BackupConfig, group_backup_id: UUID) -> None:
    """Remove a group manifest.

    Deleting it un-commits the group backup as a unit; the member manifests and
    objects follow retention per member (design §9).
    """
    try:
        s3_client(config).delete_object(
            Bucket=config.bucket_name,
            Key=group_manifest_key(group_backup_id))
    except (BotoCoreError, ClientError) as e:
        raise ManifestError(
            "Failed to delete group manifest for group backup "
            f"{group_backup_id}") from e


def _parse_group(body: bytes, key: str) -> GroupBackupManifest:
    try:
        data = json.loads(body)
    except ValueError as e:
        raise ManifestError(f"Group manifest {key} is not valid JSON") from e

    version = data.get("schema_version") if isinstance(data, dict) else None
    if version != GROUP_MANIFEST_SCHEMA_VERSION:
        # Refuse rather than guess, for the reason `_parse` refuses a manifest.
        raise ManifestError(
            f"Group manifest {key} has schema version {version}, "
            f"this build understands {GROUP_MANIFEST_SCHEMA_VERSION}")

    try:
        return GroupBackupManifest.model_validate(data)
    except ValueError as e:
        raise ManifestError(f"Group manifest {key} is malformed: {e}") from e


GROUP_EXPORT_SCHEMA_VERSION = 1


class GroupBackupExport(BaseModel):
    """A group backup carried out of a cluster in a file: the group manifest plus
    the member backup manifests it names, as one unit (design §6.5).

    This is to the group manifest what ``BackupExport`` is to a single-volume
    manifest -- the one document that does record a location, because it is not
    stored in the bucket it describes. The group manifest names the generation's
    members; ``members`` carries each member's whole restorable chain, grouped by
    the location it lives in exactly as a single-volume export groups its
    manifests. A group backup is homogeneous -- its members share one location
    (§3.4) -- so there is one group in practice, but reusing ``BackupExport`` keeps
    the member records on the single-volume import path and describes a member's
    chain the same way everywhere.
    """
    model_config = ConfigDict(extra="forbid")

    schema_version: int = GROUP_EXPORT_SCHEMA_VERSION
    group_manifest: GroupBackupManifest
    members: BackupExport


def parse_group_export(body: bytes, source: str) -> GroupBackupExport:
    """Read a group export document, naming *source* in anything it rejects.

    The group analog of :func:`parse_export`.

    Raises:
        ManifestError: It is not valid JSON, states a schema version this build
            does not understand, or is not shaped like a group export.
    """
    try:
        data = json.loads(body)
    except ValueError as e:
        raise ManifestError(f"{source} is not valid JSON") from e

    version = data.get("schema_version") if isinstance(data, dict) else None
    if version != GROUP_EXPORT_SCHEMA_VERSION:
        # Refused rather than guessed at, for the reason `_parse` refuses a
        # manifest: the field names may not mean the same thing.
        raise ManifestError(
            f"{source} has group export schema version {version}, "
            f"this build understands {GROUP_EXPORT_SCHEMA_VERSION}")

    try:
        return GroupBackupExport.model_validate(data)
    except ValueError as e:
        raise ManifestError(f"{source} is malformed: {e}") from e
