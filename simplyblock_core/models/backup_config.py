# coding=utf-8
"""Typed configuration describing where backup objects live and how to read them.

Two models, deliberately split so that "backups never carry credentials" is
enforced by the type system rather than by discipline:

``BackupLocation``
    Everything needed to *find and interpret* a backup's objects. Safe to embed
    in a backup record and in the S3 manifest. Cannot represent a secret.

``BackupConfig``
    What a cluster is configured with: a location plus the credentials and
    node-local tuning needed to act on it. Never leaves the control plane.

Neither model uses sentinel values. A field is mandatory, a boolean with a
meaningful default, or ``Optional`` where absence is a real state -- so no
caller has to compare against ``""`` or ``0`` to find out whether something was
configured.
"""
from enum import Enum
from typing import Annotated, Any, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    StringConstraints,
    model_validator,
)


NonEmptyStr = Annotated[str, StringConstraints(min_length=1, strip_whitespace=True)]
ThreadPoolSize = Annotated[int, Field(ge=1)]


class SecondaryTarget(str, Enum):
    """Which kind of secondary store the objects live in.

    Named rather than numeric because it is written into manifests that get read
    back years later. The data plane still wants the integer, hence ``wire_value``.

    ``(str, Enum)`` rather than ``StrEnum`` because tox pins ``basepython =
    python3.9``.
    """

    S3 = "s3"
    FILESYSTEM = "filesystem"

    @property
    def wire_value(self) -> int:
        return _SECONDARY_TARGET_WIRE[self]

    @classmethod
    def from_wire(cls, value: int) -> "SecondaryTarget":
        for target, wire in _SECONDARY_TARGET_WIRE.items():
            if wire == value:
                return target
        raise ValueError(f"Unknown secondary_target: {value}")


_SECONDARY_TARGET_WIRE = {
    SecondaryTarget.S3: 0,
    SecondaryTarget.FILESYSTEM: 1,
}


class S3Credentials(BaseModel):
    """A static key pair.

    A pair rather than two independent fields, so "access key set, secret
    missing" is unrepresentable instead of something a validator has to catch.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    access_key_id: SecretStr
    secret_access_key: SecretStr


class BackupLocation(BaseModel):
    """Where a backup's objects are, and how to interpret them. Never secret.

    Every field here affects whether the objects can be read back at all, which
    is why the whole model is embedded in each backup rather than looked up from
    the cluster that happened to create it.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    bucket_name: NonEmptyStr
    region: NonEmptyStr

    #: Absent means the AWS SDK resolves the endpoint from the region.
    endpoint: Optional[HttpUrl] = None

    secondary_target: SecondaryTarget = SecondaryTarget.S3
    with_compression: bool = False

    #: Selects the object key layout. ``True`` gives the backup layout
    #: ``{s3_id}/{mid}/{extent}``; ``False`` gives the secondary-tiering layout
    #: ``{tiering_id}/{lpgi}``, which cannot hold backups.
    snapshot_backups: bool = True

    verify_tls: bool = True
    use_path_style: bool = False

    @property
    def endpoint_url(self) -> Optional[str]:
        """The endpoint as the AWS SDK and boto3 want it, without a trailing slash."""
        return str(self.endpoint).rstrip("/") if self.endpoint is not None else None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_keys(cls, data: Any) -> Any:
        """Accept the untyped ``Cluster.backup_config`` dicts written before this model.

        Existing clusters and ``tests/perf/backup_config.json`` carry the shape
        the data plane's RPC used to take directly. Mapping it here means no FDB
        migration is needed.
        """
        if not isinstance(data, dict):
            return data

        data = dict(data)

        if (endpoint := data.pop("local_endpoint", None)) and "endpoint" not in data:
            data["endpoint"] = endpoint

        access_key_id = data.pop("access_key_id", None)
        secret_access_key = data.pop("secret_access_key", None)
        if access_key_id and secret_access_key and "credentials" not in data:
            data["credentials"] = {
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
            }

        # `local_testing` bundled three separate decisions into one flag. It set
        # plain HTTP, disabled certificate verification, forced path-style
        # addressing and hardcoded us-east-1 (bdev_s3_impl.hpp init_client).
        # Unpack it into the properties it actually stood for.
        if data.pop("local_testing", False):
            data.setdefault("verify_tls", False)
            data.setdefault("use_path_style", True)
            data.setdefault("region", "us-east-1")

        if isinstance(target := data.get("secondary_target"), int):
            data["secondary_target"] = SecondaryTarget.from_wire(target)

        # 0 meant "let the data plane pick"; that is now an absent value.
        if data.get("s3_thread_pool_size") == 0:
            del data["s3_thread_pool_size"]

        return data


class BackupConfig(BackupLocation):
    """A cluster's backup configuration: a location plus how to authenticate to it."""

    #: Absent means the node's own IAM role / the AWS default provider chain.
    credentials: Optional[S3Credentials] = None

    #: Absent means the data plane's own default (32 at the time of writing).
    s3_thread_pool_size: Optional[ThreadPoolSize] = None

    #: When set, each encrypted backup wraps its data encryption keys under a
    #: key derived from this secret, making the backup recoverable without the
    #: originating cluster's KMS. Absent means encrypted backups depend on that
    #: KMS remaining reachable.
    key_wrapping_secret: Optional[SecretStr] = None

    def location(self) -> BackupLocation:
        return BackupLocation.model_validate(
            self.model_dump(include=set(BackupLocation.model_fields))
        )

    def to_storage_dict(self) -> dict:
        """A dict for ``Cluster.backup_config``: JSON-safe, but secrets still wrapped.

        Not ``model_dump(mode="json")`` -- that renders ``SecretStr`` as
        ``**********`` and would silently destroy the credentials on write.
        Python-mode dump keeps the wrappers, so ``BaseModel.write_to_db``'s
        ``unwrap_secrets=True`` pass still produces plaintext at the last moment
        while every log line in between stays masked. Only the two values that
        python mode leaves non-JSON-serializable are converted here.
        """
        data = self.model_dump(exclude_none=True)
        if self.endpoint is not None:
            data["endpoint"] = self.endpoint_url
        data["secondary_target"] = self.secondary_target.value
        return data
