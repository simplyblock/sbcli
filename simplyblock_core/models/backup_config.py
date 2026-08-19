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

Absence is expressed as ``None`` rather than as ``""`` or ``0``, so no caller has
to guess whether a field was configured or merely left at its default. Where the
AWS SDK has its own resolution chain -- credentials, region, endpoint -- absent
means "let it resolve", which is a real configuration rather than a gap.

Both models serialize straight into the untyped ``dict`` fields the FoundationDB
records still use: a plain ``model_dump()`` is JSON-safe, while ``SecretStr``
stays wrapped so the plaintext is produced only by ``BaseModel.write_to_db``'s
own ``unwrap_secrets`` pass, at the last possible moment.
"""
from enum import IntEnum
from typing import Any, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    field_serializer,
    model_validator,
)


class SecondaryTarget(IntEnum):
    """The kind of secondary store, numbered as the data plane's RPC expects."""

    S3 = 0
    FILESYSTEM = 1


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

    bucket_name: str = Field(min_length=1)

    #: Absent means the AWS SDK resolves it the way it resolves credentials:
    #: from the environment, the profile, or instance metadata. Recording it is
    #: better -- a manifest that names its region can be read from anywhere,
    #: while one that does not depends on the reader's environment agreeing --
    #: but it is not required, because it is recoverable: bucket names are
    #: globally unique, so S3 can be asked where a bucket lives, and every layer
    #: below already treats an absent region this way (boto3's own resolution,
    #: and `if (region && *region)` in the data plane's init_client).
    region: Optional[str] = Field(default=None, min_length=1)

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

    @field_serializer("endpoint", when_used="unless-none")
    def _serialize_endpoint(self, endpoint: HttpUrl) -> str:
        return self.endpoint_url  # type: ignore[return-value]

    @field_serializer("secondary_target")
    def _serialize_secondary_target(self, target: SecondaryTarget) -> int:
        return int(target)

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

        # "" was how the untyped dict said "unset"; the model says None. Dropped
        # before the defaults below, so `local_testing` can still fill a region
        # that was stored as empty rather than left out.
        for key in ("region", "endpoint"):
            if data.get(key) == "":
                del data[key]

        # `local_testing` bundled three separate decisions into one flag. It set
        # plain HTTP, disabled certificate verification, forced path-style
        # addressing and hardcoded us-east-1 (bdev_s3_impl.hpp init_client).
        # Unpack it into the properties it actually stood for.
        if data.pop("local_testing", False):
            data.setdefault("verify_tls", False)
            data.setdefault("use_path_style", True)
            data.setdefault("region", "us-east-1")

        # 0 meant "let the data plane pick"; that is now an absent value.
        if data.get("s3_thread_pool_size") == 0:
            del data["s3_thread_pool_size"]

        return data


class BackupConfig(BackupLocation):
    """A cluster's backup configuration: a location plus how to authenticate to it."""

    #: Absent means the node's own IAM role / the AWS default provider chain.
    credentials: Optional[S3Credentials] = None

    #: Absent means the data plane's own default (32 at the time of writing).
    s3_thread_pool_size: Optional[int] = Field(default=None, ge=1)

    def location(self) -> BackupLocation:
        return BackupLocation.model_validate(
            self.model_dump(include=set(BackupLocation.model_fields))
        )
