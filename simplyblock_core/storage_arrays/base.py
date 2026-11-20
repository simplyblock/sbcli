"""Vendor-neutral model and adapter contract for external storage arrays.

An adapter translates five operations -- volume create/resize/delete, host
(initiator group) management and volume-to-host mapping -- into one vendor's
management API. Everything vendor-neutral (credential resolution,
re-authentication, size rounding, idempotence, delete/resize safety checks)
lives in :class:`simplyblock_core.storage_arrays.client.ArrayClient`, so
adapters stay thin and are tested against one shared contract suite
(``tests/unit/storage_arrays/test_contract.py``).
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from simplyblock_core.credentials import Credential

# -- Errors -----------------------------------------------------------------


class ArrayError(Exception):
    """Base class. Messages never contain credential values."""


class ArrayAuthError(ArrayError):
    """The array rejected the credential or the session expired."""


class ArrayNotFoundError(ArrayError):
    """The addressed object does not exist."""


class ArrayConflictError(ArrayError):
    """The request conflicts with array state (name taken, initiator owned
    by another host, volume still mapped, ...)."""


class ArrayUnsupportedError(ArrayError):
    """The adapter or array does not support the requested operation."""


class ArrayRequestError(ArrayError):
    """Any other failed request."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


# -- Model ------------------------------------------------------------------


class Protocol(str, Enum):
    ISCSI = "iscsi"
    FC = "fc"
    NVME_TCP = "nvme-tcp"
    NVME_RDMA = "nvme-rdma"
    NVME_FC = "nvme-fc"
    #: Dell PowerFlex native client (SDC); the initiator address is the SDC GUID.
    SDC = "sdc"

    @property
    def is_nvme(self) -> bool:
        return self in (Protocol.NVME_TCP, Protocol.NVME_RDMA, Protocol.NVME_FC)

    @property
    def is_scsi(self) -> bool:
        return self in (Protocol.ISCSI, Protocol.FC)


_WWPN_RE = re.compile(r"^[0-9a-f]{16}$")


def normalize_wwpn(value: str) -> str:
    """``21:00:00:24:FF:4C:1A:2B`` / ``0x2100...`` -> ``21000024ff4c1a2b``."""
    v = value.strip().lower().removeprefix("0x").replace(":", "").replace("-", "")
    if not _WWPN_RE.match(v):
        raise ValueError(f"Not a WWPN: {value!r}")
    return v


def colon_wwpn(value: str) -> str:
    """``21000024ff4c1a2b`` -> ``21:00:00:24:ff:4c:1a:2b``."""
    v = normalize_wwpn(value)
    return ":".join(v[i:i + 2] for i in range(0, 16, 2))


class Initiator(BaseModel):
    """One host port: an iSCSI IQN, an FC WWPN, an NVMe host NQN or an SDC GUID."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    protocol: Protocol
    address: str = Field(min_length=1)

    @field_validator("address")
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()

    def normalized(self) -> str:
        if self.protocol in (Protocol.FC, Protocol.NVME_FC):
            return normalize_wwpn(self.address)
        if self.protocol == Protocol.ISCSI:
            return self.address.lower()
        return self.address


class HostSpec(BaseModel):
    """A host as the array should know it. All initiators share one protocol
    family (SCSI or NVMe); a host using both is registered twice."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1, max_length=255)
    initiators: tuple[Initiator, ...] = Field(min_length=1)

    @field_validator("initiators")
    @classmethod
    def _one_family(cls, v: tuple[Initiator, ...]) -> tuple[Initiator, ...]:
        protocols = {i.protocol for i in v}
        if len(protocols) > 1 and not (
            all(p.is_scsi for p in protocols) or all(p.is_nvme for p in protocols)
        ):
            raise ValueError("A host's initiators must all be SCSI or all be NVMe")
        return v

    @property
    def protocols(self) -> frozenset[Protocol]:
        return frozenset(i.protocol for i in self.initiators)

    def addresses(self, *protocols: Protocol) -> list[str]:
        return [i.normalized() for i in self.initiators if i.protocol in protocols]


class Volume(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    #: The array's own handle for the volume (id, uuid, device number, name).
    id: str
    name: str
    size_bytes: int
    #: SCSI identity as the host sees it, ``naa.<hex>`` (lowercase). This is
    #: what matches the array volume to ``/dev/disk/by-id`` on the host.
    wwn: str | None = None
    #: NVMe identity, ``eui.<32 hex>`` (NGUID) where the array exposes one.
    nguid: str | None = None

    @property
    def identifier(self) -> str:
        """Stable identity used for safety checks: WWN, else NGUID, else id."""
        return self.wwn or self.nguid or f"id.{self.id}"


class Mapping(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    volume_id: str
    host_id: str
    #: SCSI LUN or NVMe namespace ID as presented to the host; ``None`` where
    #: the protocol has no such number (PowerFlex SDC).
    lun: int | None = None


class ArrayInfo(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    vendor: str
    model: str = ""
    version: str = ""
    serial: str = ""


class ArrayConfig(BaseModel):
    """One registered array. Holds no secret: ``credential_ref`` is resolved
    through :class:`simplyblock_core.credentials.CredentialProvider`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: str = Field(min_length=1)
    vendor: str = Field(min_length=1)
    #: Management endpoint, e.g. ``https://pure01.example.com`` or
    #: ``https://unisphere.example.com:8443``. Path prefixes are kept.
    endpoint: str = Field(min_length=1)
    credential_ref: dict[str, Any]
    #: PEM CA bundle (or pinned self-signed certificate) for the endpoint.
    ca_bundle: str | None = None
    #: Must stay True outside of labs; ``False`` disables TLS verification.
    verify_tls: bool = True
    timeout_s: float = Field(default=60.0, gt=0)
    #: Vendor-specific settings (pool, SVM, CPG, storage group, ...); each
    #: adapter documents what it reads in ``option_docs``.
    options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("endpoint")
    @classmethod
    def _https_only(cls, v: str) -> str:
        v = v.strip().rstrip("/")
        if not v.lower().startswith("https://"):
            raise ValueError("Array endpoint must be https://")
        return v


@dataclass(frozen=True)
class Capabilities:
    protocols: frozenset[Protocol]
    #: Volume sizes are rounded up to a multiple of this.
    size_granularity_bytes: int = 512
    min_size_bytes: int = 1 << 20
    #: Longest volume / host name the array accepts.
    max_name_len: int = 63
    #: Whether resize can only grow (true for every supported array today).
    grow_only: bool = True


@dataclass
class OptionDoc:
    name: str
    help: str
    required: bool = False
    default: Any = None


@dataclass
class AdapterContext:
    """What an adapter is constructed with."""

    config: ArrayConfig
    credential: Credential
    session: Any  # simplyblock_core.storage_arrays.http.RestSession
    extra: dict[str, Any] = field(default_factory=dict)


class ArrayAdapter(ABC):
    """One vendor's implementation of the contract.

    Rules every adapter follows (the contract suite checks them):

    * ``create_volume`` receives a name that is already unique and short
      enough, and a size already rounded to ``capabilities``.
    * ``get_volume``/``find_volume`` return ``None`` for a missing volume;
      every other method raises :class:`ArrayNotFoundError` for one.
    * ``ensure_host`` is idempotent: an existing host of that name gets any
      missing initiators added; an initiator registered to a *different* host
      raises :class:`ArrayConflictError`.
    * ``map_volume`` is idempotent and returns the existing mapping if there is
      one; ``unmap_volume`` of an absent mapping is a no-op.
    * ``delete_volume`` is only called with no mappings left.
    * Errors never include credential values.
    """

    vendor: ClassVar[str]
    display_name: ClassVar[str]
    capabilities: ClassVar[Capabilities]
    #: Accepted credential shapes, as alternatives of required field names,
    #: e.g. ``({"api_token"}, {"username", "password"})``.
    credential_fields: ClassVar[tuple[frozenset[str], ...]] = (
        frozenset({"username", "password"}),
    )
    option_docs: ClassVar[tuple[OptionDoc, ...]] = ()

    def __init__(self, ctx: AdapterContext):
        self.config = ctx.config
        self.credential = ctx.credential
        self.session = ctx.session
        self.options: MappingABC[str, Any] = ctx.config.options
        self._check_credential_shape()
        self._check_options()

    # -- helpers ------------------------------------------------------------

    def _check_credential_shape(self) -> None:
        have = self.credential.field_names
        if not any(shape <= have for shape in self.credential_fields):
            wanted = " or ".join("+".join(sorted(s)) for s in self.credential_fields)
            raise ArrayAuthError(
                f"{self.display_name} needs credential fields {wanted}; "
                f"got {', '.join(sorted(have)) or 'none'}")

    def _check_options(self) -> None:
        missing = [o.name for o in self.option_docs if o.required and o.name not in self.options]
        if missing:
            raise ArrayError(f"{self.display_name} array {self.config.id} needs options: "
                             + ", ".join(missing))
        known = {o.name for o in self.option_docs}
        unknown = sorted(set(self.options) - known)
        if unknown:
            raise ArrayError(f"Unknown {self.display_name} options: {', '.join(unknown)}")

    def option(self, name: str) -> Any:
        if name in self.options:
            return self.options[name]
        for o in self.option_docs:
            if o.name == name:
                return o.default
        raise KeyError(name)

    def secret(self, name: str) -> str:
        return self.credential.get(name).get_secret_value()

    def has_secret(self, name: str) -> bool:
        return name in self.credential.field_names

    # -- lifecycle ----------------------------------------------------------

    def login(self) -> None:
        """Establish a session. Called lazily by the client and again after
        an :class:`ArrayAuthError`. Basic-auth adapters may leave this empty."""

    def close(self) -> None:
        """Release the session (best effort, never raises)."""

    # -- contract -----------------------------------------------------------

    @abstractmethod
    def get_info(self) -> ArrayInfo: ...

    @abstractmethod
    def create_volume(self, name: str, size_bytes: int) -> Volume: ...

    @abstractmethod
    def get_volume(self, volume_id: str) -> Volume | None: ...

    @abstractmethod
    def find_volume(self, name: str) -> Volume | None: ...

    @abstractmethod
    def resize_volume(self, volume_id: str, size_bytes: int) -> Volume: ...

    @abstractmethod
    def delete_volume(self, volume_id: str) -> None: ...

    @abstractmethod
    def ensure_host(self, spec: HostSpec) -> str:
        """Return the host id."""

    @abstractmethod
    def delete_host(self, host_id: str) -> None:
        """Delete a host that has no mappings left (no-op if absent)."""

    @abstractmethod
    def map_volume(self, volume_id: str, host_id: str) -> Mapping: ...

    @abstractmethod
    def unmap_volume(self, volume_id: str, host_id: str) -> None: ...

    @abstractmethod
    def list_mappings(self, volume_id: str) -> list[Mapping]: ...
