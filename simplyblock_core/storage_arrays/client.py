"""Vendor-neutral front end for array adapters.

:class:`ArrayClient` is what the rest of simplyblock calls. It

* resolves the array's credential through the cluster's
  :class:`~simplyblock_core.credentials.CredentialProvider`, bound to the
  array's id and endpoint (so a rewritten endpoint cannot receive it);
* builds the adapter lazily and, when the array rejects the session or the
  credential, invalidates the cached credential, logs in again and retries
  the operation once (this is how rotated secrets are picked up);
* names volumes ``sb-<hex>`` from the simplyblock volume UUID, so names are
  unique, fit every array's length limit, and mark ownership;
* rounds sizes up to the array's granularity;
* makes create/map/unmap/delete idempotent so an interrupted task can be
  re-run;
* refuses to resize, unmap or delete a volume that is not simplyblock-owned
  (name prefix) or whose WWN/NGUID differs from the one simplyblock recorded.
"""
from __future__ import annotations

import logging
import threading
import uuid as uuid_mod
from collections.abc import Callable
from typing import Any, Self, TypeVar
from urllib.parse import urlsplit

from requests.adapters import BaseAdapter

from simplyblock_core.credentials import (
    CREDENTIAL_REF_ADAPTER,
    Credential,
    CredentialProvider,
)

from .base import (
    AdapterContext,
    ArrayAdapter,
    ArrayAuthError,
    ArrayConfig,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    HostSpec,
    Mapping,
    Volume,
)
from .http import RestSession
from .registry import adapter_class, canonical_vendor

logger = logging.getLogger(__name__)

MANAGED_PREFIX = "sb-"
_NAME_HEX_LEN = 24  # 96 bits of the volume UUID: ample, and fits 31-char limits

T = TypeVar("T")


def credential_context(config: ArrayConfig) -> str:
    """The context an array's credential is bound to: id + scheme/host/port."""
    parts = urlsplit(config.endpoint)
    port = parts.port or 443
    return f"array/{config.id}/{parts.scheme}://{(parts.hostname or '').lower()}:{port}"


def managed_name(volume_uuid: str, max_len: int, prefix: str = MANAGED_PREFIX) -> str:
    hex_id = uuid_mod.UUID(str(volume_uuid)).hex
    return (prefix + hex_id[:_NAME_HEX_LEN])[:max_len]


def round_size(size_bytes: int, granularity: int, minimum: int) -> int:
    size = max(int(size_bytes), minimum)
    return -(-size // granularity) * granularity


class ArrayClient:
    """Thread-safe: operations on one array are serialized."""

    def __init__(
        self,
        config: ArrayConfig,
        credentials: CredentialProvider,
        *,
        transport: BaseAdapter | None = None,
        session_kwargs: dict[str, Any] | None = None,
    ):
        self.config = config
        self.vendor = canonical_vendor(config.vendor)
        self.adapter_cls = adapter_class(self.vendor)
        self._credentials = credentials
        self._ref = CREDENTIAL_REF_ADAPTER.validate_python(config.credential_ref)
        self._context = credential_context(config)
        self._transport = transport
        self._session_kwargs = session_kwargs or {}
        self._adapter: ArrayAdapter | None = None
        self._lock = threading.RLock()

    @property
    def capabilities(self):
        return self.adapter_cls.capabilities

    # -- adapter lifecycle ----------------------------------------------------

    def _build(self) -> ArrayAdapter:
        credential: Credential = self._credentials.resolve(self._ref, self._context)
        session = RestSession.for_config(self.config, transport=self._transport,
                                         **self._session_kwargs)
        adapter = self.adapter_cls(AdapterContext(self.config, credential, session))
        try:
            adapter.login()
        except BaseException:
            adapter.close()
            session.close()
            raise
        return adapter

    def _drop(self) -> None:
        if self._adapter is not None:
            try:
                self._adapter.close()
            except Exception:  # pragma: no cover - best effort
                logger.debug("adapter close failed", exc_info=True)
            try:
                self._adapter.session.close()
            except Exception:  # pragma: no cover
                pass
            self._adapter = None

    def close(self) -> None:
        with self._lock:
            self._drop()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _call(self, op: str, fn: Callable[[ArrayAdapter], T]) -> T:
        with self._lock:
            for attempt in (1, 2):
                try:
                    if self._adapter is None:
                        self._adapter = self._build()
                    return fn(self._adapter)
                except ArrayAuthError:
                    self._drop()
                    if attempt == 2:
                        raise
                    logger.info("Array %s rejected the session during %s; re-authenticating",
                                self.config.id, op)
                    self._credentials.invalidate(self._ref)
            raise AssertionError("unreachable")  # pragma: no cover

    # -- checks -----------------------------------------------------------------

    def _owned(self, vol: Volume, expected_identifier: str | None, action: str) -> None:
        if not vol.name.startswith(MANAGED_PREFIX):
            raise ArrayConflictError(
                f"Refusing to {action} volume {vol.name!r} on array {self.config.id}: "
                "not created by simplyblock")
        if expected_identifier is not None and vol.identifier.lower() != expected_identifier.lower():
            raise ArrayConflictError(
                f"Refusing to {action} volume {vol.name!r} on array {self.config.id}: "
                f"identifier {vol.identifier} does not match recorded {expected_identifier}")

    # -- operations -------------------------------------------------------------

    def get_info(self) -> ArrayInfo:
        return self._call("get_info", lambda a: a.get_info())

    def volume_name(self, volume_uuid: str) -> str:
        return managed_name(volume_uuid, self.capabilities.max_name_len)

    def create_volume(self, volume_uuid: str, size_bytes: int) -> Volume:
        """Create (or, on re-run, return) the array volume for a simplyblock
        volume. An existing volume of that name smaller than requested is grown."""
        name = self.volume_name(volume_uuid)
        caps = self.capabilities
        size = round_size(size_bytes, caps.size_granularity_bytes, caps.min_size_bytes)

        def op(a: ArrayAdapter) -> Volume:
            existing = a.find_volume(name)
            if existing is not None:
                if existing.size_bytes < size:
                    return a.resize_volume(existing.id, size)
                return existing
            vol = a.create_volume(name, size)
            logger.info("Array %s: created volume %s (%s, %d bytes)",
                        self.config.id, vol.name, vol.identifier, vol.size_bytes)
            return vol

        return self._call("create_volume", op)

    def get_volume(self, volume_id: str) -> Volume | None:
        return self._call("get_volume", lambda a: a.get_volume(volume_id))

    def find_volume(self, volume_uuid: str) -> Volume | None:
        name = self.volume_name(volume_uuid)
        return self._call("find_volume", lambda a: a.find_volume(name))

    def resize_volume(self, volume_id: str, size_bytes: int,
                      expected_identifier: str | None = None) -> Volume:
        caps = self.capabilities
        size = round_size(size_bytes, caps.size_granularity_bytes, caps.min_size_bytes)

        def op(a: ArrayAdapter) -> Volume:
            vol = a.get_volume(volume_id)
            if vol is None:
                raise ArrayError(f"Volume {volume_id} not found on array {self.config.id}")
            self._owned(vol, expected_identifier, "resize")
            if size == vol.size_bytes:
                return vol
            if size < vol.size_bytes and caps.grow_only:
                raise ArrayConflictError(
                    f"Refusing to shrink volume {vol.name} from {vol.size_bytes} to {size} bytes")
            new = a.resize_volume(vol.id, size)
            logger.info("Array %s: resized volume %s %d -> %d bytes",
                        self.config.id, vol.name, vol.size_bytes, new.size_bytes)
            return new

        return self._call("resize_volume", op)

    def delete_volume(self, volume_id: str, expected_identifier: str | None = None) -> bool:
        """Unmap from every host and delete. Returns False if already gone."""
        def op(a: ArrayAdapter) -> bool:
            vol = a.get_volume(volume_id)
            if vol is None:
                return False
            self._owned(vol, expected_identifier, "delete")
            for m in a.list_mappings(vol.id):
                a.unmap_volume(vol.id, m.host_id)
            a.delete_volume(vol.id)
            logger.info("Array %s: deleted volume %s (%s)", self.config.id, vol.name, vol.identifier)
            return True

        return self._call("delete_volume", op)

    def ensure_host(self, spec: HostSpec) -> str:
        unsupported = spec.protocols - self.capabilities.protocols
        if unsupported:
            raise ArrayError(
                f"{self.adapter_cls.display_name} adapter does not support "
                + ", ".join(sorted(p.value for p in unsupported)))
        if len(spec.name) > self.capabilities.max_name_len:
            raise ArrayError(f"Host name {spec.name!r} exceeds "
                             f"{self.capabilities.max_name_len} characters")
        return self._call("ensure_host", lambda a: a.ensure_host(spec))

    def delete_host(self, host_id: str) -> None:
        self._call("delete_host", lambda a: a.delete_host(host_id))

    def map_volume(self, volume_id: str, host_id: str,
                   expected_identifier: str | None = None) -> Mapping:
        def op(a: ArrayAdapter) -> Mapping:
            vol = a.get_volume(volume_id)
            if vol is None:
                raise ArrayError(f"Volume {volume_id} not found on array {self.config.id}")
            self._owned(vol, expected_identifier, "map")
            return a.map_volume(vol.id, host_id)

        return self._call("map_volume", op)

    def unmap_volume(self, volume_id: str, host_id: str) -> None:
        self._call("unmap_volume", lambda a: a.unmap_volume(volume_id, host_id))

    def list_mappings(self, volume_id: str) -> list[Mapping]:
        return self._call("list_mappings", lambda a: a.list_mappings(volume_id))
