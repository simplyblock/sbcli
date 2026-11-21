"""External storage arrays: vendor adapters behind one contract.

Entry point: :class:`simplyblock_core.storage_arrays.client.ArrayClient`.
"""
from .base import (
    ArrayAdapter,
    ArrayAuthError,
    ArrayConfig,
    ArrayConflictError,
    ArrayError,
    ArrayInfo,
    ArrayNotFoundError,
    ArrayRequestError,
    ArrayUnsupportedError,
    Capabilities,
    HostSpec,
    Initiator,
    Mapping,
    Protocol,
    Volume,
)
from .registry import ADAPTERS, ALIASES, adapter_class, canonical_vendor

__all__ = [
    "ADAPTERS",
    "ALIASES",
    "ArrayAdapter",
    "ArrayAuthError",
    "ArrayConfig",
    "ArrayConflictError",
    "ArrayError",
    "ArrayInfo",
    "ArrayNotFoundError",
    "ArrayRequestError",
    "ArrayUnsupportedError",
    "Capabilities",
    "HostSpec",
    "Initiator",
    "Mapping",
    "Protocol",
    "Volume",
    "adapter_class",
    "canonical_vendor",
]
