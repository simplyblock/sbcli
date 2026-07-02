# coding=utf-8
"""Change-counter keys for entity watches.

Every write/remove of a watched model class atomically bumps a per-class
counter key (``watch_seq/<ClassName>``) in the same FoundationDB transaction
as the entity mutation. Watchers (the web API's SSE layer) set FDB watches on
these counter keys to learn that *something* of that class changed, then
re-read the entity range and diff.

The ``watch_seq/`` namespace is disjoint from all entity range scans:
``BaseModel.read_from_db`` only scans ``<object_type>/<ClassName>/`` prefixes.

Stdlib-only leaf module — importable from models without cycles.
"""

import struct

WATCH_SEQ_PREFIX = 'watch_seq/'

# Operand for FDB's atomic ADD mutation: little-endian 64-bit integer 1.
ONE_LE64 = struct.pack('<q', 1)


def watch_counter_key(model_cls: type) -> bytes:
    """FDB key of the per-class change counter, e.g. ``b'watch_seq/StorageNode'``."""
    return (WATCH_SEQ_PREFIX + model_cls.__name__).encode()


def unpack_counter(raw) -> int:
    """Decode a counter value produced by atomic ADD; absent/``None`` -> 0."""
    if not raw:
        return 0
    return struct.unpack('<q', bytes(raw)[:8].ljust(8, b'\x00'))[0]
