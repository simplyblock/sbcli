# coding=utf-8
"""Version-index keys for entity watches.

Every write/remove of a watched model class atomically maintains a hierarchical
version index in FoundationDB, in the same transaction as the entity mutation:

    watch_index/<ClassName>/<scope...>            rollup counter (atomic ADD)
    watch_index/<ClassName>/<scope...>/<uuid>     per-entity version (ADD; cleared on remove)

``<scope...>`` is the model's parent-id path (see ``BaseModel.watch_scope``).
Watchers (the web API's SSE layer) set a single FDB watch on the rollup key for
their scope (a list) or on an entity's version key (a detail); when it fires they
read the scope's version subtree, diff versions to learn exactly which entities
changed, and re-read only those.

The ``watch_index/`` namespace is disjoint from all entity range scans:
``BaseModel.read_from_db`` only scans ``<object_type>/<ClassName>/`` prefixes.

Stdlib-only leaf module — importable from models without cycles.
"""

import struct

WATCH_INDEX_PREFIX = 'watch_index/'

# Operand for FDB's atomic ADD mutation: little-endian 64-bit integer 1.
ONE_LE64 = struct.pack('<q', 1)


def _scope_path(model_cls: type, scope) -> str:
    """``watch_index/<ClassName>[/<s1>/<s2>...]`` for scope tuple ``scope``."""
    parts = [WATCH_INDEX_PREFIX + model_cls.__name__]
    parts.extend(str(s) for s in scope)
    return '/'.join(parts)


def watch_index_rollup_key(model_cls: type, scope=()) -> bytes:
    """Rollup key for a scope, e.g. ``b'watch_index/LVol/<pool>'``.

    Bumped on any write/remove of an entity in the scope; the watch target for a
    list stream. For a root class with empty scope this is ``watch_index/<Class>``.
    """
    return _scope_path(model_cls, scope).encode()


def watch_index_version_key(model_cls: type, scope, leaf: str) -> bytes:
    """Per-entity version key, e.g. ``b'watch_index/LVol/<pool>/<uuid>'``.

    ``leaf`` is the entity's ``get_id()`` — its object-key suffix under
    ``object/<Class>/`` (a plain uuid for flat classes, the compound
    ``<cluster>/<date>/<uuid>`` for JobSchedule) — so a watcher can reconstruct
    the object key and point-read the entity via ``read_from_db(id=leaf)``.
    Bumped on each write of the entity and cleared on remove; the watch target
    for a detail stream (flat-keyed classes).
    """
    return (_scope_path(model_cls, scope) + '/' + str(leaf)).encode()


def watch_index_version_prefix(model_cls: type, scope) -> bytes:
    """Prefix for scanning a scope's version keys (rollup key + ``/``).

    Disjoint from the rollup key itself (which has no trailing ``/``), so a
    ``get_range_startswith`` on this prefix returns only per-entity versions.
    """
    return (_scope_path(model_cls, scope) + '/').encode()


def unpack_counter(raw) -> int:
    """Decode a counter/version value produced by atomic ADD; absent/``None`` -> 0."""
    if not raw:
        return 0
    return struct.unpack('<q', bytes(raw)[:8].ljust(8, b'\x00'))[0]
