"""Backfill and verification for the declared secondary indices.

:mod:`simplyblock_core.models.indices` defines what an index *is* and
:class:`~simplyblock_core.db_controller.DBController` reads and maintains one.
This module owns the operations that run over a whole keyspace:

* :func:`build_indices` — the chunked, restartable backfill that takes an index
  from ``building`` to ``ready``. Live writes maintain an index from the moment
  its declaration ships, and the backfill only ever *adds* entries, so it needs
  no coordination with normal traffic.
* :func:`check_indices` — the two-way verifier: every entity's derived keys
  exist, and every index key resolves to a live entity that really carries that
  value. It is the field diagnostic behind a ``UniqueIndexViolation`` and the
  thing that makes the whole scheme auditable.
"""

import json
import time

import fdb

from simplyblock_core import utils
from simplyblock_core.models import indices
from simplyblock_core.models.base_model import BaseModel

logger = utils.get_logger(__name__)


#: Records whose index entries are written in one backfill transaction. The
#: read side is chunked at ``_READ_CHUNK_SIZE``; the write side commits in much
#: smaller pieces because each record contributes one key per index, so a
#: read-sized batch would put five figures of writes in a single transaction —
#: against FDB's 10MB/5s budget, and with the whole batch to redo on a retry.
#: Smaller batches also make the restart cursor finer-grained.
INDEX_WRITE_BATCH = 200

#: Seconds between two identical "this query fell back to a scan" warnings.
#: The message names a systemic condition, not an event, so one line per class
#: per interval is the useful rate.
FALLBACK_WARN_INTERVAL_SEC = 300
_last_fallback_warning: dict[tuple[str, str], float] = {}


def warn_fallback(model_cls, index, state) -> None:
    """Log — rate-limited per index — that a read did not use the index."""
    key = (model_cls.__name__, index.name)
    now = time.monotonic()
    last = _last_fallback_warning.get(key)
    if last is not None and (now - last) < FALLBACK_WARN_INTERVAL_SEC:
        return
    _last_fallback_warning[key] = now
    logger.warning(
        "Index %s.%s is %s: falling back to a full scan of %s. Run "
        "`sbctl cluster build-indices` to complete it.",
        model_cls.__name__, index.name, state, model_cls.__name__)


def indexed_model_classes() -> list[type]:
    """Every model class that declares at least one index.

    Derived from the declarations rather than from a hand-kept list, so a new
    ``_INDEXES`` is picked up by the backfill and the verifier without a second
    edit somewhere else.
    """
    import importlib
    import pkgutil

    from simplyblock_core import models

    found: dict[str, type] = {}
    for module_info in pkgutil.iter_modules(models.__path__):
        module = importlib.import_module(f'simplyblock_core.models.{module_info.name}')
        for name in dir(module):
            candidate = getattr(module, name)
            if (isinstance(candidate, type)
                    and issubclass(candidate, BaseModel)
                    and candidate.__module__ == module.__name__
                    and indices.indexes_of(candidate)):
                found[candidate.__name__] = candidate
    return [found[name] for name in sorted(found)]


def _resolve_classes(model_classes) -> list[type]:
    if model_classes is None:
        return indexed_model_classes()
    if isinstance(model_classes, type):
        return [model_classes]
    return list(model_classes)


def entity_prefix(model_cls) -> bytes:
    """``object/<ClassName>/`` — the keyspace one table occupies."""
    return model_cls.keyspace_prefix()


def _walk_range(db, prefix: bytes, chunk_size: int = BaseModel._READ_CHUNK_SIZE):
    """Yield every ``(key, value)`` under ``prefix``, one bounded chunk at a time."""
    begin = prefix
    end = BaseModel._next_prefix(prefix)
    while True:
        kvs = list(db.kv_store.get_range(begin, end, limit=chunk_size))
        if not kvs:
            return
        for kv in kvs:
            yield bytes(kv.key), bytes(kv.value)
        if len(kvs) < chunk_size:
            return
        begin = bytes(kvs[-1].key) + b'\x00'


def _walk_records(db, model_cls, start_key: bytes):
    """Yield ``(key, object)`` chunks of one table, oldest key first.

    Chunked for the same reason ``BaseModel.read_from_db`` is: an unbounded
    range read over a large table is one transaction and dies on FDB's 5s
    limit, then retries the same full scan forever.
    """
    prefix = entity_prefix(model_cls)
    begin = start_key or prefix
    end = BaseModel._next_prefix(prefix)
    chunk_size = model_cls._READ_CHUNK_SIZE
    while True:
        kvs = list(db.kv_store.get_range(begin, end, limit=chunk_size))
        if not kvs:
            return
        yield [
            (bytes(kv.key), model_cls().from_dict(json.loads(bytes(kv.value))))
            for kv in kvs
        ]
        if len(kvs) < chunk_size:
            return
        begin = bytes(kvs[-1].key) + b'\x00'


def _index_chunk_tx(tr, model_cls, index_list, records):
    for _key, obj in records:
        entity_id = str(obj.get_id()).encode()
        for index in index_list:
            for index_key in index.keys(model_cls, obj):
                tr[index_key] = entity_id


def build_indices(model_classes=None, *, force=False, log=None) -> list[str]:
    """Backfill every index that is not ``ready`` yet and flip it.

    State is tracked per index, so one bad index can be disabled without taking
    the rest of its class down with it — but the indices of a class that need
    building are walked *together*, since the walk is the expensive half and
    they all advance through the same keys. Restartable: the cursor of the last
    committed chunk lives in each index's state record, and a resumed run picks
    up from the oldest of them.

    ``force`` rebuilds indices that already reached ``ready``; a ``disabled``
    index is always skipped — that is what the kill switch is for.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    report = []
    emit = log or logger.info

    for model_cls in _resolve_classes(model_classes):
        pending = []
        cursors = []
        for index in indices.indexes_of(model_cls):
            state = db.index_state(model_cls, index)
            if state == indices.STATE_DISABLED:
                report.append(f'{model_cls.__name__}.{index.name}: disabled, skipped')
                continue
            if state == indices.STATE_READY and not force:
                continue
            pending.append(index)
            cursors.append(db.index_meta(model_cls, index).get('cursor') or '')
        if not pending:
            continue

        # Resume from the oldest cursor: an index that lags behind the others
        # (added later, or interrupted earlier) must not have the walk skip
        # past the records it still misses.
        start_key = min(cursors).encode() if all(cursors) else b''
        names = ', '.join(index.name for index in pending)
        emit(f'Building {model_cls.__name__} indices: {names}'
             + (f' (resuming at {start_key.decode()})' if start_key else ''))

        walked = 0
        for records in _walk_records(db, model_cls, start_key):
            for start in range(0, len(records), INDEX_WRITE_BATCH):
                batch = records[start:start + INDEX_WRITE_BATCH]
                fdb.transactional(_index_chunk_tx)(
                    db.kv_store, model_cls, pending, batch)
                walked += len(batch)
                cursor = batch[-1][0].decode()
                for index in pending:
                    db.set_index_state(model_cls, index, indices.STATE_BUILDING,
                                       cursor=cursor)

        for index in pending:
            db.set_index_state(model_cls, index, indices.STATE_READY)
        report.append(f'{model_cls.__name__}: {walked} records indexed into {names}')

    return report


def backfill_lvol_cluster_id(*, log=None) -> str:
    """Fill in ``LVol.cluster_id`` on volumes created before the field existed.

    The field is denormalized from the volume's node, so the value is
    recoverable; without it a per-cluster listing has to resolve the cluster's
    nodes first and filter every volume against that id list.
    """
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.lvol_model import LVol

    db = DBController()
    emit = log or logger.info
    nodes = {node.get_id(): node.cluster_id for node in db.get_storage_nodes()}

    filled = 0
    unresolved = 0
    for lvol in LVol().read_from_db(db.kv_store):
        if lvol.cluster_id:
            continue
        cluster_id = nodes.get(lvol.node_id, '')
        if not cluster_id:
            unresolved += 1
            continue
        db.atomic_update(lvol, lambda obj, value=cluster_id: setattr(obj, 'cluster_id', value))
        filled += 1

    message = f'LVol.cluster_id: {filled} volumes filled in'
    if unresolved:
        message += f', {unresolved} left blank (node unknown)'
    emit(message)
    return message


def check_indices(model_classes=None, *, repair=False, log=None) -> dict:
    """Walk both directions of every index and report (or repair) the drift.

    Forward: every key an entity's declaration derives must exist and name that
    entity. Backward: every stored index key must resolve to a live entity that
    still carries the value the key encodes.

    Read-only by default and safe against a live cluster: it takes no locks and
    holds nothing offline. A finding is not automatically a bug in the index —
    an index still ``building`` is *expected* to be missing entries — so the
    state is reported alongside.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    emit = log or logger.info
    findings: dict = {
        'missing': [], 'stale': [], 'orphaned': [], 'duplicate': [], 'repaired': 0,
    }

    for model_cls in _resolve_classes(model_classes):
        declared = indices.indexes_of(model_cls)
        states = {index.name: db.index_state(model_cls, index) for index in declared}
        expected: dict[bytes, tuple[str, indices.Index]] = {}

        for records in _walk_records(db, model_cls, b''):
            for _key, obj in records:
                entity_id = str(obj.get_id())
                for index in declared:
                    for index_key in index.keys(model_cls, obj):
                        claimed = expected.get(index_key)
                        # Two live records deriving ONE unique key is the real
                        # duplicate a UniqueIndexViolation reports at write
                        # time. It is a finding about the data, not the index,
                        # and nothing here can pick a winner.
                        if claimed is not None and claimed[0] != entity_id:
                            findings['duplicate'].append(
                                (index_key.decode(), claimed[0], entity_id))
                        expected[index_key] = (entity_id, index)

        # One chunked range read of the whole index keyspace rather than a point
        # read per expected key: the verifier is meant to be runnable against a
        # live cluster of any size.
        prefix = f'{indices.INDEX_PREFIX}{model_cls.__name__}/'.encode()
        stored = {key: value.decode() for key, value in _walk_range(db, prefix)}

        for index_key, (entity_id, index) in expected.items():
            holder = stored.get(index_key)
            if holder is None:
                findings['missing'].append((index_key.decode(), entity_id))
                if repair:
                    db.kv_store[index_key] = entity_id.encode()
                    findings['repaired'] += 1
            elif holder != entity_id:
                findings['stale'].append((index_key.decode(), holder, entity_id))
                # A non-unique key ends in the entity id it belongs to, so a
                # different value there is corruption with one right answer. A
                # unique key legitimately names one of several claimants, and
                # repairing it would just pick one — left to the operator.
                if repair and not index.unique:
                    db.kv_store[index_key] = entity_id.encode()
                    findings['repaired'] += 1

        for index_key in stored:
            if index_key in expected:
                continue
            findings['orphaned'].append(index_key.decode())
            if repair:
                db.kv_store.clear(index_key)
                findings['repaired'] += 1

        emit(f'{model_cls.__name__}: {len(expected)} expected entries, '
             f'{len(stored)} stored, states={states}')

    return findings
