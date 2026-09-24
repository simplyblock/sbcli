"""Backfill and verification for the declared secondary indices.

:mod:`simplyblock_core.models.indices` defines what an index *is* and
:class:`~simplyblock_core.db_controller.DBController` reads and maintains one.
This module owns the operations that run over a whole keyspace:

* :func:`build_indices` — the restartable backfill that takes an index from
  ``building`` to ``ready``. Live writes maintain an index from the moment its
  declaration ships, so the backfill's job is only the records that predate it;
  it coordinates with normal traffic by deriving each record's entries inside
  the transaction that writes them, and letting FDB abort the ones it raced.
  A ``Unique`` index over data that already holds duplicates is the one thing
  it cannot complete, and it says so rather than picking a winner.
* :func:`check_indices` — the two-way verifier: every entity's derived keys
  exist, and every index key resolves to a live entity that really carries that
  value. It is the field diagnostic behind a ``UniqueIndexViolation`` and the
  thing that makes the whole scheme auditable.
* :func:`disable_index` / :func:`enable_index` — the kill switch behind
  ``sbctl cluster index-state``, which takes one bad index out of service
  without touching the rest of its class, and puts it back.
"""

import enum
import json
import logging
import time

import fdb
from tenacity import (
    Retrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_exponential,
)

from simplyblock_core import utils
from simplyblock_core.models import indices
from simplyblock_core.models.base_model import BaseModel
from simplyblock_core.utils import ttl_cache

logger = utils.get_logger(__name__)


#: Records indexed between two writes of the restart cursor. Not a transaction
#: size — each record is its own transaction — only how much of the walk a run
#: interrupted here would repeat.
INDEX_CURSOR_FLUSH = 200

#: Attempts one record gets before the backfill gives up on it. A derivation
#: that keeps losing its race is a record under constant rewrite, which is rare
#: enough that the budget is about bounding the loop, not about winning.
INDEX_RACE_ATTEMPTS = 5

#: FDB's ``not_committed``: something this transaction read was written under
#: it, so its read version no longer describes the data it is committing
#: against.
_NOT_COMMITTED = 1020

#: Seconds between two identical "this query fell back to a scan" messages.
#: The message names a systemic condition, not an event, so one line per class
#: per interval is the useful rate.
FALLBACK_LOG_INTERVAL_SEC = 300
_last_fallback_log: dict[tuple[str, str], float] = {}


def log_fallback(model_cls, index, state) -> None:
    """Log — rate-limited per index — that a read did not use the index.

    Informational: the scan answers exactly what the index would, so an index
    that is still building is a slower cluster, not a broken one.
    """
    key = (model_cls.__name__, index.name)
    now = time.monotonic()
    last = _last_fallback_log.get(key)
    if last is not None and (now - last) < FALLBACK_LOG_INTERVAL_SEC:
        return
    _last_fallback_log[key] = now
    logger.info(
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


def _walk_range(db, prefix: bytes, *, start: bytes = b'',
                chunk_size: int = BaseModel._READ_CHUNK_SIZE):
    """Yield every ``(key, value)`` under ``prefix`` from ``start`` on.

    Chunked for the same reason ``BaseModel.read_from_db`` is: an unbounded
    range read over a large table is one transaction and dies on FDB's 5s
    limit, then retries the same full scan forever. Each chunk is therefore its
    own read version, and nothing a walk yields is a snapshot of the table.
    """
    begin = start or prefix
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


def _walk_entities(db, model_cls, *, start: bytes = b''):
    return _walk_range(db, entity_prefix(model_cls), start=start,
                       chunk_size=model_cls._READ_CHUNK_SIZE)


def _walk_keys(db, model_cls, start_key: bytes):
    """The key of every record of one table, oldest first.

    Keys, never records. A key is a name and cannot go stale; a record read out
    here would be a claim about a version that has moved on by the time its
    entries are written, which is exactly how a backfill writes an entry for a
    value its record no longer carries. :func:`_index_record` re-reads each key
    in the transaction that writes what it derives.
    """
    return (key for key, _value in _walk_entities(db, model_cls, start=start_key))


def _walk_records(db, model_cls):
    """Every ``(key, object)`` of one table, oldest key first.

    The objects come out of the walk's own transaction, so by the time a caller
    acts on one it is a claim about a version that may have moved. Only
    :func:`check_indices` reads this way, and only because it re-derives every
    finding inside the transaction that acts on it. The backfill takes
    :func:`_walk_keys` instead.
    """
    return ((key, model_cls().from_dict(json.loads(value)))
            for key, value in _walk_entities(db, model_cls))


class ConcurrentRecordChange(Exception):
    """A backfill transaction lost its race and its derivation is void.

    Something it read — the record itself, or a unique key it was about to
    claim — was written underneath it, so what it derived describes a version
    that is no longer current. Raised rather than swallowed because the two
    ways of not raising are both wrong: committing anyway writes an entry for a
    value nobody holds, and retrying the *same* derivation (which is what
    ``fdb.transactional`` does, since it re-runs the function with the
    arguments it was given) writes that same entry one attempt later.
    """

    def __init__(self, model_cls, key: bytes) -> None:
        super().__init__(
            f'{model_cls.__name__} record {key.decode()} changed while its '
            'index entries were being derived')
        self.model_name = model_cls.__name__
        self.key = key


def _index_record(db, model_cls, index_list, key: bytes) -> list:
    """Write one record's entries for ``index_list``, in ONE transaction.

    The record is read *here*, so the entries are derived from the same version
    the commit is checked against and no derived value outlives its record.

    ``old_keys={}`` is what makes this a backfill rather than an update: with
    the record's own keys as ``old``, the unique pre-check would short-circuit
    on every one of them and detect nothing. Empty, it checks each key and
    clears none, which is precisely "add what is missing".

    Returns the unique values the record could not claim — data that predates
    the constraint, which the caller holds the owning index back from ``ready``
    for. The list is built inside the attempt loop, so an attempt that goes on
    to lose its race cannot leave a finding behind in the one that replaces it.

    Raises :class:`ConcurrentRecordChange` when the transaction is aborted.
    """
    tr = db.kv_store.create_transaction()
    while True:
        violations: list = []
        try:
            obj = BaseModel._read_record(tr, key, model_cls)
            if obj is None:
                return violations  # removed between the walk and here
            # Redundant with the plain (non-snapshot) get above, and kept
            # because it is the guarantee this function exists for: a snapshot
            # read takes no conflict range, so switching to one would silently
            # restore the stale derivation this replaced.
            tr.add_read_conflict_key(key)
            BaseModel._apply_index_diff(tr, model_cls, index_list, {}, obj,
                                        on_violation=violations.append)
            tr.commit().wait()
            return violations
        except fdb.FDBError as error:  # type: ignore[attr-defined]  # injected by fdb.api_version()
            if error.code == _NOT_COMMITTED:
                raise ConcurrentRecordChange(model_cls, key) from error
            # Everything else is FDB's own business — too old, unknown commit
            # result, timed out. `on_error` backs off and resets the
            # transaction for another attempt, or re-raises what cannot be
            # retried. The loop body re-reads, so an attempt never inherits the
            # previous one's view.
            tr.on_error(error.code).wait()


def _index_record_retrying(db, model_cls, index_list, key: bytes) -> tuple[int, list]:
    """:func:`_index_record` with a bounded retry: ``(races lost, violations)``.

    Re-deriving is the answer to a race rather than skipping the record. Where
    the conflict came from a write of the record, skipping would be safe — that
    write maintained every index itself — but a conflict also comes from
    another record *releasing* a unique key this one derives, and then this
    record's entry is nobody else's to write.
    """
    retrying = Retrying(
        stop=stop_after_attempt(INDEX_RACE_ATTEMPTS),
        wait=wait_exponential(multiplier=0.05, max=1),
        retry=retry_if_exception_type(ConcurrentRecordChange),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    violations = retrying(_index_record, db, model_cls, index_list, key)
    return retrying.statistics.get('attempt_number', 1) - 1, violations


def build_indices(model_classes=None, *, log=None) -> list[str]:
    """Backfill every index that is not ``ready`` yet and flip it.

    State is tracked per index, so one bad index can be disabled without taking
    the rest of its class down with it — but the indices of a class that need
    building are walked *together*, since the walk is the expensive half and
    they all advance through the same keys. Restartable: a cursor lags the walk
    by at most ``INDEX_CURSOR_FLUSH`` records in each index's state record, and
    a resumed run picks up from the oldest of them.

    One record, one transaction, deriving what it writes from a read of its own
    (:func:`_index_record`). That costs a round trip per record where a batch
    cost one per two hundred, which is the price of the backfill being the same
    derivation live writes use rather than a second implementation of it — and
    of a record never being indexed under a value it no longer carries.

    An index whose values are not actually unique in the data is left
    ``building`` and named in the report, and so is a record that stayed under
    rewrite for its whole retry budget: both mean entries are missing that
    nothing else is going to add, and flipping ``ready`` over either publishes
    an index that answers short.

    A ``disabled`` index is skipped: it stays out of service until an operator
    puts it back with :func:`enable_index`. Rebuilding a ``ready`` index is
    spelled the same way — ``enable_index`` empties it and hands it back at
    ``building`` from whatever state it was in — so there is no second,
    half-clearing rebuild path here.
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
            if state == indices.STATE_READY:
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

        walked = raced = 0
        collisions: dict[str, list] = {}
        unindexed: list[str] = []
        for walked, key in enumerate(_walk_keys(db, model_cls, start_key), start=1):
            try:
                lost, violations = _index_record_retrying(
                    db, model_cls, pending, key)
            except ConcurrentRecordChange:
                logger.error('%s stayed under rewrite for %s attempts; its '
                             'entries were not derived', key.decode(),
                             INDEX_RACE_ATTEMPTS)
                unindexed.append(key.decode())
            else:
                raced += lost
                for violation in violations:
                    collisions.setdefault(violation.index_name, []).append(violation)
            if walked % INDEX_CURSOR_FLUSH == 0:
                for index in pending:
                    db.set_index_state(model_cls, index, indices.STATE_BUILDING,
                                       cursor=key.decode())

        for index in pending:
            found = collisions.get(index.name)
            if found is None and not unindexed:
                db.set_index_state(model_cls, index, indices.STATE_READY)
                continue
            for violation in found or ():
                logger.error('%s', violation)
            # Held at `building`, and the cursor reset: readers keep the scan
            # fallback, which still answers with every duplicate, and the next
            # run re-walks the whole table instead of resuming past the
            # record it could not index and flipping the index this run refused.
            db.set_index_state(model_cls, index, indices.STATE_BUILDING, cursor='')
            if found:
                report.append(
                    f'{model_cls.__name__}.{index.name}: NOT ready, {len(found)} '
                    f'duplicate value(s) in the data; resolve them (see '
                    f'`sbctl cluster check-indices`) and re-run')
        if unindexed:
            report.append(
                f'{model_cls.__name__}: NOT ready, {len(unindexed)} record(s) '
                f'could not be indexed while being rewritten; re-run')
        report.append(f'{model_cls.__name__}: {walked} records indexed into {names}'
                      + (f' ({raced} re-derived after racing a write)' if raced else ''))

    return report


def unready_indices(model_classes=None) -> list[str]:
    """``Class.index`` for every declared index reads still cannot trust.

    After a completed :func:`build_indices` this is the list of indices it
    refused to flip, which is what makes the refusal an operator-visible
    outcome rather than a line in a log. ``disabled`` is deliberate and is not
    reported here.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    return [
        f'{model_cls.__name__}.{index.name}'
        for model_cls in _resolve_classes(model_classes)
        for index in indices.indexes_of(model_cls)
        if db.index_state(model_cls, index) == indices.STATE_BUILDING
    ]


def resolve_index(qualified_name: str) -> tuple[type, indices.Index]:
    """``"LVol.node_id"`` -> the class that declares it and the declaration."""
    model_name, _, index_name = qualified_name.partition('.')
    if not index_name:
        raise ValueError(
            f'{qualified_name!r} does not name an index; expected '
            f'<Class>.<index>, e.g. LVol.node_id')
    for model_cls in indexed_model_classes():
        if model_cls.__name__ == model_name:
            return model_cls, indices.get_index(model_cls, index_name)
    raise KeyError(f'no model class {model_name!r} declares an index')


def index_states(model_classes=None, index=None) -> list[dict]:
    """Every declared index with the state record standing behind it.

    Read straight from ``index_meta/``, not through
    :meth:`DBController.index_state`, whose TTL cache would report what this
    process last saw rather than what the cluster currently holds.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    rows = []
    for model_cls in _resolve_classes(model_classes):
        declared = ((indices.get_index(model_cls, index),) if index is not None
                    else indices.indexes_of(model_cls))
        for idx in declared:
            meta = db.index_meta(model_cls, idx)
            rows.append({
                'index': f'{model_cls.__name__}.{idx.name}',
                'state': meta.get('state', indices.STATE_BUILDING),
                'cursor': meta.get('cursor', ''),
                'updated_at': meta.get('updated_at', ''),
            })
    return rows


def _index_root(model_cls, index) -> bytes:
    """The whole keyspace one index occupies.

    ``prefix`` with no values, which ends in ``/`` — so it cannot reach a
    sibling whose name it prefixes (``pool_uuid`` vs ``pool_uuid+lvol_name``).
    """
    return index.prefix(model_cls, ())


def _await_state_convergence(model_cls, index, state) -> None:
    """Block until no process can still be acting on the previous state.

    Every other process holds its own TTL-cached copy of the state, so a switch
    is not in effect fleet-wide the moment it commits. Callers that go on to
    touch the index keyspace depend on that convergence, and the operator who
    typed the command depends on it too: "disabled" that has not reached the
    readers yet is not a kill switch.
    """
    delay = ttl_cache.INDEX_STATE_CONVERGENCE_SEC
    if delay <= 0:
        return
    logger.info("Waiting %ss for %s.%s=%s to reach every process",
                delay, model_cls.__name__, index.name, state)
    time.sleep(delay)


def disable_index(model_cls, index) -> None:
    """Take one index out of service. Writes stop maintaining it, reads scan.

    The state record is the whole operation, and the entries are deliberately
    left where they are. Other processes hold the state for up to
    ``ttl_cache.INDEX_STATE_TTL_SEC`` before they see this, and until they do
    they still *read* the index: emptying it here would answer those reads with
    no rows rather than sending them to the scan, which is a wrong answer where
    the point of the switch is a slow one. The entries are cleared on the way
    back in instead — see :func:`enable_index`.

    Returns once the switch has reached every process, so that a caller
    sequencing anything after it — the operator's next command included — acts
    on a fleet that has converged rather than on one that still half-trusts the
    index.
    """
    from simplyblock_core.db_controller import DBController

    idx = indices.get_index(model_cls, index)
    DBController().set_index_state(model_cls, idx, indices.STATE_DISABLED)
    _await_state_convergence(model_cls, idx, indices.STATE_DISABLED)


def enable_index(model_cls, index) -> None:
    """Put a disabled index back into service, empty, at ``building``.

    The clear is what makes the kill switch reversible. An index out of service
    is not maintained, so its entries go *stale* rather than merely incomplete,
    and the backfill only ever adds: an index that carried its entries across a
    disable would flip back to ``ready`` still holding keys for values its
    records no longer derive.

    Emptying an index is only safe once nothing trusts it any more, which the
    ``disabled`` state alone does not establish: a process that read ``ready``
    just before the disable committed keeps using the index for up to one TTL,
    and would read the emptied keyspace as "no such records" — the wrong answer
    the disable path went out of its way not to give. So the sequence is: out of
    service (from whatever state the index is in, so enabling a ``ready`` index
    is safe too), wait out the caches, *then* clear. Ordering the state writes
    around the clear this way also means an interrupted run leaves the index
    ``disabled`` — out of service and possibly empty, never trusted and empty.

    ``building`` rather than ``ready``: writes maintain it again from here, but
    everything that changed while it was out of service is missing from it
    until :func:`build_indices` has walked the table, and until then reads keep
    the scan fallback.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    idx = indices.get_index(model_cls, index)
    if db.index_meta(model_cls, idx).get('state') != indices.STATE_DISABLED:
        db.set_index_state(model_cls, idx, indices.STATE_DISABLED)
    _await_state_convergence(model_cls, idx, indices.STATE_DISABLED)
    db.kv_store.clear_range_startswith(_index_root(model_cls, idx))
    db.set_index_state(model_cls, idx, indices.STATE_BUILDING, cursor='')


def _read_entity(tr, model_cls, entity_id):
    """The record ``entity_id`` names, or ``None``, read inside ``tr``."""
    raw = tr.get(entity_prefix(model_cls) + entity_id.encode()).wait()
    if raw is None or not raw.present():
        return None
    return model_cls().from_dict(json.loads(bytes(raw)))


class RepairOutcome(enum.Enum):
    """What one repair transaction did with one finding.

    The values are the counter names :func:`check_indices` tallies them under,
    which is what lets its caller answer "is anything still wrong?" — a question
    the repair count alone cannot, since a refusal and a finding that evaporated
    both leave it unchanged.
    """

    #: The entry was written or cleared.
    REPAIRED = 'repaired'
    #: The finding no longer holds — the walk raced a concurrent write.
    VANISHED = 'vanished'
    #: Still wrong, and only an operator can settle it.
    UNRESOLVED = 'unresolved'


def _repair_entry_tx(tr, model_cls, index, index_key, entity_id) -> RepairOutcome:
    """Point ``index_key`` at ``entity_id`` and report what happened.

    Everything the finding rests on is re-read here, which also puts it in the
    transaction's conflict range, so a concurrent write of either record aborts
    this rather than losing to it.
    """
    if index_key not in index.keys(model_cls, _read_entity(tr, model_cls, entity_id)):
        return RepairOutcome.VANISHED

    held = tr.get(index_key).wait()
    if held is not None and held.present():
        holder = index.entry_id(model_cls, index_key, bytes(held))
        if holder == entity_id:
            return RepairOutcome.VANISHED
        # Only a unique key can name another entity at all — a non-unique key
        # carries the id it belongs to. Whether that is drift or a genuine
        # duplicate is the holder's business: if it still derives the key, two
        # live records claim one value and nothing here can pick a winner. If it
        # does not, the candidate is the only claimant left.
        if index_key in index.keys(model_cls, _read_entity(tr, model_cls, holder)):
            return RepairOutcome.UNRESOLVED

    tr[index_key] = index.entry_value(entity_id)
    return RepairOutcome.REPAIRED


def _clear_orphan_tx(tr, model_cls, index, index_key, holder_id) -> RepairOutcome:
    """Clear ``index_key`` and report what happened.

    Cleared only if the record it names no longer derives it, re-read here as in
    :func:`_repair_entry_tx`. ``index`` is ``None`` — and with it ``holder_id`` —
    for a key no current declaration owns, left behind by a renamed or removed
    index: there is no record to consult, and nothing maintains it either.
    """
    held = tr.get(index_key).wait()
    if held is None or not held.present():
        return RepairOutcome.VANISHED

    if index is not None and holder_id is not None:
        if index.entry_id(model_cls, index_key, bytes(held)) != holder_id:
            return RepairOutcome.VANISHED
        obj = _read_entity(tr, model_cls, holder_id)
        if index_key in index.keys(model_cls, obj):
            return RepairOutcome.VANISHED

    tr.clear(index_key)
    return RepairOutcome.REPAIRED


def _stored_entries(db, model_cls, declared, prefix) -> dict:
    """``{key: (index, holder id)}`` for every entry stored under ``prefix``.

    The index comes from the key's own name segment, which is what says how many
    value segments :meth:`Index.entry_id` has to skip to reach the id. Both are
    ``None`` for a key no declaration owns — left behind by a renamed or removed
    index — and for one too malformed to read an id out of. Neither can equal a
    key a record derives, so both land in the orphan half of the walk, which is
    where they belong.
    """
    by_name = {index.name: index for index in declared}
    entries: dict[bytes, tuple[indices.Index | None, str | None]] = {}
    for key, value in _walk_range(db, prefix):
        try:
            index = by_name.get(key[len(prefix):].split(b'/', 1)[0].decode())
            holder = index.entry_id(model_cls, key, value) if index is not None else None
        except ValueError:
            index, holder = None, None
        entries[key] = (index, holder)
    return entries


def check_indices(model_classes=None, *, repair=False, log=None) -> dict:
    """Walk both directions of every index and report (or repair) the drift.

    Forward: every key an entity's declaration derives must exist and name that
    entity. Backward: every stored index key must resolve to a live entity that
    still carries the value the key encodes.

    Read-only by default and safe against a live cluster: it takes no locks and
    holds nothing offline. A finding is not automatically a bug in the index —
    an index still ``building`` is *expected* to be missing entries — so the
    state is reported alongside.

    A finding is a *candidate*: neither walk is isolated from concurrent
    traffic, so a record created, deleted or updated between the two shows up
    as an orphan or a missing entry with nothing actually wrong. Each repair
    re-derives that record's keys in its own transaction and acts only if the
    finding still holds; a finding that evaporated that way is counted as
    ``vanished``.

    Every finding therefore ends in exactly one of three counters — ``repaired``,
    ``vanished``, ``unresolved`` — and only the last says the cluster still needs
    something. A read-only run leaves every finding unresolved because it settles
    none of them; so does a value duplicated across two live records, which no
    repair can resolve by picking a winner.
    """
    from simplyblock_core.db_controller import DBController

    db = DBController()
    emit = log or logger.info
    findings: dict = {
        'missing': [], 'stale': [], 'orphaned': [], 'duplicate': [],
        'repaired': 0, 'vanished': 0, 'unresolved': 0,
    }

    def repair_one(fn, *args) -> None:
        findings[fdb.transactional(fn)(db.kv_store, *args).value] += 1

    for model_cls in _resolve_classes(model_classes):
        declared = indices.indexes_of(model_cls)
        states = {index.name: db.index_state(model_cls, index) for index in declared}
        expected: dict[bytes, tuple[str, indices.Index]] = {}

        for _key, obj in _walk_records(db, model_cls):
            entity_id = str(obj.get_id())
            for index in declared:
                for index_key in index.keys(model_cls, obj):
                    claimed = expected.get(index_key)
                    # Two live records deriving ONE unique key is the real
                    # duplicate a UniqueIndexViolation reports at write time.
                    # It is a finding about the data, not the index, and
                    # nothing here can pick a winner.
                    if claimed is not None and claimed[0] != entity_id:
                        findings['duplicate'].append(
                            (index_key.decode(), claimed[0], entity_id))
                        findings['unresolved'] += 1
                    expected[index_key] = (entity_id, index)

        # One chunked range read of the whole index keyspace rather than a point
        # read per expected key: the verifier is meant to be runnable against a
        # live cluster of any size.
        prefix = f'{indices.INDEX_PREFIX}{model_cls.__name__}/'.encode()
        stored = _stored_entries(db, model_cls, declared, prefix)

        for index_key, (entity_id, index) in expected.items():
            entry = stored.get(index_key)
            if entry is None:
                findings['missing'].append((index_key.decode(), entity_id))
            elif entry[1] != entity_id:
                findings['stale'].append((index_key.decode(), entry[1], entity_id))
            else:
                continue
            if repair:
                repair_one(_repair_entry_tx, model_cls, index, index_key, entity_id)
            else:
                findings['unresolved'] += 1

        for index_key, (index, holder) in stored.items():
            if index_key in expected:
                continue
            findings['orphaned'].append(index_key.decode())
            if repair:
                repair_one(_clear_orphan_tx, model_cls, index, index_key, holder)
            else:
                findings['unresolved'] += 1

        emit(f'{model_cls.__name__}: {len(expected)} expected entries, '
             f'{len(stored)} stored, states={states}')

    return findings
