# coding=utf-8
"""Generic entity-watch primitive for streaming state changes (version index).

Change signal: every write/remove of a watched model class atomically maintains a
hierarchical version index in the same FDB transaction as the mutation (see
:mod:`simplyblock_core.watches`): a per-scope ``rollup`` counter and a per-entity
``version`` counter, keyed by the model's ``watch_scope()`` path.

A subscription watches a single FDB key — the scope's rollup key (a *list*) or an
entity's version key (a *detail*). When it fires, one shared :class:`ScopeWatch`
per watch key reads that scope's version subtree, diffs versions against the
previous read to learn exactly which entities changed, point-reads only those, and
fans the resulting scope entity list out to every subscriber. Unrelated scopes do
not wake the watch, and only changed entities are re-read.

This module owns the *mechanism* only. It emits typed :class:`ChangeEvent`
batches (``created``/``updated``/``deleted`` over model objects); building wire
DTOs and framing them as SSE is the web layer's concern (``_sse.py``). The
``ChangeEvent`` interface is what lets this version-index backend replace the
earlier counter+full-re-read backend without touching controllers or the API.
"""

import asyncio
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

import fdb

from simplyblock_core import watches

logger = logging.getLogger(__name__)

# Upper bound on staleness / insurance against a missed watch: re-scan the scope
# index even without a watch fire (the version-map diff makes a no-op re-scan cheap
# — it publishes nothing when nothing changed).
WATCH_RECONCILE_SEC = 30.0

_FDB_RETRY_BACKOFF_SEC = (1.0, 2.0, 4.0)

_UNSET: Any = object()

# Filters the shared scope entity list down to a subscription's presentation set.
# Returns model objects (typically by reusing a DBController getter with ``source=``).
Select = Callable[[List[Any]], List[Any]]


class WatchUnavailable(Exception):
    """The watch backend failed irrecoverably; the stream must terminate."""


@dataclass
class ChangeEvent:
    """A single entity transition within a watched, scoped set."""

    kind: str  # 'created' | 'updated' | 'deleted'
    id: str
    # For 'deleted': the entity's current (unfiltered) model if it left the
    # scoped set but is still retrievable (e.g. an LVol whose status flipped to
    # 'deleted'), else None once it is physically gone.
    model: Optional[Any]


def diff_by_id(
        prev: Dict[str, dict], new: Dict[str, Any], full: Dict[str, Any],
) -> Tuple[List[ChangeEvent], Dict[str, dict]]:
    """Diff the scoped set against the previous emission, keyed by ``get_id()``.

    ``prev`` maps id -> the ``to_dict()`` snapshot emitted last time; ``new`` and
    ``full`` map id -> model for the current scoped and unscoped sets. Returns
    the events (created/updated in ``new`` iteration order, deletions appended)
    and the fresh id -> ``to_dict()`` snapshot to carry forward.
    """
    events: List[ChangeEvent] = []
    new_dicts: Dict[str, dict] = {}
    for entity_id, model in new.items():
        snapshot = model.to_dict()
        new_dicts[entity_id] = snapshot
        if entity_id not in prev:
            events.append(ChangeEvent('created', entity_id, model))
        elif snapshot != prev[entity_id]:
            events.append(ChangeEvent('updated', entity_id, model))
        # else unchanged: no event
    for entity_id in prev:
        if entity_id in new:
            continue
        # Left the scoped set. If still present in the unscoped set, carry its
        # current model so the wire layer can render the final representation;
        # otherwise it is physically gone (model=None -> empty representation).
        events.append(ChangeEvent('deleted', entity_id, full.get(entity_id)))
    return events, new_dicts


class _Subscription:
    """Coalescing mailbox: written by the watch thread, read by one stream."""

    def __init__(self, loop: asyncio.AbstractEventLoop, notify: asyncio.Event) -> None:
        self._loop = loop
        self._notify = notify
        self._latest: Any = _UNSET
        self._dirty = False
        self.failed = False

    def _deliver(self, state: Any) -> None:  # event-loop thread
        self._latest = state
        self._dirty = True
        self._notify.set()

    def _fail(self) -> None:  # event-loop thread
        self.failed = True
        self._notify.set()

    def deliver(self, state: Any) -> None:  # watch thread
        self._loop.call_soon_threadsafe(self._deliver, state)

    def fail(self) -> None:  # watch thread
        self._loop.call_soon_threadsafe(self._fail)

    def take(self) -> Any:
        """Latest state if there is an unconsumed publication, else ``_UNSET``."""
        if not self._dirty:
            return _UNSET
        self._dirty = False
        return self._latest


def _watch_tx(tr: Any, key: bytes) -> Any:
    return tr.watch(key)


class ScopeWatch:
    """Single FDB watch on one key + version-diff read for one watched scope.

    Delivers ``List[model]`` — the current entity set of the scope — to
    subscribers, exactly as the old class-wide hub delivered the full class list,
    so ``watch()``/``diff_by_id``/``select`` are unchanged. A list watch targets
    the scope's rollup key and reads its version subtree; a detail watch
    (``entity_id`` given) targets that entity's version key and reads it alone.
    Change is detected by diffing the version map, so a reconcile re-scan that
    finds nothing changed publishes nothing; only changed entities are re-read.
    """

    def __init__(self, model_cls: Type[Any], scope: Sequence[Any], entity_id: Optional[str]) -> None:
        self._model_cls = model_cls
        self._scope = tuple(scope)
        self._entity_id = entity_id
        if entity_id is not None:
            self._detail = True
            self._watch_key = watches.watch_index_version_key(model_cls, self._scope, entity_id)
        else:
            self._detail = False
            self._watch_key = watches.watch_index_rollup_key(model_cls, self._scope)
            self._version_prefix = watches.watch_index_version_prefix(model_cls, self._scope)
        self._name = f'{model_cls.__name__}/{"/".join(map(str, self._scope))}/{entity_id or "*"}'
        self._lock = threading.Lock()
        self._subs: set = set()
        self._cache: Any = _UNSET       # last delivered List[model]
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # Reader state (owned by the watch thread):
        self._prev_versions: Dict[str, int] = {}
        self._entities: Dict[str, Any] = {}  # leaf (get_id) -> model
        self._started = False

    def subscribe(self, loop: asyncio.AbstractEventLoop, notify: asyncio.Event) -> _Subscription:
        sub = _Subscription(loop, notify)
        with self._lock:
            alive = self._thread is not None and self._thread.is_alive() and not self._stop.is_set()
            if not alive:
                self._cache = _UNSET
                self._prev_versions = {}
                self._entities = {}
                self._started = False
                self._stop = threading.Event()
                self._wake = threading.Event()
                self._thread = threading.Thread(
                    target=self._run, args=(self._stop, self._wake),
                    daemon=True, name=f'watch-{self._name}',
                )
                self._thread.start()
            self._subs.add(sub)
            if self._cache is not _UNSET:
                # subscribe() runs on the event loop thread, so deliver directly.
                sub._deliver(self._cache)
        return sub

    def unsubscribe(self, sub: _Subscription) -> None:
        with self._lock:
            self._subs.discard(sub)
            if not self._subs:
                self._stop.set()
                self._wake.set()

    def subscriber_count(self) -> int:
        with self._lock:
            return len(self._subs)

    def _publish(self, entities: List[Any]) -> None:
        with self._lock:
            self._cache = entities
            subs = list(self._subs)
        for sub in subs:
            sub.deliver(entities)

    def _fail_all(self) -> None:
        with self._lock:
            subs = list(self._subs)
        for sub in subs:
            sub.fail()

    def _read(self, kv_store: Any) -> Tuple[List[Any], bool]:
        """Return (current scope entity list, changed?) via a version-map diff."""
        cur: Dict[str, int] = {}
        if self._detail:
            for _k, v in kv_store.get_range_startswith(self._watch_key):
                cur[str(self._entity_id)] = watches.unpack_counter(v)
        else:
            plen = len(self._version_prefix)
            for k, v in kv_store.get_range_startswith(self._version_prefix):
                leaf = bytes(k)[plen:].decode()
                cur[leaf] = watches.unpack_counter(v)

        changed = [leaf for leaf, ver in cur.items() if self._prev_versions.get(leaf) != ver]
        removed = [leaf for leaf in self._prev_versions if leaf not in cur]
        if self._started and not changed and not removed:
            return list(self._entities.values()), False

        for leaf in changed:
            # Point-read only the changed entity; leaf is its get_id() (object-key
            # suffix), so read_from_db(id=leaf) resolves the exact object key.
            rows = self._model_cls().read_from_db(kv_store, id=leaf)
            if rows:
                self._entities[leaf] = rows[0]
            else:
                self._entities.pop(leaf, None)  # raced away between index and read
        for leaf in removed:
            self._entities.pop(leaf, None)
        self._prev_versions = cur
        self._started = True
        return list(self._entities.values()), True

    def _run(self, stop: threading.Event, wake: threading.Event) -> None:
        from simplyblock_core.db_controller import DBController
        kv_store = DBController().kv_store
        set_watch = fdb.transactional(_watch_tx)
        failures = 0
        while not stop.is_set():
            watch = None
            try:
                # Watch first, then read: any write not visible to the read has a
                # commit version at or after the watch's read version, so it fires.
                watch = set_watch(kv_store, self._watch_key)
                entities, changed = self._read(kv_store)
                failures = 0
                if changed or self._cache is _UNSET:
                    self._publish(entities)
                wake.clear()
                # Runs on the FDB network thread: must only set the event.
                watch.on_ready(lambda _f: wake.set())
                wake.wait(timeout=WATCH_RECONCILE_SEC)
            except Exception:
                logger.exception('Watch for %s failed', self._name)
                failures += 1
                if failures > len(_FDB_RETRY_BACKOFF_SEC):
                    self._fail_all()
                    return
                stop.wait(timeout=_FDB_RETRY_BACKOFF_SEC[failures - 1])
            finally:
                if watch is not None:
                    try:
                        watch.cancel()
                    except Exception:
                        logger.debug('Ignoring watch cancellation failure for %s', self._name, exc_info=True)


_scope_watches: Dict[bytes, ScopeWatch] = {}
_scope_watches_lock = threading.Lock()


def get_scope_watch(model_cls: Type[Any], scope: Sequence[Any] = (), entity_id: Optional[str] = None) -> ScopeWatch:
    """Shared :class:`ScopeWatch` for one watch key (rollup for a list, version
    key for a detail). Subscribers of the same scope share one FDB watch + read."""
    scope = tuple(scope)
    if entity_id is not None:
        key = watches.watch_index_version_key(model_cls, scope, entity_id)
    else:
        key = watches.watch_index_rollup_key(model_cls, scope)
    with _scope_watches_lock:
        sw = _scope_watches.get(key)
        if sw is None:
            sw = _scope_watches[key] = ScopeWatch(model_cls, scope, entity_id)
        return sw


def backend_available() -> bool:
    """Whether the watch backend can serve streams (the DB is reachable)."""
    from simplyblock_core.db_controller import DBController
    return DBController().kv_store is not None


async def watch(
        model_cls: Type[Any],
        *,
        scope: Sequence[Any] = (),
        entity_id: Optional[str] = None,
        select: Optional[Select] = None,
        ancestors: Sequence[Tuple[Type[Any], Sequence[Any], str]] = (),
):
    """Async stream of :class:`ChangeEvent` batches for one watched scope.

    ``scope`` is the parent-id path (e.g. ``(pool_id,)`` for a pool's volumes,
    ``()`` for clusters); ``entity_id`` narrows to a single entity's version key
    (detail). ``select`` filters the shared scope entity list to this
    subscription's presentation set (reuse a DBController getter with ``source=``
    so the filter is defined once); the diff runs on its output. ``ancestors`` are
    ``(ParentClass, parent_scope, parent_id)`` triples whose disappearance closes
    the stream (a reconnect then hits the endpoint's regular 404). The first
    yielded batch is the initial state (all ``created``).

    Raises :class:`WatchUnavailable` if the backend fails irrecoverably.
    """
    loop = asyncio.get_running_loop()
    notify = asyncio.Event()
    main_sub = get_scope_watch(model_cls, scope, entity_id).subscribe(loop, notify)
    ancestor_specs = [(pcls, tuple(pscope), str(pid)) for pcls, pscope, pid in ancestors]
    ancestor_subs = [
        (get_scope_watch(pcls, pscope, pid).subscribe(loop, notify), pcls, pscope, pid)
        for pcls, pscope, pid in ancestor_specs
    ]
    try:
        prev: Dict[str, dict] = {}
        while True:
            await notify.wait()
            notify.clear()

            if main_sub.failed or any(sub.failed for sub, _, _, _ in ancestor_subs):
                raise WatchUnavailable()

            for ancestor_sub, _, _, _ in ancestor_subs:
                ancestor_state = ancestor_sub.take()
                if ancestor_state is _UNSET:
                    continue
                if not ancestor_state:  # parent's detail watch delivered []: gone
                    return

            models = main_sub.take()
            if models is _UNSET:
                continue
            events, prev = await loop.run_in_executor(
                None, _compute_batch, models, select, prev)
            yield events
    finally:
        get_scope_watch(model_cls, scope, entity_id).unsubscribe(main_sub)
        for ancestor_sub, pcls, pscope, pid in ancestor_subs:
            get_scope_watch(pcls, pscope, pid).unsubscribe(ancestor_sub)


def _compute_batch(
        models: List[Any], select: Optional[Select], prev: Dict[str, dict],
) -> Tuple[List[ChangeEvent], Dict[str, dict]]:
    full = {m.get_id(): m for m in models}
    selected = select(models) if select is not None else models
    new = {m.get_id(): m for m in selected}
    return diff_by_id(prev, new, full)
