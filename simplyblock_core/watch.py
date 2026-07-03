# coding=utf-8
"""Generic entity-watch primitive for streaming state changes.

Change signal: every write/remove of a watched model class atomically bumps
``watch_seq/<ClassName>`` in the same FDB transaction as the entity mutation
(see :mod:`simplyblock_core.watches`). One shared :class:`WatchHub` per model
class owns a single FDB watch on that counter key; when it fires, the hub
re-reads the class's entity range once and fans the parsed models out to every
subscriber. Scoping (e.g. volumes of one pool) happens per subscription via a
``select`` callable, so N scoped subscribers cost one watch and one shared range
read per change.

The hub registers the watch *before* reading the range: any write not visible to
the range read has a commit version at or after the watch's read version and
therefore fires it, so no update is lost. A reconcile timeout re-reads the range
even without a watch fire, bounding staleness for writes from pre-upgrade
components that don't bump counters.

This module owns the *mechanism* only. It emits typed :class:`ChangeEvent`
batches (``created``/``updated``/``deleted`` over model objects); building wire
DTOs and framing them as SSE is the web layer's concern (``_sse.py``). Keeping
the change-detection here — behind the ``ChangeEvent`` interface — means the
counter-and-re-read backend can later be swapped for a version-index or
change-log backend without touching controllers or the API.
"""

import asyncio
import json
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

import fdb

from simplyblock_core import watches

logger = logging.getLogger(__name__)

# Upper bound on staleness for entity writes that don't bump the change counter
# (processes from before the counter was introduced).
WATCH_RECONCILE_SEC = 30.0

_FDB_RETRY_BACKOFF_SEC = (1.0, 2.0, 4.0)

_UNSET: Any = object()

# Filters the shared full model list down to one subscription's scope. Returns
# model objects (typically by reusing a DBController getter with ``source=``).
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
    """Coalescing mailbox: written by the hub thread, read by one stream."""

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

    def deliver(self, state: Any) -> None:  # hub thread
        self._loop.call_soon_threadsafe(self._deliver, state)

    def fail(self) -> None:  # hub thread
        self._loop.call_soon_threadsafe(self._fail)

    def take(self) -> Any:
        """Latest state if there is an unconsumed publication, else ``_UNSET``."""
        if not self._dirty:
            return _UNSET
        self._dirty = False
        return self._latest


def _watch_tx(tr: Any, key: bytes) -> Any:
    return tr.watch(key)


class WatchHub:
    """Single FDB watch + shared range re-read for one watched model class.

    Delivers ``List[model]`` (the full current class range) to subscribers.
    Change detection compares the *raw* parsed records (cheap) so the reconcile
    re-read only republishes on a real change; models are constructed only when
    something changed and shared across all subscribers.
    """

    def __init__(self, model_cls: Type[Any]) -> None:
        instance = model_cls()
        self._model_cls = model_cls
        self._name = model_cls.__name__
        # Built from parts rather than get_db_id(): a fresh JobSchedule's
        # compound get_id() is not empty, which would corrupt the prefix.
        self._prefix = f'{instance.object_type}/{instance.name}/'.encode()
        self._counter_key = watches.watch_counter_key(model_cls)
        self._lock = threading.Lock()
        self._subs: set = set()
        self._cache: Any = _UNSET   # last published List[model] (for late joiners)
        self._raw: Any = _UNSET     # last raw dict[key, dict] (for change detection)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def subscribe(self, loop: asyncio.AbstractEventLoop, notify: asyncio.Event) -> _Subscription:
        sub = _Subscription(loop, notify)
        with self._lock:
            alive = self._thread is not None and self._thread.is_alive() and not self._stop.is_set()
            if not alive:
                self._cache = _UNSET
                self._raw = _UNSET
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

    def _publish(self, raw: Dict[Any, dict], models: List[Any]) -> None:
        with self._lock:
            self._raw = raw
            self._cache = models
            subs = list(self._subs)
        for sub in subs:
            sub.deliver(models)

    def _fail_all(self) -> None:
        with self._lock:
            subs = list(self._subs)
        for sub in subs:
            sub.fail()

    def _read_raw(self, kv_store: Any) -> Dict[Any, dict]:
        state: Dict[Any, dict] = {}
        for k, v in kv_store.get_range_startswith(self._prefix):
            try:
                state[bytes(k)] = json.loads(v)
            except ValueError:
                logger.warning('Skipping unparsable record %s', bytes(k))
        return state

    def _to_models(self, raw: Dict[Any, dict]) -> List[Any]:
        return [self._model_cls().from_dict(d) for d in raw.values()]

    def _run(self, stop: threading.Event, wake: threading.Event) -> None:
        from simplyblock_core.db_controller import DBController
        kv_store = DBController().kv_store
        set_watch = fdb.transactional(_watch_tx)
        failures = 0
        while not stop.is_set():
            watch = None
            try:
                # Watch first, then read: see module docstring.
                watch = set_watch(kv_store, self._counter_key)
                raw = self._read_raw(kv_store)
                failures = 0
                with self._lock:
                    changed = self._raw is _UNSET or raw != self._raw
                if changed:
                    self._publish(raw, self._to_models(raw))
                wake.clear()
                # Runs on the FDB network thread: must only set the event,
                # never block or call back into fdb.
                watch.on_ready(lambda _f: wake.set())
                wake.wait(timeout=WATCH_RECONCILE_SEC)
            except Exception:
                logger.exception('Watch hub for %s failed', self._name)
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
                        # Best-effort cleanup: cancellation failures should not
                        # mask the main loop error/retry behavior.
                        logger.debug('Ignoring watch cancellation failure for %s', self._name, exc_info=True)


_hubs: Dict[type, WatchHub] = {}
_hubs_lock = threading.Lock()


def get_hub(model_cls: Type[Any]) -> WatchHub:
    with _hubs_lock:
        hub = _hubs.get(model_cls)
        if hub is None:
            hub = _hubs[model_cls] = WatchHub(model_cls)
        return hub


def backend_available() -> bool:
    """Whether the watch backend can serve streams (the DB is reachable)."""
    from simplyblock_core.db_controller import DBController
    return DBController().kv_store is not None


async def watch(
        model_cls: Type[Any],
        *,
        select: Optional[Select] = None,
        ancestors: Sequence[Tuple[Type[Any], str]] = (),
):
    """Async stream of :class:`ChangeEvent` batches for one watched class.

    ``select`` filters the shared full model list to this subscription's scope
    (reuse a DBController getter with ``source=`` so the filter is defined once);
    the diff runs on its output, so entities entering/leaving the scoped set emit
    ``created``/``deleted``. ``ancestors`` are ``(model_cls, id)`` pairs whose
    disappearance closes the stream (a reconnect then hits the endpoint's regular
    404). The first yielded batch is the initial state (all ``created``).

    Raises :class:`WatchUnavailable` if the backend fails irrecoverably.
    """
    loop = asyncio.get_running_loop()
    notify = asyncio.Event()
    main_sub = get_hub(model_cls).subscribe(loop, notify)
    ancestor_subs = [
        (get_hub(cls).subscribe(loop, notify), cls, str(ancestor_id))
        for cls, ancestor_id in ancestors
    ]
    try:
        prev: Dict[str, dict] = {}
        while True:
            await notify.wait()
            notify.clear()

            if main_sub.failed or any(sub.failed for sub, _, _ in ancestor_subs):
                raise WatchUnavailable()

            for ancestor_sub, _, ancestor_id in ancestor_subs:
                ancestor_state = ancestor_sub.take()
                if ancestor_state is _UNSET:
                    continue
                if not any(m.get_id() == ancestor_id for m in ancestor_state):
                    return

            models = main_sub.take()
            if models is _UNSET:
                continue
            # select() filtering and to_dict() diffing are CPU-bound; keep them
            # off the event loop. Selects must not do blocking DB reads (they
            # filter the supplied model list) — DTO building, which may, stays
            # in the web layer's threadpool.
            events, prev = await loop.run_in_executor(
                None, _compute_batch, models, select, prev)
            yield events
    finally:
        get_hub(model_cls).unsubscribe(main_sub)
        for ancestor_sub, cls, _ in ancestor_subs:
            get_hub(cls).unsubscribe(ancestor_sub)


def _compute_batch(
        models: List[Any], select: Optional[Select], prev: Dict[str, dict],
) -> Tuple[List[ChangeEvent], Dict[str, dict]]:
    full = {m.get_id(): m for m in models}
    selected = select(models) if select is not None else models
    new = {m.get_id(): m for m in selected}
    return diff_by_id(prev, new, full)
