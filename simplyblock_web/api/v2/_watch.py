# coding=utf-8
"""Entity watching for the v2 API (SSE ``?watch=true``).

Change signal: every write/remove of a watched model class atomically bumps
``watch_seq/<ClassName>`` in the same FDB transaction as the entity mutation
(see ``simplyblock_core.watches``). One shared :class:`EntityWatchHub` per
model class owns a single FDB watch on that counter key; when it fires, the
hub re-reads the class's entity range once and fans the parsed state out to
every subscribed stream. Scoping (e.g. volumes of one pool) happens per
subscription, via the same filter its list endpoint applies — N scoped
subscribers cost one watch and one shared range read per change.

The hub registers the watch *before* reading the range: any write not visible
to the range read has a commit version at or after the watch's read version
and therefore fires it, so no update is lost. A reconcile timeout re-reads
the range even without a watch fire, bounding staleness for writes from
pre-upgrade components that don't bump counters.
"""

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

import fdb
from fastapi import HTTPException, Query
from sse_starlette import EventSourceResponse, ServerSentEvent
from starlette.concurrency import run_in_threadpool
from typing_extensions import Annotated

from simplyblock_core import watches
from simplyblock_core.db_controller import DBController

logger = logging.getLogger(__name__)

# Raw FDB key -> parsed entity dict. Keyed by the FDB key (not the uuid
# field): unique by construction and indifferent to compound hierarchical ids
# (JobSchedule keys are object/JobSchedule/<cluster>/<date>/<uuid>).
RawState = Dict[Any, dict]

# Filter applied per subscription; must match the corresponding list endpoint.
Projection = Callable[[RawState], RawState]

# Builds the wire DTO from a parsed entity dict. May do blocking DB reads —
# only ever called via run_in_threadpool.
DTOBuilder = Callable[[dict], Any]

# Upper bound on staleness for entity writes that don't bump the change
# counter (processes from before the counter was introduced).
WATCH_RECONCILE_SEC = 30.0

# Streams are closed after this long; bearer tokens are only validated at
# request start, so this bounds how long a revoked token keeps a live stream
# (clients re-authenticate on reconnect, like Kubernetes watch timeouts).
WATCH_MAX_LIFETIME_SEC = 3600.0

_FDB_RETRY_BACKOFF_SEC = (1.0, 2.0, 4.0)
SSE_RETRY_MS = 3000
PING_SEC = 15

_UNSET = object()

# OpenAPI ``responses`` snippet for routes that also serve ``?watch=true``.
WATCH_RESPONSES: Dict[Any, Any] = {
    200: {'content': {'text/event-stream': {'schema': {'type': 'string'}}}},
}

# Query-parameter documentation, shared by all watchable routes.
WATCH_PARAM_DESCRIPTION = (
    'Stream state changes as Server-Sent Events instead of returning a plain '
    'response: a `snapshot` event with the current state first, then '
    '`created`/`updated`/`deleted` events carrying the full resource '
    'representation. Streams do not support resume; reconnecting clients '
    'receive a fresh snapshot. Changes written by pre-upgrade components may '
    'take up to 30 seconds to appear.'
)

# ``watch: WatchParam = False`` on a route adds the documented query flag.
WatchParam = Annotated[bool, Query(description=WATCH_PARAM_DESCRIPTION)]


class _Subscription:
    """Coalescing mailbox: written by the hub thread, read by one stream."""

    def __init__(self, loop: asyncio.AbstractEventLoop, notify: asyncio.Event) -> None:
        self._loop = loop
        self._notify = notify
        self._latest: Any = _UNSET
        self._dirty = False
        self.failed = False

    def _deliver(self, state: RawState) -> None:  # event-loop thread
        self._latest = state
        self._dirty = True
        self._notify.set()

    def _fail(self) -> None:  # event-loop thread
        self.failed = True
        self._notify.set()

    def deliver(self, state: RawState) -> None:  # hub thread
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


class EntityWatchHub:
    """Single FDB watch + shared range re-read for one watched model class."""

    def __init__(self, model_cls: Type[Any]) -> None:
        instance = model_cls()
        self._name = model_cls.__name__
        # Built from parts rather than get_db_id(): a fresh JobSchedule's
        # compound get_id() is not empty, which would corrupt the prefix.
        self._prefix = f'{instance.object_type}/{instance.name}/'.encode()
        self._counter_key = watches.watch_counter_key(model_cls)
        self._lock = threading.Lock()
        self._subs: set = set()
        self._cache: Any = _UNSET
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def subscribe(self, loop: asyncio.AbstractEventLoop, notify: asyncio.Event) -> _Subscription:
        sub = _Subscription(loop, notify)
        with self._lock:
            alive = self._thread is not None and self._thread.is_alive() and not self._stop.is_set()
            if not alive:
                self._cache = _UNSET
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

    def _publish(self, state: RawState) -> None:
        with self._lock:
            self._cache = state
            subs = list(self._subs)
        for sub in subs:
            sub.deliver(state)

    def _fail_all(self) -> None:
        with self._lock:
            subs = list(self._subs)
        for sub in subs:
            sub.fail()

    def _read_range(self, kv_store: Any) -> RawState:
        state: RawState = {}
        for k, v in kv_store.get_range_startswith(self._prefix):
            try:
                state[bytes(k)] = json.loads(v)
            except ValueError:
                logger.warning('Skipping unparsable record %s', bytes(k))
        return state

    def _run(self, stop: threading.Event, wake: threading.Event) -> None:
        kv_store = DBController().kv_store
        set_watch = fdb.transactional(_watch_tx)
        failures = 0
        while not stop.is_set():
            watch = None
            try:
                # Watch first, then read: see module docstring.
                watch = set_watch(kv_store, self._counter_key)
                state = self._read_range(kv_store)
                failures = 0
                with self._lock:
                    changed = self._cache is _UNSET or state != self._cache
                if changed:
                    self._publish(state)
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


_hubs: Dict[type, EntityWatchHub] = {}
_hubs_lock = threading.Lock()


def get_hub(model_cls: Type[Any]) -> EntityWatchHub:
    with _hubs_lock:
        hub = _hubs.get(model_cls)
        if hub is None:
            hub = _hubs[model_cls] = EntityWatchHub(model_cls)
        return hub


def _build_snapshot(
        filtered: RawState, build_dto: DTOBuilder, single: bool,
) -> Tuple[List[ServerSentEvent], Dict[Any, str]]:
    cache = {key: build_dto(entity).model_dump_json() for key, entity in filtered.items()}
    if single:
        if not cache:
            return [], cache
        data = next(iter(cache.values()))
    else:
        data = '[' + ','.join(cache[key] for key in sorted(cache)) + ']'
    return [ServerSentEvent(event='snapshot', data=data, retry=SSE_RETRY_MS)], cache


def _build_diff(
        prev: RawState, new: RawState, cache: Dict[Any, str],
        build_dto: DTOBuilder, single: bool,
) -> Tuple[List[ServerSentEvent], Dict[Any, str]]:
    events: List[ServerSentEvent] = []
    new_cache: Dict[Any, str] = {}
    for key, entity in new.items():
        if key not in prev:
            new_cache[key] = build_dto(entity).model_dump_json()
            events.append(ServerSentEvent(event='updated' if single else 'created', data=new_cache[key]))
        elif entity != prev[key]:
            new_cache[key] = build_dto(entity).model_dump_json()
            # Suppress changes invisible in the DTO (e.g. JobSchedule's
            # updated_at lease heartbeat rewrites).
            if new_cache[key] != cache.get(key):
                events.append(ServerSentEvent(event='updated', data=new_cache[key]))
        else:
            new_cache[key] = cache[key]
    for key in prev:
        if key not in new:
            events.append(ServerSentEvent(event='deleted', data=cache.get(key, '{}')))
    return events, new_cache


def watch_response(
        model_cls: Type[Any],
        project: Projection,
        build_dto: DTOBuilder,
        single_id: Optional[str] = None,
        ancestors: Sequence[Tuple[Type[Any], str]] = (),
) -> EventSourceResponse:
    """SSE stream of state changes for one watched entity class.

    ``project`` must apply the same filter as the corresponding list endpoint;
    the diff runs on its output, so entities entering/leaving the filtered set
    emit ``created``/``deleted``. ``single_id`` switches to detail semantics
    (single-DTO snapshot, close after ``deleted``). ``ancestors`` are
    ``(model_cls, id)`` pairs from the route's dependency chain: the stream
    closes when any of them disappears, so a reconnect gets the endpoint's
    regular 404.
    """
    if DBController().kv_store is None:
        raise HTTPException(503, 'Database unavailable')

    single = single_id is not None
    ancestor_specs = [(cls, str(ancestor_id)) for cls, ancestor_id in ancestors]

    async def event_stream() -> Any:
        loop = asyncio.get_running_loop()
        notify = asyncio.Event()
        main_sub = get_hub(model_cls).subscribe(loop, notify)
        ancestor_subs = [
            (get_hub(cls).subscribe(loop, notify), cls, ancestor_id)
            for cls, ancestor_id in ancestor_specs
        ]
        try:
            prev: Optional[RawState] = None
            dto_cache: Dict[Any, str] = {}
            deadline = time.monotonic() + WATCH_MAX_LIFETIME_SEC
            while True:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    return
                try:
                    await asyncio.wait_for(notify.wait(), timeout)
                except asyncio.TimeoutError:
                    return
                notify.clear()

                if main_sub.failed or any(sub.failed for sub, _, _ in ancestor_subs):
                    yield ServerSentEvent(event='error', data='{"detail": "backend unavailable"}')
                    return

                for ancestor_sub, _, ancestor_id in ancestor_subs:
                    ancestor_state = ancestor_sub.take()
                    if ancestor_state is _UNSET:
                        continue
                    if not any(entity.get('uuid') == ancestor_id for entity in ancestor_state.values()):
                        return

                state = main_sub.take()
                if state is _UNSET:
                    continue
                filtered = await run_in_threadpool(project, state)
                if prev is None:
                    if single and not filtered:
                        # Deleted between dependency resolution and first
                        # publication; close so the reconnect 404s.
                        return
                    events, dto_cache = await run_in_threadpool(
                        _build_snapshot, filtered, build_dto, single)
                else:
                    events, dto_cache = await run_in_threadpool(
                        _build_diff, prev, filtered, dto_cache, build_dto, single)
                prev = filtered
                for event in events:
                    yield event
                if single and not filtered:
                    return
        finally:
            get_hub(model_cls).unsubscribe(main_sub)
            for ancestor_sub, cls, _ in ancestor_subs:
                get_hub(cls).unsubscribe(ancestor_sub)

    return EventSourceResponse(
        event_stream(),
        ping=PING_SEC,
        headers={'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no'},
    )
