# coding=utf-8
"""SSE framing for the v2 API watch streams (``?watch=true``).

Consumes a controller's :class:`~simplyblock_core.watch.ChangeEvent` stream and
frames it as Server-Sent Events: a ``snapshot`` event with the current state
first, then ``created``/``updated``/``deleted`` events carrying the full
resource representation. A ``deleted`` event carries the resource's final state
when it is still retrievable, or an empty object once it is gone entirely.

This layer owns only the wire concerns — building DTOs, suppressing changes that
are invisible in the DTO, stream lifetime, ping/retry, and the SSE envelope. It
knows nothing about FoundationDB, watches, filtering, or raw records; those live
in :mod:`simplyblock_core.watch` and the controllers.
"""

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Tuple

from fastapi import HTTPException, Query
from sse_starlette import EventSourceResponse, ServerSentEvent
from starlette.concurrency import run_in_threadpool
from typing_extensions import Annotated

from simplyblock_core.watch import ChangeEvent, WatchUnavailable, backend_available

logger = logging.getLogger(__name__)

# Builds the wire DTO from a model. May do blocking DB reads — only ever called
# via run_in_threadpool.
DTOBuilder = Callable[[Any], Any]

# Streams are closed after this long; bearer tokens are only validated at request
# start, so this bounds how long a revoked token keeps a live stream (clients
# re-authenticate on reconnect, like Kubernetes watch timeouts).
WATCH_MAX_LIFETIME_SEC = 3600.0

SSE_RETRY_MS = 3000
PING_SEC = 15

# OpenAPI ``responses`` snippet for routes that also serve ``?watch=true``.
WATCH_RESPONSES: Dict[Any, Any] = {
    200: {'content': {'text/event-stream': {'schema': {'type': 'string'}}}},
}

# Query-parameter documentation, shared by all watchable routes.
WATCH_PARAM_DESCRIPTION = (
    'Stream state changes as Server-Sent Events instead of returning a plain '
    'response: a `snapshot` event with the current state first, then '
    '`created`/`updated`/`deleted` events carrying the full resource '
    'representation. A `deleted` event carries the resource\'s final state '
    'when it is still retrievable (e.g. a volume whose status became '
    '`deleted`), or an empty object once it is gone entirely. Streams do not '
    'support resume; reconnecting clients receive a fresh snapshot. Changes '
    'written by pre-upgrade components may take up to 30 seconds to appear.'
)

# ``watch: WatchParam = False`` on a route adds the documented query flag.
WatchParam = Annotated[bool, Query(description=WATCH_PARAM_DESCRIPTION)]


def _build_snapshot(
        events: List[ChangeEvent], build_dto: DTOBuilder, single: bool,
) -> Tuple[List[ServerSentEvent], Dict[str, str]]:
    """Render the initial batch (all ``created``) as one ``snapshot`` event."""
    cache = {event.id: build_dto(event.model).model_dump_json() for event in events}
    if single:
        if not cache:
            return [], cache
        data = next(iter(cache.values()))
    else:
        data = '[' + ','.join(cache[event.id] for event in events) + ']'
    return [ServerSentEvent(event='snapshot', data=data, retry=SSE_RETRY_MS)], cache


def _build_events(
        events: List[ChangeEvent], cache: Dict[str, str], build_dto: DTOBuilder, single: bool,
) -> Tuple[List[ServerSentEvent], Dict[str, str], bool]:
    """Render a diff batch as individual ``created``/``updated``/``deleted`` events."""
    out: List[ServerSentEvent] = []
    new_cache = dict(cache)
    deleted = False
    for event in events:
        if event.kind == 'deleted':
            deleted = True
            if event.model is not None:
                try:
                    data = build_dto(event.model).model_dump_json()
                except Exception:
                    logger.exception('Failed to build DTO for deleted entity %s', event.id)
                    data = new_cache.get(event.id, '{}')
            else:
                data = '{}'
            new_cache.pop(event.id, None)
            out.append(ServerSentEvent(event='deleted', data=data))
        else:
            data = build_dto(event.model).model_dump_json()
            if event.kind == 'updated' and data == new_cache.get(event.id):
                # Change invisible in the DTO (e.g. a JobSchedule lease
                # heartbeat rewrite): suppress it.
                continue
            new_cache[event.id] = data
            out.append(ServerSentEvent(event='updated' if single else event.kind, data=data))
    return out, new_cache, deleted


def sse_response(change_stream, build_dto: DTOBuilder, *, single: bool = False) -> EventSourceResponse:
    """Frame a controller's ChangeEvent stream as an SSE response.

    ``change_stream`` is an async iterator of ``List[ChangeEvent]`` batches (the
    first batch is the initial state). ``build_dto`` converts a model into its
    wire DTO. ``single`` switches to detail semantics: a single-object snapshot
    and closing the stream once the entity is deleted.
    """
    if not backend_available():
        raise HTTPException(503, 'Database unavailable')

    async def event_stream() -> Any:
        dto_cache: Dict[str, str] = {}
        first = True
        deadline = time.monotonic() + WATCH_MAX_LIFETIME_SEC
        try:
            while True:
                timeout = deadline - time.monotonic()
                if timeout <= 0:
                    return
                try:
                    events = await asyncio.wait_for(change_stream.__anext__(), timeout)
                except asyncio.TimeoutError:
                    return
                except StopAsyncIteration:
                    return
                except WatchUnavailable:
                    yield ServerSentEvent(event='error', data='{"detail": "backend unavailable"}')
                    return

                if first:
                    first = False
                    sse_events, dto_cache = await run_in_threadpool(
                        _build_snapshot, events, build_dto, single)
                    if single and not dto_cache:
                        # Deleted between dependency resolution and first
                        # publication; close so the reconnect 404s.
                        return
                    for event in sse_events:
                        yield event
                else:
                    sse_events, dto_cache, deleted = await run_in_threadpool(
                        _build_events, events, dto_cache, build_dto, single)
                    for event in sse_events:
                        yield event
                    if single and deleted:
                        return
        finally:
            await change_stream.aclose()

    return EventSourceResponse(
        event_stream(),
        ping=PING_SEC,
        headers={'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no'},
    )
