"""simplyblock_web.app's pure-ASGI middleware (SFAM-2821).

AccessLogMiddleware and RequestTimeoutMiddleware are deliberately plain ASGI
middleware rather than starlette.middleware.base.BaseHTTPMiddleware
subclasses: that base class's internal anyio memory-object-stream plus
per-request task group is the mechanism a cancellation race can wedge
forever, taking the whole server down with it. These tests drive the ASGI
callables directly, with no FDB and no real server involved.
"""

import asyncio
import logging

import pytest

from simplyblock_web.app import AccessLogMiddleware, RequestTimeoutMiddleware


def make_scope(path='/', method='GET', headers=(), query_string=b''):
    return {
        'type': 'http',
        'method': method,
        # A real ASGI server (uvicorn) never puts the query string in `path`
        # - it's carried separately in `query_string`.
        'path': path,
        'query_string': query_string,
        'headers': list(headers),
        'client': ('127.0.0.1', 12345),
    }


async def drive(app, scope):
    sent = []

    async def receive():
        return {'type': 'http.disconnect'}

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    return sent


async def respond_ok(scope, receive, send):
    await send({'type': 'http.response.start', 'status': 200, 'headers': [(b'content-length', b'2')]})
    await send({'type': 'http.response.body', 'body': b'ok'})


class TestAccessLogMiddleware:
    def test_logs_one_line_with_the_right_fields(self, caplog):
        middleware = AccessLogMiddleware(respond_ok)
        with caplog.at_level(logging.INFO, logger='simplyblock_web.access'):
            asyncio.run(drive(middleware, make_scope(
                path='/foo', headers=[(b'content-length', b'5')])))

        record, = caplog.records
        assert record.getMessage() == 'GET /foo'
        assert record.status_code == 200
        assert record.request_size == '5'
        assert record.response_size == '2'
        assert record.client_ip == '127.0.0.1'
        assert record.duration_ms >= 0

    def test_query_string_is_not_logged(self, caplog):
        middleware = AccessLogMiddleware(respond_ok)
        with caplog.at_level(logging.INFO, logger='simplyblock_web.access'):
            asyncio.run(drive(middleware, make_scope(path='/foo', query_string=b'token=hunter2')))

        record, = caplog.records
        assert record.getMessage() == 'GET /foo'
        assert 'hunter2' not in str(record.__dict__)

    def test_non_http_scope_passes_through_unlogged(self, caplog):
        calls = []

        async def lifespan_app(scope, receive, send):
            calls.append(scope['type'])

        middleware = AccessLogMiddleware(lifespan_app)
        with caplog.at_level(logging.INFO, logger='simplyblock_web.access'):
            asyncio.run(drive(middleware, {'type': 'lifespan'}))

        assert calls == ['lifespan']
        assert caplog.records == []

    def test_logs_even_when_the_downstream_app_raises(self, caplog):
        async def boom(scope, receive, send):
            raise RuntimeError('boom')

        middleware = AccessLogMiddleware(boom)
        with caplog.at_level(logging.INFO, logger='simplyblock_web.access'):
            with pytest.raises(RuntimeError):
                asyncio.run(drive(middleware, make_scope()))

        record, = caplog.records
        assert record.status_code == 500  # no response was ever sent


class TestRequestTimeoutMiddleware:
    def test_a_fast_response_is_untouched(self):
        middleware = RequestTimeoutMiddleware(respond_ok, timeout_seconds=1)

        sent = asyncio.run(drive(middleware, make_scope()))

        start = next(m for m in sent if m['type'] == 'http.response.start')
        assert start['status'] == 200

    def test_a_hung_handler_is_cancelled_and_gets_a_504(self, caplog):
        cancelled = False

        async def hang(scope, receive, send):
            nonlocal cancelled
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled = True
                raise

        middleware = RequestTimeoutMiddleware(hang, timeout_seconds=0.05)

        with caplog.at_level(logging.WARNING):
            sent = asyncio.run(drive(middleware, make_scope(path='/slow')))

        start = next(m for m in sent if m['type'] == 'http.response.start')
        assert start['status'] == 504
        assert cancelled, "the downstream task was not cancelled"
        assert any('timed out' in r.getMessage() for r in caplog.records)

    def test_a_response_that_already_started_is_never_cut_off(self):
        """The SSE watch endpoints (api/v2) stream far longer than any sane
        request timeout - the guard must be lifted once headers are sent."""
        async def slow_stream(scope, receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            for i in range(3):
                await asyncio.sleep(0.05)  # > the timeout below, per chunk
                await send({'type': 'http.response.body', 'body': str(i).encode(), 'more_body': True})
            await send({'type': 'http.response.body', 'body': b'', 'more_body': False})

        middleware = RequestTimeoutMiddleware(slow_stream, timeout_seconds=0.03)

        sent = asyncio.run(drive(middleware, make_scope(path='/watch')))

        starts = [m for m in sent if m['type'] == 'http.response.start']
        assert len(starts) == 1 and starts[0]['status'] == 200
        bodies = [m['body'] for m in sent if m['type'] == 'http.response.body']
        assert bodies == [b'0', b'1', b'2', b'']

    def test_non_http_scope_passes_through_unaffected(self):
        calls = []

        async def lifespan_app(scope, receive, send):
            calls.append(scope['type'])

        middleware = RequestTimeoutMiddleware(lifespan_app, timeout_seconds=1)
        asyncio.run(drive(middleware, {'type': 'lifespan'}))

        assert calls == ['lifespan']
