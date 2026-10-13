
import logging
import math
import os
import ssl
import sys
import time

import anyio
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.wsgi import WSGIMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from prometheus_fastapi_instrumentator import Instrumentator
from uvicorn.config import Config

from simplyblock_core import constants
from simplyblock_core import utils as core_utils
from simplyblock_core.exceptions import NodeTransitionInProgress, PreconditionError
from simplyblock_core.settings import Settings
from simplyblock_web.api import v1, v2
from simplyblock_web.settings import Settings as WebSettings

logger = core_utils.get_logger(__name__)
logger.setLevel(constants.LOG_WEB_LEVEL)
logging.getLogger().setLevel(constants.LOG_WEB_LEVEL)

# Prevent external libraries from logging secrets (tokens, response bodies)
# at DEBUG level while keeping our own loggers at DEBUG.
for _ext_logger_name in (
    "kubernetes.client.rest",
    "urllib3",
):
    logging.getLogger(_ext_logger_name).setLevel(logging.WARNING)

access_logger = logging.getLogger('simplyblock_web.access')
_access_handler = logging.StreamHandler(stream=sys.stdout)
_access_handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s %(client_ip)s'
    ' "%(message)s" %(status_code)s %(request_size)s %(response_size)s %(duration_ms).2fms'
))
access_logger.addHandler(_access_handler)
access_logger.propagate = False


core_utils.init_sentry_sdk()


class AccessLogMiddleware:
    """One line per served HTTP request, in place of uvicorn's access log.

    Pure ASGI, not a ``starlette.middleware.base.BaseHTTPMiddleware``
    subclass: that base class buffers the downstream response through an
    internal anyio memory-object-stream plus a nested per-request task group
    so it can hand ``dispatch()`` a real ``Response`` object. A cancellation
    race in that machinery (SFAM-2821 — a client disconnect or timeout racing
    the internal receive_event) can leave a request's task stuck forever
    awaiting an event nothing will ever set, wedging every future request
    behind it without the process crashing or even logging again. Plain ASGI
    middleware only wraps ``send``; no concurrent task reads the body
    independently, so it carries none of that risk.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        client = scope.get('client')
        client_ip = client[0] if client else '-'
        request_size = '-'
        for name, value in scope.get('headers', ()):
            if name == b'content-length':
                request_size = value.decode('latin-1')
                break

        # Query strings can carry credentials (?secret=…, ?token=…) and have
        # no type info to mask by, so log the path only.
        path = scope['path']
        method = scope['method']

        status_code = 500
        response_size = '-'

        async def logging_send(message):
            nonlocal status_code, response_size
            if message['type'] == 'http.response.start':
                status_code = message['status']
                for name, value in message.get('headers', ()):
                    if name == b'content-length':
                        response_size = value.decode('latin-1')
                        break
            await send(message)

        start = time.monotonic()
        try:
            await self.app(scope, receive, logging_send)
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            access_logger.info(
                '%s %s',
                method,
                path,
                extra={
                    'client_ip': client_ip,
                    'request_size': request_size,
                    'status_code': status_code,
                    'response_size': response_size,
                    'duration_ms': duration_ms,
                },
            )


class RequestTimeoutMiddleware:
    """Bound how long a request may run before it sends its first response byte.

    SFAM-2821: a cancellation race deep in anyio/Starlette's request
    machinery can leave a request's task permanently stuck awaiting an event
    that is never set, without crashing or otherwise disturbing the rest of
    the event loop — every other in-flight and future request then just
    queues up behind a worker slot that never frees, until something notices
    the API stopped answering. Bounding time-to-first-byte here turns that
    into a single failed (504) request instead of a silent, permanent outage.

    The guard is lifted the moment headers are sent, so a long-lived response
    (the SSE watch endpoints in api/v2) is unaffected once it starts
    streaming — only the time to *start* responding is bounded.
    """

    def __init__(self, app, timeout_seconds):
        self.app = app
        self.timeout_seconds = timeout_seconds

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        response_started = False

        async def guarded_send(message):
            nonlocal response_started
            if message['type'] == 'http.response.start':
                response_started = True
                cancel_scope.deadline = math.inf
            await send(message)

        with anyio.CancelScope() as cancel_scope:
            cancel_scope.deadline = anyio.current_time() + self.timeout_seconds
            await self.app(scope, receive, guarded_send)

        if cancel_scope.cancelled_caught and not response_started:
            logger.warning(
                "Request timed out after %ss with no response: %s %s",
                self.timeout_seconds, scope.get('method'), scope.get('path'),
            )
            await send({
                'type': 'http.response.start',
                'status': 504,
                'headers': [(b'content-type', b'application/json')],
            })
            await send({
                'type': 'http.response.body',
                'body': b'{"error":"Request timed out"}',
            })


app: FastAPI = FastAPI()
Instrumentator().instrument(app).expose(app, endpoint="/_meta/metrics")

@app.exception_handler(PreconditionError)
async def precondition_handler(request: Request, exc: PreconditionError):
    logger.exception("Preciondition checks failed", exc_info=exc)
    return JSONResponse(status_code=400, content={
        "error": "Preconditions are not met",
        "detail": str(exc),
    })


@app.exception_handler(NodeTransitionInProgress)
async def node_transition_handler(request: Request, exc: NodeTransitionInProgress):
    logger.info("Node transition in progress: %s", exc)
    return JSONResponse(status_code=503, content={
        "error": "Node transition in progress; retry",
        "detail": str(exc),
    })


@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    logger.exception("Unexcpected error while processing request", exc_info=exc)
    return JSONResponse(status_code=500, content={
        "status": "An error occured while processing the request",
        "detail": str(exc),
    })


_web_settings = WebSettings()

app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=_web_settings.request_timeout_seconds)
app.add_middleware(AccessLogMiddleware)

if 2 in _web_settings.api_versions:
    app.include_router(v2.api, prefix='/api/v2')

if 1 in _web_settings.api_versions:
    app.mount('/api/v1', WSGIMiddleware(v1.api))  # For some reason this fails if done in `api/__init__.py`

    @app.api_route('/', methods=['GET'])
    @app.api_route('/cluster/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/mgmtnode/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/device/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/lvol/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/snapshot/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/storagenode/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    @app.api_route('/pool/{full_path:path}', methods=['GET', 'POST', 'PUT', 'DELETE'])
    def redirect_legacy(request: Request) -> RedirectResponse:
        redirect_url: str = f'/api/v1/{request.url.path}'
        if (query_params := str(request.query_params)):
            redirect_url += f'?{query_params}'
        return RedirectResponse(url=redirect_url, status_code=308)


def main() -> None:
    """
    Main entry point for running the FastAPI application.
    """
    settings = Settings()
    config: Config = uvicorn.Config(
        app=app,
        host='0.0.0.0',
        port=int(os.environ.get('FLASK_PORT', 5000)),
        log_level=constants.LOG_WEB_LEVEL,
        access_log=False,
        proxy_headers=True,
        forwarded_allow_ips='192.168.1.0/24',
        ssl_certfile=settings.tls_certificate if settings.tls_serve else None,
        ssl_keyfile=settings.tls_key if settings.tls_serve else None,
        ssl_ca_certs=settings.tls_certificate_authority if settings.tls_client_auth != ssl.CERT_NONE else None,
        ssl_cert_reqs=settings.tls_client_auth,
    )
    server: uvicorn.Server = uvicorn.Server(config)
    server.run()


if __name__ == '__main__':
    main()
