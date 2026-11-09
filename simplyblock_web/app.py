
import logging
import os
import re
import ssl
import sys
import time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.wsgi import WSGIMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.middleware.base import BaseHTTPMiddleware
from uvicorn.config import Config

from simplyblock_core import constants
from simplyblock_core import utils as core_utils
from simplyblock_core.controllers import control_plane_controller
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


class AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else '-'
        request_size = request.headers.get('content-length', '-')

        # Query strings can carry credentials (?secret=…, ?token=…) and have
        # no type info to mask by, so log the path only.
        path = request.url.path

        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000

        response_size = response.headers.get('content-length', '-')

        access_logger.info(
            '%s %s',
            request.method,
            path,
            extra={
                'client_ip': client_ip,
                'request_size': request_size,
                'status_code': response.status_code,
                'response_size': response_size,
                'duration_ms': duration_ms,
            },
        )
        return response


_CLUSTER_IN_PATH = re.compile(r'/api/v(?:1/cluster|2/clusters)/([0-9a-fA-F-]{36})(?:/|$)')


class StretchedControlPlaneMiddleware(BaseHTTPMiddleware):
    """A control plane that does not lead its stretched cluster
    (docs/stretched-control-plane.md) answers reads itself, sends mutating
    calls to the leader's API with a 307, and answers 503 while it has no
    FoundationDB at all. On a deployment without stretched clusters the
    verdict is "serve" at the cost of one cached read per second."""

    async def dispatch(self, request: Request, call_next):
        match = _CLUSTER_IN_PATH.search(request.url.path)
        verdict = control_plane_controller.api_verdict(
            request.method, request.url.path, match.group(1) if match else "")
        if verdict.action == "redirect":
            location = verdict.location
            if (query := str(request.query_params)):
                location += f'?{query}'
            return RedirectResponse(url=location, status_code=307)
        if verdict.action == "unavailable":
            return JSONResponse(status_code=503, content={
                "error": "Control plane unavailable",
                "detail": verdict.message,
            })
        return await call_next(request)


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

app.add_middleware(AccessLogMiddleware)
app.add_middleware(StretchedControlPlaneMiddleware)

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
