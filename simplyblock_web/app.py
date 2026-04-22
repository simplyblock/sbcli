#!/usr/bin/env python
# encoding: utf-8

import logging
import sys
import time

from fastapi import FastAPI, Request
from fastapi.middleware.wsgi import WSGIMiddleware
from fastapi.responses import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
import uvicorn
from uvicorn.config import Config

from simplyblock_web.api import public, v1
from simplyblock_core import constants, utils as core_utils

logger = core_utils.get_logger(__name__)
logger.setLevel(constants.LOG_WEB_LEVEL)
logging.getLogger().setLevel(constants.LOG_WEB_LEVEL)

access_logger = logging.getLogger('simplyblock_web.access')
_access_handler = logging.StreamHandler(stream=sys.stdout)
_access_handler.setFormatter(logging.Formatter(
    '%(asctime)s: %(thread)d: %(levelname)s: %(message)s'
    ' %(status_code)s %(duration_ms).2fms'
))
access_logger.addHandler(_access_handler)
access_logger.propagate = False


core_utils.init_sentry_sdk()


class AccessLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000
        access_logger.info(
            '%s %s',
            request.method,
            request.url.path,
            extra={
                'status_code': response.status_code,
                'duration_ms': duration_ms,
            },
        )
        return response


app: FastAPI = FastAPI()
app.add_middleware(AccessLogMiddleware)
app.include_router(public, prefix='/api')
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
    """
    Redirect legacy API routes to their corresponding v1 endpoints.
    
    Args:
        request: The incoming HTTP request
        
    Returns:
        RedirectResponse: A 308 Permanent Redirect to the v1 API endpoint
    """
    redirect_url: str = f'/api/v1/{request.url.path}'
    if (query_params := str(request.query_params)):
        redirect_url += f'?{query_params}'
    return RedirectResponse(url=redirect_url, status_code=308)


def main() -> None:
    """
    Main entry point for running the FastAPI application.
    """
    config: Config = uvicorn.Config(
        app=app,
        host='0.0.0.0',
        port=5000,
        log_level='debug',
        access_log=False,
        proxy_headers=True,
        forwarded_allow_ips='192.168.1.0/24'
    )
    server: uvicorn.Server = uvicorn.Server(config)
    server.run()


if __name__ == '__main__':
    main()
