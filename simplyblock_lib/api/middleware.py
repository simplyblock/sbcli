# coding=utf-8
"""Shared ASGI middleware."""

import logging
import sys
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

ACCESS_LOG_FORMAT = (
    '%(asctime)s %(levelname)s %(client_ip)s'
    ' "%(message)s" %(status_code)s %(request_size)s %(response_size)s %(duration_ms).2fms'
)


def build_access_logger(name='simplyblock.access', stream=sys.stdout):
    """Create (or reconfigure) a non-propagating access logger with the shared
    format. Idempotent: an existing handler set is left untouched."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(stream=stream)
        handler.setFormatter(logging.Formatter(ACCESS_LOG_FORMAT))
        logger.addHandler(handler)
    logger.propagate = False
    return logger


class AccessLogMiddleware(BaseHTTPMiddleware):
    """Request/response access log that never logs query strings.

    Query strings can carry credentials (?secret=…, ?token=…) and have no type
    info to mask by, so only the path is logged.
    """

    def __init__(self, app, logger=None):
        super().__init__(app)
        self._logger = logger or build_access_logger()

    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else '-'
        request_size = request.headers.get('content-length', '-')

        path = request.url.path

        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000

        response_size = response.headers.get('content-length', '-')

        self._logger.info(
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
