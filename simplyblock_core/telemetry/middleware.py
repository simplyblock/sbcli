"""Starlette/FastAPI middleware for telemetry metrics."""
from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from opentelemetry import trace


class TelemetryMiddleware(BaseHTTPMiddleware):
    """Capture HTTP metrics for each request."""

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Any]],
    ):
        start_time = time.perf_counter()
        status_code = 500
        response = None
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            duration = time.perf_counter() - start_time
            route = getattr(request.scope.get("route"), "path", request.url.path)
            method = request.method

            current_span = trace.get_current_span()
            if current_span and current_span.is_recording():
                current_span.set_attribute("http.route", route)

            from simplyblock_core.telemetry import record_http_request  # local import to avoid circular dependency
            record_http_request(
                method=method,
                route=route,
                status_code=status_code,
                duration_sec=duration,
            )

            # FastAPI expects the response to be returned; ensure we propagate it
