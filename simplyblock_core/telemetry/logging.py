"""Logging utilities for Simplyblock telemetry integration."""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

from opentelemetry import trace

_LOGGING_CONFIGURED = False
_JSON_HANDLER_ATTR = "_sb_is_json_handler"


def _now_iso8601() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonLogFormatter(logging.Formatter):
    """Minimal JSON formatter that adds trace/span ids when present."""

    _BASE_KEYS: Iterable[str] = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
    }

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: Dict[str, Any] = {
            "timestamp": _now_iso8601(),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        span = trace.get_current_span()
        span_context = span.get_span_context() if span else None
        if span_context and span_context.is_valid:
            payload["trace_id"] = format(span_context.trace_id, "032x")
            payload["span_id"] = format(span_context.span_id, "016x")

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        for key, value in record.__dict__.items():
            if key in self._BASE_KEYS or key.startswith("_"):
                continue
            payload.setdefault("attributes", {})[key] = value

        return json.dumps(payload, default=str, ensure_ascii=True)


def configure_json_logging(level: str | int | None = None) -> None:
    """Configure the root logger with a JSON formatter and stdout handler."""
    global _LOGGING_CONFIGURED

    root_logger = logging.getLogger()
    if isinstance(level, str):
        numeric_level = logging.getLevelName(level.upper())
        if isinstance(numeric_level, str):
            numeric_level = logging.INFO
    elif isinstance(level, int):
        numeric_level = level
    else:
        numeric_level = logging.getLevelName(os.getenv("OTEL_LOG_LEVEL", "INFO").upper())
        if isinstance(numeric_level, str):
            numeric_level = logging.INFO

    root_logger.setLevel(numeric_level)

    if _LOGGING_CONFIGURED:
        return

    for handler in root_logger.handlers:
        if getattr(handler, _JSON_HANDLER_ATTR, False):
            _LOGGING_CONFIGURED = True
            return

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    setattr(handler, _JSON_HANDLER_ATTR, True)
    root_logger.addHandler(handler)
    _LOGGING_CONFIGURED = True

    logging.captureWarnings(True)
