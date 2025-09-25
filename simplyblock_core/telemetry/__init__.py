"""OpenTelemetry integration utilities for Simplyblock."""
from __future__ import annotations

import logging
import os
import socket
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Mapping, Optional

from opentelemetry import logs, metrics, trace
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.resources import SERVICE_NAME, SERVICE_VERSION
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    AlwaysOffSampler,
    AlwaysOnSampler,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)
from opentelemetry.trace import Span, Status, StatusCode

from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as GrpcSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HttpSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as GrpcMetricExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HttpMetricExporter
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as GrpcLogExporter
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter as HttpLogExporter

from simplyblock_core.telemetry import logging as telemetry_logging

_logger = logging.getLogger(__name__)
_STATE: Optional["TelemetryState"] = None
_REQUESTS_INSTRUMENTED = False


def _env_bool(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _parse_headers(raw_headers: Optional[str]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if not raw_headers:
        return headers
    for item in raw_headers.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers


def _resolve_metric_interval() -> float:
    raw = os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000")
    try:
        millis = float(raw)
    except ValueError:
        _logger.warning("Invalid OTEL_METRIC_EXPORT_INTERVAL '%s', falling back to 60000", raw)
        millis = 60000.0
    return max(millis / 1000.0, 5.0)


def _resolve_sampler(name: str, ratio: float) -> Sampler:
    normalized = (name or "parentbased_always_on").lower()

    if normalized in {"always_on", "alway-on"}:
        return AlwaysOnSampler()
    if normalized in {"always_off", "alway-off"}:
        return AlwaysOffSampler()
    if normalized in {"traceidratio", "traceidratio-based", "traceidratio_based"}:
        return TraceIdRatioBased(ratio)
    if normalized in {"parentbased_traceidratio", "parentbased_traceidratio-based", "parentbased_traceidratio_based"}:
        return ParentBased(TraceIdRatioBased(ratio))
    if normalized in {"parentbased_always_off", "parentbased-always-off"}:
        return ParentBased(AlwaysOffSampler())
    # Default to parentbased always on
    return ParentBased(AlwaysOnSampler())


@dataclass(frozen=True)
class TelemetrySettings:
    service_name: str
    service_version: str
    protocol: str
    endpoint: Optional[str]
    traces_endpoint: Optional[str]
    metrics_endpoint: Optional[str]
    logs_endpoint: Optional[str]
    headers: Dict[str, str]
    sampler: str
    sampler_arg: float
    log_level: str
    metric_interval: float
    enable_traces: bool
    enable_metrics: bool
    enable_logs: bool
    auto_instrument_requests: bool
    export_timeout: Optional[int]


@dataclass
class TelemetryInstruments:
    request_counter: Any
    request_duration: Any
    request_errors: Any
    db_duration: Any
    db_errors: Any
    external_duration: Any
    external_errors: Any
    cli_counter: Any
    cli_duration: Any
    cli_errors: Any


@dataclass
class TelemetryState:
    settings: TelemetrySettings
    resource: Resource
    tracer_provider: Optional[TracerProvider]
    meter_provider: Optional[MeterProvider]
    logger_provider: Optional[LoggerProvider]
    instruments: Optional[TelemetryInstruments]
    initialized_at: float = field(default_factory=time.time)


def _build_settings(service_name: Optional[str], auto_instrument_requests: bool) -> TelemetrySettings:
    sampler = os.getenv("OTEL_TRACES_SAMPLER", "parentbased_always_on")
    try:
        sampler_arg = float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1.0"))
    except ValueError:
        sampler_arg = 1.0

    protocol = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc").lower()

    return TelemetrySettings(
        service_name=service_name
        or os.getenv("OTEL_SERVICE_NAME")
        or os.getenv("SIMPLYBLOCK_SERVICE_NAME")
        or "simplyblock",
        service_version=os.getenv("SIMPLY_BLOCK_VERSION", "unknown"),
        protocol=protocol,
        endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
        traces_endpoint=os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
        metrics_endpoint=os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
        logs_endpoint=os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
        headers=_parse_headers(os.getenv("OTEL_EXPORTER_OTLP_HEADERS")),
        sampler=sampler,
        sampler_arg=sampler_arg,
        log_level=os.getenv("OTEL_LOG_LEVEL", os.getenv("SIMPLYBLOCK_LOG_LEVEL", "INFO")),
        metric_interval=_resolve_metric_interval(),
        enable_traces=_env_bool("SIMPLYBLOCK_TELEMETRY_TRACES_ENABLED", True),
        enable_metrics=_env_bool("SIMPLYBLOCK_TELEMETRY_METRICS_ENABLED", True),
        enable_logs=_env_bool("SIMPLYBLOCK_TELEMETRY_LOGS_ENABLED", True),
        auto_instrument_requests=auto_instrument_requests
        and _env_bool("SIMPLYBLOCK_TELEMETRY_REQUESTS_ENABLED", True),
        export_timeout=int(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "0")) or None,
    )


def _create_resource(settings: TelemetrySettings) -> Resource:
    attrs: Dict[str, Any] = {
        SERVICE_NAME: settings.service_name,
        SERVICE_VERSION: settings.service_version,
        "service.instance.id": os.getenv("HOSTNAME", socket.gethostname()),
    }
    return Resource.create(attrs)


def _create_trace_exporter(settings: TelemetrySettings):
    endpoint = settings.traces_endpoint or settings.endpoint
    kwargs: Dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint"] = endpoint
        if endpoint.startswith("http://") and settings.protocol.startswith("grpc"):
            kwargs["insecure"] = True
    if settings.headers:
        kwargs["headers"] = settings.headers
    if settings.export_timeout:
        kwargs["timeout"] = settings.export_timeout

    if settings.protocol.startswith("http"):
        return HttpSpanExporter(**kwargs)
    return GrpcSpanExporter(**kwargs)


def _create_metric_exporter(settings: TelemetrySettings):
    endpoint = settings.metrics_endpoint or settings.endpoint
    kwargs: Dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint"] = endpoint
        if endpoint.startswith("http://") and settings.protocol.startswith("grpc"):
            kwargs["insecure"] = True
    if settings.headers:
        kwargs["headers"] = settings.headers
    if settings.export_timeout:
        kwargs["timeout"] = settings.export_timeout

    if settings.protocol.startswith("http"):
        return HttpMetricExporter(**kwargs)
    return GrpcMetricExporter(**kwargs)


def _create_log_exporter(settings: TelemetrySettings):
    endpoint = settings.logs_endpoint or settings.endpoint
    kwargs: Dict[str, Any] = {}
    if endpoint:
        kwargs["endpoint"] = endpoint
        if endpoint.startswith("http://") and settings.protocol.startswith("grpc"):
            kwargs["insecure"] = True
    if settings.headers:
        kwargs["headers"] = settings.headers
    if settings.export_timeout:
        kwargs["timeout"] = settings.export_timeout

    if settings.protocol.startswith("http"):
        return HttpLogExporter(**kwargs)
    return GrpcLogExporter(**kwargs)


def _configure_requests_instrumentation() -> None:
    global _REQUESTS_INSTRUMENTED
    if _REQUESTS_INSTRUMENTED:
        return
    try:
        RequestsInstrumentor().instrument()
        _REQUESTS_INSTRUMENTED = True
    except Exception as exc:  # pragma: no cover - defensive
        _logger.warning("Failed to instrument requests library: %s", exc)


def init_observability(service_name: Optional[str] = None, *, auto_instrument_requests: bool = True) -> TelemetryState:
    """Initialize the OpenTelemetry stack for the application."""
    global _STATE
    if _STATE is not None:
        return _STATE

    settings = _build_settings(service_name, auto_instrument_requests)
    resource = _create_resource(settings)

    tracer_provider: Optional[TracerProvider] = None
    if settings.enable_traces:
        tracer_provider = TracerProvider(resource=resource, sampler=_resolve_sampler(settings.sampler, settings.sampler_arg))
        try:
            trace_exporter = _create_trace_exporter(settings)
            tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
        except Exception as exc:  # pragma: no cover - defensive
            _logger.warning("Failed to configure trace exporter: %s", exc)
        trace.set_tracer_provider(tracer_provider)

    meter_provider: Optional[MeterProvider] = None
    instruments: Optional[TelemetryInstruments] = None
    if settings.enable_metrics:
        try:
            metric_exporter = _create_metric_exporter(settings)
            reader = PeriodicExportingMetricReader(
                metric_exporter,
                export_interval_millis=int(settings.metric_interval * 1000),
            )
            meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
        except Exception as exc:  # pragma: no cover - defensive
            _logger.warning("Failed to configure metric exporter: %s", exc)
            meter_provider = MeterProvider(resource=resource)
        metrics.set_meter_provider(meter_provider)
        meter = meter_provider.get_meter(settings.service_name)
        instruments = TelemetryInstruments(
            request_counter=meter.create_counter(
                name="simplyblock_http_request_count",
                description="Number of HTTP requests processed",
            ),
            request_duration=meter.create_histogram(
                name="simplyblock_http_request_duration_ms",
                unit="ms",
                description="Duration of HTTP requests",
            ),
            request_errors=meter.create_counter(
                name="simplyblock_http_request_error_count",
                description="Number of HTTP requests that resulted in an error",
            ),
            db_duration=meter.create_histogram(
                name="simplyblock_db_operation_duration_ms",
                unit="ms",
                description="Duration of database operations",
            ),
            db_errors=meter.create_counter(
                name="simplyblock_db_operation_error_count",
                description="Number of failing database operations",
            ),
            external_duration=meter.create_histogram(
                name="simplyblock_external_call_duration_ms",
                unit="ms",
                description="Duration of external service calls",
            ),
            external_errors=meter.create_counter(
                name="simplyblock_external_call_error_count",
                description="Number of failing external service calls",
            ),
            cli_counter=meter.create_counter(
                name="simplyblock_cli_command_count",
                description="CLI commands executed",
            ),
            cli_duration=meter.create_histogram(
                name="simplyblock_cli_command_duration_ms",
                unit="ms",
                description="Duration of CLI command execution",
            ),
            cli_errors=meter.create_counter(
                name="simplyblock_cli_command_error_count",
                description="Count of CLI commands that failed",
            ),
        )

    logger_provider: Optional[LoggerProvider] = None
    if settings.enable_logs:
        logger_provider = LoggerProvider(resource=resource)
        try:
            log_exporter = _create_log_exporter(settings)
            logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
        except Exception as exc:  # pragma: no cover - defensive
            _logger.warning("Failed to configure log exporter: %s", exc)
        logs.set_logger_provider(logger_provider)
        root_logger = logging.getLogger()
        if not any(isinstance(handler, LoggingHandler) for handler in root_logger.handlers):
            root_logger.addHandler(LoggingHandler(level=logging.NOTSET, logger_provider=logger_provider))

    telemetry_logging.configure_json_logging(settings.log_level)

    if settings.auto_instrument_requests:
        _configure_requests_instrumentation()

    _STATE = TelemetryState(
        settings=settings,
        resource=resource,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
        instruments=instruments,
    )
    return _STATE


def ensure_initialized() -> TelemetryState:
    return init_observability()


def get_tracer(name: str) -> trace.Tracer:
    ensure_initialized()
    return trace.get_tracer(name)


def get_meter(name: str) -> metrics.Meter:
    ensure_initialized()
    return metrics.get_meter(name)


@contextmanager
def start_span(name: str, **attributes: Any) -> Generator[Span, None, None]:
    tracer = get_tracer(name)
    with tracer.start_as_current_span(name) as span:
        if attributes:
            span.set_attributes(attributes)
        yield span


def _record_histogram(metric, value: float, attributes: Optional[Mapping[str, Any]] = None) -> None:
    try:
        metric.record(value, attributes=attributes)
    except Exception as exc:  # pragma: no cover - defensive
        _logger.debug("Failed to record histogram %s: %s", getattr(metric, "name", ""), exc)


def _record_counter(metric, value: float, attributes: Optional[Mapping[str, Any]] = None) -> None:
    try:
        metric.add(value, attributes=attributes)
    except Exception as exc:  # pragma: no cover - defensive
        _logger.debug("Failed to record counter %s: %s", getattr(metric, "name", ""), exc)


def record_http_request(*, method: str, route: str, status_code: int, duration_sec: float) -> None:
    state = ensure_initialized()
    if not state.instruments:
        return
    attrs = {
        "http.request.method": method.upper(),
        "http.route": route,
        "http.response.status_code": status_code,
    }
    _record_counter(state.instruments.request_counter, 1, attrs)
    _record_histogram(state.instruments.request_duration, duration_sec * 1000.0, attrs)
    if status_code >= 500:
        _record_counter(state.instruments.request_errors, 1, attrs)


def record_db_operation(*, name: str, duration_sec: float, success: bool = True) -> None:
    state = ensure_initialized()
    if not state.instruments:
        return
    attrs = {"db.operation": name}
    _record_histogram(state.instruments.db_duration, duration_sec * 1000.0, attrs)
    if not success:
        _record_counter(state.instruments.db_errors, 1, attrs)


def record_external_call(*, name: str, duration_sec: float, status_code: Optional[int] = None, success: bool = True) -> None:
    state = ensure_initialized()
    if not state.instruments:
        return
    attrs = {"external.service": name}
    if status_code is not None:
        attrs["external.status_code"] = status_code
    _record_histogram(state.instruments.external_duration, duration_sec * 1000.0, attrs)
    if not success:
        _record_counter(state.instruments.external_errors, 1, attrs)


def record_cli_command(*, name: str, duration_sec: float, success: bool = True) -> None:
    state = ensure_initialized()
    if not state.instruments:
        return
    attrs = {"cli.command": name}
    _record_counter(state.instruments.cli_counter, 1, attrs)
    _record_histogram(state.instruments.cli_duration, duration_sec * 1000.0, attrs)
    if not success:
        _record_counter(state.instruments.cli_errors, 1, attrs)


def mark_span_failure(span: Span, exc: BaseException) -> None:
    if span and span.is_recording():
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))


def instrument_fastapi(app) -> None:
    """Instrument a FastAPI instance for tracing and metrics."""
    ensure_initialized()
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        if not getattr(app.state, "_sb_fastapi_instrumented", False):
            FastAPIInstrumentor.instrument_app(app)
            setattr(app.state, "_sb_fastapi_instrumented", True)
    except Exception as exc:  # pragma: no cover - defensive
        _logger.warning("FastAPI instrumentation failed: %s", exc)

    try:
        from simplyblock_core.telemetry.middleware import TelemetryMiddleware

        if not any(getattr(m, "cls", None) is TelemetryMiddleware for m in getattr(app, "user_middleware", [])):
            app.add_middleware(TelemetryMiddleware)
    except Exception as exc:  # pragma: no cover - defensive
        _logger.warning("Failed to attach telemetry middleware: %s", exc)
