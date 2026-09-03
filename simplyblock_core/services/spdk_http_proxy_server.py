# coding=utf-8
"""HTTP front-end for SPDK's JSON-RPC unix socket.

Storage nodes run one instance of this next to every SPDK process. It accepts
basic-auth'd JSON-RPC POSTs (see ``simplyblock_core.rpc_client.RPCClient``) and
forwards the raw body to ``/mnt/ramdisk/spdk_<port>/spdk.sock``.

Importing this module has no side effects: configuration is read by
``ProxySettings``, the application is built by ``create_app`` and only ``main``
(the ``__main__`` entry point used by the deployment manifests) binds a port.
"""

import asyncio
import base64
import dataclasses
import functools
import hmac
import json
import logging
import math
import ssl
import sys
import time
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncGenerator, ClassVar, Dict, Optional, Set, Tuple

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BeforeValidator, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import ClientDisconnect

from simplyblock_core.settings import Settings
from simplyblock_core.utils.secrets import redact_rpc_params


logger = logging.getLogger(__name__)
#: Carries the access log alone, so its extra fields can be rendered by name.
#: The handler is attached by ``_configure_logging``.
access_logger = logging.getLogger(f'{__name__}.access')

#: How often the periodic timing report is emitted, and hence the interval
#: every figure in it covers.
STATS_INTERVAL_SEC = 3
#: Path the Prometheus exposition is served on, matching ``simplyblock_web``.
METRICS_ENDPOINT = '/_meta/metrics'
#: SPDK round-trips span a warm ``bdev_get_bdevs`` (well under a millisecond)
#: to ``ProxySettings.timeout``, 300s by default. Histogram's default buckets
#: stop at 10s, which would collapse the whole interesting tail into ``+Inf``:
#: RPCs issued under the port fence sit around 8s, and a slot starved by a
#: stuck ``distr_status_events_update`` holds for minutes. Hence the extra
#: resolution between 5s and 10s, and a ladder that reaches the timeout.
SPDK_DURATION_BUCKETS = (
    .001, .005, .01, .05, .1, .5, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300, math.inf)
#: Reading a request body off a loopback socket; a different order of
#: magnitude from anything SPDK does, so a separate, much finer ladder.
BODY_READ_DURATION_BUCKETS = (
    .0001, .0005, .001, .005, .01, .05, .1, .5, 1, math.inf)
#: Ceiling on distinct ``method`` label values. The method is read out of the
#: caller's JSON-RPC body, so a buggy or hostile client could otherwise mint
#: series without bound. A cap rather than an allowlist: SPDK's RPC surface is
#: hundreds of names and version-dependent, so an allowlist would silently
#: drop methods added by an SPDK upgrade.
MAX_METHOD_LABELS = 128
MAX_METHOD_LABEL_LEN = 64
#: Where methods past those limits are folded.
OTHER_METHOD_LABEL = 'other'
#: Per-attempt bound on the readiness probe, and the pause between attempts.
SPDK_READY_PROBE_TIMEOUT_SEC = 5
SPDK_READY_POLL_INTERVAL_SEC = 1
#: SPDK responses are read in one shot; matches the pre-FastAPI recv() size.
SPDK_RECV_SIZE = 1024 * 1024 * 1024


def _rpc_port_or_default(value: Any) -> Any:
    """Fall back to 8080 for an unparsable ``RPC_PORT``.

    Legacy behaviour, kept deliberately: deployments that pass a non-numeric
    port have always silently landed on 8080 rather than failing to start.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return 8080


class ProxySettings(BaseSettings):
    """Environment configuration of the proxy.

    The variable names carry no ``SB_`` prefix — they are baked into the
    deployed manifests (``simplyblock_web/templates/storage_deploy_spdk.yaml.j2``)
    and into the docker launch path
    (``simplyblock_web/api/internal/storage_node/docker.py``).
    """

    model_config = SettingsConfigDict(case_sensitive=False)

    server_ip: Annotated[str, Field(description="Address the HTTP server binds to")]
    rpc_port: Annotated[
        int,
        Field(description="Port the HTTP server binds to; also selects the SPDK unix socket"),
        BeforeValidator(_rpc_port_or_default),
    ]
    rpc_username: str
    rpc_password: SecretStr
    timeout: Annotated[
        float,
        Field(gt=0, description="Upper bound on how long a single SPDK round-trip may take"),
    ] = 5 * 60
    max_concurrent_spdk: Annotated[
        int,
        Field(gt=0, description="Number of RPCs allowed to be in flight against SPDK at once"),
    ] = 16
    max_concurrent_connections: Annotated[
        int,
        Field(
            gt=0,
            description=(
                "Cap on concurrent HTTP connections. Above max_concurrent_spdk since a "
                "kept-alive connection can sit idle, not doing SPDK work, most of the time. "
                "Enforced by uvicorn, which answers 503 past the cap rather than waiting for "
                "a slot the way the ThreadingHTTPServer this replaced blocked in accept()."
            ),
        ),
    ] = 64
    keepalive_timeout: Annotated[
        int,
        Field(
            gt=0,
            description=(
                "Seconds an idle HTTP connection is kept open. The server this replaced spoke "
                "HTTP/1.0 and closed after every response, so a connection could never be "
                "dropped underneath a client about to reuse it. RPCClient deliberately keeps "
                "POST out of its urllib3 retry set, so such a drop surfaces as a failed RPC "
                "rather than a retry - hence a window far longer than the gap between RPCs to "
                "a node, at the cost of idle connections holding a max_concurrent_connections "
                "slot for that long. Was 60s before the move to uvicorn."
            ),
        ),
    ] = 300
    spdk_timeout_margin: Annotated[
        float,
        Field(
            gt=0,
            description=(
                "Multiplier applied to the caller-supplied HTTP timeout (X-RPC-Timeout header) "
                "to derive how long the proxy waits on SPDK while holding a concurrency slot. "
                ">1 so a request that completes just after the caller's deadline still returns "
                "instead of being aborted; small enough that abandoned/stuck RPCs free their "
                "slot quickly."
            ),
        ),
    ] = 2.0
    multi_threading_enabled: Annotated[
        bool,
        Field(
            description=(
                "Serve RPCs concurrently. When false the proxy forwards one RPC at a time, "
                "matching the single-threaded HTTPServer this used to run on."
            )
        ),
    ] = False

    rpc_sock_path: Annotated[
        Optional[str],
        Field(description=(
            "Path of SPDK's JSON-RPC unix socket. Defaults to the location SPDK "
            "binds for this RPC_PORT, which is what every deployment uses."
        )),
    ] = None

    @property
    def rpc_sock(self) -> str:
        return self.rpc_sock_path or f"/mnt/ramdisk/spdk_{self.rpc_port}/spdk.sock"

    @functools.cached_property
    def authorization(self) -> str:
        """The ``Authorization`` header value clients have to present.

        Cached: it is compared on every request, including the ones on the
        data path, and neither the credentials nor their encoding change over
        the process' life.
        """
        credentials = f"{self.rpc_username}:{self.rpc_password.get_secret_value()}"
        return 'Basic ' + base64.b64encode(credentials.encode('ascii')).decode('ascii')


#: ``(count, sum)`` per ``method`` label, and cumulative bucket counts by
#: upper bound, as read back off a histogram.
HistogramSnapshot = Tuple[Dict[str, Tuple[float, float]], Dict[float, float]]


def _histogram_snapshot(histogram: Histogram) -> HistogramSnapshot:
    """Read a histogram's own totals back out of its samples."""
    per_method: Dict[str, Tuple[float, float]] = {}
    buckets: Dict[float, float] = {}
    for metric in histogram.collect():
        for sample in metric.samples:
            method = sample.labels.get('method', '')
            count, total = per_method.get(method, (0.0, 0.0))
            if sample.name.endswith('_bucket'):
                bound = float(sample.labels['le'])
                buckets[bound] = buckets.get(bound, 0.0) + sample.value
            elif sample.name.endswith('_count'):
                per_method[method] = (count + sample.value, total)
            elif sample.name.endswith('_sum'):
                per_method[method] = (count, total + sample.value)
    return per_method, buckets


def _bucket_quantile(buckets: Dict[float, float], quantile: float) -> Optional[float]:
    """Upper bound of the bucket a quantile falls into.

    Buckets are cumulative, and the difference of two cumulative readings is
    itself cumulative, so this works unchanged on an interval delta.
    """
    if not buckets:
        return None
    ordered = sorted(buckets.items())
    total = ordered[-1][1]
    if total <= 0:
        return None
    target = total * quantile
    for bound, cumulative in ordered:
        if cumulative >= target:
            return bound
    return ordered[-1][0]


def _format_seconds(seconds: float) -> str:
    return f"{seconds * 1000:.1f}ms" if seconds < 1 else f"{seconds:.2f}s"


def _format_quantile(buckets: Dict[float, float], seconds: Optional[float]) -> str:
    """Render a quantile as the comparison it actually is.

    A histogram resolves a quantile only to a bucket bound, so the figure can
    exceed the largest duration actually observed. Spelling the bound as
    ``<=`` keeps that from reading as ``p99 > max``, which is otherwise the
    obvious conclusion when both sit on the same line.
    """
    if seconds is None:
        return '=-'
    if not math.isinf(seconds):
        return f"<={_format_seconds(seconds)}"
    finite = [bound for bound in buckets if not math.isinf(bound)]
    return f">{_format_seconds(max(finite))}" if finite else '=inf'


class IntervalReport:
    """The periodic log line for one histogram.

    Everything reported comes from the histogram itself, so the log and the
    Prometheus exposition can never disagree. Histograms are cumulative, so
    interval figures are the difference between successive reads.

    The exception is ``max``: a histogram knows only bucket boundaries, and
    the Python client's Summary carries no quantiles, so the peak is kept here
    as a plain float and reset every tick. Deliberately not a Gauge -- a gauge
    reset every few seconds sawtooths and reads badly in Prometheus.
    """

    def __init__(self, name: str, histogram: Histogram) -> None:
        self.name = name
        self.histogram = histogram
        self._prev_methods: Dict[str, Tuple[float, float]] = {}
        self._prev_buckets: Dict[float, float] = {}
        self._peak_seconds = 0.0

    def observe(self, seconds: float, method: Optional[str] = None) -> None:
        target = self.histogram if method is None else self.histogram.labels(method=method)
        target.observe(seconds)
        self._peak_seconds = max(self._peak_seconds, seconds)

    def _slowest_method(self, methods: Dict[str, Tuple[float, float]]) -> Optional[str]:
        """The method with the highest mean duration over the interval."""
        slowest, slowest_mean = None, 0.0
        for method, (count, total) in methods.items():
            prev_count, prev_total = self._prev_methods.get(method, (0.0, 0.0))
            if (advanced := count - prev_count) <= 0:
                continue
            if (mean := (total - prev_total) / advanced) > slowest_mean:
                slowest, slowest_mean = method, mean
        return slowest or None

    def report(self) -> Optional[str]:
        """Summarize the interval, or ``None`` if nothing was observed in it."""
        methods, buckets = _histogram_snapshot(self.histogram)
        advanced = (
            sum(count for count, _ in methods.values())
            - sum(count for count, _ in self._prev_methods.values())
        )
        elapsed = (
            sum(total for _, total in methods.values())
            - sum(total for _, total in self._prev_methods.values())
        )
        interval_buckets = {
            bound: value - self._prev_buckets.get(bound, 0.0)
            for bound, value in buckets.items()
        }
        peak, slowest = self._peak_seconds, self._slowest_method(methods)

        self._prev_methods, self._prev_buckets = methods, buckets
        self._peak_seconds = 0.0

        if advanced <= 0:
            return None

        summary = (
            f"{self.name}:"
            f" interval_avg={_format_seconds(elapsed / advanced)}"
            f" p99{_format_quantile(interval_buckets, _bucket_quantile(interval_buckets, 0.99))}"
            f" max={_format_seconds(peak)}"
            f" n={int(advanced)}"
        )
        return summary if slowest is None else f"{summary} slowest={slowest}"


class ProxyMetrics:
    """Prometheus metrics for one proxy application.

    Each application owns its registry rather than writing into the global
    ``REGISTRY``. A storage node runs exactly one proxy per process so nothing
    is lost, and the tests build several applications in one interpreter,
    where module-level metrics would collide on the second ``create_app``.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._known_methods: Set[str] = set()

        self.spdk_response = IntervalReport('recv_from_spdk', Histogram(
            'spdk_proxy_response_duration_seconds',
            'Time awaiting and reading one JSON-RPC response from SPDK',
            ['method'],
            buckets=SPDK_DURATION_BUCKETS,
            registry=self.registry,
        ))
        self.body_read = IntervalReport('read_body', Histogram(
            'spdk_proxy_body_read_duration_seconds',
            'Time spent reading one HTTP request body',
            buckets=BODY_READ_DURATION_BUCKETS,
            registry=self.registry,
        ))
        self.slots_in_use = Gauge(
            'spdk_proxy_rpc_slots_in_use',
            'SPDK concurrency slots currently held',
            registry=self.registry,
        )
        self.unix_connections = Gauge(
            'spdk_proxy_unix_connections_open',
            'Unix-socket connections currently open towards SPDK',
            registry=self.registry,
        )
        self.failures = Counter(
            'spdk_proxy_rpc_failures_total',
            'RPCs that reached SPDK and did not come back',
            ['method', 'reason'],
            registry=self.registry,
        )

    def method_label(self, method: str) -> str:
        """Fold a caller-supplied method name into the bounded label set."""
        if method in self._known_methods:
            return method
        if len(method) > MAX_METHOD_LABEL_LEN or len(self._known_methods) >= MAX_METHOD_LABELS:
            return OTHER_METHOD_LABEL
        self._known_methods.add(method)
        return method

    def observe_response(self, method: str, seconds: float) -> None:
        self.spdk_response.observe(seconds, method=self.method_label(method))

    def observe_body_read(self, seconds: float) -> None:
        self.body_read.observe(seconds)

    def record_failure(self, method: str, reason: str) -> None:
        self.failures.labels(method=self.method_label(method), reason=reason).inc()

    @property
    def reports(self) -> Tuple[IntervalReport, ...]:
        return (self.body_read, self.spdk_response)


@dataclasses.dataclass
class RequestLog:
    """What one request contributes to its own log lines.

    Threaded through the call rather than logged where each field becomes
    known. Every RPC is a POST to the same path, so an access line that cannot
    name the JSON-RPC method cannot tell two requests apart -- and the method
    is only known once ``rpc_call`` has parsed the body, well inside the
    request. Carrying the id here too gives the pre-flight line (the only
    place the params appear) and the access line one identifier in common.
    """

    request_id: int = dataclasses.field(default_factory=time.time_ns)
    rpc_method: str = '-'


class AccessLogMiddleware(BaseHTTPMiddleware):
    """One line per served request, in place of uvicorn's access log.

    Modelled on ``simplyblock_web.app.AccessLogMiddleware``: uvicorn's own
    line is switched off in ``main`` and this replaces it, so the fields this
    server cares about land on the same record as the status code and the
    duration instead of in log lines of their own.
    """

    #: How ``_configure_logging`` renders the fields ``dispatch`` attaches to
    #: the record. Prefix matches the other lines this process emits; the rest
    #: follows ``simplyblock_web.app``, plus the two fields that are the point
    #: of having our own access log here (the JSON-RPC method, and the id
    #: shared with the request's own log line).
    LOG_FORMAT: ClassVar[str] = (
        '%(asctime)s: %(levelname)s: %(client)s "%(message)s" rpc=%(rpc_method)s'
        ' %(status_code)s req=%(request_size)s resp=%(response_size)s'
        ' %(duration_ms).2fms id=%(request_id)s'
    )

    async def dispatch(
            self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request.state.request_log = log = RequestLog()

        start = time.monotonic()
        response = await call_next(request)
        duration_ms = (time.monotonic() - start) * 1000

        # A scrape is not traffic: Prometheus polls this endpoint for the life
        # of the process, and the exposition it gets back is its own record
        # that it happened.
        if request.url.path == METRICS_ENDPOINT:
            return response

        access_logger.info(
            '%s %s',
            request.method,
            # Query strings can carry credentials and have no type info to
            # mask by, so log the path only.
            request.url.path,
            extra={
                'client': request.client.host if request.client is not None else '-',
                'request_size': request.headers.get('content-length', '-'),
                'status_code': response.status_code,
                'response_size': response.headers.get('content-length', '-'),
                'duration_ms': duration_ms,
                'rpc_method': log.rpc_method,
                'request_id': log.request_id,
            },
        )
        return response


class InvalidRequest(Exception):
    """The request body is not a JSON-RPC request object.

    A caller-side fault, kept distinct from the ``ValueError`` a failed SPDK
    round-trip raises: nothing was sent to SPDK, so it earns a 400 and a
    ``bad_request`` failure rather than a 500 that would read as "SPDK is
    broken" on a status-code or failure-rate dashboard.
    """


def _parse_request(req: bytes) -> Dict[str, Any]:
    """Decode a request body, or reject it as the caller's fault."""
    try:
        req_data = json.loads(req.decode('ascii'))
    except ValueError as e:
        raise InvalidRequest(f"body is not ASCII JSON: {e}") from e

    if not isinstance(req_data, dict) or 'method' not in req_data:
        raise InvalidRequest('body is not a JSON-RPC request object')

    return req_data


class SpdkProxy:
    """Forwards JSON-RPC requests to SPDK's unix socket."""

    def __init__(self, settings: ProxySettings) -> None:
        self.settings = settings
        self.spdk_ready = False
        #: Requests currently being served, and unix sockets currently open
        #: towards SPDK. Asserted on by tests; the operator-facing view of
        #: both is ``spdk_proxy_rpc_slots_in_use`` /
        #: ``spdk_proxy_unix_connections_open``, not a log line.
        self.active_requests = 0
        self.open_connections = 0
        self.metrics = ProxyMetrics()
        # Without MULTI_THREADING_ENABLED the proxy used to run on a
        # non-threading HTTPServer, i.e. one request at a time.
        self.concurrency_limit = (
            settings.max_concurrent_spdk if settings.multi_threading_enabled else 1)
        self._slots: Optional[asyncio.Semaphore] = None
        logger.info("SPDK concurrency limit: %s", self.concurrency_limit)

    @property
    def slots(self) -> asyncio.Semaphore:
        """Gate on the number of RPCs in flight against SPDK.

        Built on first use: up to Python 3.9 a Semaphore binds to whichever
        event loop is running when it is constructed, and the proxy is
        constructed before the server's loop exists.
        """
        if self._slots is None:
            self._slots = asyncio.Semaphore(self.concurrency_limit)
        return self._slots

    async def report_stats(self) -> None:
        """Log the interval timings every ``STATS_INTERVAL_SEC`` seconds.

        Read off the same metrics the Prometheus endpoint serves, so the two
        can never disagree. A series with no observations in the interval
        logs nothing rather than repeating a stale summary.
        """
        while True:
            await asyncio.sleep(STATS_INTERVAL_SEC)
            try:
                for report in self.metrics.reports:
                    if (summary := report.report()) is not None:
                        logger.info("Periodic stats: %s", summary)
            except (ValueError, KeyError, ZeroDivisionError) as e:
                logger.error(f"Could not summarize proxy metrics: {e}")

    async def wait_for_spdk_ready(self) -> None:
        """Block until SPDK responds to spdk_get_version on the unix socket."""
        payload = json.dumps({'id': 1, 'method': 'spdk_get_version'}).encode('ascii')
        while not self.spdk_ready:
            try:
                self.spdk_ready = await asyncio.wait_for(
                    self._probe(payload), SPDK_READY_PROBE_TIMEOUT_SEC)
            except (OSError, asyncio.TimeoutError) as e:
                logger.info(f"Waiting for SPDK to be ready: {e}")

            if self.spdk_ready:
                logger.info("SPDK is ready (spdk_get_version responded)")
                return

            await asyncio.sleep(SPDK_READY_POLL_INTERVAL_SEC)

    async def _probe(self, payload: bytes) -> bool:
        reader, writer = await asyncio.open_unix_connection(self.settings.rpc_sock)
        try:
            writer.write(payload)
            await writer.drain()

            buf = bytearray()
            while (data := await reader.read(4096)) != b'':
                buf += data
                try:
                    json.loads(buf.decode('ascii'))
                except ValueError:
                    continue
                return True
            return False
        finally:
            _close(writer)

    def _resolve_sock_timeout(self, client_timeout: Optional[str]) -> float:
        """Bound the SPDK unix-socket wait (and hence the concurrency-slot hold)
        to a value tied to the CALLER's HTTP timeout, rather than the global
        ``timeout``.

        Each in-flight RPC holds one of ``max_concurrent_spdk`` slots for the
        entire SPDK round-trip. If a slot were always held for the full global
        ``timeout`` (default 300s) while the caller abandons the request after
        its own (often 1-5s) timeout, a handful of never-completing RPCs (e.g. a
        ``distr_status_events_update`` that the distrib can't finish applying)
        would squat every slot for minutes and starve all other RPCs to this
        node — unrelated calls (port_block, bdev_get_bdevs) then never even
        reach SPDK and time out at the caller. Holding the slot only
        ~``spdk_timeout_margin``x longer than the caller waits lets slots
        recycle promptly. Capped at the global ``timeout`` so genuinely long
        operations keep today's budget; falls back to ``timeout`` when the
        caller sends no hint (backward compatible).
        """
        if client_timeout is None:
            return self.settings.timeout
        try:
            ct = float(client_timeout)
        except (TypeError, ValueError):
            return self.settings.timeout
        if ct <= 0:
            return self.settings.timeout
        return min(ct * self.settings.spdk_timeout_margin, self.settings.timeout)

    async def rpc_call(
        self,
        req: bytes,
        client_timeout: Optional[str] = None,
        log: Optional[RequestLog] = None,
    ) -> Optional[str]:
        """Forward one JSON-RPC request, returning SPDK's raw response.

        Returns ``None`` for a request without an ``id`` (a notification, which
        SPDK does not answer).

        ``log`` is filled in as the request is understood, so the access line
        can name what this call turned out to be. A caller that serves no HTTP
        request leaves it out and gets one of its own.
        """
        log = RequestLog() if log is None else log
        # Parsed before the slot is taken, so a malformed request neither
        # occupies one nor gets counted as an SPDK-side failure.
        req_data = _parse_request(req)
        log.rpc_method = str(req_data['method'])
        params = str(redact_rpc_params(req_data['params'])) if 'params' in req_data else ""
        logger.info(
            f"Request:{log.request_id} function: {log.rpc_method}, params: {params}")
        sock_timeout = self._resolve_sock_timeout(client_timeout)
        async with self.slots:
            self.metrics.slots_in_use.inc()
            try:
                return await self._rpc_call_inner(req, req_data, log, sock_timeout)
            finally:
                self.metrics.slots_in_use.dec()

    async def _rpc_call_inner(
        self,
        req: bytes,
        req_data: dict,
        log: RequestLog,
        sock_timeout: float,
    ) -> Optional[str]:
        try:
            return await asyncio.wait_for(self._exchange(req, req_data, log), sock_timeout)
        except asyncio.TimeoutError as e:
            logger.error(
                f"Socket timeout waiting for SPDK response (request {log.request_id}, "
                f"function: {log.rpc_method})")
            self.metrics.record_failure(log.rpc_method, 'timeout')
            raise ValueError('SPDK response timeout') from e
        except OSError:
            self.metrics.record_failure(log.rpc_method, 'unreachable')
            raise
        except ValueError:
            self.metrics.record_failure(log.rpc_method, 'invalid_response')
            raise

    async def _exchange(self, req: bytes, req_data: dict, log: RequestLog) -> Optional[str]:
        self.open_connections += 1
        self.metrics.unix_connections.inc()
        try:
            reader, writer = await asyncio.open_unix_connection(self.settings.rpc_sock)
            try:
                writer.write(req)
                await writer.drain()

                if 'id' not in req_data:
                    return None

                # bytearray, not bytes: it grows in place, where `bytes +=
                # bytes` copies the whole buffer on every chunk.
                buf = bytearray()
                response = None
                # Monotonic: a duration must not be measured against a clock
                # that can step backwards under NTP.
                recv_start = time.monotonic()
                while True:
                    newdata = await reader.read(SPDK_RECV_SIZE)
                    closed = newdata == b''
                    buf += newdata
                    try:
                        response = json.loads(buf.decode('ascii'))
                    except ValueError:
                        if closed:
                            break
                        continue
                    break
                self.metrics.observe_response(log.rpc_method, time.monotonic() - recv_start)

                if not response and len(buf) > 0:
                    raise ValueError('Invalid response')

                return buf.decode('ascii')
            finally:
                _close(writer)
        finally:
            self.open_connections -= 1
            self.metrics.unix_connections.dec()


def _log_task_death(task: "asyncio.Task[None]") -> None:
    """Report a fire-and-forget task that ended on its own.

    Nothing awaits the stats task, so an exception it did not anticipate would
    otherwise surface only as asyncio's "exception was never retrieved"
    warning, at garbage-collection time and detached from the failure.
    """
    if task.cancelled():
        return
    if (error := task.exception()) is not None:
        logger.error(f"Background task {task.get_name()} died: {error}", exc_info=error)
    else:
        logger.error(f"Background task {task.get_name()} returned unexpectedly")


def _close(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
    except OSError:
        pass


def require_authorization(request: Request) -> None:
    """Gate a request on the configured basic-auth credentials.

    Reads the proxy off the request's own application rather than a module
    global, so apps built by separate ``create_app`` calls stay independent.
    """
    settings: ProxySettings = request.app.state.proxy.settings
    authorization = request.headers.get('Authorization')
    # Compared as bytes: compare_digest rejects non-ASCII str outright, and
    # the header is attacker-controlled.
    if authorization is None or not hmac.compare_digest(
            authorization.encode('utf-8'),
            settings.authorization.encode('utf-8'),
    ):
        # The access line records the 401 too, but at INFO and among every
        # other request; a credential that does not match is worth a level
        # something greps for.
        client = request.client.host if request.client is not None else 'unknown'
        logger.warning(f"rejected an unauthorized request from {client}")
        raise HTTPException(status_code=401, headers={'WWW-Authenticate': 'Basic'})


def create_app(settings: ProxySettings) -> FastAPI:
    """Build the proxy application.

    Startup blocks until SPDK answers on its unix socket. uvicorn runs the
    lifespan before it binds the listening socket, so — as with the
    ``HTTPServer`` this replaced — the port stays closed until SPDK is up,
    rather than accepting requests that could only fail.
    """
    proxy = SpdkProxy(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        stats_task = asyncio.create_task(proxy.report_stats(), name='report_stats')
        stats_task.add_done_callback(_log_task_death)
        try:
            await proxy.wait_for_spdk_ready()
            logger.info('Started RPC http proxy server')
            yield
        finally:
            stats_task.cancel()
            # Awaited so shutdown does not race the cancellation ("Task was
            # destroyed but it is pending"); gathered so neither the
            # CancelledError nor an already-logged failure escapes here.
            await asyncio.gather(stats_task, return_exceptions=True)

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.proxy = proxy

    # Served on the RPC port behind the same credentials as the RPCs
    # themselves: `expose` forwards kwargs to the route decorator.
    # Ungrouped status codes: the default folds every client-side rejection
    # into one `4xx` series, which cannot tell a malformed body (400) from bad
    # credentials (401). Only a handful of codes are reachable here, so the
    # cardinality is negligible. `spdk_proxy_rpc_failures_total` deliberately
    # does not duplicate any of this -- it exists for what a status code cannot
    # say, namely which SPDK method failed and why a 500 was a 500.
    Instrumentator(
        registry=proxy.metrics.registry,
        should_group_status_codes=False,
    ).instrument(app).expose(
        app,
        endpoint=METRICS_ENDPOINT,
        include_in_schema=False,
        dependencies=[Depends(require_authorization)],
    )

    app.add_middleware(AccessLogMiddleware)

    @app.post('/{path:path}', dependencies=[Depends(require_authorization)])
    async def rpc(request: Request) -> Response:
        log: RequestLog = request.state.request_log
        proxy.active_requests += 1
        try:
            read_start = time.monotonic()
            try:
                body = await request.body()
            # The only disconnect this handler can observe. Writing the
            # response is uvicorn's, so the BrokenPipeError the
            # BaseHTTPRequestHandler this replaced had to catch around
            # `wfile.write` cannot reach here.
            except ClientDisconnect:
                logger.warning(
                    "client disconnected before the request body arrived "
                    f"(request {log.request_id})")
                return Response(status_code=400)
            proxy.metrics.observe_body_read(time.monotonic() - read_start)

            try:
                response = await proxy.rpc_call(
                    body, request.headers.get('X-RPC-Timeout'), log)
            except InvalidRequest as e:
                logger.warning(f"rejected a malformed request (request {log.request_id}): {e}")
                return Response(status_code=400)
            except ValueError:
                return Response(status_code=500)
            except OSError as e:
                # SPDK is gone (crashed, or never came back after a restart).
                # The pre-FastAPI server let this escape the handler and dropped
                # the connection; a 500 says the same thing legibly.
                logger.error(f"Could not reach SPDK on {proxy.settings.rpc_sock}: {e}")
                return Response(status_code=500)

            if response is None:
                return Response(status_code=204)

            return Response(content=response, media_type='application/json')
        finally:
            proxy.active_requests -= 1

    return app


def _configure_logging() -> None:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)

    # The access log needs a format of its own to render the fields the
    # middleware attaches, and must not propagate, or the root handler would
    # print the same line again without them. Wired here rather than at import
    # time, which the module docstring promises stays free of side effects.
    access_handler = logging.StreamHandler(stream=sys.stdout)
    access_handler.setFormatter(logging.Formatter(AccessLogMiddleware.LOG_FORMAT))
    access_logger.addHandler(access_handler)
    access_logger.propagate = False


def main() -> None:
    _configure_logging()
    # Required fields come from the environment, which mypy can't see without
    # the pydantic plugin.
    settings = ProxySettings()  # type: ignore[call-arg]
    tls = Settings()
    uvicorn.Server(uvicorn.Config(
        app=create_app(settings),
        host=settings.server_ip,
        port=settings.rpc_port,
        log_level='info',
        # Replaced by AccessLogMiddleware, not dropped.
        access_log=False,
        limit_concurrency=settings.max_concurrent_connections,
        timeout_keep_alive=settings.keepalive_timeout,
        ssl_certfile=tls.tls_certificate if tls.tls_serve else None,
        ssl_keyfile=tls.tls_key if tls.tls_serve else None,
        ssl_ca_certs=tls.tls_certificate_authority if tls.tls_client_auth != ssl.CERT_NONE else None,
        ssl_cert_reqs=tls.tls_client_auth,
    )).run()


if __name__ == '__main__':
    main()
