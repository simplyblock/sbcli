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
import hmac
import json
import logging
import ssl
import sys
import time
from contextlib import asynccontextmanager
from typing import Annotated, Any, AsyncGenerator, Dict, Optional

import uvicorn
from fastapi import FastAPI, Request, Response
from pydantic import BeforeValidator, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.requests import ClientDisconnect

from simplyblock_core.settings import Settings


logger = logging.getLogger(__name__)

#: How often the periodic timing report is emitted, and the window the
#: ``last_Ns_avg`` figure covers.
STATS_INTERVAL_SEC = 3
#: Samples are dropped wholesale once a series grows past this.
STATS_MAX_SAMPLES = 10000
#: Per-attempt bound on the readiness probe, and the pause between attempts.
SPDK_READY_PROBE_TIMEOUT_SEC = 5
SPDK_READY_POLL_INTERVAL_SEC = 1
#: SPDK responses are read in one shot; matches the pre-FastAPI recv() size.
SPDK_RECV_SIZE = 1024 * 1024 * 1024
#: Idle keep-alive window. The server this replaced spoke HTTP/1.0 and closed
#: after every response, so a connection could never be dropped underneath a
#: client that was about to reuse it. RPCClient deliberately keeps POST out of
#: its urllib3 retry set, so such a drop surfaces as a failed RPC rather than a
#: retry — hence an idle window far longer than the gap between RPCs to a node.
KEEP_ALIVE_TIMEOUT_SEC = 300


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

    @property
    def authorization(self) -> str:
        """The ``Authorization`` header value clients have to present."""
        credentials = f"{self.rpc_username}:{self.rpc_password.get_secret_value()}"
        return 'Basic ' + base64.b64encode(credentials.encode('ascii')).decode('ascii')


class TimingStats:
    """Rolling record of operation durations, reported periodically."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.samples: Dict[int, int] = {}

    def record(self, start_ns: int, duration_ns: int) -> None:
        self.samples[start_ns] = duration_ns

    def report(self, now_ns: int) -> Optional[str]:
        """Summarize the collected samples, clearing them once they pile up."""
        if not self.samples:
            return None

        durations = list(self.samples.values())
        window = [
            duration
            for start, duration in self.samples.items()
            if start > now_ns - STATS_INTERVAL_SEC * 1000 * 1000 * 1000
        ]
        summary = (
            f"{self.name}: max={max(durations)} ns,"
            f" avg={int(sum(durations) / len(durations))} ns,"
            f" last_{STATS_INTERVAL_SEC}s_avg={int(sum(window) / len(window)) if window else 0} ns"
        )

        if len(self.samples) > STATS_MAX_SAMPLES:
            self.samples.clear()

        return summary


class SpdkProxy:
    """Forwards JSON-RPC requests to SPDK's unix socket."""

    def __init__(self, settings: ProxySettings) -> None:
        self.settings = settings
        self.spdk_ready = False
        #: Requests currently being served, and unix sockets currently open
        #: towards SPDK. Both are logged per request and asserted on by tests.
        self.active_requests = 0
        self.open_connections = 0
        self.read_body_stats = TimingStats('read_body_time')
        self.recv_from_spdk_stats = TimingStats('recv_from_spdk_time')
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

    def authenticate(self, authorization: Optional[str]) -> bool:
        # Compared as bytes: compare_digest rejects non-ASCII str outright, and
        # the header is attacker-controlled.
        return authorization is not None and hmac.compare_digest(
            authorization.encode('utf-8'),
            self.settings.authorization.encode('utf-8'),
        )

    async def report_stats(self) -> None:
        """Log the collected timings every ``STATS_INTERVAL_SEC`` seconds."""
        while True:
            await asyncio.sleep(STATS_INTERVAL_SEC)
            try:
                now = time.time_ns()
                for stats in (self.read_body_stats, self.recv_from_spdk_stats):
                    if (summary := stats.report(now)) is not None:
                        logger.info("Periodic stats: %s: %s", now, summary)
            except Exception as e:
                logger.error(e)

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

            buf = b''
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

    async def rpc_call(self, req: bytes, client_timeout: Optional[str] = None) -> Optional[str]:
        """Forward one JSON-RPC request, returning SPDK's raw response.

        Returns ``None`` for a request without an ``id`` (a notification, which
        SPDK does not answer).
        """
        logger.info(f"active requests: {self.active_requests}")
        logger.info(f"active unix sockets: {self.open_connections}")
        req_data = json.loads(req.decode('ascii'))
        req_time = time.time_ns()
        params = str(req_data['params']) if 'params' in req_data else ""
        logger.info(f"Request:{req_time} function: {str(req_data['method'])}, params: {params}")
        sock_timeout = self._resolve_sock_timeout(client_timeout)
        async with self.slots:
            return await self._rpc_call_inner(req, req_data, req_time, sock_timeout)

    async def _rpc_call_inner(
        self,
        req: bytes,
        req_data: dict,
        req_time: int,
        sock_timeout: float,
    ) -> Optional[str]:
        try:
            return await asyncio.wait_for(self._exchange(req, req_data, req_time), sock_timeout)
        except asyncio.TimeoutError as e:
            logger.error(
                f"Socket timeout waiting for SPDK response (request {req_time}, "
                f"function: {req_data.get('method', 'unknown')})")
            raise ValueError('SPDK response timeout') from e

    async def _exchange(self, req: bytes, req_data: dict, req_time: int) -> Optional[str]:
        self.open_connections += 1
        try:
            reader, writer = await asyncio.open_unix_connection(self.settings.rpc_sock)
            try:
                writer.write(req)
                await writer.drain()

                if 'id' not in req_data:
                    return None

                buf = b''
                response = None
                recv_start = time.time_ns()
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
                time_diff = time.time_ns() - recv_start
                self.recv_from_spdk_stats.record(recv_start, time_diff)
                logger.info(f"recv_from_spdk_time_diff: {time_diff}")

                if not response and len(buf) > 0:
                    raise ValueError('Invalid response')

                logger.info(f"Response:{req_time}")

                return buf.decode('ascii')
            finally:
                _close(writer)
        finally:
            self.open_connections -= 1


def _close(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
    except OSError:
        pass


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
        stats_task = asyncio.create_task(proxy.report_stats())
        try:
            await proxy.wait_for_spdk_ready()
            logger.info('Started RPC http proxy server')
            yield
        finally:
            stats_task.cancel()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.proxy = proxy

    @app.post('/{path:path}')
    async def rpc(request: Request) -> Response:
        req_time = time.time_ns()
        proxy.active_requests += 1
        logger.info(f"incoming request at: {req_time}")
        logger.info(f"active server session: {proxy.active_requests}")
        try:
            if not proxy.authenticate(request.headers.get('Authorization')):
                return Response(status_code=401, headers={'WWW-Authenticate': 'Basic'})

            read_start = time.time_ns()
            try:
                body = await request.body()
            except ClientDisconnect:
                logger.warning(
                    f"client disconnected before the request body arrived (request {req_time})")
                return Response(status_code=400)
            time_diff = time.time_ns() - read_start
            proxy.read_body_stats.record(read_start, time_diff)
            logger.info(f"read_body_time_diff: {time_diff}")

            try:
                response = await proxy.rpc_call(body, request.headers.get('X-RPC-Timeout'))
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
        timeout_keep_alive=KEEP_ALIVE_TIMEOUT_SEC,
        ssl_certfile=tls.tls_certificate if tls.tls_serve else None,
        ssl_keyfile=tls.tls_key if tls.tls_serve else None,
        ssl_ca_certs=tls.tls_certificate_authority if tls.tls_client_auth != ssl.CERT_NONE else None,
        ssl_cert_reqs=tls.tls_client_auth,
    )).run()


if __name__ == '__main__':
    main()
