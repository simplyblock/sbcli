# coding=utf-8
"""Unit tests for the SPDK HTTP proxy.

The module under test is imported plainly: building the app is an explicit
``create_app()`` call, so nothing here has to neutralize import-time side
effects.
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from prometheus_client import CollectorRegistry, Histogram
from pydantic import ValidationError

from simplyblock_core.services import spdk_http_proxy_server as proxy_mod
from simplyblock_core.utils.secrets import MASK


REQUIRED_ENV = {
    "SERVER_IP": "127.0.0.1",
    "RPC_PORT": "19999",
    "RPC_USERNAME": "test",
    "RPC_PASSWORD": "secret",
}

# Every optional variable is pinned so an ambient value can't change a test.
OPTIONAL_ENV = {
    "TIMEOUT": "5",
    "MAX_CONCURRENT_SPDK": "4",
    "MAX_CONCURRENT_CONNECTIONS": "8",
    "SPDK_TIMEOUT_MARGIN": "2",
    "MULTI_THREADING_ENABLED": "True",
}


def make_settings(**overrides) -> proxy_mod.ProxySettings:
    params = dict(
        server_ip="127.0.0.1",
        rpc_port=19999,
        rpc_username="test",
        rpc_password="secret",
        timeout=5,
        max_concurrent_spdk=4,
        spdk_timeout_margin=2,
        multi_threading_enabled=True,
    )
    params.update(overrides)
    return proxy_mod.ProxySettings(**params)


def make_proxy(**overrides) -> proxy_mod.SpdkProxy:
    return proxy_mod.SpdkProxy(make_settings(**overrides))


class FakeReader:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def read(self, n=-1):
        return self._chunks.pop(0) if self._chunks else b''


class FakeWriter:
    def __init__(self):
        self.buffer = b''
        self.closed = False

    def write(self, data):
        self.buffer += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True


class FakeSpdkSocket:
    """Stands in for ``asyncio.open_unix_connection``.

    Each entry of ``attempts`` is either an exception to raise on connect or a
    list of chunks the reader hands out. The last entry repeats once exhausted,
    so a retry loop settles on a steady state.
    """

    def __init__(self, *attempts):
        self.attempts = list(attempts)
        self.paths = []
        self.writers = []

    async def __call__(self, path):
        self.paths.append(path)
        attempt = self.attempts.pop(0) if len(self.attempts) > 1 else self.attempts[0]
        writer = FakeWriter()
        self.writers.append(writer)
        if isinstance(attempt, BaseException):
            raise attempt
        return FakeReader(attempt), writer

    @property
    def attempt_count(self):
        return len(self.paths)


def patch_connect(fake):
    return patch.object(proxy_mod.asyncio, 'open_unix_connection', new=fake)


def rpc_response(**kwargs):
    return json.dumps({"jsonrpc": "2.0", "id": 1, **kwargs}).encode('ascii')


class TestProxySettings(unittest.TestCase):
    """The proxy is configured exclusively through unprefixed environment
    variables that deployed manifests already set — defaults included."""

    def test_defaults_match_the_documented_environment(self):
        with patch.dict(os.environ, REQUIRED_ENV, clear=True):
            settings = proxy_mod.ProxySettings()

        self.assertEqual(settings.server_ip, "127.0.0.1")
        self.assertEqual(settings.rpc_port, 19999)
        self.assertEqual(settings.rpc_username, "test")
        self.assertEqual(settings.rpc_password.get_secret_value(), "secret")
        self.assertEqual(settings.timeout, 5 * 60)
        self.assertEqual(settings.max_concurrent_spdk, 16)
        self.assertEqual(settings.max_concurrent_connections, 64)
        self.assertEqual(settings.spdk_timeout_margin, 2.0)
        self.assertFalse(settings.multi_threading_enabled)

    def test_optional_environment_is_read(self):
        with patch.dict(os.environ, {**REQUIRED_ENV, **OPTIONAL_ENV}, clear=True):
            settings = proxy_mod.ProxySettings()

        self.assertEqual(settings.timeout, 5)
        self.assertEqual(settings.max_concurrent_spdk, 4)
        self.assertEqual(settings.max_concurrent_connections, 8)
        self.assertEqual(settings.spdk_timeout_margin, 2.0)
        self.assertTrue(settings.multi_threading_enabled)

    def test_lowercase_environment_is_accepted(self):
        with patch.dict(os.environ, {k.lower(): v for k, v in REQUIRED_ENV.items()}, clear=True):
            self.assertEqual(proxy_mod.ProxySettings().rpc_port, 19999)

    def test_missing_required_variable_is_rejected(self):
        for name in REQUIRED_ENV:
            env = {k: v for k, v in REQUIRED_ENV.items() if k != name}
            with self.subTest(missing=name), patch.dict(os.environ, env, clear=True):
                with self.assertRaises(ValidationError):
                    proxy_mod.ProxySettings()

    def test_a_connection_cap_below_one_is_rejected(self):
        with self.assertRaises(ValidationError):
            make_settings(max_concurrent_connections=0)

    def test_unparsable_rpc_port_falls_back_to_8080(self):
        with patch.dict(os.environ, {**REQUIRED_ENV, "RPC_PORT": "not-a-port"}, clear=True):
            settings = proxy_mod.ProxySettings()

        self.assertEqual(settings.rpc_port, 8080)
        self.assertEqual(settings.rpc_sock, "/mnt/ramdisk/spdk_8080/spdk.sock")

    def test_rpc_sock_follows_the_rpc_port(self):
        self.assertEqual(
            make_settings(rpc_port=9060).rpc_sock, "/mnt/ramdisk/spdk_9060/spdk.sock")

    def test_authorization_header_is_basic_auth(self):
        # 'test:secret' base64-encoded — what RPCClient's requests session sends.
        self.assertEqual(make_settings().authorization, "Basic dGVzdDpzZWNyZXQ=")

    def test_password_is_masked_in_representations(self):
        settings = make_settings()
        self.assertNotIn("secret", repr(settings))
        self.assertNotIn("secret", str(settings))
        self.assertNotIn("secret", str(settings.model_dump()))


class TestResolveSockTimeout(unittest.TestCase):
    """The proxy must bound its SPDK wait (and hence the concurrency-slot hold)
    to the caller's HTTP timeout, so an abandoned/stuck RPC frees its slot
    quickly instead of squatting it for the full global timeout and starving
    other RPCs to the node."""

    def setUp(self):
        self.proxy = make_proxy(timeout=300, spdk_timeout_margin=2)

    def test_missing_hint_falls_back_to_global(self):
        self.assertEqual(self.proxy._resolve_sock_timeout(None), 300)

    def test_invalid_hint_falls_back_to_global(self):
        self.assertEqual(self.proxy._resolve_sock_timeout("not-a-number"), 300)
        self.assertEqual(self.proxy._resolve_sock_timeout("0"), 300)
        self.assertEqual(self.proxy._resolve_sock_timeout("-5"), 300)

    def test_short_caller_timeout_yields_short_hold(self):
        # A 1s caller (e.g. distr_status_events_update) must not pin a slot for
        # the global timeout; it gets margin x 1s, well under the global cap.
        self.assertEqual(self.proxy._resolve_sock_timeout("1"), 2)
        self.assertEqual(self.proxy._resolve_sock_timeout("3"), 6)

    def test_long_caller_timeout_capped_at_global(self):
        self.assertEqual(self.proxy._resolve_sock_timeout("180"), 300)


class TestWaitForSpdkReady(unittest.IsolatedAsyncioTestCase):

    async def test_retries_until_spdk_responds(self):
        fake = FakeSpdkSocket(
            ConnectionRefusedError("not ready"),
            ConnectionRefusedError("not ready"),
            [rpc_response(result={"version": "24.01"})],
        )
        proxy = make_proxy()

        with patch_connect(fake), patch.object(proxy_mod.asyncio, 'sleep', new=AsyncMock()):
            await proxy.wait_for_spdk_ready()

        self.assertTrue(proxy.spdk_ready)
        self.assertEqual(fake.attempt_count, 3)
        self.assertEqual(fake.paths[0], "/mnt/ramdisk/spdk_19999/spdk.sock")

    async def test_already_ready_returns_immediately(self):
        fake = FakeSpdkSocket(ConnectionRefusedError("not ready"))
        proxy = make_proxy()
        proxy.spdk_ready = True

        with patch_connect(fake):
            await proxy.wait_for_spdk_ready()

        self.assertEqual(fake.attempt_count, 0)

    async def test_probe_sends_spdk_get_version(self):
        fake = FakeSpdkSocket([rpc_response(result={})])
        proxy = make_proxy()

        with patch_connect(fake):
            await proxy.wait_for_spdk_ready()

        self.assertEqual(json.loads(fake.writers[0].buffer)["method"], "spdk_get_version")

    async def test_socket_closed_when_probe_gets_no_answer(self):
        fake = FakeSpdkSocket([], [rpc_response(result={})])  # EOF, then a real answer
        proxy = make_proxy()

        with patch_connect(fake), patch.object(proxy_mod.asyncio, 'sleep', new=AsyncMock()):
            await proxy.wait_for_spdk_ready()

        self.assertTrue(all(writer.closed for writer in fake.writers))
        self.assertEqual(fake.attempt_count, 2)


class TestRpcCall(unittest.IsolatedAsyncioTestCase):
    """Every RPC must give its unix socket back, whichever way it ends."""

    def setUp(self):
        self.proxy = make_proxy()
        self.req = json.dumps({"id": 1, "method": "test"}).encode("ascii")

    async def test_response_is_returned_verbatim(self):
        payload = rpc_response(result=True)
        fake = FakeSpdkSocket([payload])

        with patch_connect(fake):
            result = await self.proxy.rpc_call(self.req)

        self.assertEqual(result, payload.decode("ascii"))
        self.assertEqual(fake.writers[0].buffer, self.req)
        self.assertTrue(fake.writers[0].closed)
        self.assertEqual(self.proxy.open_connections, 0)

    async def test_chunked_response_is_reassembled(self):
        payload = rpc_response(result={"a": 1, "b": 2})
        fake = FakeSpdkSocket([payload[:10], payload[10:]])

        with patch_connect(fake):
            result = await self.proxy.rpc_call(self.req)

        self.assertEqual(json.loads(result)["result"], {"a": 1, "b": 2})

    async def test_socket_closed_on_timeout(self):
        async def never_answers(*args, **kwargs):
            await asyncio.sleep(3600)

        fake = FakeSpdkSocket([])

        with patch_connect(fake), patch.object(FakeReader, 'read', never_answers):
            with self.assertRaises(ValueError) as ctx:
                await self.proxy.rpc_call(self.req, client_timeout="0.01")

        self.assertIn("timeout", str(ctx.exception))
        self.assertTrue(fake.writers[0].closed)
        self.assertEqual(self.proxy.open_connections, 0)

    async def test_socket_released_on_connect_error(self):
        fake = FakeSpdkSocket(ConnectionRefusedError("refused"))

        with patch_connect(fake):
            with self.assertRaises(ConnectionRefusedError):
                await self.proxy.rpc_call(self.req)

        self.assertEqual(self.proxy.open_connections, 0)

    async def test_request_without_id_gets_no_response(self):
        req = json.dumps({"method": "notification_only"}).encode("ascii")
        fake = FakeSpdkSocket([rpc_response(result=True)])

        with patch_connect(fake):
            result = await self.proxy.rpc_call(req)

        self.assertIsNone(result)
        self.assertTrue(fake.writers[0].closed)
        self.assertEqual(self.proxy.open_connections, 0)

    async def test_truncated_response_is_rejected(self):
        fake = FakeSpdkSocket([b'{"jsonrpc": "2.0", "id"'])

        with patch_connect(fake):
            with self.assertRaises(ValueError):
                await self.proxy.rpc_call(self.req)

        self.assertEqual(self.proxy.open_connections, 0)

    async def test_caller_timeout_bounds_the_socket_wait(self):
        fake = FakeSpdkSocket([rpc_response(result=True)])
        proxy = make_proxy(timeout=300, spdk_timeout_margin=2)

        with patch_connect(fake), patch.object(
                proxy_mod.asyncio, 'wait_for', wraps=asyncio.wait_for) as wait_for:
            await proxy.rpc_call(self.req, client_timeout="3")

        self.assertEqual(wait_for.call_args.args[1], 6)


class TestRequestLog(unittest.IsolatedAsyncioTestCase):
    """The per-request log line is relied on for debugging across the org, so
    it keeps logging params — but the proxy sees bodies that have already been
    through ``unwrap_secrets_for_send``, with no ``SecretStr`` left to mask by.
    Redaction is therefore by parameter name, and unconditional.
    """

    def setUp(self):
        self.proxy = make_proxy()

    async def _log_for(self, **body):
        req = json.dumps({"id": 1, **body}).encode("ascii")
        fake = FakeSpdkSocket([rpc_response(result=True)])
        with patch_connect(fake):
            with self.assertLogs(proxy_mod.logger, "INFO") as captured:
                await self.proxy.rpc_call(req)
        return "\n".join(captured.output)

    async def test_crypto_keys_are_masked(self):
        log = await self._log_for(
            method="accel_crypto_key_create",
            params={
                "cipher": "AES_XTS",
                "name": "key_lvol_1",
                "key": "DEKSENTINELONE",
                "key2": "DEKSENTINELTWO",
            },
        )

        self.assertNotIn("DEKSENTINELONE", log)
        self.assertNotIn("DEKSENTINELTWO", log)
        self.assertIn(MASK, log)

    async def test_the_line_still_names_the_method_and_its_safe_params(self):
        """The point of the line. A future change must not "fix" a leak by
        dropping the field."""
        log = await self._log_for(
            method="accel_crypto_key_create",
            params={
                "cipher": "AES_XTS",
                "name": "key_lvol_1",
                "key": "DEKSENTINELONE",
                "key2": "DEKSENTINELTWO",
            },
        )

        self.assertIn("accel_crypto_key_create", log)
        self.assertIn("AES_XTS", log)
        self.assertIn("key_lvol_1", log)

    async def test_s3_secret_is_masked_but_its_access_key_id_is_not(self):
        log = await self._log_for(
            method="bdev_s3_create",
            params={
                "name": "s3bdev",
                "local_endpoint": "http://minio:9000",
                "access_key_id": "AKIAIDENTIFIER",
                "secret_access_key": "S3SENTINEL",
            },
        )

        self.assertNotIn("S3SENTINEL", log)
        self.assertIn("AKIAIDENTIFIER", log)
        self.assertIn("http://minio:9000", log)

    async def test_a_nested_secret_is_masked(self):
        log = await self._log_for(
            method="some_future_rpc",
            params={"outer": [{"secret_access_key": "NESTEDSENTINEL"}]},
        )

        self.assertNotIn("NESTEDSENTINEL", log)

    async def test_positional_params_do_not_break_the_line(self):
        """Redaction runs on the unconditional path for a caller-controlled
        body, so a shape SPDK never sends must not turn every RPC into a 500."""
        log = await self._log_for(method="some_future_rpc", params=[1, "two", None])

        self.assertIn("some_future_rpc", log)
        self.assertIn("two", log)

    async def test_scalar_params_do_not_break_the_line(self):
        log = await self._log_for(method="some_future_rpc", params="bare")

        self.assertIn("some_future_rpc", log)


class TestConcurrencyLimit(unittest.IsolatedAsyncioTestCase):

    async def _run_concurrently(self, proxy, count):
        peak = {"seen": 0, "current": 0}

        async def inner(*args, **kwargs):
            peak["current"] += 1
            peak["seen"] = max(peak["seen"], peak["current"])
            await asyncio.sleep(0.01)
            peak["current"] -= 1
            return rpc_response(result=True).decode()

        req = json.dumps({"id": 1, "method": "test"}).encode("ascii")
        with patch.object(proxy, '_rpc_call_inner', side_effect=inner):
            await asyncio.gather(*(proxy.rpc_call(req) for _ in range(count)))

        return peak["seen"]

    async def test_concurrency_is_capped_at_max_concurrent_spdk(self):
        proxy = make_proxy(max_concurrent_spdk=4, multi_threading_enabled=True)
        self.assertEqual(await self._run_concurrently(proxy, 12), 4)

    async def test_without_multi_threading_rpcs_are_serialized(self):
        # The pre-FastAPI server ran on a non-threading HTTPServer in this mode.
        proxy = make_proxy(max_concurrent_spdk=4, multi_threading_enabled=False)
        self.assertEqual(await self._run_concurrently(proxy, 12), 1)

    async def test_slot_released_on_exception(self):
        proxy = make_proxy(max_concurrent_spdk=1)
        req = json.dumps({"id": 1, "method": "test"}).encode("ascii")

        with patch.object(proxy, '_rpc_call_inner', side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                await proxy.rpc_call(req)

        self.assertFalse(proxy.slots.locked())


class TestEndpoint(unittest.TestCase):
    """HTTP contract seen by ``simplyblock_core.rpc_client.RPCClient``."""

    def setUp(self):
        self.app = proxy_mod.create_app(make_settings())
        self.proxy = self.app.state.proxy
        self.proxy.spdk_ready = True
        # Lifespan (and with it the readiness gate) is deliberately not run:
        # TestClient only starts it when used as a context manager.
        self.client = TestClient(self.app)
        self.body = json.dumps({"id": 1, "method": "spdk_get_version"})

    def _post(self, auth=("test", "secret"), **kwargs):
        return self.client.post("/", content=self.body, auth=auth, **kwargs)

    def test_response_is_passed_through(self):
        payload = rpc_response(result={"version": "24.01"}).decode()
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(return_value=payload)):
            response = self._post()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["result"], {"version": "24.01"})

    def test_notification_gets_204(self):
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(return_value=None)):
            response = self._post()

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b'')

    def test_wrong_credentials_get_401(self):
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock()) as rpc_call:
            response = self._post(auth=("wrong", "creds"))

        self.assertEqual(response.status_code, 401)
        rpc_call.assert_not_called()

    def test_missing_credentials_get_401(self):
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock()) as rpc_call:
            response = self.client.post("/", content=self.body)

        self.assertEqual(response.status_code, 401)
        rpc_call.assert_not_called()

    def test_non_ascii_credentials_get_401(self):
        # A header hmac.compare_digest would refuse to compare as str.
        response = self.client.post(
            "/", content=self.body, headers={"Authorization": b"Basic \xe9"})

        self.assertEqual(response.status_code, 401)

    def test_credentials_are_scoped_to_the_app_that_serves_the_request(self):
        """The dependency resolves settings per request, so two apps built in
        one process do not accept each other's credentials."""
        other = proxy_mod.create_app(make_settings(rpc_password="other"))
        other.state.proxy.spdk_ready = True

        with patch.object(other.state.proxy, 'rpc_call', new=AsyncMock(return_value=None)):
            accepted = TestClient(other).post(
                "/", content=self.body, auth=("test", "other"))
            rejected = TestClient(other).post(
                "/", content=self.body, auth=("test", "secret"))

        self.assertEqual(accepted.status_code, 204)
        self.assertEqual(rejected.status_code, 401)

    def test_bad_spdk_response_gets_500(self):
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(side_effect=ValueError("bad"))):
            response = self._post()

        self.assertEqual(response.status_code, 500)

    def test_unreachable_spdk_gets_500(self):
        error = ConnectionRefusedError("spdk is gone")
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(side_effect=error)):
            response = self._post()

        self.assertEqual(response.status_code, 500)

    def test_malformed_request_body_gets_500(self):
        response = self.client.post("/", content="not json", auth=("test", "secret"))

        self.assertEqual(response.status_code, 500)

    def test_json_body_without_a_method_gets_500(self):
        """Valid JSON that is not a JSON-RPC request used to raise KeyError out
        of the handler, bypassing the 500 path and the failure counter."""
        response = self.client.post(
            "/", content=json.dumps({"id": 1}), auth=("test", "secret"))

        self.assertEqual(response.status_code, 500)

    def test_json_body_that_is_not_an_object_gets_500(self):
        for body in ("5", "[]", '"a string"', "null"):
            with self.subTest(body=body):
                response = self.client.post("/", content=body, auth=("test", "secret"))

                self.assertEqual(response.status_code, 500)

    def test_caller_timeout_header_is_forwarded(self):
        payload = rpc_response(result=True).decode()
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(return_value=payload)) as rpc_call:
            self._post(headers={"X-RPC-Timeout": "7"})

        self.assertEqual(rpc_call.call_args.args[1], "7")

    def test_in_flight_count_returns_to_zero(self):
        payload = rpc_response(result=True).decode()
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(return_value=payload)):
            for _ in range(3):
                self._post()

        self.assertEqual(self.proxy.active_requests, 0)

    def test_in_flight_count_returns_to_zero_after_failure(self):
        with patch.object(self.proxy, 'rpc_call', new=AsyncMock(side_effect=ValueError("bad"))):
            self._post()

        self.assertEqual(self.proxy.active_requests, 0)


class TestIntervalReport(unittest.TestCase):
    """The periodic log line, computed from a histogram's own totals."""

    def setUp(self):
        registry = CollectorRegistry()
        self.report = proxy_mod.IntervalReport('spdk', Histogram(
            'test_duration_seconds', 'test', ['method'],
            buckets=(0.01, 0.1, 1, float("inf")), registry=registry))

    def test_first_tick_without_observations_reports_nothing(self):
        self.assertIsNone(self.report.report())

    def test_interval_average_covers_only_the_new_observations(self):
        self.report.observe(1.0, method="a")
        self.report.report()

        # A second interval an order of magnitude faster: a cumulative average
        # would still read ~0.5s, an interval average must read 0.1s.
        self.report.observe(0.1, method="a")
        summary = self.report.report()

        self.assertIn("interval_avg=100.0ms", summary)
        self.assertIn("n=1", summary)

    def test_quiet_interval_reports_nothing_rather_than_dividing_by_zero(self):
        self.report.observe(1.0, method="a")
        self.report.report()

        self.assertIsNone(self.report.report())

    def test_max_is_the_interval_peak_and_resets(self):
        self.report.observe(0.5, method="a")
        self.report.observe(0.02, method="a")

        self.assertIn("max=500.0ms", self.report.report())

        self.report.observe(0.02, method="a")

        self.assertIn("max=20.0ms", self.report.report())

    def test_totals_are_aggregated_across_methods(self):
        self.report.observe(0.1, method="a")
        self.report.observe(0.3, method="b")
        summary = self.report.report()

        self.assertIn("n=2", summary)
        self.assertIn("interval_avg=200.0ms", summary)

    def test_slowest_method_of_the_interval_is_named(self):
        for _ in range(10):
            self.report.observe(0.001, method="fast")
        self.report.observe(0.9, method="slow")

        self.assertIn("slowest=slow", self.report.report())

    def test_slowest_reflects_the_interval_not_history(self):
        self.report.observe(0.9, method="slow")
        self.report.report()

        self.report.observe(0.5, method="other")
        self.assertIn("slowest=other", self.report.report())

    def test_p99_lands_in_the_bucket_holding_the_tail(self):
        for _ in range(95):
            self.report.observe(0.005, method="a")
        for _ in range(5):
            self.report.observe(0.5, method="a")

        self.assertIn("p99<=1.00s", self.report.report())

    def test_p99_ignores_a_tail_thinner_than_one_percent(self):
        for _ in range(99):
            self.report.observe(0.005, method="a")
        self.report.observe(0.5, method="a")

        self.assertIn("p99<=10.0ms", self.report.report())

    def test_p99_beyond_the_last_finite_bucket_is_marked_open_ended(self):
        self.report.observe(30.0, method="a")

        self.assertIn("p99>1.00s", self.report.report())

    def test_unlabelled_histogram_reports_without_a_slowest_field(self):
        registry = CollectorRegistry()
        report = proxy_mod.IntervalReport('body', Histogram(
            'test_body_seconds', 'test', buckets=(0.01, float("inf")), registry=registry))
        report.observe(0.005)

        summary = report.report()

        self.assertIn("n=1", summary)
        self.assertNotIn("slowest=", summary)


class TestBucketQuantile(unittest.TestCase):

    def test_empty_buckets_have_no_quantile(self):
        self.assertIsNone(proxy_mod._bucket_quantile({}, 0.99))

    def test_all_zero_buckets_have_no_quantile(self):
        self.assertIsNone(proxy_mod._bucket_quantile({0.1: 0.0, float("inf"): 0.0}, 0.99))

    def test_quantile_is_the_bound_of_the_bucket_it_falls_in(self):
        buckets = {0.1: 90.0, 1.0: 99.0, float("inf"): 100.0}

        self.assertEqual(proxy_mod._bucket_quantile(buckets, 0.5), 0.1)
        self.assertEqual(proxy_mod._bucket_quantile(buckets, 0.99), 1.0)
        self.assertEqual(proxy_mod._bucket_quantile(buckets, 1.0), float("inf"))


class TestMethodLabelCardinality(unittest.TestCase):
    """The method label is caller-supplied, so its value set must be bounded."""

    def setUp(self):
        self.metrics = proxy_mod.ProxyMetrics()

    def test_known_methods_are_passed_through(self):
        self.assertEqual(self.metrics.method_label("bdev_get_bdevs"), "bdev_get_bdevs")

    def test_methods_past_the_cap_collapse(self):
        for i in range(proxy_mod.MAX_METHOD_LABELS):
            self.metrics.method_label(f"method_{i}")

        self.assertEqual(
            self.metrics.method_label("one_too_many"), proxy_mod.OTHER_METHOD_LABEL)

    def test_a_method_already_seen_survives_the_cap(self):
        self.metrics.method_label("early")
        for i in range(proxy_mod.MAX_METHOD_LABELS):
            self.metrics.method_label(f"method_{i}")

        self.assertEqual(self.metrics.method_label("early"), "early")

    def test_absurdly_long_methods_collapse(self):
        overlong = "x" * (proxy_mod.MAX_METHOD_LABEL_LEN + 1)

        self.assertEqual(self.metrics.method_label(overlong), proxy_mod.OTHER_METHOD_LABEL)


class TestMetricsEndpoint(unittest.TestCase):

    def setUp(self):
        self.app = proxy_mod.create_app(make_settings())
        self.proxy = self.app.state.proxy
        self.proxy.spdk_ready = True
        self.client = TestClient(self.app)

    def test_metrics_require_credentials(self):
        response = self.client.get(proxy_mod.METRICS_ENDPOINT)

        self.assertEqual(response.status_code, 401)

    def test_metrics_are_served_to_an_authorized_caller(self):
        response = self.client.get(proxy_mod.METRICS_ENDPOINT, auth=("test", "secret"))

        self.assertEqual(response.status_code, 200)
        self.assertIn("spdk_proxy_response_duration_seconds", response.text)
        self.assertIn("spdk_proxy_body_read_duration_seconds", response.text)
        self.assertIn("spdk_proxy_rpc_slots_in_use", response.text)
        self.assertIn("spdk_proxy_unix_connections_open", response.text)

    def test_observations_reach_the_exposition(self):
        self.proxy.metrics.observe_response("bdev_get_bdevs", 0.25)

        response = self.client.get(proxy_mod.METRICS_ENDPOINT, auth=("test", "secret"))

        self.assertIn('method="bdev_get_bdevs"', response.text)

    def test_credentials_never_appear_in_the_exposition(self):
        response = self.client.get(proxy_mod.METRICS_ENDPOINT, auth=("test", "secret"))

        self.assertNotIn("secret", response.text)

    def test_two_apps_keep_separate_metrics(self):
        """Each app owns its registry, so building a second one neither raises
        nor lets observations bleed between them."""
        other = proxy_mod.create_app(make_settings(rpc_password="other"))
        other.state.proxy.spdk_ready = True
        self.proxy.metrics.observe_response("only_in_the_first", 0.1)

        mine = self.client.get(
            proxy_mod.METRICS_ENDPOINT, auth=("test", "secret")).text
        theirs = TestClient(other).get(
            proxy_mod.METRICS_ENDPOINT, auth=("test", "other")).text

        self.assertIn('method="only_in_the_first"', mine)
        self.assertNotIn('method="only_in_the_first"', theirs)


class TestMetricsObservation(unittest.IsolatedAsyncioTestCase):
    """The gauges and counters have to settle back after every RPC."""

    def setUp(self):
        self.proxy = make_proxy()
        self.req = json.dumps({"id": 1, "method": "bdev_get_bdevs"}).encode("ascii")

    def _value(self, metric, **labels):
        return self.proxy.metrics.registry.get_sample_value(metric, labels or None)

    async def test_gauges_return_to_zero_after_a_successful_rpc(self):
        fake = FakeSpdkSocket([rpc_response(result=True)])

        with patch_connect(fake):
            await self.proxy.rpc_call(self.req)

        self.assertEqual(self._value("spdk_proxy_rpc_slots_in_use"), 0)
        self.assertEqual(self._value("spdk_proxy_unix_connections_open"), 0)

    async def test_response_duration_is_recorded_against_the_method(self):
        fake = FakeSpdkSocket([rpc_response(result=True)])

        with patch_connect(fake):
            await self.proxy.rpc_call(self.req)

        self.assertEqual(
            self._value("spdk_proxy_response_duration_seconds_count", method="bdev_get_bdevs"), 1)

    async def test_timeout_is_counted_as_a_failure(self):
        async def never_answers(*args, **kwargs):
            await asyncio.sleep(3600)

        with patch_connect(FakeSpdkSocket([])), patch.object(
                FakeReader, 'read', never_answers):
            with self.assertRaises(ValueError):
                await self.proxy.rpc_call(self.req, client_timeout="0.01")

        self.assertEqual(
            self._value(
                "spdk_proxy_rpc_failures_total", method="bdev_get_bdevs", reason="timeout"), 1)
        self.assertEqual(self._value("spdk_proxy_rpc_slots_in_use"), 0)

    async def test_unreachable_spdk_is_counted_as_a_failure(self):
        with patch_connect(FakeSpdkSocket(ConnectionRefusedError("refused"))):
            with self.assertRaises(ConnectionRefusedError):
                await self.proxy.rpc_call(self.req)

        self.assertEqual(
            self._value(
                "spdk_proxy_rpc_failures_total",
                method="bdev_get_bdevs", reason="unreachable"), 1)
        self.assertEqual(self._value("spdk_proxy_unix_connections_open"), 0)


if __name__ == "__main__":
    unittest.main()
