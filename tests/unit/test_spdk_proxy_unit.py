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
from pydantic import ValidationError

from simplyblock_core.services import spdk_http_proxy_server as proxy_mod


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
        self.assertEqual(settings.spdk_timeout_margin, 2.0)
        self.assertFalse(settings.multi_threading_enabled)

    def test_optional_environment_is_read(self):
        with patch.dict(os.environ, {**REQUIRED_ENV, **OPTIONAL_ENV}, clear=True):
            settings = proxy_mod.ProxySettings()

        self.assertEqual(settings.timeout, 5)
        self.assertEqual(settings.max_concurrent_spdk, 4)
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


if __name__ == "__main__":
    unittest.main()
