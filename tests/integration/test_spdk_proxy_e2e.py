# coding=utf-8
"""End-to-end tests for the SPDK HTTP proxy.

A real unix-socket server stands in for SPDK and the real proxy application is
served by uvicorn, so the full path is exercised: readiness gating, HTTP
framing, request forwarding, timeout handling and socket cleanup.

NOTE: Requires AF_UNIX (Linux/macOS). Skipped on Windows.
"""

import base64
import contextlib
import http.client
import json
import os
import socket
import socketserver
import sys
import tempfile
import threading
import time
import unittest

import requests
import uvicorn
from tenacity import Retrying, stop_after_delay, wait_fixed, retry_if_exception_type, retry_if_result, RetryError

if sys.platform == "win32":
    raise unittest.SkipTest("AF_UNIX not available on Windows")

from simplyblock_core.services.spdk_http_proxy_server import ProxySettings, create_app


class MockSPDKHandler(socketserver.BaseRequestHandler):
    """Handles JSON-RPC 2.0 over a unix socket, mimicking SPDK."""

    def handle(self):
        buf = b''
        while True:
            data = self.request.recv(65536)
            if not data:
                break
            buf += data
            try:
                req = json.loads(buf.decode('ascii'))
            except (ValueError, UnicodeDecodeError):
                continue

            result = self.server.dispatch(req)
            if 'id' not in req:
                # fire-and-forget
                break

            resp = json.dumps({"jsonrpc": "2.0", "id": req["id"], "result": result})
            self.request.sendall(resp.encode('ascii'))
            break


class MockSPDKServer(socketserver.ThreadingUnixStreamServer):
    """Threaded unix socket server pretending to be SPDK."""

    allow_reuse_address = True

    def __init__(self, sock_path, delay=0):
        self.sock_path = sock_path
        self._delay = delay
        self._call_log = []
        self._lock = threading.Lock()
        super().__init__(sock_path, MockSPDKHandler)

    def dispatch(self, req):
        method = req.get("method", "")
        with self._lock:
            self._call_log.append(method)
        if self._delay > 0:
            time.sleep(self._delay)
        if method == "spdk_get_version":
            return {"version": "24.01", "fields": {}}
        if method == "bdev_get_bdevs":
            return [{"name": "bdev0", "aliases": [], "product_name": "test"}]
        if method == "nvmf_get_subsystems":
            return [{"nqn": "nqn.test", "subtype": "NVMe", "namespaces": []}]
        return True

    @property
    def call_log(self):
        with self._lock:
            return list(self._call_log)


@contextlib.contextmanager
def mock_spdk(sock_path, **kwargs):
    """Serve a mock SPDK on ``sock_path`` for the duration of the block."""
    server = MockSPDKServer(sock_path, **kwargs)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        with contextlib.suppress(OSError):
            os.unlink(sock_path)


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningProxy:
    """The real proxy app, served by uvicorn on a loopback port."""

    def __init__(self, sock_path, **overrides):
        settings = ProxySettings(**{
            "server_ip": "127.0.0.1",
            "rpc_port": _free_port(),
            "rpc_sock_path": sock_path,
            "rpc_username": "test",
            "rpc_password": "test",
            "timeout": 5,
            "max_concurrent_spdk": 4,
            "multi_threading_enabled": True,
            **overrides,
        })
        self.address = ("127.0.0.1", settings.rpc_port)
        self.url = f"http://127.0.0.1:{settings.rpc_port}/"
        app = create_app(settings)
        self.proxy = app.state.proxy
        self._server = uvicorn.Server(uvicorn.Config(
            app=app, host=settings.server_ip, port=settings.rpc_port, log_level="warning"))
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._server.should_exit = True
        self._thread.join(timeout=10)

    def wait_until_serving(self, timeout=15):
        try:
            return Retrying(
                stop=stop_after_delay(timeout),
                wait=wait_fixed(0.1),
                retry=(retry_if_exception_type(requests.RequestException) | retry_if_result(lambda success: not success)),
            )(lambda: self.post("spdk_get_version").status_code == 200)
        except RetryError:
            return False

    def metric(self, name, **labels):
        return self.proxy.metrics.registry.get_sample_value(name, labels or None)

    def is_refusing_connections(self):
        try:
            requests.post(self.url, data="{}", timeout=2)
        except requests.ConnectionError:
            return True
        return False

    def post(self, method, params=None, auth=("test", "test"), headers=None, **kwargs):
        payload = {"id": 1, "method": method}
        if params:
            payload["params"] = params
        return self.request(json.dumps(payload), auth=auth, headers=headers, **kwargs)

    def request(self, body, auth=("test", "test"), headers=None, timeout=10, session=requests):
        return session.post(self.url, data=body, auth=auth, headers=headers, timeout=timeout)

    @contextlib.contextmanager
    def connection(self, timeout=10):
        """One HTTP connection, kept open across requests."""
        conn = http.client.HTTPConnection(*self.address, timeout=timeout)
        try:
            conn.connect()
            yield conn
        finally:
            conn.close()


@contextlib.contextmanager
def running_proxy(sock_path, **overrides):
    proxy = RunningProxy(sock_path, **overrides)
    proxy.start()
    try:
        yield proxy
    finally:
        proxy.stop()


@contextlib.contextmanager
def sock_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "spdk.sock")


class TestProxyE2E(unittest.TestCase):
    """Mock SPDK + the real proxy, over real HTTP."""

    @classmethod
    def setUpClass(cls):
        cls._stack = contextlib.ExitStack()
        path = cls._stack.enter_context(sock_path())
        cls._spdk = cls._stack.enter_context(mock_spdk(path))
        cls._proxy = cls._stack.enter_context(running_proxy(path))
        if not cls._proxy.wait_until_serving():
            cls._stack.close()
            raise unittest.SkipTest("proxy did not come up")

    @classmethod
    def tearDownClass(cls):
        cls._stack.close()

    def test_basic_rpc_roundtrip(self):
        response = self._proxy.post("spdk_get_version")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["result"]["version"], "24.01")

    def test_request_reaches_spdk(self):
        self._proxy.post("nvmf_get_subsystems")

        self.assertIn("nvmf_get_subsystems", self._spdk.call_log)

    def test_bdev_get_bdevs_roundtrip(self):
        response = self._proxy.post("bdev_get_bdevs")

        self.assertEqual(response.status_code, 200)
        self.assertIsInstance(response.json()["result"], list)

    def test_notification_gets_204(self):
        response = self._proxy.request(json.dumps({"method": "spdk_kill_instance"}))

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.content, b'')

    def test_unauthorized_returns_401(self):
        response = self._proxy.post("spdk_get_version", auth=("wrong", "creds"))

        self.assertEqual(response.status_code, 401)

    def test_malformed_body_returns_400(self):
        response = self._proxy.request("not json")

        self.assertEqual(response.status_code, 400)

    def test_keep_alive_serves_several_requests_on_one_connection(self):
        body = json.dumps({"id": 1, "method": "spdk_get_version"})
        headers = {"Authorization": "Basic " + base64.b64encode(b"test:test").decode("ascii")}

        with self._proxy.connection() as conn:
            for _ in range(5):
                conn.request("POST", "/", body=body, headers=headers)
                response = conn.getresponse()
                response.read()

                self.assertEqual(response.status, 200)
                self.assertFalse(response.will_close, "proxy closed the connection after a response")

    def test_unauthorized_request_does_not_corrupt_the_connection(self):
        """A rejected request leaves its body unread; on a kept-alive
        connection it must not be parsed as the next request."""
        with requests.Session() as session:
            rejected = self._proxy.post(
                "spdk_get_version", auth=("wrong", "creds"), session=session)
            self.assertEqual(rejected.status_code, 401)

            accepted = self._proxy.post("spdk_get_version", session=session)

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(accepted.json()["result"]["version"], "24.01")

    def test_no_socket_leak_after_requests(self):
        for _ in range(5):
            self._proxy.post("spdk_get_version")

        self.assertEqual(self._proxy.metric("spdk_proxy_unix_connections_open"), 0)
        self.assertEqual(self._proxy.metric("spdk_proxy_requests_in_flight"), 0)

    def test_concurrent_requests(self):
        results = []
        errors = []

        def do_request():
            try:
                results.append(self._proxy.post("spdk_get_version").status_code)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_request) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(results, [200] * 8)
        self.assertEqual(self._proxy.metric("spdk_proxy_unix_connections_open"), 0)


class TestProxyReadinessGate(unittest.TestCase):

    def test_port_stays_closed_until_spdk_answers(self):
        with sock_path() as path, running_proxy(path) as proxy:
            self.assertTrue(
                proxy.is_refusing_connections(),
                "proxy must not accept requests before SPDK is up")

            with mock_spdk(path):
                self.assertTrue(proxy.wait_until_serving())
                self.assertEqual(proxy.metric("spdk_proxy_unix_connections_open"), 0)


class TestProxyTimeout(unittest.TestCase):

    def test_caller_timeout_bounds_the_spdk_wait(self):
        """A caller-supplied X-RPC-Timeout must free the SPDK slot early rather
        than pin it for the proxy-global timeout."""
        with sock_path() as path, mock_spdk(path) as spdk, running_proxy(
                path, timeout=30, spdk_timeout_margin=2) as proxy:
            self.assertTrue(proxy.wait_until_serving())
            spdk._delay = 1  # only now, so the readiness probe stays fast

            started = time.monotonic()
            response = proxy.post("bdev_get_bdevs", headers={"X-RPC-Timeout": "0.2"})
            elapsed = time.monotonic() - started

            self.assertEqual(response.status_code, 500)
            self.assertLess(elapsed, 1)
            self.assertEqual(proxy.metric("spdk_proxy_unix_connections_open"), 0)


if __name__ == "__main__":
    unittest.main()
