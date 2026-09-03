# coding=utf-8
"""
test_spdk_proxy_e2e.py – mocked end-to-end tests for spdk_http_proxy_server.

Uses a real unix socket mock SPDK server and the real proxy HTTP server
to test the full request flow: readiness gating, request forwarding,
timeout cleanup, and zombie socket prevention.

NOTE: Requires AF_UNIX (Linux/macOS). Skipped on Windows.
"""

import base64
import json
import os
import socketserver
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import requests

if sys.platform == "win32":
    raise unittest.SkipTest("AF_UNIX not available on Windows")

from tests.conftest_proxy import import_proxy_module


def _wait_until(predicate, timeout=5, interval=0.02):
    """Poll ``predicate`` instead of a fixed sleep-then-assert.

    A single fixed sleep assumes the process gets scheduled promptly; under
    CI load (or another test's global time.sleep patch stealing this
    process's CPU -- see print_stats in spdk_http_proxy_server.py) cleanup
    that normally finishes in microseconds can take longer than that budget
    without ever actually failing to happen.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()

# ---------------------------------------------------------------------------
# Mock SPDK unix socket server
# ---------------------------------------------------------------------------

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

    def __init__(self, sock_path, ready=True, delay=0):
        self.sock_path = sock_path
        self._ready = ready
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


def _start_mock_spdk(sock_path, **kwargs):
    """Start a mock SPDK server in a background thread."""
    server = MockSPDKServer(sock_path, **kwargs)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ---------------------------------------------------------------------------
# Proxy launcher helper
# ---------------------------------------------------------------------------

class _ProxyHandle:
    """The proxy thread's bound port, when it bound, or why it could not.

    The proxy binds its HTTP port only AFTER ``wait_for_spdk_ready()``
    returns -- that ordering is the thing TestProxyReadinessGate exists to
    verify -- so the port cannot be known before the thread runs. Publishing
    it back turns two silent failure modes into loud ones:

      * a hardcoded port already in use raised OSError inside a daemon thread
        and was swallowed, so the test saw only "Proxy should eventually come
        up" after polling for 8s, with nothing to say why. That is exactly how
        it failed in CI run 33668203362 on main.
      * every other startup error looked identical to that one.

    Ports are now requested as 0, so the OS picks a free one and neither a
    sibling test class nor a parallel CI job can contend for a fixed number.
    """

    def __init__(self):
        self._ready = threading.Event()
        self.port = None
        self.bind_time = None
        self.error = None

    def _publish_port(self, port, bind_time):
        self.port = port
        self.bind_time = bind_time
        self._ready.set()

    def _publish_error(self, exc):
        self.error = exc
        self._ready.set()

    def settled_within(self, timeout):
        """True if the thread has already bound (or failed) within timeout.

        Event.wait, deliberately, not time.sleep: another module in this test
        session patches time.sleep globally, so any duration measured against
        a sleep is meaningless here. That is what made the first version of
        the gate assertion below read 7ms for a 1s delay in CI run
        33671036716 -- the delay simply never happened.
        """
        return self._ready.wait(timeout)

    def wait(self, timeout=30):
        """Bound port, or an assertion naming the actual problem."""
        if not self._ready.wait(timeout):
            raise AssertionError(
                f"proxy thread neither bound nor failed within {timeout}s")
        if self.error is not None:
            raise AssertionError(f"proxy failed to start: {self.error!r}")
        return self.port


def _start_proxy(sock_path, http_port=0, max_concurrent=4, timeout=5):
    """Start the spdk_http_proxy_server in a background thread.

    We import and configure the module, then run the server.
    Returns (thread, stop_event, module, handle); pass http_port=0 (the
    default) to get an OS-assigned port and read it off the handle.
    """
    # The proxy module starts a real server at import time, so we use the
    # shared helper that neutralizes that side-effect during import. The
    # helper also pre-sets required env vars (SERVER_IP, RPC_PORT, ...).
    mod = import_proxy_module()

    # Reconfigure module globals — the helper sets defaults; override with
    # the values this test needs.
    mod.rpc_sock = sock_path
    mod.TIMEOUT = timeout
    mod.MAX_CONCURRENT_SPDK = max_concurrent
    mod.spdk_semaphore = threading.Semaphore(max_concurrent)
    mod.spdk_ready = False
    mod.unix_sockets.clear()
    mod.ServerHandler.server_session.clear()
    mod.read_line_time_diff.clear()
    mod.recv_from_spdk_time_diff.clear()

    stop_event = threading.Event()

    handle = _ProxyHandle()

    def run():
        try:
            key = base64.b64encode(b"test:test").decode("ascii")
            mod.wait_for_spdk_ready()
            mod.ServerHandler.key = key
            from http.server import ThreadingHTTPServer
            httpd = ThreadingHTTPServer(("127.0.0.1", http_port), mod.ServerHandler)
            # Accept timeout only -- the SPDK-side budget is mod.TIMEOUT. Kept
            # short so stop_event is honoured (and the port released) within a
            # fraction of a second instead of up to `timeout`, which used to
            # leave a fixed port bound while the next test tried to claim it.
            httpd.timeout = 0.2
            handle._publish_port(httpd.server_address[1], time.monotonic())
        except Exception as e:      # noqa: BLE001 - reported via the handle
            handle._publish_error(e)
            return

        while not stop_event.is_set():
            httpd.handle_request()
        httpd.server_close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, stop_event, mod, handle


class TestProxyE2E(unittest.TestCase):
    """End-to-end tests with mock SPDK server + real proxy."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls._sock_path = os.path.join(cls._tmpdir, "spdk_test.sock")
        # Start mock SPDK
        cls._spdk_server = _start_mock_spdk(cls._sock_path)
        time.sleep(0.1)

        # Start proxy on an OS-assigned port (see _ProxyHandle).
        cls._proxy_thread, cls._stop_event, cls._mod, handle = _start_proxy(
            cls._sock_path, 0, max_concurrent=4, timeout=5)
        cls._http_port = handle.wait()

        # Wait for proxy to be ready
        for _ in range(30):
            try:
                r = requests.post(
                    f"http://127.0.0.1:{cls._http_port}/",
                    data=json.dumps({"id": 1, "method": "spdk_get_version"}),
                    auth=("test", "test"),
                    timeout=2,
                )
                if r.status_code == 200:
                    break
            except requests.ConnectionError:
                pass
            time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls._stop_event.set()
        cls._spdk_server.shutdown()
        try:
            os.unlink(cls._sock_path)
        except OSError:
            pass
        try:
            os.rmdir(cls._tmpdir)
        except OSError:
            pass

    def _post(self, method, params=None):
        payload = {"id": 1, "method": method}
        if params:
            payload["params"] = params
        r = requests.post(
            f"http://127.0.0.1:{self._http_port}/",
            data=json.dumps(payload),
            auth=("test", "test"),
            timeout=5,
        )
        return r

    def test_readiness_gate_prevents_zombie_sockets(self):
        """After startup, there should be zero lingering unix sockets."""
        self.assertEqual(len(self._mod.unix_sockets), 0)

    def test_basic_rpc_roundtrip(self):
        """A simple RPC should return a valid JSON-RPC response."""
        r = self._post("spdk_get_version")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("result", data)
        self.assertEqual(data["result"]["version"], "24.01")

    def test_bdev_get_bdevs_roundtrip(self):
        r = self._post("bdev_get_bdevs")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("result", data)
        self.assertIsInstance(data["result"], list)

    def test_no_socket_leak_after_requests(self):
        """After several requests complete, no unix sockets should be leaked."""
        for _ in range(5):
            self._post("spdk_get_version")
        _wait_until(lambda: len(self._mod.unix_sockets) == 0)
        self.assertEqual(len(self._mod.unix_sockets), 0)

    def test_concurrent_requests(self):
        """Multiple concurrent requests should all succeed."""
        results = []
        errors = []

        def do_request():
            try:
                r = self._post("spdk_get_version")
                results.append(r.status_code)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=do_request) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertTrue(all(s == 200 for s in results), f"Got statuses: {results}")

    def test_unauthorized_returns_401(self):
        """Request with wrong credentials should get 401."""
        r = requests.post(
            f"http://127.0.0.1:{self._http_port}/",
            data=json.dumps({"id": 1, "method": "spdk_get_version"}),
            auth=("wrong", "creds"),
            timeout=5,
        )
        self.assertEqual(r.status_code, 401)

    def test_session_count_returns_to_baseline(self):
        """After requests complete, server_session should be empty."""
        for _ in range(3):
            self._post("spdk_get_version")
        _wait_until(lambda: len(self._mod.ServerHandler.server_session) == 0)
        self.assertEqual(len(self._mod.ServerHandler.server_session), 0)

    def test_keep_alive_reuses_one_connection_for_several_requests(self):
        """A client session issuing several requests should open ONE TCP
        connection, not one per request."""
        connection_count = {"n": 0}
        original_setup = self._mod.ServerHandler.setup

        def counting_setup(handler_self):
            connection_count["n"] += 1
            original_setup(handler_self)

        with patch.object(self._mod.ServerHandler, "setup", counting_setup):
            with requests.Session() as session:
                for _ in range(5):
                    r = session.post(
                        f"http://127.0.0.1:{self._http_port}/",
                        data=json.dumps({"id": 1, "method": "spdk_get_version"}),
                        auth=("test", "test"),
                        timeout=5,
                    )
                    self.assertEqual(r.status_code, 200)

        self.assertEqual(
            connection_count["n"], 1,
            "expected all 5 requests on one requests.Session to reuse a "
            "single kept-alive TCP connection to the proxy")

    def test_unauthorized_request_does_not_corrupt_the_connection(self):
        """Regression test: an undrained body on a 401 used to leak into the
        next request on the same kept-alive connection ('Bad request version')."""
        with requests.Session() as session:
            r1 = session.post(
                f"http://127.0.0.1:{self._http_port}/",
                data=json.dumps({"id": 1, "method": "spdk_get_version"}),
                auth=("wrong", "creds"),
                timeout=5,
            )
            self.assertEqual(r1.status_code, 401)

            r2 = session.post(
                f"http://127.0.0.1:{self._http_port}/",
                data=json.dumps({"id": 1, "method": "spdk_get_version"}),
                auth=("test", "test"),
                timeout=5,
            )
            self.assertEqual(r2.status_code, 200)
            self.assertEqual(r2.json()["result"]["version"], "24.01")


class TestProxyReadinessGate(unittest.TestCase):
    """Test that the proxy blocks until SPDK is ready."""

    def test_proxy_waits_for_spdk(self):
        """Proxy should not accept HTTP requests until SPDK responds."""
        tmpdir = tempfile.mkdtemp()
        sock_path = os.path.join(tmpdir, "spdk_delayed.sock")

        ready_time = {"t": None}
        # SPDK appears when this test says so, rather than after a sleep a
        # sibling test module may have patched away.
        release_spdk = threading.Event()

        # Start the SPDK server only once release_spdk fires.
        def delayed_spdk():
            release_spdk.wait(30)
            server = _start_mock_spdk(sock_path)
            ready_time["t"] = time.monotonic()
            return server

        spdk_thread = threading.Thread(target=delayed_spdk, daemon=True)
        spdk_thread.start()

        _, stop_event, mod_ref, handle = _start_proxy(
            sock_path, 0, max_concurrent=4, timeout=5)

        # The gate itself: with SPDK absent, the proxy must not open its HTTP
        # port. This is the assertion the test was named for and never made.
        self.assertFalse(
            handle.settled_within(0.5),
            "proxy bound its HTTP port while SPDK was still absent: the "
            "readiness gate did not hold")

        # Now let SPDK up; the proxy should follow.
        release_spdk.set()
        http_port = handle.wait(timeout=30)

        # Wait for the bound proxy to serve. Paced with Event.wait rather
        # than time.sleep: a sibling module patches time.sleep globally, which
        # would collapse all 40 attempts into microseconds and turn a slow
        # start into a spurious "Proxy should eventually come up".
        pacer = threading.Event()
        proxy_up = False
        for _ in range(40):
            try:
                r = requests.post(
                    f"http://127.0.0.1:{http_port}/",
                    data=json.dumps({"id": 1, "method": "spdk_get_version"}),
                    auth=("test", "test"),
                    timeout=2,
                )
                if r.status_code == 200:
                    proxy_up = True
                    break
            except requests.ConnectionError:
                pass
            pacer.wait(0.2)

        stop_event.set()

        self.assertTrue(proxy_up, "Proxy should eventually come up")
        self.assertIsNotNone(ready_time["t"], "mock SPDK never came up")
        # Proxy should have waited for SPDK: verify no zombie sockets
        self.assertEqual(len(mod_ref.unix_sockets), 0)

        try:
            os.unlink(sock_path)
        except OSError:
            pass
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass


if __name__ == "__main__":
    unittest.main()
