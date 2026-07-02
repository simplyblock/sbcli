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

import requests

if sys.platform == "win32":
    raise unittest.SkipTest("AF_UNIX not available on Windows")

from tests.conftest_proxy import import_proxy_module

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

            resp = self.server.dispatch_full(req)
            if 'id' not in req:
                # fire-and-forget
                break

            self.request.sendall(json.dumps(resp).encode('ascii'))
            break


class MockSPDKServer(socketserver.ThreadingUnixStreamServer):
    """Threaded unix socket server pretending to be SPDK."""

    allow_reuse_address = True

    def __init__(self, sock_path, ready=True, delay=0):
        self.sock_path = sock_path
        self._ready = ready
        self._delay = delay
        self._call_log = []
        self._param_log = []
        self._keyring = {}  # name -> path, mimicking SPDK's keyring state
        self._lock = threading.Lock()
        super().__init__(sock_path, MockSPDKHandler)

    def dispatch_full(self, req):
        """Return a complete JSON-RPC response dict (result or error)."""
        method = req.get("method", "")
        params = req.get("params") or {}
        req_id = req.get("id")
        with self._lock:
            self._call_log.append(method)
            self._param_log.append((method, params))
        if self._delay > 0:
            time.sleep(self._delay)

        if method == "keyring_file_add_key":
            name = params.get("name")
            if name in self._keyring:
                return {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": -17, "message": "File exists"}}
            self._keyring[name] = params.get("path")
            return {"jsonrpc": "2.0", "id": req_id, "result": True}
        if method == "keyring_file_remove_key":
            name = params.get("name")
            if name not in self._keyring:
                return {"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": -2, "message": "No such file or directory"}}
            del self._keyring[name]
            return {"jsonrpc": "2.0", "id": req_id, "result": True}

        return {"jsonrpc": "2.0", "id": req_id, "result": self.dispatch(req)}

    def dispatch(self, req):
        method = req.get("method", "")
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

    @property
    def param_log(self):
        with self._lock:
            return list(self._param_log)

    @property
    def keyring(self):
        with self._lock:
            return dict(self._keyring)


def _start_mock_spdk(sock_path, **kwargs):
    """Start a mock SPDK server in a background thread."""
    server = MockSPDKServer(sock_path, **kwargs)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# ---------------------------------------------------------------------------
# Proxy launcher helper
# ---------------------------------------------------------------------------

def _start_proxy(sock_path, http_port, max_concurrent=4, timeout=5):
    """Start the spdk_http_proxy_server in a background thread.

    We import and configure the module, then run the server.
    Returns a thread + a stop event.
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

    def run():
        key = base64.b64encode(b"test:test").decode("ascii")
        mod.wait_for_spdk_ready()
        mod.ServerHandler.key = key
        from http.server import ThreadingHTTPServer
        httpd = ThreadingHTTPServer(("127.0.0.1", http_port), mod.ServerHandler)
        httpd.timeout = timeout

        while not stop_event.is_set():
            httpd.handle_request()
        httpd.server_close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, stop_event, mod


class TestProxyE2E(unittest.TestCase):
    """End-to-end tests with mock SPDK server + real proxy."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls._sock_path = os.path.join(cls._tmpdir, "spdk_test.sock")
        cls._http_port = 18199

        # Start mock SPDK
        cls._spdk_server = _start_mock_spdk(cls._sock_path)
        time.sleep(0.1)

        # Start proxy
        cls._proxy_thread, cls._stop_event, cls._mod = _start_proxy(
            cls._sock_path, cls._http_port, max_concurrent=4, timeout=5)

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
        time.sleep(0.2)
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
        time.sleep(0.2)
        self.assertEqual(len(self._mod.ServerHandler.server_session), 0)


class TestProxyKeyringInterception(unittest.TestCase):
    """End-to-end tests for the keyring_add_key interception: full HTTP
    round-trip through the real proxy against a mock SPDK unix socket."""

    KEY_MATERIAL = "DHHC-1:00:c2VjcmV0LWtleS1tYXRlcmlhbA==:"

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.mkdtemp()
        cls._sock_path = os.path.join(cls._tmpdir, "spdk_keyring.sock")
        cls._key_dir = os.path.join(cls._tmpdir, "keys")
        cls._http_port = 18201

        cls._spdk_server = _start_mock_spdk(cls._sock_path)
        time.sleep(0.1)

        cls._proxy_thread, cls._stop_event, cls._mod = _start_proxy(
            cls._sock_path, cls._http_port, max_concurrent=4, timeout=5)
        cls._mod.SPDK_PROXY_KEY_DIR = cls._key_dir

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
                pass  # proxy may still be starting up
            time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls._stop_event.set()
        cls._spdk_server.shutdown()
        cls._spdk_server.server_close()
        import shutil
        shutil.rmtree(cls._tmpdir, ignore_errors=True)

    def setUp(self):
        self._spdk_server._keyring.clear()
        self._spdk_server._call_log.clear()
        self._spdk_server._param_log.clear()
        import shutil
        shutil.rmtree(self._key_dir, ignore_errors=True)

    def _post(self, method, params=None, req_id=1):
        payload = {"id": req_id, "method": method}
        if params:
            payload["params"] = params
        return requests.post(
            f"http://127.0.0.1:{self._http_port}/",
            data=json.dumps(payload),
            auth=("test", "test"),
            timeout=5,
        )

    def test_keyring_add_key_roundtrip(self):
        r = self._post("keyring_add_key", {"name": "key_e2e", "key": self.KEY_MATERIAL})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json().get("result"))

        # File written into the proxy key dir with the exact material
        key_path = os.path.join(self._key_dir, "key_e2e")
        self.assertTrue(os.path.isfile(key_path))
        with open(key_path) as f:
            self.assertEqual(f.read(), self.KEY_MATERIAL)
        self.assertEqual(os.stat(key_path).st_mode & 0o777, 0o600)

        # SPDK saw the rewritten call: path instead of key material
        adds = [p for m, p in self._spdk_server.param_log if m == "keyring_file_add_key"]
        self.assertEqual(adds, [{"name": "key_e2e", "path": key_path}])
        self.assertEqual(self._spdk_server.keyring, {"key_e2e": key_path})
        for _, params in self._spdk_server.param_log:
            self.assertNotIn(self.KEY_MATERIAL, json.dumps(params))

    def test_duplicate_add_same_material_returns_eexist(self):
        self._post("keyring_add_key", {"name": "key_dup", "key": self.KEY_MATERIAL})
        r = self._post("keyring_add_key", {"name": "key_dup", "key": self.KEY_MATERIAL})
        self.assertEqual(r.json()["error"]["code"], -17)
        # File kept: the key is still valid and registered
        self.assertTrue(os.path.isfile(os.path.join(self._key_dir, "key_dup")))

    def test_duplicate_add_different_material_is_rejected(self):
        self._post("keyring_add_key", {"name": "key_conflict", "key": self.KEY_MATERIAL})
        r = self._post("keyring_add_key",
                       {"name": "key_conflict", "key": "DHHC-1:00:b3RoZXI=:"})
        self.assertEqual(r.json()["error"]["code"], -32000)
        with open(os.path.join(self._key_dir, "key_conflict")) as f:
            self.assertEqual(f.read(), self.KEY_MATERIAL)

    def test_remove_deletes_file(self):
        self._post("keyring_add_key", {"name": "key_rm", "key": self.KEY_MATERIAL})
        key_path = os.path.join(self._key_dir, "key_rm")
        self.assertTrue(os.path.isfile(key_path))

        r = self._post("keyring_file_remove_key", {"name": "key_rm"})
        self.assertTrue(r.json().get("result"))
        self.assertFalse(os.path.exists(key_path))
        self.assertEqual(self._spdk_server.keyring, {})

    def test_capabilities_answered_locally(self):
        r = self._post("proxy_get_capabilities", req_id=42)
        data = r.json()
        self.assertEqual(data["id"], 42)
        self.assertIn("keyring_add_key", data["result"]["interceptors"])
        self.assertNotIn("proxy_get_capabilities", self._spdk_server.call_log)

    def test_register_dhchap_keys_on_node_uses_interception(self):
        """The controller helper takes the keyring_add_key path end-to-end:
        RPCClient -> real proxy -> interceptor -> mock SPDK, with no SNodeAPI."""
        from pydantic import SecretStr

        from simplyblock_core.controllers.host_auth import _register_dhchap_keys_on_node
        from simplyblock_core.models.storage_node import StorageNode
        from simplyblock_core.rpc_client import RPCClient
        from simplyblock_core.utils import generate_dhchap_key

        snode = StorageNode()
        snode.uuid = "node-keyring-e2e"
        # Unreachable SNodeAPI: proves the fallback path is not taken.
        snode.api_endpoint = "127.0.0.1:1"

        rpc_client = RPCClient("127.0.0.1", self._http_port, "test", SecretStr("test"))

        host_nqn = "nqn.2014-08.org.nvmexpress:uuid:proxy-e2e-host"
        host_entry = {
            "nqn": host_nqn,
            "dhchap_key": generate_dhchap_key(),
            "dhchap_ctrlr_key": generate_dhchap_key(),
        }

        key_names = _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client)

        self.assertIn("dhchap_key", key_names)
        self.assertIn("dhchap_ctrlr_key", key_names)
        for key_type, key_name in key_names.items():
            key_path = os.path.join(self._key_dir, key_name)
            self.assertEqual(self._spdk_server.keyring[key_name], key_path)
            with open(key_path) as f:
                self.assertEqual(f.read(), host_entry[key_type])
        # Key material never reached SPDK's params
        for _, params in self._spdk_server.param_log:
            payload = json.dumps(params)
            self.assertNotIn(host_entry["dhchap_key"], payload)
            self.assertNotIn(host_entry["dhchap_ctrlr_key"], payload)


class TestProxyReadinessGate(unittest.TestCase):
    """Test that the proxy blocks until SPDK is ready."""

    def test_proxy_waits_for_spdk(self):
        """Proxy should not accept HTTP requests until SPDK responds."""
        tmpdir = tempfile.mkdtemp()
        sock_path = os.path.join(tmpdir, "spdk_delayed.sock")
        http_port = 18200

        ready_time = {"t": None}

        # Start SPDK server with a 1-second delay before it's available
        def delayed_spdk():
            time.sleep(1.0)
            server = _start_mock_spdk(sock_path)
            ready_time["t"] = time.monotonic()
            return server

        spdk_thread = threading.Thread(target=delayed_spdk, daemon=True)
        spdk_thread.start()

        _, stop_event, mod_ref = _start_proxy(sock_path, http_port, max_concurrent=4, timeout=5)

        # Wait for proxy to come up
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
            time.sleep(0.2)

        stop_event.set()

        self.assertTrue(proxy_up, "Proxy should eventually come up")
        # Proxy should have waited for SPDK — verify no zombie sockets
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
