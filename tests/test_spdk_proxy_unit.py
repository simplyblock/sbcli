# coding=utf-8
"""
test_spdk_proxy_unit.py – unit tests for spdk_http_proxy_server changes.
"""

import json
import socket
import threading
import time
import unittest
from unittest.mock import patch, MagicMock

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from conftest_proxy import import_proxy_module

proxy_mod = import_proxy_module()


class TestWaitForSpdkReady(unittest.TestCase):

    def setUp(self):
        proxy_mod.spdk_ready = False

    def tearDown(self):
        proxy_mod.spdk_ready = True

    def test_retries_until_spdk_responds(self):
        call_count = {"n": 0}

        def mock_socket_factory(*args, **kwargs):
            call_count["n"] += 1
            s = MagicMock()
            if call_count["n"] < 3:
                s.connect = MagicMock(side_effect=ConnectionRefusedError("not ready"))
            else:
                response = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"version": "24.01"}}).encode()
                s.connect = MagicMock()
                s.sendall = MagicMock()
                s.recv = MagicMock(side_effect=[response, b''])
            s.close = MagicMock()
            return s

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", side_effect=mock_socket_factory):
            with patch("simplyblock_core.services.spdk_http_proxy_server.time.sleep"):
                proxy_mod.wait_for_spdk_ready()

        self.assertTrue(proxy_mod.spdk_ready)
        self.assertEqual(call_count["n"], 3)

    def test_already_ready_returns_immediately(self):
        proxy_mod.spdk_ready = True
        proxy_mod.wait_for_spdk_ready()
        self.assertTrue(proxy_mod.spdk_ready)

    def test_socket_closed_on_connection_error(self):
        attempt = {"n": 0}
        socks = []

        def mock_socket_factory(*args, **kwargs):
            attempt["n"] += 1
            s = MagicMock()
            socks.append(s)
            if attempt["n"] == 1:
                s.connect = MagicMock(side_effect=OSError("no socket"))
            else:
                response = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {}}).encode()
                s.connect = MagicMock()
                s.sendall = MagicMock()
                s.recv = MagicMock(side_effect=[response, b''])
            s.close = MagicMock()
            return s

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", side_effect=mock_socket_factory):
            with patch("simplyblock_core.services.spdk_http_proxy_server.time.sleep"):
                proxy_mod.wait_for_spdk_ready()

        socks[0].close.assert_called()


class TestRpcCallInnerSocketCleanup(unittest.TestCase):

    def setUp(self):
        proxy_mod.unix_sockets.clear()

    def test_socket_closed_on_success(self):
        req_data = {"id": 1, "method": "test"}
        req = json.dumps(req_data).encode("ascii")
        response = json.dumps({"jsonrpc": "2.0", "id": 1, "result": True}).encode("ascii")

        mock_sock = MagicMock()
        mock_sock.recv = MagicMock(side_effect=[response, b''])

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", return_value=mock_sock):
            result = proxy_mod._rpc_call_inner(req, req_data, time.time_ns())

        self.assertIsNotNone(result)
        mock_sock.close.assert_called_once()
        self.assertEqual(len(proxy_mod.unix_sockets), 0)

    def test_socket_closed_on_timeout(self):
        req_data = {"id": 1, "method": "test"}
        req = json.dumps(req_data).encode("ascii")

        mock_sock = MagicMock()
        mock_sock.recv = MagicMock(side_effect=socket.timeout("timed out"))

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", return_value=mock_sock):
            with self.assertRaises(ValueError) as ctx:
                proxy_mod._rpc_call_inner(req, req_data, time.time_ns())

        self.assertIn("timeout", str(ctx.exception))
        mock_sock.close.assert_called_once()
        self.assertEqual(len(proxy_mod.unix_sockets), 0)

    def test_socket_closed_on_connect_error(self):
        req_data = {"id": 1, "method": "test"}
        req = json.dumps(req_data).encode("ascii")

        mock_sock = MagicMock()
        mock_sock.connect = MagicMock(side_effect=ConnectionRefusedError("refused"))

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", return_value=mock_sock):
            with self.assertRaises(ConnectionRefusedError):
                proxy_mod._rpc_call_inner(req, req_data, time.time_ns())

        mock_sock.close.assert_called_once()
        self.assertEqual(len(proxy_mod.unix_sockets), 0)

    def test_no_id_request_closes_socket(self):
        req_data = {"method": "notification_only"}
        req = json.dumps(req_data).encode("ascii")
        mock_sock = MagicMock()

        with patch("simplyblock_core.services.spdk_http_proxy_server.socket.socket", return_value=mock_sock):
            result = proxy_mod._rpc_call_inner(req, req_data, time.time_ns())

        self.assertIsNone(result)
        mock_sock.close.assert_called_once()
        self.assertEqual(len(proxy_mod.unix_sockets), 0)


class TestSemaphoreConcurrency(unittest.TestCase):

    def test_semaphore_limits_concurrency(self):
        max_concurrent = {"seen": 0, "current": 0}
        lock = threading.Lock()

        def mock_inner(req, req_data, req_time):
            with lock:
                max_concurrent["current"] += 1
                if max_concurrent["current"] > max_concurrent["seen"]:
                    max_concurrent["seen"] = max_concurrent["current"]
            time.sleep(0.05)
            with lock:
                max_concurrent["current"] -= 1
            return json.dumps({"jsonrpc": "2.0", "id": 1, "result": True})

        req = json.dumps({"id": 1, "method": "test"}).encode("ascii")

        with patch.object(proxy_mod, "_rpc_call_inner", side_effect=mock_inner):
            threads = []
            for _ in range(12):
                t = threading.Thread(target=proxy_mod.rpc_call, args=(req,))
                threads.append(t)
                t.start()
            for t in threads:
                t.join()

        self.assertLessEqual(max_concurrent["seen"], 4)

    def test_semaphore_released_on_exception(self):
        req = json.dumps({"id": 1, "method": "test"}).encode("ascii")

        with patch.object(proxy_mod, "_rpc_call_inner", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                proxy_mod.rpc_call(req)

        acquired = proxy_mod.spdk_semaphore.acquire(timeout=1)
        self.assertTrue(acquired)
        proxy_mod.spdk_semaphore.release()


class TestDoPostBrokenPipe(unittest.TestCase):

    def _make_handler(self):
        handler = proxy_mod.ServerHandler.__new__(proxy_mod.ServerHandler)
        proxy_mod.ServerHandler.server_session = []
        handler.key = "dGVzdDp0ZXN0"

        body = json.dumps({"id": 1, "method": "test"}).encode()
        header_map = {
            "Authorization": "Basic dGVzdDp0ZXN0",
            "Content-Length": str(len(body)),
        }
        handler.headers = MagicMock()
        handler.headers.__getitem__ = MagicMock(side_effect=lambda k: header_map.get(k, ""))
        handler.headers.__contains__ = MagicMock(side_effect=lambda k: k in header_map)
        handler.headers.get = MagicMock(side_effect=lambda k, d="": header_map.get(k, d))

        handler.rfile = MagicMock()
        handler.rfile.read = MagicMock(return_value=body)

        handler.wfile = MagicMock()
        handler.send_response = MagicMock()
        handler.send_header = MagicMock()
        handler.end_headers = MagicMock()

        return handler

    @patch.object(proxy_mod, "rpc_call")
    def test_broken_pipe_is_caught(self, mock_rpc):
        mock_rpc.return_value = '{"jsonrpc":"2.0","id":1,"result":true}'
        handler = self._make_handler()
        handler.wfile.write = MagicMock(side_effect=BrokenPipeError("client gone"))

        handler.do_POST()

        self.assertEqual(len(proxy_mod.ServerHandler.server_session), 0)

    @patch.object(proxy_mod, "rpc_call")
    def test_value_error_returns_500(self, mock_rpc):
        mock_rpc.side_effect = ValueError("bad response")
        handler = self._make_handler()

        handler.do_POST()

        handler.send_response.assert_any_call(500)
        self.assertEqual(len(proxy_mod.ServerHandler.server_session), 0)

    @patch.object(proxy_mod, "rpc_call")
    def test_session_always_cleaned_up(self, mock_rpc):
        mock_rpc.return_value = '{"jsonrpc":"2.0","id":1,"result":true}'
        handler = self._make_handler()

        handler.do_POST()

        self.assertEqual(len(proxy_mod.ServerHandler.server_session), 0)


if __name__ == "__main__":
    unittest.main()
