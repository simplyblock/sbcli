"""The web API liveness probe fails exactly when the server stops answering."""

import socket
import threading
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from fastapi.testclient import TestClient

from simplyblock_web import healthcheck


class _Handler(BaseHTTPRequestHandler):
    status = 200

    def do_GET(self):
        self.send_response(self.status if self.path == healthcheck.LIVE_PATH else 404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):
        pass


def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_a_serving_api_is_live():
    server = _serve(_Handler)
    try:
        healthcheck.probe(server.server_address[1], tls=False, timeout=2)
    finally:
        server.shutdown()


def test_a_listening_socket_nobody_serves_is_not_live():
    """The field failure: the event loop is gone but the listen socket is open,
    so the kernel completes the connection and nothing ever answers it."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(16)
        with pytest.raises(OSError):
            healthcheck.probe(sock.getsockname()[1], tls=False, timeout=0.5)


def test_nothing_listening_is_not_live():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with pytest.raises(OSError):
        healthcheck.probe(port, tls=False, timeout=0.5)


def test_an_error_status_is_not_live():
    class Failing(_Handler):
        status = 500

    server = _serve(Failing)
    try:
        with pytest.raises(urllib.error.HTTPError):
            healthcheck.probe(server.server_address[1], tls=False, timeout=2)
    finally:
        server.shutdown()


def test_the_app_serves_the_probed_path_without_auth():
    """The probe holds no credentials, so the path must exist and be open."""
    from simplyblock_web.app import app

    response = TestClient(app).get(healthcheck.LIVE_PATH)

    assert response.status_code == 204
