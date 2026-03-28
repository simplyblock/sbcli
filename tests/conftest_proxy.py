# coding=utf-8
"""
conftest_proxy.py – helper to import spdk_http_proxy_server safely in tests.

The proxy module has module-level code that starts a server, so we must
mock the entire server startup chain before import.
"""

import json
import os
import socket
import sys
import threading
from unittest.mock import patch, MagicMock

# Windows compat
if not hasattr(socket, 'AF_UNIX'):
    socket.AF_UNIX = 1

# Required env vars
os.environ.setdefault("SERVER_IP", "127.0.0.1")
os.environ.setdefault("RPC_PORT", "19999")
os.environ.setdefault("RPC_USERNAME", "test")
os.environ.setdefault("RPC_PASSWORD", "test")
os.environ.setdefault("TIMEOUT", "5")
os.environ.setdefault("MAX_CONCURRENT_SPDK", "4")


def import_proxy_module():
    """Import spdk_http_proxy_server with all side-effects neutralized."""
    # Remove cached module
    sys.modules.pop("simplyblock_core.services.spdk_http_proxy_server", None)

    # Mock socket so wait_for_spdk_ready succeeds
    resp = json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"version": "1"}}).encode()
    mock_sock = MagicMock()
    mock_sock.recv = MagicMock(side_effect=[resp, b''])

    # Mock HTTPServer so serve_forever doesn't block
    mock_httpd = MagicMock()
    mock_httpd.serve_forever = MagicMock(return_value=None)

    with patch("socket.socket", return_value=mock_sock):
        with patch("http.server.HTTPServer", return_value=mock_httpd):
            with patch("http.server.ThreadingHTTPServer", return_value=mock_httpd):
                import simplyblock_core.services.spdk_http_proxy_server as mod

    # Reinitialize for testing
    mod.spdk_semaphore = threading.Semaphore(4)
    mod.spdk_ready = True
    mod.rpc_sock = "/tmp/fake_test.sock"
    mod.unix_sockets.clear()
    mod.TIMEOUT = 5

    return mod
