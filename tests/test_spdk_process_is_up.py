# coding=utf-8
"""
test_spdk_process_is_up.py — regression tests for the SPDK Unix-socket
probe in ``simplyblock_web.api.internal.storage_node.docker.spdk_process_is_up``.

Background: in incident 2026-04-24 (run /mnt/nfs_share/n_plus_k_failover_
multi_client_ha_all_nodes-20260424-104909) the auto-restart of vm203
failed with a 5 s read timeout against vm205's spdk_proxy at port 8084
even though vm205 was online. The original implementation called
``docker.containers.list(all=True)``; that walked the dockerd API and
under post-outage Swarm reconciliation the calls ramped 5→11→31→76→80s.

A first attempt fast-pathed via a TCP probe of the proxy port; that was
incorrect because the spdk_proxy_<port> container can stay up serving
HTTP errors while SPDK itself is dead — a false-positive case the proxy
heuristic can't detect.

Authoritative signal: SPDK's JSON-RPC Unix domain socket at
``/mnt/ramdisk/spdk_<port>/spdk.sock``. SPDK binds it; if connect()
succeeds, SPDK is alive AND polling. The probe is three-state because
older SnodeAPI deployments don't bind-mount /mnt/ramdisk:

  * True  — connect() succeeded (SPDK responsive)
  * False — socket path missing or connect() refused (SPDK down/wedged)
  * None  — /mnt/ramdisk not visible (legacy deploy) — fall through to
            dockerd

These tests pin: socket-alive ⇒ True without dockerd; socket-down ⇒
False without dockerd; ramdisk-absent ⇒ dockerd fall-through with a
short explicit timeout; dockerd raise ⇒ contained (no 500).
"""

import os
import socket as real_socket
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask

from simplyblock_web.api.internal.storage_node import docker as mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app():
    """``utils.get_response`` returns ``flask.jsonify(...)`` which requires
    an application context. Tests use a throwaway Flask app to run the
    handler under a real context, then assert on the JSON payload.
    """
    return Flask(__name__)


def _query(rpc_port=8084):
    q = MagicMock()
    q.rpc_port = rpc_port
    return q


def _payload(response):
    """Extract the JSON dict from a Flask Response object."""
    return response.get_json()


def _make_container_mock(running, status="running"):
    cont = MagicMock()
    cont.attrs = {"State": {"Running": running, "Status": status}}
    return cont


# ---------------------------------------------------------------------------
# _spdk_unix_socket_alive — probe primitive
# ---------------------------------------------------------------------------


class TestUnixSocketProbe(unittest.TestCase):

    def test_returns_none_when_ramdisk_root_missing(self):
        """Legacy SnodeAPI deployments don't bind-mount /mnt/ramdisk. The
        probe MUST signal "unknown" rather than False so the handler
        falls through to dockerd; a False here would let an old deploy
        report a perfectly healthy SPDK as down.
        """
        with patch.object(mod.os.path, "isdir", return_value=False):
            self.assertIsNone(mod._spdk_unix_socket_alive(8084))

    def test_returns_false_when_socket_file_missing(self):
        """/mnt/ramdisk is mounted but spdk_<port>/spdk.sock doesn't
        exist — SPDK is not running. False, not None.
        """
        with patch.object(mod.os.path, "isdir", return_value=True), \
             patch.object(mod.os.path, "exists", return_value=False):
            self.assertFalse(mod._spdk_unix_socket_alive(8084))

    def test_returns_true_on_connect_success(self):
        """The fast path: socket file exists and accepts connections.
        Verify the probe targets ``/mnt/ramdisk/spdk_<port>/spdk.sock``
        as an AF_UNIX stream socket with a short timeout.
        """
        captured = {}

        class _FakeSock:
            def __init__(self, family, kind):
                captured["family"] = family
                captured["kind"] = kind

            def settimeout(self, t):
                captured["timeout"] = t

            def connect(self, path):
                captured["path"] = path

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        with patch.object(mod.os.path, "isdir", return_value=True), \
             patch.object(mod.os.path, "exists", return_value=True), \
             patch.object(mod.socket, "socket", _FakeSock):
            self.assertTrue(mod._spdk_unix_socket_alive(8084))

        self.assertEqual(captured["family"], real_socket.AF_UNIX)
        self.assertEqual(captured["kind"], real_socket.SOCK_STREAM)
        self.assertEqual(captured["path"], "/mnt/ramdisk/spdk_8084/spdk.sock")
        self.assertLessEqual(captured["timeout"], 1.0,
                             "probe timeout must be small enough that a wedged "
                             "socket cannot stall the API handler")

    def test_returns_false_on_connect_oserror(self):
        """Stale socket file from a crashed SPDK (or SPDK process wedged
        hard enough not to accept) must be reported as False, not blow
        up the handler with an exception.
        """
        class _FakeSock:
            def __init__(self, *_a):
                pass

            def settimeout(self, _t):
                pass

            def connect(self, _path):
                raise OSError("simulated stale-socket")

            def __enter__(self):
                return self

            def __exit__(self, *_a):
                return False

        with patch.object(mod.os.path, "isdir", return_value=True), \
             patch.object(mod.os.path, "exists", return_value=True), \
             patch.object(mod.socket, "socket", _FakeSock):
            self.assertFalse(mod._spdk_unix_socket_alive(8084))


# ---------------------------------------------------------------------------
# spdk_process_is_up — handler-level behaviour
# ---------------------------------------------------------------------------


class TestSpdkProcessIsUpFastPath(unittest.TestCase):

    def test_socket_alive_returns_true_without_dockerd(self):
        """The whole point of the fix: when SPDK's Unix socket accepts,
        ``spdk_process_is_up`` MUST return True without consulting
        dockerd — that is precisely the dependency that took 76-80 s
        in the incident.
        """
        with patch.object(mod, "_spdk_unix_socket_alive", return_value=True), \
             patch.object(mod, "get_docker_client") as gdc:
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertTrue(data.get("status"))
        self.assertEqual(data.get("results"), True)
        gdc.assert_not_called()

    def test_socket_dead_returns_false_without_dockerd(self):
        """If we have a definitive False from the Unix-socket probe
        (file missing or connect refused), we don't need dockerd —
        SPDK is not responsive. Returning False directly is faster
        and avoids dockerd entirely on the unhealthy-SPDK path.
        """
        with patch.object(mod, "_spdk_unix_socket_alive", return_value=False), \
             patch.object(mod, "get_docker_client") as gdc:
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertFalse(data.get("status"))
        self.assertIn("Unix socket", data.get("error", ""))
        gdc.assert_not_called()


class TestSpdkProcessIsUpDockerdFallback(unittest.TestCase):
    """Fall-through path used only when /mnt/ramdisk is not mounted in
    the SnodeAPI container (legacy deployments). New deploys never hit
    this — they answer from the Unix socket.
    """

    def _patch_socket_unknown(self):
        # Probe returns None ⇒ /mnt/ramdisk not visible ⇒ fall through.
        return patch.object(mod, "_spdk_unix_socket_alive", return_value=None)

    def test_dockerd_called_with_short_timeout(self):
        """Dockerd fall-through MUST pass an explicit short timeout —
        the docker-py default is 60s and that is what made the
        original incident's stall observable.
        """
        with self._patch_socket_unknown(), \
             patch.object(mod, "get_docker_client") as gdc:
            cl = MagicMock()
            cl.containers.get.return_value = _make_container_mock(running=True)
            gdc.return_value = cl
            with _make_app().app_context():
                mod.spdk_process_is_up(_query(8084))
        self.assertEqual(gdc.call_count, 1)
        kwargs = gdc.call_args.kwargs
        timeout = kwargs.get("timeout")
        if timeout is None and gdc.call_args.args:
            timeout = gdc.call_args.args[0]
        self.assertIsNotNone(timeout, "dockerd client must get an explicit timeout")
        self.assertLessEqual(timeout, 10,
                             f"dockerd client timeout {timeout}s is too high; "
                             "must be small enough to fail fast under daemon backlog")

    def test_running_container_returns_true(self):
        with self._patch_socket_unknown(), \
             patch.object(mod, "get_docker_client") as gdc:
            cl = MagicMock()
            cl.containers.get.return_value = _make_container_mock(running=True)
            gdc.return_value = cl
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertTrue(data.get("status"))
        self.assertEqual(data.get("results"), True)
        cl.containers.get.assert_called_once_with("spdk_8084")
        # Must NOT fall back to the slow list path.
        cl.containers.list.assert_not_called()

    def test_stopped_container_returns_false_with_status(self):
        with self._patch_socket_unknown(), \
             patch.object(mod, "get_docker_client") as gdc:
            cl = MagicMock()
            cl.containers.get.return_value = _make_container_mock(
                running=False, status="exited")
            gdc.return_value = cl
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertFalse(data.get("status"))
        self.assertIn("exited", data.get("error", ""))

    def test_missing_container_returns_descriptive_error(self):
        """``containers.get`` raises ``docker.errors.NotFound`` when the
        container is gone. That has to surface as a clean False, not as
        an unhandled exception 500.
        """
        with self._patch_socket_unknown(), \
             patch.object(mod, "get_docker_client") as gdc:
            cl = MagicMock()
            cl.containers.get.side_effect = mod.docker.errors.NotFound(
                "no such container")
            gdc.return_value = cl
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertFalse(data.get("status"))
        self.assertIn("not found", data.get("error", "").lower())

    def test_dockerd_timeout_returns_false_does_not_raise(self):
        """If dockerd itself times out (the 5 s budget elapses) the
        handler must not raise — it must log and return a clean False.
        """
        with self._patch_socket_unknown(), \
             patch.object(mod, "get_docker_client",
                          side_effect=Exception("simulated dockerd timeout")):
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertFalse(data.get("status"))


# ---------------------------------------------------------------------------
# Deployment invariant: SnodeAPI container must mount /mnt/ramdisk
#
# Without the bind-mount the Unix-socket probe is permanently in the
# "unknown" state and every call falls through to dockerd. Pinning the
# mount in the launch volume list keeps the deploy and the probe in
# sync.
# ---------------------------------------------------------------------------


class TestSnodeAPIMountInvariant(unittest.TestCase):

    def test_storage_node_api_container_mounts_ramdisk(self):
        import os
        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "simplyblock_core", "storage_node_ops.py",
        )
        with open(path, "r") as f:
            src = f.read()

        fn_marker = "def start_storage_node_api_container("
        start = src.find(fn_marker)
        self.assertGreater(start, 0, "start_storage_node_api_container must exist")
        # Bound the search to the function body — find the next def/EOF.
        end = src.find("\ndef ", start + 1)
        body = src[start:end] if end > start else src[start:]
        self.assertIn("/mnt/ramdisk:/mnt/ramdisk", body,
                       "SNodeAPI container must bind-mount /mnt/ramdisk so "
                       "spdk_process_is_up can probe the SPDK Unix socket "
                       "without falling through to dockerd")


if __name__ == "__main__":
    unittest.main()
