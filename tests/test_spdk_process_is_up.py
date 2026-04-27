# coding=utf-8
"""
test_spdk_process_is_up.py — regression tests for the TCP fast-path in
``simplyblock_web.api.internal.storage_node.docker.spdk_process_is_up``.

Background: in incident 2026-04-24 (run /mnt/nfs_share/n_plus_k_failover_
multi_client_ha_all_nodes-20260424-104909) the auto-restart of vm203
failed with a 5 s read timeout against vm205's spdk_proxy at port 8084
even though vm205 was online. Two latencies stacked:

  1. ``spdk_process_is_up`` on vm205 ran ``docker.containers.list(all=True)``
     which took **76-80 seconds**, ramping 5→11→31→76→80s as the daemon
     queue backed up on Swarm reconciliation after the peer outage.
  2. The SPDK proxy itself was responding in 1-3 s (reactor back-pressure).

Fix: make ``spdk_process_is_up`` answer from a sub-millisecond TCP probe
of the SPDK proxy port, only consulting dockerd as a fallback, and bound
the dockerd fall-through with an explicit short timeout. These tests pin
the new behaviour:

  * TCP-open ⇒ ``True`` without touching dockerd.
  * TCP-closed + container running ⇒ ``True`` (via ``containers.get``).
  * TCP-closed + container missing ⇒ ``False`` with descriptive error.
  * TCP-closed + dockerd raising (timeout / wedged) ⇒ ``False``.
  * The dockerd client is constructed with a short, explicit timeout.
"""

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
# TCP fast-path
# ---------------------------------------------------------------------------


class TestSpdkProcessIsUpFastPath(unittest.TestCase):

    def test_tcp_open_returns_true_without_dockerd(self):
        """The whole point of the fix: when the SPDK proxy answers TCP,
        ``spdk_process_is_up`` must return True without consulting dockerd
        — the docker socket is precisely the dependency that took 76-80 s
        in the incident.
        """
        with patch.object(mod, "_spdk_proxy_tcp_open", return_value=True), \
             patch.object(mod, "get_docker_client") as gdc:
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertTrue(data.get("status"))
        self.assertEqual(data.get("results"), True)
        gdc.assert_not_called()

    def test_tcp_probe_targets_loopback_with_short_timeout(self):
        """The probe must hit 127.0.0.1 (the local host where the proxy
        is bound) with a small timeout — it must never block the API
        handler waiting for an external host.
        """
        captured = {}

        def fake_create_connection(addr, timeout=None):
            captured["addr"] = addr
            captured["timeout"] = timeout
            raise OSError("simulated closed port for assertion only")

        with patch.object(mod.socket, "create_connection",
                          side_effect=fake_create_connection):
            mod._spdk_proxy_tcp_open(8084)
        self.assertEqual(captured["addr"], ("127.0.0.1", 8084))
        # Probe timeout must be small enough that a wedged socket
        # cannot stall the handler. 1 second matches the function default
        # and is the contract we want to pin.
        self.assertLessEqual(captured["timeout"], 1.0)


# ---------------------------------------------------------------------------
# Slow-path fall-through to dockerd
# ---------------------------------------------------------------------------


class TestSpdkProcessIsUpDockerdFallback(unittest.TestCase):

    def _patch_tcp_closed(self):
        return patch.object(mod, "_spdk_proxy_tcp_open", return_value=False)

    def test_dockerd_called_with_short_timeout(self):
        """When the TCP probe fails we *must* pass an explicit short
        timeout to ``get_docker_client`` — the docker-py default is 60 s
        and that is what made the original incident's stall observable.
        """
        with self._patch_tcp_closed(), \
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
        # 5s is the proposed value; allow up to 10s but never the 60s default.
        self.assertLessEqual(timeout, 10,
                             f"dockerd client timeout {timeout}s is too high; "
                             "must be small enough to fail fast under daemon backlog")

    def test_running_container_returns_true(self):
        with self._patch_tcp_closed(), \
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
        with self._patch_tcp_closed(), \
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
        with self._patch_tcp_closed(), \
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
        with self._patch_tcp_closed(), \
             patch.object(mod, "get_docker_client",
                          side_effect=Exception("simulated dockerd timeout")):
            with _make_app().app_context():
                resp = mod.spdk_process_is_up(_query(8084))
            data = _payload(resp)
        self.assertFalse(data.get("status"))


if __name__ == "__main__":
    unittest.main()
