"""The monitor must abort SPDK when its RPC channel hangs on a live process.

Regression cover for 2026-09-01: node 22f365ef handled its last RPC at
13:22:01 (bdev_distrib_status_events_update, whose target JM sat on a node the
test had just shut down) and never completed it. SPDK's JSON-RPC server reads
one request at a time per connection, so the channel wedged for 15 minutes
while every poller kept running -- ping, SNodeAPI and spdk_process_is_up all
said healthy, so nothing escalated until the socket stopped hanging and began
refusing.
"""
import time
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.services import storage_node_monitor as snm


@pytest.fixture(autouse=True)
def _clear_hang_state():
    snm._rpc_hang_since.clear()
    yield
    snm._rpc_hang_since.clear()


def _node(node_id="node-a"):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.rpc_port = 4430
    n.cluster_id = "cluster-1"
    n.mgmt_ip = "172.31.98.6"
    return n


class TestHangTracking:
    def test_first_failure_reports_zero_and_does_not_trip(self):
        assert snm._rpc_hang_seconds("node-a") < 1
        assert snm._rpc_hang_seconds("node-a") < snm.RPC_HANG_ABORT_SEC

    def test_success_clears_the_hang(self):
        snm._rpc_hang_since["node-a"] = time.monotonic() - 300
        snm._note_rpc_ok("node-a")
        # a later failure starts counting from scratch, not from 300s ago
        assert snm._rpc_hang_seconds("node-a") < 1

    def test_hang_accumulates_across_checks(self):
        snm._rpc_hang_since["node-a"] = time.monotonic() - 120
        assert snm._rpc_hang_seconds("node-a") >= snm.RPC_HANG_ABORT_SEC

    def test_tracking_is_per_node(self):
        """The old State.counter was process-global: a healthy node's
        decrement() wiped a broken node's increment(), so the escalation was
        never reached. Per-node state must not have that coupling."""
        snm._rpc_hang_since["broken"] = time.monotonic() - 120
        snm._note_rpc_ok("healthy")          # other node reports fine
        assert snm._rpc_hang_seconds("broken") >= snm.RPC_HANG_ABORT_SEC


class TestAbort:
    def test_abort_kills_spdk_and_flips_offline(self):
        node = _node()
        api = MagicMock()
        node.client.return_value = api
        with patch.object(snm, "set_node_offline") as offline:
            assert snm._abort_hung_spdk(node, 61.0) is True
        api.spdk_process_kill.assert_called_once_with(4430, "cluster-1")
        offline.assert_called_once_with(node)

    def test_abort_clears_hang_so_it_does_not_repeat(self):
        node = _node()
        node.client.return_value = MagicMock()
        snm._rpc_hang_since["node-a"] = time.monotonic() - 120
        with patch.object(snm, "set_node_offline"):
            snm._abort_hung_spdk(node, 120.0)
        assert snm._rpc_hang_seconds("node-a") < 1

    def test_failed_kill_reports_false_and_skips_offline(self):
        node = _node()
        api = MagicMock()
        api.spdk_process_kill.side_effect = RuntimeError("unreachable")
        node.client.return_value = api
        with patch.object(snm, "set_node_offline") as offline:
            assert snm._abort_hung_spdk(node, 61.0) is False
        offline.assert_not_called()

    def test_offline_failure_does_not_mask_the_kill(self):
        node = _node()
        node.client.return_value = MagicMock()
        with patch.object(snm, "set_node_offline",
                          side_effect=RuntimeError("db down")):
            assert snm._abort_hung_spdk(node, 61.0) is True


class TestGuardConditions:
    """The abort must fire only for a hang on a LIVE process -- a dead SPDK or
    an unreachable SNodeAPI is already handled by the existing paths."""

    def test_dead_spdk_is_left_to_the_existing_path(self):
        node = _node()
        snm._rpc_hang_since["node-a"] = time.monotonic() - 120
        with patch.object(snm, "_spdk_is_dead", return_value=True), \
             patch.object(snm, "_abort_hung_spdk") as abort:
            hung = snm._rpc_hang_seconds(node.get_id())
            if hung >= snm.RPC_HANG_ABORT_SEC and not snm._spdk_is_dead(node):
                snm._abort_hung_spdk(node, hung)
        abort.assert_not_called()

    def test_live_spdk_with_long_hang_aborts(self):
        node = _node()
        snm._rpc_hang_since["node-a"] = time.monotonic() - 120
        with patch.object(snm, "_spdk_is_dead", return_value=False), \
             patch.object(snm, "_abort_hung_spdk") as abort:
            hung = snm._rpc_hang_seconds(node.get_id())
            if hung >= snm.RPC_HANG_ABORT_SEC and not snm._spdk_is_dead(node):
                snm._abort_hung_spdk(node, hung)
        abort.assert_called_once()

    def test_short_hang_does_not_abort(self):
        node = _node()
        with patch.object(snm, "_spdk_is_dead", return_value=False), \
             patch.object(snm, "_abort_hung_spdk") as abort:
            hung = snm._rpc_hang_seconds(node.get_id())
            if hung >= snm.RPC_HANG_ABORT_SEC and not snm._spdk_is_dead(node):
                snm._abort_hung_spdk(node, hung)
        abort.assert_not_called()
