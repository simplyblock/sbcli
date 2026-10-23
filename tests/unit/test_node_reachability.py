"""The on-demand management-address probe (health_controller.management_reachability):
the storage-node monitor's own ping of each node's ``mgmt_ip``, once per
node, in parallel, one answer per node. Nodes are in-memory models; the probe
(health_controller._check_node_ping, the monitor's first check) is patched.
The per-cluster selection against the database is in
tests/integration/test_node_reachability.py."""
import threading
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import health_controller
from simplyblock_core.exceptions import ReachabilityAddressError
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, ip, site=""):
    node = StorageNode()
    node.uuid = node_id
    node.mgmt_ip = ip
    node.site = site
    return node


def test_one_answer_per_node_in_order():
    nodes = [_node("n1", "10.0.0.1", "site-a"), _node("n2", "10.0.0.2", "site-a"),
             _node("n3", "10.0.0.3", "site-b")]
    with patch.object(health_controller, "_check_node_ping",
                      side_effect=lambda ip: ip != "10.0.0.2") as probe:
        answers = health_controller.management_reachability(nodes)
    assert answers == [
        health_controller.NodeReachability("n1", "10.0.0.1", "site-a", True),
        health_controller.NodeReachability("n2", "10.0.0.2", "site-a", False),
        health_controller.NodeReachability("n3", "10.0.0.3", "site-b", True),
    ]
    assert sorted(c.args[0] for c in probe.call_args_list) == ["10.0.0.1", "10.0.0.2", "10.0.0.3"]


def test_the_probe_is_the_monitors():
    """The monitor's node cycle pings mgmt_ip through _check_node_ping; the
    on-demand read uses that same function, not a probe of its own."""
    import inspect

    from simplyblock_core.services import storage_node_monitor
    assert "health_controller._check_node_ping(snode.mgmt_ip)" in inspect.getsource(
        storage_node_monitor.check_node)


def test_the_probes_run_in_parallel():
    nodes = [_node(f"n{i}", f"10.0.0.{i}") for i in range(4)]
    barrier = threading.Barrier(len(nodes), timeout=5)

    def _probe(ip):
        barrier.wait()          # breaks (BrokenBarrierError) unless all four run at once
        return True

    with patch.object(health_controller, "_check_node_ping", side_effect=_probe):
        answers = health_controller.management_reachability(nodes)
    assert [a.reachable for a in answers] == [True] * 4


def test_no_nodes_no_probe():
    with patch.object(health_controller, "_check_node_ping") as probe:
        assert health_controller.management_reachability([]) == []
    probe.assert_not_called()


# ---------------------------------------------------------------------------
# address_reachability: the probe against addresses that are not nodes yet
# (the follower operator's preflight, design-stretched §5.4)
# ---------------------------------------------------------------------------

def test_addresses_one_answer_each_in_request_order():
    addresses = ["192.0.2.1", "2001:db8::1", "192.0.2.2", "192.0.2.1"]
    with patch.object(health_controller, "_check_node_ping",
                      side_effect=lambda ip: ip != "192.0.2.2") as probe:
        answers = health_controller.address_reachability(addresses)
    assert answers == [
        health_controller.NodeReachability("", "192.0.2.1", "", True),
        health_controller.NodeReachability("", "2001:db8::1", "", True),
        health_controller.NodeReachability("", "192.0.2.2", "", False),
        health_controller.NodeReachability("", "192.0.2.1", "", True),
    ]
    assert sorted(c.args[0] for c in probe.call_args_list) == sorted(addresses)


def test_addresses_are_probed_in_parallel():
    addresses = [f"192.0.2.{i}" for i in range(1, 5)]
    barrier = threading.Barrier(len(addresses), timeout=5)

    def _probe(ip):
        barrier.wait()
        return True

    with patch.object(health_controller, "_check_node_ping", side_effect=_probe):
        answers = health_controller.address_reachability(addresses)
    assert [a.reachable for a in answers] == [True] * 4


def test_the_address_probes_share_the_node_probes_parallelism_cap():
    addresses = [f"192.0.2.{i}" for i in range(1, health_controller._REACHABILITY_PROBES_IN_PARALLEL + 5)]
    running, peak, lock = 0, 0, threading.Lock()
    release = threading.Event()

    def _probe(ip):
        nonlocal running, peak
        with lock:
            running += 1
            peak = max(peak, running)
            if running == health_controller._REACHABILITY_PROBES_IN_PARALLEL:
                release.set()
        release.wait(5)
        with lock:
            running -= 1
        return True

    with patch.object(health_controller, "_check_node_ping", side_effect=_probe):
        health_controller.address_reachability(addresses)
    assert peak == health_controller._REACHABILITY_PROBES_IN_PARALLEL


@pytest.mark.parametrize("address", [
    "", "node-1.example.com", "localhost", "192.0.2.256", "192.0.2", "-c 9", "192.0.2.1 ",
    "fe80::1%eth0", "10.0.0.0/8",
])
def test_a_non_literal_address_is_refused_before_any_probe(address):
    with patch.object(health_controller, "_check_node_ping") as probe, \
            patch.object(health_controller.utils, "ping_host") as ping, \
            pytest.raises(ReachabilityAddressError, match="IP address literal"):
        health_controller.address_reachability(["192.0.2.1", address])
    probe.assert_not_called()
    ping.assert_not_called()


def test_more_addresses_than_the_cap_are_refused():
    addresses = [f"192.0.2.{i % 250 + 1}" for i in range(health_controller.MAX_REACHABILITY_ADDRESSES + 1)]
    with patch.object(health_controller, "_check_node_ping") as probe, \
            pytest.raises(ReachabilityAddressError, match=str(health_controller.MAX_REACHABILITY_ADDRESSES)):
        health_controller.address_reachability(addresses)
    probe.assert_not_called()


def test_exactly_the_cap_is_allowed():
    addresses = [f"192.0.2.{i + 1}" for i in range(health_controller.MAX_REACHABILITY_ADDRESSES)]
    with patch.object(health_controller, "_check_node_ping", return_value=True):
        assert len(health_controller.address_reachability(addresses)) == len(addresses)
