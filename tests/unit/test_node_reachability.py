"""The on-demand management-address probe (health_controller.management_reachability):
the storage-node monitor's own ping of each node's ``mgmt_ip``, once per
node, in parallel, one answer per node. Nodes are in-memory models; the probe
(health_controller._check_node_ping, the monitor's first check) is patched.
The per-cluster selection against the database is in
tests/integration/test_node_reachability.py."""
import threading
from unittest.mock import patch

from simplyblock_core.controllers import health_controller
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
