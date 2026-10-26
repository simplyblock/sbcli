"""The management node monitor's verdict on one node, in docker and in Kubernetes mode.

A Kubernetes deployment records its management node as 0.0.0.0 (cluster_ops.py),
because the control plane runs as pods and has no management address of its own.
The monitor pinged that address, which always answers because it is local, then
looked for it among the Kubernetes nodes' internal IPs, where it can never be,
and marked the record unreachable on every pass.
"""
from unittest.mock import MagicMock, patch

from simplyblock_core.services import mgmt_node_monitor as monitor


def _node(mode, mgmt_ip):
    node = MagicMock()
    node.mode = mode
    node.mgmt_ip = mgmt_ip
    node.hostname = ""
    return node


def _reachable(node, reachable_ips, ping=True):
    with patch.object(monitor.health_controller, "_check_node_ping", return_value=ping), \
            patch.object(monitor.time, "sleep"):
        return monitor.mgmt_node_reachable(node, reachable_ips)


def test_a_docker_node_that_answers_and_is_a_reachable_manager_is_online():
    assert _reachable(_node("docker", "10.0.0.5"), {"10.0.0.5"}) is True


def test_a_docker_node_that_does_not_answer_a_ping_is_unreachable():
    assert _reachable(_node("docker", "10.0.0.5"), {"10.0.0.5"}, ping=False) is False


def test_a_docker_node_the_swarm_does_not_report_reachable_is_unreachable():
    assert _reachable(_node("docker", "10.0.0.5"), {"10.0.0.9"}) is False


def test_the_kubernetes_record_is_online_while_a_kubernetes_node_is_ready():
    assert _reachable(_node("kubernetes", "0.0.0.0"), {"192.168.10.62"}) is True


def test_the_kubernetes_record_is_unreachable_when_no_kubernetes_node_is_ready():
    assert _reachable(_node("kubernetes", "0.0.0.0"), set()) is False
