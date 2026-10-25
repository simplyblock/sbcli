"""SPDK pod on the pod network: Multus attachment instead of hostNetwork.

Covers the agent side (template, attachment selection, network-status parsing,
the wait for the pod's addresses) and the control-plane side (how the agent's
answer becomes the node's data NIC and RPC host).
"""
import json
from types import SimpleNamespace
from typing import ClassVar

import pytest
import yaml
from jinja2 import Environment, PackageLoader
from pydantic import SecretStr

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_web.api.internal.storage_node import kubernetes as k8s_agent

# The annotation Multus wrote on the lab's test pod (worker-4, 2026-10-25).
LAB_NETWORK_STATUS = json.dumps([
    {"name": "cbr0", "interface": "eth0", "ips": ["10.244.5.144"], "mac": "8e:72:67:7c:e8:73",
     "default": True, "dns": {}, "gateway": ["10.244.5.1"]},
    {"name": "simplyblock/storage-vf", "interface": "net1", "ips": ["10.10.10.220"],
     "mac": "4e:cd:25:c1:29:a6", "dns": {}},
])


def _render(**overrides):
    values = {
        'SPDK_IMAGE': 'ultra:test', 'L_CORES': '0@1', 'CORES': 1, 'SPDK_MEM': 1024,
        'MEM_MEGA': 1536, 'MEM2_MEGA': 512, 'SERVER_IP': '192.168.10.145', 'RPC_PORT': 4420,
        'RPC_USERNAME': 'u', 'RPC_PASSWORD': 'p', 'HOSTNAME': 'worker-4', 'JOBNAME': 'j',
        'UBUNTU_JOBNAME': 'u', 'CORE_JOBNAME': 'c', 'NAMESPACE': 'simplyblock',
        'FDB_CONNECTION': '', 'SIMPLYBLOCK_DOCKER_IMAGE': 'sb:test', 'GRAYLOG_SERVER_IP': '1.2.3.4',
        'MODE': 'kubernetes', 'CLUSTER_ID': 'abcdef', 'SSD_PCIE': 'none', 'PCI_ALLOWED': '',
        'TOTAL_HP': '', 'NSOCKET': 0, 'CPU_TOPOLOGY_ENABLED': False, 'RESERVED_SYSTEM_CPUS': '0,1',
        'MCP_MAX_UNAVAILABLE': '1', 'OPENSHIFT_MCP': None, 'TLS_SERVE': False,
        'TLS_CONNECT': 'disabled', 'TLS_CLIENT_AUTH': 'none', 'TLS_PROVIDER': 'cert-manager',
        'NETWORK_ATTACHMENT': None,
    }
    values.update(overrides)
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
    docs = list(yaml.safe_load_all(env.get_template('storage_deploy_spdk.yaml.j2').render(values)))
    return docs[0]


def _proxy_env(pod):
    proxy = next(c for c in pod['spec']['containers'] if c['name'] == 'spdk-proxy-container')
    return {e['name']: e.get('value') for e in proxy['env']}


class TestTemplate:
    def test_default_stays_host_network(self):
        pod = _render()
        assert pod['spec']['hostNetwork'] is True
        assert pod['spec']['dnsPolicy'] == 'ClusterFirstWithHostNet'
        assert 'k8s.v1.cni.cncf.io/networks' not in pod['metadata'].get('annotations', {})
        assert _proxy_env(pod)['SERVER_IP'] == '192.168.10.145'

    def test_attachment_moves_pod_off_host_network(self):
        pod = _render(NETWORK_ATTACHMENT='storage-vf', SERVER_IP='0.0.0.0')
        assert pod['spec']['hostNetwork'] is False
        assert pod['spec']['dnsPolicy'] == 'ClusterFirst'
        assert pod['metadata']['annotations']['k8s.v1.cni.cncf.io/networks'] == 'storage-vf'
        # the log-collector annotation of kubernetes mode survives the merge
        assert pod['metadata']['annotations']['log-collector/enabled'] == 'true'
        assert _proxy_env(pod)['SERVER_IP'] == '0.0.0.0'

    def test_attachment_outside_kubernetes_mode_still_annotates(self):
        pod = _render(NETWORK_ATTACHMENT='storage-vf', MODE='docker')
        assert pod['metadata']['annotations'] == {'k8s.v1.cni.cncf.io/networks': 'storage-vf'}


class TestAttachmentSelection:
    @staticmethod
    def _body(value):
        return SimpleNamespace(network_attachment=value)

    def test_unset_everywhere_means_host_network(self, monkeypatch):
        monkeypatch.delenv("SPDK_NETWORK_ATTACHMENT", raising=False)
        assert k8s_agent._spdk_network_attachment(self._body(None)) is None

    def test_agent_env_is_the_default(self, monkeypatch):
        monkeypatch.setenv("SPDK_NETWORK_ATTACHMENT", " storage-vf ")
        assert k8s_agent._spdk_network_attachment(self._body(None)) == "storage-vf"

    def test_request_value_overrides_env(self, monkeypatch):
        monkeypatch.setenv("SPDK_NETWORK_ATTACHMENT", "storage-vf")
        assert k8s_agent._spdk_network_attachment(self._body("other-net")) == "other-net"

    def test_empty_request_value_forces_host_network(self, monkeypatch):
        monkeypatch.setenv("SPDK_NETWORK_ATTACHMENT", "storage-vf")
        assert k8s_agent._spdk_network_attachment(self._body("")) is None

    def test_server_ip_follows_attachment(self, monkeypatch):
        """The proxy binds every pod address on the pod network: the node IP is not there."""
        body = SimpleNamespace(network_attachment="storage-vf")
        assert k8s_agent._spdk_network_attachment(body) == "storage-vf"


class TestNetworkStatus:
    def test_lab_annotation_by_short_name(self):
        assert k8s_agent._attachment_address(LAB_NETWORK_STATUS, "storage-vf", "simplyblock") == \
            {"ip": "10.10.10.220", "interface": "net1"}

    def test_lab_annotation_by_qualified_name(self):
        assert k8s_agent._attachment_address(LAB_NETWORK_STATUS, "simplyblock/storage-vf", "other")["ip"] \
            == "10.10.10.220"

    def test_unknown_attachment(self):
        assert k8s_agent._attachment_address(LAB_NETWORK_STATUS, "nope", "simplyblock") is None

    def test_entry_without_address_yet(self):
        status = json.dumps([{"name": "simplyblock/storage-vf", "interface": "net1", "ips": []}])
        assert k8s_agent._attachment_address(status, "storage-vf", "simplyblock") is None

    @pytest.mark.parametrize("raw", [None, "", "not json", "{}"])
    def test_garbage_is_not_an_address(self, raw):
        assert k8s_agent._attachment_address(raw, "storage-vf", "simplyblock") is None


def _pod(phase, pod_ip=None, status=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(annotations={k8s_agent.NETWORK_STATUS_ANNOTATION: status} if status else {}),
        status=SimpleNamespace(phase=phase, pod_ip=pod_ip))


class _FakeCore:
    def __init__(self, pods):
        self.pods = list(pods)
        self.calls = 0

    def read_namespaced_pod(self, name, namespace):
        self.calls += 1
        return self.pods.pop(0) if len(self.pods) > 1 else self.pods[0]


class TestWaitForPodNetwork:
    def test_returns_addresses_once_multus_wired_the_pod(self):
        core = _FakeCore([
            _pod("Pending"),
            _pod("Running", pod_ip="10.244.5.144"),            # pod IP, attachment not yet
            _pod("Running", pod_ip="10.244.5.144", status=LAB_NETWORK_STATUS),
        ])
        got = k8s_agent._wait_for_pod_network(core, "snode-spdk-pod-4420-abcdef", "simplyblock",
                                              "storage-vf", timeout=5, poll_interval=0.01)
        assert got == {"pod_name": "snode-spdk-pod-4420-abcdef", "pod_ip": "10.244.5.144",
                       "data_ip": "10.10.10.220", "data_ifname": "net1",
                       "network_attachment": "storage-vf"}
        assert core.calls == 3

    def test_dead_pod_fails_fast(self):
        core = _FakeCore([_pod("Failed")])
        with pytest.raises(RuntimeError, match="reached phase Failed"):
            k8s_agent._wait_for_pod_network(core, "p", "ns", "storage-vf", timeout=5, poll_interval=0.01)
        assert core.calls == 1

    def test_times_out_when_attachment_never_appears(self):
        core = _FakeCore([_pod("Running", pod_ip="10.244.5.144")])
        with pytest.raises(RuntimeError, match="not ready yet"):
            k8s_agent._wait_for_pod_network(core, "p", "ns", "storage-vf", timeout=0.05, poll_interval=0.01)


class TestControlPlane:
    AGENT_ANSWER: ClassVar[dict] = {
        "pod_name": "snode-spdk-pod-4420-abcdef", "pod_ip": "10.244.5.144",
        "data_ip": "10.10.10.220", "data_ifname": "net1", "network_attachment": "storage-vf"}

    def test_host_network_answer_is_not_a_pod_network(self):
        assert storage_node_ops._spdk_pod_network("Pod created: 'x' in namespace 'y'") is None
        assert storage_node_ops._spdk_pod_network(True) is None
        assert storage_node_ops._spdk_pod_network({"pod_ip": "10.244.5.144"}) is None

    def test_pod_network_answer_becomes_the_data_nic(self):
        assert storage_node_ops._spdk_pod_network(self.AGENT_ANSWER) is self.AGENT_ANSWER
        nics = storage_node_ops._pod_network_data_nics(self.AGENT_ANSWER)
        assert len(nics) == 1
        assert nics[0].if_name == "net1"
        assert nics[0].ip4_address == "10.10.10.220"
        assert nics[0].trtype == "TCP"

    def test_data_ifname_defaults_to_net1(self):
        answer = dict(self.AGENT_ANSWER, data_ifname="")
        assert storage_node_ops._pod_network_data_nics(answer)[0].if_name == "net1"

    def test_rpc_client_dials_the_pod_ip(self, monkeypatch):
        monkeypatch.setenv("SB_TLS_CONNECT", "disabled")
        monkeypatch.setenv("SB_TLS_SERVE", "false")
        node = StorageNode()
        node.mgmt_ip, node.rpc_port = "192.168.10.145", 4420
        node.rpc_username, node.rpc_password = "u", SecretStr("p")
        assert node.rpc_client().host == "192.168.10.145"
        node.pod_ip = "10.244.5.144"
        assert node.rpc_client().host == "10.244.5.144"

    def test_new_fields_round_trip(self):
        node = StorageNode()
        node.pod_ip, node.network_attachment = "10.244.5.144", "storage-vf"
        back = StorageNode(node.to_dict())
        assert (back.pod_ip, back.network_attachment) == ("10.244.5.144", "storage-vf")
        assert StorageNode().pod_ip == "" and StorageNode().network_attachment == ""


class TestDataNicProbe:
    """The monitor's data-NIC probe must not look for the attachment on the host."""

    @staticmethod
    def _ping(agent, **query):
        from flask import Flask
        with Flask(__name__).app_context():
            return agent.ping_ip(agent.PingQuery(**query)).get_json()

    def test_agent_ping_without_ifname_is_reachability_only(self, monkeypatch):
        from simplyblock_web.api.internal.storage_node import docker as agent
        calls = []
        monkeypatch.setattr(agent.subprocess, "check_call", lambda cmd, **kw: calls.append(cmd))
        assert self._ping(agent, ip="10.10.10.220")["results"] is True
        assert calls and calls[0][-1] == "10.10.10.220"

    def test_agent_ping_with_ifname_still_reads_carrier(self, monkeypatch):
        from simplyblock_web.api.internal.storage_node import docker as agent
        monkeypatch.setattr(agent.subprocess, "check_call", lambda cmd, **kw: None)
        # carrier file of a nonexistent host interface -> the error path, as before
        assert self._ping(agent, ip="10.10.10.145", ifname="does-not-exist0")["status"] is False

    def test_control_plane_drops_ifname_for_pod_networked_node(self):
        from unittest.mock import MagicMock

        from simplyblock_core.controllers import health_controller
        node = StorageNode()
        api = MagicMock()
        api.ping_ip.return_value = (True, None)
        node.client = lambda **kw: api
        node.network_attachment = "storage-vf"
        assert health_controller._check_ping_from_node("10.10.10.220", ifname="net1", node=node) is True
        api.ping_ip.assert_called_once_with("10.10.10.220", None)
        node.network_attachment = ""
        health_controller._check_ping_from_node("10.10.10.145", ifname="enp1s0", node=node)
        api.ping_ip.assert_called_with("10.10.10.145", "enp1s0")

    def test_client_omits_ifname_when_none(self):
        from unittest.mock import MagicMock

        from simplyblock_core.snode_client import SNodeClient
        c = SNodeClient.__new__(SNodeClient)
        c._request = MagicMock(return_value=(True, None))
        c.ping_ip("10.10.10.220")
        c._request.assert_called_once_with("GET", "ping_ip", {"ip": "10.10.10.220"})
        c.ping_ip("10.10.10.145", "enp1s0")
        c._request.assert_called_with("GET", "ping_ip", {"ip": "10.10.10.145", "ifname": "enp1s0"})
