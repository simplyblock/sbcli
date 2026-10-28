"""The SPDK pod's network attachments: the annotation Multus reads and the pod
leaving the host network when it has any (operator design-storage-network.md §5.2, P0-1)."""

import json
from unittest.mock import patch

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader
from pydantic import ValidationError

import tests.unit.test_storage_deploy_template as deploy_template
from simplyblock_core.storage_network import NetworkAttachment
from simplyblock_web.api.internal.storage_node import network_attachments

MGMT = NetworkAttachment(name="mgmt-net", namespace="infra", interface="mgmt0", ips=["10.0.0.5/24"])
DATA = NetworkAttachment(name="fabric", namespace="infra", interface="data0", ips=["10.10.10.101/24"])


def _render(**extra):
    env = Environment(loader=FileSystemLoader(str(deploy_template.TEMPLATE_DIR)),
                      trim_blocks=True, lstrip_blocks=True)
    template = env.get_template("storage_deploy_spdk.yaml.j2")
    values = dict(
        SPDK_IMAGE="spdk:test", L_CORES="0-1", SPDK_MEM=1024, CORES=2, SERVER_IP="10.0.0.10",
        RPC_PORT=8080, RPC_USERNAME="admin", RPC_PASSWORD="secret", HOSTNAME="node-a",
        NAMESPACE="simplyblock", SIMPLYBLOCK_DOCKER_IMAGE="proxy:test", GRAYLOG_SERVER_IP="10.0.0.20",
        MODE="kubernetes", CLUSTER_ID="cluster1", SSD_PCIE="none", PCI_ALLOWED="", TOTAL_HP="",
        NSOCKET=0, FW_PORT=50001, CPU_TOPOLOGY_ENABLED=False, MEM_MEGA=1536, MEM2_MEGA=1024,
        TLS_SERVE=False, TLS_CONNECT="disabled", TLS_PROVIDER="cert-manager",
    )
    values.update(extra)
    return next(d for d in yaml.safe_load_all(template.render(values)) if d and d.get("kind") == "Pod")


def _spdk(pod):
    return next(c for c in pod["spec"]["containers"] if c["name"] == "spdk-container")


def test_the_annotation_names_each_network_with_its_interface_and_address():
    value = json.loads(network_attachments.annotation([MGMT, DATA]))

    assert value == [
        {"name": "mgmt-net", "namespace": "infra", "interface": "mgmt0", "ips": ["10.0.0.5/24"]},
        {"name": "fabric", "namespace": "infra", "interface": "data0", "ips": ["10.10.10.101/24"]},
    ]


def test_no_attachments_give_no_annotation():
    assert network_attachments.annotation([]) == ""


def test_a_pod_with_attachments_is_off_the_host_network_and_carries_the_annotation():
    pod = _render(NETWORKS=network_attachments.annotation([DATA]))

    assert not pod["spec"].get("hostNetwork")
    assert json.loads(pod["metadata"]["annotations"]["k8s.v1.cni.cncf.io/networks"])[0]["interface"] == "data0"


def test_a_pod_without_attachments_is_unchanged():
    pod = _render()

    assert pod["spec"]["hostNetwork"] is True
    assert pod["spec"]["dnsPolicy"] == "ClusterFirstWithHostNet"
    assert "k8s.v1.cni.cncf.io/networks" not in (pod["metadata"].get("annotations") or {})
    assert set(_spdk(pod)["resources"]["limits"]) == {"hugepages-2Mi", "cpu"}


def _params(**extra):
    from simplyblock_web.api.internal.storage_node.kubernetes import SPDKParams

    return SPDKParams(server_ip="10.0.0.10", rpc_port=8080, rpc_username="u", rpc_password="p",
                      l_cores="0-1", namespace=None, cluster_ip="10.0.0.1", cluster_mode="kubernetes",
                      cluster_id="c1", **extra)


def test_a_field_an_older_operator_still_sends_is_ignored():
    body = _params(networks=[{"name": "fabric", "interface": "data0", "resource": "intel.com/sriov_netdevice"}])

    assert "resource" not in network_attachments.annotation(body.networks)


@pytest.mark.parametrize("attachment", [
    {"name": "fabric", "interface": "a-name-longer-than-fifteen"},
    {"name": "Fabric_Net", "interface": "data0"},
    {"name": "fabric", "namespace": "in fra", "interface": "data0"},
    {"name": "fabric", "interface": "data0", "ips": ['10.0.0.5"\n  - injected: yes']},
    {"name": "fabric", "interface": "data0", "ips": ["not-an-address"]},
])
def test_an_attachment_the_kernel_or_kubernetes_would_refuse_is_rejected(attachment):
    with pytest.raises(ValidationError):
        _params(networks=[attachment])


# The pod reports for itself: its address and the interfaces Multus attached.

STATUS = json.dumps([
    {"name": "cbr0", "interface": "eth0", "ips": ["10.42.1.5"], "default": True},
    {"name": "infra/mgmt-net", "interface": "mgmt0", "ips": ["10.0.0.5"]},
    {"name": "infra/fabric", "interface": "data0", "ips": ["10.10.10.101"]},
])


class _Pod:
    def __init__(self, phase="Running", pod_ip="10.42.1.5", status=STATUS):
        self.status = type("S", (), {"phase": phase, "pod_ip": pod_ip})()
        self.metadata = type("M", (), {"annotations": {network_attachments.NETWORK_STATUS: status} if status else {}})()


def test_the_report_names_the_pod_address_and_each_declared_interface():
    report = network_attachments.report(_Pod(), [MGMT, DATA])

    assert report == {
        "pod_ip": "10.42.1.5",
        "interfaces": [
            {"name": "mgmt0", "ips": ["10.0.0.5"]},
            {"name": "data0", "ips": ["10.10.10.101"]},
        ],
    }


def test_the_report_is_none_until_the_pod_is_running_and_holds_every_declared_interface():
    assert network_attachments.report(_Pod(phase="Pending"), [DATA]) is None
    assert network_attachments.report(_Pod(status=""), [DATA]) is None
    assert network_attachments.report(_Pod(), [DATA, NetworkAttachment(name="x", interface="data1")]) is None  # data1 is not held yet


def test_waiting_for_the_report_polls_until_the_pod_holds_its_interfaces():
    pods = iter([_Pod(phase="Pending"), _Pod(phase="Pending"), _Pod()])

    with patch.object(network_attachments, "POLL_INTERVAL_SEC", 0):
        report = network_attachments.wait_for_report(lambda: next(pods), [DATA], timeout=5)

    assert report["interfaces"][0]["name"] == "data0"


def test_waiting_for_the_report_gives_up_with_what_was_missing():
    with patch.object(network_attachments, "POLL_INTERVAL_SEC", 0.01), \
            pytest.raises(TimeoutError, match="data0"):
        network_attachments.wait_for_report(lambda: _Pod(phase="Pending"), [DATA], timeout=0.05)


def test_the_wait_ends_before_the_node_client_gives_up_and_retries():
    from simplyblock_core.snode_client import SNodeClient

    assert network_attachments.REPORT_TIMEOUT_SEC < SNodeClient.__init__.__defaults__[0]


def test_a_pod_with_networks_binds_its_proxy_to_the_management_address():
    pod = _render(NETWORKS=network_attachments.annotation([MGMT, DATA]),
                  SERVER_IP=network_attachments.server_ip([MGMT, DATA]))

    env = {e["name"]: e for c in pod["spec"]["containers"] if c["name"] == "spdk-proxy-container" for e in c["env"]}
    assert env["SERVER_IP"]["value"] == "10.0.0.5"


def test_a_pod_without_a_management_network_binds_its_proxy_to_its_own_address():
    pod = _render(NETWORKS=network_attachments.annotation([DATA]),
                  SERVER_IP=network_attachments.server_ip([DATA]))

    containers = {c["name"]: c for c in pod["spec"]["containers"]}
    env = {e["name"]: e for e in containers["spdk-proxy-container"]["env"]}
    assert env["SERVER_IP"]["value"] == network_attachments.POD_IP_REFERENCE
    assert env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"
    names = [e["name"] for e in containers["spdk-proxy-container"]["env"]]
    assert names.index("POD_IP") < names.index("SERVER_IP"), "$(POD_IP) expands only from an earlier entry"


def _render_init_job(**extra):
    env = Environment(loader=FileSystemLoader(str(deploy_template.TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
    values = dict(NAMESPACE="simplyblock", HOSTNAME="w1", JOBNAME="snode-spdk-job-w1", SIMPLYBLOCK_DOCKER_IMAGE="sb:test",
                  TLS_CONNECT="disabled", TLS_SERVE=False, TLS_PROVIDER="cert-manager", OPENSHIFT_MCP="", MODE="kubernetes")
    values.update(extra)
    docs = [d for d in yaml.safe_load_all(env.get_template("storage_init_job.yaml.j2").render(values)) if d]
    return docs[0]


def _init_container(job):
    return job["spec"]["template"]["spec"]["containers"][0]


def test_the_preparation_job_calls_the_agent_at_its_pod_address_when_the_agent_is_off_the_host_network():
    container = _init_container(_render_init_job(AGENT_IP="10.42.1.9"))

    env = {e["name"]: e for e in container["env"]}
    assert env["API_HOST"]["value"] == "10.42.1.9"
    script = "\n".join(container.get("command", []) + container.get("args", []))
    assert "${API_HOST:-$NODE_IP}:5000/snode/apply_config" in script


def test_the_preparation_job_still_calls_the_host_address_when_the_agent_is_on_the_host_network():
    job = _render_init_job()

    container = _init_container(job)
    assert "API_HOST" not in {e["name"] for e in container["env"]}
    script = "\n".join(container.get("command", []) + container.get("args", []))
    assert "${API_HOST:-$NODE_IP}:5000/snode/apply_config" in script


class TestTheAgentReportsTheWorkersNameNotItsPods:
    """With the agent in the pod network, hostname -s answers the pod's name. The
    control plane builds the node's per-worker DNS name from the reported hostname,
    so it has to be the worker's, which the DaemonSet hands over as HOSTNAME."""

    def test_off_the_host_network_the_worker_name_is_reported_without_its_domain(self, monkeypatch):
        from simplyblock_web.api.internal.storage_node import kubernetes

        monkeypatch.setenv("HOST_IP", "192.168.10.62")
        monkeypatch.setenv("HOSTNAME", "vm13.simplyblock2.localdomain")

        assert kubernetes.reported_hostname("simplyblock-storage-node-ds-x7k2p") == "vm13"

    def test_on_the_host_network_the_hostname_is_what_it_always_was(self, monkeypatch):
        from simplyblock_web.api.internal.storage_node import kubernetes

        monkeypatch.delenv("HOST_IP", raising=False)
        monkeypatch.setenv("HOSTNAME", "vm13.simplyblock2.localdomain")

        assert kubernetes.reported_hostname("vm13") == "vm13"

    def test_without_a_hostname_variable_the_command_stands(self, monkeypatch):
        from simplyblock_web.api.internal.storage_node import kubernetes

        monkeypatch.setenv("HOST_IP", "192.168.10.62")
        monkeypatch.delenv("HOSTNAME", raising=False)

        assert kubernetes.reported_hostname("pod-name") == "pod-name"
