"""The SPDK pod's network attachments: the annotation Multus reads, the
device-plugin resources the container requests, and the pod leaving the host
network when it has any (operator design-storage-network.md §5.2, P0-1)."""

import json

import yaml
from jinja2 import Environment, FileSystemLoader

import tests.unit.test_storage_deploy_template as deploy_template
from simplyblock_web import network_attachments
from simplyblock_web.network_attachments import NetworkAttachment

MGMT = NetworkAttachment(name="mgmt-net", namespace="infra", interface="mgmt0", ips=["10.0.0.5/24"])
DATA = NetworkAttachment(name="fabric", namespace="infra", interface="data0", ips=["10.10.10.101/24"])
VF = NetworkAttachment(name="vf-net", namespace="infra", interface="data1", ips=["10.20.0.5/24"],
                       resource="intel.com/sriov_netdevice")
VF2 = NetworkAttachment(name="vf-net-2", namespace="infra", interface="data2", ips=["10.21.0.5/24"],
                        resource="intel.com/sriov_netdevice")


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


def test_the_annotation_leaves_the_device_plugin_resource_out():
    assert "resource" not in network_attachments.annotation([VF])


def test_the_resources_count_one_unit_per_attachment_that_names_one():
    assert network_attachments.resources([MGMT, VF, VF2]) == {"intel.com/sriov_netdevice": 2}


def test_no_attachments_give_no_annotation_and_no_resources():
    assert network_attachments.annotation([]) == ""
    assert network_attachments.resources([]) == {}


def test_a_pod_with_attachments_is_off_the_host_network_and_carries_the_annotation():
    pod = _render(NETWORKS=network_attachments.annotation([DATA]), NETWORK_RESOURCES={})

    assert not pod["spec"].get("hostNetwork")
    assert json.loads(pod["metadata"]["annotations"]["k8s.v1.cni.cncf.io/networks"])[0]["interface"] == "data0"


def test_a_pod_with_attachments_requests_each_resource_on_the_spdk_container():
    pod = _render(NETWORKS=network_attachments.annotation([VF, VF2]),
                  NETWORK_RESOURCES=network_attachments.resources([VF, VF2]))

    resources = _spdk(pod)["resources"]
    assert resources["limits"]["intel.com/sriov_netdevice"] == 2
    assert resources["requests"]["intel.com/sriov_netdevice"] == 2


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


def test_the_start_request_carries_the_attachments():
    body = _params(networks=[{"name": "fabric", "interface": "data0", "ips": ["10.10.10.101/24"]}])

    assert body.networks[0].interface == "data0"


def test_the_start_request_without_attachments_is_as_before():
    assert _params().networks == []


def test_an_interface_name_the_kernel_would_refuse_is_rejected():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _params(networks=[{"name": "fabric", "interface": "a-name-longer-than-fifteen"}])


# The pod reports for itself: its address and the interfaces Multus attached.

STATUS = json.dumps([
    {"name": "cbr0", "interface": "eth0", "ips": ["10.42.1.5"], "mac": "aa:aa", "default": True},
    {"name": "infra/mgmt-net", "interface": "mgmt0", "ips": ["10.0.0.5"], "mac": "bb:bb"},
    {"name": "infra/fabric", "interface": "data0", "ips": ["10.10.10.101"], "mac": "cc:cc"},
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
            {"name": "mgmt0", "ips": ["10.0.0.5"], "mac": "bb:bb"},
            {"name": "data0", "ips": ["10.10.10.101"], "mac": "cc:cc"},
        ],
    }


def test_the_report_leaves_out_an_interface_nobody_declared():
    report = network_attachments.report(_Pod(), [DATA])

    assert [i["name"] for i in report["interfaces"]] == ["data0"]


def test_the_report_is_none_until_the_pod_is_running_and_holds_every_declared_interface():
    assert network_attachments.report(_Pod(phase="Pending"), [DATA]) is None
    assert network_attachments.report(_Pod(status=""), [DATA]) is None
    assert network_attachments.report(_Pod(), [DATA, VF]) is None  # data1 is not held yet


def test_waiting_for_the_report_polls_until_the_pod_holds_its_interfaces():
    pods = iter([_Pod(phase="Pending"), _Pod(phase="Pending"), _Pod()])

    report = network_attachments.wait_for_report(lambda: next(pods), [DATA], timeout=5, sleep=lambda s: None)

    assert report["interfaces"][0]["name"] == "data0"


def test_waiting_for_the_report_gives_up_with_what_was_missing():
    import pytest

    clock = iter(range(0, 1000, 10))
    with pytest.raises(TimeoutError, match="data0"):
        network_attachments.wait_for_report(
            lambda: _Pod(phase="Pending"), [DATA], timeout=30, sleep=lambda s: None, now=lambda: next(clock))


def test_a_pod_with_networks_binds_its_proxy_to_the_management_address():
    pod = _render(NETWORKS=network_attachments.annotation([MGMT, DATA]), NETWORK_RESOURCES={},
                  SERVER_IP=network_attachments.server_ip([MGMT, DATA]))

    env = {e["name"]: e for c in pod["spec"]["containers"] if c["name"] == "spdk-proxy-container" for e in c["env"]}
    assert env["SERVER_IP"]["value"] == "10.0.0.5"


def test_a_pod_without_a_management_network_binds_its_proxy_to_its_own_address():
    pod = _render(NETWORKS=network_attachments.annotation([DATA]), NETWORK_RESOURCES={},
                  SERVER_IP=network_attachments.server_ip([DATA]))

    containers = {c["name"]: c for c in pod["spec"]["containers"]}
    env = {e["name"]: e for e in containers["spdk-proxy-container"]["env"]}
    assert env["SERVER_IP"]["value"] == "$(POD_IP)"
    assert env["POD_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.podIP"
    names = [e["name"] for e in containers["spdk-proxy-container"]["env"]]
    assert names.index("POD_IP") < names.index("SERVER_IP"), "$(POD_IP) expands only from an earlier entry"


class TestTheStartEndpointReportsTheNetworksItAttached:
    """Structural, in the manner of test_add_node_pod_cleanup: the endpoint needs a
    cluster to run. The report itself is covered above."""

    def _source(self):
        import inspect

        from simplyblock_web.api.internal.storage_node import kubernetes

        return inspect.getsource(kubernetes.spdk_process_start)

    def test_the_proxy_address_comes_from_the_attachments_when_there_are_any(self):
        assert "network_attachments.server_ip(body.networks) or body.server_ip" in self._source()

    def test_it_waits_for_the_pod_to_report_only_when_it_attached_networks(self):
        source = self._source()
        assert "if body.networks:" in source and "network_attachments.wait_for_report(" in source


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


def test_the_start_endpoint_gives_the_job_the_agents_own_address_when_networks_are_attached():
    import inspect

    from simplyblock_web.api.internal.storage_node import kubernetes

    assert "'AGENT_IP': os.environ.get(\"POD_IP\", \"\") if body.networks else \"\"" in inspect.getsource(kubernetes.spdk_process_start)


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

    def test_the_info_endpoint_reports_it(self):
        import inspect

        from simplyblock_web.api.internal.storage_node import kubernetes

        assert 'reported_hostname(node_info["hostname"])' in inspect.getsource(kubernetes.get_info)
