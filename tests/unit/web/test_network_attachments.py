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
