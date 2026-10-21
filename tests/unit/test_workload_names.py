"""Unit tests for a cluster's workload names: the Services a storage node is
reached through, the SPDK proxy's certificate Secret, and the node label
suffix. A cluster record and a node record that predate them read as the
constant names, so a running cluster's hostnames do not change on upgrade."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml
from jinja2 import Environment, FileSystemLoader

from simplyblock_core import constants
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClient
from simplyblock_web.api.internal.storage_node.kubernetes import SPDKParams

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "simplyblock_web" / "templates"

NAMES = {
    "snode_api_service": "sb-prod-node-api",
    "spdk_proxy_service": "sb-prod-spdk-proxy",
    "spdk_proxy_tls_secret": "sb-prod-spdk-proxy-tls",
    "node_label_suffix": "-abc",
}


def test_a_cluster_record_without_workload_names_reads_as_the_constant_names():
    cluster = Cluster().from_dict({"uuid": "c1"})

    assert cluster.snode_api_service == "simplyblock-storage-node-api"
    assert cluster.spdk_proxy_service == "simplyblock-spdk-proxy"
    assert cluster.spdk_proxy_tls_secret == "simplyblock-spdk-proxy-tls"
    assert cluster.node_label_suffix == ""


def test_a_node_record_without_workload_names_reads_as_the_constant_names():
    node = StorageNode().from_dict({"uuid": "n1"})

    assert node.snode_api_service == constants.K8S_SNODE_API_SERVICE
    assert node.spdk_proxy_service == constants.K8S_SPDK_PROXY_SERVICE
    assert node.spdk_proxy_tls_secret == constants.K8S_SPDK_PROXY_TLS_SECRET
    assert node.node_label_suffix == ""


def test_a_node_takes_its_clusters_workload_names():
    cluster = Cluster().from_dict({"uuid": "c1", **NAMES})
    node = StorageNode()

    node.record_workload_names(cluster)

    for field, value in NAMES.items():
        assert getattr(node, field) == value


def _tls_node(**fields) -> StorageNode:
    return StorageNode().from_dict({
        "uuid": "n1",
        "hostname": "worker-1_8080",
        "rpc_port": 8080,
        "api_endpoint": "10.0.0.1:50001",
        "mgmt_ip": "10.0.0.1",
        "cr_namespace": "storage",
        **fields,
    })


def _hosts(node: StorageNode) -> tuple[str, str]:
    """The host the node API client and the RPC client are built with."""
    settings = MagicMock(tls_connect="authenticated")
    with patch("simplyblock_core.models.storage_node.Settings", return_value=settings), \
            patch("simplyblock_core.models.storage_node.SNodeClient") as snode_client, \
            patch("simplyblock_core.models.storage_node.RPCClient") as rpc_client:
        node.client()
        node.rpc_client()
    return snode_client.call_args.args[0], rpc_client.call_args.args[0]


def test_a_node_without_workload_names_is_reached_through_the_constant_services():
    api_host, rpc_host = _hosts(_tls_node())

    assert api_host == "worker-1.simplyblock-storage-node-api.storage.svc.cluster.local:50001"
    assert rpc_host == "worker-1.simplyblock-spdk-proxy.storage.svc.cluster.local"


def test_a_node_is_reached_through_its_recorded_services_and_label_suffix():
    api_host, rpc_host = _hosts(_tls_node(**NAMES))

    assert api_host == "worker-1-abc.sb-prod-node-api.storage.svc.cluster.local:50001"
    assert rpc_host == "worker-1-abc.sb-prod-spdk-proxy.storage.svc.cluster.local"


def test_starting_spdk_sends_the_spdk_proxy_secret():
    client = SNodeClient("10.0.0.1:50001")
    with patch.object(client, "_request", return_value=(True, None)) as request:
        client.spdk_process_start(
            "0-1", 1024, cluster_ip="10.0.0.2", server_ip="10.0.0.1", rpc_port=8080,
            rpc_username="u", rpc_password=None, cluster_id="c1",
            spdk_proxy_tls_secret="sb-prod-spdk-proxy-tls")

    assert request.call_args.args[2]["spdk_proxy_tls_secret"] == "sb-prod-spdk-proxy-tls"


def test_a_node_agent_asked_without_a_secret_mounts_the_constant_one():
    params = SPDKParams(
        server_ip="10.0.0.1", rpc_port=8080, rpc_username="u", rpc_password="p",
        l_cores="0-1", namespace=None, cluster_ip="10.0.0.2", cluster_mode="kubernetes",
        cluster_id="c1")

    assert params.spdk_proxy_tls_secret == "simplyblock-spdk-proxy-tls"


def _render(template_name: str, **values) -> list[dict]:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), trim_blocks=True, lstrip_blocks=True)
    rendered = env.get_template(template_name).render(
        HOSTNAME="worker-1", NAMESPACE="storage", CLUSTER_ID="c1", RPC_PORT=8080,
        TLS_SERVE=True, TLS_CONNECT="authenticated", **values)
    return [doc for doc in yaml.safe_load_all(rendered) if doc]


def _tls_secret_names(doc: dict) -> set[str]:
    spec = doc["spec"]
    volumes = spec.get("volumes") or spec["template"]["spec"]["volumes"]
    (tls,) = [v for v in volumes if v["name"] == "tls"]
    if "secret" in tls:
        return {tls["secret"]["secretName"]}
    return {s["secret"]["name"] for s in tls["projected"]["sources"] if "secret" in s}


def test_the_spdk_pod_mounts_the_recorded_secret():
    for provider in ("cert-manager", "openshift"):
        (pod,) = [d for d in _render("storage_deploy_spdk.yaml.j2", TLS_PROVIDER=provider,
                                     SPDK_PROXY_TLS_SECRET="sb-prod-spdk-proxy-tls")
                  if d["kind"] == "Pod"]
        assert _tls_secret_names(pod) == {"sb-prod-spdk-proxy-tls"}, provider


def test_the_init_job_mounts_the_recorded_secret():
    (job,) = _render("storage_init_job.yaml.j2", SPDK_PROXY_TLS_SECRET="sb-prod-spdk-proxy-tls")

    assert _tls_secret_names(job) == {"sb-prod-spdk-proxy-tls"}


def test_add_cluster_hands_the_workload_names_to_the_create():
    """add_cluster restates _add_cluster_impl's signature by hand, so a
    parameter missing from its forwarding is dropped without an error."""
    from simplyblock_core import cluster_ops

    with patch.object(cluster_ops, "_add_cluster_impl", return_value="c1") as impl:
        cluster_ops.add_cluster(
            512, 2097152, 0, 0, 0, 0, 1, 1, 4096, 4096, "ha", False, 256, 128, 4, False, False, "",
            **NAMES)

    for field, value in NAMES.items():
        assert impl.call_args.kwargs[field] == value


def test_a_secret_name_yaml_would_read_as_another_type_stays_a_string():
    """A Secret named true, null, or 123 is a valid Kubernetes name, and
    unquoted in the template YAML reads it as a bool, a null, or an int,
    which Kubernetes refuses as a Secret reference."""
    for name in ("true", "null", "123"):
        for provider in ("cert-manager", "openshift"):
            (pod,) = [d for d in _render("storage_deploy_spdk.yaml.j2", TLS_PROVIDER=provider,
                                         SPDK_PROXY_TLS_SECRET=name)
                      if d["kind"] == "Pod"]
            assert _tls_secret_names(pod) == {name}, (name, provider)
        (job,) = _render("storage_init_job.yaml.j2", SPDK_PROXY_TLS_SECRET=name)
        assert _tls_secret_names(job) == {name}, name


def test_a_long_node_label_with_a_suffix_stays_one_dns_label():
    """A label of up to 63 characters plus a member's suffix would exceed what
    one DNS label carries. It is cut and carries an eight-character digest of
    the whole label, the rule atlas-lib's WorkloadNames.Hostname applies on the
    operator's side, so both build the same hostname."""
    import hashlib
    long = "w" * 60 + "-one"
    api_host, rpc_host = _hosts(_tls_node(hostname=f"{long}_8080", **NAMES))

    label = "w" * 50 + "-" + hashlib.sha256(long.encode()).hexdigest()[:8] + "-abc"
    assert len(label) == 63
    assert api_host == f"{label}.sb-prod-node-api.storage.svc.cluster.local:50001"
    assert rpc_host == f"{label}.sb-prod-spdk-proxy.storage.svc.cluster.local"
