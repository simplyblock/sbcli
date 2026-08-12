# coding=utf-8
"""Per-edge-cluster kubernetes access (spec §2, §9).

The CP reaches each edge site's kube-apiserver with credentials stored on the
Cluster record (k8s_api_url / k8s_token / k8s_ca_cert / k8s_namespace). An
empty k8s_api_url means "the CP's own cluster" — in-cluster config with
kubeconfig fallback (tests, single-site deployments).
"""
import logging
import tempfile

import jinja2
import yaml
from kubernetes import client as k8s_client

from simplyblock_core import utils as core_utils
from simplyblock_edge import constants as edge_constants
from simplyblock_edge.stack import _short

logger = logging.getLogger(__name__)

_ca_files: dict = {}  # cluster uuid -> temp CA bundle path (content-addressed refresh)


class EdgeK8sError(Exception):
    pass


def _ca_file_for(cluster) -> str:
    cached = _ca_files.get(cluster.uuid)
    if cached and cached[0] == cluster.k8s_ca_cert:
        return cached[1]
    with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as fh:
        fh.write(cluster.k8s_ca_cert)
        path = fh.name
    _ca_files[cluster.uuid] = (cluster.k8s_ca_cert, path)
    return path


def api_client(cluster) -> k8s_client.ApiClient:
    """kubernetes ApiClient for one edge cluster."""
    if not cluster.k8s_api_url:
        core_utils.load_kube_config_with_fallback()
        return k8s_client.ApiClient()

    configuration = k8s_client.Configuration()
    configuration.host = cluster.k8s_api_url
    configuration.api_key = {"authorization": cluster.k8s_token.get_secret_value()}
    configuration.api_key_prefix = {"authorization": "Bearer"}
    if cluster.k8s_ca_cert:
        configuration.ssl_ca_cert = _ca_file_for(cluster)
    else:
        configuration.verify_ssl = False
    return k8s_client.ApiClient(configuration)


def core_api(cluster) -> k8s_client.CoreV1Api:
    return k8s_client.CoreV1Api(api_client(cluster))


def pod_name(node) -> str:
    return f"{edge_constants.EDGE_POD_PREFIX}{_short(node.uuid)}"


def node_ready(cluster, node, timeout=edge_constants.EDGE_K8S_PROBE_TIMEOUT_SEC) -> bool:
    """True if the worker node object exists and reports Ready. Raises
    EdgeK8sError when the kube-apiserver itself is unreachable (the caller
    maps that to UNREACHABLE, not OFFLINE)."""
    try:
        obj = core_api(cluster).read_node(node.hostname, _request_timeout=timeout)
    except k8s_client.ApiException as e:
        if e.status == 404:
            return False
        raise EdgeK8sError(f"read_node {node.hostname}: {e.status}") from e
    except Exception as e:
        raise EdgeK8sError(f"kube-apiserver unreachable: {e}") from e
    for condition in (obj.status.conditions or []):
        if condition.type == "Ready":
            return condition.status == "True"
    return False


def pod_running(cluster, node, timeout=edge_constants.EDGE_K8S_PROBE_TIMEOUT_SEC) -> bool:
    """True if the node's SPDK pod exists and its phase is Running. Raises
    EdgeK8sError on apiserver unreachability."""
    try:
        pod = core_api(cluster).read_namespaced_pod(
            pod_name(node), cluster.k8s_namespace, _request_timeout=timeout)
    except k8s_client.ApiException as e:
        if e.status == 404:
            return False
        raise EdgeK8sError(f"read_namespaced_pod: {e.status}") from e
    except Exception as e:
        raise EdgeK8sError(f"kube-apiserver unreachable: {e}") from e
    return pod.status.phase == "Running"


def render_spdk_pod(cluster, node, spdk_image, proxy_image) -> dict:
    from simplyblock_edge.stack import CpuLayout, plan_cpu_layout
    layout = plan_cpu_layout(node.spdk_cpus)
    env = jinja2.Environment(loader=jinja2.PackageLoader('simplyblock_edge', 'templates'),
                             autoescape=False)
    manifest = env.get_template('edge_spdk_pod.yaml.j2').render(
        pod_name=pod_name(node),
        namespace=cluster.k8s_namespace,
        hostname=node.hostname,
        spdk_image=spdk_image,
        proxy_image=proxy_image,
        rpc_port=node.rpc_port,
        rpc_username=node.rpc_username,
        rpc_password=node.rpc_password.get_secret_value(),
        cpu=node.spdk_cpus,
        reactor_mask=CpuLayout.hex(layout.reactor_mask),
        app_mask=CpuLayout.hex(layout.app_mask),
        lvs_mask=CpuLayout.hex(layout.lvs_mask),
        nvmf_mask=CpuLayout.hex(layout.nvmf_mask),
        hugepages_mib=edge_constants.EDGE_POD_HUGEPAGES_MIB,
    )
    return yaml.safe_load(manifest)


def _job_events(cluster, job_name) -> str:
    """Last warning events involving the job, appended to failure messages so
    the status_reason on the node record explains WHY (e.g. a forbidden pod),
    not just that a wait expired."""
    try:
        events = core_api(cluster).list_namespaced_event(
            cluster.k8s_namespace,
            field_selector=f"involvedObject.name={job_name}")
        warnings = [e.message for e in events.items if e.type == 'Warning']
        return f" ({'; '.join(warnings[-2:])})" if warnings else ""
    except Exception:
        return ""


def _ensure_service_account(cluster, name):
    """Create a bare ServiceAccount in the edge namespace if missing.

    The shared cpu-topology job template pins serviceAccountName to the SA
    the HELM CHART creates on central clusters — nothing creates it on a
    bare edge cluster, so pod creation is forbidden and the job can never
    start (first live run 2026-08-13: 'error looking up service account
    simplyblock/simplyblock-storage-node-sa'). The job runs a host-prep
    script and makes no k8s API calls, so an empty SA (no RBAC) is enough.
    """
    core = core_api(cluster)
    try:
        core.read_namespaced_service_account(name, cluster.k8s_namespace)
    except k8s_client.ApiException as e:
        if e.status != 404:
            raise EdgeK8sError(f"read service account {name}: {e.status}") from e
        try:
            core.create_namespaced_service_account(
                cluster.k8s_namespace, {'metadata': {'name': name}})
        except k8s_client.ApiException as e2:
            if e2.status != 409:
                raise EdgeK8sError(f"create service account {name}: {e2.status}") from e2


def deploy_cpu_topology_job(cluster, node,
                            reserved_system_cpus=None,
                            timeout=600, interval=5):
    """Run the SAME node-preparation CPU-topology Job the central clusters
    use (simplyblock_web/templates/storage_cpu_topology.yaml.j2) against the
    edge node, through the edge cluster's k8s API: create, wait for
    completion, delete."""
    import time as _time
    _ensure_service_account(cluster, 'simplyblock-storage-node-sa')
    env = jinja2.Environment(loader=jinja2.PackageLoader('simplyblock_web', 'templates'),
                             autoescape=False)
    job_name = f"edge-cpu-topology-{_short(node.uuid)}"
    body = yaml.safe_load(env.get_template('storage_cpu_topology.yaml.j2').render(
        CORE_JOBNAME=job_name,
        HOSTNAME=node.hostname,
        NAMESPACE=cluster.k8s_namespace,
        RESERVED_SYSTEM_CPUS=(reserved_system_cpus
                              or edge_constants.EDGE_RESERVED_SYSTEM_CPUS),
    ))
    batch = k8s_client.BatchV1Api(api_client(cluster))
    try:
        batch.create_namespaced_job(cluster.k8s_namespace, body)
    except k8s_client.ApiException as e:
        if e.status != 409:
            raise EdgeK8sError(f"create cpu-topology job: {e.status}") from e
    deadline = _time.monotonic() + timeout
    try:
        while True:
            job = batch.read_namespaced_job(job_name, cluster.k8s_namespace)
            if job.status.succeeded:
                return
            if job.status.failed:
                raise EdgeK8sError(f"cpu-topology job failed on {node.hostname}"
                                   f"{_job_events(cluster, job_name)}")
            if _time.monotonic() >= deadline:
                raise EdgeK8sError(f"cpu-topology job timed out on {node.hostname}"
                                   f"{_job_events(cluster, job_name)}")
            _time.sleep(interval)
    finally:
        try:
            batch.delete_namespaced_job(job_name, cluster.k8s_namespace,
                                        propagation_policy='Foreground')
        except k8s_client.ApiException:
            pass


def deploy_spdk_pod(cluster, node, spdk_image, proxy_image):
    body = render_spdk_pod(cluster, node, spdk_image, proxy_image)
    try:
        return core_api(cluster).create_namespaced_pod(cluster.k8s_namespace, body)
    except k8s_client.ApiException as e:
        if e.status == 409:  # already exists — idempotent redeploy
            logger.info(f"SPDK pod {pod_name(node)} already exists")
            return None
        raise EdgeK8sError(f"create pod {pod_name(node)}: {e.status}") from e


def delete_spdk_pod(cluster, node):
    try:
        core_api(cluster).delete_namespaced_pod(pod_name(node), cluster.k8s_namespace)
    except k8s_client.ApiException as e:
        if e.status != 404:
            raise EdgeK8sError(f"delete pod {pod_name(node)}: {e.status}") from e
