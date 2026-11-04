import json
import logging
import os
import time
import traceback
from pathlib import Path
from typing import Annotated

import yaml
from flask_openapi3 import APIBlueprint
from jinja2 import Environment, PackageLoader
from kubernetes.client import ApiException, V1DeleteOptions
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core import utils as core_utils
from simplyblock_core.settings import Settings
from simplyblock_core.storage_network import NetworkAttachment
from simplyblock_core.utils import host_network
from simplyblock_core.utils import pci as pci_utils
from simplyblock_core.utils import shell as shell_utils
from simplyblock_web import node_utils, node_utils_k8s, utils
from simplyblock_web.api.internal.storage_node import network_attachments, pod_service
from simplyblock_web.node_utils_k8s import namespace_id_file

from .._node_info import get_static_node_info

logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)
api = APIBlueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def set_namespace(namespace):
    if not os.path.exists(namespace_id_file):
        try:
            os.makedirs(os.path.dirname(namespace_id_file), exist_ok=True)
        except Exception:
            return False
    with open(namespace_id_file, "w+") as f:
        f.write(namespace)
    return True


@api.get('/scan_devices', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'object',
        'required': ['nvme_devices', 'nvme_pcie_list', 'spdk_devices', 'spdk_pcie_list'],
        'properties': {
            'nvme_devices': {'type': 'array', 'items': {'type': 'string'}},
            'nvme_pcie_list': {'type': 'array', 'items': {'type': 'string'}},
            'spdk_devices': {'type': 'array', 'items': {'type': 'string'}},
            'spdk_pcie_list': {'type': 'array', 'items': {'type': 'string'}},
        },
    })}}},
})
def scan_devices():
    out = {
        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),
        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),
    }
    return utils.get_response(out)


def get_cluster_id():
    out, _, _ = shell_utils.run_command(f"cat {cluster_id_file}")
    return out


def set_cluster_id(cluster_id):
    out, _, _ = shell_utils.run_command(f"echo {cluster_id} > {cluster_id_file}")
    return out


def delete_cluster_id():
    out, _, _ = shell_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_nodes_config():
    file_path = constants.NODES_CONFIG_FILE
    try:
        # Open and read the JSON file
        with open(file_path, "r") as file:
            nodes_config = json.load(file)

        # Open and read the read_only JSON file
        with open(f"{file_path}_read_only", "r") as file:
            read_only_nodes_config = json.load(file)
        if nodes_config != read_only_nodes_config:
            logger.error("The nodes config has been changed, "
                         "Please run sbcli sn configure-upgrade before adding the storage node")
            return {}
        for i in range(len(nodes_config.get("nodes"))):
            if not core_utils.validate_node_config(nodes_config.get("nodes")[i]):
                return {}
        return nodes_config

    except FileNotFoundError:
        logger.error(f"The file '{file_path}' does not exist.")
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON: {e}")
        return {}


def reported_hostname(command_hostname: str) -> str:
    """The name the control plane knows this worker by.

    With the agent in the pod network, `hostname -s` answers the pod's name. The
    DaemonSet hands the worker's name over as HOSTNAME and the worker's address as
    HOST_IP, and the latter is what says the agent is off the host network."""
    if os.environ.get("HOST_IP") and os.environ.get("HOSTNAME"):
        return os.environ["HOSTNAME"].split(".")[0]
    return command_hostname


@api.get('/info', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'object',
        'additionalProperties': True,
    })}}},
})
def get_info():
    node_info = get_static_node_info()
    return utils.get_response({
        "cluster_id": get_cluster_id(),

        "hostname": reported_hostname(node_info["hostname"]),
        "system_id": node_info["system_id"],

        "cpu_count": node_info["cpu_info"]['count'],
        "cpu_hz": node_info["cpu_info"]['hz_advertised'][0] if 'hz_advertised' in node_info["cpu_info"] else 1,
        # Per-NUMA-node core ids, keyed by socket id as a string (JSON has no
        # int keys). Read-only topology -- add_node uses it to resize a
        # node's isolated-core set to the cluster's spdk_vcpu_count; nothing
        # here is persisted.
        "cpu_topology": {str(k): v for k, v in core_utils.get_numa_cores().items()},

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),

        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),

        "network_interface": core_utils.get_nics_data(),
        # Set by the DaemonSet: the agent's own interfaces may not be the host's.
        "host_ip": os.environ.get("HOST_IP", ""),

        "cloud_instance": node_info["cloud_info"],
        "nodes_config": get_nodes_config(),
    })


@api.get('/blockdevices', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'array',
        'items': {'type': 'object', 'additionalProperties': True},
    })}}},
})
def get_blockdevices():
    """Whole-disk inventory for the lblk cluster mode (eligibility fields,
    serial/WWN identity, by-id path, NUMA)."""
    return utils.get_response(node_utils.get_block_devices_info())


class _WipeBlockDeviceParams(BaseModel):
    device_name: str


@api.post('/wipe_block_device', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def wipe_block_device(body: _WipeBlockDeviceParams):
    """--force-format for lblk add-node: wipe partition/FS signatures from a
    whole disk. Refuses busy devices (mounts/holders/root disk)."""
    ok, reason = node_utils.wipe_block_device_signatures(body.device_name)
    if not ok:
        return utils.get_response(None, reason)
    return utils.get_response(True)


class _RescanBlockDeviceParams(BaseModel):
    device_path: str


@api.post('/rescan_block_device', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'object',
        'required': ['device', 'size', 'rescanned'],
        'properties': {
            'device': {'type': 'string'},
            'size': {'type': 'integer'},
            'rescanned': {'type': 'array', 'items': {'type': 'string'}},
        },
    })}}},
})
def rescan_block_device(body: _RescanBlockDeviceParams):
    """Re-read the size of a whole-disk block device (a grown LUN) and
    return the size in bytes the kernel reports afterwards."""
    try:
        return utils.get_response(node_utils.rescan_block_device(body.device_path))
    except (ValueError, OSError) as e:
        return utils.get_response(None, str(e))


class _DeviceSmartQuery(BaseModel):
    device_path: str


@api.get('/device_smart', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'object',
        'required': ['supported', 'source', 'device_path', 'data', 'reason'],
        'properties': {
            'supported': {'type': 'boolean'},
            'source': {'type': 'string', 'enum': ['nvme-cli', 'smartctl', '']},
            'device_path': {'type': 'string'},
            'data': {'type': 'object', 'nullable': True, 'additionalProperties': True},
            'reason': {'type': 'string'},
        },
    })}}},
})
def device_smart(query: _DeviceSmartQuery):
    """Raw health log of a kernel block device (lblk / AIO devices), read
    with nvme-cli or smartctl; `supported: false` when it has none."""
    try:
        return utils.get_response(node_utils.read_device_smart(query.device_path))
    except ValueError as e:
        return utils.get_response(None, str(e))


class _GPTPartitionsParams(BaseModel):
    nbd_device: str = Field('/dev/nbd0')
    jm_percent: int = Field(3, ge=0, le=100)
    num_partitions: int = Field(0, ge=0)


@api.post('/make_gpt_partitions', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def make_gpt_partitions_for_nbd(body: _GPTPartitionsParams):
    cmd_list = [
        f"parted -fs {body.nbd_device} mklabel gpt",
        f"parted -f {body.nbd_device} mkpart journal \"0%\" \"{body.jm_percent}%\""
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}",
    ]
    perc_per_partition = int((100 - body.jm_percent) / body.num_partitions)
    for i in range(body.num_partitions):
        st = body.jm_percent + (i * perc_per_partition)
        en = st + perc_per_partition
        cmd_list.append(f"parted -f {body.nbd_device} mkpart part{(i+1)} \"{st}%\" \"{en}%\"")
        sg_cmd_list.append(f"sgdisk -t {(i+2)}:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}")

    for cmd in cmd_list+sg_cmd_list:
        logger.debug(cmd)
        out, err, ret_code = shell_utils.run_command(cmd)
        logger.debug(out)
        logger.debug(ret_code)
        if ret_code != 0:
            logger.error(err)
            return utils.get_response(False, f"Error running cmd: {cmd}, returncode: {ret_code}, output: {out}, err: {err}")
        time.sleep(1)

    return utils.get_response(True)


@api.post('/delete_dev_gpt_partitions')
def delete_gpt_partitions_for_dev(body: utils.DeviceParams):
    bind_device_to_nvme(body)
    device_name = pci_utils.nvme_device_name(body.device_pci)
    cmd = f"parted -fs /dev/{device_name} mklabel gpt"
    out, err, ret_code = shell_utils.run_command(cmd)
    logger.info(f"out: {out}, err: {err}, ret_code: {ret_code}")
    return utils.get_response(ret_code == 0, error=err)


# How long a stale Job of a previous spdk_process_start attempt may take to
# disappear after its deletion was requested.
_STALE_JOB_GONE_TIMEOUT_S = 120


def _create_job_replacing_stale(batch_v1, namespace: str, job_yaml: dict):
    """Create a node-preparation Job, replacing one of the same name that a
    previous attempt left behind.

    A Job of a failed attempt is not deleted on the failure path, and the
    node-add task retries spdk_process_start: without this, every retry fails
    at create with 409 AlreadyExists and the node is never added. A Foreground
    delete is asynchronous, so the Job is waited for until it is gone before
    the new one is created.
    """
    name = job_yaml['metadata']['name']
    try:
        batch_v1.delete_namespaced_job(
            name=name,
            namespace=namespace,
            body=V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=0)
        )
        logger.info(f"Deleted stale job '{name}' left from a previous attempt")
    except ApiException as e:
        if e.status != 404:
            raise
    else:
        deadline = time.monotonic() + _STALE_JOB_GONE_TIMEOUT_S
        while True:
            try:
                batch_v1.read_namespaced_job(name=name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    break
                raise
            if time.monotonic() > deadline:
                raise RuntimeError(f"stale job '{name}' was not gone {_STALE_JOB_GONE_TIMEOUT_S}s after its deletion")
            time.sleep(1)
    return batch_v1.create_namespaced_job(namespace=namespace, body=job_yaml)


class SPDKParams(BaseModel):
    server_ip: str = Field(pattern=utils.IP_PATTERN)
    rpc_port: int = Field(ge=1, lt=65536)
    rpc_username: str
    rpc_password: str
    ssd_pcie: list[str] = Field([])
    l_cores: str
    namespace: str | None
    total_mem: int | str = Field('')
    spdk_mem: int = Field(core_utils.parse_size('64GiB'))
    system_mem: int = Field(core_utils.parse_size('4GiB'))
    fdb_connection: str = Field('')
    spdk_image: str = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    spdk_proxy_image: str | None = Field(constants.SIMPLY_BLOCK_DOCKER_IMAGE)
    cluster_ip: str = Field(pattern=utils.IP_PATTERN)
    cluster_mode: str
    socket: int | None = Field(None, ge=0)
    cluster_id: str
    mcp_max_unavailable: int | None = Field(None)
    # The Secret the SPDK proxy serves and connects with: the cluster's
    # recorded one, or the constant name from a control plane that sends none.
    spdk_proxy_tls_secret: core_utils.K8S_DNS_SUBDOMAIN = constants.K8S_SPDK_PROXY_TLS_SECRET
    # The networks the SPDK pod attaches to. With any, the pod leaves the host network.
    networks: Annotated[list[NetworkAttachment], Field(default_factory=list)]
    # The pod serves on the pod network itself, fronted by a Service, with no attachment.
    pod_network: bool = False
    # The ports the Service forwards at start: the node's known ports, which a restart
    # has and a first add lacks. The RPC port is always forwarded.
    published_ports: Annotated[list[Annotated[int, Field(ge=1, lt=65536)]], Field(default_factory=list)]


class ServicePublishParams(BaseModel):
    rpc_port: Annotated[int, Field(ge=1, lt=65536)]
    cluster_id: str
    ports: list[Annotated[int, Field(ge=1, lt=65536)]]


@api.post('/spdk_service_publish', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def spdk_service_publish(body: ServicePublishParams):
    """Set the ports a pod-network node's Service forwards; answers its cluster address."""
    try:
        address = pod_service.ensure(core_utils.get_k8s_core_client(), node_utils_k8s.get_namespace(),
                                     body.rpc_port, body.cluster_id, body.ports)
    except ApiException as e:
        return utils.get_response(False, f"Service for port {body.rpc_port} not published: {e.reason}")
    return utils.get_response(address)


@api.get('/spdk_service_delete', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_service_delete(query: utils.RPCPortParams):
    """Remove a pod-network node's Service. Only a node's removal does this: the
    address is lost with it, and a restarted pod keeps it only while it exists."""
    if not query.cluster_id:
        return utils.get_response(False, "param required: cluster_id")
    try:
        pod_service.delete(core_utils.get_k8s_core_client(), node_utils_k8s.get_namespace(),
                           query.rpc_port, query.cluster_id)
    except ApiException as e:
        return utils.get_response(False, f"Service for port {query.rpc_port} not deleted: {e.reason}")
    return utils.get_response(True)


@api.post('/spdk_process_start', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_start(body: SPDKParams):
    settings = Settings()
    ssd_pcie_params = " ".join(" -A " + addr for addr in body.ssd_pcie) if body.ssd_pcie else "none"
    ssd_pcie_list = " ".join(body.ssd_pcie)

    namespace = node_utils_k8s.get_namespace()
    if body.namespace is not None:
        namespace = body.namespace
        set_namespace(namespace)

    total_mem_mib = core_utils.convert_size(core_utils.parse_size(body.total_mem), 'MB') if body.total_mem else ""

    first_six_cluster_id = core_utils.first_six_chars(body.cluster_id)
    if _is_pod_up(body.rpc_port, first_six_cluster_id) or _is_pod_present(body.rpc_port, first_six_cluster_id):
        logger.info("SPDK pod found, removing...")
        query = utils.RPCPortParams(rpc_port=body.rpc_port, cluster_id=body.cluster_id)
        spdk_process_kill(query)

    node_prepration_job_name = "snode-spdk-job-"
    node_prepration_ubuntu_name = "snode-spdk-ubuntu-extra-"

    node_name = os.environ.get("HOSTNAME", "")
    core_isolate = os.environ.get("CORE_ISOLATION", False)
    if isinstance(core_isolate, str):
       core_isolate = core_isolate.strip().lower() in ("true")

    ubuntu_host = os.environ.get("UBUNTU_HOST", False)
    if isinstance(ubuntu_host, str):
       ubuntu_host = ubuntu_host.strip().lower() in ("true")

    openshift = os.environ.get("OPENSHIFT_CLUSTER", False)
    if isinstance(openshift, str):
       openshift = openshift.strip().lower() in ("true")

    # limit the job name length to 63 characters
    k8s_job_name_length = len(node_prepration_job_name+node_name)
    ubuntu_name_length = len(node_prepration_ubuntu_name+node_name)
    if k8s_job_name_length > 63:
        node_prepration_job_name += node_name[k8s_job_name_length-63:]
    else:
        node_prepration_job_name += node_name

    if ubuntu_name_length > 63:
        node_prepration_ubuntu_name += node_name[ubuntu_name_length-63:]
    else:
         node_prepration_ubuntu_name += node_name
    cpu_topology_enabled = os.environ.get("CPU_TOPOLOGY_ENABLED", False)
    if isinstance(cpu_topology_enabled, str):
       cpu_topology_enabled = cpu_topology_enabled.strip().lower() in ("true")
    skip_kubelet_configuration = os.environ.get("SKIP_KUBELET_CONFIGURATION", False)
    if isinstance(skip_kubelet_configuration, str):
       skip_kubelet_configuration = skip_kubelet_configuration.strip().lower() in ("true")
    reserved_system_cpus = os.environ.get("RESERVED_SYSTEM_CPUS", "0,1")
    # Initial storage-MCP maxUnavailable when it's first created for CPU-topology
    # apply = the configured parallel-add count, so the first-time,
    # pre-activation reboots roll in one wave instead of a one-at-a-time queue.
    # Source order: the value the control plane read from the StorageCluster CR
    # (spec.storageNodes.maxParallelNodeAdds) and passed in; then the
    # MAX_PARALLEL_NODE_ADDS
    # env (if an operator injects it); then constants.NODE_ADD_MAX_PARALLEL.
    # cluster_activate later narrows the pool to the cluster's fault tolerance.
    mcp_max_unavailable = (
        body.mcp_max_unavailable
        or os.environ.get("MAX_PARALLEL_NODE_ADDS")
        or str(constants.NODE_ADD_MAX_PARALLEL)
    )
    openshift_mcp = os.environ.get("OPENSHIFT_MCP")

    node_prepration_core_name = "snode-spdk-core-isolate-"
    if cpu_topology_enabled:
        node_prepration_core_name = "snode-spdk-enable-cpu-topology-"
    core_name_length = len(node_prepration_core_name+node_name)
    if core_name_length > 63:
        node_prepration_core_name += node_name[core_name_length-63:]
    else:
         node_prepration_core_name += node_name
    logger.debug(f"deploying k8s job to prepare worker: {node_name}")

    try:
        env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
        values = {
            'SPDK_IMAGE': body.spdk_image,
            "L_CORES": body.l_cores,
            "CORES": core_utils.get_total_cpu_cores(body.l_cores),
            'SPDK_MEM': core_utils.convert_size(body.spdk_mem, 'MiB'),
            'MEM_MEGA': (core_utils.convert_size(body.spdk_mem, 'MiB', round_up=True) // 2) * 2 + 512,
            'MEM2_MEGA': (core_utils.convert_size(body.system_mem, 'MiB', round_up=True) // 2) * 2,
            'SERVER_IP': network_attachments.server_ip(body.networks, body.pod_network) or body.server_ip,
            'RPC_PORT': body.rpc_port,
            'RPC_USERNAME': body.rpc_username,
            'RPC_PASSWORD': body.rpc_password,
            'HOSTNAME': node_name,
            'JOBNAME': node_prepration_job_name,
            'UBUNTU_JOBNAME': node_prepration_ubuntu_name,
            'CORE_JOBNAME': node_prepration_core_name,
            'NAMESPACE': namespace,
            'FDB_CONNECTION': body.fdb_connection,
            'SIMPLYBLOCK_DOCKER_IMAGE': body.spdk_proxy_image,
            'GRAYLOG_SERVER_IP': body.cluster_ip,
            'MODE': body.cluster_mode,
            'CLUSTER_ID': first_six_cluster_id,
            'SSD_PCIE': ssd_pcie_params,
            'PCI_ALLOWED': ssd_pcie_list,
            'TOTAL_HP': total_mem_mib,
            'NSOCKET': body.socket,
            'CPU_TOPOLOGY_ENABLED': cpu_topology_enabled,
            'RESERVED_SYSTEM_CPUS': reserved_system_cpus,
            'MCP_MAX_UNAVAILABLE': mcp_max_unavailable,
            'OPENSHIFT_MCP': openshift_mcp,
            'TLS_SERVE': settings.tls_serve,
            'TLS_CONNECT': settings.tls_connect,
            'TLS_CLIENT_AUTH': settings.model_dump()["tls_client_auth"],
            'TLS_PROVIDER': settings.tls_provider,
            'SPDK_PROXY_TLS_SECRET': body.spdk_proxy_tls_secret,
            # The operator sets POD_IP when the agent is off the host network, whatever the
            # SPDK pod's network; the preparation job (host network) then calls it there.
            'AGENT_IP': os.environ.get("POD_IP", ""),
            'NETWORKS': network_attachments.annotation(body.networks),
            'POD_NETWORK': body.pod_network,
            'FULL_CLUSTER_ID': body.cluster_id,
            'DEVICE_RESOURCES': network_attachments.device_resources(body.networks),
        }

        if ubuntu_host:
            ubuntu_template = env.get_template('ubuntu_kernel_extra.yaml.j2')
            ubuntu_yaml = yaml.safe_load(ubuntu_template.render(values))
            batch_v1 = core_utils.get_k8s_batch_client()
            ubuntu_resp = _create_job_replacing_stale(batch_v1, namespace, ubuntu_yaml)
            msg = f"Job created: '{ubuntu_resp.metadata.name}' in namespace '{namespace}"
            logger.info(msg)

            node_utils_k8s.wait_for_job_completion(ubuntu_resp.metadata.name, namespace)
            logger.info(f"Job '{ubuntu_resp.metadata.name}' completed successfully")

            batch_v1.delete_namespaced_job(
                name=ubuntu_resp.metadata.name,
                namespace=namespace,
                body=V1DeleteOptions(
                    propagation_policy='Foreground',
                    grace_period_seconds=0
                )
            )
            logger.info(f"Job deleted: '{ubuntu_resp.metadata.name}' in namespace '{namespace}")


        job_template = env.get_template('storage_init_job.yaml.j2')
        job_yaml = yaml.safe_load(job_template.render(values))
        batch_v1 = core_utils.get_k8s_batch_client()
        job_resp = _create_job_replacing_stale(batch_v1, namespace, job_yaml)
        msg = f"Job created: '{job_resp.metadata.name}' in namespace '{namespace}"
        logger.info(msg)

        node_utils_k8s.wait_for_job_completion(job_resp.metadata.name, namespace)
        logger.info(f"Job '{job_resp.metadata.name}' completed successfully")

        try:
            batch_v1.delete_namespaced_job(
                name=job_resp.metadata.name,
                namespace=namespace,
                body=V1DeleteOptions(
                    propagation_policy='Foreground',
                    grace_period_seconds=0
                )
            )
            logger.info(f"Job deleted: '{job_resp.metadata.name}' in namespace '{namespace}")
        except ApiException as e:
            if e.status != 404:
                raise
            logger.info(f"Job '{job_resp.metadata.name}' already gone, skipping delete")
        if (cpu_topology_enabled and not skip_kubelet_configuration) or (core_isolate and not cpu_topology_enabled):
            if cpu_topology_enabled and not skip_kubelet_configuration:
                if not openshift:
                    template_name = 'storage_cpu_topology.yaml.j2'
                else:
                    template_name = 'oc_storage_cpu_topology.yaml.j2'
            elif core_isolate:
                if not openshift:
                    template_name = 'storage_core_isolation.yaml.j2'
                else:
                    template_name = 'oc_storage_core_isolation.yaml.j2'
            batch_v1 = core_utils.get_k8s_batch_client()
            try:
                batch_v1.read_namespaced_job(
                    name=node_prepration_core_name,
                    namespace=namespace
                )
                logger.info(f"Existing Job '{node_prepration_core_name}' found — deleting it first...")

                batch_v1.delete_namespaced_job(
                    name=node_prepration_core_name,
                    namespace=namespace,
                    body=V1DeleteOptions(
                        propagation_policy='Foreground',
                        grace_period_seconds=0
                    )
                )

                node_utils_k8s.wait_for_job_deletion(node_prepration_core_name, namespace)

                logger.info(f"Old Job '{node_prepration_core_name}' fully deleted.")

            except ApiException as e:
                if e.status == 404:
                    logger.info(f"No pre-existing Job '{node_prepration_core_name}' found. Proceeding.")
                else:
                    raise
            core_template = env.get_template(template_name)
            core_yaml = yaml.safe_load(core_template.render(values))
            batch_v1 = core_utils.get_k8s_batch_client()
            core_resp = batch_v1.create_namespaced_job(namespace=namespace, body=core_yaml)
            msg = f"Job created: '{core_resp.metadata.name}' in namespace '{namespace}"
            logger.info(msg)
            # The OpenShift CPU-topology Job's first-time path waits up to 5 min
            # for the pool to render, sleeps 15 min for the MCO reboot, and after
            # the reboot waits up to 6 min for the NUMA plugin, plus stale-pool
            # retries and pod scheduling: ~35 min at 3 s per poll, above the
            # default 24 min, which would time out a Job about to succeed and
            # delete it.
            node_utils_k8s.wait_for_job_completion(core_resp.metadata.name, namespace, timeout=700)
            logger.info(f"Job '{core_resp.metadata.name}' completed successfully")
            batch_v1.delete_namespaced_job(
                name=core_resp.metadata.name,
                namespace=namespace,
                body=V1DeleteOptions(
                    propagation_policy='Foreground',
                    grace_period_seconds=0
                )
            )
            logger.info(f"Job deleted: '{core_resp.metadata.name}' in namespace '{namespace}")

        k8s_core_v1 = core_utils.get_k8s_core_client()
        if cpu_topology_enabled and not skip_kubelet_configuration and openshift:
            # Gate SPDK on the CPU-topology reboot ACTUALLY landing on this node.
            # The topology Job completing is not proof of that: MCO cordons/reboots
            # asynchronously (queued by the MCP maxUnavailable), so when the Job
            # returns the node often isn't even cordoned yet. Starting SPDK then
            # brings it up before cpuManagerPolicy=static is in effect (unpinned),
            # only to be killed by the reboot moments later.
            #
            # Converge check compares the rendered-config CONTENT HASH
            # (rendered-<pool>-<hash>), not the full name: a node still under an
            # older pool (e.g. a previous cluster id) with byte-identical config
            # has a different name but the same hash and must pass immediately —
            # no false wait, no needless pool migration/reboot.
            mcp_name = f"storage-{first_six_cluster_id}"

            def _cfg_hash(name):
                return name.rsplit("-", 1)[-1] if name else None

            co = core_utils.get_k8s_custom_objects_client()
            for attempt in range(240):  # ~40 min: reboots queue under MCP maxUnavailable
                try:
                    mcp = co.get_cluster_custom_object(
                        group="machineconfiguration.openshift.io", version="v1",
                        plural="machineconfigpools", name=mcp_name)
                except ApiException as e:
                    if e.status == 404:
                        # No MCP to gate on (should not happen once the Job ensures
                        # membership). Degrade to the old behaviour rather than hang:
                        # proceed as soon as the node is uncordoned.
                        logger.warning(f"MCP '{mcp_name}' not found; cannot gate on "
                                       f"topology rollout — proceeding once uncordoned")
                        node_obj = k8s_core_v1.read_node(node_name)
                        if not node_obj.spec.unschedulable:
                            break
                        time.sleep(10)
                        continue
                    raise
                target = (mcp.get("status", {}).get("configuration", {}) or {}).get("name")
                node_obj = k8s_core_v1.read_node(node_name)
                ann = node_obj.metadata.annotations or {}
                cur = ann.get("machineconfiguration.openshift.io/currentConfig")
                state = ann.get("machineconfiguration.openshift.io/state")
                if (target and state == "Done" and not node_obj.spec.unschedulable
                        and _cfg_hash(cur) and _cfg_hash(cur) == _cfg_hash(target)):
                    logger.info(f"Node '{node_name}' carries MCP '{mcp_name}' config "
                                f"(hash {_cfg_hash(cur)}), reboot done — starting SPDK")
                    break
                logger.info(f"Waiting for '{node_name}' to converge on MCP '{mcp_name}' "
                            f"(cur={cur} target={target} state={state} "
                            f"cordoned={node_obj.spec.unschedulable})... {attempt + 1}/240")
                time.sleep(10)
            else:
                return utils.get_response(
                    False, f"Node '{node_name}' did not converge on MCP '{mcp_name}' config in time")
        else:
            # Non-topology / non-OpenShift: keep the original uncordon wait.
            for attempt in range(56):
                node_obj = k8s_core_v1.read_node(node_name)
                if not node_obj.spec.unschedulable:
                    break
                if attempt == 55:
                    return utils.get_response(False, f"Node '{node_name}' remained cordoned after 6 minutes")
                logger.info(f"Node '{node_name}' is cordoned, waiting for uncordon... attempt {attempt + 1}/36")
                time.sleep(10)

        service_ip = ""
        if body.pod_network:
            # Before the pod: the Service's address is what the pod reports, and it
            # has to exist for the control plane to reach the proxy at all.
            service_ip = pod_service.ensure(k8s_core_v1, namespace, body.rpc_port, body.cluster_id,
                                            body.published_ports)

        env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('storage_deploy_spdk.yaml.j2')
        docs = yaml.safe_load_all(template.render(values))
        for dep in docs:
            logger.debug(dep)
            pod_name = k8s_core_v1.create_namespaced_pod(body=dep, namespace=namespace).metadata.name
            msg = f"Pod created: '{pod_name}' in namespace '{namespace}'"
            logger.info(msg)
        if body.networks or body.pod_network:
            report = network_attachments.wait_for_report(
                lambda: k8s_core_v1.read_namespaced_pod(pod_name, namespace), body.networks)
            if service_ip:
                report["service_ip"] = service_ip
            return utils.get_response({"message": msg, **report})
    except Exception:
        return utils.get_response(False, f"Pod failed:\n{traceback.format_exc()}")

    return utils.get_response(msg)


@api.get('/spdk_process_kill', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_kill(query: utils.RPCPortParams):
    k8s_core_v1 = core_utils.get_k8s_core_client()
    namespace = node_utils_k8s.get_namespace()
    if not query.cluster_id:
        return utils.get_response(False, "param required: cluster_id")

    first_six_cluster_id = core_utils.first_six_chars(query.cluster_id)
    pod_name = f"snode-spdk-pod-{query.rpc_port}-{first_six_cluster_id}"
    try:
        k8s_core_v1.delete_namespaced_pod(pod_name, namespace)
    except ApiException as e:
        if e.status != 404:
            # A genuine delete failure (not "already gone") must be reported
            # to the caller — add_node()'s stale-record cleanup only drops
            # its DB record once this call confirms the pod is dead; treating
            # a swallowed failure as success orphans the pod (it keeps
            # holding the host's CPU/hugepages/memory, silently starving
            # every later add-node attempt on that host — worker-3,
            # 2026-07-28).
            logger.error(f"Failed to delete pod {pod_name}: {e.body}")
            return utils.get_response(False, f"Failed to delete pod {pod_name}: {e.body}")

    fluent_pod_name = f"simplyblock-fluentd-{query.rpc_port}-{first_six_cluster_id}"
    try:
        k8s_core_v1.read_namespaced_pod(fluent_pod_name, namespace)
        logger.info(f"Deleting fluent pod {fluent_pod_name}")
        k8s_core_v1.delete_namespaced_pod(fluent_pod_name, namespace)
    except ApiException as e:
        if e.status != 404:
            # Best-effort companion log pod; don't fail the whole call over it.
            logger.warning(f"Failed to delete fluent pod {fluent_pod_name}: {e.body}")

    max_retries = 10
    found = True
    for attempt in range(max_retries):
        resp = k8s_core_v1.list_namespaced_pod(namespace)
        found = any(pod.metadata.name.startswith(pod_name) for pod in resp.items)
        if not found:
            break
        logger.info("Container found, waiting...")
        if attempt < max_retries - 1:
            time.sleep(3)

    if found:
        logger.error(f"Pod {pod_name} still present {max_retries * 3}s after delete")
        return utils.get_response(False, f"Pod {pod_name} did not terminate in time")

    return utils.get_response(True)


@api.get('/spdk_process_cleanup', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_cleanup(query: utils.RPCPortParams):
    """Authoritative SPDK teardown for failure-cleanup paths. Pod deletion is
    already synchronous and verified (see spdk_process_kill's
    poll-until-gone), so this is an alias of spdk_process_kill."""
    return spdk_process_kill(query)


def _is_pod_up(rpc_port, cluster_id):
    k8s_core_v1 = core_utils.get_k8s_core_client()
    pod_name = f"snode-spdk-pod-{rpc_port}-{cluster_id}"
    container_name = "spdk-container"
    try:
        resp = k8s_core_v1.list_namespaced_pod(node_utils_k8s.get_namespace())
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                if pod.status.phase == "Running":
                    cs = next((c for c in pod.status.container_statuses if c.name == container_name),None)
                    if cs is None:
                        logger.error(f"Container '{container_name}' not found in pod '{pod_name}'")
                        return False
                    if cs.state.running:
                        return True
    except ApiException as e:
        logger.error(f"API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    return False

def _is_pod_present(rpc_port, cluster_id):
    k8s_core_v1 = core_utils.get_k8s_core_client()
    pod_name = f"snode-spdk-pod-{rpc_port}-{cluster_id}"
    try:
        resp = k8s_core_v1.list_namespaced_pod(node_utils_k8s.get_namespace())
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                return True
    except ApiException as e:
        logger.error(f"API error while checking pod presence: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while checking pod presence: {e}")
        return False
    return False


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_is_up(query: utils.RPCPortParams):
    if not query.cluster_id:
        return utils.get_response(False, "param required: cluster_id")

    first_six_cluster_id = core_utils.first_six_chars(query.cluster_id)
    if _is_pod_up(query.rpc_port, first_six_cluster_id):
        return utils.get_response(True)
    else:
        return utils.get_response(False, "SPDK container is not running")


DHCHAP_KEY_DIR = os.environ.get("DHCHAP_KEY_DIR", "/etc/simplyblock/dhchap_keys")


class WriteKeyFileBody(BaseModel):
    name: str = Field(..., description="Key name (used as filename)")
    content: str = Field(..., description="Key content in DHHC-1:XX:base64: format")


@api.post('/write_key_file', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def write_key_file(body: WriteKeyFileBody):
    """Write a DHCHAP key file for SPDK keyring_file module."""
    import re
    if not re.match(r'^[a-zA-Z0-9_\\-]+$', body.name):
        return utils.get_response(None, "Invalid key name")
    os.makedirs(DHCHAP_KEY_DIR, mode=0o700, exist_ok=True)
    key_path = os.path.join(DHCHAP_KEY_DIR, body.name)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, body.content.encode())
    finally:
        os.close(fd)
    return utils.get_response(key_path)


def _node_config_device_ids(node_config: dict) -> set[str]:
    """Identity of a node-config entry's device set, whichever device mode the
    host is in: PCI addresses (nvme) or block-device serials (lblk). A valid
    entry carries exactly one non-empty list, so the union identifies the
    entry without this endpoint having to know the cluster's mode.
    """
    return (set(node_config.get("ssd_pcis") or [])
            | core_utils.lblk_device_serials(node_config.get("lblk_devices")))


class PersistNodeConfigParams(BaseModel):
    max_lvol: Annotated[int | None, Field(ge=0, le=constants.MAX_SUBSYSTEMS_PER_NODE)] = None
    huge_page_memory: Annotated[int | None, Field(ge=0)] = None
    # small/large_pool_count are written alongside huge_page_memory whenever
    # add_node recalculates it against the cluster's real max_lvol/core count
    # -- they are what that memory figure was derived from (calculate_pool_count
    # -> calculate_minimum_hp_memory), so they must never drift from it.
    small_pool_count: Annotated[int | None, Field(ge=0)] = None
    large_pool_count: Annotated[int | None, Field(ge=0)] = None
    # Which node slot to write. A host can carry several (one per socket, and
    # more than one per socket when nodes_per_socket > 1), so the socket alone
    # does not identify one -- the slot's device set is what tells them apart.
    # Whichever device list this host's mode uses is the one that arrives:
    # PCI addresses in nvme mode, lblk device serials in lblk mode. Both empty
    # means "don't filter on devices", NOT "match nothing".
    numa_node: Annotated[int | None, Field(ge=0)] = None
    ssd_list: list[str] | None = None
    lblk_serials: list[str] | None = None
    # CPU layout, resized to the cluster's spdk_vcpu_count at add time (see
    # storage_node_ops.apply_cluster_vcpu_count). Written together, once, by
    # the same caller -- never partially, so the file never holds a mask from
    # one layout next to a distribution from another.
    cpu_mask: str | None = None
    isolated: list[int] | None = None
    l_cores: str | None = None
    distribution: dict | None = None
    core_to_index: dict | None = None
    number_of_distribs: Annotated[int | None, Field(ge=0)] = None


@api.post('/persist_node_config', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def persist_node_config(body: PersistNodeConfigParams):
    node_info = core_utils.load_config(constants.NODES_CONFIG_FILE)
    if not node_info.get("nodes"):
        return utils.get_response(False, "Config not found")

    # An EMPTY device list means the caller is not filtering on devices, the
    # same as sending none at all -- never "match nothing". lblk-mode configs
    # always carry ssd_pcis == [] (generate_configs seeds it, validate_config
    # requires exactly one of the two lists to be non-empty), so treating []
    # as a filter made every lblk node unmatchable and failed every add-node
    # that reached here.
    requested_devices = set(body.ssd_list or []) | set(body.lblk_serials or [])
    matched = False
    for node_config in node_info["nodes"]:
        if body.numa_node is not None and node_config["socket"] != body.numa_node:
            continue
        if requested_devices and not requested_devices.intersection(
                _node_config_device_ids(node_config)):
            continue
        if body.max_lvol is not None:
            node_config["max_lvol"] = body.max_lvol
        if body.huge_page_memory is not None:
            node_config["huge_page_memory"] = body.huge_page_memory
        if body.small_pool_count is not None:
            node_config["small_pool_count"] = body.small_pool_count
        if body.large_pool_count is not None:
            node_config["large_pool_count"] = body.large_pool_count
        if body.cpu_mask is not None:
            node_config["cpu_mask"] = body.cpu_mask
        if body.isolated is not None:
            node_config["isolated"] = body.isolated
        if body.l_cores is not None:
            node_config["l-cores"] = body.l_cores
        if body.distribution is not None:
            node_config["distribution"] = body.distribution
        if body.core_to_index is not None:
            node_config["core_to_index"] = body.core_to_index
        if body.number_of_distribs is not None:
            node_config["number_of_distribs"] = body.number_of_distribs
        matched = True
        break

    if not matched:
        return utils.get_response(
            False,
            f"No node config entry matches numa_node={body.numa_node} and "
            f"devices={sorted(requested_devices)}")

    # isolated_cores/host_cpu_mask are the union of every node's "isolated"
    # list, computed once at configure time (generate_configs/regenerate_
    # config) -- nothing reads them back today (the one consumer,
    # validate_config, always recomputes the union fresh from the nodes
    # themselves), but leaving them silently stale after a per-node isolated
    # list changes here is exactly the kind of drift that bites whoever
    # trusts them next. Recompute unconditionally; cheap, and correct
    # regardless of which field this call actually changed.
    all_isolated_cores: set[int] = set()
    for n in node_info["nodes"]:
        all_isolated_cores.update(n.get("isolated") or [])
    node_info["isolated_cores"] = sorted(all_isolated_cores)
    node_info["host_cpu_mask"] = core_utils.generate_mask(all_isolated_cores)

    # get_nodes_config() refuses (both here and on k8s) whenever the live file
    # differs from its "_read_only" sibling -- that is the drift check meant
    # to catch a hand-edited config. This write is sanctioned, not drift, so
    # it must refresh the read-only baseline too, or the very next /info call
    # (this same add_node's own idempotent retry included) sees a mismatch
    # and refuses with "run sbcli sn configure-upgrade".
    core_utils.store_config_file(node_info, constants.NODES_CONFIG_FILE, create_read_only_file=True)
    return utils.get_response(True)


@api.post('/set_hugepages', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def set_hugepages():
    return utils.get_response(True)


@api.post('/apply_config', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def apply_config():
    node_info = core_utils.load_config(constants.NODES_CONFIG_FILE)
    if node_info.get("nodes"):
        nodes = node_info["nodes"]
    else:
        logger.error("Please run sbcli sn configure before adding the storage node")
        return utils.get_response(False, "Please run sbcli sn configure before adding the storage noden")

    if not core_utils.validate_config(node_info):
        return utils.get_response(False, "Config validation is incorrect")

    # Set Huge page memory
    huge_page_memory_dict: dict = {}
    for node_config in nodes:
        hg_memory = node_config["huge_page_memory"]
        if int(node_config["max_size"]) > 0:
            hg_memory = max(hg_memory , node_config["max_size"])
        numa = node_config["socket"]
        huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + hg_memory + 1000000000
    for numa, huge_page_memory in huge_page_memory_dict.items():
        num_pages = huge_page_memory // 2000000
        core_utils.set_hugepages_if_needed(numa, num_pages)

    return utils.get_response(True)


@api.get('/check', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def is_alive():
    return utils.get_response(True)


@api.get('/spdk_proxy_restart', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_proxy_restart(query: utils.RPCPortParams):
    return utils.get_response(True)


@api.post('/bind_device_to_nvme')
def bind_device_to_nvme(body: utils.DeviceParams):
    pci_utils.ensure_driver(body.device_pci, 'nvme')
    return utils.get_response(True)


@api.post('/format_device_with_4k')
def format_device_with_4k(body: utils.DeviceParams):
    pci_utils.ensure_driver(body.device_pci, 'nvme')
    core_utils.format_device_with_4k(body.device_pci)
    return utils.get_response(True)


@api.post('/bind_device_to_spdk')
def bind_device_to_spdk(body: utils.DeviceParams):
    device_path = pci_utils.device(body.device_pci)
    iommu_group = device_path / 'iommu_group'
    vfio_module = Path('/sys/module/vfio')
    noiommu_parameter = vfio_module / 'parameters' / 'enable_unsafe_noiommu_mode'
    driver_name = None

    if pci_utils.driver_loaded('vfio-pci') and iommu_group.exists():
        driver_name = 'vfio-pci'
    elif pci_utils.driver_loaded('uio_pci_generic'):
        driver_name = 'uio_pci_generic'
    elif pci_utils.driver_loaded('vfio-pci') and noiommu_parameter.exists():
        if noiommu_parameter.read_text().strip() == 'N':
            noiommu_parameter.write_text('1')
        driver_name = 'vfio-pci'
    else:
        return utils.get_response_error(
            'SPDK PCI drivers are not fully loaded and device lacks IOMMU group', 500
        )

    pci_utils.ensure_driver(body.device_pci, driver_name, override=True)
    return utils.get_response(True)


class NicQuery(BaseModel):
    nic: str


@api.get('/ifc_is_roce', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean',
    })}}},
})
def ifc_is_roce(query: NicQuery):
    """
    Check if the given interface (including VLANs, bonds, bridges)
    ultimately uses a RoCE-capable NIC.
    """
    try:
        nic = query.nic if hasattr(query, "nic") else str(query)
        underlying = host_network.underlying_interfaces(nic)
        logger.debug(f"Resolved {nic} → underlying ifaces: {underlying}")
        roce_ifaces = host_network.rdma_netdevs()
        return utils.get_response(any(iface in roce_ifaces for iface in underlying))
    except Exception as e:
        logger.error(f"ifc_is_roce failed: {e}")

    return utils.get_response(False)


@api.get('/ifc_is_tcp', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean',
    })}}},
})
def ifc_is_tcp(query: NicQuery):
    try:
        nic = query.nic if hasattr(query, "nic") else str(query)
        if core_utils.get_iface_ip(nic):
            return utils.get_response(True)
    except Exception as e:
        logger.error(e)
    return utils.get_response(False)


class PingQuery(BaseModel):
    ip: str = Field(pattern=utils.IP_PATTERN)
    ifname: str = Field(pattern=utils.IFNAME_PATTERN)


@api.get('/ping_ip', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean',
    })}}},
})
def ping_ip(query: PingQuery):
    try:
        _, err, rc = shell_utils.run_command(host_network.host_command(f"ping -c 3 -W 1 {query.ip}"))
        if rc != 0:
            # run_command returns a process's stderr as bytes, and a failure to
            # start the process as a str.
            if isinstance(err, bytes):
                err = err.decode("utf-8", errors="replace")
            return utils.get_response(False, err or f"{query.ip} does not answer")
        return utils.get_response(host_network.has_carrier(query.ifname))
    except Exception as e:
        logger.error(e)
        return utils.get_response(False, str(e))


@api.get('/read_allowed_list', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'object',
        'additionalProperties': True,
    })}}},
})
def read_allowed_list():
    try:
        with open("/etc/simplyblock/allowed_list") as f:
            cores = [int(line.strip()) for line in f.read().split(' ')]
    except Exception:
        cores = []
    resp = utils.get_response(cores)
    return resp


class CoresParams(BaseModel):
    cores: list[int] | None = Field(default=None)
    number_of_alceml_devices: int | None = Field(None, ge=0)


@api.post('/recalculate_cores_distribution', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def recalculate_cores_distribution(body: CoresParams):
    cores = body.cores
    number_of_alceml_devices = body.number_of_alceml_devices
    distribution = core_utils.recalculate_cores_distribution(cores, number_of_alceml_devices)

    resp = utils.get_response({
        "app_thread_core": distribution["app_thread_core"],
        "jm_cpu_core": distribution["jm_cpu_core"],
        "poller_cpu_cores": distribution["poller_cpu_cores"],
        "alceml_cpu_cores": distribution["alceml_cpu_cores"],
        "alceml_worker_cpu_cores": distribution["alceml_worker_cpu_cores"],
        "distrib_cpu_cores": distribution["distrib_cpu_cores"],
        "jc_singleton_core": distribution["jc_singleton_core"],
        "lvol_poller_core": distribution["lvol_poller_core"]})
    return resp
