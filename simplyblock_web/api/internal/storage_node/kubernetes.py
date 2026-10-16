import json
import logging
import os
import time
import traceback

import yaml
from flask_openapi3 import APIBlueprint
from jinja2 import Environment, PackageLoader
from kubernetes.client import ApiException, V1DeleteOptions
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core import utils as core_utils
from simplyblock_core.settings import Settings
from simplyblock_core.utils import shell as shell_utils
from simplyblock_web import node_utils, node_utils_k8s, utils
from simplyblock_web.node_utils_k8s import namespace_id_file

from .._node_info import get_static_node_info
from . import docker as snode_ops

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

        "hostname": node_info["hostname"],
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


@api.post('/join_swarm', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def join_swarm():
    return utils.get_response(True)


@api.get('/leave_swarm', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def leave_swarm():
    return utils.get_response(True)


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


api.post('/delete_dev_gpt_partitions')(snode_ops.delete_gpt_partitions_for_dev)


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


def _env_flag(name):
    """A boolean the operator sets on the node agent's DaemonSet as the string
    'true'/'false' (OPENSHIFT_CLUSTER, TALOS_CLUSTER, ...). Unset is False."""
    return os.environ.get(name, "").strip().lower() == "true"


def _hugepage_shortfall(host_hugetlb_bytes, required_mib):
    """The message to fail the add with when the host carries fewer huge
    pages than the SPDK pod is about to request, or None when it carries
    enough.

    Used where the node cannot write the pages itself (Talos), both to decide
    whether anything has to be applied and to judge the result. Without it
    the pod would sit Pending on 'Insufficient hugepages-2Mi' with nothing
    naming the number. ``host_hugetlb_bytes`` is Hugetlb from /proc/meminfo:
    the pool across every page size and NUMA node, so this is a lower bound
    -- it can only pass a pod that at least fits the host as a whole.
    """
    available_mib = int(host_hugetlb_bytes) // (1024 * 1024)
    if available_mib >= required_mib:
        return None
    pages_2mi = -(-required_mib // 2)  # ceil
    return (f"host provides {available_mib} MiB of huge pages but the SPDK pod requests "
            f"{required_mib} MiB (hugepages-2Mi), i.e. at least {pages_2mi} 2 MiB pages")


def _hugepages_per_numa(nodes):
    """2 MiB pages to reserve on each NUMA node for this host's node configs:
    the sizing the init Job's apply_config has always used, so a Talos host
    (reserved through the Talos API) and any other (written to sysfs) end up
    with the same pool. The constants are kept as they were: a flat 1 GB of
    headroom per node config, and pages of 2,000,000 rather than 2 MiB
    bytes, which over-reserves by about 5 %."""
    per_numa_bytes: dict = {}
    for node_config in nodes:
        hg_memory = node_config["huge_page_memory"]
        if int(node_config["max_size"]) > 0:
            hg_memory = max(hg_memory, node_config["max_size"])
        numa = node_config["socket"]
        per_numa_bytes[numa] = per_numa_bytes.get(numa, 0) + hg_memory + 1000000000
    return {numa: mem // 2000000 for numa, mem in per_numa_bytes.items()}


def _talos_sysfs_patch(pages_per_numa):
    """The strategic-merge machine-config patch that reserves the pages.

    machine.sysfs keys are dotted paths under /sys (Talos swaps dots for
    slashes), so this is the same per-NUMA nr_hugepages file sysfs mode
    writes, only applied by Talos. A merge patch adds to the sysfs map
    rather than replacing it, so whatever else the operator keeps there
    (cpufreq governors, ...) survives. machine.sysfs is deprecated on
    recent Talos in favour of a SysfsConfig document but still honoured,
    and it is what every supported Talos minor understands."""
    return {"machine": {"sysfs": {
        f"devices.system.node.node{numa}.hugepages.hugepages-2048kB.nr_hugepages": str(pages)
        for numa, pages in sorted(pages_per_numa.items())
    }}}


_K8S_QUANTITY_MIB = {'Ki': 1 / 1024, 'Mi': 1, 'Gi': 1024, 'Ti': 1024 * 1024}


def _quantity_to_mib(quantity):
    """A Kubernetes resource quantity ('13Gi', '12800Mi', '0', bytes) in MiB."""
    q = str(quantity or '0').strip()
    for suffix, factor in _K8S_QUANTITY_MIB.items():
        if q.endswith(suffix):
            return int(float(q[:-len(suffix)]) * factor)
    return int(float(q)) // (1024 * 1024)


def _job_name(prefix, node_name):
    """A Job name under Kubernetes' 63-character limit that keeps the end of
    the node name, where generated names differ."""
    name = prefix + node_name
    return name[len(name) - 63:] if len(name) > 63 else name


_TALOS_JOB_TIMEOUT_S = 300
_TALOS_HUGEPAGE_WAIT_S = 240
_TALOS_HUGEPAGE_POLL_S = 5


def _talos_api_secret_missing(k8s_core_v1, namespace, secret_name):
    """None when the Talos API credentials Secret is there (or cannot be
    checked), else the message to fail with. Checked up front because a
    missing Secret otherwise leaves the Job's pod in ContainerCreating for
    the whole Job timeout with a FailedMount event nobody reads."""
    try:
        k8s_core_v1.read_namespaced_secret(secret_name, namespace)
    except ApiException as e:
        if e.status == 404:
            return (f"Talos API credentials Secret '{secret_name}' not found in namespace '{namespace}': "
                    f"enable Talos API access from Kubernetes for this namespace with the os:admin role and "
                    f"create a talos.dev/v1alpha1 ServiceAccount named '{secret_name}' with roles [os:admin] "
                    f"(see docs/talos.md)")
        logger.warning("Cannot check for Secret '%s' (HTTP %s); letting the Job tell", secret_name, e.status)
    return None


def _wait_for_hugepages(k8s_core_v1, node_name, required_mib):
    """Block until the host's pool and the kubelet's view of it both cover
    the pod's request, or return the message to fail with. The kubelet reads
    huge-page capacity at start, so the second half is what the restart in
    the Job was for."""
    deadline = time.monotonic() + _TALOS_HUGEPAGE_WAIT_S
    while True:
        shortfall = _hugepage_shortfall(node_utils.get_huge_memory(), required_mib)
        kubelet_mib = None
        if shortfall is None:
            node = k8s_core_v1.read_node(node_name)
            kubelet_mib = _quantity_to_mib((node.status.capacity or {}).get('hugepages-2Mi'))
            if kubelet_mib >= required_mib:
                return None
        if time.monotonic() >= deadline:
            if shortfall is not None:
                return f"after applying the machine config patch, {shortfall}"
            return (f"the kubelet on '{node_name}' still reports {kubelet_mib} MiB of hugepages-2Mi "
                    f"capacity against the SPDK pod's {required_mib} MiB after its restart")
        time.sleep(_TALOS_HUGEPAGE_POLL_S)


def _provision_talos_hugepages(required_mib, node_name, namespace, values):
    """Make a Talos worker's huge-page pool cover the SPDK pod about to be
    created, through the Talos API, and return None -- or the message to
    fail the add with.

    Talos has no host shell and no writable sysfs for us, so the init Job's
    nr_hugepages write is replaced by a Job running talosctl against the
    node's own apid: patch the per-NUMA counts into machine.sysfs (applied
    live) and restart the kubelet so its capacity follows. A pool that
    already covers the request is left alone -- in particular no kubelet
    restart on a plain node restart.
    """
    if _hugepage_shortfall(node_utils.get_huge_memory(), required_mib) is None:
        logger.info("Huge-page pool on '%s' already covers the SPDK pod's %s MiB; nothing to apply",
                    node_name, required_mib)
        return None

    node_info = core_utils.load_config(constants.NODES_CONFIG_FILE)
    nodes = node_info.get("nodes") or []
    if not nodes:
        return "Please run sbcli sn configure before adding the storage node"
    pages_per_numa = _hugepages_per_numa(nodes)

    k8s_core_v1 = core_utils.get_k8s_core_client()
    secret_name = os.environ.get("TALOS_API_SECRET") or constants.TALOS_API_SECRET
    missing = _talos_api_secret_missing(k8s_core_v1, namespace, secret_name)
    if missing:
        return missing

    job_values = dict(values,
                      TALOS_JOBNAME=_job_name("snode-talos-hugepages-", node_name),
                      TALOSCTL_IMAGE=os.environ.get("TALOSCTL_IMAGE") or constants.TALOSCTL_IMAGE,
                      TALOS_API_SECRET=secret_name,
                      TALOS_PATCH=json.dumps(_talos_sysfs_patch(pages_per_numa), separators=(',', ':')))
    logger.info("Reserving huge pages on Talos node '%s' through the Talos API: %s 2 MiB pages per NUMA node",
                node_name, pages_per_numa)
    env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
    job_yaml = yaml.safe_load(env.get_template('storage_talos_hugepages_job.yaml.j2').render(job_values))
    batch_v1 = core_utils.get_k8s_batch_client()
    job_resp = _create_job_replacing_stale(batch_v1, namespace, job_yaml)
    logger.info("Job created: '%s' in namespace '%s'", job_resp.metadata.name, namespace)
    try:
        node_utils_k8s.wait_for_job_completion(job_resp.metadata.name, namespace, timeout=_TALOS_JOB_TIMEOUT_S)
    except (RuntimeError, TimeoutError) as e:
        # Left in place: its pod log says why talosctl failed (role not
        # allowed, apid unreachable, patch rejected).
        return (f"Talos huge-page Job on '{node_name}' did not succeed: {e}; "
                f"see the pod log of Job '{job_resp.metadata.name}'")
    try:
        batch_v1.delete_namespaced_job(
            name=job_resp.metadata.name, namespace=namespace,
            body=V1DeleteOptions(propagation_policy='Foreground', grace_period_seconds=0))
    except ApiException as e:
        if e.status != 404:
            raise
    return _wait_for_hugepages(k8s_core_v1, node_name, required_mib)


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
    firewall_port: int | None = Field(constants.FW_PORT_START)
    cluster_id: str
    mcp_max_unavailable: int | None = Field(None)


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

    # Talos is stated the same way OpenShift is: a flag the operator sets on
    # the node agent from the StorageCluster's environment, not something
    # sniffed off the host at run time. On Talos the host is immutable and
    # has no shell, so nothing here touches it directly -- no nr_hugepages
    # write, no nsenter kubelet restart, no grubby/KubeletConfig Job. Huge
    # pages are reserved through the Talos API instead (a talosctl Job
    # patching machine.sysfs and restarting the kubelet), and the add waits
    # for the pool before creating the pod; see _provision_talos_hugepages.
    talos = _env_flag("TALOS_CLUSTER")

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
            'SERVER_IP': body.server_ip,
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
            'FW_PORT': body.firewall_port,
            'CPU_TOPOLOGY_ENABLED': cpu_topology_enabled,
            'TALOS_CLUSTER': talos,
            'RESERVED_SYSTEM_CPUS': reserved_system_cpus,
            'MCP_MAX_UNAVAILABLE': mcp_max_unavailable,
            'OPENSHIFT_MCP': openshift_mcp,
            'TLS_SERVE': settings.tls_serve,
            'TLS_CONNECT': settings.tls_connect,
            'TLS_CLIENT_AUTH': settings.model_dump()["tls_client_auth"],
            'TLS_PROVIDER': settings.tls_provider,
        }

        if talos:
            err = _provision_talos_hugepages(values['MEM_MEGA'], node_name, namespace, values)
            if err:
                logger.error(err)
                return utils.get_response(False, err)

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
        wants_host_config = (cpu_topology_enabled and not skip_kubelet_configuration) or (core_isolate and not cpu_topology_enabled)
        if wants_host_config and talos:
            # Both Jobs nsenter into the host (kubelet config, grubby). Talos
            # has neither; its kubelet and kernel args come from the machine
            # config, so this is a configuration mismatch to surface, not a
            # Job to attempt and watch fail.
            logger.warning("TALOS_CLUSTER is set: skipping the CPU-topology/core-isolation Job on '%s'; "
                           "configure the kubelet and kernel args through the Talos machine config", node_name)
        elif wants_host_config:
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
            node_utils_k8s.wait_for_job_completion(core_resp.metadata.name, namespace)
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

        env = Environment(loader=PackageLoader('simplyblock_web', 'templates'), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('storage_deploy_spdk.yaml.j2')
        docs = yaml.safe_load_all(template.render(values))
        for dep in docs:
            logger.debug(dep)
            resp = k8s_core_v1.create_namespaced_pod(body=dep, namespace=namespace)
            msg = f"Pod created: '{resp.metadata.name}' in namespace '{namespace}'"
            logger.info(msg)
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
    """Authoritative SPDK teardown for failure-cleanup paths. Pod deletion in
    this deployment mode is already synchronous and verified (see
    spdk_process_kill's poll-until-gone), so this is an alias kept for parity
    with the docker agent, where kill (fast, detached remove) and cleanup
    (slow, verified remove) are distinct."""
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


DHCHAP_KEY_DIR = "/etc/simplyblock/dhchap_keys"


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


class _FirewallParams(BaseModel):
    port_id: int
    port_type: str
    action: str
    rpc_port: int = Field(ge=1, le=65536)


@api.post('/firewall_set_port', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def firewall_set_port(body: _FirewallParams):
    return utils.get_response(False, "deprecated bath post snode/firewall_set_port")

@api.get('/get_firewall', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def get_firewall():
    return utils.get_response(False, "deprecated bath get snode/get_firewall")


@api.post('/persist_node_config', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def persist_node_config(body: snode_ops.PersistNodeConfigParams):
    return snode_ops.persist_node_config(body)


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
    for numa, num_pages in _hugepages_per_numa(nodes).items():
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

api.post('/bind_device_to_nvme')(snode_ops.bind_device_to_nvme)

api.post('/bind_device_to_spdk')(snode_ops.bind_device_to_spdk)

api.get('/ifc_is_tcp')(snode_ops.ifc_is_tcp)

api.get('/ifc_is_roce')(snode_ops.ifc_is_roce)

api.post('/format_device_with_4k')(snode_ops.format_device_with_4k)

api.get('/ping_ip')(snode_ops.ping_ip)

api.get('/read_allowed_list')(snode_ops.read_allowed_list)

api.post('/recalculate_cores_distribution')(snode_ops.recalculate_cores_distribution)
