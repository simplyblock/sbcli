#!/usr/bin/env python
# encoding: utf-8
import json
import math
import os
from pathlib import Path
import time
from typing import List, Optional, Union

import cpuinfo
import docker
import psutil
import requests
import socket
from docker.types import LogConfig
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core import scripts, constants, shell_utils, utils as core_utils
import simplyblock_core.utils.pci as pci_utils
import simplyblock_core.utils as init_utils
from simplyblock_web import utils, node_utils

logger = core_utils.get_logger(__name__)

api = APIBlueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"


def get_google_cloud_info():
    try:
        headers = {'Metadata-Flavor': 'Google'}
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true",
                                headers=headers, timeout=3)
        data = response.json()
        return {
            "id": str(data["id"]),
            "type": data["machineType"].split("/")[-1],
            "cloud": "google",
            "ip": data["networkInterfaces"][0]["ip"],
            "public_ip": data["networkInterfaces"][0]["accessConfigs"][0]["externalIp"],
        }
    except Exception:
        pass


def get_equinix_cloud_info():
    try:
        response = requests.get("https://metadata.platformequinix.com/metadata", timeout=3)
        data = response.json()
        public_ip = ""
        ip = ""
        for interface in data["network"]["addresses"]:
            if interface["address_family"] == 4:
                if interface["enabled"] and interface["public"]:
                    public_ip = interface["address"]
                elif interface["enabled"] and not interface["public"]:
                    public_ip = interface["address"]
        return {
            "id": str(data["id"]),
            "type": data["class"],
            "cloud": "equinix",
            "ip": public_ip,
            "public_ip": ip
        }
    except Exception:
        pass


def get_amazon_cloud_info():
    try:
        import ec2_metadata
        import requests
        session = requests.session()
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document  # type: ignore[call-arg]
        return {
            "id": data["instanceId"],
            "type": data["instanceType"],
            "cloud": "amazon",
            "ip": data["privateIp"],
            "public_ip": "",
        }
    except Exception:
        pass


def get_docker_client(timeout=60):
    try:
        cl = docker.DockerClient(base_url='unix://var/run/docker.sock', version="auto", timeout=timeout)
        cl.info()
        return cl
    except Exception:
        ip = os.getenv("DOCKER_IP")
        if not ip:
            for ifname in core_utils.get_nics_data():
                if ifname in ["eth0", "ens0"]:
                    ip = core_utils.get_nics_data()[ifname]['ip']
                    break
        cl = docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=timeout)
        try:
            cl.info()
            return cl
        except Exception:
            pass


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


class SPDKParams(BaseModel):
    server_ip: str = Field(pattern=utils.IP_PATTERN)
    rpc_port: int = Field(constants.RPC_PORT_RANGE_START, ge=1, le=65536)
    rpc_username: str
    rpc_password: str
    ssd_pcie: Optional[List[str]] = Field(None)
    spdk_debug: Optional[bool] = Field(False)
    l_cores: Optional[str] = Field(None)
    spdk_mem: int = Field(core_utils.parse_size('4GiB'))
    total_mem: Optional[Union[int, str]] = Field('')
    multi_threading_enabled: Optional[bool] = Field(False)
    timeout: Optional[int] = Field(5 * 60)
    spdk_image: Optional[str] = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    spdk_proxy_image: Optional[str] = Field(constants.SIMPLY_BLOCK_DOCKER_IMAGE)
    cluster_ip: Optional[str] = Field(default=None, pattern=utils.IP_PATTERN)
    cluster_mode: str
    socket: Optional[int] = Field(None, ge=0)
    firewall_port: int = Field(constants.FW_PORT_START)
    cluster_id: str


@api.post('/spdk_process_start', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_start(body: SPDKParams):
    ssd_pcie_params = " -A " + " -A ".join(body.ssd_pcie) if body.ssd_pcie else ""
    ssd_pcie_list = " ".join(body.ssd_pcie) if body.ssd_pcie else "none"
    spdk_debug = '1' if body.spdk_debug else ''
    total_mem_mib = core_utils.convert_size(core_utils.parse_size(body.total_mem), 'MiB') if body.total_mem else ''
    spdk_mem_mib = core_utils.convert_size(body.spdk_mem, 'MiB')

    node_docker = get_docker_client(timeout=60 * 3)
    for name in {f"/spdk_{body.rpc_port}", f"/spdk_proxy_{body.rpc_port}"}:
        core_utils.remove_container(node_docker, name, graceful_timeout=0)

    if body.cluster_ip is not None:
        log_config = LogConfig(type=LogConfig.types.GELF, config={"gelf-address": f"tcp://{body.cluster_ip}:12202"})
    else:
        log_config = LogConfig(type=LogConfig.types.JOURNALD)

    container = node_docker.containers.run(
        body.spdk_image,
        f"/root/scripts/run_distr_with_ssd.sh {body.l_cores} {spdk_mem_mib} {spdk_debug}",
        name=f"spdk_{body.rpc_port}",
        detach=True,
        privileged=True,
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/etc/simplyblock:/etc/simplyblock',
            f'/var/tmp/spdk_{body.rpc_port}:/var/tmp',
            '/dev:/dev',
            f'/tmp/shm_{body.rpc_port}/:/dev/shm/',
            '/lib/modules/:/lib/modules/',
            '/var/lib/systemd/coredump/:/var/lib/systemd/coredump/',
            '/sys:/sys',
            '/mnt/ramdisk:/mnt/ramdisk',
        ],
        environment=[
            f"RPC_PORT={body.rpc_port}",
            f"ssd_pcie={ssd_pcie_params}",
            f"PCI_ALLOWED={ssd_pcie_list}",
            f"TOTAL_HP={total_mem_mib}",
            f"NSOCKET={body.socket}",
            f"FW_PORT={body.firewall_port}",
        ]
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )
    node_docker.containers.run(
        body.spdk_proxy_image,
        "sudo python3 simplyblock_core/services/spdk_http_proxy_server.py ",
        name=f"spdk_proxy_{body.rpc_port}",
        detach=True,
        network_mode="host",
        log_config=log_config,
        ulimits=[docker.types.Ulimit(name='nofile', soft=65536, hard=65536)],
        volumes=[
            f'/var/tmp/spdk_{body.rpc_port}:/var/tmp',
            '/mnt/ramdisk:/mnt/ramdisk',
        ],
        environment=[
            f"SERVER_IP={body.server_ip}",
            f"RPC_PORT={body.rpc_port}",
            f"RPC_USERNAME={body.rpc_username}",
            f"RPC_PASSWORD={body.rpc_password}",
            f"MULTI_THREADING_ENABLED={body.multi_threading_enabled}",
            f"TIMEOUT={body.timeout}",
        ]
        # restart_policy={"Name": "always"}
    )
    retries = 10
    while retries > 0:
        info = node_docker.containers.get(container.attrs['Id'])
        status = info.attrs['State']["Status"]
        is_running = info.attrs['State']["Running"]
        if not is_running:
            logger.info("Container is not running, waiting...")
            time.sleep(3)
            retries -= 1
        else:
            logger.info(f"Container status: {status}, Is Running: {is_running}")
            return utils.get_response(True)

    return utils.get_response(
        False, f"Container create max retries reached, Container status: {status}, Is Running: {is_running}")


@api.get('/spdk_process_kill', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_kill(query: utils.RPCPortParams):
    for name in {f"/spdk_{query.rpc_port}", f"/spdk_proxy_{query.rpc_port}"}:
        core_utils.remove_container(get_docker_client(), name, graceful_timeout=0)
    return utils.get_response(True)


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_is_up(query: utils.RPCPortParams):
    req_unique_id = time.time_ns()
    logger.debug(f"function:spdk_process_is_up start f{req_unique_id}")
    try:
        node_docker = get_docker_client()
        for cont in node_docker.containers.list(all=True):
            logger.debug(f"Container: {cont.attrs['Name']} status: {cont.attrs['State']}")
            if cont.attrs['Name'] == f"/spdk_{query.rpc_port}":
                status = cont.attrs['State']["Status"]
                is_running = cont.attrs['State']["Running"]
                logger.debug("function:spdk_process_is_up end")
                if is_running:
                    return utils.get_response(True)
                else:
                    return utils.get_response(False, f"SPDK container status: {status}, is running: {is_running}")
    except Exception as e:
        logger.error(e)
    logger.debug(f"function:spdk_process_is_up end f{req_unique_id}")
    total_time = int((time.time_ns() - req_unique_id) / (1000 * 1000 * 1000))
    logger.debug(f"function:spdk_process_is_up total time {total_time}")
    return utils.get_response(False, f"container not found: /spdk_{query.rpc_port}")


@api.get('/spdk_proxy_restart', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_proxy_restart(query: utils.RPCPortParams):
    try:
        node_docker = get_docker_client()
        for cont in node_docker.containers.list(all=True):
            if cont.attrs['Name'] == f"/spdk_proxy_{query.rpc_port}":
                cont.restart(timeout=3)
                return utils.get_response(True)
    except Exception as e:
        logger.error(e)
        return utils.get_response(False, str(e))

    return utils.get_response(False, f"container not found: /spdk_proxy_{query.rpc_port}")


def get_cluster_id():
    out, _, _ = shell_utils.run_command(f"cat {cluster_id_file}")
    return out


class FilePath(BaseModel):
    file_name: str


@api.get('/get_file_content/<string:file_name>', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def get_file_content(path: FilePath):
    out, err, _ = shell_utils.run_command(f"cat /etc/simplyblock/{path.file_name}")
    if out:
        return utils.get_response(out)
    elif err:
        err = err.decode("utf-8")
        logger.debug(err)
        return utils.get_response(None, err)


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
    if not re.match(r'^[a-zA-Z0-9_\-]+$', body.name):
        return utils.get_response(None, "Invalid key name")
    os.makedirs(DHCHAP_KEY_DIR, mode=0o700, exist_ok=True)
    key_path = os.path.join(DHCHAP_KEY_DIR, body.name)
    fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.write(fd, body.content.encode())
    finally:
        os.close(fd)
    return utils.get_response(key_path)


def set_cluster_id(cluster_id):
    ret = os.popen(f"echo {cluster_id} > {cluster_id_file}").read().strip()
    return ret


def delete_cluster_id():
    out, _, _ = shell_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_node_lsblk():
    logger.debug("function:get_node_lsblk start")
    out, err, rc = shell_utils.run_command("lsblk -J")
    if rc != 0:
        logger.error(err)
        return []
    data = json.loads(out)
    logger.debug("function:get_node_lsblk end")
    return data


def get_nodes_config():
    logger.debug("function:get_nodes_config start")
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
        logger.debug("function:get_nodes_config end")
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
    logger.debug("function:get_info start")
    resp = utils.get_response({
        "cluster_id": get_cluster_id(),

        "hostname": HOSTNAME,
        "system_id": SYSTEM_ID,

        "cpu_count": CPU_INFO['count'],
        "cpu_hz": CPU_INFO['hz_advertised'][0] if 'hz_advertised' in CPU_INFO else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),

        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),

        "network_interface": core_utils.get_nics_data(),

        "cloud_instance": CLOUD_INFO,

        "lsblk": get_node_lsblk(),
        "nodes_config": get_nodes_config(),
    })
    logger.debug("function:get_info end")
    return resp


class _JoinSwarmParams(BaseModel):
    cluster_ip: str = Field(pattern=utils.IP_PATTERN)
    cluster_id: str = Field(pattern=core_utils.UUID_PATTERN)
    join_token: str
    db_connection: str


@api.post('/join_swarm', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def join_swarm(body: _JoinSwarmParams):
    logger.info("Setting DB connection")
    scripts.set_db_config(body.db_connection)
    set_cluster_id(body.cluster_id)

    logger.info("Joining Swarm")
    node_docker = get_docker_client()
    if node_docker.info()["Swarm"]["LocalNodeState"] in ["active", "pending"]:
        logger.info("Node is part of another swarm, leaving swarm")
        for i in range(5):
            try:
                node_docker.swarm.leave(force=True)
                time.sleep(2)
                break
            except Exception as e:
                logger.warning(f"Error leaving swarm: {e}")

    for i in range(5):
        try:
            node_docker.swarm.join([f"{body.cluster_ip}:2377"], body.join_token)
            logger.info("Joining docker swarm > Done")
            return utils.get_response(True)
        except Exception as e:
            logger.warning(f"Error joining docker swarm: {e}")
    msg = "Could not join docker swarm"
    logger.error(msg)
    return utils.get_response(False, msg)


@api.get('/leave_swarm', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def leave_swarm():
    delete_cluster_id()
    for i in range(5):
        try:
            node_docker = get_docker_client()
            node_docker.swarm.leave(force=True)
            return utils.get_response(True)
        except Exception as e:
            logger.warning(f"Error leaving swarm: {e}")
    msg = "Could not leave docker swarm"
    logger.error(msg)
    return utils.get_response(False, msg)


class _GPTPartitionsParams(BaseModel):
    nbd_device: str = Field('/dev/nbd0')
    jm_percent: int = Field(3, ge=0, le=100)
    num_partitions: int = Field(1, ge=0)
    partition_percent: int = Field(0, ge=0, le=100)


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
    if body.partition_percent:
        perc_per_partition = body.partition_percent
    else:
        perc_per_partition = int((100 - body.jm_percent) / body.num_partitions)

    for i in range(body.num_partitions):
        st = body.jm_percent + (i * perc_per_partition)
        en = st + perc_per_partition
        cmd_list.append(f"parted -f {body.nbd_device} mkpart part{(i + 1)} \"{st}%\" \"{en}%\"")
        sg_cmd_list.append(f"sgdisk -t {(i + 2)}:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}")

    for cmd in cmd_list + sg_cmd_list:
        logger.debug(cmd)
        out, err, ret_code = shell_utils.run_command(cmd)
        logger.debug(out)
        logger.debug(ret_code)
        if ret_code != 0:
            logger.error(err)
            return utils.get_response(False,
                                      f"Error running cmd: {cmd}, returncode: {ret_code}, output: {out}, err: {err}")
        time.sleep(1)

    return utils.get_response(True)


@api.post('/bind_device_to_nvme')
def bind_device_to_nvme(body: utils.DeviceParams):
    pci_utils.ensure_driver(body.device_pci, 'nvme')
    return utils.get_response(True)


@api.post('/delete_dev_gpt_partitions')
def delete_gpt_partitions_for_dev(body: utils.DeviceParams):
    bind_device_to_nvme(body)
    device_name = pci_utils.nvme_device_name(body.device_pci)
    cmd = f"parted -fs /dev/{device_name} mklabel gpt"
    out, err, ret_code = shell_utils.run_command(cmd)
    logger.info(f"out: {out}, err: {err}, ret_code: {ret_code}")
    return utils.get_response(ret_code == 0, error=err)


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = shell_utils.run_command("hostname -s")
SYSTEM_ID, _, _ = shell_utils.run_command("dmidecode -s system-uuid")
CLOUD_INFO = {}
if not os.environ.get("WITHOUT_CLOUD_INFO"):
    CLOUD_INFO = get_amazon_cloud_info()
    if not CLOUD_INFO:
        CLOUD_INFO = get_google_cloud_info()

    if not CLOUD_INFO:
        CLOUD_INFO = get_equinix_cloud_info()

    if CLOUD_INFO:
        SYSTEM_ID = CLOUD_INFO["id"]


@api.post('/format_device_with_4k')
def format_device_with_4k(body: utils.DeviceParams):
    pci_utils.ensure_driver(body.device_pci, 'nvme')
    init_utils.format_device_with_4k(body.device_pci)
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


@api.post('/set_hugepages', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def set_hugepages():
    node_info = core_utils.load_config(constants.NODES_CONFIG_FILE)
    if node_info.get("nodes"):
        nodes = node_info["nodes"]
    else:
        logger.error("Please run sbcli sn configure before adding the storage node, "
                     "If you run it and the config has been manually changed please "
                     "run 'sbcli sn configure-upgrade'")
        return utils.get_response(False, "Please run sbcli sn configure before adding the storage node")

    if not core_utils.validate_config(node_info):
        return utils.get_response(False, "Config validation is incorrect")

    # Set Huge page memory
    huge_page_memory_dict: dict = {}
    for node_config in nodes:
        numa = node_config["socket"]
        huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
    for numa, huge_page_memory in huge_page_memory_dict.items():
        num_pages = math.ceil(huge_page_memory / (2048 * 1024))
        core_utils.set_hugepages_if_needed(numa, num_pages)

    return utils.get_response(True)


class NicQuery(BaseModel):
    nic: str


def resolve_underlying_ifaces(nic):
    base_path = f"/sys/class/net/{nic}"
    if not os.path.exists(base_path):
        return []

    # Handle bridge devices (collect all lower interfaces)
    bridge_path = os.path.join(base_path, "brif")
    if os.path.exists(bridge_path):
        lower_ifaces = os.listdir(bridge_path)
        resolved = []
        for lower in lower_ifaces:
            resolved.extend(resolve_underlying_ifaces(lower))
        return resolved

    # Handle bonded interfaces (collect all slaves)
    bond_slaves = os.path.join(base_path, "bonding/slaves")
    if os.path.exists(bond_slaves):
        try:
            with open(bond_slaves, "r") as f:
                slaves = f.read().strip().split()
            resolved = []
            for s in slaves:
                resolved.extend(resolve_underlying_ifaces(s))
            return resolved
        except Exception as e:
            logger.warning(f"Failed to read bond slaves for {nic}: {e}")

    # Handle VLANs (detect lower_* symlinks like lower_bond0)
    lowers = [f for f in os.listdir(base_path) if f.startswith("lower_")]
    if lowers:
        resolved = []
        for lower in lowers:
            try:
                target = os.path.basename(os.path.realpath(os.path.join(base_path, lower)))
                resolved.extend(resolve_underlying_ifaces(target))
            except Exception as e:
                logger.warning(f"Failed to resolve {lower} for {nic}: {e}")
        return resolved

    # Fallback: device symlink (for VLANs on physical ifaces)
    vlan_dev_path = os.path.join(base_path, "device")
    if os.path.islink(vlan_dev_path):
        try:
            real_path = os.path.realpath(vlan_dev_path)
            parent = real_path.split("/")[-1]
            if parent != nic and os.path.exists(f"/sys/class/net/{parent}"):
                return resolve_underlying_ifaces(parent)
        except Exception as e:
            logger.warning(f"Failed to resolve VLAN parent for {nic}: {e}")

    # Default: treat as physical
    return [nic]


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
        rdma_path = "/sys/class/infiniband/"
        if not os.path.exists(rdma_path):
            return utils.get_response(False)

        underlying = resolve_underlying_ifaces(nic)
        logger.debug(f"Resolved {nic} → underlying ifaces: {underlying}")

        for rdma_dev in os.listdir(rdma_path):
            net_path = os.path.join(rdma_path, rdma_dev, "device/net")
            if os.path.exists(net_path):
                roce_ifaces = os.listdir(net_path)
                for iface in underlying:
                    if iface in roce_ifaces:
                        return utils.get_response(True)
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
        addrs = psutil.net_if_addrs().get(nic, [])
        for addr in addrs:
            if addr.family == socket.AF_INET:
                return utils.get_response(True)
    except Exception as e:
        logger.error(e)
    return utils.get_response(False)


@api.get('/check', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def is_alive():
    return utils.get_response(True)


@api.post('/nvme_connect',
    summary='Connect NVMe-oF target',
    responses={
        200: {'content': {'application/json': {'schema': utils.response_schema({
            'type': 'boolean',
        })}},
    },
})
def connect_to_nvme(body: utils.NVMEConnectParams):
    """Connect to the indicated NVMe-oF target.
    """
    st = f"nvme connect --transport=tcp --traddr={body.ip} --trsvcid={body.port} --nqn={body.nqn}"
    logger.debug(st)
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    if ret_code == 0:
        return utils.get_response(True)
    else:
        return utils.get_response(ret_code, error=err)


@api.post('/disconnect_nqn',
    summary='Disconnect NVMe-oF device by NQN',
    responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'integer',
    })}}},
})
def disconnect_nqn(body: utils.DisconnectParams):
    """Disconnect from indicated NVMe-oF target
    """
    st = f"nvme disconnect --nqn={body.nqn}"
    out, err, ret_code = shell_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


class PingQuery(BaseModel):
    ip: str
    ifname: str

@api.get('/ping_ip', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean',
    })}}},
})
def ping_ip(query: PingQuery):
    try:
        ping_response = os.system(f"ping -c 3 -W 1 {query.ip} > /dev/null 2>&1")
        link_is_up = False
        with open(f"/sys/class/net/{query.ifname}/carrier") as f:
            link_is_up = f.read().strip() == "1"
        return utils.get_response(ping_response == 0 and link_is_up)
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
    cores: Optional[List[int]] = Field(default=None)
    number_of_alceml_devices: Optional[int] = Field(None, ge=0)


@api.post('/recalculate_cores_distribution', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def recalculate_cores_distribution(body: CoresParams):
    cores = body.cores
    number_of_alceml_devices = body.number_of_alceml_devices
    distribution = init_utils.recalculate_cores_distribution(cores, number_of_alceml_devices)

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
