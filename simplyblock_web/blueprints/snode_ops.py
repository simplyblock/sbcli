#!/usr/bin/env python
# encoding: utf-8
import json
import math
import os
from pathlib import Path
import subprocess
import time
from typing import List, Optional, Union

import cpuinfo
import docker
import requests
from docker.types import LogConfig
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core import scripts, constants, shell_utils, utils as core_utils
import simplyblock_core.utils.pci as pci_utils
from simplyblock_web import utils, node_utils

logger = core_utils.get_logger(__name__)

api = APIBlueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"


class _RPCPortQuery(BaseModel):
    rpc_port: Optional[int] = Field(constants.RPC_HTTP_PROXY_PORT)


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
        session.timeout = 3
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document
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
    rpc_port: int = Field(constants.RPC_HTTP_PROXY_PORT, ge=1, le=65536)
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
    cluster_ip: Optional[str] = Field(default=None, pattern=utils.IP_PATTERN)


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
            '/sys:/sys'],
        environment=[
            f"RPC_PORT={body.rpc_port}",
            f"ssd_pcie={ssd_pcie_params}",
            f"PCI_ALLOWED={ssd_pcie_list}",
            f"TOTAL_HP={total_mem_mib}",
        ]
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )
    node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_core/services/spdk_http_proxy_server.py",
        name=f"spdk_proxy_{body.rpc_port}",
        detach=True,
        network_mode="host",
        log_config=log_config,
        volumes=[
            f'/var/tmp/spdk_{body.rpc_port}:/var/tmp',
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
def spdk_process_kill(query: _RPCPortQuery):
    for name in {f"/spdk_{query.rpc_port}", f"/spdk_proxy_{query.rpc_port}"}:
        core_utils.remove_container(get_docker_client(), name, graceful_timeout=0)
    return utils.get_response(True)


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_is_up(query: _RPCPortQuery):
    try:
        node_docker = get_docker_client()
        for cont in node_docker.containers.list(all=True):
            logger.debug(f"Container: {cont.attrs['Name']} status: {cont.attrs['State']}")
            if cont.attrs['Name'] == f"/spdk_{query.rpc_port}":
                status = cont.attrs['State']["Status"]
                is_running = cont.attrs['State']["Running"]
                if is_running:
                    return utils.get_response(True)
                else:
                    return utils.get_response(False, f"SPDK container status: {status}, is running: {is_running}")
    except Exception as e:
        logger.error(e)
    return utils.get_response(False, f"container not found: /spdk_{query.rpc_port}")


@api.get('/spdk_proxy_restart', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_proxy_restart(query: _RPCPortQuery):
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


def set_cluster_id(cluster_id):
    ret = os.popen(f"echo {cluster_id} > {cluster_id_file}").read().strip()
    return ret


def delete_cluster_id():
    out, _, _ = shell_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_node_lsblk():
    out, err, rc = shell_utils.run_command("lsblk -J")
    if rc != 0:
        logger.error(err)
        return []
    data = json.loads(out)
    return data


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
    return utils.get_response({
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


class _DeviceParams(BaseModel):
    device_pci: str


@api.post('/bind_device_to_nvme')
def bind_device_to_nvme(body: utils.DeviceParams):
    pci_utils.ensure_driver(body.device_pci, 'nvme')
    return utils.get_response(True)


@api.post('/delete_dev_gpt_partitions')
def delete_gpt_partitions_for_dev(body: _DeviceParams):
    bind_device_to_nvme(body)
    device_name = pci_utils.nvme_device_name(body.device_pci)
    subprocess.check_call(['parted', '-fs', f'/dev/{device_name}', 'mklabel' 'gpt'])
    return utils.get_response(True)


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


@api.post('/bind_device_to_spdk')
def bind_device_to_spdk(body: utils.DeviceParams):
    device_path = pci_utils.device(body.device_pci)
    iommu_group = device_path / 'iommu_group'
    vfio_module = Path('/sys/module/vfio')
    noiommu_parameter = vfio_module / 'parameters' / 'enable_unsafe_noiommu_mode'
    driver_name = None

    if pci_utils.driver_loaded('vfio-pci'):
        if iommu_group.exists():
            driver_name = 'vfio-pci'
        elif noiommu_parameter.exists():
            if noiommu_parameter.read_text().strip() == 'N':
                noiommu_parameter.write_text('1')
            driver_name = 'vfio-pci'
        elif pci_utils.driver_loaded('uio_pci_generic'):
            driver_name = 'uio_pci_generic'
        else:
            return utils.get_response_error(
                'SPDK PCI drivers are not fully loaded and device lacks IOMMU group', 500
            )
    elif pci_utils.driver_loaded('uio_pci_generic'):
        driver_name = 'uio_pci_generic'
    else:
        return utils.get_response_error('SPDK PCI drivers are not loaded', 500)

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
    huge_page_memory_dict = {}
    for node_config in nodes:
        numa = node_config["socket"]
        huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
    for numa, huge_page_memory in huge_page_memory_dict.items():
        num_pages = math.ceil(huge_page_memory / (2048 * 1024))
        core_utils.set_hugepages_if_needed(numa, num_pages)

    return utils.get_response(True)
