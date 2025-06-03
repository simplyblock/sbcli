#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os
import time
import traceback
from typing import List, Optional, Union

import cpuinfo
import requests
from flask_openapi3 import APIBlueprint
from kubernetes.client import ApiException
from jinja2 import Environment, FileSystemLoader
import yaml
from pydantic import BaseModel, Field

from simplyblock_core import constants, shell_utils, utils as core_utils
from simplyblock_web import utils, node_utils, node_utils_k8s
from simplyblock_web.node_utils_k8s import deployment_name, namespace_id_file, pod_name

logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)
api = APIBlueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def set_namespace(namespace):
    if not os.path.exists(namespace_id_file):
        try:
            os.makedirs(os.path.dirname(namespace_id_file), exist_ok=True)
        except:
            return False
    with open(namespace_id_file, "w+") as f:
        f.write(namespace)
    return True


def get_google_cloud_info():
    try:
        headers = {'Metadata-Flavor': 'Google'}
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true", headers=headers, timeout=2)
        data = response.json()
        return {
            "id": str(data["id"]),
            "type": data["machineType"].split("/")[-1],
            "cloud": "google",
            "ip": data["networkInterfaces"][0]["ip"],
            "public_ip": data["networkInterfaces"][0]["accessConfigs"][0]["externalIp"],
        }
    except:
        pass


def get_equinix_cloud_info():
    try:
        response = requests.get("https://metadata.platformequinix.com/metadata", timeout=2)
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
    except:
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
            "public_ip":  "",
        }
    except:
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
    return {
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
        "nodes_config": get_nodes_config(),
    }


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


class _DeviceParams(BaseModel):
    device_pci: str


@api.post('/delete_dev_gpt_partitions')
def delete_gpt_partitions_for_dev(body: _DeviceParams):
    cmd_list = [
        f"echo -n \"{body.device_pci}\" > /sys/bus/pci/drivers/uio_pci_generic/unbind",
        f"echo -n \"{body.device_pci}\" > /sys/bus/pci/drivers/nvme/bind",
    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    device_name = os.popen(f"ls /sys/devices/pci0000:00/{body.device_pci}/nvme/nvme*/ | grep nvme").read().strip()
    cmd_list = [
        f"parted -fs /dev/{device_name} mklabel gpt",
        f"echo -n \"{body.device_pci}\" > /sys/bus/pci/drivers/nvme/unbind",
    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    return utils.get_response(True)


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = shell_utils.run_command("hostname -s")
SYSTEM_ID = ""
CLOUD_INFO = get_amazon_cloud_info()
if not CLOUD_INFO:
    CLOUD_INFO = get_google_cloud_info()

if not CLOUD_INFO:
    CLOUD_INFO = get_equinix_cloud_info()

if CLOUD_INFO:
    SYSTEM_ID = CLOUD_INFO["id"]
else:
    SYSTEM_ID, _, _ = shell_utils.run_command("dmidecode -s system-uuid")


class SPDKParams(BaseModel):
    server_ip: str = Field(pattern=utils.IP_PATTERN)
    rpc_port: int = Field(ge=1, lt=65536)
    rpc_username: str
    rpc_password: str
    ssd_pcie: List[str] = Field([])
    l_cores: str
    namespace: Optional[str]
    total_mem: Union[int, str] = Field('')
    spdk_mem: int = Field(core_utils.parse_size('64GiB'))
    system_mem: int = Field(core_utils.parse_size('4GiB'))
    fdb_connection: str = Field('')
    spdk_image: str = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    cluster_ip: str = Field(pattern=utils.IP_PATTERN)


@api.post('/spdk_process_start', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_start(body: SPDKParams):
    ssd_pcie_params = " ".join(" -A " + addr for addr in body.ssd_pcie) if body.ssd_pcie else "none"
    ssd_pcie_list = " ".join(body.ssd_pcie)

    namespace = node_utils_k8s.get_namespace()
    if body.namespace is not None:
        namespace = body.namespace
        set_namespace(namespace)

    total_mem_mib = core_utils.convert_size(core_utils.parse_size(body.total_mem), 'MB') if body.total_mem else ""

    if _is_pod_up():
        logger.info("SPDK deployment found, removing...")
        spdk_process_kill()

    node_name = os.environ.get("HOSTNAME")
    logger.debug(f"deploying caching node spdk on worker: {node_name}")

    try:
        env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('storage_deploy_spdk.yaml.j2')
        values = {
            'SPDK_IMAGE': body.spdk_image,
            "L_CORES": body.l_cores,
            'SPDK_MEM': core_utils.convert_size(body.spdk_mem, 'MiB'),
            'MEM_GEGA': core_utils.convert_size(body.spdk_mem, 'GiB', round_up=True),
            'MEM2_GEGA': core_utils.convert_size(body.system_mem, 'GiB', round_up=True),
            'SERVER_IP': body.server_ip,
            'RPC_PORT': body.rpc_port,
            'RPC_USERNAME': body.rpc_username,
            'RPC_PASSWORD': body.rpc_password,
            'HOSTNAME': node_name,
            'NAMESPACE': namespace,
            'FDB_CONNECTION': body.fdb_connection,
            'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
            'GRAYLOG_SERVER_IP': body.cluster_ip,
            'SSD_PCIE': ssd_pcie_params,
            'PCI_ALLOWED': ssd_pcie_list,
            'TOTAL_HP': total_mem_mib
        }
        dep = yaml.safe_load(template.render(values))
        logger.debug(dep)
        k8s_core_v1 = core_utils.get_k8s_core_client()
        resp = k8s_core_v1.create_namespaced_pod(body=dep, namespace=namespace)
        msg = f"Pod created: '{resp.metadata.name}' in namespace '{namespace}"
        logger.info(msg)
    except:
        return utils.get_response(False, f"Pod failed:\n{traceback.format_exc()}")

    return utils.get_response(msg)


@api.get('/spdk_process_kill', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_kill():
    k8s_core_v1 = core_utils.get_k8s_core_client()
    try:
        namespace = node_utils_k8s.get_namespace()
        resp = k8s_core_v1.delete_namespaced_pod(deployment_name, namespace)
        retries = 10
        while retries > 0:
            resp = k8s_core_v1.list_namespaced_pod(namespace)
            found = False
            for pod in resp.items:
                if pod.metadata.name.startswith(pod_name):
                    found = True

            if found:
                logger.info("Container found, waiting...")
                retries -= 1
                time.sleep(3)
            else:
                break

    except ApiException as e:
        logger.info(e.body)

    return utils.get_response(True)


def _is_pod_up():
    k8s_core_v1 = node_utils_k8s.get_k8s_core_client()
    try:
        resp = k8s_core_v1.list_namespaced_pod(node_utils_k8s.get_namespace())
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                return pod.status.phase == "Running"
    except ApiException as e:
        logger.error(f"API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    return False


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_is_up():
    if _is_pod_up():
        return utils.get_response(True)
    else:
        return utils.get_response(False, "SPDK container is not running")


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


class _FirewallParams(BaseModel):
    port_id: int
    port_type: str
    action: str


@api.post('/firewall_set_port', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def firewall_set_port(body: _FirewallParams):
    k8s_core_v1 = node_utils_k8s.get_k8s_core_client()

    for pod in k8s_core_v1.list_namespaced_pod(node_utils_k8s.get_namespace()).items:
        if not pod.metadata.name.startswith(pod_name):
            continue

        ret = node_utils_k8s.firewall_port_k8s(
                body.port_id,
                body.port_type,
                body.action=="block",
                k8s_core_v1,
                node_utils_k8s.get_namespace(),
                pod.metadata.name,
                "spdk_container",
        )
        return utils.get_response(ret)

    return utils.get_response(False)


@api.get('/get_firewall', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'string'
    })}}},
})
def get_firewall():
    ret = node_utils_k8s.firewall_get_k8s()
    return utils.get_response(ret)


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
    huge_page_memory_dict = {}
    for node_config in nodes:
        numa = node_config["socket"]
        huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
    for numa, huge_page_memory in huge_page_memory_dict.items():
        num_pages = huge_page_memory // (2048 * 1024)
        core_utils.set_hugepages_if_needed(numa, num_pages)

    return utils.get_response(True)
