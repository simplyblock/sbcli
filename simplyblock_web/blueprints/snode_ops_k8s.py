#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import math
import os
import time
import traceback

import cpuinfo
import requests
from flask import Blueprint
from flask import request
from kubernetes import client, config
from kubernetes.client import ApiException
from jinja2 import Environment, FileSystemLoader
import yaml

from simplyblock_web import utils, node_utils, node_utils_k8s
from simplyblock_core import scripts, constants, shell_utils, utils as core_utils
from simplyblock_web.node_utils_k8s import deployment_name, namespace_id_file, pod_name

logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)
bp = Blueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

config.load_incluster_config()
k8s_apps_v1 = client.AppsV1Api()
k8s_core_v1 = client.CoreV1Api()

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


@bp.route('/scan_devices', methods=['GET'])
def scan_devices():
    run_health_check = request.args.get('run_health_check', default=False, type=bool)
    out = {
        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),
        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),
    }
    return utils.get_response(out)


def get_cluster_id():
    out, _, _ = node_utils.run_command(f"cat {cluster_id_file}")
    return out


def set_cluster_id(cluster_id):
    out, _, _ = node_utils.run_command(f"echo {cluster_id} > {cluster_id_file}")
    return out


def delete_cluster_id():
    out, _, _ = node_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_cores_config(cpu_count):
    spdk_cpu_mask = hex(int(math.pow(2, cpu_count)) - 2)
    cores_config = {
        "cpu_mask": spdk_cpu_mask
    }
    return cores_config


@bp.route('/info', methods=['GET'])
def get_info():

    out = {
        "cluster_id": get_cluster_id(),

        "hostname": HOSTNAME,
        "system_id": SYSTEM_ID,

        "cpu_count": CPU_INFO['count'],
        "cpu_hz": CPU_INFO['hz_advertised'][0] if 'hz_advertised' in CPU_INFO else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),

        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),

        "network_interface": node_utils.get_nics_data(),

        "cloud_instance": CLOUD_INFO,
        "cores_config": get_cores_config(CPU_INFO['count']),
    }
    return utils.get_response(out)


@bp.route('/join_swarm', methods=['POST'])
def join_swarm():
    return utils.get_response(True)


@bp.route('/leave_swarm', methods=['GET'])
def leave_swarm():
    return utils.get_response(True)


@bp.route('/make_gpt_partitions', methods=['POST'])
def make_gpt_partitions_for_nbd():
    nbd_device = '/dev/nbd0'
    jm_percent = 3
    num_partitions = 1

    try:
        data = request.get_json()
        nbd_device = data['nbd_device']
        jm_percent = data['jm_percent']
        num_partitions = data['num_partitions']
    except:
        pass

    cmd_list = [
        f"parted -fs {nbd_device} mklabel gpt",
        f"parted -f {nbd_device} mkpart journal \"0%\" \"{jm_percent}%\""
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}",
    ]
    perc_per_partition = int((100 - jm_percent) / num_partitions)
    for i in range(num_partitions):
        st = jm_percent + (i * perc_per_partition)
        en = st + perc_per_partition
        cmd_list.append(f"parted -f {nbd_device} mkpart part{(i+1)} \"{st}%\" \"{en}%\"")
        sg_cmd_list.append(f"sgdisk -t {(i+2)}:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}")

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


@bp.route('/delete_dev_gpt_partitions', methods=['POST'])
def delete_gpt_partitions_for_dev():

    data = request.get_json()

    if "device_pci" not in data:
        return utils.get_response(False, "Required parameter is missing: device_pci")

    device_pci = data['device_pci']

    cmd_list = [
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/uio_pci_generic/unbind",
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/nvme/bind",
    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    device_name = os.popen(f"ls /sys/devices/pci0000:00/{device_pci}/nvme/nvme*/ | grep nvme").read().strip()
    cmd_list = [
        f"parted -fs /dev/{device_name} mklabel gpt",
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/nvme/unbind",
    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    return utils.get_response(True)


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = node_utils.run_command("hostname -s")
SYSTEM_ID = ""
CLOUD_INFO = get_amazon_cloud_info()
if not CLOUD_INFO:
    CLOUD_INFO = get_google_cloud_info()

if not CLOUD_INFO:
    CLOUD_INFO = get_equinix_cloud_info()

if CLOUD_INFO:
    SYSTEM_ID = CLOUD_INFO["id"]
else:
    SYSTEM_ID, _, _ = node_utils.run_command("dmidecode -s system-uuid")


@bp.route('/spdk_process_start', methods=['POST'])
def spdk_process_start():
    data = request.get_json()

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']
    node_cpu_count = os.cpu_count()

    namespace = node_utils_k8s.get_namespace()
    if 'namespace' in data:
        namespace = data['namespace']
        set_namespace(namespace)

    if spdk_cpu_mask:
        requested_cpu_count = len(format(int(spdk_cpu_mask, 16), 'b'))
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    spdk_mem = data.get('spdk_mem', core_utils.parse_size('64GiB'))

    spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
    # if node_utils.get_host_arch() == "aarch64":
    #     spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE_ARM64

    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']

    fdb_connection = ""
    if 'fdb_connection' in data and data['fdb_connection']:
        fdb_connection = data['fdb_connection']

    if _is_pod_up():
        logger.info("SPDK deployment found, removing...")
        spdk_process_kill()

    node_name = os.environ.get("HOSTNAME")
    logger.debug(f"deploying caching node spdk on worker: {node_name}")

    try:
        env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('storage_deploy_spdk.yaml.j2')
        values = {
            'SPDK_IMAGE': spdk_image,
            'SPDK_CPU_MASK': spdk_cpu_mask,
            'SPDK_MEM': core_utils.convert_size(spdk_mem, 'MiB'),
            'MEM_GEGA': core_utils.convert_size(spdk_mem, 'GiB'),
            'MEM2_GEGA': 2,
            'SERVER_IP': data['server_ip'],
            'RPC_PORT': data['rpc_port'],
            'RPC_USERNAME': data['rpc_username'],
            'RPC_PASSWORD': data['rpc_password'],
            'HOSTNAME': node_name,
            'NAMESPACE': namespace,
            'FDB_CONNECTION': fdb_connection,
            'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
            'GRAYLOG_SERVER_IP': data['cluster_ip']
        }
        dep = yaml.safe_load(template.render(values))
        logger.debug(dep)
        resp = k8s_apps_v1.create_namespaced_deployment(body=dep, namespace=namespace)
        msg = f"Deployment created: '{resp.metadata.name}' in namespace '{namespace}"
        logger.info(msg)
    except:
        return utils.get_response(False, f"Deployment failed:\n{traceback.format_exc()}")

    return utils.get_response(msg)


@bp.route('/spdk_process_kill', methods=['GET'])
def spdk_process_kill():

    try:
        namespace = node_utils_k8s.get_namespace()
        resp = k8s_apps_v1.delete_namespaced_deployment(deployment_name, namespace)
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


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    if _is_pod_up():
        return utils.get_response(True)
    else:
        return utils.get_response(False, f"SPDK container is not running")


@bp.route('/get_file_content/<string:file_name>', methods=['GET'])
def get_file_content(file_name):
    out, err, _ = node_utils.run_command(f"cat /etc/simplyblock/{file_name}")
    if out:
        return utils.get_response(out)
    elif err:
        err = err.decode("utf-8")
        logger.debug(err)
        return utils.get_response(None, err)


@bp.route('/firewall_set_port', methods=['POST'])
def firewall_set_port():
    data = request.get_json()
    if "port_id" not in data:
        return utils.get_response(False, "Required parameter is missing: port_id")
    if "port_type" not in data:
        return utils.get_response(False, "Required parameter is missing: port_type")
    if "action" not in data:
        return utils.get_response(False, "Required parameter is missing: action")

    port_id = data['port_id']
    port_type = data['port_type']
    action = data['action']
    container = "spdk-container"

    resp = k8s_core_v1.list_namespaced_pod(node_utils_k8s.get_namespace())
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            ret = node_utils_k8s.firewall_port_k8s(port_id, port_type, action=="block", k8s_core_v1, node_utils_k8s.get_namespace(), pod.metadata.name, container)
            return utils.get_response(ret)
    return utils.get_response(False)


@bp.route('/get_firewall', methods=['GET'])
def get_firewall():
    ret = node_utils_k8s.firewall_get_k8s()
    return utils.get_response(ret)
