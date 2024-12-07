#!/usr/bin/env python
# encoding: utf-8
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

from simplyblock_web import utils, node_utils
from simplyblock_core import scripts, constants, shell_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
bp = Blueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

node_name = os.environ.get("HOSTNAME")
deployment_name = f"snode-spdk-deployment-{node_name}"
default_namespace = 'default'
namespace_id_file = '/etc/simplyblock/namespace'
pod_name = deployment_name[:50]


config.load_incluster_config()
k8s_apps_v1 = client.AppsV1Api()
k8s_core_v1 = client.CoreV1Api()

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
# spdk_deploy_yaml = os.path.join(TOP_DIR, 'static/deploy_spdk.yaml')


def get_namespace():
    if os.path.exists(namespace_id_file):
        with open(namespace_id_file, 'r') as f:
            out = f.read()
            return out
    return default_namespace


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
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true", headers=headers)
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
        response = requests.get("https://metadata.platformequinix.com/metadata")
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
        from ec2_metadata import ec2_metadata
        data = ec2_metadata.instance_identity_document
        return {
            "id": data["instanceId"],
            "type": data["instanceType"],
            "cloud": "amazon",
            "ip": data["privateIp"],
            "public_ip": ec2_metadata.public_ipv4 or "",
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
    }
    return utils.get_response(out)


@bp.route('/join_swarm', methods=['POST'])
def join_swarm():
    # data = request.get_json()
    # cluster_ip = data['cluster_ip']
    # cluster_id = data['cluster_id']
    # join_token = data['join_token']
    # db_connection = data['db_connection']
    #
    # logger.info("Setting DB connection")
    # scripts.set_db_config(db_connection)
    # set_cluster_id(cluster_id)
    #
    # logger.info("Joining Swarm")
    # node_docker = get_docker_client()
    # if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
    #     logger.info("Node is part of another swarm, leaving swarm")
    #     node_docker.swarm.leave(force=True)
    #     time.sleep(2)
    # node_docker.swarm.join([f"{cluster_ip}:2377"], join_token)
    # retries = 10
    # while retries > 0:
    #     if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
    #         break
    #     logger.info("Waiting for node to be active...")
    #     retries -= 1
    #     time.sleep(1)
    # logger.info("Joining docker swarm > Done")
    #
    # try:
    #     nodes = node_docker.containers.list(all=True)
    #     for node in nodes:
    #         if node.attrs["Name"] == "/spdk_proxy":
    #             node_docker.containers.get(node.attrs["Id"]).restart()
    #             break
    # except:
    #     pass

    return utils.get_response(True)


@bp.route('/leave_swarm', methods=['GET'])
def leave_swarm():
    # delete_cluster_id()
    # try:
    #     node_docker = get_docker_client()
    #     node_docker.swarm.leave(force=True)
    # except:
    #     pass
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
    spdk_mem = None
    if 'spdk_mem' in data:
        spdk_mem = data['spdk_mem']
    node_cpu_count = os.cpu_count()

    namespace = get_namespace()
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

    if spdk_mem:
        spdk_mem = int(spdk_mem / (1024 * 1024))
    else:
        spdk_mem = 64096

    spdk_mem_gega = int(spdk_mem / 1024)

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
            'SPDK_MEM': spdk_mem,
            'MEM_GEGA': spdk_mem_gega,
            'MEM2_GEGA': spdk_mem_gega+4,
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
        namespace = get_namespace()
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
    resp = k8s_core_v1.list_namespaced_pod(get_namespace())
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            status = pod.status.phase
            if status == "Running":
                return True
            else:
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
