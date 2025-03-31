#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time
import traceback

import cpuinfo
import yaml
from kubernetes import client, config
from flask import Blueprint
from flask import request
from kubernetes.client import ApiException
from jinja2 import Environment, FileSystemLoader

from simplyblock_core import constants, shell_utils, utils as core_utils

from simplyblock_web import utils, node_utils

logger = logging.getLogger(__name__)

bp = Blueprint("caching_node_k", __name__, url_prefix="/cnode")


node_name = os.environ.get("HOSTNAME")
deployment_name = f"cnode-spdk-deployment-{node_name}"
default_namespace = 'default'
namespace_id_file = '/etc/simplyblock/namespace'
pod_name = 'cnode-spdk-deployment'


config.load_incluster_config()
k8s_apps_v1 = client.AppsV1Api()
k8s_core_v1 = client.CoreV1Api()

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


cpu_info = cpuinfo.get_cpu_info()
hostname, _, _ = node_utils.run_command("hostname -s")
system_id = ""
try:
    system_id, _, _ = node_utils.run_command("dmidecode -s system-uuid")
except:
    pass


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
            logger.error(traceback.format_exc())
            return False
    with open(namespace_id_file, "w+") as f:
        f.write(namespace)
    return True


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


@bp.route('/spdk_process_start', methods=['POST'])
def spdk_process_start():
    data = request.get_json()

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']
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

    spdk_mem = data.get('spdk_mem', core_utils.parse_size('64GiB'))

    spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE

    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']

    if _is_pod_up():
        logger.info("SPDK deployment found, removing...")
        spdk_process_kill()

    node_name = os.environ.get("HOSTNAME")
    logger.debug(f"deploying caching node spdk on worker: {node_name}")

    try:
        env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('caching_deploy_spdk.yaml.j2')
        values = {
            'SPDK_IMAGE': spdk_image,
            'SPDK_CPU_MASK': spdk_cpu_mask,
            'SPDK_MEM': core_utils.convert_size(spdk_mem, 'MiB'),
            'MEM_GEGA': core_utils.convert_size(spdk_mem, 'GiB'),
            'SERVER_IP': data['server_ip'],
            'RPC_PORT': data['rpc_port'],
            'RPC_USERNAME': data['rpc_username'],
            'RPC_PASSWORD': data['rpc_password'],
            'HOSTNAME': node_name,
            'NAMESPACE': namespace,
            'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
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
        retries = 20
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


@bp.route('/info', methods=['GET'])
def get_info():

    out = {
        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpu_info['count'],
        "cpu_hz": cpu_info['hz_advertised'][0] if 'hz_advertised' in cpu_info else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),

        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),

        "network_interface": node_utils.get_nics_data()
    }
    return utils.get_response(out)


@bp.route('/join_db', methods=['POST'])
def join_db():
    return utils.get_response(True)


@bp.route('/nvme_connect', methods=['POST'])
def connect_to_nvme():
    data = request.get_json()
    ip = data['ip']
    port = data['port']
    nqn = data['nqn']
    st = f"nvme connect --transport=tcp --traddr={ip} --trsvcid={port} --nqn={nqn}"
    logger.debug(st)
    out, err, ret_code = node_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    if ret_code == 0:
        return utils.get_response(True)
    else:
        return utils.get_response(ret_code, error=err)


@bp.route('/disconnect_device', methods=['POST'])
def disconnect_device():
    data = request.get_json()
    dev_path = data['dev_path']
    st = f"nvme disconnect --device={dev_path}"
    out, err, ret_code = node_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


@bp.route('/disconnect_nqn', methods=['POST'])
def disconnect_nqn():
    data = request.get_json()
    nqn = data['nqn']
    st = f"nvme disconnect --nqn={nqn}"
    out, err, ret_code = node_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


@bp.route('/disconnect_all', methods=['POST'])
def disconnect_all():
    st = "nvme disconnect-all"
    out, err, ret_code = node_utils.run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)




@bp.route('/make_gpt_partitions', methods=['POST'])
def make_gpt_partitions_for_nbd():
    nbd_device = '/dev/nbd0'
    jm_percent = 10

    try:
        data = request.get_json()
        nbd_device = data['nbd_device']
        jm_percent = data['jm_percent']
    except:
        pass

    cmd_list = [
        f"parted -fs {nbd_device} mklabel gpt",
        f"parted -f {nbd_device} mkpart journal \"0%\" \"{jm_percent}%\""
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}",
    ]

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
