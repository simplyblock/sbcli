#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time
import traceback
from typing import Optional

import cpuinfo
import yaml
from flask_openapi3 import APIBlueprint
from kubernetes.client import ApiException
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, Field

from simplyblock_core import constants, shell_utils, utils as core_utils
from simplyblock_web import utils, node_utils

logger = logging.getLogger(__name__)

api = APIBlueprint("caching_node_k", __name__, url_prefix="/cnode")


node_name = os.environ.get("HOSTNAME")
deployment_name = f"cnode-spdk-deployment-{node_name}"
default_namespace = 'default'
namespace_id_file = '/etc/simplyblock/namespace'
pod_name = 'cnode-spdk-deployment'

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


cpu_info = cpuinfo.get_cpu_info()
hostname, _, _ = shell_utils.run_command("hostname -s")
system_id = ""
try:
    system_id, _, _ = shell_utils.run_command("dmidecode -s system-uuid")
except Exception:
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
        except Exception:
            logger.error(traceback.format_exc())
            return False
    with open(namespace_id_file, "w+") as f:
        f.write(namespace)
    return True


@api.get('/scan_devices')
def scan_devices():
    return utils.get_response({
        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),
        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),
    })


class _SPDKParams(BaseModel):
    spdk_cpu_mask: Optional[str]
    namespace: Optional[str]
    spdk_mem: int = Field(core_utils.parse_size('64GiB'))
    server_ip: str = Field(pattern=utils.IP_PATTERN)
    rpc_port: int = Field(constants.RPC_HTTP_PROXY_PORT, ge=1, le=65536)
    rpc_username: str
    rpc_password: str
    spdk_image: str = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)


@api.post('/spdk_process_start')
def spdk_process_start(body: _SPDKParams):
    node_cpu_count = os.cpu_count() or 1

    namespace = get_namespace()
    if body.namespace is not None:
        set_namespace(body.namespace)

    if body.spdk_cpu_mask is not None:
        spdk_cpu_mask = body.spdk_cpu_mask
        requested_cpu_count = len(format(int(spdk_cpu_mask, 16), 'b'))
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    if _is_pod_up():
        logger.info("SPDK deployment found, removing...")
        spdk_process_kill()

    node_name = os.environ.get("HOSTNAME")
    logger.debug(f"deploying caching node spdk on worker: {node_name}")

    try:
        env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
        template = env.get_template('caching_deploy_spdk.yaml.j2')
        values = {
            'SPDK_IMAGE': body.spdk_image,
            'SPDK_CPU_MASK': spdk_cpu_mask,
            'SPDK_MEM': core_utils.convert_size(body.spdk_mem, 'MiB'),
            'MEM_GEGA': core_utils.convert_size(body.spdk_mem, 'GiB'),
            'SERVER_IP': body.server_ip,
            'RPC_PORT': body.rpc_port,
            'RPC_USERNAME': body.rpc_username,
            'RPC_PASSWORD': body.rpc_password,
            'HOSTNAME': node_name,
            'NAMESPACE': namespace,
            'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        }
        dep = yaml.safe_load(template.render(values))
        logger.debug(dep)
        k8s_apps_v1 = core_utils.get_k8s_apps_client()
        resp = k8s_apps_v1.create_namespaced_deployment(body=dep, namespace=namespace)
        msg = f"Deployment created: '{resp.metadata.name}' in namespace '{namespace}"
        logger.info(msg)
    except Exception:
        return utils.get_response(False, f"Deployment failed:\n{traceback.format_exc()}")

    return utils.get_response(msg)


@api.get('/spdk_process_kill')
def spdk_process_kill():
    k8s_core_v1 = core_utils.get_k8s_core_client()
    k8s_apps_v1 = core_utils.get_k8s_apps_client()
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
    k8s_core_v1 = core_utils.get_k8s_core_client()
    resp = k8s_core_v1.list_namespaced_pod(get_namespace())
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            status = pod.status.phase
            if status == "Running":
                return True
            else:
                return False
    return False


@api.get('/spdk_process_is_up')
def spdk_process_is_up():
    pod_is_up = _is_pod_up()
    return utils.get_response(pod_is_up, "SPDK container is not running" if not pod_is_up else None)


@api.get('/info')
def get_info():
    return utils.get_response({
        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpu_info['count'],
        "cpu_hz": cpu_info['hz_advertised'][0] if 'hz_advertised' in cpu_info else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils.get_nvme_devices(),
        "nvme_pcie_list": node_utils.get_nvme_pcie_list(),

        "spdk_devices": node_utils.get_spdk_devices(),
        "spdk_pcie_list": node_utils.get_spdk_pcie_list(),

        "network_interface": core_utils.get_nics_data()
    })


@api.post('/join_db')
def join_db():
    return utils.get_response(True)


@api.post('/nvme_connect')
def connect_to_nvme(body: utils.NVMEConnectParams):
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


@api.post('/disconnect')
def disconnect(body: utils.DisconnectParams):
    if body.nqn is not None:
        cmd = f'nvme disconnect --device={body.device_path}'
    elif body.device_path is not None:
        cmd = f'nvme disconnect --nqn={body.nqn}'
    elif body.all is not None:
        cmd = 'nvme disconnect-all'
    else:
        pass  # unreachable

    out, err, ret_code = shell_utils.run_command(cmd)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


class _GPTPartitionsParams(BaseModel):
    nbd_device: str = Field('/dev/nbd0')
    jm_percent: int = Field(10, ge=0, le=100)


@api.post('/make_gpt_partitions')
def make_gpt_partitions_for_nbd(body: _GPTPartitionsParams):
    cmd_list = [
        f"parted -fs {body.nbd_device} mklabel gpt",
        f"parted -f {body.nbd_device} mkpart journal \"0%\" \"{body.jm_percent}%\""
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}",
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


@api.post('/delete_dev_gpt_partitions')
def delete_gpt_partitions_for_dev(body: utils.DeviceParams):
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
