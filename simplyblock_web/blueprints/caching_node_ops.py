#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time
from typing import Optional

import cpuinfo
import docker
from docker.types import LogConfig
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field, validator

from simplyblock_core import scripts, constants, shell_utils, utils as core_utils
from simplyblock_web import utils, node_utils

logger = logging.getLogger(__name__)

api = APIBlueprint("caching_node", __name__, url_prefix="/cnode")


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in core_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = core_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


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
    spdk_mem: int = Field(core_utils.parse_size('64GiB'))
    server_ip: str = Field(pattern=utils.IP_PATTERN)
    rpc_port: int = Field(constants.RPC_HTTP_PROXY_PORT, ge=1, le=65536)
    rpc_username: str
    rpc_password: str
    rpc_sock: str
    spdk_image: Optional[str] = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)

    @validator('rpc_sock', pre=True, always=True)
    def set_rpc_sock_default(cls, v, values):
        return f"/var/tmp/spdk_{values.get('rpc_port')}"


@api.post('/spdk_process_start')
def spdk_process_start(body: _SPDKParams):
    node_cpu_count = os.cpu_count() or 1

    if body.spdk_cpu_mask:
        spdk_cpu_mask = body.spdk_cpu_mask
        requested_cpu_count = len(format(int(spdk_cpu_mask, 16), 'b'))
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    node_docker = get_docker_client()

    for name in {f"/spdk_{body.rpc_port}", f"/spdk_proxy_{body.rpc_port}"}:
        core_utils.remove_container(node_docker, name)

    spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE

    if 'spdk_image' in body.model_fields_set:
        core_utils.pull_docker_image_with_retry(node_docker, body.spdk_image)

    container = node_docker.containers.run(
        spdk_image,
        f"/root/scripts/run_spdk_tgt.sh {spdk_cpu_mask} {core_utils.convert_size(body.spdk_mem, 'MiB')} {body.rpc_sock}",
        name=f"spdk_{body.rpc_port}",
        detach=True,
        privileged=True,
        # network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/var/lib/systemd/coredump/:/var/lib/systemd/coredump/',
            '/sys:/sys'],
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )

    node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_core/services/spdk_http_proxy_server.py",
        name=f"spdk_proxy_{body.rpc_port}",
        detach=True,
        network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/etc/foundationdb:/etc/foundationdb'],
        environment=[
            f"SERVER_IP={body.server_ip}",
            f"RPC_PORT={body.rpc_port}",
            f"RPC_USERNAME={body.rpc_username}",
            f"RPC_PASSWORD={body.rpc_password}",
            f"RPC_SOCK={body.rpc_sock}",
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


class _SPDKProcessKillParams(utils.RPCPortParams):
    force: bool = Field(False)


@api.get('/spdk_process_kill')
def spdk_process_kill(body: _SPDKProcessKillParams):
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        if cont.attrs['Name'] == f"/spdk_{body.rpc_port}" or cont.attrs['Name'] == f"/spdk_proxy_{body.rpc_port}":
            cont.stop()
            cont.remove(force=body.force)
    return utils.get_response(True)


@api.get('/spdk_process_is_up')
def spdk_process_is_up(body: utils.RPCPortParams):
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        if cont.attrs['Name'] == f"/spdk_{body.rpc_port}":
            status = cont.attrs['State']["Status"]
            is_running = cont.attrs['State']["Running"]
            if is_running:
                return utils.get_response(cont.attrs)
            else:
                return utils.get_response(False, f"SPDK container status: {status}, is running: {is_running}")
    return utils.get_response(False, "SPDK container not found")


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = shell_utils.run_command("hostname -s")
SYSTEM_ID, _, _ = shell_utils.run_command("dmidecode -s system-uuid")


@api.get('/info')
def get_info():
    return utils.get_response({
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

        "network_interface": core_utils.get_nics_data()
    })


class _JoinDBParams(utils.RPCPortParams):
    db_connection: str


@api.post('/join_db')
def join_db(body: _JoinDBParams):
    logger.info("Setting DB connection")
    scripts.set_db_config(body.db_connection)

    try:
        node_docker = get_docker_client()
        nodes = node_docker.containers.list(all=True)
        for node in nodes:
            if node.attrs["Name"] == f"/spdk_proxy_{body.rpc_port}":
                node_docker.containers.get(node.attrs["Id"]).restart()
                break
    except Exception:
        pass
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
        f"parted -f {body.nbd_device} mkpart journal \"0%\" \"{body.jm_percent}%\"",
        f"parted -f {body.nbd_device} mkpart part \"{body.jm_percent}%\" \"100%\" ",
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}",
        f"sgdisk -t 2:6527994e-2c5a-4eec-9613-8f5944074e8b {body.nbd_device}",
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
