#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import math
import os
import time

import cpuinfo
import docker
from docker.types import LogConfig
from flask import Blueprint
from flask import request

from simplyblock_web import utils, node_utils

from simplyblock_core import scripts, constants, shell_utils
from ec2_metadata import ec2_metadata

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = node_utils.run_command("hostname -s")
SYSTEM_ID = ""
EC2_PUBLIC_IP = ""
EC2_MD = ""

try:
    SYSTEM_ID = ec2_metadata.instance_id
except:
    SYSTEM_ID, _, _ = node_utils.run_command("dmidecode -s system-uuid")

try:
    EC2_PUBLIC_IP = ec2_metadata.public_ipv4
except:
    pass

try:
    EC2_MD = ec2_metadata.instance_identity_document
except:
    pass


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in node_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = node_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


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
    try:
        data = request.get_json()
    except:
        data = {}

    set_debug = None
    if 'spdk_debug' in data and data['spdk_debug']:
        set_debug = data['spdk_debug']

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']

    spdk_mem = None
    if 'spdk_mem' in data:
        spdk_mem = data['spdk_mem']

    if spdk_mem:
        spdk_mem = int(utils.parse_size(spdk_mem) / (1000 * 1000))
    else:
        spdk_mem = 4000

    node_docker = get_docker_client()
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] in ["/spdk", "/spdk_proxy"]:
            logger.info(f"{node.attrs['Name']} container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    spdk_debug = 0
    if set_debug:
        spdk_debug = 1

    spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']
        # node_docker.images.pull(spdk_image)

    if "cluster_ip" in data and data['cluster_ip']:
        cluster_ip = data['cluster_ip']
        log_config = LogConfig(type=LogConfig.types.GELF, config={"gelf-address": f"udp://{cluster_ip}:12201"})
    else:
        log_config = LogConfig(type=LogConfig.types.JOURNALD)

    container = node_docker.containers.run(
        spdk_image,
        f"/root/scripts/run_distr.sh {spdk_cpu_mask} {spdk_mem} {spdk_debug}",
        name="spdk",
        detach=True,
        privileged=True,
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/etc/simplyblock:/etc/simplyblock',
            '/var/tmp:/var/tmp',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/var/lib/systemd/coredump/:/var/lib/systemd/coredump/',
            '/sys:/sys'],
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )
    container2 = node_docker.containers.run(
        constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE,
        "python /root/scripts/spdk_http_proxy.py",
        name="spdk_proxy",
        detach=True,
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/var/tmp:/var/tmp',
            '/etc/foundationdb:/etc/foundationdb'],
        restart_policy={"Name": "always"}
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


@bp.route('/spdk_process_kill', methods=['GET'])
def spdk_process_kill():
    force = request.args.get('force', default=False, type=bool)
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        logger.debug(cont.attrs)
        if cont.attrs['Name'] == "/spdk" or cont.attrs['Name'] == "/spdk_proxy":
            cont.stop()
            cont.remove(force=force)
    return utils.get_response(True)


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        logger.debug(cont.attrs)
        if cont.attrs['Name'] == "/spdk":
            status = cont.attrs['State']["Status"]
            is_running = cont.attrs['State']["Running"]
            if is_running:
                return utils.get_response(True)
            else:
                return utils.get_response(False, f"SPDK container status: {status}, is running: {is_running}")
    return utils.get_response(False, "SPDK container not found")


def get_cluster_id():
    out, _, _ = node_utils.run_command(f"cat {cluster_id_file}")
    return out


def set_cluster_id(cluster_id):
    out, _, _ = node_utils.run_command(f"echo {cluster_id} > {cluster_id_file}")
    return out


def delete_cluster_id():
    out, _, _ = node_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_ec2_meta():
    stream = os.popen('curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 1"')
    token = stream.read()
    stream = os.popen(f"curl -H \"X-aws-ec2-metadata-token: {token}\" http://169.254.169.254/latest/dynamic/instance-identity/document")
    out = stream.read()
    try:
        data = json.loads(out)
        return data
    except:
        return {}


def get_ec2_public_ip():
    stream = os.popen('curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 1"')
    token = stream.read()
    stream = os.popen(f"curl -H \"X-aws-ec2-metadata-token: {token}\" http://169.254.169.254/latest/meta-data/public-ipv4")
    response = stream.read()
    out = response if "404" not in response else None
    return out


@bp.route('/info', methods=['GET'])
def get_info():

    out = {
        "cluster_id": get_cluster_id(),

        "hostname": HOSTNAME,
        "system_id": SYSTEM_ID,

        "cpu_count": CPU_INFO['count'],
        "cpu_hz": CPU_INFO['hz_advertised'][0],

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),

        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),

        "network_interface": node_utils.get_nics_data(),

        "ec2_metadata": EC2_MD,

        "ec2_public_ip": EC2_PUBLIC_IP,
    }
    return utils.get_response(out)


@bp.route('/join_swarm', methods=['POST'])
def join_swarm():
    data = request.get_json()
    cluster_ip = data['cluster_ip']
    cluster_id = data['cluster_id']
    join_token = data['join_token']
    db_connection = data['db_connection']

    logger.info("Setting DB connection")
    scripts.set_db_config(db_connection)
    set_cluster_id(cluster_id)

    logger.info("Joining Swarm")
    node_docker = get_docker_client()
    if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
        logger.info("Node is part of another swarm, leaving swarm")
        node_docker.swarm.leave(force=True)
        time.sleep(2)
    node_docker.swarm.join([f"{cluster_ip}:2377"], join_token)
    retries = 10
    while retries > 0:
        if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
            break
        logger.info("Waiting for node to be active...")
        retries -= 1
        time.sleep(1)
    logger.info("Joining docker swarm > Done")

    try:
        nodes = node_docker.containers.list(all=True)
        for node in nodes:
            if node.attrs["Name"] == "/spdk_proxy":
                node_docker.containers.get(node.attrs["Id"]).restart()
                break
    except:
        pass

    return utils.get_response(True)


@bp.route('/leave_swarm', methods=['GET'])
def leave_swarm():
    delete_cluster_id()
    try:
        node_docker = get_docker_client()
        node_docker.swarm.leave(force=True)
    except:
        pass
    return utils.get_response(True)


@bp.route('/make_gpt_partitions', methods=['POST'])
def make_gpt_partitions_for_nbd():
    nbd_device = '/dev/nbd0'
    jm_percent = '3'
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
