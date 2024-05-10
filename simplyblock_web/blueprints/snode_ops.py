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

from simplyblock_core import scripts, constants

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

cpu_info = cpuinfo.get_cpu_info()
hostname, _, _ = node_utils.run_command("hostname -s")
system_id = ""
try:
    system_id, _, _ = node_utils.run_command("dmidecode -s system-uuid")
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
    node_cpu_count = os.cpu_count()

    if spdk_cpu_mask:
        try:
            requested_cpu_count = len(format(int(spdk_cpu_mask, 16), 'b'))
            if requested_cpu_count > node_cpu_count:
                return utils.get_response(
                    False,
                    f"The requested cpu count: {requested_cpu_count} "
                    f"is larger than the node's cpu count: {node_cpu_count}")
        except:
            spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    if spdk_mem:
        spdk_mem = int(utils.parse_size(spdk_mem) / (1000 * 1000))
    else:
        spdk_mem = 4000

    node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
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
    node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
    for cont in node_docker.containers.list(all=True):
        logger.debug(cont.attrs)
        if cont.attrs['Name'] == "/spdk" or cont.attrs['Name'] == "/spdk_proxy":
            cont.stop()
            cont.remove(force=force)
    return utils.get_response(True)


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
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

        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpu_info['count'],
        "cpu_hz": cpu_info['hz_advertised'][0],

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),

        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),

        "network_interface": node_utils.get_nics_data(),

        "ec2_metadata": get_ec2_meta(),

        "ec2_public_ip": get_ec2_public_ip(),
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
    node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
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
        node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
        node_docker.swarm.leave(force=True)
    except:
        pass
    return utils.get_response(True)
