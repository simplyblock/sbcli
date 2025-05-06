#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time

import docker
from docker.types import LogConfig
from flask import Blueprint
from flask import request

from simplyblock_web import utils, node_utils
from simplyblock_core import scripts, constants, utils as core_utils
from simplyblock_core.utils import pull_docker_image_with_retry

logger = logging.getLogger(__name__)

bp = Blueprint("node_api_caching_docker", __name__, url_prefix="/cnode")


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in node_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = node_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


@bp.route('/spdk_process_start', methods=['POST'])
def spdk_process_start():
    data = request.get_json()

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']
    node_cpu_count = os.cpu_count()

    if spdk_cpu_mask:
        requested_cpu_count = len(format(int(spdk_cpu_mask, 16), 'b'))
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    spdk_mem_mib = core_utils.convert_size(
            data.get('spdk_mem', core_utils.parse_size('64GiB')), 'MiB')

    node_docker = get_docker_client()
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] in ["/spdk", "/spdk_proxy"]:
            logger.info(f"{node.attrs['Name']} container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE
    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']
        pull_docker_image_with_retry(node_docker, spdk_image)

    container = node_docker.containers.run(
        spdk_image,
        f"/root/spdk/scripts/run_spdk_tgt.sh {spdk_cpu_mask} {spdk_mem_mib}",
        name="spdk",
        detach=True,
        privileged=True,
        network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/dev/hugepages:/mnt/huge',
            '/sys:/sys'],
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )

    server_ip = data['server_ip']
    rpc_port = data['rpc_port']
    rpc_username = data['rpc_username']
    rpc_password = data['rpc_password']

    container2 = node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_core/services/spdk_http_proxy_server.py",
        name="spdk_proxy",
        detach=True,
        network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/etc/foundationdb:/etc/foundationdb'],
        environment=[
            f"SERVER_IP={server_ip}",
            f"RPC_PORT={rpc_port}",
            f"RPC_USERNAME={rpc_username}",
            f"RPC_PASSWORD={rpc_password}",
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


@bp.route('/join_db', methods=['POST'])
def join_db():
    data = request.get_json()
    db_connection = data['db_connection']

    logger.info("Setting DB connection")
    ret = scripts.set_db_config(db_connection)

    try:
        node_docker = get_docker_client()
        nodes = node_docker.containers.list(all=True)
        for node in nodes:
            if node.attrs["Name"] == "/spdk_proxy":
                node_docker.containers.get(node.attrs["Id"]).restart()
                break
    except:
        pass
    return utils.get_response(True)
