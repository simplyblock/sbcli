#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time
from typing import Annotated, Optional

from pydantic import BaseModel, Field
import docker
from docker.types import LogConfig
from flask_openapi3 import APIBlueprint

from simplyblock_web import utils, node_utils
from simplyblock_core import scripts, constants, utils as core_utils
from simplyblock_core.utils import pull_docker_image_with_retry

logger = logging.getLogger(__name__)

api = APIBlueprint("node_api_caching_docker", __name__, url_prefix="/cnode")


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in core_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = core_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


class SPDKParams(BaseModel):
    server_ip: Annotated[str, Field(default=None, pattern=utils.IP_PATTERN)]
    rpc_port: Annotated[int, Field(constants.RPC_HTTP_PROXY_PORT, ge=1, le=65536)]
    rpc_username: str
    rpc_password: str
    spdk_cpu_mask: Optional[Annotated[str, Field(None, pattern=r'^0x[0-9a-zA-Z]+$')]]
    spdk_mem: Optional[Annotated[int, Field(core_utils.parse_size('64GiB'))]]
    spdk_image: Optional[str] = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)


@api.post('/spdk_process_start', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_start(body: SPDKParams):
    node_cpu_count = os.cpu_count()

    if body.spdk_cpu_mask is not None:
        spdk_cpu_mask = body.spdk_cpu_mask
        requested_cpu_count = int(spdk_cpu_mask, 16).bit_length()
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    spdk_mem_mib = core_utils.convert_size(body.spdk_mem, 'MiB')

    node_docker = get_docker_client()
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] in ["/spdk", "/spdk_proxy"]:
            logger.info(f"{node.attrs['Name']} container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    pull_docker_image_with_retry(node_docker, body.spdk_image)

    container = node_docker.containers.run(
        body.spdk_image,
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
            f"SERVER_IP={body.server_ip}",
            f"RPC_PORT={body.rpc_port}",
            f"RPC_USERNAME={body.rpc_username}",
            f"RPC_PASSWORD={body.rpc_password}",
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


class _SPDKKillQuery(BaseModel):
    force: Optional[Annotated[bool, Field(False)]]


@api.get('/spdk_process_kill', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_kill(query: _SPDKKillQuery):
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        logger.debug(cont.attrs)
        if cont.attrs['Name'] == "/spdk" or cont.attrs['Name'] == "/spdk_proxy":
            cont.stop()
            cont.remove(force=query.force)
    return utils.get_response(True)


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
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


class _DBConnectionParams(BaseModel):
    db_connection: str


@api.post('/join_db', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def join_db(body: _DBConnectionParams):
    logger.info("Setting DB connection")
    scripts.set_db_config(body.db_connection)

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
