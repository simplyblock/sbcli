#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time
from typing import Annotated, Optional

from pydantic import BaseModel, Field
import yaml
from flask_openapi3 import APIBlueprint
from kubernetes.client import ApiException

from simplyblock_core import constants, utils as core_utils
from simplyblock_web import utils


logger = logging.getLogger(__name__)

api = APIBlueprint("caching_node_k", __name__, url_prefix="/cnode")


namespace = 'default'
deployment_name = 'spdk-deployment'
pod_name = 'spdk-deployment'


TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
spdk_deploy_yaml = os.path.join(TOP_DIR, 'static/deploy_spdk.yaml')


class SPDKParams(BaseModel):
    server_ip: Annotated[str, Field(default=None, pattern=utils.IP_PATTERN)]
    rpc_port: Annotated[int, Field(constants.RPC_HTTP_PROXY_PORT, ge=1, le=65536)]
    rpc_username: str
    rpc_password: str
    spdk_cpu_mask: Optional[Annotated[str, Field(None, pattern=r'^0x[0-9a-zA-Z]+$')]]
    spdk_mem: Optional[Annotated[int, Field(core_utils.parse_size('64GiB'))]]
    spdk_image: Optional[str] = Field(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    namespace: Optional[Annotated[str, Field(None)]]
    socket: Optional[str] = '0'


@api.post('/spdk_process_start', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_start(body: SPDKParams):
    node_cpu_count = os.cpu_count()

    global namespace
    if body.namespace is not None:
        namespace = body.namespace

    if body.spdk_cpu_mask is not None:
        requested_cpu_count = int(body.spdk_cpu_mask, 16).bit_length()
        if requested_cpu_count > node_cpu_count:
            return utils.get_response(
                False,
                f"The requested cpu count: {requested_cpu_count} "
                f"is larger than the node's cpu count: {node_cpu_count}")
    else:
        spdk_cpu_mask = hex(int(math.pow(2, node_cpu_count)) - 1)

    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('caching_deploy_spdk.yaml.j2')
    values = {
        'SPDK_IMAGE': body.spdk_image,
        'SPDK_CPU_MASK': spdk_cpu_mask,
        'SPDK_MEM': core_utils.convert_size(body.spdk_mem, 'MiB'),
        'SERVER_IP': body.server_ip,
        'RPC_PORT': body.rpc_port,
        'RPC_USERNAME': body.rpc_username,
        'RPC_PASSWORD': body.rpc_password,
        'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
    }
    dep = yaml.safe_load(template.render(values))

    k8s_apps_v1 = core_utils.get_k8s_apps_client()
    k8s_core_v1 = core_utils.get_k8s_core_client()

    resp = k8s_apps_v1.create_namespaced_deployment(body=dep, namespace=namespace)
    logger.info(f"Deployment created: '{resp.metadata.name}'")

    retries = 20
    while retries > 0:
        resp = k8s_core_v1.list_namespaced_pod(namespace)
        new_pod = None
        for pod in resp.items:
            if pod.metadata.name.startswith(pod_name):
                new_pod = pod
                break

        if not new_pod:
            logger.info("Container is not running, waiting...")
            time.sleep(3)
            retries -= 1
            continue

        status = new_pod.status.phase
        logger.info(f"pod: {pod_name} status: {status}")

        if status == "Running":
            logger.info(f"Container status: {status}")
            return utils.get_response(True)
        else:
            logger.info("Container is not running, waiting...")
            time.sleep(3)
            retries -= 1

    return utils.get_response(
        False, "Deployment create max retries reached")


@api.get('/spdk_process_kill', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_kill():
    k8s_core_v1 = core_utils.get_k8s_core_client()
    k8s_apps_v1 = core_utils.get_k8s_apps_client()

    try:
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


@api.get('/spdk_process_is_up', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def spdk_process_is_up():
    k8s_core_v1 = core_utils.get_k8s_core_client()
    resp = k8s_core_v1.list_namespaced_pod(namespace)
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            status = pod.status.phase
            if status == "Running":
                return utils.get_response(True)
            else:
                return utils.get_response(False, f"SPDK container status: {status}")
    return utils.get_response(False, "SPDK container not found")


@api.post('/join_db', responses={
    200: {'content': {'application/json': {'schema': utils.response_schema({
        'type': 'boolean'
    })}}},
})
def join_db():
    return utils.get_response(True)
