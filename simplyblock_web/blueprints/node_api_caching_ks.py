#!/usr/bin/env python
# encoding: utf-8
import logging
import math
import os
import time

import yaml
from flask import Blueprint
from flask import request
from kubernetes import client, config
from kubernetes.client import ApiException

from simplyblock_core import constants, utils as core_utils
from simplyblock_web import utils


logger = logging.getLogger(__name__)

bp = Blueprint("caching_node_k", __name__, url_prefix="/cnode")


namespace = 'default'
deployment_name = 'spdk-deployment'
pod_name = 'spdk-deployment'

config.load_incluster_config()
k8s_apps_v1 = client.AppsV1Api()
k8s_core_v1 = client.CoreV1Api()

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
spdk_deploy_yaml = os.path.join(TOP_DIR, 'static/deploy_spdk.yaml')


@bp.route('/spdk_process_start', methods=['POST'])
def spdk_process_start():
    data = request.get_json()

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']
    node_cpu_count = os.cpu_count()

    global namespace
    if 'namespace' in data:
        namespace = data['namespace']

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

    # with open(spdk_deploy_yaml, 'r') as f:
    #     dep = yaml.safe_load(f)

    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('caching_deploy_spdk.yaml.j2')
    values = {
        'SPDK_IMAGE': spdk_image,
        'SPDK_CPU_MASK': spdk_cpu_mask,
        'SPDK_MEM': core_utils.convert_size(spdk_mem, 'MiB'),
        'SERVER_IP': data['server_ip'],
        'RPC_PORT': data['rpc_port'],
        'RPC_USERNAME': data['rpc_username'],
        'RPC_PASSWORD': data['rpc_password'],
        'SIMPLYBLOCK_DOCKER_IMAGE': constants.SIMPLY_BLOCK_DOCKER_IMAGE,
    }
    dep = yaml.safe_load(template.render(values))
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
        False, f"Deployment create max retries reached")


@bp.route('/spdk_process_kill', methods=['GET'])
def spdk_process_kill():

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


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    resp = k8s_core_v1.list_namespaced_pod(namespace)
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            status = pod.status.phase
            if status == "Running":
                return utils.get_response(True)
            else:
                return utils.get_response(False, f"SPDK container status: {status}")
    return utils.get_response(False, "SPDK container not found")


@bp.route('/join_db', methods=['POST'])
def join_db():
    return utils.get_response(True)
