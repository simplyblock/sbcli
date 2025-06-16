#!/usr/bin/env python
# encoding: utf-8
import logging
import os

import jc
from kubernetes.stream import stream
from simplyblock_core.utils import get_k8s_core_client
from concurrent.futures import ThreadPoolExecutor


node_name = os.environ.get("HOSTNAME")
deployment_name = f"snode-spdk-deployment-{node_name}"
pod_name = deployment_name[:50]
namespace_id_file = '/etc/simplyblock/namespace'
default_namespace = 'default'

logger = logging.getLogger(__name__)


def firewall_port_k8s(port_id=9090, port_type="tcp", block=True, k8s_core_v1=None, namespace=None, pod_name=None, container=None):
    cmd_list = []
    try:
        iptables_command_output = firewall_get_k8s()
        result = jc.parse('iptables', iptables_command_output)
        for chain in result:
            if chain['chain'] in ["INPUT", "OUTPUT"]:
                for rule in chain['rules']:
                    if str(port_id) in rule['options']:
                        cmd_list.append(f"iptables -D {chain['chain']} -p {port_type} --dport {port_id} -j {rule['target']}")

    except Exception as e:
        logger.error(e)

    if block:
        cmd_list.extend([
            f"iptables -A INPUT -p {port_type} --dport {port_id} -j DROP",
            f"iptables -A OUTPUT -p {port_type} --dport {port_id} -j DROP",
            "iptables -L -n -v",
        ])
    else:
        cmd_list.extend([
            "iptables -L -n -v",
        ])

    for cmd in cmd_list:
        try:
            ret = pod_exec(pod_name, namespace, container, cmd, k8s_core_v1)
            logger.info(ret)
        except Exception as e:
            logger.error(e)
            return False

    return True


def execute_on_pod(pod, k8s_core_v1):
    container = "spdk-container"
    return pod_exec(pod.metadata.name, get_namespace(), container, "iptables -L -n", k8s_core_v1)

def firewall_get_k8s():
    k8s_core_v1 = get_k8s_core_client()
    label_selector = "role=simplyblock-storage-node"
    resp = k8s_core_v1.list_namespaced_pod(namespace=get_namespace(), label_selector=label_selector)
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda pod: execute_on_pod(pod, k8s_core_v1), resp.items))
    
    return results


def pod_exec(name, namespace, container, command, k8s_core_v1):
    exec_command = ["/bin/sh", "-c", command]
    stdout_result, stderr_result = "", ""

    try:
        resp = stream(
            k8s_core_v1.connect_get_namespaced_pod_exec,
            name,
            namespace,
            command=exec_command,
            container=container,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            _preload_content=False
        )

        while resp.is_open():
            resp.update(timeout=5)
            if resp.peek_stdout():
                stdout_result += resp.read_stdout()
            if resp.peek_stderr():
                stderr_result += resp.read_stderr()

    except Exception as e:
        raise RuntimeError(f"Error executing command in pod '{name}'") from e

    finally:
        resp.close()

    if resp.returncode != 0:
        raise RuntimeError(f"Command failed with return code {resp.returncode}:\nSTDERR:\n{stderr_result}")

    return stdout_result


def get_namespace():
    if os.path.exists(namespace_id_file):
        with open(namespace_id_file, 'r') as f:
            out = f.read()
            return out
    return default_namespace
