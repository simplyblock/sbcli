#!/usr/bin/env python
# encoding: utf-8
import logging
import os

import jc
from kubernetes.stream import stream
from kubernetes import client, config


node_name = os.environ.get("HOSTNAME")
deployment_name = f"snode-spdk-deployment-{node_name}"
pod_name = deployment_name[:50]
namespace_id_file = '/etc/simplyblock/namespace'
default_namespace = 'default'

config.load_incluster_config()
k8s_core_v1 = client.CoreV1Api()

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
            f"iptables -A INPUT -p {port_type} --dport {port_id} -j REJECT",
            f"iptables -A OUTPUT -p {port_type} --dport {port_id} -j DROP",
            f"iptables -A OUTPUT -p {port_type} --dport {port_id} -j REJECT",
            "iptables -L -n -v",
        ])
    else:
        cmd_list.extend([
            # f"iptables -A INPUT -p {port_type} --dport {port_id} -j ACCEPT",
            # f"iptables -A OUTPUT -p {port_type} --dport {port_id} -j ACCEPT",
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


def firewall_get_k8s():
    ret = ""
    resp = k8s_core_v1.list_namespaced_pod(get_namespace())
    for pod in resp.items:
        if pod.metadata.name.startswith(pod_name):
            container = "spdk-container"
            ret = pod_exec(pod.metadata.name, get_namespace(), container, "iptables -L -n", k8s_core_v1)
    return ret


def pod_exec(name, namespace, container, command, k8s_core_v1):
    exec_command = ["/bin/sh", "-c", command]

    resp = stream(k8s_core_v1.connect_get_namespaced_pod_exec,
                  name,
                  namespace,
                  command=exec_command,
                  container=container,
                  stderr=True, stdin=False,
                  stdout=True, tty=False,
                  _preload_content=False)

    result = ""
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            result = resp.read_stdout()
            print(f"STDOUT: \n{result}")
        if resp.peek_stderr():
            print(f"STDERR: \n{resp.read_stderr()}")

    resp.close()

    if resp.returncode != 0:
        raise Exception(resp.readline_stderr())

    return result

def get_namespace():
    if os.path.exists(namespace_id_file):
        with open(namespace_id_file, 'r') as f:
            out = f.read()
            return out
    return default_namespace
