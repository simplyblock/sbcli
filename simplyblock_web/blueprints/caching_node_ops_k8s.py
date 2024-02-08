#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import math
import os
import time
import subprocess

import cpuinfo
import yaml
from kubernetes import client, config
from flask import Blueprint
from flask import request
from kubernetes.client import ApiException

from simplyblock_core import constants

from simplyblock_web import utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("caching_node_k", __name__, url_prefix="/cnode")


node_name = os.environ.get("HOSTNAME")
deployment_name = f"spdk-deployment-{node_name}"
namespace = 'default'
pod_name = 'spdk-deployment'


config.load_incluster_config()
k8s_apps_v1 = client.AppsV1Api()
k8s_core_v1 = client.CoreV1Api()

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
spdk_deploy_yaml = os.path.join(TOP_DIR, 'static/deploy_spdk.yaml')


def run_command(cmd):
    process = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode


def _get_spdk_pcie_list():  # return: ['0000:00:1e.0', '0000:00:1f.0']
    out, err, _ = run_command("ls /sys/bus/pci/drivers/uio_pci_generic")
    spdk_pcie_list = [line for line in out.split() if line.startswith("0000")]
    logger.debug(spdk_pcie_list)
    return spdk_pcie_list


def _get_nvme_pcie_list():  # return: ['0000:00:1e.0', '0000:00:1f.0']
    out, err, _ = run_command("ls /sys/bus/pci/drivers/nvme")
    spdk_pcie_list = [line for line in out.split() if line.startswith("0000")]
    logger.debug(spdk_pcie_list)
    return spdk_pcie_list


def get_nvme_pcie():
    # Returns a list of nvme pci address and vendor IDs,
    # each list item is a tuple [("PCI_ADDRESS", "VENDOR_ID:DEVICE_ID")]
    stream = os.popen("lspci -Dnn | grep -i nvme")
    ret = stream.readlines()
    devs = []
    for line in ret:
        line_array = line.split()
        devs.append((line_array[0], line_array[-1][1:-1]))
    return devs


def _get_nvme_devices():
    out, err, _ = run_command("nvme list -v -o json")
    data = json.loads(out)
    logger.debug("nvme list:")
    logger.debug(data)

    devices = []
    if data and 'Devices' in data:
        for host in data['Devices']:
            for dev in host['Subsystems']:
                if 'Controllers' in dev and dev['Controllers']:
                    controller = dev['Controllers'][0]
                    namespace = None
                    if "Namespaces" in dev and dev['Namespaces']:
                        namespace = dev['Namespaces'][0]
                    elif controller and controller["Namespaces"]:
                        namespace = controller['Namespaces'][0]
                    if namespace:
                        devices.append({
                            'nqn': dev['SubsystemNQN'],
                            'size': namespace['PhysicalSize'],
                            'sector_size': namespace['SectorSize'],
                            'device_name': namespace['NameSpace'],
                            'device_path': "/dev/"+namespace['NameSpace'],
                            'controller_name': controller['Controller'],
                            'address': controller['Address'],
                            'transport': controller['Transport'],
                            'model_id': controller['ModelNumber'],
                            'serial_number': controller['SerialNumber']})
    return devices


def get_nics_data():
    out, _, _ = run_command("ip -j address show")
    data = json.loads(out)
    logger.debug("ifaces")
    logger.debug(data)

    def _get_ip4_address(list_of_addr):
        if list_of_addr:
            for data in list_of_addr:
                if data['family'] == 'inet':
                    return data['local']
        return ""

    devices = {i["ifname"]: i for i in data}
    iface_list = {}
    for nic in devices:
        device = devices[nic]
        iface = {
            'name': device['ifname'],
            'ip': _get_ip4_address(device['addr_info']),
            'status': device['operstate'],
            'net_type': device['link_type']}
        iface_list[nic] = iface
    return iface_list


def _get_spdk_devices():
    return []


@bp.route('/scan_devices', methods=['GET'])
def scan_devices():
    run_health_check = request.args.get('run_health_check', default=False, type=bool)
    out = {
        "nvme_devices": _get_nvme_devices(),
        "nvme_pcie_list": _get_nvme_pcie_list(),
        "spdk_devices": _get_spdk_devices(),
        "spdk_pcie_list": _get_spdk_pcie_list(),
    }
    return utils.get_response(out)


@bp.route('/spdk_process_start', methods=['POST'])
def spdk_process_start():
    data = request.get_json()

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in data:
        spdk_cpu_mask = data['spdk_cpu_mask']
    spdk_mem = None
    if 'spdk_mem' in data:
        spdk_mem = data['spdk_mem']
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

    if spdk_mem:
        spdk_mem = int(spdk_mem / (1024 * 1024))
    else:
        spdk_mem = 64096

    spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE
    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']
        # node_docker.images.pull(spdk_image)

    # with open(spdk_deploy_yaml, 'r') as f:
    #     dep = yaml.safe_load(f)

    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template('deploy_spdk.yaml.j2')
    values = {
        'SPDK_IMAGE': spdk_image,
        'SPDK_CPU_MASK': spdk_cpu_mask,
        'SPDK_MEM': spdk_mem,
        'SERVER_IP': data['server_ip'],
        'RPC_PORT': data['rpc_port'],
        'RPC_USERNAME': data['rpc_username'],
        'RPC_PASSWORD': data['rpc_password'],
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


def _get_mem_info():
    out, err, _ = run_command("cat /proc/meminfo")
    data = {}
    for line in out.split('\n'):
        tm = line.split(":")
        data[tm[0].strip()] = tm[1].strip()
    return data


def get_memory():
    try:
        mem_kb = _get_mem_info()['MemTotal']
        mem_kb = mem_kb.replace(" ", "").lower()
        mem_kb = mem_kb.replace("b", "")
        return utils.parse_size(mem_kb)
    except:
        return 0


def get_huge_memory():
    try:
        mem_kb = _get_mem_info()['Hugetlb']
        mem_kb = mem_kb.replace(" ", "").lower()
        mem_kb = mem_kb.replace("b", "")
        return utils.parse_size(mem_kb)
    except:
        return 0


@bp.route('/info', methods=['GET'])
def get_info():
    hostname, _, _ = run_command("hostname -s")
    system_id, _, _ = run_command("dmidecode -s system-uuid")

    out = {
        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpuinfo.get_cpu_info()['count'],
        "cpu_hz": cpuinfo.get_cpu_info()['hz_advertised'][0],

        "memory": get_memory(),
        "hugepages": get_huge_memory(),

        "nvme_devices": _get_nvme_devices(),
        "nvme_pcie_list": _get_nvme_pcie_list(),

        "spdk_devices": _get_spdk_devices(),
        "spdk_pcie_list": _get_spdk_pcie_list(),

        "network_interface": get_nics_data()
    }
    return utils.get_response(out)


@bp.route('/join_db', methods=['POST'])
def join_db():
    # data = request.get_json()
    # db_connection = data['db_connection']
    #
    # logger.info("Setting DB connection")
    # ret = scripts.set_db_config(db_connection)
    #
    # try:
    #     node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock')
    #     nodes = node_docker.containers.list(all=True)
    #     for node in nodes:
    #         if node.attrs["Name"] == "/spdk_proxy":
    #             node_docker.containers.get(node.attrs["Id"]).restart()
    #             break
    # except:
    #     pass
    return utils.get_response(True)


@bp.route('/nvme_connect', methods=['POST'])
def connect_to_nvme():
    data = request.get_json()
    ip = data['ip']
    port = data['port']
    nqn = data['nqn']
    st = f"nvme connect --transport=tcp --traddr={ip} --trsvcid={port} --nqn={nqn}"
    logger.debug(st)
    out, err, ret_code = run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    if ret_code == 0:
        return utils.get_response(True)
    else:
        return utils.get_response(ret_code, error=err)


@bp.route('/disconnect_device', methods=['POST'])
def disconnect_device():
    data = request.get_json()
    dev_path = data['dev_path']
    st = f"nvme disconnect --device={dev_path}"
    out, err, ret_code = run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


@bp.route('/disconnect_nqn', methods=['POST'])
def disconnect_nqn():
    data = request.get_json()
    nqn = data['nqn']
    st = f"nvme disconnect --nqn={nqn}"
    out, err, ret_code = run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)


@bp.route('/disconnect_all', methods=['POST'])
def disconnect_all():
    st = "nvme disconnect-all"
    out, err, ret_code = run_command(st)
    logger.debug(ret_code)
    logger.debug(out)
    logger.debug(err)
    return utils.get_response(ret_code)
