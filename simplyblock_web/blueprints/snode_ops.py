#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import math
import os
import time
import subprocess

import cpuinfo
import docker
from docker.types import LogConfig
from flask import Blueprint
from flask import request

from simplyblock_web import utils

from simplyblock_core import scripts, constants

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("snode", __name__, url_prefix="/snode")


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

    def _get_pcie_controller(ctrl_list):
        if ctrl_list:
            for item in ctrl_list:
                if 'Transport' in item and item['Transport'] == 'pcie':
                    return item

    def _get_size_from_namespaces(namespaces):
        size = 0
        if namespaces:
            for ns in namespaces:
                size += ns['PhysicalSize']
        return size

    devices = {}
    if data and 'Devices' in data:
        for dev in data['Devices'][0]['Subsystems']:
            controller = _get_pcie_controller(dev['Controllers'])
            if not controller:
                continue
            size = _get_size_from_namespaces(controller['Namespaces'])
            devices[controller['Address']] = {
                'device_name': controller['Controller'],
                'size': size,
                'pcie_address': controller['Address'],
                'model_id': controller['ModelNumber'],
                'serial_number': controller['SerialNumber']}
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
    try:
        data = request.get_json()
    except:
        data = {}

    cmd_params = None
    if 'cmd_params' in data and data['cmd_params']:
        cmd_params = data['cmd_params']

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

    params_line = ""
    if cmd_params:
        params_line = " ".join(cmd_params)

    spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']
        node_docker.images.pull(spdk_image)

    container = node_docker.containers.run(
        spdk_image,
        f"/root/scripts/run_distr.sh {spdk_cpu_mask} {spdk_mem} {params_line}",
        name="spdk",
        detach=True,
        privileged=True,
        network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys'],
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )
    container2 = node_docker.containers.run(
        constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE,
        "python /root/scripts/spdk_http_proxy.py",
        name="spdk_proxy",
        detach=True,
        network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
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


def _get_mem_info():
    out, err, _ = run_command("cat /proc/meminfo")
    data = {}
    for line in out.split('\n'):
        tm = line.split(":")
        data[tm[0].strip()] = tm[1].strip()
    return data


cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"
def get_cluster_id():
    out, _, _ = run_command(f"cat {cluster_id_file}")
    return out


def set_cluster_id(cluster_id):
    out, _, _ = run_command(f"echo {cluster_id} > {cluster_id_file}")
    return out


def delete_cluster_id():
    out, _, _ = run_command(f"rm -f {cluster_id_file}")
    return out



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



cpu_info = cpuinfo.get_cpu_info()

@bp.route('/info', methods=['GET'])
def get_info():
    hostname, _, _ = run_command("hostname -s")
    system_id, _, _ = run_command("dmidecode -s system-uuid")

    out = {
        "cluster_id": get_cluster_id(),

        "hostname": hostname,
        "system_id": system_id,

        "cpu_count": cpu_info['count'],
        "cpu_hz": cpu_info['hz_advertised'][0],

        "memory": get_memory(),

        "hugepages": get_huge_memory(),

        "nvme_devices": _get_nvme_devices(),
        "nvme_pcie_list": _get_nvme_pcie_list(),

        "spdk_devices": _get_spdk_devices(),
        "spdk_pcie_list": _get_spdk_pcie_list(),

        "network_interface": get_nics_data()
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
