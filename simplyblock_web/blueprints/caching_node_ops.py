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

from simplyblock_web import utils, node_utils
from simplyblock_core import scripts, constants, shell_utils, utils as core_utils

logger = logging.getLogger(__name__)

bp = Blueprint("caching_node", __name__, url_prefix="/cnode")


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in node_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = node_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


def run_command(cmd):
    process = subprocess.Popen(
        cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return stdout.strip().decode("utf-8"), stderr.strip().decode("utf-8"), process.returncode


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
        for dev in data['Devices'][0]['Subsystems']:
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
    try:
        data = request.get_json()
    except:
        data = {}

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

    server_ip = data['server_ip']
    rpc_port = data['rpc_port']
    rpc_username = data['rpc_username']
    rpc_password = data['rpc_password']
    rpc_sock = f"/var/tmp/spdk_{rpc_port}.sock"
    if 'rpc_sock' in data and data['rpc_sock']:
        rpc_sock = data['rpc_sock']

    node_docker = get_docker_client()
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] in [f"/spdk_{rpc_port}", f"/spdk_proxy_{rpc_port}"]:
            logger.info(f"{node.attrs['Name']} container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    spdk_image = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE

    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']
        node_docker.images.pull(spdk_image)

    container = node_docker.containers.run(
        spdk_image,
        f"/root/scripts/run_spdk_tgt.sh {spdk_cpu_mask} {spdk_mem_mib} {rpc_sock}",
        name=f"spdk_{rpc_port}",
        detach=True,
        privileged=True,
        # network_mode="host",
        log_config=LogConfig(type=LogConfig.types.JOURNALD),
        volumes=[
            '/var/tmp:/var/tmp',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/var/lib/systemd/coredump/:/var/lib/systemd/coredump/',
            '/sys:/sys'],
        # restart_policy={"Name": "on-failure", "MaximumRetryCount": 99}
    )

    node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_core/services/spdk_http_proxy_server.py",
        name=f"spdk_proxy_{rpc_port}",
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
            f"RPC_SOCK={rpc_sock}",
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
    rpc_port = request.args.get('rpc_port', default=f"{constants.RPC_HTTP_PROXY_PORT}", type=str)
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        if cont.attrs['Name'] == f"/spdk_{rpc_port}" or cont.attrs['Name'] == f"/spdk_proxy_{rpc_port}":
            cont.stop()
            cont.remove(force=force)
    return utils.get_response(True)


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    rpc_port = request.args.get('rpc_port', default=f"{constants.RPC_HTTP_PROXY_PORT}", type=str)
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        if cont.attrs['Name'] == f"/spdk_{rpc_port}":
            status = cont.attrs['State']["Status"]
            is_running = cont.attrs['State']["Running"]
            if is_running:
                return utils.get_response(cont.attrs)
            else:
                return utils.get_response(False, f"SPDK container status: {status}, is running: {is_running}")
    return utils.get_response(False, "SPDK container not found")


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = node_utils.run_command("hostname -s")
SYSTEM_ID, _, _ = node_utils.run_command("dmidecode -s system-uuid")


@bp.route('/info', methods=['GET'])
def get_info():

    out = {
        "hostname": HOSTNAME,
        "system_id": SYSTEM_ID,

        "cpu_count": CPU_INFO['count'],
        "cpu_hz": CPU_INFO['hz_advertised'][0] if 'hz_advertised' in CPU_INFO else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": _get_nvme_devices(),
        "nvme_pcie_list": _get_nvme_pcie_list(),

        "spdk_devices": _get_spdk_devices(),
        "spdk_pcie_list": _get_spdk_pcie_list(),

        "network_interface": get_nics_data()
    }
    return utils.get_response(out)


@bp.route('/join_db', methods=['POST'])
def join_db():
    data = request.get_json()
    db_connection = data['db_connection']
    rpc_port =constants.RPC_HTTP_PROXY_PORT
    if 'rpc_port' in data:
        rpc_port = data['rpc_port']

    logger.info("Setting DB connection")
    ret = scripts.set_db_config(db_connection)

    try:
        node_docker = get_docker_client()
        nodes = node_docker.containers.list(all=True)
        for node in nodes:
            if node.attrs["Name"] == f"/spdk_proxy_{rpc_port}":
                node_docker.containers.get(node.attrs["Id"]).restart()
                break
    except:
        pass
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



@bp.route('/make_gpt_partitions', methods=['POST'])
def make_gpt_partitions_for_nbd():
    nbd_device = '/dev/nbd0'
    jm_percent = 10

    try:
        data = request.get_json()
        nbd_device = data['nbd_device']
        jm_percent = data['jm_percent']
    except:
        pass

    cmd_list = [
        f"parted -fs {nbd_device} mklabel gpt",
        f"parted -f {nbd_device} mkpart journal \"0%\" \"{jm_percent}%\"",
        f"parted -f {nbd_device} mkpart part \"{jm_percent}%\" \"100%\" ",
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}",
        f"sgdisk -t 2:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}",
    ]

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
