#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import math
import os
import time

import cpuinfo
import docker
import requests
from docker.types import LogConfig
from flask import Blueprint
from flask import request

from simplyblock_web import utils, node_utils

from simplyblock_core import scripts, constants, shell_utils

logger = logging.getLogger(__name__)

bp = Blueprint("snode", __name__, url_prefix="/snode")

cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"


def get_google_cloud_info():
    try:
        headers = {'Metadata-Flavor': 'Google'}
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true",
                                headers=headers, timeout=3)
        data = response.json()
        return {
            "id": str(data["id"]),
            "type": data["machineType"].split("/")[-1],
            "cloud": "google",
            "ip": data["networkInterfaces"][0]["ip"],
            "public_ip": data["networkInterfaces"][0]["accessConfigs"][0]["externalIp"],
        }
    except:
        pass


def get_equinix_cloud_info():
    try:
        response = requests.get("https://metadata.platformequinix.com/metadata", timeout=3)
        data = response.json()
        public_ip = ""
        ip = ""
        for interface in data["network"]["addresses"]:
            if interface["address_family"] == 4:
                if interface["enabled"] and interface["public"]:
                    public_ip = interface["address"]
                elif interface["enabled"] and not interface["public"]:
                    public_ip = interface["address"]
        return {
            "id": str(data["id"]),
            "type": data["class"],
            "cloud": "equinix",
            "ip": public_ip,
            "public_ip": ip
        }
    except:
        pass


def get_amazon_cloud_info():
    try:
        import ec2_metadata
        import requests
        session = requests.session()
        session.timeout = 3
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document
        return {
            "id": data["instanceId"],
            "type": data["instanceType"],
            "cloud": "amazon",
            "ip": data["privateIp"],
            "public_ip":  "",
        }
    except:
        pass


def get_docker_client():
    ip = os.getenv("DOCKER_IP")
    if not ip:
        for ifname in node_utils.get_nics_data():
            if ifname in ["eth0", "ens0"]:
                ip = node_utils.get_nics_data()[ifname]['ip']
                break
    return docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=60 * 5)


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

    multi_threading_enabled = False
    if 'multi_threading_enabled' in data:
        multi_threading_enabled = bool(data['multi_threading_enabled'])

    timeout = 60*5
    if 'timeout' in data:
        try:
            timeout = int(data['timeout'])
        except:
            pass

    if spdk_mem:
        spdk_mem = int(utils.parse_size(spdk_mem) / (constants.ONE_KB * constants.ONE_KB))
    else:
        spdk_mem = 4000

    node_docker = get_docker_client()
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] in ["/spdk", "/spdk_proxy"]:
            logger.info(f"{node.attrs['Name']} container found, removing...")
            node.stop(timeout=3)
            node.remove(force=True)

    spdk_debug = ""
    if set_debug:
        spdk_debug = "1"

    spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
    if 'spdk_image' in data and data['spdk_image']:
        spdk_image = data['spdk_image']

    node_docker.images.pull(spdk_image)

    if "cluster_ip" in data and data['cluster_ip']:
        cluster_ip = data['cluster_ip']
        log_config = LogConfig(type=LogConfig.types.GELF, config={"gelf-address": f"tcp://{cluster_ip}:12202"})
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
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_core/services/spdk_http_proxy_server.py",
        name="spdk_proxy",
        detach=True,
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/var/tmp:/var/tmp'
        ],
        environment=[
            f"SERVER_IP={data['server_ip']}",
            f"RPC_PORT={data['rpc_port']}",
            f"RPC_USERNAME={data['rpc_username']}",
            f"RPC_PASSWORD={data['rpc_password']}",
            f"MULTI_THREADING_ENABLED={multi_threading_enabled}",
            f"TIMEOUT={timeout}",
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
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
        if cont.attrs['Name'] == "/spdk" or cont.attrs['Name'] == "/spdk_proxy":
            cont.stop(timeout=3)
            cont.remove(force=True)
    return utils.get_response(True)


@bp.route('/spdk_process_is_up', methods=['GET'])
def spdk_process_is_up():
    node_docker = get_docker_client()
    for cont in node_docker.containers.list(all=True):
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

@bp.route('/get_file_content/<string:file_name>', methods=['GET'])
def get_file_content(file_name):
    out, err, _ = node_utils.run_command(f"cat /etc/simplyblock/{file_name}")
    if out:
        return utils.get_response(out)
    elif err:
        err = err.decode("utf-8")
        logger.debug(err)
        return utils.get_response(None, err)


def set_cluster_id(cluster_id):
    ret = os.popen(f"echo {cluster_id} > {cluster_id_file}").read().strip()
    return ret


def delete_cluster_id():
    out, _, _ = node_utils.run_command(f"rm -f {cluster_id_file}")
    return out


def get_node_lsblk():
    out, err, rc = node_utils.run_command("lsblk -J")
    if rc != 0:
        logger.error(err)
        return []
    data = json.loads(out)
    return data



@bp.route('/info', methods=['GET'])
def get_info():

    out = {
        "cluster_id": get_cluster_id(),

        "hostname": HOSTNAME,
        "system_id": SYSTEM_ID,

        "cpu_count": CPU_INFO['count'],
        "cpu_hz": CPU_INFO['hz_advertised'][0] if 'hz_advertised' in CPU_INFO else 1,

        "memory": node_utils.get_memory(),
        "hugepages": node_utils.get_huge_memory(),
        "memory_details": node_utils.get_memory_details(),

        "nvme_devices": node_utils._get_nvme_devices(),
        "nvme_pcie_list": node_utils._get_nvme_pcie_list(),

        "spdk_devices": node_utils._get_spdk_devices(),
        "spdk_pcie_list": node_utils._get_spdk_pcie_list(),

        "network_interface": node_utils.get_nics_data(),

        "cloud_instance": CLOUD_INFO,

        "lsblk": get_node_lsblk(),
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
    node_docker = get_docker_client()
    if node_docker.info()["Swarm"]["LocalNodeState"] in ["active", "pending"]:
        logger.info("Node is part of another swarm, leaving swarm")
        node_docker.swarm.leave(force=True)
        time.sleep(2)
    node_docker.swarm.join([f"{cluster_ip}:2377"], join_token)
    # retries = 10
    # while retries > 0:
    #     if node_docker.info()["Swarm"]["LocalNodeState"] == "active":
    #         break
    #     logger.info("Waiting for node to be active...")
    #     retries -= 1
    #     time.sleep(1)
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
        node_docker = get_docker_client()
        node_docker.swarm.leave(force=True)
    except:
        pass
    return utils.get_response(True)


@bp.route('/make_gpt_partitions', methods=['POST'])
def make_gpt_partitions_for_nbd():
    nbd_device = '/dev/nbd0'
    jm_percent = '3'
    num_partitions = 1
    partition_percent = 0
    try:
        data = request.get_json()
        nbd_device = data['nbd_device']
        jm_percent = data['jm_percent']
        partition_percent = data['partition_percent']
        num_partitions = data['num_partitions']
    except:
        pass

    cmd_list = [
        f"parted -fs {nbd_device} mklabel gpt",
        f"parted -f {nbd_device} mkpart journal \"0%\" \"{jm_percent}%\""
    ]
    sg_cmd_list = [
        f"sgdisk -t 1:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}",
    ]
    if partition_percent:
        perc_per_partition = int(partition_percent)
    else:
        perc_per_partition = int((100 - jm_percent) / num_partitions)

    for i in range(num_partitions):
        st = jm_percent + (i * perc_per_partition)
        en = st + perc_per_partition
        cmd_list.append(f"parted -f {nbd_device} mkpart part{(i+1)} \"{st}%\" \"{en}%\"")
        sg_cmd_list.append(f"sgdisk -t {(i+2)}:6527994e-2c5a-4eec-9613-8f5944074e8b {nbd_device}")

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
        f"echo \"nvme\" > /sys/bus/pci/devices/{device_pci}/driver_override",
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/nvme/bind",

    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    device_name = os.popen(f"ls /sys/devices/pci0000:00/{device_pci}/nvme/nvme*/ | grep nvme").read().strip()
    if device_name:
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


CPU_INFO = cpuinfo.get_cpu_info()
HOSTNAME, _, _ = node_utils.run_command("hostname -s")
SYSTEM_ID = ""
CLOUD_INFO = get_amazon_cloud_info()
if not CLOUD_INFO:
    CLOUD_INFO = get_google_cloud_info()

if not CLOUD_INFO:
    CLOUD_INFO = get_equinix_cloud_info()

if CLOUD_INFO:
    SYSTEM_ID = CLOUD_INFO["id"]
else:
    SYSTEM_ID, _, _ = node_utils.run_command("dmidecode -s system-uuid")


@bp.route('/bind_device_to_spdk', methods=['POST'])
def bind_device_to_spdk():
    data = request.get_json()
    if "device_pci" not in data:
        return utils.get_response(False, "Required parameter is missing: device_pci")

    device_pci = data['device_pci']

    cmd_list = [
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/nvme/unbind",
        f"echo \"\" > /sys/bus/pci/devices/{device_pci}/driver_override",
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers/uio_pci_generic/bind",
        f"echo \"uio_pci_generic\" > /sys/bus/pci/devices/{device_pci}/driver_override",
        f"echo -n \"{device_pci}\" > /sys/bus/pci/drivers_probe",
    ]

    for cmd in cmd_list:
        logger.debug(cmd)
        ret = os.popen(cmd).read().strip()
        logger.debug(ret)
        time.sleep(1)

    return utils.get_response(True)


@bp.route('/firewall_set_port', methods=['POST'])
def firewall_set_port():
    data = request.get_json()
    if "port_id" not in data:
        return utils.get_response(False, "Required parameter is missing: port_id")
    if "port_type" not in data:
        return utils.get_response(False, "Required parameter is missing: port_type")
    if "action" not in data:
        return utils.get_response(False, "Required parameter is missing: action")

    port_id = data['port_id']
    port_type = data['port_type']
    action = data['action']

    ret = node_utils.firewall_port(port_id, port_type, block=action=="block")
    return utils.get_response(ret)


@bp.route('/get_firewall', methods=['GET'])
def get_firewall():
    ret = node_utils.firewall_get()
    return utils.get_response(ret)
