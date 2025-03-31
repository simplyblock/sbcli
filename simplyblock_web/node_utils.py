#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os
import subprocess
import re

import jc
from kubernetes.stream import stream
from kubernetes import client, config

from simplyblock_web import utils



logger = logging.getLogger(__name__)


def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        return stdout.strip().decode("utf-8"), stderr.strip(), process.returncode
    except Exception as e:
        return "", str(e), 1


def _get_spdk_pcie_list():  # return: ['0000:00:1e.0', '0000:00:1f.0']
    out, err, _ = run_command("ls /sys/bus/pci/drivers/uio_pci_generic")
    spdk_pcie_list = [line for line in out.split() if line.startswith("0000")]
    if not spdk_pcie_list:
        out, err, _ = run_command("ls /sys/bus/pci/drivers/vfio-pci")
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
    out, err, rc = run_command("nvme list -v -o json")
    if rc != 0:
        logger.error("Error getting nvme list")
        logger.error(err)
        return []
    data = json.loads(out)
    logger.debug("nvme list:")
    logger.debug(data)

    devices = []
    if data and 'Devices' in data and data['Devices']:
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
    out, err, rc = run_command("ip -j address show")
    if rc != 0:
        logger.error(err)
        return []
    data = json.loads(out)

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


def _get_mem_info():
    out, err, rc = run_command("cat /proc/meminfo")

    if rc != 0:
        raise ValueError('Failed to get memory info')

    entry_regex = r'^(?P<name>[\w\(\)]+):\s+(?P<size>\d+)( (?P<kb>kB))?'

    return {
            m.group('name'): int(m.group('size')) * (1024 if m.group('kb') else 1)
            for line in out.splitlines()
            if (m := re.match(entry_regex, line)) is not None
    }


def get_memory():
    return _get_mem_info().get('MemTotal', 0)


def get_huge_memory():
    return _get_mem_info().get('Hugetlb', 0)


def get_memory_details():
    mem_info = _get_mem_info()
    result = {}

    if 'MemTotal' in mem_info:
        result['total'] = mem_info['MemTotal']

    if 'MemAvailable' in mem_info:
            result['free'] = mem_info['MemAvailable']

    if 'Hugetlb' in mem_info:
            result['huge_total'] = mem_info['Hugetlb']

    if 'HugePages_Free' in mem_info and 'Hugepagesize' in mem_info:
        result['huge_free'] = mem_info['HugePages_Free'] * mem_info['Hugepagesize']

    return result


def get_host_arch():
    out, err, rc = run_command("uname -m")
    return out


def firewall_port(port_id=9090, port_type="tcp", block=True):
    cmd_list = []
    try:
        iptables_command_output = firewall_get()
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
            # f"iptables -A INPUT -p {port_type} --dport {port_id} -j ACCEPT",
            # f"iptables -A OUTPUT -p {port_type} --dport {port_id} -j ACCEPT",
            "iptables -L -n -v",
        ])

    out = ""
    for cmd in cmd_list:
        stream = os.popen("docker exec spdk "+cmd)
        ret = stream.read()
        if ret != "":
            out += ret + "\n"
            logger.info(ret)

    return out


def firewall_get():
    cmd = "docker exec spdk iptables -L -n"
    stream = os.popen(cmd)
    ret = stream.read()
    return ret
