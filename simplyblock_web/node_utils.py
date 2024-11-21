#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os
import subprocess

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
    data = {}
    if rc == 0:
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


def get_memory_details():
    data = {}
    mem_info = _get_mem_info()
    try:
        mem_kb = mem_info['MemTotal']
        data['total'] = utils.parse_size(mem_kb.replace(" ", ""))

        mem_kb = mem_info['MemAvailable']
        data['free'] = utils.parse_size(mem_kb.replace(" ", ""))

        mem_kb = mem_info['Hugetlb']
        data['huge_total'] = utils.parse_size(mem_kb.replace(" ", ""))

        mem_kb = mem_info['Hugepagesize']
        hugePages_Free = int(mem_info['HugePages_Free'])
        data['huge_free'] = utils.parse_size(mem_kb.replace(" ", "")) * hugePages_Free

    except:
        pass
    return data


def get_host_arch():
    out, err, rc = run_command("uname -m")
    return out
