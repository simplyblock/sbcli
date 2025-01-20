#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import os
import subprocess
import requests
import boto3
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

def get_region():
    try:
        response = requests.get("http://169.254.169.254/latest/meta-data/placement/region", timeout=2)
        response.raise_for_status()
        region = response.text
        logger.info(f"Dynamically retrieved region: {region}")
        return region
    except Exception as e:
        logger.error(f"Failed to retrieve region: {str(e)}")
        return ""


def detach_ebs_volumes(instance_id):
    detached_volumes = []

    try:
        region = get_region()
        session = boto3.Session(region_name=region)

        ec2 = session.resource("ec2")
        client = session.client("ec2")

        instance = ec2.Instance(instance_id)
        volumes = instance.volumes.all()

        logger.info(f"Checking volumes attached to instance {instance_id}.")

        for volume in volumes:
            for tag in (volume.tags or []):
                logger.debug(f"Tags for volume {volume.id}: {tag}")
                if "simplyblock-jm" in tag['Value'] or "simplyblock-storage" in tag['Value']:
                    volume_id = volume.id
                    logger.info(f"Found volume {volume_id} with matching tags on instance {instance_id}.")

                    # Detach the volume
                    client.detach_volume(VolumeId=volume_id, InstanceId=instance_id, Force=True)
                    logger.info(f"Successfully detached volume {volume_id} from instance {instance_id}.")
                    
                    detached_volumes.append(volume_id)

        if detached_volumes:
            logger.info(f"Detached volumes: {detached_volumes}")
        else:
            logger.info(f"No volumes with matching tags found on instance {instance_id}.")

    except Exception as e:
        logger.error(f"Failed to detach EBS volumes: {str(e)}")

    return detached_volumes

def attach_ebs_volumes(instance_id, volume_ids):
    try:
        region = get_region()
        session = boto3.Session(region_name=region)  
        client = session.client("ec2")

        logger.info(f"Attaching volumes to instance {instance_id}. Volumes: {volume_ids}")

        for volume_id in volume_ids:
            device_name = get_available_device_name(instance_id)
            
            if not device_name:
                logger.error(f"Could not find an available device name for volume {volume_id}.")
                continue

            # Attach the volume to the instance
            client.attach_volume(VolumeId=volume_id, InstanceId=instance_id, Device=device_name)
            logger.info(f"Successfully attached volume {volume_id} to instance {instance_id} with device name {device_name}.")

        logger.info("All volumes attached successfully.")
        return True 
    except Exception as e:
        logger.error(f"Failed to attach EBS volumes: {str(e)}")
        return False

def get_available_device_name(instance_id):
    region = get_region()
    session = boto3.Session(region_name=region)  
    ec2 = session.client('ec2')

    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        instance = response['Reservations'][0]['Instances'][0]

        block_device_mappings = instance.get('BlockDeviceMappings', [])
        
        in_use_devices = [device['DeviceName'] for device in block_device_mappings]
        
        logger.info(f"Current devices in use by instance {instance_id}: {in_use_devices}")

        device_letter = ord('f')
        while True:
            device_name = f'/dev/sd{chr(device_letter)}'
            
            if device_name not in in_use_devices:
                logger.info(f"Available device name for attachment: {device_name}")
                return device_name

            device_letter += 1

    except Exception as e:
        logger.error(f"Failed to get available device name: {str(e)}")
        return None
