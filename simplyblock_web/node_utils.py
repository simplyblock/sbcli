#!/usr/bin/env python
# encoding: utf-8

import json
import logging
import re
from typing import List, Tuple, TypedDict

import boto3
import requests

from simplyblock_core import shell_utils
from simplyblock_core.utils.pci import PCIAddress
import simplyblock_core.utils.pci as pci_utils
from pydantic import BaseModel


# Type definitions
class NVMENamespace(BaseModel):
    NameSpace: str
    PhysicalSize: int
    SectorSize: int


class NVMeController(BaseModel):
    Controller: str
    Address: str
    Transport: str
    ModelNumber: str
    SerialNumber: str
    Namespaces: List[NVMENamespace]


class NVMeSubsystem(BaseModel):
    SubsystemNQN: str
    Controllers: List[NVMeController]
    Namespaces: List[NVMENamespace]


class NVMeDevice(BaseModel):
    nqn: str
    size: int
    sector_size: int
    device_name: str
    device_path: str
    controller_name: str
    address: str
    transport: str
    model_id: str
    serial_number: str


logger = logging.getLogger(__name__)


def get_spdk_pcie_list() -> List[PCIAddress]:
    """
    Get a list of PCIe devices bound to SPDK-compatible drivers.
    
    Returns:
        List[PCIAddress]: List of PCIe addresses (e.g., ['0000:00:1e.0', '0000:00:1f.0'])
    """
    return pci_utils.list_devices(driver_name='uio_pci_generic') or pci_utils.list_devices(driver_name='vfio-pci')


def get_nvme_pcie_list() -> List[PCIAddress]:
    """
    Get a list of NVMe PCIe devices.
    
    Returns:
        List[PCIAddress]: List of NVMe PCIe addresses (e.g., ['0000:00:1e.0', '0000:00:1f.0'])
    """
    return pci_utils.list_devices(driver_name='nvme')


def get_nvme_pcie() -> List[Tuple[str, Tuple[int, int]]]:
    """
    Get a list of NVMe PCIe devices with their vendor and device IDs.
    
    Returns:
        List[Tuple[str, Tuple[int, int]]]: List of tuples containing
            (pci_address, (vendor_id, device_id))
    """
    return [
        (address, (pci_utils.vendor_id(address), pci_utils.device_id(address)))
        for address in pci_utils.list_devices(device_class=pci_utils.NVME_CLASS)
    ]


def get_nvme_devices() -> List[NVMeDevice]:
    """
    Get detailed information about NVMe devices in the system.
    
    Returns:
        List[NVMeDevice]: A list of dictionaries containing NVMe device information
    """
    out, err, rc = shell_utils.run_command("nvme list -v -o json")
    if rc != 0:
        logger.error("Error getting nvme list: %s", err)
        return []
        
    try:
        data = json.loads(out)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse NVMe device list: %s", e)
        return []
        
    logger.debug("NVMe device list: %s", data)
    devices: List[NVMeDevice] = []
    
    if not data or 'Devices' not in data or not data['Devices']:
        return devices
        
    for dev in data['Devices'][0].get('Subsystems', []):
        if not dev.get('Controllers'):
            continue
            
        controller = dev['Controllers'][0]
        namespace = None
        
        # Try to get namespace from device first, then from controller
        if dev.get('Namespaces'):
            namespace = dev['Namespaces'][0]
        elif controller and controller.get('Namespaces'):
            namespace = controller['Namespaces'][0]
            
        if namespace:
            data = {
                'nqn': dev.get('SubsystemNQN', ''),
                'size': namespace.get('PhysicalSize', 0),
                'sector_size': namespace.get('SectorSize', 0),
                'device_name': namespace.get('NameSpace', ''),
                'device_path': f"/dev/{namespace.get('NameSpace', '')}",
                'controller_name': controller.get('Controller', ''),
                'address': controller.get('Address', ''),
                'transport': controller.get('Transport', ''),
                'model_id': controller.get('ModelNumber', ''),
                'serial_number': controller.get('SerialNumber', '')
            }
            device = NVMeDevice(**data)
            devices.append(device)
            
    return devices


def get_spdk_devices():
    return []


def _get_mem_info():
    out, _, rc = shell_utils.run_command("cat /proc/meminfo")

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
    out, err, rc = shell_utils.run_command("uname -m")
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
