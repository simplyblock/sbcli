# encoding: utf-8

import json
import logging
import re
from typing import List, Tuple

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
    logger.debug("function:get_nvme_devices start")
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
    logger.debug("function:get_nvme_devices end")

    return devices


def get_spdk_devices():
    return []


def _read_sysfs(path: str) -> str:
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except OSError:
        return ""


def _disk_holders(name: str) -> List[str]:
    """Union of /sys/block/<d>/holders and every partition's holders —
    catches LVM PVs, md members and dm-crypt without a mountpoint."""
    import os
    holders: List[str] = []
    base = f"/sys/block/{name}"
    try:
        holders.extend(os.listdir(f"{base}/holders"))
    except OSError:
        pass
    try:
        for entry in os.listdir(base):
            if entry.startswith(name):
                try:
                    holders.extend(os.listdir(f"{base}/{entry}/holders"))
                except OSError:
                    pass
    except OSError:
        pass
    return sorted(set(holders))


def _disk_by_id_path(name: str) -> str:
    """Preferred stable /dev/disk/by-id symlink for a whole disk: wwn-* first,
    then any other non-partition link. Empty when none exists."""
    import os
    by_id_dir = "/dev/disk/by-id"
    target = f"/dev/{name}"
    candidates: List[str] = []
    try:
        for entry in os.listdir(by_id_dir):
            if "-part" in entry:
                continue
            path = os.path.join(by_id_dir, entry)
            try:
                if os.path.realpath(path) == target:
                    candidates.append(path)
            except OSError:
                continue
    except OSError:
        return ""
    if not candidates:
        return ""
    candidates.sort(key=lambda p: (0 if "/wwn-" in p.replace("\\", "/") else 1, p))
    return candidates[0]


def _partition_by_id_path(name: str, partuuid: str) -> str:
    """Preferred stable path for a partition: /dev/disk/by-partuuid/<uuid>
    (stable across disk renames and unaffected by by-id link churn), falling
    back to a /dev/disk/by-id/*-part* symlink. Empty when none exists."""
    import os
    if partuuid:
        path = f"/dev/disk/by-partuuid/{partuuid.lower()}"
        try:
            if os.path.realpath(path) == f"/dev/{name}":
                return path
        except OSError:
            pass
    by_id_dir = "/dev/disk/by-id"
    target = f"/dev/{name}"
    candidates: List[str] = []
    try:
        for entry in os.listdir(by_id_dir):
            if "-part" not in entry:
                continue
            path = os.path.join(by_id_dir, entry)
            try:
                if os.path.realpath(path) == target:
                    candidates.append(path)
            except OSError:
                continue
    except OSError:
        return ""
    if not candidates:
        return ""
    candidates.sort(key=lambda p: (0 if "/wwn-" in p.replace("\\", "/") else 1, p))
    return candidates[0]


def _partition_holders(disk_name: str, part_name: str) -> List[str]:
    """Holders of a single partition (/sys/block/<disk>/<part>/holders)."""
    import os
    try:
        return sorted(set(os.listdir(f"/sys/block/{disk_name}/{part_name}/holders")))
    except OSError:
        return []


def _root_disk_names() -> List[str]:
    """Kernel names of the disk(s) backing the root filesystem."""
    out, _, rc = shell_utils.run_command("findmnt -no SOURCE /")
    if rc != 0 or not out.strip():
        return []
    source = out.strip().splitlines()[0]
    # Walk PKNAME upwards (handles /dev/sda2, dm/LVM roots, etc.).
    out, _, rc = shell_utils.run_command(f"lsblk -no PKNAME,NAME {source}")
    names = set()
    if rc == 0:
        for line in out.splitlines():
            for token in line.split():
                names.add(token.strip())
    if source.startswith("/dev/"):
        names.add(source[len("/dev/"):])
    return sorted(n for n in names if n)


def _subtree_mounted(dev: dict) -> bool:
    if dev.get("mountpoint"):
        return True
    return any(_subtree_mounted(child) for child in dev.get("children") or [])


def get_block_devices_info() -> List[dict]:
    """Inventory of block devices (whole disks AND their partitions) for the
    lblk cluster mode.

    One dict per lsblk TYPE=disk entry plus one per TYPE=part child, carrying
    everything the control plane needs for eligibility filtering, identity
    (serial-first) and AIO bdev creation. Sizes are bytes (lsblk -b).

    Disk identity: SERIAL, falling back to WWN; devices with neither get a
    synthetic-stable id derived from hostname|by-id-or-name|size so identity
    survives reboots.

    Partition identity: partitions have no lsblk SERIAL of their own, so the
    serial is derived from the parent disk's serial plus the PARTUUID
    ("<parent-serial>-part-<partuuid>") — stable across disk renames and
    unique per partition. Partitions without a PARTUUID get a synthetic id.
    """
    import hashlib
    import socket

    logger.debug("function:get_block_devices_info start")
    out, err, rc = shell_utils.run_command(
        "lsblk -J -b -o NAME,TYPE,SIZE,SERIAL,WWN,MOUNTPOINT,MODEL,ROTA,RO,VENDOR,PKNAME,PARTUUID")
    if rc != 0:
        logger.error("Error running lsblk: %s", err)
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse lsblk output: %s", e)
        return []

    root_disks = _root_disk_names()
    hostname = socket.gethostname()
    devices: List[dict] = []
    for dev in data.get("blockdevices", []):
        if dev.get("type") != "disk":
            continue
        name = dev.get("name", "")
        children = dev.get("children") or []
        by_id_path = _disk_by_id_path(name)
        serial = (dev.get("serial") or "").strip()
        wwn = (dev.get("wwn") or "").strip()
        if not serial:
            serial = wwn
        synthetic = False
        if not serial:
            seed = f"{hostname}|{by_id_path or name}|{dev.get('size') or 0}"
            serial = "SYN-" + hashlib.sha1(seed.encode()).hexdigest()[:16]
            synthetic = True
        numa_node = int(_read_sysfs(f"/sys/block/{name}/device/numa_node") or -1)
        devices.append({
            "name": name,
            "device_path": f"/dev/{name}",
            "type": dev.get("type"),
            "size": int(dev.get("size") or 0),
            "serial": serial,
            "serial_synthetic": synthetic,
            "wwn": wwn,
            "model": (dev.get("model") or "").strip(),
            "vendor": (dev.get("vendor") or "").strip(),
            "rota": bool(dev.get("rota")),
            "ro": bool(dev.get("ro")),
            "has_partitions": any(c.get("type") == "part" for c in children),
            "mounted_in_subtree": _subtree_mounted(dev),
            "holders": _disk_holders(name),
            "is_root_disk": name in root_disks,
            "by_id_path": by_id_path,
            "numa_node": numa_node,
        })
        for child in children:
            if child.get("type") != "part":
                continue
            part_name = child.get("name", "")
            partuuid = (child.get("partuuid") or "").strip()
            part_synthetic = False
            if partuuid:
                part_serial = f"{serial}-part-{partuuid.lower()}"
            else:
                seed = f"{hostname}|{serial}|{part_name}|{child.get('size') or 0}"
                part_serial = "SYN-" + hashlib.sha1(seed.encode()).hexdigest()[:16]
                part_synthetic = True
            devices.append({
                "name": part_name,
                "device_path": f"/dev/{part_name}",
                "type": "part",
                "size": int(child.get("size") or 0),
                "serial": part_serial,
                "serial_synthetic": part_synthetic or synthetic,
                "partuuid": partuuid,
                "parent_name": name,
                "parent_serial": serial,
                "wwn": wwn,
                "model": (dev.get("model") or "").strip(),
                "vendor": (dev.get("vendor") or "").strip(),
                "rota": bool(dev.get("rota")),
                "ro": bool(child.get("ro") or dev.get("ro")),
                "has_partitions": False,
                "mounted_in_subtree": _subtree_mounted(child),
                "holders": _partition_holders(name, part_name),
                "is_root_disk": part_name in root_disks,
                "by_id_path": _partition_by_id_path(part_name, partuuid),
                "numa_node": numa_node,
            })
    logger.debug("function:get_block_devices_info end")
    return devices


SB_GPT_PARTITION_TYPECODE = "6527994e-2c5a-4eec-9613-8f5944074e8b"


def split_partition_for_journal(part_name: str, jm_bytes: int) -> Tuple[dict, dict]:
    """Split an existing GPT partition into two: a journal partition of
    ``jm_bytes`` at its original start and a data partition covering the
    remainder. Used by lblk nodes running on partitions, where the journal
    can neither own a whole drive nor may we relabel one (the rest of the
    disk belongs to the OS or other software).

    The parent disk's partition table is modified ONLY within the bounds of
    the partition being split. The partition must be idle (unmounted, no
    holders, not backing root). Returns ``(jm_device, data_device)`` — the
    two new partitions' inventory dicts (get_block_devices_info shape).
    Raises ValueError on any precondition or tool failure.
    """
    import math
    import os

    inventory = {d["name"]: d for d in get_block_devices_info()}
    part = inventory.get(part_name)
    if part is None or part.get("type") != "part":
        raise ValueError(f"partition {part_name} not found")
    if part.get("mounted_in_subtree"):
        raise ValueError(f"partition {part_name} is mounted (busy)")
    if part.get("holders"):
        raise ValueError(f"partition {part_name} is held by {part['holders']} (busy)")
    if part.get("is_root_disk"):
        raise ValueError(f"partition {part_name} backs the root filesystem")
    parent = part.get("parent_name", "")
    if not parent:
        raise ValueError(f"cannot determine parent disk of {part_name}")

    out, _, rc = shell_utils.run_command(f"lsblk -ndo PTTYPE /dev/{parent}")
    if rc != 0 or out.strip() != "gpt":
        raise ValueError(
            f"disk {parent} has partition table {out.strip() or 'unknown'!r}; "
            f"splitting a partition for the journal requires GPT")

    sys_part = f"/sys/block/{parent}/{part_name}"
    try:
        part_number = int(_read_sysfs(f"{sys_part}/partition"))
        start_sector = int(_read_sysfs(f"{sys_part}/start"))
        size_sectors = int(_read_sysfs(f"{sys_part}/size"))
    except (ValueError, TypeError):
        raise ValueError(f"cannot read geometry of {part_name} from sysfs")

    # 1 MiB alignment (2048 x 512b sectors) for the data partition start.
    align = 2048
    jm_sectors = int(math.ceil(jm_bytes / 512 / align) * align)
    end_sector = start_sector + size_sectors - 1
    data_start = start_sector + jm_sectors
    if data_start + align > end_sector:
        raise ValueError(
            f"partition {part_name} ({size_sectors * 512} bytes) is too small "
            f"to split into a {jm_bytes}-byte journal plus a data partition")

    cmds = [
        f"sgdisk -d {part_number} /dev/{parent}",
        (f"sgdisk -a 1 -n {part_number}:{start_sector}:{data_start - 1} "
         f"-t {part_number}:{SB_GPT_PARTITION_TYPECODE} -c {part_number}:sb_jm /dev/{parent}"),
        (f"sgdisk -a 1 -n 0:{data_start}:{end_sector} "
         f"-t 0:{SB_GPT_PARTITION_TYPECODE} -c 0:sb_data /dev/{parent}"),
    ]
    for cmd in cmds:
        out, err, rc = shell_utils.run_command(cmd)
        if rc != 0:
            raise ValueError(f"{cmd} failed (rc={rc}): {err or out}")

    _, _, rc = shell_utils.run_command(f"partprobe /dev/{parent}")
    if rc != 0:
        _, err, rc = shell_utils.run_command(f"partx -u /dev/{parent}")
        if rc != 0:
            raise ValueError(f"failed to re-read partition table of {parent}: {err}")
    shell_utils.run_command("udevadm settle -t 5")

    # Identify the two new partitions by their start sectors.
    jm_name = data_name = ""
    try:
        for entry in os.listdir(f"/sys/block/{parent}"):
            if not entry.startswith(parent):
                continue
            e_start = _read_sysfs(f"/sys/block/{parent}/{entry}/start")
            if not e_start:
                continue
            if int(e_start) == start_sector:
                jm_name = entry
            elif int(e_start) == data_start:
                data_name = entry
    except OSError:
        pass
    if not jm_name or not data_name:
        raise ValueError(
            f"split of {part_name} completed but the new partitions were not "
            f"found on {parent} (journal at sector {start_sector}, data at "
            f"{data_start})")

    inventory = {d["name"]: d for d in get_block_devices_info()}
    if jm_name not in inventory or data_name not in inventory:
        raise ValueError(
            f"new partitions {jm_name}/{data_name} missing from inventory "
            f"after split of {part_name}")
    return inventory[jm_name], inventory[data_name]


def wipe_block_device_signatures(device_name: str) -> Tuple[bool, str]:
    """Wipe partition-table / filesystem signatures from a whole disk
    (`--force-format` on lblk add-node). Re-validates that the device is not
    busy before touching it: any mountpoint in the subtree or any holder
    refuses the wipe. Wipes partitions first, then the disk itself."""
    import re as _re
    if not _re.match(r"^[a-zA-Z0-9_\-]+$", device_name):
        return False, f"invalid device name {device_name!r}"
    for dev in get_block_devices_info():
        if dev["name"] == device_name:
            if dev["mounted_in_subtree"]:
                return False, f"device {device_name} has mounted filesystems"
            if dev["holders"]:
                return False, (f"device {device_name} is held by "
                               f"{dev['holders']}")
            if dev["is_root_disk"]:
                return False, f"device {device_name} backs the root filesystem"
            break
    else:
        return False, f"device {device_name} not found"

    out, _, rc = shell_utils.run_command(
        f"lsblk -nro NAME -x NAME /dev/{device_name}")
    if rc != 0:
        return False, f"lsblk failed for {device_name}"
    # Children (partitions) first, whole disk last.
    names = [n for n in out.split() if n and n != device_name]
    for name in names + [device_name]:
        _, err, rc = shell_utils.run_command(f"wipefs -a /dev/{name}")
        if rc != 0:
            return False, f"wipefs /dev/{name} failed: {err}"
    return True, ""


def _get_mem_info():
    logger.debug("function:_get_mem_info start")
    out, err, rc = shell_utils.run_command("cat /proc/meminfo")

    if rc != 0:
        raise ValueError('Failed to get memory info')

    entry_regex = r'^(?P<name>[\w\(\)]+):\s+(?P<size>\d+)( (?P<kb>kB))?'
    logger.debug("function:_get_mem_info end")

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
