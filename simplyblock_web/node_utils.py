
import json
import logging
import os
import re
import shutil

import boto3
import requests
from pydantic import BaseModel

import simplyblock_core.utils.pci as pci_utils
from simplyblock_core.utils import host_network
from simplyblock_core.utils import shell as shell_utils
from simplyblock_core.utils.pci import PCIAddress


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
    Namespaces: list[NVMENamespace]


class NVMeSubsystem(BaseModel):
    SubsystemNQN: str
    Controllers: list[NVMeController]
    Namespaces: list[NVMENamespace]


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


def get_spdk_pcie_list() -> list[PCIAddress]:
    """
    Get a list of PCIe devices bound to SPDK-compatible drivers.

    Returns:
        list[PCIAddress]: List of PCIe addresses (e.g., ['0000:00:1e.0', '0000:00:1f.0'])
    """
    return pci_utils.list_devices(driver_name='uio_pci_generic') or pci_utils.list_devices(driver_name='vfio-pci')


def get_nvme_pcie_list() -> list[PCIAddress]:
    """
    Get a list of NVMe PCIe devices.

    Returns:
        list[PCIAddress]: List of NVMe PCIe addresses (e.g., ['0000:00:1e.0', '0000:00:1f.0'])
    """
    return pci_utils.list_devices(driver_name='nvme')


def get_nvme_pcie() -> list[tuple[str, tuple[int, int]]]:
    """
    Get a list of NVMe PCIe devices with their vendor and device IDs.

    Returns:
        list[tuple[str, tuple[int, int]]]: List of tuples containing
            (pci_address, (vendor_id, device_id))
    """
    return [
        (address, (pci_utils.vendor_id(address), pci_utils.device_id(address)))
        for address in pci_utils.list_devices(device_class=pci_utils.NVME_CLASS)
    ]


def get_nvme_devices() -> list[NVMeDevice]:
    """
    Get detailed information about NVMe devices in the system.

    Returns:
        list[NVMeDevice]: A list of dictionaries containing NVMe device information
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
    devices: list[NVMeDevice] = []

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


def _disk_holders(name: str) -> list[str]:
    """Union of /sys/block/<d>/holders and every partition's holders —
    catches LVM PVs, md members and dm-crypt without a mountpoint."""
    import os
    holders: list[str] = []
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


DEV_ROOT = "/dev"

# The /dev/disk directories a persistent name is taken from, in the order a
# device's preferred link is looked for.
#
# The ones left out are left out for the same reason. by-uuid and by-label name
# the filesystem on the device, which mkfs writes and which moves to whatever
# disk an image is restored onto; by-path names the slot the device is plugged
# into, which is the enclosure's enumeration order rather than the device's own
# name and hands its link to the replacement when a disk is swapped. All of them
# survive a reboot and none of them identifies the device, so recording one
# would answer the wrong question stably.
PERSISTENT_LINK_DIRS = ("by-partuuid", "by-id")

# by-id links built from an identifier the device reports about itself, rather
# than from strings assembled around it.
#
# The distinction is what separates the links a real machine offers for one
# device. A namespace publishes its EUI or its UUID, and beside them two links
# built from the controller's model and serial, one of them with an index
# appended -- and that index counts the namespaces in the order they were found,
# which is the same kind of ordering the kernel name already is. A SCSI device's
# NAA designator arrives as a wwn- link, so the SCSI case needs no rule of its
# own.
_SELF_REPORTED_PREFIXES = ("wwn-", "nvme-eui.", "nvme-uuid.")


def _persistent_link_rank(link: str) -> int:
    """How much of the device's own identity a link carries, lower is better.

    A partition's by-partuuid link comes first because the identifier is
    written in the partition table on the device itself: it survives the disk
    being re-exported under another serial, which a hypervisor or an enclosure
    swap does and which renames every by-id link the partition has.
    """
    directory = os.path.basename(os.path.dirname(link))
    if directory == "by-partuuid":
        return 0
    if directory == "by-id" and os.path.basename(link).startswith(_SELF_REPORTED_PREFIXES):
        return 1
    return 2


def read_persistent_links(dev_root: str = DEV_ROOT) -> dict[str, list[str]]:
    """Every persistent /dev/disk link the host publishes, by kernel name.

    A kernel name is a position in one boot's enumeration order, not an
    identity: on the lab workers the disk the kernel calls sdb is the one the
    hypervisor calls drive-scsi0, and sda is drive-scsi2, so a host that probes
    its controllers in another order hands each kernel name to another disk.
    These links are built from what the device reports, and follow it.

    Every link is kept and not only the preferred one, because the side that
    records a device and the side that looks it up again run different code on
    different machines: a lookup matched against one side's ranking would miss
    a device that is present under another of its own names. Each device's list
    is ordered best first, so ``[0]`` is the one to write down.

    A missing directory is not a failure. A host whose udev publishes nothing
    has no persistent names, which every caller already handles, and failing
    here would take down a configure on a machine whose disks are all readable.
    Nothing is opened and no target is stat'd: the link is read and the name it
    ends in is recorded, so a link udev left behind when a device was removed
    lands under a key nobody asks about.
    """
    links: dict[str, list[str]] = {}
    for link_dir in PERSISTENT_LINK_DIRS:
        directory = os.path.join(dev_root, "disk", link_dir)
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            path = os.path.join(directory, name)
            try:
                target = os.readlink(path)
            except OSError:
                continue
            links.setdefault(os.path.basename(target), []).append(path)
    for paths in links.values():
        paths.sort(key=lambda p: (_persistent_link_rank(p), p))
    return links


def _partition_holders(disk_name: str, part_name: str) -> list[str]:
    """Holders of a single partition (/sys/block/<disk>/<part>/holders)."""
    import os
    try:
        return sorted(set(os.listdir(f"/sys/block/{disk_name}/{part_name}/holders")))
    except OSError:
        return []


def _root_disk_names() -> list[str]:
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


def get_block_devices_info() -> list[dict]:
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
    hostname = host_network.node_hostname()
    # Read once for the whole host: every device's links sit in the same two
    # directories, so a reading per device would walk them again for each disk.
    persistent = read_persistent_links()
    devices: list[dict] = []
    for dev in data.get("blockdevices", []):
        if dev.get("type") != "disk":
            continue
        name = dev.get("name", "")
        children = dev.get("children") or []
        by_id_paths = persistent.get(name, [])
        by_id_path = by_id_paths[0] if by_id_paths else ""
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
            "by_id_paths": by_id_paths,
            "numa_node": numa_node,
        })
        for child in children:
            if child.get("type") != "part":
                continue
            part_name = child.get("name", "")
            partuuid = (child.get("partuuid") or "").strip()
            part_paths = persistent.get(part_name, [])
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
                "by_id_path": part_paths[0] if part_paths else "",
                "by_id_paths": part_paths,
                "numa_node": numa_node,
            })
    logger.debug("function:get_block_devices_info end")
    return devices


SB_GPT_PARTITION_TYPECODE = "6527994e-2c5a-4eec-9613-8f5944074e8b"


ALCEML_STORAGE_SIGNATURE = b"ALCEML_STORAGE\x00"


def simplyblock_data_signature(device_name: str) -> bool | None:
    """Whether a block device carries simplyblock data.

    Every device a storage node uses -- a data device as well as a dedicated
    journal device, a whole disk as well as a partition -- starts with the
    alceml storage header, whose first field is the ``ALCEML_STORAGE``
    signature. Neither blkid nor lsblk know it, so a device in use looks like
    an empty, unpartitioned disk to both; this reads it directly.

    Returns True when the signature is present, False when the first block
    was read and carries no signature, and None when the device could not be
    read, which callers must treat like True: a device whose content is
    unknown is never partitioned."""
    try:
        with open(f"/dev/{device_name}", "rb", buffering=0) as handle:
            head = handle.read(4096)
    except OSError as e:
        logger.warning(f"cannot read the first block of /dev/{device_name}: {e}")
        return None
    return head.startswith(ALCEML_STORAGE_SIGNATURE)


def _logical_block_size(disk_name: str) -> int:
    """Logical sector size of a disk, in bytes: the unit sgdisk counts in.
    sysfs reports partition start and size in 512-byte units regardless."""
    try:
        size = int(_read_sysfs(f"/sys/block/{disk_name}/queue/logical_block_size") or 512)
    except ValueError:
        size = 512
    return size if size > 0 else 512


def split_partition_partner(part: dict, devices: list[dict]) -> tuple[dict, dict] | None:
    """The ``(journal, data)`` pair a previous split_partition_for_journal
    made of a partition, when ``part`` is one of the two; otherwise None.

    The journal partition (``sb_jm``) keeps the number, the start, and the
    PARTUUID of the partition that was split, so a re-configure that selects
    the original partition gets the journal partition. Its data partition
    (``sb_data``) is the simplyblock partition that starts right after it.
    Both carry the simplyblock GPT type. Raises ValueError for a simplyblock
    partition without its partner, which is never treated as a fresh
    partition: splitting it again would destroy the journal or the data."""
    fields = _partition_entry_fields(part["name"])
    if fields.get("type") != SB_GPT_PARTITION_TYPECODE or fields.get("name") not in ("sb_jm", "sb_data"):
        return None
    parent = part.get("parent_name", "")

    def _extent(name):
        try:
            start = int(_read_sysfs(f"/sys/block/{parent}/{name}/start"))
            size = int(_read_sysfs(f"/sys/block/{parent}/{name}/size"))
        except (ValueError, TypeError):
            return None
        return start, start + size

    own = _extent(part["name"])
    siblings = [d for d in devices
                if d.get("type") == "part" and d.get("parent_name") == parent
                and d.get("name") != part["name"]]
    want = "sb_data" if fields["name"] == "sb_jm" else "sb_jm"
    for sibling in siblings:
        extent = _extent(sibling["name"])
        if own is None or extent is None:
            continue
        adjacent = extent[0] == own[1] if want == "sb_data" else extent[1] == own[0]
        if not adjacent:
            continue
        sibling_fields = _partition_entry_fields(sibling["name"])
        if (sibling_fields.get("type") == SB_GPT_PARTITION_TYPECODE
                and sibling_fields.get("name") == want):
            return (part, sibling) if want == "sb_data" else (sibling, part)
    raise ValueError(
        f"partition {part['name']} is a simplyblock {fields['name']} partition, but its "
        f"{want} partner was not found next to it on {parent}; it is not split again")


def split_partition_for_journal(part_name: str, jm_bytes: int) -> tuple[dict, dict]:
    """Split an existing GPT partition into two: a journal partition of
    ``jm_bytes`` at its original start and a data partition covering the
    remainder. Used by lblk nodes running on partitions, where the journal
    can neither own a whole drive nor may we relabel one (the rest of the
    disk belongs to the OS or other software).

    The parent disk's partition table is modified ONLY within the bounds of
    the partition being split. The journal partition keeps the number and the
    PARTUUID of the original partition, so the selector the partition was
    chosen by (``/dev/disk/by-partuuid/...``, ``...-partN``) still answers
    and a re-configure recognises the split (split_partition_partner).
    Geometry is converted from the 512-byte units of sysfs to the disk's
    logical sectors, which sgdisk counts in, so 4K-native disks are split
    correctly. The partition must be idle (unmounted, no holders, not backing
    root) and must not carry simplyblock data. Returns ``(jm_device,
    data_device)`` -- the two new partitions' inventory dicts
    (get_block_devices_info shape). Raises ValueError on any precondition or
    tool failure.
    """
    import math

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
    if simplyblock_data_signature(part_name) is not False:
        raise ValueError(f"partition {part_name} carries simplyblock data or cannot be read; "
                         f"it is not split")
    parent = part.get("parent_name", "")
    if not parent:
        raise ValueError(f"cannot determine parent disk of {part_name}")

    # blkid -p probes the device itself. lsblk reads PTTYPE from the udev
    # database, which the storage-node container does not mount.
    out, _, rc = shell_utils.run_command(f"blkid -p -o value -s PTTYPE /dev/{parent}")
    if rc != 0 or out.strip() != "gpt":
        raise ValueError(
            f"disk {parent} has partition table {out.strip() or 'unknown'!r}; "
            f"splitting a partition for the journal requires GPT")

    sys_part = f"/sys/block/{parent}/{part_name}"
    try:
        part_number = int(_read_sysfs(f"{sys_part}/partition"))
        start_512 = int(_read_sysfs(f"{sys_part}/start"))
        size_512 = int(_read_sysfs(f"{sys_part}/size"))
    except (ValueError, TypeError):
        raise ValueError(f"cannot read geometry of {part_name} from sysfs")

    sector = _logical_block_size(parent)
    start_sector = start_512 * 512 // sector
    size_sectors = size_512 * 512 // sector
    # 1 MiB alignment of the journal size, in logical sectors.
    align = max(1, (1024 * 1024) // sector)
    jm_sectors = int(math.ceil(jm_bytes / sector / align) * align)
    end_sector = start_sector + size_sectors - 1
    data_start = start_sector + jm_sectors
    if data_start + align > end_sector:
        raise ValueError(
            f"partition {part_name} ({size_512 * 512} bytes) is too small "
            f"to split into a {jm_bytes}-byte journal plus a data partition")

    guid = f" -u {part_number}:{part['partuuid']}" if part.get("partuuid") else ""
    cmds = [
        f"sgdisk -d {part_number} /dev/{parent}",
        (f"sgdisk -a 1 -n {part_number}:{start_sector}:{data_start - 1} "
         f"-t {part_number}:{SB_GPT_PARTITION_TYPECODE} -c {part_number}:sb_jm{guid} /dev/{parent}"),
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

    # Identify the two new partitions by their start, in sysfs (512-byte) units.
    jm_start_512 = start_sector * sector // 512
    data_start_512 = data_start * sector // 512
    jm_name = data_name = ""
    try:
        for entry in os.listdir(f"/sys/block/{parent}"):
            if not entry.startswith(parent):
                continue
            e_start = _read_sysfs(f"/sys/block/{parent}/{entry}/start")
            if not e_start:
                continue
            if int(e_start) == jm_start_512:
                jm_name = entry
            elif int(e_start) == data_start_512:
                data_name = entry
    except OSError:
        pass
    if not jm_name or not data_name:
        raise ValueError(
            f"split of {part_name} completed but the new partitions were not "
            f"found on {parent} (journal at sector {start_sector}, data at "
            f"{data_start}, {sector}-byte sectors)")

    inventory = {d["name"]: d for d in get_block_devices_info()}
    if jm_name not in inventory or data_name not in inventory:
        raise ValueError(
            f"new partitions {jm_name}/{data_name} missing from inventory "
            f"after split of {part_name}")
    return inventory[jm_name], inventory[data_name]


def _partition_entry_fields(part_name: str) -> dict:
    """GPT entry name and type of one partition, probed from the device
    (``blkid -p``): lsblk reads PARTLABEL/PARTTYPE from the udev database,
    which the storage-node container does not mount."""
    out, _, rc = shell_utils.run_command(f"blkid -p -o export /dev/{part_name}")
    if rc != 0:
        return {}
    fields = {}
    for line in (out or "").splitlines():
        key, sep, value = line.partition("=")
        if sep:
            fields[key.strip()] = value.strip()
    return {"name": fields.get("PART_ENTRY_NAME", ""),
            "type": fields.get("PART_ENTRY_TYPE", "").lower()}


def journal_split_partitions(disk_name: str, devices: list[dict]) -> tuple[dict, dict] | None:
    """The ``(journal, data)`` partitions of a whole disk that
    partition_disk_for_journal laid out, or None.

    A disk qualifies only when it carries exactly two partitions, both of the
    simplyblock GPT type, labelled ``sb_jm`` and ``sb_data``. A disk with any
    other partition belongs, at least in part, to someone else, and is never
    taken for a previous journal split. ``devices`` is a
    get_block_devices_info() inventory of the host."""
    parts = [d for d in devices if d.get("type") == "part" and d.get("parent_name") == disk_name]
    if len(parts) != 2:
        return None
    jm = data = None
    for part in parts:
        entry = _partition_entry_fields(part["name"])
        if entry.get("type") != SB_GPT_PARTITION_TYPECODE:
            return None
        if entry.get("name") == "sb_jm":
            jm = part
        elif entry.get("name") == "sb_data":
            data = part
    if jm is None or data is None:
        return None
    return jm, data


def partition_disk_for_journal(disk_name: str, jm_bytes: int) -> tuple[dict, dict]:
    """Lay out an unpartitioned whole disk as a journal partition of
    ``jm_bytes`` followed by a data partition covering the rest.

    Used by lblk nodes that run on a single disk per journal, typically a
    RAID-protected LUN of a storage array, where a separate journal device is
    neither needed nor wanted. The disk must be idle (unmounted, no holders,
    not backing root) and carry no partition table. Sizes are given to sgdisk
    in MiB, so the layout is correct on 512-byte and on 4K-native disks, and
    both partitions start on a 1 MiB boundary. Returns ``(jm_device,
    data_device)`` in the get_block_devices_info() shape. Raises ValueError
    on any precondition or tool failure.
    """
    import math

    inventory = {d["name"]: d for d in get_block_devices_info()}
    disk = inventory.get(disk_name)
    if disk is None or disk.get("type") != "disk":
        raise ValueError(f"disk {disk_name} not found")
    if disk.get("mounted_in_subtree"):
        raise ValueError(f"disk {disk_name} is mounted (busy)")
    if disk.get("holders"):
        raise ValueError(f"disk {disk_name} is held by {disk['holders']} (busy)")
    if disk.get("is_root_disk"):
        raise ValueError(f"disk {disk_name} backs the root filesystem")
    if disk.get("has_partitions"):
        raise ValueError(f"disk {disk_name} already carries partitions")
    if simplyblock_data_signature(disk_name) is not False:
        raise ValueError(f"disk {disk_name} carries simplyblock data or cannot be read; "
                         f"it is not partitioned")

    jm_mib = math.ceil(jm_bytes / (1024 * 1024))
    # 1 MiB at the start for the GPT, 1 MiB at the end for its backup, and at
    # least 1 MiB of data.
    if (jm_mib + 3) * 1024 * 1024 > int(disk.get("size") or 0):
        raise ValueError(
            f"disk {disk_name} ({disk.get('size')} bytes) is too small to hold a "
            f"{jm_bytes}-byte journal plus a data partition")

    dev = f"/dev/{disk_name}"
    cmds = [
        f"sgdisk -o {dev}",
        f"sgdisk -n 1:0:+{jm_mib}M -t 1:{SB_GPT_PARTITION_TYPECODE} -c 1:sb_jm {dev}",
        f"sgdisk -n 2:0:0 -t 2:{SB_GPT_PARTITION_TYPECODE} -c 2:sb_data {dev}",
    ]
    for cmd in cmds:
        out, err, rc = shell_utils.run_command(cmd)
        if rc != 0:
            raise ValueError(f"{cmd} failed (rc={rc}): {err or out}")

    _, _, rc = shell_utils.run_command(f"partprobe {dev}")
    if rc != 0:
        _, err, rc = shell_utils.run_command(f"partx -u {dev}")
        if rc != 0:
            raise ValueError(f"failed to re-read partition table of {disk_name}: {err}")
    shell_utils.run_command("udevadm settle -t 5")

    # Identify the two partitions by their numbers: nvme1n1p1 and sdb1 alike.
    by_number = {}
    try:
        for entry in os.listdir(f"/sys/block/{disk_name}"):
            if not entry.startswith(disk_name):
                continue
            number = _read_sysfs(f"/sys/block/{disk_name}/{entry}/partition")
            if number:
                by_number[int(number)] = entry
    except (OSError, ValueError) as e:
        # Reported below as missing partitions.
        logger.warning("could not list the partitions of %s: %s", disk_name, e)
    jm_name, data_name = by_number.get(1, ""), by_number.get(2, "")
    if not jm_name or not data_name:
        raise ValueError(
            f"partitioning of {disk_name} completed but the journal and data "
            f"partitions were not found")

    inventory = {d["name"]: d for d in get_block_devices_info()}
    if jm_name not in inventory or data_name not in inventory:
        raise ValueError(
            f"new partitions {jm_name}/{data_name} missing from inventory "
            f"after partitioning {disk_name}")
    return inventory[jm_name], inventory[data_name]


def wipe_block_device_signatures(device_name: str) -> tuple[bool, str]:
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
        logger.error(f"Failed to retrieve region: {e!s}")
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
        logger.error(f"Failed to detach EBS volumes: {e!s}")

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
        logger.error(f"Failed to attach EBS volumes: {e!s}")
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
        logger.error(f"Failed to get available device name: {e!s}")
        return None


# Device paths the SMART reader accepts: an absolute /dev path of plain name
# characters. The value comes from a request, and although run_command does
# not go through a shell, it is still split on whitespace and handed to a
# privileged tool -- so it is validated, not trusted.
_DEVICE_PATH_RE = re.compile(r"^/dev/[A-Za-z0-9._:/+-]+$")
# A partition carries no health log of its own; the disk it lives on does.
_NVME_PARTITION_RE = re.compile(r"^(nvme\d+n\d+)p\d+$")
_SD_PARTITION_RE = re.compile(r"^((?:sd|vd|xvd)[a-z]+)\d+$")

#: smartctl exit-status bits 0 and 1: the command line did not parse, or the
#: device could not be opened / its type not detected. Every higher bit
#: reports a finding ABOUT the disk (failing, prefail attributes, logged
#: errors) and comes with a complete JSON document, so only these two mean
#: "no health log was read".
_SMARTCTL_FATAL_BITS = 0x03


def _smart_target(device_path: str) -> str:
    """The whole-disk node whose health log describes ``device_path``."""
    real = os.path.realpath(device_path)
    name = os.path.basename(real)
    for pattern in (_NVME_PARTITION_RE, _SD_PARTITION_RE):
        m = pattern.match(name)
        if m:
            return os.path.join(os.path.dirname(real), m.group(1))
    return real


def _smart_unsupported(path: str, reason: str) -> dict:
    return {"supported": False, "source": "", "device_path": path,
            "data": None, "reason": reason}


def read_device_smart(device_path: str) -> dict:
    """Read a kernel block device's health log, raw, for the control plane.

    Kernel NVMe namespaces (``/dev/nvme*``) are read with ``nvme smart-log``
    (nvme-cli ships in the agent image); anything else with ``smartctl -a``
    when it is installed. A device that exposes no health log at all -- a
    virtio disk in a VM, or no smartctl -- is ``supported: False`` with the
    reason, not an error: it is a property of the device, and the caller
    reports it as such. The raw tool output is returned in ``data`` and
    mapped to the SPDK field names on the control-plane side, so one mapping
    serves both agent flavours.
    """
    if not _DEVICE_PATH_RE.match(device_path or "") or ".." in device_path:
        raise ValueError(f"invalid device path {device_path!r}")

    target = _smart_target(device_path)
    if os.path.basename(target).startswith("nvme"):
        out, err, rc = shell_utils.run_command(f"nvme smart-log -o json {target}")
        if rc == 0:
            try:
                return {"supported": True, "source": "nvme-cli", "device_path": target,
                        "data": json.loads(out), "reason": ""}
            except json.JSONDecodeError as e:
                logger.error("nvme smart-log %s: unparseable output: %s", target, e)
        else:
            logger.warning("nvme smart-log %s failed (rc=%s): %s", target, rc, err)
        # Fall through: smartctl reads NVMe too, and is the only other way in.

    if shutil.which("smartctl") is None:
        return _smart_unsupported(target, "smartctl is not installed on the storage node")

    out, err, rc = shell_utils.run_command(f"smartctl -j -a {target}")
    try:
        data = json.loads(out) if out else None
    except json.JSONDecodeError:
        data = None
    if data is None or rc & _SMARTCTL_FATAL_BITS:
        messages = [m.get("string", "") for m in
                    ((data or {}).get("smartctl", {}).get("messages") or [])]
        return _smart_unsupported(
            target, "; ".join(m for m in messages if m) or f"smartctl failed (rc={rc})")
    if (data.get("smart_support") or {}).get("available") is False:
        return _smart_unsupported(target, "device does not support SMART")
    return {"supported": True, "source": "smartctl", "device_path": target,
            "data": data, "reason": ""}
