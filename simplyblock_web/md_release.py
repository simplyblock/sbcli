"""Stopping the software-RAID arrays nothing uses, so the configure can take their members.

A device a storage node is to take may be a member of an MD RAID array: an
earlier deployment built one, or the machine's provisioning did. While the array
is assembled the member is held, and every later step refuses it -- the lblk
eligibility filter as "held by md0", and an NVMe controller cannot be handed to
SPDK under an array without destroying it.

The operator's discovery drafts such a member only when the array is unused,
and says in the draft that the deployment stops it. This is where that happens:
on the worker, before the configuration is generated. It is checked again here,
on the live host, because the discovery report is older than this run and a
reboot or an administrator may have changed the answer since.

An array is unused when nothing is built on it, nothing mounts it or swaps on
it, the kernel hands it over exclusively, and it carries no signature -- a
filesystem nobody has mounted yet is still somebody's filesystem. It is stopped
only when every member is a device this configure takes, because stopping it
otherwise takes a disk out of an array for a deployment that never asked for
that disk. An array that fails any of this is left as it is, and its members
are refused by the generation exactly as before.

Stopping is not enough on its own. The members keep their RAID superblocks, and
the next boot or the next udev event assembles the array again over whatever
the storage node wrote. So the superblocks are erased, by wipefs and of the RAID
type alone, and the whole pass repeats when udev reassembled the array between
the stop and the wipe.
"""
import errno
import glob
import logging
import os
import re
import subprocess

logger = logging.getLogger(__name__)

# How often the pass repeats when an array came back between the stop and the
# wipe. udev assembles incrementally on the change event a stop produces, so a
# second pass is the ordinary race and a third is the margin.
_ATTEMPTS = 3

# A namespace path under a controller when native multipath is on: nvme1c1n1 is
# controller 1's path to namespace 1 of subsystem 1, and the block device a
# configure names is the head, nvme1n1.
_MULTIPATH_PATH = re.compile(r"^(nvme\d+)c\d+(n\d+)$")
_NAMESPACE = re.compile(r"^nvme\d+n\d+$")


class ReleaseError(RuntimeError):
    """An array this configure decided to stop is still holding its members."""


class Host:
    """The host the configure runs on, as the release reads and changes it.

    The paths are the host's own: the init container mounts the host's /sys and
    /dev, and runs in the host's PID namespace, so /proc/1 is the host's init
    and its mount table is the host's.
    """

    def __init__(self, sysfs: str = "/sys", proc: str = "/proc", dev: str = "/dev"):
        self.sysfs, self.proc, self.dev = sysfs, proc, dev

    def block(self, name: str, *parts: str) -> str:
        return os.path.join(self.sysfs, "class", "block", name, *parts)

    def entries(self, name: str, *parts: str) -> list[str]:
        try:
            return sorted(os.listdir(self.block(name, *parts)))
        except OSError:
            return []

    def is_array(self, name: str) -> bool:
        return os.path.isdir(self.block(name, "md"))

    def mounted_devices(self) -> set[str]:
        """The major:minor of every device the host has mounted."""
        devices = set()
        with open(os.path.join(self.proc, "1", "mountinfo")) as mountinfo:
            for line in mountinfo:
                fields = line.split()
                if len(fields) > 2:
                    devices.add(fields[2])
        return devices

    def swap_devices(self) -> set[str]:
        with open(os.path.join(self.proc, "swaps")) as swaps:
            return {line.split()[0] for line in swaps.readlines()[1:] if line.strip()}

    def dev_number(self, name: str) -> str:
        with open(self.block(name, "dev")) as dev:
            return dev.read().strip()

    def probe_content(self, name: str) -> str:
        """The type of the first signature blkid finds on the device, or empty.

        Low-level probing (-p) reads the device itself rather than the udev
        cache, which may predate a wipe or a mkfs.
        """
        result = subprocess.run(
            ["blkid", "-p", "-o", "export", os.path.join(self.dev, name)],
            capture_output=True, text=True, check=False)
        if result.returncode == 2:  # blkid's "nothing found"
            return ""
        if result.returncode != 0:
            raise ReleaseError(f"blkid could not read /dev/{name}: {result.stderr.strip()}")
        for line in result.stdout.splitlines():
            key, _, value = line.partition("=")
            if key in ("TYPE", "PTTYPE"):
                return value
        return "unknown"

    def exclusive_free(self, name: str) -> bool:
        try:
            fd = os.open(os.path.join(self.dev, name), os.O_RDONLY | os.O_EXCL)
        except OSError as e:
            if e.errno == errno.EBUSY:
                return False
            raise
        os.close(fd)
        return True

    def stop_array(self, name: str) -> None:
        """Stop the array, which is what mdadm --stop does; the image carries
        no mdadm, and the md driver takes the same request through sysfs."""
        with open(self.block(name, "md", "array_state"), "w") as state:
            state.write("clear")

    def wipe_raid_superblock(self, name: str) -> None:
        subprocess.run(
            ["wipefs", "--all", "--types", "linux_raid_member", os.path.join(self.dev, name)],
            capture_output=True, text=True, check=True)


def selected_block_devices(host: Host, pci_addresses: list[str], blk_paths: list[str]) -> list[str]:
    """The kernel names of the block devices a configure takes.

    A PCI address stands for every namespace of its controller, because a
    controller is handed to SPDK whole. A persistent /dev/disk name stands for
    the device it resolves to now, which is the device the generation is about
    to open.
    """
    names = set()
    for address in pci_addresses:
        if len(address.split(":")[0]) != 4:
            address = f"0000:{address}"
        pattern = os.path.join(host.sysfs, "bus", "pci", "devices", address, "nvme", "nvme*", "nvme*")
        for entry in glob.glob(pattern):
            base = os.path.basename(entry)
            multipath = _MULTIPATH_PATH.match(base)
            if multipath:
                names.add(multipath.group(1) + multipath.group(2))
            elif _NAMESPACE.match(base):
                names.add(base)
    for blk_path in blk_paths:
        names.add(os.path.basename(os.path.realpath(blk_path)))
    return sorted(names)


def _why_in_use(host: Host, array: str, selected: set[str]) -> str:
    """Why the array must be left alone, or empty when nothing uses it."""
    if host.entries(array, "holders"):
        return f"{', '.join(host.entries(array, 'holders'))} is built on it"
    if host.dev_number(array) in host.mounted_devices():
        return "it is mounted"
    if os.path.join(host.dev, array) in host.swap_devices():
        return "it is an active swap area"
    content = host.probe_content(array)
    if content:
        return f"it carries {content}"
    if not host.exclusive_free(array):
        return "the kernel refused an exclusive open"
    outside = [m for m in host.entries(array, "slaves") if m not in selected]
    if outside:
        return f"its members {', '.join(outside)} are not devices this node takes"
    return ""


def _held_arrays(host: Host, selected: list[str]) -> list[str]:
    return sorted({holder for name in selected for holder in host.entries(name, "holders")
                   if host.is_array(holder)})


def release_idle_arrays(selected: list[str], host: Host | None = None) -> list[str]:
    """Stop every unused array holding one of the selected devices and erase
    its members' RAID superblocks. Returns the arrays it stopped.

    Raises ReleaseError when an array it stopped still holds a member after
    every attempt: the generation would refuse that member, and a failure here
    names the array rather than the holder the generation would.
    """
    host = host or Host()
    chosen = set(selected)
    released: list[str] = []
    for _ in range(_ATTEMPTS):
        to_stop = []
        for array in _held_arrays(host, selected):
            why = _why_in_use(host, array, chosen)
            if why:
                logger.info(f"Left the software-RAID array {array} assembled: {why}")
                continue
            to_stop.append(array)
        if not to_stop:
            return released
        for array in to_stop:
            members = host.entries(array, "slaves")
            logger.info(f"Stopping the software-RAID array {array}, which nothing uses, "
                        f"to take its members {', '.join(members)}")
            host.stop_array(array)
            still_held = [m for m in members if array in host.entries(m, "holders")]
            if still_held:
                raise ReleaseError(
                    f"the software-RAID array {array} still holds {', '.join(still_held)} after it was stopped")
            for member in members:
                host.wipe_raid_superblock(member)
            if array not in released:
                released.append(array)
    returned = [a for a in _held_arrays(host, selected) if not _why_in_use(host, a, chosen)]
    if returned:
        raise ReleaseError(
            f"the software-RAID arrays {', '.join(returned)} were assembled again "
            f"after {_ATTEMPTS} attempts to stop them")
    return released
