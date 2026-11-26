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
the stop and the wipe. Only the members read before the checks are wiped, so a
member the array gains during the teardown is never one this erases.

Between the stop and the last wipe nothing holds the members, so a run killed
there leaves superblocks no holder points at. The array's UUID is recorded under
the state directory before the stop, and the next run finishes the wipe on the
selected devices that carry it.

The selectors are read the way the generation reads them, because this runs
before the generation validates them: a PCI address only when it is one, and a
block device only by a persistent /dev/disk name that exists. Anything else
selects nothing here and is refused by the generation as before.
"""
import errno
import glob
import json
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
_PCI_ADDRESS = re.compile(r"^[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]$")

# The record of a teardown in progress, under the state directory: which array
# was stopped and the UUID its members' superblocks carry. Written before the
# stop and removed when every member is wiped.
_JOURNAL = "md-release.json"


class ReleaseError(RuntimeError):
    """An array this configure decided to stop is still holding its members."""


class Host:
    """The host the configure runs on, as the release reads and changes it.

    The paths are the host's own: the init container mounts the host's /sys and
    /dev, and runs in the host's PID namespace, so /proc/1 is the host's init
    and its mount table is the host's.
    """

    def __init__(self, sysfs: str = "/sys", proc: str = "/proc", dev: str = "/dev",
                 state: str = "/etc/simplyblock"):
        self.sysfs, self.proc, self.dev, self.state = sysfs, proc, dev, state

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

    def member_uuid(self, name: str) -> str:
        """The UUID of the array whose RAID superblock the device carries, or
        empty when it carries none."""
        result = subprocess.run(
            ["blkid", "-p", "-o", "export", os.path.join(self.dev, name)],
            capture_output=True, text=True, check=False)
        found = dict(line.partition("=")[::2] for line in result.stdout.splitlines())
        return found.get("UUID", "") if found.get("TYPE") == "linux_raid_member" else ""

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
        # The address goes into a glob, and a selector the generation would
        # match against nothing must not match every controller here.
        if not _PCI_ADDRESS.match(address):
            logger.warning(f"Not a PCI address, so no array is stopped for it: {address}")
            continue
        pattern = os.path.join(host.sysfs, "bus", "pci", "devices", address, "nvme", "nvme*", "nvme*")
        for entry in glob.glob(pattern):
            base = os.path.basename(entry)
            multipath = _MULTIPATH_PATH.match(base)
            if multipath:
                names.add(multipath.group(1) + multipath.group(2))
            elif _NAMESPACE.match(base):
                names.add(base)
    # The generation takes a persistent /dev/disk name and refuses the rest, and
    # it does so after this runs, so the same names are all this resolves.
    persistent = os.path.join(host.dev, "disk") + os.sep
    for blk_path in blk_paths:
        if not blk_path.startswith(persistent) or not os.path.exists(blk_path):
            logger.warning(f"Not a persistent device name that exists, so no array is stopped for it: {blk_path}")
            continue
        names.add(os.path.basename(os.path.realpath(blk_path)))
    return sorted(names)


def _why_in_use(host: Host, array: str, members: list[str], selected: set[str]) -> str:
    """Why the array must be left alone, or empty when nothing uses it."""
    outside = [m for m in members if m not in selected]
    if outside:
        return f"its members {', '.join(outside)} are not devices this node takes"
    if host.entries(array, "holders"):
        return f"{', '.join(host.entries(array, 'holders'))} is built on it"
    if host.dev_number(array) in host.mounted_devices():
        return "it is mounted"
    if array in {os.path.basename(swap) for swap in host.swap_devices()}:
        return "it is an active swap area"
    content = host.probe_content(array)
    if content:
        return f"it carries {content}"
    if not host.exclusive_free(array):
        return "the kernel refused an exclusive open"
    return ""


def _held_arrays(host: Host, selected: list[str]) -> list[str]:
    return sorted({holder for name in selected for holder in host.entries(name, "holders")
                   if host.is_array(holder)})


def _journal_path(host: Host) -> str:
    return os.path.join(host.state, _JOURNAL)


def _read_journal(host: Host) -> dict[str, str]:
    """The teardowns in progress, as array UUID to array name."""
    try:
        with open(_journal_path(host)) as journal:
            return dict(json.load(journal))
    except FileNotFoundError:
        return {}


def _write_journal(host: Host, entries: dict[str, str]) -> None:
    path = _journal_path(host)
    if not entries:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        return
    os.makedirs(host.state, exist_ok=True)
    with open(path + ".tmp", "w") as journal:
        json.dump(entries, journal)
        journal.flush()
        os.fsync(journal.fileno())
    os.replace(path + ".tmp", path)


def _finish_interrupted(host: Host, selected: list[str]) -> None:
    """Wipe what a teardown that was cut short left behind.

    Once an array is stopped nothing holds its members, so the superblocks a
    run did not get to wipe are found by the UUID the record kept, and only on
    a selected device nothing holds: a device carrying that UUID under an
    assembled array is the ordinary path's to judge, and one this node does not
    take is nobody's here.
    """
    pending = _read_journal(host)
    for uuid, array in list(pending.items()):
        for name in selected:
            if not host.entries(name, "holders") and host.member_uuid(name) == uuid:
                logger.info(f"Erasing the RAID superblock {name} kept after {array} was stopped")
                host.wipe_raid_superblock(name)
        del pending[uuid]
        _write_journal(host, pending)


def _wipe_members(host: Host, array: str, members: list[str]) -> bool:
    """Erase the members' superblocks. False when the array came back first,
    which the next attempt answers; any other failure is the configure's."""
    for member in members:
        try:
            host.wipe_raid_superblock(member)
        except subprocess.CalledProcessError:
            if host.entries(member, "holders"):
                return False
            raise
    return True


def release_idle_arrays(selected: list[str], host: Host | None = None) -> list[str]:
    """Stop every unused array holding one of the selected devices and erase
    its members' RAID superblocks. Returns the arrays it stopped.

    Raises ReleaseError when an unused array still holds a member after every
    attempt: the generation would refuse that member, and a failure here names
    the array rather than the holder the generation would.
    """
    host = host or Host()
    chosen = set(selected)
    released: list[str] = []
    _finish_interrupted(host, selected)
    for _ in range(_ATTEMPTS):
        to_stop = []
        for array in _held_arrays(host, selected):
            # The members are read once, before anything else is decided, and
            # they are the only devices this pass wipes: a member the array
            # gains after this is one nothing checked.
            members = host.entries(array, "slaves")
            why = _why_in_use(host, array, members, chosen)
            if why:
                logger.info(f"Left the software-RAID array {array} assembled: {why}")
                continue
            to_stop.append((array, members))
        if not to_stop:
            return released
        for array, members in to_stop:
            if host.entries(array, "slaves") != members:
                logger.info(f"The members of {array} changed while it was checked; checking it again")
                continue
            uuid = host.member_uuid(members[0])
            logger.info(f"Stopping the software-RAID array {array}, which nothing uses, "
                        f"to take its members {', '.join(members)}")
            pending = _read_journal(host)
            if uuid:
                _write_journal(host, {**pending, uuid: array})
            host.stop_array(array)
            if any(array in host.entries(m, "holders") for m in members):
                continue  # still assembled, or assembled again: the next attempt
            if not _wipe_members(host, array, members):
                continue
            if uuid:
                pending.pop(uuid, None)
                _write_journal(host, pending)
            if array not in released:
                released.append(array)
    returned = [a for a in _held_arrays(host, selected)
                if not _why_in_use(host, a, host.entries(a, "slaves"), chosen)]
    if returned:
        raise ReleaseError(
            f"the software-RAID arrays {', '.join(returned)} still hold their members "
            f"after {_ATTEMPTS} attempts to stop them")
    return released
