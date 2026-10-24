"""Network checks the SPDK proxy answers for itself.

The proxy runs in the SPDK pod's network namespace, so these see the interfaces Multus
attached to the pod, which the node agent in the host's namespace cannot. Each name and
address is checked before it reaches /sys or a subprocess, because it arrives over the
network."""

import ipaddress
import re
import subprocess
from pathlib import Path

_IFNAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,14}$")
_SYSFS = Path("/sys/class/net")


def link_state(ifname: str, sysfs: Path = _SYSFS) -> bool:
    """Whether the interface exists in this namespace and has carrier."""
    if not _IFNAME.match(ifname):
        return False
    try:
        return (sysfs / ifname / "carrier").read_text().strip() == "1"
    except OSError:
        return False


def ping(ifname: str, ip: str) -> bool:
    """Whether the address answers a ping sent out of the interface."""
    if not _IFNAME.match(ifname):
        return False
    try:
        ipaddress.IPv4Address(ip)
    except ValueError:
        return False
    result = subprocess.run(
        ["ping", "-c", "3", "-W", "1", "-I", ifname, ip],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return result.returncode == 0
