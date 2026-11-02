"""The network check the SPDK proxy answers for itself.

The proxy runs in the SPDK pod's network namespace, so it sees the interfaces Multus
attached to the pod, which the node agent in the host's namespace cannot. Each name and
address is checked before it reaches /sys or a subprocess, because it arrives over the
network."""

import ipaddress
import re
import subprocess
from pathlib import Path

_IFNAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,14}$")
_SYSFS = Path("/sys/class/net")
_PING_TIMEOUT_SEC = 10


def check(ifname: str, ip: str, sysfs: Path = _SYSFS) -> bool:
    """Whether the interface exists in this namespace with carrier and the address
    answers a ping sent out of it."""
    if not _IFNAME.match(ifname):
        return False
    try:
        ipaddress.IPv4Address(ip)
        if (sysfs / ifname / "carrier").read_text().strip() != "1":
            return False
        result = subprocess.run(
            ["ping", "-c", "3", "-W", "1", "-I", ifname, ip], check=False, timeout=_PING_TIMEOUT_SEC,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (ValueError, OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0
