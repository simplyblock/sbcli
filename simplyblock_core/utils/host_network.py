"""The host's network, as the node agent sees it from the pod network.

The agent reports the host's interfaces, opens NVMe-oF connections, and pings
peers over the storage network, and all of that belongs to the host's network
namespace. On host networking the agent is in it. On the pod network
(design-superclusters.md P0-3), which lets two clusters' agents share a worker,
it is not, so every command about the network enters the host's namespace
through PID 1's, which the agent sees with host PID. Reading the namespace is
how the choice is made, so an agent still on host networking runs every
command exactly as before.

The interface facts the agent used to read from /sys/class/net and psutil are
read through `ip` instead, because a sysfs mount and psutil both show the
namespace the agent is in rather than the host's.
"""

import json
import os
import socket

from simplyblock_core.utils.shell import run_command

# HOST_NETWORK_NAMESPACE is the host's network namespace, as PID 1 holds it.
HOST_NETWORK_NAMESPACE = "/proc/1/ns/net"


def in_host_network() -> bool:
    """Whether this process is in the host's network namespace. One that
    cannot be read is taken to be, which is what an agent without host PID or
    off Linux has always assumed."""
    try:
        return os.readlink("/proc/self/ns/net") == os.readlink(HOST_NETWORK_NAMESPACE)
    except OSError:
        return True


def host_command(cmd: str) -> str:
    """The command, run in the host's network namespace."""
    if in_host_network():
        return cmd
    return f"nsenter --net={HOST_NETWORK_NAMESPACE} {cmd}"


def _links(cmd: str) -> list[dict]:
    out, _, rc = run_command(host_command(cmd))
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []


def underlying_interfaces(nic: str) -> list[str]:
    """The physical interfaces under a bridge, a bond, or a VLAN, or the
    interface itself, as the host's links name them. An interface the host
    does not have resolves to none."""
    links = {link["ifname"]: link for link in _links("ip -j -d link show")}

    def resolve(name: str, seen: set[str]) -> list[str]:
        if name not in links or name in seen:
            return []
        seen.add(name)
        ports = [port for port, link in links.items() if link.get("master") == name]
        if ports:
            return [iface for port in ports for iface in resolve(port, seen)]
        lower = links[name].get("link")
        if lower:
            return resolve(lower, seen)
        return [name]

    return resolve(nic, set())


def has_carrier(ifname: str) -> bool:
    """Whether the host's interface has a link partner."""
    links = _links(f"ip -j link show dev {ifname}")
    return bool(links) and "LOWER_UP" in links[0].get("flags", [])


def rdma_netdevs() -> set[str]:
    """The host's interfaces that sit on an RDMA device."""
    return {link["netdev"] for link in _links("rdma -j link show") if link.get("netdev")}


def node_hostname() -> str:
    """The Kubernetes node this agent serves. The operator sets HOSTNAME to it,
    because on the pod network the system hostname is the pod's."""
    return os.environ.get("HOSTNAME") or socket.gethostname()
