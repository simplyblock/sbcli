"""What an SPDK pod on attached networks reports, turned into the records the control
plane keeps for the node: the address its RPC is served on and its data NICs.

The pod's interfaces exist only in its own network namespace, so nothing the node agent
reads from the host describes them. The agent's start call returns what the pod holds.
"""

import uuid

from simplyblock_core.models.iface import IFace
from simplyblock_core.utils import get_mgmt_ip

MANAGEMENT_INTERFACE = "mgmt0"
ATTACHED_NET_TYPE = "attached"


def from_report(report: dict) -> tuple[str, list[dict]]:
    """The SPDK RPC address and the data NICs of the node, from the pod's report.

    The RPC address is the management interface's, and the pod's own when no management
    network was declared. The data NICs are the other interfaces, and the management
    interface carries the data when there are none. An attached interface is TCP: it is
    a kernel interface in the pod's namespace and nothing here probes it for RoCE."""
    interfaces = report.get("interfaces") or []
    if not interfaces:
        raise ValueError("the SPDK pod reported no interfaces")

    management = next((i for i in interfaces if i["name"] == MANAGEMENT_INTERFACE), None)
    data = [i for i in interfaces if i["name"] != MANAGEMENT_INTERFACE] or ([management] if management else [])

    spdk_ip = management["ips"][0] if management else report["pod_ip"]
    nics = [{
        "uuid": str(uuid.uuid4()),
        "if_name": i["name"],
        "ip4_address": i["ips"][0],
        "status": "Up",
        "net_type": ATTACHED_NET_TYPE,
        "trtype": "TCP",
    } for i in data]
    return spdk_ip, nics


def apply_report(snode, report: dict) -> None:
    """Refresh the node's RPC address and data NICs from a pod's report. A NIC keeps
    its identity across restarts, because the records that point at it are keyed by it."""
    spdk_ip, nics = from_report(report)
    known = {nic.if_name: nic.uuid for nic in snode.data_nics}
    snode.spdk_ip = spdk_ip
    snode.data_nics = [IFace({**nic, "uuid": known.get(nic["if_name"], nic["uuid"])}) for nic in nics]


def host_mgmt(node_info: dict, iface_name: str, networks) -> tuple[str, str] | None:
    """The worker's address and the interface it is on, for the node's identity.

    A node on the host network names the interface the agent reads it from. With attached
    networks the agent runs in the pod network and cannot see the host's interfaces, so it
    reports the host's address itself, and no interface is named."""
    if networks:
        host_ip = node_info.get("host_ip")
        return (host_ip, iface_name or "") if host_ip else None
    return get_mgmt_ip(node_info, iface_name)
