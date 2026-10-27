"""The networks an SPDK pod attaches to, and what the pod reports once it holds them,
turned into the records the control plane keeps for the node: the address its RPC is
served on and its data NICs.

The pod's interfaces exist only in its own network namespace, so nothing the node agent
reads from the host describes them. The agent's start call returns what the pod holds.

A pod can also serve on the pod network itself, with no attachment: the cluster CNI
gives it one interface and an address that changes on every restart. The agent then
fronts the pod with a Service, whose cluster address is what peers and clients hold
(``IFace.ip4_address``), while the target binds the pod's own address
(``IFace.listen_ip4``). A Service forwards the ports it names, so every port the node
serves on has to be published to it; ``published_ports`` is that set.
"""

import ipaddress
import time
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Annotated

from pydantic import AfterValidator, BaseModel, Field

from simplyblock_core import utils
from simplyblock_core.models.iface import IFace
from simplyblock_core.utils import K8S_DNS_SUBDOMAIN

if TYPE_CHECKING:
    from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

#: The attachment that carries SPDK RPC; the pod's own address is used when none is named so.
MANAGEMENT_INTERFACE = "mgmt0"
ATTACHED_NET_TYPE = "attached"
#: The NIC of a pod serving on the pod network: the CNI's interface, fronted by a Service.
POD_NET_TYPE = "pod"
POD_INTERFACE = "eth0"


def _address(value: str) -> str:
    ipaddress.ip_interface(value)
    return value


class NetworkAttachment(BaseModel):
    """One NetworkAttachmentDefinition the pod joins, as the operator states it. The
    node record keeps these as plain dicts, which is what the record's base model stores."""
    name: K8S_DNS_SUBDOMAIN
    namespace: Annotated[str, Field(pattern=r"^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*)?$",
                                    max_length=253)] = ""
    interface: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,14}$")
    ips: list[Annotated[str, AfterValidator(_address)]] = Field(default_factory=list)


def from_report(report: dict, known: Sequence[IFace] = ()) -> tuple[str, list[IFace]]:
    """The SPDK RPC address and the data NICs of the node, from the pod's report.

    With attachments, the RPC address is the management interface's, and the pod's own
    when no management network was declared. The data NICs are the other interfaces, and
    the management interface carries the data when there are none. An attached
    interface is TCP: it is a kernel interface in the pod's namespace and nothing here
    probes it for RoCE.

    On the pod network (a report with a ``service_ip`` and no interfaces), the RPC and
    the data are both served at the Service's address, and the one data NIC binds the
    pod's own address, which the next restart replaces.

    A NIC in ``known`` keeps its uuid, because the records that point at it are keyed
    by it."""
    interfaces = report.get("interfaces") or []
    identity = {nic.if_name: nic.uuid for nic in known}

    if not interfaces:
        service_ip = report.get("service_ip")
        if not service_ip:
            raise ValueError("the SPDK pod reported no interfaces")
        return service_ip, [IFace({
            "uuid": identity.get(POD_INTERFACE) or str(uuid.uuid4()),
            "if_name": POD_INTERFACE,
            "ip4_address": service_ip,
            "listen_ip4": report["pod_ip"],
            "status": "Up",
            "net_type": POD_NET_TYPE,
            "trtype": "TCP",
        })]

    management = next((i for i in interfaces if i["name"] == MANAGEMENT_INTERFACE), None)
    data = [i for i in interfaces if i["name"] != MANAGEMENT_INTERFACE] or [management]
    return (management["ips"][0] if management else report["pod_ip"]), [IFace({
        "uuid": identity.get(i["name"]) or str(uuid.uuid4()),
        "if_name": i["name"],
        "ip4_address": i["ips"][0],
        "status": "Up",
        "net_type": ATTACHED_NET_TYPE,
        "trtype": "TCP",
    }) for i in data]


def published_ports(node: "StorageNode") -> list[int]:
    """Every port the node's Service has to forward: its RPC, its device subsystems'
    and every lvstore it serves, as primary or secondary. Sorted, so two computations
    over the same record compare equal."""
    ports = {node.rpc_port, node.nvmf_port, node.lvol_subsys_port}
    if node.hublvol:
        ports.add(node.hublvol.nvmf_port)
    if node.transfer_hublvol:
        ports.add(node.transfer_hublvol.nvmf_port)
    for lvs_ports in (node.lvstore_ports or {}).values():
        ports.update(lvs_ports.values())
    return sorted(p for p in ports if p)


#: When the node's ports were last published by this process, by node id. Only a
#: rate limit for the monitor's repeat, which runs every few seconds: it has no
#: correctness role, so a process-local record is enough and another process
#: publishing too is harmless.
_published_at: dict[str, float] = {}


def publish_ports(node: "StorageNode", at_most_every: float = 0) -> None:
    """Hand the node's agent the ports its Service forwards. A no-op off the pod network.

    Called wherever the set changes: the node add, a restart, and every lvstore the node
    starts or stops serving. The storage-node monitor repeats it with ``at_most_every``
    seconds between attempts, so a call that fails here is caught up later rather than
    retried in place."""
    from simplyblock_core.snode_client import SNodeClientException

    if not node.pod_network:
        return
    if at_most_every and time.monotonic() - _published_at.get(node.get_id(), -at_most_every) < at_most_every:
        return
    _published_at[node.get_id()] = time.monotonic()
    ports = published_ports(node)
    try:
        _, err = node.client(timeout=30, retry=1).spdk_service_publish(node.rpc_port, node.cluster_id, ports)
    except SNodeClientException as e:
        err = str(e)
    if err:
        logger.warning("Node %s: the Service does not forward %s yet: %s", node.get_id(), ports, err)
