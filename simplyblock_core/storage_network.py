"""The networks an SPDK pod attaches to, and what the pod reports once it holds them,
turned into the records the control plane keeps for the node: the address its RPC is
served on and its data NICs.

The pod's interfaces exist only in its own network namespace, so nothing the node agent
reads from the host describes them. The agent's start call returns what the pod holds.
"""

import ipaddress
import uuid
from collections.abc import Sequence
from typing import Annotated

from pydantic import AfterValidator, BaseModel, Field

from simplyblock_core.models.iface import IFace
from simplyblock_core.utils import K8S_DNS_SUBDOMAIN

#: The attachment that carries SPDK RPC; the pod's own address is used when none is named so.
MANAGEMENT_INTERFACE = "mgmt0"
ATTACHED_NET_TYPE = "attached"


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

    The RPC address is the management interface's, and the pod's own when no management
    network was declared. The data NICs are the other interfaces, and the management
    interface carries the data when there are none. An attached interface is TCP: it is
    a kernel interface in the pod's namespace and nothing here probes it for RoCE. A NIC
    in ``known`` keeps its uuid, because the records that point at it are keyed by it."""
    interfaces = report.get("interfaces") or []
    if not interfaces:
        raise ValueError("the SPDK pod reported no interfaces")

    management = next((i for i in interfaces if i["name"] == MANAGEMENT_INTERFACE), None)
    data = [i for i in interfaces if i["name"] != MANAGEMENT_INTERFACE] or [management]
    identity = {nic.if_name: nic.uuid for nic in known}
    return (management["ips"][0] if management else report["pod_ip"]), [IFace({
        "uuid": identity.get(i["name"]) or str(uuid.uuid4()),
        "if_name": i["name"],
        "ip4_address": i["ips"][0],
        "status": "Up",
        "net_type": ATTACHED_NET_TYPE,
        "trtype": "TCP",
    }) for i in data]
