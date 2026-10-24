"""The networks an SPDK pod attaches to, as the operator states them, and what
the pod spec needs for them: the annotation Multus reads and the device-plugin
resources the SPDK container requests. A pod with none stays on the host network."""

import json
import time

from pydantic import BaseModel, Field

NETWORKS_ANNOTATION = "k8s.v1.cni.cncf.io/networks"
NETWORK_STATUS = "k8s.v1.cni.cncf.io/network-status"

# The interface the SPDK RPC is served on, when the cluster declares a management network.
MANAGEMENT_INTERFACE = "mgmt0"


class NetworkAttachment(BaseModel):
    name: str
    namespace: str = ""
    interface: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,14}$")
    ips: list[str] = Field(default_factory=list)
    # The device-plugin resource the definition names, such as an SR-IOV virtual
    # function. It is a request on the container and never part of the annotation.
    resource: str = ""


def annotation(attachments: list[NetworkAttachment]) -> str:
    """The value of the networks annotation, or "" when there are no attachments."""
    if not attachments:
        return ""
    return json.dumps([a.model_dump(exclude={"resource"}) for a in attachments])


def resources(attachments: list[NetworkAttachment]) -> dict[str, int]:
    """One unit of each named device-plugin resource per attachment that names it."""
    counts: dict[str, int] = {}
    for a in attachments:
        if a.resource:
            counts[a.resource] = counts.get(a.resource, 0) + 1
    return counts


def server_ip(attachments: list[NetworkAttachment]) -> str:
    """The address the SPDK proxy binds. The management network's, when there is one,
    and otherwise the pod's own, which only the pod knows and Kubernetes expands from
    POD_IP. "" when the pod has no attachments and the control plane's own value stands."""
    if not attachments:
        return ""
    for a in attachments:
        if a.interface == MANAGEMENT_INTERFACE and a.ips:
            return a.ips[0].split("/")[0]
    return "$(POD_IP)"


def report(pod, attachments: list[NetworkAttachment]) -> dict | None:
    """What the SPDK pod holds, for the control plane: its own address and the declared
    interfaces Multus attached, from the pod's network-status. None until the pod is
    running and holds every declared interface, because the annotation is written once
    when the attach finishes and a partial report would record the wrong addresses."""
    if pod.status.phase != "Running" or not pod.status.pod_ip:
        return None
    raw = (pod.metadata.annotations or {}).get(NETWORK_STATUS)
    if not raw:
        return None
    held = {entry.get("interface"): entry for entry in json.loads(raw)}

    interfaces = []
    for a in attachments:
        entry = held.get(a.interface)
        if entry is None or not entry.get("ips"):
            return None
        interfaces.append({"name": a.interface, "ips": entry["ips"], "mac": entry.get("mac", "")})
    return {"pod_ip": pod.status.pod_ip, "interfaces": interfaces}


def wait_for_report(read_pod, attachments: list[NetworkAttachment], timeout: float = 300,
                    sleep=time.sleep, now=time.monotonic) -> dict:
    """Poll the pod until it reports, or raise TimeoutError naming the interfaces it
    never held."""
    deadline = now() + timeout
    while True:
        result = report(read_pod(), attachments)
        if result is not None:
            return result
        if now() >= deadline:
            raise TimeoutError(
                "the SPDK pod did not hold interface(s) "
                + ", ".join(a.interface for a in attachments) + f" within {timeout:.0f}s")
        sleep(2)
