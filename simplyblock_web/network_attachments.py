"""The networks an SPDK pod attaches to, as the operator states them, and what
the pod spec needs for them: the annotation Multus reads and the device-plugin
resources the SPDK container requests. A pod with none stays on the host network."""

import json

from pydantic import BaseModel, Field

NETWORKS_ANNOTATION = "k8s.v1.cni.cncf.io/networks"


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
