"""The networks an SPDK pod attaches to, as the operator states them, and what
the pod spec needs for them: the annotation Multus reads. A pod with none stays on
the host network."""

import json

from tenacity import RetryError, Retrying, retry_if_result, stop_after_delay, wait_fixed

from simplyblock_core.storage_network import MANAGEMENT_INTERFACE, NetworkAttachment

NETWORK_STATUS = "k8s.v1.cni.cncf.io/network-status"
#: What Kubernetes expands to the pod's own address in a container env value.
POD_IP_REFERENCE = "$(POD_IP)"
#: Below SNodeClient's 300 s request timeout, so its retry never lands on a start that is still waiting.
REPORT_TIMEOUT_SEC = 240
POLL_INTERVAL_SEC = 2


def annotation(attachments: list[NetworkAttachment]) -> str:
    """The value of the networks annotation, or "" when there are no attachments."""
    if not attachments:
        return ""
    return json.dumps([a.model_dump() for a in attachments])


def server_ip(attachments: list[NetworkAttachment]) -> str:
    """The address the SPDK proxy binds. The management network's, when there is one,
    and otherwise the pod's own, which only the pod knows and Kubernetes expands from
    POD_IP. "" when the pod has no attachments and the control plane's own value stands."""
    if not attachments:
        return ""
    for a in attachments:
        if a.interface == MANAGEMENT_INTERFACE and a.ips:
            return a.ips[0].split("/")[0]
    return POD_IP_REFERENCE


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
        interfaces.append({"name": a.interface, "ips": entry["ips"]})
    return {"pod_ip": pod.status.pod_ip, "interfaces": interfaces}


def wait_for_report(read_pod, attachments: list[NetworkAttachment],
                    timeout: float = REPORT_TIMEOUT_SEC) -> dict:
    """Poll the pod until it reports, or raise TimeoutError naming the interfaces it
    never held."""
    try:
        return Retrying(
            retry=retry_if_result(lambda result: result is None),
            stop=stop_after_delay(timeout), wait=wait_fixed(POLL_INTERVAL_SEC),
        )(lambda: report(read_pod(), attachments))
    except RetryError as e:
        raise TimeoutError(
            "the SPDK pod did not hold interface(s) "
            + ", ".join(a.interface for a in attachments) + f" within {timeout:.0f}s") from e
