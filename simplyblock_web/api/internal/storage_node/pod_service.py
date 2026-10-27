"""The Service in front of an SPDK pod on the pod network.

The pod's own address changes on every restart, so peers and clients hold the
Service's cluster address instead, and the kernel initiator reconnects to it on its
own. A Service forwards only the ports it names, so the control plane publishes the
node's ports to it as they are allotted (``storage_network.published_ports``). The
Service is created before the pod, because its address is what the pod reports, and it
outlives every restart; only the node's removal deletes it.
"""

import logging

from kubernetes.client import (
    ApiException,
    V1ObjectMeta,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from simplyblock_core import constants
from simplyblock_core import utils as core_utils

logger = logging.getLogger(__name__)
logger.setLevel(constants.LOG_LEVEL)

#: The label the pod carries and the Service selects on, beside its app label: two
#: clusters in one namespace can share an RPC port.
CLUSTER_LABEL = "simplyblock.io/cluster"


def service_name(rpc_port: int, cluster_id: str) -> str:
    return f"snode-spdk-svc-{rpc_port}-{core_utils.first_six_chars(cluster_id)}"


def _ports(ports: list[int]) -> list[V1ServicePort]:
    return [V1ServicePort(name=f"p{port}", port=port, target_port=port, protocol="TCP")
            for port in sorted(set(ports))]


def ensure(core_v1, namespace: str, rpc_port: int, cluster_id: str, ports: list[int]) -> str:
    """Create the Service, or set the ports of the existing one, and return its cluster
    address. The RPC port is always among the ports, so the control plane reaches the
    proxy at the same address."""
    name = service_name(rpc_port, cluster_id)
    ports = sorted({rpc_port, *ports})
    try:
        existing = core_v1.read_namespaced_service(name, namespace)
    except ApiException as e:
        if e.status != 404:
            raise
        existing = None

    if existing is None:
        service = V1Service(
            metadata=V1ObjectMeta(name=name, namespace=namespace,
                                  labels={"role": "simplyblock-storage-node", CLUSTER_LABEL: cluster_id}),
            spec=V1ServiceSpec(
                type="ClusterIP",
                selector={"app": f"spdk-app-{rpc_port}", CLUSTER_LABEL: cluster_id},
                ports=_ports(ports),
            ),
        )
        created = core_v1.create_namespaced_service(namespace=namespace, body=service)
        logger.info("Service %s created at %s with ports %s", name, created.spec.cluster_ip, ports)
        return created.spec.cluster_ip

    held = sorted(p.port for p in (existing.spec.ports or []))
    if held != ports:
        core_v1.patch_namespaced_service(name, namespace, {"spec": {"ports": [
            {"name": p.name, "port": p.port, "targetPort": p.target_port, "protocol": p.protocol}
            for p in _ports(ports)]}})
        logger.info("Service %s ports %s -> %s", name, held, ports)
    return existing.spec.cluster_ip


def delete(core_v1, namespace: str, rpc_port: int, cluster_id: str) -> None:
    """Remove the Service; a missing one is already what was asked for."""
    name = service_name(rpc_port, cluster_id)
    try:
        core_v1.delete_namespaced_service(name, namespace)
    except ApiException as e:
        if e.status != 404:
            raise
        return
    logger.info("Service %s deleted", name)
