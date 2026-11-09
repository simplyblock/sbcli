"""Reaching a node agent whose name this site cannot resolve.

On a stretched control plane (docs/stretched-control-plane.md) the two sites
are two Kubernetes clusters. A storage node's API is named by its own cluster's
headless Service (``<host>.<service>.<namespace>.svc``), which the other
site's DNS does not serve, yet the leading control plane has to reach every
node of both sites directly. The node agent also listens on its worker's host
port 5000, and the worker's address is what the node record holds as
``mgmt_ip`` and what an add request carries as ``node_ip``; the shared VPC
routes it.

So when a name does not resolve, the TCP connection goes to that address
instead. Only the socket moves: urllib3 still wraps it in TLS for the original
name, so the certificate is verified against the name exactly as before. A
name that resolves is never touched, which keeps every single-site deployment
on the path it always took.
"""
import logging
import socket
import threading

import urllib3.util.connection as _urllib3_connection

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_registered: dict[str, str] = {}
_installed = False
_original_create_connection = _urllib3_connection.create_connection


def host_of(address: str) -> str:
    """The host part of ``host[:port]``, or of a URL."""
    host = address.split("://", 1)[-1].split("/", 1)[0]
    if host.startswith("["):
        return host[1:].split("]", 1)[0]
    return host.rsplit(":", 1)[0] if host.count(":") == 1 else host


def register(name: str, ip: str) -> None:
    """Reach ``name`` at ``ip`` when DNS does not resolve it; for a node whose
    record does not exist yet (an add in progress)."""
    name, ip = host_of(name), host_of(ip)
    if name and ip and name != ip:
        with _lock:
            _registered[name] = ip


def _from_records(name: str) -> str | None:
    """The ``mgmt_ip`` of the storage node whose API answers under ``name``."""
    from simplyblock_core.db_controller import DBController
    from simplyblock_core.models.storage_node import StorageNode
    try:
        nodes = DBController().get_storage_nodes()
    except Exception as e:  # noqa: BLE001 - a fallback, never a new failure
        logger.debug(f"peer address lookup for {name}: {e}")
        return None
    for node in nodes:
        if node.status == StorageNode.STATUS_REMOVED or not node.mgmt_ip:
            continue
        candidates = {host_of(node.api_endpoint or "")}
        # The node agent and the SPDK RPC proxy, each under its own headless
        # Service; both run on the worker (the proxy on the host network, the
        # agent on its hostPort), so mgmt_ip reaches either.
        for service in (node.snode_api_service, node.spdk_proxy_service):
            try:
                candidates.add(host_of(node._k8s_service_host(service)))
            except Exception:  # noqa: BLE001 - a record without Kubernetes names
                pass
        if name in candidates:
            return node.mgmt_ip
    return None


def lookup(name: str) -> str | None:
    with _lock:
        ip = _registered.get(name)
    return ip or _from_records(name)


def _create_connection(address, *args, **kwargs):
    try:
        return _original_create_connection(address, *args, **kwargs)
    except socket.gaierror:
        host, port = address[0], address[1]
        ip = lookup(host)
        if not ip:
            raise
        logger.info(f"{host} does not resolve here; connecting to its node address {ip}")
        return _original_create_connection((ip, port), *args, **kwargs)


def install() -> None:
    """Route connections to unresolvable node names through their addresses.
    Idempotent."""
    global _installed
    with _lock:
        if _installed:
            return
        _urllib3_connection.create_connection = _create_connection
        _installed = True
