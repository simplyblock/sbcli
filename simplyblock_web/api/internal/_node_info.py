"""Static, process-lifetime-constant identity info for a storage node.

Gathering this is not free: ``cpuinfo.get_cpu_info()`` parses ``/proc/cpuinfo``,
``hostname``/``dmidecode`` shell out, and cloud-metadata detection
(:func:`get_cloud_info`) makes network requests to well-known metadata
IPs/hostnames that block for their client timeout whenever the node isn't
actually on that cloud.

None of it changes for the life of the process, so it is gathered lazily on
first use and memoized with ``lru_cache`` rather than at import time.
``node_api_basic.py``, ``storage_node/docker.py`` and
``storage_node/kubernetes.py`` are imported transitively by almost anything
that touches ``simplyblock_web`` (test collection, the CLI, OpenAPI-schema
generation, ...), so computing this at module scope paid the network-probe cost
there regardless of whether a ``/info`` request was ever served -- that's what
made test collection slow. ``cpuinfo.get_cpu_info()`` additionally spawns a
child that probes CPUID by executing machine code from an mmap'd page; on
kernels that enforce W^X that child dies with SIGSEGV. py-cpuinfo expects this
and falls back to ``/proc/cpuinfo``, but each occurrence still costs a
multi-megabyte core dump, so it should not happen once per import.
"""
import functools
import os
from typing import Any, TypedDict
from collections.abc import Callable

import cpuinfo
import requests

from simplyblock_core import shell_utils


class StaticNodeInfo(TypedDict):
    cpu_info: dict[str, Any]
    hostname: str
    system_id: str
    cloud_info: dict[str, Any]


@functools.lru_cache(maxsize=1)
def get_static_node_info() -> StaticNodeInfo:
    """Gather and cache this node's static identity info.

    Computed on first call, not at import time. Set ``WITHOUT_CLOUD_INFO`` to
    skip the cloud-metadata probe entirely (useful in deployments where it's
    known not to apply, avoiding up to a few seconds of network timeout on
    the first call after boot).
    """
    hostname, _, _ = shell_utils.run_command("hostname -s")
    system_id, _, _ = shell_utils.run_command("dmidecode -s system-uuid")

    cloud_info: dict[str, Any] = {}
    if not os.environ.get("WITHOUT_CLOUD_INFO"):
        cloud_info = get_cloud_info() or {}
        if cloud_info:
            system_id = cloud_info["id"]

    return StaticNodeInfo(
        cpu_info=cpuinfo.get_cpu_info(),
        hostname=hostname,
        system_id=system_id,
        cloud_info=cloud_info,
    )


def get_cloud_info() -> dict | None:
    getters: list[Callable[[], dict | None]] = [_google_info, _amazon_info, _equinix_info]
    return next((
        info
        for getter in getters
        if (info := getter()) is not None
    ), None)


def _google_info() -> dict | None:
    try:
        headers = {'Metadata-Flavor': 'Google'}
        response = requests.get("http://169.254.169.254/computeMetadata/v1/instance/?recursive=true", headers=headers, timeout=2)
        data = response.json()
        return {
            "id": str(data["id"]),
            "type": data["machineType"].split("/")[-1],
            "cloud": "google",
            "ip": data["networkInterfaces"][0]["ip"],
            "public_ip": data["networkInterfaces"][0]["accessConfigs"][0]["externalIp"],
        }
    except Exception:
        return None


def _amazon_info() -> dict | None:
    try:
        import ec2_metadata
        session = requests.session()
        data = ec2_metadata.EC2Metadata(session=session).instance_identity_document  # type: ignore[call-arg]
        return {
            "id": data["instanceId"],
            "type": data["instanceType"],
            "cloud": "amazon",
            "ip": data["privateIp"],
            "public_ip": "",
        }
    except Exception:
        return None


def _equinix_info(timeout: int = 2) -> dict | None:
    try:
        response = requests.get("https://metadata.platformequinix.com/metadata", timeout=2)
        data = response.json()
        public_ip = ""
        ip = ""
        for interface in data["network"]["addresses"]:
            if interface["address_family"] == 4:
                if interface["enabled"] and interface["public"]:
                    public_ip = interface["address"]
                elif interface["enabled"] and not interface["public"]:
                    public_ip = interface["address"]
        return {
            "id": str(data["id"]),
            "type": data["class"],
            "cloud": "equinix",
            "ip": ip,
            "public_ip": public_ip
        }
    except Exception:
        return None
