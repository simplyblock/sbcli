"""Erasure-coding resource floors (product rules of 2026-11-09).

A cluster with the scheme n+k (``cluster.distr_ndcs`` = n, ``cluster.distr_npcs``
= k) has five FLOORS. They bind: operator approval and ``cluster activate``
refuse below a floor, and at runtime volume creation, planned node shutdown
and node removal refuse when the cluster AFTER the operation would be below
one. The status monitor reports DEGRADED while the online resources are below
a floor. An outage may take a running cluster below the floors; nothing is
suspended for that reason alone -- the data plane keeps serving and
migrating, and rebalancing restores the protection.

1. data devices per node >= 1
2. data devices (cluster-wide, online) >= n+k when the cluster declares
   ``raid_devices`` (every data device is protected below simplyblock), else
   >= n+2k
3. failure domains (when failure domains are used at all) >= n+k
4. nodes >= n+2k
5. journal devices are excluded from every device count

Topology overrides: a single-node cluster needs 1 node and n+k data devices;
a two-node (dual / arbitration) cluster needs 2 nodes.

Multi-attach array LUN clusters (``cluster.multi_attach``, always 1+0 on
``raid_devices``) replace the node floor by FTT+1 (at least 3 for FTT 2,
``cluster.max_fault_tolerance`` = FTT): an owner's LUN is reopened on one of
the surviving nodes. One data LUN per node is the recommended layout; only
>= 1 is enforced. The topology overrides do not apply to them.

Journal devices and the device model (floor 5)
----------------------------------------------
A node's journal lives in ``StorageNode.jm_device`` (a ``JMDevice``), never in
``StorageNode.nvme_devices``: ``_prepare_cluster_devices_jm_on_dev`` moves the
device flagged ``NVMeDevice.STATUS_JM`` out of ``nvme_devices`` into
``jm_device`` and ``_prepare_cluster_devices_on_restart`` drops a STATUS_JM
entry from the list again. The three journal shapes therefore count like this:

* partition journal nodes (``num_partitions_per_dev`` > 0, nvme and lblk
  journal-split alike): every device in ``nvme_devices`` is a data device or
  data partition; the journal partition(s) are only referenced from
  ``jm_device`` and do not count;
* ``--enable-journal-device`` / ``--lblk-journal-device`` nodes: the whole
  device that became the journal is ``jm_device`` and does not count;
* lblk journal-split nodes: the data partition is an ``nvme_devices`` entry
  (``is_partition`` True) and counts; the journal partition is ``jm_device``.

``count_data_devices`` nevertheless skips a device whose status is STATUS_JM
explicitly, for the window between flagging and preparation.
"""

from __future__ import annotations

from dataclasses import dataclass

from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

#: Device statuses that count as an online data device. The same set
#: ``_cluster_activate`` counts (READONLY and CANNOT_ALLOCATE devices still
#: hold their chunks and serve reads).
ONLINE_DEVICE_STATUSES = (
    NVMeDevice.STATUS_ONLINE,
    NVMeDevice.STATUS_READONLY,
    NVMeDevice.STATUS_CANNOT_ALLOCATE,
)


@dataclass(frozen=True)
class Floors:
    """The binding minimums of one cluster (or one volume scheme on it)."""

    nodes: int
    devices_per_node: int
    data_devices: int
    #: None when failure domains are not used on the cluster.
    failure_domains: int | None
    #: Human-readable formula behind each floor, for the violation text.
    nodes_rule: str = "n+2k"
    data_devices_rule: str = "n+2k"
    failure_domains_rule: str = "n+k"
    devices_per_node_rule: str = "data devices per node"


def _flag(obj, name: str) -> bool:
    """A boolean model flag read defensively: a MagicMock stand-in or a record
    that predates the field must read as False, never as truthy."""
    return getattr(obj, name, False) is True


def _int(value) -> int:
    """An integer model field read defensively (see ``_flag``): a stand-in
    that is not an int reads as 0, which no floor can be below."""
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def scheme(cluster, ndcs: int | None = None, npcs: int | None = None) -> tuple[int, int]:
    """``(n, k)``: the volume override when one is given, else the cluster's."""
    if ndcs or npcs:
        return _int(ndcs), _int(npcs)
    return _int(getattr(cluster, "distr_ndcs", 0)), _int(getattr(cluster, "distr_npcs", 0))


def is_single_node_cluster(cluster, member_nodes: int | None) -> bool:
    """Flagged single-node, activated as single-node, or one member."""
    return (_flag(cluster, "is_single_node")
            or _flag(cluster, "activated_single_node")
            or member_nodes == 1)


def is_two_node_cluster(cluster, member_nodes: int | None) -> bool:
    """A dual-node cluster is one whose whole membership is two nodes
    (``storage_node_ops.apply_jc_dual_node`` keys the JC dual-node flag on
    exactly that); ``cluster.two_node_arbitration`` is the arbitrated variant
    and only ever set on such a cluster."""
    return _flag(cluster, "two_node_arbitration") or member_nodes == 2


def cluster_floors(cluster, uses_failure_domains: bool, member_nodes: int | None = None,
                   ndcs: int | None = None, npcs: int | None = None) -> Floors:
    """The floors of ``cluster`` for the scheme n+k (the cluster's, or the
    volume override ``ndcs``/``npcs``).

    ``member_nodes`` is the number of primary storage nodes that belong to
    the cluster (every status but REMOVED): it decides the single-node and
    two-node topology overrides together with the cluster flags.
    ``uses_failure_domains`` is ``cluster.enable_failure_domain``.
    """
    n, k = scheme(cluster, ndcs, npcs)
    raid = _flag(cluster, "raid_devices")

    if raid:
        data_devices, data_rule = n + k, "n+k, raid_devices"
    else:
        data_devices, data_rule = n + 2 * k, "n+2k"
    nodes, nodes_rule = n + 2 * k, "n+2k"

    if _flag(cluster, "multi_attach"):
        ftt = _int(getattr(cluster, "max_fault_tolerance", 1))
        ftt = ftt if ftt in (1, 2) else 1
        return Floors(
            nodes=max(ftt + 1, 3 if ftt == 2 else 0),
            devices_per_node=1,
            data_devices=n + k,
            failure_domains=(n + k) if uses_failure_domains else None,
            nodes_rule=f"FTT+1, multi-attach FTT {ftt}",
            data_devices_rule="n+k, multi-attach",
            devices_per_node_rule=("data devices per node; multi-attach: exactly one "
                                   "data LUN per node is recommended"),
        )

    if is_single_node_cluster(cluster, member_nodes):
        nodes, nodes_rule = 1, "single-node cluster"
        # No redundancy to rebuild onto a spare: the stripe alone.
        data_devices, data_rule = n + k, "n+k, single-node cluster"
    elif is_two_node_cluster(cluster, member_nodes):
        nodes, nodes_rule = 2, "two-node cluster"

    return Floors(
        nodes=nodes,
        devices_per_node=1,
        data_devices=data_devices,
        failure_domains=(n + k) if uses_failure_domains else None,
        nodes_rule=nodes_rule,
        data_devices_rule=data_rule,
    )


def is_data_device(dev) -> bool:
    """An online data device: never the journal (floor 5, see the module
    docstring), only the statuses that hold and serve chunks."""
    status = getattr(dev, "status", None)
    if status == NVMeDevice.STATUS_JM:
        return False
    return status in ONLINE_DEVICE_STATUSES


def count_data_devices(nodes) -> tuple[int, dict[str, int]]:
    """``(total, per_node)`` of online, non-journal data devices on ``nodes``.

    Every node given is counted (the caller decides which nodes are "online"
    or "left after the operation"); a secondary-only node has no devices of
    its own and is skipped. ``per_node`` has an entry for EVERY counted node,
    zero included, so floor 1 (>= 1 data device per node) can be checked from
    it.
    """
    total = 0
    per_node: dict[str, int] = {}
    for node in nodes:
        if _flag(node, "is_secondary_node"):
            continue
        count = sum(1 for dev in (getattr(node, "nvme_devices", None) or [])
                    if is_data_device(dev))
        per_node[node.get_id()] = count
        total += count
    return total, per_node


def online_failure_domains(nodes) -> int:
    """Distinct failure-domain ids (>= 0; -1 means untagged) among ``nodes``."""
    return len({node.failure_domain for node in nodes
                if isinstance(getattr(node, "failure_domain", -1), int)
                and node.failure_domain >= 0})


def floor_violations(floors: Floors, online_nodes: int, data_devices: int,
                     per_node: dict[str, int], online_fds: int | None) -> list[str]:
    """Every floor the given online counts violate, as human-readable text
    naming the floor, e.g. ``"nodes online 3 < floor 4 (n+2k)"``. Empty when
    every floor holds. ``online_fds`` None means failure domains are not
    used (or not known) and floor 3 is not judged."""
    violations: list[str] = []
    if online_nodes < floors.nodes:
        violations.append(
            f"nodes online {online_nodes} < floor {floors.nodes} ({floors.nodes_rule})")
    for node_id, count in sorted(per_node.items()):
        if count < floors.devices_per_node:
            violations.append(
                f"node {node_id} has {count} online data devices < floor "
                f"{floors.devices_per_node} ({floors.devices_per_node_rule})")
    if data_devices < floors.data_devices:
        violations.append(
            f"data devices online {data_devices} < floor {floors.data_devices} "
            f"({floors.data_devices_rule})")
    if floors.failure_domains is not None and online_fds is not None \
            and online_fds < floors.failure_domains:
        violations.append(
            f"failure domains online {online_fds} < floor {floors.failure_domains} "
            f"({floors.failure_domains_rule})")
    return violations


def primary_members(snodes) -> list:
    """The cluster's primary storage nodes that are members: every status but
    REMOVED, secondary-only nodes left out."""
    return [n for n in snodes
            if n.status != StorageNode.STATUS_REMOVED and not _flag(n, "is_secondary_node")]


def online_members(snodes) -> list:
    return [n for n in primary_members(snodes) if n.status == StorageNode.STATUS_ONLINE]


def evaluate(cluster, snodes, exclude_node_id: str | None = None,
             ndcs: int | None = None, npcs: int | None = None,
             online_nodes=None) -> list[str]:
    """The floor violations of ``cluster`` judged on the ONLINE resources of
    ``snodes`` (every storage node of the cluster).

    ``exclude_node_id`` judges the cluster as it would be after that node
    left (a planned shutdown or removal): the node is left out of the online
    counts, but it stays a member for the topology overrides -- shutting one
    node of a three-node cluster down does not make it a two-node cluster.
    ``online_nodes`` replaces the ONLINE selection when the caller already
    narrowed it (the monitor leaves nodes it does not count out).
    """
    members = primary_members(snodes)
    if online_nodes is None:
        online_nodes = online_members(snodes)
    online = [n for n in online_nodes
              if n.get_id() != exclude_node_id and not _flag(n, "is_secondary_node")]
    uses_fd = _flag(cluster, "enable_failure_domain")
    floors = cluster_floors(cluster, uses_fd, member_nodes=len(members), ndcs=ndcs, npcs=npcs)
    total, per_node = count_data_devices(online)
    fds = online_failure_domains(online) if uses_fd else None
    return floor_violations(floors, len(online), total, per_node, fds)


def migration_affinity_floor(cluster) -> str:
    """The data plane's placement anti-affinity floor for a migration:
    ``"relaxed"`` on a cluster whose data devices are protected below
    simplyblock (``raid_devices``: several chunks of a stripe may share a
    device when no other device is writable), ``"device"`` otherwise."""
    return "relaxed" if _flag(cluster, "raid_devices") else "device"
