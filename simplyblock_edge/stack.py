# coding=utf-8
"""Pure bdev-stack planner for edge clusters (docs/edge_clusters_spec.md §4).

Every name is deterministically derived from the persisted records, so stack
assembly is idempotent and a node's stack can be reconstructed after any
restart from the EdgeNode/EdgeVolume rows alone. No RPC or DB access here —
the ops layer executes plans.

Local stack rule (per node):
    1 partition  -> the aio bdev itself
    2 partitions -> raid1  over the aio bdevs
    3+           -> raid5f over the aio bdevs

Cross-node mirror (2-node clusters): every node exposes its local top via an
internal replication subsystem; the primary attaches the peer's and builds a
raid1 of [local_top, remote leg]. Single-node clusters skip the mirror.
"""
from dataclasses import dataclass, field
from typing import List, Optional

from simplyblock_edge import constants as edge_constants


def _short(uuid: str) -> str:
    return uuid.split('-')[0]


# ------------------------------------------------------------------- naming

def aio_bdev_name(node_uuid: str, index: int) -> str:
    return f"ea_{_short(node_uuid)}_{index}"


def local_raid_name(node_uuid: str) -> str:
    return f"el_{_short(node_uuid)}"


def repl_nqn(cluster_nqn: str, node_uuid: str) -> str:
    return f"{cluster_nqn}:edge-repl:{node_uuid}"


def remote_controller_name(peer_node_uuid: str) -> str:
    return f"er_{_short(peer_node_uuid)}"


def remote_leg_bdev(peer_node_uuid: str) -> str:
    # bdev_nvme_attach_controller names the namespace bdev "<name>n<nsid>".
    return f"{remote_controller_name(peer_node_uuid)}n1"


def mirror_name(cluster_id: str) -> str:
    return f"em_{_short(cluster_id)}"


def lvs_name(cluster_id: str) -> str:
    return f"elvs_{_short(cluster_id)}"


def volume_nqn(cluster_nqn: str, volume_uuid: str) -> str:
    return f"{cluster_nqn}:edge-lvol:{volume_uuid}"


def volume_bdev(cluster_id: str, volume_name: str) -> str:
    return f"{lvs_name(cluster_id)}/{volume_name}"


# --------------------------------------------------------------------- plans

@dataclass
class AioSpec:
    bdev_name: str
    device_path: str
    block_size: int = edge_constants.EDGE_AIO_BLOCK_SIZE


@dataclass
class RaidSpec:
    name: str
    raid_level: str                  # "1" or "5f"
    base_bdevs: List[str] = field(default_factory=list)
    strip_size_kb: int = 0           # raid5f only


@dataclass
class LocalStackPlan:
    """Per-node local stack: aio bdevs, optional local raid, resulting top."""
    aio_bdevs: List[AioSpec]
    raid: Optional[RaidSpec]
    top_bdev: str


@dataclass
class MirrorPlan:
    """Primary-side cross-node mirror."""
    remote_controller: str           # bdev_nvme_attach_controller name
    remote_nqn: str
    remote_addr: str
    remote_port: int
    remote_leg: str                  # resulting namespace bdev
    raid: RaidSpec                   # raid1 [local_top, remote_leg]
    top_bdev: str


def plan_local_stack(node) -> LocalStackPlan:
    """node: EdgeNode-shaped (uuid, partitions with device_path).

    aio bdev names are keyed by the partition's ORIGINAL index in
    node.partitions (removed slots are skipped but never re-numbered), so a
    partition's bdev name is stable for the node's lifetime — reassembly and
    replace flows depend on that."""
    parts = [(i, p) for i, p in enumerate(node.partitions) if p.status != 'removed']
    if not parts:
        raise ValueError(f"Edge node {node.uuid} has no usable partitions")

    aio_bdevs = [
        AioSpec(bdev_name=aio_bdev_name(node.uuid, i), device_path=p.device_path)
        for i, p in parts
    ]

    if len(aio_bdevs) == 1:
        return LocalStackPlan(aio_bdevs=aio_bdevs, raid=None,
                              top_bdev=aio_bdevs[0].bdev_name)

    if len(aio_bdevs) == 2:
        raid = RaidSpec(name=local_raid_name(node.uuid), raid_level="1",
                        base_bdevs=[a.bdev_name for a in aio_bdevs])
    else:
        raid = RaidSpec(name=local_raid_name(node.uuid), raid_level="5f",
                        base_bdevs=[a.bdev_name for a in aio_bdevs],
                        strip_size_kb=edge_constants.EDGE_RAID5_STRIP_SIZE_KB)
    return LocalStackPlan(aio_bdevs=aio_bdevs, raid=raid, top_bdev=raid.name)


def plan_mirror(cluster_id: str, cluster_nqn: str, primary, secondary) -> MirrorPlan:
    """Primary-side plan mirroring the primary's local top with the secondary's
    replication subsystem. primary/secondary: EdgeNode-shaped."""
    local_top = plan_local_stack(primary).top_bdev
    leg = remote_leg_bdev(secondary.uuid)
    return MirrorPlan(
        remote_controller=remote_controller_name(secondary.uuid),
        remote_nqn=repl_nqn(cluster_nqn, secondary.uuid),
        remote_addr=secondary.get_data_ip(),
        remote_port=secondary.repl_port,
        remote_leg=leg,
        raid=RaidSpec(name=mirror_name(cluster_id), raid_level="1",
                      base_bdevs=[local_top, leg]),
        top_bdev=mirror_name(cluster_id),
    )


def lvstore_base_bdev(cluster_id: str, node_count: int, primary) -> str:
    """Where the lvstore sits: on the mirror for 2-node clusters, directly on
    the primary's local top for single-node clusters (spec §4.3)."""
    if node_count >= 2:
        return mirror_name(cluster_id)
    return plan_local_stack(primary).top_bdev
