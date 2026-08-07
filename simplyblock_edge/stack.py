# coding=utf-8
"""Pure bdev-stack planner for edge clusters (docs/edge_clusters_spec.md §4).

v3 (product adoption): 2-node clusters are ACTIVE/ACTIVE with the spdk-fork's
primary/secondary lvstore processing. Each node hosts its own lvstore; the
pairing node runs a live SECONDARY instance of it (lvol/snapshot/clone
creations are registered there; `bdev_lvol_update_lvstore` refreshes it;
leadership gates writes). Every lvol namespace exists on BOTH nodes with
ANA optimized (leader) / non-optimized (secondary) listeners.

Per-node layout (2-node cluster; node i, peer j):

    partitions -> aio bdevs -> local raid (1: bare aio, 2: raid1, 3+: raid5f)
    local_top  -> bdev_split(2) -> {local_top}p0  (own half)
                                   {local_top}p1  (peer half)
    repl subsystem (edge-repl:{i}) exposes ns1 = p0, ns2 = p1
    er_{j} controller on i -> er_{j}n1 (= j.p0), er_{j}n2 (= j.p1)

    mirror of store i   on node i (PRIMARY):   raid1[i.p0, er_{j}n2]
    mirror of store i   on node j (SECONDARY): raid1[j.p1, er_{i}n1]
    lvstore elvs_{i} on mirror em_{i}; role primary on i, secondary on j;
    leader = node i (normally).

Single-node clusters keep the flat layout: lvstore directly on the local top,
no split, no mirror.

Client ports are PER STORE (nvmf_port + store index) so fail-back can fence
one store's IO with a single nvmf_port_block without touching the other
store's traffic.

Everything here is pure naming/planning — no RPC or DB access. All names
derive deterministically from the records, so stack assembly is idempotent.
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


def own_half(local_top: str) -> str:
    """First split half: leg of the node's OWN store mirror (primary side)."""
    return f"{local_top}p0"


def peer_half(local_top: str) -> str:
    """Second split half: leg of the PEER's store mirror."""
    return f"{local_top}p1"


def repl_nqn(cluster_nqn: str, node_uuid: str) -> str:
    return f"{cluster_nqn}:edge-repl:{node_uuid}"


def remote_controller_name(peer_node_uuid: str) -> str:
    return f"er_{_short(peer_node_uuid)}"


def remote_half_bdev(peer_node_uuid: str, half: int) -> str:
    """Namespace bdev of the peer's exported half: ns1 = p0, ns2 = p1."""
    return f"{remote_controller_name(peer_node_uuid)}n{half}"


def mirror_name(store_node_uuid: str) -> str:
    """The mirror backing the store OWNED by store_node_uuid (instantiated on
    both nodes under the same name — the raid superblock ties them)."""
    return f"em_{_short(store_node_uuid)}"


def lvs_name(store_node_uuid: str) -> str:
    return f"elvs_{_short(store_node_uuid)}"


def store_client_port(base_port: int, store_index: int) -> int:
    """Per-store client port: fail-back fences exactly one store's IO."""
    return base_port + store_index


def volume_nqn(cluster_nqn: str, volume_uuid: str) -> str:
    return f"{cluster_nqn}:edge-lvol:{volume_uuid}"


def volume_bdev(store_node_uuid: str, volume_name: str) -> str:
    return f"{lvs_name(store_node_uuid)}/{volume_name}"


def crypto_bdev(volume_uuid: str) -> str:
    return f"ecr_{_short(volume_uuid)}"


def crypto_key_name(volume_uuid: str) -> str:
    return f"ekey_{_short(volume_uuid)}"


def volume_dek_path(cluster_id: str, volume_uuid: str) -> str:
    """KMS path for a volume's data encryption keys (AES_XTS key pair) —
    same layout as the hyperscale lvol DEKs."""
    return f"cluster/{cluster_id}/edge-volume/{volume_uuid}"


def cluster_kek_name(cluster_id: str) -> str:
    return f"edge-{cluster_id}"


# ---------------------------------------------------------------- cpu layout

@dataclass
class CpuLayout:
    """SPDK thread placement for 1-6 vCPUs (deploy-time choice):

        1 vCPU : app + lvs poller + nvmf poller all on core 0
        2 vCPU : app + lvs poller on core 0; nvmf poller on core 1
        3 vCPU : app on 0, lvs poller on 1, nvmf poller on 2
        4-6    : cores 3+ become ADDITIONAL nvmf poller cores
    """
    vcpus: int
    app_mask: int
    lvs_mask: int
    nvmf_mask: int

    @property
    def reactor_mask(self) -> int:
        return (1 << self.vcpus) - 1

    @staticmethod
    def hex(mask: int) -> str:
        return f"0x{mask:X}"


def plan_cpu_layout(vcpus: int) -> CpuLayout:
    if not 1 <= vcpus <= 6:
        raise ValueError(f"spdk_cpus must be between 1 and 6, got {vcpus}")
    if vcpus == 1:
        return CpuLayout(vcpus, app_mask=0x1, lvs_mask=0x1, nvmf_mask=0x1)
    if vcpus == 2:
        return CpuLayout(vcpus, app_mask=0x1, lvs_mask=0x1, nvmf_mask=0x2)
    nvmf_mask = ((1 << vcpus) - 1) & ~0x3  # cores 2..n-1
    return CpuLayout(vcpus, app_mask=0x1, lvs_mask=0x2, nvmf_mask=nvmf_mask)


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
    # Store mirrors carry an on-disk superblock so either node can reassemble
    # them via bdev_examine (secondary instance / takeover / fail-back).
    superblock: bool = False


@dataclass
class LocalStackPlan:
    """Per-node local stack: aio bdevs, optional local raid, resulting top,
    and (2-node clusters) the two split halves."""
    aio_bdevs: List[AioSpec]
    raid: Optional[RaidSpec]
    top_bdev: str
    split: bool = False              # 2-node: split the top into two halves

    @property
    def own_half(self) -> str:
        return own_half(self.top_bdev) if self.split else self.top_bdev

    @property
    def peer_half(self) -> str:
        if not self.split:
            raise ValueError("single-node stacks have no peer half")
        return peer_half(self.top_bdev)


@dataclass
class StorePlan:
    """One store (lvstore + mirror) as seen from ONE node."""
    store_node_uuid: str             # the designated owner of this store
    lvs: str
    mirror: RaidSpec                 # this node's instance of the mirror
    role: str                        # "primary" | "secondary" on THIS node
    client_port: int


def plan_local_stack(node, split: bool = False) -> LocalStackPlan:
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
                              top_bdev=aio_bdevs[0].bdev_name, split=split)

    if len(aio_bdevs) == 2:
        raid = RaidSpec(name=local_raid_name(node.uuid), raid_level="1",
                        base_bdevs=[a.bdev_name for a in aio_bdevs])
    else:
        raid = RaidSpec(name=local_raid_name(node.uuid), raid_level="5f",
                        base_bdevs=[a.bdev_name for a in aio_bdevs],
                        strip_size_kb=edge_constants.EDGE_RAID5_STRIP_SIZE_KB)
    return LocalStackPlan(aio_bdevs=aio_bdevs, raid=raid, top_bdev=raid.name,
                          split=split)


def plan_store(this_node, store_node, peer_node, base_port: int,
               store_index: int) -> StorePlan:
    """This node's instance of the store owned by store_node.

    Leg selection (see module docstring): the owner contributes its OWN half
    and the peer's PEER half; the secondary contributes its PEER half and the
    owner's OWN half — the same two physical halves, viewed from each side.
    """
    this_plan = plan_local_stack(this_node, split=True)
    if this_node.uuid == store_node.uuid:
        legs = [this_plan.own_half, remote_half_bdev(peer_node.uuid, 2)]
        role = "primary"
    else:
        legs = [this_plan.peer_half, remote_half_bdev(store_node.uuid, 1)]
        role = "secondary"
    return StorePlan(
        store_node_uuid=store_node.uuid,
        lvs=lvs_name(store_node.uuid),
        mirror=RaidSpec(name=mirror_name(store_node.uuid), raid_level="1",
                        base_bdevs=legs, superblock=True),
        role=role,
        client_port=store_client_port(base_port, store_index),
    )


def single_node_lvs_base(node) -> str:
    """Single-node clusters: the lvstore sits directly on the local top."""
    return plan_local_stack(node, split=False).top_bdev
