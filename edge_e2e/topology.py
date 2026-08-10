# coding=utf-8
"""Topology matrix for the edge-clusters e2e environment.

One "central" k3s cluster (control plane + a 3-node hyperscale storage
cluster on three workers) plus eight edge k3s clusters covering the drive
matrix in both node counts:

    1-node: 1 drive | 2 drives | 2 partitions of 1 drive | 4 drives
    2-node: 1 drive | 2 drives | 2 partitions of 1 drive | 4 drives
            (per node)

Note: the original ask said "3x 2-node" but enumerated four drive configs and
"eight" clusters total — this matrix realizes all four 2-node variants.
Drop one from EDGE_CLUSTERS if only three are wanted.

Edge instances are cost-effective 4-vCPU boxes; SPDK gets a single vCPU
(SIMPLYBLOCK_EDGE_POD_CPU=1 is the default in simplyblock_edge.constants).
"""
import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class DriveSpec:
    size_gb: int
    partitions: int = 1  # >1: the deploy step splits the raw volume with sgdisk


@dataclass
class EdgeClusterSpec:
    name: str
    nodes: int
    drives: List[DriveSpec]           # per node
    instance_type: str = os.getenv("EDGE_E2E_EDGE_INSTANCE_TYPE", "c5a.xlarge")  # 4 vCPU / 8 GiB

    @property
    def device_paths(self) -> List[str]:
        """Data device paths as they appear on the node, in attach order.

        AWS nitro exposes EBS volumes as /dev/nvme1n1..N (nvme0 is root).
        Partitioned variants contribute /dev/nvmeXn1p1..pP instead of the
        raw device.
        """
        paths = []
        for index, drive in enumerate(self.drives, start=1):
            if drive.partitions > 1:
                paths.extend(f"/dev/nvme{index}n1p{p}" for p in range(1, drive.partitions + 1))
            else:
                paths.append(f"/dev/nvme{index}n1")
        return paths


@dataclass
class CentralSpec:
    name: str = "edge-e2e-central"
    workers: int = 3                  # host CP services AND the storage nodes
    instance_type: str = os.getenv("EDGE_E2E_CENTRAL_INSTANCE_TYPE", "m5.2xlarge")
    mgmt_instance_type: str = os.getenv("EDGE_E2E_MGMT_INSTANCE_TYPE", "m5.xlarge")
    storage_drives: List[DriveSpec] = field(
        default_factory=lambda: [DriveSpec(size_gb=100), DriveSpec(size_gb=100)])


DATA_DRIVE_GB = int(os.getenv("EDGE_E2E_DRIVE_GB", "40"))

CENTRAL = CentralSpec()

EDGE_CLUSTERS: List[EdgeClusterSpec] = [
    # --- 1-node ---
    EdgeClusterSpec("edge-1n-1d", nodes=1, drives=[DriveSpec(DATA_DRIVE_GB)]),
    EdgeClusterSpec("edge-1n-2d", nodes=1, drives=[DriveSpec(DATA_DRIVE_GB)] * 2),
    EdgeClusterSpec("edge-1n-2p", nodes=1, drives=[DriveSpec(2 * DATA_DRIVE_GB, partitions=2)]),
    EdgeClusterSpec("edge-1n-4d", nodes=1, drives=[DriveSpec(DATA_DRIVE_GB)] * 4),
    # --- 2-node ---
    EdgeClusterSpec("edge-2n-1d", nodes=2, drives=[DriveSpec(DATA_DRIVE_GB)]),
    EdgeClusterSpec("edge-2n-2d", nodes=2, drives=[DriveSpec(DATA_DRIVE_GB)] * 2),
    EdgeClusterSpec("edge-2n-2p", nodes=2, drives=[DriveSpec(2 * DATA_DRIVE_GB, partitions=2)]),
    EdgeClusterSpec("edge-2n-4d", nodes=2, drives=[DriveSpec(DATA_DRIVE_GB)] * 4),
]

# Clusters with redundancy on the DEVICE level (device remove / EBS-detach
# tests must keep IO unaffected there): >1 partition on the node, i.e.
# everything except the single-drive-single-partition variants.
def has_device_redundancy(spec: EdgeClusterSpec) -> bool:
    return len(spec.device_paths) > 1
