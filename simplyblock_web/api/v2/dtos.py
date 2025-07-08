from ipaddress import IPv4Address
from typing import List, Literal, Tuple, Optional
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

from . import util


class ClusterDTO(BaseModel):
    id: UUID
    nqn: str
    status: Literal['active', 'read_only', 'inactive', 'suspended', 'degraded', 'unready', 'in_activation', 'in_expansion']
    rebalancing: bool
    block_size: util.Unsigned
    coding: Tuple[util.Unsigned, util.Unsigned]
    ha: bool
    utliziation_critical: util.Percent
    utilization_warning: util.Percent
    provisioned_cacacity_critical: util.Unsigned
    provisioned_cacacity_warning: util.Unsigned
    node_affinity: bool
    anti_affinity: bool

    @staticmethod
    def from_model(model: Cluster):
        return ClusterDTO(
            id=UUID(model.get_id()),
            nqn=model.nqn,
            status=model.status,  # type: ignore
            rebalancing=model.is_re_balancing,
            block_size=model.blk_size,
            coding=(model.distr_ndcs, model.distr_npcs),
            ha=model.ha_type == 'ha',
            utilization_warning=model.cap_warn,
            utliziation_critical=model.cap_crit,
            provisioned_cacacity_warning=model.prov_cap_warn,
            provisioned_cacacity_critical=model.prov_cap_crit,
            node_affinity=model.enable_node_affinity,
            anti_affinity=model.strict_node_anti_affinity,
        )


class DeviceDTO(BaseModel):
    id: UUID
    status: str
    health_check: bool
    size: int
    io_error: bool
    is_partition: bool
    nvmf_ips: List[IPv4Address]
    nvmf_nqn: str = ""
    nvmf_port: int = 0

    @staticmethod
    def from_model(model: NVMeDevice):
        return DeviceDTO(
            id=UUID(model.get_id()),
            status=model.status,
            health_check=model.health_check,
            size=model.size,
            io_error=model.io_error,
            is_partition=model.is_partition,
            nvmf_ips=[IPv4Address(ip) for ip in model.nvmf_ip.split(',')],
            nvmf_nqn=model.nvmf_nqn,
            nvmf_port=model.nvmf_port,
        )


class ManagementNodeDTO(BaseModel):
    id: UUID
    status: str
    hostname: str
    ip: IPv4Address

    @staticmethod
    def from_model(model: MgmtNode):
        return ManagementNodeDTO(
            id=UUID(model.get_id()),
            status=model.status,
            hostname=model.hostname,
            ip=IPv4Address(model.mgmt_ip),
        )


class StoragePoolDTO(BaseModel):
    id: UUID
    name: str
    status: Literal['active', 'inactive']
    max_size: util.Unsigned
    volume_max_size: util.Unsigned
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned

    @staticmethod
    def from_model(model: Pool):
        return StoragePoolDTO(
            id=UUID(model.get_id()),
            name=model.pool_name,
            status=model.status,  # type: ignore
            max_size=model.pool_max_size,
            volume_max_size=model.lvol_max_size,
            max_rw_iops=model.max_rw_ios_per_sec,
            max_rw_mbytes=model.max_rw_mbytes_per_sec,
            max_r_mbytes=model.max_r_mbytes_per_sec,
            max_w_mbytes=model.max_w_mbytes_per_sec,
        )


class SnapshotDTO(BaseModel):
    id: UUID
    name: str
    status: str
    health_check: bool
    size: util.Unsigned
    used_size: util.Unsigned
    lvol: Optional[util.UrlPath]

    @staticmethod
    def from_model(model: SnapShot, request: Request, cluster_id, pool_id):
        return SnapshotDTO(
            id=model.get_id(),
            name=model.snap_name,
            status=model.status,
            health_check=model.health_check,
            size=model.size,
            used_size=model.used_size,
            lvol=str(request.url_for(
                'clusters:pools:volumes:detail',
                cluster_id=cluster_id,
                pool_id=pool_id,
                volume_id=model.lvol.get_id(),
            )) if model.lvol is not None else None,
        )


class StorageNodeDTO(BaseModel):
    id: UUID
    status: str
    ip: IPv4Address

    @staticmethod
    def from_model(model: StorageNode):
        return StorageNodeDTO(
            id=UUID(model.get_id()),
            status=model.status,
            ip=IPv4Address(model.mgmt_ip),
        )


class TaskDTO(BaseModel):
    id: UUID
    status: str
    canceled: bool
    function_name: str
    function_params: dict
    function_result: str
    retry: util.Unsigned
    max_retry: int

    @staticmethod
    def from_model(model: JobSchedule):
        return TaskDTO(
            id=UUID(model.get_id()),
            status=model.status,
            canceled=model.canceled,
            function_name=model.function_name,
            function_params=model.function_params,
            function_result=model.function_result,
            retry=model.retry,
            max_retry=model.max_retry,
        )


class VolumeDTO(BaseModel):
    id: UUID
    name: str
    status: str
    health_check: bool
    nqn: str
    nodes: List[util.UrlPath]
    port: util.Port
    size: util.Unsigned
    cloned_from: Optional[util.UrlPath]
    crypto_key: Optional[Tuple[str, str]]
    high_availability: bool
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned

    @staticmethod
    def from_model(model: LVol, request: Request, cluster_id: str):
        return VolumeDTO(
            id=UUID(model.get_id()),
            name=model.lvol_name,
            status=model.status,
            health_check=model.health_check,
            nqn=model.nqn,
            nodes=[
                str(request.url_for(
                    'clusters:storage-nodes:detail',
                    cluster_id=cluster_id,
                    storage_node_id=node_id,
                ))
                for node_id in model.nodes
            ],
            port=model.subsys_port,
            size=model.size,
            cloned_from=str(request.url_for(
                'clusters:storage-pools:snapshots:detail',
                cluster_id=cluster_id,
                pool_id=model.pool_uuid,
                snapshot_id=model.cloned_from_snap
            )) if model.cloned_from_snap else None,
            crypto_key=(
                (model.crypto_key1, model.crypto_key2)
                if model.crypto_key1 and model.crypto_key2
                else None
            ),
            high_availability=model.ha_type == 'ha',
            max_rw_iops=model.rw_ios_per_sec,
            max_rw_mbytes=model.rw_mbytes_per_sec,
            max_r_mbytes=model.r_mbytes_per_sec,
            max_w_mbytes=model.w_mbytes_per_sec,
        )
