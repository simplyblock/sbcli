from ipaddress import IPv4Address
from typing import List, Optional
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel

from simplyblock_core.models.caching_node import CachingNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

from . import util


class CachingNodeDTO(BaseModel):
    id: UUID
    status: str

    @staticmethod
    def from_model(model: CachingNode):
        return CachingNodeDTO(
            id=UUID(model.get_id()),
            status=model.status,
        )


class ClusterDTO(BaseModel):
    id: UUID
    nqn: str
    block_size: util.Unsigned
    cap_crit: util.Percent
    cap_warn: util.Percent
    prov_cap_crit: util.Unsigned
    prov_cap_warn: util.Unsigned
    ha: bool

    @staticmethod
    def from_model(model: Cluster):
        return ClusterDTO(
            id=UUID(model.get_id()),
            nqn=model.nqn,
            block_size=model.blk_size,
            cap_warn=model.cap_warn,
            cap_crit=model.cap_crit,
            prov_cap_warn=model.prov_cap_warn,
            prov_cap_crit=model.prov_cap_crit,
            ha=model.ha_type == 'ha',
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


class PoolDTO(BaseModel):
    id: UUID
    name: str
    status: str
    max_size: util.Unsigned
    lvol_max_size: util.Unsigned
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned

    @staticmethod
    def from_model(model: Pool):
        return PoolDTO(
            id=UUID(model.get_id()),
            name=model.name,
            status=model.status,
            max_size=model.pool_max_size,
            lvol_max_size=model.lvol_max_size,
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
            name=model.name,
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
    size: util.Unsigned
    cloned_from: Optional[util.UrlPath]
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned
    ha: bool

    @staticmethod
    def from_model(model: LVol, request: Request, cluster_id: str):
        return VolumeDTO(
            id=UUID(model.get_id()),
            name=model.name,
            status=model.status,
            health_check=model.health_check,
            nqn=model.nqn,
            size=model.size,
            cloned_from=str(request.url_for(
                'clusters:pools:snapshots:detail',
                cluster_id=cluster_id,
                pool_id=model.pool_uuid,
                snapshot_id=model.cloned_from_snap
            )) if model.cloned_from_snap else None,
            ha=model.ha_type == 'ha',
            max_rw_iops=model.rw_ios_per_sec,
            max_rw_mbytes=model.rw_mbytes_per_sec,
            max_r_mbytes=model.r_mbytes_per_sec,
            max_w_mbytes=model.w_mbytes_per_sec,
        )
