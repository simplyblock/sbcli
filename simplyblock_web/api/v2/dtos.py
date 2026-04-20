from datetime import timedelta
from ipaddress import IPv4Address
from typing import List, Literal, Tuple, Optional, cast
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel

from simplyblock_core.utils import hexa_to_cpu_list
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.backup import Backup, BackupPolicy
from simplyblock_core.models.stats import StatsObject
from simplyblock_core.models.lvol_migration import LVolMigration

from . import util


ClusterStatus = Literal[
    "active",
    "read_only",
    "inactive",
    "suspended",
    "degraded",
    "unready",
    "in_activation",
    "in_expansion",
]

StoragePoolStatus = Literal["active", "inactive"]

StorageNodeStatus = Literal[
    "online",
    "offline",
    "suspended",
    "in_shutdown",
    "removed",
    "in_restart",
    "in_creation",
    "unreachable",
    "schedulable",
    "down",
]

TaskStatus = Literal["new", "running", "suspended", "done"]

TaskFunctionName = Literal[
    "device_restart",
    "node_restart",
    "device_migration",
    "failed_device_migration",
    "new_device_migration",
    "node_add",
    "port_allow",
    "balancing_on_restart",
    "balancing_on_dev_rem",
    "balancing_on_dev_add",
    "jc_comp_resume",
    "snapshot_replication",
    "lvol_sync_del",
    "lvol_migration",
    "s3_backup",
    "s3_backup_restore",
    "s3_backup_merge",
]


class CapacityStatDTO(BaseModel):
    date: int
    size_total: int
    size_prov: int
    size_used: int
    size_free: int
    size_util: int

    @staticmethod
    def from_model(model: StatsObject):
        return CapacityStatDTO(
            date=model.date,
            size_total=model.size_total,
            size_prov=model.size_prov,
            size_used=model.size_used,
            size_free=model.size_free,
            size_util=model.size_util,
        )


class ClusterDTO(BaseModel):
    id: UUID
    name: Optional[str]
    nqn: str
    status: ClusterStatus
    is_re_balancing: bool
    block_size: util.Unsigned
    distr_ndcs: int
    distr_npcs: int
    ha: bool
    utliziation_critical: util.Percent
    utilization_warning: util.Percent
    provisioned_capacity_critical: util.Unsigned
    provisioned_capacity_warning: util.Unsigned
    node_affinity: bool
    anti_affinity: bool
    secret: str
    tls_enabled: bool
    max_fault_tolerance: int
    backup_enabled: bool
    capacity: CapacityStatDTO

    @staticmethod
    def from_model(model: Cluster, stat_obj: Optional[StatsObject] = None):
        return ClusterDTO(
            id=UUID(model.get_id()),
            name=model.cluster_name,
            nqn=model.nqn,
            status=cast(ClusterStatus, model.status),
            is_re_balancing=model.is_re_balancing,
            block_size=model.blk_size,
            distr_ndcs=model.distr_ndcs,
            distr_npcs=model.distr_npcs,
            ha=model.ha_type == "ha",
            utilization_warning=model.cap_warn,
            utliziation_critical=model.cap_crit,
            provisioned_capacity_warning=model.prov_cap_warn,
            provisioned_capacity_critical=model.prov_cap_crit,
            node_affinity=model.enable_node_affinity,
            anti_affinity=model.strict_node_anti_affinity,
            secret=model.secret,
            tls_enabled=model.tls,
            max_fault_tolerance=model.max_fault_tolerance,
            backup_enabled=bool(model.backup_config),
            capacity=CapacityStatDTO.from_model(
                stat_obj if stat_obj else StatsObject()
            ),
        )


class DeviceDTO(BaseModel):
    id: UUID
    cluster_id: UUID
    storage_node_id: UUID
    model: str
    serial_number: str
    nvme_controller: str
    pcie_address: str
    status: str
    health_check: bool
    retries_exhausted: bool
    size: int
    cluster_device_order: util.Unsigned
    io_error: bool
    is_partition: bool
    nvmf_ips: List[IPv4Address]
    nvmf_nqn: str = ""
    nvmf_port: int = 0
    capacity: CapacityStatDTO

    @staticmethod
    def from_model(model: NVMeDevice, storage_node_id: str, stat_obj: Optional[StatsObject] = None):
        return DeviceDTO(
            id=UUID(model.get_id()),
            cluster_id=UUID(model.cluster_id),
            storage_node_id=UUID(storage_node_id),
            model=model.model_id,
            serial_number=model.serial_number,
            nvme_controller=model.nvme_controller,
            pcie_address=model.pcie_address,
            status=model.status,
            health_check=model.health_check,
            retries_exhausted=model.retries_exhausted,
            size=model.size,
            cluster_device_order=model.cluster_device_order,
            io_error=model.io_error,
            is_partition=model.is_partition,
            nvmf_ips=[IPv4Address(ip) for ip in model.nvmf_ip.split(",")],
            nvmf_nqn=model.nvmf_nqn,
            nvmf_port=model.nvmf_port,
            capacity=CapacityStatDTO.from_model(
                stat_obj if stat_obj else StatsObject()
            ),
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
    cluster_id: UUID
    name: str
    status: StoragePoolStatus
    max_size: util.Unsigned
    volume_max_size: util.Unsigned
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned
    capacity: CapacityStatDTO
    dhchap: bool = False
    allowed_hosts: List[str] = []

    @staticmethod
    def from_model(model: Pool, stat_obj: Optional[StatsObject] = None):
        return StoragePoolDTO(
            id=UUID(model.get_id()),
            cluster_id=UUID(model.cluster_id),
            name=model.pool_name,
            status=cast(StoragePoolStatus, model.status),
            max_size=model.pool_max_size,
            volume_max_size=model.lvol_max_size,
            max_rw_iops=model.max_rw_ios_per_sec,
            max_rw_mbytes=model.max_rw_mbytes_per_sec,
            max_r_mbytes=model.max_r_mbytes_per_sec,
            max_w_mbytes=model.max_w_mbytes_per_sec,
            dhchap=getattr(model, 'dhchap', False),
            allowed_hosts=list(getattr(model, 'allowed_hosts', [])),
            capacity=CapacityStatDTO.from_model(
                stat_obj if stat_obj else StatsObject()
            ),
        )


class SnapshotDTO(BaseModel):
    id: UUID
    name: str
    status: str
    health_check: bool
    size: util.Unsigned
    used_size: util.Unsigned
    migrating: bool
    lvol: Optional[util.UrlPath]

    @staticmethod
    def from_model(
        model: SnapShot, request: Request, cluster_id, pool_id, volume_id=None
    ):
        from simplyblock_core.controllers import migration_controller

        is_migrating = False
        if model.lvol is not None:
            active_mig = migration_controller.get_active_migration_for_lvol(
                model.lvol.uuid
            )
            is_migrating = active_mig is not None

        return SnapshotDTO(
            id=model.get_id(),
            name=model.snap_name,
            status=model.status,
            health_check=model.health_check,
            size=model.size,
            used_size=model.used_size,
            migrating=is_migrating,
            lvol=str(
                request.url_for(
                    "clusters:pools:volumes:detail",
                    cluster_id=cluster_id,
                    pool_id=pool_id,
                    volume_id=model.lvol.get_id(),
                )
            )
            if model.lvol is not None and (volume_id == model.lvol.get_id())
            else None,
        )


class StorageNodeDTO(BaseModel):
    id: UUID
    cluster_id: UUID
    status: StorageNodeStatus
    uptime: Optional[timedelta]
    hostname: str
    host_nqn: str
    cpu_total_count: util.Unsigned
    cpu_spdk_count: util.Unsigned
    cpu_poller_count: util.Unsigned
    memory: util.Unsigned
    hugepage_memory: util.Unsigned
    spdk_mem: int
    lvols: int
    lvols_max: util.Unsigned
    snapshots_max: util.Unsigned
    rpc_port: util.Port
    lvol_subsys_port: util.Port
    hublvol_port: util.Port
    nvmf_port: util.Port
    mgmt_ip: IPv4Address
    health_check: bool
    device_count: int
    online_device_count: int
    capacity: CapacityStatDTO

    @staticmethod
    def from_model(model: StorageNode, stat_obj: Optional[StatsObject] = None):
        return StorageNodeDTO(
            id=UUID(model.get_id()),
            cluster_id=UUID(model.cluster_id),
            status=cast(StorageNodeStatus, model.status),
            uptime=model.uptime(),
            hostname=model.hostname,
            host_nqn=model.host_nqn,
            cpu_total_count=model.cpu,
            cpu_spdk_count=len(hexa_to_cpu_list(model.spdk_cpu_mask)),
            cpu_poller_count=len(model.poller_cpu_cores),
            memory=model.memory,
            hugepage_memory=model.hugepages,
            spdk_mem=model.spdk_mem,
            lvols=model.lvols,
            lvols_max=model.max_lvol,
            snapshots_max=model.max_snap,
            rpc_port=model.rpc_port,
            lvol_subsys_port=model.lvol_subsys_port,
            hublvol_port=model.get_hublvol_port(),
            nvmf_port=model.nvmf_port,
            mgmt_ip=IPv4Address(model.mgmt_ip),
            health_check=model.health_check,
            device_count=len(model.nvme_devices),
            online_device_count=len([device for device in model.nvme_devices if device.status == "online" ]),
            capacity=CapacityStatDTO.from_model(
                stat_obj if stat_obj else StatsObject()
            ),
        )


class TaskDTO(BaseModel):
    id: UUID
    cluster_id: UUID
    device_id: Optional[UUID]
    storage_node_id: Optional[UUID]
    status: TaskStatus
    canceled: bool
    function_name: TaskFunctionName
    function_params: dict
    function_result: str
    max_retry: Optional[util.Unsigned]
    retry: util.Unsigned

    @staticmethod
    def from_model(model: JobSchedule):
        return TaskDTO(
            id=UUID(model.uuid),
            cluster_id=UUID(model.cluster_id),
            device_id=UUID(model.device_id) if model.device_id != "" else None,
            storage_node_id=UUID(model.node_id)
            if model.node_id != ""
            else None,
            status=cast(TaskStatus, model.status),
            canceled=model.canceled,
            function_name=cast(TaskFunctionName, model.function_name),
            function_params=model.function_params,
            function_result=model.function_result,
            max_retry=model.max_retry if model.max_retry >= 0 else None,
            retry=model.retry,
        )


class VolumeDTO(BaseModel):
    id: UUID
    cluster_id: UUID
    storage_node_id: UUID
    name: str
    status: str
    health_check: bool
    io_error: bool
    migrating: bool
    nqn: str
    hostname: str
    priority_class: util.Unsigned
    access_mode: str
    namespace: str
    fabric: str
    nodes: List[util.UrlPath]
    port: util.Port
    size: util.Unsigned
    ndcs: int
    npcs: int
    pool_uuid: str
    pool_name: str
    pvc_name: str = ""
    snapshot_name: str = ""
    blobid: int
    ns_id: int
    cloned_from: Optional[util.UrlPath]
    crypto_key: Optional[Tuple[str, str]]
    high_availability: bool
    do_replicate: bool = False
    max_namespace_per_subsys: int
    max_rw_iops: util.Unsigned
    max_rw_mbytes: util.Unsigned
    max_r_mbytes: util.Unsigned
    max_w_mbytes: util.Unsigned
    allowed_hosts: List[str]
    policy: str
    capacity: CapacityStatDTO
    rep_info: Optional[dict] = None
    from_source: bool = True

    @staticmethod
    def from_model(
        model: LVol,
        request: Request,
        cluster_id: str,
        stat_obj: Optional[StatsObject] = None,
        rep_info=None,
    ):
        from simplyblock_core.controllers import migration_controller
        from simplyblock_core.db_controller import DBController as _DBC

        active_mig = migration_controller.get_active_migration_for_lvol(model.uuid)
        _db = _DBC()
        eff_policy = _db.get_policy_for_lvol(model)
        return VolumeDTO(
            id=UUID(model.get_id()),
            cluster_id=UUID(cluster_id),
            storage_node_id=UUID(model.node_id),
            name=model.lvol_name,
            status=model.status,
            health_check=model.health_check,
            io_error=model.io_error,
            migrating=active_mig is not None,
            nqn=model.nqn,
            hostname=model.hostname,
            priority_class=model.lvol_priority_class,
            namespace=model.namespace,
            access_mode=model.mode,
            fabric=model.fabric,
            nodes=[
                str(
                    request.url_for(
                        "clusters:storage-nodes:detail",
                        cluster_id=cluster_id,
                        storage_node_id=node_id,
                    )
                )
                for node_id in model.nodes
            ],
            port=model.subsys_port,
            size=model.size,
            cloned_from=str(
                request.url_for(
                    "clusters:storage-pools:snapshots:detail",
                    cluster_id=cluster_id,
                    pool_id=model.pool_uuid,
                    snapshot_id=model.cloned_from_snap,
                )
            )
            if model.cloned_from_snap
            else None,
            crypto_key=(
                (model.crypto_key1, model.crypto_key2)
                if model.crypto_key1 and model.crypto_key2
                else None
            ),
            high_availability=model.ha_type == "ha",
            pool_uuid=model.pool_uuid,
            pool_name=model.pool_name,
            pvc_name=model.pvc_name,
            snapshot_name=model.snapshot_name,
            ndcs=model.ndcs,
            npcs=model.npcs,
            blobid=model.blobid,
            ns_id=model.ns_id,
            do_replicate=model.do_replicate,
            max_namespace_per_subsys=model.max_namespace_per_subsys,
            max_rw_iops=model.rw_ios_per_sec,
            max_rw_mbytes=model.rw_mbytes_per_sec,
            max_r_mbytes=model.r_mbytes_per_sec,
            max_w_mbytes=model.w_mbytes_per_sec,
            allowed_hosts=[h["nqn"] for h in (model.allowed_hosts or [])],
            policy=eff_policy.policy_name if eff_policy else "",
            capacity=CapacityStatDTO.from_model(
                stat_obj if stat_obj else StatsObject()
            ),
            rep_info=rep_info,
            from_source=model.from_source,
        )


class BackupDTO(BaseModel):
    id: UUID
    s3_id: int
    lvol_id: str
    lvol_name: str
    snapshot_id: str
    snapshot_name: str
    node_id: str
    status: str
    prev_backup_id: str
    size: int
    allowed_hosts: List[dict]
    created_at: int
    completed_at: int

    @staticmethod
    def from_model(model: Backup):
        return BackupDTO(
            id=UUID(model.uuid),
            s3_id=model.s3_id,
            lvol_id=model.lvol_id,
            lvol_name=model.lvol_name,
            snapshot_id=model.snapshot_id,
            snapshot_name=model.snapshot_name,
            node_id=model.node_id,
            status=model.status,
            prev_backup_id=model.prev_backup_id,
            size=model.size,
            allowed_hosts=model.allowed_hosts or [],
            created_at=model.created_at,
            completed_at=model.completed_at,
        )


class BackupPolicyDTO(BaseModel):
    id: UUID
    name: str
    max_versions: int
    max_age: str
    backup_schedule: str
    status: str

    @staticmethod
    def from_model(model: BackupPolicy):
        return BackupPolicyDTO(
            id=UUID(model.uuid),
            name=model.policy_name,
            max_versions=model.max_versions,
            max_age=model.max_age_display,
            backup_schedule=model.backup_schedule or "",
            status=model.status,
        )


class MigrationDTO(BaseModel):
    id: UUID
    lvol_id: str
    source_node_id: str
    target_node_id: str
    phase: str
    status: str
    snaps_total: int
    snaps_migrated: int
    retry_count: int
    max_retries: int
    error_message: str
    started_at: int
    completed_at: int

    @staticmethod
    def from_model(model: LVolMigration):
        return MigrationDTO(
            id=UUID(model.uuid),
            lvol_id=model.lvol_id,
            source_node_id=model.source_node_id,
            target_node_id=model.target_node_id,
            phase=model.phase,
            status=model.status,
            snaps_total=len(model.snap_migration_plan),
            snaps_migrated=len(model.snaps_migrated),
            retry_count=model.retry_count,
            max_retries=model.max_retries,
            error_message=model.error_message or "",
            started_at=model.started_at,
            completed_at=model.completed_at,
        )
