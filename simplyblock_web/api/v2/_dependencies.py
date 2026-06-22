from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup import Backup as BackupModel, BackupPolicy
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool as PoolModel
from simplyblock_core.models.snapshot import SnapShot as SnapshotModel
from simplyblock_core.models.storage_node import StorageNode as StorageNodeModel

_db = DBController()


def _lookup_cluster(cluster_id: UUID) -> ClusterModel:
    try:
        return _db.get_cluster_by_id(str(cluster_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


Cluster = Annotated[ClusterModel, Depends(_lookup_cluster)]


def _lookup_storage_node(storage_node_id: UUID, cluster: Cluster) -> StorageNodeModel:
    try:
        storage_node = _db.get_storage_node_by_id(str(storage_node_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if storage_node.cluster_id != cluster.get_id():
        raise HTTPException(404, f'StorageNode {storage_node_id} not found')
    return storage_node


StorageNode = Annotated[StorageNodeModel, Depends(_lookup_storage_node)]


def _lookup_storage_pool(pool_id: UUID, cluster: Cluster) -> PoolModel:
    try:
        pool = _db.get_pool_by_id(str(pool_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if pool.cluster_id != cluster.get_id():
        raise HTTPException(404, f'Pool {pool_id} not found')
    return pool


StoragePool = Annotated[PoolModel, Depends(_lookup_storage_pool)]


def _lookup_volume(volume_id: UUID, pool: StoragePool) -> LVol:
    try:
        volume = _db.get_lvol_by_id(str(volume_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if volume.pool_uuid != pool.get_id():
        raise HTTPException(404, f'LVol {volume_id} not found')
    return volume


Volume = Annotated[LVol, Depends(_lookup_volume)]


def _lookup_snapshot(snapshot_id: UUID, pool: StoragePool) -> SnapshotModel:
    try:
        snapshot = _db.get_snapshot_by_id(str(snapshot_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if snapshot.pool_uuid != pool.get_id():
        raise HTTPException(404, f'Snapshot {snapshot_id} not found')
    return snapshot


Snapshot = Annotated[SnapshotModel, Depends(_lookup_snapshot)]


def _lookup_device(storage_node: StorageNode, device_id: UUID) -> NVMeDevice:
    device = next(
        (d for d in storage_node.nvme_devices if d.get_id() == str(device_id)),
        None,
    )
    if device is None:
        raise HTTPException(404, f'Device {device_id} not found')
    return device


Device = Annotated[NVMeDevice, Depends(_lookup_device)]


def _lookup_task(task_id: UUID, cluster: Cluster) -> JobSchedule:
    task = _db.get_task_by_id(str(task_id))
    if task is None:
        raise HTTPException(404, 'Task does not exist')
    if task.cluster_id != cluster.get_id():
        raise HTTPException(404, 'Task does not exist')
    return task


Task = Annotated[JobSchedule, Depends(_lookup_task)]


def _lookup_management_node(management_node_id: UUID) -> MgmtNode:
    management_node = _db.get_mgmt_node_by_id(str(management_node_id))
    if management_node is None:
        raise HTTPException(404, f'ManagementNode {management_node_id} not found')
    return management_node


ManagementNode = Annotated[MgmtNode, Depends(_lookup_management_node)]


def _lookup_backup(backup_id: UUID, cluster: Cluster) -> BackupModel:
    try:
        backup = _db.get_backup_by_id(str(backup_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if backup.cluster_id != cluster.get_id():
        raise HTTPException(404, f'Backup {backup_id} not found')
    return backup


BackupResource = Annotated[BackupModel, Depends(_lookup_backup)]


def _lookup_backup_policy(policy_id: UUID, cluster: Cluster) -> BackupPolicy:
    try:
        policy = _db.get_backup_policy_by_id(str(policy_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if policy.cluster_id != cluster.get_id():
        raise HTTPException(404, f'BackupPolicy {policy_id} not found')
    return policy


Policy = Annotated[BackupPolicy, Depends(_lookup_backup_policy)]


def _lookup_migration(migration_id: UUID, cluster: Cluster) -> LVolMigration:
    try:
        migration = _db.get_migration_by_id(str(migration_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if migration.cluster_id != cluster.get_id():
        raise HTTPException(404, f'Migration {migration_id} not found')
    return migration


Migration = Annotated[LVolMigration, Depends(_lookup_migration)]
