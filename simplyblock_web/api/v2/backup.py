from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import backup_controller
from simplyblock_core.models.backup import BackupPolicy
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.lvol_model import LVol

from .cluster import Cluster
from .dtos import BackupDTO, BackupPolicyDTO

api = APIRouter(prefix='/backups')
db = DBController()


@api.get('/', name='clusters:backups:list')
def list_backups(cluster: Cluster) -> List[BackupDTO]:
    backups = db.get_backups(cluster.get_id())
    backups = sorted(backups, key=lambda b: (b.created_at, b.uuid), reverse=True)
    return [BackupDTO.from_model(b) for b in backups]


class _BackupSnapshotParams(BaseModel):
    snapshot_id: str


@api.post('/', name='clusters:backups:create', status_code=201, responses={201: {"content": None}})
def create_backup(cluster: Cluster, parameters: _BackupSnapshotParams) -> Response:
    backup_id, error = backup_controller.backup_snapshot(
        parameters.snapshot_id, cluster_id=cluster.get_id())
    if error:
        raise HTTPException(400, error)
    return Response(status_code=201, headers={'X-Backup-Id': backup_id})


class _RestoreParams(BaseModel):
    backup_id: str
    lvol_name: str
    pool: str
    target_node_id: Optional[str] = None


@api.post('/restore', name='clusters:backups:restore', status_code=202)
def restore_backup(cluster: Cluster, parameters: _RestoreParams):
    result, error = backup_controller.restore_backup(
        parameters.backup_id, parameters.lvol_name, parameters.pool,
        cluster_id=cluster.get_id(), target_node_id=parameters.target_node_id)
    if error:
        raise HTTPException(400, error)
    return {"lvol_id": result}


class _ImportParams(BaseModel):
    metadata: list


@api.post('/import', name='clusters:backups:import')
def import_backups(cluster: Cluster, parameters: _ImportParams):
    count = backup_controller.import_backups(parameters.metadata, cluster_id=cluster.get_id())
    return {"imported": count}


@api.get('/export', name='clusters:backups:export')
def export_backups(
    cluster: Cluster,
    backup_id: Optional[str] = Query(None, description="Export only the chain containing this backup UUID"),
    lvol_name: Optional[str] = Query(None, description="Export all completed backups for this lvol name"),
):
    lvol_name_filter = lvol_name
    if backup_id and not lvol_name_filter:
        try:
            backup = db.get_backup_by_id(backup_id)
            lvol_name_filter = backup.lvol_name
        except KeyError:
            raise HTTPException(404, f"Backup {backup_id} not found")
    data = backup_controller.export_backups(
        cluster_id=cluster.get_id(), lvol_name=lvol_name_filter)
    return data


class _BackupSourceSwitchParams(BaseModel):
    source_cluster_id: str


@api.post('/source-switch', name='clusters:backups:source-switch')
def source_switch(cluster: Cluster, parameters: _BackupSourceSwitchParams):
    success, error = backup_controller.switch_backup_source(
        cluster.get_id(), parameters.source_cluster_id)
    if error:
        raise HTTPException(400, error)
    return {"source_cluster_id": parameters.source_cluster_id}


@api.get('/sources', name='clusters:backups:sources')
def list_sources(cluster: Cluster):
    sources = backup_controller.get_backup_sources(cluster.get_id())
    return sources


def _lookup_lvol_in_cluster(volume_id: str, cluster: ClusterModel) -> LVol:
    try:
        volume = db.get_lvol_by_id(volume_id)
        pool = db.get_pool_by_id(volume.pool_uuid)
    except KeyError as e:
        raise HTTPException(404, str(e))
    if pool.cluster_id != cluster.get_id():
        raise HTTPException(404, f'LVol {volume_id} not found')
    return volume


@api.delete(
    '/{volume_id}',
    name='clusters:backups:delete',
    status_code=204,
    responses={204: {"content": None}},
    deprecated=True,
    summary='Deprecated — delete all backups for a volume',
    description=(
        'Deprecated. Use '
        '`DELETE /clusters/{cluster_id}/storage-pools/{pool_id}/volumes/{volume_id}/backups` '
        'instead.'
    ),
)
def delete_backups(cluster: Cluster, volume_id: UUID) -> Response:
    volume = _lookup_lvol_in_cluster(str(volume_id), cluster)
    success, error = backup_controller.delete_backups(volume.get_id())
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)


# Backup policies

policy_api = APIRouter(prefix='/backup-policies')


@policy_api.get('/', name='clusters:backup-policies:list')
def list_policies(cluster: Cluster) -> List[BackupPolicyDTO]:
    policies = db.get_backup_policies(cluster.get_id())
    return [BackupPolicyDTO.from_model(p) for p in policies]


class _PolicyCreateParams(BaseModel):
    name: str
    versions: Optional[int] = 0
    age: Optional[str] = ""
    schedule: Optional[str] = ""


@policy_api.post('/', name='clusters:backup-policies:create', status_code=201, responses={201: {"content": None}})
def create_policy(cluster: Cluster, parameters: _PolicyCreateParams) -> Response:
    policy_id, error = backup_controller.add_policy(
        cluster.get_id(), parameters.name,
        max_versions=parameters.versions or 0,
        max_age=parameters.age or "",
        schedule=parameters.schedule or "")
    if error:
        raise HTTPException(400, error)
    return Response(status_code=201, headers={'X-Policy-Id': policy_id})


def _lookup_backup_policy(policy_id: UUID, cluster: Cluster) -> BackupPolicy:
    try:
        policy = db.get_backup_policy_by_id(str(policy_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if policy.cluster_id != cluster.get_id():
        raise HTTPException(404, f'BackupPolicy {policy_id} not found')
    return policy


Policy = Annotated[BackupPolicy, Depends(_lookup_backup_policy)]


def _validate_attachment_target(target_type: str, target_id: str, cluster: ClusterModel) -> None:
    if target_type == "pool":
        try:
            pool = db.get_pool_by_id(target_id)
        except KeyError as e:
            raise HTTPException(404, str(e))
        if pool.cluster_id != cluster.get_id():
            raise HTTPException(404, f'Pool {target_id} not found')
    elif target_type == "lvol":
        _lookup_lvol_in_cluster(target_id, cluster)


@policy_api.delete('/{policy_id}', name='clusters:backup-policies:delete', status_code=204, responses={204: {"content": None}})
def delete_policy(cluster: Cluster, policy: Policy) -> Response:
    success, error = backup_controller.remove_policy(policy.uuid)
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)


class _AttachParams(BaseModel):
    target_type: str
    target_id: str


@policy_api.post('/{policy_id}/attach', name='clusters:backup-policies:attach', status_code=201)
def attach_policy(cluster: Cluster, policy: Policy, parameters: _AttachParams):
    _validate_attachment_target(parameters.target_type, parameters.target_id, cluster)
    att_id, error = backup_controller.attach_policy(
        policy.uuid, parameters.target_type, parameters.target_id)
    if error:
        raise HTTPException(400, error)
    return {"attachment_id": att_id}


@policy_api.post('/{policy_id}/detach', name='clusters:backup-policies:detach', status_code=204, responses={204: {"content": None}})
def detach_policy(cluster: Cluster, policy: Policy, parameters: _AttachParams) -> Response:
    _validate_attachment_target(parameters.target_type, parameters.target_id, cluster)
    success, error = backup_controller.detach_policy(
        policy.uuid, parameters.target_type, parameters.target_id)
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)
