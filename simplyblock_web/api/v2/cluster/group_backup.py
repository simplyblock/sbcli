"""Consistency-group backup REST surface (design-consistency-group-backup.md §8).

A group backup is CREATED under its source group
(`consistency-groups/{group_id}/backups`), because taking one resolves a
generation of that group. This router exposes the group backups themselves,
independent of the group that produced them: listing them, reading one, restoring
one into a new consistency group, and deleting one. The endpoints are thin --
restore fans out to the single-volume restore path per member and re-forms the
group; delete removes the group manifest and the record, leaving the member
backups to per-volume retention (§9).
"""
from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel

from simplyblock_core.controllers.backup import group_backup as group_backup_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup_config import S3Credentials

from .._dependencies import Cluster, GroupBackupResource
from .._dtos import GroupBackupDTO

api = APIRouter(tags=['group-backups'])
db = DBController()


@api.get('/', name='clusters:group-backups:list', response_model=list[GroupBackupDTO])
def list_group_backups(cluster: Cluster) -> list[GroupBackupDTO]:
    """List the cluster's group backups, newest first."""
    group_backups = db.get_group_backups(cluster.get_id())
    group_backups = sorted(group_backups, key=lambda g: (g.created_at, g.uuid), reverse=True)
    return [GroupBackupDTO.from_model(g) for g in group_backups]


instance_api = APIRouter(prefix='/{group_backup_id}')


@instance_api.get('/', name='clusters:group-backups:detail', response_model=GroupBackupDTO)
def get_group_backup(cluster: Cluster, group_backup: GroupBackupResource) -> GroupBackupDTO:
    return GroupBackupDTO.from_model(group_backup)


class _GroupRestoreParams(BaseModel):
    #: Name of the NEW consistency group the restore forms; must not already
    #: exist (design §6: a group restore always produces a new group).
    into_group_name: str
    pool: str
    target_node_id: str | None = None
    #: Credentials for the backup's bucket when it is not this cluster's own --
    #: the disaster-recovery case. Omit to use the nodes' instance role.
    s3_credentials: S3Credentials | None = None


@instance_api.post('/restore', name='clusters:group-backups:restore', status_code=202)
def restore_group_backup(cluster: Cluster, group_backup: GroupBackupResource,
                         parameters: _GroupRestoreParams):
    """Restore a group backup into a NEW consistency group, all-or-nothing
    (design §6). Returns the restored group id and the member volume ids; each
    volume recovers its data asynchronously, the same as a single-volume restore.
    """
    group_id, lvol_ids, error = group_backup_controller.restore_group_backup(
        group_backup.get_id(), parameters.into_group_name, parameters.pool,
        target_node_id=parameters.target_node_id,
        s3_credentials=parameters.s3_credentials)
    if error:
        raise HTTPException(422, error)
    return {"group_id": group_id, "lvol_ids": lvol_ids}


@instance_api.delete('/', name='clusters:group-backups:delete',
                     status_code=204, responses={204: {"content": None}})
def delete_group_backup(cluster: Cluster, group_backup: GroupBackupResource) -> Response:
    """Delete a group backup: its group manifest and its record. The member
    backups follow per-volume retention (design §9)."""
    success, error = group_backup_controller.delete_group_backup(group_backup.get_id())
    if error:
        raise HTTPException(400, error)
    return Response(status_code=204)


api.include_router(instance_api)
