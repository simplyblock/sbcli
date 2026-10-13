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
from typing import Union

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict

from simplyblock_core.controllers.backup import group_backup as group_backup_controller
from simplyblock_core.controllers.backup.manifest import ManifestError
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.backup_config import S3Credentials

from .._dependencies import Cluster, GroupBackupResource
from .._dtos import (
    BackupConfigDTO,
    GroupBackupDTO,
    GroupBackupExportDTO,
    GroupBackupManifestDTO,
)

api = APIRouter(tags=['group-backups'])
db = DBController()


@api.get('/', name='clusters:group-backups:list', response_model=list[GroupBackupDTO])
def list_group_backups(cluster: Cluster) -> list[GroupBackupDTO]:
    """List the cluster's group backups, newest first."""
    group_backups = db.get_group_backups(cluster.get_id())
    group_backups = sorted(group_backups, key=lambda g: (g.created_at, g.uuid), reverse=True)
    return [GroupBackupDTO.from_model(g) for g in group_backups]


@api.get('/export', name='clusters:group-backups:export')
def export_group_backup(
    cluster: Cluster,
    group_backup_id: str = Query(..., description="Export the group backup with this UUID"),
) -> GroupBackupExportDTO:
    """Export a group backup's manifests as a unit, for import elsewhere (§6.5).

    Carries the group manifest plus every member backup's chain, so a hand-carried
    file describes the group backup identically to a bucket read.
    """
    try:
        group_backup = db.get_group_backup_by_id(group_backup_id)
    except KeyError:
        raise HTTPException(404, f"Group backup {group_backup_id} not found")
    if group_backup.cluster_id != cluster.get_id():
        raise HTTPException(404, f"Group backup {group_backup_id} not found")
    return group_backup_controller.export_group_backup(
        group_backup.get_id(), cluster_id=cluster.get_id())


class _GroupImportManifests(BaseModel):
    """A group export carried in the request itself, e.g. read from a file.

    Names nothing beside it: the export records where its member backups live, so
    the caller states nothing the document has not already recorded.
    """
    model_config = ConfigDict(extra="forbid")

    metadata: GroupBackupExportDTO


class _GroupImportFromBucket(BaseModel):
    """Import whatever group backups a bucket turns out to contain.

    The disaster-recovery path: it needs a bucket and credentials for it, and
    nothing from the cluster that wrote the group backups.
    """
    model_config = ConfigDict(extra="forbid")

    bucket: BackupConfigDTO


#: The two ways to name what to import, a union rather than one model with two
#: optional fields -- the same shape the single-volume import takes, so pydantic
#: rejects a malformed body itself and the schema says "one of these two".
_GroupImportParams = Union[_GroupImportManifests, _GroupImportFromBucket]


@api.post('/import', name='clusters:group-backups:import')
def import_group_backup(cluster: Cluster, parameters: _GroupImportParams):
    """Import a group backup, preserving ids, all-or-nothing (§6.5)."""
    try:
        ids = (
            group_backup_controller.import_group_backup_from_bucket(
                parameters.bucket, cluster_id=cluster.get_id())
            if isinstance(parameters, _GroupImportFromBucket) else
            [group_backup_controller.import_group_backup(
                parameters.metadata, cluster_id=cluster.get_id())]
        )
    except ManifestError as e:
        # The bucket named in the request could not be read. 400 rather than 502,
        # as the single-volume import does: nothing here proxies for S3, and what
        # the caller supplied is the only thing that can be wrong from here.
        raise HTTPException(400, str(e)) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"imported": len(ids), "group_backup_ids": ids}


@api.post('/discover', name='clusters:group-backups:discover')
def discover_group_backups(parameters: BackupConfigDTO) -> list[GroupBackupManifestDTO]:
    """List the group backups a bucket contains, without importing anything (§6.5).

    A POST because it carries credentials, which have no business in a query
    string. Takes no cluster state at all: this is what an operator runs when the
    cluster that wrote the group backups no longer exists.
    """
    try:
        return group_backup_controller.discover_group_backups(parameters)
    except ManifestError as e:
        raise HTTPException(400, str(e)) from e


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
    (design §6). Returns the restored group id and one entry per restored member
    volume (its id and the source member's name); each volume recovers its data
    asynchronously, the same as a single-volume restore.
    """
    group_id, members, error = group_backup_controller.restore_group_backup(
        group_backup.get_id(), parameters.into_group_name, parameters.pool,
        target_node_id=parameters.target_node_id,
        s3_credentials=parameters.s3_credentials)
    if error:
        raise HTTPException(422, error)
    return {"group_id": group_id, "members": members}


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
