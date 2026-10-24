"""Consistency-group backup REST surface.

Group backups are backups, so they live under the backup tree, at
`/clusters/{cluster_id}/backups/groups/`: creating one from a generation of the
consistency group the body names, listing them, reading one and its member
backups, restoring one into a new consistency group, deleting one, and the
export, import, and discover a single-volume backup has. The endpoints are thin.
Create fans out to the single-volume backup path per member, restore fans out to
the single-volume restore path per member and re-forms the group, and delete
removes the group manifest and the record, leaving the member backups to
per-volume retention.

This router is mounted ahead of the single-volume one, whose
`/backups/{backup_id}/` would otherwise take `groups` for a backup id.
"""
from typing import Union
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict

from simplyblock_core.controllers.backup import group_backup as group_backup_controller
from simplyblock_core.controllers.backup.manifest import ManifestError
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.backup_config import S3Credentials

from .._dependencies import Cluster, GroupBackupResource
from .._dtos import (
    BackupConfigDTO,
    BackupDTO,
    GroupBackupDTO,
    GroupBackupExportDTO,
    GroupBackupManifestDTO,
)

api = APIRouter(tags=['group-backups'])
db = DBController()


@api.get('/', name='clusters:backups:groups:list', response_model=list[GroupBackupDTO])
def list_group_backups(cluster: Cluster) -> list[GroupBackupDTO]:
    """List the cluster's group backups, newest first."""
    group_backups = db.get_group_backups(cluster.get_id())
    group_backups = sorted(group_backups, key=lambda g: (g.created_at, g.uuid), reverse=True)
    return [GroupBackupDTO.from_model(g) for g in group_backups]


class _GroupBackupCreateParams(BaseModel):
    #: The consistency group to back up.
    consistency_group_id: UUID
    #: Back up an existing generation by its sequence. Omit to take a fresh
    #: crash-consistent generation and back that up.
    group_seq: int | None = None


@api.post('/', name='clusters:backups:groups:create', status_code=201, response_model=GroupBackupDTO)
def create_group_backup(cluster: Cluster, body: _GroupBackupCreateParams) -> GroupBackupDTO:
    """Back up one generation of a consistency group to S3 as one all-or-nothing
    unit. Takes a fresh crash-consistent generation unless
    ``group_seq`` names an existing one. Returns the in-progress group backup,
    which commits once every member backup completes.
    """
    try:
        group = db.get_consistency_group_by_id(str(body.consistency_group_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if group.cluster_id != cluster.get_id():
        raise HTTPException(404, f'ConsistencyGroup {body.consistency_group_id} not found')
    group_backup_id, err = group_backup_controller.create_group_backup(
        group.get_id(), group_seq=body.group_seq)
    if err is not None:
        # 409, not 422: FastAPI reserves 422 for its own validation error, whose
        # body is a list the generated clients decode; a string detail there is
        # undecodable (see restore below).
        raise HTTPException(409, err)
    return GroupBackupDTO.from_model(db.get_group_backup_by_id(group_backup_id))


@api.get('/export', name='clusters:backups:groups:export')
def export_group_backup(
    cluster: Cluster,
    group_backup_id: str = Query(..., description="Export the group backup with this UUID"),
) -> GroupBackupExportDTO:
    """Export a group backup's manifests as a unit, for import elsewhere.

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


@api.post('/import', name='clusters:backups:groups:import')
def import_group_backup(cluster: Cluster, parameters: _GroupImportParams):
    """Import a group backup, preserving ids, all-or-nothing."""
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


@api.post('/discover', name='clusters:backups:groups:discover')
def discover_group_backups(parameters: BackupConfigDTO) -> list[GroupBackupManifestDTO]:
    """List the group backups a bucket contains, without importing anything.

    A POST because it carries credentials, which have no business in a query
    string. Takes no cluster state at all: this is what an operator runs when the
    cluster that wrote the group backups no longer exists.
    """
    try:
        return group_backup_controller.discover_group_backups(parameters)
    except ManifestError as e:
        raise HTTPException(400, str(e)) from e


instance_api = APIRouter(prefix='/{group_backup_id}')


@instance_api.get('/', name='clusters:backups:groups:detail', response_model=GroupBackupDTO)
def get_group_backup(cluster: Cluster, group_backup: GroupBackupResource) -> GroupBackupDTO:
    return GroupBackupDTO.from_model(group_backup)


@instance_api.get('/members/', name='clusters:backups:groups:members', response_model=list[BackupDTO])
def list_group_backup_members(cluster: Cluster, group_backup: GroupBackupResource) -> list[BackupDTO]:
    """The group backup's member backups, as the single-volume backup records
    they are: one per member volume of the generation."""
    return [BackupDTO.from_model(b) for b in db.get_backups_by_group_backup_id(group_backup.get_id())]


class _GroupRestoreParams(BaseModel):
    #: Name of the NEW consistency group the restore forms; must not already
    #: exist: a group restore always produces a new group.
    into_group_name: str
    pool: str
    target_node_id: str | None = None
    #: Credentials for the backup's bucket when it is not this cluster's own --
    #: the disaster-recovery case. Omit to use the nodes' instance role.
    s3_credentials: S3Credentials | None = None


@instance_api.post('/restore', name='clusters:backups:groups:restore', status_code=202)
def restore_group_backup(cluster: Cluster, group_backup: GroupBackupResource,
                         parameters: _GroupRestoreParams):
    """Restore a group backup into a NEW consistency group, all-or-nothing.
    Returns the restored group id and one entry per restored member
    volume (its id and the source member's name); each volume recovers its data
    asynchronously, the same as a single-volume restore.
    """
    group_id, members, error = group_backup_controller.restore_group_backup(
        group_backup.get_id(), parameters.into_group_name, parameters.pool,
        target_node_id=parameters.target_node_id,
        s3_credentials=parameters.s3_credentials)
    if error:
        # 409, not 422: FastAPI reserves 422 for its own request-validation
        # error, whose body is a LIST of field errors, and every generated
        # client types it so. A string detail under 422 is undecodable to the
        # operator ("cannot unmarshal string into ... HTTPValidationError.detail",
        # live 2026-10-20), which hid the refusal behind a parse error.
        raise HTTPException(409, error)
    return {"group_id": group_id, "members": members}


@instance_api.delete('/', name='clusters:backups:groups:delete',
                     status_code=202, responses={202: {"content": None}})
def delete_group_backup(cluster: Cluster, group_backup: GroupBackupResource) -> Response:
    """Start deleting a group backup. Its record and manifest are
    marked deleting, and a task retires its member backups and then removes
    both; the record reads ``deleting`` until then, and ``broken`` if the delete
    could not finish after retiring a member."""
    try:
        group_backup_controller.delete_group_backup(group_backup.get_id())
    except PreconditionError as e:
        raise HTTPException(409, str(e)) from e
    except ManifestError as e:
        raise HTTPException(400, str(e)) from e
    return Response(status_code=202)


api.include_router(instance_api)
