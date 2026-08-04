from typing import Annotated, List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_web import utils

from ...._dependencies import Cluster, Migration, Volume
from ...._dtos import BatchMigrationDTO, MigrationDTO
from ....util import CreationResponseFormatParameter, creation_response

api = APIRouter()
_db = DBController()


@api.get('/', name='clusters:storage-pools:volumes:migrations:list')
def list_migrations(
    cluster: Cluster,
    batch: Annotated[bool, Query(description='List batch migrations instead of solo migrations')] = False,
) -> List:
    if batch:
        groups = _db.get_migration_groups(cluster.get_id())
        return [BatchMigrationDTO.from_model(g) for g in reversed(groups)]
    migrations = _db.get_migrations(cluster.get_id())
    return [MigrationDTO.from_model(m) for m in reversed(migrations)]


class _MigrationParams(BaseModel):
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Optional[Annotated[str, Field(pattern=utils.NQN_PATTERN)]] = None
    batch: bool = False


@api.post('/', name='cluster:storage-pools:volumes:migrations:create', status_code=201, responses={201: {"content": None}})
def create_migration(
    request: Request,
    cluster: Cluster,
    volume: Volume,
    parameters: _MigrationParams,
    response_format: CreationResponseFormatParameter = "full",
) -> Response:
    try:
        if parameters.batch:
            group_id, connect_strings = migration_controller.create_batch_migration(
                str(volume.get_id()),
                str(parameters.target_node_id),
                ctrl_loss_tmo=parameters.ctrl_loss_tmo,
                host_nqn=parameters.host_nqn,
            )
            route_kw = {
                'cluster_id': UUID(cluster.uuid),
                'pool_id': UUID(volume.pool_uuid),
                'volume_id': UUID(volume.uuid),
                'migration_id': UUID(group_id),
            }
            batch_location = str(request.app.url_path_for(
                'cluster:storage-pools:volumes:migrations:detail', **route_kw)) + "?batch=true"
            return creation_response(
                request, response_format,
                entity_id=UUID(group_id),
                route_name='cluster:storage-pools:volumes:migrations:detail',
                route_kwargs=route_kw,
                get_full=lambda id: BatchMigrationDTO.from_model(
                    _db.get_migration_group_by_id(str(id)), connect_strings=connect_strings),
                extra_headers={"Location": batch_location},
            )
        else:
            migration_id, connect_strings = migration_controller.create_migration(
                str(volume.get_id()),
                str(parameters.target_node_id),
                ctrl_loss_tmo=parameters.ctrl_loss_tmo,
                host_nqn=parameters.host_nqn,
            )
            return creation_response(
                request, response_format,
                entity_id=UUID(migration_id),
                route_name='cluster:storage-pools:volumes:migrations:detail',
                route_kwargs={
                    'cluster_id': UUID(cluster.uuid),
                    'pool_id': UUID(volume.pool_uuid),
                    'volume_id': UUID(volume.uuid),
                    'migration_id': UUID(migration_id),
                },
                get_full=lambda id: MigrationDTO.from_model(
                    _db.get_migration_by_id(str(id)), connect_strings=connect_strings),
            )
    except (ValueError, MigrationConflictError, PreconditionError, RuntimeError) as e:
        raise HTTPException(400, str(e))


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='cluster:storage-pools:volumes:migrations:detail')
def get_migration(
    cluster: Cluster,
    volume: Volume,
    migration_id: UUID,
    batch: Annotated[bool, Query(description='Treat the ID as a batch migration ID')] = False,
) -> Union[MigrationDTO, BatchMigrationDTO]:
    if batch:
        try:
            group = _db.get_migration_group_by_id(str(migration_id))
        except KeyError:
            raise HTTPException(404, f'Batch migration {migration_id} not found')
        if group.cluster_id != cluster.get_id():
            raise HTTPException(404, f'Batch migration {migration_id} not found')
        return BatchMigrationDTO.from_model(group)
    try:
        migration = _db.get_migration_by_id(str(migration_id))
    except KeyError:
        raise HTTPException(404, f'Migration {migration_id} not found')
    if migration.lvol_id != volume.get_id():
        raise HTTPException(404, f'Migration {migration_id} not found')
    return MigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400
    batch: bool = False


@instance_api.post('/continue', name='cluster:storage-pools:volumes:migrations:continue', status_code=200)
def continue_migration(
    cluster: Cluster,
    volume: Volume,
    migration_id: UUID,
    parameters: _ContinueParams,
):
    try:
        if parameters.batch:
            try:
                group = _db.get_migration_group_by_id(str(migration_id))
            except KeyError:
                raise HTTPException(404, f'Batch migration {migration_id} not found')
            if group.cluster_id != cluster.get_id():
                raise HTTPException(404, f'Batch migration {migration_id} not found')
            result_id = migration_controller.start_batch_migration(
                group_id=group.uuid,
                max_retries=parameters.max_retries,
                deadline_seconds=parameters.deadline_seconds,
            )
            return {"migration_id": result_id}
        else:
            try:
                migration = _db.get_migration_by_id(str(migration_id))
            except KeyError:
                raise HTTPException(404, f'Migration {migration_id} not found')
            if migration.lvol_id != volume.get_id():
                raise HTTPException(404, f'Migration {migration_id} not found')
            result_id = migration_controller.start_migration(
                migration_id=migration.uuid,
                max_retries=parameters.max_retries,
                deadline_seconds=parameters.deadline_seconds,
            )
            return {"migration_id": result_id}
    except (ValueError, MigrationConflictError, PreconditionError, RuntimeError) as e:
        raise HTTPException(400, str(e))


@instance_api.delete('/', name='cluster:storage-pools:volumes:migrations:cancel', status_code=200)
def cancel_migration(
    cluster: Cluster,
    volume: Volume,
    migration_id: UUID,
    batch: Annotated[bool, Query(description='Treat the ID as a batch migration ID')] = False,
):
    try:
        if batch:
            try:
                group = _db.get_migration_group_by_id(str(migration_id))
            except KeyError:
                raise HTTPException(404, f'Batch migration {migration_id} not found')
            if group.cluster_id != cluster.get_id():
                raise HTTPException(404, f'Batch migration {migration_id} not found')
            migration_controller.cancel_batch_migration(group.uuid)
        else:
            try:
                migration = _db.get_migration_by_id(str(migration_id))
            except KeyError:
                raise HTTPException(404, f'Migration {migration_id} not found')
            if migration.lvol_id != volume.get_id():
                raise HTTPException(404, f'Migration {migration_id} not found')
            migration_controller.cancel_migration(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}


@instance_api.post('/cleanup-target', name='cluster:storage-pools:volumes:migrations:cleanup-target', status_code=200)
def cleanup_migration_target(_cluster: Cluster, migration: Migration):
    """
    Idempotently remove every object this migration created on the target node(s).

    Safe to call at any migration state — objects not found are reported as
    already cleaned up rather than as errors.  Returns a report of what was
    deleted, what was already gone, and any RPC errors encountered.
    """
    try:
        result = migration_controller.cleanup_migration_target(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return result


api.include_router(instance_api)
