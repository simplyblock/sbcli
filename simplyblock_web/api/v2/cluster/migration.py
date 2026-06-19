from typing import List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from .._dependencies import Cluster, Migration
from .._dtos import MigrationDTO
from ..util import CreationResponseFormatParameter, creation_response

api = APIRouter()


@api.get('/', name='clusters:storage-pools:volumes:migrations:list')
def list_migrations(cluster: Cluster, volume: Volume) -> List[MigrationDTO]:
    db = DBController()
    all_migrations = db.get_migrations(cluster.get_id())
    migrations = [m for m in all_migrations if m.lvol_id == volume.get_id()]
    return [MigrationDTO.from_model(m) for m in reversed(migrations)]


class _PreCreateParams(BaseModel):
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Annotated[str, Field(pattern=utils.NQN_PATTERN)] | None = None


@api.post('/', name='clusters:migrations:create', status_code=201, responses={201: {"content": None}})
def start_migration(request: Request, cluster: Cluster, parameters: _MigrateParams, response_format: CreationResponseFormatParameter = "full") -> Response:
    from simplyblock_core.controllers import migration_controller
    from simplyblock_core.db_controller import DBController
    migration_id, error = migration_controller.start_migration(
        parameters.volume_id,
        parameters.target_node_id,
        max_retries=parameters.max_retries,
        deadline_seconds=parameters.deadline_seconds,
    )
    if error:
        raise HTTPException(400, error)
    return creation_response(
        request, response_format,
        entity_id=UUID(migration_id),
        route_name='clusters:migrations:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id()), 'migration_id': UUID(migration_id)},
        get_full=lambda id: MigrationDTO.from_model(DBController().get_migration_by_id(str(id))),
    )


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='clusters:migrations:detail')
def get_migration(cluster: Cluster, migration: Migration) -> MigrationDTO:
    return MigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='clusters:storage-pools:volumes:migrations:continue', status_code=200)
def continue_migration(volume: Volume, migration: Migration, parameters: _ContinueParams):
    try:
        migration_id = migration_controller.start_migration(
            migration_id=migration.uuid,
            max_retries=parameters.max_retries,
            deadline_seconds=parameters.deadline_seconds,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"migration_id": migration_id}


@instance_api.post('/cancel', name='clusters:storage-pools:volumes:migrations:cancel', status_code=200)
def cancel_migration(volume: Volume, migration: Migration):
    try:
        migration_controller.cancel_migration(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}


api.include_router(instance_api)
