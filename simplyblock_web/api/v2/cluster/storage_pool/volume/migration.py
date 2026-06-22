from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_web import utils

from ...._dependencies import Cluster, Migration
from ...._dtos import MigrationDTO
from ....util import CreationResponseFormatParameter, creation_response

api = APIRouter()


@api.get('/', name='clusters:storage-pools:volumes:migrations:list')
def list_migrations(cluster: Cluster) -> List[MigrationDTO]:
    db = DBController()
    migrations = db.get_migrations(cluster.get_id())
    return [MigrationDTO.from_model(m) for m in reversed(migrations)]


class _PreCreateParams(BaseModel):
    volume_id: UUID
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Annotated[str, Field(pattern=utils.NQN_PATTERN)] | None = None


@api.post('/', name='cluster:storage-pools:volumes:migrations:create', status_code=201, responses={201: {"content": None}})
def create_migration(request: Request, cluster: Cluster, parameters: _PreCreateParams, response_format: CreationResponseFormatParameter = "identifier") -> Response:
    try:
        migration_id, connect_strings = migration_controller.create_migration(
            str(parameters.volume_id),
            str(parameters.target_node_id),
            ctrl_loss_tmo=parameters.ctrl_loss_tmo,
            host_nqn=parameters.host_nqn,
        )
    except (ValueError, MigrationConflictError, PreconditionError) as e:
        raise HTTPException(400, str(e))
    db = DBController()
    return creation_response(
        request, response_format,
        entity_id=UUID(migration_id),
        route_name='cluster:storage-pools:volumes:migrations:detail',
        route_kwargs={'cluster_id': UUID(cluster.get_id()), 'migration_id': UUID(migration_id)},
        get_full=lambda id: MigrationDTO.from_model(
            db.get_migration_by_id(str(id)), connect_strings=connect_strings),
    )


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='cluster:storage-pools:volumes:migrations:detail')
def get_migration(cluster: Cluster, migration: Migration) -> MigrationDTO:
    return MigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='cluster:storage-pools:volumes:migrations:continue', status_code=200)
def continue_migration(cluster: Cluster, migration: Migration, parameters: _ContinueParams):
    try:
        migration_id = migration_controller.start_migration(
            migration_id=migration.uuid,
            max_retries=parameters.max_retries,
            deadline_seconds=parameters.deadline_seconds,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"migration_id": migration_id}


@instance_api.delete('/', name='cluster:storage-pools:volumes:migrations:cancel', status_code=200)
def cancel_migration(cluster: Cluster, migration: Migration):
    ok, error = migration_controller.cancel_migration(migration.get_id())
    if not ok:
        raise HTTPException(400, error)
    return {"status": "cancelled"}


api.include_router(instance_api)
