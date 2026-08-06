from typing import Annotated, List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_web import utils

from ..._dependencies import BatchMigration, Cluster, Subsystem
from ..._dtos import BatchMigrationDTO
from ...util import CreationResponseFormatParameter, creation_response

api = APIRouter()
_db = DBController()


@api.get('/', name='clusters:subsystems:migrations:list')
def list_migrations(cluster: Cluster, subsystem: Subsystem) -> List[BatchMigrationDTO]:
    groups = [
        g for g in _db.get_migration_groups(cluster.get_id())
        if g.target_nqn == subsystem
    ]
    return [BatchMigrationDTO.from_model(g) for g in reversed(groups)]


class _MigrationParams(BaseModel):
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Optional[Annotated[str, Field(pattern=utils.NQN_PATTERN)]] = None


def _resolve_member_lvol(cluster_id: str, nqn: str):
    """Pick the canonical member (lowest ns_id) of the shared subsystem
    `nqn` to bootstrap a batch migration from. create_batch_migration()
    only needs any one member's ID — it discovers the rest itself.
    """
    members = sorted(
        (lv for lv in _db.get_lvols(cluster_id) if lv.nqn == nqn),
        key=lambda lv: lv.ns_id,
    )
    if not members:
        raise HTTPException(404, f'Subsystem {nqn} not found')
    return members[0]


@api.post('/', name='clusters:subsystems:migrations:create', status_code=201, responses={201: {"content": None}})
def create_migration(
    request: Request,
    cluster: Cluster,
    subsystem: Subsystem,
    parameters: _MigrationParams,
    response_format: CreationResponseFormatParameter = "full",
) -> Response:
    lvol = _resolve_member_lvol(cluster.get_id(), subsystem)
    try:
        group_id, connect_strings = migration_controller.create_batch_migration(
            str(lvol.get_id()),
            str(parameters.target_node_id),
            ctrl_loss_tmo=parameters.ctrl_loss_tmo,
            host_nqn=parameters.host_nqn,
        )
    except (ValueError, MigrationConflictError, PreconditionError, RuntimeError) as e:
        raise HTTPException(400, str(e))

    route_kw: dict[str, Union[UUID, str]] = {
        'cluster_id': UUID(cluster.uuid),
        'nqn': subsystem,
        'migration_id': UUID(group_id),
    }
    return creation_response(
        request, response_format,
        entity_id=UUID(group_id),
        route_name='clusters:subsystems:migrations:detail',
        route_kwargs=route_kw,
        get_full=lambda id: BatchMigrationDTO.from_model(
            _db.get_migration_group_by_id(str(id)), connect_strings=connect_strings),
    )


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='clusters:subsystems:migrations:detail')
def get_migration(migration: BatchMigration) -> BatchMigrationDTO:
    return BatchMigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='clusters:subsystems:migrations:continue', status_code=200)
def continue_migration(migration: BatchMigration, parameters: _ContinueParams):
    try:
        result_id = migration_controller.start_batch_migration(
            group_id=migration.uuid,
            max_retries=parameters.max_retries,
            deadline_seconds=parameters.deadline_seconds,
        )
    except (ValueError, MigrationConflictError, PreconditionError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"migration_id": result_id}


@instance_api.delete('/', name='clusters:subsystems:migrations:cancel', status_code=200)
def cancel_migration(migration: BatchMigration):
    try:
        migration_controller.cancel_batch_migration(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}


api.include_router(instance_api)
