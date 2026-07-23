from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_web import utils

from .._dependencies import Cluster, MigrationGroup
from .._dtos import MigrationGroupDTO
from ..util import CreationResponseFormatParameter, creation_response

api = APIRouter()
db = DBController()


@api.get('/', name='clusters:migration-groups:list')
def list_migration_groups(cluster: Cluster) -> List[MigrationGroupDTO]:
    groups = db.get_migration_groups(cluster.get_id())
    return [MigrationGroupDTO.from_model(g) for g in reversed(groups)]


class _MigrationGroupParams(BaseModel):
    lvol_id: UUID
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Optional[Annotated[str, Field(pattern=utils.NQN_PATTERN)]] = None


@api.post('/', name='clusters:migration-groups:create', status_code=201, responses={201: {"content": None}})
def create_migration_group(request: Request, cluster: Cluster, parameters: _MigrationGroupParams, response_format: CreationResponseFormatParameter = "full") -> Response:
    try:
        group_id, connect_strings = migration_controller.create_batch_migration(
            str(parameters.lvol_id),
            str(parameters.target_node_id),
            ctrl_loss_tmo=parameters.ctrl_loss_tmo,
            host_nqn=parameters.host_nqn,
        )
    except (ValueError, MigrationConflictError, PreconditionError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return creation_response(
        request, response_format,
        entity_id=UUID(group_id),
        route_name='clusters:migration-groups:detail',
        route_kwargs={
            'cluster_id': UUID(cluster.uuid),
            'group_id': UUID(group_id),
        },
        get_full=lambda id: MigrationGroupDTO.from_model(
            db.get_migration_group_by_id(str(id)), connect_strings=connect_strings),
    )


instance_api = APIRouter(prefix='/{group_id}')


@instance_api.get('/', name='clusters:migration-groups:detail')
def get_migration_group(cluster: Cluster, group: MigrationGroup) -> MigrationGroupDTO:
    return MigrationGroupDTO.from_model(group)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='clusters:migration-groups:continue', status_code=200)
def continue_migration_group(cluster: Cluster, group: MigrationGroup, parameters: _ContinueParams):
    try:
        group_id = migration_controller.start_batch_migration(
            group_id=group.uuid,
            max_retries=parameters.max_retries,
            deadline_seconds=parameters.deadline_seconds,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"group_id": group_id}


@instance_api.delete('/', name='clusters:migration-groups:cancel', status_code=200)
def cancel_migration_group(cluster: Cluster, group: MigrationGroup):
    try:
        migration_controller.cancel_batch_migration(group.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}


api.include_router(instance_api)
