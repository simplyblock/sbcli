from typing import Annotated, List, Optional, Union
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import MigrationConflictError, PreconditionError
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_web import utils

from ..._dependencies import Cluster, Subsystem, SubsystemMigration
from ..._dtos import BatchMigrationDTO, MigrationDTO
from ...util import CreationResponseFormatParameter, creation_response

api = APIRouter()
_db = DBController()


@api.get('/', name='clusters:subsystems:migrations:list')
def list_migrations(cluster: Cluster, subsystem: Subsystem) -> List[Union[MigrationDTO, BatchMigrationDTO]]:
    groups = [
        g for g in _db.get_migration_groups(cluster.get_id())
        if g.target_nqn == subsystem
    ]

    # Standalone single-lvol migrations for this subsystem — excludes worker
    # migrations that belong to one of the groups above (migration_group_id
    # set), which the group already represents.
    singles = []
    for m in _db.get_migrations(cluster.get_id()):
        if m.migration_group_id:
            continue
        try:
            lvol = _db.get_lvol_by_id(m.lvol_id)
        except KeyError:
            continue
        if lvol.nqn == subsystem:
            singles.append(m)

    items = groups + singles
    return [
        BatchMigrationDTO.from_model(m) if isinstance(m, LVolMigrationGroup) else MigrationDTO.from_model(m)
        for m in reversed(items)
    ]


class _MigrationParams(BaseModel):
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Optional[Annotated[str, Field(pattern=utils.NQN_PATTERN)]] = None


def _resolve_member_lvol(cluster_id: str, nqn: str):
    """Pick the canonical member (lowest ns_id) of the subsystem `nqn` to
    bootstrap a migration from. create_batch_migration() only needs any one
    member's ID — it discovers the rest itself.
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

    # max_namespace_per_subsys > 1 means this lvol's subsystem is a candidate
    # for a coordinated batch (shared-namespace) migration — no explicit
    # --batch flag needed, the subsystem's own configured capacity decides.
    is_batch = lvol.max_namespace_per_subsys > 1
    try:
        if is_batch:
            migration_id, connect_strings = migration_controller.create_batch_migration(
                str(lvol.get_id()),
                str(parameters.target_node_id),
                ctrl_loss_tmo=parameters.ctrl_loss_tmo,
                host_nqn=parameters.host_nqn,
            )
        else:
            migration_id, connect_strings = migration_controller.create_migration(
                str(lvol.get_id()),
                str(parameters.target_node_id),
                ctrl_loss_tmo=parameters.ctrl_loss_tmo,
                host_nqn=parameters.host_nqn,
            )
    except (MigrationConflictError, PreconditionError) as e:
        # Conflicting/not-yet-satisfiable state (e.g. a migration already
        # active for this subsystem, or -- for a fallback-source migration --
        # the chosen target is the node currently serving as the fallback
        # source itself) -- matches the 409 convention used for the same
        # shape of error elsewhere in v2 (storage_node shutdown, pool/volume
        # already-exists, in-flight replication cutover).
        raise HTTPException(409, str(e))
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))

    def get_full(id):
        if is_batch:
            return BatchMigrationDTO.from_model(
                _db.get_migration_group_by_id(str(id)), connect_strings=connect_strings)
        return MigrationDTO.from_model(
            _db.get_migration_by_id(str(id)), connect_strings=connect_strings)

    return creation_response(
        request, response_format,
        entity_id=UUID(migration_id),
        route_name='clusters:subsystems:migrations:detail',
        route_kwargs={
            'cluster_id': UUID(cluster.uuid),
            'nqn': subsystem,
            'migration_id': UUID(migration_id),
        },
        get_full=get_full,
    )


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='clusters:subsystems:migrations:detail')
def get_migration(migration: SubsystemMigration) -> Union[MigrationDTO, BatchMigrationDTO]:
    if isinstance(migration, LVolMigrationGroup):
        return BatchMigrationDTO.from_model(migration)
    return MigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='clusters:subsystems:migrations:continue', status_code=200)
def continue_migration(migration: SubsystemMigration, parameters: _ContinueParams):
    try:
        if isinstance(migration, LVolMigrationGroup):
            result_id = migration_controller.start_batch_migration(
                group_id=migration.uuid,
                max_retries=parameters.max_retries,
                deadline_seconds=parameters.deadline_seconds,
            )
        else:
            result_id = migration_controller.start_migration(
                migration_id=migration.uuid,
                max_retries=parameters.max_retries,
                deadline_seconds=parameters.deadline_seconds,
            )
    except (MigrationConflictError, PreconditionError) as e:
        # Conflicting/not-yet-satisfiable state (e.g. a migration already
        # active for this subsystem, or -- for a fallback-source migration --
        # the chosen target is the node currently serving as the fallback
        # source itself) -- matches the 409 convention used for the same
        # shape of error elsewhere in v2 (storage_node shutdown, pool/volume
        # already-exists, in-flight replication cutover).
        raise HTTPException(409, str(e))
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))
    return {"migration_id": result_id}


@instance_api.delete('/', name='clusters:subsystems:migrations:cancel', status_code=200)
def cancel_migration(migration: SubsystemMigration):
    try:
        if isinstance(migration, LVolMigrationGroup):
            migration_controller.cancel_batch_migration(migration.uuid)
        else:
            migration_controller.cancel_migration(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}


@instance_api.post('/cleanup-target', name='clusters:subsystems:migrations:cleanup-target', status_code=200)
def cleanup_migration_target(migration: SubsystemMigration):
    """
    Idempotently remove every object this migration created on the target
    node(s). Only defined for a single-lvol migration; batch migration groups
    have no cleanup-target equivalent at the group level.

    Safe to call at any migration state — objects not found are reported as
    already cleaned up rather than as errors.
    """
    if isinstance(migration, LVolMigrationGroup):
        raise HTTPException(
            400,
            f"{migration.uuid} is a batch migration group; cleanup-target is "
            f"not supported at the group level.")
    try:
        result = migration_controller.cleanup_migration_target(migration.uuid)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return result


api.include_router(instance_api)
