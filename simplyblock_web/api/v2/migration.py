from typing import Annotated, List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from simplyblock_core.controllers import migration_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_migration import LVolMigration

from simplyblock_core import constants
from simplyblock_web import utils
from .cluster import Cluster
from .dtos import MigrationDTO

api = APIRouter(prefix='/migrations')


@api.get('/', name='clusters:migrations:list')
def list_migrations(cluster: Cluster) -> List[MigrationDTO]:
    db = DBController()
    migrations = db.get_migrations(cluster.get_id())
    return [MigrationDTO.from_model(m) for m in reversed(migrations)]


class _MigrateParams(BaseModel):
    migration_id: UUID
    max_retries: int = 10
    deadline_seconds: int = 14400


class _PreCreateParams(BaseModel):
    volume_id: UUID
    target_node_id: UUID
    ctrl_loss_tmo: int = constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO
    host_nqn: Annotated[str, Field(pattern=utils.NQN_PATTERN)] | None = None


class _PreCreateResponse(BaseModel):
    migration_id: UUID
    connect_strings: list[dict]


@api.post('/pre-create', name='clusters:migrations:pre-create', status_code=200)
def pre_create_on_target(cluster: Cluster, parameters: _PreCreateParams) -> _PreCreateResponse:
    """Pre-create the target NVMe-oF subsystem and bdev for a future migration.

    Returns the list of NVMe connect strings (inaccessible ANA state) for the
    target node so that the client can pre-connect before migration begins.
    """
    try:
        migration_id, connect_strings = migration_controller.pre_create_on_target(
            str(parameters.volume_id),
            str(parameters.target_node_id),
            ctrl_loss_tmo=parameters.ctrl_loss_tmo,
            host_nqn=parameters.host_nqn,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return _PreCreateResponse(migration_id=UUID(migration_id), connect_strings=connect_strings)


@api.post('/', name='clusters:migrations:create', status_code=201)
def start_migration(cluster: Cluster, parameters: _MigrateParams):
    migration_id, error = migration_controller.start_migration(
        migration_id=str(parameters.migration_id),
        max_retries=parameters.max_retries,
        deadline_seconds=parameters.deadline_seconds,
    )
    if error:
        raise HTTPException(400, error)
    return {"migration_id": migration_id}


instance_api = APIRouter(prefix='/{migration_id}')


def _lookup_migration(migration_id: UUID, cluster: Cluster) -> LVolMigration:
    db = DBController()
    try:
        migration = db.get_migration_by_id(str(migration_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if migration.cluster_id != cluster.get_id():
        raise HTTPException(404, f'Migration {migration_id} not found')
    return migration


Migration = Annotated[LVolMigration, Depends(_lookup_migration)]


@instance_api.get('/', name='clusters:migrations:detail')
def get_migration(cluster: Cluster, migration: Migration) -> MigrationDTO:
    return MigrationDTO.from_model(migration)


@instance_api.post('/cancel', name='clusters:migrations:cancel', status_code=200)
def cancel_migration(cluster: Cluster, migration: Migration):
    ok, error = migration_controller.cancel_migration(migration.get_id())
    if not ok:
        raise HTTPException(400, error)
    return {"status": "cancelled"}
