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
from .volume import Volume
from .dtos import MigrationDTO

api = APIRouter(prefix='/migrations')


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


@api.post('/', name='clusters:storage-pools:volumes:migrations:create', status_code=201)
def create_migration(volume: Volume, parameters: _PreCreateParams) -> MigrationDTO:
    """Set up the target NVMe-oF subsystem and bdev for a future migration.

    The client must nvme-connect to the returned connect_strings (inaccessible
    ANA state), then call /continue to begin the actual data transfer. This
    two-step split ensures target paths are established before cutover,
    eliminating the window where no active path exists.
    """
    try:
        migration_id, connect_strings = migration_controller.create_migration(
            volume.get_id(),
            str(parameters.target_node_id),
            ctrl_loss_tmo=parameters.ctrl_loss_tmo,
            host_nqn=parameters.host_nqn,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    db = DBController()
    migration = db.get_migration_by_id(migration_id)
    return MigrationDTO.from_model(migration, connect_strings=connect_strings)


instance_api = APIRouter(prefix='/{migration_id}')


def _lookup_migration(migration_id: UUID, volume: Volume) -> LVolMigration:
    db = DBController()
    try:
        migration = db.get_migration_by_id(str(migration_id))
    except KeyError as e:
        raise HTTPException(404, str(e))
    if migration.lvol_id != volume.get_id():
        raise HTTPException(404, f'Migration {migration_id} not found')
    return migration


Migration = Annotated[LVolMigration, Depends(_lookup_migration)]


@instance_api.get('/', name='clusters:storage-pools:volumes:migrations:detail')
def get_migration(volume: Volume, migration: Migration) -> MigrationDTO:
    return MigrationDTO.from_model(migration)


class _ContinueParams(BaseModel):
    max_retries: int = 10
    deadline_seconds: int = 14400


@instance_api.post('/continue', name='clusters:storage-pools:volumes:migrations:continue', status_code=200)
def continue_migration(volume: Volume, migration: Migration, parameters: _ContinueParams):
    try:
        migration_id = migration_controller.start_migration(
            migration_id=migration.get_id(),
            max_retries=parameters.max_retries,
            deadline_seconds=parameters.deadline_seconds,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"migration_id": migration_id}


@instance_api.post('/cancel', name='clusters:storage-pools:volumes:migrations:cancel', status_code=200)
def cancel_migration(volume: Volume, migration: Migration):
    try:
        migration_controller.cancel_migration(migration.get_id())
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"status": "cancelled"}
