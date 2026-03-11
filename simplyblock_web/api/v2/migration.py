from typing import List, TYPE_CHECKING

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

if TYPE_CHECKING:
    from simplyblock_core.controllers import migration_controller
    from simplyblock_core.db_controller import DBController

from .cluster import Cluster
from .dtos import MigrationDTO

api = APIRouter(prefix='/migrations')


@api.get('/', name='clusters:migrations:list')
def list_migrations(cluster: Cluster) -> List[MigrationDTO]:
    from simplyblock_core.db_controller import DBController
    db = DBController()
    migrations = db.get_migrations(cluster.get_id())
    return [MigrationDTO.from_model(m) for m in reversed(migrations)]


class _MigrateParams(BaseModel):
    volume_id: str
    target_node_id: str
    max_retries: int = 10
    deadline_seconds: int = 14400


@api.post('/', name='clusters:migrations:create', status_code=201)
def start_migration(cluster: Cluster, parameters: _MigrateParams):
    from simplyblock_core.controllers import migration_controller
    migration_id, error = migration_controller.start_migration(
        parameters.volume_id,
        parameters.target_node_id,
        max_retries=parameters.max_retries,
        deadline_seconds=parameters.deadline_seconds,
    )
    if error:
        raise HTTPException(400, error)
    return {"migration_id": migration_id}


instance_api = APIRouter(prefix='/{migration_id}')


@instance_api.get('/', name='clusters:migrations:detail')
def get_migration(cluster: Cluster, migration_id: str) -> MigrationDTO:
    from simplyblock_core.db_controller import DBController
    db = DBController()
    try:
        migration = db.get_migration_by_id(migration_id)
    except KeyError as e:
        raise HTTPException(404, str(e))
    return MigrationDTO.from_model(migration)


@instance_api.post('/cancel', name='clusters:migrations:cancel', status_code=200)
def cancel_migration(cluster: Cluster, migration_id: str):
    from simplyblock_core.controllers import migration_controller
    ok, error = migration_controller.cancel_migration(migration_id)
    if not ok:
        raise HTTPException(400, error)
    return {"status": "cancelled"}
