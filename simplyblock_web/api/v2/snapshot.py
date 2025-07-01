from typing import Annotated, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, StringConstraints

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.snapshot import SnapShot as SnapshotModel
from simplyblock_core import utils as core_utils

from .cluster import Cluster
from .pool import Pool
from .lvol import instance_api as volume_instance_api
from . import util

api = APIRouter(prefix='/snapshots')
db = DBController()


class SnapshotDTO(BaseModel):
    id: UUID
    name: str
    status: str
    health_check: bool
    size: util.Unsigned
    used_size: util.Unsigned
    lvol: Optional[util.UrlPath]

    @staticmethod
    def from_model(model: SnapshotModel, cluster_id, pool_id):
        volume = db.get_lvol_by_id(model.lvol.get_id()) if model.lvol is not None else None

        return SnapshotDTO(
            id=model.get_id(),
            name=model.name,
            status=model.status,
            health_check=model.health_check,
            size=model.size,
            used_size=model.used_size,
            lvol=volume_instance_api.url_path_for(
                'get',
                cluster_id=cluster_id,
                pool_id=pool_id,
                volume_id=volume.get_id(),
            ) if volume is not None else None,
        )


@api.get('/')
def list(cluster: Cluster, pool: Pool) -> List[SnapshotDTO]:
    return [
        SnapshotDTO.from_model(snapshot, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot
        in db.get_snapshots()
        if snapshot.pool_uuid == pool.get_id()
    ]


instance_api = APIRouter(prefix='/<snapshot_id>')


def _lookup_snapshot(snapshot_id: Annotated[str, StringConstraints(pattern=core_utils.UUID_PATTERN)]) -> SnapshotModel:
    snapshot = db.get_snapshot_by_id(snapshot_id)
    if snapshot is None:
        raise HTTPException(404, 'Snapshot does not exist')
    return snapshot


Snapshot = Annotated[SnapshotModel, Depends(_lookup_snapshot)]


@instance_api.get('/')
def get(cluster: Cluster, pool: Pool, snapshot: Snapshot) -> SnapshotDTO:
    return SnapshotDTO.from_model(snapshot, cluster_id=cluster.get_id(), pool_id=pool.get_id())


@instance_api.delete('/', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: Pool, snapshot: Snapshot) -> Response:
    if not snapshot_controller.delete(snapshot.get_id()):
        raise ValueError('Failed to delete snapshot')

    return Response(status_code=204)


api.include_router(instance_api)
