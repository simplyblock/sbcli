from typing import List

from fastapi import APIRouter, Response, Request

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import snapshot_controller

from ..._dependencies import Cluster, StoragePool, Snapshot
from ..._dtos import SnapshotDTO


api = APIRouter()
db = DBController()


@api.get('/', name='clusters:storage-pools:snapshots:list')
def list(request: Request, cluster: Cluster, pool: StoragePool) -> List[SnapshotDTO]:
    return [
        SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot
        in db.get_snapshots()
        if snapshot.pool_uuid == pool.get_id()
    ]


instance_api = APIRouter(prefix='/{snapshot_id}')


@instance_api.get('/', name='clusters:storage-pools:snapshots:detail')
def get(request: Request, cluster: Cluster, pool: StoragePool, snapshot: Snapshot) -> SnapshotDTO:
    return SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())


@instance_api.delete('/', name='clusters:storage-pools:snapshots:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: StoragePool, snapshot: Snapshot) -> Response:
    if not snapshot_controller.delete(snapshot.get_id()):
        raise ValueError('Failed to delete snapshot')

    return Response(status_code=204)


api.include_router(instance_api)
