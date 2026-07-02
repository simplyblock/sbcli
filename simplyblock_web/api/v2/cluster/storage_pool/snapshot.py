from typing import Callable, List, Union

from fastapi import APIRouter, Response, Request
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core.models.pool import Pool as PoolModel
from simplyblock_core.models.snapshot import SnapShot as SnapshotModel

from ..._dependencies import Cluster, StoragePool, Snapshot
from ..._dtos import SnapshotDTO
from ..._watch import WATCH_RESPONSES, WatchParam, watch_response


api = APIRouter()
db = DBController()


def _make_snapshot_dto(request: Request, cluster_id: str, pool_id: str) -> Callable[[dict], SnapshotDTO]:
    def build(data: dict) -> SnapshotDTO:
        return SnapshotDTO.from_model(SnapshotModel(data), request, cluster_id=cluster_id, pool_id=pool_id)
    return build


@api.get('/', name='clusters:storage-pools:snapshots:list', response_model=List[SnapshotDTO], responses=WATCH_RESPONSES)
def list(request: Request, cluster: Cluster, pool: StoragePool, watch: WatchParam = False) -> Union[List[SnapshotDTO], EventSourceResponse]:
    if watch:
        pool_id = pool.get_id()
        return watch_response(
            SnapshotModel,
            lambda state: {k: d for k, d in state.items() if d.get('pool_uuid') == pool_id},
            _make_snapshot_dto(request, cluster.get_id(), pool_id),
            ancestors=[(ClusterModel, cluster.get_id()), (PoolModel, pool_id)],
        )
    return [
        SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot in db.get_snapshots_by_pool_id(pool.get_id())
    ]


instance_api = APIRouter(prefix='/{snapshot_id}')


@instance_api.get('/', name='clusters:storage-pools:snapshots:detail', response_model=SnapshotDTO, responses=WATCH_RESPONSES)
def get(request: Request, cluster: Cluster, pool: StoragePool, snapshot: Snapshot, watch: WatchParam = False) -> Union[SnapshotDTO, EventSourceResponse]:
    if watch:
        snapshot_id = snapshot.get_id()
        return watch_response(
            SnapshotModel,
            lambda state: {k: d for k, d in state.items() if d.get('uuid') == snapshot_id},
            _make_snapshot_dto(request, cluster.get_id(), pool.get_id()),
            single_id=snapshot_id,
            ancestors=[(ClusterModel, cluster.get_id()), (PoolModel, pool.get_id())],
        )
    return SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())


@instance_api.delete('/', name='clusters:storage-pools:snapshots:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: StoragePool, snapshot: Snapshot) -> Response:
    if not snapshot_controller.delete(snapshot.get_id()):
        raise ValueError('Failed to delete snapshot')

    return Response(status_code=204)


api.include_router(instance_api)
