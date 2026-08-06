from typing import Callable, List, Union

from fastapi import APIRouter, Response, Request
from sse_starlette import EventSourceResponse

from simplyblock_core.db_controller import DBController
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.snapshot import SnapShot as SnapshotModel

from ..._dependencies import Cluster, StoragePool, Snapshot
from ..._dtos import SnapshotDTO
from ..._sse import WATCH_RESPONSES, WatchParam, sse_response


api = APIRouter()
db = DBController()


def _make_snapshot_dto(request: Request, cluster_id: str, pool_id: str) -> Callable[[SnapshotModel], SnapshotDTO]:
    def build(snapshot: SnapshotModel) -> SnapshotDTO:
        return SnapshotDTO.from_model(snapshot, request, cluster_id=cluster_id, pool_id=pool_id)
    return build


@api.get('/', name='clusters:storage-pools:snapshots:list', response_model=List[SnapshotDTO], responses=WATCH_RESPONSES)
def list(request: Request, cluster: Cluster, pool: StoragePool, watch: WatchParam = False) -> Union[List[SnapshotDTO], EventSourceResponse]:
    if watch:
        return sse_response(
            snapshot_controller.watch_snapshots(cluster.get_id(), pool.get_id()),
            _make_snapshot_dto(request, cluster.get_id(), pool.get_id()),
        )
    return [
        SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())
        for snapshot in db.get_snapshots_by_pool_id(pool.get_id())
    ]


instance_api = APIRouter(prefix='/{snapshot_id}')


@instance_api.get('/', name='clusters:storage-pools:snapshots:detail', response_model=SnapshotDTO, responses=WATCH_RESPONSES)
def get(request: Request, cluster: Cluster, pool: StoragePool, snapshot: Snapshot, watch: WatchParam = False) -> Union[SnapshotDTO, EventSourceResponse]:
    if watch:
        return sse_response(
            snapshot_controller.watch_snapshot(cluster.get_id(), pool.get_id(), snapshot.get_id()),
            _make_snapshot_dto(request, cluster.get_id(), pool.get_id()),
            single=True,
        )
    return SnapshotDTO.from_model(snapshot, request, cluster_id=cluster.get_id(), pool_id=pool.get_id())


@instance_api.delete('/', name='clusters:storage-pools:snapshots:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster, pool: StoragePool, snapshot: Snapshot) -> Response:
    if not snapshot_controller.delete(snapshot.get_id()):
        raise ValueError('Failed to delete snapshot')

    return Response(status_code=204)


api.include_router(instance_api)
