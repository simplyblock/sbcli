from threading import Thread
from typing import Annotated, List, Literal, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster as ClusterModel
from simplyblock_core import cluster_ops

from .dtos import ClusterDTO
from . import util as util


api = APIRouter(prefix='/clusters')
db = DBController()


class _UpdateParams(BaseModel):
    management_image: Optional[str]
    spdk_image: Optional[str]
    restart: bool = Field(False)


class ClusterParams(BaseModel):
    blk_size: Literal[512, 4096] = 512
    page_size_in_blocks: int = Field(2097152, gt=0)
    cap_warn: util.Percent = 0
    cap_crit: util.Percent = 0
    prov_cap_warn: util.Percent = 0
    prov_cap_crit: util.Percent = 0
    distr_ndcs: int = 1
    distr_npcs: int = 1
    distr_bs: int = 4096
    distr_chunk_bs: int = 4096
    ha_type: Literal['single', 'ha'] = 'single'
    qpair_count: int = 256
    max_queue_size: int = 128
    inflight_io_threshold: int = 4
    enable_qos: bool = False
    enable_node_affinity: bool = False
    strict_node_anti_affinity: bool = False


@api.get('/', name='clusters:list')
def list() -> List[ClusterDTO]:
    return [
        ClusterDTO.from_model(cluster)
        for cluster
        in db.get_clusters()
    ]


@api.post('/', name='clusters:create', status_code=201, responses={201: {"content": None}})
def add(request: Request, parameters: ClusterParams):
    cluster_id_or_false = cluster_ops.add_cluster(**parameters.model_dump())
    if not cluster_id_or_false:
        raise ValueError('Failed to create cluster')

    entity_url = request.app.url_path_for('get', cluster_id=cluster_id_or_false)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{cluster_id}')


def _lookup_cluster(cluster_id: UUID):
    try:
        return db.get_cluster_by_id(str(cluster_id))
    except KeyError as e:
        raise HTTPException(404, str(e))


Cluster = Annotated[ClusterModel, Depends(_lookup_cluster)]


@instance_api.get('/', name='clusters:detail')
def get(cluster: Cluster) -> ClusterDTO:
    return ClusterDTO.from_model(cluster)


@instance_api.delete('/', name='clusters:delete', status_code=204, responses={204: {"content": None}})
def delete(cluster: Cluster) -> Response:
    cluster_ops.delete_cluster(cluster.get_id())
    return Response(status_code=204)


@instance_api.get('/capacity', name='clusters:capacity')
def capacity(cluster: Cluster, history: Optional[str] = None):
    capacity_or_false = cluster_ops.get_capacity(
            cluster.get_id(), history)
    if not capacity_or_false:
        raise ValueError('Failed to compute capacity')

    return capacity_or_false


@instance_api.get('/iostats', name='clusters:iostats')
def iostats(cluster: Cluster, history: Optional[str] = None):
    iostats_or_false = cluster_ops.get_iostats_history(
            cluster.get_id(), history, with_sizes=True)
    if not iostats_or_false:
        raise ValueError('Failed to compute capacity')

    return iostats_or_false


@instance_api.get('/logs', name='clusters:logs')
def logs(cluster: Cluster, limit: int = 50):
    logs_or_false = cluster_ops.get_logs(
            cluster.get_id(), is_json=True, limit=limit)
    if not logs_or_false:
        raise ValueError('Failed to access logs')

    return logs_or_false


@instance_api.post('/start', name='clusters:start', status_code=202, responses={202: {"content": None}})
def start(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/shutdown', name='clusters:shutdown', status_code=202, responses={202: {"content": None}})
def shutdown(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_grace_shutdown,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/activate', name='clusters:activate', status_code=202, responses={202: {"content": None}})
def activate(cluster: Cluster) -> Response:
    Thread(
        target=cluster_ops.cluster_activate,
        args=(cluster.get_id(),),
    ).start()
    return Response(status_code=202)  # FIXME: Provide URL for checking task status


@instance_api.post('/update', name='clusters:upgrade', status_code=204, responses={204: {"content": None}})
def update( cluster: Cluster, parameters: _UpdateParams) -> Response:
    cluster_ops.update_cluster(
        cluster_id=cluster.get_id(),
        mgmt_image=parameters.management_image,
        mgmt_only=parameters.spdk_image is None and not parameters.restart,
        spdk_image=parameters.spdk_image,
        restart=parameters.restart
    )
    return Response(status_code=204)
