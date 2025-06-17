from threading import Thread
from typing import Literal, Optional

from flask import abort, jsonify
from flask_openapi3 import APIBlueprint
from pydantic import BaseModel, Field

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core import cluster_ops, utils as core_utils

from . import util as util


api = APIBlueprint('cluster', __name__, url_prefix='/clusters')
db = DBController()


class _LimitQuery(BaseModel):
    limit: int = Field(50)


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


@api.get('/')
def list():
    return [
        cluster.get_clean_dict()
        for cluster
        in db.get_clusters()
    ]


@api.put('/')
def add(body: ClusterParams):
    cluster_id_or_false = cluster_ops.add_cluster(**body.model_dump())
    if not cluster_id_or_false:
        raise ValueError('Failed to create cluster')

    return jsonify(cluster_id_or_false)



instance_api = APIBlueprint('instance', __name__, url_prefix='/<cluster_id>')


class ClusterPath(BaseModel):
    cluster_id: str = Field(pattern=core_utils.UUID_PATTERN)

    def cluster(self) -> Cluster:
        cluster = db.get_cluster_by_id(self.cluster_id)
        if cluster is None:
            abort(404)

        return cluster


@instance_api.get('/')
def get(path: ClusterPath):
    return path.cluster().get_clean_dict()


@instance_api.delete('/')
def delete(path: ClusterPath):
    none_or_false = cluster_ops.delete_cluster(path.cluster().get_id())
    success = none_or_false != False  # noqa
    if not success:
        raise ValueError('Failed to delete cluster')


@instance_api.get('/capacity')
def capacity(path: ClusterPath, query: util.HistoryQuery):
    capacity_or_false = cluster_ops.get_capacity(
            path.cluster().get_id(), query.history)
    if not capacity_or_false:
        raise ValueError('Failed to compute capacity')

    return capacity_or_false


@instance_api.get('/iostats')
def iostats(path: ClusterPath, query: util.HistoryQuery):
    iostats_or_false = cluster_ops.get_iostats_history(
            path.cluster().get_id(), query.history, with_sizes=True)
    if not iostats_or_false:
        raise ValueError('Failed to compute capacity')

    return iostats_or_false


@instance_api.get('/logs')
def logs(path: ClusterPath, query: _LimitQuery):
    logs_or_false = cluster_ops.get_logs(
            path.cluster().get_id(), is_json=True, limit=query.limit)
    if not logs_or_false:
        raise ValueError('Failed to access logs')

    return logs_or_false


@instance_api.post('/start')
def start(path: ClusterPath):
    Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(path.cluster().get_id(),),
    ).start()
    return '', 201  # FIXME: Provide URL for checking task status


@instance_api.post('/shutdown')
def shutdown(path: ClusterPath):
    Thread(
        target=cluster_ops.cluster_grace_shutdown,
        args=(path.cluster().get_id(),),
    ).start()
    return '', 201  # FIXME: Provide URL for checking task status


@instance_api.post('/activate')
def activate(path: ClusterPath):
    Thread(
        target=cluster_ops.cluster_activate,
        args=(path.cluster().get_id(),),
    ).start()
    return '', 201  # FIXME: Provide URL for checking task status


@instance_api.post('/update')
def update(path: ClusterPath, body: _UpdateParams):
    cluster_ops.update_cluster(
        cluster_id=path.cluster().get_id(),
        mgmt_image=body.management_image,
        mgmt_only=body.spdk_image is None and not body.restart,
        spdk_image=body.spdk_image,
        restart=body.restart
    )


api.register_api(instance_api)
