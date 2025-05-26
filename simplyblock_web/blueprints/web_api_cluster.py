#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import threading
from typing import Literal, Optional

from pydantic import BaseModel, Field
from flask_openapi3 import APIBlueprint

from simplyblock_core.controllers import tasks_controller, device_controller
from simplyblock_web import utils

from simplyblock_core import db_controller, cluster_ops, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core import utils as core_utils

logger = logging.getLogger(__name__)

api = APIBlueprint("cluster", __name__)
db = db_controller.DBController()


class _ClusterParams(BaseModel):
    blk_size: Literal[512, 4096] = Field(512)
    page_size_in_blocks: int = Field(2097152)
    cap_warn: int = Field(0, ge=0, le=100)
    cap_crit: int = Field(0, ge=0, le=100)
    prov_cap_warn: int = Field(0, ge=0, le=100)
    prov_cap_crit: int = Field(0, ge=0, le=100)
    distr_ndcs: int = Field(1)
    distr_npcs: int = Field(1)
    distr_bs: int = Field(4096)
    distr_chunk_bs: int = Field(4096)
    ha_type: Literal['single', 'ha'] = Field('single')
    enable_node_affinity: bool = Field(False)
    qpair_count: int = Field(256)
    max_queue_size: int = Field(128)
    inflight_io_threshold: int = Field(4)
    enable_qos: bool = Field(False)
    strict_node_anti_affinity: bool = Field(False)


@api.post('/cluster')
def add_cluster(query: _ClusterParams):
    return utils.get_response(cluster_ops.add_cluster(**query.dict()))


@api.get('/cluster')
def list_clusters():
    return utils.get_response([
        cluster.get_clean_dict().update(status_code=cluster.get_status_code())
        for cluster
        in db.get_clusters()
    ])


@api.get('/cluster/<string:uuid>')
def get_cluster(path: utils.UUIDPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if cluster is None:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)

    return utils.get_response(
        cluster.get_clean_dict().update(status_code=cluster.get_status_code())
    )


@api.get('/cluster/capacity/<string:uuid>', defaults={'history': None})
@api.get('/cluster/capacity/<string:uuid>/history/<string:history>')
def cluster_capacity(path: utils.HistoryPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        logger.error(f"Cluster not found {path.uuid}")
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)

    ret = cluster_ops.get_capacity(path.uuid, path.history, is_json=True)
    return utils.get_response(json.loads(ret))


@api.get('/cluster/iostats/<string:uuid>', defaults={'history': None})
@api.get('/cluster/iostats/<string:uuid>/history/<string:history>')
def cluster_iostats(path: utils.HistoryPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        logger.error(f"Cluster not found {path.uuid}")
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)

    data = cluster_ops.get_iostats_history(path.uuid, path.history, parse_sizes=False, with_sizes=True)
    ret = {
        "object_data": cluster.get_clean_dict(),
        "stats": data or []
    }
    return utils.get_response(ret)


@api.get('/cluster/status/<string:uuid>')
def cluster_status(path: utils.UUIDPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        logger.error(f"Cluster not found {path.uuid}")
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)
    data = cluster_ops.get_cluster_status(path.uuid, is_json=True)
    return utils.get_response(json.loads(data))


class _LimitQuery(BaseModel):
    limit: int = Field(50, gt=0)


@api.get('/cluster/get-logs/<string:uuid>')
def cluster_get_logs(path: utils.UUIDPath, query: _LimitQuery):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster already inactive")

    data = cluster_ops.get_logs(path.uuid, is_json=True, limit=query.limit)
    return utils.get_response(json.loads(data))


@api.get('/cluster/get-tasks/<string:uuid>')
def cluster_get_tasks(path: utils.UUIDPath, query: _LimitQuery):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster is inactive")

    tasks = tasks_controller.list_tasks(path.uuid, is_json=True, limit=query.limit)
    return utils.get_response(json.loads(tasks))


@api.put('/cluster/gracefulshutdown/<string:uuid>')
def cluster_grace_shutdown(path: utils.UUIDPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_grace_shutdown,
        args=(path.uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)


@api.put('/cluster/gracefulstartup/<string:uuid>')
def cluster_grace_startup(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)


@api.put('/cluster/activate/<string:uuid>')
def cluster_activate(path: utils.UUIDPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_activate,
        args=(path.uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)


@api.get('/cluster/allstats/<string:uuid>', defaults={'history': None})
@api.get('/cluster/allstats/<string:uuid>/history/<string:history>')
def cluster_allstats(path: utils.HistoryPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        logger.error(f"Cluster not found {path.uuid}")
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)

    storage_nodes = db.get_storage_nodes_by_cluster_id(path.uuid)
    return utils.get_response({
        'cluster': {
            "object_data": cluster.get_clean_dict(),
            "stats": cluster_ops.get_iostats_history(path.uuid, path.history, parse_sizes=False, with_sizes=True),
        },
        'storage_nodes': [
            {
                "object_data": node.get_clean_dict(),
                "stats": storage_node_ops.get_node_iostats_history(node.get_id(), path.history, parse_sizes=False, with_sizes=True),
            }
            for node in storage_nodes
        ],
        'devices': [
            {
                "object_data": dev.get_clean_dict(),
                "stats": device_controller.get_device_iostats(path.uuid, path.history, parse_sizes=False),
            }
            for node in db.get_storage_nodes_by_cluster_id(path.uuid)
            for dev in node.nvme_devices
        ],
        'pools': [
            {
                "object_data": pool.get_clean_dict(),
                "stats": [
                    record.get_clean_dict()
                    for record in db.get_pool_stats(pool, 1)
                ]
            }
            for pool in db.get_pools(path.uuid)
        ],
        'lvols': [
            {
                "object_data": lvol.get_clean_dict(),
                "stats": [
                    record.get_clean_dict()
                    for record
                    in db.get_lvol_stats(lvol, limit=1)
                ]
            }
            for lvol in db.get_lvols()
        ],
    })


@api.delete('/cluster/<string:uuid>')
def cluster_delete(path: utils.UUIDPath):
    cluster = db.get_cluster_by_id(path.uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {path.uuid}", 404)

    ret = cluster_ops.delete_cluster(path.uuid)
    return utils.get_response(ret)


@api.get('/cluster/show/<string:uuid>')
def show_cluster(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster is inactive")

    data = cluster_ops.list_all_info(uuid)
    return utils.get_response(json.loads(data))
