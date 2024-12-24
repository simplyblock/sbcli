#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import threading

from flask import Blueprint
from flask import request

from simplyblock_core.controllers import tasks_controller
from simplyblock_web import utils

from simplyblock_core import db_controller, cluster_ops
from simplyblock_core.models.cluster import Cluster

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("cluster", __name__)
db_controller = db_controller.DBController()


@bp.route('/cluster', methods=['POST'])
def add_cluster():

    blk_size = 512
    page_size_in_blocks = 2097152
    cap_warn = 0
    cap_crit = 0
    prov_cap_warn = 0
    prov_cap_crit = 0


    cl_data = request.get_json()
    if 'blk_size' in cl_data:
        if cl_data['blk_size'] not in [512, 4096]:
            return utils.get_response_error("blk_size can be 512 or 4096", 400)
        else:
            blk_size = cl_data['blk_size']

    if 'page_size_in_blocks' in cl_data:
        page_size_in_blocks = cl_data['page_size_in_blocks']
    distr_ndcs = cl_data.get('distr_ndcs', 1)
    distr_npcs = cl_data.get('distr_npcs', 1)
    distr_bs = cl_data.get('distr_bs', 4096)
    distr_chunk_bs = cl_data.get('distr_chunk_bs', 4096)
    ha_type = cl_data.get('ha_type', 'single')
    enable_node_affinity = cl_data.get('enable_node_affinity', False)
    qpair_count = cl_data.get('qpair_count', 256)

    max_queue_size = cl_data.get('max_queue_size', 128)
    inflight_io_threshold = cl_data.get('inflight_io_threshold', 4)
    enable_qos = cl_data.get('enable_qos', False)
    strict_node_anti_affinity = cl_data.get('strict_node_anti_affinity', False)

    ret = cluster_ops.add_cluster(blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
                                  distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity,
                                  qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity)

    return utils.get_response(ret)


@bp.route('/cluster', methods=['GET'], defaults={'uuid': None})
@bp.route('/cluster/<string:uuid>', methods=['GET'])
def list_clusters(uuid):
    cluster_id = utils.get_cluster_id(request)
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {cluster_id}", 404)

    d = cluster.get_clean_dict()
    d['status_code'] = cluster.get_status_code()
    return utils.get_response([d])


@bp.route('/cluster/capacity/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/capacity/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_capacity(uuid, history):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    ret = cluster_ops.get_capacity(uuid, history, parse_sizes=False)
    return utils.get_response(ret)


@bp.route('/cluster/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_iostats(uuid, history):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    data = cluster_ops.get_iostats_history(uuid, history, parse_sizes=False)
    ret = {
        "object_data": cluster.get_clean_dict(),
        "stats": data or []
    }
    return utils.get_response(ret)


@bp.route('/cluster/status/<string:uuid>', methods=['GET'])
def cluster_status(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    data = cluster_ops.show_cluster(uuid, is_json=True)
    return utils.get_response(json.loads(data))


@bp.route('/cluster/get-logs/<string:uuid>', methods=['GET'])
def cluster_get_logs(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster already inactive")

    data = cluster_ops.get_logs(uuid, is_json=True)
    return utils.get_response(json.loads(data))


@bp.route('/cluster/get-tasks/<string:uuid>', methods=['GET'])
def cluster_get_tasks(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster is inactive")

    tasks = tasks_controller.list_tasks(uuid, is_json=True)
    return utils.get_response(json.loads(tasks))


@bp.route('/cluster/gracefulshutdown/<string:uuid>', methods=['PUT'])
def cluster_grace_shutdown(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_grace_shutdown,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)


@bp.route('/cluster/gracefulstartup/<string:uuid>', methods=['PUT'])
def cluster_grace_startup(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)


@bp.route('/cluster/activate/<string:uuid>', methods=['PUT'])
def cluster_activate(uuid):
    cluster = db_controller.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_activate,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True)
