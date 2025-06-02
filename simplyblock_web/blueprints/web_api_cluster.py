#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import threading

from flask import abort, Blueprint, request

from simplyblock_core.controllers import tasks_controller, device_controller, lvol_controller
from simplyblock_web import utils

from simplyblock_core import db_controller, cluster_ops, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core import utils as core_utils

logger = logging.getLogger(__name__)

bp = Blueprint("cluster", __name__)
db = db_controller.DBController()


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

    return utils.get_response(cluster_ops.add_cluster(
        blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
        distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity,
        qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity
    ))


@bp.route('/cluster', methods=['GET'], defaults={'uuid': None})
@bp.route('/cluster/<string:uuid>', methods=['GET'])
def list_clusters(uuid):
    clusters_list = []
    if uuid:
        cl = db.get_cluster_by_id(uuid)
        if cl:
            clusters_list.append(cl)
        else:
            return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    else:
        cls = db.get_clusters()
        if cls:
            clusters_list.extend(cls)

    data = []
    for cluster in clusters_list:
        d = cluster.get_clean_dict()
        d['status_code'] = cluster.get_status_code()
        data.append(d)
    return utils.get_response(data)


@bp.route('/cluster/capacity/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/capacity/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_capacity(uuid, history):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    return utils.get_response(cluster_ops.get_capacity(uuid, history))


@bp.route('/cluster/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_iostats(uuid, history):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    limit = int(request.args.get('limit', 20))
    if limit > 1000:
        abort(400, 'Limit must be <=1000')

    return utils.get_response({
        "object_data": cluster.get_clean_dict(),
        "stats": cluster_ops.get_iostats_history(
            uuid, history, 
            records_count=limit,
            with_sizes=True
        ),
    })


@bp.route('/cluster/status/<string:uuid>', methods=['GET'])
def cluster_status(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    return utils.get_response(cluster_ops.get_cluster_status(uuid))


@bp.route('/cluster/get-logs/<string:uuid>', methods=['GET'])
def cluster_get_logs(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster already inactive")

    limit = 50
    try:
        args = request.args
        limit = int(args.get('limit', limit))
    except:
        pass

    return utils.get_response(cluster_ops.get_logs(uuid, limit=limit))


@bp.route('/cluster/get-tasks/<string:uuid>', methods=['GET'])
def cluster_get_tasks(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster is inactive")

    limit = 50
    try:
        args = request.args
        limit = int(args.get('limit', limit))
    except:
        pass

    tasks = tasks_controller.list_tasks(uuid, is_json=True, limit=limit)
    return utils.get_response(json.loads(tasks))


@bp.route('/cluster/gracefulshutdown/<string:uuid>', methods=['PUT'])
def cluster_grace_shutdown(uuid):
    cluster = db.get_cluster_by_id(uuid)
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
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_grace_startup,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True), 202


@bp.route('/cluster/activate/<string:uuid>', methods=['PUT'])
def cluster_activate(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    t = threading.Thread(
        target=cluster_ops.cluster_activate,
        args=(uuid,))
    t.start()
    # FIXME: Any failure within the thread are not handled
    return utils.get_response(True), 202


@bp.route('/cluster/allstats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/allstats/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_allstats(uuid, history):
    out = {}
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    out["cluster"] = {
        "object_data": cluster.get_clean_dict(),
        "stats": cluster_ops.get_iostats_history(uuid, history, with_sizes=True)
    }

    list_nodes = []
    list_devices = []
    for node in db.get_storage_nodes_by_cluster_id(uuid):
        data = storage_node_ops.get_node_iostats_history(node.get_id(), history, parse_sizes=False, with_sizes=True)
        list_nodes.append( {
            "object_data": node.get_clean_dict(),
            "stats": data or [] })
        for dev in node.nvme_devices:
            data = device_controller.get_device_iostats(uuid, history, parse_sizes=False)
            ret = {
                "object_data": dev.get_clean_dict(),
                "stats": data or []
            }
            list_devices.append(ret)

    out["storage_nodes"] = list_nodes

    out["devices"] = list_devices

    list_pools = []
    for pool in db.get_pools(uuid):
        records = db.get_pool_stats(pool, 1)
        d = []
        for r in records:
            d.append(r.get_clean_dict())

        ret = {
            "object_data": pool.get_clean_dict(),
            "stats": d or []
        }
        list_pools.append(ret)

    out["pools"] = list_pools

    list_lvols = []
    for lvol in db.get_lvols():
        records_list = db.get_lvol_stats(lvol, limit=1)
        data = []
        for r in records_list:
            data.append(r.get_clean_dict())

        ret = {
            "object_data": lvol.get_clean_dict(),
            "stats": data
        }
        list_lvols.append(ret)

    out["lvols"] = list_lvols

    return utils.get_response(out)


@bp.route('/cluster/activate/<string:uuid>', methods=['DELETE'])
def cluster_delete(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    ret = cluster_ops.delete_cluster(uuid)
    return utils.get_response(ret)


@bp.route('/cluster/show/<string:uuid>', methods=['GET'])
def show_cluster(uuid):
    cluster = db.get_cluster_by_id(uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster is inactive")

    return utils.get_response(cluster_ops.list_all_info(uuid))

