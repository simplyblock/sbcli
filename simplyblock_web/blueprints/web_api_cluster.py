#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import time
import uuid

from flask import Blueprint
from flask import request

from simplyblock_web import utils

from simplyblock_core import kv_store, cluster_ops
from simplyblock_core.models.cluster import Cluster

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("cluster", __name__)
db_controller = kv_store.DBController()


@bp.route('/cluster', methods=['POST'])
def add_cluster():
    cl_data = request.get_json()
    if 'blk_size' not in cl_data:
        return utils.get_response_error("missing required param: blk_size", 400)
    if 'page_size_in_blocks' not in cl_data:
        return utils.get_response_error("missing required param: page_size_in_blocks", 400)
    if 'model_ids' not in cl_data:
        return utils.get_response_error("missing required param: model_ids", 400)
    if 'ha_type' not in cl_data:
        return utils.get_response_error("missing required param: ha_type", 400)
    if 'tls' not in cl_data:
        return utils.get_response_error("missing required param: tls", 400)
    if 'auth-hosts-only' not in cl_data:
        return utils.get_response_error("missing required param: auth-hosts-only", 400)
    if 'dhchap' not in cl_data:
        return utils.get_response_error("missing required param: dhchap", 400)
    if 'NQN' not in cl_data:
        return utils.get_response_error("missing required param: NQN", 400)

    c = Cluster()
    c.uuid = str(uuid.uuid4())

    if cl_data['blk_size'] not in [512, 4096]:
        return utils.get_response_error("blk_size can be 512 or 4096", 400)

    if cl_data['ha_type'] not in ["single", "ha"]:
        return utils.get_response_error("ha_type can be single or ha", 400)

    if cl_data['dhchap'] not in ["off", "one-way", "bi-direct"]:
        return utils.get_response_error("dhchap can be off, one-way or bi-direct", 400)

    c.blk_size = cl_data['blk_size']
    c.page_size_in_blocks = cl_data['page_size_in_blocks']
    c.model_ids = cl_data['model_ids']
    c.ha_type = cl_data['ha_type']
    c.tls = cl_data['tls']
    c.auth_hosts_only = cl_data['auth-hosts-only']
    c.nqn = cl_data['nqn']
    c.iscsi = cl_data['iscsi'] or False
    c.dhchap = cl_data['dhchap']
    c.cluster_status = Cluster.STATUS_ACTIVE
    c.updated_at = int(time.time())
    c.write_to_db(db_controller.kv_store)

    return utils.get_response(c.to_dict()), 201


@bp.route('/cluster', methods=['GET'], defaults={'uuid': None})
@bp.route('/cluster/<string:uuid>', methods=['GET'])
def list_clusters(uuid):
    clusters_list = []
    if uuid:
        cl = db_controller.get_cluster_by_id(uuid)
        if cl:
            clusters_list.append(cl)
        else:
            return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    else:
        cls = db_controller.get_clusters()
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
    cluster = db_controller.get_cluster_by_id(id=uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    ret = cluster_ops.get_capacity(uuid, history, parse_sizes=False)
    return utils.get_response(ret)


@bp.route('/cluster/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/cluster/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def cluster_iostats(uuid, history):
    cluster = db_controller.get_cluster_by_id(id=uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)

    out = cluster_ops.get_iostats_history(uuid, history, parse_sizes=False)
    return utils.get_response(out)


@bp.route('/cluster/status/<string:uuid>', methods=['GET'])
def cluster_status(uuid):
    cluster = db_controller.get_cluster_by_id(id=uuid)
    if not cluster:
        logger.error(f"Cluster not found {uuid}")
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    data = cluster_ops.show_cluster(uuid, is_json=True)
    return utils.get_response(json.loads(data))


@bp.route('/cluster/enable/<string:uuid>', methods=['PUT'])
def cluster_enable(uuid):
    return utils.get_response("Not Implemented!")


@bp.route('/cluster/disable/<string:uuid>', methods=['PUT'])
def cluster_disable(uuid):
    return utils.get_response("Not Implemented!")


@bp.route('/cluster/get-logs/<string:uuid>', methods=['GET'])
def cluster_get_logs(uuid):
    cluster = db_controller.get_cluster_by_id(id=uuid)
    if not cluster:
        return utils.get_response_error(f"Cluster not found: {uuid}", 404)
    if cluster.status == Cluster.STATUS_INACTIVE:
        return utils.get_response("Cluster already inactive")

    data = cluster_ops.get_logs(uuid, is_json=True)
    return utils.get_response(json.loads(data))

