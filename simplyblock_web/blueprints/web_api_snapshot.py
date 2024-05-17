#!/usr/bin/env python
# encoding: utf-8

import logging
from random import random

from flask import Blueprint
from flask import request

from simplyblock_web import utils
from simplyblock_core import kv_store
from simplyblock_core.controllers import lvol_controller, snapshot_controller


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
db_controller = kv_store.DBController()
bp = Blueprint("snapshot", __name__)


def get_param_value_or_default(data, key_name, default):
    if key_name in data:
        value = utils.parse_size(data[key_name])
        if isinstance(value, int):
            return value
    return default


def validate_header_data():
    cl_id = request.headers.get('cluster')
    secret = request.headers.get('secret')
    logger.debug(f"Headers:cluster={cl_id}")
    logger.debug(f"Headers:secret={secret}")
    if not cl_id:
        logger.error("no 'cluster' key found in the request headers")
        return False
    if not secret:
        logger.error("no 'secret' key found in the request headers")
        return False
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found: {cl_id}")
        return False
    if cluster.secret == secret:
        return True


@bp.route('/snapshot/create_snapshot', methods=['POST'])
def csi_create_snapshot():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_csi_response(None, "missing required param: lvol_id", 400)
    if 'snapshot_name' not in cl_data:
        return utils.get_csi_response(None, "missing required param: snapshot_name", 400)

    snapID = snapshot_controller.add(
        cl_data['lvol_id'],
        cl_data['snapshot_name'])
    return utils.get_csi_response(snapID)


@bp.route('/snapshot/delete_snapshot/<string:uuid>', methods=['DELETE'])
def csi_delete_snapshot(uuid):
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    ret = snapshot_controller.delete(uuid)
    return utils.get_csi_response(ret)

@bp.route('/snapshot/list_snapshots', methods=['GET'])
def csi_list_snapshots():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
        data.append({
            "uuid": snap.uuid,
            "name": pool.pool_name,
            "size": str(snap.lvol.size),
            "pool_name": snap.snap_bdev,
            "pool_id": snap.lvol.pool_uuid,
            "source_uuid": snap.lvol.get_id(),
            "created_at": str(snap.created_at),
        })
    return utils.get_csi_response(data)
