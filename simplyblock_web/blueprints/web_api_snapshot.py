#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint
from flask import request

from simplyblock_web import utils
from simplyblock_core import kv_store
from simplyblock_core.controllers import snapshot_controller


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
db_controller = kv_store.DBController()
bp = Blueprint("snapshot", __name__)


@bp.route('/snapshot/create_snapshot', methods=['POST'])
def create_snapshot():
    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_response(None, "missing required param: lvol_id", 400)
    if 'snapshot_name' not in cl_data:
        return utils.get_response(None, "missing required param: snapshot_name", 400)

    snapID = snapshot_controller.add(
        cl_data['lvol_id'],
        cl_data['snapshot_name'])
    return utils.get_response(snapID)


@bp.route('/snapshot/delete_snapshot/<string:uuid>', methods=['DELETE'])
def delete_snapshot(uuid):
    ret = snapshot_controller.delete(uuid)
    return utils.get_response(ret)

@bp.route('/snapshot/list_snapshots', methods=['GET'])
def list_snapshots():
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
    return utils.get_response(data)
