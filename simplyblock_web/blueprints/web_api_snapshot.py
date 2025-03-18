#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint
from flask import request

from simplyblock_web import utils
from simplyblock_core import db_controller
from simplyblock_core.controllers import snapshot_controller


logger = logging.getLogger(__name__)

db_controller = db_controller.DBController()
bp = Blueprint("snapshot", __name__)


@bp.route('/snapshot', methods=['POST'])
def create_snapshot():
    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_response(None, "missing required param: lvol_id", 400)
    if 'snapshot_name' not in cl_data:
        return utils.get_response(None, "missing required param: snapshot_name", 400)

    snapID, err = snapshot_controller.add(
        cl_data['lvol_id'],
        cl_data['snapshot_name'])
    return utils.get_response(snapID, err, http_code=400)


@bp.route('/snapshot/<string:uuid>', methods=['DELETE'])
def delete_snapshot(uuid):
    ret = snapshot_controller.delete(uuid)
    return utils.get_response(ret)


@bp.route('/snapshot', methods=['GET'])
def list_snapshots():
    cluster_id = utils.get_cluster_id(request)
    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        if snap.cluster_id != cluster_id:
            continue
        data.append(snap.get_clean_dict())
    return utils.get_response(data)


@bp.route('/snapshot/clone', methods=['POST'])
def clone_snapshot():
    cl_data = request.get_json()
    if 'snapshot_id' not in cl_data:
        return utils.get_response(None, "missing required param: snapshot_id", 400)
    if 'clone_name' not in cl_data:
        return utils.get_response(None, "missing required param: clone_name", 400)

    new_size = 0
    if 'new_size' in cl_data:
        new_size = utils.parse_size(cl_data['new_size'])

    res, msg = snapshot_controller.clone(
        cl_data['snapshot_id'], cl_data['clone_name'], new_size)
    if res:
        return utils.get_response(msg)
    return utils.get_response(None, msg)
