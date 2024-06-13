#!/usr/bin/env python
# encoding: utf-8
import json
import logging
import threading
import time
import uuid

from flask import Blueprint
from flask import request

from simplyblock_web import utils

from simplyblock_core import kv_store
from simplyblock_core.models.deployer import Deployer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("deployer", __name__)
db_controller = kv_store.DBController()


@bp.route('/deployer/<string:uuid>', methods=['PUT'])
def update_deployer(uuid):
    dpl = db_controller.get_deployer_by_id(uuid)
    if not dpl:
        return utils.get_response_error(f"Deployer not found: {uuid}", 404)

    dpl_data = request.get_json()
    if 'snodes' not in dpl_data:
        return utils.get_response_error("missing required param: snodes", 400)
    if 'az' not in dpl_data:
        return utils.get_response_error("missing required param: az", 400)
 
    dpl.snodes = dpl_data['snodes']
    dpl.az = dpl_data['az']
    dpl.write_to_db(db_controller.kv_store)

    #TODO: call prepare terraform parameter function
    return utils.get_response(dpl.to_dict()), 201


@bp.route('/deployer', methods=['GET'], defaults={'uuid': None})
@bp.route('/deployer/<string:uuid>', methods=['GET'])
def list_deployer(uuid):
    deployers_list = []
    if uuid:
        dpl = db_controller.get_deployer_by_id(uuid)
        if dpl:
            deployers_list.append(dpl)
        else:
            return utils.get_response_error(f"Deployer not found: {uuid}", 404)
    else:
        dpls = db_controller.get_deployers()
        if dpls:
            deployers_list.extend(dpls)

    data = []
    for deployer in deployers_list:
        d = deployer.get_clean_dict()
        d['status_code'] = deployer.get_status_code()
        data.append(d)
    return utils.get_response(data)

@bp.route('/deployer', methods=['POST'])
def add_deployer():
    dpl_data = request.get_json()
    if 'snodes' not in dpl_data:
        return utils.get_response_error("missing required param: snodes", 400)
    if 'snodes_type' not in dpl_data:
        return utils.get_response_error("missing required param: snodes_type", 400)
    if 'mnodes' not in dpl_data:
        return utils.get_response_error("missing required param: mnodes", 400)
    if 'mnodes_type' not in dpl_data:
        return utils.get_response_error("missing required param: mnodes_type", 400)
    if 'az' not in dpl_data:
        return utils.get_response_error("missing required param: az", 400)
    if 'cluster_id' not in dpl_data:
        return utils.get_response_error("missing required param: cluster_id", 400)
    if 'region' not in dpl_data:
        return utils.get_response_error("missing required param: region", 400)
    if 'workspace' not in dpl_data:
        return utils.get_response_error("missing required param: workspace", 400)
    if 'bucket_name' not in dpl_data:
        return utils.get_response_error("missing required param: bucket_name", 400)

    d = Deployer()
    d.uuid        = dpl_data['cluster_id']
    d.snodes      = dpl_data['snodes']
    d.snodes_type = dpl_data['snodes_type']
    d.mnodes      = dpl_data['mnodes']
    d.mnodes_type = dpl_data['mnodes_type']
    d.az          = dpl_data['az']
    d.region      = dpl_data['region']
    d.workspace   = dpl_data['workspace']
    d.bucket_name = dpl_data['bucket_name']
    d.write_to_db(db_controller.kv_store)

    return utils.get_response(d.to_dict()), 201
