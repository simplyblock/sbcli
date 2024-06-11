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


@bp.route('/deployer', methods=['POST'])
def add_deployer():
    dpl_data = request.get_json()
    if 'snodes' not in dpl_data:
        return utils.get_response_error("missing required param: snodes", 400)
    if 'azs' not in dpl_data:
        return utils.get_response_error("missing required param: azs", 400)
    if 'cluster_id' not in dpl_data:
        return utils.get_response_error("missing required param: cluster_id", 400)

    d = Deployer()
    d.jobid = str(uuid.uuid4())
    d.snodes = dpl_data['snodes']
    d.azs = dpl_data['azs']
    d.cluster_id = dpl_data['cluster_id']
    d.write_to_db(db_controller.kv_store)

    return utils.get_response(d.to_dict()), 201


@bp.route('/deployer', methods=['GET'], defaults={'jobid': None})
@bp.route('/deployer/<string:jobid>', methods=['GET'])
def list_deployer(jobid):
    deployers_list = []
    if jobid:
        dpl = db_controller.get_deployer_by_id(jobid)
        if dpl:
            deployers_list.append(dpl)
        else:
            return utils.get_response_error(f"Deployer not found: {jobid}", 404)
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
