#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint

from simplyblock_web import utils

from simplyblock_core import db_controller

logger = logging.getLogger(__name__)

bp = Blueprint("mgmt", __name__)
db_controller = db_controller.DBController()


@bp.route('/mgmtnode', methods=['GET'], defaults={'uuid': None})
@bp.route('/mgmtnode/<string:uuid>', methods=['GET'])
def list_mgmt_nodes(uuid):
    if uuid:
        node = db_controller.get_mgmt_node_by_id(uuid)
        if not node:
            node = db_controller.get_mgmt_node_by_hostname(uuid)

        if node:
            nodes = [node]
        else:
            return utils.get_response_error(f"node not found: {uuid}", 404)
    else:
        nodes = db_controller.get_mgmt_nodes()
    data = []
    for node in nodes:
        d = node.get_clean_dict()
        d['status_code'] = node.get_status_code()
        data.append(d)
    return utils.get_response(data)
