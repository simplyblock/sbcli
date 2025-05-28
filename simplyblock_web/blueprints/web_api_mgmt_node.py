#!/usr/bin/env python
# encoding: utf-8

import logging

from flask_openapi3 import APIBlueprint

from simplyblock_web import utils

from simplyblock_core import db_controller

logger = logging.getLogger(__name__)

api = APIBlueprint("mgmt", __name__)
db = db_controller.DBController()


@api.get('/mgmtnode/<string:uuid>')
def get_mgmt_node(path: utils.UUIDPath):
    node = db.get_mgmt_node_by_id(path.uuid)
    if not node:
        node = db.get_mgmt_node_by_hostname(path.uuid)

    if not node:
        return utils.get_response_error(f"node not found: {path.uuid}", 404)

    return utils.get_response([node.get_clean_dict().update(status_code=node.get_status_code())])


@api.get('/mgmtnode')
def list_mgmt_nodes(uuid):
    return utils.get_response([
        node.get_clean_dict().update(status_code=node.get_status_code())
        for node in db.get_mgmt_nodes()
    ])
