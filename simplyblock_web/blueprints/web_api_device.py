#!/usr/bin/env python
# encoding: utf-8

import logging

from flask_openapi3 import APIBlueprint

from simplyblock_core.controllers import device_controller
from simplyblock_web import utils

from simplyblock_core import db_controller

logger = logging.getLogger(__name__)

api = APIBlueprint("device", __name__)
db = db_controller.DBController()


@api.get('/device/list/<string:uuid>')
def list_devices_by_node(path: utils.UUIDPath):
    snode = db.get_storage_node_by_id(path.uuid)
    if not snode:
        return utils.get_response_error(f"snode not found: {path.uuid}", 404)

    return utils.get_response([
        dev.get_clean_dict()
        for dev in snode.nvme_devices
    ])


@api.get('/device/<string:uuid>')
def get_storage_device(path: utils.UUIDPath):
    dev = db.get_storage_device_by_id(path.uuid)

    if not dev:
        return utils.get_response_error(f"device not found: {path.uuid}", 404)

    return utils.get_response([dev.get_clean_dict()])


@api.get('/device', defaults={'uuid': None})
def list_storage_devices(raw):
    cluster_id = utils.get_cluster_id(raw)

    return utils.get_response([
        dev.get_clean_dict()
        for node in db.get_storage_nodes_by_cluster_id(cluster_id)
        for dev in node.nvme_devices
    ])


@api.get('/device/capacity/<string:uuid>', defaults={'history': None})
@api.get('/device/capacity/<string:uuid>/history/<string:history>')
def device_capacity(path: utils.HistoryPath):
    device = db.get_storage_device_by_id(path.uuid)
    if not device:
        return utils.get_response_error(f"devices not found: {path.uuid}", 404)

    return utils.get_response(device_controller.get_device_capacity(
        path.uuid,
        path.history,
        parse_sizes=False,
    ))


@api.get('/device/iostats/<string:uuid>', defaults={'history': None})
@api.get('/device/iostats/<string:uuid>/history/<string:history>')
def device_iostats(path: utils.HistoryPath):
    device = db.get_storage_device_by_id(path.uuid)
    if not device:
        return utils.get_response_error(f"devices not found: {path.uuid}", 404)

    return utils.get_response({
        "object_data": device.get_clean_dict(),
        "stats": device_controller.get_device_iostats(path.uuid, path.history, parse_sizes=False) or []
    })


@api.get('/device/reset/<string:uuid>')
def device_reset(path: utils.UUIDPath):
    devices = db.get_storage_device_by_id(path.uuid)
    if not devices:
        return utils.get_response_error(f"devices not found: {path.uuid}", 404)

    return utils.get_response(device_controller.reset_storage_device(path.uuid))


@api.get('/device/remove/<string:uuid>')
def device_remove(path: utils.UUIDPath):
    devices = db.get_storage_device_by_id(path.uuid)
    if not devices:
        return utils.get_response_error(f"devices not found: {path.uuid}", 404)

    return utils.get_response(device_controller.device_remove(path.uuid))


@api.post('/device/<string:uuid>')
def device_add(path: utils.UUIDPath):
    devices = db.get_storage_device_by_id(path.uuid)
    if not devices:
        return utils.get_response_error(f"devices not found: {path.uuid}", 404)

    return utils.get_response(device_controller.add_device(path.uuid))
