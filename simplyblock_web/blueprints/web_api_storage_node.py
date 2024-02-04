#!/usr/bin/env python
# encoding: utf-8

import logging
import time

from flask import Blueprint, request

from simplyblock_web import utils

from simplyblock_core import kv_store
from simplyblock_core import storage_node_ops

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("snode", __name__)
db_controller = kv_store.DBController()


@bp.route('/storagenode', methods=['GET'], defaults={'uuid': None})
@bp.route('/storagenode/<string:uuid>', methods=['GET'])
def list_storage_nodes(uuid):
    if uuid:
        node = db_controller.get_storage_node_by_id(uuid)
        if not node:
            node = db_controller.get_storage_node_by_hostname(uuid)

        if node:
            nodes = [node]
        else:
            return utils.get_response_error(f"node not found: {uuid}", 404)
    else:
        nodes = db_controller.get_storage_nodes()
    data = []
    for node in nodes:
        d = node.get_clean_dict()
        d['status_code'] = node.get_status_code()
        data.append(d)
    return utils.get_response(data)


@bp.route('/storagenode/capacity/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/storagenode/capacity/<string:uuid>', methods=['GET'], defaults={'history': None})
def storage_node_capacity(uuid, history):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    data = storage_node_ops.get_node_capacity(uuid, history, parse_sizes=False)
    if data:
        return utils.get_response(data)
    else:
        return utils.get_response(False)



@bp.route('/storagenode/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/storagenode/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def storagenode_iostats(uuid, history):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    data = storage_node_ops.get_node_iostats_history(uuid, history, parse_sizes=False)

    if data:
        return utils.get_response(data)
    else:
        return utils.get_response(False)


@bp.route('/storagenode/port/<string:uuid>', methods=['GET'])
def storage_node_ports(uuid):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = []
    for nic in node.data_nics:
        out.append({
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.get_transport_type(),
            "Status": nic.status,
        })
    return utils.get_response(out)


@bp.route('/storagenode/port-io-stats/<string:uuid>', methods=['GET'])
def storage_node_port_io_stats(uuid):
    nodes = db_controller.get_storage_nodes()
    nd = None
    port = None
    for node in nodes:
        for nic in node.data_nics:
            if nic.get_id() == uuid:
                port = nic
                nd = node
                break

    if not port:
        return utils.get_response_error(f"Port not found: {uuid}", 404)

    data = db_controller.get_port_stats(nd.get_id(), port.get_id())
    out = []
    for record in data:
        out.append(record.get_clean_dict())
    return utils.get_response(out)


@bp.route('/storagenode/suspend/<string:uuid>', methods=['GET'])
def storage_node_suspend(uuid):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.suspend_storage_node(uuid, True)
    return utils.get_response(out)


@bp.route('/storagenode/resume/<string:uuid>', methods=['GET'])
def storage_node_resume(uuid):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.resume_storage_node(uuid)
    return utils.get_response(out)


@bp.route('/storagenode/shutdown/<string:uuid>', methods=['GET'])
def storage_node_shutdown(uuid):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.shutdown_storage_node(uuid)
    return utils.get_response(out)


@bp.route('/storagenode/restart/<string:uuid>', methods=['GET'])
def storage_node_restart(uuid):
    node = db_controller.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.restart_storage_node(uuid)
    return utils.get_response(out)


@bp.route('/storagenode/add', methods=['POST'])
def storage_node_add():
    req_data = request.get_json()

    if 'cluster_id' not in req_data:
        return utils.get_response_error("missing required param: cluster_id", 400)

    if 'node_ip' not in req_data:
        return utils.get_response_error("missing required param: node_ip", 400)

    if 'ifname' not in req_data:
        return utils.get_response_error("missing required param: ifname", 400)

    cluster_id = req_data['cluster_id']
    node_ip = req_data['node_ip']
    ifname = req_data['ifname']

    spdk_image = None
    if 'spdk_image' in req_data:
        spdk_image = req_data['spdk_image']

    cmd_params = None
    if 'cmd_params' in req_data:
        cmd_params = req_data['cmd_params']

    data_nics = None
    if 'data_nics' in req_data:
        data_nics = req_data['data_nics']
        data_nics = data_nics.split(",")

    spdk_cpu_mask = None
    if 'spdk_cpu_mask' in req_data:
        msk = req_data['spdk_cpu_mask']
        if utils.validate_cpu_mask(msk):
            spdk_cpu_mask = msk
        else:
            return utils.get_response_error(f"Invalid cpu mask value: {msk}", 400)

    spdk_mem = None
    if 'spdk_mem' in req_data:
        mem = req_data['spdk_mem']
        spdk_mem = utils.parse_size(mem)
        if spdk_mem < 1 * 1024 * 1024:
            return utils.get_response_error(f"SPDK memory:{mem} must be larger than 1G", 400)

    out = storage_node_ops.add_node(
        cluster_id, node_ip, ifname, data_nics, spdk_cpu_mask, spdk_mem,
        spdk_image=spdk_image, cmd_params=cmd_params)

    return utils.get_response(out)
