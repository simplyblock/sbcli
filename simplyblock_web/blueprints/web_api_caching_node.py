#!/usr/bin/env python
# encoding: utf-8

import logging
import threading

from flask import Blueprint, request


from simplyblock_web import utils
from simplyblock_core import kv_store
from simplyblock_core.controllers import caching_node_controller

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("cnode", __name__)
db_controller = kv_store.DBController()


@bp.route('/cachingnode', methods=['POST'])
def add_node_to_cluster():
    cl_data = request.get_json()
    if 'cluster_id' not in cl_data:
        return utils.get_response_error("missing required param: cluster_id", 400)
    if 'node_ip' not in cl_data:
        return utils.get_response_error("missing required param: node_ip", 400)
    if 'iface_name' not in cl_data:
        return utils.get_response_error("missing required param: iface_name", 400)

    cluster_id = cl_data['cluster_id']
    node_ip = cl_data['node_ip']
    iface_name = cl_data['iface_name']

    data_nics_list = []
    spdk_cpu_mask = None
    spdk_mem = None
    spdk_image = None
    namespace = None
    multipathing = True
    no_cache = False
    iscsi = False

    if 'spdk_cpu_mask' in cl_data:
        spdk_cpu_mask = cl_data['spdk_cpu_mask']

    if 'spdk_mem' in cl_data:
        mem = cl_data['spdk_mem']
        spdk_mem = utils.parse_size(mem)
        if spdk_mem < 1 * 1024 * 1024:
            return utils.get_response_error(f"SPDK memory:{mem} must be larger than 1G", 400)

    if 'spdk_image' in cl_data:
        spdk_image = cl_data['spdk_image']

    if 'namespace' in cl_data:
        namespace = cl_data['namespace']

    if 'multipathing' in cl_data:
        multipathing = bool(cl_data['multipathing'])

    if 'iscsi' in cl_data:
        iscsi = bool(cl_data['iscsi'])

    if 'no_cache' in cl_data:
        no_cache = bool(cl_data['no_cache'])

    t = threading.Thread(
        target=caching_node_controller.add_node,
        args=(cluster_id, node_ip, iface_name, data_nics_list, spdk_cpu_mask, spdk_mem, spdk_image, namespace, multipathing, iscsi, no_cache))
    t.start()

    return utils.get_response(True)


@bp.route('/cachingnode', methods=['GET'], defaults={'uuid': None})
@bp.route('/cachingnode/<string:uuid>', methods=['GET'])
def list_caching_nodes(uuid):
    if uuid:
        node = db_controller.get_caching_node_by_id(uuid)
        if not node:
            node = db_controller.get_storage_node_by_hostname(uuid)

        if node:
            nodes = [node]
        else:
            return utils.get_response_error(f"node not found: {uuid}", 404)
    else:
        nodes = db_controller.get_caching_nodes()
    data = []
    for node in nodes:
        d = node.get_clean_dict()
        d['status_code'] = node.get_status_code()
        data.append(d)
    return utils.get_response(data)


@bp.route('/cachingnode/systemid/<string:uuid>', methods=['GET'])
def get_caching_node_by_system_id(uuid):
    if not uuid:
        return utils.get_response(None, "missing required url param: uuid", 400)

    nodes = db_controller.get_caching_nodes()
    node_found = None
    for node in nodes:
        if node.system_uuid == uuid:
            node_found = node
            break

    if not node_found:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    d = node_found.get_clean_dict()
    d['status_code'] = node_found.get_status_code()
    return utils.get_response([d])


@bp.route('/cachingnode/connect/<string:uuid>', methods=['PUT'])
def caching_node_connect(uuid):
    cnode = db_controller.get_caching_node_by_id(uuid)
    if not cnode:
        return utils.get_response_error(f"Caching node not found: {uuid}", 404)

    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_response(None, "missing required param: lvol_id", 400)

    lvol_id = cl_data['lvol_id']
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {lvol_id}", 404)

    ret = caching_node_controller.connect(cnode.get_id(), lvol.get_id())

    return utils.get_response(ret)


@bp.route('/cachingnode/disconnect/<string:uuid>', methods=['PUT'])
def caching_node_disconnect(uuid):
    cnode = db_controller.get_caching_node_by_id(uuid)
    if not cnode:
        return utils.get_response_error(f"Caching node not found: {uuid}", 404)

    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_response(None, "missing required param: lvol_id", 400)

    lvol_id = cl_data['lvol_id']
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {lvol_id}", 404)

    ret = caching_node_controller.disconnect(cnode.get_id(), lvol.get_id())

    return utils.get_response(ret)


@bp.route('/cachingnode/lvols/<string:uuid>', methods=['GET'])
def caching_node_list_lvols(uuid):
    cnode = db_controller.get_caching_node_by_id(uuid)
    if not cnode:
        return utils.get_response_error(f"Caching node not found: {uuid}", 404)

    data = []

    for clvol in cnode.lvols:
        lvol = clvol.lvol
        logger.debug(clvol)
        logger.debug("*" * 20)
        data.append({
            "UUID": lvol.get_id(),
            "Hostname": lvol.hostname,
            "Size": lvol.size,
            "Path": clvol.device_path,
            "Status": lvol.status,
        })

    return utils.get_response(data)


@bp.route('/cachingnode/recreate/<string:hostname>', methods=['GET'])
def recreate_caching_node(hostname):
    cnode = db_controller.get_caching_node_by_hostname(hostname)
    if not cnode:
        return utils.get_response_error(f"Caching node not found: {hostname}", 404)

    data = caching_node_controller.recreate(cnode.get_id())

    return utils.get_response(data)
