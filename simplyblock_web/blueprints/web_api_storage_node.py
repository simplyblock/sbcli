#!/usr/bin/env python
# encoding: utf-8

import logging
import threading

from flask import Blueprint, request

from simplyblock_core.controllers import tasks_controller
from simplyblock_web import utils

from simplyblock_core import db_controller, storage_node_ops, utils as core_utils

logger = logging.getLogger(__name__)

bp = Blueprint("snode", __name__)
db = db_controller.DBController()


@bp.route('/storagenode', methods=['GET'], defaults={'uuid': None})
@bp.route('/storagenode/<string:uuid>', methods=['GET'])
def list_storage_nodes(uuid):
    cluster_id = utils.get_cluster_id(request)
    if uuid:
        node = db.get_storage_node_by_id(uuid)
        if node and node.cluster_id == cluster_id:
            nodes = [node]
        else:
            return utils.get_response_error(f"node not found: {uuid}", 404)
    else:
        nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    data = []
    for node in nodes:
        d = node.get_clean_dict()
        d['status_code'] = node.get_status_code()
        lvs = db.get_lvols_by_node_id(node.get_id()) or []
        d['lvols'] = len(lvs)
        data.append(d)
    return utils.get_response(data)


@bp.route('/storagenode/capacity/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/storagenode/capacity/<string:uuid>', methods=['GET'], defaults={'history': None})
def storage_node_capacity(uuid, history):
    node = db.get_storage_node_by_id(uuid)
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
    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    data = storage_node_ops.get_node_iostats_history(uuid, history, parse_sizes=False, with_sizes=True)
    ret = {
        "object_data": node.get_clean_dict(),
        "stats": data or []
    }
    return utils.get_response(ret)


@bp.route('/storagenode/port/<string:uuid>', methods=['GET'])
def storage_node_ports(uuid):
    node = db.get_storage_node_by_id(uuid)
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
    nodes = db.get_storage_nodes()
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

    data = db.get_port_stats(nd.get_id(), port.get_id())
    out = []
    for record in data:
        out.append(record.get_clean_dict())
    return utils.get_response(out)


@bp.route('/storagenode/suspend/<string:uuid>', methods=['GET'])
def storage_node_suspend(uuid):
    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.suspend_storage_node(uuid, True)
    return utils.get_response(out)


@bp.route('/storagenode/resume/<string:uuid>', methods=['GET'])
def storage_node_resume(uuid):
    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.resume_storage_node(uuid)
    return utils.get_response(out)


@bp.route('/storagenode/shutdown/<string:uuid>', methods=['GET'])
def storage_node_shutdown(uuid):
    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    force = True
    try:
        args = request.args
        force = bool(args.get('force', True))
    except:
        pass

    threading.Thread(
        target=storage_node_ops.shutdown_storage_node,
        args=(uuid, force)
    ).start()

    return utils.get_response(True)


@bp.route('/storagenode/restart', methods=['PUT'])
def storage_node_restart():
    req_data = request.get_json()
    uuid = req_data.get("uuid", "")
    node_ip = req_data.get("node_ip", "")
    reattach_volume = bool(req_data.get("reattach_volume", False))
    force = bool(req_data.get("force", ""))

    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    threading.Thread(
        target=storage_node_ops.restart_storage_node,
        kwargs={
            "node_id": uuid,
            "node_ip": node_ip,
            "force": force,
            "reattach_volume": reattach_volume,
        }
    ).start()

    return utils.get_response(True)


@bp.route('/storagenode/add', methods=['POST'])
def storage_node_add():
    req_data = request.get_json()

    if 'cluster_id' not in req_data:
        return utils.get_response_error("missing required param: cluster_id", 400)

    if 'node_addr' not in req_data:
        return utils.get_response_error("missing required param: node_addr", 400)

    if 'ifname' not in req_data:
        return utils.get_response_error("missing required param: ifname", 400)

    cluster_id = req_data['cluster_id']
    node_addr = req_data['node_addr']
    ifname = req_data['ifname']
    max_snap = int(req_data.get('max_snap', 500))

    if req_data.get('disable_ha_jm', "") == "true":
        disable_ha_jm = True
    else:
        disable_ha_jm = False

    enable_test_device = False
    param = req_data.get('enable_test_device')
    if param:
        if isinstance(param, bool):
            enable_test_device = param
        elif isinstance(param, str):
            enable_test_device = param == "true"

    spdk_image = None
    if 'spdk_image' in req_data:
        spdk_image = req_data['spdk_image']

    spdk_debug = None
    if 'spdk_debug' in req_data:
        spdk_debug = req_data['spdk_debug']

    full_page_unmap = None
    if 'full_page_unmap' in req_data:
        full_page_unmap = req_data['full_page_unmap']

    data_nics = None
    if 'data_nics' in req_data:
        data_nics = req_data['data_nics']
        data_nics = data_nics.split(",")

    namespace = "default"
    if 'namespace' in req_data:
        namespace = req_data['namespace']

    jm_percent = 3
    if 'jm_percent' in req_data:
        jm_percent = int(req_data['jm_percent'])

    partitions = 1
    if 'partitions' in req_data:
        partitions = int(req_data['partitions'])

    iobuf_small_pool_count = 0
    if 'iobuf_small_pool_count' in req_data:
        iobuf_small_pool_count = int(req_data['iobuf_small_pool_count'])

    iobuf_large_pool_count = 0
    if 'iobuf_large_pool_count' in req_data:
        iobuf_large_pool_count = int(req_data['iobuf_large_pool_count'])

    tasks_controller.add_node_add_task(cluster_id, {
        "cluster_id": cluster_id,
        "node_addr": node_addr,
        "iface_name": ifname,
        "data_nics_list": data_nics,
        "max_snap": max_snap,
        "spdk_image": spdk_image,
        "spdk_debug": spdk_debug,
        "small_bufsize": iobuf_small_pool_count,
        "large_bufsize": iobuf_large_pool_count,
        "num_partitions_per_dev": partitions,
        "jm_percent": jm_percent,
        "enable_test_device": enable_test_device,
        "namespace": namespace,
        "enable_ha_jm": not disable_ha_jm,
        "full_page_unmap": full_page_unmap,
    })

    return utils.get_response(True)


@bp.route('/storagenode/make-sec-new-primary/<string:uuid>', methods=['GET'])
def make_primary(uuid):
    node = db.get_storage_node_by_id(uuid)
    if not node:
        return utils.get_response_error(f"node not found: {uuid}", 404)

    out = storage_node_ops.make_sec_new_primary(uuid)
    return utils.get_response(out)
