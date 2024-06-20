#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint, request

from simplyblock_web import utils
from simplyblock_core.controllers import pool_controller

from simplyblock_core.models.pool import Pool
from simplyblock_core import kv_store, utils as core_utils

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("pool", __name__)
db_controller = kv_store.DBController()


@bp.route('/pool', defaults={'uuid': None, "cluster_id": None}, methods=['GET'])
@bp.route('/pool/<string:uuid>', defaults={"cluster_id": None}, methods=['GET'])
@bp.route('/pool/cluster_id/<string:cluster_id>', defaults={'uuid': None,}, methods=['GET'])
def list_pools(uuid, cluster_id):
    if uuid:
        pool = db_controller.get_pool_by_id(uuid)
        if pool:
            pools = [pool]
        else:
            return utils.get_response_error(f"Pool not found: {uuid}", 404)
    elif cluster_id:
        pools = db_controller.get_pools(cluster_id)
    else:
        cluster_id = utils.get_cluster_id(request)
        pools = db_controller.get_pools(cluster_id)
    data = []
    for pool in pools:
        data.append(pool.get_clean_dict())
    return utils.get_response(data)


@bp.route('/pool', methods=['POST'])
def add_pool():
    """
        Params:
        | name (required) | LVol name or id
        | cluster_id (required) | Cluster uuid
        | pool_max        | Pool maximum size: 10M, 10G, 10(bytes)
        | lvol_max        | LVol maximum size: 10M, 10G, 10(bytes)
        | no_secret       | pool is created with a secret
        | max_rw_iops     | Maximum Read Write IO Per Second
        | max_rw_mbytes   | Maximum Read Write Mega Bytes Per Second
        | max_r_mbytes    | Maximum Read Mega Bytes Per Second
        | max_w_mbytes    | Maximum Write Mega Bytes Per Second
    """
    pool_data = request.get_json()
    if 'name' not in pool_data:
        return utils.get_response_error("missing required param: name", 400)

    if 'cluster_id' not in pool_data:
        return utils.get_response_error("missing required param: cluster_id", 400)

    name = pool_data['name']
    cluster_id = pool_data['cluster_id']
    for p in db_controller.get_pools():
        if p.pool_name == name:
            return utils.get_response_error(f"Pool found with the same name: {name}", 400)

    pool_secret = True
    if 'no_secret' in pool_data:
        pool_secret = False

    pool_max_size = 0
    lvol_max_size = 0
    if 'pool_max' in pool_data:
        pool_max_size = utils.parse_size(pool_data['pool_max'])

    if 'lvol_max' in pool_data:
        lvol_max_size = utils.parse_size(pool_data['lvol_max'])

    max_rw_iops = utils.get_int_value_or_default(pool_data, "max_rw_iops", 0)
    max_rw_mbytes = utils.get_int_value_or_default(pool_data, "max_rw_mbytes", 0)
    max_r_mbytes_per_sec = utils.get_int_value_or_default(pool_data, "max_r_mbytes", 0)
    max_w_mbytes_per_sec = utils.get_int_value_or_default(pool_data, "max_w_mbytes", 0)

    ret = pool_controller.add_pool(
        name, pool_max_size, lvol_max_size, max_rw_iops, max_rw_mbytes,
        max_r_mbytes_per_sec, max_w_mbytes_per_sec, pool_secret, cluster_id)

    return utils.get_response(ret)


@bp.route('/pool/<string:uuid>', methods=['DELETE'])
def delete_pool(uuid):
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.status == Pool.STATUS_INACTIVE:
        return utils.get_response_error("Pool is disabled", 400)

    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    if pool.lvols:
        return utils.get_response_error(f"Can not delete Pool with LVols", 400)

    pool.remove(db_controller.kv_store)
    return utils.get_response("Done")


@bp.route('/pool/<string:uuid>', methods=['PUT'])
def update_pool(uuid):
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.status == Pool.STATUS_INACTIVE:
        return utils.get_response_error("Pool is disabled")

    # if pool.secret:
    #     req_secret = request.headers.get('secret', "")
    #     if req_secret != pool.secret:
    #         return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    pool_data = request.get_json()

    pool.pool_name = pool_data.get('name') or pool.pool_name

    if 'pool-max' in pool_data:
        pool.pool_max_size = utils.parse_size(pool_data['pool-max'])

    if 'lvol-max' in pool_data:
        pool.lvol_max_size = utils.parse_size(pool_data['lvol-max'])

    if 'max-r-iops' in pool_data:
        pool.max_r_iops = utils.parse_size(pool_data['max-r-iops'])

    if 'max-w-iops' in pool_data:
        pool.max_w_iops = utils.parse_size(pool_data['max-w-iops'])

    if 'max-r-mbytes' in pool_data:
        pool.max_r_mbytes_per_sec = utils.parse_size(pool_data['max-r-mbytes'])

    if 'max-w-mbytes' in pool_data:
        pool.max_w_mbytes_per_sec = utils.parse_size(pool_data['max-w-mbytes'])

    pool.write_to_db(db_controller.kv_store)
    return utils.get_response(pool.to_dict())


@bp.route('/pool/capacity/<string:uuid>', methods=['GET'])
def pool_capacity(uuid):
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    out = []
    total_size = 0
    for lvol_id in pool.lvols:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        total_size += lvol.size
        out.append({
            "device name": lvol.lvol_name,
            "provisioned": lvol.size,
            "util_percent": 0,
            "util": 0,
        })
    if pool.lvols:
        out.append({
            "device name": "Total",
            "provisioned": total_size,
            "util_percent": 0,
            "util": 0,
        })
    return utils.get_response(out)


@bp.route('/pool/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/pool/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def pool_iostats(uuid, history):
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    if history:
        records_number = core_utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    out = db_controller.get_pool_stats(pool, records_number)
    records_count = 20
    new_records = core_utils.process_records(out, records_count)

    return utils.get_response(new_records)
