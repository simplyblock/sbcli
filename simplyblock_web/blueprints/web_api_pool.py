#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint, request

from simplyblock_web import utils
from simplyblock_core.controllers import pool_controller

from simplyblock_core.models.pool import Pool
from simplyblock_core import kv_store
from simplyblock_core.models.device_stat import LVolStat

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bp = Blueprint("pool", __name__)
db_controller = kv_store.DBController()


@bp.route('/pool', defaults={'uuid': None}, methods=['GET'])
@bp.route('/pool/<string:uuid>', methods=['GET'])
def list_pools(uuid):
    if uuid:
        pool = db_controller.get_pool_by_id(uuid)
        if pool:
            pools = [pool]
        else:
            return utils.get_response_error(f"Pool not found: {uuid}", 404)
    else:
        pools = db_controller.get_pools()
    data = []
    for pool in pools:
        data.append(pool.get_clean_dict())
    return utils.get_response(data)


@bp.route('/pool', methods=['POST'])
def add_pool():
    pool_data = request.get_json()
    if 'name' not in pool_data:
        return utils.get_response_error("missing required param: name", 400)

    name = pool_data['name']
    for p in db_controller.get_pools():
        if p.pool_name == name:
            return utils.get_response_error(f"Pool found with the same name: {name}", 400)

    pool_secret = True
    if 'no_secret' in pool_data:
        pool_secret = False

    pool_max_size = utils.get_int_value_or_default(pool_data, "pool-max", 0)
    lvol_max_size = utils.get_int_value_or_default(pool_data, "lvol-max", 0)
    max_rw_iops = utils.get_int_value_or_default(pool_data, "max-rw-iops", 0)
    max_rw_mbytes = utils.get_int_value_or_default(pool_data, "max-rw-mbytes", 0)
    max_r_mbytes_per_sec = utils.get_int_value_or_default(pool_data, "max-r-mbytes", 0)
    max_w_mbytes_per_sec = utils.get_int_value_or_default(pool_data, "max-w-mbytes", 0)

    ret = pool_controller.add_pool(
        name, pool_max_size, lvol_max_size, max_rw_iops, max_rw_mbytes,
        max_r_mbytes_per_sec, max_w_mbytes_per_sec, pool_secret)

    return utils.get_response(ret, "Error")


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
            "provisioned": utils.humanbytes(lvol.size),
            "util_percent": 0,
            "util": 0,
        })
    if pool.lvols:
        out.append({
            "device name": "Total",
            "provisioned": utils.humanbytes(total_size),
            "util_percent": 0,
            "util": 0,
        })
    return utils.get_response(out)


@bp.route('/pool/iostats/<string:uuid>', methods=['GET'])
def pool_iostats(uuid):
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    out = []
    total_values = {
        "read_bytes_per_sec": 0,
        "read_iops": 0,
        "write_bytes_per_sec": 0,
        "write_iops": 0,
        "unmapped_bytes_per_sec": 0,
        "read_latency_ticks": 0,
        "write_latency_ticks": 0,
    }
    for lvol_id in pool.lvols:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        record = LVolStat(data={"uuid": lvol.get_id(), "node_id": lvol.node_id}).get_last(db_controller.kv_store)
        if not record:
            continue
        out.append({
            "LVol name": lvol.lvol_name,
            "bytes_read (MB/s)": record.read_bytes_per_sec,
            "num_read_ops (IOPS)": record.read_iops,
            "bytes_write (MB/s)": record.write_bytes_per_sec,
            "num_write_ops (IOPS)": record.write_iops,
            "bytes_unmapped (MB/s)": record.unmapped_bytes_per_sec,
            "read_latency_ticks": record.read_latency_ticks,
            "write_latency_ticks": record.write_latency_ticks,
        })
        total_values["read_bytes_per_sec"] += record.read_bytes_per_sec
        total_values["read_iops"] += record.read_iops
        total_values["write_bytes_per_sec"] += record.write_bytes_per_sec
        total_values["write_iops"] += record.write_iops
        total_values["unmapped_bytes_per_sec"] += record.unmapped_bytes_per_sec
        total_values["read_latency_ticks"] += record.read_latency_ticks
        total_values["write_latency_ticks"] += record.write_latency_ticks

    out.append({
        "LVol name": "Total",
        "bytes_read (MB/s)": total_values['read_bytes_per_sec'],
        "num_read_ops (IOPS)": total_values["read_iops"],
        "bytes_write (MB/s)": total_values["write_bytes_per_sec"],
        "num_write_ops (IOPS)": total_values["write_iops"],
        "bytes_unmapped (MB/s)": total_values["unmapped_bytes_per_sec"],
        "read_latency_ticks": total_values["read_latency_ticks"],
        "write_latency_ticks": total_values["write_latency_ticks"],
    })

    return utils.get_response(out)

