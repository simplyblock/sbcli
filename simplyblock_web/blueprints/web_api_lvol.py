
#!/usr/bin/env python
# encoding: utf-8

import logging

from flask import Blueprint
from flask import request

from simplyblock_core.controllers import lvol_controller, snapshot_controller

from simplyblock_web import utils

from simplyblock_core import db_controller, utils as core_utils

logger = logging.getLogger(__name__)

bp = Blueprint("lvol", __name__)
db = db_controller.DBController()


@bp.route('/lvol', defaults={'uuid': None}, methods=['GET'])
@bp.route('/lvol/<string:uuid>', methods=['GET'])
def list_lvols(uuid):
    cluster_id = utils.get_cluster_id(request)
    lvols = []
    if uuid:
        lvol = db.get_lvol_by_id(uuid)
        if lvol:
            node = db.get_storage_node_by_id(lvol.node_id)
            if node.cluster_id == cluster_id:
                lvols = [lvol]

        if not lvols:
            return utils.get_response_error(f"LVol not found: {uuid}", 404)
    else:
        lvols = db.get_lvols(cluster_id)
    data = []
    for lvol in lvols:
        tmp = lvol.get_clean_dict()
        data.append(tmp)
    return utils.get_response(data)


@bp.route('/lvol/iostats/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/lvol/iostats/<string:uuid>', methods=['GET'], defaults={'history': None})
def lvol_iostats(uuid, history):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    pool = db.get_pool_by_id(lvol.pool_uuid)
    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    data = lvol_controller.get_io_stats(uuid, history, parse_sizes=False, with_sizes=True)
    ret = {
        "object_data": lvol.get_clean_dict(),
        "stats": data or []
    }
    return utils.get_response(ret)

@bp.route('/lvol/capacity/<string:uuid>/history/<string:history>', methods=['GET'])
@bp.route('/lvol/capacity/<string:uuid>', methods=['GET'], defaults={'history': None})
def lvol_capacity(uuid, history):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    pool = db.get_pool_by_id(lvol.pool_uuid)
    if pool.secret:
        req_secret = request.headers.get('secret', "")
        if req_secret != pool.secret:
            return utils.get_response_error(f"Pool secret doesn't mach the value in the request header", 400)

    data = lvol_controller.get_capacity(uuid, history, parse_sizes=False)
    out = []
    if data:
        for record in data:
            out.append({
                "date":record["date"],
                "prov": record["size_prov"],
                "used": record["size_used"],
                "free": record["size_free"],
                "util": record["size_util"],
                "prov_util": record["size_prov_util"],
            })

    ret = {
        "object_data": lvol.get_clean_dict(),
        "stats": out or []
    }
    return utils.get_response(ret)


@bp.route('/lvol', methods=['POST'])
def add_lvol():
    """"
    Params:
        | name (required) | LVol name or id
        | size (required) | LVol size: 10M, 10G, 10(bytes)
        | pool (required) | Pool UUID or name
        | crypto          | Create a new crypto LVol
        | max_rw_iops     | Maximum Read Write IO Per Second
        | max_rw_mbytes   | Maximum Read Write Mega Bytes Per Second
        | max_r_mbytes    | Maximum Read Mega Bytes Per Second
        | max_w_mbytes    | Maximum Write Mega Bytes Per Second
        | ha_type         | LVol HA type, can be (single,ha,default=cluster's ha type), Default=default
        | crypto_key1     | the hex value of key1 to be used for lvol encryption
        | crypto_key2     | the hex value of key2 to be used for lvol encryption
        | host_id         | the hostID on which the lvol is created
        | lvol_priority_class | the LVol priority class (0, 1)
        | namespace       | the LVol namespace for k8s
        | uid             | use this UUID for this LVol
        | pvc_name        | set PVC name for this LVol
    """""

    cl_data = request.get_json()
    logger.debug(cl_data)
    if 'size' not in cl_data:
        return utils.get_response(None, "missing required param: size", 400)
    if 'name' not in cl_data:
        return utils.get_response(None, "missing required param: name", 400)
    if 'pool' not in cl_data:
        return utils.get_response(None, "missing required param: pool", 400)

    name = cl_data['name']
    pool_id_or_name = cl_data['pool']
    size = core_utils.parse_size(cl_data['size'])

    pool = None
    for p in db.get_pools():
        if pool_id_or_name == p.get_id() or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return utils.get_response(None, f"Pool not found: {pool_id_or_name}", 400)

    for lvol in db.get_lvols():  # pass
        if lvol.pool_uuid == pool.get_id():
            if lvol.lvol_name == name:
                return utils.get_response(lvol.get_id())

    rw_iops = utils.get_int_value_or_default(cl_data, "max_rw_iops", 0)
    rw_mbytes = utils.get_int_value_or_default(cl_data, "max_rw_mbytes", 0)
    r_mbytes = utils.get_int_value_or_default(cl_data, "max_r_mbytes", 0)
    w_mbytes = utils.get_int_value_or_default(cl_data, "max_w_mbytes", 0)
    # max_size = utils.get_int_value_or_default(cl_data, "max_size", 0)

    encryption = utils.get_value_or_default(cl_data, "crypto", False)

    ha_type = utils.get_value_or_default(cl_data, "ha_type", "default")

    crypto_key1 = utils.get_value_or_default(cl_data, "crypto_key1", None)
    crypto_key2 = utils.get_value_or_default(cl_data, "crypto_key2", None)
    host_id = utils.get_value_or_default(cl_data, "host_id", None)
    lvol_priority_class = utils.get_value_or_default(cl_data, "lvol_priority_class", 0)
    namespace = utils.get_value_or_default(cl_data, "namespace", None)
    uid = utils.get_value_or_default(cl_data, "uid", None)
    pvc_name = utils.get_value_or_default(cl_data, "pvc_name", None)

    ret, error = lvol_controller.add_lvol_ha(
        name=name,
        size=size,
        pool_id_or_name=pool.get_id(),

        use_crypto=encryption,

        max_size=0,
        max_rw_iops=rw_iops,
        max_rw_mbytes=rw_mbytes,
        max_r_mbytes=r_mbytes,
        max_w_mbytes=w_mbytes,

        host_id_or_name=host_id,
        ha_type=ha_type,
        crypto_key1=crypto_key1,
        crypto_key2=crypto_key2,

        use_comp=False,
        distr_vuid=0,
        lvol_priority_class=lvol_priority_class,
        namespace=namespace,
        uid=uid,
        pvc_name=pvc_name
    )

    return utils.get_response(ret, error, http_code=400)


@bp.route('/lvol/<string:uuid>', methods=['PUT'])
def update_lvol(uuid):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    cl_data = request.get_json()

    name = None
    if 'name' in cl_data:
        name = cl_data['name']

    rw_iops = 0
    if "max-rw-iops" in cl_data:
        rw_iops = cl_data['max-rw-iops']

    rw_mbytes = 0
    if "max-rw-mbytes" in cl_data:
        rw_mbytes = cl_data['max-rw-mbytes']

    r_mbytes = 0
    if "max-r-mbytes" in cl_data:
        r_mbytes = cl_data['max-r-mbytes']

    w_mbytes = 0
    if "max-w-mbytes" in cl_data:
        w_mbytes = cl_data['max-w-mbytes']

    ret = lvol_controller.set_lvol(
        uuid=uuid,
        max_rw_iops=rw_iops,
        max_rw_mbytes=rw_mbytes,
        max_r_mbytes=r_mbytes,
        max_w_mbytes=w_mbytes,
        name=name
    )
    return utils.get_response(ret)


@bp.route('/lvol/<string:uuid>', methods=['DELETE'])
def delete_lvol(uuid):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    pool = db.get_pool_by_id(lvol.pool_uuid)
    if not pool:
        return utils.get_response_error(f"Pool not found: {uuid}", 404)

    if pool.status == pool.STATUS_INACTIVE:
        return utils.get_response_error("Pool is disabled", 400)

    ret = lvol_controller.delete_lvol(uuid)

    return utils.get_response(ret)


@bp.route('/lvol/resize/<string:uuid>', methods=['PUT'])
def resize_lvol(uuid):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    cl_data = request.get_json()
    if 'size' not in cl_data:
        return utils.get_response(None, "missing required param: size", 400)

    new_size = core_utils.parse_size(cl_data['size'])

    ret, error = lvol_controller.resize_lvol(uuid, new_size)
    return utils.get_response(ret, error)


@bp.route('/lvol/connect/<string:uuid>', methods=['GET'])
def connect_lvol(uuid):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)

    ret = lvol_controller.connect_lvol(uuid)
    return utils.get_response(ret)


@bp.route('/lvol/create_snapshot', methods=['POST'])
def create_snapshot():
    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_response(None, "missing required param: lvol_id", 400)
    if 'snapshot_name' not in cl_data:
        return utils.get_response(None, "missing required param: snapshot_name", 400)

    snapID, err = snapshot_controller.add(
        cl_data['lvol_id'],
        cl_data['snapshot_name'])
    return utils.get_response(snapID, err, http_code=400)


@bp.route('/lvol/inflate_lvol/<string:uuid>', methods=['PUT'])
def inflate_lvol(uuid):
    lvol = db.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_response_error(f"LVol not found: {uuid}", 404)
    if not lvol.cloned_from_snap:
        return utils.get_response_error(f"LVol: {uuid} must be cloned LVol not regular one", 404)

    ret = lvol_controller.inflate_lvol(uuid)
    return utils.get_response(ret)
