#!/usr/bin/env python
# encoding: utf-8

import logging
from random import random

from flask import Blueprint
from flask import request

from simplyblock_web import utils
from simplyblock_core import kv_store
from simplyblock_core.controllers import lvol_controller, snapshot_controller


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
db_controller = kv_store.DBController()
bp = Blueprint("csi", __name__)


def get_vol_node():
    snodes = db_controller.get_storage_nodes()
    max_available_size = 0
    max_available_node = None
    for node in snodes:
        node_size = 0
        lvols_size = 0
        if node.status == node.STATUS_ONLINE:
            devices = node.nvme_devices
            for device in devices:
                node_size += device.size
            for lvol_id in node.lvols:
                lvol = db_controller.get_lvol_by_id(lvol_id)
                if lvol:
                    lvols_size += lvol.size
            available_size = node_size - lvols_size
            if available_size > max_available_size:
                max_available_size = available_size
                max_available_node = node
    return max_available_node


def get_snap_by_id_or_name(src_id):
    return db_controller.get_snapshot_by_id(src_id.split(":")[-1])


def get_lvol_by_id_or_name(src_id):
    return db_controller.get_lvol_by_id(src_id.split(":")[-1])


def get_param_value_or_default(data, key_name, default):
    if key_name in data:
        value = utils.parse_size(data[key_name])
        if isinstance(value, int):
            return value
    return default


def validate_header_data():
    cl_id = request.headers.get('cluster')
    secret = request.headers.get('secret')
    logger.debug(f"Headers:cluster={cl_id}")
    logger.debug(f"Headers:secret={secret}")
    if not cl_id:
        logger.error("no 'cluster' key found in the request headers")
        return False
    if not secret:
        logger.error("no 'secret' key found in the request headers")
        return False
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found: {cl_id}")
        return False
    if cluster.secret == secret:
        return True


@bp.route('/csi/get_pools', methods=['GET'])
def csi_get_pools():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    pools = db_controller.get_pools()
    data = []
    cluster_size = 512
    for pool in pools:
        total_size = 0
        for lvol_id in pool.lvols:
            lvol = db_controller.get_lvol_by_id(lvol_id)
            total_size += lvol.size

        data.append({
            "free_clusters": int((pool.pool_max_size - total_size) / cluster_size),
            "cluster_size": cluster_size,
            "total_data_clusters": int(total_size / cluster_size),
            "name": pool.pool_name,
            "uuid": pool.id,
        })
    return utils.get_csi_response(data)


@bp.route('/csi/get_volume_info/<string:pool_name>/<string:lvol_name>', methods=['GET'])
def csi_get_volume_info_by_name(pool_name, lvol_name):
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    for lv in db_controller.get_lvols():
        if lv.lvol_name == lvol_name:
            return csi_get_volume_info(lv.get_id())
    return utils.get_csi_response(None, "lvol not found")


@bp.route('/csi/get_volume_info/<string:uuid>', methods=['GET'])
def csi_get_volume_info(uuid):
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_csi_response(None, "lvol not found")
    else:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)

        out = [{
            "name": lvol.lvol_name,
            "uuid": lvol.uuid,
            "block_size": 512,
            "num_blocks": int(lvol.size / 512),
            "driver_specific": {"lvol": {"lvol_store_uuid": lvol.pool_uuid}},
            "pool_id": lvol.pool_uuid,

            "targetType": "TCP",
            "targetAddr": snode.data_nics[0].ip4_address,
            "targetPort": "4420",
            "nqn": snode.subsystem + ":lvol:" + lvol.get_id(),
            "model": lvol.get_id(),
            "lvolSize": lvol.size,
        }]

        return utils.get_csi_response(out)


@bp.route('/csi/delete_lvol/<string:uuid>', methods=['DELETE'])
def csi_delete_volume(uuid):
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    try:
        ret = lvol_controller.delete_lvol(uuid)
    except:
        pass
    return utils.get_csi_response(True)


@bp.route('/csi/create_volume', methods=['POST'])
def csi_create_volume():
    """"
    Params:
		LvolName      string `json:"lvol_name"`
		Size          int64  `json:"size"`
		LvsName       string `json:"lvs_name"`
		ClearMethod   string `json:"clear_method"`
		ThinProvision bool   `json:"thin_provision"`
		SourceType    string `json:"src_type"`
		SourceID      string `json:"src_id"`

		Qos_rw_iops   string  `json:"qos_rw_iops"`
		Qos_rw_mbytes string  `json:"qos_rw_mbytes"`
		Qos_r_mbytes  string  `json:"qos_r_mbytes"`
		Qos_w_mbytes  string  `json:"qos_w_mbytes"`
		Compression   string   `json:"compression"`
		Encryption    string   `json:"encryption"`
    """""


    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    cl_data = request.get_json()
    logger.info(cl_data)
    print(cl_data)
    if 'size' not in cl_data:
        return utils.get_csi_response(None, "missing required param: size", 400)
    if 'lvs_name' not in cl_data:
        return utils.get_csi_response(None, "missing required param: lvs_name", 400)
    if 'lvol_name' not in cl_data:
        return utils.get_csi_response(None, "missing required param: lvol_name", 400)

    if "src_type" in cl_data and cl_data["src_type"]:
        src_type = cl_data["src_type"]
        src_id = cl_data["src_id"]
        if src_type == "lvol":
            lvol = get_lvol_by_id_or_name(src_id)
            if lvol is None:
                return utils.get_csi_response(None, f"lvol not found: lvol id: {lvol}", 400)
            snap = snapshot_controller.add(src_id, f"snap_{cl_data['lvol_name']}")

        elif src_type == "snapshot":
            snap = get_snap_by_id_or_name(src_id)
            if snap is None:
                return utils.get_csi_response(None, f"Snapshot not found: snapshot id: {src_id}", 400)
        else:
            return utils.get_csi_response(None, f"Unknown source type {src_type}", 400)

        new_clone = snapshot_controller.clone(snap.get_id(), cl_data['lvol_name'])
        if cl_data['size'] > snap.lvol.size:
            logger.info("resizing")
            lvol_controller.resize_lvol(new_clone, cl_data['size'])
            return utils.get_csi_response(new_clone)

    else:
        host = get_vol_node()
        if host is None:
            return utils.get_csi_response(None, "No active storage nodes found", 400)
        rw_iops = get_param_value_or_default(cl_data, "qos_rw_iops", 0)
        rw_mbytes = get_param_value_or_default(cl_data, "qos_rw_mbytes", 0)
        r_mbytes = get_param_value_or_default(cl_data, "qos_r_mbytes", 0)
        w_mbytes = get_param_value_or_default(cl_data, "qos_w_mbytes", 0)
        compression = False
        if "compression" in cl_data and cl_data['compression'] == "True":
            compression = True
        encryption = False
        if "encryption" in cl_data and cl_data['encryption'] == "True":
            encryption = True

        distr_vuid = int(random()*10000)
        ret, error = lvol_controller.add_lvol(
            cl_data['lvol_name'],
            cl_data['size'],
            host.get_id(),
            cl_data['lvs_name'],
            compression, encryption, distr_vuid, 1, 0,
            rw_iops, rw_mbytes, r_mbytes, w_mbytes)
        return utils.get_csi_response(ret, error)


@bp.route('/csi/is_volume_published/<string:uuid>', methods=['GET'])
def csi_is_volume_published(uuid):
    """

    Args:
        uuid:

    Returns:
        bool
    """
    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        return utils.get_csi_response(None, "lvol not found")
    return utils.get_csi_response(True)


@bp.route('/csi/publish_volume/<string:uuid>', methods=['GET'])
def csi_publish_volume(uuid):
    """

    Args:
        uuid:

    Returns:
        bool
    """
    return utils.get_csi_response(True)


@bp.route('/csi/unpublish_volume/<string:uuid>', methods=['GET'])
def csi_unpublish_volume(uuid):
    """

    Args:
        uuid:

    Returns:
        bool
    """
    return utils.get_csi_response(True)


@bp.route('/csi/create_snapshot', methods=['POST'])
def csi_create_snapshot():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_csi_response(None, "missing required param: lvol_id", 400)
    if 'snapshot_name' not in cl_data:
        return utils.get_csi_response(None, "missing required param: snapshot_name", 400)

    snapID = snapshot_controller.add(
        cl_data['lvol_id'],
        cl_data['snapshot_name'])
    return utils.get_csi_response(snapID)


@bp.route('/csi/delete_snapshot/<string:uuid>', methods=['DELETE'])
def csi_delete_snapshot(uuid):
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    ret = snapshot_controller.delete(uuid)
    return utils.get_csi_response(ret)


@bp.route('/csi/resize_lvol', methods=['POST'])
def csi_resize_lvo():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    cl_data = request.get_json()
    if 'lvol_id' not in cl_data:
        return utils.get_csi_response(None, "missing required param: lvol_id", 400)
    if 'new_size' not in cl_data:
        return utils.get_csi_response(None, "missing required param: new_size", 400)
    ret = lvol_controller.resize_lvol(cl_data['lvol_id'],
                                      cl_data['new_size'])
    return utils.get_csi_response(ret)


@bp.route('/csi/list_snapshots', methods=['GET'])
def csi_list_snapshots():
    if not validate_header_data():
        return utils.get_csi_response(None, "Invalid Auth data, see API logs for more details.")

    snaps = db_controller.get_snapshots()
    data = []
    for snap in snaps:
        pool = db_controller.get_pool_by_id(snap.lvol.pool_uuid)
        data.append({
            "uuid": snap.uuid,
            "name": pool.pool_name,
            "size": str(snap.lvol.size),
            "pool_name": snap.snap_bdev,
            "pool_id": snap.lvol.pool_uuid,
            "source_uuid": snap.lvol.get_id(),
            "created_at": str(snap.created_at),
        })
    return utils.get_csi_response(data)
