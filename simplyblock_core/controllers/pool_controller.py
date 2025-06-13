# coding=utf-8

import logging as lg

import json
import random
import string
import time
import uuid

from simplyblock_core import utils
from simplyblock_core.controllers import pool_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.rpc_client import RPCClient

logger = lg.getLogger()


def _generate_string(length):
    return ''.join(random.SystemRandom().choice(
        string.ascii_letters + string.digits) for _ in range(length))


def add_pool(name, pool_max, lvol_max, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, has_secret, cluster_id):
    db_controller = DBController()
    if not name:
        logger.error("Pool name is empty!")
        return False

    pool_list = db_controller.get_pools()
    for p in pool_list:
        if p.pool_name == name:
            logger.error(f"Pool found with the same name: {name}")
            return False

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found: {cluster_id}")
        return False

    pool_max = pool_max or 0
    lvol_max = lvol_max or 0
    max_rw_iops = max_rw_iops or 0
    max_rw_mbytes = max_rw_mbytes or 0
    max_r_mbytes = max_r_mbytes or 0
    max_w_mbytes = max_w_mbytes or 0

    if max_rw_mbytes > 0:
        if max_r_mbytes > max_rw_mbytes or max_w_mbytes > max_rw_mbytes:
            logger.error("max_rw_mbytes must be greater than max_w_mbytes and max_r_mbytes")
            return False

    logger.info("Adding pool")
    pool = Pool()
    pool.uuid = str(uuid.uuid4())
    pool.cluster_id = cluster.get_id()
    pool.numeric_id = _generate_numeric_id(pool_list)
    pool.pool_name = name
    if has_secret:
        pool.secret = _generate_string(20)
    pool.pool_max_size = pool_max
    pool.lvol_max_size = lvol_max
    pool.max_rw_ios_per_sec = max_rw_iops
    pool.max_rw_mbytes_per_sec = max_rw_mbytes
    pool.max_r_mbytes_per_sec = max_r_mbytes
    pool.max_w_mbytes_per_sec = max_w_mbytes
    pool.status = "active"
    pool.write_to_db(db_controller.kv_store)
    pool_events.pool_add(pool)
    logger.info("Done")
    return pool.get_id()

def _generate_numeric_id(pool_list: list[Pool]):
    if (pool_list is None) or (len(pool_list) == 0):
        return 1

    existing_ids = []
    for p in pool_list:
        existing_ids.append(p.numeric_id)

    return (max(existing_ids) + 1)


def set_pool_value_if_above(pool, key, value):
    logger.info(f"Updating pool {key}: {value}")
    current_value = getattr(pool, key)
    if value > current_value:
        setattr(pool, key, value)
    elif value == -1:
        setattr(pool, key, 0)
    else:
        msg = f"{key}: {value} can't be less than current value: {current_value}"
        logger.error(msg)
        return False, msg
    return True, None

def qos_exists_on_child_lvol(db_controller: DBController, pool_uuid):
    for lvol in db_controller.get_lvols_by_pool_id(pool_uuid):
        if lvol.has_qos():
            return True
    return False

def set_pool(uuid, pool_max=0, lvol_max=0, max_rw_iops=0,
             max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0, name=""):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        msg = f"Pool not found: {uuid}"
        logger.error(msg)
        return False, msg

    if pool.status == Pool.STATUS_INACTIVE:
        msg = "Pool is disabled"
        logger.error(msg)
        return False, msg

    if name and name != pool.pool_name:
        for p in db_controller.get_pools():
            if p.pool_name == name:
                msg = f"Pool found with the same name: {name}"
                logger.error(msg)
                return False, msg
        pool.pool_name = name

    # Normalize inputs
    max_rw_iops = max_rw_iops or 0
    max_rw_mbytes = max_rw_mbytes or 0
    max_r_mbytes = max_r_mbytes or 0
    max_w_mbytes = max_w_mbytes or 0

    # Check for QoS conflict
    if (max_rw_iops + max_rw_mbytes + max_r_mbytes + max_w_mbytes) > 0:
        if qos_exists_on_child_lvol(db_controller, uuid):
            logger.error("One of the lvols already has QOS")
            return False, "QOS already set on one of the lvols"

    # Update values if needed
    fields_to_update = [
        ("pool_max_size", pool_max),
        ("lvol_max_size", lvol_max),
        ("max_rw_ios_per_sec", max_rw_iops),
        ("max_rw_mbytes_per_sec", max_rw_mbytes),
        ("max_r_mbytes_per_sec", max_r_mbytes),
        ("max_w_mbytes_per_sec", max_w_mbytes),
    ]

    for key, val in fields_to_update:
        if val:
            success, err = set_pool_value_if_above(pool, key, val)
            if err:
                return False, err

    # Apply QoS settings via RPC
    for hostname in db_controller.get_hostnames_by_pool_id(uuid):
        for sn in db_controller.get_storage_nodes_by_hostname(hostname):
            client = RPCClient(sn.mgmt_ip, sn.rpc_port, sn.rpc_username, sn.rpc_password)
            if not client.bdev_lvol_set_qos_limit(pool.numeric_id, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes):
                logger.error("RPC failed bdev_lvol_set_qos_limit")
                return False, "RPC failed"

    pool.write_to_db(db_controller.kv_store)
    pool_events.pool_updated(pool)
    logger.info("Done")
    return True, None

def delete_pool(uuid):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(uuid)
    if not pool:
        pool = db_controller.get_pool_by_name(uuid)
    if not pool:
        logger.error(f"Pool not found {uuid}")
        return False
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    lvols = db_controller.get_lvols_by_pool_id(uuid)
    if lvols and len(lvols) > 0:
        logger.error(f"Pool {uuid} is not empty, lvols found {len(lvols)}")
        return False

    logger.info(f"Deleting pool {pool.get_id()}")
    pool_events.pool_remove(pool)
    pool.remove(db_controller.kv_store)
    logger.info("Done")
    return True


def list_pools(is_json, cluster_id=None):
    db_controller = DBController()
    pools = db_controller.get_pools(cluster_id)
    data = []
    for pool in pools:
        lvs = db_controller.get_lvols_by_pool_id(pool.get_id()) or []
        data.append({
            "UUID": pool.get_id(),
            "Name": pool.pool_name,
            "Capacity": utils.humanbytes(get_pool_total_capacity(pool.get_id())),
            "Max size": utils.humanbytes(pool.pool_max_size),
            "LVol Max Size": utils.humanbytes(pool.lvol_max_size),
            "LVols": f"{len(lvs)}",
            "QOS": f"{pool.has_qos()}",
            "Status": pool.status,
        })

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def set_status(pool_id, status):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    logger.info(f"Setting pool:{pool_id} status to Active")
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    pool.status = status
    pool.write_to_db(db_controller.kv_store)
    logger.info("Done")


def get_pool(pool_id, is_json):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False

    data = pool.get_clean_dict()
    if is_json:
        return json.dumps(data, indent=2)
    else:
        data2 = [{"key": key, "value": data[key]} for key in data]
        return utils.print_table(data2)


def get_capacity(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False

    out = []
    total_size = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total_size += lvol.size
        out.append({
            "LVol name": lvol.lvol_name,
            "provisioned": utils.humanbytes(lvol.size),
            "util_percent": 0,
            "util": 0,
        })
    if total_size:
        out.append({
            "device name": "Total",
            "provisioned": utils.humanbytes(total_size),
            "util_percent": 0,
            "util": 0,
        })
    return utils.print_table(out)


def get_io_stats(pool_id, history, records_count=20):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    out = db_controller.get_pool_stats(pool, records_number)
    new_records = utils.process_records(out, records_count)

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record["read_io_ps"],
            "Read lat": record["read_latency_ps"],
            "Write speed": utils.humanbytes(record["write_bytes_ps"]),
            "Write IOPS": record["write_io_ps"],
            "Write lat": record["write_latency_ps"],
        })
    return utils.print_table(out)


def get_secret(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False

    if pool.secret:
        return pool.secret
    else:
        return "Pool has no secret"


def set_secret(pool_id, secret):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False

    secret = secret.strip()
    if len(secret) < 20:
        return "Secret must be at least 20 char"

    pool.secret = secret
    pool.write_to_db(db_controller.kv_store)


def get_pool_total_capacity(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    total = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total += lvol.size

    snaps = db_controller.get_snapshots()
    for snap in snaps:
        if snap.lvol.pool_uuid == pool_id:
            total += snap.used_size
    return total


def get_pool_total_rw_iops(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    if pool.max_rw_ios_per_sec <= 0:
        return 0

    total = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total += lvol.rw_ios_per_sec

    return total


def get_pool_total_rw_mbytes(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    if pool.max_rw_mbytes_per_sec <= 0:
        return 0

    total = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total += lvol.rw_mbytes_per_sec

    return total


def get_pool_total_r_mbytes(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    if pool.max_r_mbytes_per_sec <= 0:
        return 0

    total = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total += lvol.r_mbytes_per_sec

    return total


def get_pool_total_w_mbytes(pool_id):
    db_controller = DBController()
    pool = db_controller.get_pool_by_id(pool_id)
    if not pool:
        logger.error(f"Pool not found {pool_id}")
        return False
    if pool.max_w_mbytes_per_sec <= 0:
        return 0

    total = 0
    for lvol in db_controller.get_lvols_by_pool_id(pool_id):
        total += lvol.w_mbytes_per_sec

    return total
