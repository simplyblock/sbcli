# coding=utf-8
import logging as lg
import json
import string
import random
import sys
import time
import uuid
from datetime import datetime
from typing import Tuple

from simplyblock_core import utils, constants, distr_controller
from simplyblock_core.controllers import snapshot_controller, pool_controller, lvol_events, caching_node_controller, \
    tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient

logger = lg.getLogger()


def _create_crypto_lvol(rpc_client, name, base_name, key1, key2):
    key_name = f'key_{name}'
    ret = rpc_client.lvol_crypto_key_create(key_name, key1, key2)
    if not ret:
        logger.error("failed to create crypto key")
        return False
    ret = rpc_client.lvol_crypto_create(name, base_name, key_name)
    if not ret:
        logger.error(f"failed to create crypto LVol {name}")
        return False
    return ret


def _create_compress_lvol(rpc_client, base_bdev_name):
    pm_path = constants.PMEM_DIR
    ret = rpc_client.lvol_compress_create(base_bdev_name, pm_path)
    if not ret:
        logger.error("failed to create compress LVol on the storage node")
        return False
    return ret


def ask_for_device_number(devices_list):
    question = f"Enter the device number [1-{len(devices_list)}]: "
    while True:
        sys.stdout.write(question)
        choice = str(input())
        try:
            ch = int(choice.strip())
            ch -= 1
            return devices_list[ch]
        except Exception as e:
            logger.debug(e)
            sys.stdout.write(f"Please respond with numbers 1 - {len(devices_list)}\n")


def ask_for_lvol_vuid():
    question = f"Enter VUID number: "
    while True:
        sys.stdout.write(question)
        choice = str(input())
        try:
            ch = int(choice.strip())
            return ch
        except Exception as e:
            logger.debug(e)
            sys.stdout.write(f"Please respond with numbers")


def validate_add_lvol_func(name, size, host_id_or_name, pool_id_or_name,
                           max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes):
    #  Validation
    #  name validation
    db_controller = DBController()
    if not name or name == "":
        return False, "Name can not be empty"

    #  size validation
    if size < utils.parse_size('100MiB'):
        return False, "Size must be larger than 100M"

    #  host validation
    # snode = db_controller.get_storage_node_by_id(host_id_or_name)
    # if not snode:
    #     snode = db_controller.get_storage_node_by_hostname(host_id_or_name)
    #     if not snode:
    #         return False, f"Can not find storage node: {host_id_or_name}"

    # if snode.status != snode.STATUS_ONLINE:
    #     return False, "Storage node in not Online"
    #
    # if not snode.nvme_devices:
    #     return False, "Storage node has no nvme devices"

    #  pool validation
    pool = None
    for p in db_controller.get_pools():
        if pool_id_or_name == p.get_id() or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    if pool.status != pool.STATUS_ACTIVE:
        return False, f"Pool in not active: {pool_id_or_name}, status: {pool.status}"

    if 0 < pool.lvol_max_size < size:
        return False, f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, LVol size: {utils.humanbytes(size)} must be below this limit"

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + size > pool.pool_max_size:
            return False, f"Invalid LVol size: {utils.humanbytes(size)} " \
                          f"Pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"

    for lvol in db_controller.get_lvols(pool.cluster_id):
        if lvol.pool_uuid == pool.get_id():
            if lvol.lvol_name == name:
                return False, f"LVol name must be unique: {name}"

    if pool.has_qos():
        if pool.max_rw_ios_per_sec > 0:
            if max_rw_iops <= 0:
                return False, "LVol must have max_rw_iops value because the Pool has it set"
            total = pool_controller.get_pool_total_rw_iops(pool.get_id())
            if max_rw_iops + total > pool.max_rw_ios_per_sec:
                return False, f"Invalid LVol max_rw_iops: {max_rw_iops} " \
                              f"Pool Max RW IOPS has reached {total} of {pool.max_rw_ios_per_sec}"

        if pool.max_rw_mbytes_per_sec > 0:
            if max_rw_mbytes <= 0:
                return False, "LVol must have max_rw_mbytes value because the Pool has it set"
            total = pool_controller.get_pool_total_rw_mbytes(pool.get_id())
            if max_rw_mbytes + total > pool.max_rw_mbytes_per_sec:
                return False, f"Invalid LVol max_rw_mbytes: {max_rw_mbytes} " \
                              f"Pool Max RW MBytes has reached {total} of {pool.max_rw_mbytes_per_sec}"

        if pool.max_r_mbytes_per_sec > 0:
            if max_r_mbytes <= 0:
                return False, "LVol must have max_r_mbytes value because the Pool has it set"
            total = pool_controller.get_pool_total_r_mbytes(pool.get_id())
            if max_r_mbytes + total > pool.max_r_mbytes_per_sec:
                return False, f"Invalid LVol max_r_mbytes: {max_r_mbytes} " \
                              f"Pool Max R MBytes has reached {total} of {pool.max_r_mbytes_per_sec}"

        if pool.max_w_mbytes_per_sec > 0:
            if max_w_mbytes <= 0:
                return False, "LVol must have max_w_mbytes value because the Pool has it set"
            total = pool_controller.get_pool_total_w_mbytes(pool.get_id())
            if max_w_mbytes + total > pool.max_w_mbytes_per_sec:
                return False, f"Invalid LVol max_w_mbytes: {max_w_mbytes} " \
                              f"Pool Max W MBytes has reached {total} of {pool.max_w_mbytes_per_sec}"

    return True, ""


def _get_next_3_nodes(cluster_id, lvol_size=0):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    online_nodes = []
    node_stats = {}
    for node in snodes:
        if node.is_secondary_node:  # pass
            continue

        if node.status == node.STATUS_ONLINE:

            lvol_count = len(db_controller.get_lvols_by_node_id(node.get_id()))
            if lvol_count >= node.max_lvol:
                continue

            # Validate Eligible nodes for adding lvol
            # snode_api = SNodeClient(node.api_endpoint)
            # result, _ = snode_api.info()
            # memory_free = result["memory_details"]["free"]
            # huge_free = result["memory_details"]["huge_free"]
            # total_node_capacity = db_controller.get_snode_size(node.get_id())
            # error = utils.validate_add_lvol_or_snap_on_node(memory_free, huge_free, node.max_lvol, lvol_size,  total_node_capacity, len(node.lvols))
            # if error:
            #     logger.warning(error)
            #     continue
            #
            online_nodes.append(node)
            # node_stat_list = db_controller.get_node_stats(node, limit=1000)
            # combined_record = utils.sum_records(node_stat_list)
            node_st = {
                "lvol": lvol_count+1,
                # "cpu": 1 + (node.cpu * node.cpu_hz),
                # "r_io": combined_record.read_io_ps,
                # "w_io": combined_record.write_io_ps,
                # "r_b": combined_record.read_bytes_ps,
                # "w_b": combined_record.write_bytes_ps
            }

            node_stats[node.get_id()] = node_st

    if len(online_nodes) <= 1:
        return online_nodes
    cluster_stats = utils.dict_agg([node_stats[k] for k in node_stats])

    nodes_weight = utils.get_weights(node_stats, cluster_stats)

    node_start_end = {}
    n_start = 0
    for node_id in nodes_weight:
        node_start_end[node_id] = {
            "weight": nodes_weight[node_id]['total'],
            "start": n_start,
            "end": n_start + nodes_weight[node_id]['total'],
        }
        n_start = node_start_end[node_id]['end']

    for node_id in node_start_end:
        node_start_end[node_id]['%'] = int(node_start_end[node_id]['weight'] * 100 / n_start)

    ############# log
    print("Node stats")
    utils.print_table_dict({**node_stats, "Cluster": cluster_stats})
    print("Node weights")
    utils.print_table_dict({**nodes_weight, "weights": {"lvol": n_start, "total": n_start}})
    print("Node selection range")
    utils.print_table_dict(node_start_end)
    #############

    selected_node_ids = []
    while len(selected_node_ids) < min(len(node_stats), 3):
        r_index = random.randint(0, n_start)
        print(f"Random is {r_index}/{n_start}")
        for node_id in node_start_end:
            if node_start_end[node_id]['start'] <= r_index <= node_start_end[node_id]['end']:
                if node_id not in selected_node_ids:
                    selected_node_ids.append(node_id)

                    node_start_end = {}
                    n_start = 0
                    for node in nodes_weight:
                        if node in selected_node_ids:
                            continue
                        node_start_end[node] = {
                            "weight": nodes_weight[node]['total'],
                            "start": n_start,
                            "end": n_start + nodes_weight[node]['total'],
                        }
                        n_start = node_start_end[node]['end']

                    break

    ret = []
    if selected_node_ids:
        for node_id in selected_node_ids:
            node = db_controller.get_storage_node_by_id(node_id)
            print(f"Selected node: {node_id}, {node.hostname}")
            ret.append(node)
        return ret
    else:
        return online_nodes

def is_hex(s: str) -> bool:
    """
    given an input checks if the value is hex encoded or not
    """
    try:
        int(s, 16)
        return True
    except ValueError:
        return False

def validate_aes_xts_keys(key1: str, key2: str) -> Tuple[bool, str]:
    """
    Key Length: each key should be either 128 or 256 bits long.
    since hex values of the keys are expected, the key lengths should be either 32 or 64
    """

    if len(key1) != len(key2):
        return False, "both the keys should be of the same length"

    if len(key1) not in [32, 64] or len(key2) not in [32, 64]:
        return False, "each key should be either 16 or 32 bytes long"

    if not is_hex(key1):
        return False, "please provide hex encoded value for crypto_key1"

    if not is_hex(key2):
        return False, "please provide hex encoded value for crypto_key2"

    return True, ""


def add_lvol_ha(name, size, host_id_or_name, ha_type, pool_id_or_name, use_comp, use_crypto,
                distr_vuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes,
                with_snapshot=False, max_size=0, crypto_key1=None, crypto_key2=None, lvol_priority_class=0,
                uid=None, pvc_name=None, namespace=None):

    db_controller = DBController()
    logger.info(f"Adding LVol: {name}")
    host_node = None
    if host_id_or_name:
        host_node = db_controller.get_storage_node_by_id(host_id_or_name)
        if not host_node:
            host_node = db_controller.get_storage_node_by_hostname(host_id_or_name)
            if not host_node:
                return False, f"Can not find storage node: {host_id_or_name}"

    pool = None
    for p in db_controller.get_pools():
        if pool_id_or_name == p.get_id() or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    cl = db_controller.get_cluster_by_id(pool.cluster_id)
    if cl.status not in [cl.STATUS_ACTIVE, cl.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cl.status}"

    if uid:
        for lvol in db_controller.get_lvols():
            if lvol.get_id() == uid:
                if pvc_name:
                    lvol.pvc_name = pvc_name
                if name:
                    lvol.lvol_name = name
                if namespace:
                    lvol.namespace = namespace
                lvol.write_to_db()
                return uid, None

    if ha_type == "default":
        ha_type = cl.ha_type

    max_rw_iops = max_rw_iops or 0
    max_rw_mbytes = max_rw_mbytes or 0
    max_r_mbytes = max_r_mbytes or 0
    max_w_mbytes = max_w_mbytes or 0

    result, error = validate_add_lvol_func(name, size, None, pool_id_or_name,
                                           max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)

    if error:
        logger.error(error)
        return False, error

    cluster_size_prov = 0
    cluster_size_total = 0
    for lvol in db_controller.get_lvols(cl.get_id()):
        cluster_size_prov += lvol.size

    dev_count = 0
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
    online_nodes = []
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1
                    cluster_size_total += dev.size

    if len(online_nodes) == 0:
        logger.error("No online Storage nodes found")
        return False, "No online Storage nodes found"

    if dev_count == 0:
        logger.error("No NVMe devices found in the cluster")
        return False, "No NVMe devices found in the cluster"
    elif dev_count < 8:
        logger.warning("Number of active cluster devices are less than 8")
        # return False, "Number of active cluster devices are less than 8"

    # if len(online_nodes) < 3 and ha_type == "ha":
    #     logger.error("Storage nodes are less than 3 in ha cluster")
    #     return False, "Storage nodes are less than 3 in ha cluster"

    if host_node and host_node.status != StorageNode.STATUS_ONLINE:
        mgs = f"Storage node is not online. ID: {host_node.get_id()} status: {host_node.status}"
        logger.error(mgs)
        return False, mgs

    cluster_size_prov_util = int(((cluster_size_prov+size) / cluster_size_total) * 100)

    if cl.prov_cap_crit and cl.prov_cap_crit < cluster_size_prov_util:
        msg = f"Cluster provisioned cap critical would be, util: {cluster_size_prov_util}% of cluster util: {cl.prov_cap_crit}"
        logger.error(msg)
        return False, msg

    elif cl.prov_cap_warn and cl.prov_cap_warn < cluster_size_prov_util:
        logger.warning(f"Cluster provisioned cap warning, util: {cluster_size_prov_util}% of cluster util: {cl.prov_cap_warn}")

    if not distr_vuid:
        vuid = utils.get_random_vuid()
    else:
        vuid = distr_vuid

    if max_size > 0:
        if max_size < size:
            return False, f"Max size:{max_size} must be larger than size {size}"
    else:
        records = db_controller.get_cluster_capacity(cl)
        if records:
            max_size = records[0]['size_total']
        else:
            max_size = size * 10

    logger.info(f"Max size: {utils.humanbytes(max_size)}")
    lvol = LVol()
    lvol.lvol_name = name
    lvol.pvc_name = pvc_name or ""
    lvol.namespace = namespace or ""
    lvol.size = int(size)
    lvol.max_size = int(max_size)
    lvol.status = LVol.STATUS_IN_CREATION

    lvol.create_dt = str(datetime.now())
    lvol.ha_type = ha_type
    lvol.bdev_stack = []
    lvol.uuid = uid or str(uuid.uuid4())
    lvol.guid = utils.generate_hex_string(16)
    lvol.vuid = vuid
    lvol.lvol_bdev = f"LVOL_{vuid}"

    lvol.crypto_bdev = ''
    lvol.comp_bdev = ''

    lvol.mode = 'read-write'
    lvol.lvol_type = 'lvol'
    lvol.nqn = cl.nqn + ":lvol:" + lvol.uuid
    lvol.lvol_priority_class = lvol_priority_class

    nodes = []
    if host_node:
        nodes.insert(0, host_node)
    else:
        nodes = _get_next_3_nodes(cl.get_id(), lvol.size)
        if not nodes:
            return False, f"No nodes found with enough resources to create the LVol"
        host_node = nodes[0]

    lvol.hostname = host_node.hostname
    lvol.node_id = host_node.get_id()
    lvol.lvs_name = host_node.lvstore
    lvol.subsys_port = host_node.lvol_subsys_port
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.base_bdev = lvol.top_bdev

    lvol_count = len(db_controller.get_lvols_by_node_id(host_node.get_id()))
    if lvol_count > host_node.max_lvol:
        error = f"Too many lvols on node: {host_node.get_id()}, max lvols reached: {lvol_count}"
        logger.error(error)
        return False, error

    lvol_dict = {
        "type": "bdev_lvol",
        "name": lvol.lvol_bdev,
        "params": {
            "name": lvol.lvol_bdev,
            "size_in_mib": utils.convert_size(lvol.size, 'MiB'),
            "lvs_name": lvol.lvs_name,
            "lvol_priority_class": 0
        }
    }

    if cl.enable_qos and lvol.lvol_priority_class > 0:
        lvol_dict["params"]["lvol_priority_class"] = lvol.lvol_priority_class

    lvol.bdev_stack = [lvol_dict]

    if use_crypto:
        if crypto_key1 == None or crypto_key2 == None:
            return False, "encryption keys for lvol not provided"
        else:
            success, err = validate_aes_xts_keys(crypto_key1, crypto_key2)
            if not success:
                return False, err

        lvol.crypto_bdev = f"crypto_{lvol.lvol_bdev}"
        lvol.bdev_stack.append({
            "type": "crypto",
            "name": lvol.crypto_bdev,
            "params": {
                "name": lvol.crypto_bdev,
                "base_name": lvol.top_bdev,
                "key1": crypto_key1,
                "key2": crypto_key2,
            }
        })
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev
        lvol.crypto_key1 = crypto_key1
        lvol.crypto_key2 = crypto_key2

    lvol.write_to_db(db_controller.kv_store)

    if ha_type == "single":
        if host_node.status == StorageNode.STATUS_ONLINE:
            lvol_bdev, error = add_lvol_on_node(lvol, host_node)
            if error:
                lvol.remove(db_controller.kv_store)
                return False, error

            lvol.nodes = [host_node.get_id()]
            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']
        else:
            msg = f"Host node in not online: {host_node.get_id()}"
            logger.error(msg)
            lvol.remove(db_controller.kv_store)
            return False, msg

    if ha_type == "ha":
        lvol.nodes = [host_node.get_id(), host_node.secondary_node_id]
        primary_node = None
        secondary_node = None
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = f"Secondary node is in down status, can not create lvol"
                    logger.error(msg)
                    lvol.remove(db_controller.kv_store)
                    return False, msg
                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_node = sec_node

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    secondary_node = host_node
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_node = sec_node

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # primary is not online but secondary is, create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            lvol.remove(db_controller.kv_store)
            return False, msg


        if primary_node:
            lvol_bdev, error = add_lvol_on_node(lvol, primary_node)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']


        if secondary_node:
            secondary_node = db_controller.get_storage_node_by_id(secondary_node.get_id())
            if secondary_node.status == StorageNode.STATUS_ONLINE:
                lvol_bdev, error = add_lvol_on_node(lvol, secondary_node, is_primary=False)
                if error:
                    logger.error(error)
                    # remove lvol from primary
                    ret = delete_lvol_from_node(lvol.get_id(), primary_node.get_id())
                    if not ret:
                        logger.error("")
                    lvol.remove(db_controller.kv_store)
                    return False, error

    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db_controller.kv_store)
    lvol_events.lvol_create(lvol)

    # set QOS
    if max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes:
        set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)
    return lvol.uuid, None


def _create_bdev_stack(lvol, snode, is_primary=True):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    created_bdevs = []
    for bdev in lvol.bdev_stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']
        ret = None

        if type == "bmap_init":
            ret = rpc_client.ultra21_lvol_bmap_init(**params)

        elif type == "ultra_lvol":
            ret = rpc_client.ultra21_lvol_mount_lvol(**params)

        elif type == "crypto":
            ret = _create_crypto_lvol(rpc_client, **params)

        elif type == "bdev_lvstore":
            ret = rpc_client.create_lvstore(**params)

        elif type == "bdev_lvol":
            if is_primary:
                ret = rpc_client.create_lvol(**params)
            else:
                ret = rpc_client.bdev_lvol_register(
                    lvol.lvol_bdev, lvol.lvs_name, lvol.lvol_uuid, lvol.blobid, lvol.lvol_priority_class)

        elif type == "bdev_lvol_clone":
            if is_primary:
                ret = rpc_client.lvol_clone(**params)
            else:
                ret = rpc_client.bdev_lvol_clone_register(
                    lvol.lvol_bdev, lvol.snapshot_name, lvol.lvol_uuid, lvol.blobid)

        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if ret:
            bdev['status'] = "created"
            created_bdevs.append(bdev)
        else:
            if created_bdevs:
                # rollback
                _remove_bdev_stack(created_bdevs[::-1], rpc_client)
            return False, f"Failed to create BDev: {name}"

    return True, None


def add_lvol_on_node(lvol, snode, is_primary=True):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    db_controller = DBController()

    ret, msg = _create_bdev_stack(lvol, snode, is_primary=is_primary)
    if not ret:
        return False, msg

    if is_primary:
        min_cntlid = 1
    else:
        min_cntlid =  1000
    logger.info("creating subsystem %s", lvol.nqn)
    ret = rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid)
    logger.debug(ret)

    # add listeners
    logger.info("adding listeners")
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(lvol.nqn, tr_type, iface.ip4_address, lvol.subsys_port)
            if not ret:
                return False, f"Failed to create listener for {lvol.get_id()}"
            is_optimized = False
            if lvol.node_id == snode.get_id():
                is_optimized = True
            logger.info(f"Setting ANA state: {is_optimized}")
            ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                lvol.nqn, iface.ip4_address, lvol.subsys_port, is_optimized)
            if not ret:
                return False, f"Failed to set ANA state for {lvol.get_id()}"

    logger.info("Add BDev to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
    if not ret:
        return False, "Failed to add bdev to subsystem"

    spdk_mem_info_after = rpc_client.ultra21_util_get_malloc_stats()
    logger.debug("ultra21_util_get_malloc_stats:")
    logger.debug(spdk_mem_info_after)

    ret = rpc_client.get_bdevs(f"{lvol.lvs_name}/{lvol.lvol_bdev}")
    if ret:
        lvol_bdev = ret[0]
        return lvol_bdev, None
    else:
        return False, "Failed to get lvol bdev"

def is_node_leader(snode, lvs_name):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
    if ret and len(ret) > 0 and "lvs leadership" in ret[0]:
        is_leader = ret[0]["lvs leadership"]
        return is_leader
    return False

def recreate_lvol_on_node(lvol, snode, ha_inode_self=0, ana_state=None):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    base=f"{lvol.lvs_name}/{lvol.lvol_bdev}"

    if "crypto" in lvol.lvol_type:
        ret = _create_crypto_lvol(
            rpc_client, lvol.crypto_bdev, base, lvol.crypto_key1, lvol.crypto_key2)
        if not ret:
            msg=f"Failed to create crypto lvol on node {snode.get_id()}"
            logger.error(msg)
            return False, msg

    min_cntlid = 1 + 1000 * ha_inode_self
    logger.info("creating subsystem %s", lvol.nqn)
    rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid)

    # if namespace_found is False:
    logger.info("Add BDev to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
    # if not ret:
    #     return False, "Failed to add bdev to subsystem"

    # add listeners
    logger.info("adding listeners")
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            if not ana_state:
                ana_state = "non_optimized"
                if lvol.node_id == snode.get_id():
                    ana_state = "optimized"
            logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
            logger.info(f"Setting ANA state: {ana_state}")
            ret = rpc_client.listeners_create(lvol.nqn, tr_type, iface.ip4_address, lvol.subsys_port, ana_state)

    return True, None


def recreate_lvol(lvol_id):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    if lvol.ha_type == 'single':
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        is_created, error = recreate_lvol_on_node(lvol, snode)
        if error:
            logger.error(error)
            return False

    elif lvol.ha_type == "ha":
        for index, node_id in enumerate(lvol.nodes):
            sn = db_controller.get_storage_node_by_id(node_id)
            is_created, error = recreate_lvol_on_node(lvol, sn, index)
            if error:
                logger.error(error)
                return False

    return lvol


def _remove_bdev_stack(bdev_stack, rpc_client):
    for bdev in bdev_stack:
        # if 'status' in bdev and bdev['status'] == 'deleted':
        #     continue

        type = bdev['type']
        name = bdev['name']
        ret = None
        if type == "bdev_distr":
            ret = rpc_client.bdev_distrib_delete(name)
        elif type == "bmap_init":
            pass
        elif type == "ultra_lvol":
            ret = rpc_client.ultra21_lvol_dismount(name)
        elif type == "crypto":
            ret = rpc_client.lvol_crypto_delete(name)
            if ret:
                ret = rpc_client.lvol_crypto_key_delete(f'key_{name}')

        elif type == "bdev_lvstore":
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_lvol":
            name = bdev['params']["lvs_name"]+"/"+bdev['params']["name"]
            ret = rpc_client.delete_lvol(name)
        elif type == "bdev_lvol_clone":
            ret = rpc_client.delete_lvol(name)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
    return True


def delete_lvol_from_node(lvol_id, node_id, clear_data=True):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    snode = db_controller.get_storage_node_by_id(node_id)
    if not lvol or not snode:
        return True

    logger.info(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=5, retry=2)

    # 1- remove subsystem
    if rpc_client.subsystem_list(lvol.nqn):
        logger.info(f"Removing subsystem")
        rpc_client.subsystem_delete(lvol.nqn)

    # 2- remove bdevs
    logger.info(f"Removing bdev stack")
    ret = _remove_bdev_stack(lvol.bdev_stack[::-1], rpc_client)
    if not ret:
        return False

    lvol.deletion_status = 'bdevs_deleted'
    lvol.write_to_db(db_controller.kv_store)
    return True


def delete_lvol(id_or_name, force_delete=False):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(id_or_name)
    if not lvol:
        lvol = db_controller.get_lvol_by_name(id_or_name)
        if not lvol:
            logger.error(f"lvol not found: {id_or_name}")
            return False

    if lvol.status == LVol.STATUS_IN_DELETION:
        logger.info(f"lvol:{lvol.get_id()} status is in deletion")
        if not force_delete:
            return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.debug(lvol)
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    if not snode:
        logger.error(f"lvol node id not found: {lvol.node_id}")
        if not force_delete:
            return False
        lvol.remove(db_controller.kv_store)

        # if lvol is clone and snapshot is deleted, then delete snapshot
        if lvol.cloned_from_snap:
            snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            if snap and snap.deleted is True:
                lvols_count = 0
                for lvol in db_controller.get_lvols():  # pass
                    if lvol.cloned_from_snap == snap.get_id():
                        lvols_count += 1
                if lvols_count == 0:
                    snapshot_controller.delete(snap.get_id())

        logger.info("Done")
        return True

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # disconnect from caching nodes:
    cnodes = db_controller.get_caching_nodes()
    for cnode in cnodes:
        for lv in cnode.lvols:
            if lv.lvol_id == lvol.get_id():
                caching_node_controller.disconnect(cnode.get_id(), lvol.get_id())

    if lvol.ha_type == 'single':
        if snode.status  != StorageNode.STATUS_ONLINE:
            logger.error(f"Node status is not online, node: {snode.get_id()}, status: {snode.status}")
            if not force_delete:
                return False

        ret = delete_lvol_from_node(lvol.get_id(), lvol.node_id)
        if not ret:
            return False


    elif lvol.ha_type == "ha":

        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        sec_rpc_client = RPCClient(
            sec_node.mgmt_ip,
            sec_node.rpc_port,
            sec_node.rpc_username,
            sec_node.rpc_password, timeout=5, retry=1)

        primary_node = None
        secondary_node = None
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = f"Secondary node is in down status, can not delete lvol"
                    logger.error(msg)
                    return False, msg
                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_node = sec_node
                else:
                    secondary_node = None

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    secondary_node = host_node
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_node = sec_node

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # primary is not online but secondary is, create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            return False, msg

        if primary_node:

            ret = delete_lvol_from_node(lvol.get_id(), primary_node.get_id())
            if not ret:
                logger.error(f"Failed to delete lvol from node: {primary_node.get_id()}")
                if not force_delete:
                    return False

        if secondary_node:
            secondary_node = db_controller.get_storage_node_by_id(secondary_node.get_id())
            if secondary_node.status == StorageNode.STATUS_ONLINE:

                ret = delete_lvol_from_node(lvol.get_id(), secondary_node.get_id())
                if not ret:
                    logger.error(f"Failed to delete lvol from node: {secondary_node.get_id()}")
                    if not force_delete:
                        return False

    lvol = db_controller.get_lvol_by_id(lvol.get_id())
    # set status
    old_status = lvol.status
    lvol.status = LVol.STATUS_IN_DELETION
    lvol.write_to_db()
    lvol_events.lvol_status_change(lvol, lvol.status, old_status)

    # if lvol is clone and snapshot is deleted, then delete snapshot
    if lvol.cloned_from_snap:
        snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        if snap and snap.deleted is True:
            lvols_count = 0
            for lvol in db_controller.get_lvols():  # pass
                if lvol.cloned_from_snap == snap.get_id():
                    lvols_count += 1
            if lvols_count == 0:
                snapshot_controller.delete(snap.get_id())

    logger.info("Done")
    return True


def set_lvol(uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, name=None):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        logger.error(f"lvol not found: {uuid}")
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    if name:
        lvol.lvol_name = name

    snode = db_controller.get_storage_node_by_hostname(lvol.hostname)
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    rw_ios_per_sec = -1
    if max_rw_iops is not None and max_rw_iops >= 0:
        rw_ios_per_sec = max_rw_iops

    rw_mbytes_per_sec = -1
    if max_rw_mbytes is not None and max_rw_mbytes >= 0:
        rw_mbytes_per_sec = max_rw_mbytes

    r_mbytes_per_sec = -1
    if max_r_mbytes is not None and max_r_mbytes >= 0:
        r_mbytes_per_sec = max_r_mbytes

    w_mbytes_per_sec = -1
    if max_w_mbytes is not None and max_w_mbytes >= 0:
        w_mbytes_per_sec = max_w_mbytes

    ret = rpc_client.bdev_set_qos_limit(lvol.top_bdev, rw_ios_per_sec, rw_mbytes_per_sec, r_mbytes_per_sec,
                                        w_mbytes_per_sec)
    if not ret:
        return "Error setting qos limits"

    lvol.rw_ios_per_sec = rw_ios_per_sec
    lvol.rw_mbytes_per_sec = rw_mbytes_per_sec
    lvol.r_mbytes_per_sec = r_mbytes_per_sec
    lvol.w_mbytes_per_sec = w_mbytes_per_sec
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return True


def list_lvols(is_json, cluster_id, pool_id_or_name, all=False):
    db_controller = DBController()
    lvols = []
    if cluster_id:
        lvols = db_controller.get_lvols(cluster_id)
    elif pool_id_or_name:
        pool = db_controller.get_pool_by_id(pool_id_or_name)
        if not pool:
            pool = db_controller.get_pool_by_name(pool_id_or_name)
            if pool:
                for lv in db_controller.get_lvols_by_pool_id(pool.get_id()):
                    lvols.append(lv)
    else:
        lvols = db_controller.get_all_lvols()

    data = []
    for lvol in lvols:
        logger.debug(lvol)
        if lvol.deleted is True and all is False:
            continue
        size_used = 0
        records = db_controller.get_lvol_stats(lvol, 1)
        if records:
            size_used = records[0].size_used

        data.append({
            "Id": lvol.uuid,
            "Name": lvol.lvol_name,
            "Size": utils.humanbytes(lvol.size),
            "Used": f"{utils.humanbytes(size_used)}",
            "Hostname": lvol.hostname,
            "HA": lvol.ha_type,
            "BlobID": lvol.blobid or "",
            "LVolUUID": lvol.lvol_uuid or "",
            # "Priority": lvol.lvol_priority_class,
            "Status": lvol.status,
            "IO Err": lvol.io_error,
            "Health": lvol.health_check,
        })

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def list_lvols_mem(is_json, is_csv):
    db_controller = DBController()
    lvols = db_controller.get_lvols()
    data = []
    for lvol in lvols:
        if lvol.deleted is True:
            continue
        logger.debug(lvol)
        data.append({
            "id": lvol.uuid,
            "size": utils.humanbytes(lvol.size),
            "max_size": utils.humanbytes(lvol.max_size),
            **lvol.mem_diff
        })

    if is_json:
        return json.dumps(data, indent=2)
    elif is_csv:
        print(";".join(data[0].keys()))
        for d in data:
            print(";".join([str(v) for v in d.values()]))
    else:
        return utils.print_table(data)


def get_lvol(lvol_id_or_name, is_json):
    db_controller = DBController()
    lvol = None
    for lv in db_controller.get_lvols():  # pass
        if lv.get_id() == lvol_id_or_name or lv.lvol_name == lvol_id_or_name:
            lvol = lv
            break

    if not lvol:
        logger.error(f"LVol id or name not found: {lvol_id_or_name}")
        return False

    data = lvol.get_clean_dict()

    del data['nvme_dev']

    if is_json:
        return json.dumps(data, indent=2)
    else:
        data2 = [{"key": key, "value": data[key]} for key in data]
        return utils.print_table(data2)


def connect_lvol(uuid):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        logger.error(f"lvol not found: {uuid}")
        return False

    out = []
    nodes_ids = []
    if lvol.ha_type == 'single':
        nodes_ids.append(lvol.node_id)

    elif lvol.ha_type == "ha":
        nodes_ids.extend(lvol.nodes)

    for nodes_id in nodes_ids:
        snode = db_controller.get_storage_node_by_id(nodes_id)
        for nic in snode.data_nics:
            transport = nic.get_transport_type().lower()
            ip = nic.ip4_address
            port = lvol.subsys_port
            out.append({
                "transport": transport,
                "ip": ip,
                "port": port,
                "nqn": lvol.nqn,
                "reconnect-delay": constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
                "ctrl-loss-tmo": constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
                "nr-io-queues": constants.LVOL_NVME_CONNECT_NR_IO_QUEUES,
                "keep-alive-tmo": constants.LVOL_NVME_KEEP_ALIVE_TO,
                "connect": f"sudo nvme connect --reconnect-delay={constants.LVOL_NVME_CONNECT_RECONNECT_DELAY} "
                           f"--ctrl-loss-tmo={constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO} "
                           f"--nr-io-queues={constants.LVOL_NVME_CONNECT_NR_IO_QUEUES} "
                           f"--keep-alive-tmo={constants.LVOL_NVME_KEEP_ALIVE_TO} "
                           f"--transport={transport} --traddr={ip} --trsvcid={port} --nqn={lvol.nqn}",
            })
    return out


def resize_lvol(id, new_size):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(id)
    if not lvol:
        msg = f"LVol not found: {id}"
        logger.error(msg)
        return False, msg

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        msg = f"Pool is disabled {pool.get_id()}"
        logger.error(msg)
        return False, msg

    if lvol.size >= new_size:
        msg = f"New size {utils.humanbytes(new_size)} must be higher than the original size {utils.humanbytes(lvol.size)}"
        logger.error(msg)
        return False, msg

    if lvol.max_size < new_size:
        msg = f"New size {new_size} must be smaller than the max size {lvol.max_size}"
        logger.error(msg)
        return False, msg

    if 0 < pool.lvol_max_size < new_size:
        msg = f"Pool Max LVol size is: {utils.humanbytes(pool.lvol_max_size)}, "\
              f"LVol size: {utils.humanbytes(new_size)} must be below this limit"
        logger.error(msg)
        return False, msg

    if pool.pool_max_size > 0:
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        if total + new_size > pool.pool_max_size:
            msg =f"Invalid LVol size: {utils.humanbytes(new_size)}, Pool max size has reached {utils.humanbytes(total+new_size)} of {utils.humanbytes(pool.pool_max_size)}"
            logger.error(msg)
            return False, msg

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    logger.info(f"Resizing LVol: {lvol.get_id()}")
    logger.info(f"Current size: {utils.humanbytes(lvol.size)}, new size: {utils.humanbytes(new_size)}")

    size_in_mib = utils.convert_size(new_size, 'MiB')

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    if lvol.ha_type == "single":

        ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
        if not ret:
            msg = f"Error resizing lvol on node: {snode.get_id()}"
            logger.error(msg)
            return False, msg

    else:
        primary_node = None
        secondary_node = None
        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = f"Secondary node is in down status, can not resize lvol"
                    logger.error(msg)
                    return False, msg

                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_node = sec_node
                else:
                    secondary_node = None

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    secondary_node = host_node
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_node = sec_node

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node
                secondary_node = None

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # primary is not online but secondary is, create on secondary and set leader if needed,
            secondary_node = None
            primary_node = sec_node

        else:
            # both primary and secondary are not online
            msg = f"Host nodes are not online"
            logger.error(msg)
            return False, msg


        if primary_node:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {primary_node.get_id()}")

            rpc_client = RPCClient(primary_node.mgmt_ip, primary_node.rpc_port, primary_node.rpc_username,
                                       primary_node.rpc_password)

            ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            if not ret:
                msg = f"Error resizing lvol on node: {primary_node.get_id()}"
                logger.error(msg)
                return False, msg

        if secondary_node:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {secondary_node.get_id()}")
            secondary_node = db_controller.get_storage_node_by_id(secondary_node.get_id())
            if secondary_node.status == StorageNode.STATUS_ONLINE:

                sec_rpc_client = RPCClient(secondary_node.mgmt_ip, secondary_node.rpc_port, secondary_node.rpc_username,
                                           secondary_node.rpc_password)

                ret = sec_rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
                if not ret:
                    msg = f"Error resizing lvol on node: {sec_node.get_id()}"
                    logger.error(msg)
                    return False, msg

    lvol = db_controller.get_lvol_by_id(id)
    lvol.size = new_size
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")

    return True, None



def set_read_only(id):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(id)
    if not lvol:
        logger.error(f"LVol not found: {id}")
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.info(f"Setting LVol: {lvol.get_id()} read only")

    snode = db_controller.get_storage_node_by_hostname(lvol.hostname)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    ret = rpc_client.lvol_read_only(lvol.lvol_bdev)
    if not ret:
        return "Error"

    old_status = lvol.mode
    lvol.mode = 'read-only'
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    lvol_events.lvol_status_change(lvol, lvol.mode, old_status)

    return True


def create_snapshot(lvol_id, snapshot_name):
    return snapshot_controller.add(lvol_id, snapshot_name)


def get_capacity(lvol_uuid, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_uuid)
    if not lvol:
        logger.error(f"LVol not found: {lvol_uuid}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records_list = db_controller.get_lvol_stats(lvol, limit=records_number)
    if not records_list:
        return False
    cap_stats_keys = [
        "date",
        "size_total",
        "size_used",
        "size_free",
        "size_util",
    ]
    new_records = utils.process_records(records_list, records_count, keys=cap_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Total": utils.humanbytes(record['size_total']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
        })
    return out


def get_io_stats(lvol_uuid, history, records_count=20, parse_sizes=True, with_sizes=False):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_uuid)
    if not lvol:
        logger.error(f"LVol not found: {lvol_uuid}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records_list = db_controller.get_lvol_stats(lvol, limit=records_number)
    if not records_list:
        return False
    io_stats_keys = [
        "date",
        "read_bytes",
        "read_bytes_ps",
        "read_io_ps",
        "read_latency_ps",
        "write_bytes",
        "write_bytes_ps",
        "write_io_ps",
        "write_latency_ps",
        "connected_clients",
    ]
    if with_sizes:
        io_stats_keys.extend(
            [
                "size_total",
                "size_prov",
                "size_used",
                "size_free",
                "size_util",
                "size_prov_util",
                "read_latency_ticks",
                "record_duration",
                "record_end_time",
                "record_start_time",
                "unmap_bytes",
                "unmap_bytes_ps",
                "unmap_io",
                "unmap_io_ps",
                "unmap_latency_ps",
                "unmap_latency_ticks",
                "write_bytes_ps",
                "write_latency_ticks",
            ]
        )
    # combine records
    new_records = utils.process_records(records_list, records_count, keys=io_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read bytes": utils.humanbytes(record["read_bytes"]),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record['read_io_ps'],
            "Read lat": record['read_latency_ps'],
            "Write bytes": utils.humanbytes(record["write_bytes"]),
            "Write speed": utils.humanbytes(record['write_bytes_ps']),
            "Write IOPS": record['write_io_ps'],
            "Write lat": record['write_latency_ps'],
            "Con": record['connected_clients'],
        })
    return out


def migrate(lvol_id, node_id):

    # lvol = db_controller.get_lvol_by_id(lvol_id)
    # if not lvol:
    #     logger.error(f"lvol not found: {lvol_id}")
    #     return False
    #
    # old_node_id = lvol.node_id
    # old_node = db_controller.get_storage_node_by_id(old_node_id)
    # nodes = _get_next_3_nodes(old_node.cluster_id)
    # if not nodes:
    #     logger.error(f"No nodes found with enough resources to create the LVol")
    #     return False
    #
    # if node_id:
    #     nodes[0] = db_controller.get_storage_node_by_id(node_id)
    #
    # host_node = nodes[0]
    # lvol.hostname = host_node.hostname
    # lvol.node_id = host_node.get_id()
    #
    # if lvol.ha_type == 'single':
    #     ret = add_lvol_on_node(lvol, host_node)
    #     if not ret:
    #         return ret
    #
    # elif lvol.ha_type == "ha":
    #     three_nodes = nodes[:3]
    #     nodes_ids = []
    #     nodes_ips = []
    #     for node in three_nodes:
    #         nodes_ids.append(node.get_id())
    #         port = 10000 + int(random.random() * 60000)
    #         nodes_ips.append(f"{node.mgmt_ip}:{port}")
    #
    #     ha_address = ",".join(nodes_ips)
    #     for index, node in enumerate(three_nodes):
    #         ret = add_lvol_on_node(lvol, node, ha_address)
    #         if not ret:
    #             return ret
    #     lvol.nodes = nodes_ids
    #
    # # host_node.lvols.append(lvol.uuid)
    # # host_node.write_to_db(db_controller.kv_store)
    # lvol.write_to_db(db_controller.kv_store)
    #
    # lvol_events.lvol_migrate(lvol, old_node_id, lvol.node_id)

    return True


def move(lvol_id, node_id, force=False):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    target_node = db_controller.get_storage_node_by_id(node_id)
    if not target_node:
        logger.error(f"Node not found: {target_node}")
        return False

    if lvol.node_id == target_node.get_id():
        return True

    if target_node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online!: {target_node}, status: {target_node.status}")
        return False

    src_node = db_controller.get_storage_node_by_id(lvol.node_id)

    if src_node.status == StorageNode.STATUS_ONLINE:
        if not force:
            logger.error(f"Node is online!: {src_node.get_id()}, use --force to force move")
            return False

    if migrate(lvol_id, node_id):
        if src_node.status == StorageNode.STATUS_ONLINE:
            # delete lvol
            if lvol.ha_type == 'single':
                delete_lvol_from_node(lvol_id, lvol.node_id, clear_data=False)
            elif lvol.ha_type == "ha":
                for nodes_id in lvol.nodes:
                    delete_lvol_from_node(lvol_id, nodes_id, clear_data=False)

            # remove from storage node
            # src_node.lvols.remove(lvol_id)
            # src_node.write_to_db(db_controller.kv_store)
        return True
    else:
        logger.error("Failed to migrate lvol")
        return False


def inflate_lvol(lvol_id):

    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False
    if not lvol.cloned_from_snap:
        logger.error(f"LVol: {lvol_id} must be cloned LVol not regular one")
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.info(f"Inflating LVol: {lvol.get_id()}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)
    ret = rpc_client.bdev_lvol_inflate(lvol.top_bdev)
    if ret:
        lvol.cloned_from_snap = ""
        lvol.write_to_db(db_controller.kv_store)
        logger.info("Done")
    else:
        logger.error(f"Failed to inflate LVol: {lvol_id}")
    return ret
