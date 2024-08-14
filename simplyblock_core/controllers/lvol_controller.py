# coding=utf-8
import logging as lg
import json
import string
import random
import sys
import time
import uuid
from typing import Tuple

from simplyblock_core import utils, constants, distr_controller
from simplyblock_core.controllers import snapshot_controller, pool_controller, lvol_events, caching_node_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient

logger = lg.getLogger()
db_controller = DBController()


def _generate_hex_string(length):
    def _generate_string(length):
        return ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(length))

    return _generate_string(length).encode('utf-8').hex()


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
    if not name or name == "":
        return False, "Name can not be empty"

    #  size validation
    if size < 100 * 1024 * 1024:
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
        if pool_id_or_name == p.id or pool_id_or_name == p.pool_name:
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
                          f"Pool max size has reached {utils.humanbytes(total)} of {utils.humanbytes(pool.pool_max_size)}"

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


def get_jm_names(snode):
    return [snode.jm_device.jm_bdev] if snode.jm_device else []


# Deprecated
def add_lvol(name, size, host_id_or_name, pool_id_or_name, use_comp, use_crypto,
             distr_vuid, distr_ndcs, distr_npcs,
             max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes,
             distr_bs=None, distr_chunk_bs=None):
    logger.info("adding LVol")
    snode = db_controller.get_storage_node_by_id(host_id_or_name)
    if not snode:
        snode = db_controller.get_storage_node_by_hostname(host_id_or_name)
        if not snode:
            return False, f"Can not find storage node: {host_id_or_name}"

    pool = None
    for p in db_controller.get_pools():
        if pool_id_or_name == p.id or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    cl = db_controller.get_cluster_by_id(snode.cluster_id)
    if cl.status not in [cl.STATUS_ACTIVE, cl.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cl.status}"

    max_rw_iops = max_rw_iops or 0
    max_rw_mbytes = max_rw_mbytes or 0
    max_r_mbytes = max_r_mbytes or 0
    max_w_mbytes = max_w_mbytes or 0

    result, error = validate_add_lvol_func(name, size, host_id_or_name, pool_id_or_name,
                                           max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)

    if error:
        logger.error(error)
        return False, error

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    if not snode.nvme_devices:
        logger.error("Storage node has no nvme devices")
        return False, "Storage node has no nvme devices"

    dev_count = 0
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status == node.STATUS_ONLINE:
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1

    if dev_count < 8:
        logger.error("Number of active cluster devices are less than 8")
        return False, "Number of active cluster devices are less than 8"

    if snode.status != snode.STATUS_ONLINE:
        logger.error("Storage node in not Online")
        return False, "Storage node in not Online"

    lvol = LVol()
    lvol.lvol_name = name
    lvol.size = size

    bdev_stack = []

    if distr_vuid == 0:
        vuid = 1 + int(random.random() * 10000)
    else:
        vuid = distr_vuid

    num_blocks = int(size / distr_bs)
    jm_names = get_jm_names(snode)

    if distr_ndcs == 0 and distr_npcs == 0:
        if cl.ha_type == "single":
            distr_ndcs = 4
            distr_npcs = 1
        else:
            node_count = 0
            for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
                if node.status == node.STATUS_ONLINE:
                    node_count += 1
            if node_count == 3:
                distr_ndcs = 1
            elif node_count in [4, 5]:
                distr_ndcs = 2
            elif node_count >= 6:
                distr_ndcs = 4
            distr_npcs = 1

    # name, vuid, ndcs, npcs, num_blocks, block_size, alloc_names
    ret = rpc_client.bdev_distrib_create(f"distr_{name}", vuid, distr_ndcs, distr_npcs, num_blocks, distr_bs, jm_names,
                                         distr_chunk_bs, distrib_cpu_mask=snode.distrib_cpu_mask)
    bdev_stack.append({"type": "distr", "name": f"distr_{name}"})
    if not ret:
        logger.error("failed to create Distr bdev")
        return False

    time.sleep(3)
    ret = rpc_client.create_lvstore(f"LVS_{vuid}", f"distr_{name}")
    bdev_stack.append({"type": "lvs", "name": f"LVS_{vuid}"})
    if not ret:
        logger.error("failed to create lvs")
        # return False
    lvol.base_bdev = f"distr_{name}"

    ret = rpc_client.create_lvol(name, size, f"LVS_{vuid}")
    bdev_stack.append({"type": "lvol", "name": f"LVS_{vuid}/{name}"})

    if not ret:
        logger.error("failed to create LVol on the storage node")
        return False, "failed to create LVol on the storage node"
    lvol_id = ret

    lvol_type = 'lvol'
    lvol_bdev = f"LVS_{vuid}/{name}"
    crypto_bdev = ''
    comp_bdev = ''
    top_bdev = lvol_bdev
    if use_crypto is True:
        crypto_bdev = _create_crypto_lvol(rpc_client, name, lvol_bdev, "", "")
        bdev_stack.append({"type": "crypto", "name": crypto_bdev})
        if not crypto_bdev:
            return False, "Error creating crypto bdev"
        lvol_type += ',crypto'
        top_bdev = crypto_bdev

    if use_comp is True:
        n = crypto_bdev if crypto_bdev else lvol_bdev
        comp_bdev = _create_compress_lvol(rpc_client, n)
        bdev_stack.append({"type": "comp", "name": comp_bdev})
        if not comp_bdev:
            return False, "Error creating comp bdev"
        lvol_type += ',compress'
        top_bdev = comp_bdev

    subsystem_nqn = snode.subsystem + ":lvol:" + lvol_id
    logger.info("creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', lvol_id)
    logger.debug(ret)

    # add listeners
    logger.info("adding listeners")
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            ret = rpc_client.transport_list()
            found = False
            if ret:
                for ty in ret:
                    if ty['trtype'] == tr_type:
                        found = True
            if found is False:
                ret = rpc_client.transport_create(tr_type)
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")

    logger.info(f"add lvol {name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, top_bdev)

    lvol.bdev_stack = bdev_stack
    lvol.uuid = lvol_id
    lvol.vuid = vuid
    lvol.lvol_bdev = lvol_bdev
    lvol.crypto_bdev = crypto_bdev
    lvol.comp_bdev = comp_bdev
    lvol.hostname = snode.hostname
    lvol.node_id = snode.get_id()
    lvol.mode = 'read-write'
    lvol.lvol_type = lvol_type
    lvol.nqn = subsystem_nqn
    lvol.ndcs = distr_ndcs
    lvol.npcs = distr_npcs
    lvol.distr_bs = distr_bs
    lvol.distr_chunk_bs = distr_chunk_bs

    lvol.pool_uuid = pool.id
    pool.lvols.append(lvol_id)
    pool.write_to_db(db_controller.kv_store)

    lvol.write_to_db(db_controller.kv_store)

    snode.lvols.append(lvol_id)
    snode.write_to_db(db_controller.kv_store)

    # set QOS
    if max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes:
        set_lvol(lvol_id, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)
    return lvol_id, None


def _get_next_3_nodes(cluster_id, lvol_size=0):
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    online_nodes = []
    node_stats = {}
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            # Validate Eligible nodes for adding lvol
            snode_api = SNodeClient(node.api_endpoint)
            result, _ = snode_api.info()
            memory_free = result["memory_details"]["free"]
            huge_free = result["memory_details"]["huge_free"]
            total_node_capacity = db_controller.get_snode_size(node.get_id())
            error = utils.validate_add_lvol_or_snap_on_node(memory_free, huge_free, node.max_lvol, lvol_size,  total_node_capacity, len(node.lvols))
            if error:
                logger.warning(error)
                continue

            online_nodes.append(node)
            node_stat_list = db_controller.get_node_stats(node, limit=1000)
            combined_record = utils.sum_records(node_stat_list)
            node_st = {
                "lvol": len(node.lvols),
                "cpu": 1 + (node.cpu * node.cpu_hz),
                "r_io": combined_record.read_io_ps,
                "w_io": combined_record.write_io_ps,
                "r_b": combined_record.read_bytes_ps,
                "w_b": combined_record.write_bytes_ps}

            node_stats[node.get_id()] = node_st

    if len(online_nodes) < 3:
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
    utils.print_table_dict({**nodes_weight, "weights": {**constants.weights, "total": sum(constants.weights.values())}})
    print("Node selection range")
    utils.print_table_dict(node_start_end)
    #############

    selected_node_ids = []
    while len(selected_node_ids) < 3:
        r_index = random.randint(0, n_start)
        print(f"Random is {r_index}/{n_start}")
        for node_id in node_start_end:
            if node_start_end[node_id]['start'] <= r_index <= node_start_end[node_id]['end']:
                if node_id not in selected_node_ids:
                    selected_node_ids.append(node_id)
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
                distr_vuid, distr_ndcs, distr_npcs,
                max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes,
                distr_bs=None, distr_chunk_bs=None, with_snapshot=False, max_size=0, crypto_key1=None, crypto_key2=None,
                connection_type="nvmf"):

    logger.info(f"Adding LVol: {name}")
    host_node = None
    if host_id_or_name:
        host_node = db_controller.get_caching_node_by_id(host_id_or_name)
        if not host_node:
            host_node = db_controller.get_caching_node_by_hostname(host_id_or_name)
            if not host_node:
                return False, f"Can not find node: {host_id_or_name}"

    if not host_node:
        host_node = db_controller.get_caching_nodes()[0]

    cluster_obj = db_controller.get_clusters()[0]
    logger.info(f"Max size: {utils.humanbytes(max_size)}")
    lvol = LVol()
    lvol.lvol_name = name
    lvol.size = size
    lvol.max_size = max_size
    lvol.status = LVol.STATUS_ONLINE
    lvol.ha_type = "single"
    lvol.bdev_stack = []
    lvol.uuid = str(uuid.uuid4())
    lvol.guid = _generate_hex_string(16)
    lvol.lvol_bdev = f"lvs_1/{name}"
    lvol.lvs_name = f"lvs_1"

    lvol.connection_type = connection_type or "nvmf"

    lvol.crypto_bdev = ''
    lvol.comp_bdev = ''

    lvol.mode = 'read-write'
    lvol.lvol_type = 'lvol'
    lvol.nqn = cluster_obj.nqn + ":lvol:" + lvol.uuid

    lvol.base_bdev = lvol.lvol_bdev
    lvol.top_bdev = lvol.base_bdev

    lvol.bdev_stack.append({
        "type": "bdev_lvol",
        "name": lvol.lvol_bdev,
        "params": {
            "name": lvol.lvol_name,
            "size_in_mib": int(lvol.size / (1000 * 1000)),
            "lvs_name": lvol.lvs_name
        }
    })

    if use_crypto:
        if crypto_key1 == None or crypto_key2 == None:
            return False, "encryption keys for lvol not provided"
        else:
            success, err = validate_aes_xts_keys(crypto_key1, crypto_key2)
            if not success:
                return False, err

        lvol.crypto_bdev = f"crypto_{lvol.lvol_name}"
        lvol.bdev_stack.append({
            "type": "crypto",
            "name": lvol.crypto_bdev,
            "params": {
                "name": lvol.crypto_bdev,
                "base_name": lvol.base_bdev,
                "key1": crypto_key1,
                "key2": crypto_key2,
            }
        })
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev

    if use_comp is True:
        base_bdev = lvol.lvol_bdev
        if lvol.crypto_bdev:
            base_bdev = lvol.crypto_bdev
        lvol.comp_bdev = f"comp_{lvol.lvol_name}"
        lvol.bdev_stack.append({
            "type": "comp",
            "name": lvol.comp_bdev,
            "params": {
                "base_bdev_name": base_bdev
            }
        })
        lvol.lvol_type += ',compress'
        lvol.top_bdev = lvol.comp_bdev


    lvol.hostname = host_node.hostname
    lvol.node_id = host_node.get_id()

    # if ha_type == 'single':
    ret, error = add_lvol_on_node(lvol, host_node)
    if error:
        return ret, error

    host_node.lvols.append(lvol.uuid)
    host_node.write_to_db(db_controller.kv_store)
    lvol.write_to_db(db_controller.kv_store)
    # lvol_events.lvol_create(lvol)

    # set QOS
    if max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes:
        set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)
    return lvol.uuid, None


def _create_bdev_stack(lvol, snode):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    created_bdevs = []
    for bdev in lvol.bdev_stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']
        ret = None

        if type == "crypto":
            ret = _create_crypto_lvol(rpc_client, **params)

        elif type == "bdev_lvol":
            ret = rpc_client.create_lvol(**params)

        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if ret:
            bdev['status'] = "created"
            created_bdevs.append(bdev)
        else:
            if created_bdevs:
                # rollback
                _remove_bdev_stack(created_bdevs, rpc_client)
            return False, f"Failed to create BDev: {name}"

    return True, None


def add_lvol_on_node(lvol, snode, create_bdev_stack=True):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    if create_bdev_stack:
        ret, msg = _create_bdev_stack(lvol, snode)
        if not ret:
            return False, msg

    if lvol.connection_type == "nvmf":
        logger.info("creating subsystem %s", lvol.nqn)
        ret = rpc_client.subsystem_create(lvol.nqn, 'sbcli-cn', lvol.uuid)
        logger.debug(ret)

        # add listeners
        logger.info("adding listeners")
        for iface in snode.data_nics:
            if iface.ip4_address:
                tr_type = iface.get_transport_type()
                ret = rpc_client.transport_list()
                found = False
                if ret:
                    for ty in ret:
                        if ty['trtype'] == tr_type:
                            found = True
                if found is False:
                    ret = rpc_client.transport_create(tr_type)
                logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
                ret = rpc_client.listeners_create(lvol.nqn, tr_type, iface.ip4_address, "4420")
                is_optimized = False
                # if lvol.node_id == snode.get_id():
                #     is_optimized = True
                logger.info(f"Setting ANA state: {is_optimized}")
                ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                    lvol.nqn, iface.ip4_address, "4420", is_optimized)

        logger.info("Add BDev to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
        if not ret:
            return False, "Failed to add bdev to subsystem"

    elif lvol.connection_type == "nbd":
        pass
        # nbd_device = rpc_client.nbd_start_disk(lvol.top_bdev)
        # if not nbd_device:
        #     logger.error(f"Failed to start nbd dev")
        #     return False
        # lvol.nbd_device = nbd_device
    else:
        logger.error(f"Unknown connection_type: {lvol.connection_type}")
        return False


    return True, None


def recreate_lvol(lvol_id, snode):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    if lvol.ha_type == 'single':
        is_created, error = add_lvol_on_node(lvol, snode)
        if error:
            return False

    elif lvol.ha_type == "ha":
        nodes_ips = []
        for node_id in lvol.nodes:
            sn = db_controller.get_storage_node_by_id(node_id)
            port = 10000 + int(random.random() * 60000)
            nodes_ips.append(f"{sn.mgmt_ip}:{port}")

        ha_address = ",".join(nodes_ips)
        for index, node_id in enumerate(lvol.nodes):
            sn = db_controller.get_storage_node_by_id(node_id)
            is_created, error = add_lvol_on_node(lvol, sn, ha_address, index)
            if error:
                return False

    return lvol


def _remove_bdev_stack(bdev_stack, rpc_client):
    for bdev in bdev_stack:
        if 'status' in bdev and bdev['status'] == 'deleted':
            continue

        type = bdev['type']
        name = bdev['name']
        ret = None
        if type == "bdev_distr":
            ret = rpc_client.bdev_distrib_delete(name)
        elif type == "bmap_init":
            pass
        elif type == "ultra_lvol":
            ret = rpc_client.ultra21_lvol_dismount(name)
        elif type == "comp":
            ret = rpc_client.lvol_compress_delete(name)
        elif type == "crypto":
            ret = rpc_client.lvol_crypto_delete(name)
        elif type == "bdev_lvol":
            ret = rpc_client.delete_lvol(name)
        elif type == "bdev_lvol_clone":
            ret = rpc_client.delete_lvol(name)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
        time.sleep(1)


def delete_lvol_from_node(lvol_id, node_id, clear_data=True):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    snode = db_controller.get_storage_node_by_id(node_id)
    logger.info(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    # 1- remove subsystem
    if lvol.connection_type == "nvmf":
        logger.info(f"Removing subsystem")
        ret = rpc_client.subsystem_delete(lvol.nqn)
        if not ret:
            logger.warning(f"Failed to remove subsystem: {lvol.nqn}")
    # elif lvol.connection_type == "nbd":
    #     ret = rpc_client.nbd_stop_disk(lvol.nbd_device)
    #     if not ret:
    #         logger.warning(f"Failed to stop nbd: {lvol.nbd_device}")

    time.sleep(1)

    # 2- remove bdevs
    logger.info(f"Removing bdev stack")
    _remove_bdev_stack(lvol.bdev_stack[::-1], rpc_client)
    lvol.deletion_status = 'bdevs_deleted'
    lvol.write_to_db(db_controller.kv_store)

    # # 3- clear alceml devices
    # if clear_data:
    #     logger.info(f"Clearing Alceml devices")
    #     for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
    #         if node.status == StorageNode.STATUS_ONLINE:
    #             rpc_node = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
    #             for dev in node.nvme_devices:
    #                 if dev.status != NVMeDevice.STATUS_JM:
    #                     ret = rpc_node.alceml_unmap_vuid(dev.alceml_bdev, lvol.vuid)
    #
    #     lvol.deletion_status = 'alceml_unmapped'
    #     lvol.write_to_db(db_controller.kv_store)
    #
    #     # 4- clear JM
    #     jm_device = snode.jm_device
    #     ret = rpc_client.alceml_unmap_vuid(jm_device.alceml_bdev, lvol.vuid)
    #     if not ret:
    #         logger.error(f"Failed to unmap jm alceml {jm_device.alceml_bdev} with vuid {lvol.vuid}")
    #     # ret = rpc_client.bdev_jm_unmap_vuid(jm_device.jm_bdev, lvol.vuid)
    #     # if not ret:
    #     #     logger.error(f"Failed to unmap jm {jm_device.jm_bdev} with vuid {lvol.vuid}")
    #
    #     lvol.deletion_status = 'jm_unmapped'
    #     lvol.write_to_db(db_controller.kv_store)

    return True


def delete_lvol(id_or_name, force_delete=False):
    lvol = db_controller.get_lvol_by_id(id_or_name)
    if not lvol:
        lvol = db_controller.get_lvol_by_name(id_or_name)
        if not lvol:
            logger.error(f"lvol not found: {id_or_name}")
            return False

    # pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    # if pool.status == Pool.STATUS_INACTIVE:
    #     logger.error(f"Pool is disabled")
    #     return False

    logger.debug(lvol)
    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # soft delete LVol if it has snapshots
    snaps = db_controller.get_snapshots()
    for snap in snaps:
        if snap.lvol.get_id() == lvol.get_id():
            logger.warning(f"Soft delete LVol that has snapshots. Snapshot:{snap.get_id()}")
            ret = rpc_client.subsystem_delete(lvol.nqn)
            logger.debug(ret)
            lvol.deleted = True
            lvol.write_to_db(db_controller.kv_store)
            return True

    # set status
    lvol.status = LVol.STATUS_IN_DELETION
    lvol.write_to_db(db_controller.kv_store)

    # # disconnect from caching nodes:
    # cnodes = db_controller.get_caching_nodes()
    # for cnode in cnodes:
    #     for lv in cnode.lvols:
    #         if lv.lvol_id == lvol.get_id():
    #             caching_node_controller.disconnect(cnode.get_id(), lvol.get_id())

    if lvol.ha_type == 'single':
        ret = delete_lvol_from_node(lvol.get_id(), lvol.node_id)
        if not ret:
            return False
    elif lvol.ha_type == "ha":
        for nodes_id in lvol.nodes:
            ret = delete_lvol_from_node(lvol.get_id(), nodes_id)
            if not ret:
                return False

    # remove from storage node
    tmp = []
    for lvol_id in snode.lvols:
        if lvol_id != lvol.get_id():
            tmp.append(lvol_id)

    snode.lvols = tmp
    snode.write_to_db(db_controller.kv_store)

    # # remove from pool
    # if lvol.get_id() in pool.lvols:
    #     pool.lvols.remove(lvol.get_id())
    #     pool.write_to_db(db_controller.kv_store)

    # lvol_events.lvol_delete(lvol)
    lvol.remove(db_controller.kv_store)

    # if lvol is clone and snapshot is deleted, then delete snapshot
    if lvol.cloned_from_snap:
        snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        if snap.deleted is True:
            lvols_count = 0
            for lvol in db_controller.get_lvols():  # pass
                if lvol.cloned_from_snap == snap.get_id():
                    lvols_count += 1
            if lvols_count == 0:
                snapshot_controller.delete(snap.get_id())

    logger.info("Done")
    return True


def set_lvol(uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, name=None):
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


def list_lvols(is_json, cluster_id, pool_id_or_name):
    lvols = []
    if cluster_id:
        lvols = db_controller.get_lvols(cluster_id)
    elif pool_id_or_name:
        pool = db_controller.get_pool_by_id(pool_id_or_name)
        if not pool:
            pool = db_controller.get_pool_by_name(pool_id_or_name)
            if pool:
                for lv_id in pool.lvols:
                    lvols.append(db_controller.get_lvol_by_id(lv_id))
    else:
        lvols = db_controller.get_lvols()

    data = []
    for lvol in lvols:
        if lvol.deleted is True:
            continue
        logger.debug(lvol)
        data.append({
            "Id": lvol.uuid,
            "Name": lvol.lvol_name,
            "Size": utils.humanbytes(lvol.size),
            "Hostname": lvol.hostname,
            "Conn": lvol.connection_type,
            "Type": lvol.lvol_type,
            # "Mod": f"{lvol.ndcs}x{lvol.npcs}",
            "Status": lvol.status,
            "IO Err": lvol.io_error,
            "Health": lvol.health_check,
        })

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def list_lvols_mem(is_json, is_csv):
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
    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        logger.error(f"lvol not found: {uuid}")
        return False

    out = []
    if lvol.connection_type == "nvmf":
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
                port = 4420
                out.append({
                    "transport": transport,
                    "ip": ip,
                    "port": port,
                    "nqn": lvol.nqn,
                    "connect": f"sudo nvme connect --transport={transport} --traddr={ip} --trsvcid={port} --nqn={lvol.nqn}",
                })
    elif lvol.connection_type == "nbd":
        out.append({"connect": lvol.nbd_device})

    return out


def resize_lvol(id, new_size):
    lvol = db_controller.get_lvol_by_id(id)
    if not lvol:
        logger.error(f"LVol not found: {id}")
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    if lvol.size >= new_size:
        logger.error(f"New size {new_size} must be higher than the original size {lvol.size}")
        return False

    if lvol.max_size < new_size:
        logger.error(f"New size {new_size} must be smaller than the max size {lvol.max_size}")
        return False

    logger.info(f"Resizing LVol: {lvol.id}, new size: {lvol.size}")

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    num_blocks = int(new_size / lvol.distr_bs)
    if lvol.snapshot_name:
        ret = rpc_client.resize_lvol(lvol.top_bdev, num_blocks)
        if not ret:
            logger.error("Error resizing lvol")
            return False
    elif lvol.cloned_from_snap:
        ret = rpc_client.resize_clone(lvol.top_bdev, num_blocks)
        if not ret:
            logger.error("Error resizing clone")
            return False
    else:
        logger.error("Can not resize distr")
        return False

    lvol.size = new_size
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return True


def set_read_only(id):
    lvol = db_controller.get_lvol_by_id(id)
    if not lvol:
        logger.error(f"LVol not found: {id}")
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

    logger.info(f"Setting LVol: {lvol.id} read only")

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


def get_capacity(id, history):
    lvol = db_controller.get_lvol_by_id(id)
    if not lvol:
        logger.error(f"lvol not found: {id}")
        return False

    out = [{
        "provisioned": lvol.size,
        "util_percent": 0,
        "util": 0,
    }]

    return utils.print_table(out)


def get_io_stats(lvol_uuid, history, records_count=20, parse_sizes=True):
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
    new_records = utils.process_records(records_list, min(records_count, len(records_list)))

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
        })
    return out


def send_cluster_map(lvol_id):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    logger.info("Sending cluster map")
    return distr_controller.send_cluster_map_to_node(snode)


def get_cluster_map(lvol_id):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    ret = rpc_client.distr_get_cluster_map(lvol.base_bdev)
    if not ret:
        logger.error(f"Failed to get LVol cluster map: {lvol_id}")
        return False
    logger.debug(ret)
    print("*"*100)
    results, is_passed = distr_controller.parse_distr_cluster_map(ret)
    return utils.print_table(results)


def migrate(lvol_id, node_id):

    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    old_node_id = lvol.node_id
    old_node = db_controller.get_storage_node_by_id(old_node_id)
    nodes = _get_next_3_nodes(old_node.cluster_id)
    if not nodes:
        logger.error(f"No nodes found with enough resources to create the LVol")
        return False

    if node_id:
        nodes[0] = db_controller.get_storage_node_by_id(node_id)

    host_node = nodes[0]
    lvol.hostname = host_node.hostname
    lvol.node_id = host_node.get_id()

    if lvol.ha_type == 'single':
        ret = add_lvol_on_node(lvol, host_node)
        if not ret:
            return ret

    elif lvol.ha_type == "ha":
        three_nodes = nodes[:3]
        nodes_ids = []
        nodes_ips = []
        for node in three_nodes:
            nodes_ids.append(node.get_id())
            port = 10000 + int(random.random() * 60000)
            nodes_ips.append(f"{node.mgmt_ip}:{port}")

        ha_address = ",".join(nodes_ips)
        for index, node in enumerate(three_nodes):
            ret = add_lvol_on_node(lvol, node, ha_address, index)
            if not ret:
                return ret
        lvol.nodes = nodes_ids

    host_node.lvols.append(lvol.uuid)
    host_node.write_to_db(db_controller.kv_store)
    lvol.write_to_db(db_controller.kv_store)

    lvol_events.lvol_migrate(lvol, old_node_id, lvol.node_id)

    return True


def move(lvol_id, node_id, force=False):
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
            src_node.lvols.remove(lvol_id)
            src_node.write_to_db(db_controller.kv_store)
        return True
    else:
        logger.error("Failed to migrate lvol")
        return False
