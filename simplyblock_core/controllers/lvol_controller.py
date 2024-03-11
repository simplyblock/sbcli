# coding=utf-8
import logging as lg
import json
import string
import random
import sys
import time
import uuid

from simplyblock_core import utils, constants, distr_controller
from simplyblock_core.controllers import snapshot_controller, pool_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCClient


logger = lg.getLogger()
db_controller = DBController()


def _generate_hex_string(length):
    def _generate_string(length):
        return ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(length))

    return _generate_string(length).encode('utf-8').hex()


def _create_crypto_lvol(rpc_client, name, base_name):
    key_name = f'key_{name}'
    key1 = _generate_hex_string(32)
    key2 = _generate_hex_string(32)
    ret = rpc_client.lvol_crypto_key_create(key_name, key1, key2)
    if not ret:
        logger.warning("failed to create crypto key")
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

    for lvol in db_controller.get_lvols():
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
    return [f"jm_{snode.get_id()}"]


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
    for node in db_controller.get_storage_nodes():
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
            for node in db_controller.get_storage_nodes():
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
                                         distr_chunk_bs)
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
        crypto_bdev = _create_crypto_lvol(rpc_client, name, lvol_bdev)
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


def _get_next_3_nodes():
    snodes = db_controller.get_storage_nodes()
    online_nodes = []
    node_stats = {}
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
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


def add_lvol_ha(name, size, host_id_or_name, ha_type, pool_id_or_name, use_comp, use_crypto,
                distr_vuid, distr_ndcs, distr_npcs,
                max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes,
                distr_bs=None, distr_chunk_bs=None):

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
        if pool_id_or_name == p.id or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    cl = db_controller.get_clusters()[0]
    if cl.status not in [cl.STATUS_ACTIVE, cl.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cl.status}"

    records = db_controller.get_cluster_capacity(cl, 1)
    if records:
        record = records[0]
        size_prov_util = int(((record.size_prov+size) / record.size_total) * 100)

        if cl.prov_cap_crit and cl.prov_cap_crit < size_prov_util:
            msg = f"Cluster provisioned cap critical, util: {size_prov_util}% of cluster util: {cl.prov_cap_crit}"
            logger.error(msg)
            return False, msg

        elif cl.prov_cap_warn and cl.prov_cap_warn < size_prov_util:
            logger.warning(f"Cluster provisioned cap warning, util: {size_prov_util}% of cluster util: {cl.prov_cap_warn}")
    else:
        logger.warning("Cluster capacity records not found")

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

    dev_count = 0
    snodes = db_controller.get_storage_nodes()
    online_nodes = []
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1

    if dev_count == 0:
        logger.error("No NVMe devices found in the cluster")
        return False, "No NVMe devices found in the cluster"
    elif dev_count < 8:
        logger.warning("Number of active cluster devices are less than 8")
        # return False, "Number of active cluster devices are less than 8"

    if len(online_nodes) < 3 and ha_type == "ha":
        logger.error("Storage nodes are less than 3 in ha cluster")
        return False, "Storage nodes are less than 3 in ha cluster"

    if len(online_nodes) == 0:
        logger.error("No online Storage nodes found")
        return False, "No online Storage nodes found"

    if distr_vuid == 0:
        vuid = 1 + int(random.random() * 10000)
    else:
        vuid = distr_vuid

    node_count = 0
    for node in db_controller.get_storage_nodes():
        if node.status == node.STATUS_ONLINE:
            node_count += 1
    if distr_ndcs == 0 and distr_npcs == 0:
        if ha_type == "single":
            distr_ndcs = 4
            distr_npcs = 1
        else:

            if node_count == 3:
                distr_ndcs = 1
            elif node_count in [4, 5]:
                distr_ndcs = 2
            elif node_count >= 6:
                distr_ndcs = 4
            distr_npcs = 1
    else:
        if distr_ndcs + distr_npcs >= node_count:
            return False, f"ndcs+npcs: {distr_ndcs+distr_npcs} must be less than online node count: {node_count}"

    lvol = LVol()
    lvol.lvol_name = name
    lvol.size = size
    lvol.status = LVol.STATUS_ONLINE
    lvol.ha_type = ha_type
    lvol.bdev_stack = []
    lvol.uuid = str(uuid.uuid4())
    lvol.guid = _generate_hex_string(16)
    lvol.vuid = vuid
    lvol.lvol_bdev = f"LVS_{vuid}/{name}"
    lvol.lvs_name = f"LVS_{lvol.vuid}"

    lvol.crypto_bdev = ''
    lvol.comp_bdev = ''

    lvol.mode = 'read-write'
    lvol.lvol_type = 'lvol'
    lvol.nqn = cl.nqn + ":lvol:" + lvol.uuid
    lvol.ndcs = distr_ndcs
    lvol.npcs = distr_npcs
    lvol.distr_bs = distr_bs
    lvol.distr_chunk_bs = distr_chunk_bs
    lvol.distr_page_size = cl.page_size_in_blocks
    lvol.base_bdev = f"distr_{lvol.vuid}_{name}"
    # lvol.top_bdev = lvol.lvol_bdev
    lvol.top_bdev = lvol.base_bdev

    lvol.bdev_stack.append({"type": "distr", "name": lvol.base_bdev})
    # lvol.bdev_stack.append({"type": "lvs", "name": lvol.lvs_name})
    # lvol.bdev_stack.append({"type": "lvol", "name": lvol.lvol_bdev})

    if use_crypto is True:
        lvol.crypto_bdev = f"crypto_{lvol.lvol_name}"
        lvol.bdev_stack.append({"type": "crypto", "name": lvol.crypto_bdev})
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev

    if use_comp is True:
        lvol.comp_bdev = f"comp_{lvol.lvol_name}"
        lvol.bdev_stack.append({"type": "comp", "name": lvol.comp_bdev})
        lvol.lvol_type += ',compress'
        lvol.top_bdev = lvol.comp_bdev

    nodes = _get_next_3_nodes()

    if host_node:
        nodes.insert(0, host_node)
    else:
        host_node = nodes[0]

    lvol.hostname = host_node.hostname
    lvol.node_id = host_node.get_id()

    if ha_type == 'single':
        ret, error = add_lvol_on_node(lvol, host_node)
        if error:
            return ret, error

    elif ha_type == "ha":
        three_nodes = nodes[:3]
        nodes_ids = []
        nodes_ips = []
        for node in three_nodes:
            nodes_ids.append(node.get_id())
            port = 10000 + int(random.random() * 60000)
            nodes_ips.append(f"{node.mgmt_ip}:{port}")

        ha_address = ",".join(nodes_ips)
        for index, node in enumerate(three_nodes):
            ret, error = add_lvol_on_node(lvol, node, ha_address, index)
            if error:
                return ret, error
        lvol.nodes = nodes_ids

    host_node.lvols.append(lvol.uuid)
    host_node.write_to_db(db_controller.kv_store)

    lvol.pool_uuid = pool.id
    pool.lvols.append(lvol.uuid)
    pool.write_to_db(db_controller.kv_store)

    lvol.write_to_db(db_controller.kv_store)

    # set QOS
    if max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes:
        set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)
    return lvol.uuid, None


def add_lvol_on_node(lvol, snode, ha_comm_addrs=None, ha_inode_self=None):
    jm_names = get_jm_names(snode)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    num_blocks = int(lvol.size / lvol.distr_bs)
    ret = rpc_client.bdev_distrib_create(
        lvol.base_bdev, lvol.vuid, lvol.ndcs, lvol.npcs, num_blocks,
        lvol.distr_bs, jm_names, lvol.distr_chunk_bs, ha_comm_addrs, ha_inode_self, lvol.distr_page_size)
    if not ret:
        logger.error("Failed to create Distr bdev")
        return False, "Failed to create Distr bdev"
    if ret == "?":
        logger.error(f"Failed to create Distr bdev, ret={ret}")
        # return False

    if lvol.crypto_bdev:
        crypto_bdev = _create_crypto_lvol(rpc_client, lvol.crypto_bdev, lvol.lvol_bdev)
        if not crypto_bdev:
            return False, "Error creating crypto bdev"

    if lvol.comp_bdev:
        base_bdev = lvol.lvol_bdev
        if lvol.crypto_bdev:
            base_bdev = lvol.crypto_bdev

        comp_bdev = _create_compress_lvol(rpc_client, base_bdev)
        if not comp_bdev:
            return False, "Error creating comp bdev"

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

    logger.info("add lvol to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)

    logger.info("Sending cluster map to the lvol")
    snodes = db_controller.get_storage_nodes()
    cluster_map_data = distr_controller.get_distr_cluster_map(snodes, snode)
    cluster_map_data['UUID_node_target'] = snode.get_id()
    ret = rpc_client.distr_send_cluster_map(cluster_map_data)

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


def delete_lvol_from_node(lvol, node_id):
    snode = db_controller.get_storage_node_by_id(node_id)
    logger.debug(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    for bdev in lvol.bdev_stack[::-1]:
        type = bdev['type']
        name = bdev['name']
        if type == "alceml":
            ret = rpc_client.bdev_alceml_delete(name)
            if not ret:
                logger.error(f"failed to delete alceml: {name}")
            continue
        if type == "distr":
            ret = rpc_client.bdev_distrib_delete(name)
            if not ret:
                logger.error(f"failed to delete distr: {name}")
            continue
        if type == "lvs":
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
            if not ret:
                logger.error(f"failed to delete lvs: {name}")
            continue
        if type == "lvol":
            ret = rpc_client.delete_lvol(name)
            if not ret:
                logger.error(f"failed to delete lvol bdev {name}")
            continue
        if type == "ultra_pt":
            ret = rpc_client.ultra21_bdev_pass_delete(name)
            if not ret:
                logger.error(f"failed to delete ultra pt {name}")
            continue
        if type == "comp":
            ret = rpc_client.lvol_compress_delete(name)
            if not ret:
                logger.error(f"failed to delete comp bdev {name}")
            continue
        if type == "crypto":
            ret = rpc_client.lvol_crypto_delete(name)
            if not ret:
                logger.error(f"failed to delete crypto bdev {name}")
            continue

    ret = rpc_client.subsystem_delete(lvol.nqn)
    return True


def delete_lvol(uuid, force_delete=False):
    lvol = db_controller.get_lvol_by_id(uuid)
    if not lvol:
        logger.error(f"lvol not found: {uuid}")
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error(f"Pool is disabled")
        return False

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
        if snap.lvol.get_id() == uuid:
            logger.warning(f"Soft delete LVol that has snapshots. Snapshot:{snap.get_id()}")
            ret = rpc_client.subsystem_delete(lvol.nqn)
            logger.debug(ret)
            lvol.deleted = True
            lvol.write_to_db(db_controller.kv_store)
            return True

    if lvol.ha_type == 'single':
        ret = delete_lvol_from_node(lvol, lvol.node_id)
        if not ret:
            return False
    elif lvol.ha_type == "ha":
        for nodes_id in lvol.nodes:
            ret = delete_lvol_from_node(lvol, nodes_id)
            if not ret:
                return False

    # remove from storage node
    snode.lvols.remove(uuid)
    snode.write_to_db(db_controller.kv_store)

    # remove from pool
    pool.lvols.remove(uuid)
    pool.write_to_db(db_controller.kv_store)

    lvol.remove(db_controller.kv_store)

    # if lvol is clone and snapshot is deleted, then delete snapshot
    if lvol.cloned_from_snap:
        snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
        if snap.deleted is True:
            lvols_count = 0
            for lvol in db_controller.get_lvols():
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


def list_lvols(is_json):
    lvols = db_controller.get_lvols()
    data = []
    for lvol in lvols:
        if lvol.deleted is True:
            continue
        logger.debug(lvol)
        data.append({
            "id": lvol.uuid,
            "name": lvol.lvol_name,
            "size": utils.humanbytes(lvol.size),
            "pool": lvol.pool_uuid,
            "hostname": lvol.hostname,
            "ha type": lvol.ha_type,
            "status": lvol.status,
            "health": lvol.health_check,
        })

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def get_lvol(lvol_id, is_json):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
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

    logger.info(f"Resizing LVol: {lvol.id}, new size: {lvol.size}")

    # if lvol.pool_uuid:
    #     pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    #     if pool:
    #         print(pool)

    snode = db_controller.get_storage_node_by_hostname(lvol.hostname)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # ret = rpc_client.get_bdevs(lvol.lvol_name)
    # bdev_data = ret[0]
    # logger.debug(json.dumps(ret, indent=2))
    # print("is claimed:", bdev_data['claimed'])
    size_mb = int(new_size / (1024 * 1024))
    ret = rpc_client.resize_lvol(lvol.lvol_bdev, size_mb)
    if not ret:
        return "Error"

    lvol.size = new_size
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return ret


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

    lvol.mode = 'read-only'
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return True


def create_snapshot(lvol_id, snapshot_name):
    return snapshot_controller.add(lvol_id, snapshot_name)


def clone(snapshot_id, clone_name):
    return snapshot_controller.clone(snapshot_id, clone_name)


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
    new_records = utils.process_records(records_list, records_count)

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
    snodes = db_controller.get_storage_nodes()
    logger.info(f"Sending to: {snode.get_id()}")
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    cluster_map_data = distr_controller.get_distr_cluster_map(snodes, snode)
    cluster_map_data['UUID_node_target'] = snode.get_id()
    ret = rpc_client.distr_send_cluster_map(cluster_map_data)
    return ret


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
    logger.info(ret)
    print("*"*100)
    results, is_passed = distr_controller.parse_distr_cluster_map(ret)
    return utils.print_table(results)


def migrate(lvol_id):

    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    nodes = _get_next_3_nodes()

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
    return True
