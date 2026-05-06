# coding=utf-8
import copy
import logging as lg
import json
import random
import sys
import time
import uuid
from datetime import datetime
from typing import List, Tuple

from simplyblock_core import utils, constants
from simplyblock_core.controllers import snapshot_controller, pool_controller, lvol_events, tasks_controller, \
    snapshot_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.prom_client import PromClient

logger = lg.getLogger()


def _get_dhchap_group(cluster, pool=None):
    """Return the DH group to set on the target subsystem for DH-HMAC-CHAP.

    For pool-level DHCHAP the fixed DHCHAP_DHGROUP constant is used.
    Falls back to cluster.tls_config for legacy cluster-level config,
    otherwise returns 'null' (HMAC-CHAP only, no DH key exchange).
    """
    if pool and getattr(pool, 'dhchap', False):
        return constants.DHCHAP_DHGROUP
    if cluster and cluster.tls and cluster.tls_config:
        params = cluster.tls_config.get("params", cluster.tls_config)
        groups = params.get("dhchap_dhgroups") or []
        if groups:
            return groups[0]
    return "null"


def _register_pool_dhchap_keys_on_node(pool, snode, rpc_client):
    """Write pool-level DHCHAP key files to a storage node and register in SPDK keyring.

    All LVols in a DHCHAP pool share one key pair stored on the pool.
    Key names are pool-scoped so a single registration serves all LVols.

    Returns a dict with 'dhchap_key' and 'dhchap_ctrlr_key' keyring names,
    or an empty dict on failure.
    """
    snode_api = snode.client()
    safe_pool = pool.get_id().replace("-", "_")
    key_names = {}

    for key_type, key_value in (
        ("dhchap_key", pool.dhchap_key),
        ("dhchap_ctrlr_key", pool.dhchap_ctrlr_key),
    ):
        if not key_value:
            continue
        key_name = f"pool_{safe_pool}_{key_type}"
        result, error = snode_api.write_key_file(key_name, key_value)
        if error:
            logger.error("Failed to write pool key %s on node %s: %s",
                         key_name, snode.get_id(), error)
            continue
        key_path = result
        ret, err = rpc_client._request2("keyring_file_add_key",
                                        {"name": key_name, "path": key_path})
        if not ret and err:
            if err.get("code") == -17:
                logger.info("Pool key %s already in SPDK keyring on node %s, reusing",
                            key_name, snode.get_id())
            else:
                logger.error("Failed to register pool key %s in SPDK keyring on node %s: %s",
                             key_name, snode.get_id(), err.get("message", err))
                continue
        key_names[key_type] = key_name

    return key_names


def _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client):
    """Write DHCHAP key files to a storage node and register them in SPDK's keyring.

    Returns a dict mapping key type ('dhchap_key', 'dhchap_ctrlr_key', 'psk')
    to the SPDK keyring name for use in subsystem_add_host.
    """
    snode_api = snode.client()
    # Sanitize host NQN for use as filename
    safe_host = host_nqn.replace(":", "_").replace(".", "_")
    key_names = {}

    for key_type in ("dhchap_key", "dhchap_ctrlr_key", "psk"):
        key_value = host_entry.get(key_type)
        if not key_value:
            continue
        key_name = f"{key_type}_{safe_host}"
        # Write key file to storage node via SNodeAPI
        result, error = snode_api.write_key_file(key_name, key_value)
        if error:
            logger.error("Failed to write key file %s on node %s: %s", key_name, snode.get_id(), error)
            continue
        key_path = result
        # Register in SPDK keyring — "File exists" (code -17) means the key
        # is already registered, which is fine (e.g. same host on another volume).
        ret, err = rpc_client._request2("keyring_file_add_key",
                                        {"name": key_name, "path": key_path})
        if not ret and err:
            if err.get("code") == -17:
                logger.info("Key %s already in SPDK keyring on node %s, reusing",
                            key_name, snode.get_id())
            else:
                logger.error("Failed to register key %s in SPDK keyring on node %s: %s",
                             key_name, snode.get_id(), err.get("message", err))
                continue
        key_names[key_type] = key_name

    return key_names



def _create_crypto_lvol(rpc_client, name, base_name, key1, key2):
    ret = rpc_client.get_bdevs(base_name)
    if not ret:
        logger.error(f"Failed to find LVol bdev {base_name}")
        return False
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
    question = "Enter VUID number: "
    while True:
        sys.stdout.write(question)
        choice = str(input())
        try:
            ch = int(choice.strip())
            return ch
        except Exception as e:
            logger.debug(e)
            sys.stdout.write("Please respond with numbers")


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
    #     snode = db_controller.get_storage_nodes_by_hostname(host_id_or_name)
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

    # If user gave a QOS and the pool also have a QOS, return error
    if (max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes) and (pool.has_qos()):
        return False, "Both Lvol and Pool have QOS settings"

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
            subsys_count = len(set(lv.nqn for lv in db_controller.get_lvols_by_node_id(node.get_id())))
            if subsys_count >= node.max_lvol:
                continue
            if node.lvol_sync_del():
                logger.info(f"LVol sync delete task found on node: {node.get_id()}, proceeding anyway")
            online_nodes.append(node)
            node_st = {
                "lvol": subsys_count+1
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

    selected_node_ids: List[str] = []
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


def add_lvol_ha(name, size, host_id_or_name, ha_type, pool_id_or_name, use_comp=False, use_crypto=False,
                distr_vuid=0, max_rw_iops=0, max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0,
                with_snapshot=False, max_size=0, crypto_key1=None, crypto_key2=None, lvol_priority_class=0,
                uid=None, pvc_name=None, namespace=None, max_namespace_per_subsys=1, fabric="tcp", ndcs=0, npcs=0,
                allowed_hosts=None,
                do_replicate=False, replication_cluster_id=None):
    db_controller = DBController()
    logger.info(f"Adding LVol: {name}")
    host_node = None
    if host_id_or_name:
        try:
            host_node = db_controller.get_storage_node_by_id(host_id_or_name)
        except KeyError:
            nodes = db_controller.get_storage_nodes_by_hostname(host_id_or_name)
            if len(nodes) > 0:
                host_node = nodes[0]
            else:
                return False, f"Can not find storage node: {host_id_or_name}"
        if host_node.lvol_sync_del():
            logger.info(f"LVol sync delete task on node: {host_node.get_id()}, proceeding anyway")

    if namespace:
        try:
            master_lvol = db_controller.get_lvol_by_id(namespace)
        except KeyError as e:
            logger.error(e)
            return False

        host_node = db_controller.get_storage_node_by_id(master_lvol.node_id)

        lvols_count = 0
        for lv in db_controller.get_lvols(host_node.cluster_id):
            if lv.namespace == namespace:
                lvols_count += 1

        if lvols_count >= master_lvol.max_namespace_per_subsys:
            msg = f"Max namespaces reached: {lvols_count}"
            logger.error(msg)
            return False, msg

    pool = None
    for p in db_controller.get_pools():
        if pool_id_or_name == p.get_id() or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    cl = db_controller.get_cluster_by_id(pool.cluster_id)

    if (fabric == "tcp" and not cl.fabric_tcp) or (fabric == "rdma" and not cl.fabric_rdma):
        return False,  f"Fabric not available in cluster: {fabric}"

    if cl.status not in [cl.STATUS_ACTIVE, cl.STATUS_DEGRADED]:
        return False, f"Cluster is not active, status: {cl.status}"

    if lvol_priority_class > 0:
        class_found = False
        for qos_class in db_controller.get_qos(cl.uuid):
            if qos_class.class_id == lvol_priority_class:
                class_found = True
        if not class_found:
            return False, f"QOS class not found: {lvol_priority_class}"

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

    if pool.has_qos():
        host_node = db_controller.get_storage_node_by_id(pool.qos_host)

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

    if host_node and host_node.status != StorageNode.STATUS_ONLINE:
        mgs = f"Storage node is not online. ID: {host_node.get_id()} status: {host_node.status}"
        logger.error(mgs)
        return False, mgs

    if host_node and host_node.lvstore_status == "in_creation":
        mgs = f"Storage node LVStore is being recreated (restart in progress). ID: {host_node.get_id()}"
        logger.error(mgs)
        return False, mgs

    if ndcs or npcs:
        if ndcs+npcs > len(online_nodes):
            mgs = f"Online storage nodes: {len(online_nodes)} are less than the required LVol geometry: {(ndcs+npcs)}"
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
    lvol.size = int(size)
    lvol.max_size = int(max_size)
    lvol.status = LVol.STATUS_IN_CREATION
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
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
    if lvol_priority_class:
        lvol.lvol_priority_class = lvol_priority_class
    else:
        lvol.lvol_priority_class = 0
    lvol.fabric = fabric

    if namespace:
        master_lvol = db_controller.get_lvol_by_id(namespace)
        lvol.nqn = master_lvol.nqn
        lvol.namespace = namespace or ""
    else:
        lvol.nqn = cl.nqn + ":lvol:" + lvol.uuid

    lvol.max_namespace_per_subsys = max_namespace_per_subsys

    if not host_node:
        nodes = _get_next_3_nodes(cl.get_id(), lvol.size)
        if not nodes:
            return False, "No nodes found with enough resources to create the LVol"
        host_node = nodes[0]

    s_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
    attr_name = f"active_{fabric}"
    is_active_primary = getattr(host_node, attr_name)
    is_active_secondary = getattr(s_node, attr_name)
    if not is_active_primary:
        return False, f"Primary node fabric {fabric} is not active"
    if not is_active_secondary:
        return False, f"Secondary node fabric {fabric} is not active"

    lvol.hostname = host_node.hostname
    lvol.node_id = host_node.get_id()
    lvol.lvs_name = host_node.lvstore
    lvol.subsys_port = host_node.get_lvol_subsys_port(host_node.lvstore)
    lvol.top_bdev = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    lvol.base_bdev = lvol.top_bdev
    if npcs or ndcs:
        lvol.npcs = npcs or 0
        lvol.ndcs = ndcs or 0
    else:
        lvol.npcs = cl.distr_npcs
        lvol.ndcs = cl.distr_ndcs
    lvol.do_replicate = bool(do_replicate)
    if lvol.do_replicate:
        if replication_cluster_id:
            replication_cluster = db_controller.get_cluster_by_id(replication_cluster_id)
            if not replication_cluster:
                return False, f"Replication cluster not found: {replication_cluster_id}"
        else:
            replication_cluster_id = cl.snapshot_replication_target_cluster
        random_nodes = _get_next_3_nodes(replication_cluster_id, lvol.size)
        lvol.replication_node_id = random_nodes[0].get_id()

    subsys_count = len(set(lv.nqn for lv in db_controller.get_lvols_by_node_id(host_node.get_id())))
    if subsys_count > host_node.max_lvol:
        error = f"Too many subsystems on node: {host_node.get_id()}, max subsystems reached: {subsys_count}"
        logger.error(error)
        return False, error

    lvol_dict: dict = {
        "type": "bdev_lvol",
        "name": lvol.lvol_bdev,
        "params": {
            "name": lvol.lvol_bdev,
            "size_in_mib": utils.convert_size(lvol.size, 'MiB'),
            "lvs_name": lvol.lvs_name,
            "lvol_priority_class": 0
        }
    }

    if lvol.ndcs or lvol.npcs:
        lvol_dict["params"]["ndcs"] = lvol.ndcs
        lvol_dict["params"]["npcs"] = lvol.npcs

    if cl.is_qos_set() and lvol.lvol_priority_class > 0:
        lvol_dict["params"]["lvol_priority_class"] = lvol.lvol_priority_class +1

    lvol.bdev_stack = [lvol_dict]

    if use_crypto:
        if crypto_key1 is None or crypto_key2 is None:
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

    # Process allowed hosts (for host restriction and/or DH-HMAC-CHAP authentication)
    if not namespace:
        if pool.dhchap:
            # Pool-level DHCHAP: inherit allowed hosts from pool (no per-host key generation)
            lvol.allowed_hosts = [{"nqn": h} for h in pool.allowed_hosts]
        elif allowed_hosts:
            # Legacy per-lvol host restriction with pool.sec_options key generation
            host_entries = _build_host_entries(allowed_hosts, pool.sec_options or None)
            if isinstance(host_entries, tuple):
                return host_entries  # (False, error_message)
            lvol.allowed_hosts = host_entries

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
        from simplyblock_core.storage_node_ops import (
            find_leader_with_failover, check_non_leader_for_operation,
            queue_for_restart_drain, execute_on_leader_with_failover,
        )

        # Build nodes list
        secondary_ids = [host_node.secondary_node_id]
        if host_node.tertiary_node_id:
            secondary_ids.append(host_node.tertiary_node_id)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        all_nodes = [host_node]
        for sid in secondary_ids:
            try:
                all_nodes.append(db_controller.get_storage_node_by_id(sid))
            except KeyError:
                pass

        # Step 1: Pre-check all non-leaders BEFORE executing on leader
        primary_node, non_leaders = find_leader_with_failover(all_nodes, lvol.lvs_name)
        if primary_node is None:
            msg = "No leader available for lvol create"
            logger.error(msg)
            lvol.remove(db_controller.kv_store)
            return False, msg

        secondary_nodes = []
        for nl in non_leaders:
            action = check_non_leader_for_operation(
                nl.get_id(), lvol.lvs_name, operation_type="create",
                leader_op_completed=False, all_nodes=all_nodes)
            if action == "reject":
                msg = f"Cannot create lvol: non-leader {nl.get_id()[:8]} unreachable but fabric healthy"
                logger.error(msg)
                lvol.remove(db_controller.kv_store)
                return False, msg
            elif action == "proceed":
                secondary_nodes.append(nl)
            elif action == "queue":
                queue_for_restart_drain(
                    nl.get_id(), lvol.lvs_name,
                    lambda c=nl, idx=len(secondary_nodes): add_lvol_on_node(
                        lvol, c, is_primary=False, secondary_index=idx),
                    f"register create lvol {lvol.uuid} on {nl.get_id()[:8]}")
            # "skip" — disconnected or pre_block, skip

        # Step 2: Execute on leader (with failover on failure)
        def _create_on_leader(leader):
            lvol_bdev, error = add_lvol_on_node(lvol, leader)
            if error:
                raise RuntimeError(error)
            return lvol_bdev

        success, actual_leader, result = execute_on_leader_with_failover(
            all_nodes, lvol.lvs_name, _create_on_leader)
        if not success:
            logger.error(f"Failed to create lvol on leader: {result}")
            lvol.remove(db_controller.kv_store)
            return False, str(result)

        lvol_bdev = result
        lvol.lvol_uuid = lvol_bdev['uuid']
        lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

        # Step 3: Execute registration on non-leaders that passed pre-check
        for sec_idx, sec in enumerate(secondary_nodes):
            action = check_non_leader_for_operation(
                sec.get_id(), lvol.lvs_name, operation_type="create",
                leader_op_completed=True, all_nodes=all_nodes)
            if action == "proceed":
                lvol_bdev, error = add_lvol_on_node(lvol, sec, is_primary=False, secondary_index=sec_idx)
                if error:
                    logger.error(error)
                    ret = delete_lvol_from_node(lvol.get_id(), actual_leader.get_id())
                    if not ret:
                        logger.error("")
                    lvol.remove(db_controller.kv_store)
                    return False, error
            elif action == "kill_and_wait":
                logger.warning("Non-leader %s needs kill+restart for lvol create", sec.get_id()[:8])
                queue_for_restart_drain(
                    sec.get_id(), lvol.lvs_name,
                    lambda c=sec, si=sec_idx: add_lvol_on_node(lvol, c, is_primary=False, secondary_index=si),
                    f"register create lvol {lvol.uuid} on {sec.get_id()[:8]} (after kill)")
            elif action == "queue":
                queue_for_restart_drain(
                    sec.get_id(), lvol.lvs_name,
                    lambda c=sec, si=sec_idx: add_lvol_on_node(lvol, c, is_primary=False, secondary_index=si),
                    f"register create lvol {lvol.uuid} on {sec.get_id()[:8]}")
            # "skip", "reject" at this stage → already handled or skip

    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db_controller.kv_store)
    lvol_events.lvol_create(lvol)

    if pool.has_qos():
        connect_lvol_to_pool(lvol.uuid)

    # set QOS
    if max_rw_iops >= 0 or max_rw_mbytes >= 0 or max_r_mbytes >= 0 or max_w_mbytes >= 0:
        set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)

    if pool.allowed_hosts:
        for host_nqn in pool.allowed_hosts:
            logger.info(f"Adding host {host_nqn} to lvol {lvol.get_id()}")
            add_host_to_lvol(lvol.get_id(), host_nqn)

    return lvol.uuid, None


def _create_bdev_stack(lvol, snode, is_primary=True):
    rpc_client = snode.rpc_client()

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


def add_lvol_on_node(lvol, snode, is_primary=True, secondary_index=0):
    rpc_client = snode.rpc_client()

    ret, msg = _create_bdev_stack(lvol, snode, is_primary=is_primary)
    if not ret:
        return False, msg

    if not lvol.namespace:
        if is_primary:
            min_cntlid = 1
        else:
            # Each secondary needs a unique cntlid range to avoid conflicts
            # sec1: 1000, sec2: 2000, etc.
            min_cntlid = 1000 * (secondary_index + 1)
        allow_any = not bool(lvol.allowed_hosts)
        logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
        ret = rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                          max_namespaces=lvol.max_namespace_per_subsys,
                                          allow_any_host=allow_any)

        # add allowed hosts to subsystem
        if lvol.allowed_hosts:
            db_ctrl = DBController()
            cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
            pool = None
            if lvol.pool_uuid:
                try:
                    pool = db_ctrl.get_pool_by_id(lvol.pool_uuid)
                except KeyError:
                    pass
            dhchap_group = _get_dhchap_group(cluster, pool)
            pool_key_names = {}
            if pool and pool.dhchap:
                pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
            for host_entry in lvol.allowed_hosts:
                logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
                if pool and pool.dhchap:
                    rpc_client.subsystem_add_host(
                        lvol.nqn, host_entry["nqn"],
                        dhchap_key=pool_key_names.get("dhchap_key"),
                        dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                        dhchap_group=dhchap_group,
                    )
                else:
                    has_keys = any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
                    if has_keys:
                        key_names = _register_dhchap_keys_on_node(snode, host_entry["nqn"], host_entry, rpc_client)
                        rpc_client.subsystem_add_host(
                            lvol.nqn, host_entry["nqn"],
                            psk=key_names.get("psk"),
                            dhchap_key=key_names.get("dhchap_key"),
                            dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
                            dhchap_group=dhchap_group,
                        )
                    else:
                        rpc_client.subsystem_add_host(lvol.nqn, host_entry["nqn"])

        if is_primary or lvol.node_id == snode.get_id():
            ana_state = "optimized"
        else:
            ana_state = "non_optimized"

        # add listeners
        # Use the per-lvstore port for the lvol's lvstore
        listener_port = snode.get_lvol_subsys_port(lvol.lvs_name)
        logger.info("adding listeners")
        for iface in snode.data_nics:
            if iface.ip4_address and lvol.fabric==iface.trtype.lower():
                logger.info("adding listener for %s on IP %s port %s" % (lvol.nqn, iface.ip4_address, listener_port))
                ret, err = rpc_client.nvmf_subsystem_add_listener(
                    lvol.nqn, iface.trtype, iface.ip4_address, listener_port, ana_state)
                if not ret:
                    if err and "code" in err and err["code"] == -32602:
                        logger.warning("listener already exists")
                    else:
                        return False, f"Failed to create listener for {lvol.get_id()}"
            elif iface.ip4_address and lvol.fabric == "tcp" and snode.active_tcp:
                logger.info("adding listener for %s on IP %s, fabric TCP port %s" % (lvol.nqn, iface.ip4_address, listener_port))
                ret, err = rpc_client.nvmf_subsystem_add_listener(
                        lvol.nqn, "TCP", iface.ip4_address, listener_port, ana_state)
                if not ret:
                    if err and "code" in err and err["code"] == -32602:
                        logger.warning("listener already exists")
                    else:
                        return False, f"Failed to create listener for {lvol.get_id()}"

    logger.info("Add BDev to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
    if not ret:
        return False, "Failed to add bdev to subsystem"
    lvol.ns_id = int(ret)

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
    rpc_client = snode.rpc_client()
    ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
    if ret and len(ret) > 0 and "lvs leadership" in ret[0]:
        is_leader = ret[0]["lvs leadership"]
        return is_leader
    return False

def recreate_lvol_on_node(lvol, snode, ha_inode_self=0, ana_state=None):
    rpc_client = snode.rpc_client()

    base=f"{lvol.lvs_name}/{lvol.lvol_bdev}"

    if "crypto" in lvol.lvol_type:
        ret = _create_crypto_lvol(
            rpc_client, lvol.crypto_bdev, base, lvol.crypto_key1, lvol.crypto_key2)
        if not ret:
            msg=f"Failed to create crypto lvol on node {snode.get_id()}"
            logger.error(msg)
            return False, msg

    min_cntlid = 1 + 1000 * ha_inode_self
    allow_any = not bool(lvol.allowed_hosts)
    logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
    rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS,
                                allow_any_host=allow_any)

    # Re-apply allowed hosts on subsystem recreate
    if lvol.allowed_hosts:
        db_ctrl = DBController()
        cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
        pool = None
        if lvol.pool_uuid:
            try:
                pool = db_ctrl.get_pool_by_id(lvol.pool_uuid)
            except KeyError:
                pass
        dhchap_group = _get_dhchap_group(cluster, pool)
        pool_key_names = {}
        if pool and pool.dhchap:
            pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
        for host_entry in lvol.allowed_hosts:
            logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
            if pool and pool.dhchap:
                rpc_client.subsystem_add_host(
                    lvol.nqn, host_entry["nqn"],
                    dhchap_key=pool_key_names.get("dhchap_key"),
                    dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                    dhchap_group=dhchap_group,
                )
            else:
                has_keys = any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
                if has_keys:
                    key_names = _register_dhchap_keys_on_node(snode, host_entry["nqn"], host_entry, rpc_client)
                    rpc_client.subsystem_add_host(
                        lvol.nqn, host_entry["nqn"],
                        psk=key_names.get("psk"),
                        dhchap_key=key_names.get("dhchap_key"),
                        dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
                        dhchap_group=dhchap_group,
                    )
                else:
                    rpc_client.subsystem_add_host(lvol.nqn, host_entry["nqn"])

    # if namespace_found is False:
    logger.info("Add BDev to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
    # if not ret:
    #     return False, "Failed to add bdev to subsystem"

    # add listeners - use per-lvstore port
    recreate_lvs_port = snode.get_lvol_subsys_port(lvol.lvs_name)
    logger.info("adding listeners")
    for iface in snode.data_nics:
        if iface.ip4_address and lvol.fabric==iface.trtype.lower():
            if not ana_state:
                ana_state = "non_optimized"
                if lvol.node_id == snode.get_id():
                    ana_state = "optimized"
            logger.info("adding listener for %s on IP %s port %s" % (lvol.nqn, iface.ip4_address, recreate_lvs_port))
            logger.info(f"Setting ANA state: {ana_state}")
            ret = rpc_client.listeners_create(lvol.nqn, iface.trtype, iface.ip4_address, recreate_lvs_port, ana_state)

    return True, None


def recreate_lvol(lvol_id):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
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


def _remove_bdev_stack(bdev_stack, rpc_client, del_async=False):
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
        elif type == "crypto" and not del_async:
            ret = rpc_client.lvol_crypto_delete(name)
            if ret:
                ret = rpc_client.lvol_crypto_key_delete(f'key_{name}')

        elif type == "bdev_lvstore":
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_lvol":
            name = bdev['params']["lvs_name"]+"/"+bdev['params']["name"]
            ret, _ = rpc_client.delete_lvol(name, del_async=del_async)
        elif type == "bdev_lvol_clone":
            ret, _ = rpc_client.delete_lvol(name,  del_async=del_async)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
    return True


def delete_lvol_from_node(lvol_id, node_id, clear_data=True, del_async=False, force=False):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return True

    # Per design: gate sync deletes on non-leader nodes.
    from simplyblock_core.storage_node_ops import check_non_leader_for_operation, queue_for_restart_drain
    if not force:
        action = check_non_leader_for_operation(node_id, lvol.lvs_name, operation_type="delete")
        if action == "skip":
            logger.info(f"Skipping sync delete of {lvol_id} on {node_id[:8]}: node disconnected")
            lvol.deletion_status = node_id
            lvol.write_to_db(db_controller.kv_store)
            return True
        elif action == "queue":
            queue_for_restart_drain(
                node_id, lvol.lvs_name,
                lambda: delete_lvol_from_node(lvol_id, node_id, clear_data, del_async),
                f"sync delete lvol {lvol_id}")
            return True
        elif action == "retry":
            queue_for_restart_drain(
                node_id, lvol.lvs_name,
                lambda: delete_lvol_from_node(lvol_id, node_id, clear_data, del_async),
                f"retry sync delete lvol {lvol_id}")
            return True
    # action == "proceed" — execute now

    logger.info(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = snode.rpc_client(timeout=5, retry=2)

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.has_qos():
        ret = rpc_client.bdev_lvol_remove_from_group(pool.numeric_id, [lvol.top_bdev])
        if not ret:
            logger.error("RPC failed bdev_lvol_remove_from_group")

    subsystem = rpc_client.subsystem_list(lvol.nqn)
    # 1- remove subsystem
    if subsystem:
        if len(subsystem[0]["namespaces"]) > 1:
            rpc_client.nvmf_subsystem_remove_ns(lvol.nqn, lvol.ns_id)
        else:
            logger.info("Removing subsystem")
            rpc_client.subsystem_delete(lvol.nqn)

    # 2- remove bdevs
    logger.info("Removing bdev stack")
    ret = _remove_bdev_stack(lvol.bdev_stack[::-1], rpc_client, del_async)
    if not ret:
        return False

    lvol.deletion_status = node_id
    lvol.write_to_db(db_controller.kv_store)
    return True


def delete_lvol(id_or_name, force_delete=False):
    db_controller = DBController()
    try:
        lvol = (
                db_controller.get_lvol_by_id(id_or_name)
                if utils.UUID_PATTERN.match(id_or_name) is not None
                else db_controller.get_lvol_by_name(id_or_name)
        )
    except KeyError as e:
        logger.error(e)
        return False

    # Block during restart Phase 5
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        if snode.lvstore_status == "in_creation" and not force_delete:
            logger.error(f"Cannot delete lvol {lvol.uuid}: node LVStore restart in progress")
            return False
    except KeyError:
        pass

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    if active_mig and not force_delete:
        logger.error(f"Cannot delete lvol {lvol.uuid}: active migration {active_mig.uuid}")
        return False

    if lvol.status == LVol.STATUS_RESTORING and not force_delete:
        logger.error(f"Cannot delete lvol {lvol.uuid}: backup restore in progress")
        return False
    if lvol.status == LVol.STATUS_DELETED:
        logger.error(f"lvol {lvol.uuid}: deleted already")
        return False

    if lvol.status == LVol.STATUS_IN_DELETION:
        logger.info(f"lvol:{lvol.get_id()} status is in deletion")
        if not force_delete:
            return True

    logger.debug(lvol)
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
    except KeyError:
        logger.error(f"lvol node id not found: {lvol.node_id}")
        if not force_delete:
            return False
        lvol.status = LVol.STATUS_DELETED
        lvol.write_to_db(db_controller.kv_store)

        # if lvol is clone and snapshot is deleted, then delete snapshot
        if lvol.cloned_from_snap:
            try:
                snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
                if snap.deleted is True:
                    lvols_count = 0
                    for lvol in db_controller.get_lvols():  # pass
                        if lvol.cloned_from_snap == snap.get_id():
                            lvols_count += 1
                    if lvols_count == 0:
                        snapshot_controller.delete(snap.get_id())
            except KeyError:
                pass # already removed

        logger.info("Done")
        return True

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    # Persist deletion intent BEFORE any data-plane RPC. If the leader-side
    # delete then times out or errors (for example: SPDK back-pressure on
    # the leader while a peer is being container-killed in an outage soak),
    # the lvol stays in_deletion and lvol_monitor's STATUS_IN_DELETION
    # reconcile path drives it to completion. Previously the status was set
    # only after a successful leader op, so a transient leader RPC failure
    # left the lvol in 'online' state with no record of the deletion intent
    # — the API returned results=False and no background process retried.
    if lvol.status != LVol.STATUS_IN_DELETION:
        old_status = lvol.status
        lvol.status = LVol.STATUS_IN_DELETION
        lvol.write_to_db(db_controller.kv_store)
        try:
            lvol_events.lvol_status_change(lvol, lvol.status, old_status)
        except KeyError:
            pass

    if lvol.ha_type == 'single':
        ret = delete_lvol_from_node(lvol.get_id(), lvol.node_id, force=force_delete)
        if not ret:
            if not force_delete:
                return False

    elif lvol.ha_type == "ha":
        from simplyblock_core.storage_node_ops import (
            check_non_leader_for_operation,
            queue_for_restart_drain, execute_on_leader_with_failover,
        )

        host_node = db_controller.get_storage_node_by_id(snode.get_id())
        all_sec_nodes = []
        for sec_id in lvol.nodes[1:]:
            try:
                all_sec_nodes.append(db_controller.get_storage_node_by_id(sec_id))
            except KeyError:
                pass
        all_nodes = [host_node] + all_sec_nodes

        # Step 1: Execute async delete on leader (with failover)
        def _delete_on_leader(leader):
            ret = delete_lvol_from_node(lvol.get_id(), leader.get_id(), force=force_delete)
            return ret if ret else None

        success, actual_leader, result = execute_on_leader_with_failover(
            all_nodes, lvol.lvs_name, _delete_on_leader)
        if not success:
            logger.error(f"Failed to delete lvol from leader: {result}")
            if not force_delete:
                return False

        # Step 2: Sync delete on non-leaders (leader op already completed)
        non_leaders = [n for n in all_nodes if actual_leader and n.get_id() != actual_leader.get_id()]
        for nl in non_leaders:
            action = check_non_leader_for_operation(
                nl.get_id(), lvol.lvs_name, operation_type="delete",
                leader_op_completed=True, all_nodes=all_nodes)
            if action == "skip":
                continue
            elif action in ("queue", "kill_and_wait"):
                queue_for_restart_drain(
                    nl.get_id(), lvol.lvs_name,
                    lambda c=nl: delete_lvol_from_node(lvol.get_id(), c.get_id()),
                    f"sync delete lvol {lvol.get_id()} on {nl.get_id()[:8]}")
            elif action == "proceed":
                try:
                    sec_rpc = nl.rpc_client()
                    subsystem = sec_rpc.subsystem_list(lvol.nqn)
                    if subsystem:
                        if len(subsystem[0]["namespaces"]) > 1:
                            sec_rpc.nvmf_subsystem_remove_ns(lvol.nqn, lvol.ns_id)
                        else:
                            sec_rpc.subsystem_delete(lvol.nqn)
                except Exception as e:
                    logger.warning(f"Failed sync delete on {nl.get_id()}: {e}")
                    # Post-leader-op: check if we should kill or queue
                    post_action = check_non_leader_for_operation(
                        nl.get_id(), lvol.lvs_name, operation_type="delete",
                        leader_op_completed=True, all_nodes=all_nodes)
                    if post_action in ("queue", "kill_and_wait"):
                        queue_for_restart_drain(
                            nl.get_id(), lvol.lvs_name,
                            lambda c=nl: delete_lvol_from_node(lvol.get_id(), c.get_id()),
                            f"retry sync delete lvol {lvol.get_id()} on {nl.get_id()[:8]}")

    # Status was already set to STATUS_IN_DELETION above, before the
    # data-plane RPC, so we just refresh the in-memory copy in case
    # delete_lvol_from_node updated other fields (e.g. deletion_status).
    lvol = db_controller.get_lvol_by_id(lvol.get_id())

    if lvol.cloned_from_snap and lvol.delete_snap_on_lvol_delete:
        logger.info(f"Deleting snap: {lvol.cloned_from_snap}")
        snapshot_controller.delete(lvol.cloned_from_snap)

    # if lvol is clone and snapshot is deleted, then delete snapshot
    elif lvol.cloned_from_snap:
        try:
            snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            if snap.snap_ref_id:
                ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
                ref_snap.ref_count -= 1
                ref_snap.write_to_db(db_controller.kv_store)
            else:
                snap.ref_count -= 1
                snap.write_to_db(db_controller.kv_store)
            if snap.deleted is True:
                snapshot_controller.delete(snap.get_id())
        except KeyError:
            pass # already deleted

    logger.info("Done")
    return True

def connect_lvol_to_pool(uuid):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(uuid)
    except KeyError as e:
        logger.error(e)
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    rpc_client = snode.rpc_client()

    if pool.has_qos():
        ret = rpc_client.bdev_lvol_add_to_group(pool.numeric_id, [lvol.top_bdev])
        if not ret:
            logger.error("RPC failed bdev_lvol_add_to_group")
            return False

        # re-apply the QOS limits
        ret = rpc_client.bdev_lvol_set_qos_limit(pool.numeric_id, pool.max_rw_ios_per_sec,
                                            pool.max_rw_mbytes_per_sec, pool.max_r_mbytes_per_sec,
                                            pool.max_w_mbytes_per_sec)
        if not ret:
            logger.error("RPC failed bdev_set_qos_limit")
            return False

    lvol.write_to_db(db_controller.kv_store)
    pool.write_to_db(db_controller.kv_store)
    logger.info("Done")
    return True

def set_lvol(uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, name=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(uuid)
    except KeyError as e:
        logger.error(e)
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False
    if pool.has_qos():
        logger.error("Pool already has QOS settings")
        return False

    if name:
        lvol.lvol_name = name

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    rpc_client = snode.rpc_client()

    if max_rw_iops < 0:
        msg = "max_rw_iops can not be negative"
        logger.error(msg)
        return False

    if max_rw_mbytes < 0:
        msg = "max_rw_mbytes can not be negative"
        logger.error(msg)
        return False

    if max_r_mbytes < 0:
        msg = "max_r_mbytes can not be negative"
        logger.error(msg)
        return False

    if max_w_mbytes < 0:
        msg = "max_w_mbytes can not be negative"
        logger.error(msg)
        return False

    rw_ios_per_sec = lvol.rw_ios_per_sec
    if max_rw_iops is not None and max_rw_iops >= 0:
        rw_ios_per_sec = max_rw_iops

    rw_mbytes_per_sec = lvol.rw_mbytes_per_sec
    if max_rw_mbytes is not None and max_rw_mbytes >= 0:
        rw_mbytes_per_sec = max_rw_mbytes

    r_mbytes_per_sec = lvol.r_mbytes_per_sec
    if max_r_mbytes is not None and max_r_mbytes >= 0:
        r_mbytes_per_sec = max_r_mbytes

    w_mbytes_per_sec = lvol.w_mbytes_per_sec
    if max_w_mbytes is not None and max_w_mbytes >= 0:
        w_mbytes_per_sec = max_w_mbytes

    ret = rpc_client.bdev_set_qos_limit(lvol.top_bdev, rw_ios_per_sec, rw_mbytes_per_sec, r_mbytes_per_sec,
                                        w_mbytes_per_sec)
    if not ret:
        return "Error setting qos limits"

    secondary_ids = []
    if snode.secondary_node_id:
        secondary_ids.append(snode.secondary_node_id)
    if snode.tertiary_node_id:
        secondary_ids.append(snode.tertiary_node_id)
    for sec_id in secondary_ids:
        sec_node = db_controller.get_storage_node_by_id(sec_id)
        if sec_node and sec_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            ret = sec_node.rpc_client().bdev_set_qos_limit(
                lvol.top_bdev, rw_ios_per_sec, rw_mbytes_per_sec, r_mbytes_per_sec, w_mbytes_per_sec)
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
        try:
            pool = (
                    db_controller.get_pool_by_id(pool_id_or_name)
                    if utils.UUID_PATTERN.match(pool_id_or_name) is not None
                    else db_controller.get_pool_by_name(pool_id_or_name)
            )
            for lv in db_controller.get_lvols_by_pool_id(pool.get_id()):
                lvols.append(lv)
        except KeyError:
            pass
    else:
        lvols = db_controller.get_lvols()

    data = []

    # Build set of lvol UUIDs with active migrations (single DB scan)
    migrating_lvols = set()
    for m in db_controller.get_migrations(cluster_id):
        if m.is_active():
            migrating_lvols.add(m.lvol_id)

    # Build policy lookup maps (single scan of attachments + policies)
    all_attachments = db_controller.get_backup_policy_attachments(cluster_id)
    all_policies = {p.uuid: p for p in db_controller.get_backup_policies(cluster_id)}
    lvol_policy_map = {}   # lvol_id -> policy
    pool_policy_map = {}   # pool_id -> policy
    for att in all_attachments:
        pol = all_policies.get(att.policy_id)
        if not pol:
            continue
        if att.target_type == "lvol":
            lvol_policy_map[att.target_id] = pol
        elif att.target_type == "pool":
            pool_policy_map[att.target_id] = pol

    for lvol in lvols:
        logger.debug(lvol)
        if lvol.deleted is True and all is False:
            continue
        size_used = 0
        records = db_controller.get_lvol_stats(lvol, 1)
        if records:
            size_used = records[0].size_used
        if lvol.ndcs == 0 and lvol.npcs == 0:
            cl = db_controller.get_cluster_by_id(cluster_id)
            mode = f"{cl.distr_ndcs}x{cl.distr_npcs}"
        else:
            mode = f"{lvol.ndcs}x{lvol.npcs}"

        eff_policy = lvol_policy_map.get(lvol.get_id()) or pool_policy_map.get(lvol.pool_uuid)
        lvol_data = {
            "Id": lvol.uuid,
            "Name": lvol.lvol_name,
            "Size": utils.humanbytes(lvol.size),
            "Used": f"{utils.humanbytes(size_used)}",
            "Hostname": lvol.hostname,
            "HA": lvol.ha_type,
            "BlobID": lvol.blobid or "",
            "LVolUUID": lvol.lvol_uuid or "",
            "Status": lvol.status,
            "M": "M" if lvol.uuid in migrating_lvols else "",
            "IO Err": lvol.io_error,
            "Health": lvol.health_check,
            "NS ID": lvol.ns_id,
            "Mode": mode,
            "Policy": eff_policy.policy_name if eff_policy else "",
            "Replicated On": lvol.replication_node_id,
        }
        data.append(lvol_data)

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


def get_replication_info(lvol_id_or_name):
    db_controller = DBController()
    lvol = None
    for lv in db_controller.get_lvols():  # pass
        if lv.get_id() == lvol_id_or_name or lv.lvol_name == lvol_id_or_name:
            lvol = lv
            break

    if not lvol:
        logger.error(f"LVol id or name not found: {lvol_id_or_name}")
        return None

    tasks = []
    snaps = []
    out = {
        "last_snapshot_id": "",
        "last_replication_time": "",
        "last_replication_duration": "",
        "replicated_count": 0,
        "snaps": [],
        "tasks": [],
    }
    node = db_controller.get_storage_node_by_id(lvol.node_id)
    for task in db_controller.get_job_tasks(node.cluster_id):
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            logger.debug(task)
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue

            if snap.lvol.get_id() != lvol.get_id():
                continue
            snaps.append(snap)
            tasks.append(task)

    if tasks:
        tasks = sorted(tasks, key=lambda x: x.date)
        snaps = sorted(snaps, key=lambda x: x.created_at)
        out["snaps"] = [s.to_dict() for s in snaps]
        out["tasks"] = [t.to_dict() for t in tasks]
        out["replicated_count"] = len(snaps)
        last_task = tasks[-1]
        last_snap = db_controller.get_snapshot_by_id(last_task.function_params["snapshot_id"])
        out["last_snapshot_id"] = last_snap.get_id()
        out["last_replication_time"] = last_task.updated_at
        if "end_time" in last_task.function_params and "start_time" in last_task.function_params:
            duration = utils.strfdelta_seconds(
                last_task.function_params["end_time"] - last_task.function_params["start_time"])
        elif "start_time" in last_task.function_params:
            duration = utils.strfdelta_seconds(int(time.time()) - last_task.function_params["start_time"])
        else:
            duration = ""
        out["last_replication_duration"] = duration

    return out


def get_lvol(lvol_id_or_name, is_json):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id_or_name)
    for lv in db_controller.get_lvols():  # pass
        if lv.get_id() == lvol_id_or_name or lv.lvol_name == lvol_id_or_name:
            lvol = lv
            break

    if not lvol:
        logger.error(f"LVol id or name not found: {lvol_id_or_name}")
        return False

    data = lvol.get_clean_dict()

    del data['nvme_dev']

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    data['migrating'] = active_mig.uuid if active_mig else ""

    policy = db_controller.get_policy_for_lvol(lvol)
    data['policy'] = policy.policy_name if policy else ""

    if is_json:
        return json.dumps(data, indent=2)
    else:
        data2 = [{"key": key, "value": data[key]} for key in data]
        return utils.print_table(data2)


def connect_lvol(uuid, ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO, host_nqn=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(uuid)
    except KeyError:
        logger.exception("Failed to get lvol by id: %s", uuid)
        return False, "Failed to find volume"

    # Look up host entry for secrets when host_nqn is provided
    host_entry = None
    if lvol.allowed_hosts:
        if not host_nqn:
            return False, f"Volume {uuid} has allowed hosts configured; --host-nqn is required"
        for h in lvol.allowed_hosts:
            if h["nqn"] == host_nqn:
                host_entry = h
                # Note: an earlier change (sfam-2722) unconditionally
                # injected ``pool.dhchap_key`` / ``pool.dhchap_ctrlr_key``
                # into ``host_entry`` here. That broke three contracts:
                #   - it overrode existing host-level keys
                #   - it injected pool keys when the pool had no DHCHAP
                #     configured (pool.dhchap_key=None)
                #   - it emitted ``--dhchap-secret`` for hosts using PSK
                #     (TLS auth), which conflicts with the host's chosen
                #     auth mode
                # Pool-level DHCHAP keys are retrieved by clients via a
                # separate path (see ``_register_pool_dhchap_keys_on_node``);
                # the connect command must not embed them. If sfam-2722
                # needs pool keys propagated for a specific case, add a
                # guarded code path that gates on ``pool.dhchap`` and
                # only sets keys the host_entry doesn't already have.
                break
        if not host_entry:
            return False, f"Host NQN {host_nqn} not found in allowed hosts for volume {uuid}"
    elif host_nqn:
        # host_nqn provided but no allowed_hosts — volume allows any host,
        # so just pass host_nqn through without secrets
        pass

    node = db_controller.get_storage_node_by_id(lvol.node_id)
    cluster = db_controller.get_cluster_by_id(node.cluster_id)
    if cluster.status == Cluster.STATUS_SUSPENDED and cluster.snapshot_replication_target_cluster:
        logger.error("Cluster is suspended, looking for replicated lvol")
        for lv in db_controller.get_lvols(cluster.snapshot_replication_target_cluster):
            if lv.nqn == lvol.nqn:
                logger.info(f"LVol with same nqn already exists on target cluster: {lv.get_id()}")
                lvol = lv
                break

    out = []
    nodes_ids = []
    if lvol.ha_type == 'single':
        nodes_ids.append(lvol.node_id)

    elif lvol.ha_type == "ha":
        nodes_ids.extend(lvol.nodes)

    # Get the port from the primary node (first in list) — all nodes hosting
    # the same lvstore must use the same client-facing port.
    primary_snode = db_controller.get_storage_node_by_id(lvol.node_id)
    lvstore_port = primary_snode.get_lvol_subsys_port(lvol.lvs_name)

    for nodes_id in nodes_ids:
        snode = db_controller.get_storage_node_by_id(nodes_id)
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        for nic in snode.data_nics:
            ip = nic.ip4_address
            port = lvstore_port
            transport = "tcp"
            if nic.ip4_address and lvol.fabric == nic.trtype.lower():
                transport = nic.trtype.lower()

            if transport == "tcp":
                keep_alive_to = constants.LVOL_NVME_KEEP_ALIVE_TO_TCP
            else:
                keep_alive_to = constants.LVOL_NVME_KEEP_ALIVE_TO

            client_data_nic_str = ""
            if  cluster.client_data_nic:
                client_data_nic_str = f"--host-iface={cluster.client_data_nic}"

            tls_str = ""
            host_auth_str = ""
            if host_entry:
                host_auth_str = f" --hostnqn={host_nqn}"
                if host_entry.get("psk"):
                    tls_str = " --tls"
                if host_entry.get("dhchap_key"):
                    host_auth_str += f" --dhchap-secret={host_entry['dhchap_key']}"
                if host_entry.get("dhchap_ctrlr_key"):
                    host_auth_str += f" --dhchap-ctrl-secret={host_entry['dhchap_ctrlr_key']}"
            elif host_nqn:
                host_auth_str = f" --hostnqn={host_nqn}"

            connect_cmd = (
                f"sudo nvme connect --reconnect-delay={constants.LVOL_NVME_CONNECT_RECONNECT_DELAY} "
                f"--ctrl-loss-tmo={ctrl_loss_tmo} "
                f"--nr-io-queues={cluster.client_qpair_count} "
                f"--keep-alive-tmo={keep_alive_to} "
                f"--transport={transport} --traddr={ip} --trsvcid={port} --nqn={lvol.nqn} "
                f"{client_data_nic_str}{tls_str}{host_auth_str}"
            )

            entry = {
                "ns_id": lvol.ns_id,
                "transport": transport,
                "ip": ip,
                "port": port,
                "nqn": lvol.nqn,
                "reconnect-delay": constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
                "ctrl-loss-tmo": ctrl_loss_tmo,
                "nr-io-queues": cluster.client_qpair_count,
                "keep-alive-tmo": keep_alive_to,
                "host-iface": cluster.client_data_nic,
                "connect": connect_cmd,
            }

            if host_entry and host_entry.get("psk"):
                entry["tls"] = True
            if lvol.allowed_hosts:
                entry["allowed_hosts"] = [h["nqn"] for h in lvol.allowed_hosts]

            out.append(entry)
    return out, None


def resize_lvol(id, new_size):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    # Block during restart Phase 5
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        if snode.lvstore_status == "in_creation":
            msg = f"Cannot resize lvol {lvol.uuid}: node LVStore restart in progress"
            logger.error(msg)
            return False, msg
    except KeyError:
        pass

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    if active_mig:
        msg = f"Cannot resize lvol {lvol.uuid}: active migration {active_mig.uuid}"
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

    if snode.lvol_sync_del():
        logger.info(f"LVol sync delete task on node: {snode.get_id()}, proceeding with resize")

    logger.info(f"Resizing LVol: {lvol.get_id()}")
    logger.info(f"Current size: {utils.humanbytes(lvol.size)}, new size: {utils.humanbytes(new_size)}")

    size_in_mib = utils.convert_size(new_size, 'MiB')

    rpc_client = snode.rpc_client()

    if lvol.ha_type == "single":

        ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
        if not ret:
            msg = f"Error resizing lvol on node: {snode.get_id()}"
            logger.error(msg)
            return False, msg

    else:
        primary_node = None
        secondary_nodes = []
        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        # Gather all secondary nodes from lvol.nodes[1:]
        all_sec_nodes = []
        for sec_id in lvol.nodes[1:]:
            try:
                all_sec_nodes.append(db_controller.get_storage_node_by_id(sec_id))
            except KeyError:
                pass

        from simplyblock_core.storage_node_ops import check_non_leader_for_operation, queue_for_restart_drain

        # Detect current leader via RPC (no status checks)
        all_nodes = [host_node] + all_sec_nodes
        for candidate in all_nodes:
            try:
                if is_node_leader(candidate, lvol.lvs_name):
                    primary_node = candidate
                    break
            except Exception:
                continue
        if not primary_node:
            primary_node = host_node

        # Check non-leader nodes (no status checks)
        for candidate in all_nodes:
            if candidate.get_id() == primary_node.get_id():
                continue
            action = check_non_leader_for_operation(
                candidate.get_id(), lvol.lvs_name, operation_type="create")
            if action == "reject":
                msg = f"Cannot resize: non-leader {candidate.get_id()[:8]} unreachable but fabric healthy"
                logger.error(msg)
                return False, msg
            elif action == "proceed":
                secondary_nodes.append(candidate)
            elif action == "queue":
                queue_for_restart_drain(
                    candidate.get_id(), lvol.lvs_name,
                    lambda c=candidate: c.rpc_client().bdev_lvol_resize(
                            f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib),
                        f"resize lvol {lvol.uuid} on {candidate.get_id()[:8]}")
            # "skip" — disconnected or pre_block, skip

        if primary_node:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {primary_node.get_id()}")
            rpc_client = primary_node.rpc_client()
            ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            if not ret:
                msg = f"Error resizing lvol on node: {primary_node.get_id()}"
                logger.error(msg)
                return False, msg

        for sec in secondary_nodes:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {sec.get_id()}")
            sec_rpc_client = sec.rpc_client()
            ret = sec_rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            if not ret:
                msg = f"Error resizing lvol on node: {sec.get_id()}"
                logger.error(msg)
                return False, msg

    lvol = db_controller.get_lvol_by_id(id)
    lvol.size = new_size
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")

    return True, None



def set_read_only(id):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(id)
    except KeyError as e:
        logger.error(e)
        return False

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    logger.info(f"Setting LVol: {lvol.get_id()} read only")

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    rpc_client = snode.rpc_client()

    ret = rpc_client.lvol_read_only(lvol.lvol_bdev)
    if not ret:
        return "Error"

    old_status = lvol.mode
    lvol.mode = 'read-only'
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")
    lvol_events.lvol_status_change(lvol, lvol.mode, old_status)

    return True


def create_snapshot(lvol_id, snapshot_name, backup=False):
    return snapshot_controller.add(lvol_id, snapshot_name, backup=backup)


def get_capacity(lvol_uuid, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_uuid)
        pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    except KeyError as e:
        logger.error(e)
        return False

    cap_stats_keys = [
        "date",
        "size_total",
        "size_used",
        "size_free",
        "size_util",
        "size_prov",
        "size_prov_util"
    ]
    prom_client = PromClient(pool.cluster_id)
    records_list = prom_client.get_lvol_metrics(lvol_uuid, cap_stats_keys, history)
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
    try:
        lvol = db_controller.get_lvol_by_id(lvol_uuid)
        pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    except KeyError as e:
        logger.error(e)
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
    prom_client = PromClient(pool.cluster_id)
    records_list = prom_client.get_lvol_metrics(lvol_uuid, io_stats_keys, history)
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
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
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
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    if not lvol.cloned_from_snap:
        logger.error(f"LVol: {lvol_id} must be cloned LVol not regular one")
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    logger.info(f"Inflating LVol: {lvol.get_id()}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    rpc_client = snode.rpc_client()
    ret = rpc_client.bdev_lvol_inflate(lvol.top_bdev)
    if ret:
        lvol.cloned_from_snap = ""
        lvol.write_to_db(db_controller.kv_store)
        logger.info("Done")
    else:
        logger.error(f"Failed to inflate LVol: {lvol_id}")
    return ret

def replication_trigger(lvol_id):
    # create snapshot and replicate it
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    node = db_controller.get_storage_node_by_id(lvol.node_id)
    snapshot_controller.add(lvol_id, f"replication_{uuid.uuid4()}")

    tasks = []
    snaps = []
    out = {
        "lvol": lvol,
        "last_snapshot_id": "",
        "last_replication_time": "",
        "last_replication_duration": "",
        "replicated_count": 0,
        "snaps": [],
        "tasks": [],
    }
    for task in db_controller.get_job_tasks(node.cluster_id):
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            logger.debug(task)
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue

            if snap.lvol.get_id() != lvol_id:
                continue
            snaps.append(snap)
            tasks.append(task)

    if tasks:
        tasks = sorted(tasks, key=lambda x: x.date)
        snaps = sorted(snaps, key=lambda x: x.created_at)
        out["snaps"] = snaps
        out["tasks"] = tasks
        out["replicated_count"] = len(snaps)
        last_task = tasks[-1]
        last_snap = db_controller.get_snapshot_by_id(last_task.function_params["snapshot_id"])
        out["last_snapshot_id"] = last_snap.get_id()
        out["last_replication_time"] = last_task.updated_at
        duration = ""
        if "start_time" in last_task.function_params:
            if "end_time" in last_task.function_params:
                duration = utils.strfdelta_seconds(
                    last_task.function_params["end_time"] - last_task.function_params["start_time"])
            else:
                duration = utils.strfdelta_seconds(int(time.time()) - last_task.function_params["start_time"])
        out["last_replication_duration"] = duration

    return out

def replication_start(lvol_id, replication_cluster_id=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    lvol.do_replicate = True
    if not lvol.replication_node_id:
        excluded_nodes = []
        if lvol.cloned_from_snap:
            lvol_snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            if lvol_snap.source_replicated_snap_uuid:
                try:
                    org_snap = db_controller.get_snapshot_by_id(lvol_snap.source_replicated_snap_uuid)
                    excluded_nodes.append(org_snap.lvol.node_id)
                except KeyError:
                    pass
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        if not replication_cluster_id:
            replication_cluster_id = cluster.snapshot_replication_target_cluster
        if not replication_cluster_id:
            logger.error(f"Cluster: {snode.cluster_id} not replicated")
            return False
        random_nodes = _get_next_3_nodes(replication_cluster_id, lvol.size)
        for r_node in random_nodes:
            if r_node.get_id() not in excluded_nodes:
                logger.info(f"Replicating on node: {r_node.get_id()}")
                lvol.replication_node_id = r_node.get_id()
                lvol.write_to_db()
                break
        if not lvol.replication_node_id:
            logger.error(f"Replication node not found for lvol: {lvol.get_id()}")
            return False
    logger.info("Setting LVol do_replicate: True")

    for snap in db_controller.get_snapshots():
        if snap.lvol.uuid == lvol.uuid:
            if not snap.target_replicated_snap_uuid:
                task = tasks_controller.add_snapshot_replication_task(snap.cluster_id, snap.lvol.node_id, snap.get_id())
                if task:
                    snapshot_events.replication_task_created(snap)
    return True


def list_by_node(node_id=None, is_json=False):
    db_controller = DBController()
    lvols = db_controller.get_lvols()
    lvols = sorted(lvols, key=lambda x: x.create_dt)
    data = []
    for lvol in lvols:
        if node_id:
            if lvol.node_id != node_id:
                continue
        logger.debug(lvol)
        cloned_from_snap = ""
        if lvol.cloned_from_snap:
            snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            cloned_from_snap = snap.snap_uuid
        data.append({
            "UUID": lvol.uuid,
            "BDdev UUID": lvol.lvol_uuid,
            "BlobID": lvol.blobid,
            "Name": lvol.lvol_name,
            "Size": utils.humanbytes(lvol.size),
            "LVS name": lvol.lvs_name,
            "BDev": lvol.lvol_bdev,
            "Node ID": lvol.node_id,
            "Clone From Snap BDev": cloned_from_snap,
            "Created At": lvol.create_dt,
            "Status": lvol.status,
        })
    if is_json:
        return json.dumps(data, indent=2)
    return utils.print_table(data)


def clone_lvol(lvol_id, clone_name, new_size=None, pvc_name=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    host_node = db_controller.get_storage_node_by_id(lvol.node_id)
    lvol_count = len(db_controller.get_lvols_by_node_id(lvol.node_id))
    if lvol_count >= host_node.max_lvol:
        error = f"Too many lvols on node: {host_node.get_id()}, max lvols reached: {lvol_count}"
        logger.error(error)
        return False, error

    snapshot_uuid = None
    for snap in db_controller.get_snapshots_by_node_id(lvol.node_id):
        if snap.snap_name == clone_name:
            logger.info(f"Snapshot with name {clone_name} already exists for this LVol: {snap.uuid}, using it for cloning")
            snapshot_uuid = snap.uuid
            break

    if not snapshot_uuid:
        snapshot_uuid, err = snapshot_controller.add(lvol_id, clone_name, lock=False)
        if err:
            logger.error(err)
            return False, str(err)
    new_lvol_uuid, err = snapshot_controller.clone(
        snapshot_uuid, clone_name, new_size, pvc_name, delete_snap_on_lvol_delete=True, lock=False)
    if err:
        logger.error(err)
        if snapshot_uuid:
                snapshot_controller.delete(snapshot_uuid)
        return False, str(err)

    return new_lvol_uuid, False



def replication_stop(lvol_id, delete=False):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    logger.info("Setting LVol do_replicate: False")
    lvol.do_replicate = False
    lvol.write_to_db()

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    tasks = db_controller.get_job_tasks(snode.cluster_id)


    for task in tasks:
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and task.status != JobSchedule.STATUS_DONE:
            snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            if snap.lvol.uuid == lvol.uuid:
                tasks_controller.cancel_task(task.uuid)

    return True


def replicate_lvol_on_target_cluster(lvol_id):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    if not lvol.replication_node_id:
        logger.error(f"LVol: {lvol_id} replication node id not found")
        return False

    target_node = db_controller.get_storage_node_by_id(lvol.replication_node_id)
    if not target_node:
        logger.error(f"Node not found: {lvol.replication_node_id}")
        return False

    if target_node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online!: {target_node}, status: {target_node.status}")
        return False

    source_node = db_controller.get_storage_node_by_id(lvol.node_id)
    source_cluster = db_controller.get_cluster_by_id(source_node.cluster_id)
    target_cluster = db_controller.get_cluster_by_id(source_cluster.snapshot_replication_target_cluster)

    for lv in db_controller.get_lvols(source_cluster.snapshot_replication_target_cluster):
        if lv.nqn == lvol.nqn:
            logger.info(f"LVol with same nqn already exists on target cluster: {lv.get_id()}")
            return lv.get_id()

    snaps = []
    snapshot = None
    for task in db_controller.get_job_tasks(source_node.cluster_id):
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            logger.debug(task)
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue

            if snap.lvol.get_id() != lvol_id:
                continue
            snaps.append(snap)

    if snaps:
        snaps = sorted(snaps, key=lambda x: x.created_at)
        last_snapshot = snaps[-1]
        rep_snap = db_controller.get_snapshot_by_id(last_snapshot.target_replicated_snap_uuid)
        snapshot = rep_snap

    if not snapshot:
        logger.error(f"Snapshot for replication not found for lvol: {lvol_id}")
        return False

    # create lvol on target node
    new_lvol = copy.deepcopy(lvol)
    new_lvol.uuid = str(uuid.uuid4())
    new_lvol.create_dt = str(datetime.now())
    new_lvol.node_id = target_node.get_id()
    new_lvol.nodes = [target_node.get_id(), target_node.secondary_node_id]
    new_lvol.replication_node_id = ""
    new_lvol.do_replicate = False
    new_lvol.cloned_from_snap = snapshot.get_id()
    new_lvol.pool_uuid = source_cluster.snapshot_replication_target_pool
    new_lvol.lvs_name = target_node.lvstore
    new_lvol.top_bdev = f"{new_lvol.lvs_name}/{new_lvol.lvol_bdev}"
    new_lvol.snapshot_name = snapshot.snap_bdev
    new_lvol.status = LVol.STATUS_IN_CREATION
    new_lvol.nqn = target_cluster.nqn + ":lvol:" + lvol.uuid

    new_lvol.bdev_stack = [
        {
            "type": "bdev_lvol_clone",
            "name": new_lvol.top_bdev,
            "params": {
                "snapshot_name": snapshot.snap_bdev,
                "clone_name": new_lvol.lvol_bdev
            }
        }
    ]

    if new_lvol.crypto_bdev:
        new_lvol.bdev_stack.append({
            "type": "crypto",
            "name": new_lvol.crypto_bdev,
            "params": {
                "name": new_lvol.crypto_bdev,
                "base_name": new_lvol.top_bdev,
                "key1": new_lvol.crypto_key1,
                "key2": new_lvol.crypto_key2,
            }
        })

    new_lvol.write_to_db(db_controller.kv_store)

    lvol_bdev, error = add_lvol_on_node(new_lvol, target_node)
    if error:
        logger.error(error)
        new_lvol.remove(db_controller.kv_store)
        return False, error

    new_lvol.lvol_uuid = lvol_bdev['uuid']
    new_lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

    secondary_node = db_controller.get_storage_node_by_id(target_node.secondary_node_id)
    if secondary_node.status == StorageNode.STATUS_ONLINE:
        lvol_bdev, error = add_lvol_on_node(new_lvol, secondary_node, is_primary=False)
        if error:
            logger.error(error)
            # remove lvol from primary
            ret = delete_lvol_from_node(new_lvol, target_node)
            if not ret:
                logger.error("")
            new_lvol.remove(db_controller.kv_store)
            return False, error

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.write_to_db(db_controller.kv_store)
    lvol = db_controller.get_lvol_by_id(lvol_id)
    lvol.from_source = False
    lvol.write_to_db()
    lvol_events.lvol_replicated(lvol, new_lvol)

    return new_lvol.lvol_uuid


def list_replication_tasks(lvol_id):
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    node = db_controller.get_storage_node_by_id(lvol.node_id)
    tasks = []
    for task in db_controller.get_job_tasks(node.cluster_id):
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue
            if snap.lvol.get_id() != lvol_id:
                continue
            tasks.append(task)

    return tasks


def suspend_lvol(lvol_id):

    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    logger.info(f"suspending LVol subsystem: {lvol.get_id()}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    for iface in snode.data_nics:
        if iface.ip4_address and lvol.fabric == iface.trtype.lower():
            logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
            ret = snode.rpc_client().nvmf_subsystem_listener_set_ana_state(lvol.nqn, iface.ip4_address, lvol.subsys_port, ana="inaccessible")
            if not ret:
                logger.error(f"Failed to set subsystem listener state for {lvol.nqn} on {iface.ip4_address}")
                return False

    if snode.secondary_node_id:
        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if sec_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED]:
            for iface in sec_node.data_nics:
                if iface.ip4_address and lvol.fabric == iface.trtype.lower():
                    logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
                    ret = sec_node.rpc_client().nvmf_subsystem_listener_set_ana_state(lvol.nqn, iface.ip4_address, lvol.subsys_port, ana="inaccessible")
                    if not ret:
                        logger.error(f"Failed to set subsystem listener state for {lvol.nqn} on {iface.ip4_address}")
                        return False

    return True


def resume_lvol(lvol_id):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    logger.info(f"suspending LVol subsystem: {lvol.get_id()}")
    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    for iface in snode.data_nics:
        if iface.ip4_address and lvol.fabric == iface.trtype.lower():
            logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
            ret = snode.rpc_client().nvmf_subsystem_listener_set_ana_state(
                lvol.nqn, iface.ip4_address, lvol.subsys_port, is_optimized=True)
            if not ret:
                logger.error(f"Failed to set subsystem listener state for {lvol.nqn} on {iface.ip4_address}")
                return False

    if snode.secondary_node_id:
        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if sec_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED]:
            for iface in sec_node.data_nics:
                if iface.ip4_address and lvol.fabric == iface.trtype.lower():
                    logger.info("adding listener for %s on IP %s" % (lvol.nqn, iface.ip4_address))
                    ret = sec_node.rpc_client().nvmf_subsystem_listener_set_ana_state(
                        lvol.nqn, iface.ip4_address, lvol.subsys_port, is_optimized=False)
                    if not ret:
                        logger.error(f"Failed to set subsystem listener state for {lvol.nqn} on {iface.ip4_address}")
                        return False

    return True


def replicate_lvol_on_source_cluster(lvol_id, cluster_id=None, pool_uuid=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    source_node = None
    new_source_cluster = None
    try:
        source_node = db_controller.get_storage_node_by_id(lvol.node_id)
    except KeyError:
        pass
    if cluster_id and (source_node is None or source_node.cluster_id != cluster_id):
        new_source_cluster = db_controller.get_cluster_by_id(cluster_id)
        if new_source_cluster.status != Cluster.STATUS_ACTIVE:
            logger.error(f"Cluster is not active: {cluster_id}")
            return False
        # get new source node from the new cluster
        nodes = _get_next_3_nodes(new_source_cluster.get_id(), lvol.size)
        if not nodes:
            return False, "No nodes found with enough resources to create the LVol"
        source_node = nodes[0]

    if not source_node:
        logger.error(f"Node not found: {lvol.node_id}")
        return False

    if source_node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online!: {source_node.get_id()}, status: {source_node.status}")
        return False


    snaps = []
    snapshot = None
    for task in db_controller.get_job_tasks(source_node.cluster_id):
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
            logger.debug(task)
            try:
                snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
            except KeyError:
                continue

            if snap.lvol.get_id() != lvol_id:
                continue
            snaps.append(snap)

    if snaps:
        snaps = sorted(snaps, key=lambda x: x.created_at)
        snapshot = snaps[-1]

    if not snapshot:
        target_node = db_controller.get_storage_node_by_id(lvol.replication_node_id)
        logger.info(f"Looking for snapshot in target cluster: {target_node.cluster_id}")
        target_lvol_id = None
        lvol_id_in_nqn = lvol.nqn.split(":")[-1]
        for lv in db_controller.get_lvols(target_node.cluster_id):
            if lv.nqn.split(":")[-1] == lvol_id_in_nqn:
                logger.info(f"LVol with same lvol nqn already exists on target cluster: {lv.get_id()}")
                target_lvol_id = lv.get_id()

        if not target_lvol_id:
            logger.error(f"LVol with same nqn does not exist on target cluster: {target_node.cluster_id}")
            return False

        for task in db_controller.get_job_tasks(target_node.cluster_id):
            if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
                logger.debug(task)
                try:
                    snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
                except KeyError:
                    continue

                if snap.lvol.get_id() != target_lvol_id:
                    continue
                snaps.append(snap)

        if snaps:
            logger.debug(snaps)
            snaps = sorted(snaps, key=lambda x: x.created_at)
            snapshot = snaps[-1]
            snapshot = db_controller.get_snapshot_by_id(snapshot.target_replicated_snap_uuid)

    if not snapshot:
        logger.error(f"Snapshot for replication not found for lvol: {lvol_id}")
        return False

    # create lvol on target node
    new_lvol = copy.deepcopy(lvol)
    new_lvol.cloned_from_snap = snapshot.get_id()
    new_lvol.snapshot_name = snapshot.snap_bdev
    new_lvol.from_source = True
    new_lvol.node_id = source_node.get_id()
    new_lvol.nodes = [source_node.get_id(), source_node.secondary_node_id]
    new_lvol.status = LVol.STATUS_IN_CREATION
    new_lvol.vuid = utils.get_random_vuid()
    new_lvol.lvol_bdev = f"LVOL_{new_lvol.vuid}"
    new_lvol.lvs_name = source_node.lvstore
    new_lvol.top_bdev = f"{new_lvol.lvs_name}/{new_lvol.lvol_bdev}"
    if pool_uuid:
        new_pool = db_controller.get_pool_by_id(pool_uuid)
        new_lvol.pool_uuid = new_pool.get_id()
        new_lvol.pool_name = new_pool.pool_name
    if new_source_cluster:
        new_lvol.nqn = new_source_cluster.nqn + ":lvol:" + new_lvol.uuid
    new_lvol.bdev_stack = [
        {
            "type": "bdev_lvol_clone",
            "name": new_lvol.top_bdev,
            "params": {
                "snapshot_name": snapshot.snap_bdev,
                "clone_name": new_lvol.lvol_bdev
            }
        }
    ]

    if new_lvol.crypto_bdev:
        new_lvol.bdev_stack.append({
            "type": "crypto",
            "name": new_lvol.crypto_bdev,
            "params": {
                "name": new_lvol.crypto_bdev,
                "base_name": new_lvol.top_bdev,
                "key1": new_lvol.crypto_key1,
                "key2": new_lvol.crypto_key2,
            }
        })

    new_lvol.write_to_db(db_controller.kv_store)

    logger.debug(f"new lvol from_source: {new_lvol.from_source}")

    lvol_bdev, error = add_lvol_on_node(new_lvol, source_node)
    if error:
        logger.error(error)
        new_lvol.remove(db_controller.kv_store)
        return False, error

    new_lvol.lvol_uuid = lvol_bdev['uuid']
    new_lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

    secondary_node = db_controller.get_storage_node_by_id(source_node.secondary_node_id)
    if secondary_node.status == StorageNode.STATUS_ONLINE:
        lvol_bdev, error = add_lvol_on_node(new_lvol, secondary_node, is_primary=False)
        if error:
            logger.error(error)
            # remove lvol from primary
            ret = delete_lvol_from_node(new_lvol, source_node)
            if not ret:
                logger.error("")
            new_lvol.remove(db_controller.kv_store)
            return False, error

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.from_source = True
    new_lvol.write_to_db(db_controller.kv_store)
    lvol_events.lvol_replicated(lvol, new_lvol)
    logger.debug(f"new lvol from_source: {new_lvol.from_source}")

    return new_lvol.lvol_uuid


def _build_host_entries(allowed_hosts, sec_options=None):
    """Build the allowed_hosts list with auto-generated keys.

    Args:
        allowed_hosts: list of host NQN strings
        sec_options: dict with optional keys 'dhchap_key', 'dhchap_ctrlr_key', 'psk'
                     indicating which key types to generate

    Returns:
        list of dicts or (False, error_message) tuple on validation error
    """
    if sec_options:
        ok, err = utils.validate_sec_options(sec_options)
        if not ok:
            return False, err

    entries = []
    for host_nqn in allowed_hosts:
        entry = {"nqn": host_nqn}
        if sec_options:
            if "dhchap_key" in sec_options:
                entry["dhchap_key"] = utils.generate_dhchap_key()
            if "dhchap_ctrlr_key" in sec_options:
                entry["dhchap_ctrlr_key"] = utils.generate_dhchap_key()
            if "psk" in sec_options:
                entry["psk"] = utils.generate_psk_key()
        entries.append(entry)
    return entries


def add_host_to_lvol(lvol_id, host_nqn):
    """Add an allowed host to a volume's subsystem.

    For DHCHAP pools the pool's shared key pair is used automatically.
    For non-DHCHAP pools, security options are inherited from pool.sec_options.
    Returns a dict with the host NQN (and any per-host keys for non-DHCHAP pools),
    or (False, error_message) on failure.
    """
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    # Check for duplicate
    for h in lvol.allowed_hosts:
        if h["nqn"] == host_nqn:
            return False, f"Host {host_nqn} is already allowed"

    # Resolve pool
    pool = None
    if lvol.pool_uuid:
        try:
            pool = db_controller.get_pool_by_id(lvol.pool_uuid)
        except KeyError:
            pass

    entry = {"nqn": host_nqn}

    if pool and pool.dhchap:
        # Pool-level DHCHAP: use pool's shared key pair, no per-host key generation
        dhchap_group = constants.DHCHAP_DHGROUP
        for node_id in lvol.nodes:
            snode = db_controller.get_storage_node_by_id(node_id)
            if snode.status != StorageNode.STATUS_ONLINE:
                continue
            rpc_client = snode.rpc_client()
            pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
            ret = rpc_client.subsystem_add_host(
                lvol.nqn, host_nqn,
                dhchap_key=pool_key_names.get("dhchap_key"),
                dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                dhchap_group=dhchap_group,
            )
            if not ret:
                return False, f"Failed to add host {host_nqn} on node {node_id}"
    else:
        # Legacy per-host key generation from pool.sec_options
        sec_options = pool.sec_options if pool else None
        if sec_options:
            ok, err = utils.validate_sec_options(sec_options)
            if not ok:
                return False, err
            if "dhchap_key" in sec_options:
                entry["dhchap_key"] = utils.generate_dhchap_key()
            if "dhchap_ctrlr_key" in sec_options:
                entry["dhchap_ctrlr_key"] = utils.generate_dhchap_key()
            if "psk" in sec_options:
                entry["psk"] = utils.generate_psk_key()

        has_keys = any(entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
        dhchap_group = "null"
        if has_keys and lvol.nodes:
            first_node = db_controller.get_storage_node_by_id(lvol.nodes[0])
            cluster = db_controller.get_cluster_by_id(first_node.cluster_id)
            dhchap_group = _get_dhchap_group(cluster)
        for node_id in lvol.nodes:
            snode = db_controller.get_storage_node_by_id(node_id)
            if snode.status != StorageNode.STATUS_ONLINE:
                continue
            rpc_client = snode.rpc_client()
            if has_keys:
                key_names = _register_dhchap_keys_on_node(snode, host_nqn, entry, rpc_client)
                ret = rpc_client.subsystem_add_host(
                    lvol.nqn, host_nqn,
                    psk=key_names.get("psk"),
                    dhchap_key=key_names.get("dhchap_key"),
                    dhchap_ctrlr_key=key_names.get("dhchap_ctrlr_key"),
                    dhchap_group=dhchap_group,
                )
            else:
                ret = rpc_client.subsystem_add_host(lvol.nqn, host_nqn)
            if not ret:
                return False, f"Failed to add host {host_nqn} on node {node_id}"

    lvol.allowed_hosts.append(entry)
    lvol.write_to_db(db_controller.kv_store)
    logger.info(f"Added host {host_nqn} to lvol {lvol_id}")
    return entry, None


def get_host_secret(lvol_id, host_nqn):
    """Return the security credentials for a specific host on a volume.

    Returns (dict, None) on success or (False, error) on failure.
    """
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    for h in (lvol.allowed_hosts or []):
        if h["nqn"] == host_nqn:
            return h, None

    return False, f"Host {host_nqn} is not in the allowed list for volume {lvol_id}"


def remove_host_from_lvol(lvol_id, host_nqn):
    """Remove an allowed host from a volume's subsystem."""
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

    found = False
    for h in lvol.allowed_hosts:
        if h["nqn"] == host_nqn:
            found = True
            break

    if not found:
        return False, f"Host {host_nqn} is not in the allowed list"

    # Find host entry to get key info before removal
    host_entry = None
    for h in lvol.allowed_hosts:
        if h["nqn"] == host_nqn:
            host_entry = h
            break

    safe_host = host_nqn.replace(":", "_").replace(".", "_")
    errors = []

    # Remove from all nodes where the subsystem exists
    for node_id in lvol.nodes:
        snode = db_controller.get_storage_node_by_id(node_id)
        if snode.status != StorageNode.STATUS_ONLINE:
            continue
        rpc_client = snode.rpc_client()
        ret = rpc_client.subsystem_remove_host(lvol.nqn, host_nqn)
        if not ret:
            logger.error("Failed to remove host %s from node %s", host_nqn, node_id)
            errors.append(node_id)

        # Clean up keyring keys
        for key_type in ("dhchap_key", "dhchap_ctrlr_key", "psk"):
            if host_entry and host_entry.get(key_type):
                key_name = f"{key_type}_{safe_host}"
                rpc_client.keyring_file_remove_key(key_name)

    lvol.allowed_hosts = [h for h in lvol.allowed_hosts if h["nqn"] != host_nqn]
    lvol.write_to_db(db_controller.kv_store)
    logger.info(f"Removed host {host_nqn} from lvol {lvol_id}")

    if errors:
        return True, f"Warning: SPDK remove_host failed on nodes: {', '.join(errors)}"
    return True, None


def get_master_lvols_by_pool_uuid(pool_id, is_json=False):
    db_controller = DBController()
    lvols = db_controller.get_lvols_by_pool_id(pool_id)

    data = []

    for lvol in lvols:
        if lvol.deleted:
            continue
        if lvol.namespace:
            continue

        lvol_data = {
            "Id": lvol.uuid,
            "Name": lvol.lvol_name,
            "Size": utils.humanbytes(lvol.size),
            "Hostname": lvol.hostname,
            "Status": lvol.status,
            "Namespaces": len(db_controller.get_lvols_by_namespace(lvol.uuid)),
            "MaxNamespaces": lvol.max_namespace_per_subsys,
        }
        data.append(lvol_data)

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)
