# coding=utf-8
import copy
import logging as lg
import json
import math
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
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient

logger = lg.getLogger()


def _get_dhchap_group(cluster):
    """Return the DH group to set on the target subsystem for DH-HMAC-CHAP.

    Uses the first group from cluster.tls_config.dhchap_dhgroups if configured,
    otherwise returns 'null' (HMAC-CHAP only, no DH key exchange).
    """
    if cluster and cluster.tls and cluster.tls_config:
        params = cluster.tls_config.get("params", cluster.tls_config)
        groups = params.get("dhchap_dhgroups") or []
        if groups:
            return groups[0]
    return "null"


def _register_dhchap_keys_on_node(snode, host_nqn, host_entry, rpc_client):
    """Write DHCHAP key files to a storage node and register them in SPDK's keyring.

    Returns a dict mapping key type ('dhchap_key', 'dhchap_ctrlr_key', 'psk')
    to the SPDK keyring name for use in subsystem_add_host.
    """
    snode_api = SNodeClient(snode.api_endpoint)
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
                logger.warning(f"LVol sync delete task found on node: {node.get_id()}, skipping")
                continue
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
            logger.error(f"LVol sync deletion found on node: {host_node.get_id()}")
            return False, f"LVol sync deletion found on node: {host_node.get_id()}"

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

    # round size to the nearest 1G ceiling
    size = math.ceil(size/(1024*1024*1024))*1024*1024*1024

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
    # Security options are inherited from the pool
    if allowed_hosts and not namespace:
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
        # Build nodes list with all secondaries
        secondary_ids = [host_node.secondary_node_id]
        if host_node.secondary_node_id_2:
            secondary_ids.append(host_node.secondary_node_id_2)
        lvol.nodes = [host_node.get_id()] + secondary_ids

        primary_node = None
        secondary_nodes = []
        sec_node = db_controller.get_storage_node_by_id(host_node.secondary_node_id)
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if sec_node.status == StorageNode.STATUS_DOWN:
                    msg = "Secondary node is in down status, can not create lvol"
                    logger.error(msg)
                    lvol.remove(db_controller.kv_store)
                    return False, msg
                elif sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_nodes.append(sec_node)

            elif sec_node.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(sec_node, lvol.lvs_name):
                    primary_node = sec_node
                    secondary_nodes.append(host_node)
                else:
                    # both nodes are non leaders and online, set primary as leader
                    primary_node = host_node
                    secondary_nodes.append(sec_node)

            else:
                # sec node is not online, set primary as leader
                primary_node = host_node

        elif sec_node.status == StorageNode.STATUS_ONLINE:
            # primary is not online but secondary is, create on secondary and set leader if needed,
            primary_node = sec_node

        else:
            # Primary and first secondary are both offline.
            # Check if second secondary (FTT=2) is online.
            for extra_sec_id in secondary_ids[1:]:
                try:
                    extra_sec = db_controller.get_storage_node_by_id(extra_sec_id)
                    if extra_sec.status == StorageNode.STATUS_ONLINE:
                        primary_node = extra_sec
                        break
                except KeyError:
                    pass
            if not primary_node:
                msg = "Host nodes are not online"
                logger.error(msg)
                lvol.remove(db_controller.kv_store)
                return False, msg

        # Add additional secondaries (secondary_node_id_2, etc.) if online
        for extra_sec_id in secondary_ids[1:]:
            try:
                extra_sec = db_controller.get_storage_node_by_id(extra_sec_id)
                if extra_sec.status == StorageNode.STATUS_ONLINE and extra_sec.get_id() != (primary_node.get_id() if primary_node else None):
                    secondary_nodes.append(extra_sec)
            except KeyError:
                pass

        if primary_node:
            lvol_bdev, error = add_lvol_on_node(lvol, primary_node)
            if error:
                logger.error(error)
                lvol.remove(db_controller.kv_store)
                return False, error

            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

        for sec_idx, sec in enumerate(secondary_nodes):
            sec = db_controller.get_storage_node_by_id(sec.get_id())
            if sec.status == StorageNode.STATUS_ONLINE:
                lvol_bdev, error = add_lvol_on_node(lvol, sec, is_primary=False, secondary_index=sec_idx)
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

    if pool.has_qos():
        connect_lvol_to_pool(lvol.uuid)

    # set QOS
    if max_rw_iops >= 0 or max_rw_mbytes >= 0 or max_r_mbytes >= 0 or max_w_mbytes >= 0:
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


def add_lvol_on_node(lvol, snode, is_primary=True, secondary_index=0):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

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
                                          max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS,
                                          allow_any_host=allow_any)

        # add allowed hosts to subsystem
        if lvol.allowed_hosts:
            db_ctrl = DBController()
            cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
            dhchap_group = _get_dhchap_group(cluster)
            for host_entry in lvol.allowed_hosts:
                logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
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
    allow_any = not bool(lvol.allowed_hosts)
    logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
    rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                max_namespaces=constants.LVO_MAX_NAMESPACES_PER_SUBSYS,
                                allow_any_host=allow_any)

    # Re-apply allowed hosts on subsystem recreate
    if lvol.allowed_hosts:
        db_ctrl = DBController()
        cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
        dhchap_group = _get_dhchap_group(cluster)
        for host_entry in lvol.allowed_hosts:
            logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
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


def delete_lvol_from_node(lvol_id, node_id, clear_data=True, del_async=False):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return True

    logger.info(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=5, retry=2)

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

    if lvol.ha_type == 'single':
        if snode.status  != StorageNode.STATUS_ONLINE:
            logger.error(f"Node status is not online, node: {snode.get_id()}, status: {snode.status}")
            if not force_delete:
                return False

        ret = delete_lvol_from_node(lvol.get_id(), lvol.node_id)
        if not ret:
            return False


    elif lvol.ha_type == "ha":

        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        # Gather all secondary nodes from lvol.nodes[1:]
        all_sec_nodes = []
        for sec_id in lvol.nodes[1:]:
            try:
                all_sec_nodes.append(db_controller.get_storage_node_by_id(sec_id))
            except KeyError:
                pass

        primary_node = None
        secondary_nodes = []

        # Find at least one online secondary to verify status
        first_sec = all_sec_nodes[0] if all_sec_nodes else None
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if first_sec and first_sec.status == StorageNode.STATUS_DOWN:
                    msg = "Secondary node is in down status, can not delete lvol"
                    logger.error(msg)
                    return False, msg
                for sn in all_sec_nodes:
                    if sn.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(sn)

            elif first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(first_sec, lvol.lvs_name):
                    primary_node = first_sec
                    if host_node.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(host_node)
                    for sn in all_sec_nodes[1:]:
                        if sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(sn)
                else:
                    primary_node = host_node
                    for sn in all_sec_nodes:
                        if sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(sn)

            else:
                primary_node = host_node

        elif first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            primary_node = first_sec
            # Add remaining online secondaries (second_sec etc.) for cleanup
            for sn in all_sec_nodes[1:]:
                if sn.status == StorageNode.STATUS_ONLINE:
                    secondary_nodes.append(sn)

        else:
            # Primary and first secondary are both offline.
            # Check if any other secondary (e.g. second_sec in FTT=2) is online.
            for sn in all_sec_nodes[1:]:
                if sn.status == StorageNode.STATUS_ONLINE:
                    primary_node = sn
                    # Add remaining online secondaries for cleanup
                    for other_sn in all_sec_nodes:
                        if other_sn.get_id() != sn.get_id() and other_sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(other_sn)
                    break
            if not primary_node:
                msg = "Host nodes are not online"
                logger.error(msg)
                return False, msg

        # 1- delete subsystem from all secondaries
        for sec in secondary_nodes:
            sec = db_controller.get_storage_node_by_id(sec.get_id())
            if sec.status == StorageNode.STATUS_ONLINE:
                secondary_rpc_client = sec.rpc_client()
                subsystem = secondary_rpc_client.subsystem_list(lvol.nqn)
                if subsystem:
                    if len(subsystem[0]["namespaces"]) > 1:
                        logger.info("Removing namespace")
                        ret = secondary_rpc_client.nvmf_subsystem_remove_ns(lvol.nqn, lvol.ns_id)
                    else:
                        logger.info(f"Deleting subsystem for lvol:{lvol.get_id()} from node:{sec.get_id()}")
                        ret = secondary_rpc_client.subsystem_delete(lvol.nqn)
                    if not ret:
                        logger.warning(f"Failed to delete subsystem from node: {sec.get_id()}")

        # 2- delete subsystem and lvol bdev from primary
        if primary_node:

            ret = delete_lvol_from_node(lvol.get_id(), primary_node.get_id())
            if not ret:
                logger.error(f"Failed to delete lvol from node: {primary_node.get_id()}")
                if not force_delete:
                    return False

    lvol = db_controller.get_lvol_by_id(lvol.get_id())
    # set status
    old_status = lvol.status
    lvol.status = LVol.STATUS_IN_DELETION
    lvol.write_to_db()
    try:
        lvol_events.lvol_status_change(lvol, lvol.status, old_status)
    except KeyError:
        pass

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
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

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
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

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
    if snode.secondary_node_id_2:
        secondary_ids.append(snode.secondary_node_id_2)
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

    snap_dict : dict[str, int] = {}
    for lvol in lvols:
        logger.debug(lvol)
        if lvol.deleted is True and all is False:
            continue
        cloned_snapped = lvol.cloned_from_snap
        if cloned_snapped:
            snap_dict[cloned_snapped] = snap_dict.get(cloned_snapped, 0) + 1
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

    for snap, count in snap_dict.items():
        ref_snap = db_controller.get_snapshot_by_id(snap)
        ref_snap.ref_count = count
        ref_snap.write_to_db(db_controller.kv_store)

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
    except KeyError as e:
        logger.error(e)
        return False

    # Look up host entry for secrets when host_nqn is provided
    host_entry = None
    if lvol.allowed_hosts:
        if not host_nqn:
            logger.error(f"Volume {uuid} has allowed hosts configured; --host-nqn is required")
            return False
        for h in lvol.allowed_hosts:
            if h["nqn"] == host_nqn:
                host_entry = h
                break
        if not host_entry:
            logger.error(f"Host NQN {host_nqn} not found in allowed hosts for volume {uuid}")
            return False
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
    return out


def resize_lvol(id, new_size):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(id)
    except KeyError as e:
        logger.error(e)
        return False, str(e)

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

    # round size to the nearest 1G ceiling
    new_size = math.ceil(new_size/(1024*1024*1024))*1024*1024*1024

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
        logger.error(f"LVol sync deletion found on node: {snode.get_id()}")
        return False, f"LVol sync deletion found on node: {snode.get_id()}"

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
        secondary_nodes = []
        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        # Gather all secondary nodes from lvol.nodes[1:]
        all_sec_nodes = []
        for sec_id in lvol.nodes[1:]:
            try:
                all_sec_nodes.append(db_controller.get_storage_node_by_id(sec_id))
            except KeyError:
                pass

        first_sec = all_sec_nodes[0] if all_sec_nodes else None
        if host_node.status == StorageNode.STATUS_ONLINE:

            if is_node_leader(host_node, lvol.lvs_name):
                primary_node = host_node
                if first_sec and first_sec.status == StorageNode.STATUS_DOWN:
                    msg = "Secondary node is in down status, can not resize lvol"
                    logger.error(msg)
                    return False, msg
                for sn in all_sec_nodes:
                    if sn.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(sn)

            elif first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
                if is_node_leader(first_sec, lvol.lvs_name):
                    primary_node = first_sec
                    if host_node.status == StorageNode.STATUS_ONLINE:
                        secondary_nodes.append(host_node)
                    for sn in all_sec_nodes[1:]:
                        if sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(sn)
                else:
                    primary_node = host_node
                    for sn in all_sec_nodes:
                        if sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(sn)

            else:
                primary_node = host_node

        elif first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            primary_node = first_sec
            for sn in all_sec_nodes[1:]:
                if sn.status == StorageNode.STATUS_ONLINE:
                    secondary_nodes.append(sn)

        else:
            # Primary and first secondary are both offline.
            # Check if any other secondary (e.g. second_sec in FTT=2) is online.
            for sn in all_sec_nodes[1:]:
                if sn.status == StorageNode.STATUS_ONLINE:
                    primary_node = sn
                    for other_sn in all_sec_nodes:
                        if other_sn.get_id() != sn.get_id() and other_sn.status == StorageNode.STATUS_ONLINE:
                            secondary_nodes.append(other_sn)
                    break
            if not primary_node:
                msg = "Host nodes are not online"
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

        for sec in secondary_nodes:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {sec.get_id()}")
            sec = db_controller.get_storage_node_by_id(sec.get_id())
            if sec.status == StorageNode.STATUS_ONLINE:

                sec_rpc_client = RPCClient(sec.mgmt_ip, sec.rpc_port, sec.rpc_username,
                                           sec.rpc_password)

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

    Security options are inherited from the volume's pool.
    Returns a dict with the host NQN and any auto-generated keys, or (False, error).
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

    # Get sec_options from the pool
    sec_options = None
    if lvol.pool_uuid:
        try:
            pool = db_controller.get_pool_by_id(lvol.pool_uuid)
            sec_options = pool.sec_options or None
        except KeyError:
            pass

    entry = {"nqn": host_nqn}
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

    # Apply to all nodes where the subsystem exists
    has_keys = any(entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
    # Resolve DH group from cluster config (LVol has no cluster_id, get from first node)
    dhchap_group = "null"
    if has_keys and lvol.nodes:
        first_node = db_controller.get_storage_node_by_id(lvol.nodes[0])
        cluster = db_controller.get_cluster_by_id(first_node.cluster_id)
        dhchap_group = _get_dhchap_group(cluster)
    for node_id in lvol.nodes:
        snode = db_controller.get_storage_node_by_id(node_id)
        if snode.status != StorageNode.STATUS_ONLINE:
            continue
        rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
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
        rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
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


def clone_lvol(lvol_id, clone_name, new_size=None, pvc_name=None):
    db_controller = DBController()
    try:
        db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    try:
        snapshot_uuid = None
        for snap in db_controller.get_snapshots_by_lvol_id(lvol_id):
            if snap.snap_name == clone_name:
                logger.info(f"Snapshot with name {clone_name} already exists for this LVol: {snap.snap_uuid}, using it for cloning")
                snapshot_uuid = snap.snap_uuid
                break
        if not snapshot_uuid:
            for i in range(10):
                snapshot_uuid, err = snapshot_controller.add(lvol_id, clone_name)
                if err:
                    logger.error(err)
                    time.sleep(1)
                    continue
            else:
                if not snapshot_uuid:
                    logger.error("Failed to create snapshot for clone after 10 attempts")
                    return False
        new_lvol_uuid = None
        for i in range(10):
            new_lvol_uuid, err = snapshot_controller.clone(
                snapshot_uuid, clone_name, new_size, pvc_name, delete_snap_on_lvol_delete=True)
            if err:
                logger.error(err)
                time.sleep(1)
                continue
        else:
            if not new_lvol_uuid:
                logger.error("Failed to clone lvol after 10 attempts")
                if snapshot_uuid:
                    snapshot_controller.delete(snapshot_uuid)
                return False

        return new_lvol_uuid
    except Exception as e:
        logger.error(e)
        return False