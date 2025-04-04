# coding=utf-8
import json
import logging
import math
import os
import random
import re
import string
import subprocess
import sys

import docker
from prettytable import PrettyTable
from graypy import GELFTCPHandler

from simplyblock_core import constants
from simplyblock_core import shell_utils
from simplyblock_core.models.job_schedule import JobSchedule

CONFIG_KEYS = [
    "app_thread_core",
    "jm_cpu_core",
    "poller_cpu_cores",
    "alceml_cpu_cores",
    "alceml_worker_cpu_cores",
    "distrib_cpu_cores",
    "jc_singleton_core",
]

def get_env_var(name, default=None, is_required=False):
    if not name:
        logger.warning("Invalid env var name %s", name)
        return False
    if name not in os.environ and is_required:
        logger.error("env value is required: %s" % name)
        raise Exception("env value is required: %s" % name)
    return os.environ.get(name, default)


def get_baseboard_sn():
    # out, _, _ = shell_utils.run_command("dmidecode -s baseboard-serial-number")
    return get_system_id()


def get_system_id():
    out, _, _ = shell_utils.run_command("dmidecode -s system-uuid")
    return out


def get_hostname():
    out, _, _ = shell_utils.run_command("hostname -s")
    return out


def get_ips():
    out, _, _ = shell_utils.run_command("hostname -I")
    return out


def get_nics_data():
    try:
        out, _, _ = shell_utils.run_command("ip -j address show")
        data = json.loads(out)
        def _get_ip4_address(list_of_addr):
            if list_of_addr:
                for data in list_of_addr:
                    if data['family'] == 'inet':
                        return data['local']
            return ""

        devices = {i["ifname"]: i for i in data}
        iface_list = {}
        for nic in devices:
            device = devices[nic]
            iface = {
                'name': device['ifname'],
                'ip': _get_ip4_address(device['addr_info']),
                'status': device['operstate'],
                'net_type': device['link_type']}
            iface_list[nic] = iface
        return iface_list
    except Exception as e:
        logger.error(e)
        return False


def get_iface_ip(ifname):
    if not ifname:
        return False
    out = get_nics_data()
    if out and ifname in out:
        return out[ifname]['ip']
    return False


def print_table(data: list, title=None):
    if data:
        x = PrettyTable(field_names=data[0].keys(), max_width=70, title=title)
        x.align = 'l'
        for node_data in data:
            row = []
            for key in node_data:
                row.append(node_data[key])
            x.add_row(row)
        return x.__str__()


def humanbytes(B):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string."""
    if not B:
        return "0"
    B = float(B)
    KB = float(constants.ONE_KB)
    MB = float(KB ** 2) # 1,048,576
    GB = float(KB ** 3) # 1,073,741,824
    TB = float(KB ** 4) # 1,099,511,627,776

    if B < KB:
        return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
    elif KB <= B < MB:
        return '{0:.1f} KB'.format(B / KB)
    elif MB <= B < GB:
        return '{0:.1f} MB'.format(B / MB)
    elif GB <= B < TB:
        return '{0:.1f} GB'.format(B / GB)
    elif TB <= B:
        return '{0:.1f} TB'.format(B / TB)


def generate_string(length):
    return ''.join(random.SystemRandom().choice(
        string.ascii_letters + string.digits) for _ in range(length))


def get_docker_client(cluster_id=None):
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes()
    if not nodes:
        logger.error("No mgmt nodes was found in the cluster!")
        return False

    docker_ips = [node.docker_ip_port for node in nodes]

    for ip in docker_ips:
        try:
            c = docker.DockerClient(base_url=f"tcp://{ip}", version="auto")
            return c
        except Exception as e:
            print(e)
            raise e
    return False


def dict_agg(data, mean=False, keys=None):
    out = {}
    if not keys and data:
        keys = data[0].keys()
    for d in data:
        for key in keys:
            if isinstance(d[key], int) or isinstance(d[key], float):
                if key in out:
                    out[key] += d[key]
                else:
                    out[key] = d[key]
    if out and mean:
        count = len(data)
        if count > 1:
            for key in out:
                out[key] = int(out[key]/count)
    return out


def get_weights(node_stats, cluster_stats):
    """"
    node_st = {
            "lvol": len(node.lvols),
            "cpu": cpuinfo.get_cpu_info()['count']*cpuinfo.get_cpu_info()['hz_advertised'][0],
            "r_io": 0,
            "w_io": 0,
            "r_b": 0,
            "w_b": 0}
    """

    def _normalize_w(key, v):
        if key in constants.weights:
            return round(((v * constants.weights[key]) / 100), 2)
        else:
            return v

    def _get_key_w(node_id, key):
        w = 0
        if cluster_stats[key] > 0:
            w = (cluster_stats[key]/node_stats[node_id][key])*10
            # if key in ["lvol", "r_io", "w_io", "r_b", "w_b"]:  # get reverse value
            #     w = (cluster_stats[key]/node_stats[node_id][key]) * 100
        return w

    out = {}
    heavy_node_w = 0
    heavy_node_id = None
    for node_id in node_stats:
        out[node_id] = {}
        total = 0
        for key in cluster_stats:
            w = _get_key_w(node_id, key)
            w = _normalize_w(key, w)
            out[node_id][key] = w
            total += w
        out[node_id]['total'] = int(total)
        if total > heavy_node_w:
            heavy_node_w = total
            heavy_node_id = node_id

    if heavy_node_id:
        out[heavy_node_id]['total'] *= 5

    return out


def print_table_dict(node_stats):
    d = []
    for node_id in node_stats:
        data = {"node_id": node_id}
        data.update(node_stats[node_id])
        d.append(data)
    print(print_table(d))


def generate_rpc_user_and_pass():
    def _generate_string(length):
        return ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(length))

    return _generate_string(8), _generate_string(16)


def parse_history_param(history_string):
    if not history_string:
        logger.error("Invalid history value")
        return False

    # process history
    results = re.search(r'^(\d+[hmd])(\d+[hmd])?$', history_string.lower())
    if not results:
        logger.error(f"Error parsing history string: {history_string}")
        logger.info(f"History format: xxdyyh , e.g: 1d12h, 1d, 2h, 1m")
        return False

    history_in_seconds = 0
    for s in results.groups():
        if not s:
            continue
        ind = s[-1]
        v = int(s[:-1])
        if ind == 'd':
            history_in_seconds += v * (60*60*24)
        if ind == 'h':
            history_in_seconds += v * (60*60)
        if ind == 'm':
            history_in_seconds += v * 60

    records_number = int(history_in_seconds/5)
    return records_number


def process_records(records, records_count, keys=None):
    # combine records
    if not records:
        return []

    records_count = min(records_count, len(records))

    data_per_record = int(len(records) / records_count)
    new_records = []
    for i in range(records_count):
        first_index = i * data_per_record
        last_index = (i + 1) * data_per_record
        last_index = min(last_index, len(records))
        sl = records[first_index:last_index]
        rec = dict_agg(sl, mean=True, keys=keys)
        new_records.append(rec)
    return new_records


def ping_host(ip):
    logger.debug(f"Pinging ip ... {ip}")
    response = os.system(f"ping -c 1 -W 3 {ip} > /dev/null")
    if response == 0:
        logger.debug(f"{ip} is UP")
        return True
    else:
        logger.debug(f"{ip} is DOWN")
        return False


def sum_records(records):
    if len(records) == 0:
        return False
    elif len(records) == 1:
        return records[0]
    else:
        total = records[0]
        for rec in records[1:]:
            total += rec
        return total


def get_random_vuid():
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    used_vuids = []
    nodes = db_controller.get_storage_nodes()
    for node in nodes:
        for bdev in node.lvstore_stack:
            type = bdev['type']
            if type == "bdev_distr":
                vuid = bdev['params']['vuid']
            elif type == "bdev_raid"  and "jm_vuid" in bdev:
                vuid = bdev['jm_vuid']
            else:
                continue
            used_vuids.append(vuid)

    for lvol in db_controller.get_lvols():
        used_vuids.append(lvol.vuid)

    r = 1 + int(random.random() * 10000)
    while r in used_vuids:
        r = 1 + int(random.random() * 10000)
    return r


def calculate_core_allocation(cpu_count):
    '''
    If number of cpu cores >= 8, tune cpu core mask
        1. Never use core 0 for spdk.
        2. Core 1 is for app_thread
        3. Core 2 for Journal manager
        4. Poller cpu cores are 30% of Available cores
        5. Alceml cpu cores are 30% of Available cores
        6. Distribs cpu cores are 40% of Available cores
    JIRA ticket link/s
    https://simplyblock.atlassian.net/browse/SFAM-885
    '''

    if cpu_count > 64:
        cpu_count = 64

    all_cores = list(range(0, cpu_count))
    app_thread_core = all_cores[1:2]
    jm_cpu_core = all_cores[2:3]

    # Calculate available cores
    available_cores_count = cpu_count - 3

    # Calculate cpus counts
    poller_cpus_count = int(available_cores_count * 0.3)
    alceml_cpus_cout = int(available_cores_count * 0.3)

    # Calculate cpus cores
    poller_cpu_cores = all_cores[3:poller_cpus_count+3]
    alceml_cpu_cores = all_cores[3+poller_cpus_count:poller_cpus_count+alceml_cpus_cout+3]
    distrib_cpu_cores = all_cores[3+poller_cpus_count+alceml_cpus_cout:]

    return app_thread_core, jm_cpu_core, poller_cpu_cores, alceml_cpu_cores, distrib_cpu_cores

def hexa_to_cpu_list(cpu_mask):
    # Convert the hex string to an integer
    mask_int = int(cpu_mask, 16)

    # Initialize an empty list to hold the positions of the 1s
    cpu_list = []

    # Iterate over each bit position
    position = 0
    while mask_int > 0:
        # Check if the least significant bit is 1
        if mask_int & 1:
            cpu_list.append(position)

        # Shift the mask right by 1 bit to check the next bit
        mask_int >>= 1
        position += 1

    return cpu_list

def calculate_core_allocation(cpu_cores):
    if len(cpu_cores) >= 23:
        app_thread_core = [cpu_cores[pos - 1] for pos in [10]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [11, 22]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [9, 7, 8, 12, 14, 15, 19, 20]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1, 2, 3]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [13, 21]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [4, 5, 6, 16, 17, 18]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [23]]
    elif len(cpu_cores) >= 21:
        app_thread_core = [cpu_cores[pos - 1] for pos in [14]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [15, 21]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 7, 8, 9, 10, 11, 13]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1, 2]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [12, 20]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [4, 5, 6, 16, 17, 18]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [19]]
    elif len(cpu_cores) >= 19:
        app_thread_core = [cpu_cores[pos - 1] for pos in [13]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [14]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 8, 7, 9, 12, 15]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1, 2]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [11, 19]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [4, 5, 6, 16, 17, 18]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [10]]
    elif len(cpu_cores) >= 17:
        app_thread_core = [cpu_cores[pos - 1] for pos in [12]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [13]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [6, 7, 8, 11, 14]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1, 2]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [10]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 4, 5, 15, 16, 17]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [9]]
    elif len(cpu_cores) >= 15:
        app_thread_core = [cpu_cores[pos - 1] for pos in [8]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [15]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [6, 7, 10, 13]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1, 2]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [9]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 4, 5, 11, 12]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [14]]
    elif len(cpu_cores) >= 13:
        app_thread_core = [cpu_cores[pos - 1] for pos in [9]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [10]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 5, 6, 7]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [8]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 4, 11, 12]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [13]]
    elif len(cpu_cores) >= 11:
        app_thread_core = [cpu_cores[pos - 1] for pos in [8]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [9]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 4, 10]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [7]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 5, 6]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [11]]
    elif len(cpu_cores) >= 9:
        app_thread_core = [cpu_cores[pos - 1] for pos in [7]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [8]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 4]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [4, 9]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 5, 6]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [7]]
    elif len(cpu_cores) >= 7:
        app_thread_core = [cpu_cores[pos - 1] for pos in [6]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [5]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [3, 5]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 4]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [7]]
    elif len(cpu_cores) >= 5:
        app_thread_core = [cpu_cores[pos - 1] for pos in [5]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [4]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 3]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 3]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [5]]
    elif len(cpu_cores) >= 4:
        app_thread_core = [cpu_cores[pos - 1] for pos in [4]]
        jm_cpu_core = [cpu_cores[pos - 1] for pos in [4]]
        poller_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 3]]
        alceml_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        alceml_worker_cpu_cores = [cpu_cores[pos - 1] for pos in [1]]
        distrib_cpu_cores = [cpu_cores[pos - 1] for pos in [2, 3]]
        jc_singleton_core = [cpu_cores[pos - 1] for pos in [4]]
    else:
        app_thread_core = jm_cpu_core = poller_cpu_cores = alceml_cpu_cores = alceml_worker_cpu_cores = distrib_cpu_cores = jc_singleton_core = []

    return app_thread_core, jm_cpu_core, poller_cpu_cores, alceml_cpu_cores, alceml_worker_cpu_cores, distrib_cpu_cores, jc_singleton_core


def isolate_cores(spdk_cpu_mask):
    spdk_cores = hexa_to_cpu_list(spdk_cpu_mask)
    hyperthreading_enabled = is_hyperthreading_enabled_via_siblings()
    isolated_cores = spdk_cores
    siblings = parse_thread_siblings()
    isolated_full = set(isolated_cores)
    if hyperthreading_enabled:
        for cpu in siblings[0]:
            isolated_full.discard(cpu)
    else:
        isolated_full.discard(0)
    return isolated_full

def generate_mask(cores):
    mask = 0
    for core in cores:
        mask |= (1 << core)
    return f'0x{mask:X}'

def calculate_pool_count(alceml_count, number_of_distribs, cpu_count, poller_count):
    '''
    				        Small pool count				            Large pool count
    Create JM			    256						                    32					                    For each JM

    RAID                    256                                         32                                      2 one for raid of JM and one for raid of ditribs

    Create Alceml 			256						                    32					                    For each Alceml

    Create Distrib 			256						                    32					                    For each distrib

    First Send cluster map	256						                    32					                    Calculated or one time

    NVMF transport TCP 		127 * poll_groups_mask||CPUCount + 384		15 * poll_groups_mask||CPUCount + 384 	Calculated or one time

    Subsystem add NS		128 * poll_groups_mask||CPUCount		    16 * poll_groups_mask||CPUCount		    Calculated or one time

    ####Create snapshot			512						                    64					                    For each snapshot

    ####Clone lvol			    256						                    32					                    For each clone

    '''
    poller_number = poller_count if poller_count else cpu_count
    #small_pool_count = (3 + alceml_count + lvol_count + 2 * snap_count + 1) * 256 + poller_number * 127 + 384 + 128 * poller_number + constants.EXTRA_SMALL_POOL_COUNT

    small_pool_count = 384 * (alceml_count + number_of_distribs + 3 + poller_count) + (6 + alceml_count + number_of_distribs) * 256 + poller_number * 127 + 384 + 128 * poller_number + constants.EXTRA_SMALL_POOL_COUNT
    #large_pool_count = (3 + alceml_count + lvol_count + 2 * snap_count + 1) * 32 + poller_number * 15 + 384 + 16 * poller_number + constants.EXTRA_LARGE_POOL_COUNT
    large_pool_count = 48 * (alceml_count + number_of_distribs + 3 + poller_count) + (6 + alceml_count + number_of_distribs) * 32 + poller_number * 15 + 384 + 16 * poller_number + constants.EXTRA_LARGE_POOL_COUNT
    return 2*small_pool_count, 2*large_pool_count


def calculate_minimum_hp_memory(small_pool_count, large_pool_count, lvol_count, max_prov, cpu_count):
    '''
    1092 (initial consumption) + 4 * CPU + 1.0277 * POOL_COUNT(Sum in MB) + (25) * lvol_count
    then you can amend the expected memory need for the creation of lvols (6MB),
    connection number over lvols (7MB per connection), creation of snaps (12MB),
    extra buffer 2GB
    return: minimum_hp_memory in bytes
    '''
    pool_consumption = (small_pool_count * 8 + large_pool_count * 128) / 1024 + 1092
    max_prov_tb = max_prov / (1024 * 1024 * 1024 * 1024)
    memory_consumption = (4 * cpu_count + 1.0277 * pool_consumption + 25 * lvol_count) * (1024 * 1024) + (250 * 1024 * 1024) * 1.1 * max_prov_tb + constants.EXTRA_HUGE_PAGE_MEMORY
    return int(memory_consumption)


def calculate_minimum_sys_memory(max_prov, total):
    max_prov_tb = max_prov / (1024 * 1024 * 1024 * 1024)
    minimum_sys_memory = (250 * 1024 * 1024) * 1.1 * max_prov_tb + (constants.EXTRA_SYS_MEMORY * total)
    logger.debug(f"Minimum system memory is {humanbytes(minimum_sys_memory)}")
    return int(minimum_sys_memory)


def calculate_spdk_memory(minimum_hp_memory, minimum_sys_memory, free_sys_memory, huge_total_memory):
    total_free_memory = free_sys_memory + huge_total_memory
    if total_free_memory < (minimum_hp_memory + minimum_sys_memory):
        logger.warning(f"Total free memory:{humanbytes(total_free_memory)}, "
                       f"Minimum huge pages memory: {humanbytes(minimum_hp_memory)}, "
                       f"Minimum system memory: {humanbytes(minimum_sys_memory)}")
        return False, 0
    spdk_mem = int(minimum_hp_memory)
    logger.debug(f"SPDK memory is {humanbytes(spdk_mem)}")
    return True, spdk_mem


def get_total_size_per_instance_type(instance_type):
    instance_storage_data = constants.INSTANCE_STORAGE_DATA
    if instance_type in instance_storage_data:
        number_of_devices = instance_storage_data[instance_type]["number_of_devices"]
        device_size = instance_storage_data[instance_type]["size_per_device_gb"]
        return True, number_of_devices, device_size

    return False, 0, 0


def validate_add_lvol_or_snap_on_node(memory_free, huge_free, max_lvol_or_snap,
                                      lvol_or_snap_size, node_capacity, node_lvol_or_snap_count):
    min_sys_memory = 2 / 4096 * lvol_or_snap_size +  1 / 4096 * node_capacity + constants.MIN_SYS_MEMORY_FOR_LVOL
    if huge_free < constants.MIN_HUGE_PAGE_MEMORY_FOR_LVOL:
        return f"No enough huge pages memory on the node, Free memory: {humanbytes(huge_free)}, " \
               f"Min Huge memory required: {humanbytes(constants.MIN_HUGE_PAGE_MEMORY_FOR_LVOL)}"
    if memory_free < min_sys_memory:
        return f"No enough system memory on the node, Free Memory: {humanbytes(memory_free)}, " \
               f"Min Sys memory required: {humanbytes(min_sys_memory)}"
    if node_lvol_or_snap_count >= max_lvol_or_snap:
        return f"You have exceeded the max number of lvol/snap {max_lvol_or_snap}"
    return ""


def get_host_arch():
    out, _, _ = shell_utils.run_command("uname -m")
    return out


def decimal_to_hex_power_of_2(decimal_number):
    power_result = 2 ** decimal_number
    hex_result = hex(power_result)
    return hex_result


def get_logger(name=""):
    # first configure a root logger
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logg = logging.getLogger(f"root")

    log_level = os.getenv("SIMPLYBLOCK_LOG_LEVEL")
    log_level = log_level.upper() if log_level else constants.LOG_LEVEL

    try:
        logg.setLevel(log_level)
    except ValueError as e:
        logg.warning(f'Invalid SIMPLYBLOCK_LOG_LEVEL: {str(e)}')
        logg.setLevel(constants.LOG_LEVEL)

    if not logg.hasHandlers():
        logger_handler = logging.StreamHandler(stream=sys.stdout)
        logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
        logg.addHandler(logger_handler)
        gelf_handler = GELFTCPHandler('0.0.0.0', constants.GELF_PORT)
        logg.addHandler(gelf_handler)

    if name:
        logg = logging.getLogger(f"root.{name}")
        logg.propagate = True

    return logg


def parse_size(size_string: str):
    try:
        x = int(size_string)
        return x
    except Exception:
        pass
    try:
        if size_string:
            size_string = size_string.lower()
            size_string = size_string.replace(" ", "")
            size_string = size_string.replace("b", "")
            size_number = int(size_string[:-1])
            size_v = size_string[-1]
            one_k = constants.ONE_KB
            multi = 0
            if size_v == "k":
                multi = 1
            elif size_v == "m":
                multi = 2
            elif size_v == "g":
                multi = 3
            elif size_v == "t":
                multi = 4
            else:
                print(f"Error parsing size: {size_string}")
                return -1
            return int(size_number * math.pow(one_k, multi))
        else:
            return -1
    except:
        print(f"Error parsing size: {size_string}")
        return -1


def nearest_upper_power_of_2(n):
    # Check if n is already a power of 2
    if (n & (n - 1)) == 0:
        return n
    # Otherwise, return the nearest upper power of 2
    return 1 << n.bit_length()


def strfdelta(tdelta):
    remainder = int(tdelta.total_seconds())
    possible_fields = ('W', 'D', 'H', 'M', 'S')
    constants = {'W': 604800, 'D': 86400, 'H': 3600, 'M': 60, 'S': 1}
    values = {}
    out = ""
    for field in possible_fields:
        if field in constants:
            values[field], remainder = divmod(remainder, constants[field])
            if values[field] > 0:
                out += f"{values[field]}{field.lower()} "

    return out.strip()


def handle_task_result(task: JobSchedule, res: dict, allowed_error_codes = None):
    if res:
        if not allowed_error_codes:
            allowed_error_codes = [0]

        res_data = res[0]
        migration_status = res_data.get("status")
        error_code = res_data.get("error", -1)
        progress = res_data.get("progress", -1)
        if migration_status == "completed":
            if error_code == 0:
                task.function_result = "Done"
                task.status = JobSchedule.STATUS_DONE
            elif error_code in allowed_error_codes:
                task.function_result = f"mig completed with status: {error_code}"
                task.status = JobSchedule.STATUS_DONE
            else:
                task.function_result = f"mig error: {error_code}, retrying"
                task.retry += 1
                task.status = JobSchedule.STATUS_SUSPENDED
                del task.function_params['migration']

            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return True

        elif migration_status == "failed":
            task.status = JobSchedule.STATUS_DONE
            task.function_result = migration_status
            task.write_to_db()
            return True

        elif migration_status == "none":
            task.function_result = f"mig retry after restart"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
            del task.function_params['migration']
            task.write_to_db()
            return True

        else:
            task.function_result = f"Status: {migration_status}, progress:{progress}"
            task.write_to_db()
    else:
        logger.error("Failed to get mig status")


logger = get_logger(__name__)


def get_next_port(cluster_id):
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()

    port = constants.LVOL_NVMF_PORT_START
    used_ports = []
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.lvol_subsys_port > 0:
            used_ports.append(node.lvol_subsys_port)

    next_port = port
    while True:
        if next_port not in used_ports:
            return next_port
        next_port += 1

def get_next_rpc_port(cluster_id):
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()

    port = 8080
    used_ports = []
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.rpc_port > 0:
            used_ports.append(node.rpc_port)

    for i in range(1000):
        next_port = port + i

        if next_port not in used_ports:
            return next_port

    return 0

def get_next_dev_port(cluster_id):
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()

    port = 9080
    used_ports = []
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.nvmf_port > 0:
            used_ports.append(node.nvmf_port)

    for i in range(1000):
        next_port = port + i

    return port


def generate_realtime_variables_file(isolated_cores, realtime_variables_file_path="/etc/tuned/realtime-variables.conf"):
    """
    Generate or update the realtime-variables.conf file.
    Args:
        isolated_cores (set): set of isolated cores to write.
        file_path (str): Path to the file.
    """
    # Ensure the directory exists
    core_list = ",".join(map(str, isolated_cores))
    tuned_dir = "/etc/tuned/realtime"
    os.makedirs(tuned_dir, exist_ok=True)

    # Create tuned.conf
    tuned_conf_content = f"""[main]
include=latency-performance
[bootloader]
cmdline_add=isolcpus={core_list} nohz_full={core_list} rcu_nocbs={core_list}
"""

    tuned_conf_path = f"{tuned_dir}/tuned.conf"
    with open(tuned_conf_path, "w") as f:
        f.write(tuned_conf_content)
    content = f"isolated_cores={core_list}\n"
    try:
        process = subprocess.run(
            ["sudo", "tee", realtime_variables_file_path],
            input=content.encode("utf-8"),
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.PIPE,  # Capture standard error
            check=True  # Raise an error if command fails
        )
        logger.info(f"Successfully wrote to {realtime_variables_file_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error writing to file: {e}")

def run_tuned():
    try:
        subprocess.run(
            ["sudo", "tuned-adm", "profile", "realtime"],
            check=True
        )
        print(f"Successfully run the tuned adm profile")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running the tuned adm profile: {e}")


def parse_thread_siblings():
    """Parse the thread siblings from the sysfs topology."""
    siblings = {}
    for cpu in os.listdir("/sys/devices/system/cpu/"):
        if cpu.startswith("cpu") and cpu[3:].isdigit():
            cpu_id = int(cpu[3:])
            try:
                with open(f"/sys/devices/system/cpu/{cpu}/topology/thread_siblings_list") as f:
                    siblings[cpu_id] = [
                        int(x) for x in f.read().strip().split(",")
                    ]
            except FileNotFoundError:
                siblings[cpu_id] = [cpu_id]  # No siblings for this CPU
    return siblings


def is_hyperthreading_enabled_via_siblings():
    """
    Check if hyperthreading is enabled based on thread_siblings_list.
    """
    siblings = parse_thread_siblings()
    for sibling_list in siblings.values():
       if len(sibling_list) > 1:
            return True
    return False


def load_core_distribution_from_file(file_path, number_of_cores):
    """Load core distribution from the configuration file or use default."""
    # Check if the file exists
    if not os.path.exists(file_path):
        logger.warning("Configuration file not found. Using default values.")
        return None  # Indicate that defaults should be used

    # Attempt to read the file
    try:
        with open(file_path, "r") as configfile:
            config = {}
            for line in configfile:
                if line.strip() and not line.startswith("#"):  # Skip comments and empty lines
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if key in CONFIG_KEYS:
                        config[key] = [int(core) for core in value.split(",")] if value else None
                        if any(core > number_of_cores for core in config[key]):
                            raise ValueError(f"Invalid distribution, the index of the core {value}: "
                                             f"must be in range of number of cores {number_of_cores}")


            # Validate all keys are present
            if not all(key in config and config[key] is not None for key in CONFIG_KEYS):
                logger.warning("Incomplete configuration provided. Using default values.")
                return None  # Indicate that defaults should be used

            return config
    except Exception as e:
        logger.warning(f"Error reading configuration file: {e}, Using default values.")
        return None  # Indicate that defaults should be used


def store_cores_config(spdk_cpu_mask):
    cores_config = {
        "cpu_mask": spdk_cpu_mask
    }
    file_path = constants.TEMP_CORES_FILE
    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    cores_config_json = json.dumps(cores_config, indent=4)

    # Write the dictionary to the JSON file
    try:
        process = subprocess.run(
            ["sudo", "tee", file_path],
            input=cores_config_json.encode("utf-8"),
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.PIPE,  # Capture standard error
            check=True  # Raise an error if command fails
        )
        logger.info(f"JSON file successfully written to {file_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error writing to file: {e}")

def init_sentry_sdk(name=None):
    import sentry_sdk
    params = {
        "dsn": constants.SENTRY_SDK_DNS,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for tracing.
        "traces_sample_rate": 1.0,
        # Add request headers and IP for users,
        # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
        "send_default_pii": True,
    }
    if name:
        params["server_name"] = name
    sentry_sdk.init(**params)
    # from sentry_sdk import set_level
    # set_level("critical")

    return True
