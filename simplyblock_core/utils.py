# coding=utf-8
import json
import logging
import os
import random
import re
import string
import sys

import docker
from prettytable import PrettyTable
from graypy import GELFUDPHandler

from simplyblock_core import constants
from simplyblock_core import shell_utils


logger = logging.getLogger()


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


def print_table(data: list):
    if data:
        x = PrettyTable(field_names=data[0].keys(), max_width=70)
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
    KB = float(1000)
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
    from simplyblock_core.kv_store import DBController
    db_controller = DBController()
    nodes = db_controller.get_mgmt_nodes(cluster_id)
    if not nodes:
        logger.error("No mgmt nodes was found in the cluster!")
        exit(1)

    docker_ips = [node.docker_ip_port for node in nodes]

    for ip in docker_ips:
        try:
            c = docker.DockerClient(base_url=f"tcp://{ip}", version="auto")
            return c
        except docker.errors.DockerException as e:
            print(e)
    raise e


def dict_agg(data, mean=False):
    out = {}
    for d in data:
        for key in d.keys():
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
            w = (node_stats[node_id][key] / cluster_stats[key]) * 100
            if key in ["lvol", "r_io", "w_io", "r_b", "w_b"]:  # get reverse value
                w = ((cluster_stats[key]-node_stats[node_id][key]) / cluster_stats[key]) * 100
        return w

    out = {}
    for node_id in node_stats:
        out[node_id] = {}
        total = 0
        for key in cluster_stats:
            w = _get_key_w(node_id, key)
            w = _normalize_w(key, w)
            out[node_id][key] = w
            total += w
        out[node_id]['total'] = int(total)
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

    records_number = int(history_in_seconds/2)
    return records_number


def process_records(records, records_count):
    # combine records
    data_per_record = int(len(records) / records_count)
    new_records = []
    for i in range(records_count):
        first_index = i * data_per_record
        last_index = (i + 1) * data_per_record
        last_index = min(last_index, len(records))
        sl = records[first_index:last_index]
        rec = dict_agg(sl, mean=True)
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
    return 1 + int(random.random() * 10000)


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


def generate_mask(cores):
    mask = 0
    for core in cores:
        mask |= (1 << core)
    return f'0x{mask:X}'

def calculate_pool_count(alceml_count, lvol_count, snap_count, cpu_count, poller_count):
    '''
    				        Small pool count				            Large pool count
    Create JM			    256						                    32					                    For each JM

    Create Alceml 			256						                    32					                    For each Alceml

    Create Distrib 			256						                    32					                    For each distrib

    First Send cluster map	256						                    32					                    Calculated or one time

    NVMF transport TCP 		127 * poll_groups_mask||CPUCount + 384		15 * poll_groups_mask||CPUCount + 384 	Calculated or one time

    Subsystem add NS		128 * poll_groups_mask||CPUCount		    16 * poll_groups_mask||CPUCount		    Calculated or one time

    Create snapshot			512						                    64					                    For each snapshot

    Clone lvol			    256						                    32					                    For each clone
    '''
    poller_number = poller_count if poller_count else cpu_count
    small_pool_count = (3 + alceml_count + lvol_count + 2 * snap_count + 1) * 256 + poller_number * 127 + 384 + 128 * poller_number + constants.EXTRA_SMALL_POOL_COUNT
    large_pool_count = (3 + alceml_count + lvol_count + 2 * snap_count + 1) * 32 + poller_number * 15 + 384 + 16 * poller_number + constants.EXTRA_LARGE_POOL_COUNT
    return small_pool_count, large_pool_count


def calculate_minimum_hp_memory(small_pool_count, large_pool_count, lvol_count, snap_count, cpu_count):
    '''
    1092 (initial consumption) + 4 * CPU + 1.0277 * POOL_COUNT(Sum in MB) + (7 + 6) * lvol_count + 12 * snap_count
    then you can amend the expected memory need for the creation of lvols (6MB),
    connection number over lvols (7MB per connection), creation of snaps (12MB),
    extra buffer 2GB
    return: minimum_hp_memory in bytes
    '''
    pool_consumption = (small_pool_count * 8 + large_pool_count * 128) / 1024 + 1092
    memory_consumption = (4 * cpu_count + 1.0277 * pool_consumption + (6 + 7) * lvol_count + 12 * snap_count) * (1024 * 1024) + constants.EXTRA_HUGE_PAGE_MEMORY
    return memory_consumption


def calculate_minimum_sys_memory(max_prov):
    max_prov_tb = max_prov / (1024 * 1024 * 1024 * 1024)
    minimum_sys_memory = (250 * 1024 * 1024) * 1.1 * max_prov_tb + constants.EXTRA_SYS_MEMORY
    logger.debug(f"Minimum system memory is {humanbytes(minimum_sys_memory)}")
    return minimum_sys_memory


def calculate_spdk_memory(minimum_hp_memory, minimum_sys_memory, free_sys_memory, huge_total_memory):
    total_free_memory = free_sys_memory + huge_total_memory
    if total_free_memory < (minimum_hp_memory + minimum_sys_memory):
        logger.warning(f"Total free memory:{humanbytes(total_free_memory)}, "
                       f"Minimum huge pages memory: {humanbytes(minimum_hp_memory)}, "
                       f"Minimum system memory: {humanbytes(minimum_sys_memory)}")
        return False, 0
    spdk_mem = minimum_hp_memory + (total_free_memory - minimum_hp_memory - minimum_sys_memory) * 0.2
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


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(constants.LOG_LEVEL)
    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
    logger.addHandler(logger_handler)

    gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
    logger.addHandler(gelf_handler)
    return logger
