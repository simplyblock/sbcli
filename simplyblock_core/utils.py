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
import uuid
import time
from typing import Union
from kubernetes import client, config
import docker
from prettytable import PrettyTable
from docker.errors import APIError, DockerException, ImageNotFound

from simplyblock_core import constants
from simplyblock_core import shell_utils
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_web import node_utils

CONFIG_KEYS = [
    "app_thread_core",
    "jm_cpu_core",
    "poller_cpu_cores",
    "alceml_cpu_cores",
    "alceml_worker_cpu_cores",
    "distrib_cpu_cores",
    "jc_singleton_core",
]


UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


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


def get_system_cpus():
    out, _, _ = shell_utils.run_command("lscpu -p=CPU,NODE")
    return out


def get_nvme_info(dev_name):
    out, _, _ = shell_utils.run_command(f"udevadm info --query=all --name=/dev/{dev_name}")
    return out


def get_nics_data():
    out, err, rc = shell_utils.run_command("ip -j address show")
    if rc != 0:
        logger.error(err)
        return []
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


_humanbytes_parameter = {
    'si': (10, 3, math.log10, ''),
    'iec': (2, 10, math.log2, 'i'),
    'jedec': (2, 10, math.log2, ''),
}


def humanbytes(size: int, mode: str = 'si') -> str:
    """Return the given bytes as a human friendly including the appropriate unit."""
    if not size or size < 0:
        return '0 B'

    base, exponent, log, infix = _humanbytes_parameter[mode]

    prefixes = ['', 'k' if mode == 'si' else 'K', 'M', 'G', 'T', 'P', 'E', 'Z']
    exponent_multiplier = min(int(log(size) / exponent), len(prefixes) - 1)

    size_in_unit = size / (base ** (exponent * exponent_multiplier))
    prefix = prefixes[exponent_multiplier]

    return f"{size_in_unit:.1f} {prefix}{infix if prefix else ''}B"


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
                out[key] = int(out[key] / count)
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
            w = (cluster_stats[key] / node_stats[node_id][key]) * 10
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
        logger.info("History format: xxdyyh , e.g: 1d12h, 1d, 2h, 1m")
        return False

    history_in_seconds = 0
    for s in results.groups():
        if not s:
            continue
        ind = s[-1]
        v = int(s[:-1])
        if ind == 'd':
            history_in_seconds += v * (60 * 60 * 24)
        if ind == 'h':
            history_in_seconds += v * (60 * 60)
        if ind == 'm':
            history_in_seconds += v * 60

    records_number = int(history_in_seconds / 5)
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
    response = os.system(f"ping -c 3 -W 3 {ip} > /dev/null")
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
            elif type == "bdev_raid" and "jm_vuid" in bdev:
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


def pair_hyperthreads():
    vcpus = list(range(os.cpu_count()))
    half = len(vcpus) // 2
    return {vcpus[i]: vcpus[i + half] for i in range(half)}


def calculate_core_allocations(vcpu_list, alceml_count=2):
    is_hyperthreaded = is_hyperthreading_enabled_via_siblings()
    pairs = pair_hyperthreads() if is_hyperthreaded else {}
    remaining = set(vcpu_list)

    def reserve(vcpu, get_sibling=False):
        if vcpu in remaining:
            remaining.remove(vcpu)
            if is_hyperthreaded and vcpu in pairs and get_sibling:
                sibling = pairs[vcpu]
                if sibling in remaining:
                    remaining.remove(sibling)
                    return [vcpu, sibling]
            return [vcpu]
        return []

    def reserve_n(count):
        vcpus = []
        if count > 0:
            for v in sorted(remaining):
                if (count - len(vcpus)) >= 2:
                    vcpus += reserve(v, True)
                else:
                    vcpus += reserve(v)
                if len(vcpus) >= count:
                    break
        return vcpus[:count]

    assigned = {}
    assigned["app_thread_core"] = reserve_n(1)
    if (len(vcpu_list) < 12):
        vcpu = reserve_n(1)
        assigned["jm_cpu_core"] = vcpu
        vcpu = reserve_n(1)
        assigned["jc_singleton_core"] = vcpu
        assigned["alceml_worker_cpu_cores"] = vcpu
        vcpu = reserve_n(1)
        assigned["alceml_cpu_cores"] = vcpu
    elif (len(vcpu_list) < 22):
        vcpu = reserve_n(1)
        assigned["jm_cpu_core"] = vcpu
        vcpu = reserve_n(1)
        assigned["jc_singleton_core"] = vcpu
        vcpus = reserve_n(1)
        assigned["alceml_worker_cpu_cores"] = vcpus
        vcpus = reserve_n(2)
        assigned["alceml_cpu_cores"] = vcpus
    else:
        vcpus = reserve_n(1)
        assigned["jm_cpu_core"] = vcpus
        vcpu = reserve_n(1)
        assigned["jc_singleton_core"] = vcpu
        vcpus = reserve_n(int(alceml_count / 3) + ((alceml_count % 3) > 0))
        assigned["alceml_worker_cpu_cores"] = vcpus
        vcpus = reserve_n(alceml_count)
        assigned["alceml_cpu_cores"] = vcpus
    dp = int(len(remaining) / 2)
    vcpus = reserve_n(dp)
    assigned["distrib_cpu_cores"] = vcpus
    vcpus = reserve_n(dp)
    assigned["poller_cpu_cores"] = vcpus
    if len(remaining) > 0:
        if len(assigned["poller_cpu_cores"]) == 0:
            assigned["distrib_cpu_cores"] = assigned["poller_cpu_cores"] = reserve_n(1)
        else:
            assigned["distrib_cpu_cores"] = assigned["distrib_cpu_cores"] + reserve_n(1)
    # Return the individual threads as separate values
    return (
        assigned.get("app_thread_core", []),
        assigned.get("jm_cpu_core", []),
        assigned.get("poller_cpu_cores", []),
        assigned.get("alceml_cpu_cores", []),
        assigned.get("alceml_worker_cpu_cores", []),
        assigned.get("distrib_cpu_cores", []),
        assigned.get("jc_singleton_core", [])
    )

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

    small_pool_count = 384 * (alceml_count + number_of_distribs + 3 + poller_count) + (6 + alceml_count + number_of_distribs) * 256 + poller_number * 127 + 384 + 128 * poller_number + constants.EXTRA_SMALL_POOL_COUNT
    large_pool_count = 48 * (alceml_count + number_of_distribs + 3 + poller_count) + (6 + alceml_count + number_of_distribs) * 32 + poller_number * 15 + 384 + 16 * poller_number + constants.EXTRA_LARGE_POOL_COUNT

    return int(4.0 * small_pool_count), int(1.5 * large_pool_count)


def calculate_minimum_hp_memory(small_pool_count, large_pool_count, lvol_count, max_prov, cpu_count):
    '''
    1092 (initial consumption) + 4 * CPU + 1.0277 * POOL_COUNT(Sum in MB) + (25) * lvol_count
    then you can amend the expected memory need for the creation of lvols (6MB),
    connection number over lvols (7MB per connection), creation of snaps (12MB),
    extra buffer 2GB
    return: minimum_hp_memory in bytes
    '''
    pool_consumption = (small_pool_count * 8 + large_pool_count * 128) / 1024 + 1092
    memory_consumption = (4 * cpu_count + 1.0277 * pool_consumption + 12 * lvol_count) * (1024 * 1024) + (
            250 * 1024 * 1024) * 1.1 * convert_size(max_prov, 'TiB') + constants.EXTRA_HUGE_PAGE_MEMORY
    return int(memory_consumption)


def calculate_minimum_sys_memory(max_prov):
    minimum_sys_memory = (2000 * 1024) * convert_size(max_prov, 'GB') + 500 * 1024 * 1024

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
    min_sys_memory = 2 / 4096 * lvol_or_snap_size + 1 / 4096 * node_capacity + constants.MIN_SYS_MEMORY_FOR_LVOL
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
    logg = logging.getLogger()

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
        # gelf_handler = GELFTCPHandler('0.0.0.0', constants.GELF_PORT)
        # logg.addHandler(gelf_handler)

    if name:
        logg = logging.getLogger(f"root.{name}")
        logg.propagate = True

    return logg


def _parse_unit(unit: str, mode: str = 'si/iec', strict: bool = True) -> tuple[int, int]:
    """Parse the given unit, returning the associated base and exponent

    Mode can be either 'si/iec' to parse decimal (SI) and binary (IEC) units, or
    'jedec' for binary only units. If `strict`, parsing will be case-sensitive and
    expect the 'B' suffix.
    """
    regexes = {
        'si/iec': r'^((?P<prefix>[kKMGTPEZ])(?P<binary>i)?)?' + ('B$' if strict else 'B?$'),
        'jedec': r'^(?P<prefix>[KMGTPEZ])?' + ('B$' if strict else 'B?$'),
    }

    m = re.match(regexes[mode], unit, flags=re.IGNORECASE if not strict else 0)
    if m is None:
        raise ValueError("Invalid unit")

    binary = (mode == 'jedec') or (m.group('binary') is not None)
    prefix = m.group('prefix') or ''

    if strict and (binary and (prefix == 'k')) or ((not binary) and (prefix == 'K')):
        raise ValueError("Invalid unit")

    exponent_multipliers = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']
    return (
        2 if binary else 10,
        (10 if binary else 3) * exponent_multipliers.index(prefix.upper())
    )


def parse_size(size: Union[str, int], mode: str = 'si/iec', assume_unit: str = '', strict: bool = False) -> int:
    """Parse the given data size

    If passed and not explicitly given, 'assume_unit' will be assumed.
    Mode can be either 'si/iec' to parse decimal (SI) and binary (IEC) units, or
    'jedec' for binary only units. If `strict`, parsing will be case-sensitive and
    expect the 'B' suffix.
    """
    try:
        if isinstance(size, int):
            size_in_unit = size
            unit = assume_unit
        else:
            m = re.match(r'^(?P<size_in_unit>\d+) ?(?P<unit>\w+)?$', size.strip())
            if m is None:
                raise ValueError(f"Invalid size: {size}")

            size_in_unit = int(m.group('size_in_unit'))
            unit = m.group('unit') if m.group('unit') else assume_unit

        base, exponent = _parse_unit(unit, mode, strict=strict)
        return size_in_unit * (base ** exponent)
    except ValueError:
        return -1


def convert_size(size: Union[int, str], unit: str, round_up: bool = False) -> int:
    """Convert the given number of bytes to target unit

    Accepts both decimal (kB, MB, ...) and binary (KiB, MiB, ...) units.
    Note that the result will be cast to int, i.e. rounded down.
    """
    if isinstance(size, str):
        size = int(size)

    base, exponent = _parse_unit(unit, 'si/iec')
    raw = size / (base ** exponent)
    return math.ceil(raw) if round_up else int(raw)


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


def handle_task_result(task: JobSchedule, res: dict, allowed_error_codes=None):
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
            task.function_result = "mig retry after restart"
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

    port = constants.NODE_NVMF_PORT_START
    used_ports = []
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.nvmf_port > 0:
            used_ports.append(node.nvmf_port)
    next_port = port
    while True:
        if next_port not in used_ports:
            return next_port
        next_port += 1


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
        subprocess.run(
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
        print("Successfully run the tuned adm profile")
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


def store_config_file(data_config, file_path, create_read_only_file=False):
    # Ensure the directory exists
    subprocess.run(
        ["sudo", "mkdir", "-p", os.path.dirname(file_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        check=True)

    cores_config_json = json.dumps(data_config, indent=4)

    # Write the dictionary to the JSON file
    try:
        subprocess.run(
            ["sudo", "tee", file_path],
            input=cores_config_json.encode("utf-8"),
            stdout=subprocess.DEVNULL,  # Suppress standard output
            stderr=subprocess.PIPE,  # Capture standard error
            check=True  # Raise an error if command fails
        )
        logger.info(f"JSON file successfully written to {file_path}")
        # Write to read-only file
        if create_read_only_file:
            subprocess.run(
                ["sudo", "tee", f"{file_path}_read_only"],
                input=cores_config_json.encode("utf-8"),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                check=True
            )
            subprocess.run(["sudo", "chmod", "444", f"{file_path}_read_only"], check=True)

    except subprocess.CalledProcessError as e:
        logger.error(f"Error writing to file: {e}")

def load_config(file_path):
    # Load and parse a JSON config file
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config

def init_sentry_sdk(name=None):
    # import sentry_sdk
    # params = {
    #     "dsn": constants.SENTRY_SDK_DNS,
    #     # Set traces_sample_rate to 1.0 to capture 100%
    #     # of transactions for tracing.
    #     "traces_sample_rate": 1.0,
    #     # Add request headers and IP for users,
    #     # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    #     "send_default_pii": True,
    # }
    # if name:
    #     params["server_name"] = name
    # sentry_sdk.init(**params)
    # # from sentry_sdk import set_level
    # # set_level("critical")

    return True


def get_numa_cores():
    cores_by_numa = {}
    try:
        output = get_system_cpus()
        for line in output.splitlines():
            if line.startswith("#"):
                continue
            cpu_id, node_id = line.strip().split(',')
            node_id = int(node_id)
            cpu_id = int(cpu_id)
            cores_by_numa.setdefault(node_id, []).append(cpu_id)
    except Exception:
        total_cores = os.cpu_count()
        cores_by_numa[0] = list(range(total_cores))
    return cores_by_numa


def generate_hex_string(length):
    def _generate_string(length):
        return ''.join(random.SystemRandom().choice(
            string.ascii_letters + string.digits) for _ in range(length))

    return _generate_string(length).encode('utf-8').hex()


def addNvmeDevices(rpc_client, snode, devs):
    devices = []
    ret = rpc_client.bdev_nvme_controller_list()
    ctr_map = {}
    try:
        if ret:
            ctr_map = {i["ctrlrs"][0]['trid']['traddr']: i["name"] for i in ret}
    except Exception:
        pass

    next_physical_label = snode.physical_label
    for pcie in devs:

        if pcie in ctr_map:
            nvme_controller = ctr_map[pcie]
            nvme_bdevs = []
            for bdev in rpc_client.get_bdevs():
                if bdev['name'].startswith(nvme_controller):
                    nvme_bdevs.append(bdev['name'])
        else:
            pci_st = str(pcie).replace("0", "").replace(":", "").replace(".", "")
            nvme_controller = "nvme_%s" % pci_st
            nvme_bdevs, err = rpc_client.bdev_nvme_controller_attach(nvme_controller, pcie)

        for nvme_bdev in nvme_bdevs:
            rpc_client.bdev_examine(nvme_bdev)
            rpc_client.bdev_wait_for_examine()

            ret = rpc_client.get_bdevs(nvme_bdev)
            nvme_dict = ret[0]
            nvme_driver_data = nvme_dict['driver_specific']['nvme'][0]
            model_number = nvme_driver_data['ctrlr_data']['model_number']
            total_size = nvme_dict['block_size'] * nvme_dict['num_blocks']

            serial_number = nvme_driver_data['ctrlr_data']['serial_number']
            if snode.id_device_by_nqn:
                if "subnqn" in nvme_driver_data['ctrlr_data']:
                    subnqn = nvme_driver_data['ctrlr_data']['subnqn']
                    serial_number = subnqn.split(":")[-1] + f"_{nvme_driver_data['ctrlr_data']['cntlid']}"
                else:
                    logger.error(f"No subsystem nqn found for device: {nvme_driver_data['pci_address']}")

            devices.append(
                NVMeDevice({
                    'uuid': str(uuid.uuid4()),
                    'device_name': nvme_dict['name'],
                    'size': total_size,
                    'physical_label': next_physical_label,
                    'pcie_address': nvme_driver_data['pci_address'],
                    'model_id': model_number,
                    'serial_number': serial_number,
                    'nvme_bdev': nvme_bdev,
                    'nvme_controller': nvme_controller,
                    'node_id': snode.get_id(),
                    'cluster_id': snode.cluster_id,
                    'status': NVMeDevice.STATUS_ONLINE
                }))
    return devices


def get_random_snapshot_vuid():
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    used_vuids = []
    for snap in db_controller.get_snapshots():
        used_vuids.append(snap.vuid)

    r = 1 + int(random.random() * 1000000)
    while r in used_vuids:
        r = 1 + int(random.random() * 1000000)
    return r


def pull_docker_image_with_retry(client: docker.DockerClient, image_name, retries=3, delay=5):
    """
    Pulls a Docker image with retries in case of failure.

    Args:
        client (docker.DockerClient): The Docker client instance.
        image_name (str): The name of the Docker image to pull.
        retries (int): Number of retry attempts. Defaults to 3.
        delay (int): Delay between retries in seconds. Defaults to 5.

    Returns:
        docker.models.images.Image: The pulled Docker image.

    Raises:
        DockerException: If all retry attempts fail.
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"Attempt {attempt}: Pulling image '{image_name}'...")
            image = client.images.pull(image_name)
            print(f"Image '{image_name}' pulled successfully.")
            return image
        except (APIError, DockerException, ImageNotFound) as e:
            print(f"Error pulling image (attempt {attempt}): {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print("All retries failed.")
                raise


def next_free_hublvol_port(cluster_id):
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()

    port = constants.NODE_HUBLVOL_PORT_START
    used_ports = []
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.hublvol and node.hublvol.nvmf_port > 0:
            used_ports.append(node.hublvol.nvmf_port)

    next_port = port
    while True:
        if next_port not in used_ports:
            return next_port
        next_port += 1


def validate_sockets(sockets_to_use, cores_by_numa):
    for socket in sockets_to_use:
        if socket not in cores_by_numa:
            print(f"Error: Socket {socket} not in system sockets {cores_by_numa}")


def detect_nics():
    nics = {}
    net_path = "/sys/class/net"
    for nic in os.listdir(net_path):
        if nic.startswith("lo"):
            continue
        numa_node_path = os.path.join(net_path, nic, "device/numa_node")
        try:
            with open(numa_node_path, "r") as f:
                numa_node = int(f.read().strip())
        except Exception:
            numa_node = -1
        nics[nic] = numa_node
    return nics


def get_nvme_pci_devices():
    try:
        # Step 1: Get all NVMe devices that mounted or have partitions
        lsblk_output = subprocess.check_output(
            ["lsblk", "-dn", "-o", "NAME,MOUNTPOINT"],
            text=True
        )
        blocked_devices = []
        for line in lsblk_output.strip().splitlines():
            name = line.strip().split()
            partitions = subprocess.check_output(
                ["lsblk", "-n", f"/dev/{name[0]}"],
                text=True
            ).strip().splitlines()

            if len(name) > 1 or len(partitions) > 1:
                blocked_devices.append(name[0])


        # Step 3: Map NVMe devices to PCI addresses
        pci_addresses = []
        lspci_output = subprocess.check_output(
            "lspci -Dnn | grep '0108'",
            shell=True,
            text=True
        )
        pci_addresses = [line.split()[0] for line in lspci_output.strip().splitlines()]
        return pci_addresses, blocked_devices

    except subprocess.CalledProcessError:
        logger.warning("No NVMe devices with class 0108 found.")
        return [], []


def get_bound_driver(pci_address):
    driver_path = f"/sys/bus/pci/devices/{pci_address}/driver"
    if os.path.islink(driver_path):
        return os.path.basename(os.readlink(driver_path))
    return None


def unbind_pci_driver(pci_address):
    current_driver = get_bound_driver(pci_address)
    if current_driver:
        unbind_path = f"/sys/bus/pci/devices/{pci_address}/driver/unbind"
        try:
            cmd = f"echo {pci_address} | sudo tee {unbind_path}"
            subprocess.run(cmd, shell=True, check=True)
            logger.debug(f"Unbound {pci_address} from {current_driver}.")
        except Exception as e:
            logger.error(f"Failed to unbind {pci_address} from {current_driver}: {e}")


def bind_to_nvme_driver(pci_address):
    NVME_DRIVER = "nvme"
    current_driver = get_bound_driver(pci_address)
    if current_driver == NVME_DRIVER:
        logger.info(f"{pci_address} is already bound to {NVME_DRIVER}.")
        return

    # Unbind from current driver if any
    unbind_pci_driver(pci_address)

    # Bind to nvme
    new_id_path = f"/sys/bus/pci/drivers/{NVME_DRIVER}/bind"
    try:
        cmd = f"echo {pci_address} | sudo tee {new_id_path}"
        subprocess.run(cmd, shell=True, check=True)
        logger.debug(f"Bound {pci_address} to {NVME_DRIVER}.")
    except Exception as e:
        logger.error(f"Failed to bind {pci_address} to {NVME_DRIVER}: {e}")


def detect_nvmes(pci_allowed, pci_blocked):
    pci_addresses, blocked_devices = get_nvme_pci_devices()
    ssd_pci_set = set(pci_addresses)

    # Normalize SSD PCI addresses and user PCI list
    if pci_allowed:
        user_pci_set = set(
            addr if len(addr.split(":")[0]) == 4 else f"0000:{addr}"
            for addr in pci_allowed
        )

        # Check for unmatched addresses
        unmatched = user_pci_set - ssd_pci_set
        if unmatched:
            logger.error(f"Invalid PCI addresses: {', '.join(unmatched)}")
            return []

        pci_addresses = list(user_pci_set)
    elif pci_blocked:
        user_pci_set = set(
            addr if len(addr.split(":")[0]) == 4 else f"0000:{addr}"
            for addr in pci_blocked
        )
        rest = ssd_pci_set - user_pci_set
        pci_addresses = list(rest)

    for pci in pci_addresses:
        bind_to_nvme_driver(pci)

    nvme_base_path = '/sys/class/nvme/'
    nvme_devices = [dev for dev in os.listdir(nvme_base_path) if dev.startswith('nvme')]
    nvmes = {}
    for dev in nvme_devices:
        dev_name = os.path.basename(dev)
        pattern = re.compile(rf"^{re.escape(dev_name)}n\d+$")
        if any(pattern.match(block_device) for block_device in blocked_devices):
            logger.debug(f"device {dev_name} is busy.. skipping")
            continue
        device_symlink = os.path.join(nvme_base_path, dev)
        try:
            pci_address = "unknown"

            # Resolve the real path to get the actual device path
            real_path = os.path.realpath(device_symlink)

            # Read the PCI address from the 'address' file
            address_file = os.path.join(real_path, 'address')
            with open(address_file, 'r') as f:
                pci_address = f.read().strip()

            # Read the NUMA node information
            numa_node_file = os.path.join(real_path, 'numa_node')
            with open(numa_node_file, 'r') as f:
                numa_node = f.read().strip()

            if pci_address not in pci_addresses:
                continue
            nvmes[dev_name] = {"pci_address": pci_address, "numa_node": numa_node}
        except Exception:
            continue
    return nvmes


def calculate_unisolated_cores(cores):
    # calculate the number if unused system cores (UnIsolated cores)
    total = len(cores)
    if total <= 10:
        return 1
    elif total <= 20:
        return 2
    elif total <= 28:
        return 3
    else:
        return math.ceil(total * 0.15)


def get_core_indexes(core_to_index, list_of_cores):
    return [core_to_index[core] for core in list_of_cores if core in core_to_index]


def generate_core_allocation(cores_by_numa, sockets_to_use, nodes_per_socket):
    node_distribution = {}
    # Iterate over each NUMA node
    for numa_node in sockets_to_use:
        if numa_node not in cores_by_numa:
            continue
        all_cores = sorted(cores_by_numa[numa_node])
        total_cores = len(all_cores)
        num_unisolated = calculate_unisolated_cores(all_cores)

        unisolated = []
        half = total_cores // 2
        for i in range(num_unisolated):
            if i % 2 == 0:
                index = i // 2
            else:
                index = (i - 1) // 2
            if i % 2 == 0:
                unisolated.append(all_cores[index])
            else:
                unisolated.append(all_cores[half + index])

        available_cores = [c for c in all_cores if c not in unisolated]
        q1 = len(available_cores) // 4

        node_distribution[numa_node] = []

        if nodes_per_socket == 1:
            # If there's only one node, assign all available cores to it
            node_cores = available_cores
            l_cores = ",".join([f"{i}@{core}" for i, core in enumerate(node_cores)])
            core_to_index = {core: idx for idx, core in enumerate(node_cores)}
            node_distribution[numa_node].append({
                "cpu_mask": hex(sum([1 << c for c in node_cores])),
                "isolated": node_cores,
                "l-cores": l_cores,
                "distribution": calculate_core_allocations(node_cores),
                "core_to_index": core_to_index
            })
        else:
            # Distribute cores equally between the nodes
            node_0_cores = available_cores[0:q1] + available_cores[2 * q1:3 * q1]
            node_1_cores = available_cores[q1:2 * q1] + available_cores[3 * q1:4 * q1]
            if len(available_cores) % 4 >= 2:
                node_0_cores.append(available_cores[4 * q1])
                node_1_cores.append(available_cores[4 * q1 + 1])


            # Ensure the number of isolated cores is the same for both nodes
            min_isolated_cores = min(len(node_0_cores), len(node_1_cores))

            # Generate l-cores for node 0
            l_cores_0 = ",".join([f"{i}@{core}" for i, core in enumerate(node_0_cores[:min_isolated_cores])])
            core_to_index = {core: idx for idx, core in enumerate(node_0_cores)}
            isolated_cores = node_0_cores[:min_isolated_cores]
            node_distribution[numa_node].append({
                "cpu_mask": hex(sum([1 << c for c in node_0_cores[:min_isolated_cores]])),
                "isolated": isolated_cores,
                "l-cores": l_cores_0,
                "distribution": calculate_core_allocations(isolated_cores),
                "core_to_index": core_to_index
            })

            # Generate l-cores for node 1
            l_cores_1 = ",".join([f"{i}@{core}" for i, core in enumerate(node_1_cores[:min_isolated_cores])])
            core_to_index = {core: idx for idx, core in enumerate(node_1_cores)}
            isolated_cores = node_1_cores[:min_isolated_cores]
            node_distribution[numa_node].append({
                "cpu_mask": hex(sum([1 << c for c in node_1_cores[:min_isolated_cores]])),
                "isolated": isolated_cores,
                "l-cores": l_cores_1,
                "distribution": calculate_core_allocations(isolated_cores),
                "core_to_index": core_to_index
            })

    return node_distribution


def regenerate_config(new_config, old_config, force=False):
    if len(old_config.get("nodes")) != len(new_config.get("nodes")):
        logger.error("The number of node in old config not equal to the number of node in updated config")
        return False
    all_nics = detect_nics()
    for i in range(len(old_config.get("nodes"))):
        validate_node_config(new_config.get("nodes")[i])
        if old_config["nodes"][i]["socket"] != new_config["nodes"][i]["socket"]:
            logger.error("The socket is changed, please rerun sbcli configure without upgrade firstly")
            return False
        number_of_alcemls = len(new_config["nodes"][i]["ssd_pcis"])
        if (old_config["nodes"][i]["cpu_mask"] != new_config["nodes"][i]["cpu_mask"] or
                len(old_config["nodes"][i]["ssd_pcis"]) != len(new_config["nodes"][i]["ssd_pcis"]) or force):
            try:
                isolated_cores = hexa_to_cpu_list(new_config["nodes"][i]["cpu_mask"])
            except ValueError:
                logger.error(f"The updated cpu mask is incorrect {new_config['nodes'][i]['cpu_mask']}")
                return False
            old_config["nodes"][i]["number_of_alcemls"] = number_of_alcemls
            old_config["nodes"][i]["cpu_mask"] = new_config["nodes"][i]["cpu_mask"]
            old_config["nodes"][i]["l-cores"] = ",".join([f"{i}@{core}" for i, core in enumerate(isolated_cores)])
            old_config["nodes"][i]["isolated"] = isolated_cores
            distribution = calculate_core_allocations(isolated_cores, number_of_alcemls + 1)
            core_to_index = {core: idx for idx, core in enumerate(isolated_cores)}
            old_config["nodes"][i]["distribution"] ={
                "app_thread_core": get_core_indexes(core_to_index, distribution[0]),
                "jm_cpu_core": get_core_indexes(core_to_index, distribution[1]),
                "poller_cpu_cores": get_core_indexes(core_to_index, distribution[2]),
                "alceml_cpu_cores": get_core_indexes(core_to_index, distribution[3]),
                "alceml_worker_cpu_cores": get_core_indexes(core_to_index, distribution[4]),
                "distrib_cpu_cores": get_core_indexes(core_to_index, distribution[5]),
                "jc_singleton_core": get_core_indexes(core_to_index, distribution[6])}

        isolated_cores = old_config["nodes"][i]["isolated"]
        number_of_distribs = 2
        number_of_distribs_cores = len(old_config["nodes"][i]["distribution"]["distrib_cpu_cores"])
        number_of_poller_cores = len(old_config["nodes"][i]["distribution"]["poller_cpu_cores"])
        if number_of_distribs_cores > 2:
            number_of_distribs = number_of_distribs_cores
        old_config["nodes"][i]["number_of_distribs"] = number_of_distribs
        old_config["nodes"][i]["ssd_pcis"] = new_config["nodes"][i]["ssd_pcis"]
        old_config["nodes"][i]["nic_ports"] = new_config["nodes"][i]["nic_ports"]
        for nic in old_config["nodes"][i]["nic_ports"]:
            if nic not in all_nics:
                logger.error(f"The nic {nic} is not a member of system nics {all_nics}")
                return False

        small_pool_count, large_pool_count = calculate_pool_count(number_of_alcemls, 2 * number_of_distribs,
                                                                  len(isolated_cores),
                                                                  number_of_poller_cores or len(
                                                                      isolated_cores),)
        minimum_hp_memory = calculate_minimum_hp_memory(small_pool_count, large_pool_count, old_config["nodes"][i]["max_lvol"],
                                                        old_config["nodes"][i]["max_size"], len(isolated_cores))
        old_config["nodes"][i]["small_pool_count"] = small_pool_count
        old_config["nodes"][i]["large_pool_count"] = large_pool_count
        old_config["nodes"][i]["huge_page_memory"] = minimum_hp_memory
        minimum_sys_memory = calculate_minimum_sys_memory(old_config["nodes"][i]["max_size"])
        old_config["nodes"][i]["sys_memory"] =  minimum_sys_memory

    memory_details = node_utils.get_memory_details()
    free_memory = memory_details.get("free")
    huge_total = memory_details.get("huge_total")
    total_free_memory = free_memory + huge_total
    total_required_memory = 0
    all_isolated_cores = set()
    for node in old_config["nodes"]:
        if len(node["ssd_pcis"]) == 0:
            logger.error(f"There are not enough SSD devices on numa node {node['socket']}")
            return False
        total_required_memory += node["huge_page_memory"] + node["sys_memory"]
        node_cores_set = set(node["isolated"])
        all_isolated_cores.update(node_cores_set)
    if total_free_memory < total_required_memory:
            logger.error(f"The Free memory {total_free_memory} is less than required memory {total_required_memory}")
            return False
    old_config["isolated_cores"] = list(all_isolated_cores)
    old_config["host_cpu_mask"] = generate_mask(all_isolated_cores)
    return old_config


def generate_configs(max_lvol, max_prov, sockets_to_use, nodes_per_socket, pci_allowed, pci_blocked):
    system_info = {}
    nodes_config = {"nodes": []}

    cores_by_numa = get_numa_cores()
    validate_sockets(sockets_to_use, cores_by_numa)
    logger.debug(f"Cores by numa {cores_by_numa}")
    nics = detect_nics()
    nvmes = detect_nvmes(pci_allowed, pci_blocked)

    for nid in sockets_to_use:
        if nid in cores_by_numa:
            system_info[nid] = {
                "cores": cores_by_numa[nid],
                "nics": [],
                "nvmes": []
            }

    for nic, numa in nics.items():
        if numa in sockets_to_use:
            system_info[numa]["nics"].append(nic)
        else:
            system_info.setdefault(numa, {"cores": [], "nics": [], "nvmes": []})["nics"].append(nic)

    for nvme, val in nvmes.items():
        pci = val["pci_address"]
        numa = val["numa_node"]
        unbind_pci_driver(pci)
        if numa in sockets_to_use:
            system_info[numa]["nvmes"].append(pci)
        else:
            system_info.setdefault(numa, {"cores": [], "nics": [], "nvmes": []})["nvmes"].append(pci)

    nvme_by_numa = {nid: [] for nid in sockets_to_use}
    nvme_numa_neg1 = []
    for nvme_name, val in nvmes.items():
        numa = val["numa_node"]
        if numa in sockets_to_use:
            nvme_by_numa[numa].append(nvme_name)
        elif int(numa) == -1:
            nvme_numa_neg1.append(nvme_name)

    total_nodes = nodes_per_socket * len(sockets_to_use)
    all_nvmes_per_node = [[] for _ in range(total_nodes)]
    all_nvmes = []
    for devs in nvme_by_numa.values():
        all_nvmes.extend(devs)
    all_nvmes.extend(nvme_numa_neg1)

    for i, nvme_name in enumerate(all_nvmes):
        all_nvmes_per_node[i % total_nodes].append(nvme_name)

    all_nvmes_neg1_per_node = [[] for _ in range(total_nodes)]
    for i, nvme_name in enumerate(nvme_numa_neg1):
        all_nvmes_neg1_per_node[i % total_nodes].append(nvme_name)

    node_cores = generate_core_allocation(cores_by_numa, sockets_to_use, nodes_per_socket, )

    all_nodes = []
    node_index = 0
    for nid in sockets_to_use:
        nvme_list = nvme_by_numa.get(nid)
        logger.debug(f"NVME devices list {nvme_list}")
        nvme_per_core_group = [[] for _ in range(nodes_per_socket)]
        for i, nvme in enumerate(nvme_list):
            nvme_per_core_group[i % nodes_per_socket].append(nvme)

        for idx, core_group in enumerate(node_cores.get(nid, [])):
            node_info = {
                "socket": nid,
                "cpu_mask": core_group["cpu_mask"],
                "isolated": core_group["isolated"],
                "l-cores": ",".join([f"{i}@{core}" for i, core in enumerate(core_group["isolated"])]),
                "number_of_alcemls": 0,
                "distribution": {
                    "app_thread_core": get_core_indexes(core_group["core_to_index"], core_group["distribution"][0]),
                    "jm_cpu_core": get_core_indexes(core_group["core_to_index"], core_group["distribution"][1]),
                    "poller_cpu_cores": get_core_indexes(core_group["core_to_index"], core_group["distribution"][2]),
                    "alceml_cpu_cores": get_core_indexes(core_group["core_to_index"], core_group["distribution"][3]),
                    "alceml_worker_cpu_cores": get_core_indexes(core_group["core_to_index"], core_group["distribution"][4]),
                    "distrib_cpu_cores": get_core_indexes(core_group["core_to_index"], core_group["distribution"][5]),
                    "jc_singleton_core": get_core_indexes(core_group["core_to_index"], core_group["distribution"][6])
                },
                "ssd_pcis": [],
                "nic_ports": system_info[nid]["nics"]
            }
            number_of_distribs = 2
            number_of_distribs_cores = len(node_info["distribution"]["distrib_cpu_cores"])

            number_of_poller_cores = len(node_info["distribution"]["poller_cpu_cores"])
            if number_of_distribs_cores > 2:
                number_of_distribs = number_of_distribs_cores
            node_info["number_of_distribs"] = number_of_distribs

            nvme_neg1_list = all_nvmes_neg1_per_node[node_index]
            for nvme_name in nvme_neg1_list:
                node_info["ssd_pcis"].append(nvmes[nvme_name]["pci_address"])
            for nvme_name in nvme_per_core_group[idx]:
                node_info["ssd_pcis"].append(nvmes[nvme_name]["pci_address"])
            number_of_alcemls = len(node_info["ssd_pcis"])
            node_info["number_of_alcemls"] = number_of_alcemls
            small_pool_count, large_pool_count = calculate_pool_count(number_of_alcemls, 2 * number_of_distribs,
                                                                      len(core_group["isolated"]),
                                                                      number_of_poller_cores or len(
                                                                          core_group["isolated"]))
            minimum_hp_memory = calculate_minimum_hp_memory(small_pool_count, large_pool_count, max_lvol,
                                                            max_prov, len(core_group["isolated"]))
            node_info["small_pool_count"] = small_pool_count
            node_info["large_pool_count"] = large_pool_count
            node_info["max_lvol"] = max_lvol
            node_info["max_size"] = max_prov
            node_info["huge_page_memory"] = minimum_hp_memory
            minimum_sys_memory = calculate_minimum_sys_memory(max_prov)
            node_info["sys_memory"] =  minimum_sys_memory
            all_nodes.append(node_info)
            node_index += 1
    memory_details = node_utils.get_memory_details()
    free_memory = memory_details.get("free")
    huge_total = memory_details.get("huge_total")
    total_free_memory = free_memory + huge_total
    total_required_memory = 0
    all_isolated_cores = set()
    for node in all_nodes:
        if len(node["ssd_pcis"]) == 0:
            logger.error(f"There are not enough SSD devices on numa node {node['socket']}")
            return False, False
        total_required_memory += node["huge_page_memory"] + node["sys_memory"]
        node_cores_set = set(node["isolated"])
        all_isolated_cores.update(node_cores_set)
    if total_free_memory < total_required_memory:
            logger.error(f"The Free memory {total_free_memory} is less than required memory {total_required_memory}")
            return False, False
    nodes_config["nodes"] = all_nodes
    nodes_config["isolated_cores"] = list(all_isolated_cores)
    nodes_config["host_cpu_mask"] = generate_mask(all_isolated_cores)
    final_config = regenerate_config(nodes_config, nodes_config, True)
    return final_config, system_info


def set_hugepages_if_needed(node, hugepages_needed, page_size_kb=2048):
    """Set hugepages for a specific NUMA node if current number is less than needed."""
    hugepage_path = f"/sys/devices/system/node/node{node}/hugepages/hugepages-{page_size_kb}kB/nr_hugepages"

    try:
        with open(hugepage_path, "r") as f:
            current_hugepages = int(f.read().strip())

        if current_hugepages >= hugepages_needed:
            logger.debug(f"Node {node}: already has {current_hugepages} hugepages, no change needed.")
        else:
            logger.debug(f"Node {node}: has {current_hugepages} hugepages, setting to {hugepages_needed}...")
            cmd = f"echo {hugepages_needed} | sudo tee /sys/devices/system/node/node{node}/hugepages/hugepages-2048kB/nr_hugepages"
            subprocess.run(cmd, shell=True, check=True)
            logger.debug(f"Node {node}: hugepages updated to {hugepages_needed}.")

    except FileNotFoundError:
        logger.error(f"Node {node}: Hugepage path not found. Is hugepage support enabled?")
    except PermissionError:
        logger.error(f"Node {node}: Permission denied. Run the script as root.")
    except Exception as e:
        logger.error(f"Node {node}: Error occurred: {e}")


def validate_node_config(node):
    required_top_fields = [
        "socket", "cpu_mask", "isolated", "l-cores", "number_of_alcemls",
        "distribution", "ssd_pcis", "nic_ports", "number_of_distribs",
        "small_pool_count", "large_pool_count", "max_lvol", "max_size",
        "huge_page_memory", "sys_memory"
    ]

    required_distribution_fields = [
        "app_thread_core", "jm_cpu_core", "poller_cpu_cores",
        "alceml_cpu_cores", "alceml_worker_cpu_cores",
        "distrib_cpu_cores", "jc_singleton_core"
    ]

    # Check top-level fields
    for field in required_top_fields:
        if field not in node:
            logger.error(f"Missing required top-level field '{field}' in node: {node.get('socket')}")
            return False

    # Check distribution subfields
    distribution = node["distribution"]
    for field in required_distribution_fields:
        if field not in distribution:
            logger.error(f"Missing required distribution field '{field}' in node: {node.get('socket')}")
            return False

    # Check ssd_pcis fields
    for ssd in node["ssd_pcis"]:
        if not is_valid_pci_address(ssd):
            logger.error(f"Missing required SSD field '{ssd}' in node: {node.get('socket')}")
            return False

    if not node["isolated"]:
        logger.error(f"'isolated' list is empty in node: {node.get('socket')}")
        return False

    if not node["l-cores"]:
        logger.error(f"'l-cores' string is empty in node: {node.get('socket')}")
        return False
    return True


def is_valid_pci_address(address):
    pattern = r'^[0-9a-fA-F]{4}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]$'
    return re.fullmatch(pattern, address) is not None


def get_system_cores():
    """Reads the available system cores from /sys/devices/system/cpu."""
    cpu_dir = "/sys/devices/system/cpu/"
    cores = []
    for entry in os.listdir(cpu_dir):
        if entry.startswith("cpu") and entry[3:].isdigit():
            cores.append(int(entry[3:]))
    return set(cores)


def validate_config(config, upgrade=False):
    if "nodes" not in config:
        logger.error("Missing 'nodes' in config")
        return False
    all_isolated_cores = set()
    for node in config["nodes"]:
        required_keys = [
            "socket", "cpu_mask", "isolated", "l-cores", "distribution",
            "ssd_pcis", "nic_ports", "number_of_distribs",
            "small_pool_count", "large_pool_count", "max_lvol",
            "max_size", "huge_page_memory", "sys_memory"
        ]
        for key in required_keys:
            if key not in node:
                logger.error(f"Missing key '{key}' in node config")
                return False
        if upgrade:
            continue
        # Validate that cpu_mask includes isolated cores
        cpu_mask_value = int(node["cpu_mask"], 16)
        for core in node["isolated"]:
            if not (cpu_mask_value & (1 << core)):
                logger.error(f"Core {core} from 'isolated' is not included in 'cpu_mask' {node['cpu_mask']}")
                return False

        # Validate l-cores syntax
        l_cores_pairs = node["l-cores"].split(",")
        core_to_index = {}
        index_to_core = {}
        for pair in l_cores_pairs:
            if "@" not in pair:
                logger.error(f"Invalid l-cores format in node {node['socket']}: '{pair}'")
                return False
            index_str, core_str = pair.split("@")
            if not index_str.isdigit() or not core_str.isdigit():
                logger.error(f"Invalid l-cores entry in node {node['socket']}: '{pair}'")
                return False
            core = int(core_str)
            index = int(index_str)
            if core in core_to_index:
                logger.error(f"Duplicate core {core} in l-cores for node {node['socket']}")
                return False
            core_to_index[core] = index
            index_to_core[index] = core

        # Check that all cores in 'isolated' are also in l-cores
        for core in node["isolated"]:
            if core not in core_to_index:
                logger.error(f"Core {core} in 'isolated' not present in 'l-cores' for node {node['socket']}")
                return False

        # Validate distribution cores
        distribution = node["distribution"]
        for key, cores in distribution.items():
            if not isinstance(cores, list):
                logger.error(f"Distribution key '{key}' must be a list")
                return False
            for core in cores:
                if core not in index_to_core:
                    logger.error(f"Core {core} in distribution '{key}' not found in l-cores for node {node['socket']}")
                    return False
        system_cores = get_system_cores()
        # Check isolated cores are subset of system cores
        for core in node["isolated"]:
            if core not in system_cores:
                raise ValueError(f"Core {core} in node {node['socket']} is not a valid system core")

        # Check no core is used in more than one node
        node_cores_set = set(node["isolated"])
        if all_isolated_cores.intersection(node_cores_set):
            logger.error(f"Duplicate isolated cores found between nodes: {all_isolated_cores.intersection(node_cores_set)}")
            return False
        all_isolated_cores.update(node_cores_set)
    if upgrade:
        return True
    return all_isolated_cores


def get_k8s_apps_client():
    config.load_incluster_config()
    return client.AppsV1Api()

def get_k8s_core_client():
    config.load_incluster_config()
    return client.CoreV1Api()


def remove_container(client: docker.DockerClient, name, timeout=3):
    try:
        container = client.containers.get(name)
        container.stop(timeout=timeout)
        container.remove()
    except docker.errors.NotFound:
        pass
    except docker.errors.APIError as e:
        if 'Conflict ("removal of container {container.id} is already in progress")' != e.response.reason:
            raise
