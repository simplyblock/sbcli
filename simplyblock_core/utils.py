# coding=utf-8
import json
import logging
import os
import random
import re
import string

import docker
from prettytable import PrettyTable

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
        x = PrettyTable(field_names=data[0].keys())
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
    response = os.system(f"ping -c 1 {ip} > /dev/null")
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
