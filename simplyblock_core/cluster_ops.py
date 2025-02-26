# coding=utf-8
import datetime
import json
import logging
import os
import re
import tempfile
import shutil
import subprocess
import time
import uuid
from tracemalloc import Snapshot

import docker
import requests
from jinja2 import Environment, FileSystemLoader

from simplyblock_core import utils, scripts, constants, mgmt_node_ops, storage_node_ops, distr_controller, shell_utils
from simplyblock_core.controllers import cluster_events, device_controller, storage_events, pool_controller, \
    lvol_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.stats import StatsObject
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()
TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def _create_update_user(cluster_id, grafana_url, grafana_secret, user_secret, update_secret=False):
    session = requests.session()
    session.auth = ("admin", grafana_secret)
    headers = {
        'X-Requested-By': '',
        'Content-Type': 'application/json',
    }
    retries = 5
    if update_secret:
        url = f"{grafana_url}/api/users/lookup?loginOrEmail={cluster_id}"
        response = session.request("GET", url, headers=headers)
        userid = response.json().get("id")
        
        payload = json.dumps({
            "password": user_secret
        })
        
        url = f"{grafana_url}/api/admin/users/{userid}/password"

        while retries > 0:
            response = session.request("PUT", url, headers=headers, data=payload)
            if response.status_code == 200:
                logger.debug(f"user create/update {cluster_id} succeeded")
                return response.status_code == 200
            logger.debug(response.status_code)
            logger.debug("waiting for grafana api to come up")
            retries -= 1
            time.sleep(3)

    else:
        payload = json.dumps({
            "name": cluster_id,
            "login": cluster_id,
            "password": user_secret
        })
        url = f"{grafana_url}/api/admin/users"
        while retries > 0:
            response = session.request("POST", url, headers=headers, data=payload)
            if response.status_code == 200:
                logger.debug(f"user create/update {cluster_id} succeeded")
                return response.status_code == 200
            logger.debug(response.status_code)
            logger.debug("waiting for grafana api to come up")
            retries -= 1
            time.sleep(3)


def _add_graylog_input(cluster_ip, password):
    url = f"http://{cluster_ip}/graylog/api/system/inputs"
    payload = json.dumps({
        "title": "spdk log input",
        "type": "org.graylog2.inputs.gelf.tcp.GELFTCPInput",
        "configuration": {
            "bind_address": "0.0.0.0",
            "port": 12201,
            "recv_buffer_size": 262144,
            "number_worker_threads": 2,
            "override_source": None,
            "charset_name": "UTF-8",
            "decompress_size_limit": 8388608
        },
        "global": True
    })
    headers = {
        'X-Requested-By': '',
        'Content-Type': 'application/json',
    }
    session = requests.session()
    session.auth = ("admin", password)
    response = session.request("POST", url, headers=headers, data=payload)
    logger.debug(response.text)
    return response.status_code == 201

def _set_max_result_window(cluster_ip, max_window=100000):
    url_existing_indices = f"http://{cluster_ip}:9200/_all/_settings"
    payload_existing = json.dumps({
        "settings": {
            "index.max_result_window": max_window
        }
    })
    headers = {
        'Content-Type': 'application/json',
    }
    response = requests.put(url_existing_indices, headers=headers, data=payload_existing)
    if response.status_code == 200:
        logger.info("Settings updated for existing indices.")
    else:
        logger.error(f"Failed to update settings for existing indices: {response.text}")
        return False
    
    url_template = f"http://{cluster_ip}:9200/_template/all_indices_template"
    payload_template = json.dumps({
        "index_patterns": ["*"],
        "settings": {
            "index.max_result_window": max_window
        }
    })
    response_template = requests.put(url_template, headers=headers, data=payload_template)
    if response_template.status_code == 200:
        logger.info("Template created for future indices.")
        return True
    else:
        logger.error(f"Failed to create template for future indices: {response_template.text}")
        return False

def create_cluster(blk_size, page_size_in_blocks, cli_pass,
                   cap_warn, cap_crit, prov_cap_warn, prov_cap_crit, ifname, log_del_interval, metrics_retention_period,
                   contact_point, grafana_endpoint, distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type,
                   enable_node_affinity, qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity):

    logger.info("Installing dependencies...")
    ret = scripts.install_deps()
    logger.info("Installing dependencies > Done")

    if not ifname:
        ifname = "eth0"

    DEV_IP = utils.get_iface_ip(ifname)
    if not DEV_IP:
        logger.error(f"Error getting interface ip: {ifname}")
        return False

    logger.info(f"Node IP: {DEV_IP}")
    ret = scripts.configure_docker(DEV_IP)

    db_connection = f"{utils.generate_string(8)}:{utils.generate_string(32)}@{DEV_IP}:4500"
    ret = scripts.set_db_config(db_connection)

    logger.info("Configuring docker swarm...")
    c = docker.DockerClient(base_url=f"tcp://{DEV_IP}:2375", version="auto")
    try:
        if c.swarm.attrs and "ID" in c.swarm.attrs:
            logger.info("Docker swarm found, leaving swarm now")
            c.swarm.leave(force=True)
            try:
                c.volumes.get("monitoring_grafana_data").remove(force=True)
            except:
                pass
            time.sleep(3)

        c.swarm.init(DEV_IP)
        logger.info("Configuring docker swarm > Done")
    except Exception as e:
        print(e)

    if not cli_pass:
        cli_pass = utils.generate_string(10)

    # validate cluster duplicate
    logger.info("Adding new cluster object")
    c = Cluster()
    c.uuid = str(uuid.uuid4())
    c.blk_size = blk_size
    c.page_size_in_blocks = page_size_in_blocks
    c.nqn = f"{constants.CLUSTER_NQN}:{c.uuid}"
    c.cli_pass = cli_pass
    c.secret = utils.generate_string(20)
    c.grafana_secret = c.secret
    c.db_connection = db_connection
    if cap_warn and cap_warn > 0:
        c.cap_warn = cap_warn
    if cap_crit and cap_crit > 0:
        c.cap_crit = cap_crit
    if prov_cap_warn and prov_cap_warn > 0:
        c.prov_cap_warn = prov_cap_warn
    if prov_cap_crit and prov_cap_crit > 0:
        c.prov_cap_crit = prov_cap_crit
    if distr_ndcs == 0 and distr_npcs == 0:
        c.distr_ndcs = 1
        c.distr_npcs = 1
    else:
        c.distr_ndcs = distr_ndcs
        c.distr_npcs = distr_npcs
    c.distr_bs = distr_bs
    c.distr_chunk_bs = distr_chunk_bs
    c.ha_type = ha_type
    if grafana_endpoint:
        c.grafana_endpoint = grafana_endpoint
    else:
        c.grafana_endpoint = f"http://{DEV_IP}/grafana"
    c.enable_node_affinity = enable_node_affinity
    c.qpair_count = qpair_count or constants.QPAIR_COUNT

    c.max_queue_size = max_queue_size
    c.inflight_io_threshold = inflight_io_threshold
    c.enable_qos = enable_qos
    c.strict_node_anti_affinity = strict_node_anti_affinity

    alerts_template_folder = os.path.join(TOP_DIR, "simplyblock_core/scripts/alerting/")
    alert_resources_file = "alert_resources.yaml"

    env = Environment(loader=FileSystemLoader(alerts_template_folder), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template(f'{alert_resources_file}.j2')

    slack_pattern = re.compile(r"https://hooks\.slack\.com/services/\S+")
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

    if slack_pattern.match(contact_point):
        ALERT_TYPE = "slack"
    elif email_pattern.match(contact_point):
        ALERT_TYPE = "email"
    else:
        ALERT_TYPE = "slack"
        contact_point = 'https://hooks.slack.com/services/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'

    values = {
        'CONTACT_POINT': contact_point,
        'GRAFANA_ENDPOINT': c.grafana_endpoint,
        'ALERT_TYPE': ALERT_TYPE,
    }

    temp_dir = tempfile.mkdtemp()

    temp_file_path = os.path.join(temp_dir, alert_resources_file)
    with open(temp_file_path, 'w') as file:
        file.write(template.render(values))

    destination_file_path = os.path.join(alerts_template_folder, alert_resources_file)
    try:
        subprocess.run(['sudo', '-v'], check=True)  # sudo -v checks if the current user has sudo permissions
        subprocess.run(['sudo', 'mv', temp_file_path, destination_file_path], check=True)
        print(f"File moved to {destination_file_path} successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred: {e}")
    shutil.rmtree(temp_dir)

    logger.info("Deploying swarm stack ...")
    log_level = "DEBUG" if constants.LOG_WEB_DEBUG else "INFO"
    ret = scripts.deploy_stack(cli_pass, DEV_IP, constants.SIMPLY_BLOCK_DOCKER_IMAGE, c.secret, c.uuid,
                               log_del_interval, metrics_retention_period, log_level, c.grafana_endpoint)
    logger.info("Deploying swarm stack > Done")

    if ret == 0:
        logger.info("deploying swarm stack succeeded")
    else:
        logger.error("deploying swarm stack failed")

    logger.info("Configuring DB...")
    out = scripts.set_db_config_single()
    logger.info("Configuring DB > Done")

    _set_max_result_window(DEV_IP)

    _add_graylog_input(DEV_IP, c.secret)

    _create_update_user(c.uuid, c.grafana_endpoint, c.grafana_secret, c.secret)

    c.status = Cluster.STATUS_UNREADY
    c.create_dt = str(datetime.datetime.now())
    db_controller = DBController()
    c.write_to_db(db_controller.kv_store)

    cluster_events.cluster_create(c)

    mgmt_node_ops.add_mgmt_node(DEV_IP, c.uuid)

    logger.info("New Cluster has been created")
    logger.info(c.uuid)
    return c.uuid


def add_cluster(blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
                distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity, qpair_count,
                max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity):
    db_controller = DBController()
    clusters = db_controller.get_clusters()
    if not clusters:
        logger.error("No previous clusters found!")
        return False

    logger.info("Adding new cluster")
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.blk_size = blk_size
    cluster.page_size_in_blocks = page_size_in_blocks
    cluster.nqn = f"{constants.CLUSTER_NQN}:{cluster.uuid}"
    cluster.secret = utils.generate_string(20)
    cluster.strict_node_anti_affinity = strict_node_anti_affinity

    default_cluster = clusters[0]
    cluster.db_connection = default_cluster.db_connection
    cluster.grafana_secret = default_cluster.grafana_secret
    cluster.grafana_endpoint = default_cluster.grafana_endpoint

    _create_update_user(cluster.uuid, cluster.grafana_endpoint, cluster.grafana_secret, cluster.secret)

    if distr_ndcs == 0 and distr_npcs == 0:
        cluster.distr_ndcs = 2
        cluster.distr_npcs = 1
    else:
        cluster.distr_ndcs = distr_ndcs
        cluster.distr_npcs = distr_npcs
    cluster.distr_bs = distr_bs
    cluster.distr_chunk_bs = distr_chunk_bs
    cluster.ha_type = ha_type
    cluster.enable_node_affinity = enable_node_affinity
    cluster.qpair_count = qpair_count or constants.QPAIR_COUNT
    cluster.max_queue_size = max_queue_size
    cluster.inflight_io_threshold = inflight_io_threshold
    cluster.enable_qos = enable_qos
    if cap_warn and cap_warn > 0:
        cluster.cap_warn = cap_warn
    if cap_crit and cap_crit > 0:
        cluster.cap_crit = cap_crit
    if prov_cap_warn and prov_cap_warn > 0:
        cluster.prov_cap_warn = prov_cap_warn
    if prov_cap_crit and prov_cap_crit > 0:
        cluster.prov_cap_crit = prov_cap_crit

    cluster.status = Cluster.STATUS_UNREADY
    cluster.create_dt = str(datetime.datetime.now())
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_create(cluster)

    return cluster.get_id()


def cluster_activate(cl_id, force=False, force_lvstore_create=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False
    if cluster.status == Cluster.STATUS_ACTIVE:
        logger.warning("Cluster is ACTIVE")
        if not force:
            logger.warning(f"Failed to activate cluster, Cluster is in an ACTIVE state, use --force to reactivate")
            return False

    ols_status = cluster.status
    set_cluster_status(cl_id, Cluster.STATUS_IN_ACTIVATION)
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    online_nodes = []
    dev_count = 0

    for node in snodes:
        if node.is_secondary_node:
            continue
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1
    minimum_devices = cluster.distr_ndcs + cluster.distr_npcs + 1
    if dev_count < minimum_devices:
        logger.error(f"Failed to activate cluster, No enough online device.. Minimum is {minimum_devices}")
        set_cluster_status(cl_id, ols_status)
        return False

    records = db_controller.get_cluster_capacity(cluster)
    max_size = records[0]['size_total']

    if cluster.ha_type == "ha":
        for snode in snodes:
            if snode.is_secondary_node or snode.secondary_node_id:
                continue
            secondary_nodes = storage_node_ops.get_secondary_nodes(snode)
            if not secondary_nodes:
                logger.error(f"Failed to activate cluster, No enough secondary nodes")
                set_cluster_status(cl_id, ols_status)
                return False
            snode.secondary_node_id = secondary_nodes[0]
            snode.write_to_db()

    for snode in snodes:
        if snode.is_secondary_node:
            continue
        if snode.status != StorageNode.STATUS_ONLINE:
            continue
        if snode.lvstore and force_lvstore_create is False:
            logger.warning(f"Node {snode.get_id()} already has lvstore {snode.lvstore}")
            ret = storage_node_ops.recreate_lvstore(snode)
        else:
            ret = storage_node_ops.create_lvstore(snode, cluster.distr_ndcs, cluster.distr_npcs, cluster.distr_bs,
                                              cluster.distr_chunk_bs, cluster.page_size_in_blocks, max_size)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        if ret:
            snode.lvstore_status = "ready"
            snode.write_to_db()

        else:
            snode.lvstore_status = "failed"
            snode.write_to_db()
            logger.error(f"Failed to restore lvstore on node {snode.get_id()}")
            if not force:
                logger.error("Failed to activate cluster")
                set_cluster_status(cl_id, ols_status)
                return False

    for snode in snodes:
        if not snode.is_secondary_node:
            continue
        if snode.status != StorageNode.STATUS_ONLINE:
            continue

        ret = storage_node_ops.recreate_lvstore(snode)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        if ret:
            snode.lvstore_status = "ready"
            snode.write_to_db()

        else:
            snode.lvstore_status = "failed"
            snode.write_to_db()

            logger.error(f"Failed to restore lvstore on node {snode.get_id()}")
            if not force:
                logger.error("Failed to activate cluster")
                set_cluster_status(cl_id, ols_status)
                return False



    if not cluster.cluster_max_size:
        cluster = db_controller.get_cluster_by_id(cl_id)
        cluster.cluster_max_size = max_size
        cluster.cluster_max_devices = dev_count
        cluster.cluster_max_nodes = len(online_nodes)
        cluster.write_to_db(db_controller.kv_store)
    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    logger.info("Cluster activated successfully")
    return True


def get_cluster_status(cl_id, is_json=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    data = []
    for node in st:
        for dev in node.nvme_devices:
            data.append({
                "UUID": dev.get_id(),
                "Storage ID": dev.cluster_device_order,
                "Size": utils.humanbytes(dev.size),
                "Hostname": node.hostname,
                "Status": dev.status,
                "IO Error": dev.io_error,
                "Health": dev.health_check
            })
    data = sorted(data, key=lambda x: x["Storage ID"])
    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


def set_cluster_status(cl_id, status):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    if cluster.status == status:
        return True

    old_status = cluster.status
    cluster.status = status
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_status_change(cluster, cluster.status, old_status)
    return True


def suspend_cluster(cl_id):
    return set_cluster_status(cl_id, Cluster.STATUS_SUSPENDED)


def unsuspend_cluster(cl_id):
    return set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)


def degrade_cluster(cl_id):
    return set_cluster_status(cl_id, Cluster.STATUS_DEGRADED)


def cluster_set_read_only(cl_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    if cluster.status == Cluster.STATUS_READONLY:
        return True

    ret = set_cluster_status(cl_id, Cluster.STATUS_READONLY)
    if ret:
        st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
        for node in st:
            rpc_client = RPCClient(
                node.mgmt_ip, node.rpc_port,
                node.rpc_username, node.rpc_password, timeout=5, retry=2)

            for bdev in node.lvstore_stack:
                if bdev['type'] == "bdev_distr":
                    rpc_client.bdev_distrib_toggle_cluster_full(bdev['name'], cluster_full=True)

    return True


def cluster_set_active(cl_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    if cluster.status == Cluster.STATUS_ACTIVE:
        return True

    ret = set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    if ret:
        st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
        for node in st:
            rpc_client = RPCClient(
                node.mgmt_ip, node.rpc_port,
                node.rpc_username, node.rpc_password, timeout=5, retry=2)

            for bdev in node.lvstore_stack:
                if bdev['type'] == "bdev_distr":
                    rpc_client.bdev_distrib_toggle_cluster_full(bdev['name'], cluster_full=False)

    return True


def list():
    db_controller = DBController()
    cls = db_controller.get_clusters()
    mt = db_controller.get_mgmt_nodes()

    data = []
    for cl in cls:
        st = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        status = cl.status
        if cl.is_re_balancing and status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
            status = f"{status} - ReBalancing"
        data.append({
            "UUID": cl.get_id(),
            "NQN": cl.nqn,
            "ha_type": cl.ha_type,
            "#mgmt": len(mt),
            "#storage": len(st),
            "Mod": f"{cl.distr_ndcs}x{cl.distr_npcs}",
            "Status": status.upper(),
        })
    return utils.print_table(data)



def list_all_info(cluster_id):
    db_controller = DBController()
    cl = db_controller.get_cluster_by_id(cluster_id)
    if not cl:
        logger.error(f"Cluster not found {cluster_id}")
        return False

    mt = db_controller.get_mgmt_nodes()
    mt_online = [m for m in mt if m.status == MgmtNode.STATUS_ONLINE]

    data = []

    st = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
    st_online = [s for s in st if s.status == StorageNode.STATUS_ONLINE]

    pools = db_controller.get_pools(cluster_id)
    p_online = [p for p in pools if p.status == Pool.STATUS_ACTIVE]

    lvols = db_controller.get_lvols(cluster_id)
    lv_online = [p for p in lvols if p.status == LVol.STATUS_ONLINE]

    snaps = [sn for sn in db_controller.get_snapshots() if sn.cluster_id == cluster_id]

    devs = []
    devs_online = []
    for n in st:
        for dev in n.nvme_devices:
            devs.append(dev)
            if dev.status == NVMeDevice.STATUS_ONLINE:
                devs_online.append(dev)

    records = db_controller.get_cluster_capacity(cl, 1)
    if records:
        rec = records[0]
    else:
        rec = StatsObject()

    task_total = 0
    task_running = 0
    task_pending = 0
    for task in db_controller.get_job_tasks(cl.get_id()):
        task_total += 1
        if task.status == JobSchedule.STATUS_RUNNING:
            task_running += 1
        elif task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
            task_pending += 1


    data.append({
        "Cluster UUID": cl.get_id(),
        "Type": cl.ha_type.upper(),
        "Mod": f"{cl.distr_ndcs}x{cl.distr_npcs}",

        "Mgmt Nodes": f"{len(mt)}/{len(mt_online)}",
        "Storage Nodes": f"{len(st)}/{len(st_online)}",
        "Devices": f"{len(devs)}/{len(devs_online)}",
        "Pools": f"{len(pools)}/{len(p_online)}",
        "Lvols": f"{len(lvols)}/{len(lv_online)}",
        "Snaps": f"{len(snaps)}",

        "Tasks total": f"{task_total}",
        "Tasks running": f"{task_running}",
        "Tasks pending": f"{task_pending}",
        #
        # "Size total": f"{utils.humanbytes(rec.size_total)}",
        # "Size Used": f"{utils.humanbytes(rec.size_used)}",
        # "Size prov": f"{utils.humanbytes(rec.size_prov)}",
        # "Size util": f"{rec.size_util}%",
        # "Size prov util": f"{rec.size_prov_util}%",

        "Status": cl.status,

    })

    out = utils.print_table(data, title="Cluster Info")
    out += "\n"

    data = []

    data.append({
        "Cluster UUID": cl.uuid,
        # "Type": "Cluster Object",
        # "Devices": f"{len(devs)}/{len(devs_online)}",
        # "Lvols": f"{len(lvols)}/{len(lv_online)}",

        "Size prov": f"{utils.humanbytes(rec.size_prov)}",
        "Size Used": f"{utils.humanbytes(rec.size_used)}",
        "Size free": f"{utils.humanbytes(rec.size_free)}",
        "Size %": f"{rec.size_util}%",
        "Size prov %": f"{rec.size_prov_util}%",

        "Read BW/s": f"{utils.humanbytes(rec.read_bytes_ps)}",
        "Write BW/s": f"{utils.humanbytes(rec.write_bytes_ps)}",
        "Read IOP/s": f"{rec.read_io_ps}",
        "Write IOP/s": f"{rec.write_io_ps}",

        "Health": "True",
        "Status": cl.status,

    })

    out += "\n"
    out += utils.print_table(data, title="Cluster Stats")
    out += "\n"

    data = []

    dev_data = []

    for node in st:
        records = db_controller.get_node_capacity(node, 1)
        if records:
            rec = records[0]
        else:
            rec = StatsObject()

        lvs = db_controller.get_lvols_by_node_id(node.get_id()) or []
        total_devices = len(node.nvme_devices)
        online_devices = 0
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1

        data.append({
            "Storage node UUID": node.uuid,
            "Devices": f"{total_devices}/{online_devices}",
            "LVols": f"{len(lvs)}",

            "Size prov": f"{utils.humanbytes(rec.size_total)}",
            "Size Used": f"{utils.humanbytes(rec.size_used)}",
            "Size free": f"{utils.humanbytes(rec.size_free)}",
            "Size %": f"{rec.size_util}%",
            "Size prov %": f"{rec.size_prov_util}%",

            "Read BW/s": f"{utils.humanbytes(rec.read_bytes_ps)}",
            "Write BW/s": f"{utils.humanbytes(rec.write_bytes_ps)}",
            "Read IOP/s": f"{rec.read_io_ps}",
            "Write IOP/s": f"{rec.write_io_ps}",

            "Status": node.status,

        })

        for dev in node.nvme_devices:
            records = db_controller.get_device_capacity(dev)
            if records:
                rec = records[0]
            else:
                rec = StatsObject()

            dev_data.append({
                "Device UUID": dev.uuid,

                "Size total": f"{utils.humanbytes(rec.size_total)}",
                "Size Used": f"{utils.humanbytes(rec.size_used)}",
                "Size free": f"{utils.humanbytes(rec.size_free)}",
                "Size %": f"{rec.size_util}%",
                # "Size prov %": f"{rec.size_prov_util}%",

                "Read BW/s": f"{utils.humanbytes(rec.read_bytes_ps)}",
                "Write BW/s": f"{utils.humanbytes(rec.write_bytes_ps)}",
                "Read IOP/s": f"{rec.read_io_ps}",
                "Write IOP/s": f"{rec.write_io_ps}",

                "Health": dev.health_check,
                "Status": dev.status,

            })

    out += "\n"
    if data:
        out +=  utils.print_table(data, title="Storage Nodes Stats")
        out += "\n"

    out += "\n"
    if dev_data:
        out +=  utils.print_table(dev_data, title="Storage Devices Stats")
        out += "\n"

    lvol_data = []
    for lvol in db_controller.get_lvols(cluster_id):
        records = db_controller.get_lvol_stats(lvol, 1)
        if records:
            rec = records[0]
        else:
            rec = StatsObject()

        lvol_data.append({
            "LVol UUID": lvol.uuid,

            "Size prov": f"{utils.humanbytes(rec.size_total)}",
            "Size Used": f"{utils.humanbytes(rec.size_used)}",
            "Size free": f"{utils.humanbytes(rec.size_free)}",
            "Size %": f"{rec.size_util}%",
            # "Size prov %": f"{rec.size_prov_util}%",

            "Read BW/s": f"{utils.humanbytes(rec.read_bytes_ps)}",
            "Write BW/s": f"{utils.humanbytes(rec.write_bytes_ps)}",
            "Read IOP/s": f"{rec.read_io_ps}",
            "Write IOP/s": f"{rec.write_io_ps}",

            "Connections": f"{rec.connected_clients}",
            "Health": lvol.health_check,
            "Status": lvol.status,

        })

    out += "\n"
    if lvol_data:
        out += utils.print_table(lvol_data, title="LVol Stats")
        out += "\n"

    return out


def get_capacity(cluster_id, history, records_count=20, is_json=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_cluster_capacity(cluster, records_number)

    cap_stats_keys = [
        "date",
        "size_total",
        "size_prov",
        "size_used",
        "size_free",
        "size_util",
        "size_prov_util",
    ]
    new_records = utils.process_records(records, records_count, keys=cap_stats_keys)

    if is_json:
        return json.dumps(new_records, indent=2)

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Absolut": utils.humanbytes(record['size_total']),
            "Provisioned": utils.humanbytes(record['size_prov']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
            "Prov Util %": f"{record['size_prov_util']}%",
        })
    return out


def get_iostats_history(cluster_id, history_string, records_count=20, parse_sizes=True, with_sizes=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False

    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    if not nodes:
        logger.error("no nodes found")
        return False

    if history_string:
        records_number = utils.parse_history_param(history_string)
        if not records_number:
            logger.error(f"Error parsing history string: {history_string}")
            return False
    else:
        records_number = 20

    records = db_controller.get_cluster_stats(cluster, records_number)

    io_stats_keys = [
        "date",
        "read_bytes",
        "read_bytes_ps",
        "read_io_ps",
        "read_io",
        "read_latency_ps",
        "write_bytes",
        "write_bytes_ps",
        "write_io",
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
    # combine records
    new_records = utils.process_records(records, records_count, keys=io_stats_keys)

    if not parse_sizes:
        return new_records

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
    return out


def get_ssh_pass(cluster_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False
    return cluster.cli_pass


def get_secret(cluster_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False
    return cluster.secret


def set_secret(cluster_id, secret):
    
    db_controller = DBController()

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False

    secret = secret.strip()
    if len(secret) < 20:
        return "Secret must be at least 20 char"
    
    _create_update_user(cluster_id, cluster.grafana_endpoint, cluster.grafana_secret, secret, update_secret=True)
    
    cluster.secret = secret
    cluster.write_to_db(db_controller.kv_store)
    
    return "Done"


def get_logs(cluster_id, is_json=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error(f"Cluster not found {cluster_id}")
        return False

    events = db_controller.get_events(cluster_id)
    out = []
    for record in events:
        logger.debug(record)
        Storage_ID = None
        if 'storage_ID' in record.object_dict:
            Storage_ID = record.object_dict['storage_ID']

        vuid = None
        if 'vuid' in record.object_dict:
            vuid = record.object_dict['vuid']

        msg =  record.message
        if record.event in ["device_status", "node_status"]:
            msg = msg+f" ({record.count})"

        out.append({
            "Date": record.get_date_string(),
            "NodeId": record.node_id,
            "Event": record.event,
            "Level": record.event_level,
            "Message":msg,
            "Storage_ID": str(Storage_ID),
            "VUID": str(vuid),
            "Status": record.status,
        })
    if is_json:
        return json.dumps(out, indent=2)
    else:
        return utils.print_table(out)


def get_cluster(cl_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    return json.dumps(cluster.get_clean_dict(), indent=2, sort_keys=True)


def update_cluster(cl_id, mgmt_only=False, restart_cluster=False):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    if cluster.status != Cluster.STATUS_ACTIVE:
        logger.error(f"Cluster is not active")
        return False


    try:
        sbcli=constants.SIMPLY_BLOCK_CLI_NAME
        out, _, ret_code = shell_utils.run_command(f"pip install {sbcli} --upgrade")
        if ret_code == 0:
            logger.info(f"{sbcli} upgraded")
    except Exception as e:
        logger.error(e)

    try:
        logger.info("Updating mgmt cluster")
        cluster_docker = utils.get_docker_client(cl_id)
        logger.info(f"Pulling image {constants.SIMPLY_BLOCK_DOCKER_IMAGE}")
        cluster_docker.images.pull(constants.SIMPLY_BLOCK_DOCKER_IMAGE)
        image_without_tag = constants.SIMPLY_BLOCK_DOCKER_IMAGE.split(":")[0]
        for service in cluster_docker.services.list():
            if image_without_tag in service.attrs['Spec']['Labels']['com.docker.stack.image']:
                logger.info(f"Updating service {service.name}")
                service.update(image=constants.SIMPLY_BLOCK_DOCKER_IMAGE, force_update=True)
        logger.info("Done updating mgmt cluster")
    except Exception as e:
        print(e)

    if not mgmt_only:
        set_cluster_status(cl_id, Cluster.STATUS_SUSPENDED)
        logger.info("updating storage nodes")
        for node in db_controller.get_storage_nodes_by_cluster_id(cl_id):
            if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
                try:
                    storage_node_ops.start_storage_node_api_container(node.mgmt_ip)
                except Exception as e:
                    logger.error(e)

        if restart_cluster:
            logger.info("Restarting cluster")
            ret = cluster_grace_shutdown(cl_id)
            if ret:
                cluster_grace_startup(cl_id)

    logger.info("Done")
    return True


def cluster_grace_startup(cl_id, clear_data=False, spdk_image=None):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False
    # logger.info(f"Unsuspending cluster: {cl_id}")
    # unsuspend_cluster(cl_id)

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        logger.info(f"Restarting node: {node.get_id()}")
        storage_node_ops.restart_storage_node(node.get_id(), clear_data=clear_data, force=True, spdk_image=spdk_image)
        # time.sleep(5)
        get_node = db_controller.get_storage_node_by_id(node.get_id())
        if get_node.status != StorageNode.STATUS_ONLINE:
            logger.error("failed to restart node")

    return True


def cluster_grace_shutdown(cl_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        if node.status == StorageNode.STATUS_ONLINE:
            logger.info(f"Suspending node: {node.get_id()}")
            storage_node_ops.suspend_storage_node(node.get_id())
            logger.info(f"Shutting down node: {node.get_id()}")
            storage_node_ops.shutdown_storage_node(node.get_id())

    return True


def delete_cluster(cl_id):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        logger.error(f"Cluster not found {cl_id}")
        return False

    nodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    if nodes:
        logger.error("Can only remove Empty cluster, Storage nodes found")
        return False

    pools = db_controller.get_pools(cl_id)
    if pools:
        logger.error("Can only remove Empty cluster, Pools found")
        return False

    if len(db_controller.get_clusters()) == 1 :
        logger.error("Can not remove the last cluster!")
        return False

    logger.info(f"Deleting Cluster {cl_id}")
    cluster_events.cluster_delete(cluster)
    cluster.remove(db_controller.kv_store)
    logger.info("Done")

def open_db_from_zip(fip_path):
    import boto3
    s3 = boto3.client('s3')


    out = '/tmp/fdb.zip'
    try:
        os.remove(out)
    except:
        pass

    buket_name = 'simplyblock-e2e-test-logs'
    file_name = ""
    if fip_path.startswith('s3://'):
        # s3://simplyblock-e2e-test-logs/12220160320/mgmt/fdb.zip
        buket_name = fip_path.split("/")[2]
        file_name = "/".join(fip_path.split("/")[3:])


    elif len(fip_path.split('/'))<=3:
        # /12220160320/mgmt/fdb.zip
        file_name = fip_path

    elif fip_path.startswith('https://'):
        #https://simplyblock-e2e-test-logs.s3.us-east-2.amazonaws.com/12220160320/mgmt/fdb.zip
        buket_name = fip_path.split("/")[2]
        buket_name = buket_name.split(".")[0]
        file_name = "/".join(fip_path.split("/")[3:])
    else:
        file_name = fip_path

    try:
        ret = s3.download_file(buket_name, file_name, out)
    except Exception as e:
        logger.error(e)

    if os.path.exists(out):
        scripts.deploy_fdb_from_file_service(out)



def cluster_reset():
    """


set -x

CMD=$(ls ~/.local/bin/sbcli-* | awk '{n=split($0,a,"/"); print a[n]}')
cl=$($CMD cluster list | tail -n -3 | awk '{print $2}')

#$CMD cluster graceful-shutdown $cl

for sn_id in $($CMD sn list | grep / | awk '{print $2}'); do
  $CMD -d sn shutdown --force $sn_id
done

sudo mv /etc/foundationdb/fdb.cluster /etc/foundationdb/fdb.cluster.bck
for service_id in $(docker service ls | grep / | awk '{print $1}'); do
  docker service update "$service_id" --force --detach
done

# restore
fdb_cont=$(sudo docker ps | grep "app_fdb-server" | awk '{print $1}')
sudo docker rm --force $fdb_cont
sudo rm -rf /etc/foundationdb/data/*
fdbcli --exec "configure new single ssd ; writemode on ; clearrange \"\" \\xff" -C /etc/foundationdb/fdb.cluster.bck
BF=$(fdbbackup list -b file:///etc/foundationdb/backup/)
fdbrestore start -r "$BF" --dest-cluster-file /etc/foundationdb/fdb.cluster.bck -t fresh_deploy

sudo mv /etc/foundationdb/fdb.cluster.bck /etc/foundationdb/fdb.cluster

for service_id in $(docker service ls | grep / | awk '{print $1}'); do
  docker service update "$service_id" --force --detach
done

sleep 30
for sn_id in $($CMD sn list | grep / | awk '{print $2}'); do
  $CMD -d sn shutdown --force $sn_id
done

sleep 5
$CMD -d cluster graceful-startup $cl  --clear-data

$CMD -d cluster activate $cl

    """