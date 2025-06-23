# coding=utf-8
import datetime
import json
import os
import socket
import subprocess
import time
import uuid
import textwrap
import typing as t

import docker
import requests

from docker.errors import DockerException
from simplyblock_core import utils, scripts, constants, mgmt_node_ops, storage_node_ops
from simplyblock_core.controllers import cluster_events, device_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import LVolStatObject, ClusterStatObject, NodeStatObject, DeviceStatObject
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import pull_docker_image_with_retry

logger = utils.get_logger(__name__)

db_controller = DBController()

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
    response = requests.put(
        f"http://{cluster_ip}:9200/_all/_settings",
        json={"settings": {"index.max_result_window": max_window}},
    )
    response.raise_for_status()
    logger.info("Settings updated for existing indices.")

    response_template = requests.put(
        f"http://{cluster_ip}:9200/_template/all_indices_template",
        json={
            "index_patterns": ["*"],
            "settings": {"index.max_result_window": max_window},
        },
    )
    response_template.raise_for_status()
    logger.info("Template created for future indices.")

def create_cluster(blk_size, page_size_in_blocks, cli_pass,
                   cap_warn, cap_crit, prov_cap_warn, prov_cap_crit, ifname, log_del_interval, metrics_retention_period,
                   contact_point, grafana_endpoint, distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type,
                   enable_node_affinity, qpair_count, max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity) -> str:

    if distr_ndcs == 0 and distr_npcs == 0:
        raise ValueError("both distr_ndcs and distr_npcs cannot be 0")

    logger.info("Installing dependencies...")
    scripts.install_deps()
    logger.info("Installing dependencies > Done")

    if not ifname:
        ifname = "eth0"

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        raise ValueError(f"Error getting interface ip: {ifname}")

    logger.info(f"Node IP: {dev_ip}")
    scripts.configure_docker(dev_ip)

    db_connection = f"{utils.generate_string(8)}:{utils.generate_string(32)}@{dev_ip}:4500"
    scripts.set_db_config(db_connection)

    logger.info("Configuring docker swarm...")
    c = docker.DockerClient(base_url=f"tcp://{dev_ip}:2375", version="auto")
    if c.swarm.attrs and "ID" in c.swarm.attrs:
        logger.info("Docker swarm found, leaving swarm now")
        c.swarm.leave(force=True)
        try:
            c.volumes.get("monitoring_grafana_data").remove(force=True)
        except DockerException:
            pass
        time.sleep(3)

    c.swarm.init(dev_ip)
    logger.info("Configuring docker swarm > Done")
    
    hostname = socket.gethostname()
    current_node = next((node for node in c.nodes.list() if node.attrs["Description"]["Hostname"] == hostname), None)
    if current_node:
        current_spec = current_node.attrs["Spec"]
        current_labels = current_spec.get("Labels", {})
        current_labels["app"] = "graylog"
        current_spec["Labels"] = current_labels

        current_node.update(current_spec)
        
        logger.info(f"Labeled node '{hostname}' with app=graylog")
    else:
        logger.warning("Could not find current node for labeling")

    if not cli_pass:
        cli_pass = utils.generate_string(10)

    # validate cluster duplicate
    logger.info("Adding new cluster object")
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.blk_size = blk_size
    cluster.page_size_in_blocks = page_size_in_blocks
    cluster.nqn = f"{constants.CLUSTER_NQN}:{cluster.uuid}"
    cluster.cli_pass = cli_pass
    cluster.secret = utils.generate_string(20)
    cluster.grafana_secret = cluster.secret
    cluster.db_connection = db_connection
    if cap_warn and cap_warn > 0:
        cluster.cap_warn = cap_warn
    if cap_crit and cap_crit > 0:
        cluster.cap_crit = cap_crit
    if prov_cap_warn and prov_cap_warn > 0:
        cluster.prov_cap_warn = prov_cap_warn
    if prov_cap_crit and prov_cap_crit > 0:
        cluster.prov_cap_crit = prov_cap_crit
    cluster.distr_ndcs = distr_ndcs
    cluster.distr_npcs = distr_npcs
    cluster.distr_bs = distr_bs
    cluster.distr_chunk_bs = distr_chunk_bs
    cluster.ha_type = ha_type
    if grafana_endpoint:
        cluster.grafana_endpoint = grafana_endpoint
    else:
        cluster.grafana_endpoint = f"http://{dev_ip}/grafana"
    cluster.enable_node_affinity = enable_node_affinity
    cluster.qpair_count = qpair_count or constants.QPAIR_COUNT

    cluster.max_queue_size = max_queue_size
    cluster.inflight_io_threshold = inflight_io_threshold
    cluster.enable_qos = enable_qos
    cluster.strict_node_anti_affinity = strict_node_anti_affinity
    cluster.contact_point = contact_point

    utils.render_and_deploy_alerting_configs(contact_point, cluster.grafana_endpoint, cluster.uuid, cluster.secret)

    logger.info("Deploying swarm stack ...")
    log_level = "DEBUG" if constants.LOG_WEB_DEBUG else "INFO"
    scripts.deploy_stack(cli_pass, dev_ip, constants.SIMPLY_BLOCK_DOCKER_IMAGE, cluster.secret, cluster.uuid,
                               log_del_interval, metrics_retention_period, log_level, cluster.grafana_endpoint)
    logger.info("Deploying swarm stack > Done")

    logger.info("Configuring DB...")
    scripts.set_db_config_single()
    logger.info("Configuring DB > Done")

    _set_max_result_window(dev_ip)

    _add_graylog_input(dev_ip, cluster.secret)

    _create_update_user(cluster.uuid, cluster.grafana_endpoint, cluster.grafana_secret, cluster.secret)

    cluster.status = Cluster.STATUS_UNREADY
    cluster.create_dt = str(datetime.datetime.now())
    db_controller = DBController()
    cluster.write_to_db(db_controller.kv_store)

    cluster_events.cluster_create(cluster)

    mgmt_node_ops.add_mgmt_node(dev_ip, cluster.uuid)

    logger.info("New Cluster has been created")
    logger.info(cluster.uuid)
    return cluster.uuid

def parse_nvme_list_output(output, target_model):
    lines = output.splitlines()
    for line in lines:
        if target_model in line:
            return line.split()[0]

    raise ValueError(f"Device with model {target_model} not found in nvme list")


def _cleanup_nvme(mount_point, nqn_value) -> None:
    logger.info(f"Starting cleanup for NVMe device with NQN: {nqn_value}")

    # Unmount the filesystem
    subprocess.check_call(["sudo", "umount", mount_point])
    logger.info(f"Unmounted {mount_point}")

    # Disconnect NVMe device
    subprocess.check_call(["sudo", "nvme", "disconnect", "-n", nqn_value])
    logger.info(f"Disconnected NVMe device: {nqn_value}")

    # Remove the mount point directory
    subprocess.check_call(["sudo", "rm", "-rf", mount_point])
    logger.info(f"Removed mount point: {mount_point}")


def _run_fio(mount_point) -> None:
    if not os.path.exists(mount_point):
        os.makedirs(mount_point, exist_ok=True)

    try:
        fio_config = textwrap.dedent(f"""
            [test]
            ioengine=aiolib
            direct=1
            iodepth=4
            readwrite=randrw
            bs=4K
            nrfiles=4
            size=1G
            verify=md5
            numjobs=3
            directory={mount_point}
        """).strip()
        config_file = "fio.cfg"
        with open(config_file, "w") as f:
            f.write(fio_config)

        logger.info(subprocess.check_output(["sudo", "fio", config_file], text=True))
    finally:
        if os.path.exists(config_file):
            os.remove(config_file)
            logger.info("fio configuration file removed.")


def add_cluster(blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
                distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity, qpair_count,
                max_queue_size, inflight_io_threshold, enable_qos, strict_node_anti_affinity) -> str:
    db_controller = DBController()
    clusters = db_controller.get_clusters()
    if not clusters:
        raise ValueError("No previous clusters found!")

    if distr_ndcs == 0 and distr_npcs == 0:
        raise ValueError("both distr_ndcs and distr_npcs cannot be 0")

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


def cluster_activate(cl_id, force=False, force_lvstore_create=False) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if cluster.status == Cluster.STATUS_ACTIVE:
        logger.warning("Cluster is ACTIVE")
        if not force:
            raise ValueError("Failed to activate cluster, Cluster is in an ACTIVE state, use --force to reactivate")

    ols_status = cluster.status
    if ols_status == Cluster.STATUS_IN_ACTIVATION:
        ols_status = Cluster.STATUS_UNREADY
    else:
        set_cluster_status(cl_id, Cluster.STATUS_IN_ACTIVATION)
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    online_nodes = []
    dev_count = 0

    for node in snodes:
        if node.is_secondary_node:  # pass
            continue
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    dev_count += 1
    minimum_devices = cluster.distr_ndcs + cluster.distr_npcs + 1
    if dev_count < minimum_devices:
        set_cluster_status(cl_id, ols_status)
        raise ValueError(f"Failed to activate cluster, No enough online device.. Minimum is {minimum_devices}")

    records = db_controller.get_cluster_capacity(cluster)
    max_size = records[0]['size_total']

    used_nodes_as_sec = []
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    if cluster.ha_type == "ha":
        for snode in snodes:
            if snode.is_secondary_node:  # pass
                continue
            if snode.secondary_node_id:
                sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                sec_node.lvstore_stack_secondary_1 = snode.get_id()
                sec_node.write_to_db()
                used_nodes_as_sec.append(snode.secondary_node_id)
                continue
            secondary_nodes = storage_node_ops.get_secondary_nodes(snode)
            if not secondary_nodes:
                set_cluster_status(cl_id, ols_status)
                raise ValueError("Failed to activate cluster, No enough secondary nodes")

            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.secondary_node_id = secondary_nodes[0]
            snode.write_to_db()
            sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
            sec_node.lvstore_stack_secondary_1 = snode.get_id()
            sec_node.write_to_db()
            used_nodes_as_sec.append(snode.secondary_node_id)

    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for snode in snodes:
        if snode.is_secondary_node:  # pass
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
                set_cluster_status(cl_id, ols_status)
                raise ValueError("Failed to activate cluster")

    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for snode in snodes:
        if snode.status != StorageNode.STATUS_ONLINE:
            continue

        if not snode.is_secondary_node:
            continue

        logger.info(f"recreating secondary node {snode.get_id()}")
        ret = storage_node_ops.recreate_lvstore_on_sec(snode)

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
                raise ValueError("Failed to activate cluster")


    if not cluster.cluster_max_size:
        cluster = db_controller.get_cluster_by_id(cl_id)
        if cluster is None:
            raise KeyError(f"Cluster not found {cl_id}")
        cluster.cluster_max_size = max_size
        cluster.cluster_max_devices = dev_count
        cluster.cluster_max_nodes = len(online_nodes)
        cluster.write_to_db(db_controller.kv_store)
    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    logger.info("Cluster activated successfully")


def cluster_expand(cl_id) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_IN_EXPANSION,
                              Cluster.STATUS_READONLY, Cluster.STATUS_DEGRADED]:
        raise ValueError(f"Cluster status is not expected: {cluster.status}")

    ols_status = cluster.status
    set_cluster_status(cl_id, Cluster.STATUS_IN_EXPANSION)

    records = db_controller.get_cluster_capacity(cluster)
    max_size = records[0]['size_total']

    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for snode in snodes:
        if snode.status != StorageNode.STATUS_ONLINE or snode.lvstore:  # pass
            continue

        if cluster.ha_type == "ha" and not snode.secondary_node_id:

            secondary_nodes = storage_node_ops.get_secondary_nodes(snode)
            if not secondary_nodes:
                set_cluster_status(cl_id, ols_status)
                raise ValueError("Failed to expand cluster, No enough secondary nodes")

            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.secondary_node_id = secondary_nodes[0]
            snode.write_to_db()

            sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
            sec_node.lvstore_stack_secondary_1 = snode.get_id()
            sec_node.write_to_db()

        ret = storage_node_ops.create_lvstore(snode, cluster.distr_ndcs, cluster.distr_npcs, cluster.distr_bs,
                                              cluster.distr_chunk_bs, cluster.page_size_in_blocks, max_size)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        if ret:
            snode.lvstore_status = "ready"
            snode.write_to_db()

        else:
            snode.lvstore_status = "failed"
            snode.write_to_db()
            set_cluster_status(cl_id, ols_status)
            raise ValueError("Failed to expand cluster")

    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    logger.info("Cluster expanded successfully")


def get_cluster_status(cl_id) -> t.List[dict]:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    return sorted([
        {
            "UUID": dev.get_id(),
            "Storage ID": dev.cluster_device_order,
            "Physical label": dev.physical_label,
            "Size": utils.humanbytes(dev.size),
            "Hostname": node.hostname,
            "Status": dev.status,
            "IO Error": dev.io_error,
            "Health": dev.health_check
        }
        for node in db_controller.get_storage_nodes_by_cluster_id(cl_id)
        for dev in node.nvme_devices
    ], key=lambda x: x["Storage ID"])


def set_cluster_status(cl_id, status) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if cluster.status == status:
        return

    old_status = cluster.status
    cluster.status = status
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_status_change(cluster, cluster.status, old_status)


def cluster_set_read_only(cl_id) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if cluster.status == Cluster.STATUS_READONLY:
        return

    set_cluster_status(cl_id, Cluster.STATUS_READONLY)
    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            continue
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                # dev_stat = db_controller.get_device_stats(dev, 1)
                # if dev_stat and dev_stat[0].size_util >= cluster.cap_crit:
                device_controller.device_set_state(dev.get_id(), NVMeDevice.STATUS_CANNOT_ALLOCATE)


def cluster_set_active(cl_id) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if cluster.status == Cluster.STATUS_ACTIVE:
        return

    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            continue

        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_CANNOT_ALLOCATE, NVMeDevice.STATUS_READONLY]:
                dev_stat = db_controller.get_device_stats(dev, 1)
                if dev_stat and dev_stat[0].size_util < cluster.cap_crit:
                    device_controller.device_set_online(dev.get_id())


def list() -> t.List[dict]:
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
    return data



def list_all_info(cluster_id) -> str:
    db_controller = DBController()
    cl = db_controller.get_cluster_by_id(cluster_id)
    if not cl:
        raise KeyError(f"Cluster not found {cluster_id}")

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
        rec = ClusterStatObject()

    task_total = 0
    task_running = 0
    task_pending = 0
    for task in db_controller.get_job_tasks(cl.get_id()):
        task_total += 1
        if task.status == JobSchedule.STATUS_RUNNING:
            task_running += 1
        elif task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
            task_pending += 1

    status = cl.status
    if cl.is_re_balancing and status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        status = f"{status} - ReBalancing"
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
        "Status": status.upper(),

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
        "Status": status.upper(),

    })

    out += "\n"
    out += utils.print_table(data, title="Cluster Stats")
    out += "\n"

    data = []

    dev_data = []

    for node in st:
        nodecapacityrecs = db_controller.get_node_capacity(node, 1)
        if nodecapacityrecs:
            nodecapacityrec = nodecapacityrecs[0]
        else:
            nodecapacityrec = NodeStatObject()

        lvs = db_controller.get_lvols_by_node_id(node.get_id()) or []
        total_devices = len(node.nvme_devices)
        online_devices = 0
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1

        data.append({
            "Storage node UUID": node.uuid,

            "Size": f"{utils.humanbytes(nodecapacityrec.size_total)}",
            "Used": f"{utils.humanbytes(nodecapacityrec.size_used)}",
            "Free": f"{utils.humanbytes(nodecapacityrec.size_free)}",
            "Util": f"{nodecapacityrec.size_util}%",

            "Read BW/s": f"{utils.humanbytes(nodecapacityrec.read_bytes_ps)}",
            "Write BW/s": f"{utils.humanbytes(nodecapacityrec.write_bytes_ps)}",
            "Read IOP/s": f"{nodecapacityrec.read_io_ps}",
            "Write IOP/s": f"{nodecapacityrec.write_io_ps}",

            "Size prov": f"{utils.humanbytes(nodecapacityrec.size_prov)}",
            "Util prov": f"{nodecapacityrec.size_prov_util}%",

            "Devices": f"{total_devices}/{online_devices}",
            "LVols": f"{len(lvs)}",
            "Status": node.status,

        })

        for dev in node.nvme_devices:
            devicecapacityrecs = db_controller.get_device_capacity(dev)
            if devicecapacityrecs:
                devicecapacityrec = devicecapacityrecs[0]
            else:
                devicecapacityrec = DeviceStatObject()

            dev_data.append({
                "Device UUID": dev.uuid,
                "Size": f"{utils.humanbytes(devicecapacityrec.size_total)}",
                "Used": f"{utils.humanbytes(devicecapacityrec.size_used)}",
                "Free": f"{utils.humanbytes(devicecapacityrec.size_free)}",
                "Util": f"{devicecapacityrec.size_util}%",
                "Read BW/s": f"{utils.humanbytes(devicecapacityrec.read_bytes_ps)}",
                "Write BW/s": f"{utils.humanbytes(devicecapacityrec.write_bytes_ps)}",
                "Read IOP/s": f"{devicecapacityrec.read_io_ps}",
                "Write IOP/s": f"{devicecapacityrec.write_io_ps}",
                "StorgeID": dev.cluster_device_order,
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
        lvolstatsrecs = db_controller.get_lvol_stats(lvol, 1)
        if lvolstatsrecs:
            lvolstatsrec = lvolstatsrecs[0]
        else:
            lvolstatsrec = LVolStatObject()

        lvol_data.append({
            "LVol UUID": lvol.uuid,
            "Size": f"{utils.humanbytes(lvolstatsrec.size_total)}",
            "Used": f"{utils.humanbytes(lvolstatsrec.size_used)}",
            "Free": f"{utils.humanbytes(lvolstatsrec.size_free)}",
            "Util": f"{lvolstatsrec.size_util}%",
            "Read BW/s": f"{utils.humanbytes(lvolstatsrec.read_bytes_ps)}",
            "Write BW/s": f"{utils.humanbytes(lvolstatsrec.write_bytes_ps)}",
            "Read IOP/s": f"{lvolstatsrec.read_io_ps}",
            "Write IOP/s": f"{lvolstatsrec.write_io_ps}",
            "Health": lvol.health_check,
            "Status": lvol.status,
        })

    out += "\n"
    if lvol_data:
        out += utils.print_table(lvol_data, title="LVol Stats")
        out += "\n"

    return out


def get_capacity(cluster_id, history, records_count=20) -> t.List[dict]:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            raise ValueError(f"Error parsing history string: {history}")
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
    return utils.process_records(records, records_count, keys=cap_stats_keys)


def get_iostats_history(cluster_id, history_string, records_count=20, with_sizes=False) -> t.List[dict]:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    if history_string:
        records_number = utils.parse_history_param(history_string)
        if not records_number:
            raise ValueError(f"Error parsing history string: {history_string}")
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
    return utils.process_records(records, records_count, keys=io_stats_keys)


def get_ssh_pass(cluster_id) -> str:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    return cluster.cli_pass


def get_secret(cluster_id) -> str:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    return cluster.secret


def set_secret(cluster_id, secret) -> None:
    db_controller = DBController()

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    secret = secret.strip()
    if len(secret) < 20:
        raise ValueError("Secret must be at least 20 char")

    _create_update_user(cluster_id, cluster.grafana_endpoint, cluster.grafana_secret, secret, update_secret=True)

    cluster.secret = secret
    cluster.write_to_db(db_controller.kv_store)


def get_logs(cluster_id, limit=50, **kwargs) -> t.List[dict]:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    events = db_controller.get_events(cluster_id, limit=limit, reverse=True)
    out = []
    events.reverse()
    for record in events:
        Storage_ID = None
        if record.storage_id >= 0:
            Storage_ID = record.storage_id

        elif 'cluster_device_order' in record.object_dict:
            Storage_ID = record.object_dict['cluster_device_order']

        vuid = None
        if record.vuid > 0:
            vuid = record.vuid

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
    return out


def get_cluster(cl_id) -> dict:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    return cluster.get_clean_dict()


def update_cluster(cluster_id, mgmt_only=False, restart=False, spdk_image=None, mgmt_image=None, **kwargs) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cluster_id}")

    sbcli=constants.SIMPLY_BLOCK_CLI_NAME
    subprocess.check_call(f"pip install {sbcli} --upgrade".split(' '))
    logger.info(f"{sbcli} upgraded")

    logger.info("Updating mgmt cluster")
    cluster_docker = utils.get_docker_client(cluster_id)
    logger.info(f"Pulling image {constants.SIMPLY_BLOCK_DOCKER_IMAGE}")
    pull_docker_image_with_retry(cluster_docker, constants.SIMPLY_BLOCK_DOCKER_IMAGE)
    image_without_tag = constants.SIMPLY_BLOCK_DOCKER_IMAGE.split(":")[0]
    image_without_tag = image_without_tag.split("/")
    image_parts = "/".join(image_without_tag[-2:])
    service_image =  constants.SIMPLY_BLOCK_DOCKER_IMAGE
    if mgmt_image:
        service_image = mgmt_image
    for service in cluster_docker.services.list():
        if image_parts in service.attrs['Spec']['Labels']['com.docker.stack.image'] or \
        "simplyblock" in service.attrs['Spec']['Labels']['com.docker.stack.image']:
            logger.info(f"Updating service {service.name}")
            service.update(image=service_image, force_update=True)
    logger.info("Done updating mgmt cluster")

    if mgmt_only:
        return

    logger.info("Updating spdk image on storage nodes")
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            node_docker = docker.DockerClient(base_url=f"tcp://{node.mgmt_ip}:2375", version="auto", timeout=60 * 5)
            img = spdk_image if spdk_image else constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
            logger.info(f"Pulling image {img}")
            pull_docker_image_with_retry(node_docker, img)

    if not restart:
        return

    logger.info("Restarting cluster")
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status == StorageNode.STATUS_ONLINE:
            logger.info(f"Suspending node: {node.get_id()}")
            storage_node_ops.suspend_storage_node(node.get_id())
            logger.info(f"Shutting down node: {node.get_id()}")
            storage_node_ops.shutdown_storage_node(node.get_id(), force=True)

    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status == StorageNode.STATUS_OFFLINE:
            if spdk_image:
                logger.info(f"Restarting node: {node.get_id()} with SPDK image: {spdk_image}")
            else:
                logger.info(f"Restarting node: {node.get_id()}")
            storage_node_ops.restart_storage_node(node.get_id(), force=True, spdk_image=spdk_image)

    logger.info("Done")


def cluster_grace_startup(cl_id, clear_data=False, spdk_image=None) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        logger.info(f"Restarting node: {node.get_id()}")
        storage_node_ops.restart_storage_node(node.get_id(), clear_data=clear_data, force=True, spdk_image=spdk_image)
        # time.sleep(5)
        get_node = db_controller.get_storage_node_by_id(node.get_id())
        if get_node.status != StorageNode.STATUS_ONLINE:
            raise ValueError("failed to restart node")


def cluster_grace_shutdown(cl_id) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        if node.status == StorageNode.STATUS_ONLINE:
            logger.info(f"Suspending node: {node.get_id()}")
            storage_node_ops.suspend_storage_node(node.get_id())
            logger.info(f"Shutting down node: {node.get_id()}")
            storage_node_ops.shutdown_storage_node(node.get_id())


def delete_cluster(cl_id) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    nodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    if nodes:
        raise ValueError("Can only remove Empty cluster, Storage nodes found")

    pools = db_controller.get_pools(cl_id)
    if pools:
        raise ValueError("Can only remove Empty cluster, Pools found")

    if len(db_controller.get_clusters()) == 1 :
        raise ValueError("Can not remove the last cluster!")

    logger.info(f"Deleting Cluster {cl_id}")
    cluster_events.cluster_delete(cluster)
    cluster.remove(db_controller.kv_store)
    logger.info("Done")

def set(cl_id, attr, value) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cl_id)
    if not cluster:
        raise KeyError(f"Cluster not found {cl_id}")

    if attr not in cluster.get_attrs_map():
        raise KeyError('Attribute not found')

    value = cluster.get_attrs_map()[attr]['type'](value)
    logger.info(f"Setting {attr} to {value}")
    setattr(cluster, attr, value)
    cluster.write_to_db()
