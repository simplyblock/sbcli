# coding=utf-8
import json
import os
import socket
import subprocess
import threading
import time
import uuid
import typing as t
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import docker
from kubernetes import client as k8s_client
import requests

from docker.errors import DockerException
from pydantic import SecretStr

from simplyblock_core import utils, scripts, constants, mgmt_node_ops, release_upgrades, storage_node_ops
from simplyblock_core.utils import port_block
from simplyblock_core.controllers import backup_controller, cluster_events, device_controller, qos_controller, tasks_controller, tcp_ports_events
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster, HashicorpVaultSettings, DeployConfig
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.mgmt_node import MgmtNode
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.stats import LVolStatObject, ClusterStatObject, NodeStatObject, DeviceStatObject
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.prom_client import PromClient
from simplyblock_core.release_upgrades import jc_compression_upgrade
from simplyblock_core.utils import pull_docker_image_with_retry
from simplyblock_core.settings import Settings

logger = utils.get_logger(__name__)

db_controller = DBController()

SUPPORTED_ERASURE_CODING_SCHEMES = {
    (1, 0),
    (1, 1),
    (2, 1),
    (4, 1),
    (1, 2),
    (2, 2),
    (4, 2),
}

def _create_update_user(cluster_id, grafana_url, grafana_secret: SecretStr, user_secret: SecretStr, update_secret=False):
    session = requests.session()
    session.auth = ("admin", grafana_secret.get_secret_value())
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
            "password": user_secret.get_secret_value()
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
            "password": user_secret.get_secret_value()
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


def _add_graylog_input(cluster_ip, password: SecretStr):
    base_url = f"{cluster_ip}/api"
    input_url = f"{base_url}/system/inputs"

    retries = 30
    reachable = False
    session = requests.session()
    session.auth = ("admin", password.get_secret_value())
    headers = {
        'X-Requested-By': 'setup-script',
        'Content-Type': 'application/json',
    }

    last_error = None
    while retries > 0:
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

        try:
            response = session.post(input_url, headers=headers, data=payload)
        except requests.exceptions.RequestException as e:
            # Graylog may still be starting (fresh cluster bootstrap) — a
            # refused/failed connection must retry the same as a bad status
            # code, not crash add_cluster() on the very first attempt.
            last_error = str(e)
            logger.debug("Graylog input POST failed, waiting for graylog to come up: %s", e)
            retries -= 1
            time.sleep(5)
            continue

        if response.status_code == 201:
            logger.info("Graylog input created...")
            reachable = True
            break

        last_error = f"status {response.status_code}"
        logger.debug("Graylog input POST returned status %s", response.status_code)
        retries -= 1
        time.sleep(5)

    if not reachable:
        logger.error("Failed to create graylog input (%s)", last_error)
        return False

    inputs_response = session.get(input_url, headers=headers)
    if inputs_response.status_code != 200:
        logger.error("Failed to retrieve inputs (status %s)", inputs_response.status_code)
        return False

    input_id = None
    for item in inputs_response.json()["inputs"]:
        if item["title"] == "spdk log input":
            input_id = item["id"]
            break

    if not input_id:
        logger.error("Could not find created input to add extractor.")
        return False

    extractor_url = f"{input_url}/{input_id}/extractors"
    extractor_payload = {
        "title": "Extract Kubernetes JSON",
        "extractor_type": "json",
        "converters": [],
        "order": 0,
        "cursor_strategy": "copy",
        "source_field": "message",
        "target_field": "",
        "extractor_config": {},
        "condition_type": "none",
        "condition_value": ""
    }

    extractor_response = session.post(extractor_url, headers=headers, data=json.dumps(extractor_payload))
    if extractor_response.status_code != 201:
        logger.error("Failed to add JSON extractor (status %s)", extractor_response.status_code)
        return False

    logger.info("JSON extractor added successfully.")
    return True

def _set_max_result_window(cluster_ip, max_window=100000):

    url_existing_indices = f"{cluster_ip}/_all/_settings"
    headers = {
        'Content-Type': 'application/json',
    }

    retries = 30
    reachable = False
    last_error = None
    while retries > 0:
        payload_existing = json.dumps({
            "settings": {
                "index.max_result_window": max_window
            }
        })
        try:
            response = requests.put(url_existing_indices, headers=headers, data=payload_existing)
        except requests.exceptions.RequestException as e:
            # OpenSearch may still be starting (fresh cluster bootstrap) — a
            # refused/failed connection must retry the same as a bad status
            # code, not crash add_cluster() on the very first attempt
            # (2026-07-28: cluster create failed outright with an unhandled
            # ConnectionError because opensearch wasn't listening yet).
            last_error = str(e)
            logger.debug("waiting for opensearch cluster to come up: %s", e)
            retries -= 1
            time.sleep(5)
            continue
        if response.status_code == 200:
            logger.info("Settings updated for existing indices.")
            reachable = True
            break
        last_error = response.text
        logger.debug(response.status_code)
        logger.debug("waiting for opensearch cluster to come up")
        retries -= 1
        time.sleep(5)

    if not reachable:
        logger.error(f"Failed to update settings for existing indices: {last_error}")
        return False

    url_template = f"{cluster_ip}/_template/all_indices_template"
    payload_template = json.dumps({
        "index_patterns": ["*"],
        "settings": {
            "index.max_result_window": max_window
        }
    })
    try:
        response_template = requests.put(url_template, headers=headers, data=payload_template)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create template for future indices: {e}")
        return False
    if response_template.status_code == 200:
        logger.info("Template created for future indices.")
        return True
    else:
        logger.error(f"Failed to create template for future indices: {response_template.text}")
        return False


def parse_protocols(input_str: str):
    valid = {"tcp", "rdma"}

    # split by comma, strip whitespace, and lowercase
    parts = {p.strip().lower() for p in input_str.split(",")}

    # validate input
    if not parts.issubset(valid):
        raise ValueError(f"Invalid protocol(s): {parts - valid}")

    return {
        "tcp": "tcp" in parts,
        "rdma": "rdma" in parts,
    }


def create_cluster(blk_size, page_size_in_blocks, cli_pass,
                   cap_warn, cap_crit, prov_cap_warn, prov_cap_crit, ifname, mgmt_ip, log_del_interval, metrics_retention_period,
                   contact_point, grafana_endpoint, distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, mode,
                   enable_node_affinity, qpair_count, client_qpair_count, max_queue_size, inflight_io_threshold, disable_monitoring, strict_node_anti_affinity, name,
                   tls_secret, ingress_host_source, dns_name, fabric, is_single_node, client_data_nic,
                   nvmeof_tls_config=None, max_fault_tolerance=1, backup_config=None,
                   nvmf_base_port=4420, rpc_base_port=8080, snode_api_port=50001, container_image_prefix=None,
                   hashicorp_vault_settings : t.Optional[HashicorpVaultSettings] = None,
                   enable_failure_domain=False,
                   enable_hang_device=False,
                   max_subsys=0, hugepages_mem=0, spdk_vcpu_count=0,
) -> str:
    if (distr_ndcs, distr_npcs) not in SUPPORTED_ERASURE_CODING_SCHEMES:
        raise ValueError("Unsupported erasure coding scheme")

    if max_fault_tolerance > 1:
        if ha_type != "ha":
            raise ValueError("max_fault_tolerance > 1 requires ha_type='ha'")
        if distr_npcs < 2:
            raise ValueError("max_fault_tolerance > 1 requires distr_npcs >= 2")

    if (hashicorp_vault_settings is not None) and (Settings().tls_connect != "authenticated"):
        raise ValueError("External KMS requires mTLS authentication to be used")

    if ingress_host_source == "dns" or ingress_host_source == "loadbalancer":
        if not dns_name:
            raise ValueError("--dns-name is required when --ingress-host-source is dns or loadbalancer")

    if name and db_controller.kv_store is not None:
        existing_clusters = db_controller.get_clusters()
        for existing in existing_clusters:
            if existing.cluster_name and existing.cluster_name == name:
                raise ValueError(f"A cluster with the name '{name}' already exists")

    monitoring_secret = SecretStr(os.environ.get("MONITORING_SECRET", ""))

    logger.info("Installing dependencies...")
    scripts.install_deps(mode)
    logger.info("Installing dependencies > Done")

    if not ifname:
        ifname = "eth0"

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        raise ValueError(f"Error getting interface ip: {ifname}")

    db_connection = SecretStr(f"{utils.generate_string(8)}:{utils.generate_string(32)}@{dev_ip}:4500")
    scripts.set_db_config(db_connection.get_secret_value())
    logger.info(f"Node IP: {dev_ip}")
    scripts.configure_docker(dev_ip)
    logger.info("Configuring docker swarm...")
    c = docker.DockerClient(base_url=f"tcp://{dev_ip}:2375", version="auto")
    if c.swarm.attrs and "ID" in c.swarm.attrs:
        logger.warning("Warning! Docker swarm found")
        ret = utils.query_yes_no("Destroy current cluster and create new one?", default="no")
        if not ret:
            raise ValueError("Aborting")
        c.swarm.leave(force=True)
        try:
            c.volumes.get("monitoring_grafana_data").remove(force=True)
        except DockerException as e:
            logger.debug(
                "Best-effort cleanup: could not remove volume 'monitoring_grafana_data'; continuing cluster reset. Error: %s",
                e,
            )
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
        cli_pass = SecretStr(utils.generate_string(10))

    logger.info("Adding new cluster object")
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.cluster_name = name
    # New clusters use per-chunk (shared) placement from the start: every
    # distrib and JM created at add-node / activation / restart picks up the
    # flag via cluster.shared_placement (see create_lvstore and
    # bdev_jm_create). No legacy-then-migrate phase. The deferred migration
    # path (shared_placement_migration_pending) is only for clusters UPGRADED
    # from a legacy release, whose pre-existing bdevs need the one-shot
    # runtime flip via set_shared_placement.
    cluster.shared_placement = True
    # New clusters create every distrib with v2 write protection from the
    # start, so there is nothing to migrate and the runtime
    # distr_write_protection_v2 RPC is never needed here. Only a cluster
    # UPGRADED from a release without v2 goes through
    # `cluster switch-write-protection`.
    cluster.write_protection_v2 = True
    cluster.blk_size = blk_size
    cluster.page_size_in_blocks = page_size_in_blocks
    cluster.nqn = f"{constants.CLUSTER_NQN}:{cluster.uuid}"
    cluster.cli_pass = cli_pass
    cluster.secret = SecretStr(utils.generate_string(20))
    cluster.grafana_secret = monitoring_secret if mode == "kubernetes" else cluster.secret
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
    protocols = parse_protocols(fabric)
    cluster.fabric_tcp = protocols["tcp"]
    cluster.fabric_rdma = protocols["rdma"]
    cluster.is_single_node = is_single_node
    if ingress_host_source == "hostip":
        base = dev_ip
    else:
        base = dns_name

    graylog_endpoint = f"http://{base}/graylog"
    os_endpoint      = f"http://{base}/opensearch"
    default_grafana  = f"http://{base}/grafana"

    cluster.grafana_endpoint = grafana_endpoint or default_grafana
    cluster.enable_node_affinity = enable_node_affinity
    cluster.enable_hang_device = enable_hang_device
    cluster.qpair_count = qpair_count or constants.QPAIR_COUNT
    cluster.client_qpair_count = client_qpair_count or constants.CLIENT_QPAIR_COUNT

    cluster.max_queue_size = max_queue_size
    cluster.inflight_io_threshold = inflight_io_threshold
    cluster.strict_node_anti_affinity = strict_node_anti_affinity
    cluster.enable_failure_domain = enable_failure_domain
    validate_spdk_sizing(max_subsys, hugepages_mem, spdk_vcpu_count)
    cluster.max_subsys = max_subsys or 0
    cluster.hugepages_mem = hugepages_mem or 0
    cluster.spdk_vcpu_count = spdk_vcpu_count or 0
    cluster.contact_point = contact_point
    cluster.disable_monitoring = disable_monitoring
    cluster.mode = mode
    cluster.full_page_unmap = False
    cluster.client_data_nic = client_data_nic or ""
    cluster.max_fault_tolerance = max_fault_tolerance
    cluster.nvmf_base_port = nvmf_base_port
    cluster.rpc_base_port = rpc_base_port
    cluster.snode_api_port = snode_api_port
    cluster.container_image_prefix = container_image_prefix or ""
    cluster.hashicorp_vault_settings = hashicorp_vault_settings
    cluster.backup_local_path = os.path.join(constants.KVD_DB_BACKUP_PATH, cluster.uuid)

    if nvmeof_tls_config:
        cluster.tls = True
        cluster.tls_config = nvmeof_tls_config

    if backup_config:
        cluster.backup_config = backup_config

    if not disable_monitoring:
        utils.render_and_deploy_alerting_configs(contact_point, cluster.grafana_endpoint, cluster.uuid, cluster.secret.get_secret_value())

    logger.info("Deploying swarm stack ...")
    log_level = "DEBUG" if constants.LOG_WEB_DEBUG else "INFO"
    scripts.deploy_stack(cli_pass.get_secret_value(), dev_ip, constants.SIMPLY_BLOCK_DOCKER_IMAGE, cluster.secret.get_secret_value(), cluster.uuid,
                            log_del_interval, metrics_retention_period, log_level, cluster.grafana_endpoint, str(disable_monitoring))
    logger.info("Deploying swarm stack > Done")

    logger.info("Configuring DB...")
    scripts.set_db_config_single()
    logger.info("Configuring DB > Done")
    monitoring_secret = cluster.secret


    cfg = DeployConfig()
    cfg.mode = mode
    cfg.grafana_endpoint = grafana_endpoint or default_grafana
    cfg.grafana_secret = monitoring_secret if mode == "kubernetes" else cluster.secret
    cfg.db_connection = db_connection if db_connection else SecretStr("")
    cfg.disable_monitoring = disable_monitoring
    cfg.write_to_db()

    # Monitoring stack configuration (OpenSearch max_result_window, Graylog
    # GELF input + JSON extractor, Grafana admin user). Must run after the
    # mode-specific deploy block has produced a reachable graylog endpoint.
    # Pre-KMS (commit 7700b866) this lived in a single shared block after
    # the if/elif; the KMS refactor accidentally moved it into the
    # kubernetes branch only, which silently left every docker-swarm
    # deployment without a Graylog input — services were emitting GELF on
    # port 12201 but graylog was dropping them on the floor because no
    # input was configured. Restore the shared placement so both modes
    # provision monitoring.
    if not disable_monitoring:
        _set_max_result_window(os_endpoint)
        _add_graylog_input(graylog_endpoint, monitoring_secret)
        _create_update_user(cluster.uuid, cluster.grafana_endpoint, monitoring_secret, cluster.secret)

    cluster.db_connection = db_connection  # type: ignore[assignment]
    cluster.status = Cluster.STATUS_UNREADY
    cluster.create_dt = str(datetime.now())

    cluster.write_to_db(db_controller.kv_store)

    cluster_events.cluster_create(cluster)

    mgmt_node_ops.add_mgmt_node(dev_ip, mode, cluster.uuid)

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


def add_cluster(blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
                distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity, qpair_count,
                max_queue_size, inflight_io_threshold, strict_node_anti_affinity, is_single_node, name, cr_name=None,
                cr_namespace=None, cr_plural=None, fabric="tcp",
                max_subsys=0, hugepages_mem=0, spdk_vcpu_count=0,
                client_data_nic="", max_fault_tolerance=1, backup_config=None,
                nvmf_base_port=4420, rpc_base_port=8080, snode_api_port=50001,
                hashicorp_vault_settings : t.Optional[HashicorpVaultSettings] = None,
                enable_failure_domain=False,
) -> str:
    """Thin wrapper around _add_cluster_impl() that serializes create calls
    for the same name behind a ClusterCreateLock.

    The duplicate-name check inside _add_cluster_impl is a plain
    read-then-write with no atomicity: concurrent/retried create calls for the
    same name can all pass it before any of them has committed (observed
    2026-07-28: a control-plane readiness flap made the operator retry
    cluster-create in a burst, producing 6 separate clusters named
    "simplyblock-cluster" instead of one). Acquiring this lock first — and
    only for named creates, matching the check it protects — closes that
    window without touching _add_cluster_impl's body. Kept as an explicit,
    identically-shaped signature (not *args/**kwargs) since existing callers
    pass ``name`` positionally.
    """
    kwargs = dict(
        blk_size=blk_size, page_size_in_blocks=page_size_in_blocks, cap_warn=cap_warn, cap_crit=cap_crit,
        prov_cap_warn=prov_cap_warn, prov_cap_crit=prov_cap_crit, distr_ndcs=distr_ndcs, distr_npcs=distr_npcs,
        distr_bs=distr_bs, distr_chunk_bs=distr_chunk_bs, ha_type=ha_type, enable_node_affinity=enable_node_affinity,
        qpair_count=qpair_count, max_queue_size=max_queue_size, inflight_io_threshold=inflight_io_threshold,
        strict_node_anti_affinity=strict_node_anti_affinity, is_single_node=is_single_node, name=name,
        max_subsys=max_subsys, hugepages_mem=hugepages_mem, spdk_vcpu_count=spdk_vcpu_count,
        cr_name=cr_name, cr_namespace=cr_namespace, cr_plural=cr_plural, fabric=fabric,
        client_data_nic=client_data_nic, max_fault_tolerance=max_fault_tolerance, backup_config=backup_config,
        nvmf_base_port=nvmf_base_port, rpc_base_port=rpc_base_port, snode_api_port=snode_api_port,
        hashicorp_vault_settings=hashicorp_vault_settings, enable_failure_domain=enable_failure_domain,
    )
    if not name:
        return _add_cluster_impl(**kwargs)

    owner = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4()}"
    acquired, holder = db_controller.acquire_cluster_create_lock(name, owner)
    if not acquired:
        raise ValueError(f"A cluster with the name '{name}' already exists or is currently being created "
                          f"(held by {holder})")
    try:
        return _add_cluster_impl(**kwargs)
    finally:
        db_controller.release_cluster_create_lock(name, owner)


def _add_cluster_impl(blk_size, page_size_in_blocks, cap_warn, cap_crit, prov_cap_warn, prov_cap_crit,
                distr_ndcs, distr_npcs, distr_bs, distr_chunk_bs, ha_type, enable_node_affinity, qpair_count,
                max_queue_size, inflight_io_threshold, strict_node_anti_affinity, is_single_node, name, cr_name=None,
                cr_namespace=None, cr_plural=None, fabric="tcp",
                max_subsys=0, hugepages_mem=0, spdk_vcpu_count=0,
                client_data_nic="", max_fault_tolerance=1, backup_config=None,
                nvmf_base_port=4420, rpc_base_port=8080, snode_api_port=50001,
                hashicorp_vault_settings : t.Optional[HashicorpVaultSettings] = None,
                enable_failure_domain=False,
) -> str:

    clusters = db_controller.get_clusters()
    if name and clusters:
        for existing in clusters:
            if existing.cluster_name and existing.cluster_name == name:
                raise ValueError(f"A cluster with the name '{name}' already exists")

    if (distr_ndcs, distr_npcs) not in SUPPORTED_ERASURE_CODING_SCHEMES:
        raise ValueError("Unsupported erasure coding scheme")

    if max_fault_tolerance > 1:
        if ha_type != "ha":
            raise ValueError("max_fault_tolerance > 1 requires ha_type='ha'")
        if distr_npcs < 2:
            raise ValueError("max_fault_tolerance > 1 requires distr_npcs >= 2")

    if (hashicorp_vault_settings is not None) and (Settings().tls_connect != "authenticated"):
        raise ValueError("External KMS requires mTLS authentication to be used")

    logger.info("Adding new cluster")
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.cluster_name = name
    # New clusters use per-chunk (shared) placement from the start: every
    # distrib and JM created at add-node / activation / restart picks up the
    # flag via cluster.shared_placement (see create_lvstore and
    # bdev_jm_create). No legacy-then-migrate phase. The deferred migration
    # path (shared_placement_migration_pending) is only for clusters UPGRADED
    # from a legacy release, whose pre-existing bdevs need the one-shot
    # runtime flip via set_shared_placement.
    cluster.shared_placement = True
    # New clusters create every distrib with v2 write protection from the
    # start, so there is nothing to migrate and the runtime
    # distr_write_protection_v2 RPC is never needed here. Only a cluster
    # UPGRADED from a release without v2 goes through
    # `cluster switch-write-protection`.
    cluster.write_protection_v2 = True
    cluster.blk_size = blk_size
    cluster.page_size_in_blocks = page_size_in_blocks
    cluster.nqn = f"{constants.CLUSTER_NQN}:{cluster.uuid}"
    cluster.secret = SecretStr(utils.generate_string(20))
    cluster.strict_node_anti_affinity = strict_node_anti_affinity
    cluster.enable_failure_domain = enable_failure_domain

    if clusters:
        cfg = db_controller.get_deploy_config()
        cluster.mode = cfg.mode
        cluster.db_connection = cfg.db_connection
        cluster.grafana_secret = cfg.grafana_secret
        cluster.grafana_endpoint = cfg.grafana_endpoint
        cluster.disable_monitoring = cfg.disable_monitoring
    else:
        # Bootstrapping the very first cluster of a fresh deployment: no
        # DeployConfig exists yet (only the docker-swarm create_cluster()
        # path writes one), so derive the cluster-wide settings here and
        # persist them as the DeployConfig every later add_cluster() call
        # will read.
        logger.info("No previous clusters found, bootstrapping first cluster")
        enable_monitoring = os.environ.get("ENABLE_MONITORING", "")
        monitoring_secret = SecretStr(os.environ.get("MONITORING_SECRET", ""))

        cluster.mode = "kubernetes"
        cluster.disable_monitoring = enable_monitoring != "true"
        logger.info("Retrieving foundationdb connection string...")
        cluster.db_connection = utils.get_fdb_cluster_string(constants.FDB_CONFIG_NAME, constants.K8S_NAMESPACE)
        if monitoring_secret:
            cluster.grafana_secret = monitoring_secret
        elif enable_monitoring != "true":
            cluster.grafana_secret = SecretStr("")
        else:
            raise ValueError("monitoring_secret is required")
        cluster.grafana_endpoint = constants.GRAFANA_K8S_ENDPOINT

        mgmt_node_ops.add_mgmt_node("0.0.0.0", "kubernetes", cluster.uuid)

        if not cluster.disable_monitoring:
            _set_max_result_window(constants.OS_K8S_ENDPOINT)
            _add_graylog_input(constants.GRAYLOG_K8S_ENDPOINT, cluster.grafana_secret)

        cfg = DeployConfig()
        cfg.mode = cluster.mode
        cfg.grafana_endpoint = cluster.grafana_endpoint
        cfg.grafana_secret = cluster.grafana_secret
        cfg.db_connection = cluster.db_connection
        cfg.disable_monitoring = cluster.disable_monitoring
        cfg.write_to_db()

    if not cluster.disable_monitoring:
        _create_update_user(cluster.uuid, cluster.grafana_endpoint, cluster.grafana_secret, cluster.secret)

    if cluster.mode == "kubernetes":
        utils.patch_prometheus_configmap(cluster.uuid, cluster.secret.get_secret_value())

    cluster.distr_ndcs = distr_ndcs
    cluster.distr_npcs = distr_npcs
    cluster.distr_bs = distr_bs
    cluster.distr_chunk_bs = distr_chunk_bs
    cluster.ha_type = ha_type
    cluster.is_single_node = is_single_node
    cluster.enable_node_affinity = enable_node_affinity
    cluster.qpair_count = qpair_count or constants.QPAIR_COUNT
    cluster.max_queue_size = max_queue_size
    cluster.inflight_io_threshold = inflight_io_threshold
    validate_spdk_sizing(max_subsys, hugepages_mem, spdk_vcpu_count)
    cluster.max_subsys = max_subsys or 0
    cluster.hugepages_mem = hugepages_mem or 0
    cluster.spdk_vcpu_count = spdk_vcpu_count or 0
    cluster.cr_name = cr_name
    cluster.cr_namespace = cr_namespace
    cluster.cr_plural = cr_plural
    if cap_warn and cap_warn > 0:
        cluster.cap_warn = cap_warn
    if cap_crit and cap_crit > 0:
        cluster.cap_crit = cap_crit
    if prov_cap_warn and prov_cap_warn > 0:
        cluster.prov_cap_warn = prov_cap_warn
    if prov_cap_crit and prov_cap_crit > 0:
        cluster.prov_cap_crit = prov_cap_crit
    protocols = parse_protocols(fabric)
    cluster.fabric_tcp = protocols["tcp"]
    cluster.fabric_rdma = protocols["rdma"]
    cluster.full_page_unmap = False
    cluster.client_data_nic = client_data_nic or ""
    cluster.max_fault_tolerance = max_fault_tolerance
    cluster.nvmf_base_port = nvmf_base_port
    cluster.rpc_base_port = rpc_base_port
    cluster.snode_api_port = snode_api_port
    cluster.hashicorp_vault_settings = hashicorp_vault_settings
    if backup_config:
        cluster.backup_config = backup_config

    cluster.backup_local_path = os.path.join(constants.KVD_DB_BACKUP_PATH, cluster.uuid)
    cluster.status = Cluster.STATUS_UNREADY
    cluster.create_dt = str(datetime.now())
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_create(cluster)

    return cluster.get_id()


def set_name(cl_id, name) -> Cluster:
    cluster = db_controller.get_cluster_by_id(cl_id)
    if name:
        for existing in db_controller.get_clusters():
            if existing.uuid != cl_id and existing.cluster_name and existing.cluster_name == name:
                raise ValueError(f"A cluster with the name '{name}' already exists")
    old_name = cluster.cluster_name
    cluster.cluster_name = name
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_name_change(cluster, name, old_name)
    return cluster


def _wait_for_full_device_connectivity(cl_id, timeout_sec=300, poll_sec=10):
    """Block until every ONLINE primary node holds a connected remote-device
    record for every ONLINE device of every OTHER online node, or raise.

    Activation's first create_lvstore builds distribs that immediately
    read/write across ALL cluster devices. A single missing cross-node
    connection fails that create ~12 s in with an opaque distrib
    ``error_read`` and aborts the whole activation (incident 2026-07-10
    14:12: the deploy retried two node-adds via delete+re-add, producing
    four fresh device records in the final 3 minutes, and activation
    started 70 s after the last join — before peers had attached to the
    re-added nodes' devices). Node records converge as the add/health
    flows finish attaching, so waiting here is both sufficient and
    bounded; on timeout the error names the exact missing links instead
    of a distrib read error.
    """
    deadline = time.time() + timeout_sec
    prev_missing = None
    # A stalled repair is bounded by round count as well as by the wall-clock
    # deadline. The two express the same budget when the ``time.sleep`` below is
    # real, but only the round count holds when it is not: the integration
    # fixtures patch this module's ``time.sleep`` to a no-op, which turns the
    # wait into thousands of full-mesh repair passes burning CPU for the whole
    # timeout_sec. Rounds that make progress reset the counter, so a genuinely
    # long repair keeps the same unbounded-while-shrinking behavior as before.
    max_stalled_rounds = max(1, int(timeout_sec / poll_sec))
    stalled_rounds = 0
    while True:
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
        online = [n for n in snodes
                  if not n.is_secondary_node and n.status == StorageNode.STATUS_ONLINE]
        expected = {}  # device uuid -> owner node id
        for node in online:
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    expected[dev.get_id()] = node.get_id()
        missing = []
        for node in online:
            have = {rd.get_id() for rd in node.remote_devices if rd.remote_bdev}
            for dev_id, owner in expected.items():
                if owner != node.get_id() and dev_id not in have:
                    missing.append((node.get_id(), owner, dev_id))
        if not missing:
            logger.info("Pre-activation connectivity check passed: %d nodes fully meshed "
                        "over %d devices", len(online), len(expected))
            return
        if time.time() >= deadline or stalled_rounds >= max_stalled_rounds:
            sample = ", ".join(f"{n[:8]}->{o[:8]}/dev {d[:8]}" for n, o, d in missing[:8])
            raise ValueError(
                f"Failed to activate cluster: {len(missing)} cross-node device "
                f"connection(s) still missing after {timeout_sec}s "
                f"(node->device-owner/device): {sample}. Nodes are still "
                f"attaching to recently (re-)added peers — retry activation "
                f"once node health checks pass.")
        logger.warning("Pre-activation connectivity check: %d cross-node device "
                       "connection(s) missing; repairing, then waiting %ds "
                       "(%.0fs left)", len(missing), poll_sec,
                       deadline - time.time())

        # Actively REPAIR the missing links instead of only waiting for them.
        # Waiting is sufficient after node-add (the add/health flows are still
        # attaching), but after a whole-cluster parallel recovery nothing else
        # drives these: each restart's peer reconnect is best-effort and skips
        # peers that are mid-restart at that moment, and once the last restart
        # finishes no reconciliation sweeps the leftovers — a bare wait
        # livelocks activation (2026-07-13: 382 links static across repeated
        # in_activation -> suspended -> in_activation cycles). Repair mirrors
        # _reconnect_peers_to_restarted_node: per-node worker threads, DELTA
        # reconnect per missing owner, atomic_update so we never clobber the
        # node's concurrent flows. Best-effort — the re-check above is the
        # only pass/fail authority.
        # NB: a plain ``set()`` here would resolve to this module's ``set``
        # function (it shadows the builtin).
        by_node: dict = {}
        for n_id, owner_id, _ in missing:
            if n_id not in by_node:
                by_node[n_id] = {owner_id}
            else:
                by_node[n_id].add(owner_id)

        def _repair_node(node_id, owner_ids):
            # A full-mesh outage (whole-fleet reboot) leaves each node missing
            # MOST owners. The per-owner delta below pays its fixed overhead
            # (DB reads, connect round-trips, JM reconcile, atomic_update)
            # once per owner — measured ~40s each, and 31 sequential owners
            # made round 1 the 21-minute activation stall of 2026-07-16
            # (13:09:43 "1353 missing" -> 13:30:49 "15 missing", one round).
            # One FULL reconcile connects every peer's devices in parallel
            # behind a single shared surface-poll (~67s/node, 2026-07-13
            # measurement), so use it whenever more than a couple of owners
            # are missing; keep the delta for the small post-node-add case.
            if len(owner_ids) > 2:
                try:
                    node = db_controller.get_storage_node_by_id(node_id)
                    remote_devices = storage_node_ops._connect_to_remote_devs(
                        node, force_connect_restarting_nodes=True)
                    remote_jm_devices = None
                    if node.enable_ha_jm:
                        remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(node)

                    def _apply(n, rd=remote_devices, rjd=remote_jm_devices):
                        n.remote_devices = rd
                        if rjd is not None:
                            n.remote_jm_devices = rjd
                    db_controller.atomic_update(node, _apply)
                except Exception as e:
                    logger.warning(
                        "Pre-activation full reconcile of %s failed: %s",
                        node_id[:8], e)
                return
            for owner_id in sorted(owner_ids):
                try:
                    node = db_controller.get_storage_node_by_id(node_id)
                    remote_devices = storage_node_ops._connect_to_remote_devs(
                        node, force_connect_restarting_nodes=True,
                        only_node_id=owner_id)
                    remote_jm_devices = None
                    if node.enable_ha_jm:
                        remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(
                            node, only_node_id=owner_id)

                    def _apply(n, rd=remote_devices, rjd=remote_jm_devices):
                        n.remote_devices = rd
                        if rjd is not None:
                            n.remote_jm_devices = rjd
                    db_controller.atomic_update(node, _apply)
                except Exception as e:
                    logger.warning(
                        "Pre-activation repair of %s -> %s failed: %s",
                        node_id[:8], owner_id[:8], e)

        repair_threads = []
        for node_id, owner_ids in by_node.items():
            t = threading.Thread(
                target=_repair_node, args=(node_id, owner_ids),
                name=f"preact-repair-{node_id[:8]}")
            t.start()
            repair_threads.append(t)
        for t in repair_threads:
            t.join()

        # Progress-aware deadline. The FIRST completed repair round counts as
        # progress unconditionally: the round itself may consume the whole
        # initial budget (2026-07-13 validation run: 1116 links repaired at
        # ~38/min = 25+ min in round 1), and without this the already-expired
        # deadline forced a pointless abort lap on the re-check even though
        # the mesh was nearly healed. After that, extend only while the
        # missing count keeps shrinking — a stalled repair (no reduction
        # across a full round) still runs the clock out.
        if prev_missing is None or len(missing) < prev_missing:
            deadline = max(deadline, time.time() + timeout_sec / 2)
            stalled_rounds = 0
        else:
            stalled_rounds += 1
        prev_missing = len(missing)
        time.sleep(poll_sec)


def set_object_ops(cl_id, stopped) -> bool:
    """Stop or resume object lifecycle operations on one cluster.

    While stopped, creation, deletion and modification of lvols, snapshots,
    clones and pools are refused -- including parameter changes such as QoS
    limits and resizes. Read paths stay open, and the cluster keeps maintaining
    itself (restarts, migrations, rebalancing) so a stopped cluster still
    recovers from faults on its own.

    Enforcement lives in the controllers (see controllers/ops_gate.py), which
    is what both the CLI and the v2 API funnel through.
    """
    cluster = db_controller.get_cluster_by_id(cl_id)
    stopped = bool(stopped)
    if bool(getattr(cluster, "object_ops_stopped", False)) == stopped:
        logger.info(
            f"Object operations are already "
            f"{'stopped' if stopped else 'started'} on cluster {cl_id}")
        return True

    db_controller.atomic_update(
        cluster, lambda c, v=stopped: setattr(c, "object_ops_stopped", v))
    logger.info(
        f"Object operations {'stopped' if stopped else 'started'} on cluster {cl_id}")
    try:
        cluster_events.cluster_object_ops_change(
            db_controller.get_cluster_by_id(cl_id), stopped)
    except Exception as ev_err:
        # The switch is already persisted; failing to journal it must not
        # report the operation as failed.
        logger.warning(f"Could not log the object-ops change event: {ev_err}")
    return True


def validate_spdk_sizing(max_subsys=None, hugepages_mem=None, spdk_vcpu_count=None):
    """Check the cluster-wide SPDK sizing values, raising ValueError if unusable.

    These are cluster-level on purpose: set per node they let a cluster drift
    into nodes with different subsystem ceilings and different core budgets.
    """
    if max_subsys is not None and max_subsys:
        if max_subsys < 0 or max_subsys > constants.MAX_SUBSYSTEMS_PER_NODE:
            raise ValueError(
                f"max_subsys must be between 1 and "
                f"{constants.MAX_SUBSYSTEMS_PER_NODE} (0 = product default)")
    if hugepages_mem is not None and hugepages_mem < 0:
        raise ValueError("hugepages_mem cannot be negative (0 = computed)")
    if spdk_vcpu_count is not None and spdk_vcpu_count < 0:
        raise ValueError("spdk_vcpu_count cannot be negative (0 = heuristic)")


def set_spdk_sizing(cluster_id, max_subsys=None, hugepages_mem=None) -> bool:
    """Change the cluster-wide SPDK sizing that may be changed after creation.

    A node adopts these when it is added and on every restart, so the change
    lands node by node as they restart rather than immediately. spdk_vcpu_count
    is not settable here: changing a running cluster's core budget rewrites
    every node's core mask, which belongs to a deliberate re-deploy.
    """
    validate_spdk_sizing(max_subsys=max_subsys, hugepages_mem=hugepages_mem)
    cluster = db_controller.get_cluster_by_id(cluster_id)
    changes = {}
    if max_subsys is not None:
        changes["max_subsys"] = int(max_subsys)
    if hugepages_mem is not None:
        changes["hugepages_mem"] = int(hugepages_mem)
    if not changes:
        return True

    def _apply(c, values=changes):
        for key, value in values.items():
            setattr(c, key, value)

    db_controller.atomic_update(cluster, _apply)
    logger.info(
        f"Cluster {cluster_id} SPDK sizing updated: "
        + ", ".join(f"{k}={v}" for k, v in changes.items())
        + ". Nodes pick this up on their next restart.")
    return True


def _record_activated_nodes(cl_id) -> None:
    """Freeze the node set that is now part of the activated cluster.

    Written on the success path of both activation and expansion, so a later
    re-activation can tell "the same cluster" from "the same cluster plus nodes
    someone added while it was suspended".
    """
    try:
        cluster = db_controller.get_cluster_by_id(cl_id)
        node_ids = sorted(
            n.get_id() for n in db_controller.get_storage_nodes_by_cluster_id(cl_id))
        db_controller.atomic_update(
            cluster, lambda c, v=node_ids: setattr(c, "activated_node_ids", v))
    except Exception as e:
        # Never fail an otherwise-successful activation over bookkeeping; the
        # next activation or expansion rewrites it.
        logger.warning(f"Could not record the activated node set: {e}")


def cluster_activate(cl_id, force=False, force_lvstore_create=False) -> None:
    """Wrapper around the activation body that keeps ``activation_heartbeat``
    fresh for its whole duration. The storage_node_monitor watchdog uses a
    stale heartbeat to tell a DEAD activation (driver process/container gone)
    from a merely long one: without it, a wedged IN_ACTIVATION sat for the
    full node-scaled budget — 42 minutes on a 32-node cluster — before the
    revert (incident 2026-07-13, monitor container replaced mid-activation).
    """
    stop_beat = threading.Event()

    def _beat():
        while not stop_beat.wait(60):
            try:
                fresh = db_controller.get_cluster_by_id(cl_id)
                if fresh.status != Cluster.STATUS_IN_ACTIVATION:
                    continue
                now_iso = datetime.now(timezone.utc).isoformat()
                db_controller.atomic_update(
                    fresh, lambda c, v=now_iso: setattr(c, "activation_heartbeat", v))
            except Exception:
                # Never let heartbeat trouble touch the activation itself; a
                # missed beat only means the watchdog waits for the next one.
                pass

    beat_thread = threading.Thread(
        target=_beat, daemon=True, name=f"activation-heartbeat-{cl_id[:8]}")
    beat_thread.start()
    try:
        _cluster_activate_impl(
            cl_id, force=force, force_lvstore_create=force_lvstore_create)
    finally:
        stop_beat.set()


def _cluster_activate_impl(cl_id, force=False, force_lvstore_create=False) -> None:
    cluster = db_controller.get_cluster_by_id(cl_id)
    prev_status = cluster.status
    if prev_status == Cluster.STATUS_IN_ACTIVATION:
        prev_status = Cluster.STATUS_UNREADY
    try:
        _cluster_activate(cl_id, force=force, force_lvstore_create=force_lvstore_create)
    except Exception:
        # Never leave the cluster wedged in in_activation: this often runs in
        # a fire-and-forget thread, and an unhandled failure would otherwise
        # block any retry (the activate API rejects in_activation clusters).
        # The expected-failure paths inside _cluster_activate restore the
        # status themselves; this only catches what they missed.
        cluster = db_controller.get_cluster_by_id(cl_id)
        if cluster.status == Cluster.STATUS_IN_ACTIVATION:
            logger.error("Cluster activation failed unexpectedly; reverting status "
                         f"from {Cluster.STATUS_IN_ACTIVATION} to {prev_status}")
            set_cluster_status(cl_id, prev_status)
        raise


def _cluster_activate(cl_id, force=False, force_lvstore_create=False) -> None:
    cluster = db_controller.get_cluster_by_id(cl_id)

    if cluster.status == Cluster.STATUS_ACTIVE:
        logger.warning("Cluster is ACTIVE")
        if not force:
            raise ValueError("Failed to activate cluster, Cluster is in an ACTIVE state, use --force to reactivate")

    # Growth by re-activation is not a supported path. Once a cluster has been
    # activated its node set is fixed; nodes added afterwards are integrated by
    # the expansion flow ("sn add-node --expansion" on an ACTIVE cluster), which
    # rotates roles and rebalances data. Re-activating with extra nodes present
    # would pull them in with none of that.
    #
    # The ACTIVE check above does not catch it: suspend -> add-node -> activate
    # walks straight past, because a suspended cluster is not ACTIVE. There is
    # deliberately no --force escape here -- forcing it produces a cluster whose
    # roles and failure domains were never rotated for the new nodes, which is
    # not a state an operator can ask for meaningfully.
    current_node_ids = {
        n.get_id() for n in db_controller.get_storage_nodes_by_cluster_id(cl_id)}
    if cluster.activated_node_ids:
        added = sorted(current_node_ids - set(cluster.activated_node_ids))
        if added:
            raise ValueError(
                f"Cluster {cl_id} has already been activated and {len(added)} "
                f"node(s) were added since: {', '.join(n[:8] for n in added)}. "
                f"Re-activation must not grow a cluster. Remove those nodes, or "
                f"grow the cluster with 'sn add-node --expansion' while it is "
                f"ACTIVE.")

    ols_status = cluster.status
    if ols_status == Cluster.STATUS_IN_ACTIVATION:
        ols_status = Cluster.STATUS_UNREADY
    else:
        set_cluster_status(cl_id, Cluster.STATUS_IN_ACTIVATION)

    # First-time activation runs while no primary LVS is serving fabric I/O
    # yet, so the recreate paths run with activation_mode=True (peer LVS /
    # leader / hublvol RPCs short-circuited — peer stacks aren't fully built
    # during this phase, so they would not be safe to call). Re-activation
    # (e.g. suspended → in_activation after JCERR, or force-reactivating an
    # active/degraded cluster) is different: every primary's SPDK and lvstore
    # are still alive and serving I/O — the secondary's examine of its non-
    # leader raid0 races the live leader's blob-metadata writes and fails
    # with bs_load_cur_extent_page_valid CRC mismatch on every retry
    # (observed 2026-05-11, LVS_6769 on node 8084 — 22+ minute examine loop).
    # We keep activation_mode=True (so peer LVS/hublvol RPCs stay disabled)
    # and add only a firewall-only port-block on the live leader around the
    # non-leader recreate in Pass 2. Port-block is benign on peers whose
    # service isn't listening, so it's safe even against not-fully-built peers.
    is_fresh_activation = (ols_status == Cluster.STATUS_UNREADY)
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

    # The distribs created below span every online device — require the full
    # cross-node connectivity mesh before building on top of it (see
    # _wait_for_full_device_connectivity for the incident this prevents).
    try:
        _wait_for_full_device_connectivity(cl_id)
    except ValueError:
        set_cluster_status(cl_id, ols_status)
        raise

    # Failure-domain coverage check (best-effort: warn, don't block). A 2-FD
    # layout can never absorb a second independent failure once one domain
    # is down, so the hard minimum below (enforced at fresh activation) is
    # npcs+2, not npcs+1 -- this warning uses the same number so a
    # reactivation that's short of it gets the same signal without being
    # blocked (recovering a drifted layout must not turn into an outage).
    fd_desired_layout: t.Dict[str, t.Tuple[str, str]] = {}
    if cluster.enable_failure_domain:
        distinct_domains = {node.failure_domain for node in online_nodes if node.failure_domain >= 0}
        min_domains = cluster.distr_npcs + 2
        if len(distinct_domains) < min_domains:
            logger.warning(
                "Failure-domain feature is enabled but only %d distinct failure "
                "domain(s) are present (%s); at least %d are recommended to "
                "tolerate a full domain outage. Activating anyway with "
                "best-effort placement.",
                len(distinct_domains), sorted(distinct_domains) or "none", min_domains)

        # Fresh activation is the only point where the whole HA layout is
        # created at once, so the topology policy is enforced HARD here:
        # every domain must hold the same number of hosts (the +/-1 rule of
        # later expand/remove keeps oscillating around this balance), each
        # host must sit entirely in one domain, and the host topology must
        # be uniform. Roles are then assigned by an FD-interleaved rotation
        # (secondary always lands cross-domain; the planner's rotation
        # assumption becomes true by construction, which single-node
        # expansion relies on). Re-activation (recovery of an existing
        # layout) is deliberately NOT blocked: refusing to reactivate a
        # drifted cluster would turn a policy violation into an outage.
        if is_fresh_activation:
            from simplyblock_core.controllers.cluster_expansion import planner as fd_planner

            def _fd_fail(msg: str) -> None:
                set_cluster_status(cl_id, ols_status)
                raise ValueError(f"Failed to activate cluster: {msg}")

            hosts: t.Dict[str, t.List] = {}
            host_fd: t.Dict[str, int] = {}
            for node in online_nodes:
                if node.failure_domain < 0:
                    _fd_fail(f"node {node.get_id()} has no failure-domain id; "
                             f"all nodes need one on this cluster")
                hosts.setdefault(node.mgmt_ip, []).append(node)
                if host_fd.setdefault(node.mgmt_ip, node.failure_domain) != node.failure_domain:
                    _fd_fail(f"host {node.mgmt_ip} spans failure domains "
                             f"{host_fd[node.mgmt_ip]} and {node.failure_domain}; "
                             f"a host must sit entirely in one domain")

            fd_host_counts = Counter(host_fd.values())
            # See fd_activation_domain_count_violation's docstring: npcs+2
            # domains, not just the bare rotation-correctness minimum, so a
            # later single add/remove has a spare candidate instead of
            # stranding another node's secondary/tertiary with none at all.
            # This also subsumes the plain "at least two domains" floor.
            domain_count_violation = fd_planner.fd_activation_domain_count_violation(
                cluster.distr_npcs, len(fd_host_counts))
            if domain_count_violation:
                _fd_fail(domain_count_violation)
            if len(set(fd_host_counts.values())) != 1:
                _fd_fail(
                    f"failure domains must hold an EQUAL number of hosts at "
                    f"activation; current split: "
                    f"{ {fd: fd_host_counts[fd] for fd in sorted(fd_host_counts)} }. "
                    f"Add or remove hosts to balance the domains, then activate.")
            if cluster.ha_type == "ha":
                host_order = fd_planner.fd_interleaved_host_order(
                    [(ip, host_fd[ip]) for ip in hosts])
                topology = [[n.get_id() for n in hosts[ip]] for ip in host_order]
                try:
                    fd_desired_layout = fd_planner.rotation_layout(
                        topology, cluster.max_fault_tolerance)
                except ValueError as e:
                    _fd_fail(f"cannot build the failure-domain rotation: {e}")
                violations = fd_planner.compute_fd_layout_violations(
                    topology, cluster.max_fault_tolerance,
                    {n.get_id(): n.failure_domain for n in online_nodes},
                    layout=fd_desired_layout)
                if violations:
                    _fd_fail("failure-domain layout invariant violated: "
                             + "; ".join(violations))

    for node in online_nodes:
        if cluster.is_single_node or len(online_nodes) <= 2:
            node.physical_label = 0
        else:
            node.physical_label = storage_node_ops.get_next_physical_device_order(node)
        node.write_to_db()

    records = db_controller.get_cluster_capacity(cluster)
    max_size = records[0]['size_total']

    used_nodes_as_sec: t.List[str] = []
    used_nodes_as_tertiary: t.List[str] = []
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    # Process primaries grouped by failure domain. get_secondary_nodes/
    # get_secondary_nodes_2 (and their splice repairs) already sort their own
    # candidate scan by domain, which alone is enough to keep the assignment
    # domain-disjoint when domains are evenly sized. But once any node needs
    # splice-repair (uneven domain sizes, some conflict unavoidable), the
    # repair works off whatever partial assignment already exists -- so which
    # primary gets processed first still changes the outcome. Grouping here
    # too makes the result deterministic instead of order-dependent in that
    # case. A no-op when FD is disabled (all nodes share one failure_domain).
    # Fresh FD+HA activation bypasses this fallback via fd_desired_layout,
    # but reactivation and non-HA/non-fresh paths still rely on it.
    snodes = sorted(snodes, key=lambda n: n.failure_domain)
    if cluster.ha_type == "ha":
        for snode in snodes:
            # Do not assign secondary to removed node
            if snode.status == StorageNode.STATUS_REMOVED:
                continue
            if snode.is_secondary_node:  # pass
                continue
            if snode.secondary_node_id:
                sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                sec_node.lvstore_stack_secondary = snode.get_id()
                sec_node.write_to_db()
                used_nodes_as_sec.append(snode.secondary_node_id)
            else:
                if snode.get_id() in fd_desired_layout:
                    # FD-interleaved rotation (fresh FD activation): the
                    # secondary is cross-domain by construction.
                    secondary_nodes = [fd_desired_layout[snode.get_id()][0]]
                else:
                    secondary_nodes = storage_node_ops.get_secondary_nodes(snode)
                if secondary_nodes:
                    snode = db_controller.get_storage_node_by_id(snode.get_id())
                    snode.secondary_node_id = secondary_nodes[0]
                    snode.write_to_db()
                    sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                    sec_node.lvstore_stack_secondary = snode.get_id()
                    sec_node.write_to_db()
                elif not storage_node_ops.splice_stranded_secondary(snode):
                    # get_secondary_nodes()'s greedy walk closed a cycle that
                    # excludes this node, and there isn't even one existing
                    # pairing left to splice it into (only possible this early
                    # in the pass, before 2+ pairings exist).
                    set_cluster_status(cl_id, ols_status)
                    raise ValueError("Failed to activate cluster, No enough secondary nodes")
                snode = db_controller.get_storage_node_by_id(snode.get_id())
                used_nodes_as_sec.append(snode.secondary_node_id)

            # Assign second secondary when max_fault_tolerance >= 2
            if cluster.max_fault_tolerance >= 2 and not snode.tertiary_node_id:
                snode = db_controller.get_storage_node_by_id(snode.get_id())
                sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                if fd_desired_layout.get(snode.get_id(), ("", ""))[1]:
                    # FD-interleaved rotation: tertiary from the same
                    # deterministic layout as the secondary above.
                    secondary_nodes_2 = [fd_desired_layout[snode.get_id()][1]]
                else:
                    secondary_nodes_2 = storage_node_ops.get_secondary_nodes_2(
                        snode,
                        exclude_ids=[snode.secondary_node_id] + used_nodes_as_tertiary,
                        exclude_mgmt_ips=[sec_node.mgmt_ip],
                        exclude_failure_domains=[sec_node.failure_domain],
                        exclude_physical_labels=[sec_node.physical_label],
                    )
                if secondary_nodes_2:
                    snode.tertiary_node_id = secondary_nodes_2[0]
                    snode.write_to_db()
                    sec_node_2 = db_controller.get_storage_node_by_id(snode.tertiary_node_id)
                    sec_node_2.lvstore_stack_tertiary = snode.get_id()
                    sec_node_2.write_to_db()
                elif not storage_node_ops.splice_stranded_tertiary(snode):
                    # get_secondary_nodes_2()'s greedy walk closed a cycle that
                    # excludes this node, and there isn't even one existing
                    # tertiary pairing left to splice it into.
                    set_cluster_status(cl_id, ols_status)
                    raise ValueError("Failed to activate cluster, not enough nodes for dual fault tolerance")
                snode = db_controller.get_storage_node_by_id(snode.get_id())
                used_nodes_as_tertiary.append(snode.tertiary_node_id)

    # Pass 1: bring up the primary LVS on every online primary node.
    #
    # Re-activation (recreate_lvstore, activation_mode=True) only touches the
    # node being recreated plus RPCs to its peers, and every worker operates on
    # a distinct node — safe to fan out (bounded pool). A fresh create_lvstore
    # additionally writes its secondary/tertiary records (full-object
    # read-modify-write), and in a cross-pair layout the same record is
    # written both as "own" by its create and as "sec" by its partner's —
    # so creates fan out on the pool too, serializing only creates whose
    # touched-record sets intersect (per-node locks taken in sorted order,
    # the Pass 3 pattern). The old fully-serial loop cost ~40s x n — 22 min
    # at n=32, the dominant cost of a fresh activation (2026-07-13 audit).
    # Port allocation inside create_lvstore is separately serialized by
    # storage_node_ops._lvstore_port_alloc_lock.
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    pass1_recreate_ids: t.List[str] = []
    pass1_create_ids: t.List[str] = []
    for snode in snodes:
        if snode.is_secondary_node:  # pass
            continue
        if snode.status != StorageNode.STATUS_ONLINE:
            continue
        # Re-read node fresh before lvstore creation to avoid writing stale fields
        # (previous create_lvstore calls may have modified this node as a secondary)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        if snode.lvstore and force_lvstore_create is False:
            pass1_recreate_ids.append(snode.get_id())
        else:
            pass1_create_ids.append(snode.get_id())

    def _set_lvstore_status(node_id, value) -> None:
        # Atomic: full-object writes of node records race concurrent
        # parallel-pass workers AND phase transitions on the same record —
        # a stale copy written back resurrects a just-cleared restart phase
        # (observed twice on fresh activation, 2026-07-10 20:22 soak run).
        node = db_controller.get_storage_node_by_id(node_id)
        db_controller.atomic_update(
            node, lambda n, v=value: setattr(n, "lvstore_status", v))

    def _finish_pass1_node(node_id, ret) -> None:
        if ret:
            _set_lvstore_status(node_id, "ready")

            # Create S3 bdev for backup support (only if backup is configured)
            if cluster.backup_config:
                snode = db_controller.get_storage_node_by_id(node_id)
                backup_controller.create_s3_bdev(snode, cluster.backup_config)

        else:
            _set_lvstore_status(node_id, "failed")
            logger.error(f"Failed to restore lvstore on node {node_id}")
            set_cluster_status(cl_id, ols_status)
            raise ValueError("Failed to activate cluster")

    def _recreate_primary_lvs(node_id):
        snode = db_controller.get_storage_node_by_id(node_id)
        logger.warning(f"Node {node_id} already has lvstore {snode.lvstore}")
        return storage_node_ops.recreate_lvstore(snode, activation_mode=True)

    if pass1_recreate_ids:
        pass1_results: t.Dict[str, t.Any] = {}
        pass1_errors: t.List[ValueError] = []
        workers = min(constants.CLUSTER_ACTIVATION_MAX_PARALLEL_NODES, len(pass1_recreate_ids))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="activate-p1") as pool:
            futures = {pool.submit(_recreate_primary_lvs, nid): nid for nid in pass1_recreate_ids}
            for future in as_completed(futures):
                node_id = futures[future]
                try:
                    pass1_results[node_id] = future.result()
                except storage_node_ops.LVSRestartRequiredError as e:
                    logger.error(e)
                    pass1_errors.append(ValueError(
                        f"Failed to activate cluster: node {e.node_id} holds "
                        f"partial state for LVS {e.lvs_name} that examine could "
                        f"not recover. Restart node {e.node_id} before activating."))
                except Exception as e:
                    logger.error(e)
                    pass1_errors.append(ValueError("Failed to activate cluster"))
        if pass1_errors:
            set_cluster_status(cl_id, ols_status)
            raise pass1_errors[0]
        for node_id in pass1_recreate_ids:
            _finish_pass1_node(node_id, pass1_results.get(node_id))

    if pass1_create_ids:
        # Lock set per create = the records create_lvstore writes: the node
        # itself plus its secondary/tertiary. Locks are acquired in sorted-id
        # order so two creates with intersecting sets serialize deadlock-free
        # while disjoint pairs run concurrently.
        pass1_create_lock_ids: t.Dict[str, t.List[str]] = {}
        pass1_create_locks: t.Dict[str, threading.Lock] = {}
        for nid in pass1_create_ids:
            n = db_controller.get_storage_node_by_id(nid)
            touched = {nid}
            if n.secondary_node_id:
                touched.add(n.secondary_node_id)
            if n.tertiary_node_id:
                touched.add(n.tertiary_node_id)
            pass1_create_lock_ids[nid] = sorted(touched)
            for lid in pass1_create_lock_ids[nid]:
                pass1_create_locks.setdefault(lid, threading.Lock())

        def _create_primary_lvs(node_id):
            locks = [pass1_create_locks[lid] for lid in pass1_create_lock_ids[node_id]]
            for lk in locks:
                lk.acquire()
            try:
                snode = db_controller.get_storage_node_by_id(node_id)
                return storage_node_ops.create_lvstore(
                    snode, cluster.distr_ndcs, cluster.distr_npcs, cluster.distr_bs,
                    cluster.distr_chunk_bs, cluster.page_size_in_blocks, max_size)
            finally:
                for lk in reversed(locks):
                    lk.release()

        create_results: t.Dict[str, t.Any] = {}
        create_errors: t.List[ValueError] = []
        workers = min(constants.CLUSTER_ACTIVATION_MAX_PARALLEL_NODES, len(pass1_create_ids))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="activate-p1c") as pool:
            futures = {pool.submit(_create_primary_lvs, nid): nid for nid in pass1_create_ids}
            for future in as_completed(futures):
                node_id = futures[future]
                try:
                    create_results[node_id] = future.result()
                except Exception as e:
                    logger.error(e)
                    create_errors.append(ValueError("Failed to activate cluster"))
        if create_errors:
            set_cluster_status(cl_id, ols_status)
            raise create_errors[0]
        for node_id in pass1_create_ids:
            _finish_pass1_node(node_id, create_results.get(node_id))

    # Pass 2: Recreate secondary/tertiary LVS on every node that participates
    # as a non-leader for another node's LVS. In a ring topology (FTT=2 with
    # 6 nodes) every node is both a primary AND a secondary/tertiary — the old
    # is_secondary_node filter only matched dedicated secondary-only nodes,
    # skipping the ring participants entirely.
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    pass2_ids: t.List[str] = []
    for snode in snodes:
        if snode.status != StorageNode.STATUS_ONLINE:
            continue
        if db_controller.get_primary_storage_nodes_by_secondary_node_id(snode.get_id()):
            pass2_ids.append(snode.get_id())

    # Workers fan out per non-leader node, but work on the SAME primary must
    # never interleave: with FTT=2 a primary's LVS is recreated on both its
    # secondary and tertiary, and the leader port-block plus the
    # lvstore_status writes on that primary are not concurrency-safe.
    # Pre-created per-primary locks serialize exactly that, nothing more.
    pass2_primary_locks: t.Dict[str, threading.Lock] = {}
    for node_id in pass2_ids:
        for p in db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id):
            pass2_primary_locks.setdefault(p.get_id(), threading.Lock())

    def _recreate_non_leader_lvs(node_id) -> bool:
        snode = db_controller.get_storage_node_by_id(node_id)
        primary_nodes = db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id)
        logger.info(f"recreating secondary/tertiary LVS on node {node_id}")
        ret = True
        for primary_node in primary_nodes:
            with pass2_primary_locks[primary_node.get_id()]:
                # Re-read under the lock: a peer worker (the other non-leader of
                # this primary) may have written lvstore_status meanwhile.
                # Atomic: a full-object write here races the primary's OWN
                # pass-2 worker transitioning restart phases on the same
                # record — writing a stale copy back resurrects a cleared
                # phase (stale-phase generator, 2026-07-10).
                primary_node = db_controller.get_storage_node_by_id(primary_node.get_id())
                db_controller.atomic_update(
                    primary_node,
                    lambda n: setattr(n, "lvstore_status", "in_creation"))

                # On re-activation the primary's LVS is still alive and serving
                # client I/O — snode's examine of its non-leader raid0 will race
                # the leader's blob-metadata writes unless we quiesce the leader
                # first. We do this with a firewall-only port-block on the leader:
                # it has no effect on a peer whose service isn't listening (per
                # design, safe even when peer stacks aren't fully built yet) but
                # it stops the live leader from issuing writes that race the
                # examine. We deliberately do NOT switch the helper out of
                # activation_mode here: that would enable peer leader/distrib/
                # lvstore/hublvol RPCs which presume the peer's full stack is up.
                leader_blocked = False
                leader_port = None
                if not is_fresh_activation and primary_node.status == StorageNode.STATUS_ONLINE:
                    try:
                        leader_port = primary_node.get_lvol_subsys_port(primary_node.lvstore)
                        port_block.set_port(primary_node, leader_port, block=True, timeout=3, retry=1)
                        tcp_ports_events.port_deny(primary_node, leader_port)
                        leader_blocked = True
                        time.sleep(0.5)
                    except Exception as e:
                        logger.warning(
                            "Re-activation: port-block on leader %s for %s failed: %s — "
                            "proceeding without block (secondary examine may race live leader writes)",
                            primary_node.get_id(), primary_node.lvstore, e)

                try:
                    try:
                        r = storage_node_ops.recreate_lvstore_on_non_leader(
                            snode, primary_node, primary_node, activation_mode=True)
                    except storage_node_ops.LVSRestartRequiredError as e:
                        logger.error(e)
                        raise ValueError(
                            f"Failed to activate cluster: node {e.node_id} holds "
                            f"partial state for LVS {e.lvs_name} (non-leader). "
                            f"Restart node {e.node_id} before activating.")
                finally:
                    if leader_blocked:
                        try:
                            port_block.set_port(primary_node, leader_port, block=False, timeout=3, retry=1)
                            tcp_ports_events.port_allowed(primary_node, leader_port)
                        except Exception as ue:
                            logger.error(
                                "Failed to unblock leader %s:%s after non-leader recreate: %s — scheduling port_allow",
                                primary_node.get_id(), leader_port, ue)
                            try:
                                tasks_controller.add_port_allow_task(
                                    primary_node.cluster_id, primary_node.get_id(), leader_port)
                            except Exception as se:
                                logger.error("Failed to schedule port_allow fallback: %s", se)
            if not r:
                ret = False

        if ret:
            _set_lvstore_status(node_id, "ready")
        else:
            _set_lvstore_status(node_id, "failed")
            logger.error(f"Failed to restore lvstore on node {node_id}")
            raise ValueError("Failed to activate cluster")
        return True

    if pass2_ids:
        pass2_errors: t.List[ValueError] = []
        workers = min(constants.CLUSTER_ACTIVATION_MAX_PARALLEL_NODES, len(pass2_ids))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="activate-p2") as pool:
            futures = {pool.submit(_recreate_non_leader_lvs, nid): nid for nid in pass2_ids}
            for future in as_completed(futures):
                try:
                    future.result()
                except ValueError as e:
                    pass2_errors.append(e)
                except Exception as e:
                    logger.error(e)
                    pass2_errors.append(ValueError("Failed to activate cluster"))
        if pass2_errors:
            set_cluster_status(cl_id, ols_status)
            raise pass2_errors[0]

    # --- Pass 3: Create hublvols and cross-connections ---
    # All lvstores (primary + secondary/tertiary) are now up. Safe to create
    # hublvols and connect peers. This mirrors the logic in create_lvstore()
    # lines 5350-5379 and must tolerate offline nodes (FTT=1 or FTT=2).
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    pass3_ids = [n.get_id() for n in snodes
                 if not n.is_secondary_node and n.status == StorageNode.STATUS_ONLINE]

    # Workers fan out per primary, but a node may serve as secondary/tertiary
    # for several primaries: hublvol create/connect mutates DB state on both
    # the primary and its peers, so each worker holds the locks of every node
    # it touches. Locks are pre-created and acquired in sorted-id order so two
    # workers sharing a peer cannot deadlock.
    pass3_node_locks: t.Dict[str, threading.Lock] = {
        n.get_id(): threading.Lock() for n in snodes}

    def _wire_hublvols(node_id) -> None:
        snode = db_controller.get_storage_node_by_id(node_id)

        secondary_ids = []
        if snode.secondary_node_id:
            secondary_ids.append(snode.secondary_node_id)
        if snode.tertiary_node_id:
            secondary_ids.append(snode.tertiary_node_id)

        if not secondary_ids:
            return

        held: t.List[threading.Lock] = []
        try:
            for nid in sorted({node_id, *secondary_ids}):
                lock = pass3_node_locks.setdefault(nid, threading.Lock())
                lock.acquire()
                held.append(lock)

            # Create hublvol on primary
            try:
                if not snode.recreate_hublvol():
                    logger.error("Failed to recreate hublvol on %s", node_id)
            except Exception as e:
                logger.error("Error creating hublvol on %s: %s", node_id, e)

            # Create secondary hublvol on sec_1 (for tertiary multipath
            # failover). sec_1 is the CONFIGURED secondary — never
            # secondary_ids[0], which is the tertiary whenever
            # secondary_node_id is unset (e.g. demoted after a failover).
            sec1 = None
            if snode.secondary_node_id:
                sec1 = db_controller.get_storage_node_by_id(snode.secondary_node_id)
            if sec1 and sec1.status == StorageNode.STATUS_ONLINE:
                try:
                    snode = db_controller.get_storage_node_by_id(node_id)
                    sec1.create_secondary_hublvol(snode, cluster.nqn)
                except Exception as e:
                    logger.error("Error creating secondary hublvol on sec_1 %s: %s", sec1.get_id(), e)

            # Connect each secondary/tertiary to primary's hublvol
            for sec_node_id in secondary_ids:
                sec_node = db_controller.get_storage_node_by_id(sec_node_id)
                if sec_node.status != StorageNode.STATUS_ONLINE:
                    continue
                try:
                    # Brief settle beat before the connect; connect_to_hublvol
                    # itself retries via the reconnect coordinator, so a full
                    # 1s per edge was pure serial latency across the pass.
                    time.sleep(0.2)
                    # Role and failover from topology, never list position:
                    # with secondary_node_id unset the tertiary sits at
                    # index 0 and an index rule marks it "secondary" — a
                    # duplicate secondary role on the LVS (recurred in
                    # mass_create_delete_k8s 2026-07-14; each LVS must hold
                    # a unique role per node).
                    is_tert = sec_node_id == snode.tertiary_node_id
                    failover_node = sec1 if is_tert and sec1 and sec1.status == StorageNode.STATUS_ONLINE else None
                    sec_role = "tertiary" if is_tert else "secondary"
                    sec_node.connect_to_hublvol(snode, failover_node=failover_node, role=sec_role)
                except Exception as e:
                    logger.error("Error connecting %s to hublvol on %s: %s", sec_node.get_id(), node_id, e)
        finally:
            for lock in reversed(held):
                lock.release()

    if pass3_ids:
        workers = min(constants.CLUSTER_ACTIVATION_MAX_PARALLEL_NODES, len(pass3_ids))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="activate-p3") as pool:
            for future in as_completed({pool.submit(_wire_hublvols, nid) for nid in pass3_ids}):
                try:
                    future.result()
                except Exception as e:
                    # Same tolerance as the sequential loop: hublvol wiring
                    # errors are logged, not fatal to activation.
                    logger.error("Pass 3 hublvol wiring worker failed: %s", e)

    # reorder qos classes ids
    qos_classes = db_controller.get_qos(cl_id)
    index = 1
    for qos_class in qos_classes:
        if qos_class.class_name == "Default":
            qos_class.class_id = 0
        else:
            qos_class.class_id = index
            index += 1
        qos_class.write_to_db()

    if cluster.is_qos_set():
        for node in db_controller.get_storage_nodes_by_cluster_id(cl_id):
            if node.status == StorageNode.STATUS_ONLINE:
                logger.info(f"Setting Alcemls QOS weights on node {node.get_id()}")
                ret = node.rpc_client().alceml_set_qos_weights(qos_controller.get_qos_weights_list(cl_id))
                if not ret:
                    logger.error(f"Failed to set Alcemls QOS on node: {node.get_id()}")

    # Start JC compression on each node
    # (release-upgrade guard: held until `cluster upgrade-complete`, remove
    # with the jc_compression_upgrade plugin)
    if ols_status == Cluster.STATUS_UNREADY and not jc_compression_upgrade.resume_is_held(cluster):
        for node in db_controller.get_storage_nodes_by_cluster_id(cl_id):
            if node.status == StorageNode.STATUS_ONLINE:
                ret, err = node.rpc_client().jc_suspend_compression(jm_vuid=node.jm_vuid, suspend=False)
                if not ret:
                    logger.info("Failed to resume JC compression adding task...")
                    tasks_controller.add_jc_comp_resume_task(node.cluster_id, node.get_id(), jm_vuid=node.jm_vuid)

    if not cluster.cluster_max_size:
        cluster = db_controller.get_cluster_by_id(cl_id)
        cluster.cluster_max_size = max_size
        cluster.cluster_max_devices = dev_count
        cluster.cluster_max_nodes = len(online_nodes)
        cluster.write_to_db(db_controller.kv_store)

    # --- Pass 4: open client IO only now, with correct ANA ---
    # Pass 1/2 created every client-facing listener INACCESSIBLE so no client IO
    # could flow while lvstores were coming up and before Pass 3 wired the
    # hublvol redirects. Now that redirects are connected and leadership is
    # settled, flip each listener to its correct ANA state: optimized on the
    # LVS's primary, non_optimized on its secondary/tertiary. Only after this do
    # we set the cluster ACTIVE — so clients never resume IO against a primary
    # whose redirect to its peers isn't established (which is what produced the
    # mid-activation writer-conflict / EIO).
    def _set_node_ana(node_id) -> None:
        snode = db_controller.get_storage_node_by_id(node_id)
        node_lvols = [lv for lv in db_controller.get_lvols_by_node_id(node_id)
                      if lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION]]
        if not node_lvols:
            return
        # primary path -> optimized
        for lv in node_lvols:
            try:
                storage_node_ops._set_lvol_ana_on_node(lv, snode, "optimized")
            except Exception as e:
                logger.error("Pass 4: set optimized ANA on primary %s for %s failed: %s",
                             node_id, lv.nqn, e)
        # secondary/tertiary paths -> non_optimized
        for sec_id in [snode.secondary_node_id, snode.tertiary_node_id]:
            if not sec_id:
                continue
            sec_node = db_controller.get_storage_node_by_id(sec_id)
            if not sec_node or sec_node.status != StorageNode.STATUS_ONLINE:
                continue
            for lv in node_lvols:
                try:
                    storage_node_ops._set_lvol_ana_on_node(lv, sec_node, "non_optimized")
                except Exception as e:
                    logger.error("Pass 4: set non_optimized ANA on %s for %s failed: %s",
                                 sec_node.get_id(), lv.nqn, e)

    # ANA flips are RPC-only (no DB writes) so the per-primary workers need no
    # locks; different primaries touch different subsystems even when they
    # share a secondary node.
    pass4_ids = [n.get_id() for n in db_controller.get_storage_nodes_by_cluster_id(cl_id)
                 if not n.is_secondary_node and n.status == StorageNode.STATUS_ONLINE]
    if pass4_ids:
        workers = min(constants.CLUSTER_ACTIVATION_MAX_PARALLEL_NODES, len(pass4_ids))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="activate-p4") as pool:
            for future in as_completed({pool.submit(_set_node_ana, nid) for nid in pass4_ids}):
                try:
                    future.result()
                except Exception as e:
                    logger.error("Pass 4 ANA worker failed: %s", e)

    # The cluster is now active and about to serve IO. During node-add the
    # storage MCP was created wide (= parallel-add count) so the first-time
    # CPU-topology reboots happened in one parallel wave rather than a
    # serialized queue. Now narrow it to the cluster's fault tolerance so any
    # future MachineConfig/KubeletConfig rollout never reboots more storage
    # nodes at once than the data plane can absorb. Done here (success path,
    # before flipping to ACTIVE) so a failed/aborted activation leaves the
    # cluster non-serving with the wide value — harmless, since no data is at
    # risk until it goes ACTIVE. (Use max_fault_tolerance - 1 instead if you
    # want headroom for an unplanned failure concurrent with a rollout.)
    utils.set_storage_mcp_max_unavailable(cl_id, cluster.max_fault_tolerance)

    _record_activated_nodes(cl_id)
    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    logger.info("Cluster activated successfully")


def cluster_expand(cl_id) -> None:
    cluster = db_controller.get_cluster_by_id(cl_id)

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
                raise ValueError("A minimum of 2 new nodes are required to expand cluster")

            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.secondary_node_id = secondary_nodes[0]
            snode.write_to_db()

            sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
            sec_node.lvstore_stack_secondary = snode.get_id()
            sec_node.write_to_db()

        if cluster.ha_type == "ha" and cluster.max_fault_tolerance >= 2 and not snode.tertiary_node_id:
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
            # Expansion must honor the same host / failure-domain / physical-label
            # anti-affinity as initial activation: the tertiary has to be
            # disjoint from BOTH the primary and the already-picked secondary.
            # (Previously this used get_secondary_nodes with only the secondary
            # excluded by id, so an expanded cluster could land the tertiary on
            # the secondary's host or domain.)
            secondary_nodes_2 = storage_node_ops.get_secondary_nodes_2(
                snode,
                exclude_ids=[snode.secondary_node_id],
                exclude_mgmt_ips=[sec_node.mgmt_ip],
                exclude_failure_domains=[sec_node.failure_domain],
                exclude_physical_labels=[sec_node.physical_label],
            )
            if not secondary_nodes_2:
                set_cluster_status(cl_id, ols_status)
                raise ValueError("A minimum of 3 new nodes are required to expand cluster with dual fault tolerance")

            snode.tertiary_node_id = secondary_nodes_2[0]
            snode.write_to_db()

            sec_node_2 = db_controller.get_storage_node_by_id(snode.tertiary_node_id)
            sec_node_2.lvstore_stack_tertiary = snode.get_id()
            sec_node_2.write_to_db()

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

    _record_activated_nodes(cl_id)
    set_cluster_status(cl_id, Cluster.STATUS_ACTIVE)
    logger.info("Cluster expanded successfully")


def get_cluster_status(cl_id) -> t.List[dict]:
    db_controller.get_cluster_by_id(cl_id)  # ensure exists

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
    cluster = db_controller.get_cluster_by_id(cl_id)

    if cluster.status == status:
        return

    # Transactional compare-and-set: concurrent node-adds (now parallel) both
    # call this, and a plain read+write_to_db would clobber any other cluster
    # field a peer updated in between. atomic_update re-reads inside the tx and
    # only writes the status field change.
    captured = {}

    def _mutate(fresh):
        if fresh.status == status:
            return False  # already at target (a peer won the race); don't write
        captured['old'] = fresh.status
        fresh.status = status
        # Track when the cluster enters / leaves IN_ACTIVATION so the
        # storage_node_monitor watchdog can detect a wedged activation and
        # revert it. A half-finished cluster_activate otherwise leaves the
        # cluster stuck in IN_ACTIVATION forever — auto-restart refuses to queue
        # while the cluster is not SUSPENDED, so it can never recover on its own
        # (incident 2026-06-25). Stamped inside the CAS so it is written
        # atomically with the status flip.
        if status == Cluster.STATUS_IN_ACTIVATION:
            fresh.in_activation_since = datetime.now(timezone.utc).isoformat()
            fresh.activation_heartbeat = fresh.in_activation_since
        elif captured['old'] == Cluster.STATUS_IN_ACTIVATION:
            fresh.in_activation_since = ""
            fresh.activation_heartbeat = ""
        # Leaving suspension for a healthy status closes the current
        # suspend-recovery episode: clear the drain marker so the next
        # suspension starts a fresh drain (auto-restart paused -> drain ->
        # resume). Kept set across the suspended<->in_activation flapping of a
        # single recovery so the drain does not restart on every failed
        # activation attempt. Inside the CAS so it is written atomically.
        if status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
            fresh.suspend_drain_complete = False
        return True

    updated = db_controller.atomic_update(cluster, _mutate)
    if updated is None or 'old' not in captured:
        return
    cluster_events.cluster_status_change(updated, updated.status, captured['old'])


def cluster_set_read_only(cl_id) -> None:
    cluster = db_controller.get_cluster_by_id(cl_id)

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
    cluster = db_controller.get_cluster_by_id(cl_id)

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


def set_shared_placement(cl_id, enable=True, force=False) -> bool:
    """Flip the cluster-wide shared_placement flag for distrib bdevs.

    Sequence (per upgrade procedure):
      1. Preflight: every storage node must be ONLINE; cluster status must
         be ACTIVE and not rebalancing. With force=True the rebalancing
         and node-status gates are bypassed (only valid for the off->on
         transition; off->on is always safe per the data-plane spec).
      2. For every online storage node, submit the runtime RPC
         ``distr_shared_placement`` with ``enable`` and no ``name`` so it
         applies to all distrib bdevs on that node.
      3. Persist the flag on the lvstore_stack[/_secondary/_tertiary]
         distrib entries of every node so that restarts re-create with
         the new mode.
      4. Persist cluster.shared_placement so future bdev_distrib_create
         calls (new nodes, new distribs) get the flag automatically.

    The off->on direction is always safe. The on->off direction is left
    for debug only and requires force=True; the spec calls out that a
    bdev created with shared_placement=True may host two layers sharing
    a storage_ID across columns on a page, so disabling it on such a
    bdev causes undefined behavior. Callers are expected to ensure the
    bdev is balanced or empty before flipping back.
    """
    cluster = db_controller.get_cluster_by_id(cl_id)
    enable = bool(enable)

    if cluster.shared_placement == enable:
        logger.info(
            "Cluster %s shared_placement already %s; nothing to do",
            cl_id, enable)
        return True

    # Direction-specific guards.
    if not enable and not force:
        logger.error(
            "Disabling shared_placement is a debug-only operation; pass "
            "force=True after verifying every distrib bdev is balanced or "
            "empty")
        return False

    # Preflight (skippable only via force; cluster-status gate is hard).
    if cluster.status != Cluster.STATUS_ACTIVE:
        logger.error(
            "Cluster %s is %s; shared_placement can only be toggled while "
            "the cluster is %s",
            cl_id, cluster.status, Cluster.STATUS_ACTIVE)
        return False
    if cluster.is_re_balancing and not force:
        logger.error(
            "Cluster %s is rebalancing; wait for rebalance to finish "
            "(or pass force=True for the off->on direction)", cl_id)
        return False

    nodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    if not force:
        non_online = [
            n for n in nodes if n.status != StorageNode.STATUS_ONLINE
        ]
        if non_online:
            ids = ", ".join(f"{n.get_id()[:8]}={n.status}" for n in non_online)
            logger.error(
                "Cluster %s has non-online storage nodes; refusing to toggle "
                "shared_placement: %s", cl_id, ids)
            return False

    # Step 2: dispatch the runtime RPC to every online node. We do this
    # before persisting so that if SPDK rejects the flip we don't end up
    # with a divergent DB state. Failures on individual nodes are logged
    # but do not abort the operation — the per-node lvstore_stack update
    # below also gates the restart-time behavior.
    failures = []
    for node in nodes:
        if node.status != StorageNode.STATUS_ONLINE:
            logger.info(
                "Skipping runtime shared_placement RPC on %s (status=%s)",
                node.get_id()[:8], node.status)
            continue
        try:
            rpc = node.rpc_client(timeout=10, retry=2)
            ok = rpc.distr_shared_placement(enable=enable)
            if not ok:
                failures.append(node.get_id())
                logger.warning(
                    "Node %s rejected distr_shared_placement(enable=%s)",
                    node.get_id()[:8], enable)
            # JM shares the same shared-placement migration as distrib: flip
            # this node's JM bdev too. Unlike distr_shared_placement, the JM
            # RPC requires an explicit bdev name (there is exactly one JM per
            # node, named jm_<node_id>). New JMs created after this point pick
            # up the mode from cluster.shared_placement at (re)create time.
            jm_name = f"jm_{node.get_id()}"
            ok_jm = rpc.jm_set_shared_placement(name=jm_name, enable=enable)
            if not ok_jm:
                failures.append(node.get_id())
                logger.warning(
                    "Node %s rejected jm_set_shared_placement(enable=%s)",
                    node.get_id()[:8], enable)
        except Exception:
            failures.append(node.get_id())
            logger.exception(
                "Node %s raised on distr/jm shared_placement(enable=%s)",
                node.get_id()[:8], enable)

    if failures and not force:
        logger.error(
            "Aborting shared_placement toggle: %d node(s) rejected the "
            "runtime RPC: %s", len(failures), failures)
        return False

    # Step 3: persist the flag in every stored distrib stack entry on
    # every node, so restarts re-create with the new mode without needing
    # to consult the cluster row. Peers recreate their bdevs from the
    # primary's lvstore_stack, so updating primaries covers them too.
    for node in nodes:
        # Atomic compare-and-set: mutate only lvstore_stack on the freshly-read
        # node so the long per-node loop above can't clobber a concurrent
        # node.status change (lost-update class — incident 2026-06-18). Returning
        # False (no entry changed) aborts the write.
        def _mut(n, en=enable):
            changed = False
            for entry in (n.lvstore_stack or []):
                if not isinstance(entry, dict) or entry.get("type") != "bdev_distr":
                    continue
                params = entry.setdefault("params", {})
                if not isinstance(params, dict):
                    continue
                current = params.get("shared_placement", False)
                if en and not current:
                    params["shared_placement"] = True
                    changed = True
                elif not en and current:
                    # remove rather than set False, so the param dict stays
                    # minimal and matches the default-construct case.
                    params.pop("shared_placement", None)
                    changed = True
            return changed

        db_controller.atomic_update(
            db_controller.get_storage_node_by_id(node.get_id()), _mut)

    # Step 4: persist on the cluster row (atomic, so it doesn't clobber a
    # concurrent cluster.status change committed by set_cluster_status).
    db_controller.atomic_update(
        db_controller.get_cluster_by_id(cl_id),
        lambda c, v=enable: setattr(c, "shared_placement", v))
    logger.info("Cluster %s shared_placement set to %s", cl_id, enable)
    return True


def switch_write_protection(cl_id) -> bool:
    """Move a cluster from v1 to v2 distrib write protection.

    Two steps, strictly in this order, so the recorded generation never claims
    more than the data plane has actually done:

      1. Send the runtime ``distr_write_protection_v2(enable=True)`` RPC to
         every ONLINE storage node, with no ``name`` so it covers every distrib
         bdev on that node. This is the only way an already-created distrib
         gains v2 -- a create parameter cannot reach a bdev that exists. Every
         online node must succeed; a single failure aborts the switch and
         leaves the cluster on v1, so recovery is just re-running the command.

      2. Only then stamp the generation: into each node's stored distrib params
         (so a restart replays the right key) and onto the cluster row (so
         newly created distribs, and nodes added later, pick it up).

    Offline nodes are not a failure and are not skipped: they have no running
    bdevs to migrate, and step 2 rewrites their stored stack, so their distribs
    come back on v2 when they next restart.

    Idempotent: a cluster already on v2 returns True untouched.
    """
    cluster = db_controller.get_cluster_by_id(cl_id)
    if cluster.write_protection_v2:
        logger.info(
            "Cluster %s already runs v2 write protection; nothing to do", cl_id)
        return True

    nodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    online = [n for n in nodes if n.status == StorageNode.STATUS_ONLINE]
    if not online:
        logger.error(
            "Cluster %s has no online storage node; cannot switch write "
            "protection", cl_id)
        return False

    # Step 1: the runtime RPC on every online node.
    failures = []
    for node in online:
        try:
            if node.rpc_client().distr_write_protection_v2(enable=True):
                logger.info("Node %s: v2 write protection activated",
                            node.get_id())
            else:
                failures.append(node.get_id())
                logger.error(
                    "Node %s rejected distr_write_protection_v2(enable=True)",
                    node.get_id())
        except Exception as e:
            failures.append(node.get_id())
            logger.error(
                "Node %s raised on distr_write_protection_v2(enable=True): %s",
                node.get_id(), e)

    if failures:
        logger.error(
            "Aborting write-protection switch: %d of %d online node(s) failed "
            "(%s). The cluster stays on v1 -- fix those nodes and re-run.",
            len(failures), len(online), ", ".join(failures))
        return False

    # Step 2a: persist the generation in every stored distrib stack entry, on
    # every node including offline ones. Atomic compare-and-set so the long
    # per-node RPC loop above cannot clobber a concurrent node.status write
    # (lost-update class -- incident 2026-06-18).
    for node in nodes:
        def _mut(n):
            changed = False
            for entry in (n.lvstore_stack or []):
                if not isinstance(entry, dict) or entry.get("type") != "bdev_distr":
                    continue
                params = entry.get("params")
                if not isinstance(params, dict):
                    continue
                before = ("write_protection" in params,
                          "write_protection_v2" in params)
                storage_node_ops.apply_write_protection_mode(params, True)
                if before != ("write_protection" in params,
                              "write_protection_v2" in params):
                    changed = True
            return changed

        db_controller.atomic_update(
            db_controller.get_storage_node_by_id(node.get_id()), _mut)

    # Step 2b: the cluster row (atomic, so it does not clobber a concurrent
    # cluster.status write from set_cluster_status).
    db_controller.atomic_update(
        db_controller.get_cluster_by_id(cl_id),
        lambda c: setattr(c, "write_protection_v2", True))
    logger.info("Cluster %s switched to v2 write protection (%d node(s))",
                cl_id, len(online))
    return True


def list() -> t.List[dict]:
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
            "Name": cl.cluster_name if cl.cluster_name is not None else "-",
            "NQN": cl.nqn,
            "ha_type": cl.ha_type,
            "#mgmt": len(mt),
            "#storage": len(st),
            "Mod": f"{cl.distr_ndcs}x{cl.distr_npcs}",
            "Status": status.upper(),
            "Replicate": cl.snapshot_replication_target_cluster,
        })
    return data



def list_all_info(cluster_id) -> str:
    cl = db_controller.get_cluster_by_id(cluster_id)

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
    for lvol in lvols:
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
    try:
        _ = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        logger.error(f"Cluster not found: {cluster_id}")
        return []

    cap_stats_keys = [
        "date",
        "size_total",
        "size_prov",
        "size_used",
        "size_free",
        "size_util",
        "size_prov_util",
    ]
    prom_client = PromClient(cluster_id)
    records = prom_client.get_cluster_metrics(cluster_id, cap_stats_keys, history)
    return utils.process_records(records, records_count, keys=cap_stats_keys)


def get_iostats_history(cluster_id, history_string, records_count=20, with_sizes=False) -> t.List[dict]:
    try:
        _ = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        logger.error(f"Cluster not found: {cluster_id}")
        return []

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

    prom_client = PromClient(cluster_id)
    records = prom_client.get_cluster_metrics(cluster_id, io_stats_keys, history_string)
    # combine records
    return utils.process_records(records, records_count, keys=io_stats_keys)


def get_ssh_pass(cluster_id) -> str:
    return db_controller.get_cluster_by_id(cluster_id).cli_pass.get_secret_value()


def get_secret(cluster_id) -> str:
    return db_controller.get_cluster_by_id(cluster_id).secret.get_secret_value()


def set_secret(cluster_id, secret: SecretStr) -> None:
    cluster = db_controller.get_cluster_by_id(cluster_id)
    plain = secret.get_secret_value().strip()
    if len(plain) < 20:
        raise ValueError("Secret must be at least 20 char")
    secret = SecretStr(plain)

    _create_update_user(cluster_id, cluster.grafana_endpoint, cluster.grafana_secret, secret, update_secret=True)

    cluster.secret = secret
    cluster.write_to_db(db_controller.kv_store)


def set_fabric(cluster_id, fabric) -> None:
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(cluster_id)
    protocols = parse_protocols(fabric)
    cluster.fabric_tcp = protocols["tcp"]
    cluster.fabric_rdma = protocols["rdma"]
    cluster.write_to_db(db_controller.kv_store)


def change_cluster_name(cluster_id, new_name) -> None:
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if new_name:
        for existing in db_controller.get_clusters():
            if existing.uuid != cluster_id and existing.cluster_name and existing.cluster_name == new_name:
                raise ValueError(f"A cluster with the name '{new_name}' already exists")
    old_name = cluster.cluster_name
    cluster.cluster_name = new_name
    cluster.write_to_db(db_controller.kv_store)
    cluster_events.cluster_name_change(cluster, new_name, old_name)
    logger.info(f"Cluster has been renamed: {old_name} -> {new_name}")


def get_logs(cluster_id, limit=50, **kwargs) -> t.List[dict]:
    db_controller.get_cluster_by_id(cluster_id)  # ensure exists

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

        logger.debug(record)
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
    return db_controller.get_cluster_by_id(cl_id).get_clean_dict()


def update_cluster(cluster_id, mgmt_only=False, restart=False, spdk_image=None, mgmt_image=None,
                   max_subsys=None, hugepages_mem=None, **kwargs) -> None:
    cluster = db_controller.get_cluster_by_id(cluster_id)  # ensure exists

    # Cluster-wide SPDK sizing is a settings write, not an upgrade. Handled
    # first, and when no image or control-plane flag was given this returns
    # before the rollout: changing a number must not drag a whole cluster
    # through an image update. Nodes adopt the new values on their next
    # restart, so nothing here restarts anything.
    if max_subsys is not None or hugepages_mem is not None:
        set_spdk_sizing(cluster_id, max_subsys=max_subsys,
                        hugepages_mem=hugepages_mem)
        if not (spdk_image or mgmt_image or mgmt_only):
            return

    # Release-specific pre-upgrade steps (simplyblock_core/release_upgrades/).
    # Must be the very first thing the upgrade does; raises to abort the
    # upgrade before anything was changed. Completed later by
    # `cluster upgrade-complete` (upgrade_complete below).
    release_upgrades.run_pre_update(cluster)

    # An upgraded cluster's existing distribs carry v1 write protection, and no
    # create parameter can retrofit a bdev that already exists -- only the
    # runtime distr_write_protection_v2 RPC can, via
    # `sbctl cluster switch-write-protection`. Stamp the generation back to v1
    # BEFORE the rolling restart below, because those restarts replay each
    # node's stored distrib stack and must replay it under the key the running
    # bdevs actually use.
    #
    # This also demotes a cluster that was already on v2: after an upgrade the
    # switch has to be re-run either way, and claiming v2 we have not verified
    # on the new image is the one failure mode worth avoiding here.
    if cluster.write_protection_v2:
        db_controller.atomic_update(
            db_controller.get_cluster_by_id(cluster_id),
            lambda c: setattr(c, "write_protection_v2", False))
        logger.info(
            "Cluster %s stamped back to v1 write protection for the upgrade; "
            "run `sbctl cluster switch-write-protection` once every node is "
            "back online", cluster_id)

    logger.info("Updating mgmt cluster")
    if cluster.mode == "docker":
        cluster_docker = utils.get_docker_client(cluster_id)
        service_image = constants.SIMPLY_BLOCK_DOCKER_IMAGE
        if mgmt_image:
            service_image = mgmt_image
        logger.info(f"Pulling image {service_image}")
        pull_docker_image_with_retry(cluster_docker, service_image)
        service_names = []
        image_parts = ["simplyblock-io/simplyblock:", "simplyblock/simplyblock:", "simply-block/simplyblock:"]
        for service in cluster_docker.services.list():
            container_image=service.attrs['Spec']['Labels']['com.docker.stack.image']
            for part in image_parts:
                if part in container_image:
                    if service.name in ["app_CachingNodeMonitor", "app_CachedLVolStatsCollector"]:
                        logger.info(f"Removing service {service.name}")
                        service.remove()
                    else:
                        logger.info(f"Updating service {service.name}")
                        service.update(image=service_image, force_update=True)
                        service_names.append(service.attrs['Spec']['Name'])
                    break

        if "app_SnapshotMonitor" not in service_names:
            utils.create_docker_service(
                cluster_docker=cluster_docker,
                service_name="app_SnapshotMonitor",
                service_file="python3 simplyblock_core/services/snapshot_monitor.py",
                service_image=service_image)

        if "app_TasksRunnerLVolSyncDelete" not in service_names:
            utils.create_docker_service(
                cluster_docker=cluster_docker,
                service_name="app_TasksRunnerLVolSyncDelete",
                service_file="python3 simplyblock_core/services/tasks_runner_sync_lvol_del.py",
                service_image=service_image)

        if "app_TasksRunnerJCCompResume" not in service_names:
            utils.create_docker_service(
                cluster_docker=cluster_docker,
                service_name="app_TasksRunnerJCCompResume",
                service_file="python3 simplyblock_core/services/tasks_runner_jc_comp.py",
                service_image=service_image)

        if "app_BackupService" not in service_names:
            utils.create_docker_service(
                cluster_docker=cluster_docker,
                service_name="app_BackupService",
                service_file="python3 simplyblock_core/services/tasks_runner_fdb_backup.py",
                service_image=service_image)

        logger.info("Done updating mgmt cluster")

    elif cluster.mode == "kubernetes":
        utils.load_kube_config_with_fallback()
        apps_v1 = k8s_client.AppsV1Api()
        namespace = constants.K8S_NAMESPACE
        image_parts = ["simplyblock-io/simplyblock:", "simplyblock/simplyblock:", "simply-block/simplyblock:"]
        service_image = mgmt_image or constants.SIMPLY_BLOCK_DOCKER_IMAGE
        deployment_names = []
        # Update Deployments
        deployments = apps_v1.list_namespaced_deployment(namespace=namespace)
        for deploy in deployments.items:
            if deploy.metadata.name == constants.ADMIN_DEPLOY_NAME:
                logger.info(f"Skipping deployment {deploy.metadata.name}")
                continue
            deployment_names.append(deploy.metadata.name)
            for c in deploy.spec.template.spec.containers:
                for part in image_parts:
                    if part in c.image:
                        logger.info(f"Updating deployment {deploy.metadata.name} image to {service_image}")
                        c.image = service_image
                        annotations = deploy.spec.template.metadata.annotations or {}
                        annotations["pod.kubernetes.io/restartedAt"] = datetime.now(timezone.utc).isoformat()
                        deploy.spec.template.metadata.annotations = annotations
                        apps_v1.patch_namespaced_deployment(
                            name=deploy.metadata.name,
                            namespace=namespace,
                            body={"spec": {"template": deploy.spec.template}})
                        break

        if "simplyblock-tasks-runner-sync-lvol-del" not in deployment_names:
            utils.create_k8s_service(
                namespace=namespace,
                deployment_name="simplyblock-tasks-runner-sync-lvol-del",
                container_name="tasks-runner-sync-lvol-del",
                service_file="simplyblock_core/services/tasks_runner_sync_lvol_del.py",
                container_image=service_image)

        if "simplyblock-snapshot-monitor" not in deployment_names:
            utils.create_k8s_service(
                namespace=namespace,
                deployment_name="simplyblock-snapshot-monitor",
                container_name="snapshot-monitor",
                service_file="simplyblock_core/services/snapshot_monitor.py",
                container_image=service_image)

        # Update DaemonSets
        daemonsets = apps_v1.list_namespaced_daemon_set(namespace=namespace)
        for ds in daemonsets.items:
            for c in ds.spec.template.spec.containers:
                for part in image_parts:
                    if part in c.image:
                        logger.info(f"Updating daemonset {ds.metadata.name} image to {service_image}")
                        c.image = service_image
                        annotations = ds.spec.template.metadata.annotations or {}
                        annotations["pod.kubernetes.io/restartedAt"] = datetime.now(timezone.utc).isoformat()
                        ds.spec.template.metadata.annotations = annotations
                        apps_v1.patch_namespaced_daemon_set(
                            name=ds.metadata.name,
                            namespace=namespace,
                            body={"spec": {"template": ds.spec.template}})
                        break

        logger.info("Done updating mgmt cluster")


    if mgmt_only:
        return

    if cluster.mode == "docker":
        logger.info("Updating spdk image on storage nodes")
        for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
            if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                node_docker = docker.DockerClient(base_url=f"tcp://{node.mgmt_ip}:2375", version="auto", timeout=60 * 5)
                img = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE
                if spdk_image:
                    img = spdk_image
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
            try:
                storage_node_ops.restart_storage_node(node.get_id(), force=True, spdk_image=spdk_image)
            except Exception as e:
                logger.debug(e)
                logger.error(f"Failed to restart node: {node.get_id()}")
                return

    # All storage nodes have been restarted onto the upgraded SPDK image.
    # Arm the one-shot per-chunk placement migration now — and only now,
    # after the full rolling restart — so storage_node_monitor switches the
    # cluster once it settles (ACTIVE, not rebalancing, all nodes online).
    # Skipped on the early-return failure path above, so a partial/failed
    # upgrade never arms it. No-op if the cluster is already on per-chunk.
    upgraded = db_controller.get_cluster_by_id(cluster_id)
    if not upgraded.shared_placement and not upgraded.shared_placement_migration_pending:
        upgraded.shared_placement_migration_pending = True
        upgraded.write_to_db(db_controller.kv_store)
        logger.info("Armed shared_placement migration for cluster %s post-upgrade", cluster_id)

    logger.info("Done")


def upgrade_complete(cluster_id) -> bool:
    """Completes a cluster upgrade started by update_cluster: runs the
    completion step of every release-upgrade plugin that left state on the
    cluster (e.g. resuming JC compression) and stamps the installed release.
    Safe to re-run: with no pending plugin state it only re-stamps."""
    cluster = db_controller.get_cluster_by_id(cluster_id)
    for message in release_upgrades.run_upgrade_complete(cluster):
        logger.info(message)
    return True


def cluster_grace_startup(cl_id, clear_data=False, spdk_image=None) -> None:
    get_cluster = db_controller.get_cluster_by_id(cl_id)  # ensure exists

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        logger.info(f"Shutting down node: {node.get_id()}")
        storage_node_ops.shutdown_storage_node(node.get_id(), force=True)
    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        logger.info(f"Restarting node: {node.get_id()}")
        storage_node_ops.restart_storage_node(node.get_id(), clear_data=clear_data, force=True, spdk_image=spdk_image)
        # time.sleep(5)
        get_node = db_controller.get_storage_node_by_id(node.get_id())
        if get_node.status != StorageNode.STATUS_ONLINE:
            raise ValueError("failed to restart node")
    if get_cluster.status == Cluster.STATUS_UNREADY:
        logger.info("Cluster is not activated yet, please manually activate it")

    else:
        while True:
            get_cluster = db_controller.get_cluster_by_id(cl_id)
            if get_cluster.status != Cluster.STATUS_ACTIVE:
                logger.info(f"wait for cluster to be active, current status is: {get_cluster.status}")
                time.sleep(5)
            else:
                break
    logger.info("Cluster gracefully started")



def _grace_shutdown_skipped(node) -> bool:
    """Nodes a full-cluster shutdown must not touch.

    See the rationale in cluster_grace_shutdown's loop.
    """
    return node.status in (StorageNode.STATUS_REMOVED,
                           StorageNode.STATUS_IN_REMOVAL)


def cluster_grace_shutdown(cl_id) -> None:
    db_controller.get_cluster_by_id(cl_id)  # ensure exists

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    for node in st:
        # REMOVED is terminal and must survive a cluster shutdown. Without
        # this filter the sweep force-shuts-down every record it can see,
        # and shutdown_storage_node drives in_shutdown -> offline, so a node
        # that was deliberately removed comes back as a plain offline member
        # (live 2026-09-03: one graceful-shutdown resurrected all four nodes
        # removed earlier that day). That is not cosmetic --
        # failure_domain_host_map skips only STATUS_REMOVED, so those records
        # start counting toward FD host balance again, and the next
        # activation or startup acts on nodes whose devices are already
        # failed_and_migrated and which own no lvstore.
        #
        # IN_REMOVAL is skipped because node_removal_orchestrate has already
        # shut that node down and owns the rest of its lifecycle.
        # PENDING_REMOVAL is deliberately NOT skipped -- the node is still up
        # and serving at that point, so a full-cluster shutdown must stop it
        # like any other member.
        if _grace_shutdown_skipped(node):
            logger.info(f"Skipping node {node.get_id()} with status: {node.status}")
            continue
        logger.info(f"Suspending node: {node.get_id()}")
        storage_node_ops.suspend_storage_node(node.get_id(), force=True)
        logger.info(f"Shutting down node: {node.get_id()}")
        storage_node_ops.shutdown_storage_node(node.get_id(), force=True)

    # Settle check. The sweep is serial, so a node it already passed can come
    # back up behind it -- that is exactly what happened on 2026-09-03, when
    # queued restart rows put s7457 and zdgtb back ONLINE seconds after the
    # sweep had shut them down, and the command still returned as if the
    # cluster were down. shutdown_storage_node now reaps those rows, but this
    # verifies the end state rather than assuming it: anything that resurrects
    # a node by another route is caught and stopped here, once.
    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    stragglers = [n for n in st
                  if not _grace_shutdown_skipped(n)
                  and n.status != StorageNode.STATUS_OFFLINE]
    for node in stragglers:
        logger.warning(
            f"Node {node.get_id()} is {node.status} after the shutdown sweep; "
            f"shutting it down again")
        storage_node_ops.shutdown_storage_node(node.get_id(), force=True)

    st = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    still_up = [n.get_id() for n in st
                if not _grace_shutdown_skipped(n)
                and n.status != StorageNode.STATUS_OFFLINE]
    if still_up:
        # Deliberately not raising: the caller asked for a shutdown and most
        # of the cluster is down, so failing here would be less useful than
        # saying precisely which nodes are not. An operator following up with
        # `sn shutdown` needs the list, not a traceback.
        logger.error(
            f"Graceful shutdown finished with {len(still_up)} node(s) not "
            f"offline: {still_up}")


def cluster_restart(cl_id) -> None:
    """Operator-requested full-cluster restart: force-shutdown every node that
    is not already offline, restart all nodes, then reactivate.

    Implemented by arming the suspend-recovery machinery instead of
    duplicating it: clear the deliberate-shutdown markers (so nodes an
    operator stopped earlier are restarted too, and the operator-caused-
    suspension suppression in the monitor disarms), reset the drain marker,
    and flip the cluster to SUSPENDED. The storage-node monitor then drives
    the full sequence: drain (parallel force-shutdown of every non-offline
    node), parallel auto-restart of all nodes, and the gated
    ``cluster_activate`` once every node is back ONLINE.

    Works from any steady state — ACTIVE/DEGRADED/READONLY (planned restart)
    or SUSPENDED (e.g. recover from a manual-shutdown-caused suspension).
    Returns immediately; progress is observable via ``cluster show`` /
    ``cluster get-logs`` (suspended -> in_activation -> active).
    """
    cluster = db_controller.get_cluster_by_id(cl_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
                              Cluster.STATUS_READONLY, Cluster.STATUS_SUSPENDED]:
        raise ValueError(
            f"Cluster restart requires a steady cluster state "
            f"(active/degraded/read_only/suspended), current: {cluster.status}")

    # Re-arm auto recovery for operator-stopped nodes: an explicit cluster
    # restart overrides the per-node "stay down" intent.
    for node in db_controller.get_storage_nodes_by_cluster_id(cl_id):
        if node.auto_restart_disabled:
            logger.info("Clearing deliberate-shutdown marker on node %s", node.get_id())
            db_controller.atomic_update(
                node, lambda n: setattr(n, "auto_restart_disabled", False))

    set_cluster_status(cl_id, Cluster.STATUS_SUSPENDED)

    # Reset the drain marker AFTER the status flip: set_cluster_status clears
    # it only when leaving suspension, and a marker left True (cluster already
    # SUSPENDED with a completed earlier drain) would make the drain a no-op.
    cluster = db_controller.get_cluster_by_id(cl_id)
    db_controller.atomic_update(
        cluster, lambda c: setattr(c, "suspend_drain_complete", False))

    logger.info(
        "Cluster %s restart initiated: monitor will drain all nodes, restart "
        "them in parallel and reactivate the cluster", cl_id)


def delete_cluster(cl_id) -> None:
    cluster = db_controller.get_cluster_by_id(cl_id)

    nodes = db_controller.get_storage_nodes_by_cluster_id(cl_id)
    if nodes:
        raise ValueError("Can only remove Empty cluster, Storage nodes found")

    pools = db_controller.get_pools(cl_id)
    if pools:
        raise ValueError("Can only remove Empty cluster, Pools found")

    logger.info(f"Deleting Cluster {cl_id}")
    cluster_events.cluster_delete(cluster)
    cluster.remove(db_controller.kv_store)
    logger.info("Done")

def set_(cl_id, attr, value) -> bool:
    cluster = db_controller.get_cluster_by_id(cl_id)
    key_splits = attr.split(".")
    key = key_splits[0]
    if key not in cluster.get_attrs_map():
        raise KeyError('Attribute not found')

    if len(key_splits) > 1:
        key_info = cluster.get_attrs_map()[key]
        if key_info["type"] is dict:
            sub_key = key_splits[1]
            if sub_key in cluster[key]:
                cluster[key][sub_key] = value
                logger.info(f"Setting {attr} to {value}")
                cluster.write_to_db()
                return True
    else:
        value = cluster.get_attrs_map()[attr]['type'](value)
        logger.info(f"Setting {attr} to {value}")
        setattr(cluster, attr, value)
        cluster.write_to_db()
    return True


def add_replication(source_cl_id, target_cl_id, timeout=0, target_pool=None) -> bool:
    db_controller = DBController()
    # The get_*_by_id() helpers raise KeyError rather than returning None, so
    # translate here; a bare KeyError traceback used to reach the operator.
    try:
        db_controller.get_cluster_by_id(source_cl_id)
    except KeyError:
        raise ValueError(f"Cluster not found: {source_cl_id}")

    try:
        db_controller.get_cluster_by_id(target_cl_id)
    except KeyError:
        raise ValueError(f"Target cluster not found: {target_cl_id}")

    logger.info("Updating Cluster replication target")
    new_pool = None
    if target_pool:
        # --target-pool is documented as "ID or name".
        try:
            pool = db_controller.get_pool_by_id_or_name(target_pool)
        except KeyError:
            raise ValueError(f"Pool not found: {target_pool}")
        if pool.status != Pool.STATUS_ACTIVE:
            raise ValueError(f"Pool not active: {target_pool}")
        # Store the UUID: the name is mutable, the reference must not be.
        new_pool = pool.get_id()
    new_timeout = timeout if (timeout and timeout > 0) else None

    # Atomic: mutate only the replication fields on the freshly-read cluster so
    # a concurrent cluster.status change is not clobbered (incident 2026-06-18).
    def _mut(c):
        c.snapshot_replication_target_cluster = target_cl_id
        if new_pool is not None:
            c.snapshot_replication_target_pool = new_pool
        if new_timeout is not None:
            c.snapshot_replication_timeout = new_timeout
        return True

    db_controller.atomic_update(db_controller.get_cluster_by_id(source_cl_id), _mut)
    logger.info("Done")
    return True


def rebalance(cluster_id) -> bool:
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            tasks_controller.add_device_mig_task_for_node(node.get_id())
    return True
