# coding=utf-8
"""Edge-cluster control flows (docs/edge_clusters_spec.md §5).

Everything long-running or retryable is a JobSchedule task processed by
services/tasks_runner_edge.py; the functions here either complete quickly or
enqueue. RPC and k8s access go through simplyblock_edge.rpc / .k8s so tests
can substitute them.
"""
import datetime
import logging
import time
import uuid as uuid_lib

from pydantic import SecretStr

from simplyblock_core import constants as core_constants, utils as core_utils
from simplyblock_core.controllers import events_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCException
from simplyblock_lib.tasks.runner import TaskResult
from simplyblock_edge import constants as edge_constants, db, k8s, stack
from simplyblock_edge.models import EdgeNode, EdgePartition, EdgeVolume
from simplyblock_edge.rpc import node_rpc_client

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------- clusters

def create_edge_cluster(name, k8s_api_url="", k8s_token="", k8s_ca_cert="",
                        k8s_namespace="simplyblock") -> Cluster:
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    for existing in db_controller.get_clusters():
        if existing.cluster_name == name:
            raise ValueError(f"Cluster with name {name} already exists")

    cluster = Cluster()
    cluster.uuid = str(uuid_lib.uuid4())
    cluster.cluster_name = name
    cluster.cluster_type = Cluster.TYPE_EDGE
    cluster.mode = "kubernetes"
    cluster.status = Cluster.STATUS_UNREADY
    cluster.secret = SecretStr(core_utils.generate_string(20))
    cluster.nqn = f"{core_constants.CLUSTER_NQN}:{cluster.uuid}"
    cluster.k8s_api_url = k8s_api_url
    cluster.k8s_token = SecretStr(k8s_token) if isinstance(k8s_token, str) else k8s_token
    cluster.k8s_ca_cert = k8s_ca_cert
    cluster.k8s_namespace = k8s_namespace
    cluster.write_to_db(db.kv_store())
    events_controller.log_event_cluster(
        cluster.uuid, events_controller.DOMAIN_CLUSTER,
        events_controller.EVENT_OBJ_CREATED, cluster,
        events_controller.CAUSED_BY_API, f"Edge cluster created: {name}")
    return cluster


def _require_edge_cluster(cluster_id) -> Cluster:
    cluster = db.get_cluster(cluster_id)
    if cluster.cluster_type != Cluster.TYPE_EDGE:
        raise ValueError(f"Cluster {cluster_id} is not an edge cluster")
    return cluster


def set_cluster_status(cluster, new_status, caused_by=events_controller.CAUSED_BY_MONITOR):
    """CAS the cluster status (edge statuses only: unready/active/degraded/
    suspended). Deliberately bypasses cluster_ops.set_cluster_status — that
    writer stamps hyperscale activation bookkeeping."""
    if cluster.status == new_status:
        return
    old = cluster.status

    def _mutate(fresh):
        if fresh.status == new_status:
            return False
        fresh.status = new_status
        return True

    db.atomic_update(cluster, _mutate)
    cluster.status = new_status
    events_controller.log_event_cluster(
        cluster.uuid, events_controller.DOMAIN_CLUSTER,
        events_controller.EVENT_STATUS_CHANGE, cluster, caused_by,
        f"Edge cluster status changed from {old} to {new_status}")


# -------------------------------------------------------------------- nodes

def _wait_for_rpc(rpc, timeout=edge_constants.EDGE_RPC_WAIT_TIMEOUT_SEC,
                  interval=edge_constants.EDGE_RPC_WAIT_INTERVAL_SEC,
                  sleep=time.sleep):
    deadline = time.monotonic() + timeout
    while True:
        try:
            if rpc.get_version():
                return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("SPDK RPC did not come up in time")
        sleep(interval)


def _ensure_aio(rpc, spec: stack.AioSpec):
    if not rpc.get_bdevs(name=spec.bdev_name):
        rpc.bdev_aio_create(spec.bdev_name, spec.device_path, spec.block_size)


def _ensure_raid(rpc, spec: stack.RaidSpec):
    if not rpc.get_bdevs(name=spec.name):
        rpc.bdev_raid_create(spec.name, spec.base_bdevs, raid_level=spec.raid_level,
                             strip_size_kb=spec.strip_size_kb or 4,
                             superblock=spec.superblock)


def _ensure_transport(rpc):
    if not rpc.transport_list(trtype="TCP"):
        rpc.transport_create("TCP")


def _ensure_subsystem(rpc, nqn, serial):
    if rpc.subsystem_get(nqn) is None:
        rpc.subsystem_create(nqn, serial, model_number="simplyblock-edge")


def _subsystem_has_ns(rpc, nqn, bdev_name) -> bool:
    subsystem = rpc.subsystem_get(nqn) or {}
    return any(ns.get('bdev_name') == bdev_name for ns in subsystem.get('namespaces', []))


def _subsystem_has_listener(rpc, nqn, addr, port) -> bool:
    subsystem = rpc.subsystem_get(nqn) or {}
    return any(la.get('traddr') == addr and str(la.get('trsvcid')) == str(port)
               for la in (entry.get('address', entry) for entry in subsystem.get('listen_addresses', [])))


def _build_local_stack(rpc, node) -> str:
    """Idempotently create the node's aio bdevs + local raid; returns top bdev."""
    plan = stack.plan_local_stack(node)
    for aio in plan.aio_bdevs:
        _ensure_aio(rpc, aio)
    if plan.raid is not None:
        _ensure_raid(rpc, plan.raid)
    return plan.top_bdev


def _expose_repl_subsystem(rpc, cluster, node, top_bdev):
    """Every node exposes its local top on the internal replication listener."""
    nqn = stack.repl_nqn(cluster.nqn, node.uuid)
    _ensure_transport(rpc)
    _ensure_subsystem(rpc, nqn, serial=f"er{stack._short(node.uuid)}")
    if not _subsystem_has_ns(rpc, nqn, top_bdev):
        rpc.nvmf_subsystem_add_ns(nqn, top_bdev, nsid=1)
    if not _subsystem_has_listener(rpc, nqn, node.get_data_ip(), node.repl_port):
        rpc.listeners_create(nqn, "TCP", node.get_data_ip(), node.repl_port)


def _attach_remote_leg(primary_rpc, mirror: stack.MirrorPlan):
    if not primary_rpc.get_bdevs(name=mirror.remote_leg):
        primary_rpc.bdev_nvme_attach_controller(
            mirror.remote_controller, mirror.remote_nqn, mirror.remote_addr,
            mirror.remote_port, "tcp",
            ctrlr_loss_timeout_sec=-1,   # keep retrying: the peer WILL come back
            reconnect_delay_sec=2)


# ------------------------------------------------------------------- crypto

def _kms_connection(cluster):
    from simplyblock_core.kms import create_kms_connection
    return create_kms_connection(cluster)


def _ensure_crypto_stack(rpc, cluster, volume):
    """Register the volume's AES_XTS key (fetched from the KMS) and the
    crypto bdev over the lvol. Idempotent — used at create and at every
    republish (takeover/failback/restart), since SPDK-side key + bdev are
    runtime state."""
    kek = stack.cluster_kek_name(cluster.uuid)
    path = stack.volume_dek_path(cluster.uuid, volume.uuid)
    with _kms_connection(cluster) as kms:
        try:
            key1, key2 = kms.get_data_encryption_keys(path, kek)
        except Exception:
            kms.create_data_encryption_keys(path, kek)
            key1, key2 = kms.get_data_encryption_keys(path, kek)
    key_name = stack.crypto_key_name(volume.uuid)
    try:
        rpc.lvol_crypto_key_create(key_name, key1, key2)
    except RPCException as e:
        if 'exist' not in str(e.message).lower():
            raise
    if not rpc.get_bdevs(name=volume.crypto_bdev):
        rpc.lvol_crypto_create(volume.crypto_bdev, volume.lvol_bdev, key_name)


def _ns_bdev(volume) -> str:
    """The bdev the client namespace exposes: the crypto bdev when encryption
    is on, the raw lvol otherwise."""
    return volume.crypto_bdev if volume.crypto else volume.lvol_bdev


def _lvstore_host(nodes):
    """The node currently hosting the lvstore (lvstore_base set). The
    DESIGNATED primary is is_primary; after a takeover they differ until
    fail-back completes."""
    return next((n for n in nodes if n.lvstore_base
                 and n.status != EdgeNode.STATUS_REMOVED), None)


def add_edge_node(cluster_id, hostname, mgmt_ip, partitions, data_ip="",
                  deploy=True, rpc_wait_timeout=None) -> EdgeNode:
    """Add a node to an edge cluster (spec §5.2). Synchronous — bounded by the
    pod-start wait; API callers run it as a task/background call."""
    cluster = _require_edge_cluster(cluster_id)
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    if len(nodes) >= edge_constants.MAX_EDGE_NODES:
        raise ValueError(f"Edge clusters support at most {edge_constants.MAX_EDGE_NODES} nodes")
    if not partitions:
        raise ValueError("An edge node needs at least one free partition")
    if any(n.hostname == hostname for n in nodes):
        raise ValueError(f"Node {hostname} is already part of the cluster")

    primary = next((n for n in nodes if n.is_primary), None)
    if primary is not None and primary.lvstore_base:
        # 1->2 expansion under an existing lvstore needs raid1-insert-under or
        # a migration (spec §10) — reject explicitly rather than half-build.
        raise ValueError(
            "Cannot add a node: the cluster already has volumes/an lvstore on a "
            "single-node layout. Add both nodes before creating volumes.")

    node = EdgeNode()
    node.uuid = str(uuid_lib.uuid4())
    node.cluster_id = cluster_id
    node.hostname = hostname
    node.mgmt_ip = mgmt_ip
    node.data_ip = data_ip
    node.partitions = [EdgePartition({"device_path": path}) for path in partitions]
    node.is_primary = primary is None
    node.rpc_username = "edge"
    node.rpc_password = SecretStr(core_utils.generate_string(16))
    node.status = EdgeNode.STATUS_IN_CREATION
    node.write_to_db(db.kv_store())

    try:
        if deploy:
            k8s.deploy_spdk_pod(cluster, node, edge_constants.EDGE_SPDK_IMAGE,
                                edge_constants.EDGE_PROXY_IMAGE)
        rpc = node_rpc_client(node)
        _wait_for_rpc(rpc, timeout=rpc_wait_timeout or edge_constants.EDGE_RPC_WAIT_TIMEOUT_SEC)

        top_bdev = _build_local_stack(rpc, node)
        for i, part in enumerate(node.partitions):
            part.bdev_name = stack.aio_bdev_name(node.uuid, i)
        _expose_repl_subsystem(rpc, cluster, node, top_bdev)

        if primary is not None:
            # Second node: build the cross-node mirror + lvstore on the primary.
            mirror = stack.plan_mirror(cluster_id, cluster.nqn, primary, node)
            primary_rpc = node_rpc_client(primary)
            _attach_remote_leg(primary_rpc, mirror)
            _ensure_raid(primary_rpc, mirror.raid)
            primary_rpc.create_lvstore(stack.lvs_name(cluster_id), mirror.top_bdev,
                                       edge_constants.EDGE_LVS_CLUSTER_SZ, "unmap")

            def _set_lvstore(fresh):
                fresh.lvstore_base = mirror.top_bdev
                return True
            db.atomic_update(primary, _set_lvstore)
    except Exception:
        def _fail(fresh):
            fresh.status = EdgeNode.STATUS_OFFLINE
            return True
        db.atomic_update(node, _fail)
        raise

    def _online(fresh):
        fresh.partitions = node.partitions
        fresh.status = EdgeNode.STATUS_ONLINE
        fresh.online_since = str(datetime.datetime.now(datetime.timezone.utc))
        return True
    db.atomic_update(node, _online)
    node.status = EdgeNode.STATUS_ONLINE

    from simplyblock_edge.status import derive_cluster_status
    statuses = [n.status for n in db.get_edge_nodes(cluster_id)]
    set_cluster_status(db.get_cluster(cluster_id), derive_cluster_status(statuses),
                       caused_by=events_controller.CAUSED_BY_API)
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_OBJ_CREATED, node,
        events_controller.CAUSED_BY_API, f"Edge node added: {hostname}")
    return node


def shutdown_node(cluster_id, node_id):
    """Admin stop: delete the SPDK pod and pin the node DOWN — the monitor
    never auto-restarts a DOWN node (spec §5.4)."""
    cluster = _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)

    def _mutate(fresh):
        fresh.status = EdgeNode.STATUS_DOWN
        return True
    db.atomic_update(node, _mutate)
    k8s.delete_spdk_pod(cluster, node)
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, node,
        events_controller.CAUSED_BY_API, f"Edge node shut down: {node.hostname}")


def restart_node(cluster_id, node_id) -> str:
    """Admin restart: redeploy the pod if needed and enqueue reassembly."""
    cluster = _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    if node.status == EdgeNode.STATUS_DOWN:
        # Explicit restart is exactly the operator intervention DOWN waits for.
        k8s.deploy_spdk_pod(cluster, node, edge_constants.EDGE_SPDK_IMAGE,
                            edge_constants.EDGE_PROXY_IMAGE)

        def _mutate(fresh):
            fresh.status = EdgeNode.STATUS_OFFLINE
            return True
        db.atomic_update(node, _mutate)
    return add_edge_task(JobSchedule.FN_EDGE_NODE_RESTART, cluster_id, node_id,
                         max_retry=edge_constants.EDGE_NODE_RESTART_MAX_RETRY)


# -------------------------------------------------------------------- tasks

def add_edge_task(function_name, cluster_id, node_id, params=None, max_retry=-1) -> str:
    """Create a JobSchedule task, deduped per (function, node)."""
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    for task in db_controller.get_job_tasks(cluster_id):
        if (task.function_name == function_name and task.node_id == node_id
                and not task.canceled and task.status != JobSchedule.STATUS_DONE
                and task.function_params == (params or {})):
            logger.info(f"Task found, skip adding new task: {task.get_id()}")
            return task.uuid

    task = JobSchedule()
    task.uuid = str(uuid_lib.uuid4())
    task.cluster_id = cluster_id
    task.node_id = node_id
    task.date = int(time.time())
    task.function_name = function_name
    task.function_params = params or {}
    task.max_retry = max_retry
    task.status = JobSchedule.STATUS_NEW
    task.write_to_db(db.kv_store())
    return task.uuid


# ------------------------------------------------------------------ volumes

def _ensure_lvstore(cluster, nodes) -> EdgeNode:
    """Lazy lvstore creation (spec §5.2/§10): on the mirror when both nodes
    joined before the first volume, else directly on the single node's local
    top. Returns the node currently HOSTING the lvstore (the designated
    primary normally; the secondary between takeover and fail-back)."""
    host = _lvstore_host(nodes)
    if host is not None:
        return host

    primary = next((n for n in nodes if n.is_primary), None)
    if primary is None:
        raise ValueError("Edge cluster has no primary node")

    base = stack.lvstore_base_bdev(cluster.uuid, len(nodes), primary)
    rpc = node_rpc_client(primary)
    rpc.create_lvstore(stack.lvs_name(cluster.uuid), base,
                       edge_constants.EDGE_LVS_CLUSTER_SZ, "unmap")

    def _mutate(fresh):
        fresh.lvstore_base = base
        return True
    db.atomic_update(primary, _mutate)
    primary.lvstore_base = base
    return primary


def _publish_volume(rpc, node, cluster, volume, active):
    """Expose one volume's subsystem on a node. active=True publishes the
    namespace (the serving path); active=False keeps a namespace-less
    passive subsystem + listener, so clients hold a pre-established second
    path that lights up the moment a takeover adds the namespace."""
    _ensure_transport(rpc)
    _ensure_subsystem(rpc, volume.nqn, serial=f"ev{stack._short(volume.uuid)}")
    if not _subsystem_has_listener(rpc, volume.nqn, node.get_data_ip(), node.nvmf_port):
        rpc.listeners_create(volume.nqn, "TCP", node.get_data_ip(), node.nvmf_port)
    if active:
        if volume.crypto:
            _ensure_crypto_stack(rpc, cluster, volume)
        if not _subsystem_has_ns(rpc, volume.nqn, _ns_bdev(volume)):
            rpc.nvmf_subsystem_add_ns(volume.nqn, _ns_bdev(volume), nsid=volume.ns_id)


def create_volume(cluster_id, name, size, crypto=False) -> EdgeVolume:
    cluster = _require_edge_cluster(cluster_id)
    if db.get_edge_volume_by_name(cluster_id, name) is not None:
        raise ValueError(f"Volume with name {name} already exists")
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    if not nodes:
        raise ValueError("Edge cluster has no nodes")
    primary = _ensure_lvstore(cluster, nodes)
    if primary.status != EdgeNode.STATUS_ONLINE:
        raise ValueError(f"Primary node is {primary.status}, cannot create volume")

    volume = EdgeVolume()
    volume.uuid = str(uuid_lib.uuid4())
    volume.cluster_id = cluster_id
    volume.volume_name = name
    volume.size = size
    volume.lvol_bdev = stack.volume_bdev(cluster_id, name)
    volume.nqn = stack.volume_nqn(cluster.nqn, volume.uuid)
    volume.crypto = crypto
    volume.crypto_bdev = stack.crypto_bdev(volume.uuid) if crypto else ""

    rpc = node_rpc_client(primary)
    size_in_mib = size // (1024 * 1024)
    rpc.create_lvol(name, size_in_mib, stack.lvs_name(cluster_id))
    _publish_volume(rpc, primary, cluster, volume, active=True)

    # 2-node: pre-establish the passive path on the peer (spec: fail-over).
    for peer in nodes:
        if peer.uuid != primary.uuid and peer.status == EdgeNode.STATUS_ONLINE:
            _publish_volume(node_rpc_client(peer), peer, cluster, volume, active=False)

    volume.status = EdgeVolume.STATUS_ONLINE
    volume.write_to_db(db.kv_store())
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_OBJ_CREATED, volume,
        events_controller.CAUSED_BY_API,
        f"Edge volume created: {name}{' (encrypted)' if crypto else ''}")
    return volume


def delete_volume(cluster_id, volume_id):
    cluster = _require_edge_cluster(cluster_id)
    volume = db.get_edge_volume_by_id(cluster_id, volume_id)
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    host = _lvstore_host(nodes)
    if host is None:
        raise ValueError("Edge cluster has no lvstore host")

    def _mark(fresh):
        fresh.status = EdgeVolume.STATUS_IN_DELETION
        return True
    db.atomic_update(volume, _mark)

    # Tear the passive subsystem down on peers first, then the active side.
    for node in nodes:
        if node.uuid != host.uuid and node.status == EdgeNode.STATUS_ONLINE:
            try:
                node_rpc_client(node).subsystem_delete(volume.nqn)
            except RPCException:
                pass
    rpc = node_rpc_client(host)
    rpc.subsystem_delete(volume.nqn)
    if volume.crypto:
        try:
            rpc.lvol_crypto_delete(volume.crypto_bdev)
        except RPCException:
            pass
        with _kms_connection(cluster) as kms:
            kms.delete_data_encryption_keys(
                stack.volume_dek_path(cluster_id, volume.uuid))
    rpc.delete_lvol(volume.lvol_bdev)
    volume.remove(db.kv_store())
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_OBJ_DELETED, volume,
        events_controller.CAUSED_BY_API, f"Edge volume deleted: {volume.volume_name}")


def resize_volume(cluster_id, volume_id, new_size) -> EdgeVolume:
    _require_edge_cluster(cluster_id)
    volume = db.get_edge_volume_by_id(cluster_id, volume_id)
    if new_size <= volume.size:
        raise ValueError("New size must be larger than the current size")
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    host = _lvstore_host(nodes)
    if host is None:
        raise ValueError("Edge cluster has no lvstore host")
    node_rpc_client(host).bdev_lvol_resize(volume.lvol_bdev, new_size // (1024 * 1024))

    def _mutate(fresh):
        fresh.size = new_size
        return True
    db.atomic_update(volume, _mutate)
    volume.size = new_size
    return volume


def get_connect_info(cluster_id, volume_id) -> list:
    """One connect entry per node holding a listener for the volume — the
    lvstore host serves; peers are passive paths that activate on takeover.
    Clients connect ALL entries (spec: 2-node IO survives a node loss)."""
    _require_edge_cluster(cluster_id)
    volume = db.get_edge_volume_by_id(cluster_id, volume_id)
    nodes = [n for n in db.get_edge_nodes(cluster_id)
             if n.status != EdgeNode.STATUS_REMOVED]
    host = _lvstore_host(nodes)
    if host is None:
        raise ValueError("Edge cluster has no lvstore host")
    ordered = [host] + [n for n in nodes if n.uuid != host.uuid]
    return [{
        "transport": "tcp",
        "ip": node.get_data_ip(),
        "port": node.nvmf_port,
        "nqn": volume.nqn,
        "active": node.uuid == host.uuid,
        "reconnect-delay": core_constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
        "ctrl-loss-tmo": core_constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
        "nr-io-queues": 2,
    } for node in ordered]


# ------------------------------------------------------------------ devices

def replace_device(cluster_id, node_id, old_path, new_path) -> str:
    _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    part = next((p for p in node.partitions if p.device_path == old_path
                 and p.status != EdgePartition.STATUS_REMOVED), None)
    if part is None:
        raise ValueError(f"Partition {old_path} not found on node {node_id}")
    active = [p for p in node.partitions if p.status != EdgePartition.STATUS_REMOVED]
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    if len(active) < 2 and len(nodes) < 2:
        raise ValueError(
            "Cannot replace the only partition of a single-node cluster - "
            "there is no redundancy to rebuild from")

    def _mutate(fresh):
        for p in fresh.partitions:
            if p.device_path == old_path:
                p.status = EdgePartition.STATUS_FAILED
        return True
    db.atomic_update(node, _mutate)
    return add_edge_task(JobSchedule.FN_EDGE_DEVICE_REPLACE, cluster_id, node_id,
                         params={"old_path": old_path, "new_path": new_path},
                         max_retry=5)


def add_device(cluster_id, node_id, device_path) -> str:
    _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    active = [p for p in node.partitions if p.status != EdgePartition.STATUS_REMOVED]
    if len(active) < 3:
        raise ValueError(
            "Adding a device is only supported under a raid5 local stack "
            "(3+ partitions)")
    if any(p.device_path == device_path for p in active):
        raise ValueError(f"Partition {device_path} is already part of the node")

    def _mutate(fresh):
        fresh.partitions = fresh.partitions + [
            EdgePartition({"device_path": device_path,
                           "status": EdgePartition.STATUS_NEW})]
        return True
    db.atomic_update(node, _mutate)
    return add_edge_task(JobSchedule.FN_EDGE_DEVICE_ADD, cluster_id, node_id,
                         params={"device_path": device_path}, max_retry=3)


def _partition_or_raise(node, device_path):
    part = next((p for p in node.partitions if p.device_path == device_path
                 and p.status != EdgePartition.STATUS_REMOVED), None)
    if part is None:
        raise ValueError(f"Partition {device_path} not found on node {node.get_id()}")
    return part


def _require_redundancy(cluster_id, node, device_path):
    """A device may only be taken out when the data survives it: either the
    local stack is raid (>=2 partitions) or a 2-node mirror covers the node."""
    active = [p for p in node.partitions
              if p.status not in (EdgePartition.STATUS_REMOVED,)]
    nodes = [n for n in db.get_edge_nodes(cluster_id) if n.status != EdgeNode.STATUS_REMOVED]
    if len(active) < 2 and len(nodes) < 2:
        raise ValueError(
            f"Cannot take {device_path} out: single-partition single-node "
            "cluster has no redundancy")


def remove_device(cluster_id, node_id, device_path):
    """Graceful device removal (spec §5.5): drop the raid member and the aio
    bdev; IO continues on raid redundancy. The partition goes OFFLINE and can
    be brought back with restart_device."""
    _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    part = _partition_or_raise(node, device_path)
    if part.status == EdgePartition.STATUS_OFFLINE:
        return
    _require_redundancy(cluster_id, node, device_path)

    index = node.partitions.index(part)
    bdev = stack.aio_bdev_name(node.uuid, index)
    rpc = node_rpc_client(node)
    try:
        rpc.bdev_raid_remove_base_bdev(bdev)
    except RPCException:
        pass  # not a raid member (bare-aio node covered by the mirror)
    try:
        rpc.bdev_aio_delete(bdev)
    except RPCException:
        pass  # already gone

    def _mutate(fresh):
        for p in fresh.partitions:
            if p.device_path == device_path:
                p.status = EdgePartition.STATUS_OFFLINE
        return True
    db.atomic_update(node, _mutate)
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, node,
        events_controller.CAUSED_BY_API,
        f"Edge device removed (offline): {device_path} on {node.hostname}")


def restart_device(cluster_id, node_id, device_path):
    """Bring an OFFLINE/UNAVAILABLE/FAILED device back: recreate the aio bdev
    and re-add it to the local raid — SPDK rebuilds the member."""
    _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    part = _partition_or_raise(node, device_path)
    if part.status == EdgePartition.STATUS_ONLINE:
        return
    if part.status not in (EdgePartition.STATUS_OFFLINE,
                           EdgePartition.STATUS_UNAVAILABLE,
                           EdgePartition.STATUS_FAILED):
        raise ValueError(f"Device {device_path} is {part.status}, cannot restart")

    index = node.partitions.index(part)
    bdev = stack.aio_bdev_name(node.uuid, index)
    plan = stack.plan_local_stack(node)
    rpc = node_rpc_client(node)
    if not rpc.get_bdevs(name=bdev):
        rpc.bdev_aio_create(bdev, device_path)
    if plan.raid is not None:
        try:
            rpc.bdev_raid_add_base_bdev(plan.raid.name, bdev)
        except RPCException as e:
            if 'already' not in str(e.message).lower():
                raise

    def _mutate(fresh):
        for p in fresh.partitions:
            if p.device_path == device_path:
                p.status = EdgePartition.STATUS_ONLINE
                p.bdev_name = bdev
        return True
    db.atomic_update(node, _mutate)
    events_controller.log_event_cluster(
        cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, node,
        events_controller.CAUSED_BY_API,
        f"Edge device restarted: {device_path} on {node.hostname}")


# ------------------------------------------------------------ task handlers
# Called by services/tasks_runner_edge.py; return simplyblock_lib TaskResult.

def _volumes_of(cluster_id):
    return [v for v in db.get_edge_volumes(cluster_id)
            if v.status != EdgeVolume.STATUS_IN_DELETION]


def _republish_volumes(rpc, host, cluster):
    """(Re)expose every volume ACTIVELY on the lvstore host."""
    for volume in _volumes_of(host.cluster_id):
        _publish_volume(rpc, host, cluster, volume, active=True)


def _publish_passive_paths(rpc, node, cluster):
    """(Re)expose every volume's namespace-less passive path on a peer."""
    for volume in _volumes_of(node.cluster_id):
        _publish_volume(rpc, node, cluster, volume, active=False)


def _wait_raid_synced(rpc, raid_name,
                      timeout=edge_constants.EDGE_RESYNC_TIMEOUT_SEC,
                      interval=5, sleep=time.sleep, monotonic=time.monotonic):
    """Block until the mirror has both legs and no rebuild in flight.

    Fork gate (spec §10): the exact rebuild-progress fields of
    bdev_raid_get_bdevs are fork-specific; this treats "2 base bdevs present
    and no process/rebuilding marker" as synced.
    """
    deadline = monotonic() + timeout
    while True:
        entry = next((r for r in (rpc.bdev_raid_get_bdevs() or [])
                      if r.get('name') == raid_name), None)
        if entry is not None:
            members = entry.get('base_bdevs_list') or []
            rebuilding = bool(entry.get('process')) or any(
                isinstance(m, dict) and m.get('is_rebuilding') for m in members)
            if len(members) >= 2 and not rebuilding:
                return
        if monotonic() >= deadline:
            raise TimeoutError(f"raid {raid_name} did not resync in {timeout}s")
        sleep(interval)


def _reassemble_node(cluster, node, nodes) -> None:
    """Idempotently rebuild a node's stack after a pod restart (spec §5.6)."""
    rpc = node_rpc_client(node)
    top_bdev = _build_local_stack(rpc, node)
    _expose_repl_subsystem(rpc, cluster, node, top_bdev)

    peers = [n for n in nodes if n.uuid != node.uuid
             and n.status != EdgeNode.STATUS_REMOVED]
    host = _lvstore_host(nodes)

    if not peers:
        # Single node: reload the lvstore and republish the volumes.
        if node.lvstore_base:
            rpc.bdev_examine(node.lvstore_base)
            _republish_volumes(rpc, node, cluster)
        return

    peer = peers[0]
    if host is not None and host.uuid == node.uuid:
        # The returning node still hosts the lvstore: reattach the remote
        # leg, reassemble the mirror, reload, republish actively.
        mirror = stack.plan_mirror(cluster.uuid, cluster.nqn, node, peer)
        _attach_remote_leg(rpc, mirror)
        _ensure_raid(rpc, mirror.raid)
        rpc.bdev_examine(mirror.top_bdev)
        _republish_volumes(rpc, node, cluster)
    elif host is not None:
        # A peer hosts the lvstore (normal secondary restart, or a failed-over
        # primary coming back): re-add this node's leg into the HOST's mirror
        # (SPDK rebuild) and restore the passive client paths here. Fail-back,
        # if due, happens in handle_node_restart_task after the resync.
        mirror = stack.plan_mirror(cluster.uuid, cluster.nqn, host, node)
        host_rpc = node_rpc_client(host)
        _attach_remote_leg(host_rpc, mirror)
        try:
            host_rpc.bdev_raid_add_base_bdev(mirror.raid.name, mirror.remote_leg)
        except RPCException as e:
            # Already a member (the nvme controller auto-reconnected and the
            # raid never dropped the leg) is fine; anything else is not.
            if 'already' not in str(e.message).lower():
                raise
        _publish_passive_paths(rpc, node, cluster)
    # else: no lvstore anywhere yet — the local stack is all there is.


def _fail_back(cluster, primary, secondary):
    """Move the lvstore back from the secondary to the designated primary
    (spec: fail-back on node restart). Preconditions: the primary's stack is
    rebuilt and its leg re-added to the secondary-hosted mirror.

    Sequence: wait for resync -> withdraw the namespaces on the secondary
    (clients flip to path-down on that leg; the passive primary path is about
    to activate) -> release the mirror on the secondary (superblock stays on
    the legs) -> assemble mirror + lvstore on the primary -> republish
    actively there, passively on the secondary -> flip lvstore_base records.
    """
    mirror_bdev = stack.mirror_name(cluster.uuid)
    secondary_rpc = node_rpc_client(secondary)
    _wait_raid_synced(secondary_rpc, mirror_bdev)

    for volume in _volumes_of(cluster.uuid):
        try:
            secondary_rpc.nvmf_subsystem_remove_ns(volume.nqn, volume.ns_id)
        except RPCException:
            pass
        if volume.crypto:
            try:
                secondary_rpc.lvol_crypto_delete(volume.crypto_bdev)
            except RPCException:
                pass
    try:
        secondary_rpc.bdev_raid_delete(mirror_bdev)
    except RPCException:
        pass

    primary_rpc = node_rpc_client(primary)
    mirror = stack.plan_mirror(cluster.uuid, cluster.nqn, primary, secondary)
    _attach_remote_leg(primary_rpc, mirror)
    primary_rpc.bdev_examine(stack.plan_local_stack(primary).top_bdev)
    if not primary_rpc.get_bdevs(name=mirror_bdev):
        # Fork gate (spec §10): superblock examine should reassemble; fall
        # back to explicit re-creation over the synced legs.
        _ensure_raid(primary_rpc, mirror.raid)
        primary_rpc.bdev_examine(mirror_bdev)
    _republish_volumes(primary_rpc, primary, cluster)
    _publish_passive_paths(secondary_rpc, secondary, cluster)

    def _set_primary(fresh):
        fresh.lvstore_base = mirror_bdev
        return True
    db.atomic_update(primary, _set_primary)
    primary.lvstore_base = mirror_bdev

    def _clear_secondary(fresh):
        fresh.lvstore_base = ""
        return True
    db.atomic_update(secondary, _clear_secondary)
    secondary.lvstore_base = ""

    events_controller.log_event_cluster(
        cluster.uuid, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, primary,
        events_controller.CAUSED_BY_MONITOR,
        f"Edge lvstore failed back to primary {primary.hostname}")


def handle_node_restart_task(task) -> TaskResult:
    cluster = db.get_cluster(task.cluster_id)
    try:
        node = db.get_edge_node_by_id(task.cluster_id, task.node_id)
    except KeyError:
        return TaskResult.done("node not found")
    if node.status == EdgeNode.STATUS_DOWN:
        return TaskResult.done("node is down (deliberate stop) - not restarting")
    if node.status == EdgeNode.STATUS_REMOVED:
        return TaskResult.done("node is removed")

    def _restarting(fresh):
        if fresh.status in (EdgeNode.STATUS_DOWN, EdgeNode.STATUS_REMOVED):
            return False
        fresh.status = EdgeNode.STATUS_RESTARTING
        return True
    db.atomic_update(node, _restarting)
    node.status = EdgeNode.STATUS_RESTARTING

    nodes = db.get_edge_nodes(task.cluster_id)
    try:
        _reassemble_node(cluster, node, nodes)
        # Fail-back: the designated primary returns while the secondary hosts
        # the lvstore (a takeover happened). Its mirror leg was just re-added
        # above; once resynced, move the lvstore home.
        host = _lvstore_host(nodes)
        if node.is_primary and host is not None and host.uuid != node.uuid:
            _fail_back(cluster, node, host)
    except Exception as e:
        logger.error(f"Edge node reassembly failed for {node.get_id()}: {e}")

        def _back_offline(fresh):
            if fresh.status == EdgeNode.STATUS_RESTARTING:
                fresh.status = EdgeNode.STATUS_OFFLINE
                return True
            return False
        db.atomic_update(node, _back_offline)
        return TaskResult.retry(f"reassembly failed: {e}")

    def _online(fresh):
        fresh.status = EdgeNode.STATUS_ONLINE
        fresh.online_since = str(datetime.datetime.now(datetime.timezone.utc))
        return True
    db.atomic_update(node, _online)
    events_controller.log_event_cluster(
        task.cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, node,
        events_controller.CAUSED_BY_MONITOR, f"Edge node back online: {node.hostname}")
    return TaskResult.done("node reassembled and online")


def handle_failover_task(task) -> TaskResult:
    """Move the lvstore to the surviving secondary (task.node_id) after the
    designated primary stopped serving (spec: fail-over). The mirror's
    on-disk superblock lets the secondary assemble it degraded from its own
    leg; the passive client paths then activate by adding the namespaces."""
    cluster = db.get_cluster(task.cluster_id)
    try:
        secondary = db.get_edge_node_by_id(task.cluster_id, task.node_id)
    except KeyError:
        return TaskResult.done("node not found")
    nodes = [n for n in db.get_edge_nodes(task.cluster_id)
             if n.status != EdgeNode.STATUS_REMOVED]
    host = _lvstore_host(nodes)
    if host is not None and host.uuid == secondary.uuid:
        return TaskResult.done("secondary already hosts the lvstore")
    primary = next((n for n in nodes if n.is_primary), None)
    if primary is not None and primary.status == EdgeNode.STATUS_ONLINE:
        return TaskResult.done("primary recovered before takeover — nothing to do")
    if secondary.status != EdgeNode.STATUS_ONLINE:
        return TaskResult.retry(f"secondary is {secondary.status}, cannot take over")

    mirror_bdev = stack.mirror_name(cluster.uuid)
    try:
        rpc = node_rpc_client(secondary)
        top_bdev = _build_local_stack(rpc, secondary)
        rpc.bdev_examine(top_bdev)
        if not rpc.get_bdevs(name=mirror_bdev):
            # Fork gate (spec §10): examine of a superblocked leg should
            # assemble the mirror degraded; fall back to explicit single-leg
            # creation if the fork requires it.
            rpc.bdev_raid_create(mirror_bdev, [top_bdev], raid_level="1",
                                 superblock=True)
            rpc.bdev_examine(mirror_bdev)
        _republish_volumes(rpc, secondary, cluster)
    except Exception as e:
        return TaskResult.retry(f"takeover failed: {e}")

    def _set_host(fresh):
        fresh.lvstore_base = mirror_bdev
        return True
    db.atomic_update(secondary, _set_host)
    if primary is not None:
        def _clear_old(fresh):
            fresh.lvstore_base = ""
            return True
        db.atomic_update(primary, _clear_old)

    events_controller.log_event_cluster(
        task.cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, secondary,
        events_controller.CAUSED_BY_MONITOR,
        f"Edge lvstore failed over to {secondary.hostname}")
    return TaskResult.done(f"lvstore now hosted on {secondary.hostname}")


def handle_device_replace_task(task) -> TaskResult:
    old_path = task.function_params["old_path"]
    new_path = task.function_params["new_path"]
    try:
        node = db.get_edge_node_by_id(task.cluster_id, task.node_id)
    except KeyError:
        return TaskResult.done("node not found")

    index = next((i for i, p in enumerate(node.partitions)
                  if p.device_path == old_path), None)
    if index is None:
        return TaskResult.done(f"partition {old_path} not found")

    plan = stack.plan_local_stack(node)
    old_bdev = stack.aio_bdev_name(node.uuid, index)
    rpc = node_rpc_client(node)
    try:
        if plan.raid is None:
            return TaskResult.done(
                "partition is not a raid member - replace not applicable")
        if rpc.get_bdevs(name=old_bdev):
            try:
                rpc.bdev_raid_remove_base_bdev(old_bdev)
            except RPCException:
                pass  # already removed / raid already degraded past it
            rpc.bdev_aio_delete(old_bdev)
        rpc.bdev_aio_create(old_bdev, new_path)
        rpc.bdev_raid_add_base_bdev(plan.raid.name, old_bdev)
    except Exception as e:
        return TaskResult.retry(f"device replace failed: {e}")

    def _mutate(fresh):
        for p in fresh.partitions:
            if p.device_path == old_path:
                p.device_path = new_path
                p.status = EdgePartition.STATUS_ONLINE
                p.bdev_name = old_bdev
        return True
    db.atomic_update(node, _mutate)
    return TaskResult.done(f"replaced {old_path} with {new_path}; raid rebuilding")


def handle_device_add_task(task) -> TaskResult:
    device_path = task.function_params["device_path"]
    try:
        node = db.get_edge_node_by_id(task.cluster_id, task.node_id)
    except KeyError:
        return TaskResult.done("node not found")

    index = next((i for i, p in enumerate(node.partitions)
                  if p.device_path == device_path), None)
    if index is None:
        return TaskResult.done(f"partition {device_path} not found")

    plan = stack.plan_local_stack(node)
    if plan.raid is None or plan.raid.raid_level != "5f":
        return TaskResult.done("device add is only supported under raid5")

    bdev = stack.aio_bdev_name(node.uuid, index)
    rpc = node_rpc_client(node)
    try:
        if not rpc.get_bdevs(name=bdev):
            rpc.bdev_aio_create(bdev, device_path)
        # Fork-capability gate (spec §10.1): upstream raid5f cannot grow; the
        # fork's error is surfaced verbatim if unsupported.
        rpc.bdev_raid_add_base_bdev(plan.raid.name, bdev)
    except Exception as e:
        return TaskResult.retry(f"device add failed: {e}")

    def _mutate(fresh):
        for p in fresh.partitions:
            if p.device_path == device_path:
                p.status = EdgePartition.STATUS_ONLINE
                p.bdev_name = bdev
        return True
    db.atomic_update(node, _mutate)
    return TaskResult.done(f"device {device_path} added under {plan.raid.name}")
