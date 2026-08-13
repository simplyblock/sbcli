# coding=utf-8
"""Edge-cluster control flows (docs/edge_clusters_spec.md §5, v3 product
adoption).

2-node clusters run ACTIVE/ACTIVE with the spdk-fork's primary/secondary
lvstore processing: each node owns a store (lvstore over a superblocked
raid1 mirror of split halves from both nodes), runs a live SECONDARY
instance of the peer's store (creations registered via bdev_lvol_register*,
refreshed via bdev_lvol_update_lvstore), and every volume namespace exists
on both nodes with ANA optimized (leader path) / non-optimized listeners.
Fail-over promotes the survivor's secondary instance (update + set_leader +
ANA flip); fail-back fences the store's client port (nvmf_port_block),
hands leadership home after resync, and unfences.

Everything long-running or retryable is a JobSchedule task processed by
services/tasks_runner_edge.py. RPC and k8s access go through
simplyblock_edge.rpc / .k8s so tests can substitute them.
"""
import datetime
import logging
import time
import uuid as uuid_lib

from pydantic import SecretStr

from tenacity import (RetryError, Retrying, before_sleep_log,
                      retry_if_exception_type, retry_if_result,
                      stop_after_delay, wait_fixed)

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


# ---------------------------------------------------------------- rpc utils

def _wait_for_rpc(rpc, timeout=edge_constants.EDGE_RPC_WAIT_TIMEOUT_SEC,
                  interval=edge_constants.EDGE_RPC_WAIT_INTERVAL_SEC):
    """Block until the node's SPDK proxy answers (pod start)."""
    try:
        Retrying(
            stop=stop_after_delay(timeout),
            wait=wait_fixed(interval),
            retry=retry_if_result(lambda answered: not answered)
                  | retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
        )(lambda: bool(rpc.get_version()))
    except RetryError as e:
        raise TimeoutError(
            f"SPDK RPC did not come up within {timeout}s") from e


def _ensure_aio(rpc, spec: stack.AioSpec):
    if not rpc.get_bdevs(name=spec.bdev_name):
        rpc.bdev_aio_create(spec.bdev_name, spec.device_path, spec.block_size)


def _ensure_raid(rpc, spec: stack.RaidSpec):
    if not rpc.get_bdevs(name=spec.name):
        rpc.bdev_raid_create(spec.name, spec.base_bdevs, raid_level=spec.raid_level,
                             strip_size_kb=spec.strip_size_kb or 4,
                             superblock=spec.superblock)


def _ensure_split(rpc, plan: stack.LocalStackPlan):
    if plan.split and not rpc.get_bdevs(name=plan.own_half):
        rpc.bdev_split(plan.top_bdev, 2)


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


def _build_local_stack(rpc, node, split) -> stack.LocalStackPlan:
    """Idempotently create the node's aio bdevs + local raid (+ split)."""
    plan = stack.plan_local_stack(node, split=split)
    for aio in plan.aio_bdevs:
        _ensure_aio(rpc, aio)
    if plan.raid is not None:
        _ensure_raid(rpc, plan.raid)
    _ensure_split(rpc, plan)
    return plan


def _expose_repl_subsystem(rpc, cluster, node, plan: stack.LocalStackPlan):
    """Export the node's halves for the peer: ns1 = own half (the peer's
    SECONDARY instance of this node's store reads it), ns2 = peer half (leg
    of the peer's own store)."""
    nqn = stack.repl_nqn(cluster.nqn, node.uuid)
    _ensure_transport(rpc)
    _ensure_subsystem(rpc, nqn, serial=f"er{stack._short(node.uuid)}")
    if plan.split:
        if not _subsystem_has_ns(rpc, nqn, plan.own_half):
            rpc.nvmf_subsystem_add_ns(nqn, plan.own_half, nsid=1)
        if not _subsystem_has_ns(rpc, nqn, plan.peer_half):
            rpc.nvmf_subsystem_add_ns(nqn, plan.peer_half, nsid=2)
    if not _subsystem_has_listener(rpc, nqn, node.get_data_ip(), node.repl_port):
        rpc.listeners_create(nqn, "TCP", node.get_data_ip(), node.repl_port)


def _attach_peer(rpc, cluster, peer):
    """Attach the peer's repl subsystem -> er_{peer}n1 / er_{peer}n2."""
    if not rpc.get_bdevs(name=stack.remote_half_bdev(peer.uuid, 1)):
        rpc.bdev_nvme_attach_controller(
            stack.remote_controller_name(peer.uuid),
            stack.repl_nqn(cluster.nqn, peer.uuid),
            peer.get_data_ip(), peer.repl_port, "tcp",
            ctrlr_loss_timeout_sec=-1,   # keep retrying: the peer WILL come back
            reconnect_delay_sec=2)


def _instantiate_store(rpc, node, store_plan: stack.StorePlan, create_lvstore=False):
    """Bring up this node's instance of a store: mirror (examine-first, since
    the superblock is authoritative; explicit create as first-time/fallback)
    plus the lvstore itself — created fresh (owner, first time), or loaded by
    the examine with its metadata-persisted role."""
    rpc.bdev_examine(store_plan.mirror.base_bdevs[0])
    if not rpc.get_bdevs(name=store_plan.mirror.name):
        _ensure_raid(rpc, store_plan.mirror)
        rpc.bdev_examine(store_plan.mirror.name)
    if create_lvstore:
        rpc.create_lvstore(store_plan.lvs, store_plan.mirror.name,
                           edge_constants.EDGE_LVS_CLUSTER_SZ, "unmap")
    rpc.bdev_lvol_set_lvs_opts(store_plan.lvs, groupid=node.store_index,
                               subsystem_port=store_plan.client_port,
                               role=store_plan.role)
    if store_plan.role == "primary" and create_lvstore:
        rpc.bdev_lvol_set_leader(store_plan.lvs, leader=True)


# ------------------------------------------------------------------- crypto

def _kms_connection(cluster):
    from simplyblock_core.kms import create_kms_connection
    return create_kms_connection(cluster)


def _ensure_crypto_stack(rpc, cluster, volume):
    """Register the volume's AES_XTS key (fetched from the KMS) and the
    crypto bdev over the lvol. Idempotent, and executed on BOTH nodes — the
    secondary's lvol bdev exists via registration, so the crypto bdev (and
    with it the non-optimized path) is fully formed there too."""
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
    return volume.crypto_bdev if volume.crypto else volume.lvol_bdev


# -------------------------------------------------------------------- nodes

def _init_spdk_framework(rpc, node):
    """Hand the core parameters to the just-started SPDK app, in ORDER.

    The pod's entrypoint (run_distr_with_ssd.sh) starts the fork target with
    --wait-for-rpc, and the image's adjust_cpu_mask.sh remaps our identity
    l_cores map onto the cpuset the kubelet actually granted — so the CP does
    NOT know the final core ids at render time. Masks therefore cannot travel
    as env or render-time values (a first version did exactly that and the
    image ignored it). Instead:

      1. framework_start_init  — finish app startup (idempotent: a re-entered
         add/restart flow on an already-initialized process skips ahead).
      2. framework_get_reactors — learn the ACTUAL reactor lcores.
      3. bdev_lvol_create_poller_group(<lvs core mask>) — the fork requires
         this exactly ONCE per process lifetime before any lvstore work; the
         mask is built from the real reactor list per the deploy-time layout.

    nvmf poll-group masks (spdk_cpus >= 4, spec §7) are deliberately not set
    yet: nvmf_set_config must happen PRE-init where the real core ids are
    unknowable — needs a fork-side relative-mask option (spec §10 note).
    """
    fresh = True
    try:
        rpc.framework_start_init()
    except RPCException as e:
        if 'already' not in str(e.message).lower():
            raise
        fresh = False

    if not fresh:
        return

    layout = stack.plan_cpu_layout(node.spdk_cpus)
    reactors = rpc.framework_get_reactors() or {}
    lcores = sorted(r.get('lcore', 0) for r in reactors.get('reactors', []))
    if not lcores:
        lcores = list(range(node.spdk_cpus))
    lvs_mask = 0
    for i, lcore in enumerate(lcores):
        if layout.lvs_mask >> i & 1:
            lvs_mask |= 1 << lcore
    try:
        rpc.bdev_lvol_create_poller_group(stack.CpuLayout.hex(lvs_mask or 1 << lcores[0]))
    except RPCException as e:
        if 'exist' not in str(e.message).lower():
            raise


def check_node_admission(cluster_id, hostname):
    """Admission preconditions for adding `hostname`, shared by the API
    endpoint (fast 400) and add_edge_node (authoritative) — the two MUST
    agree. When the endpoint kept its own copy with the old semantics, the
    retry path was unreachable: a failed add left an offline record and the
    API 400ed "already part of the cluster" before ops could reclaim it
    (live run 2026-08-13).

    RETRY SEMANTICS. A node add that fails part-way leaves its record behind
    (offline, with status_reason). Counting those toward the node limit made
    a failed deploy UNRETRYABLE (observed 2026-08-11: "at most 2 nodes" on a
    1-node cluster). A record for the same hostname that never came online is
    the SAME node retrying: it doesn't count against the limit, doesn't
    trigger the duplicate check, and is reclaimed by the new attempt.

    Returns (established, retryable); raises ValueError when inadmissible.
    """
    all_nodes = [n for n in db.get_edge_nodes(cluster_id)
                 if n.status != EdgeNode.STATUS_REMOVED]
    retryable = [n for n in all_nodes
                 if n.hostname == hostname and not n.online_since]
    established = [n for n in all_nodes if n not in retryable]

    if len(established) >= edge_constants.MAX_EDGE_NODES:
        raise ValueError(f"Edge clusters support at most {edge_constants.MAX_EDGE_NODES} nodes")
    if any(n.hostname == hostname for n in established):
        raise ValueError(f"Node {hostname} is already part of the cluster")
    first = established[0] if established else None
    if first is not None and first.lvstore_base and not retryable:
        # A 1-node cluster with volumes has its lvstore directly on the local
        # top (unsplit) — going active/active needs a migration (spec §10).
        # Guarded to FRESH adds: a retry of a failed second-node add must
        # pass even though the aborted active/active formation may already
        # have stamped the first node's lvstore_base (the mirror base).
        raise ValueError(
            "Cannot add a node: the cluster already has volumes/an lvstore on a "
            "single-node layout. Add both nodes before creating volumes.")
    return established, retryable


def add_edge_node(cluster_id, hostname, mgmt_ip, partitions, data_ip="",
                  spdk_cpus=None, deploy=True, rpc_wait_timeout=None) -> EdgeNode:
    """Add a node to an edge cluster (spec §5.2). Synchronous — bounded by the
    pod-start wait; API callers run it as a task/background call."""
    cluster = _require_edge_cluster(cluster_id)
    if not partitions:
        raise ValueError("An edge node needs at least one free partition")

    established, retryable = check_node_admission(cluster_id, hostname)

    # Drop stale attempts for this hostname so the retry starts clean.
    for stale in retryable:
        stale.remove(db.kv_store())

    nodes = established
    first = nodes[0] if nodes else None

    node = EdgeNode()
    node.uuid = str(uuid_lib.uuid4())
    node.cluster_id = cluster_id
    node.hostname = hostname
    node.mgmt_ip = mgmt_ip
    node.data_ip = data_ip
    node.partitions = [EdgePartition({"device_path": path}) for path in partitions]
    node.is_primary = first is None
    node.spdk_cpus = spdk_cpus or edge_constants.EDGE_POD_CPU
    stack.plan_cpu_layout(node.spdk_cpus)  # validate 1..6 before any side effect
    node.rpc_username = "edge"
    node.rpc_password = SecretStr(core_utils.generate_string(16))
    node.status = EdgeNode.STATUS_IN_CREATION
    node.write_to_db(db.kv_store())

    try:
        if deploy:
            if edge_constants.EDGE_CPU_TOPOLOGY_ENABLED:
                # Same node-preparation Job the central clusters run.
                k8s.deploy_cpu_topology_job(cluster, node)
            k8s.deploy_spdk_pod(cluster, node, edge_constants.EDGE_SPDK_IMAGE,
                                edge_constants.EDGE_PROXY_IMAGE)
        rpc = node_rpc_client(node)
        _wait_for_rpc(rpc, timeout=rpc_wait_timeout or edge_constants.EDGE_RPC_WAIT_TIMEOUT_SEC)

        _init_spdk_framework(rpc, node)
        two_node = first is not None
        plan = _build_local_stack(rpc, node, split=two_node)
        for i, part in enumerate(node.partitions):
            part.bdev_name = stack.aio_bdev_name(node.uuid, i)
        _expose_repl_subsystem(rpc, cluster, node, plan)

        if two_node:
            _form_active_active(cluster, first, node)
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        logger.exception("Edge node add failed for %s: %s", hostname, reason)

        def _fail(fresh):
            fresh.status = EdgeNode.STATUS_OFFLINE
            fresh.status_reason = reason[:500]
            return True
        db.atomic_update(node, _fail)
        raise

    def _online(fresh):
        fresh.partitions = node.partitions
        fresh.status = EdgeNode.STATUS_ONLINE
        fresh.status_reason = ""
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


def _form_active_active(cluster, node_a, node_b):
    """Second-node join: re-split node_a's stack, cross-attach, create both
    stores (primary on the owner, live secondary instance on the peer)."""
    rpc_a = node_rpc_client(node_a)
    rpc_b = node_rpc_client(node_b)

    plan_a = _build_local_stack(rpc_a, node_a, split=True)
    _expose_repl_subsystem(rpc_a, cluster, node_a, plan_a)
    _attach_peer(rpc_a, cluster, node_b)
    _attach_peer(rpc_b, cluster, node_a)

    for owner, peer in ((node_a, node_b), (node_b, node_a)):
        owner_rpc = node_rpc_client(owner)
        peer_rpc = node_rpc_client(peer)
        own_plan = stack.plan_store(owner, owner, peer,
                                    owner.nvmf_port, owner.store_index)
        sec_plan = stack.plan_store(peer, owner, peer,
                                    owner.nvmf_port, owner.store_index)
        _instantiate_store(owner_rpc, owner, own_plan, create_lvstore=True)
        _instantiate_store(peer_rpc, peer, sec_plan, create_lvstore=False)
        peer_rpc.bdev_lvol_update_lvstore(own_plan.lvs)

        def _set_store(fresh, mirror=own_plan.mirror.name, lvs=own_plan.lvs):
            fresh.lvstore_base = mirror
            fresh.leader_of = [lvs]
            return True
        db.atomic_update(owner, _set_store)
        owner.lvstore_base = own_plan.mirror.name
        owner.leader_of = [own_plan.lvs]


def shutdown_node(cluster_id, node_id):
    """Admin stop: delete the SPDK pod and pin the node DOWN — the monitor
    never auto-restarts a DOWN node (spec §5.4). Fail-over of its store to
    the peer is still enqueued by the monitor (availability wins)."""
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
    """Create a JobSchedule task, deduped per (function, node, params)."""
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

def _active_nodes(cluster_id):
    return [n for n in db.get_edge_nodes(cluster_id)
            if n.status != EdgeNode.STATUS_REMOVED]


def _leader_node(nodes, lvs) -> EdgeNode:
    leader = next((n for n in nodes if lvs in n.leader_of), None)
    if leader is None:
        raise ValueError(f"No node leads {lvs}")
    return leader


def _volumes_of(cluster_id):
    return [v for v in db.get_edge_volumes(cluster_id)
            if v.status != EdgeVolume.STATUS_IN_DELETION]


def _ensure_single_node_lvstore(cluster, node) -> None:
    if node.lvstore_base:
        return
    base = stack.single_node_lvs_base(node)
    rpc = node_rpc_client(node)
    rpc.create_lvstore(stack.lvs_name(node.uuid), base,
                       edge_constants.EDGE_LVS_CLUSTER_SZ, "unmap")
    lvs = stack.lvs_name(node.uuid)

    def _mutate(fresh):
        fresh.lvstore_base = base
        fresh.leader_of = [lvs]
        return True
    db.atomic_update(node, _mutate)
    node.lvstore_base = base
    node.leader_of = [lvs]


def _pick_home(cluster_id, nodes) -> EdgeNode:
    """Placement: the ONLINE store owner with the fewest homed volumes."""
    counts = {n.uuid: 0 for n in nodes}
    for volume in _volumes_of(cluster_id):
        if volume.home_node_id in counts:
            counts[volume.home_node_id] += 1
    candidates = [n for n in nodes if n.status == EdgeNode.STATUS_ONLINE
                  and n.lvstore_base]
    if not candidates:
        raise ValueError("No online store owner available for placement")
    return min(candidates, key=lambda n: (counts[n.uuid], n.store_index))


def _set_path_state(rpc, node, volume, optimized):
    if _subsystem_has_listener(rpc, volume.nqn, node.get_data_ip(), volume.client_port):
        rpc.nvmf_subsystem_listener_set_ana_state(
            volume.nqn, node.get_data_ip(), volume.client_port,
            is_optimized=optimized)


def _publish_volume(rpc, node, cluster, volume, optimized):
    """Expose one volume on one node: subsystem, namespace (the lvol/crypto
    bdev exists on BOTH nodes — registration puts it on the secondary), and a
    listener whose ANA state encodes the path role."""
    _ensure_transport(rpc)
    _ensure_subsystem(rpc, volume.nqn, serial=f"ev{stack._short(volume.uuid)}")
    if volume.crypto:
        _ensure_crypto_stack(rpc, cluster, volume)
    if not _subsystem_has_ns(rpc, volume.nqn, _ns_bdev(volume)):
        rpc.nvmf_subsystem_add_ns(volume.nqn, _ns_bdev(volume), nsid=volume.ns_id)
    if not _subsystem_has_listener(rpc, volume.nqn, node.get_data_ip(), volume.client_port):
        rpc.listeners_create(volume.nqn, "TCP", node.get_data_ip(), volume.client_port,
                             ana_state="optimized" if optimized else "non_optimized")
    else:
        _set_path_state(rpc, node, volume, optimized)


def _lvol_identity(rpc, lvol_bdev):
    """(uuid, blobid) of a freshly created lvol — the registration payload."""
    info = (rpc.get_bdevs(name=lvol_bdev) or [{}])[0]
    blobid = (info.get('driver_specific') or {}).get('lvol', {}).get('blobid', 0)
    return info.get('uuid', ''), blobid


def create_volume(cluster_id, name, size, crypto=False) -> EdgeVolume:
    cluster = _require_edge_cluster(cluster_id)
    if db.get_edge_volume_by_name(cluster_id, name) is not None:
        raise ValueError(f"Volume with name {name} already exists")
    nodes = _active_nodes(cluster_id)
    if not nodes:
        raise ValueError("Edge cluster has no nodes")
    if len(nodes) == 1:
        _ensure_single_node_lvstore(cluster, nodes[0])

    home = _pick_home(cluster_id, nodes)
    lvs = stack.lvs_name(home.uuid)
    leader = _leader_node(nodes, lvs)

    volume = EdgeVolume()
    volume.uuid = str(uuid_lib.uuid4())
    volume.cluster_id = cluster_id
    volume.volume_name = name
    volume.size = size
    volume.home_node_id = home.uuid
    volume.lvol_bdev = stack.volume_bdev(home.uuid, name)
    volume.nqn = stack.volume_nqn(cluster.nqn, volume.uuid)
    volume.client_port = stack.store_client_port(home.nvmf_port, home.store_index)
    volume.crypto = crypto
    volume.crypto_bdev = stack.crypto_bdev(volume.uuid) if crypto else ""

    leader_rpc = node_rpc_client(leader)
    leader_rpc.create_lvol(name, size // (1024 * 1024), lvs)

    peers = [n for n in nodes if n.uuid != leader.uuid
             and n.status == EdgeNode.STATUS_ONLINE]
    if peers:
        # Product processing: register the creation on the pairing node's
        # SECONDARY lvstore instance so its lvol bdev (and with it the
        # non-optimized path) exists there immediately.
        registered_uuid, blobid = _lvol_identity(leader_rpc, volume.lvol_bdev)
        for peer in peers:
            node_rpc_client(peer).bdev_lvol_register(
                name, lvs, registered_uuid, blobid)

    for node in nodes:
        if node.status != EdgeNode.STATUS_ONLINE:
            continue
        _publish_volume(node_rpc_client(node), node, cluster, volume,
                        optimized=(node.uuid == leader.uuid))

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
    nodes = _active_nodes(cluster_id)
    leader = _leader_node(nodes, stack.lvs_name(volume.home_node_id))

    def _mark(fresh):
        fresh.status = EdgeVolume.STATUS_IN_DELETION
        return True
    db.atomic_update(volume, _mark)

    for node in nodes:
        if node.status != EdgeNode.STATUS_ONLINE:
            continue
        rpc = node_rpc_client(node)
        try:
            rpc.subsystem_delete(volume.nqn)
        except RPCException:
            pass
        if volume.crypto:
            try:
                rpc.lvol_crypto_delete(volume.crypto_bdev)
            except RPCException:
                pass
    node_rpc_client(leader).delete_lvol(volume.lvol_bdev)
    if volume.crypto:
        with _kms_connection(cluster) as kms:
            kms.delete_data_encryption_keys(
                stack.volume_dek_path(cluster_id, volume.uuid))
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
    nodes = _active_nodes(cluster_id)
    leader = _leader_node(nodes, stack.lvs_name(volume.home_node_id))
    node_rpc_client(leader).bdev_lvol_resize(volume.lvol_bdev, new_size // (1024 * 1024))

    def _mutate(fresh):
        fresh.size = new_size
        return True
    db.atomic_update(volume, _mutate)
    volume.size = new_size
    return volume


def get_connect_info(cluster_id, volume_id) -> list:
    """One entry per node exposing the volume — the leader's path is
    ANA-optimized, the peer's non-optimized. Clients connect ALL entries;
    the kernel's ANA handling steers IO."""
    _require_edge_cluster(cluster_id)
    volume = db.get_edge_volume_by_id(cluster_id, volume_id)
    nodes = _active_nodes(cluster_id)
    lvs = stack.lvs_name(volume.home_node_id)
    leader_uuid = next((n.uuid for n in nodes if lvs in n.leader_of), None)
    ordered = sorted(nodes, key=lambda n: n.uuid != leader_uuid)
    return [{
        "transport": "tcp",
        "ip": node.get_data_ip(),
        "port": volume.client_port,
        "nqn": volume.nqn,
        "active": node.uuid == leader_uuid,
        "reconnect-delay": core_constants.LVOL_NVME_CONNECT_RECONNECT_DELAY,
        "ctrl-loss-tmo": core_constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO,
        "nr-io-queues": 2,
    } for node in ordered]


# ------------------------------------------------------------------ devices

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
    nodes = _active_nodes(cluster_id)
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


def replace_device(cluster_id, node_id, old_path, new_path) -> str:
    _require_edge_cluster(cluster_id)
    node = db.get_edge_node_by_id(cluster_id, node_id)
    part = _partition_or_raise(node, old_path)
    _require_redundancy(cluster_id, node, old_path)
    del part

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


# ------------------------------------------------------------ task handlers
# Called by services/tasks_runner_edge.py; return simplyblock_lib TaskResult.

def _raid_is_synced(rpc, raid_name) -> bool:
    """True when the mirror has both legs and no rebuild in flight.

    Fork gate (spec §10): the exact rebuild-progress fields of
    bdev_raid_get_bdevs are fork-specific; this treats "2 base bdevs present
    and no process/rebuilding marker" as synced.
    """
    entry = next((r for r in (rpc.bdev_raid_get_bdevs() or [])
                  if r.get('name') == raid_name), None)
    if entry is None:
        return False
    members = entry.get('base_bdevs_list') or []
    rebuilding = bool(entry.get('process')) or any(
        isinstance(m, dict) and m.get('is_rebuilding') for m in members)
    return len(members) >= 2 and not rebuilding


def _wait_raid_synced(rpc, raid_name,
                      timeout=edge_constants.EDGE_RESYNC_TIMEOUT_SEC,
                      interval=edge_constants.EDGE_RESYNC_POLL_SEC):
    """Block until the mirror finished rebuilding (fail-back gate)."""
    try:
        Retrying(
            stop=stop_after_delay(timeout),
            wait=wait_fixed(interval),
            retry=retry_if_result(lambda synced: not synced)
                  | retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(logger, logging.DEBUG),
        )(_raid_is_synced, rpc, raid_name)
    except RetryError as e:
        raise TimeoutError(
            f"raid {raid_name} did not resync within {timeout}s") from e


def _readd_legs_on_peer(cluster, peer, returned):
    """On the surviving peer, re-add the returned node's halves into BOTH of
    the peer's raid instances (its own store and its secondary instance of
    the returned node's store)."""
    peer_rpc = node_rpc_client(peer)
    _attach_peer(peer_rpc, cluster, returned)
    for raid_name, leg in (
            (stack.mirror_name(peer.uuid), stack.remote_half_bdev(returned.uuid, 2)),
            (stack.mirror_name(returned.uuid), stack.remote_half_bdev(returned.uuid, 1))):
        try:
            peer_rpc.bdev_raid_add_base_bdev(raid_name, leg)
        except RPCException as e:
            if 'already' not in str(e.message).lower():
                raise


def _reassemble_node(cluster, node, nodes) -> None:
    """Idempotently rebuild a node's stack after a pod restart (spec §5.7)."""
    rpc = node_rpc_client(node)
    peers = [n for n in nodes if n.uuid != node.uuid
             and n.status != EdgeNode.STATUS_REMOVED]
    two_node = bool(peers)
    _init_spdk_framework(rpc, node)
    plan = _build_local_stack(rpc, node, split=two_node)
    _expose_repl_subsystem(rpc, cluster, node, plan)

    if not two_node:
        if node.lvstore_base:
            rpc.bdev_examine(node.lvstore_base)
            for volume in _volumes_of(cluster.uuid):
                _publish_volume(rpc, node, cluster, volume, optimized=True)
        return

    peer = peers[0]
    _attach_peer(rpc, cluster, peer)
    _readd_legs_on_peer(cluster, peer, node)

    # Re-instantiate BOTH stores on the returned node: its own (leadership is
    # resolved afterwards — fail-back if the peer took over) and its
    # secondary instance of the peer's store.
    own_plan = stack.plan_store(node, node, peer, node.nvmf_port, node.store_index)
    sec_plan = stack.plan_store(node, peer, node, peer.nvmf_port, peer.store_index)
    _instantiate_store(rpc, node, own_plan, create_lvstore=False)
    _instantiate_store(rpc, node, sec_plan, create_lvstore=False)
    rpc.bdev_lvol_update_lvstore(sec_plan.lvs)

    # Republish paths on the returned node — everything non-optimized until
    # leadership says otherwise (fail-back flips its own store's paths).
    for volume in _volumes_of(cluster.uuid):
        _publish_volume(rpc, node, cluster, volume, optimized=False)


def _fail_back(cluster, returned, peer):
    """Hand the returned node's store home (spec §5.7 step 4): wait for
    resync, fence the store's client port on the peer (nvmf_port_block),
    release leadership there, update + take leadership on the returned node
    (its instance was reloaded by examine during reassembly), flip ANA,
    unfence."""
    lvs = stack.lvs_name(returned.uuid)
    mirror_bdev = stack.mirror_name(returned.uuid)
    port = stack.store_client_port(returned.nvmf_port, returned.store_index)
    peer_rpc = node_rpc_client(peer)
    returned_rpc = node_rpc_client(returned)

    _wait_raid_synced(peer_rpc, mirror_bdev)

    peer_rpc.nvmf_port_block(port)
    try:
        peer_rpc.bdev_lvol_set_leader(lvs, leader=False, bs_nonleadership=True)
        if not returned_rpc.bdev_lvol_update_lvstore(lvs):
            raise RuntimeError(f"bdev_lvol_update_lvstore({lvs}) refused")
        returned_rpc.bdev_lvol_set_leader(lvs, leader=True)
        for volume in _volumes_of(cluster.uuid):
            if volume.home_node_id != returned.uuid:
                continue
            _set_path_state(returned_rpc, returned, volume, optimized=True)
            _set_path_state(peer_rpc, peer, volume, optimized=False)
    finally:
        peer_rpc.nvmf_port_unblock(port)

    def _take(fresh):
        if lvs not in fresh.leader_of:
            fresh.leader_of = fresh.leader_of + [lvs]
        return True
    db.atomic_update(returned, _take)
    returned.leader_of = list(set(returned.leader_of + [lvs]))

    def _release(fresh):
        fresh.leader_of = [name for name in fresh.leader_of if name != lvs]
        return True
    db.atomic_update(peer, _release)
    peer.leader_of = [name for name in peer.leader_of if name != lvs]

    events_controller.log_event_cluster(
        cluster.uuid, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, returned,
        events_controller.CAUSED_BY_MONITOR,
        f"Edge store {lvs} failed back to {returned.hostname}")


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
        own_lvs = stack.lvs_name(node.uuid)
        peer_leader = next((n for n in nodes if n.uuid != node.uuid
                            and own_lvs in n.leader_of), None)
        if node.lvstore_base and peer_leader is not None:
            # Fail-back: the peer took the store over while this node was
            # away — hand it home after resync (port fence + handover).
            _fail_back(cluster, node, peer_leader)
        elif node.lvstore_base and len(nodes) > 1:
            # No takeover happened (restart won the race against fail-over):
            # the records still say this node leads its own store, but its
            # SPDK-side leadership and ANA states died with the pod — resume.
            rpc = node_rpc_client(node)
            rpc.bdev_lvol_update_lvstore(own_lvs)
            rpc.bdev_lvol_set_leader(own_lvs, leader=True)
            for volume in _volumes_of(cluster.uuid):
                if volume.home_node_id == node.uuid:
                    _set_path_state(rpc, node, volume, optimized=True)
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
    """Promote the survivor's SECONDARY lvstore instance of the dead node's
    store (spec §5.6): bdev_lvol_update_lvstore (refresh in-memory metadata
    from its mirror copy) -> set_leader -> ANA flip. task.node_id is the
    survivor; params.lvs the store to take over."""
    cluster = db.get_cluster(task.cluster_id)
    try:
        survivor = db.get_edge_node_by_id(task.cluster_id, task.node_id)
    except KeyError:
        return TaskResult.done("node not found")
    lvs = task.function_params.get("lvs", "")
    nodes = _active_nodes(task.cluster_id)
    owner = next((n for n in nodes if stack.lvs_name(n.uuid) == lvs), None)
    if owner is None:
        return TaskResult.done(f"store {lvs} has no owner")
    if lvs in survivor.leader_of:
        return TaskResult.done("survivor already leads the store")
    if owner.status == EdgeNode.STATUS_ONLINE:
        return TaskResult.done("owner recovered before takeover — nothing to do")
    if survivor.status != EdgeNode.STATUS_ONLINE:
        return TaskResult.retry(f"survivor is {survivor.status}, cannot take over")

    try:
        rpc = node_rpc_client(survivor)
        if not rpc.bdev_lvol_update_lvstore(lvs):
            raise RuntimeError(f"bdev_lvol_update_lvstore({lvs}) refused")
        rpc.bdev_lvol_set_leader(lvs, leader=True)
        for volume in _volumes_of(task.cluster_id):
            if volume.home_node_id != owner.uuid:
                continue
            _publish_volume(rpc, survivor, cluster, volume, optimized=True)
    except Exception as e:
        return TaskResult.retry(f"takeover failed: {e}")

    def _take(fresh):
        if lvs not in fresh.leader_of:
            fresh.leader_of = fresh.leader_of + [lvs]
        return True
    db.atomic_update(survivor, _take)

    def _release(fresh):
        fresh.leader_of = [name for name in fresh.leader_of if name != lvs]
        return True
    db.atomic_update(owner, _release)

    events_controller.log_event_cluster(
        task.cluster_id, events_controller.DOMAIN_STORAGE,
        events_controller.EVENT_STATUS_CHANGE, survivor,
        events_controller.CAUSED_BY_MONITOR,
        f"Edge store {lvs} failed over to {survivor.hostname}")
    return TaskResult.done(f"store {lvs} now led by {survivor.hostname}")


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
        # Fork-capability gate (spec §10): upstream raid5f cannot grow; the
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
