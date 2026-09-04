import copy
import random
import time
import uuid
from datetime import datetime
from typing import Any

from simplyblock_core import utils, constants
from simplyblock_core.controllers import ops_gate
from simplyblock_core.controllers import snapshot_controller, pool_controller, lvol_events, tasks_controller, \
    snapshot_events
from simplyblock_core.db_controller import DBController, SubsystemCapacityError
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.kms import KMSException, create_kms_connection, lvol_dek_path, pool_kek_name
from simplyblock_core.controllers.host_auth import (
    _get_dhchap_group, _register_dhchap_keys_on_node, _register_pool_dhchap_keys_on_node)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.utils import capacity
from simplyblock_core.utils.nvme import HostConnectAuth, build_nvme_connect_entry
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.prom_client import PromClient


logger = utils.get_logger(__name__)


def _create_crypto_lvol(rpc_client, lvol, cluster):
    name = lvol.crypto_bdev
    base_name = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
    ret = rpc_client.get_bdevs(base_name)
    if not ret:
        logger.error(f"Failed to find LVol bdev {base_name}")
        return False

    # Idempotent: if the crypto bdev already exists from a prior partial
    # activation/restart pass, skip the key + crypto-bdev creates. SPDK
    # rejects duplicate creates with hard errors that would otherwise
    # break re-activation convergence.
    if rpc_client.get_bdevs(name):
        logger.info("crypto LVol %s already exists, skipping create", name)
        return True

    with create_kms_connection(cluster) as kms:
        try:
            original_key1, original_key2 = kms.get_data_encryption_keys(
                lvol_dek_path(cluster.get_id(), lvol.get_id()),
                pool_kek_name(lvol.pool_uuid),
            )
        except KMSException:
            logger.exception(f"Failed to get keys for lvol: {name} from KMS")
            return False

    key_name = f'key_{name}'
    ret = rpc_client.lvol_crypto_key_create(key_name, original_key1, original_key2)
    if not ret:
        # SPDK returns failure when the key name already exists. On
        # re-activation that's the same node re-issuing the same key —
        # treat existing key as benign and proceed to the crypto-bdev
        # create below. If creation genuinely failed for another reason,
        # the next call will surface it.
        logger.warning(
            "lvol_crypto_key_create returned failure for %s; if the key "
            "already exists from a prior pass this is expected — "
            "proceeding to crypto bdev create", key_name)
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


def validate_add_lvol_func(name, size, host_id_or_name, pool_id_or_name,
                           max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, all_lvols=None, all_snaps=None):
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
        total = pool_controller.get_pool_total_capacity(pool.get_id(), all_lvols=all_lvols, all_snaps=all_snaps)
        if total + size > pool.pool_max_size:
            return False, f"Invalid LVol size: {utils.humanbytes(size)} " \
                          f"Pool max size has reached {utils.humanbytes(total+size)} of {utils.humanbytes(pool.pool_max_size)}"

    # Name uniqueness via the per-pool name index (O(1)) instead of scanning
    # every lvol in the DB.
    if db_controller.lvol_name_taken(pool.get_id(), name):
        return False, f"LVol name must be unique: {name}"

    # If user gave a QOS and the pool also have a QOS, return error
    if (max_rw_iops or max_rw_mbytes or max_r_mbytes or max_w_mbytes) and (pool.has_qos()):
        return False, "Both Lvol and Pool have QOS settings"

    return True, ""


def count_lvol_subsystems(node, all_lvols=None):
    """Count the lvol subsystems for which ``node`` is the primary.

    Only primary subsystems count against ``max_lvol``: the node's memory
    reservation already provisions for the secondary/tertiary replica
    subsystems it hosts for other nodes' lvols, so replicas must not consume
    subsystem slots. Namespaced volumes share a single subsystem (one NQN) and
    count as one. LVols being deleted are excluded; lvols still in creation
    are counted (their subsystem is about to exist).

    ``all_lvols`` is an optional pre-fetched ``get_mini_lvols()`` result so
    hot paths that already hold the list avoid a second DB scan.
    """
    if all_lvols is None:
        all_lvols = DBController().get_mini_lvols()
    return len({
        lv.nqn for lv in all_lvols
        if lv.node_id == node.get_id()
        and lv.status not in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)
    })


def max_subsystems_for_node(node):
    """Effective per-node subsystem limit: the node's configured ``max_lvol``.

    ``max_lvol`` can only exceed MAX_SUBSYSTEMS_PER_NODE on a record written
    by a release that predates the cap — every configuration surface that can
    SET the value (sn configure, add-node, restart's explicit argument,
    cluster update --max-subsys, the node-configure API) clamps new values to
    the cap. Such legacy nodes were sized (huge pages, iobuf pools) for their
    configured value and must keep operating AND provisioning as configured
    after an upgrade; the previous min() against the cap retroactively froze
    their growth at the cap while they legitimately serve more. The cap is
    enforced where values are set, not where occupancy is admitted."""
    return node.max_lvol


def _get_next_3_nodes(cluster_id, lvol_size=0, all_lvols=None, namespaced=False):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    if all_lvols is None:
        all_lvols = db_controller.get_mini_lvols()

    online_nodes = []
    node_stats = {}
    nodes_at_capacity = {}
    nodes_with_ns_slot = set()
    for node in snodes:
        if node.is_secondary_node:  # pass
            continue
        if node.status == node.STATUS_ONLINE:
            subsys_count = count_lvol_subsystems(node, all_lvols)
            has_ns_slot = bool(
                namespaced and get_next_available_subsystem_on_node(node.get_id(), all_lvols))
            if subsys_count >= max_subsystems_for_node(node) and not has_ns_slot:
                # At subsystem capacity, and (for namespaced creates) no
                # existing subsystem on the node has a free namespace slot.
                nodes_at_capacity[node.get_id()] = subsys_count
                continue
            if has_ns_slot:
                nodes_with_ns_slot.add(node.get_id())
            if node.lvol_sync_del():
                logger.info(f"LVol sync delete task found on node: {node.get_id()}, proceeding anyway")
            online_nodes.append(node)
            node_st = {
                "lvol": subsys_count+1
            }
            node_stats[node.get_id()] = node_st

    if not online_nodes and nodes_at_capacity:
        logger.warning(
            "No eligible node for LVol placement: all online nodes are at max subsystem "
            "capacity (max_lvol): %s",
            ", ".join(f"{nid}={cnt}" for nid, cnt in nodes_at_capacity.items()))

    # A namespaced lvol should fill existing subsystems before opening new
    # ones — prefer nodes that have a free namespace slot when any exist.
    if nodes_with_ns_slot and len(nodes_with_ns_slot) < len(online_nodes):
        online_nodes = [n for n in online_nodes if n.get_id() in nodes_with_ns_slot]
        node_stats = {nid: st for nid, st in node_stats.items() if nid in nodes_with_ns_slot}

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

    selected_node_ids: list[str] = []
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


def _resolve_lvol_subsystem(lvol, host_node, cl, namespaced, all_lvols,
                            internal=False):
    """ADVISORY pre-check of the subsystem pick for a new lvol — fails the
    create early (before KMS keys etc.) when the node has no room at all.

    The AUTHORITATIVE pick happens later, inside the FDB transaction of
    ``DBController.claim_lvol_ns_slot`` at record-write time, which recounts
    occupancy and persists the record atomically (two concurrent creates
    otherwise both grab the same last free namespace slot). Whatever this
    function assigns to ``lvol.nqn``/``lvol.namespace`` is overwritten there.

    A namespaced lvol joins an existing subsystem on the host node when one
    has a free namespace slot; otherwise (and for non-namespaced lvols) a new
    subsystem is claimed. The node's ``max_lvol`` subsystem cap is enforced
    only when a new subsystem would actually be created — joining an existing
    one consumes no subsystem slot.

    Returns ``(True, "")`` or ``(False, error)``.
    """
    lvol.nqn = cl.nqn + ":lvol:" + lvol.uuid
    if namespaced:
        result = get_next_available_subsystem_on_node(host_node.get_id(), all_lvols)
        if result:
            lvol.nqn = result.nqn
            lvol.namespace = result.uuid
            lvol.max_namespace_per_subsys = result.max_namespace_per_subsys
            return True, ""

    # ``internal`` volumes (the REP_* copies a replication transfer lands in)
    # are created by the system, not admitted on a user's behalf, so the cap
    # does not refuse them. See claim_lvol_ns_slot for the authoritative check.
    subsys_count = count_lvol_subsystems(host_node, all_lvols)
    if not internal and subsys_count >= max_subsystems_for_node(host_node):
        return False, (f"Too many subsystems on node: {host_node.get_id()}, "
                       f"max subsystems reached: {max_subsystems_for_node(host_node)}")
    return True, ""


def check_lvstore_object_limit(host_node, all_lvols, all_snaps, new_objects=1):
    """Hard per-lvstore object cap: an lvstore serves at most
    constants.MAX_OBJECTS_PER_LVSTORE objects (lvols, clones, snapshots).
    Enforced on every create path — lvol create, snapshot create, clone.

    Objects are counted against their owning node (lvol.node_id /
    snap.lvol.node_id), and each node owns exactly one lvstore, so the
    owning-node count IS the per-lvstore count. This also gives the intended
    takeover semantics for free: when one host temporarily serves a second
    LVS (acting leader for a peer), that LVS's objects still count against
    its own budget, so each active lvstore on the host may hold the full
    limit independently. Replica registrations on secondary/tertiary are
    not counted — the same object would otherwise count against three
    lvstores it does not live in.

    ``all_lvols`` and ``all_snaps`` accept mini or full records — both minis
    carry everything used here (``.node_id`` / ``.status`` on lvols,
    ``.lvol.node_id`` on snapshots). Prefer minis: full SnapShot records
    embed the complete LVol dict and made this check cost a multi-second
    full-table scan per create at 10k+ snapshots (run 20260721).

    Returns None when within the limit, an error message otherwise.
    """
    limit = constants.MAX_OBJECTS_PER_LVSTORE
    node_id = host_node.get_id()
    lvol_count = sum(1 for lv in all_lvols
                     if lv.node_id == node_id and lv.status != LVol.STATUS_DELETED)
    snap_count = sum(1 for s in all_snaps
                     if s.lvol and s.lvol.node_id == node_id and not s.deleted)
    total = lvol_count + snap_count
    if total + new_objects > limit:
        return (f"Object limit reached on lvstore of node {node_id}: {total} "
                f"objects (lvols/clones: {lvol_count}, snapshots: "
                f"{snap_count}); the hard limit is {limit} per lvstore")
    return None


def add_lvol_ha(name, size, host_id_or_name, ha_type, pool_id_or_name, use_comp=False, use_crypto=False,
                distr_vuid=0, max_rw_iops=0, max_rw_mbytes=0, max_r_mbytes=0, max_w_mbytes=0,
                with_snapshot=False, max_size=0, lvol_priority_class=0,
                uid=None, pvc_name=None, namespaced=None, max_namespace_per_subsys=None, fabric="tcp", ndcs=0, npcs=0,
                allowed_hosts=None, do_replicate=False, replication_cluster_id=None, crypto_key=None,
                replication_policy=None, internal=False):
    db_controller = DBController()
    logger.info(f"Adding LVol: {name}")
    if max_namespace_per_subsys is None:
        # A namespaced lvol whose new subsystem allows only one namespace can
        # never be joined by later namespaced lvols, silently degenerating to
        # one-subsystem-per-lvol — default to a shareable capacity instead.
        max_namespace_per_subsys = constants.LVO_MAX_NAMESPACES_PER_SUBSYS if namespaced else 1
    if max_namespace_per_subsys > constants.MAX_NAMESPACES_PER_SUBSYSTEM:
        return False, (
            f"max_namespace_per_subsys={max_namespace_per_subsys} exceeds the "
            f"hard limit of {constants.MAX_NAMESPACES_PER_SUBSYSTEM} "
            f"namespaces per subsystem")
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
            logger.info(f"LVol sync delete task on node: {host_node.get_id()}, proceeding anyway")

    pool = None
    for p in db_controller.get_pools():
        if pool_id_or_name == p.get_id() or pool_id_or_name == p.pool_name:
            pool = p
            break
    if not pool:
        return False, f"Pool not found: {pool_id_or_name}"

    ops_gate.assert_object_ops_allowed("volume create", cluster_id=pool.cluster_id)

    cl = db_controller.get_cluster_by_id(pool.cluster_id)

    if (fabric == "tcp" and not cl.fabric_tcp) or (fabric == "rdma" and not cl.fabric_rdma):
        return False,  f"Fabric not available in cluster: {fabric}"

    if cl.status not in Cluster.MUTABLE_STATUSES:
        return False, f"Cluster is not active, status: {cl.status}"

    if lvol_priority_class > 0:
        class_found = False
        for qos_class in db_controller.get_qos(cl.uuid):
            if qos_class.class_id == lvol_priority_class:
                class_found = True
        if not class_found:
            return False, f"QOS class not found: {lvol_priority_class}"

    if uid:
        try:
            lvol = db_controller.get_lvol_by_id(uid)
            if pvc_name:
                lvol.pvc_name = pvc_name
            if name:
                lvol.lvol_name = name
            lvol.write_to_db()
            return uid, None
        except KeyError:
            pass

    if ha_type == "default":
        ha_type = cl.ha_type

    max_rw_iops = max_rw_iops or 0
    max_rw_mbytes = max_rw_mbytes or 0
    max_r_mbytes = max_r_mbytes or 0
    max_w_mbytes = max_w_mbytes or 0

    # TTL-cached scans: these feed advisory capacity math and random-vuid
    # dedup only — name uniqueness goes through the O(1) per-pool name index
    # inside validate_add_lvol_func, so a few seconds of staleness here cannot
    # admit a duplicate name. Uncached, these two full-DB reads cost seconds
    # per create at a few thousand objects and dominate mass-create runs.
    from simplyblock_core.utils.ttl_cache import cached_mini_lvols, cached_mini_snapshots
    all_lvols = cached_mini_lvols(db_controller)
    all_snaps = cached_mini_snapshots(db_controller)
    result, error = validate_add_lvol_func(name, size, None, pool_id_or_name,
                                           max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, all_lvols, all_snaps)

    if error:
        logger.error(error)
        return False, error

    if pool.has_qos():
        host_node = db_controller.get_storage_node_by_id(pool.qos_host)

    # Effective (client-visible) bytes: LVol.size is the logical size the client
    # sees, never the raw cost of storing it.
    cluster_size_prov = sum([lv.size for lv in all_lvols])
    # Snapshots hold ACTUAL bytes that provisioned sizes do not cover; admit
    # against provisioned + snapshot utilisation, the same model as the
    # pool-level check (see pool_controller.get_cluster_snapshot_utilization).
    cluster_size_prov += pool_controller.get_cluster_snapshot_utilization(
        cl.get_id(), all_snaps=all_snaps)

    dev_count = 0
    snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
    online_nodes = []
    cluster_size_total_raw = 0
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1
                    cluster_size_total_raw += dev.size
    # NVMeDevice.size is RAW (physical, parity-inclusive); cluster_size_prov is
    # the sum of provisioned lvol sizes, which is EFFECTIVE. Comparing them
    # directly understated provisioned utilisation by (ndcs+npcs)/ndcs -- 1.5x on
    # a 4+2 cluster -- so prov_cap_crit/prov_cap_warn admitted over-commits they
    # were configured to reject.
    cluster_size_total = capacity.to_effective(cluster_size_total_raw, cl)

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

    if host_node and host_node.lvstore_status == "in_creation":
        mgs = f"Storage node LVStore is being recreated (restart in progress). ID: {host_node.get_id()}"
        logger.error(mgs)
        return False, mgs

    if ndcs or npcs:
        if ndcs+npcs > len(online_nodes):
            mgs = f"Online storage nodes: {len(online_nodes)} are less than the required LVol geometry: {(ndcs+npcs)}"
            logger.error(mgs)
            return False, mgs

    if cluster_size_total <= 0:
        # Every online device reported zero usable capacity: no meaningful
        # utilisation to compute, and dividing would raise. dev_count > 0 above
        # already proved there are online devices, so this is a real anomaly
        # rather than an empty cluster.
        msg = "Cluster has no effective capacity (online devices report zero size)"
        logger.error(msg)
        return False, msg

    cluster_size_prov_util = int(((cluster_size_prov+size) / cluster_size_total) * 100)

    if cl.prov_cap_crit and cl.prov_cap_crit < cluster_size_prov_util:
        msg = f"Cluster provisioned cap critical would be, util: {cluster_size_prov_util}% of cluster util: {cl.prov_cap_crit}"
        logger.error(msg)
        return False, msg

    elif cl.prov_cap_warn and cl.prov_cap_warn < cluster_size_prov_util:
        logger.warning(f"Cluster provisioned cap warning, util: {cluster_size_prov_util}% of cluster util: {cl.prov_cap_warn}")

    if not distr_vuid:
        vuid = utils.get_random_vuid(all_lvols=all_lvols, all_snapshots=all_snaps)
    else:
        vuid = distr_vuid

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
    # ns_id semantics in the create flow: 0 = "not assigned yet". The model
    # default is 1 (a legitimate nsid), so it must be reset here — the
    # primary's namespace add assigns the real value and every replica add
    # is REQUIRED to reuse it (see add_lvol_on_node). Never let a replica
    # add run with an auto-assigned nsid: namespace IDs must be identical
    # on every path of a shared subsystem, or the client kernel rejects
    # the namespaces ("duplicate IDs in subsystem" / "IDs don't match for
    # shared namespace", mass-create incident 2026-07-06).
    lvol.ns_id = 0
    lvol.lvol_name = name
    lvol.pvc_name = pvc_name or ""
    lvol.size = int(size)
    lvol.max_size = int(max_size)
    lvol.status = LVol.STATUS_IN_CREATION
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    lvol.create_dt = str(datetime.now())
    lvol.ha_type = ha_type
    lvol.bdev_stack = []
    lvol.uuid = uid or str(uuid.uuid4())
    lvol.guid = utils.generate_hex_string(16)
    lvol.vuid = vuid
    lvol.lvol_bdev = f"LVOL_{vuid}"
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    lvol.crypto_bdev = ''
    lvol.comp_bdev = ''

    lvol.lvol_type = 'lvol'
    if lvol_priority_class:
        lvol.lvol_priority_class = lvol_priority_class
    else:
        lvol.lvol_priority_class = 0
    lvol.fabric = fabric

    if not host_node:
        nodes = _get_next_3_nodes(cl.get_id(), lvol.size, all_lvols, namespaced=bool(namespaced))
        if not nodes:
            return False, "No nodes found with enough resources to create the LVol"
        host_node = nodes[0]

    limit_error = check_lvstore_object_limit(host_node, all_lvols, all_snaps)
    if limit_error:
        logger.error(limit_error)
        return False, limit_error

    # Create a new subsystem by default unless namespaced is set and an
    # existing subsystem on the host node has a free namespace slot.
    lvol.max_namespace_per_subsys = max_namespace_per_subsys
    ret, error = _resolve_lvol_subsystem(lvol, host_node, cl, namespaced, all_lvols,
                                         internal=internal)
    if not ret:
        logger.error(error)
        return False, error

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
        # Namespaced siblings MUST replicate to the same target node.
        # A fail-over copy preserves the volume's NQN and nsid, so all
        # volumes sharing a subsystem land in the SAME subsystem on the
        # target. Picking the destination purely by capacity scattered
        # siblings across the target cluster's nodes, which splits one
        # shared subsystem across unrelated primaries: each advertises the
        # same NQN with only its own subset of namespaces, and the copies
        # collide when a sibling's nsid is already taken there (soak case 7,
        # run 20260824_215758: 14 of 20 namespaces failed over, the 15th
        # died in add_ns).
        sibling_node_id = ""
        if getattr(lvol, "namespaced", False) or lvol.max_namespace_per_subsys > 1:
            for lv in (all_lvols or db_controller.get_lvols(cl.get_id())):
                if (lv.nqn == lvol.nqn and lv.get_id() != lvol.get_id()
                        and getattr(lv, "replication_node_id", "")):
                    sibling_node_id = lv.replication_node_id
                    break
        if sibling_node_id:
            logger.info(
                f"LVol {lvol.lvol_name} shares subsystem {lvol.nqn} with an "
                f"already-replicating sibling; using its replication node "
                f"{sibling_node_id} so the shared subsystem is not split")
            lvol.replication_node_id = sibling_node_id
        else:
            random_nodes = _get_next_3_nodes(replication_cluster_id, lvol.size, all_lvols)
            lvol.replication_node_id = random_nodes[0].get_id()

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
        lvol.crypto_bdev = f"crypto_{lvol.lvol_bdev}"
        lvol.bdev_stack.append({
            "type": "crypto",
            "name": lvol.crypto_bdev,
            "params": {
                "name": lvol.crypto_bdev,
                "base_name": lvol.top_bdev
            }
        })
        lvol.lvol_type += ',crypto'
        lvol.top_bdev = lvol.crypto_bdev

    # Allowed hosts / DH-HMAC-CHAP config that applies only if this lvol ends
    # up OWNING a new subsystem — a namespaced lvol that joins an existing
    # subsystem inherits that subsystem's host configuration instead. Computed
    # up front (key generation can fail with a user-facing error); whether it
    # applies is decided inside the slot-claim transaction below.
    standalone_allowed_hosts = None
    if pool.dhchap:
        # Pool-level DHCHAP: inherit allowed hosts from pool (no per-host key generation)
        standalone_allowed_hosts = [{"nqn": h} for h in pool.allowed_hosts]
    elif allowed_hosts:
        # Legacy per-lvol host restriction with pool.sec_options key generation
        host_entries = _build_host_entries(allowed_hosts, pool.sec_options or None)
        if isinstance(host_entries, tuple):
            return host_entries  # (False, error_message)
        standalone_allowed_hosts = host_entries

    # Set pool_uuid before write_to_db and add_lvol_on_node so that
    # add_lvol_on_node can look up the pool for DHCHAP key registration.
    lvol.pool_uuid = pool.get_id()
    lvol.pool_name = pool.pool_name
    logger.info("[DHCHAP-DEBUG] create_lvol: pool_uuid=%s, pool.dhchap=%s, "
                "allowed_hosts=%s, pool.dhchap_key=%s",
                lvol.pool_uuid, pool.dhchap,
                lvol.allowed_hosts,
                bool(pool.dhchap_key) if pool.dhchap else "N/A")

    if use_crypto:
        with create_kms_connection(cl) as kms:
            try:
                if crypto_key is None:
                    kms.create_data_encryption_keys(
                        lvol_dek_path(cl.get_id(), lvol.get_id()),
                        pool_kek_name(pool.get_id()),
                    )
                else:
                    kms.import_data_encryption_keys(
                        lvol_dek_path(cl.get_id(), lvol.get_id()),
                        pool_kek_name(pool.get_id()),
                        crypto_key,
                    )
                logger.info("Created lvol keys")
            except KMSException:
                msg = "Failed to create lvol keys"
                logger.exception(msg)
                return False, msg

    # ONE FDB transaction: pick the namespace slot and persist the record
    # (STATUS_IN_CREATION) together. The record itself is the slot claim, so
    # a concurrent create/clone conflict-retries and recounts with this
    # record visible instead of racing it for the same last slot
    # (_resolve_lvol_subsystem above was only the advisory early-fail check).
    try:
        db_controller.claim_lvol_ns_slot(
            lvol, host_node, bool(namespaced),
            standalone_nqn=cl.nqn + ":lvol:" + lvol.uuid,
            standalone_allowed_hosts=standalone_allowed_hosts,
            internal=internal)
    except SubsystemCapacityError as e:
        logger.error(str(e))
        return False, str(e)

    if ha_type == "single":
        if host_node.status == StorageNode.STATUS_ONLINE:
            # INNER per-node lock. The create path took no lock at all,
            # although lvstore_op_lock documents that it "guarantees no two
            # object operations (create/delete/resize of any
            # lvol/snapshot/clone) mutate the lvstore on a node at the same
            # time" -- delete and resize took it, create did not, so a
            # create could interleave with another object's delete on the
            # same node and race the replica blob-tree mutation.
            with snapshot_controller.lvstore_op_lock(
                    host_node.cluster_id, lvol.lvs_name,
                    node_id=host_node.get_id()):
                lvol_bdev, error = add_lvol_on_node(lvol, host_node)
            if error:
                db_controller.release_lvol_ns_slot(lvol)
                return False, error

            lvol.nodes = [host_node.get_id()]
            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']
        else:
            msg = f"Host node in not online: {host_node.get_id()}"
            logger.error(msg)
            db_controller.release_lvol_ns_slot(lvol)
            return False, msg

    if ha_type == "ha":
        # OUTER chain lock: serialize this create + every replica
        # registration against any other operation on the same chain.
        # The create path took no chain lock, so a create/clone could
        # interleave with a delete or resize elsewhere in the chain while
        # it walked the same blob structure -- delete and resize have held
        # this lock all along. Keyed on the chain root; for a brand new
        # volume that is its own uuid (clones go through
        # snapshot_controller, which already locks the parent chain).
        with snapshot_controller.object_mutation_lock(cl.get_id(), lvol.uuid):
            from simplyblock_core.storage_node_ops import (
                find_leader_with_failover, check_non_leader_for_operation,
                execute_on_leader_with_failover,
            )

            # Build nodes list
            secondary_ids = [host_node.secondary_node_id]
            if host_node.tertiary_node_id:
                secondary_ids.append(host_node.tertiary_node_id)
            lvol.nodes = [host_node.get_id()] + secondary_ids

            all_nodes = [host_node]
            for sid in secondary_ids:
                try:
                    all_nodes.append(db_controller.get_storage_node_by_id(sid))
                except KeyError:
                    pass

            # Step 1: Pre-check all non-leaders BEFORE executing on leader
            primary_node, non_leaders = find_leader_with_failover(all_nodes, lvol.lvs_name)
            if primary_node is None:
                msg = "No leader available for lvol create"
                logger.error(msg)
                db_controller.release_lvol_ns_slot(lvol)
                return False, msg

            precheck_started = time.time()
            secondary_nodes = []
            for nl in non_leaders:
                # Under the chain lock: wait out a peer restart rather
                # than fragmenting [create + registers] across
                # processes. Bounded, so a wedged restart still falls
                # back to durable deferral.
                action = check_non_leader_for_operation(
                    nl.get_id(), lvol.lvs_name, operation_type="create",
                    leader_op_completed=False, all_nodes=all_nodes,
                    wait_for_restart=constants.DEFERRED_LEG_RESTART_WAIT_SEC)
                if action == "reject":
                    msg = f"Cannot create lvol: non-leader {nl.get_id()[:8]} unreachable but fabric healthy"
                    logger.error(msg)
                    db_controller.release_lvol_ns_slot(lvol)
                    return False, msg
                elif action == "proceed":
                    secondary_nodes.append(nl)
                else:
                    # "queue", "skip", or any other non-proceed verdict: DB-backed
                    # deferral (NOT the in-memory drain queue: that is per-process
                    # and dies with it — incident 2026-07-10 lost a tertiary
                    # registration this way). "skip" used to drop the registration
                    # entirely on the assumption that node-restart recovery would
                    # rebuild it — but the verdict can be stale/wrong while the
                    # node is actually up (run 20260721-213609: LVOL_109's
                    # secondary answered add_ns 200ms later, yet was never
                    # registered; every later snapshot of it failed). The sync-op
                    # runner applies the registration once the node is
                    # serviceable; a registration the restart flow already
                    # replayed is idempotent.
                    tasks_controller.add_lvol_sync_op_task(
                        host_node.cluster_id, nl.get_id(), lvol.get_id(),
                        "register", secondary_index=_lvol_secondary_index(lvol, nl))

            # Step 2: Execute on leader (with failover on failure)
            def _create_on_leader(leader):
                with snapshot_controller.lvstore_op_lock(
                        leader.cluster_id, lvol.lvs_name,
                        node_id=leader.get_id()):
                    lvol_bdev, error = add_lvol_on_node(lvol, leader)
                if error:
                    raise RuntimeError(error)
                return lvol_bdev

            success, actual_leader, result = execute_on_leader_with_failover(
                all_nodes, lvol.lvs_name, _create_on_leader, known_leader=primary_node)
            if not success:
                logger.error(f"Failed to create lvol on leader: {result}")
                # If a leader attempt got far enough to create the blob, its
                # rollback issued the ASYNC initial delete and flipped the record
                # to in_deletion (_fail_after_bdev / _create_bdev_stack). Keep the
                # record: the lvol monitor's delete state machine owns completing
                # that delete (poll → finish → sync replicas). Erasing it here
                # orphaned the async delete with no sync follow-up — 27 open
                # delete windows in run 20260721-213609.
                try:
                    fresh = db_controller.get_lvol_by_id(lvol.get_id())
                except KeyError:
                    fresh = None
                if fresh is not None and fresh.status == LVol.STATUS_IN_DELETION:
                    logger.warning(
                        "LVol %s rollback left an in-flight async delete; keeping "
                        "the record in in_deletion for the monitor to complete",
                        lvol.get_id())
                else:
                    db_controller.release_lvol_ns_slot(lvol)
                return False, str(result)

            lvol_bdev = result
            lvol.lvol_uuid = lvol_bdev['uuid']
            lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

            # Step 3: Execute registration on non-leaders that passed pre-check.
            # The Step-1 verdict is reused while fresh: re-sweeping the data-plane
            # quorum seconds after the pre-check re-verifies unchanged state and
            # doubled the probe cost of every create. Only when the leader op took
            # long enough for connectivity to plausibly have moved do we re-check
            # (and then with leader_op_completed=True, which unlocks the
            # kill_and_wait handling the second pass exists for).
            stale_precheck = (time.time() - precheck_started) > 30
            for sec in secondary_nodes:
                reg_index = _lvol_secondary_index(lvol, sec)
                action = "proceed" if not stale_precheck else check_non_leader_for_operation(
                    sec.get_id(), lvol.lvs_name, operation_type="create",
                    leader_op_completed=True, all_nodes=all_nodes,
                    wait_for_restart=constants.DEFERRED_LEG_RESTART_WAIT_SEC)
                if action == "proceed":
                    try:
                        with snapshot_controller.lvstore_op_lock(
                                sec.cluster_id, lvol.lvs_name,
                                node_id=sec.get_id()):
                            lvol_bdev, error = add_lvol_on_node(
                                lvol, sec, is_primary=False,
                                secondary_index=reg_index)
                    except Exception as e:
                        # e.g. PreconditionError from the per-node lvstore lock —
                        # the node can die while this op WAITS for the lock (the
                        # holder is stuck RPC-ing the same dead node).
                        lvol_bdev, error = None, str(e)
                    if error:
                        # Node-attributable failure between the pre-check and now
                        # (died mid-registration or while waiting on its per-node
                        # lock). Do NOT roll the whole create back: the leader op
                        # succeeded and the remaining replicas (e.g. the tertiary)
                        # must still be registered. Defer this node's registration
                        # to the durable sync-op task and CONTINUE — the volume
                        # converges to full redundancy when the node returns.
                        # WARNING, not ERROR: the create still succeeds — an
                        # ERROR line makes harnesses retry a committed create
                        # (name-unique loop, 2026-07-10 soak).
                        logger.warning(
                            "Registration of lvol %s on non-leader %s failed (%s); "
                            "deferring to sync-op task and continuing with the "
                            "remaining replicas", lvol.get_id(), sec.get_id()[:8], error)
                        tasks_controller.add_lvol_sync_op_task(
                            host_node.cluster_id, sec.get_id(), lvol.get_id(),
                            "register", secondary_index=reg_index)
                else:
                    # "kill_and_wait", "queue", "skip", "reject": the leader op is
                    # already committed, so a registration that cannot run NOW must
                    # never be dropped — defer every non-proceed verdict to the
                    # durable sync-op task (a silently skipped registration leaves
                    # the replica permanently missing and fails every later
                    # snapshot of the volume on that node; run 20260721-213609,
                    # LVOL_109).
                    if action == "kill_and_wait":
                        logger.warning("Non-leader %s needs kill+restart for lvol create", sec.get_id()[:8])
                    tasks_controller.add_lvol_sync_op_task(
                        host_node.cluster_id, sec.get_id(), lvol.get_id(),
                        "register", secondary_index=reg_index)

    lvol.status = LVol.STATUS_ONLINE
    lvol.write_to_db(db_controller.kv_store)
    lvol_events.lvol_create(lvol)

    # set QOS
    if max_rw_iops >= 0 or max_rw_mbytes >= 0 or max_r_mbytes >= 0 or max_w_mbytes >= 0:
        set_lvol(lvol.uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes)

    if pool.allowed_hosts:
        for host_nqn in pool.allowed_hosts:
            logger.info(f"Adding host {host_nqn} to lvol {lvol.get_id()}")
            add_host_to_lvol(lvol.get_id(), host_nqn)

    if replication_policy:
        # Optional at create time: assigning a policy configures replication for
        # the volume (destination, cadence and mode all come from the policy).
        # Imported here because replication_policy_controller imports this module.
        from simplyblock_core.controllers import replication_policy_controller
        try:
            replication_policy_controller.attach_policy(lvol.get_id(), replication_policy)
        except Exception as e:
            # The volume itself is created and usable; surface the failure
            # instead of silently leaving it unreplicated.
            logger.error("Volume %s created but replication policy %s could not be "
                         "attached: %s", lvol.get_id(), replication_policy, e)
            return lvol.uuid, f"Volume created but replication policy could not be attached: {e}"

    return lvol.uuid, None


def _create_bdev_stack(lvol, snode, is_primary=True):
    rpc_client = snode.rpc_client()

    created_bdevs = []
    for bdev in lvol.bdev_stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']
        # Idempotency probe per stack bdev. A by-name bdev_get_bdevs resolves
        # names, aliases and uuids server-side and returns [] / an -ENODEV
        # error (→ None) when absent — equivalent to the previous full-dump
        # membership test, but O(1) instead of serializing every bdev on the
        # node into the response (the dump grows with lvol count and was the
        # single largest cost of mass creates).
        if rpc_client.get_bdevs(name):
            continue

        ret = None
        if type == "bmap_init":
            ret = rpc_client.ultra21_lvol_bmap_init(**params)

        elif type == "ultra_lvol":
            ret = rpc_client.ultra21_lvol_mount_lvol(**params)

        elif type == "crypto":
            db_controller = DBController()
            cluster = db_controller.get_cluster_by_id(snode.cluster_id)
            ret = _create_crypto_lvol(rpc_client, lvol, cluster)

        elif type == "bdev_lvstore":
            ret = rpc_client.create_lvstore(**params)

        elif type == "bdev_lvol":
            if is_primary:
                ret = rpc_client.create_lvol(**params)
                if not ret:
                    # The bdev may already exist from a prior pass through
                    # this function (the subsystem-full retry in
                    # add_lvol_on_node re-enters _create_bdev_stack after
                    # the bdev was created but nvmf_subsystem_add_ns
                    # failed).  The idempotency probe above uses the bare
                    # stack name which doesn't resolve for lvol bdevs
                    # (SPDK registers them as lvstore/lvol_name).
                    existing = rpc_client.get_bdevs(
                        f"{lvol.lvs_name}/{name}")
                    if existing:
                        ret = existing
            else:
                ret = rpc_client.bdev_lvol_register(
                    lvol.lvol_bdev, lvol.lvs_name, lvol.lvol_uuid, lvol.blobid, lvol.lvol_priority_class)

        elif type == "bdev_lvol_clone":
            if is_primary:
                ret = rpc_client.lvol_clone(**params)
                if not ret:
                    existing = rpc_client.get_bdevs(
                        f"{lvol.lvs_name}/{name}")
                    if existing:
                        ret = existing
            else:
                ret = rpc_client.bdev_lvol_clone_register(
                    lvol.lvol_bdev, lvol.snapshot_name, lvol.lvol_uuid, lvol.blobid)
                if ret:
                    # clone_register ACKNOWLEDGES before the bdev is
                    # examinable (the same async false-success family as
                    # remove_ns in the PVC-expand and case-3 incidents). The
                    # very next step adds this bdev to the nvmf subsystem, and
                    # racing the registration lost every time on the fail-over
                    # of namespaced volumes: peer add_ns -32602 with the
                    # subsystem EMPTY, while the bdev existed moments later
                    # (run 20260825_122423, LVS_13/LVOL_121). Poll until the
                    # bdev is really there before letting add_ns proceed.
                    bdev_name = f"{lvol.lvs_name}/{lvol.lvol_bdev}"
                    for _ in range(40):
                        if rpc_client.get_bdevs(bdev_name):
                            break
                        time.sleep(0.5)
                    else:
                        logger.error(
                            f"clone_register acknowledged but {bdev_name} did "
                            f"not appear within 20s on the peer")
                        ret = None

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
                # If the rollback tore down a blob-carrying bdev on the
                # primary, it fired the ASYNC initial delete — that delete
                # must be completed by the monitor's state machine (poll →
                # finish → sync replicas), so flip the record to in_deletion
                # here. Callers must not erase the record in this state (see
                # add_lvol_ha's leader-failure branch).
                if is_primary and any(
                        b['type'] in ("bdev_lvol", "bdev_lvol_clone")
                        for b in created_bdevs):
                    try:
                        lvol.status = LVol.STATUS_IN_DELETION
                        lvol.write_to_db(DBController().kv_store)
                    except Exception:
                        logger.exception(
                            "failed to persist in_deletion after stack "
                            "rollback for %s", lvol.get_id())
            return False, f"Failed to create BDev: {name}"

    return True, None


def _resolve_namespaced_subsystem(lvol, rpc_client, snode):
    """Return True if ``lvol`` should follow the standalone subsystem-create
    path (i.e. ``lvol.namespace`` ends up empty), False if it should attach to
    the pre-existing subsystem named by ``lvol.nqn``.

    The gone-or-full cases (a concurrent lvol-delete tore the subsystem down
    after ``snapshot_controller.clone()`` picked it via
    ``get_next_available_subsystem_on_node``, or it filled up meanwhile) are
    handled where they surface: ``nvmf_subsystem_add_ns`` rejects the add and
    the -32602 fallback in ``add_lvol_on_node`` re-resolves to another
    subsystem (or downgrades to standalone) and retries. This function used to
    pre-verify via ``subsystem_list`` — a full ``nvmf_get_subsystems`` dump
    whose response grows with total lvol count, paid on EVERY create to catch
    a race that occurs at most once per max_namespaces creates. Trust the CP's
    own record and let the error path pay the dump only when it actually fires.
    """
    return not lvol.namespace


def _fail_after_bdev(lvol, rpc_client, msg):
    """Rollback an in-progress add_lvol_on_node after _create_bdev_stack has
    already produced a bdev/blob. Without this, a post-bdev-stack failure (a
    missing namespaced subsystem, a listener add error, an add_ns error) leaves
    the SPDK clone-blob in place, which then blocks the parent snapshot delete
    with "vbdev_lvol_destroy: ... has N clones". Logs but does not raise on
    rollback failure so the caller still sees the original error."""
    try:
        _remove_bdev_stack(lvol.bdev_stack[::-1], rpc_client)
        lvol.status = LVol.STATUS_IN_DELETION
        lvol.write_to_db(DBController().kv_store)
    except Exception:
        logger.exception("rollback of bdev stack failed for %s", lvol.get_id())
    return False, msg


#: Width of the controller-id window reserved for each path of a shared
#: subsystem. Every path must hand out cntlids from a DISJOINT window: the
#: host rejects a controller that presents an already-seen cntlid for the
#: same subsystem ("Duplicate cntlid N with nvmeX, subsys ..., rejecting")
#: and that path is then silently missing for the lifetime of the
#: connection — `nvme connect` reports success, the target establishes
#: qpairs, but the namespace never joins the multipath head. Incident
#: 2026-08-09: volumes 78726d0e / 3f171cfb / a2d300d3 ran permanently at
#: 2 of 3 paths this way.
LVOL_CNTLID_WINDOW = 1000


def lvol_min_cntlid(path_index: int) -> int:
    """``min_cntlid`` for the subsystem path at ``path_index`` in ``lvol.nodes``.

    0 (primary) -> 1, 1 (secondary) -> 1000, 2 (tertiary) -> 2000, ...

    Single source of truth on purpose. Each call site used to carry its own
    formula — ``1000 * (secondary_index + 1)`` on the create path versus
    ``1 + 1000 * ha_inode_self`` on the restart path — so the same node could
    be given a different window depending on which flow (re)created its
    subsystem.
    """
    return 1 if path_index <= 0 else LVOL_CNTLID_WINDOW * path_index


def _lvol_path_index(lvol, node) -> int:
    """Position of ``node`` among the lvol's paths (0 = primary/leader).

    A node that is not in ``lvol.nodes`` gets the next window ABOVE every
    assigned one rather than index 0. The previous fallback collapsed every
    unknown node onto the primary's window, which is precisely the cntlid
    collision described on LVOL_CNTLID_WINDOW.
    """
    node_id = node.get_id()
    nodes = getattr(lvol, "nodes", None) or []
    if node_id in nodes:
        return nodes.index(node_id)
    # `nodes` may not be populated yet (early create, legacy record). The
    # leader is still identifiable, and it legitimately owns window 0.
    if node_id == getattr(lvol, "node_id", None):
        return 0
    fallback = max(len(nodes), 1)
    logger.error(
        "Node %s is not among lvol %s paths %s — assigning cntlid window "
        "%s to avoid colliding with an assigned path",
        node_id, lvol.get_id(), nodes, lvol_min_cntlid(fallback))
    return fallback


def _lvol_secondary_index(lvol, node):
    """Position of ``node`` among the lvol's non-leaders (0 = secondary,
    1 = tertiary) — determines the subsystem cntlid range (1000/2000)."""
    return max(_lvol_path_index(lvol, node) - 1, 0)


def add_lvol_on_node(lvol, snode, is_primary=True, secondary_index=0):
    rpc_client = snode.rpc_client()

    # Refuse to attach a new namespace to a shared subsystem while any
    # existing member is being migrated. The ANA flip in PHASE_LVOL_MIGRATE
    # is subsystem-wide, so a concurrently added sibling namespace would be
    # left inaccessible with no recovery path.
    if lvol.namespace:
        from simplyblock_core.controllers import migration_controller
        active_mig = migration_controller.get_active_migration_for_nqn(
            lvol.nqn, snode.cluster_id)
        if active_mig:
            return False, (
                f"Cannot attach lvol {lvol.uuid} to subsystem {lvol.nqn}: "
                f"a member of that subsystem has an active migration "
                f"{active_mig.uuid}. Retry after the migration completes."
            )

    ret, msg = _create_bdev_stack(lvol, snode, is_primary=is_primary)
    if not ret:
        return _fail_after_bdev(lvol, rpc_client, msg)

    db_controller = DBController()
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.has_qos():
        connect_lvol_to_pool(lvol.uuid, snode.get_id())

    try:
        resolve_subsys = _resolve_namespaced_subsystem(lvol, rpc_client, snode)
    except Exception as e:
        return _fail_after_bdev(lvol, rpc_client, str(e))

    if resolve_subsys:
        min_cntlid = lvol_min_cntlid(0 if is_primary else secondary_index + 1)
        allow_any = not bool(lvol.allowed_hosts)
        logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
        ret = rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                          max_namespaces=lvol.max_namespace_per_subsys,
                                          allow_any_host=allow_any)

        # add allowed hosts to subsystem
        if lvol.allowed_hosts:
            db_ctrl = DBController()
            cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
            pool = None # type: ignore[assignment]
            logger.info("[DHCHAP-DEBUG] add_lvol_on_node: lvol.pool_uuid=%s", lvol.pool_uuid)
            if lvol.pool_uuid:
                try:
                    pool = db_ctrl.get_pool_by_id(lvol.pool_uuid)
                    logger.info("[DHCHAP-DEBUG] add_lvol_on_node: pool found, "
                                "pool.dhchap=%s, pool.dhchap_key=%s, pool.dhchap_ctrlr_key=%s",
                                pool.dhchap, bool(pool.dhchap_key), bool(pool.dhchap_ctrlr_key))
                except KeyError:
                    logger.error("[DHCHAP-DEBUG] add_lvol_on_node: pool NOT FOUND for pool_uuid=%s",
                                 lvol.pool_uuid)
            else:
                logger.warning("[DHCHAP-DEBUG] add_lvol_on_node: lvol.pool_uuid is EMPTY — "
                               "DHCHAP target-side config will be SKIPPED")
            dhchap_group = _get_dhchap_group(cluster, pool)
            pool_key_names = {}
            if pool and pool.dhchap:
                logger.info("[DHCHAP-DEBUG] add_lvol_on_node: DHCHAP path — registering pool keys on node %s",
                            snode.get_id())
                pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
                logger.info("[DHCHAP-DEBUG] add_lvol_on_node: pool_key_names=%s", pool_key_names)
            else:
                logger.info("[DHCHAP-DEBUG] add_lvol_on_node: NON-DHCHAP path (pool=%s, pool.dhchap=%s)",
                            pool is not None, getattr(pool, 'dhchap', None))
            for host_entry in lvol.allowed_hosts:
                logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
                if pool and pool.dhchap:
                    logger.info("[DHCHAP-DEBUG] subsystem_add_host WITH dhchap_key=%s, dhchap_ctrlr_key=%s",
                                pool_key_names.get("dhchap_key"), pool_key_names.get("dhchap_ctrlr_key"))
                    rpc_client.subsystem_add_host(
                        lvol.nqn, host_entry["nqn"],
                        dhchap_key=pool_key_names.get("dhchap_key"),
                        dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                        dhchap_group=dhchap_group,
                    )
                else:
                    has_keys = any(host_entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
                    logger.info("[DHCHAP-DEBUG] subsystem_add_host WITHOUT pool DHCHAP (has_keys=%s, host_entry_keys=%s)",
                                has_keys, list(host_entry.keys()))
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
                        logger.warning("[DHCHAP-DEBUG] subsystem_add_host PLAIN — no DHCHAP keys at all")
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
                        return _fail_after_bdev(
                            lvol, rpc_client,
                            f"Failed to create listener for {lvol.get_id()}")
            elif iface.ip4_address and lvol.fabric == "tcp" and snode.active_tcp:
                logger.info("adding listener for %s on IP %s, fabric TCP port %s" % (lvol.nqn, iface.ip4_address, listener_port))
                ret, err = rpc_client.nvmf_subsystem_add_listener(
                        lvol.nqn, "TCP", iface.ip4_address, listener_port, ana_state)
                if not ret:
                    if err and "code" in err and err["code"] == -32602:
                        logger.warning("listener already exists")
                    else:
                        return _fail_after_bdev(
                            lvol, rpc_client,
                            f"Failed to create listener for {lvol.get_id()}")

    logger.info("Add BDev to subsystem")
    # Cluster-consistent namespace IDs: the PRIMARY add lets the target
    # auto-assign (nsid omitted) and persists the result in lvol.ns_id;
    # every REPLICA add must pass that exact nsid. Auto-assignment on
    # replicas gives each node its own arrival-order nsid map — under a
    # concurrent mass create the maps diverge (verified 2026-07-06:
    # 10/10 shared subsystems diverged across the three replicas, first
    # mismatch as early as nsid=2), and the client kernel then rejects
    # the namespaces ("IDs don't match for shared namespace N" /
    # "duplicate IDs in subsystem for nsid M"), leaving lvols without
    # block devices. A replica running before the primary assigned the
    # nsid (ns_id == 0, e.g. a drain-queued registration firing early)
    # must fail loudly instead of guessing.
    if is_primary:
        requested_nsid = None
    else:
        if not lvol.ns_id:
            return _fail_after_bdev(
                lvol, rpc_client,
                f"Replica namespace add for {lvol.get_id()} has no primary-"
                f"assigned ns_id; refusing auto-assignment (divergent nsid "
                f"maps across the shared subsystem's paths)")
        requested_nsid = lvol.ns_id
    ret, err = rpc_client.nvmf_subsystem_add_ns2(
        lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid, nsid=requested_nsid)
    if  err:
        if err and err["code"] == -32602 and lvol.namespace and lvol.node_id == snode.get_id():
            logger.info("Error adding namespace to subsystem, finding new subsystem for namespaced lvol")
            # Re-claim transactionally, excluding the subsystem SPDK just
            # rejected (the DB count said it had room — SPDK is the authority
            # on its own namespace table). The lvol's record is rewritten in
            # the same transaction, so its slot moves atomically from the
            # rejected subsystem to the new one (or to a standalone one).
            cluster = DBController().get_cluster_by_id(snode.cluster_id)
            try:
                DBController().claim_lvol_ns_slot(
                    lvol, snode, True,
                    standalone_nqn=cluster.nqn + ":lvol:" + lvol.uuid,
                    exclude_nqns={lvol.nqn})
            except SubsystemCapacityError as e:
                logger.error(str(e))
                return _fail_after_bdev(lvol, rpc_client, str(e))
            return add_lvol_on_node(lvol, snode, is_primary=is_primary, secondary_index=secondary_index)
        else:
            return _fail_after_bdev(
                lvol, rpc_client, "Failed to add bdev to subsystem")

    if is_primary:
        # Persist the target-assigned nsid; replicas re-add with exactly
        # this value, so it must never be overwritten by a replica's
        # (identical) response.
        lvol.ns_id = int(ret)

    if not is_primary:
        # Replica registration: the bdev was registered with the uuid/blobid
        # the primary already produced — the caller only checks for an error
        # and never reads this dict's fields, so skip the verification RPC and
        # echo back what was registered.
        return {'uuid': lvol.lvol_uuid,
                'driver_specific': {'lvol': {'blobid': lvol.blobid}}}, None

    ret = rpc_client.get_bdevs(f"{lvol.lvs_name}/{lvol.lvol_bdev}")
    if ret:
        lvol_bdev = ret[0]
        return lvol_bdev, None
    else:
        return False, "Failed to get lvol bdev"

def is_node_leader(snode, lvs_name):
    rpc_client = snode.rpc_client()
    ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
    if ret and len(ret) > 0 and "lvs leadership" in ret[0]:
        is_leader = ret[0]["lvs leadership"]
        return is_leader
    return False

def recreate_lvol_on_node(lvol, snode, ha_inode_self=None, ana_state=None):
    """Recreate ``lvol``'s subsystem/namespace/listener on ``snode``.

    ``ha_inode_self`` is the node's path index (0 = primary). It defaults to
    None — derived from ``lvol.nodes`` — rather than 0: a caller that omitted
    it used to silently place a secondary/tertiary path in the primary's
    cntlid window, and the host then rejected that path as a duplicate.
    """
    db_controller = DBController()
    rpc_client = snode.rpc_client()

    if ha_inode_self is None:
        ha_inode_self = _lvol_path_index(lvol, snode)

    if "crypto" in lvol.lvol_type:
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        ret = _create_crypto_lvol(rpc_client, lvol, cluster)
        if not ret:
            msg=f"Failed to create crypto lvol on node {snode.get_id()}"
            logger.error(msg)
            return False, msg

    # Same window as the create path — see lvol_min_cntlid(). This used to be
    # `1 + 1000 * ha_inode_self`, and ha_inode_self defaults to 0, so a
    # non-primary path recreated through this function landed in the
    # primary's window and the host rejected it as a duplicate cntlid.
    min_cntlid = lvol_min_cntlid(ha_inode_self)
    allow_any = not bool(lvol.allowed_hosts)
    logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
    rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                max_namespaces=lvol.max_namespace_per_subsys,
                                allow_any_host=allow_any)

    # Re-apply allowed hosts on subsystem recreate
    if lvol.allowed_hosts:
        db_ctrl = DBController()
        cluster = db_ctrl.get_cluster_by_id(snode.cluster_id)
        pool = None
        if lvol.pool_uuid:
            try:
                pool = db_ctrl.get_pool_by_id(lvol.pool_uuid)
            except KeyError:
                pass
        dhchap_group = _get_dhchap_group(cluster, pool)
        pool_key_names = {}
        if pool and pool.dhchap:
            pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
        for host_entry in lvol.allowed_hosts:
            logger.info("adding allowed host %s to subsystem %s", host_entry["nqn"], lvol.nqn)
            if pool and pool.dhchap:
                rpc_client.subsystem_add_host(
                    lvol.nqn, host_entry["nqn"],
                    dhchap_key=pool_key_names.get("dhchap_key"),
                    dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                    dhchap_group=dhchap_group,
                )
            else:
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
    # Recreate must present the SAME nsid as every other path of the shared
    # subsystem — pass the persisted primary-assigned value. Legacy records
    # created before ns_id persistence carry the model default; for those
    # (and dedicated one-namespace subsystems) the stored value is the
    # correct nsid as well. Only a record with ns_id unset falls back to
    # auto-assignment.
    ret = rpc_client.nvmf_subsystem_add_ns(
        lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid,
        nsid=lvol.ns_id if lvol.ns_id else None)
    if not ret:
        # FATAL, deliberately. This used to log and fall through to the
        # listener creation below, publishing a subsystem that accepts
        # connections but has no namespace behind it. That state is invisible
        # to every layer: `nvme connect` succeeds, the target establishes
        # qpairs, the client prints "new ctrl" — but the namespace never joins
        # the multipath head, so the path silently does not exist. The kernel
        # never resets the controller (nothing is wrong with it), and the CSI
        # repair loop cannot fix it because `nvme connect` returns "already
        # connected". Incident 2026-08-09: worker-3 carried 19 such subsystems
        # (71 listeners vs 52 namespaces); volume 638be965 ran at 2 of 3 paths
        # for 11 minutes and lost all I/O when the outage took the other two.
        # Failing here leaves no listener, which IS detectable and repairable.
        msg = (f"Failed to (re)add namespace for {lvol.get_id()} to {lvol.nqn} "
               f"with nsid={lvol.ns_id or 'auto'}; refusing to publish a "
               f"listener for a subsystem with no namespace")
        logger.error(msg)
        return False, msg

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


def _remove_bdev_stack(bdev_stack, rpc_client, sync=False):
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
        elif type == "crypto" and not sync:
            ret = rpc_client.lvol_crypto_delete(name)
            if ret:
                ret = rpc_client.lvol_crypto_key_delete(f'key_{name}')

        elif type == "bdev_lvstore":
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_lvol":
            name = bdev['params']["lvs_name"]+"/"+bdev['params']["name"]
            if not rpc_client.get_bdevs(name):
                # Already gone (e.g. the monitor's finish-phase re-issues the
                # leader delete after the async pass completed). Re-deleting
                # walks the snapshot/clone metadata a second time and errors
                # on every entry the first pass cleaned ("Clone entry not
                # found", 1382x in run mass_create_delete_docker-20260716) —
                # skip instead.
                logger.info(f"BDev {name} already deleted, skipping")
                bdev['status'] = 'deleted'
                continue
            ret, _ = rpc_client.delete_lvol(name, sync=sync)
        elif type == "bdev_lvol_clone":
            if not rpc_client.get_bdevs(name):
                logger.info(f"BDev {name} already deleted, skipping")
                bdev['status'] = 'deleted'
                continue
            ret, _ = rpc_client.delete_lvol(name,  sync=sync)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
    return True


def delete_lvol_from_node(lvol_id, node_id, clear_data=True, sync=False, force=False):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return True

    # Per design: gate sync deletes on non-leader nodes.
    from simplyblock_core.storage_node_ops import check_non_leader_for_operation
    if not force:
        action = check_non_leader_for_operation(node_id, lvol.lvs_name, operation_type="delete")
        if action == "skip":
            logger.info(f"Skipping sync delete of {lvol_id} on {node_id[:8]}: node disconnected")
            lvol.deletion_status = node_id
            lvol.write_to_db(db_controller.kv_store)
            return True
        elif action in ("queue", "retry"):
            # Durable deferral (DB task) — the in-memory drain queue is
            # per-process and lossy (incident 2026-07-10).
            tasks_controller.add_lvol_sync_del_task(
                snode.cluster_id, node_id,
                f"{lvol.lvs_name}/{lvol.lvol_bdev}", lvol.node_id)
            return True
    # action == "proceed" — execute now

    logger.info(f"Deleting LVol:{lvol.get_id()} from node:{snode.get_id()}")
    rpc_client = snode.rpc_client(timeout=5, retry=2)

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.has_qos():
        ret = rpc_client.bdev_lvol_remove_from_group(pool.numeric_id, [lvol.top_bdev])
        if not ret:
            logger.error("RPC failed bdev_lvol_remove_from_group")

    # 1- remove subsystem (no-op if the pre-leader phase already removed it).
    # Deleting the bdev stack under a namespace SPDK failed to remove is what
    # dropped every connection on the shared subsystem in the 2026-06-12
    # online-expand incident (CI 27398880537) — abort so the delete is
    # retried instead of leaving surviving namespaces without a device.
    if not _remove_lvol_subsys_from_node(lvol, rpc_client) and not force:
        logger.error(
            f"Namespace/subsystem removal not confirmed for {lvol.get_id()} "
            f"on {node_id[:8]}; aborting bdev delete")
        return False

    # 2- remove bdevs
    logger.info("Removing bdev stack")
    ret = _remove_bdev_stack(lvol.bdev_stack[::-1], rpc_client, sync)
    if not ret:
        return False

    lvol.deletion_status = node_id
    lvol.write_to_db(db_controller.kv_store)
    return True


# nvmf_subsystem_remove_ns is asynchronous inside SPDK: the RPC response can
# arrive before the deferred removal (nvmf_rpc_remove_ns_paused) runs, and
# that step can still fail with no error propagated back. In the 2026-06-12
# online-expand incident (CI 27398880537) the deferred failure fired 1-6s
# after the "success" and took down every connection on the shared subsystem.
# The poll bounds below cover that window.
NS_REMOVAL_CONFIRM_TIMEOUT = 10
NS_REMOVAL_CONFIRM_INTERVAL = 0.5


def _confirm_namespace_removed(rpc_client, nqn, nsid):
    """Poll *nqn* until namespace *nsid* is no longer listed.

    Returns ``(confirmed, subsystem)`` where *subsystem* is the last
    fetched state (None/empty if the subsystem itself is gone).
    """
    deadline = time.time() + NS_REMOVAL_CONFIRM_TIMEOUT
    while True:
        subsystem = rpc_client.subsystem_get(nqn)
        if not subsystem or all(ns["nsid"] != nsid for ns in subsystem["namespaces"]):
            return True, subsystem
        if time.time() >= deadline:
            return False, subsystem
        time.sleep(NS_REMOVAL_CONFIRM_INTERVAL)


def _remove_lvol_subsys_from_node(lvol, rpc_client):
    """Remove the lvol's NVMf subsystem from one node.

    Drops just the namespace if other namespaces still live on the
    subsystem; otherwise deletes the whole subsystem. Idempotent: if the
    subsystem is already gone, this is a no-op.

    Returns True on success or when there was nothing to do. Returns
    False if an RPC returned a non-success result, or if the namespace
    is still listed on the subsystem after the confirmation window —
    the remove RPC is async inside SPDK and its deferred step can fail
    without any error reaching us. Exceptions are NOT caught here — the
    caller decides whether a slow/hung node is fatal.
    """
    subsystem = rpc_client.subsystem_get(lvol.nqn)
    if not subsystem:
        return True

    for ns in subsystem["namespaces"]:
        if ns["uuid"] == lvol.uuid:
            logger.info("Removing namespace %s from subsystem %s", ns["uuid"], lvol.nqn)
            ret = bool(rpc_client.nvmf_subsystem_remove_ns(lvol.nqn, ns['nsid']))
            if not ret:
                logger.error(f"Failed to remove namespace {ns['nsid']} from subsystem {lvol.nqn}")
                return False
            confirmed, subsystem = _confirm_namespace_removed(rpc_client, lvol.nqn, ns['nsid'])
            if not confirmed:
                logger.error(
                    f"Namespace {ns['nsid']} still present on {lvol.nqn} "
                    f"{NS_REMOVAL_CONFIRM_TIMEOUT}s after nvmf_subsystem_remove_ns "
                    "returned success — deferred removal failed inside SPDK")
                return False
            break

    if not subsystem or len(subsystem["namespaces"]) == 0:
        # SHARED subsystems: delete-on-empty is only safe when no other live
        # volume claims this NQN. With namespaced volumes the subsystem is
        # legitimately empty in the WINDOW between one member's teardown and
        # the next member's add -- and a stuck in_deletion member's retry
        # loop observes that window sooner or later. Run 20260825_224221:
        # 8 of 20 namespaced fail-overs landed, then a looping rollback
        # record deleted the shared subsystem on the HA peer and every
        # following member's add_ns died -32602 on a missing subsystem.
        db_controller = DBController()
        others = [x for x in db_controller.get_lvols_by_node_id(lvol.node_id)
                  if x.nqn == lvol.nqn and x.get_id() != lvol.get_id()
                  and x.status not in (LVol.STATUS_DELETED,)
                  and not getattr(x, "deleted", False)]
        if others:
            logger.info(
                f"Leaving subsystem {lvol.nqn} in place: {len(others)} other "
                f"volume(s) still claim it (shared/namespaced subsystem)")
            return True
        logger.info(f"Removing subsystem {lvol.nqn}")
        return bool(rpc_client.subsystem_delete(lvol.nqn))

    return True


# --- inline async->sync delete -------------------------------------------
#
# The leader's async delete returns as soon as the request is registered; the
# cluster unmap and the snapshot/clone metadata walk finish in the background.
# The sync legs on the non-leaders must not start before it has completed, or
# they race the leader's walk ("operation sneaked in between async and sync
# delete"). Polling that completion here — instead of leaving the whole sync
# stage to lvol_monitor's next cycle — is what keeps a delete at sub-second
# latency: in run 20260807 the data-plane delete took 202s on average (max
# 675s) purely because the API returned after the async leg and the serial
# monitor loop drained the sync legs at 72 objects/min against a submit rate
# of 153/min, building a ~2200-object backlog. Whenever that backlog was
# empty the very same monitor finished a delete 0.1-0.2s after the async leg,
# so the cost was queueing, not per-object work.
#
# Cadence is front-loaded: the create-rollback path (_rollback_snapshot_bdev),
# which has always run this protocol inline, was measured in that run
# completing the poll ~39ms after the async delete (SNAP_1635: async 33.354,
# poll 33.393, sync legs 33.596/33.633 — 0.28s end to end). A flat 0.5s
# interval would hold the leader's lvstore lock an order of magnitude longer
# than the work needs, and that lock hold time is the throughput ceiling for
# deletes on a busy lvstore.
ASYNC_DELETE_POLL_INTERVALS = (0.02, 0.05, 0.1, 0.25)
ASYNC_DELETE_POLL_INTERVAL_MAX = 0.5
# Bounded, and deliberately much shorter than the 15s _rollback_snapshot_bdev
# allows: this runs on every delete, under the leader's lvstore lock, so the
# bound is a cap on how long one slow object may block the lvstore. On expiry
# nothing fails — the lvol stays in_deletion and lvol_monitor completes it
# exactly as it did before this fast path existed.
ASYNC_DELETE_POLL_TIMEOUT = 2.0
# completed / not found — same set _rollback_snapshot_bdev accepts
ASYNC_DELETE_DONE_STATUSES = (0, 2, -2, -19)


def _wait_async_delete(rpc_client, bdev_name) -> bool:
    """Poll the leader until its async delete of *bdev_name* has completed.

    Returns True if it completed within ``ASYNC_DELETE_POLL_TIMEOUT``. False
    means "not finished yet, or leadership moved" — the caller must then leave
    the object in_deletion for lvol_monitor rather than starting the sync
    legs, because a sync delete issued while the leader's metadata walk is
    still running is the interleaving this protocol exists to prevent.
    """
    deadline = time.time() + ASYNC_DELETE_POLL_TIMEOUT
    attempt = 0
    while True:
        try:
            st = rpc_client.bdev_lvol_get_lvol_delete_status(bdev_name)
        except Exception as e:
            logger.warning(f"delete-status poll for {bdev_name} failed: {e}")
            return False
        # Strict: the RPC layer returns None (not a status) when the call
        # itself failed, and `False == 0` in Python — so a bare membership
        # test against the "done" set would read a failed poll as "completed"
        # and release the sync legs while the leader may still be walking
        # metadata. Anything that is not a real int is a failed poll.
        if isinstance(st, bool) or not isinstance(st, int):
            logger.warning(
                f"delete-status poll for {bdev_name} returned {st!r}; "
                f"leaving the sync legs to lvol_monitor")
            return False
        if st in ASYNC_DELETE_DONE_STATUSES:
            return True
        # -35 (leadership changed) and 4 (no async request registered) are not
        # retried here: both mean this node is no longer the right target, and
        # the monitor's poll owns re-resolving that (it resets deletion_status
        # and re-issues on the then-current leader).
        if st not in (1,):
            logger.info(
                f"async delete of {bdev_name} returned status {st}; leaving "
                f"the sync legs to lvol_monitor")
            return False
        if time.time() >= deadline:
            logger.info(
                f"async delete of {bdev_name} still running after "
                f"{ASYNC_DELETE_POLL_TIMEOUT}s; leaving the sync legs to "
                f"lvol_monitor")
            return False
        if attempt < len(ASYNC_DELETE_POLL_INTERVALS):
            time.sleep(ASYNC_DELETE_POLL_INTERVALS[attempt])
        else:
            time.sleep(ASYNC_DELETE_POLL_INTERVAL_MAX)
        attempt += 1


def _delete_lvol_from_all_nodes(lvol, snode, force_delete, lock=True) -> None:
    """Remove the lvol from every node that hosts it (single, or ha
    leader + non-leaders), each single-node delete wrapped in the INNER
    per-lvstore lock so it never overlaps a create/register on that node.

    Called under the OUTER per-object lock held by ``delete_lvol``.
    """
    db_controller = DBController()
    # force_delete no longer BYPASSES the locks (it used to disable both,
    # so a recovery delete ran fully unlocked and could interleave with any
    # create/delete/resize on the same chain/node). It now takes them
    # best-effort: bounded wait, then proceed with a warning.
    _inner = lock
    _inner_kw = {
        "best_effort": force_delete,
        "timeout": constants.FORCE_DELETE_LOCK_WAIT_SEC if force_delete else None,
    }

    if lvol.ha_type == 'single':
        with snapshot_controller.lvstore_op_lock(
                snode.cluster_id, lvol.lvs_name, node_id=lvol.node_id, enabled=_inner, **_inner_kw):
            ret = delete_lvol_from_node(lvol.get_id(), lvol.node_id, force=force_delete)
        if not ret and not force_delete:
            raise RuntimeError("Failed to delete lvol from node")

    elif lvol.ha_type == "ha":
        from simplyblock_core.storage_node_ops import (
            check_non_leader_for_operation,
            execute_on_leader_with_failover,
        )

        host_node = db_controller.get_storage_node_by_id(snode.get_id())

        # Pre-leader subsystem teardown in fixed role order:
        # tertiary -> secondary -> primary. Skip any role whose node is
        # not ONLINE (down / in_restart / unreachable / etc). A single
        # 2-second wait lands after the primary's subsystem delete so
        # multipath clients fail the path away before the leader's bdev
        # stack disappears (the leader's bdev stack is removed by the
        # async delete below, which may target a different node than
        # the primary if the LVS has failed over).
        primary_subsys_deleted = False
        for role_label, role_id in (
            ("tertiary",  snode.tertiary_node_id),
            ("secondary", snode.secondary_node_id),
            ("primary",   host_node.get_id()),
        ):
            if not role_id:
                continue
            try:
                peer = db_controller.get_storage_node_by_id(role_id)
            except KeyError:
                continue
            if peer.status != StorageNode.STATUS_ONLINE:
                logger.info(
                    f"Skipping subsystem delete for {lvol.uuid} on "
                    f"{role_id[:8]} ({role_label}): status={peer.status}")
                continue
            try:
                peer_rpc = peer.rpc_client(timeout=5, retry=2)
                with snapshot_controller.lvstore_op_lock(
                        snode.cluster_id, lvol.lvs_name, node_id=role_id, enabled=_inner, **_inner_kw):
                    ok = _remove_lvol_subsys_from_node(lvol, peer_rpc)
                if ok:
                    logger.info(
                        f"Removed subsystem/ns for {lvol.uuid} on "
                        f"{role_id[:8]} ({role_label})")
                    if role_label == "primary":
                        primary_subsys_deleted = True
                else:
                    logger.warning(
                        f"Subsystem/namespace removal not confirmed on "
                        f"{role_id[:8]} ({role_label}); continuing")
            except Exception:
                logger.exception(
                    f"Exception during subsystem delete on "
                    f"{role_id[:8]} ({role_label})")

        if primary_subsys_deleted:
            time.sleep(1)

        all_sec_nodes = []
        for sec_id in lvol.nodes[1:]:
            try:
                all_sec_nodes.append(db_controller.get_storage_node_by_id(sec_id))
            except KeyError:
                pass
        all_nodes = [host_node] + all_sec_nodes

        # Step 1: Execute async delete on leader (with failover). Each leader
        # attempt is one single-node op, so it takes the inner lvstore lock.
        # The completion poll runs INSIDE that lock, so the leader's delete
        # window stays exclusive until the async pass has finished and no
        # other object's create/delete interleaves with it (same rationale as
        # snapshot_controller._rollback_snapshot_bdev).
        async_completed = {"done": False}

        def _delete_on_leader(leader):
            with snapshot_controller.lvstore_op_lock(
                    snode.cluster_id, lvol.lvs_name, node_id=leader.get_id(), enabled=_inner, **_inner_kw):
                ret = delete_lvol_from_node(lvol.get_id(), leader.get_id(), force=force_delete)
                if ret:
                    async_completed["done"] = _wait_async_delete(
                        leader.rpc_client(), f"{lvol.lvs_name}/{lvol.lvol_bdev}")
            return ret if ret else None

        success, actual_leader, result = execute_on_leader_with_failover(
            all_nodes, lvol.lvs_name, _delete_on_leader)
        if not success:
            msg = f"Failed to delete lvol from leader: {result}"
            if not force_delete:
                raise RuntimeError(msg)
            else:
                logger.warning(msg)

        # Step 2: Sync delete on non-leaders (leader op already completed).
        # Nodes whose sync leg completes here are recorded on the lvol so
        # lvol_monitor does not issue a second one when it finalises the
        # record.
        sync_done: list[str] = []
        non_leaders = [n for n in all_nodes if actual_leader and n.get_id() != actual_leader.get_id()]
        for nl in non_leaders:
            # Under the chain lock: same reasoning as the create path --
            # a sync leg handed to the task runner executes later, in
            # another process, holding only the per-node lock.
            action = check_non_leader_for_operation(
                nl.get_id(), lvol.lvs_name, operation_type="delete",
                leader_op_completed=True, all_nodes=all_nodes,
                wait_for_restart=constants.DEFERRED_LEG_RESTART_WAIT_SEC)
            if action == "skip":
                continue
            elif action in ("queue", "kill_and_wait"):
                # Durable deferral (DB task) — the in-memory drain queue is
                # per-process and lossy (incident 2026-07-10).
                tasks_controller.add_lvol_sync_del_task(
                    snode.cluster_id, nl.get_id(),
                    f"{lvol.lvs_name}/{lvol.lvol_bdev}", lvol.node_id)
            elif action == "proceed":
                ok = False
                synced = False
                try:
                    with snapshot_controller.lvstore_op_lock(
                            snode.cluster_id, lvol.lvs_name, node_id=nl.get_id(), enabled=_inner, **_inner_kw):
                        ok = _remove_lvol_subsys_from_node(lvol, nl.rpc_client())
                        if ok and async_completed["done"]:
                            # The sync leg this non-leader owes: it clears the
                            # peer's lvol REGISTRATION. (The leader's async
                            # delete only clears data clusters; the leader's
                            # own blob/bdev removal is the sync delete that
                            # lvol_monitor's finish phase issues.) Doing the
                            # peer legs here, rather than leaving the whole
                            # sync stage to lvol_monitor, is what keeps a
                            # delete at ~0.3s instead of minutes behind a
                            # drain backlog. -19 ("no such device") means this
                            # peer is already clean and counts as done.
                            ret, err = nl.rpc_client().delete_lvol(
                                f"{lvol.lvs_name}/{lvol.lvol_bdev}", sync=True)
                            synced = bool(ret) or bool(
                                err and err.get("code") == -19)
                except Exception as e:
                    # Includes a per-node lock acquisition timeout: the node
                    # can die while this op WAITS for the lock. Never abort
                    # the loop — the remaining non-leaders must still be
                    # processed; this node's delete is deferred durably.
                    logger.warning(f"Failed sync delete on {nl.get_id()}: {e}")
                if not ok or (async_completed["done"] and not synced):
                    # Either the subsystem/ns removal did not confirm, or the
                    # sync delete was attempted and failed. Defer durably.
                    tasks_controller.add_lvol_sync_del_task(
                        snode.cluster_id, nl.get_id(),
                        f"{lvol.lvs_name}/{lvol.lvol_bdev}", lvol.node_id)
                elif synced:
                    sync_done.append(nl.get_id())
                # else: the leader's async delete had not completed within the
                # poll bound, so no sync leg was issued here. Deliberately NO
                # durable task either — a sync delete must never be issued
                # while the leader's metadata walk is still running. The lvol
                # stays in_deletion and lvol_monitor owns the sync legs, which
                # is exactly the behaviour before this fast path existed.

        if sync_done:
            # Atomic: a plain read-modify-write here races lvol_monitor's own
            # updates to the same record and silently clobbers them.
            db_controller.atomic_update(
                db_controller.get_lvol_by_id(lvol.get_id()),
                lambda x: x.sync_deleted_nodes.extend(
                    n for n in sync_done if n not in x.sync_deleted_nodes))


def delete_lvol(lvol: LVol, *, force_delete: bool = False, lock: bool = True) -> None:
    db_controller = DBController()
    ops_gate.assert_object_ops_allowed("volume delete", pool_uuid=lvol.pool_uuid)

    # Block during restart Phase 5
    snode = None
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        if snode.lvstore_status == "in_creation" and not force_delete:
            raise PreconditionError(f"Cannot delete lvol {lvol.uuid}: node LVStore restart in progress")
    except KeyError:
        if not force_delete:
            raise PreconditionError(f"lvol node id not found: {lvol.node_id}")

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    if active_mig:
        raise PreconditionError(f"Cannot delete lvol {lvol.uuid}: active migration {active_mig.uuid}")

    if lvol.status == LVol.STATUS_RESTORING and not force_delete:
        raise PreconditionError(f"Cannot delete lvol {lvol.uuid}: backup restore in progress")

    if lvol.status == LVol.STATUS_DELETED:
        raise PreconditionError(f"lvol {lvol.uuid}: deleted already")

    if lvol.status == LVol.STATUS_IN_DELETION:
        logger.info(f"lvol:{lvol.get_id()} status is in deletion")
        if not force_delete:
            return

    logger.debug(lvol)
    if snode is None:
        logger.error(f"lvol node id not found: {lvol.node_id}")

        db_controller.release_lvol_ns_slot(lvol)

        # if lvol is clone and snapshot is deleted, then delete snapshot
        if lvol.cloned_from_snap:
            try:
                snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
                if snap.deleted is True:
                    lvols_count = sum(
                        1 for lv in db_controller.get_mini_lvols()
                        if lv.cloned_from_snap == snap.get_id()
                    )
                    if lvols_count == 0:
                        snapshot_controller.delete(snap.get_id())
            except KeyError:
                pass # already removed

        logger.info("Done")
        return

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        raise PreconditionError("Pool is disabled")

    # Refuse deletes while the cluster cannot complete them. The controller
    # only runs the leader-side async delete; the sync deletes on the
    # non-leaders and the record removal are driven by lvol_monitor, which
    # skips clusters in these states — accepting the delete here would
    # strand the lvol in_deletion with its teardown half done (2026-07-12
    # mass-delete run: 8.7k deletes accepted while the cluster was stuck
    # in_activation, none ever completed). read_only stays allowed: deletes
    # free space and are the way out of a capacity-critical cluster.
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if not force_delete and cluster.status in [
            Cluster.STATUS_SUSPENDED, Cluster.STATUS_IN_ACTIVATION,
            Cluster.STATUS_UNREADY, Cluster.STATUS_INACTIVE]:
        raise PreconditionError(
            f"Cannot delete lvol {lvol.uuid}: cluster {cluster.get_id()} "
            f"status is {cluster.status}")

    # Persist deletion intent BEFORE any data-plane RPC. If the leader-side
    # delete then times out or errors (for example: SPDK back-pressure on
    # the leader while a peer is being container-killed in an outage soak),
    # the lvol stays in_deletion and lvol_monitor's STATUS_IN_DELETION
    # reconcile path drives it to completion. Previously the status was set
    # only after a successful leader op, so a transient leader RPC failure
    # left the lvol in 'online' state with no record of the deletion intent
    # — the API returned results=False and no background process retried.
    if lvol.status != LVol.STATUS_IN_DELETION:
        old_status = lvol.status
        lvol.status = LVol.STATUS_IN_DELETION
        lvol.write_to_db(db_controller.kv_store)

        try:
            lvol_events.lvol_status_change(lvol, lvol.status, old_status)
        except KeyError:
            pass

    # OUTER per-object lock: serialize this lvol's whole delete sequence
    # across all nodes and exclude a concurrent resize/clone/delete of the
    # same lvol. The INNER lvstore_op_lock (inside the helper) wraps each
    # single-node delete RPC so it never overlaps a create/register on the
    # same node, which would corrupt the replica blob tree. force_delete
    # (recovery/cleanup) bypasses the lock and pushes through.
    with snapshot_controller.object_mutation_lock(
            snode.cluster_id, lvol.uuid, enabled=lock,
            best_effort=force_delete,
            timeout=constants.FORCE_DELETE_LOCK_WAIT_SEC if force_delete else None):
        _delete_lvol_from_all_nodes(lvol, snode, force_delete, lock=lock)

    # Status was already set to STATUS_IN_DELETION above, before the
    # data-plane RPC, so we just refresh the in-memory copy in case
    # delete_lvol_from_node updated other fields (e.g. deletion_status).
    lvol = db_controller.get_lvol_by_id(lvol.get_id())

    if lvol.cloned_from_snap and lvol.delete_snap_on_lvol_delete:
        logger.info(f"Deleting snap: {lvol.cloned_from_snap}")
        snapshot_controller.delete(lvol.cloned_from_snap)

    # if lvol is clone and snapshot is deleted, then delete snapshot
    elif lvol.cloned_from_snap:
        try:
            snap = db_controller.get_snapshot_by_id(lvol.cloned_from_snap)
            # Atomic decrement: a plain read-modify-write races a concurrent
            # clone-create's increment and loses one update, leaving ref_count
            # too high (snapshot leaks, never freed) or too low.
            if snap.snap_ref_id:
                ref_snap = db_controller.get_snapshot_by_id(snap.snap_ref_id)
                if ref_snap:
                    db_controller.atomic_update(ref_snap, lambda s: setattr(s, "ref_count", s.ref_count - 1))
            else:
                db_controller.atomic_update(snap, lambda s: setattr(s, "ref_count", s.ref_count - 1))
            if snap.deleted is True:
                snapshot_controller.delete(snap.get_id())
        except KeyError:
            pass # already deleted

    cl = db_controller.get_cluster_by_id(snode.cluster_id)

    if lvol.crypto_bdev:
        with create_kms_connection(cl) as kms:
            try:
                kms.delete_data_encryption_keys(lvol_dek_path(cl.get_id(), lvol.get_id()))
                logger.info("Deleted lvol key")
            except KMSException:
                logger.exception("Failed to delete lvol key")

    logger.info("Done")

def connect_lvol_to_pool(lvol_id, node_id):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False

    snode = db_controller.get_storage_node_by_id(node_id)
    rpc_client = snode.rpc_client()

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

    logger.info("Done")
    return True

def set_lvol(uuid, max_rw_iops, max_rw_mbytes, max_r_mbytes, max_w_mbytes, name=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(uuid)
    except KeyError as e:
        logger.error(e)
        return False
    ops_gate.assert_object_ops_allowed("volume parameter change",
                                       pool_uuid=lvol.pool_uuid)
    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        logger.error("Pool is disabled")
        return False
    if pool.has_qos():
        logger.info("Pool already has QOS settings")
        return False

    if name:
        lvol.lvol_name = name

    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    rpc_client = snode.rpc_client()

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
    if snode.tertiary_node_id:
        secondary_ids.append(snode.tertiary_node_id)
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


def list_lvols(cluster_id, pool_id_or_name, all=False):
    db_controller = DBController()
    lvols = []
    if cluster_id:
        lvols = db_controller.get_lvols(cluster_id)
    elif pool_id_or_name:
        try:
            pool = db_controller.get_pool_by_id_or_name(pool_id_or_name)
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

    for lvol in lvols:
        logger.debug(lvol)
        if lvol.deleted is True and all is False:
            continue
        size_used = 0
        records = db_controller.get_lvol_stats(lvol, 1)
        if records:
            size_used = records[0].size_used
        if lvol.ndcs == 0 and lvol.npcs == 0:
            cid = cluster_id
            if not cid and lvol.node_id:
                try:
                    cid = db_controller.get_storage_node_by_id(lvol.node_id).cluster_id
                except KeyError:
                    logger.warning(
                        "Storage node %s not found for lvol %s; "
                        "falling back to mode 0x0",
                        lvol.node_id, lvol.get_id(),
                    )
            cl = db_controller.get_cluster_by_id(cid) if cid else None
            mode = f"{cl.distr_ndcs}x{cl.distr_npcs}" if cl else "0x0"
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

    return data


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
    # Heterogeneous status payload (str / int / None / list). Annotated so the
    # numeric comparisons further down ("lag > lag_budget",
    # "outstanding_count > 0") are not inferred as int-vs-object.
    out: dict[str, Any] = {
        "last_snapshot_id": "",
        "last_replication_time": "",
        "last_replication_duration": "",
        "replicated_count": 0,
        # Replication progress monitoring.
        "lag_seconds": None,            # how far the target is behind the source
        "lag": "",                      # human-readable lag
        "outstanding_count": 0,         # snapshots queued but not yet replicated
        "outstanding_bytes": 0,         # bytes still to transfer
        "outstanding": "0B",            # human-readable outstanding bytes
        # Backlog. The configured interval is a TARGET, not a guarantee: a full
        # initial sync, a slow link or a large delta can all take longer than
        # one interval, and that is not by itself a fault. What matters
        # operationally is whether the backlog is being worked off, so the age
        # of the oldest thing still waiting is reported next to the target.
        "oldest_outstanding_seconds": None,   # age of the oldest unreplicated snapshot
        "oldest_outstanding": "",             # human-readable
        "cadence_target_seconds": 0,          # the configured interval
        "cadence_met": True,                  # backlog within one interval
        # Health verdict — replication errors live in per-task strings, which
        # nobody reads until a fail-over returns stale data. These summarise it.
        "state": "not_replicating",      # in_sync|replicating|lagging|degraded|error
        "healthy": False,
        "last_error": "",               # newest failing task's reason
        "failing_count": 0,             # tasks retrying right now
        "max_retry_reached": 0,         # tasks that gave up
        "snaps": [],
        "tasks": [],
    }
    node = db_controller.get_storage_node_by_id(lvol.node_id)
    # Each replication task maps 1:1 to a source snapshot for this lvol.
    items = []  # list of (task, snap)
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
            items.append((task, snap))

    if items:
        now = int(time.time())
        tasks = sorted(tasks, key=lambda x: x.date)
        snaps = sorted(snaps, key=lambda x: x.created_at)
        out["snaps"] = [s.to_dict() for s in snaps]
        out["tasks"] = [t.to_dict() for t in tasks]
        # A snapshot is replicated once its task is done or a counterpart exists
        # on the other side. BOTH directions count: fail-back records the copy
        # in source_replicated_snap_uuid and never sets the target one, so a
        # target-only test reported every failing-back volume as 0 replicated
        # and left lag_seconds None for ever — no gate on lag could ever pass.
        def _is_replicated(task, snap):
            return (task.status == JobSchedule.STATUS_DONE
                    or bool(snap.target_replicated_snap_uuid)
                    or bool(snap.source_replicated_snap_uuid))

        replicated = [s for (t, s) in items if _is_replicated(t, s)]
        outstanding = [s for (t, s) in items if not _is_replicated(t, s)]

        # Count what actually replicated, not every snapshot that has a task —
        # the latter reported healthy replication for volumes where nothing had
        # reached the target at all.
        out["replicated_count"] = len(replicated)

        outstanding_bytes = sum(s.used_size for s in outstanding)
        out["outstanding_count"] = len(outstanding)
        out["outstanding_bytes"] = outstanding_bytes
        out["outstanding"] = utils.humanbytes(outstanding_bytes)

        interval_sec = max(1, lvol.replication_interval_min or 1) * 60
        out["cadence_target_seconds"] = interval_sec
        if outstanding:
            oldest_outstanding = max(0, now - min(s.created_at for s in outstanding))
            out["oldest_outstanding_seconds"] = oldest_outstanding
            out["oldest_outstanding"] = utils.strfdelta_seconds(oldest_outstanding)
            # The interval is a target: one snapshot still in flight within its
            # own interval is the pipeline keeping up. Anything older than that
            # means the backlog is not being worked off at the requested rate.
            out["cadence_met"] = oldest_outstanding <= interval_sec

        # Time lag = age of the most recent point-in-time that exists on the
        # target (the newest successfully-replicated snapshot).
        if replicated:
            last_replicated_created = max(s.created_at for s in replicated)
            lag_seconds = max(0, now - last_replicated_created)
            out["lag_seconds"] = lag_seconds
            out["lag"] = utils.strfdelta_seconds(lag_seconds)

        last_task = tasks[-1]
        last_snap = db_controller.get_snapshot_by_id(last_task.function_params["snapshot_id"])
        out["last_snapshot_id"] = last_snap.get_id()
        out["last_replication_time"] = last_task.updated_at
        if "end_time" in last_task.function_params and "start_time" in last_task.function_params:
            duration = utils.strfdelta_seconds(
                last_task.function_params["end_time"] - last_task.function_params["start_time"])
        elif "start_time" in last_task.function_params:
            duration = utils.strfdelta_seconds(now - last_task.function_params["start_time"])
        else:
            duration = ""
        out["last_replication_duration"] = duration

        # --- health verdict -------------------------------------------------
        # A task that keeps retrying is the ONLY signal that replication is
        # broken (network partition, node down, no LVS leader). It used to be
        # buried in task.function_result, so a volume could sit hours behind
        # while every status view looked normal.
        failing = [t for t in tasks
                   if t.status == JobSchedule.STATUS_SUSPENDED and not t.canceled]
        gave_up = [t for t in tasks
                   if t.status == JobSchedule.STATUS_DONE
                   and str(t.function_result or "").startswith(("max retry", "task cancelled"))]
        out["failing_count"] = len(failing)
        out["max_retry_reached"] = len(gave_up)
        if failing:
            out["last_error"] = str(failing[-1].function_result or "")
        elif gave_up:
            out["last_error"] = str(gave_up[-1].function_result or "")

        # Lag budget: three snapshot intervals (one missed cycle is not an
        # incident), floor 5 min so a tiny interval does not flap the verdict.
        lag_budget = max(3 * interval_sec, 300)
        lag = out["lag_seconds"]
        oldest_outstanding = out["oldest_outstanding_seconds"]
        if gave_up:
            out["state"] = "error"
        elif failing:
            out["state"] = "degraded"
        elif lag is not None and lag > lag_budget:
            out["state"] = "lagging"
        elif oldest_outstanding is not None and oldest_outstanding > lag_budget:
            # A backlog older than the budget is lagging even when lag_seconds
            # says nothing — which is exactly the case that mattered: an initial
            # sync that never completes has NO replicated snapshot, so lag stays
            # None and the volume reported "replicating"/healthy indefinitely
            # while its transfers were stuck (lab 2026-08-20, case 4).
            out["state"] = "lagging"
        elif out["outstanding_count"] > 0:
            out["state"] = "replicating"
        else:
            out["state"] = "in_sync"
        out["healthy"] = out["state"] in ("in_sync", "replicating")
        out["lag_budget_seconds"] = lag_budget

    return out


def get_lvol(lvol_id_or_name):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id_or_name)
    except KeyError:
        try:
            lvol = db_controller.get_lvol_by_name(lvol_id_or_name)
        except KeyError:
            lvol = None

    if not lvol:
        logger.error(f"LVol id or name not found: {lvol_id_or_name}")
        return False

    data = lvol.get_clean_dict()

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    data['migrating'] = active_mig.uuid if active_mig else ""

    policy = db_controller.get_policy_for_lvol(lvol)
    data['policy'] = policy.policy_name if policy else ""

    return data


def connect_lvol(uuid, ctrl_loss_tmo=constants.LVOL_NVME_CONNECT_CTRL_LOSS_TMO, host_nqn=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(uuid)
        if lvol.status == LVol.STATUS_DELETED:
            raise KeyError(f"LVol {uuid} is deleted")
    except KeyError:
        logger.exception("Failed to get lvol by id: %s", uuid)
        return False, "Failed to find volume"

    try:
        host_entry = HostConnectAuth.resolve(lvol, host_nqn, db_controller)
    except ValueError as e:
        return False, str(e)

    out = []
    for path_lvol in _connect_path_volumes(db_controller, lvol):
        entries = _connect_entries_for_volume(
            db_controller, path_lvol, ctrl_loss_tmo, host_entry, host_nqn)
        clone_id = path_lvol.get_id()
        if clone_id != uuid:
            for entry in entries:
                entry.target_lvol_id = clone_id
        out.extend(entries)
    return out, None


def _connect_path_volumes(db_controller, lvol):
    """The volume(s) whose paths a client must connect, newest role first.

    Driven purely by the replication relationship — NEVER by Cluster.status. A
    source cluster that is merely assumed dead auto-recovers within minutes when
    its SPDK containers restart, so the old "if cluster is SUSPENDED, look for a
    copy with the same NQN" redirect stopped redirecting exactly when the volume
    was still living on the target. It also consulted the single cluster-scoped
    target field (wrong as soon as a cluster has several targets) and never fired
    for a planned migration, because there the source is healthy.

      replicating / none  -> the volume itself
      cutover_pending     -> BOTH sides: the client must already hold the target
                             paths when ANA flips, which is what makes a planned
                             cutover non-disruptive
      failed_over         -> the target copy, unconditionally
      cutover_done        -> ONLY the post-move volume; the pre-migration paths
                             are not handed out any more

    The clone preserves the source NQN and ns_id, so every path returned here
    aggregates into one multipath device on the client.
    """
    from simplyblock_core.models.lvol_model import LVolReplication

    lvol_id = lvol.get_id()
    rep = None
    for candidate in reversed(db_controller.get_lvol_replication_objects()):
        source_id = candidate.source_lvol.get_id() if candidate.source_lvol else ""
        target_id = candidate.target_lvol.get_id() if candidate.target_lvol else ""
        if lvol_id in (source_id, target_id):
            rep = candidate
            break

    if rep is None or rep.state == LVolReplication.STATE_REPLICATING:
        return [lvol]

    def _live(candidate):
        if candidate is None:
            return None
        try:
            fresh = db_controller.get_lvol_by_id(candidate.get_id())
        except KeyError:
            return None
        return None if fresh.status == LVol.STATUS_DELETED else fresh

    source = _live(rep.source_lvol)
    target = _live(rep.target_lvol)

    if rep.state == LVolReplication.STATE_CUTOVER_PENDING:
        return [v for v in (target, source) if v is not None] or [lvol]
    # failed_over and cutover_done: the volume now lives on the target side.
    return [target or source or lvol]


def _connect_entries_for_volume(db_controller, lvol, ctrl_loss_tmo, host_entry, host_nqn):
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

            out.append(build_nvme_connect_entry(
                transport=transport,
                ip=ip,
                port=port,
                nqn=lvol.nqn,
                ctrl_loss_tmo=ctrl_loss_tmo,
                cluster=cluster,
                host_entry=host_entry,
                host_nqn=host_nqn,
                ns_id=lvol.ns_id,
                allowed_hosts=[h["nqn"] for h in lvol.allowed_hosts] if lvol.allowed_hosts else [],
            ))
    return out


def _resize_lvol_on_all_nodes(lvol, snode, size_in_mib, lock=True) -> None:
    """Resize the lvol on every node that hosts it (single, or ha leader +
    secondaries), each single-node resize RPC wrapped in the INNER
    per-lvstore lock. Called under the OUTER per-object lock held by
    ``resize_lvol``."""
    db_controller = DBController()

    if lvol.ha_type == "single":
        rpc_client = snode.rpc_client()
        with snapshot_controller.lvstore_op_lock(snode.cluster_id, lvol.lvs_name,
                                                 node_id=snode.get_id(), enabled=lock):
            ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
        if not ret:
            raise RuntimeError(f"Error resizing lvol on node: {snode.get_id()}")

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

        from simplyblock_core.storage_node_ops import check_non_leader_for_operation

        # Detect current leader via RPC (no status checks)
        all_nodes = [host_node] + all_sec_nodes
        for candidate in all_nodes:
            try:
                if is_node_leader(candidate, lvol.lvs_name):
                    primary_node = candidate
                    break
            except Exception:
                continue
        if not primary_node:
            primary_node = host_node

        # Check non-leader nodes (no status checks)
        for candidate in all_nodes:
            if candidate.get_id() == primary_node.get_id():
                continue
            action = check_non_leader_for_operation(
                candidate.get_id(), lvol.lvs_name, operation_type="create")
            if action == "reject":
                raise RuntimeError(f"Cannot resize: non-leader {candidate.get_id()[:8]} unreachable but fabric healthy")
            elif action == "proceed":
                secondary_nodes.append(candidate)
            elif action == "queue":
                # Durable deferral (DB task) — the in-memory drain queue is
                # per-process and lossy (incident 2026-07-10). The task
                # converges the node to the lvol's CURRENT DB size.
                tasks_controller.add_lvol_sync_op_task(
                    snode.cluster_id, candidate.get_id(), lvol.get_id(), "resize")
            # "skip" — disconnected or pre_block, skip

        if primary_node:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {primary_node.get_id()}")
            rpc_client = primary_node.rpc_client()
            with snapshot_controller.lvstore_op_lock(snode.cluster_id, lvol.lvs_name,
                                                     node_id=primary_node.get_id(), enabled=lock):
                ret = rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            if not ret:
                raise RuntimeError(f"Error resizing lvol on node: {primary_node.get_id()}")

        for sec in secondary_nodes:
            logger.info(f"Resizing LVol: {lvol.get_id()} on node: {sec.get_id()}")
            try:
                sec_rpc_client = sec.rpc_client()
                with snapshot_controller.lvstore_op_lock(snode.cluster_id, lvol.lvs_name,
                                                         node_id=sec.get_id(), enabled=lock):
                    ret = sec_rpc_client.bdev_lvol_resize(f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib)
            except Exception as e:
                # Includes a per-node lock acquisition timeout: the node can
                # die while this op WAITS for the lock (the lock holder is
                # typically stuck RPC-ing the same dead node).
                ret = False
                logger.warning(f"Resize on non-leader {sec.get_id()} raised: {e}")
            if not ret:
                # The leader already resized — aborting here would leave the
                # replicas diverged AND skip the remaining non-leaders (the
                # tertiary must still be resized). Defer this node durably
                # and continue; resize_lvol persists the new size after this
                # returns, so the task converges to the right target.
                # WARNING, not ERROR: the resize still succeeds (leader
                # resized; this node converges via the sync-op task).
                logger.warning(
                    f"Error resizing lvol on non-leader {sec.get_id()}; "
                    f"deferring to sync-op task and continuing")
                tasks_controller.add_lvol_sync_op_task(
                    snode.cluster_id, sec.get_id(), lvol.get_id(), "resize")


def resize_lvol(id, new_size, lock=True) -> None:
    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(id)
    ops_gate.assert_object_ops_allowed("volume resize", pool_uuid=lvol.pool_uuid)

    # Block during restart Phase 5
    try:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        if snode.lvstore_status == "in_creation":
            raise PreconditionError(f"Cannot resize lvol {lvol.uuid}: node LVStore restart in progress")
    except KeyError:
        pass

    from simplyblock_core.controllers import migration_controller
    active_mig = migration_controller.get_active_migration_for_lvol(lvol.uuid)
    if active_mig:
        raise PreconditionError(f"Cannot resize lvol {lvol.uuid}: active migration {active_mig.uuid}")

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.status == Pool.STATUS_INACTIVE:
        raise PreconditionError(f"Pool is disabled {pool.get_id()}")

    # Resize grows the allocation, so it is gated like create: only an
    # operational cluster may take it (same allow-list as add_lvol_ha).
    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    if cluster.status not in Cluster.MUTABLE_STATUSES:
        raise PreconditionError(
            f"Cannot resize lvol {lvol.uuid}: cluster {cluster.get_id()} "
            f"status is {cluster.status}")

    if lvol.size == new_size:
        return  # Nothing to do
    elif lvol.size > new_size:
        raise PreconditionError(f"New size {new_size} must be larger than the original size {lvol.size}")

    if new_size > lvol.max_size:
        raise PreconditionError(f"New size {new_size} must not be larger than the max size {lvol.max_size}")

    if 0 < pool.lvol_max_size < new_size:
        raise PreconditionError(f"New size {new_size} must not be larger than the pool max size {pool.lvol_max_size}")

    if pool.pool_max_size > 0:
        # get_pool_total_capacity already includes THIS lvol's current
        # provisioned size, so subtract it before adding the new size.
        # `total + new_size` double-counted the volume (old + new) and
        # rejected legal resizes: a single 4G volume in a 10G pool could
        # not grow past 6G.
        total = pool_controller.get_pool_total_capacity(pool.get_id())
        total_after = total - lvol.size + new_size
        if total_after > pool.pool_max_size:
            raise PreconditionError(f"Invalid LVol size: {new_size}, Pool max size has reached {total_after} of {pool.pool_max_size}")

    snode = db_controller.get_storage_node_by_id(lvol.node_id)

    if snode.lvol_sync_del():
        logger.info(f"LVol sync delete task on node: {snode.get_id()}, proceeding with resize")

    logger.info(f"Resizing LVol: {lvol.get_id()}")
    logger.info(f"Current size: {utils.humanbytes(lvol.size)}, new size: {utils.humanbytes(new_size)}")

    size_in_mib = utils.convert_size(new_size, 'MiB')

    # OUTER per-object lock: serialize this lvol's whole resize sequence and
    # exclude a concurrent delete/clone/resize of the same lvol. The INNER
    # lvstore_op_lock (inside the helper) wraps each single-node resize RPC.
    with snapshot_controller.object_mutation_lock(snode.cluster_id, lvol.uuid, enabled=lock):
        _resize_lvol_on_all_nodes(lvol, snode, size_in_mib, lock=lock)

    lvol = db_controller.get_lvol_by_id(id)
    lvol.size = new_size
    lvol.write_to_db(db_controller.kv_store)
    logger.info("Done")


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

    rpc_client = snode.rpc_client()
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

def replication_start(lvol_id, replication_cluster_id=None, mode=None, interval_min=None,
                      from_policy=False):
    """Enable replication for a volume and pick its destination node.

    This is the step a replication POLICY performs for you: attaching a policy
    calls it with the target, cadence and mode taken from the policy. Calling it
    directly on a policy-managed volume is refused, because the volume would then
    replicate on settings that silently diverge from its policy. It stays
    available for volumes that are managed without a policy.
    """
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    if not from_policy and lvol.replication_policy_id:
        # Truthiness, NOT `is not None`: the field defaults to the empty
        # string, so an `is not None` test treats EVERY volume as
        # policy-managed and refuses all replication starts (commit
        # 95a35804a did exactly that and broke all six lab cases).
        logger.error("LVol %s follows replication policy %s; change the policy "
                     "instead of starting replication directly",
                     lvol_id, lvol.replication_policy_id)
        return False

    lvol.do_replicate = True
    if mode in ("failover", "migration"):
        lvol.replication_mode = mode
    if interval_min is not None and interval_min >= 0:
        lvol.replication_interval_min = interval_min
    # Persist do_replicate/mode/interval regardless of whether a replication
    # node still needs to be selected below.
    lvol.write_to_db()
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

    all_snaps = db_controller.get_snapshots()
    for snap in replication_backlog(db_controller, lvol, all_snaps):
        if not snap.target_replicated_snap_uuid:
            matched = False
            for sn in all_snaps:
                if sn.lvol.node_id == lvol.replication_node_id and sn.data_uuid == snap.data_uuid:
                    snap = db_controller.get_snapshot_by_id(snap.get_id())
                    snap.target_replicated_snap_uuid = sn.get_id()
                    snap.write_to_db()
                    matched = True
                    break
            if not matched:
                # Only snapshots of a volume that HAS a replication destination
                # can be replicated forward. replication_backlog walks the clone
                # ancestry across volumes, and on a failed-over volume that
                # ancestry runs into the target-side REP_* receiving volumes,
                # which exist only to receive a transfer and therefore carry
                # replication_node_id="" / do_replicate=False. Queueing them
                # produced tasks that could never resolve a destination node
                # (330 x "StorageNode lookup with a blank id" in lab 2026-08-19,
                # the same bug that used to surface as "Multiple values
                # present"), and their endless retries starved the replication
                # runner and wedged every volume delete behind them.
                if not snap.lvol.replication_node_id:
                    logger.debug("Skipping backlog snapshot %s: its volume %s has no "
                                 "replication destination", snap.get_id(), snap.lvol.get_id())
                    continue
                task = tasks_controller.add_snapshot_replication_task(snap.cluster_id, snap.lvol.node_id, snap.get_id())
                # task may be None if the scheduler is at capacity; the next poll cycle will retry
                if task:
                    snapshot_events.replication_task_created(snap)
    return True


def replication_backlog(db_controller, lvol, all_snaps=None, max_depth=64):
    """Every snapshot the volume's data depends on, oldest first.

    A volume's data lives in a blob chain: its own clusters, plus everything
    inherited from the snapshots below it. A volume sitting on a snapshot of
    another volume is no different structurally — so the backlog is the whole
    ancestor chain, not just the snapshots recorded against this volume's uuid.

    Queueing only ``snap.lvol.uuid == lvol.uuid`` happens to be complete for a
    volume that owns its whole chain, which is why plain migration works. For a
    volume that sits on someone else's snapshot (a failed-over volume is the
    common case) the ancestors were skipped, so a destination that does not
    already hold them receives the upper deltas with holes underneath.

    Only ancestors at or below each branch point are included: snapshots taken
    on an ancestor volume AFTER we branched off it are not part of our data.
    """
    if all_snaps is None:
        all_snaps = db_controller.get_snapshots()
    by_lvol: dict[str, list] = {}
    for s in all_snaps:
        by_lvol.setdefault(s.lvol.uuid, []).append(s)

    wanted = {}
    current = lvol
    cutoff = None          # None == no branch point yet: take all of ours
    seen_lvols = set()
    for _ in range(max_depth):
        if current is None or current.uuid in seen_lvols:
            break
        seen_lvols.add(current.uuid)
        for s in by_lvol.get(current.uuid, []):
            if cutoff is None or s.created_at <= cutoff:
                wanted[s.get_id()] = s
        parent_uuid = getattr(current, "cloned_from_snap", "")
        if not parent_uuid:
            break
        try:
            parent = db_controller.get_snapshot_by_id(parent_uuid)
        except KeyError:
            logger.warning("Chain of %s stops at missing snapshot %s",
                           lvol.get_id(), parent_uuid)
            break
        wanted[parent.get_id()] = parent      # the branch point itself
        cutoff = parent.created_at
        current = parent.lvol

    # Oldest first: the destination chains each arrival onto its predecessor.
    return sorted(wanted.values(), key=lambda s: s.created_at)


def list_by_node(node_id=None):
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
    return data


def clone_lvol(lvol_id, clone_name, new_size=None, pvc_name=None):
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError:
        logger.exception("Volume lookup failed for clone request: %s", lvol_id)
        return False, "Volume not found"
    if lvol.status != LVol.STATUS_ONLINE:
        logger.error(f"LVol: {lvol_id} is not online")
        return False, "LVol is not online"

    # host_node = db_controller.get_storage_node_by_id(lvol.node_id)
    # # clone_lvol always uses namespaced=True. Only enforce the subsystem limit
    # # if there is no existing subsystem with a free namespace slot.
    # if not get_next_available_subsystem_on_node(lvol.node_id):
    #     subsys_count = len(set(
    #         lv.nqn for lv in db_controller.get_lvols_by_node_id(lvol.node_id)
    #         if lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED]
    #     ))
    #     if subsys_count >= host_node.max_lvol:
    #         error = f"Too many subsystems on node: {host_node.get_id()}, max subsystems reached: {host_node.max_lvol}"
    #         logger.error(error)
    #         return False, error

    all_lvols = db_controller.get_mini_lvols()
    all_snaps = db_controller.get_mini_snapshots()

    # Resolve the namespace slot early so we can (a) skip the subsystem limit
    # check when the clone fits into an existing subsystem, and (b) reuse the
    # result below instead of calling get_next_available_subsystem_on_node twice.
    _available_subsys = get_next_available_subsystem_on_node(lvol.node_id, all_lvols=all_lvols)

    if not _available_subsys:
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        subsys_count = count_lvol_subsystems(snode, all_lvols)
        if subsys_count >= max_subsystems_for_node(snode):
            error = (f"Too many subsystems on node: {snode.get_id()}, "
                     f"max subsystems reached: {max_subsystems_for_node(snode)}")
            logger.error(error)
            return False, error

    snapshot_uuid = None
    for snap in all_snaps:
        if snap.snap_name == clone_name and snap.lvol.node_id == lvol.node_id:
            logger.info(f"Snapshot with name {clone_name} already exists for this LVol: {snap.uuid}, using it for cloning")
            snapshot_uuid = snap.uuid
            break

    if not snapshot_uuid:
        snapshot_uuid, err = snapshot_controller.add(lvol_id, clone_name, lock=False, all_snaps=all_snaps, all_lvols=all_lvols)
        if err:
            logger.error(err)
            return False, str(err)
    new_lvol_uuid, err = snapshot_controller.clone(
        snapshot_uuid, clone_name, new_size, pvc_name, delete_snap_on_lvol_delete=True, lock=False, namespaced=True, all_snaps=all_snaps, all_lvols=all_lvols)
    if err:
        logger.error(err)
        if snapshot_uuid:
                snapshot_controller.delete(snapshot_uuid)
        return False, str(err)

    return new_lvol_uuid, False



def replication_stop(lvol_id, delete=False, from_policy=False):
    """Stop replicating a volume.

    Detaching a replication POLICY does this for you, and additionally cleans up
    the internal replication snapshots on both sides. Calling it directly on a
    policy-managed volume is refused: the volume would stop replicating while
    still claiming to follow a policy.
    """
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    if not from_policy and lvol.replication_policy_id:
        # Truthiness, NOT `is not None`: the field defaults to the empty
        # string, so an `is not None` test treats EVERY volume as
        # policy-managed and refuses all replication starts (commit
        # 95a35804a did exactly that and broke all six lab cases).
        logger.error("LVol %s follows replication policy %s; detach the policy "
                     "instead of stopping replication directly",
                     lvol_id, lvol.replication_policy_id)
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


def _create_target_lvol_clone(db_controller, lvol, target_node, pool_uuid, snapshot):
    """Create a writable clone of *lvol* on *target_node* (primary + online HA
    peers) from *snapshot*, preserving the original NQN/ns_id.

    Shared by fail-over (replicate_lvol_on_target_cluster) and migration-commit
    (replication_commit). Returns (new_lvol, error). The new lvol is left in
    STATUS_IN_CREATION with lvol_uuid/blobid populated; the caller is
    responsible for setting STATUS_ONLINE and any replication bookkeeping.
    """
    new_lvol = copy.deepcopy(lvol)
    new_lvol.uuid = str(uuid.uuid4())
    new_lvol.create_dt = str(datetime.now())
    new_lvol.node_id = target_node.get_id()
    new_lvol.nodes = [target_node.get_id()]
    if target_node.secondary_node_id:
        new_lvol.nodes.append(target_node.secondary_node_id)
    if target_node.tertiary_node_id:
        new_lvol.nodes.append(target_node.tertiary_node_id)
    new_lvol.replication_node_id = ""
    new_lvol.do_replicate = False
    # The policy belongs to the SOURCE cluster; a deep copy would carry its id to
    # the other cluster, where it names nothing and would block fail-back.
    new_lvol.replication_policy_id = ""
    new_lvol.cloned_from_snap = snapshot.get_id()
    new_lvol.pool_uuid = pool_uuid
    new_lvol.lvs_name = target_node.lvstore
    new_lvol.top_bdev = f"{new_lvol.lvs_name}/{new_lvol.lvol_bdev}"
    new_lvol.snapshot_name = snapshot.snap_bdev
    new_lvol.status = LVol.STATUS_IN_CREATION
    # Preserve the ORIGINAL subsystem NQN and namespace id: the client must
    # reconnect to the SAME NQN/NS on the target cluster — only the IP/port
    # differ. new_lvol is a deep copy of lvol, so nqn/ns_id are already
    # identical; do NOT rewrite the NQN with the target cluster's prefix.

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
        new_lvol.bdev_stack.append({"type": "crypto"})

    new_lvol.write_to_db(db_controller.kv_store)

    _evict_stale_namespace(new_lvol, target_node)

    lvol_bdev, error = add_lvol_on_node(new_lvol, target_node)
    if error:
        logger.error(error)
        db_controller.release_lvol_ns_slot(new_lvol)
        return None, error

    new_lvol.lvol_uuid = lvol_bdev['uuid']
    new_lvol.blobid = lvol_bdev['driver_specific']['lvol']['blobid']

    # Expose the volume on the secondary and tertiary target nodes too (HA),
    # so connect_lvol returns all client paths.
    for peer_id in [target_node.secondary_node_id, target_node.tertiary_node_id]:
        if not peer_id:
            continue
        try:
            peer_node = db_controller.get_storage_node_by_id(peer_id)
        except KeyError:
            continue
        if peer_node.status != StorageNode.STATUS_ONLINE:
            continue
        # The preserved-NQN subsystem exists on EVERY node of the recovered
        # HA set, each still holding the original volume's namespace at the
        # preserved nsid. Evicting only on the primary made its add_ns
        # succeed while the peer's failed with the same -32602, and the
        # peer failure rolled the whole cutover back (run 20260824_113711:
        # primary add_ns result:1, peer -32602, 0/5 cutovers).
        _evict_stale_namespace(new_lvol, peer_node)
        lvol_bdev, error = add_lvol_on_node(new_lvol, peer_node, is_primary=False)
        if error:
            logger.error(error)
            # remove lvol from primary
            ret = delete_lvol_from_node(new_lvol, target_node)
            if not ret:
                logger.error("")
            db_controller.release_lvol_ns_slot(new_lvol)
            return None, error

    return new_lvol, None


def _last_replicated_target_snapshot(db_controller, lvol_id, cluster_id, generation=0):
    """Return the target-cluster copy of the most recent FULLY replicated
    snapshot of *lvol_id*, or None.

    A fail-over point must be a snapshot whose data actually reached the target:

      * the replication task must be STATUS_DONE. A target snapshot record is
        created before/while the data is transferred, so
        ``target_replicated_snap_uuid`` being set proves only that the copy was
        allocated -- not that it holds any data. Selecting such a snapshot yields
        a clone that reads as zeros.
      * the target copy must not be in deletion. Retention deletes replicated
        snapshots, and cloning from one that is going away leaves the failed-over
        volume with an orphaned parent (again: reads as zeros).

    Candidates are walked newest-first so an incomplete or disappearing newest
    snapshot falls back to the last good one rather than failing the fail-over.
    """
    snaps = []
    for task in db_controller.get_job_tasks(cluster_id):
        if task.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
            continue
        if task.status != JobSchedule.STATUS_DONE:
            continue
        try:
            snap = db_controller.get_snapshot_by_id(task.function_params["snapshot_id"])
        except KeyError:
            continue
        if snap.lvol.get_id() != lvol_id or not snap.target_replicated_snap_uuid:
            continue
        snaps.append(snap)

    snaps.sort(key=lambda x: x.created_at, reverse=True)
    # generation 0 = newest replicated point-in-time (the default and the only
    # behaviour before tiered retention existed). A higher generation walks
    # BACK through the retained history, which is what a retention schedule is
    # for: recovering to a point before a logical corruption that a
    # minute-old copy would have replicated faithfully.
    if generation:
        if generation >= len(snaps):
            logger.error(
                f"Fail-over generation {generation} requested for {lvol_id} but only "
                f"{len(snaps)} replicated point(s)-in-time exist")
            return None
        snaps = snaps[generation:]
    for snap in snaps:
        try:
            target_snap = db_controller.get_snapshot_by_id(snap.target_replicated_snap_uuid)
        except KeyError:
            logger.warning(
                f"Fail-over candidate {snap.get_id()}: target copy "
                f"{snap.target_replicated_snap_uuid} is gone; trying an older snapshot")
            continue
        if target_snap.status == SnapShot.STATUS_IN_DELETION:
            logger.warning(
                f"Fail-over candidate {snap.get_id()}: target copy "
                f"{target_snap.get_id()} is in deletion; trying an older snapshot")
            continue
        return target_snap
    return None


def _evict_stale_namespace(new_lvol, target_node):
    """Make room for the preserved identity on a RECOVERED fail-back target.

    The cutover clone keeps the ORIGINAL volume's NQN and nsid so the client
    reconnects to the same identity. Failing back to a recovered source means
    that subsystem usually still exists there WITH the original volume's
    namespace at exactly that nsid -- and its data is outdated by definition
    (the other cluster served every write since the fail-over). add_ns then
    fails with -32602 for every retry, and the whole cutover dies on max
    retry: 2026-08-24, 5/5 fail-back cutovers, 40x "Failed to add bdev to
    subsystem". Evict a namespace occupying the clone's nsid unless it is
    already the clone's own bdev (idempotent re-run).
    """
    try:
        rpc = target_node.rpc_client()
        # subsystem_get returns ONE subsystem dict (single_or_none), not a
        # list -- indexing it with [0] raised KeyError(0), the best-effort
        # except swallowed it, and the eviction silently never ran (run
        # 20260824_104449: same 40x add_ns -32602 with the fix "in place").
        subsystem = rpc.subsystem_get(new_lvol.nqn)
        if not subsystem:
            return
        # Match by nsid OR by uuid: the stale namespace carries the volume's
        # preserved identity on both axes, and either collides with add_ns.
        stale = [ns for ns in (subsystem.get("namespaces") or [])
                 if (ns.get("nsid") == new_lvol.ns_id
                     or ns.get("uuid") == new_lvol.uuid)
                 and ns.get("bdev_name") != new_lvol.top_bdev]
        if not stale:
            return
        for ns in stale:
            logger.info(
                f"Fail-back cutover: evicting stale namespace nsid={ns.get('nsid')} "
                f"(bdev {ns.get('bdev_name')}) from {new_lvol.nqn} on "
                f"{target_node.get_id()} -- superseded by the failed-over data")
            rpc.nvmf_subsystem_remove_ns(new_lvol.nqn, ns.get("nsid"))
        # remove_ns ACKNOWLEDGES before it completes (the same async
        # false-success that dropped a shared subsystem in the PVC-expand
        # incident; its fix polls for confirmation, eb127eed). Without this
        # poll the follow-up add_ns raced the removal and lost on all 8
        # retries (run 20260824_110959: 40 evictions logged, 40 add_ns
        # -32602 right behind them).
        stale_ids = {ns.get("nsid") for ns in stale}
        deadline = time.time() + 20
        while time.time() < deadline:
            current = rpc.subsystem_get(new_lvol.nqn) or {}
            if not any(ns.get("nsid") in stale_ids
                       for ns in (current.get("namespaces") or [])):
                return
            time.sleep(1)
        logger.error(
            f"Stale namespace(s) {sorted(stale_ids)} on {new_lvol.nqn} did not "
            f"disappear within 20s of removal on {target_node.get_id()}; the "
            f"cutover's add_ns will fail and retry")
    except Exception as e:
        # Best effort: if the subsystem is not there, add_lvol_on_node creates
        # it; if the eviction genuinely failed, add_ns will say so loudly.
        logger.warning(f"Stale-namespace check on {target_node.get_id()} for "
                       f"{new_lvol.nqn} raised: {e}")


def _clone_from_last_replicated(db_controller, lvol_id, lvol, target_node, pool_uuid,
                                cluster_id, attempts=3, generation=0):
    """Pick the last fully replicated target snapshot and clone from it ATOMICALLY.

    Selecting and then cloning as two unsynchronised steps loses the data: the
    replication retention pass (_prune_internal_snapshots) deletes every
    replicated internal snapshot older than the newest, and
    snapshot_controller._delete_locked only spares a snapshot that ALREADY has a
    clone. In the window between "pick T_n" and "create the clone on T_n" there
    is no clone yet, so retention hard-deletes T_n and the fail-over volume ends
    up on a parent that no longer exists -- it reads as all zeros, with no
    filesystem, while every status field still says success.

    snapshot_controller.delete() takes object_mutation_lock(snapshot) precisely
    so a "concurrent clone-create of this same snapshot ... holds the same lock
    for its whole sequence"; the normal clone path honours that, this one did
    not. Hold the lock across the clone, re-validate under it, and fall back to
    an older snapshot if the chosen one disappeared before we got the lock.

    Returns (new_lvol, snapshot_used, error).
    """
    for _ in range(attempts):
        snapshot = _last_replicated_target_snapshot(db_controller, lvol_id, cluster_id,
                                                    generation=generation)
        if not snapshot:
            return None, None, (
                f"No replicated snapshot on target for generation {generation}"
                if generation else "No replicated snapshot on target yet")

        with snapshot_controller.object_mutation_lock(snapshot.cluster_id, snapshot.uuid):
            # Re-read INSIDE the lock: retention may have removed or started
            # removing it between the selection above and acquiring the lock.
            try:
                snap = db_controller.get_snapshot_by_id(snapshot.get_id())
            except KeyError:
                logger.warning(
                    f"Replicated snapshot {snapshot.get_id()} vanished before the clone "
                    f"could take its lock; re-selecting")
                continue
            if snap.status == SnapShot.STATUS_IN_DELETION or getattr(snap, "deleted", False):
                logger.warning(
                    f"Replicated snapshot {snap.get_id()} entered deletion before the clone "
                    f"could take its lock; re-selecting")
                continue
            new_lvol, error = _create_target_lvol_clone(
                db_controller, lvol, target_node, pool_uuid, snap)
            return new_lvol, snap, error

    return None, None, "No stable replicated snapshot to clone from"


def resolve_replication_destination(db_controller, lvol, target_node, source_node):
    """Where a fail-over or cutover copy of *lvol* belongs: (cluster, pool uuid).

    The destination CLUSTER is the one hosting the node the volume replicates
    to. Reading it from source_cluster.snapshot_replication_target_cluster
    instead only ever worked in the forward direction: a policy never writes
    that field, and on a fail-back the volume's source cluster is the one that
    was the target, whose field points somewhere else entirely (or nowhere).

    The POOL comes from the replication target the volume's policy names, when
    that target is this destination; otherwise from the source cluster's
    configured pool if that config is about this destination; otherwise the
    destination's own first ACTIVE pool.
    """
    target_cluster = db_controller.get_cluster_by_id(target_node.cluster_id)

    policy = db_controller.get_replication_policy_for_lvol(lvol)
    if policy is not None:
        try:
            target = db_controller.get_replication_target_by_id(policy.target_id)
        except KeyError:
            target = None
        if (target is not None and target.target_pool_uuid
                and target.target_cluster_id == target_node.cluster_id):
            return target_cluster, target.target_pool_uuid

    source_cluster = db_controller.get_cluster_by_id(source_node.cluster_id)
    if (source_cluster.snapshot_replication_target_cluster == target_node.cluster_id
            and source_cluster.snapshot_replication_target_pool):
        return target_cluster, source_cluster.snapshot_replication_target_pool

    for pool in db_controller.get_pools(target_node.cluster_id):
        if pool.status == Pool.STATUS_ACTIVE:
            return target_cluster, pool.get_id()
    return target_cluster, ""


def replicate_lvol_on_target_cluster(lvol_id, generation=0):
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
    target_cluster, target_pool_uuid = resolve_replication_destination(
        db_controller, lvol, target_node, source_node)

    # Match the preserved identity in FULL: nqn AND nsid. A namespaced volume
    # SHARES its subsystem with up to max-namespace-per-subsys siblings, so an
    # nqn-only test made this idempotency guard fire for every namespace after
    # the first: namespace 1's fail-over copy already carried the nqn, so
    # namespaces 2..N returned ITS id and were never failed over at all —
    # silent loss of 9 of 10 volumes in a DR event, reported as success
    # (soak case 7, run 20260824_174611).
    for lv in db_controller.get_lvols(target_cluster.get_id()):
        if lv.nqn == lvol.nqn and lv.ns_id == lvol.ns_id:
            logger.info(f"LVol with same nqn+nsid already exists on target cluster: {lv.get_id()}")
            return lv.get_id()

    new_lvol, _snapshot, error = _clone_from_last_replicated(
        db_controller, lvol_id, lvol, target_node,
        target_pool_uuid, source_node.cluster_id, generation=generation)
    if error:
        logger.error(f"Fail-over clone failed for lvol {lvol_id}: {error}")
        return False, error

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.write_to_db(db_controller.kv_store)

    # Stop replicating FROM the source we just failed away from, BEFORE the
    # relationship is recorded.
    #
    # Fail-over left do_replicate=True and the pending FN_SNAPSHOT_REPLICATION
    # tasks queued. The source cluster is only assumed dead: it auto-recovers
    # (SPDK containers restart within minutes), those tasks then finish, and each
    # completion runs snapshot_replication._prune_internal_snapshots for the
    # source volume. Retention keeps only the newest replicated internal snapshot
    # and deletes the TARGET copies of the older ones — including the snapshot
    # this fail-over volume was just cloned from. That delete goes to SPDK as
    # bdev_lvol_delete(sync=False), which frees the blocks immediately, so the
    # failed-over volume starts reading zeros ~90s after a successful fail-over:
    # no filesystem, md5 mismatch, while every status field still says online
    # (labs 2026-08-10 / 2026-08-11, validated in the spdk_proxy RPC logs).
    #
    # A failed-over volume no longer lives on the source, so there is nothing
    # left to replicate from it; any further source delta is by definition past
    # the RPO the fail-over accepted.
    replication_stop(lvol_id, from_policy=True)

    lvol = db_controller.get_lvol_by_id(lvol_id)
    lvol.from_source = False
    lvol.write_to_db()

    lvol_replication = LVolReplication()
    lvol_replication.uuid = str(uuid.uuid4())
    lvol_replication.create_dt = str(datetime.now())
    lvol_replication.source_lvol = lvol
    lvol_replication.target_lvol = new_lvol
    lvol_replication.source_cluster_id = source_node.cluster_id
    lvol_replication.target_cluster_id = target_cluster.get_id()
    lvol_replication.mode = lvol.replication_mode
    lvol_replication.state = LVolReplication.STATE_FAILED_OVER
    lvol_replication.direction = LVolReplication.DIRECTION_TO_TARGET
    lvol_replication.target_nqn = new_lvol.nqn
    lvol_replication.target_ns_id = new_lvol.ns_id
    lvol_replication.write_to_db(db_controller.kv_store)

    lvol_events.lvol_replicated(lvol, new_lvol)

    # Provide the new connection paths (primary/secondary/tertiary) — identical
    # NQN, different IP/port — so the client can fail over to the target cluster.
    connection_strings = []
    conn, conn_err = connect_lvol(new_lvol.get_id())
    if conn_err:
        logger.warning(f"Fail-over lvol created but connection-string build failed: {conn_err}")
    else:
        connection_strings = [c.model_dump(by_alias=True) for c in conn]

    return {
        "lvol_id": new_lvol.uuid,
        "nqn": new_lvol.nqn,
        "ns_id": new_lvol.ns_id,
        "connection_strings": connection_strings,
    }


def _resolve_target_map_id(target_node, lvol_bdev):
    """Look up the map_id of *lvol_bdev* on *target_node*'s lvstore."""
    composite = f"{target_node.lvstore}/{lvol_bdev}"
    lvols_list = target_node.rpc_client().bdev_lvol_get_lvols(target_node.lvstore) or []
    for entry in lvols_list:
        entry_name = entry.get('name', '') or entry.get('lvol_name', '')
        if entry_name in (lvol_bdev, composite):
            return entry.get('map_id')
    return None


def replication_commit(lvol_id, delete_source=False):
    """Planned cutover for migration mode (and the final step of fail-back).

    Enqueues the FN_REPLICATION_FINAL task, which performs an iterative
    delta-shrink before the freeze: snapshot -> wait until replicated+converted
    on the target -> snapshot again (the delta now covers only the wait
    window) -> wait again -> IMMEDIATELY build the target clone on the last
    replicated snapshot and run the final step. Two shrink rounds bound the
    freeze-window residual to seconds of writes, and — unlike the previous
    single fire-and-forget pre-snapshot — guarantee the cutover base actually
    REACHED the target: the old flow selected a base 1-2 intervals old and
    froze without waiting, silently losing the writes in between.

    The first shrink snapshot is taken here so the pipeline starts immediately;
    the runner owns the rest. Returns a dict describing the queued cutover, or
    (False, error).
    """
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
    if target_node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Target node is not online: {lvol.replication_node_id}")
        return False

    source_node = db_controller.get_storage_node_by_id(lvol.node_id)

    # Shrink round 1: freeze the current top delta and let the normal
    # replication pipeline carry it (snapshot_controller.add auto-enqueues the
    # replication task for do_replicate volumes).
    snap_uuid, snap_err = snapshot_controller.add(
        lvol_id, f"repl_commit_{uuid.uuid4()}", snap_type=SnapShot.TYPE_INTERNAL)
    if snap_err:
        logger.error(f"Shrink snapshot failed: {snap_err}")
        return False, f"Shrink snapshot failed: {snap_err}"

    task = tasks_controller.add_replication_final_task(
        source_node.cluster_id, source_node.get_id(),
        {
            "lvol_id": lvol_id,
            "src_node_id": source_node.get_id(),
            "tgt_node_id": target_node.get_id(),
            "operation": "replicate",
            "final_state": LVolReplication.STATE_CUTOVER_DONE,
            "shrink_round": 1,
            "shrink_snap_id": snap_uuid,
            "shrink_deadline": int(time.time()) + constants.REPL_CUTOVER_SHRINK_TIMEOUT_SEC,
            # Migration semantics on request: retire the source volume once
            # the cutover state is durable (see _finalize in the final runner).
            "delete_source": bool(delete_source),
        })
    if not task:
        logger.error("Failed to enqueue replication-final task")
        return False, "Failed to enqueue cutover task"

    return {"cutover_task_queued": True, "task_id": task}


def replication_failback(lvol_id, source_cluster_id=None, pool_uuid=None):
    """Configure fail-back of a failed-over volume back to a source cluster.

    The actual cutover is then performed with replication_commit — fail-back and
    the migration final step are the same operation. Two cases:

      * Recovered source — ``source_cluster_id`` omitted or equal to the original
        source cluster. Replication is pointed at the ORIGINAL source node so the
        backlog match links the snapshots already present there (by data_uuid):
        only the DELTA by which the target advanced is replicated.

      * Fresh source — a different ``source_cluster_id`` is given. A node is
        selected in that cluster and the full dataset is replicated.
    """
    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    # Locate the fail-over relationship for this (target-resident) volume.
    rep = None
    for r in reversed(db_controller.get_lvol_replication_objects()):
        if r.target_lvol and r.target_lvol.get_id() == lvol_id:
            rep = r
            break

    if rep is not None and (not source_cluster_id or source_cluster_id == rep.source_cluster_id):
        orig_node_id = rep.source_lvol.node_id
        try:
            orig_node = db_controller.get_storage_node_by_id(orig_node_id)
        except KeyError:
            orig_node = None
        if orig_node is None or orig_node.status != StorageNode.STATUS_ONLINE:
            logger.error(f"Original source node {orig_node_id} not available for delta fail-back")
            return False, "Original source node not available"
        # Pre-set the replication node to the original source node so the backlog
        # match in replication_start links the pre-existing snapshots (delta only).
        lvol.replication_node_id = orig_node_id
        lvol.write_to_db()
        return replication_start(
            lvol_id, replication_cluster_id=rep.source_cluster_id, mode="migration",
            from_policy=True)

    # Fresh source cluster: full replication to a freshly selected node.
    if not source_cluster_id:
        logger.error("source_cluster_id required for fail-back to a fresh cluster")
        return False, "source_cluster_id required"
    return replication_start(
        lvol_id, replication_cluster_id=source_cluster_id, mode="migration",
        from_policy=True)


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
            ret = snode.rpc_client().nvmf_subsystem_listener_set_ana_state(
                lvol.nqn, iface.ip4_address, lvol.subsys_port, ana="inaccessible",
                anagrpid=lvol.ns_id)
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
                        lvol.nqn, iface.ip4_address, lvol.subsys_port, ana="inaccessible",
                        anagrpid=lvol.ns_id)
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
                lvol.nqn, iface.ip4_address, lvol.subsys_port, is_optimized=True,
                anagrpid=lvol.ns_id)
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
                        lvol.nqn, iface.ip4_address, lvol.subsys_port, is_optimized=False,
                        anagrpid=lvol.ns_id)
                    if not ret:
                        logger.error(f"Failed to set subsystem listener state for {lvol.nqn} on {iface.ip4_address}")
                        return False

    return True


def replicate_lvol_on_source_cluster(lvol_id, cluster_id=None, pool_uuid=None):
    db_controller = DBController()
    lvol = None
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError:
        logger.warning(f"LVol not found: {lvol_id}, looking in lvol replications")
        # look for it in lvol replication
        lvol_replications = db_controller.get_lvol_replication_objects()
        lvol_replications.reverse()
        for lvol_replication in lvol_replications:
            if lvol_replication.source_lvol.get_id() == lvol_id:
                lvol = lvol_replication.source_lvol
                break
        if not lvol:
            logger.error(f"LVol not found: {lvol_id}")
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
        new_lvol.bdev_stack.append({"type": "crypto"})

    new_lvol.write_to_db(db_controller.kv_store)

    logger.debug(f"new lvol from_source: {new_lvol.from_source}")

    lvol_bdev, error = add_lvol_on_node(new_lvol, source_node)
    if error:
        logger.error(error)
        db_controller.release_lvol_ns_slot(new_lvol)
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
            db_controller.release_lvol_ns_slot(new_lvol)
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

    For DHCHAP pools the pool's shared key pair is used automatically.
    For non-DHCHAP pools, security options are inherited from pool.sec_options.
    Returns a dict with the host NQN (and any per-host keys for non-DHCHAP pools),
    or (False, error_message) on failure.
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

    # Resolve pool
    pool = None
    if lvol.pool_uuid:
        try:
            pool = db_controller.get_pool_by_id(lvol.pool_uuid)
        except KeyError:
            pass

    entry = {"nqn": host_nqn}

    if pool and pool.dhchap:
        # Pool-level DHCHAP: use pool's shared key pair, no per-host key generation
        dhchap_group = constants.DHCHAP_DHGROUP
        for node_id in lvol.nodes:
            snode = db_controller.get_storage_node_by_id(node_id)
            if snode.status != StorageNode.STATUS_ONLINE:
                continue
            rpc_client = snode.rpc_client()
            pool_key_names = _register_pool_dhchap_keys_on_node(pool, snode, rpc_client)
            ret = rpc_client.subsystem_add_host(
                lvol.nqn, host_nqn,
                dhchap_key=pool_key_names.get("dhchap_key"),
                dhchap_ctrlr_key=pool_key_names.get("dhchap_ctrlr_key"),
                dhchap_group=dhchap_group,
            )
            if not ret:
                return False, f"Failed to add host {host_nqn} on node {node_id}"
    else:
        # Legacy per-host key generation from pool.sec_options
        sec_options = pool.sec_options if pool else None
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

        has_keys = any(entry.get(k) for k in ("dhchap_key", "dhchap_ctrlr_key", "psk"))
        dhchap_group = "null"
        if has_keys and lvol.nodes:
            first_node = db_controller.get_storage_node_by_id(lvol.nodes[0])
            cluster = db_controller.get_cluster_by_id(first_node.cluster_id)
            dhchap_group = _get_dhchap_group(cluster)
        for node_id in lvol.nodes:
            snode = db_controller.get_storage_node_by_id(node_id)
            if snode.status != StorageNode.STATUS_ONLINE:
                continue
            rpc_client = snode.rpc_client()
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
        rpc_client = snode.rpc_client()
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


def get_namespaces_per_lvol(lvol):
    db_controller = DBController()
    ns_count = 0
    for lv in db_controller.get_lvols_by_node_id(lvol.node_id):
        if lv.nqn == lvol.nqn and lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED]:
            ns_count += 1
    return ns_count


def get_next_available_subsystem_on_node(node_id, all_lvols=None, exclude_nqns=None)-> LVol | None:
    """``exclude_nqns`` skips subsystems the caller knows are unusable even
    though the DB count says they have room (SPDK rejected the add with
    -32602 — SPDK is the authority on its own namespace table)."""
    # `is None`, NOT falsy: an empty list from an in-transaction snapshot read
    # must stay authoritative — re-reading here would escape the caller's
    # transaction (claim_lvol_ns_slot passes the snapshot it counted from).
    if all_lvols is None:
        all_lvols = DBController().get_mini_lvols()

    # Count active namespaces per NQN in a single pass instead of issuing a
    # separate DB read for every subsystem root (was O(N²)).
    ns_counts: dict[str, int] = {}

    for lv in all_lvols:
        if lv.node_id != node_id:
            continue
        if lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED]:
            ns_counts[lv.nqn] = ns_counts.get(lv.nqn, 0) + 1

    ret = []
    for lvol in all_lvols:
        if lvol.node_id != node_id:
            continue
        if lvol.status in [LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED, LVol.STATUS_IN_CREATION]:
            continue
        if exclude_nqns and lvol.nqn in exclude_nqns:
            continue
        # The subsystem's recorded max is bounded by the hard per-subsystem
        # cap: legacy subsystems created with a larger max stop accepting
        # joins at the cap.
        subsys_max = min(lvol.max_namespace_per_subsys,
                         constants.MAX_NAMESPACES_PER_SUBSYSTEM)
        if lvol.nqn in ns_counts and ns_counts.get(lvol.nqn, 0) < subsys_max:
            if lvol not in ret:
                ret.append(lvol)

    if ret:
        return ret[random.randint(0, len(ret) - 1)]
    return None
