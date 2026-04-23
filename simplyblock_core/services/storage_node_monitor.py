# coding=utf-8
import threading
import time
from datetime import datetime, timezone


from simplyblock_core import constants, db_controller, cluster_ops, storage_node_ops, utils
from simplyblock_core.controllers import health_controller, device_controller, tasks_controller, storage_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()

node_rpc_timeout_threads: dict[str, threading.Thread] = {}


def is_new_migrated_node(cluster_id, node):
    dev_lst = []
    for dev in node.nvme_devices:
        if dev.status == NVMeDevice.STATUS_ONLINE:
            dev_lst.append(dev.get_id())

    distr_names = []
    for item in node.lvstore_stack:
        if item["type"] == "bdev_distr":
            distr_names.append(item["name"])

    if dev_lst:
        tasks = db.get_job_tasks(cluster_id)
        for task in tasks:
            if task.function_name == JobSchedule.FN_NEW_DEV_MIG and task.node_id == node.get_id():
                if task.device_id not in dev_lst:
                    continue
                if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                    if "distr_name" in task.function_params and task.function_params["distr_name"] in distr_names:
                        return True
    return False


def get_next_cluster_status(cluster_id):
    logger.info(f"get_next_cluster_status for cluster_id: {cluster_id}")
    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status == cluster.STATUS_UNREADY:
        return Cluster.STATUS_UNREADY
    snodes = db.get_primary_storage_nodes_by_cluster_id(cluster_id)

    online_nodes = 0
    offline_nodes = 0
    affected_nodes = 0
    online_devices = 0
    offline_devices = 0
    jm_replication_tasks = False

    affected_physical_nodes = []

    for node in snodes:

        node_online_devices = 0
        node_offline_devices = 0

        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_SUSPENDED]:
            continue

        if node.status == StorageNode.STATUS_ONLINE:
            if is_new_migrated_node(cluster_id, node):
                continue
            online_nodes += 1
            try:
                # check for jm rep tasks:
                if node.rpc_client(timeout=10).bdev_lvol_get_lvstores(node.lvstore):
                    ret = node.rpc_client(timeout=5).jc_get_jm_status(node.jm_vuid)
                    for jm in ret:
                        if ret[jm] is False: # jm is not ready (has active replication task)
                            jm_replication_tasks = True
                            logger.warning("Replication task found!")
                            break
            except Exception:
                logger.warning("Failed to get replication task!")
        elif node.status == StorageNode.STATUS_REMOVED:
            pass
        else:
            offline_nodes += 1
        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_JM,
                              NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                node_online_devices += 1
            elif dev.status == NVMeDevice.STATUS_FAILED_AND_MIGRATED:
                pass
            else:
                node_offline_devices += 1

        if node_offline_devices > 0 or (node_online_devices == 0 and node.status != StorageNode.STATUS_REMOVED):
            affected_nodes += 1
            if node.mgmt_ip not in affected_physical_nodes:
                affected_physical_nodes.append(node.mgmt_ip)

        online_devices += node_online_devices
        offline_devices += node_offline_devices

    affected_nodes = len(affected_physical_nodes)
    logger.debug(f"online_nodes: {online_nodes}")
    logger.debug(f"offline_nodes: {offline_nodes}")
    logger.debug(f"affected_nodes: {affected_nodes}")
    logger.debug(f"online_devices: {online_devices}")
    logger.debug(f"offline_devices: {offline_devices}")
    # ndcs n = 2
    # npcs k = 1
    n = cluster.distr_ndcs
    k = cluster.distr_npcs

    # if number of devices in the cluster unavailable on DIFFERENT nodes > k --> I cannot read and in some cases cannot write (suspended)
    if affected_nodes == k and (not cluster.strict_node_anti_affinity or online_nodes >= (n + k)):
        return Cluster.STATUS_DEGRADED
    elif jm_replication_tasks:
        return Cluster.STATUS_DEGRADED
    elif (affected_nodes > k or online_devices < (n + k) or (
            online_nodes < (n + k) and cluster.strict_node_anti_affinity)):
        return Cluster.STATUS_SUSPENDED
    else:
        return Cluster.STATUS_ACTIVE


def update_cluster_status(cluster_id):
    next_current_status = get_next_cluster_status(cluster_id)
    logger.info("cluster_new_status: %s", next_current_status)

    rebalancing_task_names = {
        JobSchedule.FN_DEV_MIG,
        JobSchedule.FN_NEW_DEV_MIG,
        JobSchedule.FN_FAILED_DEV_MIG,
        JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
        JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
        JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION,
        JobSchedule.FN_LVOL_MIG,
    }
    active_rebalancing_tasks = 0
    for task in db.get_job_tasks(cluster_id):
        if task.canceled:
            continue
        if task.status == JobSchedule.STATUS_DONE:
            continue
        if task.function_name in rebalancing_task_names:
            active_rebalancing_tasks += 1

    cluster = db.get_cluster_by_id(cluster_id)
    cluster.is_re_balancing = active_rebalancing_tasks > 0
    cluster.write_to_db()

    current_cluster_status = cluster.status
    logger.info("cluster_status: %s", current_cluster_status)
    if current_cluster_status in [Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_IN_EXPANSION]:
        return

    if current_cluster_status == Cluster.STATUS_DEGRADED and next_current_status == Cluster.STATUS_ACTIVE:
        # if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_UNREADY] and cluster_current_status == Cluster.STATUS_ACTIVE:
        # cluster_ops.cluster_activate(cluster_id, True)
        cluster_ops.set_cluster_status(cluster_id, Cluster.STATUS_ACTIVE)
        return
    elif current_cluster_status == Cluster.STATUS_READONLY and next_current_status in [
        Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        return
    elif current_cluster_status == Cluster.STATUS_SUSPENDED and next_current_status \
            in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        # needs activation
        # check node status, check auto restart for nodes
        can_activate = True
        for node in db.get_storage_nodes_by_cluster_id(cluster_id):
            if node.status in [StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_IN_CREATION,
                               StorageNode.STATUS_RESTARTING]:
                logger.error(f"can not activate cluster: node is not in correct status {node.get_id()}: {node.status}")
                can_activate = False
                break

            # if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_REMOVED]:
            #     logger.error(f"can not activate cluster: node in not online {node.get_id()}: {node.status}")
            #     can_activate = False
            #     break
            if tasks_controller.get_active_node_restart_task(cluster_id, node.get_id()):
                logger.error("can not activate cluster: restart tasks found")
                can_activate = False
                break

            if node.online_since:
                diff = datetime.now(timezone.utc) - datetime.fromisoformat(node.online_since)
                if diff.total_seconds() < 30:
                    logger.error(f"can not activate cluster: node is online less than 30 seconds: {node.get_id()}")
                    can_activate = False
                    break

        if can_activate:
            cluster_ops.cluster_activate(cluster_id, force=True)
    else:
        cluster_ops.set_cluster_status(cluster_id, next_current_status)


def set_node_online(node):
    node = db.get_storage_node_by_id(node.get_id())
    if node.status in [StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN]:

        # set node online
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_ONLINE)

        # set jm dev online
        if node.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(node.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # set devices online
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
                device_controller.device_set_online(dev.get_id())

        # start migration tasks on node online status change
        online_devices_list = []
        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE,
                              NVMeDevice.STATUS_CANNOT_ALLOCATE,
                              NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                online_devices_list.append(dev.get_id())
        if online_devices_list:
            logger.info(f"Starting migration task for node {node.get_id()}")
            tasks_controller.add_device_mig_task_for_node(node.get_id())

        update_cluster_status(cluster_id)


def set_node_offline(node):
    node = db.get_storage_node_by_id(node.get_id())
    if node.status not in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_IN_SHUTDOWN]:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_OFFLINE)
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    device_controller.device_set_unavailable(dev.get_id())
            update_cluster_status(cluster_id)

            # ANA failover: update subsystem states on surviving nodes
            try:
                logger.info(f"Triggering ANA failover for node {node.get_id()}")
                storage_node_ops.trigger_ana_failover_for_node(node)
            except Exception as ana_e:
                logger.error("ANA failover for node %s failed: %s", node.get_id(), ana_e)

            # initiate restart
            logger.info(f"Node {node.get_id()} set to OFFLINE, adding to auto-restart")
            tasks_controller.add_node_to_auto_restart(node)
        except Exception as e:
            logger.debug("Setting node to OFFLINE state failed")
            logger.error(e)


def set_node_unreachable(node):
    if node.status != StorageNode.STATUS_UNREACHABLE:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_UNREACHABLE)
            update_cluster_status(cluster_id)
        except Exception as e:
            logger.debug("Setting node to UNREACHABLE state failed")
            logger.error(e)

    # Check data-plane health from surviving nodes.  If all online peers
    # report the unreachable node's remote JM as disconnected, the data
    # plane is truly down and we can escalate to offline.
    try:
        _check_data_plane_and_escalate(node)
    except Exception as e:
        logger.error("Data-plane check for unreachable node %s failed: %s", node.get_id(), e)


def is_node_data_plane_disconnected(node):
    """Return True if all other online nodes report *node*'s remote JM as disconnected.

    Returns False if no peers are available to check (conservative).
    """
    disconnected, total = _count_data_plane_votes(node)
    return total > 0 and disconnected == total


def is_node_data_plane_disconnected_quorum(node, lvs_peer_ids=None):
    """Return True if a majority of online nodes report *node*'s remote JM as disconnected.

    Returns False if no peers are available to check (conservative).
    """
    disconnected, total = _count_data_plane_votes(node)
    return total > 0 and disconnected > total // 2


def _count_data_plane_votes(node):
    """Query all other online storage nodes for *node*'s JM connectivity.

    Returns (disconnected_count, total_peers_checked).
    """
    node_id = node.get_id()
    cluster_nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)

    online_peers = [
        n for n in cluster_nodes
        if n.get_id() != node_id
        and n.status == StorageNode.STATUS_ONLINE
        and n.jm_vuid
    ]

    if not online_peers:
        logger.debug("No online peers to verify data plane for %s", node_id)
        return 0, 0

    remote_jm_key = f"remote_jm_{node_id}n1"
    disconnected = 0
    total = 0

    for peer in online_peers:
        try:
            ret = peer.rpc_client(timeout=5, retry=1).jc_get_jm_status(peer.jm_vuid)
            if not ret or remote_jm_key not in ret:
                logger.debug("Data-plane check: peer %s has no status for %s JM; ignoring vote",
                             peer.get_id(), node_id)
                continue

            total += 1
            if ret[remote_jm_key] is True:
                logger.info("Data-plane check: peer %s still sees %s JM as connected",
                            peer.get_id(), node_id)
            else:
                disconnected += 1
        except Exception as e:
            logger.debug("jc_get_jm_status on peer %s failed: %s", peer.get_id(), e)
            continue

    logger.info("Data-plane check for %s: %d/%d peers report disconnected", node_id, disconnected, total)
    return disconnected, total


def _check_data_plane_and_escalate(unreachable_node):
    """Escalate to offline if data plane is confirmed down."""
    node = db.get_storage_node_by_id(unreachable_node.get_id())
    if node.status == StorageNode.STATUS_RESTARTING:
        logger.debug("Node %s is restarting, skipping data-plane escalation", node.get_id())
        return
    if is_node_data_plane_disconnected(node):
        logger.info("Data-plane check: all peers report %s JM disconnected, escalating to offline",
                    node.get_id())
        set_node_offline(node)


def set_node_schedulable(node):
    node = db.get_storage_node_by_id(node.get_id())
    if node.status not in [StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_IN_SHUTDOWN]:
        try:
            storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_SCHEDULABLE)
            # initiate shutdown
            # initiate restart
            tasks_controller.add_node_to_auto_restart(node)
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    device_controller.device_set_unavailable(dev.get_id())
            update_cluster_status(cluster_id)
        except Exception as e:
            logger.debug("Setting node to SCHEDULABLE state failed")
            logger.error(e)


def set_node_down(node):
    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED]:
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_DOWN)
        update_cluster_status(cluster_id)


def node_rpc_timeout_check_and_report(node):
    start_time = time.time()
    try:
        rpc_client = node.rpc_client(timeout=60, retry=5)
        ret = rpc_client.get_version()
        if ret:
            logger.debug(f"SPDK version: {ret['version']}")
            return True
    except Exception as e:
        logger.debug(e)
    # RPC timeout detected, send to cluster log
    storage_events.snode_rpc_timeout(node, int(time.time() - start_time))
    return False


def node_port_check_fun(snode):
    node_port_check = True
    if snode.lvstore_status == "ready":
        ports = [snode.nvmf_port]
        if snode.lvstore_stack_secondary or snode.lvstore_stack_tertiary:
            for n in db.get_primary_storage_nodes_by_secondary_node_id(snode.get_id()):
                if n.lvstore_status != "ready":
                    continue
                # Skip port check during failback: if the primary or the
                # other secondary (sec_1) for this lvstore is online/restarting,
                # the port on this node may be intentionally blocked.
                skip = False
                if n.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_RESTARTING]:
                    skip = True
                elif n.secondary_node_id and n.secondary_node_id != snode.get_id():
                    sec1 = db.get_storage_node_by_id(n.secondary_node_id)
                    if sec1 and sec1.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_RESTARTING]:
                        skip = True
                if not skip:
                    ports.append(n.get_lvol_subsys_port(n.lvstore))
        if not snode.is_secondary_node:
            ports.append(snode.get_lvol_subsys_port(snode.lvstore))

        for port in ports:
            try:
                ret = health_controller.check_port_on_node(snode, port)
                logger.info(f"Check: node port {snode.mgmt_ip}, {port} ... {ret}")
                node_port_check &= ret
            except Exception as e:
                health_controller._log_port_check_failure(db, snode, port, e)

        node_data_nic_ping_check = False
        for data_nic in snode.data_nics:
            if data_nic.ip4_address:
                data_ping_check = health_controller._check_ping_from_node(data_nic.ip4_address, ifname=data_nic.if_name, node=snode)
                logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping_check}")
                node_data_nic_ping_check |= data_ping_check

        node_port_check &= node_data_nic_ping_check

    return node_port_check


class State:
    counter = 0
def increment():
    State.counter = 1
def decrement():
    State.counter = 0
def value():
    return State.counter

def check_node(snode):
    snode = db.get_storage_node_by_id(snode.get_id())

    if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                            StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN,
                            StorageNode.STATUS_OFFLINE]:
        logger.info(f"Node status is: {snode.status}, skipping")
        return False

    if snode.status == StorageNode.STATUS_ONLINE and snode.lvstore_status == "in_creation":
        logger.info(f"Node lvstore is in creation: {snode.get_id()}, skipping")
        return False

    logger.info(f"Checking node {snode.hostname}")

    # If the node is offline, ensure ANA failover was processed.
    # Another service may have set the node offline without triggering it.
    # Note: do NOT add auto-restart here — the node may have been
    # intentionally shut down via sbctl.  Auto-restart is only added by
    # set_node_offline() when the monitor itself detects a failure.
    if snode.status == StorageNode.STATUS_OFFLINE:
        try:
            storage_node_ops.trigger_ana_failover_for_node(snode)
        except Exception as e:
            logger.error("ANA failover for offline node %s failed: %s", snode.get_id(), e)
        return True

    # 1- check node ping
    ping_check = health_controller._check_node_ping(snode.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")
    if not ping_check:
        logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}: FAILED")
        set_node_unreachable(snode)
        return False

    # 2- check node API
    try:
        snode_api = snode.client(timeout=10, retry=2)
        ret, _ = snode_api.is_live()
        logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {ret}")
        if not ret:
            logger.info("Check: node API failed, setting node unreachable")
            set_node_unreachable(snode)
            return False
    except Exception as e:
        logger.debug(e)
        set_node_unreachable(snode)
        return False

    # 3- check spdk process through node API
    try:
        snode_api = snode.client(timeout=40, retry=2)
        is_up, _ = snode_api.spdk_process_is_up(snode.rpc_port, snode.cluster_id)
        logger.info(f"Check: spdk process {snode.mgmt_ip}:5000 ... {bool(is_up)}")
        if not is_up:
            logger.info("Check: node API failed, setting node offline")
            set_node_offline(snode)
            return False
    except Exception as e:
        logger.debug(e)
        return False

    # 4- check node rpc interface
    node_rpc_check, node_rpc_check_1 = health_controller._check_node_rpc(
        snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=20, retry=1)
    logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

    #if RPC times out, we dont know if its due to node becoming unavailable or spdk hanging
    #so we try it twice. If all other checks pass again, but only this one fails: it's the spdk process
    if not node_rpc_check:
        logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}:TIMEOUT")
        if value()==0:
           increment()
           return False

    decrement()
    if not node_rpc_check or not node_rpc_check_1:
        logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}:FAILED")
        set_node_schedulable(snode)
        return False

    #if not node_rpc_check and snode.get_id() not in node_rpc_timeout_threads:
    #    t = threading.Thread(target=node_rpc_timeout_check_and_report, args=(snode,))
    #    t.start()
    #    node_rpc_timeout_threads[snode.get_id()] = t

    node_port_check = node_port_check_fun(snode)

    if not node_port_check:
        cluster = db.get_cluster_by_id(snode.cluster_id)
        if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
            logger.error("Port check failed")
            set_node_down(snode)
            return True

    set_node_online(snode)


def loop_for_node(snode):
    # global logger
    # logger = logging.getLogger()
    # logger_handler = logging.StreamHandler(stream=sys.stdout)
    # logger_handler.setFormatter(logging.Formatter(f'%(asctime)s: node:{snode.mgmt_ip} %(levelname)s: %(message)s'))
    # logger.addHandler(logger_handler)
    while True:
        check_node(snode)
        logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
        time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    logger.info("Starting node monitor")
    threads_maps: dict[str, threading.Thread] = {}

    while True:
        clusters = db.get_clusters()
        for cluster in clusters:
            cluster_id = cluster.get_id()
            if cluster.status == Cluster.STATUS_IN_ACTIVATION:
                logger.info(f"Cluster status is: {cluster.status}, skipping monitoring")
                continue
            logger.info(f"Looping for cluster {cluster_id}")
            nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
            for node in nodes:
                node_id = node.get_id()
                if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                    logger.info(f"Creating thread for node {node_id}")
                    t = threading.Thread(target=loop_for_node, args=(node,))
                    t.start()
                    threads_maps[node_id] = t
                    logger.debug(threads_maps[node_id])

            try:
                update_cluster_status(cluster_id)
                logger.debug("Iteration has been finished...")
            except Exception:
                logger.error("Error while updating cluster status")
        time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
