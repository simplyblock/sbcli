# coding=utf-8
import time
from datetime import datetime, timezone


from simplyblock_core import constants, db_controller, cluster_ops, storage_node_ops, utils
from simplyblock_core.controllers import health_controller, device_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()


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
    cluster = db.get_cluster_by_id(cluster_id)
    if cluster.status == cluster.STATUS_UNREADY:
        return Cluster.STATUS_UNREADY
    snodes = db.get_primary_storage_nodes_by_cluster_id(cluster_id)

    online_nodes = 0
    offline_nodes = 0
    affected_nodes = 0
    online_devices = 0
    offline_devices = 0

    affected_physical_nodes = []

    for node in snodes:

        node_online_devices = 0
        node_offline_devices = 0

        if node.status == StorageNode.STATUS_IN_CREATION:
            continue

        if node.status == StorageNode.STATUS_ONLINE:
            if is_new_migrated_node(cluster_id, node):
                continue
            online_nodes += 1
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
    if affected_nodes > k:
        return Cluster.STATUS_SUSPENDED
    # if number of devices in the cluster available < n + k --> I cannot write and I cannot read (suspended)
    if online_devices < n + k:
        return Cluster.STATUS_SUSPENDED
    if cluster.strict_node_anti_affinity:
        # if (number of online nodes < number of total nodes - k) --> suspended
        if online_nodes < (len(snodes) - k):
            return Cluster.STATUS_SUSPENDED
        # if (number of online nodes < n+1) --> suspended
        if online_nodes < (n + 1):
            return Cluster.STATUS_SUSPENDED
        # if (number of online nodes < n+2 and k=2) --> suspended
        if online_nodes < (n + 2) and k == 2:
            return Cluster.STATUS_SUSPENDED
    else:
        # if (number of online nodes < number of total nodes - k) --> degraded
        if online_nodes < (len(snodes) - k):
            return Cluster.STATUS_DEGRADED
        # if (number of online nodes < n+1) --> degraded
        if online_nodes < (n + 1):
            return Cluster.STATUS_DEGRADED
        # if (number of online nodes < n+2 and k=2) --> degraded
        if online_nodes < (n + 2) and k == 2:
            return Cluster.STATUS_DEGRADED

    return Cluster.STATUS_ACTIVE


def update_cluster_status(cluster_id):
    cluster = db.get_cluster_by_id(cluster_id)
    current_cluster_status = cluster.status
    logger.info("cluster_status: %s", current_cluster_status)
    if current_cluster_status in [Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_IN_EXPANSION]:
        return

    next_current_status = get_next_cluster_status(cluster_id)
    logger.info("cluster_new_status: %s", next_current_status)

    task_pending = 0
    for task in db.get_job_tasks(cluster_id):
        if task.status != JobSchedule.STATUS_DONE and task.function_name in [
            JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG]:
            task_pending += 1

    cluster = db.get_cluster_by_id(cluster_id)
    cluster.is_re_balancing = task_pending  > 0
    cluster.write_to_db()

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
                logger.error(f"can not activate cluster: restart tasks found")
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
    if node.status != StorageNode.STATUS_ONLINE:

        # set node online
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_ONLINE)

        # set jm dev online
        if node.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(node.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # set devices online
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
                device_controller.device_set_online(dev.get_id())


def set_node_offline(node):
    if node.status != StorageNode.STATUS_UNREACHABLE:
        # set node unavailable
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_UNREACHABLE)

        # # set devices unavailable
        # for dev in node.nvme_devices:
        #     if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
        #         device_controller.device_set_unavailable(dev.get_id())

        # # set jm dev offline
        # if node.jm_device.status != JMDevice.STATUS_UNAVAILABLE:
        #     device_controller.set_jm_device_state(node.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)

def set_node_down(node):
    if node.status != StorageNode.STATUS_DOWN:
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_DOWN)

logger.info("Starting node monitor")
while True:
    clusters = db.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()
        if cluster.status == Cluster.STATUS_IN_ACTIVATION:
            logger.info(f"Cluster status is: {cluster.status}, skipping monitoring")
            continue

        nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
        for snode in nodes:
            if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                                    StorageNode.STATUS_SCHEDULABLE, StorageNode.STATUS_DOWN]:
                logger.info(f"Node status is: {snode.status}, skipping")
                continue

            if snode.status == StorageNode.STATUS_ONLINE and snode.lvstore_status == "in_creation":
                logger.info(f"Node lvstore is in creation: {snode.get_id()}, skipping")
                continue

            logger.info(f"Checking node {snode.hostname}")

            # 1- check node ping
            ping_check = health_controller._check_node_ping(snode.mgmt_ip)
            logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")
            if not ping_check:
                # time.sleep(1)
                # ping_check = health_controller._check_node_ping(snode.mgmt_ip)
                logger.info(f"Check 2: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

            # 2- check node API
            node_api_check = health_controller._check_node_api(snode.mgmt_ip)
            logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

            if snode.status == StorageNode.STATUS_SCHEDULABLE and not ping_check and not node_api_check:
                continue

            spdk_process = False
            if node_api_check:
                # 3- check spdk_process
                spdk_process = health_controller._check_spdk_process_up(snode.mgmt_ip, snode.rpc_port)
            logger.info(f"Check: spdk process {snode.mgmt_ip}:5000 ... {spdk_process}")

                # 4- check rpc
            node_rpc_check = health_controller._check_node_rpc(
                snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=5, retry=2)
            logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

            node_port_check = True

            down_ports = []
            if spdk_process and node_rpc_check and snode.lvstore_status == "ready":
                ports = [snode.nvmf_port]
                if snode.lvstore_stack_secondary_1:
                    for n in db.get_primary_storage_nodes_by_secondary_node_id(snode.get_id()):
                        if n.lvstore_status == "ready":
                            ports.append(n.lvol_subsys_port)
                if not snode.is_secondary_node:
                    ports.append(snode.lvol_subsys_port)

                for port in ports:
                    ret = health_controller._check_port_on_node(snode, port)
                    logger.info(f"Check: node port {snode.mgmt_ip}, {port} ... {ret}")
                    node_port_check &= ret
                    if not ret and port == snode.nvmf_port:
                        down_ports |= True

                for data_nic in snode.data_nics:
                    if data_nic.ip4_address:
                        data_ping_check = health_controller._check_node_ping(data_nic.ip4_address)
                        logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping_check}")
                        if not data_ping_check:
                            node_port_check = False
                            down_ports |= True


            cluster = db.get_cluster_by_id(cluster.get_id())

            # is_node_online = ping_check and spdk_process and node_rpc_check and node_port_check
            is_node_online =  spdk_process or node_rpc_check
            if is_node_online:

                if snode.status == StorageNode.STATUS_UNREACHABLE:
                    if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_UNREADY,
                                          Cluster.STATUS_SUSPENDED, Cluster.STATUS_READONLY]:
                        tasks_controller.add_node_to_auto_restart(snode)
                        continue

                if not node_port_check:
                    if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
                        logger.error(f"Port check failed")
                        set_node_down(snode)
                        continue

                set_node_online(snode)

                # # check JM device
                # if snode.jm_device:
                #     if snode.jm_device.status in [JMDevice.STATUS_ONLINE, JMDevice.STATUS_UNAVAILABLE]:
                #         ret = health_controller.check_jm_device(snode.jm_device.get_id())
                #         if ret:
                #             logger.info(f"JM bdev is online: {snode.jm_device.get_id()}")
                #             if snode.jm_device.status != JMDevice.STATUS_ONLINE:
                #                 device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)
                #         else:
                #             logger.error(f"JM bdev is offline: {snode.jm_device.get_id()}")
                #             if snode.jm_device.status != JMDevice.STATUS_UNAVAILABLE:
                #                 device_controller.set_jm_device_state(snode.jm_device.get_id(),
                #                                                       JMDevice.STATUS_UNAVAILABLE)
            else:

                if not ping_check and not node_api_check and not spdk_process:
                    # restart on new node
                    storage_node_ops.set_node_status(snode.get_id(), StorageNode.STATUS_SCHEDULABLE)
                    # set devices unavailable
                    for dev in snode.nvme_devices:
                        if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
                            device_controller.device_set_unavailable(dev.get_id())

                elif ping_check and node_api_check and (not spdk_process or not node_rpc_check):
                    # add node to auto restart
                    if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_UNREADY,
                                          Cluster.STATUS_SUSPENDED, Cluster.STATUS_READONLY]:
                        set_node_offline(snode)
                        tasks_controller.add_node_to_auto_restart(snode)
                elif not node_port_check:
                    if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
                        logger.error(f"Port check failed")
                        set_node_down(snode)

                else:
                    set_node_offline(snode)

            if ping_check and node_api_check and spdk_process and not node_rpc_check:
                # restart spdk proxy cont
                if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_UNREADY,
                                      Cluster.STATUS_SUSPENDED, Cluster.STATUS_READONLY]:
                    logger.info(f"Restarting spdk_proxy_{snode.rpc_port} on {snode.get_id()}")
                    snode_api = SNodeClient(f"{snode.mgmt_ip}:5000", timeout=60, retry=1)
                    ret, err = snode_api.spdk_proxy_restart(snode.rpc_port)
                    if ret:
                        logger.info(f"Restarting spdk_proxy on {snode.get_id()} successfully")
                        continue
                    if err:
                        logger.error(err)

        update_cluster_status(cluster_id)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
