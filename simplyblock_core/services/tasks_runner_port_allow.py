# coding=utf-8
import time


from simplyblock_core import db_controller, utils, storage_node_ops, distr_controller
from simplyblock_core.controllers import tcp_ports_events, health_controller, tasks_controller, lvol_controller
from simplyblock_core.fw_api_client import FirewallClient
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def _get_lvs_leader(lvs_name, candidates):
    for candidate in candidates:
        if not candidate or candidate.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            if lvol_controller.is_node_leader(candidate, lvs_name):
                return candidate
        except Exception as e:
            logger.warning("Failed to query leadership for %s on %s: %s",
                           lvs_name, candidate.get_id(), e)
    return None


def exec_port_allow_task(task):
    # get new task object because it could be changed from cancel task
    task = db.get_task_by_id(task.uuid)

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
        msg = f"Node is {node.status}, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # check node ping
    ping_check = health_controller._check_node_ping(node.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
    if not ping_check:
        time.sleep(1)
        ping_check = health_controller._check_node_ping(node.mgmt_ip)
        logger.info(f"Check 2: ping mgmt ip {node.mgmt_ip} ... {ping_check}")

    if not ping_check:
        msg = "Node ping is false, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # check node ping
    logger.info("connect to remote devices")
    nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
    # connect to remote devs
    try:
        node_bdevs = node.rpc_client().get_bdevs()
        logger.debug(node_bdevs)
        if node_bdevs:
            node_bdev_names = {}
            for b in node_bdevs:
                node_bdev_names[b['name']] = b
                for al in b['aliases']:
                    node_bdev_names[al] = b
        else:
            node_bdev_names = {}
        remote_devices = storage_node_ops._connect_to_remote_devs(node, reattach=False)
        if not remote_devices:
            msg = "Node unable to connect to remote devs, retry task"
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        else:
            # Re-read fresh before writing to avoid overwriting concurrent changes
            node = db.get_storage_node_by_id(task.node_id)
            node.remote_devices = remote_devices
            node.write_to_db()

        logger.info("connect to remote JM devices")
        remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(node)
        if not remote_jm_devices or len(remote_jm_devices) < 2:
            msg = "Node unable to connect to remote JMs, retry task"
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        else:
            # Re-read fresh before writing to avoid overwriting concurrent changes
            node = db.get_storage_node_by_id(task.node_id)
            node.remote_jm_devices = remote_jm_devices
            node.write_to_db()


    except Exception as e:
        logger.error(e)
        msg = "Error when connect to remote devs, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    logger.info("Sending device status event")
    for db_dev in node.nvme_devices:
        distr_controller.send_dev_status_event(db_dev, db_dev.status, node)

    # Replay node-status and device-status events for ALL other nodes to
    # this recovering node's distribs. During a network outage, this node
    # missed events that other nodes produced (e.g. a peer's graceful
    # shutdown set its devices to unavailable, but the event never reached
    # this node's distrib cluster map). Replaying now brings the map up to
    # date before the consistency check below.
    logger.info("Replaying cluster-wide status events to recovering node")
    all_nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
    for peer in all_nodes:
        if peer.get_id() == node.get_id():
            continue
        distr_controller.send_node_status_event(peer, peer.status, target_node=node)
        for peer_dev in peer.nvme_devices:
            distr_controller.send_dev_status_event(peer_dev, peer_dev.status, target_node=node)

    logger.info("Finished sending device status and now waiting 5s for JMs to connect")
    time.sleep(5)

    snode = db.get_storage_node_by_id(node.get_id())
    sec_ids = []
    if node.secondary_node_id:
        sec_ids.append(node.secondary_node_id)
    if node.tertiary_node_id:
        sec_ids.append(node.tertiary_node_id)
    for sec_id in sec_ids:
        sec_node = db.get_storage_node_by_id(sec_id)
        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
            try:
                ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                if ret:
                    lvs_info = ret[0]
                    if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                        jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(snode.jm_vuid)
                        retries = 10
                        while jc_compression_is_active:
                            if retries <= 0:
                                logger.warning("Timeout waiting for JC compression task to finish")
                                break
                            retries -= 1
                            logger.info(
                                f"JC compression task found on node: {sec_node.get_id()}, retrying in 60 seconds")
                            time.sleep(60)
                            jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(
                                snode.jm_vuid)
            except Exception as e:
                logger.error(e)
                return

    if node.lvstore_status == "ready":
        lvstore_check = health_controller._check_node_lvstore(node.lvstore_stack, node, auto_fix=True)
        if not lvstore_check:
            msg = "Node LVolStore check fail, retry later"
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        sec_ids = []
        if node.secondary_node_id:
            sec_ids.append(node.secondary_node_id)
        if node.tertiary_node_id:
            sec_ids.append(node.tertiary_node_id)
        if sec_ids:
            primary_hublvol_check = health_controller._check_node_hublvol(node)
            if not primary_hublvol_check:
                msg = "Node hublvol check fail, retry later"
                logger.warning(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return

            for sec_id in sec_ids:
                sec_node = db.get_storage_node_by_id(sec_id)
                if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                    secondary_hublvol_check = health_controller._check_sec_node_hublvol(sec_node, auto_fix=True)
                    if not secondary_hublvol_check:
                        msg = f"Secondary node {sec_id} hublvol check fail, retry later"
                        logger.warning(msg)
                        task.function_result = msg
                        task.status = JobSchedule.STATUS_SUSPENDED
                        task.write_to_db(db.kv_store)
                        return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    try:
        # wait for lvol sync delete
        lvol_sync_del_found = tasks_controller.get_lvol_sync_del_task(task.cluster_id, task.node_id)
        while lvol_sync_del_found:
            logger.info("Lvol sync delete task found, waiting")
            time.sleep(3)
            lvol_sync_del_found = tasks_controller.get_lvol_sync_del_task(task.cluster_id, task.node_id)

        port_number = task.function_params["port_number"]
        secs_to_unblock = []
        primary_lvs_port = node.get_lvol_subsys_port(node.lvstore)
        if port_number == primary_lvs_port:
            candidates = [node] + [db.get_storage_node_by_id(sid) for sid in sec_ids]
            current_leader = _get_lvs_leader(node.lvstore, candidates)

            if current_leader and current_leader.get_id() != node.get_id():
                logger.info("Current leader for %s is %s, skipping peer demotion during port_allow on %s",
                            node.lvstore, current_leader.get_id(), node.get_id())
            else:
                if current_leader is None:
                    logger.warning("No leader found for %s during port_allow on %s; attempting local restore",
                                   node.lvstore, node.get_id())
                    node.rpc_client().bdev_lvol_set_lvs_opts(
                        node.lvstore,
                        groupid=node.jm_vuid,
                        subsystem_port=primary_lvs_port,
                        role="primary"
                    )
                    node.rpc_client().bdev_lvol_set_leader(node.lvstore, leader=True)
                    current_leader = _get_lvs_leader(node.lvstore, [node])
                    if not current_leader:
                        msg = f"No leader available for {node.lvstore}, retry task"
                        logger.warning(msg)
                        task.function_result = msg
                        task.status = JobSchedule.STATUS_SUSPENDED
                        task.write_to_db(db.kv_store)
                        return

                for sid in sec_ids:
                    sn = db.get_storage_node_by_id(sid)
                    if not sn or sn.status != StorageNode.STATUS_ONLINE:
                        continue

                    sn_rpc = sn.rpc_client()
                    ret = sn.wait_for_jm_rep_tasks_to_finish(node.jm_vuid)
                    if not ret:
                        msg = f"JM replication task found on secondary {sn.get_id()}"
                        logger.warning(msg)
                        task.function_result = msg
                        task.status = JobSchedule.STATUS_SUSPENDED
                        task.write_to_db(db.kv_store)
                        return

                    sn_fw = FirewallClient(sn, timeout=5, retry=2)
                    sn_port_type = "udp" if sn.active_rdma else "tcp"
                    sn_fw.firewall_set_port(port_number, sn_port_type, "block", sn.rpc_port)
                    tcp_ports_events.port_deny(sn, port_number)

                    time.sleep(0.5)

                    sn_rpc.bdev_lvol_set_leader(node.lvstore, leader=False, bs_nonleadership=True)
                    sn_rpc.bdev_distrib_force_to_non_leader(node.jm_vuid)
                    logger.info(f"Checking for inflight IO from node: {sn.get_id()}")
                    for i in range(100):
                        is_inflight = sn_rpc.bdev_distrib_check_inflight_io(node.jm_vuid)
                        if is_inflight:
                            logger.info("Inflight IO found, retry in 100ms")
                            time.sleep(0.1)
                        else:
                            logger.info("Inflight IO NOT found, continuing")
                            break
                    else:
                        logger.error(
                            f"Timeout while checking for inflight IO after 10 seconds on node {sn.get_id()}")

                    secs_to_unblock.append(sn)

    except Exception as e:
        logger.error(e)
        return

    logger.info(f"Allow port {port_number} on node {node.get_id()}")
    fw_api = FirewallClient(snode, timeout=5, retry=2)
    port_type = "tcp"
    if node.active_rdma:
        port_type = "udp"
    fw_api.firewall_set_port(port_number, port_type, "allow", node.rpc_port)
    tcp_ports_events.port_allowed(node, port_number)

    # Unblock ports on secondaries that were blocked above
    for sn in secs_to_unblock:
        sn_fw = FirewallClient(sn, timeout=5, retry=2)
        sn_port_type = "udp" if sn.active_rdma else "tcp"
        sn_fw.firewall_set_port(port_number, sn_port_type, "allow", sn.rpc_port)
        tcp_ports_events.port_allowed(sn, port_number)

    task.function_result = f"Port {port_number} allowed on node"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


logger.info("Starting Tasks runner...")
while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                if task.function_name == JobSchedule.FN_PORT_ALLOW:
                    if task.status != JobSchedule.STATUS_DONE:
                        exec_port_allow_task(task)

    time.sleep(5)
