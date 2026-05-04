# coding=utf-8
import time


from simplyblock_core import db_controller, utils, storage_node_ops, distr_controller
from simplyblock_core.controllers import tcp_ports_events, health_controller, tasks_controller
from simplyblock_core.fw_api_client import FirewallClient
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


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

    # After a network outage, every distrib on the recovering node has a
    # stale view of remote devices (status_device=48 / is_device_available_read=0),
    # which causes DISTRIBD "Unable to read stripe" errors as soon as the
    # port is unblocked. Push the full cluster map now (covers all nodes'
    # devices, including our own) so the distribs have up-to-date status
    # before any IO is allowed through.
    logger.info("Sending full cluster map to recovering node")
    if not distr_controller.send_cluster_map_to_node(node):
        msg = "Failed to send cluster map to recovering node, retry task"
        logger.warning(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    logger.info("Cluster map sent; waiting 5s for JMs to connect")
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
                    secondary_hublvol_check = health_controller._check_sec_node_hublvol(sec_node, auto_fix=True, primary_node_id=node.get_id())
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

        # The previous implementation here did force-failback: if a peer
        # was the current LVS leader (because of an earlier failover from
        # `node`), it would block the peer's port, demote the peer, take
        # leadership locally on `node`, and additionally walk every
        # secondary and block + demote them too. That was wrong on two
        # counts:
        #
        #   - A writer conflict / leadership contention only ever blocks
        #     the *primary* (the JM heartbeat detects the dual-writer on
        #     the primary's lvstore and the CP forces the primary's
        #     distribs to non_leader). Secondaries are followers with
        #     bs_nonleader=true and have nothing to demote.
        #   - If a failover already succeeded and the peer is the
        #     legitimately-elected new leader, the cluster is correctly
        #     serving IO via the peer. There is no problem to solve.
        #     Blocking the new leader's port cuts client IO that was
        #     being served correctly, and the synchronous demote+take
        #     opens a fresh writer-conflict window.
        #
        # See incident 2026-05-02 (k8s_native_failover_ha-20260502-101452):
        # at 15:51:01 the JM forced worker5's LVS_4729 distribs to
        # non_leader (writer conflict). Failover transferred leadership
        # to worker1 (legitimate new primary). At 15:51:32 the
        # health-check on worker5's port 4434 failed (worker5 was DOWN)
        # and queued a port_allow. At 15:51:44.818 the runner here
        # logged "Current leader for LVS_4729 is peer 46544aff…;
        # demoting before port_allow on ad04496b…" and blocked
        # worker1's port + force-demoted worker1 — directly producing
        # client IO errors and a follow-on writer conflict.
        #
        # port_allow's correct scope is just allowing the port on the
        # recovering node. Leadership belongs to the JM heartbeat /
        # writer-conflict resolution mechanism, not to this task.

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

    task.function_result = f"Port {port_number} allowed on node"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


def _main():
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


if __name__ == "__main__":
    _main()
