# coding=utf-8
import time


from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


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

                if task.function_name == JobSchedule.FN_JC_COMP_RESUME:
                    if task.status != JobSchedule.STATUS_DONE:

                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)

                        if task.canceled:
                            task.function_result = "canceled"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        if task.retry >= task.max_retry:
                            task.function_result = "max retry reached, stopping task"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        try:
                            node = db.get_storage_node_by_id(task.node_id)
                        except KeyError:
                            task.function_result = "node not found"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        if node.status != StorageNode.STATUS_ONLINE:
                            msg = f"Node is {node.status}, retry task"
                            logger.info(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        node_task = tasks_controller.get_active_node_tasks(task.cluster_id, task.node_id)
                        if node_task:
                            msg="Task found on same node"
                            logger.info(msg)
                            task.retry += 1
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                        else:
                            logger.info("no task found on same node, resuming compression")
                            node = db.get_storage_node_by_id(task.node_id)
                            for n in db.get_storage_nodes_by_cluster_id(node.cluster_id):
                                if n.status != StorageNode.STATUS_ONLINE:
                                    msg = "Not all nodes are online, can not resume JC compression"
                                    logger.info(msg)
                                    task.function_result = msg
                                    task.status = JobSchedule.STATUS_SUSPENDED
                                    task.write_to_db(db.kv_store)
                                    continue

                            rpc_client = node.rpc_client(timeout=5, retry=2)
                            jm_vuid = node.jm_vuid
                            if "jm_vuid" in task.function_params:
                                jm_vuid = task.function_params["jm_vuid"]
                            try:
                                ret, err = rpc_client.jc_compression_start(jm_vuid=jm_vuid)
                            except Exception as e:
                                logger.error(e)
                                continue
                            if ret:
                                task.function_result = f"JC {node.jm_vuid} compression resumed on node"
                                task.status = JobSchedule.STATUS_DONE
                                task.write_to_db(db.kv_store)
                            elif err and "code" in err and err["code"] == -2:
                                task.function_result = f"JC {node.jm_vuid} compression not needed"
                                task.status = JobSchedule.STATUS_DONE
                                task.write_to_db(db.kv_store)
                            else:
                                msg = "JC comp resume failed, retry task"
                                logger.info(msg)
                                task.retry += 1
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)

    time.sleep(60)
