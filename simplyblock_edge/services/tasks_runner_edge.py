# coding=utf-8
"""Task runner for edge-cluster tasks (docs/edge_clusters_spec.md §7).

One TaskRunner over the three FN_EDGE_* task families, with the standard host
lease and backoff. The handlers live in edge_cluster_ops.
"""
from simplyblock_core import constants as core_constants, db_controller, utils as core_utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.tasks import TaskLease, TaskRunner
from simplyblock_edge import constants as edge_constants
from simplyblock_edge import edge_cluster_ops

logger = core_utils.get_logger(__name__)

db = db_controller.DBController()


class EdgeTaskRunner(TaskRunner):

    function_names = (
        JobSchedule.FN_EDGE_NODE_RESTART,
        JobSchedule.FN_EDGE_DEVICE_REPLACE,
        JobSchedule.FN_EDGE_DEVICE_ADD,
    )

    HANDLERS = {
        JobSchedule.FN_EDGE_NODE_RESTART: edge_cluster_ops.handle_node_restart_task,
        JobSchedule.FN_EDGE_DEVICE_REPLACE: edge_cluster_ops.handle_device_replace_task,
        JobSchedule.FN_EDGE_DEVICE_ADD: edge_cluster_ops.handle_device_add_task,
    }

    def execute(self, task):
        return self.HANDLERS[task.function_name](task)


def main():
    EdgeTaskRunner(
        db,
        lease=TaskLease(db, ttl_sec=core_constants.TASK_LEASE_TTL_SEC,
                        heartbeat_sec=core_constants.TASK_LEASE_HEARTBEAT_SEC,
                        done_status=JobSchedule.STATUS_DONE, logger=logger),
        interval_sec=edge_constants.EDGE_TASK_INTERVAL_SEC,
        retry_backoff_base_sec=edge_constants.EDGE_TASK_BACKOFF_BASE_SEC,
        retry_backoff_max_sec=edge_constants.EDGE_TASK_BACKOFF_MAX_SEC,
        cluster_filter=lambda cluster: cluster.cluster_type == Cluster.TYPE_EDGE,
        logger=logger,
    ).run_forever()


if __name__ == "__main__":
    main()
