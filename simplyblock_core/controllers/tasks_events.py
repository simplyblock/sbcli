# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule

logger = logging.getLogger()
db_controller = DBController()


def _task_event(task, message, caused_by, event):
    ec.log_event_cluster(
        cluster_id=task.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=task,
        caused_by=caused_by,
        message=message,
        node_id=task.node_id or task.uuid,
        status=task.status)


def task_create(task, caused_by=ec.CAUSED_BY_CLI):
    if task.function_name in [JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
                              JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
                              JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION]:
        _task_event(task, "Re-balancing task created", caused_by, ec.EVENT_OBJ_CREATED)
    else:
        _task_event(task, "Task created", caused_by, ec.EVENT_OBJ_CREATED)


def task_updated(task, caused_by=ec.CAUSED_BY_CLI):
    if task.function_name in [JobSchedule.FN_BALANCING_AFTER_NODE_RESTART,
                              JobSchedule.FN_BALANCING_AFTER_DEV_REMOVE,
                              JobSchedule.FN_BALANCING_AFTER_DEV_EXPANSION]:
        _task_event(task, "Re-balancing task updated", caused_by, ec.EVENT_STATUS_CHANGE)
    else:
        _task_event(task, "Task updated", caused_by, ec.EVENT_STATUS_CHANGE)


def task_canceled(task, caused_by=ec.CAUSED_BY_CLI):
    if task.function_name in [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG]:
        _task_event(task, "Subtask canceled", caused_by, ec.EVENT_STATUS_CHANGE)
    else:
        _task_event(task, "Task canceled", caused_by, ec.EVENT_STATUS_CHANGE)

