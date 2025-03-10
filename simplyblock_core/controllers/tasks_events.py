# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController

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
    _task_event(task, f"task created: {task.uuid}", caused_by, ec.EVENT_OBJ_CREATED)


def task_updated(task, caused_by=ec.CAUSED_BY_CLI):
    _task_event(task, f"Task updated: {task.uuid}", caused_by, ec.EVENT_STATUS_CHANGE)


def task_canceled(task, caused_by=ec.CAUSED_BY_CLI):
    _task_event(task, f"Task canceled: {task.uuid}", caused_by, ec.EVENT_STATUS_CHANGE)

