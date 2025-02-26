# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController

logger = logging.getLogger()
db_controller = DBController()


def _port_event(node, message, caused_by, event):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=node,
        caused_by=caused_by,
        message=message,
        node_id=node.get_id())


def port_allowed(node, port, caused_by=ec.CAUSED_BY_CLI):
    _port_event(node, f"Port allowed: {port}", caused_by, ec.EVENT_OBJ_CREATED)


def port_deny(node, port, caused_by=ec.CAUSED_BY_CLI):
    _port_event(node, f"Port blocked: {port}", caused_by, ec.EVENT_OBJ_CREATED)
