# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController

logger = logging.getLogger()


def _lvol_event(lvol, message, caused_by, event):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    ec.log_event_cluster(
        cluster_id=snode.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=lvol,
        caused_by=caused_by,
        message=message,
        node_id=lvol.get_id())


def lvol_create(lvol, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, "LVol created", caused_by, ec.EVENT_OBJ_CREATED)


def lvol_delete(lvol, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, "LVol deleted", caused_by, ec.EVENT_OBJ_DELETED)


def lvol_status_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol status changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_migrate(lvol, old_node, new_node, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol migrated from: {old_node} to: {new_node}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_health_check_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol health check changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_io_error_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol IO Error changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)

