# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.kv_store import DBController

logger = logging.getLogger()
db_controller = DBController()


def _snapshot_event(snapshot, message, caused_by, event):
    snode = db_controller.get_storage_node_by_id(snapshot.lvol.node_id)
    ec.log_event_cluster(
        cluster_id=snode.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=snapshot,
        caused_by=caused_by,
        message=message,
        node_id=snapshot.lvol.node_id)


def snapshot_create(snapshot, caused_by=ec.CAUSED_BY_CLI):
    _snapshot_event(snapshot, f"Snapshot created: {snapshot.get_id()}", caused_by, ec.EVENT_OBJ_CREATED)


def snapshot_delete(snapshot, caused_by=ec.CAUSED_BY_CLI):
    _snapshot_event(snapshot, f"Snapshot deleted: {snapshot.get_id()}", caused_by, ec.EVENT_OBJ_DELETED)


def snapshot_clone(snapshot, lvol_clone, caused_by=ec.CAUSED_BY_CLI):
    _snapshot_event(snapshot, f"Snapshot cloned: {snapshot.get_id()} clone id: {lvol_clone.get_id()}", caused_by, ec.EVENT_STATUS_CHANGE)

