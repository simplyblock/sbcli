import logging

from simplyblock_core.controllers import events_controller as ec

logger = logging.getLogger()


def _backup_event(cluster_id, backup_name, message, caused_by, event):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=backup_name,
        caused_by=caused_by,
        message=message)


def fdb_backup_created(cluster_id, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, "","FDB Backup created", caused_by, ec.EVENT_OBJ_CREATED)


def fdb_backup_restored(cluster_id, backup_name, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, backup_name,f"FDB Backup restored: {backup_name}", caused_by, ec.EVENT_STATUS_CHANGE)


def fdb_backup_failed(cluster_id,task_id,  caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id,task_id, f"FDB Backup failed: {task_id}", caused_by, ec.EVENT_STATUS_CHANGE)


