import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.models.backup import DBBackup

logger = logging.getLogger()


def _backup_event(cluster_id, obj, message, caused_by, event):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=obj,
        caused_by=caused_by,
        message=message)


def fdb_backup_created(backup_obj: DBBackup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(backup_obj.cluster_id, backup_obj,"FDB Backup created", caused_by, ec.EVENT_OBJ_CREATED)


def fdb_backup_restored(backup_obj: DBBackup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(backup_obj.cluster_id, backup_obj,f"FDB Backup restored: {backup_obj.backup_name}", caused_by, ec.EVENT_STATUS_CHANGE)


def fdb_backup_failed(cluster_id, task, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, task, f"FDB Backup failed: {task.get_id()}", caused_by, ec.EVENT_STATUS_CHANGE)


