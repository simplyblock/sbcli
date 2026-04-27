# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec

logger = logging.getLogger()


def _backup_event(cluster_id, node_id, backup, message, caused_by, event):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=backup,
        caused_by=caused_by,
        message=message,
        node_id=node_id)


def backup_created(cluster_id, node_id, backup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup created: {backup.uuid}", caused_by, ec.EVENT_OBJ_CREATED)


def backup_completed(cluster_id, node_id, backup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup completed: {backup.uuid}", caused_by, ec.EVENT_STATUS_CHANGE)


def backup_failed(cluster_id, node_id, backup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup failed: {backup.uuid}: {backup.error_message}", caused_by, ec.EVENT_STATUS_CHANGE)


def backup_restore_completed(cluster_id, node_id, backup, lvol_name, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup restored: {backup.uuid} -> {lvol_name}", caused_by, ec.EVENT_STATUS_CHANGE)


def backup_restore_failed(cluster_id, node_id, backup, lvol_name, reason, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup restore failed: {backup.uuid} -> {lvol_name}: {reason}", caused_by, ec.EVENT_STATUS_CHANGE)


def backup_merge_completed(cluster_id, node_id, backup, old_backup_id, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup merged: {old_backup_id} into {backup.uuid}", caused_by, ec.EVENT_STATUS_CHANGE)


def backup_deleted(cluster_id, node_id, backup, caused_by=ec.CAUSED_BY_CLI):
    _backup_event(cluster_id, node_id, backup,
                  f"Backup chain deleted for lvol: {backup.lvol_id}", caused_by, ec.EVENT_OBJ_DELETED)
