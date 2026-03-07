# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController

logger = logging.getLogger()
db_controller = DBController()


def _migration_event(migration, message, caused_by, event, event_level=ec.EventObj.LEVEL_INFO):
    ec.log_event_cluster(
        cluster_id=migration.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=migration,
        caused_by=caused_by,
        message=message,
        node_id=migration.source_node_id,
        event_level=event_level,
    )


def migration_created(migration, caused_by=ec.CAUSED_BY_CLI):
    _migration_event(
        migration,
        f"Volume migration created: lvol={migration.lvol_id} "
        f"src={migration.source_node_id} dst={migration.target_node_id}",
        caused_by,
        ec.EVENT_OBJ_CREATED,
    )


def migration_phase_changed(migration, caused_by=ec.CAUSED_BY_MONITOR):
    _migration_event(
        migration,
        f"Volume migration phase changed to '{migration.phase}': lvol={migration.lvol_id}",
        caused_by,
        ec.EVENT_STATUS_CHANGE,
    )


def migration_snap_copied(migration, snap_uuid, caused_by=ec.CAUSED_BY_MONITOR):
    _migration_event(
        migration,
        f"Snapshot copied to target: snap={snap_uuid} lvol={migration.lvol_id}",
        caused_by,
        ec.EVENT_STATUS_CHANGE,
    )


def migration_completed(migration, caused_by=ec.CAUSED_BY_MONITOR):
    _migration_event(
        migration,
        f"Volume migration completed: lvol={migration.lvol_id} "
        f"src={migration.source_node_id} dst={migration.target_node_id}",
        caused_by,
        ec.EVENT_STATUS_CHANGE,
    )


def migration_failed(migration, reason, caused_by=ec.CAUSED_BY_MONITOR):
    _migration_event(
        migration,
        f"Volume migration failed: lvol={migration.lvol_id} reason={reason}",
        caused_by,
        ec.EVENT_STATUS_CHANGE,
        event_level=ec.EventObj.LEVEL_ERROR,
    )


def migration_cancelled(migration, caused_by=ec.CAUSED_BY_CLI):
    _migration_event(
        migration,
        f"Volume migration cancelled: lvol={migration.lvol_id}",
        caused_by,
        ec.EVENT_STATUS_CHANGE,
        event_level=ec.EventObj.LEVEL_WARN,
    )
