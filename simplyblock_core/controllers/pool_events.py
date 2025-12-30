# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils, constants

logger = logging.getLogger()


def _add(pool, msg, event):
    ec.log_event_cluster(
        cluster_id=pool.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=pool,
        caused_by=ec.CAUSED_BY_CLI,
        message=msg,
        node_id=pool.cluster_id)


def pool_add(pool):
    _add(pool, f"Pool created {pool.pool_name}", event=ec.EVENT_OBJ_CREATED)


def pool_remove(pool):
    _add(pool, f"Pool deleted {pool.pool_name}", event=ec.EVENT_OBJ_DELETED)


def pool_updated(pool):
    _add(pool, f"Pool updated {pool.pool_name}", event=ec.EVENT_STATUS_CHANGE)


def pool_status_change(pool, new_state, old_status):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(pool.cluster_id)
    ec.log_event_cluster(
        cluster_id=pool.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=pool,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Pool status changed from {old_status} to {new_state}",
        node_id=pool.cluster_id)

    if cluster.mode == "kubernetes":
        utils.patch_cr_status(
            group=constants.CR_GROUP,
            version=constants.CR_VERSION,
            plural=pool.cr_plural,
            namespace=pool.cr_namespace,
            name=pool.cr_name,
            status_patch={"status": new_state})
