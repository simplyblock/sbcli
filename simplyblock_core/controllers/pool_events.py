# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec


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

