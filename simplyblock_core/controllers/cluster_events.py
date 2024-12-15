# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.events import EventObj

logger = logging.getLogger()
db_controller = DBController()


def cluster_create(cluster):
    ec.log_event_cluster(
        cluster_id=cluster.get_id(),
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_OBJ_CREATED,
        db_object=cluster,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Cluster created {cluster.get_id()}")


def cluster_status_change(cluster, new_state, old_status):
    ec.log_event_cluster(
        cluster_id=cluster.get_id(),
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=cluster,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Cluster status changed from {old_status} to {new_state}")


def _cluster_cap_event(cluster, msg, event_level):
    ec.log_event_cluster(
        cluster_id=cluster.get_id(),
        node_id=cluster.get_id(),
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_CAPACITY,
        db_object=cluster,
        caused_by=ec.CAUSED_BY_MONITOR,
        message=msg,
        event_level=event_level)


def cluster_cap_warn(cluster, util):
    msg = f"Cluster absolute capacity reached: {util}%"
    _cluster_cap_event(cluster, msg, event_level=EventObj.LEVEL_WARN)


def cluster_cap_crit(cluster, util):
    msg = f"Cluster absolute capacity reached: {util}%"
    _cluster_cap_event(cluster, msg, event_level=EventObj.LEVEL_CRITICAL)


def cluster_prov_cap_warn(cluster, util):
    msg = f"Cluster provisioned capacity reached: {util}%"
    _cluster_cap_event(cluster, msg, event_level=EventObj.LEVEL_WARN)


def cluster_prov_cap_crit(cluster, util):
    msg = f"Cluster provisioned capacity reached: {util}%"
    _cluster_cap_event(cluster, msg, event_level=EventObj.LEVEL_CRITICAL)


def cluster_delete(cluster):
    ec.log_event_cluster(
        cluster_id=cluster.get_id(),
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_OBJ_DELETED,
        db_object=cluster,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Cluster deleted {cluster.get_id()}")
