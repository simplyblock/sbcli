# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec


logger = logging.getLogger()


def snode_add(node):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_OBJ_CREATED,
        db_object=node,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Storage node created {node.get_id()}",
        node_id=node.get_id())


def snode_delete(node):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_OBJ_DELETED,
        db_object=node,
        caused_by=ec.CAUSED_BY_CLI,
        message=f"Storage node deleted {node.get_id()}",
        node_id=node.get_id())


def snode_status_change(node, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        message=f"Storage node status changed from: {old_status} to: {new_state}",
        node_id=node.get_id())


def snode_health_check_change(node, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        message=f"Storage node health check changed from: {old_status} to: {new_state}",
        node_id=node.get_id())



