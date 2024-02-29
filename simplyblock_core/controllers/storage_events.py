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


def snode_remove(node):
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


def device_health_check_change(cluster_id, device, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=device,
        caused_by=caused_by,
        message=f"Device health check changed from: {old_status} to: {new_state}",
        node_id=device.get_id())


def lvol_health_check_change(cluster_id, lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=lvol,
        caused_by=caused_by,
        message=f"LVol health check changed from: {old_status} to: {new_state}",
        node_id=lvol.get_id())


def lvol_status_change(cluster_id, lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=lvol,
        caused_by=caused_by,
        message=f"LVol status changed from: {old_status} to: {new_state}",
        node_id=lvol.get_id())


def device_status_change(cluster_id, device, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    ec.log_event_cluster(
        cluster_id=cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=device,
        caused_by=caused_by,
        message=f"Device status changed from: {old_status} to: {new_state}",
        node_id=device.get_id())
