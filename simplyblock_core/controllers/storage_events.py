# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.models.events import EventObj
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils, constants

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
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(node.cluster_id)
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        message=f"Storage node status changed from: {old_status} to: {new_state}",
        node_id=node.get_id())
    if cluster.mode == "kubernetes":
        utils.patch_cr_node_status(
            group=constants.CR_GROUP,
            version=constants.CR_VERSION,
            plural=node.cr_plural,
            namespace=node.cr_namespace,
            name=node.cr_name,
            node_uuid=node.get_id(),
            node_mgmt_ip=node.mgmt_ip,
            updates={"status": new_state},
        )


def snode_health_check_change(node, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(node.cluster_id)
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        message=f"Storage node health check changed from: {old_status} to: {new_state}",
        node_id=node.get_id())
    if cluster.mode == "kubernetes":
        utils.patch_cr_node_status(
            group=constants.CR_GROUP,
            version=constants.CR_VERSION,
            plural=node.cr_plural,
            namespace=node.cr_namespace,
            name=node.cr_name,
            node_uuid=node.get_id(),
            node_mgmt_ip=node.mgmt_ip,
            updates={"health": str(new_state)},
        )

def snode_restart_failed(node):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=ec.CAUSED_BY_CLI,
        message="Storage node LVStore recovery failed",
        node_id=node.get_id())


def snode_rpc_timeout(node, timeout_seconds, caused_by=ec.CAUSED_BY_MONITOR):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        event_level=EventObj.LEVEL_WARN,
        message=f"Storage node RPC timeout detected after {timeout_seconds} seconds",
        node_id=node.get_id())


def jm_repl_tasks_found(node, jm_vuid, caused_by=ec.CAUSED_BY_MONITOR):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        event_level=EventObj.LEVEL_WARN,
        message=f"JM replication task found for jm {jm_vuid}",
        node_id=node.get_id())


def node_ports_changed(node, caused_by=ec.CAUSED_BY_MONITOR):
    ec.log_event_cluster(
        cluster_id=node.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=ec.EVENT_STATUS_CHANGE,
        db_object=node,
        caused_by=caused_by,
        event_level=EventObj.LEVEL_WARN,
        message=f"Storage node ports set, LVol:{node.lvol_subsys_port} RPC:{node.rpc_port} Internal:{node.nvmf_port}",
        node_id=node.get_id())
