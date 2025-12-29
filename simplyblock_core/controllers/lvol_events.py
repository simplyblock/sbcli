# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core import utils, constants
from fastapi import Request

logger = logging.getLogger()


def _lvol_event(lvol, message, caused_by, event):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(lvol.node_id)
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ec.log_event_cluster(
        cluster_id=snode.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=lvol,
        caused_by=caused_by,
        message=message,
        node_id=lvol.get_id())
    if cluster.mode == "kubernetes":
        pool = db_controller.get_pool_by_id(lvol.pool_uuid)

        if ec.EVENT_OBJ_CREATED:
            crypto_key=(
                (lvol.crypto_key1, lvol.crypto_key2)
                if lvol.crypto_key1 and lvol.crypto_key2
                else None
            )

            node_urls = [
                f"{constants.WEBAPI_K8S_ENDPOINT}/clusters/{snode.cluster_id}/storage-nodes/{node_id}/"
                for node_id in lvol.nodes
            ]

            utils.patch_cr_lvol_status(
                group=constants.CR_GROUP,
                version=constants.CR_VERSION,
                plural=pool.cr_plural,
                namespace=pool.cr_namespace,
                name=pool.cr_name,
                add={
                    "uuid": lvol.get_id(),
                    "lvolName": lvol.lvol_name,
                    "status": lvol.status,
                    "nodeUUID": node_urls,
                    "size": utils.humanbytes(lvol.size),
                    "health": lvol.health_check,
                    "isCrypto": crypto_key != None,
                    "nqn": lvol.nqn,
                    "subsysPort": lvol.subsys_port,
                    "hostname": lvol.hostname,
                    "fabric": lvol.fabric,
                    "ha": lvol.ha_type == 'ha',
                    "poolUUID": lvol.pool_uuid,
                    "poolName": lvol.pool_name,
                    "PvcName": lvol.pvc_name,
                    "snapName": lvol.snapshot_name,
                    "clonedFromSnap": lvol.cloned_from_snap,
                    "stripeWdata": lvol.ndcs,
                    "stripeWparity": lvol.npcs,
                    "blobID": lvol.blobid,
                    "namespaceID": lvol.ns_id,
                    "qosClass": lvol.lvol_priority_class,
                    "maxNamespacesPerSubsystem": lvol.max_namespace_per_subsys,
                    "qosIOPS": lvol.rw_ios_per_sec,
                    "qosRWTP": lvol.rw_mbytes_per_sec,
                    "qosRTP": lvol.r_mbytes_per_sec,
                    "qosWTP": lvol.w_mbytes_per_sec,
                },
            )

        elif ec.EVENT_STATUS_CHANGE:
            utils.patch_cr_lvol_status(
                group=constants.CR_GROUP,
                version=constants.CR_VERSION,
                plural=pool.lvols_cr_plural,
                namespace=pool.lvols_cr_namespace,
                name=pool.lvols_cr_name,
                lvol_uuid=lvol.get_id(),
                updates={"status": lvol.status, "health": lvol.health_check},
            )
        elif ec.EVENT_OBJ_DELETED:
            utils.patch_cr_lvol_status(
                group=constants.CR_GROUP,
                version=constants.CR_VERSION,
                plural=pool.lvols_cr_plural,
                namespace=pool.lvols_cr_namespace,
                name=pool.lvols_cr_name,
                lvol_uuid=lvol.get_id(),
                remove=True,
            )

def lvol_create(lvol, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, "LVol created", caused_by, ec.EVENT_OBJ_CREATED)


def lvol_delete(lvol, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, "LVol deleted", caused_by, ec.EVENT_OBJ_DELETED)


def lvol_status_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol status changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_migrate(lvol, old_node, new_node, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol migrated from: {old_node} to: {new_node}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_health_check_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol health check changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def lvol_io_error_change(lvol, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _lvol_event(lvol, f"LVol IO Error changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)

