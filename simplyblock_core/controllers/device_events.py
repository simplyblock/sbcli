# coding=utf-8
import logging

from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core import utils, constants

logger = logging.getLogger()


def _device_event(device, message, caused_by, event):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(device.node_id)
    ec.log_event_cluster(
        cluster_id=snode.cluster_id,
        domain=ec.DOMAIN_CLUSTER,
        event=event,
        db_object=device,
        caused_by=caused_by,
        message=message,
        node_id=device.get_id(),
        storage_id=device.cluster_device_order)

    if snode.mode == "kubernetes":
        total_devices = len(snode.nvme_devices)
        online_devices = 0
        for dev in snode.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1
        utils.patch_cr_node_status(
            group=constants.CR_GROUP,
            version=constants.CR_VERSION,
            plural=snode.cr_plural,
            namespace=snode.cr_namespace,
            name=snode.cr_name,
            node_uuid=snode.get_id(),
            updates={"devices": f"{total_devices}/{online_devices}"},
        )


def device_create(device, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, f"Device created: {device.get_id()}", caused_by, ec.EVENT_OBJ_CREATED)


def device_delete(device, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, f"Device deleted: {device.get_id()}", caused_by, ec.EVENT_OBJ_DELETED)


def device_health_check_change(device, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, f"Device health changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def device_status_change(device, new_state, old_status, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, f"Device status changed from: {old_status} to: {new_state}", caused_by, ec.EVENT_STATUS_CHANGE)


def device_restarted(device, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, f"Device restarted, status: {device.status}", caused_by, ec.EVENT_STATUS_CHANGE)


def device_reset(device, caused_by=ec.CAUSED_BY_CLI):
    _device_event(device, "Device reset", caused_by, ec.EVENT_STATUS_CHANGE)
