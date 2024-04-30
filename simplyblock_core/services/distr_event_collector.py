# coding=utf-8
import logging
import os

import time
import sys


from simplyblock_core import constants, kv_store, utils, rpc_client
from simplyblock_core.controllers import events_controller, device_controller, lvol_events
from simplyblock_core.models.lvol_model import LVol

# Import the GELF logger
from graypy import GELFUDPHandler

from simplyblock_core.models.nvme_device import NVMeDevice

# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_controller = kv_store.DBController()


def process_device_event(event):
    if event.message in ['SPDK_BDEV_EVENT_REMOVE', "error_open", 'error_read', "error_write", "error_unmap"]:
        node_id = event.node_id
        storage_id = event.storage_id

        nodes = db_controller.get_storage_nodes()
        device_id = None
        for node in nodes:
            for dev in node.nvme_devices:
                if dev.cluster_device_order == storage_id:
                    if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
                        logger.info(f"The storage device is not online, skipping. status: {dev.status}")
                        event.status = 'skipped'
                        return

                    # if node.get_id() != node_id:
                    #     logger.info(f"The storage device is remote, skipping")
                    #     event.status = 'skipped-remote'
                    #     return

                    device_id = dev.get_id()
                    break

        if not device_id:
            logger.info(f"Device not found!, storage id: {storage_id} from node: {node_id}")
            event.status = 'device_not_found'
            return

        if event.message == 'SPDK_BDEV_EVENT_REMOVE':
            logger.info(f"Removing storage id: {storage_id} from node: {node_id}")
            device_controller.device_remove(device_id)
        elif event.message in ['error_write', 'error_unmap']:
            logger.info(f"Setting device to read-only")
            device_controller.device_set_io_error(device_id, True)
            device_controller.device_set_read_only(device_id)
        else:
            logger.info(f"Setting device to unavailable")
            device_controller.device_set_io_error(device_id, True)
            device_controller.device_set_unavailable(device_id)

        event.status = 'processed'


def process_lvol_event(event):
    if event.message in ["error_open", 'error_read', "error_write", "error_unmap"]:
        vuid = event.object_dict['vuid']
        lvol = None
        for lv in db_controller.get_lvols():
            if lv.vuid == vuid:
                lvol = lv
                break

        if not lvol:
            logger.error(f"LVol with vuid {vuid} not found")
            event.status = 'lvol_not_found'
        else:
            lvol.io_error = True
            if lvol.status == LVol.STATUS_ONLINE:
                logger.info("Setting LVol to offline")
                old_status = lvol.status
                lvol.status = LVol.STATUS_OFFLINE
                lvol.write_to_db(db_controller.kv_store)
                lvol_events.lvol_status_change(lvol, lvol.status, old_status, caused_by="monitor")
                lvol_events.lvol_io_error_change(lvol, True, False, caused_by="monitor")
                event.status = 'processed'
            else:
                event.status = 'skipped'
    else:
        logger.error(f"Unknown LVol event message: {event.message}")
        event.status = "event_unknown"


def process_event(event_id):
    event = db_controller.get_events(event_id)[0]
    if event.event == "device_status":
        if event.storage_id >= 0:
            process_device_event(event)

        if event.vuid >= 0:
            process_lvol_event(event)

    event.write_to_db(db_controller.kv_store)


hostname = utils.get_hostname()
logger.info("Starting Distr event collector...")
logger.info(f"Node:{hostname}")
while True:
    time.sleep(constants.DISTR_EVENT_COLLECTOR_INTERVAL_SEC)

    snode = db_controller.get_storage_node_by_hostname(hostname)
    if not snode:
        logger.error("This node is not part of the cluster, hostname: %s" % hostname)
        continue

    client = rpc_client.RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password,
        timeout=3, retry=2
    )

    events = client.distr_status_events_get()
    if not events:
        logger.error("Failed to get distr events")
        continue

    logger.info(f"Found events: {len(events)}")
    event_ids = []
    for ev in events:
        logger.debug(ev)
        ev_id = events_controller.log_distr_event(snode.cluster_id, snode.get_id(), ev)
        event_ids.append(ev_id)

    for eid in event_ids:
        logger.info(f"Processing event: {eid}")
        process_event(eid)
