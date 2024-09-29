# coding=utf-8
import threading
import time


from simplyblock_core import constants, kv_store, utils, rpc_client, distr_controller
from simplyblock_core.controllers import events_controller, device_controller, lvol_events
from simplyblock_core.models.lvol_model import LVol


from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.rpc_client import RPCClient


logger = utils.get_logger(__name__)


# get DB controller
db_controller = kv_store.DBController()


def process_device_event(event):
    if event.message in ['SPDK_BDEV_EVENT_REMOVE', "error_open", 'error_read', "error_write", "error_unmap"]:
        node_id = event.node_id
        storage_id = event.storage_id

        device = None
        for node in db_controller.get_storage_nodes():
            for dev in node.nvme_devices:
                if dev.cluster_device_order == storage_id:
                    if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
                        logger.info(f"The storage device is not online, skipping. status: {dev.status}")
                        event.status = 'skipped'
                        return

                    device = dev
                    break

        if not device:
            logger.info(f"Device not found!, storage id: {storage_id} from node: {node_id}")
            event.status = 'device_not_found'
            return

        device_id = device.get_id()
        node = db_controller.get_storage_node_by_id(node_id)
        if device.node_id != node_id:
            logger.info(f"Removing remote storage id: {storage_id} from node: {node_id}")
            new_remote_devices = []
            rpc_client = RPCClient(node.mgmt_ip, node.rpc_port,
                                   node.rpc_username, node.rpc_password)
            for rem_dev in node.remote_devices:
                if rem_dev.get_id() == device.get_id():
                    ctrl_name = rem_dev.remote_bdev[:-2]
                    rpc_client.bdev_nvme_detach_controller(ctrl_name)
                else:
                    new_remote_devices.append(rem_dev)
            node.remote_devices = new_remote_devices
            node.write_to_db(db_controller.kv_store)
            distr_controller.send_cluster_map_to_node(node)

        else:
            if event.message == 'SPDK_BDEV_EVENT_REMOVE':
                if device.node_id == node_id:
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
        # vuid = event.object_dict['vuid']
        node_id = event.node_id
        lvols = []
        for lv in db_controller.get_lvols():  # pass
            if lv.node_id == node_id:
                lvols.append(lv)

        if not lvols:
            logger.error(f"LVols on node {node_id} not found")
            event.status = 'lvols_not_found'
        else:
            for lvol in lvols:
                if lvol.status == LVol.STATUS_ONLINE:
                    logger.info("Setting LVol to offline")
                    lvol.io_error = True
                    old_status = lvol.status
                    lvol.status = LVol.STATUS_OFFLINE
                    lvol.write_to_db(db_controller.kv_store)
                    lvol_events.lvol_status_change(lvol, lvol.status, old_status, caused_by="monitor")
                    lvol_events.lvol_io_error_change(lvol, True, False, caused_by="monitor")
            event.status = 'processed'
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


def start_event_collector_on_node(node_id):
    logger.info(f"Starting Distr event collector on node: {node_id}")

    while True:

        snode = db_controller.get_storage_node_by_id(node_id)
        client = rpc_client.RPCClient(
            snode.mgmt_ip,
            snode.rpc_port,
            snode.rpc_username,
            snode.rpc_password,
            timeout=10, retry=2)

        try:
            events = client.distr_status_events_discard_then_get(0, constants.DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS)
            if events:
                logger.info(f"Found events: {len(events)}")
                event_ids = []
                for ev in events:
                    logger.debug(ev)
                    ev_id = events_controller.log_distr_event(snode.cluster_id, snode.get_id(), ev)
                    event_ids.append(ev_id)

                for eid in event_ids:
                    logger.info(f"Processing event: {eid}")
                    process_event(eid)

                logger.info(f"Discarding events: {len(events)}")
                client.distr_status_events_discard_then_get(len(events), 0)
            else:
                logger.info("no events found, sleeping")

        except Exception as e:
            logger.error("Failed to process distr events")
            logger.exception(e)

        time.sleep(constants.DISTR_EVENT_COLLECTOR_INTERVAL_SEC)



threads_maps = {}

while True:
    clusters = db_controller.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()

        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for snode in nodes:
            node_id = snode.get_id()
            # logger.info(f"Checking node {snode.hostname}")
            if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                t = threading.Thread(target=start_event_collector_on_node, args=(node_id,))
                t.start()
                threads_maps[node_id] = t

    time.sleep(5)
