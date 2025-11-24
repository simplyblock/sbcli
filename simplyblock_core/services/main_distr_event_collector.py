# coding=utf-8
import logging
import sys
import threading
import time


from simplyblock_core import constants, db_controller, utils, rpc_client, distr_controller
from simplyblock_core.controllers import events_controller, device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


utils.init_sentry_sdk()

# get DB controller
db = db_controller.DBController()

EVENTS_LIST = ['SPDK_BDEV_EVENT_REMOVE', "error_open", 'error_read', "error_write", "error_unmap",
               "error_write_cannot_allocate"]


def remove_remote_device_from_node(node_id, device_id):
    node = db.get_storage_node_by_id(node_id)
    for remote_dev in node.remote_devices:
        if remote_dev.get_id() == device_id:
            node.remote_devices.remove(remote_dev)
            node.write_to_db()
            break


def process_device_event(event, logger):
    if event.message in EVENTS_LIST:
        node_id = event.node_id
        storage_id = event.storage_id
        event_node_obj = db.get_storage_node_by_id(node_id)

        device_obj = None
        device_node_obj = None
        for node in db.get_storage_nodes():
            for dev in node.nvme_devices:
                if dev.cluster_device_order == storage_id:
                    device_obj = dev
                    device_node_obj = node
                    break

        if device_obj is None or device_node_obj is None:
            logger.info(f"Device not found!, storage id: {storage_id} from node: {node_id}")
            event.status = 'device_not_found'
            return

        if device_obj.is_connection_in_progress_to_node(event_node_obj.get_id()):
            logger.warning("Connection attempt was found from node to device, sleeping 5 seconds")
            time.sleep(5)

        device_obj.lock_device_connection(event_node_obj.get_id())

        if device_obj.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                     NVMeDevice.STATUS_CANNOT_ALLOCATE]:
            logger.info(f"The device is not online, skipping. status: {device_obj.status}")
            event.status = f'skipped:dev_{device_obj.status}'
            distr_controller.send_dev_status_event(device_obj, device_obj.status, event_node_obj)
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return


        if event_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            logger.info(f"Node is not online, skipping. status: {event_node_obj.status}")
            event.status = 'skipped:node_offline'
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return

        if device_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            logger.info(f"Node is not online, skipping. status: {device_node_obj.status}")
            event.status = f'skipped:device_node_{device_node_obj.status}'
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return


        if device_node_obj.get_id() == event_node_obj.get_id():
            if event.message in ['SPDK_BDEV_EVENT_REMOVE', 'error_open']:
                logger.info(f"Removing storage id: {storage_id} from node: {node_id}")
                device_controller.device_remove(device_obj.get_id())

            elif event.message in ['error_write', 'error_unmap']:
                logger.info("Setting device to read-only")
                device_controller.device_set_read_only(device_obj.get_id())

            elif event.message == 'error_write_cannot_allocate':
                logger.info("Setting device to cannot_allocate")
                device_controller.device_set_state(device_obj.get_id(), NVMeDevice.STATUS_CANNOT_ALLOCATE)

            else:
                logger.info("Setting device to unavailable")
                device_controller.device_set_unavailable(device_obj.get_id())
                device_controller.device_set_io_error(device_obj.get_id(), True)
        else:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())

        event.status = 'processed'
        device_obj.release_device_connection()


def process_lvol_event(event, logger):
    if event.message in ["error_open", 'error_read', "error_write", "error_unmap"]:
        vuid = event.object_dict['vuid']
        event.status = f'distr error {vuid}'
    else:
        logger.error(f"Unknown event message: {event.message}")
        event.status = "event_unknown"


def process_event(event, logger):
    if event.event == "device_status":
        if event.storage_id >= 0:
            process_device_event(event, logger)

        if event.vuid >= 0:
            process_lvol_event(event, logger)

    event.write_to_db(db.kv_store)


def start_event_collector_on_node(node_id):
    snode = db.get_storage_node_by_id(node_id)
    logger = logging.getLogger()
    logger.setLevel("DEBUG")
    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(logging.Formatter(f'%(asctime)s: node:{snode.mgmt_ip} %(levelname)s: %(message)s'))
    logger.addHandler(logger_handler)

    logger.info(f"Starting Distr event collector on node: {node_id}")

    client = rpc_client.RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password,
        timeout=2, retry=2)

    try:
        while True:
            page = 1
            events_groups = {}
            events_list = []
            while True:
                try:
                    events = client.distr_status_events_discard_then_get(
                        0, constants.DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS * page)
                    if events is False:
                        logger.error("No events received")
                        return

                    if events:
                        logger.info(f"Found events: {len(events)}")
                        for event_dict in events:
                            if "storage_ID" in event_dict:
                                sid = event_dict['storage_ID']
                            elif "vuid" in event_dict:
                                sid = event_dict['vuid']
                            else:
                                logger.error(f"Unknown event: {event_dict}")
                                continue

                            # Ignore type errors, this can be simplified to avoid them
                            et = event_dict['event_type']
                            msg = event_dict['status']
                            if sid not in events_groups:
                                events_groups[sid] = {et:{msg: 1}}
                            elif et not in events_groups[sid]:
                                events_groups[sid][et]: {msg: 1}  # type: ignore
                            elif msg not in events_groups[sid][et]:
                                events_groups[sid][et][msg]: 1  # type: ignore
                            else:
                                events_groups[sid][et][msg].count += 1  # type: ignore
                                continue

                            event = events_controller.log_distr_event(snode.cluster_id, snode.get_id(), event_dict)
                            logger.info(f"Processing event: {event.get_id()}")
                            process_event(event, logger)
                            events_groups[sid][et][msg] = event
                            events_list.append(event)

                        for ev in events_list:
                            if ev.count > 1 :
                                ev.write_to_db(db.kv_store)

                        logger.info(f"Discarding events: {len(events)}")
                        client.distr_status_events_discard_then_get(len(events), 0)
                        page *= 10
                    else:
                        logger.info("no events found, sleeping")
                        break
                except Exception as e:
                    logger.error(f"Failed to process distr events: {e}")
                    break

            time.sleep(constants.DISTR_EVENT_COLLECTOR_INTERVAL_SEC)
    except Exception as e:
        logger.error(e)

    logger.info(f"Stopping Distr event collector on node: {node_id}")


threads_maps: dict[str, threading.Thread] = {}

while True:
    clusters = db.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()

        nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
        for snode in nodes:
            node_id = snode.get_id()
            # logger.info(f"Checking node {snode.hostname}")
            if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                t = threading.Thread(target=start_event_collector_on_node, args=(node_id,))
                t.start()
                threads_maps[node_id] = t

    time.sleep(5)
