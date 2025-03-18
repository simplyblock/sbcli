# coding=utf-8
import datetime
import threading
import time


from simplyblock_core import constants, db_controller, utils, rpc_client, distr_controller
from simplyblock_core.controllers import events_controller, device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)

utils.init_sentry_sdk()

# get DB controller
db_controller = db_controller.DBController()


def process_device_event(event):
    if event.message in ['SPDK_BDEV_EVENT_REMOVE', "error_open", 'error_read', "error_write", "error_unmap"]:
        node_id = event.node_id
        storage_id = event.storage_id
        event_node_obj = db_controller.get_storage_node_by_id(node_id)

        device_obj = None
        device_node_obj = None
        for node in db_controller.get_storage_nodes():
            for dev in node.nvme_devices:
                if dev.cluster_device_order == storage_id:
                    device_obj = dev
                    device_node_obj = node
                    break

        if not device_obj:
            logger.info(f"Device not found!, storage id: {storage_id} from node: {node_id}")
            event.status = 'device_not_found'
            return

        if device_obj.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
            logger.info(f"The device is not online, skipping. status: {device_obj.status}")
            event.status = 'skipped:dev_unav'
            distr_controller.send_dev_status_event(device_obj, device_obj.status, event_node_obj)
            return

        distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)

        if event_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            logger.info(f"Node is not online, skipping. status: {event_node_obj.status}")
            event.status = 'skipped:node_offline'
            return

        if device_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            logger.info(f"Node is not online, skipping. status: {device_node_obj.status}")
            event.status = f'skipped:device_node_{device_node_obj.status}'
            return

        if device_node_obj.get_id() == event_node_obj.get_id():
            if event.message == 'SPDK_BDEV_EVENT_REMOVE':
                logger.info(f"Removing storage id: {storage_id} from node: {node_id}")
                device_controller.device_remove(device_obj.get_id())

            elif event.message in ['error_write', 'error_unmap']:
                logger.info(f"Setting device to read-only")
                device_controller.device_set_read_only(device_obj.get_id())

            else:
                logger.info(f"Setting device to unavailable")
                device_controller.device_set_unavailable(device_obj.get_id())
                device_controller.device_set_io_error(device_obj.get_id(), True)

        else:
            event_node_obj = db_controller.get_storage_node_by_id(event_node_obj.get_id())
            for dev in event_node_obj.remote_devices:
                if dev.get_id() == device_obj.get_id():
                    event_node_obj.remote_devices.remove(dev)
                    event_node_obj.write_to_db()
                    break

            # check other nodes
            node_not_connected = 0
            for node in db_controller.get_storage_nodes_by_cluster_id(event_node_obj.cluster_id):
                if node.status == StorageNode.STATUS_ONLINE and node.get_id() != device_node_obj.get_id():
                    found = False
                    for remote_device in node.remote_devices:
                        if remote_device.get_id() == device_obj.get_id():
                            found = True
                            break
                    if not found:
                        node_not_connected += 1

            if node_not_connected >= 2:
                logger.info(f"Setting device to unavailable")
                device_controller.device_set_unavailable(device_obj.get_id())
                device_controller.device_set_io_error(device_obj.get_id(), True)

        event.status = 'processed'


def process_lvol_event(event):
    if event.message in ["error_open", 'error_read', "error_write", "error_unmap"]:
        vuid = event.object_dict['vuid']
        node_id = event.node_id
        # storage_node_ops.set_node_status(node_id, StorageNode.STATUS_SUSPENDED)
        # event_node_obj = db_controller.get_storage_node_by_id(node_id)
        # tasks_controller.add_node_to_auto_restart(event_node_obj)

        # lvols = []
        # for lv in db_controller.get_lvols():  # pass
        #     if lv.node_id == node_id:
        #         lvols.append(lv)
        #
        # if not lvols:
        #     logger.error(f"LVols on node {node_id} not found")
        #     event.status = 'lvols_not_found'
        # else:
        #     for lvol in lvols:
        #         if lvol.status == LVol.STATUS_ONLINE:
        #             logger.info("Setting LVol to offline")
        #             lvol.io_error = True
        #             old_status = lvol.status
        #             lvol.status = LVol.STATUS_OFFLINE
        #             lvol.write_to_db(db_controller.kv_store)
        #             lvol_events.lvol_status_change(lvol, lvol.status, old_status, caused_by="monitor")
        #             lvol_events.lvol_io_error_change(lvol, True, False, caused_by="monitor")
        event.status = f'distr error {vuid}'
    else:
        logger.error(f"Unknown event message: {event.message}")
        event.status = "event_unknown"


def process_event(event):
    if event.event == "device_status":
        if event.storage_id >= 0:
            process_device_event(event)

        if event.vuid >= 0:
            process_lvol_event(event)

    event.write_to_db(db_controller.kv_store)


def start_event_collector_on_node(node_id):
    logger.info(f"Starting Distr event collector on node: {node_id}")

    snode = db_controller.get_storage_node_by_id(node_id)
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
            while True:
                try:
                    events = client.distr_status_events_discard_then_get(
                        0, constants.DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS * page)
                    if events:
                        logger.info(f"Found events: {len(events)}")
                        for event_dict in events:
                            if "storage_ID" in event_dict:
                                sid = event_dict['storage_ID']
                            elif "vuid" in event_dict:
                                sid = event_dict['vuid']
                            et = event_dict['event_type']
                            msg = event_dict['status']
                            if sid not in events_groups:
                                events_groups[sid] = {et:{msg: 1}}
                            elif et not in events_groups[sid]:
                                events_groups[sid][et]: {msg: 1}
                            elif msg not in events_groups[sid][et]:
                                events_groups[sid][et][msg]: 1
                            else:
                                events_groups[sid][et][msg].count += 1
                                events_groups[sid][et][msg].write_to_db()
                                logger.info(f"Event {msg} already processed")
                                continue

                            event = events_controller.log_distr_event(snode.cluster_id, snode.get_id(), event_dict)
                            logger.info(f"Processing event: {event.get_id()}")
                            process_event(event)
                            events_groups[sid][et][msg] = event

                        logger.info(f"Discarding events: {len(events)}")
                        client.distr_status_events_discard_then_get(len(events), 0)
                        page *= 10
                    else:
                        logger.info("no events found, sleeping")
                        break
                except Exception as e:
                    logger.error("Failed to process distr events")
                    logger.exception(e)
                    break

            time.sleep(constants.DISTR_EVENT_COLLECTOR_INTERVAL_SEC)
    except Exception as e:
        logger.error(e)

    logger.info(f"Stopping Distr event collector on node: {node_id}")


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
