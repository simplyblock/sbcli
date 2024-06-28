import time
import logging

from simplyblock_core import distr_controller, utils, storage_node_ops
from simplyblock_core.controllers import device_events, lvol_controller, tasks_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.rpc_client import RPCClient


logger = logging.getLogger()


def device_set_state(device_id, state):
    db_controller = DBController()
    dev = db_controller.get_storage_devices(device_id)
    if not dev:
        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    for dev in snode.nvme_devices:
        if dev.get_id() == device_id:
            device = dev
            break

    if device.status == state:
        return True

    if state == NVMeDevice.STATUS_ONLINE:
        device.retries_exhausted = False

    old_status = dev.status
    device.status = state
    distr_controller.send_dev_status_event(device, device.status)
    snode.write_to_db(db_controller.kv_store)
    device_events.device_status_change(device, device.status, old_status)
    return True


def device_set_io_error(device_id, is_error):
    db_controller = DBController()
    dev = db_controller.get_storage_devices(device_id)
    if not dev:
        logger.error("device not found")

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    device = None
    for dev in snode.nvme_devices:
        if dev.get_id() == device_id:
            device = dev
            break

    if not device:
        logger.error("device not found")

    if device.io_error == is_error:
        return True

    device.io_error = is_error
    snode.write_to_db(db_controller.kv_store)
    return True


def device_set_unavailable(device_id):
    return device_set_state(device_id, NVMeDevice.STATUS_UNAVAILABLE)


def device_set_read_only(device_id):
    return device_set_state(device_id, NVMeDevice.STATUS_READONLY)


def device_set_online(device_id):
    ret = device_set_state(device_id, NVMeDevice.STATUS_ONLINE)
    if ret:
        logger.info("Adding task to device data migration")
        task_id = tasks_controller.add_device_mig_task(device_id)
        if task_id:
            logger.info(f"Task id: {task_id}")
    return ret


def get_alceml_name(alceml_id):
    return f"alceml_{alceml_id}"


def _def_create_device_stack(device_obj, snode, force=False):

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=600
    )

    test_name = f"{device_obj.nvme_bdev}_test"
    # create testing bdev
    ret = rpc_client.bdev_passtest_create(test_name, device_obj.nvme_bdev)
    if not ret:
        logger.error(f"Failed to create bdev: {test_name}")
        if not force:
            return False

    alceml_id = device_obj.get_id()
    alceml_name = get_alceml_name(alceml_id)
    logger.info(f"adding {alceml_name}")
    ret = rpc_client.bdev_alceml_create(alceml_name, test_name, alceml_id, pba_init_mode=2,
                                        dev_cpu_mask=snode.dev_cpu_mask)
    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        if not force:
            return False

    # add pass through
    pt_name = f"{alceml_name}_PT"
    ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_name)
    if not ret:
        logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
        if not force:
            return False

    subsystem_nqn = snode.subsystem + ":dev:" + alceml_id
    logger.info("Creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', alceml_id)
    IP = None
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            ret = rpc_client.transport_list()
            found = False
            if ret:
                for ty in ret:
                    if ty['trtype'] == tr_type:
                        found = True
            if found is False:
                ret = rpc_client.transport_create(tr_type)
            # logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
            IP = iface.ip4_address
            break
    logger.info(f"Adding {pt_name} to the subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)

    if hasattr(device_obj, 'jm_bdev') and device_obj.jm_bdev:
        ret = rpc_client.bdev_jm_create(device_obj.jm_bdev, device_obj.alceml_bdev,
                                        dev_cpu_mask=snode.dev_cpu_mask)
        if not ret:
            logger.error(f"Failed to create jm bdev: {device_obj.jm_bdev}")
            if not force:
                return False

    device_obj.testing_bdev = test_name
    device_obj.alceml_bdev = alceml_name
    device_obj.pt_bdev = pt_name
    device_obj.nvmf_nqn = subsystem_nqn
    device_obj.nvmf_ip = IP
    device_obj.nvmf_port = 4420
    return True


def restart_device(device_id, force=False):
    db_controller = DBController()
    dev = db_controller.get_storage_devices(device_id)
    if not dev:
        logger.error("device not found")

    if dev.status != NVMeDevice.STATUS_REMOVED:
        logger.error("Device must be in removed status")
        if not force:
            return False

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    device_obj = None
    for dev in snode.nvme_devices:
        if dev.get_id() == device_id:
            device_obj = dev
            break

    logger.info(f"Restarting device {device_id}")
    device_set_unavailable(device_id)

    ret = _def_create_device_stack(device_obj, snode, force=force)

    if not ret:
        logger.error("Failed to create device stack")
        if not force:
            return False

    logger.info("Make other nodes connect to the device")
    snodes = db_controller.get_storage_nodes()
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id():
            continue
        if node.status != snode.STATUS_ONLINE:
            continue

        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        name = f"remote_{device_obj.alceml_bdev}"
        ret = rpc_client.bdev_nvme_attach_controller_tcp(name, device_obj.nvmf_nqn, device_obj.nvmf_ip,
                                                         device_obj.nvmf_port)
        if not ret:
            logger.error(f"Failed to connect to device: {name}")
            continue

        device_obj.remote_bdev = f"{name}n1"
        idx = -1
        for i, d in enumerate(node.remote_devices):
            if d.get_id() == device_obj.get_id():
                idx = i
                break
        if idx >= 0:
            node.remote_devices[idx] = device_obj
        else:
            node.remote_devices.append(device_obj)
        node.write_to_db(db_controller.kv_store)
        time.sleep(3)

    logger.info("Setting device io_error to False")
    device_set_io_error(device_id, False)
    logger.info("Setting device online")
    device_set_online(device_id)
    device_events.device_restarted(device_obj)
    return "Done"


def set_device_testing_mode(device_id, mode):
    db_controller = DBController()
    device = db_controller.get_storage_devices(device_id)
    if not device:
        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error("node not found")
        return False

    logger.info(f"Set device:{device_id} Test mode:{mode}")
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    ret = rpc_client.bdev_passtest_mode(device.testing_bdev, mode)
    return ret


def device_remove(device_id, force=True):
    db_controller = DBController()
    dev = db_controller.get_storage_devices(device_id)
    if not dev:
        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    for dev in snode.nvme_devices:
        if dev.get_id() == device_id:
            device = dev
            break

    logger.info("Sending device event")
    distr_controller.send_dev_status_event(device, NVMeDevice.STATUS_REMOVED)

    logger.info("Disconnecting device from all nodes")
    distr_controller.disconnect_device(device)

    logger.info("Removing device fabric")
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    ret = rpc_client.subsystem_delete(device.nvmf_nqn)
    if not ret:
        logger.error(f"Failed to remove subsystem: {device.nvmf_nqn}")
        if not force:
            return False

    logger.info("Removing device bdevs")
    ret = rpc_client.bdev_PT_NoExcl_delete(f"{device.alceml_bdev}_PT")
    if not ret:
        logger.error(f"Failed to remove bdev: {device.alceml_bdev}_PT")
        if not force:
            return False
    ret = rpc_client.bdev_alceml_delete(device.alceml_bdev)
    if not ret:
        logger.error(f"Failed to remove bdev: {device.alceml_bdev}")
        if not force:
            return False

    ret = rpc_client.bdev_passtest_delete(device.testing_bdev)
    if not ret:
        logger.error(f"Failed to remove bdev: {device.testing_bdev}")
        if not force:
            return False

    device.status = 'removed'
    snode.write_to_db(db_controller.kv_store)
    device_events.device_delete(device)

    for lvol in db_controller.get_lvols():
        lvol_controller.send_cluster_map(lvol.get_id())

    return True


def get_device(device_id):
    db_controller = DBController()
    device = db_controller.get_storage_devices(device_id)
    if not device:
        logger.error("device not found")
        return False
    out = [device.get_clean_dict()]
    return utils.print_table(out)


def get_device_capacity(device_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    device = db_controller.get_storage_devices(device_id)
    if not device:
        logger.error("device not found")

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            return False
    else:
        records_number = 20

    records = db_controller.get_device_capacity(device, records_number)
    records_list = utils.process_records(records, records_count)

    if not parse_sizes:
        return records_list

    out = []
    for record in records_list:
        logger.debug(record)
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Absolut": utils.humanbytes(record['size_total']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
        })
    return out


def get_device_iostats(device_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    device = db_controller.get_storage_devices(device_id)
    if not device:
        logger.error(f"Device not found: {device_id}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records_list = db_controller.get_device_stats(device, records_number)
    new_records = utils.process_records(records_list, records_count)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record["read_io_ps"],
            "Read lat": record["read_latency_ps"],
            "Write speed": utils.humanbytes(record["write_bytes_ps"]),
            "Write IOPS": record["write_io_ps"],
            "Write lat": record["write_latency_ps"],
        })
    return out


def reset_storage_device(dev_id):
    db_controller = DBController()
    device = db_controller.get_storage_devices(dev_id)
    if not device:
        logger.error(f"Device not found: {dev_id}")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error(f"Node not found {device.node_id}")
        return False

    if device.status == NVMeDevice.STATUS_REMOVED:
        logger.error(f"Device status: {device.status} is removed")
        return False

    logger.info("Setting devices to unavailable")
    device_set_unavailable(dev_id)
    devs = []
    for dev in snode.nvme_devices:
        if dev.get_id() == device.get_id():
            continue
        if dev.status == NVMeDevice.STATUS_ONLINE and dev.physical_label == device.physical_label:
            devs.append(dev)
            device_set_unavailable(dev.get_id())

    logger.info("Resetting device")
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    controller_name = device.nvme_controller
    response = rpc_client.reset_device(controller_name)
    if not response:
        logger.error(f"Failed to reset NVMe BDev {controller_name}")
        return False
    time.sleep(3)

    logger.info("Setting devices online")
    for dev in devs:
        device_set_online(dev.get_id())

    # set io_error flag False
    device_set_io_error(dev_id, False)
    device_set_retries_exhausted(dev_id, False)
    # set device to online
    device_set_online(dev_id)
    device_events.device_reset(device)
    return True


def device_set_retries_exhausted(device_id, retries_exhausted):
    db_controller = DBController()
    dev = db_controller.get_storage_devices(device_id)
    if not dev:
        logger.error("device not found")

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    device = None
    for dev in snode.nvme_devices:
        if dev.get_id() == device_id:
            device = dev
            break

    if not device:
        logger.error("device not found")

    if device.retries_exhausted == retries_exhausted:
        return True

    device.retries_exhausted = retries_exhausted
    snode.write_to_db(db_controller.kv_store)
    return True
