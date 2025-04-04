import time
import logging

from simplyblock_core import distr_controller, utils, storage_node_ops
from simplyblock_core.controllers import device_events, lvol_controller, tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


logger = logging.getLogger()


def device_set_state(device_id, state):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
    if not dev:
        logger.error("device not found")
        return False

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
        return False

    if state == NVMeDevice.STATUS_ONLINE:
        device.retries_exhausted = False

    if state == NVMeDevice.STATUS_REMOVED:
        device.deleted = True

    if state == NVMeDevice.STATUS_READONLY and device.status == NVMeDevice.STATUS_UNAVAILABLE:
        return False

    if device.status != state:
        old_status = dev.status
        device.status = state
        snode.write_to_db(db_controller.kv_store)
        device_events.device_status_change(device, device.status, old_status)

    if state == NVMeDevice.STATUS_ONLINE:
        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue
            node.remote_devices = storage_node_ops._connect_to_remote_devs(node)
            node.write_to_db()

    distr_controller.send_dev_status_event(device, device.status)

    return True


def device_set_io_error(device_id, is_error):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
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
    db_controller = DBController()

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=600)

    bdev_names = []
    for dev in rpc_client.get_bdevs():
        bdev_names.append(dev['name'])

    nvme_bdev = device_obj.nvme_bdev
    if snode.enable_test_device:
        test_name = f"{device_obj.nvme_bdev}_test"
        if test_name not in bdev_names:
            # create testing bdev
            ret = rpc_client.bdev_passtest_create(test_name, device_obj.nvme_bdev)
            if not ret:
                logger.error(f"Failed to create bdev: {test_name}")
                if not force:
                    return False
        else:
            logger.info(f"bdev already exists {test_name}")
        device_obj.testing_bdev = test_name
        nvme_bdev = test_name

    alceml_id = device_obj.get_id()
    alceml_name = get_alceml_name(alceml_id)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    write_protection = False
    if cluster.distr_ndcs > 1:
        write_protection = True
    if alceml_name not in bdev_names:
        logger.info(f"adding {alceml_name}")
        if snode.alceml_cpu_cores:
            alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
            ret = rpc_client.bdev_alceml_create(alceml_name, nvme_bdev, alceml_id, pba_init_mode=2,
                                                alceml_cpu_mask=alceml_cpu_mask,  pba_page_size=cluster.page_size_in_blocks, write_protection=write_protection)
            snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
        else:
            ret = rpc_client.bdev_alceml_create(alceml_name, nvme_bdev, alceml_id, pba_init_mode=2,
                                                pba_page_size=cluster.page_size_in_blocks, write_protection=write_protection)

        if not ret:
            logger.error(f"Failed to create alceml bdev: {alceml_name}")
            if not force:
                return False
    else:
        logger.info(f"bdev already exists {alceml_name}")

    # add pass through
    pt_name = f"{alceml_name}_PT"
    if pt_name not in bdev_names:
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_name)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            if not force:
                return False
    else:
        logger.info(f"bdev already exists {pt_name}")

    subsystem_nqn = snode.subsystem + ":dev:" + alceml_id
    namespace_found = False
    subsys_found = False
    ret = rpc_client.subsystem_list(subsystem_nqn)
    if ret :
        subsys_found = True
        if ret[0]["namespaces"]:
            for ns in ret[0]["namespaces"]:
                if ns['name'] == pt_name:
                    namespace_found = True
                    break

    if not subsys_found:
        logger.info("Creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', alceml_id)
        for iface in snode.data_nics:
            if iface.ip4_address:
                tr_type = iface.get_transport_type()
                ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
                device_obj.nvmf_ip = iface.ip4_address
                break
    else:
        logger.info(f"subsystem already exists {subsys_found}")

    if not namespace_found:
        logger.info(f"Adding {pt_name} to the subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
    else:
        logger.info(f"bdev already added to subsys {pt_name}")

    device_obj.alceml_bdev = alceml_name
    device_obj.alceml_name = alceml_name
    device_obj.pt_bdev = pt_name
    device_obj.nvmf_nqn = subsystem_nqn
    device_obj.nvmf_port = 4420
    return True


def restart_device(device_id, force=False):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
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

    task_id = tasks_controller.get_active_dev_restart_task(snode.cluster_id, device_id)
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not restart device")
        if force is False:
            return False

    logger.info(f"Restarting device {device_id}")
    device_set_retries_exhausted(device_id, True)
    device_set_unavailable(device_id)

    ret = _def_create_device_stack(device_obj, snode, force=force)

    if not ret:
        logger.error("Failed to create device stack")
        if not force:
            return False

    logger.info("Setting device io_error to False")
    device_set_io_error(device_id, False)
    logger.info("Setting device online")
    device_set_online(device_id)
    device_events.device_restarted(device_obj)

    # add to jm raid
    if snode.jm_device and snode.jm_device.raid_bdev and snode.jm_device.status != JMDevice.STATUS_REMOVED:
        # looking for jm partition
        rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
        jm_dev_part = f"{dev.nvme_bdev[:-1]}1"
        ret = rpc_client.get_bdevs(jm_dev_part)
        if ret:
            logger.info(f"JM part found: {jm_dev_part}")
            if snode.jm_device.status == JMDevice.STATUS_UNAVAILABLE:
                restart_jm_device(snode.jm_device.get_id(), force=True)

            if snode.jm_device.status == JMDevice.STATUS_ONLINE and \
                    jm_dev_part not in snode.jm_device.jm_nvme_bdev_list:
                remove_jm_device(snode.jm_device.get_id(), force=True)
                time.sleep(3)
                restart_jm_device(snode.jm_device.get_id(), force=True)

    return "Done"


def set_device_testing_mode(device_id, mode):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error("node not found")
        return False
    if not snode.enable_test_device:
        logger.error("Test device is disabled on this storage node")
        return False

    logger.info(f"Set device:{device_id} Test mode:{mode}")
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    ret = rpc_client.bdev_passtest_mode(device.testing_bdev, mode)
    return ret


# def set_jm_device_testing_mode(device_id, mode):
#     db_controller = DBController()
#     snode = db_controller.get_storage_by_jm_id(device_id)
#     if not snode:
#         logger.error("node not found")
#         return False
#     jm_device = snode.jm_device
#
#     if not snode.enable_test_device:
#         logger.error("Test device is disabled on this storage node")
#         return False
#
#     logger.info(f"Set device:{device_id} Test mode:{mode}")
#     # creating RPCClient instance
#     rpc_client = RPCClient(
#         snode.mgmt_ip, snode.rpc_port,
#         snode.rpc_username, snode.rpc_password)
#
#     ret = rpc_client.bdev_passtest_mode(jm_device.testing_bdev, mode)
#     return ret


def device_remove(device_id, force=True):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
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

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.error(f"Unsupported device status: {device.status}")
        return False

    task_id = tasks_controller.get_active_dev_restart_task(snode.cluster_id, device_id)
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not remove device")
        if force is False:
            return False

    logger.info("Setting device unavailable")
    device_set_unavailable(device_id)

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
    ret = rpc_client.qos_vbdev_delete(device.qos_bdev)
    if not ret:
        logger.error(f"Failed to remove bdev: {device.qos_bdev}")
        if not force:
            return False
    if snode.enable_test_device:
        ret = rpc_client.bdev_passtest_delete(device.testing_bdev)
        if not ret:
            logger.error(f"Failed to remove bdev: {device.testing_bdev}")
            if not force:
                return False

    device_set_state(device_id, NVMeDevice.STATUS_REMOVED)

    # remove device from jm raid
    if snode.jm_device.raid_bdev:
        nvme_controller = device.nvme_controller
        dev_to_remove = None
        for part in snode.jm_device.jm_nvme_bdev_list:
            if part.startswith(nvme_controller):
                dev_to_remove = part
                break

        if dev_to_remove:
            if snode.jm_device.status == NVMeDevice.STATUS_ONLINE:
                remove_jm_device(snode.jm_device.get_id(), force=True)
                time.sleep(3)

            restart_jm_device(snode.jm_device.get_id(), force=True)

    return True


def get_device(device_id):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        logger.error("device not found")
        return False
    out = [device.get_clean_dict()]
    return utils.print_table(out)


def get_device_capacity(device_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        logger.error("device not found")

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            return False
    else:
        records_number = 20

    records = db_controller.get_device_capacity(device, records_number)
    cap_stats_keys = [
        "date",
        "size_total",
        "size_used",
        "size_free",
        "size_util",
    ]
    records_list = utils.process_records(records, records_count, keys=cap_stats_keys)

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
    device = db_controller.get_storage_device_by_id(device_id)
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
    io_stats_keys = [
        "date",
        "read_bytes",
        "read_bytes_ps",
        "read_io_ps",
        "read_io",
        "read_latency_ps",
        "write_bytes",
        "write_bytes_ps",
        "write_io",
        "write_io_ps",
        "write_latency_ps",
    ]
    # combine records
    new_records = utils.process_records(records_list, records_count, keys=io_stats_keys)

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
    device = db_controller.get_storage_device_by_id(dev_id)
    if not device:
        logger.error(f"Device not found: {dev_id}")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error(f"Node not found {device.node_id}")
        return False

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
        logger.error(f"Unsupported device status: {device.status}")
        return False

    task_id = tasks_controller.get_active_dev_restart_task(snode.cluster_id, dev_id)
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not reset device")
        return False

    logger.info("Setting devices to unavailable")
    device_set_unavailable(dev_id)

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

    # set io_error flag False
    device_set_io_error(dev_id, False)
    device_set_retries_exhausted(dev_id, False)
    # set device to online
    device_set_online(dev_id)
    device_events.device_reset(device)
    return True


def device_set_retries_exhausted(device_id, retries_exhausted):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
    if not dev:
        logger.error("device not found")
        return False

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


def device_set_failed(device_id):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
    if not dev:
        logger.error("device not found")

    snode = db_controller.get_storage_node_by_id(dev.node_id)
    if not snode:
        logger.error("node not found")
        return False

    task_id = tasks_controller.get_active_dev_restart_task(snode.cluster_id, device_id)
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not fail device")
        return False

    if dev.status == NVMeDevice.STATUS_FAILED:
        return True

    ret = device_set_state(device_id, NVMeDevice.STATUS_FAILED)
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status == StorageNode.STATUS_ONLINE:
            rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
            rpc_client.distr_replace_id_in_map_prob(dev.cluster_device_order, -1)

    tasks_controller.add_device_failed_mig_task(device_id)


def add_device(device_id):
    db_controller = DBController()
    dev = db_controller.get_storage_device_by_id(device_id)
    if not dev:
        logger.error("device not found")

    if dev.status != NVMeDevice.STATUS_NEW:
        logger.error("Device must be in new state")
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

    logger.info(f"Adding device {device_id}")
    # if snode.num_partitions_per_dev == 0 or device_obj.is_partition:
    ret = _def_create_device_stack(device_obj, snode, force=True)
    if not ret:
        logger.error("Failed to create device stack")
        return False
    dev_order = storage_node_ops.get_next_cluster_device_order(db_controller, snode.cluster_id)
    device_obj.cluster_device_order = dev_order
    logger.info("Setting device online")
    device_obj.status = NVMeDevice.STATUS_ONLINE
    snode.write_to_db(db_controller.kv_store)
    device_events.device_create(device_obj)

    logger.info("Make other nodes connect to the node devices")
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    for node in snodes:
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        node.remote_devices = storage_node_ops._connect_to_remote_devs(node, force_conect_restarting_nodes=True)
        node.write_to_db()

    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    for node in snodes:
        if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            distr_controller.send_cluster_map_add_device(device_obj, node)

    tasks_controller.add_new_device_mig_task(device_id)
    return device_id

    #
    # # create partitions
    # partitions = snode.num_partitions_per_dev
    # rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    # # look for partitions
    # partitioned_devices = storage_node_ops._search_for_partitions(rpc_client, device_obj)
    # logger.debug("partitioned_devices")
    # logger.debug(partitioned_devices)
    # if len(partitioned_devices) == partitions+1:
    #     logger.info("Partitioned devices found")
    # else:
    #     logger.info(f"Creating partitions for {device_obj.nvme_bdev}")
    #     storage_node_ops._create_device_partitions(rpc_client, device_obj, snode, partitions, snode.jm_percent)
    #     partitioned_devices = storage_node_ops._search_for_partitions(rpc_client, device_obj)
    #     if len(partitioned_devices) == partitions+1:
    #         logger.info("Device partitions created")
    #     else:
    #         logger.error("Failed to create partitions")
    #         return False
    #
    # jm_part = partitioned_devices.pop(0)
    # new_devices = []
    # dev_order = storage_node_ops.get_next_cluster_device_order(db_controller, snode.cluster_id)
    # for dev in partitioned_devices:
    #     new_device = storage_node_ops._create_storage_device_stack(rpc_client, dev, snode, after_restart=False)
    #     if not new_device:
    #         logger.error("failed to create dev stack")
    #         continue
    #
    #     new_device.cluster_device_order = dev_order
    #     dev_order += 1
    #     device_events.device_create(new_device)
    #     new_devices.append(new_device)
    #
    # if new_devices:
    #     snode.nvme_devices.remove(device_obj)
    #     snode.nvme_devices.extend(new_devices)
    #     snode.write_to_db(db_controller.kv_store)
    # else:
    #     logger.error("failed to create devices")
    #     return False
    #
    # for dev in new_devices:
    #     distr_controller.send_cluster_map_add_device(dev, snode)
    #
    # logger.info("Make other nodes connect to the node devices")
    # snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    # for node in snodes:
    #     if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
    #         continue
    #     node.remote_devices = storage_node_ops._connect_to_remote_devs(node)
    #     node.write_to_db()
    #     for dev in new_devices:
    #         distr_controller.send_cluster_map_add_device(dev, node)
    #
    # for dev in new_devices:
    #     tasks_controller.add_new_device_mig_task(dev.get_id())
    #
    # # add to jm raid
    # if snode.jm_device and snode.jm_device.raid_bdev and jm_part:
    #     # looking for jm partition
    #     jm_dev_part = jm_part.nvme_bdev
    #     ret = rpc_client.get_bdevs(jm_dev_part)
    #     if ret:
    #         logger.info(f"JM part found: {jm_dev_part}")
    #         if snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_REMOVED]:
    #             restart_jm_device(snode.jm_device.get_id(), force=True, format_alceml=True)
    #
    #         if snode.jm_device.status == JMDevice.STATUS_ONLINE and \
    #                 jm_dev_part not in snode.jm_device.jm_nvme_bdev_list:
    #             remove_jm_device(snode.jm_device.get_id(), force=True)
    #             restart_jm_device(snode.jm_device.get_id(), force=True)
    #
    # return "Done"


def device_set_failed_and_migrated(device_id):
    db_controller = DBController()
    device_set_state(device_id, NVMeDevice.STATUS_FAILED_AND_MIGRATED)
    dev = db_controller.get_storage_device_by_id(device_id)
    for node in db_controller.get_storage_nodes_by_cluster_id(dev.cluster_id):
        if node.status == StorageNode.STATUS_ONLINE:
            rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
            rpc_client.distr_replace_id_in_map_prob(dev.cluster_device_order, -1)
    return True


def set_jm_device_state(device_id, state):
    db_controller = DBController()
    jm_device = None
    snode = None
    for node in db_controller.get_storage_nodes():
        if node.jm_device.get_id() == device_id:
            jm_device = node.jm_device
            snode = node
            break
    if not jm_device:
        logger.error("device not found")
        return False

    if jm_device.status != state:
        jm_device.status = state
        snode.write_to_db(db_controller.kv_store)

    if snode.enable_ha_jm and state == NVMeDevice.STATUS_ONLINE:
        # rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=5)
        # jm_bdev = f"jm_{snode.get_id()}"
        # subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        #
        # for iface in snode.data_nics:
        #     if iface.ip4_address:
        #         ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
        #             subsystem_nqn, iface.ip4_address, "4420", True)
        #         break

        # make other nodes connect to the new devices
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        for node_index, node in enumerate(snodes):
            if node.status != StorageNode.STATUS_ONLINE:
                continue
            logger.info(f"Connecting to node: {node.get_id()}")
            node.remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(node)
            node.write_to_db(db_controller.kv_store)
            logger.info(f"connected to devices count: {len(node.remote_jm_devices)}")

    return True


def remove_jm_device(device_id, force=False):
    db_controller = DBController()
    jm_device = None
    snode = None
    for node in db_controller.get_storage_nodes():
        if node.jm_device.get_id() == device_id:
            jm_device = node.jm_device
            snode = node
            break
    if not jm_device:
        logger.error("device not found")
        return False

    set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)

    if snode.status == StorageNode.STATUS_ONLINE:
        rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
        # delete jm stack
        if snode.enable_ha_jm:
            ret = rpc_client.subsystem_delete(snode.jm_device.nvmf_nqn)
            if not ret:
                logger.error("device not found")

        if snode.jm_device.pt_bdev:
            ret = rpc_client.bdev_PT_NoExcl_delete(snode.jm_device.pt_bdev)

        if snode.enable_ha_jm:
            ret = rpc_client.bdev_jm_delete(snode.jm_device.jm_bdev, safe_removal=True)
        else:
            ret = rpc_client.bdev_jm_delete(snode.jm_device.jm_bdev, safe_removal=False)

        ret = rpc_client.bdev_alceml_delete(snode.jm_device.alceml_bdev)

        # if snode.jm_device.testing_bdev:
        #     ret = rpc_client.bdev_passtest_delete(snode.jm_device.testing_bdev)

        # if len(snode.jm_device.jm_nvme_bdev_list) == 2:
        ret = rpc_client.bdev_raid_delete(snode.jm_device.raid_bdev)

    set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_REMOVED)
    return True


def restart_jm_device(device_id, force=False, format_alceml=False):
    db_controller = DBController()
    jm_device = None
    snode = None
    for node in db_controller.get_storage_nodes():
        if node.jm_device.get_id() == device_id:
            jm_device = node.jm_device
            snode = node
            break
    if not jm_device:
        logger.error("device not found")
        return False

    if jm_device.status == JMDevice.STATUS_ONLINE:
        logger.warning("device is online")
        if not force:
            return False

    # add to jm raid
    if snode.jm_device:
        rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
        if snode.jm_device.raid_bdev:
            bdevs_names = [d['name'] for d in rpc_client.get_bdevs()]
            jm_nvme_bdevs = []
            for dev in snode.nvme_devices:
                if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
                    continue
                dev_part = f"{dev.nvme_bdev[:-2]}p1"
                if dev_part in bdevs_names:
                    if dev_part not in jm_nvme_bdevs:
                        jm_nvme_bdevs.append(dev_part)

            if len(jm_nvme_bdevs) > 0:
                new_jm = storage_node_ops._create_jm_stack_on_raid(
                    rpc_client, jm_nvme_bdevs, snode, after_restart=not format_alceml)
                if not new_jm:
                    logger.error("failed to create jm stack")
                    return False

                else:
                    snode = db_controller.get_storage_node_by_id(snode.get_id())
                    snode.jm_device = new_jm
                    snode.write_to_db(db_controller.kv_store)
                    set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)
        else:
            nvme_bdev = jm_device.nvme_bdev
            # if snode.enable_test_device:
            #     ret = rpc_client.bdev_passtest_create(jm_device.testing_bdev, jm_device.nvme_bdev)
            #     if not ret:
            #         logger.error(f"Failed to create passtest bdev {jm_device.testing_bdev}")
            #         # return False
            #     nvme_bdev = jm_device.testing_bdev
            alceml_cpu_mask = ""
            alceml_worker_cpu_mask = ""
            #
            if snode.alceml_cpu_cores:
                alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
                snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)

            if snode.alceml_worker_cpu_cores:
                alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(
                    snode.alceml_worker_cpu_cores[snode.alceml_worker_cpu_index])
                snode.alceml_worker_cpu_index = (snode.alceml_worker_cpu_index + 1) % len(snode.alceml_worker_cpu_cores)

            cluster = db_controller.get_cluster_by_id(snode.cluster_id)
            ret = rpc_client.bdev_alceml_create(jm_device.alceml_bdev, nvme_bdev, jm_device.get_id(),
                                                pba_init_mode=1, alceml_cpu_mask=alceml_cpu_mask,
                                                alceml_worker_cpu_mask=alceml_worker_cpu_mask,
                                                pba_page_size=cluster.page_size_in_blocks)

            if not ret:
                logger.error(f"Failed to create alceml bdev: {jm_device.alceml_bdev}")
                return False

            jm_bdev = f"jm_{snode.get_id()}"
            ret = rpc_client.bdev_jm_create(jm_bdev, jm_device.alceml_bdev, jm_cpu_mask=snode.jm_cpu_mask)
            if not ret:
                logger.error(f"Failed to create {jm_bdev}")
                return False

            if snode.enable_ha_jm:
                # add pass through
                pt_name = f"{jm_bdev}_PT"
                ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
                if not ret:
                    logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
                    return False

                subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
                logger.info("creating subsystem %s", subsystem_nqn)
                ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
                IP = None
                for iface in snode.data_nics:
                    if iface.ip4_address:
                        tr_type = iface.get_transport_type()
                        logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                        ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
                        IP = iface.ip4_address
                        break
                logger.info(f"add {pt_name} to subsystem")
                ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
                if not ret:
                    logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
                    return False

                set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

    return True
