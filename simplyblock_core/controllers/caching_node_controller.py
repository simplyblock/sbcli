# coding=utf-8
import datetime
import json
import logging as lg
import math
import time
import uuid

import docker


from simplyblock_core import utils, scripts, constants
from simplyblock_core.cnode_client import CNodeClient
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.caching_node import CachingNode, CachedLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.pool import Pool
from simplyblock_core.rpc_client import RPCClient

logger = lg.getLogger()

db_controller = DBController()


def addNvmeDevices(rpc_client, devs, snode):
    devices = []
    ret = rpc_client.bdev_nvme_controller_list()
    ctr_map = {}
    try:
        if ret:
            ctr_map = {i["ctrlrs"][0]['trid']['traddr']: i["name"] for i in ret}
    except:
        pass

    for index, pcie in enumerate(devs):
        if pcie in ctr_map:
            nvme_controller = ctr_map[pcie]

        else:
            pci_st = str(pcie).replace("0", "").replace(":", "").replace(".", "")
            nvme_controller = "nvme_%s" % pci_st
            nvme_bdevs, err = rpc_client.bdev_nvme_controller_attach(nvme_controller, pcie)
            time.sleep(2)

        rpc_client.bdev_examine(f"{nvme_controller}n1")
        time.sleep(3)
        nvme_bdevs = []
        for bdev in rpc_client.get_bdevs():
            if bdev['name'].startswith(nvme_controller):
                nvme_bdevs.append(bdev['name'])

        for nvme_bdev in nvme_bdevs:
            time.sleep(3)
            ret = rpc_client.get_bdevs(nvme_bdev)
            nvme_dict = ret[0]
            total_size = nvme_dict['block_size'] * nvme_dict['num_blocks']
            model_number = ""
            pcie_address = ""
            serial_number = ""
            driver_specific = nvme_dict['driver_specific']
            if "nvme" in driver_specific:
                nvme_driver_data = driver_specific['nvme'][0]
                pcie_address = nvme_driver_data['pci_address']
                model_number = nvme_driver_data['ctrlr_data']['model_number']
                serial_number = nvme_driver_data['ctrlr_data']['serial_number']

            devices.append(
                NVMeDevice({
                    'uuid': str(uuid.uuid4()),
                    'device_name': nvme_dict['name'],
                    'size': total_size,
                    'driver_specific': driver_specific,
                    'pcie_address': pcie_address,
                    'model_id': model_number,
                    'serial_number': serial_number,
                    'nvme_bdev': nvme_bdev,
                    'nvme_controller': nvme_controller,
                    'node_id': snode.get_id(),
                    'cluster_id': snode.cluster_id,
                    'status': NVMeDevice.STATUS_ONLINE
                }))
    return devices


def add_node(cluster_id, node_ip, iface_name, data_nics_list, spdk_cpu_mask, spdk_mem, spdk_image=None,
             namespace=None, s3_data_path=None, ftl_buffer_size=None,
             lvstore_cluster_size=None, num_md_pages_per_cluster_ratio=None):


    db_controller = DBController()
    kv_store = db_controller.kv_store

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    logger.info(f"Add Caching node: {node_ip}")
    cnode_api = CNodeClient(node_ip)

    node_info, _ = cnode_api.info()
    system_id = node_info['system_id']
    hostname = node_info['hostname']
    logger.info(f"Node found: {node_info['hostname']}")
    snode = db_controller.get_caching_node_by_system_id(system_id)
    if snode:
        logger.error("Node already exists, try remove it first.")
        return False

    results, err = cnode_api.join_db(db_connection=cluster.db_connection)

    data_nics = []
    names = data_nics_list or [iface_name]
    for nic in names:
        device = node_info['network_interface'][nic]
        data_nics.append(
            IFace({
                'uuid': str(uuid.uuid4()),
                'if_name': device['name'],
                'ip4_address': device['ip'],
                'status': device['status'],
                'net_type': device['net_type']}))

    rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()
    BASE_NQN = cluster.nqn.split(":")[0]
    subsystem_nqn = f"{BASE_NQN}:{hostname}"
    # creating storage node object
    snode = CachingNode()
    snode.uuid = str(uuid.uuid4())
    snode.status = CachingNode.STATUS_IN_CREATION
    # snode.baseboard_sn = node_info['system_id']
    snode.system_uuid = system_id
    snode.hostname = hostname
    snode.subsystem = subsystem_nqn
    snode.data_nics = data_nics
    snode.mgmt_ip = node_info['network_interface'][iface_name]['ip']
    snode.rpc_port = constants.RPC_HTTP_PROXY_PORT
    snode.rpc_username = rpc_user
    snode.rpc_password = rpc_pass
    snode.cluster_id = cluster_id
    snode.api_endpoint = node_ip
    snode.host_secret = utils.generate_string(20)
    snode.ctrl_secret = utils.generate_string(20)

    snode.s3_data_path = s3_data_path or ""
    snode.lvstore_cluster_size = lvstore_cluster_size or 4096
    snode.num_md_pages_per_cluster_ratio = num_md_pages_per_cluster_ratio or 1
    if ftl_buffer_size:
        b_size = utils.parse_size(ftl_buffer_size)
        if b_size < utils.parse_size("6g"):
            logger.error("FTL cache must be larger than 6G")
            return False
        snode.ftl_buffer_size = b_size

    snode.cpu = node_info['cpu_count']
    snode.cpu_hz = node_info['cpu_hz']
    snode.memory = node_info['memory']

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"RAM Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"RAM Free: {utils.humanbytes(memory_details['free'])}")
        huge_total = memory_details['huge_total']
        logger.info(f"HP Total: {utils.humanbytes(huge_total)}")
        huge_free = memory_details['huge_free']
        logger.info(f"HP Free: {utils.humanbytes(huge_free)}")
        # if huge_free < 1 * 1024 * 1024:
        #     logger.warning(f"Free hugepages are less than 1G: {utils.humanbytes(huge_free)}")
        if not spdk_mem:
            if huge_total > 1024 * 1024 * 1024:
                spdk_mem = huge_total
            else:
                logger.error(f"Free hugepages are less than 1G: {utils.humanbytes(huge_total)}")
                return False

    logger.info(f"Deploying SPDK with HP: {utils.humanbytes(spdk_mem)}")
    results, err = cnode_api.spdk_process_start(
        spdk_cpu_mask, spdk_mem, spdk_image, snode.mgmt_ip,
        snode.rpc_port, snode.rpc_username, snode.rpc_password, namespace)
    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False

    retries = 20
    while retries > 0:
        resp, _ = cnode_api.spdk_process_is_up()
        if resp:
            logger.info(f"Pod is up")
            break
        else:
            logger.info("Pod is not running, waiting...")
            time.sleep(3)
            retries -= 1

    if retries == 0:
        logger.error("Pod is not running, exiting")
        return False

    time.sleep(1)

    snode.spdk_mem = spdk_mem or 0
    snode.spdk_image = spdk_image or ""
    snode.spdk_cpu_mask = spdk_cpu_mask or ""
    snode.namespace = namespace or ""

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=60*5, retry=5)

    ret = rpc_client.bdev_set_options(0,0,0,0,
                                      bdev_auto_examine=False)
    if not ret:
        logger.error("Failed to set options")
        return False

    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    # # get new node info after starting spdk
    node_info, _ = cnode_api.info()
    # mem = node_info['memory_details']['huge_free']
    # logger.info(f"Free Hugepages detected: {utils.humanbytes(mem)}")

    # adding devices
    nvme_devs = addNvmeDevices(rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    ssd_dev = nvme_devs[0]
    snode.nvme_devices = [ssd_dev]
    snode.write_to_db(db_controller.kv_store)

    if spdk_mem < 1024*1024:
        logger.error("Hugepages must be larger than 1G")
        return False

    mem = spdk_mem - 1024*1024*1024
    snode.hugepages = mem
    logger.info(f"Hugepages to be used: {utils.humanbytes(mem)}")

    ssd_size = ssd_dev.size
    supported_ssd_size = mem * 100 / 2.25

    logger.info(f"Supported SSD size: {utils.humanbytes(supported_ssd_size)}")
    logger.info(f"SSD size: {utils.humanbytes(ssd_size)}")

    supported_percent = int(supported_ssd_size*100/ssd_size)

    ftl_buffer_size = utils.parse_size("6g")
    if snode.ftl_buffer_size:
        ftl_buffer_size = snode.ftl_buffer_size

    ocf_cache_size = ssd_size - ftl_buffer_size
    ocf_cache_size = min(ocf_cache_size, supported_ssd_size)

    first_part_perc = (int(ftl_buffer_size * 100 / ssd_size))
    second_part_perc = (int(ocf_cache_size * 100 / (ssd_size-ftl_buffer_size)))

    partitions = [
        ["0%", f"{first_part_perc}%"],
        [f"{first_part_perc}%", f"{min(first_part_perc+second_part_perc, 100)}%", ],
    ]

    nbd_device = rpc_client.nbd_start_disk(ssd_dev.nvme_bdev)
    time.sleep(2)
    if not nbd_device:
        logger.error(f"Failed to start nbd dev")
        return False

    # jm_percent = min(supported_percent, (100-min_ftl_buffer_percent))
    result, error = cnode_api.make_gpt_partitions(nbd_device, partitions)
    if error:
        logger.error(f"Failed to make partitions")
        logger.error(error)
        return False
    time.sleep(3)
    rpc_client.nbd_stop_disk(nbd_device)
    time.sleep(1)
    rpc_client.bdev_nvme_detach_controller(ssd_dev.nvme_controller)
    time.sleep(1)
    rpc_client.bdev_nvme_controller_attach(ssd_dev.nvme_controller, ssd_dev.pcie_address)
    time.sleep(1)
    rpc_client.bdev_examine(ssd_dev.nvme_bdev)
    time.sleep(3)

    # cache_bdev = f"{ssd_dev.nvme_bdev}p1"
    # cache_size = int((jm_percent*ssd_size)/100)

    # adding devices
    nvme_devs = addNvmeDevices(rpc_client, [ssd_dev.pcie_address], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    snode.nvme_devices = nvme_devs
    snode.write_to_db(db_controller.kv_store)

    logger.info(f"Cache size: {utils.humanbytes(ocf_cache_size)}")

    snode.cache_bdev = nvme_devs[2].device_name
    snode.cache_size = ocf_cache_size

    filename = "/dev/nvme1n1"
    if snode.s3_data_path:
        filename = snode.s3_data_path

    ret = rpc_client.bdev_aio_create("aio_1", filename, 4096)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    ret = rpc_client.bdev_passthru_create("ftl_cache_1", nvme_devs[1].device_name, 4096)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    ret = rpc_client.bdev_ftl_create("ftl_1", "aio_1", "ftl_cache_1")
    if not ret:
        logger.error("Failed ot create bdev")
        return False
    snode.ftl_uuid = ret["uuid"]

    ret = rpc_client.bdev_ocf_create("ocf_1", 'wt', snode.cache_bdev, "ftl_1")
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    # create lvs
    md_pages_ratio = 1
    if snode.num_md_pages_per_cluster_ratio:
        md_pages_ratio = snode.num_md_pages_per_cluster_ratio
    cluster_sz = 4096
    if snode.lvstore_cluster_size:
        cluster_sz = snode.lvstore_cluster_size
    ret = rpc_client.create_lvstore(
        "lvs_1", "ocf_1", num_md_pages_per_cluster_ratio=int(md_pages_ratio), cluster_sz=cluster_sz)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    logger.info("Setting node status to Active")
    snode.status = CachingNode.STATUS_ONLINE
    snode.write_to_db(kv_store)

    logger.info("Done")
    return True


def restart_node(node_id, node_ip):

    db_controller = DBController()
    kv_store = db_controller.kv_store

    snode = db_controller.get_caching_node_by_id(node_id)
    if not snode:
        logger.error("Node not found")
        return False

    if not node_ip:
        node_ip = snode.mgmt_ip

    logger.info(f"Restarting Caching node: {node_ip}")
    cnode_api = CNodeClient(node_ip + ":5000")

    node_info, _ = cnode_api.info()
    logger.info(f"Node found: {node_info['hostname']}")

    snode.mgmt_ip = node_ip

    logger.info(f"Deploying SPDK")
    results, err = cnode_api.spdk_process_start(
        snode.spdk_cpu_mask, snode.spdk_mem, snode.spdk_image, snode.mgmt_ip,
        snode.rpc_port, snode.rpc_username, snode.rpc_password, snode.namespace)

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False

    retries = 20
    while retries > 0:
        resp, _ = cnode_api.spdk_process_is_up()
        if resp:
            logger.info(f"Pod is up")
            break
        else:
            logger.info("Pod is not running, waiting...")
            time.sleep(3)
            retries -= 1

    if retries == 0:
        logger.error("Pod is not running, exiting")
        return False

    time.sleep(1)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=60*5, retry=5)

    ret = rpc_client.bdev_set_options(0,0,0,0,
                                      bdev_auto_examine=False)
    if not ret:
        logger.error("Failed to set options")
        return False

    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    # # get new node info after starting spdk
    node_info, _ = cnode_api.info()
    # mem = node_info['memory_details']['huge_free']
    # logger.info(f"Free Hugepages detected: {utils.humanbytes(mem)}")

    nvme_devs = addNvmeDevices(rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    snode.nvme_devices = nvme_devs
    snode.write_to_db(db_controller.kv_store)


    filename = "/dev/nvme1n1"
    if snode.s3_data_path:
        filename = snode.s3_data_path

    ret = rpc_client.bdev_aio_create("aio_1", filename, 4096)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    ret = rpc_client.bdev_passthru_create("ftl_cache_1", nvme_devs[1].device_name, 4096)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    # ftl_uuid = snode.ftl_uuid
    ret = rpc_client.bdev_ftl_load("ftl_1", "aio_1", "ftl_cache_1", snode.ftl_uuid)
    if not ret:
        logger.error("Failed ot create bdev")
        return False

    ret = rpc_client.bdev_ocf_create("ocf_1", 'wt', snode.cache_bdev, "ftl_1")
    if not ret:
        logger.error("Failed ot create bdev")
        return False
    time.sleep(1)
    rpc_client.bdev_examine("ocf_1")

    # # create lvs
    # md_pages_ratio = 1
    # if snode.num_md_pages_per_cluster_ratio:
    #     md_pages_ratio = snode.num_md_pages_per_cluster_ratio
    # cluster_sz = 4096
    # if snode.lvstore_cluster_size:
    #     cluster_sz = snode.lvstore_cluster_size
    # ret = rpc_client.create_lvstore(
    #     "lvs_1", "ocf_1", num_md_pages_per_cluster_ratio=int(md_pages_ratio), cluster_sz=cluster_sz)
    # if not ret:
    #     logger.error("Failed ot create bdev")
    #     return False

    logger.info("Setting node status to Active")
    snode.status = CachingNode.STATUS_ONLINE
    snode.write_to_db(kv_store)

    logger.info("Done")
    return True





def recreate(node_id):
    db_controller = DBController()
    snode = db_controller.get_caching_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find caching node: {node_id}")
        return False

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", snode.cluster_id)
        return False

    logger.info(f"Recreating caching node: {node_id}, status: {snode.status}")
    snode_api = CNodeClient(f"{snode.mgmt_ip}:5000")

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=60*5, retry=5)

    # get new node info after starting spdk
    node_info, _ = snode_api.info()
    # adding devices
    nvme_devs = addNvmeDevices(rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    # snode.nvme_devices = nvme_devs
    # snode.write_to_db(db_controller.kv_store)

    # ssd_dev = nvme_devs[0]

    # if snode.cache_split_factor > 1:
    #     ret = rpc_client.bdev_split(ssd_dev.nvme_bdev, snode.cache_split_factor)

    logger.info(f"Cache size: {utils.humanbytes(snode.cache_size)}")

    # create tmp ocf
    logger.info(f"Creating first ocf bdev...")

    ret = rpc_client.bdev_malloc_create("malloc_tmp", 512, int((100 * 1024 * 1024) / 512))
    if not ret:
        logger.error("Failed ot create tmp malloc")
        return False
    ret = rpc_client.bdev_ocf_create("ocf_tmp", 'wt', snode.cache_bdev, "malloc_tmp")
    if not ret:
        logger.error("Failed ot create tmp OCF BDev")
        return False

    if snode.lvols:
        for lvol in snode.lvols:
            ret = connect(snode.get_id(), lvol.lvol_id)
            if ret:
                logger.info(f"connecting lvol {lvol.lvol_id} ... ok")
            else:
                logger.error(f"connecting lvol {lvol.lvol_id} .. error")

    logger.info("Setting node status to Active")
    snode.status = CachingNode.STATUS_ONLINE
    snode.write_to_db(db_controller.kv_store)

    logger.info("Done")
    return True


def connect(caching_node_id, lvol_id):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    if lvol.status != lvol.STATUS_ONLINE:
        logger.error(f"LVol must be online, lvol status: {lvol.status}")
        return False

    # pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    # if pool.status == Pool.STATUS_INACTIVE:
    #     logger.error(f"Pool is disabled")
    #     return False

    cnode = None
    if caching_node_id == 'this':
        hostn = utils.get_hostname()
        logger.info(f"Trying to get node by hostname: {hostn}")
        cnode = db_controller.get_caching_node_by_hostname(hostn)
    else:
        cnode = db_controller.get_caching_node_by_id(caching_node_id)
        if not cnode:
            logger.info(f"Caching node uuid not found: {caching_node_id}")
            cnode = db_controller.get_caching_node_by_hostname(caching_node_id)
            if not cnode:
                logger.error("Caching node not found")
                return False

    for clvol in cnode.connected_lvols:
        if clvol.lvol_id == lvol_id:
            logger.info(f"Already connected, dev path: {clvol.device_path}")
            return False

    # if cnode.cluster_id != pool.cluster_id:
    #     logger.error("Caching node and LVol are in different clusters")
    #     return False

    subsystem_nqn = lvol.nqn
    dev_path = None
    if lvol.connection_type == "nvmf":
        cnode_client = CNodeClient(cnode.api_endpoint)
        snode = db_controller.get_storage_node_by_id(lvol.node_id)
        for nic in snode.data_nics:
            ip = nic.ip4_address
            ret, _ = cnode_client.connect_nvme(ip, "4420", subsystem_nqn)
            break

        time.sleep(3)
        cnode_info, _ = cnode_client.info()
        nvme_devs = cnode_info['nvme_devices']
        dev_path = None
        for dev in nvme_devs:
            if dev['model_id'] == lvol.get_id():
                dev_path = dev['device_path']
                break

    elif lvol.connection_type == "nbd":
        rpc_client = RPCClient(cnode.mgmt_ip, cnode.rpc_port, cnode.rpc_username, cnode.rpc_password)
        dev_path = rpc_client.nbd_start_disk(lvol.top_bdev)

    if not dev_path:
        logger.error(f"Device path was not found")
        return False

    logger.info(f"Device path: {dev_path}")

    cached_lvol = CachedLVol()
    cached_lvol.uuid = lvol.get_id()
    cached_lvol.lvol_id = lvol.get_id()
    cached_lvol.hostname = cnode.hostname

    cached_lvol.local_nqn = subsystem_nqn
    cached_lvol.device_path = dev_path

    tmp = []
    for lv in cnode.connected_lvols:
        if lv.lvol_id != lvol.get_id():
            tmp.append(lv)

    tmp.append(cached_lvol)
    cnode.connected_lvols = tmp
    cnode.write_to_db(db_controller.kv_store)

    return dev_path


def disconnect(caching_node_id, lvol_id):
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"LVol not found: {lvol_id}")
        return False

    cnode = None
    if caching_node_id == 'this':
        hostn = utils.get_hostname()
        logger.info(f"Trying to get node by hostname: {hostn}")
        cnode = db_controller.get_caching_node_by_hostname(hostn)
    else:
        cnode = db_controller.get_caching_node_by_id(caching_node_id)
        if not cnode:
            logger.info(f"Caching node uuid not found: {caching_node_id}")
            cnode = db_controller.get_caching_node_by_hostname(caching_node_id)
            if not cnode:
                logger.error("Caching node not found")
                return False

    caching_lvol = None
    for clvol in cnode.connected_lvols:
        if clvol.lvol_id == lvol_id:
            caching_lvol = clvol
            break
    if not caching_lvol:
        logger.error("LVol is not connected.")
        return False

    if lvol.connection_type == "nvmf":
        try:
            cnode_client = CNodeClient(cnode.api_endpoint)
            ret, _ = cnode_client.disconnect_nqn(lvol.nqn)

        except Exception as e:
            logger.exception(e)

    elif lvol.connection_type == "nbd":
        rpc_client = RPCClient(cnode.mgmt_ip, cnode.rpc_port, cnode.rpc_username, cnode.rpc_password)
        rpc_client.nbd_stop_disk(caching_lvol.device_path)

    # remove lvol id from node lvol list
    n_list = []
    for clvol in cnode.connected_lvols:
        if clvol.lvol_id != lvol_id:
            n_list.append(clvol)
    cnode.connected_lvols = n_list
    cnode.write_to_db(db_controller.kv_store)

    return True


def deploy(ifname):
    if not ifname:
        ifname = "eth0"

    logger.info("Installing dependencies...")
    ret = scripts.install_deps()

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        logger.error(f"Error getting interface ip: {ifname}")
        return False

    logger.info(f"Node IP: {dev_ip}")
    ret = scripts.configure_docker(dev_ip)

    node_docker = docker.DockerClient(base_url=f"tcp://{dev_ip}:2375", version="auto", timeout=60 * 5)
    # create the api container
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] == "/CachingNodeAPI":
            logger.info("CachingNodeAPI container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    logger.info("Creating CachingNodeAPI container")
    cont_image = constants.SIMPLY_BLOCK_DOCKER_IMAGE
    container = node_docker.containers.run(
        cont_image,
        "python simplyblock_web/caching_node_app.py",
        detach=True,
        privileged=True,
        name="CachingNodeAPI",
        network_mode="host",
        volumes=[
            '/etc/foundationdb:/etc/foundationdb',
            '/var/tmp:/var/tmp',
            '/var/run:/var/run',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys'],
        restart_policy={"Name": "always"},
        environment=[
            f"DOCKER_IP={dev_ip}"
        ]
    )
    logger.info("Pulling SPDK images")
    spdk_core = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE
    if utils.get_host_arch() == "aarch64":
        spdk_core = constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE_ARM64
    node_docker.images.pull(spdk_core)
    return f"{dev_ip}:5000"


def list_nodes(is_json=False):
    db_controller = DBController()
    nodes = db_controller.get_caching_nodes()
    data = []
    output = ""

    for node in nodes:
        logger.debug(node)
        logger.debug("*" * 20)
        data.append({
            "UUID": node.uuid,
            "Hostname": node.hostname,
            "Management IP": node.mgmt_ip,
            "LVols": f"{len(node.lvols)}",
            "Conn LVols": f"{len(node.connected_lvols)}",
            "Cache": utils.humanbytes(node.cache_size),
            "HugeP": utils.humanbytes(node.hugepages),
            "Status": node.status,
            # "Updated At": datetime.datetime.strptime(node.updated_at, "%Y-%m-%d %H:%M:%S.%f").strftime(
            #     "%H:%M:%S, %d/%m/%Y"),
        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def list_lvols(caching_node_id):
    db_controller = DBController()
    cnode = None
    if caching_node_id == 'this':
        hostn = utils.get_hostname()
        logger.info(f"Trying to get node by hostname: {hostn}")
        cnode = db_controller.get_caching_node_by_hostname(hostn)
    else:
        cnode = db_controller.get_caching_node_by_id(caching_node_id)
        if not cnode:
            logger.info(f"Caching node uuid not found: {caching_node_id}")
            cnode = db_controller.get_caching_node_by_hostname(caching_node_id)
            if not cnode:
                logger.error("Caching node not found")
                return False

    data = []

    for clvol in cnode.connected_lvols:
        lvol = db_controller.get_lvol_by_id(clvol.lvol_id)
        logger.debug(clvol)
        logger.debug("*" * 20)
        data.append({
            "UUID": lvol.get_id(),
            "Hostname": lvol.hostname,
            "Size": utils.humanbytes(lvol.size),
            "Path": clvol.device_path,
            "Status": lvol.status,
        })

    if not data:
        return ""

    return utils.print_table(data)


def remove_node(node_id, force=False):
    db_controller = DBController()
    snode = db_controller.get_caching_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find caching node: {node_id}")
        return False

    if snode.connected_lvols:
        if force:
            for clvol in snode.connected_lvols:
                logger.info(f"Disconnecting LVol {clvol.lvol_id}")
                disconnect(node_id, clvol.lvol_id)
        else:
            logger.error("Connected LVols found on the node, use --force to disconnect all")
            return False

    if snode.lvols:
        if force:
            for lvol_id in snode.lvols:
                ret = lvol_controller.delete_lvol(lvol_id, force_delete=True)
                # logger.info(f"Disconnecting LVol {clvol.lvol_id}")
                # disconnect(node_id, clvol.lvol_id)
        else:
            logger.error("LVols found on the node, use --force to delete all")
            return False

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    rpc_client.bdev_lvol_delete_lvstore("lvs_1")
    logger.info("Removing node")
    try:
        snode_api = CNodeClient(snode.api_endpoint)
        results, err = snode_api.spdk_process_kill()
        ret = snode_api.delete_dev_gpt_partitions(snode.nvme_devices[0].pcie_address)
    except:
        pass

    snode.remove(db_controller.kv_store)
    logger.info("done")


def get_io_stats(lvol_uuid, history, records_count=20, parse_sizes=True):
    lvol = db_controller.get_lvol_by_id(lvol_uuid)
    if not lvol:
        logger.error(f"LVol not found: {lvol_uuid}")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records_list = db_controller.get_cached_lvol_stats(lvol.get_id(), limit=records_number)
    if not records_list:
        return False
    new_records = utils.process_records(records_list, min(records_count, len(records_list)))

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read bytes": utils.humanbytes(record["read_bytes"]),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record['read_io_ps'],
            "Read lat": record['read_latency_ps'],
            "Write bytes": utils.humanbytes(record["write_bytes"]),
            "Write speed": utils.humanbytes(record['write_bytes_ps']),
            "Write IOPS": record['write_io_ps'],
            "Write lat": record['write_latency_ps'],
        })
    return out

