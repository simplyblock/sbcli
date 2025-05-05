# coding=utf-8

from typing import List
from uuid import uuid4

from simplyblock_core import utils
from simplyblock_core.models.base_model import BaseNodeObject
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.rpc_client import RPCClient, RPCException

logger = utils.get_logger(__name__)


class StorageNode(BaseNodeObject):

    alceml_cpu_cores: List[int] = []
    alceml_cpu_index: int = 0
    alceml_worker_cpu_cores: List[int] = []
    alceml_worker_cpu_index: int = 0
    api_endpoint: str = ""
    app_thread_mask: str = ""
    baseboard_sn: str = ""
    cloud_instance_id: str = ""
    cloud_instance_public_ip: str = ""
    cloud_instance_type: str = ""
    cloud_name: str = ""
    cluster_id: str = ""
    cpu: int = 0
    cpu_hz: int = 0
    ctrl_secret: str = ""
    data_nics: List[IFace] = []
    distrib_cpu_cores: List[int] = []
    distrib_cpu_index: int = 0
    distrib_cpu_mask: str = ""
    enable_ha_jm: bool = False
    ha_jm_count: int = 3
    enable_test_device: bool = False
    health_check: bool = True
    host_nqn: str = ""
    host_secret: str = ""
    hostname: str = ""
    hugepages: int = 0
    ib_devices: List[IFace] = []
    id_device_by_nqn: bool = False
    iobuf_large_bufsize: int = 0
    iobuf_large_pool_count: int = 0
    iobuf_small_bufsize: int = 0
    iobuf_small_pool_count: int = 0
    is_secondary_node: bool = False
    jc_singleton_mask: str = ""
    jm_cpu_mask: str = ""
    jm_device: JMDevice = None
    jm_percent: int = 3
    jm_vuid: int = 0
    lvols: int = 0
    lvstore: str = ""
    lvstore_stack: List[dict] = []
    lvstore_stack_secondary_1: List[dict] = []
    lvstore_stack_secondary_2: List[dict] = []
    lvol_subsys_port: int = 9090
    max_lvol: int = 0
    max_prov: int = 0
    max_snap: int = 0
    memory: int = 0
    mgmt_ip: str = ""
    namespace: str = ""
    node_lvs: str = "lvs"
    num_partitions_per_dev: int = 1
    number_of_devices: int = 0
    number_of_distribs: int = 4
    nvme_devices: List[NVMeDevice] = []
    online_since: str = ""
    partitions_count: int = 0  # Unused
    poller_cpu_cores: List[int] = []
    ssd_pcie: List = []
    pollers_mask: str = ""
    primary_ip: str = ""
    raid: str = ""
    remote_devices: List[NVMeDevice] = []
    remote_jm_devices: List[JMDevice] = []
    rpc_password: str = ""
    rpc_port: int = -1
    rpc_username: str = ""
    secondary_node_id: str = ""
    sequential_number: int = 0  # Unused
    jm_ids: List[str] = []
    spdk_cpu_mask: str = ""
    spdk_debug: bool = False
    spdk_image: str = ""
    spdk_mem: int = 0
    partition_size: int = 0
    subsystem: str = ""
    system_uuid: str = ""
    lvstore_status: str = ""
    nvmf_port: int = 4420
    physical_label: int = 0
    hublvol: HubLVol = None

    def rpc_client(self, **kwargs):
        """Return rpc client to this node
        """
        return RPCClient(
            self.mgmt_ip, self.rpc_port,
            self.rpc_username, self.rpc_password, **kwargs)

    def expose_bdev(self, nqn, bdev_name, model_number, uuid, nguid, port):
        rpc_client = self.rpc_client()

        try:
            if not rpc_client.subsystem_create(
                    nqn=nqn,
                    serial_number='sbcli-cn',
                    model_number=model_number,
            ):
                logger.error("fFailed to create subsystem for {nqn}")
                # raise RPCException(f'Failed to create subsystem for {nqn}')

            for ip in (iface.ip4_address for iface in self.data_nics):
                if not rpc_client.listeners_create(
                        nqn=nqn,
                        trtype='TCP',
                        traddr=ip,
                        trsvcid=port,
                ):
                    logger.error(f'Failed to create listener for {nqn}')
                    # raise RPCException(f'Failed to create listener for {nqn}')

            if not rpc_client.nvmf_subsystem_add_ns(
                    nqn=nqn,
                    dev_name=bdev_name,
                    uuid=uuid,
                    nguid=nguid,
            ):
                logger.error(f'Failed to add namespace to subsytem {nqn}')
                # raise RPCException(f'Failed to add namespace to subsytem {nqn}')
        except RPCException as e:
            logger.exception(e)
            # if self.hublvol and rpc_client.subsystem_list(self.hublvol.nqn):
            #     rpc_client.subsystem_delete(self.hublvol.nqn)
            #
            # raise


    def create_hublvol(self, cluster_nqn):
        """Create a hublvol for this node's lvstore
        """
        logger.info(f'Creating hublvol on {self.get_id()}')
        rpc_client = self.rpc_client()

        hublvol_uuid = None
        try:
            hublvol_uuid = rpc_client.bdev_lvol_create_hublvol(self.lvstore)
            if not hublvol_uuid:
                raise RPCException('Failed to create hublvol')
            self.hublvol = HubLVol({
                'uuid': hublvol_uuid,
                'nqn': f'{cluster_nqn}:lvol:{hublvol_uuid}',
                'name': f'{self.lvstore}/hublvol',
            })

            self.expose_bdev(
                    nqn=self.hublvol.nqn,
                    bdev_name=self.hublvol.name,
                    model_number=str(uuid4()),
                    uuid=self.hublvol.uuid,
                    nguid=utils.generate_hex_string(16),
                    port=utils.next_free_port(9096),
            )
        except RPCException:
            if hublvol_uuid is not None and rpc_client.get_bdevs(hublvol_uuid):
                rpc_client.bdev_lvol_delete_hublvol(self.hublvol.nqn)

            if self.hublvol and rpc_client.subsystem_list(self.hublvol.nqn):
                rpc_client.subsystem_delete(self.hublvol.nqn)
                self.hublvol = None

            raise

        self.write_to_db()
        return self.hublvol

    def connect_to_hublvol(self, primary_node):
        """Connect to a primary node's hublvol
        """
        logger.info(f'Connecting node {self.get_id()} to hublvol on {primary_node.get_id()}')

        if primary_node.hublvol is None:
            raise ValueError(f"HubLVol of primary node {primary_node.get_id()} is not present")

        rpc_client = self.rpc_client()

        remote_bdev = None
        for ip in (iface.ip4_address for iface in primary_node.data_nics):
            remote_bdev = rpc_client.bdev_nvme_attach_controller_tcp(
                    primary_node.hublvol.name, primary_node.hublvol.nqn,
                    ip, primary_node.hublvol.nvmf_port)[0]
            if remote_bdev is not None:
                break
            else:
                logger.warning(f'Failed to connect to hublvol on {ip}')

        if remote_bdev is None:
            raise RPCException('Failed to connect to hublvol')

        if not rpc_client.bdev_lvol_set_lvs_opts(
                primary_node.lvstore,
                groupid=primary_node.jm_vuid,
                subsystem_port=self.lvol_subsys_port,
                secondary=True,
        ):
            raise RPCException('Failed to set secondary lvstore options')

        if not rpc_client.bdev_lvol_connect_hublvol(primary_node.lvstore, remote_bdev):
            raise RPCException('Failed to connect secondary lvstore to primary')
