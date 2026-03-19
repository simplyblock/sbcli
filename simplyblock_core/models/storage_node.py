# coding=utf-8
import time
from typing import List
from uuid import uuid4

from simplyblock_core import utils
from simplyblock_core.models.base_model import BaseNodeObject, BaseModel
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteDevice, RemoteJMDevice
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
    jm_device: JMDevice = None # type: ignore[assignment]
    jm_percent: int = 3
    jm_vuid: int = 0
    lvols: int = 0
    lvstore: str = ""
    lvstore_stack: List[dict] = []
    lvstore_stack_secondary_1: List[dict] = []
    lvstore_stack_secondary_2: List[dict] = []
    lvol_subsys_port: int = 9090
    lvstore_ports: dict = {}  # {lvs_name: {"lvol_subsys_port": N, "hublvol_port": M, "sec_hublvol_port": P}}
    sec_hublvols: dict = {}   # {lvs_name: HubLVol-dict} — relay hublvols for first-secondary role
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
    number_of_alceml_devices: int = 0
    nvme_devices: List[NVMeDevice] = []
    online_since: str = ""
    partitions_count: int = 0  # Unused
    poller_cpu_cores: List[int] = []
    ssd_pcie: List = []
    pollers_mask: str = ""
    primary_ip: str = ""
    raid: str = ""
    remote_devices: List[RemoteDevice] = []
    remote_jm_devices: List[RemoteJMDevice] = []
    rpc_password: str = ""
    rpc_port: int = -1
    rpc_username: str = ""
    secondary_node_id: str = ""
    secondary_node_id_2: str = ""
    sequential_number: int = 0  # Unused
    jm_ids: List[str] = []
    spdk_cpu_mask: str = ""
    l_cores: str = ""
    spdk_debug: bool = False
    spdk_image: str = ""
    spdk_mem: int = 0
    minimum_sys_memory: int = 0
    partition_size: int = 0
    subsystem: str = ""
    system_uuid: str = ""
    lvstore_status: str = ""
    nvmf_port: int = 4420
    physical_label: int = 0
    hublvol: HubLVol = None  # type: ignore[assignment]
    active_tcp: bool = True
    active_rdma: bool = False
    socket: int = 0
    firewall_port: int = 5001

    def get_lvol_subsys_port(self, lvs_name=None):
        """Get the client-facing NVMeoF port for a specific lvstore.

        Falls back to node-level lvol_subsys_port for backward compat.
        """
        if lvs_name and lvs_name in self.lvstore_ports:
            return self.lvstore_ports[lvs_name].get("lvol_subsys_port", self.lvol_subsys_port)
        return self.lvol_subsys_port

    def get_hublvol_port(self, lvs_name=None):
        """Get the hublvol NVMeoF port for a specific lvstore.

        Falls back to node-level hublvol.nvmf_port for backward compat.
        """
        if lvs_name and lvs_name in self.lvstore_ports:
            return self.lvstore_ports[lvs_name].get("hublvol_port", 0)
        if self.hublvol:
            return self.hublvol.nvmf_port
        return 0

    def get_sec_hublvol_port(self, lvs_name):
        """Get the relay hublvol port for a lvstore where this node is first-secondary."""
        if lvs_name and lvs_name in self.lvstore_ports:
            return self.lvstore_ports[lvs_name].get("sec_hublvol_port", 0)
        return 0

    def get_sec_hublvol(self, lvs_name):
        """Get the HubLVol object for a secondary relay hublvol, or None."""
        data = self.sec_hublvols.get(lvs_name)
        if not data:
            return None
        if isinstance(data, HubLVol):
            return data
        # Reconstruct from dict (after DB deserialization)
        return HubLVol(data)

    def rpc_client(self, **kwargs):
        """Return rpc client to this node
        """
        return RPCClient(
            self.mgmt_ip, self.rpc_port,
            self.rpc_username, self.rpc_password, **kwargs)

    def expose_bdev(self, nqn, bdev_name, model_number, uuid, nguid, port):
        rpc_client = self.rpc_client()

        try:
            subsys =  rpc_client.subsystem_list(nqn)
            if not subsys:
                if not rpc_client.subsystem_create(
                        nqn=nqn,
                        serial_number='sbcli-cn',
                        model_number=model_number,
                ):
                    logger.error("fFailed to create subsystem for {nqn}")
                    raise RPCException(f'Failed to create subsystem for {nqn}')

            for iface in self.data_nics:
                ip = iface.ip4_address
                if self.active_rdma:
                    if iface.trtype != "RDMA":
                        logger.info("Skipping as the RDMA is enabled")
                        continue
                    rpc_client.listeners_create(
                        nqn=nqn,
                        trtype="RDMA",
                        traddr=ip,
                        trsvcid=port,
                    )
                else:
                    if iface.trtype != "TCP":
                        logger.info("Skipping as the TCP is only enabled")
                        continue
                    rpc_client.listeners_create(
                        nqn=nqn,
                        trtype="TCP",
                        traddr=ip,
                        trsvcid=port,
                    )

            rpc_client.nvmf_subsystem_add_ns(
                    nqn=nqn,
                    dev_name=bdev_name,
                    uuid=uuid,
                    nguid=nguid,
            )
                # logger.error(f'Failed to add namespace to subsytem {nqn}')
                # raise RPCException(f'Failed to add namespace to subsytem {nqn}')
        except RPCException as e:
            logger.exception(e)
            # if self.hublvol and rpc_client.subsystem_list(self.hublvol.nqn):
            #     rpc_client.subsystem_delete(self.hublvol.nqn)
            #
            # raise

    def create_hublvol(self):
        """Create a hublvol for this node's lvstore
        """
        logger.info(f'Creating hublvol on {self.get_id()}')
        rpc_client = self.rpc_client()

        hublvol_uuid = None
        try:
            hublvol_uuid = rpc_client.bdev_lvol_create_hublvol(self.lvstore)
            if not hublvol_uuid:
                raise RPCException('Failed to create hublvol')
            # Use pre-allocated hublvol port from lvstore_ports if available
            hublvol_port = self.get_hublvol_port(self.lvstore)
            if not hublvol_port:
                hublvol_port = utils.next_free_hublvol_port(self.cluster_id)
            self.hublvol = HubLVol({
                'uuid': hublvol_uuid,
                'nqn': f'{self.host_nqn}:lvol:{hublvol_uuid}',
                'bdev_name': f'{self.lvstore}/hublvol',
                'model_number': str(uuid4()),
                'nguid': utils.generate_hex_string(16),
                'nvmf_port': hublvol_port,
            })

            self.expose_bdev(
                    nqn=self.hublvol.nqn,
                    bdev_name=self.hublvol.bdev_name,
                    model_number=self.hublvol.model_number,
                    uuid=self.hublvol.uuid,
                    nguid=self.hublvol.nguid,
                    port=self.hublvol.nvmf_port,
            )
        except RPCException:
            if hublvol_uuid is not None and rpc_client.get_bdevs(hublvol_uuid):
                rpc_client.bdev_lvol_delete_hublvol(self.hublvol.nqn)

            if self.hublvol and rpc_client.subsystem_list(self.hublvol.nqn):
                rpc_client.subsystem_delete(self.hublvol.nqn)
                self.hublvol = None  # type: ignore[assignment]

            raise

        self.write_to_db()
        return self.hublvol

    def recreate_hublvol(self):
        """reCreate a hublvol for this node's lvstore
        """

        if self.hublvol and self.hublvol.uuid:
            logger.info(f'Recreating hublvol on {self.get_id()}')
            rpc_client = self.rpc_client()

            try:
                if not rpc_client.get_bdevs(self.hublvol.bdev_name):
                    ret = rpc_client.bdev_lvol_create_hublvol(self.lvstore)
                    if not ret:
                        logger.warning(f'Failed to recreate hublvol on {self.get_id()}')
                else:
                    logger.info(f'Hublvol already exists {self.hublvol.bdev_name}')

                self.expose_bdev(
                        nqn=self.hublvol.nqn,
                        bdev_name=self.hublvol.bdev_name,
                        model_number=self.hublvol.model_number,
                        uuid=self.hublvol.uuid,
                        nguid=self.hublvol.nguid,
                        port=self.hublvol.nvmf_port
                )
                return True
            except RPCException:
                pass
        else:
            try:
                self.create_hublvol()
                return True
            except RPCException as e:
                logger.error("Error establishing hublvol: %s", e.message)
                # return False

        return self.hublvol

    def connect_to_hublvol(self, primary_node):
        """Connect to a primary node's hublvol
        """
        logger.info(f'Connecting node {self.get_id()} to hublvol on {primary_node.get_id()}')

        if primary_node.hublvol is None:
            raise ValueError(f"HubLVol of primary node {primary_node.get_id()} is not present")

        rpc_client = self.rpc_client()

        remote_bdev = f"{primary_node.hublvol.bdev_name}n1"

        if not rpc_client.get_bdevs(remote_bdev):
            ip_lst = []
            for iface in primary_node.data_nics:
                if primary_node.active_rdma and iface.trtype=="RDMA":
                   ip_lst.append((iface.ip4_address,iface.trtype))
                else:
                   if not primary_node.active_rdma and primary_node.active_tcp and iface.trtype=="TCP":
                       ip_lst.append((iface.ip4_address, iface.trtype))
                   else:
                       # you may continue here for other nics
                       raise ValueError(f"{primary_node.get_id()} has no active fabric.")
            multipath = bool(len(ip_lst) > 1)
            for (ip,tr_type) in ip_lst:
                ret = rpc_client.bdev_nvme_attach_controller(
                        primary_node.hublvol.bdev_name, primary_node.hublvol.nqn,
                        ip, primary_node.hublvol.nvmf_port,tr_type,multipath=multipath)
                if not ret and not multipath:
                    logger.warning(f'Failed to connect to hublvol on {ip}')

        if not rpc_client.bdev_lvol_set_lvs_opts(
                primary_node.lvstore,
                groupid=primary_node.jm_vuid,
                subsystem_port=primary_node.get_lvol_subsys_port(primary_node.lvstore),
                secondary=True,
        ):
            pass
            # raise RPCException('Failed to set secondary lvstore options')

        if not rpc_client.bdev_lvol_connect_hublvol(primary_node.lvstore, remote_bdev):
            pass
            # raise RPCException('Failed to connect secondary lvstore to primary')

    def create_sec_hublvol(self, primary_node):
        """Create a relay hublvol on this (first-secondary) node for the primary's lvstore.

        This allows the second secondary to chain through this node instead of
        connecting directly to the primary, preventing IO conflicts on failover.
        """
        lvs_name = primary_node.lvstore
        logger.info(f'Creating sec hublvol on {self.get_id()} for lvstore {lvs_name}')
        rpc_client = self.rpc_client()

        hublvol_uuid = None
        try:
            hublvol_uuid = rpc_client.bdev_lvol_create_hublvol(lvs_name)
            if not hublvol_uuid:
                raise RPCException('Failed to create sec hublvol')
            sec_hub_port = self.get_sec_hublvol_port(lvs_name)
            if not sec_hub_port:
                sec_hub_port = utils.get_next_nvmf_port(self.cluster_id)
                # Persist the allocated port
                if lvs_name not in self.lvstore_ports:
                    self.lvstore_ports[lvs_name] = {}
                self.lvstore_ports[lvs_name]["sec_hublvol_port"] = sec_hub_port

            sec_hublvol = HubLVol({
                'uuid': hublvol_uuid,
                'nqn': f'{self.host_nqn}:sechub:{hublvol_uuid}',
                'bdev_name': f'{lvs_name}/hublvol',
                'model_number': str(uuid4()),
                'nguid': utils.generate_hex_string(16),
                'nvmf_port': sec_hub_port,
            })

            self.expose_bdev(
                nqn=sec_hublvol.nqn,
                bdev_name=sec_hublvol.bdev_name,
                model_number=sec_hublvol.model_number,
                uuid=sec_hublvol.uuid,
                nguid=sec_hublvol.nguid,
                port=sec_hublvol.nvmf_port,
            )
        except RPCException:
            if hublvol_uuid is not None and rpc_client.get_bdevs(hublvol_uuid):
                rpc_client.bdev_lvol_delete_hublvol(lvs_name)
            raise

        if not self.sec_hublvols:
            self.sec_hublvols = {}
        self.sec_hublvols[lvs_name] = sec_hublvol.to_dict()
        self.write_to_db()
        return sec_hublvol

    def recreate_sec_hublvol(self, primary_node):
        """Re-create the relay hublvol on this (first-secondary) node after restart."""
        lvs_name = primary_node.lvstore
        sec_hublvol = self.get_sec_hublvol(lvs_name)

        if sec_hublvol and sec_hublvol.uuid:
            logger.info(f'Recreating sec hublvol on {self.get_id()} for {lvs_name}')
            rpc_client = self.rpc_client()
            try:
                if not rpc_client.get_bdevs(sec_hublvol.bdev_name):
                    ret = rpc_client.bdev_lvol_create_hublvol(lvs_name)
                    if not ret:
                        logger.warning(f'Failed to recreate sec hublvol on {self.get_id()}')
                else:
                    logger.info(f'Sec hublvol already exists {sec_hublvol.bdev_name}')

                self.expose_bdev(
                    nqn=sec_hublvol.nqn,
                    bdev_name=sec_hublvol.bdev_name,
                    model_number=sec_hublvol.model_number,
                    uuid=sec_hublvol.uuid,
                    nguid=sec_hublvol.nguid,
                    port=sec_hublvol.nvmf_port,
                )
                return True
            except RPCException:
                pass
        else:
            try:
                self.create_sec_hublvol(primary_node)
                return True
            except RPCException as e:
                logger.error("Error creating sec hublvol: %s", e.message)

        return False

    def connect_to_sec_hublvol(self, first_sec_node, primary_node):
        """Connect this (second-secondary) node to the first secondary's relay hublvol.

        Instead of connecting directly to the primary, the second secondary
        chains through the first secondary to avoid IO conflicts on failover.
        """
        lvs_name = primary_node.lvstore
        sec_hublvol = first_sec_node.get_sec_hublvol(lvs_name)
        if sec_hublvol is None:
            raise ValueError(
                f"First secondary {first_sec_node.get_id()} has no sec hublvol for {lvs_name}")

        logger.info(f'Connecting node {self.get_id()} to sec hublvol on '
                     f'{first_sec_node.get_id()} for {lvs_name}')

        rpc_client = self.rpc_client()
        remote_bdev = f"{sec_hublvol.bdev_name}n1"

        if not rpc_client.get_bdevs(remote_bdev):
            ip_lst = []
            for iface in first_sec_node.data_nics:
                if first_sec_node.active_rdma and iface.trtype == "RDMA":
                    ip_lst.append((iface.ip4_address, iface.trtype))
                elif not first_sec_node.active_rdma and first_sec_node.active_tcp and iface.trtype == "TCP":
                    ip_lst.append((iface.ip4_address, iface.trtype))
                else:
                    raise ValueError(f"{first_sec_node.get_id()} has no active fabric.")
            multipath = bool(len(ip_lst) > 1)
            for (ip, tr_type) in ip_lst:
                ret = rpc_client.bdev_nvme_attach_controller(
                    sec_hublvol.bdev_name, sec_hublvol.nqn,
                    ip, sec_hublvol.nvmf_port, tr_type, multipath=multipath)
                if not ret and not multipath:
                    logger.warning(f'Failed to connect to sec hublvol on {ip}')

        if not rpc_client.bdev_lvol_set_lvs_opts(
                primary_node.lvstore,
                groupid=primary_node.jm_vuid,
                subsystem_port=primary_node.get_lvol_subsys_port(primary_node.lvstore),
                secondary=True,
        ):
            pass

        if not rpc_client.bdev_lvol_connect_hublvol(primary_node.lvstore, remote_bdev):
            pass

    def create_alceml(self, name, nvme_bdev, uuid, **kwargs):
        logger.info(f"Adding {name}")
        alceml_cpu_mask = ""
        alceml_worker_cpu_mask = ""
        if self.alceml_cpu_cores:
            alceml_cpu_mask = utils.decimal_to_hex_power_of_2(self.alceml_cpu_cores[self.alceml_cpu_index])
            self.alceml_cpu_index = (self.alceml_cpu_index + 1) % len(self.alceml_cpu_cores)
        if self.alceml_worker_cpu_cores:
            alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(
                self.alceml_worker_cpu_cores[self.alceml_worker_cpu_index])
            self.alceml_worker_cpu_index = (self.alceml_worker_cpu_index + 1) % len(self.alceml_worker_cpu_cores)

        return self.rpc_client().bdev_alceml_create(
            name, nvme_bdev, uuid,
            alceml_cpu_mask=alceml_cpu_mask,
            alceml_worker_cpu_mask=alceml_worker_cpu_mask,
            **kwargs,
        )

    def wait_for_jm_rep_tasks_to_finish(self, jm_vuid):
        if not self.rpc_client().bdev_lvol_get_lvstores(self.lvstore):
            return True # no lvstore means no need to wait
        retry = 10
        while retry > 0:
            try:
                jm_replication_tasks = False
                ret = self.rpc_client().jc_get_jm_status(jm_vuid)
                for jm in ret:
                    if ret[jm] is False:  # jm is not ready (has active replication task)
                        jm_replication_tasks = True
                        break
                if jm_replication_tasks:
                    logger.warning(f"Replication task found on node: {self.get_id()}, jm_vuid: {jm_vuid}, retry...")
                    retry -= 1
                    time.sleep(20)
                else:
                    return True
            except Exception:
                logger.warning("Failed to get replication task!")
        return False

    def lvol_sync_del(self) -> bool:
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()
        lock = db_controller.get_lvol_del_lock(self.get_id())
        if lock:
            return True
        return False

    def lvol_del_sync_lock(self) -> bool:
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()
        lock = db_controller.get_lvol_del_lock(self.get_id())
        if not lock:
            lock = NodeLVolDelLock({"uuid": self.uuid})
            lock.write_to_db()
            logger.info(f"Created lvol_del_sync_lock on node: {self.get_id()}")
        time.sleep(0.250)
        return True

    def lvol_del_sync_lock_reset(self) -> bool:
        from simplyblock_core.db_controller import DBController
        db_controller = DBController()
        task_found = False
        sec_ids = [self.secondary_node_id]
        if self.secondary_node_id_2:
            sec_ids.append(self.secondary_node_id_2)
        tasks = db_controller.get_job_tasks(self.cluster_id)
        for task in tasks:
            if task.function_name == JobSchedule.FN_LVOL_SYNC_DEL and task.node_id in sec_ids:
                if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                    task_found = True
                    break

        lock = db_controller.get_lvol_del_lock(self.get_id())
        if task_found:
            if not lock:
                lock = NodeLVolDelLock({"uuid": self.uuid})
                lock.write_to_db()
            logger.info(f"Created lvol_del_sync_lock on node: {self.get_id()}")
        else:
            if lock:
                lock.remove(db_controller.kv_store)
                logger.info(f"remove lvol_del_sync_lock from node: {self.get_id()}")
        time.sleep(0.250)
        return True


class NodeLVolDelLock(BaseModel):
    pass