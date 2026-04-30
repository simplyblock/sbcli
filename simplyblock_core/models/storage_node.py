# coding=utf-8
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from uuid import uuid4

from simplyblock_core import utils
from simplyblock_core.models.base_model import BaseNodeObject, BaseModel
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteDevice, RemoteJMDevice
from simplyblock_core.rpc_client import RPCClient, RPCException
from simplyblock_core.settings import Settings
from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)


class StorageNode(BaseNodeObject):

    # Restart phase constants (per-LVS)
    RESTART_PHASE_PRE_BLOCK = "pre_block"
    RESTART_PHASE_BLOCKED = "blocked"
    RESTART_PHASE_POST_UNBLOCK = "post_unblock"


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
    lvstore_stack_secondary: List[dict] = []
    lvstore_stack_tertiary: List[dict] = []
    lvol_subsys_port: int = 9090
    lvstore_ports: dict = {}  # {lvs_name: {"lvol_subsys_port": N, "hublvol_port": M}}
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
    tertiary_node_id: str = ""
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
    cr_name: str = ""
    cr_namespace: str = ""
    cr_plural: str = ""
    # Per-LVS restart phase tracking: {lvs_name: phase_string}
    # Phases: "pre_block", "blocked", "post_unblock", "" (not in restart)
    # Used by other services to gate sync deletes and create/clone/resize registrations.
    restart_phases: dict = {}
    nvmf_port: int = 4420
    physical_label: int = 0
    hublvol: HubLVol = None  # type: ignore[assignment]
    active_tcp: bool = True
    active_rdma: bool = False
    socket: int = 0
    firewall_port: int = 5001
    lvol_poller_mask: str = ""
    spdk_proxy_image: str = ""

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

    def client(self, **kwargs):
        """Return API client to this node
        """
        host = self.api_endpoint
        if Settings().tls_enabled:
            port = self.api_endpoint.rsplit(":", 1)[1]
            host = f"{self._k8s_node_label()}.simplyblock-storage-node-api.{self.cr_namespace}.svc.cluster.local:{port}"
        return SNodeClient(host, **kwargs)

    def rpc_client(self, **kwargs):
        """Return rpc client to this node
        """
        host = self.mgmt_ip
        if Settings().tls_enabled:
            host = f"{self._k8s_node_label()}.simplyblock-spdk-proxy.{self.cr_namespace}.svc.cluster.local"
        return RPCClient(
            host, self.rpc_port,
            self.rpc_username, self.rpc_password, **kwargs)

    def _k8s_node_label(self) -> str:
        return self.hostname.removesuffix(f"_{self.rpc_port}")

    def expose_bdev(self, nqn, bdev_name, model_number, uuid, nguid, port,
                    ana_state=None, min_cntlid=1):
        """Expose `bdev_name` via NVMe-oF on `nqn` at `port`, one listener per data NIC.

        Idempotent: if the subsystem, a matching listener, or the namespace (by uuid)
        already exists, the corresponding RPC is skipped. This matters during
        activation/restart where the same subsystem can be re-examined by multiple
        paths (secondary hublvol shares the primary's NQN; multi-NIC nodes loop per
        NIC), and unconditional create_listener / add_ns would return -32602
        "Listener already exists" / "Invalid parameters" for state that is correct.

        ``min_cntlid`` controls the lowest controller-id the subsystem will hand
        out on Connect. When two nodes expose the SAME shared hublvol NQN
        (primary + sec_1 with ANA multipath), they must allocate from
        non-overlapping cntlid ranges so a downstream multipath attach to
        both targets does not produce ``bdev_nvme_check_multipath: cntlid N
        are duplicated``. Mirror of the lvol_controller pattern at
        lvol_controller.py:841-848 (primary=1, sec=1000, tert=2000). The
        primary's hublvol stays at 1; sec_1's hublvol uses 1000.
        """
        rpc_client = self.rpc_client()

        try:
            subsys_list = rpc_client.subsystem_list(nqn)
            subsys = subsys_list[0] if subsys_list else None
            if subsys is None:
                if not rpc_client.subsystem_create(
                        nqn=nqn,
                        serial_number='sbcli-cn',
                        model_number=model_number,
                        min_cntlid=min_cntlid,
                ):
                    logger.error(f"Failed to create subsystem for {nqn}")
                    raise RPCException(f'Failed to create subsystem for {nqn}')
                existing_listeners: set = set()
                existing_ns_uuids: set = set()
            else:
                existing_listeners = {
                    (la.get("trtype", "").upper(),
                     la.get("traddr"),
                     str(la.get("trsvcid")))
                    for la in (subsys.get("listen_addresses") or [])
                }
                existing_ns_uuids = {
                    ns.get("uuid")
                    for ns in (subsys.get("namespaces") or [])
                    if ns.get("uuid")
                }

            for iface in self.data_nics:
                ip = iface.ip4_address
                if self.active_rdma:
                    if iface.trtype != "RDMA":
                        logger.debug("Skipping non-RDMA iface %s (active_rdma=True)", ip)
                        continue
                    trtype = "RDMA"
                else:
                    if iface.trtype != "TCP":
                        logger.debug("Skipping non-TCP iface %s (active_tcp=True)", ip)
                        continue
                    trtype = "TCP"

                if (trtype, ip, str(port)) in existing_listeners:
                    logger.info(
                        f"Listener on {nqn} at {trtype}/{ip}:{port} already present, skipping"
                    )
                    continue
                rpc_client.listeners_create(
                    nqn=nqn,
                    trtype=trtype,
                    traddr=ip,
                    trsvcid=port,
                    ana_state=ana_state,
                )

            if uuid in existing_ns_uuids:
                logger.info(
                    f"Namespace {uuid} already present on {nqn}, skipping add_ns"
                )
            else:
                rpc_client.nvmf_subsystem_add_ns(
                    nqn=nqn,
                    dev_name=bdev_name,
                    uuid=uuid,
                    nguid=nguid,
                )
        except RPCException as e:
            logger.exception(e)

    @staticmethod
    def hublvol_nqn_for_lvstore(cluster_nqn, lvstore_name):
        """Deterministic shared hublvol NQN for a given LVStore group.

        Primary and sec_1 both expose the same NQN so that downstream
        nodes can use NVMe multipath (ANA) to failover between them.
        """
        return f"{cluster_nqn}:hublvol:{lvstore_name}"

    def create_hublvol(self, cluster_nqn=None):
        """Create a hublvol for this node's lvstore.

        If cluster_nqn is provided, use a shared NQN scheme for multipath.
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

            if cluster_nqn:
                nqn = self.hublvol_nqn_for_lvstore(cluster_nqn, self.lvstore)
            else:
                nqn = f'{self.host_nqn}:lvol:{hublvol_uuid}'

            self.hublvol = HubLVol({
                'uuid': hublvol_uuid,
                'nqn': nqn,
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
                    ana_state="optimized",
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

    def create_secondary_hublvol(self, primary_node, cluster_nqn):
        """Create and expose a hublvol on this node for a LVStore where this node is sec_1.

        Uses the same shared NQN as the primary's hublvol so that downstream
        nodes (tertiary) can use NVMe multipath to failover from primary to sec_1.
        The listener ANA state is non_optimized.
        """
        lvstore_name = primary_node.lvstore
        logger.info(f'Creating secondary hublvol for {lvstore_name} on {self.get_id()}')
        rpc_client = self.rpc_client()

        bdev_name = f'{lvstore_name}/hublvol'
        # Check if hublvol already exists for this LVStore on this node
        if rpc_client.get_bdevs(bdev_name):
            logger.info(f'Secondary hublvol already exists: {bdev_name}')
        else:
            ret = rpc_client.bdev_lvol_create_hublvol(lvstore_name)
            if not ret:
                logger.error(f'Failed to create secondary hublvol for {lvstore_name}')
                return None

        nqn = self.hublvol_nqn_for_lvstore(cluster_nqn, lvstore_name)
        hublvol_port = primary_node.hublvol.nvmf_port

        # The secondary's hublvol subsystem MUST use a cntlid range
        # disjoint from the primary's, otherwise a tertiary (downstream
        # multipath consumer) attaching to both will get the same cntlid
        # from each independent target subsystem and SPDK will reject the
        # second path with ``bdev_nvme_check_multipath: cntlid N are
        # duplicated`` (LVS_5918 incident, 2026-04-25 12:47:18).
        self.expose_bdev(
            nqn=nqn,
            bdev_name=bdev_name,
            model_number=primary_node.hublvol.model_number,
            uuid=primary_node.hublvol.uuid,
            nguid=primary_node.hublvol.nguid,
            port=hublvol_port,
            ana_state="non_optimized",
            min_cntlid=1000,
        )
        logger.info(f'Secondary hublvol exposed: {nqn} on port {hublvol_port}')
        return nqn

    def adopt_hublvol(self, lvs_node, cluster_nqn):
        """Adopt a peer's hublvol during LVS takeover.

        ``self`` is the new leader (the restarting node); ``lvs_node`` is the
        offline peer whose LVS is being taken over. The hublvol bdev must be
        created for the TAKEN-OVER lvstore (``lvs_node.lvstore``) — not
        ``self.lvstore``, which is self's own primary — and exposed via the
        same NQN/port/UUID as the original so existing client paths keep
        working after failover.

        Idempotent: safe to call across restart retries. Both the create and
        the subsystem expose layer are probe-guarded (expose_bdev filters
        out existing listeners/namespaces, and create_hublvol returns
        EEXIST on an already-existing bdev).
        """
        lvstore_name = lvs_node.lvstore
        if not lvs_node.hublvol or not lvs_node.hublvol.uuid:
            raise RPCException(
                f"lvs_node {lvs_node.get_id()} has no hublvol metadata for {lvstore_name}"
            )

        bdev_name = f'{lvstore_name}/hublvol'
        logger.info('Adopting hublvol %s on %s', bdev_name, self.get_id())
        rpc_client = self.rpc_client()

        if not rpc_client.get_bdevs(bdev_name):
            if not rpc_client.bdev_lvol_create_hublvol(lvstore_name):
                raise RPCException(f'Failed to create adopted hublvol for {lvstore_name}')
        else:
            logger.info('Adopted hublvol already exists: %s', bdev_name)

        nqn = self.hublvol_nqn_for_lvstore(cluster_nqn, lvstore_name)
        self.expose_bdev(
            nqn=nqn,
            bdev_name=bdev_name,
            model_number=lvs_node.hublvol.model_number,
            uuid=lvs_node.hublvol.uuid,
            nguid=lvs_node.hublvol.nguid,
            port=lvs_node.hublvol.nvmf_port,
            ana_state="optimized",
        )
        return nqn

    def recreate_hublvol(self):
        """reCreate a hublvol for this node's lvstore.

        Returns True on success, False on failure. Callers in the restart
        flow (recreate_lvstore) gate the secondary port-unblock on this
        return value, so silent-success-on-failure would defeat the
        IO-isolation invariant.
        """

        if self.hublvol and self.hublvol.uuid:
            logger.info(f'Recreating hublvol on {self.get_id()}')
            rpc_client = self.rpc_client()

            try:
                if not rpc_client.get_bdevs(self.hublvol.bdev_name):
                    ret = rpc_client.bdev_lvol_create_hublvol(self.lvstore)
                    if not ret:
                        logger.error(f'Failed to recreate hublvol on {self.get_id()}')
                        return False
                else:
                    logger.info(f'Hublvol already exists {self.hublvol.bdev_name}')

                self.expose_bdev(
                        nqn=self.hublvol.nqn,
                        bdev_name=self.hublvol.bdev_name,
                        model_number=self.hublvol.model_number,
                        uuid=self.hublvol.uuid,
                        nguid=self.hublvol.nguid,
                        port=self.hublvol.nvmf_port,
                        ana_state="optimized",
                )
                return True
            except RPCException as e:
                logger.error("RPC error recreating hublvol on %s: %s",
                             self.get_id(), getattr(e, "message", str(e)))
                return False
        else:
            try:
                self.create_hublvol()
                return True
            except RPCException as e:
                logger.error("Error establishing hublvol: %s", e.message)
                return False

    def connect_to_hublvol(self, primary_node, failover_node=None, role="secondary",
                           timeout=None, rpc_timeout=None):
        """Connect to a primary node's hublvol, optionally with multipath failover.

        If failover_node is provided (typically sec_1), sets up NVMe ANA
        multipath so that IO automatically fails over from the primary path
        (optimized) to the failover path (non_optimized) when the primary
        becomes unreachable.

        Returns True iff all three required steps succeed:
          1. at least one NVMe controller attach established the remote bdev
          2. bdev_lvol_set_lvs_opts committed
          3. bdev_lvol_connect_hublvol committed
        Returns False otherwise.

        The NVMe-oF attach itself is delegated to
        :class:`HublvolReconnectCoordinator`, which serializes across
        control-plane services on an FDB advisory lock keyed on
        ``(self.id, primary.lvstore)`` and enforces a cooldown between
        attempts.

        ``rpc_timeout`` (seconds) bounds each underlying SPDK
        ``bdev_nvme_attach_controller`` HTTP call. The LVS rejoin uses
        a sub-second value so a single in-freeze attach must land fast
        or abort fast — the freeze must not hang on a stale listener.
        ``timeout`` is the legacy alias for the same intent and is honored
        when ``rpc_timeout`` is not provided.
        """
        if rpc_timeout is None and timeout is not None:
            rpc_timeout = timeout
        logger.info(f'Connecting node {self.get_id()} to hublvol on {primary_node.get_id()}'
                     + (f' with failover to {failover_node.get_id()}' if failover_node else ''))

        if primary_node.hublvol is None:
            raise ValueError(f"HubLVol of primary node {primary_node.get_id()} is not present")

        rpc_client = self.rpc_client()
        remote_bdev = f"{primary_node.hublvol.bdev_name}n1"

        if not rpc_client.get_bdevs(remote_bdev):
            # All hublvol NVMe-oF attach/detach now flows through a single
            # cross-process coordinator (FDB-locked, cooldown-gated,
            # detach-and-wait-gone). Previously two services could fire
            # bdev_nvme_attach_controller on the same subnqn within a few
            # ms and race SPDK's async destroy, producing
            # "bdev_nvme_check_multipath: cntlid N are duplicated" and
            # leaving the hublvol torn down — after which the tertiary's
            # failover poller could claim leadership and cause a writer
            # conflict. The coordinator is the one place that issues the
            # attach.
            from simplyblock_core.db_controller import DBController
            from simplyblock_core.utils.hublvol_reconnect import (
                HublvolReconnectCoordinator,
            )
            peers = [primary_node] + ([failover_node] if failover_node else [])
            coordinator = HublvolReconnectCoordinator(DBController())
            if not coordinator.reconcile(self, primary_node, peers, role=role,
                                         rpc_timeout=rpc_timeout):
                logger.error(
                    "Hublvol reconcile failed for %s on %s (role=%s)",
                    primary_node.hublvol.bdev_name, self.get_id(), role,
                )
                return False

        if not rpc_client.bdev_lvol_set_lvs_opts(
                primary_node.lvstore,
                groupid=primary_node.jm_vuid,
                subsystem_port=primary_node.get_lvol_subsys_port(primary_node.lvstore),
                role=role,
        ):
            logger.error("bdev_lvol_set_lvs_opts failed for %s on %s",
                         primary_node.lvstore, self.get_id())
            return False

        if not rpc_client.bdev_lvol_connect_hublvol(primary_node.lvstore, remote_bdev):
            logger.error("bdev_lvol_connect_hublvol failed for %s on %s",
                         primary_node.lvstore, self.get_id())
            return False

        return True

    def add_hublvol_failover_path(self, primary_node, failover_node):
        """Ensure this node's hublvol controller for primary_node's LVStore
        has both ``primary_node`` and ``failover_node`` paths attached.

        Delegated to :class:`HublvolReconnectCoordinator` so attach / detach
        is serialized (FDB lock) and cooldown-gated across all callers. The
        coordinator handles the non-multipath → multipath rebuild on its
        own: if the currently-attached controller is in anything other than
        ``enabled``, or is missing paths that can't be added to it (e.g. a
        non-multipath ctrlr rejecting a multipath extension), it detaches,
        waits for SPDK's async destroy to finish, and reattaches all
        expected peer paths. That is the safe version of the detach+retry
        idiom this method used to implement inline — the inline version
        could race SPDK's destroy and fail with
        ``bdev_nvme_check_multipath: cntlid N are duplicated``.

        Returns True if the resulting controller is enabled with at least
        one path to ``failover_node``.
        """
        from simplyblock_core.db_controller import DBController
        from simplyblock_core.utils.hublvol_reconnect import (
            HublvolReconnectCoordinator,
        )
        coordinator = HublvolReconnectCoordinator(DBController())
        return coordinator.reconcile(
            self, primary_node, [primary_node, failover_node],
            role="failover_repair",
        )

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
        if self.tertiary_node_id:
            sec_ids.append(self.tertiary_node_id)
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

    def uptime(self) -> Optional[timedelta]:
        return (
            datetime.now(timezone.utc) - datetime.fromisoformat(self.online_since)
            if self.online_since and self.status == StorageNode.STATUS_ONLINE
            else None
        )


class NodeLVolDelLock(BaseModel):
    pass
