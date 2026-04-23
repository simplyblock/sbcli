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
        return SNodeClient(self.api_endpoint, **kwargs)

    def rpc_client(self, **kwargs):
        """Return rpc client to this node
        """
        return RPCClient(
            self.mgmt_ip, self.rpc_port,
            self.rpc_username, self.rpc_password, **kwargs)

    def expose_bdev(self, nqn, bdev_name, model_number, uuid, nguid, port, ana_state=None):
        """Expose `bdev_name` via NVMe-oF on `nqn` at `port`, one listener per data NIC.

        Idempotent: if the subsystem, a matching listener, or the namespace (by uuid)
        already exists, the corresponding RPC is skipped. This matters during
        activation/restart where the same subsystem can be re-examined by multiple
        paths (secondary hublvol shares the primary's NQN; multi-NIC nodes loop per
        NIC), and unconditional create_listener / add_ns would return -32602
        "Listener already exists" / "Invalid parameters" for state that is correct.
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

        self.expose_bdev(
            nqn=nqn,
            bdev_name=bdev_name,
            model_number=primary_node.hublvol.model_number,
            uuid=primary_node.hublvol.uuid,
            nguid=primary_node.hublvol.nguid,
            port=hublvol_port,
            ana_state="non_optimized",
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

    def connect_to_hublvol(self, primary_node, failover_node=None, role="secondary", timeout=None):
        """Connect to a primary node's hublvol, optionally with multipath failover.

        If failover_node is provided (typically sec_1), sets up NVMe ANA
        multipath so that IO automatically fails over from the primary path
        (optimized) to the failover path (non_optimized) when the primary
        becomes unreachable.

        Returns True iff all three required steps succeed:
          1. at least one NVMe controller attach established the remote bdev
          2. bdev_lvol_set_lvs_opts committed
          3. bdev_lvol_connect_hublvol committed
        Returns False otherwise. Individual per-NIC attach failures are
        tolerated as long as at least one primary path is present after the
        attach loop. Callers in the restart flow rely on this boolean to
        decide whether to unblock the secondary port.

        Args:
            timeout: if set, override the RPC timeout for attach_controller
                calls. Used during port-blocked windows to avoid long waits
                on unreachable NICs.
        """
        logger.info(f'Connecting node {self.get_id()} to hublvol on {primary_node.get_id()}'
                     + (f' with failover to {failover_node.get_id()}' if failover_node else ''))

        if primary_node.hublvol is None:
            raise ValueError(f"HubLVol of primary node {primary_node.get_id()} is not present")

        rpc_client = self.rpc_client()
        # bdev_nvme_attach_controller is hard-capped at 1s with no retries.
        # Callers may pass a lower `timeout` (e.g. 0.5s on restart); values
        # above the cap are clamped. See _ATTACH_CONTROLLER_MAX_TIMEOUT_SEC
        # in storage_node_ops.py for the rationale.
        attach_timeout = 1 if timeout is None else min(timeout, 1)
        attach_rpc = RPCClient(self.mgmt_ip, self.rpc_port,
                               self.rpc_username, self.rpc_password,
                               timeout=attach_timeout, retry=0)

        remote_bdev = f"{primary_node.hublvol.bdev_name}n1"

        if not rpc_client.get_bdevs(remote_bdev):
            # Multipathing is defined by the number of data NICs OR by role.
            # A tertiary's hublvol controller must be attached in multipath mode
            # regardless of current peer reachability, because the design expects
            # it to hold both primary and secondary (failover) paths over its
            # lifetime. SPDK does not allow widening a non-multipath controller
            # to multipath after attach (bdev_nvme.c:5849 returns -EINVAL), so
            # if we defer multipath until the failover peer is online we can
            # end up permanently stuck in single-path mode.
            multiple_nics = len([n for n in primary_node.data_nics
                                 if (primary_node.active_rdma and n.trtype == "RDMA")
                                 or (primary_node.active_tcp and n.trtype == "TCP")]) > 1
            use_multipath = "multipath" if (role == "tertiary" or failover_node or multiple_nics) else False

            # Attach primary path(s) — one per data NIC. Track whether at
            # least one primary attach succeeded: without that, the remote
            # bdev won't exist and the subsequent lvs_opts/connect_hublvol
            # would fail anyway. Multipath tolerates per-NIC failures; a
            # single working path is enough.
            primary_attached = False
            for iface in primary_node.data_nics:
                if primary_node.active_rdma and iface.trtype == "RDMA":
                    tr_type = "RDMA"
                elif not primary_node.active_rdma and primary_node.active_tcp and iface.trtype == "TCP":
                    tr_type = "TCP"
                else:
                    continue
                try:
                    ret = attach_rpc.bdev_nvme_attach_controller(
                        primary_node.hublvol.bdev_name, primary_node.hublvol.nqn,
                        iface.ip4_address, primary_node.hublvol.nvmf_port,
                        tr_type, multipath=use_multipath)
                    if ret:
                        primary_attached = True
                    else:
                        logger.warning(f'Failed to connect to hublvol on {iface.ip4_address}')
                except Exception as e:
                    logger.warning(f'Failed to connect to hublvol on {iface.ip4_address}: {e}')

            if not primary_attached:
                logger.error(
                    "No primary-path NVMe attach succeeded for hublvol of %s; "
                    "remote bdev %s will not be present",
                    primary_node.get_id(), remote_bdev,
                )
                return False

            # Attach failover path(s) — same controller name, same NQN, different IP.
            # Failover-path failures are best-effort; the overall connect still
            # succeeds as long as the primary path is present.
            if failover_node:
                for iface in failover_node.data_nics:
                    if failover_node.active_rdma and iface.trtype == "RDMA":
                        tr_type = "RDMA"
                    elif not failover_node.active_rdma and failover_node.active_tcp and iface.trtype == "TCP":
                        tr_type = "TCP"
                    else:
                        continue
                    try:
                        ret = attach_rpc.bdev_nvme_attach_controller(
                            primary_node.hublvol.bdev_name, primary_node.hublvol.nqn,
                            iface.ip4_address, primary_node.hublvol.nvmf_port,
                            tr_type, multipath="multipath")
                        if not ret:
                            logger.warning(f'Failed to connect failover hublvol path on {iface.ip4_address}')
                    except Exception as e:
                        logger.warning(f'Failed to connect failover hublvol path on {iface.ip4_address}: {e}')

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
        """Add failover_node's data-NIC paths to this node's existing hublvol
        controller for primary_node's LVStore, self-healing non-multipath
        controllers.

        SPDK (bdev_nvme.c:5849) rejects adding a multipath path to a controller
        attached with multipath='disable'/'failover'. That situation occurs when
        the initial tertiary attach ran while the failover peer was unreachable
        (single-NIC cluster + offline peer → use_multipath=False). When the
        fast-path attach fails we detach the whole controller and re-attach all
        expected paths (primary_node + failover_node) in multipath mode.

        Returns True if at least one path to failover_node ends up attached,
        with the primary path(s) still present (or re-attached).
        """
        controller_name = primary_node.hublvol.bdev_name
        nqn = primary_node.hublvol.nqn
        port = primary_node.hublvol.nvmf_port
        rpc = self.rpc_client()

        def _tr_type_for(peer, iface):
            if peer.active_rdma and iface.trtype == "RDMA":
                return "RDMA"
            if not peer.active_rdma and peer.active_tcp and iface.trtype == "TCP":
                return "TCP"
            return None

        def _attach_peer_paths(peer):
            attached = False
            for iface in peer.data_nics:
                tr_type = _tr_type_for(peer, iface)
                if tr_type is None:
                    continue
                try:
                    if rpc.bdev_nvme_attach_controller(
                            controller_name, nqn, iface.ip4_address, port,
                            tr_type, multipath="multipath"):
                        attached = True
                    else:
                        logger.warning(
                            "Failed to attach hublvol path %s (peer %s) to %s",
                            iface.ip4_address, peer.get_id(), controller_name)
                except Exception as e:
                    logger.warning(
                        "Failed to attach hublvol path %s (peer %s) to %s: %s",
                        iface.ip4_address, peer.get_id(), controller_name, e)
            return attached

        if _attach_peer_paths(failover_node):
            return True

        logger.warning(
            "Fast-path failover attach to %s failed on %s; detaching and "
            "rebuilding in multipath mode (existing controller may be in "
            "non-multipath mode)",
            controller_name, self.get_id())
        try:
            rpc.bdev_nvme_detach_controller(controller_name)
        except Exception as e:
            # Detach may fail if the controller doesn't exist yet; re-attach
            # below will reveal the real state via its own error handling.
            logger.warning(
                "Detach of %s on %s before multipath repair did not succeed: %s",
                controller_name, self.get_id(), e)

        primary_ok = _attach_peer_paths(primary_node)
        failover_ok = _attach_peer_paths(failover_node)
        if not primary_ok:
            logger.error(
                "Multipath repair of %s on %s lost primary path — no paths "
                "to %s were re-attached",
                controller_name, self.get_id(), primary_node.get_id())
        return failover_ok and primary_ok

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
