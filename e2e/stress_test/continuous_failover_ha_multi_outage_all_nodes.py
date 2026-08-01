import itertools
import os
import random
import threading
import time
from datetime import datetime

from exceptions.custom_exception import LvolNotConnectException
from logger_config import setup_logger
from stress_test.continuous_failover_ha import generate_random_sequence
from stress_test.continuous_failover_ha_multi_outage import RandomMultiClientMultiFailoverTest
from utils.common_utils import sleep_n_sec


# Volume security types cycled in equal thirds
_SEC_TYPES = ["plain", "crypto", "dhchap"]

# Outage types that affect the entire physical host (not just the target node).
# In 2-nodes-per-host deployments, these make the host unreachable or reboot it,
# so sibling nodes on the same host cannot have independent outages triggered.
HOST_LEVEL_OUTAGES = {
    "storage_node_reboot",
    "interface_full_network_interrupt",
    "interface_partial_network_interrupt",
}

# Outage types that only affect the targeted storage node process/container.
# Sibling nodes on the same host remain fully operational and reachable.
NODE_LEVEL_OUTAGES = {
    "container_stop",
    "graceful_shutdown",
    "forced_shutdown",
}

# Host-level outages that make mgmt_ip completely unreachable (all NICs down
# or host rebooted).  Siblings on the same host must be skipped entirely.
_HOST_UNREACHABLE_OUTAGES = {"storage_node_reboot", "interface_full_network_interrupt"}


class RandomMultiClientMultiFailoverAllNodesTest(RandomMultiClientMultiFailoverTest):
    """
    Same as RandomMultiClientMultiFailoverTest but outage nodes are selected from ALL
    nodes (primary and secondary alike).  Requires max_fault_tolerance > 1.

    Enhancements over the parent:
      - Expanded outage types: container_stop, graceful_shutdown, forced_shutdown,
        storage_node_reboot, interface_full/partial_network_interrupt
      - Volume mix: plain, crypto, and DHCHAP (secure pool) lvols in equal thirds
      - 32 lvols (8 per node), numjobs=8, iodepth=1 => 64 parallel IO per node
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "n_plus_k_failover_multi_client_ha_all_nodes"

        # FIO configuration: 64 parallel IO streams per node
        self.total_lvols = 40           # 8 lvols per node x 5 nodes
        self.lvol_size = "20G"
        self.int_lvol_size = 20
        self.fio_num_jobs = 8           # 8 numjobs x 8 lvols/node = 64 per node
        self.fio_numjobs = 8            # must match — _compute_fio_size() uses this attr

        # Expanded outage types with varied timing to catch races
        self.outage_types = [
            "graceful_shutdown",
            "forced_shutdown",
            # "interface_full_network_interrupt",  # disabled for no-n/w-outage run
        ]
        self.outage_types2 = [
            "container_stop",
            "graceful_shutdown",
            "forced_shutdown",
            "storage_node_reboot",
            # "interface_full_network_interrupt",  # disabled for no-n/w-outage run
        ]
        self.multipath_outage_types = [
            "container_stop",
            "graceful_shutdown",
            "forced_shutdown",
            "storage_node_reboot",
        ]

        # DHCHAP pool
        self.dhchap_pool_name = None
        self._sec_cycle = itertools.cycle(_SEC_TYPES)
        self._cached_host_nqns: dict[str, str] = {}  # fio_node -> nqn

    # ------------------------------------------------------------------
    # Helper: forced shutdown
    # ------------------------------------------------------------------
    def _forced_shutdown_node(self, node_uuid):
        """Shutdown node with force=True (immediate, no drain)."""
        self.logger.info(f"Performing forced shutdown on node {node_uuid}")
        self.sbcli_utils.shutdown_node(node_uuid, force=True)

    # ------------------------------------------------------------------
    # Helper: get (cached) client host NQN for DHCHAP
    # ------------------------------------------------------------------
    def _get_client_host_nqn(self, fio_node):
        if fio_node in self._cached_host_nqns:
            return self._cached_host_nqns[fio_node]
        nqn = self.ssh_obj.get_client_host_nqn(fio_node)
        self.logger.info(f"Client host NQN for {fio_node}: {nqn}")
        self._cached_host_nqns[fio_node] = nqn
        return nqn

    # ------------------------------------------------------------------
    # Override: pick outage nodes from every node, not just primaries
    # ------------------------------------------------------------------
    def perform_n_plus_k_outages(self):
        """
        Select K outage nodes randomly from ALL storage nodes (primary and
        secondary).  No primary/secondary exclusion constraint is applied
        because max_fault_tolerance > 1 guarantees the cluster can survive it.

        When multipath is enabled (all nodes have 2+ data NICs), there is a 50%
        chance that one data NIC will be disabled cluster-wide before per-node
        outages are triggered. In that case only container_stop and
        graceful_shutdown are used (no additional network outages).

        Two-phase approach:
          Phase 1: collect node info + pre-dump all nodes (sequential)
          Phase 2: trigger all outages simultaneously (parallel threads)
        """
        # ── Multipath: optionally disable one data NIC on ALL nodes ──────
        # Disabled for no-n/w-outage run
        use_multipath_outage = False
        # if self._is_multipath_enabled() and random.random() < 0.5:
        #     self.logger.info("Multipath detected and selected — disabling one data NIC on all nodes")
        #     self.multipath_nic_disabled = True
        #     nic_plans = self._disconnect_single_data_nic_all_nodes()
        #     self.log_outage_event(
        #         "ALL_NODES", "multipath_single_nic_down",
        #         f"Disabled 1 data NIC on {len(nic_plans)} nodes (until recovery)"
        #     )
        #     self.logger.info("Waiting 30s for multipath failover to settle...")
        #     time.sleep(30)
        #     use_multipath_outage = True
        # else:
        self.multipath_nic_disabled = False
        self.log_outage_event("ALL_NODES", "multipath_nic_outage", "SKIPPED (disabled for no-n/w-outage run)")

        all_nodes = list(self.sn_nodes_with_sec)
        self.current_outage_nodes = []

        k = self.npcs
        if len(all_nodes) < k:
            raise Exception(
                f"Need {k} outage nodes, but only {len(all_nodes)} nodes exist."
            )

        outage_nodes = random.sample(all_nodes, k)
        self.logger.info(f"Selected outage nodes (all-nodes mode): {outage_nodes}")

        # Collect diagnostics for ALL nodes before any outage is triggered
        # Skip if multipath NIC is already down — API calls would be very slow
        if not use_multipath_outage:
            self.collect_outage_diagnostics(f"pre_outage_nodes_{'_'.join(outage_nodes[:3])}")

        # Choose outage type pools based on multipath state
        if use_multipath_outage:
            types_first = self.multipath_outage_types
            types_rest = self.multipath_outage_types
        else:
            types_first = self.outage_types2 if self.npcs == 1 else self.outage_types
            types_rest = self.outage_types2

        # ── Phase 1: pick types + collect node details for ALL nodes ─────────
        # First pass: collect node details and group by physical host (mgmt_ip)
        host_to_nodes = {}   # mgmt_ip → [node_uuid, ...]
        node_info = {}       # node_uuid → (mgmt_ip, rpc_port)
        for node in outage_nodes:
            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]
            node_info[node] = (node_ip, node_rpc_port)
            host_to_nodes.setdefault(node_ip, []).append(node)

        co_located = {ip: nodes for ip, nodes in host_to_nodes.items() if len(nodes) > 1}
        if co_located:
            self.logger.info(f"Co-located outage nodes by host: {co_located}")

        # Second pass: assign outage types with host-topology constraints.
        # When two nodes share a host (same mgmt_ip), host-level outages
        # (reboot, full/partial NIC down) make the host unreachable for the
        # sibling's outage commands (all use SSH/API to mgmt_ip).
        host_has_host_level_outage = {}  # mgmt_ip → outage_type
        skip_nodes = set()               # siblings that cannot be outaged

        node_plans = []  # (node, outage_type, node_ip, node_rpc_port)
        outage_num = 0
        for node in outage_nodes:
            node_ip, node_rpc_port = node_info[node]

            if node in skip_nodes:
                self.logger.info(
                    f"Skipping outage on {node} — host {node_ip} is unreachable "
                    f"(sibling has {host_has_host_level_outage.get(node_ip, '?')})"
                )
                continue

            siblings_on_host = host_to_nodes[node_ip]

            # Pick outage type pool
            if use_multipath_outage:
                type_pool = list(self.multipath_outage_types)
            elif outage_num == 0:
                type_pool = list(types_first)
            else:
                type_pool = list(types_rest)

            # If a sibling on this host already got a host-level outage,
            # decide whether to skip or restrict this node
            if len(siblings_on_host) > 1 and node_ip in host_has_host_level_outage:
                prev_outage = host_has_host_level_outage[node_ip]
                if prev_outage in _HOST_UNREACHABLE_OUTAGES:
                    # Host unreachable (rebooted or all NICs down)
                    skip_nodes.add(node)
                    self.logger.info(
                        f"Skipping outage on {node} — host {node_ip} is unreachable "
                        f"(sibling has {prev_outage})"
                    )
                    continue
                # Partial network outage — mgmt_ip still reachable, restrict to node-level
                type_pool = [t for t in type_pool if t in NODE_LEVEL_OUTAGES]
                if not type_pool:
                    type_pool = ["graceful_shutdown"]
                self.logger.info(
                    f"Node {node} shares host {node_ip} with partial network outage; "
                    f"restricting to node-level types: {type_pool}"
                )

            outage_type = random.choice(type_pool)
            outage_num += 1

            # Record if this is a host-level outage on a shared host
            if outage_type in HOST_LEVEL_OUTAGES and len(siblings_on_host) > 1:
                host_has_host_level_outage[node_ip] = outage_type
                if outage_type in _HOST_UNREACHABLE_OUTAGES:
                    # Mark all remaining siblings for skip
                    for sib in siblings_on_host:
                        if sib != node and sib not in {p[0] for p in node_plans}:
                            skip_nodes.add(sib)
                            self.logger.info(
                                f"Will skip sibling {sib} — host {node_ip} will be "
                                f"unreachable ({outage_type} on {node})"
                            )

            node_plans.append((node, outage_type, node_ip, node_rpc_port))

        if skip_nodes:
            self.logger.info(f"Skipped nodes (host unreachable): {skip_nodes}")

        # ── Phase 2: trigger all outages simultaneously via threads ────────────
        # When co-located nodes have a mix of host-level and node-level outages,
        # start node-level outages first so their API/SSH calls complete before
        # the host-level outage makes mgmt_ip unreachable.
        outage_results = {}  # node → (effective_type, outage_dur)

        def _trigger(node, outage_type, node_ip, node_rpc_port):
            self.logger.info(f"Performing {outage_type} on node {node}.")
            node_outage_dur = 0
            effective_type = outage_type
            if outage_type == "container_stop":
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            elif outage_type == "forced_shutdown":
                self._forced_shutdown_node(node)
            elif outage_type == "storage_node_reboot":
                self.ssh_obj.reboot_node(node_ip, wait_time=300)
            elif outage_type == "interface_partial_network_interrupt":
                self._disconnect_partial_interface(node, node_ip)
                node_outage_dur = 300
            elif outage_type == "interface_full_network_interrupt":
                node_outage_dur = self._disconnect_full_interface(node, node_ip)
                effective_type = f"interface_full_network_interrupt_{node_outage_dur}sec"
            self.log_outage_event(node, effective_type, "Outage started")
            outage_results[node] = (effective_type, node_outage_dur)

        # Separate co-located node-level outages (must fire first) from
        # host-level outages (fire after a small delay) and independent plans.
        node_level_plans = []
        host_level_plans = []
        independent_plans = []
        for plan in node_plans:
            node, otype, nip, nrpc = plan
            if len(host_to_nodes[nip]) > 1 and otype in HOST_LEVEL_OUTAGES:
                host_level_plans.append(plan)
            elif len(host_to_nodes[nip]) > 1 and otype in NODE_LEVEL_OUTAGES:
                node_level_plans.append(plan)
            else:
                independent_plans.append(plan)

        # Start node-level co-located outages first
        threads_early = [
            threading.Thread(target=_trigger, args=(n, o, ip, rpc))
            for n, o, ip, rpc in node_level_plans
        ]
        threads_rest = [
            threading.Thread(target=_trigger, args=(n, o, ip, rpc))
            for n, o, ip, rpc in host_level_plans + independent_plans
        ]

        for t in threads_early:
            t.start()
        if threads_early and threads_rest:
            # Let node-level API calls complete before host-level NICs go down
            time.sleep(3)
        for t in threads_rest:
            t.start()
        for t in threads_early + threads_rest:
            t.join()

        outage_combinations = []
        for node, _, _, _ in node_plans:
            effective_type, node_outage_dur = outage_results[node]
            outage_combinations.append((node, effective_type, node_outage_dur))
            self.current_outage_nodes.append(node)

        # Also track skipped nodes — they are affected by the host-level
        # outage and will come back when the host recovers.
        for node in skip_nodes:
            host_outage = host_has_host_level_outage.get(node_info[node][0], "host_level_outage")
            outage_combinations.append((node, f"skipped_{host_outage}", 0))
            self.current_outage_nodes.append(node)

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    # ------------------------------------------------------------------
    # Override create_lvols_with_fio: cycle plain / crypto / dhchap
    # ------------------------------------------------------------------
    def create_lvols_with_fio(self, count):
        """Create *count* lvols cycling through plain, crypto, dhchap types."""
        for i in range(count):
            sec_type = next(self._sec_cycle)
            self._create_one_lvol(i, sec_type)

    def _create_one_lvol(self, index, sec_type):
        """Create a single lvol of given security type and start FIO."""
        fs_type = random.choice(["ext4", "xfs"])
        is_dhchap = sec_type == "dhchap"
        is_crypto = sec_type in ("crypto", "dhchap")
        pool = self.dhchap_pool_name if is_dhchap else self.pool_name

        prefix = {"plain": "pl", "crypto": "cr", "dhchap": "dh"}[sec_type]
        lvol_name = f"{prefix}{self.lvol_name}_{index}"

        # Ensure unique name
        attempt = 0
        while lvol_name in self.lvol_mount_details:
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            attempt += 1
            if attempt > 20:
                break

        # Determine host placement (avoid outage nodes)
        host_id = None
        if self.current_outage_nodes:
            skip_nodes = [
                n for n in self.sn_primary_secondary_map
                if self.sn_primary_secondary_map[n] in self.current_outage_nodes
            ]
            for n in self.current_outage_nodes:
                skip_nodes.append(n)
            candidates = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
            host_id = candidates[0] if candidates else None

        client_node = random.choice(self.fio_node) if isinstance(self.fio_node, list) else self.fio_node

        self.logger.info(
            f"Creating {sec_type} lvol {lvol_name!r} "
            f"(crypto={is_crypto}, dhchap={is_dhchap}, pool={pool}, "
            f"client={client_node})")

        # Create lvol — use max_namespace_per_subsys=100 so clones stay
        # on the parent's subsystem instead of spilling to another lvol's.
        try:
            if is_dhchap:
                _, err = self.ssh_obj.create_sec_lvol(
                    self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
                    encrypt=True,
                    max_namespace_per_subsys=100,
                )
                if err and "error" in err.lower():
                    self.logger.warning(f"CLI lvol creation error for {lvol_name}: {err}")
                    return
            else:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=pool,
                    size=self.lvol_size,
                    crypto=is_crypto,
                    host_id=host_id,
                    max_namespace_per_subsys=100,
                )
        except Exception as exc:
            self.logger.warning(f"lvol creation failed for {lvol_name}: {exc}. Retrying...")
            self.lvol_name = f"lvl{generate_random_sequence(15)}"
            lvol_name = f"{prefix}{self.lvol_name}_{index}"
            try:
                if is_dhchap:
                    self.ssh_obj.create_sec_lvol(
                        self.mgmt_nodes[0], lvol_name, self.lvol_size, pool,
                        encrypt=True,
                        max_namespace_per_subsys=100,
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=pool,
                        size=self.lvol_size, crypto=is_crypto, host_id=host_id,
                        max_namespace_per_subsys=100,
                    )
            except Exception as exc2:
                self.logger.warning(f"Retry lvol creation also failed: {exc2}")
                return

        sleep_n_sec(3)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        if not lvol_id:
            self.logger.warning(f"Could not find lvol ID for {lvol_name}, skipping")
            return

        # Track node placement
        try:
            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=lvol_id)[0]["node_id"]
        except Exception:
            lvol_node_id = None
        if lvol_node_id:
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

        # Get connect string (DHCHAP needs host_nqn)
        host_nqn = self._get_client_host_nqn(client_node) if is_dhchap else None
        try:
            if host_nqn:
                connect_ls, err = self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                    self.mgmt_nodes[0], lvol_id, host_nqn)
                if err or not connect_ls:
                    self.logger.warning(f"No connect string for dhchap lvol {lvol_name}: {err}")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                    return
            else:
                connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        except Exception as exc:
            self.logger.warning(f"get_connect_str failed for {lvol_name}: {exc}")
            return

        if not self.k8s_test:
            self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0], command=f"{self.base_cmd} lvol list")

        log_file = f"{self.log_path}/{lvol_name}.log"
        iolog_base = f"{self.log_path}/{lvol_name}_fio_iolog"
        self.lvol_mount_details[lvol_name] = {
            "ID": lvol_id, "Command": connect_ls, "Mount": None,
            "Device": None, "MD5": None, "FS": fs_type, "Log": log_file,
            "snapshots": [], "sec_type": sec_type, "host_nqn": host_nqn,
            "Client": client_node, "iolog_base_path": iolog_base,
        }

        # Connect NVMe
        initial_devices = self.ssh_obj.get_devices(node=client_node)
        for cmd in connect_ls:
            _, err = self.ssh_obj.exec_command(node=client_node, command=cmd)
            if err:
                self.logger.warning(f"nvme connect error for {lvol_name}: {err}")
                try:
                    nqn = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client_node, nqn_grep=nqn)
                except Exception:
                    pass
                self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=True)
                del self.lvol_mount_details[lvol_name]
                if lvol_node_id and lvol_name in self.node_vs_lvol.get(lvol_node_id, []):
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                return

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=client_node)
        lvol_device = next(
            (f"/dev/{d.strip()}" for d in final_devices if d not in initial_devices),
            None,
        )
        if not lvol_device:
            raise LvolNotConnectException(
                f"LVOL {lvol_name} ({sec_type}) did not connect")

        self.lvol_mount_details[lvol_name]["Device"] = lvol_device
        self.ssh_obj.format_disk(node=client_node, device=lvol_device, fs_type=fs_type)
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=client_node, device=lvol_device, mount_path=mount_point)

        # Verify mount succeeded — if the block device disappeared
        # (e.g. during a failover), mount silently fails and FIO
        # would write to the root FS, causing ENOSPC.
        if not self.ssh_obj.is_mountpoint(client_node, mount_point):
            self.logger.warning(
                f"[create_lvol] Mount FAILED for {lvol_name}: "
                f"{lvol_device} -> {mount_point} is not a mount point. "
                f"Skipping FIO for this lvol."
            )
            self.lvol_mount_details[lvol_name]["Mount"] = None
            return

        self.lvol_mount_details[lvol_name]["Mount"] = mount_point

        sleep_n_sec(10)
        self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
        self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
        self.ssh_obj.delete_files(client_node, [f"{iolog_base}*"])
        sleep_n_sec(5)

        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client_node, None, mount_point, log_file),
            kwargs={
                "size": self.fio_size,
                "name": f"{lvol_name}_fio",
                "rw": "randrw",
                "bs": f"{2 ** random.randint(2, 7)}K",
                "nrfiles": 16,
                "iodepth": 1,
                "numjobs": self.fio_num_jobs,
                "time_based": True,
                "runtime": 2000,
                "log_avg_msec": 1000,
                "iolog_file": iolog_base,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)
        sleep_n_sec(10)

    # ------------------------------------------------------------------
    # Override run() to set up DHCHAP pool + fault tolerance check
    # ------------------------------------------------------------------
    def run(self):
        self.logger.info("Checking cluster fault tolerance before starting test.")
        cluster_details = self.sbcli_utils.get_cluster_details()
        max_fault_tolerance = cluster_details.get("max_fault_tolerance", 0)
        self.logger.info(f"Cluster max_fault_tolerance: {max_fault_tolerance}")

        if max_fault_tolerance <= 1:
            raise Exception(
                f"This test requires max_fault_tolerance > 1, "
                f"but cluster reports max_fault_tolerance={max_fault_tolerance}. "
                f"Aborting test."
            )

        self.logger.info(
            f"max_fault_tolerance={max_fault_tolerance} — proceeding "
            f"with all-nodes outage test."
        )

        # Create DHCHAP pool via CLI (API doesn't support --dhchap)
        self.dhchap_pool_name = f"dhchap_{self.pool_name}"
        self.logger.info(f"Creating DHCHAP pool: {self.dhchap_pool_name}")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], self.dhchap_pool_name,
            self.cluster_id, dhchap=True,
        )

        # Register client host NQN at pool level for DHCHAP volumes
        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        pool_id = self.sbcli_utils.get_storage_pool_id(self.dhchap_pool_name)
        if pool_id:
            for fio_node in fio_nodes:
                host_nqn = self._get_client_host_nqn(fio_node)
                if host_nqn:
                    self.logger.info(
                        f"Registering client NQN at pool level: "
                        f"pool={pool_id} nqn={host_nqn}")
                    self.ssh_obj.add_host_to_pool(
                        self.mgmt_nodes[0], pool_id, host_nqn)

        # Start full pcap capture on all nodes for network diagnostics
        all_node_ips = set(
            self.storage_nodes + self.mgmt_nodes + self.fio_node
        )
        self.logger.info(
            f"Starting full pcap capture on {len(all_node_ips)} nodes"
        )
        for node_ip in all_node_ips:
            try:
                node_log_dir = os.path.join(
                    self.docker_logs_path, node_ip,
                )
                self.ssh_obj.make_directory(
                    node=node_ip, dir_name=node_log_dir,
                )
                self.ssh_obj.start_full_pcap_capture(
                    node_ip, node_log_dir,
                )
            except Exception as exc:
                self.logger.warning(
                    f"Failed to start pcap on {node_ip}: {exc}"
                )

        try:
            # Call the grandparent's run() via super() — this creates the
            # standard pool, computes FIO size, and runs the main test loop.
            super(RandomMultiClientMultiFailoverAllNodesTest, self).run()
        finally:
            # Stop pcap capture on all nodes
            for node_ip in all_node_ips:
                try:
                    self.ssh_obj.stop_full_pcap_capture(node_ip)
                except Exception:
                    pass
