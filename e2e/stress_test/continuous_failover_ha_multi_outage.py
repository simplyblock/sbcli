from utils.common_utils import sleep_n_sec
from datetime import datetime
from collections import defaultdict
from stress_test.continuous_failover_ha_multi_client import RandomMultiClientFailoverTest
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random
import os
import time


generated_sequences = set()

def generate_random_sequence(length):
    letters = string.ascii_uppercase
    numbers = string.digits
    all_chars = letters + numbers

    while True:
        first_char = random.choice(letters)
        remaining_chars = ''.join(random.choices(all_chars, k=length-1))
        result = first_char + remaining_chars
        if result not in generated_sequences:
            generated_sequences.add(result)
            return result


class RandomMultiClientMultiFailoverTest(RandomMultiClientFailoverTest):
    """
    Extended for N+K configuration: performs K parallel outages (K=self.npcs),
    skipping secondary outages. All existing logic for lvols, clones, fio continues as-is.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 40
        self.lvol_name = f"lvl{generate_random_sequence(15)}"
        self.clone_name = f"cln{generate_random_sequence(15)}"
        self.snapshot_name = f"snap{generate_random_sequence(15)}"
        self.lvol_size = "10G"
        self.int_lvol_size = 10
        self.fio_size = "1G"
        self.fio_num_jobs = 5
        self.fio_threads = []
        self.clone_mount_details = {}
        self.lvol_mount_details = {}
        self.sn_nodes = []
        self.current_outage_nodes = []
        self.snapshot_names = []
        self.disconnect_thread = None
        self.outage_start_time = None
        self.outage_end_time = None
        self.node_vs_lvol = {}
        self.persistent_lvols: set[str] = set()
        self.outage_dur = 0
        self.sn_nodes_with_sec = []
        self.lvol_node = ""
        self.secondary_outage = False
        self.test_name = "n_plus_k_failover_multi_client_ha"
        self.outage_types = [
            "graceful_shutdown",
            "interface_full_network_interrupt"
        ]
        self.outage_types2 = [
            "container_stop",
            "graceful_shutdown",
            "interface_full_network_interrupt"
        ]
        self._stop_spdk_mem_thread = False
        self.spdk_mem_thread = None
        self.blocked_ports = None
        self.dump_validation_errors = []
        self.multipath_outage_types = ["container_stop", "graceful_shutdown"]
        self.multipath_nic_disabled = False
        self.multipath_disconnected_nics = []  # [(mgmt_ip, iface, nic_ip), ...]
        self.outage_log_file = os.path.join("logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self._initialize_outage_log()

    def _initialize_outage_log(self):
        with open(self.outage_log_file, 'w') as log:
            log.write("Timestamp,Node,Outage_Type,Event\n")

    def log_outage_event(self, node, outage_type, event, outage_time=0):
        """Log an outage event to the outage log file.

        Args:
            node (str): Node UUID or IP where the event occurred.
            outage_type (str): Type of outage (e.g., port_network_interrupt, container_stop, graceful_shutdown).
            event (str): Event description (e.g., 'Outage started', 'Node restarted').
            outage_time (int): Minutes to add to self.outage_start_time. If 0/None, use current time.
        """
        # Compute timestamp
        if outage_time:
            # Uses self.outage_start_time (epoch seconds) + outage_time (minutes)
            base_epoch = getattr(self, "outage_start_time", None)
            if isinstance(base_epoch, (int, float)) and base_epoch > 0:
                ts_dt = datetime.fromtimestamp(int(base_epoch) + int(outage_time) * 60)
            else:
                # Fallback to now if outage_start_time is missing/invalid
                ts_dt = datetime.now()
        else:
            ts_dt = datetime.now()

        timestamp = ts_dt.strftime('%Y-%m-%d %H:%M:%S')

        # Write the log line
        with open(self.outage_log_file, 'a') as log:
            log.write(f"{timestamp},{node},{outage_type},{event}\n")

    def _build_reverse_secondary_map(self):
        rev = defaultdict(set)        # secondary -> {primary,...}
        for p, s in self.sn_primary_secondary_map.items():
            if s:
                rev[s].add(p)
        return rev

    def _pick_outage_nodes(self, primary_candidates, k):
        rev = self._build_reverse_secondary_map()
        order = primary_candidates[:]

        random.shuffle(order)

        chosen, blocked = [], set()
        for node in order:
            if node in blocked:
                continue

            chosen.append(node)
            blocked.add(node)                            # itself
            sec = self.sn_primary_secondary_map.get(node)
            if sec:
                blocked.add(sec)                         # its secondary
            blocked.update(rev.get(node, ()))           # any primary whose secondary == node

            if len(chosen) == k:
                break

        if len(chosen) < k:
            raise Exception(
                f"Cannot pick {k} nodes without primary/secondary conflicts; only {len(chosen)} possible with current topology."
            )
        return chosen

    def _is_multipath_enabled(self):
        """Return True if ALL storage nodes have 2+ data NICs (multipath capable)."""
        for node in self.sn_nodes_with_sec:
            details = self.sbcli_utils.get_storage_node_details(node)
            data_nics = details[0].get("data_nics", [])
            if len(data_nics) < 2:
                return False
        return True

    def _disconnect_single_data_nic_all_nodes(self):
        """
        Disable one data NIC on ALL storage nodes in parallel (no auto-restore).

        Uses data_nics[0]["ip4_address"] from the API (reliable), then resolves
        the actual Linux interface name via SSH (ip -o addr show) because the
        API's if_name field may not match the real interface name.

        The NICs stay down until _reconnect_multipath_nics() is called
        (after node outage recovery).

        Returns list of (mgmt_ip, resolved_iface, data_nic_ip) for logging.
        """
        node_plans = []
        for node in self.sn_nodes_with_sec:
            details = self.sbcli_utils.get_storage_node_details(node)
            mgmt_ip = details[0]["mgmt_ip"]
            data_nics = details[0].get("data_nics", [])
            if not data_nics:
                self.logger.warning(f"Node {node} has no data_nics, skipping multipath NIC disable")
                continue
            nic_ip = data_nics[0]["ip4_address"]
            iface = self.ssh_obj.get_interface_by_ip(mgmt_ip, nic_ip)
            if not iface:
                self.logger.error(
                    f"Cannot resolve interface for {nic_ip} on {mgmt_ip}, skipping this node"
                )
                continue
            node_plans.append((mgmt_ip, iface, nic_ip))

        if not node_plans:
            self.logger.error("No nodes could resolve data NIC interface; aborting multipath outage")
            return []

        self.logger.info(
            f"Multipath NIC outage: disabling one data NIC on {len(node_plans)} nodes (until recovery)"
        )

        def _bring_down(mgmt_ip, iface):
            cmd = f"nmcli connection down {iface}"
            try:
                self.ssh_obj.exec_command(node=mgmt_ip, command=cmd, max_retries=1, timeout=20)
            except Exception as e:
                self.logger.warning(f"NIC down command on {mgmt_ip} ({iface}): {e}")

        threads = []
        for mgmt_ip, iface, _ in node_plans:
            t = threading.Thread(target=_bring_down, args=(mgmt_ip, iface))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.multipath_disconnected_nics = node_plans
        for mgmt_ip, iface, nic_ip in node_plans:
            self.logger.info(f"  Disabled {iface} (IP={nic_ip}) on {mgmt_ip}")
            self.log_outage_event(mgmt_ip, "multipath_single_nic_down", f"Disabled {iface} (IP={nic_ip})")

        return node_plans

    def _reconnect_multipath_nics(self):
        """Bring back up all data NICs that were disabled by _disconnect_single_data_nic_all_nodes."""
        if not self.multipath_disconnected_nics:
            return

        self.logger.info(
            f"Reconnecting multipath NICs on {len(self.multipath_disconnected_nics)} nodes"
        )

        def _bring_up(mgmt_ip, iface):
            cmd = f"nmcli connection up {iface}"
            try:
                self.ssh_obj.exec_command(node=mgmt_ip, command=cmd, max_retries=3, timeout=20)
                self.logger.info(f"  Reconnected {iface} on {mgmt_ip}")
            except Exception as e:
                self.logger.error(f"Failed to reconnect {iface} on {mgmt_ip}: {e}")

        threads = []
        for mgmt_ip, iface, nic_ip in self.multipath_disconnected_nics:
            t = threading.Thread(target=_bring_up, args=(mgmt_ip, iface))
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for mgmt_ip, iface, nic_ip in self.multipath_disconnected_nics:
            self.log_outage_event(mgmt_ip, "multipath_single_nic_up", f"Reconnected {iface} (IP={nic_ip})")

        self.multipath_disconnected_nics = []
        self.multipath_nic_disabled = False

    def perform_n_plus_k_outages(self):
        """
        Select K outage nodes such that no two are in a primary/secondary
        relationship (in either direction). Candidates = keys of the map.

        When multipath is enabled (all nodes have 2+ data NICs), there is a 50%
        chance that one data NIC will be disabled cluster-wide before per-node
        outages are triggered. In that case only container_stop and
        graceful_shutdown are used (no additional network outages).
        """
        # ── Multipath: optionally disable one data NIC on ALL nodes ──────
        use_multipath_outage = False
        if self._is_multipath_enabled() and random.random() < 0.5:
            self.logger.info("Multipath detected and selected — disabling one data NIC on all nodes")
            self.multipath_nic_disabled = True
            nic_plans = self._disconnect_single_data_nic_all_nodes()
            self.log_outage_event(
                "ALL_NODES", "multipath_single_nic_down",
                f"Disabled 1 data NIC on {len(nic_plans)} nodes (until recovery)"
            )
            self.logger.info("Waiting 30s for multipath failover to settle...")
            time.sleep(30)
            use_multipath_outage = True
        else:
            self.multipath_nic_disabled = False
            self.log_outage_event("ALL_NODES", "multipath_nic_outage", "SKIPPED (not enabled or not selected)")

        # Candidates are nodes that are primary *for someone* (map keys)
        primary_candidates = list(self.sn_primary_secondary_map.keys())
        self.current_outage_nodes = []

        if len(primary_candidates) < self.npcs:
            raise Exception(
                f"Need {self.npcs} outage nodes, but only {len(primary_candidates)} primary-role nodes exist."
            )

        outage_nodes = self._pick_outage_nodes(primary_candidates, self.npcs)
        self.logger.info(f"Selected outage nodes: {outage_nodes}")

        # Collect diagnostics for ALL nodes before any outage is triggered
        self.collect_outage_diagnostics(f"pre_outage_nodes_{'_'.join(outage_nodes[:3])}")

        # Choose outage type pools based on multipath state
        if use_multipath_outage:
            types_first = self.multipath_outage_types
            types_rest = self.multipath_outage_types
        else:
            types_first = self.outage_types2 if self.npcs == 1 else self.outage_types
            types_rest = self.outage_types2

        outage_combinations = []
        outage_num = 0
        for node in outage_nodes:
            if outage_num == 0:
                outage_type = random.choice(types_first)
                outage_num = 1
            else:
                outage_type = random.choice(types_rest)
            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]

            self.logger.info(f"Performing {outage_type} on primary node {node}.")

            node_outage_dur = 0
            effective_type = outage_type
            if outage_type == "container_stop":
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            elif outage_type == "interface_partial_network_interrupt":
                self._disconnect_partial_interface(node, node_ip)
                node_outage_dur = 300
            elif outage_type == "interface_full_network_interrupt":
                node_outage_dur = self._disconnect_full_interface(node, node_ip)
                effective_type = f"interface_full_network_interrupt_{node_outage_dur}sec"

            self.log_outage_event(node, effective_type, "Outage started")
            outage_combinations.append((node, effective_type, node_outage_dur))
            self.current_outage_nodes.append(node)

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    def _graceful_shutdown_node(self, node):
        """Shutdown node without --force; retry every 20 s for up to 5 minutes.

        The shutdown command now triggers an internal suspend before shutting
        down.  After each call we wait 20 seconds and check the node status.
        If the node is still not offline we re-issue the command and try again
        until the 5-minute deadline, then raise so the test captures the failure
        rather than masking it with a force-kill.
        """
        self.logger.info(f"Issuing graceful shutdown (no --force) for node {node}.")
        deadline = time.time() + 300  # 5 minutes
        while True:
            try:
                self.sbcli_utils.shutdown_node(node_uuid=node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown_node raised (may already be shutting down): {e}")
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(node)
            if node_detail[0]["status"] == "offline":
                self.logger.info(f"Node {node} is offline.")
                return
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {node} did not go offline within 5 minutes of graceful shutdown."
                )
            self.logger.info(f"Node {node} not yet offline; retrying shutdown...")

    def _disconnect_partial_interface(self, node, node_ip):
        data_nics = self.sbcli_utils.get_storage_node_details(node)[0]["data_nics"]
        nic_ip = data_nics[0]["ip4_address"]
        iface = self.ssh_obj.get_interface_by_ip(node_ip, nic_ip)
        if not iface:
            self.logger.error(
                f"Cannot resolve interface for {nic_ip} on {node_ip}, "
                f"falling back to get_active_interfaces"
            )
            active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)[:1]
        else:
            active_interfaces = [iface]
        self.logger.info(f"Partial NIC disconnect on {node_ip}: {active_interfaces}")
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, 300)
        )
        self.disconnect_thread.start()

    def _disconnect_full_interface(self, node, node_ip):
        self.logger.info("Handling full interface based network interruption...")
        active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
        outage_dur = random.choice([300, 30])
        self.logger.info(f"Selected Outage seconds for n/w outage: {outage_dur}")
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, outage_dur)
        )
        self.disconnect_thread.start()
        return outage_dur
    # [COMMENTED OUT — kept for future use] Two-phase version with outage_position param:
    # def _disconnect_full_interface(self, node, node_ip, outage_position=0):
    #     self.logger.info("Handling full interface based network interruption...")
    #     active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
    #     # Force 300s for the first outage in a multi-outage scenario so the
    #     # network is still down when subsequent outages are triggered.
    #     if outage_position == 0 and self.npcs > 1:
    #         outage_dur = 300
    #     else:
    #         outage_dur = random.choice([300, 30])
    #     effective_name = f"interface_full_network_interrupt_{outage_dur}sec"
    #     self.logger.info(f"[N/W Outage] duration={outage_dur}s → {effective_name}")
    #     self.disconnect_thread = threading.Thread(
    #         target=self.ssh_obj.disconnect_all_active_interfaces,
    #         args=(node_ip, active_interfaces, outage_dur)
    #     )
    #     self.disconnect_thread.start()
    #     return outage_dur, effective_name

    def delete_random_lvols(self, count):
        """Delete random lvols during an outage, skipping lvols on any outage node."""
        # available_lvols = [
        #     lvol for node, lvols in self.node_vs_lvol.items()
        #     if node not in self.current_outage_nodes for lvol in lvols
        # ]
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items()
            for lvol in lvols
            if lvol not in self.persistent_lvols
        ]

        self.logger.info(f"Available Lvols: {available_lvols}")
        if len(available_lvols) < count:
            self.logger.warning("Not enough lvols available to delete the requested count.")
            count = len(available_lvols)

        for lvol in random.sample(available_lvols, count):
            self.logger.info(f"Deleting lvol {lvol}.")
            snapshots = self.lvol_mount_details[lvol]["snapshots"]
            to_delete = []
            for clone_name, clone_details in self.clone_mount_details.items():
                if clone_details["snapshot"] in snapshots:
                    if self.k8s_test and clone_details.get("pending_connect"):
                        # Clone was never connected/mounted — skip FIO/unmount/disconnect
                        self.logger.info(f"[pending_connect] Deleting deferred clone '{clone_name}' (no FIO/mount to clean up).")
                        deleted = self.sbcli_utils.delete_lvol(clone_name, max_attempt=20, skip_error=True)
                        if not deleted:
                            self.record_pending_lvol_delete(clone_name, clone_details['ID'])
                        if clone_name in self.lvols_without_sec_connect:
                            self.lvols_without_sec_connect.remove(clone_name)
                        to_delete.append(clone_name)
                    else:
                        self.common_utils.validate_fio_test(clone_details["Client"],
                                                            log_file=clone_details["Log"])
                        self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                        fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                        for pid in fio_pids:
                            self.ssh_obj.kill_processes(clone_details["Client"], pid=pid)
                        attempt = 1
                        while True:
                            self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                            fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                            # ≤2 means only the two grep/awk processes matched — FIO is gone
                            if len(fio_pids) <= 2:
                                break
                            # Kill any newly found PIDs (e.g. restarted FIO from restart_fio)
                            for pid in fio_pids:
                                self.ssh_obj.kill_processes(clone_details["Client"], pid=pid)
                            if attempt >= 30:  # 5 minutes (30 × 10 s)
                                self.logger.warning(
                                    f"FIO still running on clone '{clone_name}' after 5 min; "
                                    f"disconnecting lvol to force exit (remaining pids: {fio_pids})."
                                )
                                self.disconnect_lvol(clone_details['ID'])
                                break
                            attempt += 1
                            sleep_n_sec(10)

                        sleep_n_sec(10)
                        self.disconnect_lvol(clone_details['ID'])
                        self.ssh_obj.unmount_path(clone_details["Client"], f"/mnt/{clone_name}")
                        self.ssh_obj.remove_dir(clone_details["Client"], dir_path=f"/mnt/{clone_name}")
                        deleted = self.sbcli_utils.delete_lvol(clone_name, max_attempt=20, skip_error=True)
                        if not deleted:
                            self.record_pending_lvol_delete(clone_name, clone_details['ID'])
                        sleep_n_sec(30)
                        if clone_name in self.lvols_without_sec_connect:
                            self.lvols_without_sec_connect.remove(clone_name)
                        to_delete.append(clone_name)
                        self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone_name}_fio*"])
                        self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone_name}_fio_iolog*"])
                        self.ssh_obj.delete_files(clone_details["Client"], [f"/mnt/{clone_name}/*"])
                    # self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone_name}*.log"])
            for del_key in to_delete:
                del self.clone_mount_details[del_key]
            for snapshot in snapshots:
                if self.k8s_test:
                    snapshot_id = self.sbcli_utils.get_snapshot_id(snapshot)
                    self.sbcli_utils.delete_snapshot(snap_id=snapshot_id, skip_error=True)
                else:
                    snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot)
                    # snapshot_node = self.snap_vs_node[snapshot]
                    # if snapshot_node not in skip_nodes:
                    self.ssh_obj.delete_snapshot(self.mgmt_nodes[0], snapshot_id=snapshot_id, skip_error=True)
                self.record_pending_snapshot_delete(snapshot, snapshot_id)
                self.snapshot_names.remove(snapshot)

            self.common_utils.validate_fio_test(self.lvol_mount_details[lvol]["Client"],
                                                log_file=self.lvol_mount_details[lvol]["Log"])
            self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=False)
            fio_pids = self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=True)
            for pid in fio_pids:
                self.ssh_obj.kill_processes(self.lvol_mount_details[lvol]["Client"], pid=pid)
            attempt = 1
            while True:
                self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=False)
                fio_pids = self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=True)
                if len(fio_pids) <= 2:
                    break
                for pid in fio_pids:
                    self.ssh_obj.kill_processes(self.lvol_mount_details[lvol]["Client"], pid=pid)
                if attempt >= 30:  # 5 minutes (30 × 10 s)
                    self.logger.warning(
                        f"FIO still running on lvol '{lvol}' after 5 min; "
                        f"disconnecting lvol to force exit (remaining pids: {fio_pids})."
                    )
                    self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
                    break
                attempt += 1
                sleep_n_sec(10)

            sleep_n_sec(10)
            self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
            self.ssh_obj.unmount_path(self.lvol_mount_details[lvol]["Client"], f"/mnt/{lvol}")
            self.ssh_obj.remove_dir(self.lvol_mount_details[lvol]["Client"], dir_path=f"/mnt/{lvol}")
            deleted = self.sbcli_utils.delete_lvol(lvol, max_attempt=20, skip_error=True)
            if not deleted:
                self.record_pending_lvol_delete(lvol, self.lvol_mount_details[lvol]['ID'])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"/mnt/{lvol}/*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/{lvol}*.log"])
            if lvol in self.lvols_without_sec_connect:
                self.lvols_without_sec_connect.remove(lvol)
            del self.lvol_mount_details[lvol]
            for _, lvols in self.node_vs_lvol.items():
                if lvol in lvols:
                    lvols.remove(lvol)
                    break
        sleep_n_sec(60)

    def create_snapshots_and_clones(self):
        """Create snapshots and clones during an outage, avoiding lvols on outage nodes."""
        skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] in self.current_outage_nodes]
        self.logger.info(f"Skip Nodes: {skip_nodes}")
        for node in self.current_outage_nodes:
            skip_nodes.append(node)
        skip_nodes = []
        self.logger.info(f"Skip Nodes: {skip_nodes}")
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items()
            if node not in skip_nodes for lvol in lvols
        ]
        if not available_lvols:
            self.logger.warning("No available lvols to create snapshots and clones.")
            return
        self.logger.info(f"Available lvols: {available_lvols}")
        for _ in range(3):
            self.int_lvol_size += 1
            random.shuffle(available_lvols)
            lvol = available_lvols[0]
            snapshot_name = f"snap_{generate_random_sequence(15)}"
            temp_name = generate_random_sequence(5)
            if snapshot_name in self.snapshot_names:
                snapshot_name = f"{snapshot_name}_{temp_name}"
            try:
                if self.k8s_test:
                    self.sbcli_utils.add_snapshot(self.lvol_mount_details[lvol]["ID"], snapshot_name)
                    output, error = "", ""
                else:
                    output, error = self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.lvol_mount_details[lvol]["ID"], snapshot_name)
                    if "(False," in output:
                        raise Exception(output)
                    if "(False," in error:
                        raise Exception(error)
            except Exception as e:
                self.logger.warning(f"Snap creation fails with {str(e)}. Retrying with different name.")
                try:
                    snapshot_name = f"snap_{lvol}"
                    temp_name = generate_random_sequence(5)
                    snapshot_name = f"{snapshot_name}_{temp_name}"
                    if self.k8s_test:
                        self.sbcli_utils.add_snapshot(self.lvol_mount_details[lvol]["ID"], snapshot_name)
                    else:
                        self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.lvol_mount_details[lvol]["ID"], snapshot_name)
                except Exception as exp:
                    self.logger.warning(f"Retry Snap creation fails with {str(exp)}.")
                    continue
                
            self.snapshot_names.append(snapshot_name)
            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol]["ID"])[0]["node_id"]
            self.snap_vs_node[snapshot_name] = lvol_node_id
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_name)
            sleep_n_sec(30)
            if self.k8s_test:
                snapshot_id = self.sbcli_utils.get_snapshot_id(snapshot_name)
            else:
                snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)

            clone_name = f"clone_{generate_random_sequence(15)}"
            if clone_name in list(self.clone_mount_details):
                clone_name = f"clone_{generate_random_sequence(15)}"

            clone_created = False
            for clone_attempt in range(5):
                if clone_attempt > 0:
                    clone_name = f"clone_{generate_random_sequence(15)}"
                    self.logger.info(
                        f"[create_clones] Retry {clone_attempt}/4 for snapshot "
                        f"{snapshot_name!r}: new name={clone_name!r}"
                    )

                try:
                    if self.k8s_test:
                        self.sbcli_utils.add_clone(snapshot_id, clone_name)
                    else:
                        self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)
                except Exception as exc:
                    self.logger.warning(
                        f"[create_clones] add_clone raised for {clone_name!r} "
                        f"(attempt {clone_attempt + 1}/5): {exc}. Waiting 10s before retry."
                    )
                    sleep_n_sec(10)
                    continue

                # Verify clone actually appears in the lvol list (up to 10 × 3 s)
                found = False
                for check in range(10):
                    lvols_now = self.sbcli_utils.list_lvols()
                    if clone_name in lvols_now:
                        found = True
                        break
                    self.logger.info(
                        f"[create_clones] Waiting for {clone_name!r} in list "
                        f"(check {check + 1}/10)…"
                    )
                    sleep_n_sec(3)

                if found:
                    clone_created = True
                    break

                self.logger.warning(
                    f"[create_clones] {clone_name!r} not found in lvol list after "
                    f"10 checks; assuming creation failed. Waiting 10s before retry."
                )
                sleep_n_sec(10)

            if not clone_created:
                raise RuntimeError(
                    f"[create_clones] Failed to create clone for snapshot "
                    f"{snapshot_name!r} ({clone_name!r}) after 5 attempts."
                )
            fs_type = self.lvol_mount_details[lvol]["FS"]
            client = self.lvol_mount_details[lvol]["Client"]
            self.clone_mount_details[clone_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(clone_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{clone_name}.log",
                   "snapshot": snapshot_name,
                   "Client": client,
                   "iolog_base_path": f"{self.log_path}/{clone_name}_fio_iolog",
                   "pending_connect": False,
            }

            self.logger.info(f"Created clone {clone_name}.")

            # [COMMENTED OUT — kept for future use]
            # K8s temp change: defer clone connect/mount/FIO when any outage is active.
            # Uncomment if K8s secondary-only connect failures return.
            # if self.k8s_test and self.current_outage_nodes:
            #     self.clone_mount_details[clone_name]["pending_connect"] = True
            #     self.logger.info(
            #         f"[pending_connect] Clone '{clone_name}' deferred — outage active on nodes {self.current_outage_nodes}."
            #     )
            #     continue

            sleep_n_sec(3)

            if not self.k8s_test:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                          command=f"{self.base_cmd} lvol list")

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            self.clone_mount_details[clone_name]["Command"] = connect_ls

            # if self.secondary_outage:
            #     connect_ls = [connect_ls[0]]
            #     self.lvols_without_sec_connect.append(clone_name)

            # Extract NQN from connect string for later "already connected" recovery
            clone_nqn = None
            for _cs in connect_ls:
                if '--nqn=' in _cs:
                    clone_nqn = _cs.split('--nqn=')[1].split()[0]
                    break

            initial_devices = self.ssh_obj.get_devices(node=client)
            already_connected = False
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
                if error:
                    if "already connected" in error.lower():
                        # Device is already connected from a previous cycle — not an error
                        already_connected = True
                        self.logger.info(
                            f"[clone_connect] {clone_name} already connected on {client}"
                            f" (NQN={clone_nqn}); will locate existing device."
                        )
                    else:
                        self.record_failed_nvme_connect(clone_name, connect_str, client=client)

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break

            if not lvol_device and already_connected and clone_nqn:
                # Device existed before initial_devices snapshot; find it by NQN
                lvol_device = self.ssh_obj.get_nvme_device_for_nqn(client, clone_nqn)
                if lvol_device:
                    self.logger.info(
                        f"[clone_connect] Located already-connected device "
                        f"for {clone_name}: {lvol_device}"
                    )

            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.clone_mount_details[clone_name]["Device"] = lvol_device

            # Mount and Run FIO
            if fs_type == "xfs":
                self.ssh_obj.clone_mount_gen_uuid(client, lvol_device)
            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.clone_mount_details[clone_name]["Mount"] = mount_point

            # clone_node_id = self.sbcli_utils.get_lvol_details(
            #     lvol_id=self.lvol_mount_details[clone_name]["ID"])[0]["node_id"]
            
            # self.node_vs_lvol[clone_node_id].append(clone_name)

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/{clone_name}_fio_iolog*"])

            sleep_n_sec(5)

            # Start FIO
            # fio_thread = threading.Thread(
            #     target=self.ssh_obj.run_fio_test,
            #     args=(client, None, self.clone_mount_details[clone_name]["Mount"], self.clone_mount_details[clone_name]["Log"]),
            #     kwargs={
            #         "size": self.fio_size,
            #         "name": f"{clone_name}_fio",
            #         "rw": "randrw",
            #         "bs": f"{2 ** random.randint(2, 7)}K",
            #         "nrfiles": 16,
            #         "iodepth": 1,
            #         "numjobs": 5,
            #         "time_based": True,
            #         "runtime": 2000,
            #         "log_avg_msec": 1000,
            #         "iolog_file": self.clone_mount_details[clone_name]["iolog_base_path"],
            #         "debug": True,
            #     },
            # )
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, self.clone_mount_details[clone_name]["Mount"], self.clone_mount_details[clone_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{clone_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": self.fio_num_jobs,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.clone_mount_details[clone_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            self.logger.info(f"Created snapshot {snapshot_name} and clone {clone_name}.")

            if self.lvol_mount_details[lvol]["ID"]:
                self.sbcli_utils.resize_lvol(lvol_id=self.lvol_mount_details[lvol]["ID"],
                                             new_size=f"{self.int_lvol_size}G")
            sleep_n_sec(10)
            if self.clone_mount_details[clone_name]["ID"]:
                self.sbcli_utils.resize_lvol(lvol_id=self.clone_mount_details[clone_name]["ID"],
                                             new_size=f"{self.int_lvol_size}G")
        self._log_block_sizes("after_resize")

    # [COMMENTED OUT — kept for future use]
    # K8s temp method: connect/mount/FIO for clones deferred during outage.
    # Uncomment together with the deferred-connect block above if needed.
    # def _connect_pending_clones(self):
    #     """K8s only: connect, mount, and run FIO for clones deferred during outage (runtime=300s)."""
    #     pending = [(name, details) for name, details in self.clone_mount_details.items()
    #                if details.get("pending_connect")]
    #     if not pending:
    #         return
    #     self.logger.info(f"[pending_connect] Processing {len(pending)} deferred clone(s).")
    #     for clone_name, clone_details in pending:
    #         self.logger.info(f"[pending_connect] Connecting clone '{clone_name}'.")
    #         client = clone_details["Client"]
    #         fs_type = clone_details["FS"]
    #
    #         connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
    #         clone_details["Command"] = connect_ls
    #
    #         initial_devices = self.ssh_obj.get_devices(node=client)
    #         for connect_str in connect_ls:
    #             _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
    #             if error:
    #                 self.record_failed_nvme_connect(clone_name, connect_str, client=client)
    #
    #         sleep_n_sec(3)
    #         final_devices = self.ssh_obj.get_devices(node=client)
    #         lvol_device = None
    #         for device in final_devices:
    #             if device not in initial_devices:
    #                 lvol_device = f"/dev/{device.strip()}"
    #                 break
    #         if not lvol_device:
    #             self.logger.warning(f"[pending_connect] Clone '{clone_name}' did not connect after outage; skipping.")
    #             continue
    #
    #         clone_details["Device"] = lvol_device
    #
    #         if fs_type == "xfs":
    #             self.ssh_obj.clone_mount_gen_uuid(client, lvol_device)
    #         mount_point = f"{self.mount_path}/{clone_name}"
    #         self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
    #         clone_details["Mount"] = mount_point
    #
    #         sleep_n_sec(10)
    #
    #         self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
    #         self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])
    #         self.ssh_obj.delete_files(client, [f"{self.log_path}/{clone_name}_fio_iolog*"])
    #
    #         sleep_n_sec(5)
    #
    #         fio_thread = threading.Thread(
    #             target=self.ssh_obj.run_fio_test,
    #             args=(client, None, clone_details["Mount"], clone_details["Log"]),
    #             kwargs={
    #                 "size": self.fio_size,
    #                 "name": f"{clone_name}_fio",
    #                 "rw": "randrw",
    #                 "bs": f"{2 ** random.randint(2, 7)}K",
    #                 "nrfiles": 16,
    #                 "iodepth": 1,
    #                 "numjobs": 5,
    #                 "time_based": True,
    #                 "runtime": 300,
    #                 "log_avg_msec": 1000,
    #                 "iolog_file": clone_details["iolog_base_path"],
    #             },
    #         )
    #         fio_thread.start()
    #         self.fio_threads.append(fio_thread)
    #
    #         clone_details["pending_connect"] = False
    #         self.logger.info(f"[pending_connect] Clone '{clone_name}' connected and FIO started (runtime=300s).")

    def run(self):
        """Main N+K failover test loop. Performs lvol creation, fio, clone/snapshot, and multiple outages."""
        self.logger.info("Starting N+K failover test.")
        iteration = 1

        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = result["secondary_node_id"]
        self.logger.info(f"Secondary node map: {self.sn_primary_secondary_map}")

        if not self.spdk_mem_thread:
            self.spdk_mem_thread = threading.Thread(
                target=self._spdk_mem_stats_worker,
                kwargs={"interval_sec": 100},
                daemon=True
            )
            self.spdk_mem_thread.start()

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.create_lvols_with_fio(self.total_lvols)

        sleep_n_sec(30)

        while True:
            validation_thread = threading.Thread(target=self.validate_iostats_continuously, daemon=True)
            validation_thread.start()

            if self.dump_validation_errors:
                raise RuntimeError(
                    f"Placement dump validation failed: {self.dump_validation_errors}"
                )

            if iteration > 1:
                self.restart_fio(iteration=iteration)

            outage_events = self.perform_n_plus_k_outages()

            self.delete_random_lvols(5)
            self.create_lvols_with_fio(5)
            self.create_snapshots_and_clones()
            sleep_n_sec(280)

            for node, outage_type, node_outage_dur in outage_events:
                self.current_outage_node = node
                self.outage_dur = node_outage_dur
                if (outage_type == "container_stop" or "interface_full_network_interrupt" in outage_type) and self.npcs > 1:
                # [COMMENTED OUT — exact match (before duration suffix was added to outage log names)]:
                # if outage_type in ["container_stop", "interface_full_network_interrupt"] and self.npcs > 1:
                    self.restart_nodes_after_failover(outage_type, True)
                else:
                    self.restart_nodes_after_failover(outage_type)

                self.logger.info("Waiting for fallback recovery.")
                sleep_n_sec(100)

            # Reconnect multipath NICs after all node outages are recovered
            self._reconnect_multipath_nics()

            self.collect_outage_diagnostics("post_recovery")

            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            sleep_n_sec(300)

            self.retry_failed_nvme_connects()
            # [COMMENTED OUT — kept for future use] K8s deferred-connect handler
            # if self.k8s_test:
            #     self._connect_pending_clones()
            self.validate_pending_deletions()

            self.check_core_dump()

            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )

            no_task_ok = outage_type in {"partial_nw", "partial_nw_single_port", "lvol_disconnect_primary"}
            # for node, outage_type in outage_events:
            #     if not self.sbcli_utils.is_secondary_node(node):
            self.validate_migration_for_node(self.outage_start_time, 2000, None, 60, no_task_ok=no_task_ok)
            self.common_utils.manage_fio_threads(self.fio_node, self.fio_threads, timeout=5000)

            for clone, clone_details in self.clone_mount_details.items():
                if self.k8s_test and clone_details.get("pending_connect"):
                    # Connect failed after outage — no FIO was started; skip validation
                    self.logger.warning(f"[pending_connect] Skipping FIO validation for unconnected clone '{clone}'.")
                    continue
                self.common_utils.validate_fio_test(clone_details["Client"], clone_details["Log"])
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone}_fio*"])
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone}_fio_iolog*"])

            for lvol, lvol_details in self.lvol_mount_details.items():
                self.common_utils.validate_fio_test(lvol_details["Client"], lvol_details["Log"])
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])

            self.logger.info(f"N+K failover iteration {iteration} complete.")

            self.collect_outage_diagnostics(f"end_iteration_{iteration}")
            iteration += 1

