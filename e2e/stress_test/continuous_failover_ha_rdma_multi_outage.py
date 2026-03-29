import random
import threading
from datetime import datetime

from utils.common_utils import sleep_n_sec
from exceptions.custom_exception import LvolNotConnectException
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
    generate_random_sequence,
)


class RandomRDMAMultiFailoverTest(RandomMultiClientMultiFailoverTest):
    """
    N+K failover stress test with fabric selected from cluster configuration.

    Fabrics used are derived from the cluster's fabric_rdma / fabric_tcp flags:
      - both enabled  →  each lvol picks fabric randomly from ["tcp", "rdma"]
      - only rdma     →  all lvols use fabric="rdma"
      - only tcp      →  all lvols use fabric="tcp"

    The number of simultaneous outages (K) equals self.npcs (number of parity
    chunks, passed via CLI):
      - npcs = 1  →  1 node outaged at a time
      - npcs > 1  →  K nodes outaged simultaneously

    Outage node selection strategy (controlled by max_fault_tolerance, a
    separate cluster property):
      - ft > 1  →  all-nodes mode: any K nodes may be chosen, including
                   primary+secondary pairs from the same replica group.
                   The cluster's fault tolerance guarantees it can survive.
      - ft <= 1 →  non-related-nodes mode: delegates to base class which
                   ensures no primary+secondary pair is taken simultaneously.

    Network outage duration is 30, 300, or 600 s:
      - 30 s  : network blip — SPDK stays running, tests reconnect recovery.
      - 300/600 s: triggers SPDK abort, tests full restart/recovery path.
    For the first node in a multi-outage the duration is forced to 300 or
    600 s (>1 min) so both nodes are simultaneously in outage.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "n_plus_k_failover_rdma_ha"
        self.max_fault_tolerance = 1  # updated in run() from cluster details
        self.available_fabrics = ["rdma"]   # overwritten in run() from cluster info
        self.outage_types = [
            "graceful_shutdown",
            "interface_full_network_interrupt",
        ]
        self.outage_types2 = [
            "container_stop",
            "graceful_shutdown",
            "interface_full_network_interrupt",
        ]

    # ------------------------------------------------------------------
    # Lvol creation — skip logic identical to TCP parent; fabric picked
    # per-lvol from self.available_fabrics.
    # ------------------------------------------------------------------
    def create_lvols_with_fio(self, count):
        """Create ``count`` lvols (fabric chosen per-lvol from self.available_fabrics) and start FIO.

        Skips nodes that are currently in outage to avoid placing new lvols on
        unavailable storage nodes.
        """
        for i in range(count):
            fabric = random.choice(self.available_fabrics)
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            while lvol_name in self.lvol_mount_details:
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"

            self.logger.info(
                f"Creating lvol: {lvol_name}, fabric: {fabric}, "
                f"fs: {fs_type}, crypto: {is_crypto}"
            )
            try:
                self.logger.info(f"Current Outage Nodes: {self.current_outage_nodes}")
                if self.current_outage_nodes:
                    skip_nodes = [
                        n for n in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[n] in self.current_outage_nodes
                    ]
                    for n in self.current_outage_nodes:
                        skip_nodes.append(n)
                    host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                        crypto=is_crypto, key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1], host_id=host_id[0], fabric=fabric,
                    )
                elif self.current_outage_node:
                    skip_nodes = [
                        n for n in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[n] == self.current_outage_node
                    ]
                    skip_nodes.append(self.current_outage_node)
                    skip_nodes.append(self.sn_primary_secondary_map[self.current_outage_node])
                    host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                        crypto=is_crypto, key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1], host_id=host_id[0], fabric=fabric,
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                        crypto=is_crypto, key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1], fabric=fabric,
                    )
            except Exception as e:
                self.logger.warning(f"Lvol creation failed: {e}. Retrying with different name.")
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
                try:
                    if self.current_outage_node:
                        skip_nodes = [
                            n for n in self.sn_primary_secondary_map
                            if self.sn_primary_secondary_map[n] == self.current_outage_node
                        ]
                        skip_nodes.append(self.current_outage_node)
                        skip_nodes.append(self.sn_primary_secondary_map[self.current_outage_node])
                        host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                            crypto=is_crypto, key1=self.lvol_crypt_keys[0],
                            key2=self.lvol_crypt_keys[1], host_id=host_id[0], fabric=fabric,
                        )
                    else:
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name, pool_name=self.pool_name, size=self.lvol_size,
                            crypto=is_crypto, key1=self.lvol_crypt_keys[0],
                            key2=self.lvol_crypt_keys[1], fabric=fabric,
                        )
                except Exception as exp:
                    self.logger.warning(f"Retry lvol creation failed: {exp}.")
                    continue

            self.lvol_mount_details[lvol_name] = {
                "ID": self.sbcli_utils.get_lvol_id(lvol_name),
                "Command": None, "Mount": None, "Device": None, "MD5": None,
                "FS": fs_type, "Log": f"{self.log_path}/{lvol_name}.log",
                "snapshots": [], "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog",
            }

            self.logger.info(f"Created lvol {lvol_name}.")
            sleep_n_sec(3)
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                      command=f"{self.base_cmd} lvol list")

            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol_name]["ID"])[0]["node_id"]
            if lvol_node_id in self.node_vs_lvol:
                self.node_vs_lvol[lvol_node_id].append(lvol_name)
            else:
                self.node_vs_lvol[lvol_node_id] = [lvol_name]

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.lvol_mount_details[lvol_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[lvol_name]["Client"] = client_node

            initial_devices = self.ssh_obj.get_devices(node=client_node)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client_node, command=connect_str)
                if error:
                    self.record_failed_nvme_connect(lvol_name, connect_str, client=client_node)

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client_node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.lvol_mount_details[lvol_name]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=client_node, device=lvol_device, fs_type=fs_type)

            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)
            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/{lvol_name}_fio_iolog"])

            sleep_n_sec(5)
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_point,
                      self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.lvol_mount_details[lvol_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            sleep_n_sec(10)

    # ------------------------------------------------------------------
    # Network outage: 30 / 300 / 600 s — tests both SPDK-abort and no-abort paths
    # ------------------------------------------------------------------
    def _disconnect_full_interface(self, node, node_ip, outage_position=0):
        """Disconnect all network interfaces for 30, 300, or 600 s.

        Duration determines whether the node's SPDK process is aborted:
          - 30 s  (<1 min): network blip only — SPDK stays running, tests
                            reconnect/recovery without a process restart.
          - 300/600 s (>1 min): triggers SPDK abort on the node, tests full
                                restart and recovery path.

        Selection logic:
          - First node in a multi-outage (outage_position == 0, npcs > 1):
            forced to random.choice([600, 300]) so the interface is still
            down (>1 min) when the second outage fires simultaneously.
          - All other cases: random.choice([600, 300, 30]).

        Returns:
            (outage_dur, effective_name): duration in seconds and the canonical
            outage-type string e.g. ``"interface_full_network_interrupt_30sec"``.
        """
        self.logger.info("Handling full interface network interruption (RDMA)...")
        active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
        if outage_position == 0 and self.npcs > 1:
            outage_dur = random.choice([600, 300])
        else:
            outage_dur = random.choice([600, 300, 30])
        effective_name = f"interface_full_network_interrupt_{outage_dur}sec"
        self.logger.info(f"[N/W Outage] duration={outage_dur}s → {effective_name}")
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, outage_dur),
        )
        self.disconnect_thread.start()
        return outage_dur, effective_name

    # ------------------------------------------------------------------
    # perform_n_plus_k_outages: node selection depends on fault tolerance
    #   ft > 1  →  any K nodes (including primary+secondary pairs) — all-nodes mode
    #   ft <= 1 →  K non-related nodes (no primary-secondary pairs) — base class
    # ------------------------------------------------------------------
    def perform_n_plus_k_outages(self):
        """Select outage nodes and trigger K simultaneous outages.

        Node selection strategy:
          - ft > 1: sample from ALL storage nodes; primary+secondary pairs are
            permitted because the cluster fault tolerance covers it.
          - ft <= 1: delegate to the base class which enforces the non-related-
            nodes constraint (no primary+secondary pair taken together).

        Uses a two-phase approach so all outages start nearly simultaneously:
          Phase 1 (sequential): collect node details + run pre-dumps for every
            outage node before any outage is triggered.
          Phase 2 (parallel): fire each outage in its own thread so all nodes
            go down within a few seconds of each other.
        """
        if self.max_fault_tolerance <= 1:
            # Single outage or non-related-node constraint: delegate to base class
            return super().perform_n_plus_k_outages()

        # ft > 1: pick from ALL nodes — primary+secondary pairs are allowed
        all_nodes = list(self.sn_nodes_with_sec)
        self.current_outage_nodes = []

        k = self.npcs
        if len(all_nodes) < k:
            raise Exception(
                f"Need {k} outage nodes, but only {len(all_nodes)} nodes exist."
            )

        outage_nodes = random.sample(all_nodes, k)
        self.logger.info(
            f"Selected outage nodes (all-nodes mode, ft={self.max_fault_tolerance}): {outage_nodes}"
        )

        # ── Phase 1: pick types + pre-dump for ALL nodes ──────────────────────
        node_plans = []
        outage_num = 0
        for i, node in enumerate(outage_nodes):
            outage_type = (
                random.choice(self.outage_types) if outage_num == 0
                else random.choice(self.outage_types2)
            )
            outage_num = 1

            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]

            self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
                                      storage_node_id=node)
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=node_ip,
                storage_node_id=node,
                logs_path=self.docker_logs_path,
                validate_async=True,
                error_sink=self.dump_validation_errors
            )

            node_plans.append((node, outage_type, node_ip, node_rpc_port, i))

        # ── Phase 2: trigger all outages simultaneously via threads ────────────
        outage_results = {}

        def _trigger(node, outage_type, node_ip, node_rpc_port, position):
            self.logger.info(f"Performing {outage_type} on node {node}.")
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
                node_outage_dur, effective_type = self._disconnect_full_interface(
                    node, node_ip, position)
            self.log_outage_event(node, effective_type, "Outage started")
            outage_results[node] = (effective_type, node_outage_dur)

        threads = [
            threading.Thread(target=_trigger, args=(node, otype, nip, nrpc, pos))
            for node, otype, nip, nrpc, pos in node_plans
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        outage_combinations = []
        for node, _, _, _, _ in node_plans:
            effective_type, node_outage_dur = outage_results[node]
            outage_combinations.append((node, effective_type, node_outage_dur))
            self.current_outage_nodes.append(node)

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    # ------------------------------------------------------------------
    # run(): read cluster fabric flags + fault tolerance, then delegate
    # ------------------------------------------------------------------
    def run(self):
        """Read cluster config, set fabric list + fault tolerance, then run.

        Derives self.available_fabrics from the cluster's fabric_rdma / fabric_tcp
        flags and reads max_fault_tolerance into self.max_fault_tolerance so that
        perform_n_plus_k_outages() can choose the right node-selection mode.
        self.npcs (outage count) is already set from CLI kwargs and is not changed.
        """
        self.logger.info("Reading cluster config for RDMA N+K test.")
        cluster_details = self.sbcli_utils.get_cluster_details()

        # Determine available fabrics from cluster flags
        fabric_rdma = cluster_details.get("fabric_rdma", False)
        fabric_tcp = cluster_details.get("fabric_tcp", True)
        if fabric_rdma and fabric_tcp:
            self.available_fabrics = ["tcp", "rdma"]
        elif fabric_rdma:
            self.available_fabrics = ["rdma"]
        else:
            self.available_fabrics = ["tcp"]
        self.logger.info(f"Available fabrics: {self.available_fabrics}")

        # Read fault tolerance for node-selection mode; npcs (outage count) comes from CLI kwargs
        max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {max_fault_tolerance}")
        self.max_fault_tolerance = max_fault_tolerance
        self.logger.info(
            f"N+K test: {self.npcs} simultaneous outage(s) per cycle "
            f"({'all-nodes' if max_fault_tolerance > 1 else 'non-related-nodes'} mode), "
            f"fabrics: {self.available_fabrics}"
        )
        super().run()
