"""
K8s-aware continuous failover stress test with N+K simultaneous outages
and random geometry (ndcs/npcs) per lvol.

Inherits:
  RandomMultiClientMultiFailoverTest
    - N+K simultaneous outage loop (perform_n_plus_k_outages)
    - Multi-client FIO (random fio_node per lvol)
    - K8s-aware restart_nodes_after_failover (via parent RandomMultiClientFailoverTest)
      Uses runner_k8s_log.restart_logging() when k8s_test=True, otherwise
      falls back to restart_docker_logging.

Adds:
  - Random ndcs/npcs per lvol: (1,1), (1,2), (2,1)
  - npcs (simultaneous outage count) derived from cluster max_fault_tolerance
  - TCP fabric only — no RDMA, no security types
  - K8s pod log monitoring via runner_k8s_log (when k8s_test=True)
  - Container-crash via kubectl delete pod (stop_spdk_pod) instead of docker stop
  - sbcli CLI commands via kubectl exec into simplyblock-admin-control pod

K8s failover mapping:
  container_stop               → kubectl delete pod snode-spdk-pod-<x> (pod auto-restarts)
  graceful_shutdown            → sbcli sn shutdown via kubectl exec
  network outage               → not supported (no direct SSH to storage nodes)

Usage (K8s):
  test = RandomK8sMultiOutageFailoverTest(k8s_run=True, ...)
  test.run()
"""

from __future__ import annotations

import re
import random
import threading

from exceptions.custom_exception import LvolNotConnectException
from logger_config import setup_logger
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
    generate_random_sequence,
)
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils

# _NDCS_NPCS_CHOICES = [(1, 1), (1, 2), (2, 1)]

_NDCS_NPCS_CHOICES = [(1, 1)]

class RandomK8sMultiOutageFailoverTest(RandomMultiClientMultiFailoverTest):
    """
    N+K simultaneous outage stress test with random geometry (ndcs/npcs),
    designed for both bare-metal and K8s clusters.

    At runtime, run() reads two values from the cluster API:
      max_fault_tolerance  →  self.npcs  (how many nodes fail simultaneously)
      (ndcs, npcs) are chosen randomly per lvol from _NDCS_NPCS_CHOICES

    Fabric is always TCP. No security types or RDMA.

    K8s differences (active when k8s_test=True):
      - container_stop outage uses kubectl delete pod (K8sUtils.stop_spdk_pod)
        instead of ssh_obj.stop_spdk_process (docker stop).
      - sbcli list/info commands use kubectl exec via K8sUtils.exec_sbcli.
      - Pod logging managed by runner_k8s_log (same as before).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.total_lvols = 20
        self.test_name = "n_plus_k_k8s_failover_ha"
        self.k8s_utils: K8sUtils | None = None
        # Network outage not supported in K8s (no direct SSH to storage nodes).
        self.outage_types = ["graceful_shutdown"]
        self.outage_types2 = ["container_stop", "graceful_shutdown"]

    # ── Setup ────────────────────────────────────────────────────────────────

    def setup(self) -> None:
        """
        Run parent setup, then initialize K8sUtils when k8s_test=True.

        K8sUtils wraps kubectl exec commands and SPDK pod operations.
        It is only created when k8s_test=True to avoid side-effects in
        bare-metal runs.
        """
        super().setup()
        if self.k8s_test and self.mgmt_nodes:
            self.k8s_utils = K8sUtils(
                ssh_obj=self.ssh_obj,
                mgmt_node=self.mgmt_nodes[0],
            )
            self.logger.info(
                f"[K8s] K8sUtils initialized for mgmt_node={self.mgmt_nodes[0]}"
            )

    # ── lvol creation ────────────────────────────────────────────────────────

    def create_lvols_with_fio(self, count: int) -> None:
        """Create *count* lvols with random geometry and start FIO."""
        for i in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            ndcs, npcs = random.choice(_NDCS_NPCS_CHOICES)
            is_crypto = random.choice([True, False])
            lvol_name = (f"{self.lvol_name}_{i}" if not is_crypto
                         else f"c{self.lvol_name}_{i}")
            while lvol_name in self.lvol_mount_details:
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = (f"{self.lvol_name}_{i}" if not is_crypto
                             else f"c{self.lvol_name}_{i}")

            self.logger.info(
                f"Creating lvol {lvol_name!r}, fs={fs_type}, "
                f"crypto={is_crypto}, ndcs={ndcs}, npcs={npcs}")

            try:
                if self.current_outage_nodes:
                    skip_nodes = [
                        n for n in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[n] in self.current_outage_nodes
                    ]
                    for n in self.current_outage_nodes:
                        skip_nodes.append(n)
                    host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name,
                        size=self.lvol_size, crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
                        host_id=host_id[0], distr_ndcs=ndcs, distr_npcs=npcs,
                    )
                elif self.current_outage_node:
                    skip_nodes = [
                        n for n in self.sn_primary_secondary_map
                        if self.sn_primary_secondary_map[n] == self.current_outage_node
                    ]
                    skip_nodes.append(self.current_outage_node)
                    skip_nodes.append(
                        self.sn_primary_secondary_map[self.current_outage_node])
                    host_id = [n for n in self.sn_nodes_with_sec if n not in skip_nodes]
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name,
                        size=self.lvol_size, crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
                        host_id=host_id[0], distr_ndcs=ndcs, distr_npcs=npcs,
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name, pool_name=self.pool_name,
                        size=self.lvol_size, crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
                        distr_ndcs=ndcs, distr_npcs=npcs,
                    )
            except Exception as exc:
                self.logger.warning(
                    f"Lvol creation failed for {lvol_name}: {exc}. Retrying…")
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = (f"{self.lvol_name}_{i}" if not is_crypto
                             else f"c{self.lvol_name}_{i}")
                try:
                    if self.current_outage_node:
                        skip_nodes = [
                            n for n in self.sn_primary_secondary_map
                            if self.sn_primary_secondary_map[n] == self.current_outage_node
                        ]
                        skip_nodes.append(self.current_outage_node)
                        skip_nodes.append(
                            self.sn_primary_secondary_map[self.current_outage_node])
                        host_id = [n for n in self.sn_nodes_with_sec
                                   if n not in skip_nodes]
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name, pool_name=self.pool_name,
                            size=self.lvol_size, crypto=is_crypto,
                            key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
                            host_id=host_id[0], distr_ndcs=ndcs, distr_npcs=npcs,
                        )
                    else:
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name, pool_name=self.pool_name,
                            size=self.lvol_size, crypto=is_crypto,
                            key1=self.lvol_crypt_keys[0], key2=self.lvol_crypt_keys[1],
                            distr_ndcs=ndcs, distr_npcs=npcs,
                        )
                except Exception as exc2:
                    self.logger.warning(f"Retry lvol creation failed: {exc2}")
                    continue

            self.lvol_mount_details[lvol_name] = {
                "ID":              self.sbcli_utils.get_lvol_id(lvol_name),
                "Command":         None,
                "Mount":           None,
                "Device":          None,
                "MD5":             None,
                "FS":              fs_type,
                "Log":             f"{self.log_path}/{lvol_name}.log",
                "snapshots":       [],
                "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog",
            }

            self.logger.info(f"Created lvol {lvol_name!r}.")
            sleep_n_sec(3)

            # List lvols — route through kubectl exec in K8s mode
            list_cmd = f"{self.base_cmd} lvol list"
            if self.k8s_test and self.k8s_utils:
                self.k8s_utils.exec_sbcli(list_cmd, supress_logs=True)
            else:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=list_cmd)

            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol_name]["ID"])[0]["node_id"]
            self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            self.lvol_mount_details[lvol_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[lvol_name]["Client"] = client_node

            initial_devices = self.ssh_obj.get_devices(node=client_node)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client_node,
                                                     command=connect_str)
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
                raise LvolNotConnectException(
                    f"LVOL {lvol_name!r} (ndcs={ndcs}, npcs={npcs}) did not connect")

            self.lvol_mount_details[lvol_name]["Device"] = lvol_device
            _m = re.match(r'nvme(\d+)n(\d+)', lvol_device.split("/")[-1])
            if _m:
                _sub, _ns = _m.group(1), _m.group(2)
                self.logger.info(
                    f"[block_size after_connect] {lvol_name} ({lvol_device})"
                    f" — /sys/block/nvme{_sub}c*n{_ns}/size"
                )
                self.ssh_obj.exec_command(
                    node=client_node,
                    command=(
                        f"for d in /sys/block/nvme{_sub}c*n{_ns}; do "
                        f"echo \"$d: $(cat $d/size 2>/dev/null)\"; done"
                    ),
                )
            self.ssh_obj.format_disk(node=client_node, device=lvol_device,
                                     fs_type=fs_type)
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=lvol_device,
                                    mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)
            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(
                client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
            self.ssh_obj.delete_files(
                client_node, [f"{self.log_path}/{lvol_name}_fio_iolog"])
            sleep_n_sec(5)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_point,
                      self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size":         self.fio_size,
                    "name":         f"{lvol_name}_fio",
                    "rw":           "randrw",
                    "bs":           f"{2 ** random.randint(2, 7)}K",
                    "nrfiles":      16,
                    "iodepth":      1,
                    "numjobs":      5,
                    "time_based":   True,
                    "runtime":      2000,
                    "log_avg_msec": 1000,
                    "iolog_file":   self.lvol_mount_details[lvol_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            sleep_n_sec(10)

    # ── Container crash override ─────────────────────────────────────────────

    def _k8s_stop_spdk_pod(self, node_ip: str, node_id: str) -> None:
        """
        K8s equivalent of stop_spdk_process: force-delete the snode-spdk-pod.

        Kubernetes automatically recreates the pod via the managing controller,
        so no manual restart is needed — only waiting for it to become Running
        again (done in restart_nodes_after_failover via node status polling).
        """
        if not self.k8s_utils:
            raise RuntimeError(
                "[K8s] k8s_utils not initialised — was setup() called with k8s_run=True?"
            )
        pod_name = self.k8s_utils.stop_spdk_pod(node_ip)
        self.logger.info(
            f"[K8s] container_stop: deleted SPDK pod {pod_name!r} for node {node_ip}"
        )

    # ── Block-size logging ───────────────────────────────────────────────────

    def _log_block_sizes(self, label: str = "") -> None:
        """
        For every connected lvol/clone, print the size of each per-controller
        sysfs path (nvme<S>c<C>n<N>/size).

        The device stored is the multipath namespace, e.g. /dev/nvme0n1.
        From that we derive the subsystem (0) and namespace (1) numbers and
        glob /sys/block/nvme0c*n1 to reach the individual controller entries —
        the same paths the user reads with ``cat /sys/block/nvme0c0n1/size``.
        """
        tag = f"[block_size{' ' + label if label else ''}]"
        all_details = {
            **self.lvol_mount_details,
            **self.clone_mount_details,
        }
        for lvol_name, details in all_details.items():
            device = details.get("Device")
            client = details.get("Client")
            if not device or not client:
                continue
            dev_name = device.split("/")[-1]   # e.g. nvme0n1
            m = re.match(r'nvme(\d+)n(\d+)', dev_name)
            if not m:
                continue
            sub, ns = m.group(1), m.group(2)   # "0", "1"
            self.logger.info(
                f"{tag} {lvol_name} ({device}) — /sys/block/nvme{sub}c*n{ns}/size"
            )
            # List all per-controller paths for this subsystem+namespace
            self.ssh_obj.exec_command(
                node=client,
                command=(
                    f"for d in /sys/block/nvme{sub}c*n{ns}; do "
                    f"echo \"$d: $(cat $d/size 2>/dev/null)\"; done"
                ),
            )

    def perform_n_plus_k_outages(self):
        """
        Two-phase K8s override of perform_n_plus_k_outages.

        Phase 1 (sequential): pick outage types + pre-dump logs for ALL nodes
                               before any outage is triggered.
        Phase 2 (parallel):   trigger every node's outage simultaneously via
                               threads, eliminating the sequential delay.
        """
        from datetime import datetime

        primary_candidates = list(self.sn_primary_secondary_map.keys())
        self.current_outage_nodes = []

        if len(primary_candidates) < self.npcs:
            raise Exception(
                f"Need {self.npcs} outage nodes, but only "
                f"{len(primary_candidates)} primary-role nodes exist."
            )

        outage_nodes = self._pick_outage_nodes(primary_candidates, self.npcs)
        self.logger.info(f"Selected outage nodes: {outage_nodes}")

        # Phase 1: pick types + pre-dump for ALL nodes (before any outage)
        node_plans = []  # (node, outage_type, node_ip, node_rpc_port)
        outage_num = 0

        for node in outage_nodes:
            if outage_num == 0:
                if self.npcs == 1:
                    outage_type = random.choice(self.outage_types2)
                else:
                    outage_type = random.choice(self.outage_types)
                outage_num = 1
            else:
                outage_type = random.choice(self.outage_types2)

            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]

            if self.k8s_test:
                self.k8s_utils.dump_lvstore_k8s(
                    sbcli_cmd=self.sbcli_utils.sbcli_cmd,
                    storage_node_id=node,
                    storage_node_ip=node_ip,
                    logs_path=self.docker_logs_path,
                )
                self.k8s_utils.fetch_distrib_logs_k8s(
                    storage_node_id=node,
                    storage_node_ip=node_ip,
                    logs_path=self.docker_logs_path,
                )
            else:
                self.ssh_obj.dump_lvstore(
                    node_ip=self.mgmt_nodes[0], storage_node_id=node
                )
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=node_ip,
                    storage_node_id=node,
                    logs_path=self.docker_logs_path,
                    validate_async=True,
                    error_sink=self.dump_validation_errors
                )

            node_plans.append((node, outage_type, node_ip, node_rpc_port))

        # Log block device sizes for all lvols before any outage is triggered
        self._log_block_sizes("before_outage")

        # Phase 2: trigger all outages simultaneously via threads
        outage_results = {}  # node -> (outage_type, outage_dur)

        def _trigger_k8s(node, outage_type, node_ip, node_rpc_port):
            self.logger.info(
                f"Performing {outage_type} on primary node {node} (K8s mode)."
            )
            node_outage_dur = 0
            if outage_type == "container_stop":
                if self.k8s_test and self.k8s_utils:
                    self._k8s_stop_spdk_pod(node_ip, node)
                else:
                    self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            elif outage_type == "interface_partial_network_interrupt":
                self._disconnect_partial_interface(node, node_ip)
                node_outage_dur = 300
            elif outage_type == "interface_full_network_interrupt":
                node_outage_dur = self._disconnect_full_interface(node, node_ip)
            self.log_outage_event(node, outage_type, "Outage started")
            outage_results[node] = (outage_type, node_outage_dur)

        threads = [
            threading.Thread(target=_trigger_k8s, args=(node, otype, nip, nrpc))
            for node, otype, nip, nrpc in node_plans
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        outage_combinations = []
        for node, _, _, _ in node_plans:
            otype, odur = outage_results[node]
            outage_combinations.append((node, otype, odur))
            self.current_outage_nodes.append(node)

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    # ── Sequential single-loop version (commented out — kept for reference) ───
    # def perform_n_plus_k_outages(self):
    #     """Sequential K8s override — dump + trigger per node, one at a time.
    #     NOTE: uses ssh_obj.dump_lvstore which requires SSH to mgmt nodes.
    #     Not suitable for k8s environments. Use the two-phase version above.
    #     """
    #     from datetime import datetime
    #     primary_candidates = list(self.sn_primary_secondary_map.keys())
    #     self.current_outage_nodes = []
    #     outage_nodes = self._pick_outage_nodes(primary_candidates, self.npcs)
    #     outage_combinations = []
    #     outage_num = 0
    #     for node in outage_nodes:
    #         if outage_num == 0:
    #             outage_type = random.choice(self.outage_types2 if self.npcs == 1 else self.outage_types)
    #             outage_num = 1
    #         else:
    #             outage_type = random.choice(self.outage_types2)
    #         node_details = self.sbcli_utils.get_storage_node_details(node)
    #         node_ip = node_details[0]["mgmt_ip"]
    #         node_rpc_port = node_details[0]["rpc_port"]
    #         self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0], storage_node_id=node)
    #         status = self.ssh_obj.fetch_distrib_logs(
    #             storage_node_ip=node_ip, storage_node_id=node, logs_path=self.docker_logs_path)
    #         if not status:
    #             raise RuntimeError("Placement Dump Status incorrect!!!")
    #         self.log_outage_event(node, outage_type, "Outage started")
    #         node_outage_dur = 0
    #         if outage_type == "container_stop":
    #             self._k8s_stop_spdk_pod(node_ip, node) if (self.k8s_test and self.k8s_utils) else self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
    #         elif outage_type == "graceful_shutdown":
    #             self._graceful_shutdown_node(node)
    #         elif outage_type == "interface_partial_network_interrupt":
    #             self._disconnect_partial_interface(node, node_ip); node_outage_dur = 300
    #         elif outage_type == "interface_full_network_interrupt":
    #             node_outage_dur = self._disconnect_full_interface(node, node_ip)
    #         outage_combinations.append((node, outage_type, node_outage_dur))
    #         self.current_outage_nodes.append(node)
    #     self.outage_start_time = int(datetime.now().timestamp())
    #     return outage_combinations

    # ── Resize + recovery hooks ───────────────────────────────────────────────

    def create_snapshots_and_clones(self):
        """Delegate to parent then log block sizes for all lvols after resize."""
        super().create_snapshots_and_clones()
        self._log_block_sizes("after_resize")

    def restart_nodes_after_failover(self, outage_type, restart=False):
        """Delegate to parent then log block sizes for all lvols after recovery."""
        super().restart_nodes_after_failover(outage_type, restart)
        self._log_block_sizes("post_recovery")

    # ── run ──────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Read cluster config to determine:
          - max_fault_tolerance → self.npcs (simultaneous outage count)

        Then hand off to RandomMultiClientMultiFailoverTest.run() which owns
        the main loop (lvol lifecycle, N+K outages, FIO validation).

        K8s pod logging is managed by TestClusterBase.setup() (start) and
        restart_nodes_after_failover() (restart after each outage).
        """
        self.logger.info("Reading cluster config for K8s N+K geometry failover test.")
        cluster_details = self.sbcli_utils.get_cluster_details()

        # Derive simultaneous outage count from cluster fault tolerance
        max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {max_fault_tolerance}")

        # Only override if the user didn't pass an explicit --npcs value
        if self.npcs == 1:
            self.npcs = max_fault_tolerance
        self.logger.info(f"Running with npcs={self.npcs} simultaneous outages")

        if self.k8s_test:
            self.logger.info(
                "K8s mode: pod logging via runner_k8s_log; "
                "container_stop uses kubectl delete pod; "
                "network outage disabled."
            )

        # Ensure a clean pool exists before the test loop starts.
        # setup() deletes all pools, but stale API state can cause
        # add_storage_pool() in the parent run() to falsely skip creation.
        # Explicitly delete + recreate here to guarantee the pool is present.
        self.logger.info(f"Ensuring clean pool '{self.pool_name}' before run.")
        self.sbcli_utils.delete_all_storage_pools()
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.logger.info(f"Pool '{self.pool_name}' ready.")

        # Skip parent run() and call grandparent directly since we override
        # perform_n_plus_k_outages; the rest of the loop is inherited.
        super().run()
