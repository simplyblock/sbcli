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

Usage (bare-metal or K8s):
  test = RandomK8sMultiOutageFailoverTest(k8s_run=True, ...)
  test.run()

The npcs value can still be forced via the --npcs CLI arg; cluster detection
in run() only applies when --npcs default (1) is in use.
"""

from __future__ import annotations

import random
import threading

from exceptions.custom_exception import LvolNotConnectException
from logger_config import setup_logger
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
    generate_random_sequence,
)
from utils.common_utils import sleep_n_sec

_NDCS_NPCS_CHOICES = [(1, 1), (1, 2), (2, 1)]


class RandomK8sMultiOutageFailoverTest(RandomMultiClientMultiFailoverTest):
    """
    N+K simultaneous outage stress test with random geometry (ndcs/npcs),
    designed for both bare-metal and K8s clusters.

    At runtime, run() reads two values from the cluster API:
      max_fault_tolerance  →  self.npcs  (how many nodes fail simultaneously)
      (ndcs, npcs) are chosen randomly per lvol from _NDCS_NPCS_CHOICES

    Fabric is always TCP. No security types or RDMA.

    K8s pod logging:
      When k8s_test=True, the inherited restart_nodes_after_failover()
      calls self.runner_k8s_log.restart_logging() instead of docker logging.
      On startup, TestClusterBase.setup() starts runner_k8s_log automatically.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "n_plus_k_k8s_geometry_failover_ha"

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
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                      command=f"{self.base_cmd} lvol list")

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
                    self.record_failed_nvme_connect(lvol_name, connect_str)
                    sleep_n_sec(30)
                    continue

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
                "K8s mode: pod logging via runner_k8s_log. "
                "Docker logging calls will be skipped automatically.")

        super().run()
