from utils.common_utils import sleep_n_sec
from exceptions.custom_exception import LvolNotConnectException
from stress_test.continuous_failover_ha_multi_outage import (
    RandomMultiClientMultiFailoverTest,
    generate_random_sequence,
)
import threading
import random


class RandomRDMAMultiFailoverTest(RandomMultiClientMultiFailoverTest):
    """
    N+K failover stress test with fabric selected from cluster configuration.

    Fabrics used are derived from the cluster's fabric_rdma / fabric_tcp flags:
      - both enabled  →  each lvol picks fabric randomly from ["tcp", "rdma"]
      - only rdma     →  all lvols use fabric="rdma"
      - only tcp      →  all lvols use fabric="tcp"

    The number of simultaneous outages (K) is derived from max_fault_tolerance:
      - max_fault_tolerance = 1  →  1 outage at a time  (1+1 topology)
      - max_fault_tolerance = 2  →  2 outages at a time (N+2 topology)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "n_plus_k_failover_rdma_ha"
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
        """Create lvols (fabric chosen per-lvol) and start FIO."""
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
    # Network outage: 600 s (suits both RDMA and mixed-fabric clusters)
    # ------------------------------------------------------------------
    def _disconnect_full_interface(self, node, node_ip):
        self.logger.info("Handling full interface network interruption (600 s)...")
        active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
        outage_dur = 600
        self.logger.info(f"Outage duration: {outage_dur} s")
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, outage_dur),
        )
        self.disconnect_thread.start()
        return outage_dur

    # ------------------------------------------------------------------
    # run(): read cluster fabric flags + fault tolerance, then delegate
    # ------------------------------------------------------------------
    def run(self):
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

        # Derive simultaneous outage count from fault tolerance
        max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {max_fault_tolerance}")
        self.npcs = max_fault_tolerance
        self.logger.info(
            f"N+K test: {self.npcs} simultaneous outage(s) per cycle, "
            f"fabrics: {self.available_fabrics}"
        )
        super().run()
