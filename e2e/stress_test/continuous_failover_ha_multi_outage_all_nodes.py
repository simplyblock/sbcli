from stress_test.continuous_failover_ha_multi_outage import RandomMultiClientMultiFailoverTest, generate_random_sequence
import random


class RandomMultiClientMultiFailoverAllNodesTest(RandomMultiClientMultiFailoverTest):
    """
    Same as RandomMultiClientMultiFailoverTest but outage nodes are selected from ALL
    nodes (primary and secondary alike).  Requires max_fault_tolerance > 1.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "n_plus_k_failover_multi_client_ha_all_nodes"

    # ------------------------------------------------------------------
    # Override: pick outage nodes from every node, not just primaries
    # ------------------------------------------------------------------
    def perform_n_plus_k_outages(self):
        """
        Select K outage nodes randomly from ALL storage nodes (primary and
        secondary).  No primary/secondary exclusion constraint is applied
        because max_fault_tolerance > 1 guarantees the cluster can survive it.
        """
        all_nodes = list(self.sn_nodes_with_sec)
        self.current_outage_nodes = []

        k = self.npcs
        if len(all_nodes) < k:
            raise Exception(
                f"Need {k} outage nodes, but only {len(all_nodes)} nodes exist."
            )

        outage_nodes = random.sample(all_nodes, k)
        self.logger.info(f"Selected outage nodes (all-nodes mode): {outage_nodes}")

        outage_combinations = []
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

            self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
                                      storage_node_id=node)

            status = self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=node_ip,
                storage_node_id=node,
                logs_path=self.docker_logs_path
            )
            if not status:
                raise RuntimeError("Placement Dump Status incorrect!!!")

            self.logger.info(f"Performing {outage_type} on node {node}.")
            self.log_outage_event(node, outage_type, "Outage started")

            node_outage_dur = 0
            if outage_type == "container_stop":
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            elif outage_type == "interface_partial_network_interrupt":
                self._disconnect_partial_interface(node, node_ip)
                node_outage_dur = 300
            elif outage_type == "interface_full_network_interrupt":
                node_outage_dur = self._disconnect_full_interface(node, node_ip)

            outage_combinations.append((node, outage_type, node_outage_dur))
            self.current_outage_nodes.append(node)

        from datetime import datetime
        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    # ------------------------------------------------------------------
    # Override run() to validate fault-tolerance requirement first
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
            f"max_fault_tolerance={max_fault_tolerance} — proceeding with all-nodes outage test."
        )
        super().run()
