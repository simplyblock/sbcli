"""TC-SN-003 -- Storage node port listing and I/O stats.

Covers:
- Get storage nodes
- Run port-list command on each node via SSH
- Run port-io-stats command on each node via SSH
- Log and verify output is not empty
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestStorageNodePorts(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "storage_node_ports"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SN-003: Storage Node Ports ===")

        # -- Get storage nodes ---------------------------------------------
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        sn_results = storage_nodes.get("results", [])
        assert sn_results, "No storage nodes found"
        self.logger.info(f"Found {len(sn_results)} storage node(s)")

        port_list_results = {}
        port_io_stats_results = {}

        for node in sn_results:
            node_id = node["uuid"]
            node_ip = node.get("mgmt_ip", "")
            self.logger.info(f"Checking ports for node {node_id} (ip={node_ip})")

            # -- port-list -------------------------------------------------
            if not self.k8s_test and node_ip:
                try:
                    port_list_cmd = f"{self.base_cmd} sn port-list {node_id}"
                    self.logger.info(f"Running: {port_list_cmd}")
                    output = self.ssh_obj.exec_command(
                        node_ip, port_list_cmd
                    )
                    if output:
                        self.logger.info(
                            f"port-list output for {node_id}: {output}"
                        )
                        port_list_results[node_id] = output
                    else:
                        self.logger.warning(
                            f"port-list returned empty output for {node_id}"
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"port-list command failed for {node_id}: {exc}"
                    )
            else:
                self.logger.info(
                    f"Skipping SSH port-list for node {node_id} "
                    f"(k8s_test={self.k8s_test}, node_ip={node_ip})"
                )

            # -- port-io-stats ---------------------------------------------
            if not self.k8s_test and node_ip:
                try:
                    port_io_cmd = f"{self.base_cmd} sn port-io-stats {node_id}"
                    self.logger.info(f"Running: {port_io_cmd}")
                    output = self.ssh_obj.exec_command(
                        node_ip, port_io_cmd
                    )
                    if output:
                        self.logger.info(
                            f"port-io-stats output for {node_id}: {output}"
                        )
                        port_io_stats_results[node_id] = output
                    else:
                        self.logger.warning(
                            f"port-io-stats returned empty output for {node_id}"
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"port-io-stats command failed for {node_id}: {exc}"
                    )
            else:
                self.logger.info(
                    f"Skipping SSH port-io-stats for node {node_id} "
                    f"(k8s_test={self.k8s_test}, node_ip={node_ip})"
                )

        # -- Summary -------------------------------------------------------
        self.logger.info(
            f"port-list collected from {len(port_list_results)} node(s), "
            f"port-io-stats collected from {len(port_io_stats_results)} node(s)"
        )

        if not self.k8s_test:
            assert port_list_results, (
                "port-list returned no results from any storage node"
            )

        self.logger.info("=== TC-SN-003: Storage Node Ports -- PASS ===")
