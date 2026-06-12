"""Management Node Network Outage Test (Docker mode).

Stop the management node's network for 1 hour while IO is active on all
storage nodes.  Ensure no storage node failures and no IO failures.

Self-restoring iptables approach:
    A nohup background script on the mgmt node blocks all traffic for
    ``outage_duration`` seconds, then automatically flushes the rules.
    This guarantees recovery even if the test runner crashes.
"""

import threading
import time
import random
from pathlib import Path

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec


class MgmtNodeNetworkOutageTest(TestClusterBase):
    """Block mgmt-node network for 1 hour with active FIO on every storage node."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mgmt_node_network_outage"
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"
        # 1 hour outage
        self.outage_duration = 3600
        # FIO runtime: 2 hours (covers outage + recovery validation)
        self.fio_runtime = 7200
        self.fio_num_jobs = 5
        self.lvol_size = "10G"

    def run(self):
        fio_threads = []
        lvol_details = {}
        self.logger.info("=== MgmtNodeNetworkOutageTest: starting ===")

        # ------------------------------------------------------------------
        # Step 1: Create pool + 1 lvol per storage node, connect from fio client
        # ------------------------------------------------------------------
        self.sbcli_utils.add_storage_pool(self.pool_name)

        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        fio_node = random.choice(fio_nodes)

        for i, snode in enumerate(self.storage_nodes):
            node_uuid = self.sbcli_utils.get_node_without_lvols()
            lvol_name = f"mgmt_outage_{generate_random_sequence(4)}_{i}"
            self.sbcli_utils.add_lvol(lvol_name, self.pool_name,
                                      size=self.lvol_size, host_id=node_uuid)

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            connect_cmds = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for cmd in connect_cmds:
                self.ssh_obj.exec_command(fio_node, cmd)

            device = self.ssh_obj.get_lvol_vs_device(fio_node, lvol_id)
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"

            self.ssh_obj.format_disk(fio_node, device)
            self.ssh_obj.mount_path(fio_node, device, mount_path)

            lvol_details[lvol_name] = {
                "ID": lvol_id,
                "Mount": mount_path,
                "Log": log_path,
                "Device": device,
            }

        # ------------------------------------------------------------------
        # Step 2: Start FIO on every volume (time_based, runtime=7200)
        # ------------------------------------------------------------------
        self.logger.info(f"Starting FIO on {len(lvol_details)} volumes "
                         f"(runtime={self.fio_runtime}s, numjobs={self.fio_num_jobs})")

        for lvol_name, detail in lvol_details.items():
            t = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(fio_node, None, detail["Mount"], detail["Log"]),
                kwargs={
                    "size": "500M",
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": self.fio_num_jobs,
                    "time_based": True,
                    "runtime": self.fio_runtime,
                },
            )
            t.start()
            fio_threads.append(t)
            sleep_n_sec(3)

        # Let FIO warm up before triggering the outage
        self.logger.info("Waiting 60s for FIO to stabilise before outage ...")
        sleep_n_sec(60)

        # ------------------------------------------------------------------
        # Step 3: Record pre-outage state
        # ------------------------------------------------------------------
        self.logger.info("Recording pre-outage cluster/node state ...")
        pre_cluster = self.sbcli_utils.get_cluster_details()
        self.logger.info(f"  Cluster status: {pre_cluster['status']}")

        storage_nodes_resp = self.sbcli_utils.get_storage_nodes()
        pre_node_status = {}
        for sn in storage_nodes_resp["results"]:
            pre_node_status[sn["uuid"]] = sn["status"]
            self.logger.info(f"  Storage node {sn['uuid'][:8]} — {sn['status']}")

        # ------------------------------------------------------------------
        # Step 4: Block mgmt node network (self-restoring iptables)
        # ------------------------------------------------------------------
        mgmt_ip = self.mgmt_nodes[0]
        self.logger.info(f"Blocking ALL network on mgmt node {mgmt_ip} "
                         f"for {self.outage_duration}s (self-restoring) ...")

        # The 5-second delay allows the SSH command to return before the
        # DROP rules take effect.  After outage_duration the rules are
        # flushed automatically.
        iptables_script = (
            f"nohup bash -c '"
            f"sleep 5 && "
            f"iptables -A INPUT -j DROP && "
            f"iptables -A OUTPUT -j DROP && "
            f"sleep {self.outage_duration} && "
            f"iptables -F"
            f"' > /tmp/mgmt_outage.log 2>&1 &"
        )
        self.ssh_obj.exec_command(mgmt_ip, iptables_script)
        self.logger.info("iptables script launched; waiting 15s for rules to activate ...")
        sleep_n_sec(15)

        # Quick sanity: mgmt should be unreachable now
        self.logger.info("Mgmt node should be unreachable — "
                         "sleeping for outage duration ...")

        # ------------------------------------------------------------------
        # Step 5: Wait for outage duration
        # ------------------------------------------------------------------
        # Log a heartbeat every 5 minutes so the runner knows we are alive
        elapsed = 0
        heartbeat_interval = 300
        while elapsed < self.outage_duration:
            chunk = min(heartbeat_interval, self.outage_duration - elapsed)
            sleep_n_sec(chunk)
            elapsed += chunk
            self.logger.info(f"  Outage heartbeat: {elapsed}/{self.outage_duration}s elapsed")

        # ------------------------------------------------------------------
        # Step 6: Wait for mgmt node to recover (iptables auto-flushed)
        # ------------------------------------------------------------------
        self.logger.info("Outage duration elapsed — waiting for mgmt node recovery ...")
        # Give iptables flush a few extra seconds
        sleep_n_sec(30)

        # Poll for WebAppAPI container (up to 10 min)
        self.logger.info("Waiting for app_WebAppAPI container on mgmt node ...")
        webapp_up = False
        for attempt in range(60):
            try:
                containers = self.ssh_obj.get_running_containers(mgmt_ip)
                if any("app_WebAppAPI" in c for c in containers):
                    self.logger.info(f"WebAppAPI is online (attempt {attempt + 1})")
                    webapp_up = True
                    break
            except Exception as e:
                self.logger.info(f"  Attempt {attempt + 1}/60 — mgmt not reachable yet: {e}")
            sleep_n_sec(10)

        if not webapp_up:
            raise TimeoutError("app_WebAppAPI did not come online within 10 minutes "
                               "after iptables flush")

        # Extra settle time for control plane services
        self.logger.info("WebAppAPI online — waiting 120s for full control plane recovery ...")
        sleep_n_sec(120)

        # ------------------------------------------------------------------
        # Step 7: Verify cluster and storage node health
        # ------------------------------------------------------------------
        self.logger.info("Verifying cluster status is 'active' ...")
        self.sbcli_utils.wait_for_cluster_status(status="active", timeout=600)

        self.logger.info("Verifying all storage nodes are 'online' ...")
        storage_nodes_resp = self.sbcli_utils.get_storage_nodes()
        for sn in storage_nodes_resp["results"]:
            node_id = sn["uuid"]
            self.logger.info(f"  Checking storage node {node_id[:8]} ...")
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node_id, status="online", timeout=300
            )
            self.sbcli_utils.wait_for_health_status(
                node_id=node_id, status=True, timeout=300
            )
            self.logger.info(f"  Storage node {node_id[:8]} — online + healthy")

        # ------------------------------------------------------------------
        # Step 8: Wait for FIO completion
        # ------------------------------------------------------------------
        self.logger.info("Waiting for all FIO threads to complete ...")
        self.common_utils.manage_fio_threads(fio_node, fio_threads, timeout=1800)
        self.logger.info("All FIO threads completed.")

        # ------------------------------------------------------------------
        # Step 9: Validate FIO logs — no IO errors
        # ------------------------------------------------------------------
        self.logger.info("Validating FIO logs for every volume ...")
        for lvol_name, detail in lvol_details.items():
            self.logger.info(f"  Validating {lvol_name} ...")
            self.common_utils.validate_fio_test(
                node=fio_node, log_file=detail["Log"]
            )
        self.logger.info("All FIO logs validated — zero IO errors.")

        # ------------------------------------------------------------------
        # Step 10: Compare post-outage node status with pre-outage
        # ------------------------------------------------------------------
        self.logger.info("Comparing pre/post node statuses ...")
        storage_nodes_resp = self.sbcli_utils.get_storage_nodes()
        for sn in storage_nodes_resp["results"]:
            node_id = sn["uuid"]
            pre = pre_node_status.get(node_id, "unknown")
            post = sn["status"]
            self.logger.info(f"  Node {node_id[:8]}: pre={pre} post={post}")
            assert post == "online", (
                f"Storage node {node_id} is '{post}' after mgmt outage "
                f"(expected 'online')"
            )

        self.logger.info("=== MgmtNodeNetworkOutageTest: PASSED ===")
