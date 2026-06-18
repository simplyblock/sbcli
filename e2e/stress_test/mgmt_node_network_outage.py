"""Management Node Network Outage Test (Docker mode).

Stop the management node's network for 1 hour while IO is active on all
storage nodes.  Ensure no storage node failures and no IO failures.

Creates lvols with all security/filesystem combinations on every node:
  plain, crypto, auth (DHCHAP), crypto_auth  x  ext4, xfs

Self-restoring iptables approach:
    A nohup background script on the mgmt node blocks all traffic for
    ``outage_duration`` seconds, then automatically flushes the rules.
    This guarantees recovery even if the test runner crashes.
"""

import itertools
import threading
import random

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec


# Security type configurations
_SEC_CONFIGS = [
    {"label": "plain",       "crypto": False, "needs_nqn": False},
    {"label": "crypto",      "crypto": True,  "needs_nqn": False},
    {"label": "auth",        "crypto": False, "needs_nqn": True},
    {"label": "crypto_auth", "crypto": True,  "needs_nqn": True},
]
_FS_TYPES = ["ext4", "xfs"]


class MgmtNodeNetworkOutageTest(TestClusterBase):
    """Block mgmt-node network for 1 hour with active FIO on every storage node.

    Lvol matrix (per node): plain/crypto/auth/crypto_auth x ext4/xfs = 8 lvols.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mgmt_node_network_outage"
        self.mount_base = "/mnt"
        # 1 hour outage
        self.outage_duration = 3600
        # FIO runtime: 2 hours (covers outage + recovery validation)
        self.fio_runtime = 7200
        self.fio_num_jobs = 2
        self.lvol_size = "20G"

    def run(self):
        fio_threads = []
        fio_errors = []  # Collect exceptions from FIO threads
        lvol_details = {}
        self.logger.info("=== MgmtNodeNetworkOutageTest: starting ===")

        # ------------------------------------------------------------------
        # Step 1: Create pools — plain + DHCHAP
        # ------------------------------------------------------------------
        self.sbcli_utils.add_storage_pool(self.pool_name)

        dhchap_pool = f"dhchap_{self.pool_name}"
        self.logger.info(f"Creating DHCHAP pool: {dhchap_pool}")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], dhchap_pool,
            self.cluster_id, dhchap=True,
        )

        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        fio_node = random.choice(fio_nodes)

        # Register client NQN on DHCHAP pool
        host_nqn = self.ssh_obj.get_client_host_nqn(fio_node)
        dhchap_pool_id = self.sbcli_utils.get_storage_pool_id(dhchap_pool)
        if dhchap_pool_id and host_nqn:
            self.logger.info(
                f"Registering host NQN on DHCHAP pool: nqn={host_nqn}")
            self.ssh_obj.add_host_to_pool(
                self.mgmt_nodes[0], dhchap_pool_id, host_nqn)

        # ------------------------------------------------------------------
        # Step 2: Get primary storage node UUIDs
        # ------------------------------------------------------------------
        sn_resp = self.sbcli_utils.get_storage_nodes()
        node_uuids = [
            sn["uuid"] for sn in sn_resp["results"]
            if not sn.get("is_secondary_node")
        ]
        self.logger.info(
            f"Primary storage nodes ({len(node_uuids)}): "
            f"{[u[:8] for u in node_uuids]}")

        # ------------------------------------------------------------------
        # Step 3: Create lvols — every (sec_type x fs_type) on every node
        # ------------------------------------------------------------------
        combos = list(itertools.product(_SEC_CONFIGS, _FS_TYPES))
        total = len(combos) * len(node_uuids)
        self.logger.info(
            f"Creating {len(combos)} combos x {len(node_uuids)} nodes "
            f"= {total} lvols")

        idx = 0
        for node_uuid in node_uuids:
            for sec_cfg, fs_type in combos:
                label = sec_cfg["label"]
                prefix = {"plain": "pl", "crypto": "cr",
                          "auth": "au", "crypto_auth": "ca"}[label]
                lvol_name = (
                    f"mo_{prefix}_{fs_type}_"
                    f"{generate_random_sequence(4)}_{idx}"
                )
                pool = dhchap_pool if sec_cfg["needs_nqn"] else self.pool_name

                self.logger.info(
                    f"  [{idx + 1}/{total}] {label}/{fs_type} -> "
                    f"node {node_uuid[:8]}, pool={pool}")

                self.sbcli_utils.add_lvol(
                    lvol_name, pool,
                    size=self.lvol_size,
                    host_id=node_uuid,
                    crypto=sec_cfg["crypto"],
                )

                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)

                # Connect — DHCHAP volumes need host_nqn
                if sec_cfg["needs_nqn"] and host_nqn:
                    connect_cmds, _ = (
                        self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                            self.mgmt_nodes[0], lvol_id, host_nqn))
                else:
                    connect_cmds = self.sbcli_utils.get_lvol_connect_str(
                        lvol_name=lvol_name)

                for cmd in connect_cmds:
                    self.ssh_obj.exec_command(fio_node, cmd)

                device = self.ssh_obj.get_lvol_vs_device(fio_node, lvol_id)
                mount_path = f"{self.mount_base}/{lvol_name}"
                log_path = f"{self.log_path}/{lvol_name}.log"

                self.ssh_obj.format_disk(fio_node, device, fs_type=fs_type)
                self.ssh_obj.mount_path(fio_node, device, mount_path)

                lvol_details[lvol_name] = {
                    "ID": lvol_id,
                    "Mount": mount_path,
                    "Log": log_path,
                    "Device": device,
                    "sec_type": label,
                    "fs_type": fs_type,
                    "node": node_uuid[:8],
                }
                idx += 1

        self.logger.info(f"All {len(lvol_details)} lvols created and mounted")

        # ------------------------------------------------------------------
        # Step 4: Start FIO on every volume (time_based, runtime=7200)
        # ------------------------------------------------------------------
        self.logger.info(f"Starting FIO on {len(lvol_details)} volumes "
                         f"(runtime={self.fio_runtime}s, numjobs={self.fio_num_jobs})")

        for lvol_name, detail in lvol_details.items():
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(fio_node, None, detail["Mount"], detail["Log"]),
                kwargs={
                    "size": "5G",
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": self.fio_num_jobs,
                    "time_based": True,
                    "runtime": self.fio_runtime,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)
            sleep_n_sec(10)

        # Let FIO warm up before triggering the outage
        self.logger.info("Waiting 60s for FIO to stabilise before outage ...")
        sleep_n_sec(60)

        # ------------------------------------------------------------------
        # Step 5: Record pre-outage state
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
        # Step 6: Block mgmt node network (self-restoring iptables)
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
        # Step 7: Wait for outage duration + SPDK health checks
        # ------------------------------------------------------------------
        # Log a heartbeat every 5 minutes so the runner knows we are alive.
        # At each heartbeat, verify SPDK containers and processes on every
        # storage node — they must stay running even while mgmt is down.
        elapsed = 0
        heartbeat_interval = 300
        spdk_failures = []
        while elapsed < self.outage_duration:
            chunk = min(heartbeat_interval, self.outage_duration - elapsed)
            sleep_n_sec(chunk)
            elapsed += chunk
            self.logger.info(f"  Outage heartbeat: {elapsed}/{self.outage_duration}s elapsed")

            # Check SPDK on every storage node
            for sn_ip in self.storage_nodes:
                try:
                    containers = self.ssh_obj.get_running_containers(sn_ip)
                    spdk_containers = [c for c in containers if "spdk" in c.lower()]
                    if not spdk_containers:
                        msg = (f"No SPDK containers running on {sn_ip} "
                               f"at {elapsed}s into outage")
                        self.logger.error(msg)
                        spdk_failures.append(msg)
                    else:
                        self.logger.info(
                            f"  {sn_ip}: SPDK containers OK — "
                            f"{spdk_containers}")
                except Exception as e:
                    msg = (f"Cannot reach storage node {sn_ip} "
                           f"at {elapsed}s into outage: {e}")
                    self.logger.error(msg)
                    spdk_failures.append(msg)

        if spdk_failures:
            self.logger.error(
                f"SPDK health check failures during outage:\n"
                + "\n".join(spdk_failures))
            raise RuntimeError(
                f"{len(spdk_failures)} SPDK health check failure(s) "
                f"during mgmt outage: {spdk_failures[0]}")

        # ------------------------------------------------------------------
        # Step 8: Wait for mgmt node to recover (iptables auto-flushed)
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
        # Step 9: Verify cluster and storage node health
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
        # Step 10: Wait for FIO completion
        # ------------------------------------------------------------------
        self.logger.info("Waiting for all FIO threads to complete ...")
        self.common_utils.manage_fio_threads(fio_node, fio_threads, timeout=self.fio_runtime + 1800)
        self.logger.info("All FIO threads completed.")

        # Check for thread-level FIO errors
        if fio_errors:
            failed = [f"{name}: {exc}" for name, exc in fio_errors]
            self.logger.error(
                f"{len(fio_errors)}/{len(lvol_details)} FIO threads failed:\n"
                + "\n".join(failed))
            raise RuntimeError(
                f"{len(fio_errors)} FIO thread(s) failed: "
                + "; ".join(f"{n}: {e}" for n, e in fio_errors))

        # ------------------------------------------------------------------
        # Step 11: Validate FIO logs — no IO errors
        # ------------------------------------------------------------------
        self.logger.info("Validating FIO logs for every volume ...")
        for lvol_name, detail in lvol_details.items():
            self.logger.info(
                f"  Validating {lvol_name} "
                f"({detail['sec_type']}/{detail['fs_type']} "
                f"on node {detail['node']}) ...")
            log_content = self.ssh_obj.read_file(fio_node, detail["Log"])
            if not log_content or not log_content.strip():
                raise RuntimeError(
                    f"FIO log missing or empty for {lvol_name}: "
                    f"{detail['Log']}")
            self.common_utils.validate_fio_test(
                node=fio_node, log_file=detail["Log"]
            )
        self.logger.info("All FIO logs validated — zero IO errors.")

        # ------------------------------------------------------------------
        # Step 12: Compare post-outage node status with pre-outage
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


class MgmtNodeRebootTest(TestClusterBase):
    """Reboot the management node while FIO is active on every storage node.

    Same lvol security/filesystem matrix as MgmtNodeNetworkOutageTest but
    instead of blocking network traffic the mgmt node is rebooted via
    ``sudo reboot``.  Validates that storage nodes and IO are unaffected.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mgmt_node_reboot"
        self.mount_base = "/mnt"
        # FIO runtime: 1 hour (covers reboot + recovery + validation)
        self.fio_runtime = 3600
        self.fio_num_jobs = 2
        self.lvol_size = "20G"

    def run(self):
        fio_threads = []
        fio_errors = []
        lvol_details = {}
        self.logger.info("=== MgmtNodeRebootTest: starting ===")

        # ------------------------------------------------------------------
        # Step 1: Create pools — plain + DHCHAP
        # ------------------------------------------------------------------
        self.sbcli_utils.add_storage_pool(self.pool_name)

        dhchap_pool = f"dhchap_{self.pool_name}"
        self.logger.info(f"Creating DHCHAP pool: {dhchap_pool}")
        self.ssh_obj.add_storage_pool(
            self.mgmt_nodes[0], dhchap_pool,
            self.cluster_id, dhchap=True,
        )

        fio_nodes = self.fio_node if isinstance(self.fio_node, list) else [self.fio_node]
        fio_node = random.choice(fio_nodes)

        # Register client NQN on DHCHAP pool
        host_nqn = self.ssh_obj.get_client_host_nqn(fio_node)
        dhchap_pool_id = self.sbcli_utils.get_storage_pool_id(dhchap_pool)
        if dhchap_pool_id and host_nqn:
            self.logger.info(
                f"Registering host NQN on DHCHAP pool: nqn={host_nqn}")
            self.ssh_obj.add_host_to_pool(
                self.mgmt_nodes[0], dhchap_pool_id, host_nqn)

        # ------------------------------------------------------------------
        # Step 2: Get primary storage node UUIDs
        # ------------------------------------------------------------------
        sn_resp = self.sbcli_utils.get_storage_nodes()
        node_uuids = [
            sn["uuid"] for sn in sn_resp["results"]
            if not sn.get("is_secondary_node")
        ]
        self.logger.info(
            f"Primary storage nodes ({len(node_uuids)}): "
            f"{[u[:8] for u in node_uuids]}")

        # ------------------------------------------------------------------
        # Step 3: Create lvols — every (sec_type x fs_type) on every node
        # ------------------------------------------------------------------
        combos = list(itertools.product(_SEC_CONFIGS, _FS_TYPES))
        total = len(combos) * len(node_uuids)
        self.logger.info(
            f"Creating {len(combos)} combos x {len(node_uuids)} nodes "
            f"= {total} lvols")

        idx = 0
        for node_uuid in node_uuids:
            for sec_cfg, fs_type in combos:
                label = sec_cfg["label"]
                prefix = {"plain": "pl", "crypto": "cr",
                          "auth": "au", "crypto_auth": "ca"}[label]
                lvol_name = (
                    f"mr_{prefix}_{fs_type}_"
                    f"{generate_random_sequence(4)}_{idx}"
                )
                pool = dhchap_pool if sec_cfg["needs_nqn"] else self.pool_name

                self.logger.info(
                    f"  [{idx + 1}/{total}] {label}/{fs_type} -> "
                    f"node {node_uuid[:8]}, pool={pool}")

                self.sbcli_utils.add_lvol(
                    lvol_name, pool,
                    size=self.lvol_size,
                    host_id=node_uuid,
                    crypto=sec_cfg["crypto"],
                )

                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)

                # Connect — DHCHAP volumes need host_nqn
                if sec_cfg["needs_nqn"] and host_nqn:
                    connect_cmds, _ = (
                        self.ssh_obj.get_lvol_connect_str_with_host_nqn(
                            self.mgmt_nodes[0], lvol_id, host_nqn))
                else:
                    connect_cmds = self.sbcli_utils.get_lvol_connect_str(
                        lvol_name=lvol_name)

                for cmd in connect_cmds:
                    self.ssh_obj.exec_command(fio_node, cmd)

                device = self.ssh_obj.get_lvol_vs_device(fio_node, lvol_id)
                mount_path = f"{self.mount_base}/{lvol_name}"
                log_path = f"{self.log_path}/{lvol_name}.log"

                self.ssh_obj.format_disk(fio_node, device, fs_type=fs_type)
                self.ssh_obj.mount_path(fio_node, device, mount_path)

                lvol_details[lvol_name] = {
                    "ID": lvol_id,
                    "Mount": mount_path,
                    "Log": log_path,
                    "Device": device,
                    "sec_type": label,
                    "fs_type": fs_type,
                    "node": node_uuid[:8],
                }
                idx += 1

        self.logger.info(f"All {len(lvol_details)} lvols created and mounted")

        # ------------------------------------------------------------------
        # Step 4: Start FIO on every volume
        # ------------------------------------------------------------------
        self.logger.info(f"Starting FIO on {len(lvol_details)} volumes "
                         f"(runtime={self.fio_runtime}s, numjobs={self.fio_num_jobs})")

        for lvol_name, detail in lvol_details.items():
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(fio_node, None, detail["Mount"], detail["Log"]),
                kwargs={
                    "size": "5G",
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": self.fio_num_jobs,
                    "time_based": True,
                    "runtime": self.fio_runtime,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)
            sleep_n_sec(10)

        # Let FIO warm up before triggering the reboot
        self.logger.info("Waiting 60s for FIO to stabilise before reboot ...")
        sleep_n_sec(60)

        # ------------------------------------------------------------------
        # Step 5: Record pre-reboot state
        # ------------------------------------------------------------------
        self.logger.info("Recording pre-reboot cluster/node state ...")
        pre_cluster = self.sbcli_utils.get_cluster_details()
        self.logger.info(f"  Cluster status: {pre_cluster['status']}")

        storage_nodes_resp = self.sbcli_utils.get_storage_nodes()
        pre_node_status = {}
        for sn in storage_nodes_resp["results"]:
            pre_node_status[sn["uuid"]] = sn["status"]
            self.logger.info(f"  Storage node {sn['uuid'][:8]} — {sn['status']}")

        # ------------------------------------------------------------------
        # Step 6: Reboot the management node
        # ------------------------------------------------------------------
        mgmt_ip = self.mgmt_nodes[0]
        self.logger.info(f"Rebooting management node {mgmt_ip} ...")

        try:
            self.ssh_obj.exec_command(
                mgmt_ip,
                "nohup bash -c 'sleep 2 && sudo reboot' > /tmp/mgmt_reboot.log 2>&1 &",
            )
        except Exception as exc:
            # The SSH connection may break as the reboot starts — expected
            self.logger.info(f"Reboot command returned (possibly with error, expected): {exc}")

        self.logger.info("Reboot command sent — waiting 30s for node to go down ...")
        sleep_n_sec(30)

        # ------------------------------------------------------------------
        # Step 7: SPDK health checks while mgmt is rebooting
        # ------------------------------------------------------------------
        self.logger.info("Checking SPDK containers on storage nodes during reboot ...")
        spdk_failures = []
        # Check every 60s for up to 10 minutes
        for check_idx in range(10):
            sleep_n_sec(60)
            elapsed = (check_idx + 1) * 60
            for sn_ip in self.storage_nodes:
                try:
                    containers = self.ssh_obj.get_running_containers(sn_ip)
                    spdk_containers = [c for c in containers if "spdk" in c.lower()]
                    if not spdk_containers:
                        msg = (f"No SPDK containers running on {sn_ip} "
                               f"at {elapsed}s after reboot")
                        self.logger.error(msg)
                        spdk_failures.append(msg)
                    else:
                        self.logger.info(
                            f"  {sn_ip}: SPDK containers OK — "
                            f"{spdk_containers}")
                except Exception as e:
                    msg = (f"Cannot reach storage node {sn_ip} "
                           f"at {elapsed}s after reboot: {e}")
                    self.logger.error(msg)
                    spdk_failures.append(msg)

            # Check if mgmt is already back (can exit early)
            try:
                containers = self.ssh_obj.get_running_containers(mgmt_ip)
                if any("app_WebAppAPI" in c for c in containers):
                    self.logger.info(
                        f"Mgmt node is back after {elapsed}s — "
                        f"stopping SPDK checks")
                    break
            except Exception:
                self.logger.info(f"  Mgmt still rebooting ({elapsed}s) ...")

        if spdk_failures:
            self.logger.error(
                f"SPDK health check failures during reboot:\n"
                + "\n".join(spdk_failures))
            raise RuntimeError(
                f"{len(spdk_failures)} SPDK health check failure(s) "
                f"during mgmt reboot: {spdk_failures[0]}")

        # ------------------------------------------------------------------
        # Step 8: Wait for mgmt node to come back online
        # ------------------------------------------------------------------
        self.logger.info("Waiting for mgmt node SSH to come back ...")

        # Re-establish SSH connection (old one is dead after reboot)
        ssh_back = False
        for attempt in range(60):
            try:
                self.ssh_obj.connect(
                    address=mgmt_ip,
                    bastion_server_address=self.bastion_server,
                )
                self.logger.info(f"SSH reconnected to mgmt node (attempt {attempt + 1})")
                ssh_back = True
                break
            except Exception as e:
                if attempt % 6 == 0:
                    self.logger.info(
                        f"  Attempt {attempt + 1}/60 — SSH not ready: {e}")
            sleep_n_sec(10)

        if not ssh_back:
            raise TimeoutError(
                "Could not re-establish SSH to mgmt node within 10 minutes")

        # Wait for WebAppAPI container
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
                self.logger.info(
                    f"  Attempt {attempt + 1}/60 — mgmt not ready: {e}")
            sleep_n_sec(10)

        if not webapp_up:
            raise TimeoutError(
                "app_WebAppAPI did not come online within 10 minutes "
                "after reboot")

        # Extra settle time for control plane services
        self.logger.info("WebAppAPI online — waiting 120s for full control plane recovery ...")
        sleep_n_sec(120)

        # ------------------------------------------------------------------
        # Step 9: Verify cluster and storage node health
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
        # Step 10: Wait for FIO completion
        # ------------------------------------------------------------------
        self.logger.info("Waiting for all FIO threads to complete ...")
        self.common_utils.manage_fio_threads(fio_node, fio_threads, timeout=self.fio_runtime + 1800)
        self.logger.info("All FIO threads completed.")

        if fio_errors:
            failed = [f"{name}: {exc}" for name, exc in fio_errors]
            self.logger.error(
                f"{len(fio_errors)}/{len(lvol_details)} FIO threads failed:\n"
                + "\n".join(failed))
            raise RuntimeError(
                f"{len(fio_errors)} FIO thread(s) failed: "
                + "; ".join(f"{n}: {e}" for n, e in fio_errors))

        # ------------------------------------------------------------------
        # Step 11: Validate FIO logs — no IO errors
        # ------------------------------------------------------------------
        self.logger.info("Validating FIO logs for every volume ...")
        for lvol_name, detail in lvol_details.items():
            self.logger.info(
                f"  Validating {lvol_name} "
                f"({detail['sec_type']}/{detail['fs_type']} "
                f"on node {detail['node']}) ...")
            log_content = self.ssh_obj.read_file(fio_node, detail["Log"])
            if not log_content or not log_content.strip():
                raise RuntimeError(
                    f"FIO log missing or empty for {lvol_name}: "
                    f"{detail['Log']}")
            self.common_utils.validate_fio_test(
                node=fio_node, log_file=detail["Log"]
            )
        self.logger.info("All FIO logs validated — zero IO errors.")

        # ------------------------------------------------------------------
        # Step 12: Compare post-reboot node status with pre-reboot
        # ------------------------------------------------------------------
        self.logger.info("Comparing pre/post node statuses ...")
        storage_nodes_resp = self.sbcli_utils.get_storage_nodes()
        for sn in storage_nodes_resp["results"]:
            node_id = sn["uuid"]
            pre = pre_node_status.get(node_id, "unknown")
            post = sn["status"]
            self.logger.info(f"  Node {node_id[:8]}: pre={pre} post={post}")
            assert post == "online", (
                f"Storage node {node_id} is '{post}' after mgmt reboot "
                f"(expected 'online')"
            )

        self.logger.info("=== MgmtNodeRebootTest: PASSED ===")
