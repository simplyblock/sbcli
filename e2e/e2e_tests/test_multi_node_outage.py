"""E2E Multi-Node Outage Test with Data Integrity Verification.

Tests cluster resilience when 3 out of 4 storage nodes experience
simultaneous outage (random mix of SPDK crash and network disconnect).

Flow:
  1. Create 3 lvols per storage node, run FIO on all.
  2. Wait for 1 FIO per node to complete (short write), keep 2 running.
  3. Compute md5sum on completed lvols, take pre-outage snapshots+clones.
  4. Trigger simultaneous outage on 3 random nodes for ~3 minutes.
  5. Wait for recovery: all nodes online, cluster Active.
  6. Verify md5sum on completed lvols (data integrity).
  7. Create 1 new lvol per node + run FIO (basic functionality).
  8. Take post-outage snapshots+clones (snapshot/clone functionality).

Two variants:
  - TestMultiNodeOutageDocker: SSH-based (k8s_run=False)
  - TestMultiNodeOutageK8s: K8s sbcli via kubectl (k8s_run=True)
"""

import os
import random
import threading
import time

from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


class _TestMultiNodeOutageBase(TestClusterBase):
    """Shared logic for Docker and K8s multi-node outage tests."""

    def __init__(self, k8s_run=False, **kwargs):
        super().__init__(k8s_run=k8s_run, **kwargs)
        self.logger = setup_logger(__name__)

        # Test parameters
        self.lvol_size = "5G"
        self.fio_size = "1G"
        self.short_fio_runtime = 120    # seconds — short FIO should complete well within this
        self.long_fio_runtime = 1000     # seconds — long FIO runs during outage
        self.outage_duration = 180      # 3 minutes
        self.num_lvols_per_node = 3
        self.num_outage_nodes = 3

        # Internal state
        self._node_info = {}        # node_uuid -> {ip, rpc_port, data_nics, if_names}
        self._lvol_info = {}        # lvol_name -> {node_uuid, device, mount_path, fio_name}
        self._completed_lvols = []  # lvol names where short FIO completed
        self._running_lvols = []    # lvol names where long FIO is still running
        self._pre_checksums = {}    # lvol_name -> {filepath: md5}
        self._outage_plan = {}      # node_uuid -> "spdk_crash" | "network_outage"
        self._outage_threads = []

    # ── Snapshot/clone helpers (branched by k8s_test) ────────────────

    def _create_snapshot(self, lvol_id, snap_name):
        if self.k8s_test:
            self.sbcli_utils.add_snapshot(lvol_id=lvol_id, snapshot_name=snap_name)
        else:
            self.ssh_obj.add_snapshot(
                node=self.mgmt_nodes[0], lvol_id=lvol_id, snapshot_name=snap_name
            )

    def _get_snapshot_id(self, snap_name):
        if self.k8s_test:
            return self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
        else:
            return self.ssh_obj.get_snapshot_id(
                node=self.mgmt_nodes[0], snapshot_name=snap_name
            )

    def _create_clone(self, snap_id, clone_name):
        if self.k8s_test:
            self.sbcli_utils.add_clone(snapshot_id=snap_id, clone_name=clone_name)
        else:
            self.ssh_obj.add_clone(
                node=self.mgmt_nodes[0], snapshot_id=snap_id, clone_name=clone_name
            )

    # ── SPDK crash helper (branched by k8s_test) ────────────────────

    def _trigger_spdk_crash(self, node_uuid, node_ip, rpc_port):
        if self.k8s_test:
            k8s = getattr(self.sbcli_utils, "k8s", None)
            if k8s:
                k8s.stop_spdk_pod(node_ip)
            else:
                self.logger.warning(
                    f"k8s_utils not available — falling back to SSH spdk_process_kill"
                )
                self.ssh_obj.stop_spdk_process(node_ip, rpc_port, self.cluster_id)
        else:
            self.ssh_obj.stop_spdk_process(node_ip, rpc_port, self.cluster_id)

    # ── NVMe connect/reconnect helpers ──────────────────────────────

    def _connect_lvol(self, client, lvol_name):
        """Run NVMe connect commands for a lvol on the given client."""
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        if not connect_ls:
            raise RuntimeError(f"No connect strings for lvol {lvol_name}")
        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=client, command=connect_str)

    def _detect_new_device(self, client, initial_devices):
        """Return the first new device that appeared since initial_devices."""
        final_devices = self.ssh_obj.get_devices(node=client)
        for device in final_devices:
            if device not in initial_devices:
                return f"/dev/{device.strip()}"
        return None

    def _reconnect_lvol(self, client, lvol_name, mount_path):
        """Reconnect NVMe, detect device, mount without format. Returns device path."""
        # Unmount if still mounted (may fail — that's ok)
        self.ssh_obj.exec_command(
            node=client, command=f"sudo umount {mount_path} 2>/dev/null || true"
        )

        # Disconnect existing NVMe paths for this lvol
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
        if lvol_id:
            subsystems = self.ssh_obj.get_nvme_subsystems(node=client, nqn_filter=lvol_id)
            for subsys in subsystems:
                self.ssh_obj.disconnect_nvme(node=client, nqn_grep=subsys)
        sleep_n_sec(3)

        # Re-connect NVMe
        initial_devices = self.ssh_obj.get_devices(node=client)
        self._connect_lvol(client, lvol_name)
        sleep_n_sec(5)

        device = self._detect_new_device(client, initial_devices)
        if not device:
            # Device might have reconnected with same name — try the old device
            old_device = self._lvol_info.get(lvol_name, {}).get("device")
            if old_device:
                self.logger.info(
                    f"No new device detected for {lvol_name}, trying old device {old_device}"
                )
                device = old_device
            else:
                raise RuntimeError(f"Could not detect device for {lvol_name} after reconnect")

        # Mount (no format — data must be preserved)
        self.ssh_obj.exec_command(
            node=client, command=f"sudo mkdir -p {mount_path}"
        )
        self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_path)
        return device

    # ── FIO wait helper ─────────────────────────────────────────────

    def _wait_fio_complete(self, client, fio_name, timeout=300):
        """Poll until the tmux session for this FIO exits."""
        deadline = time.time() + timeout
        session = f"fio_{fio_name}"
        while time.time() < deadline:
            out, _ = self.ssh_obj.exec_command(
                node=client,
                command=f"sudo tmux has-session -t {session} 2>&1 && echo RUNNING || echo DONE",
                max_retries=1,
            )
            if "DONE" in out:
                self.logger.info(f"FIO session '{session}' completed on {client}")
                return True
            sleep_n_sec(10)
        self.logger.warning(f"FIO session '{session}' did not complete within {timeout}s")
        return False

    def _kill_fio_session(self, client, fio_name):
        """Kill a tmux FIO session if still running."""
        session = f"fio_{fio_name}"
        self.ssh_obj.exec_command(
            node=client,
            command=f"sudo tmux kill-session -t {session} 2>/dev/null || true",
            max_retries=1,
        )

    # ── Main test flow ──────────────────────────────────────────────

    def run(self):
        self.logger.info("=" * 70)
        self.logger.info("Starting Multi-Node Outage E2E Test")
        self.logger.info("=" * 70)

        client = self.fio_node[0]

        # K8s mode: establish SSH to storage nodes (needed for network outage)
        if self.k8s_test:
            for node in self.storage_nodes:
                self.logger.info(f"[setup] SSH-connecting to storage node {node}")
                self.ssh_obj.connect(
                    address=node, bastion_server_address=self.bastion_server
                )
                sleep_n_sec(1)

        # ── Step 1: Discover storage nodes ──────────────────────────
        self.logger.info("[step-1] Discovering storage nodes")
        storage_nodes_data = self.sbcli_utils.get_storage_nodes()
        node_uuids = []
        for result in storage_nodes_data["results"]:
            if not result.get("is_secondary_node", False):
                uuid = result["uuid"]
                node_uuids.append(uuid)
                self._node_info[uuid] = {
                    "ip": result["mgmt_ip"],
                    "rpc_port": result.get("rpc_port", ""),
                    "data_nics": result.get("data_nics", []),
                    "if_names": [
                        nic["if_name"]
                        for nic in result.get("data_nics", [])
                        if nic.get("if_name")
                    ],
                }

        num_nodes = len(node_uuids)
        self.logger.info(f"[step-1] Found {num_nodes} primary storage nodes: {node_uuids}")
        assert num_nodes >= 4, (
            f"Need at least 4 storage nodes for this test, found {num_nodes}"
        )

        # ── Step 2: Create pool ─────────────────────────────────────
        self.logger.info("[step-2] Creating storage pool")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name in pools, f"Pool {self.pool_name} not created"
        sleep_n_sec(5)

        # ── Step 3: Create 3 lvols per node ─────────────────────────
        self.logger.info("[step-3] Creating lvols")
        node_lvol_names = {}  # uuid -> [lvol_name, ...]
        for node_uuid in node_uuids:
            short_id = node_uuid[:6]
            node_lvol_names[node_uuid] = []
            for i in range(self.num_lvols_per_node):
                lvol_name = f"mno-{short_id}-{i}"
                self.logger.info(
                    f"  Creating lvol {lvol_name} on node {node_uuid} ({self._node_info[node_uuid]['ip']})"
                )
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.lvol_size,
                    host_id=node_uuid,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                )
                node_lvol_names[node_uuid].append(lvol_name)
                self._lvol_info[lvol_name] = {
                    "node_uuid": node_uuid,
                    "device": None,
                    "mount_path": f"/mnt/mno_{lvol_name}",
                    "fio_name": None,
                }

        total_lvols = sum(len(v) for v in node_lvol_names.values())
        self.logger.info(f"[step-3] Created {total_lvols} lvols across {num_nodes} nodes")

        # ── Step 4: Connect, format, mount all lvols ────────────────
        self.logger.info("[step-4] Connecting, formatting, and mounting all lvols")
        for lvol_name, info in self._lvol_info.items():
            initial_devices = self.ssh_obj.get_devices(node=client)
            self._connect_lvol(client, lvol_name)
            sleep_n_sec(3)

            device = self._detect_new_device(client, initial_devices)
            if not device:
                raise RuntimeError(f"No new device detected after connecting {lvol_name}")

            info["device"] = device
            mount_path = info["mount_path"]

            self.ssh_obj.unmount_path(node=client, device=device)
            self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
            self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_path)
            self.logger.info(f"  {lvol_name}: {device} → {mount_path}")

        # ── Step 5: Run short FIO (1 per node) and wait ─────────────
        self.logger.info("[step-5] Running short FIO on 1 lvol per node (write 1G)")
        for node_uuid in node_uuids:
            lvol_name = node_lvol_names[node_uuid][0]  # first lvol per node
            info = self._lvol_info[lvol_name]
            fio_name = f"short_{lvol_name}"
            info["fio_name"] = fio_name

            self.ssh_obj.run_fio_test(
                node=client,
                directory=info["mount_path"],
                log_file=os.path.join(self.log_path, f"{fio_name}.log"),
                name=fio_name,
                rw="write",
                bs="1M",
                size=self.fio_size,
                numjobs=1,
                nrfiles=4,
                runtime=self.short_fio_runtime,
                time_based=False,
                use_latency=False,
            )
            self._completed_lvols.append(lvol_name)

        # Wait for all short FIOs to complete
        self.logger.info("[step-5] Waiting for short FIOs to complete")
        for lvol_name in self._completed_lvols:
            fio_name = self._lvol_info[lvol_name]["fio_name"]
            ok = self._wait_fio_complete(client, fio_name, timeout=self.short_fio_runtime + 120)
            if not ok:
                self.logger.warning(f"Short FIO {fio_name} may not have completed cleanly")

        sleep_n_sec(5)

        # ── Step 6: Compute pre-outage md5sum on completed lvols ────
        self.logger.info("[step-6] Computing pre-outage md5sum checksums")
        for lvol_name in self._completed_lvols:
            mount_path = self._lvol_info[lvol_name]["mount_path"]
            files = self.ssh_obj.find_files(client, directory=mount_path)
            if not files or files == [""]:
                self.logger.warning(f"No files found in {mount_path} for {lvol_name}")
                continue
            checksums = self.ssh_obj.generate_checksums(client, files)
            self._pre_checksums[lvol_name] = checksums
            self.logger.info(
                f"  {lvol_name}: {len(checksums)} files checksummed"
            )

        assert self._pre_checksums, "No pre-outage checksums computed — aborting"

        # ── Step 7: Pre-outage snapshots + clones ───────────────────
        self.logger.info("[step-7] Creating pre-outage snapshots and clones")
        for lvol_name in self._completed_lvols:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if not lvol_id:
                self.logger.warning(f"Cannot find lvol_id for {lvol_name} — skipping snapshot")
                continue

            snap_name = f"{lvol_name}_snap_pre"
            clone_name = f"{lvol_name}_clone_pre"
            self.logger.info(f"  Snapshot: {snap_name}, Clone: {clone_name}")

            self._create_snapshot(lvol_id, snap_name)
            snap_id = self._get_snapshot_id(snap_name)
            if snap_id:
                self._create_clone(snap_id, clone_name)
            else:
                self.logger.warning(f"Could not get snapshot ID for {snap_name}")

        # ── Step 8: Start long FIO on remaining 2 lvols per node ────
        self.logger.info("[step-8] Starting long FIO on remaining lvols")
        for node_uuid in node_uuids:
            for lvol_name in node_lvol_names[node_uuid][1:]:  # lvols 1 and 2
                info = self._lvol_info[lvol_name]
                fio_name = f"long_{lvol_name}"
                info["fio_name"] = fio_name

                self.ssh_obj.run_fio_test(
                    node=client,
                    directory=info["mount_path"],
                    log_file=os.path.join(self.log_path, f"{fio_name}.log"),
                    name=fio_name,
                    rw="randrw",
                    bs="4K",
                    size=self.fio_size,
                    numjobs=4,
                    iodepth=16,
                    runtime=self.long_fio_runtime,
                    time_based=True,
                    rwmixread=70,
                )
                self._running_lvols.append(lvol_name)

        self.logger.info(f"[step-8] {len(self._running_lvols)} long FIOs started")
        sleep_n_sec(30)  # let FIOs establish

        # ── Step 9: Plan and execute multi-node outage ──────────────
        self.logger.info("[step-9] Planning multi-node outage")
        outage_nodes = random.sample(node_uuids, self.num_outage_nodes)
        for node_uuid in outage_nodes:
            outage_type = random.choice(["spdk_crash", "network_outage"])
            self._outage_plan[node_uuid] = outage_type

        self.logger.info("[step-9] Outage plan:")
        for node_uuid, otype in self._outage_plan.items():
            ip = self._node_info[node_uuid]["ip"]
            self.logger.info(f"  Node {node_uuid[:8]} ({ip}): {otype}")

        # Collect pre-outage diagnostics
        self.logger.info("[step-9] Collecting pre-outage diagnostics")
        try:
            self.collect_management_details(suffix="_pre_outage")
        except Exception as e:
            self.logger.warning(f"Pre-outage diagnostics failed: {e}")

        # Execute outages simultaneously
        self.logger.info("[step-9] TRIGGERING OUTAGES ON 3 NODES")
        self._outage_threads = []
        for node_uuid, outage_type in self._outage_plan.items():
            ninfo = self._node_info[node_uuid]
            node_ip = ninfo["ip"]

            if outage_type == "spdk_crash":
                t = threading.Thread(
                    target=self._trigger_spdk_crash,
                    args=(node_uuid, node_ip, ninfo["rpc_port"]),
                    daemon=True,
                )
            else:  # network_outage
                if_names = ninfo["if_names"]
                if not if_names:
                    self.logger.warning(
                        f"No interface names for {node_uuid} — falling back to get_active_interfaces"
                    )
                    if_names = self.ssh_obj.get_active_interfaces(node_ip)
                t = threading.Thread(
                    target=self.ssh_obj.disconnect_all_active_interfaces,
                    args=(node_ip, if_names, self.outage_duration),
                    daemon=True,
                )

            self._outage_threads.append(t)
            t.start()
            self.logger.info(f"  Outage thread started for {node_uuid[:8]} ({outage_type})")

        # ── Step 10: Wait for outage to pass ────────────────────────
        self.logger.info("[step-10] Waiting for cluster to become Suspended")
        try:
            self.sbcli_utils.wait_for_cluster_status(
                status=["suspended"], timeout=600
            )
            self.logger.info("[step-11] Cluster is Suspended")
        except TimeoutError:
            # Try accepting degraded as well
            self.logger.warning("Cluster did not reach Suspended — checking for degraded")
            cluster_status = self.sbcli_utils.get_cluster_status()
            self.logger.info(f"Current cluster status: {cluster_status}")


        wait_secs = self.outage_duration + 60  # extra buffer
        self.logger.info(f"[step-10] Waiting {wait_secs}s for outage period to pass")
        sleep_n_sec(wait_secs)

        # Join outage threads (network disconnect threads block for duration)
        for t in self._outage_threads:
            t.join(timeout=120)

        # ── Step 11: Wait for recovery ──────────────────────────────
        self.logger.info("[step-11] Waiting for all nodes to come back online")
        for node_uuid in outage_nodes:
            try:
                self.sbcli_utils.wait_for_storage_node_status(
                    node_uuid, status=["online"], timeout=600
                )
                self.logger.info(f"  Node {node_uuid[:8]} is online")
            except TimeoutError:
                self.logger.error(f"  Node {node_uuid[:8]} did NOT come back online within 600s")
                raise

        self.logger.info("[step-11] Waiting for cluster to become Active")
        try:
            self.sbcli_utils.wait_for_cluster_status(
                status=["active"], timeout=600
            )
            self.logger.info("[step-11] Cluster is Active")
        except TimeoutError:
            # Try accepting degraded as well
            self.logger.warning("Cluster did not reach Active — checking for degraded")
            cluster_status = self.sbcli_utils.get_cluster_status()
            self.logger.info(f"Current cluster status: {cluster_status}")
            raise

        # Collect post-recovery diagnostics
        try:
            self.collect_management_details(suffix="_post_recovery")
        except Exception as e:
            self.logger.warning(f"Post-recovery diagnostics failed: {e}")

        sleep_n_sec(30)  # settle time after recovery

        # ── Step 12: Kill remaining long FIOs (they may have errored) ─
        self.logger.info("[step-12] Killing remaining long FIO sessions")
        for lvol_name in self._running_lvols:
            fio_name = self._lvol_info[lvol_name].get("fio_name")
            if fio_name:
                self._kill_fio_session(client, fio_name)

        sleep_n_sec(10)

        # ── Step 13: Verify md5sum on completed lvols ───────────────
        self.logger.info("[step-13] Verifying data integrity (md5sum) on completed lvols")
        checksum_failures = []
        for lvol_name in self._completed_lvols:
            if lvol_name not in self._pre_checksums:
                self.logger.warning(f"No pre-outage checksum for {lvol_name} — skipping")
                continue

            mount_path = self._lvol_info[lvol_name]["mount_path"]
            self.logger.info(f"  Reconnecting {lvol_name}")

            try:
                device = self._reconnect_lvol(client, lvol_name, mount_path)
                self._lvol_info[lvol_name]["device"] = device
            except Exception as e:
                self.logger.error(f"  Failed to reconnect {lvol_name}: {e}")
                checksum_failures.append(lvol_name)
                continue

            files = self.ssh_obj.find_files(client, directory=mount_path)
            if not files or files == [""]:
                self.logger.error(f"  No files found in {mount_path} after recovery")
                checksum_failures.append(lvol_name)
                continue

            post_checksums = self.ssh_obj.generate_checksums(client, files)
            pre_set = set(self._pre_checksums[lvol_name].values())
            post_set = set(post_checksums.values())

            if pre_set == post_set:
                self.logger.info(
                    f"  {lvol_name}: CHECKSUM OK ({len(post_checksums)} files verified)"
                )
            else:
                self.logger.error(
                    f"  {lvol_name}: CHECKSUM MISMATCH!\n"
                    f"    Pre:  {self._pre_checksums[lvol_name]}\n"
                    f"    Post: {post_checksums}"
                )
                checksum_failures.append(lvol_name)

        if checksum_failures:
            raise AssertionError(
                f"Data integrity check failed on {len(checksum_failures)} lvols: {checksum_failures}"
            )
        self.logger.info("[step-13] All checksum verifications passed")

        # ── Step 14: Create 1 new lvol per node + run FIO ───────────
        self.logger.info("[step-14] Creating new lvols post-recovery and running FIO")
        new_lvol_names = []
        for node_uuid in node_uuids:
            short_id = node_uuid[:6]
            new_name = f"mno-new-{short_id}"
            self.logger.info(
                f"  Creating {new_name} on node {node_uuid[:8]} ({self._node_info[node_uuid]['ip']})"
            )
            self.sbcli_utils.add_lvol(
                lvol_name=new_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                host_id=node_uuid,
                distr_ndcs=self.ndcs,
                distr_npcs=self.npcs,
                distr_bs=self.bs,
                distr_chunk_bs=self.chunk_bs,
            )

            # Connect, format, mount
            initial_devices = self.ssh_obj.get_devices(node=client)
            self._connect_lvol(client, new_name)
            sleep_n_sec(3)
            device = self._detect_new_device(client, initial_devices)
            if not device:
                raise RuntimeError(f"No new device for post-recovery lvol {new_name}")

            new_mount = f"/mnt/mno_{new_name}"
            self.ssh_obj.unmount_path(node=client, device=device)
            self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
            self.ssh_obj.mount_path(node=client, device=device, mount_path=new_mount)

            # Run short FIO
            fio_name = f"post_{new_name}"
            self.ssh_obj.run_fio_test(
                node=client,
                directory=new_mount,
                log_file=os.path.join(self.log_path, f"{fio_name}.log"),
                name=fio_name,
                rw="write",
                bs="1M",
                size=self.fio_size,
                numjobs=1,
                nrfiles=4,
                runtime=self.short_fio_runtime,
                time_based=False,
                use_latency=False,
            )
            new_lvol_names.append(new_name)
            self._lvol_info[new_name] = {
                "node_uuid": node_uuid,
                "device": device,
                "mount_path": new_mount,
                "fio_name": fio_name,
            }

        # Wait for new FIOs to complete
        self.logger.info("[step-14] Waiting for post-recovery FIOs to complete")
        for new_name in new_lvol_names:
            fio_name = self._lvol_info[new_name]["fio_name"]
            ok = self._wait_fio_complete(client, fio_name, timeout=self.short_fio_runtime + 120)
            assert ok, f"Post-recovery FIO {fio_name} did not complete"

        self.logger.info("[step-14] All post-recovery FIOs completed successfully")

        # ── Step 15: Post-outage snapshots + clones ─────────────────
        self.logger.info("[step-15] Creating post-outage snapshots and clones")
        for lvol_name in self._completed_lvols:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if not lvol_id:
                self.logger.warning(f"Cannot find lvol_id for {lvol_name} — skipping")
                continue

            snap_name = f"{lvol_name}_snap_post"
            clone_name = f"{lvol_name}_clone_post"
            self.logger.info(f"  Snapshot: {snap_name}, Clone: {clone_name}")

            self._create_snapshot(lvol_id, snap_name)
            snap_id = self._get_snapshot_id(snap_name)
            if snap_id:
                self._create_clone(snap_id, clone_name)
            else:
                self.logger.warning(f"Could not get snapshot ID for {snap_name}")

        self.logger.info("=" * 70)
        self.logger.info("Multi-Node Outage E2E Test PASSED")
        self.logger.info("=" * 70)


class TestMultiNodeOutageDocker(_TestMultiNodeOutageBase):
    """Docker SSH-based multi-node outage test."""

    def __init__(self, **kwargs):
        super().__init__(k8s_run=False, **kwargs)
        self.test_name = "multi_node_outage_docker"


class TestMultiNodeOutageK8s(_TestMultiNodeOutageBase):
    """K8s-based multi-node outage test (sbcli via kubectl exec)."""

    def __init__(self, **kwargs):
        super().__init__(k8s_run=True, **kwargs)
        self.test_name = "multi_node_outage_k8s"
