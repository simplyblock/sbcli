import random
import re
import threading
import time
from utils.common_utils import sleep_n_sec
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from logger_config import setup_logger
from datetime import datetime
from exceptions.custom_exception import LvolNotConnectException
from pathlib import Path


class TestLvolHACluster(FioWorkloadTest):
    """
    High-volume stress test for a 3-node cluster.
    Operations:
    - Create 500 lvols (mix of crypto and non-crypto) on a single node.
    - Create 3000 snapshots.
    - Fill volumes to about 9 TiB.
    - Run FIO for storage.
    - Handle graceful shutdown, container stop, and network stop.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "100G"
        self.int_lvol_size = 100
        self.fio_size = "8G"  # default; overridden by _compute_fio_size()
        self.fio_numjobs = 1
        self.TARGET_DATA_PER_NODE_GB = 200
        self.total_lvols = 10
        self.snapshot_per_lvol = 2
        self.lvol_name = "lvl"
        self.snapshot_name = "snapshot"
        self.node = None
        self.lvol_node = None
        self.mount_path = "/mnt/"
        self.lvol_mount_details = {}
        self.clone_mount_details = {}
        self.sn_nodes = []
        self.log_path = Path.home()
        self.dump_validation_errors = []
        # Deletes that could not complete during an outage, reconciled later by
        # validate_pending_deletions(). Lives here rather than on one subclass
        # so every failover test can defer a delete the same way.
        self.pending_deletions = {
            "lvols": dict(),
            "snapshots": dict()
        }

    def _pick_client(self, index=0):
        """Return a client node for NVMe connect / mount / FIO.

        Distributes across ``self.fio_node`` list round-robin using
        *index*.  Falls back to the management node when no dedicated
        client is available.
        """
        fio = getattr(self, "fio_node", None)
        if isinstance(fio, list) and fio:
            return fio[index % len(fio)]
        if fio:
            return fio
        return self.mgmt_nodes[0]

    def _is_namespaced_lvol(self, lvol_name):
        """True when this lvol shares its subsystem with another volume."""
        det = self.lvol_mount_details.get(lvol_name) or {}
        return bool(det.get("is_parent") or det.get("parent"))

    def _assert_subsystem_sharing(self):
        """Fail if no two volumes ended up sharing a subsystem.

        Without this the test passes identically whether namespacing worked or
        every lvol quietly got its own subsystem. Nothing else in the suite
        checks it.
        """
        groups = {}
        for name, det in self.lvol_mount_details.items():
            try:
                nqn = self.sbcli_utils.get_lvol_details(lvol_id=det["ID"])[0].get("nqn")
            except Exception:
                continue
            if nqn:
                groups.setdefault(nqn, []).append(name)
        shared = {n: v for n, v in groups.items() if len(v) > 1}
        for nqn, vols in shared.items():
            self.logger.info(f"[namespace] {nqn[-28:]} -> {sorted(vols)}")
        if not shared:
            raise AssertionError(
                f"No subsystem is shared by more than one volume: {len(groups)} "
                f"distinct NQNs across {sum(len(v) for v in groups.values())} lvols. "
                f"Namespaced children were requested but each got its own subsystem."
            )
        self.logger.info(
            f"[namespace] sharing confirmed: {len(shared)} shared subsystem(s), "
            f"largest holds {max(len(v) for v in shared.values())} volumes")

    def record_pending_lvol_delete(self, lvol, lvol_id):
        self.logger.warning(f"[DEFERRED] Adding lvol to pending delete: {lvol}")
        self.pending_deletions["lvols"][lvol] = lvol_id

    def record_pending_snapshot_delete(self, snapshot, snapshot_id):
        self.logger.warning(f"[DEFERRED] Adding snapshot to pending delete: {snapshot}")
        self.pending_deletions["snapshots"][snapshot] = snapshot_id

    def _delete_stall_diag(self, lvol_id):
        """Collect the state that explains why a delete is not completing.

        A stalled delete is nearly always explained by the object's own
        ``status``/``deletion_status`` plus the state of the nodes it lives on,
        and all of it is already on the API record. Read it while we are waiting
        so a converge failure reports a diagnosis instead of a bare name.
        """
        diag = {}
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            record = details[0] if details else {}
            for field in ("status", "deletion_status", "io_error", "node_id"):
                diag[field] = record.get(field)
            node_status = {}
            for node_id in {record.get("node_id"), *(record.get("nodes") or [])}:
                if not node_id:
                    continue
                try:
                    node = self.sbcli_utils.get_storage_node_details(node_id)
                    node_status[node_id[:8]] = (node[0] if node else {}).get("status")
                except Exception as exc:
                    node_status[node_id[:8]] = f"<unreadable: {exc}>"
            diag["node_status"] = node_status
        except Exception as exc:
            diag["error"] = f"could not read lvol details: {exc}"
        return diag

    def validate_pending_deletions(self, timeout=600, interval=30):
        if not self.pending_deletions["lvols"] and not self.pending_deletions["snapshots"]:
            self.logger.info("No deferred deletions pending")
            return

        self.logger.info("Validating deferred deletions after recovery")
        start = time.time()
        retry_interval = 60  # re-issue delete every 60s if still stuck (~10 retries in 600s)
        last_lvol_retry = {}
        last_snap_retry = {}
        last_lvol_diag = {}

        while time.time() - start < timeout:
            # --- Check and retry pending lvols (clones) FIRST ---
            # Lvols/clones must be deleted before their parent snapshots,
            # because snapshots cannot be deleted while clones still exist.
            self.logger.info(f"Checking for deferred lvols: {self.pending_deletions['lvols']}")
            for lvol in list(self.pending_deletions["lvols"]):
                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol)
                if not lvol_id or lvol_id != self.pending_deletions["lvols"][lvol]:
                    self.logger.info(f"Deferred lvol '{lvol}' no longer visible, removing from pending")
                    del self.pending_deletions["lvols"][lvol]
                    last_lvol_retry.pop(lvol, None)
                    last_lvol_diag.pop(lvol, None)
                    continue

                # Record why it is still here. Logged only when it changes, so a
                # long stall leaves one explanatory line rather than hundreds.
                diag = self._delete_stall_diag(lvol_id)
                if diag != last_lvol_diag.get(lvol):
                    self.logger.info(f"Deferred lvol '{lvol}' (id={lvol_id}) state: {diag}")
                    last_lvol_diag[lvol] = diag

                # Re-issue delete if enough time has passed since last retry.
                # Kept deliberately: a stalled delete does complete once the
                # owning node comes back, and this loop is what lets it.
                now = time.time()
                if now - last_lvol_retry.get(lvol, 0) >= retry_interval:
                    self.logger.info(f"Re-issuing delete for deferred lvol '{lvol}' (id={lvol_id})")
                    try:
                        self.sbcli_utils.delete_request(api_url=f"/lvol/{lvol_id}")
                    except Exception as exc:
                        self.logger.warning(f"Re-issue delete failed for lvol '{lvol}': {exc}")
                    last_lvol_retry[lvol] = now

            # --- Reconcile pending snapshots ---
            # Snapshots with associated clones will fail to delete, so the
            # *retry* waits until all pending lvol/clone deletes have cleared.
            # The visibility check must not wait: a snapshot that went away on
            # its own has to be noticed even while a clone is stuck, or it is
            # carried to the end and reported as un-converged alongside it.
            lvols_pending = list(self.pending_deletions["lvols"])
            self.logger.info(f"Checking for deferred snapshots: {self.pending_deletions['snapshots']}")
            for snap in list(self.pending_deletions["snapshots"]):
                if self.k8s_test:
                    snap_id = self.sbcli_utils.get_snapshot_id(snap)
                else:
                    snap_id = self.ssh_obj.get_snapshot_id_delete(self.mgmt_nodes[0], snap)
                if not snap_id or snap_id != self.pending_deletions["snapshots"][snap]:
                    self.logger.info(f"Deferred snapshot '{snap}' no longer visible, removing from pending")
                    del self.pending_deletions["snapshots"][snap]
                    last_snap_retry.pop(snap, None)
                    continue

                if lvols_pending:
                    continue

                # Re-issue delete if enough time has passed since last retry
                now = time.time()
                if now - last_snap_retry.get(snap, 0) >= retry_interval:
                    self.logger.info(f"Re-issuing delete for deferred snapshot '{snap}' (id={snap_id})")
                    try:
                        if self.k8s_test:
                            self.sbcli_utils.delete_snapshot(snap_id=snap_id, skip_error=True)
                        else:
                            self.ssh_obj.delete_snapshot(self.mgmt_nodes[0], snapshot_id=snap_id, skip_error=True)
                    except Exception as exc:
                        self.logger.warning(f"Re-issue delete failed for snapshot '{snap}': {exc}")
                    last_snap_retry[snap] = now

            if lvols_pending and self.pending_deletions["snapshots"]:
                self.logger.info(
                    f"Not retrying snapshot deletes -- {len(lvols_pending)} lvol(s) still "
                    f"pending (snapshots cannot be deleted while clones exist): {lvols_pending}"
                )

            if not self.pending_deletions["lvols"] and not self.pending_deletions["snapshots"]:
                self.logger.info("All deferred deletions completed")
                return

            sleep_n_sec(interval)

        observed = {
            name: last_lvol_diag.get(name, "<never observed>")
            for name in self.pending_deletions["lvols"]
        }
        blocked = bool(self.pending_deletions["lvols"]) and bool(self.pending_deletions["snapshots"])
        raise Exception(
            f"Deletion did not converge after {timeout}s. "
            f"Lvols: {self.pending_deletions['lvols']}, "
            f"Snapshots: {self.pending_deletions['snapshots']}. "
            f"Last observed lvol state: {observed}."
            + (
                " Snapshots were never retried because a lvol/clone was still pending, "
                "so they are blocked by the above rather than failing on their own."
                if blocked else ""
            )
        )

    def _compute_fio_size(self, extra_lvols: int = 0) -> str:
        """Compute fio_size dynamically to target ~TARGET_DATA_PER_NODE_GB per node.

        As lvol + clone count varies across iterations, fio_size adjusts
        so total disk usage per node stays approximately constant.

        Args:
            extra_lvols: Number of lvols about to be created (not yet tracked).

        Returns:
            The computed fio_size string (e.g. ``"6G"``).  Also updates
            ``self.fio_size`` in place.
        """
        num_nodes = len(self.sn_nodes) or 4
        current_lvols = len(self.lvol_mount_details) + len(self.clone_mount_details)
        total_lvols = current_lvols + extra_lvols
        if total_lvols < 1:
            total_lvols = self.total_lvols

        lvols_per_node = total_lvols / num_nodes
        # Each lvol runs fio_numjobs parallel FIO jobs, each writing fio_size
        jobs_per_node = lvols_per_node * self.fio_numjobs
        fio_size_gb = int(self.TARGET_DATA_PER_NODE_GB / max(1, jobs_per_node))

        # Cap: all numjobs × fio_size must fit in the formatted filesystem.
        # Usable capacity ≈ 80% of lvol_size (ext4/xfs overhead ~5-15%).
        max_fio_gb = int(self.int_lvol_size * 0.80) // max(1, self.fio_numjobs)
        fio_size_gb = min(fio_size_gb, max_fio_gb)
        fio_size_gb = max(fio_size_gb, 1)

        self.fio_size = f"{fio_size_gb}G"
        self.logger.info(
            f"[fio_size] Computed fio_size={self.fio_size} "
            f"(target={self.TARGET_DATA_PER_NODE_GB}G/node, "
            f"total_lvols={total_lvols}, nodes={num_nodes}, "
            f"jobs/node={jobs_per_node:.1f})"
        )
        return self.fio_size
    
    def create_lvols(self):
        """Create lvols distributed round-robin across client (fio) nodes."""
        self.logger.info(f"Creating {self.total_lvols} lvols.")
        for i in range(1, self.total_lvols + 1):
            # fs_type = random.choice(["xfs", "ext4"])
            fs_type = "ext4"
            is_crypto = random.choice([True, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            client = self._pick_client(i - 1)
            self.logger.info(
                f"Creating lvol {lvol_name}, fs={fs_type}, "
                f"crypto={is_crypto}, client={client}"
            )
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=is_crypto,
                host_id=self.lvol_node
            )
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            self.lvol_mount_details[lvol_id] = {
                   "Name": lvol_name,
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Client": client,
                   "Log": f"{self.log_path}/{lvol_name}.log",
                   "snapshots": []
            }
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=client, command=connect_str)

            self.lvol_mount_details[lvol_id]["Command"] = connect_ls
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.lvol_mount_details[lvol_id]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=client, device=lvol_device, fs_type=fs_type)

            # Mount
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_id]["Mount"] = mount_point

        self.logger.info("Completed lvol creation.")

    def create_snapshots(self):
        """Create given number of snapshots for existing lvols."""
        self.logger.info("Creating given number of snapshots.")
        for idx, lvol_id in enumerate(list(self.lvol_mount_details.keys())):
            for snap_idx in range(1, self.snapshot_per_lvol + 1):
                snapshot_name = f"{self.snapshot_name}_{idx + 1}_{snap_idx}"
                self.ssh_obj.add_snapshot(node=self.node, lvol_id=lvol_id, snapshot_name=snapshot_name)
                self.lvol_mount_details[lvol_id]["snapshots"].append(snapshot_name)
        self.logger.info("Snapshots created.")

    # def fill_volumes(self):
    #     """Fill lvols with data in batches of 10."""
    #     self.logger.info("Filling volumes with data in batches of 10.")
    #     fio_threads = []
    #     lvol_items = list(self.lvol_mount_details.items())
    #     batch_size = 10

    #     # Process lvols in batches
    #     for batch_start in range(0, len(lvol_items), batch_size):
    #         self.logger.info(f"Processing batch {batch_start // batch_size + 1}.")
    #         batch = lvol_items[batch_start:batch_start + batch_size]

    #         # Run FIO in parallel for the current batch
    #         for _, lvol in batch:
    #             fio_thread = threading.Thread(
    #                 target=self.ssh_obj.run_fio_test,
    #                 args=(self.node, None, lvol["Mount"], lvol["Log"]),
    #                 kwargs={
    #                     "size": self.fio_size,
    #                     "name": f"{lvol['Name']}_fio",
    #                     "rw": "write",
    #                     "bs": "4K-128K",
    #                     "time_based": False,
    #                 },
    #             )
    #             fio_thread.start()
    #             fio_threads.append(fio_thread)

    #         # Manage and join threads for the current batch
    #         self.common_utils.manage_fio_threads(
    #             node=self.node,
    #             threads=fio_threads,
    #             timeout=10000
    #         )
    #         for thread in fio_threads:
    #             thread.join()
    #         fio_threads = []  # Clear threads for the next batch

    #     self.logger.info("Data filling for all batches completed.")

    def fill_volumes(self):
        """Fill lvols with random data using dd in batches of 10."""
        self.logger.info("Filling volumes with random data using dd in batches of 10.")
        lvol_items = list(self.lvol_mount_details.items())
        batch_size = 10
        dd_threads = []

        # Process lvols in batches
        for batch_start in range(0, len(lvol_items), batch_size):
            self.logger.info(f"Processing batch {batch_start // batch_size + 1}.")
            batch = lvol_items[batch_start:batch_start + batch_size]

            # Run dd in parallel for the current batch
            for _, lvol in batch:
                client = lvol.get("Client", self.node)
                self.logger.info(f"Running dd for lvol: {lvol['Name']} on {client}")
                mount_path = lvol["Mount"]
                thread = threading.Thread(
                    target=self.ssh_obj.create_random_files,
                    args=(client, mount_path, self.fio_size),
                )
                thread.start()
                dd_threads.append(thread)

            # Manage and join threads for the current batch
            for thread in dd_threads:
                thread.join()
            dd_threads = []  # Clear threads for the next batch

        self.logger.info("Data filling for all batches completed.")


    def calculate_md5(self):
        "Calculate Checksums"
        for lvol_id, lvol in self.lvol_mount_details.items():
            client = lvol.get("Client", self.node)
            self.logger.info(f"Generating checksums for files in base volume: {lvol['Mount']}")
            base_files = self.ssh_obj.find_files(node=client, directory=lvol['Mount'])
            base_checksums = self.ssh_obj.generate_checksums(node=client, files=base_files)
            self.logger.info(f"Base Checksum for lvol {lvol['Name']}: {base_checksums}")
            self.lvol_mount_details[lvol_id]["MD5"] = base_checksums

    def wait_for_all_devices(self, existing_devices):
        "Waiting for devices to reconnect"
        for device in existing_devices:
            retry = 10
            while retry > 0:
                devices = self.ssh_obj.get_devices(node=self.node)
                devices = [dev[0:-1] for dev in devices]
                if device in devices:
                    break
                retry -= 1
                sleep_n_sec(10)
    
    def validate_checksums(self):
        "Validating checksums"
        for _, lvol in self.lvol_mount_details.items():
            client = lvol.get("Client", self.node)
            final_files = self.ssh_obj.find_files(node=client, directory=lvol['Mount'])
            final_checksums = self.ssh_obj.generate_checksums(node=client, files=final_files)

            assert final_checksums == lvol["MD5"], f"Checksum validation for {lvol['Name']} is not successful. Intial: {lvol['MD5']}, Final: {final_checksums}"


    def _log_block_sizes(self, label: str = "") -> None:
        """Log sysfs block sizes for every connected lvol/clone."""
        tag = f"[block_size{' ' + label if label else ''}]"
        all_details = dict(self.lvol_mount_details)
        all_details.update(getattr(self, 'clone_mount_details', {}))
        for lvol_name, details in all_details.items():
            device = details.get("Device")
            client = details.get("Client") or (
                self.fio_node[0] if isinstance(
                    getattr(self, 'fio_node', None), list)
                else getattr(self, 'fio_node', None)
            )
            if not device or not client:
                continue
            dev_name = device.split("/")[-1]
            m = re.match(r'nvme(\d+)n(\d+)', dev_name)
            if not m:
                continue
            sub, ns = m.group(1), m.group(2)
            self.logger.info(
                f"{tag} {lvol_name} ({device}) "
                f"— /sys/block/nvme{sub}c*n{ns}/size"
            )
            self.ssh_obj.exec_command(
                node=client,
                command=(
                    f"for d in /sys/block/nvme{sub}c*n{ns}; do "
                    f'echo "$d: $(cat $d/size 2>/dev/null)"; done'
                ),
            )


class TestLvolHAClusterGracefulShutdown(TestLvolHACluster):
    """Tests Graceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-1: Starting high-volume stress test.")
        self.node = self._pick_client(0)
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Graceful shutdown and restart.")
        timestamp = int(datetime.now().timestamp())

        self.sbcli_utils.suspend_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "suspended",
                                                      timeout=4000)
        sleep_n_sec(10)
        self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "offline",
                                                      timeout=4000)
        sleep_n_sec(30)

        self.validate_checksums()

        restart_start_time = datetime.now()
        self.sbcli_utils.restart_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()

        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)

        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Graceful shutdown and start total time: {time_mins}")
        
        self.validate_checksums()

        
        self.logger.info("Stress test completed.")


class TestLvolHAClusterStorageNodeCrash(TestLvolHACluster):
    """Tests Ungraceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-2: Starting high-volume stress test.")
        self.node = self._pick_client(0)
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        # self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Container stop and restart.")
        timestamp = int(datetime.now().timestamp())
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]
        
        self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
        sleep_n_sec(30)
        
        self.validate_checksums()

        restart_start_time = datetime.now()
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=4000)
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()
        
        self.validate_checksums()
        
        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)

        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Crash and start total time: {time_mins}")
        
        self.logger.info("Stress test completed.")


class TestLvolHAClusterNetworkInterrupt(TestLvolHACluster):
    """Tests Graceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-3: Starting high-volume stress test.")
        self.node = self._pick_client(0)
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        cmd = (
            'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 300 && '
            'sudo nmcli dev connect eth0" &'
        )
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]

        def execute_disconnect():
            self.logger.info(f"Executing disconnect command on node {node_ip}.")
            self.ssh_obj.exec_command(node_ip, command=cmd)

        self.logger.info("Network stop and restart.")
        timestamp = int(datetime.now().timestamp())

        disconnect_thread = threading.Thread(target=execute_disconnect)
        unavailable_thread = threading.Thread(
            target=lambda: self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "schedulable", 4000)
        )

        disconnect_thread.start()
        unavailable_thread.start()

        disconnect_start_time = datetime.now()
        
        self.ssh_obj.exec_command(node_ip, command=cmd)
        
        unavailable_thread.join()
        
        self.logger.info("Waiting for 30 seconds to run checksum validation.")
        sleep_n_sec(30)
        self.validate_checksums()

        disconnect_thread.join()
        
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()
        
        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")
        output, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], 
                                              command=f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0")
        self.logger.info(f"Data migration output: {output}")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(120)

        self.validate_checksums()

        time_secs = node_up_time - disconnect_start_time
        time_mins = (time_secs.seconds - 120) / 60
        self.logger.info(f"Network reconnect and node online total time: {time_mins}")
        
        self.logger.info("Stress test completed.")


class TestLvolHAClusterPartialNetworkOutage(TestLvolHACluster):
    """Tests Graceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-4: Starting high-volume stress test.")
        self.node = self._pick_client(0)
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]
        
        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Partial Network Outage")
        timestamp = int(datetime.now().timestamp())

        unavailable_thread = threading.Thread(
            target=lambda: self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "unreachable", 4000)
        )
        unavailable_thread.start()

        lvol_ports = node_details[0]["lvol_subsys_port"]
        if not isinstance(lvol_ports, list):
            lvol_ports = [lvol_ports]
        ports_to_block = [int(port) for port in lvol_ports]
        ports_to_block.append(4420)
        
        ports_blocked = self.ssh_obj.perform_nw_outage(
            node_ip=node_ip,
            block_ports=ports_to_block,
            block_all_ss_ports=False
        )

        unavailable_thread.join()
        sleep_n_sec(300)

        self.validate_checksums()

        restart_start_time = datetime.now()
        self.ssh_obj.remove_nw_outage(node_ip=node_ip, blocked_ports=ports_blocked)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "in_restart",
                                                      timeout=4000)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()

        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)

        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Partial Outage and start total time: {time_mins}")
        
        self.validate_checksums()
        
        self.logger.info("Stress test completed.")

class TestLvolHAClusterRunAllScenarios(TestLvolHACluster):
    """
    Runs all three scenarios (Graceful Shutdown, Storage Node Crash, Network Interrupt)
    as part of a single test execution flow with setup performed only once.
    """

    def run(self):
        """Main execution for all scenarios."""
        self.logger.info("Starting high-volume stress test for all scenarios.")
        
        # Setup performed only once
        self.logger.info("Performing initial setup.")
        self.node = self._pick_client(0)
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.logger.info("Creating lvols.")
        self.create_lvols()
        
        # self.logger.info("Creating snapshots.")
        # self.create_snapshots()
        
        self.logger.info("Filling lvols with data.")
        self.fill_volumes()
        
        self.logger.info("Calculating MD5 checksums for validation.")
        self.calculate_md5()
        
        # Scenario 1: Graceful Shutdown
        self.logger.info("Running Scenario 1: Graceful Shutdown.")
        self.run_graceful_shutdown_scenario()
        
        # Scenario 2: Storage Node Crash
        self.logger.info("Running Scenario 2: Storage Node Crash.")
        self.run_storage_node_crash_scenario()
        
        # Scenario 3: Network Interrupt
        self.logger.info("Running Scenario 3: Network Interrupt.")
        self.run_network_interrupt_scenario()

        self.logger.info("All scenarios completed successfully.")

    def run_graceful_shutdown_scenario(self):
        """Graceful shutdown and restart scenario."""
        timestamp = int(datetime.now().timestamp())
        self.logger.info("Graceful shutdown initiated.")
        
        self.sbcli_utils.suspend_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "suspended",
                                                      timeout=4000)
        sleep_n_sec(10)

        self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "offline", timeout=4000)
        sleep_n_sec(30)
        self.validate_checksums()
        
        restart_start_time = datetime.now()
        self.sbcli_utils.restart_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check after restart: {actual_status}")
        
        node_up_time = datetime.now()

        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)

        self.validate_checksums()
        
        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Graceful shutdown and start total time: {time_mins} minutes.")

    def run_storage_node_crash_scenario(self):
        """Storage node crash and recovery scenario."""
        timestamp = int(datetime.now().timestamp())
        self.logger.info("Simulating storage node crash.")
        
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]
        
        self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port, self.cluster_id)
        sleep_n_sec(30)
        self.validate_checksums()
        restart_start_time = datetime.now()
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check after crash recovery: {actual_status}")
        
        node_up_time = datetime.now()

        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)

        self.validate_checksums()
        
        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Crash and recovery total time: {time_mins} minutes.")

    def run_network_interrupt_scenario(self):
        """Network interrupt and recovery scenario."""
        timestamp = int(datetime.now().timestamp())
        self.logger.info("Simulating network interruption.")
        
        cmd = (
            'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 300 && '
            'sudo nmcli dev connect eth0" &'
        )
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]

        def execute_disconnect():
            self.logger.info(f"Executing disconnect command on node {node_ip}.")
            self.ssh_obj.exec_command(node_ip, command=cmd)

        self.logger.info("Network stop and restart.")
        timestamp = int(datetime.now().timestamp())

        disconnect_thread = threading.Thread(target=execute_disconnect)
        unavailable_thread = threading.Thread(
            target=lambda: self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "schedulable", 4000)
        )

        disconnect_thread.start()
        unavailable_thread.start()

        disconnect_start_time = datetime.now()
        
        self.ssh_obj.exec_command(node_ip, command=cmd)
        
        unavailable_thread.join()
        
        self.logger.info("Waiting for 30 seconds to run checksum validation.")
        sleep_n_sec(30)
        self.validate_checksums()

        disconnect_thread.join()
        
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check after network recovery: {actual_status}")
        
        node_up_time = datetime.now()

        sleep_n_sec(1000)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 2000, None)
        sleep_n_sec(30)


        self.validate_checksums()
        
        time_secs = node_up_time - disconnect_start_time
        time_mins = (time_secs.seconds - 120) / 60
        self.logger.info(f"Network reconnect and recovery total time: {time_mins} minutes.")
