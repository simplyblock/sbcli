"""
K8s-native node migration E2E test.

Migrates a randomly chosen storage node to a user-specified worker node
by patching the StorageNode CRD with action=restart.  FIO runs as K8s
Jobs throughout the migration to verify I/O is not interrupted.

No SSH to worker nodes is required (Talos-compatible).
"""

from __future__ import annotations

import os
import random
import string
from datetime import datetime

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils
from utils.ssh_utils import RunnerK8sLog


def _rand_seq(length: int = 6) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


class K8sNativeNodeMigrationTest(TestClusterBase):
    """
    E2E test: migrate a storage node to a different K8s worker while
    FIO is running on PVCs.

    Steps
    -----
    1. Create StorageClass + VolumeSnapshotClass.
    2. Create PVCs on existing nodes and start FIO Jobs.
    3. Create snapshots + clones on existing PVCs with FIO.
    4. Randomly pick a storage node and migrate it to the target worker.
    5. Wait for migration to complete (node online, cluster active).
    6. Validate all FIO jobs and node health.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_node_migration"
        self.k8s_utils: K8sUtils | None = None

        # Target worker node for migration (K8s node name, from user)
        self.migrate_to_worker = kwargs.get("migrate_to_worker", "")
        if isinstance(self.migrate_to_worker, str):
            self.migrate_to_worker = self.migrate_to_worker.strip()

        # K8s resource naming
        self.STORAGE_CLASS_NAME = "simplyblock-csi-sc"
        self.SNAPSHOT_CLASS_NAME = "simplyblock-csi-snapshotclass"
        self.FIO_IMAGE = "dockerpinata/fio:2.1"

        # Sizing
        self.pvc_size = "10Gi"
        self.fio_size = "1G"
        self.FIO_RUNTIME = 2000

        # Counts
        self.fio_num_jobs = 2

        # Tracking
        self.pvc_details: dict[str, dict] = {}
        self.snapshot_details: dict[str, dict] = {}
        self.clone_details: dict[str, dict] = {}

        self.logger.info(f"Migrate to worker: {self.migrate_to_worker}")

    # ── Setup ─────────────────────────────────────────────────────────────────

    def setup(self):
        """K8s-native setup — no SSH to worker nodes."""
        self.logger.info("Inside K8sNativeNodeMigrationTest.setup()")

        # Retry sbcli API calls (routed through kubectl exec)
        retry = 30
        while retry > 0:
            try:
                self.logger.info("Getting all storage nodes")
                self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_lvols()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as e:
                self.logger.debug(f"API call failed: {e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry exhausted. API failed: {e}")
                    raise
                self.logger.info(f"Retrying base APIs. Attempt: {30 - retry + 1}")
                sleep_n_sec(10)

        self.client_machines = []
        self.fio_node = []

        # Log directories
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.docker_logs_path = os.path.join(
            self.nfs_log_base, f"{self.test_name}-{timestamp}"
        )
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        os.makedirs(self.docker_logs_path, exist_ok=True)

        run_file = os.getenv("RUN_DIR_FILE", None)
        if run_file:
            with open(run_file, "w") as f:
                f.write(self.docker_logs_path)

        # K8s log monitor
        self.runner_k8s_log = RunnerK8sLog(
            log_dir=self.docker_logs_path,
            test_name=self.test_name,
        )
        self.runner_k8s_log.start_logging()
        self.runner_k8s_log.monitor_pod_logs()

        # Clean old resources
        try:
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.warning(f"Cleanup of old resources failed: {e}")

        # Initialize K8sUtils
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        self.k8s_utils = K8sUtils(
            ssh_obj=self.ssh_obj,
            mgmt_node=mgmt_node,
        )
        self.logger.info(f"[K8s] K8sUtils initialized for mgmt_node={mgmt_node!r}")

    # ── FIO config ────────────────────────────────────────────────────────────

    def _build_fio_config(self, name: str) -> str:
        bs = f"{2 ** random.randint(2, 7)}k"
        return (
            f"[global]\n"
            f"name={name}-fio\n"
            f"filename_format=/spdkvol/fio-testfile.$jobnum\n"
            f"rw=randrw\n"
            f"rwmixread=50\n"
            f"bs={bs}\n"
            f"iodepth=256\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={self.fio_size}\n"
            f"numjobs={self.fio_num_jobs}\n"
            f"time_based\n"
            f"runtime={self.FIO_RUNTIME}\n"
            f"group_reporting\n"
            f"verify=md5\n"
            f"verify_dump=1\n"
            f"verify_fatal=1\n"
            f"\n"
            f"[job1]\n"
        )

    def _save_fio_pod_logs(self, job_name: str, resource_name: str):
        """Save FIO pod logs to log directory for post-mortem debugging."""
        try:
            pod_name = self.k8s_utils.get_job_pod_name(job_name)
            if not pod_name:
                return
            logs = self.k8s_utils.get_pod_logs(pod_name, tail=2000)
            if logs:
                log_file = os.path.join(self.log_path, f"{resource_name}_fio.log")
                with open(log_file, "w") as f:
                    f.write(logs)
                self.logger.info(f"Saved FIO logs for {resource_name} to {log_file}")
        except Exception as exc:
            self.logger.warning(f"Could not save FIO logs for {resource_name}: {exc}")

    # ── Main test flow ────────────────────────────────────────────────────────

    def run(self):
        self.logger.info("Starting Test: K8s Native Node Migration During FIO")

        assert self.migrate_to_worker, (
            "migrate_to_worker is required — provide --migrate_to_worker <k8s-node-name>"
        )

        # ── Step 1: Create StorageClass + VolumeSnapshotClass ─────────────
        self.logger.info("Step 1: Creating StorageClass and VolumeSnapshotClass")

        cluster_id = self.cluster_id
        pool_name = self.pool_name

        self.sbcli_utils.add_storage_pool(pool_name)
        sleep_n_sec(10)

        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        self.k8s_utils.create_volume_snapshot_class(name=self.SNAPSHOT_CLASS_NAME)

        # Record nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        node_count = len(storage_nodes)
        self.logger.info(f"Cluster has {node_count} storage nodes")

        # ── Step 2: Create PVCs on existing nodes + FIO ───────────────────
        self.logger.info("Step 2: Creating PVCs and starting FIO Jobs")

        for i in range(node_count):
            pvc_name = f"mig-pvc-{_rand_seq(4)}-{i}"
            job_name = f"fio-{pvc_name}"
            cm_name = f"fio-cfg-{pvc_name}"

            self.k8s_utils.create_pvc(
                name=pvc_name,
                size=self.pvc_size,
                storage_class=self.STORAGE_CLASS_NAME,
            )
            self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)

            fio_config = self._build_fio_config(pvc_name)
            self.k8s_utils.create_fio_job(
                job_name=job_name,
                pvc_name=pvc_name,
                configmap_name=cm_name,
                fio_config=fio_config,
                image=self.FIO_IMAGE,
            )

            self.pvc_details[pvc_name] = {
                "job_name": job_name,
                "configmap_name": cm_name,
                "snapshots": [],
            }
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details)

        # ── Step 3: Create snapshots + clones with FIO ───────────────────
        self.logger.info("Step 3: Creating snapshots and clones on existing PVCs")

        for pvc_name, detail in self.pvc_details.items():
            snap_name = f"snap-{pvc_name}"
            clone_name = f"clone-{pvc_name}"
            clone_job = f"fio-{clone_name}"
            clone_cm = f"fio-cfg-{clone_name}"

            self.k8s_utils.create_volume_snapshot(
                name=snap_name,
                pvc_name=pvc_name,
                snapshot_class=self.SNAPSHOT_CLASS_NAME,
            )
            self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)

            detail["snapshots"].append(snap_name)
            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}

            self.k8s_utils.create_clone_pvc(
                name=clone_name,
                size=self.pvc_size,
                storage_class=self.STORAGE_CLASS_NAME,
                snapshot_name=snap_name,
            )
            self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)

            fio_config = self._build_fio_config(clone_name)
            self.k8s_utils.create_fio_job(
                job_name=clone_job,
                pvc_name=clone_name,
                configmap_name=clone_cm,
                fio_config=fio_config,
                image=self.FIO_IMAGE,
            )

            self.clone_details[clone_name] = {
                "snap_name": snap_name,
                "job_name": clone_job,
                "configmap_name": clone_cm,
            }
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details)

        sleep_n_sec(30)

        # ── Step 4: Migrate a random storage node ─────────────────────────
        self.logger.info("Step 4: Migrating a randomly chosen storage node")

        online_nodes = [
            n for n in self.sbcli_utils.get_storage_nodes()["results"]
            if n["status"] == "online"
        ]
        assert len(online_nodes) > 0, "No online storage nodes available to migrate"

        migrate_node = random.choice(online_nodes)
        migrate_node_uuid = migrate_node["id"]
        self.logger.info(
            f"Randomly selected node {migrate_node_uuid} for migration "
            f"to worker '{self.migrate_to_worker}'"
        )

        migration_timestamp = int(datetime.now().timestamp())

        self.k8s_utils.patch_storage_node_migrate(
            node_uuid=migrate_node_uuid,
            target_worker=self.migrate_to_worker,
        )

        # ── Step 5: Wait for migration to complete ────────────────────────
        self.logger.info("Step 5: Waiting for migration to complete")

        sleep_n_sec(30)

        # Wait for node to come back online
        self.sbcli_utils.wait_for_storage_node_status(
            node_id=migrate_node_uuid,
            status="online",
            timeout=600,
        )

        # Wait for cluster to return to active
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id,
            status="active",
            timeout=600,
        )

        # Wait for migration/balancing tasks
        sleep_n_sec(60)
        self.validate_migration_for_node(
            migration_timestamp, 2000, None, 60, no_task_ok=True
        )
        sleep_n_sec(30)

        self.logger.info(
            f"Node {migrate_node_uuid} successfully migrated to "
            f"worker '{self.migrate_to_worker}'"
        )

        self.runner_k8s_log.restart_logging()

        # ── Step 6: Validate ─────────────────────────────────────────────
        self.logger.info("Step 6: Validating all FIO jobs and node health")

        # Validate FIO on PVCs
        for pvc_name, detail in self.pvc_details.items():
            self.logger.info(f"Validating FIO job for PVC: {pvc_name}")
            self._save_fio_pod_logs(detail["job_name"], pvc_name)
            self.k8s_utils.validate_fio_job(detail["job_name"])

        # Validate FIO on clones
        for clone_name, detail in self.clone_details.items():
            self.logger.info(f"Validating FIO job for clone: {clone_name}")
            self._save_fio_pod_logs(detail["job_name"], clone_name)
            self.k8s_utils.validate_fio_job(detail["job_name"])

        # Validate all nodes healthy
        final_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in final_nodes:
            assert node["status"] == "online", (
                f"Node {node['id']} is not online (status={node['status']})"
            )
            assert node["health_check"], (
                f"Node {node['id']} health check failed"
            )

        self.logger.info(
            f"All {len(final_nodes)} nodes online and healthy after migration."
        )
        self.logger.info("TEST CASE PASSED !!!")
