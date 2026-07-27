"""
K8s-native major upgrade E2E test.

Supports two upgrade paths:

**R25 → R26 (maintenance window)**:
  Full Helm-to-Operator migration following the production upgrade guide:
  1. Annotate FDB resources with ``helm.sh/resource-policy: keep``
  2. Shut down all storage nodes (suspend + shutdown)
  3. Uninstall old Helm chart(s)
  4. Create upgrade secret with existing cluster UUID/secret
  5. Install new operator Helm chart (FDB disabled)
  6. Shut down nodes again (prevent auto-restart)
  7. Apply CRs (StorageCluster, Pool, StorageNode)
  8. Run R25→R26 DB migration script
  9. Patch backend objects with CR references
  10. Restart storage nodes one at a time
  FIO runs before the upgrade and after to verify data integrity.
  During the maintenance window, volumes are unavailable.

**R26+ (rolling upgrade, no downtime)**:
  1. Helm upgrade (control plane)
  2. Rolling StorageNode CRD patch per node (action=restart + new images)
  FIO runs continuously throughout the entire upgrade.

No SSH to worker nodes required (Talos-compatible).
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


# R25 → R26 DB migration script — run inside admin pod
_R25_R26_MIGRATION_SCRIPT = r"""
from simplyblock_core import utils
from simplyblock_core.db_controller import DBController

db_controller = DBController()

for snode in db_controller.get_storage_nodes():
    print(f"updating storage node object: {snode.get_id()}")
    for node in db_controller.get_storage_nodes():
        if snode.get_id() == node.secondary_node_id:
            snode.lvstore_stack_secondary = node.get_id()
            break
    snode.lvstore_ports = {
        snode.lvstore: {
            "lvol_subsys_port": snode.lvol_subsys_port,
            "hublvol_port": snode.hublvol.nvmf_port
        }
    }
    if snode.lvstore_stack_secondary:
        sec = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
        snode.lvstore_ports[sec.lvstore] = {
            "lvol_subsys_port": sec.lvol_subsys_port,
            "hublvol_port": sec.hublvol.nvmf_port,
        }
    if snode.poller_cpu_cores:
        snode.lvol_poller_mask = utils.generate_mask([snode.poller_cpu_cores[-1]])
        if len(snode.poller_cpu_cores) > 1:
            snode.poller_cpu_cores = snode.poller_cpu_cores[:-1]
            snode.pollers_mask = utils.generate_mask(snode.poller_cpu_cores)

    snode.write_to_db()

print("Creating mini lvol objects")
for lvol in db_controller.get_all_lvols():
    lvol.write_to_db()

print("Creating mini Snapshots objects")
for snap in db_controller.get_snapshots():
    snap.write_to_db()

print("done")
"""

# FDB resources that need the keep annotation (Step 1 of migration guide)
_FDB_KEEP_RESOURCES = [
    ("deployment", "simplyblock-fdb-controller-manager"),
    ("serviceaccount", "simplyblock-fdb-controller-manager"),
    ("clusterrole", "simplyblock-fdb-manager-role"),
    ("clusterrole", "simplyblock-fdb-manager-clusterrole"),
    ("rolebinding", "simplyblock-fdb-manager-rolebinding"),
    ("clusterrolebinding", "simplyblock-fdb-manager-clusterrolebinding"),
    ("foundationdbcluster", "simplyblock-fdb-cluster"),
]

# Default CR names matching the k8s-native-e2e.yaml workflow
_DEFAULT_CLUSTER_CR = "simplyblock-cluster"
_DEFAULT_NODE_CR = "simplyblock-node"
_DEFAULT_POOL_CR = "simplyblock-pool"
_NAMESPACE = "simplyblock"


class K8sNativeMajorUpgrade(TestClusterBase):
    """
    K8s-native major upgrade test with two paths:

    - **R25→R26**: Maintenance window upgrade (full Helm-to-Operator migration).
      FIO runs before and after, but NOT during the maintenance window.
    - **R26+**: Rolling upgrade with no downtime.
      FIO runs continuously throughout the upgrade.
    """

    def __init__(self, **kwargs):
        # Force K8s mode
        kwargs["k8s_run"] = True
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_major_upgrade"

        # Version info (passed by upgrade_e2e.py)
        self.base_version = kwargs.get("base_version", "")
        self.target_version = kwargs.get("target_version", "latest")
        self.target_spdk_image = kwargs.get("target_spdk_image", "")
        self.target_docker_image = kwargs.get("target_docker_image", "")

        # K8s-specific config from environment
        self.target_spdk_proxy_image = os.environ.get(
            "TARGET_SPDK_PROXY_IMAGE", self.target_docker_image
        )
        self.operator_tag = os.environ.get("OPERATOR_TAG", self.target_version)
        self.simplyblock_repo = os.environ.get(
            "SIMPLYBLOCK_REPO",
            "public.ecr.aws/simply-block/simplyblock",
        )
        self.operator_repo = os.environ.get(
            "OPERATOR_REPO", "simplyblock/simplyblock-operator"
        )
        self.helm_chart_path = os.environ.get(
            "HELM_CHART_PATH",
            "/tmp/helm-charts/charts/simplyblock-operator/",
        )
        self.tls_enabled = os.environ.get("TLS_ENABLED", "").lower() in ("true", "1")
        self.csi_repository = os.environ.get("CSI_REPOSITORY", "")
        self.csi_tag = os.environ.get("CSI_TAG", "")

        # Upgrade type: "r25-to-r2x" (maintenance window) or "rolling" (R26+)
        self.upgrade_type = os.environ.get("UPGRADE_TYPE", "").lower()

        # Helm release names (for uninstall during migration)
        self.helm_release_spdk_csi = os.environ.get("HELM_RELEASE_SPDK_CSI", "spdk-csi")
        self.helm_release_sbcli = os.environ.get("HELM_RELEASE_SBCLI", "")

        # CR names
        self.cluster_cr_name = os.environ.get("CLUSTER_CR_NAME", _DEFAULT_CLUSTER_CR)
        self.node_cr_name = os.environ.get("STORAGE_NODE_CR_NAME", _DEFAULT_NODE_CR)
        self.pool_cr_name = os.environ.get("POOL_CR_NAME", _DEFAULT_POOL_CR)

        self.k8s_utils: K8sUtils | None = None

        # K8s resource naming
        self.STORAGE_CLASS_NAME = "simplyblock-csi-sc"
        self.XFS_STORAGE_CLASS_NAME = "simplyblock-csi-sc-xfs"
        self.SNAPSHOT_CLASS_NAME = "simplyblock-csi-snapshotclass"
        self.FIO_IMAGE = "dockerpinata/fio:2.1"

        # Sizing
        self.pvc_size = "10Gi"
        self.fio_size = "1G"

        # FIO runtime depends on upgrade type — set in run()
        self.fio_num_jobs = 1

        # Tracking
        self.pvc_details: dict[str, dict] = {}
        self.snapshot_details: dict[str, dict] = {}
        self.clone_details: dict[str, dict] = {}

        self.logger.info(
            f"K8s native upgrade: {self.base_version} -> {self.target_version} "
            f"(upgrade_type={self.upgrade_type or 'auto-detect'})"
        )
        self.logger.info(
            f"  target_spdk_image={self.target_spdk_image}, "
            f"target_spdk_proxy_image={self.target_spdk_proxy_image}, "
            f"target_docker_image={self.target_docker_image}"
        )

    # ── Setup ──────────────────────────────────────────────────────────────────

    def setup(self):
        """K8s-native setup — no SSH to worker nodes."""
        self.logger.info("Inside K8sNativeMajorUpgrade.setup()")

        retry = 30
        while retry > 0:
            try:
                self.logger.info("Getting all storage nodes")
                self.mgmt_nodes, self.storage_nodes = (
                    self.sbcli_utils.get_all_nodes_ip()
                )
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

        self._validate_storage_node_health()

        self.client_machines = []
        self.fio_node = []

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

        self.runner_k8s_log = RunnerK8sLog(
            log_dir=self.docker_logs_path,
            test_name=self.test_name,
        )
        self.runner_k8s_log.start_logging()
        self.runner_k8s_log.monitor_pod_logs()

        try:
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.warning(f"Cleanup of old resources failed: {e}")

        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        self.k8s_utils = K8sUtils(
            ssh_obj=self.ssh_obj,
            mgmt_node=mgmt_node,
        )
        self.logger.info(f"[K8s] K8sUtils initialized for mgmt_node={mgmt_node!r}")

        self.k8s_utils.cleanup_stale_fio_resources()
        sleep_n_sec(5)

    # ── Version checks ─────────────────────────────────────────────────────────

    def _is_maintenance_window_upgrade(self) -> bool:
        """R25→R2x requires a maintenance window (CSI→Operator migration).

        Uses the explicit UPGRADE_TYPE env var as the primary signal.
        Falls back to version string parsing if UPGRADE_TYPE is not set.
        """
        if self.upgrade_type == "r25-to-r2x":
            return True
        if self.upgrade_type == "rolling":
            return False
        # Fallback: parse version strings
        if not self.base_version or not self.target_version:
            return False
        return self.base_version.lower().startswith("r25")

    # ── FIO config ─────────────────────────────────────────────────────────────

    def _build_fio_config(self, name: str, runtime: int = None) -> tuple[str, str]:
        bs = f"{2 ** random.randint(2, 7)}k"
        run_id = _rand_seq(6)
        randseed = random.randint(1, 2**63)
        fio_runtime = runtime or self.FIO_RUNTIME

        main_config = (
            f"[global]\n"
            f"name={name}-fio\n"
            f"filename_format=/spdkvol/fio-{run_id}.$jobnum\n"
            f"rw=randrw\n"
            f"rwmixread=50\n"
            f"bs={bs}\n"
            f"iodepth=1\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={self.fio_size}\n"
            f"numjobs={self.fio_num_jobs}\n"
            f"time_based\n"
            f"runtime={fio_runtime}\n"
            f"group_reporting\n"
            f"verify=md5\n"
            f"verify_dump=1\n"
            f"verify_fatal=1\n"
            f"verify_backlog=4096\n"
            f"verify_backlog_batch=32\n"
            f"randseed={randseed}\n"
            f"max_latency=20s\n"
            f"\n"
            f"[job1]\n"
        )

        warmup_config = (
            f"[global]\n"
            f"name={name}-warmup\n"
            f"filename_format=/spdkvol/fio-{run_id}.$jobnum\n"
            f"rw=write\n"
            f"bs=1m\n"
            f"iodepth=32\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={self.fio_size}\n"
            f"numjobs={self.fio_num_jobs}\n"
            f"group_reporting\n"
            f"zero_buffers\n"
            f"\n"
            f"[job1]\n"
        )

        return main_config, warmup_config

    def _save_fio_pod_logs(self, job_name: str, resource_name: str):
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

    # ── Common: pre-upgrade data setup ─────────────────────────────────────────

    def _create_storage_classes(self, cluster_id: str, pool_name: str):
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        self.k8s_utils.create_storage_class(
            name=self.XFS_STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
            fs_type="xfs",
        )
        self.k8s_utils.create_volume_snapshot_class(name=self.SNAPSHOT_CLASS_NAME)

    def _create_pvcs_with_fio(self, count: int, runtime: int = None):
        """Create PVCs and start FIO Jobs on each."""
        for i in range(count):
            pvc_name = f"upgrade-pvc-{_rand_seq(4)}-{i}"
            job_name = f"fio-{pvc_name}"
            cm_name = f"fio-cfg-{pvc_name}"
            sc_name = random.choice(
                [self.STORAGE_CLASS_NAME, self.XFS_STORAGE_CLASS_NAME]
            )
            fs_type = "xfs" if sc_name == self.XFS_STORAGE_CLASS_NAME else "ext4"

            self.k8s_utils.create_pvc(
                name=pvc_name, size=self.pvc_size, storage_class=sc_name,
            )
            self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)

            self.pvc_details[pvc_name] = {
                "job_name": job_name,
                "configmap_name": cm_name,
                "snapshots": [],
                "storage_class": sc_name,
                "fs_type": fs_type,
            }

        for pvc_name, detail in self.pvc_details.items():
            fio_config, warmup_config = self._build_fio_config(pvc_name, runtime=runtime)
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(pvc_name, self.sbcli_utils)
            self.k8s_utils.create_fio_job(
                job_name=detail["job_name"],
                pvc_name=pvc_name,
                configmap_name=detail["configmap_name"],
                fio_config=fio_config,
                image=self.FIO_IMAGE,
                avoid_node=avoid,
                warmup_config=warmup_config,
            )
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details)

    def _create_snapshots_and_clones(self, runtime: int = None):
        """Create snapshots + clones with FIO on each clone."""
        for pvc_name, detail in self.pvc_details.items():
            snap_name = f"snap-{pvc_name}"
            clone_name = f"clone-{pvc_name}"
            clone_job = f"fio-{clone_name}"
            clone_cm = f"fio-cfg-{clone_name}"

            self.k8s_utils.create_volume_snapshot(
                name=snap_name, pvc_name=pvc_name,
                snapshot_class=self.SNAPSHOT_CLASS_NAME,
            )
            self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)

            detail["snapshots"].append(snap_name)
            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}

            clone_sc = detail.get("storage_class", self.STORAGE_CLASS_NAME)
            clone_fs_type = detail.get("fs_type", "ext4")
            self.k8s_utils.create_clone_pvc(
                name=clone_name, size=self.pvc_size,
                storage_class=clone_sc, snapshot_name=snap_name,
            )
            self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)

            fio_config, warmup_config = self._build_fio_config(clone_name, runtime=runtime)
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(clone_name, self.sbcli_utils)
            self.k8s_utils.create_fio_job(
                job_name=clone_job, pvc_name=clone_name,
                configmap_name=clone_cm, fio_config=fio_config,
                image=self.FIO_IMAGE, avoid_node=avoid,
                warmup_config=warmup_config,
            )

            self.clone_details[clone_name] = {
                "snap_name": snap_name, "job_name": clone_job,
                "configmap_name": clone_cm, "storage_class": clone_sc,
                "fs_type": clone_fs_type,
            }
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details)

    def _validate_all_fio(self, timeout: int):
        """Save logs and validate all FIO jobs (PVCs + clones)."""
        for pvc_name, detail in self.pvc_details.items():
            self.logger.info(f"Validating FIO for PVC: {pvc_name}")
            self._save_fio_pod_logs(detail["job_name"], pvc_name)
            self.k8s_utils.validate_fio_job(detail["job_name"], timeout=timeout)

        for clone_name, detail in self.clone_details.items():
            self.logger.info(f"Validating FIO for clone: {clone_name}")
            self._save_fio_pod_logs(detail["job_name"], clone_name)
            self.k8s_utils.validate_fio_job(detail["job_name"], timeout=timeout)

    def _run_post_upgrade_verification(self):
        """Create new PVC + FIO + snapshot + clone post-upgrade."""
        self.logger.info("Post-upgrade: Creating new PVC to verify provisioning")
        post_pvc = f"post-upgrade-pvc-{_rand_seq(4)}"
        post_job = f"fio-{post_pvc}"
        post_cm = f"fio-cfg-{post_pvc}"

        self.k8s_utils.create_pvc(
            name=post_pvc, size=self.pvc_size,
            storage_class=self.STORAGE_CLASS_NAME,
        )
        self.k8s_utils.wait_pvc_bound(post_pvc, timeout=300)

        fio_cfg, warmup_cfg = self._build_fio_config(post_pvc, runtime=120)
        avoid = self.k8s_utils.get_pvc_primary_k8s_node(post_pvc, self.sbcli_utils)
        self.k8s_utils.create_fio_job(
            job_name=post_job, pvc_name=post_pvc, configmap_name=post_cm,
            fio_config=fio_cfg, image=self.FIO_IMAGE, avoid_node=avoid,
            warmup_config=warmup_cfg,
        )

        post_snap = f"snap-{post_pvc}"
        post_clone = f"clone-{post_pvc}"
        post_clone_job = f"fio-{post_clone}"
        post_clone_cm = f"fio-cfg-{post_clone}"

        sleep_n_sec(30)
        self.k8s_utils.create_volume_snapshot(
            name=post_snap, pvc_name=post_pvc,
            snapshot_class=self.SNAPSHOT_CLASS_NAME,
        )
        self.k8s_utils.wait_volume_snapshot_ready(post_snap, timeout=300)

        self.k8s_utils.create_clone_pvc(
            name=post_clone, size=self.pvc_size,
            storage_class=self.STORAGE_CLASS_NAME, snapshot_name=post_snap,
        )
        self.k8s_utils.wait_pvc_bound(post_clone, timeout=300)

        clone_fio, clone_warmup = self._build_fio_config(post_clone, runtime=120)
        self.k8s_utils.create_fio_job(
            job_name=post_clone_job, pvc_name=post_clone,
            configmap_name=post_clone_cm, fio_config=clone_fio,
            image=self.FIO_IMAGE, avoid_node=avoid, warmup_config=clone_warmup,
        )

        self._save_fio_pod_logs(post_job, post_pvc)
        self.k8s_utils.validate_fio_job(post_job, timeout=600)
        self._save_fio_pod_logs(post_clone_job, post_clone)
        self.k8s_utils.validate_fio_job(post_clone_job, timeout=600)

    def _assert_all_nodes_healthy(self):
        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_node_list:
            assert node["status"] == "online", (
                f"Node {node['id']} not online (status={node['status']})"
            )
            assert node.get("health_check", True), (
                f"Node {node['id']} health check failed"
            )

    # ══════════════════════════════════════════════════════════════════════════
    # ROLLING UPGRADE (R26+, no maintenance window)
    # ══════════════════════════════════════════════════════════════════════════

    def _helm_upgrade(self):
        """Run helm upgrade to update the control plane to the target version."""
        self.logger.info(
            f"Running helm upgrade: simplyblock.tag={self.target_docker_image}, "
            f"operator.tag={self.operator_tag}"
        )

        tls_flags = ""
        if self.tls_enabled:
            tls_flags = "--set tls.enabled=true --set tls.mutual_enabled=true"

        csi_flags = ""
        if self.csi_repository:
            csi_flags += f" --set image.csi.repository={self.csi_repository}"
        if self.csi_tag:
            csi_flags += f" --set image.csi.tag={self.csi_tag}"

        helm_cmd = (
            f"helm upgrade --install spdk-csi {self.helm_chart_path} "
            f"--namespace {_NAMESPACE} "
            f"--timeout 10m "
            f"--set image.simplyblock.repository={self.simplyblock_repo} "
            f"--set image.simplyblock.tag={self.target_docker_image} "
            f"--set image.operator.repository={self.operator_repo} "
            f"--set image.operator.tag={self.operator_tag} "
            f"--set controlplane.enabled=true "
            f"--set operator.enabled=true "
            f"--set controlplane.csiHostpathDriver.enabled=true "
            f"--set controlplane.storageclass.name=local-hostpath "
            f"--set csiConfig.simplybk.ip=http://simplyblock-webappapi.simplyblock:5000 "
            f"{tls_flags} {csi_flags}"
        ).strip()

        out, err = self.k8s_utils._exec_kubectl(helm_cmd)
        self.logger.info(f"Helm upgrade stdout: {out[:500] if out else ''}")
        if err and err.strip():
            self.logger.info(f"Helm upgrade stderr: {err[:500]}")

        self.logger.info("Waiting for control plane pods ready after helm upgrade")
        self.k8s_utils._exec_kubectl(
            f"kubectl wait --for=condition=Ready pods --all -n {_NAMESPACE} "
            f"--timeout=300s --field-selector=status.phase!=Succeeded"
        )
        sleep_n_sec(15)
        self.k8s_utils.get_admin_pod(refresh=True)
        self.logger.info("Helm upgrade complete — control plane updated")

    def _run_rolling_upgrade(self, storage_node_list: list[dict]):
        """R26+ rolling upgrade: helm upgrade + per-node CRD restart."""
        self.FIO_RUNTIME = 3600  # 1 hour — FIO runs throughout

        # Pre-upgrade: create PVCs, FIO, snapshots, clones
        self.logger.info("Step 2: Creating StorageClass and VolumeSnapshotClass")
        pool_name = self.pool_name
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name)
        if actual_pool and actual_pool != pool_name:
            pool_name = actual_pool
        sleep_n_sec(10)
        self._create_storage_classes(self.cluster_id, pool_name)

        self.logger.info("Step 3: Creating PVCs and starting FIO Jobs")
        self._create_pvcs_with_fio(len(storage_node_list))

        self.logger.info("Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones()

        self.logger.info("Step 5: Waiting 60s for FIO to establish baseline")
        sleep_n_sec(60)

        # Helm upgrade
        self.logger.info("Step 6: Running helm upgrade for control plane")
        self._helm_upgrade()
        sleep_n_sec(30)

        # Rolling restart
        self.logger.info("Step 7: Rolling storage node restart with new images")
        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]

        for idx, node in enumerate(storage_node_list):
            node_id = node["id"]
            self.logger.info(
                f"Step 7.{idx + 1}: Restarting node {node_id} "
                f"({idx + 1}/{len(storage_node_list)})"
            )
            restart_ts = int(datetime.now().timestamp())

            ops_name, _ = self.k8s_utils.patch_storage_node_restart(
                node_uuid=node_id,
                spdk_image=self.target_spdk_image or None,
                spdk_proxy_image=self.target_spdk_proxy_image or None,
            )

            # Wait for the StorageNodeOps CR to reach Succeeded
            self.k8s_utils.wait_storage_node_ops_done(ops_name, timeout=600)
            self.k8s_utils.wait_spdk_pods_ready(
                expected_count=len(storage_node_list), timeout=600
            )
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node_id, status="online", timeout=600,
            )
            self.logger.info(f"Node {node_id} is back online")

            sleep_n_sec(30)
            self.validate_migration_for_node(
                restart_ts, 1200, node_id, 60, no_task_ok=True
            )
            if idx < len(storage_node_list) - 1:
                sleep_n_sec(30)

        self.logger.info("All storage nodes restarted successfully")
        self.runner_k8s_log.restart_logging()

        # Post-upgrade validation
        self.logger.info("Step 8: Post-upgrade validation")
        self._assert_all_nodes_healthy()
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=300,
        )

        fio_timeout = self.FIO_RUNTIME + 300
        self._validate_all_fio(fio_timeout)
        self.logger.info("All pre-upgrade FIO jobs validated successfully")

        self.logger.info("Step 9: Post-upgrade new PVC verification")
        self._run_post_upgrade_verification()

    # ══════════════════════════════════════════════════════════════════════════
    # MAINTENANCE WINDOW UPGRADE (R25→R26)
    # ══════════════════════════════════════════════════════════════════════════

    def _annotate_fdb_keep(self):
        """Step 1: Add helm.sh/resource-policy: keep to FDB resources."""
        self.logger.info("Migration Step 1: Annotating FDB resources with keep policy")

        # First try upgrading the existing release to add the annotation via helm
        # (as described in the guide). Fall back to direct annotation if the
        # sbcli release doesn't exist (operator-deployed clusters).
        for kind, name in _FDB_KEEP_RESOURCES:
            ns_flag = f"-n {_NAMESPACE}" if kind not in ("clusterrole", "clusterrolebinding") else ""
            cmd = (
                f"kubectl annotate {kind} {name} {ns_flag} "
                f"helm.sh/resource-policy=keep --overwrite 2>/dev/null || true"
            )
            self.k8s_utils._exec_kubectl(cmd)
        self.logger.info("FDB resources annotated with keep policy")

    def _shutdown_all_nodes(self, storage_node_list: list[dict]):
        """Step 2 / 6.1: Suspend + shutdown all storage nodes."""
        self.logger.info(f"Shutting down all {len(storage_node_list)} storage nodes")
        for node in storage_node_list:
            node_id = node["id"]
            self.logger.info(f"  Suspending node {node_id}")
            try:
                self.sbcli_utils.suspend_node(node_id)
            except Exception as e:
                self.logger.warning(f"  Suspend failed for {node_id}: {e}")

        sleep_n_sec(10)

        for node in storage_node_list:
            node_id = node["id"]
            self.logger.info(f"  Shutting down node {node_id}")
            try:
                self.sbcli_utils.shutdown_node(node_id)
            except Exception as e:
                self.logger.warning(f"  Shutdown failed for {node_id}: {e}")

        # Wait for all nodes to reach offline
        self.logger.info("Waiting for all nodes to reach offline status")
        for node in storage_node_list:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node["id"],
                status=["offline", "unavailable"],
                timeout=300,
            )
        self.logger.info("All storage nodes are offline")

    def _uninstall_helm_releases(self):
        """Steps 3-4: Uninstall old Helm charts."""
        if self.helm_release_spdk_csi:
            self.logger.info(
                f"Migration Step 3: Uninstalling helm release '{self.helm_release_spdk_csi}'"
            )
            self.k8s_utils._exec_kubectl(
                f"helm uninstall {self.helm_release_spdk_csi} "
                f"--namespace {_NAMESPACE} --wait 2>/dev/null || true"
            )
            sleep_n_sec(10)

        if self.helm_release_sbcli:
            self.logger.info(
                f"Migration Step 4: Uninstalling helm release '{self.helm_release_sbcli}'"
            )
            self.k8s_utils._exec_kubectl(
                f"helm uninstall {self.helm_release_sbcli} "
                f"--namespace {_NAMESPACE} --wait 2>/dev/null || true"
            )
            sleep_n_sec(10)

    def _create_upgrade_secret(self):
        """Step 5: Create the upgrade secret so the operator adopts the existing cluster."""
        secret_name = f"simplyblock-{self.cluster_cr_name}-upgrade"
        self.logger.info(f"Migration Step 5: Creating upgrade secret '{secret_name}'")

        # Delete if exists
        self.k8s_utils._exec_kubectl(
            f"kubectl delete secret {secret_name} -n {_NAMESPACE} 2>/dev/null || true"
        )

        cluster_id = self.cluster_id
        cluster_secret = self.cluster_secret

        cmd = (
            f"kubectl create secret generic {secret_name} "
            f"--namespace {_NAMESPACE} "
            f"--from-literal=uuid={cluster_id} "
            f"--from-literal=secret={cluster_secret}"
        )
        out, err = self.k8s_utils._exec_kubectl(cmd)
        self.logger.info(f"Upgrade secret created: {out}")

    def _install_operator_chart(self):
        """Step 6: Install the operator Helm chart with FDB disabled."""
        self.logger.info("Migration Step 6: Installing operator chart (FDB disabled)")

        tls_flags = ""
        if self.tls_enabled:
            tls_flags = "--set tls.enabled=true --set tls.mutual_enabled=true"

        csi_flags = ""
        if self.csi_repository:
            csi_flags += f" --set image.csi.repository={self.csi_repository}"
        if self.csi_tag:
            csi_flags += f" --set image.csi.tag={self.csi_tag}"

        helm_cmd = (
            f"helm upgrade --install simplyblock-operator {self.helm_chart_path} "
            f"--namespace {_NAMESPACE} "
            f"--timeout 10m "
            f"--set operator.enabled=true "
            f"--set controlplane.foundationdb.enabled=false "
            f"--set image.simplyblock.repository={self.simplyblock_repo} "
            f"--set image.simplyblock.tag={self.target_docker_image} "
            f"--set image.operator.repository={self.operator_repo} "
            f"--set image.operator.tag={self.operator_tag} "
            f"--set controlplane.csiHostpathDriver.enabled=true "
            f"--set controlplane.storageclass.name=local-hostpath "
            f"--set csiConfig.simplybk.ip=http://simplyblock-webappapi.simplyblock:5000 "
            f"{tls_flags} {csi_flags}"
        ).strip()

        out, err = self.k8s_utils._exec_kubectl(helm_cmd)
        self.logger.info(f"Helm install stdout: {out[:500] if out else ''}")
        if err and err.strip():
            self.logger.info(f"Helm install stderr: {err[:500]}")

        # Wait for operator pods to be ready
        self.logger.info("Waiting for operator pods to be ready")
        self.k8s_utils._exec_kubectl(
            f"kubectl wait --for=condition=Ready pods --all -n {_NAMESPACE} "
            f"--timeout=300s --field-selector=status.phase!=Succeeded"
        )
        sleep_n_sec(15)
        self.k8s_utils.get_admin_pod(refresh=True)
        self.logger.info("Operator chart installed")

    def _apply_custom_resources(self, storage_node_list: list[dict]):
        """Step 7: Apply StorageCluster, Pool, StorageNode CRs."""
        self.logger.info("Migration Step 7: Applying custom resources")

        # Build worker nodes YAML from the known storage nodes
        worker_yaml = ""
        worker_nodes_env = os.environ.get("WORKER_NODES", "")
        if worker_nodes_env:
            for node in worker_nodes_env.split(","):
                node = node.strip()
                if node:
                    worker_yaml += f"      - {node}\n"
        else:
            self.logger.warning("WORKER_NODES env not set, attempting to derive from K8s")
            out, _ = self.k8s_utils._exec_kubectl(
                "kubectl get nodes -l node-role.kubernetes.io/worker "
                "-o jsonpath='{.items[*].metadata.name}' 2>/dev/null || "
                "kubectl get nodes --no-headers -o custom-columns=NAME:.metadata.name"
            )
            for node_name in (out or "").replace("'", "").split():
                node_name = node_name.strip()
                if node_name:
                    worker_yaml += f"      - {node_name}\n"

        sb_repo = self.simplyblock_repo
        sb_tag = self.target_docker_image
        spdk_image = self.target_spdk_image
        mgmt_ifc = os.environ.get("MGMT_IFC", "ens18")
        data_nics = os.environ.get("DATA_NICS", "enp1s0")
        max_lvol = os.environ.get("MAX_LVOL", "30")

        cr_yaml = f"""
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: {self.cluster_cr_name}
  namespace: {_NAMESPACE}
spec:
  fabricType: tcp
  isSingleNode: false
  enableNodeAffinity: true
  strictNodeAntiAffinity: false
  stripe:
    dataChunks: {self.ndcs}
    parityChunks: {self.npcs}
  warningThreshold:
    capacity: 95
    provisionedCapacity: 97
  criticalThreshold:
    capacity: 96
    provisionedCapacity: 98
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: Pool
metadata:
  name: {self.pool_cr_name}
  namespace: {_NAMESPACE}
spec:
  clusterName: {self.cluster_cr_name}
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageNodeSet
metadata:
  name: {self.node_cr_name}
  namespace: {_NAMESPACE}
spec:
  clusterName: {self.cluster_cr_name}
  clusterImage: "{sb_repo}:{sb_tag}"
  spdkImage: "{spdk_image}"
  spdkProxyImage: "{sb_repo}:{sb_tag}"
  mgmtIfname: {mgmt_ifc}
  dataIfname:
    - {data_nics}
  maxLogicalVolumeCount: {max_lvol}
  enableCpuTopology: true
  workerNodes:
{worker_yaml}"""

        apply_cmd = f"cat <<'CREOF' | kubectl apply -f -\n{cr_yaml}\nCREOF"
        out, err = self.k8s_utils._exec_kubectl(apply_cmd)
        self.logger.info(f"CRs applied: {out}")
        sleep_n_sec(10)

    def _run_r25_to_r26_migration(self):
        """Step 8: Run the R25→R26 DB migration script via admin pod."""
        self.logger.info("Migration Step 8: Running R25->R26 DB migration script")
        script_escaped = _R25_R26_MIGRATION_SCRIPT.replace("'", "'\"'\"'")
        cmd = f"python3 -c '{script_escaped}'"
        out, err = self.k8s_utils.exec_sbcli(cmd)
        self.logger.info(f"Migration output: {out[:2000] if out else ''}")
        if err and err.strip():
            self.logger.warning(f"Migration stderr: {err[:500]}")
        if out and "done" in out.lower():
            self.logger.info("R25->R26 DB migration completed successfully")
        else:
            self.logger.warning("Migration script did not print 'done' — check output")

    def _patch_backend_cr_references(self, storage_node_list: list[dict]):
        """Step 9: Register CR details on backend objects via sbctl --dev set."""
        self.logger.info("Migration Step 9: Patching backend objects with CR references")

        sbcli = "sbctl"

        # Patch cluster
        cluster_id = self.cluster_id
        self.k8s_utils.exec_sbcli(
            f"{sbcli} --dev cluster set {cluster_id} cr_plural storageclusters"
        )
        self.k8s_utils.exec_sbcli(
            f"{sbcli} --dev cluster set {cluster_id} cr_namespace {_NAMESPACE}"
        )
        self.k8s_utils.exec_sbcli(
            f"{sbcli} --dev cluster set {cluster_id} cr_name {self.cluster_cr_name}"
        )
        self.logger.info(f"Cluster {cluster_id} CR refs patched")

        # Patch each storage node
        # TODO: Once confirmed with operator team, cr_plural may need to
        # change from "storagenodesets" to "storagenodes" and cr_name to
        # the individual StorageNode CR name (resolved via
        # k8s_utils.resolve_storage_node_cr_name()).
        for node in storage_node_list:
            node_id = node["id"]
            self.k8s_utils.exec_sbcli(
                f"{sbcli} --dev sn set {node_id} cr_plural storagenodesets"
            )
            self.k8s_utils.exec_sbcli(
                f"{sbcli} --dev sn set {node_id} cr_namespace {_NAMESPACE}"
            )
            self.k8s_utils.exec_sbcli(
                f"{sbcli} --dev sn set {node_id} cr_name {self.node_cr_name}"
            )
            self.logger.info(f"Storage node {node_id} CR refs patched")

    def _restart_nodes_sequentially(self, storage_node_list: list[dict]):
        """Step 10: Restart each storage node one at a time with new SPDK image."""
        self.logger.info(
            f"Migration Step 10: Restarting {len(storage_node_list)} nodes sequentially"
        )

        sbcli = "sbctl"
        for idx, node in enumerate(storage_node_list):
            node_id = node["id"]
            self.logger.info(
                f"  Restarting node {node_id} ({idx + 1}/{len(storage_node_list)})"
            )

            restart_ts = int(datetime.now().timestamp())

            spdk_flag = ""
            if self.target_spdk_image:
                spdk_flag = f" --spdk-image {self.target_spdk_image}"

            self.k8s_utils.exec_sbcli(
                f"{sbcli} -d --dev sn restart {node_id}{spdk_flag}"
            )

            # Wait for node online
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node_id, status="online", timeout=600,
            )
            self.logger.info(f"  Node {node_id} is back online")

            # Wait for cluster active before next node
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id, status="active", timeout=600,
            )

            # Wait for migration tasks
            sleep_n_sec(30)
            self.validate_migration_for_node(
                restart_ts, 1200, node_id, 60, no_task_ok=True
            )

            if idx < len(storage_node_list) - 1:
                sleep_n_sec(30)

        self.logger.info("All storage nodes restarted successfully")

    def _run_maintenance_upgrade(self, storage_node_list: list[dict]):
        """Full R25→R26 maintenance window upgrade path."""
        self.logger.info(
            "=" * 60 + "\n"
            "MAINTENANCE WINDOW UPGRADE (R25 -> R26)\n"
            "Volumes will be unavailable during migration steps.\n"
            + "=" * 60
        )

        # Pre-upgrade: short FIO to write + verify data, then stop
        self.logger.info("Pre-upgrade Step 2: Creating StorageClass / VolumeSnapshotClass")
        pool_name = self.pool_name
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name)
        if actual_pool and actual_pool != pool_name:
            pool_name = actual_pool
        sleep_n_sec(10)
        self._create_storage_classes(self.cluster_id, pool_name)

        pre_fio_runtime = 120  # 2 minutes — just write + verify data
        self.FIO_RUNTIME = pre_fio_runtime

        self.logger.info("Pre-upgrade Step 3: Creating PVCs and running short FIO")
        self._create_pvcs_with_fio(len(storage_node_list), runtime=pre_fio_runtime)

        self.logger.info("Pre-upgrade Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones(runtime=pre_fio_runtime)

        # Wait for pre-upgrade FIO to complete
        self.logger.info("Pre-upgrade: Waiting for FIO to complete before maintenance")
        fio_timeout = pre_fio_runtime + 300
        self._validate_all_fio(fio_timeout)
        self.logger.info("Pre-upgrade FIO completed and validated")

        # ── Begin maintenance window ──
        self.logger.info("=" * 40 + " MAINTENANCE WINDOW START " + "=" * 40)

        # Step 1: Annotate FDB resources
        self._annotate_fdb_keep()

        # Step 2: Shut down all storage nodes
        self.logger.info("Migration Step 2: Shutting down all storage nodes")
        self._shutdown_all_nodes(storage_node_list)

        # Steps 3-4: Uninstall old Helm charts
        self.logger.info("Migration Steps 3-4: Uninstalling old Helm releases")
        self._uninstall_helm_releases()

        # Step 5: Create upgrade secret
        self._create_upgrade_secret()

        # Step 6: Install new operator chart (FDB disabled)
        self._install_operator_chart()

        # Step 6.1: Shut down nodes again (prevent auto-restart)
        self.logger.info("Migration Step 6.1: Shutting down nodes again (prevent auto-restart)")
        # Re-fetch node list via admin pod (now available after operator install)
        sleep_n_sec(30)
        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        self._shutdown_all_nodes(storage_node_list)

        # Step 7: Apply CRs
        self._apply_custom_resources(storage_node_list)

        # Step 8: Run R25→R26 DB migration
        self._run_r25_to_r26_migration()
        sleep_n_sec(15)

        # Step 9: Patch backend CR references
        self._patch_backend_cr_references(storage_node_list)

        # Step 10: Restart storage nodes one at a time
        self._restart_nodes_sequentially(storage_node_list)

        # ── End maintenance window ──
        self.logger.info("=" * 40 + " MAINTENANCE WINDOW END " + "=" * 40)

        self.runner_k8s_log.restart_logging()

        # Verify cluster is active and all nodes healthy
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        self._assert_all_nodes_healthy()

        # Post-upgrade: run new FIO to verify IO works after migration
        self.logger.info("Post-upgrade: Verifying IO works after migration")
        self._run_post_upgrade_verification()

    # ── Main test flow ─────────────────────────────────────────────────────────

    def run(self):
        self.logger.info("Starting Test: K8s Native Major Upgrade")

        # Step 1: Verify cluster active and all nodes online
        self.logger.info("Step 1: Verify cluster active and all nodes online")
        cluster_details = self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=120,
        )
        self.logger.info(f"Cluster status: {cluster_details['status']}")

        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_node_list:
            assert node["status"] == "online", (
                f"Node {node['id']} not online before upgrade (status={node['status']})"
            )
        self.logger.info(f"All {len(storage_node_list)} storage nodes online")

        # Branch based on upgrade type
        if self._is_maintenance_window_upgrade():
            self.logger.info(
                "Detected R25->R26 upgrade — using MAINTENANCE WINDOW path"
            )
            self._run_maintenance_upgrade(storage_node_list)
        else:
            self.logger.info(
                "Detected standard upgrade — using ROLLING UPGRADE path (no downtime)"
            )
            self._run_rolling_upgrade(storage_node_list)

        # Final assertion
        self._assert_all_nodes_healthy()
        self.logger.info(
            f"K8s native upgrade test PASSED: "
            f"{self.base_version} -> {self.target_version}"
        )
        self.logger.info("TEST CASE PASSED !!!")
