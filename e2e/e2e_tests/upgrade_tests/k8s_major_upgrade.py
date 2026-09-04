"""
K8s-native major upgrade E2E test.

Supports two upgrade paths:

**R25 → R26 (maintenance window)**:
  Full Helm-to-Operator migration following the production upgrade guide:
  1. Patch Helm release secret to add ``helm.sh/resource-policy: keep`` to FDB resources
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

import json
import os
import random
import string
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Resources that need the keep annotation so they survive helm uninstall
# (Step 1 of migration guide: FDB + prometheus config)
# NOTE: FDB resources are NOT prefixed with the Helm release name.
# The prometheus configmap IS prefixed (e.g. "sbcli-simplyblock-prometheus-config").
# Use _get_keep_resources(release_name) to get the full list with correct names.
_FDB_KEEP_RESOURCES = [
    ("deployment", "simplyblock-fdb-controller-manager"),
    ("serviceaccount", "simplyblock-fdb-controller-manager"),
    ("clusterrole", "simplyblock-fdb-manager-role"),
    ("clusterrole", "simplyblock-fdb-manager-clusterrole"),
    ("rolebinding", "simplyblock-fdb-manager-rolebinding"),
    ("clusterrolebinding", "simplyblock-fdb-manager-clusterrolebinding"),
    ("foundationdbcluster", "simplyblock-fdb-cluster"),
    ("configmap", "simplyblock-fdb-cluster-config"),
]

# Base name for the prometheus configmap (gets prefixed with helm release name)
_PROMETHEUS_CM_BASE = "simplyblock-prometheus-config"


def _get_keep_resources(helm_release: str = "") -> list:
    """Return the full keep-resources list with correctly prefixed names.

    The prometheus configmap is created by Helm with the release prefix
    (e.g. 'sbcli-simplyblock-prometheus-config'), unlike FDB resources
    which use fixed names.
    """
    resources = list(_FDB_KEEP_RESOURCES)
    if helm_release:
        resources.append(("configmap", f"{helm_release}-{_PROMETHEUS_CM_BASE}"))
    else:
        resources.append(("configmap", _PROMETHEUS_CM_BASE))
    return resources

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
        self.helm_release_sbcli = os.environ.get("HELM_RELEASE_SBCLI", "sbcli")

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

    def _build_fio_config(
        self, name: str, runtime: int = None,
    ) -> tuple[str, str, dict]:
        """Build FIO main + warmup configs.

        Returns ``(main_config, warmup_config, metadata)`` where *metadata*
        contains ``run_id``, ``randseed``, ``bs``, and ``fio_size`` so the
        caller can later reconstruct a verify-only config for the same files.
        """
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

        # Warmup: sequential write with verify=md5 at the same bs/seed as
        # the main config.  This pre-fills every block in the file with valid
        # FIO verify headers so that a later verify_only pass (rw=read) can
        # verify the ENTIRE file, not just the ~3-4 % of blocks that randrw
        # happened to overwrite.
        warmup_config = (
            f"[global]\n"
            f"name={name}-warmup\n"
            f"filename_format=/spdkvol/fio-{run_id}.$jobnum\n"
            f"rw=write\n"
            f"bs={bs}\n"
            f"iodepth=32\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={self.fio_size}\n"
            f"numjobs={self.fio_num_jobs}\n"
            f"verify=md5\n"
            f"randseed={randseed}\n"
            f"\n"
            f"[job1]\n"
        )

        metadata = {
            "run_id": run_id,
            "randseed": randseed,
            "bs": bs,
            "fio_size": self.fio_size,
            "num_jobs": self.fio_num_jobs,
        }

        return main_config, warmup_config, metadata

    def _build_verify_only_fio_config(self, name: str, meta: dict) -> str:
        """Build a verify-only FIO config that replays the exact files/seed
        from a previous write run, confirming data integrity without writing."""
        return (
            f"[global]\n"
            f"name={name}-verify\n"
            f"filename_format=/spdkvol/fio-{meta['run_id']}.$jobnum\n"
            f"rw=read\n"
            f"bs={meta['bs']}\n"
            f"iodepth=1\n"
            f"direct=1\n"
            f"ioengine=libaio\n"
            f"size={meta['fio_size']}\n"
            f"numjobs={meta['num_jobs']}\n"
            f"verify=md5\n"
            f"verify_only\n"
            f"verify_dump=1\n"
            f"verify_fatal=1\n"
            f"randseed={meta['randseed']}\n"
            f"\n"
            f"[job1]\n"
        )

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
            # Pick a random SC; deduplicate when both names are identical.
            sc_choices = list(dict.fromkeys(
                [self.STORAGE_CLASS_NAME, self.XFS_STORAGE_CLASS_NAME]
            ))
            sc_name = random.choice(sc_choices)
            fs_type = (
                "xfs" if sc_name == self.XFS_STORAGE_CLASS_NAME else "ext4"
            )

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
            fio_config, warmup_config, fio_meta = self._build_fio_config(
                pvc_name, runtime=runtime,
            )
            detail["fio_meta"] = fio_meta
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

    def _create_snapshots_and_clones(self, runtime: int = None, skip_clone_fio: bool = False):
        """Create snapshots + clones, optionally with FIO on each clone."""
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

            if not skip_clone_fio:
                fio_config, warmup_config, _clone_meta = self._build_fio_config(
                    clone_name, runtime=runtime,
                )
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
                **({"fio_meta": _clone_meta} if not skip_clone_fio else {}),
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

    def _cleanup_fio_jobs_only(self, wait_for_termination: bool = True,
                               termination_timeout: int = 300):
        """Delete FIO jobs and configmaps but leave PVCs/snapshots/clones intact.

        Unlike ``k8s_utils.cleanup_stale_fio_resources()`` which also removes
        clone PVCs, snapshots, and test PVCs, this only removes the FIO
        workload resources so PVCs are freed for utility pod mounting.

        When *wait_for_termination* is True (default), waits for all FIO
        pods to fully terminate and gives CSI time to finish unmounting
        before returning.  This ensures no stale mounts remain.
        """
        ns = self.k8s_utils.namespace
        cmds = [
            # Delete FIO jobs by label
            f"kubectl delete jobs -n {ns} -l app=fio-benchmark --ignore-not-found",
            # Delete FIO configmaps
            (
                f"kubectl get configmaps -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null "
                f"| grep -E '^(fiocfg-|fio-cfg-)' "
                f"| xargs -r kubectl delete configmap -n {ns} --ignore-not-found"
            ),
        ]
        for cmd in cmds:
            try:
                self.k8s_utils._exec_kubectl(cmd)
            except Exception as exc:
                self.logger.warning(f"FIO job cleanup step failed: {exc}")
        self.logger.info("FIO jobs and configmaps deleted")

        if not wait_for_termination:
            return

        # Wait for all FIO pods to fully terminate
        import time
        deadline = time.time() + termination_timeout
        self.logger.info(
            f"Waiting up to {termination_timeout}s for FIO pods to terminate..."
        )
        while time.time() < deadline:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {ns} -l app=fio-benchmark "
                f"--no-headers 2>/dev/null || true",
                supress_logs=True,
            )
            pods = [line for line in out.strip().splitlines() if line.strip()]
            if not pods:
                self.logger.info("All FIO pods terminated")
                break
            self.logger.info(f"  {len(pods)} FIO pod(s) still terminating...")
            time.sleep(5)
        else:
            self.logger.warning(
                f"FIO pods did not fully terminate within {termination_timeout}s"
            )

        # Give CSI NodeUnstageVolume / NodeUnpublishVolume time to complete
        # after pods are gone — ensures no stale mounts remain on worker nodes
        self.logger.info("Waiting 60s for CSI unmount to complete...")
        time.sleep(60)

        self.logger.info("FIO jobs and pods cleaned up (PVCs preserved)")

    def _cleanup_worker_connections(self):
        """Unmount stale CSI volumes and disconnect NVMe-oF on every worker.

        After FIO pods are deleted and CSI has had time to unmount, there
        may still be stale mount-points or NVMe-oF connections left on
        worker nodes (especially if CSI cleanup didn't fully succeed).
        This method runs host-level cleanup on each worker via
        ``oc debug node/`` (OpenShift) or ``kubectl debug node/`` (K8s).

        Also deletes any lingering VolumeAttachment objects for upgrade
        PVCs so Kubernetes doesn't think the volumes are still attached.
        """
        import time

        # --- 1. Get worker node names ---
        worker_nodes_env = os.environ.get("WORKER_NODES", "")
        worker_names = []
        if worker_nodes_env:
            worker_names = [n.strip() for n in worker_nodes_env.split(",")
                           if n.strip()]
        else:
            out, _ = self.k8s_utils._exec_kubectl(
                "kubectl get nodes -l node-role.kubernetes.io/worker "
                "-o jsonpath='{.items[*].metadata.name}' 2>/dev/null || "
                "kubectl get nodes --no-headers "
                "-o custom-columns=NAME:.metadata.name",
                supress_logs=True,
            )
            worker_names = [n.strip() for n in (out or "").replace("'", "").split()
                            if n.strip()]

        if not worker_names:
            self.logger.warning("No worker nodes found, skipping connection cleanup")
            return

        self.logger.info(
            f"Cleaning stale mounts and NVMe connections on {len(worker_names)} "
            f"worker(s): {worker_names}"
        )

        is_openshift = self.k8s_utils.detect_openshift()

        # Host-level cleanup script:
        # 1) Find and unmount any simplyblock CSI volume mounts
        # 2) Disconnect all NVMe-oF subsystems
        # 3) Print nvme list to confirm all connections are gone
        # Try without sudo first; fall back to sudo if the command fails.
        # NOTE: The script is passed via shlex.quote() to avoid nested
        # quoting issues with oc debug / kubectl debug.
        import shlex
        cleanup_script = (
            'echo "=== CSI mounts before cleanup ==="; '
            "mount | grep kubernetes.io~csi || echo '(none)'; "
            'echo "=== NVMe devices before cleanup ==="; '
            "nvme list 2>/dev/null || sudo nvme list 2>/dev/null || echo '(nvme list failed)'; "
            'for mp in $(mount | grep kubernetes.io~csi | awk "{print \\$3}"); do '
            '  umount -f "$mp" 2>/dev/null || sudo umount -f "$mp" 2>/dev/null || true; '
            "done; "
            "nvme disconnect-all 2>/dev/null || sudo nvme disconnect-all 2>/dev/null || true; "
            'echo "=== NVMe devices after cleanup ==="; '
            "nvme list 2>/dev/null || sudo nvme list 2>/dev/null || echo '(nvme list failed)'; "
            "echo CLEANUP_DONE"
        )

        for node_name in worker_names:
            self.logger.info(f"  Cleaning worker: {node_name}")
            try:
                quoted = shlex.quote(cleanup_script)
                if is_openshift:
                    cmd = (
                        f"oc debug node/{node_name} "
                        f"-- chroot /host bash -c {quoted}"
                    )
                else:
                    cmd = (
                        f"kubectl debug node/{node_name} -q "
                        f"--image=busybox:latest -- chroot /host sh -c {quoted}"
                    )
                out, _ = self.k8s_utils._exec_kubectl(cmd, timeout=120)
                if "CLEANUP_DONE" in (out or ""):
                    self.logger.info(f"  Worker {node_name}: cleanup completed")
                else:
                    self.logger.warning(
                        f"  Worker {node_name}: cleanup may not have completed "
                        f"(output: {(out or '')[:200]})"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"  Worker {node_name}: cleanup failed: {exc}"
                )

        # --- 2. Delete stale VolumeAttachments for upgrade PVCs ---
        pvc_names = list(self.pvc_details.keys()) + list(self.clone_details.keys())
        if pvc_names:
            self.logger.info("Checking for stale VolumeAttachments...")
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    "kubectl get volumeattachments -o json 2>/dev/null || true",
                    supress_logs=True,
                )
                if out and out.strip():
                    va_data = json.loads(out)
                    for va in va_data.get("items", []):
                        pv_name = va.get("spec", {}).get("source", {}).get(
                            "persistentVolumeName", ""
                        )
                        va_name = va.get("metadata", {}).get("name", "")
                        # Check if this VA references one of our PVs
                        # PV names match PVC names in CSI provisioner
                        for pvc in pvc_names:
                            if pvc in pv_name:
                                self.logger.info(
                                    f"  Deleting stale VolumeAttachment: {va_name} "
                                    f"(PV: {pv_name})"
                                )
                                self.k8s_utils._exec_kubectl(
                                    f"kubectl delete volumeattachment {va_name} "
                                    f"--ignore-not-found",
                                )
                                break
            except Exception as exc:
                self.logger.warning(f"VolumeAttachment cleanup failed: {exc}")

        self.logger.info("Worker connection cleanup complete")
        # Brief pause for cleanup to propagate
        time.sleep(10)

    def _log_configmaps(self, label: str):
        """Log configmap names and save prometheus config YAML to NFS share."""
        try:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get configmap -n {_NAMESPACE} -o name 2>/dev/null || true"
            )
            self.logger.info(f"Configmaps ({label}):\n{out}")

            out_dir = os.path.join(self.docker_logs_path, "configmaps")
            os.makedirs(out_dir, exist_ok=True)

            # Save full list
            list_file = os.path.join(out_dir, f"{label}_configmap_list.txt")
            with open(list_file, "w") as f:
                f.write(out or "")

            # Save prometheus configmap YAML (try prefixed then unprefixed name)
            for cm_name in [
                f"{self.helm_release_sbcli}-simplyblock-prometheus-config",
                "simplyblock-prometheus-config",
            ]:
                yaml_out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get configmap {cm_name} -n {_NAMESPACE} "
                    f"-o yaml 2>/dev/null || true"
                )
                if yaml_out and "apiVersion" in yaml_out:
                    yaml_file = os.path.join(
                        out_dir, f"{label}_prometheus_config.yaml"
                    )
                    with open(yaml_file, "w") as f:
                        f.write(yaml_out)
                    self.logger.info(
                        f"Saved {cm_name} YAML to {yaml_file}"
                    )
                    break
        except Exception as e:
            self.logger.warning(f"Failed to log configmaps ({label}): {e}")

    def _migrate_prometheus_credentials(self):
        """Copy basic_auth credentials from old prometheus configmap to new one.

        After R25→R2x upgrade the old configmap (e.g. sbcli-simplyblock-prometheus-config)
        has the cluster credentials, but the new chart creates a fresh configmap
        (simplyblock-prometheus-config) with empty username/password.  The new chart
        also switches to https with mTLS, so we can't just swap configmaps — we
        need to inject the old credentials into the new configmap.
        """
        old_cm = f"{self.helm_release_sbcli}-{_PROMETHEUS_CM_BASE}"
        new_cm = _PROMETHEUS_CM_BASE
        self.logger.info(
            f"Migrating prometheus credentials: {old_cm} → {new_cm}"
        )

        try:
            # Read old configmap
            old_out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get configmap {old_cm} -n {_NAMESPACE} "
                f"-o jsonpath='{{.data.prometheus\\.yml}}' 2>/dev/null || true"
            )
            if not old_out or "basic_auth" not in old_out:
                self.logger.warning(
                    f"Old configmap {old_cm} not found or has no basic_auth, "
                    f"skipping credential migration"
                )
                return

            # Extract username and password from old config
            import re
            user_match = re.search(
                r"basic_auth:\s*\n\s*username:\s*['\"]?([^'\"\n]+)", old_out
            )
            pass_match = re.search(
                r"basic_auth:\s*\n\s*username:\s*[^\n]*\n\s*password:\s*['\"]?([^'\"\n]+)",
                old_out,
            )
            if not user_match or not pass_match:
                self.logger.warning(
                    "Could not parse username/password from old prometheus config"
                )
                return

            username = user_match.group(1).strip()
            password = pass_match.group(1).strip()
            self.logger.info(
                f"Extracted credentials from {old_cm}: "
                f"username={username[:8]}..."
            )

            # Read new configmap
            new_json, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get configmap {new_cm} -n {_NAMESPACE} "
                f"-o json 2>/dev/null || true"
            )
            if not new_json or "apiVersion" not in new_json:
                self.logger.warning(
                    f"New configmap {new_cm} not found, skipping credential migration"
                )
                return

            import json as json_mod
            cm_obj = json_mod.loads(new_json)
            prom_yml = cm_obj.get("data", {}).get("prometheus.yml", "")

            if not prom_yml:
                self.logger.warning(f"New configmap {new_cm} has no prometheus.yml")
                return

            # Replace empty username/password with old values
            # Handle both empty and quoted-empty forms
            prom_yml = re.sub(
                r"(username:)\s*['\"]?['\"]?\s*$",
                rf"\1 '{username}'",
                prom_yml,
                count=1,
                flags=re.MULTILINE,
            )
            prom_yml = re.sub(
                r"(password:)\s*['\"]?['\"]?\s*$",
                rf"\1 '{password}'",
                prom_yml,
                count=1,
                flags=re.MULTILINE,
            )

            cm_obj["data"]["prometheus.yml"] = prom_yml

            # Remove resourceVersion/uid to avoid conflicts, keep name/namespace
            cm_obj.get("metadata", {}).pop("resourceVersion", None)
            cm_obj.get("metadata", {}).pop("uid", None)
            cm_obj.get("metadata", {}).pop("creationTimestamp", None)
            cm_obj.get("metadata", {}).pop("managedFields", None)

            # Apply the updated configmap
            patched_json = json_mod.dumps(cm_obj)
            self.k8s_utils._exec_kubectl(
                f"echo '{patched_json}' | kubectl replace -f - -n {_NAMESPACE}"
            )
            self.logger.info(
                f"Injected credentials into {new_cm}, "
                f"restarting prometheus pod"
            )

            # Restart prometheus to pick up the new config
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pod simplyblock-prometheus-0 -n {_NAMESPACE} "
                f"--ignore-not-found"
            )
            sleep_n_sec(30)

        except Exception as e:
            self.logger.warning(f"Failed to migrate prometheus credentials: {e}")

    def _collect_worker_dmesg(self, label: str = ""):
        """Collect dmesg and journalctl from every worker node.

        Saves output to ``<docker_logs_path>/worker_dmesg/`` so that kernel-
        level NVMe connect/disconnect, filesystem mount, and I/O error events
        are captured even when the node reboots (journalctl persists).
        """
        import shlex

        worker_nodes_env = os.environ.get("WORKER_NODES", "")
        if worker_nodes_env:
            worker_names = [n.strip() for n in worker_nodes_env.split(",")
                           if n.strip()]
        else:
            out, _ = self.k8s_utils._exec_kubectl(
                "kubectl get nodes -l node-role.kubernetes.io/worker "
                "-o jsonpath='{.items[*].metadata.name}' 2>/dev/null || "
                "kubectl get nodes --no-headers "
                "-o custom-columns=NAME:.metadata.name",
                supress_logs=True,
            )
            worker_names = [n.strip() for n in (out or "").replace("'", "").split()
                            if n.strip()]

        if not worker_names:
            self.logger.warning("No worker nodes found, skipping dmesg collection")
            return

        suffix = f"_{label}" if label else ""
        out_dir = os.path.join(self.docker_logs_path, "worker_dmesg")
        os.makedirs(out_dir, exist_ok=True)

        is_openshift = self.k8s_utils.detect_openshift()

        collect_script = (
            'echo "=== dmesg (NVMe/ext4/xfs/IO) ==="; '
            "dmesg -T 2>/dev/null | "
            'grep -iE "nvme|ext4|xfs|Buffer I/O|no available path|'
            'shut down requested|mounted filesystem|unmounting|'
            'Removing ctrl|new ctrl|I/O error" || echo "(no matches)"; '
            'echo "=== journalctl -k (NVMe/IO, last 2h) ==="; '
            "journalctl -k --since '2 hours ago' --no-pager 2>/dev/null | "
            'grep -iE "nvme|ext4|xfs|Buffer I/O|no available path|'
            'shut down requested|mounted|I/O error" || echo "(no matches)"'
        )

        self.logger.info(
            f"Collecting dmesg/journalctl from {len(worker_names)} workers "
            f"(label={label or 'none'})"
        )

        for node_name in worker_names:
            try:
                quoted = shlex.quote(collect_script)
                if is_openshift:
                    cmd = (
                        f"oc debug node/{node_name} "
                        f"-- chroot /host bash -c {quoted}"
                    )
                else:
                    cmd = (
                        f"kubectl debug node/{node_name} -q "
                        f"--image=busybox:latest -- chroot /host sh -c {quoted}"
                    )
                out, _ = self.k8s_utils._exec_kubectl(cmd, timeout=120)
                fname = os.path.join(
                    out_dir, f"{node_name}{suffix}.log"
                )
                with open(fname, "w") as f:
                    f.write(out or "(empty)")
                self.logger.info(f"  {node_name}: dmesg saved ({len(out or '')} bytes)")
            except Exception as exc:
                self.logger.warning(
                    f"  {node_name}: dmesg collection failed: {exc}"
                )

    def _capture_pvc_checksums(self, pvc_names: list[str]) -> dict[str, dict]:
        """Capture MD5 checksums for all files on the given PVCs.

        Returns ``{pvc_name: {filepath: md5hash, ...}, ...}``.

        NOTE: Currently unused in the upgrade flow — FIO's built-in
        ``verify=md5`` + ``verify_only`` mode (Phase 4.1) is used instead.
        Kept for future use when unmount/remount verification is needed.
        """
        all_checksums = {}
        for pvc_name in pvc_names:
            pod_name = f"cksum-{pvc_name}"[:63]
            self.logger.info(f"Capturing checksums for PVC {pvc_name}")
            try:
                self.k8s_utils.create_utility_pod(pod_name, pvc_name)
                self.k8s_utils.wait_pod_running(pod_name)
                files = self.k8s_utils.find_files_in_pvc(pod_name)
                if files:
                    checksums = self.k8s_utils.generate_checksums_in_pvc(
                        pod_name, files,
                    )
                    all_checksums[pvc_name] = checksums
                    self.logger.info(
                        f"  {pvc_name}: captured {len(checksums)} file checksums"
                    )
                else:
                    self.logger.warning(f"  {pvc_name}: no files found on volume")
                    all_checksums[pvc_name] = {}
            except Exception as exc:
                self.logger.warning(
                    f"  Failed to capture checksums for {pvc_name}: {exc}"
                )
                all_checksums[pvc_name] = {}
            finally:
                try:
                    self.k8s_utils.delete_pod(pod_name, wait=True)
                except Exception:
                    pass
        return all_checksums

    def _verify_pvc_checksums(
        self, pre_checksums: dict[str, dict], label: str = "post-upgrade",
    ):
        """Verify that current PVC data matches previously captured checksums.

        Raises ``AssertionError`` if any checksum mismatch is found.

        NOTE: Currently unused in the upgrade flow — FIO's built-in
        ``verify=md5`` + ``verify_only`` mode (Phase 4.1) is used instead.
        Kept for future use when unmount/remount verification is needed.
        """
        mismatches = []
        for pvc_name, expected in pre_checksums.items():
            if not expected:
                self.logger.warning(
                    f"  Skipping {pvc_name} — no pre-upgrade checksums captured"
                )
                continue

            pod_name = f"verify-cksum-{pvc_name}"[:63]
            self.logger.info(f"Verifying checksums for PVC {pvc_name} ({label})")
            try:
                self.k8s_utils.create_utility_pod(pod_name, pvc_name)
                self.k8s_utils.wait_pod_running(pod_name)
                actual = self.k8s_utils.generate_checksums_in_pvc(
                    pod_name, list(expected.keys()),
                )
                for filepath, exp_hash in expected.items():
                    act_hash = actual.get(filepath)
                    if act_hash != exp_hash:
                        msg = (
                            f"MISMATCH {pvc_name}:{filepath} "
                            f"expected={exp_hash} actual={act_hash}"
                        )
                        self.logger.error(msg)
                        mismatches.append(msg)
                    else:
                        self.logger.info(
                            f"  {filepath}: {exp_hash} ✓"
                        )
            except Exception as exc:
                self.logger.warning(
                    f"  Failed to verify checksums for {pvc_name}: {exc}"
                )
            finally:
                try:
                    self.k8s_utils.delete_pod(pod_name, wait=True)
                except Exception:
                    pass

        if mismatches:
            raise AssertionError(
                f"Data integrity check failed ({label}): "
                + "; ".join(mismatches)
            )
        self.logger.info(f"All checksums verified ({label})")

    def _run_fio_on_clones(self, runtime: int = 60):
        """Run FIO on clone PVCs (after cleaning parent data from clone)."""
        clone_jobs = []
        for clone_name, detail in self.clone_details.items():
            clone_job = detail["job_name"]
            clone_cm = detail["configmap_name"]

            fio_config, warmup_config, _meta = self._build_fio_config(
                clone_name, runtime=runtime,
            )
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(
                clone_name, self.sbcli_utils,
            )
            self.k8s_utils.create_fio_job(
                job_name=clone_job, pvc_name=clone_name,
                configmap_name=clone_cm, fio_config=fio_config,
                image=self.FIO_IMAGE, avoid_node=avoid,
                warmup_config=warmup_config,
            )
            # Store FIO metadata so Phase 4.1 can build verify-only jobs
            detail["fio_meta"] = _meta
            clone_jobs.append((clone_job, clone_name))
            sleep_n_sec(5)

        # Wait for clone FIO with tolerance
        fio_timeout = runtime + 240  # runtime + 4 min buffer
        for job_name, clone_name in clone_jobs:
            try:
                self._save_fio_pod_logs(job_name, clone_name)
                self.k8s_utils.validate_fio_job(job_name, timeout=fio_timeout)
                self.logger.info(f"Clone FIO completed: {clone_name}")
            except Exception as exc:
                self.logger.warning(
                    f"Clone FIO did not complete for {clone_name}: {exc}. "
                    "Continuing — non-fatal."
                )

        # Clean up clone FIO jobs (preserve clone PVCs for checksums)
        self._cleanup_fio_jobs_only()
        sleep_n_sec(5)

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

        fio_cfg, warmup_cfg, _post_meta = self._build_fio_config(post_pvc, runtime=120)
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

        clone_fio, clone_warmup, _clone_meta = self._build_fio_config(post_clone, runtime=120)
        self.k8s_utils.create_fio_job(
            job_name=post_clone_job, pvc_name=post_clone,
            configmap_name=post_clone_cm, fio_config=clone_fio,
            image=self.FIO_IMAGE, avoid_node=avoid, warmup_config=clone_warmup,
        )

        self._save_fio_pod_logs(post_job, post_pvc)
        self.k8s_utils.validate_fio_job(post_job, timeout=600)
        self._save_fio_pod_logs(post_clone_job, post_clone)
        self.k8s_utils.validate_fio_job(post_clone_job, timeout=600)

    def _assert_all_nodes_healthy(self, timeout=120, interval=10):
        """Assert all storage nodes are online with health_check=True.

        After a maintenance upgrade the health_check field may remain
        None or False for a short period while the monitoring loop
        catches up.  Retry up to *timeout* seconds before failing.
        """
        from time import time as _now

        deadline = _now() + timeout
        while True:
            storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
            unhealthy = []
            for node in storage_node_list:
                if node["status"] != "online":
                    unhealthy.append(
                        f"Node {node['id']} not online (status={node['status']})"
                    )
                elif not node.get("health_check", False):
                    unhealthy.append(
                        f"Node {node['id']} health_check={node.get('health_check')}"
                    )

            if not unhealthy:
                self.logger.info("All storage nodes online and healthy")
                return

            if _now() >= deadline:
                for msg in unhealthy:
                    self.logger.error(msg)
                raise AssertionError(
                    f"{len(unhealthy)} node(s) unhealthy after {timeout}s: "
                    + "; ".join(unhealthy)
                )

            self.logger.info(
                f"Waiting for {len(unhealthy)} node(s) to become healthy, "
                f"retrying in {interval}s …"
            )
            sleep_n_sec(interval)

    # ── Phase 2.7: Capture pre-upgrade state ──────────────────────────────────

    def _capture_pre_upgrade_state(self):
        """Log complete cluster state before starting the upgrade (Phase 2.7)."""
        self.logger.info("=" * 40 + " PRE-UPGRADE STATE CAPTURE " + "=" * 40)

        # Cluster
        self.logger.info(f"Cluster UUID: {self.cluster_id}")
        self.logger.info("Cluster Secret: ***")

        # Storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        self.logger.info(f"Storage nodes ({len(storage_nodes)}):")
        for node in storage_nodes:
            self.logger.info(
                f"  Node {node['id']} — status={node['status']}, "
                f"hostname={node.get('hostname', 'N/A')}"
            )

        # Pools
        try:
            pools = self.sbcli_utils.list_storage_pools()
            self.logger.info(f"Storage pools: {pools}")
        except Exception as e:
            self.logger.warning(f"Could not list pools: {e}")

        # PVCs
        self.logger.info(f"Pre-upgrade PVCs ({len(self.pvc_details)}):")
        for pvc_name, detail in self.pvc_details.items():
            pv_name = self.k8s_utils.get_pvc_pv_name(pvc_name) or "N/A"
            self.logger.info(
                f"  PVC {pvc_name} -> PV {pv_name} "
                f"(SC={detail['storage_class']}, fs={detail['fs_type']})"
            )

        # Snapshots
        self.logger.info(f"Pre-upgrade snapshots ({len(self.snapshot_details)}):")
        for snap_name, detail in self.snapshot_details.items():
            self.logger.info(f"  Snapshot {snap_name} (source PVC: {detail['pvc_name']})")

        # Clones
        self.logger.info(f"Pre-upgrade clones ({len(self.clone_details)}):")
        for clone_name, detail in self.clone_details.items():
            self.logger.info(
                f"  Clone {clone_name} (from snapshot: {detail['snap_name']})"
            )

        # Lvols
        try:
            self.sbcli_utils.list_lvols()
        except Exception as e:
            self.logger.warning(f"Could not list lvols: {e}")

        self.logger.info("=" * 40 + " END PRE-UPGRADE STATE " + "=" * 40)

    # ── Phase 4.1–4.3: Verify old data post-upgrade ──────────────────────────

    def _verify_old_data_post_upgrade(self):
        """Verify pre-upgrade data survives the upgrade (Phases 4.1–4.3).

        4.1 — FIO verify-only on old PVCs (confirms data integrity)
        4.2 — Fresh randrw FIO on old PVCs (confirms IO works)
        4.3 — New snapshots + clones on old PVCs post-upgrade
        """
        self.logger.info(
            "Post-upgrade Phase 4.1: Verify old data integrity (FIO verify-only)"
        )

        # 4.1 — Verify-only FIO on each pre-upgrade PVC AND clone PVC
        # We verify both originals and clones, collecting all failures
        # instead of stopping on the first error.
        verify_jobs: list[tuple[str, str, str]] = []  # (job, pvc, type)

        # Original PVCs
        for pvc_name, detail in self.pvc_details.items():
            fio_meta = detail.get("fio_meta")
            if not fio_meta:
                self.logger.warning(
                    f"No FIO metadata for PVC {pvc_name}, skipping verify-only"
                )
                continue

            verify_job = f"verify-{pvc_name}"
            verify_cm = f"fio-verify-cfg-{pvc_name}"

            verify_config = self._build_verify_only_fio_config(pvc_name, fio_meta)
            self.k8s_utils.create_fio_job(
                job_name=verify_job, pvc_name=pvc_name,
                configmap_name=verify_cm, fio_config=verify_config,
                image=self.FIO_IMAGE,
            )
            verify_jobs.append((verify_job, pvc_name, "original"))
            sleep_n_sec(5)

        # Clone PVCs
        for clone_name, detail in self.clone_details.items():
            fio_meta = detail.get("fio_meta")
            if not fio_meta:
                self.logger.warning(
                    f"No FIO metadata for clone {clone_name}, skipping verify-only"
                )
                continue

            verify_job = f"verify-{clone_name}"
            verify_cm = f"fio-verify-cfg-{clone_name}"

            verify_config = self._build_verify_only_fio_config(
                clone_name, fio_meta,
            )
            self.k8s_utils.create_fio_job(
                job_name=verify_job, pvc_name=clone_name,
                configmap_name=verify_cm, fio_config=verify_config,
                image=self.FIO_IMAGE,
            )
            verify_jobs.append((verify_job, clone_name, "clone"))
            sleep_n_sec(5)

        # Validate ALL verify jobs in parallel to avoid sequential timeouts
        verify_failures: list[str] = []

        def _validate_one(job_name, pvc_name, pvc_type):
            self.logger.info(
                f"Validating verify-only FIO for {pvc_type} PVC: {pvc_name}"
            )
            self._save_fio_pod_logs(job_name, f"{pvc_name}-verify")
            try:
                self.k8s_utils.validate_fio_job(job_name, timeout=600)
                self.logger.info(
                    f"  PASSED: {pvc_type} PVC {pvc_name} data verified"
                )
                return None
            except Exception as exc:
                self.logger.error(
                    f"  FAILED: {pvc_type} PVC {pvc_name} verification "
                    f"failed: {exc}"
                )
                return f"{pvc_type} PVC '{pvc_name}': {exc}"

        with ThreadPoolExecutor(max_workers=len(verify_jobs)) as pool:
            futures = {
                pool.submit(_validate_one, job, pvc, ptype): (job, pvc, ptype)
                for job, pvc, ptype in verify_jobs
            }
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    verify_failures.append(result)

        if verify_failures:
            summary = "\n  ".join(verify_failures)
            self.logger.error(
                f"Phase 4.1: {len(verify_failures)}/{len(verify_jobs)} "
                f"PVC verifications failed:\n  {summary}"
            )
            raise RuntimeError(
                f"Phase 4.1: {len(verify_failures)}/{len(verify_jobs)} "
                f"PVC verifications failed:\n  {summary}"
            )

        self.logger.info(
            f"Post-upgrade Phase 4.1 PASSED: All {len(verify_jobs)} PVC "
            f"data verified intact (originals + clones)"
        )

        # 4.2 — Fresh randrw FIO on old PVCs
        self.logger.info(
            "Post-upgrade Phase 4.2: Fresh FIO on old PVCs (confirm IO works)"
        )
        fresh_jobs: list[tuple[str, str]] = []
        for pvc_name, detail in self.pvc_details.items():
            fresh_job = f"post-io-{pvc_name}"
            fresh_cm = f"fio-post-io-cfg-{pvc_name}"

            fio_config, warmup_config, _meta = self._build_fio_config(
                pvc_name, runtime=120,
            )
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(
                pvc_name, self.sbcli_utils,
            )
            self.k8s_utils.create_fio_job(
                job_name=fresh_job, pvc_name=pvc_name,
                configmap_name=fresh_cm, fio_config=fio_config,
                image=self.FIO_IMAGE, avoid_node=avoid,
                warmup_config=warmup_config,
            )
            fresh_jobs.append((fresh_job, pvc_name))
            sleep_n_sec(5)

        for job_name, pvc_name in fresh_jobs:
            self.logger.info(f"Validating fresh FIO for PVC: {pvc_name}")
            self._save_fio_pod_logs(job_name, f"{pvc_name}-post-io")
            self.k8s_utils.validate_fio_job(job_name, timeout=600)

        self.logger.info(
            "Post-upgrade Phase 4.2 PASSED: Fresh IO on old PVCs succeeded"
        )

        # 4.3 — New snapshots + clones on old PVCs
        #
        # The CSI controller pod was (re)created during Step 6 of the
        # maintenance window.  By the time we reach Phase 4.3 the
        # external-provisioner sidecar's Kubernetes watches may have gone
        # stale (observed as "Watch close" events followed by zero
        # provisioning activity).  Bouncing the pod gives it fresh watches
        # so clone PVC provisioning actually triggers.
        self.logger.info(
            "Phase 4.3 prep: Restarting CSI controller pod to refresh "
            "provisioner watches"
        )
        try:
            self.k8s_utils.delete_pod(
                "simplyblock-csi-controller-0", wait=True,
            )
            self.k8s_utils.wait_pod_ready(
                "simplyblock-csi-controller", timeout=300,
            )
            sleep_n_sec(15)  # let provisioner establish new watches
        except Exception as exc:
            self.logger.warning(
                f"CSI controller restart failed (continuing): {exc}"
            )

        self.logger.info(
            "Post-upgrade Phase 4.3: New snapshots and clones on old PVCs"
        )
        post_clone_jobs: list[tuple[str, str]] = []
        for pvc_name, detail in self.pvc_details.items():
            post_snap = f"post-snap-{pvc_name}"
            post_clone = f"post-clone-{pvc_name}"
            post_clone_job = f"fio-{post_clone}"
            post_clone_cm = f"fio-cfg-{post_clone}"

            self.k8s_utils.create_volume_snapshot(
                name=post_snap, pvc_name=pvc_name,
                snapshot_class=self.SNAPSHOT_CLASS_NAME,
            )
            self.k8s_utils.wait_volume_snapshot_ready(post_snap, timeout=300)

            clone_sc = detail.get("storage_class", self.STORAGE_CLASS_NAME)
            self.k8s_utils.create_clone_pvc(
                name=post_clone, size=self.pvc_size,
                storage_class=clone_sc, snapshot_name=post_snap,
            )
            self.k8s_utils.wait_pvc_bound(post_clone, timeout=300)

            clone_fio, clone_warmup, _meta = self._build_fio_config(
                post_clone, runtime=120,
            )
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(
                post_clone, self.sbcli_utils,
            )
            self.k8s_utils.create_fio_job(
                job_name=post_clone_job, pvc_name=post_clone,
                configmap_name=post_clone_cm, fio_config=clone_fio,
                image=self.FIO_IMAGE, avoid_node=avoid,
                warmup_config=clone_warmup,
            )
            post_clone_jobs.append((post_clone_job, post_clone))
            sleep_n_sec(5)

        for job_name, clone_name in post_clone_jobs:
            self.logger.info(f"Validating post-upgrade clone FIO: {clone_name}")
            self._save_fio_pod_logs(job_name, clone_name)
            self.k8s_utils.validate_fio_job(job_name, timeout=600)

        self.logger.info(
            "Post-upgrade Phase 4.3 PASSED: Snapshots + clones on old PVCs work"
        )

    # ── Phase 4.6: Node outage test ───────────────────────────────────────────

    def _run_node_outage_test(self):
        """Verify HA works post-upgrade by shutting down a non-primary node
        while FIO is running (Phase 4.6)."""
        self.logger.info("Post-upgrade Phase 4.6: Node outage test")

        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        if len(storage_node_list) < 2:
            self.logger.warning(
                "Only 1 storage node — skipping node outage test "
                "(need at least 2 nodes for HA validation)"
            )
            return

        # Create a PVC and start a long FIO job
        outage_pvc = f"outage-pvc-{_rand_seq(4)}"
        outage_job = f"fio-{outage_pvc}"
        outage_cm = f"fio-cfg-{outage_pvc}"

        self.k8s_utils.create_pvc(
            name=outage_pvc, size=self.pvc_size,
            storage_class=self.STORAGE_CLASS_NAME,
        )
        self.k8s_utils.wait_pvc_bound(outage_pvc, timeout=300)

        fio_config, warmup_config, _meta = self._build_fio_config(
            outage_pvc, runtime=300,
        )
        avoid = self.k8s_utils.get_pvc_primary_k8s_node(
            outage_pvc, self.sbcli_utils,
        )
        self.k8s_utils.create_fio_job(
            job_name=outage_job, pvc_name=outage_pvc,
            configmap_name=outage_cm, fio_config=fio_config,
            image=self.FIO_IMAGE, avoid_node=avoid,
            warmup_config=warmup_config,
        )

        # Wait for FIO to start running
        self.logger.info("Waiting for FIO to establish baseline before node outage")
        sleep_n_sec(30)

        # Find the primary node for this PVC and pick a different one to shut down
        primary_node_id = None
        try:
            vol_handle = self.k8s_utils.get_pvc_volume_handle(outage_pvc)
            if vol_handle:
                lvol_id = vol_handle.split(":")[-1] if ":" in vol_handle else vol_handle
                lvol_details = self.sbcli_utils.get_lvol_details(lvol_id)
                primary_node_id = lvol_details.get("node_id")
        except Exception as e:
            self.logger.warning(f"Could not determine primary node: {e}")

        # Pick a non-primary node to shut down
        victim_node = None
        for node in storage_node_list:
            if node["id"] != primary_node_id and node["status"] == "online":
                victim_node = node
                break

        if not victim_node:
            self.logger.warning(
                "Could not find a non-primary node to shut down, "
                "skipping node outage test"
            )
            self._save_fio_pod_logs(outage_job, outage_pvc)
            self.k8s_utils.validate_fio_job(outage_job, timeout=600)
            return

        victim_id = victim_node["id"]
        self.logger.info(
            f"Shutting down non-primary node {victim_id} "
            f"(primary={primary_node_id})"
        )

        # Shut down the victim node
        try:
            self.sbcli_utils.suspend_node(victim_id)
        except Exception as e:
            self.logger.warning(f"Suspend failed for {victim_id}: {e}")
        sleep_n_sec(10)
        try:
            self.sbcli_utils.shutdown_node(victim_id)
        except Exception as e:
            self.logger.warning(f"Shutdown failed for {victim_id}: {e}")

        self.sbcli_utils.wait_for_storage_node_status(
            node_id=victim_id,
            status=["offline", "unavailable"],
            timeout=300,
        )
        self.logger.info(f"Node {victim_id} is offline, FIO should continue")

        # Verify FIO is still running
        sleep_n_sec(30)

        # Restart the victim node
        self.logger.info(f"Restarting node {victim_id}")
        try:
            self.sbcli_utils.restart_node(victim_id)
        except Exception as e:
            self.logger.warning(f"Restart failed for {victim_id}: {e}")

        self.sbcli_utils.wait_for_storage_node_status(
            node_id=victim_id, status="online", timeout=600,
        )
        self.logger.info(f"Node {victim_id} is back online")

        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )

        # Validate FIO completed successfully
        self._save_fio_pod_logs(outage_job, outage_pvc)
        self.k8s_utils.validate_fio_job(outage_job, timeout=600)
        self.logger.info("Post-upgrade Phase 4.6 PASSED: Node outage test succeeded")

    # ── Phase 4.7: Final checklist ────────────────────────────────────────────

    def _run_final_checklist(self, is_maintenance_upgrade: bool = False):
        """Run the final validation checklist (Phase 4.7)."""
        self.logger.info("Post-upgrade Phase 4.7: Final checklist")

        # Cluster active
        cluster_details = self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=120,
        )
        self.logger.info(f"  Cluster status: {cluster_details['status']} ✓")

        # All nodes online
        self._assert_all_nodes_healthy()
        self.logger.info("  All storage nodes online ✓")

        # All PVCs bound
        ns = self.k8s_utils.namespace
        for pvc_name in self.pvc_details:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pvc {pvc_name} -n {ns} -o jsonpath='{{.status.phase}}'"
            )
            phase = (out or "").strip().replace("'", "")
            assert phase == "Bound", (
                f"PVC {pvc_name} not Bound (phase={phase})"
            )
        self.logger.info(f"  All {len(self.pvc_details)} pre-upgrade PVCs Bound ✓")

        # Snapshots ready
        for snap_name in self.snapshot_details:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get volumesnapshot {snap_name} -n {ns} "
                f"-o jsonpath='{{.status.readyToUse}}'"
            )
            ready = (out or "").strip().replace("'", "")
            assert ready == "true", (
                f"Snapshot {snap_name} not ready (readyToUse={ready})"
            )
        self.logger.info(
            f"  All {len(self.snapshot_details)} snapshots readyToUse ✓"
        )

        # CR refs patched (R25→R26 only)
        if is_maintenance_upgrade:
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get storagecluster {self.cluster_cr_name} "
                    f"-n {_NAMESPACE} -o jsonpath='{{.status.uuid}}'"
                )
                cr_uuid = (out or "").strip().replace("'", "")
                if cr_uuid:
                    self.logger.info(
                        f"  StorageCluster CR adopted with UUID={cr_uuid} ✓"
                    )
                else:
                    self.logger.warning(
                        "  StorageCluster CR UUID not populated in status"
                    )
            except Exception as e:
                self.logger.warning(f"  Could not verify CR adoption: {e}")

        self.logger.info("Post-upgrade Phase 4.7 PASSED: Final checklist complete")

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
            f"--set controlplane.observability.enabled=true "
            f"--set opensearch.persistence.storageClass=local-hostpath "
            f"--set controlplane.observability.minio.storageClass=local-hostpath "

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

        # Pool CR name must match the existing backend pool name so the
        # operator can adopt it during the upgrade.
        self.pool_cr_name = pool_name
        self.logger.info(
            f"Pool CR name set to '{self.pool_cr_name}' (matching backend pool)"
        )

        sleep_n_sec(10)
        self._create_storage_classes(self.cluster_id, pool_name)

        self.logger.info("Step 3: Creating PVCs and starting FIO Jobs")
        self._create_pvcs_with_fio(len(storage_node_list))

        self.logger.info("Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones()

        # Phase 2.7: Capture pre-upgrade state
        self._capture_pre_upgrade_state()

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
                restart_ts, 1200, None, 60, no_task_ok=True
            )
            if idx < len(storage_node_list) - 1:
                sleep_n_sec(30)

        self.logger.info("All storage nodes restarted successfully")
        self.runner_k8s_log.restart_logging()

        # Step 7b/7c: activate v2 write protection, then restart again
        self._switch_write_protection_and_restart(
            storage_node_list, label="Step 7b")

        # Post-upgrade validation
        self.logger.info("Step 8: Post-upgrade validation")
        self._assert_all_nodes_healthy()
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=300,
        )

        fio_timeout = self.FIO_RUNTIME + 300
        self._validate_all_fio(fio_timeout)
        self.logger.info("All pre-upgrade FIO jobs validated successfully")

        # Phase 4.1–4.3: Verify old data survives the upgrade
        self.logger.info("Step 9: Verifying old data integrity post-upgrade")
        self._verify_old_data_post_upgrade()

        # Phase 4.4–4.5: New PVC provisioning + snapshot/clone
        self.logger.info("Step 10: Post-upgrade new PVC verification")
        self._run_post_upgrade_verification()

        # Phase 4.6: Node outage test
        self._run_node_outage_test()

        # Phase 4.7: Final checklist
        self._run_final_checklist(is_maintenance_upgrade=False)

    # ══════════════════════════════════════════════════════════════════════════
    # MAINTENANCE WINDOW UPGRADE (R25→R26)
    # ══════════════════════════════════════════════════════════════════════════

    def _annotate_fdb_keep(self):
        """Step 1: Add helm.sh/resource-policy: keep to FDB resources.

        IMPORTANT: ``kubectl annotate`` on live resources does NOT protect
        against ``helm uninstall``. Helm reads annotations from its stored
        release manifest, not from the live object in etcd.

        Primary approach (Option A): Edit the chart template files on disk
        to add ``helm.sh/resource-policy: keep`` annotations, then run
        ``helm upgrade --reuse-values`` to persist the annotations into
        Helm's stored release manifest.

        Fallback (Option B): If the chart path is not available, patch the
        Helm release secret directly (decode base64→gzip→JSON, inject
        annotations, re-encode).
        """
        self.logger.info("Migration Step 1: Adding keep policy to FDB resources")

        success = False

        # Option A: Edit chart files on disk + helm upgrade --reuse-values
        r25_chart_path = os.environ.get("R25_CHART_PATH", "")
        if r25_chart_path and os.path.isdir(r25_chart_path):
            success = self._inject_keep_annotations_via_helm_upgrade(r25_chart_path)
        else:
            self.logger.info(
                f"R25_CHART_PATH not set or not found ('{r25_chart_path}'), "
                f"trying Helm release secret patch"
            )

        # Option B: Patch Helm release secret directly (fallback)
        if not success:
            self.logger.info("Falling back to Helm release secret patching")
            self._patch_helm_release_keep_annotations(self.helm_release_sbcli)

        # Also annotate live resources (belt-and-suspenders, not sufficient alone)
        keep_resources = _get_keep_resources(self.helm_release_sbcli)
        for kind, name in keep_resources:
            ns_flag = f"-n {_NAMESPACE}" if kind not in ("clusterrole", "clusterrolebinding") else ""
            cmd = (
                f"kubectl annotate {kind} {name} {ns_flag} "
                f"helm.sh/resource-policy=keep --overwrite 2>/dev/null || true"
            )
            self.k8s_utils._exec_kubectl(cmd)
        self.logger.info("FDB keep annotation step complete")

    def _inject_keep_annotations_via_helm_upgrade(self, chart_path: str) -> bool:
        """Edit chart template files on disk and run helm upgrade --reuse-values.

        This is the correct way to add keep annotations: modify the chart
        templates so Helm stores the annotation in its release manifest,
        then ``helm uninstall`` will see it and skip deletion.

        Scans ALL template files in the chart for matching resource names
        (FDB resources, prometheus config, etc.) rather than just a single
        template, since resources may live in different template files.

        Returns True if successful, False otherwise.
        """
        import glob
        templates_dir = os.path.join(chart_path, "templates")
        if not os.path.isdir(templates_dir):
            self.logger.warning(f"Templates directory not found at {templates_dir}")
            return False

        template_files = sorted(glob.glob(os.path.join(templates_dir, "*.yaml")))
        if not template_files:
            self.logger.warning(f"No YAML templates found in {templates_dir}")
            return False

        self.logger.info(f"Scanning {len(template_files)} template files in {templates_dir}")

        try:
            import re
            # For template matching, use unprefixed names (Helm templates
            # contain the base name, the release prefix is added at render time)
            keep_names = {name for _, name in _FDB_KEEP_RESOURCES}
            keep_names.add(_PROMETHEUS_CM_BASE)
            remaining_names = set(keep_names)

            for template_file in template_files:
                with open(template_file, "r") as f:
                    content = f.read()

                # Check which keep-resources exist in this template
                # Match literal "name: <exact>" OR name appearing anywhere
                # in file (e.g. Helm variable defs like printf "%s-<name>")
                names_in_file = set()
                for n in remaining_names:
                    if f"name: {n}" in content or n in content:
                        names_in_file.add(n)
                if not names_in_file:
                    continue

                self.logger.info(f"Editing template: {template_file}")
                modified = False

                for name in names_in_file:
                    # Try literal name first
                    pattern = rf'(metadata:\n)(  name: {re.escape(name)}\n)'
                    content, count = re.subn(pattern, (
                        r'\1  annotations:\n'
                        r'    "helm.sh/resource-policy": keep\n'
                        r'\2'
                    ), content)
                    if count > 0:
                        self.logger.info(f"  Injected keep annotation for: {name}")
                        modified = True
                        remaining_names.discard(name)
                        continue

                    # Fallback: match metadata + name with Helm template expression
                    # e.g. "metadata:\n  name: {{ $name }}"
                    pattern = r'(metadata:\n)(  name: \{{.*\}}\n)'
                    content, count = re.subn(pattern, (
                        r'\1  annotations:\n'
                        r'    "helm.sh/resource-policy": keep\n'
                        r'\2'
                    ), content, count=1)
                    if count > 0:
                        self.logger.info(
                            f"  Injected keep annotation for: {name} "
                            f"(Helm template variable)"
                        )
                        modified = True
                        remaining_names.discard(name)

                if modified:
                    with open(template_file, "w") as f:
                        f.write(content)

            if remaining_names:
                self.logger.warning(
                    f"Could not find templates for: {remaining_names}"
                )

            # Run helm upgrade --reuse-values to persist annotations
            self.logger.info(
                f"Running helm upgrade --reuse-values for '{self.helm_release_sbcli}'"
            )
            cmd = (
                f"helm upgrade {self.helm_release_sbcli} {chart_path} "
                f"--namespace {_NAMESPACE} --reuse-values --timeout 5m"
            )
            out, _ = self.k8s_utils._exec_kubectl(cmd)
            self.logger.info(f"Helm upgrade result: {out[:500] if out else '(empty)'}")

            # Verify the annotation is in the stored manifest
            verify_cmd = (
                f"helm get manifest {self.helm_release_sbcli} -n {_NAMESPACE} "
                f"2>/dev/null | grep -c 'resource-policy' || echo '0'"
            )
            verify_out, _ = self.k8s_utils._exec_kubectl(verify_cmd)
            annotation_count = int(verify_out.strip() or "0")
            self.logger.info(
                f"Verified: {annotation_count} resource-policy annotations "
                f"in stored manifest"
            )
            return annotation_count > 0

        except Exception as e:
            self.logger.warning(f"Failed to inject keep annotations via helm upgrade: {e}")
            return False

    def _patch_helm_release_keep_annotations(
        self, release_name: str, resource_names: set[str] | None = None,
    ):
        """Patch the Helm release secret to inject resource-policy: keep.

        Helm stores release data in secrets named sh.helm.release.v1.<name>.v<N>.
        The data is: base64 → base64 → gzip → JSON. We decode, inject the keep
        annotation into matching resource manifests, and re-encode.

        *resource_names* overrides the default FDB resource list when provided,
        allowing this method to be reused for other charts (e.g. spdk-csi
        StorageClasses).
        """
        if not release_name:
            self.logger.warning("No Helm release name provided, skipping secret patch")
            return

        fdb_resource_names = (
            resource_names
            if resource_names is not None
            else {name for _, name in _get_keep_resources(release_name)}
        )

        # Find the latest Helm release secret
        cmd = (
            f"kubectl get secrets -n {_NAMESPACE} "
            f"-l owner=helm,name={release_name} "
            f"--sort-by=.metadata.creationTimestamp "
            f"-o jsonpath='{{.items[-1].metadata.name}}' 2>/dev/null || true"
        )
        out, _ = self.k8s_utils._exec_kubectl(cmd)
        secret_name = out.strip().strip("'")
        if not secret_name:
            self.logger.warning(f"No Helm release secret found for '{release_name}', skipping patch")
            return

        self.logger.info(f"Patching Helm release secret: {secret_name}")

        # Read the release data from the secret
        cmd = (
            f"kubectl get secret {secret_name} -n {_NAMESPACE} "
            f"-o jsonpath='{{.data.release}}' 2>/dev/null || true"
        )
        out, _ = self.k8s_utils._exec_kubectl(cmd)
        raw = out.strip().strip("'")
        if not raw:
            self.logger.warning("Could not read Helm release secret data, skipping patch")
            return

        try:
            import base64
            import gzip

            # Helm release encoding: base64 → base64 → gzip → JSON
            decoded = gzip.decompress(base64.b64decode(base64.b64decode(raw)))
            release = json.loads(decoded)

            manifest = release.get("manifest", "")
            if not manifest:
                self.logger.warning("No manifest in Helm release, skipping patch")
                return

            # Split manifest into individual YAML documents
            docs = manifest.split("\n---\n")
            patched = False

            new_docs = []
            for doc in docs:
                # Check if this document is an FDB resource by looking for its name
                matched_name = None
                for fname in fdb_resource_names:
                    if f"name: {fname}" in doc:
                        matched_name = fname
                        break

                if matched_name and "helm.sh/resource-policy" not in doc:
                    # Inject the keep annotation
                    if "  annotations:" in doc:
                        doc = doc.replace(
                            "  annotations:\n",
                            '  annotations:\n    "helm.sh/resource-policy": keep\n',
                            1,
                        )
                    else:
                        doc = doc.replace(
                            "metadata:\n",
                            'metadata:\n  annotations:\n    "helm.sh/resource-policy": keep\n',
                            1,
                        )
                    patched = True
                    self.logger.info(f"  Injected keep annotation for: {matched_name}")

                new_docs.append(doc)

            if not patched:
                self.logger.info("No FDB resources needed patching (already have keep annotation or not found in manifest)")
                return

            release["manifest"] = "\n---\n".join(new_docs)

            # Re-encode: JSON → gzip → base64 → base64
            compressed = gzip.compress(json.dumps(release).encode())
            encoded = base64.b64encode(base64.b64encode(compressed)).decode()

            # Patch the secret
            patch_json = json.dumps({"data": {"release": encoded}})
            cmd = (
                f"kubectl patch secret {secret_name} -n {_NAMESPACE} "
                f"--type=merge -p '{patch_json}'"
            )
            self.k8s_utils._exec_kubectl(cmd)
            self.logger.info("Helm release secret patched successfully")

        except Exception as e:
            self.logger.warning(f"Failed to patch Helm release secret: {e}")
            self.logger.warning("FDB resources may be deleted by helm uninstall")

    def _shutdown_all_nodes(self, storage_node_list: list[dict]):
        """Step 2 / 6.1: Force-shutdown all storage nodes.

        Suspend is skipped because it fails with "Offline storage nodes
        found, cannot suspend node without --force" when any node is
        already offline (e.g. during Step 6.1 after operator install).
        Using ``shutdown --force`` bypasses the suspended-state check.
        """
        self.logger.info(f"Shutting down all {len(storage_node_list)} storage nodes (force)")
        for node in storage_node_list:
            node_id = node["id"]
            self.logger.info(f"  Shutting down node {node_id} (force=True)")
            try:
                self.sbcli_utils.shutdown_node(node_id, force=True)
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
            # Preserve StorageClasses across helm uninstall so existing
            # SC references (in PVCs, snapshots, clones) remain valid
            # after the upgrade.  The new operator chart may create its
            # own SCs, but we keep the old ones for backward compat.
            self.logger.info(
                "Preserving StorageClasses: patching spdk-csi Helm release "
                "manifest + annotating live resources"
            )
            self._patch_helm_release_keep_annotations(
                self.helm_release_spdk_csi,
                resource_names={self.STORAGE_CLASS_NAME},
            )
            # Belt-and-suspenders: also annotate live resources directly
            self.k8s_utils._exec_kubectl(
                f"kubectl annotate storageclass "
                f"--selector=app.kubernetes.io/instance={self.helm_release_spdk_csi} "
                f"helm.sh/resource-policy=keep --overwrite 2>/dev/null || true"
            )
            self.k8s_utils._exec_kubectl(
                f"kubectl annotate storageclass {self.STORAGE_CLASS_NAME} "
                f"helm.sh/resource-policy=keep --overwrite 2>/dev/null || true"
            )

            self.logger.info(
                f"Migration Step 3: Uninstalling helm release '{self.helm_release_spdk_csi}'"
            )
            self.k8s_utils._exec_kubectl(
                f"helm uninstall {self.helm_release_spdk_csi} "
                f"--namespace {_NAMESPACE} --wait 2>/dev/null || true"
            )
            sleep_n_sec(10)

            # Delete resources that survived helm uninstall due to
            # helm.sh/resource-policy: keep (e.g. simplyblock-snapshot-controller)
            self._cleanup_kept_spdk_csi_resources()

        if self.helm_release_sbcli:
            # Capture FDB cluster-config BEFORE uninstall (in case keep fails)
            fdb_cm_data = self._capture_fdb_cluster_config()

            self.logger.info(
                f"Migration Step 4: Uninstalling helm release '{self.helm_release_sbcli}'"
            )
            self.k8s_utils._exec_kubectl(
                f"helm uninstall {self.helm_release_sbcli} "
                f"--namespace {_NAMESPACE} --wait 2>/dev/null || true"
            )
            sleep_n_sec(10)

            # Verify FDB cluster-config ConfigMap survived; recreate if missing
            self._ensure_fdb_cluster_config(fdb_cm_data)

    def _capture_fdb_cluster_config(self) -> str:
        """Capture the FDB cluster file content before helm uninstall.

        Returns the cluster file data string, or empty string if unavailable.
        """
        try:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get configmap simplyblock-fdb-cluster-config "
                f"-n {_NAMESPACE} -o jsonpath='{{.data.cluster-file}}' "
                f"2>/dev/null || true"
            )
            data = (out or "").replace("'", "").strip()
            if data:
                self.logger.info(
                    f"Captured FDB cluster-config data ({len(data)} chars)")
            return data
        except Exception as e:
            self.logger.warning(f"Failed to capture FDB cluster-config: {e}")
            return ""

    def _ensure_fdb_cluster_config(self, fdb_cm_data: str):
        """Ensure the FDB cluster-config ConfigMap exists after helm uninstall.

        The admin-control pods mount this ConfigMap as a volume.  If it was
        deleted during ``helm uninstall sbcli`` despite resource-policy:keep,
        recreate it from the previously captured data.  If no captured data is
        available, attempt to extract it from a running FDB pod.
        """
        # Check if ConfigMap still exists
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get configmap simplyblock-fdb-cluster-config "
            f"-n {_NAMESPACE} --no-headers 2>/dev/null || true"
        )
        if "simplyblock-fdb-cluster-config" in (out or ""):
            self.logger.info("FDB cluster-config ConfigMap survived helm uninstall")
            return

        self.logger.warning(
            "FDB cluster-config ConfigMap was deleted during helm uninstall — "
            "recreating it")

        # Try captured data first
        if not fdb_cm_data:
            # Fallback: extract from a running FDB pod
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get pods -n {_NAMESPACE} "
                    f"-l foundationdb.org/fdb-cluster-name=simplyblock-fdb-cluster "
                    f"--no-headers -o custom-columns=NAME:.metadata.name "
                    f"2>/dev/null | head -1"
                )
                fdb_pod = (out or "").strip()
                if fdb_pod:
                    out2, _ = self.k8s_utils._exec_kubectl(
                        f"kubectl exec {fdb_pod} -n {_NAMESPACE} "
                        f"-c foundationdb -- cat /var/fdb/data/fdb.cluster "
                        f"2>/dev/null || true"
                    )
                    fdb_cm_data = (out2 or "").strip()
                    if fdb_cm_data:
                        self.logger.info(
                            f"Extracted FDB cluster file from pod {fdb_pod}")
            except Exception as e:
                self.logger.warning(f"Failed to extract FDB data from pods: {e}")

        if not fdb_cm_data:
            self.logger.error(
                "Cannot recreate FDB cluster-config ConfigMap — no data "
                "available.  Admin pods will fail to start.")
            return

        # Recreate the ConfigMap
        # Escape single quotes in the data for the kubectl command
        escaped = fdb_cm_data.replace("'", "'\\''")
        self.k8s_utils._exec_kubectl(
            f"kubectl create configmap simplyblock-fdb-cluster-config "
            f"-n {_NAMESPACE} --from-literal=cluster-file='{escaped}'"
        )
        self.logger.info("Recreated FDB cluster-config ConfigMap")

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

    def _ensure_cert_manager(self):
        """Install cert-manager if not already present (required for TLS).

        R25 clusters don't have cert-manager since TLS wasn't supported.
        The target operator chart validates cert-manager CRDs when
        tls.enabled=true, so we install it here before helm install.

        If a stale/broken cert-manager release exists, uninstall it first
        and retry the install up to 3 times.
        """
        self.logger.info("Checking if cert-manager is installed")
        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get crd certificates.cert-manager.io 2>/dev/null || true"
        )
        if "certificates.cert-manager.io" in (out or ""):
            self.logger.info("cert-manager CRDs already present")
            return

        # Uninstall stale cert-manager if present from a previous failed run
        self.logger.info("Removing any stale cert-manager release")
        self.k8s_utils._exec_kubectl(
            "helm uninstall cert-manager -n cert-manager "
            "--no-hooks --timeout 60s 2>/dev/null || true"
        )

        self.logger.info("Installing cert-manager (TLS prerequisite)")
        self.k8s_utils._exec_kubectl(
            "helm repo add jetstack https://charts.jetstack.io 2>/dev/null || true"
        )
        self.k8s_utils._exec_kubectl("helm repo update")

        last_err = None
        for attempt in range(1, 4):
            self.logger.info(f"cert-manager install attempt {attempt}/3")
            out, err = self.k8s_utils._exec_kubectl(
                "helm upgrade --install cert-manager jetstack/cert-manager "
                "--namespace cert-manager --create-namespace "
                "--version v1.13.0 --set installCRDs=true"
            )
            if err and "Error" in err:
                last_err = err
                self.logger.warning(
                    f"cert-manager install attempt {attempt} failed: {err[:200]}"
                )
                self.k8s_utils._exec_kubectl(
                    "helm uninstall cert-manager -n cert-manager "
                    "--no-hooks --timeout 60s 2>/dev/null || true"
                )
                sleep_n_sec(10)
                continue
            last_err = None
            break

        if last_err:
            raise RuntimeError(
                f"cert-manager install failed after 3 attempts: {last_err[:500]}"
            )

        self.k8s_utils._exec_kubectl(
            "kubectl wait --for=condition=Ready pods --all "
            "-n cert-manager --timeout=120s"
        )
        self.logger.info("cert-manager installed and ready")

    def _cleanup_kept_spdk_csi_resources(self):
        """Delete resources that survived ``helm uninstall spdk-csi``.

        The old spdk-csi chart sets ``helm.sh/resource-policy: keep`` on
        certain resources (e.g. simplyblock-snapshot-controller Deployment
        in kube-system).  ``helm uninstall`` honours that policy and leaves
        them behind.  These orphaned resources still carry the old Helm
        ownership annotations (``meta.helm.sh/release-name: spdk-csi``),
        which prevents the new ``simplyblock-operator`` chart from creating
        its own version of the same resource.

        The fix is straightforward: delete the orphans so the new chart
        can recreate them cleanly.
        """
        self.logger.info("Cleaning up resources kept by spdk-csi resource-policy")

        # Known resources that the spdk-csi chart marks with resource-policy: keep
        # Format: (resource_type, name, namespace_or_None)
        kept_resources = [
            ("deployment", "simplyblock-snapshot-controller", "kube-system"),
        ]

        deleted = 0
        for rtype, name, ns in kept_resources:
            ns_flag = f"-n {ns}" if ns else ""
            # Check if it exists and belongs to spdk-csi
            check_cmd = (
                f"kubectl get {rtype} {name} {ns_flag} "
                f"-o jsonpath='{{.metadata.annotations.meta\\.helm\\.sh/release-name}}' "
                f"2>/dev/null || true"
            )
            out, _ = self.k8s_utils._exec_kubectl(check_cmd)
            release = (out or "").replace("'", "").strip()
            if release == "spdk-csi":
                self.logger.info(
                    f"  Deleting {rtype}/{name} in {ns or 'cluster-scope'} "
                    f"(orphaned from spdk-csi with resource-policy: keep)"
                )
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete {rtype} {name} {ns_flag} "
                    f"--ignore-not-found"
                )
                deleted += 1
            elif release:
                self.logger.info(
                    f"  {rtype}/{name} in {ns or 'cluster-scope'} belongs to "
                    f"release '{release}', not spdk-csi — skipping"
                )
            else:
                self.logger.info(
                    f"  {rtype}/{name} in {ns or 'cluster-scope'} not found or "
                    f"has no release annotation — skipping"
                )

        self.logger.info(f"Deleted {deleted} orphaned spdk-csi resource(s)")

    def _install_operator_chart(self):
        """Step 6: Install the operator Helm chart with FDB disabled."""
        self.logger.info("Migration Step 6: Installing operator chart (FDB disabled)")

        tls_flags = ""
        if self.tls_enabled:
            self._ensure_cert_manager()
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
            f"--set controlplane.observability.enabled=true "
            f"--set opensearch.persistence.storageClass=local-hostpath "
            f"--set controlplane.observability.minio.storageClass=local-hostpath "

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

        # Wait specifically for admin-control pods to be Ready
        self.logger.info("Waiting for admin-control pods to be Ready")
        for attempt in range(60):
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pods -n {_NAMESPACE} "
                f"-l app=simplyblock-admin-control "
                f"--no-headers 2>/dev/null || true"
            )
            lines = [ln for ln in (out or "").strip().split("\n") if ln.strip()]
            ready_count = sum(
                1 for ln in lines
                if "Running" in ln and ln.split()[1].split("/")[0] == ln.split()[1].split("/")[1]
            )
            if ready_count > 0:
                self.logger.info(
                    f"  {ready_count} admin-control pod(s) Ready")
                break
            # Check for ContainerCreating with volume mount failures
            if any("ContainerCreating" in ln for ln in lines) and attempt % 10 == 9:
                self.logger.warning(
                    f"  Admin pods still ContainerCreating after {(attempt+1)*5}s — "
                    f"checking events for volume mount issues")
                for ln in lines:
                    pod_name = ln.split()[0] if ln.split() else ""
                    if pod_name and "ContainerCreating" in ln:
                        ev_out, _ = self.k8s_utils._exec_kubectl(
                            f"kubectl get events -n {_NAMESPACE} "
                            f"--field-selector involvedObject.name={pod_name} "
                            f"--sort-by='.lastTimestamp' 2>/dev/null "
                            f"| tail -5 || true"
                        )
                        if ev_out:
                            self.logger.warning(f"  Events for {pod_name}:\n{ev_out}")
            sleep_n_sec(5)
        else:
            self.logger.error(
                "Admin-control pods did not become Ready within 300s")

        self.k8s_utils.get_admin_pod(refresh=True)
        self.logger.info("Operator chart installed")

    def _get_vcpu_count(self):
        """Compute vcpuCount for StorageCluster CR.

        Reads VCPU_COUNT env var if set; otherwise queries the first
        worker node's CPU count and applies CORE_PERCENTAGE (50% for
        OpenShift, 30% otherwise).  Falls back to 4 if detection fails.
        """
        vcpu_env = os.environ.get("VCPU_COUNT", "").strip()
        if vcpu_env:
            self.logger.info(f"Using VCPU_COUNT from env: {vcpu_env}")
            return int(vcpu_env)

        core_pct = 50 if self.k8s_utils.detect_openshift() else 30
        try:
            worker_nodes_env = os.environ.get("WORKER_NODES", "")
            if worker_nodes_env:
                first_worker = worker_nodes_env.split(",")[0].strip()
            else:
                out, _ = self.k8s_utils._exec_kubectl(
                    "kubectl get nodes -l node-role.kubernetes.io/worker "
                    "-o jsonpath='{.items[0].metadata.name}' 2>/dev/null"
                )
                first_worker = (out or "").replace("'", "").strip()

            if first_worker:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get node {first_worker} "
                    f"-o jsonpath='{{.status.capacity.cpu}}'"
                )
                total_cpus = int((out or "").replace("'", "").strip())
                vcpu = max(6, total_cpus * core_pct // 100)
                self.logger.info(
                    f"Computed vcpuCount={vcpu} "
                    f"({total_cpus} CPUs * {core_pct}%)"
                )
                return vcpu
        except Exception as e:
            self.logger.warning(f"Failed to detect CPU count: {e}")

        self.logger.warning("Could not determine CPU count, defaulting vcpuCount to 6")
        return 6

    def _apply_custom_resources(self, storage_node_list: list[dict]):
        """Step 7: Apply StorageCluster, Pool, StorageNode CRs."""
        self.logger.info("Migration Step 7: Applying custom resources")

        # Remove stale StoragePool CRDs from previous runs.  Their finalizers
        # block the operator reconciler with 404 errors because the backend
        # pools no longer exist after the re-install.
        self.logger.info("Cleaning up stale StoragePool CRDs")
        self.k8s_utils._exec_kubectl(
            f"kubectl get storagepool -n {_NAMESPACE} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null "
            f"| xargs -r -I{{}} kubectl patch storagepool {{}} -n {_NAMESPACE} "
            f"--type=merge -p '{{\"metadata\":{{\"finalizers\":[]}}}}' 2>/dev/null || true"
        )
        self.k8s_utils._exec_kubectl(
            f"kubectl delete storagepool --all -n {_NAMESPACE} "
            f"--wait=false 2>/dev/null || true"
        )
        sleep_n_sec(5)

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
        vcpu_count = self._get_vcpu_count()
        is_talos = self.k8s_utils.detect_talos()
        enable_cpu_topo = "false" if is_talos else "true"
        enable_cpu_topo_skip = "true" if is_talos else "false"

        cr_yaml = f"""
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: {self.cluster_cr_name}
  namespace: {_NAMESPACE}
spec:
  fabricType: tcp
  enableNodeAffinity: true
  stripe:
    dataChunks: {self.ndcs}
    parityChunks: {self.npcs}
  warningThreshold:
    capacity: 95
    provisionedCapacity: 97
  criticalThreshold:
    capacity: 96
    provisionedCapacity: 98
  maxSubsystemCount: {max_lvol}
  vcpuCount: {vcpu_count}
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StoragePool
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
  skipKubeletConfiguration: {enable_cpu_topo_skip}
  enableCpuTopology: {enable_cpu_topo}
  workerNodes:
{worker_yaml}"""

        apply_cmd = f"cat <<'CREOF' | kubectl apply -f -\n{cr_yaml}\nCREOF"
        out, err = self.k8s_utils._exec_kubectl(apply_cmd)
        self.logger.info(f"CRs applied (stdout): {out}")
        if err and err.strip():
            # Fail fast if critical CRs were rejected by the API server
            err_stripped = err.strip()
            if any(kw in err_stripped for kw in (
                "BadRequest", "strict decoding error", "NotFound",
                "could not find the requested resource",
                "is invalid", "Required value",
            )):
                raise RuntimeError(
                    f"CRs rejected by API server: {err_stripped}"
                )
            self.logger.warning(f"CRs apply stderr: {err_stripped}")
        # Verify critical CRs were actually created — fail early instead
        # of discovering a missing CR much later during node restart.
        for cr_kind, cr_name in [
            ("storagecluster", self.cluster_cr_name),
            ("storagepool", self.pool_cr_name),
            ("storagenodeset", self.node_cr_name),
        ]:
            chk_out, chk_err = self.k8s_utils._exec_kubectl(
                f"kubectl get {cr_kind} {cr_name} -n {_NAMESPACE} "
                f"-o jsonpath='{{.metadata.name}}'"
            )
            not_found = "not found" in (chk_err or "").lower()
            no_resource = "could not find the requested resource" in (chk_err or "").lower()
            if not_found or no_resource or cr_name not in (chk_out or ""):
                raise RuntimeError(
                    f"Critical CR {cr_kind}/{cr_name} was not created. "
                    f"kubectl get stderr: {chk_err}, "
                    f"kubectl apply stderr: {err}"
                )
            self.logger.info(f"  Verified {cr_kind}/{cr_name} exists")
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

    def _disable_auto_restart_all_nodes(self, storage_node_list: list[dict]):
        """Set auto_restart_disabled=true on all nodes.

        Called before installing the R26 operator so its tasks-runner
        won't create node_restart tasks for offline nodes.
        """
        self.logger.info(
            "Disabling auto-restart on all storage nodes "
            "(prevent operator restart tasks)"
        )
        sbcli = "sbcli-dev" if self.upgrade_type == "r25-to-r2x" else "sbctl"
        for node in storage_node_list:
            node_id = node["id"]
            try:
                self.k8s_utils.exec_sbcli(
                    f"{sbcli} --dev sn set {node_id} auto_restart_disabled true"
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to disable auto-restart for {node_id}: {e}"
                )

    def _cancel_stale_restart_tasks(self):
        """Cancel any running/new node_restart tasks before our explicit restart.

        The R26 operator's tasks-runner may have created restart tasks for
        offline nodes between Step 6 (operator install) and Step 6.1
        (second shutdown).  These stale tasks block ``sn restart``.
        """
        self.logger.info("Checking for stale node_restart tasks to cancel")
        sbcli = "sbctl"
        try:
            stdout, _ = self.k8s_utils.exec_sbcli(
                f"{sbcli} cluster list-tasks {self.cluster_id} --json --limit 0"
            )
            if not stdout or not stdout.strip():
                self.logger.info("No tasks found")
                return

            tasks = json.loads(stdout)
            stale = [
                t for t in tasks
                if t.get("function") == "node_restart"
                and t.get("status") in ("running", "new")
            ]
            if not stale:
                self.logger.info("No stale node_restart tasks found")
                return

            self.logger.info(f"Found {len(stale)} stale node_restart tasks — cancelling")
            for t in stale:
                task_id = t.get("id") or t.get("task_id") or t.get("uuid")
                target = t.get("target_id", "")
                self.logger.info(f"  Cancelling task {task_id} ({target})")
                try:
                    self.k8s_utils.exec_sbcli(
                        f"{sbcli} cluster cancel-task {self.cluster_id} {task_id}"
                    )
                except Exception as e:
                    self.logger.warning(f"  cancel-task failed for {task_id}: {e}")

            # Verify all cancelled
            sleep_n_sec(5)
            stdout2, _ = self.k8s_utils.exec_sbcli(
                f"{sbcli} cluster list-tasks {self.cluster_id} --json --limit 0"
            )
            if stdout2 and stdout2.strip():
                tasks2 = json.loads(stdout2)
                remaining = [
                    t for t in tasks2
                    if t.get("function") == "node_restart"
                    and t.get("status") in ("running", "new")
                ]
                if remaining:
                    self.logger.warning(
                        f"{len(remaining)} node_restart tasks still running after cancel"
                    )
                else:
                    self.logger.info("All stale node_restart tasks cancelled successfully")
        except Exception as e:
            self.logger.warning(f"Failed to list/cancel stale tasks: {e}")

    def _restart_nodes_sequentially(self, storage_node_list: list[dict]):
        """Step 10: Restart each storage node one at a time with new SPDK image.

        In the maintenance upgrade path all nodes start offline, so the
        cluster cannot become ``active`` until every node is back online.
        We therefore restart all nodes first (waiting only for each
        individual node to reach ``online``), then let the caller check
        cluster-active status after the loop.
        """
        self.logger.info(
            f"Migration Step 10: Restarting {len(storage_node_list)} nodes sequentially"
        )

        sbcli = "sbctl"
        for idx, node in enumerate(storage_node_list):
            node_id = node["id"]
            self.logger.info(
                f"  Restarting node {node_id} ({idx + 1}/{len(storage_node_list)})"
            )

            spdk_flag = ""
            if self.target_spdk_image:
                spdk_flag = f" --spdk-image {self.target_spdk_image}"
            proxy_flag = ""
            if self.target_spdk_proxy_image:
                proxy_flag = f" --spdk-proxy-image {self.target_spdk_proxy_image}"

            self.k8s_utils.exec_sbcli(
                f"{sbcli} -d --dev sn restart {node_id}{spdk_flag}{proxy_flag}"
            )

            # Wait for node online
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node_id, status="online", timeout=600,
            )
            self.logger.info(f"  Node {node_id} is back online")

            if idx < len(storage_node_list) - 1:
                sleep_n_sec(10)

        self.logger.info("All storage nodes restarted successfully")

    def _switch_write_protection_and_restart(self, storage_node_list,
                                             label="Post-upgrade"):
        """Activate v2 distrib write protection cluster-wide, then restart
        every storage node again with ``--force``.

        An upgraded cluster's existing distribs stay on **v1** write
        protection -- only freshly created clusters start on v2. ``sbctl
        cluster switch-write-protection`` sends the runtime RPC to every
        online node and records v2 only once they all accept it, so it has to
        run after the upgrade and after every node is back online. The second
        round of restarts then verifies the v2 generation was persisted and
        that nodes come back cleanly under it.

        ``--force`` is required on this second restart: the nodes are already
        online and healthy, so a plain restart is refused as unnecessary.

        Mirrors TEMP Step 11b/11c in the docker upgrade
        (``e2e_tests/upgrade_tests/major_upgrade.py``).
        """
        sbcli = "sbctl"
        self.logger.info(
            f"{label} Step A: sbctl cluster switch-write-protection "
            f"{self.cluster_id}"
        )
        out, err = self.k8s_utils.exec_sbcli(
            f"{sbcli} -d cluster switch-write-protection {self.cluster_id}"
        )
        blob = f"{out or ''}{err or ''}"
        assert "Error" not in blob and "Traceback" not in blob, (
            f"switch-write-protection failed: out={out!r} err={err!r}"
        )
        self.logger.info(f"  switch-write-protection: {(out or '').strip()}")
        sleep_n_sec(30)

        self.logger.info(
            f"{label} Step B: restarting all {len(storage_node_list)} storage "
            f"nodes again post-switch (--force)"
        )
        for idx, node in enumerate(storage_node_list):
            node_id = node["id"]
            self.logger.info(
                f"  Post-switch restart of node {node_id} "
                f"({idx + 1}/{len(storage_node_list)})"
            )
            restart_ts = int(datetime.now().timestamp()) - 120

            self.k8s_utils.exec_sbcli(
                f"{sbcli} -d --dev sn restart {node_id} --force"
            )
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node_id, status="online", timeout=1000,
            )
            self.k8s_utils.wait_spdk_pods_ready(
                expected_count=len(storage_node_list), timeout=600,
            )
            self.logger.info(f"  Node {node_id} back online post-switch")

            try:
                self.validate_migration_for_node(
                    restart_ts, 1800, None, 60, no_task_ok=True
                )
            except Exception as exc:
                self.logger.warning(
                    f"  Post-switch migration validation for {node_id}: {exc}"
                )

            if idx < len(storage_node_list) - 1:
                sleep_n_sec(30)

        self.logger.info(
            f"{label}: write-protection switched to v2 and all nodes "
            f"restarted successfully"
        )

    def _run_maintenance_upgrade(self, storage_node_list: list[dict]):
        """Full R25→R26 maintenance window upgrade path."""
        self.logger.info(
            "=" * 60 + "\n"
            "MAINTENANCE WINDOW UPGRADE (R25 -> R26)\n"
            "Volumes will be unavailable during migration steps.\n"
            + "=" * 60
        )

        # Pre-upgrade: short FIO to write + verify data, then stop
        self.logger.info("Pre-upgrade Step 2: Create pool (R25)")

        # R25 pool name must match logicalVolume.pool_name in the spdk-csi
        # helm chart (default: "testing1"). The chart's logicalVolume config
        # auto-created StorageClass "simplyblock-csi-sc" referencing this pool.
        pool_name = "testing1"
        # R25 has no operator — create pool directly via sbcli CLI, not StoragePool CRD
        actual_pool = self.sbcli_utils.add_storage_pool_direct(
            pool_name, sbcli_cmd="sbcli-dev"
        )
        if actual_pool and actual_pool != pool_name:
            pool_name = actual_pool

        # Pool CR name must match the existing backend pool name so the
        # operator can adopt it during the upgrade.
        self.pool_cr_name = pool_name
        self.logger.info(
            f"Pool CR name set to '{self.pool_cr_name}' (matching backend pool)"
        )

        sleep_n_sec(10)

        # R25: StorageClass "simplyblock-csi-sc" was auto-created by the
        # spdk-csi chart's logicalVolume config during helm install (ext4).
        # Create a separate XFS SC on the same pool to avoid the ext4
        # FEATURE_C12 incompatibility where R25's newer mkfs.ext4 creates
        # features that the host's e2fsck/tune2fs (v1.46.5) cannot handle
        # after upgrade.
        self.logger.info(
            "Creating XFS StorageClass on same pool to avoid ext4 "
            "FEATURE_C12 incompatibility during upgrade"
        )
        self.k8s_utils.create_storage_class(
            name=self.XFS_STORAGE_CLASS_NAME,
            cluster_id=self.cluster_id,
            pool_name=pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
            fs_type="xfs",
        )
        self.logger.info(
            f"Using chart ext4 SC '{self.STORAGE_CLASS_NAME}' + "
            f"new XFS SC '{self.XFS_STORAGE_CLASS_NAME}' "
            f"(same pool={pool_name})"
        )

        pre_fio_runtime = 60  # 1 minute — just write data before upgrade
        self.FIO_RUNTIME = pre_fio_runtime

        self.logger.info("Pre-upgrade Step 3: Creating PVCs and running short FIO")
        self._create_pvcs_with_fio(len(storage_node_list), runtime=pre_fio_runtime)

        # Wait for pre-upgrade FIO to complete on ALL PVCs.
        # This is mandatory — every lvol must have data written before we
        # create snapshots/clones and proceed with the upgrade.
        self.logger.info(
            "Pre-upgrade: Waiting for FIO to complete on all PVCs"
        )
        fio_timeout = 600  # 10 minutes — enough for 6 PVCs on a busy cluster
        self._validate_all_fio(fio_timeout)
        self.logger.info("Pre-upgrade FIO completed and validated on all PVCs")

        # Clean up FIO jobs/pods and wait for full termination + volume detach
        self.logger.info("Pre-upgrade: Cleaning up FIO jobs and waiting for clean unmount")
        self._cleanup_fio_jobs_only(wait_for_termination=True)

        self.logger.info("Pre-upgrade Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones(skip_clone_fio=True)

        # Run FIO on clones (writes fresh data to clones)
        self.logger.info("Pre-upgrade Step 4.1: Running FIO on clones")
        self._run_fio_on_clones(runtime=60)

        # Phase 2.7: Capture pre-upgrade state
        self._capture_pre_upgrade_state()

        # Phase 2.8: Clean stale mounts and NVMe connections on worker nodes
        # After all FIO pods are gone and CSI has unmounted, force-clean any
        # leftover mount-points and NVMe-oF connections on every worker node.
        # This prevents stale device references from causing I/O errors when
        # storage nodes are shut down and restarted during the upgrade.
        self.logger.info(
            "Pre-upgrade: Cleaning worker node connections "
            "(unmount + NVMe disconnect)"
        )
        self._cleanup_worker_connections()

        # Capture worker dmesg BEFORE upgrade for comparison
        try:
            self._collect_worker_dmesg(label="pre_upgrade")
        except Exception as exc:
            self.logger.warning(f"Pre-upgrade dmesg collection failed: {exc}")

        # ── Begin maintenance window ──
        self.logger.info("=" * 40 + " MAINTENANCE WINDOW START " + "=" * 40)

        # Step 1: Annotate FDB resources
        self._annotate_fdb_keep()

        # Log configmaps before helm uninstall (for debugging)
        self._log_configmaps("pre_uninstall")

        # Step 2: Shut down all storage nodes
        self.logger.info("Migration Step 2: Shutting down all storage nodes")
        self._shutdown_all_nodes(storage_node_list)

        # Step 2.1: Disable auto-restart — commented out, dev fixed the
        # operator's tasks-runner to not create restart tasks for offline nodes.
        # Uncomment if the product fix regresses.
        # self._disable_auto_restart_all_nodes(storage_node_list)

        # Steps 3-4: Uninstall old Helm charts
        self.logger.info("Migration Steps 3-4: Uninstalling old Helm releases")
        self._uninstall_helm_releases()

        # Step 5: Create upgrade secret
        self._create_upgrade_secret()

        # Step 6: Install new operator chart (FDB disabled)
        self._install_operator_chart()

        # Log configmaps after operator install (for debugging)
        self._log_configmaps("post_operator_install")

        # Step 6.0.1: Migrate prometheus credentials from old configmap to new
        self._migrate_prometheus_credentials()

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

        # Step 9.1: Cancel stale restart tasks — commented out, dev fixed
        # the operator to not create restart tasks during upgrade.
        # Uncomment if stale tasks reappear.
        # self._cancel_stale_restart_tasks()

        # Step 10: Restart storage nodes one at a time
        self._restart_nodes_sequentially(storage_node_list)

        # Wait for all SPDK pods ready and all nodes online before proceeding
        self.logger.info("Waiting for all SPDK pods to be ready")
        self.k8s_utils.wait_spdk_pods_ready(
            expected_count=len(storage_node_list), timeout=600,
        )
        self.logger.info("Waiting for all storage nodes to be online")
        for node in storage_node_list:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=node["id"], status="online", timeout=600,
            )

        # Step 10b/10c: activate v2 write protection cluster-wide, then
        # restart every node again to verify it persists.
        self._switch_write_protection_and_restart(
            storage_node_list, label="Step 10b")

        # ── End maintenance window ──
        self.logger.info("=" * 40 + " MAINTENANCE WINDOW END " + "=" * 40)

        self.runner_k8s_log.restart_logging()

        # Verify cluster is active and all nodes healthy
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        self._assert_all_nodes_healthy()

        # Capture worker dmesg AFTER upgrade for NVMe/IO error analysis
        try:
            self._collect_worker_dmesg(label="post_upgrade")
        except Exception as exc:
            self.logger.warning(f"Post-upgrade dmesg collection failed: {exc}")

        # Phase 4.1–4.3: Verify old data survives the upgrade
        self.logger.info("Post-upgrade: Verifying old data integrity")
        self._verify_old_data_post_upgrade()

        # Phase 4.4–4.5: New PVC provisioning + snapshot/clone
        self.logger.info("Post-upgrade: Verifying new provisioning works")
        self._run_post_upgrade_verification()

        # Phase 4.6: Node outage test
        self._run_node_outage_test()

        # Phase 4.7: Final checklist
        self._run_final_checklist(is_maintenance_upgrade=True)

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


class K8sNativeMajorUpgradeDualNode(K8sNativeMajorUpgrade):
    """K8s-native major upgrade for dual-node-per-host (nodesPerSocket=2).

    Each worker runs 2 logical storage nodes. Rolling restarts are grouped
    by worker so both nodes on a host are restarted together before moving
    to the next host.

    Dispatch the upgrade pipeline with:
      EXTRA_SN_ARGS: "--nodes-per-socket 2"  (bootstrap creates 2 nodes/host)
      TEST_CLASS: "K8sNativeMajorUpgradeDualNode"
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "k8s_native_major_upgrade_dual_node"
        self.nodes_per_socket = 2

    # ── Helpers ────────────────────────────────────────────────────────

    def _build_ip_to_node_ids(self):
        """Build IP -> [node_id, ...] mapping from the storage-node API."""
        sn_results = self.sbcli_utils.get_storage_nodes().get("results", [])
        ip_to_ids = {}
        for r in sn_results:
            nid = r.get("id") or r.get("uuid") or r.get("node_id")
            ip = r.get("ip") or r.get("mgmt_ip") or r.get("management_ip")
            if nid and ip:
                ip_to_ids.setdefault(ip, []).append(nid)
        return ip_to_ids

    def _get_unique_worker_ips(self, storage_node_list):
        """Extract unique worker IPs from node list, preserving order."""
        seen = set()
        unique = []
        for r in storage_node_list:
            ip = r.get("ip") or r.get("mgmt_ip") or r.get("management_ip")
            if ip and ip not in seen:
                seen.add(ip)
                unique.append(ip)
        return unique

    # ── Maintenance-window upgrade overrides ───────────────────────────

    def _apply_custom_resources(self, storage_node_list):
        """Override to include nodesPerSocket in StorageNodeSet spec."""
        self.logger.info(
            "Migration Step 7: Applying custom resources "
            f"(nodesPerSocket={self.nodes_per_socket})"
        )

        # Build worker nodes YAML from environment or K8s
        worker_yaml = ""
        worker_nodes_env = os.environ.get("WORKER_NODES", "")
        if worker_nodes_env:
            for node in worker_nodes_env.split(","):
                node = node.strip()
                if node:
                    worker_yaml += f"      - {node}\n"
        else:
            self.logger.warning(
                "WORKER_NODES env not set, attempting to derive from K8s"
            )
            out, _ = self.k8s_utils._exec_kubectl(
                "kubectl get nodes -l node-role.kubernetes.io/worker "
                "-o jsonpath='{.items[*].metadata.name}' 2>/dev/null || "
                "kubectl get nodes --no-headers "
                "-o custom-columns=NAME:.metadata.name"
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
        vcpu_count = self._get_vcpu_count()
        is_talos = self.k8s_utils.detect_talos()
        enable_cpu_topo = "false" if is_talos else "true"
        enable_cpu_topo_skip = "true" if is_talos else "false"

        cr_yaml = f"""
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: {self.cluster_cr_name}
  namespace: {_NAMESPACE}
spec:
  fabricType: tcp
  enableNodeAffinity: true
  stripe:
    dataChunks: {self.ndcs}
    parityChunks: {self.npcs}
  warningThreshold:
    capacity: 95
    provisionedCapacity: 97
  criticalThreshold:
    capacity: 96
    provisionedCapacity: 98
  maxSubsystemCount: {max_lvol}
  vcpuCount: {vcpu_count}
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StoragePool
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
  skipKubeletConfiguration: {enable_cpu_topo_skip}
  enableCpuTopology: {enable_cpu_topo}
  nodesPerSocket: {self.nodes_per_socket}
  workerNodes:
{worker_yaml}"""

        apply_cmd = f"cat <<'CREOF' | kubectl apply -f -\n{cr_yaml}\nCREOF"
        out, err = self.k8s_utils._exec_kubectl(apply_cmd)
        self.logger.info(f"CRs applied (stdout): {out}")
        if err and err.strip():
            err_stripped = err.strip()
            if any(kw in err_stripped for kw in (
                "BadRequest", "strict decoding error", "NotFound",
                "could not find the requested resource",
                "is invalid", "Required value",
            )):
                raise RuntimeError(
                    f"CRs rejected by API server: {err_stripped}"
                )
            self.logger.warning(f"CRs apply stderr: {err_stripped}")
        for cr_kind, cr_name in [
            ("storagecluster", self.cluster_cr_name),
            ("storagepool", self.pool_cr_name),
            ("storagenodeset", self.node_cr_name),
        ]:
            chk_out, chk_err = self.k8s_utils._exec_kubectl(
                f"kubectl get {cr_kind} {cr_name} -n {_NAMESPACE} "
                f"-o jsonpath='{{.metadata.name}}'"
            )
            not_found = "not found" in (chk_err or "").lower()
            no_resource = "could not find the requested resource" in (chk_err or "").lower()
            if not_found or no_resource or cr_name not in (chk_out or ""):
                raise RuntimeError(
                    f"Critical CR {cr_kind}/{cr_name} was not created. "
                    f"kubectl get stderr: {chk_err}, "
                    f"kubectl apply stderr: {err}"
                )
            self.logger.info(f"  Verified {cr_kind}/{cr_name} exists")
        sleep_n_sec(10)

    def _restart_nodes_sequentially(self, storage_node_list):
        """Override to group restarts by worker (both nodes per host together)."""
        ip_to_node_ids = self._build_ip_to_node_ids()
        unique_ips = self._get_unique_worker_ips(storage_node_list)

        self.logger.info(
            f"Migration Step 10: Restarting nodes on {len(unique_ips)} workers "
            f"(nodesPerSocket={self.nodes_per_socket})"
        )

        sbcli = "sbctl"
        for worker_idx, host_ip in enumerate(unique_ips, 1):
            nids = ip_to_node_ids.get(host_ip, [])
            self.logger.info(
                f"  Worker {worker_idx}/{len(unique_ips)} ({host_ip}): "
                f"restarting {len(nids)} nodes: {nids}"
            )

            # Restart all nodes on this worker
            for node_id in nids:
                spdk_flag = ""
                if self.target_spdk_image:
                    spdk_flag = f" --spdk-image {self.target_spdk_image}"
                proxy_flag = ""
                if self.target_spdk_proxy_image:
                    proxy_flag = (
                        f" --spdk-proxy-image {self.target_spdk_proxy_image}"
                    )
                self.k8s_utils.exec_sbcli(
                    f"{sbcli} -d --dev sn restart "
                    f"{node_id}{spdk_flag}{proxy_flag}"
                )

            # Wait for all nodes on this worker to come online
            for node_id in nids:
                self.sbcli_utils.wait_for_storage_node_status(
                    node_id=node_id, status="online", timeout=600,
                )
                self.logger.info(f"  Node {node_id} is back online")

            if worker_idx < len(unique_ips):
                sleep_n_sec(10)

        self.logger.info("All storage nodes restarted successfully")

    # ── Rolling upgrade override ───────────────────────────────────────

    def _run_rolling_upgrade(self, storage_node_list):
        """Override to group rolling restarts by worker node."""
        self.FIO_RUNTIME = 3600  # 1 hour — FIO runs throughout

        # Pre-upgrade: create PVCs, FIO, snapshots, clones (same as parent)
        self.logger.info("Step 2: Creating StorageClass and VolumeSnapshotClass")
        pool_name = self.pool_name
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name)
        if actual_pool and actual_pool != pool_name:
            pool_name = actual_pool

        self.pool_cr_name = pool_name
        self.logger.info(
            f"Pool CR name set to '{self.pool_cr_name}' (matching backend pool)"
        )

        sleep_n_sec(10)
        self._create_storage_classes(self.cluster_id, pool_name)

        self.logger.info("Step 3: Creating PVCs and starting FIO Jobs")
        self._create_pvcs_with_fio(len(storage_node_list))

        self.logger.info("Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones()

        # Capture pre-upgrade state
        self._capture_pre_upgrade_state()

        self.logger.info("Step 5: Waiting 60s for FIO to establish baseline")
        sleep_n_sec(60)

        # Helm upgrade
        self.logger.info("Step 6: Running helm upgrade for control plane")
        self._helm_upgrade()
        sleep_n_sec(30)

        # Rolling restart — grouped by worker
        self.logger.info(
            "Step 7: Rolling storage node restart "
            "(dual-node, grouped by worker)"
        )
        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        ip_to_node_ids = self._build_ip_to_node_ids()
        unique_ips = self._get_unique_worker_ips(storage_node_list)
        total_nodes = len(storage_node_list)

        for worker_idx, host_ip in enumerate(unique_ips, 1):
            nids = ip_to_node_ids.get(host_ip, [])
            self.logger.info(
                f"Step 7.{worker_idx}: Restarting worker {host_ip} "
                f"({worker_idx}/{len(unique_ips)}, "
                f"{len(nids)} nodes: {nids})"
            )

            restart_ts = int(datetime.now().timestamp())

            # Create StorageNodeOps for each node on this worker
            ops_names = []
            for node_id in nids:
                ops_name, _ = self.k8s_utils.patch_storage_node_restart(
                    node_uuid=node_id,
                    spdk_image=self.target_spdk_image or None,
                    spdk_proxy_image=self.target_spdk_proxy_image or None,
                )
                ops_names.append(ops_name)

            # Wait for all StorageNodeOps to complete
            for ops_name in ops_names:
                self.k8s_utils.wait_storage_node_ops_done(
                    ops_name, timeout=600
                )

            self.k8s_utils.wait_spdk_pods_ready(
                expected_count=total_nodes, timeout=600
            )

            # Wait for all nodes on this worker to come online
            for node_id in nids:
                self.sbcli_utils.wait_for_storage_node_status(
                    node_id=node_id, status="online", timeout=600,
                )
            self.logger.info(
                f"All {len(nids)} nodes on worker {host_ip} are back online"
            )

            sleep_n_sec(30)
            for node_id in nids:
                self.validate_migration_for_node(
                    restart_ts, 1200, None, 60, no_task_ok=True
                )

            if worker_idx < len(unique_ips):
                sleep_n_sec(30)

        self.logger.info("All storage nodes restarted successfully")
        self.runner_k8s_log.restart_logging()

        # Post-upgrade validation (same as parent)
        self.logger.info("Step 8: Post-upgrade validation")
        self._assert_all_nodes_healthy()
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=300,
        )

        fio_timeout = self.FIO_RUNTIME + 300
        self._validate_all_fio(fio_timeout)
        self.logger.info("All pre-upgrade FIO jobs validated successfully")

        # Verify old data survives upgrade
        self.logger.info("Step 9: Verifying old data integrity post-upgrade")
        self._verify_old_data_post_upgrade()

        # New PVC provisioning + snapshot/clone
        self.logger.info("Step 10: Post-upgrade new PVC verification")
        self._run_post_upgrade_verification()

        # Node outage test
        self._run_node_outage_test()

        # Final checklist
        self._run_final_checklist(is_maintenance_upgrade=False)
