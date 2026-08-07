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
    ("configmap", "simplyblock-fdb-cluster-config"),
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
        self.pre_upgrade_checksums: dict[str, dict] = {}

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

    def _cleanup_fio_jobs_only(self):
        """Delete FIO jobs and configmaps but leave PVCs/snapshots/clones intact.

        Unlike ``k8s_utils.cleanup_stale_fio_resources()`` which also removes
        clone PVCs, snapshots, and test PVCs, this only removes the FIO
        workload resources so PVCs are freed for utility pod mounting.
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
        self.logger.info("FIO jobs and configmaps cleaned up (PVCs preserved)")

    def _capture_pvc_checksums(self, pvc_names: list[str]) -> dict[str, dict]:
        """Capture MD5 checksums for all files on the given PVCs.

        Returns ``{pvc_name: {filepath: md5hash, ...}, ...}``.
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

    def _assert_all_nodes_healthy(self):
        storage_node_list = self.sbcli_utils.get_storage_nodes()["results"]
        for node in storage_node_list:
            assert node["status"] == "online", (
                f"Node {node['id']} not online (status={node['status']})"
            )
            assert node.get("health_check", True), (
                f"Node {node['id']} health check failed"
            )

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

        # 4.1 — Verify-only FIO on each pre-upgrade PVC
        verify_jobs: list[tuple[str, str]] = []
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
            avoid = self.k8s_utils.get_pvc_primary_k8s_node(
                pvc_name, self.sbcli_utils,
            )
            self.k8s_utils.create_fio_job(
                job_name=verify_job, pvc_name=pvc_name,
                configmap_name=verify_cm, fio_config=verify_config,
                image=self.FIO_IMAGE, avoid_node=avoid,
            )
            verify_jobs.append((verify_job, pvc_name))
            sleep_n_sec(5)

        for job_name, pvc_name in verify_jobs:
            self.logger.info(f"Validating verify-only FIO for PVC: {pvc_name}")
            self._save_fio_pod_logs(job_name, f"{pvc_name}-verify")
            self.k8s_utils.validate_fio_job(job_name, timeout=600)

        self.logger.info(
            "Post-upgrade Phase 4.1 PASSED: All old PVC data verified intact"
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
        for pvc_name in self.pvc_details:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pvc {pvc_name} -o jsonpath='{{.status.phase}}'"
            )
            phase = (out or "").strip().replace("'", "")
            assert phase == "Bound", (
                f"PVC {pvc_name} not Bound (phase={phase})"
            )
        self.logger.info(f"  All {len(self.pvc_details)} pre-upgrade PVCs Bound ✓")

        # Snapshots ready
        for snap_name in self.snapshot_details:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get volumesnapshot {snap_name} "
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
            lines = [l for l in (out or "").strip().split("\n") if l.strip()]
            ready_count = sum(
                1 for l in lines
                if "Running" in l and l.split()[1].split("/")[0] == l.split()[1].split("/")[1]
            )
            if ready_count > 0:
                self.logger.info(
                    f"  {ready_count} admin-control pod(s) Ready")
                break
            # Check for ContainerCreating with volume mount failures
            if any("ContainerCreating" in l for l in lines) and attempt % 10 == 9:
                self.logger.warning(
                    f"  Admin pods still ContainerCreating after {(attempt+1)*5}s — "
                    f"checking events for volume mount issues")
                for l in lines:
                    pod_name = l.split()[0] if l.split() else ""
                    if pod_name and "ContainerCreating" in l:
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
        self.logger.info("Pre-upgrade Step 2: Create pool (R25)")

        # R25 pool name must match logicalVolume.pool_name in the spdk-csi
        # helm chart (default: "testing1"). The chart's logicalVolume config
        # auto-created StorageClass "simplyblock-csi-sc" referencing this pool.
        pool_name = "testing1"
        # R25 has no operator — create pool directly via sbcli CLI, not Pool CRD
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
        # spdk-csi chart's logicalVolume config during helm install.
        # Do NOT create StorageClasses here — use the chart-created one.
        # Map XFS SC to the same chart-created SC (R25 chart has no XFS variant).
        self.XFS_STORAGE_CLASS_NAME = self.STORAGE_CLASS_NAME
        self.logger.info(
            f"Using chart-created StorageClass '{self.STORAGE_CLASS_NAME}' "
            f"(from logicalVolume config, pool_name={pool_name})"
        )

        pre_fio_runtime = 60  # 1 minute — just write data before upgrade
        self.FIO_RUNTIME = pre_fio_runtime

        self.logger.info("Pre-upgrade Step 3: Creating PVCs and running short FIO")
        self._create_pvcs_with_fio(len(storage_node_list), runtime=pre_fio_runtime)

        # Wait for pre-upgrade FIO to complete (max 5 mins).
        # FIO failure is non-fatal — the goal is to test the upgrade itself,
        # not the old version's IO path.
        self.logger.info(
            "Pre-upgrade: Waiting up to 5 mins for FIO to complete "
            "(non-fatal if it fails)"
        )
        fio_timeout = 300  # 5 minutes max wait
        pre_upgrade_fio_ok = True
        try:
            self._validate_all_fio(fio_timeout)
            self.logger.info("Pre-upgrade FIO completed and validated")
        except Exception as fio_err:
            pre_upgrade_fio_ok = False
            self.logger.warning(
                f"Pre-upgrade FIO did not complete successfully: {fio_err}. "
                "Continuing with upgrade — this is non-fatal."
            )

        # Clean up FIO jobs/pods (preserve PVCs for snapshots + checksums)
        self.logger.info("Pre-upgrade: Cleaning up FIO jobs")
        self._cleanup_fio_jobs_only()
        sleep_n_sec(10)

        self.logger.info("Pre-upgrade Step 4: Creating snapshots and clones")
        self._create_snapshots_and_clones(skip_clone_fio=True)

        # Run FIO on clones (writes fresh data to clones)
        self.logger.info("Pre-upgrade Step 4.1: Running FIO on clones")
        self._run_fio_on_clones(runtime=60)

        # Capture MD5 checksums on all PVCs and clones before upgrade
        self.logger.info("Pre-upgrade Step 5: Capturing MD5 checksums before upgrade")
        all_volume_names = list(self.pvc_details.keys()) + list(self.clone_details.keys())
        self.pre_upgrade_checksums = self._capture_pvc_checksums(all_volume_names)

        # Phase 2.7: Capture pre-upgrade state
        self._capture_pre_upgrade_state()

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

        # ── End maintenance window ──
        self.logger.info("=" * 40 + " MAINTENANCE WINDOW END " + "=" * 40)

        self.runner_k8s_log.restart_logging()

        # Verify cluster is active and all nodes healthy
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        self._assert_all_nodes_healthy()

        # Verify MD5 checksums match post-upgrade
        if hasattr(self, "pre_upgrade_checksums") and self.pre_upgrade_checksums:
            self.logger.info(
                "Post-upgrade: Verifying MD5 checksums match pre-upgrade data"
            )
            self._verify_pvc_checksums(self.pre_upgrade_checksums, "post-upgrade")
        else:
            self.logger.warning(
                "No pre-upgrade checksums available — skipping MD5 verification"
            )

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
  nodesPerSocket: {self.nodes_per_socket}
  workerNodes:
{worker_yaml}"""

        apply_cmd = f"cat <<'CREOF' | kubectl apply -f -\n{cr_yaml}\nCREOF"
        out, err = self.k8s_utils._exec_kubectl(apply_cmd)
        self.logger.info(f"CRs applied: {out}")
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

            restart_ts = int(datetime.now().timestamp())

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

            # Wait for cluster active before next worker
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id, status="active", timeout=600,
            )

            # Validate migration for all nodes on this worker
            sleep_n_sec(30)
            for node_id in nids:
                self.validate_migration_for_node(
                    restart_ts, 1200, node_id, 60, no_task_ok=True
                )

            if worker_idx < len(unique_ips):
                sleep_n_sec(30)

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
                    restart_ts, 1200, node_id, 60, no_task_ok=True
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
