"""
K8s-native continuous failover stress test.

All data-plane operations (lvol create, snapshot, clone, resize, delete) happen
through native Kubernetes APIs (PVC, VolumeSnapshot, kubectl apply/delete)
instead of sbcli CLI.  FIO runs as K8s Jobs rather than SSH-based processes.

Only sbcli (via kubectl exec) is used for:
  - Verification (list lvols, check node status, IO stats)
  - Outage operations (shutdown/restart storage nodes)
  - Diagnostics (cluster details, core dump checks)

Outage types:
  container_stop     → kubectl delete pod snode-spdk-pod-<x> (auto-restarts)
  graceful_shutdown  → sbcli sn shutdown via kubectl exec

Loop structure mirrors RandomMultiClientMultiFailoverTest.run():
  1. Create StorageClass + VolumeSnapshotClass + Pool
  2. Create initial PVCs with FIO Jobs
  3. Loop:
     a. Perform N+K outages
     b. Delete some PVCs, create new ones, create snapshots & clones, resize
     c. Recover nodes
     d. Validate (FIO, IO stats, migration, core dump)
"""

from __future__ import annotations

import json
import os
import random
import shutil
import string
import subprocess
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from e2e_tests.cluster_test_base import TestClusterBase
from exceptions.custom_exception import LvolNotConnectException
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils
from utils.ssh_utils import RunnerK8sLog


def _rand_seq(length: int) -> str:
    """Generate a random alphanumeric string starting with a letter."""
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


class K8sNativeFailoverTest(TestClusterBase):
    """
    Continuous N+K failover stress test using K8s-native storage operations.

    PVCs → lvols, VolumeSnapshots → snapshots, clone PVCs → clones.
    FIO runs as K8s Jobs with ConfigMaps.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_failover_ha"
        self.k8s_utils: K8sUtils | None = None

        # K8s resource naming
        self.STORAGE_CLASS_NAME = "simplyblock-csi-sc"
        self.CRYPTO_STORAGE_CLASS_NAME = "simplyblock-csi-sc-crypto"
        self.CRYPTO_POOL_NAME = "encryption-pool"
        self.SNAPSHOT_CLASS_NAME = "simplyblock-csi-snapshotclass"
        self.FIO_IMAGE = "dockerpinata/fio:2.1"
        self.tls_enabled = str(kwargs.get("tls_enabled", os.environ.get("TLS_ENABLED", "false"))).lower() == "true"

        # Sizing
        self.pvc_size = "10Gi"
        self.int_pvc_size = 10
        self.fio_size = "1G"
        self.FIO_RUNTIME = 4000

        # Counts — total_pvcs is set dynamically to len(sn_nodes) in run()
        self.total_pvcs = 6
        self.fio_num_jobs = 1

        # Outage config
        self.npcs = kwargs.get("npcs", 1)
        self.outage_types = ["graceful_shutdown"]
        self.outage_types2 = ["container_stop", "graceful_shutdown"]

        # ── Tracking dicts ──
        # pvc_name → {job_name, configmap_name, snapshots: [snap_name, ...], node_id}
        self.pvc_details: dict[str, dict] = {}
        # snap_name → {pvc_name}
        self.snapshot_details: dict[str, dict] = {}
        # clone_pvc_name → {snap_name, job_name, configmap_name}
        self.clone_details: dict[str, dict] = {}

        # Node tracking
        self.sn_nodes: list[str] = []
        self.sn_nodes_with_sec: list[str] = []
        self.sn_primary_secondary_map: dict[str, str] = {}
        self.node_vs_pvc: dict[str, list[str]] = {}

        # Outage tracking
        self.current_outage_node: str | None = None
        self.current_outage_nodes: list[str] = []
        self.outage_start_time: int | None = None
        self.outage_end_time: int | None = None
        self.snapshot_names: list[str] = []
        self.max_fault_tolerance: int = 1  # updated in run()

        # Deferred operation tracking
        self.pending_deletions: dict[str, dict] = {
            "clones": {},      # clone_name -> clone_info dict
            "snapshots": {},   # snap_name -> snap_info dict
            "pvcs": {},        # pvc_name -> pvc_info dict
        }
        self.failed_nvme_connects: dict[str, dict] = {}  # resource_name -> connect_info
        # Per-resource list of connect commands that failed (partial multipath)
        self.failed_secondary_connects: dict[str, list[str]] = {}  # resource_name -> [cmd, ...]

        # Client-based FIO mode (when CLIENT_IP env is set)
        self.use_client_fio = False  # set in setup()
        self.fio_threads = []
        self.mount_path = "/mnt/test_location"
        # lvol_mount_details: lvol_name → {ID, Command, Mount, Device, FS, Log, Client, snapshots}
        self.lvol_mount_details: dict[str, dict] = {}
        # clone_mount_details: clone_lvol_name → {ID, snapshot, Mount, Device, FS, Log, Client}
        self.clone_mount_details: dict[str, dict] = {}

        # Outage log
        self.outage_log_file = os.path.join(
            "logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )


    # ── Setup / Teardown ─────────────────────────────────────────────────────

    def setup(self):
        """K8s-native setup: no SSH connections (Talos-compatible).

        Replaces the parent setup() entirely — FIO runs as K8s Jobs so
        no client machines, NFS mounts, or SSH connections are needed.
        """
        self.logger.info("Inside K8sNativeFailoverTest.setup()")

        # 1. Retry sbcli API calls (routed through kubectl exec via K8sSbcliUtils)
        retry = 30
        while retry > 0:
            try:
                self.logger.info("Getting all storage nodes")
                self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_lvols()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as e:
                self.logger.debug(f"API call failed with error: {e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attempt exhausted. API failed with: {e}. Exiting")
                    raise e
                self.logger.info(f"Retrying Base APIs before starting tests. Attempt: {30 - retry + 1}")
                sleep_n_sec(10)

        # 2. No client machines needed — FIO runs as K8s Jobs
        self.client_machines = []
        self.fio_node = []

        # 3. Set up log directories with NFS retry + fallback
        #    Try the configured NFS path with retries (handles stale mounts
        #    by remounting).  Fall back to ~/e2e-logs if NFS stays unusable.
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        log_base = self._prepare_log_base(self.nfs_log_base, retries=3)
        self.nfs_log_base = log_base
        self.docker_logs_path = os.path.join(log_base, f"{self.test_name}-{timestamp}")
        self.log_path = os.path.join(self.docker_logs_path, "ClientLogs")
        os.makedirs(self.log_path, exist_ok=True)
        os.makedirs(self.docker_logs_path, exist_ok=True)

        run_file = os.getenv("RUN_DIR_FILE", None)
        if run_file:
            with open(run_file, "w") as f:
                f.write(self.docker_logs_path)

        # 4. Start K8s log monitor (local kubectl, no SSH)
        self.runner_k8s_log = RunnerK8sLog(
            log_dir=self.docker_logs_path,
            test_name=self.test_name,
        )
        self.runner_k8s_log.start_logging()
        self.runner_k8s_log.monitor_pod_logs()

        # 5. Clean up old lvols/pools via sbcli (through kubectl exec)
        #    Order: clones → snapshots → lvols → pools
        #    (SPDK refuses to delete a snapshot that still has clones)
        try:
            self.sbcli_utils.delete_all_clones()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.warning(f"Cleanup of old resources failed: {e}")

        # 6. Initialize K8sUtils
        # In local kubectl mode (K8S_LOCAL_KUBECTL=1), mgmt_nodes may be empty
        # because MNODES env var is not set — K8sUtils doesn't need a real IP
        # since it runs kubectl locally via subprocess.
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        self.k8s_utils = K8sUtils(
            ssh_obj=self.ssh_obj,
            mgmt_node=mgmt_node,
        )
        self.logger.info(f"[K8s] K8sUtils initialized for mgmt_node={mgmt_node!r}")

        # 6b. Kill orphaned K8s Jobs/resources from any previous run
        self._kill_orphaned_k8s_resources()

        # 7. Client-based FIO mode: set up SSH connections to external clients
        client_ip_raw = os.environ.get("CLIENT_IP", "").strip()
        if client_ip_raw:
            self.use_client_fio = True
            self.client_machines = client_ip_raw.split()
            self.fio_node = list(self.client_machines)
            self.logger.info(
                f"[K8s] Client FIO mode enabled — clients: {self.fio_node}"
            )

            for client in self.client_machines:
                self.logger.info(f"[K8s] Connecting SSH to client {client}")
                self.ssh_obj.connect(
                    address=client,
                    bastion_server_address=self.bastion_server,
                )
                sleep_n_sec(2)
                self.ssh_obj.set_aio_max_nr(client)

            # Mount NFS on clients for shared log access (skip for cloud clusters)
            if os.environ.get("SKIP_NFS", "").strip() not in ("1", "true"):
                nfs_server = "10.10.10.140"
                nfs_path = "/srv/nfs_share"
                nfs_mount_point = "/mnt/nfs_share"
                for client in self.client_machines:
                    self.ssh_obj.ensure_nfs_mounted(
                        client, nfs_server, nfs_path, nfs_mount_point
                    )
                self.ssh_obj.ensure_nfs_mounted(
                    "localhost", nfs_server, nfs_path, nfs_mount_point, is_local=True
                )
            else:
                self.logger.info("[K8s] SKIP_NFS set — skipping NFS mount on clients")

            # Create log directories on clients
            for client in self.fio_node:
                self.ssh_obj.make_directory(node=client, dir_name=self.log_path)

            # Pre-clean on clients: unmount test dirs first, then NVMe disconnect
            for client in self.fio_node:
                # Unmount all subdirs under mount_path BEFORE NVMe disconnect
                # (NVMe disconnect fails silently if device is still mounted)
                try:
                    self.ssh_obj.exec_command(
                        node=client,
                        command=(
                            f"for mp in {self.mount_path}/*; do "
                            f"  if mountpoint -q \"$mp\" 2>/dev/null; then "
                            f"    timeout 10 umount -f \"$mp\" 2>/dev/null || umount -l \"$mp\" 2>/dev/null || true; "
                            f"  fi; "
                            f"done; "
                            f"rm -rf {self.mount_path}/* 2>/dev/null || true"
                        ),
                    )
                except Exception as exc:
                    self.logger.warning(f"[setup] Unmount test dirs on {client}: {exc}")
                sleep_n_sec(2)
                try:
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep="lvol")
                except Exception as exc:
                    self.logger.warning(f"[setup] NVMe disconnect on {client}: {exc}")
                sleep_n_sec(2)

        # Start dmesg/journalctl collectors on all K8s nodes.
        # These privileged pods stream host dmesg via nsenter, so we get
        # full kernel-level NVMe path events throughout the test.
        try:
            self._start_dmesg_collectors()
        except Exception as exc:
            self.logger.warning(f"[setup] dmesg collectors failed to start: {exc}")

        self.logger.info(
            f"K8sNativeFailoverTest.setup() complete "
            f"(client_fio={'enabled' if self.use_client_fio else 'disabled'})"
        )

    def _ensure_k8s_utils(self):
        if not self.k8s_utils:
            raise RuntimeError(
                "[K8s] k8s_utils not initialised — was setup() called with k8s_run=True?"
            )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _prepare_log_base(self, nfs_path: str, retries: int = 3) -> str:
        """Try to make *nfs_path* writable, retrying with a remount on failure.

        On each retry:
          1. Create a small temp dir under *nfs_path* to test write access.
          2. If it fails (PermissionError / OSError), attempt to remount.
          3. After all retries are exhausted, fall back to ``~/e2e-logs``.

        Returns the usable log base path.
        """
        for attempt in range(1, retries + 1):
            probe = os.path.join(nfs_path, ".probe_write_test")
            try:
                os.makedirs(probe, exist_ok=True)
                shutil.rmtree(probe, ignore_errors=True)
                self.logger.info(f"[NFS] Log base {nfs_path} is writable (attempt {attempt})")
                return nfs_path
            except OSError as exc:
                self.logger.warning(
                    f"[NFS] {nfs_path} not writable (attempt {attempt}/{retries}): {exc}"
                )
                # Try to remount — stale mounts need umount + mount
                self._try_remount_nfs(nfs_path)
                sleep_n_sec(5)

        # All retries failed — fall back to local directory
        fallback = os.path.join(os.path.expanduser("~"), "e2e-logs")
        self.logger.warning(
            f"[NFS] All {retries} attempts failed for {nfs_path} "
            f"— falling back to {fallback}"
        )
        os.makedirs(fallback, exist_ok=True)
        return fallback

    def _try_remount_nfs(self, mount_point: str):
        """Best-effort remount of an NFS mount point.

        Handles stale NFS mounts by force-unmounting first, then mounting
        using the NFS server/path from environment or known defaults.
        """
        nfs_server = os.environ.get("NFS_SERVER", "10.10.10.140")
        nfs_export = os.environ.get("NFS_EXPORT", "/srv/nfs_share")

        try:
            # Check if already mounted (possibly stale)
            result = subprocess.run(
                ["mountpoint", "-q", mount_point],
                capture_output=True, timeout=10,
            )
            if result.returncode == 0:
                self.logger.info(f"[NFS] {mount_point} is mounted — force unmounting stale mount")
                subprocess.run(
                    ["sudo", "umount", "-f", "-l", mount_point],
                    capture_output=True, timeout=30,
                )
                sleep_n_sec(2)

            # Ensure mount point directory exists
            subprocess.run(
                ["sudo", "mkdir", "-p", mount_point],
                capture_output=True, timeout=10,
            )

            # Mount
            self.logger.info(f"[NFS] Mounting {nfs_server}:{nfs_export} → {mount_point}")
            result = subprocess.run(
                ["sudo", "mount", "-t", "nfs", f"{nfs_server}:{nfs_export}", mount_point],
                capture_output=True, timeout=30,
            )
            if result.returncode == 0:
                self.logger.info(f"[NFS] Successfully remounted {mount_point}")
            else:
                stderr = result.stderr.decode(errors="replace").strip()
                self.logger.warning(f"[NFS] mount failed (rc={result.returncode}): {stderr}")
        except Exception as exc:
            self.logger.warning(f"[NFS] Remount attempt failed: {exc}")

    def _kill_orphaned_k8s_resources(self):
        """Kill orphaned FIO Jobs and other stale K8s resources from previous
        test runs that could interfere with the current run."""
        self.logger.info("[cleanup] Killing orphaned K8s resources from previous runs...")
        self.k8s_utils.cleanup_stale_fio_resources()
        sleep_n_sec(5)
        # Force-delete any pods stuck in Terminating for FIO jobs
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pods -n {self.k8s_utils.namespace} "
                f"--field-selector=status.phase=Failed -l app=fio-benchmark "
                f"--ignore-not-found"
            )
        except Exception as exc:
            self.logger.warning(f"[cleanup] Failed pod cleanup: {exc}")
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pods -n {self.k8s_utils.namespace} "
                f"--field-selector=status.phase=Succeeded -l app=fio-benchmark "
                f"--ignore-not-found"
            )
        except Exception as exc:
            self.logger.warning(f"[cleanup] Succeeded pod cleanup: {exc}")
        self.logger.info("[cleanup] Orphaned K8s resource cleanup done.")

    def teardown(self, delete_lvols=True, close_ssh=True):
        """K8s-native teardown, with optional client cleanup."""
        self.logger.info("Inside K8sNativeFailoverTest.teardown()")
        self.stop_root_monitor()

        # Collect final dmesg/journalctl and clean up collector pods
        try:
            self._stop_dmesg_collectors()
        except Exception as exc:
            self.logger.warning(f"[teardown] dmesg collector cleanup: {exc}")

        # Kill fio on client hosts BEFORE deleting PVCs to avoid IO errors
        # from volumes being deleted while fio is still running
        if self.use_client_fio and self.ssh_obj:
            self.logger.info("[teardown] Killing fio processes on client hosts...")
            for client in self.fio_node:
                try:
                    self.ssh_obj.exec_command(
                        node=client,
                        command="sudo pkill -9 fio 2>/dev/null; "
                                "sudo tmux kill-server 2>/dev/null || true"
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[teardown] Failed to kill fio on {client}: {exc}"
                    )
            sleep_n_sec(5)

        # Kill orphaned K8s FIO Jobs so they don't interfere with next run
        if self.k8s_utils:
            try:
                self._kill_orphaned_k8s_resources()
            except Exception as exc:
                self.logger.warning(f"[teardown] Orphaned job cleanup failed: {exc}")

        # Disconnect NVMe on clients if in client FIO mode
        if self.use_client_fio:
            for client in self.fio_node:
                try:
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep="lvol")
                except Exception as exc:
                    self.logger.warning(f"[teardown] NVMe disconnect on {client}: {exc}")

        if delete_lvols:
            try:
                self.sbcli_utils.delete_all_clones()
                sleep_n_sec(2)
                self.sbcli_utils.delete_all_snapshots()
                sleep_n_sec(2)
                self.sbcli_utils.delete_all_lvols()
                sleep_n_sec(2)
                self.sbcli_utils.delete_all_storage_pools()
            except Exception as e:
                self.logger.info(f"Teardown cleanup error: {e}")
                self.logger.info(traceback.format_exc())

    def collect_outage_diagnostics(self, label):
        """Override base to also collect dmesg/journalctl snapshots."""
        super().collect_outage_diagnostics(label)
        try:
            self._collect_dmesg_snapshots(label)
        except Exception as exc:
            self.logger.warning(f"[diagnostics] dmesg snapshot failed: {exc}")

    def cleanup_logs(self):
        """No-op: K8s-native test has no SSH-based logs to clean."""
        self.logger.info("cleanup_logs: skipped (K8s-native, no SSH)")

    # ── Node-level dmesg / journalctl collection ────────────────────────
    #
    # Primary : Privileged pods with ``nsenter`` streaming ``dmesg -Tw``
    #           on every node.  Works on Talos, vanilla K8s, and OpenShift.
    # Fallback: ``oc debug node/<n> -- chroot /host dmesg -T`` when on
    #           OpenShift and the collector pod is unreachable (e.g. the
    #           node was under outage and the pod hasn't restarted yet).
    #
    # Collection points (same as other metrics):
    #   - pre_outage   : before any node is taken down
    #   - post_recovery: after all nodes are back online
    #   - end_iteration: after FIO validation completes
    #   - final        : at teardown

    def _get_all_k8s_node_names(self) -> list[str]:
        """Return a list of ALL K8s node hostnames."""
        out, _ = self.k8s_utils._exec_kubectl(
            "kubectl get nodes --no-headers -o custom-columns=':metadata.name'",
            supress_logs=True,
        )
        return [n.strip() for n in out.strip().splitlines() if n.strip()]

    def _detect_openshift(self) -> bool:
        """Return True if the cluster is OpenShift (``oc`` available)."""
        if hasattr(self, '_is_openshift'):
            return self._is_openshift
        try:
            out, _ = self.k8s_utils._exec_kubectl(
                "oc version --client 2>/dev/null && echo OC_OK || echo OC_NO",
                supress_logs=True,
            )
            self._is_openshift = "OC_OK" in out
        except Exception:
            self._is_openshift = False
        self.logger.info(f"[dmesg] Platform detection: openshift={self._is_openshift}")
        return self._is_openshift

    def _start_dmesg_collectors(self):
        """Deploy a privileged pod on each K8s node that streams host dmesg.

        Each pod runs ``nsenter`` into the host PID namespace and executes
        ``dmesg -Tw`` (follow mode with human-readable timestamps).  The
        output is captured on pod stdout, retrievable via ``kubectl logs``
        at any time.

        Also starts a ``journalctl -kf`` stream in a second container for
        kernel journal messages.

        Deployed on ALL platforms (Talos, K8s, OpenShift).  On OpenShift
        ``oc debug node/`` is available as a fallback if the pod is down.
        """
        self._ensure_k8s_utils()
        self._detect_openshift()

        ns = self.k8s_utils.namespace
        nodes = self._get_all_k8s_node_names()
        if not nodes:
            self.logger.warning("[dmesg] No K8s nodes found — skipping collectors")
            return

        self._dmesg_collector_nodes = nodes
        self.logger.info(f"[dmesg] Starting dmesg/journalctl collectors on {len(nodes)} nodes")

        for node in nodes:
            pod_name = f"dmesg-collector-{node}"
            yaml_spec = (
                f"apiVersion: v1\n"
                f"kind: Pod\n"
                f"metadata:\n"
                f"  name: {pod_name}\n"
                f"  namespace: {ns}\n"
                f"  labels:\n"
                f"    app: dmesg-collector\n"
                f"spec:\n"
                f"  nodeName: {node}\n"
                f"  hostPID: true\n"
                f"  tolerations:\n"
                f"  - operator: Exists\n"
                f"  containers:\n"
                f"  - name: dmesg\n"
                f"    image: busybox\n"
                f"    command: ['nsenter', '-t', '1', '-m', '-u', '-i', '-n', '--',\n"
                f"              'sh', '-c',\n"
                f"              'dmesg -T; echo === FOLLOW ===; dmesg -Tw 2>/dev/null || while true; do sleep 30; dmesg -T; done']\n"
                f"    securityContext:\n"
                f"      privileged: true\n"
                f"  - name: journalctl\n"
                f"    image: busybox\n"
                f"    command: ['nsenter', '-t', '1', '-m', '-u', '-i', '-n', '--',\n"
                f"              'sh', '-c',\n"
                f"              'journalctl -kb --no-pager 2>/dev/null; echo === FOLLOW ===; journalctl -kf --no-pager 2>/dev/null || dmesg -T; dmesg -Tw 2>/dev/null || while true; do sleep 30; dmesg -T; done']\n"
                f"    securityContext:\n"
                f"      privileged: true\n"
                f"  restartPolicy: Always\n"
            )
            try:
                # Delete any leftover from previous run
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete pod {pod_name} -n {ns} "
                    f"--force --grace-period=0 2>/dev/null || true",
                    supress_logs=True,
                )
                self.k8s_utils._exec_kubectl(
                    f"cat <<'DMESG_EOF' | kubectl apply -f -\n{yaml_spec}DMESG_EOF",
                )
                self.logger.info(f"[dmesg] Started collector pod {pod_name} on {node}")
            except Exception as exc:
                self.logger.warning(f"[dmesg] Failed to start collector on {node}: {exc}")

        # Wait for all collector pods to be running
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl wait pods -l app=dmesg-collector -n {ns} "
                f"--for=condition=Ready --timeout=120s 2>/dev/null || true",
            )
        except Exception:
            pass

    def _collect_dmesg_from_pod(self, node: str, snap_dir: str,
                                 label: str) -> bool:
        """Try to collect dmesg/journalctl from the collector pod on *node*.

        Returns True if at least one log was saved, False otherwise.
        """
        ns = self.k8s_utils.namespace
        pod_name = f"dmesg-collector-{node}"
        saved = False
        for container, prefix in [("dmesg", "dmesg"), ("journalctl", "journalctl")]:
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl logs {pod_name} -c {container} -n {ns} "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
                if out and out.strip():
                    fname = f"{prefix}_{node}_{label}.log"
                    fpath = os.path.join(snap_dir, fname)
                    with open(fpath, "w") as f:
                        f.write(out)
                    self.logger.info(
                        f"[dmesg] Saved {prefix} for {node} "
                        f"({len(out)} bytes) → {fname}"
                    )
                    saved = True
            except Exception:
                pass
        return saved

    def _collect_dmesg_via_oc_debug(self, node: str, snap_dir: str,
                                     label: str):
        """Fallback: collect dmesg/journalctl via ``oc debug node/``."""
        for cmd_name, host_cmd in [
            ("dmesg", "dmesg -T"),
            ("journalctl", "journalctl -b --no-pager"),
        ]:
            fname = f"{cmd_name}_{node}_{label}.log"
            fpath = os.path.join(snap_dir, fname)
            # Skip if already collected from the pod
            if os.path.exists(fpath):
                continue
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"oc debug node/{node} "
                    f"-- chroot /host {host_cmd} 2>/dev/null || true",
                    supress_logs=True,
                )
                if out and out.strip():
                    with open(fpath, "w") as f:
                        f.write(out)
                    self.logger.info(
                        f"[dmesg] oc debug fallback: saved {cmd_name} for "
                        f"{node} ({len(out)} bytes) → {fname}"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[dmesg] oc debug {cmd_name} on {node} failed: {exc}"
                )

    def _collect_dmesg_snapshots(self, label: str):
        """Save a dmesg + journalctl snapshot from each node.

        Primary: reads from the persistent collector pods via kubectl logs.
        Fallback (OpenShift): if the pod is unreachable (node under outage),
        tries ``oc debug node/`` to get a host-level snapshot.
        """
        if not hasattr(self, '_dmesg_collector_nodes') or not self._dmesg_collector_nodes:
            return

        snap_dir = os.path.join(self.docker_logs_path, "node_kernel_logs")
        os.makedirs(snap_dir, exist_ok=True)

        for node in self._dmesg_collector_nodes:
            # Primary: collector pod
            got_logs = self._collect_dmesg_from_pod(node, snap_dir, label)

            # Fallback: oc debug (OpenShift only, when pod didn't yield logs)
            if not got_logs and getattr(self, '_is_openshift', False):
                self._collect_dmesg_via_oc_debug(node, snap_dir, label)

    def _stop_dmesg_collectors(self):
        """Collect final snapshots and delete all dmesg collector pods."""
        if not hasattr(self, '_dmesg_collector_nodes') or not self._dmesg_collector_nodes:
            return

        self.logger.info("[dmesg] Collecting final dmesg/journalctl snapshots")
        self._collect_dmesg_snapshots("final")

        ns = self.k8s_utils.namespace
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pods -l app=dmesg-collector -n {ns} "
                f"--force --grace-period=0 2>/dev/null || true",
                supress_logs=True,
            )
            self.logger.info("[dmesg] Cleaned up all dmesg collector pods")
        except Exception as exc:
            self.logger.warning(f"[dmesg] Cleanup failed: {exc}")

    def configure_sysctl_settings(self):
        """No-op: K8s-native test has no SSH access for sysctl."""
        self.logger.info("configure_sysctl_settings: skipped (K8s-native, no SSH)")

    def unmount_all(self, base_path=None):
        """No-op unless client FIO mode (PVCs have no host mount points)."""
        if self.use_client_fio:
            path = base_path or self.mount_path
            for client in self.fio_node:
                try:
                    self.ssh_obj.unmount_path(node=client, device=path)
                except Exception:
                    pass

    def disconnect_lvols(self):
        """No-op unless client FIO mode (PVCs have no NVMe connections)."""
        if self.use_client_fio:
            for client in self.fio_node:
                try:
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep="lvol")
                except Exception:
                    pass

    def validate_migration_for_node(self, timestamp, timeout, node_id=None,
                                    check_interval=60, no_task_ok=False):
        """K8s-native migration validation — uses kubectl exec instead of SSH.

        Replaces the parent method which does ssh_obj.exec_command() to
        mgmt_nodes[0]. This version uses K8sSbcliUtils (kubectl exec).
        """
        start_time = datetime.now(timezone.utc)
        end_time = start_time + timedelta(seconds=timeout)

        # Initial task list via kubectl exec (replaces SSH call)
        output = None
        while output is None:
            try:
                k8s = self.sbcli_utils.k8s
                output, _ = k8s.exec_sbcli(
                    f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0"
                )
            except Exception as e:
                self.logger.warning(f"Failed to get task list via kubectl exec: {e}")
                output = ""
            self.logger.info(f"Data migration output: {output}")
            if no_task_ok:
                return

        migration_tasks_found = False

        while datetime.now(timezone.utc) < end_time:
            tasks = self.sbcli_utils.get_cluster_tasks(self.cluster_id)
            filtered_tasks = self.filter_migration_tasks(
                tasks, node_id, timestamp, window_minutes=10
            )

            if filtered_tasks:
                migration_tasks_found = True
                self.logger.info(f"Checking migration tasks: {filtered_tasks}")

                all_done = True
                completed_count = 0

                for task in filtered_tasks:
                    try:
                        updated_at = datetime.fromisoformat(
                            task['updated_at']
                        ).astimezone(timezone.utc)
                    except ValueError as e:
                        self.logger.error(
                            f"Error parsing timestamp for task {task['id']}: {e}"
                        )
                        continue

                    if (datetime.now(timezone.utc) - updated_at > timedelta(minutes=65)
                            and task["status"] != "done"):
                        raise RuntimeError(
                            f"Migration task {task['id']} is stuck "
                            f"(last updated at {updated_at.isoformat()})."
                        )

                    if task['status'] == 'done':
                        completed_count += 1
                    else:
                        all_done = False

                total_tasks = len(filtered_tasks)
                remaining_tasks = total_tasks - completed_count
                self.logger.info(
                    f"Total migration tasks: {total_tasks}, "
                    f"Completed: {completed_count}, Remaining: {remaining_tasks}"
                )

                if all_done:
                    self.logger.info(
                        f"All migration tasks for "
                        f"{'node ' + node_id if node_id else 'the cluster'} "
                        f"completed successfully without any stuck tasks."
                    )
                    return
            else:
                self.logger.info(
                    f"No migration tasks found yet, retrying after {check_interval}s..."
                )

            sleep_n_sec(check_interval)

        if not migration_tasks_found and not no_task_ok:
            raise RuntimeError(
                f"No migration tasks found for "
                f"{'node ' + node_id if node_id else 'the cluster'} "
                f"after the specified timestamp {timestamp} "
                f"and function containing device migration!"
            )

        raise RuntimeError(
            f"Timeout reached: Not all migration tasks completed within "
            f"the specified timeout of {timeout} seconds."
        )

    def _initialize_outage_log(self):
        os.makedirs(os.path.dirname(self.outage_log_file), exist_ok=True)
        with open(self.outage_log_file, "w") as log:
            log.write("Timestamp,Node,Outage_Type,Event\n")

    def log_outage_event(self, node, outage_type, event, outage_time=0):
        if outage_time and isinstance(self.outage_start_time, (int, float)) and self.outage_start_time > 0:
            ts_dt = datetime.fromtimestamp(int(self.outage_start_time) + int(outage_time) * 60)
        else:
            ts_dt = datetime.now()
        timestamp = ts_dt.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.outage_log_file, "a") as log:
            log.write(f"{timestamp},{node},{outage_type},{event}\n")

    # ── FIO config builder ───────────────────────────────────────────────────

    def _build_fio_config(self, name: str) -> tuple[str, str]:
        """Build FIO main and warmup configs for a benchmark run.

        Returns:
            (main_config, warmup_config) — the warmup config does a sequential
            write pass of zeros to the same files, wiping any stale
            FIO_HDR_MAGIC left on thin-provisioned blocks by previously deleted
            lvols.  The main config uses ``verify_backlog`` so that FIO bypasses
            the rand_seed PRNG check (broken in rw modes) while still
            performing full MD5 data-integrity verification.
        """
        bs = f"{2 ** random.randint(2, 7)}k"
        run_id = _rand_seq(6)
        randseed = random.randint(1, 2**63)

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
            f"runtime={self.FIO_RUNTIME}\n"
            f"group_reporting\n"
            f"verify=md5\n"
            f"verify_dump=1\n"
            f"verify_fatal=1\n"
            f"verify_backlog=4096\n"
            f"verify_backlog_batch=32\n"
            f"randseed={randseed}\n"
            f"max_latency=20s\n"
            f"write_iolog=/spdkvol/{name}-iolog.log\n"
            f"log_avg_msec=1000\n"
            f"write_bw_log=/spdkvol/{name}-fio\n"
            f"write_lat_log=/spdkvol/{name}-fio\n"
            f"write_iops_log=/spdkvol/{name}-fio\n"
            f"\n"
            f"[job1]\n"
        )

        # Warmup: sequential write of zeros to the SAME files, wiping any
        # stale FIO_HDR_MAGIC left on thin-provisioned blocks so that stale
        # but self-consistent headers from prior lvols don't mask corruption.
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
            f"group_reporting\n"
            f"zero_buffers\n"
            f"\n"
            f"[job1]\n"
        )

        return main_config, warmup_config

    # ── PVC → lvol mapping helpers ─────────────────────────────────────────

    def _snapshot_lvol_ids(self) -> set[str]:
        """Return the set of all current lvol IDs (used for before/after diff)."""
        return set(self.sbcli_utils.list_lvols().values())

    def _find_new_lvol(self, old_ids: set[str]) -> tuple[str, str] | None:
        """Return ``(lvol_name, lvol_id)`` for the lvol created since *old_ids*.

        Returns ``None`` if no new lvol is found after a short retry window.
        """
        for _ in range(10):
            current = self.sbcli_utils.list_lvols()  # {name: id}
            for name, lid in current.items():
                if lid not in old_ids:
                    return name, lid
            sleep_n_sec(3)
        return None

    def _get_pvc_node_id(self, pvc_name: str) -> str | None:
        """Map a bound PVC to its storage node UUID via volumeHandle → lvol details."""
        try:
            vol_handle = self.k8s_utils.get_pvc_volume_handle(pvc_name)
            if not vol_handle:
                return None
            # volumeHandle format: clusterid:pool:lvolid
            lvol_id = vol_handle.split(":")[-1] if ":" in vol_handle else vol_handle
            details = self.sbcli_utils.get_lvol_details(lvol_id)
            if details:
                return details[0].get("node_id")
        except Exception as exc:
            self.logger.warning(f"[_get_pvc_node_id] Failed to map PVC {pvc_name}: {exc}")
        return None

    def _get_k8s_node_for_storage_node(self, storage_node_id: str) -> str | None:
        """Resolve a storage node UUID to its K8s node hostname."""
        try:
            details = self.sbcli_utils.get_storage_node_details(storage_node_id)
            if details:
                node_ip = details[0]["mgmt_ip"]
                return self.k8s_utils._get_k8s_node_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[_get_k8s_node] Failed to resolve k8s node for {storage_node_id}: {exc}"
            )
        return None

    # ── Client FIO helpers ────────────────────────────────────────────────

    def _connect_lvol_on_client(self, lvol_name: str, client: str):
        """NVMe-connect *lvol_name* on *client*, return (device_path, failed_cmds).

        Attempts every connect string for the lvol.  If some fail but at
        least one succeeds (device appears), returns the device path together
        with the list of failed connect commands so the caller can defer
        them for retry after recovery.  Raises LvolNotConnectException only
        when *no* path connects at all.
        """
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        if not connect_ls:
            raise RuntimeError(f"No connect strings for lvol {lvol_name}")

        failed_cmds: list[str] = []
        initial_devices = self.ssh_obj.get_devices(node=client)
        for connect_str in connect_ls:
            _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
            if error:
                self.logger.warning(
                    f"[client_fio] NVMe connect error on {client}: {error}"
                )
                failed_cmds.append(connect_str)

        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=client)
        for device in final_devices:
            if device not in initial_devices:
                if failed_cmds:
                    self.logger.warning(
                        f"[client_fio] {lvol_name}: {len(failed_cmds)}/{len(connect_ls)} "
                        f"connect paths failed — only partial multipath on {client}"
                    )
                return f"/dev/{device.strip()}", failed_cmds

        raise LvolNotConnectException(
            f"LVOL {lvol_name} did not appear as device on {client}"
        )

    def _run_fio_warmup_ssh(self, name: str, client: str, mount_point: str,
                            bs: str):
        """Run a blocking FIO write-only warmup on *client* via SSH.

        Overwrites every block in the files the main FIO run will use with
        zeros, wiping any stale FIO_HDR_MAGIC left on thin-provisioned
        storage blocks by previously deleted lvols.  Without this, the main
        FIO may read stale verify headers that look valid (correct magic +
        self-consistent MD5) and miss real corruption.

        The main FIO uses ``--verify_backlog`` to bypass FIO's broken
        rand_seed PRNG check in rw modes while keeping MD5 integrity
        verification active.
        """
        warmup_cmd = (
            f"fio --name={name}_fio --directory={mount_point} "
            f"--ioengine=libaio --direct=1 --iodepth=32 "
            f"--rw=write --bs={bs} --size={self.fio_size} "
            f"--zero_buffers "
            f"--numjobs={self.fio_num_jobs} --nrfiles=6"
        )
        self.logger.info(f"[warmup] Running FIO warmup on {client}: {name}")
        self.ssh_obj.exec_command(node=client, command=warmup_cmd, timeout=300)
        self.logger.info(f"[warmup] FIO warmup complete on {client}: {name}")

    def _start_client_fio(self, name: str, client: str, mount_point: str,
                          log_file: str, bs: str = None, randseed: int = None):
        """Launch FIO in a background thread on *client* via SSH/tmux."""
        if bs is None:
            bs = f"{2 ** random.randint(2, 7)}K"
        if randseed is None:
            randseed = random.randint(1, 2**63)

        # iolog and fio bw/lat/iops logs sit next to the fio output log
        iolog_file = log_file.replace(".log", "_iolog.log") if log_file else None
        fio_log_file = log_file.replace(".log", "_fio") if log_file else None

        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client, None, mount_point, log_file),
            kwargs={
                "size": self.fio_size,
                "name": f"{name}_fio",
                "rw": "randrw",
                "bs": bs,
                "nrfiles": 6,
                "iodepth": 1,
                "numjobs": self.fio_num_jobs,
                "time_based": True,
                "runtime": self.FIO_RUNTIME,
                "randseed": randseed,
                "iolog_file": iolog_file,
                "fio_log_file": fio_log_file,
            },
        )
        fio_thread.start()
        self.fio_threads.append(fio_thread)

    def _kill_fio_on_client(self, name: str, client: str):
        """Kill the FIO process for *name* on *client*."""
        self.ssh_obj.find_process_name(client, f"{name}_fio", return_pid=False)
        fio_pids = self.ssh_obj.find_process_name(
            client, f"{name}_fio", return_pid=True
        )
        for pid in fio_pids:
            self.ssh_obj.kill_processes(client, pid=pid)
        # Wait for fio to actually stop
        for attempt in range(30):
            fio_pids = self.ssh_obj.find_process_name(
                client, f"{name}_fio", return_pid=True
            )
            if len(fio_pids) <= 2:
                break
            for pid in fio_pids:
                self.ssh_obj.kill_processes(client, pid=pid)
            sleep_n_sec(10)

    def _disconnect_lvol_on_client(self, lvol_name: str, client: str):
        """NVMe-disconnect *lvol_name* on *client*."""
        try:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            if lvol_id:
                details = self.sbcli_utils.get_lvol_details(lvol_id)
                if details:
                    nqn = details[0].get("nqn", "")
                    if nqn:
                        self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)
                        return
        except Exception as exc:
            self.logger.warning(
                f"[client_fio] Failed to disconnect {lvol_name} on {client}: {exc}"
            )

    # ── Deferred operation recording ─────────────────────────────────────────

    def record_pending_clone_delete(self, clone_name: str, clone_info: dict):
        """Record a clone PVC deletion that failed (expected during outage)."""
        self.logger.warning(f"[DEFERRED] Adding clone to pending delete: {clone_name}")
        self.pending_deletions["clones"][clone_name] = clone_info.copy()

    def record_pending_snapshot_delete(self, snap_name: str, snap_info: dict):
        """Record a VolumeSnapshot deletion that failed (expected during outage)."""
        self.logger.warning(f"[DEFERRED] Adding snapshot to pending delete: {snap_name}")
        self.pending_deletions["snapshots"][snap_name] = snap_info.copy()

    def record_pending_pvc_delete(self, pvc_name: str, pvc_info: dict):
        """Record a PVC deletion that failed (expected during outage)."""
        self.logger.warning(f"[DEFERRED] Adding PVC to pending delete: {pvc_name}")
        self.pending_deletions["pvcs"][pvc_name] = pvc_info.copy()

    def record_failed_nvme_connect(self, name: str, connect_info: dict):
        """Record a failed NVMe connect (client mode, expected during outage)."""
        self.logger.warning(
            f"[DEFERRED] NVMe connect failed for {name} "
            f"on client {connect_info.get('client')} — will retry after recovery"
        )
        self.failed_nvme_connects[name] = connect_info

    def record_failed_secondary_connects(self, name: str, client: str,
                                         failed_cmds: list[str]):
        """Record partial-multipath connect failures for later retry."""
        self.logger.warning(
            f"[DEFERRED] {len(failed_cmds)} secondary connect(s) failed for "
            f"{name} on {client} — will retry after recovery"
        )
        self.failed_secondary_connects[name] = {
            "client": client,
            "cmds": list(failed_cmds),
        }

    def retry_failed_secondary_connects(self, timeout=120, interval=10):
        """Retry deferred secondary NVMe connects (missing multipath paths).

        Unlike retry_failed_nvme_connects (which retries full connects for
        volumes that never appeared), this retries individual connect
        commands that failed while the primary path succeeded — restoring
        full multipath.
        """
        if not self.failed_secondary_connects:
            self.logger.info("[retry_secondary] No deferred secondary connects pending")
            return

        self.logger.info(
            f"[retry_secondary] Retrying secondary connects for "
            f"{len(self.failed_secondary_connects)} volume(s)"
        )
        start = time.time()

        while time.time() - start < timeout:
            for name in list(self.failed_secondary_connects):
                info = self.failed_secondary_connects[name]
                client = info["client"]
                still_failed = []
                for cmd in info["cmds"]:
                    _, error = self.ssh_obj.exec_command(node=client, command=cmd)
                    if error:
                        still_failed.append(cmd)
                    else:
                        self.logger.info(
                            f"[retry_secondary] Reconnected secondary path "
                            f"for {name} on {client}"
                        )
                if still_failed:
                    self.failed_secondary_connects[name]["cmds"] = still_failed
                else:
                    del self.failed_secondary_connects[name]

            if not self.failed_secondary_connects:
                self.logger.info(
                    "[retry_secondary] All secondary paths reconnected"
                )
                return

            sleep_n_sec(interval)

        remaining = {
            n: len(i["cmds"])
            for n, i in self.failed_secondary_connects.items()
        }
        self.logger.warning(
            f"[retry_secondary] Could not restore all secondary paths within "
            f"{timeout}s. Remaining: {remaining}"
        )
        self.failed_secondary_connects.clear()

    # ── PVC + FIO creation ───────────────────────────────────────────────────

    def create_pvcs_with_fio(self, count: int, node_ids: list[str] = None,
                             storage_class: str = None):
        """Create *count* PVCs via K8s and start FIO on each.

        When ``self.use_client_fio`` is True, the underlying lvol is
        NVMe-connected to an external client and FIO runs via SSH.
        Otherwise FIO runs as a K8s Job (existing behaviour).

        Args:
            node_ids: If provided, pin PVC *i* to ``node_ids[i]`` using the
                      ``simplybk/host-id`` annotation.  Length must equal *count*.
            storage_class: Explicit SC name. When None, alternates between
                           regular and crypto (if TLS enabled) for 50/50 split.
        """
        self._ensure_k8s_utils()
        # Track how many PVCs already exist (for alternation index)
        existing_count = len(self.pvc_details)
        for i in range(count):
            pvc_name = f"pvc-{_rand_seq(12)}"
            target_node = node_ids[i] if node_ids and i < len(node_ids) else None

            # Determine StorageClass: explicit > 50/50 alternation > regular
            if storage_class:
                sc_name = storage_class
            elif self.tls_enabled and (existing_count + i) % 2 == 1:
                sc_name = self.CRYPTO_STORAGE_CLASS_NAME
            else:
                sc_name = self.STORAGE_CLASS_NAME

            self.logger.info(
                f"[create_pvc] Creating PVC {pvc_name} ({i+1}/{count}) SC={sc_name}"
                + (f" pinned to node {target_node}" if target_node else "")
            )

            # Snapshot lvol IDs before PVC creation (for client mode mapping)
            old_lvol_ids = self._snapshot_lvol_ids() if self.use_client_fio else set()

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.pvc_size, sc_name,
                    node_id=target_node,
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[create_pvc] PVC creation failed for {pvc_name}: {exc}")
                # Clean up the orphaned PVC so it doesn't linger and create
                # an lvol with no FIO running on it.
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                    self.logger.info(f"[create_pvc] Cleaned up orphaned PVC {pvc_name}")
                except Exception:
                    self.logger.warning(f"[create_pvc] Could not clean up PVC {pvc_name}")
                continue

            sleep_n_sec(5)

            if self.use_client_fio:
                # ── Client FIO path ──
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[create_pvc] Could not map PVC {pvc_name} to lvol — skipping"
                    )
                    continue
                lvol_name, lvol_id = lvol_info

                node_id = None
                try:
                    details = self.sbcli_utils.get_lvol_details(lvol_id)
                    if details:
                        node_id = details[0].get("node_id")
                except Exception:
                    pass

                client = self.fio_node[i % len(self.fio_node)]
                fs_type = random.choice(["ext4", "xfs"])

                try:
                    device, failed_cmds = self._connect_lvol_on_client(lvol_name, client)
                    if failed_cmds:
                        self.record_failed_secondary_connects(pvc_name, client, failed_cmds)
                except Exception as exc:
                    self.logger.warning(
                        f"[create_pvc] NVMe connect failed for {pvc_name}/{lvol_name}: {exc}"
                    )
                    self.record_failed_nvme_connect(pvc_name, {
                        "lvol_name": lvol_name,
                        "lvol_id": lvol_id,
                        "snap_name": None,
                        "client": client,
                        "fs_type": fs_type,
                    })
                    # Track PVC with placeholder fields — retry will fill them
                    self.pvc_details[pvc_name] = {
                        "job_name": None,
                        "configmap_name": None,
                        "snapshots": [],
                        "node_id": node_id,
                        "lvol_name": lvol_name,
                        "lvol_id": lvol_id,
                        "device": None,
                        "mount_path": None,
                        "client": client,
                        "log_file": None,
                        "fs_type": fs_type,
                        "storage_class": sc_name,
                    }
                    if node_id:
                        self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
                    continue

                self.ssh_obj.format_disk(node=client, device=device, fs_type=fs_type)
                mount_point = f"{self.mount_path}/{pvc_name}"
                self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_point)
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{pvc_name}.log"
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])

                # FIO warmup: write zeros to all blocks, wiping stale
                # FIO_HDR_MAGIC from thin-provisioned storage blocks.
                bs = f"{2 ** random.randint(2, 7)}K"
                try:
                    self._run_fio_warmup_ssh(pvc_name, client, mount_point, bs)
                except Exception as exc:
                    self.logger.warning(f"[create_pvc] FIO warmup failed for {pvc_name}: {exc}")

                self._start_client_fio(pvc_name, client, mount_point, log_file,
                                       bs=bs)

                self.pvc_details[pvc_name] = {
                    "job_name": None,
                    "configmap_name": None,
                    "snapshots": [],
                    "node_id": node_id,
                    "lvol_name": lvol_name,
                    "lvol_id": lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                    "fs_type": fs_type,
                    "storage_class": sc_name,
                }
                # Also track in lvol_mount_details for compatibility
                self.lvol_mount_details[lvol_name] = {
                    "ID": lvol_id,
                    "Mount": mount_point,
                    "Device": device,
                    "FS": fs_type,
                    "Log": log_file,
                    "Client": client,
                    "pvc_name": pvc_name,
                    "snapshots": [],
                }

                self.logger.info(
                    f"[create_pvc] PVC {pvc_name} → lvol {lvol_name} "
                    f"connected on {client} at {device}, FIO started"
                )
            else:
                # ── K8s Job FIO path (existing behaviour) ──
                job_name = f"fio-{pvc_name}"
                cm_name = f"fiocfg-{pvc_name}"

                node_id = self._get_pvc_node_id(pvc_name)
                avoid = self._get_k8s_node_for_storage_node(node_id) if node_id else None

                fio_config, warmup_config = self._build_fio_config(pvc_name)
                try:
                    self.k8s_utils.create_fio_job(
                        job_name, pvc_name, cm_name, fio_config,
                        image=self.FIO_IMAGE,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[create_pvc] FIO Job creation failed for {pvc_name}: {exc}"
                    )

                self.pvc_details[pvc_name] = {
                    "job_name": job_name,
                    "configmap_name": cm_name,
                    "snapshots": [],
                    "node_id": node_id,
                    "storage_class": sc_name,
                }

                self.logger.info(
                    f"[create_pvc] PVC {pvc_name} on node {node_id} with FIO Job {job_name} SC={sc_name}"
                )

            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details, snapshot_details=self.snapshot_details)

    def _ensure_per_node_coverage(self):
        """Verify every storage node has at least 1 PVC.  Create extras if needed."""
        # Refresh node_id mapping for any PVCs that don't have one yet
        for pvc_name, pvc_info in list(self.pvc_details.items()):
            if not pvc_info.get("node_id"):
                nid = self._get_pvc_node_id(pvc_name)
                if nid:
                    pvc_info["node_id"] = nid
                    self.node_vs_pvc.setdefault(nid, []).append(pvc_name)

        covered = set(self.node_vs_pvc.keys())
        missing = set(self.sn_nodes) - covered
        if missing:
            self.logger.info(
                f"[coverage] Nodes without PVC: {missing}. Creating {len(missing)} extra PVCs."
            )
            self.create_pvcs_with_fio(len(missing))
        else:
            self.logger.info("[coverage] All storage nodes have at least 1 PVC.")

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details, snapshot_details=self.snapshot_details)

    # ── Snapshot & Clone creation ────────────────────────────────────────────

    def create_snapshots_and_clones(self):
        """Create 1 snapshot + clone + FIO, then resize source & clone."""
        self._ensure_k8s_utils()
        self.int_pvc_size += 1
        available_pvcs = list(self.pvc_details.keys())
        if not available_pvcs:
            self.logger.warning("[snap_clone] No PVCs available for snapshots")
            return

        for idx in range(1):
            random.shuffle(available_pvcs)
            pvc_name = available_pvcs[0]
            snap_name = f"snap-{_rand_seq(12)}"
            clone_name = f"clone-{_rand_seq(12)}"

            # Create snapshot (k8s-native in both modes)
            try:
                self.k8s_utils.create_volume_snapshot(
                    snap_name, pvc_name, self.SNAPSHOT_CLASS_NAME
                )
                self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Snapshot creation failed for {snap_name}: {exc}")
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                    self.logger.info(f"[snap_clone] Cleaned up orphaned snapshot {snap_name}")
                except Exception:
                    self.logger.warning(f"[snap_clone] Could not clean up snapshot {snap_name}")
                continue

            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}
            self.snapshot_names.append(snap_name)
            self.pvc_details[pvc_name]["snapshots"].append(snap_name)

            # Snapshot lvol IDs before clone PVC (for client mode mapping)
            old_lvol_ids = self._snapshot_lvol_ids() if self.use_client_fio else set()

            # Create clone PVC — use same StorageClass as source PVC
            clone_sc = self.pvc_details.get(pvc_name, {}).get("storage_class", self.STORAGE_CLASS_NAME)
            sleep_n_sec(10)
            try:
                self.k8s_utils.create_clone_pvc(
                    clone_name, self.pvc_size, clone_sc, snap_name
                )
                self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Clone PVC creation failed for {clone_name}: {exc}")
                try:
                    self.k8s_utils.delete_pvc(clone_name)
                    self.logger.info(f"[snap_clone] Cleaned up orphaned clone PVC {clone_name}")
                except Exception:
                    self.logger.warning(f"[snap_clone] Could not clean up clone PVC {clone_name}")
                continue

            if self.use_client_fio:
                # ── Client FIO path for clone ──
                sleep_n_sec(5)
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[snap_clone] Could not map clone {clone_name} to lvol — skipping"
                    )
                    continue
                clone_lvol_name, clone_lvol_id = lvol_info

                client = self.fio_node[idx % len(self.fio_node)]

                try:
                    device, failed_cmds = self._connect_lvol_on_client(clone_lvol_name, client)
                    if failed_cmds:
                        self.record_failed_secondary_connects(clone_name, client, failed_cmds)
                except Exception as exc:
                    self.logger.warning(
                        f"[snap_clone] NVMe connect failed for clone {clone_name}: {exc}"
                    )
                    self.record_failed_nvme_connect(clone_name, {
                        "lvol_name": clone_lvol_name,
                        "lvol_id": clone_lvol_id,
                        "snap_name": snap_name,
                        "client": client,
                    })
                    # Track clone with placeholder fields — retry will fill them
                    self.clone_details[clone_name] = {
                        "snap_name": snap_name,
                        "job_name": None,
                        "configmap_name": None,
                        "lvol_name": clone_lvol_name,
                        "lvol_id": clone_lvol_id,
                        "device": None,
                        "mount_path": None,
                        "client": client,
                        "log_file": None,
                        "storage_class": clone_sc,
                    }
                    continue

                # Clone volumes from snapshots already have data; regenerate UUID
                # before mounting to avoid collisions with the source.
                self.ssh_obj.clone_mount_gen_uuid(client, device)
                mount_point = f"{self.mount_path}/{clone_name}"
                self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_point)
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{clone_name}.log"
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
                self._start_client_fio(clone_name, client, mount_point, log_file)

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": None,
                    "configmap_name": None,
                    "lvol_name": clone_lvol_name,
                    "lvol_id": clone_lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                    "storage_class": clone_sc,
                }
                self.clone_mount_details[clone_lvol_name] = {
                    "ID": clone_lvol_id,
                    "snapshot": snap_name,
                    "Mount": mount_point,
                    "Device": device,
                    "Log": log_file,
                    "Client": client,
                    "clone_pvc": clone_name,
                }

                self.logger.info(
                    f"[snap_clone] Clone {clone_name} → lvol {clone_lvol_name} "
                    f"connected on {client}, FIO started"
                )
            else:
                # ── K8s Job FIO path — cleanup old FIO files from source ──
                clone_job = f"fio-{clone_name}"
                clone_cm = f"fiocfg-{clone_name}"
                clone_node_id = self._get_pvc_node_id(clone_name)
                avoid = self._get_k8s_node_for_storage_node(clone_node_id) if clone_node_id else None

                fio_config, warmup_config = self._build_fio_config(clone_name)
                try:
                    self.k8s_utils.create_fio_job(
                        clone_job, clone_name, clone_cm, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(f"[snap_clone] Clone FIO Job failed for {clone_name}: {exc}")

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": clone_job,
                    "configmap_name": clone_cm,
                    "storage_class": clone_sc,
                }

            # Resize source PVC and clone PVC
            try:
                self.k8s_utils.resize_pvc(pvc_name, f"{self.int_pvc_size}Gi")
                sleep_n_sec(5)
                self.k8s_utils.resize_pvc(clone_name, f"{self.int_pvc_size}Gi")
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Resize failed: {exc}")

            self.logger.info(
                f"[snap_clone] Created snapshot {snap_name}, clone {clone_name}, "
                f"resized to {self.int_pvc_size}Gi"
            )
            sleep_n_sec(10)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details, snapshot_details=self.snapshot_details)

    # ── Delete PVCs ──────────────────────────────────────────────────────────

    def delete_random_pvcs(self, count: int):
        """Delete *count* random PVCs and their snapshots/clones, then recreate
        the same number so every node keeps at least 1 PVC."""
        self._ensure_k8s_utils()
        available = list(self.pvc_details.keys())
        if len(available) < count:
            self.logger.warning(
                f"[delete_pvcs] Only {len(available)} PVCs available, requested {count}"
            )
            count = len(available)
        if count == 0:
            return

        # Only delete PVCs from nodes that have more than 1, so no node goes to zero
        safe_to_delete = []
        for pvc_name in available:
            nid = self.pvc_details[pvc_name].get("node_id")
            if nid and len(self.node_vs_pvc.get(nid, [])) > 1:
                safe_to_delete.append(pvc_name)
        if not safe_to_delete:
            # Fallback: allow deletion but we will recreate afterwards
            safe_to_delete = available
        targets = random.sample(safe_to_delete, min(count, len(safe_to_delete)))
        deleted_node_ids = []

        for pvc_name in targets:
            self.logger.info(f"[delete_pvcs] Deleting PVC tree: {pvc_name}")
            pvc_info = self.pvc_details[pvc_name]

            # Delete clones for each snapshot of this PVC
            for snap_name in list(pvc_info["snapshots"]):
                clones_to_delete = [
                    cn for cn, cd in self.clone_details.items()
                    if cd["snap_name"] == snap_name
                ]
                for clone_name in clones_to_delete:
                    clone_info = self.clone_details[clone_name]

                    if self.use_client_fio:
                        # Stop FIO, unmount, disconnect NVMe on client
                        client = clone_info["client"]
                        self._kill_fio_on_client(clone_name, client)
                        sleep_n_sec(5)
                        try:
                            self.ssh_obj.unmount_path(client, clone_info["mount_path"])
                            self.ssh_obj.remove_dir(client, dir_path=clone_info["mount_path"])
                        except Exception as exc:
                            self.logger.warning(f"[delete_pvcs] Clone unmount failed: {exc}")
                        self._disconnect_lvol_on_client(clone_info["lvol_name"], client)
                        # Clean lvol_mount_details
                        self.clone_mount_details.pop(clone_info.get("lvol_name"), None)
                    else:
                        try:
                            self.k8s_utils.delete_job(clone_info["job_name"])
                            self.k8s_utils.delete_configmap(clone_info["configmap_name"])
                        except Exception as exc:
                            self.logger.warning(f"[delete_pvcs] Clone Job cleanup failed: {exc}")

                    try:
                        self.k8s_utils.delete_pvc(clone_name)
                    except Exception as exc:
                        self.logger.warning(f"[delete_pvcs] Clone PVC delete failed: {exc}")
                        self.record_pending_clone_delete(clone_name, clone_info)
                    del self.clone_details[clone_name]

                # Delete the snapshot
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception as exc:
                    self.logger.warning(f"[delete_pvcs] Snapshot delete failed: {exc}")
                    self.record_pending_snapshot_delete(
                        snap_name, self.snapshot_details.get(snap_name, {})
                    )
                self.snapshot_details.pop(snap_name, None)
                if snap_name in self.snapshot_names:
                    self.snapshot_names.remove(snap_name)

            # Stop FIO for the PVC itself
            if self.use_client_fio:
                client = pvc_info["client"]
                self._kill_fio_on_client(pvc_name, client)
                sleep_n_sec(5)
                try:
                    self.ssh_obj.unmount_path(client, pvc_info["mount_path"])
                    self.ssh_obj.remove_dir(client, dir_path=pvc_info["mount_path"])
                except Exception as exc:
                    self.logger.warning(f"[delete_pvcs] PVC unmount failed: {exc}")
                self._disconnect_lvol_on_client(pvc_info["lvol_name"], client)
                self.lvol_mount_details.pop(pvc_info.get("lvol_name"), None)
            else:
                try:
                    self.k8s_utils.delete_job(pvc_info["job_name"])
                    self.k8s_utils.delete_configmap(pvc_info["configmap_name"])
                except Exception as exc:
                    self.logger.warning(f"[delete_pvcs] PVC Job cleanup failed: {exc}")

            # Delete the PVC (k8s-native — CSI will remove underlying lvol)
            try:
                self.k8s_utils.delete_pvc(pvc_name)
            except Exception as exc:
                self.logger.warning(f"[delete_pvcs] PVC delete failed: {exc}")
                self.record_pending_pvc_delete(pvc_name, pvc_info)

            # Clean up tracking
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            del self.pvc_details[pvc_name]
            if node_id:
                deleted_node_ids.append(node_id)

        sleep_n_sec(30)

        # Recreate PVCs — only pin to nodes that are currently online
        # (during an outage some of the deleted_node_ids may be offline,
        #  causing PVC creation to time out).
        if deleted_node_ids:
            online_ids = []
            unpinned_count = 0
            for nid in deleted_node_ids:
                try:
                    details = self.sbcli_utils.get_storage_node_details(nid)
                    if details and details[0].get("status") == "online":
                        online_ids.append(nid)
                    else:
                        unpinned_count += 1
                        self.logger.warning(
                            f"[delete_pvcs] Node {nid} not online — "
                            f"will create replacement PVC without pinning"
                        )
                except Exception:
                    unpinned_count += 1

            if online_ids:
                self.logger.info(
                    f"[delete_pvcs] Recreating {len(online_ids)} PVCs "
                    f"pinned to online nodes: {online_ids}"
                )
                self.create_pvcs_with_fio(len(online_ids), node_ids=online_ids)
            if unpinned_count:
                self.logger.info(
                    f"[delete_pvcs] Recreating {unpinned_count} PVCs "
                    f"without node pinning (original nodes offline)"
                )
                self.create_pvcs_with_fio(unpinned_count)
        self._ensure_per_node_coverage()

    # ── Restart FIO ──────────────────────────────────────────────────────────

    def restart_fio(self, iteration: int):
        """Restart FIO on all PVCs and clones.

        Client mode: kill fio process, start new fio via SSH.
        K8s mode: delete old Job, create new Job.
        """
        self._ensure_k8s_utils()
        self.logger.info(f"[restart_fio] Restarting FIO for iteration {iteration}")

        if self.use_client_fio:
            # ── Client FIO restart ──
            skipped = 0
            for pvc_name, pvc_info in self.pvc_details.items():
                client = pvc_info.get("client")
                mount_point = pvc_info.get("mount_path")
                if not mount_point or not client:
                    self.logger.warning(
                        f"[restart_fio] Skipping {pvc_name}: "
                        f"mount_path={mount_point}, client={client} (incomplete setup)"
                    )
                    skipped += 1
                    continue
                log_file = f"{self.log_path}/{pvc_name}-{iteration}.log"

                self._kill_fio_on_client(pvc_name, client)
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
                self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{pvc_name}*"])
                sleep_n_sec(5)

                # Warmup: write zeros to wipe stale FIO_HDR_MAGIC before
                # new FIO starts — mirrors the create_pvcs_with_fio flow.
                bs = f"{2 ** random.randint(2, 7)}K"
                try:
                    self._run_fio_warmup_ssh(pvc_name, client, mount_point, bs)
                except Exception as exc:
                    self.logger.warning(f"[restart_fio] FIO warmup failed for {pvc_name}: {exc}")

                pvc_info["log_file"] = log_file
                self._start_client_fio(pvc_name, client, mount_point, log_file,
                                       bs=bs)
                sleep_n_sec(10)

            for clone_name, clone_info in self.clone_details.items():
                client = clone_info.get("client")
                mount_point = clone_info.get("mount_path")
                if not mount_point or not client:
                    self.logger.warning(
                        f"[restart_fio] Skipping clone {clone_name}: "
                        f"mount_path={mount_point}, client={client} (incomplete setup)"
                    )
                    skipped += 1
                    continue
                log_file = f"{self.log_path}/{clone_name}-{iteration}.log"

                self._kill_fio_on_client(clone_name, client)
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
                self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}*"])
                sleep_n_sec(5)

                bs = f"{2 ** random.randint(2, 7)}K"
                try:
                    self._run_fio_warmup_ssh(clone_name, client, mount_point, bs)
                except Exception as exc:
                    self.logger.warning(f"[restart_fio] FIO warmup failed for {clone_name}: {exc}")

                clone_info["log_file"] = log_file
                self._start_client_fio(clone_name, client, mount_point, log_file,
                                       bs=bs)
                sleep_n_sec(10)
        else:
            # ── K8s Job FIO restart (existing behaviour) ──
            for pvc_name, pvc_info in self.pvc_details.items():
                old_job = pvc_info["job_name"]
                old_cm = pvc_info["configmap_name"]
                new_job = f"fio-{pvc_name}-{iteration}"
                new_cm = f"fiocfg-{pvc_name}-{iteration}"

                try:
                    self.k8s_utils.delete_job(old_job)
                    self.k8s_utils.delete_configmap(old_cm)
                except Exception:
                    pass

                fio_config, warmup_config = self._build_fio_config(pvc_name)
                nid = pvc_info.get("node_id")
                avoid = self._get_k8s_node_for_storage_node(nid) if nid else None
                try:
                    self.k8s_utils.create_fio_job(
                        new_job, pvc_name, new_cm, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(f"[restart_fio] Failed to restart FIO for {pvc_name}: {exc}")
                    continue

                pvc_info["job_name"] = new_job
                pvc_info["configmap_name"] = new_cm
                sleep_n_sec(5)

            for clone_name, clone_info in self.clone_details.items():
                old_job = clone_info["job_name"]
                old_cm = clone_info["configmap_name"]
                new_job = f"fio-{clone_name}-{iteration}"
                new_cm = f"fiocfg-{clone_name}-{iteration}"

                try:
                    self.k8s_utils.delete_job(old_job)
                    self.k8s_utils.delete_configmap(old_cm)
                except Exception:
                    pass

                fio_config, warmup_config = self._build_fio_config(clone_name)
                clone_nid = self._get_pvc_node_id(clone_name)
                avoid = self._get_k8s_node_for_storage_node(clone_nid) if clone_nid else None
                try:
                    self.k8s_utils.create_fio_job(
                        new_job, clone_name, new_cm, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(f"[restart_fio] Failed to restart FIO for clone {clone_name}: {exc}")
                    continue

                clone_info["job_name"] = new_job
                clone_info["configmap_name"] = new_cm
                sleep_n_sec(5)

    # ── Outage methods ───────────────────────────────────────────────────────

    def _build_reverse_secondary_map(self):
        rev = defaultdict(set)
        for p, s in self.sn_primary_secondary_map.items():
            if s:
                rev[s].add(p)
        return rev

    def _pick_outage_nodes(self, primary_candidates, k):
        rev = self._build_reverse_secondary_map()
        order = primary_candidates[:]
        random.shuffle(order)

        chosen, blocked = [], set()
        for node in order:
            if node in blocked:
                continue
            chosen.append(node)
            blocked.add(node)
            sec = self.sn_primary_secondary_map.get(node)
            if sec:
                blocked.add(sec)
            blocked.update(rev.get(node, ()))
            if len(chosen) == k:
                break

        if len(chosen) < k:
            raise Exception(
                f"Cannot pick {k} nodes without primary/secondary conflicts; "
                f"only {len(chosen)} possible."
            )
        return chosen

    def _k8s_stop_spdk_pod(self, node_ip: str, node_id: str):
        self._ensure_k8s_utils()
        pod_name = self.k8s_utils.stop_spdk_pod(node_ip)
        self.logger.info(
            f"[K8s] container_stop: deleted SPDK pod {pod_name!r} for node {node_ip}"
        )

    def _graceful_shutdown_node(self, node: str):
        self.logger.info(f"Issuing graceful shutdown for node {node}.")
        deadline = time.time() + 300
        while True:
            try:
                self.sbcli_utils.shutdown_node(node_uuid=node, force=False)
            except Exception as e:
                self.logger.warning(f"shutdown_node raised: {e}")
            sleep_n_sec(20)
            node_detail = self.sbcli_utils.get_storage_node_details(node)
            if node_detail[0]["status"] == "offline":
                self.logger.info(f"Node {node} is offline.")
                return
            if time.time() >= deadline:
                raise RuntimeError(
                    f"Node {node} did not go offline within 5 minutes."
                )
            self.logger.info(f"Node {node} not yet offline; retrying shutdown...")

    def perform_n_plus_k_outages(self):
        """Select K nodes and trigger outages simultaneously.

        When max_fault_tolerance >= 2 the cluster can survive losing any two
        nodes including a primary+secondary pair, so outage candidates include
        ALL nodes (primary and secondary alike) and no primary/secondary
        exclusion is applied.  For FTT=1, the original logic blocks the
        secondary partner of each chosen node to avoid losing both copies of
        the same replica group.
        """
        if self.max_fault_tolerance >= 2:
            # FTT>=2: pick from ALL nodes, no exclusion needed
            all_nodes = list(self.sn_nodes_with_sec)
            candidates = all_nodes
        else:
            # FTT=1: pick from primaries only (secondary blocked)
            candidates = list(self.sn_primary_secondary_map.keys())

        self.current_outage_nodes = []

        if len(candidates) < self.npcs:
            raise Exception(
                f"Need {self.npcs} outage nodes, but only "
                f"{len(candidates)} candidate nodes exist."
            )

        if self.max_fault_tolerance >= 2:
            outage_nodes = random.sample(candidates, self.npcs)
        else:
            outage_nodes = self._pick_outage_nodes(candidates, self.npcs)
        self.logger.info(f"Selected outage nodes: {outage_nodes} (FTT={self.max_fault_tolerance})")
        self.collect_outage_diagnostics(f"pre_outage_nodes_{'_'.join(outage_nodes[:3])}")

        node_plans = []
        for i, node in enumerate(outage_nodes):
            if i == 0:
                # First outage is always graceful_shutdown
                outage_type = "graceful_shutdown"
            else:
                outage_type = random.choice(self.outage_types2)

            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]
            node_plans.append((node, outage_type, node_ip, node_rpc_port))

        # Trigger outages in parallel threads with a 10s delay between launches
        outage_combinations = []
        outage_errors = {}

        def _run_outage(node, outage_type, node_ip):
            try:
                self.logger.info(f"Performing {outage_type} on node {node}")
                if outage_type == "container_stop":
                    self._k8s_stop_spdk_pod(node_ip, node)
                elif outage_type == "graceful_shutdown":
                    self._graceful_shutdown_node(node)
                self.log_outage_event(node, outage_type, "Outage started")
            except Exception as e:
                self.logger.error(f"Outage {outage_type} on node {node} failed: {e}")
                outage_errors[node] = e

        threads = []
        for idx, (node, outage_type, node_ip, _rpc_port) in enumerate(node_plans):
            if idx > 0:
                self.logger.info("Waiting 10s before launching next outage thread...")
                sleep_n_sec(10)
            t = threading.Thread(target=_run_outage, args=(node, outage_type, node_ip))
            t.start()
            threads.append(t)
            outage_combinations.append((node, outage_type, 0))
            self.current_outage_nodes.append(node)

        for t in threads:
            t.join(timeout=600)

        if outage_errors:
            failed = ", ".join(f"{n}: {e}" for n, e in outage_errors.items())
            raise RuntimeError(f"Outage(s) failed: {failed}")

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    # ── Recovery ─────────────────────────────────────────────────────────────

    def restart_nodes_after_failover(self, outage_type, restart=False):
        """Restart the current_outage_node and wait for it to come back online."""
        node = self.current_outage_node
        self.logger.info(f"Waiting for {outage_type} recovery on node {node}")

        if outage_type == "graceful_shutdown":
            max_retries = 4
            for attempt in range(max_retries):
                try:
                    force = attempt == max_retries - 1
                    self.sbcli_utils.restart_node(node_uuid=node, force=force)
                    self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=300)
                    break
                except Exception:
                    if attempt < max_retries - 1:
                        self.logger.info(
                            f"Restart attempt {attempt+1} failed; retrying in 10s..."
                        )
                        sleep_n_sec(10)
                    else:
                        raise
            self.log_outage_event(node, outage_type, "Node restarted")

        elif outage_type == "container_stop":
            if restart:
                try:
                    self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=60)
                    self.log_outage_event(node, outage_type, "Node restarted", outage_time=2)
                except Exception:
                    # Node didn't come back automatically — force restart
                    max_retries = 4
                    for attempt in range(max_retries):
                        try:
                            force = attempt == max_retries - 1
                            self.sbcli_utils.restart_node(node_uuid=node, force=force)
                            self.sbcli_utils.wait_for_storage_node_status(
                                node, "online", timeout=300
                            )
                            break
                        except Exception:
                            if attempt < max_retries - 1:
                                sleep_n_sec(10)
                            else:
                                raise
                    self.log_outage_event(node, outage_type, "Node restarted")
            else:
                self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=300)
                self.log_outage_event(node, outage_type, "Node restarted", outage_time=2)

        # Health check deferred to after all outage nodes are online
        self.outage_end_time = int(datetime.now().timestamp())

    # ── IO Stats Validation ──────────────────────────────────────────────────

    def validate_iostats_continuously(self):
        """Background thread: validate IO stats every 300s."""
        while True:
            try:
                start_ts = datetime.now().timestamp()
                end_ts = start_ts + 300
                self.common_utils.validate_io_stats(
                    cluster_id=self.cluster_id,
                    start_timestamp=start_ts,
                    end_timestamp=end_ts,
                    time_duration=None,
                    warn_only=True,
                )
                sleep_n_sec(300)
            except Exception as e:
                self.logger.error(f"IO stats validation error: {e}")
                break

    # ── Deferred operation retry ─────────────────────────────────────────────

    def validate_pending_deletions(self, timeout=600, interval=30):
        """Retry deferred deletions after recovery, respecting dependency order.

        Order: clones → snapshots → PVCs.
        Clones must be deleted before their parent snapshots, and snapshots
        before their parent PVCs.
        """
        self._ensure_k8s_utils()
        pending = self.pending_deletions
        if not any(pending[k] for k in pending):
            self.logger.info("[deferred] No pending deletions")
            return

        self.logger.info(
            f"[deferred] Retrying deletions: "
            f"{len(pending['clones'])} clones, "
            f"{len(pending['snapshots'])} snapshots, "
            f"{len(pending['pvcs'])} PVCs"
        )
        start = time.time()

        while time.time() - start < timeout:
            # Phase 1: clones first
            for clone_name in list(pending["clones"]):
                try:
                    resource = self.k8s_utils.get_resource_json("pvc", clone_name)
                    if not resource:
                        self.logger.info(f"[deferred] Clone {clone_name} already gone")
                        del pending["clones"][clone_name]
                        continue
                    self.k8s_utils.delete_pvc(clone_name)
                    self.logger.info(f"[deferred] Re-issued delete for clone {clone_name}")
                    del pending["clones"][clone_name]
                except Exception as exc:
                    self.logger.warning(
                        f"[deferred] Clone delete retry failed for {clone_name}: {exc}"
                    )

            # Phase 2: snapshots (only after all clones gone)
            if not pending["clones"]:
                for snap_name in list(pending["snapshots"]):
                    try:
                        resource = self.k8s_utils.get_resource_json(
                            "volumesnapshot", snap_name
                        )
                        if not resource:
                            self.logger.info(
                                f"[deferred] Snapshot {snap_name} already gone"
                            )
                            del pending["snapshots"][snap_name]
                            continue
                        self.k8s_utils.delete_volume_snapshot(snap_name)
                        self.logger.info(
                            f"[deferred] Re-issued delete for snapshot {snap_name}"
                        )
                        del pending["snapshots"][snap_name]
                    except Exception as exc:
                        self.logger.warning(
                            f"[deferred] Snapshot delete retry failed for {snap_name}: {exc}"
                        )
            else:
                self.logger.info(
                    f"[deferred] Skipping snapshot retry — "
                    f"{len(pending['clones'])} clone(s) still pending"
                )

            # Phase 3: PVCs (only after snapshots gone)
            if not pending["clones"] and not pending["snapshots"]:
                for pvc_name in list(pending["pvcs"]):
                    try:
                        resource = self.k8s_utils.get_resource_json("pvc", pvc_name)
                        if not resource:
                            self.logger.info(
                                f"[deferred] PVC {pvc_name} already gone"
                            )
                            del pending["pvcs"][pvc_name]
                            continue
                        self.k8s_utils.delete_pvc(pvc_name)
                        self.logger.info(
                            f"[deferred] Re-issued delete for PVC {pvc_name}"
                        )
                        del pending["pvcs"][pvc_name]
                    except Exception as exc:
                        self.logger.warning(
                            f"[deferred] PVC delete retry failed for {pvc_name}: {exc}"
                        )

            if not any(pending[k] for k in pending):
                self.logger.info("[deferred] All pending deletions completed")
                return

            sleep_n_sec(interval)

        remaining = {k: list(v.keys()) for k, v in pending.items() if v}
        self.logger.warning(
            f"[deferred] Deletions did not fully converge within {timeout}s. "
            f"Remaining: {remaining}"
        )

    def retry_failed_nvme_connects(self, timeout=600, interval=30):
        """Retry deferred NVMe connects after recovery (client mode only).

        For each deferred resource, retries the connect and completes the
        mount + FIO setup that was skipped during the outage.
        """
        if not self.failed_nvme_connects:
            self.logger.info("[retry_nvme] No deferred NVMe connects pending")
            return
        if not self.use_client_fio:
            self.logger.warning(
                "[retry_nvme] Not in client FIO mode — clearing deferred connects"
            )
            self.failed_nvme_connects.clear()
            return

        self.logger.info(
            f"[retry_nvme] Retrying {len(self.failed_nvme_connects)} "
            f"deferred NVMe connects"
        )
        start = time.time()

        while time.time() - start < timeout:
            for name in list(self.failed_nvme_connects):
                info = self.failed_nvme_connects[name]
                lvol_name = info["lvol_name"]
                client = info["client"]

                try:
                    device, failed_cmds = self._connect_lvol_on_client(lvol_name, client)
                    if failed_cmds:
                        self.record_failed_secondary_connects(name, client, failed_cmds)
                except Exception as exc:
                    self.logger.warning(
                        f"[retry_nvme] Connect still failing for {name}: {exc}"
                    )
                    continue

                # Connect succeeded — complete the setup
                try:
                    snap_name = info.get("snap_name")
                    lvol_id = info.get("lvol_id")

                    if snap_name:
                        # Clone: regenerate UUID before mounting
                        self.ssh_obj.clone_mount_gen_uuid(client, device)
                    else:
                        # Direct PVC: format the disk
                        fs_type = info.get("fs_type", "ext4")
                        self.ssh_obj.format_disk(
                            node=client, device=device, fs_type=fs_type
                        )

                    mount_point = f"{self.mount_path}/{name}"
                    self.ssh_obj.mount_path(
                        node=client, device=device, mount_path=mount_point
                    )
                    sleep_n_sec(5)

                    log_file = f"{self.log_path}/{name}.log"
                    self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])

                    # Warmup for direct PVCs
                    bs = f"{2 ** random.randint(2, 7)}K"
                    if not snap_name:
                        try:
                            self._run_fio_warmup_ssh(name, client, mount_point, bs)
                        except Exception as exc:
                            self.logger.warning(
                                f"[retry_nvme] FIO warmup failed for {name}: {exc}"
                            )

                    self._start_client_fio(name, client, mount_point, log_file,
                                           bs=bs)

                    # Update tracking dicts
                    if snap_name:
                        self.clone_details[name].update({
                            "device": device,
                            "mount_path": mount_point,
                            "log_file": log_file,
                        })
                        self.clone_mount_details[lvol_name] = {
                            "ID": lvol_id,
                            "snapshot": snap_name,
                            "Mount": mount_point,
                            "Device": device,
                            "Log": log_file,
                            "Client": client,
                            "clone_pvc": name,
                        }
                    else:
                        self.pvc_details[name].update({
                            "device": device,
                            "mount_path": mount_point,
                            "log_file": log_file,
                        })
                        self.lvol_mount_details[lvol_name] = {
                            "ID": lvol_id,
                            "Command": None,
                            "Mount": mount_point,
                            "Device": device,
                            "FS": info.get("fs_type", "ext4"),
                            "Log": log_file,
                            "Client": client,
                        }

                    self.logger.info(
                        f"[retry_nvme] {name} connected on {client} at {device}"
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[retry_nvme] Post-connect setup failed for {name}: {exc}"
                    )
                    continue

                del self.failed_nvme_connects[name]

            if not self.failed_nvme_connects:
                self.logger.info("[retry_nvme] All deferred NVMe connects completed")
                return

            sleep_n_sec(interval)

        remaining = list(self.failed_nvme_connects.keys())
        self.logger.error(
            f"[retry_nvme] Deferred NVMe connects did not converge within "
            f"{timeout}s. Remaining: {remaining}"
        )
        self.failed_nvme_connects.clear()
        raise AssertionError(
            f"NVMe connects did not recover after {timeout}s: {remaining}"
        )

    # ── Wait for FIO completion ─────────────────────────────────────────────

    def wait_for_fio_complete(self, timeout: int = None):
        """Wait for all active FIO workloads to finish naturally.

        Client mode: poll fio processes on client hosts until none remain.
        K8s mode: wait for all FIO K8s Jobs to reach completion.

        Args:
            timeout: Max seconds to wait. Defaults to FIO_RUNTIME + 300.
        """
        if timeout is None:
            timeout = self.FIO_RUNTIME + 4000

        if self.use_client_fio:
            self.logger.info(
                f"[wait_fio] Waiting for FIO to complete on client hosts "
                f"(timeout={timeout}s) ..."
            )
            # Join FIO launch threads first
            for t in self.fio_threads:
                t.join(timeout=10)
            self.fio_threads = []

            self.common_utils.manage_fio_threads(
                self.fio_node, [], timeout=timeout
            )
            self.logger.info("[wait_fio] All client FIO processes finished.")
        else:
            self._ensure_k8s_utils()
            self.logger.info(
                f"[wait_fio] Waiting for FIO K8s Jobs to complete "
                f"(timeout={timeout}s) ..."
            )
            for pvc_name, pvc_info in self.pvc_details.items():
                job_name = pvc_info.get("job_name")
                if job_name:
                    try:
                        self.k8s_utils.wait_job_complete(
                            job_name, timeout=timeout
                        )
                    except Exception as exc:
                        self.logger.warning(
                            f"[wait_fio] Job {job_name} did not complete: {exc}"
                        )
            for clone_name, clone_info in self.clone_details.items():
                job_name = clone_info.get("job_name")
                if job_name:
                    try:
                        self.k8s_utils.wait_job_complete(
                            job_name, timeout=timeout
                        )
                    except Exception as exc:
                        self.logger.warning(
                            f"[wait_fio] Job {job_name} did not complete: {exc}"
                        )
            self.logger.info("[wait_fio] All K8s FIO Jobs finished.")

    # ── FIO Validation ───────────────────────────────────────────────────────

    def _save_fio_pod_logs(self, job_name: str, resource_name: str,
                           pvc_name: str = None):
        """Save FIO pod logs and performance data to local log directory."""
        try:
            pod_name = self.k8s_utils.get_job_pod_name(job_name)
            if not pod_name:
                return
            # Save kubectl logs (stdout/stderr)
            logs = self.k8s_utils.get_pod_logs(pod_name, tail=2000)
            if logs:
                log_file = os.path.join(self.log_path, f"{resource_name}_fio.log")
                with open(log_file, "w") as f:
                    f.write(logs)
                self.logger.info(f"Saved FIO logs for {resource_name} to {log_file}")

            # Copy FIO performance logs from /spdkvol/ inside the pod
            self._copy_fio_perf_logs(pod_name, resource_name, pvc_name=pvc_name)
        except Exception as exc:
            self.logger.warning(f"Could not save FIO logs for {resource_name}: {exc}")

    # ── FIO perf-log helpers ──────────────────────────────────────────────

    def _list_fio_perf_files(self, pod_name: str, ns: str,
                              container: str = None) -> list[str]:
        """List FIO-generated perf files in /spdkvol/ of a *running* pod.

        Returns a list of absolute paths inside the container, or ``[]`` if
        the pod is not running or the command fails.
        """
        container_flag = f"-c {container} " if container else ""
        try:
            file_list, _ = self.k8s_utils._exec_kubectl(
                f"kubectl exec {container_flag}{pod_name} -n {ns} -- "
                f"find /spdkvol/ -maxdepth 1 "
                f"\\( -name '*fio*.log' -o -name '*-iolog.log' -o -name '*_lat.*' "
                f"-o -name '*_bw.*' -o -name '*_iops.*' -o -name '*_clat.*' "
                f"-o -name '*_slat.*' \\) "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            return [f.strip() for f in file_list.strip().splitlines() if f.strip()]
        except Exception:
            return []

    def _create_copier_pod(self, copier_name: str, pvc_name: str,
                            node_name: str, ns: str):
        """Create a lightweight busybox pod on *node_name* mounting *pvc_name*.

        Used as a fallback to copy FIO perf files from the PVC volume after
        the original FIO pod has completed (``kubectl exec`` no longer works).
        """
        yaml_spec = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {copier_name}\n"
            f"  namespace: {ns}\n"
            f"  labels:\n"
            f"    app: fio-copier\n"
            f"spec:\n"
            f"  nodeName: {node_name}\n"
            f"  tolerations:\n"
            f"  - operator: Exists\n"
            f"  containers:\n"
            f"  - name: copier\n"
            f"    image: busybox\n"
            f"    command: ['sleep', '300']\n"
            f"    volumeMounts:\n"
            f"    - mountPath: /spdkvol\n"
            f"      name: vol\n"
            f"  volumes:\n"
            f"  - name: vol\n"
            f"    persistentVolumeClaim:\n"
            f"      claimName: {pvc_name}\n"
            f"  restartPolicy: Never\n"
        )
        self.k8s_utils._exec_kubectl(
            f"cat <<'COPIER_EOF' | kubectl apply -f -\n{yaml_spec}COPIER_EOF",
        )
        self.k8s_utils._exec_kubectl(
            f"kubectl wait pod/{copier_name} -n {ns} "
            f"--for=condition=Ready --timeout=120s",
        )

    def _copy_fio_perf_logs(self, pod_name: str, resource_name: str,
                             pvc_name: str = None):
        """Copy FIO perf log files (lat, bw, iops, iolog) from /spdkvol/ in
        the pod to the local ClientLogs directory.

        FIO writes files like ``{name}-fio_bw.1.log``, ``{name}-iolog.log``
        inside the PVC mount.  This method first tries ``kubectl exec`` on the
        original FIO pod.  If that fails (pod Completed), it creates a
        temporary copier pod that mounts the same PVC and copies from there.
        """
        ns = self.k8s_utils.namespace
        perf_dir = os.path.join(self.log_path, f"{resource_name}_perf")
        copier_name = None
        copy_from_pod = pod_name
        container = None

        try:
            # 1. Try direct exec on the FIO pod (works if still running)
            files = self._list_fio_perf_files(pod_name, ns)

            # 2. Fallback: create a copier pod mounting the same PVC
            if not files and pvc_name:
                node_name = self.k8s_utils.get_pod_node_name(pod_name)
                if node_name:
                    copier_name = f"fio-cp-{_rand_seq(8)}"
                    self.logger.info(
                        f"[perf_copy] FIO pod {pod_name} not running; "
                        f"creating copier pod {copier_name} on {node_name} "
                        f"for PVC {pvc_name}"
                    )
                    try:
                        self._create_copier_pod(copier_name, pvc_name,
                                                node_name, ns)
                        files = self._list_fio_perf_files(
                            copier_name, ns, container="copier"
                        )
                        copy_from_pod = copier_name
                        container = "copier"
                    except Exception as exc:
                        self.logger.warning(
                            f"[perf_copy] Copier pod failed for "
                            f"{resource_name}: {exc}"
                        )
                        files = []

            if not files:
                self.logger.info(
                    f"No FIO perf logs found for {resource_name} "
                    f"(pod={pod_name})"
                )
                return

            os.makedirs(perf_dir, exist_ok=True)
            container_flag = f" -c {container}" if container else ""
            for src_path in files:
                fname = os.path.basename(src_path)
                dest = os.path.join(perf_dir, fname)
                self.k8s_utils._exec_kubectl(
                    f"kubectl cp "
                    f"{ns}/{copy_from_pod}:{src_path} {dest}"
                    f"{container_flag} "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
            self.logger.info(
                f"Copied {len(files)} FIO perf log(s) for {resource_name} "
                f"to {perf_dir}"
            )
        except Exception as exc:
            self.logger.warning(
                f"Could not copy FIO perf logs for {resource_name} "
                f"from pod {pod_name}: {exc}"
            )
        finally:
            # Clean up copier pod if we created one
            if copier_name:
                try:
                    self.k8s_utils._exec_kubectl(
                        f"kubectl delete pod {copier_name} -n {ns} "
                        f"--force --grace-period=0 2>/dev/null || true",
                        supress_logs=True,
                    )
                except Exception:
                    pass

    def _save_all_fio_logs(self):
        """Save FIO pod logs and perf files for ALL PVCs and clones,
        regardless of success or failure."""
        self._ensure_k8s_utils()
        if self.use_client_fio:
            return
        for pvc_name, pvc_info in self.pvc_details.items():
            job_name = pvc_info.get("job_name")
            if job_name:
                self._save_fio_pod_logs(job_name, pvc_name, pvc_name=pvc_name)
        for clone_name, clone_info in self.clone_details.items():
            job_name = clone_info.get("job_name")
            if job_name:
                # Clones are PVCs themselves — pass clone_name as pvc_name
                self._save_fio_pod_logs(job_name, clone_name, pvc_name=clone_name)
        self.logger.info(
            f"[save_fio] Saved FIO logs for {len(self.pvc_details)} PVCs "
            f"and {len(self.clone_details)} clones"
        )
        # Bulk cleanup any leftover copier pods (belt-and-suspenders)
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pods -l app=fio-copier "
                f"-n {self.k8s_utils.namespace} "
                f"--force --grace-period=0 2>/dev/null || true",
                supress_logs=True,
            )
        except Exception:
            pass

    def _save_fio_mapping_summary(self):
        """Save a JSON summary file mapping every PVC/clone to its lvol,
        storage worker, FIO job, FIO K8s node, snapshot lineage, etc."""
        self._ensure_k8s_utils()

        entries = self.k8s_utils.log_fio_pvc_mapping(
            self.pvc_details, self.clone_details,
            snapshot_details=self.snapshot_details,
        )
        if not entries:
            return

        summary_path = os.path.join(self.docker_logs_path, "fio_mapping_summary.json")
        try:
            with open(summary_path, "w") as f:
                json.dump(entries, f, indent=2, default=str)
            self.logger.info(f"[save_fio] Wrote FIO mapping summary to {summary_path}")
        except Exception as exc:
            self.logger.warning(f"[save_fio] Could not write mapping summary: {exc}")

    def validate_fio_jobs(self):
        """Validate all active FIO workloads.

        Client mode: validate fio logs on clients via SSH.
        K8s mode: validate K8s Job status + pod logs.

        Always saves ALL FIO logs (pod stdout, perf files) for every
        PVC/clone first, then validates.
        """
        self._ensure_k8s_utils()

        # Save all FIO logs and mapping summary before validation
        self._save_all_fio_logs()
        self._save_fio_mapping_summary()

        if self.use_client_fio:
            for pvc_name, pvc_info in self.pvc_details.items():
                self.common_utils.validate_fio_test(
                    pvc_info["client"], log_file=pvc_info["log_file"]
                )
            for clone_name, clone_info in self.clone_details.items():
                self.common_utils.validate_fio_test(
                    clone_info["client"], log_file=clone_info["log_file"]
                )
        else:
            fio_timeout = self.FIO_RUNTIME + 300  # extra buffer over FIO runtime
            for pvc_name, pvc_info in self.pvc_details.items():
                self.k8s_utils.validate_fio_job(pvc_info["job_name"], timeout=fio_timeout)

            for clone_name, clone_info in self.clone_details.items():
                self.k8s_utils.validate_fio_job(clone_info["job_name"], timeout=fio_timeout)

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def _cleanup_all_k8s_resources(self):
        """Best-effort cleanup of all test K8s resources."""
        if not self.k8s_utils:
            return

        self.logger.info("[cleanup] Deleting all test K8s resources...")

        if self.use_client_fio:
            # ── Client cleanup: stop fio, unmount, disconnect NVMe ──
            for clone_name, clone_info in list(self.clone_details.items()):
                try:
                    client = clone_info["client"]
                    self._kill_fio_on_client(clone_name, client)
                    self.ssh_obj.unmount_path(client, clone_info["mount_path"])
                    self.ssh_obj.remove_dir(client, dir_path=clone_info["mount_path"])
                    self._disconnect_lvol_on_client(clone_info["lvol_name"], client)
                except Exception:
                    pass

            for pvc_name, pvc_info in list(self.pvc_details.items()):
                try:
                    client = pvc_info["client"]
                    self._kill_fio_on_client(pvc_name, client)
                    self.ssh_obj.unmount_path(client, pvc_info["mount_path"])
                    self.ssh_obj.remove_dir(client, dir_path=pvc_info["mount_path"])
                    self._disconnect_lvol_on_client(pvc_info["lvol_name"], client)
                except Exception:
                    pass
        else:
            # ── K8s Job cleanup ──
            for clone_name, clone_info in list(self.clone_details.items()):
                try:
                    self.k8s_utils.delete_job(clone_info["job_name"])
                    self.k8s_utils.delete_configmap(clone_info["configmap_name"])
                except Exception:
                    pass

            for pvc_name, pvc_info in list(self.pvc_details.items()):
                try:
                    self.k8s_utils.delete_job(pvc_info["job_name"])
                    self.k8s_utils.delete_configmap(pvc_info["configmap_name"])
                except Exception:
                    pass

        # Delete clone PVCs
        for clone_name in list(self.clone_details.keys()):
            try:
                self.k8s_utils.delete_pvc(clone_name)
            except Exception:
                pass

        # Delete VolumeSnapshots
        for snap_name in list(self.snapshot_details.keys()):
            try:
                self.k8s_utils.delete_volume_snapshot(snap_name)
            except Exception:
                pass

        # Delete PVCs
        for pvc_name in list(self.pvc_details.keys()):
            try:
                self.k8s_utils.delete_pvc(pvc_name)
            except Exception:
                pass

        # Delete StorageClass and VolumeSnapshotClass
        try:
            self.k8s_utils.delete_storage_class(self.STORAGE_CLASS_NAME)
        except Exception:
            pass
        try:
            self.k8s_utils.delete_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        except Exception:
            pass

        self.logger.info("[cleanup] Done.")

    # ── Main run loop ────────────────────────────────────────────────────────

    def run(self):
        self._ensure_k8s_utils()
        self._initialize_outage_log()
        self.logger.info("=== Starting K8sNativeFailoverTest ===")

        # Read cluster config
        cluster_details = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {self.max_fault_tolerance}")
        if self.npcs == 1:
            self.npcs = self.max_fault_tolerance
        if self.npcs > self.max_fault_tolerance:
            self.logger.warning(
                f"npcs={self.npcs} exceeds max_fault_tolerance="
                f"{self.max_fault_tolerance} — cluster may not survive "
                f"all simultaneous outages!"
            )
        if self.max_fault_tolerance >= 2:
            self.logger.info(
                f"FTT={self.max_fault_tolerance} — outage candidates include "
                f"ALL nodes (primary+secondary pairs allowed)"
            )
        self.logger.info(f"Running with npcs={self.npcs} simultaneous outages")

        # Clean slate: delete stale resources from previous runs, then
        # recreate so parameters are always up-to-date.
        # Order matters: clones → snapshots → lvols → pool
        # (SPDK refuses to delete a snapshot that still has clones,
        #  and pool can't be deleted while lvols reference it).
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_storage_pool(self.pool_name)
        except Exception:
            pass
        pool_test = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.pool_name = self.pool_name if pool_test == self.pool_name else pool_test

        cluster_id = self.cluster_id or ""
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        if self.tls_enabled:
            self.logger.info("TLS enabled — ensuring encryption pool exists")
            self.sbcli_utils.ensure_pool_exists(
                self.CRYPTO_POOL_NAME,
                cluster_id=self.cluster_id,
                encryption=True,
            )
            self.logger.info("TLS enabled — creating crypto StorageClass with encryption=True")
            self.k8s_utils.create_storage_class(
                name=self.CRYPTO_STORAGE_CLASS_NAME,
                cluster_id=cluster_id,
                pool_name=self.CRYPTO_POOL_NAME,
                ndcs=self.ndcs,
                npcs=self.npcs,
                encryption=True,
            )
        self.k8s_utils.delete_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        self.k8s_utils.create_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        sleep_n_sec(5)

        # Populate storage node maps
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = result["secondary_node_id"]
        self.logger.info(f"Storage nodes: {len(self.sn_nodes)}, secondary map: {self.sn_primary_secondary_map}")

        # Create initial PVCs: first 1 per storage node (pinned), then extras unpinned
        initial_pvcs = max(self.total_pvcs, len(self.sn_nodes))
        self.logger.info(f"Creating {initial_pvcs} initial PVCs ({len(self.sn_nodes)} pinned + {initial_pvcs - len(self.sn_nodes)} extra)")
        self.create_pvcs_with_fio(len(self.sn_nodes), node_ids=list(self.sn_nodes))
        if initial_pvcs > len(self.sn_nodes):
            self.create_pvcs_with_fio(initial_pvcs - len(self.sn_nodes))
        sleep_n_sec(30)
        self._ensure_per_node_coverage()

        iteration = 1
        test_failed = False
        try:
            while True:
                self.logger.info(f"=== Iteration {iteration} ===")

                # Start background IO stats validation
                validation_thread = threading.Thread(
                    target=self.validate_iostats_continuously, daemon=True
                )
                validation_thread.start()

                if iteration > 1:
                    self.restart_fio(iteration)

                # ── Outage phase ──
                outage_events = self.perform_n_plus_k_outages()

                # ── Operations during outage ──
                # Scale deletes: 1 in iter 1, 2 in iter 2, 3 in iter 3, ...
                delete_count = min(iteration, len(self.pvc_details) - len(self.sn_nodes))
                delete_count = max(delete_count, 1)
                self.logger.info(f"[scale] Deleting {delete_count} PVCs (iteration {iteration})")
                self.delete_random_pvcs(delete_count)
                self.create_snapshots_and_clones()

                # Scale up: add 2 more PVCs each iteration (net growth = 2)
                create_count = delete_count + 2
                self.logger.info(f"[scale] Creating {create_count} PVCs (total will be ~{len(self.pvc_details) + create_count})")
                self.create_pvcs_with_fio(create_count)
                sleep_n_sec(280)

                # ── Recovery phase: bring all nodes online ──
                for node, outage_type, node_outage_dur in outage_events:
                    self.current_outage_node = node
                    if outage_type == "container_stop" and self.npcs > 1:
                        self.restart_nodes_after_failover(outage_type, restart=True)
                    else:
                        self.restart_nodes_after_failover(outage_type)
                    self.logger.info("Waiting for fallback recovery.")
                    sleep_n_sec(100)

                # ── Health check after all nodes are online ──
                for node, outage_type, node_outage_dur in outage_events:
                    try:
                        self.sbcli_utils.wait_for_health_status(node, True, timeout=300)
                    except Exception as exc:
                        self.logger.warning(f"Health check did not pass for {node}: {exc}")

                self.collect_outage_diagnostics("post_recovery")

                # ── Process deferred operations ──
                if self.use_client_fio:
                    try:
                        self.retry_failed_nvme_connects()
                    except AssertionError as exc:
                        self.logger.error(f"[iteration {iteration}] {exc}")
                        test_failed = True
                    self.retry_failed_secondary_connects()
                self.validate_pending_deletions()

                # ── Validation phase ──
                sleep_n_sec(300)
                self.check_core_dump()

                time_duration = self.common_utils.calculate_time_duration(
                    start_timestamp=self.outage_start_time,
                    end_timestamp=self.outage_end_time,
                )
                try:
                    self.common_utils.validate_io_stats(
                        cluster_id=self.cluster_id,
                        start_timestamp=self.outage_start_time,
                        end_timestamp=self.outage_end_time,
                        time_duration=time_duration,
                        warn_only=True,
                    )
                except AssertionError as exc:
                    self.logger.error(
                        f"[iteration {iteration}] IO validation failed — "
                        f"zero IO detected: {exc}"
                    )
                    test_failed = True
                self.validate_migration_for_node(self.outage_start_time, 2000, None, 60)
                self.wait_for_fio_complete()
                self.validate_fio_jobs()

                self.logger.info(f"=== Iteration {iteration} complete ===")
                self.collect_outage_diagnostics(f"end_iteration_{iteration}")

                if test_failed:
                    self.logger.error(
                        f"[iteration {iteration}] Test marked as FAILED — "
                        f"stopping stress loop"
                    )
                    break

                iteration += 1

        except Exception:
            test_failed = True
            raise
        finally:
            if test_failed:
                self.logger.info("[cleanup] Test failed — skipping resource cleanup to preserve state for debugging")
                raise AssertionError("Stress test failed — see errors above")
            else:
                self._cleanup_all_k8s_resources()


class K8sNativeBasicFailoverTest(K8sNativeFailoverTest):
    """Simpler K8s-native functional failover test.

    Unlike K8sNativeFailoverTest (stress test with continuous create/delete),
    this test:
      1. Creates PVCs with FIO (1 per storage node) — once
      2. Creates snapshots + clones with FIO — once
      3. Loops: outage → recovery → validation (no creates/deletes)

    Focuses on verifying failover functionality, not stress.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "k8s_native_basic_failover"
        self.num_clones = 3

    def create_snapshots_and_clones_with_cleanup(self, count: int = None):
        """Create snapshots + clones, cleaning old FIO files from clones.

        For K8s Job mode, uses an init container to rm old fio files.
        For client FIO mode, the parent already calls delete_files().
        """
        self._ensure_k8s_utils()
        num = count or self.num_clones
        self.int_pvc_size += 1
        available_pvcs = list(self.pvc_details.keys())
        if not available_pvcs:
            self.logger.warning("[snap_clone] No PVCs available for snapshots")
            return

        for idx in range(num):
            random.shuffle(available_pvcs)
            pvc_name = available_pvcs[idx % len(available_pvcs)]
            snap_name = f"snap-{_rand_seq(12)}"
            clone_name = f"clone-{_rand_seq(12)}"

            # Create snapshot
            try:
                self.k8s_utils.create_volume_snapshot(
                    snap_name, pvc_name, self.SNAPSHOT_CLASS_NAME
                )
                self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Snapshot creation failed for {snap_name}: {exc}")
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                    self.logger.info(f"[snap_clone] Cleaned up orphaned snapshot {snap_name}")
                except Exception:
                    self.logger.warning(f"[snap_clone] Could not clean up snapshot {snap_name}")
                continue

            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}
            self.snapshot_names.append(snap_name)
            self.pvc_details[pvc_name]["snapshots"].append(snap_name)

            # Snapshot lvol IDs before clone PVC (for client mode mapping)
            old_lvol_ids = self._snapshot_lvol_ids() if self.use_client_fio else set()

            # Create clone PVC — use same StorageClass as source PVC
            clone_sc = self.pvc_details.get(pvc_name, {}).get("storage_class", self.STORAGE_CLASS_NAME)
            sleep_n_sec(10)
            try:
                self.k8s_utils.create_clone_pvc(
                    clone_name, self.pvc_size, clone_sc, snap_name
                )
                self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Clone PVC creation failed for {clone_name}: {exc}")
                try:
                    self.k8s_utils.delete_pvc(clone_name)
                    self.logger.info(f"[snap_clone] Cleaned up orphaned clone PVC {clone_name}")
                except Exception:
                    self.logger.warning(f"[snap_clone] Could not clean up clone PVC {clone_name}")
                continue

            if self.use_client_fio:
                # Client FIO path for clone (parent logic handles file cleanup)
                sleep_n_sec(5)
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[snap_clone] Could not map clone {clone_name} to lvol — skipping"
                    )
                    continue
                clone_lvol_name, clone_lvol_id = lvol_info

                client = self.fio_node[idx % len(self.fio_node)]

                try:
                    device, failed_cmds = self._connect_lvol_on_client(clone_lvol_name, client)
                    if failed_cmds:
                        self.record_failed_secondary_connects(clone_name, client, failed_cmds)
                except Exception as exc:
                    self.logger.warning(
                        f"[snap_clone] NVMe connect failed for clone {clone_name}: {exc}"
                    )
                    continue

                self.ssh_obj.clone_mount_gen_uuid(client, device)
                mount_point = f"{self.mount_path}/{clone_name}"
                self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_point)
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{clone_name}.log"
                # Delete old FIO files inherited from source PVC
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
                self._start_client_fio(clone_name, client, mount_point, log_file)

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": None,
                    "configmap_name": None,
                    "lvol_name": clone_lvol_name,
                    "lvol_id": clone_lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                    "storage_class": clone_sc,
                }
                self.clone_mount_details[clone_lvol_name] = {
                    "ID": clone_lvol_id,
                    "snapshot": snap_name,
                    "Mount": mount_point,
                    "Device": device,
                    "Log": log_file,
                    "Client": client,
                    "clone_pvc": clone_name,
                }

                self.logger.info(
                    f"[snap_clone] Clone {clone_name} → lvol {clone_lvol_name} "
                    f"connected on {client}, FIO started (files cleaned)"
                )
            else:
                # K8s Job FIO path — with init container cleanup
                clone_job = f"fio-{clone_name}"
                clone_cm = f"fiocfg-{clone_name}"
                clone_node_id = self._get_pvc_node_id(clone_name)
                avoid = self._get_k8s_node_for_storage_node(clone_node_id) if clone_node_id else None

                fio_config, warmup_config = self._build_fio_config(clone_name)
                try:
                    self.k8s_utils.create_fio_job(
                        clone_job, clone_name, clone_cm, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(f"[snap_clone] Clone FIO Job failed for {clone_name}: {exc}")

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": clone_job,
                    "configmap_name": clone_cm,
                    "storage_class": clone_sc,
                }

            # Resize source PVC and clone PVC
            try:
                self.k8s_utils.resize_pvc(pvc_name, f"{self.int_pvc_size}Gi")
                sleep_n_sec(5)
                self.k8s_utils.resize_pvc(clone_name, f"{self.int_pvc_size}Gi")
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Resize failed: {exc}")

            self.logger.info(
                f"[snap_clone] Created snapshot {snap_name}, clone {clone_name}, "
                f"resized to {self.int_pvc_size}Gi"
            )
            sleep_n_sec(10)

        self.k8s_utils.log_fio_pvc_mapping(self.pvc_details, self.clone_details, snapshot_details=self.snapshot_details)

    def run(self):
        """Simplified run loop: create once, then loop outages only."""
        self._ensure_k8s_utils()
        self._initialize_outage_log()
        self.logger.info("=== Starting K8sNativeBasicFailoverTest ===")

        # Read cluster config
        cluster_details = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {self.max_fault_tolerance}")
        if self.npcs == 1:
            self.npcs = self.max_fault_tolerance
        if self.npcs > self.max_fault_tolerance:
            self.logger.warning(
                f"npcs={self.npcs} exceeds max_fault_tolerance="
                f"{self.max_fault_tolerance} — cluster may not survive "
                f"all simultaneous outages!"
            )
        if self.max_fault_tolerance >= 2:
            self.logger.info(
                f"FTT={self.max_fault_tolerance} — outage candidates include "
                f"ALL nodes (primary+secondary pairs allowed)"
            )
        self.logger.info(f"Running with npcs={self.npcs} simultaneous outages")

        # Clean slate: delete stale resources from previous runs, then
        # recreate so parameters are always up-to-date.
        # Order matters: clones → snapshots → lvols → pool
        # (SPDK refuses to delete a snapshot that still has clones,
        #  and pool can't be deleted while lvols reference it).
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_storage_pool(self.pool_name)
        except Exception:
            pass
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        cluster_id = self.cluster_id or ""
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        self.k8s_utils.delete_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        self.k8s_utils.create_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        sleep_n_sec(5)

        # Populate storage node maps
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = result["secondary_node_id"]
        self.logger.info(
            f"Storage nodes: {len(self.sn_nodes)}, "
            f"secondary map: {self.sn_primary_secondary_map}"
        )

        # ── One-time setup: Create PVCs (1 per node, pinned) ──
        self.total_pvcs = len(self.sn_nodes)
        self.logger.info(f"Creating {self.total_pvcs} PVCs (1 per node, pinned)")
        self.create_pvcs_with_fio(self.total_pvcs, node_ids=list(self.sn_nodes))
        sleep_n_sec(30)
        self._ensure_per_node_coverage()

        # ── One-time setup: Create snapshots + clones ──
        self.logger.info(f"Creating {self.num_clones} snapshots + clones (with FIO file cleanup)")
        self.create_snapshots_and_clones_with_cleanup(self.num_clones)
        sleep_n_sec(30)

        # ── Outage loop ──
        iteration = 1
        test_failed = False
        try:
            while True:
                self.logger.info(f"=== Iteration {iteration} ===")

                # Start background IO stats validation
                validation_thread = threading.Thread(
                    target=self.validate_iostats_continuously, daemon=True
                )
                validation_thread.start()

                if iteration > 1:
                    self.restart_fio(iteration)

                # Outage phase
                outage_events = self.perform_n_plus_k_outages()
                sleep_n_sec(280)

                # Recovery phase: bring all nodes online
                for node, outage_type, node_outage_dur in outage_events:
                    self.current_outage_node = node
                    if outage_type == "container_stop" and self.npcs > 1:
                        self.restart_nodes_after_failover(outage_type, restart=True)
                    else:
                        self.restart_nodes_after_failover(outage_type)
                    self.logger.info("Waiting for fallback recovery.")
                    sleep_n_sec(100)

                # Health check after all nodes are online
                for node, outage_type, node_outage_dur in outage_events:
                    try:
                        self.sbcli_utils.wait_for_health_status(node, True, timeout=300)
                    except Exception as exc:
                        self.logger.warning(f"Health check did not pass for {node}: {exc}")

                self.collect_outage_diagnostics("post_recovery")

                # Process deferred operations
                if self.use_client_fio:
                    try:
                        self.retry_failed_nvme_connects()
                    except AssertionError as exc:
                        self.logger.error(f"[iteration {iteration}] {exc}")
                        test_failed = True
                    self.retry_failed_secondary_connects()
                self.validate_pending_deletions()

                # Validation phase
                sleep_n_sec(300)
                self.check_core_dump()

                time_duration = self.common_utils.calculate_time_duration(
                    start_timestamp=self.outage_start_time,
                    end_timestamp=self.outage_end_time,
                )
                try:
                    self.common_utils.validate_io_stats(
                        cluster_id=self.cluster_id,
                        start_timestamp=self.outage_start_time,
                        end_timestamp=self.outage_end_time,
                        time_duration=time_duration,
                        warn_only=True,
                    )
                except AssertionError as exc:
                    self.logger.error(
                        f"[iteration {iteration}] IO validation failed — "
                        f"zero IO detected: {exc}"
                    )
                    test_failed = True
                self.validate_migration_for_node(self.outage_start_time, 2000, None, 60)
                self.wait_for_fio_complete()
                self.validate_fio_jobs()

                self.logger.info(f"=== Iteration {iteration} complete ===")
                self.collect_outage_diagnostics(f"end_iteration_{iteration}")

                if test_failed:
                    self.logger.error(
                        f"[iteration {iteration}] Test marked as FAILED — "
                        f"stopping stress loop"
                    )
                    break

                iteration += 1

        except Exception:
            test_failed = True
            raise
        finally:
            if test_failed:
                self.logger.info("[cleanup] Test failed — skipping resource cleanup to preserve state for debugging")
                raise AssertionError("Stress test failed — see errors above")
            else:
                self._cleanup_all_k8s_resources()


class K8sNativeResilientFailoverTest(K8sNativeFailoverTest):
    """Resilient K8s-native failover stress test.

    Works around the PVC provisioning limitation where
    ``ndcs + npcs > len(online_nodes)`` blocks ALL volume creation during
    a degraded cluster (e.g. 2+2 geometry on 4 nodes, 2 down = 0 PVCs
    can be created).

    Strategy:
      - Create *permanent* PVCs (2 per node), snapshots (1 per node),
        and clones (1 per node) at startup.  These are NEVER deleted.
      - FIO always runs on permanent resources, so IO never drops to zero.
      - Dynamic PVCs / snapshots / clones are created and deleted only
        after recovery when the cluster is fully online.
      - Total lvol count is capped at MAX_TOTAL_LVOLS (default 36).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "k8s_native_resilient_failover"

        # Permanent resources per node
        self.PERMANENT_PVCS_PER_NODE = 2

        # Cap on total lvols (PVCs + clones) to avoid resource exhaustion
        self.MAX_TOTAL_LVOLS = int(os.environ.get("MAX_TOTAL_LVOLS", "36"))

        # Sets tracking permanent resource names (never deleted)
        self.permanent_pvcs: set[str] = set()
        self.permanent_snapshots: set[str] = set()
        self.permanent_clones: set[str] = set()

        # PVCs created during outage (not yet bound, no FIO running)
        self.deferred_pvcs: list[str] = []

    # ── Helpers ───────────────────────────────────────────────────────────

    def _is_cluster_fully_online(self) -> bool:
        """Return True only if every storage node is online."""
        for node_id in self.sn_nodes:
            try:
                details = self.sbcli_utils.get_storage_node_details(node_id)
                if not details or details[0].get("status") != "online":
                    return False
            except Exception:
                return False
        return True

    def _count_total_resources(self) -> int:
        """Count total PVCs + clones (each backed by an lvol)."""
        return len(self.pvc_details) + len(self.clone_details)

    def _create_pvcs_deferred(self, count: int):
        """Create PVCs without waiting for Bound or starting FIO.

        Used during outage — the PVCs will stay Pending until the cluster
        recovers and the geometry check passes.  After recovery, call
        ``_bind_deferred_pvcs_and_start_fio()`` to wait for bind + start FIO.
        """
        self._ensure_k8s_utils()
        for i in range(count):
            pvc_name = f"pvc-{_rand_seq(12)}"
            self.logger.info(
                f"[deferred_create] Creating PVC {pvc_name} "
                f"({i+1}/{count}) — will bind after recovery"
            )
            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.pvc_size, self.STORAGE_CLASS_NAME,
                )
            except Exception as exc:
                self.logger.warning(
                    f"[deferred_create] PVC creation failed for "
                    f"{pvc_name}: {exc}"
                )
                continue
            self.deferred_pvcs.append(pvc_name)

    def _bind_deferred_pvcs_and_start_fio(self):
        """Wait for deferred PVCs to bind, then start FIO on each.

        Called after recovery when the cluster is fully online.
        PVCs that fail to bind within timeout are cleaned up.
        """
        if not self.deferred_pvcs:
            return

        self._ensure_k8s_utils()
        self.logger.info(
            f"[deferred_bind] Waiting for {len(self.deferred_pvcs)} "
            f"deferred PVCs to bind"
        )

        for pvc_name in list(self.deferred_pvcs):
            try:
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(
                    f"[deferred_bind] PVC {pvc_name} did not bind: "
                    f"{exc} — cleaning up"
                )
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                self.deferred_pvcs.remove(pvc_name)
                continue

            sleep_n_sec(5)

            if self.use_client_fio:
                # Client FIO path
                old_lvol_ids = self._snapshot_lvol_ids()
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[deferred_bind] Could not map PVC "
                        f"{pvc_name} to lvol — skipping FIO"
                    )
                    # Still track as a regular PVC
                    self.pvc_details[pvc_name] = {
                        "job_name": None,
                        "configmap_name": None,
                        "snapshots": [],
                        "node_id": None,
                    }
                    self.deferred_pvcs.remove(pvc_name)
                    continue

                lvol_name, lvol_id = lvol_info
                node_id = None
                try:
                    details = self.sbcli_utils.get_lvol_details(lvol_id)
                    if details:
                        node_id = details[0].get("node_id")
                except Exception:
                    pass

                client = self.fio_node[
                    len(self.pvc_details) % len(self.fio_node)
                ]
                fs_type = random.choice(["ext4", "xfs"])

                try:
                    device, failed_cmds = self._connect_lvol_on_client(
                        lvol_name, client
                    )
                    if failed_cmds:
                        self.record_failed_secondary_connects(
                            pvc_name, client, failed_cmds
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"[deferred_bind] NVMe connect failed for "
                        f"{pvc_name}: {exc}"
                    )
                    self.record_failed_nvme_connect(pvc_name, {
                        "lvol_name": lvol_name,
                        "lvol_id": lvol_id,
                        "client": client,
                    })
                    self.pvc_details[pvc_name] = {
                        "job_name": None,
                        "configmap_name": None,
                        "snapshots": [],
                        "node_id": node_id,
                        "lvol_name": lvol_name,
                        "lvol_id": lvol_id,
                        "device": None,
                        "mount_path": None,
                        "client": client,
                        "log_file": None,
                    }
                    self.deferred_pvcs.remove(pvc_name)
                    continue

                mount_point = f"{self.mount_path}/{pvc_name}"
                self.ssh_obj.make_filesystem(
                    node=client, device=device, fs_type=fs_type
                )
                self.ssh_obj.mount_path(
                    node=client, device=device,
                    mount_path=mount_point,
                )
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{pvc_name}.log"
                bs = f"{2 ** random.randint(2, 7)}K"
                self._run_fio_warmup_ssh(
                    pvc_name, client, mount_point, bs
                )
                self._start_client_fio(
                    pvc_name, client, mount_point, log_file,
                    bs=bs,
                )

                self.pvc_details[pvc_name] = {
                    "job_name": None,
                    "configmap_name": None,
                    "snapshots": [],
                    "node_id": node_id,
                    "lvol_name": lvol_name,
                    "lvol_id": lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                }
                self.lvol_mount_details[lvol_name] = {
                    "ID": lvol_id,
                    "Command": None,
                    "Mount": mount_point,
                    "Device": device,
                    "FS": fs_type,
                    "Log": log_file,
                    "Client": client,
                    "snapshots": [],
                }
            else:
                # K8s Job FIO path
                job_name = f"fio-{pvc_name}"
                cm_name = f"fiocfg-{pvc_name}"
                node_id = self._get_pvc_node_id(pvc_name)
                avoid = (
                    self._get_k8s_node_for_storage_node(node_id)
                    if node_id else None
                )

                fio_config, warmup_config = self._build_fio_config(
                    pvc_name
                )
                try:
                    self.k8s_utils.create_fio_job(
                        job_name, pvc_name, cm_name, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[deferred_bind] FIO Job failed for "
                        f"{pvc_name}: {exc}"
                    )

                self.pvc_details[pvc_name] = {
                    "job_name": job_name,
                    "configmap_name": cm_name,
                    "snapshots": [],
                    "node_id": node_id,
                }

            if node_id:
                self.node_vs_pvc.setdefault(
                    node_id, []
                ).append(pvc_name)
            self.deferred_pvcs.remove(pvc_name)
            self.logger.info(
                f"[deferred_bind] PVC {pvc_name} bound, "
                f"FIO started (node={node_id})"
            )
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(
            self.pvc_details, self.clone_details,
            snapshot_details=self.snapshot_details,
        )

    def _create_permanent_snapshots_and_clones(self):
        """Create 1 snapshot + 1 clone per node from permanent PVCs.

        Picks one permanent PVC per node and creates a snapshot + clone
        pair.  The results are marked as permanent (never deleted).
        """
        self._ensure_k8s_utils()
        # Build per-node PVC map from permanent PVCs
        node_pvc_map: dict[str, str] = {}
        for pvc_name in self.permanent_pvcs:
            nid = self.pvc_details.get(pvc_name, {}).get("node_id")
            if nid and nid not in node_pvc_map:
                node_pvc_map[nid] = pvc_name

        self.logger.info(
            f"[permanent] Creating snapshots + clones for "
            f"{len(node_pvc_map)} nodes"
        )

        for node_id, pvc_name in node_pvc_map.items():
            snap_name = f"snap-{_rand_seq(12)}"
            clone_name = f"clone-{_rand_seq(12)}"

            # Create snapshot
            try:
                self.k8s_utils.create_volume_snapshot(
                    snap_name, pvc_name, self.SNAPSHOT_CLASS_NAME
                )
                self.k8s_utils.wait_volume_snapshot_ready(
                    snap_name, timeout=300
                )
            except Exception as exc:
                self.logger.warning(
                    f"[permanent] Snapshot creation failed for "
                    f"{snap_name}: {exc}"
                )
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception:
                    pass
                continue

            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}
            self.snapshot_names.append(snap_name)
            self.pvc_details[pvc_name]["snapshots"].append(snap_name)
            self.permanent_snapshots.add(snap_name)

            # Snapshot lvol IDs before clone PVC (for client mode mapping)
            old_lvol_ids = (
                self._snapshot_lvol_ids() if self.use_client_fio else set()
            )

            # Create clone PVC — use same StorageClass as source PVC
            clone_sc = self.pvc_details.get(pvc_name, {}).get(
                "storage_class", self.STORAGE_CLASS_NAME
            )
            sleep_n_sec(10)
            try:
                self.k8s_utils.create_clone_pvc(
                    clone_name, self.pvc_size, clone_sc, snap_name,
                )
                self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)
            except Exception as exc:
                self.logger.warning(
                    f"[permanent] Clone PVC creation failed for "
                    f"{clone_name}: {exc}"
                )
                try:
                    self.k8s_utils.delete_pvc(clone_name)
                except Exception:
                    pass
                continue

            self.permanent_clones.add(clone_name)

            if self.use_client_fio:
                # Client FIO path for clone
                sleep_n_sec(5)
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[permanent] Could not map clone {clone_name} "
                        f"to lvol — skipping FIO"
                    )
                    continue
                clone_lvol_name, clone_lvol_id = lvol_info
                client = self.fio_node[
                    list(node_pvc_map.keys()).index(node_id)
                    % len(self.fio_node)
                ]

                try:
                    device, failed_cmds = self._connect_lvol_on_client(
                        clone_lvol_name, client
                    )
                    if failed_cmds:
                        self.record_failed_secondary_connects(
                            clone_name, client, failed_cmds
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"[permanent] NVMe connect failed for clone "
                        f"{clone_name}: {exc}"
                    )
                    continue

                self.ssh_obj.clone_mount_gen_uuid(client, device)
                mount_point = f"{self.mount_path}/{clone_name}"
                self.ssh_obj.mount_path(
                    node=client, device=device, mount_path=mount_point
                )
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{clone_name}.log"
                self.ssh_obj.delete_files(
                    client, [f"{mount_point}/*fio*"]
                )
                self._start_client_fio(
                    clone_name, client, mount_point, log_file
                )

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": None,
                    "configmap_name": None,
                    "lvol_name": clone_lvol_name,
                    "lvol_id": clone_lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                    "storage_class": clone_sc,
                }
                self.clone_mount_details[clone_lvol_name] = {
                    "ID": clone_lvol_id,
                    "snapshot": snap_name,
                    "Mount": mount_point,
                    "Device": device,
                    "Log": log_file,
                    "Client": client,
                    "clone_pvc": clone_name,
                }
            else:
                # K8s Job FIO path with init container cleanup
                clone_job = f"fio-{clone_name}"
                clone_cm = f"fiocfg-{clone_name}"
                clone_node_id = self._get_pvc_node_id(clone_name)
                avoid = (
                    self._get_k8s_node_for_storage_node(clone_node_id)
                    if clone_node_id
                    else None
                )

                fio_config, warmup_config = self._build_fio_config(
                    clone_name
                )
                try:
                    self.k8s_utils.create_fio_job(
                        clone_job, clone_name, clone_cm, fio_config,
                        image=self.FIO_IMAGE,
                        cleanup_before_fio=True,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[permanent] Clone FIO Job failed for "
                        f"{clone_name}: {exc}"
                    )

                self.clone_details[clone_name] = {
                    "snap_name": snap_name,
                    "job_name": clone_job,
                    "configmap_name": clone_cm,
                    "storage_class": clone_sc,
                }

            self.logger.info(
                f"[permanent] Created snapshot {snap_name}, "
                f"clone {clone_name} for node {node_id}"
            )
            sleep_n_sec(10)

        self.k8s_utils.log_fio_pvc_mapping(
            self.pvc_details, self.clone_details,
            snapshot_details=self.snapshot_details,
        )

    def _delete_dynamic_pvcs(self, count: int):
        """Delete *count* dynamic (non-permanent) PVCs and their
        snapshots/clones, then recreate replacements.
        """
        self._ensure_k8s_utils()
        dynamic = [
            p for p in self.pvc_details
            if p not in self.permanent_pvcs
        ]
        if not dynamic:
            self.logger.info("[dynamic] No dynamic PVCs to delete")
            return

        count = min(count, len(dynamic))
        targets = random.sample(dynamic, count)
        deleted_node_ids = []

        for pvc_name in targets:
            self.logger.info(
                f"[dynamic] Deleting dynamic PVC tree: {pvc_name}"
            )
            pvc_info = self.pvc_details[pvc_name]

            # Delete clones for each snapshot of this PVC
            for snap_name in list(pvc_info["snapshots"]):
                if snap_name in self.permanent_snapshots:
                    continue
                clones_to_delete = [
                    cn for cn, cd in self.clone_details.items()
                    if cd["snap_name"] == snap_name
                    and cn not in self.permanent_clones
                ]
                for clone_name in clones_to_delete:
                    clone_info = self.clone_details[clone_name]

                    if self.use_client_fio:
                        client = clone_info["client"]
                        self._kill_fio_on_client(clone_name, client)
                        sleep_n_sec(5)
                        try:
                            self.ssh_obj.unmount_path(
                                client, clone_info["mount_path"]
                            )
                            self.ssh_obj.remove_dir(
                                client,
                                dir_path=clone_info["mount_path"],
                            )
                        except Exception as exc:
                            self.logger.warning(
                                f"[dynamic] Clone unmount failed: {exc}"
                            )
                        self._disconnect_lvol_on_client(
                            clone_info["lvol_name"], client
                        )
                        self.clone_mount_details.pop(
                            clone_info.get("lvol_name"), None
                        )
                    else:
                        try:
                            self.k8s_utils.delete_job(
                                clone_info["job_name"]
                            )
                            self.k8s_utils.delete_configmap(
                                clone_info["configmap_name"]
                            )
                        except Exception as exc:
                            self.logger.warning(
                                f"[dynamic] Clone Job cleanup failed: "
                                f"{exc}"
                            )

                    try:
                        self.k8s_utils.delete_pvc(clone_name)
                    except Exception as exc:
                        self.logger.warning(
                            f"[dynamic] Clone PVC delete failed: {exc}"
                        )
                        self.record_pending_clone_delete(
                            clone_name, clone_info
                        )
                    del self.clone_details[clone_name]

                # Delete the snapshot
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception as exc:
                    self.logger.warning(
                        f"[dynamic] Snapshot delete failed: {exc}"
                    )
                    self.record_pending_snapshot_delete(
                        snap_name,
                        self.snapshot_details.get(snap_name, {}),
                    )
                self.snapshot_details.pop(snap_name, None)
                if snap_name in self.snapshot_names:
                    self.snapshot_names.remove(snap_name)

            # Stop FIO for the PVC itself
            if self.use_client_fio:
                client = pvc_info["client"]
                self._kill_fio_on_client(pvc_name, client)
                sleep_n_sec(5)
                try:
                    self.ssh_obj.unmount_path(
                        client, pvc_info["mount_path"]
                    )
                    self.ssh_obj.remove_dir(
                        client, dir_path=pvc_info["mount_path"]
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[dynamic] PVC unmount failed: {exc}"
                    )
                self._disconnect_lvol_on_client(
                    pvc_info["lvol_name"], client
                )
                self.lvol_mount_details.pop(
                    pvc_info.get("lvol_name"), None
                )
            else:
                try:
                    self.k8s_utils.delete_job(pvc_info["job_name"])
                    self.k8s_utils.delete_configmap(
                        pvc_info["configmap_name"]
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[dynamic] PVC Job cleanup failed: {exc}"
                    )

            try:
                self.k8s_utils.delete_pvc(pvc_name)
            except Exception as exc:
                self.logger.warning(
                    f"[dynamic] PVC delete failed: {exc}"
                )
                self.record_pending_pvc_delete(pvc_name, pvc_info)

            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            del self.pvc_details[pvc_name]
            if node_id:
                deleted_node_ids.append(node_id)

        sleep_n_sec(30)

        # Recreate dynamic PVCs (unpinned — cluster is fully online)
        if deleted_node_ids:
            self.logger.info(
                f"[dynamic] Recreating {len(deleted_node_ids)} "
                f"dynamic PVCs"
            )
            self.create_pvcs_with_fio(len(deleted_node_ids))

    def _enforce_lvol_cap(self):
        """Delete dynamic resources if total exceeds MAX_TOTAL_LVOLS.

        Deletion order: dynamic clones + snapshots, then dynamic PVCs.
        """
        total = self._count_total_resources()
        if total <= self.MAX_TOTAL_LVOLS:
            self.logger.info(
                f"[cap] Total resources: {total} <= "
                f"{self.MAX_TOTAL_LVOLS} (within cap)"
            )
            return

        excess = total - self.MAX_TOTAL_LVOLS
        self.logger.info(
            f"[cap] Total resources: {total} exceeds cap "
            f"{self.MAX_TOTAL_LVOLS} by {excess} — pruning"
        )

        # Phase 1: delete dynamic clones (+ their orphan snapshots)
        dynamic_clones = [
            cn for cn in self.clone_details
            if cn not in self.permanent_clones
        ]
        for clone_name in dynamic_clones:
            if excess <= 0:
                break
            clone_info = self.clone_details[clone_name]
            snap_name = clone_info["snap_name"]

            if not self.use_client_fio:
                try:
                    self.k8s_utils.delete_job(clone_info["job_name"])
                    self.k8s_utils.delete_configmap(
                        clone_info["configmap_name"]
                    )
                except Exception:
                    pass
            else:
                client = clone_info.get("client")
                if client:
                    self._kill_fio_on_client(clone_name, client)
                    sleep_n_sec(2)
                    try:
                        self.ssh_obj.unmount_path(
                            client, clone_info["mount_path"]
                        )
                    except Exception:
                        pass
                    self._disconnect_lvol_on_client(
                        clone_info.get("lvol_name", ""), client
                    )
                    self.clone_mount_details.pop(
                        clone_info.get("lvol_name"), None
                    )

            try:
                self.k8s_utils.delete_pvc(clone_name)
            except Exception:
                pass
            del self.clone_details[clone_name]

            # Delete orphan dynamic snapshot
            if (snap_name not in self.permanent_snapshots
                    and snap_name in self.snapshot_details):
                other_clones = [
                    cn for cn, cd in self.clone_details.items()
                    if cd["snap_name"] == snap_name
                ]
                if not other_clones:
                    try:
                        self.k8s_utils.delete_volume_snapshot(snap_name)
                    except Exception:
                        pass
                    parent_pvc = self.snapshot_details.get(
                        snap_name, {}
                    ).get("pvc_name")
                    if (parent_pvc and parent_pvc in self.pvc_details
                            and snap_name
                            in self.pvc_details[parent_pvc]["snapshots"]):
                        self.pvc_details[parent_pvc][
                            "snapshots"
                        ].remove(snap_name)
                    self.snapshot_details.pop(snap_name, None)
                    if snap_name in self.snapshot_names:
                        self.snapshot_names.remove(snap_name)

            excess -= 1

        # Phase 2: delete dynamic PVCs if still over cap
        if excess > 0:
            dynamic_pvcs = [
                p for p in self.pvc_details
                if p not in self.permanent_pvcs
            ]
            for pvc_name in dynamic_pvcs:
                if excess <= 0:
                    break
                pvc_info = self.pvc_details[pvc_name]

                for snap_name in list(pvc_info["snapshots"]):
                    if snap_name in self.permanent_snapshots:
                        continue
                    clones = [
                        cn for cn, cd in self.clone_details.items()
                        if cd["snap_name"] == snap_name
                        and cn not in self.permanent_clones
                    ]
                    for cn in clones:
                        ci = self.clone_details[cn]
                        if not self.use_client_fio:
                            try:
                                self.k8s_utils.delete_job(
                                    ci["job_name"]
                                )
                                self.k8s_utils.delete_configmap(
                                    ci["configmap_name"]
                                )
                            except Exception:
                                pass
                        try:
                            self.k8s_utils.delete_pvc(cn)
                        except Exception:
                            pass
                        del self.clone_details[cn]
                    try:
                        self.k8s_utils.delete_volume_snapshot(snap_name)
                    except Exception:
                        pass
                    self.snapshot_details.pop(snap_name, None)
                    if snap_name in self.snapshot_names:
                        self.snapshot_names.remove(snap_name)

                if not self.use_client_fio:
                    try:
                        self.k8s_utils.delete_job(
                            pvc_info["job_name"]
                        )
                        self.k8s_utils.delete_configmap(
                            pvc_info["configmap_name"]
                        )
                    except Exception:
                        pass
                else:
                    client = pvc_info.get("client")
                    if client:
                        self._kill_fio_on_client(pvc_name, client)
                        sleep_n_sec(2)
                        try:
                            self.ssh_obj.unmount_path(
                                client, pvc_info["mount_path"]
                            )
                        except Exception:
                            pass
                        self._disconnect_lvol_on_client(
                            pvc_info.get("lvol_name", ""), client
                        )
                        self.lvol_mount_details.pop(
                            pvc_info.get("lvol_name"), None
                        )

                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass

                node_id = pvc_info.get("node_id")
                if node_id and node_id in self.node_vs_pvc:
                    if pvc_name in self.node_vs_pvc[node_id]:
                        self.node_vs_pvc[node_id].remove(pvc_name)
                del self.pvc_details[pvc_name]
                excess -= 1

        self.logger.info(
            f"[cap] After pruning: {self._count_total_resources()} "
            f"total resources (cap={self.MAX_TOTAL_LVOLS})"
        )

    # ── Main run loop ─────────────────────────────────────────────────────

    def run(self):
        """Resilient run loop: permanent resources + dynamic
        post-recovery."""
        self._ensure_k8s_utils()
        self._initialize_outage_log()
        self.logger.info(
            "=== Starting K8sNativeResilientFailoverTest ==="
        )

        # ── Cluster config ──
        cluster_details = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = cluster_details.get(
            "max_fault_tolerance", 1
        )
        self.logger.info(
            f"Cluster max_fault_tolerance: "
            f"{self.max_fault_tolerance}"
        )
        if self.npcs == 1:
            self.npcs = self.max_fault_tolerance
        if self.npcs > self.max_fault_tolerance:
            self.logger.warning(
                f"npcs={self.npcs} exceeds max_fault_tolerance="
                f"{self.max_fault_tolerance} — cluster may not "
                f"survive all simultaneous outages!"
            )
        if self.max_fault_tolerance >= 2:
            self.logger.info(
                f"FTT={self.max_fault_tolerance} — outage "
                f"candidates include ALL nodes"
            )
        self.logger.info(
            f"Running with npcs={self.npcs} simultaneous outages"
        )

        # ── Clean slate ──
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_storage_pool(self.pool_name)
        except Exception:
            pass
        pool_test = self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        self.pool_name = (
            self.pool_name
            if pool_test == self.pool_name
            else pool_test
        )

        cluster_id = self.cluster_id or ""
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        if self.tls_enabled:
            self.logger.info("TLS enabled — ensuring encryption pool exists")
            self.sbcli_utils.ensure_pool_exists(
                self.CRYPTO_POOL_NAME,
                cluster_id=self.cluster_id,
                encryption=True,
            )
            self.logger.info(
                "TLS enabled — creating crypto StorageClass "
                "with encryption=True"
            )
            self.k8s_utils.create_storage_class(
                name=self.CRYPTO_STORAGE_CLASS_NAME,
                cluster_id=cluster_id,
                pool_name=self.CRYPTO_POOL_NAME,
                ndcs=self.ndcs,
                npcs=self.npcs,
                encryption=True,
            )
        self.k8s_utils.delete_volume_snapshot_class(
            self.SNAPSHOT_CLASS_NAME
        )
        self.k8s_utils.create_volume_snapshot_class(
            self.SNAPSHOT_CLASS_NAME
        )
        sleep_n_sec(5)

        # ── Populate storage node maps ──
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = (
                result["secondary_node_id"]
            )
        self.logger.info(
            f"Storage nodes: {len(self.sn_nodes)}, "
            f"secondary map: {self.sn_primary_secondary_map}"
        )

        # ── Phase 1: Create permanent PVCs (2 per node, pinned) ──
        num_permanent = (
            self.PERMANENT_PVCS_PER_NODE * len(self.sn_nodes)
        )
        pinned_ids = []
        for node_id in self.sn_nodes:
            for _ in range(self.PERMANENT_PVCS_PER_NODE):
                pinned_ids.append(node_id)

        self.logger.info(
            f"[permanent] Creating {num_permanent} permanent PVCs "
            f"({self.PERMANENT_PVCS_PER_NODE} per node, pinned)"
        )
        self.create_pvcs_with_fio(num_permanent, node_ids=pinned_ids)
        self.permanent_pvcs = set(self.pvc_details.keys())
        self.logger.info(
            f"[permanent] {len(self.permanent_pvcs)} permanent "
            f"PVCs: {sorted(self.permanent_pvcs)}"
        )
        if not self.permanent_pvcs:
            raise RuntimeError(
                "[permanent] FATAL: No permanent PVCs were created — "
                "PVC provisioning is completely broken. "
                "Check CSI controller logs for connectivity errors."
            )
        sleep_n_sec(30)
        self._ensure_per_node_coverage()

        # ── Phase 2: Create permanent snapshots + clones ──
        self.logger.info(
            "[permanent] Creating 1 snapshot + 1 clone per node"
        )
        self._create_permanent_snapshots_and_clones()
        self.logger.info(
            f"[permanent] Permanent snapshots: "
            f"{sorted(self.permanent_snapshots)}"
        )
        self.logger.info(
            f"[permanent] Permanent clones: "
            f"{sorted(self.permanent_clones)}"
        )
        sleep_n_sec(30)

        self.logger.info(
            f"[permanent] Setup complete: "
            f"{len(self.permanent_pvcs)} PVCs, "
            f"{len(self.permanent_snapshots)} snapshots, "
            f"{len(self.permanent_clones)} clones (permanent). "
            f"Total resources: {self._count_total_resources()}"
        )

        # ── Stress loop ──
        iteration = 1
        test_failed = False
        failure_reasons = []
        try:
            while True:
                self.logger.info(f"=== Iteration {iteration} ===")

                validation_thread = threading.Thread(
                    target=self.validate_iostats_continuously,
                    daemon=True,
                )
                validation_thread.start()

                if iteration > 1:
                    self.restart_fio(iteration)

                # ── Outage phase ──
                outage_events = self.perform_n_plus_k_outages()

                # ── Operations during outage ──
                # Delete dynamic PVCs (deferred verification
                # happens post-recovery via validate_pending_deletions)
                dynamic_pvcs = [
                    p for p in self.pvc_details
                    if p not in self.permanent_pvcs
                ]
                if dynamic_pvcs:
                    del_count = min(
                        iteration, len(dynamic_pvcs)
                    )
                    del_count = max(del_count, 1)
                    self.logger.info(
                        f"[outage] Deleting {del_count} dynamic "
                        f"PVCs during outage"
                    )
                    self._delete_dynamic_pvcs(del_count)

                # Create new PVCs (will stay Pending until
                # cluster recovers — FIO starts post-recovery)
                create_count = max(del_count + 2, 2) if dynamic_pvcs else 2
                self.logger.info(
                    f"[outage] Creating {create_count} deferred "
                    f"PVCs (will bind after recovery)"
                )
                self._create_pvcs_deferred(create_count)

                sleep_n_sec(280)

                # ── Recovery phase ──
                for node, outage_type, _ in outage_events:
                    self.current_outage_node = node
                    if (outage_type == "container_stop"
                            and self.npcs > 1):
                        self.restart_nodes_after_failover(
                            outage_type, restart=True
                        )
                    else:
                        self.restart_nodes_after_failover(
                            outage_type
                        )
                    self.logger.info(
                        "Waiting for fallback recovery."
                    )
                    sleep_n_sec(100)

                # ── Health checks ──
                for node, _, _ in outage_events:
                    try:
                        self.sbcli_utils.wait_for_health_status(
                            node, True, timeout=300
                        )
                    except Exception as exc:
                        self.logger.warning(
                            f"Health check did not pass for "
                            f"{node}: {exc}"
                        )

                self.collect_outage_diagnostics("post_recovery")

                # ── Process deferred operations ──
                if self.use_client_fio:
                    try:
                        self.retry_failed_nvme_connects()
                    except AssertionError as exc:
                        msg = f"[iteration {iteration}] NVMe reconnect failed: {exc}"
                        self.logger.error(msg)
                        failure_reasons.append(msg)
                        test_failed = True
                    self.retry_failed_secondary_connects()
                self.validate_pending_deletions()

                # ── Bind deferred PVCs + start FIO ──
                self._bind_deferred_pvcs_and_start_fio()

                # ── Create snapshots + clones (post-recovery) ──
                if self._is_cluster_fully_online():
                    self.create_snapshots_and_clones()

                # ── Enforce lvol cap ──
                self._enforce_lvol_cap()

                # ── Validation phase ──
                sleep_n_sec(300)
                self.check_core_dump()

                time_duration = (
                    self.common_utils.calculate_time_duration(
                        start_timestamp=self.outage_start_time,
                        end_timestamp=self.outage_end_time,
                    )
                )
                try:
                    self.common_utils.validate_io_stats(
                        cluster_id=self.cluster_id,
                        start_timestamp=self.outage_start_time,
                        end_timestamp=self.outage_end_time,
                        time_duration=time_duration,
                        warn_only=True,
                    )
                except AssertionError as exc:
                    msg = (
                        f"[iteration {iteration}] IO validation "
                        f"failed — zero IO detected: {exc}"
                    )
                    self.logger.error(msg)
                    failure_reasons.append(msg)
                    test_failed = True
                self.validate_migration_for_node(
                    self.outage_start_time, 2000, None, 60
                )
                self.wait_for_fio_complete()
                try:
                    self.validate_fio_jobs()
                except Exception as exc:
                    msg = (
                        f"[iteration {iteration}] FIO validation "
                        f"failed: {exc}"
                    )
                    self.logger.error(msg)
                    failure_reasons.append(msg)
                    test_failed = True

                self.logger.info(
                    f"=== Iteration {iteration} complete "
                    f"(total resources: "
                    f"{self._count_total_resources()}) ==="
                )
                self.collect_outage_diagnostics(
                    f"end_iteration_{iteration}"
                )

                if test_failed:
                    self.logger.error(
                        f"[iteration {iteration}] Test marked "
                        f"as FAILED — stopping stress loop"
                    )
                    break

                iteration += 1

        except Exception as exc:
            test_failed = True
            failure_reasons.append(f"Unhandled exception: {exc}")
            raise
        finally:
            if test_failed:
                summary = "; ".join(failure_reasons) if failure_reasons else "unknown error"
                self.logger.error(
                    f"[cleanup] Test FAILED — reasons: {summary}"
                )
                self.logger.info(
                    "[cleanup] Skipping resource cleanup to "
                    "preserve state for debugging"
                )
                raise AssertionError(
                    f"Stress test failed: {summary}"
                )
            else:
                self._cleanup_all_k8s_resources()
