"""
K8sUtils: Kubernetes-specific helper for simplyblock stress/e2e tests.

All sbcli CLI commands are routed through kubectl exec into the
simplyblock-admin-control pod (running on the K3s master node).

    runner → SSH to K3s master → kubectl exec -n simplyblock <admin-pod> -- bash -c '<cmd>'

Container-crash simulation replaces docker stop with kubectl delete pod:

    runner → SSH to K3s master → kubectl delete pod snode-spdk-pod-<x> -n simplyblock

Network outage (interface block/unblock) still uses SSH directly to the
storage-node host via the underlying SshUtils instance — same as bare-metal.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import time
from datetime import datetime, timezone
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


class K8sUtils:
    """
    Kubernetes-aware command executor and failover helper.

    Parameters
    ----------
    ssh_obj : SshUtils
        An already-connected SshUtils instance.  kubectl commands are issued
        by SSH-ing to ``mgmt_node`` and running kubectl there.
    mgmt_node : str
        IP of the K3s master node (= first entry of MNODES / K3S_MNODES).
        kubectl must be available and configured on this host.
    namespace : str
        Kubernetes namespace where simplyblock is deployed (default: "simplyblock").
    """

    def __init__(self, ssh_obj, mgmt_node: str, namespace: str = "simplyblock"):
        self.ssh_obj = ssh_obj
        self.mgmt_node = mgmt_node
        self.namespace = namespace
        self._admin_pod: str | None = None
        self.logger = setup_logger(__name__)
        # Use local subprocess when K8S_LOCAL_KUBECTL=1 is set explicitly,
        # or when the runner is on the mgmt node (bastion == mgmt_node) AND
        # this is a k8s deployment (ssh_obj has no real bastion to proxy through).
        _bastion = getattr(ssh_obj, "bastion_server", None)
        _local_env = os.environ.get("K8S_LOCAL_KUBECTL", "").lower() in ("1", "true", "yes")
        _same_as_bastion = bool(_bastion) and mgmt_node == _bastion
        self.use_local_kubectl = _local_env or _same_as_bastion
        if self.use_local_kubectl:
            self.logger.info("[K8sUtils] Local kubectl mode enabled (subprocess)")

    # ── kubectl dispatch ─────────────────────────────────────────────────────

    def _exec_kubectl(self, cmd: str, supress_logs: bool = False,
                      timeout: int = 300):
        """
        Execute *cmd* either locally via subprocess (when use_local_kubectl=True)
        or via SSH to mgmt_node.  Returns (stdout, stderr) strings.

        *timeout* caps subprocess execution (default 300s / 5 min).
        """
        if self.use_local_kubectl:
            if not supress_logs:
                self.logger.info(f"[K8sUtils] local: {cmd}")
            try:
                result = subprocess.run(
                    ["bash", "-c", cmd],
                    capture_output=True, text=True,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                msg = f"[K8sUtils] subprocess timed out after {timeout}s: {cmd[:120]}"
                if not supress_logs:
                    self.logger.warning(msg)
                return "", msg
            if not supress_logs:
                if result.stdout.strip():
                    self.logger.info(f"[K8sUtils] stdout: {result.stdout.strip()}")
                if result.stderr.strip():
                    self.logger.info(f"[K8sUtils] stderr: {result.stderr.strip()}")
            return result.stdout, result.stderr
        return self.ssh_obj.exec_command(self.mgmt_node, cmd, supress_logs=supress_logs)

    # ── Admin pod discovery ──────────────────────────────────────────────────

    def get_admin_pod(self, refresh: bool = False) -> str:
        """
        Return the name of the simplyblock-admin-control-* pod.

        The result is cached after the first successful call.
        Pass ``refresh=True`` to force a fresh lookup (e.g. after a restart).
        """
        if self._admin_pod and not refresh:
            return self._admin_pod

        out, _ = self._exec_kubectl(
            (
                f"kubectl get pods -n {self.namespace} --no-headers "
                f"-o custom-columns=:metadata.name "
                f"| grep simplyblock-admin-control | head -1"
            ),
            supress_logs=True,
        )
        pod = out.strip()
        if not pod:
            raise RuntimeError(
                f"[K8sUtils] No simplyblock-admin-control pod found in namespace '{self.namespace}'"
            )
        self._admin_pod = pod
        self.logger.info(f"[K8sUtils] Admin pod resolved: {pod}")
        return pod

    # ── sbcli command execution ──────────────────────────────────────────────

    def exec_sbcli(self, command: str, supress_logs: bool = False):
        """
        Execute *command* inside the simplyblock-admin-control pod via kubectl exec.

        If the cached admin pod no longer exists (NotFound), the pod name is
        re-resolved and the command is retried once.

        Returns the same (stdout, stderr) tuple as SshUtils.exec_command.
        """
        if not supress_logs:
            self.logger.info(f"[sbcli] {command}")
        admin_pod = self.get_admin_pod()
        kubectl_cmd = (
            f"kubectl exec -n {self.namespace} {admin_pod} -- "
            f"bash -c {shlex.quote(command)}"
        )
        stdout, stderr = self._exec_kubectl(kubectl_cmd, supress_logs=supress_logs)

        # If the admin pod was recreated (e.g. during upgrade), retry with
        # a freshly-resolved pod.  kubectl may report different error strings
        # depending on the phase of termination:
        #   - "NotFound"                                (pod fully deleted)
        #   - "unable to upgrade connection: pod does not exist"
        #   - "pod … not found"
        _err = stderr or ""
        _pod_gone = (
            "NotFound" in _err
            or "pod does not exist" in _err
            or "pod not found" in _err.lower()
        )
        if _pod_gone:
            self.logger.warning(
                f"[K8sUtils] Admin pod '{admin_pod}' gone ({_err.strip()[:80]}), "
                "re-resolving..."
            )
            admin_pod = self.get_admin_pod(refresh=True)
            kubectl_cmd = (
                f"kubectl exec -n {self.namespace} {admin_pod} -- "
                f"bash -c {shlex.quote(command)}"
            )
            stdout, stderr = self._exec_kubectl(kubectl_cmd, supress_logs=supress_logs)

        return stdout, stderr

    # ── K8s node name resolution ─────────────────────────────────────────────

    def _get_k8s_node_name(self, node_ip: str) -> str:
        """Return the K8s node name (hostname) for a given storage-node IP."""
        out, _ = self._exec_kubectl(
            (
                "kubectl get nodes -o wide --no-headers "
                f"| awk '{{print $1, $6}}' | grep '{node_ip}' | awk '{{print $1}}'"
            ),
            supress_logs=True,
        )
        name = out.strip()
        if not name:
            raise RuntimeError(
                f"[K8sUtils] Cannot resolve K8s node name for IP {node_ip!r}"
            )
        return name

    def get_all_k8s_node_names(self) -> list[str]:
        """Return a list of ALL K8s node hostnames."""
        out, _ = self._exec_kubectl(
            "kubectl get nodes --no-headers -o custom-columns=':metadata.name'",
            supress_logs=True,
        )
        return [n.strip() for n in out.strip().splitlines() if n.strip()]

    def detect_openshift(self) -> bool:
        """Return True if the cluster is OpenShift.

        Checks for the ``openshift-apiserver`` namespace which only
        exists on OpenShift clusters.  This avoids false positives on
        machines where the ``oc`` CLI is installed but the target
        cluster is not OpenShift (e.g. Talos).

        The result is cached after the first call.
        """
        if hasattr(self, "_is_openshift"):
            return self._is_openshift
        try:
            out, _ = self._exec_kubectl(
                "kubectl get namespace openshift-apiserver "
                "--no-headers 2>/dev/null && echo OCP_YES || echo OCP_NO",
                supress_logs=True,
            )
            self._is_openshift = "OCP_YES" in out
        except Exception:
            self._is_openshift = False
        self.logger.info(
            f"[K8sUtils] Platform detection: openshift={self._is_openshift}"
        )
        return self._is_openshift

    def detect_talos(self) -> bool:
        """Return True if the cluster nodes run Talos Linux.

        Checks the ``osImage`` field of the first node.  The result is
        cached after the first call.
        """
        if hasattr(self, "_is_talos"):
            return self._is_talos
        try:
            out, _ = self._exec_kubectl(
                "kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.osImage}'",
                supress_logs=True,
            )
            self._is_talos = "Talos" in (out or "")
        except Exception:
            self._is_talos = False
        self.logger.info(
            f"[K8sUtils] Platform detection: talos={self._is_talos}"
        )
        return self._is_talos

    # ── SPDK pod operations ──────────────────────────────────────────────────

    def get_spdk_pod_name(self, node_ip: str) -> str:
        """
        Return the name of the ``snode-spdk-pod-*`` pod running on the
        storage node with the given IP.

        Raises RuntimeError if the pod cannot be found.
        """
        k8s_node = self._get_k8s_node_name(node_ip)
        out, _ = self._exec_kubectl(
            (
                f"kubectl get pods -n {self.namespace} -o wide --no-headers "
                f"| awk '{{print $1, $7}}' "
                f"| grep '{k8s_node}' | grep snode-spdk | awk '{{print $1}}'"
            ),
            supress_logs=True,
        )
        pod = out.strip()
        if not pod:
            raise RuntimeError(
                f"[K8sUtils] No snode-spdk-pod found on K8s node {k8s_node!r} (IP: {node_ip})"
            )
        self.logger.info(f"[K8sUtils] SPDK pod for {node_ip}: {pod}")
        return pod

    def stop_spdk_pod(self, node_ip: str) -> str:
        """
        Force-delete the ``snode-spdk-pod-*`` for the given storage node IP.

        Kubernetes will automatically recreate the pod (DaemonSet / StatefulSet).
        Returns the pod name that was deleted.
        """
        pod_name = self.get_spdk_pod_name(node_ip)
        self.logger.info(
            f"[K8sUtils] Force-deleting SPDK pod {pod_name!r} on node {node_ip}"
        )
        self._exec_kubectl(
            (
                f"kubectl delete pod {pod_name} -n {self.namespace} "
                f"--grace-period=0 --force 2>&1 || true"
            ),
        )
        return pod_name

    def exec_in_spdk_container(self, node_ip: str, command: str) -> tuple:
        """Execute a command inside the spdk-container of the SPDK pod
        for the given storage node IP.

        Returns (stdout, stderr).
        """
        pod_name = self.get_spdk_pod_name(node_ip)
        return self._exec_kubectl(
            f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
            f"bash -c {shlex.quote(command)}"
        )

    def _find_spdk_sock(self, pod_name: str) -> str:
        """Return the spdk.sock path inside spdk-container (searches /mnt/ramdisk)."""
        out, _ = self._exec_kubectl(
            f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
            f"bash -c 'find /mnt/ramdisk -name spdk.sock -maxdepth 3 2>/dev/null | head -1'",
            supress_logs=True,
        )
        sock = out.strip()
        if not sock:
            raise RuntimeError(f"[K8sUtils] spdk.sock not found in {pod_name}")
        return sock

    def dump_lvstore_k8s(self, storage_node_id: str,
                          storage_node_ip: str, logs_path: str,
                          sbcli_cmd: str = "sbctl") -> None:
        """
        K8s equivalent of ssh_utils.dump_lvstore:
          1. Run sbcli sn dump-lvstore via admin pod.
          2. Parse dump file path from output.
          3. kubectl cp the file from spdk-container → logs_path/<pod_name>/lvstore_dumps/.
        """
        try:
            out, err = self.exec_sbcli(
                f"{sbcli_cmd} --dev -d sn dump-lvstore {storage_node_id}"
            )
            combined = (out or "") + (err or "")

            dump_file = None
            for line in combined.splitlines():
                if "LVS dump file will be here" in line:
                    # Line format: "...: INFO: LVS dump file will be here: /etc/simplyblock/..."
                    # Split on the marker text to reliably extract the path
                    parts = line.split("LVS dump file will be here:", 1)
                    if len(parts) == 2:
                        dump_file = parts[1].strip()
                    break

            if not dump_file:
                self.logger.warning(
                    f"[dump_lvstore_k8s] No dump file path in output for {storage_node_id}"
                )
                return

            pod_name = self.get_spdk_pod_name(storage_node_ip)
            dest_dir = os.path.join(logs_path, pod_name, "lvstore_dumps")
            os.makedirs(dest_dir, exist_ok=True)
            safe_name = os.path.basename(dump_file).replace(":", "_")
            dest_path = os.path.join(dest_dir, safe_name)

            # kubectl cp misinterprets colons in filenames as pod:path
            # separators, so copy to a colon-free temp path first.
            kexec = f"kubectl exec -n {self.namespace} {pod_name} -c spdk-container --"
            tmp_path = f"/tmp/{safe_name}"
            self._exec_kubectl(f"{kexec} cp {dump_file} {tmp_path}")
            self._exec_kubectl(
                f"kubectl cp -n {self.namespace} {pod_name}:{tmp_path} "
                f"-c spdk-container {dest_path}"
            )
            self._exec_kubectl(f"{kexec} rm -f {tmp_path}", supress_logs=True)
            self.logger.info(f"[dump_lvstore_k8s] {dump_file} → {dest_path}")
        except Exception as e:
            self.logger.warning(f"[dump_lvstore_k8s] FAILED node={storage_node_id}: {e}")

    def fetch_distrib_logs_k8s(self, storage_node_id: str,
                                storage_node_ip: str, logs_path: str) -> bool:
        """
        K8s equivalent of ssh_utils.fetch_distrib_logs:
          1. Find spdk.sock inside spdk-container.
          2. Get bdevs via RPC, collect distrib_* names.
          3. For each distrib: create JSON config and run rpc_sock.py
             with timeout + retry (120s first, then 600s).
          4. kubectl cp result files from /tmp inside container →
             logs_path/<pod_name>/distrib_logs/.
        Returns True (non-fatal failures are logged and skipped).
        """
        try:
            pod_name = self.get_spdk_pod_name(storage_node_ip)
            sock = self._find_spdk_sock(pod_name)
            dest_dir = os.path.join(logs_path, pod_name, "distrib_logs")
            os.makedirs(dest_dir, exist_ok=True)

            kexec = (
                f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} --"
            )
            rpc_base = f"{kexec} sudo python spdk/scripts/rpc.py -s {sock}"

            # 1. Get bdevs
            bdev_out, _ = self._exec_kubectl(f"{rpc_base} bdev_get_bdevs", supress_logs=True)
            try:
                bdevs = json.loads(bdev_out)
                distribs = sorted({
                    b.get("name", "")
                    for b in bdevs
                    if isinstance(b, dict) and str(b.get("name", "")).startswith("distrib_")
                })
            except Exception as e:
                self.logger.warning(f"[fetch_distrib_logs_k8s] bdev parse failed: {e}")
                return True

            if not distribs:
                self.logger.warning(f"[fetch_distrib_logs_k8s] No distrib_* bdevs on {storage_node_ip}")
                return True

            self.logger.info(f"[fetch_distrib_logs_k8s] distribs={distribs} pod={pod_name}")

            # 2. Dump each distrib with timeout + retry
            for distrib in distribs:
                try:
                    json_cfg = json.dumps({
                        "subsystems": [{
                            "subsystem": "distr",
                            "config": [{
                                "method": "distr_debug_placement_map_dump",
                                "params": {"name": distrib}
                            }]
                        }]
                    })
                    stack_file = f"/tmp/stack_{distrib}.json"
                    rpc_log = f"/tmp/rpc_{distrib}.log"

                    # Write JSON config into the container
                    self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"echo '{json_cfg}' > {stack_file}"),
                        supress_logs=True,
                    )

                    # Try with 120s timeout first, then retry with 600s
                    rpc_succeeded = False
                    for attempt, tmo in enumerate([120, 600], 1):
                        self.logger.info(
                            f"[fetch_distrib_logs_k8s] Dumping {distrib} "
                            f"(attempt {attempt}, timeout={tmo}s)"
                        )
                        rpc_cmd = (
                            f"timeout {tmo} kubectl exec {pod_name} "
                            f"-c spdk-container -n {self.namespace} -- "
                            f"bash -c "
                            + shlex.quote(
                                f"python scripts/rpc_sock.py {stack_file} {sock} "
                                f"> {rpc_log} 2>&1"
                            )
                            + "; echo EXIT_CODE=$?"
                        )
                        rpc_out, rpc_err = self._exec_kubectl(rpc_cmd, supress_logs=True)
                        combined = (rpc_out or "") + (rpc_err or "")
                        if "EXIT_CODE=124" in combined or "EXIT_CODE=137" in combined:
                            self.logger.warning(
                                f"[fetch_distrib_logs_k8s] {distrib} RPC timed out "
                                f"after {tmo}s (attempt {attempt})"
                            )
                            continue
                        rpc_succeeded = True
                        break

                    if not rpc_succeeded:
                        self.logger.warning(
                            f"[fetch_distrib_logs_k8s] {distrib} RPC timed out on "
                            f"all attempts — skipping"
                        )
                        self._exec_kubectl(
                            f"{kexec} bash -c "
                            + shlex.quote(f"rm -f {stack_file} {rpc_log} || true"),
                            supress_logs=True,
                        )
                        continue

                    # Read the RPC log
                    log_out, _ = self._exec_kubectl(
                        f"{kexec} bash -c 'cat {rpc_log} 2>/dev/null || true'",
                        supress_logs=True,
                    )
                    self.logger.info(
                        f"[fetch_distrib_logs_k8s] {distrib} rpc_log: "
                        f"{(log_out or '').strip()[:500]}"
                    )

                    # Copy the RPC log file out
                    rpc_log_dest = os.path.join(dest_dir, f"rpc_{distrib}.log")
                    self._exec_kubectl(
                        f"kubectl cp -n {self.namespace} {pod_name}:{rpc_log} "
                        f"-c spdk-container {rpc_log_dest}"
                    )

                    # Collect any /tmp files matching this distrib name
                    ls_out, _ = self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"ls /tmp/ 2>/dev/null | grep -F '{distrib}' || true"),
                        supress_logs=True,
                    )
                    for fname in (ls_out or "").splitlines():
                        fname = fname.strip()
                        if not fname:
                            continue
                        dest = os.path.join(dest_dir, fname)
                        self._exec_kubectl(
                            f"kubectl cp -n {self.namespace} {pod_name}:/tmp/{fname} "
                            f"-c spdk-container {dest}"
                        )
                        self.logger.info(f"[fetch_distrib_logs_k8s] copied /tmp/{fname} → {dest}")

                    # Cleanup temp files in container
                    self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(f"rm -f {stack_file} {rpc_log} || true"),
                        supress_logs=True,
                    )
                except Exception as e:
                    self.logger.warning(f"[fetch_distrib_logs_k8s] distrib={distrib} error: {e}")

            return True
        except Exception as e:
            self.logger.warning(f"[fetch_distrib_logs_k8s] FAILED node={storage_node_ip}: {e}")
            return True

    def wait_spdk_pod_running(self, node_ip: str, timeout: int = 600) -> None:
        """
        Block until the ``snode-spdk-pod-*`` on the given storage node IP
        reaches the *Running* state, or raise TimeoutError.
        """
        k8s_node = self._get_k8s_node_name(node_ip)
        self.logger.info(
            f"[K8sUtils] Waiting for snode-spdk-pod on {k8s_node} to be Running "
            f"(timeout={timeout}s)..."
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                (
                    f"kubectl get pods -n {self.namespace} -o wide --no-headers "
                    f"| grep snode-spdk | grep '{k8s_node}' | awk '{{print $3}}' || true"
                ),
                supress_logs=True,
            )
            if out.strip() == "Running":
                self.logger.info(
                    f"[K8sUtils] snode-spdk-pod on {k8s_node} is Running."
                )
                return
            time.sleep(15)
        raise TimeoutError(
            f"[K8sUtils] snode-spdk-pod on {k8s_node} did not reach Running within {timeout}s"
        )

    def restart_spdk_pod(self, node_ip: str) -> None:
        """
        K8s equivalent of ssh_utils.stop_spdk_process:
        delete the SPDK pod on the given node so Kubernetes restarts it automatically.
        """
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
            self.logger.info(f"[restart_spdk_pod] Deleting pod {pod_name} on {node_ip}")
            self._exec_kubectl(f"kubectl delete pod {pod_name} -n {self.namespace}")
            self.logger.info(f"[restart_spdk_pod] Pod {pod_name} deleted; waiting for restart")
        except Exception as e:
            self.logger.warning(f"[restart_spdk_pod] FAILED for {node_ip}: {e}")

    # ── Cluster credentials ──────────────────────────────────────────────────

    def get_cluster_credentials(self, sbcli_cmd: str = "sbctl") -> tuple:
        """
        Fetch CLUSTER_ID and CLUSTER_SECRET by running sbcli inside the admin pod.

        Returns (cluster_id, cluster_secret) as strings.
        """
        out_id, _ = self.exec_sbcli(
            f"{sbcli_cmd} cluster list"
            r" | grep -Eo '[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}'"
            " | head -1"
        )
        cluster_id = out_id.strip()
        if not cluster_id:
            raise RuntimeError(
                "[K8sUtils] Could not extract cluster_id via kubectl exec"
            )

        out_sec, _ = self.exec_sbcli(
            f"{sbcli_cmd} cluster get-secret {cluster_id}"
        )
        cluster_secret = out_sec.strip().splitlines()[-1].strip()
        if not cluster_secret:
            raise RuntimeError(
                f"[K8sUtils] Could not get cluster_secret for {cluster_id}"
            )

        return cluster_id, cluster_secret

    # ── Pod readiness utilities ──────────────────────────────────────────────

    def list_files_in_spdk_pod(self, node_ip: str, path: str) -> list:
        """
        List files in *path* inside the ``spdk-container`` of the SPDK pod
        running on *node_ip*.  Returns a list of filename strings (no paths).

        Used as a K8s substitute for ``ssh_obj.list_files(node_ip, path)``
        when checking for core dumps at ``/etc/simplyblock/``.
        """
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
            out, _ = self._exec_kubectl(
                f"kubectl exec {pod_name} -c spdk-container -n {self.namespace} -- "
                f"bash -c 'ls {shlex.quote(path)} 2>/dev/null || true'",
                supress_logs=True,
            )
            return [f.strip() for f in out.splitlines() if f.strip()]
        except Exception as e:
            self.logger.warning(f"[list_files_in_spdk_pod] node={node_ip} path={path}: {e}")
            return []

    # ── Core dump collection ────────────────────────────────────────────────

    def copy_core_dumps_from_spdk_pod(self, node_ip: str,
                                       logs_path: str) -> list[str]:
        """Copy core dump files from inside the SPDK pod to *logs_path*.

        Looks for ``*.core*.zst`` and ``core.*`` files in
        ``/etc/simplyblock/`` inside the spdk-container and copies them
        via ``kubectl cp`` (same pattern as :meth:`dump_lvstore_k8s`).

        Returns a list of local paths of copied files.
        """
        copied: list[str] = []
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Cannot find SPDK pod for {node_ip}, "
                f"skipping in-pod core dump copy: {exc}"
            )
            return copied

        files = self.list_files_in_spdk_pod(node_ip, "/etc/simplyblock/")
        core_files = [
            f for f in files
            if "core" in f.lower() and "tmp_cores" not in f
        ]
        if not core_files:
            return copied

        dest_dir = os.path.join(logs_path, pod_name, "core_dumps")
        os.makedirs(dest_dir, exist_ok=True)
        kexec = (
            f"kubectl exec -n {self.namespace} {pod_name} -c spdk-container --"
        )

        for fname in core_files:
            src = f"/etc/simplyblock/{fname}"
            # kubectl cp misinterprets colons — copy to a colon-free
            # temp path first (same workaround as dump_lvstore_k8s).
            safe_name = fname.replace(":", "_")
            tmp_path = f"/tmp/{safe_name}"
            dest_path = os.path.join(dest_dir, safe_name)
            try:
                self._exec_kubectl(
                    f"{kexec} cp {shlex.quote(src)} {tmp_path}",
                    supress_logs=True,
                )
                self._exec_kubectl(
                    f"kubectl cp -n {self.namespace} "
                    f"{pod_name}:{tmp_path} -c spdk-container "
                    f"{shlex.quote(dest_path)}",
                    supress_logs=True,
                    timeout=600,
                )
                self._exec_kubectl(
                    f"{kexec} rm -f {tmp_path}", supress_logs=True
                )
                if os.path.exists(dest_path):
                    size_mb = os.path.getsize(dest_path) / (1024 * 1024)
                    self.logger.info(
                        f"[coredump] Copied pod core dump {fname} from "
                        f"{pod_name} ({size_mb:.1f} MB) -> {dest_path}"
                    )
                    copied.append(dest_path)
                else:
                    self.logger.warning(
                        f"[coredump] kubectl cp succeeded but file not "
                        f"found at {dest_path}"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] Failed to copy {fname} from {pod_name}: {exc}"
                )
        return copied

    def collect_host_core_dumps(self, node_ip: str, local_dir: str,
                                max_size_mb: int = 500) -> list[str]:
        """Collect host-level core dumps from a K8s node.

        **Primary path**: uses the already-running SPDK pod (privileged)
        to access the host filesystem via ``/proc/1/root/``.

        **Fallback**: if the SPDK pod is not running, deploys a
        platform-aware temporary pod/debug session.

        Parameters
        ----------
        node_ip : str
            Storage-node management IP.
        local_dir : str
            Local directory to save collected data.
        max_size_mb : int
            Maximum core dump file size (MB) to copy. Files larger
            than this are logged but not copied.

        Returns
        -------
        list[str]
            Local paths of all saved files (listings, info, dumps).
        """
        saved: list[str] = []
        os.makedirs(local_dir, exist_ok=True)
        host_coredump_dir = "/proc/1/root/var/lib/systemd/coredump"

        # ── Try via running SPDK pod ──────────────────────────────────────
        try:
            pod_name = self.get_spdk_pod_name(node_ip)
        except Exception:
            pod_name = None

        if pod_name:
            try:
                saved = self._collect_host_core_dumps_via_spdk(
                    pod_name, node_ip, local_dir, host_coredump_dir,
                    max_size_mb,
                )
                return saved
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] SPDK pod collection failed for "
                    f"{node_ip}: {exc}, trying fallback"
                )

        # ── Fallback ──────────────────────────────────────────────────────
        try:
            node_name = self._get_k8s_node_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Cannot resolve K8s node name for {node_ip}, "
                f"skipping host core dump fallback: {exc}"
            )
            return saved

        try:
            saved = self._collect_host_core_dumps_fallback(
                node_name, node_ip, local_dir, max_size_mb,
            )
        except Exception as exc:
            self.logger.warning(
                f"[coredump] Fallback collection failed for "
                f"{node_ip} ({node_name}): {exc}"
            )
        return saved

    def _collect_host_core_dumps_via_spdk(
        self, pod_name: str, node_ip: str, local_dir: str,
        host_coredump_dir: str, max_size_mb: int,
    ) -> list[str]:
        """Collect host core dumps using the running SPDK pod.

        The SPDK pod is privileged and can read the host filesystem
        via ``/proc/1/root/``.
        """
        saved: list[str] = []
        kexec = (
            f"kubectl exec {pod_name} -c spdk-container "
            f"-n {self.namespace} --"
        )
        label = node_ip.replace(".", "_")

        # 1. List host core dumps
        out, _ = self._exec_kubectl(
            f"{kexec} bash -c "
            f"'ls -la {host_coredump_dir}/ 2>/dev/null || echo EMPTY'",
            supress_logs=True,
        )
        listing_path = os.path.join(local_dir, f"coredump_listing_{label}.txt")
        with open(listing_path, "w") as f:
            f.write(f"# Host core dumps on {node_ip} (via SPDK pod {pod_name})\n")
            f.write("# Path: /var/lib/systemd/coredump/\n\n")
            f.write(out)
        saved.append(listing_path)

        if "EMPTY" in out or not out.strip():
            self.logger.info(
                f"[coredump] No host-level core dumps on {node_ip}"
            )
            return saved

        # Parse core file names from ls output
        core_files = []
        for line in out.strip().splitlines():
            parts = line.split()
            if parts and "core" in line.lower() and not line.startswith("total"):
                fname = parts[-1]
                core_files.append(fname)

        if core_files:
            self.logger.warning(
                f"[coredump] HOST CORE DUMPS on {node_ip}: {core_files}"
            )

        # 2. Try coredumpctl list (best-effort)
        try:
            out, _ = self._exec_kubectl(
                f"{kexec} bash -c "
                f"'chroot /proc/1/root coredumpctl list --no-pager "
                f"2>/dev/null || echo COREDUMPCTL_UNAVAILABLE'",
                supress_logs=True,
                timeout=60,
            )
            if "COREDUMPCTL_UNAVAILABLE" not in out and out.strip():
                fpath = os.path.join(
                    local_dir, f"coredumpctl_list_{label}.txt"
                )
                with open(fpath, "w") as f:
                    f.write(out)
                saved.append(fpath)
                self.logger.info(
                    f"[coredump] Saved coredumpctl list for {node_ip}"
                )
        except Exception as exc:
            self.logger.info(
                f"[coredump] coredumpctl list unavailable on {node_ip}: {exc}"
            )

        # 3. Try coredumpctl info (best-effort, contains stack traces)
        if core_files:
            try:
                out, _ = self._exec_kubectl(
                    f"{kexec} bash -c "
                    f"'chroot /proc/1/root coredumpctl info --no-pager "
                    f"2>/dev/null || true'",
                    supress_logs=True,
                    timeout=120,
                )
                if out and out.strip():
                    fpath = os.path.join(
                        local_dir, f"coredumpctl_info_{label}.txt"
                    )
                    with open(fpath, "w") as f:
                        f.write(out)
                    saved.append(fpath)
                    self.logger.info(
                        f"[coredump] Saved coredumpctl info for {node_ip}"
                    )
            except Exception as exc:
                self.logger.info(
                    f"[coredump] coredumpctl info unavailable on "
                    f"{node_ip}: {exc}"
                )

        # 4. Copy actual core dump files under size threshold
        for fname in core_files:
            host_path = f"{host_coredump_dir}/{fname}"
            try:
                size_out, _ = self._exec_kubectl(
                    f"{kexec} bash -c "
                    f"'stat -c %s {shlex.quote(host_path)} 2>/dev/null "
                    f"|| echo 0'",
                    supress_logs=True,
                )
                size_bytes = int(size_out.strip() or "0")
                size_mb = size_bytes / (1024 * 1024)
                self.logger.info(
                    f"[coredump] {node_ip}: {fname} = {size_mb:.1f} MB"
                )
                if max_size_mb > 0 and size_mb > max_size_mb:
                    self.logger.warning(
                        f"[coredump] Skipping copy of {fname} on {node_ip} "
                        f"({size_mb:.1f} MB > {max_size_mb} MB limit)"
                    )
                    continue
            except Exception:
                self.logger.warning(
                    f"[coredump] Cannot stat {fname} on {node_ip}"
                )
                continue

            safe_name = fname.replace(":", "_")
            local_path = os.path.join(local_dir, f"{label}_{safe_name}")
            tmp_path = f"/tmp/coredump_{safe_name}"
            try:
                # Copy from host path (via /proc/1/root) to temp in container
                self._exec_kubectl(
                    f"{kexec} bash -c "
                    f"'cp {shlex.quote(host_path)} {tmp_path}'",
                    supress_logs=True,
                    timeout=600,
                )
                # kubectl cp from container temp to local
                self._exec_kubectl(
                    f"kubectl cp -n {self.namespace} "
                    f"{pod_name}:{tmp_path} -c spdk-container "
                    f"{shlex.quote(local_path)}",
                    supress_logs=True,
                    timeout=600,
                )
                self._exec_kubectl(
                    f"{kexec} rm -f {tmp_path}", supress_logs=True
                )
                if os.path.exists(local_path):
                    self.logger.info(
                        f"[coredump] Copied host core dump {fname} from "
                        f"{node_ip} ({size_mb:.1f} MB) -> {local_path}"
                    )
                    saved.append(local_path)
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] Failed to copy {fname} from "
                    f"{node_ip}: {exc}"
                )
                # Clean up temp file on failure
                try:
                    self._exec_kubectl(
                        f"{kexec} rm -f {tmp_path}", supress_logs=True
                    )
                except Exception:
                    pass

        return saved

    def _collect_host_core_dumps_fallback(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """Collect host core dumps when the SPDK pod is not running.

        Uses platform-aware fallback:
        - **OpenShift**: ``oc debug node/<node> -- chroot /host``
        - **Vanilla K8s**: ephemeral privileged pod with ``nsenter``
        - **Talos Linux**: ephemeral privileged pod with ``hostPath``
          volume mount (no host binaries needed)
        """
        if self.detect_openshift():
            return self._fallback_via_oc_debug(
                node_name, node_ip, local_dir, max_size_mb
            )
        return self._fallback_via_privileged_pod(
            node_name, node_ip, local_dir, max_size_mb
        )

    def _fallback_via_oc_debug(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """OpenShift fallback: use ``oc debug node/`` for host access."""
        saved: list[str] = []
        label = node_ip.replace(".", "_")

        for cmd_name, host_cmd in [
            ("coredump_listing", "ls -la /var/lib/systemd/coredump/"),
            (
                "coredumpctl_list",
                "coredumpctl list --no-pager 2>/dev/null || echo UNAVAILABLE",
            ),
            (
                "coredumpctl_info",
                "coredumpctl info --no-pager 2>/dev/null || true",
            ),
        ]:
            try:
                out, _ = self._exec_kubectl(
                    f"oc debug node/{node_name} -- chroot /host "
                    f"bash -c {shlex.quote(host_cmd)} 2>/dev/null || true",
                    supress_logs=True,
                    timeout=120,
                )
                if out and out.strip() and "UNAVAILABLE" not in out:
                    fpath = os.path.join(
                        local_dir, f"{cmd_name}_{label}.txt"
                    )
                    with open(fpath, "w") as f:
                        f.write(out)
                    saved.append(fpath)
                    self.logger.info(
                        f"[coredump] Saved {cmd_name} for {node_ip} "
                        f"(oc debug fallback)"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"[coredump] oc debug {cmd_name} failed on "
                    f"{node_name}: {exc}"
                )
        return saved

    def _fallback_via_privileged_pod(
        self, node_name: str, node_ip: str, local_dir: str,
        max_size_mb: int,
    ) -> list[str]:
        """Vanilla K8s / Talos fallback: privileged pod with hostPath mount.

        Uses the container's own ``ls``/``sh`` on the mounted host
        directory, so no host binaries are required (Talos-compatible).
        """
        import hashlib

        saved: list[str] = []
        label = node_ip.replace(".", "_")
        hash_suffix = hashlib.md5(
            f"coredump-{node_name}-{time.time()}".encode()
        ).hexdigest()[:8]
        pod_name = f"coredump-collector-{hash_suffix}"
        ns = self.namespace

        yaml_spec = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {pod_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  nodeName: {node_name}\n"
            f"  hostPID: true\n"
            f"  tolerations:\n"
            f"  - operator: Exists\n"
            f"  containers:\n"
            f"  - name: collector\n"
            f"    image: busybox:1.37\n"
            f"    imagePullPolicy: IfNotPresent\n"
            f"    command: ['sleep', '300']\n"
            f"    securityContext:\n"
            f"      privileged: true\n"
            f"    volumeMounts:\n"
            f"    - name: host-coredumps\n"
            f"      mountPath: /host-coredumps\n"
            f"      readOnly: true\n"
            f"  volumes:\n"
            f"  - name: host-coredumps\n"
            f"    hostPath:\n"
            f"      path: /var/lib/systemd/coredump\n"
            f"      type: DirectoryOrCreate\n"
            f"  restartPolicy: Never\n"
        )

        try:
            # Deploy the collector pod
            self._exec_kubectl(
                f"kubectl delete pod {pod_name} -n {ns} "
                f"--force --grace-period=0 2>/dev/null || true",
                supress_logs=True,
            )
            self._exec_kubectl(
                f"cat <<'COREDUMP_EOF' | kubectl apply -f -\n"
                f"{yaml_spec}COREDUMP_EOF",
                supress_logs=True,
            )
            self._exec_kubectl(
                f"kubectl wait pod/{pod_name} -n {ns} "
                f"--for=condition=Ready --timeout=120s 2>/dev/null || true",
                supress_logs=True,
            )

            kexec = f"kubectl exec {pod_name} -n {ns} --"

            # 1. List core dumps via container's own ls
            out, _ = self._exec_kubectl(
                f"{kexec} ls -la /host-coredumps/ 2>/dev/null || echo EMPTY",
                supress_logs=True,
            )
            listing_path = os.path.join(
                local_dir, f"coredump_listing_{label}.txt"
            )
            with open(listing_path, "w") as f:
                f.write(
                    f"# Host core dumps on {node_ip} ({node_name}) "
                    f"[fallback pod]\n"
                )
                f.write("# Path: /var/lib/systemd/coredump/\n\n")
                f.write(out)
            saved.append(listing_path)

            if "EMPTY" in out or not out.strip():
                self.logger.info(
                    f"[coredump] No host-level core dumps on {node_ip} "
                    f"(fallback)"
                )
                return saved

            # Parse core file names
            core_files = []
            for line in out.strip().splitlines():
                parts = line.split()
                if (
                    parts
                    and "core" in line.lower()
                    and not line.startswith("total")
                ):
                    core_files.append(parts[-1])

            if core_files:
                self.logger.warning(
                    f"[coredump] HOST CORE DUMPS on {node_ip} "
                    f"(fallback): {core_files}"
                )

            # 2. Try coredumpctl via nsenter (best-effort, won't work on Talos)
            for cmd_name, host_cmd in [
                (
                    "coredumpctl_list",
                    "coredumpctl list --no-pager",
                ),
                (
                    "coredumpctl_info",
                    "coredumpctl info --no-pager",
                ),
            ]:
                try:
                    out, _ = self._exec_kubectl(
                        f"{kexec} nsenter -t 1 -m -u -i -n -- "
                        f"sh -c '{host_cmd} 2>/dev/null' "
                        f"2>/dev/null || true",
                        supress_logs=True,
                        timeout=120,
                    )
                    if out and out.strip():
                        fpath = os.path.join(
                            local_dir, f"{cmd_name}_{label}.txt"
                        )
                        with open(fpath, "w") as f:
                            f.write(out)
                        saved.append(fpath)
                        self.logger.info(
                            f"[coredump] Saved {cmd_name} for {node_ip} "
                            f"(fallback nsenter)"
                        )
                except Exception:
                    pass  # Expected on Talos

            # 3. Copy actual core dump files under size threshold
            for fname in core_files:
                try:
                    size_out, _ = self._exec_kubectl(
                        f"{kexec} stat -c '%s' "
                        f"/host-coredumps/{shlex.quote(fname)} "
                        f"2>/dev/null || echo 0",
                        supress_logs=True,
                    )
                    size_bytes = int(
                        size_out.strip().strip("'") or "0"
                    )
                    size_mb = size_bytes / (1024 * 1024)
                    self.logger.info(
                        f"[coredump] {node_ip}: {fname} = "
                        f"{size_mb:.1f} MB (fallback)"
                    )
                    if max_size_mb > 0 and size_mb > max_size_mb:
                        self.logger.warning(
                            f"[coredump] Skipping copy of {fname} on "
                            f"{node_ip} ({size_mb:.1f} MB > "
                            f"{max_size_mb} MB limit)"
                        )
                        continue
                except Exception:
                    continue

                safe_name = fname.replace(":", "_")
                local_path = os.path.join(
                    local_dir, f"{label}_{safe_name}"
                )
                try:
                    self._exec_kubectl(
                        f"kubectl cp -n {ns} "
                        f"{pod_name}:/host-coredumps/{shlex.quote(fname)} "
                        f"{shlex.quote(local_path)}",
                        supress_logs=True,
                        timeout=600,
                    )
                    if os.path.exists(local_path):
                        self.logger.info(
                            f"[coredump] Copied host core dump {fname} "
                            f"from {node_ip} ({size_mb:.1f} MB) "
                            f"-> {local_path}"
                        )
                        saved.append(local_path)
                except Exception as exc:
                    self.logger.warning(
                        f"[coredump] Failed to copy {fname} from "
                        f"{node_ip} (fallback): {exc}"
                    )
        finally:
            # Always clean up the collector pod
            try:
                self._exec_kubectl(
                    f"kubectl delete pod {pod_name} -n {ns} "
                    f"--force --grace-period=0 2>/dev/null || true",
                    supress_logs=True,
                )
            except Exception:
                pass

        return saved

    def wait_pod_ready(self, pod_name_prefix: str, timeout: int = 300) -> str:
        """
        Wait until a pod whose name starts with *pod_name_prefix* is Running.

        Returns the full pod name.
        """
        self.logger.info(
            f"[K8sUtils] Waiting for pod matching prefix {pod_name_prefix!r} to be Running..."
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                (
                    f"kubectl get pods -n {self.namespace} --no-headers "
                    f"-o custom-columns=:metadata.name,:status.phase "
                    f"| grep '{pod_name_prefix}' | head -1"
                ),
                supress_logs=True,
            )
            parts = out.strip().split()
            if len(parts) == 2 and parts[1] == "Running":
                self.logger.info(f"[K8sUtils] Pod {parts[0]} is Running.")
                return parts[0]
            time.sleep(10)
        raise TimeoutError(
            f"[K8sUtils] Pod with prefix {pod_name_prefix!r} not Running within {timeout}s"
        )

    # ── Generic YAML apply / delete ─────────────────────────────────────────

    def apply_yaml(self, yaml_content: str, namespace: str = None,
                   request_timeout: str = "60s"):
        """Apply a YAML manifest via ``kubectl apply -f -``."""
        ns = namespace or self.namespace
        escaped = yaml_content.replace("'", "'\\''")
        return self._exec_kubectl(
            f"echo '{escaped}' | kubectl apply -n {ns} "
            f"--request-timeout={request_timeout} -f -"
        )

    def apply_yaml_cluster_scoped(self, yaml_content: str):
        """Apply a cluster-scoped YAML manifest (no namespace flag)."""
        escaped = yaml_content.replace("'", "'\\''")
        return self._exec_kubectl(f"echo '{escaped}' | kubectl apply -f -")

    def delete_resource(self, kind: str, name: str, namespace: str = None):
        """Delete a K8s resource by kind and name."""
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl delete {kind} {name} -n {ns} --ignore-not-found --wait=false"
        )

    def get_resource_json(self, kind: str, name: str, namespace: str = None) -> dict:
        """Get a K8s resource as parsed JSON.  Returns ``{}`` if not found."""
        ns = namespace or self.namespace
        out, err = self._exec_kubectl(
            f"kubectl get {kind} {name} -n {ns} -o json 2>/dev/null || true",
            supress_logs=True,
        )
        text = out.strip()
        if not text or "NotFound" in (err or ""):
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {}

    # ── StorageClass & VolumeSnapshotClass (cluster-scoped) ──────────────────

    def create_storage_class(self, name: str, cluster_id: str, pool_name: str,
                             ndcs: int = 1, npcs: int = 1, fs_type: str = "ext4",
                             compression: bool = False, encryption: bool = False,
                             fabric: str = "tcp",
                             max_namespace_per_subsys: int = 1,
                             dhchap_node_label: str = None):
        """Create a simplyblock CSI StorageClass.

        dhchap_node_label: the pool's node label key
            (``simplyblock.io/pool.<ns>.<cluster>.<pool>``). Required for a
            DHCHAP pool: without it the CSI driver provisions the volume with
            no ``nodeAffinity``, so any node mounts it and allowedNodes is not
            enforced at all. With it, the driver writes a matching
            nodeAffinity onto the PV and a non-allowed node fails to mount.
            Deliberately paired with ``volumeBindingMode: Immediate`` below and
            NOT with ``allowedTopologies`` — the operator's own generated class
            uses allowedTopologies, which only resolves once the CSI node
            driver has re-registered and picked the label up as a topology key.
        """
        dhchap_param = (
            f"  dhchap_node_label: {dhchap_node_label}\n"
            if dhchap_node_label else ""
        )
        yaml_content = (
            f"allowVolumeExpansion: true\n"
            f"apiVersion: storage.k8s.io/v1\n"
            f"kind: StorageClass\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"parameters:\n"
            f"  cluster_id: \"{cluster_id}\"\n"
            f"  compression: \"{str(compression)}\"\n"
            f"  csi.storage.k8s.io/fstype: {fs_type}\n"
            f"{dhchap_param}"
            f"  distr_ndcs: \"{ndcs}\"\n"
            f"  distr_npcs: \"{npcs}\"\n"
            f"  encryption: \"{str(encryption)}\"\n"
            f"  fabric: {fabric}\n"
            f"  lvol_priority_class: \"0\"\n"
            f"  max_namespace_per_subsys: \"{max_namespace_per_subsys}\"\n"
            f"  pool_name: {pool_name}\n"
            f"  qos_r_mbytes: \"0\"\n"
            f"  qos_rw_iops: \"0\"\n"
            f"  qos_rw_mbytes: \"0\"\n"
            f"  qos_w_mbytes: \"0\"\n"
            f"  replicate: \"False\"\n"
            f"  tune2fs_reserved_blocks: \"0\"\n"
            f"provisioner: csi.simplyblock.io\n"
            f"reclaimPolicy: Delete\n"
            f"volumeBindingMode: Immediate\n"
        )
        # StorageClass parameters and volumeBindingMode are immutable —
        # delete first to allow recreation with different parameters.
        self.logger.info(f"[K8sUtils] Deleting existing StorageClass '{name}' (if any)")
        self._exec_kubectl(f"kubectl delete storageclass {name} --ignore-not-found")
        self.logger.info(f"[K8sUtils] Creating StorageClass '{name}'")
        self.apply_yaml_cluster_scoped(yaml_content)

    def create_volume_snapshot_class(self, name: str = "simplyblock-csi-snapshotclass"):
        """Create a VolumeSnapshotClass for the simplyblock CSI driver.

        If the class already exists (e.g. created by Helm), it is left as-is.
        """
        out, _ = self._exec_kubectl(
            f"kubectl get volumesnapshotclass {name} --no-headers 2>/dev/null || true",
            supress_logs=True,
        )
        if out.strip():
            self.logger.info(f"[K8sUtils] VolumeSnapshotClass '{name}' already exists, skipping creation")
            return

        yaml_content = (
            f"apiVersion: snapshot.storage.k8s.io/v1\n"
            f"kind: VolumeSnapshotClass\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"driver: csi.simplyblock.io\n"
            f"deletionPolicy: Delete\n"
        )
        self.logger.info(f"[K8sUtils] Creating VolumeSnapshotClass '{name}'")
        self.apply_yaml_cluster_scoped(yaml_content)

    def delete_storage_class(self, name: str):
        """Delete a StorageClass (cluster-scoped)."""
        self._exec_kubectl(f"kubectl delete storageclass {name} --ignore-not-found")

    def delete_volume_snapshot_class(self, name: str):
        """Delete a VolumeSnapshotClass (cluster-scoped)."""
        self._exec_kubectl(
            f"kubectl delete volumesnapshotclass {name} --ignore-not-found"
        )

    # ── PVC operations ───────────────────────────────────────────────────────

    def create_pvc(self, name: str, size: str, storage_class: str,
                   namespace: str = None, node_id: str = None):
        """Create a PersistentVolumeClaim (provisions an lvol via CSI).

        Args:
            node_id: If provided, adds ``simplybk/host-id`` annotation to pin
                     the PVC to a specific storage node.
        """
        ns = namespace or self.namespace
        annotations = ""
        if node_id:
            annotations = (
                f"  annotations:\n"
                f"    simplybk/host-id: {node_id}\n"
            )
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"{annotations}"
            f"spec:\n"
            f"  accessModes:\n"
            f"  - ReadWriteOnce\n"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {size}\n"
            f"  storageClassName: {storage_class}\n"
        )
        self.logger.info(f"[K8sUtils] Creating PVC '{name}' size={size} node={node_id or 'auto'}")
        self.apply_yaml(yaml_content, namespace=ns)

    def create_clone_pvc(self, name: str, size: str, storage_class: str,
                         snapshot_name: str, namespace: str = None):
        """Create a PVC restored from a VolumeSnapshot (clone)."""
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  storageClassName: {storage_class}\n"
            f"  dataSource:\n"
            f"    name: {snapshot_name}\n"
            f"    kind: VolumeSnapshot\n"
            f"    apiGroup: snapshot.storage.k8s.io\n"
            f"  accessModes:\n"
            f"  - ReadWriteOnce\n"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {size}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating clone PVC '{name}' from snapshot '{snapshot_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def resize_pvc(self, name: str, new_size: str, namespace: str = None):
        """Patch a PVC to request a larger size."""
        ns = namespace or self.namespace
        patch = f'{{"spec":{{"resources":{{"requests":{{"storage":"{new_size}"}}}}}}}}'
        self.logger.info(f"[K8sUtils] Resizing PVC '{name}' to {new_size}")
        self._exec_kubectl(
            f"kubectl patch pvc {name} -n {ns} -p '{patch}' --type merge"
        )

    def wait_pvc_bound(self, name: str, timeout: int = 300,
                       namespace: str = None) -> bool:
        """Poll until PVC phase is ``Bound``.  Returns True on success."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pvc {name} -n {ns} -o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True,
            )
            if out.strip() == "Bound":
                self.logger.info(f"[K8sUtils] PVC '{name}' is Bound")
                return True
            self.logger.info(f"[K8sUtils] Waiting for PVC '{name}' to bind (current: {out.strip()!r})…")
            time.sleep(5)
        raise TimeoutError(f"[K8sUtils] PVC '{name}' not Bound within {timeout}s")

    def delete_pvc(self, name: str, namespace: str = None):
        """Delete a PVC."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting PVC '{name}'")
        self.delete_resource("pvc", name, namespace=ns)

    def get_pvc_status(self, name: str, namespace: str = None) -> dict:
        """Return ``{phase, capacity}`` for a PVC."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} -o jsonpath="
            f"'{{.status.phase}} {{.status.capacity.storage}}' 2>/dev/null || true",
            supress_logs=True,
        )
        parts = out.strip().split()
        return {
            "phase": parts[0] if parts else "",
            "capacity": parts[1] if len(parts) > 1 else "",
        }

    def get_pvc_volume_handle(self, name: str, namespace: str = None) -> str:
        """Return the CSI volumeHandle (lvol ID) backing a bound PVC, or ''."""
        ns = namespace or self.namespace
        # Get the PV name from the PVC
        pv, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} "
            f"-o jsonpath='{{.spec.volumeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        pv = pv.strip()
        if not pv:
            return ""
        # Get the volumeHandle from the PV
        handle, _ = self._exec_kubectl(
            f"kubectl get pv {pv} "
            f"-o jsonpath='{{.spec.csi.volumeHandle}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return handle.strip()

    def get_pvc_pv_name(self, name: str, namespace: str = None) -> str:
        """Return the PersistentVolume name backing a bound PVC, or ''."""
        ns = namespace or self.namespace
        pv, _ = self._exec_kubectl(
            f"kubectl get pvc {name} -n {ns} "
            f"-o jsonpath='{{.spec.volumeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return pv.strip()

    def get_pvc_primary_k8s_node(self, pvc_name: str, sbcli_utils,
                                namespace: str = None) -> str | None:
        """Return the K8s node hostname where the primary storage node of a PVC lives.

        Resolves PVC → volumeHandle → lvol → storage node → mgmt_ip → K8s node name.
        Returns None if any step fails.
        """
        try:
            vol_handle = self.get_pvc_volume_handle(pvc_name, namespace=namespace)
            if not vol_handle:
                return None
            lvol_id = vol_handle.split(":")[-1] if ":" in vol_handle else vol_handle
            lvol_details = sbcli_utils.get_lvol_details(lvol_id)
            if not lvol_details:
                return None
            node_id = lvol_details[0].get("node_id")
            if not node_id:
                return None
            node_details = sbcli_utils.get_storage_node_details(node_id)
            if not node_details:
                return None
            node_ip = node_details[0]["mgmt_ip"]
            return self._get_k8s_node_name(node_ip)
        except Exception as exc:
            self.logger.warning(
                f"[K8sUtils] Failed to resolve primary k8s node for PVC {pvc_name}: {exc}"
            )
            return None

    def log_fio_pvc_mapping(self, pvc_details: dict, clone_details: dict = None,
                            extra_details: dict = None,
                            snapshot_details: dict = None):
        """Log a table mapping FIO Job → PVC → lvol ID for debugging.

        Parameters
        ----------
        pvc_details : dict
            ``{pvc_name: {"job_name": ..., "node_id": ..., "storage_class": ..., ...}}``
        clone_details : dict | None
            Same structure for clone PVCs, with optional ``snap_name`` key.
        extra_details : dict | None
            Any additional PVC sets (e.g. new-node PVCs).
        snapshot_details : dict | None
            ``{snap_name: {"pvc_name": parent_pvc}}`` for parent PVC lookup.
        """
        all_entries = []
        for label, details in [("pvc", pvc_details),
                                ("clone", clone_details),
                                ("extra", extra_details)]:
            if not details:
                continue
            for name, info in details.items():
                job = info.get("job_name") or "N/A"
                vol_handle = self.get_pvc_volume_handle(name)
                storage_node = info.get("node_id", "N/A") or "N/A"
                sc = info.get("storage_class", "N/A") or "N/A"
                snap = info.get("snap_name", "") or ""
                parent_pvc = ""
                if snap and snapshot_details:
                    parent_pvc = snapshot_details.get(snap, {}).get("pvc_name", "")

                # Resolve FIO pod's K8s node
                fio_node = "N/A"
                if job and job != "N/A":
                    try:
                        pod = self.get_job_pod_name(job)
                        if pod:
                            fio_node = self.get_pod_node_name(pod) or "N/A"
                    except Exception:
                        pass

                fs_type = info.get("fs_type", "N/A") or "N/A"

                all_entries.append({
                    "type": label,
                    "name": name or "N/A",
                    "job": job,
                    "lvol_id": vol_handle or "N/A",
                    "storage_node": storage_node,
                    "storage_class": sc,
                    "fs_type": fs_type,
                    "snap_name": snap,
                    "parent_pvc": parent_pvc,
                    "fio_k8s_node": fio_node,
                })

        if not all_entries:
            return

        self.logger.info("=" * 190)
        self.logger.info("FIO Job → PVC/Clone → Lvol → Worker Mapping")
        self.logger.info("-" * 190)
        self.logger.info(
            f"{'FIO Job':<30} {'PVC/Clone':<25} {'Lvol ID':<40} "
            f"{'Storage Node':<40} {'FIO K8s Node':<20} {'SC':<28} "
            f"{'FS':<6} {'Snapshot':<20} {'Parent PVC':<25} {'Type':<6}"
        )
        self.logger.info("-" * 190)
        for e in all_entries:
            self.logger.info(
                f"{e['job']:<30} {e['name']:<25} {e['lvol_id']:<40} "
                f"{e['storage_node']:<40} {e['fio_k8s_node']:<20} {e['storage_class']:<28} "
                f"{e['fs_type']:<6} {e['snap_name']:<20} {e['parent_pvc']:<25} {e['type']:<6}"
            )
        self.logger.info("=" * 190)
        return all_entries

    # ── VolumeSnapshot operations ────────────────────────────────────────────

    def create_volume_snapshot(self, name: str, pvc_name: str,
                               snapshot_class: str = "simplyblock-csi-snapshotclass",
                               namespace: str = None):
        """Create a VolumeSnapshot from a PVC.

        If a stale VolumeSnapshot with the same name already exists (e.g.
        from a previous test run that did not clean up), it is deleted
        first to avoid ``persistentVolumeClaimName is immutable`` errors
        from ``kubectl apply``.
        """
        ns = namespace or self.namespace
        # Remove any stale VolumeSnapshot with the same name to avoid
        # immutable-field collisions from a prior test.
        existing = self.get_resource_json("volumesnapshot", name, namespace=ns)
        if existing:
            existing_pvc = (existing.get("spec", {})
                           .get("source", {})
                           .get("persistentVolumeClaimName", ""))
            if existing_pvc != pvc_name:
                self.logger.warning(
                    f"[K8sUtils] Stale VolumeSnapshot '{name}' found "
                    f"(source PVC '{existing_pvc}' != '{pvc_name}'), "
                    f"deleting before re-creating"
                )
                self.delete_volume_snapshot(name, namespace=ns, wait=True)
        yaml_content = (
            f"apiVersion: snapshot.storage.k8s.io/v1\n"
            f"kind: VolumeSnapshot\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  volumeSnapshotClassName: {snapshot_class}\n"
            f"  source:\n"
            f"    persistentVolumeClaimName: {pvc_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating VolumeSnapshot '{name}' from PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_volume_snapshot_ready(self, name: str, timeout: int = 300,
                                    namespace: str = None) -> bool:
        """Poll until VolumeSnapshot ``readyToUse`` is true."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get volumesnapshot {name} -n {ns} "
                f"-o jsonpath='{{.status.readyToUse}}' 2>/dev/null || true",
                supress_logs=True,
            )
            if out.strip() == "true":
                self.logger.info(f"[K8sUtils] VolumeSnapshot '{name}' is ready")
                return True
            self.logger.info(
                f"[K8sUtils] Waiting for VolumeSnapshot '{name}' readyToUse "
                f"(current: {out.strip()!r})…"
            )
            time.sleep(5)
        raise TimeoutError(
            f"[K8sUtils] VolumeSnapshot '{name}' not ready within {timeout}s"
        )

    def get_volume_snapshot_handle(self, name: str, namespace: str = None) -> str:
        """Return the backend snapshot UUID for a VolumeSnapshot, or ''.

        Resolves VolumeSnapshot → boundVolumeSnapshotContent →
        VolumeSnapshotContent.status.snapshotHandle.  The CSI snapshotHandle
        may be a composite ``cluster:node:snap_uuid``; the bare UUID (last
        ``:``-separated segment) is returned so it matches ``sbcli snapshot
        list``.
        """
        ns = namespace or self.namespace
        content, _ = self._exec_kubectl(
            f"kubectl get volumesnapshot {name} -n {ns} -o jsonpath="
            f"'{{.status.boundVolumeSnapshotContentName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        content = content.strip()
        if not content:
            return ""
        handle, _ = self._exec_kubectl(
            f"kubectl get volumesnapshotcontent {content} -o jsonpath="
            f"'{{.status.snapshotHandle}}' 2>/dev/null || true",
            supress_logs=True,
        )
        handle = handle.strip()
        if not handle:
            return ""
        return handle.rsplit(":", 1)[-1] if ":" in handle else handle

    def get_volume_snapshot_phase(self, name: str, namespace: str = None) -> str:
        """Return VolumeSnapshot readyToUse status string ('' if absent)."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get volumesnapshot {name} -n {ns} "
            f"-o jsonpath='{{.status.readyToUse}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.strip()

    def delete_volume_snapshot(self, name: str, namespace: str = None,
                               wait: bool = False):
        """Delete a VolumeSnapshot.

        When *wait* is True the call blocks until the VolumeSnapshot is fully
        removed, preventing stale-object collisions in subsequent tests.
        """
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting VolumeSnapshot '{name}'"
                         f"{' (waiting)' if wait else ''}")
        if wait:
            self._exec_kubectl(
                f"kubectl delete volumesnapshot {name} -n {ns} "
                f"--ignore-not-found --wait=true --timeout=120s"
            )
        else:
            self.delete_resource("volumesnapshot", name, namespace=ns)

    def has_client_nodes(self) -> bool:
        """Return True if any K8s node has the 'client' role label."""
        out, _ = self._exec_kubectl(
            "kubectl get nodes -l node-role.kubernetes.io/client "
            "--no-headers 2>/dev/null | wc -l",
            supress_logs=True,
        )
        return int(out.strip() or "0") > 0

    # ── FIO Job operations ───────────────────────────────────────────────────

    def create_fio_job(self, job_name: str, pvc_name: str, configmap_name: str,
                       fio_config: str, namespace: str = None,
                       image: str = "dockerpinata/fio:2.1",
                       cleanup_before_fio: bool = False,
                       avoid_node: str = None,
                       warmup_config: str = None,
                       node_name: str = None):
        """Create a ConfigMap with FIO config and a Job that runs FIO against a PVC.

        Args:
            cleanup_before_fio: If True, add an init container that removes old
                FIO data files from the volume before FIO starts. Useful for
                clone PVCs that inherit files from the source.
            avoid_node: Optional K8s node hostname to avoid scheduling the FIO
                pod on (typically the primary storage node for the lvol).
                When set, a nodeAffinity rule excludes that node so the FIO
                pod runs on a secondary / non-primary node instead.
            warmup_config: Optional FIO config for a sequential write pass.
                When provided, an init container runs FIO with this config
                to pre-fill every block with valid MD5 verify headers (same
                bs, randseed, filenames, size as the main config) before the
                main randrw test.  This ensures a later verify_only pass can
                verify the entire file, not just the blocks randrw touched.
            node_name: hard-pin the job's pod to this node via ``spec.nodeName``,
                bypassing the scheduler (including StorageClass allowedTopologies).
                Mutually exclusive with avoid_node / client-node affinity, which
                are skipped when this is set.
        """
        ns = namespace or self.namespace
        # Indent fio_config for YAML embedding (each line indented by 8 spaces)
        indented_cfg = "\n".join(
            f"      {line}" for line in fio_config.strip().splitlines()
        )
        # Indent warmup config for YAML embedding
        indented_warmup = ""
        if warmup_config:
            indented_warmup = "\n".join(
                f"      {line}" for line in warmup_config.strip().splitlines()
            )
        init_containers_list = []
        if cleanup_before_fio:
            init_containers_list.append(
                "      - name: cleanup-old-fio\n"
                "        image: busybox:1.37\n"
                "        imagePullPolicy: IfNotPresent\n"
                "        command: [\"sh\", \"-c\", \"rm -f /spdkvol/*fio*\"]\n"
                "        volumeMounts:\n"
                "        - mountPath: /spdkvol\n"
                "          name: benchmark-volume\n"
            )
        if warmup_config:
            # FIO warmup init container: sequential write pass to pre-fill
            # all data files with valid verify headers matching the main config.
            init_containers_list.append(
                "      - name: fio-warmup\n"
                f"        image: {image}\n"
                "        imagePullPolicy: IfNotPresent\n"
                "        command: [\"fio\", \"/fio/fio-warmup.cfg\"]\n"
                "        volumeMounts:\n"
                "        - mountPath: /spdkvol\n"
                "          name: benchmark-volume\n"
                "        - mountPath: /fio\n"
                "          name: fio-config\n"
            )
        init_containers = ""
        if init_containers_list:
            init_containers = "      initContainers:\n" + "".join(init_containers_list)
        node_affinity_block = ""
        tolerations_block = ""
        node_name_line = f"      nodeName: {node_name}\n" if node_name else ""
        client_nodes_exist = not node_name and self.has_client_nodes()
        if client_nodes_exist:
            # Hard-pin FIO pods to client-role nodes
            node_affinity_block = (
                "        nodeAffinity:\n"
                "          requiredDuringSchedulingIgnoredDuringExecution:\n"
                "            nodeSelectorTerms:\n"
                "            - matchExpressions:\n"
                "              - key: node-role.kubernetes.io/client\n"
                "                operator: Exists\n"
            )
            # Tolerate the client-node taint so pods can schedule there
            tolerations_block = (
                "      tolerations:\n"
                "      - key: \"node-role\"\n"
                "        operator: \"Equal\"\n"
                "        value: \"client\"\n"
                "        effect: \"NoSchedule\"\n"
            )
            self.logger.info(
                f"[K8sUtils] Client nodes detected — FIO job '{job_name}' "
                f"pinned to client nodes (with toleration)"
            )
        elif not node_name and avoid_node:
            # No client nodes — at least avoid the primary storage node
            node_affinity_block = (
                f"        nodeAffinity:\n"
                f"          preferredDuringSchedulingIgnoredDuringExecution:\n"
                f"          - weight: 100\n"
                f"            preference:\n"
                f"              matchExpressions:\n"
                f"              - key: kubernetes.io/hostname\n"
                f"                operator: NotIn\n"
                f"                values:\n"
                f"                - {avoid_node}\n"
            )
        warmup_cfg_entry = ""
        if warmup_config:
            warmup_cfg_entry = (
                f"  fio-warmup.cfg: |\n"
                f"{indented_warmup}\n"
            )
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: ConfigMap\n"
            f"metadata:\n"
            f"  name: {configmap_name}\n"
            f"  namespace: {ns}\n"
            f"data:\n"
            f"  fio.cfg: |\n"
            f"{indented_cfg}\n"
            f"{warmup_cfg_entry}"
            f"---\n"
            f"apiVersion: batch/v1\n"
            f"kind: Job\n"
            f"metadata:\n"
            f"  name: {job_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  backoffLimit: 0\n"
            f"  template:\n"
            f"    metadata:\n"
            f"      labels:\n"
            f"        app: fio-benchmark\n"
            f"    spec:\n"
            f"{node_name_line}"
            f"      affinity:\n"
            f"        podAntiAffinity:\n"
            f"          preferredDuringSchedulingIgnoredDuringExecution:\n"
            f"          - weight: 100\n"
            f"            podAffinityTerm:\n"
            f"              labelSelector:\n"
            f"                matchLabels:\n"
            f"                  app: fio-benchmark\n"
            f"              topologyKey: kubernetes.io/hostname\n"
            f"{node_affinity_block}"
            f"{init_containers}"
            f"{tolerations_block}"
            f"      containers:\n"
            f"      - name: fio-benchmark\n"
            f"        image: {image}\n"
            f"        imagePullPolicy: IfNotPresent\n"
            f"        command: [\"fio\", \"--eta=always\", \"--status-interval=5\", \"/fio/fio.cfg\"]\n"
            f"        volumeMounts:\n"
            f"        - mountPath: /spdkvol\n"
            f"          name: benchmark-volume\n"
            f"        - mountPath: /fio\n"
            f"          name: fio-config\n"
            f"      volumes:\n"
            f"      - name: benchmark-volume\n"
            f"        persistentVolumeClaim:\n"
            f"          claimName: {pvc_name}\n"
            f"      - name: fio-config\n"
            f"        configMap:\n"
            f"          name: {configmap_name}\n"
            f"      restartPolicy: Never\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating FIO Job '{job_name}' on PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_job_complete(self, job_name: str, timeout: int = 600,
                          namespace: str = None) -> str:
        """Wait for a Job to reach Complete or Failed.

        Returns ``'succeeded'``, ``'failed'``, or ``'timeout'``.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get job {job_name} -n {ns} "
                f"-o jsonpath='{{.status.succeeded}} {{.status.failed}}' "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            parts = out.strip().split()
            succeeded = parts[0] if parts else ""
            failed = parts[1] if len(parts) > 1 else ""
            if succeeded and int(succeeded) >= 1:
                self.logger.info(f"[K8sUtils] Job '{job_name}' succeeded")
                return "succeeded"
            if failed and int(failed) >= 1:
                self.logger.warning(f"[K8sUtils] Job '{job_name}' failed")
                return "failed"
            time.sleep(10)
        self.logger.warning(f"[K8sUtils] Job '{job_name}' timed out after {timeout}s")
        return "timeout"

    def get_job_pod_names(self, job_name: str, namespace: str = None) -> list:
        """Get all pod names created by a Job."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pods -n {ns} --selector=job-name={job_name} "
            f"--no-headers -o custom-columns=:metadata.name",
            supress_logs=True,
        )
        return [p.strip() for p in out.strip().splitlines() if p.strip()]

    def get_job_pod_name(self, job_name: str, namespace: str = None) -> str:
        """Get the first pod name created by a Job."""
        pods = self.get_job_pod_names(job_name, namespace=namespace)
        return pods[0] if pods else ""

    def get_pod_node_name(self, pod_name: str, namespace: str = None) -> str:
        """Return the K8s node hostname where a pod is/was scheduled."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pod {pod_name} -n {ns} "
            f"-o jsonpath='{{.spec.nodeName}}' 2>/dev/null || true",
            supress_logs=True,
        )
        return out.strip()

    def get_pod_status_detail(self, pod_name: str,
                              namespace: str = None) -> dict:
        """Return pod phase and container-level waiting reason.

        Returns a dict with keys:
          - ``phase``: Pod phase (Pending, Running, Succeeded, Failed, Unknown)
          - ``reason``: Human-readable waiting reason if any container is
            stuck (e.g. ``PodInitializing``, ``ContainerCreating``,
            ``CrashLoopBackOff``, ``ErrImagePull``).  Empty string when
            no container is in a waiting state.
          - ``message``: Optional detail message from the waiting state.
        """
        ns = namespace or self.namespace
        # Phase
        phase_out, _ = self._exec_kubectl(
            f"kubectl get pod {pod_name} -n {ns} "
            f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
            supress_logs=True,
        )
        phase = phase_out.strip() or "Unknown"

        # Container waiting reason (init + regular containers)
        # jsonpath: check initContainerStatuses first, then containerStatuses
        reason = ""
        message = ""
        for path in (
            ".status.initContainerStatuses[?(@.state.waiting)].state.waiting",
            ".status.containerStatuses[?(@.state.waiting)].state.waiting",
        ):
            out, _ = self._exec_kubectl(
                f"kubectl get pod {pod_name} -n {ns} "
                f"-o jsonpath='{{range {path}}}{{.reason}}|{{.message}}{{end}}'"
                f" 2>/dev/null || true",
                supress_logs=True,
            )
            parts = out.strip().split("|", 1)
            if parts[0]:
                reason = parts[0]
                message = parts[1] if len(parts) > 1 else ""
                break

        return {"phase": phase, "reason": reason, "message": message}

    def get_pod_events(self, pod_name: str, namespace: str = None) -> str:
        """Return ``<reason>: <message>`` lines for events on a pod.

        Catches things ``get_pod_status_detail`` can't see, like a
        ``FailedMount`` warning event (kubelet's volume manager failing
        ``NodeStageVolume``), which is a Pod event, not a container
        waiting-state reason.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get events -n {ns} "
            f"--field-selector involvedObject.name={pod_name} "
            f"--sort-by=.lastTimestamp "
            f"-o jsonpath='{{range .items[*]}}{{.reason}}: {{.message}}{{\"\\n\"}}{{end}}' "
            f"2>/dev/null || true",
            supress_logs=True,
        )
        return out or ""

    def get_pod_logs(self, pod_name: str, namespace: str = None,
                     tail: int = 200) -> str:
        """Get pod logs (last *tail* lines)."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl logs {pod_name} -n {ns} --tail={tail} 2>/dev/null || true",
            supress_logs=True,
        )
        return out

    def delete_job(self, job_name: str, namespace: str = None):
        """Delete a Job (cascading to its pods)."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting Job '{job_name}'")
        self._exec_kubectl(
            f"kubectl delete job {job_name} -n {ns} "
            f"--ignore-not-found --cascade=foreground"
        )

    def delete_configmap(self, name: str, namespace: str = None):
        """Delete a ConfigMap."""
        ns = namespace or self.namespace
        self.delete_resource("configmap", name, namespace=ns)

    def cleanup_stale_fio_resources(self, namespace: str = None):
        """Remove leftover FIO Jobs, ConfigMaps, PVCs, and VolumeSnapshots
        from any previous test run so tests start clean."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Cleaning stale test resources in namespace {ns}...")
        cmds = [
            # Delete FIO jobs by label
            f"kubectl delete jobs -n {ns} -l app=fio-benchmark --ignore-not-found",
            # Delete FIO configmaps (prefixed fiocfg- or fio-cfg-)
            f"kubectl get configmaps -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(fiocfg-|fio-cfg-)' | xargs -r kubectl delete configmap -n {ns} --ignore-not-found",
            # Delete clone PVCs (prefixed clone-)
            f"kubectl get pvc -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep '^clone-' | xargs -r kubectl delete pvc -n {ns} --ignore-not-found",
            # Delete VolumeSnapshots (prefixed snap- or snapshot-)
            f"kubectl get volumesnapshot -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(snap-|snapshot-)' | xargs -r kubectl delete volumesnapshot -n {ns} --ignore-not-found --wait=true",
            # Delete test PVCs (various prefixes)
            f"kubectl get pvc -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep -E '^(pvc-|mig-pvc-|add-pvc-)' | xargs -r kubectl delete pvc -n {ns} --ignore-not-found",
        ]
        for cmd in cmds:
            try:
                self._exec_kubectl(cmd)
            except Exception as exc:
                self.logger.warning(f"[K8sUtils] Stale resource cleanup step failed: {exc}")
        self.logger.info("[K8sUtils] Stale test resource cleanup done.")

    # ── CRD patch operations (StorageNode / StorageCluster) ────────────────

    def resolve_storage_node_cr_name(self, node_uuid: str,
                                      namespace: str = None) -> str:
        """Resolve a storage node UUID to its StorageNode CR name.

        The operator creates StorageNode CRs with random names
        (e.g. ``simplyblock-node-nklffw``).  This method lists all
        StorageNode CRs and finds the one whose ``status.uuid``
        matches *node_uuid*.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node (from sbcli / API).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        str
            The ``metadata.name`` of the matching StorageNode CR.

        Raises
        ------
        ValueError
            If no StorageNode CR matches the given UUID.
        """
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get storagenodes.storage.simplyblock.io -n {ns} "
            f"-o json 2>/dev/null || true",
            supress_logs=True,
        )
        try:
            data = json.loads(out.strip()) if out and out.strip() else {}
        except Exception:
            data = {}

        for item in data.get("items", []):
            status = item.get("status", {})
            cr_uuid = status.get("uuid", "")
            if cr_uuid == node_uuid:
                cr_name = item["metadata"]["name"]
                self.logger.info(
                    f"[K8sUtils] Resolved node UUID {node_uuid} -> "
                    f"StorageNode CR '{cr_name}'"
                )
                return cr_name

        raise ValueError(
            f"[K8sUtils] No StorageNode CR found with UUID {node_uuid} "
            f"in namespace {ns}"
        )

    def create_storage_node_ops(self, name: str,
                                 storage_node_ref: str,
                                 action: str,
                                 target_worker_node: str = None,
                                 reattach_volume: bool = False,
                                 new_ssd_pcie: list[str] | None = None,
                                 namespace: str = None):
        """Create a StorageNodeOps CR to trigger a node operation.

        Replaces the old pattern of patching StorageNodeSet with
        ``spec.action``.  The operator watches StorageNodeOps CRs
        and executes the requested operation.

        Parameters
        ----------
        name : str
            Name for the StorageNodeOps CR
            (e.g. ``migrate-83c2a579-to-worker-4``).
        storage_node_ref : str
            Name of the StorageNode CR to operate on
            (e.g. ``simplyblock-node-nklffw``).
        action : str
            Operation: ``migrate``, ``restart``, ``suspend``,
            ``resume``, ``remove``, or ``shutdown``.
        target_worker_node : str | None
            Target worker node name (required for ``migrate``).
        reattach_volume : bool
            Whether to reattach volumes after the operation.
        new_ssd_pcie : list[str] | None
            PCIe addresses for new SSDs on the target worker.
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace

        spec_lines = (
            f"  storageNodeRef: {storage_node_ref}\n"
            f"  action: {action}\n"
        )
        if target_worker_node:
            spec_lines += f"  targetWorkerNode: {target_worker_node}\n"
        if reattach_volume:
            spec_lines += "  reattachVolume: true\n"
        if new_ssd_pcie:
            spec_lines += "  newSsdPcie:\n"
            for pcie in new_ssd_pcie:
                spec_lines += f'    - "{pcie}"\n'

        yaml_content = (
            "apiVersion: storage.simplyblock.io/v1alpha1\n"
            "kind: StorageNodeOps\n"
            "metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            "spec:\n"
            f"{spec_lines}"
        )

        self.logger.info(
            f"[K8sUtils] Creating StorageNodeOps '{name}' "
            f"(action={action}, ref={storage_node_ref})"
        )
        return self.apply_yaml(yaml_content, namespace=ns)

    def wait_storage_node_ops_done(self, name: str, timeout: int = 600,
                                    namespace: str = None) -> dict:
        """Poll until StorageNodeOps reaches ``Succeeded`` phase.

        Parameters
        ----------
        name : str
            StorageNodeOps CR name.
        timeout : int
            Maximum wait time in seconds (default 600).
        namespace : str | None
            Override namespace.

        Returns
        -------
        dict
            The StorageNodeOps resource JSON on success.

        Raises
        ------
        AssertionError
            If the operation reaches ``Failed`` phase.
        TimeoutError
            If the operation does not reach ``Succeeded`` within
            *timeout*.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        phase = "unknown"
        while time.time() < deadline:
            res = self.get_resource_json(
                "storagenodeops.storage.simplyblock.io", name, namespace=ns,
            )
            status = res.get("status", {})
            phase = (status.get("phase") or "").strip()
            sub_phase = (status.get("subPhase") or "").strip()

            if phase == "Succeeded":
                self.logger.info(
                    f"[K8sUtils] StorageNodeOps '{name}' Succeeded"
                )
                return res
            if phase == "Failed":
                self._dump_storage_node_ops_diagnostics(name, ns)
                raise AssertionError(
                    f"StorageNodeOps '{name}' failed: "
                    f"{status.get('message', 'no message')}"
                )
            self.logger.info(
                f"[K8sUtils] Waiting for StorageNodeOps '{name}' "
                f"(phase={phase}, subPhase={sub_phase})"
            )
            time.sleep(15)
        self._dump_storage_node_ops_diagnostics(name, ns)
        raise TimeoutError(
            f"StorageNodeOps '{name}' did not reach Succeeded within "
            f"{timeout}s (last phase={phase})"
        )

    def _dump_storage_node_ops_diagnostics(self, ops_name: str,
                                            namespace: str) -> None:
        """Log diagnostic info when a StorageNodeOps fails or times out."""
        self.logger.error(
            f"[nodeops-diag] StorageNodeOps '{ops_name}' did not succeed. "
            f"Dumping diagnostics..."
        )
        try:
            out, _ = self._exec_kubectl(
                f"kubectl describe storagenodeops.storage.simplyblock.io "
                f"{ops_name} -n {namespace}",
                supress_logs=True,
            )
            self.logger.error(f"[nodeops-diag] describe:\n{out}")
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] describe failed: {e}")

        try:
            out, _ = self._exec_kubectl(
                f"kubectl get events -n {namespace} --sort-by=.lastTimestamp "
                f"--field-selector involvedObject.name={ops_name} "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(f"[nodeops-diag] events:\n{out}")
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] events query failed: {e}")

        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} "
                f"-l app=simplyblock-operator --tail=50 "
                f"--all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[nodeops-diag] operator logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[nodeops-diag] operator logs failed: {e}")

    def cleanup_stale_node_ops(self, namespace: str = None):
        """Remove leftover StorageNodeOps CRs from previous test runs."""
        ns = namespace or self.namespace
        self.logger.info(
            f"[K8sUtils] Cleaning stale StorageNodeOps in {ns}..."
        )
        self._exec_kubectl(
            f"kubectl delete storagenodeops.storage.simplyblock.io --all "
            f"-n {ns} --ignore-not-found --wait=false 2>/dev/null || true"
        )

    def patch_storage_node_add_workers(self, new_workers: list,
                                        storage_node_set_ref: str = "simplyblock-node",
                                        namespace: str = None):
        """Add worker nodes by creating StorageNode CRs directly.

        For each worker, a ``StorageNode`` CR is created with
        ``spec.overrides.expand: true``.  The operator detects the
        new CR and handles provisioning automatically — no separate
        ``StorageCluster`` expand patch is needed.

        Device configuration (``driveSizeRange``, ``pcieModel``) is
        read from the parent StorageNodeSet and included in the
        ``overrides`` block so the init container can find the correct
        SSD devices on the new worker.

        Parameters
        ----------
        new_workers : list[str]
            Kubernetes node names to add (e.g. ``["worker-4", "worker-5"]``).
        storage_node_set_ref : str
            Name of the parent StorageNodeSet
            (default ``simplyblock-node``).
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace

        # Read device config from parent StorageNodeSet
        sns_json = self.get_resource_json(
            "storagenodeset.storage.simplyblock.io",
            storage_node_set_ref,
            namespace=ns,
        )
        sns_spec = sns_json.get("spec", {})
        drive_size_range = sns_spec.get("driveSizeRange", "")
        pcie_model = sns_spec.get("pcieModel", "")
        if drive_size_range or pcie_model:
            self.logger.info(
                f"[K8sUtils] Read device config from StorageNodeSet "
                f"'{storage_node_set_ref}': driveSizeRange={drive_size_range!r}, "
                f"pcieModel={pcie_model!r}"
            )

        for worker in new_workers:
            cr_name = f"{storage_node_set_ref}-expand-{worker}"
            overrides = "    expand: true\n"
            if drive_size_range:
                overrides += f'    driveSizeRange: "{drive_size_range}"\n'
            if pcie_model:
                overrides += f'    pcieModel: "{pcie_model}"\n'

            yaml_content = (
                "apiVersion: storage.simplyblock.io/v1alpha1\n"
                "kind: StorageNode\n"
                "metadata:\n"
                f"  name: {cr_name}\n"
                f"  namespace: {ns}\n"
                "spec:\n"
                f"  storageNodeSetRef: {storage_node_set_ref}\n"
                f"  workerNode: {worker}\n"
                "  socketIndex: 0\n"
                "  overrides:\n"
                f"{overrides}"
            )
            self.logger.info(
                f"[K8sUtils] Creating StorageNode CR '{cr_name}' "
                f"for worker '{worker}' (expand=true)"
            )
            self.apply_yaml(yaml_content, namespace=ns)

    def patch_storage_cluster_expand(self, name: str = "simplyblock-cluster",
                                      namespace: str = None):
        """Patch StorageCluster CRD to trigger cluster expansion.

        .. note::
           With the new StorageNode CR model, expansion is triggered
           automatically when a StorageNode CR is created with
           ``overrides.expand: true``.  This method is retained for
           backward compatibility but may no longer be needed.

        Parameters
        ----------
        name : str
            StorageCluster CR name (default ``simplyblock-cluster``).
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace
        cmd = (
            f"kubectl patch storageclusters.storage.simplyblock.io {name} "
            f"-n {ns} --type=merge "
            f"-p '{{\"spec\":{{\"action\":\"expand\"}}}}'"
        )
        self.logger.info(
            f"[K8sUtils] Patching StorageCluster '{name}' to trigger expansion"
        )
        out, err = self._exec_kubectl(cmd)
        return out, err

    def wait_spdk_pods_ready(self, expected_count: int, timeout: int = 600,
                              namespace: str = None) -> int:
        """Wait until at least *expected_count* snode-spdk pods are Running.

        Parameters
        ----------
        expected_count : int
            Minimum number of Running snode-spdk pods to wait for.
        timeout : int
            Maximum seconds to wait (default 600).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        int
            Number of Running pods once threshold is met.

        Raises
        ------
        TimeoutError
            If threshold is not met within *timeout* seconds.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pods -n {ns} -l role=simplyblock-storage-node "
                f"--no-headers 2>/dev/null || true",
                supress_logs=True,
            )
            running = 0
            for line in out.strip().splitlines():
                if "Running" in line:
                    running += 1
            if running >= expected_count:
                self.logger.info(
                    f"[K8sUtils] {running}/{expected_count} snode-spdk pods Running"
                )
                return running
            self.logger.info(
                f"[K8sUtils] Waiting for snode-spdk pods: "
                f"{running}/{expected_count} Running…"
            )
            time.sleep(10)
        raise TimeoutError(
            f"[K8sUtils] Only {running}/{expected_count} snode-spdk pods "
            f"Running after {timeout}s"
        )

    def patch_storage_node_migrate(self, node_uuid: str, target_worker: str,
                                     new_ssd_pcie: list[str] | None = None,
                                     reattach_volume: bool = False,
                                     name: str = "simplyblock-node",
                                     namespace: str = None):
        """Trigger node migration via a StorageNodeOps CR.

        Resolves the StorageNode CR name from the node UUID, then
        creates a ``StorageNodeOps`` CR with ``action=migrate``.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node to migrate.
        target_worker : str
            Kubernetes node name to migrate the storage node onto.
        new_ssd_pcie : list[str] | None
            PCIe addresses for new SSDs on the target worker.
        reattach_volume : bool
            Whether to reattach volumes after migration.
        name : str
            Unused (kept for backward compatibility).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        tuple[str, str]
            ``(ops_name, storage_node_cr)`` — the StorageNodeOps CR
            name and the resolved StorageNode CR name.
        """
        ns = namespace or self.namespace

        storage_node_cr = self.resolve_storage_node_cr_name(
            node_uuid, namespace=ns,
        )

        uuid_prefix = node_uuid[:8] if len(node_uuid) >= 8 else node_uuid
        worker_suffix = target_worker.replace(".", "-").replace("_", "-")
        ts = int(time.time())
        ops_name = f"migrate-{uuid_prefix}-to-{worker_suffix}-{ts}"

        self.logger.info(
            f"[K8sUtils] Migrating storage node {node_uuid} "
            f"(CR={storage_node_cr}) to worker '{target_worker}'"
            f" (newSsdPcie={new_ssd_pcie}, reattachVolume={reattach_volume})"
        )

        self.create_storage_node_ops(
            name=ops_name,
            storage_node_ref=storage_node_cr,
            action="migrate",
            target_worker_node=target_worker,
            reattach_volume=reattach_volume,
            new_ssd_pcie=new_ssd_pcie,
            namespace=ns,
        )

        return ops_name, storage_node_cr

    def patch_storage_node_restart(self, node_uuid: str,
                                    spdk_image: str = None,
                                    spdk_proxy_image: str = None,
                                    name: str = "simplyblock-node",
                                    namespace: str = None):
        """Trigger node restart via a StorageNodeOps CR.

        If *spdk_image* or *spdk_proxy_image* are provided, the
        StorageNodeSet is patched with the new images first (these
        are set-level config fields), then a ``StorageNodeOps`` CR
        with ``action=restart`` is created.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node to restart.
        spdk_image : str | None
            New SPDK container image (e.g. ``registry/spdk:tag``).
        spdk_proxy_image : str | None
            New SPDK proxy container image.
        name : str
            StorageNodeSet CR name for image patches
            (default ``simplyblock-node``).
        namespace : str | None
            Override namespace (default ``self.namespace``).

        Returns
        -------
        tuple[str, str]
            ``(ops_name, storage_node_cr)`` — the StorageNodeOps CR
            name and the resolved StorageNode CR name.
        """
        ns = namespace or self.namespace

        # If new images are provided, patch the StorageNodeSet first
        # (spdkImage/spdkProxyImage are set-level fields, not per-op)
        if spdk_image or spdk_proxy_image:
            patch_dict: dict = {"spec": {}}
            if spdk_image:
                patch_dict["spec"]["spdkImage"] = spdk_image
            if spdk_proxy_image:
                patch_dict["spec"]["spdkProxyImage"] = spdk_proxy_image

            patch_json = json.dumps(patch_dict)
            cmd = (
                f"kubectl patch storagenodesets.storage.simplyblock.io {name} "
                f"-n {ns} --type=merge -p '{patch_json}'"
            )
            self.logger.info(
                f"[K8sUtils] Patching StorageNodeSet '{name}' with new images"
                + (f" spdkImage={spdk_image}" if spdk_image else "")
                + (f" spdkProxyImage={spdk_proxy_image}" if spdk_proxy_image else "")
            )
            self._exec_kubectl(cmd)

        storage_node_cr = self.resolve_storage_node_cr_name(
            node_uuid, namespace=ns,
        )

        uuid_prefix = node_uuid[:8] if len(node_uuid) >= 8 else node_uuid
        ts = int(time.time())
        ops_name = f"restart-{uuid_prefix}-{ts}"

        self.logger.info(
            f"[K8sUtils] Restarting storage node {node_uuid} "
            f"(CR={storage_node_cr})"
        )

        self.create_storage_node_ops(
            name=ops_name,
            storage_node_ref=storage_node_cr,
            action="restart",
            namespace=ns,
        )

        return ops_name, storage_node_cr

    def validate_fio_job(self, job_name: str, namespace: str = None,
                         timeout: int = 600) -> bool:
        """Check Job succeeded and ALL pod logs have no FIO error keywords.

        Checks every pod created by the Job (not just the latest) so that
        failures from earlier attempts that were retried via backoffLimit
        are not silently masked.

        Returns True if valid.  Raises RuntimeError on failure.
        """
        ns = namespace or self.namespace

        # Quick pre-check: fail fast on image pull errors (unrecoverable).
        # PodInitializing and ContainerCreating are normal transient states
        # (e.g. init container running fio-warmup) — let them proceed.
        pod_name_pre = self.get_job_pod_name(job_name, namespace=ns)
        if pod_name_pre:
            detail = self.get_pod_status_detail(pod_name_pre, namespace=ns)
            reason = detail.get("reason", "")
            if reason in ("ErrImagePull", "ImagePullBackOff"):
                raise RuntimeError(
                    f"FIO Job '{job_name}' pod '{pod_name_pre}' never "
                    f"started: {reason} — {detail.get('message', '')}"
                )

        status = self.wait_job_complete(job_name, namespace=ns, timeout=timeout)
        if status != "succeeded":
            # Include pod status detail in the error for diagnostics
            diag = ""
            pod_name_diag = self.get_job_pod_name(job_name, namespace=ns)
            if pod_name_diag:
                detail = self.get_pod_status_detail(
                    pod_name_diag, namespace=ns
                )
                diag = (
                    f" (pod phase={detail.get('phase')}, "
                    f"reason={detail.get('reason')}, "
                    f"msg={detail.get('message', '')!r})"
                )
            raise RuntimeError(
                f"FIO Job '{job_name}' did not succeed "
                f"(status={status}){diag}"
            )
        pod_names = self.get_job_pod_names(job_name, namespace=ns)
        if not pod_names:
            self.logger.warning(
                f"[K8sUtils] Could not find pod for Job '{job_name}'; skipping log check"
            )
            return True
        for pod_name in pod_names:
            logs = self.get_pod_logs(pod_name, namespace=ns, tail=500)
            if not logs:
                continue
            logs_lower = logs.lower()
            # Check for FIO numeric error codes (e.g. err=110, err=5)
            err_match = re.search(r'\berr=([1-9]\d*)\b', logs)
            if err_match:
                raise RuntimeError(
                    f"FIO Job '{job_name}' pod '{pod_name}' reported "
                    f"err={err_match.group(1)}"
                )
            fail_words = ["error", "fail", "interrupt", "terminate"]
            for word in fail_words:
                if word in logs_lower:
                    raise RuntimeError(
                        f"FIO Job '{job_name}' pod '{pod_name}' logs "
                        f"contain '{word}'"
                    )
        return True

    # ── StorageBackup CRD operations ─────────────────────────────────────────

    def create_storage_backup(self, name: str, pvc_name: str,
                              cluster_name: str = "simplyblock-cluster",
                              namespace: str = None):
        """Create a StorageBackup CRD that triggers an S3 backup from a PVC."""
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: StorageBackup\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  clusterName: {cluster_name}\n"
            f"  pvcRef:\n"
            f"    name: {pvc_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating StorageBackup '{name}' for PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_storage_backup_done(self, name: str, timeout: int = 300,
                                  namespace: str = None) -> dict:
        """Poll until StorageBackup phase is ``Done``.  Returns resource JSON."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get_resource_json("storagebackup", name, namespace=ns)
            status = res.get("status", {})
            phase = (status.get("phase") or "").lower()
            if phase == "done":
                self.logger.info(f"[K8sUtils] StorageBackup '{name}' is Done")
                return res
            if phase == "failed":
                self._dump_backup_diagnostics(name, ns)
                raise AssertionError(
                    f"StorageBackup '{name}' failed: {status}")
            self.logger.info(
                f"[K8sUtils] Waiting for StorageBackup '{name}' "
                f"(phase={status.get('phase', 'unknown')})"
            )
            time.sleep(10)
        self._dump_backup_diagnostics(name, ns)
        raise TimeoutError(
            f"StorageBackup '{name}' not Done within {timeout}s"
        )

    def _dump_backup_diagnostics(self, backup_name: str,
                                  namespace: str) -> None:
        """Log diagnostic info when a StorageBackup times out or fails."""
        self.logger.error(
            f"[backup-diag] StorageBackup '{backup_name}' did not reach Done. "
            f"Dumping diagnostics..."
        )
        # 1. kubectl describe the StorageBackup CRD
        try:
            out, _ = self._exec_kubectl(
                f"kubectl describe storagebackup {backup_name} -n {namespace}",
                supress_logs=True,
            )
            self.logger.error(f"[backup-diag] describe storagebackup:\n{out}")
        except Exception as e:
            self.logger.warning(f"[backup-diag] describe failed: {e}")

        # 2. Recent events in the namespace related to backup
        try:
            out, _ = self._exec_kubectl(
                f"kubectl get events -n {namespace} --sort-by=.lastTimestamp "
                f"--field-selector involvedObject.name={backup_name} "
                f"2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(f"[backup-diag] events:\n{out}")
            else:
                self.logger.error("[backup-diag] No events found for StorageBackup")
        except Exception as e:
            self.logger.warning(f"[backup-diag] events query failed: {e}")

        # 3. admin-control pod logs (last 50 lines) — the operator that should reconcile
        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} -l app=simplyblock-admin-control "
                f"--tail=50 --all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[backup-diag] admin-control logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[backup-diag] admin-control logs failed: {e}")

        # 4. tasks pod backup runner logs (last 50 lines)
        try:
            out, _ = self._exec_kubectl(
                f"kubectl logs -n {namespace} -l app=simplyblock-tasks "
                f"--tail=50 --all-containers 2>/dev/null || true",
                supress_logs=True,
            )
            if out and out.strip():
                self.logger.error(
                    f"[backup-diag] tasks pod logs (tail 50):\n{out}"
                )
        except Exception as e:
            self.logger.warning(f"[backup-diag] tasks pod logs failed: {e}")

    def get_storage_backup_id(self, name: str,
                               namespace: str = None) -> str:
        """Return the backupId from a StorageBackup's status field."""
        ns = namespace or self.namespace
        res = self.get_resource_json("storagebackup", name, namespace=ns)
        return res.get("status", {}).get("backupId", "")

    def list_storage_backups(self, namespace: str = None) -> list:
        """List all StorageBackup resources.  Returns list of resource dicts."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get storagebackup -n {ns} -o json 2>/dev/null || true",
            supress_logs=True,
        )
        try:
            data = json.loads(out.strip()) if out.strip() else {}
            return data.get("items", [])
        except Exception:
            return []

    def delete_storage_backup(self, name: str, namespace: str = None):
        """Delete a StorageBackup CRD."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting StorageBackup '{name}'")
        self.delete_resource("storagebackup", name, namespace=ns)

    # ── BackupRestore CRD operations ─────────────────────────────────────────

    def create_backup_restore(self, name: str, backup_ref_name: str,
                              pvc_name: str, pvc_size: str,
                              cluster_name: str = "simplyblock-cluster",
                              storage_class: str = None,
                              target_pool: str = None,
                              namespace: str = None):
        """Create a BackupRestore CRD to restore a backup into a new PVC."""
        ns = namespace or self.namespace
        sc_line = ""
        if storage_class:
            sc_line = f"      storageClassName: {storage_class}\n"
        pool_line = ""
        if target_pool:
            pool_line = f"  targetPool: {target_pool}\n"
        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: BackupRestore\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  clusterName: {cluster_name}\n"
            f"{pool_line}"
            f"  backupRef:\n"
            f"    name: {backup_ref_name}\n"
            f"  pvcTemplate:\n"
            f"    metadata:\n"
            f"      name: {pvc_name}\n"
            f"    spec:\n"
            f"      accessModes:\n"
            f"      - ReadWriteOnce\n"
            f"      resources:\n"
            f"        requests:\n"
            f"          storage: {pvc_size}\n"
            f"{sc_line}"
        )
        self.logger.info(
            f"[K8sUtils] Creating BackupRestore '{name}' from backup "
            f"'{backup_ref_name}' -> PVC '{pvc_name}'"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_backup_restore_done(self, name: str, timeout: int = 300,
                                  namespace: str = None) -> dict:
        """Poll until BackupRestore phase is ``Done``.

        Phases: InProgress -> PVCBinding -> Done
        Returns resource JSON.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get_resource_json("backuprestore", name, namespace=ns)
            phase = (res.get("status", {}).get("phase") or "").lower()
            if phase == "done":
                self.logger.info(f"[K8sUtils] BackupRestore '{name}' is Done")
                return res
            if phase == "failed":
                raise AssertionError(
                    f"BackupRestore '{name}' failed: {res.get('status')}")
            self.logger.info(
                f"[K8sUtils] Waiting for BackupRestore '{name}' "
                f"(phase={res.get('status', {}).get('phase', 'unknown')})"
            )
            time.sleep(10)
        raise TimeoutError(
            f"BackupRestore '{name}' not Done within {timeout}s"
        )

    def delete_backup_restore(self, name: str, namespace: str = None):
        """Delete a BackupRestore CRD."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting BackupRestore '{name}'")
        self.delete_resource("backuprestore", name, namespace=ns)

    # ── BackupImport CRD operations ──────────────────────────────────────────

    def create_backup_import(self, name: str,
                              source_cluster_name: str,
                              source_backup_id: str,
                              target_cluster_name: str,
                              namespace: str = None):
        """Create a BackupImport CRD to import a backup from another cluster.

        The operator will create a corresponding StorageBackup on the target
        cluster and handle source-switching automatically.
        """
        ns = namespace or self.namespace
        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: BackupImport\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  sourceClusterName: {source_cluster_name}\n"
            f"  sourceBackupID: {source_backup_id}\n"
            f"  targetClusterName: {target_cluster_name}\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating BackupImport '{name}' "
            f"(source={source_cluster_name}/{source_backup_id} "
            f"-> target={target_cluster_name})"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_backup_import_done(self, name: str, timeout: int = 300,
                                 namespace: str = None) -> dict:
        """Poll until BackupImport phase is ``Done``.  Returns resource JSON.

        The status will contain ``storageBackupRef`` — the name of the
        StorageBackup CRD created on the target cluster.
        """
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            res = self.get_resource_json("backupimport", name, namespace=ns)
            phase = (res.get("status", {}).get("phase") or "").lower()
            if phase == "done":
                self.logger.info(f"[K8sUtils] BackupImport '{name}' is Done")
                return res
            if phase == "failed":
                raise AssertionError(
                    f"BackupImport '{name}' failed: {res.get('status')}")
            self.logger.info(
                f"[K8sUtils] Waiting for BackupImport '{name}' "
                f"(phase={res.get('status', {}).get('phase', 'unknown')})"
            )
            time.sleep(10)
        raise TimeoutError(
            f"BackupImport '{name}' not Done within {timeout}s"
        )

    def get_backup_import_storage_backup_ref(self, name: str,
                                              namespace: str = None) -> str:
        """Return the storageBackupRef from a BackupImport's status."""
        ns = namespace or self.namespace
        res = self.get_resource_json("backupimport", name, namespace=ns)
        return res.get("status", {}).get("storageBackupRef", "")

    def delete_backup_import(self, name: str, namespace: str = None):
        """Delete a BackupImport CRD."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting BackupImport '{name}'")
        self.delete_resource("backupimport", name, namespace=ns)

    # ── BackupPolicy CRD operations ──────────────────────────────────────────

    def create_backup_policy(self, name: str,
                             cluster_name: str = "simplyblock-cluster",
                             max_versions: int = 0, max_age: str = "",
                             schedule: str = "", namespace: str = None):
        """Create a BackupPolicy CRD."""
        ns = namespace or self.namespace
        spec_lines = f"  clusterName: {cluster_name}\n"
        if max_versions:
            spec_lines += f"  maxVersions: {max_versions}\n"
        if max_age:
            spec_lines += f'  maxAge: "{max_age}"\n'
        if schedule:
            spec_lines += f'  schedule: "{schedule}"\n'
        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: BackupPolicy\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"{spec_lines}"
        )
        self.logger.info(f"[K8sUtils] Creating BackupPolicy '{name}'")
        self.apply_yaml(yaml_content, namespace=ns)

    def delete_backup_policy(self, name: str, namespace: str = None):
        """Delete a BackupPolicy CRD."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting BackupPolicy '{name}'")
        self.delete_resource("backuppolicy", name, namespace=ns)

    # ── PVC annotation helpers ───────────────────────────────────────────────

    def annotate_pvc_backup_policy(self, pvc_name: str, policy_name: str,
                                    namespace: str = None):
        """Attach a BackupPolicy to a PVC via annotation."""
        ns = namespace or self.namespace
        self.logger.info(
            f"[K8sUtils] Annotating PVC '{pvc_name}' with "
            f"backup-policy='{policy_name}'"
        )
        self._exec_kubectl(
            f"kubectl annotate pvc {pvc_name} -n {ns} "
            f"simplybk/backup-policy={policy_name} --overwrite"
        )

    def remove_pvc_backup_policy_annotation(self, pvc_name: str,
                                             namespace: str = None):
        """Remove BackupPolicy annotation from a PVC."""
        ns = namespace or self.namespace
        self.logger.info(
            f"[K8sUtils] Removing backup-policy annotation from PVC "
            f"'{pvc_name}'"
        )
        self._exec_kubectl(
            f"kubectl annotate pvc {pvc_name} -n {ns} simplybk/backup-policy-"
        )

    # ── Utility pod operations (checksums) ───────────────────────────────────

    def create_utility_pod(self, pod_name: str, pvc_name: str,
                           mount_path: str = "/spdkvol",
                           namespace: str = None,
                           node_name: str = None):
        """Create an alpine utility pod that mounts a PVC for checksum operations.

        node_name: hard-pin the pod to this node via ``spec.nodeName``, bypassing
            the scheduler entirely (including any StorageClass allowedTopologies
            restriction). Used to deliberately test scheduling a pod onto a node
            outside a DHCHAP pool's allowedNodes. When set, the client-node
            affinity/toleration block below is skipped — nodeName already forces
            exact placement.
        """
        ns = namespace or self.namespace
        node_name_line = f"  nodeName: {node_name}\n" if node_name else ""
        # Build tolerations + nodeAffinity to match FIO job scheduling
        tolerations_block = ""
        node_affinity_block = ""
        if not node_name and self.has_client_nodes():
            node_affinity_block = (
                "    nodeAffinity:\n"
                "      requiredDuringSchedulingIgnoredDuringExecution:\n"
                "        nodeSelectorTerms:\n"
                "        - matchExpressions:\n"
                "          - key: node-role.kubernetes.io/client\n"
                "            operator: Exists\n"
            )
            tolerations_block = (
                "  tolerations:\n"
                "  - key: \"node-role\"\n"
                "    operator: \"Equal\"\n"
                "    value: \"client\"\n"
                "    effect: \"NoSchedule\"\n"
            )
        affinity_block = ""
        if node_affinity_block:
            affinity_block = (
                f"  affinity:\n"
                f"{node_affinity_block}"
            )
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: Pod\n"
            f"metadata:\n"
            f"  name: {pod_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"{node_name_line}"
            f"{affinity_block}"
            f"{tolerations_block}"
            f"  securityContext:\n"
            f"    seLinuxOptions:\n"
            f"      type: spc_t\n"
            f"  containers:\n"
            f"  - name: alpine\n"
            f"    image: alpine:3\n"
            f"    imagePullPolicy: IfNotPresent\n"
            f"    command: [\"sleep\", \"3600\"]\n"
            f"    volumeMounts:\n"
            f"    - mountPath: {mount_path}\n"
            f"      name: data-volume\n"
            f"  volumes:\n"
            f"  - name: data-volume\n"
            f"    persistentVolumeClaim:\n"
            f"      claimName: {pvc_name}\n"
            f"  restartPolicy: Never\n"
        )
        self.logger.info(
            f"[K8sUtils] Creating utility pod '{pod_name}' with PVC "
            f"'{pvc_name}' at {mount_path}"
        )
        self.apply_yaml(yaml_content, namespace=ns)

    def wait_pod_running(self, pod_name: str, timeout: int = 300,
                         namespace: str = None) -> bool:
        """Wait until pod is ``Running``.  Returns True on success."""
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = self._exec_kubectl(
                f"kubectl get pod {pod_name} -n {ns} "
                f"-o jsonpath='{{.status.phase}}' 2>/dev/null || true",
                supress_logs=True,
            )
            phase = out.strip()
            if phase == "Running":
                self.logger.info(f"[K8sUtils] Pod '{pod_name}' is Running")
                return True
            if phase in ("Failed", "Error"):
                raise RuntimeError(
                    f"Pod '{pod_name}' entered {phase} state")
            time.sleep(5)
        raise TimeoutError(
            f"Pod '{pod_name}' not Running within {timeout}s"
        )

    def exec_in_pod(self, pod_name: str, command: str,
                    namespace: str = None) -> tuple:
        """Execute a command inside a running pod.  Returns (stdout, stderr)."""
        ns = namespace or self.namespace
        return self._exec_kubectl(
            f"kubectl exec {pod_name} -n {ns} -- "
            f"sh -c {shlex.quote(command)}"
        )

    def find_files_in_pvc(self, pod_name: str,
                          mount_path: str = "/spdkvol",
                          namespace: str = None) -> list:
        """Find regular files in mount_path inside the pod."""
        out, _ = self.exec_in_pod(
            pod_name, f"find {mount_path} -maxdepth 2 -type f",
            namespace=namespace,
        )
        return [f.strip() for f in out.splitlines() if f.strip()]

    def generate_checksums_in_pvc(self, pod_name: str, files: list,
                                   namespace: str = None) -> dict:
        """Generate md5 checksums for files inside the pod.

        Returns ``{filepath: md5hash}`` dict.
        """
        if not files:
            return {}
        # Batch all files into a single md5sum call for efficiency
        file_list = " ".join(shlex.quote(f) for f in files)
        out, _ = self.exec_in_pod(
            pod_name, f"md5sum {file_list}",
            namespace=namespace,
        )
        checksums = {}
        for line in out.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                checksums[parts[1]] = parts[0]
        return checksums

    def delete_pod(self, pod_name: str, namespace: str = None,
                   wait: bool = False):
        """Delete a pod.

        When *wait* is True the call blocks until the pod is fully removed,
        preventing name-collision races when a new pod with the same name is
        created shortly after deletion.
        """
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting pod '{pod_name}'"
                         f"{' (waiting)' if wait else ''}")
        if wait:
            self._exec_kubectl(
                f"kubectl delete pod {pod_name} -n {ns} "
                f"--ignore-not-found --wait=true --timeout=60s"
            )
        else:
            self.delete_resource("pod", pod_name, namespace=ns)

    def wait_for_per_node_config(self, worker_node: str,
                                  configmap_name: str = "simplyblock-node-per-node-config",
                                  namespace: str = None,
                                  timeout: int = 120):
        """Wait until the per-node-config ConfigMap has an entry for *worker_node*.

        The operator updates this ConfigMap when it reconciles a StorageNode CR.
        The DaemonSet pod's ``node-env-writer`` init container reads the entry
        to set ``MAX_LVOL``, ``MAX_SIZE``, etc.  If the pod starts before the
        entry exists it gets ``MAX_LVOL=0`` and crashes.

        Args:
            worker_node: The K8s node name (e.g. ``worker-4.ocp.simplyblock.ai``).
            configmap_name: Name of the per-node-config ConfigMap.
            namespace: K8s namespace (defaults to ``self.namespace``).
            timeout: Max seconds to wait.
        """
        import time
        ns = namespace or self.namespace
        deadline = time.time() + timeout
        while time.time() < deadline:
            cm = self.get_resource_json("configmap", configmap_name, namespace=ns)
            data = cm.get("data", {})
            if worker_node in data:
                self.logger.info(
                    f"[K8sUtils] per-node-config has entry for '{worker_node}': "
                    f"{data[worker_node][:120]}..."
                )
                return
            self.logger.info(
                f"[K8sUtils] Waiting for per-node-config entry for "
                f"'{worker_node}' (keys: {list(data.keys())})..."
            )
            time.sleep(10)
        self.logger.warning(
            f"[K8sUtils] per-node-config entry for '{worker_node}' not found "
            f"after {timeout}s — proceeding anyway (pod may still fail)"
        )

    def delete_storage_node_pods_on_worker(self, worker_node: str,
                                           namespace: str = None):
        """Delete storage-node DaemonSet pods running on a specific worker.

        Call this after the per-node-config ConfigMap has been updated for the
        worker (use ``wait_for_per_node_config`` first) so the DaemonSet
        recreates the pod with the correct configuration.
        """
        ns = namespace or self.namespace
        cmd = (
            f"kubectl get pods -n {ns} "
            f"--field-selector spec.nodeName={worker_node} "
            f"--no-headers -o custom-columns=NAME:.metadata.name"
        )
        out, _ = self._exec_kubectl(cmd, supress_logs=True)
        deleted = 0
        for line in (out or "").strip().splitlines():
            pod_name = line.strip()
            if not pod_name:
                continue
            if "simplyblock-storage-node-ds" in pod_name:
                self.logger.info(
                    f"[K8sUtils] Deleting stale storage-node pod "
                    f"'{pod_name}' on worker '{worker_node}'"
                )
                self._exec_kubectl(
                    f"kubectl delete pod {pod_name} -n {ns} "
                    f"--force --grace-period=0 --ignore-not-found"
                )
                deleted += 1
        if deleted:
            self.logger.info(
                f"[K8sUtils] Deleted {deleted} stale storage-node pod(s) "
                f"on worker '{worker_node}'"
            )
        else:
            self.logger.info(
                f"[K8sUtils] No stale storage-node pods found on "
                f"worker '{worker_node}'"
            )

    def verify_pvc_mount(self, pvc_name: str, namespace: str = None,
                         timeout: int = 120) -> tuple:
        """Create a temporary pod to verify a PVC is mountable.

        Returns ``(success: bool, message: str)``.
        The temporary utility pod is always cleaned up after verification.
        """
        import random
        import string
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
        pod_name_safe = pvc_name[:40].lower().replace("_", "-")
        pod_name_v = f"mount-verify-{pod_name_safe}-{suffix}"
        ns = namespace or self.namespace
        try:
            self.create_utility_pod(pod_name_v, pvc_name, namespace=ns)
            self.wait_pod_running(pod_name_v, timeout=timeout, namespace=ns)
            out, _ = self.exec_in_pod(
                pod_name_v,
                "df -h /spdkvol && ls -la /spdkvol",
                namespace=ns,
            )
            return True, f"Mount OK: {out.strip()[:200]}"
        except TimeoutError as e:
            return False, f"Mount timeout: {e}"
        except RuntimeError as e:
            return False, f"Pod failed: {e}"
        except Exception as e:
            return False, f"Mount error: {e}"
        finally:
            try:
                self.delete_resource("pod", pod_name_v, namespace=ns)
            except Exception:
                pass


# ── K8s-native sbcli_utils replacement ──────────────────────────────────────


class K8sSbcliUtils:
    """
    Drop-in replacement for SbcliUtils in Kubernetes environments.

    All CLI calls are routed through ``kubectl exec`` into the
    simplyblock-admin-control pod via the provided K8sUtils instance.
    No REST API calls are made.

    Parameters
    ----------
    k8s : K8sUtils
        Connected K8sUtils instance.
    cluster_id : str
        Cluster UUID (used by commands that accept a cluster id).
    sbcli_cmd : str
        The CLI binary name inside the admin pod (default: ``sbcli``).
    """

    def __init__(self, k8s: K8sUtils, cluster_id: str, sbcli_cmd: str = "sbctl"):
        self.k8s = k8s
        self.cluster_id = cluster_id
        self.sbcli_cmd = sbcli_cmd
        self.logger = setup_logger(__name__)

    # ── helpers ───────────────────────────────────────────────────────────────

    def _run(self, cmd: str) -> str:
        """Execute *cmd* in the admin pod and return stripped stdout."""
        out, _ = self.k8s.exec_sbcli(cmd)
        return out.strip()

    def _run_json(self, cmd: str):
        """Execute *cmd* in the admin pod and parse stdout as JSON."""
        raw = self._run(cmd)
        if not raw:
            self.logger.warning(f"[_run_json] Empty output from: {cmd}")
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            self.logger.warning(f"[_run_json] JSON parse error from: {cmd}\n  raw={raw[:200]}\n  error={e}")
            return []

    @staticmethod
    def _cli_output_is_error(stdout: str, stderr: str) -> bool:
        """Return True if a sbcli/kubectl-exec result indicates a failure.

        ``exec_sbcli`` discards the exit code, so failures surface either as a
        non-empty stderr (``command terminated with exit code N`` from kubectl,
        or the sbcli ``Error:`` line) or as an ``Error``/``Traceback`` string
        printed to stdout.  Mirrors how the REST ``SbcliUtils`` raises on a
        non-2xx response.
        """
        blob = f"{stdout or ''}\n{stderr or ''}"
        markers = (
            "command terminated with exit code",
            "Error:",
            "Traceback (most recent call last)",
            "usage:",  # argparse rejected the arguments
        )
        return any(m in blob for m in markers)

    def _raise_if_cli_error(self, stdout: str, stderr: str, context: str = ""):
        """Raise RuntimeError when a CLI result indicates failure."""
        if self._cli_output_is_error(stdout, stderr):
            detail = (stderr or "").strip() or (stdout or "").strip()
            raise RuntimeError(f"sbcli command failed ({context}): {detail}")

    # ── lvol methods ──────────────────────────────────────────────────────────

    def list_lvols(self):
        """Return ``{lvol_name: lvol_id}`` dict."""
        items = self._run_json(f"{self.sbcli_cmd} lvol list --json")
        return {item["Name"]: item["Id"] for item in items}

    def get_lvol_id(self, lvol_name):
        return self.list_lvols().get(lvol_name)

    def lvol_exists(self, lvol_name):
        return bool(self.get_lvol_id(lvol_name))

    def get_lvol_details(self, lvol_id):
        """Return ``[{uuid, lvol_name, node_id, nqn, status, ...}]``."""
        raw = self._run(f"{self.sbcli_cmd} lvol get {lvol_id} --json")
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]

    def get_lvol_connect_str(self, lvol_name):
        """Return list of ``sudo nvme connect ...`` strings for the lvol.

        Injects ``--ctrl-loss-tmo -1`` so NVMe controllers never time out
        during a storage-node outage (matches bare-metal stress-test behaviour).
        """
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            self.logger.info(f"Lvol {lvol_name} does not exist. Exiting")
            return []
        out = self._run(f"{self.sbcli_cmd} lvol connect {lvol_id}")
        lines = [line for line in out.splitlines() if line.strip()]
        result = []
        for line in lines:
            # Replace existing --ctrl-loss-tmo <value> or --ctrl-loss-tmo=<value> with -1
            line = re.sub(r"--ctrl-loss-tmo[=\s]\S+", "--ctrl-loss-tmo -1", line)
            if "--ctrl-loss-tmo" not in line:
                line = line.rstrip() + " --ctrl-loss-tmo -1"
            result.append(line)
        return result

    def add_lvol(self, lvol_name, pool_name, size="256M", distr_ndcs=0, distr_npcs=0,
                 distr_bs=4096, distr_chunk_bs=4096, max_rw_iops=0, max_rw_mbytes=0,
                 max_r_mbytes=0, max_w_mbytes=0, host_id=None, retry=10,
                 crypto=False, fabric="tcp", cluster_id=None,
                 max_namespace_per_subsys=None, namespace=None):
        """Create an lvol via the CLI."""
        if self.lvol_exists(lvol_name):
            self.logger.info(f"LVOL {lvol_name} already exists. Skipping")
            return

        cmd = (
            f"{self.sbcli_cmd} -d lvol add"
            f" {shlex.quote(lvol_name)} {size} {shlex.quote(pool_name)}"
        )
        if host_id:
            cmd += f" --host-id {shlex.quote(host_id)}"
        if distr_ndcs and distr_npcs:
            cmd += f" --data-chunks-per-stripe {distr_ndcs} --parity-chunks-per-stripe {distr_npcs}"
        if fabric:
            cmd += f" --fabric {shlex.quote(fabric)}"
        if crypto:
            cmd += " --encrypt"
        # Namespace packing flags (parity with SbcliUtils REST body):
        #   max_namespace_per_subsys -> --max-namespace-per-subsys <N>
        #   namespace=True           -> --namespaced true
        # Without these the CLI creates an independent subsystem per lvol
        # (distinct NQN) instead of packing namespaces into a shared one.
        if max_namespace_per_subsys is not None:
            cmd += f" --max-namespace-per-subsys {int(max_namespace_per_subsys)}"
        if namespace:
            cmd += " --namespaced true"

        out, err = self.k8s.exec_sbcli(cmd)
        # CLI write ops do not raise on failure the way the REST client does;
        # surface an error so negative tests observe the expected exception.
        self._raise_if_cli_error(out, err, context=f"lvol add {lvol_name}")

    def delete_lvol(self, lvol_name, max_attempt=120, skip_error=False):
        """Delete lvol by name, retrying the delete command periodically
        if the lvol returns to online state (mirrors sbcli_utils behaviour)."""
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            if skip_error:
                self.logger.info(f"Lvol {lvol_name} not found. Continuing without delete.")
                return True
            raise Exception(f"No such Lvol {lvol_name} found!!")

        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol delete {lvol_id}")

        attempt = 0
        while attempt < max_attempt:
            lvols = self.list_lvols()
            if lvol_name not in lvols:
                self.logger.info(f"Lvol {lvol_name} deleted successfully!!")
                return True
            # Every 12 attempts, check status and retry delete if lvol is
            # back to online (e.g. delete failed during outage).
            if attempt > 0 and attempt % 12 == 0:
                try:
                    details = self.get_lvol_details(lvol_id=lvol_id)
                    cur_state = details[0]["status"] if details else "unknown"
                except Exception:
                    cur_state = "unknown"
                if cur_state == "online":
                    self.logger.info(f"Lvol {lvol_name} in online state. Retrying delete!")
                    self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol delete {lvol_id}")
            attempt += 1
            self.logger.info(f"Lvol {lvol_name} deletion in progress... ({attempt})")
            sleep_n_sec(5)

        if skip_error:
            return False
        raise Exception(f"Lvol {lvol_name} is not getting deleted!!")

    def delete_all_clones(self):
        """Delete all clone lvols (lvols with cloned_from_snap set).

        Must be called BEFORE delete_all_snapshots, because SPDK refuses
        to delete a snapshot that still has clones.
        """
        lvols = self.list_lvols()
        for name, lvol_id in lvols.items():
            details = self.get_lvol_details(lvol_id)
            if details and details[0].get("cloned_from_snap"):
                self.logger.info(f"Deleting clone lvol: {name}")
                try:
                    self.delete_lvol(lvol_name=name, skip_error=True)
                except Exception as e:
                    self.logger.warning(
                        f"Clone delete failed (continuing): {name}, err={e}"
                    )

    def delete_all_lvols(self):
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            self.logger.info(f"Deleting lvol: {name}")
            self.delete_lvol(lvol_name=name)

    def resize_lvol(self, lvol_id, new_size):
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol resize {lvol_id} {new_size}")
        self._raise_if_cli_error(out, err, context=f"lvol resize {lvol_id}")

    # ── storage node methods ──────────────────────────────────────────────────

    def get_storage_nodes(self):
        """Return ``{'results': [{uuid, mgmt_ip, status, is_secondary_node, ...}]}``."""
        items = self._run_json(f"{self.sbcli_cmd} sn list --json")
        results = []
        for item in items:
            uuid = item["UUID"]
            detail_raw = self._run(f"{self.sbcli_cmd} sn get {uuid}")
            detail = json.loads(detail_raw)
            results.append(detail)
        return {"results": results}

    def get_storage_node_details(self, storage_node_id):
        """Return ``[{uuid, mgmt_ip, status, ...}]``."""
        raw = self._run(f"{self.sbcli_cmd} sn get {storage_node_id}")
        data = json.loads(raw)
        return data if isinstance(data, list) else [data]

    def get_management_nodes(self):
        """Return ``{'results': [{'mgmt_ip': ip, ...}]}`` via sbctl cp list."""
        items = self._run_json(f"{self.sbcli_cmd} cp list --json")
        results = []
        for item in items:
            results.append({
                "mgmt_ip": item.get("IP", ""),
                "uuid": item.get("UUID", ""),
                "hostname": item.get("Hostname", ""),
                "status": item.get("Status", ""),
            })
        return {"results": results}

    def get_all_nodes_ip(self):
        """Return ``(mgmt_node_ips, storage_node_ips)`` as lists of strings."""
        mgmt_data = self.get_management_nodes()
        mgmt_ips = [n["mgmt_ip"] for n in mgmt_data["results"]]

        sn_data = self.get_storage_nodes()
        sn_ips = [n["mgmt_ip"] for n in sn_data["results"]]

        return mgmt_ips, sn_ips

    def shutdown_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn shutdown {node_uuid}{force_flag}")

    def suspend_node(self, node_uuid, expected_error_code=None):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn suspend {node_uuid}")

    def restart_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn restart {node_uuid}{force_flag}")

    def wait_for_storage_node_status(self, node_id, status, timeout=60):
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        while timeout > 0:
            try:
                node_details = self.get_storage_node_details(node_id)
                actual_status = node_details[0]["status"]
                if actual_status in status_list:
                    return node_details[0]
                self.logger.info(
                    f"Expected Status: {status_list} / Actual Status: {actual_status}"
                )
            except (json.JSONDecodeError, IndexError, KeyError) as exc:
                # Transient failure — admin-control pod may be recycling.
                self.logger.warning(
                    f"[wait_for_storage_node_status] Transient error for "
                    f"{node_id}: {exc!r}, retrying..."
                )
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(
            f"Timed out waiting for node status, {node_id}, "
            f"Expected: {status_list}, Actual: {actual_status}"
        )

    def is_secondary_node(self, node_id):
        try:
            details = self.get_storage_node_details(node_id)
            return bool(details[0].get("is_secondary_node", False))
        except Exception:
            return False

    def get_node_without_lvols(self):
        """Return a single primary node UUID that has no lvols, or empty string."""
        nodes_with_lvols = self._nodes_with_lvols()
        for result in self.get_storage_nodes()["results"]:
            if not result.get("is_secondary_node") and result["uuid"] not in nodes_with_lvols:
                return result["uuid"]
        return ""

    def get_all_node_without_lvols(self):
        """Return all primary node UUIDs that have no lvols."""
        nodes_with_lvols = self._nodes_with_lvols()
        return [
            r["uuid"]
            for r in self.get_storage_nodes()["results"]
            if not r.get("is_secondary_node") and r["uuid"] not in nodes_with_lvols
        ]

    def _nodes_with_lvols(self):
        """Return set of node UUIDs that have at least one lvol."""
        nodes = set()
        for lvol_id in self.list_lvols().values():
            try:
                details = self.get_lvol_details(lvol_id)
                nodes.add(details[0].get("node_id"))
            except Exception:
                pass
        return nodes

    # ── pool methods ──────────────────────────────────────────────────────────

    def list_storage_pools(self):
        """Return ``{pool_name: pool_id}`` dict."""
        items = self._run_json(f"{self.sbcli_cmd} pool list --json")
        return {item["Name"]: item["UUID"] for item in items}

    def get_storage_pool_id(self, pool_name):
        return self.list_storage_pools().get(pool_name)

    def add_storage_pool(self, pool_name, cluster_id=None, max_rw_iops=0, max_rw_mbytes=0,
                         max_r_mbytes=0, max_w_mbytes=0, dhchap=False, allowed_nodes=None):
        """Use an existing pool if any exist; only create via kubectl if none exist.

        Returns the actual pool name to use (may differ from *pool_name* if an
        existing pool with a different name was found).

        The operator creates pools from StoragePool CRDs. The pool name in the
        backend (visible via ``sbcli pool list``) is set by the operator and
        may differ from the CRD metadata.name.  This method waits for the
        operator to reconcile the pool so that the real pool name can be
        returned.
        """
        # 1. Check if sbcli already sees a pool
        existing = self.list_storage_pools()
        self.logger.info(f"[pool] existing pools (sbcli): {list(existing.keys())}")
        ns = self.k8s.namespace
        if existing and not dhchap and not allowed_nodes:
            actual = next(iter(existing))
            self.logger.info(f"[pool] Using existing pool '{actual}'")
            return actual

        dedicated = bool(dhchap or allowed_nodes)
        if existing and dedicated:
            # A caller with a specific dhchap/allowedNodes requirement must
            # NOT get an arbitrary existing pool handed back — reuse is only
            # safe here if that pool's own StoragePool CRD already has the
            # exact same dhchap + allowedNodes config. Other, non-DHCHAP
            # tests share pools freely via the blind-reuse path above; this
            # only narrows behavior for callers that actually asked for
            # security enforcement.
            wanted_nodes = sorted(allowed_nodes or [])
            crd_json, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} -o json 2>/dev/null || true"
            )
            try:
                crds = json.loads(crd_json).get("items", []) if crd_json.strip() else []
            except (json.JSONDecodeError, AttributeError):
                crds = []
            matched = False
            for crd in crds:
                spec = crd.get("spec", {})
                if (bool(spec.get("dhchap")) == bool(dhchap)
                        and sorted(spec.get("allowedNodes", []) or []) == wanted_nodes):
                    actual = next(iter(existing))
                    self.logger.info(
                        f"[pool] Existing CRD '{crd['metadata']['name']}' "
                        f"already matches requested dhchap={dhchap} "
                        f"allowedNodes={wanted_nodes} — reusing pool "
                        f"'{actual}'")
                    return actual
            if not matched:
                # HACK: suffix the pool name with a timestamp so it cannot
                # collide with (or be shadowed by) the pool other tests are
                # sharing. This exists only because the blind-reuse path
                # above is load-bearing for every non-DHCHAP caller — they
                # rely on being handed whatever pool already exists, so we
                # cannot simply delete leftovers. A cleaner design would be
                # one pool per test with no cross-test sharing at all; that
                # is a bigger change than this fix.
                pool_name = f"{pool_name}-{int(time.time() * 1000) % 1000000}"
                self.logger.info(
                    f"[pool] No existing pool matches dhchap={dhchap} "
                    f"allowedNodes={wanted_nodes} — creating dedicated pool "
                    f"'{pool_name}'")

        k8s_resource_name = f"simplyblock-{pool_name.lower().replace('_', '-')}"

        # 2. Check whether the CRD this call actually wants already exists.
        #    For a dedicated (dhchap/allowedNodes) request this MUST be
        #    scoped to our own resource name — checking "does any StoragePool
        #    CRD exist in the namespace" would find an unrelated leftover
        #    pool's CRD and skip creating ours entirely, then step 4 below
        #    would hand back whichever pool the operator lists first (often
        #    the unrelated one), silently dropping the dhchap/allowedNodes
        #    request. Non-dedicated calls keep the original "any CRD" check.
        if dedicated:
            crd_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepool {k8s_resource_name} -n {ns} "
                f"--no-headers -o custom-columns=NAME:.metadata.name "
                f"2>/dev/null || true"
            )
            existing_crds = [k8s_resource_name] if crd_out.strip() else []
        else:
            out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
            )
            existing_crds = [r.strip() for r in out.strip().splitlines() if r.strip()]

        # 2b. If CRDs exist, check if they're stuck in Terminating.
        #     Previous test runs may have initiated deletion but finalizers
        #     blocked completion.  Wait up to 120s for them to disappear,
        #     then force-remove finalizers if still stuck.
        if existing_crds:
            term_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storagepools -n {ns} --no-headers "
                f"-o custom-columns="
                f"NAME:.metadata.name,DEL:.metadata.deletionTimestamp "
                f"2>/dev/null || true"
            )
            terminating = []
            for line in (term_out or "").strip().splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[1] != "<none>" and parts[0] in existing_crds:
                    terminating.append(parts[0])

            if terminating:
                self.logger.warning(
                    f"[pool] Found Terminating StoragePool CRDs: "
                    f"{terminating} — waiting for deletion to complete"
                )
                deadline = time.time() + 120
                while time.time() < deadline:
                    check_out, _ = self.k8s._exec_kubectl(
                        f"kubectl get storagepools -n {ns} --no-headers "
                        f"-o custom-columns=NAME:.metadata.name "
                        f"2>/dev/null || true"
                    )
                    remaining = [
                        r.strip() for r in
                        (check_out or "").strip().splitlines()
                        if r.strip() and r.strip() in existing_crds
                    ]
                    if not remaining:
                        self.logger.info(
                            "[pool] All Terminating CRDs deleted")
                        break
                    sleep_n_sec(5)
                else:
                    # Force-remove finalizers on stuck CRDs
                    self.logger.warning(
                        "[pool] CRDs still stuck after 120s — "
                        "removing finalizers to unblock deletion"
                    )
                    for crd_name in terminating:
                        try:
                            self.k8s._exec_kubectl(
                                f"kubectl patch storagepool {crd_name} "
                                f"-n {ns} --type merge "
                                f"-p '{{\"metadata\":{{\"finalizers\":[]}}}}'"
                            )
                            self.logger.info(
                                f"[pool] Removed finalizers from "
                                f"{crd_name}")
                        except Exception as e:
                            self.logger.warning(
                                f"[pool] Failed to patch {crd_name}: "
                                f"{e}")
                    sleep_n_sec(10)

                existing_crds = [c for c in existing_crds if c not in terminating]

        if not existing_crds:
            # 3. Create the CRD via kubectl apply.
            cid = cluster_id or self.cluster_id
            cluster_details = self.get_cluster_details(cluster_id=cid)
            # sbcli cluster get returns "cluster_name" (not "name")
            cluster_name = (
                cluster_details.get("cluster_name")
                or cluster_details.get("name")
                or cluster_details.get("Name", cid)
            )

            # Look up the StorageCluster CRD name from K8s to ensure
            # the StoragePool CRD references the correct CRD resource name.
            sc_out, _ = self.k8s._exec_kubectl(
                f"kubectl get storageclusters -n {ns} --no-headers "
                f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
            )
            sc_names = [s.strip() for s in sc_out.strip().splitlines() if s.strip()]
            if sc_names:
                cluster_name = sc_names[0]
                self.logger.info(
                    f"[pool] Using StorageCluster CRD name '{cluster_name}' "
                    f"from K8s (found {len(sc_names)} CRD(s))"
                )
            else:
                self.logger.warning(
                    f"[pool] No StorageCluster CRDs found in namespace {ns}; "
                    f"falling back to cluster_name='{cluster_name}' from sbcli"
                )

            yaml_content = (
                f"apiVersion: storage.simplyblock.io/v1alpha1\n"
                f"kind: StoragePool\n"
                f"metadata:\n"
                f"  name: {k8s_resource_name}\n"
                f"  namespace: {ns}\n"
                f"spec:\n"
                f"  clusterName: {cluster_name}\n"
            )
            if dhchap:
                yaml_content += "  dhchap: true\n"
            if allowed_nodes:
                yaml_content += "  allowedNodes:\n"
                for node_name in allowed_nodes:
                    yaml_content += f"    - {node_name}\n"

            self.logger.info(
                f"[pool] Creating '{pool_name}' "
                f"(CRD={k8s_resource_name}, cluster={cluster_name}) via kubectl apply"
            )
            yaml_escaped = yaml_content.replace("'", "'\\''")
            self.k8s._exec_kubectl(f"echo '{yaml_escaped}' | kubectl apply -f -")
            existing_crds = [k8s_resource_name]
        else:
            self.logger.info(
                f"[pool] Found existing StoragePool CRD(s): {existing_crds} — "
                f"waiting for operator to reconcile"
            )

        # 4. Wait for operator to reconcile the StoragePool CRD into an actual
        #    pool visible via sbcli pool list. For a dedicated request, wait
        #    specifically for a pool name that wasn't already in `existing`
        #    at the start of this call — an unrelated pool that was already
        #    there is never a valid answer here, no matter how the dict
        #    orders. Use 300s timeout to handle slow reconciliation after
        #    pool deletion/recreation cycles.
        already_seen = set(existing.keys())
        for attempt in range(60):  # up to 300s
            pools = self.list_storage_pools()
            if dedicated:
                new_pools = [p for p in pools if p not in already_seen]
                if new_pools:
                    actual = new_pools[0]
                    self.logger.info(
                        f"[pool] Operator reconciled dedicated pool '{actual}' "
                        f"(attempt {attempt})"
                    )
                    return actual
            elif pools:
                actual = next(iter(pools))
                self.logger.info(
                    f"[pool] Operator reconciled pool '{actual}' "
                    f"(attempt {attempt})"
                )
                return actual
            if attempt % 5 == 4:
                self.logger.info(
                    f"[pool] Still waiting for operator to reconcile "
                    f"StoragePool CRD(s) {existing_crds} (attempt {attempt})"
                )
            sleep_n_sec(5)

        # 5. Raise instead of silently returning a non-existent pool name.
        #    Returning the raw pool_name caused StorageClasses to reference
        #    pools that don't exist on the backend, leading to PVC bind failures.
        raise TimeoutError(
            f"[pool] Pool not visible in sbcli after 300s. "
            f"StoragePool CRD(s): {existing_crds}. "
            f"Operator may not have reconciled the pool."
        )

    def add_storage_pool_direct(self, pool_name, cluster_id=None, sbcli_cmd=None):
        """Create a pool directly via ``sbcli pool add`` (kubectl exec).

        Unlike ``add_storage_pool()``, this does NOT create a StoragePool CRD —
        it calls the CLI directly in the admin pod.  Use this for R25
        clusters that have no operator to reconcile StoragePool CRDs.

        sbcli_cmd: override the CLI binary name (e.g. "sbcli-dev" for R25).
                   Defaults to self.sbcli_cmd.

        Returns the pool name on success.
        """
        cli = sbcli_cmd or self.sbcli_cmd

        def _list_pools():
            items = self._run_json(f"{cli} pool list --json")
            return {item["Name"]: item["UUID"] for item in items}

        # 1. Check if sbcli already sees a pool
        existing = _list_pools()
        if existing:
            actual = next(iter(existing))
            self.logger.info(f"[pool] Using existing pool '{actual}'")
            return actual

        # 2. Create via CLI
        cid = cluster_id or self.cluster_id
        cmd = f"{cli} pool add {pool_name} {cid}"
        self.logger.info(f"[pool] Creating pool directly via CLI: {cmd}")
        out = self._run(cmd)
        self.logger.info(f"[pool] pool add output: {out}")

        # 3. Wait for pool to appear in pool list
        for attempt in range(30):  # up to 150s
            pools = _list_pools()
            if pools:
                actual = next(iter(pools))
                self.logger.info(
                    f"[pool] Pool '{actual}' visible after CLI create "
                    f"(attempt {attempt})"
                )
                return actual
            sleep_n_sec(5)

        raise TimeoutError(
            f"[pool] Pool '{pool_name}' not visible in sbcli after 150s "
            f"following direct CLI creation."
        )

    def pool_crd_exists(self, pool_name):
        """Check if a StoragePool CRD exists in K8s (with or without simplyblock- prefix).

        Returns True if the StoragePool CRD is found, False otherwise.
        """
        ns = self.k8s.namespace
        # Try with simplyblock- prefix first (add_storage_pool creates these)
        k8s_name = f"simplyblock-{pool_name.lower().replace('_', '-')}"
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools {k8s_name} -n {ns} "
            f"-o jsonpath='{{.metadata.name}}' 2>/dev/null || true"
        )
        if out.strip():
            return True
        # Try with exact pool_name (ensure_pool_exists creates these)
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools {pool_name} -n {ns} "
            f"-o jsonpath='{{.metadata.name}}' 2>/dev/null || true"
        )
        return bool(out.strip())

    def ensure_pool_exists(self, pool_name, cluster_id=None, encryption=False):
        """Verify a specific pool exists; create it via kubectl if missing.

        Unlike ``add_storage_pool`` (which reuses *any* existing pool), this
        method checks for a pool with exactly *pool_name* and only creates
        it when that specific pool is absent.

        Returns the pool name.
        """
        existing = self.list_storage_pools()
        if pool_name in existing:
            self.logger.info(f"[pool] Pool '{pool_name}' already exists")
            return pool_name

        # Pool does not exist — create it
        cid = cluster_id or self.cluster_id
        cluster_details = self.get_cluster_details(cluster_id=cid)
        # sbcli cluster get returns "cluster_name" (not "name")
        cluster_name = (
            cluster_details.get("cluster_name")
            or cluster_details.get("name")
            or cluster_details.get("Name", cid)
        )

        # Look up the StorageCluster CRD name from K8s to ensure
        # the StoragePool CRD references the correct CRD resource name.
        ns = self.k8s.namespace
        sc_out, _ = self.k8s._exec_kubectl(
            f"kubectl get storageclusters -n {ns} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
        )
        sc_names = [s.strip() for s in sc_out.strip().splitlines() if s.strip()]
        if sc_names:
            cluster_name = sc_names[0]
            self.logger.info(
                f"[pool] Using StorageCluster CRD name '{cluster_name}' "
                f"from K8s (found {len(sc_names)} CRD(s))"
            )
        else:
            self.logger.warning(
                f"[pool] No StorageCluster CRDs found in namespace {ns}; "
                f"falling back to cluster_name='{cluster_name}' from sbcli"
            )
        sc_params = ""
        if encryption:
            sc_params = (
                "  storageClassParameters:\n"
                "    encryption: true\n"
            )

        yaml_content = (
            f"apiVersion: storage.simplyblock.io/v1alpha1\n"
            f"kind: StoragePool\n"
            f"metadata:\n"
            f"  name: {pool_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  clusterName: {cluster_name}\n"
            f"{sc_params}"
        )

        self.logger.info(
            f"[pool] Pool '{pool_name}' not found — creating "
            f"(cluster={cluster_name}, encryption={encryption}) via kubectl apply"
        )
        yaml_escaped = yaml_content.replace("'", "'\\''")
        self.k8s._exec_kubectl(f"echo '{yaml_escaped}' | kubectl apply -f -")

        # Wait up to 180s for the pool to become visible in sbcli
        for _ in range(36):
            pools = self.list_storage_pools()
            if pool_name in pools:
                self.logger.info(f"[pool] Pool '{pool_name}' is ready")
                return pool_name
            sleep_n_sec(5)
        raise RuntimeError(
            f"[pool] Pool '{pool_name}' not confirmed after kubectl apply "
            f"— operator did not reconcile the StoragePool CRD within 180s"
        )

    def add_host_to_pool(self, pool_id, host_nqn):
        """Run ``pool add-host <pool_id> <nqn>`` via kubectl exec.

        Registers a client NQN at pool level so it can connect to any
        DHCHAP-enabled volume in the pool.
        """
        out = self._run(f"{self.sbcli_cmd} pool add-host {pool_id} {host_nqn}")
        self.logger.info(f"[add_host_to_pool] pool={pool_id} nqn={host_nqn}: {out}")
        return out

    def remove_host_from_pool(self, pool_id, host_nqn):
        """Run ``pool remove-host <pool_id> <nqn>`` via kubectl exec."""
        out = self._run(f"{self.sbcli_cmd} pool remove-host {pool_id} {host_nqn}")
        self.logger.info(f"[remove_host_from_pool] pool={pool_id} nqn={host_nqn}: {out}")
        return out

    def disable_storage_pool(self, pool_name):
        """Set a pool's status to Inactive via ``pool disable <pool_id>``."""
        pool_id = self.get_storage_pool_id(pool_name)
        if not pool_id:
            raise RuntimeError(f"Pool {pool_name} not found; cannot disable")
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool disable {pool_id}")
        self._raise_if_cli_error(out, err, context=f"pool disable {pool_name}")
        self.logger.info(f"[pool] Disabled pool '{pool_name}' ({pool_id})")
        return out

    def enable_storage_pool(self, pool_name):
        """Set a pool's status to Active via ``pool enable <pool_id>``."""
        pool_id = self.get_storage_pool_id(pool_name)
        if not pool_id:
            raise RuntimeError(f"Pool {pool_name} not found; cannot enable")
        out, err = self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool enable {pool_id}")
        self._raise_if_cli_error(out, err, context=f"pool enable {pool_name}")
        self.logger.info(f"[pool] Enabled pool '{pool_name}' ({pool_id})")
        return out

    def delete_storage_pool(self, pool_name):
        """Delete a storage pool by removing its K8s CRD resource."""
        self.logger.info(f"[pool] Deleting pool CRD '{pool_name}'")
        ns = self.k8s.namespace
        self.k8s._exec_kubectl(
            f"kubectl delete storagepools {pool_name} -n {ns} "
            f"--timeout=60s 2>/dev/null || true"
        )
        # Wait for pool to disappear from sbcli
        for _ in range(12):
            if not self.list_storage_pools():
                self.logger.info(f"[pool] Pool '{pool_name}' deleted")
                return
            sleep_n_sec(5)
        self.logger.warning(f"[pool] Pool '{pool_name}' may not be fully removed")

    def delete_all_storage_pools(self):
        """Delete all storage pool CRD resources."""
        ns = self.k8s.namespace
        out, _ = self.k8s._exec_kubectl(
            f"kubectl get storagepools -n {ns} --no-headers "
            f"-o custom-columns=NAME:.metadata.name 2>/dev/null || true"
        )
        resources = [r.strip() for r in out.strip().splitlines() if r.strip()]
        for res in resources:
            self.logger.info(f"[pool] Deleting pool CRD '{res}'")
            self.k8s._exec_kubectl(
                f"kubectl delete storagepools {res} -n {ns} "
                f"--timeout=60s 2>/dev/null || true"
            )

    # ── cluster methods ──────────────────────────────────────────────────────

    def get_cluster_details(self, cluster_id=None):
        """Return cluster dict (includes ``status``, ``max_fault_tolerance``, etc.)."""
        cid = cluster_id or self.cluster_id
        raw = self._run(f"{self.sbcli_cmd} cluster get {cid}")
        return json.loads(raw)

    def get_cluster_tasks(self, cluster_id=None):
        """
        Return list of task dicts parsed from the ``cluster list-tasks`` table.

        Each dict contains: id, function_name, node_id, status,
        updated_at (ISO string), date (Unix timestamp int).

        Table columns: Task ID | Target ID | Function | Retry | Status | Result | Updated At
        Updated At format: "HH:MM:SS, DD/MM/YYYY"
        """
        cid = cluster_id or self.cluster_id
        out = self._run(f"{self.sbcli_cmd} cluster list-tasks {cid} --limit 0")
        tasks = []
        for line in out.splitlines():
            line = line.strip()
            # Skip border rows and header
            if not line or line.startswith("+") or "Task ID" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # Expect: ['', task_id, target_id, function, retry, status, result, updated_at, '']
            if len(parts) < 8:
                continue
            task_id = parts[1]
            target_id = parts[2]
            function_name = parts[3]
            status = parts[5]
            updated_at_raw = parts[7]

            # Skip rows that don't look like UUIDs
            if not task_id or len(task_id) != 36 or task_id.count("-") != 4:
                continue

            # Extract node_id from "NodeID:<uuid>" or leave None
            node_id = None
            if target_id.startswith("NodeID:"):
                node_id = target_id[len("NodeID:"):]

            # Parse "HH:MM:SS, DD/MM/YYYY" → ISO string + Unix timestamp
            date_ts = 0
            iso_str = updated_at_raw
            try:
                dt = datetime.strptime(updated_at_raw, "%H:%M:%S, %d/%m/%Y")
                dt = dt.replace(tzinfo=timezone.utc)
                iso_str = dt.isoformat()
                date_ts = int(dt.timestamp())
            except Exception:
                pass

            tasks.append({
                "id": task_id,
                "function_name": function_name,
                "node_id": node_id,
                "status": status,
                "updated_at": iso_str,
                "date": date_ts,
            })
        return tasks

    def get_cluster_logs(self, cluster_id=None):
        """Return list of cluster log dicts (each has ``Message``, etc.)."""
        cid = cluster_id or self.cluster_id
        return self._run_json(f"{self.sbcli_cmd} cluster get-logs {cid} --json --limit 0")

    def get_cluster_status(self, cluster_id=None):
        """Return cluster status dict."""
        details = self.get_cluster_details(cluster_id)
        return details

    def list_migration_tasks(self, cluster_id=None):
        """Return raw task list (same shape as ``get_cluster_tasks``)."""
        cid = cluster_id or self.cluster_id
        tasks = self.get_cluster_tasks(cid)
        return {"results": tasks}

    # ── device / node capacity methods ────────────────────────────────────────

    def get_device_details(self, storage_node_id):
        """Return list of device dicts for a storage node."""
        data = self._run_json(
            f"{self.sbcli_cmd} sn list-devices {storage_node_id} --json"
        )
        self.logger.info(f"Device Details: {data}")
        return data

    def get_device_capacity(self, device_id):
        """Return capacity records for a device.

        ``sbctl sn get-capacity-device`` does not support ``--json``,
        so we parse the table output.
        """
        out = self._run(f"{self.sbcli_cmd} sn get-capacity-device {device_id}")
        records = []
        headers = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+"):
                continue
            parts = [p.strip() for p in line.split("|")]
            parts = [p for p in parts if p]
            if not headers:
                headers = [h.lower().replace(" ", "_") for h in parts]
                continue
            if len(parts) == len(headers):
                records.append(dict(zip(headers, parts)))
        return records

    def get_node_capacity(self, node_id, history=None):
        """Return capacity records for a storage node.

        ``sbctl sn get-capacity`` does not support ``--json``,
        so we parse the table output.
        """
        cmd = f"{self.sbcli_cmd} sn get-capacity {node_id}"
        if history:
            cmd += f" --history {history}"
        out = self._run(cmd)
        records = []
        headers = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+"):
                continue
            parts = [p.strip() for p in line.split("|")]
            parts = [p for p in parts if p]
            if not headers:
                headers = [h.lower().replace(" ", "_") for h in parts]
                continue
            if len(parts) == len(headers):
                records.append(dict(zip(headers, parts)))
        return records

    # ── pool methods ──────────────────────────────────────────────────────────

    def get_pool_by_id(self, pool_id):
        """Return pool dict for the given pool id."""
        data = self._run_json(f"{self.sbcli_cmd} pool get {pool_id} --json")
        return data

    def get_io_stats(self, cluster_id=None, time_duration=None):
        """
        Fetch last 10 minutes of I/O stats and return a single averaged dict so
        that ``validate_io_stats`` can assert read_io + write_io > 0 over the window.

        Keys: date, read_bytes, write_bytes, read_io, write_io.
        """
        _UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4}

        def _parse_bytes(val):
            """Convert human-readable size string (e.g. '108.8 MiB') to bytes."""
            try:
                parts = val.split()
                num = float(parts[0])
                unit = parts[1].lower() if len(parts) > 1 else "b"
                return num * _UNITS.get(unit, 1)
            except Exception:
                return 0.0

        def _parse_int(val):
            try:
                return int(val)
            except Exception:
                return 0

        cid = cluster_id or self.cluster_id
        out = self._run(f"{self.sbcli_cmd} cluster get-io-stats {cid} --history 10m")
        rows = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+") or "Date" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # ['', date, read_speed, read_iops, read_lat, write_speed, write_iops, write_lat, '']
            if len(parts) < 8:
                continue
            rows.append({
                "date": parts[1],
                "read_bytes": _parse_bytes(parts[2]),
                "write_bytes": _parse_bytes(parts[5]),
                "read_io": _parse_int(parts[3]),
                "write_io": _parse_int(parts[6]),
            })

        if not rows:
            return []

        n = len(rows)
        avg = {
            "date": f"avg({rows[0]['date']} … {rows[-1]['date']})",
            "read_bytes": sum(r["read_bytes"] for r in rows) / n,
            "write_bytes": sum(r["write_bytes"] for r in rows) / n,
            "read_io": sum(r["read_io"] for r in rows) / n,
            "write_io": sum(r["write_io"] for r in rows) / n,
        }
        self.logger.info(f"[io_stats] {n} samples averaged: {avg}")
        return [avg]

    def get_cluster_capacity(self):
        """Return list of capacity records (each has ``date``, ``size_used``, etc.)."""
        raw = self._run(f"{self.sbcli_cmd} cluster get-capacity {self.cluster_id} --json")
        return json.loads(raw)

    def wait_for_cluster_status(self, cluster_id=None, status="active", timeout=60):
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        while timeout > 0:
            cluster_details = self.get_cluster_details(cluster_id=cluster_id)
            actual_status = cluster_details.get("status")
            if actual_status in status_list:
                return cluster_details
            self.logger.info(f"Expected Status: {status_list} / Actual Status: {actual_status}")
            sleep_n_sec(1)
            timeout -= 1
        raise TimeoutError(
            f"Timed out waiting for cluster status, {cluster_id or self.cluster_id}, "
            f"Expected: {status_list}, Actual: {actual_status}"
        )

    def all_expected_status(self, value_dict, expected_status):
        value_match = []
        for key, value in value_dict.items():
            self.logger.info(f"Entity: {key}, Expected: {expected_status}, Actual: {value}")
            value_match.append(value in expected_status)
        self.logger.info(f"Value: {value_match}")
        return all(value_match)

    # ── snapshot methods ──────────────────────────────────────────────────────

    def add_snapshot(self, lvol_id: str, snapshot_name: str, retry: int = 10):
        out, err = self.k8s.exec_sbcli(
            f"{self.sbcli_cmd} -d snapshot add {lvol_id} {shlex.quote(snapshot_name)}"
        )
        self._raise_if_cli_error(out, err, context=f"snapshot add {snapshot_name}")
        self.wait_for_snapshot(snapshot_name, present=True, timeout=60)

    def list_snapshots(self):
        """Parse snapshot list table output → ``{snap_name: snap_uuid}``.

        Table columns: | UUID | BDdev UUID | BlobID | Name | Size | BDev | Node ID | LVol ID | ...
        """
        out = self._run(f"{self.sbcli_cmd} snapshot list")
        result = {}
        for line in out.splitlines():
            parts = [p.strip() for p in line.split("|")]
            # parts[0]='' parts[1]=UUID parts[2]=BDdev UUID parts[3]=BlobID parts[4]=Name ...
            if len(parts) > 4:
                uuid_candidate = parts[1]
                name_candidate = parts[4]
                # UUID is a 36-char hyphenated string
                if (
                    len(uuid_candidate) == 36
                    and uuid_candidate.count("-") == 4
                    and name_candidate
                ):
                    result[name_candidate] = uuid_candidate
        return result

    def get_snapshot_id(self, snap_name: str):
        return self.list_snapshots().get(snap_name)

    def wait_for_snapshot(self, snap_name: str, present: bool = True, timeout: int = 60):
        """Poll until snap_name appears (present=True) or disappears (present=False)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            exists = snap_name in self.list_snapshots()
            if exists == present:
                return
            state = "appear" if present else "disappear"
            self.logger.info(f"[wait_for_snapshot] Waiting for '{snap_name}' to {state}...")
            time.sleep(3)
        state = "appear" if present else "disappear"
        raise TimeoutError(f"[wait_for_snapshot] '{snap_name}' did not {state} within {timeout}s")

    def delete_snapshot(self, snap_name: str = None, snap_id: str = None,
                        max_attempt: int = 60, skip_error: bool = False):
        if not snap_id:
            if not snap_name:
                raise ValueError("delete_snapshot requires snap_name or snap_id")
            snap_id = self.get_snapshot_id(snap_name)
        if not snap_id:
            if skip_error:
                self.logger.info(f"Snapshot not found (skip_error=True). snap_name={snap_name}")
                return
            raise Exception(f"Snapshot not found. snap_name={snap_name}")

        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d snapshot delete {snap_id}")

        resolve_name = snap_name or next(
            (k for k, v in self.list_snapshots().items() if v == snap_id), None
        )
        # Wait for it to disappear, retrying the delete command periodically
        attempt = 0
        while attempt < max_attempt:
            cur = self.list_snapshots()
            gone = True
            if resolve_name and resolve_name in cur:
                gone = False
            elif not resolve_name and snap_id in cur.values():
                gone = False
            if gone:
                self.logger.info(f"Snapshot {snap_name or snap_id} deleted successfully!")
                return
            if attempt > 0 and attempt % 12 == 0:
                self.logger.info(f"Snapshot {snap_name or snap_id} still present. Retrying delete!")
                self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d snapshot delete {snap_id}")
            attempt += 1
            sleep_n_sec(5)

        if skip_error:
            self.logger.warning(f"Snapshot {snap_name or snap_id} not deleted after {max_attempt} attempts")
            return
        raise Exception(f"Snapshot did not get deleted in time. snap_name={snap_name}, snap_id={snap_id}")

    def delete_all_snapshots(self):
        for snap_name in list(self.list_snapshots().keys()):
            try:
                self.delete_snapshot(snap_name=snap_name, skip_error=True)
            except Exception as e:
                self.logger.info(f"Snapshot delete failed (continuing): {snap_name}, err={e}")

    def add_clone(self, snapshot_id: str, clone_name: str):
        """Create a clone lvol from snapshot_id and wait for it to appear in lvol list."""
        out, err = self.k8s.exec_sbcli(
            f"{self.sbcli_cmd} -d snapshot clone {snapshot_id} {shlex.quote(clone_name)}"
        )
        self._raise_if_cli_error(out, err, context=f"snapshot clone {clone_name}")
        # Poll until the clone appears in lvol list
        deadline = time.time() + 60
        while time.time() < deadline:
            if self.get_lvol_id(clone_name):
                self.logger.info(f"[add_clone] '{clone_name}' is now listed.")
                return out, err
            self.logger.info(f"[add_clone] Waiting for '{clone_name}' to appear in lvol list...")
            time.sleep(3)
        raise TimeoutError(f"[add_clone] '{clone_name}' did not appear in lvol list within 60s")

    # ── task / balancing methods ──────────────────────────────────────────────

    def get_task_subtasks(self, task_id: str) -> list:
        """
        Return list of subtask dicts for the given master task_id.

        Parses the output of ``cluster get-subtasks <task_id>`` which has
        8 data columns::

            | Task ID | Node ID | Distrib | Function | Retry | Status | Result | Updated At |

        Each dict contains: id, node_id, distrib, function_name, retry, status,
        result, updated_at.
        """
        try:
            out = self._run(f"{self.sbcli_cmd} cluster get-subtasks {task_id}")
        except Exception as e:
            self.logger.warning(f"[get_task_subtasks] Failed to fetch subtasks for {task_id}: {e}")
            return []

        subtasks = []
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("+") or "Task ID" in line:
                continue
            parts = [p.strip() for p in line.split("|")]
            # get-subtasks table layout (8 data columns):
            # ['', task_id, node_id, distrib, function, retry, status, result, updated_at, '']
            if len(parts) < 9:
                continue
            sub_id = parts[1]
            if not sub_id or len(sub_id) != 36 or sub_id.count("-") != 4:
                continue
            subtasks.append({
                "id": sub_id,
                "node_id": parts[2],
                "distrib": parts[3],
                "function_name": parts[4],
                "retry": parts[5],
                "status": parts[6],
                "result": parts[7],
                "updated_at": parts[8] if len(parts) > 8 else "",
            })
        return subtasks

    def _wait_for_balancing_subtasks(self, node_id: str, timeout: int = 600) -> None:
        """
        After a node comes back online, find the latest ``balancing_on_restart``
        master task and poll its subtasks until all are ``done``.

        Polls every 15 s for up to *timeout* seconds (default 10 min).
        Logs a warning (does not raise) if the timeout is reached so the test
        can continue to the health-check step.
        """
        self.logger.info(
            f"[balancing] Waiting for balancing_on_restart subtasks after node {node_id} recovery."
        )
        tasks = self.get_cluster_tasks(self.cluster_id)
        balancing_tasks = [t for t in tasks if "balancing_on" in t.get("function_name", "")]

        if not balancing_tasks:
            self.logger.info("[balancing] No balancing_on_restart tasks found. Skipping subtask check.")
            return

        # Use the most recently updated balancing task
        latest_task = max(balancing_tasks, key=lambda t: t["date"])
        task_id = latest_task["id"]
        self.logger.info(
            f"[balancing] Latest balancing task: {task_id} status={latest_task['status']}"
        )

        if latest_task["status"] == "done":
            self.logger.info(f"[balancing] Task {task_id} is already done.")
            return

        deadline = time.time() + timeout
        while time.time() < deadline:
            subtasks = self.get_task_subtasks(task_id)
            if not subtasks:
                self.logger.info(f"[balancing] No subtasks returned for {task_id} yet. Waiting 15s…")
                time.sleep(15)
                continue

            # Build status breakdown
            status_counts = {}
            for st in subtasks:
                s = st.get("status", "unknown")
                status_counts[s] = status_counts.get(s, 0) + 1
            total = len(subtasks)
            done_count = status_counts.get("done", 0)

            self.logger.info(
                f"[balancing] Task {task_id}: {done_count}/{total} subtasks done. "
                f"status_map: {status_counts}"
            )

            # Log individual non-done subtasks for debugging
            non_done = [st for st in subtasks if st.get("status") != "done"]
            for st in non_done:
                self.logger.info(
                    f"[balancing]   subtask {st['id'][:8]}… "
                    f"distrib={st.get('distrib', '?')} "
                    f"status={st.get('status', '?')} "
                    f"retry={st.get('retry', '?')} "
                    f"node={st.get('node_id', '?')[:8]}…"
                )

            if done_count == total:
                self.logger.info(f"[balancing] All {total} subtasks done for task {task_id}.")
                return

            time.sleep(15)

        self.logger.warning(
            f"[balancing] Timed out after {timeout}s waiting for subtasks of task {task_id}. "
            f"Proceeding to health-check anyway."
        )

    def wait_for_health_status(self, node_id, status, timeout=60, device_id=None):
        """
        K8s equivalent of SbcliUtils.wait_for_health_status.

        Before checking the node's ``health_check`` field this method first
        waits for all ``balancing_on_restart`` subtasks to complete (up to
        10 minutes), then polls the node health flag until it matches *status*.

        The ``device_id`` branch is not supported in K8s mode (no REST API);
        a warning is logged and the method returns None if device_id is given.
        """
        if device_id:
            self.logger.warning(
                "[K8s] wait_for_health_status: device_id branch not supported in K8s mode. "
                "Skipping device health check."
            )
            return None

        # Step 1: wait for balancing_on_restart subtasks to finish
        self._wait_for_balancing_subtasks(node_id, timeout=600)

        # Step 2: poll node health_check flag
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        node_details = None
        while timeout > 0:
            node_details = self.get_storage_node_details(node_id)
            actual_status = node_details[0].get("health_check")
            self.logger.info(
                f"[health_check] node={node_id} expected={status_list} actual={actual_status}"
            )
            if actual_status in status_list:
                return node_details[0]
            sleep_n_sec(1)
            timeout -= 1

        # Mirror sbcli_utils: if waiting for False and node is not offline, assert True
        if node_details and False in status_list and node_details[0].get("status") != "offline":
            assert actual_status is True, "Health Status not True for node not in offline state"
            return node_details[0]

        raise TimeoutError(
            f"Timed out waiting for health_check, node_id={node_id}, "
            f"Expected: {status_list}, Actual: {actual_status}"
        )
