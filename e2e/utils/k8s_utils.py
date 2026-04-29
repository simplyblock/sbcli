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

    def _exec_kubectl(self, cmd: str, supress_logs: bool = False):
        """
        Execute *cmd* either locally via subprocess (when use_local_kubectl=True)
        or via SSH to mgmt_node.  Returns (stdout, stderr) strings.
        """
        if self.use_local_kubectl:
            if not supress_logs:
                self.logger.info(f"[K8sUtils] local: {cmd}")
            result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
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

        # If the admin pod was recreated (e.g. during outage), retry with fresh pod
        if "NotFound" in (stderr or ""):
            self.logger.warning(
                f"[K8sUtils] Admin pod '{admin_pod}' not found, re-resolving..."
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
          3. For each distrib: create JSON config and run rpc_sock.py (same as SSH path).
          4. kubectl cp result files from /tmp inside container → logs_path/<pod_name>/distrib_logs/.
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
            rpc_base = f"{kexec} python spdk/scripts/rpc.py -s {sock}"

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

            # 2. Dump each distrib using rpc_sock.py (matches SSH approach)
            for distrib in distribs:
                try:
                    # Create JSON config inside the container
                    json_cfg = (
                        '{"subsystems":[{"subsystem":"distr","config":'
                        '[{"method":"distr_debug_placement_map_dump",'
                        f'"params":{{"name":"{distrib}"}}'
                        '}]}]}'
                    )
                    stack_file = f"/tmp/stack_{distrib}.json"
                    rpc_log = f"/tmp/rpc_{distrib}.log"

                    # Write JSON config, run rpc_sock.py, capture output
                    self._exec_kubectl(
                        f"{kexec} bash -c "
                        + shlex.quote(
                            f"echo '{json_cfg}' > {stack_file} && "
                            f"python scripts/rpc_sock.py {stack_file} {sock} "
                            f"> {rpc_log} 2>&1 || true"
                        ),
                        supress_logs=True,
                    )

                    # Read the RPC log to see what happened
                    log_out, _ = self._exec_kubectl(
                        f"{kexec} bash -c 'cat {rpc_log} 2>/dev/null || true'",
                        supress_logs=True,
                    )
                    self.logger.info(
                        f"[fetch_distrib_logs_k8s] {distrib} rpc_log: {log_out.strip()[:500]}"
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
                    for fname in ls_out.splitlines():
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

    def apply_yaml(self, yaml_content: str, namespace: str = None):
        """Apply a YAML manifest via ``kubectl apply -f -``."""
        ns = namespace or self.namespace
        escaped = yaml_content.replace("'", "'\\''")
        return self._exec_kubectl(f"echo '{escaped}' | kubectl apply -n {ns} -f -")

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
                             fabric: str = "tcp"):
        """Create a simplyblock CSI StorageClass."""
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
            f"  distr_ndcs: \"{ndcs}\"\n"
            f"  distr_npcs: \"{npcs}\"\n"
            f"  encryption: \"{str(encryption)}\"\n"
            f"  fabric: {fabric}\n"
            f"  lvol_priority_class: \"0\"\n"
            f"  max_namespace_per_subsys: \"1\"\n"
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
                            extra_details: dict = None):
        """Log a table mapping FIO Job → PVC → lvol ID for debugging.

        Parameters
        ----------
        pvc_details : dict
            ``{pvc_name: {"job_name": ..., ...}}``
        clone_details : dict | None
            Same structure for clone PVCs.
        extra_details : dict | None
            Any additional PVC sets (e.g. new-node PVCs).
        """
        all_entries = []
        for label, details in [("pvc", pvc_details),
                                ("clone", clone_details),
                                ("extra", extra_details)]:
            if not details:
                continue
            for name, info in details.items():
                job = info.get("job_name", "N/A")
                vol_handle = self.get_pvc_volume_handle(name)
                all_entries.append((job, name, vol_handle or "N/A", label))

        if not all_entries:
            return

        self.logger.info("=" * 120)
        self.logger.info("FIO Job → PVC → Lvol Mapping")
        self.logger.info("-" * 120)
        self.logger.info(
            f"{'FIO Job':<40} {'PVC':<40} {'Lvol ID (volumeHandle)':<42} {'Type':<8}"
        )
        self.logger.info("-" * 120)
        for job, pvc, handle, typ in all_entries:
            self.logger.info(f"{job:<40} {pvc:<40} {handle:<42} {typ:<8}")
        self.logger.info("=" * 120)

    # ── VolumeSnapshot operations ────────────────────────────────────────────

    def create_volume_snapshot(self, name: str, pvc_name: str,
                               snapshot_class: str = "simplyblock-csi-snapshotclass",
                               namespace: str = None):
        """Create a VolumeSnapshot from a PVC."""
        ns = namespace or self.namespace
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

    def delete_volume_snapshot(self, name: str, namespace: str = None):
        """Delete a VolumeSnapshot."""
        ns = namespace or self.namespace
        self.logger.info(f"[K8sUtils] Deleting VolumeSnapshot '{name}'")
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
                       avoid_node: str = None):
        """Create a ConfigMap with FIO config and a Job that runs FIO against a PVC.

        Args:
            cleanup_before_fio: If True, add an init container that removes old
                FIO data files from the volume before FIO starts. Useful for
                clone PVCs that inherit files from the source.
            avoid_node: Optional K8s node hostname to avoid scheduling the FIO
                pod on (typically the primary storage node for the lvol).
                When set, a nodeAffinity rule excludes that node so the FIO
                pod runs on a secondary / non-primary node instead.
        """
        ns = namespace or self.namespace
        # Indent fio_config for YAML embedding (each line indented by 8 spaces)
        indented_cfg = "\n".join(
            f"      {line}" for line in fio_config.strip().splitlines()
        )
        init_containers = ""
        if cleanup_before_fio:
            init_containers = (
                f"      initContainers:\n"
                f"      - name: cleanup-old-fio\n"
                f"        image: busybox\n"
                f"        command: [\"sh\", \"-c\", \"rm -f /spdkvol/*fio*\"]\n"
                f"        volumeMounts:\n"
                f"        - mountPath: /spdkvol\n"
                f"          name: benchmark-volume\n"
            )
        node_affinity_block = ""
        client_nodes_exist = self.has_client_nodes()
        if client_nodes_exist:
            # Hard-pin FIO pods to client-role nodes
            node_affinity_block = (
                f"        nodeAffinity:\n"
                f"          requiredDuringSchedulingIgnoredDuringExecution:\n"
                f"            nodeSelectorTerms:\n"
                f"            - matchExpressions:\n"
                f"              - key: node-role.kubernetes.io/client\n"
                f"                operator: Exists\n"
            )
            self.logger.info(
                f"[K8sUtils] Client nodes detected — FIO job '{job_name}' "
                f"pinned to client nodes"
            )
        elif avoid_node:
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
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: ConfigMap\n"
            f"metadata:\n"
            f"  name: {configmap_name}\n"
            f"  namespace: {ns}\n"
            f"data:\n"
            f"  fio.cfg: |\n"
            f"{indented_cfg}\n"
            f"---\n"
            f"apiVersion: batch/v1\n"
            f"kind: Job\n"
            f"metadata:\n"
            f"  name: {job_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  backoffLimit: 4\n"
            f"  template:\n"
            f"    metadata:\n"
            f"      labels:\n"
            f"        app: fio-benchmark\n"
            f"    spec:\n"
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

    def get_job_pod_name(self, job_name: str, namespace: str = None) -> str:
        """Get the pod name created by a Job."""
        ns = namespace or self.namespace
        out, _ = self._exec_kubectl(
            f"kubectl get pods -n {ns} --selector=job-name={job_name} "
            f"--no-headers -o custom-columns=:metadata.name | head -1",
            supress_logs=True,
        )
        return out.strip()

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
            # Delete VolumeSnapshots (prefixed snap-)
            f"kubectl get volumesnapshot -n {ns} --no-headers -o custom-columns=NAME:.metadata.name "
            f"2>/dev/null | grep '^snap-' | xargs -r kubectl delete volumesnapshot -n {ns} --ignore-not-found",
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

    def patch_storage_node_add_workers(self, new_workers: list,
                                        name: str = "simplyblock-node",
                                        namespace: str = None):
        """Patch StorageNode CRD to add new worker nodes.

        Uses JSON Patch (RFC 6902) to append each worker name to
        ``spec.workerNodes``.  Workers must be added in counts of at
        least 2.

        Parameters
        ----------
        new_workers : list[str]
            Kubernetes node names to add (e.g. ``["worker-4", "worker-5"]``).
        name : str
            StorageNode CR name (default ``simplyblock-node``).
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace
        patch_ops = ",".join(
            f'{{"op":"add","path":"/spec/workerNodes/-","value":"{w}"}}'
            for w in new_workers
        )
        cmd = (
            f"kubectl patch storagenodes.storage.simplyblock.io {name} "
            f"-n {ns} --type=json -p '[{patch_ops}]'"
        )
        self.logger.info(
            f"[K8sUtils] Patching StorageNode '{name}' to add workers: {new_workers}"
        )
        out, err = self._exec_kubectl(cmd)
        return out, err

    def patch_storage_cluster_expand(self, name: str = "simplyblock-cluster",
                                      namespace: str = None):
        """Patch StorageCluster CRD to trigger cluster expansion.

        Sets ``spec.action`` to ``expand`` which the operator watches
        and acts upon.

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
                                     name: str = "simplyblock-node",
                                     namespace: str = None):
        """Patch StorageNode CRD to migrate a storage node to a different worker.

        Triggers a node restart/migration by setting ``spec.action`` to
        ``restart`` along with the target ``nodeUUID`` and ``workerNode``.

        Parameters
        ----------
        node_uuid : str
            UUID of the storage node to migrate.
        target_worker : str
            Kubernetes node name to migrate the storage node onto.
        name : str
            StorageNode CR name (default ``simplyblock-node``).
        namespace : str | None
            Override namespace (default ``self.namespace``).
        """
        ns = namespace or self.namespace
        patch = (
            f'{{"spec":{{"action":"restart",'
            f'"nodeUUID":"{node_uuid}",'
            f'"workerNode":"{target_worker}"}}}}'
        )
        cmd = (
            f"kubectl patch storagenodes.storage.simplyblock.io {name} "
            f"-n {ns} --type=merge -p '{patch}'"
        )
        self.logger.info(
            f"[K8sUtils] Migrating storage node {node_uuid} to worker '{target_worker}'"
        )
        out, err = self._exec_kubectl(cmd)
        return out, err

    def validate_fio_job(self, job_name: str, namespace: str = None,
                         timeout: int = 600) -> bool:
        """Check Job succeeded and pod logs have no FIO error keywords.

        Returns True if valid.  Raises RuntimeError on failure.
        """
        ns = namespace or self.namespace
        status = self.wait_job_complete(job_name, namespace=ns, timeout=timeout)
        if status != "succeeded":
            raise RuntimeError(
                f"FIO Job '{job_name}' did not succeed (status={status})"
            )
        pod_name = self.get_job_pod_name(job_name, namespace=ns)
        if not pod_name:
            self.logger.warning(
                f"[K8sUtils] Could not find pod for Job '{job_name}'; skipping log check"
            )
            return True
        logs = self.get_pod_logs(pod_name, namespace=ns, tail=500)
        fail_words = ["error", "fail", "interrupt", "terminate"]
        logs_lower = logs.lower()
        for word in fail_words:
            if word in logs_lower:
                raise RuntimeError(
                    f"FIO Job '{job_name}' pod logs contain '{word}'"
                )
        return True


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
        return json.loads(raw)

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
                 crypto=False, key1=None, key2=None, fabric="tcp", cluster_id=None,
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
        if crypto and key1 and key2:
            cmd += f" --encrypt --crypto-key1 {shlex.quote(key1)} --crypto-key2 {shlex.quote(key2)}"

        self.k8s.exec_sbcli(cmd)

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

    def delete_all_lvols(self):
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            self.logger.info(f"Deleting lvol: {name}")
            self.delete_lvol(lvol_name=name)

    def resize_lvol(self, lvol_id, new_size):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d lvol resize {lvol_id} {new_size}")

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
        """Return ``{'results': [{'mgmt_ip': ip, ...}]}`` from MNODES env var."""
        mnodes_env = os.environ.get("MNODES", os.environ.get("K3S_MNODES", ""))
        mgmt_ips = [ip.strip() for ip in mnodes_env.split() if ip.strip()]
        return {"results": [{"mgmt_ip": ip, "uuid": ip} for ip in mgmt_ips]}

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

    def resume_node(self, node_uuid):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn resume {node_uuid}")

    def restart_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} -d sn restart {node_uuid}{force_flag}")

    def wait_for_storage_node_status(self, node_id, status, timeout=60):
        actual_status = None
        status_list = status if isinstance(status, list) else [status]
        while timeout > 0:
            node_details = self.get_storage_node_details(node_id)
            actual_status = node_details[0]["status"]
            if actual_status in status_list:
                return node_details[0]
            self.logger.info(f"Expected Status: {status_list} / Actual Status: {actual_status}")
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
                         max_r_mbytes=0, max_w_mbytes=0):
        """Use an existing pool if any exist; only create via kubectl if none exist.

        Returns the actual pool name to use (may differ from *pool_name* if an
        existing pool with a different name was found).
        """
        existing = self.list_storage_pools()
        self.logger.info(f"[pool] existing pools: {list(existing.keys())}")
        if existing:
            actual = next(iter(existing))
            self.logger.info(f"[pool] Using existing pool '{actual}' (K8s: no new pool created)")
            return actual

        # No pools at all — create one via kubectl apply
        cid = cluster_id or self.cluster_id
        cluster_details = self.get_cluster_details(cluster_id=cid)
        cluster_name = cluster_details.get("name") or cluster_details.get("Name", cid)

        k8s_resource_name = f"simplyblock-{pool_name.lower().replace('_', '-')}"
        ns = self.k8s.namespace

        yaml_content = (
            f"apiVersion: simplyblock.simplyblock.io/v1alpha1\n"
            f"kind: SimplyBlockPool\n"
            f"metadata:\n"
            f"  name: {k8s_resource_name}\n"
            f"  namespace: {ns}\n"
            f"spec:\n"
            f"  capacityLimit: 100Gi\n"
            f"  clusterName: {cluster_name}\n"
            f"  name: {pool_name}\n"
        )

        self.logger.info(
            f"[pool] No pools found — creating '{pool_name}' via kubectl apply "
            f"(cluster={cluster_name}, resource={k8s_resource_name})"
        )
        yaml_escaped = yaml_content.replace("'", "'\\''")
        self.k8s._exec_kubectl(f"echo '{yaml_escaped}' | kubectl apply -f -")

        # Wait up to 90s for the pool to become visible in sbcli
        for _ in range(18):
            pools = self.list_storage_pools()
            if pools:
                actual = next(iter(pools))
                self.logger.info(f"[pool] Pool '{actual}' is ready")
                return actual
            sleep_n_sec(5)
        self.logger.warning("[pool] Pool not confirmed after kubectl apply")
        return pool_name

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

    def delete_storage_pool(self, pool_name):
        self.logger.info(f"[pool] K8s mode: skipping delete of pool '{pool_name}'")

    def delete_all_storage_pools(self):
        self.logger.info("[pool] K8s mode: skipping delete_all_storage_pools")

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
        self.k8s.exec_sbcli(
            f"{self.sbcli_cmd} -d snapshot add {lvol_id} {shlex.quote(snapshot_name)}"
        )
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

        Parses the output of ``cluster get-subtasks <task_id>`` which uses the
        same table format as ``cluster list-tasks``.

        Each dict contains: id, function_name, status.
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
            # ['', sub_id, target_id, function, retry, status, result, updated_at, '']
            if len(parts) < 6:
                continue
            sub_id = parts[1]
            if not sub_id or len(sub_id) != 36 or sub_id.count("-") != 4:
                continue
            subtasks.append({
                "id": sub_id,
                "function_name": parts[3] if len(parts) > 3 else "",
                "status": parts[5] if len(parts) > 5 else "",
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

            done_count = sum(1 for st in subtasks if st["status"] == "done")
            total = len(subtasks)
            self.logger.info(
                f"[balancing] Task {task_id}: {done_count}/{total} subtasks done."
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
