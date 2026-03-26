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

        Returns the same (stdout, stderr) tuple as SshUtils.exec_command.
        """
        if not supress_logs:
            self.logger.info(f"[sbcli] {command}")
        admin_pod = self.get_admin_pod()
        kubectl_cmd = (
            f"kubectl exec -n {self.namespace} {admin_pod} -- "
            f"bash -c {shlex.quote(command)}"
        )
        return self._exec_kubectl(kubectl_cmd, supress_logs=supress_logs)

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
        """Return list of ``sudo nvme connect ...`` strings for the lvol."""
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            self.logger.info(f"Lvol {lvol_name} does not exist. Exiting")
            return []
        out = self._run(f"{self.sbcli_cmd} lvol connect {lvol_id}")
        return [line for line in out.splitlines() if line.strip()]

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
            f"{self.sbcli_cmd} lvol add"
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
        """Delete lvol by name, waiting until it disappears."""
        lvol_id = self.get_lvol_id(lvol_name=lvol_name)
        if not lvol_id:
            if skip_error:
                self.logger.info(f"Lvol {lvol_name} not found. Continuing without delete.")
                return
            raise Exception(f"No such Lvol {lvol_name} found!!")

        self.k8s.exec_sbcli(f"{self.sbcli_cmd} lvol delete {lvol_id}")

        attempt = 0
        while attempt < max_attempt:
            if lvol_name not in self.list_lvols():
                self.logger.info(f"Lvol {lvol_name} deleted successfully!!")
                return
            attempt += 1
            self.logger.info(f"Lvol {lvol_name} deletion in progress... ({attempt})")
            sleep_n_sec(5)

        if skip_error:
            return
        raise Exception(f"Lvol {lvol_name} is not getting deleted!!")

    def delete_all_lvols(self):
        lvols = self.list_lvols()
        for name in list(lvols.keys()):
            self.logger.info(f"Deleting lvol: {name}")
            self.delete_lvol(lvol_name=name)

    def resize_lvol(self, lvol_id, new_size):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} lvol resize {lvol_id} {new_size}")

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
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} sn shutdown {node_uuid}{force_flag}")

    def suspend_node(self, node_uuid, expected_error_code=None):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} sn suspend {node_uuid}")

    def resume_node(self, node_uuid):
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} sn resume {node_uuid}")

    def restart_node(self, node_uuid, expected_error_code=None, force=False):
        force_flag = " --force" if force else ""
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} sn restart {node_uuid}{force_flag}")

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
        if pool_name in self.list_storage_pools():
            self.logger.info(f"Pool {pool_name} already exists. Skipping")
            return
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool add {shlex.quote(pool_name)}")

    def delete_storage_pool(self, pool_name):
        pool_id = self.get_storage_pool_id(pool_name)
        if not pool_id:
            self.logger.info("Pool does not exist. Skipping")
            return
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} pool delete {pool_id}")

    def delete_all_storage_pools(self):
        for name in list(self.list_storage_pools().keys()):
            self.logger.info(f"Deleting pool: {name}")
            self.delete_storage_pool(pool_name=name)

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
            f"{self.sbcli_cmd} snapshot add {lvol_id} {shlex.quote(snapshot_name)}"
        )

    def list_snapshots(self):
        """Parse snapshot list table output → ``{snap_name: snap_uuid}``."""
        out = self._run(f"{self.sbcli_cmd} snapshot list")
        result = {}
        for line in out.splitlines():
            parts = [p.strip() for p in line.split("|")]
            # Table columns: | UUID | BDdev UUID | BlobID | Name | ...
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
        self.k8s.exec_sbcli(f"{self.sbcli_cmd} snapshot delete {snap_id}")

    def delete_all_snapshots(self):
        for snap_name in list(self.list_snapshots().keys()):
            try:
                self.delete_snapshot(snap_name=snap_name, skip_error=True)
            except Exception as e:
                self.logger.info(f"Snapshot delete failed (continuing): {snap_name}, err={e}")
