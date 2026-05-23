"""
Bulk lvol create-delete stress test.

Creates a large batch of lvols (50 × 100G) with FIO running, waits 10 minutes,
then deletes them one-by-one verifying each deletion succeeds.  Runs 5 iterations.

Three modes:
  Docker  (BulkLvolDeleteDocker)  — sbcli API + NVMe connect + SSH FIO
  K8s     (BulkLvolDeleteK8s)     — PVC + FIO K8s Jobs OR Client SSH FIO

Invocation:
  # Docker
  python3 stress.py --testname BulkLvolDeleteDocker --ndcs 2 --npcs 2

  # K8s without client (FIO Jobs)
  python3 stress.py --testname BulkLvolDeleteK8s --ndcs 2 --npcs 2 --run_k8s True

  # K8s with client (SSH FIO)
  CLIENT_IP="10.0.0.5" python3 stress.py --testname BulkLvolDeleteK8s --ndcs 2 --npcs 2 --run_k8s True
"""

from __future__ import annotations

import random
import string
import threading
import time

from logger_config import setup_logger
from utils.common_utils import sleep_n_sec

logger = setup_logger(__name__)


def _rand_seq(length: int) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


# ─────────────────────────────────────────────────────────────────────────────
# Shared mixin: iteration loop, summary, cleanup contract
# ─────────────────────────────────────────────────────────────────────────────

class _BulkDeleteMixin:
    """Shared iteration logic: create batch → wait → delete one-by-one."""

    NUM_LVOLS = 50
    LVOL_SIZE = "100G"
    PVC_SIZE = "100Gi"
    FIO_SIZE = "1G"
    WAIT_AFTER_CREATE = 600      # 10 minutes
    NUM_ITERATIONS = 5
    DELETE_INTERVAL = 5          # seconds between sequential deletes
    FIO_RUNTIME = 2000           # seconds per FIO job

    def _run_bulk_iterations(self):
        results = []
        for iteration in range(1, self.NUM_ITERATIONS + 1):
            self.logger.info(
                f"=== Bulk Delete Iteration {iteration}/{self.NUM_ITERATIONS} ==="
            )

            names = self._bulk_create(iteration)
            self.logger.info(
                f"Created {len(names)} resources.  "
                f"Waiting {self.WAIT_AFTER_CREATE}s with FIO running..."
            )
            sleep_n_sec(self.WAIT_AFTER_CREATE)

            t_del = time.time()
            result = self._bulk_delete_sequential(iteration, names)
            result["delete_duration"] = time.time() - t_del
            results.append(result)
            self.logger.info(
                f"Iteration {iteration} done: "
                f"created={result['created']} deleted={result['deleted']} "
                f"failed={result['failed']} stale={result['stale']} "
                f"delete_time={result['delete_duration']:.1f}s"
            )

        self._bulk_cleanup()
        self._print_bulk_summary(results)
        self._write_monitoring_json(results)

        total_failed = sum(r["failed"] + r["stale"] for r in results)
        if total_failed > 0:
            raise RuntimeError(
                f"Bulk delete test had {total_failed} total failures across "
                f"{self.NUM_ITERATIONS} iterations"
            )

    # Subclasses MUST implement:
    #   _bulk_create(iteration) -> list[str]
    #   _bulk_delete_sequential(iteration, names) -> dict
    #   _bulk_cleanup()

    def _print_bulk_summary(self, results):
        self.logger.info("=== Bulk Lvol Delete Test Summary ===")
        self.logger.info(
            f"{'Iter':>4} | {'Created':>7} | {'Deleted':>7} | "
            f"{'Failed':>6} | {'Stale':>5}"
        )
        for r in results:
            self.logger.info(
                f"{r['iteration']:>4} | {r['created']:>7} | {r['deleted']:>7} | "
                f"{r['failed']:>6} | {r['stale']:>5}"
            )
        total_f = sum(r["failed"] for r in results)
        total_s = sum(r["stale"] for r in results)
        self.logger.info(f"Total failures: {total_f}  Total stale: {total_s}")

    def _write_monitoring_json(self, results):
        """Write standardised timing JSON for monitoring suite aggregation."""
        import json as _json
        from datetime import datetime, timezone
        from pathlib import Path

        phases = []
        for r in results:
            phases.append({
                "name": f"iteration_{r['iteration']}",
                "duration_sec": round(r.get("delete_duration", 0), 2),
                "status": "ok" if r["failed"] + r["stale"] == 0 else "degraded",
                "details": {
                    "created": r["created"],
                    "deleted": r["deleted"],
                    "failed": r["failed"],
                    "stale": r["stale"],
                },
            })

        total_duration = sum(r.get("delete_duration", 0) for r in results)

        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed" if all(
                r["failed"] + r["stale"] == 0 for r in results
            ) else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "batch_size": self.BATCH_SIZE,
                "num_iterations": self.NUM_ITERATIONS,
                "lvol_size": self.LVOL_SIZE,
            },
            "phases": phases,
            "summary": {
                "total_duration_sec": round(total_duration, 2),
                "key_metric": round(total_duration, 2),
                "key_metric_label": "total_delete_duration_sec",
            },
        }

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "bulk_delete_timing.json"
        with open(out_path, "w") as f:
            _json.dump(report, f, indent=2)
        self.logger.info(f"Monitoring JSON written to {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Docker variant
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.lvol_ha_stress_fio import TestLvolHACluster  # noqa: E402


class BulkLvolDeleteDocker(_BulkDeleteMixin, TestLvolHACluster):
    """
    Docker-mode bulk create+FIO → sequential delete stress test.

    Inherits from TestLvolHACluster for sbcli_utils, ssh_obj, NVMe connect,
    FIO thread management, pool/node setup.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_delete_docker"
        self.total_lvols = self.NUM_LVOLS
        self.lvol_size = self.LVOL_SIZE
        self.fio_size = self.FIO_SIZE
        self.sn_nodes = []
        self.node_vs_lvol = {}
        self.lvol_mount_details = {}
        self.fio_threads = []
        self._run_id = _rand_seq(8)

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])

        self._run_bulk_iterations()

    # ── Create ────────────────────────────────────────────────────────────

    def _bulk_create(self, iteration):
        names = []
        for i in range(self.NUM_LVOLS):
            lvol_name = f"bulk-{self._run_id}-i{iteration}-{i:03d}"
            self.logger.info(
                f"[create {iteration}] Creating lvol {lvol_name} "
                f"({i+1}/{self.NUM_LVOLS})"
            )

            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    retry=3,
                )
            except Exception as exc:
                self.logger.error(
                    f"[create {iteration}] add_lvol failed for {lvol_name}: {exc}"
                )
                continue

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            if not lvol_id:
                self.logger.error(
                    f"[create {iteration}] {lvol_name} not visible after add_lvol"
                )
                continue

            # Pick a client node for NVMe connect + FIO
            client_node = random.choice(self.fio_node)
            fs_type = random.choice(["ext4", "xfs"])

            # Get NVMe connect strings
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            # Snapshot devices before connect
            initial_devices = self.ssh_obj.get_devices(node=client_node)

            # Connect all paths
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(
                    node=client_node, command=connect_str
                )
                if error:
                    self.logger.warning(
                        f"[create {iteration}] NVMe connect warning for "
                        f"{lvol_name}: {error}"
                    )

            sleep_n_sec(3)

            # Detect new device
            final_devices = self.ssh_obj.get_devices(node=client_node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                self.logger.error(
                    f"[create {iteration}] {lvol_name} did not connect — "
                    f"no new device found"
                )
                continue

            # Format and mount
            self.ssh_obj.format_disk(
                node=client_node, device=lvol_device, fs_type=fs_type
            )
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(
                node=client_node, device=lvol_device, mount_path=mount_point
            )

            sleep_n_sec(5)

            # Clean old FIO files
            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])

            # Start FIO
            log_file = f"{self.log_path}/{lvol_name}.log"
            iolog_file = f"{self.log_path}/{lvol_name}_fio_iolog"
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_point, log_file),
                kwargs={
                    "size": self.FIO_SIZE,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": self.FIO_RUNTIME,
                    "log_avg_msec": 1000,
                    "iolog_file": iolog_file,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

            # Track
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id,
                "Command": connect_ls,
                "Mount": mount_point,
                "Device": lvol_device,
                "FS": fs_type,
                "Log": log_file,
                "Client": client_node,
                "snapshots": [],
                "iolog_base_path": iolog_file,
            }

            # Track node → lvol mapping
            lvol_node_id = None
            try:
                details = self.sbcli_utils.get_lvol_details(lvol_id)
                if details:
                    lvol_node_id = details[0].get("node_id")
            except Exception:
                pass
            if lvol_node_id:
                self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

            names.append(lvol_name)
            self.logger.info(
                f"[create {iteration}] {lvol_name} → {lvol_device} on "
                f"{client_node}, FIO started"
            )

        return names

    # ── Delete (sequential, one-by-one) ──────────────────────────────────

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0

        for idx, lvol_name in enumerate(names):
            self.logger.info(
                f"[delete {iteration}] Deleting {lvol_name} "
                f"({idx+1}/{len(names)})"
            )
            details = self.lvol_mount_details.get(lvol_name, {})
            client = details.get("Client", self.fio_node[0] if self.fio_node else None)

            # 1. Kill FIO
            if client:
                self.ssh_obj.find_process_name(
                    client, f"{lvol_name}_fio", return_pid=False
                )
                fio_pids = self.ssh_obj.find_process_name(
                    client, f"{lvol_name}_fio", return_pid=True
                )
                for pid in fio_pids:
                    self.ssh_obj.kill_processes(client, pid=pid)
                # Wait for FIO to stop
                for attempt in range(30):
                    fio_pids = self.ssh_obj.find_process_name(
                        client, f"{lvol_name}_fio", return_pid=True
                    )
                    if len(fio_pids) <= 2:
                        break
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(client, pid=pid)
                    sleep_n_sec(10)

            sleep_n_sec(5)

            # 2. Unmount
            if client and details.get("Mount"):
                try:
                    self.ssh_obj.unmount_path(client, details["Mount"])
                    self.ssh_obj.remove_dir(client, dir_path=details["Mount"])
                except Exception as exc:
                    self.logger.warning(
                        f"[delete {iteration}] Unmount failed for {lvol_name}: {exc}"
                    )

            # 3. Delete lvol
            result = self.sbcli_utils.delete_lvol(
                lvol_name, max_attempt=120, skip_error=True
            )
            if result:
                deleted += 1
                self.logger.info(
                    f"[delete {iteration}] {lvol_name} deleted successfully"
                )
            else:
                failed += 1
                self.logger.error(
                    f"[delete {iteration}] {lvol_name} FAILED to delete"
                )

            # 4. Clean up tracking
            self.lvol_mount_details.pop(lvol_name, None)
            for _, lvols in self.node_vs_lvol.items():
                if lvol_name in lvols:
                    lvols.remove(lvol_name)
                    break

            # Clean FIO log files
            if client:
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/local-{lvol_name}_fio*"]
                )
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/{lvol_name}_fio_iolog*"]
                )

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale lvols remain for this iteration
        remaining = self.sbcli_utils.list_lvols()
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale = [n for n in remaining if n.startswith(prefix)]
        if stale:
            self.logger.error(
                f"[verify {iteration}] {len(stale)} lvols still present: {stale}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": len(stale),
        }

    # ── Cleanup ──────────────────────────────────────────────────────────

    def _bulk_cleanup(self):
        self.logger.info("[cleanup] Running safety-net cleanup...")
        try:
            self.sbcli_utils.delete_all_clones()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] Safety-net cleanup failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# K8s variant
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest  # noqa: E402


class BulkLvolDeleteK8s(_BulkDeleteMixin, K8sNativeFailoverTest):
    """
    K8s-mode bulk create+FIO → sequential delete stress test.

    Inherits from K8sNativeFailoverTest for k8s_utils, PVC creation,
    FIO Job management, client SSH FIO, full setup()/teardown().

    Works in two sub-modes:
      - FIO as K8s Jobs (default, no CLIENT_IP)
      - FIO via SSH on external clients (CLIENT_IP env set)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_delete_k8s"
        self.pvc_size = self.PVC_SIZE
        self.fio_size = self.FIO_SIZE
        self.FIO_RUNTIME = _BulkDeleteMixin.FIO_RUNTIME
        self._run_id = _rand_seq(8)

    def run(self):
        # Discover storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

        # Create pool + StorageClass
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

        self._run_bulk_iterations()

    # ── Create ────────────────────────────────────────────────────────────

    def _bulk_create(self, iteration):
        names = []
        old_lvol_ids = set()

        for i in range(self.NUM_LVOLS):
            pvc_name = f"bulk-{self._run_id}-i{iteration}-{i:03d}"
            self.logger.info(
                f"[create {iteration}] Creating PVC {pvc_name} "
                f"({i+1}/{self.NUM_LVOLS})"
            )

            # Snapshot lvol IDs before PVC creation (for client mode mapping)
            if self.use_client_fio:
                old_lvol_ids = self._snapshot_lvol_ids()

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.PVC_SIZE, self.STORAGE_CLASS_NAME,
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.error(
                    f"[create {iteration}] PVC creation failed for "
                    f"{pvc_name}: {exc}"
                )
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                continue

            sleep_n_sec(5)

            if self.use_client_fio:
                # ── Client SSH FIO path ──
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[create {iteration}] Could not map PVC {pvc_name} "
                        f"to lvol — skipping"
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
                    device, failed_cmds = self._connect_lvol_on_client(
                        lvol_name, client
                    )
                except Exception as exc:
                    self.logger.error(
                        f"[create {iteration}] NVMe connect failed for "
                        f"{pvc_name}/{lvol_name}: {exc}"
                    )
                    continue

                self.ssh_obj.format_disk(
                    node=client, device=device, fs_type=fs_type
                )
                mount_point = f"{self.mount_path}/{pvc_name}"
                self.ssh_obj.mount_path(
                    node=client, device=device, mount_path=mount_point
                )
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{pvc_name}.log"
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])

                bs = f"{2 ** random.randint(2, 7)}K"
                self._start_client_fio(
                    pvc_name, client, mount_point, log_file, bs=bs
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
                    "fs_type": fs_type,
                    "storage_class": self.STORAGE_CLASS_NAME,
                }
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
                    f"[create {iteration}] PVC {pvc_name} → lvol {lvol_name} "
                    f"connected on {client}, FIO started"
                )
            else:
                # ── K8s Job FIO path ──
                job_name = f"fio-{pvc_name}"
                cm_name = f"fiocfg-{pvc_name}"

                node_id = self._get_pvc_node_id(pvc_name)
                avoid = (
                    self._get_k8s_node_for_storage_node(node_id)
                    if node_id
                    else None
                )

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
                        f"[create {iteration}] FIO Job failed for "
                        f"{pvc_name}: {exc}"
                    )

                self.pvc_details[pvc_name] = {
                    "job_name": job_name,
                    "configmap_name": cm_name,
                    "snapshots": [],
                    "node_id": node_id,
                    "storage_class": self.STORAGE_CLASS_NAME,
                }

                self.logger.info(
                    f"[create {iteration}] PVC {pvc_name} node={node_id} "
                    f"FIO Job={job_name}"
                )

            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
            names.append(pvc_name)
            sleep_n_sec(3)

        return names

    # ── Delete (sequential, one-by-one) ──────────────────────────────────

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0

        for idx, pvc_name in enumerate(names):
            self.logger.info(
                f"[delete {iteration}] Deleting PVC {pvc_name} "
                f"({idx+1}/{len(names)})"
            )
            pvc_info = self.pvc_details.get(pvc_name, {})

            # 1. Stop FIO
            if self.use_client_fio:
                client = pvc_info.get("client")
                if client:
                    self._kill_fio_on_client(pvc_name, client)
                    sleep_n_sec(5)
                    # Unmount
                    try:
                        if pvc_info.get("mount_path"):
                            self.ssh_obj.unmount_path(
                                client, pvc_info["mount_path"]
                            )
                            self.ssh_obj.remove_dir(
                                client, dir_path=pvc_info["mount_path"]
                            )
                    except Exception as exc:
                        self.logger.warning(
                            f"[delete {iteration}] Unmount failed for "
                            f"{pvc_name}: {exc}"
                        )
                    # Disconnect NVMe
                    if pvc_info.get("lvol_name"):
                        self._disconnect_lvol_on_client(
                            pvc_info["lvol_name"], client
                        )
                    self.lvol_mount_details.pop(
                        pvc_info.get("lvol_name"), None
                    )
            else:
                # Delete FIO Job + ConfigMap
                try:
                    if pvc_info.get("job_name"):
                        self.k8s_utils.delete_job(pvc_info["job_name"])
                    if pvc_info.get("configmap_name"):
                        self.k8s_utils.delete_configmap(
                            pvc_info["configmap_name"]
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"[delete {iteration}] Job cleanup failed for "
                        f"{pvc_name}: {exc}"
                    )

            # 2. Delete PVC
            try:
                self.k8s_utils.delete_pvc(pvc_name)
                deleted += 1
                self.logger.info(
                    f"[delete {iteration}] {pvc_name} deleted successfully"
                )
            except Exception as exc:
                failed += 1
                self.logger.error(
                    f"[delete {iteration}] {pvc_name} FAILED to delete: {exc}"
                )

            # 3. Clean tracking
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            self.pvc_details.pop(pvc_name, None)

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale PVCs remain for this iteration
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale_count = 0
        try:
            output = self.k8s_utils._exec_kubectl("get pvc -o name")
            if output:
                all_pvcs = output.strip().split("\n")
                stale = [p for p in all_pvcs if prefix in p]
                stale_count = len(stale)
                if stale:
                    self.logger.error(
                        f"[verify {iteration}] {stale_count} PVCs still "
                        f"present: {stale}"
                    )
        except Exception as exc:
            self.logger.warning(
                f"[verify {iteration}] Could not verify stale PVCs: {exc}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": stale_count,
        }

    # ── Cleanup ──────────────────────────────────────────────────────────

    def _bulk_cleanup(self):
        self.logger.info("[cleanup] Running safety-net cleanup...")
        try:
            # K8s resources
            prefix = f"bulk-{self._run_id}"
            output = self.k8s_utils._exec_kubectl("get pvc -o name")
            if output:
                for line in output.strip().split("\n"):
                    if prefix in line:
                        pvc_name = line.replace("persistentvolumeclaim/", "")
                        try:
                            job_name = f"fio-{pvc_name}"
                            cm_name = f"fiocfg-{pvc_name}"
                            self.k8s_utils.delete_job(job_name)
                            self.k8s_utils.delete_configmap(cm_name)
                        except Exception:
                            pass
                        try:
                            self.k8s_utils.delete_pvc(pvc_name)
                        except Exception:
                            pass
        except Exception as exc:
            self.logger.warning(f"[cleanup] K8s cleanup failed: {exc}")

        try:
            self.sbcli_utils.delete_all_clones()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] sbcli cleanup failed: {exc}")
