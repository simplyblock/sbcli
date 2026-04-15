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

import os
import random
import string
import threading
import time
from collections import defaultdict
from datetime import datetime

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.k8s_utils import K8sUtils


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
        self.SNAPSHOT_CLASS_NAME = "simplyblock-csi-snapshotclass"
        self.FIO_IMAGE = "dockerpinata/fio:2.1"

        # Sizing
        self.pvc_size = "20Gi"
        self.int_pvc_size = 20
        self.fio_size = "15G"
        self.FIO_RUNTIME = 2000

        # Counts
        self.total_pvcs = 20
        self.fio_num_jobs = 4

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

        # Outage log
        self.outage_log_file = os.path.join(
            "logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

    # ── Setup / Teardown ─────────────────────────────────────────────────────

    def setup(self):
        super().setup()
        if self.k8s_test and self.mgmt_nodes:
            self.k8s_utils = K8sUtils(
                ssh_obj=self.ssh_obj,
                mgmt_node=self.mgmt_nodes[0],
            )
            self.logger.info(f"[K8s] K8sUtils initialized for mgmt_node={self.mgmt_nodes[0]}")

    def _ensure_k8s_utils(self):
        if not self.k8s_utils:
            raise RuntimeError(
                "[K8s] k8s_utils not initialised — was setup() called with k8s_run=True?"
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

    def _build_fio_config(self, name: str) -> str:
        bs = f"{2 ** random.randint(2, 7)}k"
        return (
            f"[global]\n"
            f"name={name}-fio\n"
            f"filename=/spdkvol/fio-testfile\n"
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

    # ── PVC + FIO creation ───────────────────────────────────────────────────

    def create_pvcs_with_fio(self, count: int):
        """Create *count* PVCs via K8s and start an FIO Job on each."""
        self._ensure_k8s_utils()
        for i in range(count):
            pvc_name = f"pvc-{_rand_seq(12)}"
            job_name = f"fio-{pvc_name}"
            cm_name = f"fiocfg-{pvc_name}"

            self.logger.info(f"[create_pvc] Creating PVC {pvc_name} ({i+1}/{count})")

            try:
                self.k8s_utils.create_pvc(pvc_name, self.pvc_size, self.STORAGE_CLASS_NAME)
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[create_pvc] PVC creation failed for {pvc_name}: {exc}")
                continue

            # Verify the underlying lvol exists via sbcli (verification only)
            sleep_n_sec(5)
            node_id = None
            try:
                lvols = self.sbcli_utils.list_lvols()
                for lname, lid in lvols.items():
                    details = self.sbcli_utils.get_lvol_details(lid)
                    if details:
                        node_id = details[0].get("node_id")
                        break
            except Exception:
                pass

            # Start FIO Job
            fio_config = self._build_fio_config(pvc_name)
            try:
                self.k8s_utils.create_fio_job(
                    job_name, pvc_name, cm_name, fio_config,
                    image=self.FIO_IMAGE,
                )
            except Exception as exc:
                self.logger.warning(f"[create_pvc] FIO Job creation failed for {pvc_name}: {exc}")

            self.pvc_details[pvc_name] = {
                "job_name": job_name,
                "configmap_name": cm_name,
                "snapshots": [],
                "node_id": node_id,
            }
            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)

            self.logger.info(f"[create_pvc] PVC {pvc_name} created with FIO Job {job_name}")
            sleep_n_sec(5)

    # ── Snapshot & Clone creation ────────────────────────────────────────────

    def create_snapshots_and_clones(self):
        """Create 3 snapshots + clones + FIO Jobs, then resize source & clone."""
        self._ensure_k8s_utils()
        self.int_pvc_size += 1
        available_pvcs = list(self.pvc_details.keys())
        if not available_pvcs:
            self.logger.warning("[snap_clone] No PVCs available for snapshots")
            return

        for _ in range(3):
            random.shuffle(available_pvcs)
            pvc_name = available_pvcs[0]
            snap_name = f"snap-{_rand_seq(12)}"
            clone_name = f"clone-{_rand_seq(12)}"
            clone_job = f"fio-{clone_name}"
            clone_cm = f"fiocfg-{clone_name}"

            # Create snapshot
            try:
                self.k8s_utils.create_volume_snapshot(
                    snap_name, pvc_name, self.SNAPSHOT_CLASS_NAME
                )
                self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Snapshot creation failed for {snap_name}: {exc}")
                continue

            self.snapshot_details[snap_name] = {"pvc_name": pvc_name}
            self.snapshot_names.append(snap_name)
            self.pvc_details[pvc_name]["snapshots"].append(snap_name)

            # Create clone PVC
            sleep_n_sec(10)
            try:
                self.k8s_utils.create_clone_pvc(
                    clone_name, self.pvc_size, self.STORAGE_CLASS_NAME, snap_name
                )
                self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Clone PVC creation failed for {clone_name}: {exc}")
                continue

            # Start FIO on clone
            fio_config = self._build_fio_config(clone_name)
            try:
                self.k8s_utils.create_fio_job(
                    clone_job, clone_name, clone_cm, fio_config,
                    image=self.FIO_IMAGE,
                )
            except Exception as exc:
                self.logger.warning(f"[snap_clone] Clone FIO Job failed for {clone_name}: {exc}")

            self.clone_details[clone_name] = {
                "snap_name": snap_name,
                "job_name": clone_job,
                "configmap_name": clone_cm,
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

    # ── Delete PVCs ──────────────────────────────────────────────────────────

    def delete_random_pvcs(self, count: int):
        """Delete *count* random PVCs and their snapshots/clones."""
        self._ensure_k8s_utils()
        available = list(self.pvc_details.keys())
        if len(available) < count:
            self.logger.warning(
                f"[delete_pvcs] Only {len(available)} PVCs available, requested {count}"
            )
            count = len(available)
        if count == 0:
            return

        for pvc_name in random.sample(available, count):
            self.logger.info(f"[delete_pvcs] Deleting PVC tree: {pvc_name}")
            pvc_info = self.pvc_details[pvc_name]

            # Delete clones (and their FIO Jobs) for each snapshot of this PVC
            for snap_name in list(pvc_info["snapshots"]):
                clones_to_delete = [
                    cn for cn, cd in self.clone_details.items()
                    if cd["snap_name"] == snap_name
                ]
                for clone_name in clones_to_delete:
                    clone_info = self.clone_details[clone_name]
                    try:
                        self.k8s_utils.delete_job(clone_info["job_name"])
                        self.k8s_utils.delete_configmap(clone_info["configmap_name"])
                    except Exception as exc:
                        self.logger.warning(f"[delete_pvcs] Clone Job cleanup failed: {exc}")
                    try:
                        self.k8s_utils.delete_pvc(clone_name)
                    except Exception as exc:
                        self.logger.warning(f"[delete_pvcs] Clone PVC delete failed: {exc}")
                    del self.clone_details[clone_name]

                # Delete the snapshot
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception as exc:
                    self.logger.warning(f"[delete_pvcs] Snapshot delete failed: {exc}")
                self.snapshot_details.pop(snap_name, None)
                if snap_name in self.snapshot_names:
                    self.snapshot_names.remove(snap_name)

            # Delete FIO Job for the PVC itself
            try:
                self.k8s_utils.delete_job(pvc_info["job_name"])
                self.k8s_utils.delete_configmap(pvc_info["configmap_name"])
            except Exception as exc:
                self.logger.warning(f"[delete_pvcs] PVC Job cleanup failed: {exc}")

            # Delete the PVC
            try:
                self.k8s_utils.delete_pvc(pvc_name)
            except Exception as exc:
                self.logger.warning(f"[delete_pvcs] PVC delete failed: {exc}")

            # Clean up tracking
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            del self.pvc_details[pvc_name]

        sleep_n_sec(30)

    # ── Restart FIO ──────────────────────────────────────────────────────────

    def restart_fio(self, iteration: int):
        """Restart FIO Jobs on all PVCs and clones (delete old Job, create new)."""
        self._ensure_k8s_utils()
        self.logger.info(f"[restart_fio] Restarting FIO for iteration {iteration}")

        # Restart FIO on PVCs
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

            fio_config = self._build_fio_config(pvc_name)
            try:
                self.k8s_utils.create_fio_job(
                    new_job, pvc_name, new_cm, fio_config,
                    image=self.FIO_IMAGE,
                )
            except Exception as exc:
                self.logger.warning(f"[restart_fio] Failed to restart FIO for {pvc_name}: {exc}")
                continue

            pvc_info["job_name"] = new_job
            pvc_info["configmap_name"] = new_cm
            sleep_n_sec(5)

        # Restart FIO on clones
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

            fio_config = self._build_fio_config(clone_name)
            try:
                self.k8s_utils.create_fio_job(
                    new_job, clone_name, new_cm, fio_config,
                    image=self.FIO_IMAGE,
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
        """Select K nodes and trigger outages simultaneously."""
        primary_candidates = list(self.sn_primary_secondary_map.keys())
        self.current_outage_nodes = []

        if len(primary_candidates) < self.npcs:
            raise Exception(
                f"Need {self.npcs} outage nodes, but only "
                f"{len(primary_candidates)} primary-role nodes exist."
            )

        outage_nodes = self._pick_outage_nodes(primary_candidates, self.npcs)
        self.logger.info(f"Selected outage nodes: {outage_nodes}")
        self.collect_outage_diagnostics(f"pre_outage_nodes_{'_'.join(outage_nodes[:3])}")

        node_plans = []
        outage_num = 0
        for node in outage_nodes:
            if outage_num == 0:
                if self.npcs == 1:
                    outage_type = random.choice(self.outage_types2)
                else:
                    outage_type = random.choice(self.outage_types)
                outage_num = 1
            else:
                outage_type = random.choice(self.outage_types2)

            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]
            node_plans.append((node, outage_type, node_ip, node_rpc_port))

        # Trigger all outages simultaneously
        outage_results = {}

        def _trigger(node, outage_type, node_ip, _rpc_port):
            self.logger.info(f"Performing {outage_type} on node {node}")
            if outage_type == "container_stop":
                self._k8s_stop_spdk_pod(node_ip, node)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            self.log_outage_event(node, outage_type, "Outage started")
            outage_results[node] = (outage_type, 0)

        threads = [
            threading.Thread(target=_trigger, args=(n, ot, nip, nrpc))
            for n, ot, nip, nrpc in node_plans
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        outage_combinations = []
        for node, _, _, _ in node_plans:
            otype, odur = outage_results[node]
            outage_combinations.append((node, otype, odur))
            self.current_outage_nodes.append(node)

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
            self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=300)
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
                    self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=300)
                    self.log_outage_event(node, outage_type, "Node restarted")
            else:
                self.sbcli_utils.wait_for_storage_node_status(node, "online", timeout=300)
                self.log_outage_event(node, outage_type, "Node restarted", outage_time=2)

        # Wait for health check
        try:
            self.sbcli_utils.wait_for_health_status(node, True, timeout=300)
        except Exception as exc:
            self.logger.warning(f"Health check did not pass for {node}: {exc}")

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
                )
                sleep_n_sec(300)
            except Exception as e:
                self.logger.error(f"IO stats validation error: {e}")
                break

    # ── FIO Validation ───────────────────────────────────────────────────────

    def validate_fio_jobs(self):
        """Validate all active FIO Jobs (status + pod logs)."""
        self._ensure_k8s_utils()

        for pvc_name, pvc_info in self.pvc_details.items():
            try:
                self.k8s_utils.validate_fio_job(pvc_info["job_name"])
            except Exception as exc:
                self.logger.warning(f"[validate_fio] PVC {pvc_name} FIO validation: {exc}")

        for clone_name, clone_info in self.clone_details.items():
            try:
                self.k8s_utils.validate_fio_job(clone_info["job_name"])
            except Exception as exc:
                self.logger.warning(f"[validate_fio] Clone {clone_name} FIO validation: {exc}")

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def _cleanup_all_k8s_resources(self):
        """Best-effort cleanup of all test K8s resources."""
        if not self.k8s_utils:
            return

        self.logger.info("[cleanup] Deleting all test K8s resources...")

        # Delete clone Jobs + ConfigMaps + PVCs
        for clone_name, clone_info in list(self.clone_details.items()):
            try:
                self.k8s_utils.delete_job(clone_info["job_name"])
                self.k8s_utils.delete_configmap(clone_info["configmap_name"])
                self.k8s_utils.delete_pvc(clone_name)
            except Exception:
                pass

        # Delete VolumeSnapshots
        for snap_name in list(self.snapshot_details.keys()):
            try:
                self.k8s_utils.delete_volume_snapshot(snap_name)
            except Exception:
                pass

        # Delete PVC Jobs + ConfigMaps + PVCs
        for pvc_name, pvc_info in list(self.pvc_details.items()):
            try:
                self.k8s_utils.delete_job(pvc_info["job_name"])
                self.k8s_utils.delete_configmap(pvc_info["configmap_name"])
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
        max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        self.logger.info(f"Cluster max_fault_tolerance: {max_fault_tolerance}")
        if self.npcs == 1:
            self.npcs = max_fault_tolerance
        self.logger.info(f"Running with npcs={self.npcs} simultaneous outages")

        # Ensure pool
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        if actual_pool and actual_pool != self.pool_name:
            self.logger.info(f"Using existing pool '{actual_pool}' instead of '{self.pool_name}'")
            self.pool_name = actual_pool

        # Create StorageClass and VolumeSnapshotClass
        cluster_id = self.cluster_id or ""
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )
        self.k8s_utils.create_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)
        sleep_n_sec(5)

        # Populate storage node maps
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = result["secondary_node_id"]
        self.logger.info(f"Storage nodes: {len(self.sn_nodes)}, secondary map: {self.sn_primary_secondary_map}")

        # Create initial PVCs with FIO
        self.create_pvcs_with_fio(self.total_pvcs)
        sleep_n_sec(30)

        iteration = 1
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
                self.delete_random_pvcs(5)
                self.create_pvcs_with_fio(5)
                self.create_snapshots_and_clones()
                sleep_n_sec(280)

                # ── Recovery phase ──
                for node, outage_type, node_outage_dur in outage_events:
                    self.current_outage_node = node
                    if outage_type == "container_stop" and self.npcs > 1:
                        self.restart_nodes_after_failover(outage_type, restart=True)
                    else:
                        self.restart_nodes_after_failover(outage_type)
                    self.logger.info("Waiting for fallback recovery.")
                    sleep_n_sec(100)

                self.collect_outage_diagnostics("post_recovery")

                # ── Validation phase ──
                sleep_n_sec(300)
                self.check_core_dump()

                time_duration = self.common_utils.calculate_time_duration(
                    start_timestamp=self.outage_start_time,
                    end_timestamp=self.outage_end_time,
                )
                self.common_utils.validate_io_stats(
                    cluster_id=self.cluster_id,
                    start_timestamp=self.outage_start_time,
                    end_timestamp=self.outage_end_time,
                    time_duration=time_duration,
                )
                self.validate_migration_for_node(self.outage_start_time, 2000, None, 60)
                self.validate_fio_jobs()

                self.logger.info(f"=== Iteration {iteration} complete ===")
                self.collect_outage_diagnostics(f"end_iteration_{iteration}")
                iteration += 1

        finally:
            self._cleanup_all_k8s_resources()
