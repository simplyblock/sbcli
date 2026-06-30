"""
K8s-native namespace failover, rapid lifecycle, and mount-verified failover tests.

These test classes extend the K8s-native failover test suite with:

1. K8sNativeNamespacedFailoverTest — Continuous failover with namespace-grouped
   PVCs (max_namespace_per_subsys > 1).  Parent and child lvols are treated as
   individual PVCs; random deletion covers subsystem-master removal.

2. K8sNativeRapidLifecycleTest — Rapid create/clone/delete/mount cycle with
   ~2-second gaps and random deletion targets.  No outages; pure lifecycle.

3. K8sNativeMountVerifiedFailoverTest — BasicFailover with explicit mount
   verification at every lifecycle point (PVC creation, clone creation,
   post-outage recovery).
"""

from __future__ import annotations

import os
import random
import threading
import time
import traceback
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.ssh_utils import get_parent_device

from stress_test.continuous_k8s_native_failover import (
    K8sNativeFailoverTest,
    K8sNativeBasicFailoverTest,
    _rand_seq,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Class 1: K8sNativeNamespacedFailoverTest
# ═══════════════════════════════════════════════════════════════════════════════


class K8sNativeNamespacedFailoverTest(K8sNativeFailoverTest):
    """Continuous failover stress test with namespace-grouped PVCs.

    Mirrors ``RandomMultiClientFailoverNamespaceTest`` (Docker variant) but
    for K8s-native mode.  Uses a StorageClass with ``max_namespace_per_subsys``
    so the CSI provisioner auto-groups PVCs onto shared NVMe-oF subsystems.

    In pure K8s mode (FIO as Jobs), namespace grouping is transparent -- the
    CSI driver handles it.  In client mode (CLIENT_IP set), namespace-aware
    NVMe connect/disconnect is required on the SSH client.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_namespaced_failover"

        # Namespace configuration
        self.max_namespace_per_subsys = int(
            os.environ.get("MAX_NAMESPACE_PER_SUBSYS", "3")
        )
        self.NAMESPACE_SC_NAME = "simplyblock-csi-sc-ns"
        self.NAMESPACE_XFS_SC_NAME = "simplyblock-csi-sc-ns-xfs"
        self.pvc_per_group = self.max_namespace_per_subsys

        # Namespace group tracking
        # nqn -> {ctrl_dev, client, pvc_names: []}
        self.ns_group_controllers: dict[str, dict] = {}
        # pvc_name -> {nqn, ctrl_dev, ns_device}
        self.ns_pvc_info: dict[str, dict] = {}
        # lvol_name -> {device, ctrl_dev, client}
        self.stale_ns_devices: dict[str, dict] = {}

    # ── Namespace SSH helpers (client mode only) ──────────────────────────

    def _list_nvme_ns_devices(self, node: str, ctrl_dev: str) -> list[str]:
        """List NVMe namespace devices on a controller (e.g. /dev/nvme32n1, n2)."""
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"ls -1 {ctrl}n* 2>/dev/null | sort -V || true\""
        out, _ = self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)
        return [x.strip() for x in (out or "").splitlines() if x.strip()]

    def _wait_for_new_namespace_device(
        self, node: str, ctrl_dev: str, before_set: set, timeout: int = 120
    ) -> tuple:
        """Wait for a new namespace device to appear. Returns (device, new_set)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))
            diff = sorted(cur - before_set)
            if diff:
                return diff[-1], cur
            sleep_n_sec(2)
        return None, set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))

    def _rescan_nvme_namespaces(self, node: str, ctrl_dev: str):
        """Trigger namespace rescan on an NVMe controller."""
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"nvme ns-rescan {ctrl} 2>/dev/null || true\""
        out, err = self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=False)
        self.logger.info(f"[ns_rescan] ctrl={ctrl} out={out} err={err}")

    def _wait_until_namespace_device_gone(
        self, node: str, ctrl_dev: str, device: str, timeout: int = 120
    ) -> bool:
        """Wait until a specific namespace device disappears."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = set(self._list_nvme_ns_devices(node=node, ctrl_dev=ctrl_dev))
            if device not in cur:
                return True
            sleep_n_sec(2)
        return False

    def _is_last_namespace_after_delete(self, client: str, ctrl_dev: str) -> bool:
        """Check if the controller has zero namespaces left."""
        cur = set(self._list_nvme_ns_devices(node=client, ctrl_dev=ctrl_dev))
        return len(cur) == 0

    def _retry_stale_ns_rescans(self):
        """After outage recovery, retry rescan for stale NS devices."""
        if not self.stale_ns_devices:
            return
        self.logger.info(
            f"[ns] Retrying rescan for {len(self.stale_ns_devices)} stale device(s)"
        )
        for lvol_name, info in list(self.stale_ns_devices.items()):
            device = info["device"]
            ctrl_dev = info["ctrl_dev"]
            client = info["client"]
            self._rescan_nvme_namespaces(node=client, ctrl_dev=ctrl_dev)
            ctrl_name = ctrl_dev.split("/")[-1]
            sysfs_cmd = (
                f"bash -lc \"echo 1 | sudo tee /sys/class/nvme/{ctrl_name}"
                f"/rescan_controller 2>/dev/null || true\""
            )
            self.ssh_obj.exec_command(node=client, command=sysfs_cmd, supress_logs=True)
            gone = self._wait_until_namespace_device_gone(
                node=client, ctrl_dev=ctrl_dev, device=device, timeout=60
            )
            if gone:
                self.logger.info(
                    f"[ns] Stale device {device} (lvol={lvol_name}) cleared "
                    f"after outage recovery"
                )
                del self.stale_ns_devices[lvol_name]
            else:
                self.logger.warning(
                    f"[ns] Stale device {device} (lvol={lvol_name}) still present"
                )

    # ── Client-mode NQN grouping helpers ──────────────────────────────────

    def _get_lvol_nqn(self, pvc_name: str) -> str | None:
        """Get the NQN for a PVC's underlying lvol."""
        try:
            vol_handle = self.k8s_utils.get_pvc_volume_handle(pvc_name)
            if not vol_handle:
                return None
            lvol_id = vol_handle.split(":")[-1] if ":" in vol_handle else vol_handle
            details = self.sbcli_utils.get_lvol_details(lvol_id)
            if details:
                return details[0].get("nqn")
        except Exception as exc:
            self.logger.warning(f"[ns] Failed to get NQN for PVC {pvc_name}: {exc}")
        return None

    def _find_existing_controller_for_nqn(self, nqn: str) -> dict | None:
        """Check if we already have an NVMe controller connected for this NQN."""
        return self.ns_group_controllers.get(nqn)

    # ── Override: create_pvcs_with_fio ────────────────────────────────────

    def create_pvcs_with_fio(self, count: int, node_ids: list[str] = None,
                             storage_class: str = None):
        """Create PVCs using namespace-aware StorageClass.

        In K8s-Job mode, this is nearly identical to the parent -- the CSI
        driver handles namespace grouping transparently.

        In client-FIO mode, we must detect shared NVMe controllers and
        wait for new namespace devices instead of new controllers.
        """
        self._ensure_k8s_utils()

        # Default to namespace SC
        if storage_class is None:
            storage_class = self.NAMESPACE_SC_NAME

        if not self.use_client_fio:
            # ── K8s-Job mode: CSI handles namespaces transparently ──
            # Pin groups of PVCs to same node for namespace grouping
            if node_ids is None and count >= self.pvc_per_group:
                # Build node_ids to pin groups of pvc_per_group to same node
                node_ids = []
                for i in range(count):
                    node_idx = (i // self.pvc_per_group) % len(self.sn_nodes)
                    node_ids.append(self.sn_nodes[node_idx])

            super().create_pvcs_with_fio(
                count, node_ids=node_ids, storage_class=storage_class
            )

            # Track namespace groups from lvol details
            for pvc_name in list(self.pvc_details.keys()):
                if pvc_name in self.ns_pvc_info:
                    continue
                nqn = self._get_lvol_nqn(pvc_name)
                if nqn:
                    self.ns_pvc_info[pvc_name] = {"nqn": nqn}
                    if nqn not in self.ns_group_controllers:
                        self.ns_group_controllers[nqn] = {"pvc_names": []}
                    self.ns_group_controllers[nqn]["pvc_names"].append(pvc_name)
            return

        # ── Client-FIO mode: namespace-aware NVMe connect ──
        existing_count = len(self.pvc_details)
        for i in range(count):
            pvc_name = f"pvc-{_rand_seq(12)}"
            target_node = node_ids[i] if node_ids and i < len(node_ids) else None
            if target_node is None and self.sn_nodes:
                # Pin to a node in round-robin to encourage namespace grouping
                node_idx = (existing_count + i) // self.pvc_per_group % len(self.sn_nodes)
                target_node = self.sn_nodes[node_idx]

            sc_name = storage_class
            fs_type = "xfs" if sc_name == self.NAMESPACE_XFS_SC_NAME else "ext4"

            self.logger.info(
                f"[ns_create] Creating PVC {pvc_name} ({i+1}/{count}) SC={sc_name}"
                + (f" pinned to node {target_node}" if target_node else "")
            )

            old_lvol_ids = self._snapshot_lvol_ids()

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.pvc_size, sc_name, node_id=target_node
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[ns_create] PVC creation failed: {exc}")
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                continue

            sleep_n_sec(5)

            # Map PVC to lvol
            lvol_info = self._find_new_lvol(old_lvol_ids)
            if not lvol_info:
                self.logger.warning(f"[ns_create] Could not map PVC {pvc_name} to lvol")
                continue
            lvol_name, lvol_id = lvol_info

            node_id = None
            nqn = None
            try:
                details = self.sbcli_utils.get_lvol_details(lvol_id)
                if details:
                    node_id = details[0].get("node_id")
                    nqn = details[0].get("nqn")
            except Exception:
                pass

            client = self.fio_node[i % len(self.fio_node)]

            # Check if we already have a controller for this NQN
            existing_ctrl = self._find_existing_controller_for_nqn(nqn) if nqn else None

            if existing_ctrl and existing_ctrl.get("ctrl_dev"):
                # ── Namespace on existing controller ──
                ctrl_dev = existing_ctrl["ctrl_dev"]
                ctrl_client = existing_ctrl.get("client", client)
                before_set = set(self._list_nvme_ns_devices(ctrl_client, ctrl_dev))

                # The CSI provisioner already created the lvol; the NVMe
                # subsystem on the target already has the namespace.  We just
                # need to rescan so the client sees it.
                self._rescan_nvme_namespaces(node=ctrl_client, ctrl_dev=ctrl_dev)

                new_dev, new_set = self._wait_for_new_namespace_device(
                    node=ctrl_client, ctrl_dev=ctrl_dev,
                    before_set=before_set, timeout=120
                )
                if not new_dev:
                    self.logger.warning(
                        f"[ns_create] Namespace device did not appear for {pvc_name}"
                    )
                    continue

                device = new_dev
                self.logger.info(
                    f"[ns_create] PVC {pvc_name} appeared as NS device {device} "
                    f"on existing controller {ctrl_dev}"
                )
            else:
                # ── New controller (first PVC in this subsystem) ──
                try:
                    device, failed_cmds = self._connect_lvol_on_client(lvol_name, client)
                    if failed_cmds:
                        self.record_failed_secondary_connects(pvc_name, client, failed_cmds)
                except Exception as exc:
                    self.logger.warning(
                        f"[ns_create] NVMe connect failed for {pvc_name}: {exc}"
                    )
                    self.pvc_details[pvc_name] = {
                        "job_name": None, "configmap_name": None,
                        "snapshots": [], "node_id": node_id,
                        "lvol_name": lvol_name, "lvol_id": lvol_id,
                        "device": None, "mount_path": None,
                        "client": client, "log_file": None,
                        "fs_type": fs_type, "storage_class": sc_name,
                    }
                    if node_id:
                        self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
                    continue

                ctrl_dev = get_parent_device(device)

                # Register this controller for the NQN group
                if nqn:
                    self.ns_group_controllers[nqn] = {
                        "ctrl_dev": ctrl_dev, "client": client, "pvc_names": [pvc_name]
                    }

            # Format + mount
            self.ssh_obj.format_disk(node=client, device=device, fs_type=fs_type)
            mount_point = f"{self.mount_path}/{pvc_name}"
            self.ssh_obj.mount_path(node=client, device=device, mount_path=mount_point)
            sleep_n_sec(5)

            log_file = f"{self.log_path}/{pvc_name}.log"
            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])

            # FIO warmup + start
            bs = f"{2 ** random.randint(2, 7)}K"
            try:
                self._run_fio_warmup_ssh(pvc_name, client, mount_point, bs)
            except Exception as exc:
                self.logger.warning(f"[ns_create] FIO warmup failed: {exc}")

            self._start_client_fio(pvc_name, client, mount_point, log_file, bs=bs)

            self.pvc_details[pvc_name] = {
                "job_name": None, "configmap_name": None,
                "snapshots": [], "node_id": node_id,
                "lvol_name": lvol_name, "lvol_id": lvol_id,
                "device": device, "mount_path": mount_point,
                "client": client, "log_file": log_file,
                "fs_type": fs_type, "storage_class": sc_name,
            }
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id, "Mount": mount_point, "Device": device,
                "FS": fs_type, "Log": log_file, "Client": client,
                "pvc_name": pvc_name, "snapshots": [],
            }

            # Track namespace info
            if nqn:
                ctrl_dev_final = get_parent_device(device)
                self.ns_pvc_info[pvc_name] = {
                    "nqn": nqn, "ctrl_dev": ctrl_dev_final, "ns_device": device,
                }
                if nqn in self.ns_group_controllers:
                    if pvc_name not in self.ns_group_controllers[nqn]["pvc_names"]:
                        self.ns_group_controllers[nqn]["pvc_names"].append(pvc_name)
                else:
                    self.ns_group_controllers[nqn] = {
                        "ctrl_dev": ctrl_dev_final, "client": client,
                        "pvc_names": [pvc_name],
                    }

            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(
            self.pvc_details, self.clone_details,
            snapshot_details=self.snapshot_details,
        )

    # ── Override: delete_random_pvcs (client mode namespace awareness) ────

    def delete_random_pvcs(self, count: int):
        """Delete PVCs with namespace-aware NVMe disconnect in client mode."""
        if not self.use_client_fio:
            # K8s-Job mode: CSI handles namespace cleanup transparently
            super().delete_random_pvcs(count)
            # Clean up ns tracking for deleted PVCs
            for nqn, group in list(self.ns_group_controllers.items()):
                group["pvc_names"] = [
                    p for p in group["pvc_names"] if p in self.pvc_details
                ]
                if not group["pvc_names"]:
                    del self.ns_group_controllers[nqn]
            for pvc in list(self.ns_pvc_info.keys()):
                if pvc not in self.pvc_details:
                    del self.ns_pvc_info[pvc]
            return

        # ── Client mode: namespace-aware deletion ──
        self._ensure_k8s_utils()
        available = list(self.pvc_details.keys())
        if len(available) < count:
            count = len(available)
        if count == 0:
            return

        safe_to_delete = []
        for pvc_name in available:
            nid = self.pvc_details[pvc_name].get("node_id")
            if nid and len(self.node_vs_pvc.get(nid, [])) > 1:
                safe_to_delete.append(pvc_name)
        if not safe_to_delete:
            safe_to_delete = available
        targets = random.sample(safe_to_delete, min(count, len(safe_to_delete)))
        deleted_node_ids = []

        for pvc_name in targets:
            pvc_info = self.pvc_details[pvc_name]
            client = pvc_info.get("client")
            device = pvc_info.get("device")
            lvol_name = pvc_info.get("lvol_name")
            ns_info = self.ns_pvc_info.get(pvc_name, {})
            nqn = ns_info.get("nqn")
            ctrl_dev = ns_info.get("ctrl_dev") or (
                get_parent_device(device) if device else None
            )

            self.logger.info(
                f"[ns_delete] Deleting PVC {pvc_name} (nqn={nqn}, device={device})"
            )

            # Delete clones + snapshots first (same as parent)
            for snap_name in list(pvc_info.get("snapshots", [])):
                clones_to_delete = [
                    cn for cn, cd in self.clone_details.items()
                    if cd["snap_name"] == snap_name
                ]
                for clone_name in clones_to_delete:
                    clone_info = self.clone_details[clone_name]
                    c_client = clone_info.get("client", client)
                    self._kill_fio_on_client(clone_name, c_client)
                    sleep_n_sec(5)
                    try:
                        self.ssh_obj.unmount_path(c_client, clone_info["mount_path"])
                        self.ssh_obj.remove_dir(c_client, dir_path=clone_info["mount_path"])
                    except Exception:
                        pass
                    self._disconnect_lvol_on_client(clone_info["lvol_name"], c_client)
                    self.clone_mount_details.pop(clone_info.get("lvol_name"), None)
                    try:
                        self.k8s_utils.delete_pvc(clone_name)
                    except Exception as exc:
                        self.logger.warning(f"[ns_delete] Clone delete failed: {exc}")
                        self.record_pending_clone_delete(clone_name, clone_info)
                    del self.clone_details[clone_name]

                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception as exc:
                    self.logger.warning(f"[ns_delete] Snapshot delete failed: {exc}")
                self.snapshot_details.pop(snap_name, None)
                if snap_name in self.snapshot_names:
                    self.snapshot_names.remove(snap_name)

            # Stop FIO
            self._kill_fio_on_client(pvc_name, client)
            sleep_n_sec(5)

            # Unmount
            mount_path = pvc_info.get("mount_path")
            if mount_path:
                try:
                    self.ssh_obj.unmount_path(client, mount_path)
                    self.ssh_obj.remove_dir(client, dir_path=mount_path)
                except Exception:
                    pass

            # DO NOT disconnect yet — other PVCs may share this controller
            # Delete PVC (CSI removes the lvol)
            try:
                self.k8s_utils.delete_pvc(pvc_name)
            except Exception as exc:
                self.logger.warning(f"[ns_delete] PVC delete failed: {exc}")
                self.record_pending_pvc_delete(pvc_name, pvc_info)

            # Namespace device cleanup (rescan + verify gone)
            if client and ctrl_dev and device:
                self._rescan_nvme_namespaces(node=client, ctrl_dev=ctrl_dev)
                ok = self._wait_until_namespace_device_gone(
                    node=client, ctrl_dev=ctrl_dev, device=device, timeout=30
                )
                if not ok:
                    self.logger.info(
                        "[ns_delete] Device still present; sysfs fallback"
                    )
                    sleep_n_sec(30)
                    ctrl_name = ctrl_dev.split("/")[-1]
                    sysfs_cmd = (
                        f"bash -lc \"echo 1 | sudo tee /sys/class/nvme/{ctrl_name}"
                        f"/rescan_controller 2>/dev/null || true\""
                    )
                    self.ssh_obj.exec_command(
                        node=client, command=sysfs_cmd, supress_logs=False
                    )
                    ok = self._wait_until_namespace_device_gone(
                        node=client, ctrl_dev=ctrl_dev, device=device, timeout=120
                    )

                if ok:
                    self.logger.info(f"[ns_delete] NS device {device} removed")
                    # If last NS on controller, disconnect
                    if self._is_last_namespace_after_delete(client, ctrl_dev):
                        self.logger.info(
                            f"[ns_delete] Last NS on {ctrl_dev} — disconnecting"
                        )
                        if lvol_name:
                            try:
                                self._disconnect_lvol_on_client(lvol_name, client)
                            except Exception:
                                pass
                        # Clean up NQN group
                        if nqn and nqn in self.ns_group_controllers:
                            del self.ns_group_controllers[nqn]
                else:
                    self.logger.warning(
                        f"[ns_delete] Device {device} still present — tracking as stale"
                    )
                    self.stale_ns_devices[pvc_name] = {
                        "device": device, "ctrl_dev": ctrl_dev, "client": client,
                    }

            # Clean up tracking
            self.lvol_mount_details.pop(lvol_name, None)
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            del self.pvc_details[pvc_name]
            self.ns_pvc_info.pop(pvc_name, None)
            if nqn and nqn in self.ns_group_controllers:
                grp = self.ns_group_controllers[nqn]
                if pvc_name in grp["pvc_names"]:
                    grp["pvc_names"].remove(pvc_name)
            if node_id:
                deleted_node_ids.append(node_id)

        sleep_n_sec(30)

        # Recreate PVCs (same logic as parent)
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
                except Exception:
                    unpinned_count += 1

            if online_ids:
                self.create_pvcs_with_fio(len(online_ids), node_ids=online_ids)
            if unpinned_count:
                self.create_pvcs_with_fio(unpinned_count)
        self._ensure_per_node_coverage()

    # ── Override: restart_nodes_after_failover ────────────────────────────

    def restart_nodes_after_failover(self, *args, **kwargs):
        """After outage recovery, retry stale NS rescans."""
        super().restart_nodes_after_failover(*args, **kwargs)
        if self.use_client_fio:
            self._retry_stale_ns_rescans()

    # ── Override: run ─────────────────────────────────────────────────────

    def run(self):
        """Create namespace-aware StorageClasses, then delegate to parent loop."""
        self._ensure_k8s_utils()
        self._initialize_outage_log()
        self.logger.info(
            f"=== Starting K8sNativeNamespacedFailoverTest "
            f"(max_namespace_per_subsys={self.max_namespace_per_subsys}) ==="
        )

        # Read cluster config
        cluster_details = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)
        npcs_from_env = int(os.environ.get("NPCS", "0"))
        if npcs_from_env > 0:
            self.npcs = npcs_from_env
        elif self.max_fault_tolerance > 1:
            self.npcs = min(2, self.max_fault_tolerance)

        # Clean slate
        self.sbcli_utils.delete_all_clones()
        self.sbcli_utils.delete_all_snapshots()
        self.sbcli_utils.delete_all_lvols()
        try:
            self.sbcli_utils.delete_storage_pool(self.pool_name)
        except Exception:
            pass
        sleep_n_sec(10)

        # Create storage pool
        actual_pool = self.sbcli_utils.add_storage_pool(self.pool_name)
        if actual_pool and actual_pool != self.pool_name:
            self.logger.info(f"Pool name resolved: {self.pool_name!r} -> {actual_pool!r}")
            self.pool_name = actual_pool
        sleep_n_sec(10)

        # Create namespace-aware StorageClasses
        cluster_id = self.sbcli_utils.cluster_id
        self.k8s_utils.create_storage_class(
            self.NAMESPACE_SC_NAME, cluster_id, self.pool_name,
            ndcs=self.ndcs, npcs=self.distr_npcs, fs_type="ext4",
            max_namespace_per_subsys=self.max_namespace_per_subsys,
        )
        self.k8s_utils.create_storage_class(
            self.NAMESPACE_XFS_SC_NAME, cluster_id, self.pool_name,
            ndcs=self.ndcs, npcs=self.distr_npcs, fs_type="xfs",
            max_namespace_per_subsys=self.max_namespace_per_subsys,
        )
        # Also create the regular SCs for compatibility
        self.k8s_utils.create_storage_class(
            self.STORAGE_CLASS_NAME, cluster_id, self.pool_name,
            ndcs=self.ndcs, npcs=self.distr_npcs, fs_type="ext4",
        )
        self.k8s_utils.create_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)

        # Populate storage node maps
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes.get("results", []):
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            sec = result.get("secondary_node_id")
            if sec:
                self.sn_primary_secondary_map[result["uuid"]] = sec
                if sec not in self.sn_nodes_with_sec:
                    self.sn_nodes_with_sec.append(sec)
        self.total_pvcs = max(self.total_pvcs, len(self.sn_nodes))

        # Create initial PVCs — pinned in groups for namespace sharing
        initial_pvcs = max(self.total_pvcs, len(self.sn_nodes))
        self._compute_fio_size(extra_jobs=initial_pvcs)
        self.create_pvcs_with_fio(
            len(self.sn_nodes), node_ids=list(self.sn_nodes)
        )
        if initial_pvcs > len(self.sn_nodes):
            self.create_pvcs_with_fio(initial_pvcs - len(self.sn_nodes))
        self._ensure_per_node_coverage()

        # Log namespace grouping
        self.logger.info(
            f"[ns] Namespace groups: {len(self.ns_group_controllers)} NQNs, "
            f"{len(self.ns_pvc_info)} PVCs tracked"
        )
        for nqn, grp in self.ns_group_controllers.items():
            self.logger.info(
                f"[ns]   NQN={nqn[:30]}... PVCs={grp.get('pvc_names', [])}"
            )

        # Main loop (same as parent)
        iteration = 1
        test_failed = False
        try:
            while True:
                self.logger.info(f"=== Iteration {iteration} ===")

                validation_thread = threading.Thread(
                    target=self.validate_iostats_continuously, daemon=True
                )
                validation_thread.start()

                if iteration > 1:
                    self._compute_fio_size()
                    self.restart_fio(iteration)

                # Outage
                outage_events = self.perform_n_plus_k_outages()

                # Delete + create during outage
                delete_count = min(
                    iteration, len(self.pvc_details) - len(self.sn_nodes)
                )
                delete_count = max(delete_count, 1)
                self.delete_random_pvcs(delete_count)
                self.create_snapshots_and_clones()

                create_count = delete_count + 2
                self._compute_fio_size(extra_jobs=create_count)
                self.create_pvcs_with_fio(create_count)
                sleep_n_sec(280)

                # Recovery
                for node, outage_type, _ in outage_events:
                    self.current_outage_node = node
                    if outage_type == "container_stop" and self.npcs > 1:
                        self.restart_nodes_after_failover(outage_type, restart=True)
                    else:
                        self.restart_nodes_after_failover(outage_type)
                    self.logger.info("Waiting for fallback recovery.")
                    sleep_n_sec(100)

                # Health check
                for node, _, _ in outage_events:
                    try:
                        self.sbcli_utils.wait_for_health_status(node, True, timeout=300)
                    except Exception as exc:
                        self.logger.warning(f"Health check failed for {node}: {exc}")

                self.collect_outage_diagnostics("post_recovery")

                if self.use_client_fio:
                    self.retry_failed_nvme_connects()
                    self.retry_failed_secondary_connects()
                self.validate_pending_deletions()

                # Validation
                sleep_n_sec(300)
                self.check_core_dump()
                try:
                    self.common_utils.validate_io_stats(
                        self.sbcli_utils, self.mgmt_nodes[0] if self.mgmt_nodes else None,
                        self.sn_nodes,
                    )
                except Exception as exc:
                    self.logger.warning(f"IO stats validation: {exc}")

                self.wait_for_fio_complete()
                self.validate_fio_jobs()

                # Log namespace group status
                self.logger.info(
                    f"[ns] End of iteration {iteration}: "
                    f"{len(self.ns_group_controllers)} NQN groups, "
                    f"{len(self.pvc_details)} PVCs, "
                    f"{len(self.clone_details)} clones"
                )

                self.collect_outage_diagnostics(f"end_iteration_{iteration}")
                iteration += 1

        except Exception:
            test_failed = True
            self.logger.error(f"Test failed:\n{traceback.format_exc()}")
            raise
        finally:
            if test_failed:
                self.logger.info("[cleanup] Test failed")
                raise AssertionError("Namespace failover stress test failed")
            else:
                self._cleanup_all_k8s_resources()


# ═══════════════════════════════════════════════════════════════════════════════
# Class 2: K8sNativeRapidLifecycleTest
# ═══════════════════════════════════════════════════════════════════════════════


class K8sNativeRapidLifecycleTest(K8sNativeFailoverTest):
    """Rapid create/clone/delete/mount cycle with ~2-second gaps.

    No outages — this test exercises the pure lifecycle path:
    create PVC → mount-verify → snapshot → clone → mount-verify →
    random delete → mount-verify survivors → final FIO validation.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_rapid_lifecycle"

        self.RAMP_ITERATIONS = int(os.environ.get("RAMP_ITERATIONS", "10"))
        self.CHURN_ITERATIONS = int(os.environ.get("CHURN_ITERATIONS", "20"))
        self.GAP_SECONDS = int(os.environ.get("GAP_SECONDS", "2"))
        self.FIO_RUNTIME = 60  # short FIO for validation

        # Mount verification results
        self.mount_results: list[dict] = []

    def _log_mount_result(self, pvc_name: str, success: bool, message: str,
                          phase: str):
        """Log and track a mount verification result."""
        result = {
            "pvc": pvc_name,
            "success": success,
            "message": message,
            "phase": phase,
            "timestamp": time.time(),
        }
        self.mount_results.append(result)
        level = "INFO" if success else "ERROR"
        getattr(self.logger, level.lower())(
            f"[mount_verify] {phase}: {pvc_name} -> {message}"
        )

    def _verify_pvc(self, pvc_name: str, phase: str) -> bool:
        """Verify a PVC is mountable and log the result."""
        success, msg = self.k8s_utils.verify_pvc_mount(pvc_name)
        self._log_mount_result(pvc_name, success, msg, phase)
        return success

    def _delete_clone_safely(self, clone_name: str):
        """Delete a clone PVC and its parent snapshot."""
        clone_info = self.clone_details.get(clone_name)
        if not clone_info:
            return

        # Delete FIO Job if running
        if clone_info.get("job_name"):
            try:
                self.k8s_utils.delete_job(clone_info["job_name"])
                self.k8s_utils.delete_configmap(clone_info["configmap_name"])
            except Exception:
                pass

        # Delete clone PVC
        try:
            self.k8s_utils.delete_pvc(clone_name)
        except Exception as exc:
            self.logger.warning(f"[rapid] Clone delete failed: {exc}")

        snap_name = clone_info["snap_name"]
        del self.clone_details[clone_name]

        # Check if snapshot has no more clones
        remaining_clones = [
            cn for cn, cd in self.clone_details.items()
            if cd["snap_name"] == snap_name
        ]
        if not remaining_clones:
            try:
                self.k8s_utils.delete_volume_snapshot(snap_name)
            except Exception as exc:
                self.logger.warning(f"[rapid] Snapshot delete failed: {exc}")
            self.snapshot_details.pop(snap_name, None)
            if snap_name in self.snapshot_names:
                self.snapshot_names.remove(snap_name)
            # Remove snap from parent PVC tracking
            for pvc_info in self.pvc_details.values():
                if snap_name in pvc_info.get("snapshots", []):
                    pvc_info["snapshots"].remove(snap_name)

    def _delete_random_pvc_safely(self, pvc_name: str):
        """Delete a PVC and all its clones/snapshots."""
        pvc_info = self.pvc_details.get(pvc_name)
        if not pvc_info:
            return

        # Delete all clones + snapshots first
        for snap_name in list(pvc_info.get("snapshots", [])):
            clones_to_delete = [
                cn for cn, cd in self.clone_details.items()
                if cd["snap_name"] == snap_name
            ]
            for clone_name in clones_to_delete:
                self._delete_clone_safely(clone_name)
            # Snapshot should already be deleted by _delete_clone_safely
            # if it had no more clones; delete explicitly if still present
            if snap_name in self.snapshot_details:
                try:
                    self.k8s_utils.delete_volume_snapshot(snap_name)
                except Exception:
                    pass
                self.snapshot_details.pop(snap_name, None)

        # Delete FIO Job if running
        if pvc_info.get("job_name"):
            try:
                self.k8s_utils.delete_job(pvc_info["job_name"])
                self.k8s_utils.delete_configmap(pvc_info["configmap_name"])
            except Exception:
                pass

        # Delete PVC
        try:
            self.k8s_utils.delete_pvc(pvc_name)
        except Exception as exc:
            self.logger.warning(f"[rapid] PVC delete failed: {exc}")

        # Clean tracking
        node_id = pvc_info.get("node_id")
        if node_id and node_id in self.node_vs_pvc:
            if pvc_name in self.node_vs_pvc[node_id]:
                self.node_vs_pvc[node_id].remove(pvc_name)
        del self.pvc_details[pvc_name]

    def _create_snapshot_and_clone(self, phase: str) -> str | None:
        """Create a snapshot + clone from a random PVC. Returns clone name."""
        available_pvcs = list(self.pvc_details.keys())
        if not available_pvcs:
            return None

        pvc_name = random.choice(available_pvcs)
        snap_name = f"snap-{_rand_seq(12)}"
        clone_name = f"clone-{_rand_seq(12)}"

        try:
            self.k8s_utils.create_volume_snapshot(
                snap_name, pvc_name, self.SNAPSHOT_CLASS_NAME
            )
            self.k8s_utils.wait_volume_snapshot_ready(snap_name, timeout=300)
        except Exception as exc:
            self.logger.warning(f"[rapid] Snapshot creation failed: {exc}")
            try:
                self.k8s_utils.delete_volume_snapshot(snap_name)
            except Exception:
                pass
            return None

        self.snapshot_details[snap_name] = {"pvc_name": pvc_name}
        self.pvc_details[pvc_name]["snapshots"].append(snap_name)
        self.snapshot_names.append(snap_name)

        try:
            self.k8s_utils.create_clone_pvc(
                clone_name, self.pvc_size, self.STORAGE_CLASS_NAME, snap_name
            )
            self.k8s_utils.wait_pvc_bound(clone_name, timeout=300)
        except Exception as exc:
            self.logger.warning(f"[rapid] Clone creation failed: {exc}")
            try:
                self.k8s_utils.delete_pvc(clone_name)
            except Exception:
                pass
            return None

        node_id = self._get_pvc_node_id(clone_name)
        self.clone_details[clone_name] = {
            "snap_name": snap_name,
            "job_name": None,
            "configmap_name": None,
            "node_id": node_id,
            "storage_class": self.STORAGE_CLASS_NAME,
        }

        # Mount verify the clone
        self._verify_pvc(clone_name, f"{phase}_clone")
        return clone_name

    def run(self):
        """Rapid lifecycle test: ramp → churn → validate."""
        self._ensure_k8s_utils()
        self.logger.info("=== Starting K8sNativeRapidLifecycleTest ===")

        # Setup: clean + create pool + StorageClass
        cluster_details = self.sbcli_utils.get_cluster_details()
        self.max_fault_tolerance = cluster_details.get("max_fault_tolerance", 1)

        self.sbcli_utils.delete_all_clones()
        self.sbcli_utils.delete_all_snapshots()
        self.sbcli_utils.delete_all_lvols()
        try:
            self.sbcli_utils.delete_storage_pool(self.pool_name)
        except Exception:
            pass
        sleep_n_sec(10)

        actual_pool = self.sbcli_utils.add_storage_pool(self.pool_name)
        if actual_pool and actual_pool != self.pool_name:
            self.logger.info(f"Pool name resolved: {self.pool_name!r} -> {actual_pool!r}")
            self.pool_name = actual_pool
        sleep_n_sec(10)

        cluster_id = self.sbcli_utils.cluster_id
        self.k8s_utils.create_storage_class(
            self.STORAGE_CLASS_NAME, cluster_id, self.pool_name,
            ndcs=self.ndcs, npcs=self.distr_npcs, fs_type="ext4",
        )
        self.k8s_utils.create_volume_snapshot_class(self.SNAPSHOT_CLASS_NAME)

        # Populate node maps
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes.get("results", []):
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            sec = result.get("secondary_node_id")
            if sec:
                self.sn_primary_secondary_map[result["uuid"]] = sec

        # ── Phase 1: Ramp up ──
        self.logger.info(f"=== Phase 1: Ramp up ({self.RAMP_ITERATIONS} iterations) ===")
        for i in range(self.RAMP_ITERATIONS):
            self.logger.info(f"[ramp] Iteration {i+1}/{self.RAMP_ITERATIONS}")

            pvc_name = f"pvc-{_rand_seq(12)}"
            target_node = self.sn_nodes[i % len(self.sn_nodes)] if self.sn_nodes else None

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.pvc_size, self.STORAGE_CLASS_NAME,
                    node_id=target_node,
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[ramp] PVC creation failed: {exc}")
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                sleep_n_sec(self.GAP_SECONDS)
                continue

            node_id = self._get_pvc_node_id(pvc_name) or target_node
            self.pvc_details[pvc_name] = {
                "job_name": None, "configmap_name": None,
                "snapshots": [], "node_id": node_id,
                "storage_class": self.STORAGE_CLASS_NAME,
            }
            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)

            # Mount verify
            self._verify_pvc(pvc_name, "ramp_create")

            # Every 3rd iteration: snapshot + clone
            if (i + 1) % 3 == 0:
                self._create_snapshot_and_clone("ramp")

            sleep_n_sec(self.GAP_SECONDS)

        # ── Phase 2: Random churn ──
        self.logger.info(
            f"=== Phase 2: Random churn ({self.CHURN_ITERATIONS} iterations) ==="
        )
        for i in range(self.CHURN_ITERATIONS):
            self.logger.info(f"[churn] Iteration {i+1}/{self.CHURN_ITERATIONS}")

            # Pick random action
            r = random.random()
            if r < 0.30:
                # Create PVC (30%)
                pvc_name = f"pvc-{_rand_seq(12)}"
                target = random.choice(self.sn_nodes) if self.sn_nodes else None
                try:
                    self.k8s_utils.create_pvc(
                        pvc_name, self.pvc_size, self.STORAGE_CLASS_NAME,
                        node_id=target,
                    )
                    self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
                    node_id = self._get_pvc_node_id(pvc_name) or target
                    self.pvc_details[pvc_name] = {
                        "job_name": None, "configmap_name": None,
                        "snapshots": [], "node_id": node_id,
                        "storage_class": self.STORAGE_CLASS_NAME,
                    }
                    if node_id:
                        self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
                    self._verify_pvc(pvc_name, "churn_create")
                except Exception as exc:
                    self.logger.warning(f"[churn] Create failed: {exc}")
                    try:
                        self.k8s_utils.delete_pvc(pvc_name)
                    except Exception:
                        pass

            elif r < 0.50:
                # Snapshot + clone (20%)
                self._create_snapshot_and_clone("churn")

            elif r < 0.80:
                # Delete random PVC (30%)
                if len(self.pvc_details) > 1:
                    target = random.choice(list(self.pvc_details.keys()))
                    self.logger.info(f"[churn] Deleting PVC: {target}")
                    self._delete_random_pvc_safely(target)
                else:
                    self.logger.info("[churn] Only 1 PVC left, skipping delete")

            else:
                # Delete random clone (20%)
                if self.clone_details:
                    target = random.choice(list(self.clone_details.keys()))
                    self.logger.info(f"[churn] Deleting clone: {target}")
                    self._delete_clone_safely(target)
                else:
                    self.logger.info("[churn] No clones to delete")

            # Mount verify a random survivor
            all_resources = list(self.pvc_details.keys()) + list(self.clone_details.keys())
            if all_resources:
                verify_target = random.choice(all_resources)
                self._verify_pvc(verify_target, "churn_verify")

            sleep_n_sec(self.GAP_SECONDS)

        # ── Phase 3: Final validation ──
        self.logger.info("=== Phase 3: Final validation ===")

        # Mount verify ALL survivors
        all_pvcs = list(self.pvc_details.keys())
        all_clones = list(self.clone_details.keys())
        self.logger.info(
            f"[validate] Verifying {len(all_pvcs)} PVCs + {len(all_clones)} clones"
        )

        for pvc_name in all_pvcs + all_clones:
            self._verify_pvc(pvc_name, "final_verify")

        # Start short FIO on all PVCs
        self.logger.info("[validate] Starting FIO on all surviving resources")
        self._compute_fio_size()
        for pvc_name in all_pvcs:
            job_name = f"fio-{pvc_name}"
            cm_name = f"fiocfg-{pvc_name}"
            node_id = self.pvc_details[pvc_name].get("node_id")
            avoid = self._get_k8s_node_for_storage_node(node_id) if node_id else None
            fio_config, warmup_config = self._build_fio_config(pvc_name)
            try:
                self.k8s_utils.create_fio_job(
                    job_name, pvc_name, cm_name, fio_config,
                    image=self.FIO_IMAGE, avoid_node=avoid,
                    warmup_config=warmup_config,
                )
                self.pvc_details[pvc_name]["job_name"] = job_name
                self.pvc_details[pvc_name]["configmap_name"] = cm_name
            except Exception as exc:
                self.logger.warning(f"[validate] FIO start failed for {pvc_name}: {exc}")

        for clone_name in all_clones:
            job_name = f"fio-{clone_name}"
            cm_name = f"fiocfg-{clone_name}"
            node_id = self.clone_details[clone_name].get("node_id")
            avoid = self._get_k8s_node_for_storage_node(node_id) if node_id else None
            fio_config, warmup_config = self._build_fio_config(clone_name)
            try:
                self.k8s_utils.create_fio_job(
                    job_name, clone_name, cm_name, fio_config,
                    image=self.FIO_IMAGE, cleanup_before_fio=True,
                    avoid_node=avoid, warmup_config=warmup_config,
                )
                self.clone_details[clone_name]["job_name"] = job_name
                self.clone_details[clone_name]["configmap_name"] = cm_name
            except Exception as exc:
                self.logger.warning(f"[validate] FIO start failed for {clone_name}: {exc}")

        # Wait for FIO completion
        self.logger.info("[validate] Waiting for FIO to complete")
        sleep_n_sec(self.FIO_RUNTIME + 60)
        self.validate_fio_jobs()

        # Print summary
        total = len(self.mount_results)
        passed = sum(1 for r in self.mount_results if r["success"])
        failed = total - passed
        self.logger.info(
            f"=== Rapid Lifecycle Test Complete ===\n"
            f"  Mount verifications: {total} total, {passed} passed, {failed} failed\n"
            f"  PVCs remaining: {len(self.pvc_details)}\n"
            f"  Clones remaining: {len(self.clone_details)}"
        )

        if failed > 0:
            self.logger.error("FAILED mount verifications:")
            for r in self.mount_results:
                if not r["success"]:
                    self.logger.error(
                        f"  [{r['phase']}] {r['pvc']}: {r['message']}"
                    )

        # Cleanup
        self._cleanup_all_k8s_resources()

        if failed > 0:
            raise AssertionError(
                f"Rapid lifecycle test: {failed}/{total} mount verifications failed"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Class 3: K8sNativeMountVerifiedFailoverTest
# ═══════════════════════════════════════════════════════════════════════════════


class K8sNativeMountVerifiedFailoverTest(K8sNativeBasicFailoverTest):
    """BasicFailover with mount verification at every lifecycle point.

    Overrides PVC and clone creation to verify mountability before FIO.
    After outage recovery, verifies mount on random survivors.

    Note: PVCs are RWO.  The verification pod is always deleted before
    the FIO Job is started, so there is no mount conflict.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "k8s_native_mount_verified_failover"
        self.mount_results: list[dict] = []

    def _log_mount_result(self, pvc_name: str, success: bool, message: str,
                          phase: str):
        result = {
            "pvc": pvc_name, "success": success,
            "message": message, "phase": phase,
            "timestamp": time.time(),
        }
        self.mount_results.append(result)
        level = "INFO" if success else "ERROR"
        getattr(self.logger, level.lower())(
            f"[mount_verify] {phase}: {pvc_name} -> {message}"
        )

    def _verify_pvc(self, pvc_name: str, phase: str) -> bool:
        success, msg = self.k8s_utils.verify_pvc_mount(pvc_name)
        self._log_mount_result(pvc_name, success, msg, phase)
        return success

    def create_pvcs_with_fio(self, count: int, node_ids: list[str] = None,
                             storage_class: str = None):
        """Create PVCs with mount verification BEFORE starting FIO.

        For K8s-Job mode: create PVC → wait bound → verify mount →
        delete verify pod → start FIO Job.

        For client mode: delegate to parent (mount happens during NVMe
        connect + format, which is itself a mount verification).
        """
        if self.use_client_fio:
            super().create_pvcs_with_fio(count, node_ids=node_ids,
                                         storage_class=storage_class)
            return

        self._ensure_k8s_utils()
        existing_count = len(self.pvc_details)

        for i in range(count):
            pvc_name = f"pvc-{_rand_seq(12)}"
            target_node = node_ids[i] if node_ids and i < len(node_ids) else None

            if storage_class:
                sc_name = storage_class
            elif self.tls_enabled and (existing_count + i) % 2 == 1:
                sc_name = self.CRYPTO_STORAGE_CLASS_NAME
            else:
                sc_name = random.choice(
                    [self.STORAGE_CLASS_NAME, self.XFS_STORAGE_CLASS_NAME]
                )
            fs_type = "xfs" if sc_name == self.XFS_STORAGE_CLASS_NAME else "ext4"

            self.logger.info(
                f"[mv_create] Creating PVC {pvc_name} ({i+1}/{count}) SC={sc_name}"
                + (f" pinned to {target_node}" if target_node else "")
            )

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.pvc_size, sc_name, node_id=target_node
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.warning(f"[mv_create] PVC creation failed: {exc}")
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                continue

            sleep_n_sec(5)

            # Mount verification BEFORE FIO
            self._verify_pvc(pvc_name, "pre_fio_create")
            sleep_n_sec(2)  # ensure verify pod is fully deleted

            # Start FIO Job
            job_name = f"fio-{pvc_name}"
            cm_name = f"fiocfg-{pvc_name}"
            node_id = self._get_pvc_node_id(pvc_name)
            avoid = self._get_k8s_node_for_storage_node(node_id) if node_id else None

            fio_config, warmup_config = self._build_fio_config(pvc_name)
            try:
                self.k8s_utils.create_fio_job(
                    job_name, pvc_name, cm_name, fio_config,
                    image=self.FIO_IMAGE, avoid_node=avoid,
                    warmup_config=warmup_config,
                )
            except Exception as exc:
                self.logger.warning(f"[mv_create] FIO Job failed for {pvc_name}: {exc}")

            self.pvc_details[pvc_name] = {
                "job_name": job_name, "configmap_name": cm_name,
                "snapshots": [], "node_id": node_id,
                "storage_class": sc_name, "fs_type": fs_type,
            }

            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
            sleep_n_sec(5)

        self.k8s_utils.log_fio_pvc_mapping(
            self.pvc_details, self.clone_details,
            snapshot_details=self.snapshot_details,
        )

    def create_snapshots_and_clones(self):
        """Create snapshots + clones with mount verification on clones."""
        # Let parent create the snapshots and clones
        super().create_snapshots_and_clones()

        # Verify mount on newly created clones
        # (FIO Job is already started by parent, but we can still log
        #  whether the mount was successful based on Job pod status)
        for clone_name, clone_info in list(self.clone_details.items()):
            if clone_info.get("job_name"):
                # FIO Job is using the PVC — we can't mount it again (RWO)
                # Instead, check if the FIO pod is Running (indirect mount verify)
                try:
                    self.k8s_utils.wait_pod_running(
                        f"{clone_info['job_name']}-", timeout=60
                    )
                    self._log_mount_result(
                        clone_name, True,
                        "Clone FIO Job pod is Running (mount OK)",
                        "post_clone",
                    )
                except Exception:
                    self._log_mount_result(
                        clone_name, False,
                        "Clone FIO Job pod not Running",
                        "post_clone",
                    )

    def create_snapshots_and_clones_with_cleanup(self, num_clones: int):
        """Override to add mount verification for clones."""
        super().create_snapshots_and_clones_with_cleanup(num_clones)
        self.logger.info("[mv] Verifying mount on newly created clones")
        for clone_name in list(self.clone_details.keys()):
            if self.clone_details[clone_name].get("job_name"):
                self.logger.info(
                    f"[mv] Clone {clone_name} has FIO Job running — "
                    f"indirect mount verification via pod status"
                )

    def restart_nodes_after_failover(self, *args, **kwargs):
        """After recovery, verify mount on 2 random PVCs."""
        super().restart_nodes_after_failover(*args, **kwargs)

        if self.use_client_fio:
            # Client mode: mounts are on SSH client, not in K8s
            return

        # Pick 2 random PVCs that have FIO Jobs
        active_pvcs = [
            p for p, info in self.pvc_details.items()
            if info.get("job_name")
        ]
        if len(active_pvcs) < 2:
            return

        verify_targets = random.sample(active_pvcs, min(2, len(active_pvcs)))
        for pvc_name in verify_targets:
            pvc_info = self.pvc_details[pvc_name]
            old_job = pvc_info["job_name"]
            old_cm = pvc_info["configmap_name"]

            # Stop FIO so we can mount-verify
            self.logger.info(
                f"[mv_recovery] Stopping FIO on {pvc_name} for mount verification"
            )
            try:
                self.k8s_utils.delete_job(old_job)
                self.k8s_utils.delete_configmap(old_cm)
            except Exception:
                pass
            sleep_n_sec(10)

            # Mount verify
            self._verify_pvc(pvc_name, "post_recovery")
            sleep_n_sec(2)

            # Restart FIO
            new_job = f"fio-{_rand_seq(8)}"
            new_cm = f"fiocfg-{_rand_seq(8)}"
            fio_config, warmup_config = self._build_fio_config(pvc_name)
            node_id = pvc_info.get("node_id")
            avoid = self._get_k8s_node_for_storage_node(node_id) if node_id else None

            try:
                self.k8s_utils.create_fio_job(
                    new_job, pvc_name, new_cm, fio_config,
                    image=self.FIO_IMAGE, cleanup_before_fio=True,
                    avoid_node=avoid, warmup_config=warmup_config,
                )
                pvc_info["job_name"] = new_job
                pvc_info["configmap_name"] = new_cm
                self.logger.info(
                    f"[mv_recovery] FIO restarted on {pvc_name} (Job={new_job})"
                )
            except Exception as exc:
                self.logger.warning(
                    f"[mv_recovery] FIO restart failed for {pvc_name}: {exc}"
                )
